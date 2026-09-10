//! SASL PLAIN and SCRAM (RFC 5802/7677). SCRAM credentials use the RFC's
//! permitted ASCII-only profile; non-ASCII credentials are rejected explicitly.
//! PLAIN accepts UTF-8 credentials and requires an established TLS capability.

use std::num::NonZeroU32;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use ring::{digest, hmac, pbkdf2, rand::SecureRandom};
use zeroize::Zeroizing;

use crate::{SecurityError, reserve, tls::VerifiedTls};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScramMechanism {
    Sha256,
    Sha512,
}

impl ScramMechanism {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Sha256 => "SCRAM-SHA-256",
            Self::Sha512 => "SCRAM-SHA-512",
        }
    }

    fn hmac(self) -> hmac::Algorithm {
        match self {
            Self::Sha256 => hmac::HMAC_SHA256,
            Self::Sha512 => hmac::HMAC_SHA512,
        }
    }

    fn digest(self) -> &'static digest::Algorithm {
        match self {
            Self::Sha256 => &digest::SHA256,
            Self::Sha512 => &digest::SHA512,
        }
    }

    fn pbkdf2(self) -> pbkdf2::Algorithm {
        match self {
            Self::Sha256 => pbkdf2::PBKDF2_HMAC_SHA256,
            Self::Sha512 => pbkdf2::PBKDF2_HMAC_SHA512,
        }
    }

    const fn output_len(self) -> usize {
        match self {
            Self::Sha256 => 32,
            Self::Sha512 => 64,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct SaslLimits {
    pub credential_bytes: usize,
    pub message_bytes: usize,
    pub salt_bytes: usize,
    pub max_iterations: u32,
}

impl Default for SaslLimits {
    fn default() -> Self {
        Self {
            credential_bytes: 4096,
            message_bytes: 16 * 1024,
            salt_bytes: 1024,
            max_iterations: 1_000_000,
        }
    }
}

impl SaslLimits {
    /// # Errors
    /// Rejects zero limits, excessively large configured bounds, or iteration
    /// caps below the SCRAM minimum of 4096.
    pub fn validate(self) -> Result<Self, SecurityError> {
        if self.credential_bytes == 0 || self.credential_bytes > 64 * 1024 {
            return Err(SecurityError::InvalidConfig {
                field: "credential_bytes",
            });
        }
        if self.message_bytes < 256 || self.message_bytes > 1024 * 1024 {
            return Err(SecurityError::InvalidConfig {
                field: "message_bytes",
            });
        }
        if self.salt_bytes == 0 || self.salt_bytes > self.message_bytes {
            return Err(SecurityError::InvalidConfig {
                field: "salt_bytes",
            });
        }
        if !(4096..=10_000_000).contains(&self.max_iterations) {
            return Err(SecurityError::InvalidConfig {
                field: "max_iterations",
            });
        }
        Ok(self)
    }
}

/// Construct the Kafka PLAIN initial response (empty authorization identity).
/// The returned credential bytes are zeroized when dropped.
///
/// # Errors
/// Rejects empty credentials, NULs, or configured bounds before allocation.
pub fn plain_response(
    _tls: &VerifiedTls<'_>,
    username: &str,
    password: &str,
    limits: SaslLimits,
) -> Result<Zeroizing<Vec<u8>>, SecurityError> {
    let limits = limits.validate()?;
    validate_credential(username, limits)?;
    validate_credential(password, limits)?;
    Ok(Zeroizing::new(bounded_join(
        &[b"\0", username.as_bytes(), b"\0", password.as_bytes()],
        limits.message_bytes,
    )?))
}

/// One SCRAM exchange. Consuming transitions prevent challenge replay.
pub struct ScramClient {
    mechanism: ScramMechanism,
    limits: SaslLimits,
    password: Zeroizing<Vec<u8>>,
    nonce: Vec<u8>,
    first_bare: Vec<u8>,
    first: Vec<u8>,
}

impl ScramClient {
    /// Starts an exchange with a fresh 192-bit nonce from ring's OS CSPRNG.
    ///
    /// # Errors
    /// Returns credential/bound validation errors or entropy failure. Invalid
    /// inputs consume no entropy.
    pub fn start(
        mechanism: ScramMechanism,
        username: &str,
        password: &str,
        limits: SaslLimits,
    ) -> Result<Self, SecurityError> {
        let limits = validate_scram_credentials(username, password, limits)?;
        let mut nonce = [0u8; 24];
        ring::rand::SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| SecurityError::EntropyUnavailable)?;
        let encoded = encode_base64(&nonce, limits.message_bytes)?;
        Self::with_nonce(mechanism, username, password, limits, &encoded)
    }

    /// Simulation-only nonce injection. The caller obtains fresh bytes from its
    /// domain-separated Workload RNG; production builds expose only `start`.
    ///
    /// # Errors
    /// Validates credentials and the printable, comma-free nonce before use.
    #[cfg(any(test, feature = "test-support"))]
    pub fn start_with_nonce_for_test(
        mechanism: ScramMechanism,
        username: &str,
        password: &str,
        limits: SaslLimits,
        nonce: &[u8],
    ) -> Result<Self, SecurityError> {
        Self::with_nonce(mechanism, username, password, limits, nonce)
    }

    fn with_nonce(
        mechanism: ScramMechanism,
        username: &str,
        password: &str,
        limits: SaslLimits,
        nonce: &[u8],
    ) -> Result<Self, SecurityError> {
        let limits = validate_scram_credentials(username, password, limits)?;
        if nonce.len() < 16
            || nonce.len() > limits.message_bytes / 4
            || !nonce.iter().all(|b| matches!(b, 0x21..=0x2b | 0x2d..=0x7e))
        {
            return Err(SecurityError::InvalidServerNonce);
        }
        let mut escaped = Vec::new();
        let escaped_len = username
            .bytes()
            .try_fold(0usize, |n, b| {
                n.checked_add(if matches!(b, b',' | b'=') { 3 } else { 1 })
            })
            .ok_or(SecurityError::InvalidCredentials)?;
        reserve(&mut escaped, escaped_len, "SASL username")?;
        for byte in username.bytes() {
            match byte {
                b',' => escaped.extend_from_slice(b"=2C"),
                b'=' => escaped.extend_from_slice(b"=3D"),
                byte => escaped.push(byte),
            }
        }
        let first_bare = bounded_join(&[b"n=", &escaped, b",r=", nonce], limits.message_bytes)?;
        let first = bounded_join(&[b"n,,", &first_bare], limits.message_bytes)?;
        Ok(Self {
            mechanism,
            limits,
            password: Zeroizing::new(bounded_join(
                &[password.as_bytes()],
                limits.credential_bytes,
            )?),
            nonce: bounded_join(&[nonce], limits.message_bytes)?,
            first_bare,
            first,
        })
    }

    #[must_use]
    pub fn initial_response(&self) -> &[u8] {
        &self.first
    }

    /// Validates a server challenge and yields owned, bounded PBKDF2 work.
    /// No password derivation runs on this path.
    ///
    /// # Errors
    /// Rejects duplicate/malformed attributes, mandatory extensions, nonce
    /// mismatches, invalid salt, and iteration counts outside the configured cap.
    pub fn on_server_first(self, message: &[u8]) -> Result<ScramWork, SecurityError> {
        let attrs = attributes(message, self.limits.message_bytes)?;
        if attrs.len() < 3 || attrs[0].0 != b'r' || attrs[1].0 != b's' || attrs[2].0 != b'i' {
            return Err(SecurityError::InvalidChallenge);
        }
        let nonce = attrs[0].1;
        if nonce.len() <= self.nonce.len()
            || !nonce.starts_with(&self.nonce)
            || !nonce.iter().all(|b| matches!(b, 0x21..=0x2b | 0x2d..=0x7e))
        {
            return Err(SecurityError::InvalidServerNonce);
        }
        let salt = decode_base64(attrs[1].1, self.limits.salt_bytes)?;
        if salt.is_empty() {
            return Err(SecurityError::InvalidChallenge);
        }
        let number = attrs[2].1;
        if number.is_empty() || number[0] == b'0' {
            return Err(SecurityError::InvalidChallenge);
        }
        let iterations = number
            .iter()
            .try_fold(0u32, |value, byte| {
                if !byte.is_ascii_digit() {
                    return None;
                }
                value.checked_mul(10)?.checked_add(u32::from(byte - b'0'))
            })
            .filter(|n| (4096..=self.limits.max_iterations).contains(n))
            .and_then(NonZeroU32::new)
            .ok_or(SecurityError::InvalidChallenge)?;
        let final_without_proof = bounded_join(&[b"c=biws,r=", nonce], self.limits.message_bytes)?;
        let auth_message = bounded_join(
            &[&self.first_bare, b",", message, b",", &final_without_proof],
            self.limits.message_bytes,
        )?;
        Ok(ScramWork {
            mechanism: self.mechanism,
            limits: self.limits,
            password: self.password,
            salt,
            iterations,
            auth_message,
            final_without_proof,
        })
    }
}

/// Owned PBKDF2 input. Submit through a separately budgeted control worker.
pub struct ScramWork {
    mechanism: ScramMechanism,
    limits: SaslLimits,
    password: Zeroizing<Vec<u8>>,
    salt: Vec<u8>,
    iterations: NonZeroU32,
    auth_message: Vec<u8>,
    final_without_proof: Vec<u8>,
}

impl ScramWork {
    /// Conservative admission bound covering inputs, output and crypto scratch.
    ///
    /// # Errors
    /// Reports accounting overflow before worker admission.
    pub fn retained_bytes(&self) -> Result<usize, SecurityError> {
        [
            self.password.capacity(),
            self.salt.capacity(),
            self.auth_message.capacity(),
            self.final_without_proof.capacity(),
            self.limits.message_bytes,
            2048,
        ]
        .into_iter()
        .try_fold(0usize, |sum, n| sum.checked_add(n))
        .ok_or(SecurityError::ResourceExhausted {
            resource: "SASL control bytes",
            limit: usize::MAX,
        })
    }

    /// Computes the SCRAM proof. Host callers run this only on control workers;
    /// a simulation job may compute it at its explicit modeled completion.
    ///
    /// # Errors
    /// Reports bounded output allocation failure.
    pub fn compute(self) -> Result<ScramProof, SecurityError> {
        let len = self.mechanism.output_len();
        let mut salted = Zeroizing::new([0u8; 64]);
        pbkdf2::derive(
            self.mechanism.pbkdf2(),
            self.iterations,
            &self.salt,
            &self.password,
            &mut salted[..len],
        );
        let salted_key = hmac::Key::new(self.mechanism.hmac(), &salted[..len]);
        let client_key = hmac::sign(&salted_key, b"Client Key");
        let stored_key = digest::digest(self.mechanism.digest(), client_key.as_ref());
        let client_signature = hmac::sign(
            &hmac::Key::new(self.mechanism.hmac(), stored_key.as_ref()),
            &self.auth_message,
        );
        let mut proof = Zeroizing::new([0u8; 64]);
        for (i, byte) in proof[..len].iter_mut().enumerate() {
            *byte = client_key.as_ref()[i] ^ client_signature.as_ref()[i];
        }
        let proof64 = Zeroizing::new(encode_base64(&proof[..len], self.limits.message_bytes)?);
        let response = Zeroizing::new(bounded_join(
            &[&self.final_without_proof, b",p=", &proof64],
            self.limits.message_bytes,
        )?);
        let server_key = hmac::sign(&salted_key, b"Server Key");
        Ok(ScramProof {
            response,
            verifier: ScramVerifier {
                key: hmac::Key::new(self.mechanism.hmac(), server_key.as_ref()),
                auth_message: self.auth_message,
                limits: self.limits,
                signature_len: len,
            },
        })
    }
}

/// Client-final response plus one-use server verifier. Sending the response is
/// insufficient for authentication; `verify` must succeed.
pub struct ScramProof {
    pub response: Zeroizing<Vec<u8>>,
    pub verifier: ScramVerifier,
}

pub struct ScramVerifier {
    key: hmac::Key,
    auth_message: Vec<u8>,
    limits: SaslLimits,
    signature_len: usize,
}

impl ScramVerifier {
    /// Verifies the server-final message in constant time through ring.
    ///
    /// # Errors
    /// Rejects server errors, malformed fields, and incorrect signatures.
    pub fn verify(self, message: &[u8]) -> Result<(), SecurityError> {
        let attrs = attributes(message, self.limits.message_bytes)?;
        if attrs[0].0 == b'e' {
            return Err(SecurityError::AuthenticationRejected);
        }
        if attrs[0].0 != b'v' || attrs.iter().any(|(name, _)| *name == b'e') {
            return Err(SecurityError::InvalidChallenge);
        }
        let signature = decode_base64(attrs[0].1, self.signature_len)?;
        hmac::verify(&self.key, &self.auth_message, &signature)
            .map_err(|_| SecurityError::InvalidServerSignature)
    }
}

fn validate_credential(value: &str, limits: SaslLimits) -> Result<(), SecurityError> {
    if value.is_empty() || value.len() > limits.credential_bytes || value.contains('\0') {
        Err(SecurityError::InvalidCredentials)
    } else {
        Ok(())
    }
}

fn validate_scram_credentials(
    username: &str,
    password: &str,
    limits: SaslLimits,
) -> Result<SaslLimits, SecurityError> {
    let limits = limits.validate()?;
    validate_credential(username, limits)?;
    validate_credential(password, limits)?;
    if !username
        .bytes()
        .chain(password.bytes())
        .all(|b| (0x20..=0x7e).contains(&b))
    {
        return Err(SecurityError::InvalidCredentials);
    }
    // The production nonce encodes to 32 bytes. Preflight the full initial
    // response before obtaining any entropy or allocating an escaped username.
    let initial_len = username.bytes().try_fold(40usize, |n, b| {
        n.checked_add(if matches!(b, b',' | b'=') { 3 } else { 1 })
    });
    if initial_len.is_none_or(|len| len > limits.message_bytes) {
        return Err(SecurityError::ResourceExhausted {
            resource: "SASL message bytes",
            limit: limits.message_bytes,
        });
    }
    Ok(limits)
}

fn bounded_join(parts: &[&[u8]], limit: usize) -> Result<Vec<u8>, SecurityError> {
    let len = parts
        .iter()
        .try_fold(0usize, |sum, part| sum.checked_add(part.len()))
        .filter(|len| *len <= limit)
        .ok_or(SecurityError::ResourceExhausted {
            resource: "SASL message bytes",
            limit,
        })?;
    let mut result = Vec::new();
    reserve(&mut result, len, "SASL message bytes")?;
    for part in parts {
        result.extend_from_slice(part);
    }
    Ok(result)
}

fn encode_base64(bytes: &[u8], limit: usize) -> Result<Vec<u8>, SecurityError> {
    let len = base64::encoded_len(bytes.len(), true)
        .filter(|len| *len <= limit)
        .ok_or(SecurityError::ResourceExhausted {
            resource: "SASL base64 bytes",
            limit,
        })?;
    let mut result = Vec::new();
    reserve(&mut result, len, "SASL base64 bytes")?;
    result.resize(len, 0);
    STANDARD
        .encode_slice(bytes, &mut result)
        .map_err(|_| SecurityError::InvalidChallenge)?;
    Ok(result)
}

fn decode_base64(bytes: &[u8], limit: usize) -> Result<Vec<u8>, SecurityError> {
    // The estimate may overrun an exact digest bound by two bytes for padding.
    let len = base64::decoded_len_estimate(bytes.len());
    if len > limit.saturating_add(2) {
        return Err(SecurityError::InvalidChallenge);
    }
    let mut result = Vec::new();
    reserve(&mut result, len, "SASL base64 bytes")?;
    result.resize(len, 0);
    let written = STANDARD
        .decode_slice(bytes, &mut result)
        .map_err(|_| SecurityError::InvalidChallenge)?;
    if written > limit {
        return Err(SecurityError::InvalidChallenge);
    }
    result.truncate(written);
    Ok(result)
}

fn attributes(message: &[u8], limit: usize) -> Result<Vec<(u8, &[u8])>, SecurityError> {
    if message.is_empty() || message.len() > limit {
        return Err(SecurityError::InvalidChallenge);
    }
    let mut seen = [false; 128];
    let mut result = Vec::new();
    result
        .try_reserve_exact(26)
        .map_err(|_| SecurityError::ResourceExhausted {
            resource: "SASL attributes",
            limit: 26,
        })?;
    for part in message.split(|b| *b == b',') {
        if part.len() < 3
            || part[1] != b'='
            || !part[0].is_ascii_alphabetic()
            || !part[2..].iter().all(|b| (0x21..=0x7e).contains(b))
        {
            return Err(SecurityError::InvalidChallenge);
        }
        let name = part[0];
        if name == b'm' {
            return Err(SecurityError::UnsupportedExtension);
        }
        if seen[usize::from(name)] || result.len() == 26 {
            return Err(SecurityError::InvalidChallenge);
        }
        seen[usize::from(name)] = true;
        result.push((name, &part[2..]));
    }
    Ok(result)
}

#[cfg(test)]
mod tests;
