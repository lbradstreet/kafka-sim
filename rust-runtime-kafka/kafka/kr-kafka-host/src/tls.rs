//! Passive rustls unbuffered client. All transport progress is supplied by the
//! owner of a `ColdStream`: acknowledge ciphertext only after the corresponding
//! owned write completes, and retain that write even if its user waiter drops.
//! A transport failure fences this connection; Kafka retry plaintext is then
//! encrypted afresh by a new TLS connection.

use crate::{SecurityError, reserve};
use rustls::{
    ClientConfig, client::UnbufferedClientConnection, pki_types::ServerName,
    unbuffered::ConnectionState,
};
use std::{marker::PhantomData, sync::Arc};
use zeroize::Zeroize;

/// Fixed allocation budgets. Ciphertext is divided equally between RX and TX;
/// plaintext contains at most one complete TLS application record.
#[derive(Clone, Copy, Debug)]
pub struct TlsLimits {
    pub plaintext_bytes: usize,
    pub ciphertext_bytes: usize,
}

impl Default for TlsLimits {
    fn default() -> Self {
        Self {
            plaintext_bytes: 16 * 1024,
            ciphertext_bytes: 256 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TlsProgress {
    Transmit,
    Receive,
    Plaintext,
    Ready,
    PeerClosed,
}

/// Borrowed proof that certificate-verified TLS has reached application traffic.
/// Its constructor is private; PLAIN cannot use a requested-but-unverified TLS
/// configuration as authorization to expose a password.
pub struct VerifiedTls<'a> {
    _connection: PhantomData<&'a TlsClient>,
}

pub struct TlsClient {
    connection: UnbufferedClientConnection,
    // Exactly two fixed backing allocations. RX/TX occupy disjoint halves.
    ciphertext: Vec<u8>,
    plaintext: Vec<u8>,
    half: usize,
    rx_len: usize,
    tx_start: usize,
    tx_len: usize,
    plain_start: usize,
    plain_len: usize,
    handshake_transmitted: bool,
    established: bool,
    peer_closed: bool,
    local_closed: bool,
    failed: bool,
}

impl TlsClient {
    /// Builds a client using caller-supplied trust, entropy and time providers.
    /// Production should use rustls's ring provider and verified trust roots;
    /// no process-global crypto-provider installation is performed here.
    ///
    /// # Errors
    /// Rejects insufficient/oversized budgets, early data, invalid server/config
    /// combinations, or fixed-buffer allocation failure before starting I/O.
    pub fn new(
        config: Arc<ClientConfig>,
        server_name: ServerName<'static>,
        limits: TlsLimits,
    ) -> Result<Self, SecurityError> {
        if !(16 * 1024..=1024 * 1024).contains(&limits.plaintext_bytes) {
            return Err(SecurityError::InvalidConfig {
                field: "tls_plaintext_bytes",
            });
        }
        if !(2 * (16 * 1024 + 2048)..=16 * 1024 * 1024).contains(&limits.ciphertext_bytes)
            || !limits.ciphertext_bytes.is_multiple_of(2)
        {
            return Err(SecurityError::InvalidConfig {
                field: "tls_ciphertext_bytes",
            });
        }
        if config.enable_early_data {
            return Err(SecurityError::InvalidConfig {
                field: "tls_early_data",
            });
        }
        let mut ciphertext = Vec::new();
        reserve(
            &mut ciphertext,
            limits.ciphertext_bytes,
            "TLS ciphertext bytes",
        )?;
        ciphertext.resize(limits.ciphertext_bytes, 0);
        let mut plaintext = Vec::new();
        reserve(
            &mut plaintext,
            limits.plaintext_bytes,
            "TLS plaintext bytes",
        )?;
        plaintext.resize(limits.plaintext_bytes, 0);
        let connection = UnbufferedClientConnection::new(config, server_name)
            .map_err(|_| SecurityError::TlsFailed)?;
        Ok(Self {
            connection,
            ciphertext,
            plaintext,
            half: limits.ciphertext_bytes / 2,
            rx_len: 0,
            tx_start: 0,
            tx_len: 0,
            plain_start: 0,
            plain_len: 0,
            handshake_transmitted: false,
            established: false,
            peer_closed: false,
            local_closed: false,
            failed: false,
        })
    }

    #[must_use]
    pub fn retained_capacity(&self) -> usize {
        self.ciphertext.capacity() + self.plaintext.capacity()
    }

    #[must_use]
    pub const fn max_encrypt_bytes(&self) -> usize {
        16 * 1024
    }

    #[must_use]
    pub fn peer_verified(&self) -> Option<VerifiedTls<'_>> {
        (self.established && !self.failed && !self.peer_closed && !self.local_closed).then_some(
            VerifiedTls {
                _connection: PhantomData,
            },
        )
    }

    #[must_use]
    pub fn outbound_ciphertext(&self) -> &[u8] {
        &self.ciphertext[self.half + self.tx_start..self.half + self.tx_len]
    }

    /// Acknowledges exactly the known prefix completed by the owned stream.
    /// On uncertain transport errors call `transport_failed` instead.
    ///
    /// # Errors
    /// Rejects over-reported completion progress without altering the cursor.
    pub fn consume_outbound(&mut self, bytes: usize) -> Result<(), SecurityError> {
        if self.failed || bytes > self.tx_len - self.tx_start {
            return Err(SecurityError::InvalidState);
        }
        self.tx_start += bytes;
        if self.tx_start == self.tx_len {
            self.tx_start = 0;
            self.tx_len = 0;
        }
        Ok(())
    }

    #[must_use]
    pub fn plaintext(&self) -> &[u8] {
        &self.plaintext[self.plain_start..self.plain_len]
    }

    /// # Errors
    /// Rejects consumption beyond the exposed plaintext without state change.
    pub fn consume_plaintext(&mut self, bytes: usize) -> Result<(), SecurityError> {
        if bytes > self.plain_len - self.plain_start {
            return Err(SecurityError::InvalidState);
        }
        self.plaintext[self.plain_start..self.plain_start + bytes].zeroize();
        self.plain_start += bytes;
        if self.plain_start == self.plain_len {
            self.plain_start = 0;
            self.plain_len = 0;
        }
        Ok(())
    }

    /// Copies a bounded prefix from a completed read. A zero return means the
    /// owner must drive or consume existing data before supplying more bytes.
    ///
    /// # Errors
    /// Rejects a fenced/closed connection.
    pub fn receive_ciphertext(&mut self, bytes: &[u8]) -> Result<usize, SecurityError> {
        if self.failed || self.peer_closed {
            return Err(SecurityError::InvalidState);
        }
        let count = bytes.len().min(self.half - self.rx_len);
        self.ciphertext[self.rx_len..self.rx_len + count].copy_from_slice(&bytes[..count]);
        self.rx_len += count;
        Ok(count)
    }

    /// Processes at most one record/handshake transition. Call again for a new
    /// state after consuming its bytes; no transport, clock, or worker is hidden.
    ///
    /// # Errors
    /// Certificate, framing, authentication or buffer failures fence the client.
    pub fn drive(&mut self) -> Result<TlsProgress, SecurityError> {
        if self.failed {
            return Err(SecurityError::TlsFailed);
        }
        if self.tx_len != 0 {
            return Ok(TlsProgress::Transmit);
        }
        if self.plain_len != 0 {
            return Ok(TlsProgress::Plaintext);
        }
        if self.peer_closed {
            return Ok(TlsProgress::PeerClosed);
        }
        // Acknowledging TransmitTlsData can expose a second immediate state.
        // Bounded loop prevents an accidental library/API change busy-spinning.
        for _ in 0..8 {
            let (incoming, outgoing) = self.ciphertext.split_at_mut(self.half);
            let status = self
                .connection
                .process_tls_records(&mut incoming[..self.rx_len]);
            let mut discard = status.discard;
            let result = match status.state {
                Ok(ConnectionState::EncodeTlsData(mut data)) => data
                    .encode(outgoing)
                    .map(|len| {
                        self.tx_len = len;
                        self.handshake_transmitted = true;
                        Some(TlsProgress::Transmit)
                    })
                    .map_err(|_| SecurityError::ResourceExhausted {
                        resource: "TLS handshake ciphertext",
                        limit: self.half,
                    }),
                Ok(ConnectionState::TransmitTlsData(data)) => {
                    if self.handshake_transmitted {
                        data.done();
                        self.handshake_transmitted = false;
                        Ok(None)
                    } else {
                        Err(SecurityError::InvalidState)
                    }
                }
                Ok(ConnectionState::BlockedHandshake) => {
                    if self.rx_len - discard == self.half {
                        Err(SecurityError::ResourceExhausted {
                            resource: "TLS handshake receive",
                            limit: self.half,
                        })
                    } else {
                        Ok(Some(TlsProgress::Receive))
                    }
                }
                Ok(ConnectionState::WriteTraffic(_)) => {
                    self.established = true;
                    Ok(Some(TlsProgress::Ready))
                }
                Ok(ConnectionState::ReadTraffic(mut traffic)) => {
                    self.established = true;
                    match traffic.next_record() {
                        Some(Ok(record)) if record.payload.len() <= self.plaintext.len() => {
                            self.plaintext[..record.payload.len()].copy_from_slice(record.payload);
                            self.plain_len = record.payload.len();
                            discard += record.discard;
                            Ok(Some(TlsProgress::Plaintext))
                        }
                        Some(Ok(_)) => Err(SecurityError::ResourceExhausted {
                            resource: "TLS plaintext bytes",
                            limit: self.plaintext.len(),
                        }),
                        Some(Err(_)) => Err(SecurityError::TlsFailed),
                        None => Ok(None),
                    }
                }
                Ok(ConnectionState::PeerClosed | ConnectionState::Closed) => {
                    self.peer_closed = true;
                    Ok(Some(TlsProgress::PeerClosed))
                }
                _ => Err(SecurityError::TlsFailed),
            };
            if discard > self.rx_len {
                self.transport_failed();
                return Err(SecurityError::TlsFailed);
            }
            incoming.copy_within(discard..self.rx_len, 0);
            self.rx_len -= discard;
            match result {
                Ok(Some(progress)) => return Ok(progress),
                Ok(None) => {}
                Err(error) => {
                    self.transport_failed();
                    return Err(error);
                }
            }
        }
        self.transport_failed();
        Err(SecurityError::TlsFailed)
    }

    /// Encrypts one bounded plaintext chunk into reusable ciphertext storage.
    /// Caller-owned compressed bytes remain untouched and may be retried on a
    /// fresh connection. `drive` must first report `Ready`.
    ///
    /// # Errors
    /// Returns a no-effect state/bound rejection, or fences on TLS failure.
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Result<(), SecurityError> {
        if self.failed
            || !self.established
            || self.peer_closed
            || self.local_closed
            || self.tx_len != 0
            || self.plain_len != 0
        {
            return Err(SecurityError::InvalidState);
        }
        if plaintext.is_empty() || plaintext.len() > self.max_encrypt_bytes() {
            return Err(SecurityError::ResourceExhausted {
                resource: "TLS plaintext bytes",
                limit: self.max_encrypt_bytes(),
            });
        }
        let status = self.connection.process_tls_records(&mut []);
        match status.state {
            Ok(ConnectionState::WriteTraffic(mut traffic)) => {
                let result = traffic.encrypt(plaintext, &mut self.ciphertext[self.half..]);
                match result {
                    Ok(bytes) => {
                        self.tx_len = bytes;
                        Ok(())
                    }
                    Err(_) => {
                        self.transport_failed();
                        Err(SecurityError::TlsFailed)
                    }
                }
            }
            _ => {
                self.transport_failed();
                Err(SecurityError::InvalidState)
            }
        }
    }

    /// Encrypts close_notify after the owner has drained previous writes.
    ///
    /// # Errors
    /// Requires an active connection with no unread or unsent data.
    pub fn close_notify(&mut self) -> Result<(), SecurityError> {
        if self.failed
            || !self.established
            || self.local_closed
            || self.tx_len != 0
            || self.plain_len != 0
            || self.rx_len != 0
        {
            return Err(SecurityError::InvalidState);
        }
        let status = self.connection.process_tls_records(&mut []);
        match status.state {
            Ok(ConnectionState::WriteTraffic(mut traffic)) => {
                match traffic.queue_close_notify(&mut self.ciphertext[self.half..]) {
                    Ok(bytes) => {
                        self.tx_len = bytes;
                        self.local_closed = true;
                        Ok(())
                    }
                    Err(_) => {
                        self.transport_failed();
                        Err(SecurityError::TlsFailed)
                    }
                }
            }
            _ => {
                self.transport_failed();
                Err(SecurityError::InvalidState)
            }
        }
    }

    /// # Errors
    /// Reports truncation unless an authenticated close_notify was processed.
    pub fn transport_eof(&mut self) -> Result<(), SecurityError> {
        if self.peer_closed {
            Ok(())
        } else {
            self.transport_failed();
            Err(SecurityError::TruncatedTls)
        }
    }

    pub fn transport_failed(&mut self) {
        self.failed = true;
        self.plaintext.zeroize();
        self.ciphertext.zeroize();
        self.plain_start = 0;
        self.plain_len = 0;
        self.tx_start = 0;
        self.tx_len = 0;
        self.rx_len = 0;
    }
}

impl Drop for TlsClient {
    fn drop(&mut self) {
        self.plaintext.zeroize();
    }
}
