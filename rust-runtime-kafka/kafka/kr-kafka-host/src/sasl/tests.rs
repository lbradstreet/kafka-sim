use super::*;

const NONCE: &[u8] = b"rOprNGfwEbeRWgbNEkqO";
const CHALLENGE: &[u8] =
    b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096";
const FINAL_PREFIX: &str = "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,p=";

fn client(mechanism: ScramMechanism) -> ScramClient {
    ScramClient::start_with_nonce_for_test(
        mechanism,
        "user",
        "pencil",
        SaslLimits::default(),
        NONCE,
    )
    .unwrap()
}

#[test]
fn sha256_matches_rfc7677_and_requires_the_server_signature() {
    // RFC 7677 section 3's complete exchange, including server proof.
    let client = client(ScramMechanism::Sha256);
    assert_eq!(
        client.initial_response(),
        b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
    );
    let proof = client
        .on_server_first(CHALLENGE)
        .unwrap()
        .compute()
        .unwrap();
    assert_eq!(
        &*proof.response,
        format!("{FINAL_PREFIX}dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=").as_bytes()
    );
    proof
        .verifier
        .verify(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
        .unwrap();
    let proof = self::client(ScramMechanism::Sha256)
        .on_server_first(CHALLENGE)
        .unwrap()
        .compute()
        .unwrap();
    assert_eq!(
        proof
            .verifier
            .verify(b"v=7rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="),
        Err(SecurityError::InvalidServerSignature)
    );
}

#[test]
fn sha512_matches_independent_python_hashlib_hmac_fixture() {
    // Same RFC inputs, independently evaluated using hashlib.pbkdf2_hmac,
    // hashlib.sha512 and hmac.digest (fixture recipe in README).
    let proof = client(ScramMechanism::Sha512)
        .on_server_first(CHALLENGE)
        .unwrap()
        .compute()
        .unwrap();
    assert_eq!(&*proof.response, format!("{FINAL_PREFIX}gMGXRcevScNtxZ6/8lQYpGtnsNAc3mGcmNomv+xnoOMw+3R2xNJdMNnzMlTN8PPC6wdp6dybEmDYXYTxwnYPJQ==").as_bytes());
    proof.verifier.verify(b"v=ZQnYEgWQMFmmsM8aQMF0nDDCy/AgCzkwk8CmMZYcMg0vSVlKDanekLtifDSeVGT4+5ZxXnJq199RVG2rR7N7Zw==").unwrap();
}

#[test]
fn malformed_challenges_are_rejected_before_derivation() {
    for challenge in [
        "r=rOprNGfwEbeRWgbNEkqO,s=c2FsdA==,i=4096", // no server contribution
        "r=othernonceisnotclient,s=c2FsdA==,i=4096",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA==,i=0",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA==,i=04096",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA==,i=4095",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA==,i=1000001",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA==,i=99999999999999",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA==,i=+4096",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA==,i=4096,r=duplicate",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA,i=4096",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=,i=4096",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdB==,i=4096", // noncanonical trailing bits
        "m=extension,r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA==,i=4096",
        "r=rOprNGfwEbeRWgbNEkqOserver,s=c2FsdA==,i=4096,",
    ] {
        assert!(
            client(ScramMechanism::Sha256)
                .on_server_first(challenge.as_bytes())
                .is_err(),
            "accepted {challenge}"
        );
    }
}

#[test]
fn username_escaping_and_ascii_profile_are_explicit() {
    let client = ScramClient::start_with_nonce_for_test(
        ScramMechanism::Sha512,
        "a,b=c",
        "password",
        SaslLimits::default(),
        NONCE,
    )
    .unwrap();
    assert_eq!(
        client.initial_response(),
        b"n,,n=a=2Cb=3Dc,r=rOprNGfwEbeRWgbNEkqO"
    );
    for (user, password) in [
        ("", "p"),
        ("user", ""),
        ("u\0ser", "p"),
        ("u", "p\0"),
        ("ümlaut", "p"),
        ("u", "\t"),
    ] {
        assert!(
            ScramClient::start_with_nonce_for_test(
                ScramMechanism::Sha256,
                user,
                password,
                SaslLimits::default(),
                NONCE
            )
            .is_err()
        );
    }
}

#[test]
fn malformed_server_final_and_rejection_cannot_authenticate() {
    for message in [
        b"".as_slice(),
        b"e=invalid-proof",
        b"v=AAA=,e=error",
        b"v=AAAA,v=AAAA",
        b"m=required,v=AAAA",
        b"v=unencoded",
        b"x=unknown",
    ] {
        let proof = client(ScramMechanism::Sha256)
            .on_server_first(CHALLENGE)
            .unwrap()
            .compute()
            .unwrap();
        assert!(proof.verifier.verify(message).is_err());
    }
}

#[test]
fn configured_salt_and_message_bounds_reject_without_growing() {
    let limits = SaslLimits {
        salt_bytes: 3,
        ..SaslLimits::default()
    };
    assert!(
        ScramClient::start_with_nonce_for_test(
            ScramMechanism::Sha256,
            "user",
            "pencil",
            limits,
            NONCE
        )
        .unwrap()
        .on_server_first(CHALLENGE)
        .is_err()
    );
    assert!(
        client(ScramMechanism::Sha256)
            .on_server_first(&vec![b'r'; 16 * 1024 + 1])
            .is_err()
    );
    let limits = SaslLimits {
        max_iterations: 4095,
        ..SaslLimits::default()
    };
    assert_eq!(
        limits.validate().unwrap_err(),
        SecurityError::InvalidConfig {
            field: "max_iterations"
        }
    );
}

#[test]
fn production_starts_fresh_nonce_for_every_exchange() {
    let first = ScramClient::start(
        ScramMechanism::Sha256,
        "user",
        "password",
        SaslLimits::default(),
    )
    .unwrap();
    let second = ScramClient::start(
        ScramMechanism::Sha256,
        "user",
        "password",
        SaslLimits::default(),
    )
    .unwrap();
    assert_ne!(first.initial_response(), second.initial_response());
}
