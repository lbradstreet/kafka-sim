use kr_kafka_protocol::errors::{self, DeliveryCertainty, ErrorClass};
use sha2::{Digest, Sha256};

#[test]
fn every_pinned_java_error_is_present_once_with_its_original_name_and_number() {
    let source = include_str!("fixtures/Errors.java");
    assert_eq!(
        Sha256::digest(source.as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>(),
        errors::ERROR_SOURCE_SHA256
    );
    let provenance: serde_json::Value =
        serde_json::from_str(include_str!("../../../schemas/PROVENANCE.lock")).unwrap();
    assert_eq!(
        provenance["upstream"]["commit"],
        errors::ERROR_SOURCE_REVISION
    );
    let mut upstream = Vec::new();
    for line in source.lines() {
        let Some((name, rest)) = line.trim_start().split_once('(') else {
            continue;
        };
        if name.is_empty()
            || !name
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            continue;
        }
        let Some((code, _)) = rest.split_once(',') else {
            continue;
        };
        let Ok(code) = code.parse::<i16>() else {
            continue;
        };
        upstream.push((code, name));
    }
    assert_eq!(
        upstream.len(),
        137,
        "review the complete inventory on every source bump"
    );
    assert_eq!(upstream.len(), errors::ALL_ERRORS.len());
    for ((code, name), entry) in upstream.into_iter().zip(errors::ALL_ERRORS) {
        assert_eq!((entry.code, entry.name), (code, name));
        assert_eq!(errors::lookup(code), Some(entry));
        assert_eq!(errors::classify(code), entry.class);
        assert_ne!(
            entry.class,
            ErrorClass::UnknownFatal,
            "unclassified pinned error {name}"
        );
    }
    assert!(
        errors::ALL_ERRORS
            .windows(2)
            .all(|pair| pair[0].code < pair[1].code)
    );
}

#[test]
fn produce_actions_match_every_row_of_the_design_response_table() {
    let groups: &[(ErrorClass, &[i16])] = &[
        (ErrorClass::Success, &[errors::NONE]),
        (
            ErrorClass::DuplicateSequence,
            &[errors::DUPLICATE_SEQUENCE_NUMBER],
        ),
        (
            ErrorClass::RefreshMetadata,
            &[
                errors::NOT_LEADER_OR_FOLLOWER,
                errors::LEADER_NOT_AVAILABLE,
                errors::FENCED_LEADER_EPOCH,
                errors::UNKNOWN_LEADER_EPOCH,
                errors::UNKNOWN_TOPIC_OR_PARTITION,
            ],
        ),
        (ErrorClass::RefreshTopicId, &[errors::UNKNOWN_TOPIC_ID]),
        (ErrorClass::TopicFatal, &[errors::INCONSISTENT_TOPIC_ID]),
        (
            ErrorClass::Retry,
            &[
                errors::REQUEST_TIMED_OUT,
                errors::KAFKA_STORAGE_ERROR,
                errors::NOT_ENOUGH_REPLICAS,
                errors::NOT_ENOUGH_REPLICAS_AFTER_APPEND,
            ],
        ),
        (
            ErrorClass::SequenceRecovery,
            &[
                errors::OUT_OF_ORDER_SEQUENCE_NUMBER,
                errors::UNKNOWN_PRODUCER_ID,
            ],
        ),
        (
            ErrorClass::DefinitiveNotWritten,
            &[
                errors::MESSAGE_TOO_LARGE,
                errors::RECORD_LIST_TOO_LARGE,
                errors::INVALID_RECORD,
                errors::CORRUPT_MESSAGE,
                errors::UNSUPPORTED_COMPRESSION_TYPE,
                errors::UNSUPPORTED_FOR_MESSAGE_FORMAT,
                errors::INVALID_REQUIRED_ACKS,
                errors::TOPIC_AUTHORIZATION_FAILED,
            ],
        ),
        (
            ErrorClass::ProducerFatal,
            &[
                errors::INVALID_PRODUCER_EPOCH,
                errors::PRODUCER_FENCED,
                errors::CLUSTER_AUTHORIZATION_FAILED,
                errors::INVALID_PRODUCER_ID_MAPPING,
            ],
        ),
    ];
    let mut checked = std::collections::BTreeSet::new();
    for &(class, codes) in groups {
        for &code in codes {
            assert!(
                checked.insert(code),
                "response table contains duplicate code {code}"
            );
            assert_eq!(errors::classify(code), class, "code={code}");
        }
    }
    for entry in errors::ALL_ERRORS {
        if !checked.contains(&entry.code) {
            assert_eq!(
                entry.class,
                ErrorClass::ProducerFatal,
                "non-Produce error {} must fail closed",
                entry.name
            );
        }
    }
}

#[test]
fn every_unknown_i16_code_fails_closed_instead_of_becoming_success() {
    let mut unknown = 0usize;
    for code in i16::MIN..=i16::MAX {
        if errors::lookup(code).is_none() {
            unknown += 1;
            assert_eq!(
                errors::classify(code),
                ErrorClass::UnknownFatal,
                "code={code}"
            );
            assert!(errors::classify(code).is_producer_fatal());
            assert_eq!(
                errors::terminal_certainty(true, false, Some(code)),
                DeliveryCertainty::Unknown
            );
        }
    }
    assert_eq!(unknown, 65_536 - errors::ALL_ERRORS.len());
}

#[test]
fn delivery_certainty_uses_acknowledgements_and_the_full_transmission_history() {
    for transmitted in [false, true] {
        assert_eq!(
            errors::terminal_certainty(transmitted, false, None),
            if transmitted {
                DeliveryCertainty::Unknown
            } else {
                DeliveryCertainty::NotWritten
            }
        );
        for entry in errors::ALL_ERRORS {
            assert_eq!(
                errors::terminal_certainty(transmitted, true, Some(entry.code)),
                DeliveryCertainty::Acked,
                "a later error cannot undo a parsed acknowledgement"
            );
            let expected = match entry.class {
                ErrorClass::Success | ErrorClass::DuplicateSequence => DeliveryCertainty::Acked,
                ErrorClass::DefinitiveNotWritten | ErrorClass::SequenceRecovery => {
                    DeliveryCertainty::NotWritten
                }
                _ => DeliveryCertainty::Unknown,
            };
            assert_eq!(
                errors::terminal_certainty(transmitted, false, Some(entry.code)),
                expected,
                "code={}",
                entry.code
            );
        }
    }
    // A successful socket write supplies no Kafka response evidence.
    assert_eq!(
        errors::terminal_certainty(true, false, None),
        DeliveryCertainty::Unknown
    );
    // Refresh-confirmed deletion cannot erase an ambiguous earlier Produce.
    assert_eq!(
        errors::terminal_certainty(true, false, Some(errors::UNKNOWN_TOPIC_ID)),
        DeliveryCertainty::Unknown
    );
    // Producer-wide fail-closed collateral work has no response of its own.
    assert_eq!(
        errors::terminal_certainty(false, false, None),
        DeliveryCertainty::NotWritten
    );
    assert_eq!(
        errors::terminal_certainty(true, false, None),
        DeliveryCertainty::Unknown
    );
    assert_eq!(
        errors::terminal_certainty(true, false, Some(errors::DUPLICATE_SEQUENCE_NUMBER)),
        DeliveryCertainty::Acked
    );
}

#[test]
fn routing_and_retry_flags_do_not_bypass_sequence_recovery() {
    for code in [
        errors::OUT_OF_ORDER_SEQUENCE_NUMBER,
        errors::UNKNOWN_PRODUCER_ID,
    ] {
        assert_eq!(errors::classify(code), ErrorClass::SequenceRecovery);
        assert!(!errors::classify(code).is_retriable());
        assert!(!errors::classify(code).is_producer_fatal());
    }
    for code in [errors::NOT_LEADER_OR_FOLLOWER, errors::UNKNOWN_TOPIC_ID] {
        assert!(errors::classify(code).is_retriable());
        assert!(errors::classify(code).requires_metadata_refresh());
    }
    assert!(errors::classify(errors::REQUEST_TIMED_OUT).is_retriable());
    assert!(!errors::classify(errors::REQUEST_TIMED_OUT).requires_metadata_refresh());
    assert!(errors::classify(errors::NONE).is_success());
    assert!(errors::classify(errors::DUPLICATE_SEQUENCE_NUMBER).is_success());
}
