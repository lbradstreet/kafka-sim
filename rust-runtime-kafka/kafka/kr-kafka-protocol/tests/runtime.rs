use std::sync::Arc;

use kr_kafka_protocol::plan::{EncodeLimits, Records, SharedBytes};
use kr_kafka_protocol::wire::{DecodeLimits, Error, Reader, Sequence, TaggedFields, Wire, Writer};

fn reader(bytes: &[u8], flexible: bool) -> Reader<'_> {
    Reader::new(bytes, 0, flexible, DecodeLimits::default()).unwrap()
}

fn writer(flexible: bool) -> Writer<'static> {
    Writer::new(0, flexible, EncodeLimits::default())
}

#[test]
fn primitives_match_big_endian_wire_bytes_and_borrow_input() {
    let expected = [
        0x81, 0x01, 0x23, 0x89, 0xab, 0xcd, 0xef, 1, 0, 2, b'h', b'i',
    ];
    let mut w = writer(false);
    w.write_i8(-127).unwrap();
    w.write_i16(0x123).unwrap();
    w.write_i32(0x89abcdefu32 as i32).unwrap();
    w.write_bool(true).unwrap();
    w.write_string("hi").unwrap();
    assert_eq!(w.finish().unwrap().to_vec().unwrap(), expected);
    let mut r = reader(&expected, false);
    assert_eq!(r.read_i8().unwrap(), -127);
    assert_eq!(r.read_i16().unwrap(), 0x123);
    assert_eq!(r.read_i32().unwrap(), 0x89abcdefu32 as i32);
    assert!(r.read_bool().unwrap());
    let value = r.read_string().unwrap();
    assert_eq!(value, "hi");
    assert_eq!(value.as_ptr(), expected[10..].as_ptr());
    r.finish().unwrap();
}

#[test]
fn compact_and_classic_null_empty_and_utf8_are_distinct() {
    for flexible in [false, true] {
        let mut w = writer(flexible);
        w.write_nullable_string(None).unwrap();
        w.write_nullable_string(Some("")).unwrap();
        w.write_string("λ").unwrap();
        w.write_nullable_bytes(None).unwrap();
        w.write_bytes(&[]).unwrap();
        let bytes = w.finish().unwrap().to_vec().unwrap();
        let mut r = reader(&bytes, flexible);
        assert_eq!(r.read_nullable_string().unwrap(), None);
        assert_eq!(r.read_nullable_string().unwrap(), Some(""));
        assert_eq!(r.read_string().unwrap(), "λ");
        assert_eq!(r.read_nullable_bytes().unwrap(), None);
        assert_eq!(r.read_bytes().unwrap(), &[]);
        r.finish().unwrap();
    }
    assert_eq!(
        reader(&[0xff, 0xff], false).read_string(),
        Err(Error::NullNotAllowed)
    );
    assert_eq!(reader(&[0], true).read_bytes(), Err(Error::NullNotAllowed));
    assert_eq!(
        reader(&[0xff, 0xfe], false).read_string(),
        Err(Error::InvalidLength { value: -2 })
    );
    assert_eq!(
        reader(&[0, 1, 0xff], false).read_string(),
        Err(Error::InvalidUtf8)
    );
    assert_eq!(
        reader(&[2], false).read_bool(),
        Err(Error::InvalidBoolean { value: 2 })
    );
    assert!(matches!(
        reader(&[2, b'a'], true).read_i32(),
        Err(Error::UnexpectedEof { .. })
    ));
}

#[test]
fn unsigned_varints_reject_noncanonical_and_overflowing_encodings() {
    for value in [0, 1, 127, 128, 16383, 16384, u32::MAX] {
        let mut w = writer(true);
        w.write_uvarint(value).unwrap();
        let bytes = w.finish().unwrap().to_vec().unwrap();
        let mut r = reader(&bytes, true);
        assert_eq!(r.read_uvarint().unwrap(), value);
        r.finish().unwrap();
    }
    for bytes in [
        &[0x80, 0][..],
        &[0x81, 0],
        &[0xff, 0xff, 0xff, 0xff, 0x10],
        &[0x80; 5],
    ] {
        assert_eq!(
            reader(bytes, true).read_uvarint(),
            Err(Error::InvalidVarint),
            "bytes={bytes:?}"
        );
    }
    assert!(reader(&[0x80], true).read_uvarint().is_err());
}

#[test]
fn compact_lengths_preserve_kafkas_signed_type_bounds() {
    let maximum = "x".repeat(i16::MAX as usize);
    let oversized = "x".repeat(i16::MAX as usize + 1);
    for flexible in [false, true] {
        let mut w = writer(flexible);
        w.write_string(&maximum).unwrap();
        let bytes = w.finish().unwrap().to_vec().unwrap();
        assert_eq!(reader(&bytes, flexible).read_string().unwrap(), maximum);
        assert_eq!(
            writer(flexible).write_string(&oversized),
            Err(Error::LengthOverflow)
        );
    }
    assert_eq!(
        reader(&[0x81, 0x80, 2], true).read_string(),
        Err(Error::InvalidLength { value: 32768 })
    );
    for bytes in [
        &[0x81, 0x80, 0x80, 0x80, 8][..],
        &[0xff, 0xff, 0xff, 0xff, 0x0f],
    ] {
        assert!(matches!(
            reader(bytes, true).read_nullable_bytes(),
            Err(Error::InvalidLength { .. })
        ));
        assert!(matches!(
            Sequence::<i8>::read_nullable(&mut reader(bytes, true)),
            Err(Error::InvalidLength { .. })
        ));
    }
    // The exact maximum is representable; the absent payload is the error.
    assert_eq!(
        reader(&[0x80, 0x80, 0x80, 0x80, 8], true).read_bytes(),
        Err(Error::UnexpectedEof {
            needed: i32::MAX as usize,
            remaining: 0
        })
    );
}

#[test]
fn decoded_arrays_validate_all_elements_and_can_reencode_in_another_mode() {
    let bytes = [3, 2, b'a', 3, b'b', b'c'];
    let mut r = reader(&bytes, true);
    let values = Sequence::<&str>::read(&mut r).unwrap();
    r.finish().unwrap();
    assert_eq!(values.len(), 2);
    let collected = values.iter().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(collected, ["a", "bc"]);
    assert_eq!(collected[0].as_ptr(), bytes[2..].as_ptr());
    let mut w = Writer::new(0, false, EncodeLimits::default());
    values.write(&mut w).unwrap();
    assert_eq!(
        w.finish().unwrap().to_vec().unwrap(),
        [0, 0, 0, 2, 0, 1, b'a', 0, 2, b'b', b'c']
    );
    assert!(Sequence::<&str>::read(&mut reader(&[3, 2, b'a', 2, 0xff], true)).is_err());
    assert!(Sequence::<i32>::read(&mut reader(&[0x7f, 0xff, 0xff, 0xff], false)).is_err());
}

#[test]
fn aggregate_array_limits_include_nested_arrays_and_known_tag_payloads() {
    // Outer count 2 + inner counts 1 and 1 needs four element credits.
    let bytes = [3, 2, 1, 2, 2];
    let limits = DecodeLimits {
        max_array_elements: 3,
        ..DecodeLimits::default()
    };
    let mut r = Reader::new(&bytes, 0, true, limits).unwrap();
    assert!(matches!(
        Sequence::<Sequence<i8>>::read(&mut r),
        Err(Error::ResourceExhausted {
            resource: "decoded array elements",
            limit: 3
        })
    ));

    let bytes = [2, 0, 2, 2, 10, 1, 2, 2, 11];
    let limits = DecodeLimits {
        max_array_elements: 1,
        ..DecodeLimits::default()
    };
    let mut r = Reader::new(&bytes, 0, true, limits).unwrap();
    let tags = TaggedFields::read(&mut r).unwrap();
    let mut tags = tags.iter();
    let first = tags.next().unwrap();
    r.with_subreader(first.payload, Sequence::<i8>::read)
        .unwrap();
    let second = tags.next().unwrap();
    assert!(matches!(
        r.with_subreader(second.payload, Sequence::<i8>::read),
        Err(Error::ResourceExhausted {
            resource: "decoded array elements",
            limit: 1
        })
    ));
}

#[test]
fn known_tag_plans_charge_aggregate_encode_element_and_tag_budgets() {
    let values = [1i8, 2];
    let sequence = Sequence::new(&values);
    let limits = EncodeLimits {
        max_array_elements: 3,
        ..EncodeLimits::default()
    };
    let mut w = Writer::new(0, true, limits);
    let mut first = w.child();
    sequence.write(&mut first).unwrap();
    let mut second = w.child();
    sequence.write(&mut second).unwrap();
    let known = [(0, first.finish().unwrap()), (1, second.finish().unwrap())];
    assert!(matches!(
        w.write_tagged_fields(&TaggedFields::default(), &known),
        Err(Error::ResourceExhausted {
            resource: "encoded array elements",
            limit: 3
        })
    ));
    assert!(w.is_empty());

    let mut inner = writer(true);
    let unknown = TaggedFields::read(&mut reader(&[1, 3, 0], true)).unwrap();
    inner.write_tagged_fields(&unknown, &[]).unwrap();
    let limits = EncodeLimits {
        max_tags: 1,
        ..EncodeLimits::default()
    };
    let mut w = Writer::new(0, true, limits);
    assert!(matches!(
        w.write_tagged_fields(&TaggedFields::default(), &[(0, inner.finish().unwrap())]),
        Err(Error::ResourceExhausted {
            resource: "encoded tags",
            limit: 1
        })
    ));
}

#[test]
fn known_tag_payload_nesting_is_counted_at_the_parent_depth() {
    for max_depth in [2, 3] {
        let limits = EncodeLimits {
            max_depth,
            ..EncodeLimits::default()
        };
        let mut w = Writer::new(0, true, limits);
        let result = w.with_struct(|w| {
            let mut payload = writer(true);
            payload.with_struct(|w| w.write_i8(7))?;
            w.write_tagged_fields(&TaggedFields::default(), &[(0, payload.finish()?)])
        });
        assert_eq!(result.is_ok(), max_depth == 3);

        let bytes = [1, 0, 1, 7];
        let limits = DecodeLimits {
            max_depth,
            ..DecodeLimits::default()
        };
        let mut r = Reader::new(&bytes, 0, true, limits).unwrap();
        let result = r.with_struct(|r| {
            let tags = TaggedFields::read(r)?;
            r.with_subreader(tags.iter().next().unwrap().payload, |r| {
                r.with_struct(|r| r.read_i8())
            })
        });
        assert_eq!(result.is_ok(), max_depth == 3);
    }
}

#[test]
fn known_tag_child_arenas_share_a_cumulative_budget_before_final_merge() {
    let limits = EncodeLimits {
        max_bytes: 100,
        max_metadata_bytes: 100,
        ..EncodeLimits::default()
    };
    let w = Writer::new(0, true, limits);
    let values = [7i64; 8];
    let sequence = Sequence::new(&values);
    let mut first = w.child_after(&[]).unwrap();
    sequence.write(&mut first).unwrap();
    let known = [(0, first.finish().unwrap())];
    assert_eq!(known[0].1.metadata_len(), 65);
    let mut second = w.child_after(&known).unwrap();
    assert_eq!(second.limits().max_metadata_bytes, 35);
    assert!(matches!(
        sequence.write(&mut second),
        Err(Error::ResourceExhausted { .. })
    ));
    assert!(known[0].1.metadata_len() + second.len() <= 100);
}

#[test]
fn child_budgets_allow_exact_small_tag_envelopes_and_metadata_coalescing() {
    let limits = EncodeLimits {
        max_bytes: 4,
        max_metadata_bytes: 4,
        max_segments: 1,
        ..EncodeLimits::default()
    };
    let mut w = Writer::new(0, true, limits);
    let mut child = w.child_after(&[]).unwrap();
    child.write_i8(7).unwrap();
    w.write_tagged_fields(&TaggedFields::default(), &[(0, child.finish().unwrap())])
        .unwrap();
    let plan = w.finish().unwrap();
    assert_eq!(plan.to_vec().unwrap(), [1, 0, 1, 7]);
    assert_eq!(plan.segment_count(), 1);

    let limits = EncodeLimits {
        max_segments: 3,
        ..EncodeLimits::default()
    };
    let w = Writer::new(0, true, limits);
    let mut first = w.child_after(&[]).unwrap();
    first.write_records(&Records::Borrowed(b"a")).unwrap();
    let known = [
        (0, first.finish().unwrap()),
        (1, writer(true).finish().unwrap()),
    ];
    let mut next = w.child_after(&known).unwrap();
    next.write_i8(7).unwrap();
    assert_eq!(next.limits().max_segments, 1);
}

#[test]
fn byte_and_tag_limits_accept_exact_bounds_and_reject_the_next_unit() {
    assert!(
        Reader::new(
            &[0; 5],
            0,
            false,
            DecodeLimits {
                max_bytes: 4,
                ..DecodeLimits::default()
            }
        )
        .is_err()
    );
    let tags = [2, 1, 0, 2, 0];
    for max_tags in [1, 2] {
        let mut r = Reader::new(
            &tags,
            0,
            true,
            DecodeLimits {
                max_tags,
                ..DecodeLimits::default()
            },
        )
        .unwrap();
        assert_eq!(TaggedFields::read(&mut r).is_ok(), max_tags == 2);
    }
    let limits = EncodeLimits {
        max_bytes: 5,
        ..EncodeLimits::default()
    };
    let mut w = Writer::new(0, false, limits);
    w.write_records(&Records::Borrowed(&[42])).unwrap();
    assert_eq!(w.len(), 5);
    assert!(matches!(
        w.write_i8(0),
        Err(Error::ResourceExhausted {
            resource: "encoded bytes",
            limit: 5
        })
    ));
}

#[test]
fn tag_blocks_require_sorted_unique_ids_and_exact_known_payload_consumption() {
    for bytes in [
        &[2, 1, 0, 1, 0][..],
        &[2, 2, 0, 1, 0],
        &[1, 0, 2, 1],
        &[1, 0x80, 0, 0],
    ] {
        assert!(
            TaggedFields::read(&mut reader(bytes, true)).is_err(),
            "bytes={bytes:?}"
        );
    }
    let bytes = [3, 0, 1, 7, 2, 2, 8, 9, 4, 0];
    let mut r = reader(&bytes, true);
    let tags = TaggedFields::read(&mut r).unwrap();
    r.finish().unwrap();
    assert_eq!(tags.iter().map(|tag| tag.id).collect::<Vec<_>>(), [0, 2, 4]);
    assert_eq!(
        r.with_subreader(tags.iter().nth(1).unwrap().payload, |r| r.read_i8()),
        Err(Error::TrailingBytes { remaining: 1 })
    );
    let unknown = tags.excluding(&[2]);
    let mut payload = writer(true);
    payload.write_i16(0x0102).unwrap();
    let mut w = Writer::new(0, true, EncodeLimits::default());
    w.write_tagged_fields(&unknown, &[(2, payload.finish().unwrap())])
        .unwrap();
    assert_eq!(
        w.finish().unwrap().to_vec().unwrap(),
        [3, 0, 1, 7, 2, 2, 1, 2, 4, 0]
    );

    let mut w = Writer::new(0, true, EncodeLimits::default());
    assert!(
        w.write_tagged_fields(&tags, &[(2, writer(true).finish().unwrap())])
            .is_err()
    );
    assert!(w.is_empty());
}

#[test]
fn nesting_and_field_encoding_overrides_restore_state_on_failure() {
    let mut r = Reader::new(
        &[],
        0,
        true,
        DecodeLimits {
            max_depth: 1,
            ..DecodeLimits::default()
        },
    )
    .unwrap();
    assert!(r.with_struct(|r| r.with_struct(|_| Ok(()))).is_err());
    r.with_struct(|_| Ok(())).unwrap();
    assert!(r.with_flexible(false, |r| r.read_i8()).is_err());
    assert!(r.flexible());
    let mut w = Writer::new(
        0,
        true,
        EncodeLimits {
            max_depth: 1,
            ..EncodeLimits::default()
        },
    );
    assert!(w.with_struct(|w| w.with_struct(|_| Ok(()))).is_err());
    w.with_struct(|_| Ok(())).unwrap();
    assert!(
        w.with_flexible(false, |_| Err::<(), _>(Error::InvalidValue("test")))
            .is_err()
    );
    assert!(w.flexible());
}

#[test]
fn metadata_limits_and_external_records_keep_allocation_identity() {
    let owner = SharedBytes::new(Arc::from(&b"--record-payload--"[..]));
    let chunk = owner.slice(2..16).unwrap();
    assert_eq!(chunk.as_slice(), b"record-payload");
    assert!(chunk.shares_allocation(&owner));
    assert!(owner.slice(core::ops::Range { start: 5, end: 2 }).is_err());
    assert!(owner.slice(0..100).is_err());
    let chunks = [chunk];
    let limits = EncodeLimits {
        max_metadata_bytes: 9,
        ..EncodeLimits::default()
    };
    let mut w = Writer::new(0, false, limits);
    w.write_i32(0).unwrap();
    w.write_records(&Records::Chunks(&chunks)).unwrap();
    w.write_i8(7).unwrap();
    let plan = w.finish_frame().unwrap();
    assert_eq!(plan.metadata_len(), 9);
    assert_eq!(plan.segment_count(), 3);
    assert!(
        plan.shared_segments()
            .next()
            .unwrap()
            .shares_allocation(&owner)
    );
    let expected = [
        0, 0, 0, 19, 0, 0, 0, 14, b'r', b'e', b'c', b'o', b'r', b'd', b'-', b'p', b'a', b'y', b'l',
        b'o', b'a', b'd', 7,
    ];
    assert_eq!(plan.to_vec().unwrap(), expected);
    let mut w = Writer::new(0, false, limits);
    assert!(w.write_bytes(&[0; 6]).is_err());
    let mut w = Writer::new(
        0,
        false,
        EncodeLimits {
            max_segments: 1,
            ..EncodeLimits::default()
        },
    );
    assert!(w.write_records(&Records::Chunks(&chunks)).is_err());
}

#[test]
fn owned_shared_plans_outlive_request_descriptors_without_copying() {
    let owner = SharedBytes::new(Arc::from(&b"record"[..]));
    let plan = {
        let descriptors = vec![owner.clone()];
        let topic = String::from("topic");
        let mut w = Writer::new(0, false, EncodeLimits::default());
        w.write_string(&topic).unwrap();
        w.write_records(&Records::Chunks(&descriptors)).unwrap();
        let plan = w.finish().unwrap();
        let metadata_pointer = plan.segments().next().unwrap().as_ptr();
        let owned = plan.try_into_owned().unwrap();
        assert_eq!(owned.segments().next().unwrap().as_ptr(), metadata_pointer);
        owned
    };
    assert!(
        plan.shared_segments()
            .next()
            .unwrap()
            .shares_allocation(&owner)
    );
    assert_eq!(
        plan.to_vec().unwrap(),
        [
            0, 5, b't', b'o', b'p', b'i', b'c', 0, 0, 0, 6, b'r', b'e', b'c', b'o', b'r', b'd'
        ]
    );
    let borrowed = Vec::from(&b"record"[..]);
    let mut w = Writer::new(0, false, EncodeLimits::default());
    w.write_records(&Records::Borrowed(&borrowed)).unwrap();
    let plan = w.finish().unwrap();
    let original = plan.to_vec().unwrap();
    let plan = plan.try_into_owned().unwrap_err();
    assert_eq!(plan.to_vec().unwrap(), original);
    assert_eq!(plan.segments().nth(1).unwrap().as_ptr(), borrowed.as_ptr());
}

#[test]
fn stages_bound_fragments_and_advance_only_by_confirmed_prefixes() {
    let mut w = writer(false);
    w.write_i16(0x0102).unwrap();
    w.write_records(&Records::Borrowed(b"abcdef")).unwrap();
    w.write_i8(9).unwrap();
    let plan = w.finish().unwrap();
    let expected = plan.to_vec().unwrap();
    let mut cursor = plan.cursor();
    let other = plan.clone();
    assert!(
        cursor
            .confirm(other.cursor().stage(5, 2).unwrap(), 1)
            .is_err()
    );
    assert_eq!(cursor.confirmed(), 0);
    assert!(cursor.stage(0, 2).is_err());
    assert!(cursor.stage(2, 0).is_err());
    let dropped = cursor.stage(100, 1).unwrap();
    assert_eq!(dropped.len(), 6);
    drop(dropped);
    assert_eq!(cursor.confirmed(), 0);
    let stale = cursor.stage(5, 2).unwrap();
    cursor.confirm(cursor.stage(5, 2).unwrap(), 0).unwrap();
    assert_eq!(cursor.confirmed(), 0);
    let first = cursor.stage(5, 2).unwrap();
    cursor.confirm(first, 3).unwrap();
    assert!(cursor.confirm(stale, 1).is_err());
    assert_eq!(cursor.confirmed(), 3);
    let invalid = cursor.stage(4, 2).unwrap();
    assert!(cursor.confirm(invalid, 5).is_err());
    assert_eq!(cursor.confirmed(), 3);
    let mut observed = expected[..3].to_vec();
    while cursor.remaining() != 0 {
        let stage = cursor.stage(5, 2).unwrap();
        assert!(stage.chunks().len() <= 2);
        assert!(stage.len() <= 5);
        let count = stage.len().min(2);
        observed.extend(
            stage
                .chunks()
                .iter()
                .flat_map(|chunk| chunk.iter().copied())
                .take(count),
        );
        cursor.confirm(stage, count).unwrap();
    }
    assert_eq!(observed, expected);
    assert!(cursor.stage(0, 0).unwrap().is_empty());
}

#[test]
fn frame_finalization_requires_placeholder_and_exact_signed_length() {
    assert!(writer(false).finish_frame().is_err());
    let mut w = writer(false);
    w.write_i32(1).unwrap();
    assert!(w.finish_frame().is_err());
    let mut w = writer(false);
    w.write_i32(0).unwrap();
    assert_eq!(w.finish_frame().unwrap().to_vec().unwrap(), [0; 4]);
}

#[derive(Clone, Debug)]
struct Entry<'a> {
    name: &'a str,
    values: Sequence<'a, i32>,
}
impl<'a> Wire<'a> for Entry<'a> {
    fn read(r: &mut Reader<'a>) -> Result<Self, Error> {
        r.with_struct(|r| {
            Ok(Self {
                name: r.read_string()?,
                values: Sequence::read(r)?,
            })
        })
    }
    fn write(&self, w: &mut Writer<'a>) -> Result<(), Error> {
        w.with_struct(|w| {
            w.write_string(self.name)?;
            self.values.write(w)
        })
    }
    fn is_default(&self) -> bool {
        self.name.is_empty() && self.values.is_empty()
    }
}

#[test]
fn bounded_parser_campaign_has_canonical_roundtrips_and_no_panics() {
    // Exhaust a small deterministic byte domain, then mutate a valid nested
    // fixture at every position. Every accepted message must canonicalize to
    // exactly the input, including array and string prefixes.
    let limits = DecodeLimits {
        max_bytes: 128,
        max_array_elements: 16,
        max_depth: 8,
        max_tags: 8,
    };
    let values = [1, -2, 300];
    let entries = [Entry {
        name: "topic",
        values: Sequence::new(&values),
    }];
    for flexible in [false, true] {
        let mut w = Writer::new(0, flexible, EncodeLimits::default());
        Sequence::new(&entries).write(&mut w).unwrap();
        let valid = w.finish().unwrap().to_vec().unwrap();
        let mut cases = Vec::new();
        for a in 0..32u8 {
            for b in 0..32u8 {
                cases.push(vec![a, b]);
            }
        }
        for len in 0..=valid.len() {
            cases.push(valid[..len].to_vec());
        }
        for index in 0..valid.len() {
            for value in [0, 1, 0x7f, 0x80, 0xfe, 0xff] {
                let mut changed = valid.clone();
                changed[index] = value;
                cases.push(changed);
            }
        }
        let mut accepted = 0;
        for (case, bytes) in cases.iter().enumerate() {
            let mut r = Reader::new(bytes, 0, flexible, limits).unwrap();
            if let Ok(decoded) = Sequence::<Entry>::read(&mut r) {
                if r.finish().is_err() {
                    continue;
                }
                accepted += 1;
                let mut w = Writer::new(0, flexible, EncodeLimits::default());
                decoded.write(&mut w).unwrap();
                assert_eq!(
                    &w.finish().unwrap().to_vec().unwrap(),
                    bytes,
                    "flexible={flexible} case={case}"
                );
            }
        }
        assert!(accepted > 0, "campaign must accept its valid fixture");
    }
}

#[test]
fn opaque_records_use_compact_lengths_instead_of_record_zigzag_varints() {
    let mut w = writer(true);
    w.write_nullable_records(None).unwrap();
    w.write_nullable_records(Some(&Records::Borrowed(&[])))
        .unwrap();
    w.write_records(&Records::Borrowed(&[0xff, 0])).unwrap();
    let bytes = w.finish().unwrap().to_vec().unwrap();
    // Two bytes have compact length 3. Record-format zigzag length would be 4.
    assert_eq!(bytes, [0, 1, 3, 0xff, 0]);
    let mut r = reader(&bytes, true);
    assert!(r.read_nullable_records().unwrap().is_none());
    assert!(r.read_nullable_records().unwrap().unwrap().is_empty());
    let Records::Borrowed(payload) = r.read_records().unwrap() else {
        panic!("expected borrowed records")
    };
    assert_eq!(payload, [0xff, 0]);
    r.finish().unwrap();
}

#[test]
fn opaque_record_header_is_owned_metadata_and_payloads_remain_shared() {
    let chunks = [
        SharedBytes::from(vec![0xff; 512]),
        SharedBytes::from(vec![0; 513]),
    ];
    for flexible in [false, true] {
        let header = [0xa5; 61];
        let mut expected = header.to_vec();
        for chunk in &chunks {
            expected.extend_from_slice(chunk.as_slice());
        }
        let mut ordinary = Writer::new(0, flexible, EncodeLimits::default());
        ordinary
            .write_records(&Records::Borrowed(&expected))
            .unwrap();
        let ordinary = ordinary.finish().unwrap().to_vec().unwrap();
        let plan = {
            let local_header = header;
            let mut writer = Writer::new(0, flexible, EncodeLimits::default());
            writer
                .write_records(&Records::HeaderAndChunks {
                    header: &local_header,
                    chunks: &chunks,
                })
                .unwrap();
            writer.finish().unwrap().try_into_owned().unwrap()
        };
        assert_eq!(plan.to_vec().unwrap(), ordinary);
        assert_eq!(plan.segment_count(), 3);
        assert_eq!(plan.metadata_len(), ordinary.len() - 1025);
        assert_eq!(plan.metadata_capacity(), plan.metadata_len());
        for (actual, expected) in plan.shared_segments().zip(&chunks) {
            assert!(actual.shares_allocation(expected));
        }
        let decoded = plan.to_vec().unwrap();
        let Records::Borrowed(actual) = reader(&decoded, flexible).read_records().unwrap() else {
            panic!("borrowed decode");
        };
        assert_eq!(actual, expected);
        let mut writer = Writer::new(
            0,
            flexible,
            EncodeLimits {
                max_metadata_bytes: plan.metadata_len() - 1,
                ..EncodeLimits::default()
            },
        );
        assert!(
            writer
                .write_records(&Records::HeaderAndChunks {
                    header: &header,
                    chunks: &chunks
                })
                .is_err()
        );
    }
    assert!(
        Records::HeaderAndChunks {
            header: &[],
            chunks: &[]
        }
        .is_empty()
    );
    assert!(
        !Records::HeaderAndChunks {
            header: &[0],
            chunks: &[]
        }
        .is_empty()
    );
}

#[test]
fn metadata_growth_clamps_retained_capacity_at_the_exact_admitted_limit() {
    for limit in [1, 3, 7, 61, 100, 513] {
        let mut writer = Writer::new(
            0,
            false,
            EncodeLimits {
                max_metadata_bytes: limit,
                ..EncodeLimits::default()
            },
        );
        for _ in 0..limit {
            writer.write_u8(7).unwrap();
        }
        assert!(writer.write_u8(8).is_err());
        let plan = writer.finish().unwrap();
        assert_eq!(plan.metadata_len(), limit);
        assert_eq!(plan.metadata_capacity(), limit);
        assert_eq!(plan.to_vec().unwrap(), vec![7; limit]);
    }
}
