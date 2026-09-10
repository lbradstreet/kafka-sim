use kr_runtime::rng::RandomStream;
use kr_runtime::{
    CompletionCertainty, DeterminismCheckpoint, RandomHandle, RuntimeConfig, SimRuntime,
};
use kr_runtime_io::datagram::{
    DatagramBindRequest, DatagramDirection, DatagramError, DatagramProviderSubmit,
    DatagramSocketSubmit, DatagramTruncation, RecvFromRequest, ScriptedDatagramFault,
    SendToRequest, SimDatagramAfterEnqueueCertainty, SimDatagramConfig, SimDatagramFault,
    SimDatagramNetwork, SimDatagramSocket,
};
use kr_runtime_io::network::{NetworkAddress, NodeId};
use std::collections::{BTreeMap, VecDeque};

const SEEDS: u64 = 12;
const BATCHES: usize = 4;
const SENDS_PER_BATCH: usize = 10;
const INGRESS_DATAGRAMS: usize = 3;
const INGRESS_BYTES: usize = 12;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct Coverage {
    reliable: u64,
    loss: u64,
    duplicate: u64,
    corruption: u64,
    network_truncation: u64,
    fail_before: u64,
    error_after: u64,
    partition: u64,
    forced_queue_full: u64,
    empty_datagrams: u64,
    receive_truncation: u64,
    ingress_drops: u64,
    reordered_pairs: u64,
}

impl Coverage {
    fn add(&mut self, other: Self) {
        self.reliable += other.reliable;
        self.loss += other.loss;
        self.duplicate += other.duplicate;
        self.corruption += other.corruption;
        self.network_truncation += other.network_truncation;
        self.fail_before += other.fail_before;
        self.error_after += other.error_after;
        self.partition += other.partition;
        self.forced_queue_full += other.forced_queue_full;
        self.empty_datagrams += other.empty_datagrams;
        self.receive_truncation += other.receive_truncation;
        self.ingress_drops += other.ingress_drops;
        self.reordered_pairs += other.reordered_pairs;
    }
}

#[derive(Debug, Eq, PartialEq)]
struct Artifact {
    trace: Vec<String>,
    checkpoint: DeterminismCheckpoint,
    coverage: Coverage,
}

#[derive(Clone, Debug)]
struct ScheduledPacket {
    delay: u64,
    admission: usize,
    copy: usize,
    source: NetworkAddress,
    destination: NetworkAddress,
    payload: Vec<u8>,
}

#[derive(Clone, Debug)]
struct ExpectedPacket {
    admission: usize,
    copy: usize,
    source: NetworkAddress,
    payload: Vec<u8>,
}

fn address(index: usize) -> NetworkAddress {
    NetworkAddress {
        node: NodeId(index as u64 + 1),
        port: 20_000 + index as u16,
    }
}

fn below(random: &RandomHandle, upper: u64) -> u64 {
    random
        .random_below(upper)
        .expect("model random bound is nonzero")
}

fn bind(
    runtime: &mut SimRuntime,
    network: &SimDatagramNetwork,
    address: NetworkAddress,
) -> SimDatagramSocket {
    runtime
        .block_on(network.submit_bind(DatagramBindRequest { address }))
        .expect("runtime drives model bind")
        .expect("model bind succeeds")
}

fn script(
    network: &SimDatagramNetwork,
    direction: DatagramDirection,
    ordinal: u64,
    tag: u64,
    action: SimDatagramFault,
) {
    network
        .push_fault(ScriptedDatagramFault {
            tag,
            direction,
            send_ordinal: ordinal,
            action,
        })
        .expect("model fault script is valid and bounded");
}

fn corrupt(payload: &mut [u8], offset: usize, xor: u8) {
    if !payload.is_empty() && xor != 0 {
        let index = offset % payload.len();
        payload[index] ^= xor;
    }
}

fn receive_one(
    runtime: &mut SimRuntime,
    socket: &SimDatagramSocket,
    expected: ExpectedPacket,
    max_bytes: usize,
    coverage: &mut Coverage,
    trace: &mut Vec<String>,
) {
    let prefix = vec![0xa5];
    let result = runtime
        .block_on(socket.submit_try_recv_from(RecvFromRequest {
            buffer: prefix.clone(),
            max_bytes,
        }))
        .expect("runtime drives modeled nonblocking receive")
        .expect("oracle has a packet");
    let copied = max_bytes.min(expected.payload.len());
    let expected_buffer = [prefix, expected.payload[..copied].to_vec()].concat();
    assert_eq!(result.buffer, expected_buffer);
    assert_eq!(result.bytes_received, copied);
    assert_eq!(result.datagram_len, expected.payload.len());
    assert_eq!(result.source, expected.source);
    let expected_truncation = if copied == expected.payload.len() {
        DatagramTruncation::Complete
    } else {
        coverage.receive_truncation += 1;
        DatagramTruncation::Truncated
    };
    assert_eq!(result.truncation, expected_truncation);
    trace.push(format!(
        "r:{}:{}:{}:{}:{}",
        expected.admission,
        expected.copy,
        expected.source.node.0,
        expected.payload.len(),
        copied
    ));
}

fn run_seed(seed: u64) -> Artifact {
    let mut runtime = SimRuntime::new(RuntimeConfig {
        seed,
        ..RuntimeConfig::default()
    });
    let random = runtime.random_source(RandomStream::Workload);
    let network = SimDatagramNetwork::new(
        runtime.handle(),
        SimDatagramConfig {
            max_queued_datagrams_per_socket: INGRESS_DATAGRAMS,
            max_queued_bytes_per_socket: INGRESS_BYTES,
            ..SimDatagramConfig::default()
        },
    )
    .expect("valid model network");
    let sockets = [
        bind(&mut runtime, &network, address(0)),
        bind(&mut runtime, &network, address(1)),
        bind(&mut runtime, &network, address(2)),
    ];
    let mut ordinals = BTreeMap::<DatagramDirection, u64>::new();
    let mut inboxes = BTreeMap::<NetworkAddress, VecDeque<ExpectedPacket>>::new();
    for index in 0..sockets.len() {
        inboxes.insert(address(index), VecDeque::new());
    }
    let mut trace = Vec::with_capacity(BATCHES * SENDS_PER_BATCH * 2);
    let mut coverage = Coverage::default();
    let mut admission = 0usize;
    let mut expected_delivery_events = 0u64;

    for batch in 0..BATCHES {
        let mut scheduled = Vec::<ScheduledPacket>::new();
        for step in 0..SENDS_PER_BATCH {
            admission += 1;
            let source_index = below(&random, sockets.len() as u64) as usize;
            let offset = 1 + below(&random, (sockets.len() - 1) as u64) as usize;
            let destination_index = (source_index + offset) % sockets.len();
            let source = address(source_index);
            let destination = address(destination_index);
            let direction = DatagramDirection {
                source,
                destination,
            };
            let ordinal = ordinals.entry(direction).or_insert(0);
            *ordinal += 1;
            let ordinal = *ordinal;
            let length = if step == SENDS_PER_BATCH - 1 {
                0
            } else {
                below(&random, 8) as usize
            };
            let payload = (0..length)
                .map(|_| random.random_u64().expect("model runtime active") as u8)
                .collect::<Vec<_>>();
            coverage.empty_datagrams += u64::from(payload.is_empty());
            let request_buffer = payload.clone();
            let original_pointer = request_buffer.as_ptr();
            let mut transformed = payload.clone();
            let delay = ((SENDS_PER_BATCH - step) % 4) as u64;
            let family = step % 10;
            let tag = (batch * SENDS_PER_BATCH + step) as u64 + 1;
            let mut copies = 1usize;
            let mut dropped = false;
            let mut expected_error = None;

            match family {
                0 => coverage.reliable += 1,
                1 => {
                    script(&network, direction, ordinal, tag, SimDatagramFault::Drop);
                    dropped = true;
                    coverage.loss += 1;
                }
                2 => {
                    script(
                        &network,
                        direction,
                        ordinal,
                        tag,
                        SimDatagramFault::Duplicate {
                            additional_copies: 1,
                        },
                    );
                    copies = 2;
                    coverage.duplicate += 1;
                }
                3 => {
                    script(
                        &network,
                        direction,
                        ordinal,
                        tag,
                        SimDatagramFault::Corrupt {
                            offset: 5,
                            xor: 0x5a,
                        },
                    );
                    corrupt(&mut transformed, 5, 0x5a);
                    let truncated_len = transformed.len().saturating_sub(1);
                    script(
                        &network,
                        direction,
                        ordinal,
                        tag + 1_000,
                        SimDatagramFault::Truncate { len: truncated_len },
                    );
                    transformed.truncate(truncated_len);
                    coverage.corruption += 1;
                    coverage.network_truncation += 1;
                }
                4 => {
                    let truncated_len = transformed.len().saturating_sub(1);
                    script(
                        &network,
                        direction,
                        ordinal,
                        tag,
                        SimDatagramFault::Truncate { len: truncated_len },
                    );
                    transformed.truncate(truncated_len);
                    script(
                        &network,
                        direction,
                        ordinal,
                        tag + 1_000,
                        SimDatagramFault::Corrupt {
                            offset: 5,
                            xor: 0x5a,
                        },
                    );
                    corrupt(&mut transformed, 5, 0x5a);
                    coverage.corruption += 1;
                    coverage.network_truncation += 1;
                }
                5 => {
                    script(
                        &network,
                        direction,
                        ordinal,
                        tag,
                        SimDatagramFault::FailBefore,
                    );
                    copies = 0;
                    expected_error = Some((CompletionCertainty::NotApplied, tag));
                    coverage.fail_before += 1;
                }
                6 => {
                    script(
                        &network,
                        direction,
                        ordinal,
                        tag,
                        SimDatagramFault::ErrorAfterEnqueue {
                            certainty: SimDatagramAfterEnqueueCertainty::MayHaveApplied,
                        },
                    );
                    expected_error = Some((CompletionCertainty::MayHaveApplied, tag));
                    coverage.error_after += 1;
                }
                7 => {
                    script(
                        &network,
                        direction,
                        ordinal,
                        tag,
                        SimDatagramFault::Partition,
                    );
                    dropped = true;
                    coverage.partition += 1;
                }
                8 => {
                    script(
                        &network,
                        direction,
                        ordinal,
                        tag,
                        SimDatagramFault::QueueFull,
                    );
                    copies = 0;
                    expected_error = Some((CompletionCertainty::NotApplied, 0));
                    coverage.forced_queue_full += 1;
                }
                9 => coverage.reliable += 1,
                _ => unreachable!(),
            }
            if copies != 0 && !dropped && delay != 0 {
                script(
                    &network,
                    direction,
                    ordinal,
                    tag + 2_000,
                    SimDatagramFault::Delay {
                        additional: kr_runtime::SimDuration::from_nanos(delay),
                    },
                );
            }

            let completion = runtime
                .block_on(sockets[source_index].submit_send_to(SendToRequest {
                    buffer: request_buffer,
                    destination,
                }))
                .expect("runtime drives modeled send");
            match expected_error {
                Some((certainty, expected_tag)) => {
                    let error = completion.expect_err("scripted terminal send failure");
                    assert_eq!(error.certainty(), certainty);
                    if expected_tag == 0 {
                        assert!(matches!(
                            error.error().error(),
                            DatagramError::ResourceExhausted { .. }
                        ));
                    } else {
                        assert_eq!(
                            error.error().error(),
                            &DatagramError::Injected { tag: expected_tag }
                        );
                    }
                    assert_eq!(error.error().buffer(), Some(payload.as_slice()));
                    if !payload.is_empty() {
                        assert_eq!(
                            error.error().buffer().expect("send has buffer").as_ptr(),
                            original_pointer
                        );
                    }
                    if certainty == CompletionCertainty::MayHaveApplied {
                        assert_eq!(error.error().bytes_transferred(), payload.len());
                    } else {
                        assert_eq!(error.error().bytes_transferred(), 0);
                    }
                    trace.push(format!(
                        "s:{batch}:{step}:{}:{}:{ordinal}:err:{certainty:?}:{copies}:{dropped}",
                        source.node.0, destination.node.0
                    ));
                }
                None => {
                    let result = completion.expect("modeled send succeeds");
                    assert_eq!(result.buffer, payload);
                    assert_eq!(result.bytes_sent, payload.len());
                    if !payload.is_empty() {
                        assert_eq!(result.buffer.as_ptr(), original_pointer);
                    }
                    trace.push(format!(
                        "s:{batch}:{step}:{}:{}:{ordinal}:ok:{copies}:{dropped}",
                        source.node.0, destination.node.0
                    ));
                }
            }

            if !dropped && copies != 0 {
                for copy in 0..copies {
                    scheduled.push(ScheduledPacket {
                        delay,
                        admission,
                        copy,
                        source,
                        destination,
                        payload: transformed.clone(),
                    });
                }
            }
        }

        scheduled.sort_by_key(|packet| (packet.delay, packet.admission, packet.copy));
        for pair in scheduled.windows(2) {
            if pair[0].admission > pair[1].admission {
                coverage.reordered_pairs += 1;
            }
        }
        runtime
            .run_until_stalled()
            .expect("runtime drains the modeled delivery batch");
        expected_delivery_events += u64::try_from(scheduled.len()).expect("bounded packet count");
        for packet in scheduled {
            let inbox = inboxes
                .get_mut(&packet.destination)
                .expect("all destinations are modeled");
            let queued_bytes = inbox
                .iter()
                .map(|packet| packet.payload.len())
                .sum::<usize>();
            if inbox.len() >= INGRESS_DATAGRAMS
                || queued_bytes + packet.payload.len() > INGRESS_BYTES
            {
                coverage.ingress_drops += 1;
            } else {
                inbox.push_back(ExpectedPacket {
                    admission: packet.admission,
                    copy: packet.copy,
                    source: packet.source,
                    payload: packet.payload,
                });
            }
        }

        let status = network.status();
        assert_eq!(status.scheduled_datagrams, 0);
        assert_eq!(status.scheduled_bytes, 0);
        assert_eq!(status.counters.delivery_events, expected_delivery_events);
        assert_eq!(status.pending_faults, 0);
        assert_eq!(
            status.queued_datagrams,
            inboxes.values().map(VecDeque::len).sum::<usize>()
        );
        assert_eq!(
            status.queued_bytes,
            inboxes
                .values()
                .flat_map(|inbox| inbox.iter())
                .map(|packet| packet.payload.len())
                .sum::<usize>()
        );

        for (index, socket) in sockets.iter().enumerate() {
            let available = inboxes.get(&address(index)).expect("modeled inbox").len();
            let drains = if batch + 1 == BATCHES {
                available
            } else {
                below(&random, (available.min(1) + 1) as u64) as usize
            };
            for _ in 0..drains {
                let expected = inboxes
                    .get_mut(&address(index))
                    .expect("modeled inbox")
                    .pop_front()
                    .expect("modeled drain has packet");
                let max_bytes = below(&random, (expected.payload.len() + 2) as u64) as usize;
                receive_one(
                    &mut runtime,
                    socket,
                    expected,
                    max_bytes,
                    &mut coverage,
                    &mut trace,
                );
            }
        }
    }

    for (index, socket) in sockets.iter().enumerate() {
        while let Some(expected) = inboxes
            .get_mut(&address(index))
            .expect("modeled inbox")
            .pop_front()
        {
            let max_bytes = expected.payload.len();
            receive_one(
                &mut runtime,
                socket,
                expected,
                max_bytes,
                &mut coverage,
                &mut trace,
            );
        }
        let empty = runtime
            .block_on(socket.submit_try_recv_from(RecvFromRequest {
                buffer: Vec::new(),
                max_bytes: 1,
            }))
            .expect("runtime drives final nonblocking drain")
            .expect_err("modeled inbox is empty");
        assert_eq!(empty.error().error(), &DatagramError::WouldBlock);
    }
    let final_status = network.status();
    assert_eq!(final_status.scheduled_datagrams, 0);
    assert_eq!(final_status.scheduled_bytes, 0);
    assert_eq!(final_status.queued_datagrams, 0);
    assert_eq!(final_status.queued_bytes, 0);
    assert_eq!(
        final_status.counters.dropped_ingress_full,
        coverage.ingress_drops
    );

    for socket in &sockets {
        runtime
            .block_on(socket.submit_close())
            .expect("runtime drives model close")
            .expect("model close succeeds");
    }
    drop(sockets);
    drop(network);
    drop(random);
    runtime.shutdown().expect("model runtime shuts down");
    Artifact {
        trace,
        checkpoint: runtime.snapshot().determinism_checkpoint(),
        coverage,
    }
}

#[test]
fn seeded_datagram_model_preserves_atomic_delivery_and_replays() {
    let mut coverage = Coverage::default();
    for seed in 0..SEEDS {
        let first = run_seed(seed);
        let repeated = run_seed(seed);
        assert_eq!(first, repeated, "datagram model seed {seed} diverged");
        coverage.add(first.coverage);
    }
    assert!(coverage.reliable > 0, "{coverage:#?}");
    assert!(coverage.loss > 0, "{coverage:#?}");
    assert!(coverage.duplicate > 0, "{coverage:#?}");
    assert!(coverage.corruption > 0, "{coverage:#?}");
    assert!(coverage.network_truncation > 0, "{coverage:#?}");
    assert!(coverage.fail_before > 0, "{coverage:#?}");
    assert!(coverage.error_after > 0, "{coverage:#?}");
    assert!(coverage.partition > 0, "{coverage:#?}");
    assert!(coverage.forced_queue_full > 0, "{coverage:#?}");
    assert!(coverage.empty_datagrams > 0, "{coverage:#?}");
    assert!(coverage.receive_truncation > 0, "{coverage:#?}");
    assert!(coverage.ingress_drops > 0, "{coverage:#?}");
    assert!(coverage.reordered_pairs > 0, "{coverage:#?}");
}
