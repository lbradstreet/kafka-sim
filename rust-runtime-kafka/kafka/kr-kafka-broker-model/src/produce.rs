use super::*;
use kr_kafka_record::{BatchDecodeError, inspect_batch};
use wire::{plan::Records, produce_request, produce_response};

#[derive(Clone, Copy, PartialEq, Eq)]
enum TopicWire<'a> {
    Name(&'a str),
    Id(TopicId),
}
struct InputTopic<'a> {
    wire: TopicWire<'a>,
    partitions: Vec<(i32, Option<&'a [u8]>)>,
}
struct PartitionResult {
    index: i32,
    error: i16,
    offset: i64,
    leader: i32,
    epoch: i32,
}
impl PartitionResult {
    fn new(index: i32) -> Self {
        Self {
            index,
            error: 0,
            offset: -1,
            leader: -1,
            epoch: -1,
        }
    }
}
impl BrokerModel {
    pub(super) fn produce(
        &mut self,
        broker: i32,
        request: &produce_request::View<'_>,
        version: i16,
        correlation: i32,
        fault: FaultPlan,
    ) -> Result<Vec<u8>> {
        let mut input = Vec::new();
        let acks;
        let transactional;
        match request {
            produce_request::View::V9(request) => {
                acks = request.acks;
                transactional = request.transactional_id.is_some();
                for topic in request.topic_data.iter() {
                    let topic = topic?;
                    let mut partitions = Vec::new();
                    for partition in topic.partition_data.iter() {
                        let partition = partition?;
                        partitions.push((partition.index, record_bytes(partition.records)?));
                    }
                    input.push(InputTopic {
                        wire: TopicWire::Name(topic.name),
                        partitions,
                    });
                }
            }
            produce_request::View::V13(request) => {
                acks = request.acks;
                transactional = request.transactional_id.is_some();
                for topic in request.topic_data.iter() {
                    let topic = topic?;
                    let mut partitions = Vec::new();
                    for partition in topic.partition_data.iter() {
                        let partition = partition?;
                        partitions.push((partition.index, record_bytes(partition.records)?));
                    }
                    input.push(InputTopic {
                        wire: TopicWire::Id(topic.topic_id),
                        partitions,
                    });
                }
            }
        }
        if input.len() > self.config.max_topics
            || input.iter().map(|t| t.partitions.len()).sum::<usize>() > self.config.max_partitions
        {
            return Err(Error::Limit("produce partitions"));
        }
        // Reject structural duplicates before any partition mutates. Kafka Produce
        // is one batch per partition, never an implicit multi-batch merge.
        for (i, topic) in input.iter().enumerate() {
            if input[..i].iter().any(|t| t.wire == topic.wire) {
                return Err(Error::InvalidRequest("duplicate Produce topic"));
            }
            for (j, (index, _)) in topic.partitions.iter().enumerate() {
                if topic.partitions[..j]
                    .iter()
                    .any(|(previous, _)| previous == index)
                {
                    return Err(Error::InvalidRequest("duplicate Produce partition"));
                }
            }
        }
        let forced = if version > self.config.produce_max_version {
            Some(code::UNSUPPORTED_VERSION)
        } else if acks != -1 {
            Some(code::INVALID_REQUIRED_ACKS)
        } else if transactional {
            Some(code::INVALID_REQUEST)
        } else {
            fault.reject_before_commit
        };
        let mut results = Vec::new();
        for topic in &input {
            let location = self.topics.iter().position(|t| {
                !t.deleted
                    && match topic.wire {
                        TopicWire::Name(name) => t.name == name,
                        TopicWire::Id(id) => t.id == id,
                    }
            });
            let mut partitions = Vec::new();
            for &(index, records) in &topic.partitions {
                let mut result = PartitionResult::new(index);
                if let Some(error) = forced {
                    result.error = error;
                } else if let Some(location) = location {
                    if let Some(partition) = usize::try_from(index)
                        .ok()
                        .and_then(|i| self.topics[location].partitions.get(i))
                    {
                        result.leader = partition.leader;
                        result.epoch = partition.epoch;
                        if partition.leader != broker {
                            result.error = code::NOT_LEADER_OR_FOLLOWER;
                        } else if let Some(records) = records {
                            (result.error, result.offset) = self.append(
                                location,
                                index,
                                records,
                                fault.duplicate_sequence_error,
                            )?;
                        } else {
                            result.error = code::INVALID_RECORD;
                        }
                    } else {
                        result.error = code::UNKNOWN_TOPIC_OR_PARTITION;
                    }
                } else {
                    result.error = match topic.wire {
                        TopicWire::Name(_) => code::UNKNOWN_TOPIC_OR_PARTITION,
                        TopicWire::Id(_) => code::UNKNOWN_TOPIC_ID,
                    };
                }
                if result.error != 0 && result.error != code::DUPLICATE_SEQUENCE_NUMBER {
                    self.stats.rejected_batches += 1;
                }
                partitions.push(result);
            }
            results.push(partitions);
        }
        self.produce_response(
            &input,
            &results,
            version,
            correlation,
            fault.throttle_time_ms,
        )
    }
    fn append(
        &mut self,
        topic: usize,
        index: i32,
        bytes: &[u8],
        explicit_duplicate: bool,
    ) -> Result<(i16, i64)> {
        let batch = match inspect_batch(bytes, self.config.batch_limits) {
            Ok(batch) => batch,
            Err(error) => {
                return Ok((
                    match error {
                        BatchDecodeError::Checksum
                        | BatchDecodeError::Compression
                        | BatchDecodeError::Truncated => code::CORRUPT_MESSAGE,
                        BatchDecodeError::Limit(_) => code::MESSAGE_TOO_LARGE,
                        _ => code::INVALID_RECORD,
                    },
                    -1,
                ));
            }
        };
        let header = batch.header;
        let identity = header.identity;
        let partition = &self.topics[topic].partitions[index as usize];
        let position = partition
            .producers
            .iter()
            .position(|p| p.id == identity.producer_id);
        if let Some(position) = position {
            let state = &partition.producers[position];
            if identity.producer_epoch < state.epoch {
                return Ok((code::INVALID_PRODUCER_EPOCH, -1));
            }
            if identity.producer_epoch == state.epoch {
                if let Some(old) = state.history.iter().find(|h| {
                    h.base_sequence == identity.base_sequence && h.count == header.record_count
                }) {
                    self.stats.duplicate_batches += 1;
                    return Ok((
                        if explicit_duplicate {
                            code::DUPLICATE_SEQUENCE_NUMBER
                        } else {
                            0
                        },
                        old.base_offset,
                    ));
                }
                if identity.base_sequence != state.next_sequence {
                    return Ok((code::OUT_OF_ORDER_SEQUENCE_NUMBER, -1));
                }
            } else if identity.base_sequence != 0 {
                return Ok((code::OUT_OF_ORDER_SEQUENCE_NUMBER, -1));
            }
        } else if partition.producers.len() == self.config.max_producers {
            return Ok((code::KAFKA_STORAGE_ERROR, -1));
        }
        // Bounds are checked before log, sequence state, or offsets change.
        let raw_bytes = batch
            .raw_bytes()
            .len()
            .checked_add(61)
            .ok_or(Error::Limit("decoded log bytes"))?;
        let retained_bytes = raw_bytes
            .checked_add(bytes.len())
            .ok_or(Error::Limit("retained log bytes"))?;
        if self.log.len() == self.config.max_log_batches
            || header.record_count as usize
                > self.config.max_log_records - self.stats.committed_records
            || retained_bytes > self.config.max_log_bytes - self.stats.log_bytes
        {
            return Ok((code::KAFKA_STORAGE_ERROR, -1));
        }
        let base_offset = partition.next_offset;
        let next_offset = base_offset
            .checked_add(i64::from(header.record_count))
            .ok_or(Error::Limit("log offsets"))?;
        let leader_epoch = partition.epoch;
        let mut wire = Vec::new();
        wire.try_reserve_exact(bytes.len())
            .map_err(|_| Error::Limit("wire log allocation"))?;
        let retained_bytes = raw_bytes
            .checked_add(wire.capacity())
            .ok_or(Error::Limit("retained log bytes"))?;
        if retained_bytes > self.config.max_log_bytes - self.stats.log_bytes {
            return Ok((code::KAFKA_STORAGE_ERROR, -1));
        }
        wire.extend_from_slice(bytes);
        wire[..8].copy_from_slice(&base_offset.to_be_bytes());
        wire[12..16].copy_from_slice(&leader_epoch.to_be_bytes());
        self.log
            .try_reserve(1)
            .map_err(|_| Error::Limit("log allocation"))?;
        self.wire_log
            .try_reserve(1)
            .map_err(|_| Error::Limit("wire log index allocation"))?;
        self.topics[topic].partitions[index as usize]
            .batches
            .try_reserve(1)
            .map_err(|_| Error::Limit("partition log index allocation"))?;
        let mut records = Vec::new();
        for record in batch.records() {
            let record = record.map_err(|_| Error::InvalidRequest("validated batch changed"))?;
            let mut headers = Vec::new();
            for header in record.headers {
                let header =
                    header.map_err(|_| Error::InvalidRequest("validated header changed"))?;
                headers.push(CommittedHeader {
                    key: header.key.to_string(),
                    value: header.value.map(<[u8]>::to_vec),
                });
            }
            records.push(CommittedRecord {
                offset: base_offset + i64::from(record.offset_delta),
                sequence: next_sequence(identity.base_sequence, record.offset_delta),
                timestamp: record.timestamp,
                key: record.key.map(<[u8]>::to_vec),
                value: record.value.map(<[u8]>::to_vec),
                headers,
            });
        }
        let topic_id = self.topics[topic].id;
        let partition = &mut self.topics[topic].partitions[index as usize];
        let position = match position {
            Some(p) => p,
            None => {
                partition.producers.push(ProducerState {
                    id: identity.producer_id,
                    epoch: identity.producer_epoch,
                    next_sequence: identity.base_sequence,
                    history: VecDeque::new(),
                });
                partition.producers.len() - 1
            }
        };
        let state = &mut partition.producers[position];
        if state.epoch != identity.producer_epoch {
            state.epoch = identity.producer_epoch;
            state.history.clear();
        }
        if state.history.len() == 5 {
            state.history.pop_front();
        }
        state.history.push_back(BatchHistory {
            base_sequence: identity.base_sequence,
            count: header.record_count,
            base_offset,
        });
        state.next_sequence = next_sequence(identity.base_sequence, header.record_count);
        partition.next_offset = next_offset;
        self.stats.committed_records += header.record_count as usize;
        self.stats.committed_batches += 1;
        self.stats.log_bytes += retained_bytes;
        self.stats.decoded_log_bytes += raw_bytes;
        self.stats.wire_log_bytes += wire.capacity();
        partition.batches.push(self.log.len());
        self.wire_log.push(wire);
        self.log.push(CommittedBatch {
            topic: topic_id,
            partition: index,
            identity,
            base_offset,
            records,
        });
        Ok((0, base_offset))
    }
    fn produce_response(
        &self,
        input: &[InputTopic<'_>],
        results: &[Vec<PartitionResult>],
        version: i16,
        correlation: i32,
        throttle: i32,
    ) -> Result<Vec<u8>> {
        if version == 9 {
            use produce_response::v9::*;
            let partitions: Vec<Vec<_>> = results
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|p| PartitionProduceResponse {
                            index: p.index,
                            error_code: p.error,
                            base_offset: p.offset,
                            log_start_offset: 0,
                            ..Default::default()
                        })
                        .collect()
                })
                .collect();
            let topics: Vec<_> = input
                .iter()
                .zip(&partitions)
                .map(|(topic, p)| TopicProduceResponse {
                    name: match topic.wire {
                        TopicWire::Name(name) => name,
                        _ => unreachable!(),
                    },
                    partition_responses: (&p[..]).into(),
                    ..Default::default()
                })
                .collect();
            self.encode(
                Response::ProduceResponse(produce_response::View::V9(ProduceResponse {
                    responses: (&topics[..]).into(),
                    throttle_time_ms: throttle,
                    ..Default::default()
                })),
                version,
                correlation,
            )
        } else if version == 13 {
            use produce_response::v13::*;
            let partitions: Vec<Vec<_>> = results
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|p| PartitionProduceResponse {
                            index: p.index,
                            error_code: p.error,
                            base_offset: p.offset,
                            log_start_offset: 0,
                            current_leader: LeaderIdAndEpoch {
                                leader_id: p.leader,
                                leader_epoch: p.epoch,
                                ..Default::default()
                            },
                            ..Default::default()
                        })
                        .collect()
                })
                .collect();
            let topics: Vec<_> = input
                .iter()
                .zip(&partitions)
                .map(|(topic, p)| TopicProduceResponse {
                    topic_id: match topic.wire {
                        TopicWire::Id(id) => id,
                        _ => unreachable!(),
                    },
                    partition_responses: (&p[..]).into(),
                    ..Default::default()
                })
                .collect();
            let nodes: Vec<_> = self
                .brokers
                .iter()
                .map(|b| NodeEndpoint {
                    node_id: b.id,
                    host: &b.host,
                    port: i32::from(b.port),
                    ..Default::default()
                })
                .collect();
            self.encode(
                Response::ProduceResponse(produce_response::View::V13(ProduceResponse {
                    responses: (&topics[..]).into(),
                    throttle_time_ms: throttle,
                    node_endpoints: (&nodes[..]).into(),
                    ..Default::default()
                })),
                version,
                correlation,
            )
        } else {
            use produce_response::v10::*;
            let partitions: Vec<Vec<_>> = results
                .iter()
                .map(|r| {
                    r.iter()
                        .map(|p| PartitionProduceResponse {
                            index: p.index,
                            error_code: p.error,
                            base_offset: p.offset,
                            log_start_offset: 0,
                            current_leader: LeaderIdAndEpoch {
                                leader_id: p.leader,
                                leader_epoch: p.epoch,
                                ..Default::default()
                            },
                            ..Default::default()
                        })
                        .collect()
                })
                .collect();
            let topics: Vec<_> = input
                .iter()
                .zip(&partitions)
                .map(|(topic, p)| TopicProduceResponse {
                    name: match topic.wire {
                        TopicWire::Name(name) => name,
                        _ => unreachable!(),
                    },
                    partition_responses: (&p[..]).into(),
                    ..Default::default()
                })
                .collect();
            let nodes: Vec<_> = self
                .brokers
                .iter()
                .map(|b| NodeEndpoint {
                    node_id: b.id,
                    host: &b.host,
                    port: i32::from(b.port),
                    ..Default::default()
                })
                .collect();
            self.encode(
                Response::ProduceResponse(produce_response::View::V10(ProduceResponse {
                    responses: (&topics[..]).into(),
                    throttle_time_ms: throttle,
                    node_endpoints: (&nodes[..]).into(),
                    ..Default::default()
                })),
                version,
                correlation,
            )
        }
    }
}
fn record_bytes(records: Option<Records<'_>>) -> Result<Option<&[u8]>> {
    match records {
        None => Ok(None),
        Some(Records::Borrowed(bytes)) => Ok(Some(bytes)),
        Some(Records::Chunks(_) | Records::HeaderAndChunks { .. }) => Err(Error::InvalidRequest(
            "decoded requests must borrow frame bytes",
        )),
    }
}
fn next_sequence(base: i32, count: i32) -> i32 {
    ((i64::from(base) + i64::from(count)) % (i64::from(i32::MAX) + 1)) as i32
}
