use super::*;
use wire::metadata_response::{self as api, v12::*};
impl BrokerModel {
    pub(super) fn metadata(
        &self,
        request: &wire::metadata_request::v12::MetadataRequest<'_>,
        correlation: i32,
        throttle: i32,
    ) -> Result<Vec<u8>> {
        let mut selected = Vec::new();
        if let Some(topics) = &request.topics {
            if topics.len() > self.config.max_topics {
                return Err(Error::Limit("metadata topics"));
            }
            for query in topics.iter() {
                let query = query?;
                let found = if query.topic_id != [0; 16] {
                    self.topics
                        .iter()
                        .find(|t| t.id == query.topic_id && !t.deleted)
                } else {
                    self.topics
                        .iter()
                        .find(|t| !t.deleted && Some(t.name.as_str()) == query.name)
                };
                if let Some(topic) = found {
                    if query.name.is_some_and(|n| n != topic.name) {
                        selected.push((
                            None,
                            query.topic_id,
                            query.name,
                            code::INCONSISTENT_TOPIC_ID,
                        ));
                    } else {
                        selected.push((Some(topic), topic.id, Some(topic.name.as_str()), 0));
                    }
                } else {
                    selected.push((
                        None,
                        query.topic_id,
                        query.name,
                        if query.topic_id == [0; 16] {
                            code::UNKNOWN_TOPIC_OR_PARTITION
                        } else {
                            code::UNKNOWN_TOPIC_ID
                        },
                    ));
                }
            }
        } else {
            for topic in self.topics.iter().filter(|t| !t.deleted) {
                selected.push((Some(topic), topic.id, Some(topic.name.as_str()), 0));
            }
        }
        let partitions: Vec<Vec<_>> = selected
            .iter()
            .map(|(topic, _, _, _)| {
                topic.map_or_else(Vec::new, |t| {
                    t.partitions
                        .iter()
                        .enumerate()
                        .map(|(index, p)| MetadataResponsePartition {
                            partition_index: index as i32,
                            leader_id: p.leader,
                            leader_epoch: p.epoch,
                            replica_nodes: core::slice::from_ref(&p.leader).into(),
                            isr_nodes: core::slice::from_ref(&p.leader).into(),
                            ..Default::default()
                        })
                        .collect()
                })
            })
            .collect();
        let topics: Vec<_> = selected
            .iter()
            .zip(&partitions)
            .map(|((_, id, name, error), partitions)| MetadataResponseTopic {
                error_code: *error,
                topic_id: *id,
                name: *name,
                partitions: (&partitions[..]).into(),
                ..Default::default()
            })
            .collect();
        let brokers: Vec<_> = self
            .brokers
            .iter()
            .map(|b| MetadataResponseBroker {
                node_id: b.id,
                host: &b.host,
                port: i32::from(b.port),
                ..Default::default()
            })
            .collect();
        self.encode(
            Response::MetadataResponse(api::View::V12(MetadataResponse {
                throttle_time_ms: throttle,
                brokers: (&brokers[..]).into(),
                cluster_id: Some("kr-kafka-broker-model-v1"),
                controller_id: self.brokers.first().map_or(-1, |b| b.id),
                topics: (&topics[..]).into(),
                ..Default::default()
            })),
            12,
            correlation,
        )
    }
}
