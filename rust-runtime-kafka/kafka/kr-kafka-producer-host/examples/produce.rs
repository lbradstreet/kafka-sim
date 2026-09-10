//! One-record Linux smoke client. Topic creation is an operator responsibility.
use kr_kafka_producer::{
    config::{BrokerEndpoint, ProducerConfig, TransportPolicy},
    types::{DeliveryKind, Event, RecordDescriptor},
};
use kr_kafka_producer_host::producer::{HostProducer, HostStatus};
use kr_runtime::RuntimeDuration;
use std::{error::Error, time::Duration};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 5 {
        return Err("usage: produce <uring|readiness> <host> <port> <topic> <value>".into());
    }
    let transport = match args[0].as_str() {
        "uring" => TransportPolicy::Uring,
        "readiness" => TransportPolicy::Readiness,
        _ => return Err("backend must be uring or readiness".into()),
    };
    let config = ProducerConfig {
        bootstrap: vec![BrokerEndpoint {
            host: args[1].clone(),
            port: args[2].parse()?,
        }],
        transport,
        brokers_max: 4,
        input_bytes: 4 * 1024 * 1024,
        compressed_bytes: 4 * 1024 * 1024,
        codec_contexts: 1,
        record_descriptors: 1024,
        pending_records_per_topic: 1024,
        delivery_event_capacity: 1024,
        max_live_leases: 64,
        release_event_capacity: 64,
        max_batches: 1024,
        ..Default::default()
    };
    config.validate()?;
    let producer = HostProducer::start(config)?;
    let client = producer.client();
    let work = (|| -> Result<(), Box<dyn Error>> {
        // A topic handle can accept input while its initial metadata is pending.
        let topic = client.open_topic(&args[3])?;
        let record = RecordDescriptor {
            topic,
            partition_hint: None,
            lane_hint: None,
            key: None,
            value: Some(args[4].as_bytes()),
            headers: &[],
            timestamp_ms: 0,
            user_token: 1,
            delivery_timeout: None,
        };
        let submitted = client.submit_copy(&[record]);
        if submitted.accepted != 1 {
            return Err(format!("admission rejected: {:?}", submitted.error).into());
        }
        let flush = client.flush()?;
        producer.close(RuntimeDuration::from_nanos(30_000_000_000))?;
        let mut acked = false;
        let mut flushed = false;
        let mut closed = false;
        let mut events = [Event::Fatal { code: 0 }; 64];
        loop {
            let count = client.poll_events(&mut events);
            for event in &events[..count] {
                println!("{event:?}");
                match event {
                    Event::Delivery(delivery) => {
                        acked = delivery.outcome.kind == DeliveryKind::Acked
                    }
                    Event::FlushDone { token } if *token == flush => flushed = true,
                    Event::Closed { .. } => closed = true,
                    _ => {}
                }
            }
            if closed {
                break;
            }
            if count == 0 {
                if !matches!(producer.status(), HostStatus::Running) {
                    break;
                }
                // This is the application thread; the producer owns its executor.
                std::thread::sleep(Duration::from_millis(1));
            }
        }
        if !acked || !flushed || !closed {
            return Err(
                "producer did not finish with an acknowledged record and completed flush".into(),
            );
        }
        Ok(())
    })();
    // Every exit path fences input and observes the actual owner termination.
    let _ = producer.close(RuntimeDuration::ZERO);
    let joined = producer.join();
    work?;
    joined?;
    Ok(())
}
