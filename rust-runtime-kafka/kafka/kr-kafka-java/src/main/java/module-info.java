/** Kafka's supported idempotent producer subset over the versioned Rust ABI. */
module io.krkafka {
    requires transitive kafka.clients;
    exports io.krkafka.producer;
}
