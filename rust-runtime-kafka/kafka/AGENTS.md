4. **Done: reusable Kafka client layer.** `kr-kafka-client` now owns the shared
   connection driver, connector contract, endpoint/security configuration,
   topic metadata value types and ApiVersions/Metadata/SASL codecs.
   `kr-kafka-host` depends on the shared client without producer or record
   dependencies. `kr-kafka-producer-host` composes the producer owner and
   calibration with that reusable host. Produce/identity policy stays in the
   producer. Consumer protocol APIs and consumer state machines remain future
   work; the extraction precedes real-broker validation.
