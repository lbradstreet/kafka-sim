use crate::*;

pub use decoder::ArtifactHeaderDecoder;
pub use encoder::ArtifactHeaderEncoder;

pub use crate::SBE_SCHEMA_ID;
pub use crate::SBE_SCHEMA_VERSION;
pub use crate::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 240;
pub const SBE_TEMPLATE_ID: u16 = 4;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct ArtifactHeaderEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for ArtifactHeaderEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for ArtifactHeaderEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> ArtifactHeaderEncoder<'a> {
        pub fn wrap(mut self, buf: WriteBuf<'a>, offset: usize) -> Self {
            let limit = offset + SBE_BLOCK_LENGTH as usize;
            self.buf = buf;
            self.initial_offset = offset;
            self.offset = offset;
            self.limit = limit;
            self
        }

        #[inline]
        pub const fn encoded_length(&self) -> usize {
            self.limit - self.offset
        }

        pub fn header(self, offset: usize) -> MessageHeaderEncoder<Self> {
            let mut header = MessageHeaderEncoder::default().wrap(self, offset);
            header.block_length(SBE_BLOCK_LENGTH);
            header.template_id(SBE_TEMPLATE_ID);
            header.schema_id(SBE_SCHEMA_ID);
            header.version(SBE_SCHEMA_VERSION);
            header
        }

        /// primitive field 'artifactSchema'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn artifact_schema(&mut self, value: u32) -> &mut Self {
            let offset = self.offset;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'runtimeReproductionSchema'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 4
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn runtime_reproduction_schema(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 4;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'determinismCheckpointSchema'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 8
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn determinism_checkpoint_schema(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 8;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'traceSchema'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 12
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn trace_schema(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 12;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'rngVersion'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 16
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn rng_version(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 16;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'seed'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 20
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn seed(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 20;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxTasks'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 28
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_tasks(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 28;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxTimers'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 36
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_timers(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 36;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxStepsPerRun'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 44
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_steps_per_run(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 44;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxTimePresent'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 52
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn max_time_present(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 52;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'maxTimeNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 53
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_time_ns(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 53;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'retainedEventCount'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 61
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn retained_event_count(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 61;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'retentionMode'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 69
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn retention_mode(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 69;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'capacityUnit'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 70
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn capacity_unit(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 70;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'prefixCapacity'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 71
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn prefix_capacity(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 71;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'tailCapacity'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 79
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn tail_capacity(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 79;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'retainedBytes'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 87
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn retained_bytes(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 87;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'samplingMode'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 95
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn sampling_mode(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 95;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'samplingAlgorithmVersion'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 96
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn sampling_algorithm_version(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 96;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'samplingPeriod'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 100
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn sampling_period(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 100;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'samplingPhase'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 108
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn sampling_phase(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 108;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'fingerprintScope'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 116
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn fingerprint_scope(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 116;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'droppedEvents'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 117
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn dropped_events(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 117;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'lastSequencePresent'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 125
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn last_sequence_present(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 125;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'lastSequence'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 126
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn last_sequence(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 126;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'orderingViolationPresent'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 134
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn ordering_violation_present(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 134;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'previousSequence'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 135
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn previous_sequence(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 135;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'rejectedSequence'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 143
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn rejected_sequence(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 143;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'traceFingerprint'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 151
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn trace_fingerprint(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 151;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'nowNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 159
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn now_ns(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 159;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'totalSteps'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 167
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn total_steps(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 167;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'nextEnqueueSequence'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 175
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn next_enqueue_sequence(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 175;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'nextTimerSequence'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 183
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn next_timer_sequence(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 183;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'nextTimerId'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 191
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn next_timer_id(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 191;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'readyTasks'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 199
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn ready_tasks(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 199;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'liveTimers'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 207
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn live_timers(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 207;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'liveTasks'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 215
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn live_tasks(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 215;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'stopped'
        /// - min value: 0
        /// - max value: 254
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 223
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn stopped(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 223;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'randomStreamCount'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 224
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn random_stream_count(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 224;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'taskCount'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 228
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn task_count(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 228;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'startTimeNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 232
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn start_time_ns(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 232;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'UTF-8'
        #[inline]
        pub fn driver(&mut self, value: &str) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u32::MAX - 1) as usize);
            self.set_limit(limit + 4 + data_length);
            self.get_buf_mut().put_u32_at(limit, data_length as u32);
            self.get_buf_mut()
                .put_slice_at(limit + 4, &value[0..data_length].as_bytes());
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'UTF-8'
        #[inline]
        pub fn outcome(&mut self, value: &str) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u32::MAX - 1) as usize);
            self.set_limit(limit + 4 + data_length);
            self.get_buf_mut().put_u32_at(limit, data_length as u32);
            self.get_buf_mut()
                .put_slice_at(limit + 4, &value[0..data_length].as_bytes());
            self
        }
    }
} // end encoder

pub mod decoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Clone, Copy, Debug, Default)]
    pub struct ArtifactHeaderDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for ArtifactHeaderDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for ArtifactHeaderDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for ArtifactHeaderDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> ArtifactHeaderDecoder<'a> {
        pub fn wrap(
            mut self,
            buf: ReadBuf<'a>,
            offset: usize,
            acting_block_length: u16,
            acting_version: u16,
        ) -> Self {
            let limit = offset + acting_block_length as usize;
            self.buf = buf;
            self.initial_offset = offset;
            self.offset = offset;
            self.limit = limit;
            self.acting_block_length = acting_block_length;
            self.acting_version = acting_version;
            self
        }

        #[inline]
        pub const fn encoded_length(&self) -> usize {
            self.limit - self.offset
        }

        pub fn header(self, mut header: MessageHeaderDecoder<ReadBuf<'a>>, offset: usize) -> Self {
            debug_assert_eq!(SBE_TEMPLATE_ID, header.template_id());
            let acting_block_length = header.block_length();
            let acting_version = header.version();

            self.wrap(
                header.parent().unwrap(),
                offset + message_header_codec::ENCODED_LENGTH,
                acting_block_length,
                acting_version,
            )
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn artifact_schema(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn runtime_reproduction_schema(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 4)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn determinism_checkpoint_schema(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 8)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn trace_schema(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 12)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn rng_version(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 16)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn seed(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 20)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_tasks(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 28)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_timers(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 36)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_steps_per_run(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 44)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_time_present(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 52)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_time_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 53)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn retained_event_count(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 61)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn retention_mode(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 69)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn capacity_unit(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 70)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn prefix_capacity(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 71)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn tail_capacity(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 79)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn retained_bytes(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 87)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn sampling_mode(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 95)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn sampling_algorithm_version(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 96)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn sampling_period(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 100)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn sampling_phase(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 108)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn fingerprint_scope(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 116)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn dropped_events(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 117)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn last_sequence_present(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 125)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn last_sequence(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 126)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn ordering_violation_present(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 134)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn previous_sequence(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 135)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn rejected_sequence(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 143)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn trace_fingerprint(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 151)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn now_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 159)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn total_steps(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 167)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn next_enqueue_sequence(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 175)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn next_timer_sequence(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 183)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn next_timer_id(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 191)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn ready_tasks(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 199)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn live_timers(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 207)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn live_tasks(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 215)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn stopped(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 223)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn random_stream_count(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 224)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn task_count(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 228)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn start_time_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 232)
        }

        /// VAR_DATA DECODER - character encoding: 'UTF-8'
        #[inline]
        pub fn driver_decoder(&mut self) -> (usize, usize) {
            let offset = self.get_limit();
            let data_length = self.get_buf().get_u32_at(offset) as usize;
            self.set_limit(offset + 4 + data_length);
            (offset + 4, data_length)
        }

        #[inline]
        pub fn driver_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'UTF-8'
        #[inline]
        pub fn outcome_decoder(&mut self) -> (usize, usize) {
            let offset = self.get_limit();
            let data_length = self.get_buf().get_u32_at(offset) as usize;
            self.set_limit(offset + 4 + data_length);
            (offset + 4, data_length)
        }

        #[inline]
        pub fn outcome_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }
    }
} // end decoder
