use crate::*;

pub use decoder::StorageTraceArtifactDecoder;
pub use encoder::StorageTraceArtifactEncoder;

pub use crate::SBE_SCHEMA_ID;
pub use crate::SBE_SCHEMA_VERSION;
pub use crate::SBE_SEMANTIC_VERSION;

pub const SBE_BLOCK_LENGTH: u16 = 169;
pub const SBE_TEMPLATE_ID: u16 = 1;

pub mod encoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Debug, Default)]
    pub struct StorageTraceArtifactEncoder<'a> {
        buf: WriteBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
    }

    impl<'a> Writer<'a> for StorageTraceArtifactEncoder<'a> {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            &mut self.buf
        }
    }

    impl<'a> Encoder<'a> for StorageTraceArtifactEncoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }

        /// Set all optional fields to their 'null' values.
        #[inline]
        fn nullify_optional_fields(&mut self) -> &mut Self {
            {
                let mut composite_encoder = core::mem::take(self).config_encoder();
                composite_encoder.nullify_optional_fields();
                *self = composite_encoder.parent().expect("parent missing");
            }
            {
                let mut composite_encoder = core::mem::take(self).runtime_encoder();
                composite_encoder.nullify_optional_fields();
                *self = composite_encoder.parent().expect("parent missing");
            }
            self
        }
    }

    impl<'a> StorageTraceArtifactEncoder<'a> {
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

        /// primitive field 'artifactSchemaVersion'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn artifact_schema_version(&mut self, value: u32) -> &mut Self {
            let offset = self.offset;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'startedAtNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 4
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn started_at_ns(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 4;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'completedAtNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 12
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn completed_at_ns(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 12;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// COMPOSITE ENCODER
        #[inline]
        pub fn config_encoder(
            self,
        ) -> storage_config_snapshot_codec::StorageConfigSnapshotEncoder<Self> {
            let offset = self.offset + 20;
            storage_config_snapshot_codec::StorageConfigSnapshotEncoder::default()
                .wrap(self, offset)
        }

        /// COMPOSITE ENCODER
        #[inline]
        pub fn runtime_encoder(self) -> runtime_metadata_codec::RuntimeMetadataEncoder<Self> {
            let offset = self.offset + 84;
            runtime_metadata_codec::RuntimeMetadataEncoder::default().wrap(self, offset)
        }

        /// GROUP ENCODER (id=10)
        #[inline]
        pub fn steps_encoder(
            self,
            count: u16,
            steps_encoder: StepsEncoder<Self>,
        ) -> StepsEncoder<Self> {
            steps_encoder.wrap(self, count)
        }

        /// VAR_DATA ENCODER - character encoding: 'UTF-8'
        #[inline]
        pub fn scenario(&mut self, value: &str) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length].as_bytes());
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'UTF-8'
        #[inline]
        pub fn source_test(&mut self, value: &str) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length].as_bytes());
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'UTF-8'
        #[inline]
        pub fn provider(&mut self, value: &str) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length].as_bytes());
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'UTF-8'
        #[inline]
        pub fn generator(&mut self, value: &str) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length].as_bytes());
            self
        }
    }

    #[derive(Debug, Default)]
    pub struct StepsEncoder<P> {
        parent: Option<P>,
        count: u16,
        index: usize,
        offset: usize,
        initial_limit: usize,
    }

    impl<'a, P> Writer<'a> for StepsEncoder<P>
    where
        P: Writer<'a> + Default,
    {
        #[inline]
        fn get_buf_mut(&mut self) -> &mut WriteBuf<'a> {
            if let Some(parent) = self.parent.as_mut() {
                parent.get_buf_mut()
            } else {
                panic!("parent was None")
            }
        }
    }

    impl<'a, P> Encoder<'a> for StepsEncoder<P>
    where
        P: Encoder<'a> + Default,
    {
        #[inline]
        fn get_limit(&self) -> usize {
            self.parent.as_ref().expect("parent missing").get_limit()
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.parent
                .as_mut()
                .expect("parent missing")
                .set_limit(limit);
        }

        /// Set all optional fields to their 'null' values.
        #[inline]
        fn nullify_optional_fields(&mut self) -> &mut Self {
            self.offset_opt(None);
            self.transfer_length_opt(None);
            self.result_length_opt(None);
            {
                let mut composite_encoder = core::mem::take(self).before_encoder();
                composite_encoder.nullify_optional_fields();
                *self = composite_encoder.parent().expect("parent missing");
            }
            {
                let mut composite_encoder = core::mem::take(self).after_encoder();
                composite_encoder.nullify_optional_fields();
                *self = composite_encoder.parent().expect("parent missing");
            }
            self
        }
    }

    impl<'a, P> StepsEncoder<P>
    where
        P: Encoder<'a> + Default,
    {
        #[inline]
        pub fn wrap(mut self, mut parent: P, count: u16) -> Self {
            let initial_limit = parent.get_limit();
            parent.set_limit(initial_limit + 4);
            parent
                .get_buf_mut()
                .put_u16_at(initial_limit, Self::block_length());
            parent.get_buf_mut().put_u16_at(initial_limit + 2, count);
            self.parent = Some(parent);
            self.count = count;
            self.index = usize::MAX;
            self.offset = usize::MAX;
            self.initial_limit = initial_limit;
            self
        }

        #[inline]
        pub const fn block_length() -> u16 {
            157
        }

        #[inline]
        pub fn parent(&mut self) -> SbeResult<P> {
            self.parent.take().ok_or(SbeErr::ParentNotSet)
        }

        /// will return Some(current index) when successful otherwise None
        #[inline]
        pub fn advance(&mut self) -> SbeResult<Option<usize>> {
            let index = self.index.wrapping_add(1);
            if index >= self.count as usize {
                return Ok(None);
            }
            if let Some(parent) = self.parent.as_mut() {
                self.offset = parent.get_limit();
                parent.set_limit(self.offset + Self::block_length() as usize);
                self.index = index;
                Ok(Some(index))
            } else {
                Err(SbeErr::ParentNotSet)
            }
        }

        /// primitive field 'sequence'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn sequence(&mut self, value: u32) -> &mut Self {
            let offset = self.offset;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'startedAtNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 4
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn started_at_ns(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 4;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'completedAtNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 12
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn completed_at_ns(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 12;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// REQUIRED enum
        #[inline]
        pub fn operation(&mut self, value: storage_operation::StorageOperation) -> &mut Self {
            let offset = self.offset + 20;
            self.get_buf_mut().put_u8_at(offset, value as u8);
            self
        }

        /// REQUIRED enum
        #[inline]
        pub fn outcome(&mut self, value: storage_outcome::StorageOutcome) -> &mut Self {
            let offset = self.offset + 21;
            self.get_buf_mut().put_u8_at(offset, value as u8);
            self
        }

        /// REQUIRED enum
        #[inline]
        pub fn certainty(&mut self, value: completion_certainty::CompletionCertainty) -> &mut Self {
            let offset = self.offset + 22;
            self.get_buf_mut().put_u8_at(offset, value as u8);
            self
        }

        /// primitive field 'offset'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 23
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn offset(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 23;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// optional primitive field 'offset'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 23
        /// - encodedLength: 8
        /// - version: 0
        /// Set to `None` to encode the field null value.
        #[inline]
        pub fn offset_opt(&mut self, value: Option<u64>) -> &mut Self {
            match value {
                Some(value) => self.offset(value),
                None => self.offset(0xffffffffffffffff_u64),
            };
            self
        }

        /// primitive field 'transferLength'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 31
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn transfer_length(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 31;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// optional primitive field 'transferLength'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 31
        /// - encodedLength: 8
        /// - version: 0
        /// Set to `None` to encode the field null value.
        #[inline]
        pub fn transfer_length_opt(&mut self, value: Option<u64>) -> &mut Self {
            match value {
                Some(value) => self.transfer_length(value),
                None => self.transfer_length(0xffffffffffffffff_u64),
            };
            self
        }

        /// primitive field 'resultLength'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 39
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn result_length(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 39;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// optional primitive field 'resultLength'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 39
        /// - encodedLength: 8
        /// - version: 0
        /// Set to `None` to encode the field null value.
        #[inline]
        pub fn result_length_opt(&mut self, value: Option<u64>) -> &mut Self {
            match value {
                Some(value) => self.result_length(value),
                None => self.result_length(0xffffffffffffffff_u64),
            };
            self
        }

        /// COMPOSITE ENCODER
        #[inline]
        pub fn before_encoder(
            self,
        ) -> storage_status_snapshot_codec::StorageStatusSnapshotEncoder<Self> {
            let offset = self.offset + 47;
            storage_status_snapshot_codec::StorageStatusSnapshotEncoder::default()
                .wrap(self, offset)
        }

        /// COMPOSITE ENCODER
        #[inline]
        pub fn after_encoder(
            self,
        ) -> storage_status_snapshot_codec::StorageStatusSnapshotEncoder<Self> {
            let offset = self.offset + 102;
            storage_status_snapshot_codec::StorageStatusSnapshotEncoder::default()
                .wrap(self, offset)
        }

        /// VAR_DATA ENCODER - character encoding: 'UTF-8'
        #[inline]
        pub fn phase(&mut self, value: &str) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length].as_bytes());
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'UTF-8'
        #[inline]
        pub fn description(&mut self, value: &str) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length].as_bytes());
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'UTF-8'
        #[inline]
        pub fn summary(&mut self, value: &str) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length].as_bytes());
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'None'
        #[inline]
        pub fn request_bytes(&mut self, value: &[u8]) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length]);
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'None'
        #[inline]
        pub fn result_bytes(&mut self, value: &[u8]) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length]);
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'None'
        #[inline]
        pub fn accepted_bytes_before(&mut self, value: &[u8]) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length]);
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'None'
        #[inline]
        pub fn durable_bytes_before(&mut self, value: &[u8]) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length]);
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'None'
        #[inline]
        pub fn accepted_bytes_after(&mut self, value: &[u8]) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length]);
            self
        }

        /// VAR_DATA ENCODER - character encoding: 'None'
        #[inline]
        pub fn durable_bytes_after(&mut self, value: &[u8]) -> &mut Self {
            let limit = self.get_limit();
            let data_length = value.len().min((u16::MAX - 1) as usize);
            self.set_limit(limit + 2 + data_length);
            self.get_buf_mut().put_u16_at(limit, data_length as u16);
            self.get_buf_mut()
                .put_slice_at(limit + 2, &value[0..data_length]);
            self
        }
    }
} // end encoder

pub mod decoder {
    use super::*;
    use message_header_codec::*;

    #[derive(Clone, Copy, Debug, Default)]
    pub struct StorageTraceArtifactDecoder<'a> {
        buf: ReadBuf<'a>,
        initial_offset: usize,
        offset: usize,
        limit: usize,
        pub acting_block_length: u16,
        pub acting_version: u16,
    }

    impl ActingVersion for StorageTraceArtifactDecoder<'_> {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.acting_version
        }
    }

    impl<'a> Reader<'a> for StorageTraceArtifactDecoder<'a> {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            &self.buf
        }
    }

    impl<'a> Decoder<'a> for StorageTraceArtifactDecoder<'a> {
        #[inline]
        fn get_limit(&self) -> usize {
            self.limit
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.limit = limit;
        }
    }

    impl<'a> StorageTraceArtifactDecoder<'a> {
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
        pub fn artifact_schema_version(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn started_at_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 4)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn completed_at_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 12)
        }

        /// COMPOSITE DECODER
        #[inline]
        pub fn config_decoder(
            self,
        ) -> storage_config_snapshot_codec::StorageConfigSnapshotDecoder<Self> {
            let offset = self.offset + 20;
            storage_config_snapshot_codec::StorageConfigSnapshotDecoder::default()
                .wrap(self, offset)
        }

        /// COMPOSITE DECODER
        #[inline]
        pub fn runtime_decoder(self) -> runtime_metadata_codec::RuntimeMetadataDecoder<Self> {
            let offset = self.offset + 84;
            runtime_metadata_codec::RuntimeMetadataDecoder::default().wrap(self, offset)
        }

        /// GROUP DECODER (id=10)
        #[inline]
        pub fn steps_decoder(self) -> StepsDecoder<Self> {
            StepsDecoder::default().wrap(self)
        }

        /// VAR_DATA DECODER - character encoding: 'UTF-8'
        #[inline]
        pub fn scenario_decoder(&mut self) -> (usize, usize) {
            let offset = self.get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn scenario_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'UTF-8'
        #[inline]
        pub fn source_test_decoder(&mut self) -> (usize, usize) {
            let offset = self.get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn source_test_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'UTF-8'
        #[inline]
        pub fn provider_decoder(&mut self) -> (usize, usize) {
            let offset = self.get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn provider_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'UTF-8'
        #[inline]
        pub fn generator_decoder(&mut self) -> (usize, usize) {
            let offset = self.get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn generator_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }
    }

    #[derive(Debug, Default)]
    pub struct StepsDecoder<P> {
        parent: Option<P>,
        block_length: u16,
        count: u16,
        index: usize,
        offset: usize,
    }

    impl<'a, P> ActingVersion for StepsDecoder<P>
    where
        P: Reader<'a> + ActingVersion + Default,
    {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.parent.as_ref().unwrap().acting_version()
        }
    }

    impl<'a, P> Reader<'a> for StepsDecoder<P>
    where
        P: Reader<'a> + Default,
    {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            self.parent.as_ref().expect("parent missing").get_buf()
        }
    }

    impl<'a, P> Decoder<'a> for StepsDecoder<P>
    where
        P: Decoder<'a> + ActingVersion + Default,
    {
        #[inline]
        fn get_limit(&self) -> usize {
            self.parent.as_ref().expect("parent missing").get_limit()
        }

        #[inline]
        fn set_limit(&mut self, limit: usize) {
            self.parent
                .as_mut()
                .expect("parent missing")
                .set_limit(limit);
        }
    }

    impl<'a, P> StepsDecoder<P>
    where
        P: Decoder<'a> + ActingVersion + Default,
    {
        pub fn wrap(mut self, mut parent: P) -> Self {
            let initial_offset = parent.get_limit();
            let block_length = parent.get_buf().get_u16_at(initial_offset);
            let count = parent.get_buf().get_u16_at(initial_offset + 2);
            parent.set_limit(initial_offset + 4);
            self.parent = Some(parent);
            self.block_length = block_length;
            self.count = count;
            self.index = usize::MAX;
            self.offset = 0;
            self
        }

        /// group token - Token{signal=BEGIN_GROUP, name='steps', referencedName='null', description='null', packageName='null', id=10, version=0, deprecated=0, encodedLength=157, offset=169, componentTokenCount=145, encoding=Encoding{presence=REQUIRED, primitiveType=null, byteOrder=LITTLE_ENDIAN, minValue=null, maxValue=null, nullValue=null, constValue=null, characterEncoding='null', epoch='null', timeUnit=null, semanticType='null'}}
        #[inline]
        pub fn parent(&mut self) -> SbeResult<P> {
            self.parent.take().ok_or(SbeErr::ParentNotSet)
        }

        #[inline]
        pub fn acting_version(&mut self) -> u16 {
            self.parent.as_ref().unwrap().acting_version()
        }

        #[inline]
        pub fn count(&self) -> u16 {
            self.count
        }

        /// will return Some(current index) when successful otherwise None
        pub fn advance(&mut self) -> SbeResult<Option<usize>> {
            let index = self.index.wrapping_add(1);
            if index >= self.count as usize {
                return Ok(None);
            }
            if let Some(parent) = self.parent.as_mut() {
                self.offset = parent.get_limit();
                parent.set_limit(self.offset + self.block_length as usize);
                self.index = index;
                Ok(Some(index))
            } else {
                Err(SbeErr::ParentNotSet)
            }
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn sequence(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn started_at_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 4)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn completed_at_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 12)
        }

        /// REQUIRED enum
        #[inline]
        pub fn operation(&self) -> storage_operation::StorageOperation {
            self.get_buf().get_u8_at(self.offset + 20).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn outcome(&self) -> storage_outcome::StorageOutcome {
            self.get_buf().get_u8_at(self.offset + 21).into()
        }

        /// REQUIRED enum
        #[inline]
        pub fn certainty(&self) -> completion_certainty::CompletionCertainty {
            self.get_buf().get_u8_at(self.offset + 22).into()
        }

        /// primitive field - 'OPTIONAL' { null_value: '0xffffffffffffffff_u64' }
        #[inline]
        pub fn offset(&self) -> Option<u64> {
            let value = self.get_buf().get_u64_at(self.offset + 23);
            if value == 0xffffffffffffffff_u64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0xffffffffffffffff_u64' }
        #[inline]
        pub fn transfer_length(&self) -> Option<u64> {
            let value = self.get_buf().get_u64_at(self.offset + 31);
            if value == 0xffffffffffffffff_u64 {
                None
            } else {
                Some(value)
            }
        }

        /// primitive field - 'OPTIONAL' { null_value: '0xffffffffffffffff_u64' }
        #[inline]
        pub fn result_length(&self) -> Option<u64> {
            let value = self.get_buf().get_u64_at(self.offset + 39);
            if value == 0xffffffffffffffff_u64 {
                None
            } else {
                Some(value)
            }
        }

        /// COMPOSITE DECODER
        #[inline]
        pub fn before_decoder(
            self,
        ) -> storage_status_snapshot_codec::StorageStatusSnapshotDecoder<Self> {
            let offset = self.offset + 47;
            storage_status_snapshot_codec::StorageStatusSnapshotDecoder::default()
                .wrap(self, offset)
        }

        /// COMPOSITE DECODER
        #[inline]
        pub fn after_decoder(
            self,
        ) -> storage_status_snapshot_codec::StorageStatusSnapshotDecoder<Self> {
            let offset = self.offset + 102;
            storage_status_snapshot_codec::StorageStatusSnapshotDecoder::default()
                .wrap(self, offset)
        }

        /// VAR_DATA DECODER - character encoding: 'UTF-8'
        #[inline]
        pub fn phase_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn phase_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'UTF-8'
        #[inline]
        pub fn description_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn description_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'UTF-8'
        #[inline]
        pub fn summary_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn summary_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'None'
        #[inline]
        pub fn request_bytes_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn request_bytes_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'None'
        #[inline]
        pub fn result_bytes_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn result_bytes_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'None'
        #[inline]
        pub fn accepted_bytes_before_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn accepted_bytes_before_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'None'
        #[inline]
        pub fn durable_bytes_before_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn durable_bytes_before_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'None'
        #[inline]
        pub fn accepted_bytes_after_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn accepted_bytes_after_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }

        /// VAR_DATA DECODER - character encoding: 'None'
        #[inline]
        pub fn durable_bytes_after_decoder(&mut self) -> (usize, usize) {
            let offset = self.parent.as_ref().expect("parent missing").get_limit();
            let data_length = self.get_buf().get_u16_at(offset) as usize;
            self.parent
                .as_mut()
                .unwrap()
                .set_limit(offset + 2 + data_length);
            (offset + 2, data_length)
        }

        #[inline]
        pub fn durable_bytes_after_slice(&'a self, coordinates: (usize, usize)) -> &'a [u8] {
            debug_assert!(self.get_limit() >= coordinates.0 + coordinates.1);
            self.get_buf().get_slice_at(coordinates.0, coordinates.1)
        }
    }
} // end decoder
