use crate::*;

pub use decoder::StorageConfigSnapshotDecoder;
pub use encoder::StorageConfigSnapshotEncoder;

pub const ENCODED_LENGTH: usize = 64;

pub mod encoder {
    use super::*;

    #[derive(Debug, Default)]
    pub struct StorageConfigSnapshotEncoder<P> {
        parent: Option<P>,
        offset: usize,
    }

    impl<'a, P> Writer<'a> for StorageConfigSnapshotEncoder<P>
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

    impl<'a, P> StorageConfigSnapshotEncoder<P>
    where
        P: Writer<'a> + Default,
    {
        pub fn wrap(mut self, parent: P, offset: usize) -> Self {
            self.parent = Some(parent);
            self.offset = offset;
            self
        }

        /// parent fns
        #[inline]
        pub fn parent(&mut self) -> SbeResult<P> {
            self.parent.take().ok_or(SbeErr::ParentNotSet)
        }

        /// primitive field 'maxFileBytes'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_file_bytes(&mut self, value: u64) -> &mut Self {
            let offset = self.offset;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxReadBytes'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 8
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_read_bytes(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 8;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxWriteBytes'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 16
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_write_bytes(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 16;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxReadChunk'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 24
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_read_chunk(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 24;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxWriteChunk'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 32
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_write_chunk(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 32;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxInFlight'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 40
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_in_flight(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 40;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'maxScriptedFaults'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 48
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn max_scripted_faults(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 48;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'defaultLatencyNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 56
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn default_latency_ns(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 56;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// Set all optional fields to their null values.
        #[inline]
        pub fn nullify_optional_fields(&mut self) -> &mut Self {
            self
        }
    }
} // end encoder mod

pub mod decoder {
    use super::*;

    #[derive(Debug, Default)]
    pub struct StorageConfigSnapshotDecoder<P> {
        parent: Option<P>,
        offset: usize,
    }

    impl<'a, P> ActingVersion for StorageConfigSnapshotDecoder<P>
    where
        P: Reader<'a> + ActingVersion + Default,
    {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.parent.as_ref().unwrap().acting_version()
        }
    }

    impl<'a, P> Reader<'a> for StorageConfigSnapshotDecoder<P>
    where
        P: Reader<'a> + Default,
    {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            self.parent.as_ref().expect("parent missing").get_buf()
        }
    }

    impl<'a, P> StorageConfigSnapshotDecoder<P>
    where
        P: Reader<'a> + Default,
    {
        pub fn wrap(mut self, parent: P, offset: usize) -> Self {
            self.parent = Some(parent);
            self.offset = offset;
            self
        }

        #[inline]
        pub fn parent(&mut self) -> SbeResult<P> {
            self.parent.take().ok_or(SbeErr::ParentNotSet)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_file_bytes(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_read_bytes(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 8)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_write_bytes(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 16)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_read_chunk(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 24)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_write_chunk(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 32)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_in_flight(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 40)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn max_scripted_faults(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 48)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn default_latency_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 56)
        }
    }
} // end decoder mod
