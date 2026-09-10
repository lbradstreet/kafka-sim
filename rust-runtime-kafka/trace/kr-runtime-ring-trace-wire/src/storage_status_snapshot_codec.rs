use crate::*;

pub use decoder::StorageStatusSnapshotDecoder;
pub use encoder::StorageStatusSnapshotEncoder;

pub const ENCODED_LENGTH: usize = 55;

pub mod encoder {
    use super::*;

    #[derive(Debug, Default)]
    pub struct StorageStatusSnapshotEncoder<P> {
        parent: Option<P>,
        offset: usize,
    }

    impl<'a, P> Writer<'a> for StorageStatusSnapshotEncoder<P>
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

    impl<'a, P> StorageStatusSnapshotEncoder<P>
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

        /// REQUIRED enum
        #[inline]
        pub fn session(&mut self, value: session_state::SessionState) -> &mut Self {
            let offset = self.offset;
            self.get_buf_mut().put_u8_at(offset, value as u8);
            self
        }

        /// primitive field 'acceptedLen'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 1
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn accepted_len(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 1;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'durableLen'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 9
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn durable_len(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 9;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'fsyncGateVersion'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 17
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn fsync_gate_version(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 17;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'hasFsyncGatedData'
        /// - min value: 0
        /// - max value: 1
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 21
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn has_fsync_gated_data(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 21;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'inFlight'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 22
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn in_flight(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 22;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'inFlightLimit'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 30
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn in_flight_limit(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 30;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'pendingFaults'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 38
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn pending_faults(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 38;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'faultHits'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 46
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn fault_hits(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 46;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'closed'
        /// - min value: 0
        /// - max value: 1
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 54
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn closed(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 54;
            self.get_buf_mut().put_u8_at(offset, value);
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
    pub struct StorageStatusSnapshotDecoder<P> {
        parent: Option<P>,
        offset: usize,
    }

    impl<'a, P> ActingVersion for StorageStatusSnapshotDecoder<P>
    where
        P: Reader<'a> + ActingVersion + Default,
    {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.parent.as_ref().unwrap().acting_version()
        }
    }

    impl<'a, P> Reader<'a> for StorageStatusSnapshotDecoder<P>
    where
        P: Reader<'a> + Default,
    {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            self.parent.as_ref().expect("parent missing").get_buf()
        }
    }

    impl<'a, P> StorageStatusSnapshotDecoder<P>
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

        /// REQUIRED enum
        #[inline]
        pub fn session(&self) -> session_state::SessionState {
            self.get_buf().get_u8_at(self.offset).into()
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn accepted_len(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 1)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn durable_len(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 9)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn fsync_gate_version(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 17)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn has_fsync_gated_data(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 21)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn in_flight(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 22)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn in_flight_limit(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 30)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn pending_faults(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 38)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn fault_hits(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 46)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn closed(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 54)
        }
    }
} // end decoder mod
