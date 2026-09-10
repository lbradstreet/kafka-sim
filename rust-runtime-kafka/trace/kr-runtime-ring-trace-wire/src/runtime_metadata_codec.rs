use crate::*;

pub use decoder::RuntimeMetadataDecoder;
pub use encoder::RuntimeMetadataEncoder;

pub const ENCODED_LENGTH: usize = 85;

pub mod encoder {
    use super::*;

    #[derive(Debug, Default)]
    pub struct RuntimeMetadataEncoder<P> {
        parent: Option<P>,
        offset: usize,
    }

    impl<'a, P> Writer<'a> for RuntimeMetadataEncoder<P>
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

    impl<'a, P> RuntimeMetadataEncoder<P>
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

        /// primitive field 'reproductionSchema'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 0
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn reproduction_schema(&mut self, value: u32) -> &mut Self {
            let offset = self.offset;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'checkpointSchema'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 4
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn checkpoint_schema(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 4;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'rngVersion'
        /// - min value: 0
        /// - max value: 4294967294
        /// - null value: 0xffffffff_u32
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 8
        /// - encodedLength: 4
        /// - version: 0
        #[inline]
        pub fn rng_version(&mut self, value: u32) -> &mut Self {
            let offset = self.offset + 8;
            self.get_buf_mut().put_u32_at(offset, value);
            self
        }

        /// primitive field 'stopped'
        /// - min value: 0
        /// - max value: 1
        /// - null value: 0xff_u8
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 12
        /// - encodedLength: 1
        /// - version: 0
        #[inline]
        pub fn stopped(&mut self, value: u8) -> &mut Self {
            let offset = self.offset + 12;
            self.get_buf_mut().put_u8_at(offset, value);
            self
        }

        /// primitive field 'seed'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 13
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn seed(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 13;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'nowNs'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 21
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn now_ns(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 21;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'totalSteps'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 29
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn total_steps(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 29;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'nextEnqueueSequence'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 37
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn next_enqueue_sequence(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 37;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'nextTimerSequence'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 45
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn next_timer_sequence(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 45;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'nextTimerId'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 53
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn next_timer_id(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 53;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'readyTasks'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 61
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn ready_tasks(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 61;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'liveTimers'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 69
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn live_timers(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 69;
            self.get_buf_mut().put_u64_at(offset, value);
            self
        }

        /// primitive field 'liveTasks'
        /// - min value: 0
        /// - max value: -2
        /// - null value: 0xffffffffffffffff_u64
        /// - characterEncoding: null
        /// - semanticType: null
        /// - encodedOffset: 77
        /// - encodedLength: 8
        /// - version: 0
        #[inline]
        pub fn live_tasks(&mut self, value: u64) -> &mut Self {
            let offset = self.offset + 77;
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
    pub struct RuntimeMetadataDecoder<P> {
        parent: Option<P>,
        offset: usize,
    }

    impl<'a, P> ActingVersion for RuntimeMetadataDecoder<P>
    where
        P: Reader<'a> + ActingVersion + Default,
    {
        #[inline]
        fn acting_version(&self) -> u16 {
            self.parent.as_ref().unwrap().acting_version()
        }
    }

    impl<'a, P> Reader<'a> for RuntimeMetadataDecoder<P>
    where
        P: Reader<'a> + Default,
    {
        #[inline]
        fn get_buf(&self) -> &ReadBuf<'a> {
            self.parent.as_ref().expect("parent missing").get_buf()
        }
    }

    impl<'a, P> RuntimeMetadataDecoder<P>
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
        pub fn reproduction_schema(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn checkpoint_schema(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 4)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn rng_version(&self) -> u32 {
            self.get_buf().get_u32_at(self.offset + 8)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn stopped(&self) -> u8 {
            self.get_buf().get_u8_at(self.offset + 12)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn seed(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 13)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn now_ns(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 21)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn total_steps(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 29)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn next_enqueue_sequence(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 37)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn next_timer_sequence(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 45)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn next_timer_id(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 53)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn ready_tasks(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 61)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn live_timers(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 69)
        }

        /// primitive field - 'REQUIRED'
        #[inline]
        pub fn live_tasks(&self) -> u64 {
            self.get_buf().get_u64_at(self.offset + 77)
        }
    }
} // end decoder mod
