use crate::{
    BATCH_HEADER_BYTES, CodecPool, Compression, Error, OutputPool, OwnedRecord, Result,
    SharedBytes,
    codec::CodecLease,
    output::{ChunkWriter, OutputLease},
    record::RecordCursor,
};
use alloc::{collections::VecDeque, vec::Vec};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchConfig {
    pub raw_limit: u32,
    pub output_limit: u32,
    pub chunk_bytes: u32,
    pub progressive_threshold: u32,
}
impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            raw_limit: 1024 * 1024,
            output_limit: 1024 * 1024,
            chunk_bytes: 512 * 1024,
            progressive_threshold: 16 * 1024,
        }
    }
}
impl BatchConfig {
    pub fn validate(self) -> Result<Self> {
        if self.raw_limit == 0
            || self.raw_limit > i32::MAX as u32
            || self.output_limit == 0
            || self.output_limit > i32::MAX as u32 - 49
            || self.chunk_bytes < BATCH_HEADER_BYTES as u32
            || self.chunk_bytes > self.envelope_bytes()
            || self.progressive_threshold > self.raw_limit
        {
            return Err(Error::InvalidConfig);
        }
        Ok(self)
    }
    /// Maximum compressed payload plus fixed header, reserved before activation.
    pub fn envelope_bytes(self) -> u32 {
        self.output_limit.saturating_add(BATCH_HEADER_BYTES as u32)
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BatchState {
    Deferred,
    Progressive,
    Sealing,
    Sealed,
    Taken,
    Failed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EncodeBudget {
    pub input_bytes: usize,
    pub codec_calls: usize,
}
impl Default for EncodeBudget {
    fn default() -> Self {
        Self {
            input_bytes: 16 * 1024,
            codec_calls: 128,
        }
    }
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EncodeProgress {
    pub input_bytes: usize,
    pub codec_calls: usize,
    /// Actual compressor end/probe calls, included in `codec_calls`. They can
    /// perform sealing work while consuming zero additional record input bytes.
    pub seal_calls: usize,
    pub records_released: u32,
    pub sealed: bool,
    pub waiting_for_context: bool,
    pub waiting_for_output: bool,
}
struct Pending {
    record: OwnedRecord,
    cursor: RecordCursor,
}

/// Work performed while abandoning this batch owner's references. Output pool
/// reservations can outlive `done` while a provider retains finalized chunks.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AbortProgress {
    pub records_released: usize,
    pub chunks_released: usize,
    pub done: bool,
}

/// A terminal payload with incremental destruction. Converting into this owner
/// prevents any further encoding or finalization without dropping queued input.
/// Drop still releases everything; cooperative owners retain it and call
/// `abort_step` until done to bound each poll's destruction work.
pub struct BatchAbort {
    pending: VecDeque<Pending>,
    writer: Option<ChunkWriter>,
    codec: Option<CodecLease>,
    chunks: Vec<SharedBytes>,
    lease: Option<OutputLease>,
}
impl BatchAbort {
    /// Observed descriptor/chunk backing bytes; excludes referenced pool storage
    /// and Arc/Rc control blocks. This diagnostic visits retained record headers.
    pub fn metadata_capacity_bytes(&self) -> Result<usize> {
        metadata_capacity(&self.pending, self.writer.as_ref(), &self.chunks)
    }
    /// Drops at most the requested input records and output references. A single
    /// record's header count is bounded by the caller's admission contract. A
    /// zero/zero budget releases nothing, including the codec context. Otherwise
    /// the one codec context is returned without additional compression work.
    pub fn abort_step(&mut self, max_records: usize, max_chunks: usize) -> AbortProgress {
        let mut result = AbortProgress::default();
        if max_records != 0 || max_chunks != 0 {
            self.codec = None;
        }
        while result.records_released < max_records {
            let Some(record) = self.pending.pop_front() else {
                break;
            };
            drop(record);
            result.records_released += 1;
        }
        if let Some(writer) = &mut self.writer {
            result.chunks_released = writer.abort_chunks(max_chunks);
            if writer.allocated() == 0 {
                self.writer = None;
            }
        }
        while result.chunks_released < max_chunks {
            let Some(chunk) = self.chunks.pop() else {
                break;
            };
            drop(chunk);
            result.chunks_released += 1;
        }
        if self.chunks.is_empty() {
            self.lease = None;
        }
        result.done = self.pending.is_empty()
            && self.writer.is_none()
            && self.codec.is_none()
            && self.chunks.is_empty();
        result
    }
}

/// A passive, bounded encoder. `push` retains input spans. `progress` drops a
/// descriptor only after its last byte was consumed (reported in FIFO order as
/// `records_released`). Seal releases the compressor before identity assignment.
/// Dropping or an encoding failure releases all remaining input and context
/// leases. The owner translates failures to definitive pre-transmission outcomes.
pub struct RecordBatchBuilder {
    config: BatchConfig,
    compression: Compression,
    pool: OutputPool,
    state: BatchState,
    pending: VecDeque<Pending>,
    writer: Option<ChunkWriter>,
    codec: Option<CodecLease>,
    raw_bytes: u32,
    count: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    seal_requested: bool,
    compact_tail: bool,
}
impl RecordBatchBuilder {
    /// Observed descriptor/chunk backing bytes. Includes owned header vectors;
    /// excludes shared pool storage and Arc/Rc control blocks. This diagnostic
    /// visits retained records, and is not a bounded encoding work operation.
    pub fn metadata_capacity_bytes(&self) -> Result<usize> {
        metadata_capacity(&self.pending, self.writer.as_ref(), &Vec::new())
    }
    pub fn new(
        config: BatchConfig,
        compression: Compression,
        output_pool: OutputPool,
    ) -> Result<Self> {
        let config = config.validate()?;
        if let Compression::Zstd { level } = compression {
            if !(1..=3).contains(&level) {
                return Err(Error::InvalidConfig);
            }
            #[cfg(not(feature = "zstd"))]
            return Err(Error::UnsupportedCompression);
        }
        Ok(Self {
            config,
            compression,
            pool: output_pool,
            state: BatchState::Deferred,
            pending: VecDeque::new(),
            writer: None,
            codec: None,
            raw_bytes: 0,
            count: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            seal_requested: false,
            compact_tail: false,
        })
    }
    pub fn state(&self) -> BatchState {
        self.state
    }
    /// Enable a best-effort copy of at most 4 KiB on final seal. It can release
    /// a much larger backing allocation. Scratch stays inside the reserved
    /// output envelope; exhaustion skips compaction without delaying sealing.
    pub fn enable_tail_compaction(&mut self) {
        self.compact_tail = true;
    }
    /// Whether this builder currently retains a live compressor context.
    #[must_use]
    pub fn holds_codec_context(&self) -> bool {
        self.codec.is_some()
    }
    pub fn base_timestamp(&self) -> Option<i64> {
        (self.count != 0).then_some(self.base_timestamp)
    }
    pub fn raw_bytes(&self) -> u32 {
        self.raw_bytes
    }
    pub fn record_count(&self) -> i32 {
        self.count
    }
    pub fn retained_records(&self) -> usize {
        self.pending.len()
    }
    pub fn output_bytes(&self) -> usize {
        self.writer.as_ref().map_or(0, ChunkWriter::len)
    }
    pub fn allocated_output_bytes(&self) -> usize {
        self.writer.as_ref().map_or(0, ChunkWriter::allocated)
    }
    /// Rejects before modifying the batch if the descriptor cannot fit. A caller
    /// may retry the same shared input in a fresh batch; there is no hidden split.
    pub fn push(&mut self, record: OwnedRecord) -> Result<u32> {
        if self.seal_requested
            || !matches!(self.state, BatchState::Deferred | BatchState::Progressive)
        {
            return Err(Error::Closed);
        }
        let base = if self.count == 0 {
            record.timestamp
        } else {
            self.base_timestamp
        };
        let (size, delta) = record.size(base, self.count)?;
        let raw = self
            .raw_bytes
            .checked_add(size)
            .ok_or(Error::LengthOverflow)?;
        if raw > self.config.raw_limit {
            return Err(Error::RawTooLarge);
        }
        let count = self.count.checked_add(1).ok_or(Error::LengthOverflow)?;
        self.pending
            .try_reserve(1)
            .map_err(|_| Error::AllocationFailed)?;
        if self.count == 0 {
            self.base_timestamp = base;
            self.max_timestamp = base;
        }
        self.max_timestamp = self.max_timestamp.max(record.timestamp);
        self.pending.push_back(Pending {
            record,
            cursor: RecordCursor::new(size, delta, self.count),
        });
        self.count = count;
        self.raw_bytes = raw;
        Ok(size)
    }
    pub fn request_seal(&mut self) -> Result<()> {
        if matches!(self.state, BatchState::Failed | BatchState::Taken) {
            return Err(Error::Closed);
        }
        if self.count == 0 {
            return Err(Error::EmptyBatch);
        }
        self.seal_requested = true;
        if self.state != BatchState::Sealed {
            self.state = BatchState::Sealing;
        }
        Ok(())
    }
    pub fn progress(
        &mut self,
        codecs: &mut CodecPool,
        budget: EncodeBudget,
    ) -> Result<EncodeProgress> {
        let result = self.progress_inner(codecs, budget);
        if result.is_err() {
            self.state = BatchState::Failed;
            self.pending.clear();
            self.writer = None;
            self.codec = None;
        }
        result
    }
    /// The encoding contract of `progress`, but an error fences the builder
    /// while retaining all remaining input/output/context owners. Cooperative
    /// actors use `into_abort` afterwards to release them under a work budget.
    pub fn progress_retained(
        &mut self,
        codecs: &mut CodecPool,
        budget: EncodeBudget,
    ) -> Result<EncodeProgress> {
        let result = self.progress_inner(codecs, budget);
        if result.is_err() {
            self.state = BatchState::Failed;
        }
        result
    }
    /// Fences further work without destroying any queued records or chunks.
    pub fn into_abort(self) -> BatchAbort {
        BatchAbort {
            pending: self.pending,
            writer: self.writer,
            codec: self.codec,
            chunks: Vec::new(),
            lease: None,
        }
    }
    fn progress_inner(
        &mut self,
        codecs: &mut CodecPool,
        budget: EncodeBudget,
    ) -> Result<EncodeProgress> {
        let mut progress = EncodeProgress::default();
        if self.state == BatchState::Failed {
            return Err(Error::Failed);
        }
        if self.state == BatchState::Taken {
            return Err(Error::Closed);
        }
        if self.state == BatchState::Sealed {
            progress.sealed = true;
            return Ok(progress);
        }
        if budget.input_bytes == 0 || budget.codec_calls == 0 || self.count == 0 {
            return Ok(progress);
        }
        if self.writer.is_none() {
            if !self.seal_requested && self.raw_bytes < self.config.progressive_threshold {
                return Ok(progress);
            }
            let codec = match self.compression {
                Compression::None => None,
                Compression::Zstd { level } => match codecs.acquire(level)? {
                    Some(codec) => Some(codec),
                    None => {
                        progress.waiting_for_context = true;
                        return Ok(progress);
                    }
                },
            };
            let writer = match ChunkWriter::new(
                &self.pool,
                self.config.envelope_bytes() as usize,
                self.config.chunk_bytes as usize,
            ) {
                Ok(writer) => writer,
                Err(Error::OutputExhausted) => {
                    progress.waiting_for_output = true;
                    return Ok(progress);
                }
                Err(error) => return Err(error),
            };
            self.codec = codec;
            self.writer = Some(writer);
            if !self.seal_requested {
                self.state = BatchState::Progressive;
            }
        }
        let writer = self.writer.as_mut().expect("activated");
        let mut scratch = [0; 64];
        while progress.input_bytes < budget.input_bytes && progress.codec_calls < budget.codec_calls
        {
            let Some(pending) = self.pending.front_mut() else {
                break;
            };
            let Some(segment) = pending.cursor.segment(&pending.record, &mut scratch) else {
                self.pending.pop_front();
                progress.records_released += 1;
                continue;
            };
            if pending.cursor.position == segment.len() {
                pending.cursor.advance_segment();
                continue;
            }
            let available = &segment[pending.cursor.position..];
            let input = &available[..available
                .len()
                .min(budget.input_bytes - progress.input_bytes)];
            let output = match writer.writable() {
                Ok(output) => output,
                Err(Error::OutputExhausted) => {
                    progress.waiting_for_output = true;
                    return Ok(progress);
                }
                Err(error) => return Err(error),
            };
            let used = if let Some(codec) = &mut self.codec {
                let (used, written, _) = codec.step(input, output, false)?;
                writer.advance(written);
                used
            } else {
                let used = input.len().min(output.len());
                output[..used].copy_from_slice(&input[..used]);
                writer.advance(used);
                used
            };
            progress.codec_calls += 1;
            progress.input_bytes += used;
            pending.cursor.position += used;
        }
        // Retire a record even if its final payload exactly consumed the quota.
        // Empty header suffixes require no codec work, and this loop is bounded
        // by the descriptor whose final byte was just consumed.
        while let Some(pending) = self.pending.front_mut() {
            match pending.cursor.segment(&pending.record, &mut scratch) {
                None => {
                    self.pending.pop_front();
                    progress.records_released += 1;
                }
                Some(span) if span.len() == pending.cursor.position => {
                    pending.cursor.advance_segment()
                }
                Some(_) => break,
            }
        }
        if self.seal_requested && self.pending.is_empty() {
            let mut complete = self.codec.is_none();
            while !complete && progress.codec_calls < budget.codec_calls {
                // A full envelope may still finish with zero output. Probe with a
                // one-byte stack buffer and fail if zstd emits that extra byte.
                match writer.writable() {
                    Ok(output) => {
                        let (_, written, done) =
                            self.codec
                                .as_mut()
                                .expect("codec")
                                .step(&[], output, true)?;
                        writer.advance(written);
                        complete = done;
                    }
                    Err(Error::CompressedTooLarge) => {
                        let mut extra = [0; 1];
                        let (_, written, done) =
                            self.codec
                                .as_mut()
                                .expect("codec")
                                .step(&[], &mut extra, true)?;
                        if written != 0 {
                            return Err(Error::CompressedTooLarge);
                        }
                        complete = done;
                    }
                    Err(Error::OutputExhausted) => {
                        progress.waiting_for_output = true;
                        return Ok(progress);
                    }
                    Err(error) => return Err(error),
                }
                progress.codec_calls += 1;
                progress.seal_calls += 1;
            }
            if complete {
                if self.compact_tail {
                    writer.compact_tail(4096);
                }
                writer.lease.shrink(writer.allocated());
                self.codec = None;
                self.state = BatchState::Sealed;
                progress.sealed = true;
            }
        }
        Ok(progress)
    }
    /// Consumes the finished output exactly once. The resulting batch carries no
    /// input descriptors or compressor context and is still CRC/identity-less.
    pub fn take_sealed(&mut self) -> Option<SealedBatch> {
        if self.state != BatchState::Sealed {
            return None;
        }
        self.state = BatchState::Taken;
        Some(SealedBatch {
            writer: self.writer.take().expect("sealed writer"),
            compression: self.compression,
            count: self.count,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            raw_bytes: self.raw_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Identity {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}
impl Identity {
    fn validate(self) -> Result<Self> {
        if self.producer_id < 0 || self.producer_epoch < 0 || self.base_sequence < 0 {
            Err(Error::InvalidIdentity)
        } else {
            Ok(self)
        }
    }
}
pub struct SealedBatch {
    writer: ChunkWriter,
    compression: Compression,
    count: i32,
    base_timestamp: i64,
    max_timestamp: i64,
    raw_bytes: u32,
}
impl SealedBatch {
    /// Observed chunk-vector backing, excluding the separately reported pool.
    pub fn metadata_capacity_bytes(&self) -> Result<usize> {
        self.writer.metadata_capacity_bytes()
    }
    pub fn chunk_count(&self) -> usize {
        self.writer.chunk_count()
    }
    /// Retains the output owner for bounded cooperative destruction.
    pub fn into_abort(self) -> BatchAbort {
        BatchAbort {
            pending: VecDeque::new(),
            writer: Some(self.writer),
            codec: None,
            chunks: Vec::new(),
            lease: None,
        }
    }
    pub fn wire_bytes(&self) -> usize {
        self.writer.len()
    }
    pub fn record_count(&self) -> i32 {
        self.count
    }
    pub fn raw_bytes(&self) -> u32 {
        self.raw_bytes
    }
    pub fn finalize(mut self, identity: Identity) -> Result<FinalizedBatch> {
        let identity = identity.validate()?;
        let len = self.writer.len();
        let header = self.writer.first_mut();
        header[..8].copy_from_slice(&0i64.to_be_bytes());
        header[8..12].copy_from_slice(
            &i32::try_from(len - 12)
                .map_err(|_| Error::LengthOverflow)?
                .to_be_bytes(),
        );
        header[12..16].copy_from_slice(&(-1i32).to_be_bytes());
        header[16] = 2;
        let attributes: i16 = match self.compression {
            Compression::None => 0,
            Compression::Zstd { .. } => 4,
        };
        header[21..23].copy_from_slice(&attributes.to_be_bytes());
        header[23..27].copy_from_slice(&(self.count - 1).to_be_bytes());
        header[27..35].copy_from_slice(&self.base_timestamp.to_be_bytes());
        header[35..43].copy_from_slice(&self.max_timestamp.to_be_bytes());
        write_identity(header, identity);
        header[57..61].copy_from_slice(&self.count.to_be_bytes());
        let crc = self.writer.crc();
        self.writer.first_mut()[17..21].copy_from_slice(&crc.to_be_bytes());
        let (chunks, lease) = self.writer.freeze();
        Ok(FinalizedBatch {
            chunks,
            lease,
            identity,
            transmitted: false,
            len,
            count: self.count,
            raw_bytes: self.raw_bytes,
        })
    }
}
/// Immutable retry representation. Cloning chunks preserves allocation identity
/// for ordinary retries. The only mutation is explicit untransmitted epoch reset,
/// which requires all previously built request plans to have released their views.
pub struct FinalizedBatch {
    chunks: Vec<SharedBytes>,
    lease: OutputLease,
    identity: Identity,
    transmitted: bool,
    len: usize,
    count: i32,
    raw_bytes: u32,
}
impl FinalizedBatch {
    /// Observed chunk-vector backing, excluding the separately reported pool.
    pub fn metadata_capacity_bytes(&self) -> Result<usize> {
        self.chunks
            .capacity()
            .checked_mul(core::mem::size_of::<SharedBytes>())
            .ok_or(Error::LengthOverflow)
    }
    /// Retains immutable retry chunks for bounded cooperative destruction.
    /// Provider references continue holding the output pool reservation.
    pub fn into_abort(self) -> BatchAbort {
        BatchAbort {
            pending: VecDeque::new(),
            writer: None,
            codec: None,
            chunks: self.chunks,
            lease: Some(self.lease),
        }
    }
    pub fn chunks(&self) -> &[SharedBytes] {
        &self.chunks
    }
    pub fn wire_bytes(&self) -> usize {
        self.len
    }
    pub fn record_count(&self) -> i32 {
        self.count
    }
    pub fn raw_bytes(&self) -> u32 {
        self.raw_bytes
    }
    pub fn identity(&self) -> Identity {
        self.identity
    }
    pub fn transmitted(&self) -> bool {
        self.transmitted
    }
    pub fn mark_transmitted(&mut self) {
        self.transmitted = true;
    }
    pub fn refinalize(&mut self, identity: Identity) -> Result<()> {
        let identity = identity.validate()?;
        if identity == self.identity {
            return Ok(());
        }
        if self.transmitted {
            return Err(Error::Transmitted);
        }
        (|| {
            let header = self.chunks[0].try_as_mut().ok_or(Error::SharedOutput)?;
            write_identity(header, identity);
            let mut crc = !0;
            for (index, chunk) in self.chunks.iter().enumerate() {
                crc = crate::record::crc_update(
                    crc,
                    &chunk.as_slice()[if index == 0 { 21 } else { 0 }..],
                );
            }
            self.chunks[0].try_as_mut().expect("unique header")[17..21]
                .copy_from_slice(&(!crc).to_be_bytes());
            self.identity = identity;
            Ok(())
        })()
    }
}
fn write_identity(header: &mut [u8], identity: Identity) {
    header[43..51].copy_from_slice(&identity.producer_id.to_be_bytes());
    header[51..53].copy_from_slice(&identity.producer_epoch.to_be_bytes());
    header[53..57].copy_from_slice(&identity.base_sequence.to_be_bytes());
}

fn metadata_capacity(
    pending: &VecDeque<Pending>,
    writer: Option<&ChunkWriter>,
    chunks: &Vec<SharedBytes>,
) -> Result<usize> {
    let mut bytes = pending
        .capacity()
        .checked_mul(core::mem::size_of::<Pending>())
        .ok_or(Error::LengthOverflow)?;
    for record in pending {
        bytes = bytes
            .checked_add(
                record
                    .record
                    .headers
                    .capacity()
                    .checked_mul(core::mem::size_of::<crate::OwnedHeader>())
                    .ok_or(Error::LengthOverflow)?,
            )
            .ok_or(Error::LengthOverflow)?;
    }
    bytes = bytes
        .checked_add(
            chunks
                .capacity()
                .checked_mul(core::mem::size_of::<SharedBytes>())
                .ok_or(Error::LengthOverflow)?,
        )
        .ok_or(Error::LengthOverflow)?;
    bytes
        .checked_add(writer.map_or(Ok(0), ChunkWriter::metadata_capacity_bytes)?)
        .ok_or(Error::LengthOverflow)
}
