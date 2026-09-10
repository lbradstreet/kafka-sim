use crate::{Error, Result};
use alloc::{rc::Rc, vec::Vec};
use core::cell::RefCell;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Compression {
    #[default]
    None,
    Zstd {
        level: u8,
    },
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ZstdConfig {
    pub level: u8,
    pub window_log: u32,
}
impl Default for ZstdConfig {
    fn default() -> Self {
        Self {
            level: 1,
            window_log: 20,
        }
    }
}
impl ZstdConfig {
    pub fn validate(self) -> Result<Self> {
        if !(1..=3).contains(&self.level) || !(10..=23).contains(&self.window_log) {
            Err(Error::InvalidConfig)
        } else {
            Ok(self)
        }
    }
}

/// Fixed-count, fixed-parameter contexts, warmed once at construction. A lease
/// returns its context on seal, failure, cancellation, or builder drop. This
/// owner-local type must remain on the actor or its chosen affine worker lane.
#[derive(Clone)]
pub struct CodecPool {
    inner: Rc<RefCell<Pool>>,
}
struct Pool {
    config: ZstdConfig,
    total: usize,
    workspace_bytes: usize,
    idle: Vec<Context>,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CodecPoolStatus {
    pub capacity: usize,
    pub available: usize,
    pub workspace_bytes: usize,
}
#[cfg(feature = "zstd")]
type Context = zstd_safe::CCtx<'static>;
#[cfg(not(feature = "zstd"))]
struct Context;
impl CodecPool {
    /// A zero-context pool supports uncompressed batches without zstd allocation.
    pub fn new(contexts: usize, config: ZstdConfig) -> Result<Self> {
        let config = config.validate()?;
        #[cfg(not(feature = "zstd"))]
        if contexts != 0 {
            return Err(Error::UnsupportedCompression);
        }
        let mut idle = Vec::new();
        idle.try_reserve_exact(contexts)
            .map_err(|_| Error::AllocationFailed)?;
        #[allow(unused_mut)]
        let mut workspace_bytes = 0usize;
        #[cfg(feature = "zstd")]
        for _ in 0..contexts {
            use zstd_safe::{
                CParameter as P, InBuffer, OutBuffer, zstd_sys::ZSTD_EndDirective::ZSTD_e_continue,
            };
            let mut context = Context::try_create().ok_or(Error::AllocationFailed)?;
            for parameter in [
                P::CompressionLevel(i32::from(config.level)),
                P::WindowLog(config.window_log),
                P::NbWorkers(0),
                P::ContentSizeFlag(false),
                P::ChecksumFlag(false),
            ] {
                context
                    .set_parameter(parameter)
                    .map_err(Error::CodecFailure)?;
            }
            context
                .set_pledged_src_size(None)
                .map_err(Error::CodecFailure)?;
            let mut scratch = [0; 256];
            let mut output = OutBuffer::around(&mut scratch[..]);
            let mut input = InBuffer::around(&[0]);
            context
                .compress_stream2(&mut output, &mut input, ZSTD_e_continue)
                .map_err(Error::CodecFailure)?;
            workspace_bytes = workspace_bytes
                .checked_add(context.sizeof())
                .ok_or(Error::LengthOverflow)?;
            context
                .reset(zstd_safe::ResetDirective::SessionOnly)
                .map_err(Error::CodecFailure)?;
            idle.push(context);
        }
        Ok(Self {
            inner: Rc::new(RefCell::new(Pool {
                config,
                total: contexts,
                workspace_bytes,
                idle,
            })),
        })
    }
    /// Actual context-slot backing bytes, excluding codec workspaces (reported
    /// by `status`) and Rc/control-block storage. Construction fixes capacity.
    pub fn metadata_capacity_bytes(&self) -> Result<usize> {
        self.inner
            .borrow()
            .idle
            .capacity()
            .checked_mul(core::mem::size_of::<Context>())
            .ok_or(Error::LengthOverflow)
    }
    pub fn status(&self) -> CodecPoolStatus {
        let pool = self.inner.borrow();
        CodecPoolStatus {
            capacity: pool.total,
            available: pool.idle.len(),
            workspace_bytes: pool.workspace_bytes,
        }
    }
    pub(crate) fn acquire(&mut self, level: u8) -> Result<Option<CodecLease>> {
        let mut pool = self.inner.borrow_mut();
        if level != pool.config.level {
            return Err(Error::CodecConfigurationMismatch);
        }
        Ok(pool.idle.pop().map(|context| CodecLease {
            context: Some(context),
            pool: self.clone(),
        }))
    }
}
pub(crate) struct CodecLease {
    context: Option<Context>,
    pool: CodecPool,
}
impl CodecLease {
    /// Exactly one compressor call, bounded by caller-provided input/output spans.
    pub(crate) fn step(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        end: bool,
    ) -> Result<(usize, usize, bool)> {
        #[cfg(feature = "zstd")]
        {
            use zstd_safe::{InBuffer, OutBuffer, zstd_sys::ZSTD_EndDirective::*};
            let mut input = InBuffer::around(input);
            let mut output = OutBuffer::around(output);
            let remaining = self
                .context
                .as_mut()
                .expect("live lease")
                .compress_stream2(
                    &mut output,
                    &mut input,
                    if end { ZSTD_e_end } else { ZSTD_e_continue },
                )
                .map_err(Error::CodecFailure)?;
            Ok((input.pos, output.pos(), remaining == 0))
        }
        #[cfg(not(feature = "zstd"))]
        {
            let _ = (input, output, end);
            Err(Error::UnsupportedCompression)
        }
    }
}
impl Drop for CodecLease {
    #[allow(unused_mut)]
    fn drop(&mut self) {
        if let Some(mut context) = self.context.take() {
            #[cfg(feature = "zstd")]
            {
                let _ = context.reset(zstd_safe::ResetDirective::SessionOnly);
            }
            self.pool.inner.borrow_mut().idle.push(context);
        }
    }
}

#[cfg(all(test, feature = "zstd"))]
mod tests {
    use super::*;
    #[test]
    fn warmed_workspace_is_a_bound_through_multiple_windows_and_resets() {
        for level in 1..=3 {
            for window_log in [10, 15, 18] {
                let mut pool = CodecPool::new(1, ZstdConfig { level, window_log }).unwrap();
                let reserved = pool.status().workspace_bytes;
                for pass in 0..3 {
                    let mut lease = pool.acquire(level).unwrap().unwrap();
                    let input = [pass; 4096];
                    let mut output = [0; 512];
                    for _ in 0..((2usize << window_log) / input.len()).max(1) {
                        let mut consumed = 0;
                        while consumed != input.len() {
                            consumed += lease
                                .step(&input[consumed..], &mut output, false)
                                .unwrap()
                                .0;
                            assert!(
                                lease.context.as_ref().unwrap().sizeof() <= reserved,
                                "level={level} window_log={window_log} pass={pass}"
                            );
                        }
                    }
                    while !lease.step(&[], &mut output, true).unwrap().2 {}
                    assert!(lease.context.as_ref().unwrap().sizeof() <= reserved);
                }
                assert_eq!(pool.status().available, 1);
            }
        }
    }
}
