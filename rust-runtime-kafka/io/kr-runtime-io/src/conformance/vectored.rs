//! Reusable vectored checks for all owned stream providers.
use super::{read_exact, write_all};
use crate::network::{
    ByteStreamSubmit, ByteStreamVectoredSubmit, ColdStream, NetworkError, NetworkFailure,
    ReadRequest, ReadResult, SharedBytes, VectoredWriteFailure, VectoredWriteRequest,
    VectoredWriteResult, WriteRequest, WriteSegment,
};
use kr_runtime::{CompletionCertainty, CompletionResult};
use std::{
    future::{Future, poll_fn},
    pin::Pin,
    task::Poll,
};

fn segment(bytes: &SharedBytes, start: u32, end: u32) -> WriteSegment {
    WriteSegment {
        bytes: bytes.clone(),
        range: start..end,
    }
}

async fn check_payloads<W, WF, R, RF>(max_segments: usize, write: W, read: R) -> Result<(), String>
where
    W: Fn(VectoredWriteRequest) -> WF,
    WF: Future<Output = CompletionResult<VectoredWriteResult, VectoredWriteFailure>>,
    R: Fn(ReadRequest) -> RF,
    RF: Future<Output = CompletionResult<ReadResult, NetworkFailure>>,
{
    if !(3..=65_536).contains(&max_segments) {
        return Err("conformance requires a bounded segment cap >= 3".into());
    }
    let owner = SharedBytes::from(b"_abcdef_".to_vec());
    let mut invalid_requests = vec![
        vec![],
        vec![segment(&owner, 3, 2)],
        vec![segment(&owner, 0, 9)],
        vec![segment(&owner, 0, 0)],
        vec![segment(&owner, 0, 1); max_segments + 1],
    ];
    let mut oversized_storage = Vec::with_capacity(max_segments + 1);
    oversized_storage.push(segment(&owner, 0, 1));
    invalid_requests.push(oversized_storage);
    for segments in invalid_requests {
        let original_ptr = segments.as_ptr();
        let original_capacity = segments.capacity();
        let expected: Vec<_> = segments
            .iter()
            .map(|s| (s.bytes.as_ptr(), s.range.clone()))
            .collect();
        match write(VectoredWriteRequest { segments }).await {
            Err(error) => {
                if error.certainty() != CompletionCertainty::NotApplied {
                    return Err(format!("invalid request certainty: {error}"));
                }
                let failure = error.into_parts().1;
                if !matches!(failure.error, NetworkError::InvalidRequest { .. })
                    || failure.bytes_transferred != 0
                {
                    return Err(format!("invalid request failure: {failure:?}"));
                }
                let actual: Vec<_> = failure
                    .segments
                    .iter()
                    .map(|s| (s.bytes.as_ptr(), s.range.clone()))
                    .collect();
                if actual != expected
                    || failure.segments.as_ptr() != original_ptr
                    || failure.segments.capacity() != original_capacity
                {
                    return Err("rejection replaced segment storage or a backing allocation".into());
                }
            }
            Ok(result) => return Err(format!("invalid request succeeded: {result:?}")),
        }
    }
    if owner.strong_count() != 1 {
        return Err(
            "invalid request retained an allocation after returned ownership was dropped".into(),
        );
    }

    // The reference byte sequence is independent of segment slicing/progress code.
    let expected = b"abcdef";
    let mut segments = vec![
        segment(&owner, 1, 2),
        segment(&owner, 2, 5),
        segment(&owner, 5, 7),
    ];
    let mut consumed = 0usize;
    while consumed < expected.len() {
        let pointer = segments.as_ptr();
        let ranges: Vec<_> = segments.iter().map(|s| s.range.clone()).collect();
        let result = write(VectoredWriteRequest { segments })
            .await
            .map_err(|e| format!("vectored write failed: {e}"))?;
        if result.bytes_written == 0 || result.bytes_written > expected.len() - consumed {
            return Err(format!("invalid prefix progress {}", result.bytes_written));
        }
        if result.segments.as_ptr() != pointer
            || result.segments.len() != ranges.len()
            || !result
                .segments
                .iter()
                .zip(ranges)
                .all(|(s, range)| s.range == range && s.bytes.shares_allocation(&owner))
        {
            return Err("completion replaced or mutated segment ownership".into());
        }
        let actual = read_exact(&read, result.bytes_written).await?;
        if actual != expected[consumed..consumed + result.bytes_written] {
            return Err(format!(
                "segment order/progress mismatch at {consumed}: {actual:?}"
            ));
        }
        consumed += result.bytes_written;
        let mut prefix = result.bytes_written;
        segments = result.segments;
        while let Some(first) = segments.first_mut() {
            let len = (first.range.end - first.range.start) as usize;
            if prefix < len {
                first.range.start += u32::try_from(prefix).map_err(|_| "test progress overflow")?;
                break;
            }
            prefix -= len;
            segments.remove(0);
        }
    }
    drop(segments);
    if owner.strong_count() != 1 {
        return Err("terminal completion leaked a shared allocation".into());
    }
    Ok(())
}

/// Checks range/capacity rejection, exact ownership, ordered prefixes including
/// partial segments, abandonment, and close rejection on a fresh connected pair.
///
/// Configure operation bytes and directional capacity to at least 16 bytes.
/// Runs unchanged on deterministic and host providers. To force partial writes,
/// configure their test transfer cap below six bytes. Pair with
/// [`check_blocked_vectored_stream_provider`] for retained blocked operations.
///
/// # Errors
/// Returns the first observed contract violation.
pub async fn check_vectored_stream_provider<L: ByteStreamVectoredSubmit, R: ByteStreamSubmit>(
    left: &L,
    right: &R,
) -> Result<(), String> {
    check_payloads(
        left.max_segments(),
        |request| left.submit_write_vectored(request),
        |request| right.submit_read(request),
    )
    .await?;
    let owner = SharedBytes::from(vec![42]);
    drop(left.submit_write_vectored(VectoredWriteRequest {
        segments: vec![segment(&owner, 0, 1)],
    }));
    if read_exact(|r| right.submit_read(r), 1).await? != [42] {
        return Err("abandoned admitted vectored write did not apply".into());
    }
    left.submit_close().await.map_err(|e| e.to_string())?;
    check_closed(|request| left.submit_write_vectored(request), &owner).await?;
    right.submit_close().await.map_err(|e| e.to_string())?;
    Ok(())
}

/// Cold twin of [`check_vectored_stream_provider`], also proving that an
/// unpolled write has no effect and first poll admits a write exactly once.
///
/// # Errors
/// Returns the first observed contract violation.
pub async fn check_cold_vectored_stream_provider<
    L: ByteStreamVectoredSubmit,
    R: ByteStreamSubmit,
>(
    left: &ColdStream<L>,
    right: &ColdStream<R>,
) -> Result<(), String> {
    let owner = SharedBytes::from(vec![42]);
    drop(left.write_vectored(VectoredWriteRequest {
        segments: vec![segment(&owner, 0, 1)],
    }));
    if owner.strong_count() != 1 {
        return Err("unpolled cold write retained ownership".into());
    }
    write_all(|r| left.write(r), vec![7]).await?;
    if read_exact(|r| right.read(r), 1).await? != [7] {
        return Err("unpolled vectored write applied bytes".into());
    }
    check_payloads(
        left.max_segments(),
        |request| left.write_vectored(request),
        |request| right.read(request),
    )
    .await?;
    let mut admitted = Box::pin(left.write_vectored(VectoredWriteRequest {
        segments: vec![segment(&owner, 0, 1)],
    }));
    poll_fn(|cx| {
        let output = admitted.as_mut().poll(cx);
        drop(output);
        Poll::Ready(())
    })
    .await;
    drop(admitted);
    if read_exact(|r| right.read(r), 1).await? != [42] {
        return Err("first-poll admitted vectored write did not apply".into());
    }
    left.close().await.map_err(|e| e.to_string())?;
    check_closed(|request| left.write_vectored(request), &owner).await?;
    right.close().await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn check_closed<W, WF>(write: W, owner: &SharedBytes) -> Result<(), String>
where
    W: Fn(VectoredWriteRequest) -> WF,
    WF: Future<Output = CompletionResult<VectoredWriteResult, VectoredWriteFailure>>,
{
    let segments = vec![segment(owner, 0, 1)];
    let pointer = segments.as_ptr();
    match write(VectoredWriteRequest { segments }).await {
        Err(error) if error.certainty() == CompletionCertainty::NotApplied => {
            let failure = error.into_parts().1;
            if failure.error != NetworkError::ConnectionClosed
                || failure.bytes_transferred != 0
                || failure.segments.as_ptr() != pointer
                || !failure.segments[0].bytes.shares_allocation(owner)
            {
                return Err(format!("post-close ownership failure: {failure:?}"));
            }
        }
        output => return Err(format!("post-close completion: {output:?}")),
    }
    Ok(())
}

async fn check_blocked<W, WF, LR, LRF, RW, RWF, RR, RRF>(
    write: W,
    left_read: LR,
    right_write: RW,
    right_read: RR,
    buffered_prefix: &[u8],
    exact_prefix: bool,
) -> Result<usize, String>
where
    W: Fn(VectoredWriteRequest) -> WF,
    WF: Future<Output = CompletionResult<VectoredWriteResult, VectoredWriteFailure>>,
    LR: Fn(ReadRequest) -> LRF,
    LRF: Future<Output = CompletionResult<ReadResult, NetworkFailure>>,
    RW: Fn(WriteRequest) -> RWF,
    RWF: Future<Output = CompletionResult<crate::network::WriteResult, NetworkFailure>>,
    RR: Fn(ReadRequest) -> RRF,
    RRF: Future<Output = CompletionResult<ReadResult, NetworkFailure>>,
{
    if !exact_prefix && buffered_prefix.contains(&42) {
        return Err("bounded blocked prefix must exclude the marker byte 42".into());
    }
    let owner = SharedBytes::from(vec![42]);
    let mut pending = Box::pin(write(VectoredWriteRequest {
        segments: vec![segment(&owner, 0, 1)],
    }));
    let was_pending = poll_fn(|cx| Poll::Ready(Pin::new(&mut pending).poll(cx).is_pending())).await;
    if !was_pending {
        return Err("blocked-write fixture did not block its write".into());
    }
    drop(pending);
    if owner.strong_count() != 2 {
        return Err(
            "abandoned pending operation released or duplicated its backing allocation".into(),
        );
    }
    // Sending an ACK in the other direction must remain possible while blocked.
    write_all(right_write, vec![7]).await?;
    if read_exact(left_read, 1).await? != [7] {
        return Err("blocked write prevented opposite-direction ACK".into());
    }
    if owner.strong_count() != 2 {
        return Err("unrelated ACK released blocked send ownership".into());
    }
    let prefix_bytes = if exact_prefix {
        if read_exact(&right_read, buffered_prefix.len()).await? != buffered_prefix {
            return Err("blocked write modified bytes preceding its prefix".into());
        }
        if read_exact(right_read, 1).await? != [42] {
            return Err("abandoned blocked write did not terminalize in order".into());
        }
        buffered_prefix.len()
    } else {
        read_bounded_prefix(right_read, buffered_prefix).await?
    };
    // Completion may be delayed after peer visibility. The caller must finish
    // its provider or drive delayed completions before inspecting final refcount.
    Ok(prefix_bytes)
}

// A preceding native send can complete with a short prefix. Its CQE need not
// become visible until after the peer receives the following marker. Drain a
// bounded, independently known byte pattern without predicting that CQE; the
// fixture must compare the returned prefix length with actual write completion.
async fn read_bounded_prefix<R, RF>(read: R, prefix: &[u8]) -> Result<usize, String>
where
    R: Fn(ReadRequest) -> RF,
    RF: Future<Output = CompletionResult<ReadResult, NetworkFailure>>,
{
    let mut received = 0;
    loop {
        let maximum = prefix
            .len()
            .checked_sub(received)
            .and_then(|remaining| remaining.checked_add(1))
            .ok_or("blocked prefix length overflow")?
            .min(64 * 1024);
        let result = read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: maximum,
        })
        .await
        .map_err(|error| format!("blocked prefix read failed: {error}"))?;
        if result.bytes_read == 0
            || result.bytes_read != result.buffer.len()
            || result.bytes_read > maximum
            || result.end_of_stream
        {
            return Err(format!("invalid blocked prefix read progress {result:?}"));
        }
        for (index, byte) in result.buffer.iter().copied().enumerate() {
            if byte == 42 {
                if index + 1 != result.buffer.len() {
                    return Err("unexpected bytes after abandoned blocked write marker".into());
                }
                return Ok(received + index);
            }
            if prefix.get(received + index).copied() != Some(byte) {
                return Err("blocked write modified or exceeded its possible prefix".into());
            }
        }
        received += result.bytes_read;
    }
}

/// Checks retained ownership and independent read progress for a pair whose
/// left-to-right direction was deliberately filled by the provider fixture.
/// `buffered_prefix` must exactly describe those prior bytes. The opposite
/// direction must be empty. No fault may prevent eventual terminal completion.
///
/// # Errors
/// Returns the first observed contract violation, including an unblocked fixture.
pub async fn check_blocked_vectored_stream_provider<
    L: ByteStreamVectoredSubmit,
    R: ByteStreamSubmit,
>(
    left: &L,
    right: &R,
    buffered_prefix: &[u8],
) -> Result<(), String> {
    check_blocked(
        |r| left.submit_write_vectored(r),
        |r| left.submit_read(r),
        |r| right.submit_write(r),
        |r| right.submit_read(r),
        buffered_prefix,
        true,
    )
    .await
    .map(|_| ())
}

/// Cold twin of [`check_blocked_vectored_stream_provider`].
///
/// # Errors
/// Returns the first observed contract violation.
pub async fn check_cold_blocked_vectored_stream_provider<
    L: ByteStreamVectoredSubmit,
    R: ByteStreamSubmit,
>(
    left: &ColdStream<L>,
    right: &ColdStream<R>,
    buffered_prefix: &[u8],
) -> Result<(), String> {
    check_blocked(
        |r| left.write_vectored(r),
        |r| left.read(r),
        |r| right.write(r),
        |r| right.read(r),
        buffered_prefix,
        true,
    )
    .await
    .map(|_| ())
}

/// Like [`check_blocked_vectored_stream_provider`], but a preceding admitted
/// send may report short progress after the peer starts draining. Its possible
/// prefix must exclude byte 42, which uniquely identifies the abandoned write.
/// Returns the actual preceding byte count. The fixture must compare this count
/// with its prefilled bytes plus the preceding send's actual completion; merely
/// passing this check does not establish exact preceding-write progress.
///
/// # Errors
/// Rejects a marker-containing fixture, missing blocked ownership/read progress,
/// altered prefix bytes, a prefix longer than supplied, or bytes after the marker.
pub async fn check_bounded_blocked_vectored_stream_provider<
    L: ByteStreamVectoredSubmit,
    R: ByteStreamSubmit,
>(
    left: &L,
    right: &R,
    possible_prefix: &[u8],
) -> Result<usize, String> {
    check_blocked(
        |r| left.submit_write_vectored(r),
        |r| left.submit_read(r),
        |r| right.submit_write(r),
        |r| right.submit_read(r),
        possible_prefix,
        false,
    )
    .await
}

/// Cold twin of [`check_bounded_blocked_vectored_stream_provider`].
///
/// # Errors
/// Returns the first fixture, ownership, or observed byte-progress violation.
pub async fn check_cold_bounded_blocked_vectored_stream_provider<
    L: ByteStreamVectoredSubmit,
    R: ByteStreamSubmit,
>(
    left: &ColdStream<L>,
    right: &ColdStream<R>,
    possible_prefix: &[u8],
) -> Result<usize, String> {
    check_blocked(
        |r| left.write_vectored(r),
        |r| left.read(r),
        |r| right.write(r),
        |r| right.read(r),
        possible_prefix,
        false,
    )
    .await
}

async fn check_exhausted<W, WF>(write: W) -> Result<(), String>
where
    W: Fn(VectoredWriteRequest) -> WF,
    WF: Future<Output = CompletionResult<VectoredWriteResult, VectoredWriteFailure>>,
{
    let owner = SharedBytes::from(vec![42]);
    let segments = vec![segment(&owner, 0, 1)];
    let pointer = segments.as_ptr();
    match write(VectoredWriteRequest { segments }).await {
        Err(error) => {
            if error.certainty() != CompletionCertainty::NotApplied {
                return Err(format!("resource rejection certainty: {error}"));
            }
            let failure = error.into_parts().1;
            if !matches!(failure.error, NetworkError::ResourceExhausted { .. })
                || failure.bytes_transferred != 0
                || failure.segments.as_ptr() != pointer
                || failure.segments.len() != 1
                || !failure.segments[0].bytes.shares_allocation(&owner)
                || failure.segments[0].range != (0..1)
            {
                return Err(format!(
                    "resource rejection lost ownership or progress: {failure:?}"
                ));
            }
        }
        Ok(output) => return Err(format!("exhausted fixture admitted a write: {output:?}")),
    }
    if owner.strong_count() != 1 {
        return Err("resource rejection retained a shared allocation".into());
    }
    Ok(())
}

/// Checks exact returned ownership and `NotApplied` rejection with a fixture
/// whose write-operation or write-byte admission budget is already exhausted.
///
/// # Errors
/// Returns a violation, including a fixture that unexpectedly admits the write.
pub async fn check_exhausted_vectored_stream_provider<S: ByteStreamVectoredSubmit>(
    stream: &S,
) -> Result<(), String> {
    check_exhausted(|request| stream.submit_write_vectored(request)).await
}

/// Cold twin of [`check_exhausted_vectored_stream_provider`].
///
/// # Errors
/// Returns the first observed contract violation.
pub async fn check_cold_exhausted_vectored_stream_provider<S: ByteStreamVectoredSubmit>(
    stream: &ColdStream<S>,
) -> Result<(), String> {
    check_exhausted(|request| stream.write_vectored(request)).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        cell::Cell,
        future::ready,
        task::{Context, Waker},
    };

    fn observe(actual: &[u8], possible: &[u8], chunk: usize) -> Result<usize, String> {
        let cursor = Cell::new(0);
        let read = |request: ReadRequest| {
            let start = cursor.get();
            let count = (actual.len() - start).min(request.max_bytes).min(chunk);
            cursor.set(start + count);
            ready(Ok(ReadResult {
                buffer: actual[start..start + count].to_vec(),
                bytes_read: count,
                end_of_stream: count == 0,
            }))
        };
        let mut future = std::pin::pin!(read_bounded_prefix(read, possible));
        let Poll::Ready(result) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("in-memory oracle must finish in one poll");
        };
        result
    }

    #[test]
    fn bounded_prefix_oracle_accepts_short_and_full_progress_across_read_boundaries() {
        let possible = [1, 2, 3, 4, 5];
        for count in 0..=possible.len() {
            let mut actual = possible[..count].to_vec();
            actual.push(42);
            for chunk in [1, 2, 64] {
                assert_eq!(observe(&actual, &possible, chunk).unwrap(), count);
            }
        }
    }

    #[test]
    fn bounded_prefix_oracle_rejects_corruption_overrun_missing_marker_and_trailing_bytes() {
        let possible = [1, 2, 3];
        for actual in [&[1, 9, 42][..], &[1, 2, 3, 4, 42], &[1, 2, 3], &[1, 42, 2]] {
            assert!(
                observe(actual, &possible, 64).is_err(),
                "accepted {actual:?}"
            );
        }
    }
}
