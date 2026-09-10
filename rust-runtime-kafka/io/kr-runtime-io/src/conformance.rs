//! Shared behavioral checks for simulated and production I/O providers.

mod vectored;
pub use vectored::{
    check_blocked_vectored_stream_provider, check_bounded_blocked_vectored_stream_provider,
    check_cold_blocked_vectored_stream_provider,
    check_cold_bounded_blocked_vectored_stream_provider,
    check_cold_exhausted_vectored_stream_provider, check_cold_vectored_stream_provider,
    check_exhausted_vectored_stream_provider, check_vectored_stream_provider,
};

use crate::datagram::{
    ColdDatagramNetwork, DatagramBindRequest, DatagramError, DatagramFailure,
    DatagramProviderSubmit, DatagramSocketSubmit, DatagramTruncation, RecvFromRequest,
    RecvFromResult, SendToRequest,
};
use crate::network::{
    ByteStreamSubmit, ColdNetwork, ColdStream, ConnectRequest, ListenRequest, NetworkError,
    NetworkFailure, NetworkListenerSubmit, NetworkProviderSubmit, ReadRequest, ReadResult,
    WriteRequest, WriteResult,
};
use crate::{ColdFile, FileIoSubmit, ReadAtRequest, WriteAtRequest};
use kr_runtime::{CompletionCertainty, CompletionResult};
use std::fmt::Debug;
use std::future::{Future, poll_fn};
use std::task::Poll;

/// Exercises the implementation-independent bound datagram contract.
///
/// The three addresses must be distinct, bindable provider-native addresses.
/// `expired_deadline` must already be elapsed on the provider's paired
/// monotonic clock. The check covers exclusive binding, source attribution,
/// empty and truncated atomic datagrams, one socket communicating with several
/// peers, eager submission after response abandonment, nonblocking draining,
/// deadline failure, close fencing, and exact owned buffers on rejection.
pub async fn check_datagram_provider<P>(
    provider: &P,
    first_address: P::Address,
    second_address: P::Address,
    third_address: P::Address,
    expired_deadline: P::Instant,
) -> Result<(), String>
where
    P: DatagramProviderSubmit,
    P::Address: Debug + Eq,
{
    let first = provider
        .submit_bind(DatagramBindRequest {
            address: first_address,
        })
        .await
        .map_err(|error| format!("first datagram bind failed: {error}"))?;
    let second = provider
        .submit_bind(DatagramBindRequest {
            address: second_address,
        })
        .await
        .map_err(|error| format!("second datagram bind failed: {error}"))?;
    let third = provider
        .submit_bind(DatagramBindRequest {
            address: third_address,
        })
        .await
        .map_err(|error| format!("third datagram bind failed: {error}"))?;
    let first_bound = first.local_addr();
    let second_bound = second.local_addr();
    let third_bound = third.local_addr();

    match provider
        .submit_bind(DatagramBindRequest {
            address: first_bound.clone(),
        })
        .await
    {
        Err(error)
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().error() == &DatagramError::AddressInUse => {}
        Err(error) => {
            return Err(format!(
                "duplicate datagram bind returned the wrong failure: {error}"
            ));
        }
        Ok(_) => return Err("duplicate datagram bind unexpectedly succeeded".to_owned()),
    }

    let empty_send = first
        .submit_send_to(SendToRequest {
            buffer: Vec::new(),
            destination: second_bound.clone(),
        })
        .await
        .map_err(|error| format!("empty datagram send failed: {error}"))?;
    if empty_send.bytes_sent != 0 || !empty_send.buffer.is_empty() {
        return Err(format!("empty datagram send returned {empty_send:?}"));
    }
    let empty_receive = second
        .submit_recv_from(RecvFromRequest {
            buffer: b"prefix".to_vec(),
            max_bytes: 0,
        })
        .await
        .map_err(|error| format!("empty datagram receive failed: {error}"))?;
    if empty_receive.buffer != b"prefix"
        || empty_receive.bytes_received != 0
        || empty_receive.datagram_len != 0
        || empty_receive.source != first_bound
        || empty_receive.truncation != DatagramTruncation::Complete
    {
        return Err(format!("empty datagram receive returned {empty_receive:?}"));
    }

    send_exact(&first, second_bound.clone(), b"second".to_vec()).await?;
    send_exact(&first, third_bound.clone(), b"third".to_vec()).await?;
    let second_result = recv_one(&second, Vec::new(), 64).await?;
    let third_result = recv_one(&third, Vec::new(), 64).await?;
    if second_result.buffer != b"second"
        || third_result.buffer != b"third"
        || second_result.source != first_bound
        || third_result.source != first_bound
    {
        return Err(format!(
            "one-to-many datagram routing mismatch: second={second_result:?}, third={third_result:?}"
        ));
    }

    send_exact(&first, second_bound.clone(), b"truncate".to_vec()).await?;
    let truncated = recv_one(&second, b"!".to_vec(), 3).await?;
    if truncated.buffer != b"!tru"
        || truncated.bytes_received != 3
        || truncated.datagram_len != 8
        || truncated.truncation != DatagramTruncation::Truncated
    {
        return Err(format!("truncated datagram returned {truncated:?}"));
    }

    // Dropping either response must not retract its eagerly admitted effect.
    drop(first.submit_send_to(SendToRequest {
        buffer: b"visible".to_vec(),
        destination: second_bound.clone(),
    }));
    let live_receive = second.submit_recv_from(RecvFromRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    });
    let visible = live_receive
        .await
        .map_err(|error| format!("receive after abandoned responses failed: {error}"))?;
    if visible.buffer != b"visible" {
        return Err(format!(
            "dropped send response semantics returned {visible:?}"
        ));
    }

    drop(second.submit_recv_from(RecvFromRequest {
        buffer: Vec::new(),
        max_bytes: 64,
    }));
    let first_marker = b"abandoned-receive-a";
    let second_marker = b"live-receive-b";
    send_exact(&first, second_bound.clone(), first_marker.to_vec()).await?;
    send_exact(&first, second_bound.clone(), second_marker.to_vec()).await?;
    let after_abandoned_receive = recv_after_abandoned_receive(
        |request| second.submit_recv_from(request),
        RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 64,
        },
    )
    .await?;
    if after_abandoned_receive.buffer.as_slice() != first_marker
        && after_abandoned_receive.buffer.as_slice() != second_marker
    {
        return Err(format!(
            "receive after an abandoned response returned an unknown marker: {after_abandoned_receive:?}"
        ));
    }

    match second
        .submit_try_recv_from(RecvFromRequest {
            buffer: b"would-block".to_vec(),
            max_bytes: 64,
        })
        .await
    {
        Err(error)
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().error() == &DatagramError::WouldBlock
                && error.error().buffer() == Some(&b"would-block"[..]) => {}
        Err(error) => return Err(format!("nonblocking drain returned wrong failure: {error}")),
        Ok(result) => {
            return Err(format!(
                "dropped receive response left an extra datagram for the nonblocking drain: {result:?}"
            ));
        }
    }

    match second
        .submit_recv_from_until(
            RecvFromRequest {
                buffer: b"deadline".to_vec(),
                max_bytes: 64,
            },
            expired_deadline,
        )
        .await
    {
        Err(error)
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().error() == &DatagramError::DeadlineExceeded
                && error.error().buffer() == Some(&b"deadline"[..]) => {}
        Err(error) => return Err(format!("expired receive returned wrong failure: {error}")),
        Ok(result) => {
            return Err(format!("expired receive unexpectedly returned {result:?}"));
        }
    }

    first
        .submit_close()
        .await
        .map_err(|error| format!("first datagram close failed: {error}"))?;
    first
        .submit_close()
        .await
        .map_err(|error| format!("repeated datagram close failed: {error}"))?;
    let rejected_buffer = b"send-after-close".to_vec();
    match first
        .submit_send_to(SendToRequest {
            buffer: rejected_buffer.clone(),
            destination: second_bound,
        })
        .await
    {
        Err(error)
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().error() == &DatagramError::SocketClosed
                && error.error().buffer() == Some(rejected_buffer.as_slice()) => {}
        Err(error) => return Err(format!("send after close returned wrong failure: {error}")),
        Ok(result) => return Err(format!("send after close returned {result:?}")),
    }

    let rebound = provider
        .submit_bind(DatagramBindRequest {
            address: first_bound,
        })
        .await
        .map_err(|error| format!("rebind after datagram close failed: {error}"))?;
    rebound
        .submit_close()
        .await
        .map_err(|error| format!("rebound datagram close failed: {error}"))?;

    let close_buffer = b"pending-before-close".to_vec();
    let mut pending_before_close = Box::pin(second.submit_recv_from(RecvFromRequest {
        buffer: close_buffer.clone(),
        max_bytes: 64,
    }));
    second
        .submit_close()
        .await
        .map_err(|error| format!("second datagram close failed: {error}"))?;
    let receive_after_close =
        poll_fn(|context| Poll::Ready(pending_before_close.as_mut().poll(context))).await;
    match receive_after_close {
        Poll::Ready(Err(error))
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().error() == &DatagramError::SocketClosed
                && error.error().buffer() == Some(close_buffer.as_slice())
                && error.error().bytes_transferred() == 0 => {}
        Poll::Ready(Err(error)) => {
            return Err(format!(
                "receive pending before close returned the wrong failure: {error}"
            ));
        }
        Poll::Ready(Ok(result)) => {
            return Err(format!(
                "receive pending before close unexpectedly returned {result:?}"
            ));
        }
        Poll::Pending => {
            return Err(
                "successful close returned before an earlier receive was terminal".to_owned(),
            );
        }
    }
    third
        .submit_close()
        .await
        .map_err(|error| format!("third datagram close failed: {error}"))?;
    Ok(())
}

async fn send_exact<S>(socket: &S, destination: S::Address, buffer: Vec<u8>) -> Result<(), String>
where
    S: DatagramSocketSubmit,
{
    let expected = buffer.clone();
    let result = socket
        .submit_send_to(SendToRequest {
            buffer,
            destination,
        })
        .await
        .map_err(|error| format!("datagram send failed: {error}"))?;
    if result.buffer != expected || result.bytes_sent != expected.len() {
        return Err(format!("datagram send returned {result:?}"));
    }
    Ok(())
}

async fn recv_one<S>(
    socket: &S,
    buffer: Vec<u8>,
    max_bytes: usize,
) -> Result<crate::datagram::RecvFromResult<S::Address>, String>
where
    S: DatagramSocketSubmit,
{
    socket
        .submit_recv_from(RecvFromRequest { buffer, max_bytes })
        .await
        .map_err(|error| format!("datagram receive failed: {error}"))
}

async fn recv_after_abandoned_receive<A, F, Fut>(
    mut receive: F,
    mut request: RecvFromRequest,
) -> Result<RecvFromResult<A>, String>
where
    A: Debug,
    F: FnMut(RecvFromRequest) -> Fut,
    Fut: Future<Output = CompletionResult<RecvFromResult<A>, DatagramFailure>>,
{
    loop {
        let max_bytes = request.max_bytes;
        match receive(request).await {
            Ok(result) => return Ok(result),
            Err(error)
                if error.certainty() == CompletionCertainty::NotApplied
                    && matches!(
                        error.error().error(),
                        DatagramError::ResourceExhausted { .. }
                    ) =>
            {
                let (_, failure) = error.into_parts();
                let buffer = failure.into_buffer().ok_or_else(|| {
                    "concurrent receive rejection did not return its request buffer".to_owned()
                })?;
                request = RecvFromRequest { buffer, max_bytes };
                yield_once().await;
            }
            Err(error) => {
                return Err(format!(
                    "receive after an abandoned response failed: {error}"
                ));
            }
        }
    }
}

async fn yield_once() {
    let mut yielded = false;
    poll_fn(|context| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            context.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

/// Exercises the backend-independent cold datagram contract.
///
/// The two addresses must be distinct, bindable provider-native addresses,
/// and `expired_deadline` must already be elapsed on the provider's paired
/// monotonic clock. Where the warm suite proves that dropped responses were
/// already admitted, this check proves the opposite for never-polled
/// futures: an unpolled bind binds nothing and consumes no capacity, an
/// unpolled send enqueues no datagram, an unpolled receive consumes neither
/// a datagram nor pending-receive capacity, and an unpolled close submits
/// nothing. It ends with the close fence and exact owned buffers on
/// rejection.
pub async fn check_cold_datagram_provider<P>(
    network: &ColdDatagramNetwork<P>,
    first_address: P::Address,
    second_address: P::Address,
    expired_deadline: P::Instant,
) -> Result<(), String>
where
    P: DatagramProviderSubmit,
    P::Address: Debug,
{
    // Never-polled binds bind nothing: far more are constructed and dropped
    // than any provider's socket bound admits, then the real bind succeeds.
    for _ in 0..64 {
        drop(network.bind(DatagramBindRequest {
            address: first_address.clone(),
        }));
    }
    let first = network
        .bind(DatagramBindRequest {
            address: first_address,
        })
        .await
        .map_err(|error| format!("cold bind after dropped unpolled binds failed: {error}"))?;
    let second = network
        .bind(DatagramBindRequest {
            address: second_address,
        })
        .await
        .map_err(|error| format!("second cold bind failed: {error}"))?;
    let second_bound = second.local_addr();

    // A never-polled send enqueues nothing: only the awaited marker arrives.
    drop(first.send_to(SendToRequest {
        buffer: b"never".to_vec(),
        destination: second_bound.clone(),
    }));
    let sent = first
        .send_to(SendToRequest {
            buffer: b"real".to_vec(),
            destination: second_bound.clone(),
        })
        .await
        .map_err(|error| format!("cold send failed: {error}"))?;
    if sent.bytes_sent != 4 || sent.buffer != b"real" {
        return Err(format!("cold send returned {sent:?}"));
    }
    let received = second
        .recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 64,
        })
        .await
        .map_err(|error| format!("cold receive failed: {error}"))?;
    if received.buffer != b"real" {
        return Err(format!(
            "datagram after a dropped unpolled send was {received:?}, expected real"
        ));
    }

    // Never-polled receives consume neither capacity nor the next datagram.
    for _ in 0..64 {
        drop(second.recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 64,
        }));
    }
    first
        .send_to(SendToRequest {
            buffer: b"kept".to_vec(),
            destination: second_bound.clone(),
        })
        .await
        .map_err(|error| format!("cold marker send failed: {error}"))?;
    let kept = second
        .recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 64,
        })
        .await
        .map_err(|error| format!("cold receive after dropped unpolled receives failed: {error}"))?;
    if kept.buffer != b"kept" {
        return Err(format!(
            "datagram after dropped unpolled receives was {kept:?}, expected kept"
        ));
    }

    // Nonblocking availability and expired deadlines are observed at
    // first-poll admission, with exact owned buffers on the clean failures.
    match second
        .try_recv_from(RecvFromRequest {
            buffer: b"would-block".to_vec(),
            max_bytes: 64,
        })
        .await
    {
        Err(error)
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().error() == &DatagramError::WouldBlock
                && error.error().buffer() == Some(&b"would-block"[..]) => {}
        Err(error) => {
            return Err(format!(
                "cold nonblocking drain returned wrong failure: {error}"
            ));
        }
        Ok(result) => {
            return Err(format!(
                "cold nonblocking drain unexpectedly returned {result:?}"
            ));
        }
    }
    match second
        .recv_from_until(
            RecvFromRequest {
                buffer: b"deadline".to_vec(),
                max_bytes: 64,
            },
            expired_deadline,
        )
        .await
    {
        Err(error)
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().error() == &DatagramError::DeadlineExceeded
                && error.error().buffer() == Some(&b"deadline"[..]) => {}
        Err(error) => {
            return Err(format!(
                "cold expired receive returned wrong failure: {error}"
            ));
        }
        Ok(result) => {
            return Err(format!(
                "cold expired receive unexpectedly returned {result:?}"
            ));
        }
    }

    // A never-polled close submits nothing: the socket still sends.
    drop(first.close());
    first
        .send_to(SendToRequest {
            buffer: b"still-open".to_vec(),
            destination: second_bound.clone(),
        })
        .await
        .map_err(|error| format!("cold send after dropped unpolled close failed: {error}"))?;
    second
        .recv_from(RecvFromRequest {
            buffer: Vec::new(),
            max_bytes: 64,
        })
        .await
        .map_err(|error| format!("cold receive of still-open marker failed: {error}"))?;

    // Explicit close fences and rejects later sends with the exact buffer.
    first
        .close()
        .await
        .map_err(|error| format!("cold close failed: {error}"))?;
    first
        .close()
        .await
        .map_err(|error| format!("repeated cold close failed: {error}"))?;
    let rejected_buffer = b"send-after-close".to_vec();
    match first
        .send_to(SendToRequest {
            buffer: rejected_buffer.clone(),
            destination: second_bound,
        })
        .await
    {
        Err(error)
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().error() == &DatagramError::SocketClosed
                && error.error().buffer() == Some(rejected_buffer.as_slice()) => {}
        Err(error) => {
            return Err(format!(
                "cold send after close returned wrong failure: {error}"
            ));
        }
        Ok(result) => {
            return Err(format!(
                "cold send after close unexpectedly returned {result:?}"
            ));
        }
    }
    second
        .close()
        .await
        .map_err(|error| format!("second cold close failed: {error}"))?;
    Ok(())
}

/// Exercises the implementation-independent single-file contract.
///
/// The provider must return a newly opened empty file with room for the small
/// requests below. The check intentionally drops admitted write and set_len
/// futures and verifies that a later sync still fences them — including a
/// group of concurrently admitted commuting writes the provider is free to
/// overlap and complete out of admission order.
pub async fn check_empty_file<F>(io: F) -> Result<(), String>
where
    F: FileIoSubmit,
{
    let initial = io.submit_len().await.map_err(|error| error.to_string())?;
    if initial.len != 0 {
        return Err(format!("new file length was {}, expected 0", initial.len));
    }

    let rejected_buffer = b"owned-on-error".to_vec();
    match io
        .submit_write_at(WriteAtRequest::new(u64::MAX, rejected_buffer.clone()))
        .await
    {
        Err(error)
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().buffer == rejected_buffer => {}
        Err(error) => {
            return Err(format!(
                "overflowing write returned wrong certainty or buffer: {error:?}"
            ));
        }
        Ok(result) => {
            return Err(format!(
                "overflowing write unexpectedly succeeded with {result:?}"
            ));
        }
    }

    drop(io.submit_set_len(3));
    let synced = io.submit_sync().await.map_err(|error| error.to_string())?;
    if synced.durable_len != 3 {
        return Err(format!(
            "sync after dropped set_len future reported durable length {}, expected 3",
            synced.durable_len
        ));
    }

    let mut offset = 0_u64;
    let mut remaining = b"abc".to_vec();
    while !remaining.is_empty() {
        let written = io
            .submit_write_at(WriteAtRequest::new(offset, remaining))
            .await
            .map_err(|error| error.to_string())?;
        if written.bytes_written == 0 || written.bytes_written > written.buffer.len() {
            return Err(format!(
                "invalid successful write length {} for {} requested bytes",
                written.bytes_written,
                written.buffer.len()
            ));
        }
        offset += written.bytes_written as u64;
        remaining = written.buffer[written.bytes_written..].to_vec();
    }
    io.submit_sync().await.map_err(|error| error.to_string())?;

    let mut contents = Vec::new();
    let mut offset = 0_u64;
    while contents.len() < 3 {
        let read = io
            .submit_read_at(ReadAtRequest::new(offset, vec![0; 3 - contents.len()]))
            .await
            .map_err(|error| error.to_string())?;
        if read.bytes_read == 0 || read.bytes_read != read.buffer.len() {
            return Err(format!(
                "invalid successful read length {} with buffer length {}",
                read.bytes_read,
                read.buffer.len()
            ));
        }
        offset += read.bytes_read as u64;
        contents.extend(read.buffer);
    }
    if contents != b"abc" {
        return Err(format!(
            "read after sync returned {contents:?}, expected abc"
        ));
    }

    let shrunk = io
        .submit_set_len(2)
        .await
        .map_err(|error| error.to_string())?;
    if shrunk.len != 2 {
        return Err(format!("set_len returned {}, expected 2", shrunk.len));
    }
    let accepted = io.submit_len().await.map_err(|error| error.to_string())?;
    if accepted.len != 2 {
        return Err(format!(
            "length after set_len was {}, expected 2",
            accepted.len
        ));
    }
    let synced = io.submit_sync().await.map_err(|error| error.to_string())?;
    if synced.durable_len != 2 {
        return Err(format!(
            "final sync reported durable length {}, expected 2",
            synced.durable_len
        ));
    }

    // Several concurrently admitted commuting writes, all responses dropped.
    // A provider may overlap these and complete them in any internal order —
    // single-byte writes cannot be short, the file is pre-extended so no
    // write extends it — but one sync must fence every one of them.
    let extended = io
        .submit_set_len(5)
        .await
        .map_err(|error| error.to_string())?;
    if extended.len != 5 {
        return Err(format!("set_len returned {}, expected 5", extended.len));
    }
    for (offset, byte) in [(2_u64, b'c'), (4, b'e'), (3, b'd')] {
        drop(io.submit_write_at(WriteAtRequest::new(offset, vec![byte])));
    }
    let synced = io.submit_sync().await.map_err(|error| error.to_string())?;
    if synced.durable_len != 5 {
        return Err(format!(
            "sync after dropped commuting writes reported durable length {}, expected 5",
            synced.durable_len
        ));
    }
    let mut contents = Vec::new();
    let mut offset = 0_u64;
    while contents.len() < 5 {
        let read = io
            .submit_read_at(ReadAtRequest::new(offset, vec![0; 5 - contents.len()]))
            .await
            .map_err(|error| error.to_string())?;
        if read.bytes_read == 0 || read.bytes_read != read.buffer.len() {
            return Err(format!(
                "invalid successful read length {} with buffer length {}",
                read.bytes_read,
                read.buffer.len()
            ));
        }
        offset += read.bytes_read as u64;
        contents.extend(read.buffer);
    }
    if contents != b"abcde" {
        return Err(format!(
            "read after fencing dropped commuting writes returned {contents:?}, expected abcde"
        ));
    }

    Ok(())
}

/// Exercises the backend-independent cold file façade contract.
///
/// The backend must be a newly opened empty file with room for the small
/// requests below. Where [`check_empty_file`] intentionally proves that a
/// dropped unpolled future was already admitted, this check proves the
/// opposite: a never-polled [`ColdFile`] operation admits nothing, is never
/// fenced by a later sync, and effects follow first-poll admission order
/// rather than construction order. Pending-only cases (poll to `Pending`,
/// drop, remain admitted) need a suspending backend and live with the
/// provider-specific tests instead.
pub async fn check_cold_empty_file<F>(cold: ColdFile<F>) -> Result<(), String>
where
    F: FileIoSubmit,
{
    let initial = cold.len().await.map_err(|error| error.to_string())?;
    if initial.len != 0 {
        return Err(format!("new file length was {}, expected 0", initial.len));
    }

    // Constructed and dropped without polling: no admission, no effect, and
    // no participation in the later sync fence.
    drop(cold.write_at(WriteAtRequest::new(0, b"never".to_vec())));
    drop(cold.set_len(3));
    drop(cold.sync());
    let synced = cold.sync().await.map_err(|error| error.to_string())?;
    if synced.durable_len != 0 {
        return Err(format!(
            "sync after dropped unpolled operations reported durable length {}, expected 0",
            synced.durable_len
        ));
    }
    let unchanged = cold.len().await.map_err(|error| error.to_string())?;
    if unchanged.len != 0 {
        return Err(format!(
            "dropped unpolled operations changed the length to {}",
            unchanged.len
        ));
    }

    // First-poll rejection is NotApplied and returns the exact owned buffer.
    let rejected_buffer = b"owned-on-error".to_vec();
    match cold
        .write_at(WriteAtRequest::new(u64::MAX, rejected_buffer.clone()))
        .await
    {
        Err(error)
            if error.certainty() == CompletionCertainty::NotApplied
                && error.error().buffer == rejected_buffer => {}
        Err(error) => {
            return Err(format!(
                "overflowing cold write returned wrong certainty or buffer: {error:?}"
            ));
        }
        Ok(result) => {
            return Err(format!(
                "overflowing cold write unexpectedly succeeded with {result:?}"
            ));
        }
    }

    // Two construction-ordered writes polled in reverse: effects must follow
    // first-poll admission order, so the construction-first write lands last.
    let first_constructed = cold.write_at(WriteAtRequest::new(0, b"a".to_vec()));
    let second_constructed = cold.write_at(WriteAtRequest::new(0, b"b".to_vec()));
    let second_written = second_constructed
        .await
        .map_err(|error| error.to_string())?;
    let first_written = first_constructed.await.map_err(|error| error.to_string())?;
    if second_written.bytes_written != 1 || first_written.bytes_written != 1 {
        return Err(format!(
            "single-byte poll-order writes reported {} and {} written bytes",
            second_written.bytes_written, first_written.bytes_written
        ));
    }
    let ordered = cold
        .read_at(ReadAtRequest::new(0, vec![0; 1]))
        .await
        .map_err(|error| error.to_string())?;
    if ordered.buffer != b"a" {
        return Err(format!(
            "reverse-polled writes left {:?}, expected the construction-first write to land last",
            ordered.buffer
        ));
    }

    // An ordinary awaited round trip behaves exactly as the eager contract.
    let mut offset = 0_u64;
    let mut remaining = b"abc".to_vec();
    while !remaining.is_empty() {
        let written = cold
            .write_at(WriteAtRequest::new(offset, remaining))
            .await
            .map_err(|error| error.to_string())?;
        if written.bytes_written == 0 || written.bytes_written > written.buffer.len() {
            return Err(format!(
                "invalid successful cold write length {} for {} requested bytes",
                written.bytes_written,
                written.buffer.len()
            ));
        }
        offset += written.bytes_written as u64;
        remaining = written.buffer[written.bytes_written..].to_vec();
    }
    let synced = cold.sync().await.map_err(|error| error.to_string())?;
    if synced.durable_len != 3 {
        return Err(format!(
            "sync after awaited writes reported durable length {}, expected 3",
            synced.durable_len
        ));
    }

    let mut contents = Vec::new();
    let mut offset = 0_u64;
    while contents.len() < 3 {
        let read = cold
            .read_at(ReadAtRequest::new(offset, vec![0; 3 - contents.len()]))
            .await
            .map_err(|error| error.to_string())?;
        if read.bytes_read == 0 || read.bytes_read != read.buffer.len() {
            return Err(format!(
                "invalid successful cold read length {} with buffer length {}",
                read.bytes_read,
                read.buffer.len()
            ));
        }
        offset += read.bytes_read as u64;
        contents.extend(read.buffer);
    }
    if contents != b"abc" {
        return Err(format!(
            "cold read after sync returned {contents:?}, expected abc"
        ));
    }

    let shrunk = cold.set_len(2).await.map_err(|error| error.to_string())?;
    if shrunk.len != 2 {
        return Err(format!("cold set_len returned {}, expected 2", shrunk.len));
    }
    let synced = cold.sync().await.map_err(|error| error.to_string())?;
    if synced.durable_len != 2 {
        return Err(format!(
            "final cold sync reported durable length {}, expected 2",
            synced.durable_len
        ));
    }

    Ok(())
}

/// Exercises the common connected byte-stream contract on both endpoints.
///
/// The pair must be fresh, connected, and configured with enough directional
/// capacity for the small payloads below. The check covers zero-capacity reads,
/// eager admission after write and pending-read response abandonment, partial
/// transfers, full-duplex progress, TCP-style final-bytes/EOF ordering, and
/// idempotent half-close and close.
pub async fn check_connected_stream_pair<L, R>(left: &L, right: &R) -> Result<(), String>
where
    L: ByteStreamSubmit,
    R: ByteStreamSubmit,
{
    let zero = right
        .submit_read(ReadRequest {
            buffer: b"prefix".to_vec(),
            max_bytes: 0,
        })
        .await
        .map_err(|error| format!("zero-capacity read failed: {error}"))?;
    if zero.buffer != b"prefix" || zero.bytes_read != 0 || zero.end_of_stream {
        return Err(format!("zero-capacity read returned {zero:?}"));
    }

    drop(left.submit_write(WriteRequest {
        buffer: b"go".to_vec(),
    }));
    let abandoned = read_exact(|request| right.submit_read(request), 2).await?;
    if abandoned != b"go" {
        return Err(format!(
            "dropped admitted write produced {abandoned:?}, expected go"
        ));
    }

    // Dropping a pending read abandons only its response. The admitted read
    // remains first in stream order and consumes these later bytes.
    drop(right.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 2,
    }));
    write_all(|request| left.submit_write(request), b"rx".to_vec()).await?;

    write_all(|request| left.submit_write(request), b"hello".to_vec()).await?;
    let hello = read_exact(|request| right.submit_read(request), 5).await?;
    if hello != b"hello" {
        return Err(format!("partial round trip produced {hello:?}"));
    }

    let pending_left_read = left.submit_read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 2,
    });
    write_all(|request| left.submit_write(request), b"up".to_vec()).await?;
    let up = read_exact(|request| right.submit_read(request), 2).await?;
    if up != b"up" {
        return Err(format!("full-duplex outbound path produced {up:?}"));
    }
    write_all(|request| right.submit_write(request), b"ok".to_vec()).await?;
    let incoming = pending_left_read
        .await
        .map_err(|error| format!("pending opposite-direction read failed: {error}"))?;
    if incoming.bytes_read != 2 || incoming.buffer != b"ok" || incoming.end_of_stream {
        return Err(format!(
            "full-duplex opposite-direction read returned {incoming:?}"
        ));
    }

    write_all(|request| left.submit_write(request), b"fin".to_vec()).await?;
    left.submit_shutdown_write()
        .await
        .map_err(|error| format!("write-half shutdown failed: {error}"))?;
    left.submit_shutdown_write()
        .await
        .map_err(|error| format!("repeated write-half shutdown failed: {error}"))?;
    let final_bytes = read_exact(|request| right.submit_read(request), 3).await?;
    if final_bytes != b"fin" {
        return Err(format!(
            "half-close final bytes were {final_bytes:?}, expected fin"
        ));
    }
    let eof = right
        .submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        })
        .await
        .map_err(|error| format!("EOF read failed: {error}"))?;
    if eof.bytes_read != 0 || !eof.end_of_stream {
        return Err(format!("half-close EOF read returned {eof:?}"));
    }

    left.submit_close()
        .await
        .map_err(|error| format!("left close failed: {error}"))?;
    left.submit_close()
        .await
        .map_err(|error| format!("repeated left close failed: {error}"))?;
    right
        .submit_close()
        .await
        .map_err(|error| format!("right close failed: {error}"))?;
    right
        .submit_close()
        .await
        .map_err(|error| format!("repeated right close failed: {error}"))?;

    let rejected_buffer = b"write-after-close".to_vec();
    match left
        .submit_write(WriteRequest {
            buffer: rejected_buffer.clone(),
        })
        .await
    {
        Err(error) => {
            let certainty = error.certainty();
            let failure = error.into_parts().1;
            let category_matches = failure.error() == &NetworkError::ConnectionClosed;
            let returned = failure.into_buffer();
            if certainty != CompletionCertainty::NotApplied
                || !category_matches
                || returned.as_deref() != Some(rejected_buffer.as_slice())
            {
                return Err(format!(
                    "write after close returned certainty {certainty:?}, category_match={category_matches}, buffer={returned:?}"
                ));
            }
        }
        Ok(result) => {
            return Err(format!(
                "write after close unexpectedly returned {result:?}"
            ));
        }
    }
    Ok(())
}

/// Exercises the common connection-oriented provider and listener contract.
///
/// `listen_request.address` may request a provider-selected address such as a
/// TCP port of zero. Connections therefore target [`NetworkListenerSubmit::local_address`]
/// rather than the requested address. The two client addresses must be valid
/// distinct local identities for the provider.
///
/// The check verifies that dropping connect and accept futures abandons only
/// their responses, that live accepts preserve FIFO connection order, and that
/// listener close rejects pending and later accepts before succeeding
/// idempotently.
pub async fn check_network_provider<P>(
    provider: &P,
    listen_request: ListenRequest<P::Address>,
    first_client_address: P::Address,
    second_client_address: P::Address,
) -> Result<(), String>
where
    P: NetworkProviderSubmit,
{
    let listener = provider
        .submit_listen(listen_request)
        .await
        .map_err(|error| format!("listen failed: {error}"))?;
    let remote = listener.local_address();

    let abandoned_connect_accept = listener.submit_accept();
    drop(provider.submit_connect(ConnectRequest {
        local: first_client_address.clone(),
        remote: remote.clone(),
    }));
    let abandoned_connect_server = abandoned_connect_accept
        .await
        .map_err(|error| format!("accept for dropped connect failed: {error}"))?;
    let abandoned_connect_eof = abandoned_connect_server
        .submit_read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        })
        .await
        .map_err(|error| format!("read after dropped connect failed: {error}"))?;
    if abandoned_connect_eof.bytes_read != 0 || !abandoned_connect_eof.end_of_stream {
        return Err(format!(
            "dropped connect did not establish and then close its stream: {abandoned_connect_eof:?}"
        ));
    }
    abandoned_connect_server
        .submit_close()
        .await
        .map_err(|error| format!("close dropped-connect server failed: {error}"))?;
    drop(abandoned_connect_server);

    let first_fifo_accept = listener.submit_accept();
    let second_fifo_accept = listener.submit_accept();
    let first_fifo_client = provider
        .submit_connect(ConnectRequest {
            local: first_client_address.clone(),
            remote: remote.clone(),
        })
        .await
        .map_err(|error| format!("first FIFO connect failed: {error}"))?;
    let second_fifo_client = provider
        .submit_connect(ConnectRequest {
            local: second_client_address.clone(),
            remote: remote.clone(),
        })
        .await
        .map_err(|error| format!("second FIFO connect failed: {error}"))?;
    let first_fifo_server = first_fifo_accept
        .await
        .map_err(|error| format!("first FIFO accept failed: {error}"))?;
    let second_fifo_server = second_fifo_accept
        .await
        .map_err(|error| format!("second FIFO accept failed: {error}"))?;
    write_all(
        |request| first_fifo_client.submit_write(request),
        b"first".to_vec(),
    )
    .await?;
    write_all(
        |request| second_fifo_client.submit_write(request),
        b"second".to_vec(),
    )
    .await?;
    let first_marker = read_exact(|request| first_fifo_server.submit_read(request), 5).await?;
    let second_marker = read_exact(|request| second_fifo_server.submit_read(request), 6).await?;
    if first_marker != b"first" || second_marker != b"second" {
        return Err(format!(
            "accept FIFO mismatch: first={first_marker:?}, second={second_marker:?}"
        ));
    }
    first_fifo_client
        .submit_close()
        .await
        .map_err(|error| format!("close first FIFO client failed: {error}"))?;
    first_fifo_server
        .submit_close()
        .await
        .map_err(|error| format!("close first FIFO server failed: {error}"))?;
    drop(first_fifo_client);
    drop(first_fifo_server);

    check_connected_stream_pair(&second_fifo_client, &second_fifo_server).await?;
    drop(second_fifo_client);
    drop(second_fifo_server);

    drop(listener.submit_accept());
    let first_client = provider
        .submit_connect(ConnectRequest {
            local: first_client_address,
            remote: remote.clone(),
        })
        .await
        .map_err(|error| format!("first connect failed: {error}"))?;

    let second_accept = listener.submit_accept();
    let second_client = provider
        .submit_connect(ConnectRequest {
            local: second_client_address,
            remote,
        })
        .await
        .map_err(|error| format!("second connect failed: {error}"))?;
    let second_server = second_accept
        .await
        .map_err(|error| format!("second accept failed: {error}"))?;

    // If dropping the first accept incorrectly cancelled it, the second accept
    // receives first_client. Mark both clients so that mismatch fails with data
    // or EOF instead of waiting for an unbounded timeout.
    let _first_marker_result = write_all(
        |request| first_client.submit_write(request),
        b"one".to_vec(),
    )
    .await;
    write_all(
        |request| second_client.submit_write(request),
        b"two".to_vec(),
    )
    .await?;
    let marker = read_exact(|request| second_server.submit_read(request), 3).await?;
    if marker != b"two" {
        return Err(format!(
            "second accept received marker {marker:?}, expected second connection marker two"
        ));
    }

    second_client
        .submit_close()
        .await
        .map_err(|error| format!("close second marker client failed: {error}"))?;
    second_server
        .submit_close()
        .await
        .map_err(|error| format!("close second marker server failed: {error}"))?;
    let _first_close_result = first_client.submit_close().await;
    drop(first_client);
    drop(second_client);
    drop(second_server);

    let pending_accept = listener.submit_accept();
    listener
        .submit_close()
        .await
        .map_err(|error| format!("listener close failed: {error}"))?;
    match pending_accept.await {
        Err(error)
            if error.error().error() == &NetworkError::ListenerClosed
                && error.certainty() == CompletionCertainty::NotApplied => {}
        Err(error) => {
            return Err(format!(
                "pending accept failed with {} ({:?}), expected listener closed (not applied)",
                error.error().error(),
                error.certainty()
            ));
        }
        Ok(_) => return Err("pending accept succeeded after listener close".to_owned()),
    }
    listener
        .submit_close()
        .await
        .map_err(|error| format!("repeated listener close failed: {error}"))?;

    match listener.submit_accept().await {
        Err(error)
            if error.error().error() == &NetworkError::ListenerClosed
                && error.certainty() == CompletionCertainty::NotApplied => {}
        Err(error) => {
            return Err(format!(
                "post-close accept failed with {} ({:?}), expected listener closed (not applied)",
                error.error().error(),
                error.certainty()
            ));
        }
        Ok(_) => return Err("post-close accept unexpectedly succeeded".to_owned()),
    }

    Ok(())
}

/// Exercises the backend-independent cold byte-stream contract on both
/// endpoints.
///
/// The pair must be fresh, connected, and configured with enough directional
/// capacity for the small payloads below. Where the warm suite proves that a
/// dropped future's operation was already admitted, this check proves the
/// opposite for never-polled futures: an unpolled write sends nothing, an
/// unpolled read consumes neither bytes nor capacity, and an unpolled
/// shutdown or close submits no control operation. It ends by closing both
/// streams and asserting exact owned buffers on post-close rejection.
pub async fn check_cold_stream_pair<L, R>(
    left: &ColdStream<L>,
    right: &ColdStream<R>,
) -> Result<(), String>
where
    L: ByteStreamSubmit,
    R: ByteStreamSubmit,
{
    // Never-polled writes send nothing: only the awaited marker arrives.
    drop(left.write(WriteRequest {
        buffer: b"never".to_vec(),
    }));
    write_all(|request| left.write(request), b"yes".to_vec()).await?;
    let marker = read_exact(|request| right.read(request), 3).await?;
    if marker != b"yes" {
        return Err(format!(
            "bytes after a dropped unpolled write were {marker:?}, expected yes"
        ));
    }

    // A never-polled read consumes neither bytes nor a queue position: the
    // next awaited read observes the very next bytes.
    drop(right.read(ReadRequest {
        buffer: Vec::new(),
        max_bytes: 2,
    }));
    write_all(|request| left.write(request), b"ab".to_vec()).await?;
    let after_unpolled_read = read_exact(|request| right.read(request), 2).await?;
    if after_unpolled_read != b"ab" {
        return Err(format!(
            "bytes after a dropped unpolled read were {after_unpolled_read:?}, expected ab"
        ));
    }

    // A never-polled shutdown leaves the write half open.
    drop(left.shutdown_write());
    write_all(|request| left.write(request), b"fin".to_vec()).await?;
    left.shutdown_write()
        .await
        .map_err(|error| format!("cold write-half shutdown failed: {error}"))?;
    let final_bytes = read_exact(|request| right.read(request), 3).await?;
    if final_bytes != b"fin" {
        return Err(format!(
            "half-close final bytes were {final_bytes:?}, expected fin"
        ));
    }
    let eof = right
        .read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: 1,
        })
        .await
        .map_err(|error| format!("cold EOF read failed: {error}"))?;
    if eof.bytes_read != 0 || !eof.end_of_stream {
        return Err(format!("cold half-close EOF read returned {eof:?}"));
    }

    // A never-polled close leaves the stream open for the explicit close.
    drop(right.close());
    right
        .close()
        .await
        .map_err(|error| format!("cold right close failed: {error}"))?;
    left.close()
        .await
        .map_err(|error| format!("cold left close failed: {error}"))?;
    left.close()
        .await
        .map_err(|error| format!("repeated cold left close failed: {error}"))?;

    // First-poll rejection is NotApplied and returns the exact owned buffer.
    let rejected_buffer = b"write-after-close".to_vec();
    match left
        .write(WriteRequest {
            buffer: rejected_buffer.clone(),
        })
        .await
    {
        Err(error) => {
            let certainty = error.certainty();
            let failure = error.into_parts().1;
            let category_matches = failure.error() == &NetworkError::ConnectionClosed;
            let returned = failure.into_buffer();
            if certainty != CompletionCertainty::NotApplied
                || !category_matches
                || returned.as_deref() != Some(rejected_buffer.as_slice())
            {
                return Err(format!(
                    "cold write after close returned certainty {certainty:?}, category_match={category_matches}, buffer={returned:?}"
                ));
            }
        }
        Ok(result) => {
            return Err(format!(
                "cold write after close unexpectedly returned {result:?}"
            ));
        }
    }
    Ok(())
}

/// Exercises the backend-independent cold control-plane contract.
///
/// `listen_request.address` may request a provider-selected address such as a
/// TCP port of zero; connections target the bound listener address. The check
/// proves that never-polled listen, connect, accept, and listener-close
/// futures admit nothing — no bound address, no consumed connection or FIFO
/// position, no capacity, and no submitted close — then runs the cold stream
/// pair check over one accepted connection and closes the listener.
pub async fn check_cold_network_provider<P>(
    network: &ColdNetwork<P>,
    listen_request: ListenRequest<P::Address>,
    first_client_address: P::Address,
    second_client_address: P::Address,
) -> Result<(), String>
where
    P: NetworkProviderSubmit,
{
    // Never-polled listens consume no listener capacity or binding: far more
    // are constructed and dropped than any provider's listener bound admits.
    for _ in 0..64 {
        drop(network.listen(listen_request.clone()));
    }
    let listener = network
        .listen(listen_request)
        .await
        .map_err(|error| format!("cold listen after dropped unpolled listens failed: {error}"))?;
    let remote = listener.local_address();

    // A never-polled connect establishes nothing: the accepted connection is
    // the awaited one, identified by its marker.
    drop(network.connect(ConnectRequest {
        local: first_client_address.clone(),
        remote: remote.clone(),
    }));
    let live_accept = listener.accept();
    let client = network
        .connect(ConnectRequest {
            local: first_client_address,
            remote: remote.clone(),
        })
        .await
        .map_err(|error| format!("cold connect failed: {error}"))?;
    let server = live_accept
        .await
        .map_err(|error| format!("cold accept failed: {error}"))?;
    write_all(|request| client.write(request), b"live".to_vec()).await?;
    let marker = read_exact(|request| server.read(request), 4).await?;
    if marker != b"live" {
        return Err(format!(
            "accepted connection carried marker {marker:?}, expected live from the awaited connect"
        ));
    }

    // A never-polled accept consumes no queued connection.
    let second_client = network
        .connect(ConnectRequest {
            local: second_client_address,
            remote,
        })
        .await
        .map_err(|error| format!("second cold connect failed: {error}"))?;
    drop(listener.accept());
    let second_server = listener
        .accept()
        .await
        .map_err(|error| format!("cold accept after dropped unpolled accept failed: {error}"))?;
    write_all(|request| second_client.write(request), b"queued".to_vec()).await?;
    let queued_marker = read_exact(|request| second_server.read(request), 6).await?;
    if queued_marker != b"queued" {
        return Err(format!(
            "accept after a dropped unpolled accept carried {queued_marker:?}, expected queued"
        ));
    }
    second_client
        .close()
        .await
        .map_err(|error| format!("close second cold client failed: {error}"))?;
    second_server
        .close()
        .await
        .map_err(|error| format!("close second cold server failed: {error}"))?;

    check_cold_stream_pair(&client, &server).await?;

    // A never-polled listener close leaves the listener open; the explicit
    // close then rejects later accepts.
    drop(listener.close());
    let pending_accept = listener.accept();
    listener
        .close()
        .await
        .map_err(|error| format!("cold listener close failed: {error}"))?;
    match pending_accept.await {
        Err(error)
            if error.error().error() == &NetworkError::ListenerClosed
                && error.certainty() == CompletionCertainty::NotApplied => {}
        Err(error) => {
            return Err(format!(
                "accept polled after cold listener close failed with {} ({:?}), expected listener closed (not applied)",
                error.error().error(),
                error.certainty()
            ));
        }
        Ok(_) => {
            return Err("accept first polled after cold listener close succeeded".to_owned());
        }
    }
    listener
        .close()
        .await
        .map_err(|error| format!("repeated cold listener close failed: {error}"))?;
    Ok(())
}

async fn write_all<F, Fut>(mut write: F, mut remaining: Vec<u8>) -> Result<(), String>
where
    F: FnMut(WriteRequest) -> Fut,
    Fut: Future<Output = CompletionResult<WriteResult, NetworkFailure>>,
{
    while !remaining.is_empty() {
        let result = write(WriteRequest { buffer: remaining })
            .await
            .map_err(|error| format!("stream write failed: {error}"))?;
        if result.bytes_written == 0 || result.bytes_written > result.buffer.len() {
            return Err(format!(
                "invalid write progress {} for {} bytes",
                result.bytes_written,
                result.buffer.len()
            ));
        }
        remaining = result.buffer[result.bytes_written..].to_vec();
    }
    Ok(())
}

async fn read_exact<F, Fut>(mut read: F, expected: usize) -> Result<Vec<u8>, String>
where
    F: FnMut(ReadRequest) -> Fut,
    Fut: Future<Output = CompletionResult<ReadResult, NetworkFailure>>,
{
    let mut output = Vec::new();
    while output.len() < expected {
        let result = read(ReadRequest {
            buffer: Vec::new(),
            max_bytes: expected - output.len(),
        })
        .await
        .map_err(|error| format!("stream read failed: {error}"))?;
        if result.bytes_read == 0
            || result.bytes_read != result.buffer.len()
            || result.end_of_stream
        {
            return Err(format!("invalid exact-read progress {result:?}"));
        }
        output.extend(result.buffer);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kr_runtime::{CompletionError, SimRuntime};
    use std::cell::Cell;

    #[test]
    fn abandoned_receive_retry_preserves_the_rejected_buffer() {
        let attempts = Cell::new(0usize);
        let allocation = Cell::new(std::ptr::null::<u8>());
        let mut runtime = SimRuntime::default();
        let result = runtime
            .block_on(recv_after_abandoned_receive::<u8, _, _>(
                |mut request| {
                    let attempt = attempts.get();
                    attempts.set(attempt + 1);
                    if attempt == 0 {
                        allocation.set(request.buffer.as_ptr());
                        std::future::ready(Err(CompletionError::not_applied(
                            DatagramFailure::with_buffer(
                                DatagramError::ResourceExhausted {
                                    resource: "concurrent receives",
                                    limit: 1,
                                },
                                request.buffer,
                                0,
                            ),
                        )))
                    } else {
                        assert_eq!(request.buffer.as_ptr(), allocation.get());
                        request.buffer.extend_from_slice(b"packet");
                        std::future::ready(Ok(RecvFromResult {
                            buffer: request.buffer,
                            bytes_received: 6,
                            datagram_len: 6,
                            source: 7u8,
                            truncation: DatagramTruncation::Complete,
                        }))
                    }
                },
                RecvFromRequest {
                    buffer: Vec::with_capacity(8),
                    max_bytes: 8,
                },
            ))
            .expect("runtime drives retry")
            .expect("retry succeeds");

        assert_eq!(attempts.get(), 2);
        assert_eq!(result.buffer, b"packet");
    }
}
