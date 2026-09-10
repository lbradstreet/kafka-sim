//! Backend-neutral ring contract checks for provider test suites.

use kr_runtime::CompletionCertainty;

use super::{AppendRequest, ReadRequest, RingCursor, RingError, RingPosition, RingWriter};

/// Exercises ownership, batching, durable-only reads, pagination, conditional
/// append, trim, sync, stale cursors, and status against an empty ring.
///
/// The provider must be configured to accept at least three records, sixteen
/// batch/read payload bytes, and read pages of two records. Providers remain
/// responsible for crash cuts, corruption, cancellation, resource leaks, and
/// implementation-specific capacity tests.
pub async fn check_ring_contract<R>(ring: &R) -> Result<(), String>
where
    R: RingWriter,
{
    let initial = ring
        .status()
        .await
        .map_err(|error| format!("initial status failed: {error}"))?;
    ensure(
        initial.accepted_head == RingCursor::START
            && initial.accepted_tail == RingCursor::START
            && initial.durable_head == RingCursor::START
            && initial.durable_tail == RingCursor::START
            && initial.retained_records == 0
            && initial.retained_payload_bytes == 0,
        format!("new ring status was {initial:?}"),
    )?;

    let empty = ring
        .read(ReadRequest::new(RingCursor::START, 1, 16))
        .await
        .map_err(|error| format!("empty read failed: {error}"))?;
    ensure(
        empty.records.is_empty()
            && empty.next_cursor == RingCursor::START
            && !empty.has_more
            && empty.payload_bytes == 0,
        format!("empty read returned {empty:?}"),
    )?;

    let alpha = b"alpha".to_vec();
    let beta = b"beta".to_vec();
    let first_buffers = vec![alpha.clone(), beta.clone()];
    let appended = ring
        .append(AppendRequest::new(first_buffers.clone()).expecting(RingCursor::START))
        .await
        .map_err(|error| format!("first batch append failed: {error:?}"))?;
    ensure(
        appended.first_position == RingPosition::new(0)
            && appended.next_cursor == RingCursor::new(2)
            && appended.records == first_buffers,
        format!("first append returned {appended:?}"),
    )?;

    let before_sync = ring
        .read(ReadRequest::new(RingCursor::START, 2, 16))
        .await
        .map_err(|error| format!("pre-sync read failed: {error}"))?;
    ensure(
        before_sync.records.is_empty(),
        "read exposed an accepted-but-unsynced batch",
    )?;

    let rejected_buffers = vec![b"stale".to_vec()];
    let conflict = ring
        .append(AppendRequest::new(rejected_buffers.clone()).expecting(RingCursor::START))
        .await
        .expect_err("stale conditional append unexpectedly succeeded");
    ensure(
        conflict.certainty() == CompletionCertainty::NotApplied
            && conflict.error().records == rejected_buffers
            && conflict.error().error
                == (RingError::PositionConflict {
                    expected: RingCursor::START,
                    actual: RingCursor::new(2),
                }),
        format!("conditional append returned {conflict:?}"),
    )?;

    let first_sync = ring
        .sync()
        .await
        .map_err(|error| format!("first sync failed: {error}"))?;
    ensure(
        first_sync.durable_head == RingCursor::START
            && first_sync.durable_tail == RingCursor::new(2),
        format!("first sync returned {first_sync:?}"),
    )?;

    let byte_limited = ring
        .read(ReadRequest::new(RingCursor::START, 2, 4))
        .await
        .expect_err("byte-limited read unexpectedly made progress");
    ensure(
        byte_limited.certainty() == CompletionCertainty::NotApplied
            && *byte_limited.error()
                == (RingError::ReadBudgetTooSmall {
                    needed: 5,
                    available: 4,
                }),
        format!("byte-limited read returned {byte_limited:?}"),
    )?;

    let first_page = ring
        .read(ReadRequest::new(RingCursor::START, 1, 16))
        .await
        .map_err(|error| format!("first page failed: {error}"))?;
    ensure(
        first_page.records.len() == 1
            && first_page.records[0].position == RingPosition::new(0)
            && first_page.records[0].buffer == alpha
            && first_page.next_cursor == RingCursor::new(1)
            && first_page.has_more
            && first_page.payload_bytes == 5,
        format!("first page returned {first_page:?}"),
    )?;

    let second_page = ring
        .read(ReadRequest::new(first_page.next_cursor, 1, 16))
        .await
        .map_err(|error| format!("second page failed: {error}"))?;
    ensure(
        second_page.records.len() == 1
            && second_page.records[0].position == RingPosition::new(1)
            && second_page.records[0].buffer == beta
            && second_page.next_cursor == RingCursor::new(2)
            && !second_page.has_more
            && second_page.payload_bytes == 4,
        format!("second page returned {second_page:?}"),
    )?;

    let gamma = b"gamma".to_vec();
    let third = ring
        .append(AppendRequest::new(vec![gamma.clone()]).expecting(RingCursor::new(2)))
        .await
        .map_err(|error| format!("third append failed: {error:?}"))?;
    ensure(
        third.first_position == RingPosition::new(2)
            && third.next_cursor == RingCursor::new(3)
            && third.records == vec![gamma.clone()],
        format!("third append returned {third:?}"),
    )?;

    let invalid_trim = ring
        .trim(RingCursor::new(3))
        .await
        .expect_err("trim beyond durable tail unexpectedly succeeded");
    ensure(
        invalid_trim.certainty() == CompletionCertainty::NotApplied
            && *invalid_trim.error()
                == (RingError::TrimPastDurableTail {
                    requested: RingCursor::new(3),
                    durable_tail: RingCursor::new(2),
                }),
        format!("invalid trim returned {invalid_trim:?}"),
    )?;

    let trim = ring
        .trim(RingCursor::new(1))
        .await
        .map_err(|error| format!("trim failed: {error}"))?;
    ensure(
        trim.accepted_head == RingCursor::new(1),
        format!("trim returned {trim:?}"),
    )?;
    let repeated_trim = ring
        .trim(RingCursor::START)
        .await
        .map_err(|error| format!("idempotent trim failed: {error}"))?;
    ensure(
        repeated_trim == trim,
        format!("stale trim moved the head: {repeated_trim:?}"),
    )?;

    let pending = ring
        .status()
        .await
        .map_err(|error| format!("pending status failed: {error}"))?;
    ensure(
        pending.accepted_head == RingCursor::new(1)
            && pending.accepted_tail == RingCursor::new(3)
            && pending.durable_head == RingCursor::START
            && pending.durable_tail == RingCursor::new(2)
            && pending.retained_records == 3
            && pending.retained_payload_bytes == 14
            && pending.pending_reclaim_records == 1
            && pending.pending_reclaim_payload_bytes == 5,
        format!("pending status was {pending:?}"),
    )?;

    let pending_read = ring
        .read(ReadRequest::new(RingCursor::START, 2, 16))
        .await
        .map_err(|error| format!("pending-trim read failed: {error}"))?;
    ensure(
        pending_read.records.len() == 2
            && pending_read.records[0].buffer == alpha
            && pending_read.records[1].buffer == beta
            && !pending_read.has_more,
        format!("pending trim changed durable visibility: {pending_read:?}"),
    )?;

    let second_sync = ring
        .sync()
        .await
        .map_err(|error| format!("second sync failed: {error}"))?;
    ensure(
        second_sync.durable_head == RingCursor::new(1)
            && second_sync.durable_tail == RingCursor::new(3)
            && second_sync.reclaimed_records == 1
            && second_sync.reclaimed_payload_bytes == 5,
        format!("second sync returned {second_sync:?}"),
    )?;

    let stale = ring
        .read(ReadRequest::new(RingCursor::START, 2, 16))
        .await
        .expect_err("expired cursor unexpectedly succeeded");
    ensure(
        stale.certainty() == CompletionCertainty::NotApplied
            && *stale.error()
                == (RingError::CursorExpired {
                    requested: RingCursor::START,
                    oldest: RingCursor::new(1),
                }),
        format!("expired cursor returned {stale:?}"),
    )?;

    let retained = ring
        .read(ReadRequest::new(RingCursor::new(1), 2, 16))
        .await
        .map_err(|error| format!("retained read failed: {error}"))?;
    ensure(
        retained.records.len() == 2
            && retained.records[0].buffer == beta
            && retained.records[1].buffer == gamma
            && retained.next_cursor == RingCursor::new(3)
            && !retained.has_more,
        format!("retained read returned {retained:?}"),
    )?;

    let past_tail = ring
        .read(ReadRequest::new(RingCursor::new(99), 1, 16))
        .await
        .map_err(|error| format!("past-tail read failed: {error}"))?;
    ensure(
        past_tail.records.is_empty()
            && past_tail.next_cursor == RingCursor::new(99)
            && !past_tail.has_more,
        format!("past-tail read returned {past_tail:?}"),
    )?;

    let clone = ring.clone();
    drop(clone.append(AppendRequest::new(vec![b"drop".to_vec()])));
    drop(ring.sync());
    let fenced = clone
        .read(ReadRequest::new(RingCursor::new(3), 1, 16))
        .await
        .map_err(|error| format!("cross-clone dropped-future read failed: {error}"))?;
    ensure(
        fenced.records.len() == 1
            && fenced.records[0].position == RingPosition::new(3)
            && fenced.records[0].buffer == b"drop"
            && fenced.next_cursor == RingCursor::new(4)
            && !fenced.has_more,
        format!("cross-clone dropped futures were not fenced: {fenced:?}"),
    )?;

    Ok(())
}

fn ensure(condition: bool, message: impl Into<String>) -> Result<(), String> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
