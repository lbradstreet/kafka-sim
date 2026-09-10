use std::any::Any;
use std::mem;
use std::panic::{AssertUnwindSafe, catch_unwind};

use crate::task::{MAX_PANIC_MESSAGE_BYTES, PanicRecord};

/// Runs a best-effort callback without allowing an unwind panic to escape.
///
/// This is intended for notification and cleanup boundaries where the work is
/// already committed and there is no useful error to return. Panic payloads
/// are arbitrary user values whose destructors may also panic. The callback's
/// payload is therefore destroyed behind a second unwind boundary; a nested
/// panic payload is leaked rather than invoking another untrusted destructor.
///
/// Callbacks deliberately return `()` so a successful user value cannot be
/// dropped after leaving the containment boundary.
pub fn contain_panic(callback: impl FnOnce()) {
    if let Err(payload) = catch_unwind(AssertUnwindSafe(callback)) {
        discard_panic_payload(payload);
    }
}

pub(crate) fn discard_panic_payload(payload: Box<dyn Any + Send>) {
    if let Err(secondary) = catch_unwind(AssertUnwindSafe(|| drop(payload))) {
        mem::forget(secondary);
    }
}

/// Converts an owned panic payload into the runtime's bounded diagnostic form.
///
/// Runtime unwind boundaries use this after catching a panic. Panic payloads
/// are arbitrary user values, so their destructor is contained behind a
/// second unwind boundary. A nested panic payload is intentionally leaked
/// rather than recursively invoking an untrusted destructor.
pub(crate) fn panic_record_from_payload(payload: Box<dyn Any + Send>) -> PanicRecord {
    let (message, message_truncated) = if let Some(message) = payload.downcast_ref::<&str>() {
        truncate_utf8(message, MAX_PANIC_MESSAGE_BYTES)
    } else if let Some(message) = payload.downcast_ref::<String>() {
        truncate_utf8(message, MAX_PANIC_MESSAGE_BYTES)
    } else {
        ("non-string panic payload".to_owned(), false)
    };
    let record = PanicRecord {
        message,
        message_truncated,
    };
    discard_panic_payload(payload);
    record
}

fn truncate_utf8(value: &str, max_bytes: usize) -> (String, bool) {
    let (end, truncated) = utf8_prefix_end(value, max_bytes);
    (value[..end].to_owned(), truncated)
}

fn utf8_prefix_end(value: &str, max_bytes: usize) -> (usize, bool) {
    if value.len() <= max_bytes {
        return (value.len(), false);
    }
    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    (end, true)
}

#[cfg(test)]
mod tests {
    use std::panic::{AssertUnwindSafe, catch_unwind, panic_any};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    struct PanicOnDrop(Arc<AtomicBool>);

    impl Drop for PanicOnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
            panic!("panic payload destructor escaped");
        }
    }

    #[test]
    fn callback_and_payload_destructor_panics_are_contained() {
        let payload_dropped = Arc::new(AtomicBool::new(false));
        let dropped = Arc::clone(&payload_dropped);

        let boundary = catch_unwind(AssertUnwindSafe(|| {
            contain_panic(|| panic_any(PanicOnDrop(dropped)));
        }));

        assert!(boundary.is_ok(), "nested panic escaped containment");
        assert!(payload_dropped.load(Ordering::SeqCst));
    }

    #[test]
    fn panic_records_are_utf8_safe_and_byte_bounded() {
        let exact = "a".repeat(MAX_PANIC_MESSAGE_BYTES);
        let exact_record = panic_record_from_payload(Box::new(exact));
        assert_eq!(exact_record.message.len(), MAX_PANIC_MESSAGE_BYTES);
        assert!(!exact_record.message_truncated);

        let oversized = "b".repeat(MAX_PANIC_MESSAGE_BYTES + 1);
        let oversized_record = panic_record_from_payload(Box::new(oversized));
        assert_eq!(oversized_record.message.len(), MAX_PANIC_MESSAGE_BYTES);
        assert!(oversized_record.message_truncated);

        let multibyte = format!("{}é", "c".repeat(MAX_PANIC_MESSAGE_BYTES - 1));
        let multibyte_record = panic_record_from_payload(Box::new(multibyte));
        assert_eq!(multibyte_record.message.len(), MAX_PANIC_MESSAGE_BYTES - 1);
        assert!(multibyte_record.message_truncated);

        let non_string_record = panic_record_from_payload(Box::new(17_u64));
        assert_eq!(non_string_record.message, "non-string panic payload");
        assert!(!non_string_record.message_truncated);
    }

    #[test]
    fn panic_payload_destructor_is_contained_while_recording() {
        let payload_dropped = Arc::new(AtomicBool::new(false));
        let dropped = Arc::clone(&payload_dropped);

        let boundary = catch_unwind(AssertUnwindSafe(|| {
            panic_record_from_payload(Box::new(PanicOnDrop(dropped)))
        }));

        let record = boundary.expect("nested panic escaped recording boundary");
        assert_eq!(record.message, "non-string panic payload");
        assert!(payload_dropped.load(Ordering::SeqCst));
    }
}
