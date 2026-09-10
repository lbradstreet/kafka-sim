//! Every dereference is justified by the C caller's documented validity window.
//! Numeric checks reject NULL, misalignment, overflow and version mismatch; they
//! cannot make a dangling or fabricated address valid.
use crate::{KR_ERR_INVALID, KR_ERR_VERSION, KrSpan};
use std::mem::{align_of, size_of};
pub(crate) fn check<T>(pointer: *const T, count: usize) -> Result<(), i32> {
    if count == 0 {
        return Ok(());
    }
    let bytes = count
        .checked_mul(size_of::<T>())
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or(KR_ERR_INVALID)?;
    if pointer.is_null()
        || !(pointer as usize).is_multiple_of(align_of::<T>())
        || (pointer as usize).checked_add(bytes).is_none()
    {
        return Err(KR_ERR_INVALID);
    }
    Ok(())
}
pub(crate) unsafe fn versioned<T: Copy>(pointer: *const T) -> Result<T, i32> {
    check(pointer, 1)?;
    // SAFETY: caller guarantees a readable version word at this aligned pointer.
    let version = unsafe { pointer.cast::<u32>().read() };
    if version as usize != size_of::<T>() {
        return Err(KR_ERR_VERSION);
    }
    // SAFETY: matching size plus the caller's readable initialized struct contract.
    Ok(unsafe { pointer.read() })
}
pub(crate) unsafe fn span<'a>(span: KrSpan, limit: usize) -> Result<&'a [u8], i32> {
    if span.len as usize > limit {
        return Err(KR_ERR_INVALID);
    }
    if span.len == 0 {
        return Ok(&[]);
    }
    check(span.ptr, span.len as usize)?;
    // SAFETY: caller keeps this input immutable and readable for the ABI call.
    Ok(unsafe { std::slice::from_raw_parts(span.ptr, span.len as usize) })
}
pub(crate) unsafe fn write<T>(pointer: *mut T, value: T) -> Result<(), i32> {
    check(pointer, 1)?;
    // SAFETY: caller supplies a writable, nonaliasing output of the stated type.
    unsafe { pointer.write(value) };
    Ok(())
}
