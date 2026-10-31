//! ABI v1 imports from module `leani` and the exported entry points.
//!
//! Every pointer is a 32-bit offset into this module's memory. Inputs are
//! written by the host into buffers from `leani_alloc`; the instance lives
//! for one call, so they are never freed. A module exports:
//!
//! | Export | Signature | Called with |
//! |---|---|---|
//! | `memory` | memory | |
//! | `leani_alloc` | `(len: i32) -> i32` | the buffer size the host will write |
//! | `leani_describe` | `(ptr, len) -> i32` | the parameters as JSON |
//! | `leani_map` | `(ptr, len) -> i32` | postcard [`MapInput`] |
//! | `leani_reduce` | `(ptr, len) -> i32` | postcard [`ReduceInput`] |
//!
//! Each entry returns 0 on success; any other value, or a trap, is a failure,
//! whose message the guest reports with `fail`. Results go to `output`: the
//! manifest JSON from `leani_describe`, the delta bytes from `leani_map`. A
//! module may import:
//!
//! | Import | Signature | Effect |
//! |---|---|---|
//! | `output` | `(ptr, len)` | set the call's result |
//! | `fail` | `(ptr, len)` | record the failure message |
//! | `log` | `(ptr, len)` | debug-log up to 16 KiB per call |
//! | `keccak256` | `(ptr, len, out)` | write the 32-byte hash at `out` |
//! | `read` | `(state, name, name_len, key, key_len) -> i32` | value length, or -1 when absent; `state` 1 reads private state |
//! | `scan` | `(state, name, name_len, prefix, prefix_len, limit) -> i32` | length of a postcard `Vec<(Vec<u8>, Vec<u8>)>` |
//! | `take_value` | `(dst)` | copy the last read or scan result to `dst` |
//! | `write` | `(operation, name, name_len, key, key_len, value, value_len) -> i32` | a [`WriteOperation`] |
//! | `emit` | `(kind, kind_len, key, key_len, delete, payload, payload_len)` | publish a change |
//!
//! State imports are available only in `leani_reduce`, and reads only in
//! ordered reduction.

use crate::{Error, Hash, MapInput, Processor, ReduceInput, Store, WriteOperation};

#[link(wasm_import_module = "leani")]
unsafe extern "C" {
    /// Set the call's result bytes.
    fn output(ptr: *const u8, len: usize);
    /// Record the call's failure message.
    fn fail(ptr: *const u8, len: usize);
    #[link_name = "log"]
    fn host_log(ptr: *const u8, len: usize);
    #[link_name = "keccak256"]
    fn host_keccak256(ptr: *const u8, len: usize, out: *mut u8);
    /// Copy the value the last read or scan returned into `dst`.
    fn take_value(dst: *mut u8);
    /// Returns the value's length, or -1 when the key is absent.
    #[link_name = "read"]
    fn host_read(
        state: u32,
        name: *const u8,
        name_len: usize,
        key: *const u8,
        key_len: usize,
    ) -> i32;
    /// Returns the length of the encoded `(key, value)` list.
    #[link_name = "scan"]
    fn host_scan(
        state: u32,
        name: *const u8,
        name_len: usize,
        prefix: *const u8,
        prefix_len: usize,
        limit: u32,
    ) -> i32;
    #[link_name = "write"]
    fn host_write(
        operation: u32,
        name: *const u8,
        name_len: usize,
        key: *const u8,
        key_len: usize,
        value: *const u8,
        value_len: usize,
    ) -> i32;
    #[link_name = "emit"]
    fn host_emit(
        kind: *const u8,
        kind_len: usize,
        key: *const u8,
        key_len: usize,
        delete: u32,
        payload: *const u8,
        payload_len: usize,
    );
}

// Wasm32 pointers are 32 bits wide.
#[allow(clippy::cast_possible_truncation)]
pub fn alloc(len: u32) -> u32 {
    let buffer = vec![0_u8; len as usize].leak();
    buffer.as_mut_ptr() as usize as u32
}

pub fn describe<P: Processor>(ptr: u32, len: u32) -> u32 {
    run(|| {
        let params = params::<P>(input(ptr, len))?;
        set_output(&serde_json::to_vec(&P::manifest(&params)?)?);
        Ok(())
    })
}

pub fn map<P: Processor>(ptr: u32, len: u32) -> u32 {
    run(|| {
        let input: MapInput = postcard::from_bytes(input(ptr, len))
            .map_err(|error| Error::new(format!("the map input does not decode: {error}")))?;
        let delta = P::map(&params::<P>(&input.params)?, &input.block)?;
        set_output(&postcard::to_allocvec(&delta)?);
        Ok(())
    })
}

pub fn reduce<P: Processor>(ptr: u32, len: u32) -> u32 {
    run(|| {
        let input: ReduceInput = postcard::from_bytes(input(ptr, len))
            .map_err(|error| Error::new(format!("the reduce input does not decode: {error}")))?;
        let delta = postcard::from_bytes(&input.delta)
            .map_err(|error| Error::new(format!("the delta does not decode: {error}")))?;
        P::reduce(
            &params::<P>(&input.params)?,
            &input.block,
            delta,
            &mut Store { _private: () },
        )
    })
}

fn params<P: Processor>(json: &[u8]) -> Result<P::Params, Error> {
    serde_json::from_slice(json).map_err(|error| Error::new(format!("invalid params: {error}")))
}

fn run(body: impl FnOnce() -> Result<(), Error>) -> u32 {
    std::panic::set_hook(Box::new(|info| report_failure(&info.to_string())));
    match body() {
        Ok(()) => 0,
        Err(error) => {
            report_failure(&error.0);
            1
        }
    }
}

fn input(ptr: u32, len: u32) -> &'static [u8] {
    // SAFETY: the host wrote `len` bytes at `ptr`, a leaked `alloc` buffer.
    unsafe { core::slice::from_raw_parts(ptr as usize as *const u8, len as usize) }
}

fn set_output(bytes: &[u8]) {
    // SAFETY: the host copies `bytes` before the call returns.
    unsafe { output(bytes.as_ptr(), bytes.len()) }
}

fn report_failure(message: &str) {
    // SAFETY: as `set_output`.
    unsafe { fail(message.as_ptr(), message.len()) }
}

fn take(len: i32) -> Vec<u8> {
    let mut value = vec![0_u8; usize::try_from(len).unwrap_or(0)];
    // SAFETY: the host writes exactly the `len` bytes it announced.
    unsafe { take_value(value.as_mut_ptr()) };
    value
}

pub(crate) fn read(state: bool, name: &str, key: &[u8]) -> Option<Vec<u8>> {
    // SAFETY: the host only reads the given ranges.
    let len = unsafe {
        host_read(
            u32::from(state),
            name.as_ptr(),
            name.len(),
            key.as_ptr(),
            key.len(),
        )
    };
    (len >= 0).then(|| take(len))
}

pub(crate) fn scan(state: bool, name: &str, prefix: &[u8], limit: u32) -> Vec<u8> {
    // SAFETY: as `read`.
    let len = unsafe {
        host_scan(
            u32::from(state),
            name.as_ptr(),
            name.len(),
            prefix.as_ptr(),
            prefix.len(),
            limit,
        )
    };
    take(len)
}

pub(crate) fn write(operation: WriteOperation, name: &str, key: &[u8], value: &[u8]) {
    // SAFETY: as `read`.
    unsafe {
        host_write(
            operation as u32,
            name.as_ptr(),
            name.len(),
            key.as_ptr(),
            key.len(),
            value.as_ptr(),
            value.len(),
        );
    }
}

pub(crate) fn emit(kind: &str, key: &[u8], delete: bool, payload: &[u8]) {
    // SAFETY: as `read`.
    unsafe {
        host_emit(
            kind.as_ptr(),
            kind.len(),
            key.as_ptr(),
            key.len(),
            u32::from(delete),
            payload.as_ptr(),
            payload.len(),
        );
    }
}

pub(crate) fn keccak256(bytes: &[u8]) -> Hash {
    let mut hash = [0_u8; 32];
    // SAFETY: the host writes exactly 32 bytes at `hash`.
    unsafe { host_keccak256(bytes.as_ptr(), bytes.len(), hash.as_mut_ptr()) };
    hash
}

pub(crate) fn log(message: &str) {
    // SAFETY: as `read`.
    unsafe { host_log(message.as_ptr(), message.len()) }
}
