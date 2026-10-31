//! Write a [Leani](https://leani.dev) processor in Rust and run it as a
//! sandboxed Wasm module, without compiling it into the node.
//!
//! Implement [`Processor`], export it with [`export_processor!`], and build
//! the crate as a `cdylib` for `wasm32-unknown-unknown`:
//!
//! ```no_run
//! use leani_wasm_guest::{Block, BlockRef, Error, Manifest, Processor, Store, export_processor};
//!
//! struct Blocks;
//!
//! impl Processor for Blocks {
//!     type Params = serde_json::Value;
//!     type Delta = u64;
//!
//!     fn manifest(_: &Self::Params) -> Result<Manifest, Error> {
//!         Ok(Manifest::new("blocks"))
//!     }
//!
//!     fn map(_: &Self::Params, block: &Block) -> Result<u64, Error> {
//!         Ok(block.timestamp)
//!     }
//!
//!     fn reduce(_: &Self::Params, block: &BlockRef, timestamp: u64, store: &mut Store) -> Result<(), Error> {
//!         store.upsert("blocks", "block", &block.number.to_be_bytes(), &timestamp)
//!     }
//! }
//!
//! export_processor!(Blocks);
//! ```
//!
//! The host gives a guest no clock, randomness, network, or filesystem, and
//! instantiates it afresh for every call, so `map` is a pure function of the
//! parameters and the block.

pub mod abi;
pub mod solidity;
mod u256;

#[cfg(target_arch = "wasm32")]
#[allow(unsafe_code)]
mod ffi;

use serde::{Serialize, de::DeserializeOwned};

pub use crate::{
    abi::{
        ABI_VERSION, Address, Block, BlockRef, Capability, Entries, Filter, Hash, Header, Log,
        Manifest, MapInput, Receipt, ReduceInput, Reduction, Requirements, TopicFilter,
        Transaction, Withdrawal, Word, WriteOperation, hex, parse_hex,
    },
    u256::U256,
};
pub use serde_json;

/// This SDK's version, which projects that carry a copy of it declare.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// A processor: what it needs, how it maps one block to a delta, and how it
/// reduces the delta into state and published changes.
pub trait Processor {
    /// Operator parameters, decoded from the configured JSON or TOML table.
    type Params: DeserializeOwned;
    /// What `map` passes to `reduce`; stored by the node between the two.
    /// It travels as [postcard](https://postcard.jamesmunns.com), which is
    /// not self-describing: `serde_json::Value`, `#[serde(flatten)]`,
    /// untagged enums, and `skip_serializing_if` do not round-trip.
    type Delta: Serialize + DeserializeOwned;

    /// Declare the material, filter, and reduction mode. The node calls it
    /// once, when it loads or checks the processor.
    ///
    /// # Errors
    ///
    /// Returns an error for invalid parameters, which stops the load.
    fn manifest(params: &Self::Params) -> Result<Manifest, Error>;

    /// Map one block. Runs in parallel and must depend only on its inputs.
    ///
    /// # Errors
    ///
    /// An error fails the block; the node retries and halts the lane rather
    /// than skip it. Skip logs that do not decode instead of failing.
    fn map(params: &Self::Params, block: &Block) -> Result<Self::Delta, Error>;

    /// Apply one block's delta. Blocks reduce in chain order when the
    /// manifest is [`Manifest::ordered`].
    ///
    /// # Errors
    ///
    /// An error fails the block's commit.
    fn reduce(
        params: &Self::Params,
        block: &BlockRef,
        delta: Self::Delta,
        store: &mut Store,
    ) -> Result<(), Error>;
}

/// A processor failure, reported to the node with its message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error(pub String);

impl Error {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl From<String> for Error {
    fn from(message: String) -> Self {
        Self(message)
    }
}

impl From<&str> for Error {
    fn from(message: &str) -> Self {
        Self(message.to_owned())
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self(error.to_string())
    }
}

impl From<postcard::Error> for Error {
    fn from(error: postcard::Error) -> Self {
        Self(error.to_string())
    }
}

/// The node's state for this processor during `reduce`.
///
/// Public collections hold JSON entities, served by the node's API.
/// Private state namespaces hold any bytes and are never exposed. Every
/// access goes to the node's atomic block transaction, and reads see the
/// block's own earlier writes. Reads need [`Manifest::ordered`]: in a
/// block-local processor they fail the call. Every method panics outside a
/// Leani Wasm host.
#[derive(Debug)]
pub struct Store {
    _private: (),
}

impl Store {
    /// Read a public entity's JSON bytes; `None` when the key has none.
    pub fn get(&mut self, collection: &str, key: &[u8]) -> Option<Vec<u8>> {
        host::read(false, collection, key)
    }

    /// Read and decode a public entity; `Ok(None)` when the key has none.
    ///
    /// # Errors
    ///
    /// Returns an error when the stored JSON does not decode as `T`.
    pub fn get_json<T: DeserializeOwned>(
        &mut self,
        collection: &str,
        key: &[u8],
    ) -> Result<Option<T>, Error> {
        self.get(collection, key)
            .map(|bytes| serde_json::from_slice(&bytes))
            .transpose()
            .map_err(Error::from)
    }

    /// Write a public entity. The node rejects a value that is not JSON.
    pub fn put(&mut self, collection: &str, key: &[u8], json: &[u8]) {
        host::write(WriteOperation::Put, collection, key, json);
    }

    /// Delete a public entity.
    pub fn delete(&mut self, collection: &str, key: &[u8]) {
        host::write(WriteOperation::Delete, collection, key, &[]);
    }

    /// Read a private state value; `None` when the key has none.
    pub fn state_get(&mut self, namespace: &str, key: &[u8]) -> Option<Vec<u8>> {
        host::read(true, namespace, key)
    }

    /// Write a private state value.
    pub fn state_put(&mut self, namespace: &str, key: &[u8], value: &[u8]) {
        host::write(WriteOperation::StatePut, namespace, key, value);
    }

    /// Delete a private state value.
    pub fn state_delete(&mut self, namespace: &str, key: &[u8]) {
        host::write(WriteOperation::StateDelete, namespace, key, &[]);
    }

    /// Add `entity_key` to `index` under `index_key`.
    pub fn index_put(&mut self, index: &str, index_key: &[u8], entity_key: &[u8]) {
        host::write(WriteOperation::IndexPut, index, index_key, entity_key);
    }

    /// Remove `entity_key` from `index` under `index_key`.
    pub fn index_delete(&mut self, index: &str, index_key: &[u8], entity_key: &[u8]) {
        host::write(WriteOperation::IndexDelete, index, index_key, entity_key);
    }

    /// The first `limit` public entities whose keys start with `prefix`, in
    /// key order. The node refuses a `limit` above 10,000.
    ///
    /// # Errors
    ///
    /// Returns an error when the node's reply does not decode.
    pub fn scan_prefix(
        &mut self,
        collection: &str,
        prefix: &[u8],
        limit: u32,
    ) -> Result<Entries, Error> {
        host::scan(false, collection, prefix, limit)
    }

    /// The first `limit` private state entries whose keys start with
    /// `prefix`, in key order. The node refuses a `limit` above 10,000.
    ///
    /// # Errors
    ///
    /// Returns an error when the node's reply does not decode.
    pub fn state_scan_prefix(
        &mut self,
        namespace: &str,
        prefix: &[u8],
        limit: u32,
    ) -> Result<Entries, Error> {
        host::scan(true, namespace, prefix, limit)
    }

    /// Publish an upsert change. The node keeps one change per key per
    /// block, the last.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` does not encode as JSON.
    pub fn emit_upsert<T: Serialize>(
        &mut self,
        kind: &str,
        key: &[u8],
        value: &T,
    ) -> Result<(), Error> {
        host::emit(kind, key, false, &serde_json::to_vec(value)?);
        Ok(())
    }

    /// Publish a delete change.
    pub fn emit_delete(&mut self, kind: &str, key: &[u8]) {
        host::emit(kind, key, true, &[]);
    }

    /// Write a public entity and publish it as an upsert change.
    ///
    /// # Errors
    ///
    /// Returns an error when `value` does not encode as JSON.
    pub fn upsert<T: Serialize>(
        &mut self,
        collection: &str,
        kind: &str,
        key: &[u8],
        value: &T,
    ) -> Result<(), Error> {
        let json = serde_json::to_vec(value)?;
        self.put(collection, key, &json);
        host::emit(kind, key, false, &json);
        Ok(())
    }

    /// Delete a public entity and publish the delete.
    pub fn remove(&mut self, collection: &str, kind: &str, key: &[u8]) {
        self.delete(collection, key);
        self.emit_delete(kind, key);
    }
}

/// Keccak-256, computed natively by the host.
///
/// # Panics
///
/// Panics outside a Leani Wasm host.
#[must_use]
pub fn keccak256(bytes: &[u8]) -> Hash {
    host::keccak256(bytes)
}

/// Log a debug message through the node, which bounds and labels it.
pub fn log(message: &str) {
    host::log(message);
}

/// Export `$processor` as the module's Leani entry points, `leani_alloc`,
/// `leani_describe`, `leani_map`, and `leani_reduce`. Call it once, at the
/// crate root; off wasm32 it expands to nothing, so the crate still builds
/// and tests natively.
#[macro_export]
macro_rules! export_processor {
    ($processor:ty) => {
        #[cfg(target_arch = "wasm32")]
        #[unsafe(no_mangle)]
        pub extern "C" fn leani_alloc(len: u32) -> u32 {
            $crate::__private::alloc(len)
        }

        #[cfg(target_arch = "wasm32")]
        #[unsafe(no_mangle)]
        pub extern "C" fn leani_describe(ptr: u32, len: u32) -> u32 {
            $crate::__private::describe::<$processor>(ptr, len)
        }

        #[cfg(target_arch = "wasm32")]
        #[unsafe(no_mangle)]
        pub extern "C" fn leani_map(ptr: u32, len: u32) -> u32 {
            $crate::__private::map::<$processor>(ptr, len)
        }

        #[cfg(target_arch = "wasm32")]
        #[unsafe(no_mangle)]
        pub extern "C" fn leani_reduce(ptr: u32, len: u32) -> u32 {
            $crate::__private::reduce::<$processor>(ptr, len)
        }
    };
}

#[doc(hidden)]
#[cfg(target_arch = "wasm32")]
pub mod __private {
    pub use crate::ffi::{alloc, describe, map, reduce};
}

/// Host access: real imports inside the Wasm host, a clear panic natively.
mod host {
    use crate::{Entries, Error};
    #[cfg(not(target_arch = "wasm32"))]
    use crate::{Hash, WriteOperation};

    #[cfg(target_arch = "wasm32")]
    pub(crate) use crate::ffi::{emit, keccak256, log, read, write};

    pub(crate) fn scan(
        state: bool,
        name: &str,
        prefix: &[u8],
        limit: u32,
    ) -> Result<Entries, Error> {
        #[cfg(target_arch = "wasm32")]
        {
            postcard::from_bytes(&crate::ffi::scan(state, name, prefix, limit)).map_err(Error::from)
        }
        #[cfg(not(target_arch = "wasm32"))]
        {
            let _ = (state, name, prefix, limit);
            outside_host()
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn read(_: bool, _: &str, _: &[u8]) -> Option<Vec<u8>> {
        outside_host()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn write(_: WriteOperation, _: &str, _: &[u8], _: &[u8]) {
        outside_host()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn emit(_: &str, _: &[u8], _: bool, _: &[u8]) {
        outside_host()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn keccak256(_: &[u8]) -> Hash {
        outside_host()
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn log(_: &str) {
        outside_host()
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn outside_host() -> ! {
        panic!("Leani host functions exist only inside the Leani Wasm host")
    }
}
