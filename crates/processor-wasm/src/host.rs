//! The wasmi host: engine, ABI v1 imports, and call drivers.
//!
//! State access in `reduce` is asynchronous, but the interpreter is not.
//! Writes queue in the host; a read suspends the guest with a [`Suspend`]
//! host trap, the driver applies the queued writes and serves the read
//! against the block's transaction, and the call resumes with the result.
//! Reads therefore see every earlier write of the same block, as with a
//! native reducer, while a write-only reduce never suspends.

use std::{collections::HashMap, fmt, sync::LazyLock};

use leani_processor_api::{ChangeOperation, DomainChange, ProcessorError, ReducerTransaction};
use leani_wasm_guest::{Manifest, WriteOperation};
use wasmi::{
    Caller, Config, EnforcedLimits, Engine, Extern, ExternType, Instance, Linker, Memory, Module,
    Store, StoreLimits, StoreLimitsBuilder, TrapCode, TypedResumableCall, Val, ValType,
    errors::HostError,
};

use crate::{WasmLimits, WasmModule, guest_error};

pub(crate) static ENGINE: LazyLock<Engine> = LazyLock::new(|| {
    let mut config = Config::default();
    config.consume_fuel(true);
    // Modules may come from untrusted uploads: bound their shape, which
    // costs host time and memory before fuel and the store limits apply.
    config.enforced_limits(EnforcedLimits::strict());
    Engine::new(&config)
});

static LINKER: LazyLock<Linker<HostState>> = LazyLock::new(linker);

const IMPORTS: [&str; 9] = [
    "output",
    "fail",
    "log",
    "keccak256",
    "take_value",
    "read",
    "scan",
    "write",
    "emit",
];
/// Entry points and their number of `i32` parameters; each returns an `i32`.
const ENTRY_POINTS: [(&str, usize); 4] = [
    ("leani_alloc", 1),
    ("leani_describe", 2),
    ("leani_map", 2),
    ("leani_reduce", 2),
];
/// Longest collection, namespace, index, or change-kind name.
const NAME_BYTES: u64 = 128;
/// Longest state, entity, or index key.
const KEY_BYTES: u64 = 64 << 10;
/// Longest failure message kept.
const FAILURE_BYTES: u32 = 4 << 10;
/// Log bytes one call may write.
const LOG_BYTES: usize = 16 << 10;
/// Most entries one scan may return.
const SCAN_ENTRIES: u32 = 10_000;
/// Queued write bytes that force a flush.
const QUEUED_BYTES: usize = 16 << 20;
/// Bytes of change kinds, keys, and payloads one call may hold: they stay
/// in host memory until the call ends.
const CHANGE_BYTES: usize = 64 << 20;
/// Elements of the module's one table, which holds the functions it calls
/// indirectly: a few hundred in practice, allocated on every instantiation.
const TABLE_ELEMENTS: usize = 10_000;

pub(crate) fn check_interface(module: &Module) -> Result<(), String> {
    for import in module.imports() {
        if import.module() != "leani" || !IMPORTS.contains(&import.name()) {
            return Err(format!(
                "it imports `{}.{}`, which is not part of the Leani ABI; build for wasm32-unknown-unknown without WASI",
                import.module(),
                import.name()
            ));
        }
    }
    if !matches!(module.get_export("memory"), Some(ExternType::Memory(_))) {
        return Err("it does not export `memory`".to_owned());
    }
    for (name, params) in ENTRY_POINTS {
        let fits = matches!(
            module.get_export(name),
            Some(ExternType::Func(signature))
                if signature.params() == &[ValType::I32; 2][..params]
                    && signature.results() == [ValType::I32]
        );
        if !fits {
            return Err(format!(
                "it does not export `{name}` as the Leani ABI defines it; a guest SDK exports it"
            ));
        }
    }
    Ok(())
}

pub(crate) fn describe(
    module: &WasmModule,
    params: &[u8],
    limits: &WasmLimits,
    label: &str,
) -> Result<Manifest, ProcessorError> {
    let (mut store, instance) = instantiate(module, limits, Phase::Describe, label)?;
    let output = call(&mut store, instance, "describe", params)?;
    serde_json::from_slice(&output)
        .map_err(|error| guest_error("describe", &format!("the manifest is invalid: {error}")))
}

pub(crate) fn map(
    module: &WasmModule,
    input: &[u8],
    limits: &WasmLimits,
    label: &str,
) -> Result<Vec<u8>, ProcessorError> {
    let (mut store, instance) = instantiate(module, limits, Phase::Map, label)?;
    call(&mut store, instance, "map", input)
}

pub(crate) async fn reduce(
    module: &WasmModule,
    input: &[u8],
    limits: &WasmLimits,
    ordered: bool,
    label: &str,
    transaction: &mut dyn ReducerTransaction,
) -> Result<Vec<DomainChange>, ProcessorError> {
    let (mut store, instance) = instantiate(module, limits, Phase::Reduce { ordered }, label)?;
    let (pointer, len) = write_input(&mut store, instance, input, "reduce")?;
    let entry = instance
        .get_typed_func::<(u32, u32), u32>(&store, "leani_reduce")
        .map_err(|error| guest_error("reduce", &error.to_string()))?;
    let mut call = entry
        .call_resumable(&mut store, (pointer, len))
        .map_err(|error| failure(&store, "reduce", Some(&error)))?;
    let status = loop {
        match call {
            TypedResumableCall::Finished(status) => break status,
            TypedResumableCall::HostTrap(invocation) => {
                let Some(request) = store.data_mut().pending.take() else {
                    return Err(failure(&store, "reduce", Some(invocation.host_error())));
                };
                flush(transaction, store.data_mut()).await?;
                let result = serve(request, transaction, store.data_mut()).await?;
                call = invocation
                    .resume(&mut store, &[Val::I32(result)])
                    .map_err(|error| failure(&store, "reduce", Some(&error)))?;
            }
            TypedResumableCall::OutOfFuel(_) => {
                return Err(guest_error("reduce", &out_of_fuel(limits)));
            }
        }
    };
    if status != 0 {
        return Err(failure(&store, "reduce", None));
    }
    flush(transaction, store.data_mut()).await?;
    Ok(last_change_per_key(std::mem::take(
        &mut store.data_mut().changes,
    )))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Describe,
    Map,
    Reduce { ordered: bool },
}

#[derive(Debug)]
struct HostState {
    store_limits: StoreLimits,
    limits: WasmLimits,
    phase: Phase,
    label: String,
    output: Option<Vec<u8>>,
    failure: Option<String>,
    /// The value of the last read or scan, until the guest takes it.
    value: Option<Vec<u8>>,
    pending: Option<Request>,
    /// Writes not yet applied, in order.
    writes: Vec<Request>,
    queued_bytes: usize,
    changes: Vec<DomainChange>,
    change_bytes: usize,
    host_calls: u64,
    log_bytes: usize,
}

#[derive(Debug)]
enum Request {
    Read {
        state: bool,
        name: String,
        key: Vec<u8>,
    },
    Scan {
        state: bool,
        name: String,
        prefix: Vec<u8>,
        limit: usize,
    },
    Write {
        operation: WriteOperation,
        name: String,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// Apply the queued writes only.
    Flush,
}

/// The host trap that suspends a guest until its state access is served.
#[derive(Debug)]
struct Suspend;

impl fmt::Display for Suspend {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("suspended for a state access")
    }
}

impl HostError for Suspend {}

fn instantiate(
    module: &WasmModule,
    limits: &WasmLimits,
    phase: Phase,
    label: &str,
) -> Result<(Store<HostState>, Instance), ProcessorError> {
    let mut store = Store::new(
        &ENGINE,
        HostState {
            store_limits: StoreLimitsBuilder::new()
                .memory_size(usize::try_from(limits.memory_bytes).unwrap_or(usize::MAX))
                .memories(1)
                .tables(1)
                .table_elements(TABLE_ELEMENTS)
                .instances(1)
                .build(),
            limits: *limits,
            phase,
            label: label.to_owned(),
            output: None,
            failure: None,
            value: None,
            pending: None,
            writes: Vec::new(),
            queued_bytes: 0,
            changes: Vec::new(),
            change_bytes: 0,
            host_calls: 0,
            log_bytes: 0,
        },
    );
    store.limiter(|state| &mut state.store_limits);
    let name = phase_name(phase);
    store
        .set_fuel(limits.fuel)
        .map_err(|error| guest_error(name, &error.to_string()))?;
    let instance = LINKER
        .instantiate_and_start(&mut store, &module.module)
        .map_err(|error| failure(&store, name, Some(&error)))?;
    Ok((store, instance))
}

/// Copy `input` into a guest buffer from `leani_alloc`.
fn write_input(
    store: &mut Store<HostState>,
    instance: Instance,
    input: &[u8],
    call: &str,
) -> Result<(u32, u32), ProcessorError> {
    let len =
        u32::try_from(input.len()).map_err(|_| guest_error(call, "the input exceeds 4 GiB"))?;
    let pointer = instance
        .get_typed_func::<u32, u32>(&*store, "leani_alloc")
        .and_then(|alloc| alloc.call(&mut *store, len))
        .map_err(|error| failure(store, call, Some(&error)))?;
    instance
        .get_export(&*store, "memory")
        .and_then(Extern::into_memory)
        .ok_or_else(|| guest_error(call, "the module exports no memory"))?
        .write(&mut *store, address(pointer), input)
        .map_err(|_| guest_error(call, "`leani_alloc` returned a buffer outside memory"))?;
    Ok((pointer, len))
}

/// Run a non-suspending entry point and return the bytes it output.
fn call(
    store: &mut Store<HostState>,
    instance: Instance,
    call: &str,
    input: &[u8],
) -> Result<Vec<u8>, ProcessorError> {
    let (pointer, len) = write_input(store, instance, input, call)?;
    let status = instance
        .get_typed_func::<(u32, u32), u32>(&*store, &format!("leani_{call}"))
        .and_then(|entry| entry.call(&mut *store, (pointer, len)))
        .map_err(|error| failure(store, call, Some(&error)))?;
    if status != 0 {
        return Err(failure(store, call, None));
    }
    store
        .data_mut()
        .output
        .take()
        .ok_or_else(|| guest_error(call, "returned no output"))
}

fn failure(store: &Store<HostState>, call: &str, error: Option<&wasmi::Error>) -> ProcessorError {
    let state = store.data();
    let message = if error.and_then(wasmi::Error::as_trap_code) == Some(TrapCode::OutOfFuel) {
        out_of_fuel(&state.limits)
    } else if let Some(reported) = &state.failure {
        reported.clone()
    } else if let Some(error) = error {
        error.to_string()
    } else {
        "failed without a message".to_owned()
    };
    guest_error(call, &message)
}

fn out_of_fuel(limits: &WasmLimits) -> String {
    format!("exhausted its fuel limit of {}", limits.fuel)
}

fn phase_name(phase: Phase) -> &'static str {
    match phase {
        Phase::Describe => "describe",
        Phase::Map => "map",
        Phase::Reduce { .. } => "reduce",
    }
}

async fn flush(
    transaction: &mut dyn ReducerTransaction,
    state: &mut HostState,
) -> Result<(), ProcessorError> {
    for write in std::mem::take(&mut state.writes) {
        serve(write, transaction, state).await?;
    }
    state.queued_bytes = 0;
    Ok(())
}

async fn serve(
    request: Request,
    transaction: &mut dyn ReducerTransaction,
    state: &mut HostState,
) -> Result<i32, ProcessorError> {
    let value = match request {
        Request::Read {
            state: true,
            name,
            key,
        } => transaction.state_get(&name, &key).await?,
        Request::Read { name, key, .. } => transaction.get(&name, &key).await?,
        Request::Scan {
            state: private,
            name,
            prefix,
            limit,
        } => {
            let entries = if private {
                transaction.state_scan_prefix(&name, &prefix, limit).await?
            } else {
                transaction.scan_prefix(&name, &prefix, limit).await?
            };
            Some(
                postcard::to_allocvec(&entries)
                    .map_err(|error| ProcessorError::State(error.to_string()))?,
            )
        }
        Request::Write {
            operation,
            name,
            key,
            value,
        } => {
            match operation {
                WriteOperation::Put => transaction.put(&name, key, value).await?,
                WriteOperation::Delete => transaction.delete(&name, &key).await?,
                WriteOperation::StatePut => transaction.state_put(&name, key, value).await?,
                WriteOperation::StateDelete => transaction.state_delete(&name, &key).await?,
                WriteOperation::IndexPut => transaction.index_put(&name, key, value).await?,
                WriteOperation::IndexDelete => {
                    transaction.index_delete(&name, &key, &value).await?;
                }
            }
            return Ok(0);
        }
        Request::Flush => return Ok(0),
    };
    Ok(match value {
        Some(value) => {
            let len = i32::try_from(value.len())
                .map_err(|_| ProcessorError::State("a stored value exceeds 2 GiB".to_owned()))?;
            state.value = Some(value);
            len
        }
        None => -1,
    })
}

/// Keep the last change of each key, in emission order: a block publishes a
/// key's net mutation once.
fn last_change_per_key(changes: Vec<DomainChange>) -> Vec<DomainChange> {
    let mut last = HashMap::with_capacity(changes.len());
    for (index, change) in changes.iter().enumerate() {
        last.insert(change.key.as_slice(), index);
    }
    let keep = changes
        .iter()
        .enumerate()
        .map(|(index, change)| last.get(change.key.as_slice()) == Some(&index))
        .collect::<Vec<_>>();
    changes
        .into_iter()
        .zip(keep)
        .filter_map(|(change, keep)| keep.then_some(change))
        .collect()
}

#[allow(clippy::too_many_lines)]
fn linker() -> Linker<HostState> {
    let mut linker = Linker::new(&ENGINE);
    linker
        .func_wrap(
            "leani",
            "output",
            |mut caller: Caller<'_, HostState>, pointer: u32, len: u32| {
                let limit = caller.data().limits.value_bytes;
                let bytes = read(&mut caller, pointer, len, limit, "the output")?;
                caller.data_mut().output = Some(bytes);
                Ok::<_, wasmi::Error>(())
            },
        )
        .expect("define leani.output");
    linker
        .func_wrap(
            "leani",
            "fail",
            |mut caller: Caller<'_, HostState>, pointer: u32, len: u32| {
                let bytes = read(
                    &mut caller,
                    pointer,
                    len.min(FAILURE_BYTES),
                    u64::MAX,
                    "a failure",
                )?;
                caller.data_mut().failure = Some(String::from_utf8_lossy(&bytes).into_owned());
                Ok::<_, wasmi::Error>(())
            },
        )
        .expect("define leani.fail");
    linker
        .func_wrap(
            "leani",
            "log",
            |mut caller: Caller<'_, HostState>, pointer: u32, len: u32| {
                let remaining = LOG_BYTES.saturating_sub(caller.data().log_bytes);
                let len = len.min(u32::try_from(remaining).unwrap_or(u32::MAX));
                if len > 0 {
                    let bytes = read(&mut caller, pointer, len, u64::MAX, "a log message")?;
                    let state = caller.data_mut();
                    state.log_bytes += bytes.len();
                    tracing::debug!(
                        target: "leani::wasm",
                        processor_instance = %state.label,
                        "{}",
                        String::from_utf8_lossy(&bytes)
                    );
                }
                Ok::<_, wasmi::Error>(())
            },
        )
        .expect("define leani.log");
    linker
        .func_wrap(
            "leani",
            "keccak256",
            |mut caller: Caller<'_, HostState>, pointer: u32, len: u32, out: u32| {
                let limit = caller.data().limits.value_bytes;
                let bytes = read(&mut caller, pointer, len, limit, "keccak256 input")?;
                let hash = alloy_primitives::keccak256(&bytes);
                write(&mut caller, out, hash.as_slice(), "the keccak256 output")
            },
        )
        .expect("define leani.keccak256");
    linker
        .func_wrap(
            "leani",
            "take_value",
            |mut caller: Caller<'_, HostState>, destination: u32| {
                let value = caller
                    .data_mut()
                    .value
                    .take()
                    .ok_or_else(|| wasmi::Error::new("take_value without a read or scan"))?;
                write(&mut caller, destination, &value, "the value")
            },
        )
        .expect("define leani.take_value");
    linker
        .func_wrap(
            "leani",
            "read",
            |mut caller: Caller<'_, HostState>,
             state: u32,
             name: u32,
             name_len: u32,
             key: u32,
             key_len: u32| {
                reading(&mut caller)?;
                let request = Request::Read {
                    state: state != 0,
                    name: read_name(&mut caller, name, name_len)?,
                    key: read(&mut caller, key, key_len, KEY_BYTES, "a key")?,
                };
                suspend::<i32>(&mut caller, request)
            },
        )
        .expect("define leani.read");
    linker
        .func_wrap(
            "leani",
            "scan",
            |mut caller: Caller<'_, HostState>,
             state: u32,
             name: u32,
             name_len: u32,
             prefix: u32,
             prefix_len: u32,
             limit: u32| {
                reading(&mut caller)?;
                if limit > SCAN_ENTRIES {
                    return Err(wasmi::Error::new(format!(
                        "a scan returns at most {SCAN_ENTRIES} entries, not {limit}"
                    )));
                }
                let request = Request::Scan {
                    state: state != 0,
                    name: read_name(&mut caller, name, name_len)?,
                    prefix: read(&mut caller, prefix, prefix_len, KEY_BYTES, "a prefix")?,
                    limit: usize::try_from(limit).unwrap_or(usize::MAX),
                };
                suspend::<i32>(&mut caller, request)
            },
        )
        .expect("define leani.scan");
    linker
        .func_wrap(
            "leani",
            "write",
            |mut caller: Caller<'_, HostState>,
             operation: u32,
             name: u32,
             name_len: u32,
             key: u32,
             key_len: u32,
             value: u32,
             value_len: u32| {
                reducing(&mut caller)?;
                let operation = WriteOperation::try_from(operation)
                    .map_err(|code| wasmi::Error::new(format!("unknown write operation {code}")))?;
                let limit = match operation {
                    WriteOperation::IndexPut | WriteOperation::IndexDelete => KEY_BYTES,
                    _ => caller.data().limits.value_bytes,
                };
                let value = read(&mut caller, value, value_len, limit, "a value")?;
                if operation == WriteOperation::Put {
                    json_value(&value, "an entity")?;
                }
                let key = read(&mut caller, key, key_len, KEY_BYTES, "a key")?;
                let name = read_name(&mut caller, name, name_len)?;
                let state = caller.data_mut();
                state.queued_bytes += name.len() + key.len() + value.len();
                state.writes.push(Request::Write {
                    operation,
                    name,
                    key,
                    value,
                });
                if state.queued_bytes > QUEUED_BYTES {
                    return suspend::<i32>(&mut caller, Request::Flush);
                }
                Ok(0)
            },
        )
        .expect("define leani.write");
    linker
        .func_wrap(
            "leani",
            "emit",
            |mut caller: Caller<'_, HostState>,
             kind: u32,
             kind_len: u32,
             key: u32,
             key_len: u32,
             delete: u32,
             payload: u32,
             payload_len: u32| {
                reducing(&mut caller)?;
                let state = caller.data();
                if u64::try_from(state.changes.len()).unwrap_or(u64::MAX) >= state.limits.changes {
                    return Err(wasmi::Error::new(format!(
                        "a block may emit at most {} changes",
                        state.limits.changes
                    )));
                }
                let limit = state.limits.value_bytes;
                let payload = read(&mut caller, payload, payload_len, limit, "a change payload")?;
                let operation = if delete == 0 {
                    json_value(&payload, "an upsert change")?;
                    ChangeOperation::Upsert
                } else {
                    if !payload.is_empty() {
                        json_value(&payload, "a delete change")?;
                    }
                    ChangeOperation::Delete
                };
                let change = DomainChange {
                    kind: read_name(&mut caller, kind, kind_len)?,
                    key: read(&mut caller, key, key_len, KEY_BYTES, "a change key")?,
                    operation,
                    payload,
                };
                let bytes = change.kind.len() + change.key.len() + change.payload.len();
                let state = caller.data_mut();
                if state.change_bytes + bytes > CHANGE_BYTES {
                    return Err(wasmi::Error::new(format!(
                        "a call's changes may hold at most {CHANGE_BYTES} bytes"
                    )));
                }
                state.change_bytes += bytes;
                state.changes.push(change);
                Ok(())
            },
        )
        .expect("define leani.emit");
    linker
}

fn suspend<T>(caller: &mut Caller<'_, HostState>, request: Request) -> Result<T, wasmi::Error> {
    caller.data_mut().pending = Some(request);
    Err(wasmi::Error::host(Suspend))
}

/// Count a state read, write, or change emit, which only reduce may make,
/// against the host-call budget, and return whether the reduction is ordered.
fn reducing(caller: &mut Caller<'_, HostState>) -> Result<bool, wasmi::Error> {
    let state = caller.data_mut();
    let Phase::Reduce { ordered } = state.phase else {
        return Err(wasmi::Error::new(
            "state is available only in reduce, not in describe or map",
        ));
    };
    state.host_calls += 1;
    if state.host_calls > state.limits.host_calls {
        return Err(wasmi::Error::new(format!(
            "a block may make at most {} state accesses",
            state.limits.host_calls
        )));
    }
    Ok(ordered)
}

/// Allow a state read: a block-local reduce must not depend on other blocks.
fn reading(caller: &mut Caller<'_, HostState>) -> Result<(), wasmi::Error> {
    if reducing(caller)? {
        Ok(())
    } else {
        Err(wasmi::Error::new(
            "a block_local processor cannot read state; declare ordered reduction to read it",
        ))
    }
}

fn json_value(bytes: &[u8], what: &str) -> Result<(), wasmi::Error> {
    serde_json::from_slice::<serde::de::IgnoredAny>(bytes)
        .map(|_| ())
        .map_err(|error| wasmi::Error::new(format!("{what} is not JSON: {error}")))
}

fn memory(caller: &Caller<'_, HostState>) -> Result<Memory, wasmi::Error> {
    caller
        .get_export("memory")
        .and_then(Extern::into_memory)
        .ok_or_else(|| wasmi::Error::new("the module exports no memory"))
}

fn address(pointer: u32) -> usize {
    usize::try_from(pointer).unwrap_or(usize::MAX)
}

/// Copy guest bytes out, for one unit of fuel per byte: wasmi counts a host
/// call as one instruction, so the host charges for the work its bytes
/// cause, such as hashing or parsing them.
fn read(
    caller: &mut Caller<'_, HostState>,
    pointer: u32,
    len: u32,
    limit: u64,
    what: &str,
) -> Result<Vec<u8>, wasmi::Error> {
    if u64::from(len) > limit {
        return Err(wasmi::Error::new(format!(
            "{what} is {len} bytes, more than the {limit}-byte limit"
        )));
    }
    charge(caller, address(len))?;
    let mut bytes = vec![0; address(len)];
    memory(caller)?
        .read(&*caller, address(pointer), &mut bytes)
        .map_err(|_| wasmi::Error::new(format!("{what} lies outside the module's memory")))?;
    Ok(bytes)
}

fn read_name(
    caller: &mut Caller<'_, HostState>,
    pointer: u32,
    len: u32,
) -> Result<String, wasmi::Error> {
    let bytes = read(caller, pointer, len, NAME_BYTES, "a name")?;
    match String::from_utf8(bytes) {
        Ok(name) if !name.is_empty() => Ok(name),
        _ => Err(wasmi::Error::new("a name must be non-empty UTF-8")),
    }
}

/// Copy bytes into the guest, for one unit of fuel per byte.
fn write(
    caller: &mut Caller<'_, HostState>,
    pointer: u32,
    bytes: &[u8],
    what: &str,
) -> Result<(), wasmi::Error> {
    charge(caller, bytes.len())?;
    memory(caller)?
        .write(caller, address(pointer), bytes)
        .map_err(|_| wasmi::Error::new(format!("{what} lies outside the module's memory")))
}

fn charge(caller: &mut Caller<'_, HostState>, bytes: usize) -> Result<(), wasmi::Error> {
    let fuel = caller.get_fuel()?;
    match fuel.checked_sub(u64::try_from(bytes).unwrap_or(u64::MAX)) {
        Some(left) => caller.set_fuel(left),
        None => Err(TrapCode::OutOfFuel.into()),
    }
}
