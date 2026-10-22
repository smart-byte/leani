// Write a Leani processor in TypeScript, as AssemblyScript compiles it, and
// run it as a sandboxed Wasm module without rebuilding the node.
//
// Extend `Processor`, register it, and export the entry points:
//
//   import { Block, BlockRef, Json, Manifest, Processor, Reader, Store, Writer, bigEndian64, register } from "./leani";
//   export * from "./leani/exports";
//
//   class Blocks extends Processor {
//     manifest(params: Json): Manifest {
//       return new Manifest("blocks");
//     }
//
//     map(params: Json, block: Block, delta: Writer): void {
//       delta.uint(block.timestamp);
//     }
//
//     reduce(params: Json, block: BlockRef, delta: Reader, store: Store): void {
//       store.upsert("blocks", "block", bigEndian64(block.number), Json.object().set("timestamp", delta.uint()));
//     }
//   }
//
//   register(new Blocks());
//
// The host gives a guest no clock, randomness, network, or filesystem, and
// instantiates it afresh for every call, so `map` is a pure function of the
// parameters and the block. A thrown error fails the call with its message.

import { Block, BlockRef, Manifest } from "./abi";
import { text, utf8 } from "./bytes";
import * as ffi from "./ffi";
import { Json, Jsonable } from "./json";
import { Reader, Writer } from "./postcard";

export { ABI_VERSION, Block, BlockRef, Capability, Header, Log, Manifest, Receipt, Some, Transaction, Withdrawal } from "./abi";
export { bigEndian32, bigEndian64, concat, equal, hex, indexOf, parseHex, text, utf8 } from "./bytes";
export { Json, JsonKind, Jsonable } from "./json";
export { Reader, Writer } from "./postcard";
export { U256 } from "./u256";

/** A processor: what it needs, how it maps one block to a delta, and how it reduces the delta into state and published changes. */
export abstract class Processor {
  /** Declare the material, filter, and reduction mode. Throw for invalid parameters. */
  abstract manifest(params: Json): Manifest;

  /**
   * Map one block, writing what `reduce` needs to `delta`. Runs in parallel
   * and must depend only on its inputs. A thrown error fails the block; the
   * node retries and halts the lane rather than skip it, so skip logs that
   * do not decode instead.
   */
  abstract map(params: Json, block: Block, delta: Writer): void;

  /**
   * Apply one block's delta. Blocks reduce in chain order when the manifest
   * is `ordered()`. A thrown error fails the block's commit.
   */
  abstract reduce(params: Json, block: BlockRef, delta: Reader, store: Store): void;
}

let registered: Processor | null = null;

/** Make `processor` the one the module's entry points run. */
export function register(processor: Processor): void {
  registered = processor;
}

/** The registered processor. */
export function processor(): Processor {
  const current = registered;
  if (current == null) throw new Error("no processor is registered: call `register(new YourProcessor())`");
  return current;
}

/** A key and its value, as a scan returns them. */
export class Entry {
  constructor(
    public key: Uint8Array,
    public value: Uint8Array,
  ) {}
}

/**
 * The node's state for this processor during `reduce`.
 *
 * Public collections hold JSON entities, served by the node's API. Private
 * state namespaces hold any bytes and are never exposed. Every access goes
 * to the node's atomic block transaction.
 */
export class Store {
  /** Read a public entity's JSON bytes. */
  get(collection: string, key: Uint8Array): Uint8Array | null {
    return ffi.read(false, collection, key);
  }

  /** Read and parse a public entity. */
  getJson(collection: string, key: Uint8Array): Json | null {
    const bytes = this.get(collection, key);
    return bytes == null ? null : Json.parse(text(bytes));
  }

  /** Write a public entity. */
  put(collection: string, key: Uint8Array, value: Jsonable): void {
    ffi.write(ffi.PUT, collection, key, json(value));
  }

  /** Delete a public entity. */
  delete(collection: string, key: Uint8Array): void {
    ffi.write(ffi.DELETE, collection, key, new Uint8Array(0));
  }

  /** Read a private state value, `null` when the key has none. */
  stateGet(namespace: string, key: Uint8Array): Uint8Array | null {
    return ffi.read(true, namespace, key);
  }

  /** Write a private state value. */
  statePut(namespace: string, key: Uint8Array, value: Uint8Array): void {
    ffi.write(ffi.STATE_PUT, namespace, key, value);
  }

  /** Delete a private state value. */
  stateDelete(namespace: string, key: Uint8Array): void {
    ffi.write(ffi.STATE_DELETE, namespace, key, new Uint8Array(0));
  }

  /** Add `entityKey` to `index` under `indexKey`. */
  indexPut(index: string, indexKey: Uint8Array, entityKey: Uint8Array): void {
    ffi.write(ffi.INDEX_PUT, index, indexKey, entityKey);
  }

  /** Remove `entityKey` from `index` under `indexKey`. */
  indexDelete(index: string, indexKey: Uint8Array, entityKey: Uint8Array): void {
    ffi.write(ffi.INDEX_DELETE, index, indexKey, entityKey);
  }

  /** Public entities whose keys start with `prefix`, in key order. */
  scanPrefix(collection: string, prefix: Uint8Array, limit: u32): Entry[] {
    return entries(ffi.scan(false, collection, prefix, limit));
  }

  /** Private state entries whose keys start with `prefix`, in key order. */
  stateScanPrefix(namespace: string, prefix: Uint8Array, limit: u32): Entry[] {
    return entries(ffi.scan(true, namespace, prefix, limit));
  }

  /** Publish an upsert change. The node keeps one change per key per block, the last. */
  emitUpsert(kind: string, key: Uint8Array, value: Jsonable): void {
    ffi.emit(kind, key, false, json(value));
  }

  /** Publish a delete change. */
  emitDelete(kind: string, key: Uint8Array): void {
    ffi.emit(kind, key, true, new Uint8Array(0));
  }

  /** Write a public entity and publish it as an upsert change. */
  upsert(collection: string, kind: string, key: Uint8Array, value: Jsonable): void {
    const bytes = json(value);
    ffi.write(ffi.PUT, collection, key, bytes);
    ffi.emit(kind, key, false, bytes);
  }

  /** Delete a public entity and publish the delete. */
  remove(collection: string, kind: string, key: Uint8Array): void {
    this.delete(collection, key);
    this.emitDelete(kind, key);
  }
}

/** Keccak-256, computed natively by the host. */
export function keccak256(bytes: Uint8Array): Uint8Array {
  return ffi.keccak(bytes);
}

/** Log a debug message through the node, which bounds and labels it. */
export function log(message: string): void {
  ffi.log(message);
}

function json(value: Jsonable): Uint8Array {
  return utf8(value.toJson().toString());
}

function entries(encoded: Uint8Array): Entry[] {
  const input = new Reader(encoded);
  const count = <i32>input.uint();
  const list = new Array<Entry>(count);
  for (let index = 0; index < count; index++) list[index] = new Entry(input.bytes(), input.bytes());
  return list;
}

/** Parse the configured parameters. */
export function parameters(bytes: Uint8Array): Json {
  return Json.parse(text(bytes), "params");
}
