// ABI v1 imports from module `leani`, and the failure path every thrown
// error takes.
//
// Every pointer is a 32-bit offset into this module's memory. The host
// writes inputs into buffers from `leani_alloc`; the instance lives for one
// call, so nothing is ever freed.

import { utf8 } from "./bytes";

/** Set the call's result bytes. */
// @ts-ignore: decorator
@external("leani", "output")
declare function host_output(ptr: usize, len: usize): void;

/** Record the call's failure message. */
// @ts-ignore: decorator
@external("leani", "fail")
declare function host_fail(ptr: usize, len: usize): void;

// @ts-ignore: decorator
@external("leani", "log")
declare function host_log(ptr: usize, len: usize): void;

// @ts-ignore: decorator
@external("leani", "keccak256")
declare function host_keccak256(ptr: usize, len: usize, out: usize): void;

/** Copy the value the last read or scan returned into `dst`. */
// @ts-ignore: decorator
@external("leani", "take_value")
declare function host_take_value(dst: usize): void;

/** Returns the value's length, or -1 when the key is absent. */
// @ts-ignore: decorator
@external("leani", "read")
declare function host_read(state: u32, name: usize, nameLen: usize, key: usize, keyLen: usize): i32;

/** Returns the length of the encoded `(key, value)` list. */
// @ts-ignore: decorator
@external("leani", "scan")
declare function host_scan(state: u32, name: usize, nameLen: usize, prefix: usize, prefixLen: usize, limit: u32): i32;

// @ts-ignore: decorator
@external("leani", "write")
declare function host_write(operation: u32, name: usize, nameLen: usize, key: usize, keyLen: usize, value: usize, valueLen: usize): i32;

// @ts-ignore: decorator
@external("leani", "emit")
declare function host_emit(kind: usize, kindLen: usize, key: usize, keyLen: usize, deleted: u32, payload: usize, payloadLen: usize): void;

/** The `leani.write` operation codes. */
export const PUT: u32 = 0;
export const DELETE: u32 = 1;
export const STATE_PUT: u32 = 2;
export const STATE_DELETE: u32 = 3;
export const INDEX_PUT: u32 = 4;
export const INDEX_DELETE: u32 = 5;

/** The `len` bytes the host wrote at `ptr`. */
export function input(ptr: u32, len: u32): Uint8Array {
  const bytes = new Uint8Array(<i32>len);
  memory.copy(bytes.dataStart, <usize>ptr, <usize>len);
  return bytes;
}

export function output(bytes: Uint8Array): void {
  host_output(bytes.dataStart, bytes.length);
}

export function read(state: bool, name: string, key: Uint8Array): Uint8Array | null {
  const encoded = utf8(name);
  const len = host_read(state ? 1 : 0, encoded.dataStart, encoded.length, key.dataStart, key.length);
  return len < 0 ? null : take(len);
}

export function scan(state: bool, name: string, prefix: Uint8Array, limit: u32): Uint8Array {
  const encoded = utf8(name);
  return take(host_scan(state ? 1 : 0, encoded.dataStart, encoded.length, prefix.dataStart, prefix.length, limit));
}

export function write(operation: u32, name: string, key: Uint8Array, value: Uint8Array): void {
  const encoded = utf8(name);
  host_write(operation, encoded.dataStart, encoded.length, key.dataStart, key.length, value.dataStart, value.length);
}

export function emit(kind: string, key: Uint8Array, deleted: bool, payload: Uint8Array): void {
  const encoded = utf8(kind);
  host_emit(encoded.dataStart, encoded.length, key.dataStart, key.length, deleted ? 1 : 0, payload.dataStart, payload.length);
}

export function keccak(bytes: Uint8Array): Uint8Array {
  const hash = new Uint8Array(32);
  host_keccak256(bytes.dataStart, bytes.length, hash.dataStart);
  return hash;
}

export function log(message: string): void {
  const encoded = utf8(message);
  host_log(encoded.dataStart, encoded.length);
}

function take(len: i32): Uint8Array {
  const value = new Uint8Array(len > 0 ? len : 0);
  host_take_value(value.dataStart);
  return value;
}

/**
 * Every thrown error and failed assertion ends here, by the build's
 * `--use abort=assembly/leani/ffi/abort`: the node fails the call with the
 * message, as it does a Rust processor's error.
 */
function abort(message: string | null, file: string | null, line: u32, column: u32): void {
  let text = message != null ? message : "aborted";
  // Where the processor's own code failed; the SDK's places are no help.
  if (file != null && !file.startsWith("assembly/leani/")) {
    text += ` (${file}:${line}:${column})`;
  }
  const encoded = utf8(text);
  host_fail(encoded.dataStart, encoded.length);
  unreachable();
}
