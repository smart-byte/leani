// The module's entry points, as the Leani ABI names them. Export them from
// the processor's file with `export * from "./leani/exports";`.

import { Block, BlockRef } from "./abi";
import { utf8 } from "./bytes";
import { input, output } from "./ffi";
import { Store, parameters, processor } from "./index";
import { Reader, Writer } from "./postcard";

/** A buffer for the host to write a call's input into. */
export function leani_alloc(len: u32): u32 {
  return <u32>heap.alloc(<usize>len);
}

/** The manifest, as JSON, for the parameters. */
export function leani_describe(ptr: u32, len: u32): u32 {
  const manifest = processor().manifest(parameters(input(ptr, len)));
  output(utf8(manifest.toJson().toString()));
  return 0;
}

/** One block's delta. */
export function leani_map(ptr: u32, len: u32): u32 {
  const reader = new Reader(input(ptr, len));
  const params = parameters(reader.bytes());
  const block = Block.read(reader);
  const delta = new Writer();
  processor().map(params, block, delta);
  output(delta.finish());
  return 0;
}

/** Apply one block's delta to the store. */
export function leani_reduce(ptr: u32, len: u32): u32 {
  const reader = new Reader(input(ptr, len));
  const params = parameters(reader.bytes());
  const block = BlockRef.read(reader);
  const delta = new Reader(reader.bytes());
  processor().reduce(params, block, delta, new Store());
  return 0;
}
