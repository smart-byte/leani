// Postcard (https://postcard.jamesmunns.com/wire-format), the encoding of
// the host's inputs, and of the delta `map` hands to `reduce`.

import { text as decodeText, utf8 } from "./bytes";
import { U256 } from "./u256";

/** Reads encoded values in the order they were written. Throws when the bytes end early. */
export class Reader {
  private offset: i32 = 0;

  constructor(private input: Uint8Array) {}

  /** Whether every byte has been read. */
  get done(): bool {
    return this.offset >= this.input.length;
  }

  byte(): u8 {
    if (this.offset >= this.input.length) throw new Error("the input ends early");
    return this.input[this.offset++];
  }

  /** An unsigned integer of any width up to 64 bits: a varint. */
  uint(): u64 {
    let value: u64 = 0;
    for (let shift: u64 = 0; shift < 64; shift += 7) {
      const byte = this.byte();
      value |= (<u64>(byte & 0x7f)) << shift;
      if ((byte & 0x80) == 0) return value;
    }
    throw new Error("a varint is too long");
  }

  flag(): bool {
    const byte = this.byte();
    if (byte > 1) throw new Error(`${byte} is not a bool`);
    return byte == 1;
  }

  /** `length` bytes as they are, such as a 20-byte address or a 32-byte hash. */
  raw(length: i32): Uint8Array {
    if (length < 0 || this.offset + length > this.input.length) throw new Error("the input ends early");
    const bytes = this.input.slice(this.offset, this.offset + length);
    this.offset += length;
    return bytes;
  }

  /** A length, then that many bytes. */
  bytes(): Uint8Array {
    return this.raw(<i32>this.uint());
  }

  /** A length, then that many bytes of UTF-8. */
  text(): string {
    return decodeText(this.bytes());
  }

  /** 32 bytes as an unsigned 256-bit integer. */
  u256(): U256 {
    return new U256(this.raw(32));
  }
}

/** Writes values for a `Reader` to read back in the same order. */
export class Writer {
  private buffer: Uint8Array = new Uint8Array(256);
  private length: i32 = 0;

  byte(value: u8): Writer {
    this.reserve(1);
    this.buffer[this.length++] = value;
    return this;
  }

  uint(value: u64): Writer {
    while (value >= 0x80) {
      this.byte(<u8>(value & 0x7f) | 0x80);
      value >>= 7;
    }
    return this.byte(<u8>value);
  }

  flag(value: bool): Writer {
    return this.byte(value ? 1 : 0);
  }

  /** Bytes as they are: the reader must know their length. */
  raw(value: Uint8Array): Writer {
    this.reserve(value.length);
    this.buffer.set(value, this.length);
    this.length += value.length;
    return this;
  }

  /** A length, then the bytes. */
  bytes(value: Uint8Array): Writer {
    return this.uint(<u64>value.length).raw(value);
  }

  text(value: string): Writer {
    return this.bytes(utf8(value));
  }

  u256(value: U256): Writer {
    return this.raw(value.bytes);
  }

  /** What has been written. */
  finish(): Uint8Array {
    return this.buffer.slice(0, this.length);
  }

  private reserve(extra: i32): void {
    if (this.length + extra <= this.buffer.length) return;
    let size = this.buffer.length * 2;
    while (size < this.length + extra) size *= 2;
    const grown = new Uint8Array(size);
    grown.set(this.buffer.subarray(0, this.length));
    this.buffer = grown;
  }
}
