// Solidity signatures, and the ABI encoding of event and call arguments.
//
// Parse a signature as Solidity writes it, then decode a log or a call with
// it:
//
//   const transfer = Signature.parse("Transfer(address indexed from, address indexed to, uint256 value)");
//   const topic = transfer.topic();
//   for (let index = 0; index < block.logs.length; index++) {
//     const log = block.logs[index];
//     if (log.topics.length == 0 || !equal(log.topics[0], topic)) continue;
//     // Another contract may emit the topic with another layout.
//     const args = transfer.decodeEvent(log.topics.slice(1), log.data);
//     if (args == null) continue;
//   }
//
// Supported argument types: `address`, `bool`, `uint<N>`, `int<N>`,
// `bytes<N>`, `bytes`, and `string`; tuples and arrays are not. An indexed
// `bytes` or `string` decodes to its hash.

import { hex, text, utf8 } from "./bytes";
import { keccak } from "./ffi";
import { Json, Jsonable } from "./json";
import { U256 } from "./u256";

/** A supported argument type. */
export enum Kind {
  Address,
  Bool,
  /** `uint<bits>`. */
  Uint,
  /** `int<bits>`. */
  Int,
  /** `bytes<length>`. */
  FixedBytes,
  Bytes,
  String,
}

/** One declared argument. */
export class Input {
  constructor(
    /** The declared name, or `arg<position>` for an unnamed argument. */
    public name: string,
    public kind: Kind,
    /** The bits of `uint<bits>` and `int<bits>`, the length of `bytes<length>`. */
    public size: i32,
    /** Only events index arguments. */
    public indexed: bool,
  ) {}

  /** The type as the canonical signature names it. */
  canonicalType(): string {
    switch (this.kind) {
      case Kind.Address:
        return "address";
      case Kind.Bool:
        return "bool";
      case Kind.Uint:
        return `uint${this.size}`;
      case Kind.Int:
        return `int${this.size}`;
      case Kind.FixedBytes:
        return `bytes${this.size}`;
      case Kind.Bytes:
        return "bytes";
      default:
        return "string";
    }
  }
}

/** An event or function signature. */
export class Signature {
  constructor(
    public name: string,
    /** As hashed, without names: `Transfer(address,address,uint256)`. */
    public canonical: string,
    public inputs: Input[],
  ) {}

  /**
   * Parse `Name(type [indexed] [name], ...)`, with or without a leading
   * `event` or `function` and whatever follows the closing parenthesis,
   * such as `external returns (bool)`, but `anonymous`. Throws naming what
   * does not parse.
   */
  static parse(text: string): Signature {
    let body = trim(text);
    while (body.endsWith(";")) body = body.substring(0, body.length - 1);
    body = trim(body);
    if (body.startsWith("event ")) body = trim(body.substring(6));
    else if (body.startsWith("function ")) body = trim(body.substring(9));
    const open = body.indexOf("(");
    if (open < 0) throw new Error(invalid(text, "missing `(`"));
    const close = body.indexOf(")", open);
    if (close < 0) throw new Error(invalid(text, "missing the closing `)`"));
    // Its logs carry no signature topic to find them by.
    if (whitespaceSplit(body.substring(close + 1)).includes("anonymous")) {
      throw new Error(invalid(text, "anonymous events are not supported"));
    }
    const name = trim(body.substring(0, open));
    if (!identifier(name)) throw new Error(invalid(text, "the name is not an identifier"));
    const list = body.substring(open + 1, close);
    if (list.includes("(") || list.includes("[") || list.includes("]")) {
      throw new Error(invalid(text, "tuples and arrays are not supported"));
    }
    const inputs = new Array<Input>();
    const parts = list.split(",");
    for (let part = 0; part < parts.length; part++) {
      const argument = trim(parts[part]);
      if (argument.length == 0) continue;
      const words = whitespaceSplit(argument);
      const input = parseType(text, words[0], inputs.length);
      for (let word = 1; word < words.length; word++) {
        const next = words[word];
        if (next == "indexed") input.indexed = true;
        // Data locations of function arguments.
        else if (next == "memory" || next == "calldata" || next == "storage") continue;
        else if (identifier(next)) input.name = next;
        else throw new Error(invalid(text, `unexpected \`${next}\``));
      }
      inputs.push(input);
    }
    if (indexedCount(inputs) > 3) throw new Error(invalid(text, "an event has at most three indexed arguments"));
    const types = inputs.map<string>((input: Input) => input.canonicalType());
    return new Signature(name, `${name}(${types.join(",")})`, inputs);
  }

  /** An event's topic 0: the Keccak-256 hash of the canonical signature. */
  topic(): Uint8Array {
    return keccak(utf8(this.canonical));
  }

  /** A function's selector: the first four bytes of that hash. */
  selector(): Uint8Array {
    return this.topic().slice(0, 4);
  }

  /**
   * Decode an event's arguments, in declaration order, from its indexed
   * `topics` (those after topic 0) and its `data`, or `null` when the log
   * does not have this layout.
   */
  decodeEvent(topics: Uint8Array[], data: Uint8Array): Value[] | null {
    if (topics.length != indexedCount(this.inputs)) return null;
    let topic = 0;
    let head = 0;
    const values = new Array<Value>();
    for (let index = 0; index < this.inputs.length; index++) {
      const input = this.inputs[index];
      let value: Value | null;
      if (input.indexed) {
        const word = topics[topic++];
        // Indexed dynamic values are stored as their hash.
        value = input.kind == Kind.Bytes || input.kind == Kind.String ? Value.ofBytes(word) : decodeWord(input, word);
      } else {
        value = argument(input, data, head);
        head += 32;
      }
      if (value == null) return null;
      values.push(value);
    }
    return values;
  }

  /** Decode a call's arguments from its input after the four-byte selector, or `null` when the input does not have this layout. */
  decodeArguments(data: Uint8Array): Value[] | null {
    const values = new Array<Value>();
    for (let index = 0; index < this.inputs.length; index++) {
      const value = argument(this.inputs[index], data, index * 32);
      if (value == null) return null;
      values.push(value);
    }
    return values;
  }
}

export enum ValueKind {
  Address,
  Bool,
  Uint,
  /** A signed integer, as its sign and magnitude. */
  Int,
  /** `bytes<N>`, `bytes`, or the hash of an indexed `bytes` or `string`. */
  Bytes,
  String,
}

/** A decoded argument. */
export class Value extends Jsonable {
  /** An address's or bytes' bytes. */
  bytes: Uint8Array = new Uint8Array(0);
  flag: bool = false;
  /** An unsigned integer, or a signed one's magnitude. */
  magnitude: U256 = U256.zero();
  negative: bool = false;
  text: string = "";

  constructor(public kind: ValueKind) {
    super();
  }

  static ofBytes(bytes: Uint8Array): Value {
    const value = new Value(ValueKind.Bytes);
    value.bytes = bytes;
    return value;
  }

  /**
   * The value as the node serves JSON: addresses and bytes as `0x` hex,
   * integers as decimal strings, which JSON numbers cannot hold exactly.
   */
  toJson(): Json {
    switch (this.kind) {
      case ValueKind.Address:
      case ValueKind.Bytes:
        return Json.string(hex(this.bytes));
      case ValueKind.Bool:
        return Json.bool(this.flag);
      case ValueKind.Uint:
        return Json.string(this.magnitude.toString());
      case ValueKind.Int:
        return Json.string((this.negative ? "-" : "") + this.magnitude.toString());
      default:
        return Json.string(this.text);
    }
  }

  /** An unsigned integer, or a non-negative signed one. */
  asUint(): U256 | null {
    return this.kind == ValueKind.Uint || (this.kind == ValueKind.Int && !this.negative) ? this.magnitude : null;
  }

  /** The address, or `null` for any other value. */
  asAddress(): Uint8Array | null {
    return this.kind == ValueKind.Address ? this.bytes : null;
  }
}

function indexedCount(inputs: Input[]): i32 {
  let count = 0;
  for (let index = 0; index < inputs.length; index++) if (inputs[index].indexed) count++;
  return count;
}

// AssemblyScript keeps a thrown error's message only from
// `throw new Error(message)` itself, so this returns the message.
function invalid(text: string, reason: string): string {
  return `signature \`${text}\`: ${reason}`;
}

/** An argument's type, named `arg<position>` until a name follows. */
function parseType(signature: string, type: string, position: i32): Input {
  const name = `arg${position}`;
  if (type == "address") return new Input(name, Kind.Address, 0, false);
  if (type == "bool") return new Input(name, Kind.Bool, 0, false);
  if (type == "bytes") return new Input(name, Kind.Bytes, 0, false);
  if (type == "string") return new Input(name, Kind.String, 0, false);
  if (type == "uint") return new Input(name, Kind.Uint, 256, false);
  if (type == "int") return new Input(name, Kind.Int, 256, false);
  if (type.startsWith("uint")) return new Input(name, Kind.Uint, width(signature, type, 4, 256, 8), false);
  if (type.startsWith("int")) return new Input(name, Kind.Int, width(signature, type, 3, 256, 8), false);
  if (type.startsWith("bytes")) return new Input(name, Kind.FixedBytes, width(signature, type, 5, 32, 1), false);
  throw new Error(invalid(signature, `unsupported type \`${type}\``));
}

/** The width after a type's prefix: a multiple of `step` up to `maximum`. */
function width(signature: string, type: string, prefix: i32, maximum: i32, step: i32): i32 {
  let digits = type.substring(prefix);
  // As Rust parses a u16: an optional `+`, then decimal digits.
  if (digits.startsWith("+")) digits = digits.substring(1);
  let bits = 0;
  let valid = digits.length > 0;
  for (let index = 0; index < digits.length && valid; index++) {
    const digit = digits.charCodeAt(index) - 48;
    valid = digit >= 0 && digit <= 9 && bits <= 6553;
    bits = bits * 10 + digit;
  }
  if (!valid || bits > 65535 || bits < step || bits > maximum || bits % step != 0) {
    throw new Error(invalid(signature, `\`${type}\` has an invalid width`));
  }
  return bits;
}

/** The non-indexed argument whose head slot starts at `head` in `data`. */
function argument(input: Input, data: Uint8Array, head: i32): Value | null {
  if (<i64>head + 32 > <i64>data.length) return null;
  const slot = data.subarray(head, head + 32);
  if (input.kind == Kind.Bytes || input.kind == Kind.String) return dynamic(input.kind, data, slot);
  return decodeWord(input, slot);
}

/** A static value from one 32-byte word, checking its ABI padding. */
function decodeWord(input: Input, word: Uint8Array): Value | null {
  switch (input.kind) {
    case Kind.Address: {
      if (!filled(word, 0, 12, 0)) return null;
      const value = new Value(ValueKind.Address);
      value.bytes = word.slice(12, 32);
      return value;
    }
    case Kind.Bool: {
      if (!filled(word, 0, 31, 0) || word[31] > 1) return null;
      const value = new Value(ValueKind.Bool);
      value.flag = word[31] == 1;
      return value;
    }
    case Kind.Uint: {
      if (!filled(word, 0, 32 - input.size / 8, 0)) return null;
      const value = new Value(ValueKind.Uint);
      value.magnitude = new U256(word.slice(0, 32));
      return value;
    }
    case Kind.Int: {
      const bytes = input.size / 8;
      const negative = (word[32 - bytes] & 0x80) != 0;
      if (!filled(word, 0, 32 - bytes, negative ? 0xff : 0)) return null;
      const value = new Value(ValueKind.Int);
      value.negative = negative;
      if (negative) {
        // Two's complement: invert and add one.
        const inverted = new Uint8Array(32);
        for (let index = 0; index < 32; index++) inverted[index] = ~word[index];
        const magnitude = new U256(inverted).checkedAdd(U256.fromU64(1));
        if (magnitude == null) return null;
        value.magnitude = magnitude;
      } else {
        value.magnitude = new U256(word.slice(0, 32));
      }
      return value;
    }
    case Kind.FixedBytes: {
      if (!filled(word, input.size, 32, 0)) return null;
      return Value.ofBytes(word.slice(0, input.size));
    }
    default:
      return null;
  }
}

/** A `bytes` or `string` value from the tail its head slot points at. */
function dynamic(kind: Kind, data: Uint8Array, slot: Uint8Array): Value | null {
  const offset = small(slot);
  if (offset < 0 || offset + 32 > <i64>data.length) return null;
  const length = small(data.subarray(<i32>offset, <i32>offset + 32));
  const start = offset + 32;
  if (length < 0 || start + length > <i64>data.length) return null;
  const bytes = data.slice(<i32>start, <i32>(start + length));
  if (kind != Kind.String) return Value.ofBytes(bytes);
  const value = new Value(ValueKind.String);
  value.text = text(bytes);
  return value;
}

/** A word that fits below 2^32, as offsets and lengths must, or -1. */
function small(word: Uint8Array): i64 {
  if (!filled(word, 0, 28, 0)) return -1;
  return (<i64>word[28] << 24) | (<i64>word[29] << 16) | (<i64>word[30] << 8) | <i64>word[31];
}

function filled(bytes: Uint8Array, start: i32, end: i32, fill: u8): bool {
  for (let index = start; index < end; index++) {
    if (bytes[index] != fill) return false;
  }
  return true;
}

function identifier(text: string): bool {
  if (text.length == 0) return false;
  for (let index = 0; index < text.length; index++) {
    const code = text.charCodeAt(index);
    const letter = (code >= 65 && code <= 90) || (code >= 97 && code <= 122) || code == 95 || code == 36;
    const digit = code >= 48 && code <= 57;
    if (!(letter || (digit && index > 0))) return false;
  }
  return true;
}

/** Whitespace as Rust's `char::is_whitespace` knows it. */
function space(code: i32): bool {
  return (
    (code >= 0x09 && code <= 0x0d) ||
    code == 0x20 ||
    code == 0x85 ||
    code == 0xa0 ||
    code == 0x1680 ||
    (code >= 0x2000 && code <= 0x200a) ||
    code == 0x2028 ||
    code == 0x2029 ||
    code == 0x202f ||
    code == 0x205f ||
    code == 0x3000
  );
}

function trim(text: string): string {
  let start = 0;
  let end = text.length;
  while (start < end && space(text.charCodeAt(start))) start++;
  while (end > start && space(text.charCodeAt(end - 1))) end--;
  return text.substring(start, end);
}

function whitespaceSplit(text: string): string[] {
  const words = new Array<string>();
  let start = -1;
  for (let index = 0; index <= text.length; index++) {
    const blank = index == text.length || space(text.charCodeAt(index));
    if (blank && start >= 0) {
      words.push(text.substring(start, index));
      start = -1;
    } else if (!blank && start < 0) {
      start = index;
    }
  }
  return words;
}
