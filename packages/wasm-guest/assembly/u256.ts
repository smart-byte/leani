// Minimal unsigned 256-bit arithmetic for token amounts.

import { Json, JsonKind, Jsonable } from "./json";

/**
 * An unsigned 256-bit integer stored as its 32 big-endian bytes, as
 * Ethereum encodes quantities. Converts to JSON as a decimal string.
 */
export class U256 extends Jsonable {
  readonly bytes: Uint8Array;

  constructor(bytes: Uint8Array) {
    super();
    if (bytes.length != 32) throw new Error(`a U256 is 32 bytes, not ${bytes.length}`);
    this.bytes = bytes;
  }

  static zero(): U256 {
    return new U256(new Uint8Array(32));
  }

  static max(): U256 {
    const bytes = new Uint8Array(32);
    bytes.fill(0xff);
    return new U256(bytes);
  }

  static fromU64(value: u64): U256 {
    const bytes = new Uint8Array(32);
    for (let index = 0; index < 8; index++) bytes[31 - index] = <u8>(value >> (<u64>(index * 8)));
    return new U256(bytes);
  }

  /** An unsigned decimal integer. Throws for anything else, or for more than 256 bits. */
  static parse(text: string): U256 {
    if (text.length == 0) throw new Error(`\`${text}\` is not an unsigned decimal integer`);
    let value = U256.zero();
    for (let index = 0; index < text.length; index++) {
      const digit = text.charCodeAt(index) - 48;
      if (digit < 0 || digit > 9) throw new Error(`\`${text}\` is not an unsigned decimal integer`);
      const next = value.mulAdd(10, <u8>digit);
      if (next == null) throw new Error(`\`${text}\` does not fit 256 bits`);
      value = next;
    }
    return value;
  }

  /** A decimal string, or an integer small enough for JSON numbers. */
  static fromJson(value: Json): U256 {
    if (value.kind == JsonKind.String) return U256.parse(value.asString());
    return U256.fromU64(value.asU64());
  }

  isZero(): bool {
    for (let index = 0; index < 32; index++) {
      if (this.bytes[index] != 0) return false;
    }
    return true;
  }

  /** -1, 0, or 1 as this is less than, equal to, or greater than `other`. */
  compare(other: U256): i32 {
    for (let index = 0; index < 32; index++) {
      const a = this.bytes[index];
      const b = other.bytes[index];
      if (a != b) return a < b ? -1 : 1;
    }
    return 0;
  }

  @operator("<")
  static lt(a: U256, b: U256): bool {
    return a.compare(b) < 0;
  }

  @operator("<=")
  static le(a: U256, b: U256): bool {
    return a.compare(b) <= 0;
  }

  @operator(">")
  static gt(a: U256, b: U256): bool {
    return a.compare(b) > 0;
  }

  @operator(">=")
  static ge(a: U256, b: U256): bool {
    return a.compare(b) >= 0;
  }

  /** The sum, or `null` on overflow. */
  checkedAdd(other: U256): U256 | null {
    const sum = new Uint8Array(32);
    let carry: u32 = 0;
    for (let index = 31; index >= 0; index--) {
      const total = <u32>this.bytes[index] + <u32>other.bytes[index] + carry;
      sum[index] = <u8>total;
      carry = total >> 8;
    }
    return carry == 0 ? new U256(sum) : null;
  }

  /** The sum, or `U256.max()` on overflow. */
  saturatingAdd(other: U256): U256 {
    const sum = this.checkedAdd(other);
    return sum != null ? sum : U256.max();
  }

  /** The difference, or `null` when `other` is larger. */
  checkedSub(other: U256): U256 | null {
    if (this.compare(other) < 0) return null;
    const difference = new Uint8Array(32);
    let borrow: i32 = 0;
    for (let index = 31; index >= 0; index--) {
      let value = <i32>this.bytes[index] - <i32>other.bytes[index] - borrow;
      borrow = value < 0 ? 1 : 0;
      if (value < 0) value += 256;
      difference[index] = <u8>value;
    }
    return new U256(difference);
  }

  /** Multiply by a small factor and add a digit; `null` on overflow. */
  private mulAdd(factor: u8, addend: u8): U256 | null {
    const product = new Uint8Array(32);
    let carry = <u32>addend;
    for (let index = 31; index >= 0; index--) {
      const total = <u32>this.bytes[index] * <u32>factor + carry;
      product[index] = <u8>total;
      carry = total >> 8;
    }
    return carry == 0 ? new U256(product) : null;
  }

  /** The decimal digits. */
  toString(): string {
    // Divide by 10^9 over eight 32-bit limbs: each remainder fits a u64.
    const limbs = new StaticArray<u32>(8);
    for (let index = 0; index < 8; index++) {
      limbs[index] =
        (<u32>this.bytes[index * 4] << 24) |
        (<u32>this.bytes[index * 4 + 1] << 16) |
        (<u32>this.bytes[index * 4 + 2] << 8) |
        <u32>this.bytes[index * 4 + 3];
    }
    // At most 78 digits, written from the last in chunks of nine.
    const places = 81;
    const digits = new StaticArray<u16>(places);
    let at = places;
    while (true) {
      let remainder: u64 = 0;
      let zero = true;
      for (let index = 0; index < 8; index++) {
        const current = (remainder << 32) | <u64>limbs[index];
        limbs[index] = <u32>(current / 1_000_000_000);
        remainder = current % 1_000_000_000;
        if (limbs[index] != 0) zero = false;
      }
      let chunk = <u32>remainder;
      if (zero) {
        do {
          digits[--at] = <u16>(0x30 + (chunk % 10));
          chunk /= 10;
        } while (chunk != 0);
        break;
      }
      for (let digit = 0; digit < 9; digit++) {
        digits[--at] = <u16>(0x30 + (chunk % 10));
        chunk /= 10;
      }
    }
    return String.UTF16.decodeUnsafe(changetype<usize>(digits) + (<usize>at << 1), <usize>(places - at) << 1);
  }

  toJson(): Json {
    return Json.string(this.toString());
  }
}
