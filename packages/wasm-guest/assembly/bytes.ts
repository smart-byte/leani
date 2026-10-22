// Bytes as Ethereum and the node spell them: `0x` hex, big-endian keys, and
// UTF-8 text.

/** `0x`-prefixed lowercase hex. */
export function hex(bytes: Uint8Array): string {
  // Written straight into the string's UTF-16 units: entities hold many.
  const length = 2 + bytes.length * 2;
  const out = changetype<string>(__new(<usize>length << 1, idof<string>()));
  const base = changetype<usize>(out);
  store<u16>(base, 0x30);
  store<u16>(base, 0x78, 2);
  for (let index = 0; index < bytes.length; index++) {
    const byte = <u32>bytes[index];
    const at = base + ((<usize>index + 1) << 2);
    store<u16>(at, digit(byte >> 4));
    store<u16>(at, digit(byte & 0x0f), 2);
  }
  return out;
}

// @ts-ignore: decorator
@inline
export function digit(nibble: u32): u16 {
  return <u16>(nibble < 10 ? 0x30 + nibble : 0x57 + nibble);
}

/** Exactly `length` bytes of hex, with or without `0x`. Throws for a wrong length or a non-hex digit. */
export function parseHex(text: string, length: i32): Uint8Array {
  const digits = text.startsWith("0x") ? text.substring(2) : text;
  if (digits.length != length * 2) throw new Error(`\`${text}\` is not ${length} bytes of hex`);
  const bytes = new Uint8Array(length);
  for (let index = 0; index < length; index++) {
    const high = nibble(digits.charCodeAt(index * 2));
    const low = nibble(digits.charCodeAt(index * 2 + 1));
    if (high < 0 || low < 0) throw new Error(`\`${text}\` contains a non-hex digit`);
    bytes[index] = <u8>((high << 4) | low);
  }
  return bytes;
}

export function nibble(code: i32): i32 {
  if (code >= 48 && code <= 57) return code - 48;
  if (code >= 97 && code <= 102) return code - 87;
  if (code >= 65 && code <= 70) return code - 55;
  return -1;
}

/** Whether `a` and `b` hold the same bytes. */
export function equal(a: Uint8Array, b: Uint8Array): bool {
  if (a.length != b.length) return false;
  for (let index = 0; index < a.length; index++) {
    if (a[index] != b[index]) return false;
  }
  return true;
}

/** The position of `bytes` in `list`, or -1. */
export function indexOf(list: Uint8Array[], bytes: Uint8Array): i32 {
  for (let index = 0; index < list.length; index++) {
    if (equal(list[index], bytes)) return index;
  }
  return -1;
}

/** `a` followed by `b`, as a key of two parts. */
export function concat(a: Uint8Array, b: Uint8Array): Uint8Array {
  const joined = new Uint8Array(a.length + b.length);
  joined.set(a);
  joined.set(b, a.length);
  return joined;
}

/** The eight big-endian bytes of `value`: keys in numeric order. */
export function bigEndian64(value: u64): Uint8Array {
  const bytes = new Uint8Array(8);
  for (let index = 0; index < 8; index++) bytes[index] = <u8>(value >> (<u64>(56 - index * 8)));
  return bytes;
}

/** The four big-endian bytes of `value`. */
export function bigEndian32(value: u32): Uint8Array {
  const bytes = new Uint8Array(4);
  for (let index = 0; index < 4; index++) bytes[index] = <u8>(value >> (<u32>(24 - index * 8)));
  return bytes;
}

/** `text` as UTF-8 bytes. */
export function utf8(text: string): Uint8Array {
  return Uint8Array.wrap(String.UTF8.encode(text));
}

/**
 * UTF-8 bytes as text, each ill-formed sequence replaced by U+FFFD as Rust's
 * `String::from_utf8_lossy` replaces it, so both languages read alike.
 */
export function text(bytes: Uint8Array): string {
  const units = new Array<u16>();
  let index = 0;
  while (index < bytes.length) {
    const lead = <u32>bytes[index];
    if (lead < 0x80) {
      units.push(<u16>lead);
      index++;
      continue;
    }
    // The sequence's length and its second byte's range, by its lead byte.
    let length = 0;
    let low: u32 = 0x80;
    let high: u32 = 0xbf;
    if (lead >= 0xc2 && lead <= 0xdf) length = 2;
    else if (lead >= 0xe0 && lead <= 0xef) {
      length = 3;
      if (lead == 0xe0) low = 0xa0;
      if (lead == 0xed) high = 0x9f;
    } else if (lead >= 0xf0 && lead <= 0xf4) {
      length = 4;
      if (lead == 0xf0) low = 0x90;
      if (lead == 0xf4) high = 0x8f;
    }
    if (length == 0) {
      units.push(0xfffd);
      index++;
      continue;
    }
    let point = lead & (0xff >> (<u32>length + 1));
    let taken = 1;
    while (taken < length && index + taken < bytes.length) {
      const next = <u32>bytes[index + taken];
      if (next < low || next > high) break;
      point = (point << 6) | (next & 0x3f);
      low = 0x80;
      high = 0xbf;
      taken++;
    }
    index += taken;
    if (taken < length) {
      // The maximal valid prefix becomes one replacement character.
      units.push(0xfffd);
    } else if (point >= 0x10000) {
      point -= 0x10000;
      units.push(<u16>(0xd800 + (point >> 10)));
      units.push(<u16>(0xdc00 + (point & 0x3ff)));
    } else {
      units.push(<u16>point);
    }
  }
  return String.UTF16.decodeUnsafe(units.dataStart, <usize>units.length << 1);
}
