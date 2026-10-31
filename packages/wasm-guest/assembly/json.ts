// JSON: the parameters a processor reads, and the entities and changes it
// writes. Integers too large for JSON numbers, such as token amounts, travel
// as decimal strings, as `U256` writes them.

import { digit, hex, nibble } from "./bytes";

/** A value that converts to JSON: `Json` itself, `U256`, `Some`, a decoded Solidity value. */
export abstract class Jsonable {
  abstract toJson(): Json;
}

export enum JsonKind {
  Null,
  Bool,
  Number,
  String,
  Array,
  Object,
}

/** A JSON value, parsed or built. Objects keep their keys in order. */
export class Json extends Jsonable {
  /** A bool's value. */
  private flag: bool = false;
  /** A number's text, or a string's value. */
  private text: string = "";
  /** An array's items, or an object's values. */
  private items: Json[] = [];
  /** An object's keys. */
  private names: string[] = [];

  constructor(public kind: JsonKind) {
    super();
  }

  /** Parse JSON text. Throws for invalid JSON, naming it `what`. */
  static parse(text: string, what: string = "JSON"): Json {
    const parser = new Parser(text, what);
    const value = parser.value();
    parser.space();
    if (parser.index < text.length) parser.fail("trailing characters");
    return value;
  }

  static object(): Json {
    return new Json(JsonKind.Object);
  }

  static array(): Json {
    return new Json(JsonKind.Array);
  }

  static string(value: string): Json {
    const json = new Json(JsonKind.String);
    json.text = value;
    return json;
  }

  static bool(value: bool): Json {
    const json = new Json(JsonKind.Bool);
    json.flag = value;
    return json;
  }

  /** A number from its JSON text, such as `"42"`. */
  static number(text: string): Json {
    const json = new Json(JsonKind.Number);
    json.text = text;
    return json;
  }

  /**
   * Any value as JSON: strings, bools, and integers as they are, bytes as
   * `0x` hex, `null` as null, a `Jsonable` as it converts, and arrays of
   * those. Floats are refused: they print differently in every language.
   */
  static from<T>(value: T): Json {
    if (isNullable<T>()) {
      if (changetype<usize>(value) == 0) return new Json(JsonKind.Null);
      return Json.from<NonNullable<T>>(changetype<NonNullable<T>>(value));
    }
    if (isString<T>()) return Json.string(changetype<string>(value));
    if (isBoolean<T>()) return Json.bool(<bool>value);
    if (isInteger<T>()) return Json.number(value.toString());
    if (value instanceof Jsonable) return changetype<Jsonable>(value).toJson();
    if (value instanceof Uint8Array) return Json.string(hex(changetype<Uint8Array>(value)));
    if (isArray<T>()) {
      const array = Json.array();
      for (let index = 0; index < value.length; index++) array.push(value[index]);
      return array;
    }
    ERROR("JSON holds strings, bools, integers, bytes, U256, Json, and arrays of them; write a float as a string");
    return new Json(JsonKind.Null);
  }

  toJson(): Json {
    return this;
  }

  get isNull(): bool {
    return this.kind == JsonKind.Null;
  }

  /** An object's member, or `null` when it has none of that name. */
  get(key: string): Json | null {
    if (this.kind != JsonKind.Object) return null;
    const index = this.names.indexOf(key);
    return index < 0 ? null : this.items[index];
  }

  /** An object's member. Throws when it is missing. */
  field(key: string): Json {
    const value = this.get(key);
    if (value == null) throw new Error(`missing field \`${key}\``);
    return value;
  }

  /** An array's items. Throws for anything else. */
  values(): Json[] {
    if (this.kind != JsonKind.Array) throw new Error(`expected an array, not ${this.toString()}`);
    return this.items.slice(0);
  }

  /** A string's value. Throws for anything else. */
  asString(): string {
    if (this.kind != JsonKind.String) throw new Error(`expected a string, not ${this.toString()}`);
    return this.text;
  }

  /** A bool's value. Throws for anything else. */
  asBool(): bool {
    if (this.kind != JsonKind.Bool) throw new Error(`expected a bool, not ${this.toString()}`);
    return this.flag;
  }

  /** A non-negative integer number. Throws for anything else. */
  asU64(): u64 {
    const invalid = `expected an unsigned integer, not ${this.toString()}`;
    if (this.kind != JsonKind.Number) throw new Error(invalid);
    let value: u64 = 0;
    for (let index = 0; index < this.text.length; index++) {
      const digit = this.text.charCodeAt(index) - 48;
      if (digit < 0 || digit > 9) throw new Error(invalid);
      if (value > (u64.MAX_VALUE - <u64>digit) / 10) throw new Error(invalid);
      value = value * 10 + <u64>digit;
    }
    return value;
  }

  /** A string as it is, anything else as JSON text. */
  toText(): string {
    return this.kind == JsonKind.String ? this.text : this.toString();
  }

  /** Set an object's member, in place of one of the same name. */
  set<T>(key: string, value: T): Json {
    if (this.kind != JsonKind.Object) throw new Error(`cannot set \`${key}\` on ${this.toString()}`);
    const json = Json.from<T>(value);
    const index = this.names.indexOf(key);
    if (index < 0) {
      this.names.push(key);
      this.items.push(json);
    } else {
      this.items[index] = json;
    }
    return this;
  }

  /** Append to an array. */
  push<T>(value: T): Json {
    if (this.kind != JsonKind.Array) throw new Error(`cannot append to ${this.toString()}`);
    this.items.push(Json.from<T>(value));
    return this;
  }

  /** Refuse an object with keys other than `keys`, as Rust's `deny_unknown_fields` does. */
  only(keys: string[]): Json {
    if (this.kind != JsonKind.Object) throw new Error(`invalid params: expected an object, not ${this.toString()}`);
    for (let index = 0; index < this.names.length; index++) {
      const name = this.names[index];
      if (keys.includes(name)) continue;
      const expected = keys.map<string>((key: string) => `\`${key}\``).join(", ");
      throw new Error(
        `invalid params: unknown field \`${name}\`, ${keys.length == 1 ? "expected" : "expected one of"} ${expected}`,
      );
    }
    return this;
  }

  toString(): string {
    const out = new Text();
    this.write(out);
    return out.finish();
  }

  private write(out: Text): void {
    switch (this.kind) {
      case JsonKind.Null:
        out.append("null");
        break;
      case JsonKind.Bool:
        out.append(this.flag ? "true" : "false");
        break;
      case JsonKind.Number:
        out.append(this.text);
        break;
      case JsonKind.String:
        out.quote(this.text);
        break;
      case JsonKind.Array:
        out.unit(0x5b);
        for (let index = 0; index < this.items.length; index++) {
          if (index > 0) out.unit(0x2c);
          this.items[index].write(out);
        }
        out.unit(0x5d);
        break;
      default:
        out.unit(0x7b);
        for (let index = 0; index < this.names.length; index++) {
          if (index > 0) out.unit(0x2c);
          out.quote(this.names[index]);
          out.unit(0x3a);
          this.items[index].write(out);
        }
        out.unit(0x7d);
    }
  }
}

/** JSON text built in one buffer of UTF-16 units, as AssemblyScript strings hold them. */
class Text {
  private buffer: Uint16Array = new Uint16Array(256);
  private length: i32 = 0;

  unit(code: u16): void {
    this.reserve(1);
    this.buffer[this.length++] = code;
  }

  append(text: string): void {
    this.reserve(text.length);
    memory.copy(this.buffer.dataStart + (<usize>this.length << 1), changetype<usize>(text), <usize>text.length << 1);
    this.length += text.length;
  }

  /** `text` as a JSON string, escaped as Rust's `serde_json` escapes it. */
  quote(text: string): void {
    this.unit(0x22);
    let start = 0;
    for (let index = 0; index < text.length; index++) {
      const code = text.charCodeAt(index);
      if (code >= 0x20 && code != 0x22 && code != 0x5c) continue;
      this.append(text.substring(start, index));
      this.unit(0x5c);
      if (code == 0x22 || code == 0x5c) this.unit(<u16>code);
      else if (code == 0x08) this.unit(0x62);
      else if (code == 0x0c) this.unit(0x66);
      else if (code == 0x0a) this.unit(0x6e);
      else if (code == 0x0d) this.unit(0x72);
      else if (code == 0x09) this.unit(0x74);
      else {
        this.append("u00");
        this.unit(digit(<u32>(code >> 4)));
        this.unit(digit(<u32>(code & 0x0f)));
      }
      start = index + 1;
    }
    this.append(start == 0 ? text : text.substring(start));
    this.unit(0x22);
  }

  finish(): string {
    return String.UTF16.decodeUnsafe(this.buffer.dataStart, <usize>this.length << 1);
  }

  private reserve(extra: i32): void {
    if (this.length + extra <= this.buffer.length) return;
    let size = this.buffer.length * 2;
    while (size < this.length + extra) size *= 2;
    const grown = new Uint16Array(size);
    grown.set(this.buffer.subarray(0, this.length));
    this.buffer = grown;
  }
}

class Parser {
  index: i32 = 0;

  constructor(
    public text: string,
    public what: string,
  ) {}

  fail(reason: string): void {
    throw new Error(`invalid ${this.what} at character ${this.index}: ${reason}`);
  }

  space(): void {
    while (this.index < this.text.length) {
      const code = this.text.charCodeAt(this.index);
      if (code != 0x20 && code != 0x09 && code != 0x0a && code != 0x0d) return;
      this.index++;
    }
  }

  value(): Json {
    this.space();
    if (this.index >= this.text.length) this.fail("unexpected end");
    const code = this.text.charCodeAt(this.index);
    if (code == 0x7b) return this.object();
    if (code == 0x5b) return this.array();
    if (code == 0x22) return Json.string(this.string());
    if (code == 0x2d || (code >= 0x30 && code <= 0x39)) return this.number();
    if (this.literal("true")) return Json.bool(true);
    if (this.literal("false")) return Json.bool(false);
    if (this.literal("null")) return new Json(JsonKind.Null);
    this.fail("expected a value");
    return new Json(JsonKind.Null);
  }

  literal(word: string): bool {
    if (this.text.substr(this.index, word.length) != word) return false;
    this.index += word.length;
    return true;
  }

  object(): Json {
    const object = Json.object();
    this.index++;
    this.space();
    if (this.peek() == 0x7d) {
      this.index++;
      return object;
    }
    while (true) {
      this.space();
      if (this.peek() != 0x22) this.fail("expected a key");
      const key = this.string();
      this.space();
      if (this.peek() != 0x3a) this.fail("expected `:`");
      this.index++;
      object.set(key, this.value());
      this.space();
      const next = this.peek();
      this.index++;
      if (next == 0x7d) return object;
      if (next != 0x2c) this.fail("expected `,` or `}`");
    }
  }

  array(): Json {
    const array = Json.array();
    this.index++;
    this.space();
    if (this.peek() == 0x5d) {
      this.index++;
      return array;
    }
    while (true) {
      array.push(this.value());
      this.space();
      const next = this.peek();
      this.index++;
      if (next == 0x5d) return array;
      if (next != 0x2c) this.fail("expected `,` or `]`");
    }
  }

  string(): string {
    this.index++;
    const parts = new Array<string>();
    let start = this.index;
    while (true) {
      if (this.index >= this.text.length) this.fail("unterminated string");
      const code = this.text.charCodeAt(this.index);
      if (code == 0x22) break;
      if (code < 0x20) this.fail("a control character in a string");
      if (code != 0x5c) {
        this.index++;
        continue;
      }
      parts.push(this.text.substring(start, this.index));
      const escaped = this.text.charCodeAt(this.index + 1);
      this.index += 2;
      if (escaped == 0x22) parts.push('"');
      else if (escaped == 0x5c) parts.push("\\");
      else if (escaped == 0x2f) parts.push("/");
      else if (escaped == 0x62) parts.push("\b");
      else if (escaped == 0x66) parts.push("\f");
      else if (escaped == 0x6e) parts.push("\n");
      else if (escaped == 0x72) parts.push("\r");
      else if (escaped == 0x74) parts.push("\t");
      else if (escaped == 0x75) {
        let unit = 0;
        for (let digit = 0; digit < 4; digit++) {
          const value = nibble(this.peek());
          if (value < 0) this.fail("an invalid \\u escape");
          unit = (unit << 4) | value;
          this.index++;
        }
        parts.push(String.fromCharCode(unit));
      } else this.fail("an invalid escape");
      start = this.index;
    }
    parts.push(this.text.substring(start, this.index));
    this.index++;
    return parts.join("");
  }

  number(): Json {
    const start = this.index;
    if (this.peek() == 0x2d) this.index++;
    if (!this.digits()) this.fail("expected a digit");
    if (this.peek() == 0x2e) {
      this.index++;
      if (!this.digits()) this.fail("expected a digit after `.`");
    }
    if (this.peek() == 0x65 || this.peek() == 0x45) {
      this.index++;
      if (this.peek() == 0x2b || this.peek() == 0x2d) this.index++;
      if (!this.digits()) this.fail("expected an exponent");
    }
    return Json.number(this.text.substring(start, this.index));
  }

  digits(): bool {
    const start = this.index;
    while (this.index < this.text.length) {
      const code = this.text.charCodeAt(this.index);
      if (code < 0x30 || code > 0x39) break;
      this.index++;
    }
    return this.index > start;
  }

  peek(): i32 {
    return this.index < this.text.length ? this.text.charCodeAt(this.index) : -1;
  }
}
