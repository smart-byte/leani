// Decode any contract event from its Solidity signature: no code to write.
//
// Parameters:
//
//   {
//     "addresses": ["0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"],
//     "events": ["Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"]
//   }
//
// `addresses` is optional: without it, every contract emitting one of the
// events matches. `"withTransactionHash": true` adds each event's
// transaction hash, which the node's sources must then supply. Supported
// argument types: `address`, `bool`, `uint<N>`, `int<N>`, `bytes<N>`,
// `bytes`, and `string`; an indexed `bytes` or `string` decodes to its hash.
// Block-local: each event is keyed by its block number and log index.

import {
  Block,
  BlockRef,
  Capability,
  Json,
  Manifest,
  Processor,
  Reader,
  Store,
  Writer,
  bigEndian32,
  bigEndian64,
  concat,
  indexOf,
  parseHex,
  register,
} from "./leani";
import { Signature } from "./leani/solidity";
export * from "./leani/exports";

/** One decoded event, as `map` hands it to `reduce`. */
class Decoded {
  constructor(
    public contract: Uint8Array,
    /** Index into the configured events. */
    public event: i32,
    public transactionHash: Uint8Array | null,
    public transactionIndex: u32,
    public logIndex: u32,
    /** The arguments by name, as JSON. */
    public args: Json,
  ) {}

  write(delta: Writer): void {
    const hash = this.transactionHash;
    delta.raw(this.contract).uint(<u64>this.event).flag(hash != null);
    if (hash != null) delta.raw(hash);
    delta.uint(this.transactionIndex).uint(this.logIndex).text(this.args.toString());
  }

  static read(delta: Reader): Decoded {
    const contract = delta.raw(20);
    const event = <i32>delta.uint();
    const transactionHash = delta.flag() ? delta.raw(32) : null;
    const transactionIndex = <u32>delta.uint();
    const logIndex = <u32>delta.uint();
    return new Decoded(contract, event, transactionHash, transactionIndex, logIndex, Json.parse(delta.text()));
  }
}

class EventDecoder extends Processor {
  manifest(params: Json): Manifest {
    params.only(["addresses", "events", "withTransactionHash"]);
    const events = parseEvents(params);
    const list = params.get("addresses");
    const addresses = list == null ? new Array<Uint8Array>() : list.values().map<Uint8Array>((address: Json) => parseHex(address.asString(), 20));
    const manifest = new Manifest("event-decoder")
      .require(Capability.Logs)
      .addresses(addresses)
      .topic(0, events.map<Uint8Array>((event: Signature) => event.topic()));
    const withHash = params.get("withTransactionHash");
    return withHash != null && withHash.asBool() ? manifest.logsWithTransactionHash() : manifest;
  }

  map(params: Json, block: Block, delta: Writer): void {
    const events = parseEvents(params);
    const topics = events.map<Uint8Array>((event: Signature) => event.topic());
    for (let index = 0; index < block.logs.length; index++) {
      const log = block.logs[index];
      const position = log.topics.length > 0 ? indexOf(topics, log.topics[0]) : -1;
      if (position < 0) continue;
      const event = events[position];
      // Another contract may emit the same topic with another layout: a log
      // that does not decode is not this event.
      const values = event.decodeEvent(log.topics.slice(1), log.data);
      if (values == null) continue;
      const args = Json.object();
      for (let input = 0; input < event.inputs.length; input++) args.set(event.inputs[input].name, values[input]);
      new Decoded(log.address, position, log.transactionHash, log.transactionIndex, log.logIndex, args).write(delta);
    }
  }

  reduce(params: Json, block: BlockRef, delta: Reader, store: Store): void {
    const events = parseEvents(params);
    while (!delta.done) {
      const log = Decoded.read(delta);
      if (log.event >= events.length) throw new Error("a delta names an unknown event");
      const event = events[log.event];
      const entity = Json.object()
        .set("contract", log.contract)
        .set("event", event.name)
        .set("signature", event.canonical)
        .set("blockNumber", block.number)
        .set("timestamp", block.timestamp);
      const hash = log.transactionHash;
      if (hash != null) entity.set("transactionHash", hash);
      entity.set("transactionIndex", log.transactionIndex).set("logIndex", log.logIndex).set("args", log.args);
      // A log index is unique within its block.
      const key = concat(bigEndian64(block.number), bigEndian32(log.logIndex));
      store.upsert("events", `event.${event.name}`, key, entity);
    }
  }
}

function parseEvents(params: Json): Signature[] {
  const list = params.field("events").values();
  if (list.length == 0 || list.length > 32) throw new Error("give between 1 and 32 events");
  return list.map<Signature>((text: Json) => Signature.parse(text.asString()));
}

register(new EventDecoder());
