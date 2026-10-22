// ERC-20 transfers, optionally of chosen tokens and above a minimum value.
//
// Block-local: each transfer is keyed by its transaction hash and log index,
// which no other block shares, so blocks reduce independently.
//
// Parameters (all optional):
//
//   { "tokens": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"], "minValue": "1000000000" }

import {
  Block,
  BlockRef,
  Json,
  Manifest,
  Processor,
  Reader,
  Store,
  U256,
  Writer,
  bigEndian32,
  concat,
  keccak256,
  parseHex,
  register,
  utf8,
} from "./leani";
export * from "./leani/exports";

/** One transfer, as `map` hands it to `reduce`. */
class Transfer {
  constructor(
    public token: Uint8Array,
    public from: Uint8Array,
    public to: Uint8Array,
    public value: U256,
    public transactionHash: Uint8Array,
    public logIndex: u32,
  ) {}

  write(delta: Writer): void {
    delta.raw(this.token).raw(this.from).raw(this.to).u256(this.value).raw(this.transactionHash).uint(this.logIndex);
  }

  static read(delta: Reader): Transfer {
    return new Transfer(delta.raw(20), delta.raw(20), delta.raw(20), delta.u256(), delta.raw(32), <u32>delta.uint());
  }
}

class Transfers extends Processor {
  manifest(params: Json): Manifest {
    params.only(["tokens", "minValue"]);
    // A threshold that does not parse fails here, as it loads, not in
    // every block.
    minValue(params);
    // The node filters logs by contract and topic before the guest sees
    // them, whatever the source delivered.
    return new Manifest("erc20-transfers")
      .logsWithTransactionHash()
      .addresses(tokens(params))
      .topic(0, [keccak256(utf8("Transfer(address,address,uint256)"))]);
  }

  map(params: Json, block: Block, delta: Writer): void {
    const minimum = minValue(params);
    for (let index = 0; index < block.logs.length; index++) {
      const log = block.logs[index];
      // ERC-721 shares the signature with three indexed arguments and
      // no data; other layouts are not ERC-20 transfers, so skip them.
      if (log.topics.length != 3 || log.data.length != 32) continue;
      const from = address(log.topics[1]);
      const to = address(log.topics[2]);
      if (from == null || to == null) continue;
      const value = new U256(log.data);
      if (minimum != null && value < minimum) continue;
      const transactionHash = log.transactionHash;
      if (transactionHash == null) throw new Error("a transfer log lacks its transaction hash");
      new Transfer(log.address, from, to, value, transactionHash, log.logIndex).write(delta);
    }
  }

  reduce(params: Json, block: BlockRef, delta: Reader, store: Store): void {
    while (!delta.done) {
      const transfer = Transfer.read(delta);
      // The public entity and change payload.
      const entity = Json.object()
        .set("token", transfer.token)
        .set("from", transfer.from)
        .set("to", transfer.to)
        .set("value", transfer.value)
        .set("transactionHash", transfer.transactionHash)
        .set("logIndex", transfer.logIndex)
        .set("blockNumber", block.number)
        .set("timestamp", block.timestamp);
      const key = concat(transfer.transactionHash, bigEndian32(transfer.logIndex));
      store.upsert("transfers", "erc20.transfer", key, entity);
    }
  }
}

/** Token contracts to watch; empty watches every contract. */
function tokens(params: Json): Uint8Array[] {
  const list = params.get("tokens");
  if (list == null) return [];
  return list.values().map<Uint8Array>((token: Json) => parseHex(token.asString(), 20));
}

/** Smallest transfer to keep, in the token's base units. */
function minValue(params: Json): U256 | null {
  const minimum = params.get("minValue");
  return minimum == null || minimum.isNull ? null : U256.fromJson(minimum);
}

/** The address in an indexed argument, if its ABI padding is zero. */
function address(topic: Uint8Array): Uint8Array | null {
  for (let index = 0; index < 12; index++) {
    if (topic[index] != 0) return null;
  }
  return topic.slice(12, 32);
}

register(new Transfers());
