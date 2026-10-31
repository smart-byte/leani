// Running ERC-20 transfer count and volume per token.
//
// Ordered: each block adds to totals earlier blocks wrote, so blocks reduce
// in chain order and an undo restores the previous totals.
//
// Parameters: `{ "tokens": ["0x…"] }`; empty watches every contract.

import {
  Block,
  BlockRef,
  Capability,
  Json,
  Manifest,
  Processor,
  Reader,
  Store,
  U256,
  Writer,
  hex,
  keccak256,
  parseHex,
  register,
  utf8,
} from "./leani";
export * from "./leani/exports";

/** A token's transfers in one block. */
class Volume {
  transfers: u64 = 0;
  volume: U256 = U256.zero();

  constructor(public token: Uint8Array) {}
}

class TokenVolume extends Processor {
  manifest(params: Json): Manifest {
    params.only(["tokens"]);
    const list = params.get("tokens");
    const tokens = list == null ? new Array<Uint8Array>() : list.values().map<Uint8Array>((token: Json) => parseHex(token.asString(), 20));
    return new Manifest("token-volume")
      .ordered()
      .require(Capability.Logs)
      .addresses(tokens)
      .topic(0, [keccak256(utf8("Transfer(address,address,uint256)"))]);
  }

  // The delta: one (token, value) per transfer, in log order.
  map(params: Json, block: Block, delta: Writer): void {
    for (let index = 0; index < block.logs.length; index++) {
      const log = block.logs[index];
      // Three topics and a one-word value: an ERC-20 transfer.
      if (log.topics.length != 3 || log.data.length != 32) continue;
      delta.raw(log.address).raw(log.data);
    }
  }

  reduce(params: Json, block: BlockRef, delta: Reader, store: Store): void {
    // This block's totals per token, written in token order.
    const totals = new Map<string, Volume>();
    while (!delta.done) {
      const token = delta.raw(20);
      const value = delta.u256();
      const name = hex(token);
      if (!totals.has(name)) totals.set(name, new Volume(token));
      const tally = totals.get(name);
      tally.transfers += 1;
      tally.volume = tally.volume.saturatingAdd(value);
    }
    const names = totals.keys().sort();
    for (let index = 0; index < names.length; index++) {
      const added = totals.get(names[index]);
      let transfers: u64 = 0;
      let volume = U256.zero();
      const stored = store.getJson("volumes", added.token);
      if (stored != null) {
        transfers = stored.field("transfers").asU64();
        volume = U256.fromJson(stored.field("volume"));
      }
      const entity = Json.object()
        .set("token", added.token)
        .set("transfers", transfers + added.transfers)
        .set("volume", volume.saturatingAdd(added.volume))
        .set("lastBlock", block.number);
      store.upsert("volumes", "erc20.volume", added.token, entity);
    }
  }
}

register(new TokenVolume());
