// Gas used, gas limit, utilization, and base fee of every block.
//
// Block-local, keyed by block number, and needs only block headers, which
// a node following blocks fetches.

import { Block, BlockRef, Capability, Json, Manifest, Processor, Reader, Store, Writer, bigEndian64, register } from "./leani";
export * from "./leani/exports";

class BlockGas extends Processor {
  manifest(params: Json): Manifest {
    params.only(["minUtilizationPercent"]);
    // A threshold that does not parse fails here, as it loads, not in
    // every block.
    minUtilizationPercent(params);
    return new Manifest("block-gas").require(Capability.Header);
  }

  // The delta: nothing for a block below the minimum; else gas used, gas
  // limit, utilization, and the base fee when the block has one.
  map(params: Json, block: Block, delta: Writer): void {
    const header = block.header;
    if (header == null) throw new Error("the block has no header");
    const gasUsed = header.gasUsed;
    const gasLimit = header.gasLimit;
    if (gasUsed == null || gasLimit == null) return;
    const used = gasUsed.value;
    const limit = gasLimit.value;
    // Hundredths of a percent, so 9_731 is 97.31%.
    const utilizationBasisPoints = limit == 0 ? 0 : saturatingMul(used, 10_000) / limit;
    if (utilizationBasisPoints < saturatingMul(minUtilizationPercent(params), 100)) return;
    const fee = header.baseFeePerGas;
    delta.uint(used).uint(limit).uint(utilizationBasisPoints).flag(fee != null);
    if (fee != null) delta.u256(fee);
  }

  reduce(params: Json, block: BlockRef, delta: Reader, store: Store): void {
    if (delta.done) return;
    const entity = Json.object()
      .set("number", block.number)
      .set("timestamp", block.timestamp)
      .set("gasUsed", delta.uint())
      .set("gasLimit", delta.uint())
      .set("utilizationBasisPoints", delta.uint())
      .set("baseFeePerGas", delta.flag() ? delta.u256() : null);
    store.upsert("blocks", "block.gas", bigEndian64(block.number), entity);
  }
}

function minUtilizationPercent(params: Json): u64 {
  const minimum = params.get("minUtilizationPercent");
  return minimum == null ? 0 : minimum.asU64();
}

function saturatingMul(a: u64, b: u64): u64 {
  return b != 0 && a > u64.MAX_VALUE / b ? u64.MAX_VALUE : a * b;
}

register(new BlockGas());
