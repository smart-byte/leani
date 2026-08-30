import {
  applyEntityChange,
  createLeaniClient,
  isResetRequired,
  type EntityChangeTarget,
} from "@leani/sdk";

interface PoolPrice {
  pool: `0x${string}`;
  reserve0: string;
  reserve1: string;
}

const leani = createLeaniClient({ baseUrl: "http://127.0.0.1:8080" });
const processor = "uniswap-v2-prices";
const collection = "uniswap.pool_prices";
const prices = new Map<string, PoolPrice>();
const target: EntityChangeTarget<PoolPrice> = {
  put: (key, value) => {
    prices.set(key, value);
  },
  delete: (key) => {
    prices.delete(key);
  },
};

// docs:start query-then-follow
const snapshot = await leani.processors.queryAndFollow<PoolPrice>(processor, collection);

for (const entity of snapshot.data) prices.set(entity.key, entity.data);
await leani.processors.releaseSnapshot(processor, snapshot.snapshotId);

for await (const change of leani.processors.subscribe<PoolPrice>(processor, {
  after: snapshot.boundaryCursor,
})) {
  if (isResetRequired(change)) throw new Error("snapshot rebuild required");

  if (change.operation === "finalized") {
    console.log("finalized through", change.block?.number);
  } else {
    // Apply and undo carry the forward or inverse entity mutation.
    await applyEntityChange(target, change);
  }
}
// docs:end query-then-follow
