// docs:start query-then-follow
import {
  applyEntityChange, createLeaniClient, LeaniError,
  type GenericSnapshotPage, type LeaniClient, type UniswapPoolPrice,
} from "@leani/sdk";

// Run examples/sdk-stream/node.toml first (see the guide).
export async function followPrices(
  leani: LeaniClient = createLeaniClient({ baseUrl: "http://127.0.0.1:8080" }),
  prices = new Map<string, UniswapPoolPrice>(),
  signal?: AbortSignal,
): Promise<void> {
  const processor = "usdc-weth-latest";
  const collection = "uniswap.pools.current";
  const target = {
    put: (key: string, value: UniswapPoolPrice) => { prices.set(key, value); },
    delete: (key: string) => { prices.delete(key); },
  };
  while (!signal?.aborted) {
    try {
      const snapshot = await leani.processors.queryAndFollow<UniswapPoolPrice>(
        processor, collection, { signal },
      );
      const seed = new Map<string, UniswapPoolPrice>();
      try {
        let page: GenericSnapshotPage<UniswapPoolPrice> = snapshot;
        while (true) {
          for (const entity of page.data) seed.set(entity.key, entity.data);
          if (!page.nextCursor) break;
          page = await leani.processors.queryEntities<UniswapPoolPrice>(
            processor, collection, { cursor: page.nextCursor, signal },
          );
        }
      } finally {
        // Cleanup gets its own deadline even if the subscription was cancelled.
        await leani.processors.releaseSnapshot(processor, snapshot.snapshotId)
          .catch((error: unknown) => {
            if (!(error instanceof LeaniError && error.status === 404)) throw error;
          });
      }
      prices.clear();
      for (const [key, value] of seed) prices.set(key, value);
      console.log(`Loaded ${prices.size} pools`);
      for await (const change of leani.processors.subscribe<UniswapPoolPrice>(
        processor, { after: snapshot.boundaryCursor, signal },
      )) {
        // Both apply and undo carry the entity mutation to perform.
        await applyEntityChange(target, change);
        if (change.operation === "finalized") {
          console.log("finalized through", change.data.throughBlock);
        }
      }
    } catch (error) {
      if (signal?.aborted) return;
      // ResetRequiredError from SSE is also a LeaniError with cursor_expired.
      if (!(error instanceof LeaniError) ||
          !["cursor_expired", "query_snapshot_expired"].includes(error.code)) throw error;
      console.log("Retention moved; rebuilding the snapshot");
    }
  }
}

if (import.meta.main) await followPrices();
// docs:end query-then-follow
