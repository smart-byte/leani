// docs:start keep-in-sync
import {
  applyEntityChange,
  isLiveGapChange,
  LeaniError,
  type EntityChangeTarget,
  type ProcessorCoverage,
} from "@smart-byte/leani-sdk";
import {
  createBackfillSubscriptionClient,
  liveGapBackfill,
  type BackfillRange,
  type BackfillStatus,
} from "@smart-byte/leani-sdk/backfill";

/**
 * Blocks from `from` that the processor lacks, through the last block it
 * processed. Before its first block, that is everything up to the finalized
 * head.
 */
export function coverageHoles(coverage: ProcessorCoverage, from: number): BackfillRange[] {
  const through = coverage.processedThrough ?? coverage.chainFinalizedHead?.number;
  if (through === undefined) return [];
  const holes: BackfillRange[] = [];
  let next = Math.max(from, coverage.configuredStartBlock);
  for (const { fromBlock, toBlock } of coverage.available) {
    if (fromBlock > next) holes.push({ fromBlock: next, toBlock: Math.min(fromBlock - 1, through) });
    next = Math.max(next, toBlock + 1);
  }
  if (next <= through) holes.push({ fromBlock: next, toBlock: through });
  return holes.filter((hole) => hole.fromBlock <= hole.toBlock);
}

/** An idempotent destination that also keeps the ranges it still has to request. */
export interface SyncedDestination<T> extends EntityChangeTarget<T> {
  /** Keep a range to request, together with the rows of the batch that announced it. */
  rememberGap(range: BackfillRange): Promise<void>;
  /** The ranges kept and not yet requested. */
  gaps(): Promise<BackfillRange[]>;
  /** Drop a range once a subscription delivers it. */
  forgetGap(range: BackfillRange): Promise<void>;
}

export interface SyncOptions {
  baseUrl: string;
  processor: string;
  consumer: string;
  credential: string;
  /** The first block the destination keeps. */
  fromBlock: number;
  /**
   * Consume each new subscription's history lane, beside the live lane:
   * start `runBackfill()` from `@smart-byte/leani-sdk/backfill` here, but do
   * not await it, which would hold the live lane back. Given
   * `liveGapBackfill(subscription, …)` again, it resumes this subscription,
   * applies and acknowledges its batches, and deletes it once complete.
   */
  onBackfill(subscription: BackfillStatus): void | Promise<void>;
  signal?: AbortSignal;
}

/**
 * Follow a processor's live lane into a destination, and request as history
 * every block the processor lacks: the holes in the coverage each session
 * starts with, and the blocks the live lane announces it skipped.
 */
export async function keepInSync<T>(
  destination: SyncedDestination<T>,
  options: SyncOptions,
): Promise<void> {
  const leani = createBackfillSubscriptionClient({ baseUrl: options.baseUrl });
  const requested: BackfillRange[] = [];
  const request = async () => {
    for (const range of await destination.gaps()) {
      const inFlight = requested.some(
        (earlier) => earlier.fromBlock <= range.fromBlock && range.toBlock <= earlier.toBlock,
      );
      if (!inFlight) {
        try {
          await options.onBackfill(await leani.create(liveGapBackfill(range, {
            processor: options.processor,
            consumer: {
              id: options.consumer,
              role: "required",
              leaseTtlSeconds: 300,
              credential: options.credential,
            },
            signal: options.signal,
          })));
        } catch (error) {
          // Keep the range and try again after the next batch: a request
          // that fails must not hold the live lane back.
          console.warn(`request for blocks ${range.fromBlock}-${range.toBlock} failed`, error);
          return;
        }
        requested.push(range);
      }
      await destination.forgetGap(range);
    }
  };
  let delayMs = 1_000;
  while (!options.signal?.aborted) {
    try {
      const session = await leani.streamLive<T>(options.processor, options.consumer, {
        credential: options.credential,
        signal: options.signal,
      });
      delayMs = 1_000;
      try {
        for (const hole of coverageHoles(session.hello.coverage, options.fromBlock)) {
          await destination.rememberGap(hole);
        }
        await request();
        for await (const record of session) {
          if (record.type !== "batch") continue;
          for (const change of record.changes) {
            if (isLiveGapChange(change)) await destination.rememberGap(change.data);
            else await applyEntityChange(destination, change);
          }
          await session.acknowledge(record.acknowledgeableCursor);
          await request();
        }
      } finally {
        await session.close();
      }
    } catch (error) {
      if (options.signal?.aborted) return;
      // An outage ends only this session: reconnect, which redelivers what
      // was not acknowledged and checks coverage again. A replaced session
      // can still hold the lease for a moment. Anything else, such as a
      // rejected credential, is the caller's.
      const transient =
        error instanceof LeaniError &&
        (error.retryable ||
          error.code === "consumer_session_active" ||
          error.code === "consumer_session_lost");
      if (!transient) throw error;
      console.warn(`live session failed; reconnecting in ${delayMs} ms`, error);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
      delayMs = Math.min(delayMs * 2, 30_000);
    }
  }
}
// docs:end keep-in-sync
