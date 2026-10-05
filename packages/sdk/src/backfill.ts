import { LeaniError, TransportError, responseError } from "./errors.ts";
export { LeaniError, TransportError } from "./errors.ts";
import type {
  ChangeEnvelope,
  DurableConsumer,
  FetchLike,
  ProcessorCoverage,
  ProcessorSummary,
} from "./index.ts";
import {
  concatBytes,
  idleWatchdog,
  readChunks,
  readJson,
  readText,
  resolveUnder,
  send,
  withDeadline,
} from "./transport.ts";

export type BackfillExecutionMode = "fill_missing" | "recompute";
export type BackfillState =
  | "waiting_for_consumer"
  | "queued"
  | "running"
  | "backpressured"
  | "storage_backpressured"
  | "draining"
  | "complete_reclaimable"
  | "completed"
  | "failed"
  | "cancelled";

export interface BackfillRange {
  fromBlock: number;
  toBlock: number;
}

export interface RequestedBackfillRange {
  fromBlock: number;
  toBlock: number | "finalized";
}

export interface BackfillStatus {
  id: string;
  owner: "subscription";
  processor: string;
  /**
   * Whether the node configures the processor instance `processor` now.
   * Without it, the stream and acknowledgement routes refuse the
   * subscription: cancel it and delete it with `discardUnacknowledged: true`.
   */
  processorConfigured: boolean;
  /** The required consumer; tell your subscriptions apart by it, not by `id`. */
  consumer: string | null;
  deliveryStreamId?: string;
  publicationRevision?: string;
  fromBlock: number;
  toBlock: number;
  ranges: BackfillRange[];
  requestedBlocks: number;
  processedBlocks: number;
  remainingBlocks: number;
  capturedFinalizedTarget?: number;
  mode: BackfillExecutionMode;
  batching?: EffectiveBackfillBatching;
  state: BackfillState;
  attempts: number;
  updatedAtUnixMs: number;
  report: unknown | null;
  lastError: string | null;
}

export interface HistoricalWorkDeletion {
  id: string;
  owner: "subscription";
  removedJobs: number;
  removedSubscriptionRanges: number;
  removedConsumers: number;
  removedDeliveryRecords: number;
  removedDeliveryStreams: number;
  removedCoverageIntervals: number;
  removedCoverageSegments: number;
  removedExactCoverage: number;
  removedAppliedBlocks: number;
  removedFinalizedUndo: number;
  retainedProcessorOutput: boolean;
  retainedLiveStream: boolean;
}

export type BackfillBatchingRequest = Partial<
  Omit<
    EffectiveBackfillBatching,
    "maximumBufferedBatches" | "maximumBufferedBytes"
  >
>;

export type BackfillBatchingProfile = "latency" | "balanced" | "throughput";

const BACKFILL_BATCHING_PROFILES: Record<
  BackfillBatchingProfile,
  Required<BackfillBatchingRequest>
> = {
  latency: {
    targetEncodedBytes: 256 * 1024,
    maximumEncodedBytes: 1024 * 1024,
    maximumEvents: 2_000,
    maximumProcessedBlocks: 512,
    maximumDelayMs: 10,
    compression: "gzip",
  },
  balanced: {
    targetEncodedBytes: 1024 * 1024,
    maximumEncodedBytes: 4 * 1024 * 1024,
    maximumEvents: 10_000,
    maximumProcessedBlocks: 4_096,
    maximumDelayMs: 25,
    compression: "gzip",
  },
  throughput: {
    targetEncodedBytes: 4 * 1024 * 1024,
    maximumEncodedBytes: 16 * 1024 * 1024,
    maximumEvents: 20_000,
    maximumProcessedBlocks: 8_192,
    maximumDelayMs: 50,
    compression: "gzip",
  },
};

export function backfillBatchingProfile(
  profile: BackfillBatchingProfile,
): Required<BackfillBatchingRequest> {
  return { ...BACKFILL_BATCHING_PROFILES[profile] };
}

export interface EffectiveBackfillBatching {
  targetEncodedBytes: number;
  maximumEncodedBytes: number;
  maximumEvents: number;
  maximumProcessedBlocks: number;
  maximumDelayMs: number;
  maximumBufferedBatches: number;
  maximumBufferedBytes: number;
  compression: "none" | "gzip";
}

interface CreateBackfillSubscriptionBase {
  processor: string;
  mode?: BackfillExecutionMode;
  consumer?: {
    id: string;
    role: "required";
    leaseTtlSeconds: number;
    credential?: string;
  };
  limits?: {
    maxUnacknowledgedBlocks: number;
    maxUnacknowledgedBytes: number;
    resumeBelowRatio: number;
  };
  batching?: BackfillBatchingRequest;
  idempotencyKey: string;
  signal?: AbortSignal;
}

export type CreateBackfillSubscription = CreateBackfillSubscriptionBase &
  (
    | {
        fromBlock: number;
        toBlock: number | "finalized";
        ranges?: never;
      }
    | {
        ranges: RequestedBackfillRange[];
        fromBlock?: never;
        toBlock?: never;
      }
  );

/**
 * The `fill_missing` subscription for the blocks a live gap notice
 * announces, its `LiveGapChangeData`, or any other range the destination
 * lacks. Its idempotency key derives from the consumer and the range, so
 * creating it again, as after the notice is delivered again, returns the
 * same subscription.
 */
export function liveGapBackfill(
  range: BackfillRange,
  request: Omit<CreateBackfillSubscriptionBase, "mode" | "idempotencyKey"> & {
    consumer: NonNullable<CreateBackfillSubscriptionBase["consumer"]>;
  },
): CreateBackfillSubscription {
  return {
    ...request,
    mode: "fill_missing",
    fromBlock: range.fromBlock,
    toBlock: range.toBlock,
    idempotencyKey: `live-gap:${request.consumer.id}:${range.fromBlock}-${range.toBlock}`,
  };
}

export interface BackfillStreamHello {
  type: "hello";
  apiVersion: "1";
  chainId: number;
  processor: ProcessorSummary;
  subscriptionId: string;
  streamId: string;
  streamKind: "backfill";
  publicationRevision: string;
  ranges: BackfillRange[];
  acknowledgedCursor: string;
  storeEpoch: string;
  heartbeatIntervalMs: string;
  sessionExpiresAtUnixMs: string;
  leaseTtlMs: string;
}

export interface BackfillStreamBatch<T = unknown> {
  type: "batch";
  streamId: string;
  originKind: "historical_backfill" | "recompute";
  originId: string;
  publicationRevision: string;
  fromBlock: number;
  throughBlock: number;
  processedBlockCount: string;
  domainChangeCount: string;
  progressUnitCount: string;
  rawPayloadBytes: string;
  uncompressedEncodedBytes: string;
  transmittedBytes: string;
  buildDelayMs: string;
  firstCursor: string | null;
  lastCursor: string | null;
  acknowledgeableCursor: string;
  changes: ChangeEnvelope<T>[];
}

export interface BackfillStreamCompletion {
  type: "backfill_complete";
  subscriptionId: string;
  streamId: string;
  ranges: BackfillRange[];
  throughBlock: number;
  cursor: string;
  mode: BackfillExecutionMode;
  disposition:
    | "published_all"
    | "published_missing_only"
    | "already_covered_noop";
  requestedBlockCount: string;
  coveredBeforeRequestBlockCount: string;
  coveredBeforeRequestRanges: BackfillRange[];
  newlyProcessedBlockCount: string;
  republishedBlockCount: string;
  domainChangeCount: string;
  preexistingCoverageSkipped: boolean;
  repairHint?: "recompute";
}

export interface BackfillStreamHeartbeat {
  type: "heartbeat";
  emittedAt: string;
}

export interface BackfillStreamReset {
  type: "reset_required";
  code: "reset_required";
  message: string;
  earliestAvailableSequence: string | null;
  latestAvailableSequence: string | null;
}

export interface BackfillStreamFailure {
  type: "error";
  code: string;
  message: string;
  earliestAvailableSequence: string | null;
  latestAvailableSequence: string | null;
}

export type BackfillStreamRecord<T = unknown> =
  | BackfillStreamBatch<T>
  | BackfillStreamCompletion
  | BackfillStreamHeartbeat;

export interface LiveStreamHello {
  type: "hello";
  apiVersion: "1";
  chainId: number;
  processor: ProcessorSummary;
  streamId: string;
  streamKind: "live";
  acknowledgedCursor: string;
  storeEpoch: string;
  heartbeatIntervalMs: string;
  sessionExpiresAtUnixMs: string;
  leaseTtlMs: string;
  /** What the processor covers as the session starts: diff it against the
   * destination to find blocks to request as history. */
  coverage: ProcessorCoverage;
}

export interface LiveStreamBatch<T = unknown> {
  type: "batch";
  streamId: string;
  originKind: "live" | "live_recovery";
  originId: string;
  publicationRevision: "0";
  fromBlock: number;
  throughBlock: number;
  processedBlockCount: "1";
  domainChangeCount: string;
  progressUnitCount: "1";
  rawPayloadBytes: string;
  uncompressedEncodedBytes: string;
  transmittedBytes: string;
  buildDelayMs: string;
  firstCursor: string;
  lastCursor: string;
  acknowledgeableCursor: string;
  changes: ChangeEnvelope<T>[];
}

export type LiveStreamRecord<T = unknown> =
  | LiveStreamBatch<T>
  | BackfillStreamHeartbeat;

export interface DeliverySession<THello, TRecord>
  extends AsyncIterable<TRecord> {
  readonly hello: THello;
  acknowledge(
    cursor: string,
    options?: { signal?: AbortSignal },
  ): Promise<DurableConsumer>;
  close(): Promise<void>;
}

export interface BackfillDeliverySession<T = unknown>
  extends DeliverySession<BackfillStreamHello, BackfillStreamRecord<T>> {
  batches(): AsyncIterable<BackfillStreamBatch<T>>;
  /**
   * Every acknowledgement boundary with the changes it covers: each batch,
   * including one without changes, then the completion. Commit `events`,
   * then acknowledge `ackCursor`.
   */
  deliveryBatches(): AsyncIterable<HistoryDeliveryBatch<T>>;
  /**
   * The batches' changes, flattened. They carry no acknowledgement
   * boundary; acknowledge from `deliveryBatches()` or `batches()`.
   */
  events(): AsyncIterable<ChangeEnvelope<T>>;
}

export type LiveDeliverySession<T = unknown> = DeliverySession<
  LiveStreamHello,
  LiveStreamRecord<T>
>;

export interface MultiLaneSubscriptionRequest {
  processor: string;
  consumer: string;
  credential?: string;
  lanes: {
    live?: boolean;
    history?: Array<{ subscriptionId: string }>;
  };
  view?: "live_priority" | "lane_specific";
  batching?: BackfillBatchingRequest;
  signal?: AbortSignal;
}

export type UnifiedDeliveryBatch<T = unknown> =
  | {
      laneKind: "live";
      laneId: "live";
      streamId: string;
      ackCursor: string;
      events: ChangeEnvelope<T>[];
      completion?: never;
      record: LiveStreamBatch<T>;
    }
  | {
      laneKind: "history";
      laneId: string;
      streamId: string;
      ackCursor: string;
      events: ChangeEnvelope<T>[];
      completion?: BackfillStreamCompletion;
      record: BackfillStreamBatch<T> | BackfillStreamCompletion;
    };

/** One history lane's batch, as `BackfillDeliverySession.deliveryBatches()` yields it. */
export type HistoryDeliveryBatch<T = unknown> = Extract<
  UnifiedDeliveryBatch<T>,
  { laneKind: "history" }
>;

export interface MultiLaneDelivery<T = unknown> {
  batches(): AsyncIterable<UnifiedDeliveryBatch<T>>;
  liveBatches(): AsyncIterable<UnifiedDeliveryBatch<T>>;
  historyBatches(
    subscriptionId: string,
  ): AsyncIterable<UnifiedDeliveryBatch<T>>;
  acknowledge(
    batch: UnifiedDeliveryBatch<T>,
    options?: { signal?: AbortSignal },
  ): Promise<DurableConsumer>;
  close(): Promise<void>;
}

export interface BackfillSubscriptionClientOptions {
  baseUrl: string | URL;
  token?: string;
  fetch?: FetchLike;
  /** Deadline for JSON requests; established delivery streams stay open. */
  timeoutMs?: number;
}

export class BackfillStreamError extends Error {
  readonly code: string;
  readonly earliestAvailableSequence: string | null;
  readonly latestAvailableSequence: string | null;

  constructor(record: BackfillStreamFailure | BackfillStreamReset) {
    super(record.message);
    this.name =
      record.type === "reset_required"
        ? "BackfillStreamResetError"
        : "BackfillStreamError";
    this.code = record.code;
    this.earliestAvailableSequence = record.earliestAvailableSequence;
    this.latestAvailableSequence = record.latestAvailableSequence;
  }
}

export interface BackfillSubscriptionClient {
  readonly baseUrl: URL;
  create(request: CreateBackfillSubscription): Promise<BackfillStatus>;
  list(options?: { signal?: AbortSignal }): Promise<BackfillStatus[]>;
  inspect(
    subscription: string,
    options?: { signal?: AbortSignal },
  ): Promise<BackfillStatus>;
  cancel(
    subscription: string,
    options?: { signal?: AbortSignal },
  ): Promise<BackfillStatus>;
  /** Re-queue a failed subscription to resume from its committed progress. */
  retry(
    subscription: string,
    options?: { signal?: AbortSignal },
  ): Promise<BackfillStatus>;
  /**
   * Delete a terminal subscription. A failed or cancelled one is refused
   * while its required consumer has unacknowledged records, unless
   * `discardUnacknowledged` is set.
   */
  delete(
    subscription: string,
    options?: { signal?: AbortSignal; discardUnacknowledged?: boolean },
  ): Promise<HistoricalWorkDeletion>;
  changes<T = unknown>(
    subscription: string,
    consumer: string,
    options?: {
      credential?: string;
      limit?: number;
      signal?: AbortSignal;
    },
  ): Promise<{
    data: ChangeEnvelope<T>[];
    nextCursor: string | null;
    coverage: unknown;
  }>;
  stream<T = unknown>(
    subscription: string,
    consumer: string,
    options?: {
      credential?: string;
      batching?: BackfillBatchingRequest;
      signal?: AbortSignal;
    },
  ): Promise<BackfillDeliverySession<T>>;
  streamLive<T = unknown>(
    processor: string,
    consumer: string,
    options?: { credential?: string; signal?: AbortSignal },
  ): Promise<LiveDeliverySession<T>>;
  subscribe<T = unknown>(
    request: MultiLaneSubscriptionRequest,
  ): Promise<MultiLaneDelivery<T>>;
}

export function createBackfillSubscriptionClient(
  options: BackfillSubscriptionClientOptions,
): BackfillSubscriptionClient {
  const baseUrl = normalizeBaseUrl(options.baseUrl);
  const fetchImpl = options.fetch ?? globalThis.fetch;
  if (!fetchImpl) {
    throw new TypeError("a fetch implementation is required");
  }

  const timeoutMs = options.timeoutMs ?? 30_000;
  assertPositiveInteger(timeoutMs, "timeoutMs");
  const requestJson = async <T>(
    method: "GET" | "POST" | "DELETE",
    path: string,
    body: unknown,
    signal: AbortSignal | undefined,
    credential?: string,
    sessionToken?: string,
  ): Promise<T> => {
    const headers = requestHeaders(
      options.token,
      "application/json",
      credential,
      sessionToken,
    );
    const response = await send(fetchImpl, resolveUnder(baseUrl, path), {
      method,
      headers:
        method === "GET" || body !== undefined
          ? headers
          : markLeaniRequest(headers),
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: withDeadline(signal, timeoutMs),
    }, signal);
    if (!response.ok) {
      throw await responseError(response);
    }
    return readJson<T>(response, signal);
  };

  return Object.freeze({
    baseUrl,
    create: (request: CreateBackfillSubscription) =>
      requestJson<BackfillStatus>(
        "POST",
        "admin/v1/backfill-subscriptions",
        {
          processor: request.processor,
          ...(request.ranges === undefined
            ? {
                fromBlock: assertBlock(request.fromBlock, "fromBlock"),
                toBlock: assertUpperBound(request.toBlock, "toBlock"),
              }
            : {
                ranges: request.ranges.map((range, index) => ({
                  fromBlock: assertBlock(
                    range.fromBlock,
                    `ranges[${index}].fromBlock`,
                  ),
                  toBlock: assertUpperBound(
                    range.toBlock,
                    `ranges[${index}].toBlock`,
                  ),
                })),
              }),
          mode: request.mode ?? "fill_missing",
          consumer:
            request.consumer === undefined
              ? undefined
              : {
                  id: assertIdentity(request.consumer.id, "consumer.id"),
                  role: request.consumer.role,
                  leaseTtlSeconds: assertPositiveInteger(
                    request.consumer.leaseTtlSeconds,
                    "consumer.leaseTtlSeconds",
                  ),
                  credential:
                    request.consumer.credential === undefined
                      ? undefined
                      : assertIdentity(
                          request.consumer.credential,
                          "consumer.credential",
                        ),
                },
          limits:
            request.limits === undefined
              ? undefined
              : {
                  maxUnacknowledgedBlocks: assertPositiveInteger(
                    request.limits.maxUnacknowledgedBlocks,
                    "limits.maxUnacknowledgedBlocks",
                  ),
                  maxUnacknowledgedBytes: assertPositiveInteger(
                    request.limits.maxUnacknowledgedBytes,
                    "limits.maxUnacknowledgedBytes",
                  ),
                  resumeBelowRatio: assertRatio(
                    request.limits.resumeBelowRatio,
                    "limits.resumeBelowRatio",
                  ),
                },
          batching:
            request.batching === undefined
              ? undefined
              : {
                  targetEncodedBytes:
                    request.batching.targetEncodedBytes === undefined
                      ? undefined
                      : assertPositiveInteger(
                          request.batching.targetEncodedBytes,
                          "batching.targetEncodedBytes",
                        ),
                  maximumEncodedBytes:
                    request.batching.maximumEncodedBytes === undefined
                      ? undefined
                      : assertPositiveInteger(
                          request.batching.maximumEncodedBytes,
                          "batching.maximumEncodedBytes",
                        ),
                  maximumEvents:
                    request.batching.maximumEvents === undefined
                      ? undefined
                      : assertPositiveInteger(
                          request.batching.maximumEvents,
                          "batching.maximumEvents",
                        ),
                  maximumProcessedBlocks:
                    request.batching.maximumProcessedBlocks === undefined
                      ? undefined
                      : assertPositiveInteger(
                          request.batching.maximumProcessedBlocks,
                          "batching.maximumProcessedBlocks",
                        ),
                  maximumDelayMs:
                    request.batching.maximumDelayMs === undefined
                      ? undefined
                      : assertPositiveInteger(
                          request.batching.maximumDelayMs,
                          "batching.maximumDelayMs",
                        ),
                  compression: request.batching.compression,
                },
          idempotencyKey: assertIdentity(
            request.idempotencyKey,
            "idempotencyKey",
          ),
        },
        request.signal,
      ),
    list: async (listOptions?: { signal?: AbortSignal }) => {
      const response = await requestJson<{ data: BackfillStatus[] }>(
        "GET",
        "admin/v1/backfill-subscriptions",
        undefined,
        listOptions?.signal,
      );
      return response.data;
    },
    inspect: (
      subscription: string,
      inspectOptions?: { signal?: AbortSignal },
    ) =>
      requestJson<BackfillStatus>(
        "GET",
        `admin/v1/backfill-subscriptions/${encodeURIComponent(assertIdentity(subscription, "subscription"))}`,
        undefined,
        inspectOptions?.signal,
      ),
    cancel: (
      subscription: string,
      cancelOptions?: { signal?: AbortSignal },
    ) =>
      requestJson<BackfillStatus>(
        "POST",
        `admin/v1/backfill-subscriptions/${encodeURIComponent(assertIdentity(subscription, "subscription"))}/cancel`,
        undefined,
        cancelOptions?.signal,
      ),
    retry: (
      subscription: string,
      retryOptions?: { signal?: AbortSignal },
    ) =>
      requestJson<BackfillStatus>(
        "POST",
        `admin/v1/backfill-subscriptions/${encodeURIComponent(assertIdentity(subscription, "subscription"))}/retry`,
        undefined,
        retryOptions?.signal,
      ),
    delete: (
      subscription: string,
      deleteOptions?: { signal?: AbortSignal; discardUnacknowledged?: boolean },
    ) =>
      requestJson<HistoricalWorkDeletion>(
        "DELETE",
        `admin/v1/backfill-subscriptions/${encodeURIComponent(assertIdentity(subscription, "subscription"))}${deleteOptions?.discardUnacknowledged ? "?discardUnacknowledged=true" : ""}`,
        undefined,
        deleteOptions?.signal,
      ),
    changes: <T>(
      subscription: string,
      consumer: string,
      changeOptions: {
        credential?: string;
        limit?: number;
        signal?: AbortSignal;
      } = {},
    ) => {
      const path = consumerPath(subscription, consumer, "changes");
      const query =
        changeOptions.limit === undefined
          ? ""
          : `?limit=${assertPositiveInteger(changeOptions.limit, "limit")}`;
      return requestJson<{
        data: ChangeEnvelope<T>[];
        nextCursor: string | null;
        coverage: unknown;
      }>(
        "GET",
        `${path}${query}`,
        undefined,
        changeOptions.signal,
        changeOptions.credential,
      );
    },
    stream: <T>(
      subscription: string,
      consumer: string,
      streamOptions: {
        credential?: string;
        batching?: BackfillBatchingRequest;
        signal?: AbortSignal;
      } = {},
    ) =>
      openBackfillStream<T>(
        fetchImpl,
        baseUrl,
        options.token,
        timeoutMs,
        subscription,
        consumer,
        streamOptions.credential,
        streamOptions.batching,
        streamOptions.signal,
      ),
    streamLive: <T>(
      processor: string,
      consumer: string,
      streamOptions: { credential?: string; signal?: AbortSignal } = {},
    ) =>
      openLiveStream<T>(
        fetchImpl,
        baseUrl,
        options.token,
        timeoutMs,
        processor,
        consumer,
        streamOptions.credential,
        streamOptions.signal,
      ),
    subscribe: <T>(request: MultiLaneSubscriptionRequest) =>
      openMultiLaneDelivery<T>(
        fetchImpl,
        baseUrl,
        options.token,
        timeoutMs,
        request,
      ),
  });
}

interface UnifiedLane<T> {
  readonly kind: "live" | "history";
  readonly laneId: string;
  readonly streamId: string;
  readonly deliveryOrdering: "canonical" | "block_versioned_idempotent";
  next(): Promise<IteratorResult<UnifiedDeliveryBatch<T>>>;
  acknowledge(
    cursor: string,
    options?: { signal?: AbortSignal },
  ): Promise<DurableConsumer>;
  close(): Promise<void>;
}

async function openMultiLaneDelivery<T>(
  fetchImpl: FetchLike,
  baseUrl: URL,
  token: string | undefined,
  timeoutMs: number,
  request: MultiLaneSubscriptionRequest,
): Promise<MultiLaneDelivery<T>> {
  const processor = assertIdentity(request.processor, "processor");
  const consumer = assertIdentity(request.consumer, "consumer");
  const history = request.lanes.history ?? [];
  const historyIds = history.map(({ subscriptionId }, index) =>
    assertIdentity(subscriptionId, `lanes.history[${index}].subscriptionId`),
  );
  if (new Set(historyIds).size !== historyIds.length) {
    throw new TypeError("history subscription IDs must be unique");
  }
  if (!request.lanes.live && historyIds.length === 0) {
    throw new TypeError("subscribe requires at least one live or history lane");
  }
  const view = request.view ?? "live_priority";
  const lanes: UnifiedLane<T>[] = [];
  try {
    if (request.lanes.live) {
      const open = (signal = request.signal) =>
        openLiveStream<T>(
          fetchImpl,
          baseUrl,
          token,
          timeoutMs,
          processor,
          consumer,
          request.credential,
          signal,
        );
      lanes.push(unifiedLiveLane(await open(), open, request.signal));
    }
    for (const subscriptionId of historyIds) {
      const open = (signal = request.signal) =>
        openBackfillStream<T>(
          fetchImpl,
          baseUrl,
          token,
          timeoutMs,
          subscriptionId,
          consumer,
          request.credential,
          request.batching,
          signal,
        );
      lanes.push(
        unifiedHistoryLane(
          subscriptionId,
          await open(),
          open,
          request.signal,
        ),
      );
    }
  } catch (error) {
    await Promise.allSettled(lanes.map((lane) => lane.close()));
    throw error;
  }
  if (
    lanes.length > 1 &&
    lanes.some((lane) => lane.deliveryOrdering === "canonical")
  ) {
    await Promise.allSettled(lanes.map((lane) => lane.close()));
    throw new TypeError(
      "canonical processors cannot interleave live and history lanes",
    );
  }

  const claimed = new Set<string>();
  const claim = (selected: UnifiedLane<T>[]): void => {
    const duplicate = selected.find((lane) => claimed.has(lane.streamId));
    if (duplicate) {
      throw new TypeError(`delivery lane ${duplicate.streamId} is already being iterated`);
    }
    for (const lane of selected) {
      claimed.add(lane.streamId);
    }
  };
  const iterate = (selected: UnifiedLane<T>): AsyncIterable<UnifiedDeliveryBatch<T>> =>
    iterateUnifiedLanes([selected], claim);
  const byStream = new Map(lanes.map((lane) => [lane.streamId, lane]));
  const live = lanes.find((lane) => lane.kind === "live");
  const byHistoryId = new Map(
    lanes
      .filter((lane) => lane.kind === "history")
      .map((lane) => [lane.laneId, lane]),
  );
  return Object.freeze({
    batches: () => {
      if (view !== "live_priority") {
        throw new TypeError(
          "batches() requires view=live_priority; use lane-specific iterators",
        );
      }
      return iterateUnifiedLanes(lanes, claim);
    },
    liveBatches: () => {
      if (view !== "lane_specific") {
        throw new TypeError("liveBatches() requires view=lane_specific");
      }
      if (!live) {
        throw new TypeError("the delivery subscription has no live lane");
      }
      return iterate(live);
    },
    historyBatches: (subscriptionId: string) => {
      if (view !== "lane_specific") {
        throw new TypeError("historyBatches() requires view=lane_specific");
      }
      const id = assertIdentity(subscriptionId, "subscriptionId");
      const lane = byHistoryId.get(id);
      if (!lane) {
        throw new TypeError(`history subscription ${id} is not selected`);
      }
      return iterate(lane);
    },
    acknowledge: (
      batch: UnifiedDeliveryBatch<T>,
      acknowledgeOptions?: { signal?: AbortSignal },
    ) => {
      const lane = byStream.get(batch.streamId);
      if (!lane || lane.laneId !== batch.laneId || lane.kind !== batch.laneKind) {
        throw new TypeError("delivery batch does not belong to this subscription object");
      }
      return lane.acknowledge(batch.ackCursor, acknowledgeOptions);
    },
    close: async () => {
      const outcomes = await Promise.allSettled(lanes.map((lane) => lane.close()));
      const failed = outcomes.find(
        (outcome): outcome is PromiseRejectedResult => outcome.status === "rejected",
      );
      if (failed) {
        throw failed.reason;
      }
    },
  });
}

function unifiedLiveLane<T>(
  initialSession: LiveDeliverySession<T>,
  open: (signal?: AbortSignal) => Promise<LiveDeliverySession<T>>,
  signal: AbortSignal | undefined,
): UnifiedLane<T> {
  const connection = reconnectingLaneConnection(initialSession, open, signal);
  return {
    kind: "live",
    laneId: "live",
    streamId: initialSession.hello.streamId,
    deliveryOrdering: initialSession.hello.processor.deliveryOrdering,
    async next() {
      while (true) {
        const next = await connection.next();
        if (next.done) return { done: true, value: undefined };
        if (next.value.type === "heartbeat") continue;
        const record = next.value;
        return {
          done: false,
          value: {
            laneKind: "live",
            laneId: "live",
            streamId: record.streamId,
            ackCursor: record.acknowledgeableCursor,
            events: record.changes,
            record,
          },
        };
      }
    },
    acknowledge: connection.acknowledge,
    close: connection.close,
  };
}

function unifiedHistoryLane<T>(
  subscriptionId: string,
  initialSession: BackfillDeliverySession<T>,
  open: (signal?: AbortSignal) => Promise<BackfillDeliverySession<T>>,
  signal: AbortSignal | undefined,
): UnifiedLane<T> {
  const connection = reconnectingLaneConnection(initialSession, open, signal);
  let complete = false;
  return {
    kind: "history",
    laneId: subscriptionId,
    streamId: initialSession.hello.streamId,
    deliveryOrdering: initialSession.hello.processor.deliveryOrdering,
    async next() {
      if (complete) return { done: true, value: undefined };
      while (true) {
        const next = await connection.next();
        if (next.done) return { done: true, value: undefined };
        if (next.value.type === "heartbeat") continue;
        complete = next.value.type === "backfill_complete";
        return {
          done: false,
          value: historyDeliveryBatch(subscriptionId, next.value),
        };
      }
    },
    acknowledge: connection.acknowledge,
    close: connection.close,
  };
}

/** A history batch or the completion with the cursor that acknowledges it. */
function historyDeliveryBatch<T>(
  laneId: string,
  record: BackfillStreamBatch<T> | BackfillStreamCompletion,
): HistoryDeliveryBatch<T> {
  if (record.type === "backfill_complete") {
    return {
      laneKind: "history",
      laneId,
      streamId: record.streamId,
      ackCursor: record.cursor,
      events: [],
      completion: record,
      record,
    };
  }
  return {
    laneKind: "history",
    laneId,
    streamId: record.streamId,
    ackCursor: record.acknowledgeableCursor,
    events: record.changes,
    record,
  };
}

interface ReconnectingLaneConnection<THello, TRecord> {
  next(): Promise<IteratorResult<TRecord>>;
  acknowledge(
    cursor: string,
    options?: { signal?: AbortSignal },
  ): Promise<DurableConsumer>;
  close(): Promise<void>;
}

function reconnectingLaneConnection<
  THello extends {
    streamId: string;
    storeEpoch: string;
    processor: ProcessorSummary;
  },
  TRecord,
>(
  initialSession: DeliverySession<THello, TRecord>,
  open: (signal?: AbortSignal) => Promise<DeliverySession<THello, TRecord>>,
  signal: AbortSignal | undefined,
): ReconnectingLaneConnection<THello, TRecord> {
  const expected = initialSession.hello;
  const stop = new AbortController();
  const reconnectSignal = signal
    ? AbortSignal.any([signal, stop.signal])
    : stop.signal;
  let session = initialSession;
  let iterator = session[Symbol.asyncIterator]();
  let closed = false;
  let reconnectTask: Promise<void> | undefined;

  const reconnect = (): Promise<void> => {
    reconnectTask ??= (async () => {
      await session.close().catch(() => undefined);
      let delayMs = 25;
      while (!reconnectSignal.aborted) {
        try {
          const replacement = await open(reconnectSignal);
          try {
            validateReplacementHello(expected, replacement.hello);
          } catch (error) {
            await replacement.close().catch(() => undefined);
            throw error;
          }
          session = replacement;
          iterator = replacement[Symbol.asyncIterator]();
          return;
        } catch (error) {
          if (!isRetryableDeliveryDisconnect(error)) throw error;
          await waitForReconnectDelay(delayMs, reconnectSignal);
          delayMs = Math.min(delayMs * 2, 5_000);
        }
      }
      throw reconnectSignal.reason ?? new DOMException("Aborted", "AbortError");
    })().finally(() => {
      reconnectTask = undefined;
    });
    return reconnectTask;
  };

  const next = async (): Promise<IteratorResult<TRecord>> => {
    while (!closed && !reconnectSignal.aborted) {
      try {
        const result = await iterator.next();
        if (!result.done) return result;
      } catch (error) {
        if (!isRetryableDeliveryDisconnect(error)) throw error;
      }
      await reconnect();
    }
    return { done: true, value: undefined };
  };

  const acknowledge = async (
    cursor: string,
    options?: { signal?: AbortSignal },
  ): Promise<DurableConsumer> => {
    try {
      return await session.acknowledge(cursor, options);
    } catch (error) {
      if (!isRetryableDeliveryDisconnect(error)) throw error;
      await reconnect();
      return session.acknowledge(cursor, options);
    }
  };

  return {
    next,
    acknowledge,
    close: async () => {
      if (closed) return;
      closed = true;
      stop.abort();
      await session.close();
      // A stopped reconnect rejects with the abort reason, which may be the
      // caller's own, of any type.
      await reconnectTask?.catch((error: unknown) => {
        if (!reconnectSignal.aborted) throw error;
      });
    },
  };
}

function validateReplacementHello<
  THello extends {
    streamId: string;
    storeEpoch: string;
    processor: ProcessorSummary;
  },
>(expected: THello, replacement: THello): void {
  if (
    replacement.streamId !== expected.streamId ||
    replacement.storeEpoch !== expected.storeEpoch ||
    replacement.processor.id !== expected.processor.id ||
    replacement.processor.version !== expected.processor.version ||
    replacement.processor.codeHash !== expected.processor.codeHash ||
    replacement.processor.configHash !== expected.processor.configHash ||
    replacement.processor.deliveryOrdering !==
      expected.processor.deliveryOrdering
  ) {
    throw streamProtocolError(
      "live",
      "replacement connection changed its durable lane identity",
    );
  }
}

/**
 * Classified by where the failure came from, never by its message: the SDK
 * reports every failure below HTTP as a retryable TransportError, and a
 * caller's abort, a stream record, or a local check is never retried.
 */
function isRetryableDeliveryDisconnect(error: unknown): boolean {
  return (
    error instanceof LeaniError &&
    (error.retryable ||
      error.code === "consumer_session_active" ||
      error.code === "consumer_session_lost")
  );
}

async function waitForReconnectDelay(
  delayMs: number,
  signal: AbortSignal,
): Promise<void> {
  if (signal.aborted) {
    throw signal.reason ?? new DOMException("Aborted", "AbortError");
  }
  await new Promise<void>((resolve, reject) => {
    const finish = () => {
      signal.removeEventListener("abort", abort);
      resolve();
    };
    const timeout = setTimeout(finish, delayMs);
    const abort = () => {
      clearTimeout(timeout);
      signal.removeEventListener("abort", abort);
      reject(signal.reason ?? new DOMException("Aborted", "AbortError"));
    };
    signal.addEventListener("abort", abort, { once: true });
  });
}

function iterateUnifiedLanes<T>(
  lanes: UnifiedLane<T>[],
  claim: (lanes: UnifiedLane<T>[]) => void,
): AsyncIterable<UnifiedDeliveryBatch<T>> {
  return {
    async *[Symbol.asyncIterator]() {
      claim(lanes);
      const ready: number[] = [];
      const settled = new Map<
        number,
        | { result: IteratorResult<UnifiedDeliveryBatch<T>>; error?: never }
        | { result?: never; error: unknown }
      >();
      let wake: (() => void) | undefined;
      let active = lanes.length;
      const schedule = (index: number): void => {
        lanes[index]!.next().then(
          (result) => {
            settled.set(index, { result });
            ready.push(index);
            wake?.();
            wake = undefined;
          },
          (error: unknown) => {
            settled.set(index, { error });
            ready.push(index);
            wake?.();
            wake = undefined;
          },
        );
      };
      for (let index = 0; index < lanes.length; index += 1) schedule(index);
      while (active > 0) {
        if (ready.length === 0) {
          await new Promise<void>((resolve) => {
            wake = resolve;
          });
        }
        const livePosition = ready.findIndex(
          (index) => lanes[index]!.kind === "live",
        );
        const [index] = ready.splice(livePosition >= 0 ? livePosition : 0, 1);
        const outcome = settled.get(index!);
        settled.delete(index!);
        if (!outcome) throw new Error("delivery scheduler lost a settled lane");
        if ("error" in outcome) throw outcome.error;
        if (outcome.result.done) {
          active -= 1;
          continue;
        }
        const completed = outcome.result.value.completion !== undefined;
        if (completed) {
          active -= 1;
        }
        yield outcome.result.value;
        if (!completed) {
          schedule(index!);
          await Promise.resolve();
        }
      }
    },
  };
}

async function openBackfillStream<T>(
  fetchImpl: FetchLike,
  baseUrl: URL,
  token: string | undefined,
  timeoutMs: number,
  subscription: string,
  consumer: string,
  credential: string | undefined,
  batching: BackfillBatchingRequest | undefined,
  signal: AbortSignal | undefined,
): Promise<BackfillDeliverySession<T>> {
  const session = await openDeliverySession<
    BackfillStreamHello,
    BackfillStreamRecord<T>
  >(
    fetchImpl,
    baseUrl,
    token,
    timeoutMs,
    backfillStreamPath(subscription, consumer, batching),
    consumerPath(subscription, consumer, "ack"),
    consumerPath(subscription, consumer, "lease"),
    credential,
    signal,
    "backfill",
  );
  const batches = async function* (): AsyncGenerator<BackfillStreamBatch<T>> {
    for await (const record of session) {
      if (record.type === "batch") {
        yield record;
      }
    }
  };
  // The stream ends once the completion is acknowledged, so iteration
  // continues past it rather than closing the session first.
  const deliveryBatches = async function* (): AsyncGenerator<
    HistoryDeliveryBatch<T>
  > {
    for await (const record of session) {
      if (record.type !== "heartbeat") {
        yield historyDeliveryBatch(session.hello.subscriptionId, record);
      }
    }
  };
  const events = async function* (): AsyncGenerator<ChangeEnvelope<T>> {
    for await (const batch of batches()) {
      yield* batch.changes;
    }
  };
  return Object.freeze({
    hello: session.hello,
    acknowledge: session.acknowledge.bind(session),
    close: session.close.bind(session),
    batches,
    deliveryBatches,
    events,
    [Symbol.asyncIterator]: session[Symbol.asyncIterator].bind(session),
  });
}

async function openLiveStream<T>(
  fetchImpl: FetchLike,
  baseUrl: URL,
  token: string | undefined,
  timeoutMs: number,
  processor: string,
  consumer: string,
  credential: string | undefined,
  signal: AbortSignal | undefined,
): Promise<LiveDeliverySession<T>> {
  return openDeliverySession<LiveStreamHello, LiveStreamRecord<T>>(
    fetchImpl,
    baseUrl,
    token,
    timeoutMs,
    liveConsumerPath(processor, consumer, "stream"),
    liveConsumerPath(processor, consumer, "ack"),
    liveConsumerPath(processor, consumer, "lease"),
    credential,
    signal,
    "live",
  );
}

async function openDeliverySession<THello, TRecord>(
  fetchImpl: FetchLike,
  baseUrl: URL,
  token: string | undefined,
  timeoutMs: number,
  streamPath: string,
  acknowledgePath: string,
  renewPath: string,
  credential: string | undefined,
  signal: AbortSignal | undefined,
  streamKind: "backfill" | "live",
): Promise<DeliverySession<THello, TRecord>> {
  const connection = new AbortController();
  // A connection that fails on its own, rather than through the caller's
  // abort or close(), keeps the failure for the iterator and acknowledge().
  let failure: unknown;
  const fail = (error: unknown): void => {
    if (connection.signal.aborted) {
      return;
    }
    failure = error;
    connection.abort(error);
  };
  const stopFromCaller = () => connection.abort(signal?.reason);
  signal?.addEventListener("abort", stopFromCaller, { once: true });
  if (signal?.aborted) {
    stopFromCaller();
  }
  // Until its hello arrives, the stream may stay silent as long as a JSON
  // request may take; afterwards, for three heartbeat intervals.
  let watchdog = idleWatchdog(timeoutMs, () =>
    fail(
      new TransportError(
        `${streamKind} stream sent no hello within ${timeoutMs} ms`,
      ),
    ));
  let records: AsyncGenerator<unknown> | undefined;
  let wireHello: SessionStreamHello & Record<string, unknown>;
  let sessionToken: string;
  let leaseTtlMs: number;
  let heartbeatIntervalMs: number;
  try {
    watchdog.arm();
    const response = await send(fetchImpl, resolveUnder(baseUrl, streamPath), {
      headers: requestHeaders(token, "application/x-ndjson", credential),
      signal: connection.signal,
    }, signal);
    if (!response.ok) {
      throw await responseError(response);
    }
    if (!response.body) {
      throw new BackfillStreamError({
        type: "error",
        code: "stream_body_missing",
        message: `${streamKind} stream response has no body`,
        earliestAvailableSequence: null,
        latestAvailableSequence: null,
      });
    }
    records = ndjsonRecords(response.body, connection.signal, () =>
      watchdog.arm());
    const first = await records.next();
    if (first.done) {
      throw streamProtocolError(streamKind, "stream ended before its hello record");
    }
    const firstType = (first.value as { type?: unknown }).type;
    if (firstType === "error" || firstType === "reset_required") {
      throw new BackfillStreamError(
        first.value as BackfillStreamFailure | BackfillStreamReset,
      );
    }
    if (firstType !== "hello") {
      throw streamProtocolError(streamKind, "first stream record is not hello");
    }
    wireHello = first.value as SessionStreamHello & Record<string, unknown>;
    sessionToken = assertIdentity(
      wireHello.sessionToken,
      "stream hello sessionToken",
    );
    leaseTtlMs = parseHelloDuration(wireHello.leaseTtlMs, "leaseTtlMs");
    heartbeatIntervalMs = parseHelloDuration(
      wireHello.heartbeatIntervalMs,
      "heartbeatIntervalMs",
    );
  } catch (error) {
    connection.abort();
    signal?.removeEventListener("abort", stopFromCaller);
    await records?.return(undefined).catch(() => undefined);
    throw failure ?? error;
  } finally {
    watchdog.disarm();
  }
  const stream = records;
  const { sessionToken: _sessionToken, ...publicHello } = wireHello;
  const idleMs = 3 * heartbeatIntervalMs;
  watchdog = idleWatchdog(idleMs, () =>
    fail(
      new TransportError(`${streamKind} stream sent nothing for ${idleMs} ms`),
    ));
  const renewal = new AbortController();
  const renewalTask = renewSessionUntilStopped(
    fetchImpl,
    baseUrl,
    token,
    timeoutMs,
    renewPath,
    credential,
    sessionToken,
    leaseTtlMs,
    renewal.signal,
  ).catch((error: unknown) => {
    // A lease that cannot be renewed ends the connection at once instead of
    // at the next record, which may never come.
    if (!renewal.signal.aborted) {
      fail(error);
    }
  });
  let iteratorClaimed = false;
  let closeTask: Promise<void> | undefined;
  const close = (): Promise<void> => {
    closeTask ??= (async () => {
      renewal.abort();
      watchdog.disarm();
      connection.abort();
      signal?.removeEventListener("abort", stopFromCaller);
      try {
        await stream.return(undefined);
      } catch (error) {
        if (!connection.signal.aborted) {
          throw error;
        }
      }
      await renewalTask;
      try {
        const released = await send(fetchImpl, resolveUnder(baseUrl, renewPath), {
          method: "DELETE",
          headers: markLeaniRequest(
            requestHeaders(token, "application/json", credential, sessionToken),
          ),
          signal: AbortSignal.timeout(timeoutMs),
        }, undefined);
        void released.body?.cancel().catch(() => undefined);
      } catch {
        // The lease also expires and is passively released when the stream
        // disconnects. Explicit release only makes clean reconnect immediate.
      }
    })();
    return closeTask;
  };
  const iterate = async function* (): AsyncGenerator<TRecord> {
    if (iteratorClaimed) {
      throw new TypeError("a delivery session can only be iterated once");
    }
    iteratorClaimed = true;
    try {
      while (true) {
        let next: IteratorResult<unknown>;
        // Only a pending read counts silence, not the time the application
        // spends on a record.
        watchdog.arm();
        try {
          next = await stream.next();
        } catch (error) {
          // The caller's abort and close() end the iteration quietly.
          if (failure === undefined && connection.signal.aborted) {
            return;
          }
          throw failure ?? error;
        } finally {
          watchdog.disarm();
        }
        if (failure !== undefined) {
          throw failure;
        }
        if (next.done) {
          return;
        }
        const record = next.value as
          | TRecord
          | BackfillStreamFailure
          | BackfillStreamReset;
        const recordType = (record as { type?: unknown }).type;
        if (recordType === "error" || recordType === "reset_required") {
          throw new BackfillStreamError(
            record as BackfillStreamFailure | BackfillStreamReset,
          );
        }
        if (recordType === "hello") {
          throw streamProtocolError(streamKind, "stream sent a second hello record");
        }
        yield record as TRecord;
      }
    } finally {
      await close();
    }
  };
  return Object.freeze({
    hello: publicHello as unknown as THello,
    acknowledge: async (
      cursor: string,
      acknowledgeOptions?: { signal?: AbortSignal },
    ): Promise<DurableConsumer> => {
      if (closeTask !== undefined) {
        throw new TypeError("delivery session is closed");
      }
      if (failure !== undefined) {
        throw failure;
      }
      const caller = acknowledgeOptions?.signal;
      const acknowledge = await send(
        fetchImpl,
        resolveUnder(baseUrl, acknowledgePath),
        {
          method: "POST",
          headers: requestHeaders(
            token,
            "application/json",
            credential,
            sessionToken,
          ),
          body: JSON.stringify({ cursor: assertIdentity(cursor, "cursor") }),
          signal: withDeadline(caller, timeoutMs),
        },
        caller,
      );
      if (!acknowledge.ok) {
        throw await responseError(acknowledge);
      }
      return readJson<DurableConsumer>(acknowledge, caller);
    },
    close,
    [Symbol.asyncIterator]: iterate,
  });
}

function streamProtocolError(
  streamKind: "backfill" | "live",
  message: string,
): BackfillStreamError {
  return new BackfillStreamError({
    type: "error",
    code: "invalid_stream_protocol",
    message: `${streamKind} ${message}`,
    earliestAvailableSequence: null,
    latestAvailableSequence: null,
  });
}

interface SessionStreamHello {
  type: "hello";
  sessionToken: string;
  leaseTtlMs: string;
  heartbeatIntervalMs: string;
}

async function renewSessionUntilStopped(
  fetchImpl: FetchLike,
  baseUrl: URL,
  token: string | undefined,
  timeoutMs: number,
  renewPath: string,
  credential: string | undefined,
  sessionToken: string,
  leaseTtlMs: number,
  stop: AbortSignal,
): Promise<void> {
  const intervalMs = Math.max(50, Math.floor(leaseTtlMs / 3));
  const firstRetryMs = Math.max(10, Math.floor(intervalMs / 10));
  // The node renewed the lease no earlier than the request that renewed it.
  let renewedAt = performance.now();
  let delayMs = intervalMs;
  let retryMs = firstRetryMs;
  while (!stop.aborted) {
    await abortableDelay(delayMs, stop);
    if (stop.aborted) {
      return;
    }
    const attemptedAt = performance.now();
    const leaseLeftMs = renewedAt + leaseTtlMs - attemptedAt;
    try {
      const response = await send(fetchImpl, resolveUnder(baseUrl, renewPath), {
        method: "POST",
        headers: markLeaniRequest(
          requestHeaders(token, "application/json", credential, sessionToken),
        ),
        // An answer after the lease lapsed could not keep it.
        signal: withDeadline(
          stop,
          Math.max(1, Math.min(timeoutMs, Math.floor(leaseLeftMs))),
        ),
      }, stop);
      if (!response.ok) {
        throw await responseError(response);
      }
      await readText(response, stop);
      renewedAt = attemptedAt;
      delayMs = intervalMs;
      retryMs = firstRetryMs;
    } catch (error) {
      // Retry a transient failure, with backoff, while the lease holds.
      const retryable = error instanceof LeaniError && error.retryable;
      if (
        stop.aborted ||
        !retryable ||
        renewedAt + leaseTtlMs - performance.now() <= retryMs
      ) {
        throw error;
      }
      delayMs = retryMs;
      retryMs *= 2;
    }
  }
}

function abortableDelay(delayMs: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const finish = () => {
      clearTimeout(timeout);
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const timeout = setTimeout(finish, delayMs);
    signal.addEventListener("abort", finish, { once: true });
  });
}

function parseHelloDuration(value: string, field: string): number {
  const milliseconds = Number(value);
  if (!Number.isSafeInteger(milliseconds) || milliseconds <= 0) {
    throw new TypeError(`stream hello contains an invalid ${field}`);
  }
  return milliseconds;
}

export async function* parseNdjson(
  body: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncGenerator<unknown> {
  yield* ndjsonRecords(body, signal);
}

/**
 * The largest NDJSON record the client buffers: the node's default buffer
 * for a batch's changes, 64 MiB, and room for the batch's own fields.
 */
const MAX_NDJSON_RECORD_BYTES = 65 * 1024 * 1024;

const LINE_FEED = 0x0a;

/**
 * Split NDJSON records at line feeds, scanning each byte once. A line feed is
 * ASCII, so a scan by byte never splits a UTF-8 character.
 */
async function* ndjsonRecords(
  body: ReadableStream<Uint8Array>,
  signal: AbortSignal | undefined,
  onChunk?: () => void,
): AsyncGenerator<unknown> {
  const decoder = new TextDecoder();
  // The bytes received so far of the record in progress.
  let pending: Uint8Array[] = [];
  let pendingBytes = 0;
  for await (const chunk of readChunks(body, signal, onChunk)) {
    let lineStart = 0;
    let newline = chunk.indexOf(LINE_FEED);
    while (newline >= 0) {
      const tail = chunk.subarray(lineStart, newline);
      if (pendingBytes + tail.length > MAX_NDJSON_RECORD_BYTES) {
        throw ndjsonRecordTooLarge();
      }
      const line = decoder
        .decode(concatBytes(pending, pendingBytes, tail))
        .trim();
      pending = [];
      pendingBytes = 0;
      lineStart = newline + 1;
      if (line) {
        yield JSON.parse(line) as unknown;
      }
      newline = chunk.indexOf(LINE_FEED, lineStart);
    }
    const rest = chunk.subarray(lineStart);
    if (rest.length > 0) {
      pendingBytes += rest.length;
      if (pendingBytes > MAX_NDJSON_RECORD_BYTES) {
        throw ndjsonRecordTooLarge();
      }
      pending.push(rest);
    }
  }
  if (signal?.aborted) {
    throw signal.reason ?? new DOMException("aborted", "AbortError");
  }
  const finalLine = decoder
    .decode(concatBytes(pending, pendingBytes, new Uint8Array()))
    .trim();
  if (finalLine) {
    yield JSON.parse(finalLine) as unknown;
  }
}

function ndjsonRecordTooLarge(): LeaniError {
  return new LeaniError(
    `NDJSON record exceeds ${MAX_NDJSON_RECORD_BYTES} bytes`,
    { status: 200, code: "invalid_response", retryable: false },
  );
}

function normalizeBaseUrl(value: string | URL): URL {
  const url = new URL(value);
  if (!url.pathname.endsWith("/")) {
    url.pathname += "/";
  }
  return url;
}

function requestHeaders(
  token: string | undefined,
  accept: string,
  credential?: string,
  sessionToken?: string,
): Headers {
  const headers = new Headers({ accept });
  if (token) {
    headers.set("authorization", `Bearer ${token}`);
  }
  if (credential !== undefined) {
    headers.set(
      "x-leani-consumer-credential",
      assertIdentity(credential, "credential"),
    );
  }
  if (sessionToken !== undefined) {
    headers.set(
      "x-leani-consumer-session",
      assertIdentity(sessionToken, "sessionToken"),
    );
  }
  if (accept === "application/json") {
    headers.set("content-type", "application/json");
  }
  return headers;
}

/** Mark a mutation without a JSON body, which the node otherwise refuses. */
function markLeaniRequest(headers: Headers): Headers {
  headers.set("x-leani-request", "1");
  return headers;
}

function consumerPath(
  subscription: string,
  consumer: string,
  operation: "changes" | "ack" | "lease" | "stream",
): string {
  return (
    `v1/backfill-subscriptions/${encodeURIComponent(assertIdentity(subscription, "subscription"))}` +
    `/consumers/${encodeURIComponent(assertIdentity(consumer, "consumer"))}/${operation}`
  );
}

function backfillStreamPath(
  subscription: string,
  consumer: string,
  batching: BackfillBatchingRequest | undefined,
): string {
  const path = consumerPath(subscription, consumer, "stream");
  if (batching === undefined) return path;
  const query = new URLSearchParams();
  for (const [field, value] of [
    ["targetEncodedBytes", batching.targetEncodedBytes],
    ["maximumEncodedBytes", batching.maximumEncodedBytes],
    ["maximumEvents", batching.maximumEvents],
    ["maximumProcessedBlocks", batching.maximumProcessedBlocks],
    ["maximumDelayMs", batching.maximumDelayMs],
  ] as const) {
    if (value !== undefined) {
      query.set(field, String(assertPositiveInteger(value, `batching.${field}`)));
    }
  }
  return query.size === 0 ? path : `${path}?${query}`;
}

function liveConsumerPath(
  processor: string,
  consumer: string,
  operation: "ack" | "lease" | "stream",
): string {
  return (
    `v1/processors/${encodeURIComponent(assertIdentity(processor, "processor"))}` +
    `/streams/live/consumers/${encodeURIComponent(assertIdentity(consumer, "consumer"))}/${operation}`
  );
}

function assertBlock(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${field} must be a non-negative safe integer`);
  }
  return value;
}

function assertUpperBound(
  value: number | "finalized",
  field: string,
): number | "finalized" {
  return value === "finalized" ? value : assertBlock(value, field);
}

function assertPositiveInteger(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new TypeError(`${field} must be a positive safe integer`);
  }
  return value;
}

function assertIdentity(value: string, field: string): string {
  if (!value.trim()) {
    throw new TypeError(`${field} must not be empty`);
  }
  return value;
}

function assertRatio(value: number, field: string): number {
  if (!Number.isFinite(value) || value <= 0 || value >= 1) {
    throw new TypeError(`${field} must be greater than zero and less than one`);
  }
  return value;
}
