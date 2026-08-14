import type {
  ChangeEnvelope,
  DurableConsumer,
  FetchLike,
  ProcessorSummary,
} from "./index.ts";

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
  delete(
    subscription: string,
    options?: { signal?: AbortSignal },
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

  const requestJson = async <T>(
    method: "GET" | "POST" | "DELETE",
    path: string,
    body: unknown,
    signal: AbortSignal | undefined,
    credential?: string,
    sessionToken?: string,
  ): Promise<T> => {
    const response = await fetchImpl(new URL(path, baseUrl), {
      method,
      headers: requestHeaders(
        options.token,
        "application/json",
        credential,
        sessionToken,
      ),
      body: body === undefined ? undefined : JSON.stringify(body),
      signal,
    });
    if (!response.ok) {
      throw await responseError(response);
    }
    return (await response.json()) as T;
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
    delete: (
      subscription: string,
      deleteOptions?: { signal?: AbortSignal },
    ) =>
      requestJson<HistoricalWorkDeletion>(
        "DELETE",
        `admin/v1/backfill-subscriptions/${encodeURIComponent(assertIdentity(subscription, "subscription"))}`,
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
      const url = new URL(path, baseUrl);
      if (changeOptions.limit !== undefined) {
        url.searchParams.set(
          "limit",
          String(assertPositiveInteger(changeOptions.limit, "limit")),
        );
      }
      return fetchImpl(url, {
        headers: requestHeaders(
          options.token,
          "application/json",
          changeOptions.credential,
        ),
        signal: changeOptions.signal,
      }).then(async (response) => {
        if (!response.ok) {
          throw await responseError(response);
        }
        return (await response.json()) as {
          data: ChangeEnvelope<T>[];
          nextCursor: string | null;
          coverage: unknown;
        };
      });
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
        if (next.value.type === "backfill_complete") {
          complete = true;
          const record = next.value;
          return {
            done: false,
            value: {
              laneKind: "history",
              laneId: subscriptionId,
              streamId: record.streamId,
              ackCursor: record.cursor,
              events: [],
              completion: record,
              record,
            },
          };
        }
        const record = next.value;
        return {
          done: false,
          value: {
            laneKind: "history",
            laneId: subscriptionId,
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
      await reconnectTask?.catch((error: unknown) => {
        if (!isAbortError(error)) throw error;
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

function isRetryableDeliveryDisconnect(error: unknown): boolean {
  if (isAbortError(error) || error instanceof BackfillStreamError) return false;
  if (!(error instanceof Error)) return false;
  if (
    error.name === "consumer_session_active" ||
    error.name === "consumer_session_lost"
  ) {
    return true;
  }
  if (error.name === "NetworkError" || error.name === "TimeoutError") return true;
  return (
    error.name === "TypeError" &&
    /fetch|network|load failed|socket|connection/i.test(error.message)
  );
}

function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === "AbortError";
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
    events,
    [Symbol.asyncIterator]: session[Symbol.asyncIterator].bind(session),
  });
}

async function openLiveStream<T>(
  fetchImpl: FetchLike,
  baseUrl: URL,
  token: string | undefined,
  processor: string,
  consumer: string,
  credential: string | undefined,
  signal: AbortSignal | undefined,
): Promise<LiveDeliverySession<T>> {
  return openDeliverySession<LiveStreamHello, LiveStreamRecord<T>>(
    fetchImpl,
    baseUrl,
    token,
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
  streamPath: string,
  acknowledgePath: string,
  renewPath: string,
  credential: string | undefined,
  signal: AbortSignal | undefined,
  streamKind: "backfill" | "live",
): Promise<DeliverySession<THello, TRecord>> {
  const connectionStop = new AbortController();
  const stopFromCaller = () => connectionStop.abort(signal?.reason);
  if (signal?.aborted) {
    stopFromCaller();
  } else {
    signal?.addEventListener("abort", stopFromCaller, { once: true });
  }
  const response = await fetchImpl(new URL(streamPath, baseUrl), {
    headers: requestHeaders(token, "application/x-ndjson", credential),
    signal: connectionStop.signal,
  });
  if (!response.ok) {
    signal?.removeEventListener("abort", stopFromCaller);
    throw await responseError(response);
  }
  if (!response.body) {
    signal?.removeEventListener("abort", stopFromCaller);
    throw new BackfillStreamError({
      type: "error",
      code: "stream_body_missing",
      message: `${streamKind} stream response has no body`,
      earliestAvailableSequence: null,
      latestAvailableSequence: null,
    });
  }
  const records = parseNdjson(response.body, connectionStop.signal);
  const first = await records.next();
  if (first.done) {
    connectionStop.abort();
    signal?.removeEventListener("abort", stopFromCaller);
    throw streamProtocolError(streamKind, "stream ended before its hello record");
  }
  const firstType = (first.value as { type?: unknown }).type;
  if (firstType === "error" || firstType === "reset_required") {
    connectionStop.abort();
    signal?.removeEventListener("abort", stopFromCaller);
    throw new BackfillStreamError(
      first.value as BackfillStreamFailure | BackfillStreamReset,
    );
  }
  if (firstType !== "hello") {
    connectionStop.abort();
    signal?.removeEventListener("abort", stopFromCaller);
    throw streamProtocolError(streamKind, "first stream record is not hello");
  }
  const wireHello = first.value as SessionStreamHello &
    Record<string, unknown>;
  const sessionToken = assertIdentity(
    wireHello.sessionToken,
    "stream hello sessionToken",
  );
  const leaseTtlMs = parseLeaseTtl(wireHello.leaseTtlMs);
  const { sessionToken: _sessionToken, ...publicHello } = wireHello;
  const renewalStop = new AbortController();
  let renewalError: unknown;
  const renewalTask = renewSessionUntilStopped(
    fetchImpl,
    baseUrl,
    token,
    renewPath,
    credential,
    sessionToken,
    leaseTtlMs,
    renewalStop.signal,
  ).catch((error: unknown) => {
    if (!renewalStop.signal.aborted) {
      renewalError = error;
    }
  });
  let iteratorClaimed = false;
  let closeTask: Promise<void> | undefined;
  const close = (): Promise<void> => {
    closeTask ??= (async () => {
      renewalStop.abort();
      connectionStop.abort();
      signal?.removeEventListener("abort", stopFromCaller);
      try {
        await records.return(undefined);
      } catch (error) {
        if (!connectionStop.signal.aborted) {
          throw error;
        }
      }
      await renewalTask;
      try {
        await fetchImpl(new URL(renewPath, baseUrl), {
          method: "DELETE",
          headers: requestHeaders(
            token,
            "application/json",
            credential,
            sessionToken,
          ),
        });
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
        if (renewalError !== undefined) {
          throw renewalError;
        }
        const next = await records.next();
        if (next.done) {
          if (renewalError !== undefined) {
            throw renewalError;
          }
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
      if (renewalError !== undefined) {
        throw renewalError;
      }
      const acknowledge = await fetchImpl(
        new URL(acknowledgePath, baseUrl),
        {
          method: "POST",
          headers: requestHeaders(
            token,
            "application/json",
            credential,
            sessionToken,
          ),
          body: JSON.stringify({ cursor: assertIdentity(cursor, "cursor") }),
          signal: acknowledgeOptions?.signal,
        },
      );
      if (!acknowledge.ok) {
        throw await responseError(acknowledge);
      }
      return (await acknowledge.json()) as DurableConsumer;
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
}

async function renewSessionUntilStopped(
  fetchImpl: FetchLike,
  baseUrl: URL,
  token: string | undefined,
  renewPath: string,
  credential: string | undefined,
  sessionToken: string,
  leaseTtlMs: number,
  stop: AbortSignal,
): Promise<void> {
  const intervalMs = Math.max(50, Math.floor(leaseTtlMs / 3));
  while (!stop.aborted) {
    await abortableDelay(intervalMs, stop);
    if (stop.aborted) {
      return;
    }
    const response = await fetchImpl(new URL(renewPath, baseUrl), {
      method: "POST",
      headers: requestHeaders(
        token,
        "application/json",
        credential,
        sessionToken,
      ),
      signal: stop,
    });
    if (!response.ok) {
      throw await responseError(response);
    }
    await response.arrayBuffer();
  }
}

function abortableDelay(delayMs: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    const timeout = setTimeout(resolve, delayMs);
    signal.addEventListener(
      "abort",
      () => {
        clearTimeout(timeout);
        resolve();
      },
      { once: true },
    );
  });
}

function parseLeaseTtl(value: string): number {
  const ttl = Number(value);
  if (!Number.isSafeInteger(ttl) || ttl <= 0) {
    throw new TypeError("stream hello contains an invalid leaseTtlMs");
  }
  return ttl;
}

export async function* parseNdjson(
  body: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncGenerator<unknown> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  try {
    while (true) {
      if (signal?.aborted) {
        throw signal.reason ?? new DOMException("aborted", "AbortError");
      }
      const { done, value } = await reader.read();
      buffer += decoder.decode(value, { stream: !done });
      let newline = buffer.indexOf("\n");
      while (newline >= 0) {
        const line = buffer.slice(0, newline).trim();
        buffer = buffer.slice(newline + 1);
        if (line) {
          yield JSON.parse(line) as unknown;
        }
        newline = buffer.indexOf("\n");
      }
      if (done) {
        const finalLine = buffer.trim();
        if (finalLine) {
          yield JSON.parse(finalLine) as unknown;
        }
        return;
      }
    }
  } finally {
    reader.releaseLock();
  }
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

async function responseError(response: Response): Promise<Error> {
  let message = `Leani request failed with HTTP ${response.status}`;
  let code = "http_error";
  try {
    const body = (await response.json()) as {
      error?: { code?: string; message?: string };
    };
    code = body.error?.code ?? code;
    message = body.error?.message ?? message;
  } catch {
    // Preserve the stable HTTP fallback when the peer did not send JSON.
  }
  const error = new Error(message);
  error.name = code;
  return error;
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
