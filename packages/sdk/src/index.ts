import { LeaniError, responseError, errorFromBody } from "./errors.ts";
export { LeaniError, type LeaniErrorBody } from "./errors.ts";

export const SDK_VERSION = "0.1.0-rc.1" as const;

export type {
  components as OpenApiComponents,
  operations as OpenApiOperations,
  paths as OpenApiPaths,
} from "./openapi.generated.ts";

export type Hex = `0x${string}`;
export type Finality = "preview" | "included" | "finalized";
export type ChangeOperation =
  | "apply"
  | "undo"
  | "finalized"
  | "reset_required";

export interface LeaniClientOptions {
  baseUrl: string | URL;
  token?: string;
  timeoutMs?: number;
  fetch?: FetchLike;
  /**
   * Override typed query-extension mounts with canonical `basePath` values
   * returned by `capabilities()` when an ergonomic alias is unavailable.
   */
  queryBasePaths?: {
    blobs?: string;
    erc20?: string;
    uniswap?: string;
  };
  reconnect?: {
    initialDelayMs?: number;
    maxDelayMs?: number;
    jitter?: number;
  };
}

export type FetchLike = (
  input: RequestInfo | URL,
  init?: RequestInit,
) => Promise<Response>;

export interface ProcessorSummary {
  id: string;
  instance: string;
  version: string;
  codeHash: Hex;
  configHash: Hex;
  genericApi: "processor-v1";
  changeSchema: string;
  queryExtensions: QueryExtensionSummary[];
  subscriptions: boolean;
  artifactRetention: "none" | "window" | "full";
  deliveryOrdering: "canonical" | "block_versioned_idempotent";
}

export interface QueryExtensionSummary {
  id: string;
  basePath: string;
  aliasPath: string | null;
}

export interface QueryExtensionCapability extends QueryExtensionSummary {
  processor: string;
}

export interface Capabilities {
  chainId: number;
  ethereumRpc: Record<string, unknown>;
  processors: ProcessorSummary[];
  queryExtensions: QueryExtensionCapability[];
}

export interface NodeStatus {
  apiVersion: "1";
  project: string;
  version: string;
  chainId: number;
  uptimeSeconds: number;
  databaseBytes: number;
  processor: ProcessorSummary;
  processors: ProcessorSummary[];
  readiness: Readiness;
}

export interface Readiness {
  liveRequired: boolean;
  liveReady: boolean;
  finalityRequired: boolean;
  finalityReady: boolean;
  ready: boolean;
}

export interface Health {
  status: "live" | "ready" | "not_ready" | "failed";
  uptimeSeconds: number;
  database: "available" | "unavailable";
  readiness: Readiness;
  reasons: string[];
}

export interface CoverageInterval {
  fromBlock: number;
  toBlock: number;
  finality: Finality;
}

export interface ProcessorCoverage {
  chainId: number;
  chainFinalizedHead: {
    number: number;
    hash: string;
  } | null;
  requested?: {
    fromBlock: number;
    toBlock: number;
  };
  available: CoverageInterval[];
  configuredStartBlock: number;
  processedThrough: number | null;
  finalizedThrough: number | null;
  complete: boolean;
  state:
    | "starting"
    | "backfilling"
    | "catching_up"
    | "live"
    | "degraded"
    | "failed";
}

export interface Page<T> {
  data: T[];
  nextCursor: string | null;
  coverage: ProcessorCoverage;
}

export interface BlobsBlock {
  network: string;
  blockNumber: number;
  blockHash: Hex;
  parentHash: Hex;
  timestamp: number;
  size: string;
  blobCount: number;
  blobGasUsed: string;
  excessBlobGas: string;
  blobBaseFee: string;
  executionBaseFee: string;
  gasUsed: string;
  gasLimit: string;
  executionEthBurnedWei: string;
  blobEthBurnedWei: string;
  reserveFeeWei: string | null;
  transactionCount: number;
  targetBlobsPerBlock: number;
  maxBlobsPerBlock: number;
  transformVersion: number;
  finality: Finality;
}

export interface BlobTransaction {
  network: string;
  blockNumber: number;
  blockHash: Hex;
  txHash: Hex;
  transactionIndex: number;
  senderAddress: Hex;
  blobVersionedHashes: Hex[];
  blobCount: number;
  totalBurnedWei: string;
  executionBurnedWei: string;
  blobBurnedWei: string;
}

export interface BlobsSnapshotEntry {
  block: BlobsBlock;
  transactions: BlobTransaction[];
}

export interface ChangeHead {
  earliestSequence: string | null;
  latestSequence: string | null;
  cursor: string | null;
}

export interface GenericOutputEntity<T = unknown> {
  ordinal: string;
  key: Hex;
  schema: string;
  data: T;
  blockNumber: number;
  blockTimestamp: number;
  finality: Finality;
}

export interface OutputBounds {
  earliestBlock: number;
  latestBlock: number;
  earliestTimestamp: number;
  latestTimestamp: number;
}

export interface GenericSnapshotPage<T = unknown> {
  data: GenericOutputEntity<T>[];
  nextCursor: string | null;
  snapshotId: string;
  boundaryCursor: string | null;
  rowCount: string;
  valueBytes: string;
  expiresAtUnixMs: string;
  retainedBounds: OutputBounds | null;
  coverage: ProcessorCoverage;
  recovery: {
    follow: string | null;
    outsideRetention: "create_processor_instance_or_source_scan";
  };
}

export interface FollowSnapshotPage<T = unknown> extends GenericSnapshotPage<T> {
  boundaryCursor: string;
  recovery: GenericSnapshotPage<T>["recovery"] & { follow: string };
}

export interface OutputQueryOptions {
  fromBlock?: number;
  toBlock?: number;
  fromTimestamp?: number;
  toTimestamp?: number;
  limit?: number;
  cursor?: string;
  signal?: AbortSignal;
}

export type ConsumerRole = "required" | "best_effort";
export type ConsumerState = "active" | "reset_required" | "revoked";
export type ConsumerStart =
  | { position: "earliest_retained" }
  | { position: "current_head" }
  | { position: "cursor"; cursor: string };

export interface CreateConsumerOptions {
  id: string;
  role: ConsumerRole;
  start: ConsumerStart;
  leaseTtlSeconds: number;
  credential: string;
  signal?: AbortSignal;
}

export interface DurableConsumer {
  id: string;
  processorInstance: string;
  role: ConsumerRole;
  state: ConsumerState;
  acknowledgedSequence: string;
  deliveredSequence: string;
  acknowledgedCursor: string;
  deliveredCursor: string;
  leaseTtlMs: string;
  leaseExpiresAtUnixMs: string;
  leaseActive: boolean;
  lagChanges: string;
  lagBlocks: string;
  lagBytes: string;
  lagAgeMs: string;
  createdAtUnixMs: string;
  updatedAtUnixMs: string;
}

export interface BlobSchedule {
  name: string;
  activationBlock: number;
  activationTimestamp: number;
  forkId: Hex;
  targetBlobsPerBlock: number;
  maxBlobsPerBlock: number;
  baseFeeUpdateFraction: string;
  eip7918: boolean;
  source: "chain_spec" | "eth_config" | "configured";
}

export interface Erc20Balance {
  token: Hex;
  address: Hex;
  balance: string;
  asOfBlock: number;
  blockHash: Hex;
  finality: Finality;
  coverageFrom: number;
  complete: boolean;
  method: "erc20_transfer_ledger";
}

export interface EvmEvent {
  schemaVersion: number;
  contract: Hex;
  event: string;
  signature: string;
  blockNumber: number;
  blockHash: Hex;
  blockTimestamp: number;
  bucketStartTimestamp?: number;
  transactionHash: Hex;
  transactionIndex: number;
  logIndex: number;
  finality: Finality;
  values: Record<string, string>;
}

export interface TransactionStats {
  from: Hex;
  to: Hex;
  count: number;
  totalValueWei: string;
  asOfBlock: number;
  blockHash: Hex;
  finality: Finality;
  coverageFrom: number;
  complete: boolean;
}

export interface UniswapPoolPrice {
  pool: Hex;
  kind: "v2" | "v3";
  reserve0: string | null;
  reserve1: string | null;
  /** Signed token0 pool delta for a V3 swap, in raw token units. */
  amount0: string | null;
  /** Signed token1 pool delta for a V3 swap, in raw token units. */
  amount1: string | null;
  sqrtPriceX96: string | null;
  blockNumber: number;
  blockHash: Hex;
  logIndex: number;
  finality: Finality;
}

export interface LatestUniswapObservation {
  data: UniswapPoolPrice;
  timestamp: number;
}

export interface ConfiguredUniswapPool {
  address: Hex;
  kind: "v2" | "v3";
}

export interface ConfiguredUniswapPools {
  data: ConfiguredUniswapPool[];
}

export interface BlockSummary {
  chainId: number;
  blockNumber: number;
  blockHash: Hex;
  parentHash: Hex;
  timestamp: number;
  gasLimit: number | null;
  gasUsed: number | null;
  baseFeePerGas: string | null;
  blobGasUsed: number | null;
  excessBlobGas: number | null;
  transactionCount: number | null;
  sizeBytes: number | null;
  finality: Finality;
}

export interface LatestBlockSummary {
  data: BlockSummary;
}

export interface ChangeMetadata {
  apiVersion: "1";
  sequence: string;
  cursor: string;
  originKind:
    | "live"
    | "live_recovery"
    | "historical_backfill"
    | "recompute";
  originId: string;
  publicationRevision: string;
  chainId: number;
  block: {
    number: number;
    hash: Hex;
    parentHash: Hex;
    timestamp: number;
  } | null;
  finality: Finality;
  kind: string;
  schema: string;
  key: string | null;
  /** Present only on `reset_required` events, as the current coverage hint. */
  coverage?: ProcessorCoverage;
  /** RFC 3339 UTC emission time with millisecond precision. */
  emittedAt: string;
}

/** System transitions never pretend to contain a processor entity payload. */
export type ChangeEnvelope<T = unknown> = ChangeMetadata & (
  | { operation: "apply" | "undo"; data: T | null }
  | { operation: "finalized"; data: { throughBlock: number } }
  | { operation: "reset_required"; data: {
      earliestAvailableSequence: string;
      latestAvailableSequence: string;
      action: "query_snapshot_then_resume";
    } }
);

export interface LiveLaneReset {
  processor: string;
  state: "paused";
  reason: "operator_reset_pending_replay";
  firstUnappliedBlock: number;
  firstUnappliedHash: Hex;
  requiredDeliveryBytes: string;
}

/**
 * Connection-scoped stream handshake. Sent as the first frame of every
 * stream connection (including reconnects), synthesized at accept time,
 * never stored, and never acknowledged.
 */
export interface StreamHello {
  apiVersion: "1";
  chainId: number;
  processor: ProcessorSummary;
  coverage: ProcessorCoverage;
}

export class ResetRequiredError extends LeaniError {
  readonly change: ChangeEnvelope;

  constructor(change: ChangeEnvelope) {
    super("the durable stream cursor requires a snapshot reset", {
      status: 410,
      code: "cursor_expired",
      retryable: false,
      details: { cursor: change.cursor },
    });
    this.name = "ResetRequiredError";
    this.change = change;
  }
}

export interface ListBlocksOptions {
  fromBlock: number;
  toBlock: number;
  limit?: number;
  cursor?: string;
  allowPartial?: boolean;
  signal?: AbortSignal;
}

export interface ListTransactionsOptions {
  blockNumber: number;
  limit?: number;
  cursor?: string;
  signal?: AbortSignal;
}

export interface ChangesOptions {
  after?: string;
  limit?: number;
  signal?: AbortSignal;
}

export interface SubscribeOptions {
  after?: string;
  signal?: AbortSignal;
  /** Receives the connection handshake frame; called once per (re)connect. */
  onHello?: (hello: StreamHello) => void;
}

/** Select a configured instance when more than one processor has this kind. */
export interface ProcessorSubscribeOptions extends SubscribeOptions {
  processor?: string;
}

export interface LeaniClient {
  readonly baseUrl: URL;
  request<T>(
    path: string,
    params?: Record<string, QueryValue>,
    options?: { signal?: AbortSignal },
  ): Promise<T>;
  health: {
    live(options?: { signal?: AbortSignal }): Promise<Health>;
    ready(options?: { signal?: AbortSignal }): Promise<Health>;
  };
  status(options?: { signal?: AbortSignal }): Promise<NodeStatus>;
  capabilities(options?: { signal?: AbortSignal }): Promise<Capabilities>;
  processors: {
    list(options?: { signal?: AbortSignal }): Promise<ProcessorSummary[]>;
    status(
      id: string,
      options?: { signal?: AbortSignal },
    ): Promise<ProcessorCoverage>;
    changes<T = unknown>(
      id: string,
      options?: ChangesOptions,
    ): Promise<Page<ChangeEnvelope<T>>>;
    changeHead(
      id: string,
      options?: { signal?: AbortSignal },
    ): Promise<ChangeHead>;
    resetLiveLane(
      id: string,
      options?: { signal?: AbortSignal },
    ): Promise<LiveLaneReset>;
    listCollections(
      id: string,
      options?: { signal?: AbortSignal },
    ): Promise<{
      processor: ProcessorSummary;
      outputMode: "none" | "latest" | "window" | "full";
      retained: boolean;
      data: string[];
    }>;
    queryEntities<T = unknown>(
      id: string,
      collection: string,
      options?: OutputQueryOptions,
    ): Promise<GenericSnapshotPage<T>>;
    /** First snapshot page plus an atomic stream boundary. Read all nextCursor pages and releaseSnapshot before following. */
    queryAndFollow<T = unknown>(
      id: string,
      collection: string,
      options?: Omit<OutputQueryOptions, "cursor">,
    ): Promise<FollowSnapshotPage<T>>;
    getEntity<T = unknown>(
      id: string,
      collection: string,
      key: Hex,
      options?: { signal?: AbortSignal },
    ): Promise<GenericOutputEntity<T>>;
    releaseSnapshot(
      id: string,
      snapshotId: string,
      options?: { signal?: AbortSignal },
    ): Promise<void>;
    consumers: {
      list(
        id: string,
        options?: { signal?: AbortSignal },
      ): Promise<DurableConsumer[]>;
      create(
        id: string,
        options: CreateConsumerOptions,
      ): Promise<DurableConsumer>;
      inspect(
        id: string,
        consumer: string,
        options?: { signal?: AbortSignal },
      ): Promise<DurableConsumer>;
      revoke(
        id: string,
        consumer: string,
        options?: { signal?: AbortSignal },
      ): Promise<DurableConsumer>;
      renew(
        id: string,
        consumer: string,
        credential: string,
        options?: { signal?: AbortSignal },
      ): Promise<DurableConsumer>;
      changes<T = unknown>(
        id: string,
        consumer: string,
        credential: string,
        options?: { limit?: number; signal?: AbortSignal },
      ): Promise<Page<ChangeEnvelope<T>>>;
      acknowledge(
        id: string,
        consumer: string,
        credential: string,
        cursor: string,
        options?: { signal?: AbortSignal },
      ): Promise<DurableConsumer>;
    };
    subscribe<T = unknown>(
      id: string,
      options?: SubscribeOptions,
    ): AsyncGenerator<ChangeEnvelope<T>>;
  };
  blobs: {
    getBlock(
      number: number,
      options?: { signal?: AbortSignal },
    ): Promise<BlobsBlock>;
    listBlocks(options: ListBlocksOptions): Promise<Page<BlobsBlock>>;
    listSnapshot(
      options: ListBlocksOptions,
    ): Promise<Page<BlobsSnapshotEntry>>;
    getTransaction(
      hash: Hex,
      options?: { signal?: AbortSignal },
    ): Promise<BlobTransaction>;
    listTransactions(
      options: ListTransactionsOptions,
    ): Promise<Page<BlobTransaction>>;
    getSchedule(options?: {
      atBlock?: number;
      signal?: AbortSignal;
    }): Promise<BlobSchedule>;
    subscribe(
      options?: ProcessorSubscribeOptions,
    ): AsyncGenerator<ChangeEnvelope<BlobsSnapshotEntry>>;
  };
  erc20: {
    getBalance(
      token: Hex,
      address: Hex,
      options?: { signal?: AbortSignal },
    ): Promise<Erc20Balance>;
    subscribe(
      options?: ProcessorSubscribeOptions,
    ): AsyncGenerator<ChangeEnvelope<Erc20Balance>>;
  };
  uniswap: {
    getPool(
      address: Hex,
      options?: { signal?: AbortSignal },
    ): Promise<UniswapPoolPrice>;
    getLatestObservation(
      address: Hex,
      options?: { processor?: string; signal?: AbortSignal },
    ): Promise<LatestUniswapObservation>;
    listConfiguredPools(options?: {
      processor?: string;
      signal?: AbortSignal;
    }): Promise<ConfiguredUniswapPools>;
    subscribe(
      options?: ProcessorSubscribeOptions,
    ): AsyncGenerator<ChangeEnvelope<UniswapPoolPrice>>;
  };
  blocks: {
    getLatest(options?: {
      processor?: string;
      signal?: AbortSignal;
    }): Promise<LatestBlockSummary>;
    getByNumber(
      number: number,
      options?: { processor?: string; signal?: AbortSignal },
    ): Promise<LatestBlockSummary>;
    subscribe(
      options?: ProcessorSubscribeOptions,
    ): AsyncGenerator<ChangeEnvelope<BlockSummary>>;
  };
}

interface ResolvedOptions {
  baseUrl: URL;
  token?: string;
  timeoutMs: number;
  fetch: FetchLike;
  queryBasePaths: {
    blobs: string;
    erc20: string;
    uniswap: string;
  };
  reconnect: {
    initialDelayMs: number;
    maxDelayMs: number;
    jitter: number;
  };
}

/**
 * Create a Bun/Node/browser client for the native v1 query and durable change
 * API. Ethereum JSON-RPC remains a separate endpoint used directly by viem,
 * ethers, or another Ethereum library.
 */
export function createLeaniClient(
  options: LeaniClientOptions,
): LeaniClient {
  const resolved = resolveOptions(options);

  const get = <T>(
    path: string,
    query?: Record<string, QueryValue>,
    signal?: AbortSignal,
    extraHeaders?: Record<string, string>,
  ): Promise<T> =>
    requestJson<T>(
      resolved,
      "GET",
      path,
      query,
      undefined,
      signal,
      extraHeaders,
    );

  const post = <T>(
    path: string,
    body?: unknown,
    signal?: AbortSignal,
    extraHeaders?: Record<string, string>,
  ): Promise<T> =>
    requestJson<T>(
      resolved,
      "POST",
      path,
      undefined,
      body,
      signal,
      extraHeaders,
    );

  const remove = (
    path: string,
    signal?: AbortSignal,
  ): Promise<void> => requestVoid(resolved, "DELETE", path, signal);

  const removeJson = <T>(
    path: string,
    signal?: AbortSignal,
  ): Promise<T> =>
    requestJson<T>(
      resolved,
      "DELETE",
      path,
      undefined,
      undefined,
      signal,
    );

  const subscribe = <T>(
    processor: string,
    options: SubscribeOptions = {},
  ): AsyncGenerator<ChangeEnvelope<T>> =>
    subscribeProcessor<T>(resolved, processor, options);

  const request = <T>(
    path: string,
    params?: Record<string, QueryValue>,
    requestOptions?: { signal?: AbortSignal },
  ): Promise<T> => get<T>(path, params, requestOptions?.signal);

  return Object.freeze({
    baseUrl: resolved.baseUrl,
    request,
    health: Object.freeze({
      live: (options?: { signal?: AbortSignal }) =>
        get<Health>("health/live", undefined, options?.signal),
      ready: (options?: { signal?: AbortSignal }) =>
        get<Health>("health/ready", undefined, options?.signal),
    }),
    status: (options?: { signal?: AbortSignal }) =>
      get<NodeStatus>("v1/status", undefined, options?.signal),
    capabilities: (options?: { signal?: AbortSignal }) =>
      request<Capabilities>("v1/capabilities", undefined, options),
    processors: Object.freeze({
      list: async (options?: { signal?: AbortSignal }) => {
        const response = await get<{ data: ProcessorSummary[] }>(
          "v1/processors",
          undefined,
          options?.signal,
        );
        return response.data;
      },
      status: (id: string, options?: { signal?: AbortSignal }) =>
        get<ProcessorCoverage>(
          `v1/processors/${encodeURIComponent(id)}/status`,
          undefined,
          options?.signal,
        ),
      changes: <T = unknown>(
        id: string,
        changesOptions: ChangesOptions = {},
      ) =>
        get<Page<ChangeEnvelope<T>>>(
          `v1/processors/${encodeURIComponent(id)}/changes`,
          {
            after: changesOptions.after,
            limit: changesOptions.limit,
          },
          changesOptions.signal,
        ),
      changeHead: (
        id: string,
        options?: { signal?: AbortSignal },
      ) =>
        get<ChangeHead>(
          `v1/processors/${encodeURIComponent(id)}/changes/head`,
          undefined,
          options?.signal,
        ),
      resetLiveLane: (
        id: string,
        options?: { signal?: AbortSignal },
      ) =>
        post<LiveLaneReset>(
          `admin/v1/processors/${encodeURIComponent(id)}/lanes/live/reset`,
          undefined,
          options?.signal,
        ),
      listCollections: (
        id: string,
        options?: { signal?: AbortSignal },
      ) =>
        get<{
          processor: ProcessorSummary;
          outputMode: "none" | "latest" | "window" | "full";
          retained: boolean;
          data: string[];
        }>(
          `v1/processors/${encodeURIComponent(id)}/collections`,
          undefined,
          options?.signal,
        ),
      queryEntities: <T = unknown>(
        id: string,
        collection: string,
        queryOptions: OutputQueryOptions = {},
      ) =>
        get<GenericSnapshotPage<T>>(
          `v1/processors/${encodeURIComponent(id)}/collections/${encodeURIComponent(collection)}/entities`,
          outputQueryValues(queryOptions),
          queryOptions.signal,
        ),
      queryAndFollow: <T = unknown>(
        id: string,
        collection: string,
        queryOptions: Omit<OutputQueryOptions, "cursor"> = {},
      ) =>
        post<FollowSnapshotPage<T>>(
          `v1/processors/${encodeURIComponent(id)}/collections/${encodeURIComponent(collection)}/query-and-follow`,
          outputQueryBody(queryOptions),
          queryOptions.signal,
        ),
      getEntity: <T = unknown>(
        id: string,
        collection: string,
        key: Hex,
        options?: { signal?: AbortSignal },
      ) =>
        get<GenericOutputEntity<T>>(
          `v1/processors/${encodeURIComponent(id)}/collections/${encodeURIComponent(collection)}/entities/${encodeURIComponent(assertEntityKey(key))}`,
          undefined,
          options?.signal,
        ),
      releaseSnapshot: (
        id: string,
        snapshotId: string,
        options?: { signal?: AbortSignal },
      ) =>
        remove(
          `v1/processors/${encodeURIComponent(id)}/query-snapshots/${encodeURIComponent(snapshotId)}`,
          options?.signal,
        ),
      consumers: Object.freeze({
        list: async (
          id: string,
          options?: { signal?: AbortSignal },
        ) => {
          const response = await get<{ data: DurableConsumer[] }>(
            `v1/processors/${encodeURIComponent(id)}/consumers`,
            undefined,
            options?.signal,
          );
          return response.data;
        },
        create: (
          id: string,
          consumerOptions: CreateConsumerOptions,
        ) =>
          post<DurableConsumer>(
            `v1/processors/${encodeURIComponent(id)}/consumers`,
            {
              id: consumerOptions.id,
              role: consumerOptions.role,
              start: consumerOptions.start,
              leaseTtlSeconds: assertPositiveSafeInteger(
                consumerOptions.leaseTtlSeconds,
                "leaseTtlSeconds",
              ),
              credential: consumerOptions.credential,
            },
            consumerOptions.signal,
          ),
        inspect: (
          id: string,
          consumer: string,
          options?: { signal?: AbortSignal },
        ) =>
          get<DurableConsumer>(
            consumerPath(id, consumer),
            undefined,
            options?.signal,
          ),
        revoke: (
          id: string,
          consumer: string,
          options?: { signal?: AbortSignal },
        ) =>
          removeJson<DurableConsumer>(
            consumerPath(id, consumer),
            options?.signal,
          ),
        renew: (
          id: string,
          consumer: string,
          credential: string,
          options?: { signal?: AbortSignal },
        ) =>
          post<DurableConsumer>(
            `${consumerPath(id, consumer)}/lease`,
            undefined,
            options?.signal,
            consumerCredentialHeader(credential),
          ),
        changes: <T = unknown>(
          id: string,
          consumer: string,
          credential: string,
          options: { limit?: number; signal?: AbortSignal } = {},
        ) =>
          get<Page<ChangeEnvelope<T>>>(
            `${consumerPath(id, consumer)}/changes`,
            { limit: options.limit },
            options.signal,
            consumerCredentialHeader(credential),
          ),
        acknowledge: (
          id: string,
          consumer: string,
          credential: string,
          cursor: string,
          options?: { signal?: AbortSignal },
        ) =>
          post<DurableConsumer>(
            `${consumerPath(id, consumer)}/ack`,
            { cursor },
            options?.signal,
            consumerCredentialHeader(credential),
          ),
      }),
      subscribe,
    }),
    blobs: Object.freeze({
      getBlock: (number: number, options?: { signal?: AbortSignal }) =>
        request<BlobsBlock>(
          `${resolved.queryBasePaths.blobs}/blocks/${assertSafeInteger(number, "block number")}`,
          undefined,
          options,
        ),
      listBlocks: (listOptions: ListBlocksOptions) =>
        request<Page<BlobsBlock>>(
          `${resolved.queryBasePaths.blobs}/blocks`,
          {
            fromBlock: assertSafeInteger(
              listOptions.fromBlock,
              "fromBlock",
            ),
            toBlock: assertSafeInteger(listOptions.toBlock, "toBlock"),
            limit: listOptions.limit,
            cursor: listOptions.cursor,
            allowPartial: listOptions.allowPartial,
          },
          { signal: listOptions.signal },
        ),
      listSnapshot: (listOptions: ListBlocksOptions) =>
        request<Page<BlobsSnapshotEntry>>(
          `${resolved.queryBasePaths.blobs}/snapshot`,
          {
            fromBlock: assertSafeInteger(
              listOptions.fromBlock,
              "fromBlock",
            ),
            toBlock: assertSafeInteger(listOptions.toBlock, "toBlock"),
            limit: listOptions.limit,
            cursor: listOptions.cursor,
            allowPartial: listOptions.allowPartial,
          },
          { signal: listOptions.signal },
        ),
      getTransaction: (
        hash: Hex,
        options?: { signal?: AbortSignal },
      ) =>
        request<BlobTransaction>(
          `${resolved.queryBasePaths.blobs}/transactions/${encodeURIComponent(assertHash(hash))}`,
          undefined,
          options,
        ),
      listTransactions: (listOptions: ListTransactionsOptions) =>
        request<Page<BlobTransaction>>(
          `${resolved.queryBasePaths.blobs}/transactions`,
          {
            blockNumber: assertSafeInteger(
              listOptions.blockNumber,
              "blockNumber",
            ),
            limit: listOptions.limit,
            cursor: listOptions.cursor,
          },
          { signal: listOptions.signal },
        ),
      getSchedule: (
        scheduleOptions: {
          atBlock?: number;
          signal?: AbortSignal;
        } = {},
      ) =>
        request<BlobSchedule>(
          `${resolved.queryBasePaths.blobs}/schedule`,
          {
            atBlock:
              scheduleOptions.atBlock === undefined
                ? undefined
                : assertSafeInteger(scheduleOptions.atBlock, "atBlock"),
          },
          { signal: scheduleOptions.signal },
        ),
      subscribe: (subscribeOptions: ProcessorSubscribeOptions = {}) =>
        subscribe<BlobsSnapshotEntry>(
          subscribeOptions.processor ?? "blobs-money",
          subscribeOptions,
        ),
    }),
    erc20: Object.freeze({
      getBalance: (
        token: Hex,
        address: Hex,
        options?: { signal?: AbortSignal },
      ) =>
        request<Erc20Balance>(
          `${resolved.queryBasePaths.erc20}/balances/${encodeURIComponent(assertAddress(token))}/${encodeURIComponent(assertAddress(address))}`,
          undefined,
          options,
        ),
      subscribe: (subscribeOptions: ProcessorSubscribeOptions = {}) =>
        subscribe<Erc20Balance>(subscribeOptions.processor ?? "erc20-balances", subscribeOptions),
    }),
    uniswap: Object.freeze({
      getPool: (
        address: Hex,
        options?: { signal?: AbortSignal },
      ) =>
        request<UniswapPoolPrice>(
          `${resolved.queryBasePaths.uniswap}/pools/${encodeURIComponent(assertAddress(address))}`,
          undefined,
          options,
        ),
      getLatestObservation: (
        address: Hex,
        options: { processor?: string; signal?: AbortSignal } = {},
      ) =>
        request<LatestUniswapObservation>(
          `v1/processors/${encodeURIComponent(options.processor ?? "uniswap-observations")}/query/pools/${encodeURIComponent(assertAddress(address))}/latest`,
          undefined,
          { signal: options.signal },
        ),
      listConfiguredPools: (
        options: { processor?: string; signal?: AbortSignal } = {},
      ) =>
        request<ConfiguredUniswapPools>(
          `v1/processors/${encodeURIComponent(options.processor ?? "uniswap-observations")}/query/pools`,
          undefined,
          { signal: options.signal },
        ),
      subscribe: (subscribeOptions: ProcessorSubscribeOptions = {}) =>
        subscribe<UniswapPoolPrice>(subscribeOptions.processor ?? "uniswap-observations", subscribeOptions),
    }),
    blocks: Object.freeze({
      getLatest: (
        options: { processor?: string; signal?: AbortSignal } = {},
      ) =>
        request<LatestBlockSummary>(
          `v1/processors/${encodeURIComponent(options.processor ?? "block-summary")}/query/latest`,
          undefined,
          { signal: options.signal },
        ),
      getByNumber: (
        number: number,
        options: { processor?: string; signal?: AbortSignal } = {},
      ) =>
        request<LatestBlockSummary>(
          `v1/processors/${encodeURIComponent(options.processor ?? "block-summary")}/query/blocks/${assertSafeInteger(number, "block number")}`,
          undefined,
          { signal: options.signal },
        ),
      subscribe: (subscribeOptions: ProcessorSubscribeOptions = {}) =>
        subscribe<BlockSummary>(subscribeOptions.processor ?? "block-summary", subscribeOptions),
    }),
  });
}

function resolveOptions(options: LeaniClientOptions): ResolvedOptions {
  const baseUrl = new URL(options.baseUrl);
  baseUrl.pathname = `${baseUrl.pathname.replace(/\/+$/, "")}/`;
  const timeoutMs = options.timeoutMs ?? 15_000;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs <= 0) {
    throw new TypeError("timeoutMs must be a positive safe integer");
  }
  const jitter = options.reconnect?.jitter ?? 0.2;
  if (!Number.isFinite(jitter) || jitter < 0 || jitter > 1) {
    throw new TypeError("reconnect.jitter must be in 0..=1");
  }
  return {
    baseUrl,
    token: options.token,
    timeoutMs,
    fetch: options.fetch ?? globalThis.fetch.bind(globalThis),
    queryBasePaths: {
      blobs: normalizeQueryBasePath(
        options.queryBasePaths?.blobs,
        "v1/q/blobs",
      ),
      erc20: normalizeQueryBasePath(
        options.queryBasePaths?.erc20,
        "v1/q/erc20",
      ),
      uniswap: normalizeQueryBasePath(
        options.queryBasePaths?.uniswap,
        "v1/q/uniswap",
      ),
    },
    reconnect: {
      initialDelayMs: options.reconnect?.initialDelayMs ?? 250,
      maxDelayMs: options.reconnect?.maxDelayMs ?? 10_000,
      jitter,
    },
  };
}

function normalizeQueryBasePath(
  configured: string | undefined,
  fallback: string,
): string {
  const path = (configured ?? fallback).replace(/\/+$/g, "");
  if (
    path.length === 0 ||
    /^[a-z][a-z0-9+.-]*:/i.test(path) ||
    path.startsWith("//") ||
    path.includes("?") ||
    path.includes("#")
  ) {
    throw new TypeError("query base path must be a non-empty same-origin path");
  }
  return path;
}

export type QueryValue = string | number | boolean | undefined;

function buildUrl(
  options: ResolvedOptions,
  path: string,
  query?: Record<string, QueryValue>,
): URL {
  if (/^[a-z][a-z0-9+.-]*:/i.test(path) || path.startsWith("//")) {
    throw new TypeError("request path must not contain an origin or URL scheme");
  }
  const url = new URL(path, options.baseUrl);
  if (url.origin !== options.baseUrl.origin) {
    throw new TypeError("request path must resolve to the configured Leani origin");
  }
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value !== undefined) {
        url.searchParams.set(key, String(value));
      }
    }
  }
  return url;
}

async function requestJson<T>(
  options: ResolvedOptions,
  method: "GET" | "POST" | "DELETE",
  path: string,
  query?: Record<string, QueryValue>,
  body?: unknown,
  signal?: AbortSignal,
  extraHeaders?: Record<string, string>,
): Promise<T> {
  const deadline = AbortSignal.timeout(options.timeoutMs);
  const combined = signal
    ? AbortSignal.any([signal, deadline])
    : deadline;
  const response = await options.fetch(buildUrl(options, path, query), {
    method,
    headers: headers(
      options,
      "application/json",
      body === undefined
        ? extraHeaders
        : { "content-type": "application/json", ...extraHeaders },
    ),
    body: body === undefined ? undefined : JSON.stringify(body),
    signal: combined,
  });
  if (!response.ok) {
    throw await responseError(response);
  }
  return (await response.json()) as T;
}

async function requestVoid(
  options: ResolvedOptions,
  method: "DELETE",
  path: string,
  signal?: AbortSignal,
): Promise<void> {
  const deadline = AbortSignal.timeout(options.timeoutMs);
  const combined = signal
    ? AbortSignal.any([signal, deadline])
    : deadline;
  const response = await options.fetch(buildUrl(options, path), {
    method,
    headers: headers(options, "application/json"),
    signal: combined,
  });
  if (!response.ok) {
    throw await responseError(response);
  }
}

async function* subscribeProcessor<T>(
  options: ResolvedOptions,
  processor: string,
  subscribeOptions: SubscribeOptions,
): AsyncGenerator<ChangeEnvelope<T>> {
  let after = subscribeOptions.after;
  let lastSequence: string | undefined;
  let delayMs = options.reconnect.initialDelayMs;
  while (!subscribeOptions.signal?.aborted) {
    let response: Response;
    try {
      response = await options.fetch(
        buildUrl(
          options,
          `v1/processors/${encodeURIComponent(processor)}/stream`,
          { after },
        ),
        {
          headers: headers(options, "text/event-stream"),
          signal: subscribeOptions.signal,
        },
      );
    } catch (error) {
      if (subscribeOptions.signal?.aborted) {
        return;
      }
      await abortableDelay(
        jitter(delayMs, options.reconnect.jitter),
        subscribeOptions.signal,
      );
      delayMs = Math.min(delayMs * 2, options.reconnect.maxDelayMs);
      continue;
    }
    if (!response.ok) {
      const error = await responseError(response);
      if (!error.retryable) {
        throw error;
      }
      await abortableDelay(jitter(delayMs, options.reconnect.jitter), subscribeOptions.signal);
      delayMs = Math.min(delayMs * 2, options.reconnect.maxDelayMs);
      continue;
    }
    if (!response.body) {
      throw new LeaniError("stream response has no body", {
        status: response.status,
        code: "internal",
        retryable: true,
      });
    }
    delayMs = options.reconnect.initialDelayMs;
    try {
      for await (const message of parseSse(response.body, subscribeOptions.signal)) {
        if (!message.data) {
          continue;
        }
        if (message.event === "hello") {
          subscribeOptions.onHello?.(JSON.parse(message.data) as StreamHello);
          continue;
        }
        if (message.event === "error") {
          throw errorFromBody(200, JSON.parse(message.data));
        }
        const change = JSON.parse(message.data) as ChangeEnvelope<T>;
        validateChangeEnvelope(change);
        if (change.operation === "reset_required") {
          throw new ResetRequiredError(change as ChangeEnvelope);
        }

        if (
          lastSequence !== undefined &&
          compareSequences(change.sequence, lastSequence) <= 0
        ) {
          continue;
        }
        lastSequence = change.sequence;
        after = change.cursor;
        yield change;
      }
    } catch (error) {
      if (subscribeOptions.signal?.aborted) return;
      if (!(error instanceof StreamReadError)) throw error;
      // Resume only transport failures; malformed events and user callbacks
      // must fail visibly instead of entering an endless reconnect loop.
    }
    if (subscribeOptions.signal?.aborted) {
      break;
    }
    await abortableDelay(
      jitter(delayMs, options.reconnect.jitter),
      subscribeOptions.signal,
    );
    delayMs = Math.min(delayMs * 2, options.reconnect.maxDelayMs);
  }
}

interface SseMessage {
  id?: string;
  event?: string;
  data?: string;
}

class StreamReadError extends Error {}

export async function* parseSse(
  body: ReadableStream<Uint8Array>,
  signal?: AbortSignal,
): AsyncGenerator<SseMessage> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  const cancel = () => { void reader.cancel().catch(() => undefined); };
  signal?.addEventListener("abort", cancel, { once: true });
  if (signal?.aborted) cancel();
  try {
    while (true) {
      const { done, value } = await reader.read().catch((cause: unknown) => {
        throw new StreamReadError("SSE connection interrupted", { cause });
      });
      buffer += decoder.decode(value, { stream: !done });
      let boundary = findSseFrameBoundary(buffer, done);
      while (boundary) {
        if (signal?.aborted) return;
        const frame = buffer.slice(0, boundary.index);
        buffer = buffer.slice(boundary.index + boundary.length);
        const parsed = parseSseFrame(frame);
        if (parsed) {
          yield parsed;
        }
        boundary = findSseFrameBoundary(buffer, done);
      }
      if (done) {
        // Only a blank line completes an SSE event. Discard a partial final
        // frame so reconnect resumes from the last fully delivered cursor.
        break;
      }
    }
  } finally {
    signal?.removeEventListener("abort", cancel);
    await reader.cancel().catch(() => undefined);
    reader.releaseLock();
  }
}

function findSseFrameBoundary(
  buffer: string,
  endOfStream: boolean,
): { index: number; length: number } | null {
  for (let index = 0; index < buffer.length; index += 1) {
    const firstLength = sseLineEndingLength(buffer, index, endOfStream);
    if (firstLength === 0) {
      continue;
    }
    const secondLength = sseLineEndingLength(
      buffer,
      index + firstLength,
      endOfStream,
    );
    if (secondLength > 0) {
      return { index, length: firstLength + secondLength };
    }
    index += firstLength - 1;
  }
  return null;
}

function sseLineEndingLength(
  buffer: string,
  index: number,
  endOfStream: boolean,
): number {
  const character = buffer[index];
  if (character === "\n") {
    return 1;
  }
  if (character !== "\r") {
    return 0;
  }
  if (buffer[index + 1] === "\n") {
    return 2;
  }
  if (index + 1 < buffer.length || endOfStream) {
    return 1;
  }
  return 0;
}

function parseSseFrame(frame: string): SseMessage | undefined {
  const message: SseMessage = {};
  const data: string[] = [];
  for (const line of frame.split(/\r\n|\r|\n/)) {
    if (line.length === 0 || line.startsWith(":")) {
      continue;
    }
    const separator = line.indexOf(":");
    const field = separator < 0 ? line : line.slice(0, separator);
    let value = separator < 0 ? "" : line.slice(separator + 1);
    if (value.startsWith(" ")) {
      value = value.slice(1);
    }
    if (field === "id") {
      message.id = value;
    } else if (field === "event") {
      message.event = value;
    } else if (field === "data") {
      data.push(value);
    }
  }
  if (data.length > 0) {
    message.data = data.join("\n");
  }
  return Object.keys(message).length > 0 ? message : undefined;
}

function headers(
  options: ResolvedOptions,
  accept: string,
  extra?: Record<string, string>,
): Record<string, string> {
  const result: Record<string, string> = { Accept: accept, ...extra };
  if (options.token) {
    result.Authorization = `Bearer ${options.token}`;
  }
  return result;
}

function validateChangeEnvelope(value: ChangeEnvelope): void {
  if (
    !value ||
    value.apiVersion !== "1" ||
    !/^\d+$/.test(value.sequence) ||
    typeof value.cursor !== "string" ||
    value.cursor.length === 0 ||
    !["apply", "undo", "finalized", "reset_required"].includes(value.operation) ||
    typeof value.schema !== "string" ||
    value.schema.length === 0
  ) {
    throw new LeaniError("invalid change envelope", {
      status: 200,
      code: "invalid_response",
      retryable: false,
    });
  }
}

export function compareSequences(left: string, right: string): -1 | 0 | 1 {
  if (!/^\d+$/.test(left) || !/^\d+$/.test(right)) {
    throw new TypeError("sequences must be unsigned decimal strings");
  }
  const comparison = BigInt(left) - BigInt(right);
  return comparison < 0n ? -1 : comparison > 0n ? 1 : 0;
}

export function isResetRequired(
  change: ChangeEnvelope,
): change is ChangeEnvelope & { operation: "reset_required" } {
  return change.operation === "reset_required";
}

export interface EntityChangeTarget<T> {
  put(key: string, value: T): void | Promise<void>;
  delete(key: string): void | Promise<void>;
}

export async function applyEntityChange<T>(
  target: EntityChangeTarget<T>,
  change: ChangeEnvelope<T>,
): Promise<void> {
  if ((change.operation !== "apply" && change.operation !== "undo") || !change.key) {
    return;
  }
  if (change.data === null || change.kind.endsWith(".delete")) {
    await target.delete(change.key);
  } else {
    await target.put(change.key, change.data);
  }
}

/**
 * Commit destination state before constructing the acknowledgement request.
 * If the destination commit rejects, `acknowledge` is never invoked and the
 * durable consumer safely receives the same changes again.
 */
export async function commitThenAcknowledge<T>(
  destinationCommit: () => Promise<T>,
  acknowledge: (committed: T) => Promise<unknown>,
): Promise<T> {
  const committed = await destinationCommit();
  await acknowledge(committed);
  return committed;
}

export interface CursorStore {
  get(): string | undefined;
  set(cursor: string): void;
  clear(): void;
}

export function createInMemoryCursorStore(initial?: string): CursorStore {
  let cursor = initial;
  return {
    get: () => cursor,
    set: (value: string) => {
      cursor = value;
    },
    clear: () => {
      cursor = undefined;
    },
  };
}

function assertSafeInteger(value: number, field: string): number {
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError(`${field} must be a non-negative safe integer`);
  }
  return value;
}

function assertPositiveSafeInteger(value: number, field: string): number {
  const result = assertSafeInteger(value, field);
  if (result === 0) {
    throw new TypeError(`${field} must be positive`);
  }
  return result;
}

function optionalSafeInteger(
  value: number | undefined,
  field: string,
): number | undefined {
  return value === undefined ? undefined : assertSafeInteger(value, field);
}

function outputQueryBody(
  options: Omit<OutputQueryOptions, "cursor">,
): Record<string, number | undefined> {
  return {
    fromBlock: optionalSafeInteger(options.fromBlock, "fromBlock"),
    toBlock: optionalSafeInteger(options.toBlock, "toBlock"),
    fromTimestamp: optionalSafeInteger(
      options.fromTimestamp,
      "fromTimestamp",
    ),
    toTimestamp: optionalSafeInteger(options.toTimestamp, "toTimestamp"),
    limit:
      options.limit === undefined
        ? undefined
        : assertPositiveSafeInteger(options.limit, "limit"),
  };
}

function outputQueryValues(
  options: OutputQueryOptions,
): Record<string, QueryValue> {
  return {
    ...outputQueryBody(options),
    cursor: options.cursor,
  };
}

function consumerPath(processor: string, consumer: string): string {
  return `v1/processors/${encodeURIComponent(processor)}/consumers/${encodeURIComponent(consumer)}`;
}

function consumerCredentialHeader(
  credential: string,
): Record<string, string> {
  if (credential.length === 0) {
    throw new TypeError("consumer credential must not be empty");
  }
  return { "x-leani-consumer-credential": credential };
}

function assertEntityKey(value: string): Hex {
  if (!/^0x(?:[0-9a-fA-F]{2})*$/.test(value)) {
    throw new TypeError("entity key must contain complete hexadecimal bytes");
  }
  return value as Hex;
}

function assertHash(value: string): Hex {
  if (!/^0x[0-9a-fA-F]{64}$/.test(value)) {
    throw new TypeError("transaction hash must contain exactly 32 bytes");
  }
  return value as Hex;
}

function assertAddress(value: string): Hex {
  if (!/^0x[0-9a-fA-F]{40}$/.test(value)) {
    throw new TypeError("address must contain exactly 20 bytes");
  }
  return value as Hex;
}

function jitter(delayMs: number, amount: number): number {
  const factor = 1 - amount + Math.random() * amount * 2;
  return Math.max(0, Math.round(delayMs * factor));
}

async function abortableDelay(
  milliseconds: number,
  signal?: AbortSignal,
): Promise<void> {
  if (signal?.aborted) {
    return;
  }
  await new Promise<void>((resolve) => {
    const finish = () => {
      clearTimeout(timeout);
      signal?.removeEventListener("abort", finish);
      resolve();
    };
    const timeout = setTimeout(finish, milliseconds);
    signal?.addEventListener("abort", finish, { once: true });
  });
}
