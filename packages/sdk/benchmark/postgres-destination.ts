import { SQL } from "bun";

import {
  createBackfillSubscriptionClient,
  type BackfillStreamBatch,
  type LiveStreamBatch,
} from "../src/backfill.ts";
import type { ChangeEnvelope } from "../src/index.ts";

interface DestinationRow {
  [key: string]: unknown;
  [key: number]: unknown;
  [key: symbol]: unknown;
  run_id: string;
  sequence: string;
  block_number: number;
  kind: string;
  key_hex: string;
  payload_hex: string;
}

type DestinationSchema = "generic_event_log" | "blobs_application";

interface BlobsBlockRow {
  [key: string]: unknown;
  [key: number]: unknown;
  [key: symbol]: unknown;
  run_id: string;
  network: string;
  block_number: number;
  block_hash: string;
  timestamp: string;
  size: string;
  blob_count: number;
  blob_gas_used: string;
  excess_blob_gas: string;
  blob_base_fee: string;
  execution_base_fee: string;
  gas_used: string;
  gas_limit: string;
  eth_burned_execution: string;
  blob_eth_burned: string;
  reserve_fee: string | null;
  transaction_count: number;
  target_blobs_per_block: number;
  max_blobs_per_block: number;
  transform_version: number;
}

interface BlobsTransactionRow {
  [key: string]: unknown;
  [key: number]: unknown;
  [key: symbol]: unknown;
  run_id: string;
  network: string;
  block_number: number;
  tx_hash: string;
  sender_address: string;
  blob_count: number;
  eth_burned: string;
  execution_eth_burned: string;
  blob_eth_burned: string;
}

interface BlobsBatchRows {
  blocks: BlobsBlockRow[];
  transactions: BlobsTransactionRow[];
}

interface Options {
  baseUrl: string;
  subscription: string;
  processor: string;
  consumer: string;
  runId: string;
  consumerDelayMs: number;
  consumerReconnectEveryBatches: number;
  consumerDropAckResponseOnce: boolean;
  concurrentLiveBlocks: number;
  expectedLiveEvents: number;
  destinationSchema: DestinationSchema;
  expectedDestinationRows: number;
  benchmarkStartedUnixMs: number;
}

const options = parseOptions(Bun.argv.slice(2));
const postgresUrl = process.env.LEANI_BENCHMARK_POSTGRES_URL;
if (!postgresUrl) {
  throw new TypeError(
    "LEANI_BENCHMARK_POSTGRES_URL is required for sdk-postgres",
  );
}

const sql = new SQL(postgresUrl, { max: 2 });
let droppedAckResponse = false;
const client = createBackfillSubscriptionClient({
  baseUrl: options.baseUrl,
  fetch: async (input, init) => {
    const request = new Request(input, init);
    const response = await fetch(request);
    if (
      options.consumerDropAckResponseOnce &&
      !droppedAckResponse &&
      request.method === "POST" &&
      request.url.endsWith("/ack") &&
      response.ok
    ) {
      droppedAckResponse = true;
      simulatedAckResponseLosses += 1;
      consumerReconnects += 1;
      throw new TypeError(
        "fetch failed after the server applied the acknowledgement",
      );
    }
    return response;
  },
});
const hash = new Bun.CryptoHasher("sha256");
const liveHash = new Bun.CryptoHasher("sha256");
let batches = 0;
let processedBlocks = 0;
let events = 0;
let rawPayloadBytes = 0;
let uncompressedEncodedBytes = 0;
let transmittedBytes = 0;
let acknowledgements = 0;
let consumerReconnects = 0;
let simulatedAckResponseLosses = 0;
let destinationTransactions = 0;
let destinationTransactionMs = 0;
let completionSequence: string | null = null;
let liveBatches = 0;
let liveProcessedBlocks = 0;
let liveEvents = 0;
let liveRawPayloadBytes = 0;
let liveAcknowledgements = 0;
let liveAcknowledgedSequence: string | null = null;
let encodedJsonBytes = 0;
let firstBatchMs: number | null = null;
let firstDestinationCommitMs: number | null = null;
let firstAcknowledgementMs: number | null = null;
let firstLiveBatchMs: number | null = null;
const batchSamples: Array<{
  processedBlocks: number;
  domainEvents: number;
  rawPayloadBytes: number;
  uncompressedEncodedBytes: number;
  transmittedBytes: number;
  encodedJsonBytes: number;
  destinationTransactionMs: number;
}> = [];
const benchmarkStartedUnixMs = options.benchmarkStartedUnixMs;
let stage = "prepare destination";

try {
  await prepareDestination(sql, options.destinationSchema);
  stage = "sample PostgreSQL WAL before delivery";
  const walBefore = await postgresWalBytes(sql);
  stage = "consume unified delivery lanes";
  let historyCompleted = false;
  while (!historyCompleted || liveEvents < options.expectedLiveEvents) {
    const delivery = await client.subscribe<unknown>({
      processor: options.processor,
      consumer: options.consumer,
      lanes: {
        live:
          options.expectedLiveEvents > 0 && liveEvents < options.expectedLiveEvents,
        history: historyCompleted
          ? []
          : [{ subscriptionId: options.subscription }],
      },
      view: "live_priority",
    });
    let reconnect = false;
    for await (const batch of delivery.batches()) {
      if (batch.laneKind === "live") {
        const record = batch.record;
        const batchProcessedBlocks = Number(record.processedBlockCount);
        if (batchProcessedBlocks !== 1) {
          throw new Error("SDK destination received an invalid live block count");
        }
        firstLiveBatchMs ??= elapsedSinceUnixMilliseconds(benchmarkStartedUnixMs);
        const rows = eventRows(laneRunId(options.runId, "live"), record);
        const applicationRows = blobsBatchRows(
          laneRunId(options.runId, "live"),
          record,
          options.destinationSchema,
        );
        const transactionStarted = performance.now();
        await persistBatch(
          sql,
          laneRunId(options.runId, "live"),
          record,
          liveEvents,
          rows,
          applicationRows,
          options.destinationSchema,
        );
        destinationTransactions += 1;
        destinationTransactionMs += elapsedMilliseconds(transactionStarted);
        for (const row of rows) {
          const payload = decodeHex(row.payload_hex);
          updateLengthPrefixed(liveHash, payload);
          liveRawPayloadBytes += payload.byteLength;
        }
        liveProcessedBlocks += batchProcessedBlocks;
        liveEvents += rows.length;
        liveBatches += 1;
        const acknowledged = await delivery.acknowledge(batch);
        liveAcknowledgements += 1;
        acknowledgements += 1;
        liveAcknowledgedSequence = acknowledged.acknowledgedSequence;
        if (liveEvents > options.expectedLiveEvents) {
          throw new Error("SDK destination received more live events than expected");
        }
        if (historyCompleted && liveEvents === options.expectedLiveEvents) {
          break;
        }
        if (
          options.consumerReconnectEveryBatches > 0 &&
          (batches + liveBatches) % options.consumerReconnectEveryBatches === 0
        ) {
          reconnect = true;
          break;
        }
        continue;
      }
      const record = batch.record;
      encodedJsonBytes += Buffer.byteLength(JSON.stringify(record)) + 1;
      if (record.type === "batch") {
        const batchJsonBytes = Buffer.byteLength(JSON.stringify(record)) + 1;
        const batchProcessedBlocks = Number(record.processedBlockCount);
        const batchUncompressedEncodedBytes = Number(
          record.uncompressedEncodedBytes,
        );
        const batchTransmittedBytes = Number(record.transmittedBytes);
        if (!Number.isSafeInteger(batchProcessedBlocks) || batchProcessedBlocks <= 0) {
          throw new Error("SDK destination received an invalid processed block count");
        }
        if (
          !Number.isSafeInteger(batchUncompressedEncodedBytes) ||
          batchUncompressedEncodedBytes < 0 ||
          !Number.isSafeInteger(batchTransmittedBytes) ||
          batchTransmittedBytes < 0
        ) {
          throw new Error("SDK destination received invalid delivery byte counts");
        }
        firstBatchMs ??= elapsedSinceUnixMilliseconds(benchmarkStartedUnixMs);
        const rows = eventRows(laneRunId(options.runId, "history"), record);
        const applicationRows = blobsBatchRows(
          laneRunId(options.runId, "history"),
          record,
          options.destinationSchema,
        );
        if (options.consumerDelayMs > 0) {
          await Bun.sleep(options.consumerDelayMs);
        }
        const transactionStarted = performance.now();
        await persistBatch(
          sql,
          laneRunId(options.runId, "history"),
          record,
          events,
          rows,
          applicationRows,
          options.destinationSchema,
        );
        destinationTransactions += 1;
        const transactionMs = elapsedMilliseconds(transactionStarted);
        destinationTransactionMs += transactionMs;
        firstDestinationCommitMs ??=
          elapsedSinceUnixMilliseconds(benchmarkStartedUnixMs);
        let batchRawPayloadBytes = 0;
        for (const row of rows) {
          const payload = decodeHex(row.payload_hex);
          updateLengthPrefixed(hash, payload);
          rawPayloadBytes += payload.byteLength;
          batchRawPayloadBytes += payload.byteLength;
        }
        processedBlocks += batchProcessedBlocks;
        events += rows.length;
        uncompressedEncodedBytes += batchUncompressedEncodedBytes;
        transmittedBytes += batchTransmittedBytes;
        batches += 1;
        batchSamples.push({
          processedBlocks: batchProcessedBlocks,
          domainEvents: rows.length,
          rawPayloadBytes: batchRawPayloadBytes,
          uncompressedEncodedBytes: batchUncompressedEncodedBytes,
          transmittedBytes: batchTransmittedBytes,
          encodedJsonBytes: batchJsonBytes,
          destinationTransactionMs: transactionMs,
        });
        await delivery.acknowledge(batch);
        acknowledgements += 1;
        firstAcknowledgementMs ??=
          elapsedSinceUnixMilliseconds(benchmarkStartedUnixMs);
        if (
          options.consumerReconnectEveryBatches > 0 &&
          (batches + liveBatches) % options.consumerReconnectEveryBatches === 0
        ) {
          reconnect = true;
          break;
        }
        continue;
      }

      await sql.begin(async (transaction) => {
        await transaction`
          UPDATE leani_benchmark_cursors
          SET cursor = ${record.cursor}, completed = TRUE,
              updated_at = clock_timestamp()
          WHERE run_id = ${laneRunId(options.runId, "history")}
        `;
      });
      destinationTransactions += 1;
      const acknowledged = await delivery.acknowledge(batch);
      acknowledgements += 1;
      completionSequence = acknowledged.acknowledgedSequence;
      historyCompleted = true;
      if (liveEvents >= options.expectedLiveEvents) break;
    }
    await delivery.close();
    if (reconnect) {
      consumerReconnects += 1;
    } else if (
      !historyCompleted ||
      liveEvents < options.expectedLiveEvents
    ) {
      throw new Error("SDK destination stream ended before completion");
    }
  }

  stage = "verify historical destination";
  const destination = await destinationState(
    sql,
    options.destinationSchema,
    laneRunId(options.runId, "history"),
  );
  if (
    !destination ||
    Number(destination.rows) !== options.expectedDestinationRows ||
    !destination.completed
  ) {
    throw new Error("PostgreSQL destination verification failed");
  }
  stage = "verify live destination";
  const liveDestination = await destinationState(
    sql,
    options.destinationSchema,
    laneRunId(options.runId, "live"),
  );
  if (
    options.concurrentLiveBlocks > 0 &&
    (!liveDestination ||
      (options.destinationSchema === "generic_event_log" &&
        Number(liveDestination.rows) !== liveEvents))
  ) {
    throw new Error("PostgreSQL live destination verification failed");
  }
  stage = "measure destination storage";
  const relation = await destinationStorage(sql, options.destinationSchema);
  stage = "sample PostgreSQL WAL after delivery";
  const walAfter = await postgresWalBytes(sql);
  stage = "emit destination report";
  process.stdout.write(
    `${JSON.stringify({
      batches,
      processedBlocks,
      domainEvents: events,
      progressBoundaries: batches,
      completionRecords: completionSequence === null ? 0 : 1,
      rawPayloadBytes,
      uncompressedEncodedBytes,
      transmittedBytes,
      encodedJsonBytes,
      acknowledgements,
      consumerReconnects,
      simulatedAckResponseLosses,
      prunedRecords: 0,
      completionSequence,
      destinationDigest: hash.digest("hex"),
      liveBatches,
      liveProcessedBlocks,
      liveDomainEvents: liveEvents,
      liveRawPayloadBytes,
      liveAcknowledgements,
      liveAcknowledgedSequence,
      liveDestinationDigest: liveHash.digest("hex"),
      timeToFirstLiveBatchMs: firstLiveBatchMs,
      destinationTransactions,
      destinationTransactionMs,
      timeToFirstBatchMs: firstBatchMs,
      timeToFirstDestinationCommitMs: firstDestinationCommitMs,
      timeToFirstAcknowledgementMs: firstAcknowledgementMs,
      destinationSchema:
        options.destinationSchema === "generic_event_log"
          ? "leani.benchmark.generic-event-log.v1"
          : "blobs-money.benchmark.application.v1",
      destinationRowSemantics:
        options.destinationSchema === "generic_event_log"
          ? "one_domain_event_per_row"
          : "one_block_row_plus_one_blob_transaction_row",
      destinationRows: Number(destination.rows),
      destinationThroughBlock: Number(destination.through_block),
      destinationTableBytes: relation.tableBytes,
      destinationIndexBytes: relation.indexBytes,
      destinationTotalBytes: relation.totalBytes,
      destinationWalWrittenBytes: Math.max(0, walAfter - walBefore),
      batchSamples,
    })}\n`,
  );
} catch (error) {
  console.error(`SDK PostgreSQL benchmark failed while attempting to ${stage}`);
  throw error;
} finally {
  await sql.close();
}

function eventRows(
  runId: string,
  batch: BackfillStreamBatch<unknown> | LiveStreamBatch<unknown>,
): DestinationRow[] {
  return batch.changes.flatMap((change) => {
    const payload = canonicalEvent(change);
    if (payload === null || change.key === null) {
      return [];
    }
    return [{
      run_id: runId,
      sequence: change.sequence,
      block_number: change.block?.number ?? 0,
      kind: change.kind,
      key_hex: change.key,
      payload_hex: Buffer.from(payload).toString("hex"),
    }];
  });
}

function blobsBatchRows(
  runId: string,
  batch: BackfillStreamBatch<unknown> | LiveStreamBatch<unknown>,
  schema: DestinationSchema,
): BlobsBatchRows {
  const output: BlobsBatchRows = { blocks: [], transactions: [] };
  if (schema !== "blobs_application") return output;
  for (const change of batch.changes) {
    if (change.kind !== "blobs.block.put") continue;
    const data = change.data as {
      block?: Record<string, unknown>;
      transactions?: Array<Record<string, unknown>>;
    } | null;
    if (!data?.block || !Array.isArray(data.transactions)) {
      throw new TypeError("blobs application change has invalid data");
    }
    const block = data.block;
    const network = requiredString(block, "network");
    const blockNumber = requiredNumber(block, "blockNumber");
    output.blocks.push({
      run_id: runId,
      network,
      block_number: blockNumber,
      block_hash: requiredString(block, "blockHash"),
      timestamp: requiredIntegerString(block, "timestamp"),
      size: requiredString(block, "size"),
      blob_count: requiredNumber(block, "blobCount"),
      blob_gas_used: requiredString(block, "blobGasUsed"),
      excess_blob_gas: requiredString(block, "excessBlobGas"),
      blob_base_fee: requiredString(block, "blobBaseFee"),
      execution_base_fee: requiredString(block, "executionBaseFee"),
      gas_used: requiredString(block, "gasUsed"),
      gas_limit: requiredString(block, "gasLimit"),
      eth_burned_execution: requiredString(block, "executionEthBurnedWei"),
      blob_eth_burned: requiredString(block, "blobEthBurnedWei"),
      reserve_fee: optionalString(block, "reserveFeeWei"),
      transaction_count: requiredNumber(block, "transactionCount"),
      target_blobs_per_block: requiredNumber(block, "targetBlobsPerBlock"),
      max_blobs_per_block: requiredNumber(block, "maxBlobsPerBlock"),
      transform_version: requiredNumber(block, "transformVersion"),
    });
    for (const transaction of data.transactions) {
      const transactionNetwork = requiredString(transaction, "network");
      const transactionBlock = requiredNumber(transaction, "blockNumber");
      if (transactionNetwork !== network || transactionBlock !== blockNumber) {
        throw new TypeError("blobs application transaction identity mismatch");
      }
      output.transactions.push({
        run_id: runId,
        network,
        block_number: blockNumber,
        tx_hash: requiredString(transaction, "txHash"),
        sender_address: requiredString(transaction, "senderAddress"),
        blob_count: requiredNumber(transaction, "blobCount"),
        eth_burned: requiredString(transaction, "totalBurnedWei"),
        execution_eth_burned: requiredString(transaction, "executionBurnedWei"),
        blob_eth_burned: requiredString(transaction, "blobBurnedWei"),
      });
    }
  }
  return output;
}

function requiredString(value: Record<string, unknown>, field: string): string {
  const fieldValue = value[field];
  if (typeof fieldValue !== "string") {
    throw new TypeError(`blobs application ${field} is not a string`);
  }
  return fieldValue;
}

function optionalString(
  value: Record<string, unknown>,
  field: string,
): string | null {
  const fieldValue = value[field];
  if (fieldValue === null) return null;
  if (typeof fieldValue !== "string") {
    throw new TypeError(`blobs application ${field} is not a nullable string`);
  }
  return fieldValue;
}

function requiredNumber(value: Record<string, unknown>, field: string): number {
  const fieldValue = value[field];
  if (!Number.isSafeInteger(fieldValue) || Number(fieldValue) < 0) {
    throw new TypeError(`blobs application ${field} is not a safe unsigned integer`);
  }
  return Number(fieldValue);
}

function requiredIntegerString(
  value: Record<string, unknown>,
  field: string,
): string {
  const fieldValue = value[field];
  if (typeof fieldValue === "string" && /^\d+$/.test(fieldValue)) {
    return fieldValue;
  }
  if (Number.isSafeInteger(fieldValue) && Number(fieldValue) >= 0) {
    return String(fieldValue);
  }
  throw new TypeError(`blobs application ${field} is not an unsigned integer`);
}

async function persistBatch(
  sql: SQL,
  runId: string,
  batch: BackfillStreamBatch<unknown> | LiveStreamBatch<unknown>,
  priorEvents: number,
  rows: DestinationRow[],
  applicationRows: BlobsBatchRows,
  schema: DestinationSchema,
): Promise<void> {
  await sql.begin(async (transaction) => {
    if (schema === "generic_event_log" && rows.length > 0) {
      await transaction`
        INSERT INTO leani_benchmark_events
          ${transaction(
            rows,
            "run_id",
            "sequence",
            "block_number",
            "kind",
            "key_hex",
            "payload_hex",
          )}
        ON CONFLICT (run_id, sequence) DO UPDATE SET
          block_number = EXCLUDED.block_number,
          kind = EXCLUDED.kind,
          key_hex = EXCLUDED.key_hex,
          payload_hex = EXCLUDED.payload_hex
      `;
    }
    if (schema === "blobs_application" && applicationRows.blocks.length > 0) {
      const blockNumbers = applicationRows.blocks.map((block) => block.block_number);
      await transaction`
        DELETE FROM leani_benchmark_blob_transactions
        WHERE run_id = ${runId} AND block_number IN ${transaction(blockNumbers)}
      `;
      await transaction`
        INSERT INTO leani_benchmark_blob_blocks
          ${transaction(applicationRows.blocks)}
        ON CONFLICT (run_id, network, block_number) DO UPDATE SET
          block_hash = EXCLUDED.block_hash,
          timestamp = EXCLUDED.timestamp,
          size = EXCLUDED.size,
          blob_count = EXCLUDED.blob_count,
          blob_gas_used = EXCLUDED.blob_gas_used,
          excess_blob_gas = EXCLUDED.excess_blob_gas,
          blob_base_fee = EXCLUDED.blob_base_fee,
          execution_base_fee = EXCLUDED.execution_base_fee,
          gas_used = EXCLUDED.gas_used,
          gas_limit = EXCLUDED.gas_limit,
          eth_burned_execution = EXCLUDED.eth_burned_execution,
          blob_eth_burned = EXCLUDED.blob_eth_burned,
          reserve_fee = EXCLUDED.reserve_fee,
          transaction_count = EXCLUDED.transaction_count,
          target_blobs_per_block = EXCLUDED.target_blobs_per_block,
          max_blobs_per_block = EXCLUDED.max_blobs_per_block,
          transform_version = EXCLUDED.transform_version
      `;
      if (applicationRows.transactions.length > 0) {
        await transaction`
          INSERT INTO leani_benchmark_blob_transactions
            ${transaction(applicationRows.transactions)}
          ON CONFLICT (run_id, network, tx_hash) DO UPDATE SET
            block_number = EXCLUDED.block_number,
            sender_address = EXCLUDED.sender_address,
            blob_count = EXCLUDED.blob_count,
            eth_burned = EXCLUDED.eth_burned,
            execution_eth_burned = EXCLUDED.execution_eth_burned,
            blob_eth_burned = EXCLUDED.blob_eth_burned
        `;
      }
    }
    await transaction`
      INSERT INTO leani_benchmark_cursors
        (run_id, cursor, through_block, domain_events, updated_at)
      VALUES
        (${runId}, ${batch.acknowledgeableCursor}, ${batch.throughBlock},
         ${priorEvents + rows.length}, clock_timestamp())
      ON CONFLICT (run_id) DO UPDATE SET
        cursor = EXCLUDED.cursor,
        through_block = EXCLUDED.through_block,
        domain_events = EXCLUDED.domain_events,
        updated_at = EXCLUDED.updated_at
    `;
  });
}

function laneRunId(runId: string, lane: "history" | "live"): string {
  return `${runId}:${lane}`;
}

function canonicalEvent(change: ChangeEnvelope<unknown>): Uint8Array | null {
  if (change.key === null) {
    return null;
  }
  const key = decodeHex(change.key);
  if (change.kind === "synthetic.counter.put") {
    const data = change.data as { encoding?: unknown; value?: unknown } | null;
    if (data?.encoding !== "hex" || typeof data.value !== "string") {
      throw new TypeError("counter change has no hex payload");
    }
    return concatenate(Uint8Array.of(0), key, decodeHex(data.value));
  }
  if (change.kind === "blobs.block.put") {
    const data = change.data as {
      block?: { blockHash?: unknown };
      transactions?: Array<{
        txHash?: unknown;
        blobCount?: unknown;
        blobVersionedHashes?: unknown;
      }>;
    } | null;
    if (
      typeof data?.block?.blockHash !== "string" ||
      !Array.isArray(data.transactions)
    ) {
      throw new TypeError("blobs change has invalid data");
    }
    const parts = [
      Uint8Array.of(1),
      key,
      decodeHex(data.block.blockHash),
      unsignedBytes(BigInt(data.transactions.length), 4),
    ];
    for (const transaction of data.transactions) {
      if (
        typeof transaction.txHash !== "string" ||
        typeof transaction.blobCount !== "number" ||
        !Array.isArray(transaction.blobVersionedHashes)
      ) {
        throw new TypeError("blobs transaction has invalid data");
      }
      parts.push(
        decodeHex(transaction.txHash),
        unsignedBytes(BigInt(transaction.blobCount), 4),
      );
      for (const hash of transaction.blobVersionedHashes) {
        if (typeof hash !== "string") {
          throw new TypeError("blob versioned hash is not a string");
        }
        parts.push(decodeHex(hash));
      }
    }
    return concatenate(...parts);
  }
  if (change.kind === "uniswap.price.observation.put") {
    const data = change.data as { sqrtPriceX96?: unknown } | null;
    if (typeof data?.sqrtPriceX96 !== "string") {
      throw new TypeError("Uniswap observation has no square-root price");
    }
    return concatenate(
      Uint8Array.of(2),
      key,
      unsignedBytes(BigInt(data.sqrtPriceX96), 32),
    );
  }
  return null;
}

function concatenate(...parts: Uint8Array[]): Uint8Array {
  const output = new Uint8Array(
    parts.reduce((total, part) => total + part.byteLength, 0),
  );
  let offset = 0;
  for (const part of parts) {
    output.set(part, offset);
    offset += part.byteLength;
  }
  return output;
}

function unsignedBytes(value: bigint, width: number): Uint8Array {
  if (value < 0n || value >= 1n << BigInt(width * 8)) {
    throw new RangeError(`value does not fit in ${width} bytes`);
  }
  const output = new Uint8Array(width);
  for (let index = width - 1; index >= 0; index -= 1) {
    output[index] = Number(value & 0xffn);
    value >>= 8n;
  }
  return output;
}

async function prepareDestination(
  sql: SQL,
  schema: DestinationSchema,
): Promise<void> {
  await sql`
    CREATE TABLE IF NOT EXISTS leani_benchmark_events (
      run_id TEXT NOT NULL,
      sequence BIGINT NOT NULL,
      block_number BIGINT NOT NULL,
      kind TEXT NOT NULL,
      key_hex TEXT NOT NULL,
      payload_hex TEXT NOT NULL,
      PRIMARY KEY (run_id, sequence)
    )
  `;
  await sql`
    CREATE TABLE IF NOT EXISTS leani_benchmark_cursors (
      run_id TEXT PRIMARY KEY,
      cursor TEXT NOT NULL,
      through_block BIGINT NOT NULL,
      domain_events BIGINT NOT NULL,
      completed BOOLEAN NOT NULL DEFAULT FALSE,
      updated_at TIMESTAMPTZ NOT NULL
    )
  `;
  if (schema === "blobs_application") {
    await sql`
      CREATE TABLE IF NOT EXISTS leani_benchmark_blob_blocks (
        run_id TEXT NOT NULL,
        network VARCHAR(32) NOT NULL,
        block_number INTEGER NOT NULL,
        block_hash VARCHAR(66) NOT NULL,
        timestamp BIGINT NOT NULL,
        size TEXT NOT NULL,
        blob_count INTEGER NOT NULL,
        blob_gas_used TEXT NOT NULL,
        excess_blob_gas TEXT NOT NULL,
        blob_base_fee TEXT NOT NULL,
        execution_base_fee TEXT NOT NULL,
        gas_used TEXT NOT NULL,
        gas_limit TEXT NOT NULL,
        eth_burned_execution TEXT NOT NULL,
        blob_eth_burned TEXT NOT NULL,
        reserve_fee TEXT,
        transaction_count INTEGER NOT NULL,
        target_blobs_per_block INTEGER NOT NULL,
        max_blobs_per_block INTEGER NOT NULL,
        transform_version INTEGER NOT NULL,
        PRIMARY KEY (run_id, network, block_number)
      )
    `;
    await sql`
      CREATE INDEX IF NOT EXISTS leani_benchmark_blob_blocks_timestamp_idx
      ON leani_benchmark_blob_blocks (run_id, network, timestamp)
    `;
    await sql`
      CREATE TABLE IF NOT EXISTS leani_benchmark_blob_transactions (
        run_id TEXT NOT NULL,
        network VARCHAR(32) NOT NULL,
        block_number INTEGER NOT NULL,
        tx_hash VARCHAR(80) NOT NULL,
        sender_address VARCHAR(64) NOT NULL,
        blob_count INTEGER NOT NULL,
        eth_burned TEXT NOT NULL,
        execution_eth_burned TEXT NOT NULL,
        blob_eth_burned TEXT NOT NULL,
        PRIMARY KEY (run_id, network, tx_hash)
      )
    `;
    await sql`
      CREATE INDEX IF NOT EXISTS leani_benchmark_blob_transactions_block_idx
      ON leani_benchmark_blob_transactions (run_id, network, block_number)
    `;
    await sql`
      TRUNCATE leani_benchmark_blob_blocks,
               leani_benchmark_blob_transactions,
               leani_benchmark_cursors
    `;
  } else {
    await sql`TRUNCATE leani_benchmark_events, leani_benchmark_cursors`;
  }
}

async function destinationState(
  sql: SQL,
  schema: DestinationSchema,
  runId: string,
): Promise<{ rows: string; through_block: string; completed: boolean } | undefined> {
  if (schema === "generic_event_log") {
    const [state] = await sql<
      Array<{ rows: string; through_block: string; completed: boolean }>
    >`
      SELECT COUNT(events.sequence)::text AS rows,
             COALESCE(MAX(cursors.through_block), 0)::text AS through_block,
             COALESCE(BOOL_OR(cursors.completed), FALSE) AS completed
      FROM leani_benchmark_cursors AS cursors
      LEFT JOIN leani_benchmark_events AS events USING (run_id)
      WHERE cursors.run_id = ${runId}
    `;
    return state;
  }
  const [state] = await sql<
    Array<{ rows: string; through_block: string; completed: boolean }>
  >`
    SELECT ((SELECT COUNT(*)
             FROM leani_benchmark_blob_blocks
             WHERE run_id = ${runId}) +
            (SELECT COUNT(*)
             FROM leani_benchmark_blob_transactions
             WHERE run_id = ${runId}))::text AS rows,
           cursors.through_block::text AS through_block,
           cursors.completed
    FROM leani_benchmark_cursors AS cursors
    WHERE cursors.run_id = ${runId}
  `;
  return state;
}

async function destinationStorage(
  sql: SQL,
  schema: DestinationSchema,
): Promise<{
  tableBytes: number;
  indexBytes: number;
  totalBytes: number;
}> {
  const [sizes] = schema === "generic_event_log"
    ? await sql<
      Array<{ table_bytes: string; index_bytes: string; total_bytes: string }>
    >`
      SELECT
        (pg_relation_size('leani_benchmark_events') +
         pg_relation_size('leani_benchmark_cursors'))::text AS table_bytes,
        (pg_indexes_size('leani_benchmark_events') +
         pg_indexes_size('leani_benchmark_cursors'))::text AS index_bytes,
        (pg_total_relation_size('leani_benchmark_events') +
         pg_total_relation_size('leani_benchmark_cursors'))::text AS total_bytes
    `
    : await sql<
      Array<{ table_bytes: string; index_bytes: string; total_bytes: string }>
    >`
      SELECT
        (pg_relation_size('leani_benchmark_blob_blocks') +
         pg_relation_size('leani_benchmark_blob_transactions') +
         pg_relation_size('leani_benchmark_cursors'))::text AS table_bytes,
        (pg_indexes_size('leani_benchmark_blob_blocks') +
         pg_indexes_size('leani_benchmark_blob_transactions') +
         pg_indexes_size('leani_benchmark_cursors'))::text AS index_bytes,
        (pg_total_relation_size('leani_benchmark_blob_blocks') +
         pg_total_relation_size('leani_benchmark_blob_transactions') +
         pg_total_relation_size('leani_benchmark_cursors'))::text AS total_bytes
    `;
  return {
    tableBytes: Number(sizes?.table_bytes ?? 0),
    indexBytes: Number(sizes?.index_bytes ?? 0),
    totalBytes: Number(sizes?.total_bytes ?? 0),
  };
}

async function postgresWalBytes(sql: SQL): Promise<number> {
  const [wal] = await sql<Array<{ wal_bytes: string }>>`
    SELECT pg_wal_lsn_diff(pg_current_wal_insert_lsn(), '0/0')::text AS wal_bytes
  `;
  return Number(wal?.wal_bytes ?? 0);
}

function updateLengthPrefixed(
  hash: Bun.CryptoHasher,
  bytes: Uint8Array,
): void {
  const length = new Uint8Array(8);
  new DataView(length.buffer).setBigUint64(0, BigInt(bytes.byteLength));
  hash.update(length);
  hash.update(bytes);
}

function decodeHex(value: string): Uint8Array {
  const encoded = value.startsWith("0x") ? value.slice(2) : value;
  if (encoded.length % 2 !== 0 || !/^[0-9a-f]*$/i.test(encoded)) {
    throw new TypeError("destination received invalid hex");
  }
  return Uint8Array.fromHex(encoded);
}

function elapsedMilliseconds(started: number): number {
  return Math.max(0, Math.round(performance.now() - started));
}

function elapsedSinceUnixMilliseconds(started: number): number {
  return Math.max(0, Date.now() - started);
}

function parseOptions(arguments_: string[]): Options {
  const values = new Map<string, string>();
  for (let index = 0; index < arguments_.length; index += 2) {
    const name = arguments_[index];
    const value = arguments_[index + 1];
    if (!name?.startsWith("--") || value === undefined) {
      throw new TypeError("benchmark destination arguments must be --name value pairs");
    }
    values.set(name.slice(2), value);
  }
  const required = (name: string): string => {
    const value = values.get(name);
    if (!value) {
      throw new TypeError(`--${name} is required`);
    }
    return value;
  };
  const delay = Number(values.get("consumer-delay-ms") ?? "0");
  if (!Number.isSafeInteger(delay) || delay < 0 || delay > 60_000) {
    throw new TypeError("--consumer-delay-ms is invalid");
  }
  const reconnectEveryBatches = Number(
    values.get("consumer-reconnect-every-batches") ?? "0",
  );
  if (
    !Number.isSafeInteger(reconnectEveryBatches) ||
    reconnectEveryBatches < 0
  ) {
    throw new TypeError("--consumer-reconnect-every-batches is invalid");
  }
  const dropAckResponseOnce =
    values.get("consumer-drop-ack-response-once") ?? "false";
  if (dropAckResponseOnce !== "true" && dropAckResponseOnce !== "false") {
    throw new TypeError("--consumer-drop-ack-response-once is invalid");
  }
  const concurrentLiveBlocks = Number(
    values.get("concurrent-live-blocks") ?? "0",
  );
  if (
    !Number.isSafeInteger(concurrentLiveBlocks) ||
    concurrentLiveBlocks < 0 ||
    concurrentLiveBlocks > 100_000
  ) {
    throw new TypeError("--concurrent-live-blocks is invalid");
  }
  const expectedLiveEvents = Number(values.get("expected-live-events") ?? "0");
  if (!Number.isSafeInteger(expectedLiveEvents) || expectedLiveEvents < 0) {
    throw new TypeError("--expected-live-events is invalid");
  }
  const benchmarkStartedUnixMs = Number(required("benchmark-started-unix-ms"));
  if (!Number.isSafeInteger(benchmarkStartedUnixMs) || benchmarkStartedUnixMs <= 0) {
    throw new TypeError("--benchmark-started-unix-ms is invalid");
  }
  const destinationSchema = required("destination-schema");
  if (
    destinationSchema !== "generic_event_log" &&
    destinationSchema !== "blobs_application"
  ) {
    throw new TypeError("--destination-schema is invalid");
  }
  const expectedDestinationRows = Number(required("expected-destination-rows"));
  if (!Number.isSafeInteger(expectedDestinationRows) || expectedDestinationRows < 0) {
    throw new TypeError("--expected-destination-rows is invalid");
  }
  return {
    baseUrl: required("base-url"),
    subscription: required("subscription"),
    processor: required("processor"),
    consumer: required("consumer"),
    runId: required("run-id"),
    consumerDelayMs: delay,
    consumerReconnectEveryBatches: reconnectEveryBatches,
    consumerDropAckResponseOnce: dropAckResponseOnce === "true",
    concurrentLiveBlocks,
    expectedLiveEvents,
    destinationSchema,
    expectedDestinationRows,
    benchmarkStartedUnixMs,
  };
}
