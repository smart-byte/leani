import {
  LeaniError,
  createLeaniClient,
  isResetRequired,
  type ChangeEnvelope,
} from "@leani/sdk";

export interface DestinationTransaction<T> {
  /** Must be idempotent because a committed change can be redelivered. */
  apply(change: ChangeEnvelope<T>): Promise<void>;
  /** Store this beside application state in the same transaction. */
  storeLeaniCursor(cursor: string, sequence: string): Promise<void>;
}

export interface Destination<T> {
  transaction(work: (transaction: DestinationTransaction<T>) => Promise<void>): Promise<void>;
}

export interface ConsumerOptions {
  baseUrl: string;
  processor: string;
  consumer: string;
  credential: string;
  leaseTtlSeconds?: number;
  signal?: AbortSignal;
}

// docs:start durable-consumer
export async function consumeDurably<T>(
  destination: Destination<T>,
  options: ConsumerOptions,
): Promise<void> {
  const leani = createLeaniClient({ baseUrl: options.baseUrl });
  const consumers = leani.processors.consumers;

  try {
    await consumers.inspect(options.processor, options.consumer, {
      signal: options.signal,
    });
  } catch (error) {
    if (!(error instanceof LeaniError) || error.status !== 404) throw error;
    await consumers.create(options.processor, {
      id: options.consumer,
      role: "required",
      start: { position: "earliest_retained" },
      leaseTtlSeconds: options.leaseTtlSeconds ?? 60,
      credential: options.credential,
      signal: options.signal,
    });
  }

  while (!options.signal?.aborted) {
    const page = await consumers.changes<T>(
      options.processor,
      options.consumer,
      options.credential,
      { limit: 256, signal: options.signal },
    );

    if (page.data.length === 0) {
      await consumers.renew(
        options.processor,
        options.consumer,
        options.credential,
        { signal: options.signal },
      );
      await new Promise((resolve) => setTimeout(resolve, 500));
      continue;
    }

    for (const change of page.data) {
      if (isResetRequired(change)) {
        throw new Error("retained history no longer covers this consumer; rebuild from a snapshot");
      }

      await destination.transaction(async (transaction) => {
        await transaction.apply(change);
        await transaction.storeLeaniCursor(change.cursor, change.sequence);
      });

      // Acknowledge only after application state and its cursor commit together.
      await consumers.acknowledge(
        options.processor,
        options.consumer,
        options.credential,
        change.cursor,
        { signal: options.signal },
      );
    }
  }
}
// docs:end durable-consumer
