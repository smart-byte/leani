import { SQL } from "bun";
import type { ChangeEnvelope } from "@leani/sdk";
import {
  consumeDurably,
  type Destination,
  type DestinationTransaction,
} from "../sdk-subscription/index.ts";

function required(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} is required`);
  return value;
}

// docs:start postgres-destination
class PostgresDestination implements Destination<unknown> {
  constructor(
    private readonly sql: SQL,
    private readonly processor: string,
    private readonly network: string,
  ) {}

  async migrate(): Promise<void> {
    await this.sql`
      create table if not exists leani_applied_changes (
        processor text not null,
        network text not null,
        sequence numeric(20, 0) not null,
        cursor text not null,
        operation text not null,
        kind text not null,
        entity_key text,
        payload jsonb,
        block_number bigint,
        primary key (processor, network, sequence)
      )
    `;
    await this.sql`
      create table if not exists leani_cursors (
        processor text not null,
        network text not null,
        cursor text not null,
        sequence numeric(20, 0) not null,
        updated_at timestamptz not null default now(),
        primary key (processor, network)
      )
    `;
  }

  async transaction(
    work: (transaction: DestinationTransaction<unknown>) => Promise<void>,
  ): Promise<void> {
    await this.sql.begin(async (sql) => {
      await work({
        apply: async (change: ChangeEnvelope<unknown>) => {
          await sql`
            insert into leani_applied_changes (
              processor, network, sequence, cursor, operation, kind,
              entity_key, payload, block_number
            ) values (
              ${this.processor}, ${this.network}, ${change.sequence},
              ${change.cursor}, ${change.operation}, ${change.kind},
              ${change.key}, ${JSON.stringify(change.data)}::jsonb,
              ${change.block?.number ?? null}
            )
            on conflict (processor, network, sequence) do nothing
          `;
        },
        storeLeaniCursor: async (cursor: string, sequence: string) => {
          await sql`
            insert into leani_cursors (
              processor, network, cursor, sequence, updated_at
            ) values (
              ${this.processor}, ${this.network}, ${cursor}, ${sequence}, now()
            )
            on conflict (processor, network) do update set
              cursor = excluded.cursor,
              sequence = excluded.sequence,
              updated_at = excluded.updated_at
            where leani_cursors.sequence <= excluded.sequence
          `;
        },
      });
    });
  }
}
// docs:end postgres-destination

const processor = process.env.LEANI_PROCESSOR?.trim() || "usdc-weth-latest";
const network = process.env.LEANI_NETWORK?.trim() || "mainnet";
const sql = new SQL(required("DATABASE_URL"));
const destination = new PostgresDestination(sql, processor, network);

await destination.migrate();
console.log(`Consuming ${processor} into PostgreSQL; press Ctrl-C to stop.`);
await consumeDurably(destination, {
  baseUrl: process.env.LEANI_URL?.trim() || "http://127.0.0.1:8080",
  processor,
  consumer: process.env.LEANI_CONSUMER?.trim() || "postgres-example",
  credential: required("LEANI_CONSUMER_CREDENTIAL"),
});
