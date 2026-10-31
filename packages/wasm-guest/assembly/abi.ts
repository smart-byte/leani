// Leani Wasm ABI version 1: the block a host passes to a guest, and the
// manifest a guest returns, as the Rust SDK's `abi.rs` defines them.
//
// Inputs are postcard encoded. Hashes, addresses, and 256-bit quantities are
// big-endian bytes. The manifest is JSON.

import { Json, Jsonable } from "./json";
import { Reader } from "./postcard";
import { U256 } from "./u256";

/** The ABI version a manifest declares. */
export const ABI_VERSION: u32 = 1;

/**
 * A number or flag the source may lack, as Rust's `Option` holds one: a
 * missing value is `null`, a present one is `Some`. Hashes, addresses,
 * bytes, and `U256` values are simply `null` when missing.
 */
export class Some<T> extends Jsonable {
  constructor(public value: T) {
    super();
  }

  toJson(): Json {
    return Json.from<T>(this.value);
  }
}

/**
 * One canonical execution block, reduced to the material the manifest
 * requires and to its filter. Material it does not require is empty, or
 * `null` for the header. Transactions, receipts, and logs are in block
 * order. The block's finality is deliberately absent: a delta must not
 * depend on it.
 */
export class Block {
  chainId: u64 = 0;
  number: u64 = 0;
  hash: Uint8Array = new Uint8Array(32);
  parentHash: Uint8Array = new Uint8Array(32);
  timestamp: u64 = 0;
  header: Header | null = null;
  transactions: Transaction[] = [];
  receipts: Receipt[] = [];
  logs: Log[] = [];
  withdrawals: Withdrawal[] = [];

  static read(input: Reader): Block {
    const block = new Block();
    block.chainId = input.uint();
    block.number = input.uint();
    block.hash = input.raw(32);
    block.parentHash = input.raw(32);
    block.timestamp = input.uint();
    if (input.flag()) block.header = Header.read(input);
    if (input.flag()) block.transactions = list<Transaction>(input, Transaction.read);
    if (input.flag()) block.receipts = list<Receipt>(input, Receipt.read);
    if (input.flag()) block.logs = list<Log>(input, Log.read);
    if (input.flag()) block.withdrawals = list<Withdrawal>(input, Withdrawal.read);
    return block;
  }
}

/**
 * Header fields. A field is `null` when the manifest does not require its
 * group, `Capability.Header` for gas and fees or `Capability.BlockSize` for
 * the size and transaction count, or the source could not supply it.
 */
export class Header {
  gasLimit: Some<u64> | null = null;
  gasUsed: Some<u64> | null = null;
  baseFeePerGas: U256 | null = null;
  blobGasUsed: Some<u64> | null = null;
  excessBlobGas: Some<u64> | null = null;
  sizeBytes: Some<u64> | null = null;
  transactionCount: Some<u32> | null = null;

  static read(input: Reader): Header {
    const header = new Header();
    header.gasLimit = optionalU64(input);
    header.gasUsed = optionalU64(input);
    header.baseFeePerGas = optionalU256(input);
    header.blobGasUsed = optionalU64(input);
    header.excessBlobGas = optionalU64(input);
    header.sizeBytes = optionalU64(input);
    header.transactionCount = input.flag() ? new Some<u32>(<u32>input.uint()) : null;
    return header;
  }
}

/**
 * A transaction of the block. A field is `null`, or empty, when the manifest
 * does not require its group or the source could not supply it.
 */
export class Transaction {
  hash: Uint8Array = new Uint8Array(32);
  index: u32 = 0;
  transactionType: u8 = 0;
  /** Present when the manifest requires senders or transactions. */
  from: Uint8Array | null = null;
  /** Also `null` for a contract creation. */
  to: Uint8Array | null = null;
  nonce: Some<u64> | null = null;
  gasLimit: Some<u64> | null = null;
  value: U256 | null = null;
  /** Present when the manifest requires calldata or transactions. */
  input: Uint8Array | null = null;
  maxFeePerBlobGas: U256 | null = null;
  blobVersionedHashes: Uint8Array[] = [];

  static read(input: Reader): Transaction {
    const transaction = new Transaction();
    transaction.hash = input.raw(32);
    transaction.index = <u32>input.uint();
    transaction.transactionType = input.byte();
    transaction.from = optionalRaw(input, 20);
    transaction.to = optionalRaw(input, 20);
    transaction.nonce = optionalU64(input);
    transaction.gasLimit = optionalU64(input);
    transaction.value = optionalU256(input);
    transaction.input = input.flag() ? input.bytes() : null;
    transaction.maxFeePerBlobGas = optionalU256(input);
    transaction.blobVersionedHashes = hashes(input);
    return transaction;
  }
}

/**
 * A receipt of the block. A field is `null`, or empty, when the manifest
 * does not require its group or the source could not supply it.
 */
export class Receipt {
  transactionHash: Uint8Array = new Uint8Array(32);
  transactionIndex: u32 = 0;
  transactionType: u8 = 0;
  success: Some<bool> | null = null;
  gasUsed: Some<u64> | null = null;
  effectiveGasPrice: U256 | null = null;
  blobGasUsed: Some<u64> | null = null;
  logs: Log[] = [];

  static read(input: Reader): Receipt {
    const receipt = new Receipt();
    receipt.transactionHash = input.raw(32);
    receipt.transactionIndex = <u32>input.uint();
    receipt.transactionType = input.byte();
    receipt.success = input.flag() ? new Some<bool>(input.flag()) : null;
    receipt.gasUsed = optionalU64(input);
    receipt.effectiveGasPrice = optionalU256(input);
    receipt.blobGasUsed = optionalU64(input);
    receipt.logs = list<Log>(input, Log.read);
    return receipt;
  }
}

/** A log the block emitted. */
export class Log {
  address: Uint8Array = new Uint8Array(20);
  topics: Uint8Array[] = [];
  data: Uint8Array = new Uint8Array(0);
  /** Present when the manifest asks for `logsWithTransactionHash`. */
  transactionHash: Uint8Array | null = null;
  transactionIndex: u32 = 0;
  logIndex: u32 = 0;

  static read(input: Reader): Log {
    const log = new Log();
    log.address = input.raw(20);
    log.topics = hashes(input);
    log.data = input.bytes();
    log.transactionHash = optionalRaw(input, 32);
    log.transactionIndex = <u32>input.uint();
    log.logIndex = <u32>input.uint();
    return log;
  }
}

/** A validator withdrawal the block processed. */
export class Withdrawal {
  index: u64 = 0;
  validatorIndex: u64 = 0;
  address: Uint8Array = new Uint8Array(20);
  amountGwei: u64 = 0;

  static read(input: Reader): Withdrawal {
    const withdrawal = new Withdrawal();
    withdrawal.index = input.uint();
    withdrawal.validatorIndex = input.uint();
    withdrawal.address = input.raw(20);
    withdrawal.amountGwei = input.uint();
    return withdrawal;
  }
}

/** The block a delta belongs to, as `reduce` receives it. */
export class BlockRef {
  chainId: u64 = 0;
  number: u64 = 0;
  hash: Uint8Array = new Uint8Array(32);
  parentHash: Uint8Array = new Uint8Array(32);
  timestamp: u64 = 0;

  static read(input: Reader): BlockRef {
    const block = new BlockRef();
    block.chainId = input.uint();
    block.number = input.uint();
    block.hash = input.raw(32);
    block.parentHash = input.raw(32);
    block.timestamp = input.uint();
    return block;
  }
}

/** A material or field group of a block, as named in node configuration. */
export namespace Capability {
  export const Header = "header";
  /** `to`, nonce, gas limit, and value of every transaction. */
  export const Body = "body";
  /** Every transaction field group. */
  export const Transactions = "transactions";
  /** Transaction input bytes. */
  export const Calldata = "calldata";
  /** Recovered sender of every transaction. */
  export const Senders = "senders";
  /** Blob versioned hashes and fee caps. */
  export const BlobFields = "blob_fields";
  export const BlockSize = "block_size";
  /** Complete receipts with their logs. */
  export const Receipts = "receipts";
  /** Status, gas used, and effective gas price of every receipt. */
  export const ReceiptOutcome = "receipt_outcome";
  export const Logs = "logs";
  export const Withdrawals = "withdrawals";
}

/**
 * What a processor needs and how it reduces. It may depend on the
 * parameters, such as a filter on a configured contract.
 */
export class Manifest {
  private reduction: string = "block_local";
  private capabilities: string[] = [];
  private logTransactionHash: bool = false;
  private addressFilter: Uint8Array[] = [];
  private senderFilter: Uint8Array[] = [];
  private recipientFilter: Uint8Array[] = [];
  private topicPositions: u8[] = [];
  private topicAlternatives: Array<Uint8Array[]> = [];

  /** A block-local processor with no requirements yet. `name` is short and portable, the default schema prefix. */
  constructor(public name: string) {}

  /** Reduce blocks in chain order, reading state earlier blocks wrote. */
  ordered(): Manifest {
    this.reduction = "ordered";
    return this;
  }

  /** Require a `Capability`. */
  require(capability: string): Manifest {
    if (!this.capabilities.includes(capability)) this.capabilities.push(capability);
    return this;
  }

  /** Require logs with their transaction hashes. */
  logsWithTransactionHash(): Manifest {
    this.logTransactionHash = true;
    return this.require(Capability.Logs);
  }

  /** Keep only logs, and transactions, of these contracts. */
  addresses(addresses: Uint8Array[]): Manifest {
    this.addressFilter = this.addressFilter.concat(addresses);
    return this;
  }

  /** Keep only transactions to these accounts, and their logs. */
  recipients(recipients: Uint8Array[]): Manifest {
    this.recipientFilter = this.recipientFilter.concat(recipients);
    return this;
  }

  /** Keep only transactions from these accounts, and their logs. Needs `Capability.Senders`. */
  senders(senders: Uint8Array[]): Manifest {
    this.senderFilter = this.senderFilter.concat(senders);
    return this;
  }

  /** Keep only logs whose topic at `position` is one of `alternatives`. */
  topic(position: u8, alternatives: Uint8Array[]): Manifest {
    this.topicPositions.push(position);
    this.topicAlternatives.push(alternatives);
    return this;
  }

  /** The manifest as the host reads it. */
  toJson(): Json {
    const filter = Json.object();
    if (this.addressFilter.length) filter.set("addresses", this.addressFilter);
    if (this.topicPositions.length) {
      const topics = Json.array();
      for (let index = 0; index < this.topicPositions.length; index++) {
        topics.push(
          Json.object().set("position", this.topicPositions[index]).set("alternatives", this.topicAlternatives[index]),
        );
      }
      filter.set("topics", topics);
    }
    if (this.senderFilter.length) filter.set("senders", this.senderFilter);
    if (this.recipientFilter.length) filter.set("recipients", this.recipientFilter);
    return Json.object()
      .set("abi", ABI_VERSION)
      .set("name", this.name)
      .set("reduction", this.reduction)
      .set(
        "requires",
        Json.object()
          .set("capabilities", this.capabilities)
          .set("log_transaction_hash", this.logTransactionHash)
          .set("filter", filter),
      );
  }
}

function list<T>(input: Reader, read: (input: Reader) => T): T[] {
  const length = <i32>input.uint();
  const items = new Array<T>(length);
  for (let index = 0; index < length; index++) items[index] = read(input);
  return items;
}

function hashes(input: Reader): Uint8Array[] {
  const length = <i32>input.uint();
  const items = new Array<Uint8Array>(length);
  for (let index = 0; index < length; index++) items[index] = input.raw(32);
  return items;
}

function optionalU64(input: Reader): Some<u64> | null {
  return input.flag() ? new Some<u64>(input.uint()) : null;
}

function optionalU256(input: Reader): U256 | null {
  return input.flag() ? input.u256() : null;
}

function optionalRaw(input: Reader, length: i32): Uint8Array | null {
  return input.flag() ? input.raw(length) : null;
}
