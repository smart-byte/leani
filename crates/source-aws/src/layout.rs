//! Physical tables and column names of each export layout.

use leani_chain::DatasetLayout;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Table {
    Blocks,
    /// One row per transaction with its receipt's outcome.
    Receipts,
    /// One row per transaction, read only for its type where the receipt
    /// rows lack it.
    TransactionTypes,
    /// The same for failed transactions, which `SonarX` keeps apart.
    FailedTransactionTypes,
    Logs,
}

impl Table {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Blocks => "blocks",
            Self::Receipts => "receipts",
            Self::TransactionTypes => "transactions",
            Self::FailedTransactionTypes => "transactions_failed",
            Self::Logs => "logs",
        }
    }

    /// The table's directory under the dataset prefix.
    pub(crate) const fn directory(self, layout: DatasetLayout) -> &'static str {
        match (self, layout) {
            (Self::Blocks, _) => "blocks",
            (Self::Logs, _) => "logs",
            (Self::Receipts, DatasetLayout::SonarxEvmV1) => "receipts",
            (Self::FailedTransactionTypes, _) => "transactions_failed",
            (Self::Receipts | Self::TransactionTypes, _) => "transactions",
        }
    }

    /// Tables a layout reads for receipts and logs, the blocks first.
    pub(crate) const fn all(layout: DatasetLayout) -> &'static [Self] {
        match layout {
            DatasetLayout::AwsEthereumV1 => &[Self::Blocks, Self::Receipts, Self::Logs],
            DatasetLayout::SonarxEvmV1 => &[
                Self::Blocks,
                Self::Receipts,
                Self::TransactionTypes,
                Self::Logs,
            ],
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Column {
    Number,
    Hash,
    ParentHash,
    Timestamp,
    Nonce,
    Ommers,
    Bloom,
    TransactionsRoot,
    StateRoot,
    ReceiptsRoot,
    Miner,
    MixHash,
    ExtraData,
    Difficulty,
    GasLimit,
    GasUsed,
    TransactionCount,
    BaseFee,
    BlobGasUsed,
    ExcessBlobGas,
    BeaconRoot,
    WithdrawalsRoot,
    Size,
    TransactionIndex,
    TransactionHash,
    TransactionType,
    Status,
    CumulativeGas,
    EffectiveGasPrice,
    DepositNonce,
    DepositVersion,
    LogIndex,
    Address,
    Data,
    /// A list column of every topic.
    Topics,
    /// Topic columns by position, for layouts without a list.
    Topic(u8),
}

impl Column {
    pub(crate) const BLOCK: &[Self] = &[
        Self::Number,
        Self::Hash,
        Self::ParentHash,
        Self::Timestamp,
        Self::Nonce,
        Self::Ommers,
        Self::Bloom,
        Self::TransactionsRoot,
        Self::StateRoot,
        Self::ReceiptsRoot,
        Self::Miner,
        Self::MixHash,
        Self::ExtraData,
        Self::Difficulty,
        Self::GasLimit,
        Self::GasUsed,
        Self::TransactionCount,
        Self::BaseFee,
        Self::BlobGasUsed,
        Self::ExcessBlobGas,
        Self::BeaconRoot,
        Self::WithdrawalsRoot,
        Self::Size,
    ];
    pub(crate) const RECEIPT: &[Self] = &[
        Self::Number,
        Self::TransactionIndex,
        Self::TransactionHash,
        Self::TransactionType,
        Self::Status,
        Self::CumulativeGas,
        Self::GasUsed,
        Self::EffectiveGasPrice,
        Self::DepositNonce,
        Self::DepositVersion,
    ];
    pub(crate) const TRANSACTION_TYPE: &[Self] =
        &[Self::Number, Self::TransactionIndex, Self::TransactionType];
    pub(crate) const LOG: &[Self] = &[
        Self::Number,
        Self::TransactionIndex,
        Self::LogIndex,
        Self::Address,
        Self::Data,
        Self::Topics,
        Self::Topic(0),
        Self::Topic(1),
        Self::Topic(2),
        Self::Topic(3),
    ];

    pub(crate) const fn of(table: Table) -> &'static [Self] {
        match table {
            Table::Blocks => Self::BLOCK,
            Table::Receipts => Self::RECEIPT,
            Table::TransactionTypes | Table::FailedTransactionTypes => Self::TRANSACTION_TYPE,
            Table::Logs => Self::LOG,
        }
    }

    /// The column's name in `table` of `layout`, or `None` where the layout
    /// has no such column. A file may still lack a named column, as `SonarX`
    /// Arbitrum blocks lack the blob fields.
    pub(crate) const fn name(self, layout: DatasetLayout, table: Table) -> Option<&'static str> {
        match layout {
            DatasetLayout::AwsEthereumV1 => aws_ethereum(self, table),
            DatasetLayout::SonarxEvmV1 => sonarx(self, table),
        }
    }
}

const fn aws_ethereum(column: Column, table: Table) -> Option<&'static str> {
    use Column as C;
    Some(match (table, column) {
        (Table::Blocks, C::Number) => "number",
        (Table::Blocks, C::Hash) | (Table::Receipts, C::TransactionHash) => "hash",
        (Table::Blocks, C::ParentHash) => "parent_hash",
        (Table::Blocks, C::Timestamp) => "timestamp",
        (Table::Blocks, C::Nonce) => "nonce",
        (Table::Blocks, C::Ommers) => "sha3_uncles",
        (Table::Blocks, C::Bloom) => "logs_bloom",
        (Table::Blocks, C::TransactionsRoot) => "transactions_root",
        (Table::Blocks, C::StateRoot) => "state_root",
        (Table::Blocks, C::ReceiptsRoot) => "receipts_root",
        (Table::Blocks, C::Miner) => "miner",
        (Table::Blocks, C::ExtraData) => "extra_data",
        (Table::Blocks, C::Difficulty) => "difficulty",
        (Table::Blocks, C::GasLimit) => "gas_limit",
        (Table::Blocks, C::GasUsed) => "gas_used",
        (Table::Blocks, C::TransactionCount) => "transaction_count",
        (Table::Blocks, C::BaseFee) => "base_fee_per_gas",
        (Table::Blocks, C::Size) => "size",
        (Table::Receipts | Table::Logs, C::Number) => "block_number",
        (Table::Receipts | Table::Logs, C::TransactionIndex) => "transaction_index",
        (Table::Receipts, C::TransactionType) => "transaction_type",
        (Table::Receipts, C::Status) => "receipt_status",
        (Table::Receipts, C::CumulativeGas) => "receipt_cumulative_gas_used",
        (Table::Receipts, C::GasUsed) => "receipt_gas_used",
        (Table::Receipts, C::EffectiveGasPrice) => "receipt_effective_gas_price",
        (Table::Logs, C::LogIndex) => "log_index",
        (Table::Logs, C::Address) => "address",
        (Table::Logs, C::Data) => "data",
        (Table::Logs, C::TransactionHash) => "transaction_hash",
        (Table::Logs, C::Topics) => "topics",
        _ => return None,
    })
}

const fn sonarx(column: Column, table: Table) -> Option<&'static str> {
    use Column as C;
    Some(match (table, column) {
        (_, C::Number) => "BLOCK_NUMBER",
        (Table::Blocks, C::Hash) => "BLOCK_HASH",
        (Table::Blocks, C::ParentHash) => "BLOCK_PARENT_HASH",
        (Table::Blocks, C::Timestamp) => "TIMESTAMP",
        (Table::Blocks, C::Nonce) => "NONCE",
        (Table::Blocks, C::Ommers) => "SHA3_UNCLES",
        (Table::Blocks, C::Bloom) => "LOGS_BLOOM",
        (Table::Blocks, C::TransactionsRoot) => "TRANSACTIONS_ROOT",
        (Table::Blocks, C::StateRoot) => "STATE_ROOT",
        (Table::Blocks, C::ReceiptsRoot) => "RECEIPTS_ROOT",
        (Table::Blocks, C::Miner) => "MINER",
        (Table::Blocks, C::MixHash) => "MIX_HASH",
        (Table::Blocks, C::ExtraData) => "EXTRA_DATA",
        (Table::Blocks, C::Difficulty) => "DIFFICULTY",
        (Table::Blocks, C::GasLimit) => "GAS_LIMIT",
        (Table::Blocks | Table::Receipts, C::GasUsed) => "GAS_USED",
        (Table::Blocks, C::TransactionCount) => "TRANSACTION_COUNT",
        (Table::Blocks, C::BaseFee) => "BASE_FEE_PER_GAS",
        (Table::Blocks, C::BlobGasUsed) => "BLOB_GAS_USED",
        (Table::Blocks, C::ExcessBlobGas) => "EXCESS_BLOB_GAS",
        (Table::Blocks, C::BeaconRoot) => "PARENT_BEACON_BLOCK_ROOT",
        (Table::Blocks, C::WithdrawalsRoot) => "WITHDRAWALS_ROOT",
        (Table::Blocks, C::Size) => "SIZE",
        (_, C::TransactionIndex) => "TRANSACTION_INDEX",
        (Table::Receipts | Table::Logs, C::TransactionHash) => "TRANSACTION_HASH",
        (Table::TransactionTypes | Table::FailedTransactionTypes, C::TransactionType) => {
            "TRANSACTION_TYPE"
        }
        (Table::Receipts, C::Status) => "STATUS",
        (Table::Receipts, C::CumulativeGas) => "CUMULATIVE_GAS_USED",
        (Table::Receipts, C::EffectiveGasPrice) => "EFFECTIVE_GAS_PRICE",
        (Table::Receipts, C::DepositNonce) => "DEPOSIT_NONCE",
        (Table::Receipts, C::DepositVersion) => "DEPOSIT_RECEIPT_VERSION",
        (Table::Logs, C::LogIndex) => "LOG_INDEX",
        (Table::Logs, C::Address) => "ADDRESS",
        (Table::Logs, C::Data) => "DATA",
        (Table::Logs, C::Topic(0)) => "TOPIC1",
        (Table::Logs, C::Topic(1)) => "TOPIC2",
        (Table::Logs, C::Topic(2)) => "TOPIC3",
        (Table::Logs, C::Topic(3)) => "TOPIC4",
        _ => return None,
    })
}
