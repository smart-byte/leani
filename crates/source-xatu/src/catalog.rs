//! Deterministic Xatu object resolution and footer-only schema inspection.

use std::{collections::BTreeSet, fmt, str::FromStr, sync::Arc};

use futures::{StreamExt, stream};
use leani_primitives::BlockRange;
use object_store::{ObjectStore, ObjectStoreExt, http::HttpBuilder, path::Path as ObjectPath};
use parquet::arrow::async_reader::AsyncFileReader;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use url::Url;

use crate::reader::ObjectStoreReader;

pub const DEFAULT_XATU_BASE_URL: &str =
    "https://data.ethpandaops.io/xatu/mainnet/databases/default/";
pub const XATU_DATA_ORIGIN: &str = "https://data.ethpandaops.io/xatu/";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct XatuCatalogConfig {
    pub base_url: Url,
    pub network: String,
}

impl Default for XatuCatalogConfig {
    fn default() -> Self {
        Self {
            base_url: Url::parse(DEFAULT_XATU_BASE_URL).expect("constant Xatu URL is valid"),
            network: "mainnet".to_owned(),
        }
    }
}

impl XatuCatalogConfig {
    /// Build the public catalog location for a portable Xatu network name.
    ///
    /// # Errors
    ///
    /// Returns [`XatuError::Network`] for an empty or unsafe network name and
    /// [`XatuError::Url`] if the resulting URL is invalid.
    pub fn public(network: impl Into<String>) -> Result<Self, XatuError> {
        let network = network.into();
        if network.is_empty()
            || !network
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(XatuError::Network(network));
        }
        let base_url = Url::parse(&format!("{XATU_DATA_ORIGIN}{network}/databases/default/"))
            .map_err(XatuError::Url)?;
        Ok(Self { base_url, network })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum XatuTable {
    CanonicalExecutionBlock,
    CanonicalExecutionTransaction,
    CanonicalExecutionLogs,
    CanonicalBeaconBlock,
    CanonicalBeaconBlockExecutionTransaction,
    CanonicalBeaconBlockWithdrawal,
}

impl XatuTable {
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::CanonicalExecutionBlock => "canonical_execution_block",
            Self::CanonicalExecutionTransaction => "canonical_execution_transaction",
            Self::CanonicalExecutionLogs => "canonical_execution_logs",
            Self::CanonicalBeaconBlock => "canonical_beacon_block",
            Self::CanonicalBeaconBlockExecutionTransaction => {
                "canonical_beacon_block_execution_transaction"
            }
            Self::CanonicalBeaconBlockWithdrawal => "canonical_beacon_block_withdrawal",
        }
    }

    #[must_use]
    pub const fn partition_kind(self) -> PartitionKind {
        match self {
            Self::CanonicalExecutionBlock
            | Self::CanonicalExecutionTransaction
            | Self::CanonicalExecutionLogs => PartitionKind::BlockChunk1000,
            Self::CanonicalBeaconBlock
            | Self::CanonicalBeaconBlockExecutionTransaction
            | Self::CanonicalBeaconBlockWithdrawal => PartitionKind::Daily,
        }
    }

    #[must_use]
    pub const fn required_columns(self) -> &'static [&'static str] {
        match self {
            Self::CanonicalExecutionBlock => &[
                "block_date_time",
                "block_number",
                "block_hash",
                "gas_used",
                "extra_data",
                "base_fee_per_gas",
                "meta_network_name",
            ],
            Self::CanonicalExecutionTransaction => &[
                "block_number",
                "transaction_index",
                "transaction_hash",
                "from_address",
                "to_address",
                "value",
                "gas_used",
                "gas_price",
                "transaction_type",
                "success",
                "meta_network_name",
            ],
            Self::CanonicalExecutionLogs => &[
                "block_number",
                "transaction_index",
                "transaction_hash",
                "log_index",
                "address",
                "topic0",
                "topic1",
                "topic2",
                "topic3",
                "data",
                "meta_network_name",
            ],
            Self::CanonicalBeaconBlock => &[
                "slot",
                "slot_start_date_time",
                "block_root",
                "block_version",
                "block_total_bytes",
                "execution_payload_block_hash",
                "execution_payload_block_number",
                "execution_payload_base_fee_per_gas",
                "execution_payload_blob_gas_used",
                "execution_payload_excess_blob_gas",
                "execution_payload_gas_limit",
                "execution_payload_gas_used",
                "execution_payload_parent_hash",
                "execution_payload_transactions_count",
                "execution_payload_transactions_total_bytes",
                "meta_network_name",
            ],
            Self::CanonicalBeaconBlockExecutionTransaction => &[
                "slot",
                "slot_start_date_time",
                "block_root",
                "position",
                "hash",
                "from",
                "to",
                "gas_price",
                "gas",
                "type",
                "size",
                "blob_gas",
                "blob_gas_fee_cap",
                "blob_hashes",
                "meta_network_name",
            ],
            Self::CanonicalBeaconBlockWithdrawal => &[
                "slot",
                "slot_start_date_time",
                "block_root",
                "block_version",
                "withdrawal_index",
                "withdrawal_validator_index",
                "withdrawal_address",
                "withdrawal_amount",
                "meta_network_name",
            ],
        }
    }
}

impl fmt::Display for XatuTable {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PartitionKind {
    BlockChunk1000,
    Daily,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct XatuDate {
    pub year: u16,
    pub month: u8,
    pub day: u8,
}

impl XatuDate {
    /// Create a Gregorian calendar date.
    ///
    /// # Errors
    ///
    /// Returns [`XatuError::Date`] for an impossible date.
    pub const fn new(year: u16, month: u8, day: u8) -> Result<Self, XatuError> {
        if year == 0 || month == 0 || month > 12 || day == 0 || day > days_in_month(year, month) {
            Err(XatuError::Date)
        } else {
            Ok(Self { year, month, day })
        }
    }

    /// Return the following calendar date.
    ///
    /// # Errors
    ///
    /// Returns [`XatuError::Date`] if the year would overflow.
    pub const fn next(self) -> Result<Self, XatuError> {
        let days = days_in_month(self.year, self.month);
        if self.day < days {
            Self::new(self.year, self.month, self.day + 1)
        } else if self.month < 12 {
            Self::new(self.year, self.month + 1, 1)
        } else {
            match self.year.checked_add(1) {
                Some(year) => Self::new(year, 1, 1),
                None => Err(XatuError::Date),
            }
        }
    }

    /// Convert a non-negative Unix timestamp to its UTC calendar date.
    ///
    /// # Errors
    ///
    /// Returns [`XatuError::Date`] if the represented year is outside the
    /// portable `u16` date range.
    pub fn from_unix_seconds(timestamp: u64) -> Result<Self, XatuError> {
        let days = i64::try_from(timestamp / 86_400).map_err(|_| XatuError::Date)?;
        let shifted = days.checked_add(719_468).ok_or(XatuError::Date)?;
        let era = shifted.div_euclid(146_097);
        let day_of_era = shifted - era * 146_097;
        let year_of_era =
            (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let mut year = year_of_era + era * 400;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_prime = (5 * day_of_year + 2) / 153;
        let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
        let month = month_prime + if month_prime < 10 { 3 } else { -9 };
        year += i64::from(month <= 2);
        Self::new(
            u16::try_from(year).map_err(|_| XatuError::Date)?,
            u8::try_from(month).map_err(|_| XatuError::Date)?,
            u8::try_from(day).map_err(|_| XatuError::Date)?,
        )
    }
}

impl fmt::Display for XatuDate {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{:04}-{:02}-{:02}",
            self.year, self.month, self.day
        )
    }
}

impl FromStr for XatuDate {
    type Err = XatuError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut parts = value.split('-');
        let year = parts
            .next()
            .ok_or(XatuError::Date)?
            .parse()
            .map_err(|_| XatuError::Date)?;
        let month = parts
            .next()
            .ok_or(XatuError::Date)?
            .parse()
            .map_err(|_| XatuError::Date)?;
        let day = parts
            .next()
            .ok_or(XatuError::Date)?
            .parse()
            .map_err(|_| XatuError::Date)?;
        if parts.next().is_some() {
            return Err(XatuError::Date);
        }
        Self::new(year, month, day)
    }
}

const fn days_in_month(year: u16, month: u8) -> u8 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(400) || (year.is_multiple_of(4) && !year.is_multiple_of(100)) => {
            29
        }
        2 => 28,
        _ => 0,
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogObject {
    pub table: XatuTable,
    pub partition: String,
    pub location: String,
    pub url: Url,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ParquetColumn {
    pub path: String,
    pub physical_type: String,
    pub logical_type: Option<String>,
    pub max_definition_level: i16,
    pub max_repetition_level: i16,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectInspection {
    pub object: CatalogObject,
    pub size_bytes: u64,
    pub e_tag: Option<String>,
    pub version: Option<String>,
    pub last_modified: String,
    pub rows: i64,
    pub row_groups: usize,
    pub created_by: Option<String>,
    pub columns: Vec<ParquetColumn>,
    pub selected_columns: Vec<String>,
    pub missing_required_columns: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ObjectProbeFailure {
    pub object: CatalogObject,
    pub error: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CatalogProbeReport {
    pub catalog_base_url: Url,
    pub network: String,
    pub objects: Vec<ObjectInspection>,
    pub failures: Vec<ObjectProbeFailure>,
    pub total_object_bytes: u64,
}

impl CatalogProbeReport {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.failures.is_empty()
            && self
                .objects
                .iter()
                .all(|object| object.missing_required_columns.is_empty())
    }
}

#[derive(Clone, Debug)]
pub struct XatuCatalog {
    config: XatuCatalogConfig,
    store: Arc<dyn ObjectStore>,
}

impl XatuCatalog {
    /// Create an HTTP-backed read-only catalog.
    ///
    /// # Errors
    ///
    /// Returns [`XatuError`] if the HTTP store cannot be configured.
    pub fn new(config: XatuCatalogConfig) -> Result<Self, XatuError> {
        let store = HttpBuilder::new()
            .with_url(config.base_url.as_str().trim_end_matches('/'))
            .build()
            .map_err(|error| XatuError::ObjectStore(error.to_string()))?;
        Ok(Self {
            config,
            store: Arc::new(store),
        })
    }

    #[must_use]
    pub fn config(&self) -> &XatuCatalogConfig {
        &self.config
    }

    pub(crate) fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    /// Resolve every 1,000-block execution partition intersecting `range`.
    ///
    /// # Errors
    ///
    /// Returns [`XatuError::PartitionKind`] for daily tables.
    pub fn execution_objects(
        &self,
        table: XatuTable,
        range: BlockRange,
    ) -> Result<Vec<CatalogObject>, XatuError> {
        if !matches!(table.partition_kind(), PartitionKind::BlockChunk1000) {
            return Err(XatuError::PartitionKind);
        }
        let first = (range.start().0 / 1_000) * 1_000;
        let last = (range.end().0 / 1_000) * 1_000;
        let mut objects = Vec::new();
        let mut chunk = first;
        loop {
            let partition = chunk.to_string();
            objects.push(self.object(table, &format!("1000/{partition}.parquet"), partition)?);
            if chunk == last {
                break;
            }
            chunk = chunk.checked_add(1_000).ok_or(XatuError::BlockRange)?;
        }
        Ok(objects)
    }

    /// Resolve every daily beacon partition in an inclusive date range.
    ///
    /// # Errors
    ///
    /// Returns [`XatuError`] for an execution table, reversed range, invalid
    /// URL, or date overflow.
    pub fn daily_objects(
        &self,
        table: XatuTable,
        start: XatuDate,
        end: XatuDate,
    ) -> Result<Vec<CatalogObject>, XatuError> {
        if !matches!(table.partition_kind(), PartitionKind::Daily) {
            return Err(XatuError::PartitionKind);
        }
        if start > end {
            return Err(XatuError::Date);
        }
        let mut objects = Vec::new();
        let mut date = start;
        loop {
            let partition = date.to_string();
            objects.push(self.object(
                table,
                &format!(
                    "{}/{}/{}.parquet",
                    date.year,
                    u16::from(date.month),
                    u16::from(date.day)
                ),
                partition,
            )?);
            if date == end {
                break;
            }
            date = date.next()?;
        }
        Ok(objects)
    }

    fn object(
        &self,
        table: XatuTable,
        suffix: &str,
        partition: String,
    ) -> Result<CatalogObject, XatuError> {
        let location = format!("{}/{suffix}", table.name());
        let url = self
            .config
            .base_url
            .join(&location)
            .map_err(XatuError::Url)?;
        Ok(CatalogObject {
            table,
            partition,
            location,
            url,
        })
    }

    /// Inspect object heads and Parquet footers with bounded concurrency.
    ///
    /// Individual failures are retained in the report so probes are useful
    /// during outages and schema transitions.
    ///
    /// # Errors
    ///
    /// Returns [`XatuError::Concurrency`] when concurrency is zero.
    pub async fn inspect(
        &self,
        objects: Vec<CatalogObject>,
        concurrency: usize,
    ) -> Result<CatalogProbeReport, XatuError> {
        if concurrency == 0 {
            return Err(XatuError::Concurrency);
        }
        let store = Arc::clone(&self.store);
        let mut outcomes = stream::iter(objects)
            .map(move |object| {
                let store = Arc::clone(&store);
                async move {
                    match inspect_object(store, object.clone()).await {
                        Ok(inspection) => Ok(inspection),
                        Err(error) => Err(ObjectProbeFailure {
                            object,
                            error: error.to_string(),
                        }),
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<_>>()
            .await;
        outcomes.sort_by(|left, right| outcome_key(left).cmp(&outcome_key(right)));

        let mut inspections = Vec::new();
        let mut failures = Vec::new();
        for outcome in outcomes {
            match outcome {
                Ok(inspection) => {
                    if !inspection.missing_required_columns.is_empty() {
                        failures.push(ObjectProbeFailure {
                            object: inspection.object.clone(),
                            error: XatuError::Schema {
                                table: inspection.object.table,
                                missing: inspection.missing_required_columns.clone(),
                            }
                            .to_string(),
                        });
                    }
                    inspections.push(inspection);
                }
                Err(failure) => failures.push(failure),
            }
        }
        let total_object_bytes = inspections.iter().fold(0_u64, |total, object| {
            total.saturating_add(object.size_bytes)
        });
        Ok(CatalogProbeReport {
            catalog_base_url: self.config.base_url.clone(),
            network: self.config.network.clone(),
            objects: inspections,
            failures,
            total_object_bytes,
        })
    }
}

fn outcome_key(outcome: &Result<ObjectInspection, ObjectProbeFailure>) -> (XatuTable, &str) {
    match outcome {
        Ok(inspection) => (
            inspection.object.table,
            inspection.object.partition.as_str(),
        ),
        Err(failure) => (failure.object.table, failure.object.partition.as_str()),
    }
}

async fn inspect_object(
    store: Arc<dyn ObjectStore>,
    object: CatalogObject,
) -> Result<ObjectInspection, XatuError> {
    let location = ObjectPath::parse(&object.location)
        .map_err(|error| XatuError::ObjectStore(error.to_string()))?;
    let head = store
        .head(&location)
        .await
        .map_err(|error| XatuError::ObjectStore(error.to_string()))?;
    let (mut reader, _) = ObjectStoreReader::new(store, location, head.size);
    let metadata = reader
        .get_metadata(None)
        .await
        .map_err(|error| XatuError::Parquet(error.to_string()))?;
    let schema = metadata.file_metadata().schema_descr();
    let columns = schema
        .columns()
        .iter()
        .map(|column| ParquetColumn {
            path: column.path().string(),
            physical_type: format!("{:?}", column.physical_type()),
            logical_type: column.logical_type_ref().map(|value| format!("{value:?}")),
            max_definition_level: column.max_def_level(),
            max_repetition_level: column.max_rep_level(),
        })
        .collect::<Vec<_>>();
    let missing_required_columns = missing_required_columns(object.table, &columns);
    let selected_columns = object
        .table
        .required_columns()
        .iter()
        .map(|column| (*column).to_owned())
        .collect();

    Ok(ObjectInspection {
        object,
        size_bytes: head.size,
        e_tag: head.e_tag,
        version: head.version,
        last_modified: head.last_modified.to_rfc3339(),
        rows: metadata.file_metadata().num_rows(),
        row_groups: metadata.num_row_groups(),
        created_by: metadata.file_metadata().created_by().map(str::to_owned),
        columns,
        selected_columns,
        missing_required_columns,
    })
}

fn missing_required_columns(table: XatuTable, columns: &[ParquetColumn]) -> Vec<String> {
    let actual = columns
        .iter()
        .filter_map(|column| column.path.split('.').next())
        .collect::<BTreeSet<_>>();
    table
        .required_columns()
        .iter()
        .copied()
        .filter(|column| !actual.contains(column))
        .map(str::to_owned)
        .collect()
}

#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum XatuError {
    #[error("Xatu table uses another partition kind")]
    PartitionKind,
    #[error("invalid Xatu date")]
    Date,
    #[error("invalid Xatu network name: {0}")]
    Network(String),
    #[error("invalid block range for Xatu partitioning")]
    BlockRange,
    #[error("catalog concurrency must be greater than zero")]
    Concurrency,
    #[error("Xatu URL error: {0}")]
    Url(url::ParseError),
    #[error("Xatu object store error: {0}")]
    ObjectStore(String),
    #[error("Xatu Parquet error: {0}")]
    Parquet(String),
    #[error("Xatu projected data is invalid: {0}")]
    Data(String),
    #[error(
        "Xatu table {table} does not completely cover {range:?}: expected {expected} rows, received {actual}"
    )]
    IncompleteRange {
        table: XatuTable,
        range: BlockRange,
        expected: usize,
        actual: usize,
    },
    #[error("Xatu projection exceeded the {resource} budget: {actual} > {limit}")]
    Budget {
        resource: &'static str,
        actual: u64,
        limit: u64,
    },
    #[error("Xatu projection was cancelled")]
    Cancelled,
    #[error("Xatu table {table} is missing required columns: {missing:?}")]
    Schema {
        table: XatuTable,
        missing: Vec<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;
    use leani_primitives::BlockNumber;

    #[test]
    fn execution_partitions_are_exact_and_stable() {
        let catalog = XatuCatalog::new(XatuCatalogConfig::default()).expect("catalog");
        let range =
            BlockRange::new(BlockNumber(19_426_589), BlockNumber(19_427_001)).expect("range");
        let objects = catalog
            .execution_objects(XatuTable::CanonicalExecutionBlock, range)
            .expect("objects");
        assert_eq!(
            objects
                .iter()
                .map(|object| object.partition.as_str())
                .collect::<Vec<_>>(),
            vec!["19426000", "19427000"]
        );
        assert_eq!(
            objects[0].url.as_str(),
            "https://data.ethpandaops.io/xatu/mainnet/databases/default/canonical_execution_block/1000/19426000.parquet"
        );
    }

    #[test]
    fn daily_partitions_handle_leap_days() {
        let catalog = XatuCatalog::new(XatuCatalogConfig::default()).expect("catalog");
        let objects = catalog
            .daily_objects(
                XatuTable::CanonicalBeaconBlock,
                XatuDate::new(2024, 2, 28).expect("date"),
                XatuDate::new(2024, 3, 1).expect("date"),
            )
            .expect("objects");
        assert_eq!(
            objects
                .iter()
                .map(|object| object.partition.as_str())
                .collect::<Vec<_>>(),
            vec!["2024-02-28", "2024-02-29", "2024-03-01"]
        );
    }

    #[test]
    fn dates_parse_strictly() {
        assert_eq!(
            "2024-03-13".parse::<XatuDate>().expect("date"),
            XatuDate::new(2024, 3, 13).expect("date")
        );
        assert!("2024-02-30".parse::<XatuDate>().is_err());
        assert!("2024-3".parse::<XatuDate>().is_err());
    }

    #[test]
    fn unix_timestamps_convert_to_utc_dates() {
        assert_eq!(
            XatuDate::from_unix_seconds(0).expect("epoch"),
            XatuDate::new(1970, 1, 1).expect("date")
        );
        assert_eq!(
            XatuDate::from_unix_seconds(1_710_288_000).expect("Dencun"),
            XatuDate::new(2024, 3, 13).expect("date")
        );
        assert_eq!(
            XatuDate::from_unix_seconds(1_709_164_800).expect("leap day"),
            XatuDate::new(2024, 2, 29).expect("date")
        );
    }

    #[test]
    fn public_catalog_rejects_path_injection() {
        assert!(XatuCatalogConfig::public("mainnet").is_ok());
        assert!(matches!(
            XatuCatalogConfig::public("../mainnet"),
            Err(XatuError::Network(_))
        ));
    }

    #[test]
    fn required_schema_fails_closed() {
        let columns = vec![ParquetColumn {
            path: "block_number".to_owned(),
            physical_type: "INT64".to_owned(),
            logical_type: None,
            max_definition_level: 0,
            max_repetition_level: 0,
        }];
        assert!(!missing_required_columns(XatuTable::CanonicalExecutionBlock, &columns).is_empty());
    }
}
