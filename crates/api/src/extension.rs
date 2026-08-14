//! Read-only, processor-scoped HTTP query extensions.

use std::{fmt, sync::Arc};

use axum::Router;
use futures::{StreamExt as _, TryStreamExt as _, stream};
use leani_primitives::{BlockRange, ProcessorCursor};
use leani_processor_api::{Processor, ProcessorDescriptor};
use serde::{Deserialize, Serialize};

use crate::{ApiError, ApiState, CoverageResponse, coverage, decode_cursor_payload, encode_cursor};

const MAX_CURSOR_NAMESPACE_BYTES: usize = 128;
const MAX_CURSOR_SCOPE_BYTES: usize = 256;
const MAX_INDEX_LOOKUP_KEYS: usize = 10_000;

/// A trusted native, read-only query surface owned by one processor instance.
pub trait QueryExtension: Send + Sync + 'static {
    /// Stable identity of the extension's HTTP response contract.
    fn id(&self) -> &str;

    /// Optional ergonomic mount below `/v1/q`.
    ///
    /// The canonical instance-scoped route is always available. An alias is
    /// installed only when no other extension requests the same segment.
    fn alias(&self) -> Option<&str> {
        None
    }

    /// Build routes whose state is the structurally scoped query context.
    fn routes(&self) -> Router<QueryContext>;
}

/// One processor and the query extension that is allowed to read its output.
#[derive(Clone)]
pub struct QueryExtensionRegistration {
    processor: Arc<dyn Processor>,
    extension: Arc<dyn QueryExtension>,
}

impl QueryExtensionRegistration {
    #[must_use]
    pub fn new(processor: Arc<dyn Processor>, extension: Arc<dyn QueryExtension>) -> Self {
        Self {
            processor,
            extension,
        }
    }

    #[must_use]
    pub fn processor(&self) -> &Arc<dyn Processor> {
        &self.processor
    }

    #[must_use]
    pub fn extension(&self) -> &Arc<dyn QueryExtension> {
        &self.extension
    }
}

impl fmt::Debug for QueryExtensionRegistration {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryExtensionRegistration")
            .field("processor", &self.processor.descriptor().instance)
            .field("extension", &self.extension.id())
            .field("alias", &self.extension.alias())
            .finish()
    }
}

/// Discoverable route metadata for one processor-owned query extension.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryExtensionSummary {
    pub id: String,
    pub base_path: String,
    pub alias_path: Option<String>,
}

/// Bounded, read-only access to one processor instance's committed output.
///
/// Entity scans observe latest committed state. Applications that require a
/// stable multi-page bootstrap followed by a precise stream boundary should
/// use the generic query-and-follow API instead.
#[derive(Clone)]
pub struct QueryContext {
    pub(crate) state: ApiState,
    processor: Arc<dyn Processor>,
    extension_id: Arc<str>,
}

impl fmt::Debug for QueryContext {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryContext")
            .field("processor", &self.processor.descriptor().instance)
            .field("extension_id", &self.extension_id)
            .finish_non_exhaustive()
    }
}

impl QueryContext {
    pub(crate) fn new(
        state: ApiState,
        processor: Arc<dyn Processor>,
        extension_id: Arc<str>,
    ) -> Self {
        Self {
            state,
            processor,
            extension_id,
        }
    }

    #[must_use]
    pub fn descriptor(&self) -> &ProcessorDescriptor {
        self.processor.descriptor()
    }

    #[must_use]
    pub fn extension_id(&self) -> &str {
        &self.extension_id
    }

    #[must_use]
    pub fn processor(&self) -> Arc<dyn Processor> {
        self.processor.clone()
    }

    #[must_use]
    pub fn chain_id(&self) -> u64 {
        self.state.config.chain_id.0
    }

    /// Read one entity from the owning processor instance.
    ///
    /// # Errors
    ///
    /// Returns a store error using the API's stable error envelope.
    pub async fn entity(&self, collection: &str, key: &[u8]) -> Result<Option<Vec<u8>>, ApiError> {
        Ok(self
            .state
            .store
            .entity(self.descriptor(), collection, key)
            .await?)
    }

    /// Read several entities concurrently while preserving key order.
    ///
    /// The bounded concurrency prevents extension handlers from turning one
    /// page into a long sequence of independent `SQLite` round trips.
    ///
    /// # Errors
    ///
    /// Returns a store error using the API's stable error envelope.
    pub async fn entities(
        &self,
        collection: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<Vec<u8>>>, ApiError> {
        let context = self.clone();
        let collection: Arc<str> = Arc::from(collection);
        stream::iter(keys.iter().cloned())
            .map(move |key| {
                let context = context.clone();
                let collection = collection.clone();
                async move { context.entity(&collection, &key).await }
            })
            .buffered(16)
            .try_collect()
            .await
    }

    /// Scan latest committed entities for the owning processor instance.
    ///
    /// The caller may request one item beyond the configured page limit for
    /// look-ahead pagination, but no larger unbounded scan.
    ///
    /// # Errors
    ///
    /// Rejects zero or oversized limits and failed store reads.
    pub async fn scan(
        &self,
        collection: &str,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, ApiError> {
        let maximum = self.state.config.max_page_size.saturating_add(1);
        if limit == 0 || limit > maximum {
            return Err(ApiError::too_expensive(&format!(
                "extension scan limit must be in 1..={maximum}"
            )));
        }
        Ok(self
            .state
            .store
            .scan_entities(self.descriptor(), collection, after, limit)
            .await?)
    }

    /// Read entity keys from one processor-maintained index.
    ///
    /// # Errors
    ///
    /// Rejects zero or oversized limits and failed store reads.
    pub async fn index_keys(
        &self,
        index: &str,
        value: &[u8],
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, ApiError> {
        if limit == 0 || limit > MAX_INDEX_LOOKUP_KEYS {
            return Err(ApiError::too_expensive(
                "extension index limit must be in 1..=10000",
            ));
        }
        Ok(self
            .state
            .store
            .index_keys(self.descriptor(), index, value, limit)
            .await?)
    }

    /// Read deterministic indexed entity keys after an optional exclusive
    /// key boundary.
    ///
    /// # Errors
    ///
    /// Rejects zero or oversized limits and failed store reads.
    pub async fn index_keys_after(
        &self,
        index: &str,
        value: &[u8],
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Vec<u8>>, ApiError> {
        if limit == 0 || limit > MAX_INDEX_LOOKUP_KEYS {
            return Err(ApiError::too_expensive(
                "extension index limit must be in 1..=10000",
            ));
        }
        Ok(self
            .state
            .store
            .index_keys_after(self.descriptor(), index, value, after, limit)
            .await?)
    }

    /// Resolve durable coverage for the owning processor.
    ///
    /// # Errors
    ///
    /// Returns a store error using the API's stable error envelope.
    pub async fn coverage(&self, range: Option<BlockRange>) -> Result<CoverageResponse, ApiError> {
        coverage(&self.state, self.processor.as_ref(), range).await
    }

    /// Resolve coverage and fail with the standard range-incomplete response
    /// when any requested block is absent.
    ///
    /// # Errors
    ///
    /// Returns `range_incomplete` or a store error.
    pub async fn require_complete_coverage(
        &self,
        range: BlockRange,
    ) -> Result<CoverageResponse, ApiError> {
        let coverage = self.coverage(Some(range)).await?;
        if coverage.complete {
            Ok(coverage)
        } else {
            Err(ApiError::range_incomplete(coverage))
        }
    }

    /// Read the owning processor's latest committed cursor.
    ///
    /// # Errors
    ///
    /// Returns a store error using the API's stable error envelope.
    pub async fn processor_cursor(&self) -> Result<Option<ProcessorCursor>, ApiError> {
        Ok(self.state.store.processor_cursor(self.descriptor()).await?)
    }

    /// Apply the node's configured page-size policy.
    ///
    /// # Errors
    ///
    /// Rejects zero or oversized requested limits.
    pub fn page_limit(&self, requested: Option<usize>) -> Result<usize, ApiError> {
        crate::page_limit(&self.state, requested)
    }

    /// Encode an arbitrary last-key cursor scoped to this extension route.
    ///
    /// # Errors
    ///
    /// Rejects invalid namespaces or serialization failures.
    pub fn encode_scan_cursor(&self, namespace: &str, key: &[u8]) -> Result<String, ApiError> {
        self.encode_scoped_scan_cursor(namespace, &[], key)
    }

    /// Encode a last-key cursor bound to an extension-defined request scope.
    ///
    /// # Errors
    ///
    /// Rejects invalid namespaces, oversized scopes, or serialization
    /// failures.
    pub fn encode_scoped_scan_cursor(
        &self,
        namespace: &str,
        scope: &[u8],
        key: &[u8],
    ) -> Result<String, ApiError> {
        validate_cursor_namespace(namespace)?;
        validate_cursor_scope(scope)?;
        encode_cursor(&ExtensionScanCursor {
            version: 2,
            epoch: self.state.store.epoch(),
            chain_id: self.chain_id(),
            processor_instance: self.descriptor().instance.to_string(),
            processor_version: self.descriptor().version.to_string(),
            extension_id: self.extension_id.to_string(),
            namespace: namespace.to_owned(),
            scope: scope.to_vec(),
            key: key.to_vec(),
        })
    }

    /// Decode an arbitrary last-key cursor and verify every scope boundary.
    ///
    /// # Errors
    ///
    /// Rejects malformed, stale, or cross-scope cursors.
    pub fn decode_scan_cursor(&self, namespace: &str, cursor: &str) -> Result<Vec<u8>, ApiError> {
        self.decode_scoped_scan_cursor(namespace, &[], cursor)
    }

    /// Decode a last-key cursor and verify its extension-defined request
    /// scope in addition to the node, processor, and route boundaries.
    ///
    /// # Errors
    ///
    /// Rejects malformed, stale, or cross-scope cursors.
    pub fn decode_scoped_scan_cursor(
        &self,
        namespace: &str,
        scope: &[u8],
        cursor: &str,
    ) -> Result<Vec<u8>, ApiError> {
        validate_cursor_namespace(namespace)?;
        validate_cursor_scope(scope)?;
        let cursor: ExtensionScanCursor = decode_cursor_payload(cursor)?;
        if cursor.version != 2
            || cursor.epoch != self.state.store.epoch()
            || cursor.chain_id != self.chain_id()
            || cursor.processor_instance != self.descriptor().instance.as_str()
            || cursor.processor_version != self.descriptor().version.to_string()
            || cursor.extension_id != self.extension_id.as_ref()
            || cursor.namespace != namespace
            || cursor.scope != scope
        {
            return Err(ApiError::cursor(
                "cursor belongs to another store, chain, processor, extension, or namespace",
            ));
        }
        Ok(cursor.key)
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ExtensionScanCursor {
    version: u8,
    epoch: [u8; 16],
    chain_id: u64,
    processor_instance: String,
    processor_version: String,
    extension_id: String,
    namespace: String,
    scope: Vec<u8>,
    key: Vec<u8>,
}

fn validate_cursor_namespace(namespace: &str) -> Result<(), ApiError> {
    if namespace.is_empty() || namespace.len() > MAX_CURSOR_NAMESPACE_BYTES {
        Err(ApiError::invalid(
            "cursor namespace must contain 1..=128 bytes",
        ))
    } else {
        Ok(())
    }
}

fn validate_cursor_scope(scope: &[u8]) -> Result<(), ApiError> {
    if scope.len() > MAX_CURSOR_SCOPE_BYTES {
        Err(ApiError::invalid(
            "cursor scope must contain at most 256 bytes",
        ))
    } else {
        Ok(())
    }
}
