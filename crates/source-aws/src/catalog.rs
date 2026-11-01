//! Day partitions of a dataset and the blocks each day holds.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use leani_chain::DatasetLayout;
use object_store::{ObjectStore, path::Path};
use parquet::{arrow::async_reader::ParquetRecordBatchStreamBuilder, file::statistics::Statistics};
use url::Url;

use crate::{
    AwsError,
    layout::{Column, Table},
};

/// How long the list of published days is trusted before it is listed again.
const DATES_TTL: Duration = Duration::from_hours(1);
/// S3 pages hold up to 1,000 keys, far below this.
const MAX_PAGE_BYTES: usize = 8 * 1_024 * 1_024;
/// A dataset's days fit in a few pages.
const MAX_PAGES: usize = 64;

/// Directory listing of an object store, one level at a time.
#[async_trait]
pub trait Listing: Send + Sync + std::fmt::Debug {
    /// Names of the directories directly under `prefix`, which ends in `/`.
    async fn directories(&self, prefix: &str) -> Result<Vec<String>, AwsError>;
    /// Keys and sizes of the objects directly under `prefix`.
    async fn objects(&self, prefix: &str) -> Result<Vec<(String, u64)>, AwsError>;
}

/// Anonymous S3 `ListObjectsV2` over HTTPS.
#[derive(Clone, Debug)]
pub struct S3Listing {
    client: reqwest::Client,
    bucket: Url,
}

impl S3Listing {
    /// List the bucket at `bucket`, a virtual-hosted bucket URL.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be built.
    pub fn new(bucket: Url) -> Result<Self, AwsError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|error| AwsError::Store(error.to_string()))?;
        Ok(Self { client, bucket })
    }

    /// Every page of the listing of `prefix`: its sub-prefixes and objects.
    async fn list(&self, prefix: &str) -> Result<(Vec<String>, Vec<(String, u64)>), AwsError> {
        let failed = |detail: String| AwsError::Listing {
            prefix: prefix.to_owned(),
            detail,
        };
        let (mut prefixes, mut objects) = (Vec::new(), Vec::new());
        let mut token: Option<String> = None;
        for _ in 0..MAX_PAGES {
            let mut query = vec![
                ("list-type", "2".to_owned()),
                ("prefix", prefix.to_owned()),
                ("delimiter", "/".to_owned()),
            ];
            if let Some(token) = &token {
                query.push(("continuation-token", token.clone()));
            }
            let response = self
                .client
                .get(self.bucket.clone())
                .query(&query)
                .send()
                .await
                .map_err(|error| failed(error.to_string()))?;
            if !response.status().is_success() {
                return Err(failed(format!("HTTP {}", response.status())));
            }
            let body = read_page(response).await.map_err(failed)?;
            for block in elements(&body, "CommonPrefixes") {
                if let Some(prefix) = elements(block, "Prefix").next() {
                    prefixes.push(unescape(prefix));
                }
            }
            for block in elements(&body, "Contents") {
                let key = elements(block, "Key").next().map(unescape);
                let size = elements(block, "Size")
                    .next()
                    .and_then(|size| size.parse().ok());
                if let (Some(key), Some(size)) = (key, size) {
                    objects.push((key, size));
                }
            }
            token = elements(&body, "NextContinuationToken")
                .next()
                .map(unescape);
            if token.is_none() {
                return Ok((prefixes, objects));
            }
        }
        Err(failed(format!("more than {MAX_PAGES} pages")))
    }
}

#[async_trait]
impl Listing for S3Listing {
    async fn directories(&self, prefix: &str) -> Result<Vec<String>, AwsError> {
        let (prefixes, _) = self.list(prefix).await?;
        Ok(prefixes
            .iter()
            .filter_map(|child| child.strip_prefix(prefix))
            .map(|name| name.trim_end_matches('/').to_owned())
            .collect())
    }

    async fn objects(&self, prefix: &str) -> Result<Vec<(String, u64)>, AwsError> {
        Ok(self.list(prefix).await?.1)
    }
}

/// A listing page's text, refused past [`MAX_PAGE_BYTES`].
async fn read_page(mut response: reqwest::Response) -> Result<String, String> {
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|error| error.to_string())? {
        if body.len() + chunk.len() > MAX_PAGE_BYTES {
            return Err(format!("a page exceeds {MAX_PAGE_BYTES} bytes"));
        }
        body.extend_from_slice(&chunk);
    }
    String::from_utf8(body).map_err(|error| error.to_string())
}

/// The text of each `<tag>…</tag>` in `xml`, in order. S3 listings nest no
/// element inside one of the same name.
fn elements<'a>(xml: &'a str, tag: &str) -> impl Iterator<Item = &'a str> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut rest = xml;
    std::iter::from_fn(move || {
        let start = rest.find(&open)? + open.len();
        let end = start + rest[start..].find(&close)?;
        let text = &rest[start..end];
        rest = &rest[end + close.len()..];
        Some(text)
    })
}

fn unescape(text: &str) -> String {
    text.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

/// Listing through an [`ObjectStore`] that lists natively, such as a local
/// or in-memory store.
#[derive(Debug)]
pub struct StoreListing(pub Arc<dyn ObjectStore>);

#[async_trait]
impl Listing for StoreListing {
    async fn directories(&self, prefix: &str) -> Result<Vec<String>, AwsError> {
        let listed = self
            .0
            .list_with_delimiter(Some(&Path::from(prefix)))
            .await
            .map_err(|error| AwsError::Store(error.to_string()))?;
        Ok(listed
            .common_prefixes
            .iter()
            .filter_map(|path| path.filename().map(str::to_owned))
            .collect())
    }

    async fn objects(&self, prefix: &str) -> Result<Vec<(String, u64)>, AwsError> {
        let listed = self
            .0
            .list_with_delimiter(Some(&Path::from(prefix)))
            .await
            .map_err(|error| AwsError::Store(error.to_string()))?;
        Ok(listed
            .objects
            .into_iter()
            .map(|object| (object.location.to_string(), object.size))
            .collect())
    }
}

/// One day partition: its date directory and the blocks it holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Day {
    pub date: String,
    pub first: u64,
    pub last: u64,
}

#[derive(Debug)]
pub(crate) struct Catalog {
    layout: DatasetLayout,
    prefix: String,
    listing: Arc<dyn Listing>,
    store: Arc<dyn ObjectStore>,
    dates: tokio::sync::Mutex<Option<(Instant, Vec<String>)>>,
    spans: Mutex<BTreeMap<String, (u64, u64)>>,
}

impl Catalog {
    pub(crate) fn new(
        layout: DatasetLayout,
        prefix: &str,
        listing: Arc<dyn Listing>,
        store: Arc<dyn ObjectStore>,
    ) -> Self {
        Self {
            layout,
            prefix: prefix.trim_matches('/').to_owned(),
            listing,
            store,
            dates: tokio::sync::Mutex::new(None),
            spans: Mutex::new(BTreeMap::new()),
        }
    }

    pub(crate) fn store(&self) -> Arc<dyn ObjectStore> {
        Arc::clone(&self.store)
    }

    fn table_prefix(&self, table: Table) -> String {
        format!("{}/{}/", self.prefix, table.directory(self.layout))
    }

    /// Objects of `table` for `date`.
    pub(crate) async fn objects(
        &self,
        table: Table,
        date: &str,
    ) -> Result<Vec<(String, u64)>, AwsError> {
        let prefix = format!("{}{date}/", self.table_prefix(table));
        let mut objects = self.listing.objects(&prefix).await?;
        objects.retain(|(key, _)| key.ends_with(".parquet"));
        Ok(objects)
    }

    /// Days published in every table the layout reads, oldest first. A day
    /// one table lacks is still being published, or has no rows there.
    async fn dates(&self) -> Result<Vec<String>, AwsError> {
        let mut cached = self.dates.lock().await;
        if let Some((listed, dates)) = cached.as_ref()
            && listed.elapsed() < DATES_TTL
        {
            return Ok(dates.clone());
        }
        let mut common: Option<BTreeSet<String>> = None;
        let directories = Table::all(self.layout)
            .iter()
            .map(|table| table.directory(self.layout))
            .collect::<BTreeSet<_>>();
        for directory in directories {
            let prefix = format!("{}/{directory}/", self.prefix);
            let dates = self
                .listing
                .directories(&prefix)
                .await?
                .into_iter()
                .filter(|name| name.starts_with("date="))
                .collect::<BTreeSet<_>>();
            common = Some(match common {
                Some(common) => common.intersection(&dates).cloned().collect(),
                None => dates,
            });
        }
        let dates = common.unwrap_or_default().into_iter().collect::<Vec<_>>();
        *cached = Some((Instant::now(), dates.clone()));
        Ok(dates)
    }

    /// The first and last block of `date`, from its block files' footers.
    pub(crate) async fn span(&self, date: &str) -> Result<(u64, u64), AwsError> {
        if let Some(span) = self
            .spans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(date)
        {
            return Ok(*span);
        }
        let number = Column::Number
            .name(self.layout, Table::Blocks)
            .unwrap_or_default();
        let mut span: Option<(u64, u64)> = None;
        for (key, size) in self.objects(Table::Blocks, date).await? {
            let reader = crate::read::StoreReader::new(Arc::clone(&self.store), &key, size);
            let builder = ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .map_err(|error| AwsError::Parquet(format!("{key}: {error}")))?;
            let metadata = builder.metadata();
            let column = metadata
                .file_metadata()
                .schema_descr()
                .columns()
                .iter()
                .position(|column| column.name() == number)
                .ok_or_else(|| AwsError::MissingColumn {
                    table: Table::Blocks.label(),
                    column: number.to_owned(),
                })?;
            for row_group in metadata.row_groups() {
                let (min, max) = row_group
                    .column(column)
                    .statistics()
                    .and_then(block_bounds)
                    .ok_or_else(|| AwsError::Malformed {
                        table: Table::Blocks.label(),
                        detail: format!("{key} has no block-number statistics"),
                    })?;
                span =
                    Some(span.map_or((min, max), |(first, last)| (first.min(min), last.max(max))));
            }
        }
        let span = span.ok_or_else(|| AwsError::Malformed {
            table: Table::Blocks.label(),
            detail: format!("{date} has no block rows"),
        })?;
        self.spans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(date.to_owned(), span);
        Ok(span)
    }

    /// The published days holding blocks `first..=last`, in order. Fails
    /// when a block in the range is in no published day.
    pub(crate) async fn days(&self, first: u64, last: u64) -> Result<Vec<Day>, AwsError> {
        let dates = self.dates().await?;
        // Days hold increasing, disjoint block spans: find the first day
        // ending at or after `first`.
        let (mut low, mut high) = (0, dates.len());
        while low < high {
            let middle = low + (high - low) / 2;
            if self.span(&dates[middle]).await?.1 < first {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let mut days = Vec::new();
        let mut next = first;
        for date in &dates[low..] {
            let (day_first, day_last) = self.span(date).await?;
            if day_first > last {
                break;
            }
            if day_first > next {
                break;
            }
            days.push(Day {
                date: date.clone(),
                first: day_first,
                last: day_last,
            });
            next = day_last.saturating_add(1);
            if day_last >= last {
                return Ok(days);
            }
        }
        let missing_from = next;
        Err(AwsError::NotPublished(
            leani_primitives::BlockRange::new(
                leani_primitives::BlockNumber(missing_from),
                leani_primitives::BlockNumber(last.max(missing_from)),
            )
            .map_err(|error| AwsError::Store(error.to_string()))?,
        ))
    }
}

/// Block-number bounds of a column chunk: `INT64` statistics, or the
/// big-endian `DECIMAL(38, 0)` `SonarX` stores numbers as.
fn block_bounds(statistics: &Statistics) -> Option<(u64, u64)> {
    match statistics {
        Statistics::Int64(values) => Some((
            u64::try_from(*values.min_opt()?).ok()?,
            u64::try_from(*values.max_opt()?).ok()?,
        )),
        Statistics::FixedLenByteArray(values) => {
            let decode = |bytes: &[u8]| {
                u64::try_from(i128::from_be_bytes(<[u8; 16]>::try_from(bytes).ok()?)).ok()
            };
            Some((
                decode(values.min_bytes_opt()?)?,
                decode(values.max_bytes_opt()?)?,
            ))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An S3 endpoint that answers every request with `page`.
    fn serving(page: String) -> Url {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        std::thread::spawn(move || {
            for mut socket in listener.incoming().flatten() {
                let _ = std::io::Read::read(&mut socket, &mut [0; 4_096]);
                let reply = format!(
                    "HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{page}",
                    page.len()
                );
                let _ = std::io::Write::write_all(&mut socket, reply.as_bytes());
            }
        });
        format!("http://{address}/").parse().unwrap()
    }

    #[tokio::test]
    async fn listings_stop_at_their_page_bounds() {
        let endless = serving(
            "<ListBucketResult><NextContinuationToken>t</NextContinuationToken></ListBucketResult>"
                .to_owned(),
        );
        let error = S3Listing::new(endless)
            .unwrap()
            .directories("v1/")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("more than 64 pages"), "{error}");
        let huge = serving("x".repeat(MAX_PAGE_BYTES + 1));
        let error = S3Listing::new(huge)
            .unwrap()
            .directories("v1/")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("exceeds"), "{error}");
    }

    #[test]
    fn s3_listing_pages_parse() {
        let page = "<ListBucketResult><Prefix>v1/blocks/</Prefix>\
            <NextContinuationToken>a&amp;b</NextContinuationToken>\
            <Contents><Key>v1/blocks/date=2026-10-14/x.parquet</Key><Size>37</Size></Contents>\
            <CommonPrefixes><Prefix>v1/blocks/date=2026-10-14/</Prefix></CommonPrefixes>\
            </ListBucketResult>";
        let prefixes = elements(page, "CommonPrefixes")
            .filter_map(|block| elements(block, "Prefix").next())
            .collect::<Vec<_>>();
        assert_eq!(prefixes, ["v1/blocks/date=2026-10-14/"]);
        let keys = elements(page, "Contents")
            .filter_map(|block| elements(block, "Key").next())
            .collect::<Vec<_>>();
        assert_eq!(keys, ["v1/blocks/date=2026-10-14/x.parquet"]);
        assert_eq!(
            elements(page, "NextContinuationToken")
                .next()
                .map(unescape)
                .as_deref(),
            Some("a&b")
        );
    }
}
