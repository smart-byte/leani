//! Byte ranges of the dataset's objects kept on temporary disk, so the
//! chunks of one day read each footer, page index, and dictionary page once.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    future::Future,
    io::{self, Read as _, Seek as _, SeekFrom, Write as _},
    ops::Range,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard, OnceLock, PoisonError,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;

/// An object's range: its key and size, then the range's bounds.
type Key = (String, u64, u64, u64);

/// What a range's fetcher holds until the range is kept or given up.
type Gate = Arc<tokio::sync::Mutex<()>>;

/// Ranges written in turn around one temporary file of at most `capacity`
/// bytes: a new range overwrites the oldest, and a range read again as the
/// writes come near it is written anew, so the ranges every chunk of a day
/// reads stay while those read once go.
#[derive(Debug)]
pub(crate) struct RangeCache {
    directory: PathBuf,
    capacity: u64,
    /// Made at the first write; `None` once making it failed.
    file: OnceLock<Option<Mutex<File>>>,
    ring: Mutex<Ring>,
    /// Ranges being fetched, each with the lock its fetcher holds meanwhile.
    flights: Mutex<HashMap<Key, Gate>>,
}

/// The missing ranges of one read, by index: those it fetches, each with
/// its flight, and those others are fetching, each with their gate.
#[derive(Default)]
struct Turns {
    fetch: Vec<(usize, Flight)>,
    wait: Vec<(usize, Gate)>,
}

/// A range one caller fetches while others asking for it wait.
struct Flight {
    cache: Arc<RangeCache>,
    key: Key,
    lock: Gate,
    _held: tokio::sync::OwnedMutexGuard<()>,
}

impl Drop for Flight {
    fn drop(&mut self) {
        let mut flights = self
            .cache
            .flights
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if flights
            .get(&self.key)
            .is_some_and(|lock| Arc::ptr_eq(lock, &self.lock))
        {
            flights.remove(&self.key);
        }
    }
}

#[derive(Debug, Default)]
struct Ring {
    head: u64,
    claimed: u64,
    entries: HashMap<Key, Entry>,
    /// Where each entry starts: entries never overlap.
    offsets: BTreeMap<u64, Key>,
}

#[derive(Clone, Copy, Debug)]
struct Entry {
    id: u64,
    offset: u64,
    len: u64,
    /// Whether its bytes are written: a claimed entry is not read before.
    written: bool,
}

impl Ring {
    /// Room for `len` bytes at the head, the entries there forgotten.
    fn claim(&mut self, key: Key, len: u64, capacity: u64) -> Entry {
        if self.head.saturating_add(len) > capacity {
            self.head = 0;
        }
        let (start, end) = (self.head, self.head + len);
        let overlapping = self
            .offsets
            .range(..end)
            .rev()
            .take_while(|(offset, key)| **offset + self.entries[*key].len > start)
            .map(|(_, key)| key.clone())
            .collect::<Vec<_>>();
        for key in overlapping {
            self.forget(&key);
        }
        self.forget(&key);
        self.claimed += 1;
        let entry = Entry {
            id: self.claimed,
            offset: start,
            len,
            written: false,
        };
        self.entries.insert(key.clone(), entry);
        self.offsets.insert(start, key);
        self.head = end;
        entry
    }

    fn forget(&mut self, key: &Key) {
        if let Some(entry) = self.entries.remove(key) {
            self.offsets.remove(&entry.offset);
        }
    }
}

/// The caches this process shares, by directory and bucket.
static SHARED: Mutex<Vec<(PathBuf, String, Arc<RangeCache>)>> = Mutex::new(Vec::new());

impl RangeCache {
    /// A cache whose file is made in `directory` at its first write.
    pub(crate) fn new(directory: PathBuf, capacity: u64) -> Self {
        Self {
            directory,
            capacity,
            file: OnceLock::new(),
            ring: Mutex::default(),
            flights: Mutex::default(),
        }
    }

    /// The cache of `bucket`'s ranges in `directory` this process shares:
    /// sources built for each backfill keep what earlier ones read. The
    /// first caller sets its capacity.
    pub(crate) fn shared(directory: &Path, bucket: &str, capacity: u64) -> Arc<Self> {
        let mut shared = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some((.., cache)) = shared
            .iter()
            .find(|(kept, kept_bucket, _)| kept == directory && kept_bucket == bucket)
        {
            return Arc::clone(cache);
        }
        let cache = Arc::new(Self::new(directory.to_owned(), capacity));
        shared.push((directory.to_owned(), bucket.to_owned(), Arc::clone(&cache)));
        cache
    }

    /// `ranges` of the object `size` bytes long at `object`: the kept ones
    /// from the file, the rest from `fetch`, then kept. A range another
    /// caller is fetching is waited for, not fetched again.
    pub(crate) async fn read<F, Fut>(
        self: &Arc<Self>,
        object: &str,
        size: u64,
        ranges: Vec<Range<u64>>,
        fetch: F,
    ) -> object_store::Result<Vec<Bytes>>
    where
        F: Fn(Vec<Range<u64>>) -> Fut,
        Fut: Future<Output = object_store::Result<Vec<Bytes>>>,
    {
        let mut read = self.get(object, size, ranges.clone()).await;
        let missing = (0..ranges.len())
            .filter(|&index| read[index].is_none())
            .collect::<Vec<_>>();
        let Turns {
            fetch: mine,
            wait: theirs,
        } = self.fly(object, size, &ranges, &missing);
        let indices = mine.iter().map(|(index, _)| *index).collect::<Vec<_>>();
        for (index, bytes) in self
            .fetch_kept(object, size, &ranges, &indices, &fetch)
            .await?
        {
            read[index] = Some(bytes);
        }
        drop(mine);
        if theirs.is_empty() {
            return Ok(read.into_iter().map(Option::unwrap_or_default).collect());
        }
        for (_, lock) in &theirs {
            drop(lock.lock().await);
        }
        let waited = theirs.iter().map(|(index, _)| *index).collect::<Vec<_>>();
        let late = self
            .get(
                object,
                size,
                waited.iter().map(|&index| ranges[index].clone()).collect(),
            )
            .await;
        let mut again = Vec::new();
        for (index, bytes) in waited.into_iter().zip(late) {
            match bytes {
                Some(bytes) => read[index] = Some(bytes),
                None => again.push(index),
            }
        }
        for (index, bytes) in self
            .fetch_kept(object, size, &ranges, &again, &fetch)
            .await?
        {
            read[index] = Some(bytes);
        }
        Ok(read.into_iter().map(Option::unwrap_or_default).collect())
    }

    /// Which of the `missing` indices of `ranges` this caller fetches.
    fn fly(
        self: &Arc<Self>,
        object: &str,
        size: u64,
        ranges: &[Range<u64>],
        missing: &[usize],
    ) -> Turns {
        let mut flights = self.flights.lock().unwrap_or_else(PoisonError::into_inner);
        let mut turns = Turns::default();
        for &index in missing {
            let key = (
                object.to_owned(),
                size,
                ranges[index].start,
                ranges[index].end,
            );
            if let Some(lock) = flights.get(&key) {
                turns.wait.push((index, Arc::clone(lock)));
                continue;
            }
            let lock = Arc::new(tokio::sync::Mutex::new(()));
            let Ok(held) = Arc::clone(&lock).try_lock_owned() else {
                turns.wait.push((index, lock));
                continue;
            };
            flights.insert(key.clone(), Arc::clone(&lock));
            turns.fetch.push((
                index,
                Flight {
                    cache: Arc::clone(self),
                    key,
                    lock,
                    _held: held,
                },
            ));
        }
        turns
    }

    /// The `indices` of `ranges` from `fetch`, kept.
    async fn fetch_kept<F, Fut>(
        self: &Arc<Self>,
        object: &str,
        size: u64,
        ranges: &[Range<u64>],
        indices: &[usize],
        fetch: &F,
    ) -> object_store::Result<Vec<(usize, Bytes)>>
    where
        F: Fn(Vec<Range<u64>>) -> Fut,
        Fut: Future<Output = object_store::Result<Vec<Bytes>>>,
    {
        if indices.is_empty() {
            return Ok(Vec::new());
        }
        let wanted = indices
            .iter()
            .map(|&index| ranges[index].clone())
            .collect::<Vec<_>>();
        let fetched = fetch(wanted.clone()).await?;
        self.put(
            object,
            size,
            wanted.into_iter().zip(fetched.iter().cloned()).collect(),
        )
        .await;
        Ok(indices.iter().copied().zip(fetched).collect())
    }

    /// The kept bytes of each of `ranges` of the object, `None` for a range
    /// not kept.
    async fn get(
        self: &Arc<Self>,
        object: &str,
        size: u64,
        ranges: Vec<Range<u64>>,
    ) -> Vec<Option<Bytes>> {
        let count = ranges.len();
        let (cache, object) = (Arc::clone(self), object.to_owned());
        tokio::task::spawn_blocking(move || {
            ranges
                .into_iter()
                .map(|range| cache.get_one(&(object.clone(), size, range.start, range.end)))
                .collect()
        })
        .await
        .unwrap_or_else(|_| vec![None; count])
    }

    /// Keep `ranges` of the object, over the oldest kept ranges once the
    /// file is full.
    async fn put(self: &Arc<Self>, object: &str, size: u64, ranges: Vec<(Range<u64>, Bytes)>) {
        let (cache, object) = (Arc::clone(self), object.to_owned());
        // A range that could not be kept is fetched again when read.
        let _ = tokio::task::spawn_blocking(move || {
            for (range, bytes) in ranges {
                cache.put_one(&(object.clone(), size, range.start, range.end), &bytes);
            }
        })
        .await;
    }

    fn ring(&self) -> MutexGuard<'_, Ring> {
        self.ring.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn get_one(&self, key: &Key) -> Option<Bytes> {
        let file = self.file.get()?.as_ref()?;
        let entry = self
            .ring()
            .entries
            .get(key)
            .copied()
            .filter(|entry| entry.written)?;
        let bytes = Bytes::from(read_at(file, entry.offset, entry.len).ok()?);
        let mut ring = self.ring();
        // An entry overwritten while it was read is forgotten by now.
        if ring.entries.get(key).map(|kept| kept.id) != Some(entry.id) {
            return None;
        }
        let ahead = (entry.offset + self.capacity - ring.head) % self.capacity;
        if ahead < self.capacity / 2 {
            let entry = ring.claim(key.clone(), entry.len, self.capacity);
            drop(ring);
            self.write(file, key, entry, &bytes);
        }
        Some(bytes)
    }

    fn put_one(&self, key: &Key, bytes: &Bytes) {
        let len = bytes.len() as u64;
        if len == 0 || len > self.capacity {
            return;
        }
        let Some(file) = self.file() else {
            return;
        };
        let entry = {
            let mut ring = self.ring();
            if ring.entries.contains_key(key) {
                return;
            }
            ring.claim(key.clone(), len, self.capacity)
        };
        self.write(file, key, entry, bytes);
    }

    fn write(&self, file: &Mutex<File>, key: &Key, entry: Entry, bytes: &[u8]) {
        let written = write_at(file, entry.offset, bytes).is_ok();
        let mut ring = self.ring();
        if ring.entries.get(key).map(|kept| kept.id) == Some(entry.id) {
            if written {
                if let Some(kept) = ring.entries.get_mut(key) {
                    kept.written = true;
                }
            } else {
                ring.forget(key);
            }
        }
    }

    fn file(&self) -> Option<&Mutex<File>> {
        self.file
            .get_or_init(|| match create(&self.directory) {
                Ok(file) => Some(Mutex::new(file)),
                Err(error) => {
                    tracing::warn!(
                        directory = %self.directory.display(),
                        %error,
                        "AWS public history keeps no byte ranges on disk"
                    );
                    None
                }
            })
            .as_ref()
    }
}

/// A new file in `directory`, unlinked at once: the system frees it when the
/// process ends, however it ends.
fn create(directory: &Path) -> io::Result<File> {
    static MADE: AtomicU64 = AtomicU64::new(0);
    let path = directory.join(format!(
        ".leani-ranges-{}-{}",
        std::process::id(),
        MADE.fetch_add(1, Ordering::Relaxed)
    ));
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(&path)?;
    if let Err(error) = std::fs::remove_file(&path) {
        drop(file);
        let _ = std::fs::remove_file(&path);
        return Err(error);
    }
    Ok(file)
}

fn read_at(file: &Mutex<File>, offset: u64, len: u64) -> io::Result<Vec<u8>> {
    let mut file = file.lock().unwrap_or_else(PoisonError::into_inner);
    file.seek(SeekFrom::Start(offset))?;
    let mut bytes = vec![0; usize::try_from(len).map_err(io::Error::other)?];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn write_at(file: &Mutex<File>, offset: u64, bytes: &[u8]) -> io::Result<()> {
    let mut file = file.lock().unwrap_or_else(PoisonError::into_inner);
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache(capacity: u64) -> Arc<RangeCache> {
        Arc::new(RangeCache::new(std::env::temp_dir(), capacity))
    }

    async fn put(cache: &Arc<RangeCache>, range: Range<u64>) {
        let bytes = (range.start..range.end)
            .map(|byte| (byte % 251) as u8)
            .collect::<Vec<_>>();
        cache
            .put("object", 1_000, vec![(range, bytes.into())])
            .await;
    }

    async fn kept(cache: &Arc<RangeCache>, range: Range<u64>) -> bool {
        let expected = (range.start..range.end)
            .map(|byte| (byte % 251) as u8)
            .collect::<Vec<_>>();
        match cache
            .get("object", 1_000, vec![range])
            .await
            .pop()
            .flatten()
        {
            Some(bytes) => {
                assert_eq!(bytes.as_ref(), expected.as_slice());
                true
            }
            None => false,
        }
    }

    #[tokio::test]
    async fn a_kept_range_reads_back_as_it_was_written() {
        let cache = cache(1_000);
        let range = 100..400;
        put(&cache, range.clone()).await;
        assert!(kept(&cache, range.clone()).await);
        assert!(!kept(&cache, 100..399).await);
        assert!(
            cache.get("other", 1_000, vec![range.clone()]).await[0].is_none(),
            "another object's range"
        );
        assert!(
            cache.get("object", 999, vec![range]).await[0].is_none(),
            "the object at another size"
        );
    }

    #[tokio::test]
    async fn new_ranges_overwrite_the_oldest_once_the_file_is_full() {
        let cache = cache(100);
        put(&cache, 0..40).await;
        put(&cache, 40..80).await;
        put(&cache, 80..120).await;
        assert!(!kept(&cache, 0..40).await);
        assert!(kept(&cache, 40..80).await);
        assert!(kept(&cache, 80..120).await);
        put(&cache, 0..200).await;
        assert!(!kept(&cache, 0..200).await, "a range larger than the file");
    }

    #[tokio::test]
    async fn a_range_read_again_outlives_ranges_read_once() {
        let cache = cache(100);
        put(&cache, 0..30).await;
        put(&cache, 30..60).await;
        assert!(kept(&cache, 0..30).await);
        put(&cache, 60..90).await;
        put(&cache, 90..120).await;
        assert!(kept(&cache, 0..30).await);
        assert!(!kept(&cache, 30..60).await);
    }

    #[tokio::test]
    async fn the_caches_of_one_directory_and_bucket_are_one() {
        let directory = std::env::temp_dir();
        let first = RangeCache::shared(&directory, "https://one.example/", 100);
        let again = RangeCache::shared(&directory, "https://one.example/", 100);
        let other = RangeCache::shared(&directory, "https://two.example/", 100);
        assert!(Arc::ptr_eq(&first, &again));
        assert!(!Arc::ptr_eq(&first, &other));
    }
}
