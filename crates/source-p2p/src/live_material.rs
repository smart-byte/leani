//! Live material that the subscriptions of one source share.
//!
//! Each live subscription runs its own download loop. Two that follow the
//! same head, such as the node's live lane and an ad-hoc run's feed, ask for
//! the same blocks moments apart. A block's verified bodies or receipts are
//! downloaded once, by whichever subscription asks first, and kept for the
//! most recent blocks. A block hash commits to the header that verifies its
//! material, so the material is the same for every subscription that asks.

use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    sync::{Mutex, MutexGuard, PoisonError},
};

use alloy_primitives::B256;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::P2pError;

/// Blocks kept per kind of material.
const RECENT_BLOCKS: usize = 32;

#[derive(Debug)]
pub(crate) struct SharedMaterial<V> {
    state: Mutex<State<V>>,
}

#[derive(Debug)]
struct State<V> {
    /// The most recent blocks' material, oldest first.
    recent: VecDeque<(B256, V)>,
    /// The blocks a subscription is downloading: their material arrives on
    /// these channels.
    pending: HashMap<B256, watch::Receiver<Option<V>>>,
}

impl<V> Default for SharedMaterial<V> {
    fn default() -> Self {
        Self {
            state: Mutex::new(State {
                recent: VecDeque::new(),
                pending: HashMap::new(),
            }),
        }
    }
}

impl<V: Clone> SharedMaterial<V> {
    /// The material of `hashes`, in their order. `fetch` downloads the
    /// blocks no other subscription has or is downloading, given by their
    /// indices into `hashes`, and returns their material in that order. A
    /// block another subscription is downloading is waited for at most
    /// `LIVE_MATERIAL_TIMEOUT`, then downloaded here too: that subscription
    /// may stall, such as one whose consumer stopped reading.
    pub(crate) async fn get_or_fetch<F, Fut>(
        &self,
        hashes: &[B256],
        cancellation: &CancellationToken,
        mut fetch: F,
    ) -> Result<Vec<V>, P2pError>
    where
        F: FnMut(Vec<usize>) -> Fut,
        Fut: Future<Output = Result<Vec<V>, P2pError>>,
    {
        let mut material = vec![None; hashes.len()];
        let mut waiting = Vec::new();
        let mut claim = Claim {
            shared: self,
            blocks: Vec::new(),
        };
        {
            let mut state = self.lock();
            for (index, hash) in hashes.iter().enumerate() {
                if let Some((_, value)) = state.recent.iter().find(|(recent, _)| recent == hash) {
                    material[index] = Some(value.clone());
                } else if let Some(receiver) = state.pending.get(hash) {
                    waiting.push((index, receiver.clone()));
                } else {
                    let (sender, receiver) = watch::channel(None);
                    state.pending.insert(*hash, receiver);
                    claim.blocks.push((index, *hash, sender));
                }
            }
        }
        let mut downloaded = claim.blocks.len();
        if !claim.blocks.is_empty() {
            let indices = claim.blocks.iter().map(|(index, ..)| *index).collect();
            claim.complete(fetch(indices).await?, &mut material)?;
        }
        let mut abandoned = Vec::new();
        let deadline = tokio::time::Instant::now() + crate::LIVE_MATERIAL_TIMEOUT;
        for (index, mut receiver) in waiting {
            let value = tokio::select! {
                value = tokio::time::timeout_at(deadline, receiver.wait_for(Option::is_some)) => {
                    value.ok().and_then(Result::ok).and_then(|value| value.clone())
                }
                () = cancellation.cancelled() => return Err(P2pError::Cancelled),
            };
            match value {
                Some(value) => material[index] = Some(value),
                None => abandoned.push(index),
            }
        }
        if !abandoned.is_empty() {
            downloaded += abandoned.len();
            let fetched = fetch(abandoned.clone()).await?;
            if fetched.len() != abandoned.len() {
                return Err(incomplete(fetched.len(), abandoned.len()));
            }
            let mut state = self.lock();
            for (index, value) in abandoned.into_iter().zip(fetched) {
                state.keep(hashes[index], value.clone());
                material[index] = Some(value);
            }
        }
        let shared = hashes.len() - downloaded;
        if shared > 0 {
            debug!(
                shared,
                downloaded, "served live material without downloading it again"
            );
        }
        Ok(material
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .expect("every block was kept, claimed, or waited for"))
    }

    fn lock(&self) -> MutexGuard<'_, State<V>> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl<V> State<V> {
    fn keep(&mut self, hash: B256, value: V) {
        if self.recent.iter().any(|(recent, _)| *recent == hash) {
            return;
        }
        if self.recent.len() == RECENT_BLOCKS {
            self.recent.pop_front();
        }
        self.recent.push_back((hash, value));
    }
}

/// The blocks one subscription downloads for every subscription. Dropped
/// before it completes, it hands them back: their channels close, and the
/// subscriptions waiting for them download them themselves.
struct Claim<'a, V: Clone> {
    shared: &'a SharedMaterial<V>,
    blocks: Vec<(usize, B256, watch::Sender<Option<V>>)>,
}

impl<V: Clone> Claim<'_, V> {
    fn complete(mut self, fetched: Vec<V>, material: &mut [Option<V>]) -> Result<(), P2pError> {
        if fetched.len() != self.blocks.len() {
            return Err(incomplete(fetched.len(), self.blocks.len()));
        }
        let mut state = self.shared.lock();
        for ((index, hash, sender), value) in self.blocks.drain(..).zip(fetched) {
            state.pending.remove(&hash);
            state.keep(hash, value.clone());
            sender.send_replace(Some(value.clone()));
            material[index] = Some(value);
        }
        Ok(())
    }
}

impl<V: Clone> Drop for Claim<'_, V> {
    fn drop(&mut self) {
        if self.blocks.is_empty() {
            return;
        }
        let mut state = self.shared.lock();
        for (_, hash, _) in &self.blocks {
            state.pending.remove(hash);
        }
    }
}

fn incomplete(returned: usize, expected: usize) -> P2pError {
    P2pError::IncompleteResponse {
        component: "live material",
        returned,
        expected,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use tokio::sync::Notify;

    use super::*;

    fn hash(byte: u8) -> B256 {
        B256::repeat_byte(byte)
    }

    fn answer(asked: &Mutex<Vec<Vec<u8>>>, hashes: &[B256], indices: &[usize]) -> Vec<u8> {
        let bytes = indices
            .iter()
            .map(|index| hashes[*index][0])
            .collect::<Vec<_>>();
        asked.lock().expect("asked").push(bytes.clone());
        bytes
    }

    #[tokio::test]
    async fn subscriptions_asking_together_download_a_block_once() {
        let shared = SharedMaterial::default();
        let cancellation = CancellationToken::new();
        let asked = Mutex::new(Vec::new());
        let gate = Notify::new();
        let (lane, run) = ([hash(1), hash(2)], [hash(2), hash(3)]);
        let (lane_material, run_material, ()) = tokio::join!(
            shared.get_or_fetch(&lane, &cancellation, |indices| {
                let bytes = answer(&asked, &lane, &indices);
                let gate = &gate;
                async move {
                    gate.notified().await;
                    Ok(bytes)
                }
            }),
            shared.get_or_fetch(&run, &cancellation, |indices| {
                let bytes = answer(&asked, &run, &indices);
                async move { Ok(bytes) }
            }),
            async {
                tokio::task::yield_now().await;
                gate.notify_one();
            },
        );
        assert_eq!(lane_material.expect("lane"), [1, 2]);
        assert_eq!(run_material.expect("run"), [2, 3]);
        assert_eq!(*asked.lock().expect("asked"), [vec![1, 2], vec![3]]);
    }

    #[tokio::test]
    async fn a_recent_block_is_not_downloaded_again() {
        let shared = SharedMaterial::default();
        let cancellation = CancellationToken::new();
        let asked = Mutex::new(Vec::new());
        for hashes in [vec![hash(1)], vec![hash(1), hash(2)]] {
            let material = shared
                .get_or_fetch(&hashes, &cancellation, |indices| {
                    let bytes = answer(&asked, &hashes, &indices);
                    async move { Ok(bytes) }
                })
                .await
                .expect("material");
            assert_eq!(material.len(), hashes.len());
        }
        assert_eq!(*asked.lock().expect("asked"), [vec![1], vec![2]]);
    }

    #[tokio::test]
    async fn a_waiting_subscription_downloads_what_another_gave_up() {
        let shared = SharedMaterial::default();
        let cancellation = CancellationToken::new();
        let asked = Mutex::new(Vec::new());
        let gate = Notify::new();
        let block = [hash(1)];
        let (failed, recovered, ()) = tokio::join!(
            shared.get_or_fetch(&block, &cancellation, |indices| {
                answer(&asked, &block, &indices);
                let gate = &gate;
                async move {
                    gate.notified().await;
                    Err(P2pError::Timeout {
                        component: "receipts",
                    })
                }
            }),
            shared.get_or_fetch(&block, &cancellation, |indices| {
                let bytes = answer(&asked, &block, &indices);
                async move { Ok(bytes) }
            }),
            async {
                tokio::task::yield_now().await;
                gate.notify_one();
            },
        );
        assert!(failed.is_err());
        assert_eq!(recovered.expect("recovered"), [1]);
        assert_eq!(*asked.lock().expect("asked"), [vec![1], vec![1]]);
    }

    #[tokio::test(start_paused = true)]
    async fn a_waiting_subscription_downloads_what_a_stalled_one_holds() {
        let shared = SharedMaterial::default();
        let cancellation = CancellationToken::new();
        let asked = Mutex::new(Vec::new());
        let block = [hash(1)];
        let started = tokio::time::Instant::now();
        let (stalled, (recovered, waited)) = tokio::join!(
            tokio::time::timeout(
                crate::LIVE_MATERIAL_TIMEOUT * 3,
                shared.get_or_fetch(&block, &cancellation, |indices| {
                    answer(&asked, &block, &indices);
                    std::future::pending()
                })
            ),
            async {
                let material = shared
                    .get_or_fetch(&block, &cancellation, |indices| {
                        let bytes = answer(&asked, &block, &indices);
                        async move { Ok(bytes) }
                    })
                    .await;
                (material, started.elapsed())
            },
        );
        assert!(stalled.is_err());
        assert_eq!(recovered.expect("recovered"), [1]);
        assert_eq!(waited, crate::LIVE_MATERIAL_TIMEOUT);
        assert_eq!(*asked.lock().expect("asked"), [vec![1], vec![1]]);
    }

    #[tokio::test]
    async fn only_the_most_recent_blocks_are_kept() {
        let shared = SharedMaterial::default();
        let cancellation = CancellationToken::new();
        let asked = Mutex::new(Vec::new());
        let blocks = u8::try_from(RECENT_BLOCKS).expect("small") + 1;
        for byte in (0..blocks).chain([blocks - 1, 0]) {
            let block = [hash(byte)];
            shared
                .get_or_fetch(&block, &cancellation, |indices| {
                    let bytes = answer(&asked, &block, &indices);
                    async move { Ok(bytes) }
                })
                .await
                .expect("material");
        }
        let asked = asked.lock().expect("asked");
        // The newest block was kept; the oldest left and is downloaded again.
        assert_eq!(asked.len(), usize::from(blocks) + 1);
        assert_eq!(asked.last(), Some(&vec![0]));
    }
}
