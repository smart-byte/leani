//! A rollup's finality from Ethereum: the newest block whose data is final
//! there, and the newest block its settlement contracts accepted.

use futures::future::Either;
use leani_primitives::{BlockHash, BlockNumber};
use tokio::sync::watch;

use crate::{FinalityEvent, SourceError};

/// A rollup block a finality lane names; `None` until it names one.
pub type RollupBlock = Option<(BlockNumber, BlockHash)>;

/// The blocks a rollup's finality lanes name as they move. Either may be
/// absent: a node that reads no batches finalizes blocks as they settle, and
/// one without an L1 anchor settles none.
#[derive(Clone, Debug, Default)]
pub struct RollupFinality {
    pub finalized: Option<watch::Receiver<RollupBlock>>,
    pub settled: Option<watch::Receiver<RollupBlock>>,
}

impl RollupFinality {
    /// The same blocks for a new subscriber, who hears the current ones
    /// first.
    #[must_use]
    pub fn subscribe(&self) -> Self {
        let mark = |watch: &Option<watch::Receiver<RollupBlock>>| {
            watch.clone().map(|mut watch| {
                watch.mark_changed();
                watch
            })
        };
        Self {
            finalized: mark(&self.finalized),
            settled: mark(&self.settled),
        }
    }

    /// The next block either lane names, as a finality event; a finalized
    /// rollup block carries its number as the beacon slot and a zero root.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::Disconnected`] once a lane is gone.
    pub async fn next(&mut self) -> Result<FinalityEvent, SourceError> {
        loop {
            let finalized = std::pin::pin!(moved(&mut self.finalized));
            let settled = std::pin::pin!(moved(&mut self.settled));
            match futures::future::select(finalized, settled).await {
                Either::Left((moved, _)) => {
                    if let Some((number, hash)) = moved? {
                        return Ok(FinalityEvent::Finalized {
                            block_number: number,
                            block_hash: hash,
                            beacon_slot: number.0,
                            beacon_block_root: [0; 32],
                        });
                    }
                }
                Either::Right((moved, _)) => {
                    if let Some((number, hash)) = moved? {
                        return Ok(FinalityEvent::Settled {
                            block_number: number,
                            block_hash: hash,
                        });
                    }
                }
            }
        }
    }
}

/// The block `watch` names once it changes; never for an absent watch.
async fn moved(
    watch: &mut Option<watch::Receiver<RollupBlock>>,
) -> Result<RollupBlock, SourceError> {
    let Some(watch) = watch else {
        return std::future::pending().await;
    };
    watch
        .changed()
        .await
        .map_err(|_| SourceError::Disconnected("the rollup's L1 finality ended".to_owned()))?;
    Ok(*watch.borrow_and_update())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn each_lane_names_its_own_level() {
        let (finalized, finalized_watch) = watch::channel(None);
        let (settled, settled_watch) =
            watch::channel(Some((BlockNumber(3), BlockHash::new([3; 32]))));
        let mut rollup = RollupFinality {
            finalized: Some(finalized_watch),
            settled: Some(settled_watch),
        }
        .subscribe();
        // A new subscriber hears the settled block it missed.
        assert_eq!(
            rollup.next().await.expect("event"),
            FinalityEvent::Settled {
                block_number: BlockNumber(3),
                block_hash: BlockHash::new([3; 32]),
            }
        );
        finalized.send_replace(Some((BlockNumber(9), BlockHash::new([9; 32]))));
        assert_eq!(
            rollup.next().await.expect("event"),
            FinalityEvent::Finalized {
                block_number: BlockNumber(9),
                block_hash: BlockHash::new([9; 32]),
                beacon_slot: 9,
                beacon_block_root: [0; 32],
            }
        );
        drop(settled);
        assert!(matches!(
            rollup.next().await,
            Err(SourceError::Disconnected(_))
        ));
    }
}
