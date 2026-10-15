//! Read-time field-group checks for stored material.

use futures::{StreamExt, future};
use leani_primitives::{BlockRange, CapabilitySet};

use crate::{BlockFrameStream, SourceError};

/// End `frames` at the first frame lacking a field the `required` groups
/// promise, with [`SourceError::MissingMaterial`] for the rest of `range`.
///
/// Stored material records which components it holds, not which fields: a
/// segment captured for a narrower request plans like any other, so its frames
/// are checked as they are read. The runtime passes the rest of the range to
/// the next source.
#[must_use = "streams must be polled to read and validate stored frames"]
pub fn stop_at_unfilled_groups(
    frames: BlockFrameStream,
    required: CapabilitySet,
    range: BlockRange,
    available: CapabilitySet,
) -> BlockFrameStream {
    let groups = required.with_derivable();
    Box::pin(frames.scan(false, move |ended, item| {
        if *ended {
            return future::ready(None);
        }
        let item = item.and_then(|frame| {
            if frame.group_violation(groups).is_none() {
                return Ok(frame);
            }
            *ended = true;
            Err(SourceError::MissingMaterial {
                gaps: vec![BlockRange::new(frame.block.number, range.end()).unwrap_or(range)],
                required: required.bits(),
                available: available.bits(),
            })
        });
        future::ready(Some(item))
    }))
}

#[cfg(test)]
mod tests {
    use futures::stream;
    use leani_primitives::{
        BlockFrame, BlockHash, BlockNumber, BlockRef, Capability, ChainId, Finality, Material,
        MissingReason, TransactionEnvelope, TransactionHash, VerificationReport,
    };

    use super::*;

    fn frame(number: u64, from: Option<leani_primitives::Address>) -> BlockFrame {
        BlockFrame {
            chain_id: ChainId(1),
            block: BlockRef {
                number: BlockNumber(number),
                hash: BlockHash::new([u8::try_from(number).unwrap_or(u8::MAX); 32]),
                parent_hash: BlockHash::ZERO,
                timestamp: number,
            },
            finality: Finality::Finalized,
            header: Material::Missing(MissingReason::NotRequested),
            transactions: Material::Complete(vec![TransactionEnvelope {
                hash: TransactionHash::new([7; 32]),
                transaction_type: 2,
                index: 0,
                encoded: None,
                from,
                to: None,
                nonce: Some(0),
                gas_limit: Some(21_000),
                value: Some(leani_primitives::Quantity::new([0; 32])),
                input: None,
                max_fee_per_gas: None,
                max_priority_fee_per_gas: None,
                max_fee_per_blob_gas: None,
                blob_versioned_hashes: Vec::new(),
                size_bytes: None,
            }]),
            receipts: Material::Missing(MissingReason::NotRequested),
            logs: Material::Missing(MissingReason::NotRequested),
            withdrawals: Material::Missing(MissingReason::NotRequested),
            blob_sidecars: Material::Missing(MissingReason::NotRequested),
            traces: Material::Missing(MissingReason::NotRequested),
            state_diffs: Material::Missing(MissingReason::NotRequested),
            provenance: Vec::new(),
            verification: VerificationReport::default(),
        }
    }

    #[tokio::test]
    async fn stored_frames_end_at_the_first_unfilled_group() {
        let sender = Some(leani_primitives::Address::new([1; 20]));
        let range = BlockRange::new(BlockNumber(1), BlockNumber(4)).expect("range");
        let stored: BlockFrameStream = Box::pin(stream::iter(
            [frame(1, sender), frame(2, None), frame(3, sender)].map(Ok),
        ));
        let required = CapabilitySet::of(Capability::Body).with(Capability::Senders);
        let available = CapabilitySet::of(Capability::Transactions);
        let read = stop_at_unfilled_groups(stored, required, range, available)
            .collect::<Vec<_>>()
            .await;
        assert_eq!(read.len(), 2, "{read:?}");
        assert_eq!(
            read[0].as_ref().expect("covered frame").block.number,
            BlockNumber(1)
        );
        assert_eq!(
            read[1].as_ref().expect_err("uncovered frame"),
            &SourceError::MissingMaterial {
                gaps: vec![BlockRange::new(BlockNumber(2), BlockNumber(4)).expect("rest")],
                required: required.bits(),
                available: available.bits(),
            }
        );

        // a request that reads no senders keeps every frame
        let stored: BlockFrameStream =
            Box::pin(stream::iter([frame(1, sender), frame(2, None)].map(Ok)));
        let body = CapabilitySet::of(Capability::Body);
        let read = stop_at_unfilled_groups(stored, body, range, available)
            .collect::<Vec<_>>()
            .await;
        assert!(read.iter().all(Result::is_ok), "{read:?}");
    }
}
