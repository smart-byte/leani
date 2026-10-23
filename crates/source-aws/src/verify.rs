//! Rebuilding a block's commitments from its rows.

use alloy_primitives::{B256, Bloom, Log, LogData, b256};
use alloy_rlp::Encodable;

use crate::read::{BlockRow, LogRow, ReceiptRow};

/// The requests hash every Prague (Isthmus) header commits to on chains
/// whose blocks carry no EIP-7685 requests. The exports have no column for
/// it, so a rebuilt header tries it after none.
const EMPTY_REQUESTS_HASH: B256 =
    b256!("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");

const OP_DEPOSIT: u8 = 0x7e;

/// Whether the rebuilt header hashes to the row's block hash, or `None`
/// when the layout cannot rebuild it.
pub(crate) fn header_hash_matches(block: &BlockRow) -> Option<bool> {
    let header = block.header.as_ref()?;
    // Fork-specific fields absent from the row are absent from the header;
    // only the requests hash has no column to tell.
    let without = header.hash_slow() == block.hash;
    Some(
        without
            || (header.withdrawals_root.is_some() && {
                let mut prague = header.clone();
                prague.requests_hash = Some(EMPTY_REQUESTS_HASH);
                prague.hash_slow() == block.hash
            }),
    )
}

/// The consensus encoding of a receipt and the bloom of its logs.
/// `logs` are the receipt's own, in log-index order.
pub(crate) fn encode_receipt(receipt: &ReceiptRow, kind: u8, logs: &[LogRow]) -> (Vec<u8>, Bloom) {
    let logs = logs
        .iter()
        .map(|log| Log {
            address: log.address,
            data: LogData::new_unchecked(log.topics.clone(), log.data.clone()),
        })
        .collect::<Vec<_>>();
    let mut bloom = Bloom::ZERO;
    for log in &logs {
        bloom.accrue_log(log);
    }
    let success = receipt.success.unwrap_or(false);
    // A deposit receipt hashes its nonce and version only from Canyon on,
    // which added the version: before, the nonce is stored but not hashed.
    let (nonce, version) = if kind == OP_DEPOSIT && receipt.deposit_version.is_some() {
        (receipt.deposit_nonce, receipt.deposit_version)
    } else {
        (None, None)
    };
    let payload_length = success.length()
        + receipt.cumulative_gas.length()
        + bloom.length()
        + logs.length()
        + nonce.map_or(0, |nonce| nonce.length())
        + version.map_or(0, |version| version.length());
    let mut encoded = Vec::with_capacity(payload_length + 8);
    if kind != 0 {
        encoded.push(kind);
    }
    alloy_rlp::Header {
        list: true,
        payload_length,
    }
    .encode(&mut encoded);
    success.encode(&mut encoded);
    receipt.cumulative_gas.encode(&mut encoded);
    bloom.encode(&mut encoded);
    logs.encode(&mut encoded);
    if let Some(nonce) = nonce {
        nonce.encode(&mut encoded);
    }
    if let Some(version) = version {
        version.encode(&mut encoded);
    }
    (encoded, bloom)
}

pub(crate) fn receipts_root(encoded: &[Vec<u8>]) -> B256 {
    alloy_trie::root::ordered_trie_root_with_encoder(encoded, |receipt, out| {
        out.extend_from_slice(receipt);
    })
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Bytes, address};

    use super::*;

    fn receipt(cumulative_gas: u64) -> ReceiptRow {
        ReceiptRow {
            number: 1,
            index: 0,
            hash: B256::ZERO,
            transaction_type: None,
            success: Some(true),
            cumulative_gas,
            gas_used: None,
            effective_gas_price: None,
            deposit_nonce: Some(7),
            deposit_version: Some(1),
        }
    }

    #[test]
    fn receipts_encode_like_alloy() {
        let log = LogRow {
            number: 1,
            transaction_index: 0,
            log_index: 0,
            address: address!("4200000000000000000000000000000000000006"),
            topics: vec![B256::repeat_byte(1)],
            data: Bytes::from_static(&[1, 2, 3]),
        };
        let (encoded, bloom) = encode_receipt(&receipt(21_000), 2, std::slice::from_ref(&log));
        let alloy = alloy_consensus::ReceiptEnvelope::Eip1559(alloy_consensus::ReceiptWithBloom {
            receipt: alloy_consensus::Receipt {
                status: true.into(),
                cumulative_gas_used: 21_000,
                logs: vec![Log {
                    address: log.address,
                    data: LogData::new_unchecked(log.topics.clone(), log.data.clone()),
                }],
            },
            logs_bloom: bloom,
        });
        let mut expected = Vec::new();
        alloy_eips::eip2718::Encodable2718::encode_2718(&alloy, &mut expected);
        assert_eq!(encoded, expected);
        // The deposit fields ride only on deposit receipts.
        let (legacy, _) = encode_receipt(&receipt(21_000), 0, &[]);
        assert_eq!(legacy[0] & 0xc0, 0xc0, "legacy receipts carry no type byte");
        let (deposit, _) = encode_receipt(&receipt(21_000), OP_DEPOSIT, &[]);
        assert_eq!(deposit[0], OP_DEPOSIT);
        assert!(deposit.len() > legacy.len() + 1);
        let mut regolith = receipt(21_000);
        regolith.deposit_version = None;
        let (regolith, _) = encode_receipt(&regolith, OP_DEPOSIT, &[]);
        assert_eq!(
            regolith[1..],
            legacy[..],
            "pre-Canyon deposits hash no nonce"
        );
    }
}
