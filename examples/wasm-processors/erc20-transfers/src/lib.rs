//! ERC-20 transfers, optionally of chosen tokens and above a minimum value.
//!
//! Block-local: each transfer is keyed by its transaction hash and log index,
//! which no other block shares, so blocks reduce independently.
//!
//! Parameters (all optional):
//!
//! ```json
//! { "tokens": ["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"], "minValue": "1000000000" }
//! ```

use leani_wasm_guest::{
    Address, Block, BlockRef, Error, Hash, Manifest, Processor, Store, U256, Word,
    export_processor, hex, keccak256, parse_hex,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Params {
    /// Token contracts to watch; empty watches every contract.
    #[serde(default)]
    tokens: Vec<String>,
    /// Smallest transfer to keep, in the token's base units.
    #[serde(default)]
    min_value: Option<U256>,
}

#[derive(Deserialize, Serialize)]
pub struct Transfer {
    token: Address,
    from: Address,
    to: Address,
    value: Word,
    transaction_hash: Hash,
    log_index: u32,
}

pub struct Transfers;

impl Processor for Transfers {
    type Params = Params;
    type Delta = Vec<Transfer>;

    fn manifest(params: &Params) -> Result<Manifest, Error> {
        let tokens = params
            .tokens
            .iter()
            .map(|token| parse_hex::<20>(token))
            .collect::<Result<Vec<_>, _>>()?;
        // The node filters logs by contract and topic before the guest sees
        // them, whatever the source delivered.
        Ok(Manifest::new("erc20-transfers")
            .logs_with_transaction_hash()
            .addresses(tokens)
            .topic(0, [keccak256(b"Transfer(address,address,uint256)")]))
    }

    fn map(params: &Params, block: &Block) -> Result<Vec<Transfer>, Error> {
        let mut transfers = Vec::new();
        for log in block.logs.as_deref().unwrap_or_default() {
            // ERC-721 shares the signature with three indexed arguments and
            // no data; other layouts are not ERC-20 transfers, so skip them.
            let [_, from, to] = log.topics.as_slice() else {
                continue;
            };
            let (Some(from), Some(to), Ok(value)) = (
                address(from),
                address(to),
                Word::try_from(log.data.as_slice()),
            ) else {
                continue;
            };
            if params
                .min_value
                .is_some_and(|minimum| U256(value) < minimum)
            {
                continue;
            }
            transfers.push(Transfer {
                token: log.address,
                from,
                to,
                value,
                transaction_hash: log
                    .transaction_hash
                    .ok_or("a transfer log lacks its transaction hash")?,
                log_index: log.log_index,
            });
        }
        Ok(transfers)
    }

    fn reduce(
        _: &Params,
        block: &BlockRef,
        transfers: Vec<Transfer>,
        store: &mut Store,
    ) -> Result<(), Error> {
        for transfer in transfers {
            let mut key = transfer.transaction_hash.to_vec();
            key.extend_from_slice(&transfer.log_index.to_be_bytes());
            store.upsert(
                "transfers",
                "erc20.transfer",
                &key,
                &Entity {
                    token: hex(&transfer.token),
                    from: hex(&transfer.from),
                    to: hex(&transfer.to),
                    value: U256(transfer.value),
                    transaction_hash: hex(&transfer.transaction_hash),
                    log_index: transfer.log_index,
                    block_number: block.number,
                    timestamp: block.timestamp,
                },
            )?;
        }
        Ok(())
    }
}

/// The public entity and change payload.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Entity {
    token: String,
    from: String,
    to: String,
    value: U256,
    transaction_hash: String,
    log_index: u32,
    block_number: u64,
    timestamp: u64,
}

/// The address in an indexed argument, if its ABI padding is zero.
fn address(topic: &Hash) -> Option<Address> {
    let (padding, address) = topic.split_at(12);
    padding
        .iter()
        .all(|byte| *byte == 0)
        .then(|| Address::try_from(address).ok())
        .flatten()
}

export_processor!(Transfers);
