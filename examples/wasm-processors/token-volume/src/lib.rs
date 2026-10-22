//! Running ERC-20 transfer count and volume per token.
//!
//! Ordered: each block adds to totals earlier blocks wrote, so blocks reduce
//! in chain order and an undo restores the previous totals.
//!
//! Parameters: `{ "tokens": ["0x…"] }`; empty watches every contract.

use std::collections::BTreeMap;

use leani_wasm_guest::{
    Address, Block, BlockRef, Error, Manifest, Processor, Store, U256, Word, export_processor, hex,
    keccak256, parse_hex,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Params {
    #[serde(default)]
    tokens: Vec<String>,
}

#[derive(Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Volume {
    token: String,
    transfers: u64,
    volume: U256,
    last_block: u64,
}

pub struct TokenVolume;

impl Processor for TokenVolume {
    type Params = Params;
    /// One `(token, value)` per transfer, in log order.
    type Delta = Vec<(Address, Word)>;

    fn manifest(params: &Params) -> Result<Manifest, Error> {
        let tokens = params
            .tokens
            .iter()
            .map(|token| parse_hex::<20>(token))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Manifest::new("token-volume")
            .ordered()
            .require(leani_wasm_guest::Capability::Logs)
            .addresses(tokens)
            .topic(0, [keccak256(b"Transfer(address,address,uint256)")]))
    }

    fn map(_: &Params, block: &Block) -> Result<Self::Delta, Error> {
        Ok(block
            .logs
            .as_deref()
            .unwrap_or_default()
            .iter()
            // Three topics and a one-word value: an ERC-20 transfer.
            .filter(|log| log.topics.len() == 3)
            .filter_map(|log| Some((log.address, Word::try_from(log.data.as_slice()).ok()?)))
            .collect())
    }

    fn reduce(
        _: &Params,
        block: &BlockRef,
        transfers: Self::Delta,
        store: &mut Store,
    ) -> Result<(), Error> {
        let mut block_totals = BTreeMap::<Address, (u64, U256)>::new();
        for (token, value) in transfers {
            let (count, volume) = block_totals.entry(token).or_default();
            *count += 1;
            *volume = volume.saturating_add(U256(value));
        }
        for (token, (count, volume)) in block_totals {
            let mut total = store
                .get_json::<Volume>("volumes", &token)?
                .unwrap_or_else(|| Volume {
                    token: hex(&token),
                    ..Volume::default()
                });
            total.transfers += count;
            total.volume = total.volume.saturating_add(volume);
            total.last_block = block.number;
            store.upsert("volumes", "erc20.volume", &token, &total)?;
        }
        Ok(())
    }
}

export_processor!(TokenVolume);
