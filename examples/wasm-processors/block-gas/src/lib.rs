//! Gas used, gas limit, utilization, and base fee of every block.
//!
//! Block-local, keyed by block number, and needs only block headers, which
//! a node following blocks fetches.

use leani_wasm_guest::{
    Block, BlockRef, Capability, Error, Manifest, Processor, Store, U256, export_processor,
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct Params {
    #[serde(default)]
    min_utilization_percent: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Gas {
    gas_used: u64,
    gas_limit: u64,
    /// Hundredths of a percent, so 9_731 is 97.31%.
    utilization_basis_points: u64,
    base_fee_per_gas: Option<U256>,
}

pub struct BlockGas;

impl Processor for BlockGas {
    type Params = Params;
    type Delta = Option<Gas>;

    fn manifest(_: &Params) -> Result<Manifest, Error> {
        Ok(Manifest::new("block-gas").require(Capability::Header))
    }

    fn map(params: &Params, block: &Block) -> Result<Option<Gas>, Error> {
        let header = block.header.as_ref().ok_or("the block has no header")?;
        let (Some(gas_used), Some(gas_limit)) = (header.gas_used, header.gas_limit) else {
            return Ok(None);
        };
        let utilization_basis_points = gas_used
            .saturating_mul(10_000)
            .checked_div(gas_limit)
            .unwrap_or(0);
        if utilization_basis_points < params.min_utilization_percent.saturating_mul(100) {
            return Ok(None);
        }
        Ok(Some(Gas {
            gas_used,
            gas_limit,
            utilization_basis_points,
            base_fee_per_gas: header.base_fee_per_gas.map(U256),
        }))
    }

    fn reduce(
        _: &Params,
        block: &BlockRef,
        gas: Option<Gas>,
        store: &mut Store,
    ) -> Result<(), Error> {
        let Some(gas) = gas else {
            return Ok(());
        };
        store.upsert(
            "blocks",
            "block.gas",
            &block.number.to_be_bytes(),
            &Entity {
                number: block.number,
                timestamp: block.timestamp,
                gas,
            },
        )
    }
}

#[derive(Serialize)]
struct Entity {
    number: u64,
    timestamp: u64,
    #[serde(flatten)]
    gas: Gas,
}

export_processor!(BlockGas);
