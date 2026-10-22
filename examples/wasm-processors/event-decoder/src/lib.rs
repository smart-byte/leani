//! Decode any contract event from its Solidity signature: no code to write.
//!
//! Parameters:
//!
//! ```json
//! {
//!   "addresses": ["0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"],
//!   "events": ["Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)"]
//! }
//! ```
//!
//! `addresses` is optional: without it, every contract emitting one of the
//! events matches. `"withTransactionHash": true` adds each event's
//! transaction hash, which the node's sources must then supply. Supported
//! argument types: `address`, `bool`, `uint<N>`, `int<N>`, `bytes<N>`,
//! `bytes`, and `string`; an indexed `bytes` or `string` decodes to its hash.
//! Block-local: each event is keyed by its block number and log index.

use leani_wasm_guest::{
    Address, Block, BlockRef, Error, Hash, Manifest, Processor, Store, export_processor, hex,
    parse_hex,
    serde_json::{Map, Value},
    solidity::{self, Signature},
};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Params {
    #[serde(default)]
    addresses: Vec<String>,
    events: Vec<String>,
    #[serde(default, rename = "withTransactionHash")]
    with_transaction_hash: bool,
}

#[derive(Deserialize, Serialize)]
pub struct Decoded {
    contract: Address,
    /// Index into the configured events.
    event: u16,
    transaction_hash: Option<Hash>,
    transaction_index: u32,
    log_index: u32,
    /// The decoded arguments, in declaration order.
    args: Vec<solidity::Value>,
}

pub struct EventDecoder;

impl Processor for EventDecoder {
    type Params = Params;
    type Delta = Vec<Decoded>;

    fn manifest(params: &Params) -> Result<Manifest, Error> {
        let events = parse_events(params)?;
        let addresses = params
            .addresses
            .iter()
            .map(|address| parse_hex::<20>(address))
            .collect::<Result<Vec<_>, _>>()?;
        let manifest = Manifest::new("event-decoder")
            .require(leani_wasm_guest::Capability::Logs)
            .addresses(addresses)
            .topic(0, events.iter().map(Signature::topic));
        Ok(if params.with_transaction_hash {
            manifest.logs_with_transaction_hash()
        } else {
            manifest
        })
    }

    fn map(params: &Params, block: &Block) -> Result<Vec<Decoded>, Error> {
        let events = parse_events(params)?;
        let topics = events.iter().map(Signature::topic).collect::<Vec<_>>();
        let mut decoded = Vec::new();
        for log in block.logs.as_deref().unwrap_or_default() {
            let Some(index) = log
                .topics
                .first()
                .and_then(|topic| topics.iter().position(|candidate| candidate == topic))
            else {
                continue;
            };
            // Another contract may emit the same topic with another layout:
            // a log that does not decode is not this event.
            let Some(values) = events[index].decode_event(&log.topics[1..], &log.data) else {
                continue;
            };
            decoded.push(Decoded {
                contract: log.address,
                event: u16::try_from(index).map_err(|_| "too many events")?,
                transaction_hash: log.transaction_hash,
                transaction_index: log.transaction_index,
                log_index: log.log_index,
                args: values,
            });
        }
        Ok(decoded)
    }

    fn reduce(
        params: &Params,
        block: &BlockRef,
        decoded: Vec<Decoded>,
        store: &mut Store,
    ) -> Result<(), Error> {
        let events = parse_events(params)?;
        for log in decoded {
            let event = events
                .get(usize::from(log.event))
                .ok_or("a delta names an unknown event")?;
            let mut args = Map::new();
            for (input, value) in event.inputs.iter().zip(&log.args) {
                args.insert(input.name.clone(), value.to_json());
            }
            // A log index is unique within its block.
            let mut key = block.number.to_be_bytes().to_vec();
            key.extend_from_slice(&log.log_index.to_be_bytes());
            store.upsert(
                "events",
                &format!("event.{}", event.name),
                &key,
                &Entity {
                    contract: hex(&log.contract),
                    event: &event.name,
                    signature: &event.canonical,
                    block_number: block.number,
                    timestamp: block.timestamp,
                    transaction_hash: log.transaction_hash.map(|hash| hex(&hash)),
                    transaction_index: log.transaction_index,
                    log_index: log.log_index,
                    args,
                },
            )?;
        }
        Ok(())
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Entity<'a> {
    contract: String,
    event: &'a str,
    signature: &'a str,
    block_number: u64,
    timestamp: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    transaction_hash: Option<String>,
    transaction_index: u32,
    log_index: u32,
    args: Map<String, Value>,
}

fn parse_events(params: &Params) -> Result<Vec<Signature>, Error> {
    if params.events.is_empty() || params.events.len() > 32 {
        return Err(Error::new("give between 1 and 32 events"));
    }
    params
        .events
        .iter()
        .map(|text| Signature::parse(text))
        .collect()
}

export_processor!(EventDecoder);
