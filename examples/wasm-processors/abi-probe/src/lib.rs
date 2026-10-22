//! Test fixture for the Leani Wasm host: `{"mode": …}` selects a behavior
//! that exercises one host guarantee. Not an example to copy.

use leani_wasm_guest::{
    Block, BlockRef, Capability, Error, Manifest, Processor, Store, export_processor, log,
    serde_json::{Value, json},
};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Params {
    mode: String,
}

pub struct Probe;

impl Processor for Probe {
    type Params = Params;
    type Delta = Vec<u8>;

    fn manifest(params: &Params) -> Result<Manifest, Error> {
        let manifest = Manifest::new("abi-probe").require(Capability::Header);
        Ok(match params.mode.as_str() {
            "state" | "replay" => manifest.ordered(),
            "logs" => manifest.require(Capability::Logs),
            "bad-manifest" => Manifest::new("not a portable name"),
            "describe-panic" => panic!("probe describe panic"),
            _ => manifest,
        })
    }

    fn map(params: &Params, block: &Block) -> Result<Vec<u8>, Error> {
        match params.mode.as_str() {
            "spin" => loop {
                std::hint::black_box(block.number);
            },
            "grow" => {
                // Fits only when the host lets memory grow past 128 MiB.
                std::hint::black_box(vec![1_u8; 128 << 20]);
                Ok(Vec::new())
            }
            "panic" => panic!("probe map panic at block {}", block.number),
            "fail" => Err(Error::new("probe refused the block")),
            "floats" => {
                // NaN payloads differ across CPUs unless the host
                // canonicalizes them.
                #[expect(
                    clippy::neg_multiply,
                    reason = "arithmetic on NaN, which negation is not"
                )]
                let nan = std::hint::black_box(f64::NAN) * -1.0;
                let ratio = std::hint::black_box(f64::from(
                    u32::try_from(block.number % 1000).unwrap_or(0),
                )) / 7.0;
                Ok([
                    (ratio.sqrt() + nan).to_bits().to_be_bytes(),
                    ratio.to_bits().to_be_bytes(),
                ]
                .concat())
            }
            "logs" => Ok(serde_json_bytes(&json!({
                "logs": block.logs.as_ref().map(|logs| logs.iter().map(|log| log.log_index).collect::<Vec<_>>()),
                "transactions": block.transactions.is_some(),
                "header": block.header.as_ref().map(|header| header.gas_used),
            }))),
            _ => {
                log(&format!("mapping block {}", block.number));
                Ok(block.number.to_be_bytes().to_vec())
            }
        }
    }

    fn reduce(
        params: &Params,
        block: &BlockRef,
        delta: Vec<u8>,
        store: &mut Store,
    ) -> Result<(), Error> {
        let key = block.number.to_be_bytes();
        match params.mode.as_str() {
            "read-block-local" => {
                let _ = store.get("blocks", &key);
                Ok(())
            }
            "bad-entity" => {
                store.put("blocks", &key, b"not json");
                Ok(())
            }
            "state" => {
                // Read-your-writes within one block, private state, scans,
                // and indexes, then one change per key although two emitted.
                let previous = store.state_get("totals", b"count").map_or(0, |bytes| {
                    u64::from_be_bytes(bytes.try_into().unwrap_or_default())
                });
                store.state_put("totals", b"count", &(previous + 1).to_be_bytes());
                let reread = store
                    .state_get("totals", b"count")
                    .ok_or("lost a state write")?;
                if reread != (previous + 1).to_be_bytes() {
                    return Err(Error::new("read did not see the block's own write"));
                }
                let scanned = store.state_scan_prefix("totals", b"c", 10)?;
                store.index_put(
                    "by-parity",
                    &[u8::from(block.number.is_multiple_of(2))],
                    &key,
                );
                store.emit_upsert("probe.total", b"total", &json!({ "count": previous }))?;
                store.upsert(
                    "totals",
                    "probe.total",
                    b"total",
                    &json!({ "count": previous + 1, "scanned": scanned.len(), "delta": delta.len() }),
                )
            }
            "replay" => {
                let seen = store.get_json::<Value>("totals", b"total")?;
                store.upsert(
                    "totals",
                    "probe.total",
                    b"total",
                    &json!({ "block": block.number, "previous": seen }),
                )
            }
            _ => store.upsert(
                "blocks",
                "probe.block",
                &key,
                &json!({ "number": block.number, "delta": delta }),
            ),
        }
    }
}

fn serde_json_bytes(value: &Value) -> Vec<u8> {
    value.to_string().into_bytes()
}

export_processor!(Probe);
