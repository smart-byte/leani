use leani_finality_consensus_p2p::{ConsensusP2pConfig, VerifiedConsensusP2p};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();
    let mut arguments = std::env::args().skip(1);
    let encoded = arguments.next().ok_or(
        "usage: mainnet_probe 0x<recent-finalized-beacon-root> <beacon-slot> [bootnode-enr ...]",
    )?;
    let checkpoint_slot = arguments
        .next()
        .ok_or("missing finalized beacon slot")?
        .parse::<u64>()?;
    let encoded = encoded
        .strip_prefix("0x")
        .ok_or("checkpoint root must start with 0x")?;
    let mut checkpoint = [0_u8; 32];
    hex::decode_to_slice(encoded, &mut checkpoint)?;
    let mut config = ConsensusP2pConfig::default();
    let bootnodes = arguments.collect::<Vec<_>>();
    if !bootnodes.is_empty() {
        config.bootnodes = bootnodes;
    }
    let source = VerifiedConsensusP2p::mainnet(config)?;
    let report = source.probe_checkpoint(checkpoint, checkpoint_slot).await;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.accepted {
        return Err("consensus P2P probe was not accepted".into());
    }
    Ok(())
}
