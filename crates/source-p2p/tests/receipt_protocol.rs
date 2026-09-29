//! Exercise the public probe across a real ETH handshake and wire decoder.
//! The peer advertises a newer receipt protocol than the session negotiates:
//! selecting a request from its advertised capabilities must not break receipt
//! acquisition. Dispatcher-only tests cannot catch a caller bypassing it.

use std::{net::SocketAddr, sync::Arc, time::Duration};

use alloy_consensus::{
    EthereumReceipt, EthereumTxEnvelope, Header, Signed, TxLegacy, TxReceipt as _, TxType,
    constants::EMPTY_OMMER_ROOT_HASH,
    proofs::{calculate_receipt_root, calculate_transaction_root},
};
use alloy_eips::{BlockHashOrNumber, eip2124::Head};
use alloy_primitives::Signature;
use futures::StreamExt as _;
use leani_primitives::{BlockHash, BlockNumber, BlockRange};
use leani_source_api::SourceBudget;
use leani_source_p2p::{RethP2pConfig, RethP2pSource, parse_trusted_peer};
use reth_chainspec::MAINNET;
use reth_ethereum_primitives::BlockBody;
use reth_network::{
    EthNetworkPrimitives, NetworkConfigBuilder, NetworkEvent, NetworkEventListenerProvider as _,
    NetworkInfo as _,
    config::rng_secret_key,
    eth_requests::IncomingEthRequest,
    types::{EthVersion, Receipts69},
};
use reth_tasks::Runtime;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn probe_receipts_use_the_negotiated_protocol() {
    let receipt = EthereumReceipt {
        tx_type: TxType::Legacy,
        success: true,
        cumulative_gas_used: 21_000,
        logs: Vec::new(),
    };
    let body = BlockBody {
        transactions: vec![EthereumTxEnvelope::Legacy(Signed::new_unhashed(
            TxLegacy {
                gas_limit: 21_000,
                ..TxLegacy::default()
            },
            Signature::test_signature(),
        ))],
        ..BlockBody::default()
    };
    let first = Header {
        number: 10,
        timestamp: 100,
        gas_used: 21_000,
        transactions_root: calculate_transaction_root(&body.transactions),
        receipts_root: calculate_receipt_root(&[receipt.with_bloom_ref()]),
        ommers_hash: EMPTY_OMMER_ROOT_HASH,
        ..Header::default()
    };
    let second = Header {
        number: 11,
        timestamp: 112,
        parent_hash: first.hash_slow(),
        ..first.clone()
    };
    let headers = Arc::new([first, second]);
    let range = BlockRange::new(BlockNumber(10), BlockNumber(11)).unwrap();
    let expected_tip = BlockHash::new(headers[1].hash_slow().0);

    let mut peer_config =
        NetworkConfigBuilder::<EthNetworkPrimitives>::new(rng_secret_key(), Runtime::test())
            .listener_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .disable_discovery()
            .disable_tx_gossip(true)
            .set_head(Head {
                number: 11,
                hash: headers[1].hash_slow(),
                timestamp: 112,
                ..Head::default()
            })
            .build_with_noop_provider(MAINNET.clone());
    peer_config.hello_message.protocols = vec![EthVersion::Eth69.into(), EthVersion::Eth70.into()];
    let (requests, mut incoming) = mpsc::channel(16);
    let peer = Box::pin(peer_config.manager())
        .await
        .unwrap()
        .with_eth_request_handler(requests);
    let handle = peer.handle().clone();
    let address = handle.local_addr();
    let record = format!("enode://{:x}@{address}", handle.peer_id());
    let mut events = handle.event_listener();
    let peer_task = tokio::spawn(peer);
    let served_bodies = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let bodies_seen = served_bodies.clone();
    let responder = tokio::spawn(async move {
        while let Some(request) = incoming.recv().await {
            match request {
                IncomingEthRequest::GetBlockHeaders {
                    request, response, ..
                } => {
                    let start = match request.start_block {
                        BlockHashOrNumber::Number(number) => number,
                        BlockHashOrNumber::Hash(hash) => headers
                            .iter()
                            .find(|header| header.hash_slow() == hash)
                            .map_or(u64::MAX, |header| header.number),
                    };
                    let selected = headers
                        .iter()
                        .filter(|header| header.number >= start)
                        .take(usize::try_from(request.limit).unwrap())
                        .cloned()
                        .collect::<Vec<_>>();
                    let _ = response.send(Ok(selected.into()));
                }
                IncomingEthRequest::GetBlockBodies {
                    request, response, ..
                } => {
                    let selected = request
                        .0
                        .iter()
                        .filter(|hash| headers.iter().any(|header| header.hash_slow() == **hash))
                        .map(|_| body.clone())
                        .collect::<Vec<_>>();
                    bodies_seen.fetch_add(selected.len(), std::sync::atomic::Ordering::Relaxed);
                    let _ = response.send(Ok(selected.into()));
                }
                IncomingEthRequest::GetReceipts69 {
                    request, response, ..
                } => {
                    let selected = request
                        .0
                        .iter()
                        .filter(|hash| headers.iter().any(|header| header.hash_slow() == **hash))
                        .map(|_| vec![receipt.clone()])
                        .collect();
                    let _ = response.send(Ok(Receipts69(selected)));
                }
                _ => panic!("unexpected request on the negotiated eth/69 session"),
            }
        }
    });
    let source = RethP2pSource::mainnet(RethP2pConfig {
        minimum_peers: 1,
        preferred_peers: 1,
        body_serving_peer_target: 1,
        max_outbound_peers: 1,
        max_concurrent_dials: 1,
        trusted_peers: vec![parse_trusted_peer(&record).unwrap()],
        enable_discv5: false,
        peer_wait_timeout: Duration::from_secs(10),
        request_timeout: Duration::from_secs(2),
        retries: 1,
        persistent_retries: false,
        ..RethP2pConfig::default()
    })
    .unwrap();
    let probe = source.probe_fixed_range(
        range,
        Some(expected_tip),
        SourceBudget {
            max_input_bytes: 1_000_000,
            max_frame_bytes: 1_000_000,
            max_frames: 2,
            max_buffered_frames: 2,
            max_in_flight_requests: 1,
            temporary_disk_bytes: 1,
            max_resident_bytes: 1_000_000,
        },
        CancellationToken::new(),
    );
    let negotiation = async {
        while let Some(event) = events.next().await {
            if let NetworkEvent::ActivePeerSession { info, .. } = event {
                return info.version;
            }
        }
        panic!("peer event stream closed before negotiation");
    };
    let result = tokio::time::timeout(Duration::from_secs(20), async {
        tokio::join!(probe, negotiation)
    })
    .await;
    source.shutdown().await;
    peer_task.abort();
    responder.abort();
    let _ = peer_task.await;
    let _ = responder.await;
    let (result, negotiated) = result.expect("bounded loopback probe");
    assert_eq!(negotiated, EthVersion::Eth69);
    assert!(
        served_bodies.load(std::sync::atomic::Ordering::Relaxed) >= 2,
        "the probe must reach receipt acquisition"
    );
    let result = result.expect("receipts must use eth/69 despite the peer advertising eth/70");
    assert_eq!(result.metrics.receipts, 2);
    assert_eq!(result.frames.len(), 2);
    assert!(
        result
            .frames
            .iter()
            .all(|frame| frame.receipts.as_complete().is_some_and(|r| r.len() == 1))
    );
}
