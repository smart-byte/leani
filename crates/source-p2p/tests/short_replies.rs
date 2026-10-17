//! eth/68 and eth/69 peers stop a reply at their response size limit, so a
//! body or receipt request may come back with only its first blocks. Exercise
//! such replies across a real ETH session through the public probe, which uses
//! the same batched body and receipt requests as history and the live lane.

use std::{
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use alloy_consensus::{
    EthereumReceipt, EthereumTxEnvelope, Header, Signed, TxLegacy, TxReceipt as _, TxType,
    constants::EMPTY_OMMER_ROOT_HASH,
    proofs::{calculate_receipt_root, calculate_transaction_root},
};
use alloy_eips::{BlockHashOrNumber, eip2124::Head};
use alloy_primitives::{B256, Signature};
use leani_primitives::{BlockHash, BlockNumber, BlockRange};
use leani_source_api::SourceBudget;
use leani_source_p2p::{
    P2pError, P2pProbeResult, RethP2pConfig, RethP2pSource, parse_trusted_peer,
};
use reth_chainspec::MAINNET;
use reth_ethereum_primitives::BlockBody;
use reth_network::{
    EthNetworkPrimitives, NetworkConfigBuilder, NetworkInfo as _,
    config::rng_secret_key,
    eth_requests::IncomingEthRequest,
    types::{EthVersion, Receipts69},
};
use reth_tasks::Runtime;
use tokio::{sync::mpsc, task::JoinHandle};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Kind {
    Bodies,
    Receipts,
}

/// The peer's answer to a request for some blocks.
#[derive(Clone, Copy)]
enum Reply {
    /// The first `n` requested blocks.
    Blocks(usize),
    /// The first `n` requested bodies, the first of them without its
    /// transaction.
    WrongFirstBody(usize),
}

/// A loopback eth/69 peer serving blocks 10.. with one transaction each.
struct LoopbackPeer {
    record: String,
    range: BlockRange,
    tip: BlockHash,
    /// Blocks asked for by each body or receipt request, in arrival order.
    requests: Arc<Mutex<Vec<(Kind, usize)>>>,
    tasks: [JoinHandle<()>; 2],
}

/// Blocks 10.. with one transaction each, and that transaction's body and
/// receipt, which every block shares.
fn chain(blocks: u64) -> (Vec<Header>, BlockBody, EthereumReceipt) {
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
    let mut headers = Vec::<Header>::new();
    for number in 10..10 + blocks {
        headers.push(Header {
            number,
            timestamp: 100 + (number - 10) * 12,
            gas_used: 21_000,
            parent_hash: headers.last().map_or(B256::ZERO, Header::hash_slow),
            transactions_root: calculate_transaction_root(&body.transactions),
            receipts_root: calculate_receipt_root(&[receipt.with_bloom_ref()]),
            ommers_hash: EMPTY_OMMER_ROOT_HASH,
            ..Header::default()
        });
    }
    (headers, body, receipt)
}

impl LoopbackPeer {
    /// `reply(kind, nth, requested)` answers the peer's `nth` request of
    /// `kind`, counted from zero, for `requested` blocks.
    async fn start(blocks: u64, reply: fn(Kind, usize, usize) -> Reply) -> Self {
        let (headers, body, receipt) = chain(blocks);
        let last = headers.last().expect("at least one block").clone();
        let mut config =
            NetworkConfigBuilder::<EthNetworkPrimitives>::new(rng_secret_key(), Runtime::test())
                .listener_addr("127.0.0.1:0".parse::<SocketAddr>().unwrap())
                .disable_discovery()
                .disable_tx_gossip(true)
                .set_head(Head {
                    number: last.number,
                    hash: last.hash_slow(),
                    timestamp: last.timestamp,
                    ..Head::default()
                })
                .build_with_noop_provider(MAINNET.clone());
        // eth/70 receipt replies continue on their own; eth/69 ones do not.
        config.hello_message.protocols = vec![EthVersion::Eth69.into()];
        let (sender, mut incoming) = mpsc::channel(16);
        let network = Box::pin(config.manager())
            .await
            .unwrap()
            .with_eth_request_handler(sender);
        let handle = network.handle().clone();
        let record = format!("enode://{:x}@{}", handle.peer_id(), handle.local_addr());
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = requests.clone();
        let tip = BlockHash::new(last.hash_slow().0);
        let range = BlockRange::new(BlockNumber(10), BlockNumber(last.number)).unwrap();
        let responder = tokio::spawn(async move {
            let mut served = [0_usize; 2];
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
                        let requested = request.0.len();
                        log.lock().unwrap().push((Kind::Bodies, requested));
                        let (count, wrong_first) = match reply(Kind::Bodies, served[0], requested) {
                            Reply::Blocks(count) => (count, false),
                            Reply::WrongFirstBody(count) => (count, true),
                        };
                        served[0] += 1;
                        let bodies = (0..count.min(requested))
                            .map(|index| {
                                if wrong_first && index == 0 {
                                    BlockBody::default()
                                } else {
                                    body.clone()
                                }
                            })
                            .collect::<Vec<_>>();
                        let _ = response.send(Ok(bodies.into()));
                    }
                    IncomingEthRequest::GetReceipts69 {
                        request, response, ..
                    } => {
                        let requested = request.0.len();
                        log.lock().unwrap().push((Kind::Receipts, requested));
                        let Reply::Blocks(count) = reply(Kind::Receipts, served[1], requested)
                        else {
                            panic!("receipt replies serve blocks");
                        };
                        served[1] += 1;
                        let receipts = vec![vec![receipt.clone()]; count.min(requested)];
                        let _ = response.send(Ok(Receipts69(receipts)));
                    }
                    _ => panic!("unexpected request on the eth/69 session"),
                }
            }
        });
        Self {
            record,
            range,
            tip,
            requests,
            tasks: [tokio::spawn(network), responder],
        }
    }

    fn source(&self, material_request_blocks: usize) -> RethP2pSource {
        RethP2pSource::mainnet(RethP2pConfig {
            minimum_peers: 1,
            preferred_peers: 1,
            body_serving_peer_target: 1,
            max_outbound_peers: 1,
            max_concurrent_dials: 1,
            trusted_peers: vec![parse_trusted_peer(&self.record).unwrap()],
            enable_discv5: false,
            peer_wait_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(2),
            retries: 1,
            persistent_retries: false,
            material_request_blocks,
            ..RethP2pConfig::default()
        })
        .unwrap()
    }

    async fn probe(&self, source: &RethP2pSource) -> Result<P2pProbeResult, P2pError> {
        let probe = source.probe_fixed_range(
            self.range,
            Some(self.tip),
            SourceBudget {
                max_input_bytes: 1_000_000,
                max_frame_bytes: 1_000_000,
                max_frames: self.range.len(),
                max_buffered_frames: 8,
                // One request at a time keeps the request order exact.
                max_in_flight_requests: 1,
                temporary_disk_bytes: 1,
                max_resident_bytes: 1_000_000,
            },
            CancellationToken::new(),
        );
        tokio::time::timeout(Duration::from_secs(20), probe)
            .await
            .expect("bounded loopback probe")
    }

    /// Stop the peer and return its request log.
    fn stop(self) -> Vec<(Kind, usize)> {
        for task in &self.tasks {
            task.abort();
        }
        self.requests.lock().unwrap().clone()
    }
}

/// Blocks asked for by each request of `kind`, in arrival order.
fn requested(log: &[(Kind, usize)], kind: Kind) -> Vec<usize> {
    log.iter()
        .filter(|(request_kind, _)| *request_kind == kind)
        .map(|(_, blocks)| *blocks)
        .collect()
}

fn assert_complete(result: &P2pProbeResult, blocks: usize) {
    assert_eq!(result.frames.len(), blocks);
    assert!(
        result
            .frames
            .iter()
            .all(|frame| frame.receipts.as_complete().is_some_and(|r| r.len() == 1)),
        "every block keeps its one transaction's receipt"
    );
}

#[tokio::test]
async fn short_replies_keep_their_blocks_and_ask_only_for_the_rest() {
    let peer = LoopbackPeer::start(3, |_, _, requested| Reply::Blocks(requested.min(1))).await;
    let source = peer.source(8);
    let result = peer.probe(&source).await;
    let metrics = source.request_metrics();
    source.shutdown().await;
    let log = peer.stop();

    assert_complete(&result.expect("short replies complete the probe"), 3);
    assert_eq!(requested(&log, Kind::Bodies), [3, 2, 1]);
    assert_eq!(requested(&log, Kind::Receipts), [3, 2, 1]);
    assert_eq!(metrics.bodies.failed, 0, "a short reply is not a failure");
    assert_eq!(metrics.receipts.failed, 0, "a short reply is not a failure");
}

#[tokio::test]
async fn a_failed_request_does_not_narrow_later_requests() {
    // Each kind's first request gets an empty reply; every later one is full.
    let peer = LoopbackPeer::start(4, |_, nth, requested| {
        Reply::Blocks(if nth == 0 { 0 } else { requested })
    })
    .await;
    let source = peer.source(4);
    let first = peer.probe(&source).await;
    let second = peer.probe(&source).await;
    source.shutdown().await;
    let log = peer.stop();

    assert_complete(
        &first.expect("the empty reply is retried block by block"),
        4,
    );
    assert_complete(&second.expect("the second probe is served"), 4);
    assert_eq!(requested(&log, Kind::Bodies), [4, 1, 1, 1, 1, 4]);
    assert_eq!(requested(&log, Kind::Receipts), [4, 1, 1, 1, 1, 4]);
}

#[tokio::test]
async fn a_short_reply_with_a_wrong_body_is_fetched_again() {
    let peer = LoopbackPeer::start(3, |kind, nth, requested| match (kind, nth) {
        (Kind::Bodies, 0) => Reply::WrongFirstBody(1),
        _ => Reply::Blocks(requested),
    })
    .await;
    let source = peer.source(8);
    let result = peer.probe(&source).await;
    source.shutdown().await;
    let log = peer.stop();

    assert_complete(&result.expect("the wrong body is discarded"), 3);
    assert_eq!(requested(&log, Kind::Bodies), [3, 1, 1, 1]);
}
