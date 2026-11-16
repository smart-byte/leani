//! A Nitro chain's sequencer feed: every message names its block's hash and
//! carries the sequencer's signature over it.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use alloy_primitives::{Address, B256, Signature, keccak256};
use base64::{Engine, engine::general_purpose::STANDARD};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use url::Url;

use crate::websocket::Socket;

/// Signed blocks kept for the live lane to check its blocks against.
const KEPT_BLOCKS: usize = 16_384;
/// Message bytes kept for the live lane to make blocks from: minutes of
/// Arbitrum One's.
const KEPT_MESSAGE_BYTES: usize = 8 << 20;
/// The longest pause between reconnects.
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// Silence after which a feed connection counts as dead: feeds send blocks
/// several times a second, and a stalled relay connection need not close.
const FEED_SILENCE: Duration = Duration::from_secs(30);

#[derive(Debug, thiserror::Error)]
pub enum FeedError {
    #[error("undecodable feed message: {0}")]
    Decode(String),
    #[error("feed message {sequence} is unsigned or names no block")]
    Unsigned { sequence: u64 },
    #[error("feed message {sequence} was signed by {recovered}, not {expected}")]
    WrongSigner {
        sequence: u64,
        expected: Address,
        recovered: Address,
    },
}

/// Where a chain's feed is and who signs it.
#[derive(Clone, Debug)]
pub struct FeedConfig {
    pub url: Url,
    pub chain_id: u64,
    pub signer: Address,
    /// The block of message 0.
    pub first_block: u64,
}

/// The fields of a signed message that its block's transactions come from.
#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Message {
    pub kind: u8,
    pub sender: Address,
    pub l1_block: u64,
    pub timestamp: u64,
    pub l1_base_fee: u128,
    pub l2: Vec<u8>,
}

/// A block the sequencer signed, and the message it signed with it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SignedBlock {
    pub number: u64,
    pub hash: B256,
    pub message: Arc<Message>,
}

#[derive(Debug, Default)]
struct Signed {
    hashes: BTreeMap<u64, B256>,
    /// The newest signed blocks with their messages, within
    /// [`KEPT_MESSAGE_BYTES`].
    blocks: BTreeMap<u64, SignedBlock>,
    message_bytes: usize,
}

impl Signed {
    fn insert(&mut self, number: u64, hash: B256) {
        // Signed again with another hash, a block takes the chain above it
        // along: those blocks were built on the replaced one.
        if self
            .hashes
            .insert(number, hash)
            .is_some_and(|replaced| replaced != hash)
        {
            self.hashes.split_off(&(number + 1));
            for (_, block) in self.blocks.split_off(&(number + 1)) {
                self.message_bytes -= block.message.l2.len();
            }
        }
        while self.hashes.len() > KEPT_BLOCKS {
            self.hashes.pop_first();
        }
    }
}

/// Recent block hashes the sequencer signed, by number, and the newest
/// blocks' messages.
#[derive(Clone, Debug, Default)]
pub struct SignedHashes {
    signed: Arc<Mutex<Signed>>,
    /// Marked changed once a block is signed, after it is kept.
    changes: tokio::sync::watch::Sender<()>,
}

impl SignedHashes {
    fn lock(&self) -> std::sync::MutexGuard<'_, Signed> {
        self.signed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn insert(&self, number: u64, hash: B256) {
        self.lock().insert(number, hash);
        self.changes.send_replace(());
    }

    /// Keep a signed block's hash, and its message among the newest.
    pub(crate) fn keep(&self, block: SignedBlock) {
        let mut signed = self.lock();
        signed.insert(block.number, block.hash);
        signed.message_bytes += block.message.l2.len();
        if let Some(replaced) = signed.blocks.insert(block.number, block) {
            signed.message_bytes -= replaced.message.l2.len();
        }
        while signed.message_bytes > KEPT_MESSAGE_BYTES || signed.blocks.len() > KEPT_BLOCKS {
            let Some((_, oldest)) = signed.blocks.pop_first() else {
                break;
            };
            signed.message_bytes -= oldest.message.l2.len();
        }
        drop(signed);
        self.changes.send_replace(());
    }

    /// A receiver that changes each time a block is signed.
    #[must_use]
    pub(crate) fn changes(&self) -> tokio::sync::watch::Receiver<()> {
        self.changes.subscribe()
    }

    #[must_use]
    pub fn get(&self, number: u64) -> Option<B256> {
        self.lock().hashes.get(&number).copied()
    }

    /// The block signed at `number` with its message, while kept and still
    /// the one signed there.
    #[must_use]
    pub(crate) fn block(&self, number: u64) -> Option<SignedBlock> {
        let signed = self.lock();
        signed
            .blocks
            .get(&number)
            .filter(|block| signed.hashes.get(&number) == Some(&block.hash))
            .cloned()
    }

    #[must_use]
    pub fn newest(&self) -> Option<(u64, B256)> {
        self.lock()
            .hashes
            .last_key_value()
            .map(|(number, hash)| (*number, *hash))
    }
}

#[derive(Deserialize)]
struct Broadcast {
    #[serde(default)]
    messages: Vec<FeedMessage>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct FeedMessage {
    sequence_number: u64,
    message: MessageWithMetadata,
    block_hash: Option<B256>,
    #[serde(rename = "signatureV2", default)]
    signature: Option<String>,
    #[serde(default)]
    block_metadata: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct MessageWithMetadata {
    message: IncomingMessage,
    delayed_messages_read: u64,
}

#[derive(Deserialize)]
struct IncomingMessage {
    header: IncomingHeader,
    #[serde(rename = "l2Msg")]
    l2_msg: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct IncomingHeader {
    kind: u8,
    sender: Address,
    block_number: u64,
    timestamp: u64,
    request_id: Option<B256>,
    #[serde(rename = "baseFeeL1")]
    base_fee_l1: Option<u128>,
}

fn base64(field: &str, sequence: u64) -> Result<Vec<u8>, FeedError> {
    STANDARD
        .decode(field)
        .map_err(|error| FeedError::Decode(format!("message {sequence}: {error}")))
}

/// Nitro's `SignatureHash`: a fixed prefix, then every signed field in
/// order, `l2` last. The L1 base fee is a big integer's minimal big-endian
/// bytes, none for zero.
fn signature_hash(
    chain_id: u64,
    message: &FeedMessage,
    hash: B256,
    l2: &[u8],
) -> Result<B256, FeedError> {
    let sequence = message.sequence_number;
    let incoming = &message.message.message;
    let header = &incoming.header;
    let mut data = b"Arbitrum Nitro Feed:".to_vec();
    data.extend(chain_id.to_be_bytes());
    data.extend(sequence.to_be_bytes());
    data.extend(hash.0);
    if let Some(metadata) = &message.block_metadata {
        data.extend(base64(metadata, sequence)?);
    }
    data.extend(message.message.delayed_messages_read.to_be_bytes());
    data.push(header.kind);
    data.extend(header.sender.0);
    data.extend(header.block_number.to_be_bytes());
    data.extend(header.timestamp.to_be_bytes());
    if let Some(request) = header.request_id {
        data.extend(request.0);
    }
    if let Some(fee) = header.base_fee_l1 {
        let bytes = fee.to_be_bytes();
        let first = bytes
            .iter()
            .position(|byte| *byte != 0)
            .unwrap_or(bytes.len());
        data.extend(&bytes[first..]);
    }
    data.extend(l2);
    Ok(keccak256(data))
}

/// The blocks one feed frame names, each checked against the configured
/// signer.
///
/// # Errors
///
/// Returns an error for an undecodable frame or a message the signer did
/// not sign.
pub(crate) fn signed_blocks(
    config: &FeedConfig,
    frame: &str,
) -> Result<Vec<SignedBlock>, FeedError> {
    let broadcast: Broadcast =
        serde_json::from_str(frame).map_err(|error| FeedError::Decode(error.to_string()))?;
    broadcast
        .messages
        .iter()
        .map(|message| {
            let sequence = message.sequence_number;
            let (Some(hash), Some(signature)) = (message.block_hash, &message.signature) else {
                return Err(FeedError::Unsigned { sequence });
            };
            let invalid = |error: alloy_primitives::SignatureError| {
                FeedError::Decode(format!("message {sequence}: {error}"))
            };
            let signature = Signature::from_raw(&base64(signature, sequence)?).map_err(invalid)?;
            let incoming = &message.message.message;
            let l2 = base64(&incoming.l2_msg, sequence)?;
            let recovered = signature
                .recover_address_from_prehash(&signature_hash(config.chain_id, message, hash, &l2)?)
                .map_err(invalid)?;
            if recovered != config.signer {
                return Err(FeedError::WrongSigner {
                    sequence,
                    expected: config.signer,
                    recovered,
                });
            }
            let header = &incoming.header;
            Ok(SignedBlock {
                number: config.first_block + sequence,
                hash,
                message: Arc::new(Message {
                    kind: header.kind,
                    sender: header.sender,
                    l1_block: header.block_number,
                    timestamp: header.timestamp,
                    l1_base_fee: header.base_fee_l1.unwrap_or(0),
                    l2,
                }),
            })
        })
        .collect()
}

/// Follow the feed until cancelled, keeping each block the signer signed in
/// `hashes`. A dropped connection is retried from the message after the last
/// one, which relays replay for minutes.
pub async fn follow(config: FeedConfig, hashes: SignedHashes, cancellation: CancellationToken) {
    let mut next = None;
    let mut backoff = Duration::from_secs(1);
    loop {
        let session = tokio::select! {
            () = cancellation.cancelled() => return,
            session = read_feed(&config, next, &hashes) => session,
        };
        match session {
            Ok(Some(last)) => {
                next = Some(last + 1 - config.first_block);
                backoff = Duration::from_secs(1);
                warn!(last, "the sequencer feed closed; reconnecting");
            }
            Ok(None) => warn!("the sequencer feed closed before a block"),
            Err(error) => warn!(%error, ?backoff, "the sequencer feed failed; reconnecting"),
        }
        tokio::select! {
            () = cancellation.cancelled() => return,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// TLS naming its crypto provider: the build links two, and rustls picks no
/// default then.
fn tls_config() -> Result<Arc<rustls::ClientConfig>, String> {
    let roots = rustls::RootCertStore::from_iter(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .map(|builder| Arc::new(builder.with_root_certificates(roots).with_no_client_auth()))
        .map_err(|error| error.to_string())
}

/// Read one feed connection until it closes, returning the last block it
/// signed.
async fn read_feed(
    config: &FeedConfig,
    from_sequence: Option<u64>,
    hashes: &SignedHashes,
) -> Result<Option<u64>, String> {
    let mut headers = vec![("Arbitrum-Feed-Client-Version", "2".to_owned())];
    if let Some(sequence) = from_sequence {
        headers.push(("Arbitrum-Requested-Sequence-Number", sequence.to_string()));
    }
    let mut socket = Socket::connect(&config.url, &headers, tls_config()?).await?;
    let mut last = None;
    loop {
        let Ok(message) = tokio::time::timeout(FEED_SILENCE, socket.next_message()).await else {
            warn!(silent_for = ?FEED_SILENCE, "the sequencer feed went silent");
            break;
        };
        let Some(message) = message? else {
            break;
        };
        let text = String::from_utf8(message).map_err(|error| error.to_string())?;
        // Nitro skips a frame whose signatures fail, and so does this lane.
        let blocks = match signed_blocks(config, &text) {
            Ok(blocks) => blocks,
            Err(error) => {
                warn!(%error, "skipping a feed frame");
                continue;
            }
        };
        for block in blocks {
            let number = block.number;
            if last.is_none() {
                info!(block = number, "first block from the sequencer feed");
            }
            hashes.keep(block);
            last = Some(number);
        }
    }
    Ok(last)
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{address, b256};

    use super::*;

    fn config(chain_id: u64, signer: Address, first_block: u64) -> FeedConfig {
        FeedConfig {
            url: Url::parse("wss://feed.invalid").unwrap(),
            chain_id,
            signer,
            first_block,
        }
    }

    #[test]
    fn feed_messages_recover_the_chains_batch_poster() {
        let one = config(
            42_161,
            address!("C1b634853Cb333D3aD8663715b08f41A3Aec47cc"),
            22_207_817,
        );
        let blocks = signed_blocks(&one, include_str!("fixtures/arbitrum-one-feed.json")).unwrap();
        assert_eq!(
            blocks
                .iter()
                .map(|block| (block.number, block.hash))
                .collect::<Vec<_>>(),
            [(
                518_420_127,
                b256!("449bbfefffd3e4faf80453a57942a396017895a38cde573568eb5bb68a25cf4f")
            )]
        );
        // Deposits sign a request id and the L1 base fee too.
        let nova = config(
            42_170,
            address!("0C5911d57B24FCF1DC8B2608eFbAe57C7098E32D"),
            0,
        );
        let blocks =
            signed_blocks(&nova, include_str!("fixtures/arbitrum-nova-feed.json")).unwrap();
        assert_eq!(
            blocks.iter().map(|block| block.number).collect::<Vec<_>>(),
            [85_287_622, 85_287_623, 85_287_624, 85_287_625]
        );
        assert_eq!(
            blocks[0].hash,
            b256!("2f238566cc443870084d97f9315bfe6bf35ebbedd8940353a2f4275b298d9097")
        );
    }

    fn block(number: u64, hash: u8, l2_bytes: usize) -> SignedBlock {
        SignedBlock {
            number,
            hash: B256::repeat_byte(hash),
            message: Arc::new(Message {
                kind: 3,
                sender: Address::ZERO,
                l1_block: 0,
                timestamp: 0,
                l1_base_fee: 0,
                l2: vec![0; l2_bytes],
            }),
        }
    }

    #[test]
    fn messages_are_kept_for_the_newest_blocks_within_their_byte_budget() {
        let signed = SignedHashes::default();
        let half = KEPT_MESSAGE_BYTES / 2;
        for number in 1..=3 {
            signed.keep(block(number, 1, half));
        }
        // The oldest message gave way; its hash is still kept.
        assert_eq!(signed.block(1), None);
        assert_eq!(signed.get(1), Some(B256::repeat_byte(1)));
        assert_eq!(signed.block(3), Some(block(3, 1, half)));
        // Signed again with another hash, a block drops the old message.
        signed.insert(3, B256::repeat_byte(2));
        assert_eq!(signed.block(3), None);
        assert_eq!(signed.block(2), Some(block(2, 1, half)));
    }

    #[test]
    fn a_block_signed_again_with_another_hash_drops_the_blocks_above_it() {
        let signed = SignedHashes::default();
        for number in 1..=3 {
            signed.keep(block(number, 1, 8));
        }
        signed.keep(block(2, 2, 8));
        assert_eq!(signed.newest(), Some((2, B256::repeat_byte(2))));
        assert_eq!(signed.block(3), None);
        assert_eq!(signed.get(1), Some(B256::repeat_byte(1)));
    }

    #[test]
    fn another_signer_is_refused() {
        let forged = config(42_161, Address::repeat_byte(0x11), 22_207_817);
        assert!(matches!(
            signed_blocks(&forged, include_str!("fixtures/arbitrum-one-feed.json")),
            Err(FeedError::WrongSigner {
                sequence: 496_212_310,
                ..
            })
        ));
    }
}
