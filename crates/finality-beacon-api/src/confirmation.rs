//! Fast Confirmation Rule confirmations from operator-named beacon nodes.
//!
//! Each endpoint streams `fast_confirmation` events (beacon-APIs #598). An
//! event counts only for a beacon root this node verified as a
//! sync-committee-attested head with at least
//! [`CONFIRMATION_SYNC_PARTICIPANTS`] signers, so an endpoint can only label
//! as confirmed a block Leani already follows as attested.

use std::{collections::HashMap, time::Duration};

use leani_chain::BeaconNetwork;
use leani_source_api::{
    AttestedHead, AttestedHeadReceiver, ConfirmationVotes, NetworkConfirmationEndpointSnapshot,
    NetworkConfirmationEndpointState, NetworkConfirmationHeadSnapshot, NetworkConfirmationSnapshot,
    NetworkTelemetry,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use url::Url;

use crate::{
    BeaconApiError, Clock, current_slot,
    http::{normalized_endpoint, redacted_url, request_url},
};

/// Sync-committee signers an attested head needs before a confirmation of it
/// counts: three quarters, the support Fast Confirmation itself requires
/// with its 25% adversary bound.
const CONFIRMATION_SYNC_PARTICIPANTS: u64 = 384;
/// Slots without an event after which an endpoint is stale. Fast
/// Confirmation names a block every slot, even without progress.
const FRESH_SLOTS: u32 = 3;
/// Slots below the newest verified head whose roots an event may name.
const VERIFIED_ROOT_SLOTS: u64 = 64;
/// Longest event-stream line and event an endpoint may send. A
/// `fast_confirmation` event is about a hundred bytes.
const MAX_SSE_LINE_BYTES: usize = 4 * 1_024;
const MAX_SSE_EVENT_BYTES: usize = 4 * 1_024;
const RECONNECT_FIRST: Duration = Duration::from_secs(1);
const RECONNECT_MAX: Duration = Duration::from_secs(30);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const EVENTS_PATH: &str = "eth/v1/events?topics=fast_confirmation";

/// Endpoints whose Fast Confirmation events Leani trusts, and how many must
/// agree.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConfirmationConfig {
    pub endpoints: Vec<Url>,
    pub minimum_agreement: usize,
}

impl ConfirmationConfig {
    /// # Errors
    ///
    /// Fails without endpoints, or when `minimum_agreement` is outside
    /// `1..=endpoints.len()`.
    pub fn validate(&self) -> Result<(), BeaconApiError> {
        if self.endpoints.is_empty() {
            return Err(BeaconApiError::InvalidConfig(
                "confirmation needs at least one endpoint".to_owned(),
            ));
        }
        if self.minimum_agreement == 0 || self.minimum_agreement > self.endpoints.len() {
            return Err(BeaconApiError::InvalidConfig(format!(
                "confirmation minimum agreement {} must be within 1..={}",
                self.minimum_agreement,
                self.endpoints.len()
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct EndpointTrack {
    /// Newest event whose root is not verified yet: root and slot.
    held: Option<([u8; 32], u64)>,
    /// The head its newest event named, once verified with enough signers.
    vote: Option<AttestedHead>,
    /// The last head it voted for, to tell a reset.
    accepted: Option<AttestedHead>,
    last_event_slot: Option<u64>,
    received_slot: Option<u64>,
    reset: bool,
    failed: bool,
}

/// Agreement over endpoint confirmations, fed verified heads and events.
#[derive(Debug)]
struct ConfirmationTracker {
    minimum_agreement: usize,
    verified: HashMap<[u8; 32], AttestedHead>,
    newest_verified_slot: u64,
    endpoints: Vec<EndpointTrack>,
}

impl ConfirmationTracker {
    #[must_use]
    fn new(endpoints: usize, minimum_agreement: usize) -> Self {
        Self {
            minimum_agreement,
            verified: HashMap::new(),
            newest_verified_slot: 0,
            endpoints: vec![EndpointTrack::default(); endpoints],
        }
    }

    /// Record a head this node verified, or a better-signed copy of one, and
    /// apply every held event that named it.
    fn observe_attested(&mut self, head: AttestedHead) {
        self.newest_verified_slot = self.newest_verified_slot.max(head.beacon_slot);
        let floor = self
            .newest_verified_slot
            .saturating_sub(VERIFIED_ROOT_SLOTS);
        self.verified.insert(head.beacon_block_root, head);
        self.verified.retain(|_, head| head.beacon_slot >= floor);
        for endpoint in &mut self.endpoints {
            match endpoint.held {
                Some((root, _)) if root == head.beacon_block_root => {
                    if accept(endpoint, head) {
                        endpoint.held = None;
                    }
                }
                Some((_, slot)) if slot < floor => endpoint.held = None,
                _ => {}
            }
        }
    }

    /// Record an endpoint's event naming `root` at `slot`, received in local
    /// slot `wall_slot`. Each event replaces the endpoint's vote: one naming
    /// a root not verified yet, or verified with too few signers, leaves it
    /// without a vote until a good enough head for that root arrives, since
    /// the new root need not descend from the old one.
    fn observe_event(&mut self, endpoint: usize, root: [u8; 32], slot: u64, wall_slot: u64) {
        let Some(track) = self.endpoints.get_mut(endpoint) else {
            return;
        };
        track.failed = false;
        track.last_event_slot = Some(slot);
        track.received_slot = Some(wall_slot);
        if let Some(head) = self.verified.get(&root)
            && accept(track, *head)
        {
            track.held = None;
        } else {
            track.held = Some((root, slot));
            track.reset = track
                .accepted
                .is_some_and(|previous| slot < previous.beacon_slot);
            track.vote = None;
        }
    }

    /// Record that an endpoint's stream failed.
    fn observe_error(&mut self, endpoint: usize) {
        if let Some(track) = self.endpoints.get_mut(endpoint) {
            track.failed = true;
        }
    }

    /// The votes of endpoints with an event in the last [`FRESH_SLOTS`]
    /// slots, newest first.
    #[must_use]
    fn votes(&self, wall_slot: u64) -> ConfirmationVotes {
        let mut votes = self
            .endpoints
            .iter()
            .filter(|track| fresh(track, wall_slot))
            .filter_map(|track| track.vote)
            .collect::<Vec<_>>();
        votes.sort_by_key(|head| std::cmp::Reverse(head.beacon_slot));
        ConfirmationVotes {
            minimum_agreement: self.minimum_agreement,
            votes,
        }
    }

    /// The `minimum_agreement`-th newest fresh vote, or `None` with fewer.
    #[must_use]
    fn agreed(&self, wall_slot: u64) -> Option<AttestedHead> {
        self.votes(wall_slot)
            .votes
            .get(self.minimum_agreement.checked_sub(1)?)
            .copied()
    }

    /// Each endpoint's state, labeled by `labels`, with `agreed`, the head
    /// the endpoints last agreed on.
    #[must_use]
    fn status(
        &self,
        labels: &[String],
        wall_slot: u64,
        agreed: Option<AttestedHead>,
    ) -> NetworkConfirmationSnapshot {
        NetworkConfirmationSnapshot {
            minimum_agreement: self.minimum_agreement,
            agreed_head: agreed.map(|head| NetworkConfirmationHeadSnapshot {
                beacon_slot: head.beacon_slot,
                block_number: head.block_number.0,
                block_hash: head.block_hash.to_string(),
            }),
            lag_slots: agreed.map(|head| wall_slot.saturating_sub(head.beacon_slot)),
            endpoints: self
                .endpoints
                .iter()
                .zip(labels)
                .map(|(track, label)| NetworkConfirmationEndpointSnapshot {
                    endpoint: label.clone(),
                    state: if track.failed {
                        NetworkConfirmationEndpointState::Error
                    } else if track.received_slot.is_none() {
                        NetworkConfirmationEndpointState::Connecting
                    } else if !fresh(track, wall_slot) {
                        NetworkConfirmationEndpointState::Stale
                    } else if track.reset {
                        NetworkConfirmationEndpointState::Reset
                    } else {
                        NetworkConfirmationEndpointState::Connected
                    },
                    last_event_slot: track.last_event_slot,
                    last_event_received_slot: track.received_slot,
                })
                .collect(),
        }
    }
}

fn fresh(track: &EndpointTrack, wall_slot: u64) -> bool {
    track
        .received_slot
        .is_some_and(|received| wall_slot.saturating_sub(received) <= u64::from(FRESH_SLOTS))
}

/// Take `head` as the endpoint's newest confirmation if enough of the sync
/// committee signed it, and say whether it did. A lower slot than its
/// previous one is a reset: the lower value counts, so agreement waits until
/// enough endpoints pass it.
fn accept(track: &mut EndpointTrack, head: AttestedHead) -> bool {
    if head.sync_participants < CONFIRMATION_SYNC_PARTICIPANTS {
        debug!(
            beacon_slot = head.beacon_slot,
            sync_participants = head.sync_participants,
            "a confirmed head has too few sync-committee signers to count yet"
        );
        return false;
    }
    track.reset = track
        .accepted
        .is_some_and(|previous| head.beacon_slot < previous.beacon_slot);
    track.accepted = Some(head);
    track.vote = Some(head);
    true
}

/// One server-sent event: its name and data.
#[derive(Clone, Debug, Eq, PartialEq)]
struct SseEvent {
    name: String,
    data: String,
}

/// Incremental `text/event-stream` parser with bounded lines and events.
#[derive(Debug, Default)]
struct SseParser {
    line: Vec<u8>,
    name: String,
    data: String,
    has_data: bool,
    /// The last byte was a CR, so a following LF ends no second line.
    after_cr: bool,
}

impl SseParser {
    fn feed(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, String> {
        let mut events = Vec::new();
        for &byte in bytes {
            let after_cr = std::mem::replace(&mut self.after_cr, byte == b'\r');
            match byte {
                b'\n' if after_cr => {}
                b'\n' | b'\r' => {
                    let line = std::mem::take(&mut self.line);
                    if let Some(event) = self.line_done(&line)? {
                        events.push(event);
                    }
                }
                _ => {
                    if self.line.len() >= MAX_SSE_LINE_BYTES {
                        return Err(format!(
                            "event-stream line exceeds {MAX_SSE_LINE_BYTES} bytes"
                        ));
                    }
                    self.line.push(byte);
                }
            }
        }
        Ok(events)
    }

    fn line_done(&mut self, line: &[u8]) -> Result<Option<SseEvent>, String> {
        if line.is_empty() {
            let name = std::mem::take(&mut self.name);
            let data = std::mem::take(&mut self.data);
            return Ok(std::mem::take(&mut self.has_data).then(|| SseEvent {
                name: if name.is_empty() {
                    "message".to_owned()
                } else {
                    name
                },
                data,
            }));
        }
        let line = std::str::from_utf8(line).map_err(|_| "event-stream line is not UTF-8")?;
        if line.starts_with(':') {
            return Ok(None);
        }
        let (field, value) = line.split_once(':').unwrap_or((line, ""));
        let value = value.strip_prefix(' ').unwrap_or(value);
        match field {
            "event" => value.clone_into(&mut self.name),
            "data" => {
                if self.has_data {
                    self.data.push('\n');
                }
                self.data.push_str(value);
                self.has_data = true;
                if self.data.len() > MAX_SSE_EVENT_BYTES {
                    return Err(format!("event exceeds {MAX_SSE_EVENT_BYTES} bytes"));
                }
            }
            _ => {}
        }
        Ok(None)
    }
}

/// The confirmed root and slot of a `fast_confirmation` event's data.
fn parse_fast_confirmation(data: &str) -> Result<([u8; 32], u64), String> {
    let value: serde_json::Value =
        serde_json::from_str(data).map_err(|error| format!("invalid event JSON: {error}"))?;
    let block = value["block"]
        .as_str()
        .and_then(|block| block.strip_prefix("0x"))
        .and_then(|block| hex::decode(block).ok())
        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
        .ok_or("event block is not a 32-byte hex root")?;
    let slot = match &value["slot"] {
        serde_json::Value::String(slot) => slot.parse().ok(),
        slot => slot.as_u64(),
    }
    .ok_or("event slot is not an unsigned integer")?;
    Ok((block, slot))
}

#[derive(Debug)]
enum EndpointMessage {
    Event { root: [u8; 32], slot: u64 },
    Failed,
}

/// Follow `config`'s endpoints until `cancellation`. Their votes go to the
/// returned channel and their status to `telemetry`, which holds none once
/// following ends. `attested` carries the heads this node verified.
///
/// # Errors
///
/// Fails for an invalid `config` or an HTTP client that cannot be built.
pub fn spawn_confirmation(
    config: &ConfirmationConfig,
    network: BeaconNetwork,
    mut attested: AttestedHeadReceiver,
    clock: Clock,
    telemetry: NetworkTelemetry,
    cancellation: CancellationToken,
) -> Result<watch::Receiver<ConfirmationVotes>, BeaconApiError> {
    config.validate()?;
    let client = reqwest::Client::builder()
        .connect_timeout(CONNECT_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .user_agent(concat!("leani/", env!("CARGO_PKG_VERSION")))
        .build()
        .map_err(BeaconApiError::HttpClient)?;
    let slot = Duration::from_secs(network.seconds_per_slot);
    let idle = slot.saturating_mul(FRESH_SLOTS);
    let labels = crate::endpoint_labels(&config.endpoints, "confirmation.endpoints");
    let (messages, mut inbox) = mpsc::channel(64);
    for (index, endpoint) in config.endpoints.iter().enumerate() {
        tokio::spawn(follow_endpoint(
            client.clone(),
            normalized_endpoint(endpoint),
            index,
            idle,
            messages.clone(),
            cancellation.clone(),
        ));
    }
    drop(messages);
    let mut tracker = ConfirmationTracker::new(config.endpoints.len(), config.minimum_agreement);
    // The first events name the head verified before this subscription.
    if let Some(head) = attested.latest() {
        tracker.observe_attested(head);
    }
    let wall_slot = move || current_slot(&network, clock.now());
    let (votes, votes_receiver) = watch::channel(tracker.votes(wall_slot()));
    telemetry.set_confirmation(Some(tracker.status(&labels, wall_slot(), None)));
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(slot);
        let mut following = true;
        let mut agreed = None;
        loop {
            tokio::select! {
                () = cancellation.cancelled() => break,
                changed = attested.changed(), if following => {
                    following = changed;
                    if let Some(head) = attested.latest() {
                        tracker.observe_attested(head);
                    }
                }
                message = inbox.recv() => match message {
                    Some((index, EndpointMessage::Event { root, slot })) => {
                        tracker.observe_event(index, root, slot, wall_slot());
                    }
                    Some((index, EndpointMessage::Failed)) => tracker.observe_error(index),
                    None => break,
                },
                _ = tick.tick() => {}
            }
            let now = wall_slot();
            votes.send_if_modified(|current| {
                let next = tracker.votes(now);
                let changed = *current != next;
                *current = next;
                changed
            });
            agreed = tracker.agreed(now).or(agreed);
            telemetry.set_confirmation(Some(tracker.status(&labels, now, agreed)));
        }
        telemetry.set_confirmation(None);
    });
    Ok(votes_receiver)
}

/// Stream one endpoint's events into `messages`, reconnecting with capped
/// backoff after any failure, until `cancellation`.
async fn follow_endpoint(
    client: reqwest::Client,
    endpoint: Url,
    index: usize,
    idle: Duration,
    messages: mpsc::Sender<(usize, EndpointMessage)>,
    cancellation: CancellationToken,
) {
    let mut backoff = RECONNECT_FIRST;
    loop {
        let streamed = tokio::select! {
            () = cancellation.cancelled() => return,
            streamed = stream_events(&client, &endpoint, index, idle, &messages) => streamed,
        };
        let error = match streamed {
            Ok(received) => {
                if received {
                    backoff = RECONNECT_FIRST;
                }
                "the event stream ended".to_owned()
            }
            Err(error) => error,
        };
        warn!(
            endpoint = %redacted_url(&endpoint),
            %error,
            "confirmation endpoint stream failed; reconnecting"
        );
        if messages
            .send((index, EndpointMessage::Failed))
            .await
            .is_err()
        {
            return;
        }
        tokio::select! {
            () = cancellation.cancelled() => return,
            () = tokio::time::sleep(backoff) => {}
        }
        backoff = backoff.saturating_mul(2).min(RECONNECT_MAX);
    }
}

/// Read one connection's events. Returns whether any event arrived before
/// the stream ended, or why it failed.
async fn stream_events(
    client: &reqwest::Client,
    endpoint: &Url,
    index: usize,
    idle: Duration,
    messages: &mpsc::Sender<(usize, EndpointMessage)>,
) -> Result<bool, String> {
    let url = request_url(endpoint, EVENTS_PATH).map_err(|error| error.to_string())?;
    let mut response = tokio::time::timeout(
        idle,
        client
            .get(url)
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .send(),
    )
    .await
    .map_err(|_| "no response".to_owned())?
    .map_err(|error| error.without_url().to_string())?;
    if !response.status().is_success() {
        return Err(format!("status {}", response.status().as_u16()));
    }
    let mut parser = SseParser::default();
    let mut received = false;
    loop {
        let chunk = tokio::time::timeout(idle, response.chunk())
            .await
            .map_err(|_| format!("no data for {} s", idle.as_secs()))?
            .map_err(|error| error.without_url().to_string())?;
        let Some(chunk) = chunk else {
            return Ok(received);
        };
        for event in parser.feed(&chunk)? {
            if event.name != "fast_confirmation" {
                continue;
            }
            let (root, slot) = parse_fast_confirmation(&event.data)?;
            received = true;
            if messages
                .send((index, EndpointMessage::Event { root, slot }))
                .await
                .is_err()
            {
                return Ok(received);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use leani_primitives::{BlockHash, BlockNumber};

    use super::*;

    fn head(slot: u64, root: u8, participants: u64) -> AttestedHead {
        AttestedHead {
            beacon_slot: slot,
            beacon_block_root: [root; 32],
            block_number: BlockNumber(slot + 1_000),
            block_hash: BlockHash::new([root; 32]),
            sync_participants: participants,
        }
    }

    #[test]
    fn an_event_waits_for_its_verified_root() {
        let mut tracker = ConfirmationTracker::new(1, 1);
        tracker.observe_event(0, [1; 32], 10, 11);
        assert_eq!(tracker.agreed(11), None, "the root is not verified yet");
        tracker.observe_attested(head(10, 1, 500));
        assert_eq!(tracker.agreed(11), Some(head(10, 1, 500)));
        // A later event for an already verified root applies at once.
        tracker.observe_attested(head(11, 2, 500));
        tracker.observe_event(0, [2; 32], 11, 12);
        assert_eq!(tracker.agreed(12), Some(head(11, 2, 500)));
    }

    #[test]
    fn unknown_and_expired_roots_never_count() {
        let mut tracker = ConfirmationTracker::new(1, 1);
        tracker.observe_attested(head(10, 1, 500));
        tracker.observe_attested(head(10 + VERIFIED_ROOT_SLOTS + 1, 9, 500));
        tracker.observe_event(0, [1; 32], 10, 80);
        assert_eq!(tracker.agreed(80), None, "the root left the window");
        tracker.observe_event(0, [7; 32], 79, 80);
        assert_eq!(tracker.agreed(80), None, "never verified");
        // A held root older than the window is dropped, not applied later.
        tracker.observe_attested(head(200, 3, 500));
        tracker.observe_attested(head(79, 7, 500));
        assert_eq!(tracker.agreed(80), None);
    }

    #[test]
    fn a_confirmation_needs_three_quarters_of_the_sync_committee() {
        let mut tracker = ConfirmationTracker::new(1, 1);
        tracker.observe_attested(head(10, 1, CONFIRMATION_SYNC_PARTICIPANTS - 1));
        tracker.observe_event(0, [1; 32], 10, 11);
        assert_eq!(tracker.agreed(11), None);
        tracker.observe_attested(head(11, 2, CONFIRMATION_SYNC_PARTICIPANTS));
        tracker.observe_event(0, [2; 32], 11, 12);
        assert_eq!(
            tracker.agreed(12),
            Some(head(11, 2, CONFIRMATION_SYNC_PARTICIPANTS))
        );
    }

    #[test]
    fn a_held_event_waits_for_a_head_with_enough_signers() {
        let mut tracker = ConfirmationTracker::new(1, 1);
        tracker.observe_event(0, [1; 32], 10, 11);
        tracker.observe_attested(head(10, 1, CONFIRMATION_SYNC_PARTICIPANTS - 1));
        assert_eq!(tracker.agreed(11), None);
        // A better-signed copy of the same head lets the event count.
        tracker.observe_attested(head(10, 1, 500));
        assert_eq!(tracker.agreed(11), Some(head(10, 1, 500)));
        // The same holds for an event naming an already weak head.
        tracker.observe_attested(head(11, 2, 1));
        tracker.observe_event(0, [2; 32], 11, 12);
        assert_eq!(tracker.agreed(12), None);
        tracker.observe_attested(head(11, 2, 400));
        assert_eq!(tracker.agreed(12), Some(head(11, 2, 400)));
    }

    #[test]
    fn agreement_takes_the_quorum_head_among_fresh_endpoints() {
        let mut tracker = ConfirmationTracker::new(3, 2);
        for slot in 10..=12 {
            tracker.observe_attested(head(slot, u8::try_from(slot).unwrap(), 500));
        }
        tracker.observe_event(0, [12; 32], 12, 20);
        tracker.observe_event(1, [11; 32], 11, 20);
        assert_eq!(tracker.agreed(20), Some(head(11, 11, 500)));
        // Endpoint 1 goes stale: one fresh endpoint cannot agree with itself.
        assert_eq!(tracker.agreed(20 + u64::from(FRESH_SLOTS) + 1), None);
        tracker.observe_event(2, [10; 32], 10, 24);
        tracker.observe_event(0, [12; 32], 12, 24);
        assert_eq!(tracker.agreed(24), Some(head(10, 10, 500)));
        let labels = ["a".to_owned(), "b".to_owned(), "c".to_owned()];
        let lagging = tracker.status(&labels, 26, tracker.agreed(24));
        assert_eq!(lagging.lag_slots, Some(16), "from the agreed head's slot");
        assert_eq!(
            lagging.agreed_head.map(|head| head.block_number),
            Some(1_010)
        );
        let states = tracker
            .status(&labels, 24, None)
            .endpoints
            .iter()
            .map(|endpoint| endpoint.state)
            .collect::<Vec<_>>();
        assert_eq!(
            states,
            [
                NetworkConfirmationEndpointState::Connected,
                NetworkConfirmationEndpointState::Stale,
                NetworkConfirmationEndpointState::Connected,
            ]
        );
    }

    #[test]
    fn a_reset_replaces_the_vote() {
        let mut tracker = ConfirmationTracker::new(1, 1);
        tracker.observe_attested(head(10, 1, 500));
        tracker.observe_attested(head(12, 2, 500));
        tracker.observe_event(0, [2; 32], 12, 13);
        assert_eq!(tracker.agreed(13), Some(head(12, 2, 500)));
        // The rule fell back: the lower head is the endpoint's vote now, and
        // the store, which never demotes, keeps what it confirmed.
        tracker.observe_event(0, [1; 32], 10, 14);
        assert_eq!(tracker.agreed(14), Some(head(10, 1, 500)));
        assert_eq!(
            tracker.status(&["a".to_owned()], 14, None).endpoints[0].state,
            NetworkConfirmationEndpointState::Reset
        );
        // A fall back to a root outside the window leaves no vote at all:
        // the old one no longer stands for what the endpoint confirms.
        tracker.observe_event(0, [2; 32], 12, 15);
        tracker.observe_event(0, [0xee; 32], 3, 16);
        assert_eq!(tracker.votes(16).votes, []);
        assert_eq!(
            tracker.status(&["a".to_owned()], 16, None).endpoints[0].state,
            NetworkConfirmationEndpointState::Reset
        );
    }

    #[test]
    fn the_event_stream_parser_reads_frames_across_chunks() {
        let mut parser = SseParser::default();
        let first = parser
            .feed(
                b": keepalive\r\nevent: head\ndata: {}\n\nevent: fast_confirmation\r\ndata: {\"bl",
            )
            .expect("parse");
        assert_eq!(
            first,
            [SseEvent {
                name: "head".to_owned(),
                data: "{}".to_owned()
            }]
        );
        let root = format!("0x{}", "ab".repeat(32));
        let rest = format!("ock\": \"{root}\", \"slot\": \"15569676\"}}\r\n\r\n");
        let events = parser.feed(rest.as_bytes()).expect("parse");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "fast_confirmation");
        assert_eq!(
            parse_fast_confirmation(&events[0].data).expect("event"),
            ([0xab; 32], 15_569_676)
        );
        let joined = parser.feed(b"data: a\ndata: b\n\n").expect("parse");
        assert_eq!(joined[0].data, "a\nb");
    }

    #[test]
    fn malformed_or_oversized_streams_fail() {
        let mut parser = SseParser::default();
        assert!(parser.feed(&vec![b'x'; MAX_SSE_LINE_BYTES + 1]).is_err());
        let mut parser = SseParser::default();
        let line = format!("data: {}\n", "y".repeat(MAX_SSE_LINE_BYTES - 7));
        let mut oversized = Vec::new();
        for _ in 0..2 {
            oversized.extend_from_slice(line.as_bytes());
        }
        assert!(parser.feed(&oversized).is_err());
        assert!(SseParser::default().feed(&[0xff, b'\n']).is_err());
        for data in [
            "not json",
            r#"{"block": "0x12", "slot": "1"}"#,
            r#"{"slot": "1"}"#,
            &format!(r#"{{"block": "0x{}", "slot": "-1"}}"#, "00".repeat(32)),
        ] {
            assert!(parse_fast_confirmation(data).is_err(), "{data}");
        }
        assert_eq!(
            parse_fast_confirmation(&format!(
                r#"{{"block": "0x{}", "slot": 7}}"#,
                "01".repeat(32)
            ))
            .expect("numeric slot"),
            ([1; 32], 7)
        );
    }

    #[test]
    fn configurations_need_endpoints_and_a_reachable_agreement() {
        let endpoint = Url::parse("https://beacon.example/").expect("url");
        let config = |endpoints: Vec<Url>, minimum_agreement| ConfirmationConfig {
            endpoints,
            minimum_agreement,
        };
        assert!(config(vec![endpoint.clone()], 1).validate().is_ok());
        assert!(config(Vec::new(), 1).validate().is_err());
        assert!(config(vec![endpoint.clone()], 0).validate().is_err());
        assert!(config(vec![endpoint], 2).validate().is_err());
    }
}
