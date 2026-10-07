//! Material presence and completeness semantics.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{Address, BlockNumber, BlockRange, TransactionHash};

/// Exact predicate applied before source material reached the node.
///
/// An item matches when it satisfies every field. A missing block range and an
/// empty list are wildcards: they do not constrain their field. Sources apply
/// a scope with [`Self::matches_block`] to the block, with
/// [`Self::matches_transaction`] and [`Self::matches_sender`] to each
/// transaction, and with [`Self::matches_log`] to each log of a matching
/// transaction.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct FilterScope {
    pub block_range: Option<BlockRange>,
    pub addresses: Vec<Address>,
    pub topics: Vec<TopicFilter>,
    pub transaction_hashes: Vec<TransactionHash>,
    pub transaction_types: Vec<u8>,
    /// The serde default lets JSON written before this field existed, such as
    /// stored processor descriptors and JSON-lines history archives, decode as
    /// a wildcard. Durable postcard records always encode every field, so it
    /// adds no compatibility for them.
    #[serde(default)]
    pub senders: Vec<Address>,
    /// Defaulted for the same JSON inputs as [`Self::senders`].
    #[serde(default)]
    pub recipients: Vec<Address>,
}

impl FilterScope {
    /// Whether every item `filter` can match also matches this scope, so
    /// material complete for this scope is complete for `filter`.
    ///
    /// Each field is compared on its own and must cover the filter's:
    ///
    /// - A wildcard covers anything, and only a wildcard covers a wildcard.
    /// - A block range covers the ranges inside it.
    /// - A value list covers every non-empty list of values it contains.
    /// - Every topic position this scope constrains must also be constrained
    ///   by `filter`, and only to topics this scope allows there. When several
    ///   [`TopicFilter`]s share a position, a topic must satisfy all of them.
    #[must_use]
    pub fn covers(&self, filter: &Self) -> bool {
        let block_range = match (self.block_range, filter.block_range) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(scope), Some(filter)) => {
                scope.start() <= filter.start() && filter.end() <= scope.end()
            }
        };
        block_range && self.covers_values(filter)
    }

    /// [`Self::covers`] for the items of one block, as a [`crate::BlockFrame`]
    /// carries them.
    ///
    /// Block ranges are compared only at `block`: a filter whose range
    /// excludes `block` needs nothing from it, and a scope whose range
    /// excludes `block` supplies nothing for it.
    #[must_use]
    pub fn covers_at(&self, filter: &Self, block: BlockNumber) -> bool {
        if filter
            .block_range
            .is_some_and(|range| !range.contains(block))
        {
            return true;
        }
        self.block_range.is_none_or(|range| range.contains(block)) && self.covers_values(filter)
    }

    fn covers_values(&self, filter: &Self) -> bool {
        list_covers(&self.addresses, &filter.addresses)
            && topics_cover(&self.topics, &filter.topics)
            && list_covers(&self.transaction_hashes, &filter.transaction_hashes)
            && list_covers(&self.transaction_types, &filter.transaction_types)
            && list_covers(&self.senders, &filter.senders)
            && list_covers(&self.recipients, &filter.recipients)
    }

    /// Whether `block` lies in [`Self::block_range`]. Nothing in a block
    /// outside it matches.
    #[must_use]
    pub fn matches_block(&self, block: BlockNumber) -> bool {
        self.block_range.is_none_or(|range| range.contains(block))
    }

    /// Whether a transaction's type, recipient, and hash match. A contract
    /// creation has no recipient, so only a recipient wildcard matches it.
    /// `hash` is called only when [`Self::transaction_hashes`] constrains it.
    /// The sender, which is costly to recover, is matched separately by
    /// [`Self::matches_sender`].
    #[must_use]
    pub fn matches_transaction(
        &self,
        transaction_type: u8,
        recipient: Option<&Address>,
        hash: impl FnOnce() -> TransactionHash,
    ) -> bool {
        list_matches(&self.transaction_types, Some(&transaction_type))
            && list_matches(&self.recipients, recipient)
            && (self.transaction_hashes.is_empty() || self.matches_transaction_hash(&hash()))
    }

    /// Whether a transaction hash matches, for a source that knows only the
    /// hash of a log's transaction.
    #[must_use]
    pub fn matches_transaction_hash(&self, hash: &TransactionHash) -> bool {
        list_matches(&self.transaction_hashes, Some(hash))
    }

    /// Whether a transaction's sender matches. An unknown sender matches only
    /// the wildcard.
    #[must_use]
    pub fn matches_sender(&self, sender: Option<&Address>) -> bool {
        list_matches(&self.senders, sender)
    }

    /// Whether a log's address and topics match. Each topic filter needs a
    /// topic at its position, so a log with fewer topics does not match it.
    #[must_use]
    pub fn matches_log<T: AsRef<[u8]>>(&self, address: &Address, topics: &[T]) -> bool {
        self.matches_log_address(address)
            && self.topics.iter().all(|filter| {
                topics
                    .get(usize::from(filter.position))
                    .is_some_and(|topic| {
                        filter
                            .alternatives
                            .iter()
                            .any(|alternative| alternative.as_slice() == topic.as_ref())
                    })
            })
    }

    /// Whether a log's address matches, for a source that rules a log out
    /// before decoding its topics.
    #[must_use]
    pub fn matches_log_address(&self, address: &Address) -> bool {
        list_matches(&self.addresses, Some(address))
    }

    /// Whether this scope selects transactions by their type, hash, sender,
    /// or recipient, so matching it needs the transactions themselves. A
    /// block's matching transactions, with their receipts and logs, may then
    /// be fewer than its own.
    #[must_use]
    pub fn constrains_transactions(&self) -> bool {
        !self.transaction_types.is_empty()
            || !self.transaction_hashes.is_empty()
            || !self.senders.is_empty()
            || !self.recipients.is_empty()
    }

    /// Whether this scope selects logs by their address or topics, so a
    /// block's matching logs may be fewer than its matching transactions'.
    #[must_use]
    pub fn constrains_logs(&self) -> bool {
        !self.addresses.is_empty() || !self.topics.is_empty()
    }
}

/// An empty list is a wildcard: it matches any value, even an unknown one.
fn list_matches<T: PartialEq>(list: &[T], value: Option<&T>) -> bool {
    list.is_empty() || value.is_some_and(|value| list.contains(value))
}

/// An empty list is a wildcard: it covers every list and only it covers itself.
fn list_covers<T: Ord>(scope: &[T], filter: &[T]) -> bool {
    scope.is_empty() || (!filter.is_empty() && contains_all(scope, filter))
}

fn topics_cover(scope: &[TopicFilter], filter: &[TopicFilter]) -> bool {
    scope.iter().all(|required| {
        let mut constraints = filter
            .iter()
            .filter(|topic| topic.position == required.position);
        let Some(first) = constraints.next() else {
            // The filter leaves this position open, so it also matches topics
            // this scope excludes.
            return false;
        };
        // A topic matches the filter here only if every constraint allows it.
        let matchable = first.alternatives.iter().filter(|value| {
            constraints
                .clone()
                .all(|topic| topic.alternatives.contains(value))
        });
        contains_all(&required.alternatives, matchable)
    })
}

/// Whether `values` holds every item of `required`.
fn contains_all<'a, T: Ord + 'a>(values: &[T], required: impl IntoIterator<Item = &'a T>) -> bool {
    // Filters are usually a handful of values. Index long ones so a scope with
    // thousands of addresses is not rescanned for every required value.
    const LINEAR_SCAN_LIMIT: usize = 16;
    let mut required = required.into_iter();
    if values.len() <= LINEAR_SCAN_LIMIT {
        required.all(|value| values.contains(value))
    } else {
        let values = values.iter().collect::<BTreeSet<_>>();
        required.all(|value| values.contains(value))
    }
}

/// Topic predicate at an exact log topic position.
///
/// A log matches when its topic at `position` is one of `alternatives`, so an
/// empty `alternatives` list matches no log. It is not a wildcard.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TopicFilter {
    pub position: u8,
    pub alternatives: Vec<[u8; 32]>,
}

/// What the source can claim about the filtered result.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Completeness {
    /// All values matching the recorded predicate are present and the claim was
    /// independently verified.
    VerifiedPredicate,
    /// The dataset declares all matching values are present; the node could not
    /// independently reconstruct the full committed material.
    DatasetDeclared,
    /// A best-effort subset that must never satisfy a processor requiring
    /// complete or predicate-complete material.
    Partial,
}

/// Why a component is absent. Empty complete vectors use `Complete(vec![])`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MissingReason {
    NotRequested,
    Unsupported,
    NotAvailable,
    TemporarilyUnavailable,
    Pruned,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaterialKind {
    Complete,
    Filtered,
    Missing,
}

/// Material state with no ambiguous `Option<T>`.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Material<T> {
    Complete(T),
    Filtered {
        value: T,
        scope: FilterScope,
        completeness: Completeness,
    },
    Missing(MissingReason),
}

impl<T> Material<T> {
    #[must_use]
    pub const fn kind(&self) -> MaterialKind {
        match self {
            Self::Complete(_) => MaterialKind::Complete,
            Self::Filtered { .. } => MaterialKind::Filtered,
            Self::Missing(_) => MaterialKind::Missing,
        }
    }

    #[must_use]
    pub const fn is_present(&self) -> bool {
        !matches!(self, Self::Missing(_))
    }

    #[must_use]
    pub const fn is_complete(&self) -> bool {
        matches!(self, Self::Complete(_))
    }

    #[must_use]
    pub const fn as_complete(&self) -> Option<&T> {
        match self {
            Self::Complete(value) => Some(value),
            Self::Filtered { .. } | Self::Missing(_) => None,
        }
    }

    #[must_use]
    pub const fn as_present(&self) -> Option<&T> {
        match self {
            Self::Complete(value) | Self::Filtered { value, .. } => Some(value),
            Self::Missing(_) => None,
        }
    }

    #[must_use]
    pub fn map<U>(self, mapper: impl FnOnce(T) -> U) -> Material<U> {
        match self {
            Self::Complete(value) => Material::Complete(mapper(value)),
            Self::Filtered {
                value,
                scope,
                completeness,
            } => Material::Filtered {
                value: mapper(value),
                scope,
                completeness,
            },
            Self::Missing(reason) => Material::Missing(reason),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TOPIC_A: [u8; 32] = [0xa1; 32];
    const TOPIC_B: [u8; 32] = [0xb2; 32];
    const TOPIC_C: [u8; 32] = [0xc3; 32];

    fn address(byte: u8) -> Address {
        Address::new([byte; 20])
    }

    fn range(start: u64, end: u64) -> BlockRange {
        BlockRange::new(BlockNumber(start), BlockNumber(end)).expect("range")
    }

    fn topic(position: u8, alternatives: &[[u8; 32]]) -> TopicFilter {
        TopicFilter {
            position,
            alternatives: alternatives.to_vec(),
        }
    }

    fn topics(filters: &[TopicFilter]) -> FilterScope {
        FilterScope {
            topics: filters.to_vec(),
            ..FilterScope::default()
        }
    }

    fn narrow() -> FilterScope {
        FilterScope {
            block_range: Some(range(10, 20)),
            addresses: vec![address(1), address(2)],
            topics: vec![topic(0, &[TOPIC_A, TOPIC_B])],
            transaction_hashes: vec![TransactionHash::new([3; 32])],
            transaction_types: vec![2, 3],
            senders: vec![address(4)],
            recipients: vec![address(5)],
        }
    }

    /// `narrow()` with one field widened to its wildcard.
    fn widenings() -> Vec<FilterScope> {
        let mut widened = Vec::new();
        let mut scope = narrow();
        scope.block_range = None;
        widened.push(scope);
        let mut scope = narrow();
        scope.addresses.clear();
        widened.push(scope);
        let mut scope = narrow();
        scope.topics.clear();
        widened.push(scope);
        let mut scope = narrow();
        scope.transaction_hashes.clear();
        widened.push(scope);
        let mut scope = narrow();
        scope.transaction_types.clear();
        widened.push(scope);
        let mut scope = narrow();
        scope.senders.clear();
        widened.push(scope);
        let mut scope = narrow();
        scope.recipients.clear();
        widened.push(scope);
        widened
    }

    #[test]
    fn wildcards_cover_everything_and_are_covered_only_by_wildcards() {
        let wildcard = FilterScope::default();
        assert!(wildcard.covers(&wildcard));
        assert!(wildcard.covers(&narrow()));
        assert!(narrow().covers(&narrow()));
        assert!(!narrow().covers(&wildcard));
        for widened in widenings() {
            assert!(widened.covers(&narrow()), "{widened:?}");
            assert!(!narrow().covers(&widened), "{widened:?}");
        }
    }

    #[test]
    fn a_value_list_covers_its_own_non_empty_subsets() {
        let scope = FilterScope {
            addresses: vec![address(1), address(2)],
            ..FilterScope::default()
        };
        let filter = |addresses: Vec<Address>| FilterScope {
            addresses,
            ..FilterScope::default()
        };
        assert!(scope.covers(&filter(vec![address(2)])));
        assert!(scope.covers(&filter(vec![address(2), address(1)])));
        assert!(!scope.covers(&filter(vec![address(2), address(3)])));
        assert!(!scope.covers(&filter(vec![address(3)])));

        let senders = FilterScope {
            senders: vec![address(1)],
            ..FilterScope::default()
        };
        let recipients = FilterScope {
            recipients: vec![address(1)],
            ..FilterScope::default()
        };
        assert!(!senders.covers(&recipients));
        assert!(!recipients.covers(&senders));

        let types = FilterScope {
            transaction_types: vec![2, 3],
            ..FilterScope::default()
        };
        assert!(types.covers(&FilterScope {
            transaction_types: vec![3],
            ..FilterScope::default()
        }));
        assert!(!types.covers(&FilterScope {
            transaction_types: vec![1],
            ..FilterScope::default()
        }));
    }

    #[test]
    fn long_value_lists_are_compared_as_sets() {
        let many = (0..64).map(address).collect::<Vec<_>>();
        let scope = FilterScope {
            addresses: many.clone(),
            ..FilterScope::default()
        };
        let mut reversed = many;
        reversed.reverse();
        assert!(scope.covers(&FilterScope {
            addresses: reversed.clone(),
            ..FilterScope::default()
        }));
        reversed.push(address(200));
        assert!(!scope.covers(&FilterScope {
            addresses: reversed,
            ..FilterScope::default()
        }));
    }

    #[test]
    fn topic_coverage_follows_positions_and_alternatives() {
        let scope = topics(&[topic(0, &[TOPIC_A, TOPIC_B])]);
        assert!(scope.covers(&topics(&[topic(0, &[TOPIC_A])])));
        assert!(scope.covers(&topics(&[topic(0, &[TOPIC_B, TOPIC_A])])));
        assert!(scope.covers(&topics(&[topic(0, &[TOPIC_A]), topic(1, &[TOPIC_C])])));
        assert!(!scope.covers(&topics(&[topic(0, &[TOPIC_A, TOPIC_C])])));
        // The filter leaves position 0 open, so it also matches other topics there.
        assert!(!scope.covers(&topics(&[topic(1, &[TOPIC_A])])));
        // Several constraints on one position allow only their common topics.
        assert!(scope.covers(&topics(&[
            topic(0, &[TOPIC_A, TOPIC_C]),
            topic(0, &[TOPIC_A, TOPIC_B]),
        ])));
        assert!(!scope.covers(&topics(&[
            topic(0, &[TOPIC_A, TOPIC_C]),
            topic(0, &[TOPIC_C, TOPIC_B]),
        ])));
        let both = topics(&[topic(0, &[TOPIC_A, TOPIC_C]), topic(0, &[TOPIC_A, TOPIC_B])]);
        assert!(both.covers(&topics(&[topic(0, &[TOPIC_A])])));
        assert!(!both.covers(&topics(&[topic(0, &[TOPIC_B])])));
    }

    #[test]
    fn a_topic_without_alternatives_matches_nothing_rather_than_everything() {
        let impossible = topics(&[topic(0, &[])]);
        assert!(!impossible.covers(&topics(&[topic(0, &[TOPIC_A])])));
        assert!(impossible.covers(&impossible));
        assert!(topics(&[topic(0, &[TOPIC_A])]).covers(&impossible));
    }

    #[test]
    fn block_range_coverage_is_interval_containment() {
        let scope = FilterScope {
            block_range: Some(range(10, 20)),
            ..FilterScope::default()
        };
        let filter = |block_range| FilterScope {
            block_range,
            ..FilterScope::default()
        };
        assert!(scope.covers(&filter(Some(range(10, 20)))));
        assert!(scope.covers(&filter(Some(range(12, 15)))));
        assert!(!scope.covers(&filter(Some(range(9, 15)))));
        assert!(!scope.covers(&filter(Some(range(15, 21)))));
        assert!(!scope.covers(&filter(None)));
        assert!(filter(None).covers(&scope));
    }

    #[test]
    fn coverage_at_a_block_compares_block_ranges_only_at_that_block() {
        let scope = FilterScope {
            block_range: Some(BlockRange::single(BlockNumber(5))),
            addresses: vec![address(1)],
            ..FilterScope::default()
        };
        let filter = FilterScope {
            addresses: vec![address(1)],
            ..FilterScope::default()
        };
        assert!(!scope.covers(&filter));
        assert!(scope.covers_at(&filter, BlockNumber(5)));
        // The scope supplies nothing for another block.
        assert!(!scope.covers_at(&filter, BlockNumber(6)));
        // A filter that excludes the block needs nothing from it.
        let later = FilterScope {
            block_range: Some(range(10, 20)),
            ..filter.clone()
        };
        assert!(scope.covers_at(&later, BlockNumber(6)));
        // Every other field is still compared.
        let other = FilterScope {
            addresses: vec![address(2)],
            ..filter
        };
        assert!(!scope.covers_at(&other, BlockNumber(5)));
    }

    #[test]
    fn items_match_only_when_every_field_matches() {
        let scope = narrow();
        let hash = TransactionHash::new([3; 32]);
        assert!(scope.matches_block(BlockNumber(10)) && scope.matches_block(BlockNumber(20)));
        assert!(!scope.matches_block(BlockNumber(9)) && !scope.matches_block(BlockNumber(21)));

        assert!(scope.matches_transaction(2, Some(&address(5)), || hash));
        assert!(!scope.matches_transaction(1, Some(&address(5)), || hash));
        assert!(!scope.matches_transaction(2, Some(&address(6)), || hash));
        assert!(
            !scope.matches_transaction(2, None, || hash),
            "a contract creation has no recipient"
        );
        let other_hash = TransactionHash::new([9; 32]);
        assert!(!scope.matches_transaction(2, Some(&address(5)), || other_hash));
        assert!(scope.matches_sender(Some(&address(4))));
        assert!(!scope.matches_sender(Some(&address(5))));
        assert!(
            !scope.matches_sender(None),
            "an unknown sender matches only the wildcard"
        );

        assert!(scope.matches_log(&address(2), &[TOPIC_B]));
        assert!(!scope.matches_log(&address(3), &[TOPIC_B]));
        assert!(!scope.matches_log(&address(2), &[TOPIC_C]));
        assert!(
            !scope.matches_log::<[u8; 32]>(&address(2), &[]),
            "a log without the filtered topic"
        );

        let wildcard = FilterScope::default();
        assert!(wildcard.matches_block(BlockNumber(0)));
        assert!(wildcard.matches_transaction(0, None, || unreachable!("no hash is compared")));
        assert!(wildcard.matches_sender(None));
        assert!(wildcard.matches_log::<[u8; 32]>(&address(3), &[]));
    }

    #[test]
    fn log_topics_match_at_their_positions() {
        let second = topics(&[topic(1, &[TOPIC_B])]);
        assert!(second.matches_log(&address(1), &[TOPIC_A, TOPIC_B]));
        assert!(!second.matches_log(&address(1), &[TOPIC_B, TOPIC_A]));
        assert!(!second.matches_log(&address(1), &[TOPIC_B]));
        // Several filters at one position must all allow its topic.
        let both = topics(&[topic(0, &[TOPIC_A, TOPIC_B]), topic(0, &[TOPIC_B, TOPIC_C])]);
        assert!(both.matches_log(&address(1), &[TOPIC_B]));
        assert!(!both.matches_log(&address(1), &[TOPIC_A]));
        assert!(!topics(&[topic(0, &[])]).matches_log(&address(1), &[TOPIC_A]));
    }

    #[test]
    fn only_item_fields_constrain_their_items() {
        let wildcard = FilterScope::default();
        let constraints =
            |scope: &FilterScope| (scope.constrains_transactions(), scope.constrains_logs());
        assert_eq!(constraints(&wildcard), (false, false));
        assert_eq!(constraints(&narrow()), (true, true));
        // A block range is matched against the block, not against its items.
        let ranged = FilterScope {
            block_range: Some(range(1, 2)),
            ..FilterScope::default()
        };
        assert_eq!(constraints(&ranged), (false, false));
        for scope in [
            FilterScope {
                transaction_types: vec![3],
                ..FilterScope::default()
            },
            FilterScope {
                transaction_hashes: vec![TransactionHash::new([3; 32])],
                ..FilterScope::default()
            },
            FilterScope {
                senders: vec![address(4)],
                ..FilterScope::default()
            },
            FilterScope {
                recipients: vec![address(5)],
                ..FilterScope::default()
            },
        ] {
            assert_eq!(constraints(&scope), (true, false), "{scope:?}");
        }
        for scope in [
            FilterScope {
                addresses: vec![address(1)],
                ..FilterScope::default()
            },
            topics(&[topic(0, &[TOPIC_A])]),
        ] {
            assert_eq!(constraints(&scope), (false, true), "{scope:?}");
        }
    }

    #[test]
    fn empty_complete_is_not_missing_or_filtered() {
        let complete: Material<Vec<u8>> = Material::Complete(Vec::new());
        let missing: Material<Vec<u8>> = Material::Missing(MissingReason::NotRequested);
        let filtered = Material::Filtered {
            value: Vec::<u8>::new(),
            scope: FilterScope::default(),
            completeness: Completeness::DatasetDeclared,
        };

        assert_eq!(complete.kind(), MaterialKind::Complete);
        assert_eq!(missing.kind(), MaterialKind::Missing);
        assert_eq!(filtered.kind(), MaterialKind::Filtered);
        assert!(complete.as_complete().is_some());
        assert!(filtered.as_complete().is_none());
    }
}
