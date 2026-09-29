use leani_primitives::{
    BlockHash, BlockNumber, BlockRef, Capability, CapabilitySet, ChainId, FilterScope, Finality,
    LogFieldSet,
};
use leani_processor_api::{
    DataRequirement, DeliveryOrdering, EncodedDelta, LifecyclePolicies, ProcessorDescriptor,
    ProcessorId, ProcessorInstanceId, ProcessorSchemas, PublicationPolicy, ReductionMode,
    RetentionPolicy, StartPoint,
};
use semver::Version;

fn descriptor() -> ProcessorDescriptor {
    ProcessorDescriptor {
        id: ProcessorId::new("fixture").expect("processor ID"),
        instance: ProcessorInstanceId::new("fixture-main").expect("instance ID"),
        version: Version::new(1, 2, 3),
        code_hash: BlockHash::new([0x01; 32]),
        config_hash: BlockHash::new([0x02; 32]),
        start: StartPoint::Genesis,
        requirements: vec![DataRequirement {
            capabilities: CapabilitySet::of(Capability::Header),
            log_fields: LogFieldSet::NONE,
            allow_filtered: false,
            filter: FilterScope::default(),
            minimum_finality: Finality::Included,
        }],
        mode: ReductionMode::BlockLocal,
        delivery_ordering: DeliveryOrdering::BlockVersionedIdempotent,
        publication: PublicationPolicy::FinalizedOnly,
        lifecycle: LifecyclePolicies::from_legacy(RetentionPolicy::FullOutputHistory),
        schemas: ProcessorSchemas {
            delta_version: 1,
            entity_schema: "fixture.entity.v1".to_owned(),
            change_schema: "fixture.change.v1".to_owned(),
        },
    }
}

#[test]
fn encoded_delta_v1_encoding_is_stable() {
    let descriptor = descriptor();
    let delta = EncodedDelta::new(
        &descriptor,
        ChainId(1),
        BlockRef {
            number: BlockNumber(42),
            hash: BlockHash::new([0x11; 32]),
            parent_hash: BlockHash::new([0x22; 32]),
            timestamp: 1_700_000_000,
        },
        vec![0xde, 0xad, 0xbe, 0xef],
    );
    let encoded = delta.encode_durable().expect("encode fixture");
    let expected = include_str!("fixtures/encoded_delta_v1.hex").trim();
    assert_eq!(hex::encode(&encoded), expected);
    let decoded =
        EncodedDelta::decode_durable(&descriptor, &hex::decode(expected).expect("fixture hex"))
            .expect("decode fixture");
    assert_eq!(decoded, delta);
}
