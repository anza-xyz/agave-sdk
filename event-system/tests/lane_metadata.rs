#![cfg(target_os = "linux")]

mod common;

use {
    crate::common::{TEST_CONFIG, TEST_EVENT, TestContextBuilder, TestEvent},
    agave_event_system::{StreamConfig, publisher::PublishError},
    std::assert_matches,
};

#[test]
fn lanes_metadata_for_all_lanes() {
    const CAPACITY: usize = 4;

    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let (publisher_factory, subscriber) = test_context
        .create_stream_factory_with_subscriber::<TestEvent>(StreamConfig {
            capacity: CAPACITY,
            publisher_slots: 2,
            ..TEST_CONFIG
        });
    let mut publisher = publisher_factory.try_create_publisher().unwrap();

    publisher.publish_batch(&[TEST_EVENT; CAPACITY]).unwrap();
    assert_matches!(
        publisher.publish(&TEST_EVENT),
        Err(PublishError::FailedToSend)
    );

    let lanes_metadata = subscriber.lanes_metadata();
    let mut lanes_metadata_iter = lanes_metadata.iter();

    let first_lane = lanes_metadata_iter.next().unwrap();
    assert_eq!(first_lane.lane(), 0);
    assert_eq!(
        first_lane.rejected_items(),
        1,
        "last publish above failed, so rejection count should be 1"
    );

    let second_lane = lanes_metadata_iter.next().unwrap();
    assert_eq!(second_lane.lane(), 1);
    assert_eq!(
        second_lane.rejected_items(),
        0,
        "nothing is published on this lane so no rejected items"
    );

    assert_matches!(
        lanes_metadata_iter.next(),
        None,
        "the stream is configured with 2 publisher slots"
    );
}
