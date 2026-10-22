#![cfg(target_os = "linux")]

mod common;

use {
    crate::common::TestContextBuilder,
    agave_event_system::{
        CreateStreamError, Event, EventSystem, PublisherFactory, StreamConfig, event,
        publisher::{PublishError, Publisher},
        stream_name::StreamName,
        subscriber::{
            self, AvailableStream, DecodedMessage, TryConnectError, TryConnectTypedError,
        },
    },
    common::{
        TEST_CONFIG, TEST_EVENT, TEST_STREAM_NAME, TestEnumEvent, TestEvent, assert_is_empty,
        assert_received,
    },
    rstest::rstest,
    std::assert_matches,
    tempfile::TempDir,
    wincode_dynamic::Value,
};

#[test]
fn create_event_system_fails_when_path_is_a_file() {
    const EXISTING_CONTENTS: &[u8] = b"existing file contents are preserved";

    let event_system_directory = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(&event_system_directory, EXISTING_CONTENTS).unwrap();

    assert_matches!(EventSystem::new(&event_system_directory), Err(_));
    assert_eq!(
        std::fs::read(&event_system_directory).unwrap(),
        EXISTING_CONTENTS
    );
}

#[test]
fn create_event_system_fails_when_directory_is_not_an_event_system_directory() {
    let directory = TempDir::new().unwrap();
    std::fs::create_dir(directory.path().join("unrelated")).unwrap();

    assert_matches!(EventSystem::new(directory.path()), Err(_));
}

#[test]
fn event_systems_can_share_a_directory() {
    const FIRST_STREAM_NAME: StreamName = agave_event_system::stream_name!("first-stream");
    const SECOND_STREAM_NAME: StreamName = agave_event_system::stream_name!("second-stream");

    let directory = TempDir::new().unwrap();
    let event_system_path = directory.path().join("event-system");

    let first_event_system = EventSystem::new(&event_system_path).unwrap();
    let second_event_system = EventSystem::new(&event_system_path).unwrap();

    first_event_system.set_stream_policy("on".parse().unwrap());
    second_event_system.set_stream_policy("on".parse().unwrap());

    let first_publisher_factory = first_event_system
        .create_stream::<TestEvent>(FIRST_STREAM_NAME, TEST_CONFIG)
        .unwrap();
    let second_publisher_factory = second_event_system
        .create_stream::<TestEvent>(SECOND_STREAM_NAME, TEST_CONFIG)
        .unwrap();

    let mut second_publisher = second_publisher_factory.try_create_publisher().unwrap();

    let explorer = subscriber::StreamExplorer::new(event_system_path);
    assert_eq!(explorer.available_streams().count(), 2);

    drop(first_publisher_factory);
    drop(first_event_system);

    let mut available_streams: Vec<AvailableStream> = explorer.available_streams().collect();
    assert_eq!(
        available_streams.len(),
        1,
        "only the first event system's stream is removed"
    );
    let mut subscriber = available_streams
        .pop()
        .unwrap()
        .try_connect_typed::<TestEvent>()
        .unwrap();
    assert_eq!(&SECOND_STREAM_NAME, subscriber.stream_name());

    second_publisher.publish(&TEST_EVENT).unwrap();
    assert_received([&mut subscriber]);
}

#[test]
fn stream_name_is_reserved_across_event_systems_until_dropped() {
    let directory = TempDir::new().unwrap();
    let first_event_system = EventSystem::new(directory.path()).unwrap();
    let second_event_system = EventSystem::new(directory.path()).unwrap();

    let first_publisher_factory = first_event_system
        .create_stream::<TestEvent>(TEST_STREAM_NAME, TEST_CONFIG)
        .unwrap();
    let first_publisher = first_publisher_factory.try_create_publisher().unwrap();

    assert_matches!(
        second_event_system.create_stream::<TestEvent>(TEST_STREAM_NAME, TEST_CONFIG),
        Err(CreateStreamError::StreamNameAlreadyInUse)
    );

    drop(first_publisher_factory);

    assert_matches!(
        second_event_system.create_stream::<TestEvent>(TEST_STREAM_NAME, TEST_CONFIG),
        Err(CreateStreamError::StreamNameAlreadyInUse)
    );

    drop(first_publisher);

    assert_matches!(
        second_event_system.create_stream::<TestEvent>(TEST_STREAM_NAME, TEST_CONFIG),
        Ok(_),
        "the first event system's stream should no longer be reserving the name after dropping \
         both its publisher factory and its publisher"
    );
}

#[test]
fn create_stream_reserves_names_only_after_success() {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    const REUSED_STREAM_NAME: StreamName = agave_event_system::stream_name!("reused-stream-name");

    let invalid_config = StreamConfig {
        capacity: 0,
        ..TEST_CONFIG
    };

    assert_matches!(
        test_context
            .event_system
            .create_stream::<TestEvent>(REUSED_STREAM_NAME, invalid_config),
        Err(CreateStreamError::Queue(_))
    );
    let _publisher_factory = test_context
        .event_system
        .create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG)
        .expect("test-events is unused stream name as it failed above");

    assert_matches!(
        test_context
            .event_system
            .create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG),
        Err(CreateStreamError::StreamNameAlreadyInUse),
        "creation of the same stream name must now fail, since it succeeded above."
    );
}

#[test]
fn stream_can_be_recreated_after_dropping_all_handles() {
    const REUSED_STREAM_NAME: StreamName = TEST_STREAM_NAME;
    let directory = TempDir::new().unwrap();
    let event_system = EventSystem::new(directory.path()).unwrap();

    let factory_1 = event_system
        .create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG)
        .unwrap();
    let factory_2 = factory_1.clone();

    drop(factory_1);

    assert_matches!(
        event_system.create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG),
        Err(_),
        "factory_2 is still alive preventing re-creation"
    );

    drop(factory_2);
    assert_matches!(
        event_system.create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG),
        Ok(PublisherFactory { .. }),
        "all factory handles are dropped, recycling the stream name to be reused."
    );
}

#[rstest]
fn publisher_creation_respects_slot_limit(#[values(1, 2, 4)] publisher_slots: usize) {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();

    let stream_config = StreamConfig {
        publisher_slots,
        ..TEST_CONFIG
    };

    let publisher_factory: PublisherFactory<TestEvent> = test_context
        .event_system
        .create_stream(TEST_STREAM_NAME, stream_config)
        .unwrap();

    for _ in 0..publisher_slots {
        publisher_factory
            .try_create_publisher()
            .expect("publisher slot is available");
    }

    assert_matches!(
        publisher_factory.try_create_publisher(),
        None,
        "publisher slots are exhausted"
    );
}

#[rstest]
fn typed_subscribers_can_connect_and_receive_events(#[values(1, 2)] subscriber_slots: usize) {
    const TEST_EVENT: TestEvent = TestEvent { value: 42 };

    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let stream_config = StreamConfig {
        subscriber_slots,
        ..TEST_CONFIG
    };
    let publisher_factory: PublisherFactory<TestEvent> = test_context
        .event_system
        .create_stream(TEST_STREAM_NAME, stream_config)
        .unwrap();
    let mut publisher = publisher_factory.try_create_publisher().unwrap();

    let explorer = subscriber::StreamExplorer::new(test_context.event_system_path());
    let connect_subscriber = || {
        explorer
            .available_streams()
            .next()
            .unwrap()
            .try_connect_typed::<TestEvent>()
    };
    let mut subscribers: Vec<_> = (0..subscriber_slots)
        .map(|_| connect_subscriber().unwrap())
        .collect();

    assert_matches!(
        connect_subscriber(),
        Err(TryConnectTypedError::Connection(
            TryConnectError::SubscriberSlotsExhausted
        ))
    );

    publisher.publish(&TEST_EVENT).unwrap();
    for subscriber in &mut subscribers {
        assert_eq!(&TEST_STREAM_NAME, subscriber.stream_name());
        assert_eq!("TestEvent", subscriber.type_name());

        let received_event = subscriber.try_recv().unwrap().decode().unwrap();
        assert_eq!(TEST_EVENT, received_event);
    }
}

#[rstest]
#[case::struct_event(TestEvent { value: 42 }, None)]
#[case::enum_event(TestEnumEvent::Value { value: 42 }, Some("Value"))]
fn dynamic_subscriber_can_connect_and_decode_events<E: Event>(
    #[case] event: E,
    #[case] expected_variant_name: Option<&str>,
) {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let publisher_factory: PublisherFactory<E> = test_context
        .event_system
        .create_stream(TEST_STREAM_NAME, TEST_CONFIG)
        .unwrap();

    let mut publisher = publisher_factory.try_create_publisher().unwrap();

    let subscriber = subscriber::StreamExplorer::new(test_context.event_system_path());
    let mut available_streams: Vec<AvailableStream> = subscriber.available_streams().collect();

    assert_eq!(available_streams.len(), 1, "only one stream is published");
    let available_stream = available_streams.pop().unwrap();
    let mut subscriber = available_stream.try_connect_dynamic().unwrap();

    publisher.publish(&event).unwrap();
    let received_message = subscriber.try_recv().unwrap();
    let (variant_name, mut fields) = match received_message.decode().unwrap() {
        DecodedMessage::Struct { fields } => (None, fields),
        DecodedMessage::Enum {
            variant_name,
            fields,
        } => (Some(variant_name), fields),
    };
    assert_eq!(variant_name, expected_variant_name);

    // Fields decode lazily, so consume the iterator to exercise payload decoding.
    let field = fields.next().unwrap().unwrap();
    assert_eq!(field.name(), "value");
    assert_eq!(field.value(), &Value::U64(42));
    assert!(fields.next().is_none(), "no extra fields are present");
}

#[event(max_serialized_size = 16)]
#[derive(Debug, PartialEq)]
struct BoundedEvent {
    values: Vec<u8>,
}

fn fitting_event() -> BoundedEvent {
    BoundedEvent { values: vec![1] }
}

/// Serializes to more than the `max_serialized_size` of [`BoundedEvent`].
fn oversized_event() -> BoundedEvent {
    BoundedEvent {
        values: vec![2; 16],
    }
}

#[rstest]
#[case::single(|publisher: &mut Publisher<BoundedEvent>| publisher.publish(&oversized_event()))]
#[case::batch(|publisher: &mut Publisher<BoundedEvent>| {
    publisher.publish_batch(&[fitting_event(), oversized_event()])
})]
fn failed_serialization_sends_nothing(
    #[case] publish_oversized: fn(&mut Publisher<BoundedEvent>) -> Result<(), PublishError>,
) {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let (mut publisher, mut subscriber) = test_context.create_stream_with_subscriber();

    assert_matches!(
        publish_oversized(&mut publisher),
        Err(PublishError::Serialization(_))
    );
    assert_is_empty([&mut subscriber]);

    publisher.publish(&fitting_event()).unwrap();
    let received_event = subscriber.try_recv().unwrap().decode().unwrap();
    assert_eq!(received_event, fitting_event());
    assert_is_empty([&mut subscriber]);
}
