#![cfg(target_os = "linux")]

mod common;

use {
    crate::common::TestContextBuilder,
    agave_event_system::{
        CreateStreamError, Event, EventSystem, ProducerFactory, StreamConfig,
        stream_name::StreamName,
        subscriber::{
            self, AvailableStream, DecodedMessage, TryConnectError, TryConnectTypedError,
        },
    },
    common::{TEST_CONFIG, TEST_STREAM_NAME, TestEnumEvent, TestEvent},
    rstest::rstest,
    std::{assert_matches, io::ErrorKind},
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
fn create_event_system_fails_when_directory_is_reused() {
    let directory = TempDir::new().unwrap();
    let path = directory.path();

    let _event_system = EventSystem::new(path).unwrap();

    let event_system_with_reused_path_result = EventSystem::new(path);
    assert_matches!(event_system_with_reused_path_result, Err(_));
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
    let _producer_factory = test_context
        .event_system
        .create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG)
        .expect("test-events is unused stream name as it failed above");

    assert_matches!(
        test_context.event_system.create_stream::<TestEvent>(REUSED_STREAM_NAME, TEST_CONFIG),
        Err(CreateStreamError::FileSystem(error))
            if matches!(
                error.kind(),
                // Linux permits EEXIST or ENOTEMPTY for a nonempty destination.
                ErrorKind::AlreadyExists | ErrorKind::DirectoryNotEmpty
            ),
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
        Ok(ProducerFactory { .. }),
        "all factory handles are dropped, recycling the stream name to be reused."
    );
}

#[rstest]
fn producer_creation_respects_slot_limit(#[values(1, 2, 4)] producer_slots: usize) {
    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();

    let stream_config = StreamConfig {
        producer_slots,
        ..TEST_CONFIG
    };

    let producer_factory: ProducerFactory<TestEvent> = test_context
        .event_system
        .create_stream(TEST_STREAM_NAME, stream_config)
        .unwrap();

    for _ in 0..producer_slots {
        producer_factory
            .try_create_producer()
            .expect("producer slot is available");
    }

    assert_matches!(
        producer_factory.try_create_producer(),
        None,
        "producer slots are exhausted"
    );
}

#[rstest]
fn typed_subscribers_can_connect_and_receive_events(#[values(1, 2)] consumer_slots: usize) {
    const TEST_EVENT: TestEvent = TestEvent { value: 42 };

    let test_context = TestContextBuilder::new()
        .with_policy_enabling_all_streams()
        .build();
    let stream_config = StreamConfig {
        consumer_slots,
        ..TEST_CONFIG
    };
    let producer_factory: ProducerFactory<TestEvent> = test_context
        .event_system
        .create_stream(TEST_STREAM_NAME, stream_config)
        .unwrap();
    let mut producer = producer_factory.try_create_producer().unwrap();

    let explorer = subscriber::StreamExplorer::new(test_context.event_system_path());
    let connect_subscriber = || {
        explorer
            .available_streams()
            .next()
            .unwrap()
            .try_connect_typed::<TestEvent>()
    };
    let mut subscribers: Vec<_> = (0..consumer_slots)
        .map(|_| connect_subscriber().unwrap())
        .collect();

    assert_matches!(
        connect_subscriber(),
        Err(TryConnectTypedError::Connection(
            TryConnectError::SubscriberSlotsExhausted
        ))
    );

    producer.emit_event(&TEST_EVENT).unwrap();
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
    let producer_factory: ProducerFactory<E> = test_context
        .event_system
        .create_stream(TEST_STREAM_NAME, TEST_CONFIG)
        .unwrap();

    let mut producer = producer_factory.try_create_producer().unwrap();

    let subscriber = subscriber::StreamExplorer::new(test_context.event_system_path());
    let mut available_streams: Vec<AvailableStream> = subscriber.available_streams().collect();

    assert_eq!(available_streams.len(), 1, "only one stream is published");
    let available_stream = available_streams.pop().unwrap();
    let mut subscriber = available_stream.try_connect_dynamic().unwrap();

    producer.emit_event(&event).unwrap();
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
