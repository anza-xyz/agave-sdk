#![cfg(target_os = "linux")]

use {
    agave_event_system::{
        CreateStreamError, Event, EventSystem, ProducerFactory, StreamConfig, event,
        subscriber::{self, AvailableStream, DecodedMessage},
    },
    rstest::rstest,
    std::{assert_matches, io::ErrorKind, path::PathBuf},
    tempfile::TempDir,
    wincode_dynamic::Value,
};

#[event]
#[derive(Debug, PartialEq, PartialOrd)]
struct TestEvent {
    value: u64,
}

#[event]
#[derive(Debug, PartialEq, PartialOrd)]
enum ComplexEvent {
    Variant1 {
        value: u64,
        time: u64,
        signature: [u8; 12],
    },
    Variant2 {
        value: u64,
        is_valid: bool,
        sender: [u8; 15],
    },
}

const TEST_CONFIG: StreamConfig = StreamConfig {
    capacity: 2,
    producer_slots: 1,
    consumer_slots: 1,
};

struct TestContext {
    event_system: EventSystem,
    directory: TempDir,
}

impl TestContext {
    fn new_event_system() -> Self {
        let directory = TempDir::new().unwrap();
        let event_system = EventSystem::new(directory.path()).unwrap();

        Self {
            event_system,
            directory,
        }
    }

    fn event_system_path(&self) -> PathBuf {
        self.directory.path().to_path_buf()
    }
}

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

#[rstest]
#[case::empty("")]
#[case::embedded_nul("stream\0name")]
#[case::current_directory(".")]
#[case::parent_directory("..")]
#[case::nested_path("nested/stream")]
#[case::absolute_path("/absolute")]
#[case::trailing_slash("trailing/")]
#[case::double_trailing_slash("trailing//")]
#[case::trailing_dot("trailing/.")]
fn create_stream_rejects_invalid_stream_names(#[case] invalid_name: &str) {
    let test_context = TestContext::new_event_system();

    assert_matches!(
        test_context.event_system.create_stream::<TestEvent>(invalid_name, TEST_CONFIG),
        Err(CreateStreamError::InvalidStreamName(name)) if name == invalid_name
    );
}

#[test]
fn create_stream_reserves_names_only_after_success() {
    let test_context = TestContext::new_event_system();
    const REUSED_STREAM_NAME: &str = "reused-stream-name";

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
    const REUSED_STREAM_NAME: &str = "reused-stream-name";
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
    let test_context = TestContext::new_event_system();
    let stream_config = StreamConfig {
        producer_slots,
        ..TEST_CONFIG
    };

    let producer_factory: ProducerFactory<TestEvent> = test_context
        .event_system
        .create_stream("test-stream", stream_config)
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

#[test]
fn subscriber_can_connect_to_stream_and_see_stream_info() {
    const STREAM_A_NAME: &str = "stream_a";
    const TEST_EVENT: TestEvent = TestEvent { value: 42 };

    let test_context = TestContext::new_event_system();
    let producer_factory: ProducerFactory<TestEvent> = test_context
        .event_system
        .create_stream(STREAM_A_NAME, TEST_CONFIG)
        .unwrap();

    let mut producer = producer_factory.try_create_producer().unwrap();

    let subscriber = subscriber::StreamExplorer::new(test_context.event_system_path());
    let mut available_streams: Vec<AvailableStream> = subscriber.available_streams().collect();

    assert_eq!(available_streams.len(), 1, "only one stream is published");
    let available_stream = available_streams.pop().unwrap();
    let mut subscriber = available_stream.try_connect_typed::<TestEvent>().unwrap();

    assert_eq!(STREAM_A_NAME, subscriber.stream_name());
    assert_eq!("TestEvent", subscriber.type_name());

    producer.emit_event(&TEST_EVENT).unwrap();
    let received_event = subscriber.try_recv().unwrap().decode().unwrap();

    assert_eq!(TEST_EVENT, received_event);
}

#[rstest]
fn events_emitted_by_producer_are_received_by_all_subscribers(
    #[values(1, 2, 4)] consumer_slots: usize,
) {
    use agave_event_system::subscriber::{TryConnectError, TryConnectTypedError};

    const TEST_EVENT: TestEvent = TestEvent { value: 42 };

    let test_context = TestContext::new_event_system();
    let stream_config = StreamConfig {
        consumer_slots,
        ..TEST_CONFIG
    };
    let producer_factory: ProducerFactory<TestEvent> = test_context
        .event_system
        .create_stream("test-stream", stream_config)
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
        let received_event = subscriber.try_recv().unwrap().decode().unwrap();
        assert_eq!(TEST_EVENT, received_event);
    }
}

struct ExpectedMessage {
    variant_name: Option<&'static str>,
    fields: Vec<(&'static str, Value<'static>)>,
}

#[rstest]
#[case::struct_events(
    vec![TestEvent { value: 42 }, TestEvent { value: 99 }],
    vec![
        ExpectedMessage {
            variant_name: None,
            fields: vec![("value", Value::U64(42))],
        },
        ExpectedMessage {
            variant_name: None,
            fields: vec![("value", Value::U64(99))],
        },
    ],
)]
#[case::enum_events(
    vec![
        ComplexEvent::Variant1 {
            value: 42,
            time: 123_456,
            signature: *b"signature123",
        },
        ComplexEvent::Variant2 {
            value: 99,
            is_valid: true,
            sender: *b"sender123456789",
        },
    ],
    vec![
        ExpectedMessage {
            variant_name: Some("Variant1"),
            fields: vec![
                ("value", Value::U64(42)),
                ("time", Value::U64(123_456)),
                ("signature", Value::Bytes(b"signature123".as_slice().into())),
            ],
        },
        ExpectedMessage {
            variant_name: Some("Variant2"),
            fields: vec![
                ("value", Value::U64(99)),
                ("is_valid", Value::Bool(true)),
                ("sender", Value::Bytes(b"sender123456789".as_slice().into())),
            ],
        },
    ],
)]
fn events_emitted_by_producer_are_received_by_untyped_subscriber<E: Event>(
    #[case] test_events: Vec<E>,
    #[case] expected_messages: Vec<ExpectedMessage>,
) {
    const STREAM_A_NAME: &str = "stream_a";

    let test_context = TestContext::new_event_system();
    let producer_factory: ProducerFactory<E> = test_context
        .event_system
        .create_stream(STREAM_A_NAME, TEST_CONFIG)
        .unwrap();

    let mut producer = producer_factory.try_create_producer().unwrap();

    let subscriber = subscriber::StreamExplorer::new(test_context.event_system_path());
    let mut available_streams: Vec<AvailableStream> = subscriber.available_streams().collect();

    assert_eq!(available_streams.len(), 1, "only one stream is published");
    let available_stream = available_streams.pop().unwrap();
    let mut subscriber = available_stream.try_connect_dynamic().unwrap();

    for event in &test_events {
        producer.emit_event(event).unwrap();
    }

    for expected_message in expected_messages {
        let received_message = subscriber.try_recv().unwrap();
        let (variant_name, mut fields) = match received_message.decode().unwrap() {
            DecodedMessage::Struct { fields } => (None, fields),
            DecodedMessage::Enum {
                variant_name,
                fields,
            } => (Some(variant_name), fields),
        };

        assert_eq!(variant_name, expected_message.variant_name);
        for (expected_name, expected_value) in expected_message.fields {
            let field = fields.next().unwrap().unwrap();
            assert_eq!(field.name(), expected_name);
            assert_eq!(field.value(), &expected_value);
        }
        assert!(fields.next().is_none(), "no extra fields are present");
    }
    assert!(matches!(
        subscriber.try_recv(),
        Err(subscriber::TryRecvError::Empty)
    ));
}
