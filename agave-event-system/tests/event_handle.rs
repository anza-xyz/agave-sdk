#![cfg(target_os = "linux")]

use {
    agave_event_system::{
        EmitEventError, Event, EventStreamConfig, EventSystem, NewSubscriberError, event,
        new_subscriber,
    },
    std::{assert_matches, fs::OpenOptions},
    tempfile::TempDir,
};

const SLOT_EVENTS_STREAM_NAME: &str = "slot_events";
const SHRED_RECEIVED_EVENT_STREAM_NAME: &str = "shred_received_event";
const DYNAMIC_EVENTS_STREAM_NAME: &str = "dynamic_events";
const COMPLETED_EVENT_NAME: &str = "Completed";
const SHRED_RECEIVED_EVENT_NAME: &str = "ShredReceivedEvent";
const DYNAMIC_EVENT_NAME: &str = "DynamicEvent";

#[event]
#[derive(Debug, PartialEq, Eq)]
enum SlotEvents {
    Completed { slot: u64 },
}

#[event]
#[derive(Debug, PartialEq, Eq)]
struct ShredReceivedEvent {
    shred_id: u64,
}

#[event(max_serialized_size = 64)]
#[derive(Debug, PartialEq, Eq)]
struct DynamicEvent {
    values: Vec<u8>,
}

const SIMPLE_EVENT_STREAM_CONFIG: EventStreamConfig = EventStreamConfig {
    capacity: 2,
    producer_slots: 1,
    consumer_slots: 1,
};

#[test]
fn dynamic_event_uses_declared_max_serialized_size() {
    assert_eq!(
        std::mem::size_of::<<DynamicEvent as Event>::QueueCell>(),
        64
    );

    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(&event_system_directory).unwrap();

    let event_handle = system
        .create_event_handle::<DynamicEvent>(DYNAMIC_EVENTS_STREAM_NAME, SIMPLE_EVENT_STREAM_CONFIG)
        .unwrap();
    let mut subscriber = new_subscriber(event_system_directory).unwrap();

    let mut producer = event_handle.try_create_producer().unwrap();
    let sent = DynamicEvent {
        values: vec![1, 2, 3, 4],
    };
    producer.publish_event(&sent).unwrap();

    let received = subscriber.try_recv().unwrap();
    assert_eq!(
        received.metadata.event_stream_name(),
        DYNAMIC_EVENTS_STREAM_NAME
    );
    assert_eq!(received.metadata.event_name(), DYNAMIC_EVENT_NAME);
    assert_eq!(
        wincode::deserialize::<DynamicEvent>(&received.message.payload).unwrap(),
        sent
    );

    let oversized = DynamicEvent {
        values: vec![0; 64],
    };
    assert_matches!(
        producer.publish_event(&oversized),
        Err(EmitEventError::Serialization(_))
    );
}

#[cfg(target_os = "linux")]
fn current_thread_id() -> u64 {
    // SAFETY: `gettid` has no caller requirements.
    let tid = unsafe { libc::gettid() };
    tid as u64
}

#[cfg(target_os = "macos")]
fn current_thread_id() -> u64 {
    let mut thread_id = 0;
    // SAFETY: Passing zero requests the calling thread's ID, and `thread_id`
    // points to valid writable memory.
    let error = unsafe { libc::pthread_threadid_np(0, &mut thread_id) };
    assert_eq!(error, 0);
    thread_id
}

#[test]
fn external_event_streams_get_distinct_queues() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(&event_system_directory).unwrap();

    let slot_events = system
        .create_event_handle::<SlotEvents>(SLOT_EVENTS_STREAM_NAME, SIMPLE_EVENT_STREAM_CONFIG)
        .unwrap();
    let shred_events = system
        .create_event_handle::<ShredReceivedEvent>(
            SHRED_RECEIVED_EVENT_STREAM_NAME,
            SIMPLE_EVENT_STREAM_CONFIG,
        )
        .unwrap();

    for event_stream_name in [SLOT_EVENTS_STREAM_NAME, SHRED_RECEIVED_EVENT_STREAM_NAME] {
        let event_stream_directory = event_system_directory
            .join("event-streams")
            .join(event_stream_name);
        let queue_path = event_stream_directory.join("queue");
        assert!(queue_path.is_file());
        assert!(
            std::fs::symlink_metadata(&queue_path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert!(event_stream_directory.join("schema").is_file());
    }

    let mut subscriber = new_subscriber(event_system_directory).unwrap();

    let sent_slot = SlotEvents::Completed { slot: 123 };
    slot_events
        .try_create_producer()
        .unwrap()
        .publish_event(&sent_slot)
        .unwrap();

    let sent_shred = ShredReceivedEvent { shred_id: 456 };
    shred_events
        .try_create_producer()
        .unwrap()
        .publish_event(&sent_shred)
        .unwrap();

    let mut received_slot = false;
    let mut received_shred = false;
    for _ in 0..2 {
        let received = subscriber.try_recv().unwrap();
        assert_eq!(received.metadata.thread_id, current_thread_id());
        match received.metadata.event_stream_name() {
            SLOT_EVENTS_STREAM_NAME => {
                assert_eq!(received.metadata.event_name(), COMPLETED_EVENT_NAME);
                assert_eq!(
                    wincode::deserialize::<SlotEvents>(&received.message.payload).unwrap(),
                    sent_slot
                );
                received_slot = true;
            }
            SHRED_RECEIVED_EVENT_STREAM_NAME => {
                assert_eq!(received.metadata.event_name(), SHRED_RECEIVED_EVENT_NAME);
                assert_eq!(
                    wincode::deserialize::<ShredReceivedEvent>(&received.message.payload).unwrap(),
                    sent_shred
                );
                received_shred = true;
            }
            event_stream_name => panic!("unexpected event stream: {event_stream_name}"),
        }
    }
    assert!(received_slot);
    assert!(received_shred);
}

#[test]
fn sealed_file_cannot_be_resized() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(&event_system_directory).unwrap();
    let _event_handle = system
        .create_event_handle::<SlotEvents>(SLOT_EVENTS_STREAM_NAME, SIMPLE_EVENT_STREAM_CONFIG)
        .unwrap();
    let queue_path = event_system_directory
        .join("event-streams")
        .join(SLOT_EVENTS_STREAM_NAME)
        .join("queue");
    let queue_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(queue_path)
        .unwrap();
    let queue_size = queue_file.metadata().unwrap().len();

    assert_eq!(
        queue_file.set_len(0).unwrap_err().raw_os_error(),
        Some(libc::EPERM)
    );
    assert_eq!(
        queue_file
            .set_len(queue_size.saturating_add(1))
            .unwrap_err()
            .raw_os_error(),
        Some(libc::EPERM)
    );
}

#[test]
fn struct_event_can_be_published_and_received() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(event_system_directory.clone()).unwrap();

    let event_handle = system
        .create_event_handle(SHRED_RECEIVED_EVENT_STREAM_NAME, SIMPLE_EVENT_STREAM_CONFIG)
        .unwrap();

    let mut subscriber = new_subscriber(event_system_directory).unwrap();
    let sent = ShredReceivedEvent { shred_id: 123 };
    event_handle
        .try_create_producer()
        .unwrap()
        .publish_event(&sent)
        .unwrap();

    let received = subscriber.try_recv().unwrap();
    assert_eq!(
        received.metadata.event_stream_name(),
        SHRED_RECEIVED_EVENT_STREAM_NAME
    );
    assert_eq!(received.metadata.event_name(), SHRED_RECEIVED_EVENT_NAME);
    assert_eq!(
        wincode::deserialize::<ShredReceivedEvent>(&received.message.payload).unwrap(),
        sent
    );
}

#[test]
fn producer_keeps_published_queue_file_alive() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(&event_system_directory).unwrap();
    let event_handle = system
        .create_event_handle::<SlotEvents>(SLOT_EVENTS_STREAM_NAME, SIMPLE_EVENT_STREAM_CONFIG)
        .unwrap();
    let mut producer = event_handle.try_create_producer().unwrap();

    drop(event_handle);
    drop(system);

    let mut subscriber = new_subscriber(event_system_directory).unwrap();
    let sent = SlotEvents::Completed { slot: 123 };
    producer.publish_event(&sent).unwrap();

    let received = subscriber.try_recv().unwrap();
    assert_eq!(
        wincode::deserialize::<SlotEvents>(&received.message.payload).unwrap(),
        sent
    );
}

#[test]
fn subscriber_rejects_queue_without_resize_seals() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(&event_system_directory).unwrap();
    let _event_handle = system
        .create_event_handle::<SlotEvents>(SLOT_EVENTS_STREAM_NAME, SIMPLE_EVENT_STREAM_CONFIG)
        .unwrap();
    let queue_path = event_system_directory
        .join("event-streams")
        .join(SLOT_EVENTS_STREAM_NAME)
        .join("queue");
    std::fs::remove_file(&queue_path).unwrap();
    std::fs::write(queue_path, []).unwrap();

    let error = new_subscriber(event_system_directory).unwrap_err();
    assert!(matches!(
        error,
        NewSubscriberError::ValidateEventStreamQueue(_)
    ));
}

#[test]
fn subscriber_ignores_event_stream_created_after_startup() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(&event_system_directory).unwrap();

    let mut subscriber = new_subscriber(event_system_directory).unwrap();

    let slot_events = system
        .create_event_handle::<SlotEvents>(SLOT_EVENTS_STREAM_NAME, SIMPLE_EVENT_STREAM_CONFIG)
        .unwrap();
    let mut producer = slot_events.try_create_producer().unwrap();
    let sent = SlotEvents::Completed { slot: 789 };
    producer.publish_event(&sent).unwrap();

    assert!(subscriber.try_recv().is_none());
}

#[test]
fn subscriber_creation_fails_when_an_event_stream_cannot_be_joined() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(&event_system_directory).unwrap();

    let _slot_events = system
        .create_event_handle::<SlotEvents>(SLOT_EVENTS_STREAM_NAME, SIMPLE_EVENT_STREAM_CONFIG)
        .unwrap();

    let _subscriber = new_subscriber(event_system_directory.clone()).unwrap();
    let error = new_subscriber(event_system_directory).unwrap_err();

    assert!(matches!(error, NewSubscriberError::JoinEventStreamQueue(_)));
}

#[test]
fn malformed_event_stream_does_not_block_valid_event_streams() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(&event_system_directory).unwrap();

    let malformed_event_stream = event_system_directory
        .join("event-streams")
        .join("malformed_events");
    std::fs::create_dir(&malformed_event_stream).unwrap();
    std::fs::write(malformed_event_stream.join("schema"), b"incomplete schema").unwrap();

    let slot_events = system
        .create_event_handle::<SlotEvents>(SLOT_EVENTS_STREAM_NAME, SIMPLE_EVENT_STREAM_CONFIG)
        .unwrap();

    let mut subscriber = new_subscriber(event_system_directory).unwrap();
    let sent = SlotEvents::Completed { slot: 321 };
    slot_events
        .try_create_producer()
        .unwrap()
        .publish_event(&sent)
        .unwrap();

    let received = subscriber.try_recv().unwrap();
    assert_eq!(
        received.metadata.event_stream_name(),
        SLOT_EVENTS_STREAM_NAME
    );
    assert_eq!(
        wincode::deserialize::<SlotEvents>(&received.message.payload).unwrap(),
        sent
    );
}
