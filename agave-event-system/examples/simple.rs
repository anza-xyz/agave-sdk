use {
    agave_event_system::{EventStreamConfig, EventSystem, event, new_subscriber},
    tempfile::TempDir,
};

#[event]
#[derive(Debug, PartialEq, Eq)]
enum SlotEvents {
    ReplaySlotComplete {
        slot: u64,
        num_shreds: u64,
        num_entries: u64,
        num_txs: u64,
    },
}

#[event]
#[derive(Debug, PartialEq, Eq)]
enum ShredEvents {
    ShredReceived { shred_id: u64, sender_id: u64 },
}

const SIMPLE_CONFIG: EventStreamConfig = EventStreamConfig {
    producer_slots: 1,
    consumer_slots: 1,
    capacity: 2,
};

fn main() {
    let tmp_dir = TempDir::new().unwrap();
    let event_system_directory = tmp_dir.path().join("event-system");
    let system = EventSystem::create(&event_system_directory).unwrap();

    let slot_events = system
        .create_event_handle("slot_events", SIMPLE_CONFIG)
        .unwrap();
    let shred_events = system
        .create_event_handle("shred_events", SIMPLE_CONFIG)
        .unwrap();

    let mut subscriber = new_subscriber(event_system_directory).unwrap();

    let sent_slot_event = SlotEvents::ReplaySlotComplete {
        slot: 75_491,
        num_shreds: 200,
        num_entries: 120,
        num_txs: 4096,
    };
    let mut slot_producer = slot_events.try_create_producer().unwrap();
    slot_producer.publish_event(&sent_slot_event).unwrap();

    let sent_shred_event = ShredEvents::ShredReceived {
        shred_id: 42,
        sender_id: 7,
    };
    let mut shred_producer = shred_events.try_create_producer().unwrap();
    shred_producer.publish_event(&sent_shred_event).unwrap();

    for _ in 0..2 {
        let received = subscriber.try_recv().unwrap();
        match received.metadata.event_stream_name() {
            "slot_events" => {
                assert_eq!(received.metadata.event_name(), "ReplaySlotComplete");
                assert_eq!(
                    wincode::deserialize::<SlotEvents>(&received.message.payload).unwrap(),
                    sent_slot_event
                );
            }
            "shred_events" => {
                assert_eq!(received.metadata.event_name(), "ShredReceived");
                assert_eq!(
                    wincode::deserialize::<ShredEvents>(&received.message.payload).unwrap(),
                    sent_shred_event
                );
            }
            event_stream_name => panic!("unexpected event stream: {event_stream_name}"),
        }
    }
}
