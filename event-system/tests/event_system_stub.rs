use agave_event_system::{EventSystem, StreamConfig, event, stream_name};

#[cfg(not(target_os = "linux"))]
#[event]
struct TestEvent {
    value: u64,
}

#[test]
#[cfg(not(target_os = "linux"))]
fn event_system_is_a_no_op() {
    let event_system_directory = std::env::temp_dir().join(format!(
        "agave-event-system-stub-test-{}",
        std::process::id()
    ));
    assert!(!event_system_directory.exists());

    let event_system = EventSystem::new(&event_system_directory).unwrap();
    let publisher_factory = event_system
        .create_stream::<TestEvent>(
            stream_name!("test-stream"),
            StreamConfig {
                capacity: 0,
                publisher_slots: 0,
                subscriber_slots: 0,
            },
        )
        .unwrap();
    let _cloned_publisher_factory = publisher_factory.clone();

    assert!(!event_system_directory.exists());
}

#[event(max_serialized_size = 16)]
struct DynamicEvent {
    value: String,
}

#[test]
fn explicit_stub_produces_no_op_publishers() {
    let event_system: EventSystem = EventSystem::stub();
    let cloned_system = event_system.clone();
    event_system.set_stream_policy("on".parse().unwrap());

    // Invalid queue limits and duplicate names are ignored by the stub.
    for system in [&event_system, &cloned_system] {
        let factory: agave_event_system::PublisherFactory<DynamicEvent> = system
            .create_stream(
                stream_name!("test-stream"),
                StreamConfig {
                    capacity: 0,
                    publisher_slots: 0,
                    subscriber_slots: 0,
                },
            )
            .unwrap();
        let cloned_factory = factory.clone();
        for factory in [&factory, &cloned_factory] {
            let mut publisher: agave_event_system::publisher::Publisher<DynamicEvent> =
                factory.try_create_publisher().unwrap();
            let _another_publisher = factory.try_create_publisher().unwrap();
            // This exceeds the event's encoding limit, but stubs never serialize.
            let event = DynamicEvent {
                value: "x".repeat(1024),
            };
            publisher.publish(&event).unwrap();
            publisher.publish_batch(&[event]).unwrap();
            publisher.publish_batch(&[]).unwrap();
        }
    }
}
