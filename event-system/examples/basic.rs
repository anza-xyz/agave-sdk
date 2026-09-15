use {
    agave_event_system::{
        EventSystem, StreamConfig, event,
        subscriber::{DecodedMessage, StreamExplorer},
        wincode,
    },
    std::error::Error,
};

#[event]
#[derive(Debug)]
enum JobEvent {
    Started { job_id: u64 },
    Finished { job_id: u64 },
}

fn main() -> Result<(), Box<dyn Error>> {
    if !cfg!(target_os = "linux") {
        return Err("This example requires Linux.".into());
    }

    let directory = tempfile::tempdir()?;
    let event_system = EventSystem::new(directory.path())?;
    let factory = event_system.create_stream::<JobEvent>(
        "jobs",
        StreamConfig {
            capacity: 8,
            producer_slots: 1,
            consumer_slots: 2,
        },
    )?;
    let mut producer = factory.try_create_producer().ok_or("No producer slots")?;

    let explorer = StreamExplorer::new(directory.path().to_path_buf());
    let mut typed = explorer
        .available_streams()
        .next()
        .ok_or("Stream not found")?
        .try_connect_typed::<JobEvent>()?;
    let mut dynamic = explorer
        .available_streams()
        .next()
        .ok_or("Stream not found")?
        .try_connect_dynamic()?;

    for event in [
        JobEvent::Started { job_id: 42 },
        JobEvent::Finished { job_id: 42 },
    ] {
        producer.emit_event(&event)?;
        println!("Typed: {:?}", typed.try_recv()?.decode()?);

        let message = dynamic.try_recv()?;
        if let DecodedMessage::Enum {
            variant_name,
            fields,
        } = message.decode()?
        {
            println!("Dynamic: {variant_name}");
            for field in fields {
                let field = field?;
                println!("  {} = {:?}", field.name(), field.value());
            }
        }
    }

    Ok(())
}
