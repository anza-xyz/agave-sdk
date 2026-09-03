use {
    crate::{
        EVENT_QUEUE_FILE_NAME, EVENT_SCHEMA_FILE_NAME, EVENT_STREAMS_DIRECTORY_NAME, queue_file,
    },
    shaq::broadcast::SliceConsumer,
    std::{
        fs::OpenOptions,
        path::{Path, PathBuf},
        sync::Arc,
    },
    thiserror::Error,
    wincode_dynamic::{RootSchema, Value},
};

#[derive(Debug, Clone)]
pub struct EventPayload<'a> {
    pub schema: &'a RootSchema,
    pub payload: Box<[u8]>,
}

#[derive(Debug, Clone)]
pub struct ReceivedTracingEvent<'a> {
    pub message: EventPayload<'a>,
    pub metadata: ReceivedMetadata<'a>,
}

#[derive(Debug, Clone)]
pub struct ReceivedMetadata<'a> {
    event_stream_name: &'a str,
    event_name: &'a str,
    /// thread id of the thread that emitted the event
    pub thread_id: u64,
}

impl<'a> ReceivedMetadata<'a> {
    pub fn event_stream_name(&self) -> &str {
        self.event_stream_name
    }

    pub fn event_name(&self) -> &str {
        self.event_name
    }
}

#[derive(Debug)]
pub struct EventSubscriber {
    state: SubscriberState,
}

#[derive(Debug, Default)]
struct SubscriberState {
    inner_receivers: Vec<EventStreamReceiver>,
    current_index: usize,
}

#[derive(Debug)]
struct EventStreamReceiver {
    receiver: SliceConsumer,
    event_stream_name: Arc<str>,
    event_names: Box<[Arc<str>]>,
    schema: Arc<RootSchema>,
}

impl EventSubscriber {
    pub(crate) fn new(event_system_directory: PathBuf) -> Result<Self, NewSubscriberError> {
        let event_system_directory = std::fs::canonicalize(event_system_directory)
            .map_err(NewSubscriberError::ResolveEventSystemDirectory)?;
        let event_streams_directory = event_system_directory.join(EVENT_STREAMS_DIRECTORY_NAME);
        let inner_receivers = discover_event_streams(&event_streams_directory)?;

        Ok(Self {
            state: SubscriberState {
                inner_receivers,
                current_index: 0,
            },
        })
    }

    pub fn try_recv(&mut self) -> Option<ReceivedTracingEvent<'_>> {
        self.state.try_recv()
    }
}

impl SubscriberState {
    fn try_recv<'a>(&'a mut self) -> Option<ReceivedTracingEvent<'a>> {
        let receiver_count = self.inner_receivers.len();

        if receiver_count == 0 {
            return None;
        }

        for receiver_index in (self.current_index..receiver_count).chain(0..self.current_index) {
            let (thread_id, payload) = {
                let receiver = &mut self
                    .inner_receivers
                    .get_mut(receiver_index)
                    .expect("index is in bounds due to modulo above")
                    .receiver;
                let Some(read_guard) = receiver.try_reserve_read() else {
                    continue;
                };

                let thread_id = read_guard.lane_metadata().producer_id().get();
                let payload = Box::from(read_guard.as_slice());

                (thread_id, payload)
            };

            let event_index = {
                let event_stream_receiver = &self.inner_receivers[receiver_index];
                let event_index = event_index(&event_stream_receiver.schema, &payload)?;
                event_stream_receiver.event_names.get(event_index)?;
                event_index
            };

            self.current_index = receiver_index.saturating_add(1);
            if self.current_index == receiver_count {
                self.current_index = 0;
            }

            let event_stream_receiver = &self.inner_receivers[receiver_index];
            let event_name = &event_stream_receiver.event_names[event_index];
            return Some(ReceivedTracingEvent {
                metadata: ReceivedMetadata {
                    event_stream_name: event_stream_receiver.event_stream_name.as_ref(),
                    event_name: event_name.as_ref(),
                    thread_id,
                },
                message: EventPayload {
                    schema: &event_stream_receiver.schema,
                    payload,
                },
            });
        }

        None
    }
}

fn discover_event_streams(
    event_streams_directory: &Path,
) -> Result<Vec<EventStreamReceiver>, NewSubscriberError> {
    let mut event_streams = Vec::new();

    for entry in std::fs::read_dir(event_streams_directory)
        .map_err(NewSubscriberError::DiscoverExistingEventStreams)?
    {
        let entry = entry.map_err(NewSubscriberError::DiscoverExistingEventStreams)?;
        let event_stream_directory = entry.path();
        if !event_stream_directory.is_dir() {
            continue;
        }

        let Some(event_stream_name) = event_stream_name(&event_stream_directory) else {
            continue;
        };
        let queue_path = event_stream_directory.join(EVENT_QUEUE_FILE_NAME);
        let schema_path = event_stream_directory.join(EVENT_SCHEMA_FILE_NAME);
        let Ok(encoded_schema) = std::fs::read(schema_path) else {
            continue;
        };
        let Ok(schema) = wincode::deserialize::<RootSchema>(&encoded_schema) else {
            continue;
        };
        let event_names = match &schema {
            RootSchema::Struct(schema) => Box::from([Arc::<str>::from(schema.name())]),
            RootSchema::Enum { variants, .. } => variants
                .iter()
                .map(|variant| Arc::<str>::from(variant.name()))
                .collect(),
        };

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&queue_path)
            .map_err(NewSubscriberError::OpenEventStreamQueue)?;
        queue_file::verify_seals(&file).map_err(NewSubscriberError::ValidateEventStreamQueue)?;
        // SAFETY:
        // - the required immutable seals above ensure the file cannot be
        //   resized while joined.
        // - shaq validates the queue layout before joining.
        // - Event's safety contract requires queue cells whose entire
        //   representation is initialized and meaningful as bytes.
        let receiver = unsafe { SliceConsumer::join(&file) }
            .map_err(NewSubscriberError::JoinEventStreamQueue)?;
        event_streams.push(EventStreamReceiver {
            receiver,
            event_stream_name: Arc::from(event_stream_name),
            event_names,
            schema: Arc::new(schema),
        });
    }

    Ok(event_streams)
}

#[derive(Debug, Error)]
pub enum NewSubscriberError {
    #[error("Failed to resolve the event-system directory")]
    ResolveEventSystemDirectory(#[source] std::io::Error),
    #[error("Failed to discover existing event streams")]
    DiscoverExistingEventStreams(#[source] std::io::Error),
    #[error("Failed to open an event-stream queue")]
    OpenEventStreamQueue(#[source] std::io::Error),
    #[error("Failed to validate an event-stream queue")]
    ValidateEventStreamQueue(#[source] std::io::Error),
    #[error("Failed to join an event-stream queue")]
    JoinEventStreamQueue(#[source] shaq::error::Error),
}

fn event_stream_name(event_stream_directory: &Path) -> Option<&str> {
    event_stream_directory.file_name()?.to_str()
}

fn event_index(schema: &RootSchema, payload: &[u8]) -> Option<usize> {
    if matches!(schema, RootSchema::Struct(_)) {
        return Some(0);
    }

    let RootSchema::Enum {
        variants,
        tag_encoding,
        ..
    } = schema
    else {
        return None;
    };

    let variant_index = match tag_encoding.parse(payload).ok()? {
        Value::U8(value) => usize::from(value),
        Value::U16(value) => usize::from(value),
        Value::U32(value) => usize::try_from(value).ok()?,
        Value::U64(value) => usize::try_from(value).ok()?,
        Value::I8(value) => usize::try_from(value).ok()?,
        Value::I16(value) => usize::try_from(value).ok()?,
        Value::I32(value) => usize::try_from(value).ok()?,
        Value::I64(value) => usize::try_from(value).ok()?,
        Value::F32(_)
        | Value::F64(_)
        | Value::Bool(_)
        | Value::String(_)
        | Value::Bytes(_)
        | Value::Vec(_) => return None,
    };

    variants.get(variant_index).map(|_| variant_index)
}

#[cfg(test)]
mod tests {
    use {
        super::event_index,
        wincode::{SchemaRead, SchemaWrite},
        wincode_dynamic::{RootSchema, SchemaDynamic},
    };

    #[derive(SchemaRead, SchemaWrite, SchemaDynamic)]
    #[wincode(tag_encoding = "u8")]
    enum Events {
        First,
        Second { value: u64 },
    }

    #[test]
    fn variant_index_comes_from_the_encoded_enum_discriminant() {
        let schema = Events::schema();
        assert!(matches!(schema, RootSchema::Enum { .. }));

        let first = wincode::serialize(&Events::First).unwrap();
        assert_eq!(event_index(&schema, &first), Some(0));

        let second = wincode::serialize(&Events::Second { value: 42 }).unwrap();
        assert_eq!(event_index(&schema, &second), Some(1));
    }
}
