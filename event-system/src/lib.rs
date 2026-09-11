#![cfg_attr(docsrs, feature(doc_cfg))]
//! An event system implemented on Linux.
//!
//! On all other targets, the public API is available but all operations are
//! no-ops.

// This is needed to use the `#[event]` macro in order to resolve the [`Event`] trait.
// The reason is that the macro expands to use [`agave_event_system::Event`],
// but in the self crate it is referred to as [`crate::Event`].
#[cfg(test)]
extern crate self as agave_event_system;

pub use {
    crate::{
        event_system::{
            CreateEventSystemError, CreateStreamError, EventQueueError, EventSystem, StreamConfig,
        },
        producer_factory::ProducerFactory,
    },
    agave_event_system_derive::event,
    queue_cell::event_queue_cell_size,
};
use {
    wincode::{SchemaWrite, config::DefaultConfig},
    wincode_dynamic::SchemaDynamic,
};

pub mod producer;
#[cfg(not(target_os = "linux"))]
pub mod subscriber;

#[doc(hidden)]
pub mod __private {
    pub use {wincode, wincode_dynamic};

    pub mod event_macro {
        pub use {wincode::*, wincode_dynamic::*};
    }
}

#[cfg_attr(target_os = "linux", path = "backend/linux.rs")]
#[cfg_attr(not(target_os = "linux"), path = "backend/stub.rs")]
mod backend;
mod event_system;
mod producer_factory;
mod queue_cell;

/// An event type that can be sent on an event stream.
///
/// The [`Event`] trait should only be implemented with the [`event`] macro.
///
/// ```
/// # use agave_event_system::event;
/// #[event]
/// #[derive(Debug, PartialEq, Eq)]
/// enum SlotEvents {
///     Completed { slot: u64 },
/// }
/// ```
///
/// Events containing dynamically sized values must declare a
/// `max_serialized_size` strictly greater than the statically known portion of
/// their encoding.
///
/// ```
/// # use agave_event_system::event;
/// #[event(max_serialized_size = 1024)]
/// struct Message {
///     contents: String,
/// }
/// ```
///
/// Omitting the bound for a dynamically sized event is a compile-time error.
///
/// ```compile_fail
/// # use agave_event_system::event;
/// #[event]
/// struct UnboundedMessage {
///     contents: String,
/// }
/// ```
///
/// The bound must also exceed wincode's dynamic serialized-size lower bound.
///
/// ```compile_fail
/// # use agave_event_system::event;
/// #[event(max_serialized_size = 8)]
/// struct UndersizedMessage {
///     fixed: u64,
///     contents: String,
/// }
/// ```
///
/// # Safety
///
/// [`Event::QueueCell`] must be valid for every bit pattern, contain no
/// uninitialized padding, and expose its complete representation through
/// [`AsMut<[u8]>`]. The [`event`] macro satisfies these requirements by using
/// `[u8; N]`.
pub unsafe trait Event:
    Sized + 'static + SchemaDynamic + SchemaWrite<DefaultConfig, Src = Self>
{
    /// The fixed-size storage used for an encoded event in a queue.
    ///
    /// This associated type works around the lack of stable generic const
    /// expressions. The [`event`] macro defines it as a byte array sized from
    /// [`SchemaDynamic::SERIALIZED_SIZE`].
    type QueueCell: Copy + AsMut<[u8]>;
}
