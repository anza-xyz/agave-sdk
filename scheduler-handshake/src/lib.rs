#![cfg(unix)]
#![cfg_attr(docsrs, feature(doc_cfg))]

//! Handshake and shared-memory session setup for Agave external schedulers.
//!
//! The client and server negotiate compatible protocol versions over a Unix-domain socket.
//! On success, the server passes the file descriptors for the shared allocator and queues to the
//! client and both sides receive typed session handles.

pub mod client;
pub mod server;
mod shared;
#[cfg(test)]
mod tests;

pub use shared::*;

/// Creates both sides of a local scheduling session without a socket handshake.
///
/// Validates the logon counts and initializes the shared allocator and queues. Both endpoints
/// use this build's interfaces, so no protocol-version negotiation is performed.
///
/// # Panics
///
/// May panic if allocator size or queue capacity calculations overflow.
pub fn setup_local_session(
    logon: ClientLogon,
) -> Result<(AgaveSession, ClientSession), SessionSetupError> {
    let (agave, files) = server::Server::setup_session(logon)?;
    let client = client::setup_session(&logon, files)?;
    Ok((agave, client))
}

/// Returns the major version of this crate and its handshake protocol.
pub fn version() -> u64 {
    env!("CARGO_PKG_VERSION_MAJOR")
        .parse()
        .expect("crate major version must be a u64")
}
