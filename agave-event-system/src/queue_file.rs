#![cfg(target_os = "linux")]

use std::{
    fs::File,
    io,
    os::fd::{AsRawFd, FromRawFd},
    path::Path,
};

//  seals needed for shaq's safety requirement to not resize file
const REQUIRED_SEALS: libc::c_int = libc::F_SEAL_SHRINK | libc::F_SEAL_GROW | libc::F_SEAL_SEAL;

pub(crate) fn create_anonymous_file() -> io::Result<File> {
    // SAFETY: the name is a valid, static C string and the flags are supported
    // by memfd_create. Ownership of a successful descriptor is transferred to
    // `File` exactly once below.
    let file_descriptor = unsafe {
        libc::memfd_create(
            c"agave-event-stream".as_ptr(),
            libc::MFD_CLOEXEC | libc::MFD_ALLOW_SEALING,
        )
    };
    if file_descriptor == -1 {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: memfd_create returned a new, owned file descriptor.
    Ok(unsafe { File::from_raw_fd(file_descriptor) })
}

pub(crate) fn seal(file: &File) -> io::Result<()> {
    // SAFETY: `file` owns a valid descriptor and F_ADD_SEALS accepts the seal
    // bitmask as its third argument.
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, REQUIRED_SEALS) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }

    verify_seals(file)
}

pub(crate) fn verify_seals(file: &File) -> io::Result<()> {
    // SAFETY: `file` owns a valid descriptor and F_GET_SEALS takes no third
    // argument.
    let seals = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GET_SEALS) };
    if seals == -1 {
        return Err(io::Error::last_os_error());
    }
    if seals & REQUIRED_SEALS != REQUIRED_SEALS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "event-stream queue is not sealed against resizing",
        ));
    }

    Ok(())
}

pub(crate) fn publish_anonymous_file(file: &File, queue_path: &Path) -> io::Result<()> {
    let proc_fd_path = format!("/proc/{}/fd/{}", std::process::id(), file.as_raw_fd());
    std::os::unix::fs::symlink(proc_fd_path, queue_path)
}
