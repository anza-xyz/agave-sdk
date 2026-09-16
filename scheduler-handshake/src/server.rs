use {
    crate::{
        AgaveCheckWorkerSession, AgaveHandshakeError, AgaveTpuToPackSession, AgaveWorkerSession,
        ClientLogon, ProtocolVersions,
        shared::{
            AgaveSession, GLOBAL_ALLOCATORS, HUGE_PAGE_SIZE, LOGON_FAILURE, LOGON_SUCCESS,
            PageSize, checked_file_size,
        },
    },
    agave_scheduler_bindings::{
        CheckWorkerToPackMessage, PackToCheckWorkerMessage, PackToExecutionWorkerMessage,
    },
    nix::sys::socket::{self, ControlMessage, MsgFlags, UnixAddr},
    rts_alloc::Allocator,
    std::{
        ffi::CStr,
        fs::File,
        io::{IoSlice, Read, Write},
        os::{
            fd::{AsRawFd, FromRawFd},
            unix::net::{UnixListener, UnixStream},
        },
        path::Path,
        time::{Duration, Instant},
    },
};

type ShaqError = shaq::error::Error;
type RtsAllocError = rts_alloc::error::Error;

const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(1);
const SHMEM_NAME: &CStr = c"/agave-scheduler-bindings";

/// Implements the Agave side of the scheduler bindings handshake protocol.
pub struct Server {
    listener: UnixListener,

    buffer: [u8; 1024],
}

impl Server {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, std::io::Error> {
        let listener = UnixListener::bind(path)?;

        Ok(Self {
            listener,
            buffer: [0; 1024],
        })
    }

    pub fn accept(&mut self) -> Result<AgaveSession, AgaveHandshakeError> {
        // Wait for next stream.
        let (mut stream, _) = self.listener.accept()?;
        stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;

        match self.handle_logon(&mut stream) {
            Ok(session) => Ok(session),
            Err(err) => {
                let reason = err.to_string();
                let reason_len = u8::try_from(reason.len()).unwrap_or(u8::MAX);

                let buffer_len = 2usize.checked_add(usize::from(reason_len)).unwrap();
                self.buffer[0] = LOGON_FAILURE;
                self.buffer[1] = reason_len;
                self.buffer[2..buffer_len]
                    .copy_from_slice(&reason.as_bytes()[..usize::from(reason_len)]);

                stream.set_nonblocking(true)?;
                // NB: Caller will still error out even if our write fails so it's fine to ignore the
                // result.
                let _ = stream.write(&self.buffer[..buffer_len])?;

                Err(err)
            }
        }
    }

    fn handle_logon(
        &mut self,
        stream: &mut UnixStream,
    ) -> Result<AgaveSession, AgaveHandshakeError> {
        // Receive & validate the logon message.
        let logon = self.recv_logon(stream)?;

        // Setup the requested shared memory regions.
        let (session, files) = Self::setup_session(logon)?;

        // Send the file descriptors to the client.
        let fds_raw: Vec<_> = files.iter().map(|file| file.as_raw_fd()).collect();
        let iov = [IoSlice::new(&[LOGON_SUCCESS])];
        let cmsgs = [ControlMessage::ScmRights(&fds_raw)];
        let sent =
            socket::sendmsg::<UnixAddr>(stream.as_raw_fd(), &iov, &cmsgs, MsgFlags::empty(), None)
                .map_err(std::io::Error::from)?;
        debug_assert_eq!(sent, 1);

        Ok(session)
    }

    fn recv_logon(&mut self, stream: &mut UnixStream) -> Result<ClientLogon, AgaveHandshakeError> {
        // Read the logon message.
        let handshake_start = Instant::now();
        let mut buffer_len = 0;
        while buffer_len < self.buffer.len() {
            let read = stream.read(&mut self.buffer[buffer_len..])?;
            if read == 0 {
                return Err(AgaveHandshakeError::EofDuringHandshake);
            }

            // SAFETY: We cannot read a value greater than buffer.len() which itself is a usize.
            buffer_len = buffer_len.checked_add(read).unwrap();

            if handshake_start.elapsed() > HANDSHAKE_TIMEOUT {
                return Err(AgaveHandshakeError::Timeout);
            }
        }

        // Ensure exact version matches for the handshake and all shared-memory interfaces.
        let client_versions =
            ProtocolVersions::try_from_bytes(&self.buffer[..ProtocolVersions::SERIALIZED_SIZE])
                .unwrap();
        let server_versions = ProtocolVersions::current();
        if client_versions != server_versions {
            return Err(AgaveHandshakeError::Version {
                server: server_versions,
                client: client_versions,
            });
        }

        // Read the logon message, cannot panic as we ensure the correct buf size at compile time
        // (hence the const just below).
        const LOGON_END: usize =
            ProtocolVersions::SERIALIZED_SIZE + core::mem::size_of::<ClientLogon>();
        let logon =
            ClientLogon::try_from_bytes(&self.buffer[ProtocolVersions::SERIALIZED_SIZE..LOGON_END])
                .unwrap();

        Ok(logon)
    }

    /// Creates the server endpoints and files in protocol order for one client setup.
    pub(crate) fn setup_session(
        logon: ClientLogon,
    ) -> Result<(AgaveSession, Vec<File>), AgaveHandshakeError> {
        logon.validate()?;

        // Setup the allocator in shared memory (`worker_count`, `check_worker_count`, and
        // `allocator_handles` have been validated so this won't panic).
        let (allocator_file, tpu_to_pack_allocator) = Self::create_allocator(&logon)?;

        // Setup the global queues.
        let (tpu_to_pack_file, tpu_to_pack_queue) =
            Self::create_producer(logon.tpu_to_pack_capacity, PageSize::Huge)?;
        let (progress_tracker_file, progress_tracker) =
            Self::create_producer(logon.progress_tracker_capacity, PageSize::Standard)?;
        let (pack_to_check_worker_file, pack_to_check_worker) =
            Self::create_mpmc_consumer::<PackToCheckWorkerMessage>(
                logon.pack_to_check_worker_capacity,
            )?;
        let (check_worker_to_pack_file, check_worker_to_pack) =
            Self::create_mpmc_producer::<CheckWorkerToPackMessage>(
                logon.check_worker_to_pack_capacity,
                PageSize::Huge,
            )?;

        let check_workers = (0..logon.check_worker_count)
            .map(|_| {
                Ok(AgaveCheckWorkerSession {
                    allocator: Allocator::join_from_existing(&tpu_to_pack_allocator)?,
                    pack_to_check_worker: pack_to_check_worker.clone(),
                    check_worker_to_pack: check_worker_to_pack.clone(),
                })
            })
            .collect::<Result<Vec<_>, AgaveHandshakeError>>()?;

        // Setup the worker sessions.
        let (worker_files, workers) = (0..logon.worker_count).try_fold(
            (Vec::default(), Vec::default()),
            |(mut fds, mut workers), _| {
                let allocator = Allocator::join_from_existing(&tpu_to_pack_allocator)?;

                let (pack_to_worker_file, pack_to_worker) =
                    Self::create_consumer(logon.pack_to_worker_capacity)?;
                let (worker_to_pack_file, worker_to_pack) =
                    Self::create_producer(logon.worker_to_pack_capacity, PageSize::Huge)?;

                fds.extend([pack_to_worker_file, worker_to_pack_file]);
                workers.push(AgaveWorkerSession {
                    allocator,
                    pack_to_worker,
                    worker_to_pack,
                });

                Ok::<_, AgaveHandshakeError>((fds, workers))
            },
        )?;

        Ok((
            AgaveSession {
                flags: logon.flags,
                tpu_to_pack: AgaveTpuToPackSession {
                    allocator: tpu_to_pack_allocator,
                    producer: tpu_to_pack_queue,
                },
                progress_tracker,
                check_workers,
                workers,
            },
            [
                allocator_file,
                tpu_to_pack_file,
                progress_tracker_file,
                pack_to_check_worker_file,
                check_worker_to_pack_file,
            ]
            .into_iter()
            .chain(worker_files)
            .collect(),
        ))
    }

    fn create_allocator(logon: &ClientLogon) -> Result<(File, Allocator), RtsAllocError> {
        let allocator_count = GLOBAL_ALLOCATORS
            .checked_add(logon.worker_count)
            .unwrap()
            .checked_add(logon.check_worker_count)
            .unwrap()
            .checked_add(logon.allocator_handles)
            .unwrap();

        let create = |page_size: PageSize| {
            let allocator_file = Self::create_shmem(page_size)?;
            let allocator_file_size = Self::align_file_size(logon.allocator_size, page_size)?;

            // SAFETY: We just created this file and thus can uniquely initialize it.
            unsafe {
                Allocator::create(
                    &allocator_file,
                    allocator_file_size,
                    u32::try_from(allocator_count).unwrap(),
                    // Allocator slab size: chosen as 2 MiB, but need not match the OS page size.
                    HUGE_PAGE_SIZE as u32,
                )
            }
            .map(|allocator| (allocator_file, allocator))
        };

        // Try to create with huge pages, fallback to regular pages.
        create(PageSize::Huge).or_else(|_| create(PageSize::Standard))
    }

    fn create_producer<T>(
        capacity: usize,
        page_size: PageSize,
    ) -> Result<(File, shaq::spsc::Producer<T>), ShaqError> {
        let create = |page_size: PageSize| {
            let file = Self::create_shmem(page_size)?;
            let minimum_file_size = shaq::spsc::try_minimum_file_size::<T>(capacity)?;
            let file_size = Self::align_file_size(minimum_file_size, page_size)?;

            // SAFETY: uniqely creating as producer
            unsafe { shaq::spsc::Producer::create(&file, file_size) }
                .map(|producer| (file, producer))
        };

        // Try to create with huge pages, fallback to regular pages.
        match page_size {
            PageSize::Huge => create(PageSize::Huge).or_else(|_| create(PageSize::Standard)),
            PageSize::Standard => create(PageSize::Standard),
        }
    }

    fn create_consumer(
        capacity: usize,
    ) -> Result<(File, shaq::spsc::Consumer<PackToExecutionWorkerMessage>), ShaqError> {
        let create = |page_size: PageSize| {
            let file = Self::create_shmem(page_size)?;
            let minimum_file_size =
                shaq::spsc::try_minimum_file_size::<PackToExecutionWorkerMessage>(capacity)?;
            let file_size = Self::align_file_size(minimum_file_size, page_size)?;

            // SAFETY: uniquely creating as consumer.
            unsafe { shaq::spsc::Consumer::create(&file, file_size) }
                .map(|producer| (file, producer))
        };

        // Try to create with huge pages, fallback to regular pages.
        create(PageSize::Huge).or_else(|_| create(PageSize::Standard))
    }

    fn create_mpmc_producer<T>(
        capacity: usize,
        page_size: PageSize,
    ) -> Result<(File, shaq::mpmc::Producer<T>), ShaqError> {
        let create = |page_size: PageSize| {
            let file = Self::create_shmem(page_size)?;
            let minimum_file_size = shaq::mpmc::try_minimum_file_size::<T>(capacity)?;
            let file_size = Self::align_file_size(minimum_file_size, page_size)?;

            // SAFETY: uniquely creating as producer.
            unsafe { shaq::mpmc::Producer::create(&file, file_size) }
                .map(|producer| (file, producer))
        };

        match page_size {
            PageSize::Huge => create(PageSize::Huge).or_else(|_| create(PageSize::Standard)),
            PageSize::Standard => create(PageSize::Standard),
        }
    }

    fn create_mpmc_consumer<T>(
        capacity: usize,
    ) -> Result<(File, shaq::mpmc::Consumer<T>), ShaqError> {
        let create = |page_size: PageSize| {
            let file = Self::create_shmem(page_size)?;
            let minimum_file_size = shaq::mpmc::try_minimum_file_size::<T>(capacity)?;
            let file_size = Self::align_file_size(minimum_file_size, page_size)?;

            // SAFETY: uniquely creating as consumer.
            unsafe { shaq::mpmc::Consumer::create(&file, file_size) }
                .map(|consumer| (file, consumer))
        };

        create(PageSize::Huge).or_else(|_| create(PageSize::Standard))
    }

    #[cfg(any(
        target_os = "linux",
        target_os = "l4re",
        target_os = "android",
        target_os = "emscripten"
    ))]
    fn create_shmem(page_size: PageSize) -> Result<File, std::io::Error> {
        let flags = match page_size {
            PageSize::Huge => libc::MFD_HUGETLB | libc::MFD_HUGE_2MB,
            PageSize::Standard => 0,
        };

        unsafe {
            let ret = libc::memfd_create(SHMEM_NAME.as_ptr(), flags);
            if ret == -1 {
                return Err(std::io::Error::last_os_error());
            }

            Ok(File::from_raw_fd(ret))
        }
    }

    #[cfg(not(any(
        target_os = "linux",
        target_os = "l4re",
        target_os = "android",
        target_os = "emscripten"
    )))]
    fn create_shmem(page_size: PageSize) -> Result<File, std::io::Error> {
        if matches!(page_size, PageSize::Huge) {
            return Err(std::io::ErrorKind::Unsupported.into());
        }

        unsafe {
            // Clean up the previous link if one exists.
            let ret = libc::shm_unlink(SHMEM_NAME.as_ptr());
            if ret == -1 {
                let err = std::io::Error::last_os_error();
                if err.kind() != std::io::ErrorKind::NotFound {
                    return Err(err);
                }
            }

            // Create a new shared memory object.
            let ret = libc::shm_open(
                SHMEM_NAME.as_ptr(),
                libc::O_CREAT | libc::O_EXCL | libc::O_RDWR,
                #[cfg(not(target_os = "macos"))]
                {
                    libc::S_IRUSR | libc::S_IWUSR
                },
                #[cfg(any(target_os = "macos", target_os = "ios"))]
                {
                    (libc::S_IRUSR | libc::S_IWUSR) as libc::c_uint
                },
            );
            if ret == -1 {
                return Err(std::io::Error::last_os_error());
            }
            let file = File::from_raw_fd(ret);

            // Clean up after ourself.
            let ret = libc::shm_unlink(SHMEM_NAME.as_ptr());
            if ret == -1 {
                return Err(std::io::Error::last_os_error());
            }

            Ok(file)
        }
    }

    fn align_file_size(size: usize, page_size: PageSize) -> Result<usize, std::io::Error> {
        checked_file_size(size, page_size).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "file size cannot be represented",
            )
        })
    }
}
