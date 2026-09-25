/// Returns a timestamp in nanoseconds from Linux's `CLOCK_MONOTONIC`.
///
/// Event producers can include this value in their event payloads. Timestamps
/// are comparable across processes sharing the same clock (including its time
/// namespace) during the same boot. They are not Unix timestamps and do not
/// include time spent suspended.
///
/// Returns zero on non-Linux targets, where the event system is a no-op.
///
/// # Panics
///
/// Panics if the clock cannot be read or the timestamp does not fit in a `u64`.
#[inline]
pub fn monotonic_timestamp_ns() -> u64 {
    #[cfg(target_os = "linux")]
    {
        use {
            nix::time::{ClockId, clock_gettime},
            std::time::Duration,
        };

        let timestamp =
            clock_gettime(ClockId::CLOCK_MONOTONIC).expect("CLOCK_MONOTONIC must be available");
        Duration::from(timestamp)
            .as_nanos()
            .try_into()
            .expect("monotonic timestamp must fit in u64 nanoseconds")
    }
    #[cfg(not(target_os = "linux"))]
    {
        0
    }
}
