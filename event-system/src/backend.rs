#[cfg(target_os = "linux")]
mod linux;
#[cfg_attr(target_os = "linux", allow(dead_code))]
pub(crate) mod stub;

#[cfg(target_os = "linux")]
pub(crate) use linux::*;
#[cfg(not(target_os = "linux"))]
pub(crate) use stub::*;
