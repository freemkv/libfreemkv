//! Host-certificate collection — the one libfreemkv-side concern left from the
//! old in-tree AACS handshake. The cert mutual-auth itself now lives in the
//! `freemkv-unlock` AACS unlocker; libfreemkv only gathers the certs (a
//! keysource concern) and hands them across the seam.

/// Union the host certificates a scan can offer the drive: the explicit
/// `DriveCredentials`, then each key source's `host_certs(mkb)`. Host certs are
/// keysource-served, never compiled in. `mkb` lets a source pick a
/// generation-appropriate cert (the default impl ignores it).
pub fn collect_host_certs(
    opts: &crate::disc::ScanOptions,
    mkb: Option<u32>,
) -> Vec<crate::aacs::types::HostCert> {
    let mut host_certs: Vec<crate::aacs::types::HostCert> = Vec::new();
    if let Some(c) = &opts.credentials {
        host_certs.extend(c.host_certs.iter().cloned());
    }
    for src in &opts.key_sources {
        host_certs.extend(src.host_certs(mkb));
    }
    host_certs
}
