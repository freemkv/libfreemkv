//! Executable inventory of the BD+ host-service surface.
//!
//! The inventory makes incomplete work visible in the API instead of hiding it
//! in a prose roadmap.  Each entry names the observable service boundary; the
//! session dispatcher is filled in as the argument contract and behavior are
//! established.  Public architecture evidence comes from the [BDSVM patent]
//! and the [player-verification patent].
//!
//! [BDSVM patent]: https://patentimages.storage.googleapis.com/aa/30/8e/7e223065e263a8/US8625967.pdf
//! [player-verification patent]: https://patentimages.storage.googleapis.com/f5/4e/3b/67d458e8a13c92/JP2008198272A.pdf

/// Current implementation state of one externally observable VM service.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ImplementationState {
    /// The known contract is implemented and covered by focused tests.
    Implemented,
    /// The common path works, but at least one known contract detail is open.
    Partial,
    /// The service is identified but deliberately returns `UnsupportedTrap`.
    Stub,
}

/// One host service exposed to Security VM code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostService {
    pub trap_id: u32,
    pub name: &'static str,
    pub state: ImplementationState,
    pub gap: Option<&'static str>,
}

/// The current host-service denominator used to measure core progress.
///
/// `Partial` entries remain gaps.  A service moves to `Implemented` only when
/// its known argument validation, event scope, state changes and result ABI are
/// represented in code and exercised by a meaningful test.
pub const HOST_SERVICES: &[HostService] = &[
    service(
        0x010,
        "finish event",
        ImplementationState::Implemented,
        None,
    ),
    service(
        0x020,
        "submit conversion/fix-up table",
        ImplementationState::Implemented,
        None,
    ),
    service(
        0x110,
        "AES",
        ImplementationState::Partial,
        Some("complete key-ID and mode matrix"),
    ),
    service(
        0x120,
        "private-key signature",
        ImplementationState::Stub,
        Some("message construction, key selectors and 40-byte result ABI"),
    ),
    service(
        0x130,
        "random bytes",
        ImplementationState::Implemented,
        None,
    ),
    service(
        0x140,
        "SHA-1",
        ImplementationState::Stub,
        Some("stream state and operation selectors"),
    ),
    service(
        0x210,
        "add with carry",
        ImplementationState::Implemented,
        None,
    ),
    service(
        0x220,
        "multiply with carry/ripple",
        ImplementationState::Implemented,
        None,
    ),
    service(0x230, "XOR block", ImplementationState::Implemented, None),
    service(0x310, "move memory", ImplementationState::Implemented, None),
    service(
        0x320,
        "search memory",
        ImplementationState::Implemented,
        None,
    ),
    service(0x330, "fill memory", ImplementationState::Implemented, None),
    service(
        0x410,
        "attach code slot",
        ImplementationState::Stub,
        Some("slot state and instruction-filter contract"),
    ),
    service(
        0x420,
        "read code slot",
        ImplementationState::Stub,
        Some("serialized slot record layout"),
    ),
    service(
        0x430,
        "write code slot",
        ImplementationState::Stub,
        Some("serialized slot record layout and mutation rules"),
    ),
    service(
        0x510,
        "application registers",
        ImplementationState::Implemented,
        None,
    ),
    service(
        0x520,
        "device discovery",
        ImplementationState::Partial,
        Some("complete question schemas and dynamic clock fields"),
    ),
    service(
        0x530,
        "DiscoveryRAM",
        ImplementationState::Implemented,
        None,
    ),
    service(
        0x540,
        "load content code",
        ImplementationState::Partial,
        Some("nonzero content offsets and complete filename contract"),
    ),
    service(
        0x550,
        "media check",
        ImplementationState::Stub,
        Some("disc-file reader capability and digest output contract"),
    ),
    service(
        0x560,
        "run native",
        ImplementationState::Stub,
        Some("generation applicability and safe host boundary"),
    ),
    service(
        0x570,
        "reserved or vendor-specific service",
        ImplementationState::Stub,
        Some("identity and contract are not established"),
    ),
    service(
        0x8010,
        "diagnostic log",
        ImplementationState::Stub,
        Some("bounded observer interface"),
    ),
];

const fn service(
    trap_id: u32,
    name: &'static str,
    state: ImplementationState,
    gap: Option<&'static str>,
) -> HostService {
    HostService {
        trap_id,
        name,
        state,
        gap,
    }
}

/// Count the services in each implementation state.
pub fn host_service_counts() -> (usize, usize, usize) {
    HOST_SERVICES.iter().fold((0, 0, 0), |mut counts, service| {
        match service.state {
            ImplementationState::Implemented => counts.0 += 1,
            ImplementationState::Partial => counts.1 += 1,
            ImplementationState::Stub => counts.2 += 1,
        }
        counts
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inventory_has_unique_ids_and_every_incomplete_service_names_its_gap() {
        for (index, service) in HOST_SERVICES.iter().enumerate() {
            assert!(!service.name.is_empty());
            assert_eq!(
                service.state == ImplementationState::Implemented,
                service.gap.is_none()
            );
            assert!(
                !HOST_SERVICES[index + 1..]
                    .iter()
                    .any(|later| later.trap_id == service.trap_id)
            );
        }
    }
}
