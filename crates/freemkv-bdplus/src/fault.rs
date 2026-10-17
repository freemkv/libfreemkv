//! Structured BD+ engine faults. Rendering belongs to the application.

/// Result of a BD+ engine operation.
pub type BdPlusResult<T> = std::result::Result<T, BdPlusError>;

/// Engine fault details contain positions and identifiers, never player secrets.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BdPlusError {
    /// Malformed SVM framing at a source file offset.
    SvmFormat { offset: usize },
    /// The bootstrap control area is absent or the body exceeds VM memory.
    BootstrapLength { length: usize },
    /// A host tried to execute before satisfying a pending trap.
    PendingTrap { trap_id: u32 },
    /// There is no pending trap to satisfy.
    NoPendingTrap,
    /// A player reply does not belong to the request currently awaiting input.
    PlayerRequestMismatch { expected: u64, supplied: u64 },
    /// A player reply has the wrong semantic shape for the pending operation.
    PlayerReplyType,
    /// The pending player request already has a reply awaiting consumption.
    PlayerReplyAlreadySupplied { request_id: u64 },
    /// The authored title sequence completed without submitting a FUT.
    PreparationEndedWithoutTable,
    /// The session finished a different event than preparation started.
    PreparationEventMismatch,
    /// Another event is still executing.
    EventInProgress,
    /// No event is executing.
    NoActiveEvent,
    /// A host requested a register outside the architectural register file.
    RegisterIndex { index: usize },
    /// A modeled player RAM range overlaps an installed region.
    PlayerMemoryOverlap { address: u32 },
    /// A persistent player-slot image has the wrong serialized length.
    SlotImageLength { length: usize },
    /// A trap is outside the currently implemented host ABI.
    UnsupportedTrap { trap_id: u32, pc: u32 },
    /// Further submissions require verified merge semantics before publication.
    MultipleTableSubmissions,
    /// The operation was cancelled through its Halt token.
    Cancelled,
    /// A host-specified range exceeds the VM address space.
    MemoryRange { address: u32, length: usize },
    /// Malformed table data at an absolute byte position in the submitted table.
    FixupFormat { offset: usize },
    /// The selected clip has no submitted table.
    MissingTable { clip_id: u32 },
    /// A segment must be resolved before publishing repairs.
    UnresolvedSegment { clip_id: u32, segment: usize },
    /// A submitted table repeats a clip identifier.
    DuplicateTable { clip_id: u32 },
    /// A segment index does not exist.
    MissingSegment { clip_id: u32, segment: usize },
    /// A segment has already been resolved; the encoded original is immutable.
    SegmentAlreadyResolved { clip_id: u32, segment: usize },
    /// A decoded descriptor has a reserved entry type.
    DescriptorType {
        clip_id: u32,
        segment: usize,
        entry: usize,
        kind: u8,
    },
    /// Offset arithmetic cannot be represented.
    OffsetOverflow,
    /// An active descriptor addresses packet headers or reverses its repairs.
    DescriptorAddress {
        clip_id: u32,
        segment: usize,
        entry: usize,
    },
    /// Active replacements overlap and no verified precedence is available.
    OverlappingPatches { offset: u64 },
    /// An input exceeds an explicit engine resource limit.
    ResourceLimit { requested: usize, limit: usize },
    /// Content code requested a section load but no content reader is attached.
    NoContentReader,
    /// A requested content section is not available in the attached reader.
    MissingContent { name: u32, section: u32 },
    /// A trap was issued in an event where its ABI does not allow it.
    TrapNotPermittedInEvent { trap_id: u32 },
}
