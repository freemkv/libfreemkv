//! Local BD+ event execution and player host services.
//!
//! No network calls occur here. Missing material leaves the trap pending;
//! after the application provisions it, run retries the same host operation.
//! Device discovery, private-key operations, DiscoveryRAM and Security VM / application
//! register exchange are described together in a [player-verification patent].
//! A separate [secure-execution patent] explicitly illustrates the BD+ AES
//! split: ordinary ECB work surrounds a protected player-key unwrap.
//!
//! [player-verification patent]: ../docs/jp2008198272_bdplus_discoveryram_privatekey.pdf "upstream: https://patentimages.storage.googleapis.com/f5/4e/3b/67d458e8a13c92/JP2008198272A.pdf"
//! [secure-execution patent]: ../docs/us20100241855_bdplus_secure_execution_fixup.pdf "upstream: https://patentimages.storage.googleapis.com/5e/f1/fc/fb0a718c511e94/US20100241855A1.pdf"

use super::{
    BdPlusContent, BdPlusDiscContext, BdPlusError, BdPlusEvent, BdPlusFixupTable, BdPlusPlayer,
    BdPlusResult, BdPlusVm, PlayerReply, Svm, VmBoundary,
};
use crate::Halt;
use crate::slot::{BdPlusSlotStorage, SLOT_COUNT, SLOT_SIZE, SlotSession};
use sha1::{Digest, Sha1};
use std::collections::HashMap;

const OK: u32 = 0;
const INVALID: u32 = 0x8000_0001;
const TOO_LARGE: u32 = 0x8000_0002;

/// Missing semantic capability; IDs/ranges contain no secret values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlayerOperation {
    AesKey {
        key_id: u32,
        wrapped_key: [u8; 16],
    },
    ApplicationRead {
        register: u32,
    },
    ApplicationWrite {
        register: u32,
        value: u32,
    },
    Discovery {
        device: u32,
        question: u32,
        capacity: usize,
    },
    DiagnosticLog {
        message: Vec<u8>,
    },
    Memory {
        address: u32,
        length: usize,
    },
    MediaRead {
        path: Vec<u8>,
        offset: u64,
        length: usize,
    },
    MediaKey,
    PrivateKeySign {
        key_id: u32,
        control_word: u32,
        message: Vec<u8>,
    },
    Random {
        length: usize,
    },
    SlotStorage,
    VolumeId,
}

/// A stable request handle for material acquired outside VM execution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlayerRequest {
    id: u64,
    operation: PlayerOperation,
}

impl PlayerRequest {
    pub fn id(&self) -> u64 {
        self.id
    }

    pub fn operation(&self) -> PlayerOperation {
        self.operation.clone()
    }
}

/// Owned semantic result supplied by an application-side provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlayerMaterial {
    ApplicationAcknowledged,
    ApplicationValue(u32),
    DiagnosticAcknowledged,
    UnwrappedAesKey([u8; 16]),
    Discovery(Vec<u8>),
    Memory(Vec<u8>),
    Media(Vec<u8>),
    MediaKey([u8; 16]),
    Signature([u8; 40]),
    Random(Vec<u8>),
    SlotStorage(BdPlusSlotStorage),
    VolumeId([u8; 16]),
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionBoundary {
    FinishedEvent(BdPlusEvent),
    NeedPlayerMaterial(PlayerRequest),
    Watchdog,
    Yield,
}

/// One disc operation's VM and player context. Retain across events.
pub struct BdPlusSession<P> {
    vm: BdPlusVm,
    player: P,
    content: Option<Box<dyn BdPlusContent + Send + Sync>>,
    disc: Option<BdPlusDiscContext>,
    pending: Option<(u32, u32)>,
    player_request: Option<PlayerRequest>,
    player_reply: Option<PlayerMaterial>,
    next_request_id: u64,
    table: Option<BdPlusFixupTable>,
    sha1: HashMap<u32, Sha1>,
    slots: Option<SlotSession>,
}

impl<P> std::fmt::Debug for BdPlusSession<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BdPlusSession")
            .field("vm", &self.vm)
            .field("table", &self.table)
            .finish_non_exhaustive()
    }
}

impl<P: BdPlusPlayer> BdPlusSession<P> {
    pub fn new(svm: &Svm, player: P) -> BdPlusResult<Self> {
        Ok(Self {
            vm: BdPlusVm::load(svm)?,
            player,
            content: None,
            disc: None,
            pending: None,
            player_request: None,
            player_reply: None,
            next_request_id: 1,
            table: None,
            sha1: HashMap::new(),
            slots: None,
        })
    }

    /// Attach the disc context (VID + MK + timestamp) in one call.
    pub fn with_disc_context(mut self, disc: BdPlusDiscContext) -> Self {
        self.disc = Some(disc);
        self
    }

    /// Attach a content library in one call.
    pub fn with_content<C: BdPlusContent + Send + Sync + 'static>(mut self, content: C) -> Self {
        self.content = Some(Box::new(content));
        self
    }
    /// Attach a numbered-SVM reader so content code can request further sections.
    pub fn set_content<C: BdPlusContent + Send + Sync + 'static>(&mut self, content: C) {
        self.content = Some(Box::new(content));
    }
    /// Supply the live disc context (VID, timestamp) that synthesized discovery
    /// and later disc-coupled traps read from.
    pub fn set_disc_context(&mut self, disc: BdPlusDiscContext) {
        self.disc = Some(disc);
    }
    pub fn vm(&self) -> &BdPlusVm {
        &self.vm
    }
    pub fn player_mut(&mut self) -> &mut P {
        &mut self.player
    }
    pub fn fixup_table(&self) -> Option<&BdPlusFixupTable> {
        self.table.as_ref()
    }
    /// Replace the provider-owned persistent player-slot image.
    pub fn set_slot_storage(&mut self, storage: BdPlusSlotStorage) {
        self.slots = Some(SlotSession::new(storage));
    }
    /// Inspect the current image; persist it after the session if desired.
    pub fn slot_storage(&self) -> Option<&BdPlusSlotStorage> {
        self.slots.as_ref().map(|slots| &slots.storage)
    }
    pub fn begin_event(&mut self, event: BdPlusEvent) -> BdPlusResult<()> {
        self.vm.begin_event(event)
    }

    /// Bind an owned provider reply to the exact request that produced it.
    pub fn provide_player_material(
        &mut self,
        request_id: u64,
        material: PlayerMaterial,
    ) -> BdPlusResult<()> {
        let request = self
            .player_request
            .as_ref()
            .ok_or(BdPlusError::NoPendingTrap)?;
        if request.id != request_id {
            return Err(BdPlusError::PlayerRequestMismatch {
                expected: request.id,
                supplied: request_id,
            });
        }
        if self.player_reply.is_some() {
            return Err(BdPlusError::PlayerReplyAlreadySupplied { request_id });
        }
        let compatible = matches!(material, PlayerMaterial::Unsupported)
            || matches!(
                (&request.operation, &material),
                (
                    PlayerOperation::AesKey { .. },
                    PlayerMaterial::UnwrappedAesKey(_)
                ) | (
                    PlayerOperation::ApplicationRead { .. },
                    PlayerMaterial::ApplicationValue(_)
                ) | (
                    PlayerOperation::ApplicationWrite { .. },
                    PlayerMaterial::ApplicationAcknowledged
                ) | (
                    PlayerOperation::Discovery { .. },
                    PlayerMaterial::Discovery(_)
                ) | (
                    PlayerOperation::DiagnosticLog { .. },
                    PlayerMaterial::DiagnosticAcknowledged
                ) | (PlayerOperation::Memory { .. }, PlayerMaterial::Memory(_))
                    | (PlayerOperation::MediaRead { .. }, PlayerMaterial::Media(_))
                    | (PlayerOperation::MediaKey, PlayerMaterial::MediaKey(_))
                    | (
                        PlayerOperation::PrivateKeySign { .. },
                        PlayerMaterial::Signature(_)
                    )
                    | (PlayerOperation::Random { .. }, PlayerMaterial::Random(_))
                    | (PlayerOperation::SlotStorage, PlayerMaterial::SlotStorage(_))
                    | (PlayerOperation::VolumeId, PlayerMaterial::VolumeId(_))
            );
        if !compatible {
            return Err(BdPlusError::PlayerReplyType);
        }
        self.player_reply = Some(material);
        Ok(())
    }

    /// Run a bounded slice, preserving the current instruction/trap on a miss.
    pub fn run(&mut self, budget: u64, halt: &Halt) -> BdPlusResult<SessionBoundary> {
        let end = self.vm.instructions().saturating_add(budget);
        loop {
            if halt.is_cancelled() {
                return Err(BdPlusError::Cancelled);
            }
            let (trap, pc) = if let Some(pending) = self.pending {
                pending
            } else {
                match self
                    .vm
                    .run(end.saturating_sub(self.vm.instructions()), halt)?
                {
                    VmBoundary::Trap { trap_id, pc } => {
                        self.pending = Some((trap_id, pc));
                        (trap_id, pc)
                    }
                    VmBoundary::Watchdog => return Ok(SessionBoundary::Watchdog),
                    VmBoundary::Yield => return Ok(SessionBoundary::Yield),
                }
            };
            if trap == 0x10 {
                let event = self.vm.finish_event()?;
                self.pending = None;
                match event {
                    Some(BdPlusEvent::ComputeSp { clip_id, segment }) => {
                        let key: [u8; 16] = self.vm.memory().read(0x10, 16)?.try_into().unwrap();
                        let mask: [u8; 8] = self.vm.memory().read(0x20, 8)?.try_into().unwrap();
                        let table = self
                            .table
                            .as_mut()
                            .ok_or(BdPlusError::MissingTable { clip_id })?;
                        table.resolve_segment(clip_id, segment as usize, &key, &mask)?;
                        return Ok(SessionBoundary::FinishedEvent(BdPlusEvent::ComputeSp {
                            clip_id,
                            segment,
                        }));
                    }
                    Some(event) => return Ok(SessionBoundary::FinishedEvent(event)),
                    None => {
                        // Idle-loop Finished: content code's dispatcher ran
                        // through without a live event. Continue looping; the
                        // caller resumes execution or injects a new event.
                        continue;
                    }
                }
            }
            match self.service(trap, pc)? {
                Service::Status(status) => {
                    self.vm.resume_trap(Some(status))?;
                    self.pending = None;
                    self.player_request = None;
                    self.player_reply = None;
                }
                Service::Need(operation) => {
                    let request = match self.player_request.as_ref() {
                        Some(request) => {
                            if request.operation != operation {
                                return Err(BdPlusError::PlayerReplyType);
                            }
                            request.clone()
                        }
                        None => {
                            let request = PlayerRequest {
                                id: self.next_request_id,
                                operation,
                            };
                            self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
                            self.player_request = Some(request.clone());
                            request
                        }
                    };
                    return Ok(SessionBoundary::NeedPlayerMaterial(request));
                }
            }
        }
    }

    fn service(&mut self, trap: u32, pc: u32) -> BdPlusResult<Service> {
        let args: [u32; 6] = std::array::from_fn(|i| self.vm.argument(i as u8));
        match trap {
            0x20 => {
                if args[0] != 0 {
                    let bytes = self.vm.memory().read(args[1], args[0] as usize)?;
                    let table = BdPlusFixupTable::parse(bytes)?;
                    if self.table.is_some() {
                        return Err(BdPlusError::MultipleTableSubmissions);
                    }
                    self.table = Some(table);
                }
                Ok(Service::Status(OK))
            }
            0x110 => self.aes([args[0], args[1], args[2], args[3], args[4]]),
            0x120 => self.private_key([args[0], args[1], args[2], args[3], args[4]]),
            0x130 => {
                let length = args[1] as usize;
                self.vm.memory().read(args[0], length)?;
                let bytes = match self.player_reply.as_ref() {
                    Some(PlayerMaterial::Random(bytes)) if bytes.len() == length => bytes,
                    Some(PlayerMaterial::Random(_)) | Some(PlayerMaterial::Unsupported) => {
                        return Ok(Service::Status(INVALID));
                    }
                    _ => return Ok(Service::Need(PlayerOperation::Random { length })),
                };
                self.vm.memory_mut().write(args[0], bytes)?;
                Ok(Service::Status(OK))
            }
            0x140 => self.sha1([args[0], args[1], args[2], args[3], args[4]]),
            0x210 => {
                let length = match (args[2] as usize).checked_mul(4) {
                    Some(length) => length,
                    None => return Ok(Service::Status(INVALID)),
                };
                if args[0] & 3 != 0 || args[1] & 3 != 0 {
                    return Ok(Service::Status(INVALID));
                }
                let destination_range = checked_range(args[0], length)?;
                let source_range = checked_range(args[1], length)?;
                if args[0] != args[1] && ranges_overlap(&destination_range, &source_range) {
                    return Ok(Service::Status(INVALID));
                }
                let source = self.vm.memory().read(args[1], length)?.to_vec();
                let destination = self.vm.memory_mut().read_mut(args[0], length)?;
                let mut carry = 0_u64;
                for index in (0..length).step_by(4).rev() {
                    let left = u32::from_be_bytes([
                        destination[index],
                        destination[index + 1],
                        destination[index + 2],
                        destination[index + 3],
                    ]);
                    let right = u32::from_be_bytes([
                        source[index],
                        source[index + 1],
                        source[index + 2],
                        source[index + 3],
                    ]);
                    let sum = u64::from(left) + u64::from(right) + carry;
                    destination[index..index + 4].copy_from_slice(&(sum as u32).to_be_bytes());
                    carry = sum >> 32;
                }
                Ok(Service::Status(carry as u32))
            }
            0x220 => {
                let source_length = match (args[2] as usize).checked_mul(4) {
                    Some(length) => length,
                    None => return Ok(Service::Status(INVALID)),
                };
                let destination_length = match source_length.checked_add(4) {
                    Some(length) => length,
                    None => return Ok(Service::Status(INVALID)),
                };
                if args[0] & 3 != 0 || args[1] & 3 != 0 {
                    return Ok(Service::Status(INVALID));
                }
                let destination_range = checked_range(args[0], destination_length)?;
                let source_range = checked_range(args[1], source_length)?;
                if ranges_overlap(&destination_range, &source_range) {
                    return Ok(Service::Status(INVALID));
                }
                let source = self.vm.memory().read(args[1], source_length)?.to_vec();
                let destination = self.vm.memory_mut().read_mut(args[0], destination_length)?;
                let mut carry = 0_u64;
                for source_index in (0..source_length).step_by(4).rev() {
                    let value = u32::from_be_bytes([
                        source[source_index],
                        source[source_index + 1],
                        source[source_index + 2],
                        source[source_index + 3],
                    ]);
                    let product = u64::from(value) * u64::from(args[3]) + carry;
                    destination[source_index + 4..source_index + 8]
                        .copy_from_slice(&(product as u32).to_be_bytes());
                    carry = product >> 32;
                }
                destination[..4].copy_from_slice(&(carry as u32).to_be_bytes());
                Ok(Service::Status(OK))
            }
            0x230 => {
                let length = (args[2] as usize)
                    .checked_mul(4)
                    .ok_or(BdPlusError::OffsetOverflow)?;
                let source = self.vm.memory().read(args[1], length)?.to_vec();
                let destination = self.vm.memory_mut().read_mut(args[0], length)?;
                for (dst, src) in destination.iter_mut().zip(source) {
                    *dst ^= src;
                }
                Ok(Service::Status(OK))
            }
            0x310 => {
                let bytes = self.vm.memory().read(args[1], args[2] as usize)?.to_vec();
                self.vm.memory_mut().write(args[0], &bytes)?;
                Ok(Service::Status(OK))
            }
            0x320 => {
                if args[4] & 3 != 0 {
                    return Ok(Service::Status(INVALID));
                }
                let region = self.vm.memory().read(args[0], args[1] as usize)?;
                let needle = self.vm.memory().read(args[2], args[3] as usize)?;
                self.vm.memory().read(args[4], 4)?;
                let found = if needle.is_empty() || region.len() < needle.len() {
                    0
                } else {
                    region
                        .windows(needle.len())
                        .position(|candidate| candidate == needle)
                        .map_or(0, |offset| args[0] + offset as u32)
                };
                self.vm.memory_mut().store_u32(args[4], found);
                Ok(Service::Status(OK))
            }
            0x330 => {
                self.vm
                    .memory_mut()
                    .read_mut(args[0], args[2] as usize)?
                    .fill(args[1] as u8);
                Ok(Service::Status(OK))
            }
            0x410 => self.slot_attach(args[0], args[1], pc),
            0x420 => self.slot_read(args[0], args[1]),
            0x430 => self.slot_write(args[0]),
            0x510 => {
                if !matches!(
                    self.vm.event(),
                    Some(BdPlusEvent::ApplicationLayer) | Some(BdPlusEvent::ComputeSp { .. })
                ) {
                    return Err(BdPlusError::TrapNotPermittedInEvent { trap_id: trap });
                }
                let (register, operation, address) = (args[0], args[1], args[2]);
                let valid = match register {
                    0 => operation == 1,
                    1 => operation == 0,
                    2 => operation <= 1,
                    _ => false,
                };
                if !valid || address & 3 != 0 {
                    return Ok(Service::Status(INVALID));
                }
                self.vm.memory().read(address, 4)?;
                if operation == 0 {
                    let value = match self.player_reply.as_ref() {
                        Some(PlayerMaterial::ApplicationValue(value)) => *value,
                        Some(PlayerMaterial::Unsupported) => return Ok(Service::Status(INVALID)),
                        _ => {
                            return Ok(Service::Need(PlayerOperation::ApplicationRead {
                                register,
                            }));
                        }
                    };
                    self.vm.memory_mut().store_u32(address, value);
                } else {
                    let value = self.vm.memory().load_u32(address);
                    match self.player_reply.as_ref() {
                        Some(PlayerMaterial::ApplicationAcknowledged) => {}
                        Some(PlayerMaterial::Unsupported) => return Ok(Service::Status(INVALID)),
                        _ => {
                            return Ok(Service::Need(PlayerOperation::ApplicationWrite {
                                register,
                                value,
                            }));
                        }
                    }
                }
                Ok(Service::Status(OK))
            }
            0x540 => {
                match self.vm.event() {
                    Some(BdPlusEvent::Start)
                    | Some(BdPlusEvent::Shutdown)
                    | Some(BdPlusEvent::TitleInit { .. }) => {}
                    _ => return Err(BdPlusError::TrapNotPermittedInEvent { trap_id: trap }),
                }
                let (name, section, len_ptr, destination) = (args[0], args[1], args[3], args[4]);
                let capacity =
                    u32::from_be_bytes(self.vm.memory().read(len_ptr, 4)?.try_into().unwrap())
                        as usize;
                let content = self.content.as_ref().ok_or(BdPlusError::NoContentReader)?;
                let body = content.section_body(name, section)?;
                let Some(body) = body.get(args[2] as usize..) else {
                    return Ok(Service::Status(INVALID));
                };
                let actual = body.len().min(capacity);
                self.vm.memory().read(destination, actual)?;
                self.vm.memory_mut().write(destination, &body[..actual])?;
                self.vm
                    .memory_mut()
                    .write(len_ptr, &(actual as u32).to_be_bytes())?;
                Ok(Service::Status(OK))
            }
            0x520 => {
                let (device, question) = (args[0], args[1]);
                let capacity = self.vm.memory().read(args[3], 4)?;
                let capacity = u32::from_be_bytes(capacity.try_into().unwrap()) as usize;
                // Discovery(1,3) is synthesized from the disc context, not
                // stored in the profile: year/month/day/hour/min/sec/centi at
                // offset 0, 16-byte VID at offset 24, static tail trailing.
                let synthesized_1_3 = if device == 1 && question == 3 {
                    if let Some(PlayerMaterial::VolumeId(volume_id)) = self.player_reply.take() {
                        self.disc.get_or_insert_with(Default::default).volume_id = Some(volume_id);
                        self.player_request = None;
                    }
                    let volume_id = match self.player_reply.as_ref() {
                        Some(PlayerMaterial::Unsupported) => {
                            return Ok(Service::Status(INVALID));
                        }
                        _ => self.disc.as_ref().and_then(|disc| disc.volume_id),
                    };
                    let Some(volume_id) = volume_id else {
                        return Ok(Service::Need(PlayerOperation::VolumeId));
                    };
                    self.disc.as_ref().and_then(|disc| {
                        let mut context = disc.clone();
                        context.volume_id = Some(volume_id);
                        context.discovery_record()
                    })
                } else {
                    None
                };
                let bytes: &[u8] = if let Some(ref record) = synthesized_1_3 {
                    record
                } else if matches!(
                    self.player_reply,
                    Some(PlayerMaterial::Discovery(_)) | Some(PlayerMaterial::Unsupported)
                ) {
                    match self.player_reply.as_ref() {
                        Some(PlayerMaterial::Discovery(bytes)) => bytes.as_slice(),
                        Some(PlayerMaterial::Unsupported) => return Ok(Service::Status(INVALID)),
                        _ => return Err(BdPlusError::PlayerReplyType),
                    }
                } else {
                    match self.player.discovery(device, question) {
                        PlayerReply::Ready(bytes) => bytes,
                        PlayerReply::Unsupported => return Ok(Service::Status(INVALID)),
                        PlayerReply::Missing => {
                            return Ok(Service::Need(PlayerOperation::Discovery {
                                device,
                                question,
                                capacity,
                            }));
                        }
                    }
                };
                if bytes.len() > capacity {
                    return Ok(Service::Status(INVALID));
                }
                self.vm.memory().read(args[2], bytes.len())?;
                self.vm.memory_mut().write(args[2], bytes)?;
                self.vm
                    .memory_mut()
                    .write(args[3], &(bytes.len() as u32).to_be_bytes())?;
                Ok(Service::Status(OK))
            }
            0x530 => {
                let (address, destination, length) = (args[0], args[1], args[2] as usize);
                if length > 0x20_0000 {
                    return Ok(Service::Status(TOO_LARGE));
                }
                self.vm.memory().read(destination, length)?;
                if address.checked_add(args[2]).is_none() {
                    return Ok(Service::Status(INVALID));
                }
                if length == 0 {
                    return Ok(Service::Status(OK));
                }
                let bytes: &[u8] = if matches!(
                    self.player_reply,
                    Some(PlayerMaterial::Memory(_)) | Some(PlayerMaterial::Unsupported)
                ) {
                    match self.player_reply.as_ref() {
                        Some(PlayerMaterial::Memory(bytes)) if bytes.len() == length => bytes,
                        Some(PlayerMaterial::Memory(_)) | Some(PlayerMaterial::Unsupported) => {
                            return Ok(Service::Status(INVALID));
                        }
                        _ => return Err(BdPlusError::PlayerReplyType),
                    }
                } else {
                    match self.player.memory(address, length) {
                        PlayerReply::Ready(bytes) if bytes.len() == length => bytes,
                        PlayerReply::Ready(_) | PlayerReply::Unsupported => {
                            return Ok(Service::Status(INVALID));
                        }
                        PlayerReply::Missing => {
                            return Ok(Service::Need(PlayerOperation::Memory { address, length }));
                        }
                    }
                };
                self.vm.memory_mut().write(destination, bytes)?;
                Ok(Service::Status(OK))
            }
            0x550 => self.media_check(args),
            0x8010 => {
                let message = self.vm.memory().read(args[0], args[1] as usize)?.to_vec();
                match self.player_reply.as_ref() {
                    Some(PlayerMaterial::DiagnosticAcknowledged) => Ok(Service::Status(OK)),
                    Some(PlayerMaterial::Unsupported) => Ok(Service::Status(INVALID)),
                    _ => Ok(Service::Need(PlayerOperation::DiagnosticLog { message })),
                }
            }
            _ => Err(BdPlusError::UnsupportedTrap { trap_id: trap, pc }),
        }
    }

    fn aes(&mut self, args: [u32; 5]) -> BdPlusResult<Service> {
        if !matches!(
            self.vm.event(),
            Some(BdPlusEvent::Start)
                | Some(BdPlusEvent::Shutdown)
                | Some(BdPlusEvent::TitleInit { .. })
        ) {
            return Err(BdPlusError::TrapNotPermittedInEvent { trap_id: 0x110 });
        }
        let op = args[4];
        if !matches!(op, 0..=6 | 0xfff1_0000..=0xfff1_0002) {
            return Ok(Service::Status(INVALID));
        }
        let length = (args[2] as usize)
            .checked_mul(16)
            .ok_or(BdPlusError::OffsetOverflow)?;
        let destination_range = checked_range(args[0], length)?;
        let source_range = checked_range(args[1], length)?;
        if args[0] != args[1] && ranges_overlap(&destination_range, &source_range) {
            return Ok(Service::Status(INVALID));
        }
        let source = self.vm.memory().read(args[1], length)?.to_vec();
        self.vm.memory().read(args[0], length)?;
        self.vm.memory().read(args[3], 16)?;
        let key: [u8; 16] = if op == 0xfff1_0002 {
            match self.player_reply.as_ref() {
                Some(PlayerMaterial::MediaKey(key)) => *key,
                Some(PlayerMaterial::Unsupported) => return Ok(Service::Status(INVALID)),
                _ => match self.disc.as_ref() {
                    Some(BdPlusDiscContext {
                        media_key: Some(key),
                        ..
                    }) => *key,
                    _ => return Ok(Service::Need(PlayerOperation::MediaKey)),
                },
            }
        } else {
            let supplied: [u8; 16] = self.vm.memory().read(args[3], 16)?.try_into().unwrap();
            if matches!(op, 0xfff1_0000 | 0xfff1_0001) {
                supplied
            } else {
                match self.player_reply.as_ref() {
                    Some(PlayerMaterial::UnwrappedAesKey(key)) => *key,
                    Some(PlayerMaterial::Unsupported) => return Ok(Service::Status(INVALID)),
                    _ => match self.player.unwrap_aes_key(op, &supplied) {
                        PlayerReply::Ready(key) => key,
                        PlayerReply::Unsupported => return Ok(Service::Status(INVALID)),
                        PlayerReply::Missing => {
                            return Ok(Service::Need(PlayerOperation::AesKey {
                                key_id: op,
                                wrapped_key: supplied,
                            }));
                        }
                    },
                }
            }
        };
        let mut output = Vec::with_capacity(length);
        for block in source.as_chunks::<16>().0 {
            let decoded = if op == 0xfff1_0000 {
                crate::crypto::aes_ecb_encrypt(&key, block)
            } else {
                crate::crypto::aes_ecb_decrypt(&key, block)
            };
            output.extend_from_slice(&decoded);
        }
        self.vm.memory_mut().write(args[0], &output)?;
        Ok(Service::Status(OK))
    }

    fn slot_attach(&mut self, slot: u32, code_words: u32, pc: u32) -> BdPlusResult<Service> {
        if let Some(service) = self.provision_slot_storage() {
            return Ok(service);
        }
        let Some(slots) = self.slots.as_mut() else {
            return Ok(Service::Need(PlayerOperation::SlotStorage));
        };
        slots.reset_attachment();
        if slot == u32::MAX {
            if let Some(PlayerMaterial::VolumeId(volume_id)) = self.player_reply.take() {
                self.disc.get_or_insert_with(Default::default).volume_id = Some(volume_id);
                self.player_request = None;
            }
            let volume_id = match self.player_reply.as_ref() {
                Some(PlayerMaterial::Unsupported) => return Ok(Service::Status(INVALID)),
                _ => self.disc.as_ref().and_then(|disc| disc.volume_id),
            };
            let Some(volume_id) = volume_id else {
                return Ok(Service::Need(PlayerOperation::VolumeId));
            };
            return Ok(Service::Status(
                slots.allocate(volume_id).unwrap_or(INVALID),
            ));
        }
        let Ok(index) = usize::try_from(slot) else {
            return Ok(Service::Status(INVALID));
        };
        if index >= SLOT_COUNT {
            return Ok(Service::Status(INVALID));
        }
        let Some(code_length) = (code_words as usize).checked_mul(4) else {
            return Ok(Service::Status(INVALID));
        };
        let code = self.vm.memory().read(pc, code_length)?;
        let entry = self.vm.memory().read(0x1000, 16)?;
        let mut hash = Sha1::new();
        hash.update(pc.to_be_bytes());
        hash.update(self.vm.instruction_filter().to_be_bytes());
        hash.update(entry);
        hash.update(code);
        let first = hash.finalize();
        let digest: [u8; 20] = Sha1::digest(first).into();
        Ok(Service::Status(if slots.authenticate(index, &digest) {
            OK
        } else {
            INVALID
        }))
    }

    fn slot_read(&mut self, destination: u32, slot: u32) -> BdPlusResult<Service> {
        if let Some(service) = self.provision_slot_storage() {
            return Ok(service);
        }
        let Some(slots) = self.slots.as_ref() else {
            return Ok(Service::Need(PlayerOperation::SlotStorage));
        };
        if slot == u32::MAX {
            self.vm
                .memory_mut()
                .write(destination, &slots.status_bytes())?;
            return Ok(Service::Status(OK));
        }
        let Ok(index) = usize::try_from(slot) else {
            return Ok(Service::Status(INVALID));
        };
        let Some(record) = slots.read(index) else {
            return Ok(Service::Status(INVALID));
        };
        self.vm.memory_mut().write(destination, &record)?;
        Ok(Service::Status(OK))
    }

    fn slot_write(&mut self, source: u32) -> BdPlusResult<Service> {
        if let Some(service) = self.provision_slot_storage() {
            return Ok(service);
        }
        let Some(slots) = self.slots.as_mut() else {
            return Ok(Service::Need(PlayerOperation::SlotStorage));
        };
        let mut record = [0; SLOT_SIZE];
        record.copy_from_slice(self.vm.memory().read(source, SLOT_SIZE)?);
        slots.write_attached(&record);
        Ok(Service::Status(OK))
    }

    fn provision_slot_storage(&mut self) -> Option<Service> {
        if self.slots.is_some() {
            return None;
        }
        match self.player_reply.take() {
            Some(PlayerMaterial::SlotStorage(storage)) => {
                self.slots = Some(SlotSession::new(storage));
                self.player_request = None;
                None
            }
            Some(PlayerMaterial::Unsupported) => Some(Service::Status(INVALID)),
            _ => Some(Service::Need(PlayerOperation::SlotStorage)),
        }
    }

    fn sha1(&mut self, args: [u32; 5]) -> BdPlusResult<Service> {
        const CONTEXT_SIZE: usize = 0x200;
        const UPDATE: u32 = 0;
        const INIT: u32 = 1;
        const FINAL: u32 = 2;
        const BLOCK: u32 = 3;

        let (context, source, length, operation) = (args[0], args[1], args[2], args[3]);
        if context & 3 != 0 {
            return Ok(Service::Status(INVALID));
        }
        let context_range = checked_range(context, CONTEXT_SIZE)?;
        let source_range = checked_range(source, length as usize)?;
        self.vm.memory().read(context, CONTEXT_SIZE)?;
        let input = self.vm.memory().read(source, length as usize)?.to_vec();
        if ranges_overlap(&context_range, &source_range) {
            return Ok(Service::Status(INVALID));
        }

        match operation {
            INIT => {
                self.vm
                    .memory_mut()
                    .read_mut(context, CONTEXT_SIZE)?
                    .fill(0);
                let mut state = Sha1::new();
                state.update(&input);
                self.sha1.insert(context, state);
            }
            UPDATE => {
                self.sha1.entry(context).or_default().update(&input);
            }
            FINAL => {
                let mut state = self.sha1.remove(&context).unwrap_or_default();
                state.update(&input);
                let digest = state.finalize();
                self.vm.memory_mut().write(context, &digest)?;
            }
            BLOCK => {
                let digest = Sha1::digest(input);
                self.vm.memory_mut().write(context, &digest)?;
            }
            _ => return Ok(Service::Status(INVALID)),
        }
        Ok(Service::Status(OK))
    }

    fn private_key(&mut self, args: [u32; 5]) -> BdPlusResult<Service> {
        const SIGNATURE_SIZE: usize = 40;
        const MAX_MESSAGE_SIZE: usize = 0x20_000;

        if !matches!(
            self.vm.event(),
            Some(BdPlusEvent::Start)
                | Some(BdPlusEvent::Shutdown)
                | Some(BdPlusEvent::TitleInit { .. })
        ) {
            return Err(BdPlusError::TrapNotPermittedInEvent { trap_id: 0x120 });
        }
        let (key_id, destination, source, length, control_word) =
            (args[0], args[1], args[2], args[3], args[4]);
        if key_id > 1 || length as usize > MAX_MESSAGE_SIZE {
            return Ok(Service::Status(INVALID));
        }
        let output = checked_range(destination, SIGNATURE_SIZE)?;
        let input = checked_range(source, length as usize)?;
        self.vm.memory().read(destination, SIGNATURE_SIZE)?;
        let message = self.vm.memory().read(source, length as usize)?.to_vec();
        if ranges_overlap(&output, &input) {
            return Ok(Service::Status(INVALID));
        }
        let signature = match self.player_reply.as_ref() {
            Some(PlayerMaterial::Signature(signature)) => signature,
            Some(PlayerMaterial::Unsupported) => return Ok(Service::Status(INVALID)),
            _ => {
                return Ok(Service::Need(PlayerOperation::PrivateKeySign {
                    key_id,
                    control_word,
                    message,
                }));
            }
        };
        self.vm.memory_mut().write(destination, signature)?;
        Ok(Service::Status(OK))
    }

    fn media_check(&mut self, args: [u32; 6]) -> BdPlusResult<Service> {
        const BLOCK_SIZE: usize = 0x200;
        const DIGEST_SIZE: usize = 20;
        const MAX_LENGTH: usize = 0x20_0000;

        if !matches!(self.vm.event(), Some(BdPlusEvent::Start)) {
            return Err(BdPlusError::TrapNotPermittedInEvent { trap_id: 0x550 });
        }
        let (path_address, path_length, offset_high, offset_low, length_address, destination) =
            (args[0], args[1], args[2], args[3], args[4], args[5]);
        if path_length == 0 || path_length > 0x400 || length_address & 3 != 0 {
            return Ok(Service::Status(INVALID));
        }
        let requested = self.vm.memory().load_u32(length_address) as usize;
        if requested > MAX_LENGTH {
            return Ok(Service::Status(TOO_LARGE));
        }
        if !requested.is_multiple_of(BLOCK_SIZE) {
            return Ok(Service::Status(INVALID));
        }
        let digest_capacity = requested / BLOCK_SIZE * DIGEST_SIZE;
        self.vm.memory().read(length_address, 4)?;
        self.vm.memory().read(destination, digest_capacity)?;
        let path = self
            .vm
            .memory()
            .read(path_address, path_length as usize)?
            .to_vec();
        let offset = (u64::from(offset_high) << 32) | u64::from(offset_low);
        let bytes = match self.player_reply.as_ref() {
            Some(PlayerMaterial::Media(bytes))
                if bytes.len() <= requested && bytes.len() % BLOCK_SIZE == 0 =>
            {
                bytes
            }
            Some(PlayerMaterial::Media(_)) | Some(PlayerMaterial::Unsupported) => {
                return Ok(Service::Status(INVALID));
            }
            _ => {
                return Ok(Service::Need(PlayerOperation::MediaRead {
                    path,
                    offset,
                    length: requested,
                }));
            }
        };

        let mut digests = Vec::with_capacity(bytes.len() / BLOCK_SIZE * DIGEST_SIZE);
        for block in bytes.as_chunks::<BLOCK_SIZE>().0 {
            digests.extend_from_slice(&Sha1::digest(block));
        }
        self.vm.memory_mut().write(destination, &digests)?;
        self.vm
            .memory_mut()
            .store_u32(length_address, bytes.len() as u32);
        Ok(Service::Status(OK))
    }
}

enum Service {
    Status(u32),
    Need(PlayerOperation),
}

fn checked_range(address: u32, length: usize) -> BdPlusResult<std::ops::Range<usize>> {
    let start = address as usize;
    let end = start
        .checked_add(length)
        .ok_or(BdPlusError::OffsetOverflow)?;
    Ok(start..end)
}

fn ranges_overlap(left: &std::ops::Range<usize>, right: &std::ops::Range<usize>) -> bool {
    left.start < right.end && right.start < left.end
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
