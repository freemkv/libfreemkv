//! Local BD+ event execution and player host services.
//!
//! No network calls occur here. Missing material leaves the trap pending;
//! after the application provisions it, run retries the same host operation.

use super::{
    BdPlusContent, BdPlusDiscContext, BdPlusError, BdPlusEvent, BdPlusFixupTable, BdPlusPlayer,
    BdPlusResult, BdPlusVm, PlayerReply, Svm, VmBoundary,
};
use crate::Halt;

const OK: u32 = 0;
const INVALID: u32 = 0x8000_0001;

/// Missing semantic capability; IDs/ranges contain no secret values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PlayerOperation {
    AesKey { key_id: u32 },
    Discovery { device: u32, question: u32 },
    Memory { address: u32, length: usize },
    MediaKey,
}

/// A stable request handle for material acquired outside VM execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PlayerRequest {
    id: u64,
    operation: PlayerOperation,
}

impl PlayerRequest {
    pub fn id(self) -> u64 {
        self.id
    }

    pub fn operation(self) -> PlayerOperation {
        self.operation
    }
}

/// Owned semantic result supplied by an application-side provider.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PlayerMaterial {
    UnwrappedAesKey([u8; 16]),
    Discovery(Vec<u8>),
    Memory(Vec<u8>),
    MediaKey([u8; 16]),
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
    pub fn begin_event(&mut self, event: BdPlusEvent) -> BdPlusResult<()> {
        self.vm.begin_event(event)
    }

    /// Bind an owned provider reply to the exact request that produced it.
    pub fn provide_player_material(
        &mut self,
        request_id: u64,
        material: PlayerMaterial,
    ) -> BdPlusResult<()> {
        let request = self.player_request.ok_or(BdPlusError::NoPendingTrap)?;
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
                (request.operation, &material),
                (
                    PlayerOperation::AesKey { .. },
                    PlayerMaterial::UnwrappedAesKey(_)
                ) | (
                    PlayerOperation::Discovery { .. },
                    PlayerMaterial::Discovery(_)
                ) | (PlayerOperation::Memory { .. }, PlayerMaterial::Memory(_))
                    | (PlayerOperation::MediaKey, PlayerMaterial::MediaKey(_))
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
                    let request = match self.player_request {
                        Some(request) => {
                            if request.operation != operation {
                                return Err(BdPlusError::PlayerReplyType);
                            }
                            request
                        }
                        None => {
                            let request = PlayerRequest {
                                id: self.next_request_id,
                                operation,
                            };
                            self.next_request_id = self.next_request_id.wrapping_add(1).max(1);
                            self.player_request = Some(request);
                            request
                        }
                    };
                    return Ok(SessionBoundary::NeedPlayerMaterial(request));
                }
            }
        }
    }

    fn service(&mut self, trap: u32, pc: u32) -> BdPlusResult<Service> {
        let args: [u32; 5] = std::array::from_fn(|i| self.vm.argument(i as u8));
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
            0x110 => self.aes(args),
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
            0x330 => {
                self.vm
                    .memory_mut()
                    .read_mut(args[0], args[2] as usize)?
                    .fill(args[1] as u8);
                Ok(Service::Status(OK))
            }
            0x540 => {
                // Nonzero offset behavior still requires reference checks.
                if args[2] != 0 {
                    return Err(BdPlusError::UnsupportedContentOffset { offset: args[2] });
                }
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
                // Discovery(1,3) is synthesized from the disc context, not
                // stored in the profile: year/month/day/hour/min/sec/centi at
                // offset 0, 16-byte VID at offset 24, static tail trailing.
                let synthesized_1_3 = if device == 1 && question == 3 {
                    self.disc.as_ref().and_then(|disc| disc.discovery_record())
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
                            }));
                        }
                    }
                };
                let capacity = self.vm.memory().read(args[3], 4)?;
                let capacity = u32::from_be_bytes(capacity.try_into().unwrap()) as usize;
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
            _ => Err(BdPlusError::UnsupportedTrap { trap_id: trap, pc }),
        }
    }

    fn aes(&mut self, args: [u32; 5]) -> BdPlusResult<Service> {
        let length = (args[2] as usize)
            .checked_mul(16)
            .ok_or(BdPlusError::OffsetOverflow)?;
        let source = self.vm.memory().read(args[1], length)?.to_vec();
        self.vm.memory().read(args[0], length)?;
        let op = args[4];
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
                            return Ok(Service::Need(PlayerOperation::AesKey { key_id: op }));
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
}

enum Service {
    Status(u32),
    Need(PlayerOperation),
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod tests;
