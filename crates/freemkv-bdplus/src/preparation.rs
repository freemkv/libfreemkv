//! Resumable disc preparation using the observed BD+ title lifecycle.
//!
//! Later lifecycle work keeps the Security VM/application boundary explicit;
//! a public [BD+ Security VM and BD-J patent] corroborates that communication
//! channel while leaving its detailed protection rules unspecified.
//!
//! [BD+ Security VM and BD-J patent]: https://patentimages.storage.googleapis.com/67/98/54/4ed1397d632ec3/JP2018147536A.pdf

use super::{
    BdPlusError, BdPlusEvent, BdPlusPlayer, BdPlusResult, BdPlusSession, Halt, PlayerRequest,
    SessionBoundary,
};

/// Caller-supplied authored title IDs and progress through their initialization.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BdPlusPreparation {
    titles: Vec<u32>,
    phase: PreparationPhase,
    active: Option<BdPlusEvent>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PreparationPhase {
    Start,
    GlobalTitle,
    Title(usize),
    Complete,
}

/// A preparation slice either made progress, needs material, or produced a FUT.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PreparationBoundary {
    EventFinished(BdPlusEvent),
    NeedPlayerMaterial(PlayerRequest),
    TableReady,
    Yield,
}

impl BdPlusPreparation {
    pub fn new(titles: impl IntoIterator<Item = u32>) -> Self {
        Self {
            titles: titles.into_iter().collect(),
            phase: PreparationPhase::Start,
            active: None,
        }
    }

    fn current_event(&self) -> Option<BdPlusEvent> {
        match self.phase {
            PreparationPhase::Start => Some(BdPlusEvent::Start),
            PreparationPhase::GlobalTitle => Some(BdPlusEvent::TitleInit { title: 0xffff }),
            PreparationPhase::Title(index) => self
                .titles
                .get(index)
                .copied()
                .map(|title| BdPlusEvent::TitleInit { title }),
            PreparationPhase::Complete => None,
        }
    }

    fn finish_current(&mut self) {
        self.active = None;
        self.phase = match self.phase {
            PreparationPhase::Start => PreparationPhase::GlobalTitle,
            PreparationPhase::GlobalTitle if self.titles.is_empty() => PreparationPhase::Complete,
            PreparationPhase::GlobalTitle => PreparationPhase::Title(0),
            PreparationPhase::Title(index) if index + 1 == self.titles.len() => {
                PreparationPhase::Complete
            }
            PreparationPhase::Title(index) => PreparationPhase::Title(index + 1),
            PreparationPhase::Complete => PreparationPhase::Complete,
        };
    }

    fn is_complete(&self) -> bool {
        self.phase == PreparationPhase::Complete
    }
}

impl<P: BdPlusPlayer> BdPlusSession<P> {
    /// Advance preparation without hiding provider requests or execution budgets.
    pub fn advance_preparation(
        &mut self,
        preparation: &mut BdPlusPreparation,
        budget: u64,
        halt: &Halt,
    ) -> BdPlusResult<PreparationBoundary> {
        if preparation.active.is_none() {
            let event = preparation
                .current_event()
                .ok_or(BdPlusError::PreparationEndedWithoutTable)?;
            self.begin_event(event)?;
            preparation.active = Some(event);
        }
        match self.run(budget, halt)? {
            SessionBoundary::FinishedEvent(event) => {
                if preparation.active != Some(event) {
                    return Err(BdPlusError::PreparationEventMismatch);
                }
                preparation.finish_current();
                if self.fixup_table().is_some() {
                    Ok(PreparationBoundary::TableReady)
                } else if preparation.is_complete() {
                    Err(BdPlusError::PreparationEndedWithoutTable)
                } else {
                    Ok(PreparationBoundary::EventFinished(event))
                }
            }
            SessionBoundary::NeedPlayerMaterial(request) => {
                Ok(PreparationBoundary::NeedPlayerMaterial(request))
            }
            SessionBoundary::Watchdog | SessionBoundary::Yield => Ok(PreparationBoundary::Yield),
        }
    }
}

#[cfg(test)]
#[path = "preparation_tests.rs"]
mod tests;
