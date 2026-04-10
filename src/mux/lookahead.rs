//! LookaheadBuffer — generic pre-scan buffer for stream pipelines.
//!
//! Accumulates data up to a configurable limit. When the consumer finds
//! what it needs, the buffer can be drained (fast path, no re-read).
//! If the buffer fills before the consumer is satisfied, it signals
//! overflow — the caller should discard and re-read from the source.
//!
//! Used by MkvStream to collect SPS/PPS before writing the MKV header.
//! Reusable for any stream stage that needs to look ahead.

/// Default lookahead buffer size: 5 MB.
pub const DEFAULT_LOOKAHEAD_SIZE: usize = 5 * 1024 * 1024;

/// Lookahead buffer states.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LookaheadState {
    /// Still collecting data, haven't found what we need yet.
    Collecting,
    /// Found what we need, buffer has the data ready to drain.
    Ready,
    /// Buffer overflowed before finding what we need.
    /// Caller should discard buffer, finish scanning without buffering,
    /// then re-read from the source.
    Overflow,
}

/// A bounded lookahead buffer.
pub struct LookaheadBuffer {
    data: Vec<u8>,
    max_size: usize,
    state: LookaheadState,
}

impl LookaheadBuffer {
    /// Create a new buffer with the given max size.
    /// Pass 0 for no buffering (always overflows immediately).
    pub fn new(max_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(max_size.min(DEFAULT_LOOKAHEAD_SIZE)),
            max_size,
            state: LookaheadState::Collecting,
        }
    }

    /// Push data into the buffer. Returns the new state.
    /// If the buffer would overflow, transitions to Overflow state.
    pub fn push(&mut self, chunk: &[u8]) -> LookaheadState {
        if self.state != LookaheadState::Collecting {
            return self.state;
        }

        if self.data.len() + chunk.len() > self.max_size {
            self.state = LookaheadState::Overflow;
            return self.state;
        }

        self.data.extend_from_slice(chunk);
        self.state
    }

    /// Mark the buffer as ready — we found what we need.
    pub fn mark_ready(&mut self) {
        if self.state == LookaheadState::Collecting {
            self.state = LookaheadState::Ready;
        }
    }

    /// Get the buffered data (only valid in Ready state).
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Take ownership of the buffered data, clearing the buffer.
    pub fn drain(&mut self) -> Vec<u8> {
        self.state = LookaheadState::Collecting;
        std::mem::take(&mut self.data)
    }

    /// Current state.
    pub fn state(&self) -> LookaheadState {
        self.state
    }

    /// How many bytes are buffered.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Is the buffer empty?
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Max size this buffer can hold.
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_flow() {
        let mut buf = LookaheadBuffer::new(100);
        assert_eq!(buf.push(b"hello"), LookaheadState::Collecting);
        assert_eq!(buf.push(b"world"), LookaheadState::Collecting);
        assert_eq!(buf.len(), 10);
        buf.mark_ready();
        assert_eq!(buf.state(), LookaheadState::Ready);
        assert_eq!(buf.data(), b"helloworld");
    }

    #[test]
    fn test_overflow() {
        let mut buf = LookaheadBuffer::new(5);
        assert_eq!(buf.push(b"abc"), LookaheadState::Collecting);
        assert_eq!(buf.push(b"def"), LookaheadState::Overflow);
    }

    #[test]
    fn test_zero_size() {
        let mut buf = LookaheadBuffer::new(0);
        assert_eq!(buf.push(b"a"), LookaheadState::Overflow);
    }
}
