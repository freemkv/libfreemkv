# halt

## Why `Ordering::Relaxed`

`Ordering::Relaxed` is sufficient on both load and store because this flag
is purely advisory — no other memory operations piggyback on it for
happens-before ordering. Callers that need to publish data across threads
do so via channels or other synchronization, not via this bit.

## Why 250ms for `POLL_INTERVAL`

250 ms is the sweet spot between responsiveness (operator presses Stop,
sees it take effect within ~quarter-second) and waste (atomic load + clock
read is cheap but not free at thousands of hertz). It's centralised here
so the half-dozen halt-polling loops across `io` can't drift apart
silently.
