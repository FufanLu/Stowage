//! Hybrid Logical Clock (Kulkarni et al., 2014).
//!
//! A [`Timestamp`] is `(wall, logical)`: physical nanoseconds plus a counter that
//! breaks ties when events happen faster than the clock ticks or when a remote node's
//! clock is ahead of ours. HLC timestamps are
//! - **monotonic** on each node, even if the wall clock steps backwards;
//! - **causal**: if event A happened-before event B (via a message), `ts(A) < ts(B)`;
//! - **close to real time**: `wall` never runs ahead of the fastest node's clock, and
//!   we refuse remote timestamps more than `max_offset` ahead of us.
//!
//! The transaction layer uses these as MVCC versions, so the encoding
//! ([`Timestamp::encode`]) is fixed-width big-endian and sorts the same way as the
//! struct.

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::env::Clock;
use crate::error::{Error, Result};

/// A point in hybrid-logical time. Ordering is lexicographic on `(wall, logical)`.
#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp {
    /// Nanoseconds since the UNIX epoch.
    pub wall: u64,
    /// Tie-breaking counter within one `wall` value.
    pub logical: u32,
}

impl Timestamp {
    pub const MIN: Timestamp = Timestamp {
        wall: 0,
        logical: 0,
    };
    pub const MAX: Timestamp = Timestamp {
        wall: u64::MAX,
        logical: u32::MAX,
    };
    /// Width of [`Timestamp::encode`].
    pub const ENCODED_LEN: usize = 12;

    pub const fn new(wall: u64, logical: u32) -> Self {
        Self { wall, logical }
    }

    /// The smallest timestamp strictly greater than `self`.
    pub fn next(self) -> Self {
        match self.logical.checked_add(1) {
            Some(logical) => Self { logical, ..self },
            None => Self::new(self.wall + 1, 0),
        }
    }

    /// Big-endian `wall ‖ logical`; byte order equals timestamp order.
    pub fn encode(self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[..8].copy_from_slice(&self.wall.to_be_bytes());
        out[8..].copy_from_slice(&self.logical.to_be_bytes());
        out
    }

    pub fn decode(buf: &[u8]) -> Result<Self> {
        let buf: &[u8; Self::ENCODED_LEN] = buf
            .get(..Self::ENCODED_LEN)
            .and_then(|b| b.try_into().ok())
            .ok_or_else(|| {
                Error::Corruption(format!("timestamp needs 12 bytes, got {}", buf.len()))
            })?;
        let mut wall = [0u8; 8];
        let mut logical = [0u8; 4];
        wall.copy_from_slice(&buf[..8]);
        logical.copy_from_slice(&buf[8..]);
        Ok(Self::new(
            u64::from_be_bytes(wall),
            u32::from_be_bytes(logical),
        ))
    }
}

/// Rendered like `1730881234.000000123,5` (seconds.nanos,logical) — the same shape
/// CockroachDB uses, which makes traces easy to line up across nodes.
impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{:09},{}",
            self.wall / 1_000_000_000,
            self.wall % 1_000_000_000,
            self.logical
        )
    }
}

impl fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// A node's hybrid logical clock. Cheap to share: wrap it in an `Arc`.
#[derive(Debug)]
pub struct Hlc {
    clock: Arc<dyn Clock>,
    max_offset_nanos: u64,
    last: Mutex<Timestamp>,
}

impl Hlc {
    /// `max_offset_nanos` bounds how far any two nodes' wall clocks may disagree
    /// (CockroachDB defaults to 500ms). Remote timestamps beyond it are rejected.
    pub fn new(clock: Arc<dyn Clock>, max_offset_nanos: u64) -> Self {
        Self {
            clock,
            max_offset_nanos,
            last: Mutex::new(Timestamp::MIN),
        }
    }

    pub fn max_offset_nanos(&self) -> u64 {
        self.max_offset_nanos
    }

    /// Timestamp for a local event (or for sending a message).
    pub fn now(&self) -> Timestamp {
        let pt = self.clock.wall_nanos();
        let mut last = self.lock();
        *last = if pt > last.wall {
            Timestamp::new(pt, 0)
        } else {
            last.next()
        };
        *last
    }

    /// Merge a timestamp received from another node and return a timestamp for the
    /// receive event, which is greater than both `remote` and anything issued before.
    pub fn update(&self, remote: Timestamp) -> Result<Timestamp> {
        let pt = self.clock.wall_nanos();
        if remote.wall > pt.saturating_add(self.max_offset_nanos) {
            return Err(Error::ClockOffset {
                offset_nanos: remote.wall - pt,
                max_offset_nanos: self.max_offset_nanos,
            });
        }
        let mut last = self.lock();
        let wall = pt.max(last.wall).max(remote.wall);
        let next = if wall == last.wall && wall == remote.wall {
            Timestamp::new(wall, last.logical.max(remote.logical)).next()
        } else if wall == last.wall {
            last.next()
        } else if wall == remote.wall {
            remote.next()
        } else {
            Timestamp::new(wall, 0)
        };
        *last = next;
        Ok(next)
    }

    /// Latest timestamp issued, without advancing the clock.
    pub fn last(&self) -> Timestamp {
        *self.lock()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Timestamp> {
        // A panic while holding this lock cannot leave a torn Timestamp (it is Copy
        // and assigned in one step), so poisoning carries no information.
        self.last.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[derive(Debug, Default)]
    struct ManualClock(AtomicU64);
    impl ManualClock {
        fn set(&self, v: u64) {
            self.0.store(v, Ordering::SeqCst);
        }
    }
    impl Clock for ManualClock {
        fn monotonic_nanos(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
        fn wall_nanos(&self) -> u64 {
            self.0.load(Ordering::SeqCst)
        }
    }

    fn hlc(start: u64) -> (Arc<ManualClock>, Hlc) {
        let clock = Arc::new(ManualClock::default());
        clock.set(start);
        (clock.clone(), Hlc::new(clock, 500))
    }

    #[test]
    fn follows_wall_clock_when_it_advances() {
        let (clock, h) = hlc(1_000);
        assert_eq!(h.now(), Timestamp::new(1_000, 0));
        clock.set(2_000);
        assert_eq!(h.now(), Timestamp::new(2_000, 0));
    }

    #[test]
    fn logical_breaks_ties_and_survives_clock_going_backwards() {
        let (clock, h) = hlc(1_000);
        let a = h.now();
        let b = h.now();
        clock.set(900); // NTP step backwards
        let c = h.now();
        assert!(a < b && b < c);
        assert_eq!(c, Timestamp::new(1_000, 2));
    }

    #[test]
    fn update_orders_receive_after_send() {
        let (_c1, sender) = hlc(1_200);
        let (_c2, receiver) = hlc(1_000); // 200ns behind, within max offset
        let sent = sender.now();
        let recv = receiver.update(sent).unwrap();
        assert!(recv > sent);
        assert_eq!(recv, Timestamp::new(1_200, 1));
        assert!(receiver.now() > recv);
    }

    #[test]
    fn update_takes_max_logical_when_walls_tie() {
        let (_c, h) = hlc(1_000);
        h.now(); // (1000,0)
        let recv = h.update(Timestamp::new(1_000, 7)).unwrap();
        assert_eq!(recv, Timestamp::new(1_000, 8));
    }

    #[test]
    fn rejects_remote_beyond_max_offset() {
        let (_c, h) = hlc(1_000);
        let err = h.update(Timestamp::new(1_501, 0)).unwrap_err();
        assert!(matches!(
            err,
            Error::ClockOffset {
                offset_nanos: 501,
                max_offset_nanos: 500
            }
        ));
        assert!(h.update(Timestamp::new(1_500, 0)).is_ok());
    }

    #[test]
    fn encoding_preserves_order() {
        let ts = [
            Timestamp::new(0, 0),
            Timestamp::new(0, 1),
            Timestamp::new(1, 0),
            Timestamp::new(1, u32::MAX),
            Timestamp::new(256, 0),
            Timestamp::new(u64::MAX, 0),
        ];
        for w in ts.windows(2) {
            assert!(w[0] < w[1]);
            assert!(w[0].encode() < w[1].encode());
            assert_eq!(Timestamp::decode(&w[1].encode()).unwrap(), w[1]);
        }
        assert!(Timestamp::decode(&[0; 11]).is_err());
    }

    #[test]
    fn next_carries_into_wall() {
        assert_eq!(Timestamp::new(5, u32::MAX).next(), Timestamp::new(6, 0));
    }

    #[test]
    fn display_matches_cockroach_style() {
        let ts = Timestamp::new(1_730_881_234_000_000_123, 5);
        assert_eq!(ts.to_string(), "1730881234.000000123,5");
    }
}
