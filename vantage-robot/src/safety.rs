//! Teleop disconnect failsafe. A per-session watchdog that trips a session into a
//! safe state when control input goes stale, so the robot never acts on a command
//! from a session it can no longer hear from. Pure logic with an injected clock so
//! the safety behaviour is verified deterministically, independent of the network.
//!
//! The safety gate: the robot MUST NOT act on a command for a session unless that
//! session's watchdog is armed and live (`is_live`). Staleness (no control within
//! `CONTROL_TIMEOUT`), a channel close, or a disconnect all revoke liveness.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use vantage_protocol::SessionId;

/// No control message (command or keepalive) within this window ⇒ safe state.
/// The client sends a keepalive every 100 ms, so ~5 consecutive losses trip it.
pub const CONTROL_TIMEOUT: Duration = Duration::from_millis(500);

#[derive(Default)]
pub struct Watchdog {
    last: HashMap<SessionId, Instant>,
    tripped: HashMap<SessionId, bool>,
}

impl Watchdog {
    /// Begin watching a session. It starts live with a full timeout of grace.
    pub fn arm(&mut self, session: SessionId, now: Instant) {
        self.last.insert(session.clone(), now);
        self.tripped.insert(session, false);
    }

    /// Stop watching a session (disconnect / channel close / teardown).
    pub fn disarm(&mut self, session: &SessionId) {
        self.last.remove(session);
        self.tripped.remove(session);
    }

    /// A fresh control message/keepalive arrived: re-arm and clear any staleness trip
    /// (a momentary stall self-heals when input resumes).
    pub fn feed(&mut self, session: &SessionId, now: Instant) {
        if let Some(t) = self.last.get_mut(session) {
            *t = now;
        }
        if let Some(tr) = self.tripped.get_mut(session) {
            *tr = false;
        }
    }

    /// Sessions that newly went stale this tick, each with a safe-state reason
    /// (idempotent: a session that stays stale is reported only on the tick it
    /// first trips).
    pub fn tick(&mut self, now: Instant) -> Vec<(SessionId, &'static str)> {
        let mut newly_stale: Vec<SessionId> = Vec::new();
        for (s, &t) in &self.last {
            let already_tripped = *self.tripped.get(s).unwrap_or(&false);
            if now.duration_since(t) > CONTROL_TIMEOUT && !already_tripped {
                newly_stale.push(s.clone());
            }
        }
        for s in &newly_stale {
            self.tripped.insert(s.clone(), true);
        }
        newly_stale
            .into_iter()
            .map(|s| (s, "control stale"))
            .collect()
    }

    /// Whether commands for this session may currently be acted on. False for an
    /// unknown (unarmed) or a tripped session.
    pub fn is_live(&self, session: &SessionId) -> bool {
        matches!(self.tripped.get(session), Some(false))
    }
}

/// A command whose estimated queueing delay exceeds this (beyond the session's
/// best-observed path delay) is too old to act on and is dropped.
pub const STALE_LIMIT: Duration = Duration::from_millis(500);

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Admit {
    Fresh,
    /// seq did not advance: a reordered or duplicated datagram.
    OutOfOrder,
    /// Estimated age beyond STALE_LIMIT: arrived, but too late to act on.
    Stale,
}

/// Per-session admission gate for `ControlEnvelope`s. Age is estimated without
/// clock sync: the minimum observed spread (arrival − sent) approximates clock
/// offset + best-case path delay, so spread − min_spread approximates extra
/// queueing delay. The first envelope of a session is Fresh and seeds the baseline.
#[derive(Default)]
pub struct Freshness {
    sessions: HashMap<SessionId, SessionFreshness>,
}

struct SessionFreshness {
    last_seq: u64,
    min_spread_ms: i128,
}

impl Freshness {
    pub fn admit(&mut self, session: &SessionId, seq: u64, sent_ms: u64, arrival_ms: u64) -> Admit {
        let spread = arrival_ms as i128 - sent_ms as i128;
        match self.sessions.get_mut(session) {
            None => {
                self.sessions.insert(
                    session.clone(),
                    SessionFreshness {
                        last_seq: seq,
                        min_spread_ms: spread,
                    },
                );
                Admit::Fresh
            }
            Some(st) => {
                if seq <= st.last_seq {
                    return Admit::OutOfOrder;
                }
                st.last_seq = seq;
                if spread < st.min_spread_ms {
                    st.min_spread_ms = spread;
                }
                if spread - st.min_spread_ms > STALE_LIMIT.as_millis() as i128 {
                    Admit::Stale
                } else {
                    Admit::Fresh
                }
            }
        }
    }

    pub fn forget(&mut self, session: &SessionId) {
        self.sessions.remove(session);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sess() -> SessionId {
        SessionId("s1".into())
    }

    #[test]
    fn armed_session_is_live_and_does_not_trip_within_timeout() {
        let t0 = Instant::now();
        let mut w = Watchdog::default();
        w.arm(sess(), t0);
        assert!(w.is_live(&sess()));
        assert!(w.tick(t0 + Duration::from_millis(400)).is_empty());
        assert!(w.is_live(&sess()));
    }

    #[test]
    fn stale_session_trips_once_then_is_idempotent() {
        let t0 = Instant::now();
        let mut w = Watchdog::default();
        w.arm(sess(), t0);
        let trips = w.tick(t0 + Duration::from_millis(600));
        assert_eq!(trips, vec![(sess(), "control stale")]);
        assert!(!w.is_live(&sess()));
        // Still stale on the next tick, but already reported — no duplicate.
        assert!(w.tick(t0 + Duration::from_millis(700)).is_empty());
    }

    #[test]
    fn feed_keeps_it_live_and_recovers_after_a_trip() {
        let t0 = Instant::now();
        let mut w = Watchdog::default();
        w.arm(sess(), t0);
        // Trip it.
        assert!(!w.tick(t0 + Duration::from_millis(600)).is_empty());
        assert!(!w.is_live(&sess()));
        // A fresh command clears the trip (recovery).
        w.feed(&sess(), t0 + Duration::from_millis(650));
        assert!(w.is_live(&sess()));
        assert!(w.tick(t0 + Duration::from_millis(700)).is_empty());
    }

    #[test]
    fn disarmed_session_is_not_live_and_never_trips() {
        let t0 = Instant::now();
        let mut w = Watchdog::default();
        w.arm(sess(), t0);
        w.disarm(&sess());
        assert!(!w.is_live(&sess()));
        assert!(w.tick(t0 + Duration::from_secs(10)).is_empty());
    }
}

#[cfg(test)]
mod freshness_tests {
    use super::*;

    fn sess() -> SessionId {
        SessionId("s1".into())
    }

    #[test]
    fn first_envelope_is_fresh_and_seeds_the_baseline() {
        let mut f = Freshness::default();
        assert_eq!(f.admit(&sess(), 1, 0, 1000), Admit::Fresh);
    }

    #[test]
    fn advancing_seq_with_steady_delay_stays_fresh() {
        let mut f = Freshness::default();
        f.admit(&sess(), 1, 0, 20);
        assert_eq!(f.admit(&sess(), 2, 100, 125), Admit::Fresh);
        assert_eq!(f.admit(&sess(), 3, 200, 218), Admit::Fresh);
    }

    #[test]
    fn reordered_or_duplicate_seq_is_rejected() {
        let mut f = Freshness::default();
        f.admit(&sess(), 5, 0, 10);
        assert_eq!(f.admit(&sess(), 5, 0, 20), Admit::OutOfOrder);
        assert_eq!(f.admit(&sess(), 4, 0, 30), Admit::OutOfOrder);
    }

    #[test]
    fn command_delayed_beyond_stale_limit_is_stale() {
        let mut f = Freshness::default();
        f.admit(&sess(), 1, 0, 10); // baseline spread 10ms
        // sent at 100, arrives at 700: spread 600, est. age 590 > STALE_LIMIT
        assert_eq!(f.admit(&sess(), 2, 100, 700), Admit::Stale);
    }

    #[test]
    fn baseline_tightens_when_a_faster_path_is_observed() {
        let mut f = Freshness::default();
        f.admit(&sess(), 1, 0, 300); // pessimistic first baseline (spread 300)
        assert_eq!(f.admit(&sess(), 2, 100, 150), Admit::Fresh); // spread 50 tightens it
        // spread 600 → est. age 550 against the tightened baseline → stale
        assert_eq!(f.admit(&sess(), 3, 200, 800), Admit::Stale);
    }

    #[test]
    fn stale_envelope_does_not_block_the_next_fresh_one() {
        let mut f = Freshness::default();
        f.admit(&sess(), 1, 0, 10);
        assert_eq!(f.admit(&sess(), 2, 100, 700), Admit::Stale);
        assert_eq!(f.admit(&sess(), 3, 700, 715), Admit::Fresh);
    }

    #[test]
    fn forget_resets_the_session() {
        let mut f = Freshness::default();
        f.admit(&sess(), 9, 0, 10);
        f.forget(&sess());
        assert_eq!(f.admit(&sess(), 1, 0, 10), Admit::Fresh);
    }

    #[test]
    fn sessions_are_independent() {
        let mut f = Freshness::default();
        f.admit(&SessionId("a".into()), 9, 0, 10);
        assert_eq!(f.admit(&SessionId("b".into()), 1, 0, 10), Admit::Fresh);
    }
}
