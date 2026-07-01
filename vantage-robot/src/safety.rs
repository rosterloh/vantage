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
        newly_stale.into_iter().map(|s| (s, "control stale")).collect()
    }

    /// Whether commands for this session may currently be acted on. False for an
    /// unknown (unarmed) or a tripped session.
    pub fn is_live(&self, session: &SessionId) -> bool {
        matches!(self.tripped.get(session), Some(false))
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
