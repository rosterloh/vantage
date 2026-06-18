use std::collections::HashMap;

use vantage_protocol::{RobotId, SessionId};

/// Maps a client session to the robot it is connected to, and back.
#[derive(Default)]
pub struct Sessions {
    by_session: HashMap<SessionId, RobotId>,
}

impl Sessions {
    pub fn open(&mut self, session: SessionId, robot: RobotId) {
        self.by_session.insert(session, robot);
    }

    pub fn robot_for(&self, session: &SessionId) -> Option<&RobotId> {
        self.by_session.get(session)
    }

    /// Remove a session; returns the robot it was attached to, if any.
    pub fn close(&mut self, session: &SessionId) -> Option<RobotId> {
        self.by_session.remove(session)
    }

    /// All open client sessions for a given robot (used when the robot drops).
    pub fn sessions_for(&self, robot: &RobotId) -> Vec<SessionId> {
        self.by_session
            .iter()
            .filter(|(_, r)| *r == robot)
            .map(|(s, _)| s.clone())
            .collect()
    }

    pub fn consumer_count(&self) -> usize {
        self.by_session.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_then_lookup() {
        let mut s = Sessions::default();
        s.open(SessionId("sess-1".into()), RobotId("r1".into()));
        assert_eq!(s.robot_for(&SessionId("sess-1".into())), Some(&RobotId("r1".into())));
        assert_eq!(s.consumer_count(), 1);
    }

    #[test]
    fn close_returns_robot_and_drops_count() {
        let mut s = Sessions::default();
        s.open(SessionId("sess-1".into()), RobotId("r1".into()));
        assert_eq!(s.close(&SessionId("sess-1".into())), Some(RobotId("r1".into())));
        assert_eq!(s.consumer_count(), 0);
    }

    #[test]
    fn sessions_for_robot() {
        let mut s = Sessions::default();
        s.open(SessionId("a".into()), RobotId("r1".into()));
        s.open(SessionId("b".into()), RobotId("r1".into()));
        s.open(SessionId("c".into()), RobotId("r2".into()));
        let mut got = s.sessions_for(&RobotId("r1".into()));
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got, vec![SessionId("a".into()), SessionId("b".into())]);
    }
}
