use std::collections::HashMap;
use std::time::{Duration, Instant};

use vantage_protocol::signalling::RobotInfo;
use vantage_protocol::RobotId;

struct Entry {
    info: RobotInfo,
    last_seen: Instant,
}

/// Live set of registered robots with heartbeat-based expiry.
pub struct Registry {
    robots: HashMap<RobotId, Entry>,
    ttl: Duration,
}

impl Registry {
    pub fn new(ttl: Duration) -> Self {
        Self { robots: HashMap::new(), ttl }
    }

    pub fn register(&mut self, info: RobotInfo, now: Instant) {
        self.robots.insert(info.id.clone(), Entry { info, last_seen: now });
    }

    /// Returns false if the robot was unknown (e.g. already expired).
    pub fn heartbeat(&mut self, id: &RobotId, now: Instant) -> bool {
        match self.robots.get_mut(id) {
            Some(e) => { e.last_seen = now; true }
            None => false,
        }
    }

    pub fn remove(&mut self, id: &RobotId) {
        self.robots.remove(id);
    }

    /// Drop entries whose last heartbeat is older than the TTL.
    pub fn prune(&mut self, now: Instant) {
        let ttl = self.ttl;
        self.robots.retain(|_, e| now.duration_since(e.last_seen) < ttl);
    }

    /// Current live robots (call `prune` first for accuracy).
    pub fn list(&self) -> Vec<RobotInfo> {
        self.robots.values().map(|e| e.info.clone()).collect()
    }

    pub fn len(&self) -> usize {
        self.robots.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn robot(id: &str) -> RobotInfo {
        RobotInfo { id: RobotId(id.into()), name: id.into(), capabilities: vec![] }
    }

    #[test]
    fn registered_robot_is_listed() {
        let t0 = Instant::now();
        let mut r = Registry::new(Duration::from_secs(10));
        r.register(robot("a"), t0);
        assert_eq!(r.list().len(), 1);
    }

    #[test]
    fn stale_robot_is_pruned() {
        let t0 = Instant::now();
        let mut r = Registry::new(Duration::from_secs(10));
        r.register(robot("a"), t0);
        r.prune(t0 + Duration::from_secs(11));
        assert_eq!(r.len(), 0);
    }

    #[test]
    fn heartbeat_keeps_robot_alive() {
        let t0 = Instant::now();
        let mut r = Registry::new(Duration::from_secs(10));
        r.register(robot("a"), t0);
        assert!(r.heartbeat(&RobotId("a".into()), t0 + Duration::from_secs(8)));
        r.prune(t0 + Duration::from_secs(11)); // 3s since last heartbeat -> alive
        assert_eq!(r.len(), 1);
    }

    #[test]
    fn heartbeat_for_unknown_robot_returns_false() {
        let mut r = Registry::new(Duration::from_secs(10));
        assert!(!r.heartbeat(&RobotId("ghost".into()), Instant::now()));
    }
}
