use sysinfo::{Components, System};
use vantage_protocol::telemetry::{DeviceInfo, TempReading};

pub struct Sampler {
    sys: System,
    components: Components,
}

impl Sampler {
    pub fn new() -> Self {
        Self {
            sys: System::new_all(),
            components: Components::new_with_refreshed_list(),
        }
    }

    pub fn sample(&mut self) -> DeviceInfo {
        self.sys.refresh_cpu_usage();
        self.sys.refresh_memory();
        self.components.refresh(false);

        let cpu_percent = self.sys.global_cpu_usage();
        let temps = self
            .components
            .iter()
            .filter_map(|c| {
                let celsius = c.temperature()?;
                Some(TempReading {
                    label: c.label().to_string(),
                    celsius,
                })
            })
            .collect();

        DeviceInfo {
            cpu_percent,
            mem_used_mb: self.sys.used_memory() / (1024 * 1024),
            mem_total_mb: self.sys.total_memory() / (1024 * 1024),
            temps,
            uptime_s: System::uptime(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sample_reports_nonzero_total_memory() {
        let mut s = Sampler::new();
        let info = s.sample();
        assert!(info.mem_total_mb > 0, "total memory should be discoverable");
    }
}
