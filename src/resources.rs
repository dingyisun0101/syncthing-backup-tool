use crate::config::Resources;

/// Conservative accounting includes simultaneous ZIP indexes, manifest parsing,
/// path allocations, compression state, and verification buffers.
pub struct MemoryBudget {
    maximum: u64,
    used: u64,
}
pub fn verify_service_limit(resources: &Resources) -> anyhow::Result<()> {
    if std::env::var("SYNCTHING_BACKUP_SYSTEMD").as_deref() != Ok("1") {
        return Ok(());
    }
    let membership = std::fs::read_to_string("/proc/self/cgroup")?;
    let group = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or_else(|| {
            anyhow::anyhow!("the supplied unit requires cgroup v2 for memory-limit verification")
        })?;
    let path = std::path::Path::new("/sys/fs/cgroup")
        .join(group.trim_start_matches('/'))
        .join("memory.max");
    let actual = std::fs::read_to_string(path)?;
    let page = rustix::param::page_size() as u64;
    let expected = resources.memory_limit_bytes / page * page;
    anyhow::ensure!(
        actual.trim().parse::<u64>().ok() == Some(expected),
        "systemd MemoryMax differs from memory_limit_bytes; regenerate the unit, run systemctl daemon-reload, then restart"
    );
    Ok(())
}
impl MemoryBudget {
    pub fn new(resources: &Resources) -> Self {
        Self {
            maximum: resources.memory_budget_bytes / resources.max_concurrent_snapshots as u64,
            used: 16 * 1024 * 1024 + resources.io_buffer_bytes as u64 * 4,
        }
    }
    pub fn entry(&mut self, path: &str) -> anyhow::Result<()> {
        let amount = 8192 + path.len() as u64 * 16;
        anyhow::ensure!(self.used.saturating_add(amount) <= self.maximum,
            crate::domain::Permanent("archive metadata exceeds memory budget; increase memory_budget_bytes or split this target".into()));
        self.used += amount;
        Ok(())
    }
    pub fn manifest_limit(&self) -> u64 {
        self.maximum / 4
    }
}
