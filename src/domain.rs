use crate::config::{Resources, Target};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug)]
pub struct Permanent(pub String);
impl fmt::Display for Permanent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}
impl std::error::Error for Permanent {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JobSpec {
    #[serde(default)]
    pub backends: crate::config::BackendChoices,
    pub target: Target,
    pub resources: Resources,
}

#[derive(Clone, Debug)]
pub struct Job {
    pub id: String,
    pub spec: JobSpec,
    pub attempts: u32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Fingerprint {
    pub device: u64,
    pub inode: u64,
    pub size: u64,
    pub mtime_seconds: i64,
    pub mtime_nanoseconds: i64,
    pub ctime_seconds: i64,
    pub ctime_nanoseconds: i64,
    pub mode: u32,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Entry {
    pub path: String,
    pub kind: String,
    pub metadata: Fingerprint,
    pub sha256: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    pub job_id: String,
    pub target: Target,
    pub capture_start: String,
    pub capture_finish: String,
    pub consistency: String,
    pub entries: Vec<Entry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Snapshot {
    #[serde(default)]
    pub backends: crate::config::BackendChoices,
    pub job_id: String,
    pub target_id: String,
    pub policy_id: String,
    pub capture_ms: i64,
    pub filename: String,
    pub bytes: u64,
    pub sha256: String,
    pub target: Target,
}

impl JobSpec {
    pub fn policy_id(&self) -> anyhow::Result<String> {
        use sha2::{Digest, Sha256};
        Ok(hex::encode(Sha256::digest(serde_json::to_vec(&(
            self.target.policy_id()?,
            &self.backends,
        ))?)))
    }
}
