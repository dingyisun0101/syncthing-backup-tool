use crate::{api::*, config::BackendChoices};
use anyhow::{Result, ensure};
use std::sync::Arc;

#[derive(Clone)]
pub struct Modules {
    pub scripts: Arc<dyn ScriptRunner>,
    pub source: Arc<dyn SourceProvider>,
    pub synchronizer: Arc<dyn Synchronizer>,
    pub archiver: Arc<dyn Archiver>,
    pub storage: Arc<dyn StorageProvider>,
    pub scheduler: Arc<dyn SchedulingPolicy>,
    pub queue: Arc<dyn QueuePolicy>,
    pub retention: Arc<dyn RetentionPolicy>,
}
impl Modules {
    pub fn from_choices(choices: &BackendChoices) -> Result<Self> {
        ensure!(
            choices.scripts == "local_process"
                && choices.source == "live_directory"
                && choices.sync == "rsync"
                && choices.archive == "infozip"
                && choices.storage == "local"
                && choices.state == "sqlite"
                && choices.scheduler == "interval"
                && choices.queue == "bounded_fifo"
                && choices.retention == "oldest_first",
            "unsupported backend selection"
        );
        Ok(Self {
            scripts: Arc::new(crate::hooks::LocalProcess),
            source: Arc::new(crate::source::LiveDirectory),
            synchronizer: Arc::new(crate::source::Rsync),
            archiver: Arc::new(crate::archive::InfoZip),
            storage: Arc::new(crate::storage::LocalStorage),
            scheduler: Arc::new(crate::scheduler::Interval),
            queue: Arc::new(crate::queue::BoundedFifo),
            retention: Arc::new(crate::retention::OldestFirst),
        })
    }
}
