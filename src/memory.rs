use anyhow::{anyhow, Result};
use std::{
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};
use sysinfo::{ProcessExt, System, SystemExt};

#[derive(Clone)]
pub struct MemoryUsage {
    usage: Arc<RwLock<u64>>,
}

impl MemoryUsage {
    pub fn new(update_interval_secs: u64) -> Result<Self> {
        let mut system = System::new();
        let pid = sysinfo::get_current_pid().map_err(|e| anyhow!(e))?;
        let usage = Arc::new(RwLock::new(0));
        let interval = Duration::from_secs(update_interval_secs);
        let usage_clone = usage.clone();

        thread::spawn(move || loop {
            system.refresh_process(pid);
            let new_usage = system.process(pid).unwrap().memory();
            *usage_clone.write().unwrap() = new_usage;
            thread::sleep(interval);
        });

        Ok(MemoryUsage { usage })
    }

    pub fn get(&self) -> u64 {
        *self.usage.read().unwrap()
    }
}
