use anyhow::{anyhow, Result};
use sysinfo::{Pid, ProcessesToUpdate, System};

/// Struct that allows checking the memory usage of the current process.
#[derive(Debug)]
pub struct ProcessMemoryUsageChecker {
    system: System,
    pid: Pid,
}

impl ProcessMemoryUsageChecker {
    pub fn new() -> Result<Self> {
        let system = System::new();
        let pid = sysinfo::get_current_pid().map_err(|e| anyhow!(e))?;
        Ok(Self { system, pid })
    }

    pub fn get(&mut self) -> u64 {
        self.system
            .refresh_processes(ProcessesToUpdate::Some(&[self.pid]));
        self.system.process(self.pid).unwrap().memory()
    }
}
