use anyhow::{anyhow, Result};
use sysinfo::{Pid, System};

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
        self.system.refresh_process(self.pid);
        self.system.process(self.pid).unwrap().memory()
    }
}
