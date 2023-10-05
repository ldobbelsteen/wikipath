use indicatif::{ProgressBar, ProgressStyle};
use std::{io::Read, time::Duration};

const REFRESH_INTERVAL_MS: u64 = 500;

pub fn spinner(msg: &str) -> ProgressBar {
    let result = ProgressBar::new_spinner()
        .with_style(ProgressStyle::with_template("[{elapsed_precise}] {msg} {spinner}").unwrap())
        .with_message(msg.to_string());
    result.enable_steady_tick(Duration::from_millis(REFRESH_INTERVAL_MS));
    result
}

pub fn byte(msg: &str, current_bytes: u64, total_bytes: u64) -> ProgressBar {
    let result = ProgressBar::new(total_bytes)
        .with_position(current_bytes)
        .with_style(
            ProgressStyle::with_template(" ➔  [{bar}] {percent}% ({bytes_per_sec})")
                .unwrap()
                .progress_chars("##-"),
        )
        .with_message(msg.to_string());
    result.enable_steady_tick(Duration::from_millis(REFRESH_INTERVAL_MS));
    result
}

/// Proxy of a reader that acts as a way to keep track of the number of bytes
/// already read in a progress bar.
#[derive(Debug)]
pub struct Reader<R: Read> {
    inner_reader: R,
    progress: ProgressBar,
}

impl<R: Read> Reader<R> {
    pub fn new(inner_reader: R, progress: ProgressBar) -> Self {
        Self {
            inner_reader,
            progress,
        }
    }
}

impl<R: Read> Read for Reader<R> {
    fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        let res = self.inner_reader.read(into)?;
        self.progress.inc(res as u64);
        Ok(res)
    }
}
