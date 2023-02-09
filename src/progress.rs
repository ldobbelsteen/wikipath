use indicatif::{ProgressBar, ProgressStyle};
use lazy_static::lazy_static;
use std::{io::Read, time::Duration};

const REFRESH_INTERVAL_MS: u64 = 500;

lazy_static! {
    static ref SPINNER_STYLE: ProgressStyle =
        ProgressStyle::with_template("[{elapsed_precise}] {msg} {spinner}").unwrap();
    static ref BYTE_STYLE: ProgressStyle =
        ProgressStyle::with_template(" ➔  [{bar}] {percent}% ({bytes_per_sec})")
            .unwrap()
            .progress_chars("##-");
    static ref UNIT_STYLE: ProgressStyle =
        ProgressStyle::with_template(" ➔  [{bar}] {percent}% ({per_sec})")
            .unwrap()
            .progress_chars("##-");
}

pub fn spinner(msg: &str) -> ProgressBar {
    let result = ProgressBar::new_spinner()
        .with_style(SPINNER_STYLE.clone())
        .with_message(msg.to_string());
    result.enable_steady_tick(Duration::from_millis(REFRESH_INTERVAL_MS));
    result
}

pub fn byte(msg: &str, current_bytes: u64, total_bytes: u64) -> ProgressBar {
    let result = ProgressBar::new(total_bytes)
        .with_position(current_bytes)
        .with_style(BYTE_STYLE.clone())
        .with_message(msg.to_string());
    result.enable_steady_tick(Duration::from_millis(REFRESH_INTERVAL_MS));
    result
}

pub fn unit(total: u64) -> ProgressBar {
    let result = ProgressBar::new(total).with_style(UNIT_STYLE.clone());
    result.enable_steady_tick(Duration::from_millis(REFRESH_INTERVAL_MS));
    result
}

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
