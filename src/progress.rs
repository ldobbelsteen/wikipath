use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use lazy_static::lazy_static;
use std::io::Read;

const REFRESH_RATE: u8 = 3;

lazy_static! {
    static ref STEP_STYLE: ProgressStyle =
        ProgressStyle::with_template("[{elapsed_precise}] {msg} {spinner}").unwrap();
    static ref FILE_STYLE: ProgressStyle =
        ProgressStyle::with_template("{msg} [{bar}] {bytes} / {total_bytes} ({bytes_per_sec})")
            .unwrap()
            .progress_chars("##-");
}

pub fn multi_progress() -> MultiProgress {
    MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(REFRESH_RATE))
}

pub fn step_progress(msg: String) -> ProgressBar {
    let step = ProgressBar::new_spinner()
        .with_style(STEP_STYLE.clone())
        .with_message(msg);
    step.set_draw_target(ProgressDrawTarget::stderr_with_hz(REFRESH_RATE));
    step.enable_steady_tick(std::time::Duration::from_millis(
        (1000.0 / (REFRESH_RATE as f64)).floor() as u64,
    ));
    step
}

pub fn file_progress(msg: String, current_bytes: u64, total_bytes: u64) -> ProgressBar {
    ProgressBar::with_draw_target(
        Some(total_bytes),
        ProgressDrawTarget::stderr_with_hz(REFRESH_RATE),
    )
    .with_position(current_bytes)
    .with_style(FILE_STYLE.clone())
    .with_message(msg)
}

pub struct ProgressReader<R: Read> {
    inner_reader: R,
    progress: ProgressBar,
}

impl<R: Read> ProgressReader<R> {
    pub fn new(inner_reader: R, progress: ProgressBar) -> Self {
        Self {
            inner_reader,
            progress,
        }
    }
}

impl<R: Read> Read for ProgressReader<R> {
    fn read(&mut self, into: &mut [u8]) -> std::io::Result<usize> {
        let res = self.inner_reader.read(into)?;
        self.progress.inc(res as u64);
        Ok(res)
    }
}