use crate::{
    database::{Database, Metadata},
    dump::{self, Dump},
    progress,
};
use anyhow::Result;
use indicatif::{HumanDuration, MultiProgress};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

/// Build a database in a certain language. Outputs the database into the
/// specified directory. Dump files are temporarily downloaded to the system's
/// temporary directory.
pub async fn build(
    language_code: &str,
    databases_dir: &Path,
    dumps_dir: &Path,
    thread_count: usize,
    max_memory_usage: u64,
) -> Result<PathBuf> {
    let start = Instant::now();
    println!("\n[INFO] Building '{language_code}' database...");
    let progress = MultiProgress::new();

    // Get the info of the latest Wikipedia database dump.
    let latest = dump::Dump::get_latest_external(language_code).await?;
    let metadata = Metadata {
        language_code: latest.get_language_code(),
        dump_date: latest.get_dump_date(),
    };

    let tmp_path = databases_dir.join(metadata.to_tmp_name());
    if Path::new(&tmp_path).exists() {
        println!("[WARNING] Temporary database from previous build found, removing...");
        std::fs::remove_file(&tmp_path)?;
    }

    let final_path = databases_dir.join(metadata.to_name());
    if Path::new(&final_path).exists() {
        println!("[WARNING] Database already exists, skipping...");
        return Ok(final_path);
    }

    // Download the relevant dump files to a temporary directory.
    let dump = Dump::download_external(dumps_dir, latest, progress.clone()).await?;

    // Create a new database and prepare for ingestion.
    let mut database = Database::open(&tmp_path)?;
    let transaction = database.begin_write()?;
    let build = Arc::new(transaction.open_build(max_memory_usage)?);

    // Parse the page dump, extracting all pages' IDs and titles.
    let step = progress.add(progress::spinner("Parsing page dump"));
    dump.parse_page(build.clone(), &progress, thread_count)?;
    step.finish();

    // Parse the redirect dump, extracting all redirects from page to page.
    let step = progress.add(progress::spinner("Parsing redirects dump"));
    dump.parse_redir(build.clone(), &progress, thread_count)?;
    step.finish();

    // Parse the pagelink dump, extracting all links from page to page.
    let step = progress.add(progress::spinner("Parsing links dump"));
    dump.parse_link(build.clone(), &progress, thread_count)?;
    step.finish();

    // Flush any cached data to disk.
    let step = progress.add(progress::spinner("Flushing to disk"));
    let build = Arc::try_unwrap(build).unwrap_or_else(|_| panic!("could not unwrap build arc"));
    build.flush()?;
    transaction.commit()?;
    step.finish();

    // Compact the on-disk database.
    let step = progress.add(progress::spinner("Compacting database"));
    database.compact()?;
    step.finish();

    // Move file from temporary path to permanent.
    drop(database);
    std::fs::rename(tmp_path, &final_path)?;
    drop(progress);
    println!(
        "[INFO] Database '{}' succesfully built in {}!",
        metadata.to_name(),
        HumanDuration(start.elapsed())
    );

    Ok(final_path)
}
