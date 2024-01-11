use crate::{
    database::{BufferedLinkInserter, Database, Metadata},
    dump::{self, Dump},
    parse::cleanup_redirects,
};
use anyhow::Result;
use humantime::format_duration;
use log::{info, warn};
use std::{
    path::{Path, PathBuf},
    thread,
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
    memory_limit: u64,
) -> Result<PathBuf> {
    let start = Instant::now();
    info!("building '{language_code}' database...");

    info!("getting latest dump information...");
    let latest = dump::Dump::get_latest_external(language_code).await?;
    let metadata = Metadata {
        language_code: latest.get_language_code(),
        dump_date: latest.get_dump_date(),
    };

    let tmp_path = databases_dir.join(metadata.to_tmp_name());
    if Path::new(&tmp_path).exists() {
        warn!("temporary database from previous build found, removing...");
        std::fs::remove_file(&tmp_path)?;
    }

    let final_path = databases_dir.join(metadata.to_name());
    if Path::new(&final_path).exists() {
        warn!("database already exists, skipping...");
        return Ok(final_path);
    }

    let mut database = Database::open(&tmp_path)?;
    let txn = database.begin_write()?;
    let mut build = txn.begin_build()?;

    let dump = Dump::download_external(dumps_dir, latest).await?;

    info!("parsing page dump...");
    let pages = dump.parse_page_dump(thread_count)?;
    info!("{} unique pages found!", pages.len());

    info!("parsing redirects dump...");
    let mut redirs = dump.parse_redir_dump(&pages, thread_count)?;
    info!("{} raw redirects found!", redirs.len());

    info!("cleaning up redirects...");
    cleanup_redirects(&mut redirs);
    info!("{} clean redirects found!", redirs.len());

    info!("inserting redirects into database...");
    build.insert_redirects(&redirs)?;

    info!("parsing links dump & inserting into database...");
    thread::scope(|scope| -> Result<()> {
        let buffer = BufferedLinkInserter::for_txn(&mut build, memory_limit, scope)?;
        dump.parse_link_dump(&pages, &redirs, thread_count, |source, target| {
            buffer.insert(source, target);
        })?;
        info!("inserting remaining buffered links into database...");
        let link_count = buffer.flush()?;
        info!("{link_count} links found!");
        Ok(())
    })?;

    info!("comitting transaction...");
    drop(build);
    txn.commit()?;

    info!("compacting database...");
    database.compact()?;

    drop(database);
    std::fs::rename(tmp_path, &final_path)?;
    info!(
        "database '{}' succesfully built in {}!",
        metadata.to_name(),
        format_duration(start.elapsed())
    );

    Ok(final_path)
}
