use anyhow::{anyhow, Result};
use humantime::format_duration;
use log::{info, warn};
use std::{
    cmp::max,
    path::{Path, PathBuf},
    thread,
    time::Instant,
};
use wp::{cleanup_redirects, BufferedLinkInserter, Database, Metadata, TableDumpFiles};

/// Build a database in a certain language. Outputs the database into the specified directory. Dump
/// files are downloaded into the specified directory to prevent re-downloading when re-building a
/// database. Uses the specified number of threads and uses the specified number of bytes as a
/// ceiling for memory usage.
pub async fn build(
    language_code: &str,
    date_code: &str,
    databases_dir: &Path,
    dumps_dir: &Path,
    thread_count: usize,
    memory_limit: u64,
) -> Result<PathBuf> {
    let start = Instant::now();
    info!("building '{language_code}' database...");

    info!("getting dump information...");
    let external = TableDumpFiles::get_external(language_code, date_code).await?;
    let metadata = Metadata {
        language_code: external.get_language_code(),
        date_code: external.get_date_code(),
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

    {
        let mut db = Database::open(&tmp_path)?;
        let txn = db.begin_write()?;

        {
            let mut build = txn.begin_build()?;
            let files = TableDumpFiles::download_external(dumps_dir, external).await?;

            info!("parsing page table dump...");
            let title_to_id = files.parse_page_table_dump(thread_count)?;
            if title_to_id.is_empty() {
                return Err(anyhow!(
                    "nothing parsed from page table, possibly caused by schema changes"
                ));
            }
            info!("{} page titles found!", title_to_id.len());

            info!("parsing redirect table dump...");
            let mut redirects = files.parse_redirect_table_dump(&title_to_id, thread_count)?;
            if redirects.is_empty() {
                return Err(anyhow!(
                    "nothing parsed from redirect table, possibly caused by schema changes"
                ));
            }
            info!("{} unfiltered redirects found!", redirects.len());

            info!("cleaning up redirects...");
            cleanup_redirects(&mut redirects);
            info!("{} redirects found!", redirects.len());

            info!("inserting redirects into database...");
            build.insert_redirects(&redirects)?;

            info!("parsing linktarget table dump...");
            let linktarget_to_target =
                files.parse_linktarget_table_dump(&title_to_id, thread_count)?;
            if linktarget_to_target.is_empty() {
                return Err(anyhow!(
                    "nothing parsed from linktarget table, possibly caused by schema changes"
                ));
            }
            info!("{} linktargets found!", linktarget_to_target.len());
            drop(title_to_id);

            info!("parsing pagelinks table dump & inserting links into database...");
            thread::scope(|scope| -> Result<()> {
                let buffer = BufferedLinkInserter::for_txn(&mut build, memory_limit, scope)?;
                files.parse_pagelinks_table_dump(
                    &linktarget_to_target,
                    &redirects,
                    max(thread_count - 1, 1),
                    |source, target| {
                        buffer.insert(source, target);
                    },
                )?;
                info!("inserting remaining buffered links into database...");
                let link_count = buffer.flush()?;
                if link_count == 0 {
                    return Err(anyhow!(
                        "nothing parsed from pagelinks table, possibly caused by schema changes"
                    ));
                }
                info!("{link_count} links inserted!");
                Ok(())
            })?;
        }

        info!("comitting transaction...");
        txn.commit()?;

        info!("compacting database...");
        db.compact()?;
    }

    std::fs::rename(tmp_path, &final_path)?;
    info!(
        "database '{}' succesfully built in {}!",
        metadata.to_name(),
        format_duration(start.elapsed())
    );

    Ok(final_path)
}
