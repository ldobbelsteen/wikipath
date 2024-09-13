use anyhow::{anyhow, Result};
use humantime::format_duration;
use std::{
    cmp::max,
    path::{Path, PathBuf},
    thread,
    time::Instant,
};
use wp::{
    cleanup_redirects, BufferedLinkWriteTransaction, Database, Metadata, TableDumpFiles,
    WriteTransaction,
};

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
    process_memory_limit: u64,
) -> Result<PathBuf> {
    let overall_start = Instant::now();
    log::info!("building '{language_code}' database...");

    log::info!("getting dump information...");
    let external = TableDumpFiles::get_external(language_code, date_code).await?;
    let metadata = Metadata {
        language_code: external.get_language_code(),
        date_code: external.get_date_code(),
    };

    let tmp_path = databases_dir.join(metadata.to_tmp_name());
    if Path::new(&tmp_path).exists() {
        log::warn!("temporary database from previous build found, removing...");
        std::fs::remove_dir_all(&tmp_path)?;
    }

    let final_path = databases_dir.join(metadata.to_name());
    if Path::new(&final_path).exists() {
        log::warn!("database already exists, skipping...");
        return Ok(final_path);
    }

    let files = TableDumpFiles::download_external(dumps_dir, external).await?;

    let build_start = Instant::now();

    {
        let db = Database::open(&tmp_path)?;

        log::info!("parsing page table dump...");
        let title_to_id = files.parse_page_table_dump(thread_count)?;
        if title_to_id.is_empty() {
            return Err(anyhow!(
                "nothing parsed from page table, possibly caused by schema changes"
            ));
        }
        log::info!("{} page titles found!", title_to_id.len());

        log::info!("parsing redirect table dump...");
        let mut redirects = files.parse_redirect_table_dump(&title_to_id, thread_count)?;
        if redirects.is_empty() {
            return Err(anyhow!(
                "nothing parsed from redirect table, possibly caused by schema changes"
            ));
        }
        log::info!("{} unfiltered redirects found!", redirects.len());

        log::info!("cleaning up redirects...");
        cleanup_redirects(&mut redirects);
        log::info!("{} redirects found!", redirects.len());

        log::info!("inserting redirects into database...");
        {
            let mut txn = WriteTransaction::begin(&db)?;
            for (source, target) in &redirects {
                txn.insert_redirect(source, target)?;
            }
            txn.commit()?;
        }

        log::info!("parsing linktarget table dump...");
        let linktarget_to_target = files.parse_linktarget_table_dump(&title_to_id, thread_count)?;
        if linktarget_to_target.is_empty() {
            return Err(anyhow!(
                "nothing parsed from linktarget table, possibly caused by schema changes"
            ));
        }
        log::info!("{} linktargets found!", linktarget_to_target.len());
        drop(title_to_id);

        log::info!("parsing pagelinks table dump...");
        thread::scope(|thread_scope| -> Result<()> {
            let txn = BufferedLinkWriteTransaction::begin(db, process_memory_limit, thread_scope)?;
            files.parse_pagelinks_table_dump(
                &linktarget_to_target,
                &redirects,
                max(thread_count - 1, 1),
                |source, target| {
                    txn.insert_link(source, target);
                },
            )?;

            log::info!("inserting buffered links into database...");
            let link_count = txn.flush_and_commit()?;

            if link_count == 0 {
                return Err(anyhow!(
                    "nothing parsed from pagelinks table, possibly caused by schema changes"
                ));
            }
            log::info!("{link_count} links inserted!");

            Ok(())
        })?;
    }

    std::fs::rename(tmp_path, &final_path)?;
    log::info!(
        "database '{}' succesfully built in {} (or {} without init and download time)!",
        metadata.to_name(),
        format_duration(overall_start.elapsed()),
        format_duration(build_start.elapsed())
    );

    Ok(final_path)
}
