use anyhow::{anyhow, Result};
use humantime::format_duration;
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Instant,
};
use wp::{cleanup_redirects, Database, Metadata, Mode, TableDumpFiles};

/// Build a database in a certain language. Outputs the database into the specified directory. Dump
/// files are downloaded into the specified directory to prevent re-downloading when re-building a
/// database. Uses the specified number of threads in total.
#[allow(clippy::too_many_lines)]
pub async fn build(
    language_code: &str,
    date_code: &str,
    databases_dir: &Path,
    dumps_dir: &Path,
    thread_count: usize,
) -> Result<PathBuf> {
    log::info!("building '{language_code}' database");
    let overall_start = Instant::now();

    log::info!("getting dump information");
    let external = TableDumpFiles::get_external(language_code, date_code).await?;
    let metadata = Metadata {
        language_code: external.get_language_code(),
        date_code: external.get_date_code(),
    };

    let tmp_path = databases_dir.join("build").join(metadata.to_name());
    if Path::new(&tmp_path).exists() {
        log::warn!("temporary database from previous build found, removing");
        std::fs::remove_dir_all(&tmp_path)?;
    }

    let final_path = databases_dir.join(metadata.to_name());
    if Path::new(&final_path).exists() {
        log::warn!("database already exists, skipping");
        return Ok(final_path);
    }

    let files = TableDumpFiles::download_external(dumps_dir, external).await?;

    log::info!("creating new database");
    std::fs::create_dir_all(&tmp_path)?;
    let db = Database::open(&tmp_path, Mode::Build)?;
    let build_start = Instant::now();

    {
        log::info!("parsing page table dump");
        let title_to_id = files.parse_page_table_dump(thread_count)?;
        if title_to_id.is_empty() {
            return Err(anyhow!(
                "nothing parsed from page table, possibly caused by schema changes"
            ));
        }
        log::info!("{} page titles found!", title_to_id.len());

        log::info!("parsing redirect table dump");
        let redirects = files.parse_redirect_table_dump(&title_to_id, thread_count)?;
        if redirects.is_empty() {
            return Err(anyhow!(
                "nothing parsed from redirect table, possibly caused by schema changes"
            ));
        }
        log::info!("{} redirects found!", redirects.len());

        log::info!("cleaning up redirects");
        let redirects = cleanup_redirects(redirects);
        log::info!("{} clean redirects found!", redirects.len());

        log::info!("inserting redirects into database");
        let mut txn = db.write_txn()?;
        for (source, target) in &redirects {
            db.insert_redirect(&mut txn, *source, *target)?;
        }
        txn.commit()?;

        log::info!("parsing linktarget table dump");
        let linktarget_to_target = files.parse_linktarget_table_dump(&title_to_id, thread_count)?;
        if linktarget_to_target.is_empty() {
            return Err(anyhow!(
                "nothing parsed from linktarget table, possibly caused by schema changes"
            ));
        }
        log::info!("{} linktargets found!", linktarget_to_target.len());

        drop(title_to_id); // not needed anymore

        log::info!("parsing pagelinks table dump & inserting links into database");
        let link_count = Arc::new(Mutex::new(0));
        files.parse_pagelinks_table_dump(
            |batch| {
                let mut txn = db.write_txn()?;
                let size = batch.size();

                log::debug!("inserting links from batch of size {}", size);
                let mut total_insert_count = 0;
                let mut append_insert_count = 0;
                for (target, sources) in batch.drain() {
                    let append = db.insert_links_incoming(&mut txn, target, sources)?;
                    if append {
                        append_insert_count += 1;
                    }
                    total_insert_count += 1;
                }

                let ratio = f64::from(append_insert_count) / f64::from(total_insert_count);
                log::debug!("{:.2}% of links were appended", ratio * 100.0);

                log::debug!("committing links insertion");
                txn.commit()?;

                *link_count.lock().unwrap() += size;
                Ok(())
            },
            &linktarget_to_target,
            &redirects,
            thread_count,
        )?;
        let link_count = *link_count.lock().unwrap();
        if link_count == 0 {
            return Err(anyhow!(
                "nothing parsed from pagelinks table, possibly caused by schema changes"
            ));
        }
        log::info!("{} links found!", link_count);
    }

    log::info!("generating outgoing table");
    let mut txn = db.write_txn()?;
    db.generate_outgoing_table(&mut txn)?;
    txn.commit()?;

    log::info!("copying database to final path");
    db.copy_to_serve(&final_path)?;

    log::info!(
        "database '{}' succesfully built in {} (or {} without init and download time)!",
        metadata.to_name(),
        format_duration(overall_start.elapsed()),
        format_duration(build_start.elapsed())
    );

    Ok(final_path)
}
