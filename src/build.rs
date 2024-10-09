use crate::{
    database::{Database, Metadata, Mode},
    dump::TableDumpFiles,
    parse::cleanup_redirects,
};
use anyhow::{anyhow, Result};
use humantime::format_duration;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};

impl Database {
    /// Build a database in a certain language. Requires the database metadata and the downloaded
    /// dump files. The database will be built in the specified temporary path and then copied
    /// to the final path. Note that the temporary path should point to a directory that does not
    /// yet exist, and the final path fo a file that does not exist. Uses the specified number
    /// of threads in total.
    pub fn build(
        metadata: &Metadata,
        dump_files: &TableDumpFiles,
        tmp_path: &Path,
        final_path: &Path,
        thread_count: usize,
    ) -> Result<()> {
        let start = Instant::now();

        if tmp_path.exists() {
            return Err(anyhow!(
                "temporary database path '{}' already exists",
                tmp_path.display()
            ));
        }

        log::info!("creating new database");
        std::fs::create_dir_all(tmp_path)?;

        let db = Database::open(tmp_path, Mode::Build)?;

        {
            log::info!("parsing page table dump");
            let title_to_id = dump_files.parse_page_table(thread_count)?;
            if title_to_id.is_empty() {
                return Err(anyhow!(
                    "nothing parsed from page table, possibly caused by schema changes"
                ));
            }
            log::info!("{} page titles found!", title_to_id.len());

            log::info!("parsing redirect table dump");
            let redirects = dump_files.parse_redirect_table(&title_to_id, thread_count)?;
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
            let linktarget_to_target =
                dump_files.parse_linktarget_table(&title_to_id, thread_count)?;
            if linktarget_to_target.is_empty() {
                return Err(anyhow!(
                    "nothing parsed from linktarget table, possibly caused by schema changes"
                ));
            }
            log::info!("{} linktargets found!", linktarget_to_target.len());

            drop(title_to_id); // not needed anymore

            log::info!("parsing pagelinks table dump & inserting links into database");
            let link_count = Arc::new(Mutex::new(0));
            dump_files.parse_pagelinks_table(
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
        db.copy_to_serve(final_path)?;

        log::info!(
            "database '{}' succesfully built in {}!",
            metadata.to_name(),
            format_duration(start.elapsed())
        );

        Ok(())
    }
}
