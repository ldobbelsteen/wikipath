use crate::database::{Database, Metadata};
use anyhow::Result;
use std::{fs, path::Path};

/// Remove databases with different date but the same language code as the given anchor.
/// This function will remove all databases in the given directory that have the same language code
/// as the anchor but a different date code. This is useful when building a new database, as the
/// old databases with the same language code but different date code are no longer needed.
pub fn remove_different_date_databases(anchor: &Metadata, dir: &Path) -> Result<()> {
    log::debug!(
        "removing databases with different date in '{}'",
        dir.display()
    );

    for entry in fs::read_dir(dir)? {
        let path = entry?.path();

        match Database::get_metadata(&path) {
            Ok(md) => {
                if md.language_code == anchor.language_code && md.date_code != anchor.date_code {
                    if path.is_dir() {
                        fs::remove_dir_all(&path)?;
                    } else {
                        fs::remove_file(&path)?;
                    }
                    log::info!("removed database with different date '{}'", path.display());
                }
            }
            Err(e) => {
                log::debug!("skipping non-database path '{}': {}", path.display(), e);
            }
        }
    }

    Ok(())
}
