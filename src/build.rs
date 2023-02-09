use crate::{database, dump};
use error_chain::error_chain;
use indicatif::{HumanBytes, HumanDuration};
use std::{path::PathBuf, time::Instant};

error_chain! {
    foreign_links {
        Database(database::Error);
        Dump(dump::Error);
    }
}

pub async fn build(
    language_code: &str,
    databases_dir: &PathBuf,
    dumps_dir: &PathBuf,
    cache_capacity: u64,
    thread_count: usize,
) -> Result<()> {
    let start = Instant::now();
    println!("\nBuilding '{}' database...", language_code);

    let metadata = dump::Dump::latest_metadata(language_code).await?;
    let name = format!("{}-{}", metadata.language_code, metadata.dump_date);
    let dump = dump::Dump::download(dumps_dir, metadata).await?;
    let path = databases_dir.join(&name);
    let database = database::Database::create(&path, &dump, cache_capacity, thread_count)?;

    println!(
        "Database succesfully built '{}' of size {} in {}!",
        name,
        HumanBytes(database.size()),
        HumanDuration(start.elapsed())
    );
    Ok(())
}
