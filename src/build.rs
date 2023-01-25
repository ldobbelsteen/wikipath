use crate::{database, dump};
use error_chain::error_chain;
use indicatif::HumanDuration;
use std::time::Instant;

error_chain! {
    foreign_links {
        Dump(dump::Error);
        Database(database::Error);
    }
}

pub async fn build(lang_code: &str, database_dir: &str, dumps_dir: &str) -> Result<()> {
    let start = Instant::now();
    println!("\nBuilding '{}' database...", lang_code);
    let dump = dump::Dump::download(dumps_dir, lang_code).await?;
    let path = database::Database::build(database_dir, &dump)?;
    println!(
        "Database succesfully built at {} in {}!",
        path.display(),
        HumanDuration(start.elapsed())
    );
    Ok(())
}
