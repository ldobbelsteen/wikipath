use crate::{database, dump};
use error_chain::error_chain;
use indicatif::HumanDuration;
use std::{path::PathBuf, time::Instant};

error_chain! {
    foreign_links {
        Builder(database::BuilderError);
        Download(dump::DownloadError);
    }
}

pub async fn build(
    language_code: &str,
    databases_dir: &PathBuf,
    dumps_dir: &PathBuf,
) -> Result<()> {
    let start = Instant::now();
    println!("\nBuilding '{}' database...", language_code);

    let metadata = dump::Dump::latest_metadata(language_code).await?;
    let name = databases_dir.join(format!("{}-{}", metadata.language_code, metadata.dump_date));

    let mut builder = database::Builder::new(&name)?;
    let dump = dump::Dump::download(dumps_dir, metadata).await?;
    builder.build(&dump)?;

    println!(
        "Database succesfully built in {}!",
        HumanDuration(start.elapsed())
    );
    Ok(())
}
