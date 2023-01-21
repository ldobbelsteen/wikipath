use std::time::Instant;

use indicatif::HumanDuration;

use crate::{database::Database, dump::Dump};

pub async fn build(lang_code: &str, database_dir: &str, dumps_dir: &str) {
    let start = Instant::now();
    let dump = Dump::download(dumps_dir, lang_code)
        .await
        .unwrap_or_else(|e| {
            eprintln!("FATAL: {}", e);
            std::process::exit(1);
        });
    let path = Database::build(database_dir, &dump).unwrap_or_else(|e| {
        eprintln!("FATAL: {}", e);
        std::process::exit(1);
    });
    println!(
        "Database succesfully built at {} in {}!",
        path.display(),
        HumanDuration(start.elapsed())
    );
}
