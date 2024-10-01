#![warn(clippy::pedantic)]

use anyhow::Result;
use clap::{Parser, Subcommand};
use database::Database;
use dump::TableDumpFiles;
use humantime::format_duration;
use std::{
    path::{Path, PathBuf},
    time::Instant,
};
use tokio::signal;

mod build;
mod database;
mod dump;
mod parse;
mod search;
mod serve;

#[derive(Parser)]
struct Arguments {
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Build Wikipath database(s).
    Build {
        /// Language(s) to build, separated by commas. Use ISO codes from <https://en.wikipedia.org/wiki/List_of_Wikipedias>.
        #[clap(long, default_value = "en")]
        languages: String,
        /// Date of the dump to build the database from. Use the dates from e.g. <https://dumps.wikimedia.org/enwiki>.
        #[clap(long, default_value = "latest")]
        date: String,
        /// Directory to output database(s) to.
        #[clap(long, default_value = "./databases")]
        databases: String,
        /// Directory to download the dump files to. Uses the temporary directory by default.
        #[clap(long)]
        dumps: Option<String>,
        /// Number of threads to use while parsing. Uses all by default.
        #[clap(long)]
        threads: Option<usize>,
    },
    /// Serve Wikipath database(s).
    Serve {
        /// Directory of databases.
        #[clap(short, default_value = "./databases")]
        databases: String,
        /// Port on which to serve the web interface and api.
        #[clap(short, default_value_t = 1789)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }

    env_logger::builder().format_target(false).try_init()?;

    let args = Arguments::parse();

    let ctrl_c = async {
        signal::ctrl_c().await.expect("failed to listen for ctrl-c");
    };

    match args.action {
        Action::Serve { databases, port } => {
            let databases_dir = Path::new(&databases);
            tokio::select! {
                res = serve::serve(databases_dir, port) => res,
                () = ctrl_c => {
                    log::info!("ctrl-c received, exiting");
                    Ok(())
                },
            }
        }
        Action::Build {
            languages,
            date,
            databases,
            dumps,
            threads,
        } => {
            let date_code = date;
            let databases_dir = Path::new(&databases);
            let dumps_dir = dumps.map_or(std::env::temp_dir().join("wikipath"), PathBuf::from);
            let thread_count = threads.unwrap_or_else(num_cpus::get);

            for language_code in languages.split(',') {
                log::info!("building '{}' database", language_code);

                log::info!("getting dump information");
                let external_dump_files =
                    TableDumpFiles::get_external(language_code, &date_code).await?;

                let metadata = external_dump_files.get_metadata();

                let tmp_path = databases_dir.join("build").join(metadata.to_name());
                if Path::new(&tmp_path).exists() {
                    log::warn!("temporary database from previous build found, removing");
                    std::fs::remove_dir_all(&tmp_path)?;
                }

                let final_path = databases_dir.join(metadata.to_name());
                if Path::new(&final_path).exists() {
                    log::warn!("database already exists, skipping");
                    continue;
                }

                let start = Instant::now();
                let dump_files =
                    TableDumpFiles::download_external(&dumps_dir, external_dump_files).await?;
                log::info!(
                    "dump files downloaded in {}!",
                    format_duration(start.elapsed())
                );

                Database::build(&metadata, &dump_files, &tmp_path, &final_path, thread_count)?;
            }

            Ok(())
        }
    }
}
