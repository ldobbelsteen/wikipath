#![warn(clippy::pedantic)]

use anyhow::Result;
use clap::{Parser, Subcommand};
use database::Database;
use std::path::{Path, PathBuf};
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

    tokio::select! {
        res = ctrl_c => {
            log::info!("ctrl-c received, exiting");
            Ok(res)
        },
        res = async {
            match args.action {
                Action::Build {
                    languages,
                    date,
                    databases,
                    dumps,
                    threads,
                } => {
                    let databases_dir = Path::new(&databases);
                    let dumps_dir = dumps.map_or(std::env::temp_dir().join("wikipath"), PathBuf::from);
                    let thread_count = threads.unwrap_or_else(num_cpus::get);

                    for language_code in languages.split(',') {
                        Database::build(
                            language_code,
                            &date,
                            databases_dir,
                            &dumps_dir,
                            thread_count
                        )
                        .await?;
                    }

                    Ok(())
                }
                Action::Serve { databases, port } => {
                    let databases_dir = Path::new(&databases);
                    serve::serve(databases_dir, port).await
                }
            }
        } => res,
    }
}
