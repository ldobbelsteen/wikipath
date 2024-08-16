#![warn(clippy::pedantic)]

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

mod build;
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
        /// Maximum number of gigabytes (GB) of memory that can be used (higher values prevent the buffer having to be flushed prematurely and in turn improve performance).
        #[clap(long, default_value = "12")]
        memory: u64,
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
    match args.action {
        Action::Build {
            languages,
            date,
            databases,
            dumps,
            threads,
            memory,
        } => {
            let databases_dir = Path::new(&databases);
            std::fs::create_dir_all(databases_dir)?;

            let dumps_dir = dumps.map_or(std::env::temp_dir().join("wikipath"), PathBuf::from);
            std::fs::create_dir_all(&dumps_dir)?;

            let thread_count = threads.unwrap_or_else(num_cpus::get);
            let memory_limit = memory * 1024 * 1024 * 1024;
            for language_code in languages.split(',') {
                build::build(
                    language_code,
                    &date,
                    databases_dir,
                    &dumps_dir,
                    thread_count,
                    memory_limit,
                )
                .await?;
            }
        }
        Action::Serve { databases, port } => {
            let databases_dir = Path::new(&databases);
            serve::serve(databases_dir, port).await?;
        }
    }

    Ok(())
}
