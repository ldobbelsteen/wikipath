#![warn(clippy::pedantic)]

use clap::{Parser, Subcommand};
use std::path::{Path, PathBuf};

mod build;
mod database;
mod dump;
mod memory;
mod parse;
mod progress;
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
        /// Language(s) to build, separated by commas. Uses ISO codes from https://en.wikipedia.org/wiki/List_of_Wikipedias.
        #[clap(long)]
        languages: String,
        /// Directory to output database(s) to.
        #[clap(long, default_value = "./databases")]
        databases: String,
        /// Directory to download the dump files to. Uses the temporary directory by default.
        #[clap(long)]
        dumps: Option<String>,
        /// Number of threads to use while parsing. Uses all by default.
        #[clap(long)]
        threads: Option<usize>,
        /// Maximum number of gigabytes (GB) of memory that can be used for caching (not a hard limit).
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
async fn main() {
    let args = Arguments::parse();
    match args.action {
        Action::Build {
            languages,
            databases,
            dumps,
            threads,
            memory,
        } => {
            let databases_dir = Path::new(&databases);
            let dumps_dir = dumps.map_or(std::env::temp_dir().join("wikipath"), PathBuf::from);
            let thread_count = threads.unwrap_or_else(num_cpus::get);
            let max_memory_usage = memory * 1024 * 1024 * 1024;
            for language_code in languages.split(',') {
                if let Err(e) = build::build(
                    language_code,
                    databases_dir,
                    &dumps_dir,
                    thread_count,
                    max_memory_usage,
                )
                .await
                {
                    eprintln!("[FATAL] {e}");
                    std::process::exit(1);
                }
            }
        }
        Action::Serve { databases, port } => {
            let databases_dir = Path::new(&databases);
            if let Err(e) = serve::serve(databases_dir, port).await {
                eprintln!("[FATAL] {e}");
                std::process::exit(1);
            }
        }
    }
}
