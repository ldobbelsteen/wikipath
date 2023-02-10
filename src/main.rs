use clap::{Parser, Subcommand};
use std::path::PathBuf;
use sysinfo::{System, SystemExt};

mod build;
mod database;
mod dump;
mod parse;
mod progress;
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
        language: String,
        /// Directory to output database(s) to.
        #[clap(long, default_value = "./databases")]
        databases: String,
        /// Directory to download dump files to.
        #[clap(long, default_value = "./dumps")]
        dumps: String,
        /// Page cache size in gigabytes (GB). Uses half of system memory by default.
        #[clap(long)]
        cache: Option<u64>,
        /// Number of threads to use while parsing. Uses all by default.
        #[clap(long)]
        threads: Option<usize>,
    },
    /// Serve Wikipath database(s).
    Serve {
        /// Directory of databases.
        #[clap(long, default_value = "./databases")]
        databases: String,
        /// Port on which to serve web interface.
        #[clap(long, default_value_t = 1789)]
        port: u16,
        /// Per-database page cache size in gigabytes (GB).
        #[clap(long, default_value_t = 1)]
        cache: u64,
    },
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    match args.action {
        Action::Build {
            language,
            databases,
            dumps,
            cache,
            threads,
        } => {
            let databases_dir = PathBuf::from(databases);
            let dumps_dir = PathBuf::from(dumps);
            let cache_capacity = cache.map_or_else(
                || {
                    let mut sys = System::new();
                    sys.refresh_memory();
                    sys.total_memory() / 2
                },
                |cache| cache * 1024 * 1024 * 1024,
            );
            let thread_count = threads.unwrap_or_else(num_cpus::get);
            for language in language.split(',') {
                if let Err(e) = build::build(
                    language,
                    &databases_dir,
                    &dumps_dir,
                    cache_capacity,
                    thread_count,
                )
                .await
                {
                    eprintln!("[FATAL] {}", e);
                    std::process::exit(1);
                }
            }
        }
        Action::Serve {
            databases,
            port,
            cache,
        } => {
            let databases_dir = PathBuf::from(databases);
            let cache_capacity = cache * 1024 * 1024 * 1024;
            if let Err(e) = serve::serve(&databases_dir, port, cache_capacity).await {
                eprintln!("[FATAL] {}", e);
                std::process::exit(1);
            }
        }
    }
}
