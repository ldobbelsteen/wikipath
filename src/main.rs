use clap::{Parser, Subcommand};
use std::path::Path;

mod build;
mod database;
mod dump;
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
        #[clap(short, long)]
        languages: String,
        /// Directory to output database(s) to.
        #[clap(short, long, default_value = "./databases")]
        output: String,
        /// Number of threads to use while parsing. Uses all by default.
        #[clap(short, long)]
        threads: Option<usize>,
    },
    /// Serve Wikipath database(s).
    Serve {
        /// Directory of databases.
        #[clap(short, long, default_value = "./databases")]
        databases_dir: String,
        /// Port on which to serve web interface.
        #[clap(short, long, default_value_t = 1789)]
        listening_port: u16,
    },
}

#[tokio::main]
async fn main() {
    let args = Arguments::parse();
    match args.action {
        Action::Build {
            languages,
            output,
            threads,
        } => {
            let databases_dir = Path::new(&output);
            let thread_count = threads.unwrap_or_else(num_cpus::get);
            for language_code in languages.split(',') {
                if let Err(e) = build::build(language_code, databases_dir, thread_count).await {
                    eprintln!("[FATAL] {}", e);
                    std::process::exit(1);
                }
            }
        }
        Action::Serve {
            databases_dir,
            listening_port,
        } => {
            let databases_dir = Path::new(&databases_dir);
            if let Err(e) = serve::serve(databases_dir, listening_port).await {
                eprintln!("[FATAL] {}", e);
                std::process::exit(1);
            }
        }
    }
}
