use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod build;
mod database;
mod dump;
mod progress;
mod serve;

#[derive(Parser)]
struct Arguments {
    #[command(subcommand)]
    action: Action,
}

#[derive(Subcommand)]
enum Action {
    /// Build Wikipath database(s)
    Build {
        /// Language(s) to build, separated by commas
        #[clap(long)]
        language: String,
        /// Directory to output database(s) to
        #[clap(long, default_value = "./databases")]
        databases: String,
        /// Directory to download dump files to
        #[clap(long, default_value = "./dumps")]
        dumps: String,
    },
    /// Serve Wikipath database(s)
    Serve {
        /// Directory of databases
        #[clap(long, default_value = "./databases")]
        databases: String,
        /// Port on which to serve web interface
        #[clap(long, default_value_t = 1789)]
        port: u16,
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
        } => {
            let databases_dir = PathBuf::from(databases);
            let dumps_dir = PathBuf::from(dumps);
            for language in language.split(",") {
                if let Err(e) = build::build(&language, &databases_dir, &dumps_dir).await {
                    eprintln!("FATAL: {}", e);
                    std::process::exit(1);
                }
            }
        }
        Action::Serve { databases, port } => {
            let databases_dir = PathBuf::from(databases);
            if let Err(e) = serve::serve(&databases_dir, port).await {
                eprintln!("FATAL: {}", e);
                std::process::exit(1);
            }
        }
    }
}
