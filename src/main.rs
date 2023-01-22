use clap::{Parser, Subcommand};

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
    Build {
        #[clap(long)]
        language: String,
        #[clap(long, default_value = "./databases")]
        databases: String,
        #[clap(long, default_value = "./dumps")]
        dumps: String,
    },
    Serve {
        #[clap(long, default_value = "./databases")]
        databases: String,
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
            if let Err(e) = build::build(&language, &databases, &dumps).await {
                eprintln!("FATAL: {}", e);
                std::process::exit(1);
            }
        }
        Action::Serve { databases, port } => {
            if let Err(e) = serve::serve(&databases, port).await {
                eprintln!("FATAL: {}", e);
                std::process::exit(1);
            }
        }
    }
}
