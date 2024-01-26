#![warn(clippy::pedantic)]

use std::env;
use std::process::Command;

fn main() {
    let skip_npm = env::var("SKIP_NPM").map(|v| v == "1").unwrap_or(false);

    if !skip_npm {
        let install_command = Command::new("npm")
            .args(["install", "--prefix", "web"])
            .status()
            .unwrap();
        assert!(
            install_command.success(),
            "failed to execute npm install: {install_command}"
        );

        let build_command = Command::new("npm")
            .args(["run", "build", "--prefix", "web"])
            .status()
            .unwrap();
        assert!(
            build_command.success(),
            "failed to execute npm build: {build_command}"
        );
    }
}
