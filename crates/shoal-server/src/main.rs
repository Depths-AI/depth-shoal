use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "depth-shoal")]
#[command(about = "Depth Shoal server (cache/buffer modes)", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Prints build info and exits
    Info,
}

fn main() {
    let cli = Cli::parse();

    match cli.command {
        Command::Info => {
            println!("depth-shoal (shoal-core v{})", shoal_core::version());
        }
    }
}
