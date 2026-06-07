use clap::{Parser, Subcommand};
use std::collections::HashMap;

#[derive(Parser)]
#[command(name = "mntrs", about = "Mount remote storage to local directory via FUSE")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Mount {
        storage: String,
        mountpoint: String,
        #[arg(long = "opt", value_name = "KEY=VAL", num_args = 0..)]
        opt: Vec<String>,
    },
    Unmount { target: String },
    List,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();
    match cli.command {
        Commands::Mount { storage, mountpoint, opt } => {
            let opts: HashMap<String, String> = opt.iter()
                .filter_map(|kv| kv.split_once('='))
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();
            mntrs::cmd::mount::mount(&storage, &mountpoint, &opts)?;
        }
        Commands::Unmount { target } => {
            mntrs::cmd::unmount::unmount(&target)?;
        }
        Commands::List => {
            mntrs::cmd::list::list()?;
        }
    }
    Ok(())
}
