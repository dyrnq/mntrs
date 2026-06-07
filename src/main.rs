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
    /// Mount storage to a local directory
    Mount {
        /// Storage URL (s3://bucket, hdfs://namenode/path, gs://bucket, etc.)
        storage: String,
        /// Local mount point
        mountpoint: String,
        /// Storage options: --opt endpoint=URL --opt access-key=KEY
        #[arg(long = "opt", value_name = "KEY=VAL", num_args = 0..)]
        opt: Vec<String>,
    },
    /// Unmount a mounted directory
    Unmount {
        /// Mount point or name
        target: String,
    },
    /// List active mounts
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
            todo!("unmount {}", target);
        }
        Commands::List => {
            todo!("list mounts");
        }
    }
    Ok(())
}
