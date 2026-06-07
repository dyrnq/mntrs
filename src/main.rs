use mntrs::cmd::mount;
use std::collections::HashMap;

fn main() {
    tracing_subscriber::fmt::init();

    let mut opts = HashMap::new();
    opts.insert("endpoint".into(), "http://192.168.6.130:19000".into());
    opts.insert("access-key".into(), "u5SybesIDVX9b6Pk".into());
    opts.insert("secret-key".into(), "lOpH1v7kdM6H8NkPu1H2R6gLc9jcsmWM".into());
    opts.insert("region".into(), "us-east-1".into());

    mount::mount("s3://maven-repo", "/tmp/mntrs-test", &opts).unwrap();
}
