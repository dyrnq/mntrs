pub mod cmd;

use std::path::Path;
use fuser::Filesystem;
use opendal::Operator;

pub struct MntrsFs {
    pub op: Operator,
}

impl Filesystem for MntrsFs {}
