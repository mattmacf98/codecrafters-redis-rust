use std::slice::Iter;
use crate::{resp::types::RespType};

pub mod ping;
pub mod echo;
pub mod get;
pub mod set;
pub mod rpush;
pub mod lpush;
pub mod lrange;
pub mod llen;
pub mod lpop;
pub mod blpop;
pub mod type_command;
pub mod xadd;
pub mod xrange;
pub mod xread;
pub mod incr;
pub mod info;
pub mod replconf;
pub mod psync;
pub mod publish;
pub mod keys;
pub mod unsubscribe;
pub mod subscribe;
pub mod wait;

pub trait RedisCommand {
    fn execute(&self, iter: &mut Iter<'_, RespType>) -> Vec<String>;
}