use std::slice::Iter;
use crate::{redis::create_simple_string_resp, resp::types::RespType};

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

pub trait RedisCommand {
    fn execute(&self, iter: &mut Iter<'_, RespType>) -> String;
}