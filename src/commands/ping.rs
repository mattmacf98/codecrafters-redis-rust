use std::slice::Iter;

use crate::{commands::RedisCommand, redis::create_simple_string_resp, resp::types::RespType};

pub struct PingCommand {}

impl PingCommand {
    pub fn new() -> Self {
        PingCommand {}
    }
}

impl RedisCommand for PingCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        vec![create_simple_string_resp(String::from("PONG"))]
    }
}