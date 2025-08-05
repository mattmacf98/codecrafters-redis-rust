use std::slice::Iter;

use crate::{commands::RedisCommand, redis::create_simple_string_resp, resp::types::RespType};

pub struct ReplConfCommand {}

impl ReplConfCommand {
    pub fn new() -> Self {
        ReplConfCommand {}
    }
}

impl RedisCommand for ReplConfCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        vec![create_simple_string_resp("OK".to_string())]
    }
}