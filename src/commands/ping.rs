use std::slice::Iter;

use crate::{commands::RedisCommand, resp::{create_array_resp, create_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct PingCommand {
    is_subscribe_context: bool,
}

impl PingCommand {
    pub fn new(is_subscribe_context: bool) -> Self {
        PingCommand { is_subscribe_context }
    }
}

impl RedisCommand for PingCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        if self.is_subscribe_context {
            vec![create_array_resp(vec![create_bulk_string_resp("pong".to_string()), create_bulk_string_resp("".to_string())])]
        } else {
            vec![create_simple_string_resp(String::from("PONG"))]
        }
    }
}