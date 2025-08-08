use std::slice::Iter;

use crate::{commands::RedisCommand, resp::{create_simple_string_resp, types::RespType}};

pub struct EchoCommand {
    message: String
}

impl EchoCommand {
    pub fn new(message: String) -> Self {
        EchoCommand {
            message: message
        }
    }
}

impl RedisCommand for EchoCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        vec![create_simple_string_resp(self.message.clone())]
    }
}