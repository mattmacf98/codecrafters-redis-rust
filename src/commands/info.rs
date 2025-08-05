use std::{collections::HashMap, fmt::format, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::CacheVal, create_bulk_string_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct InfoCommand {
    role: String
}

impl InfoCommand {
    pub fn new(role: String) -> Self {
        InfoCommand { 
            role: role
        }
    }
}

impl RedisCommand for InfoCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> String {
        return create_bulk_string_resp(format!("role:{}", self.role));
    }
}