use std::{collections::HashMap, fmt::format, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::CacheVal, create_bulk_string_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct InfoCommand {
    role: String,
    master_repl_id: Option<String>,
    master_repl_offset: Option<u128>,
}

impl InfoCommand {
    pub fn new(role: String,  master_repl_id: Option<String>, master_repl_offset: Option<u128>,) -> Self {
        InfoCommand { 
            role: role,
            master_repl_id: master_repl_id,
            master_repl_offset: master_repl_offset,
        }
    }
}

impl RedisCommand for InfoCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let mut info_string = format!("role:{}", self.role);
        if let Some(master_id) = &self.master_repl_id {
            info_string += format!("\r\nmaster_replid:{master_id}").as_str()
        }
        if let Some(master_offset) = &self.master_repl_offset {
            info_string += format!("\r\nmaster_repl_offset:{master_offset}").as_str()
        }
        vec![create_bulk_string_resp(info_string)]
    }
}