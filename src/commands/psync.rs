use std::{collections::HashMap, fmt::format, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::CacheVal, create_bulk_string_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct PsyncCommand {
    master_repl_id: String,
    master_repl_offset: u128,
}

impl PsyncCommand {
    pub fn new(master_repl_id: String, master_repl_offset: u128,) -> Self {
        PsyncCommand {
            master_repl_id: master_repl_id,
            master_repl_offset: master_repl_offset,
        }
    }
}

impl RedisCommand for PsyncCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        return vec![
            create_simple_string_resp(format!("FULLRESYNC {} {}", self.master_repl_id, self.master_repl_offset)),
            "EMPTY_RDB".into()
        ]
    }
}