use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::CacheVal, create_bulk_string_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct InfoCommand {}

impl InfoCommand {
    pub fn new() -> Self {
        InfoCommand { }
    }
}

impl RedisCommand for InfoCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> String {
        return create_bulk_string_resp("role:master".into());
    }
}