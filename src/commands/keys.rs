use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::CacheVal, create_array_resp, create_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct KeysCommand {
    pattern: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>,
}

impl KeysCommand {
    pub fn new(pattern: String, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        KeysCommand { pattern, cache }
    }
}

impl RedisCommand for KeysCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let mut keys = vec![];
        // for now always assume the pattern is *
        for (key, _) in self.cache.lock().unwrap().iter() {
            keys.push(key.clone());
        }
        return vec![create_array_resp(keys.into_iter().map(|x| create_bulk_string_resp(x)).collect())];
    }
}