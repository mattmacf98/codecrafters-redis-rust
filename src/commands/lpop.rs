use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::CacheVal, create_array_resp, create_bulk_string_resp, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct LpopCommand {
    list_key: String,
    count: Option<usize>,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl LpopCommand {
    pub fn new(list_key: String, count: Option<usize>, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        LpopCommand {
            list_key: list_key,
            count: count,
            cache: cache
        }
    }
}

impl RedisCommand for LpopCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let mut cache_guard = self.cache.lock().unwrap();
        return match cache_guard.get_mut(&self.list_key) {
            Some(CacheVal::List(val)) if val.list.len() > 0 => {
                match self.count {
                    Some(count_to_pop) if count_to_pop == 1 => {
                        let val = val.list.remove(0);
                        vec![create_bulk_string_resp(val)]
                    },
                    Some(count_to_pop) => {
                        let mut vals = vec![];
                        for _ in 0..count_to_pop {
                            vals.push(val.list.remove(0))
                        }
                        let bulk_strs: Vec<String> = vals.iter().map(|item| create_bulk_string_resp(item.to_string())).collect();
                        vec![create_array_resp(bulk_strs)]
                    },
                    None => {
                        let val = val.list.remove(0);
                        vec![create_bulk_string_resp(val)]
                    }
                }
            }
            _ => vec![create_null_bulk_string_resp()]
        }
    }
}