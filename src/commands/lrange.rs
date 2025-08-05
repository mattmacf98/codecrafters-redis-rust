use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::{CacheVal, ListCacheVal, StringCacheVal}, create_array_resp, create_bulk_string_resp, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct LrangeCommand {
    list_key: String,
    start: i64,
    end: i64,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl LrangeCommand {
    pub fn new(list_key: String, start: i64, end: i64, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        LrangeCommand {
            list_key: list_key,
            start: start,
            end: end,
            cache: cache,
        }
    }
}

impl RedisCommand for LrangeCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> String {
        let mut cache_gaurd = self.cache.lock().unwrap();
        return match cache_gaurd.get_mut(&self.list_key) {
            Some(CacheVal::List(list_cache_val)) => {
                let mut start_idx = self.start;
                let mut end_idx = self.end;

                if start_idx < 0 {
                    start_idx += list_cache_val.list.len() as i64;
                }
                if end_idx < 0 {
                    end_idx += list_cache_val.list.len() as i64;
                }
                if start_idx >= list_cache_val.list.len() as i64 || start_idx > end_idx {
                    return create_array_resp(vec![]);
                }
                
                end_idx = end_idx.min(list_cache_val.list.len() as i64 - 1);
                start_idx = start_idx.max(0);
                let vals =  list_cache_val.list[(start_idx as usize)..=(end_idx as usize)].to_vec();
                let bulk_strs: Vec<String> = vals.iter().map(|item| create_bulk_string_resp(item.to_string())).collect();
                create_array_resp(bulk_strs)
            },
            _ =>  create_array_resp(vec![])
        }
    }
}