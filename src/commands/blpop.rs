use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{client::{CacheVal, ListCacheVal}, create_array_resp, create_bulk_string_resp, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct BlpopCommand {
    list_key: String,
    connection_id: String,
    timeout_seconds: f32,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl BlpopCommand {
    pub fn new(list_key: String, connection_id: String, timeout_seconds: f32, cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        BlpopCommand {
            list_key: list_key,
            connection_id: connection_id,
            timeout_seconds: timeout_seconds,
            cache: cache
        }
    }
}

impl RedisCommand for BlpopCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> String {
        let expiration = if self.timeout_seconds != 0.0 {
            let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis();
            Some(now + ((self.timeout_seconds * 1000.0) as u128))
        } else {
            None
        };

        loop {
            let mut cache_gaurd = self.cache.lock().unwrap();
            if !cache_gaurd.contains_key(&self.list_key) {
                cache_gaurd.insert(self.list_key.clone(), CacheVal::List(ListCacheVal { list: vec![], block_queue: vec![] }));
            }

            match cache_gaurd.get_mut(&self.list_key) {
                Some(CacheVal::List(list_cache_val)) => {
                    if list_cache_val.block_queue.len() > 0 && list_cache_val.block_queue.first().is_some_and(|id| !self.connection_id.eq(id)) {
                        // if there is a line and you are not at the front
                        if list_cache_val.block_queue.iter().find(|&id| self.connection_id.eq(id)).is_none() {
                            // add yourself to the queue 
                            list_cache_val.block_queue.push(self.connection_id.clone());
                        }
                    } else {
                        // you are at the front, do your logic
                        if list_cache_val.list.len() == 0 {
                            // no items
                            if list_cache_val.block_queue.iter().find(|&id| self.connection_id.eq(id)).is_none() {
                                // add yourself to the queue 
                                list_cache_val.block_queue.push(self.connection_id.clone());
                            }
                        } else {
                            let val = list_cache_val.list.remove(0);
                            if list_cache_val.block_queue.len() > 0 {
                                list_cache_val.block_queue.remove(0);
                            }
                            let bulk_strs = vec![create_bulk_string_resp(self.list_key.clone()),create_bulk_string_resp(val)];
                            return create_array_resp(bulk_strs);
                        }
                    }

                    // break if you have waited too long
                    let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();
                    if expiration.is_some() && now > expiration.unwrap() {
                        let index = list_cache_val.block_queue.iter().position(|id| self.connection_id.eq(id));
                        if index.is_some() {
                            list_cache_val.block_queue.remove(index.unwrap());
                        }
                        break;
                    }
                },
                _ => panic!("SHOULD NOT GET HERE")
            }
        }

        create_null_bulk_string_resp()
    }
}