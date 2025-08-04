use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{redis::{client, create_array_resp, create_basic_err_resp, create_bulk_string_resp, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub enum CacheVal {
    String(StringCacheVal),
    List(ListCacheVal),
    Stream(StreamCacheVal)
}
pub struct StringCacheVal {
    val: String,
    expiry_time: Option<u128>
}

pub struct ListCacheVal {
    list: Vec<String>,
    block_queue: Vec<String>
}

pub struct StreamCacheVal {
    stream: Vec<StreamItem>
}

pub struct KeyVal {
    key: String,
    val: String
}

pub struct StreamItem {
    id: String,
    key_vals: Vec<KeyVal>
}
pub struct Client {
    id: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>
}

impl Client {
    pub fn new(cache: Arc<Mutex<HashMap<String, CacheVal>>>) -> Self {
        Client {
            id: uuid::Uuid::new_v4().to_string(),
            cache: cache
        }
    }

    pub fn handle_command(&mut self, cmd: RespType) -> Option<String> {
        match cmd {
            RespType::Array(resp_types) => {
                let mut iter = resp_types.iter();

                while let Some(cmd) = iter.next() {
                    if let RespType::String(s) = cmd {
                        let command = s.to_lowercase();

                        match command.as_str() {
                            "ping" => return Some(create_simple_string_resp(String::from("PONG"))),
                            "echo" => {
                                if let RespType::String(message) = iter.next().expect("Should have echo message") {
                                    return Some(create_simple_string_resp(message.to_string()));
                                }
                            },
                            "get" => return self.handle_get(&mut iter),
                            "set" => return self.handle_set(&mut iter),
                            "rpush" => {
                                let size = self.handle_rpush(&mut iter);
                                return Some(create_int_resp(size));
                            },
                            "lpush" => {
                                let size = self.handle_lpush(&mut iter);
                                return Some(create_int_resp(size));
                            },
                            "lrange" => {
                                let vals = self.handle_lrange(&mut iter);
                                let bulk_strs: Vec<String> = vals.iter().map(|item| create_bulk_string_resp(item.to_string())).collect();
                                return Some(create_array_resp(bulk_strs));
                            },
                            "llen" => {
                                let size = self.handle_llen(&mut iter);
                                return Some(create_int_resp(size));
                            },
                            "lpop" => return Some(self.handle_lpop(&mut iter)),
                            "blpop" => return Some(self.handle_blpop(&mut iter)),
                            "type" => return Some(self.handle_type(&mut iter)),
                            "xadd" => return Some(self.handle_xadd(&mut iter)),
                            _ => {}
                        }
                    } 
                }
            },
            _ => panic!("ONLY EXPECTING ARRAY COMMANDS")
        }

        None
    }

    fn handle_xadd(&self, iter: &mut Iter<'_, RespType>) -> String {
        let mut cache_guard = self.cache.lock().unwrap();
        if let Some(RespType::String(key)) = iter.next() {
            if !cache_guard.contains_key(key.into()) {
                cache_guard.insert(key.into(), CacheVal::Stream(StreamCacheVal { stream: vec![] }));
            }

            match cache_guard.get_mut(key.into()) {
                Some(CacheVal::Stream(cache_stream)) => {
                    if let Some(RespType::String(entry_id)) = iter.next() {
                        let parts: Vec<&str> = entry_id.split('-').collect();
                        if parts.len() != 2 {
                            return create_basic_err_resp("ERR Invalid stream ID format".to_string());
                        }

                        if entry_id == "0-0" {
                            return create_basic_err_resp("ERR The ID specified in XADD must be greater than 0-0".to_string());
                        }
                        
                        let stream_id = parts[1];
                        let final_id_sequence = match (stream_id, cache_stream.stream.is_empty()) {
                            ("*", true) => if parts[0] == "0" { 1 } else { 0 },
                            ("*", false) => {
                                let last_id = &cache_stream.stream.last().unwrap().id;
                                let last_id_parts: Vec<&str> = last_id.split('-').collect();
                                if last_id_parts[0] == parts[0] {
                                    let last_id_sequence = last_id_parts[1].parse::<i64>().unwrap_or(0);
                                    last_id_sequence + 1    
                                } else {
                                    if parts[0] == "0" { 1 } else { 0 }
                                }
                                
                            },
                            (val, _) => val.parse::<i64>().unwrap_or(0)
                        };

                        let entry_id = format!("{}-{}", parts[0], final_id_sequence);

                        if !cache_stream.stream.is_empty() {
                            let last_id = &cache_stream.stream.last().unwrap().id;
                            if entry_id <= last_id.clone() {
                                return create_basic_err_resp("ERR The ID specified in XADD is equal or smaller than the target stream top item".to_string());
                            }
                        }

                        let mut kvs: Vec<KeyVal> = vec![];
                        loop {
                            match (iter.next(), iter.next()) {
                                (Some(RespType::String(entry_key)), Some(RespType::String(entry_val))) => {
                                    kvs.push(KeyVal { key: entry_key.to_string(), val: entry_val.to_string() });
                                },
                                _ => break
                            }
                        }
                        
                        cache_stream.stream.push(StreamItem { id: entry_id.clone().into(), key_vals: kvs });
                        return create_bulk_string_resp(entry_id.to_string());
                    }
                }
                _ => return create_null_bulk_string_resp()
            }
        }

        return create_null_bulk_string_resp();
    }

    fn handle_type(&self, iter: &mut Iter<'_, RespType>) -> String {
        let cache_guard = self.cache.lock().unwrap();
        if let Some(RespType::String(key)) = iter.next() {
            let value = cache_guard.get(key);
            match value {
                Some(CacheVal::String(_)) => return create_simple_string_resp("string".to_string()),
                Some(CacheVal::List(_)) => return create_simple_string_resp("list".to_string()),
                Some(CacheVal::Stream(_)) => return create_simple_string_resp("stream".to_string()),
                None => return create_simple_string_resp("none".to_string())
            }
        }

        return create_simple_string_resp("none".to_string());
    }

    fn handle_get(&self, iter: &mut Iter<'_, RespType>) -> Option<String> {
        let cache_guard = self.cache.lock().unwrap();
        if let Some(RespType::String(key)) = iter.next() {
            let value = cache_guard.get(key);
            return match value {
                Some(CacheVal::String(v)) =>  {
                    match v.expiry_time {
                        Some(exp) => {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis();
                            if now < exp {
                                Some(create_simple_string_resp(v.val.to_string()))
                            } else {
                                Some(create_null_bulk_string_resp())
                            }
                        },
                        None => Some(create_simple_string_resp(v.val.to_string()))
                    }
                }
                _ => Some(create_null_bulk_string_resp())
            }
        }

        None
    }

    fn handle_set(&mut self, iter: &mut Iter<'_, RespType>) -> Option<String> {
        let mut cache_guard = self.cache.lock().unwrap();
        if let (Some(RespType::String(key)), Some(RespType::String(val))) = (iter.next(), iter.next()) {
            match (iter.next(), iter.next()) {
                (Some(RespType::String(px)), Some(RespType::String(exp))) if px.to_lowercase().eq("px") => {
                    if let Ok(exp_millis) = exp.parse::<u128>() {
                        let expiry = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() + exp_millis;
                        cache_guard.insert(key.clone(), CacheVal::String(StringCacheVal { val: val.to_string(), expiry_time: Some(expiry) }));
                        return Some(create_simple_string_resp("OK".to_string()))
                    } else {
                        panic!("Invalid expiry time")
                    }
                },
                (_, _) => {
                    cache_guard.insert(key.clone(), CacheVal::String(StringCacheVal { val: val.to_string(), expiry_time: None }));
                    return Some(create_simple_string_resp("OK".to_string()));
                }
            }
        }

        None
    }

    fn handle_llen(&mut self, iter: &mut Iter<'_, RespType>) -> usize {
        let cache_gaurd = self.cache.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {

            match cache_gaurd.get(list_key.into()) {
                Some(CacheVal::List(val)) => {
                    return val.list.len();
                },
                _ => return 0,
            }
        }

        return 0;
    }

    fn handle_lpop(&mut self, iter: &mut Iter<'_, RespType>) -> String {
        let mut cache_guard = self.cache.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {
            match cache_guard.get_mut(list_key.into()) {
                Some(CacheVal::List(val)) if val.list.len() > 0 => {
                    let mut count_to_pop = 0;
                    if let Some(RespType::String(count)) = iter.next() {
                        if let Ok(num) = count.parse::<usize>() {
                            count_to_pop = num.min(val.list.len());
                        }
                    }

                    if count_to_pop == 0 {
                        let val = val.list.remove(0);
                        return create_bulk_string_resp(val);
                    } else {
                        let mut vals = vec![];
                        for _ in 0..count_to_pop {
                            vals.push(val.list.remove(0))
                        }
                        let bulk_strs: Vec<String> = vals.iter().map(|item| create_bulk_string_resp(item.to_string())).collect();
                        return create_array_resp(bulk_strs);
                    }
                }
                _ => return create_null_bulk_string_resp()
            }
        }

        return create_null_bulk_string_resp();
    }

    fn handle_blpop(&mut self, iter: &mut Iter<'_, RespType>) -> String {
        if let Some(RespType::String(list_key)) = iter.next() {
            let mut expiration: Option<u128> = None;
            if let Some(RespType::String(seconds)) = iter.next() {
                if let Ok(num) = seconds.parse::<f32>() {
                    if num != 0.0 {
                        let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap()
                                .as_millis();
                        expiration = Some(now + ((num * 1000.0) as u128));
                    }
                }
            }

            loop {
                let mut cache_gaurd = self.cache.lock().unwrap();
                if !cache_gaurd.contains_key(list_key.into()) {
                    cache_gaurd.insert(list_key.into(), CacheVal::List(ListCacheVal { list: vec![], block_queue: vec![] }));
                }

                match cache_gaurd.get_mut(list_key.into()) {
                    Some(CacheVal::List(list_cache_val)) => {
                        if list_cache_val.block_queue.len() > 0 && list_cache_val.block_queue.first().is_some_and(|id| !self.id.eq(id)) {
                            // if there is a line and you are not at the front
                            if list_cache_val.block_queue.iter().find(|&id| self.id.eq(id)).is_none() {
                                // add yourself to the queue 
                                list_cache_val.block_queue.push(self.id.clone());
                            }
                        } else {
                            // you are at the front, do your logic
                            if list_cache_val.list.len() == 0 {
                                // no items
                                if list_cache_val.block_queue.iter().find(|&id| self.id.eq(id)).is_none() {
                                    // add yourself to the queue 
                                    list_cache_val.block_queue.push(self.id.clone());
                                }
                            } else {
                                let val = list_cache_val.list.remove(0);
                                if list_cache_val.block_queue.len() > 0 {
                                    list_cache_val.block_queue.remove(0);
                                }
                                let bulk_strs = vec![create_bulk_string_resp(list_key.into()),create_bulk_string_resp(val)];
                                return create_array_resp(bulk_strs);
                            }
                        }

                        // break if you have waited too long
                        let now = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis();
                        if expiration.is_some() && now > expiration.unwrap() {
                            let index = list_cache_val.block_queue.iter().position(|id| self.id.eq(id));
                            if index.is_some() {
                                list_cache_val.block_queue.remove(index.unwrap());
                            }
                            break;
                        }
                    },
                    _ => panic!("SHOULD NOT GET HERE")
                }
            }
        }

        return create_null_bulk_string_resp();
    }

    fn handle_rpush(&mut self, iter: &mut Iter<'_, RespType>) -> usize {
        let mut cache_gaurd = self.cache.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {
            match cache_gaurd.get_mut(list_key.into()) {
                Some(CacheVal::List(list_cache_val)) => {
                    while let Some(RespType::String(val)) = iter.next() {
                        list_cache_val.list.push(val.into());
                    }
                    return list_cache_val.list.len();
                },
                None => {
                    let mut list = vec![];
                    while let Some(RespType::String(val)) = iter.next() {
                        list.push(val.into());
                    }

                    let len = list.len();
                    cache_gaurd.insert(list_key.into(), CacheVal::List(ListCacheVal { list: list, block_queue: vec![] }));
                    return len;
                },
                _ => return 0
            }
        }

        return 0;
    }

    fn handle_lpush(&mut self, iter: &mut Iter<'_, RespType>) -> usize {
        let mut cache_gaurd = self.cache.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {
            match cache_gaurd.get_mut(list_key.into()) {
                Some(CacheVal::List(list_cache_val)) => {
                    while let Some(RespType::String(val)) = iter.next() {
                        list_cache_val.list.insert(0, val.into());
                    }
                    return list_cache_val.list.len();
                },
                None => {
                    let mut list = vec![];
                    while let Some(RespType::String(val)) = iter.next() {
                        list.push(val.into());
                    }

                    let len = list.len();
                    list.reverse();
                    cache_gaurd.insert(list_key.into(), CacheVal::List(ListCacheVal { list: list, block_queue: vec![] }));
                    return len;
                },
                _ => return 0
            }
        }

        return 0;
    }

    fn handle_lrange(&self, iter: &mut Iter<'_, RespType>) -> Vec<String> {
        let cache_gaurd = self.cache.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {
            match cache_gaurd.get(list_key.into()) {
                Some(CacheVal::List(list_cache_val)) => {
                    match (iter.next(), iter.next()) {
                        (Some(RespType::String(start)), Some(RespType::String(end))) => {
                            if let (Ok(mut start_idx), Ok(mut end_idx)) = (start.parse::<i64>(), end.parse::<i64>()) {
                                if start_idx < 0 {
                                    start_idx += list_cache_val.list.len() as i64;
                                }
                                if end_idx < 0 {
                                    end_idx += list_cache_val.list.len() as i64;
                                }
                                if start_idx >= list_cache_val.list.len() as i64 || start_idx > end_idx {
                                    return vec![];
                                }
                                
                                end_idx = end_idx.min(list_cache_val.list.len() as i64 - 1);
                                start_idx = start_idx.max(0);
                                return list_cache_val.list[(start_idx as usize)..=(end_idx as usize)].to_vec();
                            }
                        },
                        (_, _) => return vec![]
                    }
                }
                _ => return vec![]
            }
        }
        return vec![];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xadd_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        
        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("1526919030474-0".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string()),
            RespType::String("humidity".to_string()),
            RespType::String("92".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        println!("{}", value);
        assert!(value.eq("$15\r\n1526919030474-0\r\n"));
        let cach_gaurd = cache.lock().unwrap();
        assert!(cach_gaurd.contains_key("stream_key"));
        let cache_val = cach_gaurd.get("stream_key").unwrap();
        match cache_val {
            CacheVal::Stream(val) => {
                assert!(val.stream.len() == 1);
                assert!(val.stream.get(0).unwrap().id.eq("1526919030474-0"));
            },
            _ => panic!("Incorrect cache type")
        }
    }

    #[test]
    fn test_xadd_command_partial() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone());
        
        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("0-*".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$3\r\n0-1\r\n"));

        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("1-*".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        println!("{}", value);
        assert!(value.eq("$3\r\n1-0\r\n"));
        
        
        {
            let mut cache_guard = cache.lock().unwrap();
            let stream_item = StreamItem {id: "2-1".into(), key_vals: vec![]};
            cache_guard.insert("stream_key".to_string(), CacheVal::Stream(StreamCacheVal { stream: vec![stream_item] }));
        }

        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("2-*".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$3\r\n2-2\r\n"));
    }

    #[test]
    fn test_xadd_command_validation() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone());
        
        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("0-0".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("-ERR The ID specified in XADD must be greater than 0-0\r\n"));
        
        {
            let mut cache_guard = cache.lock().unwrap();
            let stream_item = StreamItem {id: "1-1".into(), key_vals: vec![]};
            cache_guard.insert("stream_key".to_string(), CacheVal::Stream(StreamCacheVal { stream: vec![stream_item] }));
        }

        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("1-1".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));

        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("0-1".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
    }

    #[test]
    fn test_type_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone());

        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal::String(StringCacheVal { val: "bar".to_string(), expiry_time: None }));
            cache_guard.insert("bar".to_string(), CacheVal::List(ListCacheVal {list: vec![], block_queue: vec![]}));
            cache_guard.insert("faz".to_string(), CacheVal::Stream(StreamCacheVal { stream: vec![] }));
        }

        let cmds = vec![
            RespType::String("TYPE".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+string\r\n"));

        let cmds = vec![
            RespType::String("TYPE".to_string()),
            RespType::String("bar".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+list\r\n"));

        let cmds = vec![
            RespType::String("TYPE".to_string()),
            RespType::String("faz".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+stream\r\n"));

        let cmds = vec![
            RespType::String("TYPE".to_string()),
            RespType::String("other".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+none\r\n"));
    }

    #[test]
    fn test_set_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        
        let cmds = vec![
            RespType::String("SET".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+OK\r\n"));
        let cach_gaurd = cache.lock().unwrap();
        assert!(cach_gaurd.contains_key("foo"));
        let cache_val = cach_gaurd.get("foo").unwrap();
        match cache_val {
            CacheVal::String(val) => {
                assert!(val.val.eq("bar"));
                assert!(val.expiry_time.is_none());
            },
            _ => panic!("Incorrect cache type")
        }
    }

    #[test]
    fn test_set_with_expr_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("SET".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string()),
            RespType::String("PX".to_string()),
            RespType::String("100".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+OK\r\n"));
        let cache_guard = cache.lock().unwrap();
        assert!(cache_guard.contains_key("foo"));
        let cache_val = cache_guard.get("foo").unwrap();
        match cache_val {
            CacheVal::String(val) => {
                assert!(val.val.eq("bar"));
                assert!(val.expiry_time.is_some());
                assert!(val.expiry_time.unwrap() > 100);
            },
            _ => panic!("Incorrect cache type")
        }
    }

    #[test]
    fn test_get_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal::String(StringCacheVal { val: "bar".to_string(), expiry_time: None }));
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+bar\r\n"));
    }

    #[test]
    fn test_get_expired_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal::String(StringCacheVal { val: "bar".to_string(), expiry_time: Some(500) }));
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$-1\r\n"));
    }

    #[test]
    fn test_get_non_expired_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();

        let mut client = Client::new(cache.clone());
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal::String(StringCacheVal { val: "bar".to_string(), expiry_time: Some(now + 60000) }));
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+bar\r\n"));
    }

    #[test]
    fn test_null_get_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$-1\r\n"));
    }

    #[test]
    fn test_ping_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![RespType::String("PING".to_string())];
        let cmd = RespType::Array(cmds);
        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+PONG\r\n"));
    }

    #[test]
    fn test_echo_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("ECHO".to_string()),
            RespType::String("hello".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+hello\r\n"));
    }

    #[test]
    fn test_llen_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LLEN".to_string()),
            RespType::String("list_key".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":0\r\n"));

        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("list_key".into(), CacheVal::List(ListCacheVal { list: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()], block_queue: vec![] }));
        }
        let cmds = vec![
            RespType::String("LLEN".to_string()),
            RespType::String("list_key".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":6\r\n"));
    }

    #[test]
    fn test_lpop_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LPOP".to_string()),
            RespType::String("list_key".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$-1\r\n"));

        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("list_key".into(), CacheVal::List(ListCacheVal { list: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()], block_queue: vec![] }));
        }
        let cmds = vec![
            RespType::String("LPOP".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("2".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("*2\r\n$1\r\na\r\n$1\r\nb\r\n"));
        let cache_guard = cache.lock().unwrap();
        match cache_guard.get("list_key".into()) {
            Some(CacheVal::List(val)) => {
                assert!(val.list.len() == 4);
                assert!(val.list.get(0).unwrap().eq("c"));
            },
            _ => panic!("list value not found"),
        }
    }

    #[test]
    fn test_rpush_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("RPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("foo".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":1\r\n"));
        let cache_guard = cache.lock().unwrap();
        match cache_guard.get("list_key".into()) {
            Some(CacheVal::List(val)) => {
                assert!(val.list.len() == 1);
                assert!(val.list.get(0).unwrap().eq("foo"));
            }
            _ => panic!("not list key")
        }
        drop(cache_guard);

        let cmds = vec![
            RespType::String("RPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("bar".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":2\r\n"));
        let cache_guard = cache.lock().unwrap();
        match cache_guard.get("list_key".into()) {
            Some(CacheVal::List(val)) => {
                assert!(val.list.len() == 2);
                assert!(val.list.get(1).unwrap().eq("bar"));
            }
            _ => panic!("not list key")
        }
    }

    #[test]
    fn test_multi_rpush_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("RPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string()),
            RespType::String("baz".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":3\r\n"));
        let cache_gaurd = cache.lock().unwrap();
        match cache_gaurd.get("list_key".into()) {
            Some(CacheVal::List(val)) => {
                assert!(val.list.len() == 3);
                assert!(val.list.get(0).unwrap().eq("foo"));
            }
            _ => panic!("Not list key")
        }

    }

    #[test]
    fn test_multi_lpush_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string()),
            RespType::String("baz".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":3\r\n"));
        let cache_guard = cache.lock().unwrap();
        match cache_guard.get("list_key".into()) {
            Some(CacheVal::List(val)) => {
                assert!(val.list.len() == 3);
                assert!(val.list.get(0).unwrap().eq("baz"));
            }
            _ => panic!("not list key")
        }
        drop(cache_guard);

        let cmds = vec![
            RespType::String("LPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("first".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":4\r\n"));
        let cache_guard = cache.lock().unwrap();
        match cache_guard.get("list_key".into()) {
            Some(CacheVal::List(val)) => {
                assert!(val.list.len() == 4);
                assert!(val.list.get(0).unwrap().eq("first"));
            }
            _ => panic!("not list key")
        }
    }

    #[test]
    fn test_lrange_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LRANGE".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("2".to_string()),
            RespType::String("4".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        {
            let mut cache_gaurd = cache.lock().unwrap();
            cache_gaurd.insert("list_key".into(), CacheVal::List(ListCacheVal {list: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()], block_queue: vec![]}));
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"));
    }

    #[test]
    fn test_empty_lrange_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LRANGE".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("2".to_string()),
            RespType::String("4".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("*0\r\n"));
    }

    #[test]
    fn test_negative_lrange_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LRANGE".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("2".to_string()),
            RespType::String("-2".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone());
        {
            let mut cache_gaurd = cache.lock().unwrap();
            cache_gaurd.insert("list_key".into(), CacheVal::List(ListCacheVal {list: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()], block_queue: vec![]}));
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"));
    }
}