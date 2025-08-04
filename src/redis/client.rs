use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{redis::{client, create_array_resp, create_bulk_string_resp, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};


pub struct CacheVal {
    val: String,
    expiry_time: Option<u128>
}
pub struct Client {
    id: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>,
    lists: Arc<Mutex<HashMap<String, Vec<String>>>>,
    blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>>
}

impl Client {
    pub fn new(cache: Arc<Mutex<HashMap<String, CacheVal>>>, lists: Arc<Mutex<HashMap<String, Vec<String>>>>, blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>>) -> Self {
        Client {
            id: uuid::Uuid::new_v4().to_string(),
            blocked_list_queue: blocked_list_queue,
            cache: cache,
            lists: lists,
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
                            _ => {}
                        }
                    } 
                }
            },
            _ => panic!("ONLY EXPECTING ARRAY COMMANDS")
        }

        None
    }

    fn handle_get(&self, iter: &mut Iter<'_, RespType>) -> Option<String> {
        let cache_guard = self.cache.lock().unwrap();
        if let Some(RespType::String(key)) = iter.next() {
            let value = cache_guard.get(key);
            return match value {
                Some(v) =>  {
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
                None => Some(create_null_bulk_string_resp())
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
                        cache_guard.insert(key.clone(), CacheVal { val: val.to_string(), expiry_time: Some(expiry) });
                        return Some(create_simple_string_resp("OK".to_string()))
                    } else {
                        panic!("Invalid expiry time")
                    }
                },
                (_, _) => {
                    cache_guard.insert(key.clone(), CacheVal { val: val.to_string(), expiry_time: None });
                    return Some(create_simple_string_resp("OK".to_string()));
                }
            }
        }

        None
    }

    fn handle_llen(&mut self, iter: &mut Iter<'_, RespType>) -> usize {
        let lists_guard = self.lists.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {
            if !lists_guard.contains_key(list_key.into()) {
                return 0;
            }
            return lists_guard.get(list_key.into()).unwrap().len();
        }

        return 0;
    }

    fn handle_lpop(&mut self, iter: &mut Iter<'_, RespType>) -> String {
        let mut lists_guard = self.lists.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {
            if !lists_guard.contains_key(list_key.into()) || lists_guard.get(list_key.into()).expect("should have list").len() == 0 {
                return create_null_bulk_string_resp();
            }

            let mut count_to_pop = 0;
            let list = lists_guard.get_mut(list_key.into()).expect("should have list");
            if let Some(RespType::String(count)) = iter.next() {
                if let Ok(num) = count.parse::<usize>() {
                    count_to_pop = num.min(list.len());
                }
            }

            if count_to_pop == 0 {
                let val = list.remove(0);
                return create_bulk_string_resp(val);
            } else {
                let mut vals = vec![];
                for _ in 0..count_to_pop {
                    vals.push(list.remove(0))
                }
                let bulk_strs: Vec<String> = vals.iter().map(|item| create_bulk_string_resp(item.to_string())).collect();
                return create_array_resp(bulk_strs);
            }
        }

        return create_null_bulk_string_resp();
    }

    fn handle_blpop(&mut self, iter: &mut Iter<'_, RespType>) -> String {
        if let Some(RespType::String(list_key)) = iter.next() {
            loop {
                let mut blocked_list_queue_gaurd = self.blocked_list_queue.lock().unwrap();
                if !blocked_list_queue_gaurd.contains_key(list_key.into()) {
                    blocked_list_queue_gaurd.insert(list_key.into(), vec![]);
                }

                let queue = blocked_list_queue_gaurd.get_mut(list_key.into()).unwrap();
                if queue.len() > 0 && queue.first().is_some_and(|id| !self.id.eq(id)) {
                    // if there is a line and you are not at the front
                    if queue.iter().find(|&id| self.id.eq(id)).is_none() {
                        // add yourself to the queue 
                        queue.push(self.id.clone());
                    }
                } else {
                    // you are at the front, do your logic
                    let mut lists_guard = self.lists.lock().unwrap();
                    if !lists_guard.contains_key(list_key.into()) || lists_guard.get_mut(list_key.into()).expect("should have list").len() == 0 {
                        if queue.iter().find(|&id| self.id.eq(id)).is_none() {
                            // add yourself to the queue 
                            queue.push(self.id.clone());
                        }
                        continue;
                    }

                    let val = lists_guard.get_mut(list_key.into()).expect("should have list").remove(0);
                    if queue.len() > 0 {
                        queue.remove(0);
                    }
                    let bulk_strs = vec![create_bulk_string_resp(list_key.into()),create_bulk_string_resp(val)];
                    return create_array_resp(bulk_strs);
                }
            }
        }

        return create_null_bulk_string_resp();
    }

    fn handle_rpush(&mut self, iter: &mut Iter<'_, RespType>) -> usize {
        let mut lists_guard = self.lists.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {
            if !lists_guard.contains_key(list_key.into()) {
                lists_guard.insert(list_key.into(), vec![]);
            }

            let list = lists_guard.get_mut(list_key.into()).unwrap();
            while let Some(RespType::String(val)) = iter.next() {
                list.push(val.into());
            }

            return lists_guard.get(list_key.into()).unwrap().len();
        }

        return 0;
    }

    fn handle_lpush(&mut self, iter: &mut Iter<'_, RespType>) -> usize {
        let mut lists_guard = self.lists.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {
            if !lists_guard.contains_key(list_key.into()) {
                lists_guard.insert(list_key.into(), vec![]);
            }

            let list = lists_guard.get_mut(list_key.into()).unwrap();
            while let Some(RespType::String(val)) = iter.next() {
                list.insert(0, val.into());
            }
            return lists_guard.get(list_key.into()).unwrap().len();
        }

        return 0;
    }

    fn handle_lrange(&self, iter: &mut Iter<'_, RespType>) -> Vec<String> {
        let lists_guard = self.lists.lock().unwrap();
        if let Some(RespType::String(list_key)) = iter.next() {
            if !lists_guard.contains_key(list_key.into()) {
                return vec![];
            }

            match (iter.next(), iter.next()) {
                (Some(RespType::String(start)), Some(RespType::String(end))) => {
                    if let (Ok(mut start_idx), Ok(mut end_idx)) = (start.parse::<i64>(), end.parse::<i64>()) {
                        let list = lists_guard.get(list_key.into()).unwrap();
                        if start_idx < 0 {
                            start_idx += list.len() as i64;
                        }
                        if end_idx < 0 {
                            end_idx += list.len() as i64;
                        }
                        if start_idx >= list.len() as i64 || start_idx > end_idx {
                            return vec![];
                        }
                        
                        end_idx = end_idx.min(list.len() as i64 - 1);
                        start_idx = start_idx.max(0);
                        return list[(start_idx as usize)..=(end_idx as usize)].to_vec();
                    }
                },
                (_, _) => return vec![]
            }
        }
        return vec![];
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_set_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("SET".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+OK\r\n"));
        let cach_gaurd = cache.lock().unwrap();
        assert!(cach_gaurd.contains_key("foo"));
        let cache_val = cach_gaurd.get("foo").unwrap();
        assert!(cache_val.val.eq("bar"));
        assert!(cache_val.expiry_time.is_none());
    }

    #[test]
    fn test_set_with_expr_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("SET".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string()),
            RespType::String("PX".to_string()),
            RespType::String("100".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+OK\r\n"));
        let cache_guard = cache.lock().unwrap();
        assert!(cache_guard.contains_key("foo"));
        let cache_val = cache_guard.get("foo").unwrap();
        assert!(cache_val.val.eq("bar"));
        assert!(cache_val.expiry_time.is_some());
        assert!(cache_val.expiry_time.unwrap() > 100);
    }

    #[test]
    fn test_get_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal { val: "bar".to_string(), expiry_time: None });
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+bar\r\n"));
    }

    #[test]
    fn test_get_expired_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal { val: "bar".to_string(), expiry_time: Some(500) });
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$-1\r\n"));
    }

    #[test]
    fn test_get_non_expired_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal { val: "bar".to_string(), expiry_time: Some(now + 60000) });
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+bar\r\n"));
    }

    #[test]
    fn test_null_get_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$-1\r\n"));
    }

    #[test]
    fn test_ping_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![RespType::String("PING".to_string())];
        let cmd = RespType::Array(cmds);
        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+PONG\r\n"));
    }

    #[test]
    fn test_echo_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("ECHO".to_string()),
            RespType::String("hello".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+hello\r\n"));
    }

    #[test]
    fn test_llen_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LLEN".to_string()),
            RespType::String("list_key".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":0\r\n"));

        {
            let mut lists_guard = lists.lock().unwrap();
            lists_guard.insert("list_key".into(), vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()]);
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
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LPOP".to_string()),
            RespType::String("list_key".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$-1\r\n"));

        {
            let mut lists_guard = lists.lock().unwrap();
            lists_guard.insert("list_key".into(), vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()]);
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
        let lists_guard = lists.lock().unwrap();
        assert!(lists_guard.get("list_key".into()).unwrap().len() == 4);
        assert!(lists_guard.get("list_key".into()).unwrap().get(0).unwrap().eq("c"));
    }

    #[test]
    fn test_rpush_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("RPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("foo".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":1\r\n"));
        let lists_guard = lists.lock().unwrap();
        assert!(lists_guard.get("list_key".into()).unwrap().len() == 1);
        assert!(lists_guard.get("list_key".into()).unwrap().get(0).unwrap().eq("foo"));
        drop(lists_guard);

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
        let lists_guard = lists.lock().unwrap();
        assert!(lists_guard.get("list_key".into()).unwrap().len() == 2);
        assert!(lists_guard.get("list_key".into()).unwrap().get(1).unwrap().eq("bar"));
    }

    #[test]
    fn test_multi_rpush_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("RPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string()),
            RespType::String("baz".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":3\r\n"));
        let lists_guard = lists.lock().unwrap();
        assert!(lists_guard.get("list_key".into()).unwrap().len() == 3);
        assert!(lists_guard.get("list_key".into()).unwrap().get(0).unwrap().eq("foo"));
    }

    #[test]
    fn test_multi_lpush_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string()),
            RespType::String("baz".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":3\r\n"));
        let lists_guard = lists.lock().unwrap();
        assert!(lists_guard.get("list_key".into()).unwrap().len() == 3);
        assert!(lists_guard.get("list_key".into()).unwrap().get(0).unwrap().eq("baz"));
        drop(lists_guard);

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
        let lists_guard = lists.lock().unwrap();
        assert!(lists_guard.get("list_key".into()).unwrap().len() == 4);
        assert!(lists_guard.get("list_key".into()).unwrap().get(0).unwrap().eq("first"));
    }

    #[test]
    fn test_lrange_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LRANGE".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("2".to_string()),
            RespType::String("4".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        {
            let mut lists_guard = lists.lock().unwrap();
            lists_guard.insert("list_key".into(), vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()]);
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"));
    }

    #[test]
    fn test_empty_lrange_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LRANGE".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("2".to_string()),
            RespType::String("4".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("*0\r\n"));
    }

    #[test]
    fn test_negative_lrange_command() {
        let lists: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let blocked_list_queue: Arc<Mutex<HashMap<String, Vec<String>>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LRANGE".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("2".to_string()),
            RespType::String("-2".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), lists.clone(), blocked_list_queue.clone());
        {
            let mut lists_guard = lists.lock().unwrap();
            lists_guard.insert("list_key".into(), vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()]);
        }
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"));
    }
}