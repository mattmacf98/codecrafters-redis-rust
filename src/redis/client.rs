use std::{collections::HashMap, slice::Iter};

use crate::{redis::{create_array_resp, create_bulk_string_resp, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};


struct CacheVal {
    val: String,
    expiry_time: Option<u128>
}
pub struct Client {
    cache: HashMap<String, CacheVal>,
    lists: HashMap<String, Vec<String>>
}

impl Client {
    pub fn new() -> Self {
        Client {
            cache: HashMap::new(),
            lists: HashMap::new(),
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
                            "lrange" => {
                                let vals = self.handle_lrange(&mut iter);
                                let bulk_strs: Vec<String> = vals.iter().map(|item| create_bulk_string_resp(item.to_string())).collect();
                                return Some(create_array_resp(bulk_strs));
                            }
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
        if let Some(RespType::String(key)) = iter.next() {
            let value = self.cache.get(key);
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
        if let (Some(RespType::String(key)), Some(RespType::String(val))) = (iter.next(), iter.next()) {
            match (iter.next(), iter.next()) {
                (Some(RespType::String(px)), Some(RespType::String(exp))) if px.to_lowercase().eq("px") => {
                    if let Ok(exp_millis) = exp.parse::<u128>() {
                        let expiry = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() + exp_millis;
                        self.cache.insert(key.clone(), CacheVal { val: val.to_string(), expiry_time: Some(expiry) });
                        return Some(create_simple_string_resp("OK".to_string()))
                    } else {
                        panic!("Invalid expiry time")
                    }
                },
                (_, _) => {
                    self.cache.insert(key.clone(), CacheVal { val: val.to_string(), expiry_time: None });
                    return Some(create_simple_string_resp("OK".to_string()));
                }
            }
        }

        None
    }

    fn handle_rpush(&mut self, iter: &mut Iter<'_, RespType>) -> usize {
        if let Some(RespType::String(list_key)) = iter.next() {
            if !self.lists.contains_key(list_key.into()) {
                self.lists.insert(list_key.into(), vec![]);
            }

            let mut vals: Vec<String> = vec![];
            while let Some(RespType::String(val)) = iter.next() {
                vals.push(val.into());
            }

            self.lists.get_mut(list_key.into()).unwrap().extend(vals);
            return self.lists.get(list_key.into()).unwrap().len();
        }

        return 0;
    }

    fn handle_lrange(&self, iter: &mut Iter<'_, RespType>) -> Vec<String> {
        if let Some(RespType::String(list_key)) = iter.next() {
            if !self.lists.contains_key(list_key.into()) {
                return vec![];
            }

            match (iter.next(), iter.next()) {
                (Some(RespType::String(start)), Some(RespType::String(end))) => {
                    if let (Ok(start_idx), Ok(end_idx)) = (start.parse::<usize>(), end.parse::<usize>()) {
                        let list = self.lists.get(list_key.into()).unwrap();
                        if start_idx >= list.len() || start_idx > end_idx {
                            return vec![];
                        }
                        
                        let end_idx = end_idx.min(list.len() - 1);
                        return list[start_idx..=end_idx].to_vec();
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
        let cmds = vec![
            RespType::String("SET".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+OK\r\n"));
        assert!(client.cache.contains_key("foo"));
        let cache_val = client.cache.get("foo").unwrap();
        assert!(cache_val.val.eq("bar"));
        assert!(cache_val.expiry_time.is_none());
    }

    #[test]
    fn test_set_with_expr_command() {
        let cmds = vec![
            RespType::String("SET".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string()),
            RespType::String("PX".to_string()),
            RespType::String("100".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+OK\r\n"));
        assert!(client.cache.contains_key("foo"));
        let cache_val = client.cache.get("foo").unwrap();
        assert!(cache_val.val.eq("bar"));
        assert!(cache_val.expiry_time.is_some());
        assert!(cache_val.expiry_time.unwrap() > 100);
    }

    #[test]
    fn test_get_command() {
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        client.cache.insert("foo".to_string(), CacheVal { val: "bar".to_string(), expiry_time: None });
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+bar\r\n"));
    }

    #[test]
    fn test_get_expired_command() {
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        client.cache.insert("foo".to_string(), CacheVal { val: "bar".to_string(), expiry_time: Some(500) });
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$-1\r\n"));
    }

    #[test]
    fn test_get_non_expired_command() {
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis();

        let mut client = Client::new();
        client.cache.insert("foo".to_string(), CacheVal { val: "bar".to_string(), expiry_time: Some(now + 60000) });
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+bar\r\n"));
    }

    #[test]
    fn test_null_get_command() {
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("$-1\r\n"));
    }

    #[test]
    fn test_ping_command() {
        let cmds = vec![RespType::String("PING".to_string())];
        let cmd = RespType::Array(cmds);
        let mut client = Client::new();
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+PONG\r\n"));
    }

    #[test]
    fn test_echo_command() {
        let cmds = vec![
            RespType::String("ECHO".to_string()),
            RespType::String("hello".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("+hello\r\n"));
    }

    #[test]
    fn test_rpush_command() {
        let cmds = vec![
            RespType::String("RPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("foo".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":1\r\n"));
        assert!(client.lists.get("list_key".into()).unwrap().len() == 1);
        assert!(client.lists.get("list_key".into()).unwrap().get(0).unwrap().eq("foo"));

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
        assert!(client.lists.get("list_key".into()).unwrap().len() == 2);
        assert!(client.lists.get("list_key".into()).unwrap().get(1).unwrap().eq("bar"));
    }

    #[test]
    fn test_multi_rpush_command() {
        let cmds = vec![
            RespType::String("RPUSH".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("bar".to_string()),
            RespType::String("baz".to_string()),
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq(":3\r\n"));
        assert!(client.lists.get("list_key".into()).unwrap().len() == 3);
        assert!(client.lists.get("list_key".into()).unwrap().get(0).unwrap().eq("foo"));
    }

    #[test]
    fn test_lrange_command() {
        let cmds = vec![
            RespType::String("LRANGE".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("2".to_string()),
            RespType::String("4".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        client.lists.insert("list_key".into(), vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()]);
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"));
    }

    #[test]
    fn test_empty_lrange_command() {
        let cmds = vec![
            RespType::String("LRANGE".to_string()),
            RespType::String("list_key".to_string()),
            RespType::String("2".to_string()),
            RespType::String("4".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        let res = client.handle_command(cmd);
        assert!(res.is_some());
        let value = res.unwrap();
        assert!(value.eq("*0\r\n"));
    }
}