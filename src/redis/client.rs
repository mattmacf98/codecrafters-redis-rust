use std::collections::HashMap;

use crate::{redis::{create_bulk_string_resp, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};


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
                for i in 0..resp_types.len() {
                    if let RespType::String(s) = resp_types.get(i).unwrap() {
                        let command = s.to_lowercase();

                        // PING
                        if command.eq("ping") {
                            return Some(create_simple_string_resp(String::from("PONG")));
                        }

                        // ECHO
                        if command.eq("echo") && i < resp_types.len() - 1 {
                            if let RespType::String(message) = resp_types.get(i + 1).unwrap() {
                                return Some(create_simple_string_resp(message.to_string()));
                            }
                        }

                        // SET
                        if command.eq("set") {
                            return self.handle_set(i, &resp_types);
                        }

                        // GET
                        if command.eq("get") {
                            return self.handle_get(i, &resp_types);
                        } 

                        if command.eq("rpush") {
                            self.handle_rpush(i, &resp_types);
                            return Some(create_int_resp(1));
                        }
                    }
                }
            },
            _ => panic!("unhandled type")
        }

        None
    }

    fn handle_rpush(&mut self, i: usize, resp_types: &Vec<RespType>) {
        if i + 2 < resp_types.len() {
            match (resp_types.get(i + 1).unwrap(), resp_types.get(i + 2).unwrap()) {
                (RespType::String(list_key), RespType::String(val)) => {
                    if !self.lists.contains_key(list_key) {
                        self.lists.insert(list_key.into(), vec![]);
                    }
                    
                    self.lists.get_mut(list_key.into()).unwrap().push(val.into())
                },
                (_,_) => {
                    panic!("INVALID RPUSH")
                }
            }
        }
    }

    fn handle_set(&mut self, i: usize, resp_types: &Vec<RespType>) -> Option<String> {
        if i + 4 < resp_types.len() {
            match (resp_types.get(i + 1).unwrap(), resp_types.get(i + 2).unwrap(), resp_types.get(i + 3).unwrap(), resp_types.get(i + 4).unwrap()) {
                (RespType::String(key), RespType::String(val), px, exp ) => {
                    match (px, exp) {
                        (RespType::String(px), RespType::String(exp)) => {
                            if px.to_lowercase().eq("px") {
                                if let Ok(exp_millis) = exp.parse::<u128>() {
                                    let expiry = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis() + exp_millis;
                                    self.cache.insert(key.clone(), CacheVal { val: val.to_string(), expiry_time: Some(expiry) });
                                    return Some(create_simple_string_resp("OK".to_string()))
                                } else {
                                    panic!("Invalid expiry time")
                                }
                            } else {
                                self.cache.insert(key.clone(), CacheVal { val: val.to_string(), expiry_time: None });
                                return Some(create_simple_string_resp("OK".to_string()))
                            }
                        }
                        (_,_) => {
                            self.cache.insert(key.clone(), CacheVal { val: val.to_string(), expiry_time: None });
                            return Some(create_simple_string_resp("OK".to_string()))
                        }
                    }
                },
                (_, _, _ ,_) => panic!("INVALID SET COMMANDS")
            }
        } else if i + 2 < resp_types.len() {
            match (resp_types.get(i + 1).unwrap(), resp_types.get(i + 2).unwrap()) {
                (RespType::String(key), RespType::String(val)) => {
                    self.cache.insert(key.clone(), CacheVal { val: val.to_string(), expiry_time: None });
                    return Some(create_simple_string_resp("OK".to_string()))
                },
                (_, _) => panic!("INVALID SET COMMANDS")
            }
        }

        None
    }

    fn handle_get(&self, i: usize, resp_types: &Vec<RespType>) -> Option<String> {
        if i + 1 < resp_types.len() {
            if let RespType::String(key) = resp_types.get(i + 1).unwrap() {
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
        }

        None
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
    }
}