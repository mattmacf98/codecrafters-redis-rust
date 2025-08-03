use std::collections::HashMap;

use crate::{redis::{create_bulk_string_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub struct Client {
    cache: HashMap<String, String>,
}

impl Client {
    pub fn new() -> Self {
        Client {
            cache: HashMap::new(),
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
                        if command.eq("set") && i < resp_types.len() - 2 {
                            match (resp_types.get(i + 1).unwrap(), resp_types.get(i + 2).unwrap()) {
                                (RespType::String(key), RespType::String(val)) => {
                                    self.cache.insert(key.clone(), val.clone());
                                    return Some(create_simple_string_resp("OK".to_string()))
                                },
                                (_, _) => panic!("INVALID SET COMMANDS")
                            }
                        }

                        // GET
                        if command.eq("get") && i < resp_types.len() - 1 {
                            if let RespType::String(key) = resp_types.get(i + 1).unwrap() {
                                let value = self.cache.get(key);
                                return match value {
                                    Some(v) => Some(create_simple_string_resp(v.to_string())),
                                    None => Some(create_null_bulk_string_resp())
                                }
                            }
                        } 
                    }
                }
            },
            _ => panic!("unhandled type")
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
        assert!(client.cache.get("foo").unwrap().eq("bar"))
    }

    #[test]
    fn test_get_command() {
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new();
        client.cache.insert("foo".to_string(), "bar".to_string());
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
}