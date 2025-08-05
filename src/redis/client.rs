use core::num;
use std::{collections::HashMap, fmt::format, io::Write, net::TcpStream, slice::Iter, str::FromStr, sync::{Arc, Mutex}};

use tokio::stream;

use crate::{commands::{blpop::BlpopCommand, echo::EchoCommand, get::GetCommand, incr::IncrCommand, info::InfoCommand, llen::LlenCommand, lpop::LpopCommand, lpush::LpushCommand, lrange::LrangeCommand, ping::PingCommand, psync::PsyncCommand, replconf::ReplConfCommand, rpush::RpushCommand, set::SetCommand, type_command::TypeCommand, xadd::XaddCommand, xrange::XrangeCommand, xread::XreadCommand, RedisCommand}, redis::{create_array_resp, create_basic_err_resp, create_bulk_string_resp, create_int_resp, create_null_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub enum CacheVal {
    String(StringCacheVal),
    List(ListCacheVal),
    Stream(StreamCacheVal)
}
pub struct StringCacheVal {
    pub(crate) val: String,
    pub(crate) expiry_time: Option<u128>
}

pub struct ListCacheVal {
    pub(crate) list: Vec<String>,
    pub(crate) block_queue: Vec<String>
}

pub struct StreamCacheVal {
    pub(crate) stream: Vec<StreamItem>
}

#[derive(Clone)]
pub struct KeyVal {
    pub(crate) key: String,
    pub(crate) val: String
}

#[derive(Clone)]
pub struct StreamItem {
    pub(crate) id: String,
    pub(crate) key_vals: Vec<KeyVal>
}
pub struct Client {
    id: String,
    replica_of: Option<String>,
    master_repl_id: Option<String>,
    master_repl_offset: Option<u128>,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>,
    staged_commands: Vec<RespType>,
    staging_commands: bool
}

impl Client {
    pub fn new(cache: Arc<Mutex<HashMap<String, CacheVal>>>, replica_of: Option<String>) -> Self {

        let mut master_repl_id = None;
        let mut master_repl_offset = None;
        if replica_of.is_none() {
            //indicating this is a master instance
            master_repl_id = Some("8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".into());
            master_repl_offset = Some(0);
        }

        Client {
            id: uuid::Uuid::new_v4().to_string(),
            replica_of: replica_of,
            staged_commands: vec![],
            master_repl_offset: master_repl_offset,
            master_repl_id: master_repl_id,
            staging_commands: false,
            cache: cache
        }
    }

    pub fn handle_command(&mut self, cmd: RespType) -> String {

        match cmd {
            RespType::Array(resp_types) => {
                let mut iter = resp_types.iter();

                if let RespType::String(s) = iter.next().unwrap() {
                    let command = s.to_lowercase();

                    if self.staging_commands && command.ne("exec") && command.ne("discard") {
                        // only stage commands here
                        let cmd_clone = RespType::Array(resp_types.clone());
                        self.staged_commands.push(cmd_clone);
                        return create_simple_string_resp("QUEUED".into());
                    }

                    match command.as_str() {
                        "psync" => {
                            let redis_command = PsyncCommand::new(self.master_repl_id.clone().expect("master should have id"), self.master_repl_offset.clone().expect("master should have offset"));
                            return redis_command.execute(&mut iter);
                        },
                        "info" => {
                            let role = if self.replica_of.is_none() {"master"} else {"slave"};
                            let redis_command = InfoCommand::new(role.to_string(), self.master_repl_id.clone(), self.master_repl_offset.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "replconf" => {
                            let redis_command = ReplConfCommand::new();
                            return redis_command.execute(&mut iter);
                        }
                        "multi" => {
                            self.staging_commands = true;
                            return create_simple_string_resp("OK".into());
                        },
                        "discard" => {
                            if self.staging_commands {
                                self.staging_commands = false;
                                self.staged_commands.clear();
                                return create_simple_string_resp("OK".into());
                            } else {
                                return create_basic_err_resp("ERR DISCARD without MULTI".to_string());
                            }
                        }
                        "exec" => {
                            if self.staging_commands {
                                self.staging_commands = false;
                                let mut results = vec![];
                                for staged_command in self.staged_commands.clone() {
                                    let res = self.handle_command(staged_command.clone());
                                    results.push(res);
                                }
                                self.staged_commands.clear();
                                return create_array_resp(results);
                            } else {
                                return create_basic_err_resp("ERR EXEC without MULTI".to_string());
                            }
                        }
                        "ping" => {
                            let redis_command = PingCommand::new();
                            return redis_command.execute(&mut iter);
                        },
                        "echo" => {
                            let message = match iter.next().expect("Should have echo message") {
                                RespType::String(message) => message,
                                _ => panic!("Echo command expects a string message")
                            };
                            let redis_command = EchoCommand::new(message.to_string());
                            return redis_command.execute(&mut iter);
                        },
                        "get" => {
                            let key = match iter.next().expect("Should have key") {
                                RespType::String(key) => key,
                                _ => panic!("Get command expects a string key")
                            };
                            let redis_command = GetCommand::new(key.to_string(), self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "set" => {
                            let (key, value) = match (iter.next().expect("Should have key"), iter.next().expect("Should have value")) {
                                (RespType::String(key), RespType::String(value)) => (key,value),
                                _ => panic!("Set command expects a string key and value")
                            };
                            let expire: Option<u128> = match (iter.next(), iter.next()) {
                                (Some(RespType::String(px)), Some(RespType::String(exp))) if px.to_lowercase().eq("px") => {
                                    if let Ok(exp_millis) = exp.parse::<u128>() {
                                        Some(exp_millis)
                                    } else {
                                        panic!("Invalid expiry time")
                                    }
                                },
                                _ => None
                            };
                            let redis_command = SetCommand::new(key.to_string(), value.to_string(), expire, self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "rpush" => {
                            let list_key = match iter.next().expect("Should have list key") {
                                RespType::String(list_key) => list_key,
                                _ => panic!("RPUSH command expects a list key")
                            };
                            let redis_command = RpushCommand::new(list_key.to_string(), self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "lpush" => {
                            let list_key = match iter.next().expect("Should have list key") {
                                RespType::String(list_key) => list_key,
                                _ => panic!("LPUSH command expects a list key")
                            };
                            let redis_command = LpushCommand::new(list_key.to_string(), self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "lrange" => {
                            let list_key = match iter.next().expect("Should have list key") {
                                RespType::String(list_key) => list_key,
                                _ => panic!("LRANGE command expects a list key")
                            };
                            let start = match Self::extract_num(&mut iter) {
                                Some(val) => val,
                                None => panic!("LRANGE could not parse start int"),
                            };
                            let end = match Self::extract_num(&mut iter) {
                                Some(val) => val,
                                None => panic!("LRANGE could not parse end int"),
                            };

                            let redis_command = LrangeCommand::new(list_key.to_string(), start, end, self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "llen" => {
                            let list_key = match iter.next().expect("Should have list key") {
                                RespType::String(list_key) => list_key,
                                _ => panic!("LLEN command expects a list key")
                            };
                            let redis_command = LlenCommand::new(list_key.to_string(), self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "lpop" => {
                            let list_key = match iter.next().expect("Should have list key") {
                                RespType::String(list_key) => list_key,
                                _ => panic!("LPOP command expects a list key")
                            };
                            let count = match Self::extract_num(&mut iter) {
                                Some(val) => Some(val),
                                None => None
                            };
                            let redis_command = LpopCommand::new(list_key.to_string(), count, self.cache.clone());
                            return redis_command.execute(&mut iter);
                        }
                        "blpop" => {
                            let list_key = match iter.next().expect("Should have list key") {
                                RespType::String(list_key) => list_key,
                                _ => panic!("BLPOP command expects a list key")
                            };
                            let timeout = match Self::extract_num(&mut iter) {
                                Some(val) => val,
                                None => panic!("BLPOP could not parse timeout float"),
                            };
                            let redis_command = BlpopCommand::new(list_key.to_string(), self.id.clone(), timeout, self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "type" => {
                            let key = match iter.next().expect("Should have key") {
                                RespType::String(key) => key,
                                _ => panic!("TYPE command expects a key")
                            };
                            let redis_command = TypeCommand::new(key.to_string(), self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "xadd" => {
                            let stream_key = match iter.next().expect("Should have stream key") {
                                RespType::String(stream_key) => stream_key,
                                _ => panic!("XADD command expects a stream_key")
                            };
                            let entry_id = match iter.next().expect("Should have stream entry_id") {
                                RespType::String(stream_key) => stream_key,
                                _ => panic!("XADD command expects a entry_id")
                            };
                            let redis_command = XaddCommand::new(stream_key.to_string(), entry_id.to_string(), self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "xrange" => {
                            let stream_key = match iter.next().expect("Should have stream key") {
                                RespType::String(stream_key) => stream_key,
                                _ => panic!("XRANGE command expects a stream_key")
                            };
                            let start_id = match iter.next().expect("Should have stream start_id") {
                                RespType::String(stream_key) => stream_key,
                                _ => panic!("XRANGE command expects a start_id")
                            };
                            let end_id = match iter.next().expect("Should have stream end_id") {
                                RespType::String(stream_key) => stream_key,
                                _ => panic!("XRANGE command expects a end_id")
                            };
                            let redis_command = XrangeCommand::new(stream_key.to_string(), start_id.to_string(), end_id.to_string(), self.cache.clone());
                            return redis_command.execute(&mut iter);
                        },
                        "xread" => {
                            let mut timeout_ms = None;
                            let mut keywords_consumed = 1;
                            loop {
                                let keyword = match iter.next().expect("Should have a keyword") {
                                    RespType::String(keyword) => keyword,
                                    _ => panic!("XREAD command expects a keyword first")
                                };
                                keywords_consumed += 1;

                                match keyword.as_str() {
                                    "streams" => break,
                                    "block" => {
                                        timeout_ms = match Self::extract_num(&mut iter) {
                                            Some(val) => Some(val),
                                            None => panic!("expected timeout after block keyword")
                                        };
                                        keywords_consumed += 1
                                    }
                                    _ => panic!("Unrecognized XREAD keyword")
                                }
                            }
                            

                            let num_streams = (resp_types.len() - keywords_consumed)/ 2;
                            let mut stream_keys = vec![];
                            let mut start_ids = vec![];
                            for _ in 0..num_streams {
                                let stream_key = match iter.next().expect("Should have stream key") {
                                    RespType::String(stream_key) => stream_key,
                                    _ => panic!("XREAD command expects a stream_key")
                                };
                                stream_keys.push(stream_key);
                            }
                            
                            for _ in 0..num_streams { 
                                let start_id = match iter.next().expect("Should have stream start_id") {
                                    RespType::String(stream_key) => stream_key,
                                    _ => panic!("XREAD command expects a start_id")
                                };
                                start_ids.push(start_id);
                            } 
                            
                            let mut stream_responses = vec![];

                            for i in 0..num_streams {
                                let redis_command = XreadCommand::new(stream_keys[i].to_string(), timeout_ms, start_ids[i].to_string(), self.cache.clone());
                                stream_responses.push(redis_command.execute(&mut iter));
                            }
                            
                            if stream_responses.len() == 1 && stream_responses[0].clone().eq(&create_null_bulk_string_resp()) {
                                // this is pretty bad spec design by redis to expect a null bulk string if timeout but an array if success, I would have just had it return an empty array or the null bulk string in an array
                                return create_null_bulk_string_resp();
                            } else {
                                return create_array_resp(stream_responses);
                            }
                        }
                        "incr" => {
                            let key = match iter.next().expect("Should have key") {
                                RespType::String(key) => key,
                                _ => panic!("INCR command expects a string key")
                            };
                            let redis_command = IncrCommand::new(key.to_string(), self.cache.clone());
                            return redis_command.execute(&mut iter);
                        }
                        _ => panic!("UNEXPECTED COMMAND")
                    }
                } else {
                    panic!("UNEXPECTED ARRAY ENTRY")
                } 
            },
            _ => panic!("ONLY EXPECTING ARRAY COMMANDS")
        }
    }

    fn extract_num<T>(iter: &mut Iter<'_, RespType>) -> Option<T> where T: FromStr {
        match iter.next() {
            Some(RespType::String(int_str)) => {
                match int_str.parse::<T>() {
                    Ok(val) => Some(val),
                    Err(_) => None
                }
            }
            _ => None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone(), None);

        let cmds = vec![
            RespType::String("MULTI".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.eq("+OK\r\n"));

        let cmds = vec![
            RespType::String("SET".to_string()),
            RespType::String("foo".to_string()),
            RespType::String("41".to_string()),
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq("+QUEUED\r\n"));

        let cmds = vec![
            RespType::String("INCR".to_string()),
            RespType::String("foo".to_string()),
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq("+QUEUED\r\n"));

        let cmds = vec![
            RespType::String("EXEC".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq("*2\r\n+OK\r\n:42\r\n"));
    }

    #[test]
    fn test_incr_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone(), None);

        let cmds = vec![
            RespType::String("INCR".to_string()),
            RespType::String("key".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.eq(":1\r\n"));

        let cmds = vec![
            RespType::String("INCR".to_string()),
            RespType::String("key".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq(":2\r\n"));
    }

    #[test]
    fn test_multi_stream_xread_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone(), None);

        {
            let mut cache_guard = cache.lock().unwrap();
            let stream_item_one = StreamItem {id: "0-1".into(), key_vals: vec![KeyVal {key: "foo".to_string(), val: "bar".to_string()} ]};
            let stream_item_two = StreamItem {id: "0-2".into(), key_vals: vec![KeyVal {key: "bar".to_string(), val: "baz".to_string()} ]};
            let stream_item_three = StreamItem {id: "0-3".into(), key_vals: vec![KeyVal {key: "baz".to_string(), val: "foo".to_string()} ]};
            cache_guard.insert("stream_key".to_string(), CacheVal::Stream(StreamCacheVal { stream: vec![stream_item_one.clone(), stream_item_two.clone(), stream_item_three.clone()] }));
            cache_guard.insert("other_stream_key".to_string(), CacheVal::Stream(StreamCacheVal { stream: vec![stream_item_one, stream_item_two, stream_item_three] }));
        }
        
        let cmds = vec![
            RespType::String("XREAD".to_string()),
            RespType::String("streams".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("other_stream_key".to_string()),
            RespType::String("0-2".to_string()),
            RespType::String("0-1".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.eq("*2\r\n*2\r\n$10\r\nstream_key\r\n*1\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n*2\r\n$16\r\nother_stream_key\r\n*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n"));
    }

    #[test]
    fn test_xread_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone(), None);

        {
            let mut cache_guard = cache.lock().unwrap();
            let stream_item_one = StreamItem {id: "0-1".into(), key_vals: vec![KeyVal {key: "foo".to_string(), val: "bar".to_string()} ]};
            let stream_item_two = StreamItem {id: "0-2".into(), key_vals: vec![KeyVal {key: "bar".to_string(), val: "baz".to_string()} ]};
            let stream_item_three = StreamItem {id: "0-3".into(), key_vals: vec![KeyVal {key: "baz".to_string(), val: "foo".to_string()} ]};
            cache_guard.insert("stream_key".to_string(), CacheVal::Stream(StreamCacheVal { stream: vec![stream_item_one, stream_item_two, stream_item_three] }));
        }
        
        let cmds = vec![
            RespType::String("XREAD".to_string()),
            RespType::String("streams".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("0-2".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.eq("*1\r\n*2\r\n$10\r\nstream_key\r\n*1\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n"));
    }

    #[test]
    fn test_xrange_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone(), None);

        {
            let mut cache_guard = cache.lock().unwrap();
            let stream_item_one = StreamItem {id: "0-1".into(), key_vals: vec![KeyVal {key: "foo".to_string(), val: "bar".to_string()} ]};
            let stream_item_two = StreamItem {id: "0-2".into(), key_vals: vec![KeyVal {key: "bar".to_string(), val: "baz".to_string()} ]};
            let stream_item_three = StreamItem {id: "0-3".into(), key_vals: vec![KeyVal {key: "baz".to_string(), val: "foo".to_string()} ]};
            cache_guard.insert("stream_key".to_string(), CacheVal::Stream(StreamCacheVal { stream: vec![stream_item_one, stream_item_two, stream_item_three] }));
        }
        
        let cmds = vec![
            RespType::String("XRANGE".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("0-2".to_string()),
            RespType::String("0-3".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.eq("*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n"));

        let cmds = vec![
            RespType::String("XRANGE".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("-".to_string()),
            RespType::String("0-2".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.eq("*2\r\n*2\r\n$3\r\n0-1\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n"));

        let cmds = vec![
            RespType::String("XRANGE".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("0-2".to_string()),
            RespType::String("+".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let res = client.handle_command(cmd);
        assert!(res.eq("*2\r\n*2\r\n$3\r\n0-2\r\n*2\r\n$3\r\nbar\r\n$3\r\nbaz\r\n*2\r\n$3\r\n0-3\r\n*2\r\n$3\r\nbaz\r\n$3\r\nfoo\r\n"));
    }

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

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq("$15\r\n1526919030474-0\r\n"));
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
        let mut client = Client::new(cache.clone(), None);
        
        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("0-*".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq("$3\r\n0-1\r\n"));

        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("1-*".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        println!("{}", res);
        assert!(res.eq("$3\r\n1-0\r\n"));
        
        
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
        assert!(res.eq("$3\r\n2-2\r\n"));
    }

    #[test]
    fn test_xadd_command_full() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone(), None);
        
        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("*".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        let parts: Vec<&str>  = res.split("\r\n").collect();
        let id_parts: Vec<&str> = parts[1].split("-").collect();
        assert!(id_parts[1].eq("0"));
    }

    #[test]
    fn test_xadd_command_validation() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone(), None);
        
        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("0-0".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq("-ERR The ID specified in XADD must be greater than 0-0\r\n"));
        
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
        assert!(res.eq("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));

        let cmds = vec![
            RespType::String("XADD".to_string()),
            RespType::String("stream_key".to_string()),
            RespType::String("0-1".to_string()),
            RespType::String("temperature".to_string()),
            RespType::String("36".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n"));
    }

    #[test]
    fn test_type_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let mut client = Client::new(cache.clone(), None);

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
        assert!(res.eq("+string\r\n"));

        let cmds = vec![
            RespType::String("TYPE".to_string()),
            RespType::String("bar".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq("+list\r\n"));

        let cmds = vec![
            RespType::String("TYPE".to_string()),
            RespType::String("faz".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq("+stream\r\n"));

        let cmds = vec![
            RespType::String("TYPE".to_string()),
            RespType::String("other".to_string())
        ];
        let cmd = RespType::Array(cmds);
        let res = client.handle_command(cmd);
        assert!(res.eq("+none\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq("+OK\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq("+OK\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal::String(StringCacheVal { val: "bar".to_string(), expiry_time: None }));
        }
        let res = client.handle_command(cmd);
        assert!(res.eq("+bar\r\n"));
    }

    #[test]
    fn test_get_expired_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), None);
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal::String(StringCacheVal { val: "bar".to_string(), expiry_time: Some(500) }));
        }
        let res = client.handle_command(cmd);
        assert!(res.eq("$-1\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        {
            let mut cache_guard = cache.lock().unwrap();
            cache_guard.insert("foo".to_string(), CacheVal::String(StringCacheVal { val: "bar".to_string(), expiry_time: Some(now + 60000) }));
        }
        let res = client.handle_command(cmd);
        assert!(res.eq("+bar\r\n"));
    }

    #[test]
    fn test_null_get_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("GET".to_string()),
            RespType::String("foo".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq("$-1\r\n"));
    }

    #[test]
    fn test_ping_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![RespType::String("PING".to_string())];
        let cmd = RespType::Array(cmds);
        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq("+PONG\r\n"));
    }

    #[test]
    fn test_echo_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("ECHO".to_string()),
            RespType::String("hello".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq("+hello\r\n"));
    }

    #[test]
    fn test_llen_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LLEN".to_string()),
            RespType::String("list_key".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq(":0\r\n"));

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
        assert!(res.eq(":6\r\n"));
    }

    #[test]
    fn test_lpop_command() {
        let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
        let cmds = vec![
            RespType::String("LPOP".to_string()),
            RespType::String("list_key".to_string())
        ];
        let cmd = RespType::Array(cmds);

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq("$-1\r\n"));

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
        assert!(res.eq("*2\r\n$1\r\na\r\n$1\r\nb\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq(":1\r\n"));
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
        assert!(res.eq(":2\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq(":3\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq(":3\r\n"));
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
        assert!(res.eq(":4\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        {
            let mut cache_gaurd = cache.lock().unwrap();
            cache_gaurd.insert("list_key".into(), CacheVal::List(ListCacheVal {list: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()], block_queue: vec![]}));
        }
        let res = client.handle_command(cmd);
        assert!(res.eq("*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        let res = client.handle_command(cmd);
        assert!(res.eq("*0\r\n"));
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

        let mut client = Client::new(cache.clone(), None);
        {
            let mut cache_gaurd = cache.lock().unwrap();
            cache_gaurd.insert("list_key".into(), CacheVal::List(ListCacheVal {list: vec!["a".into(), "b".into(), "c".into(), "d".into(), "e".into(), "f".into()], block_queue: vec![]}));
        }
        let res = client.handle_command(cmd);
        assert!(res.eq("*3\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n"));
    }
}