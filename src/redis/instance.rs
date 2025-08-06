use std::{collections::HashMap, io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex}, thread};

use crate::{redis::{client::{CacheVal, Client}, create_array_resp, create_bulk_string_resp}, resp::types::RespType};
pub struct Instance {
    is_master: bool,
    replica_of: Option<String>,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>,
    replica_streams: Arc<Mutex<Vec<TcpStream>>>
}

impl Instance {
    pub fn new(replica_of: Option<String>) -> Self {
        Instance {
             is_master: replica_of.is_none(),
             replica_of: replica_of,
             cache: Arc::new(Mutex::new(HashMap::new())),
             replica_streams: Arc::new(Mutex::new(vec![]))
        }
    }    

    pub fn start(&self, port: String) {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", port)).unwrap();
        println!("Logs from your program will appear here!");
        println!("Starting Redis server on port {}", port);

        if !self.is_master {
            self.handle_replica_handshake(port);
        }

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("accepted new connection");
                    let client = Client::new(self.cache.clone(), self.replica_of.clone());
                    thread::spawn(move || {
                        Self::handle_client(stream, client);
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }

    fn handle_replica_handshake(&self, port: String) {
        let master_instance_parts: Vec<String> = self.replica_of.clone().expect("should have master").clone().split(" ").map(String::from).collect();
        let mut master_stream = TcpStream::connect(format!("{}:{}", master_instance_parts[0].clone(), master_instance_parts[1].clone())).unwrap();
        
        let ping_message = create_array_resp(vec![create_bulk_string_resp("PING".into())]);
        master_stream.write_all(ping_message.as_bytes()).unwrap();

        let mut response_buf = [0; 512];
        master_stream.read(&mut response_buf).unwrap();
        println!("Received from master: {}", String::from_utf8_lossy(&response_buf));

        let replconf_message_one = create_array_resp(vec![create_bulk_string_resp("REPLCONF".into()), create_bulk_string_resp("listening-port".into()), create_bulk_string_resp(port.to_string())]);
        master_stream.write_all(replconf_message_one.as_bytes()).unwrap();

        let mut response_buf = [0; 512];
        master_stream.read(&mut response_buf).unwrap();
        println!("Received from master: {}", String::from_utf8_lossy(&response_buf));

        let replconf_message_two = create_array_resp(vec![create_bulk_string_resp("REPLCONF".into()), create_bulk_string_resp("capa".into()), create_bulk_string_resp("psync2".into())]);
        master_stream.write_all(replconf_message_two.as_bytes()).unwrap();

        let mut response_buf = [0; 512];
        master_stream.read(&mut response_buf).unwrap();
        println!("Received from master: {}", String::from_utf8_lossy(&response_buf));

        let psync_message = create_array_resp(vec![create_bulk_string_resp("PSYNC".into()), create_bulk_string_resp("?".into()), create_bulk_string_resp("-1".into())]);
        master_stream.write_all(psync_message.as_bytes()).unwrap();

        let mut response_buf = [0; 512];
        master_stream.read(&mut response_buf).unwrap();
        println!("Received from master: {}", String::from_utf8_lossy(&response_buf));
    }

    fn handle_client(mut stream: TcpStream, mut client: Client) {
        loop {
            let mut buf = [0; 512];
            let read_count = stream.read(&mut buf).unwrap();
            if read_count == 0 {
                break;
            }
            let buffer = bytes::BytesMut::from(&buf[..read_count]);
            println!("received: {}", String::from_utf8_lossy(&buffer));
            let resp_res = RespType::parse(&buffer, 0);
            match resp_res {
                Ok(res) => {
                    let commands = client.handle_command(res.0);
                    for command in commands {
                        if command.eq("EMPTY_RDB") {
                            let file = include_bytes!("../../empty.rdb");
                            stream.write_all(format!("${}\r\n", file.len()).as_bytes()).unwrap();
                            stream.write_all(file).unwrap();
                        } else {
                            stream.write_all(command.as_bytes()).unwrap();
                        }
                    }
                },
                Err(e) => panic!("ERROR {:?}", e),
            };
        }
    }
}