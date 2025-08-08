use std::{collections::HashMap, io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex}, thread};

use bytes::BytesMut;

use crate::{instance::Instance, redis::client::{CacheVal, Client}, rdb::rdb::Rdb, resp::{create_array_resp, create_bulk_string_resp, types::RespType}};

pub struct ReplicaInstance {
    port: String,
    rdb_dir: String,
    rdb_file: String,
    replica_of: Option<String>,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>,
    channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    client_to_stream: Arc<Mutex<HashMap<String, TcpStream>>>,
    write_commands: Arc<Mutex<Vec<String>>>
}

impl ReplicaInstance {
    pub fn new(port: String, rdb_dir: String, rdb_file: String, replica_of: Option<String>) -> Self {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let rdb_data = std::fs::read(format!("{}/{}", rdb_dir, rdb_file));
        match rdb_data {
            Ok(data) => {
                let rdb = Rdb::new(BytesMut::from(&data[..]));
                rdb.apply_to_db(cache.clone());
            }
            Err(e) => println!("Error reading RDB file: {} treat as empty", e)
        }

        ReplicaInstance { 
            port, rdb_dir, rdb_file, replica_of, cache,  
            channel_to_subscribers: Arc::new(Mutex::new(HashMap::new())), 
            client_to_stream: Arc::new(Mutex::new(HashMap::new())), 
            write_commands: Arc::new(Mutex::new(vec![])) 
        }
    }

    fn handle_replica_handshake(&self, port: String) -> TcpStream {
        let master_instance_parts: Vec<String> = self.replica_of.clone().expect("should have master").clone().split(" ").map(String::from).collect();
        let mut master_stream = TcpStream::connect(format!("{}:{}", master_instance_parts[0].clone(), master_instance_parts[1].clone())).unwrap();

        master_stream = Self::call_and_wait_for_response(master_stream, create_array_resp(vec![create_bulk_string_resp("PING".into())]));
        master_stream = Self::call_and_wait_for_response(master_stream, create_array_resp(vec![create_bulk_string_resp("REPLCONF".into()), create_bulk_string_resp("listening-port".into()), create_bulk_string_resp(port.to_string())]));
        master_stream = Self::call_and_wait_for_response(master_stream, create_array_resp(vec![create_bulk_string_resp("REPLCONF".into()), create_bulk_string_resp("capa".into()), create_bulk_string_resp("psync2".into())]));

        let psync_message = create_array_resp(vec![create_bulk_string_resp("PSYNC".into()), create_bulk_string_resp("?".into()), create_bulk_string_resp("-1".into())]);
        master_stream.write_all(psync_message.as_bytes()).unwrap();
        master_stream
    }

    fn call_and_wait_for_response(mut stream: TcpStream, message: String) -> TcpStream {
        stream.write_all(message.as_bytes()).unwrap();
        let mut response_buf = [0; 512];
        stream.read(&mut response_buf).unwrap();
        stream
    }

    fn handle_master_connection(mut stream: TcpStream, mut client: Client) {
        let mut master_bytes_consumed = 0;
        loop {
            let mut buf = [0; 512];
            let read_count = stream.read(&mut buf).unwrap();
            if read_count == 0 {
                break;
            }
            let buffer = bytes::BytesMut::from(&buf[..read_count]);
            let mut cur = 0;
            while cur < buffer.len() {
                let resp_res = RespType::parse(&buffer, cur);
                match resp_res {
                    Ok(res) => {
                        let commands = client.handle_command(res.0.clone());
                        cur = res.1;
                        for command in commands.iter() {
                            if command.eq("SEND_REPLCONF_ACK") {
                                stream.write_all(create_array_resp(vec![create_bulk_string_resp("REPLCONF".into()), create_bulk_string_resp("ACK".into()), create_bulk_string_resp((master_bytes_consumed).to_string())]).as_bytes()).unwrap();
                            }
                        }
                        if let RespType::Array(arr) = &res.0 {
                            let num_bytes = RespType::Array(arr.clone()).to_string().as_bytes().len();
                            master_bytes_consumed += num_bytes;
                            println!("Replica has consumed {} bytes from master", master_bytes_consumed);
                        }
                    },
                    Err(e) => {
                        println!("ERR: {:?}", e);
                        cur += 1;
                    },
                };
                println!("{} CONSUMED OUT OF {}", cur, buffer.len());
            }
        }
    }
}

impl Instance for ReplicaInstance {
    fn start(&self) {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port)).unwrap();
        println!("Logs from your program will appear here!");
        println!("Starting Redis server on port {}", self.port);

        // Create special stream with master
        let master_stream = self.handle_replica_handshake(self.port.clone());
        let client = Client::new(self.cache.clone(), self.write_commands.clone(), Arc::new(Mutex::new(vec![])), Arc::new(Mutex::new(0)), self.replica_of.clone(), self.channel_to_subscribers.clone(), self.client_to_stream.clone(), self.rdb_dir.clone(), self.rdb_file.clone());
        thread::spawn(move || {
            Self::handle_master_connection(master_stream, client);
        });

        //handle normal client connections
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("accepted new connection");
                    let mut client = Client::new(
                        self.cache.clone(), self.write_commands.clone(), Arc::new(Mutex::new(vec![])), 
                        Arc::new(Mutex::new(0)),  self.replica_of.clone(), self.channel_to_subscribers.clone(), 
                        self.client_to_stream.clone(), self.rdb_dir.clone(), self.rdb_file.clone()
                    );
                    self.client_to_stream.lock().unwrap().insert(client.id.clone(), stream.try_clone().unwrap());
                    thread::spawn(move || {
                        client.handle_connection(stream);
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}