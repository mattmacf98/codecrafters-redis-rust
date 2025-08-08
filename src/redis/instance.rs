use std::{collections::HashMap, io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex}, thread};

use bytes::BytesMut;

use crate::{redis::{client::{self, CacheVal, Client}, create_array_resp, create_bulk_string_resp}, resp::{rdb::Rdb, types::RespType}};
pub struct Instance {
    is_master: bool,
    rdb_dir: String,
    rdb_file: String,
    replica_of: Option<String>,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>,
    channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    write_commands: Arc<Mutex<Vec<String>>>,
    replica_streams: Arc<Mutex<Vec<TcpStream>>>,
    ack_replicas: Arc<Mutex<usize>>
}

impl Instance {
    pub fn new(rdb_dir: String, rdb_file: String, replica_of: Option<String>) -> Self {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let rdb_data = std::fs::read(format!("{}/{}", rdb_dir, rdb_file));
        match rdb_data {
            Ok(data) => {
                let rdb = Rdb::new(BytesMut::from(&data[..]));
                rdb.apply_to_db(cache.clone());
            }
            Err(e) => println!("Error reading RDB file: {} treat as empty", e)
        }

        Instance {
             is_master: replica_of.is_none(),
             replica_of: replica_of,
             rdb_dir: rdb_dir,
             rdb_file: rdb_file,
             channel_to_subscribers: Arc::new(Mutex::new(HashMap::new())),
             replica_streams: Arc::new(Mutex::new(vec![])),
             cache: cache,
             ack_replicas: Arc::new(Mutex::new(0)),
             write_commands: Arc::new(Mutex::new(vec![]))
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
                    let mut client = Client::new(
                        self.cache.clone(), self.write_commands.clone(), self.replica_streams.clone(), 
                        self.ack_replicas.clone(),  self.replica_of.clone(), self.channel_to_subscribers.clone(), self.rdb_dir.clone(), self.rdb_file.clone()
                    );
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

        let client = Client::new(self.cache.clone(), self.write_commands.clone(), self.replica_streams.clone(), self.ack_replicas.clone(),
          self.replica_of.clone(), self.channel_to_subscribers.clone(), self.rdb_dir.clone(), self.rdb_file.clone());
        thread::spawn(move || {
            Self::handle_master_connection(master_stream, client);
        });
    }

    fn handle_master_connection(mut stream: TcpStream, mut client: Client) {
        let mut master_bytes_consumed = 0;
        loop {
            println!("LOOP");

            let mut buf = [0; 512];
            let read_count = stream.read(&mut buf).unwrap();
            if read_count == 0 {
                break;
            }
            let buffer = bytes::BytesMut::from(&buf[..read_count]);
            println!("received client: {}", String::from_utf8_lossy(&buffer));
            let mut cur = 0;
            while cur < buffer.len() {
                let resp_res = RespType::parse(&buffer, cur);
                match resp_res {
                    Ok(res) => {
                        println!("CLIENT EXECUTING: {:?}", &res.0);
                        
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