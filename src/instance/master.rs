use std::{collections::HashMap, net::TcpListener, sync::{Arc, Mutex}, thread};
use std::net::TcpStream;

use bytes::BytesMut;

use crate::{instance::Instance, redis::client::{CacheVal, Client}, rdb::rdb::Rdb};

pub struct MasterInstance {
    port: String,
    rdb_dir: String,
    rdb_file: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>,
    channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    client_to_stream: Arc<Mutex<HashMap<String, TcpStream>>>,
    write_commands: Arc<Mutex<Vec<String>>>,
    replica_streams: Arc<Mutex<Vec<TcpStream>>>,
    ack_replicas: Arc<Mutex<usize>>
}

impl MasterInstance {
    pub fn new(port: String, rdb_dir: String, rdb_file: String) -> Self {
        let cache = Arc::new(Mutex::new(HashMap::new()));
        let rdb_data = std::fs::read(format!("{}/{}", rdb_dir, rdb_file));
        match rdb_data {
            Ok(data) => {
                let rdb = Rdb::new(BytesMut::from(&data[..]));
                rdb.apply_to_db(cache.clone());
            }
            Err(e) => println!("Error reading RDB file: {} treat as empty", e)
        }

        MasterInstance { 
            port, rdb_dir, rdb_file, cache, 
            channel_to_subscribers: Arc::new(Mutex::new(HashMap::new())), 
            client_to_stream: Arc::new(Mutex::new(HashMap::new())), 
            write_commands: Arc::new(Mutex::new(vec![])), 
            replica_streams: Arc::new(Mutex::new(vec![])), 
            ack_replicas: Arc::new(Mutex::new(0)) 
        }
    }
}

impl Instance for MasterInstance {
    fn start(&self) {
        let listener = TcpListener::bind(format!("127.0.0.1:{}", self.port)).unwrap();
        println!("Logs from your program will appear here!");
        println!("Starting Redis server on port {}", self.port);

        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    println!("accepted new connection");
                    let mut client = Client::new(
                        self.cache.clone(), self.write_commands.clone(), self.replica_streams.clone(), 
                        self.ack_replicas.clone(),  None, self.channel_to_subscribers.clone(), 
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
