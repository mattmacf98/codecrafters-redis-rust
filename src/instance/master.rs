use std::{collections::HashMap, io::{Read, Write}, net::TcpListener, sync::{Arc, Mutex}, thread};
use std::net::TcpStream;

use bytes::BytesMut;

use crate::{instance::Instance, rdb::rdb::Rdb, redis::client::{CacheVal, Client}, resp::{create_array_resp, create_bulk_string_resp, types::RespType}};


struct MasterStreamReplicaData {
    replica_clients: Arc<Mutex<Vec<String>>>,
    ack_replicas: Arc<Mutex<usize>>,
    write_commands: Arc<Mutex<Vec<String>>>,
    client_to_stream: Arc<Mutex<HashMap<String, TcpStream>>>
}

impl MasterStreamReplicaData {
    pub fn new(replica_clients: Arc<Mutex<Vec<String>>>, ack_replicas: Arc<Mutex<usize>>, write_commands: Arc<Mutex<Vec<String>>>, client_to_stream: Arc<Mutex<HashMap<String, TcpStream>>>) -> Self {
        Self {
            replica_clients,
            ack_replicas,
            write_commands,
            client_to_stream
        }
    }
}

pub struct MasterInstance {
    port: String,
    rdb_dir: String,
    rdb_file: String,
    cache: Arc<Mutex<HashMap<String, CacheVal>>>,
    channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    client_to_stream: Arc<Mutex<HashMap<String, TcpStream>>>,
    write_commands: Arc<Mutex<Vec<String>>>,
    replica_clients: Arc<Mutex<Vec<String>>>,
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
            replica_clients: Arc::new(Mutex::new(vec![])), 
            ack_replicas: Arc::new(Mutex::new(0)) 
        }
    }

    fn handle_client_connection(mut stream: TcpStream, mut client: Client, master_stream_replica_data: MasterStreamReplicaData) {
        loop {

            // READ THE COMMANDS FROM THE CLIENT
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

            // REGISTER THE CONNECTION AS A REPLICA CONNECTION
            if client.is_replica_connection  {
                let mut replica_clients_gaurd = master_stream_replica_data.replica_clients.lock().unwrap();
                if !replica_clients_gaurd.contains(&client.id) {
                    replica_clients_gaurd.push(client.id.clone());

                    // if there are no commands, jsut add to the acked
                    let write_commands_gaurd = master_stream_replica_data.write_commands.lock().unwrap();
                    if write_commands_gaurd.len() == 0 {
                        let mut ack_replica_gaurd = master_stream_replica_data.ack_replicas.lock().unwrap();
                        *ack_replica_gaurd += 1;
                    }
                }
            }

            // SEND THE COMMANDS TO THE REPLICAS
            let mut write_commands_gaurd = master_stream_replica_data.write_commands.lock().unwrap();
            let mut replica_clients_gaurd = master_stream_replica_data.replica_clients.lock().unwrap();
            println!("COMMANDS LEN {} REPLICAS {}", write_commands_gaurd.len(), replica_clients_gaurd.len());
            if write_commands_gaurd.len() == 0 {
                continue;
            }

            let mut ack_replica_gaurd = master_stream_replica_data.ack_replicas.lock().unwrap();
            *ack_replica_gaurd = 0;
            println!("RESET ACKS TO 0");
            for client_id in replica_clients_gaurd.iter_mut() {
                let client_to_stream_gaurd = master_stream_replica_data.client_to_stream.lock().unwrap();
                let mut replica_stream = client_to_stream_gaurd.get(client_id).unwrap();
                for command in write_commands_gaurd.iter() {
                    replica_stream.write_all(command.as_bytes()).unwrap();
                }

                let stream_clone = replica_stream.try_clone().unwrap();
                thread::spawn(move || {
                    Self::send_get_ack_request(stream_clone);
                });
            }
            write_commands_gaurd.clear();
        }
    }

    fn send_get_ack_request(mut stream: TcpStream) {
        // what is the redis actual spec about sending GET ACK? some tests expect it to send GETACK immediately after any write is sent to the master node, 
        // while others got upset and wanted to receieve all sets (coming from multiple requests) to be propagated to the slave before the GETACK is sent.
        thread::sleep(std::time::Duration::from_millis(100));
        stream.write_all(create_array_resp(vec![create_bulk_string_resp("REPLCONF".into()), create_bulk_string_resp("GETACK".into()), create_bulk_string_resp("*".into())]).as_bytes()).unwrap();
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
                    let client = Client::new(
                        self.cache.clone(), self.write_commands.clone(), 
                        self.ack_replicas.clone(),  None, self.channel_to_subscribers.clone(), 
                        self.client_to_stream.clone(), self.rdb_dir.clone(), self.rdb_file.clone()
                    );
                    self.client_to_stream.lock().unwrap().insert(client.id.clone(), stream.try_clone().unwrap());
                    let master_stream_replica_data = MasterStreamReplicaData::new(self.replica_clients.clone(), self.ack_replicas.clone(), self.write_commands.clone(), self.client_to_stream.clone());
                    thread::spawn(move || {
                        Self::handle_client_connection(stream, client, master_stream_replica_data);
                    });
                }
                Err(e) => {
                    println!("error: {}", e);
                }
            }
        }
    }
}
