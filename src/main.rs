#![allow(unused_imports)]
use std::{collections::HashMap, io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex}, thread};
use clap::Parser;

use crate::{redis::{client::{self, CacheVal, Client, StringCacheVal}, create_array_resp, create_bulk_string_resp, create_simple_string_resp}, resp::types::RespType};

pub mod resp;
pub mod redis;
pub mod commands;

#[derive(Parser)]
#[command(name = "codecrafters-redis")]
#[command(about = "A Redis server implementation")]
struct Args {
    /// Port to bind the server to
    #[arg(long, default_value = "6379")]
    port: u16,
    /// Replicate from another Redis server
    #[arg(long)]
    replicaof: Option<String>,
}

fn main() {
    // Parse command line arguments
    let args = Args::parse();
    
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    println!("Starting Redis server on port {}", args.port);
    
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).unwrap();
    let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));

    if args.replicaof.is_some() {
        let master_instance_parts: Vec<String> = args.replicaof.clone().expect("should have master").clone().split(" ").map(String::from).collect();
        let mut master_stream = TcpStream::connect(format!("{}:{}", master_instance_parts[0], master_instance_parts[1])).unwrap();
        let ping_message = create_array_resp(vec![create_bulk_string_resp("PING".into())]);
        master_stream.write_all(ping_message.as_bytes()).unwrap();
    }
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let client = Client::new(cache.clone(), args.replicaof.clone());
                thread::spawn(move || {
                    handle_client(stream, client);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
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
                let res = client.handle_command(res.0);
                stream.write_all(res.as_bytes()).unwrap();
            },
            Err(e) => panic!("ERROR {:?}", e),
        };
    }
}
