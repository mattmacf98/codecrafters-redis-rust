#![allow(unused_imports)]
use std::{collections::HashMap, io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex}, thread};
use clap::Parser;

use crate::{redis::{client::{self, CacheVal, Client, StringCacheVal}, create_simple_string_resp}, resp::types::RespType};

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
}

fn main() {
    // Parse command line arguments
    let args = Args::parse();
    
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    println!("Starting Redis server on port {}", args.port);
    
    let listener = TcpListener::bind(format!("127.0.0.1:{}", args.port)).unwrap();
    let cache: Arc<Mutex<HashMap<String, CacheVal>>> = Arc::new(Mutex::new(HashMap::new()));
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let client = Client::new(cache.clone());
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
