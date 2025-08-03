#![allow(unused_imports)]
use std::{io::{Read, Write}, net::{TcpListener, TcpStream}, sync::{Arc, Mutex}, thread};

use crate::{redis::{client::{self, Client}, create_simple_string_resp}, resp::types::RespType};

pub mod resp;
pub mod redis;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let client = Arc::new(Mutex::new(Client::new()));
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let client_clone = client.clone();
                thread::spawn(move || {
                    handle_client(stream, client_clone);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream, client: Arc<Mutex<Client>>) {
    loop {
        let mut buf = [0; 512];
        let read_count = stream.read(&mut buf).unwrap();
        if read_count == 0 {
            break;
        }
        let buffer = bytes::BytesMut::from(&buf[..read_count]);
        let resp_res = RespType::parse(&buffer, 0);
        match resp_res {
            Ok(res) => {
                let mut guard = client.lock().unwrap();
                let res = guard.handle_command(res.0);
                if let Some(message) = res {
                    stream.write_all(message.as_bytes()).unwrap();
                }
            },
            Err(e) => panic!("ERROR {:?}", e),
        };
    }
}
