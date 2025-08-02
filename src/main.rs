#![allow(unused_imports)]
use std::{io::{Read, Write}, net::{TcpListener, TcpStream}, thread};

use crate::resp::types::RespType;

pub mod resp;

fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    println!("Logs from your program will appear here!");
    
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move || {
                    handle_client(stream);
                });
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn handle_client(mut stream: TcpStream) {
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
                match res.0 {
                    RespType::String(s) => {
                        if s.to_lowercase().eq("ping") {
                            stream.write_all(b"+PONG\r\n").unwrap();
                        }
                    },
                    RespType::Array(resp_types) => {
                        for i in 0..resp_types.len() {
                            if let RespType::String(s) = resp_types.get(i).unwrap() {
                                if s.to_lowercase().eq("ping") {
                                    stream.write_all(b"+PONG\r\n").unwrap();
                                }
                                if s.to_lowercase().eq("echo") && i < resp_types.len() - 1 {
                                    if let RespType::String(message) = resp_types.get(i + 1).unwrap() {
                                        stream.write_all(format!("+{}\r\n", message).as_bytes()).unwrap();
                                    }
                                }
                            }
                        }
                    },
                    _ => panic!("unhandled type")
                }
            },
            Err(e) => panic!("ERROR {:?}", e),
        };
    }
}
