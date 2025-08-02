#![allow(unused_imports)]
use std::{io::{Read, Write}, net::{TcpListener, TcpStream}, thread};

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

        stream.write_all(b"+PONG\r\n").unwrap();
    }
}
