use std::{collections::HashMap, io::Write, net::TcpStream, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, resp::{create_array_resp, create_bulk_string_resp, create_int_resp}, resp::types::RespType};

pub struct PublishCommand {
    channel: String,
    message: String,
    channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    client_to_stream: Arc<Mutex<HashMap<String, TcpStream>>>,
}

impl PublishCommand {
    pub fn new(channel: String, message: String, channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>, client_to_stream: Arc<Mutex<HashMap<String, TcpStream>>>) -> Self {
        PublishCommand { channel, message, channel_to_subscribers, client_to_stream }
    }
}

impl RedisCommand for PublishCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let channel_to_subscribers_gaurd = self.channel_to_subscribers.lock().unwrap();
        match channel_to_subscribers_gaurd.get(self.channel.as_str()) {
            Some(subs) => {
                for sub in subs.iter() {
                    match self.client_to_stream.lock().unwrap().get(sub) {
                        Some(mut stream) => {
                            stream.write_all(create_array_resp(vec![create_bulk_string_resp("message".into()), create_bulk_string_resp(self.channel.clone().into()), create_bulk_string_resp(self.message.clone().into())]).as_bytes()).unwrap();
                        },
                        _ => {
                            println!("SUB {} NOT FOUND", sub);
                        }
                    }
                }
                return vec![create_int_resp(subs.len() as i64)];
            },
            _ => {
                return vec![create_int_resp(0)];
            }
        }
    }
}