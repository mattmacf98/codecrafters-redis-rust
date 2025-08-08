use std::slice::Iter;
use std::sync::{Arc, Mutex};

use crate::{commands::RedisCommand, redis::{create_int_resp, create_simple_string_resp}, resp::types::RespType};

pub struct WaitCommand {
    num_replicas: usize,
    timeout_ms: u128,
    ack_replicas: Arc<Mutex<usize>>
}

impl WaitCommand {
    pub fn new(num_replicas: usize, timeout_ms: u128, ack_replicas: Arc<Mutex<usize>>) -> Self {
        WaitCommand { num_replicas, timeout_ms, ack_replicas }
    }
}

impl RedisCommand for WaitCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let expiration = now + self.timeout_ms;

        loop {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis();
            let ack_replica_gaurd = self.ack_replicas.lock().unwrap();

            if now > expiration || *ack_replica_gaurd >= self.num_replicas {
                println!("SENDING WAIT {}",ack_replica_gaurd);
                return vec![create_int_resp(ack_replica_gaurd)];
            }
        }
    }
}