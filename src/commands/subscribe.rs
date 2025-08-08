use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, resp::{create_array_resp, create_bulk_string_resp, create_int_resp}, resp::types::RespType};

pub struct SubscribeCommand {
    id: String,
    channel: String,
    channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    num_subscribed_channels: i64
}

impl SubscribeCommand {
    pub fn new(id: String, channel: String, channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>, num_subscribed_channels: i64) -> Self {
        SubscribeCommand { id, channel, channel_to_subscribers, num_subscribed_channels }
    }
}

impl RedisCommand for SubscribeCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let mut channel_to_subscribers_gaurd = self.channel_to_subscribers.lock().unwrap();
        match channel_to_subscribers_gaurd.get_mut(self.channel.as_str()) {
            Some(subs) => {
                subs.push(self.id.clone());
            },
            _ => {
                channel_to_subscribers_gaurd.insert(self.channel.to_string(), vec![self.id.clone()]);
            }
        }
        return vec![create_array_resp(vec![create_bulk_string_resp("subscribe".into()), create_bulk_string_resp(self.channel.to_string().into()), create_int_resp(self.num_subscribed_channels)])];
    }
}