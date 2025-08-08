use std::{collections::HashMap, slice::Iter, sync::{Arc, Mutex}};

use crate::{commands::RedisCommand, redis::{create_array_resp, create_bulk_string_resp, create_int_resp, create_simple_string_resp}, resp::types::RespType};

pub struct UnsubscribeCommand {
    id: String,
    channel: String,
    channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>,
    num_subscribed_channels: i64
}

impl UnsubscribeCommand {
    pub fn new(id: String, channel: String, channel_to_subscribers: Arc<Mutex<HashMap<String, Vec<String>>>>, num_subscribed_channels: i64) -> Self {
        UnsubscribeCommand { id, channel, channel_to_subscribers, num_subscribed_channels }
    }
}

impl RedisCommand for UnsubscribeCommand {
    fn execute(&self, _: &mut Iter<'_, RespType>) -> Vec<String> {
        let mut channel_to_subscribers_gaurd = self.channel_to_subscribers.lock().unwrap();
        match channel_to_subscribers_gaurd.get_mut(self.channel.as_str()) {
            Some(subs) => {
                subs.retain(|x| x != &self.id);
            },
            _ => {}
        }
        return vec![create_array_resp(vec![create_bulk_string_resp("unsubscribe".into()), create_bulk_string_resp(self.channel.to_string().into()), create_int_resp(self.num_subscribed_channels)])];
    }
}