use std::slice::Iter;

use crate::{commands::RedisCommand, resp::{create_simple_string_resp, types::RespType}};

pub struct ReplConfCommand {}

impl ReplConfCommand {
    pub fn new() -> Self {
        ReplConfCommand {}
    }
}

impl RedisCommand for ReplConfCommand {
    fn execute(&self, iter: &mut Iter<'_, RespType>) -> Vec<String> {
        let keyword = match iter.next().expect("Should have a keyword key") {
            RespType::String(keyword) => keyword,
            _ => panic!("REPLCONF command expects a keyword")
        };
        if keyword.eq("listening-port") || keyword.eq("capa") {
            return vec![create_simple_string_resp("OK".to_string())]
        }

        assert!(keyword.to_lowercase().eq("getack"));
        let star = match iter.next().expect("Should have * key") {
            RespType::String(star) => star,
            _ => panic!("REPLCONF command expects a *")
        };
        assert!(star.to_lowercase().eq("*"));
        return vec!["SEND_REPLCONF_ACK".into()];
    }
}