use std::{collections::HashMap, sync::{Arc, Mutex}};

use bytes::BytesMut;

use crate::redis::client::{CacheVal, StringCacheVal};

pub struct Rdb {
    version: String,
    metadata: String,
    key_values: Vec<KeyValue>,
}

pub struct KeyValue {
    key: String,
    value: String,
    expiry_time: Option<u128>,
}

impl Rdb {
    pub fn new(rdb_data: BytesMut) -> Self {
        let (version, pos) = Self::extract_version(&rdb_data, 0);
        let (metadata, pos) = Self::extarct_metadata_section(&rdb_data, pos);
        let (key_values, pos) = Self::extract_key_values(&rdb_data, pos);
        Self { version: version, metadata: metadata, key_values: key_values }
    }

    pub fn apply_to_db(&self, cache: Arc<Mutex<HashMap<String, CacheVal>>>) {
        let mut cache = cache.lock().unwrap();
        for key_value in self.key_values.iter() {
            cache.insert(key_value.key.clone(), CacheVal::String(StringCacheVal { val: key_value.value.clone(), expiry_time: key_value.expiry_time.clone() }));
        }
    }

    fn extract_version(rdb_data: &BytesMut, pos: usize) -> (String, usize) {
        // First 5 bytes should be "REDIS"
        if  &rdb_data[pos..pos+5] != b"REDIS" {
            panic!("Invalid RDB file format");
        }

        let mut version_bytes = vec![];
        let mut current_pos = pos + 5;
        loop {
            let byte = rdb_data[current_pos];
            current_pos += 1;
            match byte {
                0xFA => {
                    // 0xFA is the end of the version string
                    break;
                }
                _ => {
                    version_bytes.push(byte);
                }
            }
        }

        let version_str = std::str::from_utf8(&version_bytes).expect("Version should be valid UTF-8");
        println!("Version: {}", version_str);
        return (version_str.to_string(), current_pos);
    }

    fn extarct_metadata_section(rdb_data: &BytesMut, pos: usize) -> (String, usize) {
        let mut metadata_bytes = vec![];
        let mut current_pos = pos;
        loop {
            let byte = rdb_data[current_pos];
            current_pos += 1;
            match byte {
                0xFE => {
                    break;
                }
                _ => {
                    metadata_bytes.push(byte);
                }
            }
        }
        let metadata_str = String::from_utf8_lossy(&metadata_bytes);
        println!("Metadata: {}", metadata_str);
        return (metadata_str.to_string(), current_pos);
    }

    fn extract_key_values(rdb_data: &BytesMut, pos: usize) -> (Vec<KeyValue>, usize) {
        let mut current_pos = pos;
        let db_index = rdb_data[current_pos] as usize;
        println!("DB index: {}", db_index);
        current_pos += 1;

        if rdb_data[current_pos] == 0xFF {
            println!("No keys");
            return (Vec::new(), current_pos);
        }

        assert_eq!(rdb_data[current_pos], 0xFB);
        current_pos += 1;

        let total_keys = rdb_data[current_pos] as usize;
        println!("Total keys: {}", total_keys);
        current_pos += 1;

        let expiring_keys = rdb_data[current_pos] as usize;
        println!("Expiring keys: {}", expiring_keys);
        current_pos += 1;


        let mut key_values = Vec::new();
        for _ in 0..total_keys {
            let mut key_expiry_time = None;
            match rdb_data[current_pos] {
                0xFD => {
                    // OxFD is the timestamp in seconds, eat 4 bytes
                    let seconds_bytes = &rdb_data[current_pos+1..current_pos+5];
                    let seconds = u32::from_le_bytes(seconds_bytes.try_into().unwrap()) as u128;
                    key_expiry_time = Some(seconds);
                    println!("Key expiry time: {}", seconds);
                    current_pos += 5;
                },
                0xFC => {
                    // OxFC is the timestamp in milliseconds, eat 8 bytes
                    let milliseconds_bytes = &rdb_data[current_pos+1..current_pos+9];
                    let milliseconds = u64::from_le_bytes(milliseconds_bytes.try_into().unwrap()) as u128;
                    key_expiry_time = Some(milliseconds);
                    println!("Key expiry time: {}", milliseconds);
                    current_pos += 9;
                },
                _ => {
                    // do nothing
                }
            }

            let val_type = rdb_data[current_pos] as usize;
            println!("Value type: {}", val_type);
            current_pos += 1;

            //ASSUME STRING TYPE
            let key_length = rdb_data[current_pos] as usize;
            println!("Key length: {}", key_length);
            current_pos += 1;

            let mut key_bytes = vec![];
            for _ in 0..key_length {
             key_bytes.push(rdb_data[current_pos]);
             current_pos += 1;
            }
            let key_str = String::from_utf8_lossy(&key_bytes);
            println!("Key: {}", key_str);

            let value_length = rdb_data[current_pos] as usize;
            println!("Value length: {}", value_length);
            current_pos += 1;

            let mut value_bytes = vec![];
            for _ in 0..value_length {
             value_bytes.push(rdb_data[current_pos]);
             current_pos += 1;
            }
            let value_str = String::from_utf8_lossy(&value_bytes);
            println!("Value: {}", value_str);
            key_values.push(KeyValue { key: key_str.to_string(), value: value_str.to_string(), expiry_time: key_expiry_time });
        }

        return (key_values, current_pos);
    }
}