use std::{collections::HashMap, sync::{Arc, Mutex}};

use bytes::BytesMut;

use crate::redis::client::{CacheVal, StringCacheVal};

pub struct Rdb {
    version: String,
    metadata: HashMap<String, String>,
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

        if rdb_data[pos] == 0xFF {
            println!("No keys");
            return Self { version: version, metadata: metadata, key_values: Vec::new() };
        }

        let (key_values, pos) = Self::extract_key_values(&rdb_data, pos + 1);
        Self { version: version, metadata: metadata, key_values: key_values }
    }

    pub fn apply_to_db(&self, cache: Arc<Mutex<HashMap<String, CacheVal>>>) {
        let mut cache = cache.lock().unwrap();
        for key_value in self.key_values.iter() {
            cache.insert(key_value.key.clone(), CacheVal::String(StringCacheVal { val: key_value.value.clone(), expiry_time: key_value.expiry_time.clone() }));
        }
    }

    fn read_string(rdb_data: &BytesMut, pos: usize, bytes: usize) -> String {
        String::from_utf8_lossy(&rdb_data[pos..pos+bytes]).to_string()
    }

    fn extract_string(rdb_data: &BytesMut, pos: usize) -> (String, usize) {
        let mut cur_pos = pos;
        let type_byte = rdb_data[cur_pos];
        if type_byte & 0xc0 == 0xc0 {
            let bytes_len = type_byte & !0xc0;
            match bytes_len {
                0 => {
                    // 1 byte
                    let byte = i8::from_le_bytes(rdb_data[cur_pos + 1..cur_pos + 2].try_into().unwrap());
                    return (byte.to_string(), cur_pos + 2);
                }
                1 => {
                    // 2 bytes
                    let byte = i16::from_le_bytes(rdb_data[cur_pos + 1..cur_pos + 3].try_into().unwrap());
                    return (byte.to_string(), cur_pos + 3);
                }
                2 => {
                    // 4 bytes
                    let byte = i32::from_le_bytes(rdb_data[cur_pos + 1..cur_pos + 5].try_into().unwrap());
                    return (byte.to_string(), cur_pos + 5);
                },
                _ => panic!("Invalid string type byte: {}", type_byte)
            }
        } else {
            // length-prefixed string
            let str_len = rdb_data[cur_pos] as usize;
            cur_pos += 1;
            return (Self::read_string(rdb_data, cur_pos, str_len), cur_pos + str_len);
        }
    }

    fn extract_version(rdb_data: &BytesMut, pos: usize) -> (String, usize) {
        let mut cur_pos = pos;
        // First 5 bytes should be "REDIS"
        let magic = Self::read_string(rdb_data, cur_pos, 5);
        if  magic.ne("REDIS") {
            panic!("Invalid RDB file format");
        }
        cur_pos += 5;

        let version_str = Self::read_string(rdb_data, cur_pos, 4);
        println!("Version: {}", version_str);
        cur_pos += 4;

        return (version_str.to_string(), cur_pos);
    }

    fn extarct_metadata_section(rdb_data: &BytesMut, pos: usize) -> (HashMap<String, String>, usize) {
        let mut cur_pos = pos;
        assert!(rdb_data[cur_pos] == 0xFA);
        cur_pos += 1;
        
        let mut metadata: HashMap<String, String> = HashMap::new();

        loop {
            let byte = rdb_data[cur_pos];
            match byte {
                0xFA => {
                    // metadata sep, just eat
                    cur_pos += 1;
                    continue;
                }
                0xFE | 0xFF => {
                    break;
                }
                _ => {
                    let (key, new_pos) = Self::extract_string(rdb_data, cur_pos);
                    cur_pos = new_pos;
                    let (val, new_pos) = Self::extract_string(rdb_data, cur_pos);
                    cur_pos = new_pos;
                    metadata.insert(key, val);
                }
            }
        }
        println!("Metadata: {:?}", metadata);
        return (metadata, cur_pos);
    }

    fn extract_key_values(rdb_data: &BytesMut, pos: usize) -> (Vec<KeyValue>, usize) {
        let mut cur_pos = pos;
        let db_index = rdb_data[cur_pos] as usize;
        println!("DB index: {}", db_index);
        cur_pos += 1;

        assert_eq!(rdb_data[cur_pos], 0xFB);
        cur_pos += 1;

        let total_keys = rdb_data[cur_pos] as usize;
        println!("Total keys: {}", total_keys);
        cur_pos += 1;

        let expiring_keys = rdb_data[cur_pos] as usize;
        println!("Expiring keys: {}", expiring_keys);
        cur_pos += 1;


        let mut key_values = Vec::new();
        for _ in 0..total_keys {
            let mut key_expiry_time = None;
            match rdb_data[cur_pos] {
                0xFD => {
                    // OxFD is the timestamp in seconds, eat 4 bytes
                    let seconds_bytes = &rdb_data[cur_pos+1..cur_pos+5];
                    let seconds = u32::from_le_bytes(seconds_bytes.try_into().unwrap()) as u128;
                    key_expiry_time = Some(seconds);
                    println!("Key expiry time: {}", seconds);
                    cur_pos += 5;
                },
                0xFC => {
                    // OxFC is the timestamp in milliseconds, eat 8 bytes
                    let milliseconds_bytes = &rdb_data[cur_pos+1..cur_pos+9];
                    let milliseconds = u64::from_le_bytes(milliseconds_bytes.try_into().unwrap()) as u128;
                    key_expiry_time = Some(milliseconds);
                    println!("Key expiry time: {}", milliseconds);
                    cur_pos += 9;
                },
                _ => {
                    // do nothing
                }
            }

            let val_type = rdb_data[cur_pos] as usize;
            cur_pos += 1;

            let (key, new_pos) = Self::extract_string(rdb_data, cur_pos);
            cur_pos = new_pos;
            let (val, new_pos) = Self::extract_string(rdb_data, cur_pos);
            cur_pos = new_pos;
            key_values.push(KeyValue { key: key, value: val, expiry_time: key_expiry_time });
        }

        return (key_values, cur_pos);
    }
}

pub mod tests {
    use super::*;

    #[test]
    fn test_empty_rdb() {
        let rdb_data = std::fs::read("empty.rdb").unwrap();
        let rdb = Rdb::new(BytesMut::from(&rdb_data[..]));
        assert_eq!(rdb.version, "0011");
    }
}