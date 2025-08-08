use std::fmt::Display;

pub mod types;

#[derive(Debug)]
pub enum RespError {
    InvalidBulkString(String),
    InvalidSimpleString(String),
    InvalidArray(String),
    Other(String)
}

impl std::fmt::Display for RespError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespError::InvalidBulkString(msg) => msg.as_str().fmt(f),
            RespError::InvalidSimpleString(msg) => msg.as_str().fmt(f),
            RespError::InvalidArray(msg) => msg.as_str().fmt(f),
            RespError::Other(msg) => msg.as_str().fmt(f)
        }
    }
}

pub fn create_simple_string_resp(str: String) -> String {
    return format!("+{}\r\n", str);
}

pub fn create_basic_err_resp(str: String) -> String {
    return format!("-{}\r\n", str);
}

pub fn create_bulk_string_resp(str: String) -> String {
    let len = str.len();
    return format!("${}\r\n{}\r\n", len, str);
}

pub fn create_null_bulk_string_resp() -> String {
    return "$-1\r\n".to_string();
}

pub fn create_int_resp<T>(n: T) -> String where T : Display {
    return format!(":{}\r\n", n);
}

pub fn create_array_resp(items: Vec<String>) -> String {
    let mut str = format!("*{}\r\n", items.len());
    for item in items {
        str.push_str(&item);
    }
    str
}