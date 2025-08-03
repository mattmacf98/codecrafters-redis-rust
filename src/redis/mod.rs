pub mod client;

pub fn create_simple_string_resp(str: String) -> String {
    return format!("+{}\r\n", str);
}

pub fn create_bulk_string_resp(str: String) -> String {
    let len = str.len();
    return format!("${}\r\n{}\r\n", len, str);
}

pub fn create_null_bulk_string_resp() -> String {
    return "$-1\r\n".to_string();
}

pub fn create_int_resp(n: usize) -> String {
    return format!(":{}\r\n", n);
}