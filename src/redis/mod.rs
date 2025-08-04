pub mod client;

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

pub fn create_int_resp(n: usize) -> String {
    return format!(":{}\r\n", n);
}

pub fn create_array_resp(items: Vec<String>) -> String {
    let mut str = format!("*{}\r\n", items.len());
    for item in items {
        str.push_str(&item);
    }
    str
}