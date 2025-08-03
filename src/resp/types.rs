use std::result::Result::Ok;
use bytes::BytesMut;

use crate::resp::RespError;

#[derive(Debug)]
pub enum RespType {
    String(String),
    Error(String),
    Int(i64),
    Array(Vec<RespType>),
    NullArray,
    NullBulkString
}

impl RespType {
    pub fn parse(buffer: &BytesMut, pos: usize) -> Result<(RespType, usize), RespError> {
        let c = buffer[pos] as char;
        match c {
            '$' => Self::bulk_string(buffer, pos + 1),
            '+' => Self::simple_string(buffer, pos + 1),
            '*' => Self::array(buffer, pos + 1),
            _ => Err(RespError::Other(format!("Invalid RESP data type: {}", c)))
        }
    }

    fn word(buffer: &BytesMut, pos: usize) -> Option<(String, usize)> {
        if buffer.len() < pos {
            return None;
        }

        let mut word: Option<&[u8]> = None;
        let mut bytes_read = 0;
        for i in pos..buffer.len() {
            if buffer[i - 1] == b'\r' && buffer[i] == b'\n' {
                word = Some(&buffer[pos..(i-1)]);
                bytes_read = i + 1 - pos;   
                break;  
            }
        }

        if word.is_none() {
            return None;
        }

        let utf8_str= String::from_utf8(word.expect("to be present").to_vec());
        match utf8_str {
            Ok(s) => Some((s, bytes_read)),
            Err(_) => None
        }
    }

    fn int(buffer: &BytesMut, pos: usize) -> Option<(i64, usize)> {
        let word = Self::word(buffer, pos);
        if word.is_none(){
            return None;
        }
        let (word_str, bytes_read) = word.expect("to be here");
        let int = word_str.parse::<i64>();
        match int {
            Ok(n) => {
                Some((n, bytes_read))
            },
            Err(_) => {
                None
            }
        }
    }

    fn simple_string(buffer: &BytesMut, pos: usize) -> Result<(RespType, usize), RespError> {
        if let Some((word_str, len)) = Self::word(&buffer, pos) {
            Ok((RespType::String(word_str), len + 1))
        } else {
            Err(RespError::InvalidSimpleString(String::from("Invalid value for simple string")))
        }
    }

    fn bulk_string(buffer: &BytesMut, pos: usize) -> Result<(RespType, usize), RespError> {
        let (str_size, bytes_read) = match Self::int(buffer, pos) {
            Some(res) => res,
            None => return Err(RespError::InvalidBulkString(String::from("Bad Bulk str"))),
        };

        let offset = pos + bytes_read;
        let word = &buffer[offset..(offset+str_size as usize)];
        let utf8_str= String::from_utf8(word.to_vec());
        match utf8_str {
            Ok(s) => Ok((RespType::String(s), bytes_read + 2 + (str_size as usize + 1))),
            Err(_) => Err(RespError::InvalidBulkString(String::from("String not UTF-8")))
        }
    }

    fn array(buffer: &BytesMut, pos: usize) -> Result<(RespType, usize), RespError> {
        let (array_size, bytes_read) = match Self::int(buffer, pos) {
            Some(i) => i,
            None => return Err(RespError::InvalidArray(String::from("Bad Array length"))),
        };

        let mut values = Vec::with_capacity(array_size as usize);
        let mut curr_pos = pos + bytes_read;

        for _ in 0..array_size {
            match Self::parse(buffer, curr_pos) {
                Ok(res) => {
                    values.push(res.0);
                    curr_pos += res.1;
                },
                Err(_) => return Err(RespError::InvalidArray(String::from("Bad array")))
            }
        }

        Ok((RespType::Array(values), curr_pos))
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_simple_string() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"+OK\r\n");
        
        let result = RespType::simple_string(&buffer, 1);
        assert!(result.is_ok());
        
        let (resp, _) = result.unwrap();
        match resp {
            RespType::String(s) => assert_eq!(s, "OK"),
            _ => panic!("Expected String type")
        }
    }

    #[test]
    fn test_bulk_string() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"$5\r\nhello\r\n");
        
        let result = RespType::bulk_string(&buffer, 1);
        assert!(result.is_ok());
        
        let (resp, _) = result.unwrap();
        match resp {
            RespType::String(s) => assert_eq!(s, "hello"),
            _ => panic!("Expected String type")
        }
    }

    #[test]
    fn test_array() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n");
        
        let result = RespType::array(&buffer, 1);
        println!("{:?}", result.as_ref().err());
        assert!(result.is_ok());
        
        let (resp, _) = result.unwrap();
        match resp {
            RespType::Array(arr) => {
                assert_eq!(arr.len(), 2);
                match &arr[0] {
                    RespType::String(s) => assert_eq!(s, "ECHO"),
                    _ => panic!("Expected String type")
                }
                match &arr[1] {
                    RespType::String(s) => assert_eq!(s, "hello"),
                    _ => panic!("Expected String type")
                }
            },
            _ => panic!("Expected Array type")
        }
    }


    #[test]
    fn test_array_single() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"*1\r\n$4\r\nPING\r\n");
        
        let result = RespType::array(&buffer, 1);
        println!("{:?}", result.as_ref().err());
        assert!(result.is_ok());
        
        let (resp, _) = result.unwrap();
        match resp {
            RespType::Array(arr) => {
                assert_eq!(arr.len(), 1);
                match &arr[0] {
                    RespType::String(s) => assert_eq!(s, "PING"),
                    _ => panic!("Expected String type")
                }
            },
            _ => panic!("Expected Array type")
        }
    }
}
