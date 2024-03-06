use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
use bytes::BytesMut;
use anyhow::Result;


#[derive(Clone, Debug)]
pub enum Value {
    SimpleString(String),
    BulkString(String),
    NullBulkString(),
    Array(Vec<Value>),
    Text(String),
    BulkString2(String),
}

impl Value {
    pub fn serialize(self) -> String {
        match self {
            Value::SimpleString(s) => format!("+{}\r\n", s),
            Value::BulkString(s) => format!("${}\r\n{}\r\n", s.len(), s),
            Value::NullBulkString() => format!("$-1\r\n"),
            Value::Array(arr) => {
                let result = arr.iter().map(|x| x.clone().serialize()).collect::<Vec<String>>().join("");
                format!("*{}\r\n{}", arr.len(), result)
            },
            Value::Text(s) => format!("{}",s),
            Value::BulkString2(s) => format!("${}\r\n{}", s.len(), s),
        }
    }
}

pub struct Parser {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Parser {
    pub fn new(stream: TcpStream) -> Self {
        Parser {
            stream,
            buffer: BytesMut::with_capacity(512),
        }
    }

    pub async fn read_value(&mut self) -> Result<Option<Value>> {
        let bytes_read = self.stream.read_buf(&mut self.buffer).await?;
    
        if bytes_read == 0 {
            return Ok(None);
        }

        let (message, _) = parse_message(self.buffer.split())?;
        Ok(Some(message))
    }

    pub async fn write(&mut self, value: Value) -> Result<()> {
        self.stream.write(value.serialize().as_bytes()).await?;
    
        Ok(())
    }
}

fn parse_message(buffer: BytesMut) -> Result<(Value, usize)> {
    match buffer[0] as char {
        '+' => parse_simple_string(buffer),
        '*' => parse_array(buffer),
        '$' => parse_bulk_string(buffer),
        _ => Err(anyhow::anyhow!("Invalid value type {:?}", buffer)),
    }
}

fn parse_simple_string(buffer: BytesMut) -> Result<(Value, usize)> {
    if let Some((line, len)) = read_until(&buffer[1..]) {
        let string = String::from_utf8(line.to_vec()).unwrap();
        return Ok((Value::SimpleString(string), len + 1));
    }

    return Err(anyhow::anyhow!("Invalid simple string {:?}", buffer));
} 

fn parse_array(buffer: BytesMut) -> Result<(Value, usize)> {
    let (arr_length, mut bytes_consumed) = if let Some((line, len)) = read_until(&buffer[1..]) {
        let arr_length = parse_int(line)?;

        (arr_length, len+1)
    } else {
        return Err(anyhow::anyhow!("Invalid array {:?}", buffer));
    };

    let mut items = vec![];
    for _ in 0..arr_length {
        let (array_item, len) = parse_message(BytesMut::from(&buffer[bytes_consumed..]))?;

        items.push(array_item);
        bytes_consumed += len;
    }

    Ok((Value::Array(items), bytes_consumed))
} 

fn parse_bulk_string(buffer: BytesMut) -> Result<(Value, usize)> {
    let (string_length, bytes_consumed) = if let Some((line, len)) = read_until(&buffer[1..]) {
        let string_length = parse_int(line)?;

        (string_length as usize, len+1)
    } else {
        return Err(anyhow::anyhow!("Invalid bulk string {:?}", buffer));
    };

    return Ok((Value::BulkString(String::from_utf8(buffer[bytes_consumed..bytes_consumed+string_length].to_vec())?), bytes_consumed+string_length+2));
}

fn parse_int(buffer: &[u8]) -> Result<i64> {
    Ok(String::from_utf8(buffer.to_vec())?.parse::<i64>()?)
}

fn read_until(buffer: &[u8]) -> Option<(&[u8], usize)> {
    for i in 1..buffer.len() {
        if buffer[i-1] == b'\r' && buffer[i] == b'\n' {
            return Some((&buffer[..i-1], i+1));
        }
    }

    return None;
}