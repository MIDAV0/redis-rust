use database::Database;
// Uncomment this block to pass the first stage
use tokio::{sync::Mutex, net::{TcpListener, TcpStream}};
use anyhow::Result;
use std::sync::Arc;
use std::env;

mod parser;
mod database;

#[tokio::main]
async fn main() {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    let port_num = parse_cli_port().unwrap_or(6379);
    let (master_host, master_port) = parse_cli_replica().unwrap_or(("".to_string(), 6379));
    let master_host_copy = master_host.clone();
    if port_num != 6379 && !master_host.is_empty() {
        println!("Running replica at port {}", master_port);
        // Connect to send request
        println!("Connecting to master server '{}'", master_host);
        let connect_stream = TcpStream::connect(format!("{}:{}", master_host, master_port)).await.unwrap();
        let mut parser: parser::Parser = parser::Parser::new(connect_stream);
        let response = parser::Value::Array(vec![parser::Value::BulkString("PING".to_string())]);
        parser.write(response).await.unwrap();

        let repl_commands = parser.read_value().await.unwrap();
        if let Some(v) = repl_commands {
            let (command, _args) = parse_command(v).unwrap();
            println!("Com: {}", command);
            match command.as_str() {
                "pong" => {
                    parser.write(
                        parser::Value::Array(vec![
                            parser::Value::BulkString("REPLCONF".to_string()),
                            parser::Value::BulkString("listening-port".to_string()),
                            parser::Value::BulkString(port_num.to_string()),
                        ])
                    ).await.unwrap();
                    parser.write(
                        parser::Value::Array(vec![
                            parser::Value::BulkString("REPLCONF".to_string()),
                            parser::Value::BulkString("capa".to_string()),
                            parser::Value::BulkString("psync2".to_string()),
                        ])
                    ).await.unwrap();
                    parser.write(
                        parser::Value::Array(vec![
                            parser::Value::BulkString("PSYNC".to_string()),
                            parser::Value::BulkString("?".to_string()),
                            parser::Value::BulkString("-1".to_string()),
                        ])
                    ).await.unwrap();
                },
                _ => {
                    println!("Handshake failed. Haven't received PONG");
                    return
                }
            }
        }
        println!("Listening to FULLRESYNC");
        let rdb_commands = parser.read_value().await.unwrap();
        if let Some(v) = rdb_commands {
            let (command, _args) = parse_command(v).unwrap();
            println!("Command: {}", command);
        }
    }


    println!("Logs from your program will appear here!");
    let listener = TcpListener::bind(("127.0.0.1", port_num)).await.unwrap();
    let database = Arc::new(Mutex::new(database::Database::new(master_host_copy, port_num)));
    
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let database = database.clone();

        tokio::spawn(async move {
            handle_request(stream, database).await;
        });
    }
}

async fn handle_request(stream: TcpStream, database: Arc<Mutex<Database>>) {
    let mut parser: parser::Parser = parser::Parser::new(stream);

    loop {
        let value = parser.read_value().await.unwrap();

        let response =if let Some(v) = value {
            let (command, args) = parse_command(v).unwrap();
            println!("Commm: {}", &command);
            match command.as_str() {
                "ping" | "PING" => parser::Value::SimpleString("PONG".to_string()),
                "echo" => args.first().unwrap().clone(),
                "set" => {
                    let mut db = database.lock().await;
                    let key = if let parser::Value::BulkString(k) = args.get(0).unwrap().clone() {
                        k
                    } else {
                        panic!("Invalid value type");
                    };
                    let value = if let parser::Value::BulkString(v) = args.get(1).unwrap().clone() {
                        v
                    } else {
                        panic!("Invalid value type");
                    };
                    let experation: u128 = if args.len() == 4 {
                        if let parser::Value::BulkString(expiration_instruction) = args.get(2).unwrap().clone() {
                            match expiration_instruction.as_str() {
                                "px" | "PX" | "pX" | "Px" => {
                                    if let parser::Value::BulkString(ex) = args.get(3).unwrap().clone() {
                                        ex.parse::<u128>().unwrap()
                                    } else {
                                        0
                                    }
                                },
                                _ => 0
                            }
                        } else {
                            0
                        }
                    } else {0};
                    match db.set(&key, &value, experation) {
                        None => parser::Value::SimpleString("OK".to_string()),
                        Some(_r) => parser::Value::NullBulkString(),
                    }
                },
                "get" => {
                    let db = database.lock().await;
                    let key = if let parser::Value::BulkString(k) = args.get(0).unwrap().clone() {
                        k
                    } else {
                        panic!("Invalid value type");
                    };
                    if let Some(r) = db.get(&key) {
                        parser::Value::SimpleString(r.to_string())
                    } else {
                        parser::Value::NullBulkString()
                    }
                }
                "info" => {
                    let db = database.lock().await;
                    let host_info = db.host_info().unwrap();
                    if host_info.eq("role:master") {
                        parser::Value::Text(format!("$101\r\n# Replication\nrole:master\nmaster_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb\nmaster_repl_offset:0\r\n"))
                    } else {
                        let mut info = String::new();
                        info.push_str(&format!("{}\n",db.host_info().unwrap()));
                        info.push_str(&format!("{}\n",db.repl_id().unwrap()));
                        info.push_str(&format!("{}",db.repl_offset().unwrap()));
                        parser::Value::Text(format!("${}\r\n{}\r\n", info.len(), info))               
                    }
                },
                "replconf" | "REPLCONF" => parser::Value::SimpleString("OK".to_string()),
                "psync" | "PSYNC" => parser::Value::SimpleString(format!("FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0")),
                _ => panic!("Not supported command!"),
            }
        } else {
            break;
        };

        parser.write(response).await.unwrap();
    }        
}


fn parse_command(buffer: parser::Value) -> Result<(String, Vec<parser::Value>)> {
    match buffer {
        parser::Value::Array(v) => {
            Ok((
                unpack_string(v.first().unwrap().clone()).unwrap(),
                v.into_iter().skip(1).collect(),
            ))
        },
        parser::Value::SimpleString(s) => Ok((s.to_lowercase(), vec![])),
        _ => Err(anyhow::anyhow!("Invalid command")),
    }
}

fn parse_cli_port() -> Option<u16> {
    let index = env::args().position(|x| x == "--port")?;
    let value = env::args().nth(index + 1)?;
    value.parse().ok()
}

fn parse_cli_replica() -> Option<(String, u16)> {
    let index = env::args().position(|x| x == "--replicaof")?;
    let host = env::args().nth(index + 1)?;
    let port = env::args().nth(index + 2)?;
    let port_num = if let Some(n) = port.parse().ok() {n} else {panic!("Invalid port number")};
    Some((host, port_num))
}

fn unpack_string(value: parser::Value) -> Result<String> {
    match value {
        parser::Value::BulkString(s) => Ok(s),
        _ => Err(anyhow::anyhow!("Invalid value type")),
    }
}