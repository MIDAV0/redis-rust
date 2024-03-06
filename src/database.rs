use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
struct MapValue {
    value: String,
    expiration: u128,
}

impl MapValue {
    fn new(v: String, e: u128) -> Self {
        MapValue {
            value: v,
            expiration: e,
        }
    }
}

#[derive(Clone)]
pub struct Database {
    db: HashMap<String, MapValue>,
    master_host: String,
    current_port: u16,
}

impl Database {
    pub fn new(host: String, port: u16) -> Self {
        Database {
            db: HashMap::new(),
            master_host: host,
            current_port: port,
        }
    }

    pub fn set(&mut self, key: &String, value: &String, expiration: u128) -> Option<String> {
        let start = SystemTime::now();
        let since_e = start.duration_since(UNIX_EPOCH).expect("Time error");
        let time_stamp = if expiration == 0 {0} else {expiration + since_e.as_millis()};
        if let Some(mv) = self.db.insert(key.to_owned(), MapValue::new(value.to_owned(), time_stamp)) {
            Some(mv.value)
        } else {
            None
        }
    }
    pub fn get(&self, key: &String) -> Option<&String> {
        if let Some(mv) = self.db.get(key) {
            // Get current time
            let start = SystemTime::now();
            let since_e = start.duration_since(UNIX_EPOCH).expect("Time error");
            if mv.expiration == 0 || since_e.as_millis() <= mv.expiration {
                Some(&mv.value)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn host_info(&self) -> Option<String> {
        let role = if self.master_host.is_empty() {
            "master"
        } else {
            "slave"
        };
        Some(format!("role:{}", role))
    }

    pub fn current_port_info(&self) -> Option<u16> {
        Some(self.current_port)
    }

    pub fn repl_id(&self) -> Option<String> {
        Some("master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb".to_string())
    }
    pub fn repl_offset(&self) -> Option<String> {
        Some(format!("master_repl_offset:{}", 0))
    }
}