use std::io::prelude::*;
use std::io::BufReader;
use std::fs::File;

use serde_json::Value;
use serde_json::from_str;

use inv_index::slot::Slot;
use inv_index::slot::InMemorySlot;

pub struct Index {
    slot: String,
    in_memory: bool
}

impl Index {
    pub fn new(slot: &str, in_memory: bool) -> Index {
        Index {
            slot: slot.to_owned(),
            in_memory: in_memory
        }
    }

    pub fn build_index(&self, filename: &str, params: &[&str]) -> Result<(), String> {
        let mut slot: InMemorySlot = InMemorySlot::new(self.slot.as_str());

        slot.load();
        self.process_request_log(filename, params, &mut slot);
        slot.save();

        Ok(())
    }

    fn process_request_log<T: Slot>(&self, filename: &str, params: &[&str], slot: &mut T) {
        let log_file = File::open(filename).unwrap();
        let reader = BufReader::new(log_file);

        for line in reader.lines() {
            let _ = line.map(|line| {
                let data: Value = from_str(line.as_str()).unwrap();
                let uri = self.extract_uri(&data);

                for param in params.iter() {
                    self.extract_param_into_slot(&data, uri.as_str(), param, slot);
                }
            });
        }
    }

    fn extract_uri(&self, data: &Value) -> String {
        let request_timestamp: u64 = Some(&data)
            .and_then(|v| v.as_object())
            .and_then(|v| v.get("timestamp"))
            .and_then(|v| v.as_u64())
            .unwrap();

        let request_id: &str = Some(&data)
            .and_then(|v| v.as_object())
            .and_then(|v| v.get("requestID"))
            .and_then(|v| v.as_str())
            .unwrap();

        format!("{}:{}", request_timestamp, request_id)
    }

    fn extract_param_into_slot<T: Slot>(&self, data: &Value, uri: &str, param: &str, slot: &mut T) {
        let mut value: Option<&Value> = Some(data);

        for field in param.split('.') {
            value = value.and_then(|v| {
                v.as_array().and_then(|v| {
                        let index: Option<usize> = (*field).parse().ok();
                        index.and_then(|i| v.get(i))
                }).or_else(|| {
                    v.as_object().and_then(|v| v.get(field))
                })
            })
        } // got the final field!

        let value: Option<String> = value.map(|v| match *v {
            Value::String(ref s) => s.to_owned(),
            ref val @ _ => val.to_string()
        });

        value.map(|v| slot.put(uri, param, v.as_str()) );
    }
}
