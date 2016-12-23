use std::collections::HashMap;
use std::collections::HashSet;
use std::rc::Rc;

use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::fs::File;

use std::path::PathBuf;

use rustc_serialize::json;
use serde_json::Value;
use serde_json::from_str;

pub struct Index {
    slot: String,
    in_memory: bool
}

pub trait Slot {
    fn load(&mut self);
    fn put(&mut self, uri: &str, key: &str, value: &str);
    fn save(&self);
}

#[derive(RustcDecodable, RustcEncodable)]
pub struct InMemorySlot {
    directory: String,
    uri_map: HashMap<Rc<String>, usize>,
    direct: Vec<HashMap<Rc<String>, Rc<String>>>,
    invert: HashMap<Rc<String>, HashMap<Rc<String>, HashSet<usize>>>
}

pub struct ExternalSlot {
    directory: String
}

impl InMemorySlot {
    pub fn new(directory: &str) -> InMemorySlot {
        InMemorySlot {
            directory: directory.to_owned(),
            uri_map: HashMap::new(),
            direct: vec!(),
            invert: HashMap::new()
        }
    }
}

impl Slot for InMemorySlot {
    fn load(&mut self) {

    }

    fn save(&self) {
        let mut direct_path = PathBuf::new();
        direct_path.push(&self.directory);
        direct_path.push("index.doc");

        let file = File::create(direct_path).unwrap();
        let mut writer = BufWriter::new(file);

        for (i, doc) in self.direct.iter().enumerate() {
            let line = format!("{}\t{}\n",i, json::encode(&doc).unwrap());
            writer.write(line.as_bytes()).unwrap();
        }

        for (key, inv) in self.invert.iter() {
            let mut inv_path = PathBuf::new();
            inv_path.push(&self.directory);
            inv_path.push(format!("index.inv.{}", key));

            let inv_file = File::create(inv_path).unwrap();
            let mut inv_writer = BufWriter::new(inv_file);

            let mut sorted_keys: Vec<Rc<String>> = inv.keys().map(|k| k.clone()).collect();
            sorted_keys.sort();

            for inv_key in sorted_keys.iter() {
                let mut sorted_docs: Vec<usize> = inv.get(inv_key).unwrap()
                    .iter().map(|v| *v).collect();
                sorted_docs.sort();

                for doc in sorted_docs.iter() {
                    let line = format!("{}\t{}\n", inv_key, doc);
                    inv_writer.write(line.as_bytes()).unwrap();
                }
            }
        }
    }

    fn put(&mut self, uri: &str, key: &str, value: &str) {
        let r_uri = Rc::new(uri.to_owned());
        let r_key = Rc::new(key.to_owned());
        let r_val = Rc::new(value.to_owned());

        let doc_id: usize = self.uri_map.get(&r_uri)
            .map(|v| *v)
            .unwrap_or_else(|| {
                let direct_size = self.direct.len();
                let mut doc_map = HashMap::new();
                doc_map.insert(Rc::new(".URI.".to_owned()), r_uri.clone());

                self.direct.push(doc_map);
                self.uri_map.insert(r_uri.clone(), direct_size);

                direct_size
            });

        self.direct.get_mut(doc_id).map(|mut doc| {
            doc.insert(r_key.clone(), r_val.clone());
        });

        if !self.invert.contains_key(&r_key) {
            self.invert.insert(r_key.clone(), HashMap::new());
        }

        self.invert.get_mut(&r_key).map(|mut inv| {
            if !inv.contains_key(&r_val) {
                inv.insert(r_val.clone(), HashSet::new());
            }

            inv.get_mut(&r_val).map(|mut iset| {
                iset.insert(doc_id);
            });
        });
    }
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

                for param in params.iter() {
                    let uri = format!("{}:{}", request_timestamp, request_id);
                    self.extract_param_into_slot(&data, uri.as_str(), param, slot);
                }
            });
        }
    }

    fn extract_param_into_slot<T: Slot>(&self, data: &Value, uri: &str, param: &str, slot: &mut T) {
        let mut value: Option<&Value> = Some(data);

        for field in param.split(".") {
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
