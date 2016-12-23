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

const INDEX_DOC: &'static str = "index.doc";
macro_rules! index_inv_filename {
    () => ("index.inv");
    ($param:expr) => (format!("index.inv.{}", $param));
}
macro_rules! format_index_line {
    () => ("{}\t{}\n");
    ($key:expr, $value:expr) => (format!("{}\t{}\n", $key, $value));
}

macro_rules! make_path {
    ( $( $x:expr ),* ) => {
        {
            let mut tmp_path = PathBuf::new();
            $(
                tmp_path.push($x);
             )*
            tmp_path
        }
    }
}

macro_rules! insert_if_not_present {
    ($map:expr, $ke:expr, $init:expr) => (
        if !$map.contains_key(&$ke) {
            $map.insert($ke.clone(), $init);
        }
    )
}

macro_rules! invert_insert_doc {
    ($map:expr, $key:expr, $val: expr, $doc: expr) => (
        insert_if_not_present!($map, $key, HashMap::new());

        $map.get_mut(&$key).map(|mut inv| {
            insert_if_not_present!(inv, $val, HashSet::new());

            inv.get_mut(&$val).map(|mut iset| {
                iset.insert($doc);
            });
        });
    )
}

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
        let direct_path = make_path![&self.directory, INDEX_DOC];
        File::open(direct_path).ok().map(|file| {
            let reader = BufReader::new(file);

            for line in reader.lines() {
                let line = line.unwrap();
                let mut fields = line.splitn(2, '\t');

                let doc_id: usize = fields.next().unwrap().parse().unwrap();
                let json_doc: &str = fields.next().unwrap();
                let data: Value = from_str(json_doc).unwrap();

                data.as_object().map(|o| {
                    if doc_id != self.direct.len() {
                        panic!(format!("Error reading document:{} - direct index size: {}", doc_id, self.direct.len()));
                    }

                    let uri = o.get("URI").unwrap().as_str().unwrap();
                    for (key, value) in o.iter() {
                        if key != "URI" {
                            self.put(uri, key.as_str(), value.as_str().unwrap());
                        }
                    }
                });
            }
        });
    }

    fn save(&self) {
        let direct_path = make_path![&self.directory, INDEX_DOC];

        let file = File::create(direct_path).unwrap();
        let mut writer = BufWriter::new(file);

        for (i, doc) in self.direct.iter().enumerate() {
            let line = format_index_line!(i, json::encode(&doc).unwrap());
            writer.write(line.as_bytes()).unwrap();
        }

        for (key, inv) in self.invert.iter() {
            let inv_path = make_path![&self.directory, index_inv_filename!(key)];

            let inv_file = File::create(inv_path).unwrap();
            let mut inv_writer = BufWriter::new(inv_file);

            let mut sorted_keys: Vec<Rc<String>> = inv.keys().map(|k| k.clone()).collect();
            sorted_keys.sort();

            for inv_key in sorted_keys.iter() {
                let mut sorted_docs: Vec<usize> = inv.get(inv_key).unwrap()
                    .iter().map(|v| *v).collect();
                sorted_docs.sort();

                for doc in sorted_docs.iter() {
                    let line = format_index_line!(inv_key, doc);
                    inv_writer.write(line.as_bytes()).unwrap();
                }
            }
        }
    }

    fn put(&mut self, uri: &str, key: &str, value: &str) {
        if key == "URI" {
            panic!("Key cannot be equal URI, it is used for internal purposes");
        }

        let r_uri = Rc::new(uri.to_owned());
        let r_key = Rc::new(key.to_owned());
        let r_val = Rc::new(value.to_owned());

        let r_uri_key = Rc::new("URI".to_owned());

        let doc_id: usize = self.uri_map.get(&r_uri)
            .map(|v| *v)
            .unwrap_or_else(|| {
                let direct_size = self.direct.len();
                let mut doc_map = HashMap::new();
                doc_map.insert(r_uri_key.clone(), r_uri.clone());

                self.direct.push(doc_map);
                self.uri_map.insert(r_uri.clone(), direct_size);

                direct_size
            });

        self.direct.get_mut(doc_id).map(|mut doc| {
            doc.insert(r_key.clone(), r_val.clone());
        });

        invert_insert_doc!(self.invert, r_key, r_val, doc_id);
        invert_insert_doc!(self.invert, r_uri_key, r_uri, doc_id);
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
