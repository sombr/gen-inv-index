extern crate serde;
extern crate serde_json;
extern crate rustc_serialize;

extern crate clap;
use clap::{Arg, App};

mod index;
use index::Index;

fn main() {
    let matches = App::new("json log index builder")
        .version("1.0")
        .author("Serge Toropov (@sombr)")
        .about("Generates a direct and an invert index")
        .arg(Arg::with_name("request-log")
             .index(1)
             .required(true)
             .short("f")
             .long("filename")
             .value_name("FILE")
             .help("Path to the request log")
             .takes_value(true))
        .arg(Arg::with_name("index-slot")
             .index(2)
             .required(true)
             .short("s")
             .long("slot")
             .value_name("DIRECTORY")
             .help("Path to an index directory (slot)")
             .takes_value(true))
        .arg(Arg::with_name("param")
             .index(3)
             .short("p")
             .required(true)
             .long("parameter")
             .value_name("TARGET")
             .help("Path to the target value to plot")
             .takes_value(true)
             .multiple(true))
        .arg(Arg::with_name("in-memory")
             .short("m")
             .long("in-memory")
             .help("Perform sorting / merging in memory or with external sort & merge utils")
             .takes_value(false))
        .get_matches();

    let filename          = matches.value_of("request-log").unwrap();
    let slot              = matches.value_of("index-slot").unwrap();
    let in_memory         = matches.is_present("in-memory");
    let params: Vec<&str> = matches.values_of("param").unwrap().collect();

    let index = Index::new( slot, in_memory );
    let _ = index.build_index(filename, params.as_slice()).unwrap();
}
