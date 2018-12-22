
#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;
extern crate md5;
extern crate flate2;
extern crate csv;
extern crate threadpool;

use std::env;
use std::path::Path;

mod datafile;
mod types;
mod manifest;
use datafile::LoadedDataFile;
use threadpool::ThreadPool;
use std::sync::{Arc, Barrier};
use std::sync::mpsc::channel;
use std::io;
use std::io::BufWriter;
use std::io::Write;

fn main() {
    env::set_current_dir("./nixos-sats-data").unwrap();
    let manifest = manifest::ManifestLoader::load(Path::new("./nix-cache/Analytics/2018-12-06T08-00Z/manifest.json")).unwrap();

    let mut data = manifest.files.iter()
        .map(|datafile| datafile.size)
        .collect::<Vec<u32>>();
    data.sort();

    println!("Data files: {}", data.len());

    if data.len() == 0 {
        panic!("No data files.");
    }

    println!("Smallest data file: {:?} bytes", data.get(0).unwrap());
    data.reverse();
    println!("Largest data file: {:?} bytes", data.get(0).unwrap());
    let fields = manifest.file_schema;
    println!("Fields: {:?}", fields);


    let read_pool = ThreadPool::new(8);
    let (send, recv) = channel();
    let (send_sum, recv_sum) = channel();

    let mut i = 0;
    for file in manifest.files.into_iter() {
        i = i + 1;
        let send = send.clone();
        let send_sum = send_sum.clone();
        let fields = fields.clone();
        read_pool.execute(move|| {
            let mut loadedDataFile: LoadedDataFile<_> = LoadedDataFile::new(&file).unwrap();

            let mut count = 0;

            for record in loadedDataFile.all(&fields) {
                count = count + 1;

                if let Some(size) = record.size {
                    if size > 436 {
                        continue;
                    }
                } else {
                    continue;
                }

                if let Some(ref key) = record.key {
                    if ! key.ends_with(".narinfo") {
                        continue;
                    }
                } else {
                    continue;
                }

                send.send(record).unwrap();
            }
            send_sum.send(count);
        });
    }
    drop(send);
    drop(send_sum);

    let mut stream = BufWriter::new(io::stdout());
    for record in recv.iter() {
        stream.write(format!("recd: {:?}\n", record).as_bytes());
    }

    let mut sum = 0;
    for record in recv_sum.iter() {
        sum = record + sum;
    }
    println!("Sum: {}", sum);
}
