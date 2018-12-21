
#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;
extern crate md5;
extern crate flate2;
extern crate csv;

use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use flate2::read::GzDecoder;
use std::env;
mod types;
use types::{Manifest,DataFileField,KeyRecord};

fn main() {
    env::set_current_dir("./nixos-sats-data").unwrap();
    let hash_file = File::open("./nix-cache/Analytics/2018-12-06T08-00Z/manifest.checksum").unwrap();
    let mut buf_reader = BufReader::new(hash_file);
    let mut checksum = String::new();
    buf_reader.read_to_string(&mut checksum).unwrap();

    let file = File::open("./nix-cache/Analytics/2018-12-06T08-00Z/manifest.json").unwrap();
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).unwrap();
    let found_hash = format!("{:x}", md5::compute(&contents));

    if checksum.trim() != found_hash {
        println!("Checksum mismatch:");
        println!("Expected: {}", checksum.trim());
        println!("On-disk: {}", found_hash);
        panic!();
    }

    let manifest = serde_json::from_str::<Manifest>(&contents)
        .unwrap();

    if manifest.file_format != "CSV" {
        panic!("File schema {} is unsupported, only CSV is supported",
               manifest.file_format);
    }

    let fields: Vec<DataFileField> = {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .trim(csv::Trim::All)
            .from_reader(manifest.file_schema.as_bytes());

        rdr.records().next().unwrap().unwrap()
            .iter()
            .map::<DataFileField, _>(|s| s.into())
            .collect::<Vec<DataFileField>>()
    };

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

    for file in manifest.files.into_iter() {
        let mut buf_reader = BufReader::new(File::open(file.key).unwrap());
        let mut buffer = vec![0; file.size as usize];
        buf_reader.read(&mut buffer).unwrap();
        let found_hash = format!("{:x}", md5::compute(&buffer));

        if file.md5_checksum != found_hash {
            println!("{} -- checksum mismatch", file.key);
            println!("expect: {}", file.md5_checksum);
            println!("actual: {}", found_hash);
            panic!();
        }
        println!("OK: {}", file.key);

        let mut d = GzDecoder::new(&buffer[..]);

        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(d);

        for record_data in rdr.records() {
            let data = record_data.unwrap();
            let mut record: KeyRecord = Default::default();
            for (value, field) in data.iter().zip(fields.iter()) {
                match field {
                    DataFileField::Bucket => { record.bucket = Some(value); },
                    DataFileField::Key => { record.key = Some(value); },
                    DataFileField::Size => { record.size = Some(value.parse().unwrap()); },
                    DataFileField::ETag => { record.etag = Some(value); },
                    DataFileField::StorageClass => { record.storage_class = Some(value); },
                }
            }

            if let Some(7) = record.size {
                println!("{:?}", record.key);
            }
        }
    }
}