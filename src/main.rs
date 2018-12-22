
#[macro_use]
extern crate serde_derive;

extern crate serde;
extern crate serde_json;
extern crate md5;
extern crate flate2;
extern crate csv;

use std::env;
use std::path::Path;

mod datafile;
mod types;
mod manifest;
use datafile::LoadedDataFile;

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

    for file in manifest.files.into_iter() {
        println!("{:?}", file);
        let mut loadedDataFile: LoadedDataFile<_> = LoadedDataFile::new(file).unwrap();
        for record in loadedDataFile.all(&fields) {
             println!("{:?}", record);
        }

        /*
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
        */
    }
}
