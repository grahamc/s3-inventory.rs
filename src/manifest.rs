
use serde;
use serde_json;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io;
use std::path::Path;
use md5;
use csv;
use serde::de::Error;
use datafile::{DataFile, DataFileField};

pub struct ManifestLoader {}

#[derive(Debug)]
pub enum LoadError {
    Io(io::Error),
    ChecksumMismatch { expected: String, actual: String },
    ParseError(serde_json::Error),
    UnsupportedManifestFormat(String),
}

#[derive(Deserialize, Debug)]
pub struct Manifest {
    #[serde(rename="fileFormat")]
    pub file_format: String,
    #[serde(rename="fileSchema", deserialize_with="data_file_field_deserializer")]
    pub file_schema: Vec<DataFileField>,
    pub files: Vec<DataFile>,
}

fn data_file_field_deserializer<'de, D>(input: D) -> Result<Vec<DataFileField>, D::Error>
where D: serde::Deserializer<'de> {
    let raw: &str = serde::Deserialize::deserialize(input)?;

    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .trim(csv::Trim::All)
        .from_reader(raw.as_bytes());

    match rdr.records().next() {
        Some(Ok(record)) => {
            let mut fields: Vec<DataFileField> = vec![];

            for field in record.iter() {
                match field {
                    "Bucket" => { fields.push(DataFileField::Bucket); },
                    "Key" => { fields.push(DataFileField::Key); },
                    "Size" => { fields.push(DataFileField::Size); },
                    "ETag" => { fields.push(DataFileField::ETag); },
                    "StorageClass" => { fields.push(DataFileField::StorageClass); },
                    field => {
                        return Err(D::Error::unknown_variant(
                            field,
                            &["Bucket", "Key", "Size", "ETag",
                              "StorageClass"]
                        ));
                    }
                }
            }

            Ok(fields)
        }
        Some(Err(err)) => {
            Err(D::Error::custom(
                format!("error parsing fields as a CSV: {:?}", err)
            ))
        }
        None => {
            Err(D::Error::invalid_length(0, &"at least one field"))
        }
    }
}

impl ManifestLoader {
    pub fn load(manifest_path: &Path) -> Result<Manifest, LoadError> {
        // Validate the file's checksum matches
        let checksum_path = manifest_path.clone().with_file_name("manifest.checksum");

        let expected_checksum: String = {
            let checksum_file = File::open(checksum_path)?;
            let mut checksum_reader = BufReader::new(checksum_file);
            let mut checksum = String::new();
            checksum_reader.read_to_string(&mut checksum)?;

            checksum.trim().to_owned()
        };

        let manifest_file = File::open(manifest_path)?;
        let mut manifest_reader = BufReader::new(manifest_file);
        let mut contents = String::new();
        manifest_reader.read_to_string(&mut contents)?;
        let actual_checksum = format!("{:x}", md5::compute(&contents));

        if actual_checksum != expected_checksum {
            return Err(LoadError::ChecksumMismatch {
                expected: expected_checksum,
                actual: actual_checksum
            });
        }

        let manifest: Manifest = serde_json::from_str(&contents)?;

        if manifest.file_format != "CSV" {
            return Err(LoadError::UnsupportedManifestFormat(manifest.file_format));
        }

        return Ok(manifest);
    }
}

impl From<io::Error> for LoadError {
    fn from(err: io::Error) -> LoadError {
        LoadError::Io(err)
    }
}

impl From<serde_json::Error> for LoadError {
    fn from(err: serde_json::Error) -> LoadError {
        LoadError::ParseError(err)
    }
}
