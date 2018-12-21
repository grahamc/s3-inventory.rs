
use serde_json;
use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io;
use std::path::Path;
use md5;

use types::Manifest;

pub struct ManifestLoader {}

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

        Ok(serde_json::from_str(&contents)?)
    }
}

#[derive(Debug)]
pub enum LoadError {
    Io(io::Error),
    ChecksumMismatch { expected: String, actual: String },
    ParseError(serde_json::Error),
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