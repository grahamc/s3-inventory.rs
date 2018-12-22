use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::io;
use flate2::read::GzDecoder;
use csv;
use md5;
use types::KeyRecord;


#[derive(Deserialize, Debug)]
pub struct DataFile {
    pub key: String,
    pub size: u32,
    #[serde(rename="MD5checksum")]
    pub md5_checksum: String,
}

#[derive(Debug, Copy, Clone)]
pub enum DataFileField {
    Bucket,
    Key,
    Size,
    ETag,
    StorageClass,
}

impl DataFile {
}

#[derive(Debug)]
pub enum DataFileError {
    Io(io::Error),
    ChecksumMismatch { expected: String, actual: String },
}

impl From<io::Error> for DataFileError {
    fn from(err: io::Error) -> DataFileError {
        DataFileError::Io(err)
    }
}

pub struct LoadedDataFile<R> {
    reader: csv::Reader<R>

}

impl <'a, R: io::Read + 'a> LoadedDataFile<R> {
    pub fn all(&'a mut self, fields: &Vec<DataFileField>) -> Vec<KeyRecord> {
        self.reader.records()
            .map::<KeyRecord,_>(|record_data| {
                let data = record_data.unwrap().clone();
                let mut record: KeyRecord = Default::default();
                let iter = data.into_iter().zip(fields.iter());
                for (value, field) in iter {
                    match field {
                        DataFileField::Bucket => { record.bucket = Some(value.to_owned()); },
                        DataFileField::Key => { record.key = Some(value.to_owned()); },
                        DataFileField::Size => { record.size = Some(value.parse().unwrap()); },
                        DataFileField::ETag => { record.etag = Some(value.to_owned()); },
                        DataFileField::StorageClass => { record.storage_class = Some(value.to_owned()); },
                    }
                }

                record
            })
            .collect()
    }
}

impl <'a> LoadedDataFile<GzDecoder<io::Cursor<Vec<u8>>>> {
    pub fn new(df: &DataFile) -> Result<LoadedDataFile<GzDecoder<io::Cursor<Vec<u8>>>>, DataFileError> {
        let contents = {
            let mut reader = BufReader::new(File::open(&df.key)?);
            let mut buffer = vec![0; df.size as usize];
            reader.read(&mut buffer)?;
            buffer
        };

        let found_hash = format!("{:x}", md5::compute(&contents));

        if df.md5_checksum != found_hash {
            return Err(DataFileError::ChecksumMismatch {
                expected: df.md5_checksum.clone(),
                actual: found_hash
            })
        }

        Ok(LoadedDataFile {
            reader: csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(GzDecoder::new(io::Cursor::new(contents)))
        })
    }
}
