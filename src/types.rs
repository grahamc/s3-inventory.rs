
#[derive(Deserialize, Debug)]
pub struct Manifest {
    #[serde(rename="fileFormat")]
    pub file_format: String,
    #[serde(rename="fileSchema")]
    pub file_schema: String,
    pub files: Vec<DataFile>,
}

#[derive(Deserialize, Debug)]
pub struct DataFile {
    pub key: String,
    pub size: u32,
    #[serde(rename="MD5checksum")]
    pub md5_checksum: String,
}

#[derive(Debug)]
pub enum DataFileField {
    Bucket,
    Key,
    Size,
    ETag,
    StorageClass,
}

#[derive(Debug, Default)]
pub struct KeyRecord<'a> {
    pub bucket: Option<&'a str>,
    pub key: Option<&'a str>,
    pub size: Option<usize>,
    pub etag: Option<&'a str>,
    pub storage_class: Option<&'a str>,
}

impl <'a> From<&'a str> for DataFileField {
    fn from(s: &'a str) -> DataFileField {
        match s {
            "Bucket" => DataFileField::Bucket,
            "Key" => DataFileField::Key,
            "Size" => DataFileField::Size,
            "ETag" => DataFileField::ETag,
            "StorageClass" => DataFileField::StorageClass,
            _ => panic!("Unknown data field: {:?}", s),
        }
    }
}
