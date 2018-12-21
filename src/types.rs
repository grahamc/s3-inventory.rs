
#[derive(Deserialize, Debug)]
pub struct Manifest<'a> {
    #[serde(rename="fileFormat")]
    pub file_format: &'a str,
    #[serde(rename="fileSchema")]
    pub file_schema: &'a str,
    pub files: Vec<DataFile<'a>>,
}

#[derive(Deserialize, Debug)]
pub struct DataFile<'a> {
    pub key: &'a str,
    pub size: u32,
    #[serde(rename="MD5checksum")]
    pub md5_checksum: &'a str,
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
