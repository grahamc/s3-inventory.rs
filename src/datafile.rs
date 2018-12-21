
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
