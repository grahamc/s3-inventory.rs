
#[derive(Debug, Default)]
pub struct KeyRecord {
    pub bucket: Option<String>,
    pub key: Option<String>,
    pub size: Option<usize>,
    pub etag: Option<String>,
    pub storage_class: Option<String>,
}
