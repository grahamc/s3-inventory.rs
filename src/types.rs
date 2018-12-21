
#[derive(Debug, Default)]
pub struct KeyRecord<'a> {
    pub bucket: Option<&'a str>,
    pub key: Option<&'a str>,
    pub size: Option<usize>,
    pub etag: Option<&'a str>,
    pub storage_class: Option<&'a str>,
}
