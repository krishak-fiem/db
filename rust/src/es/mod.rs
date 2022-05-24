use elasticsearch::*;

pub fn get_client() -> Elasticsearch {
    let client = Elasticsearch::default();
    client
}

pub async fn create_index(index_name: &str) {
    let client = get_client();

    client
        .index(IndexParts::Index(index_name))
        .send()
        .await
        .unwrap();
}
