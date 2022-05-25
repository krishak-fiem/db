use elasticsearch::{
    http::{response::Response, transport::Transport, StatusCode},
    indices::{IndicesCreateParts, IndicesExistsParts, IndicesPutMappingParts},
    *,
};

pub fn get_client() -> Elasticsearch {
    let transport = Transport::single_node("http://localhost:9200").unwrap();
    let client = Elasticsearch::new(transport);
    client
}

pub async fn create_index_if_not_exists(client: &Elasticsearch, index_name: &str) {
    let exists = client
        .indices()
        .exists(IndicesExistsParts::Index(&[index_name]))
        .send()
        .await
        .unwrap();

    if exists.status_code() == StatusCode::NOT_FOUND {
        let response = client
            .indices()
            .create(IndicesCreateParts::Index(index_name))
            .send()
            .await;

        match response {
            Err(e) => println!("Problem creating index: {}", e),
            _ => (),
        }
    }
}

pub async fn update_mapping(client: &Elasticsearch, map: serde_json::Value, index_name: &str) {
    let res = client
        .indices()
        .put_mapping(IndicesPutMappingParts::Index(&[index_name]))
        .body(&map)
        .send()
        .await;

    match res {
        Err(e) => println!("Problem updating mapping: {}", e),
        _ => (),
    }
}

pub async fn es_operation(client: &Elasticsearch, data: serde_json::Value, index_name: &str) {
    let res = client
        .index(IndexParts::Index(index_name))
        .body(&data)
        .send()
        .await;

    match res {
        Err(e) => {
            println!("Problem executing the query: {}", e);
        }
        _ => (),
    }
}

pub async fn es_query(
    client: &Elasticsearch,
    data: serde_json::Value,
    index_name: &str,
) -> Result<Response, Box<dyn std::error::Error>> {
    let res = client
        .search(SearchParts::Index(&vec![index_name]))
        .body(&data)
        .pretty(true)
        .send()
        .await;

    match res {
        Err(e) => {
            println!("Problem executing the query: {}", e);
            Err(Box::new(e))
        }
        Ok(result) => Ok(result),
    }
}

pub async fn es_update(
    client: &Elasticsearch,
    data: serde_json::Value,
    index_name: &str,
) -> Result<Response, Box<dyn std::error::Error>> {
    let res = client
        .index(IndexParts::Index(index_name))
        .body(&data)
        .pretty(true)
        .send()
        .await;

    match res {
        Err(e) => {
            println!("Problem executing the query: {}", e);
            Err(Box::new(e))
        }
        Ok(result) => Ok(result),
    }
}
