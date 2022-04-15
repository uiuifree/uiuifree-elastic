use dotenv::dotenv;
use elastic_parser::{Hit, SearchResponse};
use elastic_query_builder::QueryBuilder;
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::response::Response;
use elasticsearch::http::transport::Transport;
use elasticsearch::indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts};
use elasticsearch::params::Refresh;
use elasticsearch::{
    BulkParts, DeleteParts, Elasticsearch, IndexParts, ScrollParts, SearchParts, UpdateParts,
};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::json;
use std::env;

extern crate serde;
// #[macro_use]
extern crate serde_derive;
extern crate serde_json;

pub fn el_client() -> Result<Elasticsearch, String> {
    dotenv().ok();
    let host = env::var("ELASTIC_HOST").unwrap_or("http://localhost:9200".to_string());
    let transport = Transport::single_node(host.as_str());

    return match transport {
        Ok(v) => Ok(Elasticsearch::new(v)),
        Err(_) => Err("Error Elastic Connection ".to_string()),
    };
}

pub async fn exist_index(index: &str) -> bool {
    let elastic;
    match el_client() {
        Ok(v) => {
            elastic = v;
        }
        Err(_) => {
            return false;
        }
    }
    return match elastic
        .indices()
        .exists(IndicesExistsParts::Index(&[index]))
        .send()
        .await
    {
        Ok(v) => v.status_code() == 200,
        Err(_) => false,
    };
}

pub async fn create_index<T>(index: &str, json: T) -> Result<bool, String>
where
    T: Serialize,
{
    return match el_client()?
        .indices()
        .create(IndicesCreateParts::Index(index))
        .body(json)
        .send()
        .await
    {
        Ok(v) => Ok(v.status_code() == 200),
        Err(e) => Err(e.to_string()),
    };
}

pub async fn recreate_index<T>(index: &str, json: T) -> Result<bool, String>
where
    T: Serialize,
{
    let elastic = el_client()?;
    if exist_index(index).await {
        let res = elastic
            .indices()
            .delete(IndicesDeleteParts::Index(&[index]))
            .send()
            .await;
        if res.is_err() {
            return Err(res.err().unwrap().to_string());
        }
    }
    let result = create_index(index, json).await?;
    Ok(result)
}

pub async fn update<T: serde::Serialize>(index: &str, id: &str, source: T) -> Result<(), String> {
    let client = el_client();
    if client.is_err() {
        return Err(client.err().unwrap());
    }
    let _ = client
        .unwrap()
        .update(UpdateParts::IndexId(index, id))
        .body(json!({ "doc": source }))
        .send()
        .await;

    return Ok(());
}

pub async fn bulk_insert<T: serde::Serialize>(
    index: &str,
    sources: Vec<T>,
) -> Result<Response, elasticsearch::Error> {
    let mut body: Vec<JsonBody<_>> = Vec::with_capacity(4);
    for source in sources {
        body.push(json!({"index": {}}).into());
        body.push(json!(source).into())
    }
    let client = el_client().unwrap();
    client.bulk(BulkParts::Index(index)).body(body).send().await
}

pub async fn delete(index: &str, id: &str) -> Result<Response, elasticsearch::Error> {
    let client = el_client().unwrap();
    client.delete(DeleteParts::IndexId(index, id)).send().await
}

pub async fn refresh(index: &str) -> Result<Response, elasticsearch::Error> {
    let client = el_client().unwrap();
    client
        .index(IndexParts::Index(index))
        .refresh(Refresh::True)
        .body(json!({}))
        .send()
        .await
}

pub async fn first_search<T>(
    index: &str,
    query_builder: QueryBuilder,
) -> Result<Option<Hit<T>>, String>
where
    T: DeserializeOwned + 'static,
{
    let client = el_client();
    if client.is_err() {
        return Ok(None);
    }
    let client = client.unwrap();
    return match client
        .search(SearchParts::Index(&[index]))
        .body(query_builder.build())
        .size(1)
        .send()
        .await
    {
        Ok(response) => {
            return match response.json::<SearchResponse<T>>().await {
                Ok(v) => {
                    let value = v.hits.hits;
                    if value.is_none() {
                        return Ok(None);
                    }
                    let value = value.unwrap().pop();
                    if value.is_none() {
                        return Ok(None);
                    }
                    Ok(Some(value.unwrap()))
                }
                Err(v) => {
                    return Err(v.to_string());
                }
            };
        }
        Err(_) => Ok(None),
    };
}

pub async fn scroll<T>(scroll_id: &str, alive: &str) -> Result<Option<SearchResponse<T>>, String>
where
    T: DeserializeOwned + 'static,
{
    let client = el_client();
    if client.is_err() {
        return Ok(None);
    }
    let client = client.unwrap();

    match client
        .scroll(ScrollParts::ScrollId(scroll_id))
        .scroll(alive)
        .send()
        .await
    {
        Ok(response) => {
            return match response.json::<SearchResponse<T>>().await {
                Ok(v) => Ok(Some(v)),
                Err(v) => {
                    return Err(v.to_string());
                }
            };
        }
        Err(_) => Ok(None),
    }
}

pub async fn search<T>(
    index: &str,
    query_builder: QueryBuilder,
) -> Result<Option<SearchResponse<T>>, String>
where
    T: DeserializeOwned + 'static,
{
    let client = el_client();
    if client.is_err() {
        return Ok(None);
    }
    let client = client.unwrap();

    // println!("OK");
    if !query_builder.get_scroll().is_empty() {
        return match client
            .search(SearchParts::Index(&[index]))
            .body(query_builder.build())
            .from(query_builder.get_from())
            .size(query_builder.get_size())
            .scroll(query_builder.get_scroll())
            .send()
            .await
        {
            Ok(response) => {
                return match response.json::<SearchResponse<T>>().await {
                    Ok(v) => Ok(Some(v)),
                    Err(v) => {
                        return Err(v.to_string());
                    }
                };
            }
            Err(_) => Ok(None),
        };
    }
    match client
        .search(SearchParts::Index(&[index]))
        .body(query_builder.build())
        .from(query_builder.get_from())
        .size(query_builder.get_size())
        .send()
        .await
    {
        Ok(response) => {
            return match response.json::<SearchResponse<T>>().await {
                Ok(v) => Ok(Some(v)),
                Err(v) => {
                    return Err(v.to_string());
                }
            };
        }
        Err(_) => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        // query.add_must("hoge");
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
