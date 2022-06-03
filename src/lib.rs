pub mod error;

use crate::error::ElasticError;
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

pub fn el_client() -> Result<Elasticsearch, ElasticError> {
    dotenv().ok();
    let host = env::var("ELASTIC_HOST").unwrap_or("http://localhost:9200".to_string());
    let transport = Transport::single_node(host.as_str());

    return match transport {
        Ok(v) => Ok(Elasticsearch::new(v)),
        Err(e) => Err(ElasticError::Connection(e.to_string())),
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

pub async fn create_index<T>(index: &str, json: T) -> Result<bool, ElasticError>
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
        Err(e) => Err(ElasticError::Send(e.to_string())),
    };
}

pub async fn recreate_index<T>(index: &str, json: T) -> Result<bool, ElasticError>
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
            return Err(ElasticError::Response(res.err().unwrap().to_string()));
        }
    }
    let result = create_index(index, json).await?;
    Ok(result)
}

pub async fn update<T: serde::Serialize>(
    index: &str,
    id: &str,
    source: T,
) -> Result<(), ElasticError> {
    let client = el_client()?;
    let res = client
        .update(UpdateParts::IndexId(index, id))
        .body(json!({ "doc": source }))
        .send()
        .await;
    if res.is_err() {
        return Err(ElasticError::Response(res.unwrap_err().to_string()));
    }
    let code = res.as_ref().unwrap().status_code();
    if code == 404 {
        return Err(ElasticError::NotFound(format!("not found entity: {}", id)));
    }
    let res = res.unwrap();
    if res.status_code() != 200 {
        return Err(ElasticError::Response(res.text().await.unwrap_or_default()));
    }
    Ok(())
}
pub async fn update_or_create<T: serde::Serialize>(
    index: &str,
    id: &str,
    source: &T,
) -> Result<(), ElasticError> {
    let res = update(index, id, source).await;
    if res.is_ok() {
        return Ok(());
    }
    if res.is_err() {
        let error = res.unwrap_err();
        match error {
            ElasticError::NotFound(_) => {}
            _ => {
                return Err(error);
            }
        };
    }
    let res = insert_by_id(index, id, source).await;
    if res.is_err() {
        return Err(res.unwrap_err());
    }
    let res = res.unwrap();
    if res.status_code() != 200 {
        return Err(ElasticError::Response(res.text().await.unwrap_or_default()));
    }
    Ok(())
}

pub async fn insert_by_id<T: serde::Serialize>(
    index: &str,
    id: &str,
    source: T,
) -> Result<Response, ElasticError> {
    let mut body: Vec<JsonBody<_>> = Vec::with_capacity(4);
    body.push(json!({"index": {"_id":id}}).into());
    body.push(json!(source).into());
    let client = el_client()?;
    let res = client.bulk(BulkParts::Index(index)).body(body).send().await;
    if res.is_err() {
        return Err(ElasticError::Response(res.err().unwrap().to_string()));
    }
    return Ok(res.unwrap());
}

pub async fn bulk_insert<T: serde::Serialize>(
    index: &str,
    sources: Vec<T>,
) -> Result<Response, ElasticError> {
    let mut body: Vec<JsonBody<_>> = Vec::with_capacity(4);
    for source in sources {
        body.push(json!({"index": {}}).into());
        body.push(json!(source).into())
    }
    let client = el_client()?;
    let res = client.bulk(BulkParts::Index(index)).body(body).send().await;
    if res.is_err() {
        return Err(ElasticError::Response(res.err().unwrap().to_string()));
    }
    return Ok(res.unwrap());
}

pub async fn delete(index: &str, id: &str) -> Result<Response, ElasticError> {
    let client = el_client()?;
    let res = client.delete(DeleteParts::IndexId(index, id)).send().await;
    if res.is_err() {
        return Err(ElasticError::Response(res.err().unwrap().to_string()));
    }
    return Ok(res.unwrap());
}

pub async fn refresh(index: &str) -> Result<Response, ElasticError> {
    let client = el_client()?;
    let res = client
        .index(IndexParts::Index(index))
        .refresh(Refresh::True)
        .body(json!({}))
        .send()
        .await;
    if res.is_err() {
        return Err(ElasticError::Response(res.err().unwrap().to_string()));
    }
    return Ok(res.unwrap());
}

pub async fn first_search<T>(
    index: &str,
    query_builder: QueryBuilder,
) -> Result<Option<Hit<T>>, ElasticError>
where
    T: DeserializeOwned + 'static,
{
    let client = el_client()?;
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
                    return Err(ElasticError::Response(v.to_string()));
                }
            };
        }
        Err(_) => Ok(None),
    };
}

pub async fn scroll<T>(
    scroll_id: &str,
    alive: &str,
) -> Result<Option<SearchResponse<T>>, ElasticError>
where
    T: DeserializeOwned + 'static,
{
    let client = el_client()?;
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
                    return Err(ElasticError::Response(v.to_string()));
                }
            };
        }
        Err(_) => Ok(None),
    }
}

pub async fn search<T>(
    index: &str,
    query_builder: QueryBuilder,
) -> Result<Option<SearchResponse<T>>, ElasticError>
where
    T: DeserializeOwned + 'static,
{
    let client = el_client()?;
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
                        return Err(ElasticError::JsonParse(v.to_string()));
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
                    return Err(ElasticError::JsonParse(v.to_string()));
                }
            };
        }
        Err(_) => Ok(None),
    }
}
