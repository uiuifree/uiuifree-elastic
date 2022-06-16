pub mod error;

use crate::error::ElasticError;
use dotenv::dotenv;
use elastic_parser::{Doc, Hit, SearchResponse, Shards};
use elastic_query_builder::QueryBuilder;
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::response::Response;
use elasticsearch::http::transport::Transport;
use elasticsearch::indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts, IndicesRefreshParts};
use elasticsearch::{BulkParts, DeleteParts, Elasticsearch, Error, GetParts, IndexParts, ScrollParts, SearchParts, UpdateParts};
use serde::de::DeserializeOwned;
use serde::{Serialize, Deserialize};
use serde_json::{json, Value};
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

async fn parse_response<T: for<'de> serde::Deserialize<'de>>(input: Result<Response, Error>) -> Result<T, ElasticError> {
    if input.is_err() {
        return Err(ElasticError::Send(input.unwrap_err().to_string()));
    }
    let input = input.unwrap();
    let status_code = input.status_code().as_u16();
    if status_code != 200 {
        let c = input.text().await;
        return Err(ElasticError::Status(status_code, c.unwrap()));
    }
    let a = input.json::<Value>().await;
    if a.is_err() {
        return Err(ElasticError::JsonParse(a.err().unwrap().to_string()));
    }
    let v = a.unwrap();
    let b = serde_json::from_value(v.clone());
    if b.is_err() {
        return Err(ElasticError::JsonParse(v.to_string()));
    }
    return Ok(b.unwrap());
}


pub struct ElasticApi {}

impl ElasticApi {
    pub fn get() -> GetApi {
        GetApi::default()
    }
    pub fn update() -> UpdateApi {
        UpdateApi::default()
    }
    pub fn indices() -> IndicesApi {
        IndicesApi::default()
    }
    pub fn search() -> SearchApi {
        SearchApi::default()
    }
    pub fn bulk() -> BulkApi {
        BulkApi::default()
    }
    pub fn index() -> IndexApi {
        IndexApi::default()
    }
}

#[derive(Default)]
pub struct DeleteApi {}

pub struct DeleteDocResponse {}

impl DeleteApi {
    pub async fn doc(&self, index: &str, id: &str) -> Result<Response, ElasticError> {
        let client = el_client()?;
        let res = client.delete(DeleteParts::IndexId(index, id)).send().await;
        if res.is_err() {
            return Err(ElasticError::Response(res.err().unwrap().to_string()));
        }
        return Ok(res.unwrap());
    }
}


#[derive(Default)]
pub struct SearchApi {}

impl SearchApi {
    pub async fn search<T>(
        &self,
        index: &str,
        query_builder: &QueryBuilder,
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

        let res = client
            .search(SearchParts::Index(&[index]))
            .body(query_builder.build())
            .from(query_builder.get_from())
            .size(query_builder.get_size())
            .send()
            .await;
        parse_response(res).await
    }
    pub async fn scroll<T>(
        &self,
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

    pub async fn first_search<T>(
        &self,
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
                        let hits = v.hits;
                        if hits.is_none() {
                            return Ok(None);
                        }
                        let hits = hits.unwrap();
                        let value = hits.hits;
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
}

#[derive(Default)]
pub struct IndicesApi {}

impl IndicesApi {
    pub async fn exists(&self, index: &str) -> Result<(), ElasticError> {
        let res = el_client()?
            .indices().exists(IndicesExistsParts::Index(&[index]))
            .send()
            .await;
        // el_client()?.index(IndexParts::IndexId("1","1")).body()

        if res.is_err() {
            return Err(ElasticError::Connection(res.unwrap_err().to_string()));
        }
        if res.unwrap().status_code() != 200 {
            return Err(ElasticError::NotFound(index.to_string()));
        }
        Ok(())
    }
    pub async fn refresh(&self, index: &str) -> Result<IndicesRefreshResponse, ElasticError> {
        let client = el_client()?;
        let res = client
            .indices()
            .refresh(IndicesRefreshParts::Index(&[index]))
            .send()
            .await;
        if res.is_err() {
            return Err(ElasticError::Connection(res.err().unwrap().to_string()));
        }

        parse_response(res).await
    }
    pub async fn create<T>(&self, index: &str, json: T) -> Result<bool, ElasticError>
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
            Ok(v) => {
                let s = v.status_code();
                Ok(s == 200)
            }
            Err(e) => Err(ElasticError::Send(e.to_string())),
        };
    }


    pub async fn delete(&self, index: &str) -> Result<bool, ElasticError>
    {
        let res = el_client()?
            .indices()
            .delete(IndicesDeleteParts::Index(&[index]))
            .send()
            .await;
        if res.is_err() {
            return Err(ElasticError::Response(res.err().unwrap().to_string()));
        }
        let res = res.unwrap();
        Ok(res.status_code() == 200)
    }

    pub async fn recreate<T>(&self, index: &str, json: T) -> Result<bool, ElasticError>
        where
            T: Serialize,
    {
        if ElasticApi::indices().exists(index).await.is_ok() {
            let _ = ElasticApi::indices().delete(index).await?;
        }
        Ok(ElasticApi::indices().create(index, json).await?)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndicesRefreshResponse {
    pub _shards: Option<Shards>,
}

#[derive(Default)]
pub struct GetApi {}

impl GetApi {
    pub async fn source<T: for<'de> serde::Deserialize<'de>>(&self, index: &str, id: &str) -> Result<T, ElasticError> {
        let res = el_client()?
            .get(GetParts::IndexTypeId(index, "_source", id))
            .send()
            .await;
        parse_response(res).await
    }
    pub async fn doc<T: for<'de> serde::Deserialize<'de>>(&self, index: &str, id: &str) -> Result<Doc<T>, ElasticError> {
        let res = el_client()?
            .get(GetParts::IndexTypeId(index, "_doc", id))
            .send()
            .await;

        parse_response::<Doc<T>>(res).await
    }
}


#[derive(Default)]
pub struct UpdateApi {}

impl UpdateApi {
    pub async fn doc<T: serde::Serialize>(
        &self,
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
}

#[derive(Default)]
pub struct BulkApi {}

impl BulkApi {
    pub async fn bulk<T: serde::Serialize>(
        &self,
        sources: Vec<T>,
    ) -> Result<Value, ElasticError>
    {
        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(4);
        for source in sources {
            body.push(json!(source).into())
        }
        let client = el_client()?;
        parse_response(client.bulk(BulkParts::None).body(body).send().await).await
        // let res = client.bulk(BulkParts::None).body(body).send().await;
        // if res.is_err() {
        //     return Err(ElasticError::Response(res.err().unwrap().to_string()));
        // }
        //
        // return Ok(res.unwrap());
    }
    pub async fn insert_index<T: serde::Serialize>(
        &self,
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
    pub async fn insert_index_by_id<T: serde::Serialize>(
        &self,
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
}

/// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
#[derive(Default)]
pub struct IndexApi {}

impl IndexApi {
    pub async fn doc<T: serde::Serialize>(
        &self,
        index: &str,
        id: &str,
        source: T,
    ) -> Result<(), ElasticError> {
        let client = el_client()?;
        let res = client
            .index(IndexParts::IndexId(index, id))
            .body(source)
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
        // println!("status: {}",code);
        if res.status_code() != 200 && res.status_code() != 201 {
            return Err(ElasticError::Response(res.text().await.unwrap_or_default()));
        }
        Ok(())
    }
}

