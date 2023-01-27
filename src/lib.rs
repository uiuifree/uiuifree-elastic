pub mod error;

use crate::error::ElasticError;
use dotenv::dotenv;
use elastic_parser::{Doc, Hit, SearchResponse, Shards};
use elastic_query_builder::QueryBuilder;
use elasticsearch::http::request::JsonBody;
use elasticsearch::http::response::Response;
use elasticsearch::http::transport::{SingleNodeConnectionPool, Transport};
use elasticsearch::indices::{IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts, IndicesRefreshParts};
use elasticsearch::{BulkParts, DeleteByQueryParts, DeleteParts, Error, GetParts, GetSourceParts, IndexParts, ScrollParts, SearchParts, UpdateByQueryParts, UpdateParts};
use serde::de::DeserializeOwned;
use serde::{Serialize, Deserialize};
use serde_json::{json, Value};
use std::env;
pub use elasticsearch::http::transport::*;
pub use elasticsearch::Elasticsearch;

extern crate serde;
// #[macro_use]
extern crate serde_derive;
extern crate serde_json;

pub use elastic_parser;
pub use elastic_query_builder;
pub use elasticsearch::http::Url;
use elasticsearch::params::Refresh;

pub fn el_client() -> Result<Elasticsearch, ElasticError> {
    dotenv().ok();
    let host = env::var("ELASTIC_HOST").unwrap_or("http://localhost:9200".to_string());
    let transport = Transport::single_node(host.as_str());
    // let transport = SingleNodeConnectionPool::new(Url::parse(host.as_str()).unwrap());

    return match transport {
        Ok(v) => Ok(Elasticsearch::new(v)),
        Err(e) => Err(ElasticError::Connection(e.to_string())),
    };
}

pub fn el_single_node(url: &str) -> Elasticsearch {
    let pool = SingleNodeConnectionPool::new(Url::parse(url).unwrap());
    Elasticsearch::new(TransportBuilder::new(pool).build().unwrap())
}

fn bool_to_refresh(value: bool) -> Refresh {
    match value {
        true => Refresh::True,
        false => Refresh::False,
    }
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


pub struct ElasticApi {
    client: Elasticsearch,
}

impl ElasticApi {
    pub fn new(client: Elasticsearch) -> ElasticApi {
        ElasticApi {
            client
        }
    }
    pub fn get(&self) -> GetApi {
        GetApi::new(&self)
    }
    pub fn update(&self) -> UpdateApi {
        UpdateApi::new(&self)
    }
    pub fn indices(&self) -> IndicesApi {
        IndicesApi::new(&self)
    }
    pub fn search(&self) -> SearchApi {
        SearchApi::new(&self)
    }
    pub fn bulk(&self) -> BulkApi {
        BulkApi::new(&self)
    }
    pub fn index(&self) -> IndexApi {
        IndexApi::new(&self)
    }
    pub fn delete_by_query(&self) -> DeleteByQueryApi {
        DeleteByQueryApi::new(&self)
    }
    pub fn update_by_query(&self) -> UpdateByQuery {
        UpdateByQuery::new(&self)
    }
}

pub struct SearchApi<'a> {
    api: &'a ElasticApi,
}

impl SearchApi<'_> {
    pub fn new(api: &ElasticApi) -> SearchApi {
        SearchApi {
            api
        }
    }
}

pub struct GetApi<'a> {
    api: &'a ElasticApi,
}

impl GetApi<'_> {
    pub fn new(api: &ElasticApi) -> GetApi {
        GetApi {
            api
        }
    }
}

pub struct DeleteApi<'a> {
    api: &'a ElasticApi,
}

impl DeleteApi<'_> {
    pub fn new(api: &ElasticApi) -> DeleteApi {
        DeleteApi {
            api
        }
    }
}

pub struct DeleteDocResponse {}

impl DeleteApi<'_> {
    pub async fn doc(&self, index: &str, id: &str) -> Result<Response, ElasticError> {
        let res = self.api.client.delete(DeleteParts::IndexId(index, id)).send().await;
        if res.is_err() {
            return Err(ElasticError::Response(res.err().unwrap().to_string()));
        }
        return Ok(res.unwrap());
    }
}


impl SearchApi<'_> {
    pub async fn search<T>(
        &self,
        index: &[&str],
        query_builder: &QueryBuilder,
    ) -> Result<Option<SearchResponse<T>>, ElasticError>
        where
            T: DeserializeOwned + 'static + Clone,
    {
        if !query_builder.get_scroll().is_empty() {
            return match self.api.client
                .search(SearchParts::Index(index))
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

        let res = self.api.client
            .search(SearchParts::Index(&index))
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
            T: DeserializeOwned + 'static + Clone,
    {
        match self.api.client
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
            T: DeserializeOwned + 'static + Clone,
    {
        return match self.api.client
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

pub struct IndicesApi<'a> {
    api: &'a ElasticApi,
}

impl IndicesApi<'_> {
    pub fn new(api: &ElasticApi) -> IndicesApi {
        IndicesApi {
            api
        }
    }
}

impl IndicesApi<'_> {
    pub async fn exists(&self, index: &str) -> Result<(), ElasticError> {
        let res = self.api.client
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
        let res = self.api.client
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
        return match self.api.client
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
        let res = self.api.client
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
        if self.api.indices().exists(index).await.is_ok() {
            let _ = self.api.indices().delete(index).await?;
        }
        Ok(self.api.indices().create(index, json).await?)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct IndicesRefreshResponse {
    pub _shards: Option<Shards>,
}


impl GetApi<'_> {
    pub async fn source<T: for<'de> serde::Deserialize<'de>>(&self, index: &str, id: &str) -> Result<T, ElasticError> {
        let res = self.api.client
            .get_source(GetSourceParts::IndexId(index, id))
            .send()
            .await;
        parse_response(res).await
    }
    pub async fn doc<T: for<'de> serde::Deserialize<'de>>(&self, index: &str, id: &str) -> Result<Doc<T>, ElasticError> {
        let res = self.api.client
            .get(GetParts::IndexId(index, id))
            .send()
            .await;

        parse_response::<Doc<T>>(res).await
    }
}


pub struct UpdateApi<'a> {
    api: &'a ElasticApi,
}

impl UpdateApi<'_> {
    pub fn new(api: &ElasticApi) -> UpdateApi {
        UpdateApi {
            api
        }
    }
}


impl UpdateApi<'_> {
    pub async fn doc<T: serde::Serialize>(
        &self,
        index: &str,
        id: &str,
        source: T,
        refresh: bool,
    ) -> Result<(), ElasticError> {
        let res = self.api.client
            .update(UpdateParts::IndexId(index, id))
            .refresh(bool_to_refresh(refresh))
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


pub struct BulkApi<'a> {
    api: &'a ElasticApi,
}

impl BulkApi<'_> {
    pub fn new(api: &ElasticApi) -> BulkApi {
        BulkApi {
            api
        }
    }
}


impl BulkApi<'_> {
    pub async fn bulk<T: serde::Serialize>(
        &self,
        sources: Vec<T>,
        refresh: bool,
    ) -> Result<Value, ElasticError>
    {
        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(4);
        for source in sources {
            body.push(json!(source).into())
        }

        parse_response(self.api.client.bulk(BulkParts::None).body(body).refresh(bool_to_refresh(refresh)).send().await).await
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
        let res = self.api.client.bulk(BulkParts::Index(index)).body(body).send().await;
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
        refresh:bool
    ) -> Result<Response, ElasticError> {
        let mut body: Vec<JsonBody<_>> = Vec::with_capacity(4);
        body.push(json!({"index": {"_id":id}}).into());
        body.push(json!(source).into());

        let res = self.api.client.bulk(BulkParts::Index(index)).body(body).refresh(bool_to_refresh(refresh)).send().await;
        if res.is_err() {
            return Err(ElasticError::Response(res.err().unwrap().to_string()));
        }
        return Ok(res.unwrap());
    }
}

/// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
pub struct IndexApi<'a> {
    api: &'a ElasticApi,
}

impl IndexApi<'_> {
    pub fn new(api: &ElasticApi) -> IndexApi {
        IndexApi {
            api
        }
    }
}

impl IndexApi<'_> {
    pub async fn create<T: serde::Serialize>(
        &self,
        index: &str,
        source: T,
        refresh: bool,
    ) -> Result<(), ElasticError> {
        let res = self.api.client
            .index(IndexParts::Index(index))
            .refresh(bool_to_refresh(refresh))
            .body(source)
            .send()
            .await;
        if res.is_err() {
            return Err(ElasticError::Response(res.unwrap_err().to_string()));
        }
        let code = res.as_ref().unwrap().status_code();
        if code == 404 {
            return Err(ElasticError::NotFound(format!("not found entity")));
        }
        let res = res.unwrap();
        // println!("status: {}",code);
        if res.status_code() != 200 && res.status_code() != 201 {
            return Err(ElasticError::Response(res.text().await.unwrap_or_default()));
        }
        Ok(())
    }
    pub async fn doc<T: serde::Serialize>(
        &self,
        index: &str,
        id: &str,
        source: T,
        refresh: bool,
    ) -> Result<(), ElasticError> {
        let res = self.api.client
            .index(IndexParts::IndexId(index, id))
            .refresh(bool_to_refresh(refresh))
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

/// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-update-by-query.html
pub struct UpdateByQuery<'a> {
    api: &'a ElasticApi,
}

impl UpdateByQuery<'_> {
    pub fn new(api: &ElasticApi) -> UpdateByQuery {
        UpdateByQuery {
            api
        }
    }
}

impl UpdateByQuery<'_> {
    pub async fn index(
        &self,
        index: &str,
        query_builder: &QueryBuilder,
        refresh: bool,
    ) -> Result<(), ElasticError> {
        let res = self.api.client
            .update_by_query(UpdateByQueryParts::Index(&[index]))
            .refresh(refresh)
            .body(query_builder.build())
            .send()
            .await;
        if res.is_err() {
            return Err(ElasticError::Response(res.unwrap_err().to_string()));
        }
        let code = res.as_ref().unwrap().status_code();
        if code == 404 {
            return Err(ElasticError::NotFound(format!("not found entity")));
        }
        let res = res.unwrap();
        if res.status_code() != 200 {
            return Err(ElasticError::Response(res.text().await.unwrap_or_default()));
        }
        Ok(())
    }
}


/// https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-delete-by-query.html
pub struct DeleteByQueryApi<'a> {
    api: &'a ElasticApi,
}

impl DeleteByQueryApi<'_> {
    pub fn new(api: &ElasticApi) -> DeleteByQueryApi {
        DeleteByQueryApi {
            api
        }
    }
}

impl DeleteByQueryApi<'_> {
    pub async fn index(
        &self,
        index: &str,
        query_builder: &QueryBuilder,
        refresh:bool
    ) -> Result<(), ElasticError> {
        let res = self.api.client
            .delete_by_query(DeleteByQueryParts::Index(&[index]))
            .body(query_builder.build())
            .refresh(refresh)
            .send()
            .await;
        if res.is_err() {
            return Err(ElasticError::Response(res.unwrap_err().to_string()));
        }
        let code = res.as_ref().unwrap().status_code();
        if code == 404 {
            return Err(ElasticError::NotFound(format!("not found index")));
        }
        let res = res.unwrap();
        // println!("status: {}",code);
        if res.status_code() != 200 && res.status_code() != 201 {
            return Err(ElasticError::Response(res.text().await.unwrap_or_default()));
        }
        Ok(())
    }
}

