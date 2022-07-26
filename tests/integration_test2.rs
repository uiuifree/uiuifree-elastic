use uiuifree_elastic::{el_client, ElasticApi};
use serde_json::{ Value};
use std::time::{ Instant};
#[tokio::test]
pub async fn case01() {
    let client = el_client().unwrap();
    let api  = ElasticApi::new(client);
    let data = api.get().doc::<Value>("test_case","1").await;
    println!("{:?}",data);
    assert!(true);
}

#[tokio::test]
pub async fn case02() {
    let start = Instant::now();
    // let transport = Transport::single_node("http://localhost:9200");
    // let els = Elasticsearch::new(transport.unwrap() );
    // for _ in 1..10000{
    //     assert!(els.indices().exists(IndicesExistsParts::Index(&["test"])).send().await.is_ok());
    // }

    // let client = el_single_node("http://localhost:9200");
    let api  = ElasticApi::new(el_client().unwrap());
    for _ in 1..1000{
        assert!(api.indices().exists("test_case").await.is_ok())
    }

    let end = start.elapsed();
    println!("{}.{:03}秒経過しました。", end.as_secs(), end.subsec_nanos() / 1_000_000);
    assert!(true);
}

