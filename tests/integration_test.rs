use uiuifree_elastic::{ElasticApi};
use serde::{Serialize, Deserialize};
use serde_json::{json};
use elastic_query_builder::query::match_query::MatchQuery;
use elastic_query_builder::QueryBuilder;

#[derive(Debug,Default, Clone, Serialize, Deserialize)]
struct TestData {
    name: Option<String>,
}
#[derive(Debug,Default, Clone, Serialize, Deserialize)]
struct TestData2 {
    name: Option<String>,
    created_at: Option<String>,
}

#[tokio::test]
pub async fn case03() {
    let test_index = "test_case";
    let test1 = TestData2 {
        name: Some("テストデータ24".to_string()),
        created_at:None
        // created_at: Some("2020-01-01 00:00:00".to_string()),
    };
    let res = ElasticApi::index().doc(test_index,"25",&test1.clone()).await;
    assert!(res.is_ok(),"{}",res.err().unwrap().to_string());
    let res = ElasticApi::index().doc(test_index,"25",&test1.clone()).await;
    assert!(res.is_ok(),"{}",res.err().unwrap().to_string());

}



#[tokio::test]
pub async fn case01() {
    let test_index = "test_case";
    let test_id = "2";

    // INDEX API テストケース
    assert!(ElasticApi::indices().exists("hoge").await.is_err(), "found hoge");
    if ElasticApi::indices().exists(test_index).await.is_ok() {
        assert!(ElasticApi::indices().delete(test_index).await.is_ok(), "削除が失敗しました。")
    }
    assert!(ElasticApi::indices().create(test_index, json!({
        "mappings": {
                    "properties": {
                      "name": {
                        "type": "keyword"
                      },
                      "created_at": {
                        "type": "date"
                      },
                    }
                  }
    })).await.is_ok(), "Index作成");
    // refresh
    let refresh = ElasticApi::indices().refresh(test_index).await;
    assert!(refresh.is_ok(), "Index作成 {}", refresh.unwrap_err().to_string());

    // BulkAPI テストケース
    let test1 = TestData {
        name: Some("テストデータ1".to_string())
    };

    let insert = ElasticApi::bulk().insert_index_by_id(test_index, "1", test1.clone()).await;
    assert!(insert.is_ok(), "INSERT");
    let insert = ElasticApi::bulk().insert_index_by_id(test_index, test_id, test1.clone()).await;
    assert!(insert.is_ok(), "INSERT");

    let test2 = TestData {
        name: Some("テストデータ2".to_string())
    };
    let refresh = ElasticApi::indices().refresh(test_index).await;
    assert!(refresh.is_ok(), "refresh {}", refresh.unwrap_err().to_string());
    let insert = ElasticApi::bulk().insert_index_by_id(test_index, test_id, test2.clone()).await;
    assert!(insert.is_ok(), "INSERT");
    let refresh = ElasticApi::indices().refresh(test_index).await;
    assert!(refresh.is_ok(), "Index作成 {}", refresh.unwrap_err().to_string());


    // GET API テストケース
    let get = ElasticApi::get().doc::<TestData>(test_index, test_id).await;
    assert!(get.is_ok(), "{}", get.unwrap_err().to_string());
    let get = get.unwrap();
    assert!(get._source.is_some() && get._source.unwrap().name == test2.name, "name not found ");

    let get = ElasticApi::get().source::<TestData>(test_index, test_id).await;
    assert!(get.is_ok(), "{}", get.unwrap_err().to_string());
    let get = get.unwrap();
    assert_eq!(get.name, test2.name, "name not found ");


    // Search API テストケース
    let mut builder = QueryBuilder::new();
    builder.set_query(MatchQuery::new("name", "テストデータ2"));

    let res = ElasticApi::search().search::<TestData>(test_index, &builder).await;
    let res = res.unwrap().unwrap().hits.unwrap().hits.unwrap();
    assert_ne!(0, res.len());
    for hit in res {
        let name = hit._source.unwrap_or_default().name.unwrap();
        assert_eq!(name, "テストデータ2");
    }
    // sort
    let mut builder = QueryBuilder::new();
    builder.set_sort(json!([
        {"name":"desc"}
    ]));

    let res = ElasticApi::search().search::<TestData>(test_index, &builder).await;
    assert!(res.is_ok(),"{:?}",res.err().unwrap());

    let res = res.unwrap().unwrap();
    let total = res.total_value();
    let sources =res.sources();
    assert_ne!(0, sources.len());
    assert_eq!(total, sources.len());
    for source in sources{
        assert!( source.name.is_some());
    }

    // Bulk Insert
    let refresh = ElasticApi::indices().refresh(test_index).await;
    assert!(refresh.is_ok(), "refresh {}", refresh.unwrap_err().to_string());

    let values = vec![
        json!({"delete":{"_index":test_index,"_id":test_id}}),
        json!({"create":{"_index":test_index,"_id":"3"}}),
        json!({"name":"bulk name3"}),
        json!({"create":{"_index":test_index,"_id":"4"}}),
        json!({"name":"bulk name4"}),
    ];
    let e = ElasticApi::bulk().bulk(values).await;
    assert!(e.is_ok(), "{}", e.err().unwrap().to_string());
}

