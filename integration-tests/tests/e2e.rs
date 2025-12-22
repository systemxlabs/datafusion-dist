use datafusion_dist_integration_tests::{setup_containers, utils::assert_e2e};

#[tokio::test(flavor = "multi_thread")]
async fn test_join() {
    setup_containers().await;

    assert_e2e(
        "select * from simple as t1 join simple as t2",
        r#"+-------+-----+-------+-----+
| name  | age | name  | age |
+-------+-----+-------+-----+
| Alice | 25  | Bob   | 30  |
| Bob   | 30  | Bob   | 30  |
| Bob   | 30  | Alice | 25  |
| Alice | 25  | Alice | 25  |
+-------+-----+-------+-----+"#,
    )
    .await;
}
