use datafusion_dist_integration_tests::utils::assert_planner_output;

#[tokio::test]
async fn join_planning() {
    assert_planner_output(
        "select * from simple as t1 join simple as t2",
        r#"===============Stage 0===============
CrossJoinExec
  CoalescePartitionsExec
    UnresolvedExec: delegated_plan=DataSourceExec, delegated_stage=1
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
===============Stage 1===============
DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;
}
