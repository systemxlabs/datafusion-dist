use datafusion_dist_integration_tests::utils::assert_planner;

#[tokio::test]
async fn join_planning() {
    assert_planner(
        "select * from simple as t1 join simple as t2",
        r#"===============Stage 0 (partitions=2)===============
CrossJoinExec
  CoalescePartitionsExec
    UnresolvedExec: delegated_plan=DataSourceExec, delegated_stage=1
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
===============Stage 1 (partitions=2)===============
DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;
}

#[tokio::test]
async fn unio_planning() {
    assert_planner(
        "select * from simple union select * from simple",
        r#"===============Stage 0 (partitions=12)===============
AggregateExec: mode=FinalPartitioned, gby=[name@0 as name, age@1 as age], aggr=[]
  CoalesceBatchesExec: target_batch_size=8192
    RepartitionExec: partitioning=Hash([name@0, age@1], 12), input_partitions=4
      UnresolvedExec: delegated_plan=AggregateExec, delegated_stage=1
===============Stage 1 (partitions=4)===============
AggregateExec: mode=Partial, gby=[name@0 as name, age@1 as age], aggr=[]
  UnionExec
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;
}
