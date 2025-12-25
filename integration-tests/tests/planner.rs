use datafusion_dist_integration_tests::utils::{
    assert_planner, assert_stage_distributed_into_nodes, assert_stage_distributed_into_one_node,
    mock_alive_nodes, schedule_tasks,
};

#[tokio::test]
async fn full_table_scan_planning() -> Result<(), Box<dyn std::error::Error>> {
    let stage_plans = assert_planner(
        "select * from simple",
        "DataSourceExec: partitions=2, partition_sizes=[1, 1]\n",
        r#"===============Stage 0 (partitions=2)===============
DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;

    let nodes = mock_alive_nodes();
    let distribution = schedule_tasks(&nodes, &stage_plans).await?;
    assert_stage_distributed_into_nodes(0, &distribution, 2);
    Ok(())
}
#[tokio::test]
async fn cross_join_planning() -> Result<(), Box<dyn std::error::Error>> {
    let stage_plans = assert_planner(
        "select * from simple as t1 join simple as t2",
        r#"CrossJoinExec
  CoalescePartitionsExec
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
        r#"===============Stage 0 (partitions=2)===============
CrossJoinExec
  CoalescePartitionsExec
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;

    let nodes = mock_alive_nodes();
    let distribution = schedule_tasks(&nodes, &stage_plans).await?;
    assert_stage_distributed_into_one_node(0, &distribution);
    Ok(())
}

#[tokio::test]
async fn unio_planning() -> Result<(), Box<dyn std::error::Error>> {
    let stage_plans = assert_planner(
        "select * from simple union select * from simple",
        r#"AggregateExec: mode=FinalPartitioned, gby=[name@0 as name, age@1 as age], aggr=[]
  CoalesceBatchesExec: target_batch_size=8192
    RepartitionExec: partitioning=Hash([name@0, age@1], 12), input_partitions=12
      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=4
        AggregateExec: mode=Partial, gby=[name@0 as name, age@1 as age], aggr=[]
          UnionExec
            DataSourceExec: partitions=2, partition_sizes=[1, 1]
            DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
        r#"===============Stage 0 (partitions=12)===============
AggregateExec: mode=FinalPartitioned, gby=[name@0 as name, age@1 as age], aggr=[]
  CoalesceBatchesExec: target_batch_size=8192
    RepartitionExec: partitioning=Hash([name@0, age@1], 12), input_partitions=12
      RepartitionExec: partitioning=RoundRobinBatch(12), input_partitions=4
        AggregateExec: mode=Partial, gby=[name@0 as name, age@1 as age], aggr=[]
          UnresolvedExec: delegated_plan=UnionExec, delegated_stage=1
===============Stage 1 (partitions=4)===============
UnionExec
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;

    let nodes = mock_alive_nodes();
    let distribution = schedule_tasks(&nodes, &stage_plans).await?;
    assert_stage_distributed_into_one_node(0, &distribution);
    assert_stage_distributed_into_nodes(1, &distribution, nodes.len());

    Ok(())
}
