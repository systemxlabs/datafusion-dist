use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion_dist::planner::{DefaultPlanner, DisplayableStagePlans, DistPlanner};
use datafusion_dist_integration_tests::{
    data::build_session_context,
    utils::{
        assert_planner, assert_stage_distributed_into_nodes,
        assert_stage_distributed_into_one_node, mock_alive_nodes, schedule_tasks,
    },
};
use uuid::Uuid;

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

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_nodes(0, &distribution, 2);
    Ok(())
}
#[tokio::test]
async fn cross_join() -> Result<(), Box<dyn std::error::Error>> {
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

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_one_node(0, &distribution);
    Ok(())
}

#[tokio::test]
async fn nested_loop_join_planning() -> Result<(), Box<dyn std::error::Error>> {
    let stage_plans = assert_planner(
        "select * from simple as t1 join simple as t2 on t1.age > t2.age",
        r#"NestedLoopJoinExec: join_type=Inner, filter=age@0 > age@1
  CoalescePartitionsExec
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
        r#"===============Stage 0 (partitions=2)===============
NestedLoopJoinExec: join_type=Inner, filter=age@0 > age@1
  CoalescePartitionsExec
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_one_node(0, &distribution);
    Ok(())
}

#[tokio::test]
async fn hash_join_collect_left() -> Result<(), Box<dyn std::error::Error>> {
    let stage_plans = assert_planner(
        "select * from simple as t1 join simple as t2 on t1.name = t2.name",
        r#"CoalesceBatchesExec: target_batch_size=8192
  HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(name@0, name@0)]
    CoalescePartitionsExec
      DataSourceExec: partitions=2, partition_sizes=[1, 1]
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
        r#"===============Stage 0 (partitions=2)===============
CoalesceBatchesExec: target_batch_size=8192
  HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(name@0, name@0)]
    CoalescePartitionsExec
      DataSourceExec: partitions=2, partition_sizes=[1, 1]
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_one_node(0, &distribution);
    Ok(())
}

#[tokio::test]
async fn hash_join_partitioned() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = build_session_context();
    let state = ctx.state_ref();
    let mut state = state.write();
    state.config_mut().options_mut().set(
        "datafusion.optimizer.hash_join_single_partition_threshold",
        "0",
    )?;
    state.config_mut().options_mut().set(
        "datafusion.optimizer.hash_join_single_partition_threshold_rows",
        "0",
    )?;
    drop(state);

    let plan = ctx
        .sql("select * from simple as t1 join simple as t2 on t1.name = t2.name")
        .await?
        .create_physical_plan()
        .await?;
    let plan_str = DisplayableExecutionPlan::new(plan.as_ref())
        .indent(true)
        .to_string();
    println!("Physical plan: {plan_str}");
    assert_eq!(
        plan_str,
        r#"CoalesceBatchesExec: target_batch_size=8192
  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(name@0, name@0)]
    CoalesceBatchesExec: target_batch_size=8192
      RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
        DataSourceExec: partitions=2, partition_sizes=[1, 1]
    CoalesceBatchesExec: target_batch_size=8192
      RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
        DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    );

    let dist_planner = DefaultPlanner {};
    let stage_plans = dist_planner.plan_stages(Uuid::new_v4(), plan).unwrap();
    let actual = DisplayableStagePlans(&stage_plans).to_string();
    println!("Planner output: {actual}");
    assert_eq!(
        actual,
        r#"===============Stage 0 (partitions=12)===============
CoalesceBatchesExec: target_batch_size=8192
  HashJoinExec: mode=Partitioned, join_type=Inner, on=[(name@0, name@0)]
    CoalesceBatchesExec: target_batch_size=8192
      RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
        DataSourceExec: partitions=2, partition_sizes=[1, 1]
    CoalesceBatchesExec: target_batch_size=8192
      RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
        DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#
    );
    //     assert_eq!(
    //         actual,
    //         r#"===============Stage 0 (partitions=12)===============
    // CoalesceBatchesExec: target_batch_size=8192
    //   HashJoinExec: mode=Partitioned, join_type=Inner, on=[(name@0, name@0)]
    //     UnresolvedExec: delegated_plan=CoalesceBatchesExec, delegated_stage=2
    //     UnresolvedExec: delegated_plan=CoalesceBatchesExec, delegated_stage=1
    // ===============Stage 1 (partitions=12)===============
    // CoalesceBatchesExec: target_batch_size=8192
    //   RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
    //     DataSourceExec: partitions=2, partition_sizes=[1, 1]
    // ===============Stage 2 (partitions=12)===============
    // CoalesceBatchesExec: target_batch_size=8192
    //   RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
    //     DataSourceExec: partitions=2, partition_sizes=[1, 1]
    // "#,
    //     );

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_one_node(0, &distribution);
    // assert_stage_distributed_into_nodes(0, &distribution, nodes.len());
    // assert_stage_distributed_into_one_node(1, &distribution);
    // assert_stage_distributed_into_one_node(2, &distribution);
    Ok(())
}

#[tokio::test]
async fn union_aggregate() -> Result<(), Box<dyn std::error::Error>> {
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
        UnresolvedExec: delegated_plan=AggregateExec, delegated_stage=1
===============Stage 1 (partitions=4)===============
AggregateExec: mode=Partial, gby=[name@0 as name, age@1 as age], aggr=[]
  UnionExec
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_one_node(0, &distribution);
    assert_stage_distributed_into_nodes(1, &distribution, nodes.len());

    Ok(())
}

#[tokio::test]
async fn sort_with_preserve_partitioning() -> Result<(), Box<dyn std::error::Error>> {
    let stage_plans = assert_planner(
        "select * from simple order by age",
        r#"SortPreservingMergeExec: [age@1 ASC NULLS LAST]
  SortExec: expr=[age@1 ASC NULLS LAST], preserve_partitioning=[true]
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
        r#"===============Stage 0 (partitions=1)===============
SortPreservingMergeExec: [age@1 ASC NULLS LAST]
  UnresolvedExec: delegated_plan=SortExec, delegated_stage=1
===============Stage 1 (partitions=2)===============
SortExec: expr=[age@1 ASC NULLS LAST], preserve_partitioning=[true]
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_one_node(0, &distribution);
    assert_stage_distributed_into_nodes(1, &distribution, 2);
    Ok(())
}
