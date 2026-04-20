use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion_dist::planner::{DefaultPlanner, DisplayableStagePlans, DistPlanner};
use datafusion_dist_integration_tests::{
    data::build_session_context,
    utils::{
        assert_planner, assert_stage_distributed_into_nodes_v2, mock_alive_nodes, schedule_tasks,
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
    assert_stage_distributed_into_nodes_v2(0, &nodes, &distribution, &[1, 1, 0]);
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
    assert_stage_distributed_into_nodes_v2(0, &nodes, &distribution, &[2, 0, 0]);
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
    assert_stage_distributed_into_nodes_v2(0, &nodes, &distribution, &[2, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn hash_join_collect_left() -> Result<(), Box<dyn std::error::Error>> {
    let stage_plans = assert_planner(
        "select * from simple as t1 join simple as t2 on t1.name = t2.name",
        r#"HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(name@0, name@0)]
  CoalescePartitionsExec
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
        r#"===============Stage 0 (partitions=2)===============
HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(name@0, name@0)]
  CoalescePartitionsExec
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_nodes_v2(0, &nodes, &distribution, &[2, 0, 0]);
    Ok(())
}

#[tokio::test]
async fn hash_join_partitioned() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = build_session_context();
    {
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
    }

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
        r#"HashJoinExec: mode=Partitioned, join_type=Inner, on=[(name@0, name@0)]
  RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
  RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    );

    let dist_planner = DefaultPlanner {};
    let stage_plans = dist_planner
        .plan_stages(Uuid::new_v4().to_string().as_str().into(), plan)
        .unwrap();
    let actual = DisplayableStagePlans(&stage_plans).to_string();
    println!("Planner output: {actual}");
    assert_eq!(
        actual,
        r#"===============Stage 0 (partitions=12)===============
HashJoinExec: mode=Partitioned, join_type=Inner, on=[(name@0, name@0)]
  UnresolvedExec: delegated_plan=RepartitionExec, delegated_stage=2
  UnresolvedExec: delegated_plan=RepartitionExec, delegated_stage=1
===============Stage 1 (partitions=12)===============
RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
===============Stage 2 (partitions=12)===============
RepartitionExec: partitioning=Hash([name@0], 12), input_partitions=2
  DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    );

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_nodes_v2(0, &nodes, &distribution, &[4, 4, 4]);
    assert_stage_distributed_into_nodes_v2(1, &nodes, &distribution, &[12, 0, 0]);
    assert_stage_distributed_into_nodes_v2(2, &nodes, &distribution, &[0, 12, 0]);
    Ok(())
}

#[tokio::test]
async fn union_aggregate() -> Result<(), Box<dyn std::error::Error>> {
    let stage_plans = assert_planner(
        "select * from simple union select * from simple",
        r#"AggregateExec: mode=FinalPartitioned, gby=[name@0 as name, age@1 as age], aggr=[]
  RepartitionExec: partitioning=Hash([name@0, age@1], 12), input_partitions=4
    AggregateExec: mode=Partial, gby=[name@0 as name, age@1 as age], aggr=[]
      UnionExec
        DataSourceExec: partitions=2, partition_sizes=[1, 1]
        DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
        r#"===============Stage 0 (partitions=12)===============
AggregateExec: mode=FinalPartitioned, gby=[name@0 as name, age@1 as age], aggr=[]
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

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_nodes_v2(0, &nodes, &distribution, &[12, 0, 0]);
    assert_stage_distributed_into_nodes_v2(1, &nodes, &distribution, &[0, 2, 2]);

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
    assert_stage_distributed_into_nodes_v2(0, &nodes, &distribution, &[1, 0, 0]);
    assert_stage_distributed_into_nodes_v2(1, &nodes, &distribution, &[0, 1, 1]);
    Ok(())
}

#[tokio::test]
async fn window_rank_with_partition_ordering() -> Result<(), Box<dyn std::error::Error>> {
    let stage_plans = assert_planner(
        r#"select * from (select *,rank() over(partition by id order by view_updated desc nulls last) rk from "public"."file_grid_original_44691_20260313152925290"  )t1 where rk = 1;"#,
        r#"ProjectionExec: expr=[id@0 as id, file_name@1 as file_name, view_updated@2 as view_updated, rank() PARTITION BY [public.file_grid_original_44691_20260313152925290.id] ORDER BY [public.file_grid_original_44691_20260313152925290.view_updated DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 as rk]
  FilterExec: rank() PARTITION BY [public.file_grid_original_44691_20260313152925290.id] ORDER BY [public.file_grid_original_44691_20260313152925290.view_updated DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 = 1
    BoundedWindowAggExec: wdw=[rank() PARTITION BY [public.file_grid_original_44691_20260313152925290.id] ORDER BY [public.file_grid_original_44691_20260313152925290.view_updated DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { "rank() PARTITION BY [public.file_grid_original_44691_20260313152925290.id] ORDER BY [public.file_grid_original_44691_20260313152925290.view_updated DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW": UInt64 }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      SortExec: expr=[id@0 ASC NULLS LAST, view_updated@2 DESC NULLS LAST], preserve_partitioning=[true]
        RepartitionExec: partitioning=Hash([id@0], 12), input_partitions=2
          DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
        r#"===============Stage 0 (partitions=12)===============
ProjectionExec: expr=[id@0 as id, file_name@1 as file_name, view_updated@2 as view_updated, rank() PARTITION BY [public.file_grid_original_44691_20260313152925290.id] ORDER BY [public.file_grid_original_44691_20260313152925290.view_updated DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 as rk]
  FilterExec: rank() PARTITION BY [public.file_grid_original_44691_20260313152925290.id] ORDER BY [public.file_grid_original_44691_20260313152925290.view_updated DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW@3 = 1
    BoundedWindowAggExec: wdw=[rank() PARTITION BY [public.file_grid_original_44691_20260313152925290.id] ORDER BY [public.file_grid_original_44691_20260313152925290.view_updated DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW: Field { "rank() PARTITION BY [public.file_grid_original_44691_20260313152925290.id] ORDER BY [public.file_grid_original_44691_20260313152925290.view_updated DESC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW": UInt64 }, frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
      UnresolvedExec: delegated_plan=SortExec, delegated_stage=1
===============Stage 1 (partitions=12)===============
SortExec: expr=[id@0 ASC NULLS LAST, view_updated@2 DESC NULLS LAST], preserve_partitioning=[true]
  RepartitionExec: partitioning=Hash([id@0], 12), input_partitions=2
    DataSourceExec: partitions=2, partition_sizes=[1, 1]
"#,
    )
    .await;

    let (local_node, nodes) = mock_alive_nodes();
    let distribution = schedule_tasks(&local_node, &nodes, &stage_plans).await?;
    assert_stage_distributed_into_nodes_v2(0, &nodes, &distribution, &[4, 4, 4]);
    assert_stage_distributed_into_nodes_v2(1, &nodes, &distribution, &[12, 0, 0]);
    Ok(())
}
