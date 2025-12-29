use std::{
    sync::Arc,
    time::Duration,
};

use datafusion_dist::{
    cluster::{NodeId, NodeState, NodeStatus},
    config::DistConfig,
    planner::TaskId,
    runtime::DistRuntime,
};
use datafusion_dist_cluster_postgres::PostgresClusterBuilder;
use datafusion_dist_integration_tests::{
    data::build_session_context,
    utils::{execute_flightsql_query, mock_alive_nodes},
};
use datafusion_dist_network_tonic::{
    network::DistTonicNetwork,
    protobuf::dist_tonic_service_server::DistTonicServiceServer,
    server::DistTonicServer,
};
use datafusion_proto::physical_plan::DefaultPhysicalExtensionCodec;
use tonic::transport::Server;
use uuid::Uuid;

#[tokio::test]
async fn test_draining_rejects_new_tasks() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = build_session_context();
    let cluster = PostgresClusterBuilder::new("postgres", 5432, "postgres", "password")
        .build()
        .await?;

    let app_extension_codec = Arc::new(DefaultPhysicalExtensionCodec {});
    let network = DistTonicNetwork::new(50060, app_extension_codec.clone());

    let config = DistConfig::default();

    let runtime = DistRuntime::new(
        ctx.task_ctx(),
        Arc::new(config),
        Arc::new(cluster),
        Arc::new(network),
    );
    runtime.start().await;

    // Submit a job to ensure runtime is working
    let df = ctx.sql("SELECT * FROM simple").await?;
    let plan = df.create_physical_plan().await?;
    let (_job_id, _distribution) = runtime.submit(plan).await?;

    // Trigger draining with timeout
    runtime.draining(Some(Duration::from_secs(5))).await;

    // Try to submit another job - should fail
    let df2 = ctx.sql("SELECT * FROM simple").await?;
    let plan2 = df2.create_physical_plan().await?;
    let result = runtime.submit(plan2).await;

    assert!(result.is_err(), "Should reject new tasks after draining");
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("rejecting new tasks"));

    Ok(())
}

#[tokio::test]
async fn test_draining_updates_node_status() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = build_session_context();
    let cluster = PostgresClusterBuilder::new("postgres", 5432, "postgres", "password")
        .build()
        .await?;

    let app_extension_codec = Arc::new(DefaultPhysicalExtensionCodec {});
    let network = DistTonicNetwork::new(50061, app_extension_codec.clone());

    let config = DistConfig::default();

    let runtime = DistRuntime::new(
        ctx.task_ctx(),
        Arc::new(config),
        Arc::new(cluster),
        Arc::new(network),
    );
    runtime.start().await;

    let node_id = runtime.node_id.clone();

    // Wait for initial heartbeat
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check initial status is Available
    let alive_nodes = cluster.alive_nodes().await?;
    let node_state = alive_nodes.get(&node_id).expect("Node should be alive");
    assert_eq!(
        node_state.status,
        NodeStatus::Available,
        "Initial status should be Available"
    );

    // Trigger draining with timeout
    runtime.draining(Some(Duration::from_secs(5))).await;

    // Wait for heartbeat to update
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check status is now Draining
    let alive_nodes = cluster.alive_nodes().await?;
    let node_state = alive_nodes.get(&node_id).expect("Node should still be alive");
    assert_eq!(
        node_state.status,
        NodeStatus::Draining,
        "Status should be Draining after draining"
    );

    Ok(())
}

#[tokio::test]
async fn test_exit_updates_node_status() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = build_session_context();
    let cluster = PostgresClusterBuilder::new("postgres", 5432, "postgres", "password")
        .build()
        .await?;

    let app_extension_codec = Arc::new(DefaultPhysicalExtensionCodec {});
    let network = DistTonicNetwork::new(50062, app_extension_codec.clone());

    let config = DistConfig::default();

    let runtime = DistRuntime::new(
        ctx.task_ctx(),
        Arc::new(config),
        Arc::new(cluster),
        Arc::new(network),
    );
    runtime.start().await;

    let node_id = runtime.node_id.clone();

    // Wait for initial heartbeat
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Trigger draining with timeout
    runtime.draining(Some(Duration::from_secs(5))).await;

    // Wait for heartbeat to update
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check status is Draining
    let alive_nodes = cluster.alive_nodes().await?;
    let node_state = alive_nodes.get(&node_id).expect("Node should still be alive");
    assert_eq!(
        node_state.status,
        NodeStatus::Draining,
        "Status should be Draining"
    );

    // Trigger exit
    runtime.exit().await;

    // Wait for heartbeat to update
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Check status is now Exited
    let alive_nodes = cluster.alive_nodes().await?;
    let node_state = alive_nodes.get(&node_id).expect("Node should still be alive");
    assert_eq!(
        node_state.status,
        NodeStatus::Exited,
        "Status should be Exited after exit"
    );

    Ok(())
}

#[tokio::test]
async fn test_scheduler_filters_draining_nodes() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = build_session_context();
    let cluster = PostgresClusterBuilder::new("postgres", 5432, "postgres", "password")
        .build()
        .await?;

    let app_extension_codec = Arc::new(DefaultPhysicalExtensionCodec {});
    let network = DistTonicNetwork::new(50063, app_extension_codec.clone());

    let config = DistConfig::default();

    let runtime = DistRuntime::new(
        ctx.task_ctx(),
        Arc::new(config),
        Arc::new(cluster.clone()),
        Arc::new(network),
    );
    runtime.start().await;

    let node_id = runtime.node_id.clone();

    // Wait for initial heartbeat
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Submit a job
    let df = ctx.sql("SELECT * FROM simple").await?;
    let plan = df.create_physical_plan().await?;
    let (job_id, distribution) = runtime.submit(plan).await?;

    // Verify job was scheduled to this node
    assert!(
        distribution.values().any(|id| id == &node_id),
        "Job should be scheduled to this node"
    );

    // Trigger draining with timeout
    runtime.draining(Some(Duration::from_secs(5))).await;

    // Wait for heartbeat to update
    tokio::time::sleep(Duration::from_secs(1)).await;

    // Submit another job - should not be scheduled to this node
    let df2 = ctx.sql("SELECT * FROM simple").await?;
    let plan2 = df2.create_physical_plan().await?;
    let (_job_id2, distribution2) = runtime.submit(plan2).await?;

    assert!(
        !distribution2.values().any(|id| id == &node_id),
        "Job should not be scheduled to draining node"
    );

    // Clean up first job
    let _ = runtime.cancel_job(job_id).await;

    Ok(())
}

#[tokio::test]
async fn test_graceful_exit_timeout() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = build_session_context();
    let cluster = PostgresClusterBuilder::new("postgres", 5432, "postgres", "password")
        .build()
        .await?;

    let app_extension_codec = Arc::new(DefaultPhysicalExtensionCodec {});
    let network = DistTonicNetwork::new(50064, app_extension_codec.clone());

    let config = DistConfig::default();

    let runtime = DistRuntime::new(
        ctx.task_ctx(),
        Arc::new(config),
        Arc::new(cluster),
        Arc::new(network),
    );
    runtime.start().await;

    // Submit a long-running job
    let df = ctx.sql("SELECT * FROM simple").await?;
    let plan = df.create_physical_plan().await?;
    let (_job_id, _distribution) = runtime.submit(plan).await?;

    // Trigger graceful exit with very short timeout - should timeout quickly
    let start = std::time::Instant::now();
    runtime.graceful_exit(Some(Duration::from_millis(100))).await;
    let elapsed = start.elapsed();

    // Should timeout within 100ms + some margin
    assert!(
        elapsed < Duration::from_millis(500),
        "Graceful exit should timeout quickly, but took {:?}",
        elapsed
    );

    // Verify status is Exited
    let alive_nodes = cluster.alive_nodes().await?;
    let node_state = alive_nodes.get(&runtime.node_id).expect("Node should be alive");
    assert_eq!(
        node_state.status,
        NodeStatus::Exited,
        "Status should be Exited after graceful exit timeout"
    );

    Ok(())
}

#[tokio::test]
async fn test_graceful_exit_waits_for_task_completion() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = build_session_context();
    let cluster = PostgresClusterBuilder::new("postgres", 5432, "postgres", "password")
        .build()
        .await?;

    let app_extension_codec = Arc::new(DefaultPhysicalExtensionCodec {});
    let network = DistTonicNetwork::new(50065, app_extension_codec.clone());

    let config = DistConfig::default();

    let runtime = DistRuntime::new(
        ctx.task_ctx(),
        Arc::new(config),
        Arc::new(cluster),
        Arc::new(network),
    );
    runtime.start().await;

    // Submit a quick job
    let df = ctx.sql("SELECT * FROM simple LIMIT 1").await?;
    let plan = df.create_physical_plan().await?;
    let (job_id, _distribution) = runtime.submit(plan).await?;

    // Give the job time to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Trigger graceful exit with timeout - should complete quickly since tasks are done
    let start = std::time::Instant::now();
    runtime.graceful_exit(Some(Duration::from_secs(10))).await;
    let elapsed = start.elapsed();

    // Should complete within 2 seconds (one check interval)
    assert!(
        elapsed < Duration::from_secs(3),
        "Graceful exit should complete quickly when tasks are done, but took {:?}",
        elapsed
    );

    // Verify status is Exited
    let alive_nodes = cluster.alive_nodes().await?;
    let node_state = alive_nodes.get(&runtime.node_id).expect("Node should be alive");
    assert_eq!(
        node_state.status,
        NodeStatus::Exited,
        "Status should be Exited after graceful exit"
    );

    // Clean up
    let _ = runtime.cancel_job(job_id).await;

    Ok(())
}

#[tokio::test]
async fn test_draining_without_timeout_waits_indefinitely() -> Result<(), Box<dyn std::error::Error>> {
    let ctx = build_session_context();
    let cluster = PostgresClusterBuilder::new("postgres", 5432, "postgres", "password")
        .build()
        .await?;

    let app_extension_codec = Arc::new(DefaultPhysicalExtensionCodec {});
    let network = DistTonicNetwork::new(50066, app_extension_codec.clone());

    let config = DistConfig::default();

    let runtime = DistRuntime::new(
        ctx.task_ctx(),
        Arc::new(config),
        Arc::new(cluster),
        Arc::new(network),
    );
    runtime.start().await;

    // Submit a quick job
    let df = ctx.sql("SELECT * FROM simple LIMIT 1").await?;
    let plan = df.create_physical_plan().await?;
    let (job_id, _distribution) = runtime.submit(plan).await?;

    // Give the job time to complete
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Trigger draining without timeout - should wait indefinitely for tasks to complete
    let start = std::time::Instant::now();
    runtime.draining(None).await;
    let elapsed = start.elapsed();

    // Should complete within 2 seconds (one check interval) since tasks are done
    assert!(
        elapsed < Duration::from_secs(3),
        "Draining should complete quickly when tasks are done, but took {:?}",
        elapsed
    );

    // Verify status is Draining
    let alive_nodes = cluster.alive_nodes().await?;
    let node_state = alive_nodes.get(&runtime.node_id).expect("Node should be alive");
    assert_eq!(
        node_state.status,
        NodeStatus::Draining,
        "Status should be Draining after draining"
    );

    // Clean up
    let _ = runtime.cancel_job(job_id).await;

    Ok(())
}
