use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use arrow::array::RecordBatch;
use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion_dist_integration_tests::{
    setup_containers,
    utils::{execute_flightsql_query, healthy_check_all_nodes},
};
use futures::TryStreamExt;
use tonic::transport::Endpoint;

#[tokio::test]
async fn panic_task() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    // Use panic UDF to trigger a panic during execution
    let err = execute_flightsql_query("SELECT panic()").await.unwrap_err();
    let err_msg = err.to_string();
    println!("err msg: {err_msg}");
    assert!(err_msg.contains("udf panicked"));

    // Verify the service is still working after panic by querying simple table
    let batches = execute_flightsql_query("SELECT * FROM simple").await?;
    assert!(!batches.is_empty());

    Ok(())
}

#[tokio::test]
async fn client_not_poll() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    let endpoint = Endpoint::from_static("http://localhost:50061");
    let channel = endpoint.connect().await?;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);

    // not poll data
    let _flight_info = flight_sql_client
        .execute("select * from simple".to_string(), None)
        .await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    let batches = execute_flightsql_query("SELECT * FROM running_jobs").await?;
    assert!(!batches.is_empty());

    tokio::time::sleep(Duration::from_secs(30)).await;

    // job should be cleaned up
    let batches = execute_flightsql_query("SELECT * FROM running_jobs").await?;
    assert!(batches.is_empty());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cpu_intensive_task() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    let cpu_intensive_task_finished = Arc::new(AtomicBool::new(false));
    let cpu_intensive_task_finished_clone = cpu_intensive_task_finished.clone();

    let cpu_thread_join_handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let now = std::time::Instant::now();

                println!("cpu intensive task starting");
                let batches = execute_flightsql_query("SELECT cpu_intensive(2000000000)")
                    .await
                    .map_err(|e| e.to_string())?;
                assert!(!batches.is_empty());

                let duration_secs = now.elapsed().as_secs();
                println!("cpu intensive task cost {duration_secs}s");
                assert!(duration_secs > 5);

                cpu_intensive_task_finished_clone.store(true, std::sync::atomic::Ordering::SeqCst);

                Ok::<_, String>(())
            })
    });

    let health_thread_join_handle = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let mut checked = false;
                while !cpu_intensive_task_finished.load(std::sync::atomic::Ordering::SeqCst) {
                    // Verify the service is still working
                    let now = std::time::Instant::now();

                    println!("health task starting");
                    healthy_check_all_nodes().await.map_err(|e| e.to_string())?;

                    let duration_secs = now.elapsed().as_secs();
                    println!("heath check task cost {duration_secs}s");
                    assert!(duration_secs < 1);

                    checked = true;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
                assert!(checked);

                Ok::<_, String>(())
            })
    });

    health_thread_join_handle.join().unwrap().unwrap();
    cpu_thread_join_handle.join().unwrap().unwrap();

    Ok(())
}

/// Test that fetching multiple endpoints sequentially from the same FlightInfo
/// does not panic when the plan contains RepartitionExec with fewer input
/// partitions than output partitions.
///
/// Regression test for: RepartitionExec "partition not used yet" panic
/// when `reset_state()` does not recursively reset children's state.
#[tokio::test]
async fn multi_endpoint_repartition_reset_state() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    let endpoint = Endpoint::from_static("http://localhost:50061");
    let channel = endpoint.connect().await?;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);
    flight_sql_client.handshake("admin", "admin123").await?;

    // Force partitioned hash join to create RepartitionExec with
    // input_partitions=1 (single_partition table) and output_partitions=12
    let sql =
        "SELECT * FROM single_partition AS t1 JOIN single_partition AS t2 ON t1.name = t2.name";
    let flight_info = flight_sql_client.execute(sql.to_string(), None).await?;

    assert!(
        flight_info.endpoint.len() > 1,
        "Expected multiple endpoints, got {}",
        flight_info.endpoint.len()
    );
    println!("FlightInfo has {} endpoints", flight_info.endpoint.len());

    // Fetch endpoints sequentially (not concurrently) to verify that
    // each endpoint can be consumed independently without state reuse panic.
    let mut total_rows = 0;
    for (i, ep) in flight_info.endpoint.iter().enumerate() {
        let ticket = ep.ticket.as_ref().expect("ticket is required").clone();
        let stream = flight_sql_client.do_get(ticket).await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        println!("Endpoint {}: {} rows", i, rows);
        total_rows += rows;
    }

    println!("Total rows across all endpoints: {}", total_rows);
    Ok(())
}
