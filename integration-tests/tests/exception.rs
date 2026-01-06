use std::{
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion_dist_integration_tests::{
    setup_containers,
    utils::{execute_flightsql_query, healthy_check_all_nodes},
};
use tonic::transport::Endpoint;

#[tokio::test]
async fn panic_task() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    // Use panic UDF to trigger a panic during execution
    let batches = execute_flightsql_query("SELECT panic()").await?;
    assert!(batches.is_empty());

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
