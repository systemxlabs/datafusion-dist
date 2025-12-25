use std::time::Duration;

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion_dist_integration_tests::{setup_containers, utils::execute_flightsql_query};
use tonic::transport::Endpoint;

#[tokio::test]
async fn panic_task() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    // Use panic UDF to trigger a panic during execution
    let batches = execute_flightsql_query("SELECT panic('intentional panic for testing')").await?;
    assert!(batches.is_empty());

    // Verify the service is still working after panic by querying simple table
    let batches = execute_flightsql_query("SELECT * FROM simple").await?;
    assert!(
        !batches.is_empty(),
        "Expected non-empty result from simple table"
    );

    Ok(())
}

#[tokio::test]
async fn client_not_poll() -> Result<(), Box<dyn std::error::Error>> {
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
