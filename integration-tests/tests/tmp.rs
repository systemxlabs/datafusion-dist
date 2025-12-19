use std::error::Error;

use arrow_flight::sql::client::FlightSqlServiceClient;
use datafusion::arrow::{array::RecordBatch, util::pretty::pretty_format_batches};
use datafusion_dist_integration_tests::setup_containers;
use futures::TryStreamExt;
use tonic::transport::Endpoint;

#[tokio::test(flavor = "multi_thread")]
async fn test_tmp() -> Result<(), Box<dyn Error>> {
    setup_containers();

    let endpoint = Endpoint::from_static("http://localhost:50061");
    let channel = endpoint.connect().await?;
    let mut flight_sql_client = FlightSqlServiceClient::new(channel);

    let flight_info = flight_sql_client
        .execute("select 1".to_string(), None)
        .await?;

    let mut batches = Vec::new();
    for endpoint in flight_info.endpoint {
        let ticket = endpoint
            .ticket
            .as_ref()
            .expect("ticket is required")
            .clone();
        let stream = flight_sql_client.do_get(ticket).await?;
        let result: Vec<RecordBatch> = stream.try_collect().await?;
        batches.extend(result);
    }

    let batches_str = pretty_format_batches(&batches)?.to_string();
    println!("{batches_str}");

    assert_eq!(
        batches_str,
        r#"+----------+
| Int64(1) |
+----------+
| 1        |
+----------+"#
    );

    Ok(())
}
