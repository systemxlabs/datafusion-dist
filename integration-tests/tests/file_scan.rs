//! End to end test of a multi partition file scan on a multi node cluster.

use std::{collections::HashSet, error::Error, time::Duration};

use arrow_flight::{Ticket, sql::client::FlightSqlServiceClient};
use datafusion::{
    arrow::{error::ArrowError, record_batch::RecordBatch},
    scalar::ScalarValue,
};
use datafusion_dist::cluster::NodeId;
use datafusion_dist_integration_tests::{data::csv_dir_rows, setup_containers, ticket::NodeTask};
use futures::TryStreamExt;
use tonic::transport::{Channel, Endpoint};

/// Runs the scan until its partitions are spread over more than one node.
///
/// Nodes register in the cluster with a heartbeat, so the first queries of a
/// freshly started cluster can still be scheduled on a subset of the nodes.
/// Every row is checked once the scan is really distributed, which is the
/// situation that used to return each row once per node.
async fn scan_tickets(
    client: &mut FlightSqlServiceClient<Channel>,
) -> Result<Vec<Ticket>, Box<dyn Error>> {
    let mut attempt = 0;
    loop {
        let flight_info = client
            .execute("select * from csv_dir".to_string(), None)
            .await?;
        let tickets = flight_info
            .endpoint
            .iter()
            .map(|endpoint| endpoint.ticket.clone().expect("ticket is required"))
            .collect::<Vec<_>>();
        let nodes = distinct_nodes(&tickets)?;

        attempt += 1;
        if nodes.len() > 1 || attempt > 30 {
            println!(
                "csv_dir scan: {} endpoint(s) on {} node(s) after {attempt} attempt(s)",
                tickets.len(),
                nodes.len()
            );
            return Ok(tickets);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

fn distinct_nodes(tickets: &[Ticket]) -> Result<HashSet<NodeId>, ArrowError> {
    tickets
        .iter()
        .map(NodeTask::from_ticket)
        .map(|node_task| node_task.map(|node_task| node_task.node_id()))
        .collect()
}

#[tokio::test]
async fn multi_node_csv_scan_returns_exact_rows() -> Result<(), Box<dyn Error>> {
    setup_containers().await;

    let channel = Endpoint::from_static("http://localhost:50061")
        .connect()
        .await?;
    let mut client = FlightSqlServiceClient::new(channel);
    client.handshake("admin", "admin123").await?;

    // `csv_dir` is a directory of files, so the scan has one partition per
    // file, and the ticket of every endpoint tells which node serves it.
    let tickets = scan_tickets(&mut client).await?;
    assert!(
        tickets.len() > 1,
        "expected one endpoint per partition of csv_dir, got {}",
        tickets.len()
    );
    let nodes = distinct_nodes(&tickets)?;
    assert!(
        nodes.len() > 1,
        "expected the scan to be distributed across nodes, got {nodes:?}"
    );

    // Reading every endpoint must return every row exactly once. A partition
    // that also reads the files of its siblings returns duplicated rows here.
    let mut rows = Vec::new();
    for ticket in tickets {
        let stream = client.do_get(ticket).await?;
        let batches = stream.try_collect::<Vec<RecordBatch>>().await?;
        for batch in &batches {
            for row in 0..batch.num_rows() {
                rows.push((
                    ScalarValue::try_from_array(batch.column(0), row)?.to_string(),
                    ScalarValue::try_from_array(batch.column(1), row)?.to_string(),
                ));
            }
        }
    }

    let mut expected = csv_dir_rows()
        .into_iter()
        .map(|(id, name)| (id.to_string(), name))
        .collect::<Vec<_>>();
    rows.sort();
    expected.sort();
    assert_eq!(rows, expected);

    Ok(())
}
