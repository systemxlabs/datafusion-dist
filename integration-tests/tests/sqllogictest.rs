use std::time::Duration;

use datafusion_dist_integration_tests::{setup_containers, utils::execute_flightsql_query};
use sqllogictest_flightsql::runner::FlightSqlDB;

#[tokio::test]
async fn sqllogictest() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    let mut tester = sqllogictest::Runner::new(|| async {
        FlightSqlDB::new_from_endpoint("dist", "http://localhost:50061", "admin", "admin123").await
    });
    tester.with_column_validator(sqllogictest::strict_column_validator);

    tester.run_file_async("tests/sqllogictest.slt").await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    // all jobs should be cleaned up
    let batches = execute_flightsql_query("select * from running_jobs").await?;
    assert!(batches.is_empty());

    Ok(())
}

/// Stress test: execute 50 SQL queries concurrently within 3 minutes
/// Each query must complete successfully
#[tokio::test]
async fn concurrent_queries_stress_test() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    let sql1 = "select count(*) from \"public\".\"file_grid_original_44691_20260313152925290\"";
    let sql2 = "select * from simple as t1 join simple as t2 on t1.age > t2.age";
    let sql3 = "select id, rank() over (partition by id order by view_updated desc nulls last) from \"public\".\"file_grid_original_44691_20260313152925290\"";

    // 50 concurrent queries: repeat 3 SQL patterns to fill 50
    let queries: Vec<_> = vec![sql1, sql2, sql3]
        .into_iter()
        .cycle()
        .take(50)
        .collect();

    let start = std::time::Instant::now();
    let futures = queries
        .iter()
        .map(|sql| async move { execute_flightsql_query(sql).await });

    let results = futures::future::join_all(futures).await;
    let elapsed = start.elapsed();

    // Verify all queries succeeded
    for (i, result) in results.iter().enumerate() {
        if result.is_err() {
            eprintln!("Query {} failed: {:?}", i, result.as_ref().err());
        }
        assert!(result.is_ok(), "Query {} failed", i);
    }

    // Verify execution time is under 3 minutes
    let max_duration = std::time::Duration::from_secs(180);
    assert!(
        elapsed < max_duration,
        "50 queries took {} seconds, expected < 180",
        elapsed.as_secs()
    );

    // Wait for jobs to clean up
    tokio::time::sleep(Duration::from_secs(5)).await;

    let batches = execute_flightsql_query("select * from running_jobs").await?;
    assert!(
        batches.is_empty(),
        "Jobs still running after concurrent queries"
    );

    Ok(())
}
