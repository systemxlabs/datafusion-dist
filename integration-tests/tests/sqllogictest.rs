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

/// Stress test: execute multiple SQL queries concurrently
/// Each query must complete successfully without errors
#[tokio::test]
async fn concurrent_queries_stress_test() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    // Define multiple SQL queries that exercise different execution paths
    let queries = vec![
        // Aggregation queries
        "select id, count(*), sum(view_updated) from \"public\".\"file_grid_original_44691_20260313152925290\" group by id order by id",
        "select count(*) from \"public\".\"file_grid_original_44691_20260313152925290\"",
        "select max(view_updated), min(view_updated) from \"public\".\"file_grid_original_44691_20260313152925290\"",

        // Join queries
        "select t1.id, t2.id from \"public\".\"file_grid_original_44691_20260313152925290\" t1 join \"public\".\"file_grid_original_44691_20260313152925290\" t2 on t1.id = t2.id limit 10",
        "select * from simple as t1 join simple as t2 on t1.age > t2.age",

        // Window function queries
        "select id, rank() over (partition by id order by view_updated desc nulls last) from \"public\".\"file_grid_original_44691_20260313152925290\"",

        // Subquery with aggregation
        "select max(cnt) from (select id, count(*) as cnt from \"public\".\"file_grid_original_44691_20260313152925290\" group by id) sub",

        // UNION ALL with aggregation
        "select id, count(*) from (select id from \"public\".\"file_grid_original_44691_20260313152925290\" union all select id from \"public\".\"file_grid_original_44691_20260313152925290\") t group by id",

        // ORDER BY multi-column
        "select id, file_name, view_updated from \"public\".\"file_grid_original_44691_20260313152925290\" order by id, file_name, view_updated nulls last",

        // DISTINCT query
        "select distinct id, file_name from \"public\".\"file_grid_original_44691_20260313152925290\"",
    ];

    // Execute all queries concurrently
    let futures = queries.iter().map(|sql| {
        async move {
            execute_flightsql_query(sql).await
        }
    });

    let results = futures::future::join_all(futures).await;

    // Verify all queries succeeded
    for (i, result) in results.iter().enumerate() {
        if result.is_err() {
            eprintln!("Query {} failed: {:?}", i, result.as_ref().err());
        }
        assert!(result.is_ok(), "Query {} failed", i);
    }

    // Wait for jobs to clean up
    tokio::time::sleep(Duration::from_secs(5)).await;

    let batches = execute_flightsql_query("select * from running_jobs").await?;
    assert!(batches.is_empty(), "Jobs still running after concurrent queries");

    Ok(())
}
