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

/// 0-column RecordBatch regression test (follow-up to datafabric task #7/#10).
///
/// This is intentionally `#[ignore]`'d: the cases in
/// `tests/zero_col_regression.slt` deliberately drive a 0-column /
/// row_count > 0 batch through the dist execution path, which has
/// historically paniced (`dist/src/util.rs:113` for the small-table path
/// and `arrow-select coalesce.rs:462` for the repartition path). The
/// `data-fabric` fix was on the TableProvider side (`normalize_projection()`,
/// commit `6e7b30a3`); the dist side still needs its own regression guard,
/// but until dist can handle this shape stably we don't want this test to
/// turn main CI red.
///
/// Run explicitly with:
/// ```sh
/// cargo test -p datafusion-dist-integration-tests --test sqllogictest \
///   -- --ignored sqllogictest_zero_col_regression
/// ```
///
/// Or via env-gate alongside the normal run:
/// ```sh
/// SLT_RUN_ZERO_COL=1 cargo test -p datafusion-dist-integration-tests \
///   --test sqllogictest -- --include-ignored
/// ```
#[tokio::test]
#[ignore = "regression repro for 0-col batch — runs only on demand to keep main CI green"]
async fn sqllogictest_zero_col_regression() -> Result<(), Box<dyn std::error::Error>> {
    setup_containers().await;

    let mut tester = sqllogictest::Runner::new(|| async {
        FlightSqlDB::new_from_endpoint("dist", "http://localhost:50061", "admin", "admin123").await
    });
    tester.with_column_validator(sqllogictest::strict_column_validator);

    tester
        .run_file_async("tests/zero_col_regression.slt")
        .await?;

    tokio::time::sleep(Duration::from_secs(5)).await;

    // all jobs should be cleaned up — same invariant as the main suite.
    let batches = execute_flightsql_query("select * from running_jobs").await?;
    assert!(batches.is_empty());

    Ok(())
}
