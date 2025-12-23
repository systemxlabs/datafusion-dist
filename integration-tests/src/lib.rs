pub mod data;
pub mod docker;
pub mod utils;

use std::sync::OnceLock;

use crate::{docker::DockerCompose, utils::execute_flightsql_query};

static CONTAINERS: OnceLock<DockerCompose> = OnceLock::new();

pub async fn setup_containers() {
    let _ = CONTAINERS.get_or_init(|| {
        let docker_compose =
            DockerCompose::new("integration-tests-containers", env!("CARGO_MANIFEST_DIR"));
        docker_compose.up();
        docker_compose
    });

    let mut retry = 0;
    loop {
        match execute_flightsql_query("select 1").await {
            Ok(_) => break,
            Err(err) => {
                eprintln!("flightsql healthy check (select 1) failed: {err:?}");
            }
        }
        retry += 1;
        if retry > 20 {
            panic!("containers still not ready after 200 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}
