pub mod data;
pub mod docker;
pub mod ticket;
pub mod utils;

use std::sync::OnceLock;

use crate::{docker::DockerCompose, utils::healthy_check_all_nodes};

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
        // Every node must be up before tests run queries: nodes register
        // themselves in the cluster with a heartbeat, and a query submitted
        // while some of them are still missing is only scheduled on the nodes
        // that are alive at that moment.
        match healthy_check_all_nodes().await {
            Ok(()) => break,
            Err(err) => {
                eprintln!("cluster healthy check failed: {err:?}");
            }
        }
        retry += 1;
        if retry > 20 {
            panic!("containers still not ready after 200 seconds");
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}
