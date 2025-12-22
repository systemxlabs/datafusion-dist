pub mod data;
pub mod docker;
pub mod utils;

use std::sync::OnceLock;

use crate::docker::DockerCompose;

static CONTAINERS: OnceLock<DockerCompose> = OnceLock::new();

pub fn setup_containers() {
    let _ = CONTAINERS.get_or_init(|| {
        let docker_compose =
            DockerCompose::new("integration-tests-containers", env!("CARGO_MANIFEST_DIR"));
        docker_compose.up();
        docker_compose
    });
}
