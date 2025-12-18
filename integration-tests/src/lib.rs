pub mod docker;

use std::sync::OnceLock;

use crate::docker::DockerCompose;

static POSTGRES_DB: OnceLock<DockerCompose> = OnceLock::new();

pub fn setup_postgres_db() {
    let _ = POSTGRES_DB.get_or_init(|| {
        let docker_compose = DockerCompose::new(
            "postgres",
            format!("{}/testdata/postgres", env!("CARGO_MANIFEST_DIR")),
        );
        docker_compose.up();
        docker_compose
    });
}
