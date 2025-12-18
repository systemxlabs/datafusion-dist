use std::path::Path;

fn main() -> Result<(), String> {
    let proto_path = Path::new("network_tonic.proto");
    let out_dir = Path::new(env!("OUT_DIR"));

    tonic_build::configure()
        .out_dir(out_dir)
        .protoc_arg("--experimental_allow_proto3_optional")
        .compile_well_known_types(true)
        .compile_protos(&[proto_path], &["proto"])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    let prost = out_dir.join("network_tonic.rs");
    let target = Path::new("../src/protobuf.rs");
    println!("Copying {} to {}", prost.display(), target.display(),);
    std::fs::copy(prost, target).unwrap();

    Ok(())
}
