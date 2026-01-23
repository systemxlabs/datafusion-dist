use std::path::Path;

fn main() -> Result<(), String> {
    let proto_path = Path::new("proto/network_tonic.proto");
    let out_dir = Path::new(env!("OUT_DIR"));

    tonic_prost_build::configure()
        .out_dir(out_dir)
        .protoc_arg("--experimental_allow_proto3_optional")
        .extern_path(".datafusion_common", "::datafusion_proto::protobuf")
        .extern_path(".datafusion", "::datafusion_proto::protobuf")
        .extern_path(".arrow_flight", "::arrow_flight")
        .compile_well_known_types(true)
        .compile_protos(&[proto_path], &[Path::new("proto")])
        .map_err(|e| format!("protobuf compilation failed: {e}"))?;

    let prost = out_dir.join("network_tonic.rs");
    let target = Path::new("../src/protobuf.rs");
    println!("Copying {} to {}", prost.display(), target.display(),);
    std::fs::copy(prost, target).unwrap();

    Ok(())
}
