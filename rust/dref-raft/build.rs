// Compile shared protobuf definitions from the repo-root `proto/` directory.
// Both the Scala and Rust implementations must use these files as the single
// source of truth for cross-language consensus compatibility.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto_root = std::path::PathBuf::from("../../proto");
    let protos = [
        proto_root.join("dref.proto"),
        proto_root.join("raft.proto"),
        proto_root.join("state_command.proto"),
        proto_root.join("dref_consensus.proto"),
    ];

    for p in &protos {
        println!("cargo:rerun-if-changed={}", p.display());
    }
    println!("cargo:rerun-if-changed=build.rs");

    tonic_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &protos
                .iter()
                .map(|p| p.to_str().unwrap())
                .collect::<Vec<_>>(),
            &[proto_root.to_str().unwrap()],
        )?;

    Ok(())
}
