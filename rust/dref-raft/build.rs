// Compile the protobuf definitions for the DRefRaft service and the internal
// Raft transport. We deliberately keep the wire format of `dref.proto` and
// `raft.proto` 100% compatible with the Scala project (same field numbers,
// same service names), so a Rust node and a Scala node could in principle
// talk to each other.
//
// `raft_network.proto` is Rust-only: the Scala impl ships MicroRaft messages
// as serialized Java objects, which doesn't translate to Rust.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Use the bundled protoc so the build doesn't require the user to install
    // it system-wide. tonic-build / prost-build read $PROTOC, so just set
    // that.
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    std::env::set_var("PROTOC", protoc);

    let proto_root = std::path::PathBuf::from("proto");
    let protos = [
        proto_root.join("dref.proto"),
        proto_root.join("raft.proto"),
        proto_root.join("raft_network.proto"),
    ];

    // Rerun if any proto changes.
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
