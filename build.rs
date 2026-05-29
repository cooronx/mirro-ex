fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    // Use a vendored protoc so local builds do not depend on system packages.
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    prost_build::Config::new().compile_protos(&["proto/marketdata.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/marketdata.proto");

    Ok(())
}
