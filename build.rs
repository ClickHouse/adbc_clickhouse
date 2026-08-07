//! Emit the resolved `arrow-array` version via `cargo metadata` so
//! `DriverArrowVersion` stays in sync when Arrow crates are bumped.

use cargo_metadata::MetadataCommand;

fn main() {
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=Cargo.lock");

    let metadata = MetadataCommand::new()
        .manifest_path("Cargo.toml")
        .exec()
        .expect("failed to run cargo metadata");

    let version = metadata
        .packages
        .iter()
        .find(|p| p.name.as_str() == "arrow-array")
        .map(|p| p.version.to_string())
        .expect("package `arrow-array` not found in cargo metadata");

    println!("cargo:rustc-env=ADBC_DRIVER_ARROW_VERSION={version}");
}
