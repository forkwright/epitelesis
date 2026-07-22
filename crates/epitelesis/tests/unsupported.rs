//! Typed witness for backends that cannot provide rustix waitid semantics.

#![cfg(not(all(
    unix,
    not(any(
        target_os = "cygwin",
        target_os = "horizon",
        target_os = "openbsd",
        target_os = "redox",
        target_os = "wasi"
    ))
)))]

use std::time::Duration;

use epitelesis::{Capability, Command, Error, run};

#[test]
fn unsupported_backend_is_rejected_before_spawn() {
    let command = match Command::new("definitely-not-created").deadline(Duration::from_secs(1)) {
        Ok(command) => command,
        Err(error) => panic!("fixture deadline failed: {error:?}"),
    };
    match run(command) {
        Err(Error::UnsupportedCapability {
            capability: Capability::OwnedProcessGroup,
            ..
        }) => {}
        other => panic!("backend did not return typed unsupported capability: {other:?}"),
    }
}
