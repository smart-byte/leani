use std::{process::ExitCode, time::Duration};

use leani::{Exit, run};

/// How long the process waits, once the command returned, for blocking work
/// still in flight before it exits anyway.
const RUNTIME_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Runtime::new() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("error: start the async runtime: {error}");
            return Exit::Failure.into();
        }
    };
    let exit = match runtime.block_on(run()) {
        Ok(exit) => exit.into(),
        Err(error) => {
            eprintln!("error: {error:#}");
            Exit::Failure.into()
        }
    };
    runtime.shutdown_timeout(RUNTIME_SHUTDOWN_TIMEOUT);
    exit
}
