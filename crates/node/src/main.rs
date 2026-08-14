use std::process::ExitCode;

use leani::{Exit, run};

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(exit) => exit.into(),
        Err(error) => {
            eprintln!("error: {error:#}");
            Exit::Failure.into()
        }
    }
}
