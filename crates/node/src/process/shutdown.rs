//! Bounded shutdown of `serve`: the shutdown signals, the background tasks,
//! and the deadline for them and open connections.

use std::time::Duration;

use anyhow::Result;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// How long `serve` waits after the shutdown began for open connections and
/// background tasks to finish, before it exits anyway.
pub(super) const SHUTDOWN_DEADLINE: Duration = Duration::from_secs(10);

/// The exit code after a second shutdown signal cut the graceful shutdown
/// short.
const FORCED_SHUTDOWN_EXIT_CODE: i32 = 130;

/// The background tasks of `serve`, named for the log.
#[derive(Default)]
pub(super) struct BackgroundTasks {
    tasks: tokio::task::JoinSet<Result<()>>,
    names: std::collections::HashMap<tokio::task::Id, String>,
}

impl BackgroundTasks {
    pub(super) fn spawn(
        &mut self,
        name: impl Into<String>,
        task: impl Future<Output = Result<()>> + Send + 'static,
    ) {
        let id = self.tasks.spawn(task).id();
        self.names.insert(id, name.into());
    }

    /// Wait for the next task to end, and return its name and outcome: its
    /// error or panic, if any.
    async fn join_next(&mut self) -> Option<(String, Result<()>)> {
        let (id, outcome) = match self.tasks.join_next_with_id().await? {
            Ok((id, outcome)) => (id, outcome),
            Err(error) => (error.id(), Err(anyhow::Error::new(error))),
        };
        Some((self.names.remove(&id).unwrap_or_default(), outcome))
    }
}

/// Run `servers` until the node shuts down, then give open connections and
/// the background tasks [`SHUTDOWN_DEADLINE`] to finish; whatever is still
/// open after it is abandoned.
///
/// Each background task runs until the shutdown. One that ends before it,
/// by returning, failing, or panicking, is fatal: the node shuts down, and
/// this returns that task's error. The network lane supervisor restarts
/// failed lanes itself, and stays until the shutdown when they halt.
pub(super) async fn serve_until_shutdown(
    servers: impl Future<Output = Result<()>>,
    background: &mut BackgroundTasks,
    cancellation: &CancellationToken,
) -> Result<()> {
    tokio::pin!(servers);
    let mut served = None;
    let mut failure = None;
    tokio::select! {
        // First: once the shutdown began, tasks end because of it.
        biased;
        () = cancellation.cancelled() => {}
        result = &mut servers => served = Some(result),
        Some((task, outcome)) = background.join_next() => {
            let error = outcome
                .err()
                .unwrap_or_else(|| anyhow::anyhow!("it stopped before the node shut down"));
            tracing::error!(
                task,
                error = format!("{error:#}"),
                "a background task ended; shutting the node down"
            );
            failure = Some(error.context(format!("background task `{task}` ended")));
        }
    }
    cancellation.cancel();
    let deadline = tokio::time::Instant::now() + SHUTDOWN_DEADLINE;
    let served = match served {
        Some(result) => result,
        None => tokio::time::timeout_at(deadline, &mut servers)
            .await
            .unwrap_or_else(|_| {
                warn!(
                    deadline = ?SHUTDOWN_DEADLINE,
                    "open connections did not close by the shutdown deadline; abandoning them"
                );
                Ok(())
            }),
    };
    loop {
        match tokio::time::timeout_at(deadline, background.join_next()).await {
            Ok(Some((task, Err(error)))) => warn!(
                task,
                error = format!("{error:#}"),
                "a background task failed while the node shut down"
            ),
            Ok(Some((_, Ok(())))) => {}
            Ok(None) => break,
            Err(_) => {
                warn!(
                    tasks = ?background.names.values().collect::<Vec<_>>(),
                    deadline = ?SHUTDOWN_DEADLINE,
                    "background tasks did not stop by the shutdown deadline; abandoning them"
                );
                break;
            }
        }
    }
    failure.map_or(served, Err)
}

/// Cancel `cancellation` at the first shutdown signal. A second signal calls
/// `exit` with [`FORCED_SHUTDOWN_EXIT_CODE`] at once, instead of waiting for
/// the graceful shutdown.
pub(super) async fn forward_shutdown_signals(
    signals: impl futures::Stream<Item = ()>,
    cancellation: CancellationToken,
    exit: impl FnOnce(i32),
) {
    use futures::StreamExt as _;

    let mut signals = std::pin::pin!(signals);
    if signals.next().await.is_none() {
        warn!("the shutdown signal handler stopped");
        return;
    }
    info!(
        deadline = ?SHUTDOWN_DEADLINE,
        "shutdown signal received; a second one exits at once"
    );
    cancellation.cancel();
    if signals.next().await.is_some() {
        warn!("second shutdown signal received; exiting before the shutdown finished");
        exit(FORCED_SHUTDOWN_EXIT_CODE);
    }
}

/// Ctrl-C, and SIGTERM on Unix. Each [`Self::recv`] waits for the next one,
/// so a second signal is not missed while the first is handled.
pub(super) struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    #[cfg(unix)]
    pub(super) fn new() -> std::io::Result<Self> {
        use tokio::signal::unix::{SignalKind, signal};

        Ok(Self {
            interrupt: signal(SignalKind::interrupt())?,
            terminate: signal(SignalKind::terminate())?,
        })
    }

    #[cfg(not(unix))]
    #[allow(clippy::unnecessary_wraps)]
    pub(super) fn new() -> std::io::Result<Self> {
        Ok(Self {})
    }

    #[cfg(unix)]
    pub(super) async fn recv(&mut self) -> std::io::Result<()> {
        tokio::select! {
            received = self.interrupt.recv() => received,
            received = self.terminate.recv() => received,
        }
        .ok_or_else(|| std::io::Error::other("the signal driver stopped"))
    }

    #[cfg(not(unix))]
    pub(super) async fn recv(&mut self) -> std::io::Result<()> {
        tokio::signal::ctrl_c().await
    }

    /// The signals as a stream, which ends if the signal handler fails.
    pub(super) fn into_stream(self) -> impl futures::Stream<Item = ()> {
        futures::stream::unfold(self, |mut signals| async move {
            signals.recv().await.ok().map(|()| ((), signals))
        })
    }
}
