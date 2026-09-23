use crate::{db::Db, workflow::Task};
use anyhow::{bail, Result};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, BufReader},
    process::Command,
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

enum Outcome {
    Success,
    Failed(String),
    Interrupted,
}
enum AttemptResult {
    Success,
    Failure(String),
    Interrupted,
}

pub async fn execute(db: Db, run: &str, cancel: CancellationToken, resume: bool) -> Result<String> {
    let detail = db
        .detail(run)?
        .ok_or_else(|| anyhow::anyhow!("exécution inconnue : {run}"))?;
    detail.workflow.validate()?;
    if !resume && detail.run.status != "pending" {
        bail!("exécution déjà démarrée : {run}");
    }
    if resume {
        if detail.run.status == "success" || detail.run.status == "failed" {
            bail!("exécution terminée : {run}");
        }
        db.recover(run, &detail.workflow)?;
    }
    db.set_run(run, "running")?;
    let source_dir = PathBuf::from(detail.source_dir);
    let limit = detail.max_parallel as usize;
    let mut jobs = JoinSet::new();
    loop {
        let snapshot = db.detail(run)?.unwrap();
        let statuses: HashMap<_, _> = snapshot
            .tasks
            .iter()
            .map(|t| (t.id.as_str(), t.status.as_str()))
            .collect();
        if !cancel.is_cancelled() {
            let mut newly_blocked = false;
            for task in &detail.workflow.tasks {
                if statuses[task.id.as_str()] != "pending" {
                    continue;
                }
                if task
                    .dependencies
                    .iter()
                    .any(|dep| matches!(statuses[dep.as_str()], "failed" | "blocked"))
                {
                    db.set_task(run, &task.id, "blocked", Some("dépendance échouée"))?;
                    newly_blocked = true;
                }
            }
            if newly_blocked {
                continue;
            }
            let snapshot = db.detail(run)?.unwrap();
            let statuses: HashMap<_, _> = snapshot
                .tasks
                .iter()
                .map(|t| (t.id.as_str(), t.status.as_str()))
                .collect();
            for task in &detail.workflow.tasks {
                if jobs.len() >= limit {
                    break;
                }
                if statuses[task.id.as_str()] == "pending"
                    && task
                        .dependencies
                        .iter()
                        .all(|dep| statuses[dep.as_str()] == "success")
                {
                    db.set_task(run, &task.id, "queued", None)?;
                    let db = db.clone();
                    let run = run.to_string();
                    let task = task.clone();
                    let dir = source_dir.clone();
                    let cancel = cancel.clone();
                    jobs.spawn(async move {
                        let outcome = worker(db, &run, &task, &dir, cancel).await;
                        (task.id, outcome)
                    });
                }
            }
        }
        if jobs.is_empty() {
            let snapshot = db.detail(run)?.unwrap();
            let status = if cancel.is_cancelled() {
                "interrupted"
            } else if snapshot.tasks.iter().all(|t| t.status == "success") {
                "success"
            } else if snapshot.tasks.iter().any(|t| t.status == "pending") {
                bail!("scheduler bloqué : tâches en attente sans travail actif");
            } else {
                "failed"
            };
            db.set_run(run, status)?;
            return Ok(status.to_string());
        }
        if let Some(result) = jobs.join_next().await {
            let (task, outcome) = result?;
            match outcome? {
                Outcome::Success => db.set_task(run, &task, "success", None)?,
                Outcome::Failed(error) => db.set_task(run, &task, "failed", Some(&error))?,
                Outcome::Interrupted => db.set_task(
                    run,
                    &task,
                    "pending",
                    Some("interrompue ; utiliser kordrel resume"),
                )?,
            }
        }
    }
}

async fn worker(
    db: Db,
    run: &str,
    task: &Task,
    source_dir: &Path,
    cancel: CancellationToken,
) -> Result<Outcome> {
    loop {
        if cancel.is_cancelled() {
            return Ok(Outcome::Interrupted);
        }
        let attempt = db.start_attempt(run, &task.id)?;
        let result = run_attempt(db.clone(), run, task, source_dir, attempt, cancel.clone()).await;
        match result {
            AttemptResult::Success => {
                db.finish_attempt(run, &task.id, attempt, "success", None)?;
                return Ok(Outcome::Success);
            }
            AttemptResult::Interrupted => {
                db.finish_attempt(run, &task.id, attempt, "interrupted", Some("arrêt demandé"))?;
                return Ok(Outcome::Interrupted);
            }
            AttemptResult::Failure(error) => {
                db.finish_attempt(run, &task.id, attempt, "failed", Some(&error))?;
                if attempt >= task.attempts() {
                    return Ok(Outcome::Failed(error));
                }
                db.set_task(run, &task.id, "retrying", Some(&error))?;
                let delay = 250u64
                    .saturating_mul(1u64 << ((attempt - 1).clamp(0, 16) as u32))
                    .min(30_000);
                tokio::select! { _ = tokio::time::sleep(Duration::from_millis(delay)) => {}, _ = cancel.cancelled() => return Ok(Outcome::Interrupted) }
            }
        }
    }
}

async fn read_pipe<R: AsyncRead + Unpin>(
    pipe: R,
    db: Db,
    run: String,
    task: String,
    attempt: i64,
    stream: &'static str,
) {
    let mut lines = BufReader::new(pipe).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        if db.log(&run, &task, attempt, stream, &line).is_err() {
            break;
        }
    }
}

async fn run_attempt(
    db: Db,
    run: &str,
    task: &Task,
    source_dir: &Path,
    attempt: i64,
    cancel: CancellationToken,
) -> AttemptResult {
    let mut cmd = Command::new(&task.command);
    cmd.args(&task.args)
        .envs(&task.env)
        .current_dir(source_dir.join(task.workdir.as_deref().unwrap_or(".")))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(e) => return AttemptResult::Failure(format!("démarrage impossible : {e}")),
    };
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let out = tokio::spawn(read_pipe(
        stdout,
        db.clone(),
        run.into(),
        task.id.clone(),
        attempt,
        "stdout",
    ));
    let err = tokio::spawn(read_pipe(
        stderr,
        db,
        run.into(),
        task.id.clone(),
        attempt,
        "stderr",
    ));
    let result = if let Some(seconds) = task.timeout_seconds {
        tokio::select! {
            status = child.wait() => status.map(|s| if s.success() { AttemptResult::Success } else { AttemptResult::Failure(format!("code de sortie : {s}")) }).unwrap_or_else(|e| AttemptResult::Failure(e.to_string())),
            _ = tokio::time::sleep(Duration::from_secs(seconds as u64)) => { let _ = child.kill().await; AttemptResult::Failure(format!("timeout après {seconds} s")) },
            _ = cancel.cancelled() => { let _ = child.kill().await; AttemptResult::Interrupted },
        }
    } else {
        tokio::select! {
            status = child.wait() => status.map(|s| if s.success() { AttemptResult::Success } else { AttemptResult::Failure(format!("code de sortie : {s}")) }).unwrap_or_else(|e| AttemptResult::Failure(e.to_string())),
            _ = cancel.cancelled() => { let _ = child.kill().await; AttemptResult::Interrupted },
        }
    };
    let _ = out.await;
    let _ = err.await;
    result
}
