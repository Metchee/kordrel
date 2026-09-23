use kordrel::{
    api,
    db::Db,
    scheduler,
    workflow::{Task, Workflow},
};
use std::{collections::HashMap, path::Path, time::Duration};
use tempfile::TempDir;
use tokio_util::sync::CancellationToken;

fn task(id: &str, script: &str, deps: &[&str]) -> Task {
    Task {
        id: id.into(),
        command: "sh".into(),
        args: vec!["-c".into(), script.into()],
        dependencies: deps.iter().map(|s| s.to_string()).collect(),
        workdir: None,
        timeout_seconds: None,
        retries: None,
        max_attempts: None,
        env: HashMap::new(),
    }
}
fn workflow(tasks: Vec<Task>) -> Workflow {
    Workflow {
        name: "test".into(),
        max_parallel: Some(2),
        tasks,
    }
}
fn setup() -> (TempDir, Db) {
    let temp = TempDir::new().unwrap();
    let db = Db::open(&temp.path().join("kordrel.db")).unwrap();
    (temp, db)
}
async fn run(db: Db, w: Workflow, dir: &Path, limit: usize) -> (String, String) {
    w.validate().unwrap();
    let id = db.create_run(&w, dir, limit).unwrap();
    let status = scheduler::execute(db, &id, CancellationToken::new(), false)
        .await
        .unwrap();
    (id, status)
}

#[tokio::test]
async fn dependencies_and_failure_propagation() {
    let (dir, db) = setup();
    let w = workflow(vec![
        task("prepare", "printf ok > marker", &[]),
        task("check", "test -f marker", &["prepare"]),
        task("fail", "exit 9", &["check"]),
        task("blocked", "printf bad > unwanted", &["fail"]),
        task("descendant", "printf bad > unwanted2", &["blocked"]),
    ]);
    let (id, status) = run(db.clone(), w, dir.path(), 2).await;
    assert_eq!(status, "failed");
    let view = db.detail(&id).unwrap().unwrap();
    assert_eq!(
        view.tasks
            .iter()
            .map(|t| t.status.as_str())
            .collect::<Vec<_>>(),
        vec!["success", "success", "failed", "blocked", "blocked"]
    );
    assert!(!dir.path().join("unwanted").exists());
    assert!(!dir.path().join("unwanted2").exists());
}

#[tokio::test]
async fn parallel_respects_limit() {
    let (dir, db) = setup();
    let w = workflow(vec![
        task("a", "sleep 0.35", &[]),
        task("b", "sleep 0.35", &[]),
    ]);
    let (id, status) = run(db.clone(), w.clone(), dir.path(), 2).await;
    assert_eq!(status, "success");
    let tasks = db.detail(&id).unwrap().unwrap().tasks;
    assert!(
        tasks[0].started_at.unwrap() < tasks[1].finished_at.unwrap()
            && tasks[1].started_at.unwrap() < tasks[0].finished_at.unwrap()
    );
    let (id, status) = run(db.clone(), w, dir.path(), 1).await;
    assert_eq!(status, "success");
    let tasks = db.detail(&id).unwrap().unwrap().tasks;
    assert!(tasks[0].finished_at.unwrap() <= tasks[1].started_at.unwrap());
}

#[tokio::test]
async fn retry_success_and_final_failure() {
    let (dir, db) = setup();
    let mut a = task(
        "flaky",
        "if test -f once; then exit 0; else touch once; exit 3; fi",
        &[],
    );
    a.retries = Some(1);
    let mut b = task("always_fails", "exit 4", &[]);
    b.retries = Some(1);
    let (id, status) = run(db.clone(), workflow(vec![a, b]), dir.path(), 2).await;
    assert_eq!(status, "failed");
    let view = db.detail(&id).unwrap().unwrap();
    assert_eq!(view.tasks[0].status, "success");
    assert_eq!(view.tasks[0].attempts, 2);
    assert_eq!(view.tasks[1].status, "failed");
    assert_eq!(view.tasks[1].attempts, 2);
    assert_eq!(view.attempts.len(), 4);
}

#[tokio::test]
async fn timeout_kills_task() {
    let (dir, db) = setup();
    let mut a = task("slow", "exec sleep 5", &[]);
    a.timeout_seconds = Some(1);
    let (id, status) = run(db.clone(), workflow(vec![a]), dir.path(), 1).await;
    assert_eq!(status, "failed");
    assert!(db.detail(&id).unwrap().unwrap().attempts[0]
        .error
        .as_ref()
        .unwrap()
        .contains("timeout"));
}

#[tokio::test]
async fn interrupted_run_resumes_without_repeating_success() {
    let (dir, db) = setup();
    let a = task("first", "printf x >> count", &[]);
    let mut b = task(
        "second",
        "if test -f started; then exit 0; else touch started; exec sleep 5; fi",
        &["first"],
    );
    b.max_attempts = Some(2);
    let w = workflow(vec![a, b]);
    let id = db.create_run(&w, dir.path(), 1).unwrap();
    let cancel = CancellationToken::new();
    let run_id = id.clone();
    let running_db = db.clone();
    let running_cancel = cancel.clone();
    let work = tokio::spawn(async move {
        scheduler::execute(running_db, &run_id, running_cancel, false).await
    });
    for _ in 0..100 {
        if dir.path().join("started").exists() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(dir.path().join("started").exists());
    cancel.cancel();
    assert_eq!(work.await.unwrap().unwrap(), "interrupted");
    assert_eq!(
        scheduler::execute(db.clone(), &id, CancellationToken::new(), true)
            .await
            .unwrap(),
        "success"
    );
    assert_eq!(
        std::fs::read_to_string(dir.path().join("count")).unwrap(),
        "x"
    );
    let view = db.detail(&id).unwrap().unwrap();
    assert_eq!(view.tasks[0].attempts, 1);
    assert_eq!(view.tasks[1].attempts, 2);
}

#[tokio::test]
async fn crash_recovery_closes_stale_attempt() {
    let (dir, db) = setup();
    let first = task("first", "printf x >> count", &[]);
    let mut second = task("second", "printf y > result", &["first"]);
    second.max_attempts = Some(2);
    let w = workflow(vec![first, second]);
    let id = db.create_run(&w, dir.path(), 1).unwrap();
    db.set_run(&id, "running").unwrap();
    let number = db.start_attempt(&id, "first").unwrap();
    std::fs::write(dir.path().join("count"), "x").unwrap();
    db.finish_attempt(&id, "first", number, "success", None)
        .unwrap();
    db.set_task(&id, "first", "success", None).unwrap();
    db.start_attempt(&id, "second").unwrap();
    assert_eq!(
        scheduler::execute(db.clone(), &id, CancellationToken::new(), true)
            .await
            .unwrap(),
        "success"
    );
    let view = db.detail(&id).unwrap().unwrap();
    assert_eq!(
        view.attempts
            .iter()
            .find(|a| a.task_id == "second" && a.number == 1)
            .unwrap()
            .status,
        "interrupted"
    );
    assert_eq!(view.tasks[0].attempts, 1);
    assert_eq!(
        std::fs::read_to_string(dir.path().join("count")).unwrap(),
        "x"
    );
}

#[tokio::test]
async fn api_returns_persisted_states_and_logs() {
    use tower::ServiceExt;
    let (dir, db) = setup();
    let (id, _) = run(
        db.clone(),
        workflow(vec![task("hello", "echo hello", &[])]),
        dir.path(),
        1,
    )
    .await;
    let app = api::router(db);
    let response = app
        .clone()
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/api/runs/{id}"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), axum::http::StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["tasks"][0]["status"], "success");
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri(format!("/api/runs/{id}/logs/hello"))
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&body).unwrap()[0]["line"],
        "hello"
    );
}
