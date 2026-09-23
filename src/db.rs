use crate::workflow::Workflow;
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension};
use serde::Serialize;
use std::{
    path::Path,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Clone)]
pub struct Db(Arc<Mutex<Connection>>);

#[derive(Clone, Debug, Serialize)]
pub struct RunSummary {
    pub id: String,
    pub name: String,
    pub status: String,
    pub created_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct TaskView {
    pub id: String,
    pub status: String,
    pub attempts: i64,
    pub started_at: Option<i64>,
    pub finished_at: Option<i64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct AttemptView {
    pub task_id: String,
    pub number: i64,
    pub status: String,
    pub started_at: i64,
    pub finished_at: Option<i64>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct LogLine {
    pub id: i64,
    pub task_id: String,
    pub attempt: i64,
    pub stream: String,
    pub line: String,
    pub created_at: i64,
}

#[derive(Clone, Debug, Serialize)]
pub struct RunDetail {
    #[serde(flatten)]
    pub run: RunSummary,
    pub workflow: Workflow,
    pub source_dir: String,
    pub max_parallel: i64,
    pub tasks: Vec<TaskView>,
    pub attempts: Vec<AttemptView>,
}

#[derive(Clone, Debug, Serialize)]
pub struct EventRow {
    pub id: i64,
    pub run_id: String,
    pub kind: String,
    pub payload: String,
    pub created_at: i64,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path).with_context(|| format!("SQLite {}", path.display()))?;
        conn.busy_timeout(Duration::from_secs(5))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;
            CREATE TABLE IF NOT EXISTS runs (id TEXT PRIMARY KEY, name TEXT NOT NULL, status TEXT NOT NULL, workflow_json TEXT NOT NULL, source_dir TEXT NOT NULL, max_parallel INTEGER NOT NULL, created_at INTEGER NOT NULL, started_at INTEGER, finished_at INTEGER);
            CREATE TABLE IF NOT EXISTS tasks (run_id TEXT NOT NULL, id TEXT NOT NULL, status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0, started_at INTEGER, finished_at INTEGER, error TEXT, PRIMARY KEY(run_id,id), FOREIGN KEY(run_id) REFERENCES runs(id));
            CREATE TABLE IF NOT EXISTS attempts (run_id TEXT NOT NULL, task_id TEXT NOT NULL, number INTEGER NOT NULL, status TEXT NOT NULL, started_at INTEGER NOT NULL, finished_at INTEGER, error TEXT, PRIMARY KEY(run_id,task_id,number));
            CREATE TABLE IF NOT EXISTS logs (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, task_id TEXT NOT NULL, attempt INTEGER NOT NULL, stream TEXT NOT NULL, line TEXT NOT NULL, created_at INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS logs_run_id ON logs(run_id,id);
            CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, kind TEXT NOT NULL, payload TEXT NOT NULL, created_at INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS events_run_id ON events(run_id,id);")?;
        Ok(Self(Arc::new(Mutex::new(conn))))
    }

    fn event(conn: &Connection, run: &str, kind: &str, value: serde_json::Value) -> Result<()> {
        conn.execute(
            "INSERT INTO events(run_id,kind,payload,created_at) VALUES (?1,?2,?3,?4)",
            params![run, kind, value.to_string(), now()],
        )?;
        Ok(())
    }

    pub fn create_run(
        &self,
        workflow: &Workflow,
        source_dir: &Path,
        max_parallel: usize,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("INSERT INTO runs(id,name,status,workflow_json,source_dir,max_parallel,created_at) VALUES (?1,?2,'pending',?3,?4,?5,?6)", params![id,workflow.name,serde_json::to_string(workflow)?,source_dir.to_string_lossy(),max_parallel as i64,now()])?;
        for task in &workflow.tasks {
            tx.execute(
                "INSERT INTO tasks(run_id,id,status) VALUES (?1,?2,'pending')",
                params![id, task.id],
            )?;
        }
        Self::event(&tx, &id, "run", serde_json::json!({"status":"pending"}))?;
        tx.commit()?;
        Ok(id)
    }

    pub fn set_run(&self, run: &str, status: &str) -> Result<()> {
        let conn = self.0.lock().unwrap();
        conn.execute("UPDATE runs SET status=?2, started_at=COALESCE(started_at,?3), finished_at=CASE WHEN ?2='running' THEN NULL ELSE ?3 END WHERE id=?1", params![run,status,now()])?;
        Self::event(&conn, run, "run", serde_json::json!({"status":status}))
    }

    pub fn set_task(&self, run: &str, task: &str, status: &str, error: Option<&str>) -> Result<()> {
        let conn = self.0.lock().unwrap();
        conn.execute("UPDATE tasks SET status=?3,error=?4,started_at=CASE WHEN ?3='running' THEN ?5 ELSE started_at END,finished_at=CASE WHEN ?3 IN ('success','failed','blocked') THEN ?5 ELSE NULL END WHERE run_id=?1 AND id=?2", params![run,task,status,error,now()])?;
        Self::event(
            &conn,
            run,
            "task",
            serde_json::json!({"task_id":task,"status":status,"error":error}),
        )
    }

    pub fn start_attempt(&self, run: &str, task: &str) -> Result<i64> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("UPDATE tasks SET status='running',attempts=attempts+1,started_at=COALESCE(started_at,?3),finished_at=NULL,error=NULL WHERE run_id=?1 AND id=?2", params![run,task,now()])?;
        let number: i64 = tx.query_row(
            "SELECT attempts FROM tasks WHERE run_id=?1 AND id=?2",
            params![run, task],
            |r| r.get(0),
        )?;
        tx.execute("INSERT INTO attempts(run_id,task_id,number,status,started_at) VALUES (?1,?2,?3,'running',?4)", params![run,task,number,now()])?;
        Self::event(
            &tx,
            run,
            "task",
            serde_json::json!({"task_id":task,"status":"running","attempt":number}),
        )?;
        tx.commit()?;
        Ok(number)
    }

    pub fn finish_attempt(
        &self,
        run: &str,
        task: &str,
        number: i64,
        status: &str,
        error: Option<&str>,
    ) -> Result<()> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("UPDATE attempts SET status=?4,finished_at=?5,error=?6 WHERE run_id=?1 AND task_id=?2 AND number=?3", params![run,task,number,status,now(),error])?;
        if status == "success" {
            tx.execute("UPDATE tasks SET status='success',finished_at=?3,error=NULL WHERE run_id=?1 AND id=?2", params![run,task,now()])?;
            Self::event(
                &tx,
                run,
                "task",
                serde_json::json!({"task_id":task,"status":"success"}),
            )?;
        }
        Self::event(
            &tx,
            run,
            "attempt",
            serde_json::json!({"task_id":task,"attempt":number,"status":status,"error":error}),
        )?;
        tx.commit()?;
        Ok(())
    }

    pub fn log(&self, run: &str, task: &str, attempt: i64, stream: &str, line: &str) -> Result<()> {
        let conn = self.0.lock().unwrap();
        conn.execute("INSERT INTO logs(run_id,task_id,attempt,stream,line,created_at) VALUES (?1,?2,?3,?4,?5,?6)", params![run,task,attempt,stream,line,now()])?;
        Self::event(
            &conn,
            run,
            "log",
            serde_json::json!({"task_id":task,"attempt":attempt,"stream":stream,"line":line}),
        )
    }

    pub fn recover(&self, run: &str, workflow: &Workflow) -> Result<()> {
        let mut conn = self.0.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute("UPDATE attempts SET status='interrupted',finished_at=?2,error='Kordrel interrompu' WHERE run_id=?1 AND status='running'", params![run,now()])?;
        for task in &workflow.tasks {
            let (status, count): (String, i64) = tx.query_row(
                "SELECT status,attempts FROM tasks WHERE run_id=?1 AND id=?2",
                params![run, task.id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            if matches!(status.as_str(), "running" | "retrying" | "queued")
                || status == "pending" && count >= task.attempts()
            {
                let (new_status, error) = if count >= task.attempts() {
                    ("failed", "budget de tentatives épuisé après interruption")
                } else {
                    ("pending", "tentative interrompue, reprise autorisée")
                };
                tx.execute("UPDATE tasks SET status=?3,error=?4,finished_at=CASE WHEN ?3='failed' THEN ?5 ELSE NULL END WHERE run_id=?1 AND id=?2", params![run,task.id,new_status,error,now()])?;
                Self::event(
                    &tx,
                    run,
                    "task",
                    serde_json::json!({"task_id":task.id,"status":new_status,"error":error}),
                )?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_runs(&self) -> Result<Vec<RunSummary>> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id,name,status,created_at,started_at,finished_at FROM runs ORDER BY created_at DESC LIMIT 100")?;
        let rows = stmt
            .query_map([], |r| {
                Ok(RunSummary {
                    id: r.get(0)?,
                    name: r.get(1)?,
                    status: r.get(2)?,
                    created_at: r.get(3)?,
                    started_at: r.get(4)?,
                    finished_at: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn detail(&self, id: &str) -> Result<Option<RunDetail>> {
        let conn = self.0.lock().unwrap();
        let head: Option<(RunSummary,String,String,i64)> = conn.query_row("SELECT id,name,status,created_at,started_at,finished_at,workflow_json,source_dir,max_parallel FROM runs WHERE id=?1", [id], |r| Ok((RunSummary { id:r.get(0)?,name:r.get(1)?,status:r.get(2)?,created_at:r.get(3)?,started_at:r.get(4)?,finished_at:r.get(5)? },r.get(6)?,r.get(7)?,r.get(8)?))).optional()?;
        let Some((run, json, source_dir, max_parallel)) = head else {
            return Ok(None);
        };
        let mut stmt = conn.prepare("SELECT id,status,attempts,started_at,finished_at,error FROM tasks WHERE run_id=?1 ORDER BY rowid")?;
        let tasks = stmt
            .query_map([id], |r| {
                Ok(TaskView {
                    id: r.get(0)?,
                    status: r.get(1)?,
                    attempts: r.get(2)?,
                    started_at: r.get(3)?,
                    finished_at: r.get(4)?,
                    error: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let mut stmt = conn.prepare("SELECT task_id,number,status,started_at,finished_at,error FROM attempts WHERE run_id=?1 ORDER BY rowid")?;
        let attempts = stmt
            .query_map([id], |r| {
                Ok(AttemptView {
                    task_id: r.get(0)?,
                    number: r.get(1)?,
                    status: r.get(2)?,
                    started_at: r.get(3)?,
                    finished_at: r.get(4)?,
                    error: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(Some(RunDetail {
            run,
            workflow: serde_json::from_str(&json)?,
            source_dir,
            max_parallel,
            tasks,
            attempts,
        }))
    }

    pub fn logs(&self, run: &str, task: Option<&str>) -> Result<Vec<LogLine>> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id,task_id,attempt,stream,line,created_at FROM logs WHERE run_id=?1 AND (?2 IS NULL OR task_id=?2) ORDER BY id")?;
        let rows = stmt
            .query_map(params![run, task], |r| {
                Ok(LogLine {
                    id: r.get(0)?,
                    task_id: r.get(1)?,
                    attempt: r.get(2)?,
                    stream: r.get(3)?,
                    line: r.get(4)?,
                    created_at: r.get(5)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }

    pub fn events_after(&self, run: &str, after: i64) -> Result<Vec<EventRow>> {
        let conn = self.0.lock().unwrap();
        let mut stmt = conn.prepare("SELECT id,run_id,kind,payload,created_at FROM events WHERE run_id=?1 AND id>?2 ORDER BY id LIMIT 200")?;
        let rows = stmt
            .query_map(params![run, after], |r| {
                Ok(EventRow {
                    id: r.get(0)?,
                    run_id: r.get(1)?,
                    kind: r.get(2)?,
                    payload: r.get(3)?,
                    created_at: r.get(4)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows)
    }
}
