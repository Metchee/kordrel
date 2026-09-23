use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use fs2::FileExt;
use kordrel::{api, db::Db, scheduler, workflow::Workflow};
use std::{
    fs::OpenOptions,
    net::SocketAddr,
    path::{Path, PathBuf},
};
use tokio_util::sync::CancellationToken;

#[derive(Parser)]
#[command(
    name = "kordrel",
    version,
    about = "Orchestrateur local de workflows YAML"
)]
struct Cli {
    #[arg(
        long,
        global = true,
        env = "KORDREL_DB",
        help = "Base SQLite (nouvelle base ou historique existant par défaut)"
    )]
    db: Option<PathBuf>,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Validate {
        file: PathBuf,
    },
    Run {
        file: PathBuf,
        #[arg(long)]
        max_parallel: Option<i64>,
    },
    Resume {
        run_id: String,
    },
    Status {
        run_id: String,
    },
    Logs {
        run_id: String,
        task_id: Option<String>,
    },
    Serve {
        #[arg(long, default_value = "127.0.0.1:8080")]
        listen: SocketAddr,
    },
    Simulate {
        file: PathBuf,
        #[arg(long)]
        max_parallel: Option<i64>,
    },
}

fn parallel(workflow: &Workflow, override_value: Option<i64>) -> Result<usize> {
    let n = override_value.or(workflow.max_parallel).unwrap_or(4);
    if !(1..=1024).contains(&n) {
        bail!("max_parallel doit être entre 1 et 1024");
    }
    Ok(n as usize)
}

fn database_path(explicit: Option<PathBuf>) -> PathBuf {
    if let Some(path) = explicit {
        return path;
    }
    if let Some(path) = std::env::var_os("FORGE_DB") {
        return PathBuf::from(path);
    }
    prefer_database(
        PathBuf::from(".kordrel/kordrel.db"),
        PathBuf::from(".forge/forge.db"),
    )
}

fn prefer_database(current: PathBuf, legacy: PathBuf) -> PathBuf {
    if current.exists() || !legacy.exists() {
        current
    } else {
        legacy
    }
}

async fn drive(db: Db, db_path: &Path, id: &str, resume: bool) -> Result<String> {
    let lock_prefix = if db_path.ends_with(".forge/forge.db") {
        "forge"
    } else {
        "kordrel"
    };
    let lock_path = db_path.with_file_name(format!("{lock_prefix}-{id}.lock"));
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    lock.try_lock_exclusive()
        .with_context(|| format!("l'exécution {id} est déjà contrôlée par un autre processus"))?;
    let cancel = CancellationToken::new();
    let signal = cancel.clone();
    ctrlc::set_handler(move || {
        eprintln!("Kordrel : arrêt demandé, attente des tâches actives…");
        signal.cancel();
    })?;
    let result = scheduler::execute(db.clone(), id, cancel, resume).await;
    if result.is_err() {
        if let Ok(Some(run)) = db.detail(id) {
            if run.run.status == "running" {
                let _ = db.recover(id, &run.workflow);
                let _ = db.set_run(id, "interrupted");
            }
        }
    }
    FileExt::unlock(&lock)?;
    result
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let db_path = database_path(cli.db.clone());
    match cli.command {
        Commands::Validate { file } => {
            let w = Workflow::load(&file)?;
            println!("Workflow '{}' valide ({} tâches)", w.name, w.tasks.len());
        }
        Commands::Simulate { file, max_parallel } => {
            let w = Workflow::load(&file)?;
            let n = parallel(&w, max_parallel)?;
            println!("{} · limite de concurrence {n}", w.name);
            for (i, group) in w.groups(n).iter().enumerate() {
                println!("groupe {} : {}", i + 1, group.join(", "));
            }
        }
        Commands::Run { file, max_parallel } => {
            let path = std::fs::canonicalize(&file)
                .with_context(|| format!("fichier introuvable : {}", file.display()))?;
            let w = Workflow::load(&path)?;
            let n = parallel(&w, max_parallel)?;
            let db = Db::open(&db_path)?;
            let id = db.create_run(&w, path.parent().unwrap(), n)?;
            println!("run-id: {id}");
            let status = drive(db, &db_path, &id, false).await?;
            println!("état final : {status}");
            if status == "failed" {
                std::process::exit(1);
            }
        }
        Commands::Resume { run_id } => {
            let db = Db::open(&db_path)?;
            let status = drive(db, &db_path, &run_id, true).await?;
            println!("{run_id} : {status}");
            if status == "failed" {
                std::process::exit(1);
            }
        }
        Commands::Status { run_id } => {
            let db = Db::open(&db_path)?;
            let run = db
                .detail(&run_id)?
                .ok_or_else(|| anyhow::anyhow!("exécution inconnue : {run_id}"))?;
            println!("{} [{}] {}", run.run.name, run.run.id, run.run.status);
            for t in run.tasks {
                println!(
                    "  {:<24} {:<12} essais={}{}",
                    t.id,
                    t.status,
                    t.attempts,
                    t.error.map(|e| format!(" erreur={e}")).unwrap_or_default()
                );
            }
        }
        Commands::Logs { run_id, task_id } => {
            let db = Db::open(&db_path)?;
            if db.detail(&run_id)?.is_none() {
                bail!("exécution inconnue : {run_id}");
            }
            for line in db.logs(&run_id, task_id.as_deref())? {
                println!(
                    "{} #{} {}: {}",
                    line.task_id, line.attempt, line.stream, line.line
                );
            }
        }
        Commands::Serve { listen } => {
            let db = Db::open(&db_path)?;
            let listener = tokio::net::TcpListener::bind(listen).await?;
            let shutdown = CancellationToken::new();
            let signal = shutdown.clone();
            ctrlc::set_handler(move || signal.cancel())?;
            println!("dashboard : http://{listen}");
            axum::serve(listener, api::router(db))
                .with_graceful_shutdown(async move { shutdown.cancelled().await })
                .await?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::prefer_database;

    #[test]
    fn existing_database_is_preserved_during_rename() {
        let temp = tempfile::tempdir().unwrap();
        let current = temp.path().join(".kordrel/kordrel.db");
        let legacy = temp.path().join(".forge/forge.db");
        assert_eq!(prefer_database(current.clone(), legacy.clone()), current);
        std::fs::create_dir_all(legacy.parent().unwrap()).unwrap();
        std::fs::write(&legacy, []).unwrap();
        assert_eq!(prefer_database(current.clone(), legacy.clone()), legacy);
        std::fs::create_dir_all(current.parent().unwrap()).unwrap();
        std::fs::write(&current, []).unwrap();
        assert_eq!(prefer_database(current.clone(), legacy), current);
    }
}
