use crate::db::Db;
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse,
    },
    routing::get,
    Json, Router,
};
use futures_util::stream;
use std::{collections::VecDeque, convert::Infallible, time::Duration};

pub fn router(db: Db) -> Router {
    Router::new()
        .route("/", get(|| async { Html(include_str!("dashboard.html")) }))
        .route("/api/runs", get(list_runs))
        .route("/api/runs/{id}", get(detail))
        .route("/api/runs/{id}/tasks", get(tasks))
        .route("/api/runs/{id}/logs", get(logs))
        .route("/api/runs/{id}/logs/{task}", get(task_logs))
        .route("/api/runs/{id}/events", get(events))
        .with_state(db)
}

type ApiError = (StatusCode, Json<serde_json::Value>);
fn failure(status: StatusCode, message: impl ToString) -> ApiError {
    (
        status,
        Json(serde_json::json!({"error":message.to_string()})),
    )
}

async fn list_runs(State(db): State<Db>) -> Result<Json<Vec<crate::db::RunSummary>>, ApiError> {
    db.list_runs()
        .map(Json)
        .map_err(|e| failure(StatusCode::INTERNAL_SERVER_ERROR, e))
}

async fn detail(
    State(db): State<Db>,
    Path(id): Path<String>,
) -> Result<Json<crate::db::RunDetail>, ApiError> {
    db.detail(&id)
        .map_err(|e| failure(StatusCode::INTERNAL_SERVER_ERROR, e))?
        .map(Json)
        .ok_or_else(|| failure(StatusCode::NOT_FOUND, "exécution inconnue"))
}

async fn tasks(
    State(db): State<Db>,
    Path(id): Path<String>,
) -> Result<Json<Vec<crate::db::TaskView>>, ApiError> {
    Ok(Json(detail(State(db), Path(id)).await?.0.tasks))
}

async fn logs(
    State(db): State<Db>,
    Path(id): Path<String>,
) -> Result<Json<Vec<crate::db::LogLine>>, ApiError> {
    if db
        .detail(&id)
        .map_err(|e| failure(StatusCode::INTERNAL_SERVER_ERROR, e))?
        .is_none()
    {
        return Err(failure(StatusCode::NOT_FOUND, "exécution inconnue"));
    }
    db.logs(&id, None)
        .map(Json)
        .map_err(|e| failure(StatusCode::INTERNAL_SERVER_ERROR, e))
}

async fn task_logs(
    State(db): State<Db>,
    Path((id, task)): Path<(String, String)>,
) -> Result<Json<Vec<crate::db::LogLine>>, ApiError> {
    let run = db
        .detail(&id)
        .map_err(|e| failure(StatusCode::INTERNAL_SERVER_ERROR, e))?
        .ok_or_else(|| failure(StatusCode::NOT_FOUND, "exécution inconnue"))?;
    if !run.tasks.iter().any(|t| t.id == task) {
        return Err(failure(StatusCode::NOT_FOUND, "tâche inconnue"));
    }
    db.logs(&id, Some(&task))
        .map(Json)
        .map_err(|e| failure(StatusCode::INTERNAL_SERVER_ERROR, e))
}

async fn events(
    State(db): State<Db>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<impl IntoResponse, ApiError> {
    if db
        .detail(&id)
        .map_err(|e| failure(StatusCode::INTERNAL_SERVER_ERROR, e))?
        .is_none()
    {
        return Err(failure(StatusCode::NOT_FOUND, "exécution inconnue"));
    }
    let cursor = headers
        .get("last-event-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(0);
    let stream = stream::unfold(
        (db, id, cursor, VecDeque::<crate::db::EventRow>::new()),
        |(db, id, mut cursor, mut pending)| async move {
            loop {
                if let Some(row) = pending.pop_front() {
                    cursor = row.id;
                    let event = Event::default()
                        .id(row.id.to_string())
                        .event(row.kind)
                        .data(row.payload);
                    return Some((Ok::<_, Infallible>(event), (db, id, cursor, pending)));
                }
                match db.events_after(&id, cursor) {
                    Ok(rows) => pending.extend(rows),
                    Err(e) => {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        return Some((
                            Ok(Event::default().event("error").data(e.to_string())),
                            (db, id, cursor, pending),
                        ));
                    }
                }
                if pending.is_empty() {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
            }
        },
    );
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}
