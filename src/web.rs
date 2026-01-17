use axum::{
    extract::{Json, Query, State},
    http::StatusCode,
    routing::{get, post},
    Router,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::{sync::Arc, time::Instant};
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    config::Settings,
    observability::METRICS as OBS_METRICS,
    redis_client::RedisClient,
};

#[derive(Debug, Serialize)]
pub struct SubmitResponse {
    pub status: String,
    pub message: String,
}

#[derive(Debug)]
pub struct AppState {
    pub settings: Settings,
    pub redis: Arc<RedisClient>,
    pub http_client: Client,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api1", post(handle_api1))
        .route("/streams/peek", get(handle_stream_peek))
        .route("/streams/pending", get(handle_stream_pending))
        .route("/health", get(handle_health))
        .route("/metrics", get(handle_metrics))
        .with_state(state)
}

/// 健康检查
async fn handle_health(
    State(state): State<Arc<AppState>>,
) -> (StatusCode, Json<serde_json::Value>) {
    let mut health_status = serde_json::json!({
        "status": "healthy",
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    // Redis 健康
    match state.redis.check_health().await {
        Ok(info_map) => {
            health_status["redis"] = serde_json::json!({
                "status": "healthy",
                "info": info_map
            });
        }
        Err(e) => {
            health_status["status"] = serde_json::json!("degraded");
            health_status["redis"] = serde_json::json!({
                "status": "unhealthy",
                "error": format!("{}", e)
            });
        }
    }

    // MySQL 健康（可选）
    // 已移除数据库依赖，不再检查 MySQL

    let status_code = match health_status["status"].as_str() {
        Some("healthy") => StatusCode::OK,
        Some("degraded") => StatusCode::OK,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };

    (status_code, Json(health_status))
}

/// /api1 入口：限流 + 入队 Redis Stream
async fn handle_api1(
    State(state): State<Arc<AppState>>,
    Json(req): Json<serde_json::Value>,
) -> Result<(StatusCode, Json<SubmitResponse>), (StatusCode, String)> {
    let start_time = Instant::now();
    let trace_id = Uuid::new_v4().to_string();
    let span = tracing::span!(tracing::Level::INFO, "handle_api1", trace_id = %trace_id);
    let _guard = span.enter();
    
    info!(trace_id = %trace_id, "收到 /api1 请求");

    // 移除入口限流控制 - 让请求直接入队，由 Worker 进行分布式限流
    // if !state.smart_match_limiter.check().await {
    //     OBS_METRICS.record_rate_limit_rejected("api1");
    //     OBS_METRICS.record_http_request("POST", "api1", 429, start_time.elapsed());
    //     warn!(trace_id = %trace_id, "api1 请求被限流");
    //     return Err((StatusCode::TOO_MANY_REQUESTS, "rate limited".into()));
    // }

    // 组装任务
    let id = Uuid::new_v4().to_string();
    let downstream_url = state.settings.downstream_url.clone()
        .unwrap_or_else(|| "http://192.168.9.123:63374/api1".to_string());
    let task = serde_json::json!({
        "id": id,
        "task_name": "api1",
        "payload": req,
        "attempts": 0,
        "created_at": chrono::Utc::now().to_rfc3339(),
        "downstream_url": downstream_url,
        "rate_limit_key": "api1",
        "trace_id": trace_id,
    });

    info!(trace_id = %trace_id, task_id = %id, "准备将任务入队");

    // 入队
    let redis_start = Instant::now();
    let stream = state.settings.get_stream_name();
    let task_json = serde_json::to_string(&task)
        .map_err(|e| {
            error!(trace_id = %trace_id, error = %e, "序列化任务失败");
            (StatusCode::INTERNAL_SERVER_ERROR, format!("Failed to serialize task: {}", e))
        })?;

    match state.redis.xadd(&stream, &task_json).await {
        Ok(_id) => {
            let redis_duration = redis_start.elapsed();
            OBS_METRICS.record_redis_operation("xadd", "success", redis_duration);
            
            if let Ok(len) = state.redis.xlen(&stream).await {
                OBS_METRICS.update_queue_depth(&stream, len as i64);
            }
            
            OBS_METRICS.record_task_enqueued("api1");
            info!(trace_id = %trace_id, task_id = %id, stream = %stream, "api1 任务已入队");
        }
        Err(e) => {
            let redis_duration = redis_start.elapsed();
            OBS_METRICS.record_redis_operation("xadd", "error", redis_duration);
            error!(trace_id = %trace_id, task_id = %id, error = %e, "api1 入队错误");
            OBS_METRICS.record_http_request("POST", "api1", 500, start_time.elapsed());
            return Err((StatusCode::INTERNAL_SERVER_ERROR, format!("enqueue error: {}", e)));
        }
    }

    let total_duration = start_time.elapsed();
    OBS_METRICS.record_http_request("POST", "api1", 200, total_duration);
    info!(trace_id = %trace_id, task_id = %id, duration_ms = total_duration.as_millis(), "api1 请求处理完成");
    
    Ok((
        StatusCode::OK,
        Json(SubmitResponse { status: "ok".into(), message: "enqueued".into() }),
    ))
}

/// /metrics 暴露 Prometheus 文本
async fn handle_metrics() -> (StatusCode, String) {
    (StatusCode::OK, OBS_METRICS.gather())
}

#[derive(Debug, Deserialize)]
struct PeekParams {
    #[serde(default = "default_peek_count")]
    count: usize,
}

fn default_peek_count() -> usize { 20 }

#[derive(Debug, Serialize)]
struct PeekEntry {
    id: String,
    data: String,
}

#[derive(Debug, Serialize)]
struct PeekResponse {
    stream: String,
    count: usize,
    entries: Vec<PeekEntry>,
}

/// 只读查看最新 N 条 stream 数据（不会 ACK / 改 offset）
async fn handle_stream_peek(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PeekParams>,
) -> Result<Json<PeekResponse>, (StatusCode, String)> {
    let count = params.count.clamp(1, 200);
    let stream = state.settings.get_stream_name();

    let entries = state.redis.xrange_latest(&stream, count)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("xrange failed: {}", e)))?;

    let entries = entries.into_iter()
        .map(|(id, data)| PeekEntry { id, data })
        .collect::<Vec<_>>();

    Ok(Json(PeekResponse { stream, count, entries }))
}

#[derive(Debug, Deserialize)]
struct PendingParams {
    #[serde(default = "default_pending_count")]
    count: usize,
}

fn default_pending_count() -> usize { 50 }

#[derive(Debug, Serialize)]
struct PendingEntry {
    id: String,
    consumer: String,
    idle_ms: i64,
    delivered: i64,
}

#[derive(Debug, Serialize)]
struct PendingResponse {
    stream: String,
    group: String,
    count: usize,
    entries: Vec<PendingEntry>,
}

/// 查看 pending 队列（只读，不 ACK）
async fn handle_stream_pending(
    State(state): State<Arc<AppState>>,
    Query(params): Query<PendingParams>,
) -> Result<Json<PendingResponse>, (StatusCode, String)> {
    let count = params.count.clamp(1, 200);
    let stream = state.settings.get_stream_name();
    let group = state.settings.get_consumer_group();

    let entries = state.redis
        .xpending_range(&stream, &group, "-", "+", count)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("xpending failed: {}", e)))?;

    let entries = entries.into_iter()
        .map(|(id, consumer, idle, delivered)| PendingEntry { id, consumer, idle_ms: idle, delivered })
        .collect::<Vec<_>>();

    Ok(Json(PendingResponse { stream, group, count, entries }))
}
