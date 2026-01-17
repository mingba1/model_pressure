use crate::web::AppState;
use crate::observability::METRICS as OBS_METRICS;
use std::sync::Arc;
use std::time::Instant as StdInstant;
use tokio::time::{sleep, Duration, Instant};
use tracing::{error, info, warn};

const MAX_ATTEMPTS: i64 = 3;
const PENDING_CLAIM_BATCH: usize = 100;

/// 转发任务到下游服务
/// 返回是否成功
/// 优先使用任务数据中的 downstream_url，如果没有则使用配置中的下游地址
async fn forward_task(
    data: &str,
    task_id: &str,
    default_downstream: Option<&String>,
    http_client: &reqwest::Client,
) -> bool {
    let forward_start = StdInstant::now();
    
    // 尝试从任务数据中解析 downstream_url
    let task_downstream = serde_json::from_str::<serde_json::Value>(data)
        .ok()
        .and_then(|v| v.get("downstream_url").and_then(|u| u.as_str()).map(|s| s.to_string()));
    
    // 优先使用任务数据中的 downstream_url，否则使用配置中的
    let downstream_url = task_downstream.as_ref().or(default_downstream);
    
    if let Some(url) = downstream_url {
        // 从任务数据中提取 payload，如果存在则发送 payload，否则发送整个任务数据
        let body = match serde_json::from_str::<serde_json::Value>(data) {
            Ok(jsonv) => {
                // 如果有 payload 字段，只发送 payload；否则发送整个任务数据
                if let Some(payload) = jsonv.get("payload") {
                    serde_json::to_string(payload).unwrap_or_else(|_| data.to_string())
                } else {
                    data.to_string()
                }
            }
            Err(_) => data.to_string(),
        };
        
        match http_client
            .post(url)
            .json(&serde_json::json!(serde_json::from_str::<serde_json::Value>(&body).unwrap_or_else(|_| serde_json::json!({}))))
            .header("content-type", "application/json")
            .send()
            .await
        {
            Ok(r) => {
                let success = r.status().is_success();
                let duration = forward_start.elapsed();
                if success {
                    OBS_METRICS.record_downstream_call(url, "success", duration);
                    info!(task_id = %task_id, url = %url, duration_ms = duration.as_millis(), "成功转发任务到下游服务");
                } else {
                    OBS_METRICS.record_downstream_call(url, "failure", duration);
                    warn!(task_id = %task_id, url = %url, status = %r.status(), duration_ms = duration.as_millis(), "下游服务返回非成功状态");
                }
                success
            }
            Err(e) => {
                let duration = forward_start.elapsed();
                OBS_METRICS.record_downstream_call(url, "failure", duration);
                warn!(task_id = %task_id, url = %url, error = %e, duration_ms = duration.as_millis(), "转发任务到下游服务失败");
                false
            }
        }
    } else {
        // No downstream configured, consider as success
        warn!(task_id = %task_id, "未配置下游服务");
        true
    }
}

pub async fn run_worker(state: Arc<AppState>) {
    OBS_METRICS.worker_active.inc();
    info!("worker: starting Streams consumer (XREADGROUP)");

    let stream = state.settings.get_stream_name();
    let group = state.settings.get_consumer_group();
    let consumer = state.settings.get_consumer_name();
    let visibility = state.settings.get_visibility_timeout();
    let downstream = state.settings.downstream_url.clone();

    // 从项目根目录读取脚本文件
    let script = match std::fs::read_to_string("scripts/token_bucket.lua") {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to read token_bucket.lua: {}", e);
            // 使用默认的 token bucket 脚本
            "local key = KEYS[1]\nlocal capacity = tonumber(ARGV[1])\nlocal refill_rate = tonumber(ARGV[2])\nlocal now = tonumber(ARGV[3])\nlocal cost = tonumber(ARGV[4] or 1)\n\nlocal bucket = redis.call('hgetall', key)\nlocal tokens = capacity\nlocal last_refill = now\n\nif #bucket > 0 then\n    tokens = tonumber(bucket[2]) or capacity\n    last_refill = tonumber(bucket[4]) or now\nend\n\nlocal refill_amount = (now - last_refill) / 1000 * refill_rate\ntokens = math.min(capacity, tokens + refill_amount)\n\nif tokens >= cost then\n    tokens = tokens - cost\n    redis.call('hmset', key, 'tokens', tokens, 'last_refill', now)\n    return 1\nelse\n    redis.call('hmset', key, 'tokens', tokens, 'last_refill', now)\n    return 0\nend" .to_string()
        }
    };

    // ensure consumer group exists
    {
        let _ = state.redis.xgroup_create(&stream, &group).await;
    }

    let mut last_pending_check = Instant::now();

    loop {
        // 先读取一个任务（不阻塞），检查任务类型以确定使用哪个限流器
        let read_start = Instant::now();
        match state.redis.xreadgroup(&group, &consumer, &stream, 1, 100).await {
            Ok(Some(entries)) if !entries.is_empty() => {
                let read_duration = read_start.elapsed().as_secs_f64();
                OBS_METRICS.redis_operations_total.with_label_values(&["xreadgroup"]).inc();
                OBS_METRICS.redis_operation_duration_seconds.with_label_values(&["xreadgroup"]).observe(read_duration);
                
                // 获取第一个任务来检查限流配置
                let (_first_id, first_data) = &entries[0];
                
                // 解析任务数据，获取限流配置
                let rate_limit_key = serde_json::from_str::<serde_json::Value>(first_data)
                    .ok()
                    .and_then(|v| v.get("rate_limit_key").and_then(|k| k.as_str().map(|s| s.to_string())))
                    .unwrap_or_else(|| "default".to_string());
                
                // 根据 rate_limit_key 确定限流参数
                // 优化：大幅提高 smart_match1 的限流值以达到 200 QPS 目标
                let (capacity, refill_rate) = match rate_limit_key.as_str() {
                    "smart_match1" => (200, 200),  // 200 QPS for smart_match1 (从 50 提升到 200)
                    _ => (100, 100),  // 默认 100 QPS
                };
                
                // distributed token-bucket check with task-specific key
                let bucket_key = format!("rate:bucket:{}", rate_limit_key);
                let now_ms = chrono::Utc::now().timestamp_millis().to_string();
                let allowed = match state.redis
                    .eval_int(&script, &[&bucket_key], &[&capacity.to_string(), &refill_rate.to_string(), &now_ms, "1"]) 
                    .await
                {
                    Ok(v) => v,
                    Err(e) => { warn!("worker: token bucket eval error: {}", e); 0 }
                };

                if allowed == 0 {
                    // 限流：立即重入队并 ACK，避免卡在 pending
                    for (id, data) in entries {
                        if let Err(e) = state.redis.xadd(&stream, &data).await {
                            warn!(task_id = %id, error = %e, "限流重入队失败");
                        }
                        if let Err(e) = state.redis.xack(&stream, &group, &id).await {
                            warn!(task_id = %id, error = %e, "限流 ACK 失败");
                        }
                    }
                    OBS_METRICS.rate_limit_hits_total
                        .with_label_values(&["smart_match1", "smart_match1"])
                        .inc();
                    sleep(Duration::from_millis(100)).await;
                    continue;
                }
                
                // 限流通过，处理所有读取到的任务
                for (id, data) in entries {
                    process_task(&state, &stream, &group, &id, &data, downstream.as_ref(), &state.http_client).await;
                }
            }
            Ok(Some(_entries)) => {
                // 空结果，继续循环
            }
            Ok(None) => {
                // timeout，继续循环
            }
            Err(e) => {
                OBS_METRICS.redis_errors_total.inc();
                OBS_METRICS.worker_errors_total
                    .with_label_values(&["xreadgroup_error"])
                    .inc();
                
                let err_str = format!("{}", e);
                if err_str.contains("10054") || err_str.contains("连接") || err_str.contains("connection") 
                    || err_str.contains("closed") || err_str.contains("timeout") {
                    warn!("worker: xreadgroup connection lost, will retry: {}", e);
                    sleep(Duration::from_millis(500)).await;
                } else {
                    error!("worker: xreadgroup error: {}", e);
                    sleep(Duration::from_secs(1)).await;
                }
            }
        }
        
        // 检查 pending 任务
        if last_pending_check.elapsed() > Duration::from_secs(30) {
            last_pending_check = Instant::now();
            if let Err(e) = check_and_claim_pending(&state, &stream, &group, &consumer, visibility).await {
                warn!("worker: pending claim check failed: {}", e);
            }
        }
    }
}

/// 处理单个任务
async fn process_task(
    state: &Arc<AppState>,
    stream: &str,
    group: &str,
    id: &str,
    data: &str,
    default_downstream: Option<&String>,
    http_client: &reqwest::Client,
) {
    let process_start = Instant::now();
    let span = tracing::span!(tracing::Level::INFO, "process_task", task_id = %id);
    let _enter = span.enter();
    
    info!("worker: processing task id={}", id);

    let mut attempts = 0i64;
    let task_name = "unknown";
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(data) {
        attempts = v.get("attempts").and_then(|a| a.as_i64()).unwrap_or(0);
        if let Some(name) = v.get("task_name").and_then(|n| n.as_str()) {
            let _ = span.record("task_name", name);
        }
    }

    OBS_METRICS.tasks_dequeued_total
        .with_label_values(&[task_name])
        .inc();

    // 使用提取的转发函数
    let forward_start = Instant::now();
    let success = forward_task(
        data,
        id,
        default_downstream,
        http_client,
    ).await;
    let forward_duration = forward_start.elapsed().as_secs_f64();
    
    if let Some(downstream) = default_downstream {
        OBS_METRICS.task_forward_duration_seconds
            .with_label_values(&[downstream])
            .observe(forward_duration);
    }

    let processing_duration = process_start.elapsed().as_secs_f64();
    
    if success {
        OBS_METRICS.tasks_processed_total
            .with_label_values(&[task_name])
            .inc();
        OBS_METRICS.task_processing_duration_seconds
            .with_label_values(&[task_name])
            .observe(processing_duration);
        
        let ack_start = Instant::now();
        if let Err(e) = state.redis.xack(stream, group, id).await {
            OBS_METRICS.redis_errors_total.inc();
            warn!("worker: failed to acknowledge task {}: {}", id, e);
        } else {
            let ack_duration = ack_start.elapsed().as_secs_f64();
            OBS_METRICS.redis_operations_total.with_label_values(&["xack"]).inc();
            OBS_METRICS.redis_operation_duration_seconds.with_label_values(&["xack"]).observe(ack_duration);
            info!("worker: successfully processed task {}", id);
        }
    } else {
        attempts += 1;
        OBS_METRICS.tasks_failed_total
            .with_label_values(&[task_name, "forward_failed"])
            .inc();
        
        if attempts >= MAX_ATTEMPTS {
            OBS_METRICS.tasks_dlq_total
                .with_label_values(&[task_name])
                .inc();
            
            let dlq = format!("{}:dlq", stream);
            match state.redis.xadd_dlq(&dlq, data).await {
                Ok(_) => {
                    OBS_METRICS.redis_operations_total.with_label_values(&["xadd_dlq"]).inc();
                    if let Err(e) = state.redis.xack(stream, group, id).await {
                        warn!("worker: moved to DLQ but failed to acknowledge task {}: {}", id, e);
                    } else {
                        warn!("worker: moved id={} to DLQ after {} attempts", id, attempts);
                    }
                }
                Err(e) => {
                    OBS_METRICS.redis_errors_total.inc();
                    warn!("worker: failed to move task {} to DLQ: {}", id, e);
                    // Still acknowledge to avoid reprocessing
                    let _ = state.redis.xack(stream, group, id).await;
                }
            }
        } else {
            OBS_METRICS.tasks_retried_total
                .with_label_values(&[task_name, &attempts.to_string()])
                .inc();
            match serde_json::from_str::<serde_json::Value>(data) {
                Ok(mut v) => {
                    v["attempts"] = serde_json::Value::Number(serde_json::Number::from(attempts));
                    // 使用 JSON 序列化
                    match serde_json::to_string(&v) {
                        Ok(newp) => {
                            match state.redis.xadd(stream, &newp).await {
                                Ok(_) => {
                                    if let Err(e) = state.redis.xack(stream, group, id).await {
                                        warn!("worker: requeued but failed to acknowledge task {}: {}", id, e);
                                    } else {
                                        info!("worker: requeued task {} for retry #{}", id, attempts);
                                    }
                                }
                                Err(e) => {
                                    warn!("worker: failed to requeue task {}: {}", id, e);
                                    // Still acknowledge to avoid reprocessing
                                    let _ = state.redis.xack(stream, group, id).await;
                                }
                            }
                        }
                        Err(e) => {
                            warn!("worker: failed to serialize updated task data: {} for id={}", e, id);
                            // Still acknowledge to avoid reprocessing
                            let _ = state.redis.xack(stream, group, id).await;
                        }
                    }
                }
                Err(e) => {
                    warn!("worker: failed to parse task data for retry: {} for id={}", e, id);
                    // Still acknowledge to avoid reprocessing
                    let _ = state.redis.xack(stream, group, id).await;
                }
            }
        }
    }
}

async fn check_and_claim_pending(state: &Arc<AppState>, stream: &str, group: &str, consumer_name: &str, visibility_secs: u64) -> anyhow::Result<()> {
    let pending = state.redis.xpending_range(stream, group, "-", "+", PENDING_CLAIM_BATCH).await?;
    let mut to_claim: Vec<String> = Vec::new();
    for (id, _consumer, idle, _delivered) in pending {
        if idle as u64 > visibility_secs * 1000u64 { to_claim.push(id); }
    }

    if to_claim.is_empty() { return Ok(()); }

    let ids_refs: Vec<&str> = to_claim.iter().map(|s| s.as_str()).collect();
    if let Ok(Some(entries)) = state.redis.xclaim(stream, group, consumer_name, (visibility_secs * 1000) as usize, &ids_refs).await {
        let downstream = state.settings.downstream_url.as_ref();
        for (id, data) in entries {
            process_task(state, stream, group, &id, &data, downstream, &state.http_client).await;
        }
    }

    Ok(())
}
