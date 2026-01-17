use prometheus::{
    Encoder, TextEncoder, Registry, IntCounter, IntGauge, Histogram, HistogramOpts,
    Counter, CounterVec, GaugeVec, HistogramVec, Opts,
};
use once_cell::sync::Lazy;
use std::time::Duration;

/// 全局指标注册表
pub static METRICS: Lazy<ObservabilityMetrics> = Lazy::new(|| {
    let registry = Registry::new();
    
    // === 任务相关指标 ===
    
    // 任务入队总数（按任务类型）
    let tasks_enqueued = CounterVec::new(
        Opts::new("gateway_tasks_enqueued_total", "Total number of tasks enqueued")
            .namespace("gateway"),
        &["task_name"]
    ).unwrap();
    
    // 任务处理总数（按任务类型和状态）
    let tasks_processed = CounterVec::new(
        Opts::new("gateway_tasks_processed_total", "Total number of tasks processed")
            .namespace("gateway"),
        &["task_name", "status"] // status: success, failure, retry, dlq
    ).unwrap();
    
    // 任务处理延迟（按任务类型）
    let task_duration = HistogramVec::new(
        HistogramOpts::new("gateway_task_duration_seconds", "Task processing duration in seconds")
            .namespace("gateway")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
        &["task_name", "stage"] // stage: total, forward, redis
    ).unwrap();
    
    // 任务重试次数
    let task_retries = CounterVec::new(
        Opts::new("gateway_task_retries_total", "Total number of task retries")
            .namespace("gateway"),
        &["task_name"]
    ).unwrap();
    
    // === 队列相关指标 ===
    
    // 队列深度（当前待处理任务数）
    let queue_depth = GaugeVec::new(
        Opts::new("gateway_queue_depth", "Current queue depth")
            .namespace("gateway"),
        &["stream_name"]
    ).unwrap();
    
    // Pending 任务数（超时未确认的任务）
    let pending_tasks = GaugeVec::new(
        Opts::new("gateway_pending_tasks", "Number of pending tasks")
            .namespace("gateway"),
        &["stream_name"]
    ).unwrap();
    
    // === HTTP 请求相关指标 ===
    
    // HTTP 请求总数（按端点和方法）
    let http_requests = CounterVec::new(
        Opts::new("gateway_http_requests_total", "Total number of HTTP requests")
            .namespace("gateway"),
        &["method", "endpoint", "status_code"]
    ).unwrap();
    
    // HTTP 请求延迟
    let http_duration = HistogramVec::new(
        HistogramOpts::new("gateway_http_duration_seconds", "HTTP request duration in seconds")
            .namespace("gateway")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
        &["method", "endpoint"]
    ).unwrap();
    
    // 限流拒绝的请求数
    let rate_limit_rejected = CounterVec::new(
        Opts::new("gateway_rate_limit_rejected_total", "Total number of rate-limited requests")
            .namespace("gateway"),
        &["endpoint"]
    ).unwrap();
    
    // === Redis 相关指标 ===
    
    // Redis 操作总数（按操作类型）
    let redis_operations = CounterVec::new(
        Opts::new("gateway_redis_operations_total", "Total number of Redis operations")
            .namespace("gateway"),
        &["operation", "status"] // operation: xadd, xreadgroup, xack, etc.
    ).unwrap();
    
    // Redis 操作延迟
    let redis_duration = HistogramVec::new(
        HistogramOpts::new("gateway_redis_duration_seconds", "Redis operation duration in seconds")
            .namespace("gateway")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
        &["operation"]
    ).unwrap();
    
    // === 下游服务相关指标 ===
    
    // 下游服务调用总数（按状态）
    let downstream_calls = CounterVec::new(
        Opts::new("gateway_downstream_calls_total", "Total number of downstream service calls")
            .namespace("gateway"),
        &["status"] // status: success, failure, timeout
    ).unwrap();
    
    // 下游服务调用延迟
    let downstream_duration = Histogram::with_opts(
        HistogramOpts::new("gateway_downstream_duration_seconds", "Downstream service call duration in seconds")
            .namespace("gateway")
            .buckets(vec![0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0])
    ).unwrap();
    
    // === Worker 相关指标 ===
    
    // 活跃 Worker 数量
    let active_workers = IntGauge::new("gateway_active_workers", "Number of active workers")
        .unwrap();
    
    // Worker 处理的任务数
    let worker_tasks = CounterVec::new(
        Opts::new("gateway_worker_tasks_total", "Total number of tasks processed by workers")
            .namespace("gateway"),
        &["worker_id"]
    ).unwrap();
    
    // === 系统资源指标 ===
    
    // 内存使用（如果可用）
    // 注意：需要额外的依赖来获取系统指标
    
    // 注册所有指标
    registry.register(Box::new(tasks_enqueued.clone())).unwrap();
    registry.register(Box::new(tasks_processed.clone())).unwrap();
    registry.register(Box::new(task_duration.clone())).unwrap();
    registry.register(Box::new(task_retries.clone())).unwrap();
    registry.register(Box::new(queue_depth.clone())).unwrap();
    registry.register(Box::new(pending_tasks.clone())).unwrap();
    registry.register(Box::new(http_requests.clone())).unwrap();
    registry.register(Box::new(http_duration.clone())).unwrap();
    registry.register(Box::new(rate_limit_rejected.clone())).unwrap();
    registry.register(Box::new(redis_operations.clone())).unwrap();
    registry.register(Box::new(redis_duration.clone())).unwrap();
    registry.register(Box::new(downstream_calls.clone())).unwrap();
    registry.register(Box::new(downstream_duration.clone())).unwrap();
    registry.register(Box::new(active_workers.clone())).unwrap();
    registry.register(Box::new(worker_tasks.clone())).unwrap();
    
    ObservabilityMetrics {
        registry,
        tasks_enqueued,
        tasks_processed,
        task_duration,
        task_retries,
        queue_depth,
        pending_tasks,
        http_requests,
        http_duration,
        rate_limit_rejected,
        redis_operations,
        redis_duration,
        downstream_calls,
        downstream_duration,
        active_workers,
        worker_tasks,
    }
});

/// 可观测性指标集合
pub struct ObservabilityMetrics {
    pub registry: Registry,
    
    // 任务指标
    pub tasks_enqueued: CounterVec,
    pub tasks_processed: CounterVec,
    pub task_duration: HistogramVec,
    pub task_retries: CounterVec,
    
    // 队列指标
    pub queue_depth: GaugeVec,
    pub pending_tasks: GaugeVec,
    
    // HTTP 指标
    pub http_requests: CounterVec,
    pub http_duration: HistogramVec,
    pub rate_limit_rejected: CounterVec,
    
    // Redis 指标
    pub redis_operations: CounterVec,
    pub redis_duration: HistogramVec,
    
    // 下游服务指标
    pub downstream_calls: CounterVec,
    pub downstream_duration: Histogram,
    
    // Worker 指标
    pub active_workers: IntGauge,
    pub worker_tasks: CounterVec,
}

impl ObservabilityMetrics {
    /// 导出 Prometheus 格式的指标
    pub fn export(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap_or_default()
    }
    
    /// 记录任务入队
    pub fn record_task_enqueued(&self, task_name: &str) {
        self.tasks_enqueued.with_label_values(&[task_name]).inc();
    }
    
    /// 记录任务处理完成
    pub fn record_task_processed(&self, task_name: &str, status: &str, duration: Duration) {
        self.tasks_processed.with_label_values(&[task_name, status]).inc();
        self.task_duration
            .with_label_values(&[task_name, "total"])
            .observe(duration.as_secs_f64());
    }
    
    /// 记录任务重试
    pub fn record_task_retry(&self, task_name: &str) {
        self.task_retries.with_label_values(&[task_name]).inc();
    }
    
    /// 更新队列深度
    pub fn update_queue_depth(&self, stream_name: &str, depth: i64) {
        self.queue_depth.with_label_values(&[stream_name]).set(depth as f64);
    }
    
    /// 更新 Pending 任务数
    pub fn update_pending_tasks(&self, stream_name: &str, count: i64) {
        self.pending_tasks.with_label_values(&[stream_name]).set(count as f64);
    }
    
    /// 记录 HTTP 请求
    pub fn record_http_request(&self, method: &str, endpoint: &str, status_code: u16, duration: Duration) {
        let status = status_code.to_string();
        self.http_requests.with_label_values(&[method, endpoint, &status]).inc();
        self.http_duration
            .with_label_values(&[method, endpoint])
            .observe(duration.as_secs_f64());
    }
    
    /// 记录限流拒绝
    pub fn record_rate_limit_rejected(&self, endpoint: &str) {
        self.rate_limit_rejected.with_label_values(&[endpoint]).inc();
    }
    
    /// 记录 Redis 操作
    pub fn record_redis_operation(&self, operation: &str, status: &str, duration: Duration) {
        self.redis_operations.with_label_values(&[operation, status]).inc();
        self.redis_duration
            .with_label_values(&[operation])
            .observe(duration.as_secs_f64());
    }
    
    /// 记录下游服务调用
    pub fn record_downstream_call(&self, status: &str, duration: Duration) {
        self.downstream_calls.with_label_values(&[status]).inc();
        self.downstream_duration.observe(duration.as_secs_f64());
    }
    
    /// 更新活跃 Worker 数量
    pub fn update_active_workers(&self, count: i64) {
        self.active_workers.set(count);
    }
    
    /// 记录 Worker 处理的任务
    pub fn record_worker_task(&self, worker_id: &str) {
        self.worker_tasks.with_label_values(&[worker_id]).inc();
    }
}
