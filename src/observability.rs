use prometheus::{
    Encoder, IntCounter, IntCounterVec, IntGauge, HistogramVec,
    HistogramOpts, Opts, Registry, TextEncoder,
};
use once_cell::sync::Lazy;
use std::time::Duration;

/// 全局指标集合
pub struct Metrics {
    pub registry: Registry,
    
    // HTTP 请求指标
    pub http_requests_total: IntCounterVec,
    pub http_request_duration_seconds: HistogramVec,
    pub http_request_size_bytes: HistogramVec,
    pub http_response_size_bytes: HistogramVec,
    
    // 任务队列指标
    pub tasks_enqueued_total: IntCounterVec,
    pub tasks_dequeued_total: IntCounterVec,
    pub tasks_processed_total: IntCounterVec,
    pub tasks_failed_total: IntCounterVec,
    pub tasks_retried_total: IntCounterVec,
    pub tasks_dlq_total: IntCounterVec,
    pub queue_depth: IntGauge,
    pub queue_pending: IntGauge,
    
    // 任务处理时间指标
    pub task_processing_duration_seconds: HistogramVec,
    pub task_forward_duration_seconds: HistogramVec,
    
    // Worker 指标
    pub worker_active: IntGauge,
    pub worker_errors_total: IntCounterVec,
    
    // 限流指标
    pub rate_limit_hits_total: IntCounterVec,
    
    // Redis 指标
    pub redis_operations_total: IntCounterVec,
    pub redis_operation_duration_seconds: HistogramVec,
    pub redis_errors_total: IntCounter,
    
    // 数据库指标
    pub database_queries_total: IntCounterVec,
    pub database_query_duration_seconds: HistogramVec,
    pub database_errors_total: IntCounter,
    
    // 下游服务指标
    pub downstream_requests_total: IntCounterVec,
    pub downstream_request_duration_seconds: HistogramVec,
    pub downstream_errors_total: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        
        // HTTP 请求指标
        let http_requests_total = IntCounterVec::new(
            Opts::new("http_requests_total", "Total number of HTTP requests")
                .namespace("gateway"),
            &["method", "path", "status_code"]
        ).unwrap();
        
        let http_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new("http_request_duration_seconds", "HTTP request duration in seconds")
                .namespace("gateway")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]),
            &["method", "path", "status_code"]
        ).unwrap();
        
        let http_request_size_bytes = HistogramVec::new(
            HistogramOpts::new("http_request_size_bytes", "HTTP request size in bytes")
                .namespace("gateway")
                .buckets(vec![100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0]),
            &["method", "path"]
        ).unwrap();
        
        let http_response_size_bytes = HistogramVec::new(
            HistogramOpts::new("http_response_size_bytes", "HTTP response size in bytes")
                .namespace("gateway")
                .buckets(vec![100.0, 500.0, 1000.0, 5000.0, 10000.0, 50000.0, 100000.0]),
            &["method", "path", "status_code"]
        ).unwrap();
        
        // 任务队列指标
        let tasks_enqueued_total = IntCounterVec::new(
            Opts::new("tasks_enqueued_total", "Total number of tasks enqueued")
                .namespace("gateway"),
            &["task_name", "kind"]
        ).unwrap();
        
        let tasks_dequeued_total = IntCounterVec::new(
            Opts::new("tasks_dequeued_total", "Total number of tasks dequeued")
                .namespace("gateway"),
            &["task_name"]
        ).unwrap();
        
        let tasks_processed_total = IntCounterVec::new(
            Opts::new("tasks_processed_total", "Total number of tasks processed successfully")
                .namespace("gateway"),
            &["task_name"]
        ).unwrap();
        
        let tasks_failed_total = IntCounterVec::new(
            Opts::new("tasks_failed_total", "Total number of tasks failed")
                .namespace("gateway"),
            &["task_name", "error_type"]
        ).unwrap();
        
        let tasks_retried_total = IntCounterVec::new(
            Opts::new("tasks_retried_total", "Total number of tasks retried")
                .namespace("gateway"),
            &["task_name", "attempt"]
        ).unwrap();
        
        let tasks_dlq_total = IntCounterVec::new(
            Opts::new("tasks_dlq_total", "Total number of tasks moved to DLQ")
                .namespace("gateway"),
            &["task_name"]
        ).unwrap();
        
        let queue_depth = IntGauge::new(
            "queue_depth", 
            "Current queue depth"
        ).unwrap();
        
        let queue_pending = IntGauge::new(
            "queue_pending",
            "Current pending queue size"
        ).unwrap();
        
        // 任务处理时间指标
        let task_processing_duration_seconds = HistogramVec::new(
            HistogramOpts::new("task_processing_duration_seconds", "Task processing duration in seconds")
                .namespace("gateway")
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
            &["task_name"]
        ).unwrap();
        
        let task_forward_duration_seconds = HistogramVec::new(
            HistogramOpts::new("task_forward_duration_seconds", "Task forward duration in seconds")
                .namespace("gateway")
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0]),
            &["downstream_url"]
        ).unwrap();
        
        // Worker 指标
        let worker_active = IntGauge::new(
            "worker_active",
            "Number of active workers"
        ).unwrap();
        
        let worker_errors_total = IntCounterVec::new(
            Opts::new("worker_errors_total", "Total number of worker errors")
                .namespace("gateway"),
            &["error_type"]
        ).unwrap();
        
        // 限流指标
        let rate_limit_hits_total = IntCounterVec::new(
            Opts::new("rate_limit_hits_total", "Total number of rate limit hits")
                .namespace("gateway"),
            &["endpoint", "rate_limit_key"]
        ).unwrap();
        
        // Redis 指标
        let redis_operations_total = IntCounterVec::new(
            Opts::new("redis_operations_total", "Total number of Redis operations")
                .namespace("gateway"),
            &["operation"]
        ).unwrap();
        
        let redis_operation_duration_seconds = HistogramVec::new(
            HistogramOpts::new("redis_operation_duration_seconds", "Redis operation duration in seconds")
                .namespace("gateway")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
            &["operation"]
        ).unwrap();
        
        let redis_errors_total = IntCounter::new(
            "redis_errors_total",
            "Total number of Redis errors"
        ).unwrap();
        
        // 数据库指标
        let database_queries_total = IntCounterVec::new(
            Opts::new("database_queries_total", "Total number of database queries")
                .namespace("gateway"),
            &["query_type"]
        ).unwrap();
        
        let database_query_duration_seconds = HistogramVec::new(
            HistogramOpts::new("database_query_duration_seconds", "Database query duration in seconds")
                .namespace("gateway")
                .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]),
            &["query_type"]
        ).unwrap();
        
        let database_errors_total = IntCounter::new(
            "database_errors_total",
            "Total number of database errors"
        ).unwrap();
        
        // 下游服务指标
        let downstream_requests_total = IntCounterVec::new(
            Opts::new("downstream_requests_total", "Total number of downstream requests")
                .namespace("gateway"),
            &["downstream_url", "status_code"]
        ).unwrap();
        
        let downstream_request_duration_seconds = HistogramVec::new(
            HistogramOpts::new("downstream_request_duration_seconds", "Downstream request duration in seconds")
                .namespace("gateway")
                .buckets(vec![0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]),
            &["downstream_url"]
        ).unwrap();
        
        let downstream_errors_total = IntCounterVec::new(
            Opts::new("downstream_errors_total", "Total number of downstream errors")
                .namespace("gateway"),
            &["downstream_url", "error_type"]
        ).unwrap();
        
        // 注册所有指标
        registry.register(Box::new(http_requests_total.clone())).unwrap();
        registry.register(Box::new(http_request_duration_seconds.clone())).unwrap();
        registry.register(Box::new(http_request_size_bytes.clone())).unwrap();
        registry.register(Box::new(http_response_size_bytes.clone())).unwrap();
        registry.register(Box::new(tasks_enqueued_total.clone())).unwrap();
        registry.register(Box::new(tasks_dequeued_total.clone())).unwrap();
        registry.register(Box::new(tasks_processed_total.clone())).unwrap();
        registry.register(Box::new(tasks_failed_total.clone())).unwrap();
        registry.register(Box::new(tasks_retried_total.clone())).unwrap();
        registry.register(Box::new(tasks_dlq_total.clone())).unwrap();
        registry.register(Box::new(queue_depth.clone())).unwrap();
        registry.register(Box::new(queue_pending.clone())).unwrap();
        registry.register(Box::new(task_processing_duration_seconds.clone())).unwrap();
        registry.register(Box::new(task_forward_duration_seconds.clone())).unwrap();
        registry.register(Box::new(worker_active.clone())).unwrap();
        registry.register(Box::new(worker_errors_total.clone())).unwrap();
        registry.register(Box::new(rate_limit_hits_total.clone())).unwrap();
        registry.register(Box::new(redis_operations_total.clone())).unwrap();
        registry.register(Box::new(redis_operation_duration_seconds.clone())).unwrap();
        registry.register(Box::new(redis_errors_total.clone())).unwrap();
        registry.register(Box::new(database_queries_total.clone())).unwrap();
        registry.register(Box::new(database_query_duration_seconds.clone())).unwrap();
        registry.register(Box::new(database_errors_total.clone())).unwrap();
        registry.register(Box::new(downstream_requests_total.clone())).unwrap();
        registry.register(Box::new(downstream_request_duration_seconds.clone())).unwrap();
        registry.register(Box::new(downstream_errors_total.clone())).unwrap();
        
        Self {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            http_request_size_bytes,
            http_response_size_bytes,
            tasks_enqueued_total,
            tasks_dequeued_total,
            tasks_processed_total,
            tasks_failed_total,
            tasks_retried_total,
            tasks_dlq_total,
            queue_depth,
            queue_pending,
            task_processing_duration_seconds,
            task_forward_duration_seconds,
            worker_active,
            worker_errors_total,
            rate_limit_hits_total,
            redis_operations_total,
            redis_operation_duration_seconds,
            redis_errors_total,
            database_queries_total,
            database_query_duration_seconds,
            database_errors_total,
            downstream_requests_total,
            downstream_request_duration_seconds,
            downstream_errors_total,
        }
    }
    
    /// 导出 Prometheus 格式的指标
    pub fn gather(&self) -> String {
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap_or_default()
    }
    
    /// 记录 HTTP 请求指标
    pub fn record_http_request(&self, method: &str, path: &str, status_code: u16, duration: Duration) {
        let status_str = status_code.to_string();
        self.http_requests_total
            .with_label_values(&[method, path, &status_str])
            .inc();
        self.http_request_duration_seconds
            .with_label_values(&[method, path, &status_str])
            .observe(duration.as_secs_f64());
    }
    
    /// 记录限流拒绝
    pub fn record_rate_limit_rejected(&self, endpoint: &str) {
        self.rate_limit_hits_total
            .with_label_values(&[endpoint, endpoint])
            .inc();
    }
    
    /// 记录 Redis 操作
    pub fn record_redis_operation(&self, operation: &str, status: &str, duration: Duration) {
        self.redis_operations_total
            .with_label_values(&[operation])
            .inc();
        self.redis_operation_duration_seconds
            .with_label_values(&[operation])
            .observe(duration.as_secs_f64());
        if status.eq_ignore_ascii_case("error") {
            self.redis_errors_total.inc();
        }
    }
    
    /// 更新队列深度
    pub fn update_queue_depth(&self, _stream: &str, depth: i64) {
        self.queue_depth.set(depth);
        self.queue_pending.set(depth);
    }
    
    /// 记录任务入队
    pub fn record_task_enqueued(&self, task_name: &str) {
        self.tasks_enqueued_total
            .with_label_values(&[task_name, "default"])
            .inc();
    }
    
    /// 记录下游调用
    pub fn record_downstream_call(&self, downstream_url: &str, status: &str, duration: Duration) {
        self.downstream_requests_total
            .with_label_values(&[downstream_url, status])
            .inc();
        self.downstream_request_duration_seconds
            .with_label_values(&[downstream_url])
            .observe(duration.as_secs_f64());
        if status.eq_ignore_ascii_case("failure") {
            self.downstream_errors_total
                .with_label_values(&[downstream_url, "failure"])
                .inc();
        }
    }
    
    /// 更新活跃 worker 数量
    pub fn update_active_workers(&self, count: i64) {
        self.worker_active.set(count);
    }
}

/// 全局指标实例
pub static METRICS: Lazy<Metrics> = Lazy::new(|| Metrics::new());

/// 用于追踪请求的辅助结构
pub struct RequestMetrics {
    pub start_time: std::time::Instant,
    pub method: String,
    pub path: String,
}

impl RequestMetrics {
    pub fn new(method: String, path: String) -> Self {
        Self {
            start_time: std::time::Instant::now(),
            method,
            path,
        }
    }
    
    pub fn record(&self, status_code: u16, request_size: usize, response_size: usize) {
        let duration = self.start_time.elapsed().as_secs_f64();
        let status_str = status_code.to_string();
        
        METRICS.http_requests_total
            .with_label_values(&[&self.method, &self.path, &status_str])
            .inc();
        
        METRICS.http_request_duration_seconds
            .with_label_values(&[&self.method, &self.path, &status_str])
            .observe(duration);
        
        METRICS.http_request_size_bytes
            .with_label_values(&[&self.method, &self.path])
            .observe(request_size as f64);
        
        METRICS.http_response_size_bytes
            .with_label_values(&[&self.method, &self.path, &status_str])
            .observe(response_size as f64);
    }
}
