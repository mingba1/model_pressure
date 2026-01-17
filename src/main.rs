use std::{net::SocketAddr, sync::Arc};
use once_cell::sync::OnceCell;
use reqwest::Client;
use tracing::{error, info, warn};
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};
use tracing_subscriber::layer::Layer;
use tracing_appender::{non_blocking, rolling};

mod config;
mod redis_client;
mod worker;
// database 模块保留文件但此处不再使用
mod observability;
mod web;

use config::Settings;
use redis_client::RedisClient;
use observability::METRICS as OBS_METRICS;
use web::{AppState, build_router};

// 保留文件日志写入线程的 guard，避免被提前 drop
static FILE_LOG_GUARD: OnceCell<tracing_appender::non_blocking::WorkerGuard> = OnceCell::new();

/// 初始化日志系统
fn init_logging(settings: &Settings) -> anyhow::Result<()> {
    use tracing_subscriber::fmt::time::ChronoUtc;
    
    // 获取日志配置
    let log_cfg = settings.get_log_config();
    
    // 确定日志级别
    // 将级别转成拥有所有权的 String，避免返回临时引用
    let log_level = log_cfg
        .as_ref()
        .and_then(|c| c.level.clone())
        .or_else(|| std::env::var("RUST_LOG").ok())
        .unwrap_or_else(|| "info".to_string());
    
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(log_level.as_str()));
    
    // 构建 subscriber
    let registry = tracing_subscriber::registry().with(filter);
    
    // 控制台输出（使用 Option 包装，保持类型一致）
    let console_layer = log_cfg
        .as_ref()
        .map(|c| c.console.unwrap_or(true))
        .unwrap_or(true)
        .then(|| {
            fmt::layer()
                .with_target(false)
                .with_timer(ChronoUtc::rfc_3339())
        });
    
    // 文件输出（如果配置了）
    let file_layer = if let Some(log_cfg) = log_cfg.as_ref() {
        if let Some(file_path) = &log_cfg.file {
            // 确保日志目录存在
            if let Some(parent) = std::path::Path::new(file_path).parent() {
                std::fs::create_dir_all(parent)?;
            }
            
            let max_file_size = log_cfg.max_file_size_mb.unwrap_or(100) * 1024 * 1024; // 转换为字节
            let max_files = log_cfg.max_files.unwrap_or(10);
            let _ = (max_file_size, max_files); // 预留，当前实现未使用滚动大小
            
            // 创建日志文件 appender（按日期滚动）
            // rolling::daily(directory, filename_prefix)
            // 例如：logs/app.log -> directory: logs, filename_prefix: app
            let directory = std::path::Path::new(file_path).parent().unwrap_or(std::path::Path::new("."));
            let filename_prefix = std::path::Path::new(file_path)
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("app");

            let file_appender = rolling::daily(directory, filename_prefix);
            
            let (non_blocking_appender, guard) = non_blocking(file_appender);
            let _ = FILE_LOG_GUARD.set(guard);
            
            Some(if log_cfg.json_format.unwrap_or(false) {
                fmt::layer()
                    .with_writer(non_blocking_appender)
                    .with_timer(ChronoUtc::rfc_3339())
                    .json()
                    .boxed()
            } else {
                fmt::layer()
                    .with_writer(non_blocking_appender)
                    .with_timer(ChronoUtc::rfc_3339())
                    .boxed()
            })
        } else {
            None
        }
    } else {
        None
    };
    
    registry
        .with(console_layer)
        .with(file_layer)
        .init();
    
    // 现在可以使用日志了
    if let Some(log_cfg) = log_cfg {
        if log_cfg.file.is_some() {
            tracing::info!("日志文件输出已启用: {}", log_cfg.file.as_ref().unwrap());
        }
    }
    
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 先加载配置以获取日志配置（不验证，因为可能日志配置有问题）
    let settings_temp = Settings::from_yaml_path("config.yaml")
        .map_err(|e| anyhow::anyhow!("Failed to load config for logging setup: {}", e))?;
    
    // 初始化日志系统
    init_logging(&settings_temp)?;
    
    info!("日志系统已初始化");

    // 使用已加载的配置（避免重复加载）
    let settings = settings_temp;
    
    info!("Loaded config for env: {}", settings.current_env);
    
    // 验证配置
    if let Err(e) = settings.validate() {
        error!("Config validation failed: {}", e);
        return Err(anyhow::anyhow!("Invalid configuration: {}", e));
    }
    
    // 打印日志配置信息
    if let Some(log_cfg) = settings.get_log_config() {
        info!("日志配置:");
        info!("  级别: {}", log_cfg.level.as_deref().unwrap_or("info"));
        info!("  控制台输出: {}", log_cfg.console.unwrap_or(true));
        if let Some(file) = &log_cfg.file {
            info!("  文件输出: {}", file);
            info!("  最大文件大小: {} MB", log_cfg.max_file_size_mb.unwrap_or(100));
            info!("  保留文件数: {}", log_cfg.max_files.unwrap_or(10));
            info!("  JSON 格式: {}", log_cfg.json_format.unwrap_or(false));
        } else {
            info!("  文件输出: 未配置");
        }
    }
    
    // 打印 Redis 配置信息（隐藏密码）
    if let Some(redis_cfg) = settings.get_redis_config() {
        let redis_url = settings.build_redis_url();
        // 隐藏密码的 URL（用于日志显示）
        let safe_url = if redis_url.contains("@") {
            let parts: Vec<&str> = redis_url.split("@").collect();
            if parts.len() == 2 {
                let auth_part = parts[0];
                if auth_part.contains(":") {
                    let user_pass: Vec<&str> = auth_part.split(":").collect();
                    if user_pass.len() >= 3 {
                        format!("redis://{}:***@{}", user_pass[0], parts[1])
                    } else {
                        format!("redis://:***@{}", parts[1])
                    }
                } else {
                    format!("redis://:***@{}", parts[1])
                }
            } else {
                redis_url.clone()
            }
        } else {
            redis_url.clone()
        };
        
        info!("========================================");
        info!("Redis 配置信息:");
        info!("  Host: {}", redis_cfg.host.as_ref().unwrap_or(&"N/A".to_string()));
        info!("  Port: {}", redis_cfg.port.unwrap_or(0));
        info!("  DB: {}", redis_cfg.db.unwrap_or(0));
        info!("  Username: {}", redis_cfg.username.as_ref().map(|s| s.as_str()).unwrap_or("(none)"));
        info!("  Password: {}", if redis_cfg.password.is_some() { "***" } else { "(none)" });
        info!("  Connection URL: {}", safe_url);
        info!("========================================");
    } else {
        warn!("Redis配置未找到，当前环境: {}", settings.current_env);
    }
    
    // 打印 Stream 和 Consumer 配置
    info!("Stream 配置:");
    info!("  Stream Name: {}", settings.get_stream_name());
    info!("  Consumer Group: {}", settings.get_consumer_group());
    info!("  Consumer Name: {}", settings.get_consumer_name());
    info!("  Visibility Timeout: {} 秒", settings.get_visibility_timeout());
    
    // 打印下游服务配置
    if let Some(downstream) = &settings.downstream_url {
        info!("下游服务配置:");
        info!("  Downstream URL: {}", downstream);
    }
    
    // 打印 MySQL 配置信息（如果配置了）
    if let Some(mysql_cfg) = settings.get_mysql_config() {
        info!("MySQL 配置:");
        info!("  Host: {}", mysql_cfg.host.as_ref().unwrap_or(&"N/A".to_string()));
        info!("  Port: {}", mysql_cfg.port.unwrap_or(3306));
        info!("  Database: {}", mysql_cfg.database.as_ref().unwrap_or(&"N/A".to_string()));
        info!("  User: {}", mysql_cfg.user.as_ref().unwrap_or(&"N/A".to_string()));
        info!("  Pool Size: {}", mysql_cfg.pool_size.unwrap_or(10));
        info!("  Max Overflow: {}", mysql_cfg.max_overflow.unwrap_or(5));
    } else {
        info!("MySQL 配置: 未配置");
    }

    // redis client
    info!("正在初始化 Redis 客户端...");
    let redis = match RedisClient::new(&settings).await {
        Ok(client) => {
            info!("Redis 客户端初始化成功");
            client
        }
        Err(e) => {
            error!("Redis 客户端初始化失败: {}", e);
            return Err(e);
        }
    };

    // 优化 HTTP 客户端配置，提高并发处理能力
    let http_client = Client::builder()
        .pool_max_idle_per_host(20)  // 每个主机最大空闲连接数
        .pool_idle_timeout(std::time::Duration::from_secs(30))  // 连接空闲超时
        .timeout(std::time::Duration::from_secs(10))  // 请求超时
        .build()
        .expect("Failed to build HTTP client");

    let shared_state = Arc::new(AppState {
        settings,
        redis: Arc::new(redis),
        http_client,
    });

    // spawn background workers to consume queue and forward to downstream
    // 优化：启动多个 Worker 以提升并发处理能力
    let worker_count = std::env::var("WORKER_COUNT")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(8);  // 默认 8 个 Worker（从4个增加到8个，提高并发处理能力）
    
    info!("启动 {} 个 Worker 实例", worker_count);
    OBS_METRICS.update_active_workers(worker_count as i64);
    for i in 0..worker_count {
        let s = shared_state.clone();
        let worker_id = format!("worker-{}", i + 1);
        tokio::spawn(async move {
            info!(worker_id = %worker_id, "Worker 启动");
            worker::run_worker(s).await;
        });
    }

    // build routes
    let app = build_router(shared_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 63370));
    info!("Starting server on {}", addr);
    axum::Server::bind(&addr)
        .serve(app.into_make_service())
        .await
        .unwrap();

    Ok(())
}

