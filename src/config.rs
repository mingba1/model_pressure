use serde::{Deserialize, Serialize};
use std::fs;
use urlencoding;
use uuid;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RedisConfig {
    pub url: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub db: Option<u32>,
    pub username: Option<String>,
    pub password: Option<String>,
    /// 连接池最大大小（默认：deadpool-redis 默认值，通常是 10）
    pub pool_max_size: Option<usize>,
    /// 连接池最小大小（默认：0）
    pub pool_min_size: Option<usize>,
    /// 获取连接的超时时间（秒，默认：30）
    pub pool_timeout: Option<u64>,
}

impl RedisConfig {
    /// 构建Redis连接URL
    pub fn build_url(&self) -> anyhow::Result<String> {
        // 如果提供了完整URL，直接使用
        if let Some(url) = &self.url {
            if !url.trim().is_empty() {
                return Ok(url.clone());
            }
        }

        // 否则从各字段构建URL
        let host = self.host.as_ref().ok_or_else(|| anyhow::anyhow!("Redis host is required"))?;
        let port = self.port.ok_or_else(|| anyhow::anyhow!("Redis port is required"))?;
        let db = self.db.unwrap_or(0);
        
        match (self.username.as_ref(), self.password.as_ref()) {
            (Some(user), Some(pass)) if !user.trim().is_empty() => {
                Ok(format!("redis://{}:{}@{}:{}/{}?protocol=resp3", 
                    urlencoding::encode(user), 
                    urlencoding::encode(pass), 
                    host, 
                    port, 
                    db))
            },
            (_, Some(pass)) => {
                Ok(format!("redis://:{}@{}:{}/{}?protocol=resp3", 
                    urlencoding::encode(pass), 
                    host, 
                    port, 
                    db))
            },
            _ => {
                Ok(format!("redis://{}:{}/{}?protocol=resp3", 
                    host, 
                    port, 
                    db))
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: Option<String>,
    pub port: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Settings {
    pub current_env: String,
    pub server: Option<ServerConfig>,
    pub dev: Option<EnvConfig>,
    pub test: Option<EnvConfig>,
    pub prod: Option<EnvConfig>,

    // derived
    pub downstream_url: Option<String>,
    // visibility timeout in seconds for processing list
    pub visibility_timeout_secs: Option<u64>,
    // stream / consumer group settings
    pub stream_name: Option<String>,
    pub consumer_group: Option<String>,
    pub consumer_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogConfig {
    /// 日志级别 (trace, debug, info, warn, error)
    pub level: Option<String>,
    /// 是否输出到控制台 (默认: true)
    pub console: Option<bool>,
    /// 日志文件路径 (如果设置，将输出到文件)
    pub file: Option<String>,
    /// 日志文件最大大小 (MB, 默认: 100)
    pub max_file_size_mb: Option<u64>,
    /// 保留的日志文件数量 (默认: 10)
    pub max_files: Option<usize>,
    /// 是否使用 JSON 格式 (默认: false)
    pub json_format: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MysqlConfig {
    pub url: Option<String>,
    pub host: Option<String>,
    pub port: Option<u16>,
    pub database: Option<String>,
    pub user: Option<String>,
    pub password: Option<String>,
    pub charset: Option<String>,
    /// 连接池大小
    pub pool_size: Option<u32>,
    /// 最大溢出连接数
    pub max_overflow: Option<u32>,
    /// 连接超时时间（秒）
    pub connect_timeout: Option<u64>,
    /// 空闲连接超时时间（秒）
    pub idle_timeout: Option<u64>,
}

impl MysqlConfig {
    /// 构建 MySQL 连接 URL
    pub fn build_url(&self) -> anyhow::Result<String> {
        if let Some(url) = &self.url {
            if !url.trim().is_empty() {
                return Ok(url.clone());
            }
        }

        let host = self.host.as_ref().ok_or_else(|| anyhow::anyhow!("MySQL host is required"))?;
        let port = self.port.unwrap_or(3306);
        let database = self.database.as_ref().ok_or_else(|| anyhow::anyhow!("MySQL database is required"))?;
        let user = self.user.as_ref().ok_or_else(|| anyhow::anyhow!("MySQL user is required"))?;
        let password = self.password.as_ref().ok_or_else(|| anyhow::anyhow!("MySQL password is required"))?;
        let charset = self.charset.as_deref().unwrap_or("utf8mb4");

        Ok(format!("mysql://{}:{}@{}:{}/{}?charset={}", 
            urlencoding::encode(user),
            urlencoding::encode(password),
            host,
            port,
            database,
            charset
        ))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EnvConfig {
    pub debug: Option<bool>,
    pub mysql: Option<MysqlConfig>,
    pub redis: Option<RedisConfig>,
    pub callback_url: Option<String>,
    pub log: Option<LogConfig>,
}

impl Settings {
    pub fn from_yaml_path(path: &str) -> anyhow::Result<Self> {
        let s = fs::read_to_string(path)?;
        let mut cfg: Settings = serde_yaml::from_str(&s)?;
        // set derived fields
        let env_cfg = match cfg.current_env.as_str() {
            "dev" => cfg.dev.clone(),
            "test" => cfg.test.clone(),
            "prod" => cfg.prod.clone(),
            _ => cfg.test.clone(),
        };

        cfg.downstream_url = env_cfg.and_then(|e| e.callback_url);
        Ok(cfg)
    }

    /// 获取当前环境的Redis配置
    pub fn get_redis_config(&self) -> Option<&RedisConfig> {
        if self.current_env == "local" {
            return None;
        }

        match self.current_env.as_str() {
            "dev" => self.dev.as_ref()?.redis.as_ref(),
            "test" => self.test.as_ref()?.redis.as_ref(),
            "prod" => self.prod.as_ref()?.redis.as_ref(),
            _ => self.test.as_ref()?.redis.as_ref(),
        }
    }

    pub fn build_redis_url(&self) -> String {
        // 如果当前环境是"local"，直接返回空字符串
        if self.current_env == "local" {
            return "".to_string();
        }

        // 尝试从当前环境获取Redis配置并构建URL
        if let Some(redis_cfg) = self.get_redis_config() {
            if let Ok(url) = redis_cfg.build_url() {
                return url;
            }
        }

        "".to_string()
    }

    pub fn get_queue_key(&self) -> String {
        // derive queue/key name (similar to task prefix configuration)
        // fallback to "hybird_tasks"
        "hybird_tasks".to_string()
    }

    pub fn get_processing_queue_key(&self) -> String {
        format!("{}:processing", self.get_queue_key())
    }

    pub fn get_visibility_timeout(&self) -> u64 {
        self.visibility_timeout_secs.unwrap_or(3600)
    }

    pub fn get_stream_name(&self) -> String {
        self.stream_name.clone().unwrap_or_else(|| format!("{}:stream", self.get_queue_key()))
    }

    pub fn get_consumer_group(&self) -> String {
        self.consumer_group.clone().unwrap_or_else(|| "gateway_group".to_string())
    }

    pub fn get_consumer_name(&self) -> String {
        self.consumer_name.clone().unwrap_or_else(|| format!("consumer-{}", uuid::Uuid::new_v4()))
    }

    /// 获取当前环境的日志配置
    pub fn get_log_config(&self) -> Option<&LogConfig> {
        match self.current_env.as_str() {
            "dev" => self.dev.as_ref()?.log.as_ref(),
            "test" => self.test.as_ref()?.log.as_ref(),
            "prod" => self.prod.as_ref()?.log.as_ref(),
            _ => None,
        }
    }

    /// 获取当前环境的 MySQL 配置
    pub fn get_mysql_config(&self) -> Option<&MysqlConfig> {
        match self.current_env.as_str() {
            "dev" => self.dev.as_ref()?.mysql.as_ref(),
            "test" => self.test.as_ref()?.mysql.as_ref(),
            "prod" => self.prod.as_ref()?.mysql.as_ref(),
            _ => None,
        }
    }

    /// 验证配置的有效性
    pub fn validate(&self) -> anyhow::Result<()> {
        // 验证环境名称
        if !matches!(self.current_env.as_str(), "dev" | "test" | "prod" | "local") {
            return Err(anyhow::anyhow!("Invalid current_env: {}. Must be one of: dev, test, prod, local", self.current_env));
        }

        // 如果不是 local 环境，必须配置 Redis
        if self.current_env != "local" {
            if self.get_redis_config().is_none() {
                return Err(anyhow::anyhow!("Redis config is required for environment: {}", self.current_env));
            }
        }

        Ok(())
    }
}
