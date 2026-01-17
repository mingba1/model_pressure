use crate::config::{RedisConfig, Settings};
use deadpool_redis::{Config, Pool, Runtime};
use deadpool_redis::redis::AsyncCommands as DeadpoolAsyncCommands;
use std::sync::Arc;
use tokio::sync::Mutex as TokioMutex;

pub struct RedisClient {
    pub pool: Pool,
    // 专门用于阻塞操作的持久连接（XREADGROUP 需要长时间阻塞）
    // redis 0.24 使用 Connection 而不是 ConnectionManager
    blocking_conn: Arc<TokioMutex<Option<redis::aio::Connection>>>,
    client: redis::Client,
}

impl std::fmt::Debug for RedisClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisClient")
            .field("pool", &"deadpool_redis::Pool")
            .finish()
    }
}

impl RedisClient {
    /// 从Settings创建Redis客户端
    pub async fn new(settings: &Settings) -> anyhow::Result<Self> {
        // 获取当前环境的Redis配置
        let redis_config = settings.get_redis_config().ok_or_else(|| anyhow::anyhow!("Redis config not found for current environment"))?;
        Self::from_config(redis_config).await
    }
    
    /// 直接从RedisConfig创建Redis客户端
    pub async fn from_config(config: &RedisConfig) -> anyhow::Result<Self> {
        use tracing::{info, warn};
        
        // 构建Redis连接URL
        let url = config.build_url()?;
        
        // 打印连接信息（隐藏密码）
        let safe_url = if url.contains("@") {
            let parts: Vec<&str> = url.split("@").collect();
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
                url.clone()
            }
        } else {
            url.clone()
        };
        
        info!("正在连接 Redis: {}", safe_url);
        
        let client = match redis::Client::open(url.as_str()) {
            Ok(c) => {
                info!("Redis Client 创建成功");
                c
            }
            Err(e) => {
                warn!("Redis Client 创建失败: {}", e);
                return Err(anyhow::anyhow!("Failed to create Redis client: {}", e));
            }
        };
        
        // 创建连接池
        info!("正在创建 Redis 连接池...");
        let mut cfg = Config::from_url(url.clone());
        
        // 打印从 config.yaml 读取的配置
        info!("从 config.yaml 读取的连接池配置:");
        if let Some(max_size) = config.pool_max_size {
            info!("  pool_max_size: {} (已配置)", max_size);
        } else {
            info!("  pool_max_size: 未配置，将使用默认值");
        }
        
        if let Some(min_size) = config.pool_min_size {
            info!("  pool_min_size: {} (已配置)", min_size);
        } else {
            info!("  pool_min_size: 未配置");
        }
        
        if let Some(timeout) = config.pool_timeout {
            info!("  pool_timeout: {} 秒 (已配置)", timeout);
        } else {
            info!("  pool_timeout: 未配置，将使用默认值");
        }
        
        // 应用自定义连接池配置（如果有）
        // deadpool-redis 0.14 中，pool 是 Option<PoolConfig>
        // PoolConfig 只有 max_size、timeouts 和 queue_mode 字段
        if let Some(max_size) = config.pool_max_size {
            if cfg.pool.is_none() {
                cfg.pool = Some(deadpool_redis::PoolConfig::default());
            }
            if let Some(ref mut pool_cfg) = cfg.pool {
                pool_cfg.max_size = max_size;
            }
        }
        
        // deadpool-redis 0.14 不支持 min_idle，但记录配置值用于参考
        if let Some(min_size) = config.pool_min_size {
            warn!("  pool_min_size={} 已配置，但 deadpool-redis 0.14 不支持 min_idle，该配置将被忽略", min_size);
            warn!("  提示：连接池会在需要时自动创建连接，无需预先维护最小连接数");
        }
        
        if let Some(timeout) = config.pool_timeout {
            if cfg.pool.is_none() {
                cfg.pool = Some(deadpool_redis::PoolConfig::default());
            }
            if let Some(ref mut pool_cfg) = cfg.pool {
                pool_cfg.timeouts = deadpool_redis::Timeouts {
                    wait: Some(std::time::Duration::from_secs(timeout)),
                    create: None,
                    recycle: None,
                };
            }
        }
        
        // 打印实际应用的配置
        let final_max_size = cfg.pool.as_ref().map(|p| p.max_size).unwrap_or(10);
        let final_timeout = cfg.pool.as_ref()
            .and_then(|p| p.timeouts.wait)
            .map(|d: std::time::Duration| d.as_secs())
            .unwrap_or(30);
        
        info!("实际应用的连接池配置:");
        info!("  最大连接数 (max_size): {}", final_max_size);
        info!("  连接获取超时 (timeout): {} 秒", final_timeout);
        if config.pool_min_size.is_some() {
            info!("  最小连接数 (min_idle): 不支持（deadpool-redis 0.14 限制）");
        } else {
            info!("  最小连接数 (min_idle): 0 (默认，连接按需创建)");
        }
        
        let pool = match cfg.create_pool(Some(Runtime::Tokio1)) {
            Ok(p) => {
                info!("✅ Redis 连接池创建成功");
                p
            }
            Err(e) => {
                warn!("Redis 连接池创建失败: {}", e);
                return Err(anyhow::anyhow!("Failed to create Redis pool: {}", e));
            }
        };
        
        // 测试连接池
        info!("正在测试 Redis 连接...");
        let mut conn = match pool.get().await {
            Ok(c) => {
                info!("从连接池获取连接成功");
                c
            }
            Err(e) => {
                warn!("从连接池获取连接失败: {}", e);
                return Err(anyhow::anyhow!("Failed to get connection from pool: {}", e));
            }
        };
        
        match deadpool_redis::redis::cmd("PING").query_async::<_, String>(&mut *conn).await {
            Ok(pong) => {
                info!("Redis PING 测试成功: {}", pong);
            }
            Err(e) => {
                warn!("Redis PING 测试失败: {}", e);
                return Err(anyhow::anyhow!("Redis PING failed: {}", e));
            }
        }
        
        // 创建专门用于阻塞操作的持久连接
        // redis 0.24 使用 get_connection 而不是 get_connection_manager
        info!("正在创建阻塞连接...");
        let manager = match client.get_async_connection().await {
            Ok(m) => {
                info!("阻塞连接创建成功");
                m
            }
            Err(e) => {
                warn!("阻塞连接创建失败: {}", e);
                return Err(anyhow::anyhow!("Failed to create blocking connection: {}", e));
            }
        };
        let blocking_conn = Arc::new(TokioMutex::new(Some(manager)));
        
        info!("Redis 客户端初始化完成");
        Ok(Self { pool, blocking_conn, client })
    }

    /// 获取连接池中的连接
    async fn get_conn(&self) -> anyhow::Result<deadpool_redis::Connection> {
        self.pool.get().await.map_err(|e| anyhow::anyhow!("Failed to get connection from pool: {}", e))
    }

    pub async fn enqueue(&self, queue: &str, payload: String) -> anyhow::Result<()> {
        let mut conn = self.get_conn().await?;
        let _: () = conn.lpush(queue, payload).await?;
        Ok(())
    }

    pub async fn xadd(&self, stream: &str, payload: &str) -> anyhow::Result<String> {
        let mut conn = self.get_conn().await?;
        // use '*' id
        let id: String = deadpool_redis::redis::cmd("XADD").arg(stream).arg("*").arg("data").arg(payload).query_async(&mut *conn).await?;
        Ok(id)
    }

    pub async fn xreadgroup(&self, group: &str, consumer: &str, stream: &str, count: usize, block_ms: usize) -> anyhow::Result<Option<Vec<(String, String)>>> {
        // 使用持久连接进行阻塞操作，避免每次创建新连接导致超时
        let mut conn_guard = self.blocking_conn.lock().await;
        
        let mut need_reconnect = conn_guard.is_none();
        if !need_reconnect {
            // 测试现有连接是否可用
            if let Some(conn) = conn_guard.as_mut() {
                match deadpool_redis::redis::cmd("PING").query_async::<_, String>(conn).await {
                    Ok(_) => {
                        // 连接正常
                    }
                    Err(_) => {
                        // 连接已断开，需要重连
                        need_reconnect = true;
                        *conn_guard = None;
                    }
                }
            }
        }
        
        if need_reconnect {
            use tracing::{info, warn};
            info!("重新创建阻塞连接...");
            match self.client.get_async_connection().await {
                Ok(new_conn) => {
                    info!("阻塞连接重新创建成功");
                    *conn_guard = Some(new_conn);
                }
                Err(e) => {
                    warn!("阻塞连接重新创建失败: {}", e);
                    return Err(anyhow::anyhow!("Failed to reconnect blocking connection: {}", e));
                }
            }
        }
        
        let conn = conn_guard.as_mut().unwrap();
        
        // XREADGROUP GROUP group consumer COUNT count BLOCK block_ms STREAMS stream >
        let result = redis::cmd("XREADGROUP")
            .arg("GROUP").arg(group).arg(consumer)
            .arg("COUNT").arg(count)
            .arg("BLOCK").arg(block_ms)
            .arg("STREAMS").arg(stream)
            .arg(">")
            .query_async(conn)
            .await;
        
        // 如果连接错误，清空连接以便下次重连
        let res: Option<redis::Value> = match result {
            Ok(v) => v,
            Err(e) => {
                // 连接断开，清空连接以便下次自动重连
                // 错误可能是：网络中断、服务器超时、资源限制等
                *conn_guard = None;
                let err_msg = format!("{}", e);
                // 区分不同类型的连接错误
                if err_msg.contains("10054") || err_msg.contains("连接") || err_msg.contains("connection") || err_msg.contains("closed") {
                    return Err(anyhow::anyhow!("Redis connection closed by server (likely timeout or network issue): {}", e));
                }
                return Err(anyhow::anyhow!("xreadgroup error: {}", e));
            }
        };

        // parse value into vector of (id, data)
        if let Some(v) = res {
            // Complex parsing; provide a simple best-effort conversion
            let mut out = Vec::new();
            if let redis::Value::Bulk(streams) = v {
                for s in streams.into_iter() {
                    if let redis::Value::Bulk(items) = s {
                        // items[0]=stream name, items[1]=entries
                        if items.len() >= 2 {
                            if let redis::Value::Bulk(entries) = &items[1] {
                                for e in entries {
                                    if let redis::Value::Bulk(pair) = e {
                                        if pair.len() >= 2 {
                                            if let redis::Value::Data(idb) = &pair[0] {
                                                let id = String::from_utf8_lossy(idb).to_string();
                                                if let redis::Value::Bulk(kv) = &pair[1] {
                                                    // kv is [key, val]
                                                    if kv.len() >= 2 {
                                                        if let redis::Value::Data(valb) = &kv[1] {
                                                            let val = String::from_utf8_lossy(valb).to_string();
                                                            out.push((id, val));
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            return Ok(Some(out));
        }
        Ok(None)
    }

    pub async fn xack(&self, stream: &str, group: &str, id: &str) -> anyhow::Result<i64> {
        let mut conn = self.get_conn().await?;
        let n: i64 = deadpool_redis::redis::cmd("XACK").arg(stream).arg(group).arg(id).query_async(&mut *conn).await?;
        Ok(n)
    }

    pub async fn xlen(&self, stream: &str) -> anyhow::Result<i64> {
        let mut conn = self.get_conn().await?;
        let l: i64 = deadpool_redis::redis::cmd("XLEN").arg(stream).query_async(&mut *conn).await?;
        Ok(l)
    }

    pub async fn xgroup_create(&self, stream: &str, group: &str) -> anyhow::Result<()> {
        let mut conn = self.get_conn().await?;
        // XGROUP CREATE <stream> <group> $ MKSTREAM
        let res: deadpool_redis::redis::RedisResult<String> = deadpool_redis::redis::cmd("XGROUP").arg("CREATE").arg(stream).arg(group).arg("$").arg("MKSTREAM").query_async(&mut *conn).await;
        match res {
            Ok(_) => Ok(()),
            Err(e) => {
                // ignore BUSYGROUP error
                let s = format!("{}", e);
                if s.contains("BUSYGROUP") {
                    Ok(())
                } else {
                    Err(anyhow::anyhow!(e))
                }
            }
        }
    }

    pub async fn eval_int(&self, script: &str, keys: &[&str], args: &[&str]) -> anyhow::Result<i64> {
        let mut conn = self.get_conn().await?;
        let mut cmd = deadpool_redis::redis::cmd("EVAL");
        cmd.arg(script).arg(keys.len());
        for k in keys { cmd.arg(k); }
        for a in args { cmd.arg(a); }
        let v: i64 = cmd.query_async(&mut *conn).await?;
        Ok(v)
    }

    pub async fn xpending_range(&self, stream: &str, group: &str, start: &str, end: &str, count: usize) -> anyhow::Result<Vec<(String, String, i64, i64)>> {
        let mut conn = self.get_conn().await?;
        // XPENDING <stream> <group> <start> <end> <count>
        let res: deadpool_redis::redis::Value = deadpool_redis::redis::cmd("XPENDING").arg(stream).arg(group).arg(start).arg(end).arg(count).query_async(&mut *conn).await?;
        let mut out = Vec::new();
        if let deadpool_redis::redis::Value::Bulk(items) = res {
            for it in items {
                if let deadpool_redis::redis::Value::Bulk(entry) = it {
                    // [id, consumer, idle, delivered]
                    if entry.len() >= 4 {
                        let id = match &entry[0] { deadpool_redis::redis::Value::Data(b) => String::from_utf8_lossy(b).to_string(), _ => String::new() };
                        let consumer = match &entry[1] { deadpool_redis::redis::Value::Data(b) => String::from_utf8_lossy(b).to_string(), _ => String::new() };
                        let idle = match &entry[2] { deadpool_redis::redis::Value::Int(i) => *i as i64, deadpool_redis::redis::Value::Data(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap_or(0), _ => 0 };
                        let delivered = match &entry[3] { deadpool_redis::redis::Value::Int(i) => *i as i64, deadpool_redis::redis::Value::Data(b) => String::from_utf8_lossy(b).parse::<i64>().unwrap_or(0), _ => 0 };
                        out.push((id, consumer, idle, delivered));
                    }
                }
            }
        }
        Ok(out)
    }

    pub async fn xclaim(&self, stream: &str, group: &str, consumer: &str, min_idle_ms: usize, ids: &[&str]) -> anyhow::Result<Option<Vec<(String, String)>>> {
        let mut conn = self.get_conn().await?;
        let mut cmd = deadpool_redis::redis::cmd("XCLAIM");
        cmd.arg(stream).arg(group).arg(consumer).arg(min_idle_ms);
        for id in ids { cmd.arg(id); }
        // return full entries
        let res: deadpool_redis::redis::Value = cmd.query_async(&mut *conn).await?;
        if let deadpool_redis::redis::Value::Bulk(streams) = res {
            let mut out = Vec::new();
            for s in streams {
                if let deadpool_redis::redis::Value::Bulk(pair) = s {
                    if pair.len() >= 2 {
                        if let deadpool_redis::redis::Value::Data(idb) = &pair[0] {
                            let id = String::from_utf8_lossy(idb).to_string();
                            if let deadpool_redis::redis::Value::Bulk(kv) = &pair[1] {
                                // kv is [key,val,...] — we look for 'data' key
                                let mut val = String::new();
                                let mut i = 0;
                                while i + 1 < kv.len() {
                                    if let deadpool_redis::redis::Value::Data(kb) = &kv[i] {
                                        let key = String::from_utf8_lossy(kb).to_string();
                                        if key == "data" {
                                            if let deadpool_redis::redis::Value::Data(vb) = &kv[i+1] {
                                                val = String::from_utf8_lossy(vb).to_string();
                                                break;
                                            }
                                        }
                                    }
                                    i += 2;
                                }
                                out.push((id, val));
                            }
                        }
                    }
                }
            }
            return Ok(Some(out));
        }
        Ok(None)
    }

    pub async fn xadd_dlq(&self, stream: &str, payload: &str) -> anyhow::Result<String> {
        // Add to DLQ stream
        self.xadd(stream, payload).await
    }

    pub async fn brpop(&self, queue: &str, timeout_secs: usize) -> anyhow::Result<Option<String>> {
        let mut conn = self.get_conn().await?;
        // redis 0.24 中 brpop 的 timeout 参数是 f64 类型
        let res: Option<(String, String)> = conn.brpop(queue, timeout_secs as f64).await?;
        Ok(res.map(|(_k, v)| v))
    }

    pub async fn brpoplpush(&self, src: &str, dst: &str, timeout_secs: usize) -> anyhow::Result<Option<String>> {
        let mut conn = self.get_conn().await?;
        let res: Option<String> = deadpool_redis::redis::cmd("BRPOPLPUSH")
            .arg(src)
            .arg(dst)
            .arg(timeout_secs)
            .query_async(&mut *conn)
            .await?;
        Ok(res)
    }

    pub async fn llen(&self, queue: &str) -> anyhow::Result<usize> {
        let mut conn = self.get_conn().await?;
        let len: usize = conn.llen(queue).await?;
        Ok(len)
    }

    pub async fn check_health(&self) -> anyhow::Result<serde_json::Value> {
        let mut conn = self.get_conn().await?;
        let pong: String = deadpool_redis::redis::cmd("PING").query_async(&mut *conn).await?;
        let info: String = deadpool_redis::redis::cmd("INFO").query_async(&mut *conn).await?;
        Ok(serde_json::json!({"ping": pong, "info_len": info.len()}))
    }

    pub async fn lrem(&self, list: &str, count: isize, value: &str) -> anyhow::Result<i64> {
        let mut conn = self.get_conn().await?;
        let res: i64 = deadpool_redis::redis::cmd("LREM").arg(list).arg(count).arg(value).query_async(&mut *conn).await?;
        Ok(res)
    }

    pub async fn set_kv(&self, key: &str, val: &str) -> anyhow::Result<()> {
        let mut conn = self.get_conn().await?;
        let _: () = conn.set(key, val).await?;
        Ok(())
    }

    pub async fn get_kv(&self, key: &str) -> anyhow::Result<Option<String>> {
        let mut conn = self.get_conn().await?;
        let val: Option<String> = conn.get(key).await?;
        Ok(val)
    }

    pub async fn del_kv(&self, key: &str) -> anyhow::Result<i64> {
        let mut conn = self.get_conn().await?;
        let n: i64 = conn.del(key).await?;
        Ok(n)
    }

    pub async fn lrange(&self, list: &str, start: isize, stop: isize) -> anyhow::Result<Vec<String>> {
        let mut conn = self.get_conn().await?;
        let vals: Vec<String> = conn.lrange(list, start, stop).await?;
        Ok(vals)
    }

    /// XRANGE 查看最新 N 条（只读，不会 ACK）
    pub async fn xrange_latest(&self, stream: &str, count: usize) -> anyhow::Result<Vec<(String, String)>> {
        let mut conn = self.get_conn().await?;
        // 从最新向前取 count 条，使用 XREVRANGE 以避免全量
        let res: deadpool_redis::redis::Value = deadpool_redis::redis::cmd("XREVRANGE")
            .arg(stream)
            .arg("+").arg("-")
            .arg("COUNT").arg(count)
            .query_async(&mut *conn)
            .await?;

        let mut out = Vec::new();
        if let deadpool_redis::redis::Value::Bulk(entries) = res {
            for e in entries {
                if let deadpool_redis::redis::Value::Bulk(pair) = e {
                    if pair.len() >= 2 {
                        // pair[0]=id, pair[1]=fields
                        let id = match &pair[0] {
                            deadpool_redis::redis::Value::Data(b) => String::from_utf8_lossy(b).to_string(),
                            _ => String::new(),
                        };
                        let mut data = String::new();
                        if let deadpool_redis::redis::Value::Bulk(kv) = &pair[1] {
                            let mut i = 0;
                            while i + 1 < kv.len() {
                                if let deadpool_redis::redis::Value::Data(kb) = &kv[i] {
                                    let key = String::from_utf8_lossy(kb).to_string();
                                    if key == "data" {
                                        if let deadpool_redis::redis::Value::Data(vb) = &kv[i+1] {
                                            data = String::from_utf8_lossy(vb).to_string();
                                            break;
                                        }
                                    }
                                }
                                i += 2;
                            }
                        }
                        out.push((id, data));
                    }
                }
            }
        }
        Ok(out)
    }
}
