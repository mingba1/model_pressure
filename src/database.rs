use crate::config::Settings;
use sqlx::{MySqlPool, Pool, MySql};
use tracing::{error, info};

/// MySQL 数据库客户端
pub struct DatabaseClient {
    pool: Pool<MySql>,
}

impl DatabaseClient {
    /// 从配置创建数据库客户端
    pub async fn new(settings: &Settings) -> anyhow::Result<Option<Self>> {
        let mysql_cfg = match settings.get_mysql_config() {
            Some(cfg) => cfg,
            None => {
                info!("MySQL 配置未找到，跳过数据库初始化");
                return Ok(None);
            }
        };

        let url = mysql_cfg.build_url()?;
        
        // 隐藏密码的 URL（用于日志显示）
        let safe_url = if url.contains("@") {
            let parts: Vec<&str> = url.split("@").collect();
            if parts.len() == 2 {
                let auth_part = parts[0];
                if auth_part.contains("://") && auth_part.contains(":") {
                    let protocol_user: Vec<&str> = auth_part.split("://").collect();
                    if protocol_user.len() == 2 {
                        let user_pass: Vec<&str> = protocol_user[1].split(":").collect();
                        if user_pass.len() == 2 {
                            format!("{}://{}:***@{}", protocol_user[0], user_pass[0], parts[1])
                        } else {
                            format!("{}://:***@{}", protocol_user[0], parts[1])
                        }
                    } else {
                        format!("mysql://:***@{}", parts[1])
                    }
                } else {
                    format!("mysql://:***@{}", parts[1])
                }
            } else {
                "mysql://:***@***".to_string()
            }
        } else {
            "mysql://***".to_string()
        };

        info!("正在连接 MySQL: {}", safe_url);

        // 创建连接池
        let pool = match MySqlPool::connect(&url).await {
            Ok(p) => {
                info!("MySQL 连接池创建成功");
                p
            }
            Err(e) => {
                error!("MySQL 连接失败: {}", e);
                return Err(anyhow::anyhow!("Failed to connect to MySQL: {}", e));
            }
        };

        // 测试连接
        match sqlx::query("SELECT 1").execute(&pool).await {
            Ok(_) => {
                info!("MySQL 连接测试成功");
            }
            Err(e) => {
                error!("MySQL 连接测试失败: {}", e);
                return Err(anyhow::anyhow!("MySQL connection test failed: {}", e));
            }
        }

        Ok(Some(Self { pool }))
    }

    /// 获取连接池
    pub fn pool(&self) -> &Pool<MySql> {
        &self.pool
    }

    /// 执行查询（示例：查询任务状态）
    pub async fn query_task_status(&self, task_id: &str) -> anyhow::Result<Option<serde_json::Value>> {
        // 示例查询，根据实际表结构调整
        let result = sqlx::query_scalar::<_, Option<String>>(
            "SELECT status FROM tasks WHERE id = ?"
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await?;

        match result {
            Some(Some(status)) => Ok(Some(serde_json::json!({"status": status}))),
            Some(None) => Ok(None),
            None => Ok(None),
        }
    }

    /// 插入任务记录（示例）
    pub async fn insert_task(&self, task_id: &str, task_name: &str, payload: &serde_json::Value) -> anyhow::Result<()> {
        // 示例插入，根据实际表结构调整
        let payload_str = serde_json::to_string(payload)?;
        
        sqlx::query(
            "INSERT INTO tasks (id, task_name, payload, created_at) VALUES (?, ?, ?, NOW())"
        )
        .bind(task_id)
        .bind(task_name)
        .bind(payload_str)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// 更新任务状态（示例）
    pub async fn update_task_status(&self, task_id: &str, status: &str) -> anyhow::Result<()> {
        sqlx::query(
            "UPDATE tasks SET status = ?, updated_at = NOW() WHERE id = ?"
        )
        .bind(status)
        .bind(task_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// 健康检查
    pub async fn check_health(&self) -> anyhow::Result<serde_json::Value> {
        let result = sqlx::query_scalar::<_, i64>("SELECT 1")
            .fetch_one(&self.pool)
            .await?;

        Ok(serde_json::json!({
            "status": "healthy",
            "ping": result
        }))
    }
}
