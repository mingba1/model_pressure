## 架构图（Mermaid）

```mermaid
flowchart LR
  Client --> LB[Load Balancer]
  LB --> GW[Gateway (Rust)\n/submit, /smart_match1\nRate limit -> XADD]
  GW -->|XADD| Redis[Redis Streams\nXADD/XREADGROUP\ntoken-bucket state]
  GW -->|202 + task_id| Client

  Workers[Worker Pool (Rust)\nXREADGROUP\nLua token-bucket\nHTTP forward] --> Redis
  Workers --> Down[Downstream Service (HTTP)]
  Down --> DB[(MySQL / Storage)]

  note left of Redis
    - Monitor XLEN / XPENDING
    - Streams consumer groups
  end note
```

说明：
- 网关：Axum/Tokio，入口 `/submit`、`/smart_match1`，入口级限流，写入 Redis Streams。
- 队列：Redis Streams + token bucket Lua，支持 XREADGROUP 消费。
- Worker：并发消费，按任务携带或配置的 downstream_url 进行 HTTP 转发，失败重试与 DLQ。
- 下游：HTTP 服务，可选持久化 MySQL/Storage。
- 观测：Prometheus `/metrics`，健康检查 `/health`。
