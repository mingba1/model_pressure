# rust_gateway

Minimal Rust gateway that:
- Loads configuration from `config.yaml`
- Exposes `/submit` endpoint (rate-limited to 200 QPS)
- Enqueues tasks into Redis Streams queue
- Optionally forwards task to downstream HTTP callback URL
- Exposes `/health` endpoint for Redis health

Quick start:

```bash
# from project root
cargo build --release
RUST_LOG=info cargo run --release
```

API:
- POST /addData
  - JSON body: 任意 JSON 对象（会作为 payload 传递到下游）
  - Rate limit: 200 QPS (接口级别)
  - 下游限流: 10 QPS (使用 Redis token bucket)
  - 下游地址: http://192.168.9.123:63374/smart_match1
- GET /health
- GET /metrics

Notes:
- This project uses Redis Streams (XADD/XREADGROUP) as the queue backend. The current design is intentionally simple and interoperable via shared Redis keys or HTTP callbacks.

Streams + token-bucket + metrics:
- This repo now supports Redis Streams (XADD / XREADGROUP) as the primary queue, a distributed token-bucket implemented via `scripts/token_bucket.lua` (EVAL), and Prometheus metrics at `/metrics`.

Quick docker (Redis + bridge):

```bash
docker-compose up --build
```

Run the Rust gateway locally with:

```bash
cargo run
```

PlantUML diagram: `diagrams/gateway.puml`


## 启停脚本

### Windows
```cmd
# 启动（开发模式）
scripts\start.bat dev

# 启动（发布模式，推荐用于生产环境）
scripts\start.bat release

# 停止
scripts\stop.bat
```

### PowerShell
```powershell
# 启动（开发模式）
.\scripts\run.ps1 dev

# 启动（发布模式）
.\scripts\run.ps1 release

# 停止
.\scripts\stop.ps1
```

## 性能测试

### 快速开始

1. **启动服务器（发布模式）**
   ```powershell
   .\scripts\run.ps1 release
   ```

2. **运行性能测试**
   
   PowerShell 版本：
   ```powershell
   # 基本测试（10000 请求，100 并发）
   .\scripts\benchmark.ps1
   
   # 高并发测试（50000 请求，500 并发）
   .\scripts\benchmark.ps1 -Concurrency 500 -TotalRequests 50000
   ```
   
   Python 版本（推荐，更高效）：
   ```bash
   pip install aiohttp
   python scripts/benchmark.py 100 10000
   ```


详细说明请参考 `scripts/README.md`# model_pressure
