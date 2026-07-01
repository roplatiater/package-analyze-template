# DataFilter Pipeline Demo

本项目是本地可验证的模拟 DataFilter Pipeline：`fetch_raw -> unzip_package -> analyze_phrase_stats -> summarize_result`。API/周期触发器只创建 metadata，真正执行由 step worker 按 step_type 认领完成。

## 安装

```bash
/home/richard/usr/micromamba/envs/venv/bin/python -m pip install -r backend/requirements.txt
npm install
```

## 运行后端

```bash
PYTHONPATH=backend /home/richard/usr/micromamba/envs/venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000 --reload
```

后端数据目录：`backend/data/runs/<run_id>`、`backend/data/artifacts/raw_packages/...`、`backend/data/artifacts/raw/...`、`backend/data/artifacts/results/...`、`backend/data/tmp/...`。`manifest.json` 是 artifact 唯一完成标记。

主要后端设置可通过环境变量覆盖，包括 `DATA_ROOT`、`METADATA_STORE`（Phase 1 默认 `json`）、`SQLITE_PATH`、worker/heartbeat 间隔、自动 pipeline、in-process workers、通知机器人、CORS origins，以及预留的 `AUTH_ENABLED`/OIDC 配置占位。测试可安全覆盖 `settings.data_root` 使用隔离临时目录。

默认 demo 模式会在 FastAPI lifespan 中启动周期触发器和 in-process workers，后端启动后会自动产出结果。

## Worker 模式

默认：`INPROCESS_WORKERS_ENABLED=true`，后端内置 worker 自动执行。

真实 worker 模式：不要同时开两套 worker。先关闭 in-process worker：

```bash
INPROCESS_WORKERS_ENABLED=false PYTHONPATH=backend /home/richard/usr/micromamba/envs/venv/bin/python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

然后分别启动：

```bash
PYTHONPATH=backend /home/richard/usr/micromamba/envs/venv/bin/python -m app.worker fetch_raw
PYTHONPATH=backend /home/richard/usr/micromamba/envs/venv/bin/python -m app.worker unzip_package
PYTHONPATH=backend /home/richard/usr/micromamba/envs/venv/bin/python -m app.worker analyze_phrase_stats
PYTHONPATH=backend /home/richard/usr/micromamba/envs/venv/bin/python -m app.worker summarize_result
```

支持 `--once` 便于测试。默认 `METADATA_STORE=json` 时使用本地 JSON metadata 和 Linux `fcntl.flock` 文件锁，适合本机演示，不是分布式队列。

SQLite metadata/queue runtime is available with `METADATA_STORE=sqlite`. It uses `backend/app/repositories/sqlite_store.py` with WAL, schema, import helper, atomic queue APIs, guarded worker heartbeat/finish/fail writes, and manifest-aware dependency validation in the worker before downstream execution. SQLite 模式适合单机本地磁盘或 Docker volume，不适合 NFS/多主机队列。Default runtime remains JSON unless the env var is set.

升级注意：`fetch_raw` 现在产出 `raw_package/package.zip`，随后由 `unzip_package` 安全展开为 `raw/data.json`。请在没有 active/in-flight old runs 时升级；已完成旧结果仍可读取，已完成旧 raw 可被分析步骤兼容处理。

通知机器人默认启用（`NOTIFICATION_ROBOT_ENABLED=true`），会比较默认 source/dataset/analysis_type 的最新两次完成结果，并以确定性文件写入 `backend/data/outbox/notifications/<previous>__<newest>.json`。幂等状态保存在 `backend/data/index/notification_state.json`，最近 sent 历史保留 50 条。可用 `NOTIFICATION_ROBOT_INTERVAL_SECONDS` 调整轮询间隔。

添加新 DataFilter：新增 filter 实现 `build_cache_key(context)`、`run(context,tmp_dir)`、`build_manifest(context,output)`；在 `backend/app/filters/registry.py` 注册 step_type/filter/input/output/path；在 `backend/app/pipeline/templates.py` 更新 template；启动对应 `python -m app.worker <step_type>`。

## 运行前端

```bash
npm run dev
```

前端开发服务器会把 `/api` 代理到 `http://127.0.0.1:8000`。

## API

- `GET /api/health`
- `GET /api/health/ready`：检查 data root；SQLite 模式还检查 DB 连接
- `GET /api/metrics`：JSON 指标快照（metadata store、job/step status、queued/running 等）
- `GET /api/results`：返回已完成且 manifest 校验通过的结果列表
- `POST /api/jobs`：手动立即拉包分析，body 可为空；可传 `{ "params": { "simulate_fail_stage": "analyze", "delay_ms": 500, "force_refresh": false } }`
- `GET /api/jobs` / `GET /api/jobs/{run_id}`
- `POST /api/jobs/{run_id}/retry`：失败任务重试，并移除模拟失败参数，复用已成功 raw
- `GET /api/results/{run_id}`：成功后返回 `result.json`，未完成返回 409
- `GET /api/cache/status`

## 常用验证

```bash
/home/richard/usr/micromamba/envs/venv/bin/python -m compileall backend/app
PYTHONPATH=backend /home/richard/usr/micromamba/envs/venv/bin/python -m pytest
npm run build
```

Smoke 建议：启动后等待 scheduled job 产生结果；手动 POST 创建 manual job 并查看结果列表新增；创建 `simulate_fail_stage=analyze` 验证 failed；调用 retry 验证成功且 raw 复用。

## Observability / Auth / Deployment

后端会输出简易结构化 request log，包含 request id、method、path、status、duration。`/api/health` 和 `/api/health/ready` 不鉴权；`/api/metrics`、写接口和结果/对比/缓存接口在 `AUTH_ENABLED=true` 时受保护。

Auth 当前是边界占位：默认 `AUTH_ENABLED=false` 保持本地行为；启用后如果未实现 OIDC verifier 会 fail closed（403）。仅当明确设置 `AUTH_TRUSTED_HEADER_ENABLED=true` 时才接受 `X-Auth-User`，此模式只应放在可信反向代理之后。

Docker: `docker compose up --build` 会启动 API 和四个 worker，使用 SQLite 与共享 named volume。SQLite 适合单机/本地 volume，多主机/网络文件系统不在当前保证范围内。API 是默认 scheduler/notification owner，worker 服务关闭 scheduler/robot。

systemd: 示例在 `deploy/systemd/`。从 `.env.example` 创建 `/etc/datafilter/datafilter.env`，API 设置 `INPROCESS_WORKERS_ENABLED=false`，每个 step type 启动一个 `datafilter-worker@<step_type>`。
