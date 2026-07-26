# 本地部署（podman）

本机 kiro-rs 网关的实际部署形态与升级/回滚步骤。2026-07-26 随模型注册表分支首次部署验证。

## 运行形态

- 镜像：`localhost/kiro-rs:model-registry`（本仓库 `Dockerfile` 多阶段构建：bun 前端 + rust musl release）
- 数据目录：宿主 `/workspace/podman_project/kiro/data` → 容器 `/app/config/`
  - `config.json`（`apiKey` 是 **admin 登录密钥**；客户端鉴权在 `client_api_keys.json`，两者不通用）
  - `credentials.json`、`client_api_keys.json`、统计与缓存文件
  - `models.json` 只在首次通过 admin 编辑模型或开启同步后出现；不存在时行为与旧版完全一致
- 端口：`127.0.0.1:19095 → 8990`（宿主侧仅本机）
- 网络：**必须加入 `unified` 网络** —— sub2api / nginx / multica 等消费方按容器名 `kiro-rs:8990` 直连。
  漏掉的症状：消费方报 `dial tcp: lookup kiro-rs on 10.90.0.1:53: no such host`。
- 注意：仓库根的 `docker-compose.yml` 端口写的是 9095，与实际部署（19095、手工 podman run）**不一致**，不要直接用它重建。

## 升级

```bash
cd /workspace/podman_project/kiro/kiro.rs
podman build -t localhost/kiro-rs:model-registry .
podman stop kiro-rs && podman rm kiro-rs
podman run -d --name kiro-rs --restart unless-stopped \
  --add-host host.docker.internal:host-gateway \
  --network unified \
  -p 127.0.0.1:19095:8990 \
  -v /workspace/podman_project/kiro/data:/app/config/ \
  localhost/kiro-rs:model-registry \
  ./kiro-rs -c /app/config/config.json --credentials /app/config/credentials.json
```

复刻容器的最小核对集：**端口、挂载、命令行、网络、restart 策略** —— 五项都要 `podman inspect` 原容器确认，缺网络这一项曾导致消费方 DNS 解析失败。

## 升级后验证

1. `podman logs kiro-rs`：出现「模型表已装载」「模型同步调度器已启动」，无 error/panic。
2. 宿主侧：`curl -H "Authorization: Bearer <client_api_keys.json 里的 key>" http://127.0.0.1:19095/v1/models` 返回 23 个模型（零回归基线）。
3. **容器网络内**：`podman exec nginx curl -s -o /dev/null -w "%{http_code}" http://kiro-rs:8990/v1/models` 返回 401（未带鉴权即可，证明 DNS+TCP 通）。宿主侧验证代替不了这一步。

## 回滚

旧官方镜像保留在本地，数据目录不受影响：

```bash
podman stop kiro-rs && podman rm kiro-rs
# 用上面同一条 podman run，把镜像换成 docker.io/zyphrzero/kiro-rs:latest
```
