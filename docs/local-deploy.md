# 本地部署（podman）

本机 kiro-rs 网关的实际部署形态与升级/回滚步骤。2026-07-26 随模型注册表分支首次部署验证。

## 运行形态

- 镜像：`localhost/kiro-rs:0.8.8`（本仓库 `Dockerfile` 多阶段构建：bun 前端 + rust musl release）
- 数据目录：宿主 `/workspace/podman_project/kiro/data` → 容器 `/app/config/`
  - `config.json`（`apiKey` 是 **admin 登录密钥**；客户端鉴权在 `client_api_keys.json`，两者不通用）
  - `credentials.json`、`client_api_keys.json`、统计与缓存文件
  - `models.json` 只在首次通过 admin 编辑模型或开启同步后出现；不存在时行为与旧版完全一致
- 端口：`127.0.0.1:19095 → 8990`（宿主侧仅本机）
- 网络：**必须且只能加入 `unified` 网络，并保留 `kiro-rs` DNS alias** —— sub2api / nginx / multica 等消费方按容器名 `kiro-rs:8990` 直连。
  漏掉的症状：消费方报 `dial tcp: lookup kiro-rs on 10.90.0.1:53: no such host`。
- 注意：仓库根的 `docker-compose.yml` 端口写的是 9095，与实际部署（19095、手工 podman run）**不一致**，不要直接用它重建。
- `--network-alias kiro-rs`：**实测可省**（podman 自动把容器名注册进 DNS，`getent hosts kiro-rs`
  仍能解析）。显式写上更保险，漏了不会出故障。
- `--add-host host.docker.internal:host-gateway`：当前配置下**未被任何代码路径使用**
  （代理走 LAN IP），漏了无影响。若将来把 `countTokensApiUrl` 等指向宿主服务则必须加回。

## 流式超时配置

`config.json` 的 `streamIdleTimeoutSecs`（空闲，生产 300）与 `streamTotalTimeoutSecs`
（总，生产 1800）控制流式上游超时，**改配置 + 重启即可生效，不必重建镜像**。
未设置时取代码默认值，老配置可直接启动。取值依据与三次定错的教训见
[流式请求的超时设计与阈值定法](streaming-timeouts.md)——改这两个值之前务必先读，
定错会导致「杀掉正在正常生成的请求」这种最贵的失败。

## 升级

```bash
cd /workspace/podman_project/kiro/kiro.rs
podman build -t localhost/kiro-rs:0.8.8 .
podman stop kiro-rs && podman rm kiro-rs
podman run -d --name kiro-rs --restart unless-stopped \
  --add-host host.docker.internal:host-gateway \
  --network unified \
  --network-alias kiro-rs \
  -p 127.0.0.1:19095:8990 \
  -v /workspace/podman_project/kiro/data:/app/config/ \
  localhost/kiro-rs:0.8.8 \
  ./kiro-rs -c /app/config/config.json --credentials /app/config/credentials.json
```

复刻容器的最小核对集：**端口、挂载、命令行、网络、restart 策略**。停旧容器前必须用 `podman inspect kiro-rs` 核对五项；网络以 `.NetworkSettings.Networks` 为准，不能只看 `.HostConfig.NetworkMode`。未确认网络和 alias 时不得删除或替换旧容器。

## 升级后验证

1. `podman logs kiro-rs`：出现「模型表已装载」「模型同步调度器已启动」，无 error/panic。
2. 宿主侧：`curl -H "x-api-key: <client_api_keys.json 里启用中的 key>" http://127.0.0.1:19095/v1/models` 返回该 key 所属分组可用的模型。
   **0.8.4 起模型列表按凭据组收窄**（组内并集），不同 key 的条数本就不同（实测 29 / 31），
   不再有"固定 23 个"这种全局基线；判据改为「两个不同分组的 key 返回条数确有差异」。
   注意 `config.json` 的 `apiKey` 被镜像为 `client_api_keys.json` 的系统键（id=0），
   该键若已 disabled 则返回 401——这是正确行为，调试一律用启用中的 client key。
3. **容器 DNS**：`podman exec sub2api getent hosts kiro-rs` 必须解析出 `10.90.x.x` 地址。
4. **容器网络内**：`podman exec sub2api curl -sS -o /dev/null -w "%{http_code}" http://kiro-rs:8990/v1/models` 返回 401（未带鉴权即可，证明 DNS、TCP、HTTP 和鉴权层均可达）。宿主侧验证代替不了这一步。

## 回滚

旧官方镜像保留在本地，数据目录不受影响：

```bash
podman stop kiro-rs && podman rm kiro-rs
# 用上面同一条 podman run，把镜像换成 docker.io/zyphrzero/kiro-rs:latest
```
