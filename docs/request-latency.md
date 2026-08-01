# 第一跳耗时的构成与归因方法

面向后来维护者:面板上「第一跳 3s+」是什么、为什么它基本不可优化、以及**怎么验证
连接有没有被复用**。最后一节记录了本次归因中的一处判断错误——那部分比结论更值得读,
因为它是一类很容易再犯的错:**把自己探针的成本算到生产路径上。**

## 一、结论先行

第一跳约 3s **不是故障、不是重试、不可优化**,它几乎全部是上游 AWS 从收到请求到
准备好吐响应头(prefill / 排队)的时间。

近 2h 实测(n=3260,attempt-0):

| 指标 | 值 |
| --- | --- |
| avg | 3320ms |
| p50 / p90 / p95 | 2533 / 4778 / 6067ms |
| min | 1224ms |
| 单跳成功占比 | 3300 / 3329 条 `total_attempts=1` |

拆层:

| 层 | 耗时 | 依据 |
| --- | --- | --- |
| kiro-rs 预处理 | ~3ms | `trace_attempts.started_ms` 均值 |
| 建连(TCP+SOCKS+TLS) | ≈0 | 连接被复用,无每请求握手(见第三节) |
| **上游 prefill → 响应头** | **~3s** | 余量;且与请求体大小无关(见第二节) |

## 二、`attempt.duration_ms` 到底量的是什么

`attempt_start` 在**重试循环开头**取(`src/kiro/provider.rs`,`for attempt in ..` 内
第一行),成功时在收到响应头处上报。所以它覆盖:

获取凭据(含必要时的 token 刷新)→ 解析 profileArn → 生成 machineId → 构造请求体
→ 发送 → **等到上游响应头到达**。

终点是「响应头到达」,不含 body/流内容。可以和 `first_token` phase 的 `started_ms`
对照确认:流式成功请求里 attempt-0 均值 3035ms,`first_token` phase 起点均值 3207ms,
两者基本重合。

**三条排除项**(都有实测,不是推断):

1. **不在本地。** 容器内到代理 TCP 建连 0ms、DNS 解析 0ms、SOCKS5 协商 ~1ms。
2. **不随请求体大小变化。** 按 `input_tokens` 分组,`<1k` 是 3031ms,`>100k` 是
   2839ms——大的反而略快。所以不是上传大 body 的传输时间。
3. **不是 token 刷新。** 刷新只在临近过期时触发,解释不了几乎每条请求都有的固定开销。

缓存命中略快(命中 3162ms / 未命中 3917ms),方向合理但幅度远不足以解释这 3s。

## 三、连接是复用的——以及怎么验证

`src/kiro/provider.rs` 给每个上游请求都加了 `.header("Connection", "close")`。
**这行在这条路径上不生效,不要以为它导致了每请求重新握手。**

- 上游 `q.<region>.amazonaws.com` 的 ALPN 协商结果是 **h2**(TLSv1.3,实测)。
- `Connection` 是 HTTP/1.1 的逐连接头,HTTP/2 禁止(RFC 9113 §8.2.2)。
- 流式 client(`build_streaming_client`)本来就按复用设计:显式配了
  `http2_keep_alive_interval` / `http2_keep_alive_timeout` / `tcp_keepalive`
  (`src/http_client.rs`),且 client 按出口代理缓存(`streaming_client_for`)。

**验证连接复用的正确方法:看 socket 身份是否稳定,不要读代码里的 header 猜。**

```bash
# 容器内到代理的 ESTABLISHED 连接,隔几秒采样多次
podman exec kiro-rs sh -c 'cat /proc/net/tcp /proc/net/tcp6' \
  | awk '$4=="01"'    # 01 = ESTABLISHED, 06 = TIME_WAIT
```

把第 2 列(本地 addr:port)解出来看:**本地端口在多次采样间不变 = 同一 socket 一直活着
= 在复用**。实测 1 条到代理的连接、本地端口 30 秒内 6 次采样完全不变,承载约
27 请求/分钟,TIME_WAIT 稳定在 1~4。若每请求真的新建连接,会看到端口不停变、
TIME_WAIT 持续堆积。

(端口号在 `/proc/net/tcp` 里是十六进制,`10301` = `0x283D`。手写正则容易错,建议解析
后按十进制比对。)

## 四、能优化的部分:代理选择

唯一不依赖上面推理、纯统计的结论。同一台代理主机的不同端口,延迟差异显著:

| 出口 | 样本 | attempt-0 avg |
| --- | --- | --- |
| `socks5h://…:10301` | 2865 | 2978ms |
| `socks5://…:10401` | 371 | **5018ms** |

10401 比 10301 慢约 2 秒。摘掉或降权是零代码改动的优化。

单独测各端口的 TLS 握手往返(仅代表链路质量,不代表生产每请求都付这个成本):
10201 最快 ~373ms、10401 ~410ms、10101 ~510ms、10301 ~528ms。注意**握手快的端口
不等于跑模型请求快**——10301 握手最慢却是实际表现最好的,说明瓶颈在代理出口到 AWS
的路径质量,不在握手。所以选代理要看 trace 里的 attempt-0 统计,不要看探活延迟。

查询:

```sql
SELECT COALESCE(a.proxy_url,'<null>'), COUNT(*), CAST(AVG(a.duration_ms) AS INT)
FROM trace_attempts a JOIN traces t USING(trace_id)
WHERE a.attempt=0 AND a.outcome='success' AND t.ts > datetime('now','-2 hours')
GROUP BY 1 ORDER BY 2 DESC;
```

## 五、归因中犯的错:把探针成本算进生产路径

第一轮结论是「`Connection: close` 让每请求多付 ~530ms TLS 握手,摘掉可提速」。**错的。**

错因:我写了个 Python 探针测分层耗时,每次探测都新建连接,于是每次都测到 ~530ms 握手。
我看到代码里有 `Connection: close`,就把这个**探针自身的成本**当成了生产路径的成本——
两件事被一个看起来合理的因果串了起来,但生产侧从没被测量过。

拆穿它的是两个独立证据:ALPN 实际协商 h2(那个 header 根本不该生效)、以及 socket
身份 30 秒不变(连接确实在复用)。**都不是靠读代码推出来的,是测出来的。**

方法论:
- 探针测的是**探针的路径**,不是生产的路径。要主张生产路径的成本,就得测生产路径
  本身(socket 状态、trace 埋点),或者让探针精确复现生产的连接生命周期。
- 「代码里写了 X」到「X 生效」之间,隔着协议协商、库的行为、运行时配置。h2 会剥掉
  `Connection` 这种事,只看业务代码是看不到的。
- 有一个便宜的判据就先用它。数一下 ESTABLISHED socket 花了 30 秒,比推理链可靠得多。

另有一处小失误值得记:用同一条 TLS 连接连打 3 个请求想测 keep-alive 往返,第 2、3 次
读到 0ms——那是在读第 1 个响应残留在缓冲区里的字节,不是真的零延迟。复用连接测往返
必须把上一个响应完整读干净(按 `Content-Length` 或读到连接半关)再计时。

