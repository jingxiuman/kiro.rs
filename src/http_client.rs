//! HTTP Client 构建模块
//!
//! 提供统一的 HTTP Client 构建功能，支持代理配置

use reqwest::{Client, Proxy};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::model::config::TlsBackend;

/// 上游链路保活参数。
///
/// 下游（客户端方向）已有 25s 的 SSE ping（handlers::PING_INTERVAL_SECS），但下游活着
/// 不等于上游活着：Opus 长思考期间上游连接可能几分钟无数据帧，链路中间的代理会把
/// 静默连接判死并 RST_STREAM（实测多次中断集中在 ~240s）。上游保活须独立配置，
/// 且间隔必须显著小于链路上最短的 idle timeout。
const HTTP2_KEEP_ALIVE_INTERVAL_SECS: u64 = 25;
/// PING 发出后多久没有 ACK 判定连接已死。宁可早断快重试，不要挂在死连接上耗满总超时。
const HTTP2_KEEP_ALIVE_TIMEOUT_SECS: u64 = 15;
/// TCP 层保活首探时间。reqwest 0.12 默认已开（15s 起、15s 间隔、3 次重试），这里显式
/// 钉住取值以免依赖升级悄悄改变行为。经代理隧道时 h2 PING 走端到端、TCP keepalive
/// 只覆盖到代理这一跳，两层各防一段链路。
const TCP_KEEPALIVE_SECS: u64 = 30;

/// 流式上游连接的**空闲**超时：两个数据帧之间允许的最长间隔，每收到一帧即重置。
///
/// 不能用总超时（`Client::timeout`）约束流式响应：它覆盖整个 body 读取过程，会在
/// 「生成正常但耗时长」时把成功的请求斩断。实测 Opus 长思考单条请求可跑到 712s，
/// 而旧的 720s 总超时距此仅 8s——撞上去的后果是烧掉十分钟和全部输入 token 后
/// 才拿到 timeout，是最贵的失败形态。
///
/// 取值须高于**合法**静默间隔的上限。历史中断簇集中在 240~265s（中间代理的 idle
/// timeout 判死 RST_STREAM），说明合法静默确实能接近甚至超过 240s；300s 留出余量。
/// 传输层真死不靠这个兜底——h2 PING（25s 间隔 + 15s ACK 超时）约 40s 就能识别，
/// 本超时只管「连接活着、PING 有回应，但上游这条流再也不吐数据」的卡死。
const STREAM_IDLE_TIMEOUT_SECS: u64 = 300;

/// 编译期钉住上述取值下界：调低到合法静默上限（观测 240~265s）以下，就会把正常的
/// Opus 长思考误判为卡死，正是本次改动要消除的失败形态。放编译期而非测试里，
/// 是因为这是常量间的不变式，改错时应当直接编不过。
const _: () = assert!(STREAM_IDLE_TIMEOUT_SECS > 265);

/// 进程级出网策略：开启后，任何「无代理」的出网请求都会被拒绝。
///
/// 用全局状态而非函数参数，是为了让**新增的出网点默认受保护**——本模块是 Kiro 上游
/// reqwest::Client 的唯一出口，漏传一个参数就会静默裸连，而全局开关漏不掉。
/// （admin 的更新器 binary_update.rs 与 release 检查 service.rs 各自独立建 client，
/// 不走本出口，也不受本模块的代理策略与保活配置约束。）
static REQUIRE_PROXY: AtomicBool = AtomicBool::new(false);

/// 由 main 在读完配置后设置一次（config.requireProxy）
pub fn set_require_proxy(enabled: bool) {
    REQUIRE_PROXY.store(enabled, Ordering::Relaxed);
}

/// 当前是否强制走代理
pub fn require_proxy() -> bool {
    REQUIRE_PROXY.load(Ordering::Relaxed)
}

/// 代理配置
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ProxyConfig {
    /// 代理地址，支持 http/https/socks5
    pub url: String,
    /// 代理认证用户名
    pub username: Option<String>,
    /// 代理认证密码
    pub password: Option<String>,
}

impl ProxyConfig {
    /// 从 url 创建代理配置
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            username: None,
            password: None,
        }
    }

    /// 设置认证信息
    pub fn with_auth(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self.password = Some(password.into());
        self
    }
}

/// 构建 HTTP Client
///
/// # Arguments
/// * `proxy` - 可选的代理配置
/// * `timeout_secs` - 超时时间（秒）
///
/// # Returns
/// 配置好的 reqwest::Client
pub fn build_client(
    proxy: Option<&ProxyConfig>,
    timeout_secs: u64,
    tls_backend: TlsBackend,
) -> anyhow::Result<Client> {
    build_client_with_policy(proxy, Timeout::Total(timeout_secs), tls_backend, require_proxy())
}

/// 构建**流式**上游 Client：用空闲超时（[`STREAM_IDLE_TIMEOUT_SECS`]）而非总超时。
///
/// 仅供聊天/流式路径使用。认证、token 刷新、版本探测、代理探活等一问一答的调用
/// 应继续用 [`build_client`]——对它们总超时才是正确语义。
pub fn build_streaming_client(
    proxy: Option<&ProxyConfig>,
    tls_backend: TlsBackend,
) -> anyhow::Result<Client> {
    build_client_with_policy(proxy, Timeout::Idle, tls_backend, require_proxy())
}

/// Client 的超时语义。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Timeout {
    /// 总请求超时（秒），覆盖建连到 body 读完的全过程。适用于一问一答。
    Total(u64),
    /// 空闲超时：每次成功读取后重置，不限制总时长。适用于流式。
    Idle,
}

/// [`build_client`] 的策略显式版本。仅用于测试：全局开关会让并发跑的其它用例互相干扰。
fn build_client_with_policy(
    proxy: Option<&ProxyConfig>,
    timeout: Timeout,
    tls_backend: TlsBackend,
    require_proxy: bool,
) -> anyhow::Result<Client> {
    // 失败即拒绝，不降级直连：代理不可用时裸连会把真实 IP 暴露给上游
    if require_proxy && proxy.is_none() {
        anyhow::bail!(
            "requireProxy 已开启，但本次出网没有可用代理（凭据与全局均未配置代理，\
             或绑定的代理已被禁用），已拒绝请求以避免真实 IP 泄露"
        );
    }

    let mut builder = Client::builder()
        .http2_keep_alive_interval(Duration::from_secs(HTTP2_KEEP_ALIVE_INTERVAL_SECS))
        .http2_keep_alive_timeout(Duration::from_secs(HTTP2_KEEP_ALIVE_TIMEOUT_SECS))
        // 不开 http2_keep_alive_while_idle：有活跃流时 PING 本来就发（覆盖流式长静默这个
        // 目标场景）；池中全空闲连接 90s 就被驱逐，活不到需要保活的时刻，开了只多流量
        .tcp_keepalive(Duration::from_secs(TCP_KEEPALIVE_SECS));

    builder = match timeout {
        Timeout::Total(secs) => builder.timeout(Duration::from_secs(secs)),
        // 不设总超时：长流的活性由 read_timeout + h2 PING 两层保证，见
        // STREAM_IDLE_TIMEOUT_SECS 的说明
        Timeout::Idle => builder.read_timeout(Duration::from_secs(STREAM_IDLE_TIMEOUT_SECS)),
    };

    match tls_backend {
        TlsBackend::Rustls => {
            builder = builder.use_rustls_tls();
        }
        TlsBackend::NativeTls => {
            #[cfg(feature = "native-tls")]
            {
                builder = builder.use_native_tls();
            }
            #[cfg(not(feature = "native-tls"))]
            {
                anyhow::bail!("此构建版本未包含 native-tls 后端，请在配置中改用 rustls");
            }
        }
    }

    if let Some(proxy_config) = proxy {
        let mut proxy = Proxy::all(&proxy_config.url)?;

        // 设置代理认证
        if let (Some(username), Some(password)) = (&proxy_config.username, &proxy_config.password) {
            proxy = proxy.basic_auth(username, password);
        }

        builder = builder.proxy(proxy);
        tracing::debug!("HTTP Client 使用代理: {}", proxy_config.url);
    }

    Ok(builder.build()?)
}

/// source 链最大展开层数。hyper/h2 的链通常 2~4 层，6 层足够且防止意外的自引用循环。
const ERROR_CHAIN_MAX_DEPTH: usize = 6;

/// 把 reqwest 错误连同其 source 链渲染成单行，供 trace / 运维面板归因。
///
/// 只打顶层 Display 会丢掉真正的原因：reqwest 把 body 阶段的所有失败都压成同一个
/// `error decoding response body`，而「h2 RST_STREAM / connection closed before
/// message completed / timed out」的区别只存在于 source 链里——这三者修法完全不同。
///
/// 标签紧跟顶层消息、排在 source 链之前：下游消费方（[`crate::admin::ops`] /
/// `proxy_pool`）会把错误串截到 200 字符，标签放尾部会被正好截掉。
pub fn describe_reqwest_error(err: &reqwest::Error) -> String {
    render_error_chain(err, reqwest_kind_tag(err))
}

/// reqwest 的分类谓词并不互斥（总超时会同时命中 timeout+body+decode），全部列出更利于归因
fn reqwest_kind_tag(err: &reqwest::Error) -> Option<String> {
    let mut kinds: Vec<&str> = Vec::new();
    if err.is_timeout() {
        kinds.push("timeout");
    }
    if err.is_connect() {
        kinds.push("connect");
    }
    if err.is_request() {
        kinds.push("request");
    }
    if err.is_body() {
        kinds.push("body");
    }
    if err.is_decode() {
        kinds.push("decode");
    }
    (!kinds.is_empty()).then(|| kinds.join("+"))
}

fn render_error_chain(err: &(dyn std::error::Error + 'static), tag: Option<String>) -> String {
    let head = err.to_string();
    let mut out = head.clone();
    if let Some(tag) = tag {
        out.push_str(" [");
        out.push_str(&tag);
        out.push(']');
    }

    // hyper/h2 常把同一句话在相邻两层各重复一遍，纯噪声
    let mut prev = head;
    let mut source = err.source();
    let mut depth = 0usize;
    while let Some(cause) = source {
        if depth >= ERROR_CHAIN_MAX_DEPTH {
            out.push_str(" <- …");
            break;
        }
        let msg = cause.to_string();
        if msg != prev {
            out.push_str(" <- ");
            out.push_str(&msg);
            prev = msg;
        }
        source = cause.source();
        depth += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_proxy_config_new() {
        let config = ProxyConfig::new("http://127.0.0.1:7890");
        assert_eq!(config.url, "http://127.0.0.1:7890");
        assert!(config.username.is_none());
        assert!(config.password.is_none());
    }

    #[test]
    fn test_proxy_config_with_auth() {
        let config = ProxyConfig::new("socks5://127.0.0.1:1080").with_auth("user", "pass");
        assert_eq!(config.url, "socks5://127.0.0.1:1080");
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
    }

    #[test]
    fn test_build_client_without_proxy() {
        let client = build_client(None, 30, TlsBackend::Rustls);
        assert!(client.is_ok());
    }

    #[test]
    fn test_build_client_with_proxy() {
        let config = ProxyConfig::new("http://127.0.0.1:7890");
        let client = build_client(Some(&config), 30, TlsBackend::Rustls);
        assert!(client.is_ok());
    }

    #[test]
    fn build_streaming_client_succeeds_with_and_without_proxy() {
        assert!(build_client_with_policy(None, Timeout::Idle, TlsBackend::Rustls, false).is_ok());
        let proxy = ProxyConfig::new("socks5://127.0.0.1:1080");
        assert!(
            build_client_with_policy(Some(&proxy), Timeout::Idle, TlsBackend::Rustls, false).is_ok()
        );
    }

    /// 流式 client 同样受 requireProxy 约束：漏掉它就等于给流式路径开了裸连后门，
    /// 而流式恰好是全部聊天流量的走向。
    #[test]
    fn require_proxy_also_guards_streaming_client() {
        let err = build_client_with_policy(None, Timeout::Idle, TlsBackend::Rustls, true)
            .expect_err("无代理时流式 client 也应拒绝");
        assert!(err.to_string().contains("requireProxy"), "{err}");
    }

    /// 直接测策略版本：全局开关会被并发跑的其它用例观察到，造成假失败
    #[test]
    fn require_proxy_rejects_direct_egress() {
        let err = build_client_with_policy(None, Timeout::Total(30), TlsBackend::Rustls, true)
            .expect_err("无代理时应拒绝");
        let msg = err.to_string();
        assert!(msg.contains("requireProxy"), "{msg}");
        assert!(msg.contains("IP"), "错误应说明拒绝理由: {msg}");
    }

    #[test]
    fn require_proxy_allows_egress_through_a_proxy() {
        let proxy = ProxyConfig::new("socks5://127.0.0.1:1080");
        assert!(
            build_client_with_policy(Some(&proxy), Timeout::Total(30), TlsBackend::Rustls, true)
                .is_ok()
        );
    }

    #[test]
    fn direct_egress_still_allowed_when_policy_is_off() {
        // 默认关闭时行为必须与加固前完全一致
        assert!(
            build_client_with_policy(None, Timeout::Total(30), TlsBackend::Rustls, false).is_ok()
        );
    }

    #[test]
    fn require_proxy_defaults_to_off() {
        assert!(!require_proxy(), "默认必须关闭，否则升级即断网");
    }

    /// 无标签渲染。链的展开逻辑与 [`describe_reqwest_error`] 共用 [`render_error_chain`]，
    /// 但 reqwest::Error 的内部构造器不公开，造不出指定的 source 链，故直接测内部函数。
    fn render_error_chain_for_test(err: &(dyn std::error::Error + 'static)) -> String {
        render_error_chain(err, None)
    }

    /// 构造任意深度的 source 链（reqwest::Error 的内部构造器不公开，无法直接造）
    #[derive(Debug)]
    struct ChainErr {
        msg: String,
        source: Option<Box<ChainErr>>,
    }

    impl std::fmt::Display for ChainErr {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(&self.msg)
        }
    }

    impl std::error::Error for ChainErr {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source
                .as_deref()
                .map(|e| e as &(dyn std::error::Error + 'static))
        }
    }

    /// 从最内层往外拼一条链
    fn chain(msgs: &[&str]) -> ChainErr {
        let mut iter = msgs.iter().rev();
        let mut err = ChainErr {
            msg: iter.next().expect("至少一层").to_string(),
            source: None,
        };
        for msg in iter {
            err = ChainErr {
                msg: msg.to_string(),
                source: Some(Box::new(err)),
            };
        }
        err
    }

    #[test]
    fn chain_without_source_is_just_the_message() {
        let err = chain(&["error decoding response body"]);
        assert_eq!(render_error_chain_for_test(&err), "error decoding response body");
    }

    #[test]
    fn chain_appends_each_cause_in_order() {
        // 这正是要区分的场景：顶层串一样，底层原因不同
        let timeout = chain(&["error decoding response body", "operation timed out"]);
        assert_eq!(
            render_error_chain_for_test(&timeout),
            "error decoding response body <- operation timed out"
        );

        let reset = chain(&[
            "error decoding response body",
            "stream error received: unspecific protocol error detected",
            "http2 error: stream error received: RST_STREAM",
        ]);
        assert_eq!(
            render_error_chain_for_test(&reset),
            "error decoding response body \
             <- stream error received: unspecific protocol error detected \
             <- http2 error: stream error received: RST_STREAM"
        );
    }

    #[test]
    fn adjacent_duplicate_causes_are_collapsed() {
        let err = chain(&[
            "error decoding response body",
            "connection closed before message completed",
            "connection closed before message completed",
        ]);
        assert_eq!(
            render_error_chain_for_test(&err),
            "error decoding response body <- connection closed before message completed"
        );
    }

    #[test]
    fn chain_is_capped_at_max_depth() {
        let msgs: Vec<String> = (0..ERROR_CHAIN_MAX_DEPTH + 4)
            .map(|i| format!("layer{i}"))
            .collect();
        let refs: Vec<&str> = msgs.iter().map(String::as_str).collect();
        let rendered = render_error_chain_for_test(&chain(&refs));

        assert!(rendered.ends_with(" <- …"), "超深链应截断: {rendered}");
        assert!(rendered.contains("layer0"));
        // 顶层不占 depth，故最后保留的是 layer{MAX_DEPTH}
        assert!(!rendered.contains(&format!("layer{}", ERROR_CHAIN_MAX_DEPTH + 1)));
    }

    #[test]
    fn reqwest_timeout_error_is_tagged_before_the_chain() {
        // 真实 reqwest::Error：连不通且超时的请求
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        // Client 的构造要在 runtime 上下文内（连接池需要 reactor）
        let err = rt.block_on(async {
            let client = build_client(None, 1, TlsBackend::Rustls).unwrap();
            // 192.0.2.0/24 是 TEST-NET-1，保证无响应 → 触发超时
            client.get("http://192.0.2.1/").send().await
        });
        let err = err.expect_err("应超时");

        let rendered = describe_reqwest_error(&err);
        let tag_start = rendered.find('[').expect("应有分类标签");
        assert!(rendered[tag_start..].starts_with("[timeout"), "{rendered}");
        // 标签必须排在 source 链之前，否则会被下游 200 字符截断吃掉
        if let Some(chain_start) = rendered.find(" <- ") {
            assert!(tag_start < chain_start, "标签应先于链: {rendered}");
        }
    }
}
