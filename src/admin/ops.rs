//! 运维（Ops）模块：上游问题与代理池问题的统一统计和自动处置
//!
//! 两个组件：
//! - [`OpsStore`]：DuckDB 存储。处置事件写 `ops_events` 表；统计聚合直接对
//!   kiro.duckdb 里既有的 `traces` / `trace_attempts` 表做 SQL 聚合（同一
//!   DuckDB 实例上的独立连接，进程内 MVCC 并发读写安全），不引入第二份数据。
//! - [`OpsRuntime`]：请求路径上的反馈编排。provider / 流处理把「网络错误、
//!   流中断」按所用代理上报到这里；连续失败达阈值的代理被自动禁用后，
//!   在此完成受影响凭据的解绑/换绑，并记录处置事件。
//!
//! 设计参考 sub2api 的 ops 体系（错误分类统计、自动冷却/禁用、处置留痕），
//! 按单二进制 + SQLite 的体量裁剪。

use std::path::PathBuf;
use std::sync::Arc;

use chrono::Utc;
use duckdb::Connection;
use parking_lot::Mutex;
use serde::Serialize;

use super::proxy_pool::ProxyPoolManager;
use crate::kiro::token_manager::MultiTokenManager;

/// 事件默认查询条数
const DEFAULT_EVENTS_LIMIT: usize = 100;
/// 统计窗口上限（小时）：防御性限制，避免全表扫描过大窗口
const MAX_WINDOW_HOURS: i64 = 24 * 31;
/// 交叉表每个 error_type 最多回多少个维度桶。
/// 判断"是否集中"只需要看头部几个，全量回传对结论无增益。
const CROSSTAB_TOP_BUCKETS: usize = 6;
/// 错误指纹模板的最大长度（字符）。
///
/// 上游 500 会把整个 JSON body 拼进消息，不截断的话模板本身就是几百字符的
/// 唯一串，归并率归零。110 是实测取值：足够放下 `{"message":"…"}` 里区分
/// 根因的那句话（如 `unexpectedly high load` vs `an unexpected error`），
/// 又短到能把后面的 `reason` 字段等尾部差异切掉。
const FINGERPRINT_MAX_CHARS: usize = 110;
/// 每个指纹保留的原始样本条数上限
const FINGERPRINT_SAMPLES: usize = 2;
/// 指纹接口的 limit 上限
pub const MAX_FINGERPRINT_LIMIT: usize = 200;

/// 判定「这一跳的边际收益塌了」的阈值：到达该跳的请求里靠它救回的不足 1/5。
///
/// 为什么是 0.20 而不是别的数：实测 7 天分布的逐跳边际收益是
/// 0.95 / 0.37 / 0.25 / 0.15 —— 前三跳落在 0.25 以上，最后一跳掉到 0.15，
/// 中间那道空档（0.15 与 0.25 之间）就是自然分界，0.20 取在空档中点，
/// 两侧各留 5 个百分点余量，日常波动不至于让判定来回翻。
///
/// 阈值的业务含义：跌破它意味着该跳约 4/5 的重试预算是在给注定失败的请求陪跑。
/// 上游 429 多为账号级配额，这些白打的重试还会在账号间连环撞墙、放大限流
/// （见 `provider.rs` 里 `MAX_TOTAL_RETRIES` 上方的注释），所以代价不只是延迟。
///
/// 这里刻意不取更严的 0.30：那会把第 3 跳（实测 0.25，净救回 61 条）也判成塌掉，
/// 而那一跳还在实打实地救请求。
const YIELD_COLLAPSE_THRESHOLD: f64 = 0.20;

/// 重试阶梯的最大深度。与 `provider.rs` 的 `MAX_TOTAL_RETRIES` 对齐，
/// 在此重复一份而非跨模块引用：这里只是聚合侧的防御性上限，
/// 用来在库里出现异常大的 `total_attempts` 时截断输出，
/// 不参与任何重试决策，两者不需要保持强耦合。
const RETRY_LADDER_MAX_DEPTH: u32 = 4;

/// 处置事件分类
pub mod event_category {
    /// 请求级反馈触发的代理自动禁用
    pub const PROXY_AUTO_DISABLE: &str = "proxy_auto_disable";
    /// 自动禁用后受影响凭据的解绑/换绑
    pub const PROXY_REASSIGN: &str = "proxy_reassign";
    /// 健康检查（探测）触发的代理自动禁用
    pub const PROXY_PROBE_DISABLE: &str = "proxy_probe_disable";
    /// 恢复探针把自动禁用的代理放回账号池
    pub const PROXY_AUTO_RECOVER: &str = "proxy_auto_recover";
}

/// 一条处置/异常事件
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsEvent {
    pub id: u64,
    pub ts: String,
    pub category: String,
    /// info / warn / error
    pub severity: String,
    /// 事件主体（如 `proxy#3` / `credential#5`）
    pub subject: String,
    pub message: String,
}

/// 运维概览（窗口内）
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsOverview {
    pub window_hours: i64,
    pub total: u64,
    pub success: u64,
    pub error: u64,
    pub interrupted: u64,
    /// error_type → 数量（含 interrupted 类）
    pub by_error_type: Vec<ErrorTypeCount>,
    pub avg_duration_ms: u64,
    /// 平均首 token 延迟，**仅统计流式请求**。
    ///
    /// 非流式自 0.8.7 起也有 `first_token_ms`，但混进来会让这个指标的口径从
    /// 「流式首 token」变成「流式+非流式混合平均」，与历史窗口不可比 ——
    /// 非流式的首字节含义（等上游吐第一口，之后还要收完整段）与流式（此后即可
    /// 边收边发给客户端）对运维决策的指向也不同。故显式钉死在流式上。
    pub avg_first_token_ms: Option<u64>,
    /// 中断类请求（stream_interrupted / upstream_truncated）的耗时分位。
    /// 多个中断集中在同一时长通常指向链路上的固定超时。
    pub interrupted_duration: Option<DurationPercentiles>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorTypeCount {
    pub error_type: String,
    pub count: u64,
}

/// 耗时分位。
///
/// 替代原先的平均值：平均值会给出一个任何真实样本都不在的数字。实测中断同时
/// 存在 ~240s 与 ~720s 两簇（720s 恰是 `MCP_TOTAL_TIMEOUT_SECS`），平均落在
/// 中间的 ~300s —— 两簇里都不存在,反而把"存在两个不同固定超时"这个结论抹掉。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DurationPercentiles {
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    /// 样本数。实测 7 天窗口仅约 20 条，此时 p99 就是最大值本身；
    /// 面板必须一并显示 n，否则 p99 会被当成有统计意义的分位读。
    pub n: u64,
}

/// 从样本算分位。就地排序后按最近秩取值。
///
/// 不用 SQL 算：SQLite 无 `PERCENTILE_CONT`，而中断样本量在百级以下，
/// 取回来排序比在 SQL 里拼窗口函数更简单也更好测。
fn percentiles(mut v: Vec<u64>) -> Option<DurationPercentiles> {
    if v.is_empty() {
        return None;
    }
    v.sort_unstable();
    let at = |p: f64| -> u64 {
        let idx = ((v.len() as f64 - 1.0) * p).round() as usize;
        v[idx.min(v.len() - 1)]
    };
    Some(DurationPercentiles {
        p50: at(0.50),
        p95: at(0.95),
        p99: at(0.99),
        n: v.len() as u64,
    })
}

/// 把一条自由文本错误消息归一化成 `(HTTP 状态码, 指纹模板)`。
///
/// 纯函数，不碰数据库 —— 归一化规则的对错只能靠样例验证，能单测是硬要求。
///
/// 全部规则按此顺序生效，顺序本身承载语义：
/// 1. 空白折叠：504 的 HTML body 里有换行，不折叠会让同一模板出现多种写法。
/// 2. 砍掉 ` <- ` 之后的 reqwest 因果链。同一根因有时带链有时不带（取决于
///    错误在哪一层被捕获），不砍就会分裂成两类。
/// 3. 剥掉 reqwest 的 kind 标签（`[decode]` / `[timeout+decode]`）。它是分类
///    元信息而非消息内容，留着会让同源错误按标签再分裂一次。**代价**：
///    `timeout+decode` 与纯 `decode` 会被并进同一指纹，两者修法其实不同 ——
///    这一维差异只能从 `samples` 里的原始消息看回来。
/// 4. 提取状态码并换成 `<status>` 占位。必须早于第 6 步，否则状态码会被
///    当成普通数字抹平，`500 Internal Server Error` 和 `400 Bad Request`
///    就并成一类了，而它俩是必须分开的两件事。
/// 5. URL 只留到 path，砍掉 query。当前样本里的 URL 本身是稳定的，这条是
///    防御性规则（上游一旦带上时间戳/请求 id 参数，指纹会立刻爆炸）。
/// 6. 长 id 收敛成 `<id>`：`toolu_bdrk_013iHieazSJUr5RHg31UftAr` 这类 token
///    每次调用都不同。判据是「≥12 字符且数字 ≥4 个」，刻意排除 `us-east-1`、
///    `claude-opus-5` 这类含少量数字的稳定标识 —— 它们整体被抹成 `<id>` 会
///    直接丢掉可读性，而下一条只动其中的数字，模板仍认得出是什么。
/// 7. 余下数字一律换 `N`：覆盖 `（8/1）`→`（N/N）`、`bytes=26719`、
///    `idle_ms=9086` 等计数/时长。**代价**：区号/版本号也在内 ——
///    `us-east-1`→`us-east-N`、`claude-opus-5` 与 `-4` 会同模板。可接受的理由是
///    模型与地域维度另有 [`OpsStore::error_crosstab`] 可看，指纹这一维不重复承担。
/// 8. 截断到 [`FINGERPRINT_MAX_CHARS`]，防超长 JSON body 把指纹撑成唯一串。
pub fn normalize_error_message(raw: &str) -> (Option<u16>, String) {
    // 1. 空白折叠
    let mut s: String = {
        let mut out = String::with_capacity(raw.len());
        let mut in_ws = false;
        for ch in raw.trim().chars() {
            if ch.is_whitespace() {
                in_ws = true;
            } else {
                if in_ws && !out.is_empty() {
                    out.push(' ');
                }
                in_ws = false;
                out.push(ch);
            }
        }
        out
    };
    // 2. 砍因果链
    if let Some(idx) = s.find(" <- ") {
        s.truncate(idx);
    }
    // 3. kind 标签（形如 ` [timeout+decode]`）**保留不剥**。
    //
    // 它由 describe_reqwest_error 从固定谓词集生成，携带这类错误里最关键的归因
    // 区分：`[timeout+decode]` = 本地超时掐死了流（我们的配置杀了健康流），
    // `[decode]` = 上游断了。两者处置动作相反。
    //
    // 实测代价：剥掉会把 9 条裸消息 + 6 条 `[decode]` + 3 条 `[timeout+decode]`
    // 并成同一个指纹（共 18 条），把最该分开的三种情况混成一条。
    // 标签基数有界（谓词集固定），保留它不会让指纹数爆炸。
    // 4. 提状态码
    let (status, s) = extract_http_status(&s);
    // 5. URL 去 query
    let s = strip_url_queries(&s);
    // 6~7. 逐 token 归一化
    let mut out = String::with_capacity(s.len());
    for token in split_keep_delims(&s) {
        match token {
            Token::Delim(d) => out.push(d),
            Token::Word(w) => out.push_str(&normalize_token(w)),
        }
    }
    // 8. 截断
    let mut fingerprint = String::new();
    for (i, ch) in out.chars().enumerate() {
        if i >= FINGERPRINT_MAX_CHARS {
            fingerprint.push('…');
            break;
        }
        fingerprint.push(ch);
    }
    (status, fingerprint)
}

/// 剥掉紧跟顶层消息的 reqwest kind 标签（形如 ` [timeout+decode]`）。
///
/// **归一化流程刻意不用它** —— 见 [`normalize_error_message`] 第 3 步的说明。
/// 保留这个函数是因为它单独测试过、且未来若出现基数失控的标签仍可启用。
#[allow(dead_code)]
fn strip_kind_tag(s: &str) -> String {
    let Some(open) = s.find(" [") else {
        return s.to_string();
    };
    let rest = &s[open + 2..];
    let Some(close) = rest.find(']') else {
        return s.to_string();
    };
    let inner = &rest[..close];
    let looks_like_tag = !inner.is_empty()
        && inner.len() <= 30
        && inner
            .chars()
            .all(|c| c.is_ascii_lowercase() || c == '+' || c == '_');
    if !looks_like_tag {
        return s.to_string();
    }
    let mut out = s[..open].to_string();
    out.push_str(&rest[close + 1..]);
    out
}

/// 从消息里摘出 HTTP 状态码，并把它换成 `<status>` 占位。
///
/// 只认「独立的 3 位数字（100..=599），且后面紧跟以大写字母开头的原因短语」
/// 这一形态。宽松匹配任意 3 位数会把 `idle_ms=218`、`bytes=26719` 里的片段
/// 当成状态码 —— 那比不提取更糟，会造出假的状态码维度。
fn extract_http_status(s: &str) -> (Option<u16>, String) {
    let bytes = s.as_bytes();
    let mut i = 0usize;
    while i + 3 <= bytes.len() {
        if !bytes[i].is_ascii_digit() {
            i += 1;
            continue;
        }
        // 数字串必须恰好 3 位，且左边界不是字母/数字/`.`/`=`/`/`
        let start = i;
        let mut end = i;
        while end < bytes.len() && bytes[end].is_ascii_digit() {
            end += 1;
        }
        let left_ok = start == 0 || {
            let p = bytes[start - 1];
            !(p as char).is_ascii_alphanumeric() && !matches!(p, b'.' | b'=' | b'/' | b'_' | b'-')
        };
        if end - start == 3 && left_ok {
            let code: u16 = s[start..end].parse().unwrap_or(0);
            // 右边必须是空格 + 大写字母开头的原因短语（`500 Internal Server Error`）
            let reason_ok = s[end..]
                .strip_prefix(' ')
                .and_then(|r| r.chars().next())
                .is_some_and(|c| c.is_ascii_uppercase());
            if (100..=599).contains(&code) && reason_ok {
                let mut out = String::with_capacity(s.len());
                out.push_str(&s[..start]);
                out.push_str("<status>");
                out.push_str(&s[end..]);
                return (Some(code), out);
            }
        }
        i = end.max(start + 1);
    }
    (None, s.to_string())
}

/// 把消息里每个 URL 截到 path，丢掉 `?` 之后的 query。
///
/// query 里常带请求 id / 时间戳，是指纹爆炸的经典来源。当前样本的 URL 恰好
/// 不带 query，所以这条规则在生产数据上是空操作 —— 保留它是因为上游一变就来不及。
fn strip_url_queries(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(pos) = rest.find("://") {
        // scheme://host/path 的结束边界：空白或右括号
        let after = &rest[pos + 3..];
        let url_end = after
            .find(|c: char| c.is_whitespace() || c == ')' || c == '"')
            .unwrap_or(after.len());
        let url = &after[..url_end];
        out.push_str(&rest[..pos + 3]);
        match url.find('?') {
            Some(q) => out.push_str(&url[..q]),
            None => out.push_str(url),
        }
        rest = &after[url_end..];
    }
    out.push_str(rest);
    out
}

/// 分词结果：word 参与归一化，delim 原样保留（保留标点才看得出模板结构）
enum Token<'a> {
    Word(&'a str),
    Delim(char),
}

/// 按「非字母数字下划线」切分，同时保留分隔符本身。
///
/// 不把 `-` 算进 word：否则 `us-east-1`、`claude-opus-5` 会被当成一个长 token，
/// 撞上 [`normalize_token`] 的长 id 规则被整体抹成 `<id>`，丢掉可读性。
fn split_keep_delims(s: &str) -> Vec<Token<'_>> {
    let mut out = Vec::new();
    let mut word_start: Option<usize> = None;
    for (idx, ch) in s.char_indices() {
        if ch.is_alphanumeric() || ch == '_' {
            word_start.get_or_insert(idx);
        } else {
            if let Some(st) = word_start.take() {
                out.push(Token::Word(&s[st..idx]));
            }
            out.push(Token::Delim(ch));
        }
    }
    if let Some(st) = word_start.take() {
        out.push(Token::Word(&s[st..]));
    }
    out
}

/// 单 token 归一化：长随机 id → `<id>`，其余数字 → `N`（连续数字折叠成一个 `N`）
fn normalize_token(w: &str) -> String {
    let digits = w.chars().filter(|c| c.is_ascii_digit()).count();
    if digits == 0 {
        return w.to_string();
    }
    let has_alpha = w.chars().any(|c| c.is_alphabetic());
    if has_alpha && w.chars().count() >= 12 && digits >= 4 {
        return "<id>".to_string();
    }
    let mut out = String::with_capacity(w.len());
    let mut prev_digit = false;
    for ch in w.chars() {
        if ch.is_ascii_digit() {
            if !prev_digit {
                out.push('N');
            }
            prev_digit = true;
        } else {
            prev_digit = false;
            out.push(ch);
        }
    }
    out
}

/// 交叉表的维度
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrosstabDimension {
    Credential,
    Proxy,
    Model,
    /// 上游端点（`cli` / `ide`）。
    ///
    /// 加这个维度的直接原因：实测 transient 类错误（占全部错误约一半）在控制模型
    /// 后，`cli` 端点的失败率是 `ide` 的两个数量级以上，而 credential 与 proxy
    /// 两个维度上看到的信号都只是它的下游投影——那张凭据恰好绑着 cli。
    /// 少了这个维度，面板会把人引向「禁用某张凭据/某个代理」，而真正的杠杆在端点。
    Endpoint,
}

impl CrosstabDimension {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "credential" => Some(Self::Credential),
            "proxy" => Some(Self::Proxy),
            "model" => Some(Self::Model),
            "endpoint" => Some(Self::Endpoint),
            _ => None,
        }
    }

    /// 可接受取值，供 handler 拼错误消息用。
    /// 单独抽出来是为了新增维度时不会漏改那条 400 文案（漏改的话，
    /// 用户会被告知一个已经支持的维度不被支持）。
    pub const ALL: [&'static str; 4] = ["credential", "proxy", "model", "endpoint"];

    fn as_str(self) -> &'static str {
        match self {
            Self::Credential => "credential",
            Self::Proxy => "proxy",
            Self::Model => "model",
            Self::Endpoint => "endpoint",
        }
    }
}

/// 交叉表里的一个维度桶
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrosstabBucket {
    /// 维度取值。credential 为 id 字符串；proxy 为 URL（`direct` = 直连，
    /// 空串 = 未知）；model 为模型名
    pub key: String,
    pub count: u64,
    /// 该桶在窗口内的**全部**请求数（成功+失败），即流量基线
    pub traffic: u64,
    /// 超额倍数 = （本桶错误数 / 该 error_type 总错误数）÷（本桶流量 / 窗口总流量）
    ///
    /// 为什么必须有这个数：裸计数和裸集中度都会被流量分布带偏。实测
    /// `claude-opus-5` 占全流量 63%，于是**每一种**错误都"集中"在它身上，
    /// 看着像它有问题，其实只是它承载得多。lift ≈ 1 表示该桶的错误份额与它的
    /// 流量份额相称（没有异常）；lift 明显 > 1 才是"这个对象错得不成比例"。
    ///
    /// `None` = 该桶流量为 0（窗口内只有错误没有流量记录，理论上不该出现），
    /// 此时无法计算比值，不填 0 以免被误读成"低于预期"。
    pub lift: Option<f64>,
}

/// 一个 error_type 在某维度上的分布
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CrosstabRow {
    pub error_type: String,
    /// 该 error_type 在窗口内的总数
    pub total: u64,
    /// 头部维度桶，降序
    pub buckets: Vec<CrosstabBucket>,
    /// 集中度 = 最大桶 / total，范围 (0, 1]。
    ///
    /// 接近 1 说明该错误压在单个对象上；接近 1/维度基数 说明均匀散开。
    ///
    /// **单看这个数会误判**：流量本身不均匀时，占流量最大的对象在每种错误上都会
    /// 显得"集中"。必须与桶上的 [`CrosstabBucket::lift`] 一起读 —— 集中度高
    /// 且 lift ≈ 1 只是流量使然；集中度高**且** lift 明显 > 1 才是真信号。
    pub concentration: f64,
    /// 该 error_type 覆盖到的不同维度取值数，用来判断 concentration 的分母量级
    pub distinct_keys: u64,
}

/// 一类归一化后的错误消息（指纹）
///
/// 为什么需要它：`error_type` 只有十几种取值，`error_message` 却是自由文本，
/// 面板上「同一个上游错误重复 400 次」与「400 个各不相同的错误」长得一样，
/// 而两者的处置动作相反（等上游恢复 vs 逐条排查）。实测 7 天窗口内 201 条
/// 非空消息只对应 15 个不同取值（13:1），归并后头部两三条就解释了大半错误量。
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorFingerprint {
    /// 归一化后的模板
    pub fingerprint: String,
    /// 从消息里提取出的 HTTP 状态码。
    ///
    /// 提取成独立字段而不是留在模板里，是为了让它先于数字归一化被摘走 ——
    /// 否则 `500 Internal Server Error` 与 `400 Bad Request` 会被并成一类，
    /// 而状态码恰是这类消息里最有区分度的一维。
    pub http_status: Option<u16>,
    pub count: u64,
    /// 1~2 条**原始未归一化**消息。
    ///
    /// 归一化规则出错时（把两个不同根因误并成一类），这是唯一能发现问题的途径：
    /// 只看模板永远看不出它盖住了什么。不可省略。
    pub samples: Vec<String>,
    /// 首末出现时间（RFC3339），用来区分「历史遗留」与「正在发生」
    pub first_seen: String,
    pub last_seen: String,
    /// 映射到该指纹的 error_type 列表。
    ///
    /// 长度 > 1 说明同一句消息被归到了不同 error_type —— 分类逻辑与消息内容
    /// 存在歧义，值得暴露而不是隐藏。
    pub error_types: Vec<String>,
}

/// 按小时的请求趋势点
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsTrendPoint {
    /// 桶起始（unix 秒，整点对齐）
    pub bucket_epoch: i64,
    pub total: u64,
    pub success: u64,
    pub error: u64,
    pub interrupted: u64,
    /// 该桶内 error_type → 计数，**稀疏**：只出现实际发生过的类型。
    ///
    /// 为什么要它：只有四个总数时，能看到「10 点错误涨了」，但看不到涨的是
    /// `stream_interrupted` 还是 `auth_failed` —— 突发事件和长期基线噪声
    /// 处置动作完全不同，混在一个数里就分不开。
    ///
    /// 为什么稀疏而不是补零：error_type 集合是运行时动态的（随代码增减），
    /// 实测 7 天内只出现 7 种、失败率约 2.2%，给每个桶把未出现的类型补 0
    /// 会产出大量无意义字段。空桶整个字段被省略，前端按「缺失即无错误」读。
    ///
    /// **口径警告：本字段各值之和 ≠ [`Self::error`]**。两者维度不同：
    /// `error` 是 `total - success - interrupted`，即 `final_status = 'error'`；
    /// 本字段按 `traces.error_type` 列分组，而实测 `error_type` 同时存在于
    /// `final_status = 'error'`（`transient` / `network_error` / `unknown` /
    /// `bad_request` / `upstream_truncated`）和 `final_status = 'interrupted'`
    /// （`stream_interrupted` / `client_disconnected`）两类状态上。
    /// 所以本字段之和 ≈ `error + interrupted`，而不是 `error`。
    /// 前端若拿本字段与 `error` 对账必然对不上，这不是 bug。
    #[serde(skip_serializing_if = "std::collections::HashMap::is_empty")]
    pub by_error_type: std::collections::HashMap<String, u64>,
}

/// 按凭据的窗口统计
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsCredentialRow {
    pub credential_id: u64,
    /// email 由 handler 层根据 token_manager 补充
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    pub total: u64,
    pub success: u64,
    pub error: u64,
    pub interrupted: u64,
    /// attempt 级失败分类
    pub auth_failed: u64,
    pub account_throttled: u64,
    pub network_error: u64,
    pub other_failed: u64,
}

/// 按代理的窗口统计（'' = 直连）
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OpsProxyRow {
    /// 代理 URL；空串表示直连
    pub proxy_url: String,
    pub attempts: u64,
    pub success: u64,
    pub network_error: u64,
    pub other_failed: u64,
    /// 最终中断/截断的 trace 数（按该 trace 成功跳所用代理归属）
    pub interrupted: u64,
}

/// 重试阶梯的一级（第 `attempt` 跳）
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryLadderStep {
    /// 第几跳，从 1 开始计（人读口径）。
    ///
    /// 注意与库里的 `trace_attempts.attempt` 差一：后者 0-based（provider 的
    /// `for attempt in 0..max_retries`）。阶梯对外用 1-based，是为了和
    /// `traces.total_attempts`（跳数，1 表示只跑了首跳）同口径 —— 这两个数
    /// 摆在一起看的场合远多于和 attempt 下标对照。
    pub attempt: u32,
    /// 到达这一跳的请求数，**累积量**。
    ///
    /// 跑了 4 跳的请求同时算作到达过第 1/2/3 跳，即 `total_attempts >= attempt`。
    /// 绝不能写成 `total_attempts = attempt`：那算的是"停在这一跳"，第 1 跳会
    /// 只剩下单跳完成的请求（实测 8813 / 9189），把分母缩掉近 4%，
    /// 于是每一跳的 `marginal_yield` 都被系统性抬高。
    pub reached: u64,
    /// 在这一跳成功的请求数，即 `total_attempts = attempt AND final_status = 'success'`。
    /// 与 `reached` 相反，这里必须是等值条件 —— 它要的就是"恰好停在这一跳且成功"。
    pub rescued: u64,
    /// 边际收益 = `rescued / reached`。
    ///
    /// 读法：到达这一跳的请求里，有多大比例是靠这一跳救回来的。这是决定
    /// "还值不值得再重试一次"的唯一指标 —— 累计救回数只会单调增，永远显得划算，
    /// 看不出第几跳开始白打。`reached = 0` 时为 0.0。
    pub marginal_yield: f64,
    /// 进入这一跳前的退避间隔中位数（毫秒）。
    ///
    /// `None` = 该跳没有任何一对相邻跳两端 `started_ms` 都非 NULL（见
    /// [`RetryEffectiveness::backoff_coverage`]）。第 1 跳恒为 `None`：它前面没有跳。
    pub backoff_p50_ms: Option<u64>,
}

/// 重试有效性统计（窗口内）。
///
/// 存在的理由：`MAX_TOTAL_RETRIES` 一直是拍出来的常数，库里每一跳都留了痕
/// （`trace_attempts`），但从没聚合回答过"重试到底救回了多少请求、第几跳开始白打"。
/// 没有这张表就只能在"多重试总比少重试好"的直觉上调参数，而实测边际收益
/// 在最后一跳掉到 ~0.15 —— 那一跳的绝大多数配额是在给注定失败的请求陪跑，
/// 且 429 场景下还会在账号间连环撞墙（见 `MAX_TOTAL_RETRIES` 上方的注释）。
#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryEffectiveness {
    pub window_hours: i64,
    /// 逐跳阶梯，按跳数升序。深度取窗口内实测最大跳数（见 `retry_effectiveness` 注释）
    pub steps: Vec<RetryLadderStep>,
    /// 重试净救回数 = 首跳没搞定、但最终成功的请求数（`total_attempts >= 2 AND success`）。
    /// 这是"有重试"与"没重试"两个世界的差值，也是重试机制的全部收益。
    pub total_rescued: u64,
    /// 跑完全部重试仍失败的请求数（到达最深一跳且最终非 success）。
    ///
    /// 与 `total_rescued` 配成一对读：前者是收益，后者是"最贵的那批失败"——
    /// 它们每个都占满了整份重试预算才放弃。
    pub total_exhausted: u64,
    /// 边际收益塌掉的跳数：首个 `marginal_yield < YIELD_COLLAPSE_THRESHOLD` 的跳。
    ///
    /// 只从第 2 跳起判定 —— 第 1 跳的 yield 就是整体成功率（实测 0.95），
    /// 它不是"重试的收益"，拿同一把尺子量没有意义。
    /// `None` = 没有任何一跳塌掉，当前重试预算还没打到收益悬崖。
    pub yield_collapse_at: Option<u32>,
    /// 退避间隔的可算比例 = 两端 `started_ms` 都非 NULL 的相邻跳对数 ÷ 相邻跳对总数。
    ///
    /// **这个字段是防静默低估的**：`started_ms` 是后加的列，历史行全是 NULL，
    /// 算不出间隔的跳对会被直接跳过。没有这个数的话，跨越那次 schema 变更的窗口
    /// 会拿极少数样本的中位数冒充全窗口的退避特征，而且看不出来。
    /// 实测 7 天窗口该值为 0.00（`started_ms` 只覆盖最近约 1.4 小时，且那段时间
    /// 没有任何多跳请求），此时 backoff 数字整体不可用。
    pub backoff_coverage: f64,
}

/// 退避间隔的中间结果（内部用，不出接口）。
/// 覆盖率与逐跳中位数必须一起返回：单看中位数无法判断它背后有多少样本。
struct BackoffSamples {
    coverage: f64,
    /// 1-based 跳数 → 进入该跳前的退避间隔中位数（毫秒）
    p50_by_hop: std::collections::BTreeMap<u32, u64>,
}

/// Ops DuckDB 存储。
///
/// 与 [`super::trace_db::TraceStore`] 共用同一个 kiro.duckdb（同一进程内
/// DuckDB 实例上的独立连接，MVCC 保证两个写入方互不阻塞）。
pub struct OpsStore {
    conn: Mutex<Connection>,
}

impl OpsStore {
    /// 打开（或复用）共享 kiro.duckdb。建表（含 ops_events）由
    /// [`super::duck::open_shared`] 完成。
    pub fn open(path: PathBuf) -> duckdb::Result<Self> {
        let conn = crate::admin::duck::open_shared(&path)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// 内存兜底（kiro.duckdb 打开失败时，事件仅进程内可见，聚合返回空）。
    /// 内存库不经过 open_shared，需自行执行建表——duck.rs SCHEMA 同时带出
    /// 空的 traces 系列表，让聚合查询稳定返回空集而不是报错。
    pub fn open_in_memory() -> duckdb::Result<Self> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch(crate::admin::duck::SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    /// 记录一条处置事件。失败仅 warn，不阻塞请求路径。
    pub fn record_event(&self, category: &str, severity: &str, subject: &str, message: &str) {
        let now = Utc::now();
        let res = self.conn.lock().execute(
            "INSERT INTO ops_events (ts, ts_epoch, category, severity, subject, message) \
             VALUES (?1,?2,?3,?4,?5,?6)",
            duckdb::params![
                now.to_rfc3339(),
                now.timestamp(),
                category,
                severity,
                subject,
                message
            ],
        );
        if let Err(e) = res {
            tracing::warn!("ops 事件写入失败: {}", e);
        }
    }

    /// 最近的处置事件（时间倒序）
    pub fn recent_events(&self, limit: usize) -> Vec<OpsEvent> {
        let limit = if limit == 0 { DEFAULT_EVENTS_LIMIT } else { limit.min(1000) };
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(
            "SELECT id, ts, category, severity, subject, message FROM ops_events \
             ORDER BY ts_epoch DESC, id DESC LIMIT ?1",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 事件查询失败: {}", e);
                return Vec::new();
            }
        };
        let rows = stmt.query_map([limit as i64], |row| {
            Ok(OpsEvent {
                id: row.get::<_, i64>(0)? as u64,
                ts: row.get(1)?,
                category: row.get(2)?,
                severity: row.get(3)?,
                subject: row.get(4)?,
                message: row.get(5)?,
            })
        });
        match rows {
            Ok(r) => r.flatten().collect(),
            Err(e) => {
                tracing::warn!("ops 事件查询失败: {}", e);
                Vec::new()
            }
        }
    }

    /// 清理过期事件（与 trace 保留天数对齐，由 main 的每日清理循环调用）
    pub fn cleanup(&self, retention_days: u64) {
        let cutoff = (Utc::now() - chrono::Duration::days(retention_days.max(1) as i64)).timestamp();
        match self
            .conn
            .lock()
            .execute("DELETE FROM ops_events WHERE ts_epoch < ?1", [cutoff])
        {
            Ok(n) if n > 0 => tracing::info!("已清理 {} 条过期 ops 事件", n),
            Ok(_) => {}
            Err(e) => tracing::warn!("ops 事件清理失败: {}", e),
        }
    }

    fn window_cutoff(hours: i64) -> i64 {
        let hours = hours.clamp(1, MAX_WINDOW_HOURS);
        Utc::now().timestamp() - hours * 3600
    }

    /// 窗口内总体概览
    pub fn overview(&self, hours: i64) -> OpsOverview {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut out = OpsOverview {
            window_hours: hours.clamp(1, MAX_WINDOW_HOURS),
            ..Default::default()
        };
        let head = conn.query_row(
            "SELECT COUNT(*), \
             COALESCE(SUM(final_status = 'success'), 0), \
             COALESCE(SUM(final_status = 'interrupted'), 0), \
             COALESCE(CAST(AVG(duration_ms) AS INTEGER), 0), \
             CAST(AVG(CASE WHEN is_stream = 1 THEN first_token_ms END) AS INTEGER) \
             FROM traces WHERE ts_epoch >= ?1",
            [cutoff],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, Option<i64>>(4)?,
                ))
            },
        );
        match head {
            Ok((total, success, interrupted, avg_dur, avg_ftt)) => {
                out.total = total as u64;
                out.success = success as u64;
                out.interrupted = interrupted as u64;
                out.error = (total - success - interrupted).max(0) as u64;
                out.avg_duration_ms = avg_dur.max(0) as u64;
                out.avg_first_token_ms = avg_ftt.map(|v| v.max(0) as u64);
            }
            Err(e) => {
                tracing::warn!("ops overview 查询失败: {}", e);
                return out;
            }
        }
        // 错误类型分布
        let mut stmt = match conn.prepare(
            "SELECT error_type, COUNT(*) FROM traces \
             WHERE ts_epoch >= ?1 AND error_type IS NOT NULL \
             GROUP BY error_type ORDER BY COUNT(*) DESC",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 错误分布查询失败: {}", e);
                return out;
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok(ErrorTypeCount {
                error_type: row.get(0)?,
                count: row.get::<_, i64>(1)? as u64,
            })
        }) {
            out.by_error_type = rows.flatten().collect();
        }
        // 中断类耗时分位（固定超时特征探测）。
        // 口径与原先的平均值完全一致（同一 error_type 过滤），只是换成分位，
        // 保证与历史窗口可比。
        out.interrupted_duration = conn
            .prepare(
                "SELECT duration_ms FROM traces \
                 WHERE ts_epoch >= ?1 \
                 AND error_type IN ('stream_interrupted', 'upstream_truncated')",
            )
            .and_then(|mut stmt| {
                let rows = stmt.query_map([cutoff], |row| row.get::<_, i64>(0))?;
                Ok(rows.flatten().map(|v| v.max(0) as u64).collect::<Vec<u64>>())
            })
            .map_err(|e| tracing::warn!("ops 中断耗时分位查询失败: {}", e))
            .ok()
            .and_then(percentiles);
        out
    }

    /// error_type × 维度 的交叉表。
    ///
    /// 回答「这个错误是全局的还是压在某个对象上」—— 一维边际统计
    /// （[`Self::overview`] 的 `by_error_type`）答不出这个，而两种情况的处置动作相反。
    ///
    /// **代理归属口径**：按该 trace 的最后一跳所用代理。已用真实数据核实过，
    /// 中断类 trace 的最后一跳 outcome 全为 `success`，即最后一跳就是成功跳，
    /// 因此这一条规则同时满足「失败归给失败发生处」与 [`Self::by_proxy`] 里
    /// 「中断归给成功跳」两个口径，无需按 error_type 分叉。
    pub fn error_crosstab(&self, hours: i64, dim: CrosstabDimension) -> Vec<CrosstabRow> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();

        // 最后一跳的关联。proxy 与 endpoint 都只存在于 trace_attempts 上，
        // 共用这一段；credential 与 model 直接取 traces 列，不需要 join。
        const LAST_HOP_JOIN: &str = " JOIN trace_attempts a ON a.trace_id = t.trace_id \
             AND a.attempt = (SELECT MAX(a2.attempt) FROM trace_attempts a2 \
                              WHERE a2.trace_id = t.trace_id)";
        let (key_expr, join) = match dim {
            CrosstabDimension::Credential => ("CAST(t.final_credential_id AS TEXT)", ""),
            CrosstabDimension::Model => ("t.model", ""),
            CrosstabDimension::Proxy => ("COALESCE(a.proxy_url, '')", LAST_HOP_JOIN),
            CrosstabDimension::Endpoint => ("COALESCE(a.endpoint, '')", LAST_HOP_JOIN),
        };
        let sql = format!(
            "SELECT t.error_type, {key} AS k, COUNT(*) AS c \
             FROM traces t{join} \
             WHERE t.ts_epoch >= ?1 AND t.error_type IS NOT NULL \
             GROUP BY t.error_type, k ORDER BY t.error_type ASC, c DESC",
            key = key_expr,
            join = join,
        );

        // 流量基线：同一维度上**全部**请求（不筛 error_type）的分布。
        // 没有它就无法区分"这个对象错得不成比例"与"这个对象承载得最多"。
        let baseline_sql = format!(
            "SELECT {key} AS k, COUNT(*) FROM traces t{join} \
             WHERE t.ts_epoch >= ?1 GROUP BY k",
            key = key_expr,
            join = join,
        );
        let mut traffic: std::collections::HashMap<String, u64> = std::collections::HashMap::new();
        match conn.prepare(&baseline_sql).and_then(|mut stmt| {
            let rows = stmt.query_map([cutoff], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
            })?;
            Ok(rows.flatten().collect::<Vec<_>>())
        }) {
            Ok(rows) => traffic.extend(rows),
            Err(e) => {
                // 基线拿不到时不假装能算 lift：后面 lift 会是 None，
                // 前端据此隐藏该列，而不是显示一个错的倍数。
                tracing::warn!("ops 交叉表基线查询失败（dim={}）: {}", dim.as_str(), e);
            }
        }
        let total_traffic: u64 = traffic.values().sum();

        let mut stmt = match conn.prepare(&sql) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 交叉表查询失败（dim={}）: {}", dim.as_str(), e);
                return Vec::new();
            }
        };
        let rows = match stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? as u64,
            ))
        }) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("ops 交叉表查询失败（dim={}）: {}", dim.as_str(), e);
                return Vec::new();
            }
        };

        // SQL 已按 (error_type, count DESC) 排好，这里顺序聚成每个 error_type 一行
        let mut out: Vec<CrosstabRow> = Vec::new();
        for (error_type, key, count) in rows.flatten() {
            let bucket = CrosstabBucket {
                traffic: traffic.get(&key).copied().unwrap_or(0),
                key,
                count,
                lift: None, // total 未知，等下面补
            };
            match out.last_mut() {
                Some(row) if row.error_type == error_type => {
                    row.total += count;
                    row.distinct_keys += 1;
                    if row.buckets.len() < CROSSTAB_TOP_BUCKETS {
                        row.buckets.push(bucket);
                    }
                }
                _ => out.push(CrosstabRow {
                    error_type,
                    total: count,
                    buckets: vec![bucket],
                    concentration: 0.0,
                    distinct_keys: 1,
                }),
            }
        }
        // 集中度与 lift 的分母都要等该 error_type 全部桶累完才知道
        for row in &mut out {
            let top = row.buckets.first().map(|b| b.count).unwrap_or(0);
            row.concentration = if row.total == 0 {
                0.0
            } else {
                top as f64 / row.total as f64
            };
            for b in &mut row.buckets {
                // lift = 错误份额 ÷ 流量份额
                b.lift = if row.total == 0 || total_traffic == 0 || b.traffic == 0 {
                    None
                } else {
                    let err_share = b.count as f64 / row.total as f64;
                    let traffic_share = b.traffic as f64 / total_traffic as f64;
                    Some(err_share / traffic_share)
                };
            }
        }
        out.sort_by_key(|row| std::cmp::Reverse(row.total));
        out
    }

    /// 错误消息按指纹归并，降序。
    ///
    /// 回答 [`Self::error_crosstab`] 答不出的那一问：同一个 `error_type` 里，
    /// 到底是一个上游错误刷了几百次，还是几百个互不相同的错误。前者等上游恢复，
    /// 后者要逐条排查 —— 不归并的话面板上这两种情况完全同形。
    ///
    /// 归并在 Rust 侧做而不是 SQL：归一化要按顺序做状态码提取、因果链截断、
    /// token 级数字抹平，SQLite 没有正则，拼 `REPLACE` 既读不懂也测不了。
    /// 窗口内消息量在千级以下，全取回内存聚合的代价可以忽略。
    pub fn error_fingerprints(&self, hours: i64, limit: usize) -> Vec<ErrorFingerprint> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(
            "SELECT error_message, COALESCE(error_type, ''), ts_epoch FROM traces \
             WHERE ts_epoch >= ?1 AND error_message IS NOT NULL AND error_message <> '' \
             ORDER BY ts_epoch ASC",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 错误指纹查询失败: {}", e);
                return Vec::new();
            }
        };
        let rows = match stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        }) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("ops 错误指纹查询失败: {}", e);
                return Vec::new();
            }
        };

        // key = (状态码, 模板)。状态码进 key 是关键：同一模板不同状态码必须分开。
        let mut acc: std::collections::HashMap<(Option<u16>, String), ErrorFingerprint> =
            std::collections::HashMap::new();
        for (message, error_type, ts_epoch) in rows.flatten() {
            let (http_status, fingerprint) = normalize_error_message(&message);
            let ts = chrono::DateTime::<Utc>::from_timestamp(ts_epoch, 0)
                .map(|t| t.to_rfc3339())
                .unwrap_or_default();
            let entry = acc
                .entry((http_status, fingerprint.clone()))
                .or_insert_with(|| ErrorFingerprint {
                    fingerprint,
                    http_status,
                    count: 0,
                    samples: Vec::new(),
                    // SQL 已按 ts 升序，首条即 first_seen
                    first_seen: ts.clone(),
                    last_seen: ts.clone(),
                    error_types: Vec::new(),
                });
            entry.count += 1;
            entry.last_seen = ts;
            // 样本取**互不相同**的原始串：两条一样的样本无法暴露误并
            if entry.samples.len() < FINGERPRINT_SAMPLES && !entry.samples.contains(&message) {
                entry.samples.push(message);
            }
            if !error_type.is_empty() && !entry.error_types.contains(&error_type) {
                entry.error_types.push(error_type);
            }
        }

        let mut out: Vec<ErrorFingerprint> = acc.into_values().collect();
        for row in &mut out {
            row.error_types.sort();
        }
        // count 相同时按 fingerprint 定序，保证输出稳定（HashMap 遍历序不定）
        out.sort_by(|a, b| {
            b.count
                .cmp(&a.count)
                .then_with(|| a.fingerprint.cmp(&b.fingerprint))
        });
        out.truncate(limit);
        out
    }

    /// 按小时的请求趋势，带 error_type 分层
    ///
    /// **为什么一条 SQL + 顺序聚合**，而不是「桶总数」与「桶×类型」两条查询：
    /// `GROUP BY bucket, error_type` 的结果里，四个总数完全可以由同一批行
    /// 累加得出（把每个桶的各类型行按 status 汇总即可），拆两条会让同一份
    /// 数据被扫两遍，还引入两次查询之间口径漂移的可能。代价是四个总数不再由
    /// SQL 直接给出，而是在 Rust 里累加 —— 但这样 `error` 的算法（由 total
    /// 减出）与改动前逐字一致，向后兼容不依赖「我重写的 SQL 恰好等价」。
    ///
    /// error_type 列没有索引，也不需要：9278 行量级下这是一次全表扫，
    /// 而原来按 bucket 分组同样是全表扫，加维度只是多了分组键，没有量级变化。
    pub fn error_trend(&self, hours: i64) -> Vec<OpsTrendPoint> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        // 按 (桶, error_type) 出计数，同时带上该组的 status 归属。
        // success 行的 error_type 为 NULL，用 '' 占位以免 GROUP BY 丢行。
        let mut stmt = match conn.prepare(
            "SELECT (ts_epoch // 3600) * 3600 AS bucket, \
             COALESCE(error_type, '') AS et, \
             COUNT(*), \
             COALESCE(SUM(final_status = 'success'), 0), \
             COALESCE(SUM(final_status = 'interrupted'), 0) \
             FROM traces WHERE ts_epoch >= ?1 \
             GROUP BY bucket, et ORDER BY bucket ASC",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 趋势查询失败: {}", e);
                return Vec::new();
            }
        };
        let rows = match stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, i64>(4)?,
            ))
        }) {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!("ops 趋势查询失败: {}", e);
                return Vec::new();
            }
        };

        // SQL 已按 bucket 升序，这里顺序聚成每桶一点
        let mut out: Vec<OpsTrendPoint> = Vec::new();
        for (bucket, error_type, count, success, interrupted) in rows.flatten() {
            if out.last().is_none_or(|p| p.bucket_epoch != bucket) {
                out.push(OpsTrendPoint {
                    bucket_epoch: bucket,
                    total: 0,
                    success: 0,
                    error: 0,
                    interrupted: 0,
                    by_error_type: std::collections::HashMap::new(),
                });
            }
            let point = out.last_mut().expect("non-empty after push");
            point.total += count.max(0) as u64;
            point.success += success.max(0) as u64;
            point.interrupted += interrupted.max(0) as u64;
            if !error_type.is_empty() {
                *point.by_error_type.entry(error_type).or_insert(0) += count.max(0) as u64;
            }
        }
        // error 仍旧由总数减出，与改动前口径一致（见 by_error_type 上的口径警告）
        for p in &mut out {
            p.error = p.total.saturating_sub(p.success).saturating_sub(p.interrupted);
        }
        out
    }

    /// 按凭据的窗口统计（trace 顶层 + attempt 级失败分类合并）
    pub fn by_credential(&self, hours: i64) -> Vec<OpsCredentialRow> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut map: std::collections::HashMap<u64, OpsCredentialRow> =
            std::collections::HashMap::new();

        let mut stmt = match conn.prepare(
            "SELECT final_credential_id, COUNT(*), \
             COALESCE(SUM(final_status = 'success'), 0), \
             COALESCE(SUM(error_type IN ('stream_interrupted', 'upstream_truncated')), 0) \
             FROM traces WHERE ts_epoch >= ?1 AND final_credential_id != 0 \
             GROUP BY final_credential_id",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 凭据统计查询失败: {}", e);
                return Vec::new();
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u64,
                row.get::<_, i64>(3)? as u64,
            ))
        }) {
            for (cred, total, success, interrupted) in rows.flatten() {
                let row = map.entry(cred).or_default();
                row.credential_id = cred;
                row.total = total;
                row.success = success;
                row.interrupted = interrupted;
                row.error = total.saturating_sub(success).saturating_sub(interrupted);
            }
        }

        let mut stmt = match conn.prepare(
            "SELECT a.credential_id, a.outcome, COUNT(*) FROM trace_attempts a \
             JOIN traces t ON t.trace_id = a.trace_id \
             WHERE t.ts_epoch >= ?1 AND a.credential_id != 0 AND a.outcome != 'success' \
             GROUP BY a.credential_id, a.outcome",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 凭据 attempt 统计查询失败: {}", e);
                return map.into_values().collect();
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, i64>(0)? as u64,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)? as u64,
            ))
        }) {
            for (cred, outcome, cnt) in rows.flatten() {
                let row = map.entry(cred).or_default();
                row.credential_id = cred;
                match outcome.as_str() {
                    "auth_failed" => row.auth_failed += cnt,
                    "account_throttled" => row.account_throttled += cnt,
                    "network_error" => row.network_error += cnt,
                    _ => row.other_failed += cnt,
                }
            }
        }

        let mut out: Vec<OpsCredentialRow> = map.into_values().collect();
        out.sort_by_key(|row| std::cmp::Reverse(row.total));
        out
    }

    /// 按代理的窗口统计。proxy_url 为 NULL 的 attempt 归入 ''（直连/未知）。
    pub fn by_proxy(&self, hours: i64) -> Vec<OpsProxyRow> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut map: std::collections::HashMap<String, OpsProxyRow> =
            std::collections::HashMap::new();

        let mut stmt = match conn.prepare(
            "SELECT COALESCE(a.proxy_url, ''), COUNT(*), \
             COALESCE(SUM(a.outcome = 'success'), 0), \
             COALESCE(SUM(a.outcome = 'network_error'), 0) \
             FROM trace_attempts a JOIN traces t ON t.trace_id = a.trace_id \
             WHERE t.ts_epoch >= ?1 GROUP BY COALESCE(a.proxy_url, '')",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 代理统计查询失败: {}", e);
                return Vec::new();
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u64,
                row.get::<_, i64>(3)? as u64,
            ))
        }) {
            for (url, attempts, success, network_error) in rows.flatten() {
                let row = map.entry(url.clone()).or_default();
                row.proxy_url = url;
                row.attempts = attempts;
                row.success = success;
                row.network_error = network_error;
                row.other_failed = attempts
                    .saturating_sub(success)
                    .saturating_sub(network_error);
            }
        }

        // 中断/截断 trace 按其成功跳（2xx 已拿到、随后断流）所用代理归属
        let mut stmt = match conn.prepare(
            "SELECT COALESCE(a.proxy_url, ''), COUNT(*) \
             FROM traces t JOIN trace_attempts a \
             ON a.trace_id = t.trace_id AND a.outcome = 'success' \
             WHERE t.ts_epoch >= ?1 \
             AND t.error_type IN ('stream_interrupted', 'upstream_truncated') \
             GROUP BY COALESCE(a.proxy_url, '')",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 代理中断统计查询失败: {}", e);
                return map.into_values().collect();
            }
        };
        if let Ok(rows) = stmt.query_map([cutoff], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as u64))
        }) {
            for (url, cnt) in rows.flatten() {
                let row = map.entry(url.clone()).or_default();
                row.proxy_url = url;
                row.interrupted += cnt;
            }
        }

        let mut out: Vec<OpsProxyRow> = map.into_values().collect();
        out.sort_by_key(|row| std::cmp::Reverse(row.attempts));
        out
    }

    /// 重试有效性阶梯：逐跳回答「到达这一跳的请求有多少、其中多少是靠这一跳救回的」。
    ///
    /// **口径全部建立在 `traces.total_attempts`（跳数）上，不数 `trace_attempts` 行数。**
    /// 两者在正常情况下一致，但前者是 trace 落库时一次性写定的权威跳数，
    /// 后者可能因 attempt 写入失败而缺行 —— 用它当分母会让阶梯凭空变浅。
    ///
    /// 阶梯深度取窗口内实测最大跳数（再压到 [`RETRY_LADDER_MAX_DEPTH`]），
    /// 而不是固定输出 4 级：预算是按分组账号数动态算的（见 `provider.rs` 的
    /// `retry_budget`），小分组根本到不了第 4 跳，硬输出 4 级只会多两行
    /// `reached = 0` 的空行，还会让 `total_exhausted` 的"最深一跳"错位到没人到过的跳上。
    ///
    /// backoff 部分单独降级：它依赖后加的 `started_ms` 列，覆盖率由
    /// [`RetryEffectiveness::backoff_coverage`] 单独报出，不与阶梯的可信度绑在一起。
    pub fn retry_effectiveness(&self, hours: i64) -> RetryEffectiveness {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut out = RetryEffectiveness {
            window_hours: hours.clamp(1, MAX_WINDOW_HOURS),
            // 无多跳请求时 backoff 没有任何待算的跳对，覆盖率按"该算的都算了"记 1.0；
            // 记 0.0 会被误读成"数据丢了"，而这里根本没有数据需要算。
            backoff_coverage: 1.0,
            ..Default::default()
        };

        // (跳数 → 该跳数下的 trace 总数, 其中 success 数)。
        // GREATEST(total_attempts, 1)：任何落库的 trace 至少跑过一跳，把异常的 0
        // 归到第 1 跳，保证「第 1 跳 reached == 窗口内 trace 总数」这条恒等式
        // 在脏数据下也不破 —— 它是这张表的自检锚点。
        let mut buckets: std::collections::BTreeMap<u32, (u64, u64)> =
            std::collections::BTreeMap::new();
        let mut stmt = match conn.prepare(
            "SELECT GREATEST(total_attempts, 1) AS hops, \
             COUNT(*), COALESCE(SUM(final_status = 'success'), 0) \
             FROM traces WHERE ts_epoch >= ?1 GROUP BY hops ORDER BY hops",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 重试阶梯查询失败: {}", e);
                return out;
            }
        };
        match stmt.query_map([cutoff], |row| {
            Ok((
                row.get::<_, i64>(0)?.max(1) as u32,
                row.get::<_, i64>(1)? as u64,
                row.get::<_, i64>(2)? as u64,
            ))
        }) {
            Ok(rows) => {
                for (hops, total, success) in rows.flatten() {
                    let e = buckets.entry(hops).or_insert((0, 0));
                    e.0 += total;
                    e.1 += success;
                }
            }
            Err(e) => {
                tracing::warn!("ops 重试阶梯读取失败: {}", e);
                return out;
            }
        }
        if buckets.is_empty() {
            return out;
        }

        let depth = buckets
            .keys()
            .copied()
            .max()
            .unwrap_or(1)
            .clamp(1, RETRY_LADDER_MAX_DEPTH);

        // 退避间隔：先算，供逐跳填充。库里 attempt 是 0-based，+1 换成阶梯的 1-based。
        let backoff = self.collect_backoff(&conn, cutoff);
        out.backoff_coverage = backoff.coverage;

        for hop in 1..=depth {
            // 累积到达数：跳数 >= hop 的全部 trace。深度被 RETRY_LADDER_MAX_DEPTH
            // 截断时，超出的桶仍要计入最深一跳的 reached，否则会漏掉请求。
            let reached: u64 = buckets
                .iter()
                .filter(|(h, _)| **h >= hop)
                .map(|(_, (total, _))| *total)
                .sum();
            let rescued = buckets.get(&hop).map(|(_, s)| *s).unwrap_or(0);
            out.steps.push(RetryLadderStep {
                attempt: hop,
                reached,
                rescued,
                marginal_yield: if reached == 0 {
                    0.0
                } else {
                    rescued as f64 / reached as f64
                },
                backoff_p50_ms: backoff.p50_by_hop.get(&hop).copied(),
            });
        }

        // 净收益：首跳没搞定但最终成功。这才是"有重试 vs 没重试"的差值。
        out.total_rescued = buckets
            .iter()
            .filter(|(h, _)| **h >= 2)
            .map(|(_, (_, success))| *success)
            .sum();
        // 最贵的那批失败：占满整份重试预算仍未成功。
        out.total_exhausted = buckets
            .iter()
            .filter(|(h, _)| **h >= depth)
            .map(|(_, (total, success))| total.saturating_sub(*success))
            .sum();
        // 从第 2 跳起找首个塌点：第 1 跳的 yield 是整体成功率，不是重试的收益。
        out.yield_collapse_at = out
            .steps
            .iter()
            .find(|s| s.attempt >= 2 && s.reached > 0 && s.marginal_yield < YIELD_COLLAPSE_THRESHOLD)
            .map(|s| s.attempt);
        out
    }

    /// 相邻跳的退避间隔样本 + 覆盖率。
    ///
    /// 间隔 = `后一跳.started_ms - (前一跳.started_ms + 前一跳.duration_ms)`，
    /// 即扣掉前一跳自身耗时后剩下的纯等待。直接减 `started_ms` 之差会把上游
    /// 耗时（可达几十秒）算进退避，得出的数字与 `retry_delay` 毫无关系。
    ///
    /// 只取两端 `started_ms` 都非 NULL 的跳对；分母是相邻跳对总数，
    /// 二者之比即覆盖率（该列是后加的，历史行为 NULL）。
    fn collect_backoff(&self, conn: &Connection, cutoff: i64) -> BackoffSamples {
        let mut out = BackoffSamples {
            coverage: 1.0,
            p50_by_hop: std::collections::BTreeMap::new(),
        };
        // 覆盖率的分子分母一次查出，保证两者口径（同一 JOIN、同一窗口）严格一致。
        let counted = conn.query_row(
            "SELECT COUNT(*), \
             COALESCE(SUM(a.started_ms IS NOT NULL AND b.started_ms IS NOT NULL), 0) \
             FROM trace_attempts a \
             JOIN trace_attempts b ON b.trace_id = a.trace_id AND b.attempt = a.attempt + 1 \
             JOIN traces t ON t.trace_id = a.trace_id \
             WHERE t.ts_epoch >= ?1",
            [cutoff],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        );
        match counted {
            Ok((total_pairs, usable_pairs)) => {
                out.coverage = if total_pairs <= 0 {
                    1.0
                } else {
                    usable_pairs as f64 / total_pairs as f64
                };
                if usable_pairs <= 0 {
                    return out;
                }
            }
            Err(e) => {
                tracing::warn!("ops 退避覆盖率查询失败: {}", e);
                // 覆盖率算不出来时不假装 1.0：报 0.0 并让所有 p50 缺席，
                // 前端据此隐藏 backoff 而不是显示一个来源不明的中位数。
                out.coverage = 0.0;
                return out;
            }
        }

        let mut samples: std::collections::BTreeMap<u32, Vec<u64>> =
            std::collections::BTreeMap::new();
        let mut stmt = match conn.prepare(
            "SELECT b.attempt, b.started_ms - (a.started_ms + a.duration_ms) AS gap \
             FROM trace_attempts a \
             JOIN trace_attempts b ON b.trace_id = a.trace_id AND b.attempt = a.attempt + 1 \
             JOIN traces t ON t.trace_id = a.trace_id \
             WHERE t.ts_epoch >= ?1 \
             AND a.started_ms IS NOT NULL AND b.started_ms IS NOT NULL",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("ops 退避间隔查询失败: {}", e);
                return out;
            }
        };
        match stmt.query_map([cutoff], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?))
        }) {
            Ok(rows) => {
                for (attempt_0based, gap) in rows.flatten() {
                    // 0-based attempt → 1-based 跳数；间隔归给"进入的那一跳"
                    let hop = (attempt_0based.max(0) as u32).saturating_add(1);
                    // 负间隔只可能来自时钟/记账误差，钳到 0 而非丢弃：
                    // 丢弃会让覆盖率与实际参与计算的样本数不一致。
                    samples.entry(hop).or_default().push(gap.max(0) as u64);
                }
            }
            Err(e) => {
                tracing::warn!("ops 退避间隔读取失败: {}", e);
                return out;
            }
        }
        for (hop, v) in samples {
            if let Some(d) = percentiles(v) {
                out.p50_by_hop.insert(hop, d.p50);
            }
        }
        out
    }

    /// 按 (段, 出口) 统计窗口内的失败率。
    /// 出口取该 trace 最后一跳的 proxy_url —— 流生命周期发生在最终成功建连的那一跳上。
    pub fn phase_baseline(&self, hours: i64) -> Vec<PhaseBaselineRow> {
        let cutoff = Self::window_cutoff(hours);
        let conn = self.conn.lock();
        let mut stmt = match conn.prepare(
            "SELECT p.phase, \
                    COALESCE((SELECT a.proxy_url FROM trace_attempts a \
                              WHERE a.trace_id = p.trace_id \
                              ORDER BY a.attempt DESC LIMIT 1), '') AS proxy, \
                    COUNT(*) AS total, \
                    SUM(CASE WHEN p.outcome NOT IN ('success', 'client_disconnected') THEN 1 ELSE 0 END) AS failed \
             FROM trace_phases p \
             JOIN traces t ON t.trace_id = p.trace_id \
             WHERE t.ts_epoch >= ?1 \
             GROUP BY p.phase, proxy",
        ) {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!("phase_baseline 查询失败: {}", e);
                return Vec::new();
            }
        };
        let rows = stmt.query_map([cutoff], |row| {
            Ok(PhaseBaselineRow {
                phase: row.get(0)?,
                proxy_url: row.get(1)?,
                total: row.get::<_, i64>(2)? as u64,
                failed: row.get::<_, i64>(3)? as u64,
            })
        });
        match rows {
            Ok(r) => r.filter_map(|x| x.ok()).collect(),
            Err(e) => {
                tracing::warn!("phase_baseline 读取失败: {}", e);
                Vec::new()
            }
        }
    }
}

/// 某段 × 某出口的窗口失败率，用于错误详情里的对照基线
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PhaseBaselineRow {
    pub phase: String,
    /// 出口：'direct' = 直连；'' = 未知（该列存在前的历史行）
    pub proxy_url: String,
    pub total: u64,
    pub failed: u64,
}

/// 请求路径上的运维反馈编排。
///
/// 挂在 provider（网络错误/成功）与流处理（中断）上；所有方法都不阻塞、
/// 不返回错误——运维反馈失败不能影响正常请求。
pub struct OpsRuntime {
    pool: Arc<ProxyPoolManager>,
    token_manager: Arc<MultiTokenManager>,
    events: Arc<OpsStore>,
}

impl OpsRuntime {
    pub fn new(
        pool: Arc<ProxyPoolManager>,
        token_manager: Arc<MultiTokenManager>,
        events: Arc<OpsStore>,
    ) -> Self {
        Self {
            pool,
            token_manager,
            events,
        }
    }

    pub fn events(&self) -> &Arc<OpsStore> {
        &self.events
    }

    /// 真实请求经代理成功：清零该代理的请求级失败计数
    pub fn report_proxy_success(&self, proxy_url: Option<&str>) {
        if let Some(url) = proxy_url {
            self.pool.report_request_success(url);
        }
    }

    /// 真实请求经代理失败（网络错误 / 流中断）：累计并按需自动禁用 + 解绑换绑
    pub fn report_proxy_failure(&self, proxy_url: Option<&str>, error: &str) {
        let Some(url) = proxy_url else { return };
        let Some(disabled) = self.pool.report_request_failure(url, error) else {
            return;
        };
        self.events.record_event(
            event_category::PROXY_AUTO_DISABLE,
            "error",
            &format!("proxy#{}", disabled.id),
            &format!(
                "代理 {} 连续 {} 次真实请求失败，已自动禁用（最近错误: {}）",
                disabled.url,
                disabled.request_failures,
                error.chars().take(200).collect::<String>()
            ),
        );
        self.reassign_after_disable(&disabled.url, disabled.id);
    }

    /// 流中断反馈：计入所用代理的请求级失败（直连中断只落 trace 统计，无处置动作）
    pub fn report_stream_interrupted(
        &self,
        credential_id: u64,
        proxy_url: Option<&str>,
        error: &str,
    ) {
        tracing::debug!(
            "流中断反馈: 凭据 #{} 代理 {:?}",
            credential_id,
            proxy_url
        );
        self.report_proxy_failure(proxy_url, error);
    }

    /// 健康检查自动禁用后的补充处置（由 AdminService 的探测循环调用）：
    /// 记事件并解绑受影响凭据。
    pub fn handle_probe_auto_disable(&self, proxy_id: u64, proxy_url: &str) {
        self.events.record_event(
            event_category::PROXY_PROBE_DISABLE,
            "error",
            &format!("proxy#{}", proxy_id),
            &format!("代理 {} 连续探测失败，已自动禁用", proxy_url),
        );
        self.reassign_after_disable(proxy_url, proxy_id);
    }

    /// 恢复探针把代理放回账号池后：只记事件。
    ///
    /// **不换绑凭据**——当初被 [`Self::reassign_after_disable`] 迁走的凭据留在新代理上。
    /// 恢复的是「可用性」（重新进入可分配池），不是「谁绑在谁上」。它们已经在新代理上
    /// 跑得好好的，再搬一次只是多一次扰动。
    pub fn handle_probe_auto_recover(&self, proxy_id: u64, proxy_url: &str) {
        tracing::info!("代理 #{} 已通过恢复探测，放回账号池: {}", proxy_id, proxy_url);
        self.events.record_event(
            event_category::PROXY_AUTO_RECOVER,
            "info",
            &format!("proxy#{}", proxy_id),
            &format!("代理 {} 连续探测成功，已自动放回账号池（凭据绑定不变）", proxy_url),
        );
    }

    /// 代理被自动禁用后：把绑定它的凭据换绑到池内其它可用代理（无可用则清除、
    /// 回退全局代理/直连），并记录处置事件。
    fn reassign_after_disable(&self, disabled_url: &str, proxy_id: u64) {
        let replacement = self
            .pool
            .assignable_urls()
            .into_iter()
            .find(|u| u != disabled_url);
        match self
            .token_manager
            .reassign_proxy_url(disabled_url, replacement.clone())
        {
            Ok(affected) if affected.is_empty() => {}
            Ok(affected) => {
                let target = replacement.as_deref().unwrap_or(
                if crate::http_client::require_proxy() {
                    "（无可用代理；requireProxy 已开启，这些凭据的请求将被拒绝而非裸连）"
                } else {
                    "（无可用代理，回退全局/直连）"
                },
            );
                tracing::warn!(
                    "代理 #{} 自动禁用：凭据 {:?} 已换绑到 {}",
                    proxy_id,
                    affected,
                    target
                );
                self.events.record_event(
                    event_category::PROXY_REASSIGN,
                    "warn",
                    &format!("proxy#{}", proxy_id),
                    &format!(
                        "受影响凭据 {} 已从 {} 换绑到 {}",
                        affected
                            .iter()
                            .map(|id| format!("#{}", id))
                            .collect::<Vec<_>>()
                            .join(", "),
                        disabled_url,
                        target
                    ),
                );
            }
            Err(e) => {
                tracing::warn!("代理 #{} 自动禁用后换绑凭据失败: {}", proxy_id, e);
                self.events.record_event(
                    event_category::PROXY_REASSIGN,
                    "error",
                    &format!("proxy#{}", proxy_id),
                    &format!("换绑凭据失败: {}", e),
                );
            }
        }
    }
}

/// 共享句柄
pub type SharedOpsRuntime = Arc<OpsRuntime>;

#[cfg(test)]
mod tests {
    use super::*;

    fn mem_store_with_traces() -> OpsStore {
        OpsStore::open_in_memory().unwrap()
    }

    fn insert_trace(
        store: &OpsStore,
        trace_id: &str,
        epoch_offset_secs: i64,
        final_status: &str,
        error_type: Option<&str>,
        credential_id: u64,
        duration_ms: u64,
    ) {
        let epoch = Utc::now().timestamp() - epoch_offset_secs;
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO traces (trace_id, ts, ts_epoch, key_id, key_source, model, is_stream, \
                 final_status, final_credential_id, error_type, total_attempts, duration_ms) \
                 VALUES (?1, '2026', ?2, 1, 'clientKey', 'm', 1, ?3, ?4, ?5, 1, ?6)",
                duckdb::params![
                    trace_id,
                    epoch,
                    final_status,
                    credential_id as i64,
                    error_type,
                    duration_ms as i64
                ],
            )
            .unwrap();
    }

    fn insert_attempt(
        store: &OpsStore,
        trace_id: &str,
        outcome: &str,
        credential_id: u64,
        proxy_url: Option<&str>,
    ) {
        insert_attempt_on(store, trace_id, outcome, credential_id, proxy_url, "ide");
    }

    /// 可指定端点的变体。真实库里目前只有 `ide` 一个端点，端点维度的行为
    /// 无法用真实数据覆盖，只能在测试里构造多端点场景。
    fn insert_attempt_on(
        store: &OpsStore,
        trace_id: &str,
        outcome: &str,
        credential_id: u64,
        proxy_url: Option<&str>,
        endpoint: &str,
    ) {
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO trace_attempts (trace_id, attempt, credential_id, endpoint, \
                 outcome, duration_ms, proxy_url) VALUES (?1, 0, ?2, ?3, ?4, 100, ?5)",
                duckdb::params![trace_id, credential_id as i64, endpoint, outcome, proxy_url],
            )
            .unwrap();
    }

    /// 带 total_attempts 的 trace 插入。重试阶梯的口径完全建立在这一列上，
    /// 而 [`insert_trace`] 把它硬编码为 1，故单独开一个。
    fn insert_trace_hops(
        store: &OpsStore,
        trace_id: &str,
        final_status: &str,
        total_attempts: u32,
    ) {
        let epoch = Utc::now().timestamp() - 60;
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO traces (trace_id, ts, ts_epoch, key_id, key_source, model, is_stream, \
                 final_status, final_credential_id, error_type, total_attempts, duration_ms) \
                 VALUES (?1, '2026', ?2, 1, 'clientKey', 'm', 1, ?3, 1, NULL, ?4, 100)",
                duckdb::params![trace_id, epoch, final_status, total_attempts as i64],
            )
            .unwrap();
    }

    /// 带 attempt 下标与 started_ms 的 attempt 插入（`started_ms = None` 模拟历史行）
    fn insert_attempt_at(
        store: &OpsStore,
        trace_id: &str,
        attempt: u32,
        outcome: &str,
        started_ms: Option<u64>,
        duration_ms: u64,
    ) {
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO trace_attempts (trace_id, attempt, credential_id, endpoint, \
                 outcome, duration_ms, started_ms, proxy_url) \
                 VALUES (?1, ?2, 1, 'ide', ?3, ?4, ?5, NULL)",
                duckdb::params![
                    trace_id,
                    attempt as i64,
                    outcome,
                    duration_ms as i64,
                    started_ms.map(|v| v as i64)
                ],
            )
            .unwrap();
    }

    fn insert_phase(store: &OpsStore, trace_id: &str, seq: u32, phase: &str, outcome: &str) {
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO trace_phases (trace_id, seq, phase, started_ms, duration_ms, outcome) \
                 VALUES (?1, ?2, ?3, 0, 100, ?4)",
                duckdb::params![trace_id, seq as i64, phase, outcome],
            )
            .unwrap();
    }

    #[test]
    fn events_roundtrip_and_cleanup() {
        let store = mem_store_with_traces();
        store.record_event(event_category::PROXY_AUTO_DISABLE, "error", "proxy#1", "msg");
        let events = store.recent_events(10);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].category, event_category::PROXY_AUTO_DISABLE);
        store.cleanup(30);
        assert_eq!(store.recent_events(10).len(), 1, "未过期事件不应被清理");
    }

    #[test]
    fn overview_counts_statuses_and_interrupted_duration() {
        let store = mem_store_with_traces();
        insert_trace(&store, "t1", 60, "success", None, 1, 1000);
        insert_trace(&store, "t2", 60, "interrupted", Some("stream_interrupted"), 1, 245_000);
        insert_trace(&store, "t3", 60, "error", Some("upstream_truncated"), 2, 247_000);
        insert_trace(&store, "t4", 60, "error", Some("auth_failed"), 2, 500);
        // 窗口外的记录不计
        insert_trace(&store, "t-old", 3600 * 48, "error", Some("auth_failed"), 2, 500);

        let o = store.overview(24);
        assert_eq!(o.total, 4);
        assert_eq!(o.success, 1);
        assert_eq!(o.interrupted, 1);
        assert_eq!(o.error, 2);
        // 中断类耗时分位：样本 [245000, 247000]，n=2。
        // 最近秩取值（无插值）：p50 的秩 = (2-1)*0.5 = 0.5，round 进位到 1 → 取上界。
        // 偶数样本没有真正的中位数，不插值就必然偏向一侧，这是刻意取舍。
        let d = o.interrupted_duration.expect("应有中断样本");
        assert_eq!(d.n, 2);
        assert_eq!(d.p50, 247_000);
        assert_eq!(d.p99, 247_000);
        let truncated = o
            .by_error_type
            .iter()
            .find(|e| e.error_type == "upstream_truncated")
            .unwrap();
        assert_eq!(truncated.count, 1);
    }

    /// 分位替代平均值的理由必须可执行验证：双簇样本下平均值落在两簇之间的
    /// 空档（任何真实样本都不在那里），而 p50/p99 各自落在真实簇上。
    /// 这正是实测 ~240s 与 ~720s 两簇的情形。
    #[test]
    fn percentiles_expose_bimodal_clusters_that_average_would_hide() {
        // 6 个 240s 簇 + 3 个 720s 簇
        let mut v: Vec<u64> = vec![240_000; 6];
        v.extend(vec![720_000; 3]);
        let avg = v.iter().sum::<u64>() / v.len() as u64;
        let d = percentiles(v).unwrap();

        assert_eq!(d.n, 9);
        assert_eq!(d.p50, 240_000, "p50 落在主簇");
        assert_eq!(d.p99, 720_000, "p99 暴露次簇");
        // 平均值 400s 既不在 240 簇也不在 720 簇 —— 它描述的是不存在的请求
        assert_eq!(avg, 400_000);
        assert!(
            avg != d.p50 && avg != d.p99,
            "平均值落在两簇之间的空档，会抹掉'存在两个固定超时'这个结论"
        );
    }

    /// 交叉表的全部价值在于区分「压在单个对象上」与「均匀散开」，
    /// 因为两者的处置动作相反。这个测试把两种形态放在同一窗口里对比。
    #[test]
    fn crosstab_separates_concentrated_from_spread_errors() {
        let store = mem_store_with_traces();
        // network_error：4 条全压在凭据 7 上 → 集中，处置=换掉这个凭据/它的代理
        for (i, id) in [(0, 7), (1, 7), (2, 7), (3, 7)] {
            insert_trace(
                &store,
                &format!("conc{}", i),
                60,
                "error",
                Some("network_error"),
                id,
                500,
            );
        }
        // transient：4 条散在 4 个不同凭据 → 系统性，换单个凭据无效
        for (i, id) in [(0, 1), (1, 2), (2, 3), (3, 4)] {
            insert_trace(
                &store,
                &format!("spread{}", i),
                60,
                "error",
                Some("transient"),
                id,
                500,
            );
        }

        let rows = store.error_crosstab(24, CrosstabDimension::Credential);
        let conc = rows
            .iter()
            .find(|r| r.error_type == "network_error")
            .expect("应有 network_error 行");
        let spread = rows
            .iter()
            .find(|r| r.error_type == "transient")
            .expect("应有 transient 行");

        assert_eq!(conc.total, 4);
        assert_eq!(conc.distinct_keys, 1);
        assert!(
            (conc.concentration - 1.0).abs() < 1e-9,
            "全压在一个凭据上，集中度应为 1"
        );
        assert_eq!(conc.buckets[0].key, "7");

        assert_eq!(spread.total, 4);
        assert_eq!(spread.distinct_keys, 4);
        assert!(
            (spread.concentration - 0.25).abs() < 1e-9,
            "均匀散在 4 个凭据上，集中度应为 1/4"
        );

        assert!(
            conc.concentration > spread.concentration,
            "集中度必须能把两种形态分开，否则这张表没有意义"
        );
    }

    /// 趋势分层：同一小时内不同 error_type 必须分开计，且原有四个总数不变。
    ///
    /// 「原有四个数不变」是这份改动的向后兼容底线 —— 前端 TrendChart 依赖它们。
    /// SQL 从 `GROUP BY bucket` 改成 `GROUP BY bucket, error_type` 后行数变多，
    /// 若累加写错，total/success/interrupted 会被重复计或漏计。
    #[test]
    fn trend_splits_error_types_without_changing_totals() {
        let store = mem_store_with_traces();
        // 同一小时内：2 成功 + 2 个不同 error_type + 1 个中断类
        insert_trace(&store, "s1", 60, "success", None, 1, 100);
        insert_trace(&store, "s2", 60, "success", None, 1, 100);
        insert_trace(&store, "e1", 60, "error", Some("transient"), 1, 100);
        insert_trace(&store, "e2", 60, "error", Some("network_error"), 1, 100);
        insert_trace(&store, "i1", 60, "interrupted", Some("stream_interrupted"), 1, 100);

        let pts = store.error_trend(24);
        assert_eq!(pts.len(), 1, "五条同小时记录应聚成一个桶");
        let p = &pts[0];

        // 四个总数：改动前的口径
        assert_eq!(p.total, 5);
        assert_eq!(p.success, 2);
        assert_eq!(p.interrupted, 1);
        assert_eq!(p.error, 2, "error = total - success - interrupted");

        // 分层：两个 error 类 + 一个 interrupted 类，各自独立
        assert_eq!(p.by_error_type.get("transient"), Some(&1));
        assert_eq!(p.by_error_type.get("network_error"), Some(&1));
        assert_eq!(p.by_error_type.get("stream_interrupted"), Some(&1));
        assert_eq!(p.by_error_type.len(), 3);
        assert!(
            !p.by_error_type.contains_key(""),
            "success 行的 NULL error_type 不得作为一个类型泄漏进来"
        );

        // 口径警告的可执行版本：之和 = error + interrupted，不等于 error。
        // 前端拿这个字段与 error 对账必然对不上，这里把它钉成契约而非 bug。
        let sum: u64 = p.by_error_type.values().sum();
        assert_eq!(sum, p.error + p.interrupted, "之和应等于 error + interrupted");
        assert_ne!(sum, p.error, "之和不等于 error —— 这是预期行为");
    }

    /// 全成功的桶必须整个省略 by_error_type（稀疏设计的另一半）。
    /// 若空 map 也序列化出去，每个正常小时都会多带一个空对象。
    #[test]
    fn trend_omits_error_map_for_clean_buckets() {
        let store = mem_store_with_traces();
        insert_trace(&store, "ok1", 60, "success", None, 1, 100);
        insert_trace(&store, "ok2", 60, "success", None, 1, 100);

        let pts = store.error_trend(24);
        assert_eq!(pts.len(), 1);
        assert!(pts[0].by_error_type.is_empty(), "无错误的桶该是空 map");
        // skip_serializing_if 生效：JSON 里不该出现该字段
        let json = serde_json::to_string(&pts[0]).unwrap();
        assert!(
            !json.contains("byErrorType"),
            "空 map 应被 skip_serializing_if 省略，实际: {}",
            json
        );
    }

    /// 跨小时的记录必须分到不同桶，且各桶的分层互不污染。
    /// 顺序聚合（依赖 SQL 的 bucket 升序）写错时，最典型的症状就是后一桶把前一桶
    /// 的类型累计进去。
    #[test]
    fn trend_keeps_buckets_independent_across_hours() {
        let store = mem_store_with_traces();
        // 60 秒前与 2 小时前，落在不同小时桶
        insert_trace(&store, "now-e", 60, "error", Some("transient"), 1, 100);
        insert_trace(
            &store,
            "old-e",
            3600 * 2 + 60,
            "error",
            Some("network_error"),
            1,
            100,
        );

        let pts = store.error_trend(24);
        assert_eq!(pts.len(), 2, "应分成两个桶");
        // SQL 按 bucket 升序 → 早的在前
        assert!(pts[0].bucket_epoch < pts[1].bucket_epoch);
        assert_eq!(pts[0].by_error_type.get("network_error"), Some(&1));
        assert!(
            !pts[0].by_error_type.contains_key("transient"),
            "早桶不得含晚桶的类型"
        );
        assert_eq!(pts[1].by_error_type.get("transient"), Some(&1));
        assert!(
            !pts[1].by_error_type.contains_key("network_error"),
            "晚桶不得累计早桶的类型"
        );
    }

    /// endpoint 维度：错误集中在某个端点时必须能被区分出来。
    ///
    /// 这个维度在当前生产数据上没有区分力（实测 `trace_attempts.endpoint` 只有
    /// `ide` 与空串两个取值，全部 transient 错误都在 ide 上、lift 恰为 1.00），
    /// 所以行为只能靠构造数据覆盖 —— 这也正是加测试而非依赖线上观察的理由。
    #[test]
    fn crosstab_endpoint_dimension_separates_endpoints() {
        let store = mem_store_with_traces();
        // cli 端点：3 条 transient（少量流量、全是错）
        for i in 0..3 {
            let id = format!("cli{}", i);
            insert_trace(&store, &id, 60, "error", Some("transient"), 1, 100);
            insert_attempt_on(&store, &id, "transient", 1, None, "cli");
        }
        // ide 端点：1 条 transient + 6 条成功
        insert_trace(&store, "ide-err", 60, "error", Some("transient"), 1, 100);
        insert_attempt_on(&store, "ide-err", "transient", 1, None, "ide");
        for i in 0..6 {
            let id = format!("ide-ok{}", i);
            insert_trace(&store, &id, 60, "success", None, 1, 100);
            insert_attempt_on(&store, &id, "success", 1, None, "ide");
        }

        let rows = store.error_crosstab(24, CrosstabDimension::Endpoint);
        let tr = rows
            .iter()
            .find(|r| r.error_type == "transient")
            .expect("应有 transient 行");
        assert_eq!(tr.total, 4);

        let cli = tr.buckets.iter().find(|b| b.key == "cli").unwrap();
        let ide = tr.buckets.iter().find(|b| b.key == "ide").unwrap();
        assert_eq!(cli.traffic, 3, "cli 端点流量只有那 3 条");
        assert_eq!(ide.traffic, 7, "ide 端点流量含成功的 6 条");
        // cli：错误份额 3/4=75%，流量份额 3/10=30% → lift 2.5
        let cli_lift = cli.lift.unwrap();
        assert!(
            (cli_lift - 2.5).abs() < 1e-9,
            "cli lift 应为 2.5，实际 {}",
            cli_lift
        );
        assert!(
            cli_lift > ide.lift.unwrap(),
            "错误集中的端点 lift 必须高于另一端点"
        );
    }

    /// 端点为空串（请求未走到上游、没有端点可记）时不能崩、也不能被当成一个
    /// 正常端点混进排名。实测生产库里这类行有 72 条，全部属于凭据 0。
    #[test]
    fn crosstab_endpoint_handles_empty_endpoint() {
        let store = mem_store_with_traces();
        insert_trace(&store, "noep", 60, "error", Some("unknown"), 0, 100);
        insert_attempt_on(&store, "noep", "unknown", 0, None, "");

        let rows = store.error_crosstab(24, CrosstabDimension::Endpoint);
        let unk = rows.iter().find(|r| r.error_type == "unknown").unwrap();
        assert_eq!(unk.buckets[0].key, "", "空端点保持空串，由前端决定如何展示");
        assert_eq!(unk.total, 1);
    }

    /// lift 存在的理由必须可执行验证：流量分布不均时，裸集中度会把"承载最多的
    /// 对象"误报成"有问题的对象"。这里构造两个凭据 —— A 承载 90% 流量、错误份额
    /// 也正好 90%（相称，不该报警），B 只承载 10% 流量却吃下 50% 的另一类错误
    /// （不成比例，才是真信号）。集中度会给出相反的排序，lift 才纠正过来。
    #[test]
    fn lift_separates_disproportionate_errors_from_high_traffic_ones() {
        let store = mem_store_with_traces();
        // 凭据 1：90 条成功流量 + 9 条 transient
        for i in 0..90 {
            insert_trace(&store, &format!("ok1-{}", i), 60, "success", None, 1, 100);
        }
        for i in 0..9 {
            insert_trace(
                &store,
                &format!("tr1-{}", i),
                60,
                "error",
                Some("transient"),
                1,
                100,
            );
        }
        // 凭据 2：10 条成功流量 + 1 条 transient（与凭据1同比例）
        for i in 0..10 {
            insert_trace(&store, &format!("ok2-{}", i), 60, "success", None, 2, 100);
        }
        insert_trace(&store, "tr2-0", 60, "error", Some("transient"), 2, 100);

        let rows = store.error_crosstab(24, CrosstabDimension::Credential);
        let tr = rows.iter().find(|r| r.error_type == "transient").unwrap();

        // 集中度 0.9：看着像"高度集中在凭据 1"
        assert!(
            (tr.concentration - 0.9).abs() < 1e-9,
            "集中度应为 9/10 = 0.9"
        );

        // 但 lift ≈ 1：凭据 1 的错误份额(90%)与它的流量份额(99/110≈90%)相称，
        // 集中只是因为它承载得多，不是它有问题
        let b1 = tr.buckets.iter().find(|b| b.key == "1").unwrap();
        let lift1 = b1.lift.expect("有流量应能算 lift");
        assert!(
            (lift1 - 1.0).abs() < 0.05,
            "承载最多的凭据 lift 应≈1（相称），实际 {:.3}",
            lift1
        );
        assert_eq!(b1.traffic, 99, "traffic 须含成功+失败的全部请求");

        // 凭据 2 同比例 → lift 也应 ≈1
        let b2 = tr.buckets.iter().find(|b| b.key == "2").unwrap();
        let lift2 = b2.lift.unwrap();
        assert!(
            (lift2 - 1.0).abs() < 0.05,
            "同比例的凭据 lift 也应≈1，实际 {:.3}",
            lift2
        );
    }

    /// 与上一个测试互补：错误份额明显超出流量份额时 lift 必须显著 > 1。
    /// 这是真正该报警的形态。
    #[test]
    fn lift_flags_low_traffic_object_with_outsized_errors() {
        let store = mem_store_with_traces();
        // 凭据 1：99 条成功，0 错误（承载绝大部分流量但很健康）
        for i in 0..99 {
            insert_trace(&store, &format!("ok-{}", i), 60, "success", None, 1, 100);
        }
        // 凭据 9：1 条成功 + 5 条 network_error（流量极少却错得多）
        insert_trace(&store, "ok9", 60, "success", None, 9, 100);
        for i in 0..5 {
            insert_trace(
                &store,
                &format!("ne9-{}", i),
                60,
                "error",
                Some("network_error"),
                9,
                100,
            );
        }

        let rows = store.error_crosstab(24, CrosstabDimension::Credential);
        let ne = rows
            .iter()
            .find(|r| r.error_type == "network_error")
            .unwrap();
        let b9 = ne.buckets.iter().find(|b| b.key == "9").unwrap();
        let lift9 = b9.lift.unwrap();

        // 错误份额 5/5 = 100%，流量份额 6/105 ≈ 5.7% → lift ≈ 17.5
        assert!(
            lift9 > 5.0,
            "低流量高错误的对象 lift 必须显著>1 才能被发现，实际 {:.2}",
            lift9
        );
    }

    /// 桶数超过 CROSSTAB_TOP_BUCKETS 时，total 与 distinct_keys 必须仍按全量计，
    /// 只截断 buckets 列表 —— 否则集中度的分母会被截断后的和污染，
    /// 让"散开"的错误看起来像"集中"。
    #[test]
    fn crosstab_truncates_buckets_without_corrupting_totals() {
        let store = mem_store_with_traces();
        // 10 个不同凭据各 1 条，超过 CROSSTAB_TOP_BUCKETS(6)
        for id in 1..=10u64 {
            insert_trace(
                &store,
                &format!("t{}", id),
                60,
                "error",
                Some("transient"),
                id,
                500,
            );
        }
        let rows = store.error_crosstab(24, CrosstabDimension::Credential);
        let row = rows.iter().find(|r| r.error_type == "transient").unwrap();

        assert_eq!(row.buckets.len(), CROSSTAB_TOP_BUCKETS, "桶列表应被截断");
        assert_eq!(row.total, 10, "total 必须是全量，不能只算被保留的桶");
        assert_eq!(row.distinct_keys, 10, "distinct_keys 必须是全量");
        assert!(
            (row.concentration - 0.1).abs() < 1e-9,
            "集中度分母须用全量 total(10)，否则 1/6 会被误报成集中"
        );
    }

    #[test]
    fn percentiles_handles_empty_and_single() {
        assert!(percentiles(Vec::new()).is_none(), "无样本应返回 None");
        let one = percentiles(vec![5_000]).unwrap();
        assert_eq!((one.p50, one.p95, one.p99, one.n), (5_000, 5_000, 5_000, 1));
    }

    #[test]
    fn by_proxy_attributes_interruptions_to_proxy_of_success_hop() {
        let store = mem_store_with_traces();
        // 走代理 p1 的中断
        insert_trace(&store, "t1", 60, "interrupted", Some("stream_interrupted"), 1, 245_000);
        insert_attempt(&store, "t1", "success", 1, Some("socks5://p1:1080"));
        // 直连成功
        insert_trace(&store, "t2", 60, "success", None, 2, 1000);
        insert_attempt(&store, "t2", "success", 2, None);
        // 走 p1 的网络错误跳 + 换直连成功
        insert_trace(&store, "t3", 60, "success", None, 2, 1500);
        insert_attempt(&store, "t3", "network_error", 1, Some("socks5://p1:1080"));

        let rows = store.by_proxy(24);
        let p1 = rows.iter().find(|r| r.proxy_url == "socks5://p1:1080").unwrap();
        assert_eq!(p1.attempts, 2);
        assert_eq!(p1.success, 1);
        assert_eq!(p1.network_error, 1);
        assert_eq!(p1.interrupted, 1);
        let direct = rows.iter().find(|r| r.proxy_url.is_empty()).unwrap();
        assert_eq!(direct.attempts, 1);
        assert_eq!(direct.interrupted, 0);
    }

    #[test]
    fn by_credential_merges_trace_and_attempt_stats() {
        let store = mem_store_with_traces();
        insert_trace(&store, "t1", 60, "interrupted", Some("stream_interrupted"), 5, 245_000);
        insert_attempt(&store, "t1", "success", 5, None);
        insert_trace(&store, "t2", 60, "error", Some("auth_failed"), 5, 400);
        insert_attempt(&store, "t2", "auth_failed", 5, None);

        let rows = store.by_credential(24);
        let c5 = rows.iter().find(|r| r.credential_id == 5).unwrap();
        assert_eq!(c5.total, 2);
        assert_eq!(c5.interrupted, 1);
        assert_eq!(c5.error, 1);
        assert_eq!(c5.auth_failed, 1);
    }

    #[test]
    fn phase_baseline_groups_by_phase_and_proxy() {
        let store = mem_store_with_traces();
        // 出口 A：3 条 streaming，其中 1 条中断
        for (i, oc) in ["success", "success", "stream_interrupted"].iter().enumerate() {
            let id = format!("a{}", i);
            insert_trace(&store, &id, 60, "success", None, 1, 100);
            insert_attempt(&store, &id, "success", 1, Some("socks5://a:1080"));
            insert_phase(&store, &id, 0, "streaming", oc);
        }
        // 出口 B：1 条 streaming，全成功
        insert_trace(&store, "b0", 60, "success", None, 2, 100);
        insert_attempt(&store, "b0", "success", 2, Some("socks5://b:1080"));
        insert_phase(&store, "b0", 0, "streaming", "success");

        let rows = store.phase_baseline(24);
        let a = rows
            .iter()
            .find(|r| r.phase == "streaming" && r.proxy_url == "socks5://a:1080")
            .expect("应有出口 A 的 streaming 行");
        assert_eq!(a.total, 3);
        assert_eq!(a.failed, 1);

        let b = rows
            .iter()
            .find(|r| r.phase == "streaming" && r.proxy_url == "socks5://b:1080")
            .expect("应有出口 B 的 streaming 行");
        assert_eq!(b.failed, 0);
    }

    /// 构造一份形状与生产同类的跳数分布：绝大多数请求单跳成功，
    /// 越深的跳到达越少、边际收益越低，最后一跳塌到阈值以下。
    ///
    /// 布局（跳数: 成功 / 失败）
    /// - 1 跳: 80 / 0
    /// - 2 跳:  6 / 4
    /// - 3 跳:  4 / 4
    /// - 4 跳:  1 / 9
    fn ladder_fixture() -> OpsStore {
        let store = mem_store_with_traces();
        for (hops, ok, bad) in [(1u32, 80, 0), (2, 6, 4), (3, 4, 4), (4, 1, 9)] {
            for i in 0..ok {
                insert_trace_hops(&store, &format!("ok-{}-{}", hops, i), "success", hops);
            }
            for i in 0..bad {
                insert_trace_hops(&store, &format!("bad-{}-{}", hops, i), "error", hops);
            }
        }
        store
    }

    /// 这张表最容易写错的地方：`reached` 必须是累积量。
    /// 跑了 4 跳的请求也算作到达过第 1/2/3 跳。
    ///
    /// 同时钉死自检恒等式：**第 1 跳的 reached == 窗口内 trace 总数**。
    /// 如果实现误用了 `total_attempts = N` 这种等值条件，第 1 跳只会剩下
    /// 单跳完成的请求（这里是 80 而不是 108），这条断言立刻炸。
    #[test]
    fn retry_ladder_reached_is_cumulative_and_first_hop_covers_all_traces() {
        let store = ladder_fixture();
        let r = store.retry_effectiveness(24);

        // 窗口内 trace 总数，独立于被测方法算出来作对照
        let win_total: u64 = store
            .conn
            .lock()
            .query_row(
                "SELECT COUNT(*) FROM traces WHERE ts_epoch >= ?1",
                [Utc::now().timestamp() - 24 * 3600],
                |row| row.get::<_, i64>(0),
            )
            .unwrap() as u64;
        assert_eq!(win_total, 108, "fixture 总数");

        assert_eq!(r.steps.len(), 4, "实测最大跳数为 4，应输出 4 级");
        assert_eq!(
            r.steps[0].reached, win_total,
            "第 1 跳 reached 必须等于窗口内全部 trace 数（每个请求都跑过首跳）"
        );

        // 累积量：reached 逐跳严格递减，且等于「跳数 >= hop」的和
        assert_eq!(r.steps[0].reached, 108); // 全部
        assert_eq!(r.steps[1].reached, 28); // 10+8+10
        assert_eq!(r.steps[2].reached, 18); // 8+10
        assert_eq!(r.steps[3].reached, 10); // 10
        for w in r.steps.windows(2) {
            assert!(
                w[0].reached >= w[1].reached,
                "累积到达数必须单调不增，实际 {} → {}",
                w[0].reached,
                w[1].reached
            );
        }

        // rescued 是等值条件：恰好停在这一跳且成功
        assert_eq!(
            r.steps.iter().map(|s| s.rescued).collect::<Vec<_>>(),
            vec![80, 6, 4, 1]
        );
    }

    /// 边际收益必须用累积分母算，且塌点判定要能把「还在救请求的跳」和
    /// 「基本在陪跑的跳」分开 —— 这是决定是否下调重试预算的唯一依据。
    #[test]
    fn retry_ladder_marginal_yield_and_collapse_point() {
        let store = ladder_fixture();
        let r = store.retry_effectiveness(24);

        // 第 2 跳 6/28 ≈ 0.214、第 3 跳 4/18 ≈ 0.222 —— 都在阈值之上
        assert!(r.steps[1].marginal_yield > YIELD_COLLAPSE_THRESHOLD);
        assert!(r.steps[2].marginal_yield > YIELD_COLLAPSE_THRESHOLD);
        // 第 4 跳 1/10 = 0.10 —— 塌了
        assert!((r.steps[3].marginal_yield - 0.10).abs() < 1e-9);
        assert_eq!(
            r.yield_collapse_at,
            Some(4),
            "塌点应指向第 4 跳，而不是仍在救请求的第 3 跳"
        );

        // 净收益 = 首跳没搞定但最终成功 = 6+4+1
        assert_eq!(r.total_rescued, 11);
        // 最贵的失败 = 到达最深一跳仍未成功 = 10-1
        assert_eq!(r.total_exhausted, 9);

        // 若用等值分母（rescued/停在该跳的数）算，第 2 跳会是 6/10 = 0.6，
        // 三倍高于真实的 0.214 —— 这个对比就是累积口径存在的理由
        assert!(
            r.steps[1].marginal_yield < 0.3,
            "累积分母下第 2 跳应约 0.214，实际 {:.3}",
            r.steps[1].marginal_yield
        );
    }

    /// 全是单跳请求时，阶梯只有 1 级，且不该报出任何塌点
    /// （第 1 跳的 yield 是整体成功率，不是重试的收益）。
    #[test]
    fn retry_ladder_single_hop_window_has_no_collapse() {
        let store = mem_store_with_traces();
        for i in 0..5 {
            insert_trace_hops(&store, &format!("s{}", i), "success", 1);
        }
        let r = store.retry_effectiveness(24);
        assert_eq!(r.steps.len(), 1);
        assert_eq!(r.steps[0].reached, 5);
        assert_eq!(r.total_rescued, 0, "没有重试就没有救回");
        assert_eq!(r.yield_collapse_at, None, "第 1 跳不参与塌点判定");
        assert!(
            (r.backoff_coverage - 1.0).abs() < 1e-9,
            "没有相邻跳对时覆盖率记 1.0（该算的都算了），不是 0.0"
        );
    }

    /// `started_ms` 是后加的列，历史行为 NULL。跨越那次 schema 变更的窗口里，
    /// 算不出间隔的跳对被跳过，backoff 数字会**静默**基于极少数样本。
    /// coverage 就是防这个的：它必须随 NULL 行的出现而下降。
    #[test]
    fn backoff_coverage_drops_when_started_ms_is_null() {
        let store = mem_store_with_traces();
        // 新行：两跳的 started_ms 都有 → 可算间隔
        insert_trace_hops(&store, "new", "success", 2);
        insert_attempt_at(&store, "new", 0, "transient", Some(1_000), 800);
        insert_attempt_at(&store, "new", 1, "success", Some(5_000), 300);
        // 历史行：started_ms 全 NULL → 算不出
        insert_trace_hops(&store, "old", "success", 2);
        insert_attempt_at(&store, "old", 0, "transient", None, 800);
        insert_attempt_at(&store, "old", 1, "success", None, 300);

        let r = store.retry_effectiveness(24);
        // 2 对相邻跳，只有 1 对两端非 NULL
        assert!(
            (r.backoff_coverage - 0.5).abs() < 1e-9,
            "覆盖率应为 1/2，实际 {:.3}",
            r.backoff_coverage
        );
        // 间隔 = 5000 - (1000 + 800) = 3200，扣掉了前一跳自身耗时
        assert_eq!(
            r.steps[1].backoff_p50_ms,
            Some(3_200),
            "退避须扣除前一跳耗时，否则把上游耗时算成退避"
        );
        assert_eq!(r.steps[0].backoff_p50_ms, None, "第 1 跳前面没有跳");

        // 全部为 NULL 时覆盖率归零，且不残留任何中位数
        let empty = mem_store_with_traces();
        insert_trace_hops(&empty, "o1", "success", 2);
        insert_attempt_at(&empty, "o1", 0, "transient", None, 800);
        insert_attempt_at(&empty, "o1", 1, "success", None, 300);
        let r2 = empty.retry_effectiveness(24);
        assert!(
            r2.backoff_coverage.abs() < 1e-9,
            "全 NULL 时覆盖率应为 0，实际 {:.3}",
            r2.backoff_coverage
        );
        assert!(
            r2.steps.iter().all(|s| s.backoff_p50_ms.is_none()),
            "覆盖率为 0 时不得给出任何退避中位数"
        );
    }

    /// 窗口外的 trace 不能进阶梯，否则「第 1 跳 == 窗口内总数」的恒等式会破。
    #[test]
    fn retry_ladder_respects_window() {
        let store = mem_store_with_traces();
        insert_trace_hops(&store, "in", "success", 1);
        // 窗口外（48 小时前）
        let epoch = Utc::now().timestamp() - 3600 * 48;
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO traces (trace_id, ts, ts_epoch, key_id, key_source, model, is_stream, \
                 final_status, final_credential_id, error_type, total_attempts, duration_ms) \
                 VALUES ('out', '2026', ?1, 1, 'clientKey', 'm', 1, 'error', 1, NULL, 4, 100)",
                [epoch],
            )
            .unwrap();

        let r = store.retry_effectiveness(24);
        assert_eq!(r.steps.len(), 1, "窗口外的 4 跳 trace 不应把阶梯撑到 4 级");
        assert_eq!(r.steps[0].reached, 1);
        assert_eq!(r.total_exhausted, 0);
    }

    #[test]
    fn client_disconnect_not_counted_as_failure_in_baseline() {
        let store = mem_store_with_traces();
        insert_trace(&store, "c0", 60, "success", None, 1, 100);
        insert_attempt(&store, "c0", "success", 1, Some("socks5://a:1080"));
        insert_phase(&store, "c0", 0, "streaming", "client_disconnected");

        let rows = store.phase_baseline(24);
        let row = rows.iter().find(|r| r.phase == "streaming").unwrap();
        assert_eq!(row.failed, 0, "客户端断开不是故障，不得计入失败率");
        assert_eq!(row.total, 1, "但仍计入总数");
    }

    // ===== 错误消息指纹 =====

    /// 真实样本（取自生产库 7 天窗口），测试直接用原串，避免手编的消息
    /// 掩盖真实模式。
    const MSG_HIGH_LOAD: &str = "流式 API 请求失败: 500 Internal Server Error {\"message\":\"Encountered unexpectedly high load when processing the request, please try again.\",\"reason\":\"MODEL_TEMPORARILY_UNAVAILABLE\"}";
    const MSG_UNEXPECTED: &str = "流式 API 请求失败: 500 Internal Server Error {\"message\":\"Encountered an unexpected error when processing the request, please try again.\",\"reason\":null}";
    const MSG_BAD_MODEL: &str = "流式 API 请求失败: 400 Bad Request {\"message\":\"Invalid model ID. Please select a different model to continue.\",\"reason\":\"INVALID_MODEL_ID\"}";
    const MSG_DECODE_BARE: &str = "error decoding response body";
    const MSG_DECODE_CHAINED: &str = "error decoding response body [decode] <- request or response body error <- error reading a body from connection <- stream error received: unexpected internal error encountered";

    fn insert_trace_with_message(
        store: &OpsStore,
        trace_id: &str,
        epoch_offset_secs: i64,
        error_type: &str,
        error_message: &str,
    ) {
        let epoch = Utc::now().timestamp() - epoch_offset_secs;
        store
            .conn
            .lock()
            .execute(
                "INSERT INTO traces (trace_id, ts, ts_epoch, key_id, key_source, model, is_stream, \
                 final_status, final_credential_id, error_type, error_message, total_attempts, \
                 duration_ms) VALUES (?1, '2026', ?2, 1, 'clientKey', 'm', 1, 'error', 1, ?3, ?4, 1, 500)",
                duckdb::params![trace_id, epoch, error_type, error_message],
            )
            .unwrap();
    }

    /// 防过度归一化的守卫。两条消息都是 500、前缀完全一致，只在 JSON body 里
    /// 分叉：`unexpectedly high load`（上游过载，等它恢复）vs
    /// `an unexpected error`（上游内部错误，要报 case）。根因与处置都不同，
    /// 一旦被并成一类，面板就会把两件事报成一件 —— 这是本模块最容易犯且
    /// 最难发现的错误，故单列一个测试钉死。
    #[test]
    fn two_distinct_500s_must_not_merge() {
        let (s1, f1) = normalize_error_message(MSG_HIGH_LOAD);
        let (s2, f2) = normalize_error_message(MSG_UNEXPECTED);
        assert_eq!(s1, Some(500));
        assert_eq!(s2, Some(500));
        assert_ne!(f1, f2, "两种 500 根因不同，指纹不得相同");
        // 截断长度必须留够到分叉点，否则上面的 assert_ne 会因为都被截在
        // 公共前缀里而失效 —— 这里显式验证分叉词进了模板
        assert!(f1.contains("high load"), "过载特征词应在模板内: {f1}");
        assert!(
            f2.contains("unexpected error"),
            "内部错误特征词应在模板内: {f2}"
        );
    }

    /// 状态码必须先摘成独立字段再抹数字。若顺序反了，`500 Internal Server Error`
    /// 与 `400 Bad Request` 的数字都变成 `N`，两者只剩原因短语可区分，而更长的
    /// JSON body 一截断就可能同模板。
    #[test]
    fn http_status_extracted_before_digit_normalization() {
        let (s500, f500) = normalize_error_message(MSG_HIGH_LOAD);
        let (s400, f400) = normalize_error_message(MSG_BAD_MODEL);
        assert_eq!(s500, Some(500));
        assert_eq!(s400, Some(400));
        assert!(!f500.contains("500"), "状态码应被占位替换: {f500}");
        assert!(f500.contains("<status>"));
        assert_ne!(f500, f400);
    }

    /// 计数/时长里的三位数不是状态码。宽松匹配任意三位数会给
    /// `idle_ms=218` 造出一个假的 218 状态码，凭空多出一维错误分类。
    #[test]
    fn three_digit_counters_are_not_mistaken_for_status() {
        let (status, fp) = normalize_error_message("client_gone=true bytes=26719 idle_ms=218");
        assert_eq!(status, None, "计数里的三位数不得被当成状态码");
        assert_eq!(fp, "client_gone=true bytes=N idle_ms=N");
    }

    /// 同一根因有时带 reqwest 因果链有时不带（取决于在哪一层被捕获）。
    /// 不砍链就分裂成两类，头部排序被稀释。
    #[test]
    fn causal_chain_stripped_but_kind_tag_kept() {
        // 因果链（`<- ` 之后）剥掉：它是同一根因的下游转述，保留会让每种链条
        // 组合各成一个指纹。
        let (_, bare) = normalize_error_message(MSG_DECODE_BARE);
        assert_eq!(bare, "error decoding response body");
        let (_, chained) = normalize_error_message(MSG_DECODE_CHAINED);
        assert!(
            !chained.contains(" <- "),
            "因果链应被剥掉: {chained}"
        );

        // 但 kind 标签必须留住。`[timeout+decode]` = 本地超时掐死了一条健康流
        // （我们的配置造成的），`[decode]` = 上游断了。两者处置动作相反：
        // 前者要调 streamIdleTimeoutSecs，后者只能等上游。
        // http_client.rs 里 describe_reqwest_error 的注释就明确写了这个用途。
        let (_, timeout) = normalize_error_message(
            "error decoding response body [timeout+decode] <- request or response body error <- operation timed out",
        );
        assert!(
            timeout.contains("timeout"),
            "本地超时的标签必须留在指纹里: {timeout}"
        );
        assert_ne!(
            timeout, chained,
            "本地超时与上游断流不得并成一类——这是这类错误里最重要的归因区分"
        );
        assert_ne!(timeout, bare, "带超时标签的与裸消息不得并成一类");
    }

    /// 括号内的日期/计数归一化：`（8/1）`与`（9/1）`是同一条「凭据全禁用」告警，
    /// 分开看会让它在头部排序里被拆成两条。全角括号必须与半角同样处理。
    #[test]
    fn digits_in_brackets_normalized() {
        let (_, a) = normalize_error_message("所有凭据均已禁用（8/1）");
        let (_, b) = normalize_error_message("所有凭据均已禁用（9/1）");
        assert_eq!(a, b);
        assert_eq!(a, "所有凭据均已禁用（N/N）");
    }

    /// 长随机 id 每次调用都不同，不收敛的话每条都是独立指纹。
    /// 同时验证不误伤 `claude-opus-5` / `us-east-1` 这类含少量数字的稳定标识。
    #[test]
    fn random_ids_collapse_but_stable_identifiers_survive() {
        let (_, fp) = normalize_error_message(
            "Upstream ended before completing tool_use toolu_bdrk_013iHieazSJUr5RHg31UftAr (str_replace) JSON input; buffered 0 bytes.",
        );
        assert!(fp.contains("<id>"), "随机 tool id 应收敛: {fp}");
        assert!(
            fp.contains("str_replace"),
            "工具名是有效区分维度，须保留: {fp}"
        );
        assert!(fp.contains("buffered N bytes"));

        let (_, url) = normalize_error_message(
            "error sending request for url (https://q.us-east-1.amazonaws.com/generateAssistantResponse)",
        );
        assert!(
            url.contains("amazonaws.com/generateAssistantResponse"),
            "稳定 URL 不得被抹成 <id>: {url}"
        );
    }

    /// URL 的 query 是指纹爆炸的经典来源（请求 id / 时间戳）。
    #[test]
    fn url_query_stripped() {
        let (_, a) = normalize_error_message("failed GET https://h.example.com/v1/chat?rid=abc111");
        let (_, b) = normalize_error_message("failed GET https://h.example.com/v1/chat?rid=xyz999");
        assert_eq!(a, b);
        assert!(!a.contains('?'), "query 应被砍掉: {a}");
    }

    /// 超长 JSON body 不截断的话模板本身就是唯一串，归并率归零。
    #[test]
    fn long_message_truncated() {
        let raw = format!("boom {}", "x".repeat(500));
        let (_, fp) = normalize_error_message(&raw);
        assert!(
            fp.chars().count() <= FINGERPRINT_MAX_CHARS + 1,
            "模板长度应受限，实际 {}",
            fp.chars().count()
        );
        assert!(fp.ends_with('…'), "截断应有可见标记: {fp}");
    }

    /// 端到端：归并、计数降序、原始样本保留、时间窗过滤。
    #[test]
    fn fingerprints_aggregate_and_keep_raw_samples() {
        let store = mem_store_with_traces();
        for i in 0..5 {
            insert_trace_with_message(&store, &format!("hl{i}"), 60, "transient", MSG_HIGH_LOAD);
        }
        for i in 0..2 {
            insert_trace_with_message(&store, &format!("ue{i}"), 60, "transient", MSG_UNEXPECTED);
        }
        insert_trace_with_message(&store, "d0", 60, "stream_interrupted", MSG_DECODE_BARE);
        insert_trace_with_message(&store, "d1", 60, "stream_interrupted", MSG_DECODE_CHAINED);
        // 窗口外不计
        insert_trace_with_message(&store, "old", 3600 * 48, "transient", MSG_HIGH_LOAD);

        let rows = store.error_fingerprints(24, 50);
        // 4 个指纹：两种 500 各一个，decode 裸消息与带 [decode] 标签的各一个。
        // 后两者不合并是刻意的（标签携带归因信息，见 normalize_error_message
        // 第 3 步）—— 这里正是钉住「不过度归并」的地方。
        assert_eq!(rows.len(), 4, "5+2+1+1 条应归成 4 个指纹: {rows:#?}");
        assert_eq!(rows[0].count, 5, "应按 count 降序");
        assert_eq!(rows[0].http_status, Some(500));
        // 样本必须是原始未归一化串，否则误并无从发现
        assert_eq!(rows[0].samples[0], MSG_HIGH_LOAD);
        assert!(!rows[0].samples[0].contains("<status>"));
        assert!(!rows[0].first_seen.is_empty() && !rows[0].last_seen.is_empty());

        let bare = rows
            .iter()
            .find(|r| r.fingerprint == "error decoding response body")
            .expect("裸 decode 消息应自成一条");
        assert_eq!(bare.count, 1);
        assert_eq!(bare.http_status, None);

        let tagged = rows
            .iter()
            .find(|r| r.fingerprint.contains("[decode]"))
            .expect("带 [decode] 标签的应自成一条，标签不剥");
        assert_eq!(tagged.count, 1);
        // 样本是原始串，含因果链 —— 这才看得出模板盖住了什么
        assert!(tagged.samples.iter().any(|s| s.contains("<- ")));
    }

    /// 同一句消息落到多个 error_type 时必须暴露出来：那说明分类逻辑与消息内容
    /// 存在歧义（同样的上游回复被判成了不同类别），是分类规则的 bug 线索。
    #[test]
    fn fingerprint_exposes_ambiguous_error_types() {
        let store = mem_store_with_traces();
        insert_trace_with_message(&store, "a0", 60, "transient", MSG_HIGH_LOAD);
        insert_trace_with_message(&store, "a1", 60, "upstream_error", MSG_HIGH_LOAD);

        let rows = store.error_fingerprints(24, 50);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].error_types, vec!["transient", "upstream_error"]);
    }

    /// limit 生效且不影响归并（先归并再截断，不是先截断再归并）。
    #[test]
    fn fingerprint_limit_truncates_after_aggregation() {
        let store = mem_store_with_traces();
        insert_trace_with_message(&store, "x0", 60, "transient", MSG_HIGH_LOAD);
        insert_trace_with_message(&store, "x1", 60, "transient", MSG_UNEXPECTED);
        insert_trace_with_message(&store, "x2", 60, "bad_request", MSG_BAD_MODEL);

        let rows = store.error_fingerprints(24, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].count, 1);
    }

    /// 空消息与全空窗口不得 panic，返回空集
    #[test]
    fn fingerprints_empty_window() {
        let store = mem_store_with_traces();
        assert!(store.error_fingerprints(24, 50).is_empty());
        assert_eq!(normalize_error_message(""), (None, String::new()));
    }
}
