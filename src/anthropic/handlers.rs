//! Anthropic API Handler 函数

use std::convert::Infallible;
use std::time::Instant;

use crate::admin::client_keys::SharedClientKeyManager;
use crate::admin::usage_stats::UsageRecord;
use crate::admin::usage_store::SharedUsageStore;
use crate::admin::trace_db::{
    SharedTraceStore, TraceAttempt, TraceKeySource, TracePhase, TraceRecord, TraceSink, outcome,
    phase,
};
use crate::http_client::describe_reqwest_error;
use crate::kiro::model::events::Event;
use crate::kiro::model::requests::kiro::KiroRequest;
use crate::kiro::parser::decoder::EventStreamDecoder;
use crate::token;
use anyhow::Error;
use axum::{
    Json as JsonExtractor,
    body::Body,
    extract::{Extension, State},
    http::{StatusCode, header},
    response::{IntoResponse, Json, Response},
};
use bytes::Bytes;
use chrono::Utc;
use futures::{Stream, StreamExt, stream};
use serde_json::json;
use std::time::Duration;
use tokio::time::interval;
use uuid::Uuid;

use super::converter::{ConversionError, convert_request_with_mode};
use super::middleware::{AppState, KeyContext};
use super::stream::{BufferedStreamContext, SseEvent, StreamContext, ToolJsonAccumulatorError};
use super::types::{
    CountTokensRequest, CountTokensResponse, ErrorResponse, MessagesRequest, Model, ModelsResponse,
    OutputConfig, Thinking,
};
use super::websearch;

/// 请求结束时记录用量的钩子
///
/// 在 handler 入口构造，调用 [`Self::record`] 时把当次请求的 input/output token、
/// 命中的上游凭据 ID、状态写入：
/// - `kiro.duckdb` 的 usage_records 表（持久化 + 仪表盘统计同源）
/// - 客户端 Key 计数（按 Key 累计）
#[derive(Clone)]
pub(crate) struct UsageRecordHook {
    pub usage: Option<SharedUsageStore>,
    pub client_keys: Option<SharedClientKeyManager>,
    pub key_id: u64,
    pub model: String,
    pub started_at: Instant,
    /// 消耗回写目标。credits 与余额 remaining 同量纲（已由生产数据验证，误差 <2%）。
    pub dispatcher: Option<std::sync::Arc<crate::kiro::dispatch::GroupDispatcher>>,
    pub group: Option<String>,
}

impl UsageRecordHook {
    pub fn from_state(state: &AppState, key_id: u64, model: String, group: Option<String>) -> Self {
        Self {
            usage: state.usage_store.clone(),
            client_keys: state.client_keys.clone(),
            key_id,
            model,
            started_at: Instant::now(),
            dispatcher: state.dispatcher.clone(),
            group,
        }
    }

    pub fn record(
        &self,
        credential_id: u64,
        input_tokens: i32,
        output_tokens: i32,
        cache_tokens: (i32, i32),
        credits: f64,
        status: &str,
    ) {
        let (cache_creation_tokens, cache_read_tokens) = cache_tokens;
        let rec = UsageRecord {
            ts: Utc::now().to_rfc3339(),
            key_id: self.key_id,
            credential_id,
            model: self.model.clone(),
            input_tokens: input_tokens.max(0) as u64,
            output_tokens: output_tokens.max(0) as u64,
            cache_creation_tokens: cache_creation_tokens.max(0) as u64,
            cache_read_tokens: cache_read_tokens.max(0) as u64,
            credits: if credits.is_finite() && credits > 0.0 {
                credits
            } else {
                0.0
            },
            duration_ms: self.started_at.elapsed().as_millis() as u64,
            status: status.to_string(),
        };
        if let Some(u) = &self.usage {
            u.record(&rec);
        }
        if status == "success" && self.key_id != 0
            && let Some(m) = &self.client_keys {
                m.record_usage(
                    self.key_id,
                    rec.input_tokens,
                    rec.output_tokens,
                    rec.cache_creation_tokens,
                    rec.cache_read_tokens,
                    rec.credits,
                );
            }
        // 反向路径：把本次实际消耗回写给调度器。
        // 粘滞命中与新分配都会经过这里，长会话的消耗因此照样计入——
        // 这是本设计能承诺「额度消耗趋同」而非仅「新会话数加权」的依据。
        if credential_id != 0
            && let Some(d) = &self.dispatcher
        {
            d.report_consumption(self.group.as_deref(), credential_id, credits);
        }
    }
}

/// 单次请求的链路追踪器
///
/// 在 handler 入口构造，作为 [`TraceSink`] 传入 provider；provider 在重试循环里
/// 每跳调用 [`on_attempt`](TraceSink::on_attempt) 累积一条 [`TraceAttempt`]。
/// 请求结束时调用 [`Self::finalize`] 组装 [`TraceRecord`] 并写入 SQLite。
///
/// `store` 为 None（未启用 Admin / trace）时所有方法都是空操作，零开销。
pub(crate) struct RequestTracer {
    store: Option<SharedTraceStore>,
    trace_id: String,
    ts: String,
    key_id: u64,
    key_source: TraceKeySource,
    model: String,
    is_stream: bool,
    /// Claude Code 会话 id（metadata.user_id 的 JSON 或 legacy 格式），可为 None
    session_id: Option<String>,
    started_at: Instant,
    /// 首个上游 chunk 到达时刻（仅流式标记；取第一次）
    first_token_at: parking_lot::Mutex<Option<Instant>>,
    attempts: parking_lot::Mutex<Vec<TraceAttempt>>,
    /// 已关闭的流生命周期段
    phases: parking_lot::Mutex<Vec<TracePhase>>,
    /// 当前打开的段：(段名, 起点)
    open_phase: parking_lot::Mutex<Option<(&'static str, Instant)>>,
    /// 流形态摘要：观察发给客户端的事件序列（类型/时刻/字节，不存内容）
    shape: parking_lot::Mutex<super::stream::StreamShape>,
}

/// 本次请求的用量快照（落入 trace 行，与 usage_log 同源）
#[derive(Clone, Copy, Default)]
pub(crate) struct TraceUsage {
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_creation_tokens: u64,
    pub cache_read_tokens: u64,
    pub credits: f64,
}

impl TraceUsage {
    /// 错误早退等无用量场景
    pub fn zero() -> Self {
        Self::default()
    }
}

struct RequestTraceOptions {
    key_ctx: KeyContext,
    model: String,
    is_stream: bool,
    session_id: Option<String>,
}

struct ResponseProcessingConfig {
    thinking_enabled: bool,
    /// display:"omitted"——思考正文不下发，签名=恢复键
    thinking_omitted: bool,
    /// 恢复键正文存储（omitted 时使用）
    thinking_text_store:
        Option<std::sync::Arc<crate::admin::request_body_store::RequestBodyStore>>,
    tool_name_map: std::collections::HashMap<String, String>,
    known_tool_names: std::collections::HashSet<String>,
    cache_usage: super::cache_metering::CacheUsage,
    group: Option<String>,
    context_window: i32,
    sticky_key: Option<String>,
}

/// 从请求 metadata.user_id 提取 Claude Code 会话 id。
/// 支持当前 JSON 格式与旧版 `_session_<uuid>` 格式。
fn session_id_of(payload: &super::types::MessagesRequest) -> Option<String> {
    payload
        .metadata
        .as_ref()
        .and_then(|m| m.user_id.as_deref())
        .and_then(super::metadata::extract_session_id)
}

/// 会话粘滞 key。**只在能解析出 UUID session 时启用**。
///
/// 刻意不复用 cache_metering::isolation_seed：那个函数只在 key_id == 0 时走
/// cc 级降级，普通 client key 直接返回 key:<key_id>，会让同一 client key 下
/// 的所有会话共享一条粘滞记录，流量被永久钉死在一个账号——正是本功能要修的病。
fn dispatch_sticky_key(req: &MessagesRequest) -> Option<String> {
    req.metadata
        .as_ref()
        .and_then(|m| m.user_id.as_deref())
        .and_then(super::metadata::extract_session_id)
}

/// omitted 轻量往返的回程恢复：历史 assistant 消息里「空正文 + kiro-thinking-v1 恢复键」
/// 的 thinking 块，凭键从本地 blob 回填正文。必须在 token 计数 / cache 计量 / 转换
/// 之前调用——上游看到的内容与计量口径要一致。键失效（已过保留期）时保持为空，
/// 不伪造内容；该块推理上下文丢失，与原版签名过期语义一致。
fn restore_omitted_thinking(
    payload: &mut MessagesRequest,
    store: &crate::admin::request_body_store::RequestBodyStore,
) {
    const KEY_PREFIX: &str = "kiro-thinking-v1:";
    for msg in payload.messages.iter_mut() {
        if msg.role != "assistant" {
            continue;
        }
        let serde_json::Value::Array(blocks) = &mut msg.content else {
            continue;
        };
        for b in blocks.iter_mut() {
            if b["type"] != "thinking" {
                continue;
            }
            let empty = b["thinking"].as_str().map(str::is_empty).unwrap_or(true);
            let Some(id) = b["signature"]
                .as_str()
                .and_then(|s| s.strip_prefix(KEY_PREFIX))
            else {
                continue;
            };
            if empty && let Some(text) = store.load(id) {
                b["thinking"] = serde_json::Value::String(
                    String::from_utf8_lossy(&text).into_owned(),
                );
            }
        }
    }
}

/// 把入站原始请求体按 trace_id 落盘（storeRequestBodies 启用时）。
/// gzip + fs 走 spawn_blocking，不占请求路径；失败仅告警。
fn persist_request_body(
    state: &AppState,
    tracer: &RequestTracer,
    raw_body: &Option<Extension<super::middleware::RawRequestBody>>,
) {
    let (Some(store), Some(Extension(raw))) = (&state.request_body_store, raw_body) else {
        return;
    };
    if !store.is_enabled() {
        return;
    }
    let store = store.clone();
    let trace_id = tracer.trace_id.clone();
    let bytes = raw.0.clone();
    tokio::task::spawn_blocking(move || store.save(&trace_id, &bytes));
}

impl RequestTracer {
    fn new(state: &AppState, options: RequestTraceOptions) -> Self {
        Self {
            store: state.trace_store.clone(),
            trace_id: Uuid::new_v4().to_string(),
            ts: Utc::now().to_rfc3339(),
            key_id: options.key_ctx.key_id,
            key_source: options.key_ctx.key_source,
            model: options.model,
            is_stream: options.is_stream,
            session_id: options.session_id,
            started_at: Instant::now(),
            first_token_at: parking_lot::Mutex::new(None),
            attempts: parking_lot::Mutex::new(Vec::new()),
            phases: parking_lot::Mutex::new(Vec::new()),
            open_phase: parking_lot::Mutex::new(None),
            shape: parking_lot::Mutex::new(super::stream::StreamShape::default()),
        }
    }

    /// 观察一批即将下发给客户端的 SSE 事件，累积流形态摘要。
    /// 必须在事件转字节的唯一出口调用，保证形态与客户端所见一致。
    pub fn observe_events(&self, events: &[super::stream::SseEvent]) {
        if self.store.is_none() || events.is_empty() {
            return;
        }
        let elapsed_ms = self.started_at.elapsed().as_millis() as u64;
        self.shape.lock().observe(events, elapsed_ms);
    }

    /// 标记首个上游 chunk 到达（幂等，仅记录第一次）
    pub fn mark_first_token(&self) {
        let mut slot = self.first_token_at.lock();
        if slot.is_none() {
            *slot = Some(Instant::now());
        }
    }

    /// 打开一个流生命周期段。重复 open 会覆盖前一个未关闭的段（视为埋点漏关，丢弃之）。
    pub fn open_phase(&self, name: &'static str) {
        *self.open_phase.lock() = Some((name, Instant::now()));
    }

    /// 关闭当前段并入队。名字不匹配或未 open 时静默忽略——埋点错误不得影响主流程。
    pub fn close_phase(
        &self,
        name: &'static str,
        outcome: &str,
        bytes: Option<u64>,
        detail: Option<String>,
    ) {
        let Some((open_name, started)) = self.open_phase.lock().take() else {
            return;
        };
        if open_name != name {
            return;
        }
        let mut phases = self.phases.lock();
        let seq = phases.len() as u32;
        phases.push(TracePhase {
            seq,
            phase: name.to_string(),
            started_ms: started.duration_since(self.started_at).as_millis() as u64,
            duration_ms: started.elapsed().as_millis() as u64,
            outcome: outcome.to_string(),
            bytes,
            detail,
        });
    }

    /// 组装并落库一条完整链路。store 为 None 时不做任何事。
    pub fn finalize(
        &self,
        final_status: &str,
        error_type: Option<&str>,
        error_message: Option<&str>,
        interrupted_after_bytes: Option<u64>,
        usage: TraceUsage,
    ) {
        let Some(store) = &self.store else { return };
        let attempts = std::mem::take(&mut *self.attempts.lock());
        let phases = std::mem::take(&mut *self.phases.lock());
        // 最终凭据：最后一跳的命中凭据（成功跳即命中凭据，失败跳即最后尝试的凭据）
        let final_credential_id = attempts.last().map(|a| a.credential_id).unwrap_or(0);
        let first_token_ms = self
            .first_token_at
            .lock()
            .map(|t| t.duration_since(self.started_at).as_millis() as u64);
        let (first_render_ms, stream_shape) = {
            let shape = self.shape.lock();
            (shape.first_render_ms(), shape.to_json())
        };
        let duration_ms = self.started_at.elapsed().as_millis() as u64;
        // 假活流（dead-air）分类：见 [`classify_dead_air`]。只在没有更高优先级
        // 分类时补记——已有 error_type 说明主因是别的故障（断流/截断/断连），
        // 主因决定处置动作，假活细节仍可从 stream_shape 追溯。
        // 纯计算、不新增任何可失败路径，落库失败与既有逻辑一样仅 warn。
        let dead_air = (error_type.is_none() && self.is_stream)
            .then(|| classify_dead_air(first_token_ms, first_render_ms, duration_ms))
            .flatten();
        let (error_type, error_message) = match dead_air {
            Some(msg) => (Some(outcome::DEAD_AIR), Some(msg)),
            None => (error_type, error_message.map(str::to_string)),
        };
        let rec = TraceRecord {
            trace_id: self.trace_id.clone(),
            ts: self.ts.clone(),
            key_id: self.key_id,
            key_source: self.key_source,
            operation: crate::admin::trace_db::operation::INFERENCE.to_string(),
            model: self.model.clone(),
            is_stream: self.is_stream,
            final_status: final_status.to_string(),
            final_credential_id,
            error_type: error_type.map(|s| s.to_string()),
            error_message,
            total_attempts: attempts.len() as u32,
            duration_ms,
            interrupted_after_bytes,
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cache_creation_tokens: usage.cache_creation_tokens,
            cache_read_tokens: usage.cache_read_tokens,
            credits: usage.credits,
            first_token_ms,
            first_render_ms,
            stream_shape,
            session_id: self.session_id.clone(),
            attempts,
            phases,
        };
        store.insert(&rec);
    }

    /// 客户端断开专用的收尾：`finalize` 在这条路径上原本永不会被调用——
    /// unfold 的 `None` 分支因为响应 body 被 axum 直接丢弃而永不执行，
    /// `StreamPhaseGuard::Drop` 是唯一能观测到断连的位置。复用 `finalize`
    /// 而不是重写一份落库逻辑，保持 attempts/phases 组装单一入口。
    ///
    /// `current_phase` 必须是 guard 实际打开的那个段名（FIRST_TOKEN 或
    /// STREAMING），不能硬编码——理由与 `StreamPhaseGuard::current_phase`
    /// 字段的文档一致。
    pub fn finalize_on_disconnect(&self, current_phase: &'static str, sent_bytes: u64, detail: String) {
        self.close_phase(
            current_phase,
            crate::admin::trace_db::outcome::CLIENT_DISCONNECTED,
            Some(sent_bytes),
            Some(detail.clone()),
        );
        // "interrupted" 与实时/缓冲流路径里上游断流复用同一个 final_status
        // （见 handlers.rs 中 STREAM_INTERRUPTED 的两处先例），仅 error_type
        // 换成 CLIENT_DISCONNECTED 以区分责任方。
        self.finalize(
            "interrupted",
            Some(crate::admin::trace_db::outcome::CLIENT_DISCONNECTED),
            Some(&detail),
            Some(sent_bytes),
            TraceUsage::zero(),
        );
    }
}

/// 假活流（dead-air）判定阈值：首个上游 chunk（first_token）到首个客户端
/// 可渲染帧（first_render）之间允许的最大间隔。
///
/// 依据：健康流实测 first_render ≈ first_token + 1ms（首个 chunk 里就带内容
/// delta）；病态案例是 712 秒无任何可渲染帧的假活。两者相差五个数量级，
/// 30 秒落在这条鸿沟中间——正常抖动（秒级以内）远够不着，真假活（分钟级）
/// 必被命中。刻意用常量而非配置项：没有任何已知场景需要把它调到别处，
/// 加配置只会稀释「超过它一定有病」这个判据的确定性。
const DEAD_AIR_THRESHOLD_MS: u64 = 30_000;

/// 假活流判定：流已开始（首字节已到）却长时间没有任何可渲染帧。
/// 命中时返回描述消息（作 error_message，进 ops 错误指纹），否则 None。
///
/// 两种形态共用同一判据：
/// - 首帧迟到：`first_render - first_token > 阈值`；
/// - 从未渲染（次要形态，做进来的理由）：`first_render` 为 None 时以流结束
///   时刻（`duration_ms`）代替——它是同一病灶的极端形（712s 病态案例若在
///   渲染前中止即属此类），漏掉它等于假活越严重反而越不可见。
///
/// `first_token` 为 None（一个字节都没来）不判：那类故障由既有
/// error/interrupted 分类负责，这里量的是「活着却不出活」。
fn classify_dead_air(
    first_token_ms: Option<u64>,
    first_render_ms: Option<u64>,
    duration_ms: u64,
) -> Option<String> {
    let first_token = first_token_ms?;
    let gap = first_render_ms
        .unwrap_or(duration_ms)
        .saturating_sub(first_token);
    if gap <= DEAD_AIR_THRESHOLD_MS {
        return None;
    }
    Some(match first_render_ms {
        Some(_) => format!("假活流: 首个可渲染帧滞后首字节 {gap}ms"),
        None => format!("假活流: 流结束仍无可渲染帧(首字节后 {gap}ms)"),
    })
}

impl TraceSink for RequestTracer {
    fn on_queue_wait(&self, credential_id: u64, waited_ms: u64, outcome: &str) {
        // 门禁排队不走 open/close 单槽（provider 侧无法保证配对），直接补记完整段。
        // 起点回推：上报即等待结束，now − waited 即入队时刻。
        let elapsed = self.started_at.elapsed().as_millis() as u64;
        let mut phases = self.phases.lock();
        let seq = phases.len() as u32;
        phases.push(TracePhase {
            seq,
            phase: phase::QUEUE.to_string(),
            started_ms: elapsed.saturating_sub(waited_ms),
            duration_ms: waited_ms,
            outcome: outcome.to_string(),
            bytes: None,
            detail: Some(format!("credential #{credential_id}")),
        });
    }

    fn on_attempt(&self, mut attempt: TraceAttempt) {
        // provider 只知道本跳耗时（它不持有请求起点），起点偏移在这里回推：
        // 本跳结束时刻 = 现在（provider 上报即结束），减去本跳耗时即起点。
        // 用 saturating_sub 兜住毫秒截断导致的 duration > elapsed 边界。
        let elapsed = self.started_at.elapsed().as_millis() as u64;
        attempt.started_ms = Some(elapsed.saturating_sub(attempt.duration_ms));
        self.attempts.lock().push(attempt);
    }
}

/// 挂在流状态里的哨兵：流被 drop 而未经由终态方法收尾时，说明客户端提前断开。
///
/// 需要它的原因：客户端断开时 axum 直接 drop response body，unfold 的
/// `None` 分支永不执行，`finalize` 也就永不调用——该请求会在 traces 里完全消失。
/// Drop 是唯一能观测到这件事的位置。
///
/// **方向性风险与防线**：`armed` 默认 `true`——任何未经解释的 drop 都会被记成
/// `client_disconnected`。这是刻意的：判定必须"宁可冤枉（错记成客户端断开），
/// 不可漏放（把真实的上游故障悄悄吞掉、代理逃过追责）"。防线是两个按值消费的
/// 终态方法 `into_completed` / `into_upstream_error`——它们各自解除 `armed`
/// 后再记录真实结果，编译器保证调用后 guard 被移走、`Drop` 变成 no-op。
/// 未来新增任何"流退出"分支，都必须经过这两个方法之一；否则一次遗漏就会把
/// 真实的上游故障悄悄记成客户端断开。
pub(crate) struct StreamPhaseGuard {
    tracer: std::sync::Arc<RequestTracer>,
    sent_bytes: u64,
    last_chunk_at: Instant,
    /// 见 [`StreamPhaseGuard::max_idle_ms`]
    max_idle_ms: u64,
    armed: bool,
    /// guard 构造时打开的是 FIRST_TOKEN 段；[`Self::mark_first_chunk`] 把它切到
    /// STREAMING。终态方法/Drop 必须按这个字段收尾，不能硬编码 STREAMING——
    /// 否则若 guard 在首个 chunk 到达前就被消费（上游零 chunk 断流、或客户端在
    /// 等待首个 token 期间断开），close_phase 会因段名不匹配把 FIRST_TOKEN 段
    /// 静默丢弃（见 close_phase 文档），这正是本功能要防止的"静默丢失"本身。
    current_phase: &'static str,
}

impl StreamPhaseGuard {
    pub fn new(tracer: std::sync::Arc<RequestTracer>, sent_bytes: u64) -> Self {
        Self {
            tracer,
            sent_bytes,
            last_chunk_at: Instant::now(),
            max_idle_ms: 0,
            armed: true,
            current_phase: phase::FIRST_TOKEN,
        }
    }

    /// 首个 chunk 到达：关闭 first_token 段，打开 streaming 段，并让 guard
    /// 记住当前打开的是哪个段。幂等由调用方的 first_chunk 标志保证
    /// （两条流路径各自维护，只在 first_chunk == true 时调用一次）。
    pub fn mark_first_chunk(&mut self) {
        self.tracer.close_phase(phase::FIRST_TOKEN, outcome::SUCCESS, Some(0), None);
        self.tracer.open_phase(phase::STREAMING);
        self.current_phase = phase::STREAMING;
    }

    /// 更新已发送字节数与最后一个 chunk 的时刻（每个 chunk 后调用）
    pub fn set_bytes(&mut self, sent_bytes: u64) {
        self.sent_bytes = sent_bytes;
        let gap = self.last_chunk_at.elapsed().as_millis() as u64;
        self.max_idle_ms = self.max_idle_ms.max(gap);
        self.last_chunk_at = Instant::now();
    }

    /// 距上一个 chunk 的间隔——区分「突然断」与「先卡死再断」
    pub fn idle_ms(&self) -> u64 {
        self.last_chunk_at.elapsed().as_millis() as u64
    }

    /// 收到响应头后，响应体各 chunk 之间的最长间隔（含首个 chunk 前与末尾未闭合段）。
    ///
    /// guard 在 `reqwest::Response` 返回后才创建，因此不覆盖请求发出到响应头返回的
    /// 等待；`read_timeout` 则从请求发出起生效。成功流记录该值可补充空闲阈值的观测
    /// 依据，但本地超时会截断超过阈值的样本，成功样本集存在选择偏差，不能据此证明
    /// 健康流与超时流互不重叠。
    pub fn max_idle_ms(&self) -> u64 {
        self.max_idle_ms.max(self.idle_ms())
    }

    /// 流正常结束：按值消费 guard，记当前实际打开的段（FIRST_TOKEN 或
    /// STREAMING，取决于是否已收到过 chunk）成功。
    pub fn into_completed(mut self, sent_bytes: u64) {
        self.armed = false; // 先解除，随后的 Drop 成为 no-op
        let max_idle_ms = self.max_idle_ms();
        self.tracer.close_phase(
            self.current_phase,
            crate::admin::trace_db::outcome::SUCCESS,
            Some(sent_bytes),
            // 成功流也写响应体阶段的 max_idle_ms，供后续结合超时样本校准阈值。
            Some(format!("max_idle_ms={}", max_idle_ms)),
        );
    }

    /// 上游断流：按值消费 guard，记当前实际打开的段为 stream_interrupted 并写齐三个判别位。
    pub fn into_upstream_error(mut self, sent_bytes: u64, err: &dyn std::fmt::Display) {
        self.armed = false;
        let idle_ms = self.idle_ms();
        self.tracer.close_phase(
            self.current_phase,
            crate::admin::trace_db::outcome::STREAM_INTERRUPTED,
            Some(sent_bytes),
            Some(format!(
                "client_gone=false bytes={} idle_ms={} max_idle_ms={} err={}",
                sent_bytes,
                idle_ms,
                self.max_idle_ms.max(idle_ms),
                err
            )),
        );
    }
}

impl Drop for StreamPhaseGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let detail = format!(
            "client_gone=true bytes={} idle_ms={}",
            self.sent_bytes,
            self.idle_ms()
        );
        // 断连时 finalize() 永不会被正常调用（见 finalize_on_disconnect 文档），
        // 这里必须连带落库，否则该请求会在 traces 表里完全消失。
        self.tracer
            .finalize_on_disconnect(self.current_phase, self.sent_bytes, detail);
    }
}

/// 流正常结束的 finish 段：按 tool_use 累积器结果开合。
///
/// streaming 段本身由 [`StreamPhaseGuard::into_completed`] 关闭，此处只管 finish——
/// **调用方必须保证 guard 已经消费完毕再调用本函数**：`open_phase` 会覆盖当前打开的段，
/// 若在 guard 消费前 open(FINISH)，guard 随后 close(STREAMING) 会因段名不匹配被静默
/// 忽略，streaming 段就此丢失且无任何报错（这是本任务的第一个陷阱）。
pub(crate) fn phase_on_finish(
    tracer: &RequestTracer,
    sent_bytes: u64,
    tool_json_error: Option<&ToolJsonAccumulatorError>,
    tool_json_message: Option<String>,
) {
    tracer.open_phase(phase::FINISH);
    let finish_outcome = match tool_json_error {
        Some(err) => crate::anthropic::stream::phase_outcome_for(err),
        None => outcome::SUCCESS,
    };
    tracer.close_phase(phase::FINISH, finish_outcome, Some(sent_bytes), tool_json_message);
}

/// 取追踪器里最后一跳的 outcome（用于把 provider 的失败分类提升到 record.error_type）。
/// 返回 'static str（outcome 常量），无 attempt 时返回 None。
fn last_attempt_outcome(tracer: &RequestTracer) -> Option<&'static str> {
    let last = tracer.attempts.lock().last()?.outcome.clone();
    Some(match last.as_str() {
        outcome::QUOTA_EXHAUSTED => outcome::QUOTA_EXHAUSTED,
        outcome::ACCOUNT_THROTTLED => outcome::ACCOUNT_THROTTLED,
        outcome::AUTH_FAILED => outcome::AUTH_FAILED,
        outcome::TRANSIENT => outcome::TRANSIENT,
        outcome::NETWORK_ERROR => outcome::NETWORK_ERROR,
        outcome::BAD_REQUEST => outcome::BAD_REQUEST,
        _ => outcome::UNKNOWN,
    })
}

/// Image-budget warning threshold (in raw base64 chars, not decoded bytes).
/// Emits a warning when the total base64 char count of all image content in one request exceeds this threshold.
/// The threshold does not reject the request (the upstream makes the final call); it only gives operators more precise diagnostics.
const IMAGE_BUDGET_WARN_BYTES: usize = 800 * 1024;

/// Budget statistics for the image content in one inbound request.
struct ImageBudget {
    count: usize,
    total_b64_bytes: usize,
    largest_b64_bytes: usize,
}

/// Counts the total number of images in the payload and their base64 byte size.
/// Looks only at inline base64 (image source.type == "base64"), skipping url-mode images (which do not
/// go directly into a Bedrock single message body). This is a lightweight O(N) scan that does not decode base64.
fn count_image_budget(payload: &super::types::MessagesRequest) -> ImageBudget {
    let mut count = 0usize;
    let mut total = 0usize;
    let mut largest = 0usize;
    for msg in &payload.messages {
        if let serde_json::Value::Array(arr) = &msg.content {
            for item in arr {
                if item.get("type").and_then(|v| v.as_str()) != Some("image") {
                    continue;
                }
                let Some(src) = item.get("source") else { continue };
                if src.get("type").and_then(|v| v.as_str()) != Some("base64") {
                    continue;
                }
                let n = src.get("data").and_then(|v| v.as_str()).map(|s| s.len()).unwrap_or(0);
                count += 1;
                total += n;
                if n > largest {
                    largest = n;
                }
            }
        }
    }
    ImageBudget {
        count,
        total_b64_bytes: total,
        largest_b64_bytes: largest,
    }
}

/// 将 KiroProvider 错误映射为 HTTP 响应
pub(super) fn map_provider_error(err: Error) -> Response {
    if let Some(rate_limit) = err.downcast_ref::<crate::kiro::error::UpstreamRateLimitError>() {
        tracing::warn!(error = %err, "上游限流（映射为 429）");
        let mut response = (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ErrorResponse::new(
                "rate_limit_error",
                "Upstream rate limit exceeded. Retry later.",
            )),
        )
            .into_response();
        if let Some(value) = rate_limit
            .retry_after()
            .and_then(|value| value.parse::<header::HeaderValue>().ok())
        {
            response.headers_mut().insert(header::RETRY_AFTER, value);
        }
        return response;
    }

    let err_str = err.to_string();

    // 上下文窗口满了（对话历史累积超出模型上下文窗口限制）
    if err_str.contains("CONTENT_LENGTH_EXCEEDS_THRESHOLD") {
        tracing::warn!(error = %err, "上游拒绝请求：上下文窗口已满（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Context window is full. Reduce conversation history, system prompt, or tools.",
            )),
        )
            .into_response();
    }

    // 单次输入太长（请求体本身超出上游限制）
    if err_str.contains("Input is too long") {
        tracing::warn!(error = %err, "上游拒绝请求：输入过长（不应重试）");
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Input is too long. Reduce the size of your messages.",
            )),
        )
            .into_response();
    }

    // Bedrock client-side validation errors (tool_use <-> tool_result mismatch, invalid message sequence, etc.)
    // The root cause is the client's own messages array, not an upstream failure, so it must not map to 5xx
    // otherwise it triggers an upstream cooldown that amplifies one client error into a 30+ burst of 503s.
    // Detection is centralized in the endpoint layer (single source of truth for the markers); the provider
    // already bails out without retry on these, and this mapping is the client-facing safety net.
    if crate::kiro::endpoint::default_is_client_validation_error(&err_str) {
        tracing::warn!(
            error = %err,
            "client messages array violates the protocol (Bedrock validation; mapped to 400 to avoid a false cooldown)"
        );
        // Return a stable, client-facing message and avoid echoing the raw upstream
        // error string (which can carry request IDs or internal validation details).
        // The full error is already logged above for diagnostics.
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(
                "invalid_request_error",
                "Invalid message sequence: tool_use and tool_result blocks must be correctly paired and ordered.".to_string(),
            )),
        )
            .into_response();
    }

    // 上游临时过载（AWS Kiro：500 MODEL_TEMPORARILY_UNAVAILABLE / "high load"）。
    // provider 已按瞬态错误重试 4 次仍失败。映射为 529 overloaded_error 而非 502：
    // 529 是 Anthropic 语义的"过载"，上游网关/客户端据此走"退避重试 / 切换账号"，
    // 而 502(bad gateway) 常被判为"该上游坏了"，反而放大故障转移。
    if err_str.contains("MODEL_TEMPORARILY_UNAVAILABLE")
        || err_str.contains("high load")
    {
        tracing::warn!(error = %err, "上游临时过载（映射为 529 overloaded_error）");
        let status = StatusCode::from_u16(529).unwrap_or(StatusCode::SERVICE_UNAVAILABLE);
        return (
            status,
            Json(ErrorResponse::new(
                "overloaded_error",
                "Upstream model temporarily unavailable due to high load. Retry later.",
            )),
        )
            .into_response();
    }

    tracing::error!("Kiro API 调用失败: {}", err);
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse::new(
            "api_error",
            "Upstream API request failed.",
        )),
    )
        .into_response()
}

/// 计算 Anthropic usage 口径的 input_tokens
fn resolve_usage_input_tokens(
    fallback_total_input_tokens: i32,
    context_total_input_tokens: Option<i32>,
) -> i32 {
    context_total_input_tokens.unwrap_or(fallback_total_input_tokens)
}

/// 可用模型列表。改造后查 `ModelRegistry`，不再硬编码。
fn available_models() -> Vec<Model> {
    crate::anthropic::model_registry::current_registry().exposed_models()
}

/// 按组支持集过滤 /v1/models 输出。
/// `allowed` 为 upstream id 集合;exposed id 先经注册表 resolve 再比对。
/// `auto` 恒保留;解析失败的行保留(保守,不因视图层吞行)。
fn filter_models_by_group(
    models: Vec<Model>,
    allowed: &std::collections::HashSet<String>,
) -> Vec<Model> {
    use crate::anthropic::model_registry::{current_registry, Resolution};
    let registry = current_registry();
    models
        .into_iter()
        .filter(|m| {
            if m.id == "auto" {
                return true;
            }
            match registry.resolve(&m.id, false) {
                Resolution::Mapped { upstream_id, .. }
                | Resolution::Passthrough { upstream_id, .. } => {
                    upstream_id == "auto" || allowed.contains(&upstream_id)
                }
                _ => true,
            }
        })
        .collect()
}

/// 请求转换失败 → 对客户端的 HTTP 响应（anthropic 路由，中文文案）。
///
/// **抽出来的唯一目的是可测**：`post_messages` 与 `post_messages_cc` 原先各内联
/// 一份完全相同的 `match` + `format!`，而要走到那段代码得有一个活的上游 provider，
/// 于是「请求一个未收录的模型到底拿到什么状态码、什么报文」这件事实际上从未被
/// 测过——只有一条比对字面量的哨兵测试，它证明不了路由真的这样响应。
///
/// **注意：web-search 路径（`websearch_loop::run_round`）另有一份英文文案，
/// 那是刻意的差异，不要顺手合并到这里。** 两份文案面向的客户端不同。
pub(crate) fn conversion_error_response(e: &ConversionError) -> (StatusCode, Json<ErrorResponse>) {
    let (error_type, message) = match e {
        ConversionError::UnsupportedModel(model) => {
            ("invalid_request_error", format!("模型不支持: {}", model))
        }
        ConversionError::ModelDisabled(model) => {
            ("invalid_request_error", format!("模型已禁用: {}", model))
        }
        ConversionError::EmptyMessages => ("invalid_request_error", "消息列表为空".to_string()),
        ConversionError::UnsupportedToolMapping(reason) => {
            ("invalid_request_error", format!("工具映射不支持: {}", reason))
        }
    };
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse::new(error_type, message)),
    )
}

/// 组支持校验的纯判定层：requested 先 resolve 成 upstream id 再比对。
/// 放行条件：auto / 解析失败（交给下游既有 400 路径）/ allowed 命中。
pub(crate) fn group_model_check_against(
    allowed: &std::collections::HashSet<String>,
    requested_model: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    use crate::anthropic::model_registry::{allow_passthrough, current_registry, Resolution};
    let upstream = match current_registry().resolve(requested_model, allow_passthrough()) {
        Resolution::Mapped { upstream_id, .. }
        | Resolution::Passthrough { upstream_id, .. } => upstream_id,
        _ => return Ok(()),
    };
    if upstream == "auto" || allowed.contains(&upstream) {
        return Ok(());
    }
    Err((
        StatusCode::NOT_FOUND,
        Json(ErrorResponse::new(
            "not_found_error",
            format!("model not supported for this key group: {}", requested_model),
        )),
    ))
}

/// 入口封装：取组支持集（None=不设限）后调用判定层。
pub(crate) fn group_model_check(
    state: &AppState,
    group: Option<&str>,
    requested_model: &str,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    let Some(provider) = &state.kiro_provider else { return Ok(()) };
    match provider.token_manager().group_supported_models(group) {
        Some(allowed) => group_model_check_against(&allowed, requested_model),
        None => Ok(()),
    }
}

/// GET /v1/models
///
/// 返回可用的模型列表
pub async fn get_models(
    State(state): State<AppState>,
    Extension(key_ctx): Extension<KeyContext>,
) -> impl IntoResponse {
    tracing::info!("Received GET /v1/models request");

    let mut models = available_models();
    if let Some(provider) = &state.kiro_provider
        && let Some(allowed) = provider
            .token_manager()
            .group_supported_models(key_ctx.group.as_deref())
    {
        models = filter_models_by_group(models, &allowed);
    }

    Json(ModelsResponse {
        object: "list".to_string(),
        data: models,
    })
}

/// 校验 max_tokens：必须为正数
///
/// 上游对 `max_tokens <= 0` 会返回难以定位的错误，这里在入口处提前拒绝。
fn validate_max_tokens(max_tokens: i32) -> Result<(), ErrorResponse> {
    if max_tokens <= 0 {
        Err(ErrorResponse::new(
            "invalid_request_error",
            "max_tokens must be greater than 0",
        ))
    } else {
        Ok(())
    }
}

/// POST /v1/messages
///
/// 创建消息（对话）
pub async fn post_messages(
    State(state): State<AppState>,
    Extension(key_ctx): Extension<KeyContext>,
    raw_body: Option<Extension<super::middleware::RawRequestBody>>,
    JsonExtractor(mut payload): JsonExtractor<MessagesRequest>,
) -> Response {
    // Count the image budget on inbound to provide precise diagnostics for later context-window-full errors
    let img_stats = count_image_budget(&payload);
    tracing::info!(
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        message_count = %payload.messages.len(),
        image_count = %img_stats.count,
        image_total_b64_kb = %(img_stats.total_b64_bytes / 1024),
        image_largest_b64_kb = %(img_stats.largest_b64_bytes / 1024),
        "Received POST /v1/messages request"
    );
    if let Err(error) = validate_max_tokens(payload.max_tokens) {
        return (StatusCode::BAD_REQUEST, Json(error)).into_response();
    }
    // omitted 轻量往返回程：先凭恢复键回填历史思考正文，
    // 再进 token 计数 / cache 计量 / 转换——上游所见与计量口径一致。
    if let Some(store) = &state.thinking_text_store {
        restore_omitted_thinking(&mut payload, store);
    }
    if img_stats.total_b64_bytes > IMAGE_BUDGET_WARN_BYTES {
        tracing::warn!(
            image_count = %img_stats.count,
            image_total_b64_kb = %(img_stats.total_b64_bytes / 1024),
            "incoming image payload is large; if upstream rejects with CONTENT_LENGTH_EXCEEDS_THRESHOLD, reduce image count or use lower-resolution screenshots"
        );
    }
    let hook = UsageRecordHook::from_state(&state, key_ctx.key_id, payload.model.clone(), key_ctx.group.clone());
    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            hook.record(0, 0, 0, (0, 0), 0.0, "error");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    // 组支持校验：请求的模型不在该 key 所属组的支持集内 → 404，不再路由上游吃 400
    if let Err(resp) = group_model_check(&state, key_ctx.group.as_deref(), &payload.model) {
        hook.record(0, 0, 0, (0, 0), 0.0, "error");
        return resp.into_response();
    }

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!("检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        let resp = websearch::handle_websearch_request(
            provider,
            &payload,
            input_tokens,
            key_ctx.group.as_deref(),
        )
        .await;
        // WebSearch 路径走 MCP 端点，没有 credential_id 上下文，统一记 0
        let status = if resp.status().is_success() { "success" } else { "error" };
        hook.record(0, input_tokens, 0, (0, 0), 0.0, status);
        return resp;
    }

    let payload_stream = payload.stream;
    // Mixed-tools (web_search + exec...) case: web_search coexists with other tools and falls onto the normal chat path,
    // where the upstream may return a tool_use with name=web_search. Take the internal agentic loop: search internally and feed the results back.
    if websearch::has_web_search_among_tools(&payload) {
        tracing::info!("detected mixed tools containing web_search, entering the web_search agentic loop");
        return super::websearch_loop::run_web_search_loop(provider, payload, hook, payload_stream, key_ctx.group.clone(), state.tool_compatibility_mode)
            .await;
    }

    // 转换请求
    let conversion_result = match convert_request_with_mode(&payload, state.tool_compatibility_mode) {
        Ok(result) => result,
        Err(e) => {
            tracing::warn!("请求转换失败: {}", e);
            hook.record(0, 0, 0, (0, 0), 0.0, "error");
            // 文案与状态码见 conversion_error_response（抽出来是为了可测）
            return conversion_error_response(&e).into_response();
        }
    };

    // Build the Kiro request. profile_arn is injected by the provider layer from the actual
    // credentials; additional_model_request_fields is already filtered by converter model support.
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        profile_arn: None,
        additional_model_request_fields: conversion_result.additional_model_request_fields,
    };

    let request_body = match serde_json::to_string(&kiro_request) {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("序列化请求失败: {}", e);
            hook.record(0, 0, 0, (0, 0), 0.0, "error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    format!("序列化请求失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    tracing::debug!("Kiro request body: {}", request_body);

    // 估算输入 tokens
    let total_input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system.clone(),
        payload.messages.clone(),
        payload.tools.clone(),
    ) as i32;

    // 检查是否启用了thinking
    let thinking_enabled = payload
        .thinking
        .as_ref()
        .map(|t| t.is_enabled())
        .unwrap_or(false);
    // display:"omitted"：思考正文不下发（签名=恢复键，回传时服务端恢复）
    let thinking_omitted = thinking_enabled
        && payload
            .thinking
            .as_ref()
            .and_then(|t| t.display.as_deref())
            == Some("omitted");

    let context_window = conversion_result.context_window;
    let tool_name_map = conversion_result.tool_name_map;
    let known_tool_names = conversion_result.known_tool_names;

    // CacheMeter：根据 cache_control 断点查 / 写中转层提示词缓存。
    // 返回 estimate 口径的覆盖量；真实 input/cache 互斥分摊在拿到 total 真值时进行。
    let cache_usage = state
        .cache_meter
        .as_ref()
        .map(|cache| super::cache_metering::compute_cache_usage(cache, &payload, key_ctx.key_id))
        .unwrap_or_default();

    if payload.stream {
        // 流式响应
        let tracer = std::sync::Arc::new(RequestTracer::new(
            &state,
            RequestTraceOptions {
                key_ctx: key_ctx.clone(),
                model: payload.model.clone(),
                is_stream: true,
                session_id: session_id_of(&payload),
            },
        ));
        persist_request_body(&state, &tracer, &raw_body);
        handle_stream_request(
            provider,
            &request_body,
            &payload.model,
            total_input_tokens,
            hook,
            tracer,
            ResponseProcessingConfig {
                thinking_enabled,
                thinking_omitted,
                thinking_text_store: state.thinking_text_store.clone(),
                tool_name_map,
                known_tool_names,
                cache_usage,
                group: key_ctx.group.clone(),
                context_window,
                sticky_key: dispatch_sticky_key(&payload),
            },
        )
        .await
    } else {
        // 非流式响应：仅在配置开启时提取 thinking 块
        let extract_thinking = state.extract_thinking && thinking_enabled;
        let tracer = std::sync::Arc::new(RequestTracer::new(
            &state,
            RequestTraceOptions {
                key_ctx: key_ctx.clone(),
                model: payload.model.clone(),
                is_stream: false,
                session_id: session_id_of(&payload),
            },
        ));
        persist_request_body(&state, &tracer, &raw_body);
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            total_input_tokens,
            hook,
            tracer,
            ResponseProcessingConfig {
                thinking_enabled: extract_thinking,
                thinking_omitted,
                thinking_text_store: state.thinking_text_store.clone(),
                tool_name_map,
                known_tool_names,
                cache_usage,
                group: key_ctx.group.clone(),
                context_window,
                sticky_key: dispatch_sticky_key(&payload),
            },
        )
        .await
    }
}

/// 处理流式请求
async fn handle_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    input_tokens: i32,
    hook: UsageRecordHook,
    tracer: std::sync::Arc<RequestTracer>,
    config: ResponseProcessingConfig,
) -> Response {
    let ResponseProcessingConfig {
        thinking_enabled,
        thinking_omitted,
        thinking_text_store,
        tool_name_map,
        known_tool_names,
        cache_usage,
        group,
        context_window,
        sticky_key,
    } = config;
    // 调用 Kiro API（支持多凭据故障转移）
    let call_result = match provider.call_api_stream(request_body, Some(tracer.as_ref()), group.as_deref(), sticky_key.as_deref()).await {
        Ok(resp) => resp,
        Err(e) => {
            hook.record(0, input_tokens, 0, (0, 0), 0.0, "error");
            // 重试链路全部失败、未开始返回内容：error_type 取最后一跳分类
            tracer.finalize("error", last_attempt_outcome(&tracer), Some(&e.to_string()), None, TraceUsage::zero());
            return map_provider_error(e);
        }
    };
    let response = call_result.response;
    let credential_id = call_result.credential_id;
    let ops_feedback =
        StreamOpsFeedback::from_call(&provider, credential_id, call_result.proxy_url.clone());

    // 创建流处理上下文
    let mut ctx = StreamContext::new_with_thinking(
        model,
        input_tokens,
        context_window,
        thinking_enabled,
        tool_name_map,
        known_tool_names,
    );
    ctx.set_thinking_omitted(thinking_omitted, thinking_text_store);
    ctx.cache_usage = cache_usage;

    // 生成初始事件
    let initial_events = ctx.generate_initial_events();

    // 创建 SSE 流；并发门禁 permit 挂进流闭包，流结束/断开时才释放凭据并发位
    let gate_permit = call_result.permit;
    let stream =
        create_sse_stream(response, ctx, initial_events, hook, credential_id, tracer, ops_feedback)
            .map(move |item| {
                let _keep = &gate_permit;
                item
            });

    // 返回 SSE 响应
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// Ping 事件间隔（25秒）
const PING_INTERVAL_SECS: u64 = 25;

/// 流中断时的运维反馈句柄。
///
/// 流式响应 2xx 后 provider 已退出重试循环，中断只能在流处理层发现；
/// 这里把「凭据 + 实际所用代理」带进流状态，中断时反馈给 [`OpsRuntime`]
/// （计入代理请求级失败，达阈值自动禁用+换绑）。ops 未启用时为 None，零开销。
pub(crate) struct StreamOpsFeedback {
    ops: crate::admin::ops::SharedOpsRuntime,
    credential_id: u64,
    proxy_url: Option<String>,
}

impl StreamOpsFeedback {
    /// 从 provider 与调用结果构造；ops 未注入时返回 None
    pub(crate) fn from_call(
        provider: &crate::kiro::provider::KiroProvider,
        credential_id: u64,
        proxy_url: Option<String>,
    ) -> Option<Self> {
        provider.ops_runtime().map(|ops| Self {
            ops,
            credential_id,
            proxy_url,
        })
    }

    /// 请求完整送达：提交一次代理成功（清零请求级失败计数）
    fn report_success(&self) {
        self.ops.report_proxy_success(self.proxy_url.as_deref());
    }

    /// 传输链路失败（连接已建立但响应未完整送达：流断开 / 上游截断）：
    /// 提交一次代理失败（累计，达阈值自动禁用+换绑）
    fn report_transport_failure(&self, error: &str) {
        self.ops
            .report_stream_interrupted(self.credential_id, self.proxy_url.as_deref(), error);
    }
}

/// 请求终态代理反馈：每个外部请求只调用一次。
/// - `transport_failed = true`：连接已建立但响应未完整（流断开 / 上游截断）→ 计代理失败
/// - `transport_failed = false`：请求完整送达 → 清零；
///   上游内容错误（InvalidJson 等非传输问题）不应走这里，直接不调用即可（no-op）。
fn report_stream_outcome(ops: &Option<StreamOpsFeedback>, transport_failed: bool, error: &str) {
    let Some(fb) = ops else { return };
    if transport_failed {
        fb.report_transport_failure(error);
    } else {
        fb.report_success();
    }
}

/// 创建 ping 事件的 SSE 字符串
fn create_ping_sse() -> Bytes {
    Bytes::from("event: ping\ndata: {\"type\": \"ping\"}\n\n")
}

/// 创建 SSE 事件流
fn create_sse_stream(
    response: reqwest::Response,
    ctx: StreamContext,
    initial_events: Vec<SseEvent>,
    hook: UsageRecordHook,
    credential_id: u64,
    tracer: std::sync::Arc<RequestTracer>,
    ops_feedback: Option<StreamOpsFeedback>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    // 先发送初始事件（同时喂形态摘要——观察必须发生在对应 finalize 之前）
    tracer.observe_events(&initial_events);
    let initial_stream = stream::iter(
        initial_events
            .into_iter()
            .map(|e| Ok(Bytes::from(e.to_sse_string()))),
    );

    // 然后处理 Kiro 响应流，同时每25秒发送 ping 保活
    let body_stream = response.bytes_stream();

    // 段埋点：first_token 段在此打开，guard 承载 streaming 段（详见 StreamPhaseGuard 文档）。
    tracer.open_phase(phase::FIRST_TOKEN);
    let guard = StreamPhaseGuard::new(tracer.clone(), 0);

    let processing_stream = stream::unfold(
        (body_stream, ctx, EventStreamDecoder::new(), false, interval(Duration::from_secs(PING_INTERVAL_SECS)), hook, credential_id, tracer, 0u64, ops_feedback, true, Some(guard)),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval, hook, credential_id, tracer, mut sent_bytes, ops_feedback, mut first_chunk, mut guard)| async move {
            if finished {
                return None;
            }

            // 使用 select! 同时等待数据和 ping 定时器
            tokio::select! {
                // 处理数据流
                chunk_result = body_stream.next() => {
                    match chunk_result {
                        Some(Ok(chunk)) => {
                            tracer.mark_first_token();
                            sent_bytes += chunk.len() as u64;
                            if let Some(g) = guard.as_mut() {
                                if first_chunk {
                                    g.mark_first_chunk();
                                }
                                g.set_bytes(sent_bytes);
                            }
                            first_chunk = false;
                            // 解码事件
                            if let Err(e) = decoder.feed(&chunk) {
                                tracing::warn!("缓冲区溢出: {}", e);
                            }

                            let mut events = Vec::new();
                            for result in decoder.decode_iter() {
                                match result {
                                    Ok(frame) => {
                                        if let Ok(event) = Event::from_frame(frame) {
                                            let sse_events = ctx.process_kiro_event(&event);
                                            events.extend(sse_events);
                                        }
                                    }
                                    Err(e) => {
                                        tracing::warn!("解码事件失败: {}", e);
                                    }
                                }
                            }

                            // 转换为 SSE 字节流（先喂形态摘要）
                            tracer.observe_events(&events);
                            let bytes: Vec<Result<Bytes, Infallible>> = events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();

                            Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback, first_chunk, guard)))
                        }
                        Some(Err(e)) => {
                            // reqwest 把 body 阶段所有失败压成同一句 Display，归因靠 source 链
                            let detail = describe_reqwest_error(&e);
                            tracing::error!("读取响应流失败: {}", detail);
                            // 按值消费 guard：本分支已显式处理结局，Drop 不得再判客户端断开。
                            if let Some(g) = guard.take() {
                                g.into_upstream_error(sent_bytes, &detail);
                            }
                            // 发送最终事件并结束（记为 error）
                            let final_events = ctx.generate_final_events();
                            tracer.observe_events(&final_events);
                            record_stream_usage(&hook, &ctx, credential_id, "error");
                            // 连接已建立后断流 = 传输链路失败，计入所用代理
                            report_stream_outcome(&ops_feedback, true, &detail);
                            // 已开始返回内容后上游断流：标记为 interrupted，带已发送字节数
                            tracer.finalize(
                                "interrupted",
                                Some(outcome::STREAM_INTERRUPTED),
                                Some(&detail),
                                Some(sent_bytes),
                                stream_trace_usage(&ctx),
                            );
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback, first_chunk, guard)))
                        }
                        None => {
                            // 流正常结束：先按值消费 guard 收尾 streaming 段，再打开 finish 段——
                            // 顺序不可反（见 phase_on_finish 文档的陷阱说明）。
                            if let Some(g) = guard.take() {
                                g.into_completed(sent_bytes);
                            }
                            // 流结束，发送最终事件（generate_final_events 内部会 finish()
                            // 累积器，据此判定是否有半截 / 非法工具调用 JSON）。
                            let final_events = ctx.generate_final_events();
                            tracer.observe_events(&final_events);
                            phase_on_finish(
                                &tracer,
                                sent_bytes,
                                ctx.tool_json_error(),
                                ctx.tool_json_error_message(),
                            );
                            if let Some(message) = ctx.tool_json_error_message() {
                                // 工具调用 JSON 半截 / 非法：实时流已回 200，无法改状态码，
                                // 只能记 error 并让 generate_final_events 补发的 `error` 事件透传给客户端。
                                // 区分两类：IncompleteJson = 上游截断（传输链路，计代理失败）；
                                // InvalidJson = 上游返回完整但非法 JSON（内容问题，不罚代理）。
                                let incomplete =
                                    ctx.tool_json_error_incomplete().unwrap_or(true);
                                record_stream_usage(&hook, &ctx, credential_id, "error");
                                report_stream_outcome(&ops_feedback, incomplete, &message);
                                tracer.finalize(
                                    "error",
                                    Some(if incomplete {
                                        outcome::UPSTREAM_TRUNCATED
                                    } else {
                                        outcome::UPSTREAM_INVALID
                                    }),
                                    Some(&message),
                                    Some(sent_bytes),
                                    stream_trace_usage(&ctx),
                                );
                            } else {
                                record_stream_usage(&hook, &ctx, credential_id, "success");
                                // 请求完整送达：提交一次代理成功（清零请求级失败计数）
                                report_stream_outcome(&ops_feedback, false, "");
                                tracer.finalize(
                                    "success",
                                    None,
                                    None,
                                    None,
                                    stream_trace_usage(&ctx),
                                );
                            }
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback, first_chunk, guard)))
                        }
                    }
                }
                // 发送 ping 保活
                _ = ping_interval.tick() => {
                    tracing::trace!("发送 ping 保活事件");
                    let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                    Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback, first_chunk, guard)))
                }
            }
        },
    )
    .flatten();

    initial_stream.chain(processing_stream)
}

/// 从 StreamContext 提取最终用量并写入 hook
fn record_stream_usage(
    hook: &UsageRecordHook,
    ctx: &StreamContext,
    credential_id: u64,
    status: &str,
) {
    // 互斥分摊后的 (input, cache_creation, cache_read)，与 trace 上报口径一致。
    let (input, cache_creation, cache_read) = ctx.resolved_usage();
    hook.record(
        credential_id,
        input,
        ctx.output_tokens,
        (cache_creation, cache_read),
        ctx.credits,
        status,
    );
}

/// 从 StreamContext 提取用量，转成 trace 行用量（与 record_stream_usage 同源）
fn stream_trace_usage(ctx: &StreamContext) -> TraceUsage {
    let (input, cache_creation, cache_read) = ctx.resolved_usage();
    TraceUsage {
        input_tokens: input.max(0) as u64,
        output_tokens: ctx.output_tokens.max(0) as u64,
        cache_creation_tokens: cache_creation.max(0) as u64,
        cache_read_tokens: cache_read.max(0) as u64,
        credits: if ctx.credits.is_finite() && ctx.credits > 0.0 { ctx.credits } else { 0.0 },
    }
}

/// 处理非流式请求
/// 非流式 body 读取失败
#[derive(Debug)]
struct NonStreamBodyError {
    /// 已归因的错误描述（同 `describe_reqwest_error`）
    detail: String,
    /// 断开前已从上游收到的字节数——区分「一个字节都没来」与「收了一半断了」
    sent_bytes: u64,
}

/// 逐 chunk 收完非流式响应体，顺路量出首字节延迟并分段。
///
/// 为什么不用 `response.bytes()`：整段生成都塌在那一个 await 里，拆不开——
/// 非流式请求慢，到底是等上游想（首字节前）还是传得慢（首字节后），日志里分不出。
/// 逐 chunk 读之后 `first_token` 与 `body_read` 才成为两段。
///
/// **超时语义不变**：两条聊天路径共用 provider 的流式 client，同时受可配置的
/// `read_timeout` 空闲超时与绝对总超时约束；逐 chunk 读与一把梭读受相同约束。
///
/// 累积完再一次性 `feed` 给 decoder，与原先按整块喂完全一致——响应内容零变化。
async fn read_non_stream_body(
    response: reqwest::Response,
    tracer: &RequestTracer,
) -> Result<Vec<u8>, NonStreamBodyError> {
    tracer.open_phase(phase::FIRST_TOKEN);
    let mut stream = response.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut first_seen = false;
    while let Some(chunk) = stream.next().await {
        match chunk {
            Ok(bytes) => {
                // 空 data frame（h2 允许）不算"首字节到达"：算了的话 first_token_ms
                // 会提前、且后续读失败会被误归因到 BODY_READ 而非 FIRST_TOKEN。
                // 空 chunk 仍照常参与循环，只是不推进段状态。
                if !first_seen && !bytes.is_empty() {
                    first_seen = true;
                    tracer.mark_first_token();
                    tracer.close_phase(phase::FIRST_TOKEN, outcome::SUCCESS, None, None);
                    tracer.open_phase(phase::BODY_READ);
                }
                buf.extend_from_slice(&bytes);
            }
            Err(e) => {
                let detail = describe_reqwest_error(&e);
                let sent = buf.len() as u64;
                // 关在首字节前还是首字节后，决定了这次失败该归因给「上游没响应」
                // 还是「传输中断」——两段各自的 outcome 都要落库。
                let open = if first_seen {
                    phase::BODY_READ
                } else {
                    phase::FIRST_TOKEN
                };
                tracer.close_phase(
                    open,
                    outcome::STREAM_INTERRUPTED,
                    Some(sent),
                    Some(detail.clone()),
                );
                return Err(NonStreamBodyError {
                    detail,
                    sent_bytes: sent,
                });
            }
        }
    }
    let total = buf.len() as u64;
    if first_seen {
        tracer.close_phase(phase::BODY_READ, outcome::SUCCESS, Some(total), None);
    } else {
        // 上游返回 2xx 但 body 为空：first_token 段永远等不到 chunk，
        // 在此显式关段，否则它会一直挂着、最终被 finalize 丢弃（埋点漏关）。
        tracer.close_phase(
            phase::FIRST_TOKEN,
            outcome::UPSTREAM_TRUNCATED,
            Some(0),
            Some("上游响应体为空".to_string()),
        );
    }
    Ok(buf)
}

async fn handle_non_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    input_tokens: i32,
    hook: UsageRecordHook,
    tracer: std::sync::Arc<RequestTracer>,
    config: ResponseProcessingConfig,
) -> Response {
    let ResponseProcessingConfig {
        thinking_enabled,
        thinking_omitted,
        thinking_text_store,
        tool_name_map,
        // 非流式路径直接处理结构化 Event::ToolUse，不经过 <invoke> 文本嗅探。
        known_tool_names: _,
        cache_usage,
        group,
        // 请求入口只取一次窗口快照，避免响应阶段读取热重载后的新注册表。
        context_window,
        sticky_key,
    } = config;
    // 调用 Kiro API（支持多凭据故障转移）
    let call_result = match provider.call_api(request_body, Some(tracer.as_ref()), group.as_deref(), sticky_key.as_deref()).await {
        Ok(resp) => resp,
        Err(e) => {
            hook.record(0, input_tokens, 0, (0, 0), 0.0, "error");
            tracer.finalize("error", last_attempt_outcome(&tracer), Some(&e.to_string()), None, TraceUsage::zero());
            return map_provider_error(e);
        }
    };
    let response = call_result.response;
    let credential_id = call_result.credential_id;
    // 并发门禁 permit：持有到本函数返回（响应体在此函数内读完），显式命名防误删
    let _gate_permit = call_result.permit;
    let ops_feedback =
        StreamOpsFeedback::from_call(&provider, credential_id, call_result.proxy_url.clone());

    // 读取响应体
    let body_bytes = match read_non_stream_body(response, tracer.as_ref()).await {
        Ok(bytes) => bytes,
        Err(NonStreamBodyError { detail, sent_bytes }) => {
            tracing::error!("读取响应体失败: {}", detail);
            hook.record(credential_id, input_tokens, 0, (0, 0), 0.0, "error");
            // 连接已建立后 body 读取失败 = 传输链路失败，计入所用代理
            report_stream_outcome(&ops_feedback, true, &detail);
            tracer.finalize(
                "interrupted",
                Some(outcome::STREAM_INTERRUPTED),
                Some(&detail),
                // 非流式没向客户端发过字节，这里记的是「从上游收到多少就断了」，
                // 与流式的「已下发多少」语义不同；沿用同一字段是为了让 UI
                // 统一显示「中断前 N 字节」，判断是空响应还是半截。
                Some(sent_bytes),
                TraceUsage::zero(),
            );
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse::new(
                    "api_error",
                    format!("读取响应失败: {}", detail),
                )),
            )
                .into_response();
        }
    };

    // 解析事件流
    tracer.open_phase(phase::DECODE);
    let mut decoder = EventStreamDecoder::new();
    // feed 失败（缓冲区溢出）保持既有行为——继续用已解出的部分组装响应，不改状态码。
    // 但必须把错误留下来喂给 close_phase：否则一次确凿的解码失败会被记成 success，
    // 污染刚建立的 phase 成功率基线。埋点不得比它所观测的现实更乐观。
    let decode_error = decoder.feed(&body_bytes).err().map(|e| {
        tracing::warn!("缓冲区溢出: {}", e);
        e.to_string()
    });

    let mut text_content = String::new();
    let mut native_thinking = String::new();
    let mut native_redacted_thinking: Vec<String> = Vec::new();
    let mut tool_uses: Vec<serde_json::Value> = Vec::new();
    let mut has_tool_use = false;
    let mut stop_reason = "end_turn".to_string();
    // 从 contextUsageEvent 计算的实际输入 tokens
    let mut context_input_tokens: Option<i32> = None;
    // meteringEvent 上报的 credit 计费量（上游真实下发）；
    // input/cache_* 的互斥分摊在拿到 total 真值后由 cache_usage 完成。
    let mut credits: f64 = 0.0;
    // 最近一次 meteringEvent 的完整 payload，用于在响应体 usage 中透传
    // credit_usage / credit_unit / credit_unit_plural 字段，与 /v1/messages
    // 流式（message_delta）行为一致；如果上游多次下发则取最后一次。
    let mut metering: Option<crate::kiro::model::events::MeteringEvent> = None;

    // 工具调用参数 JSON 累积器：按 tool_use_id 缓冲分片，stop 时整体解析。
    // 半截 / 非法 JSON 显式暴露为错误（返回 502），不再静默回退 {} 或丢弃。
    let mut tool_accumulator = super::stream::ToolJsonAccumulator::new();
    let mut tool_json_error: Option<super::stream::ToolJsonAccumulatorError> = None;

    for result in decoder.decode_iter() {
        match result {
            Ok(frame) => {
                if let Ok(event) = Event::from_frame(frame) {
                    match event {
                        Event::AssistantResponse(resp) => {
                            text_content.push_str(&resp.content);
                        }
                        Event::ReasoningContent(reasoning) => {
                            if let Some(text) = reasoning.text
                                && !text.is_empty()
                            {
                                native_thinking.push_str(&text);
                            }
                            if let Some(redacted) = reasoning.redacted_content
                                && !redacted.is_empty()
                            {
                                native_redacted_thinking.push(redacted);
                            }
                        }
                        Event::ToolUse(tool_use) => {
                            has_tool_use = true;
                            match tool_accumulator.push(&tool_use, &tool_name_map) {
                                Ok(Some(completed)) => {
                                    tool_uses.push(completed.to_anthropic_block());
                                }
                                Ok(None) => {}
                                Err(e) => {
                                    tracing::error!("{}", e);
                                    tool_json_error = Some(e);
                                }
                            }
                        }
                        Event::ContextUsage(context_usage) => {
                            // 从上下文使用百分比计算实际的 input_tokens
                            // 窗口值由请求入口随 ConversionResult 传入，不再回头查全局注册表
                            let window_size = context_window;
                            let actual_input_tokens =
                                (context_usage.context_usage_percentage * (window_size as f64)
                                    / 100.0) as i32;
                            context_input_tokens = Some(actual_input_tokens);
                            // 上下文使用量达到 100% 时，设置 stop_reason 为 model_context_window_exceeded
                            if context_usage.context_usage_percentage >= 100.0 {
                                stop_reason = "model_context_window_exceeded".to_string();
                            }
                            tracing::debug!(
                                "收到 contextUsageEvent: {}%, 计算 input_tokens: {}",
                                context_usage.context_usage_percentage,
                                actual_input_tokens
                            );
                        }
                        Event::Metering(event_metering) => {
                            // 上游只下发 credit；token / cache 字段不存在
                            credits += event_metering.usage;
                            tracing::debug!(
                                usage = event_metering.usage,
                                unit = %event_metering.unit,
                                unit_plural = %event_metering.unit_plural,
                                "metering credits +{:.6}", event_metering.usage
                            );
                            metering = Some(event_metering);
                        }
                        Event::Exception { exception_type, .. }
                            if exception_type == "ContentLengthExceededException" => {
                                stop_reason = "max_tokens".to_string();
                            }
                        _ => {}
                    }
                }
            }
            Err(e) => {
                tracing::warn!("解码事件失败: {}", e);
            }
        }
    }

    // 收尾：若仍有未收到 stop=true 的工具调用缓冲（上游在参数写到一半时截断），
    // finish() 返回 IncompleteJson。已有错误则保持不变。
    if tool_json_error.is_none()
        && let Err(e) = tool_accumulator.finish()
    {
        tracing::error!("{}", e);
        tool_json_error = Some(e);
    }

    // decode 段到此结束（tool json 判定属于解码结果的一部分）。
    //
    // 优先级：帧解码失败 > tool json 失败。前者说明字节流本身就没读全（缓冲区溢出），
    // 后者只是解出来的内容不合法；先报更靠底层的那个，归因才不会指错方向。
    let (decode_outcome, decode_detail) = match (&decode_error, &tool_json_error) {
        (Some(e), _) => (outcome::UPSTREAM_TRUNCATED, Some(format!("帧解码失败: {e}"))),
        (None, Some(err)) => (
            if err.is_incomplete() {
                outcome::UPSTREAM_TRUNCATED
            } else {
                outcome::UPSTREAM_INVALID
            },
            Some(err.message()),
        ),
        (None, None) => (outcome::SUCCESS, None),
    };
    tracer.close_phase(
        phase::DECODE,
        decode_outcome,
        Some(body_bytes.len() as u64),
        decode_detail,
    );

    // 工具调用 JSON 半截 / 非法：非流式路径尚未发送任何字节，直接回 502，
    // 明确暴露上游问题，而不是把无法解析的参数当成完整调用返回。
    // 区分上游截断（传输，计代理失败 + upstream_truncated）与非法 JSON（内容，不罚代理 + upstream_invalid）。
    if let Some(err) = tool_json_error {
        let incomplete = err.is_incomplete();
        let message = err.message();
        hook.record(credential_id, input_tokens, 0, (0, 0), 0.0, "error");
        report_stream_outcome(&ops_feedback, incomplete, &message);
        tracer.finalize(
            "error",
            Some(if incomplete {
                outcome::UPSTREAM_TRUNCATED
            } else {
                outcome::UPSTREAM_INVALID
            }),
            Some(&message),
            None,
            TraceUsage::zero(),
        );
        return (
            StatusCode::BAD_GATEWAY,
            Json(ErrorResponse::new("upstream_tool_json_error", message)),
        )
            .into_response();
    }

    tracer.open_phase(phase::ASSEMBLE);

    // 确定 stop_reason
    if has_tool_use && stop_reason == "end_turn" {
        stop_reason = "tool_use".to_string();
    }

    // 剥离混入文本的字面 <tool_use> XML 泄漏（非流式：整段文本已就绪，一次性剥离）。
    let text_content = crate::kiro::model::events::strip_tool_use_xml_leaks(&text_content);

    // 构建响应内容
    let mut content = build_non_stream_content(
        thinking_enabled,
        text_content,
        native_thinking,
        native_redacted_thinking,
        thinking_omitted,
        thinking_text_store.as_deref(),
    );
    content.extend(tool_uses);

    // 估算输出 tokens（上游不下发 token，全部走估算）
    let output_tokens = token::estimate_output_tokens(&content);

    // 输入 tokens：contextUsage 真实值优先，否则用客户端估算
    let total_input_tokens = resolve_usage_input_tokens(input_tokens, context_input_tokens);
    // 互斥分摊：input + cache_creation + cache_read == total
    let (final_input_tokens, cache_creation_tokens, cache_read_tokens) =
        cache_usage.split_against_total(total_input_tokens);

    // 构建 Anthropic 响应
    let mut usage_json = json!({
        "input_tokens": final_input_tokens,
        "output_tokens": output_tokens,
        "cache_creation_input_tokens": cache_creation_tokens,
        "cache_read_input_tokens": cache_read_tokens
    });
    // 透传上游 meteringEvent 的 credit_* 字段，让客户端拿到与 Kiro
    // 后端口径一致的计费元数据；只在收到过 meteringEvent 时才追加。
    if let Some(m) = &metering {
        usage_json["credit_usage"] = json!(m.usage);
        usage_json["credit_unit"] = json!(m.unit);
        usage_json["credit_unit_plural"] = json!(m.unit_plural);
    }
    let response_body = json!({
        "id": format!("msg_{}", Uuid::new_v4().to_string().replace('-', "")),
        "type": "message",
        "role": "assistant",
        "content": content,
        "model": model,
        "stop_reason": stop_reason,
        "stop_sequence": null,
        "usage": usage_json
    });

    tracer.close_phase(phase::ASSEMBLE, outcome::SUCCESS, None, None);

    hook.record(
        credential_id,
        final_input_tokens,
        output_tokens,
        (cache_creation_tokens, cache_read_tokens),
        credits,
        "success",
    );
    // 非流式请求完整送达：提交一次代理成功（清零请求级失败计数）
    report_stream_outcome(&ops_feedback, false, "");
    tracer.finalize(
        "success",
        None,
        None,
        None,
        TraceUsage {
            input_tokens: final_input_tokens.max(0) as u64,
            output_tokens: output_tokens.max(0) as u64,
            cache_creation_tokens: cache_creation_tokens.max(0) as u64,
            cache_read_tokens: cache_read_tokens.max(0) as u64,
            credits: if credits.is_finite() && credits > 0.0 { credits } else { 0.0 },
        },
    );
    (StatusCode::OK, Json(response_body)).into_response()
}

fn build_non_stream_content(
    thinking_enabled: bool,
    text_content: String,
    native_thinking: String,
    native_redacted_thinking: Vec<String>,
    thinking_omitted: bool,
    thinking_text_store: Option<&crate::admin::request_body_store::RequestBodyStore>,
) -> Vec<serde_json::Value> {
    // omitted：正文不下发，存 blob 换恢复键（与流式 thinking_close_signature 同语义）
    let omitted_signature = |text: &str| -> Option<String> {
        if !thinking_omitted || text.is_empty() {
            return None;
        }
        let store = thinking_text_store?;
        let id = uuid::Uuid::new_v4().to_string();
        store.save(&id, text.as_bytes());
        Some(format!("kiro-thinking-v1:{}", id))
    };
    let mut content = Vec::new();
    let has_native_thinking = !native_thinking.is_empty();

    if thinking_enabled {
        if has_native_thinking {
            if let Some(sig) = omitted_signature(&native_thinking) {
                content.push(json!({
                    "type": "thinking",
                    "thinking": "",
                    "signature": sig,
                }));
            } else {
                content.push(json!({
                    "type": "thinking",
                    "thinking": native_thinking.clone(),
                    // 真签名不透传（与流式路径一致）：回传即被 serde 丢弃，只膨胀历史。
                    "signature": super::stream::THINKING_SIGNATURE_PLACEHOLDER,
                }));
            }
        } else {
            // 从完整文本中提取 thinking 块，兼容旧的 <thinking> 文本路径。
            let (thinking, remaining_text) =
                super::stream::extract_thinking_from_complete_text(&text_content);

            if let Some(thinking_text) = thinking {
                if let Some(sig) = omitted_signature(&thinking_text) {
                    content.push(json!({
                        "type": "thinking",
                        "thinking": "",
                        "signature": sig,
                    }));
                } else {
                    content.push(json!({
                        "type": "thinking",
                        "thinking": thinking_text,
                        "signature": super::stream::THINKING_SIGNATURE_PLACEHOLDER,
                    }));
                }
            }

            if !remaining_text.is_empty() {
                content.push(json!({
                    "type": "text",
                    "text": remaining_text
                }));
            }
        }

        for redacted in native_redacted_thinking {
            content.push(json!({
                "type": "redacted_thinking",
                "data": redacted
            }));
        }

        if has_native_thinking && !text_content.is_empty() {
            content.push(json!({
                "type": "text",
                "text": text_content
            }));
        }
    } else if !text_content.is_empty() {
        content.push(json!({
            "type": "text",
            "text": text_content
        }));
    } else if has_native_thinking {
        content.push(json!({
            "type": "text",
            "text": native_thinking
        }));
    }
    content
}

/// 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
///
/// - Opus 4.6：覆写为 adaptive 类型
/// - 其他模型：覆写为 enabled 类型
/// - budget_tokens 固定为 20000
fn override_thinking_from_model_name(payload: &mut MessagesRequest) {
    let model_lower = payload.model.to_lowercase();
    if !model_lower.contains("thinking") {
        return;
    }

    let is_opus_4_6 = model_lower.contains("opus")
        && (model_lower.contains("4-6") || model_lower.contains("4.6"));

    let thinking_type = if is_opus_4_6 { "adaptive" } else { "enabled" };

    tracing::info!(
        model = %payload.model,
        thinking_type = thinking_type,
        "模型名包含 thinking 后缀，覆写 thinking 配置"
    );

    // 保留客户端原有的 display 声明（-thinking 后缀只覆写开关，不覆写展示模式）
    let display = payload.thinking.as_ref().and_then(|t| t.display.clone());
    payload.thinking = Some(Thinking {
        thinking_type: thinking_type.to_string(),
        budget_tokens: 20000,
        display,
    });

    if is_opus_4_6 {
        payload.output_config = Some(OutputConfig {
            effort: "high".to_string(),
        });
    }
}

/// POST /v1/messages/count_tokens
///
/// 计算消息的 token 数量
pub async fn count_tokens(
    State(state): State<AppState>,
    Extension(key_ctx): Extension<KeyContext>,
    JsonExtractor(payload): JsonExtractor<CountTokensRequest>,
) -> Response {
    tracing::info!(
        model = %payload.model,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages/count_tokens request"
    );

    // 组支持校验：即便本端点是纯本地计数、不路由上游，视图仍需与组对齐 ——
    // 客户端不该在 count_tokens 上看到一个之后 /v1/messages 会 404 的模型。
    if let Err(resp) = group_model_check(&state, key_ctx.group.as_deref(), &payload.model) {
        return resp.into_response();
    }

    let total_tokens = token::count_all_tokens(
        payload.model,
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

    Json(CountTokensResponse {
        input_tokens: total_tokens.max(1),
    })
    .into_response()
}

/// POST /cc/v1/messages
///
/// Claude Code 兼容端点，与 /v1/messages 的区别在于：
/// - 流式响应会等待 kiro 端返回 contextUsageEvent 后再发送 message_start
/// - message_start 中的 input_tokens 是从 contextUsageEvent 计算的准确值
pub async fn post_messages_cc(
    State(state): State<AppState>,
    Extension(key_ctx): Extension<KeyContext>,
    raw_body: Option<Extension<super::middleware::RawRequestBody>>,
    JsonExtractor(mut payload): JsonExtractor<MessagesRequest>,
) -> Response {
    tracing::info!(
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        message_count = %payload.messages.len(),
        "Received POST /cc/v1/messages request"
    );
    if let Err(error) = validate_max_tokens(payload.max_tokens) {
        return (StatusCode::BAD_REQUEST, Json(error)).into_response();
    }
    // omitted 轻量往返回程：先凭恢复键回填历史思考正文，
    // 再进 token 计数 / cache 计量 / 转换——上游所见与计量口径一致。
    if let Some(store) = &state.thinking_text_store {
        restore_omitted_thinking(&mut payload, store);
    }
    let hook = UsageRecordHook::from_state(&state, key_ctx.key_id, payload.model.clone(), key_ctx.group.clone());

    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            hook.record(0, 0, 0, (0, 0), 0.0, "error");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse::new(
                    "service_unavailable",
                    "Kiro API provider not configured",
                )),
            )
                .into_response();
        }
    };

    // 组支持校验：请求的模型不在该 key 所属组的支持集内 → 404，不再路由上游吃 400
    if let Err(resp) = group_model_check(&state, key_ctx.group.as_deref(), &payload.model) {
        hook.record(0, 0, 0, (0, 0), 0.0, "error");
        return resp.into_response();
    }

    // 检测模型名是否包含 "thinking" 后缀，若包含则覆写 thinking 配置
    override_thinking_from_model_name(&mut payload);

    // 检查是否为 WebSearch 请求
    if websearch::has_web_search_tool(&payload) {
        tracing::info!("检测到 WebSearch 工具，路由到 WebSearch 处理");

        // 估算输入 tokens
        let input_tokens = token::count_all_tokens(
            payload.model.clone(),
            payload.system.clone(),
            payload.messages.clone(),
            payload.tools.clone(),
        ) as i32;

        let resp = websearch::handle_websearch_request(
            provider,
            &payload,
            input_tokens,
            key_ctx.group.as_deref(),
        )
        .await;
        let status = if resp.status().is_success() { "success" } else { "error" };
        hook.record(0, input_tokens, 0, (0, 0), 0.0, status);
        return resp;
    }

    let payload_stream = payload.stream;
    // Mixed-tools (web_search + exec...) case: web_search coexists with other tools and falls onto the normal chat path,
    // where the upstream may return a tool_use with name=web_search. Take the internal agentic loop: search internally and feed the results back.
    if websearch::has_web_search_among_tools(&payload) {
        tracing::info!("detected mixed tools containing web_search, entering the web_search agentic loop");
        return super::websearch_loop::run_web_search_loop(provider, payload, hook, payload_stream, key_ctx.group.clone(), state.tool_compatibility_mode)
            .await;
    }

    // 转换请求
    let conversion_result = match convert_request_with_mode(&payload, state.tool_compatibility_mode) {
        Ok(result) => result,
        Err(e) => {
            tracing::warn!("请求转换失败: {}", e);
            hook.record(0, 0, 0, (0, 0), 0.0, "error");
            // 文案与状态码见 conversion_error_response（抽出来是为了可测）
            return conversion_error_response(&e).into_response();
        }
    };

    // Build the Kiro request. profile_arn is injected by the provider layer from the actual
    // credentials; additional_model_request_fields is already filtered by converter model support.
    let kiro_request = KiroRequest {
        conversation_state: conversion_result.conversation_state,
        profile_arn: None,
        additional_model_request_fields: conversion_result.additional_model_request_fields,
    };

    let request_body = match serde_json::to_string(&kiro_request) {
        Ok(body) => body,
        Err(e) => {
            tracing::error!("序列化请求失败: {}", e);
            hook.record(0, 0, 0, (0, 0), 0.0, "error");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse::new(
                    "internal_error",
                    format!("序列化请求失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    tracing::debug!("Kiro request body: {}", request_body);

    // 计算总 input tokens
    let total_input_tokens = token::count_all_tokens(
        payload.model.clone(),
        payload.system.clone(),
        payload.messages.clone(),
        payload.tools.clone(),
    ) as i32;

    // 检查是否启用了thinking
    let thinking_enabled = payload
        .thinking
        .as_ref()
        .map(|t| t.is_enabled())
        .unwrap_or(false);
    // display:"omitted"：思考正文不下发（签名=恢复键，回传时服务端恢复）
    let thinking_omitted = thinking_enabled
        && payload
            .thinking
            .as_ref()
            .and_then(|t| t.display.as_deref())
            == Some("omitted");

    let context_window = conversion_result.context_window;
    let tool_name_map = conversion_result.tool_name_map;
    let known_tool_names = conversion_result.known_tool_names;

    // CacheMeter：根据 cache_control 断点查 / 写中转层提示词缓存（estimate 口径）。
    let cache_usage = state
        .cache_meter
        .as_ref()
        .map(|cache| super::cache_metering::compute_cache_usage(cache, &payload, key_ctx.key_id))
        .unwrap_or_default();

    if payload.stream {
        // 流式响应（缓冲模式）
        let tracer = std::sync::Arc::new(RequestTracer::new(
            &state,
            RequestTraceOptions {
                key_ctx: key_ctx.clone(),
                model: payload.model.clone(),
                is_stream: true,
                session_id: session_id_of(&payload),
            },
        ));
        persist_request_body(&state, &tracer, &raw_body);
        handle_stream_request_buffered(
            provider,
            &request_body,
            &payload.model,
            total_input_tokens,
            hook,
            tracer,
            ResponseProcessingConfig {
                thinking_enabled,
                thinking_omitted,
                thinking_text_store: state.thinking_text_store.clone(),
                tool_name_map,
                known_tool_names,
                cache_usage,
                group: key_ctx.group.clone(),
                context_window,
                sticky_key: dispatch_sticky_key(&payload),
            },
        )
        .await
    } else {
        // 非流式响应：仅在配置开启时提取 thinking 块
        let extract_thinking = state.extract_thinking && thinking_enabled;
        let tracer = std::sync::Arc::new(RequestTracer::new(
            &state,
            RequestTraceOptions {
                key_ctx: key_ctx.clone(),
                model: payload.model.clone(),
                is_stream: false,
                session_id: session_id_of(&payload),
            },
        ));
        persist_request_body(&state, &tracer, &raw_body);
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            total_input_tokens,
            hook,
            tracer,
            ResponseProcessingConfig {
                thinking_enabled: extract_thinking,
                thinking_omitted,
                thinking_text_store: state.thinking_text_store.clone(),
                tool_name_map,
                known_tool_names,
                cache_usage,
                group: key_ctx.group.clone(),
                context_window,
                sticky_key: dispatch_sticky_key(&payload),
            },
        )
        .await
    }
}

/// 处理流式请求（缓冲版本）
///
/// 与 `handle_stream_request` 不同，此函数会缓冲所有事件直到流结束，
/// 然后用从 contextUsageEvent 计算的正确 input_tokens 生成 message_start 事件。
async fn handle_stream_request_buffered(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    fallback_input_tokens: i32,
    hook: UsageRecordHook,
    tracer: std::sync::Arc<RequestTracer>,
    config: ResponseProcessingConfig,
) -> Response {
    let ResponseProcessingConfig {
        thinking_enabled,
        thinking_omitted,
        thinking_text_store,
        tool_name_map,
        known_tool_names,
        cache_usage,
        group,
        context_window,
        sticky_key,
    } = config;
    // 调用 Kiro API（支持多凭据故障转移）
    let call_result = match provider.call_api_stream(request_body, Some(tracer.as_ref()), group.as_deref(), sticky_key.as_deref()).await {
        Ok(resp) => resp,
        Err(e) => {
            hook.record(0, fallback_input_tokens, 0, (0, 0), 0.0, "error");
            tracer.finalize("error", last_attempt_outcome(&tracer), Some(&e.to_string()), None, TraceUsage::zero());
            return map_provider_error(e);
        }
    };
    let response = call_result.response;
    let credential_id = call_result.credential_id;
    let ops_feedback =
        StreamOpsFeedback::from_call(&provider, credential_id, call_result.proxy_url.clone());

    // 创建缓冲流处理上下文
    let mut ctx = BufferedStreamContext::new(
        model,
        fallback_input_tokens,
        context_window,
        thinking_enabled,
        tool_name_map,
        known_tool_names,
    );
    ctx.set_thinking_omitted(thinking_omitted, thinking_text_store);
    ctx.set_cache_usage(cache_usage);

    // 创建缓冲 SSE 流；并发门禁 permit 挂进流闭包，流结束/断开时才释放凭据并发位
    let gate_permit = call_result.permit;
    let stream = create_buffered_sse_stream(response, ctx, hook, credential_id, tracer, ops_feedback)
        .map(move |item| {
            let _keep = &gate_permit;
            item
        });

    // 返回 SSE 响应
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(header::CONNECTION, "keep-alive")
        .body(Body::from_stream(stream))
        .unwrap()
}

/// 创建缓冲 SSE 事件流
///
/// 工作流程：
/// 1. 等待上游流完成，期间只发送 ping 保活信号
/// 2. 使用 StreamContext 的事件处理逻辑处理所有 Kiro 事件，结果缓存
/// 3. 流结束后，用正确的 input_tokens 更正 message_start 事件
/// 4. 一次性发送所有事件
fn create_buffered_sse_stream(
    response: reqwest::Response,
    ctx: BufferedStreamContext,
    hook: UsageRecordHook,
    credential_id: u64,
    tracer: std::sync::Arc<RequestTracer>,
    ops_feedback: Option<StreamOpsFeedback>,
) -> impl Stream<Item = Result<Bytes, Infallible>> {
    let body_stream = response.bytes_stream();

    // 段埋点：first_token 段在此打开，guard 承载 streaming 段（详见 StreamPhaseGuard 文档）。
    tracer.open_phase(phase::FIRST_TOKEN);
    let guard = StreamPhaseGuard::new(tracer.clone(), 0);

    stream::unfold(
        (
            body_stream,
            ctx,
            EventStreamDecoder::new(),
            false,
            interval(Duration::from_secs(PING_INTERVAL_SECS)),
            hook,
            credential_id,
            tracer,
            0u64,
            ops_feedback,
            true,
            Some(guard),
        ),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval, hook, credential_id, tracer, mut sent_bytes, ops_feedback, mut first_chunk, mut guard)| async move {
            if finished {
                return None;
            }

            loop {
                tokio::select! {
                    // 使用 biased 模式，优先检查 ping 定时器
                    // 避免在上游 chunk 密集时 ping 被"饿死"
                    biased;

                    // 优先检查 ping 保活（等待期间唯一发送的数据）
                    _ = ping_interval.tick() => {
                        tracing::trace!("发送 ping 保活事件（缓冲模式）");
                        let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                        return Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback, first_chunk, guard)));
                    }

                    // 然后处理数据流
                    chunk_result = body_stream.next() => {
                        match chunk_result {
                            Some(Ok(chunk)) => {
                                tracer.mark_first_token();
                                sent_bytes += chunk.len() as u64;
                                if let Some(g) = guard.as_mut() {
                                    if first_chunk {
                                        g.mark_first_chunk();
                                    }
                                    g.set_bytes(sent_bytes);
                                }
                                first_chunk = false;
                                // 解码事件
                                if let Err(e) = decoder.feed(&chunk) {
                                    tracing::warn!("缓冲区溢出: {}", e);
                                }

                                for result in decoder.decode_iter() {
                                    match result {
                                        Ok(frame) => {
                                            if let Ok(event) = Event::from_frame(frame) {
                                                // 缓冲事件（复用 StreamContext 的处理逻辑）
                                                ctx.process_and_buffer(&event);
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!("解码事件失败: {}", e);
                                        }
                                    }
                                }
                                // 继续读取下一个 chunk，不发送任何数据
                            }
                            Some(Err(e)) => {
                                let detail = describe_reqwest_error(&e);
                                tracing::error!("读取响应流失败: {}", detail);
                                // 按值消费 guard：本分支已显式处理结局，Drop 不得再判客户端断开。
                                if let Some(g) = guard.take() {
                                    g.into_upstream_error(sent_bytes, &detail);
                                }
                                // 发生错误，完成处理并返回所有事件
                                let all_events = ctx.finish_and_get_all_events();
                                tracer.observe_events(&all_events);
                                let (i, o, cc, cr, credits) = ctx.final_usage();
                                hook.record(credential_id, i, o, (cc, cr), credits, "error");
                                report_stream_outcome(&ops_feedback, true, &detail);
                                // 缓冲模式 chunk 读取失败：上游中途断流
                                tracer.finalize(
                                    "interrupted",
                                    Some(outcome::STREAM_INTERRUPTED),
                                    Some(&detail),
                                    Some(sent_bytes),
                                    TraceUsage {
                                        input_tokens: i.max(0) as u64,
                                        output_tokens: o.max(0) as u64,
                                        cache_creation_tokens: cc.max(0) as u64,
                                        cache_read_tokens: cr.max(0) as u64,
                                        credits: if credits.is_finite() && credits > 0.0 { credits } else { 0.0 },
                                    },
                                );
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback, first_chunk, guard)));
                            }
                            None => {
                                // 流正常结束：先按值消费 guard 收尾 streaming 段，再打开 finish 段——
                                // 顺序不可反（见 phase_on_finish 文档的陷阱说明）。
                                if let Some(g) = guard.take() {
                                    g.into_completed(sent_bytes);
                                }
                                // 流结束，完成处理并返回所有事件（已更正 input_tokens）。
                                // finish_and_get_all_events 内部会 finish() 累积器；若有半截 /
                                // 非法工具调用 JSON，error 事件已随缓冲发出，这里据此记 error。
                                let all_events = ctx.finish_and_get_all_events();
                                tracer.observe_events(&all_events);
                                let (i, o, cc, cr, credits) = ctx.final_usage();
                                let trace_usage = TraceUsage {
                                    input_tokens: i.max(0) as u64,
                                    output_tokens: o.max(0) as u64,
                                    cache_creation_tokens: cc.max(0) as u64,
                                    cache_read_tokens: cr.max(0) as u64,
                                    credits: if credits.is_finite() && credits > 0.0 { credits } else { 0.0 },
                                };
                                phase_on_finish(
                                    &tracer,
                                    sent_bytes,
                                    ctx.tool_json_error(),
                                    ctx.tool_json_error_message(),
                                );
                                if let Some(message) = ctx.tool_json_error_message() {
                                    // 区分上游截断（传输，计代理失败）与上游非法 JSON（内容，不罚代理），
                                    // 见实时流路径同名分支
                                    let incomplete =
                                        ctx.tool_json_error_incomplete().unwrap_or(true);
                                    hook.record(credential_id, i, o, (cc, cr), credits, "error");
                                    report_stream_outcome(&ops_feedback, incomplete, &message);
                                    tracer.finalize(
                                        "error",
                                        Some(if incomplete {
                                            outcome::UPSTREAM_TRUNCATED
                                        } else {
                                            outcome::UPSTREAM_INVALID
                                        }),
                                        Some(&message),
                                        Some(sent_bytes),
                                        trace_usage,
                                    );
                                } else {
                                    hook.record(credential_id, i, o, (cc, cr), credits, "success");
                                    report_stream_outcome(&ops_feedback, false, "");
                                    tracer.finalize("success", None, None, None, trace_usage);
                                }
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback, first_chunk, guard)));
                            }
                        }
                    }
                }
            }
        },
    )
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 构造一个不依赖 AppState / usage_store / client_keys 的最小 `UsageRecordHook`，
    /// 专门用来验证 `record()` 是否真的把消耗回写给了 `GroupDispatcher`——
    /// 这条接线是本任务（消耗回写闭环）存在的全部理由，且没有被其他任何测试覆盖：
    /// dispatch.rs 里的测试只验证 `GroupDispatcher::report_consumption` 自身正确，
    /// 不验证生产路径是否调用了它。
    fn hook_with_dispatcher(
        group: Option<&str>,
    ) -> (UsageRecordHook, std::sync::Arc<crate::kiro::dispatch::GroupDispatcher>) {
        let cache = std::sync::Arc::new(crate::admin::balance_cache::BalanceCache::new(None));
        let dispatcher = std::sync::Arc::new(crate::kiro::dispatch::GroupDispatcher::new(cache));
        let hook = UsageRecordHook {
            usage: None,
            client_keys: None,
            key_id: 1,
            model: "test-model".to_string(),
            started_at: Instant::now(),
            dispatcher: Some(dispatcher.clone()),
            group: group.map(|g| g.to_string()),
        };
        (hook, dispatcher)
    }

    #[test]
    fn record_writes_consumption_back_to_dispatcher() {
        let (hook, dispatcher) = hook_with_dispatcher(Some("G"));
        hook.record(7, 100, 200, (0, 0), 12.5, "success");
        assert_eq!(
            dispatcher.consumed_of(Some("G"), 7),
            12.5,
            "record() 必须把入参 credits 回写给对应 (group, credential_id) 的调度器桶"
        );
    }

    #[test]
    fn record_with_credential_id_zero_does_not_write_back() {
        // credential_id == 0 表示本次请求没有分配到凭据，不该污染任何账号的消耗
        let (hook, dispatcher) = hook_with_dispatcher(Some("G"));
        hook.record(0, 100, 200, (0, 0), 12.5, "success");
        assert_eq!(
            dispatcher.consumed_of(Some("G"), 0),
            0.0,
            "credential_id=0（未分配凭据）不应回写"
        );
    }

    #[test]
    fn record_routes_consumption_to_the_correct_group_bucket() {
        // 两个不同 group 的 hook 各自 record 同一个 credential_id，互不串桶
        let cache = std::sync::Arc::new(crate::admin::balance_cache::BalanceCache::new(None));
        let dispatcher = std::sync::Arc::new(crate::kiro::dispatch::GroupDispatcher::new(cache));
        let hook_a = UsageRecordHook {
            usage: None,
            client_keys: None,
            key_id: 1,
            model: "test-model".to_string(),
            started_at: Instant::now(),
            dispatcher: Some(dispatcher.clone()),
            group: Some("A".to_string()),
        };
        let hook_b = UsageRecordHook {
            usage: None,
            client_keys: None,
            key_id: 1,
            model: "test-model".to_string(),
            started_at: Instant::now(),
            dispatcher: Some(dispatcher.clone()),
            group: Some("B".to_string()),
        };
        hook_a.record(7, 10, 10, (0, 0), 40.0, "success");
        hook_b.record(7, 10, 10, (0, 0), 5.0, "success");
        assert_eq!(dispatcher.consumed_of(Some("A"), 7), 40.0, "A 组不应包含 B 组的消耗");
        assert_eq!(dispatcher.consumed_of(Some("B"), 7), 5.0, "B 组不应包含 A 组的消耗");
    }

    #[test]
    fn dispatch_sticky_key_only_from_uuid_session() {
        // Claude Code 形态：metadata.user_id 内含 session uuid
        let with_session: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-sonnet-5",
            "messages": [{"role": "user", "content": "hi"}],
            "metadata": {"user_id": "{\"device_id\":\"d\",\"session_id\":\"0b4445e1-1111-4222-8333-444455556666\"}"}
        })).unwrap();
        assert_eq!(
            dispatch_sticky_key(&with_session).as_deref(),
            Some("0b4445e1-1111-4222-8333-444455556666")
        );

        // 无 metadata：必须返回 None 而不是降级到 key_id。
        // 降级会让同一 client key 下的所有会话共享一条粘滞记录被永久钉死。
        let without: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-sonnet-5",
            "messages": [{"role": "user", "content": "hi"}]
        })).unwrap();
        assert_eq!(dispatch_sticky_key(&without), None);

        // metadata 存在但不含合法 uuid：同样返回 None
        let bad: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-sonnet-5",
            "messages": [{"role": "user", "content": "hi"}],
            "metadata": {"user_id": "not-a-session"}
        })).unwrap();
        assert_eq!(dispatch_sticky_key(&bad), None);
    }

    #[test]
    fn restore_omitted_thinking_refills_text_from_store() {
        // omitted 轻量往返的回程：客户端历史里是空正文 + kiro-thinking-v1 恢复键，
        // 预处理必须凭键恢复正文，converter 才能把推理上下文带给上游。
        let root = std::env::temp_dir().join(format!(
            "kiro-restore-test-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let store = crate::admin::request_body_store::RequestBodyStore::new(root.clone(), true, 7);
        store.save("abc-123", "被省略的推理".as_bytes());

        let mut payload: MessagesRequest = serde_json::from_value(serde_json::json!({
            "model": "claude-opus-5",
            "max_tokens": 100,
            "messages": [
                {"role": "user", "content": "hi"},
                {"role": "assistant", "content": [
                    {"type": "thinking", "thinking": "", "signature": "kiro-thinking-v1:abc-123"},
                    {"type": "thinking", "thinking": "", "signature": "kiro-thinking-v1:missing-id"},
                    {"type": "thinking", "thinking": "本来就有", "signature": "kiro-rs-thinking-signature"},
                    {"type": "text", "text": "ok"}
                ]}
            ]
        }))
        .unwrap();

        restore_omitted_thinking(&mut payload, &store);

        let content = &payload.messages[1].content;
        assert_eq!(content[0]["thinking"], "被省略的推理", "恢复键应回填正文");
        assert_eq!(content[1]["thinking"], "", "键失效（过期）时保持为空，不伪造");
        assert_eq!(content[2]["thinking"], "本来就有", "非 omitted 块不动");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn tracer_observe_events_persists_stream_shape_and_first_render() {
        // 形态摘要必须经 observe_events → finalize 落到 trace 行；
        // 这是事件语义层唯一的服务端观测点。
        let store = std::sync::Arc::new(
            crate::admin::trace_db::TraceStore::open_in_memory().unwrap(),
        );
        let tracer = RequestTracer {
            store: Some(store.clone()),
            trace_id: "t-shape-int".to_string(),
            ts: Utc::now().to_rfc3339(),
            key_id: 1,
            key_source: TraceKeySource::ClientKey,
            model: "claude-opus-5".to_string(),
            is_stream: true,
            session_id: None,
            started_at: Instant::now(),
            first_token_at: parking_lot::Mutex::new(None),
            attempts: parking_lot::Mutex::new(Vec::new()),
            phases: parking_lot::Mutex::new(Vec::new()),
            open_phase: parking_lot::Mutex::new(None),
            shape: parking_lot::Mutex::new(crate::anthropic::stream::StreamShape::default()),
        };

        tracer.observe_events(&[
            crate::anthropic::stream::SseEvent::new(
                "content_block_start",
                serde_json::json!({"index":0,"content_block":{"type":"thinking","thinking":""}}),
            ),
            crate::anthropic::stream::SseEvent::new(
                "content_block_delta",
                serde_json::json!({"index":0,"delta":{"type":"thinking_delta","thinking":"abc"}}),
            ),
        ]);
        tracer.finalize("success", None, None, None, TraceUsage::zero());

        let (out, _) = store.query_paged(&crate::admin::trace_db::TraceQuery {
            limit: 10,
            ..Default::default()
        });
        let rec = out
            .iter()
            .find(|r| r.trace_id == "t-shape-int")
            .expect("trace 行应已落库");
        let shape = rec.stream_shape.as_deref().expect("形态摘要应已持久化");
        assert!(shape.contains("\"t\":\"thinking\""), "形态应含 thinking 块: {shape}");
        assert!(
            rec.first_render_ms.is_some(),
            "非空 thinking_delta 应记为首个可渲染帧"
        );
    }

    /// 构造可控时钟的 tracer：`started_secs_ago` 把请求起点拨到过去，
    /// 让 finalize 的 duration / first_render（observe 用真实 now 计算 elapsed）
    /// 落在假活阈值两侧，而不用真的 sleep。
    fn tracer_with_clock(
        store: std::sync::Arc<crate::admin::trace_db::TraceStore>,
        trace_id: &str,
        started_secs_ago: u64,
        is_stream: bool,
    ) -> RequestTracer {
        RequestTracer {
            store: Some(store),
            trace_id: trace_id.to_string(),
            ts: Utc::now().to_rfc3339(),
            key_id: 1,
            key_source: TraceKeySource::ClientKey,
            model: "claude-opus-5".to_string(),
            is_stream,
            session_id: None,
            started_at: Instant::now()
                .checked_sub(Duration::from_secs(started_secs_ago))
                .expect("回拨起点应在 Instant 可表示范围内"),
            first_token_at: parking_lot::Mutex::new(None),
            attempts: parking_lot::Mutex::new(Vec::new()),
            phases: parking_lot::Mutex::new(Vec::new()),
            open_phase: parking_lot::Mutex::new(None),
            shape: parking_lot::Mutex::new(crate::anthropic::stream::StreamShape::default()),
        }
    }

    fn finalized_record(
        store: &crate::admin::trace_db::TraceStore,
        trace_id: &str,
    ) -> crate::admin::trace_db::TraceRecord {
        let (out, _) = store.query_paged(&crate::admin::trace_db::TraceQuery {
            limit: 10,
            ..Default::default()
        });
        out.into_iter()
            .find(|r| r.trace_id == trace_id)
            .expect("trace 行应已落库")
    }

    fn renderable_text_delta() -> crate::anthropic::stream::SseEvent {
        crate::anthropic::stream::SseEvent::new(
            "content_block_delta",
            serde_json::json!({"index":0,"delta":{"type":"text_delta","text":"hi"}}),
        )
    }

    #[test]
    fn dead_air_gap_over_threshold_is_classified_on_success() {
        // 首字节早到、首个可渲染帧晚到 30s 以上：即便流最终 success，
        // 也必须打上 dead_air 分类，否则假活流在 ops 错误分类里完全不可见。
        let store = std::sync::Arc::new(
            crate::admin::trace_db::TraceStore::open_in_memory().unwrap(),
        );
        let tracer = tracer_with_clock(store.clone(), "t-dead-air", 40, true);
        *tracer.first_token_at.lock() = Some(tracer.started_at + Duration::from_millis(5));
        // observe 用真实 now 计算 elapsed ≈ 40_000ms → gap ≈ 40s > 30s
        tracer.observe_events(&[renderable_text_delta()]);
        tracer.finalize("success", None, None, None, TraceUsage::zero());

        let rec = finalized_record(&store, "t-dead-air");
        assert_eq!(rec.final_status, "success", "假活不改变 final_status 口径");
        assert_eq!(
            rec.error_type.as_deref(),
            Some(outcome::DEAD_AIR),
            "首帧滞后超阈值必须分类为 dead_air"
        );
        let msg = rec.error_message.as_deref().unwrap_or("");
        assert!(msg.contains("假活"), "error_message 应描述假活: {msg}");
    }

    #[test]
    fn healthy_late_stream_with_prompt_render_is_not_flagged() {
        // 长请求但首帧紧跟首字节（健康流实测差 ≈1ms）：不得误伤。
        let store = std::sync::Arc::new(
            crate::admin::trace_db::TraceStore::open_in_memory().unwrap(),
        );
        let tracer = tracer_with_clock(store.clone(), "t-healthy", 40, true);
        // 首字节也很晚（上游 prefill 40s），但渲染帧随即到达 → gap ≈ 数 ms
        *tracer.first_token_at.lock() = Some(Instant::now() - Duration::from_millis(3));
        tracer.observe_events(&[renderable_text_delta()]);
        tracer.finalize("success", None, None, None, TraceUsage::zero());

        let rec = finalized_record(&store, "t-healthy");
        assert_eq!(rec.error_type, None, "首帧紧跟首字节的流不得判假活");
    }

    #[test]
    fn dead_air_stream_never_rendering_is_flagged_at_stream_end() {
        // 次要形态：首字节之后直到流结束都没有任何可渲染帧。这是假活的
        // 极端形（病态案例 712s 在渲染前中止即属此类），以流结束时刻代替
        // first_render 参与同一判据，不另设阈值。
        let store = std::sync::Arc::new(
            crate::admin::trace_db::TraceStore::open_in_memory().unwrap(),
        );
        let tracer = tracer_with_clock(store.clone(), "t-never-render", 40, true);
        *tracer.first_token_at.lock() = Some(tracer.started_at + Duration::from_millis(5));
        tracer.finalize("success", None, None, None, TraceUsage::zero());

        let rec = finalized_record(&store, "t-never-render");
        assert_eq!(
            rec.error_type.as_deref(),
            Some(outcome::DEAD_AIR),
            "整流无可渲染帧且超阈值必须判假活"
        );
        let msg = rec.error_message.as_deref().unwrap_or("");
        assert!(msg.contains("无可渲染帧"), "消息应区分「从未渲染」形态: {msg}");
    }

    #[test]
    fn dead_air_never_overrides_primary_error_type() {
        // 已有主因分类（如 stream_interrupted）时不得被 dead_air 覆盖：
        // 主因决定处置动作，假活细节仍可从 stream_shape 追溯。
        let store = std::sync::Arc::new(
            crate::admin::trace_db::TraceStore::open_in_memory().unwrap(),
        );
        let tracer = tracer_with_clock(store.clone(), "t-primary-err", 40, true);
        *tracer.first_token_at.lock() = Some(tracer.started_at + Duration::from_millis(5));
        tracer.finalize(
            "interrupted",
            Some(outcome::STREAM_INTERRUPTED),
            Some("上游断流"),
            Some(128),
            TraceUsage::zero(),
        );

        let rec = finalized_record(&store, "t-primary-err");
        assert_eq!(
            rec.error_type.as_deref(),
            Some(outcome::STREAM_INTERRUPTED),
            "主因分类不得被 dead_air 覆盖"
        );
        assert_eq!(rec.error_message.as_deref(), Some("上游断流"));
    }

    #[test]
    fn dead_air_ignores_non_stream_and_streams_without_first_token() {
        // 非流式没有「渲染帧」概念（shape 恒空），长请求不得误判；
        // 首字节从未到达的流由既有分类（error/interrupted）负责，也不判假活。
        let store = std::sync::Arc::new(
            crate::admin::trace_db::TraceStore::open_in_memory().unwrap(),
        );
        let non_stream = tracer_with_clock(store.clone(), "t-non-stream", 40, false);
        *non_stream.first_token_at.lock() =
            Some(non_stream.started_at + Duration::from_millis(5));
        non_stream.finalize("success", None, None, None, TraceUsage::zero());

        let no_first_token = tracer_with_clock(store.clone(), "t-no-token", 40, true);
        no_first_token.finalize("success", None, None, None, TraceUsage::zero());

        assert_eq!(
            finalized_record(&store, "t-non-stream").error_type,
            None,
            "非流式不得判假活"
        );
        assert_eq!(
            finalized_record(&store, "t-no-token").error_type,
            None,
            "无首字节的流不得判假活"
        );
    }

    #[test]
    fn bedrock_client_validation_errors_map_to_400() {
        // 客户端校验错误必须映射为 400（而非 5xx），否则会被 provider 当作上游
        // 瞬态错误触发冷却，放大成 503 风暴。识别逻辑集中在 endpoint 层。
        for needle in [
            // 精确 reason（provider 错误串里嵌着上游 body）
            "非流式 API 请求失败: 500 {\"reason\":\"TOOL_USE_RESULT_MISMATCH\"}",
            // message 级特异短语（纯文本报文）
            "Expected toolResult blocks but found none",
        ] {
            let resp = map_provider_error(anyhow::anyhow!(needle.to_string()));
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "错误串 `{needle}` 应映射为 400"
            );
        }
    }

    #[test]
    fn generic_upstream_error_still_maps_to_502() {
        // 回归：普通上游错误不应被新分支误伤，仍应是 502 BAD_GATEWAY。
        let resp = map_provider_error(anyhow::anyhow!("connection reset by peer"));
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        // 回归：宽泛的 ValidationException 不再被当作客户端校验错误而误判为 400，
        // 仍按上游错误走 502（避免把可重试故障误杀）。
        let resp = map_provider_error(anyhow::anyhow!(
            "ValidationException: transient backend issue".to_string()
        ));
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
    }

    #[test]
    fn upstream_overload_maps_to_529_overloaded_error() {
        // AWS Kiro 高负载：500 MODEL_TEMPORARILY_UNAVAILABLE → 529（而非 502），
        // 便于上游网关识别为过载并退避/切换账号。
        let resp = map_provider_error(anyhow::anyhow!(
            "流式 API 请求失败: 500 Internal Server Error \
             {\"message\":\"Encountered unexpectedly high load when processing the request, please try again.\",\
             \"reason\":\"MODEL_TEMPORARILY_UNAVAILABLE\"}"
                .to_string()
        ));
        assert_eq!(resp.status().as_u16(), 529);
    }

    #[test]
    fn upstream_rate_limit_maps_to_429_with_retry_after() {
        let err = crate::kiro::error::UpstreamRateLimitError::new(Some("1800".to_string()));
        let resp = map_provider_error(err.into());

        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            resp.headers().get(header::RETRY_AFTER).unwrap(),
            "1800"
        );
    }

    #[test]
    fn upstream_rate_limit_drops_invalid_retry_after() {
        let err = crate::kiro::error::UpstreamRateLimitError::new(Some(
            "not-a-retry-delay".to_string(),
        ));
        let resp = map_provider_error(err.into());

        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
        assert!(resp.headers().get(header::RETRY_AFTER).is_none());
    }

    #[tokio::test]
    async fn generic_upstream_error_does_not_expose_raw_body() {
        let secret = "aws-account=123456789012 request-id=private-request";
        let resp = map_provider_error(anyhow::anyhow!(secret));
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

        let body = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();
        assert!(!body.contains(secret));
        assert!(body.contains("Upstream API request failed"));
    }

    #[test]
    fn non_stream_native_thinking_precedes_redacted_and_text() {
        let content = build_non_stream_content(
            true,
            "final answer".to_string(),
            "native thinking".to_string(),
            vec!["encrypted-thinking".to_string()],
            false,
            None,
        );

        assert_eq!(content.len(), 3);
        assert_eq!(content[0]["type"], "thinking");
        assert_eq!(content[0]["thinking"], "native thinking");
        // 真签名不透传（与流式路径一致）：回传即被 serde 丢弃，只膨胀历史。
        assert_eq!(
            content[0]["signature"],
            crate::anthropic::stream::THINKING_SIGNATURE_PLACEHOLDER
        );
        assert_eq!(content[1]["type"], "redacted_thinking");
        assert_eq!(content[1]["data"], "encrypted-thinking");
        assert_eq!(content[2]["type"], "text");
        assert_eq!(content[2]["text"], "final answer");
    }

    #[test]
    fn non_stream_omitted_thinking_stores_text_and_emits_restore_key() {
        let root = std::env::temp_dir().join(format!(
            "kiro-ns-omit-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let store = std::sync::Arc::new(
            crate::admin::request_body_store::RequestBodyStore::new(root.clone(), true, 7),
        );
        let content = build_non_stream_content(
            true,
            "final answer".to_string(),
            "native thinking".to_string(),
            Vec::new(),
            true,
            Some(&store),
        );
        assert_eq!(content[0]["type"], "thinking");
        assert_eq!(content[0]["thinking"], "", "omitted 时非流式同样不下发正文");
        let sig = content[0]["signature"].as_str().unwrap();
        assert!(sig.starts_with("kiro-thinking-v1:"), "签名应为恢复键: {sig}");
        let restored = store.load(sig.trim_start_matches("kiro-thinking-v1:")).unwrap();
        assert_eq!(String::from_utf8_lossy(&restored), "native thinking");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn non_stream_legacy_thinking_extraction_still_works_without_native_reasoning() {
        let content = build_non_stream_content(
            true,
            "<thinking>legacy thinking</thinking>\n\nfinal answer".to_string(),
            String::new(),
            Vec::new(),
            false,
            None,
        );

        assert_eq!(content.len(), 2);
        assert_eq!(content[0]["type"], "thinking");
        assert_eq!(content[0]["thinking"], "legacy thinking");
        assert_eq!(
            content[0]["signature"],
            crate::anthropic::stream::THINKING_SIGNATURE_PLACEHOLDER
        );
        assert_eq!(content[1]["type"], "text");
        assert_eq!(content[1]["text"], "final answer");
    }

    #[test]
    fn non_stream_native_thinking_downgrades_to_text_when_thinking_disabled() {
        let content = build_non_stream_content(
            false,
            String::new(),
            "native thinking fallback".to_string(),
            vec!["ignored-redacted".to_string()],
            false,
            None,
        );

        assert_eq!(content.len(), 1);
        assert_eq!(content[0]["type"], "text");
        assert_eq!(content[0]["text"], "native thinking fallback");
    }

    #[test]
    fn available_models_include_opus_4_7_variants() {
        let models = available_models();
        let ids: Vec<&str> = models.iter().map(|model| model.id.as_str()).collect();

        assert!(ids.contains(&"claude-opus-4-7"));
        assert!(ids.contains(&"claude-opus-4-7-thinking"));
    }

    #[test]
    fn count_image_budget_handles_empty() {
        let req: super::super::types::MessagesRequest = serde_json::from_str(r#"{
            "model": "claude-opus-4-7",
            "max_tokens": 100,
            "messages": []
        }"#).unwrap();
        let stats = count_image_budget(&req);
        assert_eq!(stats.count, 0);
        assert_eq!(stats.total_b64_bytes, 0);
        assert_eq!(stats.largest_b64_bytes, 0);
    }

    #[test]
    fn trace_session_id_supports_current_json_metadata() {
        let req: super::super::types::MessagesRequest = serde_json::from_str(r#"{
            "model": "claude-opus-5",
            "max_tokens": 100,
            "messages": [],
            "metadata": {
                "user_id": "{\"device_id\":\"device\",\"account_uuid\":\"\",\"session_id\":\"8bb5523b-ec7c-4540-a9ca-beb6d79f1552\"}"
            }
        }"#).unwrap();

        assert_eq!(
            session_id_of(&req).as_deref(),
            Some("8bb5523b-ec7c-4540-a9ca-beb6d79f1552")
        );
    }

    #[test]
    fn count_image_budget_counts_inline_base64() {
        let req: super::super::types::MessagesRequest = serde_json::from_str(r#"{
            "model": "claude-opus-4-7",
            "max_tokens": 100,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "text", "text": "hi"},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/png", "data": "AAAA1111"}},
                    {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": "BBBBBBBBBB"}},
                    {"type": "image", "source": {"type": "url", "url": "https://example.com/x.png"}}
                ]
            }]
        }"#).unwrap();
        let stats = count_image_budget(&req);
        assert_eq!(stats.count, 2);
        assert_eq!(stats.total_b64_bytes, 18);
        assert_eq!(stats.largest_b64_bytes, 10);
    }

    #[test]
    fn count_image_budget_skips_url_only_images() {
        let req: super::super::types::MessagesRequest = serde_json::from_str(r#"{
            "model": "claude-opus-4-7",
            "max_tokens": 100,
            "messages": [{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "url", "url": "https://example.com/x.png"}}
                ]
            }]
        }"#).unwrap();
        let stats = count_image_budget(&req);
        assert_eq!(stats.count, 0);
    }

    #[test]
    fn available_models_include_4_8_variants() {
        let models = available_models();
        let ids: Vec<&str> = models.iter().map(|model| model.id.as_str()).collect();

        assert!(ids.contains(&"claude-opus-4-8"));
        assert!(ids.contains(&"claude-opus-4-8-thinking"));
        assert!(ids.contains(&"claude-sonnet-4-8"));
        assert!(ids.contains(&"claude-sonnet-4-8-thinking"));
    }

    // ================= 模型注册表：端到端接缝（Task 14） =================
    //
    // 这一组测试的对象是**接缝**，不是解析/校验逻辑本身（后者由
    // model_registry.rs / model_registry_store.rs 的单测覆盖）。这里只验证
    // 「注册表 → 全局 holder → handlers → HTTP 响应」这条链真的通着：
    // 单测里 `registry.exposed_models()` 全绿，不代表 `/v1/models` 端点
    // 也读的是同一份表。
    //
    // **全局状态纪律**（本分支出过一次污染事故）：
    // 1. 任何 `install_registry()` 的测试都先取 `MODEL_GLOBALS_TEST_LOCK` 守卫。
    //    测试期装表写的是**线程本地覆盖**（见 model_registry 里 `REGISTRY_OVERRIDE`
    //    的说明），测试之间根本不共享注册表；守卫的作用是给覆盖划定作用域，并让
    //    「装表发生在非测试线程」这类错误当场 panic 而不是静默失效。
    // 2. 末尾复原（`install_registry(ModelRegistry::builtin())`）已非必需 ——
    //    守卫 Drop 会清掉覆盖。既有写法保留不动，多写一次无害。
    // 3. **只往 builtin 上追加合成行，绝不改动既有 builtin 行的标志位。**
    //    这条在线程本地方案下已不再是正确性前提（并行读者读不到本线程的覆盖），
    //    但仍是好习惯：断言对象越少被动过，测试意图越清楚。
    use crate::anthropic::model_registry::{
        builtin_rows, install_registry, ModelOrigin, ModelRegistry, ModelRow, ModelStatus,
        MODEL_GLOBALS_TEST_LOCK,
    };

    /// 以 opus-4.8 为模板造一行「谁也不认识」的合成模型，
    /// 用于在不触碰任何既有 builtin 行的前提下测可见性规则。
    fn t14_row(upstream: &str, sort: i32) -> ModelRow {
        let mut row = builtin_rows()
            .into_iter()
            .find(|r| r.upstream_id == "claude-opus-4.8")
            .expect("builtin 必须含 claude-opus-4.8");
        row.upstream_id = upstream.to_string();
        row.exposed_id = upstream.replace('.', "-");
        row.display_name = format!("T14 {}", upstream);
        row.origin = ModelOrigin::Synced;
        row.sort_order = sort;
        row.status = ModelStatus::Active;
        row.enabled = true;
        row.listed = true;
        row.expose_thinking_variant = true;
        row
    }

    /// `/v1/models` 的可见性规则必须跟随**当前装载的注册表**，而不是编译期常量。
    ///
    /// 三条规则一起断言（它们共用同一个 filter，分开写只会重复搭建成本）：
    /// - `deprecated` 仍然出现 —— 上游下线不该让在用客户端的模型列表突然缺项；
    /// - `enabled == false` 连同其 `-thinking` 变体一起移除 —— 人工下线要彻底；
    /// - `listed == false` 不出现，但**仍可解析** —— 这是 gpt-5 prefix 行的语义。
    #[test]
    fn models_endpoint_visibility_follows_installed_registry() {
        let _guard = MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let mut r = ModelRegistry::builtin();
        {
            let rows = r.rows_mut();
            let mut dep = t14_row("t14-deprecated", 9001);
            dep.status = ModelStatus::Deprecated;
            rows.push(dep);

            let mut off = t14_row("t14-disabled", 9002);
            off.enabled = false;
            rows.push(off);

            let mut hidden = t14_row("t14-unlisted", 9003);
            hidden.listed = false;
            rows.push(hidden);
        }
        install_registry(r);

        let ids: Vec<String> = available_models().into_iter().map(|m| m.id).collect();
        let resolves = |name: &str| {
            crate::anthropic::model_registry::current_registry().resolve(name, false)
        };
        let deprecated_still_resolves = matches!(
            resolves("t14-deprecated"),
            crate::anthropic::model_registry::Resolution::Mapped { .. }
        );
        let unlisted_still_resolves = matches!(
            resolves("t14-unlisted"),
            crate::anthropic::model_registry::Resolution::Mapped { .. }
        );
        let disabled_reason = resolves("t14-disabled");

        // 断言前先复原，避免 panic 时把污染留给后续测试
        install_registry(ModelRegistry::builtin());

        assert!(ids.contains(&"t14-deprecated".to_string()), "deprecated 应保留在 /v1/models");
        assert!(
            ids.contains(&"t14-deprecated-thinking".to_string()),
            "deprecated 行的 thinking 变体同样保留"
        );
        assert!(deprecated_still_resolves, "deprecated 仍必须可解析（不打断在用客户端）");

        assert!(!ids.contains(&"t14-disabled".to_string()), "enabled=false 应从列表移除");
        assert!(
            !ids.contains(&"t14-disabled-thinking".to_string()),
            "enabled=false 的 thinking 变体也要移除"
        );
        assert!(
            matches!(
                disabled_reason,
                crate::anthropic::model_registry::Resolution::Rejected(
                    crate::anthropic::model_registry::RejectReason::Disabled
                )
            ),
            "禁用行应报 Disabled 而非 Unknown"
        );

        assert!(!ids.contains(&"t14-unlisted".to_string()), "listed=false 不应出现在列表");
        assert!(unlisted_still_resolves, "listed=false 只影响列表，不影响解析");
    }

    /// `/v1/models` 按调用 key 的凭据组收窄：`allowed` 之外的 upstream id 应被过滤掉，
    /// `auto` 恒保留。
    #[test]
    fn models_list_narrowed_by_group_support_set() {
        let registry = crate::anthropic::model_registry::current_registry();
        let models = registry.exposed_models();
        assert!(!models.is_empty(), "内置注册表不应为空");
        let mut allowed = std::collections::HashSet::new();
        allowed.insert("claude-opus-5".to_string());
        let filtered = filter_models_by_group(models.clone(), &allowed);
        for m in &filtered {
            if m.id == "auto" {
                continue;
            }
            let upstream = match registry.resolve(&m.id, false) {
                crate::anthropic::model_registry::Resolution::Mapped { upstream_id, .. }
                | crate::anthropic::model_registry::Resolution::Passthrough { upstream_id, .. } => {
                    upstream_id
                }
                _ => panic!("exposed 模型必可解析: {}", m.id),
            };
            assert_eq!(upstream, "claude-opus-5", "过滤后只应剩 allowed 内的模型: {}", m.id);
        }
        assert!(filtered.len() < models.len(), "应有收窄效果");
    }

    /// `GET /v1/models` 的**响应报文**必须由注册表行逐字段派生。
    ///
    /// 单测 `exposed_models()` 断言的是 Vec<Model>，覆盖不到序列化这一层：
    /// 字段名（`max_tokens` / `display_name` / `type`）走的是 serde 重命名，
    /// 改错一个名字所有单测照样全绿、客户端却直接读不到。
    ///
    /// 同时钉死 `max_tokens` 取的是 `maxOutputTokens` 而**不是** contextWindow
    /// —— 这两个量在改造前的硬编码里恰好都存在，混淆一次就会把 64K 输出上限
    /// 报成 1M。这里刻意让两者取互不相同的值，混淆即失败。
    #[tokio::test]
    async fn get_models_endpoint_serializes_registry_rows() {
        let _guard = MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let mut r = ModelRegistry::builtin();
        {
            let mut row = t14_row("t14-shape", 9004);
            row.display_name = "T14 Shape".to_string();
            row.owned_by = "t14-owner".to_string();
            row.model_type = "t14-type".to_string();
            row.created = 1_700_000_000;
            row.context_window = 987_654; // 与下面的输出上限刻意不同
            row.max_output_tokens = 12_345;
            r.rows_mut().push(row);
        }
        install_registry(r);

        let state = AppState::new(false, crate::model::config::ToolCompatibilityMode::default());
        let key_ctx = KeyContext {
            key_id: 0,
            group: None,
            key_source: crate::admin::trace_db::TraceKeySource::MasterApiKey,
        };
        let resp = get_models(State(state), Extension(key_ctx)).await.into_response();
        let status = resp.status();
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();

        install_registry(ModelRegistry::builtin());

        assert_eq!(status, StatusCode::OK);
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["object"], "list");

        let data = json["data"].as_array().expect("data 必须是数组");
        let base = data
            .iter()
            .find(|m| m["id"] == "t14-shape")
            .expect("装载的行必须出现在 /v1/models 报文里");
        assert_eq!(base["object"], "model");
        assert_eq!(base["display_name"], "T14 Shape");
        assert_eq!(base["owned_by"], "t14-owner");
        assert_eq!(base["type"], "t14-type");
        assert_eq!(base["created"], 1_700_000_000i64);
        assert_eq!(base["max_tokens"], 12_345, "max_tokens 必须取 maxOutputTokens");
        assert_ne!(base["max_tokens"], 987_654, "max_tokens 不得取 contextWindow");

        let thinking = data
            .iter()
            .find(|m| m["id"] == "t14-shape-thinking")
            .expect("thinking 变体应由 exposeThinkingVariant 派生");
        assert_eq!(thinking["display_name"], "T14 Shape (Thinking)");
        assert_eq!(thinking["max_tokens"], 12_345, "变体与基行共用同一组窗口值");
    }

    /// 零回归底线（spec §11.1）：**没有 `models.json` 时，`/v1/models` 的报文
    /// 必须与纯内置默认逐字节一致。**
    ///
    /// 这条串起 store 的「文件缺失 → builtin 且不算降级」与 handlers 的
    /// 「列表读全局表」两段。分开测都绿、接错线（比如启动时忘了 install、
    /// 或 store 把 builtin 丢了）却发现不了，只有走完整条链才能钉住。
    #[test]
    fn absent_models_json_serves_byte_identical_builtin_listing() {
        let _guard = MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let mut path = std::env::temp_dir();
        path.push(format!("kiro-t14-absent-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let outcome = crate::anthropic::model_registry_store::ModelRegistryStore::new(path).load();
        assert!(outcome.degraded_reason.is_none(), "文件不存在不是降级状态");

        install_registry(outcome.registry);
        let served = serde_json::to_string(&available_models()).unwrap();
        install_registry(ModelRegistry::builtin());

        let expected = serde_json::to_string(&ModelRegistry::builtin().exposed_models()).unwrap();
        assert_eq!(served, expected, "无 models.json 时列表必须与内置默认逐字节一致");
    }

    /// 覆盖层的完整链路：`models.json` 落盘 → store 加载 → 装载全局 →
    /// `/v1/models` 与请求解析同时反映它。
    ///
    /// 覆盖三件事：
    /// - 覆盖层对**内置行**是叠加而非替换（不在文件里的内置行必须还在）；
    /// - 被覆盖的字段真的走到了对外报文（displayName / maxOutputTokens）；
    /// - `aliases` 只存在于文件里、不在内置默认中，能被 `map_model` 解析到。
    #[tokio::test]
    async fn overlay_file_flows_through_store_into_listing_and_resolution() {
        let _guard = MODEL_GLOBALS_TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());

        let mut path = std::env::temp_dir();
        path.push(format!("kiro-t14-overlay-{}.json", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let store =
            crate::anthropic::model_registry_store::ModelRegistryStore::new(path.clone());

        store
            .mutate(|f| {
                let mut row = t14_row("t14-overlay", 9005);
                row.display_name = "覆盖层名字".to_string();
                row.max_output_tokens = 4_242;
                f.models.push(row);
                f.aliases.push(crate::anthropic::model_registry::ModelAlias {
                    from: "t14-alias".to_string(),
                    to: "t14-overlay".to_string(),
                });
                Ok(())
            })
            .await
            .expect("覆盖层应通过校验并落盘");

        let outcome = store.load();
        let degraded = outcome.degraded_reason.clone();
        install_registry(outcome.registry);

        let models = available_models();
        let alias_target = crate::anthropic::converter::map_model("t14-alias");
        let builtin_still_there = models.iter().any(|m| m.id == "claude-opus-4-8");

        install_registry(ModelRegistry::builtin());
        let _ = std::fs::remove_file(&path);

        assert!(degraded.is_none(), "合法覆盖层不应触发降级: {:?}", degraded);
        assert!(builtin_still_there, "覆盖层必须叠加在内置默认之上，而不是替换掉它");

        let row = models
            .iter()
            .find(|m| m.id == "t14-overlay")
            .expect("覆盖层新增的行应出现在 /v1/models");
        assert_eq!(row.display_name, "覆盖层名字");
        assert_eq!(row.max_tokens, 4_242);

        assert_eq!(
            alias_target.as_deref(),
            Some("t14-overlay"),
            "只存在于 models.json 的 alias 必须能被请求解析用到"
        );
    }

    /// 三条路由各自的未知/禁用模型文案。
    ///
    /// **这条只是文案漂移的哨兵，不是行为测试**：handlers 的两处（`post_messages`
    /// 与 OpenAI 兼容层）与 websearch_loop 各自内联了一份 `format!`，无法在不
    /// 起完整 handler 的前提下直接调用。这里断言的是它们各自参照的基准 ——
    /// `ConversionError` 的 `Display`（中文）。websearch 路径的英文文案在
    /// `websearch_loop.rs` 的同名测试里。
    #[test]
    fn unknown_model_messages_per_route_unchanged() {
        use crate::anthropic::converter::ConversionError;

        let e = ConversionError::UnsupportedModel("claude-opus-9".to_string());
        assert_eq!(e.to_string(), "模型不支持: claude-opus-9");
        assert_eq!(format!("模型不支持: {}", "claude-opus-9"), e.to_string());

        let e = ConversionError::ModelDisabled("claude-opus-9".to_string());
        assert_eq!(e.to_string(), "模型已禁用: claude-opus-9");
        assert_eq!(format!("模型已禁用: {}", "claude-opus-9"), e.to_string());
    }

    /// anthropic 两条路由（`post_messages` / `post_messages_cc`）的错误**响应**本身。
    ///
    /// 与上面那条哨兵的区别：哨兵比的是 `ConversionError::Display` 的字面量，
    /// 证明不了路由真的这样回；这条直接把响应构造出来，解出真实报文，
    /// 断言**状态码 + JSON 结构 + 每一条文案**。四个变体一个不落 ——
    /// 漏掉一个分支（比如以后新增变体后 match 写错）会在这里当场暴露。
    ///
    /// 覆盖的是抽出来的 `conversion_error_response`，两条路由的 `Err` 分支现在
    /// 都只调用它，不再各自内联一份 `format!`。
    #[tokio::test]
    async fn conversion_error_response_carries_status_and_chinese_body() {
        use crate::anthropic::converter::ConversionError;

        let cases: Vec<(ConversionError, &str)> = vec![
            (
                ConversionError::UnsupportedModel("claude-opus-9".to_string()),
                "模型不支持: claude-opus-9",
            ),
            (
                ConversionError::ModelDisabled("claude-opus-9".to_string()),
                "模型已禁用: claude-opus-9",
            ),
            (ConversionError::EmptyMessages, "消息列表为空"),
            (
                ConversionError::UnsupportedToolMapping("web_search".to_string()),
                "工具映射不支持: web_search",
            ),
        ];

        for (err, expected_message) in cases {
            let resp = conversion_error_response(&err).into_response();
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "{:?} 必须是 400，客户端靠状态码区分「请求写错了」与「上游挂了」",
                err
            );
            let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                json["error"]["type"], "invalid_request_error",
                "{:?} 的 error.type 不对，实际报文: {}",
                err, json
            );
            assert_eq!(
                json["error"]["message"], expected_message,
                "{:?} 的文案不对，实际报文: {}",
                err, json
            );
        }
    }

    /// 中文/英文两套文案是**刻意**不同的（anthropic 路由 vs web-search 路由）。
    /// 谁要是"顺手统一"，这里当场红。
    #[test]
    fn anthropic_and_websearch_routes_keep_distinct_wording() {
        use crate::anthropic::converter::ConversionError;

        let err = ConversionError::UnsupportedModel("claude-opus-9".to_string());
        let (zh_status, zh) = conversion_error_response(&err);
        let (en_status, en) =
            crate::anthropic::websearch_loop::conversion_error_response(&err);

        assert_eq!(zh_status, en_status, "两条路由的状态码必须一致（都是 400）");
        assert_eq!(zh.0.error.message, "模型不支持: claude-opus-9");
        assert_eq!(en.0.error.message, "unsupported model: claude-opus-9");
        assert_ne!(
            zh.0.error.message, en.0.error.message,
            "两条路由的文案不得合并：客户端可能在匹配其中一份"
        );
    }

    /// 组支持校验（纯逻辑层）：allowed 集合里没有该模型解析出的 upstream id → 404 not_found_error。
    ///
    /// 用 `claude-opus-4-7` 而非 brief 草稿里的 `claude-opus-5`：注册表里没有
    /// `-5` 这个 exposed_id，在 `allow_passthrough() == false`（测试期默认值）下
    /// 会直接 `Rejected(Unknown)`，走的是"放行交给下游 400"分支而非本测试要
    /// 覆盖的"命中但不在组内"分支——用一个真实存在于内置注册表的 exposed_id
    /// 才能实际验证拒绝路径。
    #[test]
    fn group_model_check_rejects_with_404_not_found_error() {
        let mut allowed = std::collections::HashSet::new();
        allowed.insert("glm-5".to_string());
        let err = group_model_check_against(&allowed, "claude-opus-4-7")
            .expect_err("组不支持应拒绝");
        assert_eq!(err.0, StatusCode::NOT_FOUND);
        assert_eq!(err.1.0.error.error_type, "not_found_error");
        assert_eq!(
            err.1.0.error.message,
            "model not supported for this key group: claude-opus-4-7"
        );
    }

    #[test]
    fn group_model_check_allows_auto_alias_and_unresolvable() {
        let allowed: std::collections::HashSet<String> =
            [("glm-5".to_string())].into_iter().collect();
        // auto 恒放行
        assert!(group_model_check_against(&allowed, "auto").is_ok());
        // 注册表不认识的名字：放行，交由既有 conversion_error_response 路径报 400，
        // 两类错误不混同
        assert!(group_model_check_against(&allowed, "no-such-model-xyz").is_ok());
    }

    /// max_tokens 必须为正数：0 与负数应被拒绝，正数放行。
    #[test]
    fn max_tokens_must_be_positive() {
        assert!(validate_max_tokens(1).is_ok());
        assert!(validate_max_tokens(super::super::types::DEFAULT_MAX_TOKENS).is_ok());

        let err = validate_max_tokens(0).expect_err("0 必须被拒绝");
        assert_eq!(err.error.error_type, "invalid_request_error");
        assert_eq!(err.error.message, "max_tokens must be greater than 0");

        assert!(validate_max_tokens(-1).is_err());
    }
}

#[cfg(test)]
mod tracer_tests {
    use super::*;
    use crate::admin::trace_db::{outcome, phase};

    /// 构造一个 store 为 None 的 tracer：验证 phase API 在未启用 trace 时不 panic
    fn detached_tracer() -> RequestTracer {
        RequestTracer {
            store: None,
            trace_id: "t".to_string(),
            ts: "now".to_string(),
            key_id: 0,
            key_source: TraceKeySource::MasterApiKey,
            model: "m".to_string(),
            is_stream: true,
            session_id: None,
            started_at: Instant::now(),
            first_token_at: parking_lot::Mutex::new(None),
            attempts: parking_lot::Mutex::new(Vec::new()),
            phases: parking_lot::Mutex::new(Vec::new()),
            open_phase: parking_lot::Mutex::new(None),
            shape: parking_lot::Mutex::new(crate::anthropic::stream::StreamShape::default()),
        }
    }

    /// 用给定 chunk 序列拼一个真 `reqwest::Response`（走 http::Response 转换，
    /// 不起网络），用来驱动 `read_non_stream_body` 的分段逻辑。
    fn fake_response(chunks: Vec<&'static [u8]>) -> reqwest::Response {
        let stream = futures::stream::iter(
            chunks
                .into_iter()
                .map(|c| Ok::<_, std::io::Error>(Bytes::from_static(c))),
        );
        let body = reqwest::Body::wrap_stream(stream);
        reqwest::Response::from(http::Response::new(body))
    }

    /// 非流式必须给出 first_token / body_read 两段，而不是一个黑盒总时长。
    /// 这是本次埋点的目的：区分「等上游想」与「收得慢」。
    #[tokio::test]
    async fn non_stream_body_read_splits_first_token_from_body() {
        let t = detached_tracer();
        let body = read_non_stream_body(fake_response(vec![b"abc", b"defg"]), &t)
            .await
            .expect("读取应成功");
        assert_eq!(body, b"abcdefg", "分片累积后必须与原始字节完全一致");

        let phases = t.phases.lock();
        assert_eq!(
            phases.iter().map(|p| p.phase.as_str()).collect::<Vec<_>>(),
            vec![phase::FIRST_TOKEN, phase::BODY_READ],
        );
        assert!(phases.iter().all(|p| p.outcome == outcome::SUCCESS));
        assert_eq!(phases[1].bytes, Some(7), "body_read 记的是累计字节");
        assert!(t.first_token_at.lock().is_some(), "非流式也要标记首字节");
    }

    /// 上游 2xx 但 body 为空：first_token 段永远等不到 chunk。必须显式关段，
    /// 否则它一直挂在 open_phase 里、被 finalize 静默丢弃——这条链路在
    /// 日志里就完全没有分段，等于回到改动前。
    #[tokio::test]
    async fn empty_upstream_body_still_closes_first_token_phase() {
        let t = detached_tracer();
        let body = read_non_stream_body(fake_response(vec![]), &t)
            .await
            .unwrap();
        assert!(body.is_empty());

        let phases = t.phases.lock();
        assert_eq!(phases.len(), 1, "空 body 只有 first_token 一段");
        assert_eq!(phases[0].phase, phase::FIRST_TOKEN);
        assert_eq!(
            phases[0].outcome,
            outcome::UPSTREAM_TRUNCATED,
            "空响应体是上游问题，不能记成 success"
        );
        assert!(
            t.first_token_at.lock().is_none(),
            "一个 chunk 都没来，first_token_ms 必须保持 None 而非 0"
        );
    }

    /// 空 data frame 不得算作首字节：算了的话 first_token_ms 会提前，
    /// 且"上游一个字节都没吐"这件事会被伪装成"已开始传输"。
    #[tokio::test]
    async fn empty_leading_chunk_does_not_count_as_first_token() {
        let t = detached_tracer();
        // 只有空 chunk，随后干净 EOF —— 等价于空响应体
        let body = read_non_stream_body(fake_response(vec![b"", b""]), &t)
            .await
            .unwrap();
        assert!(body.is_empty());

        let phases = t.phases.lock();
        assert_eq!(phases.len(), 1, "空 chunk 不应推进到 body_read 段");
        assert_eq!(phases[0].phase, phase::FIRST_TOKEN);
        assert_eq!(phases[0].outcome, outcome::UPSTREAM_TRUNCATED);
        assert!(
            t.first_token_at.lock().is_none(),
            "没有任何实际字节，first_token_ms 必须保持 None"
        );
    }

    /// 空 chunk 之后才来真数据：首字节应记在真数据那一刻，段照常推进。
    #[tokio::test]
    async fn empty_chunk_followed_by_data_marks_first_token_on_the_data() {
        let t = detached_tracer();
        let body = read_non_stream_body(fake_response(vec![b"", b"xy"]), &t)
            .await
            .unwrap();
        assert_eq!(body, b"xy");

        let phases = t.phases.lock();
        assert_eq!(
            phases.iter().map(|p| p.phase.as_str()).collect::<Vec<_>>(),
            vec![phase::FIRST_TOKEN, phase::BODY_READ],
        );
        assert_eq!(phases[1].bytes, Some(2));
        assert!(t.first_token_at.lock().is_some());
    }

    /// attempt 的 started_ms 由 sink 侧回推——provider 填的是 None。
    /// 没有它，色块条无法把重试 backoff 与真实跳耗时区分开。
    /// `started_ms` 由 sink 侧回推——provider 填 None（它不持有请求起点）。
    ///
    /// 断言到具体数值而非仅 `is_some()`：只测非空的话，即使所有跳都错算成 0
    /// 也能通过，而"全 0"恰好等于"色块条上重试 backoff 全部消失"这个失败形态。
    #[tokio::test]
    async fn on_attempt_backfills_started_ms_preserving_gap_between_hops() {
        let t = detached_tracer();
        let mk = |attempt: u32, duration_ms: u64| TraceAttempt {
            attempt,
            credential_id: 1,
            endpoint: "ide".to_string(),
            http_status: Some(200),
            outcome: outcome::SUCCESS.to_string(),
            error_snippet: None,
            duration_ms,
            started_ms: None,
            proxy_url: None,
        };
        // 第 0 跳：耗时 20ms，在请求开始后不久上报 → 起点应接近 0
        tokio::time::sleep(Duration::from_millis(20)).await;
        t.on_attempt(mk(0, 20));
        // 模拟 backoff：等 40ms 再跑第 1 跳（耗时 20ms）
        tokio::time::sleep(Duration::from_millis(60)).await;
        t.on_attempt(mk(1, 20));

        let got = t.attempts.lock();
        let s0 = got[0].started_ms.expect("第 0 跳应有起点");
        let s1 = got[1].started_ms.expect("第 1 跳应有起点");
        // 采样延迟只会让起点右移，给宽容上界即可；关键是别退化成 0
        assert!(s0 <= 15, "第 0 跳起点应接近请求起点，实际 {s0}");
        assert!(
            s1 >= s0 + 20,
            "第 1 跳起点必须晚于第 0 跳终点，否则 backoff 空隙会被抹平：s0={s0} s1={s1}"
        );
        assert!(
            s1 > got[0].started_ms.unwrap() + got[0].duration_ms,
            "两跳之间应留出可识别的 backoff 空隙"
        );
    }

    #[test]
    fn phases_accumulate_in_order_with_seq() {
        let t = detached_tracer();
        t.open_phase(phase::FIRST_TOKEN);
        t.close_phase(phase::FIRST_TOKEN, outcome::SUCCESS, Some(0), None);
        t.open_phase(phase::STREAMING);
        t.close_phase(phase::STREAMING, outcome::SUCCESS, Some(20211), None);
        t.open_phase(phase::FINISH);
        t.close_phase(
            phase::FINISH,
            outcome::UPSTREAM_TRUNCATED,
            Some(20211),
            Some("buffered 331 bytes".to_string()),
        );

        let got = t.phases.lock();
        assert_eq!(got.len(), 3);
        assert_eq!(
            got.iter().map(|p| p.seq).collect::<Vec<_>>(),
            vec![0, 1, 2],
            "seq 必须连续递增"
        );
        assert_eq!(got[2].outcome, outcome::UPSTREAM_TRUNCATED);
        assert_eq!(got[2].bytes, Some(20211));
    }

    /// max_idle_ms 必须是响应体阶段的**最大** chunk 间隔，不是最后一段。
    ///
    /// 这个区别就是它存在的意义：卡死通常发生在流中段（长思考），末尾往往是连续
    /// 输出，只记末值会把最长静默完全漏掉；该峰值是校准空闲超时的观测之一。
    #[test]
    fn max_idle_ms_tracks_the_largest_gap_not_the_last() {
        let t = std::sync::Arc::new(detached_tracer());
        t.open_phase(phase::FIRST_TOKEN);
        let mut guard = StreamPhaseGuard::new(t.clone(), 0);
        guard.mark_first_chunk();

        // 中段一个较长静默，随后是密集输出：末值远小于峰值
        std::thread::sleep(std::time::Duration::from_millis(40));
        guard.set_bytes(100); // 记下 ~40ms 的峰值
        guard.set_bytes(200); // ~0ms，不得覆盖峰值
        guard.set_bytes(300);

        assert!(
            guard.max_idle_ms() >= 40,
            "应保留中段峰值，实际 {}",
            guard.max_idle_ms()
        );

        guard.into_completed(300);
        let got = t.phases.lock();
        let streaming = got
            .iter()
            .find(|p| p.phase == phase::STREAMING)
            .expect("streaming 段应已关闭");
        assert_eq!(streaming.outcome, outcome::SUCCESS);
        let detail = streaming.detail.as_deref().unwrap_or_default();
        assert!(
            detail.starts_with("max_idle_ms="),
            "成功流也必须写 max_idle_ms（否则合法静默分布仍不可见）: {detail}"
        );
        let recorded: u64 = detail
            .trim_start_matches("max_idle_ms=")
            .parse()
            .expect("max_idle_ms 应为整数");
        assert!(recorded >= 40, "落盘值应是峰值而非末值: {recorded}");
    }

    #[test]
    fn close_without_open_is_ignored_not_panic() {
        let t = detached_tracer();
        // 异常路径：埋点漏了 open 直接 close，不得 panic、不得写入半截段
        t.close_phase(phase::STREAMING, outcome::SUCCESS, None, None);
        assert!(t.phases.lock().is_empty());
    }

    #[test]
    fn close_with_mismatched_name_is_ignored_and_does_not_wedge_tracer() {
        let t = detached_tracer();
        // 异常路径：open 了 A，却拿 B 来 close——名字不匹配，静默忽略，且
        // open_phase 已经被 .take() 出来丢弃，不会残留一个「一直开着」的段。
        t.open_phase(phase::FIRST_TOKEN);
        t.close_phase(phase::STREAMING, outcome::SUCCESS, None, None);
        assert!(
            t.phases.lock().is_empty(),
            "名字不匹配不得写入任何段"
        );

        // 证明没有被腐蚀：mismatch 之后重新 open/close 仍能正常记录，seq 从 0 开始。
        t.open_phase(phase::FIRST_TOKEN);
        t.close_phase(phase::FIRST_TOKEN, outcome::SUCCESS, Some(1), None);
        let got = t.phases.lock();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].seq, 0, "mismatch 不得腐蚀 seq 计数器");
    }

    #[test]
    fn reopen_without_close_discards_previous_open_segment() {
        let t = detached_tracer();
        // 文档语义：重复 open 视为埋点漏关，丢弃前一个未关闭的段。
        t.open_phase(phase::FIRST_TOKEN);
        t.open_phase(phase::STREAMING);
        t.close_phase(phase::STREAMING, outcome::SUCCESS, None, None);

        let got = t.phases.lock();
        assert_eq!(got.len(), 1, "只应记录后一个 open 对应的段");
        assert_eq!(got[0].phase, phase::STREAMING);
    }

    /// 探针：验证 stream 被 drop 时，unfold 状态里的 Drop impl 会执行。
    /// 这是 `StreamPhaseGuard` 方案的前提——若此断言失败，客户端断开检测需改用其它手段。
    #[test]
    fn dropped_unfold_state_runs_drop_impl() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        struct Marker(Arc<AtomicBool>);
        impl Drop for Marker {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let flag = Arc::new(AtomicBool::new(false));
        let marker = Marker(flag.clone());

        let s = futures::stream::unfold(marker, |m| async move { Some((1u8, m)) });
        let s = Box::pin(s);
        drop(s); // 模拟客户端断开：整个 stream 被丢弃

        assert!(
            flag.load(Ordering::SeqCst),
            "unfold 状态被 drop 时 Drop impl 应执行；若此断言失败，客户端断开检测需改用其它手段"
        );
    }

    #[test]
    fn client_disconnect_marks_phase_and_does_not_charge_proxy() {
        let t = std::sync::Arc::new(detached_tracer());
        t.open_phase(phase::FIRST_TOKEN);
        {
            let mut guard = StreamPhaseGuard::new(t.clone(), 4096);
            guard.mark_first_chunk(); // 已收到过 chunk，段已切到 STREAMING
            // guard 在此作用域结束时 drop —— 模拟客户端断开
        }
        let got = t.phases.lock();
        // mark_first_chunk 先关闭一段 FIRST_TOKEN（成功），guard 的 Drop 再关闭
        // 一段 STREAMING（client_disconnected）——两段都应被记录，只断言后者。
        assert_eq!(got.len(), 2);
        let seg = &got[1];
        assert_eq!(seg.phase, phase::STREAMING);
        assert_eq!(
            seg.outcome,
            outcome::CLIENT_DISCONNECTED,
            "客户端断开必须与上游断流区分，否则会冤枉代理"
        );
        assert_eq!(seg.bytes, Some(4096));
        let detail = seg.detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("client_gone=true"),
            "detail 必须带 client_gone 判别位，实际: {detail}"
        );
        assert!(
            detail.contains("idle_ms="),
            "detail 必须带 idle_ms 判别位，实际: {detail}"
        );
        assert!(
            detail.contains("bytes="),
            "detail 必须带 bytes 判别位，实际: {detail}"
        );
    }

    #[test]
    fn normal_completion_does_not_mark_client_disconnect() {
        let t = std::sync::Arc::new(detached_tracer());
        t.open_phase(phase::FIRST_TOKEN);
        {
            let mut guard = StreamPhaseGuard::new(t.clone(), 4096);
            guard.mark_first_chunk(); // 已收到过 chunk，段已切到 STREAMING
            guard.into_completed(4096); // 正常收尾：按值消费，解除哨兵
        }
        let got = t.phases.lock();
        // mark_first_chunk 先关闭一段 FIRST_TOKEN，into_completed 再关闭一段
        // STREAMING——两段都应被记录，只断言后者。
        assert_eq!(
            got.len(),
            2,
            "正常收尾应记 first_token + streaming 两段成功，而不是空白"
        );
        let seg = &got[1];
        assert_eq!(seg.phase, phase::STREAMING);
        assert_eq!(
            seg.outcome,
            outcome::SUCCESS,
            "正常收尾不得被误记成客户端断开"
        );
        assert_eq!(seg.bytes, Some(4096));
    }

    /// 回归测试（评审 Critical）：guard 在 FIRST_TOKEN 段仍打开、尚未收到首个
    /// chunk 时就被按值消费（例如上游 200 后立即断流，body 里零个 chunk）。
    /// 若 guard 的终态方法硬编码 close_phase(STREAMING,...)，这里的段名会与
    /// 实际打开的 FIRST_TOKEN 不匹配，被 close_phase 静默丢弃——现象是
    /// phases 里什么都没有，这正是本功能要消灭的"静默丢失"本身。
    #[test]
    fn guard_consumed_before_first_chunk_closes_first_token_not_streaming() {
        let t = std::sync::Arc::new(detached_tracer());
        t.open_phase(phase::FIRST_TOKEN); // 尚未收到首个 chunk，未调用 mark_first_chunk
        {
            let guard = StreamPhaseGuard::new(t.clone(), 0);
            guard.into_upstream_error(0, &"upstream reset before any chunk");
        }
        let got = t.phases.lock();
        assert_eq!(
            got.len(),
            1,
            "尚未收到首个 chunk 时上游断流也必须记录一段，不能因为段名不匹配被吞掉"
        );
        assert_eq!(
            got[0].phase,
            phase::FIRST_TOKEN,
            "guard 消费时实际打开的段是 FIRST_TOKEN，不是 STREAMING，必须按实际打开的段名收尾"
        );
        assert_eq!(got[0].outcome, outcome::STREAM_INTERRUPTED);
    }

    /// 同上，但覆盖 Drop 路径（客户端在等待首个 token 期间断开）。
    #[test]
    fn guard_dropped_before_first_chunk_closes_first_token_not_streaming() {
        let t = std::sync::Arc::new(detached_tracer());
        t.open_phase(phase::FIRST_TOKEN);
        {
            let _guard = StreamPhaseGuard::new(t.clone(), 0);
            // 客户端在收到首个 chunk 之前断开：guard 未被任何终态方法消费，走 Drop。
        }
        let got = t.phases.lock();
        assert_eq!(
            got.len(),
            1,
            "等待首个 token 期间客户端断开也必须记录一段，不能被段名不匹配吞掉"
        );
        assert_eq!(
            got[0].phase,
            phase::FIRST_TOKEN,
            "Drop 收尾时也必须按实际打开的段名（FIRST_TOKEN），不能硬编码 STREAMING"
        );
        assert_eq!(got[0].outcome, outcome::CLIENT_DISCONNECTED);
    }

    #[test]
    fn upstream_error_does_not_mark_client_disconnect() {
        let t = std::sync::Arc::new(detached_tracer());
        t.open_phase(phase::FIRST_TOKEN);
        {
            let mut guard = StreamPhaseGuard::new(t.clone(), 512);
            guard.mark_first_chunk(); // 已收到过 chunk，段已切到 STREAMING
            guard.into_upstream_error(512, &"connection reset");
        }
        let got = t.phases.lock();
        // mark_first_chunk 先关闭一段 FIRST_TOKEN，into_upstream_error 再关闭一段
        // STREAMING——两段都应被记录，只断言后者。
        assert_eq!(got.len(), 2);
        let seg = &got[1];
        assert_eq!(seg.phase, phase::STREAMING);
        assert_eq!(
            seg.outcome,
            outcome::STREAM_INTERRUPTED,
            "上游断流不得被误记成客户端断开"
        );
        assert_eq!(seg.bytes, Some(512));
        let detail = seg.detail.as_deref().unwrap_or("");
        assert!(
            detail.contains("client_gone=false"),
            "上游断流的 detail 必须明确 client_gone=false，实际: {detail}"
        );
        assert!(
            detail.contains("bytes="),
            "detail 必须带 bytes 判别位，实际: {detail}"
        );
        assert!(
            detail.contains("idle_ms="),
            "detail 必须带 idle_ms 判别位，实际: {detail}"
        );
        assert!(
            detail.contains("err=connection reset"),
            "detail 必须带上游错误文本，实际: {detail}"
        );
    }

    /// 端到端验证 axum drop 前提：真实起一个 axum 服务，用裸 TCP socket 连接，
    /// 读到几个 chunk 后直接 drop 掉 socket（不发 FIN 的优雅关闭，模拟客户端
    /// 突然断开），等待服务端感知，断言 streaming 段被记成 client_disconnected
    /// 且 bytes > 0（证明断开发生在已发出内容之后）。
    ///
    /// Task 4 的探针只证明了 `stream::unfold` 状态被 drop 时 Drop impl 会执行
    /// （纯 Rust 语义）。它没有证明 axum 在客户端真实断开的连接上，会在 guard
    /// 还能起作用的时机把响应 body 的 unfold 状态 drop 掉——这是本测试要补的那一环。
    #[tokio::test]
    async fn client_disconnect_end_to_end() {
        use axum::extract::State;
        use axum::response::IntoResponse;
        use axum::routing::get;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::{TcpListener, TcpStream};

        // tracer 用内存 TraceStore：本测试断言的是落库结果，而不是 tracer 的
        // 内存态 phases——原始事故的病灶正是"断连时 finalize 永不执行，phases
        // 停留在内存里、随 Arc 一起被丢弃，traces 表里连一行都没有"。
        // Drop 现在必须调用 finalize_on_disconnect，把这个洞补上。
        let store = crate::admin::trace_db::TraceStore::open_in_memory()
            .expect("打开内存 trace store 失败");
        let tracer = std::sync::Arc::new(RequestTracer {
            store: Some(std::sync::Arc::new(store)),
            trace_id: "e2e-disconnect".to_string(),
            ts: "now".to_string(),
            key_id: 0,
            key_source: TraceKeySource::MasterApiKey,
            model: "m".to_string(),
            is_stream: true,
            session_id: None,
            started_at: Instant::now(),
            first_token_at: parking_lot::Mutex::new(None),
            attempts: parking_lot::Mutex::new(Vec::new()),
            phases: parking_lot::Mutex::new(Vec::new()),
            open_phase: parking_lot::Mutex::new(None),
            shape: parking_lot::Mutex::new(crate::anthropic::stream::StreamShape::default()),
        });
        // 与真实调用点保持一致：guard 构造前先手动打开 FIRST_TOKEN，
        // 首个 chunk 到达时再由 guard.mark_first_chunk() 切到 STREAMING
        // （guard 内部记着 current_phase，收尾时按它，而不是硬编码 STREAMING）。
        tracer.open_phase(phase::FIRST_TOKEN);

        // 路由：慢速 SSE 流，每 50ms 发一个 chunk，共 20 个；guard 挂在流状态里。
        async fn slow_sse_handler(State(tracer): State<std::sync::Arc<RequestTracer>>) -> impl IntoResponse {
            let guard = StreamPhaseGuard::new(tracer, 0);
            let body = stream::unfold((0u32, Some(guard), true), |(i, mut guard, mut first_chunk)| async move {
                if i >= 20 {
                    if let Some(g) = guard.take() {
                        g.into_completed((i as u64) * 5);
                    }
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
                let sent = (i + 1) as u64 * 5;
                if let Some(g) = guard.as_mut() {
                    if first_chunk {
                        g.mark_first_chunk();
                        first_chunk = false;
                    }
                    g.set_bytes(sent);
                }
                let chunk: Result<Bytes, Infallible> = Ok(Bytes::from("event: ping\ndata: x\n\n"));
                Some((chunk, (i + 1, guard, first_chunk)))
            });
            axum::response::Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, "text/event-stream")
                .body(Body::from_stream(body))
                .unwrap()
        }

        let app = axum::Router::new()
            .route("/stream", get(slow_sse_handler))
            .with_state(tracer.clone());

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("绑定本地端口失败");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        // 裸 socket 连接，发原始 HTTP 请求
        let mut sock = TcpStream::connect(addr)
            .await
            .expect("连接测试服务器失败");
        sock.write_all(
            b"GET /stream HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n",
        )
        .await
        .expect("发送请求失败");

        // 读够至少 3 个 chunk 的时长（>150ms）再 drop，确保断开确实发生在
        // 已发出内容之后——只看字节数不够，响应头本身就可能超过任意阈值。
        let mut received = 0usize;
        let read_until = tokio::time::Instant::now() + Duration::from_millis(220);
        loop {
            let mut buf = [0u8; 512];
            let remaining = read_until.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, sock.read(&mut buf)).await {
                Ok(Ok(n)) if n > 0 => received += n,
                _ => break,
            }
        }
        assert!(received > 0, "测试服务器应至少发出了响应头/若干个 chunk");

        // 模拟客户端突然断开：直接 drop 掉 socket（不走 shutdown() 优雅关闭）
        drop(sock);

        // 等待服务端感知断开——服务端下一次写入会因连接已断失败，触发 axum 丢弃
        // response body 的 unfold 状态，进而运行 guard 的 Drop。
        tokio::time::sleep(Duration::from_millis(500)).await;

        // 评审 Important #2 的回归断言：不再看 tracer.phases（那只证明了内存态
        // 埋点，不证明落库），而是查 TraceStore，证明 finalize_on_disconnect
        // 真的把这条 trace 写进了 traces 表——ops UI 能查到它。
        let query = crate::admin::trace_db::TraceQuery {
            operation: None,
            status: None,
            error_type: Some(outcome::CLIENT_DISCONNECTED.to_string()),
            credential_id: None,
            key_id: None,
            failed_attempt_credential_id: None,
            model: None,
            only_failed: false,
            credential_ids: None,
            limit: 10,
            offset: 0,
        };
        let (records, _total) = tracer
            .store
            .as_ref()
            .expect("测试 tracer 必须带 store")
            .query_paged(&query);
        let rec = records
            .iter()
            .find(|r| r.trace_id == tracer.trace_id)
            .expect(
                "断连必须落库一条 trace 记录——finalize 在此路径上原本永不会被调用，\
                 这正是本回归要堵上的洞",
            );
        assert_eq!(rec.final_status, "interrupted");
        assert_eq!(rec.error_type.as_deref(), Some(outcome::CLIENT_DISCONNECTED));
        let seg = rec
            .phases
            .iter()
            .find(|p| p.phase == phase::STREAMING)
            .expect("落库记录里必须包含断连时的 streaming 段");
        assert_eq!(
            seg.outcome,
            outcome::CLIENT_DISCONNECTED,
            "客户端主动断开应记为 client_disconnected，实际: {}",
            seg.outcome
        );
        assert!(
            seg.bytes.unwrap_or(0) > 0,
            "断开必须发生在已发出内容之后，bytes 应 > 0，实际: {:?}",
            seg.bytes
        );
    }

    /// 回归测试（评审 Important #1）：直接覆盖 `phase_on_finish` 这个真正的事故缝合处，
    /// 而不是只测 `phase_outcome_for` 这个纯映射函数。若有人以后把 `phase::FINISH`
    /// 换成 `phase::STREAMING`，或者把 open_phase/close_phase 的调用顺序倒过来，
    /// 只测纯函数的话全量测试仍会全绿——这里要能捉住这类回归。
    #[test]
    fn phase_on_finish_records_finish_segment_with_mapped_outcome() {
        let t = detached_tracer();
        t.open_phase(phase::STREAMING); // 模拟 guard 已经 into_completed 关闭了 streaming 段

        let err = ToolJsonAccumulatorError::IncompleteJson {
            tool_use_id: "tu-1".to_string(),
            name: "str_replace".to_string(),
            bytes: 42,
        };
        phase_on_finish(&t, 1234, Some(&err), Some("half-written tool json".to_string()));

        let got = t.phases.lock();
        let seg = got.last().expect("phase_on_finish 必须记录一段 finish");
        assert_eq!(seg.phase, phase::FINISH, "必须记到 finish 段，不能记错名字");
        assert_eq!(
            seg.outcome,
            outcome::UPSTREAM_TRUNCATED,
            "IncompleteJson 必须映射为 upstream_truncated"
        );
        assert_eq!(seg.bytes, Some(1234));
        assert_eq!(seg.detail.as_deref(), Some("half-written tool json"));
    }

    /// 同上，覆盖无 tool_json_error 的正常收尾路径（finish 段应记 success）。
    #[test]
    fn phase_on_finish_records_success_when_no_tool_json_error() {
        let t = detached_tracer();
        t.open_phase(phase::STREAMING);

        phase_on_finish(&t, 999, None, None);

        let got = t.phases.lock();
        let seg = got.last().expect("phase_on_finish 必须记录一段 finish");
        assert_eq!(seg.phase, phase::FINISH);
        assert_eq!(seg.outcome, outcome::SUCCESS);
        assert_eq!(seg.bytes, Some(999));
    }
}
