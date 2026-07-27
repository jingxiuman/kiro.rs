//! Anthropic API Handler 函数

use std::convert::Infallible;
use std::time::Instant;

use crate::admin::client_keys::SharedClientKeyManager;
use crate::admin::usage_stats::{SharedAggregator, SharedRecorder, UsageRecord};
use crate::admin::trace_db::{
    SharedTraceStore, TraceAttempt, TraceKeySource, TraceRecord, TraceSink, outcome,
};
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
use super::stream::{BufferedStreamContext, SseEvent, StreamContext};
use super::types::{
    CountTokensRequest, CountTokensResponse, ErrorResponse, MessagesRequest, Model, ModelsResponse,
    OutputConfig, Thinking,
};
use super::websearch;

/// 请求结束时记录用量的钩子
///
/// 在 handler 入口构造，调用 [`Self::record`] 时把当次请求的 input/output token、
/// 命中的上游凭据 ID、状态写入：
/// - `usage_log.YYYY-MM-DD.jsonl`（持久化历史）
/// - 内存聚合器（仪表盘趋势）
/// - 客户端 Key 计数（按 Key 累计）
#[derive(Clone)]
pub(crate) struct UsageRecordHook {
    pub recorder: Option<SharedRecorder>,
    pub aggregator: Option<SharedAggregator>,
    pub client_keys: Option<SharedClientKeyManager>,
    pub key_id: u64,
    pub model: String,
    pub started_at: Instant,
}

impl UsageRecordHook {
    pub fn from_state(state: &AppState, key_id: u64, model: String) -> Self {
        Self {
            recorder: state.usage_recorder.clone(),
            aggregator: state.usage_aggregator.clone(),
            client_keys: state.client_keys.clone(),
            key_id,
            model,
            started_at: Instant::now(),
        }
    }

    pub fn record(
        &self,
        credential_id: u64,
        input_tokens: i32,
        output_tokens: i32,
        cache_creation_tokens: i32,
        cache_read_tokens: i32,
        credits: f64,
        status: &str,
    ) {
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
        if let Some(r) = &self.recorder {
            r.record(&rec);
        }
        if let Some(a) = &self.aggregator {
            a.ingest(&rec);
        }
        if status == "success" && self.key_id != 0 {
            if let Some(m) = &self.client_keys {
                m.record_usage(
                    self.key_id,
                    rec.input_tokens,
                    rec.output_tokens,
                    rec.cache_creation_tokens,
                    rec.cache_read_tokens,
                    rec.credits,
                );
            }
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
    started_at: Instant,
    /// 首个上游 chunk 到达时刻（仅流式标记；取第一次）
    first_token_at: parking_lot::Mutex<Option<Instant>>,
    attempts: parking_lot::Mutex<Vec<TraceAttempt>>,
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
            started_at: Instant::now(),
            first_token_at: parking_lot::Mutex::new(None),
            attempts: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// 标记首个上游 chunk 到达（幂等，仅记录第一次）
    pub fn mark_first_token(&self) {
        let mut slot = self.first_token_at.lock();
        if slot.is_none() {
            *slot = Some(Instant::now());
        }
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
        // 最终凭据：最后一跳的命中凭据（成功跳即命中凭据，失败跳即最后尝试的凭据）
        let final_credential_id = attempts.last().map(|a| a.credential_id).unwrap_or(0);
        let first_token_ms = self
            .first_token_at
            .lock()
            .map(|t| t.duration_since(self.started_at).as_millis() as u64);
        let rec = TraceRecord {
            trace_id: self.trace_id.clone(),
            ts: self.ts.clone(),
            key_id: self.key_id,
            key_source: self.key_source,
            model: self.model.clone(),
            is_stream: self.is_stream,
            final_status: final_status.to_string(),
            final_credential_id,
            error_type: error_type.map(|s| s.to_string()),
            error_message: error_message.map(|s| s.to_string()),
            total_attempts: attempts.len() as u32,
            duration_ms: self.started_at.elapsed().as_millis() as u64,
            interrupted_after_bytes,
            input_tokens: usage.input_tokens,
            output_tokens: usage.output_tokens,
            cache_creation_tokens: usage.cache_creation_tokens,
            cache_read_tokens: usage.cache_read_tokens,
            credits: usage.credits,
            first_token_ms,
            attempts,
            // Task 3 起填充：流生命周期分段
            phases: Vec::new(),
        };
        store.insert(&rec);
    }
}

impl TraceSink for RequestTracer {
    fn on_attempt(&self, attempt: TraceAttempt) {
        self.attempts.lock().push(attempt);
    }
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

/// GET /v1/models
///
/// 返回可用的模型列表
pub async fn get_models() -> impl IntoResponse {
    tracing::info!("Received GET /v1/models request");

    let models = available_models();

    Json(ModelsResponse {
        object: "list".to_string(),
        data: models,
    })
}

/// POST /v1/messages
///
/// 创建消息（对话）
pub async fn post_messages(
    State(state): State<AppState>,
    Extension(key_ctx): Extension<KeyContext>,
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
    if img_stats.total_b64_bytes > IMAGE_BUDGET_WARN_BYTES {
        tracing::warn!(
            image_count = %img_stats.count,
            image_total_b64_kb = %(img_stats.total_b64_bytes / 1024),
            "incoming image payload is large; if upstream rejects with CONTENT_LENGTH_EXCEEDS_THRESHOLD, reduce image count or use lower-resolution screenshots"
        );
    }
    let hook = UsageRecordHook::from_state(&state, key_ctx.key_id, payload.model.clone());
    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            hook.record(0, 0, 0, 0, 0, 0.0, "error");
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
        hook.record(0, input_tokens, 0, 0, 0, 0.0, status);
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
            hook.record(0, 0, 0, 0, 0, 0.0, "error");
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
            hook.record(0, 0, 0, 0, 0, 0.0, "error");
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
            },
        ));
        handle_stream_request(
            provider,
            &request_body,
            &payload.model,
            total_input_tokens,
            thinking_enabled,
            tool_name_map,
            known_tool_names,
            hook,
            cache_usage,
            tracer,
            key_ctx.group.clone(),
            context_window,
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
            },
        ));
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            total_input_tokens,
            extract_thinking,
            tool_name_map,
            known_tool_names,
            hook,
            cache_usage,
            tracer,
            key_ctx.group.clone(),
            context_window,
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
    thinking_enabled: bool,
    tool_name_map: std::collections::HashMap<String, String>,
    known_tool_names: std::collections::HashSet<String>,
    hook: UsageRecordHook,
    cache_usage: super::cache_metering::CacheUsage,
    tracer: std::sync::Arc<RequestTracer>,
    group: Option<String>,
    // 请求入口随 ConversionResult 传入的输入上下文窗口，见 handle_non_stream_request 同名参数注释。
    context_window: i32,
) -> Response {
    // 调用 Kiro API（支持多凭据故障转移）
    let call_result = match provider.call_api_stream(request_body, Some(tracer.as_ref()), group.as_deref()).await {
        Ok(resp) => resp,
        Err(e) => {
            hook.record(0, input_tokens, 0, 0, 0, 0.0, "error");
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
    ctx.cache_usage = cache_usage;

    // 生成初始事件
    let initial_events = ctx.generate_initial_events();

    // 创建 SSE 流
    let stream =
        create_sse_stream(response, ctx, initial_events, hook, credential_id, tracer, ops_feedback);

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
    // 先发送初始事件
    let initial_stream = stream::iter(
        initial_events
            .into_iter()
            .map(|e| Ok(Bytes::from(e.to_sse_string()))),
    );

    // 然后处理 Kiro 响应流，同时每25秒发送 ping 保活
    let body_stream = response.bytes_stream();

    let processing_stream = stream::unfold(
        (body_stream, ctx, EventStreamDecoder::new(), false, interval(Duration::from_secs(PING_INTERVAL_SECS)), hook, credential_id, tracer, 0u64, ops_feedback),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval, hook, credential_id, tracer, mut sent_bytes, ops_feedback)| async move {
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

                            // 转换为 SSE 字节流
                            let bytes: Vec<Result<Bytes, Infallible>> = events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();

                            Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback)))
                        }
                        Some(Err(e)) => {
                            tracing::error!("读取响应流失败: {}", e);
                            // 发送最终事件并结束（记为 error）
                            let final_events = ctx.generate_final_events();
                            record_stream_usage(&hook, &ctx, credential_id, "error");
                            // 连接已建立后断流 = 传输链路失败，计入所用代理
                            report_stream_outcome(&ops_feedback, true, &e.to_string());
                            // 已开始返回内容后上游断流：标记为 interrupted，带已发送字节数
                            tracer.finalize(
                                "interrupted",
                                Some(outcome::STREAM_INTERRUPTED),
                                Some(&e.to_string()),
                                Some(sent_bytes),
                                stream_trace_usage(&ctx),
                            );
                            let bytes: Vec<Result<Bytes, Infallible>> = final_events
                                .into_iter()
                                .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                .collect();
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback)))
                        }
                        None => {
                            // 流结束，发送最终事件（generate_final_events 内部会 finish()
                            // 累积器，据此判定是否有半截 / 非法工具调用 JSON）。
                            let final_events = ctx.generate_final_events();
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
                            Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback)))
                        }
                    }
                }
                // 发送 ping 保活
                _ = ping_interval.tick() => {
                    tracing::trace!("发送 ping 保活事件");
                    let bytes: Vec<Result<Bytes, Infallible>> = vec![Ok(create_ping_sse())];
                    Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback)))
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
        cache_creation,
        cache_read,
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
async fn handle_non_stream_request(
    provider: std::sync::Arc<crate::kiro::provider::KiroProvider>,
    request_body: &str,
    model: &str,
    input_tokens: i32,
    thinking_enabled: bool,
    tool_name_map: std::collections::HashMap<String, String>,
    // 非流式路径直接处理结构化 Event::ToolUse，不经过 <invoke> 文本嗅探，
    // 因此这里不需要工具表校验；保留参数以对齐调用方签名。
    _known_tool_names: std::collections::HashSet<String>,
    hook: UsageRecordHook,
    cache_usage: super::cache_metering::CacheUsage,
    tracer: std::sync::Arc<RequestTracer>,
    group: Option<String>,
    // 请求入口随 ConversionResult 传入的输入上下文窗口，单请求内只取一次快照，
    // 避免响应处理阶段回头查全局注册表（热重载可能导致「用旧表映射、用新表计量」）。
    context_window: i32,
) -> Response {
    // 调用 Kiro API（支持多凭据故障转移）
    let call_result = match provider.call_api(request_body, Some(tracer.as_ref()), group.as_deref()).await {
        Ok(resp) => resp,
        Err(e) => {
            hook.record(0, input_tokens, 0, 0, 0, 0.0, "error");
            tracer.finalize("error", last_attempt_outcome(&tracer), Some(&e.to_string()), None, TraceUsage::zero());
            return map_provider_error(e);
        }
    };
    let response = call_result.response;
    let credential_id = call_result.credential_id;
    let ops_feedback =
        StreamOpsFeedback::from_call(&provider, credential_id, call_result.proxy_url.clone());

    // 读取响应体
    let body_bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(e) => {
            tracing::error!("读取响应体失败: {}", e);
            hook.record(credential_id, input_tokens, 0, 0, 0, 0.0, "error");
            // 连接已建立后 body 读取失败 = 传输链路失败，计入所用代理
            report_stream_outcome(&ops_feedback, true, &e.to_string());
            tracer.finalize(
                "interrupted",
                Some(outcome::STREAM_INTERRUPTED),
                Some(&e.to_string()),
                None,
                TraceUsage::zero(),
            );
            return (
                StatusCode::BAD_GATEWAY,
                Json(ErrorResponse::new(
                    "api_error",
                    format!("读取响应失败: {}", e),
                )),
            )
                .into_response();
        }
    };

    // 解析事件流
    let mut decoder = EventStreamDecoder::new();
    if let Err(e) = decoder.feed(&body_bytes) {
        tracing::warn!("缓冲区溢出: {}", e);
    }

    let mut text_content = String::new();
    let mut native_thinking = String::new();
    let mut native_thinking_signature: Option<String> = None;
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
                            if let Some(signature) = reasoning.signature
                                && !signature.is_empty()
                            {
                                native_thinking_signature = Some(signature);
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
                        Event::Exception { exception_type, .. } => {
                            if exception_type == "ContentLengthExceededException" {
                                stop_reason = "max_tokens".to_string();
                            }
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

    // 工具调用 JSON 半截 / 非法：非流式路径尚未发送任何字节，直接回 502，
    // 明确暴露上游问题，而不是把无法解析的参数当成完整调用返回。
    // 区分上游截断（传输，计代理失败 + upstream_truncated）与非法 JSON（内容，不罚代理 + upstream_invalid）。
    if let Some(err) = tool_json_error {
        let incomplete = err.is_incomplete();
        let message = err.message();
        hook.record(credential_id, input_tokens, 0, 0, 0, 0.0, "error");
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
        native_thinking_signature,
        native_redacted_thinking,
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

    hook.record(
        credential_id,
        final_input_tokens,
        output_tokens,
        cache_creation_tokens,
        cache_read_tokens,
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
    native_thinking_signature: Option<String>,
    native_redacted_thinking: Vec<String>,
) -> Vec<serde_json::Value> {
    let mut content = Vec::new();
    let has_native_thinking = !native_thinking.is_empty();

    if thinking_enabled {
        if has_native_thinking {
            content.push(json!({
                "type": "thinking",
                "thinking": native_thinking.clone(),
                "signature": native_thinking_signature
                    .unwrap_or_else(|| super::stream::THINKING_SIGNATURE_PLACEHOLDER.to_string()),
            }));
        } else {
            // 从完整文本中提取 thinking 块，兼容旧的 <thinking> 文本路径。
            let (thinking, remaining_text) =
                super::stream::extract_thinking_from_complete_text(&text_content);

            if let Some(thinking_text) = thinking {
                content.push(json!({
                    "type": "thinking",
                    "thinking": thinking_text,
                    "signature": super::stream::THINKING_SIGNATURE_PLACEHOLDER,
                }));
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

    payload.thinking = Some(Thinking {
        thinking_type: thinking_type.to_string(),
        budget_tokens: 20000,
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
    Extension(_key_ctx): Extension<KeyContext>,
    JsonExtractor(payload): JsonExtractor<CountTokensRequest>,
) -> impl IntoResponse {
    tracing::info!(
        model = %payload.model,
        message_count = %payload.messages.len(),
        "Received POST /v1/messages/count_tokens request"
    );

    let total_tokens = token::count_all_tokens(
        payload.model,
        payload.system,
        payload.messages,
        payload.tools,
    ) as i32;

    Json(CountTokensResponse {
        input_tokens: total_tokens.max(1) as i32,
    })
}

/// POST /cc/v1/messages
///
/// Claude Code 兼容端点，与 /v1/messages 的区别在于：
/// - 流式响应会等待 kiro 端返回 contextUsageEvent 后再发送 message_start
/// - message_start 中的 input_tokens 是从 contextUsageEvent 计算的准确值
pub async fn post_messages_cc(
    State(state): State<AppState>,
    Extension(key_ctx): Extension<KeyContext>,
    JsonExtractor(mut payload): JsonExtractor<MessagesRequest>,
) -> Response {
    tracing::info!(
        model = %payload.model,
        max_tokens = %payload.max_tokens,
        stream = %payload.stream,
        message_count = %payload.messages.len(),
        "Received POST /cc/v1/messages request"
    );
    let hook = UsageRecordHook::from_state(&state, key_ctx.key_id, payload.model.clone());

    // 检查 KiroProvider 是否可用
    let provider = match &state.kiro_provider {
        Some(p) => p.clone(),
        None => {
            tracing::error!("KiroProvider 未配置");
            hook.record(0, 0, 0, 0, 0, 0.0, "error");
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
        hook.record(0, input_tokens, 0, 0, 0, 0.0, status);
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
            hook.record(0, 0, 0, 0, 0, 0.0, "error");
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
            hook.record(0, 0, 0, 0, 0, 0.0, "error");
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
            },
        ));
        handle_stream_request_buffered(
            provider,
            &request_body,
            &payload.model,
            thinking_enabled,
            tool_name_map,
            known_tool_names,
            hook,
            total_input_tokens,
            cache_usage,
            tracer,
            key_ctx.group.clone(),
            context_window,
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
            },
        ));
        handle_non_stream_request(
            provider,
            &request_body,
            &payload.model,
            total_input_tokens,
            extract_thinking,
            tool_name_map,
            known_tool_names,
            hook,
            cache_usage,
            tracer,
            key_ctx.group.clone(),
            context_window,
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
    thinking_enabled: bool,
    tool_name_map: std::collections::HashMap<String, String>,
    known_tool_names: std::collections::HashSet<String>,
    hook: UsageRecordHook,
    fallback_input_tokens: i32,
    cache_usage: super::cache_metering::CacheUsage,
    tracer: std::sync::Arc<RequestTracer>,
    group: Option<String>,
    // 请求入口随 ConversionResult 传入的输入上下文窗口，见 handle_non_stream_request 同名参数注释。
    context_window: i32,
) -> Response {
    // 调用 Kiro API（支持多凭据故障转移）
    let call_result = match provider.call_api_stream(request_body, Some(tracer.as_ref()), group.as_deref()).await {
        Ok(resp) => resp,
        Err(e) => {
            hook.record(0, fallback_input_tokens, 0, 0, 0, 0.0, "error");
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
    ctx.set_cache_usage(cache_usage);

    // 创建缓冲 SSE 流
    let stream =
        create_buffered_sse_stream(response, ctx, hook, credential_id, tracer, ops_feedback);

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
        ),
        |(mut body_stream, mut ctx, mut decoder, finished, mut ping_interval, hook, credential_id, tracer, mut sent_bytes, ops_feedback)| async move {
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
                        return Some((stream::iter(bytes), (body_stream, ctx, decoder, false, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback)));
                    }

                    // 然后处理数据流
                    chunk_result = body_stream.next() => {
                        match chunk_result {
                            Some(Ok(chunk)) => {
                                tracer.mark_first_token();
                                sent_bytes += chunk.len() as u64;
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
                                tracing::error!("读取响应流失败: {}", e);
                                // 发生错误，完成处理并返回所有事件
                                let all_events = ctx.finish_and_get_all_events();
                                let (i, o, cc, cr, credits) = ctx.final_usage();
                                hook.record(credential_id, i, o, cc, cr, credits, "error");
                                report_stream_outcome(&ops_feedback, true, &e.to_string());
                                // 缓冲模式 chunk 读取失败：上游中途断流
                                tracer.finalize(
                                    "interrupted",
                                    Some(outcome::STREAM_INTERRUPTED),
                                    Some(&e.to_string()),
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
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback)));
                            }
                            None => {
                                // 流结束，完成处理并返回所有事件（已更正 input_tokens）。
                                // finish_and_get_all_events 内部会 finish() 累积器；若有半截 /
                                // 非法工具调用 JSON，error 事件已随缓冲发出，这里据此记 error。
                                let all_events = ctx.finish_and_get_all_events();
                                let (i, o, cc, cr, credits) = ctx.final_usage();
                                let trace_usage = TraceUsage {
                                    input_tokens: i.max(0) as u64,
                                    output_tokens: o.max(0) as u64,
                                    cache_creation_tokens: cc.max(0) as u64,
                                    cache_read_tokens: cr.max(0) as u64,
                                    credits: if credits.is_finite() && credits > 0.0 { credits } else { 0.0 },
                                };
                                if let Some(message) = ctx.tool_json_error_message() {
                                    // 区分上游截断（传输，计代理失败）与上游非法 JSON（内容，不罚代理），
                                    // 见实时流路径同名分支
                                    let incomplete =
                                        ctx.tool_json_error_incomplete().unwrap_or(true);
                                    hook.record(credential_id, i, o, cc, cr, credits, "error");
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
                                    hook.record(credential_id, i, o, cc, cr, credits, "success");
                                    report_stream_outcome(&ops_feedback, false, "");
                                    tracer.finalize("success", None, None, None, trace_usage);
                                }
                                let bytes: Vec<Result<Bytes, Infallible>> = all_events
                                    .into_iter()
                                    .map(|e| Ok(Bytes::from(e.to_sse_string())))
                                    .collect();
                                return Some((stream::iter(bytes), (body_stream, ctx, decoder, true, ping_interval, hook, credential_id, tracer, sent_bytes, ops_feedback)));
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
            Some("real-signature".to_string()),
            vec!["encrypted-thinking".to_string()],
        );

        assert_eq!(content.len(), 3);
        assert_eq!(content[0]["type"], "thinking");
        assert_eq!(content[0]["thinking"], "native thinking");
        assert_eq!(content[0]["signature"], "real-signature");
        assert_eq!(content[1]["type"], "redacted_thinking");
        assert_eq!(content[1]["data"], "encrypted-thinking");
        assert_eq!(content[2]["type"], "text");
        assert_eq!(content[2]["text"], "final answer");
    }

    #[test]
    fn non_stream_legacy_thinking_extraction_still_works_without_native_reasoning() {
        let content = build_non_stream_content(
            true,
            "<thinking>legacy thinking</thinking>\n\nfinal answer".to_string(),
            String::new(),
            None,
            Vec::new(),
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
            Some("ignored-signature".to_string()),
            vec!["ignored-redacted".to_string()],
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

        let resp = get_models().await.into_response();
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
}
