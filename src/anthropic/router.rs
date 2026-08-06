//! Anthropic API 路由配置

use axum::{
    Router,
    extract::DefaultBodyLimit,
    middleware,
    routing::{get, post},
};

use crate::admin::client_keys::SharedClientKeyManager;
use crate::admin::usage_store::SharedUsageStore;
use crate::admin::trace_db::SharedTraceStore;
use crate::kiro::provider::KiroProvider;
use crate::model::config::ToolCompatibilityMode;

use super::{
    handlers::{count_tokens, get_models, post_messages, post_messages_cc},
    middleware::{AppState, auth_middleware, cors_layer},
    openai::post_chat_completions,
    responses::post_responses,
    cache_metering::SharedCacheMeter,
};

/// 请求体最大大小限制 (50MB)
const MAX_BODY_SIZE: usize = 50 * 1024 * 1024;

/// 创建带有 KiroProvider 的 Anthropic API 路由
///
/// 给嵌入到其他 Rust 项目的下游使用者预留的扩展点。
///
/// # 模型注册表契约（spec §3.2）
///
/// 本函数不接受配置，模型解析走进程级注册表
/// （`crate::anthropic::model_registry` 的 `REGISTRY` holder）。
///
/// - **要用自定义模型表的调用方，必须在调用本函数之前**完成
///   `model_registry::install_registry()` / `set_allow_passthrough()`
///   （参见 `main.rs` 的启动接线）。router 建好之后再装，已经在途的请求
///   可能用的还是旧表。
/// - **不装也不会 panic**：holder 的初值就是 `ModelRegistry::builtin()`，
///   未初始化时行为等同于改造前的硬编码模型表。
#[allow(dead_code)]
pub fn create_router_with_provider(
    kiro_provider: Option<std::sync::Arc<KiroProvider>>,
    extract_thinking: bool,
    tool_compatibility_mode: ToolCompatibilityMode,
) -> Router {
    create_router(
        kiro_provider,
        extract_thinking,
        tool_compatibility_mode,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
    )
}

/// 创建 Anthropic API 路由（供 main.rs 使用）
#[allow(clippy::too_many_arguments)]
pub fn create_router(
    kiro_provider: Option<std::sync::Arc<KiroProvider>>,
    extract_thinking: bool,
    tool_compatibility_mode: ToolCompatibilityMode,
    client_keys: Option<SharedClientKeyManager>,
    usage_store: Option<SharedUsageStore>,
    cache_meter: Option<SharedCacheMeter>,
    trace_store: Option<SharedTraceStore>,
    request_body_store: Option<std::sync::Arc<crate::admin::request_body_store::RequestBodyStore>>,
    thinking_text_store: Option<std::sync::Arc<crate::admin::request_body_store::RequestBodyStore>>,
    dispatcher: Option<std::sync::Arc<crate::kiro::dispatch::GroupDispatcher>>,
) -> Router {
    let mut state = AppState::new(extract_thinking, tool_compatibility_mode);
    if let Some(provider) = kiro_provider {
        state = state.with_kiro_provider(provider);
    }
    state = state.with_usage(client_keys, usage_store);
    state = state.with_cache_meter(cache_meter);
    state = state.with_trace_store(trace_store);
    state = state.with_dispatcher(dispatcher);
    state.request_body_store = request_body_store;
    state.thinking_text_store = thinking_text_store;

    // 需要认证的 /v1 路由
    // 请求体保留启用时才挂原始字节捕获层（层本身有缓冲成本，不白挂）
    let capture = state
        .request_body_store
        .as_ref()
        .map(|s| s.is_enabled())
        .unwrap_or(false);
    let messages_route = if capture {
        post(post_messages).layer(middleware::from_fn(super::middleware::capture_raw_body))
    } else {
        post(post_messages)
    };
    let messages_cc_route = if capture {
        post(post_messages_cc).layer(middleware::from_fn(super::middleware::capture_raw_body))
    } else {
        post(post_messages_cc)
    };
    let v1_routes = Router::new()
        .route("/models", get(get_models))
        .route("/messages", messages_route)
        .route("/messages/count_tokens", post(count_tokens))
        .route("/chat/completions", post(post_chat_completions))
        .route("/responses", post(post_responses))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    // 需要认证的 /cc/v1 路由（Claude Code 兼容端点）
    // 与 /v1 的区别：流式响应会等待 contextUsageEvent 后再发送 message_start
    let cc_v1_routes = Router::new()
        .route("/messages", messages_cc_route)
        .route("/messages/count_tokens", post(count_tokens))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            auth_middleware,
        ));

    Router::new()
        .nest("/v1", v1_routes)
        .nest("/cc/v1", cc_v1_routes)
        .layer(cors_layer())
        .layer(DefaultBodyLimit::max(MAX_BODY_SIZE))
        .with_state(state)
}
