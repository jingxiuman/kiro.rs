//! Anthropic API 中间件

use std::sync::Arc;

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode},
    middleware::Next,
    response::{IntoResponse, Json, Response},
};

use crate::admin::client_keys::{KeyAuth, SharedClientKeyManager};
use crate::admin::trace_db::{SharedTraceStore, TraceKeySource};
use crate::admin::usage_store::SharedUsageStore;
use crate::common::auth;
use crate::kiro::provider::KiroProvider;

use super::cache_metering::SharedCacheMeter;
use super::types::ErrorResponse;

/// 命中的鉴权上下文（注入到请求扩展，供 handler 记录用量）
#[derive(Clone, Debug)]
pub struct KeyContext {
    /// 命中的客户端 Key id
    pub key_id: u64,
    /// 该 Key 绑定的账号分组；None 表示未绑定，可使用全部账号
    pub group: Option<String>,
    /// 命中的入口 Key 类型。
    pub key_source: TraceKeySource,
}

/// 应用共享状态
#[derive(Clone)]
pub struct AppState {
    /// Kiro Provider（可选，用于实际 API 调用）
    /// 内部使用 MultiTokenManager，已支持线程安全的多凭据管理
    pub kiro_provider: Option<Arc<KiroProvider>>,
    /// 是否开启非流式响应的 thinking 块提取
    pub extract_thinking: bool,
    /// 工具兼容模式（ClaudeCode 内置工具名/入参双向适配 / Raw 透传）
    pub tool_compatibility_mode: crate::model::config::ToolCompatibilityMode,
    /// 客户端 Key 管理器（可选，未启用 Admin 时为 None）
    pub client_keys: Option<SharedClientKeyManager>,
    /// 用量存储（DuckDB：写入 + 统计查询）
    pub usage_store: Option<SharedUsageStore>,
    /// 中转层缓存计量（基于 cache_control 断点的内存缓存）
    pub cache_meter: Option<SharedCacheMeter>,
    /// 请求链路追踪存储（SQLite，可选）
    pub trace_store: Option<SharedTraceStore>,
    /// 请求体全量保留存储（可选，storeRequestBodies=true 时启用）
    pub request_body_store: Option<std::sync::Arc<crate::admin::request_body_store::RequestBodyStore>>,
    /// omitted 思考正文存储（恢复键 kiro-thinking-v1 的后端，常开）
    pub thinking_text_store: Option<std::sync::Arc<crate::admin::request_body_store::RequestBodyStore>>,
    /// 上游侧字节存储（可选，storeUpstreamBodies=true 时启用）：
    /// 发往 Kiro 的请求体与上游原始响应字节
    pub upstream_body_store: Option<std::sync::Arc<crate::admin::request_body_store::RequestBodyStore>>,
    /// weighted 模式的组内选号 dispatcher（可选，未启用 weighted 时为 None）。
    /// 供消耗回写等下游任务从 `state.dispatcher` 取句柄，与 `token_manager`
    /// 内部持有的是同一个 `Arc`（由 main.rs 双向注入）。
    pub dispatcher: Option<std::sync::Arc<crate::kiro::dispatch::GroupDispatcher>>,
}

/// 由 [`capture_raw_body`] 塞进 request extensions 的原始请求体字节。
/// Bytes 引用计数克隆，无拷贝开销。
#[derive(Clone)]
pub struct RawRequestBody(pub bytes::Bytes);

/// 缓冲请求体并把原始字节存入 extensions，供 handler 落盘请求体。
/// 仅在 request_body_store 启用时挂载本层；Json 解析语义不受影响。
pub async fn capture_raw_body(
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let (parts, body) = req.into_parts();
    match axum::body::to_bytes(body, 50 * 1024 * 1024).await {
        Ok(bytes) => {
            let mut req = axum::extract::Request::from_parts(parts, axum::body::Body::from(bytes.clone()));
            req.extensions_mut().insert(RawRequestBody(bytes));
            next.run(req).await
        }
        Err(e) => {
            // 体积超限等：交回一个空体请求让下游 Json 提取器报它自己的错，
            // 观测层不改变错误语义。
            tracing::warn!("请求体缓冲失败: {}", e);
            let req = axum::extract::Request::from_parts(parts, axum::body::Body::empty());
            next.run(req).await
        }
    }
}

impl AppState {
    /// 创建新的应用状态（不含 client_keys 的基础构造，供嵌入 / 测试使用）
    #[allow(dead_code)]
    pub fn new(
        extract_thinking: bool,
        tool_compatibility_mode: crate::model::config::ToolCompatibilityMode,
    ) -> Self {
        Self {
            kiro_provider: None,
            extract_thinking,
            tool_compatibility_mode,
            client_keys: None,
            usage_store: None,
            cache_meter: None,
            trace_store: None,
            request_body_store: None,
            thinking_text_store: None,
            upstream_body_store: None,
            dispatcher: None,
        }
    }

    /// 设置 KiroProvider
    ///
    /// 收 `Arc` 而不是按值 move：Admin 的 `POST /models/test` 要与 `/v1/messages`
    /// 共用同一个 provider 实例（同一份账号池 / 代理 / client 缓存），否则测出来的
    /// 不是生产链路实际会发生的事。
    pub fn with_kiro_provider(mut self, provider: Arc<KiroProvider>) -> Self {
        self.kiro_provider = Some(provider);
        self
    }

    /// 注入用量记录组件
    pub fn with_usage(
        mut self,
        client_keys: Option<SharedClientKeyManager>,
        usage_store: Option<SharedUsageStore>,
    ) -> Self {
        self.client_keys = client_keys;
        self.usage_store = usage_store;
        self
    }

    /// 注入缓存计量器
    pub fn with_cache_meter(mut self, cache: Option<SharedCacheMeter>) -> Self {
        self.cache_meter = cache;
        self
    }

    /// 注入链路追踪存储
    pub fn with_trace_store(mut self, store: Option<SharedTraceStore>) -> Self {
        self.trace_store = store;
        self
    }

    /// 注入 weighted 模式的选号 dispatcher
    pub fn with_dispatcher(mut self, dispatcher: Option<Arc<crate::kiro::dispatch::GroupDispatcher>>) -> Self {
        self.dispatcher = dispatcher;
        self
    }
}

/// 累计 credit 上限的豁免端点：模型列表与 token 计数不产生 credit。
///
/// 挡住它们不会省下任何额度，只会让客户端表现成「连不上」而不是「额度用完」，
/// 把排查方向带偏。`ends_with` 同时覆盖 `/v1` 与 `/cc/v1` 两套前缀。
fn credit_limit_exempt(path: &str) -> bool {
    path.ends_with("/models") || path.ends_with("/messages/count_tokens")
}

/// API Key 认证中间件
///
/// 所有入口 Key 统一按已存储的完整值精确匹配，不限制前缀。命中后向请求扩展注入
/// [`KeyContext`]，供 handler 记录用量时使用。
pub async fn auth_middleware(
    State(state): State<AppState>,
    mut request: Request<Body>,
    next: Next,
) -> Response {
    let presented = match auth::extract_api_key(&request) {
        Some(k) => k,
        None => {
            let error = ErrorResponse::authentication_error();
            return (StatusCode::UNAUTHORIZED, Json(error)).into_response();
        }
    };

    if let Some(mgr) = &state.client_keys {
        let exempt = credit_limit_exempt(request.uri().path());
        match mgr.verify_and_touch(&presented) {
            KeyAuth::Granted(id) => {
                let group = mgr.group_of(id);
                request.extensions_mut().insert(KeyContext {
                    key_id: id,
                    group,
                    key_source: TraceKeySource::ClientKey,
                });
                return next.run(request).await;
            }
            // 超限：在 `next.run` 之前返回，用量记录、链路追踪、调度器消耗回写
            // 全部在 handler 内构造，因此天然一个都不会执行——这正是把闸设在
            // 中间件而不是 handler 的原因。计次则由 `verify_and_touch` 保证不发生。
            KeyAuth::Exhausted(id) if !exempt => {
                tracing::info!(key_id = id, "客户端 Key 累计 credit 已达上限，拒绝计费请求");
                return (
                    StatusCode::TOO_MANY_REQUESTS,
                    Json(ErrorResponse::new(
                        "rate_limit_error",
                        "Client key credit limit reached. Raise or clear maxCredits, or reset the key's stats.",
                    )),
                )
                    .into_response();
            }
            KeyAuth::Exhausted(id) => {
                let group = mgr.group_of(id);
                request.extensions_mut().insert(KeyContext {
                    key_id: id,
                    group,
                    key_source: TraceKeySource::ClientKey,
                });
                return next.run(request).await;
            }
            KeyAuth::Rejected => {}
        }
    }

    let error = ErrorResponse::authentication_error();
    (StatusCode::UNAUTHORIZED, Json(error)).into_response()
}

/// CORS 中间件层
///
/// **安全说明**：当前配置允许所有来源（Any），这是为了支持公开 API 服务。
/// 如果需要更严格的安全控制，请根据实际需求配置具体的允许来源、方法和头信息。
///
/// # 配置说明
/// - `allow_origin(Any)`: 允许任何来源的请求
/// - `allow_methods(Any)`: 允许任何 HTTP 方法
/// - `allow_headers(Any)`: 允许任何请求头
pub fn cors_layer() -> tower_http::cors::CorsLayer {
    use tower_http::cors::{Any, CorsLayer};

    CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::admin::client_keys::ClientKeyManager;
    use crate::model::config::ToolCompatibilityMode;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    /// 造一个只挂了客户端 Key 管理器的最小路由（无上游 provider / usage / trace）。
    fn router_with(mgr: Arc<ClientKeyManager>) -> axum::Router {
        super::super::router::create_router(
            None,
            false,
            ToolCompatibilityMode::default(),
            Some(mgr),
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
    }

    fn exhausted_manager() -> (Arc<ClientKeyManager>, String) {
        let mgr = Arc::new(ClientKeyManager::new());
        let entry = mgr.create("capped".to_string(), None, None);
        assert!(mgr.set_max_credits(entry.id, Some(5.0)));
        mgr.record_usage(entry.id, 0, 0, 0, 0, 5.0);
        (mgr, entry.key)
    }

    /// 超限的 Key 打计费端点必须拿到 429（而不是 401——那会把「额度用完」
    /// 误导成「密钥无效」，排查方向直接跑偏）。
    #[tokio::test]
    async fn exhausted_key_gets_429_on_messages() {
        let (mgr, key) = exhausted_manager();
        let resp = router_with(mgr)
            .oneshot(
                HttpRequest::builder()
                    .method("POST")
                    .uri("/v1/messages")
                    .header("x-api-key", &key)
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"model":"auto","max_tokens":16,"messages":[]}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS);
    }

    /// 不计费的辅助端点在超限时仍须服务：挡住它们只会让客户端表现成「连不上」，
    /// 而不是「额度用完」。
    #[tokio::test]
    async fn exhausted_key_still_serves_models() {
        let (mgr, key) = exhausted_manager();
        let resp = router_with(mgr)
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/v1/models")
                    .header("x-api-key", &key)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    /// 未超限的 Key 不受影响：没有这条，上面两条可以靠「永远 429 / 永远放行」通过。
    #[tokio::test]
    async fn key_under_limit_is_not_rate_limited() {
        let mgr = Arc::new(ClientKeyManager::new());
        let entry = mgr.create("normal".to_string(), None, None);
        let resp = router_with(mgr)
            .oneshot(
                HttpRequest::builder()
                    .method("GET")
                    .uri("/v1/models")
                    .header("x-api-key", &entry.key)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
