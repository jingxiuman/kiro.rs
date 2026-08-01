mod admin;
mod admin_ui;
mod anthropic;
mod common;
mod http_client;
mod image_resize;
mod kiro;
mod model;
pub mod token;

use std::collections::HashMap;
use std::sync::Arc;

use clap::Parser;
use kiro::endpoint::{CliEndpoint, IdeEndpoint, KiroEndpoint};
use kiro::model::credentials::{CredentialsConfig, KiroCredentials};
use kiro::provider::KiroProvider;
use kiro::token_manager::MultiTokenManager;
use model::arg::Args;
use model::config::Config;

#[tokio::main]
async fn main() {
    // 解析命令行参数
    let args = Args::parse();

    // 初始化日志
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // 解析配置/凭证路径
    let config_path = args
        .config
        .unwrap_or_else(|| Config::default_config_path().to_string());
    let credentials_path = args
        .credentials
        .unwrap_or_else(|| KiroCredentials::default_credentials_path().to_string());

    // 文件不存在时自动初始化（Docker 首次部署友好）
    ensure_config_files(&config_path, &credentials_path);

    // 加载配置
    let config = Config::load(&config_path).unwrap_or_else(|e| {
        tracing::error!("加载配置失败: {}", e);
        std::process::exit(1);
    });

    // 加载凭证（支持单对象或数组格式）
    let credentials_config = CredentialsConfig::load(&credentials_path).unwrap_or_else(|e| {
        tracing::error!("加载凭证失败: {}", e);
        std::process::exit(1);
    });

    // 判断是否为多凭据格式（用于刷新后回写）
    let is_multiple_format = credentials_config.is_multiple();

    // 转换为按优先级排序的凭据列表
    let mut credentials_list = credentials_config.into_sorted_credentials();

    // 检查 KIRO_API_KEY 环境变量，自动创建 API Key 凭据
    if let Ok(kiro_api_key) = std::env::var("KIRO_API_KEY") {
        if kiro_api_key.is_empty() {
            tracing::warn!("KIRO_API_KEY 环境变量已设置但为空，视为未配置");
        } else {
            tracing::info!("检测到 KIRO_API_KEY 环境变量，添加 API Key 凭据（最高优先级）");
            let api_key_cred = KiroCredentials {
                kiro_api_key: Some(kiro_api_key),
                auth_method: Some("api_key".to_string()),
                priority: 0,
                ..Default::default()
            };
            credentials_list.insert(0, api_key_cred);
        }
    }

    tracing::info!("已加载 {} 个凭据配置", credentials_list.len());

    // 仅显示安全的元数据，避免在日志里泄露 token / client_secret
    let first_credentials = credentials_list.first().cloned().unwrap_or_default();
    tracing::debug!(
        id = ?first_credentials.id,
        email = ?first_credentials.email,
        auth_method = ?first_credentials.auth_method,
        priority = first_credentials.priority,
        endpoint = ?first_credentials.endpoint,
        "已选定主凭证"
    );

    let configured_api_key = config.api_key.clone().filter(|k| !k.trim().is_empty());

    // 构建代理配置
    let proxy_config = config.proxy_url.as_ref().map(|url| {
        let mut proxy = http_client::ProxyConfig::new(url);
        if let (Some(username), Some(password)) = (&config.proxy_username, &config.proxy_password) {
            proxy = proxy.with_auth(username, password);
        }
        proxy
    });

    if proxy_config.is_some() {
        tracing::info!("已配置 HTTP 代理: {}", config.proxy_url.as_ref().unwrap());
    }

    // 出网策略：必须在任何 build_client 之前装配，否则预热的 client 会绕过检查
    http_client::set_require_proxy(config.require_proxy);
    if config.require_proxy {
        tracing::info!("已开启 requireProxy：无可用代理时拒绝出网，不降级直连");
    }

    // 启动 Kiro IDE 版本自动获取：从官方元数据端点拉取 currentRelease，
    // 用于流式端点 User-Agent（替代写死的版本号）；失败时回退 config.kiroVersion。
    kiro::kiro_version::spawn_refresher(
        proxy_config.clone(),
        config.tls_backend,
        std::time::Duration::from_secs(12 * 3600),
    );

    // 构建端点注册表
    let mut endpoints: HashMap<String, Arc<dyn KiroEndpoint>> = HashMap::new();
    {
        let ide = IdeEndpoint::new();
        endpoints.insert(ide.name().to_string(), Arc::new(ide));
        let cli = CliEndpoint::new();
        endpoints.insert(cli.name().to_string(), Arc::new(cli));
    }

    // 校验默认端点存在
    if !endpoints.contains_key(&config.default_endpoint) {
        tracing::error!("默认端点 \"{}\" 未注册", config.default_endpoint);
        std::process::exit(1);
    }

    // 校验所有凭据声明的端点都已注册
    for cred in &credentials_list {
        let name = cred.endpoint.as_deref().unwrap_or(&config.default_endpoint);
        if !endpoints.contains_key(name) {
            tracing::error!(
                "凭据 id={:?} 指定了未知端点 \"{}\"（已注册: {:?}）",
                cred.id,
                name,
                endpoints.keys().collect::<Vec<_>>()
            );
            std::process::exit(1);
        }
    }

    let endpoint_names: Vec<String> = endpoints.keys().cloned().collect();

    // 创建 MultiTokenManager 和 KiroProvider
    let token_manager = MultiTokenManager::new(
        config.clone(),
        credentials_list,
        proxy_config.clone(),
        Some(credentials_path.into()),
        is_multiple_format,
    )
    .unwrap_or_else(|e| {
        tracing::error!("创建 Token 管理器失败: {}", e);
        std::process::exit(1);
    });
    let token_manager = Arc::new(token_manager);

    // 代理池提前到 admin 分支之外创建：请求级反馈（网络错误/流中断 → 自动禁用+换绑）
    // 不依赖 admin 是否启用；AdminService 与 KiroProvider 共享同一实例。
    let proxy_pool_path = token_manager.cache_dir().map(|d| d.join("proxy_pool.json"));
    let proxy_pool = Arc::new(admin::proxy_pool::ProxyPoolManager::new(
        proxy_pool_path,
        config.tls_backend,
    ));

    // traces.db 路径。trace 与 ops 两个存储共用此文件（各持独立连接，WAL 并发安全），
    // 且必须共享同一「持久化 / 内存兜底」决策：否则会出现「运维页有历史统计、
    // 请求日志页却为空」的错位。因此先定 trace_store，再据其结果建 ops_store。
    let traces_db_path = token_manager
        .cache_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."))
        .join("traces.db");

    // 请求链路追踪存储（SQLite，traces.db）。失败不致命：trace 不可用但服务正常。
    let trace_store: Option<admin::SharedTraceStore> = match admin::TraceStore::open(
        traces_db_path.clone(),
        config.trace_enabled,
        config.trace_retention_days,
    ) {
        Ok(s) => Some(std::sync::Arc::new(s)),
        Err(e) => {
            tracing::warn!("打开 traces.db 失败，请求链路追踪不可用: {}", e);
            None
        }
    };

    // Ops 事件存储：trace 主库成功时用同一持久化文件；trace 降级到内存时 ops 也用内存，
    // 保证两者查询落在同一数据库视图。
    let ops_store = Arc::new(if trace_store.is_some() {
        admin::OpsStore::open(traces_db_path.clone()).unwrap_or_else(|e| {
            tracing::warn!("打开 ops 存储失败，处置事件仅进程内可见: {}", e);
            admin::OpsStore::open_in_memory().expect("内存 ops 存储初始化失败")
        })
    } else {
        admin::OpsStore::open_in_memory().expect("内存 ops 存储初始化失败")
    });
    let ops_runtime = Arc::new(admin::OpsRuntime::new(
        proxy_pool.clone(),
        token_manager.clone(),
        ops_store.clone(),
    ));

    // Arc 共享：Anthropic 路由与 AdminService 必须拿到**同一个** provider 实例，
    // 否则 Admin 的 POST /models/test 测的就不是生产链路（另起一个 provider 会换掉
    // client 缓存，账号池状态也不同源）。
    let kiro_provider = Arc::new(
        KiroProvider::with_proxy(
            token_manager.clone(),
            proxy_config.clone(),
            endpoints,
            config.default_endpoint.clone(),
        )
        .with_ops(ops_runtime.clone()),
    );

    // 初始化 count_tokens 配置
    token::init_config(token::CountTokensConfig {
        api_url: config.count_tokens_api_url.clone(),
        api_key: config.count_tokens_api_key.clone(),
        auth_type: config.count_tokens_auth_type.clone(),
        proxy: proxy_config,
        tls_backend: config.tls_backend,
    });

    // 客户端 Key 管理器 + 用量记录器 + 聚合器（与凭据文件同目录）
    let cache_dir = token_manager
        .cache_dir()
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    let client_keys_path = admin::client_keys::default_path_in(&cache_dir);
    let client_key_manager = std::sync::Arc::new(
        admin::ClientKeyManager::load(&client_keys_path).unwrap_or_else(|e| {
            tracing::warn!("加载客户端 Key 失败 ({}): {}", client_keys_path.display(), e);
            admin::ClientKeyManager::new()
        }),
    );
    // 用量存储（kiro.duckdb）。与 trace/ops 共用同一 DuckDB 文件；起不来 fail-fast——
    // 它同时承载写入与统计端点，静默降级会让「服务在跑但统计悄悄丢失」。
    let duckdb_path = cache_dir.join("kiro.duckdb");
    let usage_store = std::sync::Arc::new(
        admin::UsageStore::open(&duckdb_path, config.usage_log_retention_days as i64)
            .unwrap_or_else(|e| panic!("打开 {} 失败: {}", duckdb_path.display(), e)),
    );
    // 旧版 usage_log.*.jsonl 一次性导入（幂等，导入后原文件改名 .imported 归档）
    let imported = usage_store.import_legacy_jsonl(&cache_dir);
    if imported > 0 {
        tracing::info!("历史 usage_log JSONL 导入完成: {} 行", imported);
    }

    // 账号分组注册表（持久化到 groups.json）。
    // 启动时若文件不存在则首次创建，并把现有凭据 / 客户端 Key 的 groups 字段反向迁移进去，
    // 保证老用户升级后所有已用分组都自动注册，不会因为本次改造而消失。
    let groups_path = admin::groups::default_path_in(&cache_dir);
    let group_manager = std::sync::Arc::new(
        admin::GroupManager::load(&groups_path).unwrap_or_else(|e| {
            tracing::warn!("加载分组注册表失败 ({}): {}", groups_path.display(), e);
            admin::GroupManager::new()
        }),
    );
    {
        let mut all_used: Vec<String> = token_manager.list_credential_groups();
        all_used.extend(client_key_manager.used_group_names());
        let added = group_manager.bootstrap_from_existing(all_used);
        if added > 0 {
            tracing::info!("分组注册表：自动迁移 {} 个已用分组", added);
        }
    }

    // 启动后定期清理过期 usage_log 与 trace / ops 事件记录
    {
        let recorder = usage_store.clone();
        let trace_store = trace_store.clone();
        let ops_store = ops_store.clone();
        tokio::spawn(async move {
            let day = std::time::Duration::from_secs(24 * 3600);
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            loop {
                recorder.cleanup_old_logs();
                if let Some(ts) = &trace_store {
                    ts.cleanup();
                    ops_store.cleanup(ts.retention_days());
                }
                tokio::time::sleep(day).await;
            }
        });
    }

    if let Some(initial_key) = configured_api_key.as_ref() {
        client_key_manager.sync_system_key(
            "默认密钥".to_string(),
            Some("由 config.json apiKey 自动同步（系统密钥）".to_string()),
            initial_key.clone(),
        );
    }

    // CacheMeter：模拟 Anthropic 缓存、计量 cache_read/creation token 的进程内组件。
    // 持久化到 cache_dir/cache_metering.json，启动时自动加载未过期条目。
    let cache_meter = std::sync::Arc::new(anthropic::cache_metering::CacheMeter::new(Some(
        cache_dir.join("cache_metering.json"),
    )));
    cache_meter.clone().spawn_background();

    // ---- 模型注册表：必须在建 router 之前装好 ----
    // 位置：凭据目录（与既有 registry / cache 同级，spec §4.2）。
    // 放在 admin 分支之外：AdminService 仅在 adminApiKey 非空时创建，
    // 挂在其内会让未配管理密钥的部署既没有模型表也没有自动同步（spec §3.2 / §6.1）。
    let model_store = std::sync::Arc::new(
        anthropic::model_registry_store::ModelRegistryStore::new(cache_dir.join("models.json")),
    );
    // 同步运行时配置的唯一 holder：调度器（本分支）与 AdminService（admin 分支）共用同一份，
    // 这样 PATCH /models/settings 才能热生效，不必重启。
    let model_sync_settings = std::sync::Arc::new(parking_lot::RwLock::new(
        admin::ModelSyncSettings::from_config(&config),
    ));
    // 供下方 customModels 导入判断降级态用；outcome 本身作用域到本块结束。
    let model_registry_degraded_reason: Option<String>;
    {
        let outcome = model_store.load();
        if let Some(reason) = &outcome.degraded_reason {
            tracing::error!("模型表降级运行（使用内置默认）: {}", reason);
        }
        model_registry_degraded_reason = outcome.degraded_reason.clone();
        // 调度层的凭据过滤靠这份记录；不灌就永远走「无记录 → 放行」，等于没生效。
        token_manager.set_credential_support(outcome.file.credential_support);
        anthropic::model_registry::install_registry(outcome.registry);
        anthropic::model_registry::set_allow_passthrough(
            model_sync_settings.read().allow_passthrough,
        );
        // 回读而非回显入参：证明确实写进了运行时状态，而不是"调用过写入口"。
        tracing::info!(
            "模型表已装载: {} 行有效模型, credentialSupport 覆盖 {} 个凭据, 未知模型透传={}",
            anthropic::model_registry::current_registry().rows().len(),
            token_manager.credential_support().len(),
            anthropic::model_registry::allow_passthrough()
        );
    }

    // ---- 旧版 customModels → 模型注册表的启动时一次性导入 ----
    // 决策：不保留 PR #46 引入的独立 customModels 运行时映射机制（已被模型
    // 注册表整体取代）；customModels 字段仅用于兼容老配置文件，在这里被消费
    // 一次，转换成 Manual 行写入 models.json，此后完全由注册表接管。
    // 每次启动都跑这一步，但只在「注册表里确实还没有对应行」时才真正写盘，
    // 因此天然幂等：已导入过的条目下次会在 plan_import 里全部落进 skipped。
    if !config.custom_models.is_empty()
        && model::custom_models_import::should_skip_import_when_degraded(
            &model_registry_degraded_reason,
        )
    {
        tracing::warn!(
            "模型表当前处于降级态（{}），跳过本轮 customModels 导入——降级态下 \
             effective_rows 只是内置默认表，据此判重不可信，等 models.json 修好后再启动一次即可",
            model_registry_degraded_reason.as_deref().unwrap_or("未知原因")
        );
    } else if !config.custom_models.is_empty() {
        let effective_rows: Vec<_> =
            anthropic::model_registry::current_registry().rows().to_vec();
        let plan = model::custom_models_import::plan_import(
            &config.custom_models,
            &effective_rows,
            chrono::Utc::now(),
        );
        for skipped in &plan.skipped {
            tracing::info!(
                "customModels 条目已跳过（不覆盖模型注册表）: id={}, backendId={}, 原因={}",
                skipped.id,
                skipped.backend_id,
                skipped.reason
            );
        }
        if !plan.rows_to_add.is_empty() {
            let added_count = plan.rows_to_add.len();
            match model_store
                .mutate(|file| {
                    file.models.extend(plan.rows_to_add.clone());
                    Ok(())
                })
                .await
            {
                Ok(file) => match anthropic::model_registry::ModelRegistry::from_file(file) {
                    Ok(registry) => {
                        anthropic::model_registry::install_registry(registry);
                        tracing::info!(
                            "customModels 已导入模型注册表（新增 {} 行）；建议迁移到 admin UI 管理，\
                             该配置项后续版本可能移除",
                            added_count
                        );
                    }
                    Err(e) => {
                        tracing::error!("customModels 导入后重新装载模型表失败: {}，本次导入不生效", e);
                    }
                },
                Err(e) => {
                    tracing::error!("customModels 导入写入 models.json 失败: {}", e);
                }
            }
        } else {
            tracing::info!("customModels 已全部存在于模型注册表，无需导入");
        }
    }

    // 同步服务：定时调度器与 admin 的手动触发端点共用同一个实例（同一把写锁串行化）。
    let model_sync_service = std::sync::Arc::new(anthropic::model_sync::ModelSyncService::new(
        model_store.clone(),
        token_manager.clone() as std::sync::Arc<dyn anthropic::model_sync::ModelListFetcher>,
    ));
    spawn_model_sync_scheduler(
        model_sync_service.clone(),
        model_store.clone(),
        token_manager.clone(),
        model_sync_settings.clone(),
    );

    let anthropic_app = anthropic::create_router(
        Some(kiro_provider.clone()),
        config.extract_thinking,
        config.tool_compatibility_mode,
        Some(client_key_manager.clone()),
        Some(usage_store.clone()),
        Some(cache_meter.clone()),
        trace_store.clone(),
    );

    // 构建 Admin API 路由（配置了非空 adminApiKey 时启用）
    // 安全检查：空字符串被视为未配置，防止空 key 绕过认证
    let app = if let Some(admin_key) = &config.admin_api_key {
        if admin_key.trim().is_empty() {
            tracing::warn!("admin_api_key 配置为空，Admin API 未启用");
            anthropic_app
        } else {
            // Admin 查询需要一个确定的 store；traces.db 打开失败时用内存兜底（仅本进程有效）
            let admin_trace_store = trace_store.clone().unwrap_or_else(|| {
                std::sync::Arc::new(
                    admin::TraceStore::open_in_memory()
                        .expect("内存 trace store 初始化失败"),
                )
            });
            let admin_service =
                admin::AdminService::new(token_manager.clone(), endpoint_names.clone(), proxy_pool.clone())
                    .with_ops(ops_runtime.clone())
                    .with_log_governance(
                        Some(admin_trace_store.clone()),
                        Some(usage_store.clone()),
                    )
                    // /models* 全部 7 组端点依赖这两个注入；不注入则返回「未初始化」。
                    .with_model_registry(
                        Some(model_store.clone()),
                        Some(model_sync_service.clone()),
                    )
                    // 与调度器共用 holder，见上面 model_sync_settings 的注释。
                    .with_model_sync_settings(model_sync_settings.clone())
                    // POST /models/test 发真实请求用，与 /v1/messages 同一实例。
                    .with_kiro_provider(kiro_provider.clone());
            let admin_state = admin::AdminState::new(
                admin_key,
                admin_service,
                client_key_manager.clone(),
                usage_store.clone(),
                admin_trace_store,
                group_manager.clone(),
            );

            // 启动余额后台刷新调度器（每 5 分钟一次，与缓存 TTL 对齐）
            admin_state
                .service
                .start_balance_refresher(std::time::Duration::from_secs(300));

            // 启动代理池健康检查调度器（每 5 分钟一次）
            admin_state
                .service
                .start_proxy_health_checker(std::time::Duration::from_secs(300));

            // 启动自动更新调度器：每分钟检查一次本地时间，到达 update_auto_apply_time
            // 且开启 update_auto_apply 时执行一次更新；否则静默等待。
            admin_state.service.start_auto_update_scheduler();

            let admin_app = admin::create_admin_router(admin_state);

            // 创建 Admin UI 路由
            let admin_ui_app = admin_ui::create_admin_ui_router();

            tracing::info!("Admin API 已启用");
            tracing::info!("Admin UI 已启用: /admin");
            anthropic_app
                .nest("/api/admin", admin_app)
                .nest("/admin", admin_ui_app)
        }
    } else {
        anthropic_app
    };

    // 启动服务器
    let addr = format!("{}:{}", config.host, config.port);
    tracing::info!("启动 Anthropic API 端点: {}", addr);
    tracing::info!("可用 API:");
    tracing::info!("  GET  /v1/models");
    tracing::info!("  POST /v1/messages");
    tracing::info!("  POST /v1/messages/count_tokens");
    tracing::info!("Admin API:");
    tracing::info!("  GET  /api/admin/credentials");
    tracing::info!("  POST /api/admin/credentials/:index/disabled");
    tracing::info!("  POST /api/admin/credentials/:index/priority");
    tracing::info!("  POST /api/admin/credentials/:index/reset");
    tracing::info!("  GET  /api/admin/credentials/:index/balance");
    tracing::info!("Admin UI:");
    tracing::info!("  GET  /admin");

    let listener = tokio::net::TcpListener::bind(&addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

/// 每日模型同步调度器（spec §6.1）。
///
/// 三点说明：
/// 1. **建在 admin 分支之外**。`AdminService` 仅在 `adminApiKey` 非空时创建，
///    调度器若挂在其内，没配管理密钥的部署就完全没有自动同步。
/// 2. **无论开关是否打开都启动**，循环内每 30 秒读一次共享 holder。关闭时纯空转，
///    不发任何上游请求、不写任何文件 —— 零行为回归照样成立；打开开关则立即
///    热生效，不必重启。
/// 3. **没有「启动后跑一次」**：那会让首次启动就改写 models.json，与零行为回归矛盾。
fn spawn_model_sync_scheduler(
    service: Arc<anthropic::model_sync::ModelSyncService>,
    store: Arc<anthropic::model_registry_store::ModelRegistryStore>,
    token_manager: Arc<MultiTokenManager>,
    settings: Arc<parking_lot::RwLock<admin::ModelSyncSettings>>,
) {
    use chrono::Timelike;

    tokio::spawn(async move {
        tracing::info!("模型同步调度器已启动（开关状态在每轮循环中读取）");
        // 同一分钟内避免重复触发：记录最近一次跑过的「日期 + 时:分」
        let mut last_run_marker: Option<String> = None;

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(30)).await;

            let current = settings.read().clone();
            if !current.enabled {
                continue;
            }
            let Ok((target_hour, target_minute)) = admin::parse_auto_apply_time(&current.time)
            else {
                tracing::warn!("modelSyncTime 配置无效: {}，跳过本轮检查", current.time);
                continue;
            };

            let now = chrono::Local::now();
            let marker = format!("{}-{:02}:{:02}", now.format("%Y-%m-%d"), now.hour(), now.minute());
            if now.hour() != target_hour || now.minute() != target_minute {
                continue;
            }
            if last_run_marker.as_deref() == Some(marker.as_str()) {
                continue;
            }
            last_run_marker = Some(marker);

            match service.sync_once(current.probe_credential_id, chrono::Utc::now()).await {
                Ok(s) => {
                    tracing::info!(
                        "模型同步完成: 轮次={:?} 新增={} 更新={} 标记deprecated={} 可信={} 来源={}",
                        s.round, s.added, s.updated, s.deprecated, s.trusted, s.source
                    );
                    // 本轮顺带记录的 credentialSupport 要灌回调度层才生效。
                    token_manager.set_credential_support(store.load().file.credential_support);
                }
                Err(e) => tracing::warn!("模型同步跳过: {}", e),
            }
        }
    });
}

/// 文件不存在时初始化配置/凭证文件
///
/// - `config.json`：写入带随机 `apiKey`（每次启动同步为系统 Key）/ `adminApiKey`（管理面板登录密钥）
///   的最小默认配置；`host` 设为 `0.0.0.0` 以适配容器场景，端口/默认端点等其余字段沿用代码默认值。
/// - `credentials.json`：写入空数组 `[]`，便于后续通过 Admin UI 添加凭据。
///
/// 任一步失败都仅打印警告，不中断启动；后续 `Config::load` / `CredentialsConfig::load`
/// 仍会按既有逻辑处理（失败再退出）。
fn ensure_config_files(config_path: &str, credentials_path: &str) {
    let config_p = std::path::Path::new(config_path);
    if !config_p.exists() {
        if let Some(parent) = config_p.parent()
            && !parent.as_os_str().is_empty()
                && let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::warn!("创建配置目录失败 {}: {}", parent.display(), e);
                }
        let api_key = format!("sk-kiro-rs-{}", random_token(24));
        let admin_api_key = format!("sk-admin-{}", random_token(24));
        let default = serde_json::json!({
            "host": "0.0.0.0",
            "port": 8990,
            "apiKey": api_key,
            "adminApiKey": admin_api_key,
            "region": "us-east-1",
            "tlsBackend": "rustls",
            "defaultEndpoint": "ide"
        });
        match serde_json::to_string_pretty(&default)
            .map_err(anyhow::Error::from)
            .and_then(|s| std::fs::write(config_p, s).map_err(anyhow::Error::from))
        {
            Ok(_) => {
                tracing::info!("已生成默认配置: {}", config_p.display());
                tracing::info!("  apiKey      = {}（每次启动时同步为系统 Key）", api_key);
                tracing::info!("  adminApiKey = {}（管理面板登录密钥）", admin_api_key);
                tracing::info!("请妥善保存上述密钥，可在配置文件中修改");
            }
            Err(e) => tracing::warn!("写入默认配置失败 {}: {}", config_p.display(), e),
        }
    }

    let cred_p = std::path::Path::new(credentials_path);
    if !cred_p.exists() {
        if let Some(parent) = cred_p.parent()
            && !parent.as_os_str().is_empty()
                && let Err(e) = std::fs::create_dir_all(parent) {
                    tracing::warn!("创建凭证目录失败 {}: {}", parent.display(), e);
                }
        if let Err(e) = std::fs::write(cred_p, "[]\n") {
            tracing::warn!("写入空凭证文件失败 {}: {}", cred_p.display(), e);
        } else {
            tracing::info!("已生成空凭证文件: {}（可通过 Admin UI 添加凭据）", cred_p.display());
        }
    }
}

/// 生成一段长度为 `len` 的字母数字随机字符串，用于默认 API Key
fn random_token(len: usize) -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    (0..len)
        .map(|_| {
            let idx = fastrand::usize(..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}
