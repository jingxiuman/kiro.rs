//! 按凭证并发门禁（借鉴 sub2api 的 AccountWaitPlan）
//!
//! 目标：降低单凭证触发上游 suspicious-activity 风控 429 的概率。
//! 凭证并发满时**排队等待原凭证**（上限 `queue_depth`、超时 `timeout`），
//! 而不是立即切换凭证放大请求分散度；排队失败由调用方（provider 重试环）
//! 决定切换或兜底放行。
//!
//! 语义边界：门禁只管「并发满」；429 风控冷却/故障转移语义不在此处、保持不变。
//! `max_concurrent == 0` 表示禁用，所有请求直接放行（零开销路径）。

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// 一次门禁获取的结果
pub enum GateOutcome {
    /// 门禁未启用（max_concurrent == 0），直接放行
    Disabled,
    /// 获取成功；permit 存活期间占用该凭证一个并发额度，
    /// `waited` 是排队耗时（快速路径为 0）
    Acquired {
        permit: OwnedSemaphorePermit,
        waited: Duration,
    },
    /// 等待队列已满，未入队
    QueueFull,
    /// 入队后等待超时；`waited` 为实际等待时长（≈配置的 timeout）
    Timeout { waited: Duration },
}

/// 每凭证一个信号量 + 等待计数
struct Gate {
    sem: Arc<Semaphore>,
    waiting: AtomicUsize,
}

/// 全部凭证的并发门禁。挂在 KiroProvider 上，进程内单例。
pub struct CredentialGates {
    max_concurrent: usize,
    queue_depth: usize,
    timeout: Duration,
    gates: Mutex<HashMap<u64, Arc<Gate>>>,
}

impl CredentialGates {
    pub fn new(max_concurrent: usize, queue_depth: usize, timeout: Duration) -> Self {
        Self {
            max_concurrent,
            queue_depth,
            timeout,
            gates: Mutex::new(HashMap::new()),
        }
    }

    /// 获取凭证 `id` 的一个并发额度。
    ///
    /// 快速路径 try_acquire 不排队；满载时若等待人数已达 `queue_depth` 返回
    /// [`GateOutcome::QueueFull`]，否则入队最多等 `timeout`。
    pub async fn acquire(&self, id: u64) -> GateOutcome {
        if self.max_concurrent == 0 {
            return GateOutcome::Disabled;
        }
        let gate = {
            let mut map = self.gates.lock();
            map.entry(id)
                .or_insert_with(|| {
                    Arc::new(Gate {
                        sem: Arc::new(Semaphore::new(self.max_concurrent)),
                        waiting: AtomicUsize::new(0),
                    })
                })
                .clone()
        };
        // 快速路径：有空余额度直接拿，不排队。
        if let Ok(permit) = gate.sem.clone().try_acquire_owned() {
            return GateOutcome::Acquired {
                permit,
                waited: Duration::ZERO,
            };
        }
        // 满载：先占队位再等；fetch_add 返回旧值，旧值已达上限说明队满。
        if gate.waiting.fetch_add(1, Ordering::AcqRel) >= self.queue_depth {
            gate.waiting.fetch_sub(1, Ordering::AcqRel);
            return GateOutcome::QueueFull;
        }
        let started = std::time::Instant::now();
        let result = tokio::time::timeout(self.timeout, gate.sem.clone().acquire_owned()).await;
        gate.waiting.fetch_sub(1, Ordering::AcqRel);
        match result {
            Ok(Ok(permit)) => GateOutcome::Acquired {
                permit,
                waited: started.elapsed(),
            },
            // Semaphore 永不 close；防御性处理为放行语义
            Ok(Err(_closed)) => GateOutcome::Disabled,
            Err(_elapsed) => GateOutcome::Timeout {
                waited: started.elapsed(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn gates(cap: usize, depth: usize, timeout_ms: u64) -> Arc<CredentialGates> {
        Arc::new(CredentialGates::new(
            cap,
            depth,
            Duration::from_millis(timeout_ms),
        ))
    }

    #[tokio::test]
    async fn disabled_when_cap_zero() {
        let g = gates(0, 3, 100);
        assert!(matches!(g.acquire(1).await, GateOutcome::Disabled));
    }

    #[tokio::test]
    async fn acquire_within_cap_is_immediate() {
        let g = gates(2, 3, 100);
        let a = g.acquire(1).await;
        let b = g.acquire(1).await;
        assert!(matches!(a, GateOutcome::Acquired { .. }));
        assert!(matches!(b, GateOutcome::Acquired { .. }));
        // 不同凭证互不影响
        assert!(matches!(g.acquire(2).await, GateOutcome::Acquired { .. }));
    }

    #[tokio::test]
    async fn waiter_resolves_when_permit_released() {
        let g = gates(1, 3, 5_000);
        let first = g.acquire(1).await;
        let GateOutcome::Acquired { permit, .. } = first else {
            panic!("first acquire should succeed");
        };
        let g2 = g.clone();
        let waiter = tokio::spawn(async move { g2.acquire(1).await });
        // 让 waiter 进入等待后释放
        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(permit);
        let out = waiter.await.unwrap();
        assert!(
            matches!(out, GateOutcome::Acquired { .. }),
            "释放后排队者应获得额度"
        );
    }

    #[tokio::test]
    async fn queue_full_rejected_immediately() {
        let g = gates(1, 1, 5_000);
        let GateOutcome::Acquired { permit: _hold, .. } = g.acquire(1).await else {
            panic!("first acquire should succeed");
        };
        // 占满唯一队位
        let g2 = g.clone();
        let _waiter = tokio::spawn(async move { g2.acquire(1).await });
        tokio::time::sleep(Duration::from_millis(50)).await;
        // 第三个请求：队满，应立即拒绝而非等待
        let started = Instant::now();
        let out = g.acquire(1).await;
        assert!(matches!(out, GateOutcome::QueueFull), "队满应拒绝入队");
        assert!(
            started.elapsed() < Duration::from_millis(500),
            "QueueFull 应立即返回，不应等待超时"
        );
    }

    #[tokio::test]
    async fn wait_timeout_returns_timeout() {
        let g = gates(1, 3, 100);
        let GateOutcome::Acquired { permit: _hold, .. } = g.acquire(1).await else {
            panic!("first acquire should succeed");
        };
        let started = Instant::now();
        let out = g.acquire(1).await;
        assert!(
            matches!(out, GateOutcome::Timeout { .. }),
            "超时应返回 Timeout"
        );
        assert!(started.elapsed() >= Duration::from_millis(90));
    }
}
