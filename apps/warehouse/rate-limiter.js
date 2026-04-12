/**
 * モール別レート制限・同時実行制御
 * daily-syncとツール経由のAPI呼び出しが競合しないようにする
 */

class Semaphore {
  constructor(max = 1) {
    this.max = max;
    this.current = 0;
    this.queue = [];
  }

  async acquire() {
    if (this.current < this.max) {
      this.current++;
      return;
    }
    return new Promise((resolve) => this.queue.push(resolve));
  }

  release() {
    this.current--;
    if (this.queue.length > 0) {
      this.current++;
      const next = this.queue.shift();
      next();
    }
  }

  get pending() {
    return this.queue.length;
  }

  get active() {
    return this.current;
  }
}

// モール別セマフォ（同時実行数）
const limiters = {
  'sp-api': new Semaphore(1),      // SP-API: 同時1
  'rakuten': new Semaphore(1),     // 楽天: 同時1
  'qoo10': new Semaphore(1),       // Qoo10: 同時1
  'aupay': new Semaphore(1),       // au PAY: 同時1
  'linegift': new Semaphore(1),    // LINEギフト: 同時1
  'ne': new Semaphore(1),          // NE: 同時1
};

/**
 * レート制限付きで関数を実行する
 * @param {string} mall - モール名（'sp-api', 'rakuten' 等）
 * @param {Function} fn - 実行する非同期関数
 * @returns {Promise<any>}
 */
export async function withRateLimit(mall, fn) {
  const sem = limiters[mall];
  if (!sem) throw new Error(`Unknown mall for rate limiting: ${mall}`);

  await sem.acquire();
  try {
    return await fn();
  } finally {
    sem.release();
  }
}

/**
 * レート制限状態を取得（監視用）
 */
export function getRateLimitStatus() {
  const status = {};
  for (const [mall, sem] of Object.entries(limiters)) {
    status[mall] = { active: sem.active, pending: sem.pending, max: sem.max };
  }
  return status;
}

/**
 * Expressミドルウェア: リクエスト単位でレート制限を適用
 * @param {string} mall - モール名
 */
export function rateLimitMiddleware(mall) {
  return async (req, res, next) => {
    const sem = limiters[mall];
    if (!sem) return next();

    // 待ちが多すぎる場合はリジェクト
    if (sem.pending >= 5) {
      return res.status(429).json({
        ok: false,
        error: 'RATE_LIMIT_QUEUE_FULL',
        message: `Too many pending requests for ${mall}. Active: ${sem.active}, Pending: ${sem.pending}`,
        requestId: req.requestId,
      });
    }

    await sem.acquire();
    // レスポンス完了時にリリース
    res.on('finish', () => sem.release());
    next();
  };
}
