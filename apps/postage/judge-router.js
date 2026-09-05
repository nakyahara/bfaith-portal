/**
 * 判定 API (セッション認証なし・x-api-key 認証)。
 *
 * 呼び出し元 = 伝票出しPCの shipping-upload-launcher (ブラウザではないのでセッションが無い)。
 * picking の ingest-api と同じ流儀:
 *   - トークンは env POSTAGE_JUDGE_KEY (未設定なら 503 fail-closed)
 *   - server.js で requireAppAccess 付き本体より先に mount する
 *
 * POST /apps/postage/judge-api/batch
 *   { slip_nos: ["1545500", ...], date?: "YYYY-MM-DD", source?: "launcher", batch_ref?: "出荷_13" }
 *   → { ok, date, tariff, composition_available, results: [{ slip_no, status, status_label, print_text,
 *        band_name, amount_yen, material_name, decision_id, reason, reason_label, detail, method_code, ... }], summary }
 * GET  /apps/postage/judge-api/health  → 接続確認 (料金表と構成データが読めるか)
 */
import express from 'express';
import crypto from 'crypto';
import { judgeBatch, JudgeInputError, MAX_SLIPS_PER_CALL } from './judge-service.js';
import { getTariffVersionFor, jstToday } from './db.js';
import { mirrorAvailable } from './composition.js';

const router = express.Router();

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function requireJudgeKey(req, res, next) {
  const expected = process.env.POSTAGE_JUDGE_KEY;
  if (!expected) return res.status(503).json({ ok: false, error: 'postage_judge_key_unset' });
  const provided = req.headers['x-api-key'];
  if (typeof provided !== 'string' || !timingSafeEq(provided, expected)) {
    return res.status(401).json({ ok: false, error: 'APIキーが不正です' });
  }
  next();
}

// ── 呼び出し回数の上限 (プロセス内・1 分の窓) ──
// 1 日の呼び出しは出荷の回数 (数回) + やり直し程度。キー漏洩やランチャーの再送ループで
// 同期処理 (判定 + SQLite INSERT) がイベントループを塞ぎ続けるのを防ぐ
export const RATE_LIMIT = { windowMs: 60_000, maxCalls: 30 };
const callTimes = [];
function rateLimit(req, res, next) {
  const now = Date.now();
  while (callTimes.length && now - callTimes[0] > RATE_LIMIT.windowMs) callTimes.shift();
  if (callTimes.length >= RATE_LIMIT.maxCalls) {
    res.setHeader('Retry-After', String(Math.ceil((callTimes[0] + RATE_LIMIT.windowMs - now) / 1000)));
    return res.status(429).json({ ok: false, error: `呼び出しが多すぎます (1分に ${RATE_LIMIT.maxCalls} 回まで)` });
  }
  callTimes.push(now);
  next();
}
/** テスト用。窓を空にする */
export function _resetRateLimit() { callTimes.length = 0; }

router.use(requireJudgeKey);
// 本体 (server.js) の共通 JSON parser はこの経路を除外している。認証 → ここで初めて body を読む (256kb)
router.use(express.json({ limit: '256kb' }));

router.get('/health', (_req, res) => {
  const tariff = getTariffVersionFor(jstToday());
  res.json({
    ok: true,
    date: jstToday(),
    tariff: tariff ? { id: tariff.tariff_version_id, name: tariff.name } : null,
    composition_available: mirrorAvailable(),
    max_slips_per_call: MAX_SLIPS_PER_CALL,
  });
});

router.post('/batch', rateLimit, (req, res) => {
  try {
    const r = judgeBatch({
      slip_nos: req.body?.slip_nos,
      date: req.body?.date,
      source: req.body?.source,
      batch_ref: req.body?.batch_ref,
      actor: typeof req.body?.source === 'string' ? `judge-api:${req.body.source.slice(0, 40)}` : 'judge-api',
    });
    res.json({ ok: true, ...r });
  } catch (e) {
    if (e instanceof JudgeInputError) return res.status(400).json({ ok: false, error: e.message });
    console.error('[postage judge-api]', e);
    res.status(500).json({ ok: false, error: '判定に失敗しました' });
  }
});

// express.json の構文エラー等
// eslint-disable-next-line no-unused-vars
router.use((err, _req, res, _next) => {
  if (err?.type === 'entity.parse.failed') return res.status(400).json({ ok: false, error: 'JSON が読めません' });
  if (err?.type === 'entity.too.large') return res.status(413).json({ ok: false, error: '本文が大きすぎます' });
  console.error('[postage judge-api]', err);
  res.status(500).json({ ok: false, error: '判定に失敗しました' });
});

export default router;
