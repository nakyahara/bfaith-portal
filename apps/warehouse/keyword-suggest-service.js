/**
 * Amazon サジェスト収集 サービスAPI (SP広告KW PR1・2026-09-23)
 * /service-api/keyword-suggest にマウント
 *
 * 呼び手 = Render の product-hub (商品詳細「📣 SP広告KW」タブ)。
 * 🚨 サジェストの口 (completion.amazon.co.jp) は公式 API ではない。**会社の回線 (miniPC) から、人が検索ボックスに
 *   打つのと同じ頻度帯で**叩くためにここに置く。Render から直接は叩かない
 *   (『Amazon_SP広告KW自動生成_設計方針_20260922.md』§1 データの出どころ・中原さん 2026-09-23)。
 *
 * 守ること:
 *   - 種 KW 1 つにつき 1 リクエスト (基本 1 + ひらがな 46 = 47 回・0.2 秒間隔 ≈ 10 秒)。深掘りは受け付けない
 *   - **同時 1 本は「収集そのもの」で守る** (PR #1408 Codex R1 #1 / R2 #2 #3)。rate-limiter の待ち行列は使わない
 *     (枠は接続が切れると解放されるうえ、待たされた分だけ Render の 45 秒を食う)。走っている間は接続の有無にかかわらず**即 429**。
 *     呼び手 (Render) が待ち切れず切断したら収集を中断する (Amazon を叩き続けない)。
 *     `active` は収集の Promise が決着するまで解放しない (終了を確かめてから次を入れる)。中断しても決着しないときは
 *     解放せずに「止まっていない」と 429 で伝え、ログに残す (黙って 2 本走らせるより、人に見えるほうを選ぶ)
 *   - 全体の期限 DEADLINE_MS は実行中の取得にも効く。失敗の再試行やタイムアウトが重なっても、Render の待ち (45 秒) の内側で
 *     「ここまで取れた」を状態つきで返す (残りは unrun・summary.stopped='deadline')
 *   - 結果には prefix ごとの状態 (success/empty/failed/unrun) を必ず付けて返す — 失敗と 0 件を呼び手が見分けられるように
 *   - User-Agent は env KEYWORD_SUGGEST_UA (plain|browser、既定 browser)。素の UA で同じ結果が返ると分かったら plain に切り替える
 */
import { Router } from 'express';
import { okResponse, errorResponse } from './error-handler.js';
import { getSuggestions } from '../keyword-researcher/suggest.js';

const router = Router();

export const SEED_MAX_LEN = 60;
export const MAX_REQUESTS = 80;      // 基本 1 + ひらがな 46 + アルファベット 26 = 73 が上限。それ以上は unrun
export const DELAY_MS = 200;
export const TIMEOUT_MS = 8000;
export const DEADLINE_MS = 40_000;   // Render 側の待ち (keyword-suggest-client.js の 45 秒) の内側
export const STUCK_AFTER_MS = 5_000; // 中断してもこれだけ経って決着しなければ「止まっていない」と扱う (ログ + 429 の文言)

/** 種 KW の検査。1〜60 文字・制御文字なし・空白は 1 つに寄せる */
export function normalizeSeed(raw) {
  const s = String(raw ?? '').replace(/[\x00-\x1f\x7f]/g, ' ').replace(/\s+/g, ' ').trim();
  if (!s) return { ok: false, error: 'seed（種キーワード）は必須です' };
  if (s.length > SEED_MAX_LEN) return { ok: false, error: `seed は ${SEED_MAX_LEN} 文字以内にしてください` };
  return { ok: true, seed: s };
}

function userAgentFromEnv() {
  return process.env.KEYWORD_SUGGEST_UA === 'plain' ? 'plain' : 'browser';
}

/** いま走っている収集 (プロセスに 1 つ)。接続が切れても収集が決着するまで残る */
let active = null;
export function _activeForTest() { return active; }

const canWrite = (res) => !res.destroyed && !res.writableEnded;

/**
 * POST /service-api/keyword-suggest
 * body: { seed, hiragana?: boolean (既定 true), alphabet?: boolean (既定 false), userAgent?: 'plain'|'browser' (省略時 env) }
 * → { ok, result: { seed, total, suggestions, prefixes, summary, fetchedAt, options } }
 *   429 BUSY = 別の収集が走っている (待たない。呼び手が少し待って押し直す)
 */
router.post('/', async (req, res) => {
  const body = req.body || {};
  const n = normalizeSeed(body.seed);
  if (!n.ok) return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: n.error, requestId: req.requestId });
  if (body.depth != null && Number(body.depth) > 1) {
    return errorResponse(res, { status: 400, error: 'BAD_REQUEST', message: '深掘り (depth>1) はこの口では受け付けません', requestId: req.requestId });
  }
  if (active) {
    const sec = Math.round((Date.now() - active.startedAt) / 1000);
    return errorResponse(res, {
      status: 429, error: 'BUSY',
      message: active.stuck
        ? `前の収集「${active.seed}」の通信が終わっていません (${sec} 秒経過)。しばらく待っても直らなければ WarehouseServer の再起動が必要です`
        : `別の収集「${active.seed}」が走っています (${sec} 秒経過)。終わってからもう一度`,
      requestId: req.requestId,
    });
  }
  const ua = body.userAgent === 'plain' || body.userAgent === 'browser' ? body.userAgent : userAgentFromEnv();
  const ac = new AbortController();
  const me = { seed: n.seed, startedAt: Date.now(), stuck: false, stuckTimer: null };
  active = me;
  // 中断したのに決着しない = 止まっていない。解放はしない (2 本走らせない) が、人に見えるようにする
  const markStuckLater = () => {
    if (me.stuckTimer) return;
    me.stuckTimer = setTimeout(() => {
      if (active === me) { me.stuck = true; console.error(`[keyword-suggest] 収集「${me.seed}」の通信が中断後 ${STUCK_AFTER_MS / 1000} 秒経っても決着しない`); }
    }, STUCK_AFTER_MS);
  };
  // 呼び手が待ち切れずに切った → 収集を止める (結果を届ける先が無いのに Amazon を叩き続けない)。
  // 🚨 'close' の時点で res.destroyed は既に true。「書けるか」(canWrite) ではなく「正常に書き終えたか」で切断を見る (R3 #1)
  const onClose = () => { if (!res.writableEnded) { ac.abort(); markStuckLater(); } };
  res.on('close', onClose);
  const track = { pending: null };
  try {
    const result = await getSuggestions(n.seed, {
      hiragana: body.hiragana !== false,
      alphabet: body.alphabet === true,
      depth: 1,
      delayMs: DELAY_MS,
      timeoutMs: TIMEOUT_MS,
      maxRequests: MAX_REQUESTS,
      deadlineMs: DEADLINE_MS,
      signal: ac.signal,
      retries: 1,
      userAgent: ua,
      track,
    });
    if (canWrite(res)) okResponse(res, { result });
  } catch (e) {
    if (canWrite(res)) errorResponse(res, { status: 500, error: 'SUGGEST_ERROR', message: e.message, requestId: req.requestId });
  } finally {
    res.removeListener('close', onClose);
    // 🚨 応答は返したが裏の通信 (signal を無視した fetch) が決着していない → 決着するまで active を持ち続ける (次の収集と重ねない。R3 #2)
    if (track.pending) {
      markStuckLater();
      try { await track.pending; } catch (_) { /* 決着すればよい */ }
    }
    if (me.stuckTimer) clearTimeout(me.stuckTimer);
    if (active === me) active = null;
  }
});

export default router;
