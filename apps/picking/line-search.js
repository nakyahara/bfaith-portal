/**
 * line-search — LINE在庫検索ボット (欠品通知の公式アカウントに同居・miniPC standalone専用)。
 *
 * 商品名 (の一部)・商品ID・バーコードを送ると SKU 候補をクイックリプライで返し、
 * タップ (postback) でロケーション別フリー在庫を返信する。
 *
 * 費用ゼロ設計: すべて応答メッセージ (reply API) = LINEの無料枠 (push 月200通) を消費しない。
 *
 * 応答する場所 (誤爆防止 — 欠品通知グループの人間の会話に反応しない):
 *   - 1:1 チャット (botへのDM): 常に応答
 *   - グループ/ルーム: env PICKING_LINE_SEARCH_TO (カンマ区切りID) に載っているものだけ応答。
 *     groupId は既存の webhook ログで取れる (グループにbotを招待して発言するとログに出る)
 *
 * fail-soft: 検索失敗・warehouse停止でもエラー文を返信するだけで、他機能 (欠品通知) に影響しない。
 * データ源 = warehouse service-api (raw_lz_inventory、毎時9-18時更新)。
 */
import { fetchStockLocations, fetchStockSearch, buildStockLocationsText } from './stock-locations.js';

const REPLY_URL = 'https://api.line.me/v2/bot/message/reply';
const TIMEOUT_MS = 5000;
const MAX_CANDIDATES = 10;      // クイックリプライは最大13個。余裕を持って10
const USAGE = '商品名の一部 (例: ティーツリー)・商品ID・バーコードを送ると在庫ロケーションを返します。';
const CIRCLED = ['①', '②', '③', '④', '⑤', '⑥', '⑦', '⑧', '⑨', '⑩'];

const parseIds = (v) => String(v || '').split(',').map((s) => s.trim()).filter(Boolean);

/** このイベント元で検索に応答してよいか。1:1は常にOK・グループ/ルームは許可リストのみ。 */
export function isSearchSource(source) {
  const src = source || {};
  if (src.type === 'user') return true;
  const id = src.groupId || src.roomId;
  return Boolean(id && parseIds(process.env.PICKING_LINE_SEARCH_TO).includes(id));
}

async function replyLine(replyToken, messages, fetchFn = fetch) {
  const token = process.env.PICKING_LINE_CHANNEL_TOKEN;
  if (!token || !replyToken) return false;
  try {
    const res = await fetchFn(REPLY_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
      body: JSON.stringify({ replyToken, messages }),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (!res.ok) {
      const t = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status}: ${t.slice(0, 150)}`);
    }
    return true;
  } catch (e) {
    console.warn(`[line-search] 返信失敗 (握りつぶす): ${e.message}`);
    return false;
  }
}

/** SKU のロケーション別在庫の返信テキスト。データ無しは null。 */
async function buildStockText(sku, deps) {
  const data = await deps.fetchStockLocations(sku);
  if (!data || data.locations.length === 0) return null;
  const body = buildStockLocationsText(data, { title: '在庫ロケーション', maxLines: 10 });
  return `${data.name || ''}\n(${String(sku).trim().toLowerCase()})\n${body}`;
}

/** 検索メッセージへの応答メッセージ配列を組み立てる (LINE reply API の messages 形式)。 */
export async function buildSearchReplyMessages(query, deps) {
  const q = String(query || '').trim();
  if (!q) return [{ type: 'text', text: USAGE }];
  if (q.length < 2) return [{ type: 'text', text: `キーワードは2文字以上で送ってください。${USAGE}` }];
  if (q.length > 100) return [{ type: 'text', text: 'キーワードが長すぎます (100文字まで)。' }];
  const result = await deps.fetchStockSearch(q);
  if (!result) return [{ type: 'text', text: '在庫データを取得できませんでした。少し待ってもう一度送ってください。' }];
  // LINE側の上限対策: postback data 300文字 / label 20文字 / 本文はSKUごとの商品名を切り詰め。
  // 異常に長いSKU (data上限を超える) は件数分岐の前に除外する (1件ヒットの経路にも通さない)
  const items = result.items.filter((it) => String(it.sku).length <= 200);
  const notFound = () => [{ type: 'text', text: `「${q.slice(0, 30)}」に一致する商品が見つかりませんでした。別のキーワードで試してください。` }];
  if (items.length === 0) return notFound();
  if (items.length === 1) {
    const text = await buildStockText(items[0].sku, deps);
    return [{ type: 'text', text: text || `該当データが見つかりませんでした (${String(items[0].sku).slice(0, 40)})。` }];
  }
  const top = items.slice(0, MAX_CANDIDATES);
  const shortName = (it) => String(it.name || it.sku).slice(0, 30);
  const lines = top.map((it, i) => `${CIRCLED[i]} ${shortName(it)} — フリー${it.free}`);
  if (items.length > MAX_CANDIDATES) lines.push(`…他にも候補があります (${MAX_CANDIDATES}件まで表示)。キーワードを絞ってください`);
  return [{
    type: 'text',
    text: `🔍 ${top.length}件見つかりました。下のボタンをタップしてください\n${lines.join('\n')}`,
    quickReply: {
      items: top.map((it, i) => ({
        type: 'action',
        action: {
          type: 'postback',
          // ラベルは20文字制限。番号+名前の頭で識別できるようにする
          label: `${CIRCLED[i]} ${String(it.name || it.sku)}`.slice(0, 20),
          data: `stock:${it.sku}`,
          displayText: `${CIRCLED[i]} ${shortName(it)}`,
        },
      })),
    },
  }];
}

/** postback (候補タップ) への応答メッセージ配列。 */
export async function buildPostbackReplyMessages(data, deps) {
  const m = String(data || '').match(/^stock:(.+)$/);
  if (!m) return null;   // 在庫検索以外のpostbackには触らない
  // dataはLINE署名で「LINE経由」なのは保証されるが、このquickReply由来とまでは限らない。
  // SKUとして妥当な形だけ通す (長さ・制御文字。文字種はロジザード側が自由なので縛らない)
  const sku = m[1].trim();
  if (!sku || sku.length > 200 || /[\x00-\x1F\x7F]/.test(sku)) return null;
  const text = await buildStockText(sku, deps);
  return [{ type: 'text', text: text || `該当データが見つかりませんでした (${sku.slice(0, 40)})。在庫が動いた可能性があります。もう一度検索してください。` }];
}

// webhook再送 (LINE側リトライ) の重複処理よけ。webhookEventId を10分だけ記録する。
// 開始時に「予約」し、処理失敗・返信失敗なら予約を消す (再送での自己回復を妨げない)
const DEDUP_TTL_MS = 10 * 60 * 1000;
const DEDUP_MAX = 5000;
const _seenEvents = new Map();   // webhookEventId → 記録時刻 (Mapは挿入順=先頭が最古)

function reserveEvent(ev, nowMs = Date.now()) {
  const id = ev?.webhookEventId;
  if (!id) return { duplicate: false, release: () => {} };   // IDが無いイベントは判定不能=処理する
  // 期限切れ掃除は先頭 (最古) から、期限内に当たったら打ち切り (毎回の全件走査を避ける)
  for (const [k, at] of _seenEvents) {
    if (nowMs - at > DEDUP_TTL_MS) _seenEvents.delete(k);
    else break;
  }
  if (_seenEvents.has(id)) return { duplicate: true, release: () => {} };
  if (_seenEvents.size >= DEDUP_MAX) _seenEvents.delete(_seenEvents.keys().next().value);   // 最古を追い出す
  _seenEvents.set(id, nowMs);
  return { duplicate: false, release: () => _seenEvents.delete(id) };
}

async function handleOneEvent(ev, d) {
  if (!ev || !ev.replyToken || !isSearchSource(ev.source)) return;
  const dedup = reserveEvent(ev);
  if (dedup.duplicate) return;   // 再送は無視 (reply token使用済みエラーのノイズ防止)
  try {
    let sent = true;   // 返信対象外のイベント種別は「処理済み」でよい
    if (ev.type === 'message' && ev.message?.type === 'text') {
      sent = await d.reply(ev.replyToken, await buildSearchReplyMessages(ev.message.text, d));
    } else if (ev.type === 'postback') {
      const messages = await buildPostbackReplyMessages(ev.postback?.data, d);
      if (messages) sent = await d.reply(ev.replyToken, messages);
    }
    if (!sent) dedup.release();   // 返信失敗はLINEの再送で自己回復させる
  } catch (e) {
    dedup.release();
    console.warn(`[line-search] イベント処理失敗 (スキップ): ${e.message}`);
  }
}

// 1 webhookに大量イベントが来ても下流 (warehouse / LINE reply) へ一斉に流さない
const CONCURRENCY = 5;

/**
 * webhook イベント列を処理して必要な返信を送る (fire-and-forget 前提・全 fail-soft)。
 * イベント単位で並行処理 (直列だと先頭の遅延で後続の reply token 期限を食い潰すため)。
 * deps はテスト用注入 (既定は実物)。
 */
export async function processLineEvents(events, deps = {}) {
  const d = {
    fetchStockLocations,
    fetchStockSearch,
    reply: replyLine,
    ...deps,
  };
  const list = events || [];
  for (let i = 0; i < list.length; i += CONCURRENCY) {
    await Promise.allSettled(list.slice(i, i + CONCURRENCY).map((ev) => handleOneEvent(ev, d)));
  }
}
