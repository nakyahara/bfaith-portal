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
  const result = await deps.fetchStockSearch(q);
  if (!result) return [{ type: 'text', text: '在庫データを取得できませんでした。少し待ってもう一度送ってください。' }];
  const items = result.items;
  if (items.length === 0) return [{ type: 'text', text: `「${q}」に一致する商品が見つかりませんでした。別のキーワードで試してください。` }];
  if (items.length === 1) {
    const text = await buildStockText(items[0].sku, deps);
    return [{ type: 'text', text: text || `該当データが見つかりませんでした (${items[0].sku})。` }];
  }
  const top = items.slice(0, MAX_CANDIDATES);
  const lines = top.map((it, i) => `${CIRCLED[i]} ${it.name || it.sku} — フリー${it.free}`);
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
          displayText: `${CIRCLED[i]} ${String(it.name || it.sku).slice(0, 80)}`,
        },
      })),
    },
  }];
}

/** postback (候補タップ) への応答メッセージ配列。 */
export async function buildPostbackReplyMessages(data, deps) {
  const m = String(data || '').match(/^stock:(.+)$/);
  if (!m) return null;   // 在庫検索以外のpostbackには触らない
  const text = await buildStockText(m[1], deps);
  return [{ type: 'text', text: text || `該当データが見つかりませんでした (${m[1]})。在庫が動いた可能性があります。もう一度検索してください。` }];
}

/**
 * webhook イベント列を処理して必要な返信を送る (fire-and-forget 前提・全 fail-soft)。
 * deps はテスト用注入 (既定は実物)。
 */
export async function processLineEvents(events, deps = {}) {
  const d = {
    fetchStockLocations,
    fetchStockSearch,
    reply: replyLine,
    ...deps,
  };
  for (const ev of events || []) {
    try {
      if (!ev || !ev.replyToken || !isSearchSource(ev.source)) continue;
      if (ev.type === 'message' && ev.message?.type === 'text') {
        const messages = await buildSearchReplyMessages(ev.message.text, d);
        await d.reply(ev.replyToken, messages);
      } else if (ev.type === 'postback') {
        const messages = await buildPostbackReplyMessages(ev.postback?.data, d);
        if (messages) await d.reply(ev.replyToken, messages);
      }
    } catch (e) {
      console.warn(`[line-search] イベント処理失敗 (スキップ): ${e.message}`);
    }
  }
}
