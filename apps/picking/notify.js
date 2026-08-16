/**
 * 欠品の即時通知 (要件§5.6 確定 2026-08-11 → 2026-08-13 中原さん要望で LINE を第一候補に)。
 *
 * 送信先 (設定されているものへ送る。両方あれば両方):
 *   - LINE: env PICKING_LINE_CHANNEL_TOKEN (LINE公式アカウントの Messaging API
 *     チャネルアクセストークン)。⚠ LINE Notify は2025-03終了のため Messaging API を使う。
 *     env PICKING_LINE_TO (カンマ区切りの userId/groupId) があれば push、
 *     無ければ broadcast (公式アカウントを友だち追加した全員に届く。社内専用アカウント前提)
 *   - 土日祝 (JST) は env PICKING_LINE_TO_HOLIDAY があればそちらへ送る
 *     (2026-08-15 中原さん要望: 休日は別の休日専用LINEグループに通知)。
 *     未設定の土日祝は平日と同じ PICKING_LINE_TO へ (通知が消えるより誤配のほうがまし)
 *   - Google Chat: env PICKING_ALERT_WEBHOOK (旧経路。互換のため残す)
 *
 * - どちらも未設定なら何もしない (導入前でも動く)
 * - fail-soft: 通知失敗でピッキング作業は止めない (呼び出し側は fire-and-forget)
 * - 在庫修正・出荷保留の後続対応は通知を受けた管理者が行う (システムは記録と通知まで)
 */

import crypto from 'node:crypto';
import { isJstWeekendOrHoliday } from './jp-holiday.js';
import { fetchStockLocations, buildStockLocationsText, stockLookupConfigured } from './stock-locations.js';

const TIMEOUT_MS = 5000;

const parseTo = (v) => String(v || '').split(',').map((s) => s.trim()).filter(Boolean);

/**
 * LINEのpush宛先を決める。土日祝 (JST) は PICKING_LINE_TO_HOLIDAY を優先し、
 * 未設定なら平日と同じ宛先へフォールバック (通知の取りこぼしを作らない)。
 * @returns {{to: string[], holiday: boolean}} to が空なら broadcast
 */
export function resolveLineTo(now = new Date()) {
  const holiday = isJstWeekendOrHoliday(now);
  const holidayTo = parseTo(process.env.PICKING_LINE_TO_HOLIDAY);
  if (holiday && holidayTo.length > 0) return { to: holidayTo, holiday };
  return { to: parseTo(process.env.PICKING_LINE_TO), holiday };
}

/** 読み手ファースト (現場の管理者が次の行動を決められる形) の欠品メッセージ。 */
export function buildShortageText({ batch, line, worker, shortageQty, stockText }) {
  const picked = line.qty - shortageQty;
  return [
    '🚨 ピッキング欠品',
    `${batch.folder_name || ''}｜${batch.hikiate_class}`,
    `ロケ: ${line.locationLabel || line.location}`,
    `商品: ${line.product_name || ''}`,
    // NE在庫修正でそのまま検索・コピーできるよう独立行 (2026-08-16 中原さん要望)
    `商品コード: ${line.sku}`,
    `欠品 ${shortageQty}個 / 指示 ${line.qty}個${picked > 0 ? ` (${picked}個は確保済み)` : ''}`,
    `作業者: ${worker}`,
    // 他ロケ在庫 (ロジザード在庫スナップショット。取得失敗時も「取得できず」を必ず出す)
    stockText || null,
  ].filter((s) => s !== null).join('\n');
}

async function postJson(url, headers, body) {
  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: JSON.stringify(body),
      signal: ctrl.signal,
    });
    if (!res.ok) {
      const t = await res.text().catch(() => '');
      throw new Error(`HTTP ${res.status}: ${t.slice(0, 150)}`);
    }
  } finally {
    clearTimeout(timer);
  }
}

/** LINE Messaging API へ送信。宛先指定 (push) か全友だち (broadcast)。 */
async function sendLine(text, now = new Date()) {
  const token = process.env.PICKING_LINE_CHANNEL_TOKEN;
  if (!token) return false;
  const headers = { Authorization: `Bearer ${token}` };
  const messages = [{ type: 'text', text }];
  const { to } = resolveLineTo(now);
  if (to.length > 0) {
    for (const id of to) {
      await postJson('https://api.line.me/v2/bot/message/push', headers, { to: id, messages });
    }
  } else {
    await postJson('https://api.line.me/v2/bot/message/broadcast', headers, { messages });
  }
  return true;
}

/** Google Chat webhook へ送信 (旧経路・互換)。 */
async function sendGChat(text) {
  const webhook = process.env.PICKING_ALERT_WEBHOOK;
  if (!webhook) return false;
  await postJson(webhook, {}, { text });
  return true;
}

/**
 * 欠品通知。設定済みの経路すべてへ送る。片方の失敗でもう片方を止めない。
 * @returns {'disabled'|'sent'} 全経路失敗は throw (呼び出し側が warn ログ)
 */
export async function notifyShortage(info, now = new Date()) {
  if (!process.env.PICKING_LINE_CHANNEL_TOKEN && !process.env.PICKING_ALERT_WEBHOOK) return 'disabled';
  // 同一SKUの他ロケ在庫 (ロジザード毎時スナップショット) を warehouse から取る。
  // fail-soft: 取得・整形のどんな失敗でも通知本体は止めない (想定外レスポンス形状で
  // 整形が throw しても「取得できず」に落とす)。呼び出し側は fire-and-forget なので待ってよい。
  // warehouse連携が未設定の環境では在庫行そのものを出さない (「取得できず」のノイズ防止)
  let stockText = null;
  if (stockLookupConfigured()) {
    try {
      stockText = buildStockLocationsText(await fetchStockLocations(info.line?.sku), {
        excludeBlock: info.line?.block,
        excludeLocation: info.line?.location,
        now,
      });
    } catch (e) {
      console.warn(`[picking-notify] 他ロケ在庫の整形失敗 (通知は継続): ${e.message}`);
      stockText = '📍 他ロケ在庫: 取得できず';
    }
  }
  const text = buildShortageText({ ...info, stockText });
  const results = await Promise.allSettled([sendLine(text, now), sendGChat(text)]);
  const failures = results.filter((r) => r.status === 'rejected');
  const sent = results.some((r) => r.status === 'fulfilled' && r.value === true);
  if (!sent) {
    throw new Error(failures.map((f) => f.reason?.message).join(' / ') || '送信先なし');
  }
  if (failures.length > 0) {
    console.warn(`[picking-notify] 一部経路の送信失敗: ${failures.map((f) => f.reason?.message).join(' / ')}`);
  }
  return 'sent';
}

/**
 * LINE webhook の署名検証つき受け口 (groupId 取得用の最小実装)。
 *
 * 用途: グループ宛 push (グループは人数に関係なく1通カウント = 無料枠で十分) にするには
 * bot をグループに招待して groupId を知る必要があり、それは webhook イベントでしか取れない。
 * LINE Developers コンソールで Webhook URL に /apps/picking/line-webhook を設定し、
 * bot をグループに招待 (またはグループで発言) すると、サーバーログに groupId が出る。
 * PICKING_LINE_TO に設定したら Webhook 利用は終了してよい (設定は残しても無害)。
 *
 * @param rawBody Buffer (署名は生のボディで検証する)
 * @param signature X-Line-Signature ヘッダ
 * @returns 200を返してよければ true (署名不一致は false = 呼び出し側が403)
 */
export function handleLineWebhook(rawBody, signature) {
  const secret = process.env.PICKING_LINE_CHANNEL_SECRET;
  if (!secret) return false;   // 未設定なら受け付けない (fail-closed)
  const expected = crypto.createHmac('sha256', secret).update(rawBody).digest('base64');
  const a = Buffer.from(String(signature || ''));
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return false;
  try {
    const body = JSON.parse(rawBody.toString('utf8'));
    for (const ev of body.events || []) {
      const src = ev.source || {};
      // 通知先設定に使うIDをログへ (groupId / roomId / userId)
      const id = src.groupId || src.roomId || src.userId;
      if (id) {
        console.log(`[picking-line] webhook event type=${ev.type} sourceType=${src.type} id=${id} → PICKING_LINE_TO に設定してください`);
      }
    }
  } catch (e) {
    console.warn(`[picking-line] webhook body の解析失敗: ${e.message}`);
  }
  return true;
}
