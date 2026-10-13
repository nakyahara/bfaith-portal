#!/usr/bin/env node
/**
 * master-register-summary.mjs — 新商品の登録と product-hub のカードの知らせ・要確認の商品の 1 行のまとめ (読むだけ・Company DB構想 14 ⑤-2a・仮レビュー L6・22 の Q-4)
 *
 * 出すもの (最後の 1 行): 登録の状態ごとの数 (下書き・要確認 ほか) と、カード作成待ち (まだ・失敗)・衝突の数・いちばん古い知らせの時刻・
 *   🆕 要確認 (NE で直接作られた商品) の確かめの表の答え (中原さんの決定 10/13 Q5 a = 毎朝のまとめに「要確認 N 件」と一覧):
 *   確認済みにできる件数とコード・直す所がある件数と多い理由・一覧 (はじめの 10 件のコード)
 *   例: ⚠️ 新商品の登録: 下書き 3・要確認 42 / カード作成待ちなし / 要確認 42 件: 確認済みにできる 2 (q01・q02)・直す所あり 40 (推奨保有月数 29・原価 11 …) → マスタの入力の商品の画面で確かめる・一覧: …
 *   その上の行 (ログだけ) = 要確認の 1 件ずつ (コード・確認済みにできる / 直す所)
 * 何も無い日は「なし」の 1 行 (exit 0)。表が無い (0052 の前) = ⏭️ の 1 行 (exit 0)。要確認の確かめの関数が無い (0079 の前) = その部分だけ「0079 の前」。
 *   要確認が 500 件を超える = 一覧は出さない (ops.quarantine_confirm_list(null) の上限・件数だけ)。要確認の表を読めない = その部分だけ「読めない」(行は出す)
 *   DB に届かない = ❌ (exit 1)。🆕 --daily (毎朝の daily-sync の 1 段) = 届かないときも止めずに ⚠️「読めない」(exit 0 = 毎朝の通知を止めない・再試行に載せない)
 *   --daily は中の期限 DAILY_DEADLINE_MS (60 秒 = daily-sync の 120 秒より短い) で打ち切って ⚠️「読めない」(exit 0)。接続が応答しないときも親に殺されない (Codex #1717 R1 High)
 *   コードは 1 行に直して 40 文字まで (改行・制御文字でログの行や朝のまとめを壊さない。Codex #1717 R1 Medium)
 * 使い方 (miniPC・毎朝の daily-sync の段「新商品と要確認のまとめ」= 台帳 config/jobs-registry.mjs の warehouse-daily-sync の中。新しい定期実行ではない):
 *   node -r dotenv/config scripts/company-db/master-register-summary.mjs [--json] [--daily]
 * env: COMPANY_DB_WATCH_URL (見張りの読むだけのロール watcher = 0052 で select・0079 で確かめの実行を渡してある) / 無ければ COMPANY_DB_URL
 */
import { openPgClient } from './migrate.mjs';

export const REG_LABELS = Object.freeze({ draft: '下書き', ne_pending: 'NE登録待ち', ne_confirmed: 'NE確認済み', distributable: '配る対象', quarantined: '要確認', cancelled: 'やめた' });
/** 要確認の一覧の上限 (ops.quarantine_confirm_list(null) と同じ 500 件) */
export const QUARANTINE_LIST_MAX = 500;
/** 一行に出すコードの数 */
export const QUARANTINE_LINE_CODES = 10;
/** 確かめの表の行 (key) → まとめの言葉。ne.* は「NE と違う (マスタの判断)」にまとめる */
export const QUARANTINE_KEY_LABELS = Object.freeze({
  state: '状態', code_length: 'コードが長い', kind: '区分', name: '名前', price: '売価', tax: '税率', handling: '取扱', sales_class: '売上分類',
  reorder_months: '推奨保有月数', supplier: '代表の仕入先', cost: '原価', parent: '代表 (まとまり)', components: '構成品', component_request: '構成の依頼',
  ne_run: '照合待ち', ne_observed: '照合待ち', live_file: 'CSV の途中', ne: 'NE と違う (マスタの判断)',
});
const labelOf = (key) => QUARANTINE_KEY_LABELS[String(key).startsWith('ne.') ? 'ne' : key] ?? key;
const oneLine = (s, n = 160) => String(s ?? '').replace(/\s+/g, ' ').slice(0, n);
/** --daily の中の期限 (daily-sync の親の 120 秒より十分短く)。試験だけ env REGISTER_SUMMARY_DEADLINE_MS で縮める */
export const DAILY_DEADLINE_MS = 60000;
/** コードの長さの上限 (まとめ・ログに出すとき) */
export const CODE_SHOW_MAX = 40;
/** まとめ・ログに出すコード: 改行・制御文字・向きを変える文字を空白にして 1 行・40 文字まで (超えたら …) */
export function safeCode(x) {
  const s = String(x ?? '?').replace(/[\u0000-\u001f\u007f-\u009f\u200e\u200f\u2028\u2029\u202a-\u202e\u2066-\u2069]/g, ' ').replace(/\s+/g, ' ').trim() || '?';
  return s.length > CODE_SHOW_MAX ? `${s.slice(0, CODE_SHOW_MAX - 1)}…` : s;
}
/** p が ms で終わらなければ「…が N 秒で終わらない」で断る (タイマーは片づける) */
export function withDeadline(p, ms, what) {
  let timer;
  const t = new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`${what}が ${Math.round(ms / 1000)} 秒で終わらない`)), ms); });
  return Promise.race([p, t]).finally(() => clearTimeout(timer));
}

/**
 * 要確認の部分 (db = { query })。戻り値 { state: none | not_applied | too_many | unreadable | ok, total, ready: [code], notReady: [{ code, reasons: [言葉] }], reasonCounts: { 言葉: 数 }, text, details: [行], error? }
 * total = 登録の状態が要確認の数 (登録の表から)。一覧は 500 件までのときだけ ops.quarantine_confirm_list(null) で読む
 */
export async function summarizeQuarantine(db, total) {
  const base = { total, ready: [], notReady: [], reasonCounts: {}, details: [] };
  if (!total) return { ...base, state: 'none', text: '要確認なし' };
  const has = (await db.query(`select to_regprocedure('ops.quarantine_confirm_list(text[])') is not null as ok`)).rows[0].ok;
  if (!has) return { ...base, state: 'not_applied', text: `要確認 ${total} 件 (確かめの表は 0079 の前 = まだ出せない)` };
  if (total > QUARANTINE_LIST_MAX) return { ...base, state: 'too_many', text: `要確認 ${total} 件 (${QUARANTINE_LIST_MAX} 件を超える = 一覧は出さない・マスタの入力の一覧で「要確認」に絞る)` };
  let list;
  try {
    list = (await db.query('select ops.quarantine_confirm_list(null) as r')).rows[0].r;
    if (!Array.isArray(list)) throw new Error('一覧の形が違う');
  } catch (e) {
    return { ...base, state: 'unreadable', error: oneLine(e && e.message), text: `要確認 ${total} 件 (確かめの表を読めない: ${oneLine(e && e.message, 120)})` };
  }
  const ready = [], notReady = [], reasonCounts = {}, details = [], codes = [];
  for (const x of list) {
    const code = safeCode(x?.code ?? x?.sku_id);
    codes.push(code);   // 一覧 = 確かめの表の順のまま (確認済みにできるものを先に集めない。Codex #1717 R1 Medium)
    if (x?.ok === true) { ready.push(code); details.push(`  ✅ ${code}: 確認済みにできる`); continue; }
    const reasons = [...new Set((Array.isArray(x?.items) ? x.items : []).filter((it) => it && it.ok !== true).map((it) => labelOf(it.key)))];
    for (const r of reasons) reasonCounts[r] = (reasonCounts[r] || 0) + 1;
    notReady.push({ code, reasons });
    details.push(`  ・ ${code}: ${reasons.join('・') || '直す所がある'}`);
  }
  const top = Object.entries(reasonCounts).sort((a, b) => b[1] - a[1] || (a[0] < b[0] ? -1 : 1)).slice(0, 5).map(([k, n]) => `${k} ${n}`).join('・');
  const shown = codes.slice(0, QUARANTINE_LINE_CODES).join('・') + (codes.length > QUARANTINE_LINE_CODES ? ` ほか ${codes.length - QUARANTINE_LINE_CODES} 件` : '');
  const readyText = ready.length ? `確認済みにできる ${ready.length} (${ready.slice(0, QUARANTINE_LINE_CODES).join('・')}${ready.length > QUARANTINE_LINE_CODES ? ' ほか' : ''})` : '確認済みにできる 0';
  const text = `要確認 ${list.length} 件: ${readyText}・直す所あり ${notReady.length}${top ? ` (${top})` : ''} → マスタの入力の商品の画面で確かめる・一覧: ${shown}`;
  return { ...base, total: list.length, state: 'ok', ready, notReady, reasonCounts, details, text };
}

/** 1 行を作る (db = { query })。返り値 { line, counts, quarantine, details } */
export async function summarize(db) {
  const has = (await db.query(`select to_regclass('ops.product_hub_outbox') is not null and to_regclass('ops.master_registrations') is not null as ok`)).rows[0].ok;
  if (!has) return { line: '⏭️ 新商品の登録: 0052 がまだ (表が無い)', counts: null, quarantine: null, details: [] };
  const reg = Object.fromEntries((await db.query(`select state, count(*)::int as n from ops.master_registrations where state <> 'available' group by state`)).rows.map((r) => [r.state, r.n]));
  const card = Object.fromEntries((await db.query(`select status, n, to_char(oldest_at at time zone 'Asia/Tokyo', 'MM-DD HH24:MI') as oldest from ops.v_product_hub_outbox_open`)).rows.map((r) => [r.status, r]));
  const regText = Object.entries(REG_LABELS).filter(([k]) => reg[k]).map(([k, v]) => `${v} ${reg[k]}`).join('・') || '途中の新商品なし';
  const waiting = (card.pending?.n || 0) + (card.failed?.n || 0);
  const oldest = [card.pending?.oldest, card.failed?.oldest].filter(Boolean).sort()[0] || null;
  const cardText = waiting || card.conflict
    ? `カード作成待ち ${waiting}${card.failed ? ` (失敗 ${card.failed.n})` : ''}${oldest ? `・いちばん古い ${oldest}` : ''}${card.conflict ? `・衝突 ${card.conflict.n}` : ''} → マスタの入力の一覧「カード」で絞る`
    : 'カード作成待ちなし';
  const quarantine = await summarizeQuarantine(db, reg.quarantined || 0);
  const warn = waiting || card.conflict || reg.quarantined ? '⚠️' : '📦';
  return {
    line: `${warn} 新商品の登録: ${regText} / ${cardText}${quarantine.state === 'none' ? '' : ` / ${quarantine.text}`}`,
    counts: { registrations: reg, cards: Object.fromEntries(Object.entries(card).map(([k, v]) => [k, v.n])),
      quarantine: { state: quarantine.state, total: quarantine.total, ready: quarantine.ready.length, not_ready: quarantine.notReady.length, reasons: quarantine.reasonCounts } },
    quarantine, details: quarantine.details,
  };
}

/** 一番最後の 1 行 (daily-sync の要約に載る)。--daily = 読めない・届かないも止めずに ⚠️ (exit 0) */
export function failureLine(err, { daily = false } = {}) {
  const msg = oneLine(err && err.message, 300);
  return daily ? { line: `⚠️ 新商品と要確認のまとめ: 読めない (${msg}) = 毎朝の通知は止めない・明日また読む`, code: 0 } : { line: `❌ 新商品の登録のまとめ: ${msg}`, code: 1 };
}

const isMain = process.argv[1] && /master-register-summary\.mjs$/i.test(process.argv[1]);
if (isMain) {
  const daily = process.argv.includes('--daily');
  const deadlineMs = Number(process.env.REGISTER_SUMMARY_DEADLINE_MS) > 0 ? Number(process.env.REGISTER_SUMMARY_DEADLINE_MS) : DAILY_DEADLINE_MS;
  let code = 1, line = '', timedOut = false;
  const url = process.env.COMPANY_DB_WATCH_URL || process.env.COMPANY_DB_URL;
  let client = null;
  const work = (async () => {
    if (!url) throw new Error('COMPANY_DB_WATCH_URL (か COMPANY_DB_URL) が要る');
    // 接続とクエリにも pg 側の期限 (応答しない DB で待ち続けない)
    client = await openPgClient(url, { connectionTimeoutMillis: 15000, query_timeout: 25000 });
    client.on('error', () => {});   // 落ちた接続の誤りは query の失敗として受ける
    await client.query(`set default_transaction_read_only = on`);
    await client.query(`set statement_timeout = '20s'`);
    const r = await summarize({ query: (t, p) => client.query(t, p) });
    if (process.argv.includes('--json')) console.log(JSON.stringify(r.counts));
    for (const d of r.details) console.log(d);
    return r.line;
  })();
  try {
    // --daily = 中の期限で打ち切る (親の daily-sync の 120 秒に殺されると ❌ になる。Codex #1717 R1 High)
    line = await (daily ? withDeadline(work, deadlineMs, 'まとめ') : work); code = 0;
  } catch (e) {
    timedOut = daily && /秒で終わらない$/.test(String(e && e.message));
    work.catch(() => {});
    ({ line, code } = failureLine(e, { daily }));
  }
  if (client) { try { await withDeadline(client.end(), 5000, '接続の片づけ'); } catch { timedOut = true; } }
  console.log(line);
  process.exitCode = code;
  // 固まった接続が残ると process が終わらない = 書き終えてから終える (固まらなかった朝は普通に終わる)
  if (timedOut) { try { client?.connection?.stream?.destroy(); } catch { /* */ } setTimeout(() => process.exit(code), 2000).unref(); }
}
