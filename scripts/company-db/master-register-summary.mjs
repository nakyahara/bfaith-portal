#!/usr/bin/env node
/**
 * master-register-summary.mjs — 新商品の登録と product-hub のカードの知らせの 1 行のまとめ (読むだけ・Company DB構想 14 ⑤-2a・仮レビュー L6)
 *
 * 出すもの (1 行): 登録の状態ごとの数 (下書き・要確認 ほか) と、カード作成待ち (まだ・失敗)・衝突の数・いちばん古い知らせの時刻
 *   例: 📦 新商品の登録: 下書き 3・要確認 1 / カード作成待ち 2 (失敗 1・いちばん古い 10-02 09:10)・衝突 1 → マスタの入力の一覧「カード」で絞る
 * 何も無い日は「なし」の 1 行 (exit 0)。表が無い (0051 の前) = ⏭️ の 1 行 (exit 0)。DB に届かない = ❌ (exit 1)
 * 使い方 (miniPC・毎朝のまとめに足すときは daily-sync の 1 ステップ = 台帳 config/jobs-registry.mjs の daily-sync の中。🚨 足すのは中原さんの OK の後):
 *   node -r dotenv/config scripts/company-db/master-register-summary.mjs [--json]
 * env: COMPANY_DB_WATCH_URL (見張りの読むだけのロール watcher = 0051 で select を渡してある) / 無ければ COMPANY_DB_URL
 */
import { openPgClient } from './migrate.mjs';

export const REG_LABELS = Object.freeze({ draft: '下書き', ne_pending: 'NE登録待ち', ne_confirmed: 'NE確認済み', distributable: '配る対象', quarantined: '要確認', cancelled: 'やめた' });

/** 1 行を作る (db = { query })。返り値 { line, counts } */
export async function summarize(db) {
  const has = (await db.query(`select to_regclass('ops.product_hub_outbox') is not null and to_regclass('ops.master_registrations') is not null as ok`)).rows[0].ok;
  if (!has) return { line: '⏭️ 新商品の登録: 0051 がまだ (表が無い)', counts: null };
  const reg = Object.fromEntries((await db.query(`select state, count(*)::int as n from ops.master_registrations where state <> 'available' group by state`)).rows.map((r) => [r.state, r.n]));
  const card = Object.fromEntries((await db.query(`select status, n, to_char(oldest_at at time zone 'Asia/Tokyo', 'MM-DD HH24:MI') as oldest from ops.v_product_hub_outbox_open`)).rows.map((r) => [r.status, r]));
  const regText = Object.entries(REG_LABELS).filter(([k]) => reg[k]).map(([k, v]) => `${v} ${reg[k]}`).join('・') || '途中の新商品なし';
  const waiting = (card.pending?.n || 0) + (card.failed?.n || 0);
  const oldest = [card.pending?.oldest, card.failed?.oldest].filter(Boolean).sort()[0] || null;
  const cardText = waiting || card.conflict
    ? `カード作成待ち ${waiting}${card.failed ? ` (失敗 ${card.failed.n})` : ''}${oldest ? `・いちばん古い ${oldest}` : ''}${card.conflict ? `・衝突 ${card.conflict.n}` : ''} → マスタの入力の一覧「カード」で絞る`
    : 'カード作成待ちなし';
  const warn = waiting || card.conflict || reg.quarantined ? '⚠️' : '📦';
  return { line: `${warn} 新商品の登録: ${regText} / ${cardText}`, counts: { registrations: reg, cards: Object.fromEntries(Object.entries(card).map(([k, v]) => [k, v.n])) } };
}

const isMain = process.argv[1] && /master-register-summary\.mjs$/i.test(process.argv[1]);
if (isMain) {
  let code = 1, line = '';
  const url = process.env.COMPANY_DB_WATCH_URL || process.env.COMPANY_DB_URL;
  let client = null;
  try {
    if (!url) throw new Error('COMPANY_DB_WATCH_URL (か COMPANY_DB_URL) が要る');
    client = await openPgClient(url);
    await client.query(`set default_transaction_read_only = on`);
    await client.query(`set statement_timeout = '10s'`);
    const r = await summarize({ query: (t, p) => client.query(t, p) });
    if (process.argv.includes('--json')) console.log(JSON.stringify(r.counts));
    line = r.line; code = 0;
  } catch (e) {
    line = `❌ 新商品の登録のまとめ: ${String(e && e.message).replace(/\s+/g, ' ').slice(0, 300)}`;
  } finally { if (client) { try { await client.end(); } catch { /* */ } } }
  console.log(line);
  process.exitCode = code;
}
