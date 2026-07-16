#!/usr/bin/env node
/**
 * fetch-rakuten-review-contacts.js — フォローメール用 連絡先取得 CLI (mall-csv-fetcher P2 PR-B)
 *
 * 楽天ペイ受注API (searchOrder dateType=発送日 → getOrder v7) から
 * 「注文番号+マスクメールアドレス+最終発送日」だけを allowlist 抽出し、
 * AES-256-GCM で暗号化して rakuten_order_contacts に UPSERT する。
 * 氏名・住所・電話番号など他のPIIは一切保存しない。宛先はログにも出さない。
 *
 * サブコマンド:
 *   fetch [--days 25]       発送日が直近N日の注文の連絡先を取得 (既定。終了後に purge も実行)
 *   purge                   contact_delete_at 超過の暗号文を null 化
 *   suppress --email <addr> [--reason <text>]   配信停止登録 (HMACのみ保存。解除は手動SQLで明示的に)
 *   stats                   件数サマリ (宛先は表示しない)
 *
 * env: DATA_DIR (必須) / RAKUTEN_SERVICE_SECRET / RAKUTEN_LICENSE_KEY (fetch時) /
 *      CONTACTS_ENC_KEY / CONTACTS_HMAC_KEY (gen-review-contact-keys.js で生成)
 * exit code: 0=成功 / 1=失敗 / 2=env・引数エラー
 */
import 'dotenv/config';
import path from 'node:path';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { rakutenRequest } from './rakuten-client.js';
import {
  ensureContactTables, loadContactKeys, extractContact, upsertContacts,
  purgeExpiredContacts, addSuppression,
} from './rakuten-review-contacts-lib.js';

const args = process.argv.slice(2);
function getArg(flag) { const i = args.indexOf(flag); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; }
const DATA_DIR = (process.env.DATA_DIR || getArg('--data-dir') || '').trim();
// mode は先頭引数のみ (フラグ値をmodeと誤認しない — feedback_runScript_quote_trap と同系の罠)
const mode = (args[0] && !args[0].startsWith('--')) ? args[0] : 'fetch';

if (!DATA_DIR) { console.error('FATAL: DATA_DIR is required'); process.exit(2); }
const dbPath = path.join(DATA_DIR, 'warehouse.db');
if (!fs.existsSync(dbPath)) { console.error(`FATAL: warehouse.db not found at ${dbPath}`); process.exit(2); }

const db = new Database(dbPath);
db.pragma('journal_mode = WAL');
db.pragma('busy_timeout = 5000');
ensureContactTables(db);

async function callRMS(endpoint, body) {
  const result = await rakutenRequest({ path: `/es/2.0/order/${endpoint}/`, method: 'POST', body });
  if (result.status < 200 || result.status >= 300) throw new Error(`RMS API ${endpoint} HTTP ${result.status}`);
  const data = result.data;
  // レスポンス本文はログに出さない (PII対策、rakuten-orders.js と同方針)
  if (data && data.MessageModelList) {
    const errors = data.MessageModelList.filter((m) => m.messageType === 'ERROR');
    // message本文は含めない (楽天側メッセージに注文番号等が含まれ得る — Codex R1 medium)。codeのみ
    if (errors.length > 0) throw new Error(`RMS API ${endpoint}: ${errors.map((e) => e.messageCode).join(', ')} (本文はPII対策で非表示)`);
  }
  return data;
}

async function runFetch() {
  const keys = loadContactKeys();
  const days = Math.min(Math.max(parseInt(getArg('--days'), 10) || 25, 1), 63);
  const end = new Date(Date.now() + 9 * 3600 * 1000);
  const start = new Date(end.getTime() - (days - 1) * 86400000);
  const startDate = start.toISOString().slice(0, 10);
  const endDate = end.toISOString().slice(0, 10);
  console.log(`[contacts] 発送日 ${startDate} 〜 ${endDate} (${days}日) の連絡先を取得`);

  // searchOrder: dateType=4 (発送日)。キャンセル系 (800,900) は対象外
  const orderNumbers = [];
  let page = 1, totalPages = 1;
  do {
    const data = await callRMS('searchOrder', {
      dateType: 4,
      startDatetime: `${startDate}T00:00:00+0900`,
      endDatetime: `${endDate}T23:59:59+0900`,
      orderProgressList: [300, 400, 500, 600, 700],
      PaginationRequestModel: { requestRecordsAmount: 1000, requestPage: page },
    });
    orderNumbers.push(...(data.orderNumberList || []));
    totalPages = data.PaginationResponseModel?.totalPages || 1;
    console.log(`[contacts] searchOrder page ${page}/${totalPages}: 累計 ${orderNumbers.length}件`);
    page++;
  } while (page <= totalPages);

  if (orderNumbers.length === 0) {
    console.log('[contacts] 対象注文なし (正常終了)');
    return { fetched: 0, inserted: 0, updated: 0, skipped: 0 };
  }

  let inserted = 0, updated = 0, skipped = 0, noEmail = 0;
  for (let i = 0; i < orderNumbers.length; i += 100) {
    const batch = orderNumbers.slice(i, i + 100);
    console.log(`[contacts] getOrder: ${i + 1}〜${Math.min(i + 100, orderNumbers.length)} / ${orderNumbers.length}`);
    const data = await callRMS('getOrder', { orderNumberList: batch, version: 7 });
    const records = [];
    for (const order of data.OrderModelList || []) {
      const rec = extractContact(order, keys);
      if (!rec) { skipped++; continue; }
      if (!rec.masked_email_enc) noEmail++;
      records.push(rec);
    }
    const r = upsertContacts(db, records);
    inserted += r.inserted;
    updated += r.updated;
  }
  console.log(`[contacts] 完了: insert ${inserted} / update ${updated} / 抽出不能スキップ ${skipped} / メールなし ${noEmail}`);

  const purged = purgeExpiredContacts(db);
  if (purged > 0) console.log(`[contacts] purge: 期限超過 ${purged}件の暗号文を削除`);
  return { fetched: orderNumbers.length, inserted, updated, skipped };
}

function runStats() {
  const t = db.prepare(`
    SELECT COUNT(*) total,
           SUM(CASE WHEN purged_at IS NULL AND masked_email_enc IS NOT NULL THEN 1 ELSE 0 END) active,
           SUM(CASE WHEN purged_at IS NOT NULL THEN 1 ELSE 0 END) purged,
           MIN(shipping_datetime) mn, MAX(shipping_datetime) mx
    FROM rakuten_order_contacts
  `).get();
  const s = db.prepare(`SELECT COUNT(*) c FROM rakuten_contact_suppressions`).get();
  console.log(`contacts: total=${t.total} active=${t.active} purged=${t.purged} shipping=[${t.mn || '-'} .. ${t.mx || '-'}]`);
  console.log(`suppressions: ${s.c}件`);
}

try {
  if (mode === 'fetch') {
    await runFetch();
  } else if (mode === 'purge') {
    const purged = purgeExpiredContacts(db);
    console.log(`[contacts] purge: ${purged}件`);
  } else if (mode === 'suppress') {
    const email = getArg('--email');
    if (!email) { console.error('FATAL: --email <addr> が必要'); process.exit(2); }
    const keys = loadContactKeys();
    const hash = addSuppression(db, email, getArg('--reason') || 'manual', keys);
    console.log(`[contacts] 配信停止に登録しました (hash=${hash.slice(0, 12)}…。生アドレスは保存していません)`);
  } else if (mode === 'stats') {
    runStats();
  } else {
    console.error(`FATAL: unknown mode '${mode}' (fetch / purge / suppress / stats)`);
    process.exitCode = 2;
  }
} catch (e) {
  console.error(`FATAL: ${e.message}`);
  process.exitCode = 1;
} finally {
  db.close();
}
