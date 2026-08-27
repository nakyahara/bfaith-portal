#!/usr/bin/env node
/**
 * test-review-engine-malls.mjs — PR-Y-0 スモーク: モール別エンジン (createCampaignEngine / createSenderEngine)
 *   ① Yahoo 束縛でテーブル名が yahoo_* になり、楽天テーブルに触れない
 *   ② 楽天互換エクスポートが従来どおり rakuten_* を使う (同一 DB に両モールが共存できる)
 *   ③ sender の宛先解決アダプタ: async / retryable → claim 解放 (attempt 行も消える) → 次回送れる / 非 retryable → failed_safe
 * 実行: node apps/warehouse/test-review-engine-malls.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { MALL_TABLES, tablesFor, createCampaignEngine, ensureCampaignTables, planCampaigns, getCutover } from './rakuten-review-campaign-lib.js';
import { createSenderEngine, RAKUTEN_SENDER_ADAPTER, selectEligibleActions } from './rakuten-review-sender-lib.js';
import { ensureContactTables, loadContactKeys, encryptEmail, hmacEmail, hmacOrderKey, upsertContacts } from './rakuten-review-contacts-lib.js';
import { ensureRakutenReviewTables } from './rakuten-review-lib.js';
import { ensureCouponRegistry } from './rakuten-coupon-lib.js';

let passed = 0, failed = 0;
const check = (name, cond, detail = '') => { if (cond) { console.log(`  ✅ ${name}`); passed++; } else { console.log(`  ❌ ${name} ${detail}`); failed++; } };

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'review-malls-'));
const db = new Database(path.join(tmp, 'warehouse.db'));
db.pragma('foreign_keys = ON');
const NOW = '2026-09-13T03:00:00.000Z';

console.log('=== 1. テーブル束縛 ===');
{
  const Y = createCampaignEngine('yahoo');
  const R = createCampaignEngine('rakuten');
  check('tablesFor: 未知モールは throw', (() => { try { tablesFor('amazon'); return false; } catch { return true; } })());
  check('MALL_TABLES は凍結', Object.isFrozen(MALL_TABLES) && Object.isFrozen(MALL_TABLES.yahoo));
  // Yahoo 側は contacts/reviews 表も yahoo_* 名で最低限用意 (planner が JOIN するため)
  db.exec(`CREATE TABLE yahoo_order_contacts (order_number TEXT PRIMARY KEY, order_key_hmac TEXT, masked_email_enc TEXT, masked_email_hash TEXT,
    order_datetime TEXT, shipping_datetime TEXT, order_progress INTEGER, contact_delete_at TEXT, fetched_at TEXT, purged_at TEXT, deleted_at TEXT)`);
  db.exec(`CREATE TABLE yahoo_contact_suppressions (email_hash TEXT PRIMARY KEY, reason TEXT, created_at TEXT)`);
  db.exec(`CREATE TABLE fact_yahoo_reviews (review_url TEXT PRIMARY KEY, order_number TEXT, rating INTEGER, posted_at TEXT, first_seen_at TEXT, is_deleted INTEGER DEFAULT 0)`);
  Y.ensureCampaignTables(db);
  const names = db.prepare(`SELECT name FROM sqlite_master WHERE type IN ('table','index') ORDER BY name`).all().map((r) => r.name);
  check('Yahoo エンジンは yahoo_* テーブル/idx_yca_* だけを作る',
    names.includes('yahoo_campaign_actions') && names.includes('yahoo_order_campaign_ownership') && names.includes('yahoo_campaign_meta')
    && names.some((n) => n.startsWith('idx_yca_')) && !names.some((n) => n.startsWith('rakuten_')) && !names.some((n) => n.startsWith('idx_rca_')), names.join(','));
  db.prepare(`INSERT INTO yahoo_order_contacts (order_number, shipping_datetime, masked_email_enc, fetched_at) VALUES ('b-faith01-1', '2026-09-01T12:00:00+09:00', '(api)', ?)`).run(NOW);
  const c = Y.planCampaigns(db, { nowIso: NOW, couponEpochOverride: '2026-08-01T00:00:00.000Z' });
  check('Yahoo plan: フォロー action が yahoo_campaign_actions に入る', c.followInserted === 1
    && db.prepare(`SELECT COUNT(*) n FROM yahoo_campaign_actions`).get().n === 1);
  check('Yahoo plan: ownership は shadow (vendor)', db.prepare(`SELECT owner, reason FROM yahoo_order_campaign_ownership`).get()?.reason === 'shadow');
  check('Yahoo stats/getCutover が動く', Y.campaignStats(db, NOW).byStatus.length === 1 && Y.getCutover(db).stage === 'shadow');

  // 楽天互換エクスポートは rakuten_* を作り、yahoo_* に触れない
  ensureRakutenReviewTables(db); ensureContactTables(db); ensureCampaignTables(db); R.ensureCampaignTables(db); ensureCouponRegistry(db);
  const names2 = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' ORDER BY name`).all().map((r) => r.name);
  check('楽天互換: rakuten_campaign_actions が別に存在', names2.includes('rakuten_campaign_actions') && names2.includes('yahoo_campaign_actions'));
  planCampaigns(db, { nowIso: NOW, couponEpochOverride: '2026-08-01T00:00:00.000Z' });
  check('楽天 plan は yahoo_ の action を増やさない', db.prepare(`SELECT COUNT(*) n FROM yahoo_campaign_actions`).get().n === 1
    && db.prepare(`SELECT COUNT(*) n FROM rakuten_campaign_actions`).get().n === 0 && getCutover(db).stage === 'shadow');
}

console.log('=== 2. sender アダプタ (Yahoo 束縛・宛先は API 即時取得を模擬) ===');
{
  const sent = [];
  let apiCalls = 0, apiMode = 'retryable';
  const Y = createSenderEngine({
    mall: 'yahoo',
    monthlyCouponFor: () => null,
    resolveRecipient: async (_db, action) => {
      apiCalls++;
      if (apiMode === 'retryable') { const e = new Error('429'); e.retryable = true; e.code = 'api_429'; throw e; }
      if (apiMode === 'fatal') { const e = new Error('order gone'); e.code = 'order_not_found'; throw e; }
      return `user-${action.order_number}@example.com`;
    },
    buildMail: (a) => ({ subject: `S ${a.order_number}`, text: `T ${a.order_number}` }),
    fromHeader: '"雑貨イズム" <info@b-faith.biz>',
  });
  check('createSenderEngine: Yahoo 束縛', Y.adapter.fromHeader.includes('info@') && Y.tables.actions === 'yahoo_campaign_actions'
    && RAKUTEN_SENDER_ADAPTER.mall === 'rakuten');
  check('createSenderEngine: 楽天以外でモール固有項目が欠けると throw (楽天既定を継承しない)',
    (() => { try { createSenderEngine({ mall: 'yahoo', resolveRecipient: async () => 'x' }); return false; } catch (e) { return /buildMail|monthlyCouponFor|fromHeader/.test(e.message); } })()
    && (() => { try { createSenderEngine({}); return false; } catch { return true; } })()
    && createSenderEngine({ mall: 'rakuten', fromHeader: 'X <x@b-faith.biz>' }).adapter.fromHeader.startsWith('X'));
  // ready なフォロー action + ownership self
  const T = tablesFor('yahoo');
  db.prepare(`INSERT OR REPLACE INTO ${T.ownership} (order_number, owner, reason, decided_at) VALUES ('b-faith01-1', 'self', 'test', ?)`).run(NOW);
  db.prepare(`UPDATE ${T.actions} SET status = 'ready', scheduled_at = '2026-09-11T03:00:00.000Z', ready_at = ? WHERE order_number = 'b-faith01-1'`).run(NOW);
  const sel = Y.selectEligibleActions(db, { nowIso: NOW, limit: 10 });
  check('Yahoo gate: self + 期限内 + 発送日一致 → eligible 1', sel.eligible.length === 1, JSON.stringify(sel.skipped));
  const sendFn = async (m) => { sent.push(m); };
  const keys = loadContactKeys({ CONTACTS_ENC_KEY: crypto.randomBytes(32).toString('hex'), CONTACTS_HMAC_KEY: crypto.randomBytes(32).toString('hex') });
  const r1 = await Y.processReadyActions(db, { keys, sendFn, nowIso: NOW, limit: 10 });
  const st = () => db.prepare(`SELECT status, claim_token FROM ${T.actions} WHERE order_number = 'b-faith01-1'`).get();
  check('retryable: 送信せず claim を解放 (ready に戻り attempt 行なし)', r1.recipientRetry === 1 && r1.sent === 0 && sent.length === 0
    && st().status === 'ready' && st().claim_token === null
    && db.prepare(`SELECT COUNT(*) n FROM ${T.attempts}`).get().n === 0, JSON.stringify(r1));
  apiMode = 'ok';
  const r2 = await Y.processReadyActions(db, { keys, sendFn, nowIso: NOW, limit: 10 });
  check('次回: 宛先取得成功 → 送信 (at-most-once 維持)', r2.sent === 1 && sent.length === 1 && sent[0].to === 'user-b-faith01-1@example.com'
    && sent[0].from.includes('info@') && st().status === 'sent'
    && db.prepare(`SELECT outcome FROM ${T.attempts}`).get().outcome === 'accepted', JSON.stringify(r2));
  check('宛先はログ/戻り値に出ない', !JSON.stringify(r2).includes('example.com'));
  // 非 retryable → failed_safe
  db.prepare(`INSERT INTO yahoo_order_contacts (order_number, shipping_datetime, masked_email_enc, fetched_at) VALUES ('b-faith01-2', '2026-09-01T12:00:00+09:00', '(api)', ?)`).run(NOW);
  db.prepare(`INSERT INTO ${T.ownership} (order_number, owner, reason, decided_at) VALUES ('b-faith01-2', 'self', 'test', ?)`).run(NOW);
  createCampaignEngine('yahoo').planCampaigns(db, { nowIso: NOW });
  db.prepare(`UPDATE ${T.actions} SET status = 'ready', scheduled_at = '2026-09-11T03:00:00.000Z', ready_at = ? WHERE order_number = 'b-faith01-2'`).run(NOW);
  apiMode = 'fatal';
  const r3 = await Y.processReadyActions(db, { keys, sendFn, nowIso: NOW, limit: 10 });
  check('非 retryable → failed_safe 終端 (note=エラーコード)', r3.failedSafe === 1 && sent.length === 1
    && db.prepare(`SELECT status FROM ${T.actions} WHERE order_number = 'b-faith01-2'`).get().status === 'failed_safe'
    && db.prepare(`SELECT note FROM ${T.attempts} WHERE action_id = (SELECT id FROM ${T.actions} WHERE order_number = 'b-faith01-2')`).get().note === 'order_not_found');
  // releaseClaim の安全弁: token 必須 / 他人の token では解放できない / attempt が触られていたら解放しない
  db.prepare(`INSERT INTO yahoo_order_contacts (order_number, shipping_datetime, masked_email_enc, fetched_at) VALUES ('b-faith01-3', '2026-09-01T12:00:00+09:00', '(api)', ?)`).run(NOW);
  db.prepare(`INSERT INTO ${T.ownership} (order_number, owner, reason, decided_at) VALUES ('b-faith01-3', 'self', 'test', ?)`).run(NOW);
  createCampaignEngine('yahoo').planCampaigns(db, { nowIso: NOW });
  db.prepare(`UPDATE ${T.actions} SET status = 'ready', scheduled_at = '2026-09-11T03:00:00.000Z', ready_at = ? WHERE order_number = 'b-faith01-3'`).run(NOW);
  const id3 = db.prepare(`SELECT id FROM ${T.actions} WHERE order_number = 'b-faith01-3'`).get().id;
  const cl = Y.claimActionGuarded(db, id3, NOW);
  check('releaseClaim: token 無しは throw', (() => { try { Y.releaseClaim(db, { actionId: id3 }); return false; } catch (e) { return /claimToken/.test(e.message); } })());
  check('releaseClaim: 他人の token では解放しない (claimed のまま)', Y.releaseClaim(db, { actionId: id3, claimToken: 'not-mine', nowIso: NOW }) === false
    && db.prepare(`SELECT status FROM ${T.actions} WHERE id = ?`).get(id3).status === 'claimed');
  db.prepare(`UPDATE ${T.attempts} SET note = 'touched' WHERE action_id = ?`).run(id3);
  check('releaseClaim: attempt が触られていたら解放せずロールバック', Y.releaseClaim(db, { actionId: id3, claimToken: cl.claimToken, nowIso: NOW }) === false
    && db.prepare(`SELECT status FROM ${T.actions} WHERE id = ?`).get(id3).status === 'claimed'
    && db.prepare(`SELECT COUNT(*) n FROM ${T.attempts} WHERE action_id = ?`).get(id3).n === 1);
  db.prepare(`UPDATE ${T.attempts} SET note = NULL WHERE action_id = ?`).run(id3);
  check('releaseClaim: 正しい token + 未使用 attempt → 解放', Y.releaseClaim(db, { actionId: id3, claimToken: cl.claimToken, nowIso: NOW }) === true
    && db.prepare(`SELECT status FROM ${T.actions} WHERE id = ?`).get(id3).status === 'ready'
    && db.prepare(`SELECT COUNT(*) n FROM ${T.attempts} WHERE action_id = ?`).get(id3).n === 0);
  check('楽天側の rakuten_campaign_delivery_attempts は空のまま', db.prepare(`SELECT COUNT(*) n FROM rakuten_campaign_delivery_attempts`).get().n === 0);
  check('楽天互換 selectEligibleActions は rakuten_ を見る (0件)', selectEligibleActions(db, { nowIso: NOW, limit: 10 }).eligible.length === 0);
}

console.log(`\n結果: ${passed} PASS / ${failed} FAIL`);
db.close();
fs.rmSync(tmp, { recursive: true, force: true });
process.exit(failed > 0 ? 1 : 0);
