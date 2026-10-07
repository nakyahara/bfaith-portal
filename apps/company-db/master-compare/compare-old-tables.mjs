/**
 * compare-old-tables.mjs — 毎朝のマスタ照合 ②b = Company DB ↔ 古い表 (NE に欄が無い列。マスタ正本切替 ④a・Codex #1564 R1 H3)
 *
 * 何を比べるか: 持ち主が C (Company DB) で、NE の商品マスタに欄が無い列 = ② (NE との照合) では比べられない列:
 *   税区分 (skus.tax_class)・売上分類 (products.sales_class)・送料 3 つ (skus.shipping)・推奨保有月数 (skus.reorder_months)
 *   C の今の値 ↔ 古い表 (m_products の 税区分・売上分類・送料・送料コード・配送方法 / product_shipping / m_reorder_setting)。
 *   比べ方は入れた後の確かめ (master-publish.js の verifyApplied) と同じ (空 = 行が無い・セットの導く列は数だけ)
 * なぜ NE へ直す提案 (to_ne)・判断の台帳 (decisions) を使わないか:
 *   NE にこの欄が無い = NE へ直す先が無い・人が「どちらを正にするか」決める差ではない。古い表は毎朝の作り直しが C の値で書く (④a) ので、
 *   差は「写しの後に C を直した (翌朝の作り直しで入る = 反映待ち)」か「C の値が入っていない (写し・作り直し・古い画面の書き込みの異常)」だけ
 * 分け方 (② の反映待ちと同じ日の期限。台帳 = pending.mjs を別の置き場所で):
 *   今朝の作り直しが使った世代の値 = C の今の値なのに古い表が違う = breach (世代にあった値が入っていない / 後から書き換えられた)
 *   世代の値と C の今の値が違う (写しの後に C を直した) = lag (反映待ち)。始まり = 最初に見た時刻。
 *     期限 = 始まりより後に C を読んだ世代での作り直し (翌朝)。その世代でも違えば overdue (= breach)
 *   台帳が使えない (排他が取れない・信用できない) 朝の lag = held (判定できない = blocked)
 * 比べない朝 (not_applied): 今朝の作り直しが世代を使っていない / 世代の持ち主でこの 4 つが全部 load (今 = 何も読まない・何も書かない)
 * 判定できない朝 (blocked): 作り直しが今朝のもの (同じ daily-sync の回) でない / 作り直しの世代が今朝の写しでない / 世代が読めない
 * 由来: 結果に作り直しの ID・世代 (番号・ID・ハッシュ・C を読んだ時刻・持ち主のハッシュ)。朝の要約 (run.mjs summaryLine) と証跡 master-compare の old_tables
 * 🚨 Company DB には何も書かない (watcher の読み取りの取引の中で読むだけ)。warehouse.db は読み取り専用で開く
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';
import { normSku } from '../../../lib/sku-norm.js';
import { COMPANY_ID } from './compare-load.mjs';
import { readLedger, writeLedger, acquireLock, pendingDir, markWriteFailed } from './pending.mjs';
import { publishValueOf } from '../publish/fetch.mjs';
import { PUBLISH_COLUMNS, PRESENCE_COL, publishCols, entriesOfRows, readCurrentPublish, verifyApplied } from '../../warehouse/master-publish.js';
import { latestBuild, publishOfBuild } from '../../warehouse/master-material.js';

export const OLD_FORMAT = 'mc-old-v1';
/** NE に欄が無い = ② で比べられない列 (④a が古い表に写す列の一部) */
export const OLD_TABLE_COLS = Object.freeze(['tax_class', 'sales_class', 'shipping', 'reorder_months']);
/** 反映待ちの台帳の置き場所 (DATA_DIR/<これ>/pending。② の台帳とは別) */
export const OLD_RESULT_DIR = path.join('cdb-master-compare', 'old-tables');
/** 全件 JSON に載せる反映待ち (lag) の上限 (件数は全部数える)。breach・overdue・held は全部載せる (見張り W13:old が案件にする) */
export const OLD_ITEMS_MAX = 200;
const TO_NE = Object.freeze({ state: 'not_applicable', reason: 'NE に欄が無い (NE へ直す先が無い・判断の台帳の対象にしない)。古い表は毎朝の作り直しが C の値で書く = 差は写しの遅れか異常だけ' });

const jstDate = (iso) => { const ms = Date.parse(iso); return Number.isFinite(ms) ? new Date(ms + 9 * 3600000).toISOString().slice(0, 10) : null; };
const hashOf = (v) => crypto.createHash('sha256').update(JSON.stringify(v ?? null)).digest('hex');
/** 古い表の確かめの列 → 照合の列 (product_shipping は送料の列) */
const COL_OF_PROBLEM = { tax_class: 'tax_class', sales_class: 'sales_class', shipping: 'shipping', product_shipping: 'shipping', reorder_months: 'reorder_months' };

/** C の今の値 (世代と同じ形の entries。この 4 つの列だけ) */
async function readCdbOldCols(db, cols) {
  const skus = (await db.query(`select s.code, s.code_norm, s.sku_kind, s.tax_class, case when s.sku_kind = 'single' then p.sales_class::int end as sales_class,
      s.shipping_code, s.shipping_method, s.shipping_cost_jpy::float8 as shipping_cost_jpy, s.reorder_months::float8 as reorder_months
    from core.skus s left join core.products p on p.product_id = s.product_id where s.company_id = $1 order by s.code_norm`, [COMPANY_ID])).rows;
  const rows = [];
  for (const s of skus) {
    rows.push({ code_norm: s.code_norm, col: PRESENCE_COL, code: s.code, sku_kind: s.sku_kind, value: 'true' });
    for (const col of cols) {
      const v = publishValueOf(s, col, null);
      if (v !== undefined) rows.push({ code_norm: s.code_norm, col, code: s.code, sku_kind: s.sku_kind, value: JSON.stringify(v) });
    }
  }
  return entriesOfRows(rows);
}

/**
 * ②b の本体 (run.mjs が ② の後に、同じ読み取りの取引の中で呼ぶ)。台帳は自分で排他を取って読み・書く (② の台帳とは別)
 * @param {object} p
 * @param {{ query: Function }} p.db   Company DB (読み取りの取引の中)
 * @param {string} p.dataDir           miniPC の DATA_DIR (warehouse.db・台帳)
 * @param {string} p.asOfJst
 * @param {string|null} p.syncRunId
 * @param {string} p.cdbReadAt         この取引で C を読んだ時刻 (ISO)
 * @param {string} p.compareRunId      台帳の版の名前
 * @param {object} [p.taxRates]        試験で差し替える (既定 = rebuild-m-products.js の TAX_RATES)
 * @returns {Promise<object>} 全件 JSON の old_tables 節
 */
export async function compareOldTables({ db, dataDir, asOfJst, syncRunId = null, cdbReadAt, compareRunId, now = new Date(), taxRates = null, openSqlite = null }) {
  const out = { format: OLD_FORMAT, verdict: null, reason: null, cols: [], to_ne: TO_NE, build: null, publish: null,
    counts: { keys: 0, checked: 0, derived: 0, not_in_cdb: 0, not_in_ne: 0, mismatches: 0, breach: 0, overdue: 0, lag: 0, held: 0 }, items: [], pending: null };
  const done = (verdict, reason = null) => Object.assign(out, { verdict, reason });
  const file = path.join(dataDir, 'warehouse.db');
  if (!openSqlite && !fs.existsSync(file)) return done('not_applied', 'no_warehouse_db');
  const sqlite = openSqlite ? openSqlite() : new Database(file, { readonly: true, fileMustExist: true });
  try {
    // ── 1. 今朝の作り直しと、それが使った世代 (持ち主) ──
    let build;
    try { build = latestBuild(sqlite); } catch { return done('not_applied', 'no_build_table'); }
    const bp = publishOfBuild(build);
    if (!build || !bp) return done('not_applied', build ? 'build_without_generation' : 'no_build');
    let gen;
    try { gen = sqlite.prepare('SELECT * FROM cdb_publish_generations WHERE generation_no = ?').get(bp.generation_no) || null; } catch { gen = null; }
    if (!gen) return done('blocked', 'generation_missing');
    let ownership;
    try { ownership = JSON.parse(gen.ownership); } catch { return done('blocked', 'generation_ownership_unreadable'); }
    out.cols = publishCols(ownership).filter((c) => OLD_TABLE_COLS.includes(c));
    out.build = { build_id: build.build_id, published_at: build.published_at, daily_sync_run_id: build.daily_sync_run_id ?? null };
    out.publish = { generation_no: bp.generation_no, generation_id: bp.generation_id, content_hash: bp.content_hash, applied_hash: bp.applied_hash, ownership_hash: bp.ownership_hash,
      cdb_read_at: gen.cdb_read_at };
    if (!out.cols.length) return done('not_applied', 'load_owned');   // 今 = 全部 load (C の値を読まない・台帳も書かない)
    // ── 2. 鮮度 (② と同じ: 作り直しが今朝・同じ daily-sync の回。世代も今朝の写し) ──
    if (jstDate(build.published_at) !== asOfJst || (syncRunId && build.daily_sync_run_id !== syncRunId)) return done('blocked', 'stale_build');
    if (jstDate(gen.cdb_read_at) !== asOfJst) return done('blocked', 'stale_generation');   // 写しが落ちた朝 = 前の世代で作り直した = 反映待ちの期限を数えられない
    // 作り直しが使った世代の値 (今の世代の印が動いていれば読めない = 判定しない)
    const pub = readCurrentPublish(sqlite);
    if (pub.problem || pub.generation.generation_no !== bp.generation_no) return done('blocked', pub.problem ? `publication_${pub.problem}` : 'generation_moved');
    // ── 3. C の今の値 ↔ 古い表 (入れた後の確かめと同じ比べ方。持ち主はこの 4 つのうち C の列だけ) ──
    const entries = await readCdbOldCols(db, out.cols);
    const own = Object.fromEntries(out.cols.map((c) => [PUBLISH_COLUMNS[c], 'company']));
    const rates = taxRates || (await import('../../warehouse/rebuild-m-products.js')).TAX_RATES;
    // 区分の持ち主が C の世代 = 作り直しが区分を写した (構成の行の無い C のセットは C の値をそのまま = 導いたセットとして読まない)。区分そのものは ② の kind で比べる
    const v = verifyApplied(sqlite, { publication: { entries }, ownership: own, taxRates: rates, maxProblems: Infinity, kindCopied: ownership['skus.sku_kind'] === 'company' });
    Object.assign(out.counts, { keys: v.counts.keys, checked: v.counts.checked, derived: v.counts.derived, not_in_cdb: v.counts.not_in_cdb, not_in_ne: v.counts.not_in_ne });
    // ── 4. 分ける (世代にあった値 = breach / 写しの後に直した = lag・期限は台帳) ──
    const release = (() => { try { return acquireLock(pendingDir(dataDir, OLD_RESULT_DIR)); } catch { return null; } })();
    try {
      const ledger = release ? readLedger(dataDir, OLD_RESULT_DIR) : { state: 'locked', reason: 'pending/.lock がある', head: null, entries: new Map() };
      const ledgerOk = ledger.state === 'ok' || ledger.state === 'initial';
      out.pending = { state: ledger.state, reason: ledger.reason ?? null };
      const genReadMs = Date.parse(gen.cdb_read_at);
      const newPending = new Map();
      const seen = new Set();
      for (const p of v.problems) {
        const col = COL_OF_PROBLEM[p.col];
        const norm = p.code ? normSku(String(p.code).split(',')[0]) : null;
        if (!col || !norm || String(p.code).includes(',')) {   // 正規化の重なり・知らない列 = 異常 (breach)
          out.counts.breach++; out.counts.mismatches++;
          out.items.push({ code: p.code, col: p.col, c: p.expected, old: p.actual, class: 'breach', why: 'not_comparable' });
          continue;
        }
        const k = `${norm}|${col}`;
        if (seen.has(k)) continue;   // 送料 (m_products と product_shipping) は 1 件に数える
        seen.add(k);
        out.counts.mismatches++;
        const target = entries.get(norm)?.v?.[col];
        const genV = pub.entries.get(norm)?.v?.[col];
        const targetHash = hashOf(target);
        const unit = `${k}|${targetHash}`;
        let cls, why, prev = null;
        if (genV !== undefined && hashOf(genV) === targetHash) { cls = 'breach'; why = 'generation_had_value'; }   // 世代にあった値が古い表に無い
        else if (!ledgerOk) { cls = 'held'; why = `pending_${ledger.state}`; }
        else {
          prev = ledger.entries.get(unit) || null;
          if (prev && genReadMs > Date.parse(prev.start_at)) { cls = 'overdue'; why = 'past_next_build'; }   // 始まりより後に C を読んだ世代でも違う
          else {
            cls = 'lag'; why = 'changed_after_publish';
            newPending.set(unit, { unit, key: norm, col, target_hash: targetHash, start_generation: prev?.start_generation ?? bp.generation_id, start_at: prev?.start_at ?? cdbReadAt });
          }
        }
        out.counts[cls]++;
        if (cls !== 'lag' || out.counts.lag <= OLD_ITEMS_MAX) {
          out.items.push({ code: p.code, norm, col, c: target ?? null, generation_value: genV ?? null, old: p.actual, class: cls, why,
            start_at: newPending.get(unit)?.start_at ?? prev?.start_at ?? null, start_generation: newPending.get(unit)?.start_generation ?? prev?.start_generation ?? null });
        }
      }
      // 台帳 = 今朝の lag だけ (消えた差は台帳から外れる = 回復)。信用できるときだけ新しい版
      if (ledgerOk) {
        try {
          const w = writeLedger(dataDir, OLD_RESULT_DIR, { compareRunId, ledger, entries: [...newPending.values()], now });
          out.pending.written = { compare_run_id: w.compare_run_id, sha256: w.sha256, entries: newPending.size };
        } catch (e) {
          const msg = String(e && e.message).slice(0, 200);
          const marked = markWriteFailed(dataDir, OLD_RESULT_DIR, { compare_run_id: compareRunId, error: msg });
          Object.assign(out.pending, { state: 'write_failed', reason: `台帳を保存できない (${msg})${marked ? '' : '・失敗の印も書けない'}` });
        }
      }
    } finally { if (release) release(); }
    const c = out.counts;
    if (c.breach || c.overdue) return done('breach');
    if (c.held) return done('blocked', `pending_${out.pending.state}`);
    if (out.pending.state === 'write_failed') return done('blocked', 'pending_write_failed');
    return done(c.lag ? 'lag' : 'pass');
  } finally { try { if (!openSqlite) sqlite.close(); } catch { /* */ } }
}

/** 朝の要約の ②b (比べない朝 = null = 要約に出さない) */
export function oldTablesSummary(o) {
  if (!o || o.verdict === 'not_applied') return null;
  if (o.verdict === 'error') return `⚠️ ②b 古い表: 照合が落ちた (${String(o.error || '').slice(0, 120)})`;
  if (o.verdict === 'blocked') return `⚠️ ②b 古い表: 判定できない (${o.reason})`;
  const c = o.counts || {};
  const by = {};
  for (const it of o.items || []) if (it.class === 'breach' || it.class === 'overdue') by[it.col] = (by[it.col] || 0) + 1;
  const where = `世代 ${o.publish?.generation_no ?? '?'}・作り直し ${o.build?.build_id ?? '?'}`;
  if (o.verdict === 'breach') {
    return `⚠️ ②b 古い表: C の値が入っていない ${c.breach + c.overdue} 件 (${Object.entries(by).map(([k, n]) => `${k} ${n}`).join(' / ')}・${where})${c.lag ? `・反映待ち ${c.lag}` : ''}`;
  }
  if (o.verdict === 'lag') return `ℹ️ ②b 古い表: 反映待ち ${c.lag} 件 (写しの後に C を直した = 翌朝の作り直しで入る・${where})`;
  return `✅ ②b 古い表: 差 0 (列 ${o.cols.join('・')}・SKU ${c.keys}・${where})`;
}
/** 朝の要約の先頭に出すか (⚠️) */
export const oldTablesBad = (o) => !!o && (o.verdict === 'error' || o.verdict === 'blocked' || o.verdict === 'breach');
