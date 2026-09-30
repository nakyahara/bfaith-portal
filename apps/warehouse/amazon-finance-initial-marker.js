#!/usr/bin/env node
/**
 * amazon-finance-initial-marker.js — 決済のそろいの初期の印 (D-65 案 a) を取り込む CLI と規則 (D7b-1b-3)
 *
 * 設計 = AI_reference システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md D-65・§3.1
 *   Reports API は 90 日より前の決済のレポートを返さない = 「レポートが 1 本まるごと欠けていない」を API だけでは言えない。
 *   → 中原さんが Seller Central の「過去の決済情報」(支払い) を 1 回書き出し、verified_from 〜 verified_through の期待の決済の集合を印にする。
 *   その後は毎回の完全な一覧 (amazon_settlement_report_inventory) を追記で積み上げる (鎖・amazon-finance-coverage.js)。
 *   印の表 = 追記だけの 2 層 (warehouse.db の initial_marker_headers / initial_marker_settlements)。印を直すときは新しい印 (新しい epoch) を作る。
 *   🚨 印が無い間は coverage を complete にしない (fail-closed)。
 *
 * 入力 (JSON か CSV):
 *   JSON = { evidence_kind, verified_from, verified_through, captured_at, note?, settlements: [{ settlement_id, start, end, total, currency, report_id? }] }
 *   CSV  = 1 行目が見出し settlement_id,start,end,total,currency[,report_id] (verified_from などは引数で渡す)
 *   日付だけ (YYYY-MM-DD) = JST の日。verified_from = その日の JST 00:00・verified_through = **次の日** の JST 00:00 (半開区間 [from, through))。
 *   日時 (時差つき ISO) = 実時刻 (UTC に直す)。決済の期間は 日付だけ = jst_date (見出しの JST の日と比べる) / 日時 = time (実時刻の一致)
 *   total = 円 (小数 2 桁まで・"1,234" のカンマは可・負も可) → micro (浮動小数を使わない)
 *   report_id = 保持期間の中で対応を確かめられたものだけ (R21 H1)。空 = null
 *
 * 使い方:
 *   node apps/warehouse/amazon-finance-initial-marker.js --file marker.json                 → 読んで確かめるだけ (dry-run・SQLite の採った見出しと突き合わせる)
 *   node apps/warehouse/amazon-finance-initial-marker.js --file marker.csv --verified-from 2026-01-01 --verified-through 2026-09-21 --captured-at 2026-10-01T10:00:00+09:00 [--evidence-kind seller_central_payments_export]
 *   node apps/warehouse/amazon-finance-initial-marker.js --file marker.json --commit        → 新しい印 (新しい epoch) を書く
 *   node apps/warehouse/amazon-finance-initial-marker.js --list                             → 印の一覧
 * env: DATA_DIR (必須)
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { canonicalSha256 } from '../company-db/canonical-hash.mjs';
import { cmpUtf8, selectDocumentVersions, readVersions } from './amazon-settlement-versions.js';
import { jstDayStartUtc, jstDateOfUtc } from './amazon-finance-coverage.js';
import { normalizeApiTime } from './amazon-settlement-inventory.js';

export const MARKER_FORMAT = 'fim-v1';
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const isRealDate = (s) => DATE_RE.test(s) && new Date(`${s}T00:00:00Z`).toISOString().slice(0, 10) === s;
const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400e3).toISOString().slice(0, 10);
const SETTLEMENT_ID_RE = /^[0-9A-Za-z-]{1,40}$/;

/** 日付 / 日時 → UTC 'YYYY-MM-DDTHH:MM:SSZ'。through = true なら日付は次の日の始まり */
export function markerTime(v, what, { through = false } = {}) {
  const s = String(v ?? '').trim();
  if (isRealDate(s)) return jstDayStartUtc(through ? addDays(s, 1) : s);
  const t = normalizeApiTime(s);
  if (!t || !/Z$/.test(t) || !/[zZ]|[+-]\d{2}:\d{2}$/.test(s)) throw new Error(`${what} は YYYY-MM-DD か時差つきの日時: ${s}`);
  return t;
}
/** 円 (文字) → micro (BigInt)。小数 2 桁まで */
export function yenToMicro(v, what) {
  const s = String(v ?? '').trim().replace(/,/g, '').replace(/^¥/, '');
  const m = /^(-?)(\d+)(?:\.(\d{1,2}))?$/.exec(s);
  if (!m) throw new Error(`${what} は円の数 (小数 2 桁まで): ${v}`);
  const micro = BigInt(m[2]) * 1000000n + BigInt((m[3] || '').padEnd(2, '0') || '0') * 10000n;
  return m[1] ? -micro : micro;
}

/** 入力を確かめて正規化する (例外 = 理由)。戻り = { header, settlements (決済 ID の UTF-8 のバイトの順), detailDigest } */
export function normalizeMarker(input, { now = new Date() } = {}) {
  if (!input || typeof input !== 'object') throw new Error('印の入力が object でない');
  const kind = String(input.evidence_kind || 'seller_central_payments_export');
  if (!/^[a-z0-9_]{3,60}$/.test(kind)) throw new Error(`evidence_kind が不正: ${kind}`);
  const from = markerTime(input.verified_from, 'verified_from');
  const through = markerTime(input.verified_through, 'verified_through', { through: true });
  const captured = markerTime(input.captured_at, 'captured_at');
  if (Date.parse(from) >= Date.parse(through)) throw new Error(`verified_from (${from}) が verified_through (${through}) より前でない`);
  if (Date.parse(captured) > now.getTime() + 10 * 60e3) throw new Error(`captured_at (${captured}) が未来`);
  if (Date.parse(captured) < Date.parse(through) - 86400e3 * 2) throw new Error(`captured_at (${captured}) が verified_through (${through}) より前 = 書き出した後の決済を「無い」と言えない`);
  if (!Array.isArray(input.settlements) || !input.settlements.length) throw new Error('settlements が空');
  const seen = new Set();
  const settlements = input.settlements.map((x, i) => {
    const sid = String(x.settlement_id ?? '').trim();
    if (!SETTLEMENT_ID_RE.test(sid)) throw new Error(`settlements[${i}].settlement_id が不正: ${sid}`);
    if (seen.has(sid)) throw new Error(`決済 ID が 2 回: ${sid}`);
    seen.add(sid);
    const st = String(x.start ?? '').trim(), en = String(x.end ?? '').trim();
    let precision, ps, pe;
    if (isRealDate(st) && isRealDate(en)) { precision = 'jst_date'; ps = st; pe = en; if (ps > pe) throw new Error(`${sid}: 期間が逆 (${ps} 〜 ${pe})`); }
    else { precision = 'time'; ps = markerTime(st, `${sid}.start`); pe = markerTime(en, `${sid}.end`); if (Date.parse(ps) >= Date.parse(pe)) throw new Error(`${sid}: 期間が逆 (${ps} 〜 ${pe})`); }
    const currency = String(x.currency ?? '').trim().toUpperCase();
    if (!/^[A-Z]{3}$/.test(currency)) throw new Error(`${sid}: 通貨が不正: ${x.currency}`);
    const rid = x.report_id == null || String(x.report_id).trim() === '' ? null : String(x.report_id).trim();
    if (rid != null && !/^[0-9A-Za-z-]{1,40}$/.test(rid)) throw new Error(`${sid}: report_id が不正: ${rid}`);
    return { settlement_id: sid, period_start: ps, period_end: pe, period_precision: precision, total_amount_micro: yenToMicro(x.total, `${sid}.total`).toString(), currency, report_id: rid };
  }).sort((a, b) => cmpUtf8(a.settlement_id, b.settlement_id));
  const detailDigest = canonicalSha256(settlements);
  return { header: { evidence_kind: kind, verified_from: from, verified_through: through, captured_at: captured, note: input.note ? String(input.note).slice(0, 500) : null }, settlements, detailDigest };
}

/** 印の digest (coverage の initial_marker_digest)。見出し + 明細 */
export function markerDigest({ markerId, header, sourceFileHash, detailDigest }) {
  return canonicalSha256({ format: MARKER_FORMAT, marker_id: markerId, evidence_kind: header.evidence_kind, verified_from: header.verified_from, verified_through: header.verified_through,
    captured_at: header.captured_at, source_file_hash: sourceFileHash, detail_digest: detailDigest });
}

/** CSV (1 行目が見出し) → settlements */
export function parseMarkerCsv(text) {
  const lines = text.replace(/^﻿/, '').split(/\r?\n/).filter((l) => l.trim() !== '');
  if (!lines.length) throw new Error('CSV が空');
  const head = lines[0].split(',').map((h) => h.trim());
  for (const c of ['settlement_id', 'start', 'end', 'total', 'currency']) if (!head.includes(c)) throw new Error(`CSV の見出しに ${c} が無い (${head.join(',')})`);
  return lines.slice(1).map((l) => { const cols = l.split(','); return Object.fromEntries(head.map((h, i) => [h, (cols[i] ?? '').trim()])); });
}

/** SQLite の採った見出しと突き合わせる (調べ) */
export function compareWithSqlite(db, settlements) {
  const selected = selectDocumentVersions(readVersions(db));
  return settlements.map((m) => {
    const v = selected.get(m.settlement_id);
    if (!v) return { settlement_id: m.settlement_id, status: 'missing_in_sqlite' };
    const s = normalizeApiTime(v.header_start), e = normalizeApiTime(v.header_end);
    const period = m.period_precision === 'time' ? m.period_start === s && m.period_end === e : (s && e && m.period_start === jstDateOfUtc(s) && m.period_end === jstDateOfUtc(e));
    const amount = String(m.total_amount_micro) === String(v.header_total_micro) && m.currency === v.header_currency;
    return { settlement_id: m.settlement_id, status: period && amount ? 'match' : 'differs', sqlite: { start: s, end: e, total_micro: v.header_total_micro, currency: v.header_currency, layer: v.source_layer } };
  });
}

/** 新しい印を書く (1 取引・新しい epoch)。戻り = { markerId, epoch, markerDigest } */
export function insertMarker(db, norm, { sourceFileName = null, sourceFileHash, now = new Date() }) {
  return db.transaction(() => {
    const epoch = (db.prepare(`SELECT COALESCE(MAX(evidence_epoch), 0) + 1 AS n FROM initial_marker_headers`).get().n);
    const markerId = `im-${epoch}-${norm.header.captured_at.slice(0, 10).replace(/-/g, '')}`;
    const digest = markerDigest({ markerId, header: norm.header, sourceFileHash, detailDigest: norm.detailDigest });
    db.prepare(`INSERT INTO initial_marker_headers (marker_id, evidence_epoch, evidence_kind, verified_from, verified_through, captured_at, source_file_name, source_file_hash, settlement_count, detail_digest, marker_digest, created_at, note)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`).run(markerId, epoch, norm.header.evidence_kind, norm.header.verified_from, norm.header.verified_through, norm.header.captured_at,
      sourceFileName, sourceFileHash, norm.settlements.length, norm.detailDigest, digest, now.toISOString(), norm.header.note);
    const ins = db.prepare(`INSERT INTO initial_marker_settlements (marker_id, settlement_id, period_start, period_end, period_precision, total_amount_micro, currency, report_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const s of norm.settlements) ins.run(markerId, s.settlement_id, s.period_start, s.period_end, s.period_precision, BigInt(s.total_amount_micro), s.currency, s.report_id);
    return { markerId, epoch, markerDigest: digest };
  }).immediate();
}

export function parseArgs(argv) {
  const out = { file: null, commit: false, list: false, verifiedFrom: null, verifiedThrough: null, capturedAt: null, evidenceKind: null, note: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const val = () => { const v = argv[++i]; if (v === undefined || String(v).startsWith('--')) throw new Error(`${a} に値が無い`); return v; };
    if (a === '--file') out.file = val();
    else if (a === '--commit') out.commit = true;
    else if (a === '--list') out.list = true;
    else if (a === '--verified-from') out.verifiedFrom = val();
    else if (a === '--verified-through') out.verifiedThrough = val();
    else if (a === '--captured-at') out.capturedAt = val();
    else if (a === '--evidence-kind') out.evidenceKind = val();
    else if (a === '--note') out.note = val();
    else throw new Error(`知らない引数: ${a}`);
  }
  if (!out.list && !out.file) throw new Error('--file か --list');
  return out;
}

/** CLI の本体 (試験から呼ぶ)。db = warehouse.db (書く接続) */
export function runMarkerCli(db, a, { log = console.log, now = new Date() } = {}) {
  if (a.list) {
    const hs = db.prepare(`SELECT * FROM initial_marker_headers ORDER BY evidence_epoch`).all();
    for (const h of hs) log(`  epoch ${h.evidence_epoch} ${h.marker_id}: [${h.verified_from}, ${h.verified_through}) 撮影 ${h.captured_at}・決済 ${h.settlement_count}・digest ${h.marker_digest.slice(0, 12)}…`);
    if (!hs.length) log('  印はまだ無い (coverage は complete にならない)');
    return { markers: hs.length };
  }
  const raw = fs.readFileSync(a.file, 'utf8');
  const fileHash = crypto.createHash('sha256').update(raw, 'utf8').digest('hex');
  let input;
  if (/\.json$/i.test(a.file)) input = JSON.parse(raw);
  else input = { settlements: parseMarkerCsv(raw) };
  for (const [k, v] of [['verified_from', a.verifiedFrom], ['verified_through', a.verifiedThrough], ['captured_at', a.capturedAt], ['evidence_kind', a.evidenceKind], ['note', a.note]]) if (v != null) input[k] = v;
  const norm = normalizeMarker(input, { now });
  log(`[marker] 期間 [${norm.header.verified_from}, ${norm.header.verified_through})・撮影 ${norm.header.captured_at}・決済 ${norm.settlements.length}・明細の digest ${norm.detailDigest.slice(0, 12)}…`);
  const cmp = compareWithSqlite(db, norm.settlements);
  for (const c of cmp) log(`  ${c.status === 'match' ? '✅' : c.status === 'missing_in_sqlite' ? '⚠️ SQLite に無い' : '❌ 違う'} ${c.settlement_id}${c.sqlite ? ` (SQLite ${c.sqlite.start} 〜 ${c.sqlite.end} ${c.sqlite.total_micro} ${c.sqlite.currency} ${c.sqlite.layer})` : ''}`);
  const summary = { match: cmp.filter((c) => c.status === 'match').length, missing: cmp.filter((c) => c.status === 'missing_in_sqlite').length, differs: cmp.filter((c) => c.status === 'differs').length };
  log(`[marker] 突き合わせ: 一致 ${summary.match} / SQLite に無い ${summary.missing} (手で取り込む = amazon-settlement-manual-file.js) / 違う ${summary.differs}`);
  if (!a.commit) { log('[marker] dry-run (書かない)。書くなら --commit'); return { committed: false, norm, summary }; }
  const w = insertMarker(db, norm, { sourceFileName: path.basename(a.file), sourceFileHash: fileHash, now });
  log(`[marker] ✅ 印を書いた: ${w.markerId} (epoch ${w.epoch}・digest ${w.markerDigest.slice(0, 12)}…)。次の coordinator の回から、この印の後の一覧の回を積み上げる`);
  return { committed: true, ...w, norm, summary };
}

const isDirectRun = process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href;
if (isDirectRun) {
  (async () => {
    const a = parseArgs(process.argv.slice(2));
    if (!process.env.DATA_DIR) throw new Error('DATA_DIR が無い');
    const { initDB, getDB } = await import('./db.js');
    await initDB();
    runMarkerCli(getDB(), a);
  })().catch((e) => { console.error(`❌ 初期の印: ${e.message}`); process.exitCode = 1; });
}
