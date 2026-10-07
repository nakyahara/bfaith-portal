/**
 * lz-import-plan.mjs — ロジザードの毎日の商品マスタの取込の「何を・いつ」(純粋。マスタ正本切替 ③c-1b-2a)
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b 契約 v3」と v2 §1・§3。
 *   - 取込は miniPC の 00:20 の定時の回だけ。始めてよい時刻 = JST 00:15〜00:55 (v2 §1)
 *   - 対象 = **前の日 (JST) の lz-daily の正式な証跡 1 つだけ** (daily-sync の回 = sync_run_id あり)。それより前の日は探さない (R0-7)
 *   - 取り込む前に、CSV の全部の商品 ID が直前の書き出しにあり・削除されていないこと (1 つでも欠ける = その夜は取り込まない。L-7 = 無い ID の行はエラー = 一部だけの取込になる)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import iconv from 'iconv-lite';
import { readEvidence } from '../company-db/push/evidence.mjs';
import { parseCsvBytes } from './lz-compare.mjs';

export const WINDOW = Object.freeze({ fromMin: 15, toMin: 55 });   // JST 00:15〜00:55
const sha256 = (b) => crypto.createHash('sha256').update(b).digest('hex');
const jst = (d) => new Date(d.getTime() + 9 * 3600 * 1000);

/** JST の日付 (YYYY-MM-DD) */
export const jstDateOf = (now) => jst(now).toISOString().slice(0, 10);
/** 対象の lz-daily の日 = 前の日 (JST) */
export const targetAsOf = (now) => jstDateOf(new Date(now.getTime() - 86400000));
/** 取込を始めてよい時刻か (JST 00:15 以上 00:55 未満) */
export function inWindow(now) {
  const j = jst(now);
  const min = j.getUTCHours() * 60 + j.getUTCMinutes();
  return min >= WINDOW.fromMin && min < WINDOW.toMin;
}

/**
 * 対象の lz-daily を選ぶ (前の日の正式な証跡だけ)。
 * @param {object} p
 * @param {boolean} [p.requirePass]  本番の取込 = true (合格だけ)。影の取込 = false (合否は記録するだけ)
 * @param {string} [p.asOf]  対象の日 (**手の試しだけ**。定時は必ず前の日 = 渡さない)
 * @returns {{ ok: boolean, reason: string|null, asOf: string, evidence?: object, csvPath?: string, csvBuf?: Buffer }}
 */
/** 送る前の版 (lzd-v2・ポータルに送らない) の証跡を影だけ許す、対象の日の最後 (③c-1b-3b-3 を入れる前後の 2 日だけ。Codex #1540 R2 Medium) */
export const LEGACY_V2_UNTIL = '2026-09-30';

export function pickTarget({ dataDir, now, requirePass = true, asOf = targetAsOf(now), legacyV2Until = LEGACY_V2_UNTIL }) {
  const ev = readEvidence(dataDir, asOf)['lz-daily'];   // daily-sync の回の名前 (手の回 = lz-daily.manual は見ない)
  const no = (reason, extra = {}) => ({ ok: false, reason, asOf, ...extra });
  if (!ev) return no('no_evidence');
  if (ev.error) return no('evidence_unreadable');
  if (!ev.sync_run_id) return no('not_daily_sync');
  if (ev.state !== 'complete') return no(`not_complete_${ev.state || 'unknown'}`);   // running / skipped = その日は作れていない (前の日を探さない)
  if (ev.as_of !== asOf) return no('as_of_mismatch');
  if (requirePass && ev.verdict !== 'pass') return no('not_pass', { evidence: ev });
  // ポータルに送れた成果物だけ (③c-1b-3b 契約 K3-1。Codex #1540 R1 High)。
  // 経過措置: 送る前の版 (lzd-v2) は、影 (requirePass = false) で・対象の日が LEGACY_V2_UNTIL までの証跡だけ許す (期限つき)
  const stored = !!(ev.portal && ev.portal.ok === true);
  const legacy = !requirePass && ev.version === 'lzd-v2' && typeof ev.as_of === 'string' && ev.as_of <= legacyV2Until;
  if (!stored && !legacy) return no('portal_not_stored', { evidence: ev });
  if (!ev.deadline || Date.parse(ev.deadline) < now.getTime()) return no('deadline_passed', { evidence: ev });
  if (!ev.csv || typeof ev.csv.path !== 'string' || !/^lz-daily\/\d{4}-\d{2}-\d{2}\/lzd_[0-9TZ]+_[0-9a-f]{6}\/cdb_logizard_shohinmaster_upload\.csv$/.test(ev.csv.path)) return no('csv_path_bad');
  const csvPath = path.join(dataDir, ...ev.csv.path.split('/'));
  let csvBuf;
  try { csvBuf = fs.readFileSync(csvPath); } catch { return no('csv_missing'); }
  if (sha256(csvBuf) !== ev.csv.sha256) return no('csv_sha256_mismatch');
  const rows = Math.max(0, parseCsvBytes(csvBuf).records.length - 1);
  if (rows !== ev.csv.rows || rows < 1) return no('csv_rows_mismatch');
  return { ok: true, reason: null, asOf, evidence: ev, csvPath, csvBuf };
}

/** CSV の商品 ID (1 列目・Shift_JIS) */
export function csvIds(csvBuf) {
  return parseCsvBytes(csvBuf).records.slice(1).map((r) => iconv.decode(Buffer.from(r.cells[0]), 'cp932'));
}

/**
 * 取り込む前の確かめ: CSV の全部の商品 ID が、直前の書き出し (readLzShohinMaster の結果) にあり・削除されていない
 * @returns {{ ok: boolean, rows: number, missing: string[], deleted: string[] }}
 */
export function precheck({ csvBuf, lz }) {
  const ids = csvIds(csvBuf), missing = [], deleted = [];
  for (const id of ids) {
    const r = lz.byId.get(id);
    if (!r) missing.push(id);
    else if (r.deleted !== '0') deleted.push(id);
  }
  return { ok: ids.length > 0 && !missing.length && !deleted.length, rows: ids.length, missing, deleted };
}
