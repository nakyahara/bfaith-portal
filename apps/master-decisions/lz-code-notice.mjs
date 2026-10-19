/**
 * lz-code-notice.mjs — 朝のまとめ (daily-sync) に「NE の商品コードの書き方」の問題を、商品と直し方つきで出す (10/16 中原さん)
 *
 * なぜ: 2026-10-13 にポータルの外から小文字の商品マスタ CSV が NE に予約アップされ、NE の書き方が 2 つになった商品 (u60e-05 など) や
 *   NE とロジザードで大文字・小文字が違う商品 (u60e-10 / U60E-10) ができた。ロジザード毎日の商品マスタ (scripts/company-db/lz-daily.mjs) は
 *   これらを不正として出さない (守りは効いている) が、朝のまとめには「不合格」としか出ず、どの商品を・どう直すかが誰にも分からなかった。
 * 材料 = 同じ朝の lz-daily の報告 (report.json の classes.invalid)。新しい問い合わせはしない・lz-daily の合否は変えない。
 *   証跡 lz-daily (DATA_DIR/company-db-evidence/<今日>/lz-daily.json) が complete・この daily-sync の回 (sync_run_id)・報告の sha256 が証跡と同じときだけ読む。
 *   🚨 読めない・合わない・壊れている = 何も足さない (まとめを壊さない・止めない。この関数は投げない)
 * 対象の理由と直し方 (書き方を推測しない = 見えている書き方だけを並べる):
 *   code_collided     = NE に大文字・小文字だけ違う書き方が 2 つ以上 → NE で 1 つにそろえる
 *   lz_case_collision = NE とロジザードで大文字・小文字が違う → ロジザードの書き方が 1 つなら NE をその書き方に直す / 2 つ以上なら確かめてそろえる
 *   code_invalid      = NE の書き方が使えない (使えない文字・壊れた記録) → NE の商品コードを確かめる
 *   norm_collided     = NE に同じコードに読める商品が 2 つ以上 → NE で 1 つにそろえる
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { readEvidence } from '../company-db/push/evidence.mjs';

/** まとめに出す行の上限 (超えた分は「ほか N 件」) */
export const NOTICE_MAX_LINES = 10;
/** 対象の理由 (lz-cdb.mjs の classifyForLz・lz-snapshot.mjs の joinLzSnapshot の理由) */
export const NOTICE_REASONS = Object.freeze(['code_collided', 'lz_case_collision', 'code_invalid', 'norm_collided']);
const CODE_SHOW_MAX = 40;

/** まとめに出すコード: 改行・制御文字・向きを変える文字を空白にして 1 行・40 文字まで (master-register-summary.mjs の safeCode と同じ決まり) */
export function showCode(x) {
  const s = String(x ?? '?').replace(/[\u0000-\u001f\u007f-\u009f\u200e\u200f\u2028\u2029\u202a-\u202e\u2066-\u2069]/g, ' ').replace(/\s+/g, ' ').trim() || '?';
  return s.length > CODE_SHOW_MAX ? `${s.slice(0, CODE_SHOW_MAX - 1)}…` : s;
}
const strs = (v) => (Array.isArray(v) ? v.filter((s) => typeof s === 'string' && s) : []);
const list = (xs, n = 4) => xs.slice(0, n).map(showCode).join(' / ') + (xs.length > n ? ` ほか ${xs.length - n}` : '');

/** 1 件の行 (対象でない理由 = null) */
export function noticeLine(x) {
  if (!x || !NOTICE_REASONS.includes(x.reason)) return null;
  const code = showCode(x.code_norm ?? x.ne_code);
  const ne = strs(x.ne_spellings);
  const lz = strs(x.lz_ids);
  const lzText = lz.length ? `・ロジザード = ${list(lz)}` : '';
  if (x.reason === 'code_collided') {
    const what = ne.length >= 2 ? `${ne.length} つ (${list(ne)})` : '2 つ以上';
    return `・${code}: NE に大文字・小文字だけ違う書き方が ${what}${lzText} → NE で 1 つにそろえる`;
  }
  if (x.reason === 'lz_case_collision') {
    const neCode = showCode(x.ne_code ?? x.code_norm);
    if (lz.length === 1) return `・${code}: NE = ${neCode} / ロジザード = ${showCode(lz[0])} → NE を ${showCode(lz[0])} に直す (NE の試しの後はポータルの「NEへ送る」が直します)`;
    return `・${code}: NE = ${neCode} / ロジザード = ${lz.length ? `${list(lz)} (書き方が ${lz.length} つ)` : '大文字・小文字の違う書き方'} → どれが正しいか確かめて NE とロジザードをそろえる`;
  }
  if (x.reason === 'code_invalid') return `・${code}: NE の書き方が使えない (使えない文字・壊れた記録${ne.length ? `: ${list(ne)}` : ''})${lzText} → NE の商品コードを確かめる`;
  return `・${code}: NE に同じコードに読める商品が 2 つ以上${lzText} → NE で 1 つにそろえる`;
}

/**
 * 報告の classes.invalid → まとめの行 (見出し + 1 件ずつ + ほか N 件)。対象が無い = []
 * @param {Array<object>} invalid
 */
export function noticeLines(invalid, { max = NOTICE_MAX_LINES } = {}) {
  const lines = (Array.isArray(invalid) ? invalid : []).map(noticeLine).filter(Boolean);
  if (!lines.length) return [];
  return [`⚠️ NE の商品コードの書き方 ${lines.length} 件 (ロジザードに取り込んでいません)`,
    ...lines.slice(0, max), ...(lines.length > max ? [`・ほか ${lines.length - max} 件 (lz-daily の報告 report.json の classes.invalid)`] : [])];
}

/**
 * 同じ朝の lz-daily の報告を読んで、まとめの行を返す。読めない・この回のものでない・壊れている = [] (投げない)
 * @param {{ dataDir: string, asOf: string, syncRunId: string|null, max?: number, warn?: (s: string) => void }} p
 */
export function readLzCodeNotice({ dataDir, asOf, syncRunId, max = NOTICE_MAX_LINES, warn = () => {} }) {
  try {
    if (!dataDir || !asOf || !syncRunId) return [];
    const ev = readEvidence(dataDir, asOf)['lz-daily'];
    // この daily-sync の回の、作れた回の報告だけ (前の朝・手で流した回・見送った回の報告で知らせない)
    if (!ev || ev.state !== 'complete' || ev.sync_run_id !== syncRunId || ev.as_of !== asOf || !ev.report || typeof ev.report.path !== 'string') return [];
    const file = path.resolve(dataDir, ev.report.path);
    const root = path.resolve(dataDir) + path.sep;
    if (!file.startsWith(root)) return [];   // 証跡の道が DATA_DIR の外を指す = 読まない
    const buf = fs.readFileSync(file);
    if (crypto.createHash('sha256').update(buf).digest('hex') !== ev.report.sha256) { warn('[lz-code-notice] 報告の sha256 が証跡と違う = 知らせない'); return []; }
    const report = JSON.parse(buf.toString('utf8'));
    if (!report || report.run_id !== ev.run_id) return [];
    return noticeLines(report.classes && report.classes.invalid, { max });
  } catch (e) {
    warn(`[lz-code-notice] lz-daily の報告を読めない (まとめには足さない): ${String(e && e.message).slice(0, 160)}`);
    return [];
  }
}
