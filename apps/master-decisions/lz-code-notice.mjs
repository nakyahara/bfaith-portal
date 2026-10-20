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
 * 🆕 lzd-v4 (2026-10-21 中原さん「悪い行だけ外して、残りは毎晩取り込む」): 報告に classes.excluded (外した行) があれば、書き方の問題に限らず
 *   外した行を全部 (原価が無い・仕入先が無い なども) 商品のページへのリンク (Google Chat の <URL|文字>) と直し方つきで出す。見出し = やさしい 1 文 + 今夜送るかどうか
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { readEvidence } from '../company-db/push/evidence.mjs';
import { excludedSentence } from './lz-cdb.mjs';

/** 商品のページ (ポータルの商品マスタ) */
export const PORTAL_URL = 'https://bfaith-portal.onrender.com';
export const skuPageUrl = (code) => `${PORTAL_URL}/apps/master-edit/sku/${encodeURIComponent(String(code ?? ''))}`;

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

/** Google Chat のリンク (<URL|文字>)。文字に < > | を入れない */
export function codeLink(x) {
  const raw = x && (x.ne_code ?? x.code_norm);
  return `<${skuPageUrl(raw)}|${showCode(raw).replace(/[<>|]/g, ' ')}>`;
}

/** 書き方の問題でない外した行の直し方 */
const FIX = Object.freeze({
  cdb_cost_missing: '原価が無い → 商品マスタで原価を入れる',
  cdb_cost_shape: '原価の形が変 → 商品マスタで原価を直す',
  cdb_supplier_shape: '仕入先の番号が 4 桁でない → 商品マスタで仕入先を確かめる',
  cdb_name_blank: '商品名が無い → 商品マスタで商品名を入れる',
  ne_name_blank: 'NE の商品名が空 → NE で商品名を入れる',
  lz_deleted: 'ロジザードで削除済み → 使う商品ならロジザードで戻す',
});

/**
 * 外した行 1 件 (リンクつき)。書き方の問題 = noticeLine と同じ文・ほか = 理由と直し方
 */
export function excludedLine(x) {
  if (!x || typeof x !== 'object') return null;
  const link = codeLink(x);
  if (NOTICE_REASONS.includes(x.reason)) return noticeLine(x, { head: link });
  if (x.reason === 'cdb_supplier_count') {
    const many = Array.isArray(x.suppliers) && x.suppliers.length >= 2;
    return `・${link}: ${many ? `仕入先が ${x.suppliers.length} つ → 商品マスタで代表の仕入先を 1 つにする` : '仕入先が無い → 商品マスタで仕入先を入れる'}`;
  }
  return `・${link}: ${FIX[x.reason] || `${showCode(x.label || x.reason)} → 商品マスタを確かめる`}`;
}

/**
 * 報告の classes.excluded (外した行) → まとめの行 (見出し + 1 件ずつ + ほか N 件)。無い = []
 * @param {{ verdict?: string, classes?: { excluded?: object[] } }} report
 */
export function excludedNoticeLines(report, { max = NOTICE_MAX_LINES } = {}) {
  const xs = report && report.classes && Array.isArray(report.classes.excluded) ? report.classes.excluded : [];
  if (!xs.length) return [];
  const head = excludedSentence(xs, { show: 0 });
  // 少ない理由を先に (上限 10 行で切れても、どの種類の問題があるかが全部見える)。同じ数なら報告の順
  const n = xs.reduce((m, x) => m.set(x && x.reason, (m.get(x && x.reason) || 0) + 1), new Map());
  const lines = xs.map((x, i) => ({ x, i })).sort((a, b) => (n.get(a.x && a.x.reason) - n.get(b.x && b.x.reason)) || (a.i - b.i))
    .map(({ x }) => excludedLine(x)).filter(Boolean);
  return [`⚠️ ${head} = ${report.verdict === 'pass' ? 'ほかの商品は今夜ロジザードに送ります' : '今朝は不合格なので、今夜はロジザードに何も送りません'}`,
    ...lines.slice(0, max), ...(lines.length > max ? [`・ほか ${lines.length - max} 件 (lz-daily の報告 report.json の classes.excluded)`] : [])];
}

/** 1 件の行 (対象でない理由 = null)。head = 行の頭のコード (既定 = 文字だけ・外した行の知らせではリンク) */
export function noticeLine(x, { head = null } = {}) {
  if (!x || !NOTICE_REASONS.includes(x.reason)) return null;
  const code = head || showCode(x.code_norm ?? x.ne_code);
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
    // 🆕 lzd-v4 の報告 = 外した行を全部 (リンクつき)。前の版の報告 = 書き方の問題だけ (今までどおり)
    if (report.classes && Array.isArray(report.classes.excluded)) return excludedNoticeLines(report, { max });
    return noticeLines(report.classes && report.classes.invalid, { max });
  } catch (e) {
    warn(`[lz-code-notice] lz-daily の報告を読めない (まとめには足さない): ${String(e && e.message).slice(0, 160)}`);
    return [];
  }
}
