/**
 * finance/coverage-manifest.mjs — 決済のそろい (coverage) の manifest の共通の部品 (D7b-1b-2)
 *   設計 = AI_reference『CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』v26 §3.1。
 *   🚨 Render の受け口 (ingest/finance-coverage.mjs) と miniPC の coordinator (D7b-1b-3・後の PR) が **同じ関数** を使う (二つの実装を作らない)。
 *
 * ① receipt digest = 会社 × モール × scope の **今有効な受領記録のうち lines > 0 のもの全部** (墓石 lines = 0 は除く・疑似注文を含む・期間では絞らない) の要約:
 *     正規の JSON {"format":"frd-v1","receipts":[{lines, mall_order_no, set_checksum, transform_version}, …]} の SHA-256 (UTF-8・16 進)
 *     並び = mall_order_no の **UTF-8 のバイトの順** (PostgreSQL は collate "C"・SQLite の送り手は Buffer.compare)。run ID・run token は入れない。
 *     数 (receipt_count) と行の数の合計 (receipt_lines) も同じ集合から。
 *     🚨 51 万注文でも手元に全部を持たない: createReceiptDigester() に 1 行ずつ **並んだ順に** 渡す (順が崩れたら例外 = 黙って別の digest にしない)
 * ② request_hash = complete の要求の正規の JSON の SHA-256 (状態・世代・token・complete_to・manifest の全部)。サーバーの作る列 (completed_at など) は入れない
 * ③ validateCoverageManifest = manifest の形を確かめて正規化する (送り手は送る前に・受け口は受けたときに同じ関数で)
 *
 * 正規の JSON の決まり = canonical-hash.mjs (鍵の順は固定・null は null・数は安全な整数だけ)。
 *   ID と bigint (世代・source_revision・一覧 / 印の ID) = **10 進の文字列** / 件数 = 数 / 日時 = UTC の YYYY-MM-DDTHH:MM:SSZ / 日付 = YYYY-MM-DD
 * 🚨 保存済みの digest / hash と同じ式のまま変えない (変えるときは format の版を上げる)
 */
import crypto from 'node:crypto';
import { canonicalJsonStrict, canonicalSha256 } from '../canonical-hash.mjs';

export const RECEIPT_DIGEST_FORMAT = 'frd-v1';
export const REQUEST_HASH_FORMAT = 'fcr-v1';
export const COVERAGE_STATES = ['updating', 'complete'];
export const RUN_TOKEN_RE = /^[0-9A-Za-z._:-]{16,100}$/;
export const EVIDENCE_ID_RE = /^[0-9A-Za-z._:-]{1,80}$/;
export const HEX64_RE = /^[0-9a-f]{64}$/;
const BIGINT_RE = /^(0|[1-9][0-9]{0,18})$/;
const BIGINT_MAX = 9223372036854775807n;
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const UTC_SECOND_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/;

/** UTF-8 のバイトの順 (PostgreSQL の collate "C" と同じ) */
export const cmpUtf8 = (a, b) => Buffer.compare(Buffer.from(a, 'utf8'), Buffer.from(b, 'utf8'));

// ─── ① receipt digest ───
function receiptItem(r, where) {
  if (!r || typeof r !== 'object' || Array.isArray(r)) throw new Error(`${where}: object でない`);
  if (typeof r.mall_order_no !== 'string' || r.mall_order_no === '') throw new Error(`${where}.mall_order_no が文字列でない`);
  if (typeof r.set_checksum !== 'string') throw new Error(`${where}.set_checksum が文字列でない`);
  if (r.transform_version !== null && typeof r.transform_version !== 'string') throw new Error(`${where}.transform_version が文字列か null でない`);
  if (!Number.isSafeInteger(r.lines) || r.lines <= 0) throw new Error(`${where}.lines は 1 以上の整数 (墓石 lines = 0 は入れない): ${r.lines}`);
  return { lines: r.lines, mall_order_no: r.mall_order_no, set_checksum: r.set_checksum, transform_version: r.transform_version };
}

/**
 * 1 行ずつ足す digest。add(受領記録) を mall_order_no の UTF-8 のバイトの順に呼び、最後に finish() → { count, lines, digest }。
 *   結果は canonicalSha256({ format, receipts: [全部] }) と同じ (試験で固定)。順が崩れた・同じ番号が 2 回 = 例外
 */
export function createReceiptDigester() {
  const h = crypto.createHash('sha256');
  h.update(`{"format":${JSON.stringify(RECEIPT_DIGEST_FORMAT)},"receipts":[`, 'utf8');
  let count = 0, lines = 0, prev = null, done = false;
  return {
    add(r) {
      if (done) throw new Error('finish の後に add した');
      const it = receiptItem(r, `receipts[${count}]`);
      if (prev !== null && cmpUtf8(prev, it.mall_order_no) >= 0) throw new Error(`受領記録の並びが UTF-8 のバイトの順でない (${prev} の後に ${it.mall_order_no})`);
      h.update((count ? ',' : '') + canonicalJsonStrict(it), 'utf8');
      count++; lines += it.lines; prev = it.mall_order_no;
      if (!Number.isSafeInteger(lines)) throw new Error('行の数の合計が安全な整数を超えた');
    },
    finish() {
      if (done) throw new Error('finish を 2 回呼んだ');
      done = true;
      h.update(']}', 'utf8');
      return { count, lines, digest: h.digest('hex') };
    },
  };
}

/** 受領記録の配列から (lines = 0 の墓石は除く・並べ直す)。送り手の SQLite の側はこれを使う */
export function receiptDigest(receipts) {
  const list = receipts.filter((r) => !(r && r.lines === 0)).slice().sort((a, b) => cmpUtf8(String(a && a.mall_order_no), String(b && b.mall_order_no)));
  const d = createReceiptDigester();
  for (const r of list) d.add(r);
  return d.finish();
}

// ─── ③ manifest の形 ───
/** 世代・source_revision など (bigint) = 安全な整数か 10 進の文字列 → 10 進の文字列。min 以上 */
export function bigintText(v, name, { min = 0n } = {}) {
  let s = null;
  if (typeof v === 'number' && Number.isSafeInteger(v)) s = String(v);
  else if (typeof v === 'string' && BIGINT_RE.test(v)) s = v;
  if (s === null || BigInt(s) > BIGINT_MAX || BigInt(s) < min) throw new Error(`${name} は ${min} 以上の整数 (数か 10 進の文字列): ${String(v).slice(0, 30)}`);
  return s;
}
const isRealDate = (s) => typeof s === 'string' && DATE_RE.test(s) && !Number.isNaN(Date.parse(`${s}T00:00:00Z`)) && new Date(Date.parse(`${s}T00:00:00Z`)).toISOString().slice(0, 10) === s;
/** UTC の 'YYYY-MM-DDTHH:MM:SSZ' (秒まで・実在する時刻) */
export const isUtcSecond = (v) => typeof v === 'string' && UTC_SECOND_RE.test(v) && !Number.isNaN(Date.parse(v)) && new Date(Date.parse(v)).toISOString().replace(/\.\d{3}Z$/, 'Z') === v;
/** 実時刻の JST の日 (YYYY-MM-DD) */
export const jstDateOf = (iso) => new Date(Date.parse(iso) + 9 * 3600e3).toISOString().slice(0, 10);
const addDays = (d, n) => new Date(Date.parse(`${d}T00:00:00Z`) + n * 86400e3).toISOString().slice(0, 10);

/** manifest の列 (表 core.finance_coverage の列と同じ名前・この順で hash に入る順とは関係ない) と形 */
export const MANIFEST_FIELDS = Object.freeze({
  complete_to: 'date',
  settlements_through: 'time',
  source_revision: 'bigint',
  headers_count: 'count1',
  headers_checksum: 'hex',
  receipt_count: 'count',
  receipt_lines: 'count',
  receipt_digest: 'hex',
  inventory_snapshot_id: 'id',
  inventory_count: 'count',
  inventory_digest: 'hex',
  inventory_completed_at: 'time',
  initial_marker_id: 'id',
  initial_marker_digest: 'hex',
  selected_documents_count: 'count1',
  selected_documents_digest: 'hex',
  evidence_chain_from: 'time',
  evidence_chain_through: 'time',
  expected_report_count: 'count',
  expected_report_digest: 'hex',
  inventory_runs_digest: 'hex',
});
/** 未来の時刻の許し (時計のずれ) */
export const FUTURE_SKEW_MS = 10 * 60e3;

/**
 * manifest を確かめて正規化する (例外 = 理由の文字列)。now = 比べる今の時刻 (Date)
 *   ・全部の列が必須 (null 不可) = 欠けた complete を作らない
 *   ・complete_to = settlements_through の JST の日の前日 (§3.1: end の日は途中)
 *   ・時刻 (settlements_through・inventory_completed_at・evidence_chain_through) は未来でない (今 + 10 分まで)・evidence_chain_from ≤ evidence_chain_through
 *   ・receipt_lines ≥ receipt_count・どちらかが 0 なら両方 0 / selected_documents_count = headers_count (1 つの決済 = 採った文書 1 つ = 見出し 1 つ)
 *   ・知らない鍵は例外 (黙って落とさない = hash に入らない値を送らせない)
 */
export function validateCoverageManifest(m, { now = new Date() } = {}) {
  if (!m || typeof m !== 'object' || Array.isArray(m)) throw new Error('manifest は object');
  for (const k of Object.keys(m)) if (!Object.hasOwn(MANIFEST_FIELDS, k)) throw new Error(`manifest に知らない鍵: ${k}`);
  const out = {};
  for (const [k, kind] of Object.entries(MANIFEST_FIELDS)) {
    if (!Object.hasOwn(m, k) || m[k] === null || m[k] === undefined) throw new Error(`manifest.${k} が無い (complete は全部必須)`);
    const v = m[k];
    switch (kind) {
      case 'date': if (!isRealDate(v)) throw new Error(`manifest.${k} は実在する YYYY-MM-DD: ${String(v).slice(0, 30)}`); out[k] = v; break;
      case 'time': if (!isUtcSecond(v)) throw new Error(`manifest.${k} は UTC の YYYY-MM-DDTHH:MM:SSZ: ${String(v).slice(0, 30)}`); out[k] = v; break;
      case 'bigint': out[k] = bigintText(v, `manifest.${k}`); break;
      case 'count': case 'count1': {
        const min = kind === 'count1' ? 1 : 0;
        if (!Number.isSafeInteger(v) || v < min || (k !== 'receipt_lines' && v > 2147483647)) throw new Error(`manifest.${k} は ${min} 以上の整数: ${String(v).slice(0, 30)}`);
        out[k] = v; break;
      }
      case 'hex': if (typeof v !== 'string' || !HEX64_RE.test(v)) throw new Error(`manifest.${k} は 64 桁の 16 進 (小文字)`); out[k] = v; break;
      case 'id': if (typeof v !== 'string' || !EVIDENCE_ID_RE.test(v)) throw new Error(`manifest.${k} は ID の文字列 (英数と ._:- の 1〜80 文字・数の ID は 10 進の文字列): ${String(v).slice(0, 30)}`); out[k] = v; break;
      default: throw new Error(`内部: 知らない形 ${kind}`);
    }
  }
  const expectTo = addDays(jstDateOf(out.settlements_through), -1);
  if (out.complete_to !== expectTo) throw new Error(`manifest.complete_to (${out.complete_to}) は settlements_through (${out.settlements_through}) の JST の日の前日 (${expectTo}) でない`);
  const limit = now.getTime() + FUTURE_SKEW_MS;
  for (const k of ['settlements_through', 'inventory_completed_at', 'evidence_chain_through']) {
    if (Date.parse(out[k]) > limit) throw new Error(`manifest.${k} (${out[k]}) が未来`);
  }
  if (out.evidence_chain_from > out.evidence_chain_through) throw new Error('manifest.evidence_chain_from が evidence_chain_through より後');
  if (out.receipt_lines < out.receipt_count || ((out.receipt_count === 0) !== (out.receipt_lines === 0))) throw new Error(`manifest.receipt_lines (${out.receipt_lines}) と receipt_count (${out.receipt_count}) が合わない (lines > 0 の受領記録だけを数える)`);
  if (out.selected_documents_count !== out.headers_count) throw new Error(`manifest.selected_documents_count (${out.selected_documents_count}) と headers_count (${out.headers_count}) が違う (1 つの決済 = 採った文書 1 つ)`);
  return out;
}

// ─── ② request_hash ───
/**
 * complete の要求の正規の hash。中身 = { format, company_id, mall, scope_key, source, state: 'complete', generation (10 進の文字列), run_token, manifest の全部の列 }
 *   manifest は validateCoverageManifest を通した値 (日時・ID・bigint は文字列・件数は数)
 */
export function coverageRequestHash({ companyId, mall, scopeKey, source, generation, runToken, manifest }) {
  return canonicalSha256({
    format: REQUEST_HASH_FORMAT, company_id: companyId, mall, scope_key: scopeKey, source, state: 'complete',
    generation: bigintText(generation, 'generation', { min: 1n }), run_token: runToken, ...manifest,
  });
}
