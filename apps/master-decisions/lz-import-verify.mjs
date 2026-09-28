/**
 * lz-import-verify.mjs — ロジザードの毎日の商品マスタを取り込んだ後の確かめ (純粋・マスタ正本切替 ③c-1b-2b-1a)
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3「③c-1b-2b 設計 v1 §2・契約 v3 G・K4・K8」。
 * 入力 = 取り込んだ CSV の行 (validateImportCsv の table) / 直前と直後の一覧 (readLzShohinMaster = 43 列の文字) / (試験) バーコードの前後。
 *
 * 決まり (rules_version を証跡に残す):
 *   取り込んだ商品: 直後の一覧にある・exact の列が CSV の文字のとおり・対象外の列 (observe の候補の列とシステムの列を除く全部) が直前と同じ
 *   取り込まなかった商品: 直後の一覧にある・システムの列も含めて全部の列が直前と同じ (大文字小文字だけ違う別の商品を書き換えていない)
 *   直前に無くて直後にある商品・直前にあって直後に無い商品 = 差
 *   observe = 実機の取込で決めるまで比べずに記録だけ (ふりがなの対応する列・仕入単価の書き方・取り込んだ商品のシステムの列)。
 *     observe が残る決まり = decided: false = **本番の合格と数えない** (試験の記録で区別する。毎晩の本番は決まりが全部 exact になるまで動かさない)
 * 差が 1 つでも = ok: false (verify_failed)。差は全部返す (呼び手が verify.json に・知らせは件数と先頭の数件)。
 * 前後の一致はバイトそのまま (raw) で比べる (違うバイトが同じ文字に読めても見落とさない)。
 * 一覧に読めないバイト (U+FFFD に読めるセル) がある = 証跡の破損 = 比べずに差 evidence_broken (K4・Codex #1519 R1 High)。
 */
import { LZ_SHOHIN } from './lz-cdb.mjs';
import { DAILY } from './lz-csv.mjs';
import { parseCsvBytes } from './lz-compare.mjs';
import iconv from 'iconv-lite';

export const SYSTEM_COLS = Object.freeze(['登録日時', '変更日時', 'インポート日時']);

/** 2b-1 の決まり (実機の取込の前)。mode: key = 商品ID の文字の一致 / exact = CSV の文字のとおり / observe = 記録だけ (lz = 候補の列) */
export const RULES_2B1 = Object.freeze({
  version: 'lzv-2b1-observe',
  targets: Object.freeze([
    Object.freeze({ csv: '形式/型番', lz: ['商品ID'], mode: 'key' }),
    Object.freeze({ csv: '商品名', lz: ['商品名'], mode: 'exact' }),
    Object.freeze({ csv: 'ふりがな', lz: ['検索名称', '検索名称2'], mode: 'observe' }),   // どちらに入るかは実機で決める
    Object.freeze({ csv: '仕入単価', lz: ['仕入単価'], mode: 'observe' }),                // 書き方 (1200 / 1200.00 など) は実機で決める
    Object.freeze({ csv: '取引先id', lz: ['商品予備項目００３'], mode: 'exact' }),
  ]),
  importedSystem: 'observe',   // 取り込んだ商品のシステムの列 (変わるのが正しいかは実機で決める)
});

const colIndex = (name) => {
  const i = LZ_SHOHIN.header.indexOf(name);
  if (i < 0) throw new Error(`ロジザードの一覧に列が無い: ${name}`);
  return i;
};

/** 決まりの形を確かめて、列の番号に直す */
export function compileRules(rules) {
  if (!rules || typeof rules.version !== 'string' || !Array.isArray(rules.targets)) throw new Error('rules の形が違う');
  const csvCols = rules.targets.map((t) => t.csv);
  if (csvCols.length !== DAILY.header.length || csvCols.some((c, i) => c !== DAILY.header[i])) throw new Error('rules.targets は DAILY の見出しの順で 5 つ');
  if (rules.targets[0].mode !== 'key' || rules.targets[0].lz.length !== 1) throw new Error('1 つ目 (形式/型番) は key で 1 列');
  for (const t of rules.targets.slice(1)) {
    if (!['exact', 'observe'].includes(t.mode)) throw new Error(`mode は exact / observe: ${t.csv}`);
    if (t.mode === 'exact' && t.lz.length !== 1) throw new Error(`exact は 1 列: ${t.csv}`);
  }
  if (!['exact_unchanged', 'observe'].includes(rules.importedSystem)) throw new Error('importedSystem は exact_unchanged / observe');
  const targets = rules.targets.map((t, i) => ({ ...t, csvIdx: i, lzIdx: t.lz.map(colIndex) }));
  const targetLz = new Set(targets.slice(1).flatMap((t) => t.lzIdx));
  const system = new Set(SYSTEM_COLS.map(colIndex));
  const idIdx = targets[0].lzIdx[0];
  const decided = targets.every((t) => t.mode !== 'observe') && rules.importedSystem !== 'observe';
  return { version: rules.version, targets, targetLz, system, idIdx, decided, importedSystem: rules.importedSystem };
}

/**
 * 取り込んだ後の確かめ (商品マスタ)
 * @param {object} p
 * @param {string[][]} p.table  取り込んだ CSV の行 (validateImportCsv の table)
 * @param {{ ok: boolean, byId: Map<string, { cells: string[] }> }} p.pre   直前の一覧
 * @param {{ ok: boolean, byId: Map<string, { cells: string[] }> }} p.post  直後の一覧
 * @param {object} [p.rules]
 * @returns {{ ok: boolean, decided: boolean, rules_version: string, diffs: object[], observed: { targets: object[], imported_system: object[] }, counts: object }}
 */
export function verifyImport({ table, pre, post, rules = RULES_2B1 }) {
  const R = compileRules(rules);
  if (!pre || !pre.ok || !post || !post.ok) throw new Error('直前と直後の一覧 (ok) が要る');
  const H = LZ_SHOHIN.header;
  const diffs = [], observed = { targets: [], imported_system: [] };
  const broken = [['pre', pre], ['post', post]].filter(([, x]) => !x.encoding || x.encoding.fffd > 0);
  if (broken.length) {
    return { ok: false, decided: R.decided, rules_version: R.version, observed,
      diffs: broken.map(([side, x]) => ({ id: null, kind: 'evidence_broken', side, fffd: x.encoding ? x.encoding.fffd : null })),
      counts: { imported: 0, untouched: 0, diffs: broken.length, observed_targets: 0, observed_system: 0 } };
  }
  const same = (a, b, col) => Buffer.from(a.raw[col]).equals(Buffer.from(b.raw[col]));
  const imported = new Set();
  for (const row of table) {
    const id = row[0];
    imported.add(id);
    const a = pre.byId.get(id), b = post.byId.get(id);
    if (!b) { diffs.push({ id, kind: 'missing_after' }); continue; }
    if (!a) { diffs.push({ id, kind: 'missing_before' }); continue; }   // 押す前の確かめ (全部ある) を通ったのに = 証跡の食い違い
    for (const t of R.targets.slice(1)) {
      const csv = row[t.csvIdx];
      if (t.mode === 'exact') {
        const col = t.lzIdx[0];
        if (b.cells[col] !== csv) diffs.push({ id, kind: 'target_mismatch', col: H[col], csv, pre: a.cells[col], post: b.cells[col] });
      } else {
        observed.targets.push({ id, csv_col: t.csv, csv, lz: t.lzIdx.map((col) => ({ col: H[col], pre: a.cells[col], post: b.cells[col] })) });
      }
    }
    for (let col = 0; col < H.length; col++) {
      if (col === R.idIdx || R.targetLz.has(col)) continue;
      if (R.system.has(col)) {
        if (same(a, b, col)) continue;
        if (R.importedSystem === 'observe') observed.imported_system.push({ id, col: H[col], pre: a.cells[col], post: b.cells[col] });
        else diffs.push({ id, kind: 'system_changed', col: H[col], pre: a.cells[col], post: b.cells[col] });
        continue;
      }
      if (!same(a, b, col)) diffs.push({ id, kind: 'non_target_changed', col: H[col], pre: a.cells[col], post: b.cells[col] });
    }
  }
  let untouched = 0;
  for (const [id, a] of pre.byId) {
    if (imported.has(id)) continue;
    const b = post.byId.get(id);
    if (!b) { diffs.push({ id, kind: 'vanished' }); continue; }
    untouched++;
    for (let col = 0; col < H.length; col++) {
      if (same(a, b, col)) continue;
      diffs.push({ id, kind: R.system.has(col) ? 'untouched_system_changed' : 'untouched_changed', col: H[col], pre: a.cells[col], post: b.cells[col] });
    }
  }
  for (const id of post.byId.keys()) if (!pre.byId.has(id)) diffs.push({ id, kind: 'appeared' });
  return {
    ok: diffs.length === 0, decided: R.decided, rules_version: R.version, diffs, observed,
    counts: { imported: imported.size, untouched, diffs: diffs.length, observed_targets: observed.targets.length, observed_system: observed.imported_system.length },
  };
}

/**
 * バーコード情報の書き出し (② SKU / バーコード情報) を読む。見出しに 商品ID・バーコード・全部の行の列の数が同じ (K4)
 * @returns {{ ok: boolean, reason: string|null, header: string[], rows: number, byId: Map<string, string[]> }}  byId = 商品ID → その商品のバーコード (文字のまま・重複も数だけ持つ・並べ替え済み)
 */
export function readBarcodeExport(buf) {
  const bad = (reason) => ({ ok: false, reason, header: [], rows: 0, byId: new Map() });
  const b = Buffer.from(buf || []);
  if (!b.length) return bad('barcode_empty');
  if (/<html|<!DOCTYPE|SUSPENDED/i.test(b.subarray(0, 2000).toString('latin1'))) return bad('barcode_html');
  const P = parseCsvBytes(b);
  if (P.shape.unterminated || P.shape.bare_quote || P.shape.after_quote) return bad('barcode_broken');
  const dec = (x) => iconv.decode(Buffer.from(x), 'cp932');
  for (const r of P.records) for (const c of r.cells) if (!iconv.encode(dec(c), 'cp932').equals(Buffer.from(c))) return bad('barcode_encoding');
  const header = (P.records[0] || { cells: [] }).cells.map(dec);
  const idIdx = header.indexOf('商品ID'), bcIdx = header.indexOf('バーコード');
  if (idIdx < 0 || bcIdx < 0 || header.indexOf('商品ID', idIdx + 1) >= 0 || header.indexOf('バーコード', bcIdx + 1) >= 0) return bad('barcode_header');
  const body = P.records.slice(1).map((r) => r.cells.map(dec));
  if (body.some((r) => r.length !== header.length)) return bad('barcode_row_width');
  const byId = new Map();
  for (const r of body) {
    const id = r[idIdx];
    if (!byId.has(id)) byId.set(id, []);
    byId.get(id).push(r[bcIdx]);
  }
  for (const list of byId.values()) list.sort();
  return { ok: true, reason: null, header, rows: body.length, byId };
}

/**
 * バーコードの前後を比べる (取り込んだ商品と大文字小文字の候補)。商品ID とバーコードを文字として: 増えた・消えた・重複の数 (K4)。
 * 商品名などほかの列の変化は差にしない (見出しの 商品ID・バーコード の位置が変わったら差)
 * @param {{ pre, post, ids: Iterable<string> }} p
 * @returns {{ ok: boolean, diffs: Array<{ id, kind: 'header_changed'|'added'|'removed', barcode?: string }> }}
 */
export function compareBarcodes({ pre, post, ids }) {
  if (!pre || !pre.ok || !post || !post.ok) throw new Error('バーコードの前と後 (ok) が要る');
  const diffs = [];
  const pos = (h) => [h.indexOf('商品ID'), h.indexOf('バーコード')].join(',');
  if (pos(pre.header) !== pos(post.header)) diffs.push({ id: null, kind: 'header_changed' });
  for (const id of new Set(ids)) {
    const a = pre.byId.get(id) || [], b = post.byId.get(id) || [];
    const count = (list) => list.reduce((m, r) => m.set(r, (m.get(r) || 0) + 1), new Map());
    const ca = count(a), cb = count(b);
    for (const [barcode, n] of ca) for (let k = cb.get(barcode) || 0; k < n; k++) diffs.push({ id, kind: 'removed', barcode });
    for (const [barcode, n] of cb) for (let k = ca.get(barcode) || 0; k < n; k++) diffs.push({ id, kind: 'added', barcode });
  }
  return { ok: diffs.length === 0, diffs };
}
