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


/** 2b-1 の決まり (実機の取込の前)。mode: key = 商品ID の文字の一致 / exact = CSV の文字のとおり / observe = 記録だけ (lz = 候補の列)
 * 決まりは中の配列まで全部凍結する (実行中に列を書き換える裏口を作らない。Codex #1556 R2 Medium) */
export const RULES_2B1 = Object.freeze({
  version: 'lzv-2b1-observe',
  targets: Object.freeze([
    Object.freeze({ csv: '形式/型番', lz: Object.freeze(['商品ID']), mode: 'key' }),
    Object.freeze({ csv: '商品名', lz: Object.freeze(['商品名']), mode: 'exact' }),
    Object.freeze({ csv: 'ふりがな', lz: Object.freeze(['検索名称', '検索名称2']), mode: 'observe' }),   // どちらに入るかは実機で決める
    Object.freeze({ csv: '仕入単価', lz: Object.freeze(['仕入単価']), mode: 'observe' }),                // 書き方 (1200 / 1200.00 など) は実機で決める
    Object.freeze({ csv: '取引先id', lz: Object.freeze(['商品予備項目００３']), mode: 'exact' }),
  ]),
  importedSystem: 'observe',   // 取り込んだ商品のシステムの列 (変わるのが正しいかは実機で決める)
});

/**
 * 2b-2b の決まり = 2026-09-30 の実機の少数件の試験で決めた (AI_reference CompanyDB構想/10 §6.3「実機の少数件の試験の結果」):
 *   ふりがな → **検索名称** (CSV の文字そのまま・検索名称2 は変わらない = 対象外の列として前後が同じ)・
 *   仕入単価 = **文字のまま** (CSV の "5200" → ロジザードの "5200")・
 *   取り込んだ商品のシステムの列 = **import_stamp** (登録日時は変わらない・変更日時とインポート日時は取込の時刻に変わる = 値が同じ商品も)。
 *   import_stamp = 2 つが同じ・実在する JST の 14 桁・前の値より後・**取込の時刻の窓の中** (押した時刻 − 10 分 〜 結果を読んだ時刻 + 10 分。Codex #1556 R1 High)
 * observe の列がゼロ = decided: true
 */
export const RULES_2B2 = Object.freeze({
  version: 'lzv-2b2',
  targets: Object.freeze([
    Object.freeze({ csv: '形式/型番', lz: Object.freeze(['商品ID']), mode: 'key' }),
    Object.freeze({ csv: '商品名', lz: Object.freeze(['商品名']), mode: 'exact' }),
    Object.freeze({ csv: 'ふりがな', lz: Object.freeze(['検索名称']), mode: 'exact' }),
    Object.freeze({ csv: '仕入単価', lz: Object.freeze(['仕入単価']), mode: 'exact' }),
    Object.freeze({ csv: '取引先id', lz: Object.freeze(['商品予備項目００３']), mode: 'exact' }),
  ]),
  importedSystem: 'import_stamp',
});

/**
 * 毎晩の本番 (③c-1b-2b-2) の確かめの列の決まり = RULES_2B2 (2b-2b で入れた)。
 * decided:true (observe の列がゼロ) だけ入れる (エンジンが照らす)。毎晩の本番が動くのは、ポータルの旗 (LZ_MANUAL_V4) と miniPC の LZ_DAILY_IMPORT=on の後 (切替の PR)
 */
export const RULES_NIGHTLY = RULES_2B2;

/**
 * 見張りの商品 (毎晩の取込の K4。2026-10-03 00:20 の本番の最初の夜に、書き出しの最後の商品 zuko5 が CSV にあって押せなかった)。
 * ロジザードにだけ登録する商品 (Company DB・NE には無い = 取り込む CSV に入らない)・バーコード 1 本・商品ID の順 (バイト順) で全商品の最後に来る ID。
 * 商品マスタとバーコードの書き出しの前後すべてで、この商品が最後の行 = その前の全商品の行がそろっている (後ろから切れていない) と言える
 * = 取り込む全商品が「後ろに別の商品の行がある商品」になり、K4 (比べる商品が最後の商品なら確かめられない) を弱めずに毎晩押せる。
 * 変える = ロジザードの登録を変えるのと同時に、ここ 1 か所だけ。
 */
export const LZ_SENTINEL_ID = 'zzzzzzzzzz';
/** 見張りの商品のバーコード (1 本だけ・今あるどれとも重ならない英数字・8 / 13 桁の数字にしない = 入荷検品のバーコードマスタ・Company DB の JAN に入らない)。ID と同じく、ここ 1 か所 */
export const LZ_SENTINEL_BARCODE = 'LZGUARD0001';

/**
 * 商品ID のかたまりが、書き出しの行の順に CP932 のバイト順で厳密に増えていくか (本物の書き出しの順。2026-10-03 の商品マスタ 5,085 行・バーコード 5,203 行で確かめた)。
 * 同じ ID の 2 つ目のかたまり・順の崩れ = false (見張りが最後にあっても、順の前提が崩れた夜は「その前の全商品がそろっている」と言えない。Codex #1597 R1 Medium)
 * @param {Buffer[]} idBytes  かたまりごとの商品ID の生のバイト (行の順)
 */
export function idsAscending(idBytes) {
  for (let i = 1; i < idBytes.length; i++) if (Buffer.compare(idBytes[i - 1], idBytes[i]) >= 0) return false;
  return true;
}

/**
 * 見張りの商品の確かめ (side = pre / post)。shohin = readLzShohinMaster (ok)・barcode = readBarcodeExport (ok)。渡さない側は見ない。
 *   - 最後の行が見張り (sentinel_not_last_*)
 *   - 商品ID のかたまりがバイト順で厳密に増えていく (order_broken_*。同じ ID の 2 つ目のかたまりも)
 *   - 見張りのバーコードが sentinelBarcode の 1 本だけ・ほかの商品に同じバーコードが無い (sentinel_barcode_* / sentinel_barcode_shared_*)
 * @returns {Array<{ id, kind: string, last?: string|null, present?: boolean, got?: string[], others?: string[] }>}  present = その書き出しに見張りがあるか
 */
export function sentinelDiffs({ sentinel, side, shohin = null, barcode = null, sentinelBarcode = LZ_SENTINEL_BARCODE }) {
  const diffs = [];
  if (shohin) {
    let last = null;
    const raw = [];
    for (const [id, v] of shohin.byId) { last = id; raw.push(v.raw[LZ_SHOHIN.cols.id]); }   // 書き出しの行の順 (readLzShohinMaster は重複を断る = Map の順 = 行の順)
    if (last !== sentinel) diffs.push({ id: sentinel, kind: `sentinel_not_last_shohin_${side}`, last, present: shohin.byId.has(sentinel) });
    if (!idsAscending(raw)) diffs.push({ id: null, kind: `order_broken_shohin_${side}` });
  }
  if (barcode) {
    if (barcode.lastId !== sentinel) diffs.push({ id: sentinel, kind: `sentinel_not_last_barcode_${side}`, last: barcode.lastId ?? null, present: barcode.byId.has(sentinel) });
    if (barcode.ordered !== true) diffs.push({ id: null, kind: `order_broken_barcode_${side}` });
    const got = barcode.byId.get(sentinel) || [];
    if (barcode.byId.has(sentinel) && !(got.length === 1 && got[0] === sentinelBarcode)) diffs.push({ id: sentinel, kind: `sentinel_barcode_${side}`, got: got.slice(0, 10) });
    const others = [];
    for (const [id, list] of barcode.byId) if (id !== sentinel && list.includes(sentinelBarcode)) others.push(id);
    if (others.length) diffs.push({ id: sentinel, kind: `sentinel_barcode_shared_${side}`, others: others.slice(0, 10) });
  }
  return diffs;
}

/** 取込の時刻の印 (ロジザードの 変更日時・インポート日時 = YYYYMMDDHHMMSS の 14 桁・JST。2026-09-30 の実機) */
const STAMP_RE = /^\d{14}$/;
/** 実在する JST の日時の 14 桁か (2030-02-30・25 時などは違う) */
export function isRealStamp(s) {
  if (!STAMP_RE.test(String(s))) return false;
  const [y, mo, d, h, mi, se] = [0, 4, 6, 8, 10, 12].map((i, k) => Number(String(s).slice(i, k === 0 ? 4 : i + 2)));
  const t = new Date(Date.UTC(y, mo - 1, d, h, mi, se));
  return t.getUTCFullYear() === y && t.getUTCMonth() === mo - 1 && t.getUTCDate() === d && t.getUTCHours() === h && t.getUTCMinutes() === mi && t.getUTCSeconds() === se;
}
/** epoch ms → JST の 14 桁 (ロジザードの印と同じ形) */
export const jstStamp = (ms) => new Date(Number(ms) + 9 * 3600 * 1000).toISOString().replace(/[-:T]/g, '').slice(0, 14);
/** 取込の時刻の窓の余白 (ロジザードの時計とこの回の時計のずれ・処理の長さ。狭すぎ = 誤って verify_failed (人が見る = 安全側)) */
export const STAMP_TOLERANCE_MS = 10 * 60 * 1000;

/** import_stamp で取込の時刻の印として照らす列 (登録日時は印ではない = 変わらない) */
const STAMP_COLS = new Set(['変更日時', 'インポート日時']);

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
  if (!['exact_unchanged', 'observe', 'import_stamp'].includes(rules.importedSystem)) throw new Error('importedSystem は exact_unchanged / observe / import_stamp');
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
export function verifyImport({ table, pre, post, rules = RULES_2B1, importWindow = null }) {
  const R = compileRules(rules);
  // import_stamp = 取込の時刻の窓が要る (押した時刻 − 余白 〜 結果の時刻 + 余白・JST の 14 桁。呼び手 = エンジンが記録から渡す。Codex #1556 R1 High)
  if (R.importedSystem === 'import_stamp' && !(importWindow && isRealStamp(importWindow.from) && isRealStamp(importWindow.to) && importWindow.from <= importWindow.to)) {
    throw new Error('import_stamp には取込の時刻の窓 (importWindow = { from, to } の JST 14 桁) が要る');
  }
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
        if (R.importedSystem === 'import_stamp' && STAMP_COLS.has(H[col])) continue;   // 取込の時刻の印は下でまとめて照らす
        if (same(a, b, col)) continue;
        if (R.importedSystem === 'observe') observed.imported_system.push({ id, col: H[col], pre: a.cells[col], post: b.cells[col] });
        else diffs.push({ id, kind: 'system_changed', col: H[col], pre: a.cells[col], post: b.cells[col] });
        continue;
      }
      if (!same(a, b, col)) diffs.push({ id, kind: 'non_target_changed', col: H[col], pre: a.cells[col], post: b.cells[col] });
    }
    // import_stamp: 変更日時とインポート日時 = 同じ値の 14 桁で、どちらも前の値より後 (取り込まれた = 時刻が進む。進まない = 取り込まれていない行)
    if (R.importedSystem === 'import_stamp') {
      const u = b.cells[colIndex('変更日時')], im = b.cells[colIndex('インポート日時')];
      const uPre = a.cells[colIndex('変更日時')], imPre = a.cells[colIndex('インポート日時')];
      const why = !STAMP_RE.test(u) || !STAMP_RE.test(im) ? 'not_14_digits' : !isRealStamp(u) || !isRealStamp(im) ? 'not_real_datetime' : u !== im ? 'not_equal'
        : !(u > uPre) || !(im > imPre) ? 'not_after_pre' : u < importWindow.from || u > importWindow.to ? 'outside_import_window' : null;
      if (why) diffs.push({ id, kind: 'import_stamp_bad', why, pre: { 変更日時: uPre, インポート日時: imPre }, post: { 変更日時: u, インポート日時: im }, window: importWindow });
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
  // 本物の書き出しは末尾が改行で終わらない (2026-09-29) = 改行で終わる = 行の切れ目で切れた疑い (Codex #1530 R1 High)
  if (P.shape.trailing_newline) return bad('barcode_truncated');
  const dec = (x) => iconv.decode(Buffer.from(x), 'cp932');
  for (const r of P.records) for (const c of r.cells) if (!iconv.encode(dec(c), 'cp932').equals(Buffer.from(c))) return bad('barcode_encoding');
  const header = (P.records[0] || { cells: [] }).cells.map(dec);
  const idIdx = header.indexOf('商品ID'), bcIdx = header.indexOf('バーコード');
  if (idIdx < 0 || bcIdx < 0 || header.indexOf('商品ID', idIdx + 1) >= 0 || header.indexOf('バーコード', bcIdx + 1) >= 0) return bad('barcode_header');
  const body = P.records.slice(1).map((r) => r.cells.map(dec));
  if (body.some((r) => r.length !== header.length)) return bad('barcode_row_width');
  const byId = new Map();
  let grouped = true, prev = null;   // 商品ごとの行がひとまとまりか (本物の書き出しは商品ID の順 = ひとまとまり。9/29)
  const blockBytes = [];   // かたまりごとの商品ID の生のバイト (CP932・行の順) = ordered (バイト順で厳密に増えていく。Codex #1597 R1 Medium)
  for (const [k, r] of body.entries()) {
    const id = r[idIdx];
    if (id !== prev) blockBytes.push(Buffer.from(P.records[k + 1].cells[idIdx]));
    if (id !== prev && byId.has(id)) grouped = false;
    prev = id;
    if (!byId.has(id)) byId.set(id, []);
    byId.get(id).push(r[bcIdx]);
  }
  for (const list of byId.values()) list.sort();
  // lastId = 最後の行の商品 (途中で切れるのは後ろから = ひとまとまりなら、後ろに別の商品の行がある商品の行は全部そろっている。Codex #1530 R3 High)
  return { ok: true, reason: null, header, rows: body.length, byId, grouped, lastId: prev, ordered: idsAscending(blockBytes) };
}

/** 商品マスタ (readLzShohinMaster) にあってバーコードの書き出しに無い商品ID (途中で切れた疑い) */
/**
 * バーコードの前後を比べる (取り込んだ商品と大文字小文字の候補)。商品ID とバーコードを文字として: 増えた・消えた・重複の数 (K4)。
 * 商品名などほかの列の変化は差にしない (見出しの 商品ID・バーコード の位置が変わったら差)
 * @param {{ pre, post, ids: Iterable<string>, cover?: { pre?: object, post?: object } }} p  cover = 同じ回に書き出した商品マスタ (readLzShohinMaster)。全商品がバーコードの書き出しにあること
 * @returns {{ ok: boolean, diffs: Array<{ id, kind: 'header_changed'|'added'|'removed', barcode?: string }> }}
 */
export function barcodeMissing(lz, bc) {
  return [...lz.byId.keys()].filter((id) => !bc.byId.has(id));
}

export function compareBarcodes({ pre, post, ids, cover = null, sentinel = null, sentinelBarcode = LZ_SENTINEL_BARCODE }) {
  if (!pre || !pre.ok || !post || !post.ok) throw new Error('バーコードの前と後 (ok) が要る');
  const diffs = [];
  const pos = (h) => [h.indexOf('商品ID'), h.indexOf('バーコード')].join(',');
  if (pos(pre.header) !== pos(post.header)) diffs.push({ id: null, kind: 'header_changed' });
  // 後の行が前より少ない = 途中で切れた・消えた (前後とも対象より手前で切れて「同じ」に見えるのを防ぐ。Codex #1530 R1 High)
  // 同じ回に書き出した商品マスタの全商品が、バーコードの書き出しにある (9/29 に本物で確かめた: 5,070 商品が全部ある・書き出しは商品ID の順)。
  // 無い = 途中で切れた (行の切れ目でちょうど切れて、行の数も前後で同じに見える場合も。Codex #1530 R2 High)
  if (cover && cover.pre) { const m = barcodeMissing(cover.pre, pre); if (m.length) diffs.push({ id: null, kind: 'missing_in_pre_barcode', count: m.length, head: m.slice(0, 10) }); }
  if (cover && cover.post) { const m = barcodeMissing(cover.post, post); if (m.length) diffs.push({ id: null, kind: 'missing_in_post_barcode', count: m.length, head: m.slice(0, 10) }); }
  // 比べる商品の行が全部そろっていると言えるのは: 商品ごとの行がひとまとまり・その商品が最後の商品でない (後ろに別の商品の行がある)。
  // 最後の商品の 2 本目以降で行の切れ目ちょうどに切れても、商品のそろいと行の数では分からない = 比べる商品が最後なら「確かめられない」(Codex #1530 R3 High)
  if (cover) {
    for (const [side, x] of [['pre', pre], ['post', post]]) {
      if (x.grouped === false) diffs.push({ id: null, kind: `barcode_not_grouped_${side}` });
      for (const id of new Set(ids)) if (x.lastId === id) diffs.push({ id, kind: `target_is_last_${side}` });
    }
  }
  // 見張りの商品 (毎晩): 前後のバーコードと商品マスタ (cover) の最後の行が見張り・見張りは比べる商品でない。
  // 片側だけ最後の商品の途中で切れた (+ 増えた / 消えた が隠れる)・両側が同じ所で切れた、のどれも見張りが最後に無い = 差 (Codex #1595 R1 Medium)
  if (sentinel) {
    if (new Set(ids).has(sentinel)) diffs.push({ id: sentinel, kind: 'sentinel_is_target' });
    for (const [side, x] of [['pre', pre], ['post', post]]) diffs.push(...sentinelDiffs({ sentinel, sentinelBarcode, side, barcode: x, shohin: cover && cover[side] ? cover[side] : null }));
  }
  if (post.rows < pre.rows) diffs.push({ id: null, kind: 'rows_decreased', pre: pre.rows, post: post.rows });
  for (const id of new Set(ids)) {
    const a = pre.byId.get(id) || [], b = post.byId.get(id) || [];
    const count = (list) => list.reduce((m, r) => m.set(r, (m.get(r) || 0) + 1), new Map());
    const ca = count(a), cb = count(b);
    for (const [barcode, n] of ca) for (let k = cb.get(barcode) || 0; k < n; k++) diffs.push({ id, kind: 'removed', barcode });
    for (const [barcode, n] of cb) for (let k = ca.get(barcode) || 0; k < n; k++) diffs.push({ id, kind: 'added', barcode });
  }
  return { ok: diffs.length === 0, diffs };
}
