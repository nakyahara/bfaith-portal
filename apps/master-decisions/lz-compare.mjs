/**
 * lz-compare.mjs — ロジザード用 CSV の影運転の突き合わせ (③b-2a。設計 = AI_reference CompanyDB構想/10 §6.2「③b-2」契約 v1〜v3)
 *   GAS の出力と、NE の取得の値から作った CSV (lz-csv.mjs) を、コード (1 列目のバイト) で 1 行ずつ比べる。読むだけ・書くのは呼び手
 *
 * 差の分け方 (契約 v2 H4 / v3 H2 = 報告の分類と合否は別に持つ):
 *   shape      出力の形 (BOM・改行・最後の改行・見出し・列の数・引用符の付き方・読めない CSV)
 *   input      入力の値・集合の違い = GAS が読んだ入力 (repro = その入力をこちらの変換に通したもの) で GAS と同じものが作れた差だけ (時刻のずれ)
 *   allowed    許すと決めた仕様の差 (ALLOWED に書いたものだけ = 中原さんが決めて設計書に書いたもの)
 *   undeterminable 判定できない (推測で書いた形が GAS と違った・同じでも GAS の入力で確かめられない・元のコードが無くて作れない行・比べる材料が無い)
 *   unexplained 説明できない差 (上のどれでもない)
 * 🚨 合格 = 説明できない差 0・許していない形の差 0・判定できない 0 (時刻のずれは GAS の入力で再現できたものだけ)
 */
import iconv from 'iconv-lite';

export const LZ_COMPARE_VERSION = 'lzc-v1';
/** 許すと決めた仕様の差 (中原さん 2026-09-27) */
export const ALLOWED = Object.freeze({ row_order: '並びだけの違い (L-3「困らない」。③c の少しの件数の実機の取込でも確かめる)' });

const latin1 = (b) => Buffer.from(b).toString('latin1');
/** 報告に出す形 = 読める文字 (CP932 で戻す) とバイト */
const show = (b) => ({ text: iconv.decode(Buffer.from(b), 'cp932'), hex: Buffer.from(b).toString('hex') });

/**
 * CSV (Shift_JIS のバイト) を読む。区切り・引用符・改行は 1 バイト (Shift_JIS の 2 バイト目は 0x40 以上 = 重ならない)
 * 閉じ引用符のあとは区切り・改行・終わりだけ (それ以外 = after_quote = 壊れた CSV。"a,"b を "a,b" と同じに読まない)
 * @returns {{ records: Array<{ cells: Buffer[], quoted: boolean[] }>, shape: { bom, crlf, lf, cr, trailing_newline, bare_quote, after_quote, unterminated } }}
 */
export function parseCsvBytes(buf) {
  const b = Buffer.from(buf);
  const shape = { bom: false, crlf: 0, lf: 0, cr: 0, trailing_newline: false, bare_quote: 0, after_quote: 0, unterminated: false };
  let i = 0;
  if (b[0] === 0xef && b[1] === 0xbb && b[2] === 0xbf) { shape.bom = true; i = 3; }
  const records = [];
  let cells = [], quoted = [], cell = [], inQ = false, afterQ = false, wasQ = false, fieldStart = true, lastWasNewline = false;
  const endCell = () => { cells.push(Buffer.from(cell)); quoted.push(wasQ); cell = []; wasQ = false; fieldStart = true; };
  const endRecord = () => { endCell(); records.push({ cells, quoted }); cells = []; quoted = []; };
  for (; i < b.length; i++) {
    const c = b[i];
    lastWasNewline = false;
    if (inQ) {
      if (c === 0x22) { if (b[i + 1] === 0x22) { cell.push(0x22); i++; } else { inQ = false; afterQ = true; } } else cell.push(c);
      continue;
    }
    if (afterQ) { afterQ = false; if (c !== 0x2c && c !== 0x0d && c !== 0x0a) shape.after_quote++; }
    if (c === 0x22) {
      if (fieldStart) { inQ = true; wasQ = true; fieldStart = false; } else { shape.bare_quote++; cell.push(c); }
      continue;
    }
    if (c === 0x2c) { endCell(); continue; }
    if (c === 0x0d || c === 0x0a) {
      if (c === 0x0d && b[i + 1] === 0x0a) { shape.crlf++; i++; } else if (c === 0x0d) shape.cr++; else shape.lf++;
      endRecord();
      lastWasNewline = true;
      continue;
    }
    fieldStart = false;
    cell.push(c);
  }
  if (inQ) shape.unterminated = true;
  if (lastWasNewline) shape.trailing_newline = true;
  else if (cell.length || cells.length || wasQ || !fieldStart || records.length === 0) endRecord();
  return { records, shape };
}

/** CSV の中身の行 (見出しを除く) をコードごとに集める。同じコードは上書きせずに全部持つ */
function byKey(records) {
  const m = new Map();
  records.forEach((r, idx) => {
    const k = latin1(r.cells[0] || Buffer.alloc(0));
    if (!m.has(k)) m.set(k, []);
    m.get(k).push({ ...r, idx });
  });
  return m;
}
const same = (a, b) => Buffer.compare(Buffer.from(a), Buffer.from(b)) === 0;

/**
 * @param {object} p
 * @param {Buffer} p.gas  GAS の出力
 * @param {{ bytes: Buffer, rows: Array<{ key, code_norm, unverified: Array<{col, why}> }>, unmade: Array<{ code_norm, reason }>, file_unverified: string[] }} p.ours  buildLzCsv の結果
 * @param {object|Buffer|null} [p.repro]  GAS が読んだ入力をこちらの変換に通したもの (buildLzCsv の結果。時刻のずれを確かめる材料。無ければ時刻のずれとは言えない)。
 *   推測の形を「確かめた」にするには buildLzCsv の結果 (行ごとの印つき) が要る (バイトだけ = 確かめられない)
 * @param {number[]} p.compareCols  比べる列 (新商品の人の 3 列は外す)
 * @param {string[]} [p.header]  列の名前 (比べた列・比べない列を報告に出す)
 * @param {string} [p.setCheck]  集合を比べられないときの理由 (新商品で、ロジザードにある商品の一覧が無い など) = 判定できない
 */
export function compareLz({ gas, ours, repro = null, compareCols, setCheck = null, header = null }) {
  const reproBytes = repro == null ? null : Buffer.isBuffer(repro) ? repro : repro.bytes;
  const reproMeta = repro && !Buffer.isBuffer(repro) ? new Map(repro.rows.map((r) => [r.key, r])) : null;
  const reproUnmade = repro && !Buffer.isBuffer(repro) ? new Set((repro.unmade || []).map((u) => u.code_norm)) : null;
  const G = parseCsvBytes(gas), O = parseCsvBytes(ours.bytes), R = reproBytes ? parseCsvBytes(reproBytes) : null;
  const out = {
    version: LZ_COMPARE_VERSION,
    counts: { gas_rows: Math.max(0, G.records.length - 1), ours_rows: Math.max(0, O.records.length - 1), same_rows: 0 },
    shape: [], allowed: [], input: [], undeterminable: [], unexplained: [], rules_first_seen: [],
    repro: R ? (reproMeta ? 'given' : 'given_bytes_only') : 'none',
    compared_cols: compareCols.map((i) => (header ? header[i] : i)),
    not_compared_cols: header ? header.filter((_, i) => !compareCols.includes(i)) : [],
  };
  const push = (cls, x) => out[cls].push(x);
  if (setCheck) push('undeterminable', { what: 'set_not_checked', reason: setCheck });   // 集合 (どの商品が載るか) は確かめていない
  // ── 1. 形 ──
  for (const k of ['bom', 'trailing_newline']) if (G.shape[k] !== O.shape[k]) push('shape', { what: k, gas: G.shape[k], ours: O.shape[k] });
  const nl = (s) => (s.cr ? 'cr' : s.lf && s.crlf ? 'mixed' : s.lf ? 'lf' : 'crlf');
  if (nl(G.shape) !== nl(O.shape)) push('shape', { what: 'newline', gas: nl(G.shape), ours: nl(O.shape) });
  if (G.shape.bare_quote || G.shape.after_quote || G.shape.unterminated) push('unexplained', { what: 'gas_csv_broken', bare_quote: G.shape.bare_quote, after_quote: G.shape.after_quote, unterminated: G.shape.unterminated });
  const gh = G.records[0] || { cells: [] }, oh = O.records[0] || { cells: [] };
  if (gh.cells.length !== oh.cells.length || gh.cells.some((c, i) => !same(c, oh.cells[i]) || gh.quoted[i] !== oh.quoted[i])) {   // 見出しの引用符も (Codex #1498 R2 Low)
    push('shape', { what: 'header', gas: gh.cells.map((c) => show(c).text), ours: oh.cells.map((c) => show(c).text) });
  }
  const width = oh.cells.length;
  const gBody = G.records.slice(1), oBody = O.records.slice(1);
  const badWidth = gBody.filter((r) => r.cells.length !== width);
  if (badWidth.length) push('shape', { what: 'gas_row_width', rows: badWidth.length, first: latin1(badWidth[0].cells[0] || Buffer.alloc(0)) });
  for (const f of ours.file_unverified || []) {
    // 推測で書いたファイルの形 (0 件など): 見出しの行まで同じなら確かめたことにする
    if (same(gas, ours.bytes)) out.rules_first_seen.push({ what: f }); else push('undeterminable', { what: `file_${f}` });
  }
  // ── 2. 行 (コードで) ──
  const gm = byKey(gBody), om = byKey(oBody), rm = R ? byKey(R.records.slice(1)) : null;
  const meta = new Map(ours.rows.map((r) => [r.key, r]));   // 元のコードは英数字と - _ だけ (0041 の CHECK) = latin1 の文字と同じ
  for (const [k, list] of gm) if (list.length > 1) push('unexplained', { what: 'gas_duplicate_code', code: k, rows: list.length });
  for (const [k, list] of om) if (list.length > 1) push('unexplained', { what: 'ours_duplicate_code', code: k, rows: list.length });
  const unmadeByNorm = new Map((ours.unmade || []).map((u) => [u.code_norm, u]));
  const orderG = [], orderO = [];
  for (const [k, gl] of gm) {
    const ol = om.get(k);
    if (!ol) {
      const u = unmadeByNorm.get(k.toLowerCase());
      if (u) { push('undeterminable', { what: 'unmade', code: k, reason: u.reason }); continue; }
      const rl = rm && rm.get(k);
      if (rl && rl.length === 1 && gl.length === 1 && rowSame(rl[0], gl[0], compareCols)) {
        const qd = compareCols.filter((c) => rl[0].quoted[c] !== gl[0].quoted[c]);
        if (qd.length) push('shape', { what: 'quoting', code: k, cols: qd, note: '再現の行と引用符の付き方が違う (Codex #1498 R3)' });
        else push('input', { what: 'only_gas', code: k, why: 'GAS の入力にあって NE の取得に無い (再現できた)' });
      }
      else push('unexplained', { what: 'only_gas', code: k });
      continue;
    }
    if (gl.length !== 1 || ol.length !== 1) continue;   // 重複は上で数えた
    orderG.push([gl[0].idx, k]); orderO.push([ol[0].idx, k]);
    const g = gl[0], o = ol[0];
    const m = meta.get(k);
    const unv = (col) => (m ? m.unverified.filter((u) => u.col === col) : []);
    let rowSameAll = true;
    for (const col of compareCols) {
      const gc = g.cells[col] ?? Buffer.alloc(0), oc = o.cells[col] ?? Buffer.alloc(0);
      if (same(gc, oc)) {
        if (g.quoted[col] !== o.quoted[col]) { push('shape', { what: 'quoting', code: k, col, gas: g.quoted[col], ours: o.quoted[col] }); rowSameAll = false; }
        const u = unv(col);
        if (u.length) {
          // 出力が同じでも、GAS が読んだ入力が同じ文字・形だった証拠が無ければ確かめたことにしない (入力がもともと「?」だったかもしれない)
          const rr = reproMeta && reproMeta.get(k), rl = rm && rm.get(k);
          const confirmed = !!rr && !!rl && rl.length === 1 && same(rl[0].cells[col] ?? Buffer.alloc(0), gc)
            && u.every((x) => rr.unverified.some((y) => y.col === col && y.why === x.why && (x.ch == null || y.ch === x.ch)));
          if (confirmed) for (const x of u) out.rules_first_seen.push({ code: k, col, why: x.why, ...(x.ch ? { ch: x.ch, cp: x.cp } : {}) });
          else push('undeterminable', { what: 'unverified_unconfirmed', code: k, col, why: u.map((x) => x.why), note: 'GAS の出力と同じでも、GAS が読んだ入力で同じ文字・形だったか確かめられない' });
        }
        continue;
      }
      rowSameAll = false;
      const d = { code: k, col, gas: show(gc), ours: show(oc) };
      if (unv(col).length) { push('undeterminable', { what: 'unverified_rule', ...d, why: unv(col).map((u) => u.why) }); continue; }
      const rl = rm && rm.get(k);
      if (rl && rl.length === 1 && same(rl[0].cells[col] ?? Buffer.alloc(0), gc)) {
        if (rl[0].quoted[col] !== g.quoted[col]) push('shape', { what: 'quoting', code: k, col, gas: g.quoted[col], repro: rl[0].quoted[col], note: '再現で中身は消えたが引用符の付き方が残る (Codex #1498 R3)' });
        else push('input', { what: 'value', ...d, why: 'GAS の入力からは同じものが作れた (時刻のずれ)' });
      }
      else push('unexplained', { what: 'value', ...d, ...(R ? {} : { note: 'GAS の入力が無い = 時刻のずれとは言えない' }) });
    }
    if (rowSameAll) out.counts.same_rows++;
  }
  for (const [k, ol] of om) {
    if (gm.has(k)) continue;
    if (ol.length !== 1) continue;
    // GAS の入力に無いと言えるのは、再現の材料 (行ごとの記録つき) があって、その材料で作れなかった行でもないときだけ (Codex #1498 R2 M)
    if (rm && !rm.has(k) && reproUnmade && reproUnmade.has(k.toLowerCase())) push('undeterminable', { what: 'only_ours', code: k, reason: 'repro_unmade', note: '再現の材料で作れなかった = GAS の入力に無いとは言えない' });
    else if (rm && !rm.has(k) && reproUnmade) push('input', { what: 'only_ours', code: k, why: 'GAS の入力に無い (NE の取得のほうが新しい)' });
    else push('unexplained', { what: 'only_ours', code: k, ...(rm && !rm.has(k) ? { note: 'バイトだけの再現の材料 = 作れなかった行か分からない' } : {}) });
  }
  // 作れなかった行のうち、GAS にも無いもの (大文字・小文字の違いも見る)
  const gLower = new Set([...gm.keys()].map((k) => k.toLowerCase()));
  for (const u of ours.unmade || []) if (!gLower.has(u.code_norm)) push('undeterminable', { what: 'unmade', code: u.code_norm, reason: u.reason });
  // ── 3. 並び (両方にある行の順) ──
  const seqG = orderG.sort((a, b) => a[0] - b[0]).map((x) => x[1]), seqO = orderO.sort((a, b) => a[0] - b[0]).map((x) => x[1]);
  if (seqG.some((k, i) => k !== seqO[i])) push('allowed', { what: 'row_order', why: ALLOWED.row_order });
  // ── 4. 合否 ──
  const shapeBad = out.shape.length;
  out.verdict = !shapeBad && !out.undeterminable.length && !out.unexplained.length ? 'pass' : 'fail';
  out.summary = {
    shape: shapeBad, allowed: out.allowed.length, input: out.input.length, undeterminable: out.undeterminable.length, unexplained: out.unexplained.length,
    rules_first_seen: out.rules_first_seen.length,
  };
  return out;
}
function rowSame(a, b, cols) {
  return cols.every((c) => same(a.cells[c] ?? Buffer.alloc(0), b.cells[c] ?? Buffer.alloc(0)));
}

/**
 * ロジザードにある商品の一覧 (auto-barcode.js ② のバーコードマスタ.csv) → 商品ID の集合。
 * GAS は A 列を使う (「バーコード情報」の A 列 = 商品ID)。A 列の見出しが「商品ID」でなければ判定できない
 * @returns {{ ids: Set<string>|null, reason: string|null, rows: number }}
 */
/**
 * GAS が読む NE の品番マスタ (logi_hinban.csv) の形 (2026-09-28 に実ファイルで確かめた: Shift_JIS・CRLF・最後に改行あり・全部の値が引用符つき・31 列)。
 * GAS が使う列 = H 形式/型番 (元の書き方)・B 商品名 (ふりがなも同じ)・V 取引先id・X 仕入単価
 */
export const LOGI_HINBAN = Object.freeze({
  file: 'logi_hinban.csv',
  header: Object.freeze(['品番', '商品名', 'ふりがな', 'メモ', 'シーズン年', 'シーズンid', '入数', '形式/型番', '服種区分', '仕入区分', '出荷形態区分', '区分1', '区分2', '区分3', '区分4', '区分5', '区分6', '区分7',
    '大分類', '中分類', '小分類', '取引先id', '設定上代', '仕入単価', '売上原価', 'ブランドid', 'ブランド記号', 'ブランド名', 'サイズid', '色id', 'バーコード']),
  cols: Object.freeze({ code: 7, name: 1, supplier: 21, cost: 23 }),
});
/**
 * GAS が読んだ入力をこちらの変換に通すための項目にする (契約 v3 H2 = 固定した GAS の入力での再現)。
 * 仕入単価は GAS の入力では整数の文字 (実測 5,008 / 5,008) = そのまま使う。整数でない = 推測の印。見出しが違う = 使わない (ok: false)。
 * 列の数が違う行が 1 つでもあれば使わない (ok: false)。捨てた行の商品が「GAS の入力に無い」= 時刻のずれ と取り違えられるため (Codex #1504 R1 High)
 * @returns {{ ok: boolean, reason: string|null, items: object[], rows: number, bad_rows: number }}
 */
export function itemsFromLogiHinban(buf) {
  const P = parseCsvBytes(buf);
  if (P.shape.unterminated || P.shape.bare_quote || P.shape.after_quote) return { ok: false, reason: 'logi_hinban_broken', items: [], rows: 0, bad_rows: 0 };
  const dec = (b) => iconv.decode(Buffer.from(b), 'cp932');
  const head = (P.records[0] || { cells: [] }).cells.map(dec);
  if (head.length !== LOGI_HINBAN.header.length || head.some((h, i) => h !== LOGI_HINBAN.header[i])) return { ok: false, reason: 'logi_hinban_header', items: [], rows: 0, bad_rows: 0 };
  const C = LOGI_HINBAN.cols, items = [];
  let bad = 0;
  for (const r of P.records.slice(1)) {
    if (r.cells.length !== LOGI_HINBAN.header.length) { bad++; continue; }
    const code = dec(r.cells[C.code]), cost = dec(r.cells[C.cost]);
    items.push({ code_norm: code.toLowerCase(), ne_code: code || null, code_reason: code ? null : 'no_code', name: dec(r.cells[C.name]),
      cost_text: cost, cost_unverified: /^(0|[1-9]\d*)$/.test(cost) ? null : 'cost_shape', supplier: dec(r.cells[C.supplier]) });
  }
  if (bad) return { ok: false, reason: 'logi_hinban_row_width', items: [], rows: P.records.length - 1, bad_rows: bad };
  return { ok: true, reason: null, items, rows: P.records.length - 1, bad_rows: 0 };
}

export function lzIdsFromBarcodeMaster(buf) {
  const P = parseCsvBytes(buf);
  if (P.shape.unterminated || P.shape.bare_quote || P.shape.after_quote) return { ids: null, reason: 'barcode_master_broken', rows: 0 };
  const head = P.records[0];
  const h0 = head ? Buffer.from(head.cells[0]).toString('latin1') : '';
  if (h0 !== Buffer.from([0x8f, 0xa4, 0x95, 0x69, 0x49, 0x44]).toString('latin1')) return { ids: null, reason: 'barcode_master_header', rows: 0 };   // 「商品ID」の Shift_JIS
  const ids = new Set(P.records.slice(1).map((r) => latin1(r.cells[0] || Buffer.alloc(0))).filter((x) => x !== ''));
  return { ids, reason: null, rows: P.records.length - 1 };
}
