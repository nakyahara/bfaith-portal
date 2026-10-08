/**
 * 撮影指示書 (スプレッドシート) の中身を組む純粋関数 (画像制作の新フロー PR-D・2026-10-09)。
 * 設計 = 共有ドライブ システム設計/商品ハブ_画像制作の新フロー_設計_20261008.md §3.4
 *
 * ここは Google にも DB にも触らない。入力 (商品・撮影の種類・カット) → 出力 (ファイル名とシートごとの行) だけ。
 * 書き込みは services/sheets-writer.js、DB と材料集め (shootSheetCutsFor) は services/shoot-sheet-service.js。
 *
 * 🚨 材料は AI の出力 (LP構成) と人の入力。スプレッドシートの数式として解釈されないよう、
 *    書き込みは必ず valueInputOption=RAW (sheets-writer が固定している)。RAW なら `=IMPORTXML(...)` も文字のまま入る。
 *    先頭に `'` を付ける方式は採らない — RAW では `'` がそのまま見えてしまい、
 *    LP構成によく出る「- 箇条書き」の行がすべて `'- …` になる
 */
import { createHash } from 'node:crypto';

import { parseConstructionDoc } from './lp-parser.js';

export const SHOOT_SHEET_MODES = { inhouse: '社内撮影', photographer: 'カメラマン撮影' };
/** 1 タブ目 / 2 タブ目の名前。更新のときはこの名前のタブだけを書き換える (人が足したタブには触らない) */
export const SHEET_MAIN = '撮影指示';
export const SHEET_REQUEST = '依頼文';
export const MANAGED_SHEETS = [SHEET_MAIN, SHEET_REQUEST];
/** カット一覧の見出し (設計 §3.4 の列) */
export const CUT_COLUMNS = ['カット番号', '使う画像', 'カット名', '構図', '小物', '背景', 'トーン', 'NG'];
/** カット 1 つの欄 (API で受ける形。文字列だけ) */
export const CUT_FIELDS = ['label', 'cut', 'composition', 'props', 'background', 'tone', 'ng', 'role', 'title'];
export const MAX_CUTS = 40;
export const CUT_FIELD_MAX = 2000;
/** Sheets の 1 セルの上限は 50,000 文字。依頼文などもこれより短く切る */
const CELL_MAX = 5000;
/** 見出しの行数 (タイトル・商品名・商品コード・撮影の種類・画像フォルダ・空行)。この次の行がカット一覧の見出し */
const HEADER_ROWS = 6;

const str = (v) => (v == null ? '' : String(v));
/** セルに入れる文字: 制御文字 (改行・タブ以外) を落とし、長さを切る */
export function cellText(v, max = CELL_MAX) {
  const s = str(v).replace(/\r\n?/g, '\n').replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/g, '').trim();
  return s.length > max ? s.slice(0, max) : s;
}

/**
 * ファイル名 `撮影指示書_<商品コード>（社内撮影|カメラマン撮影）`。
 * 商品コードの / \ などは Drive では使えるが、Drive for desktop で G: に同期されるので画像フォルダ名と同じく寄せる
 */
export function shootSheetTitle(productCode, shootMode) {
  const code = str(productCode).replace(/[\u0000-\u001f\u007f]/g, '').replace(/[\\/]/g, '／').replace(/[:*?"<>|]/g, '_').trim().slice(0, 80);
  return `撮影指示書_${code || '(商品コードなし)'}（${SHOOT_SHEET_MODES[shootMode] || '撮影'}）`;
}

/** カット 1 つを決まった形にそろえる (欠けた欄は空文字・長さを切る)。no は 1 からの連番を振り直す */
export function normalizeCuts(cuts) {
  return (Array.isArray(cuts) ? cuts : []).slice(0, MAX_CUTS).map((c, i) => {
    const o = { no: i + 1 };
    for (const f of CUT_FIELDS) o[f] = cellText(c && c[f], CUT_FIELD_MAX);
    return o;
  });
}

/**
 * API で受けたカット (画面・B/C から渡す口) を検査する。形が違えば error を返す (黙って空にしない)
 * @returns {{cuts: Array|null, error: string|null}}
 */
export function validateCutsInput(v) {
  if (!Array.isArray(v)) return { cuts: null, error: 'cuts は配列で指定してください' };
  if (v.length > MAX_CUTS) return { cuts: null, error: `カットは ${MAX_CUTS} 個までです` };
  for (const [i, c] of v.entries()) {
    if (!c || typeof c !== 'object' || Array.isArray(c)) return { cuts: null, error: `cuts[${i}] の形が不正です` };
    for (const f of CUT_FIELDS) {
      if (c[f] != null && typeof c[f] !== 'string') return { cuts: null, error: `cuts[${i}].${f} は文字列で指定してください` };
      if (typeof c[f] === 'string' && c[f].length > CUT_FIELD_MAX) return { cuts: null, error: `cuts[${i}].${f} が長すぎます (${CUT_FIELD_MAX} 文字まで)` };
    }
  }
  return { cuts: normalizeCuts(v), error: null };
}

/**
 * 撮影指示書の中身。
 * @param {{productCode, productName, shootMode: 'inhouse'|'photographer', folderUrl, cuts, requestText}} input
 * @returns {{title: string, sheets: Array<{name, rows: string[][], format}>}}
 *   rows は文字列だけの 2 次元配列 (RAW で書く)。format は sheets-writer の書式指定
 */
export function buildShootSheet(input) {
  const mode = input && input.shootMode;
  if (!SHOOT_SHEET_MODES[mode]) throw new Error('撮影の種類は 社内撮影 / カメラマン撮影 のどちらかです');
  const cuts = normalizeCuts(input.cuts);
  const main = [
    ['撮影指示書'],
    ['商品名', cellText(input.productName)],
    ['商品コード', cellText(input.productCode)],
    ['撮影の種類', SHOOT_SHEET_MODES[mode]],
    ['画像フォルダ', cellText(input.folderUrl)],
    [],
    CUT_COLUMNS.slice(),
  ];
  if (cuts.length) {
    for (const c of cuts) {
      main.push([String(c.no), c.label, c.cut, c.composition, c.props, c.background, c.tone, c.ng]);
    }
  } else {
    // 要撮影の画像を LP構成から見つけられなかった。表は残して、人が書き足せるようにする
    main.push(['', '', '(LP構成から撮影が要る画像を見つけられませんでした。撮るカットをここに書き足してください)']);
  }
  const sheets = [{
    name: SHEET_MAIN,
    rows: main,
    format: {
      boldRows: [0, HEADER_ROWS],
      shadedRows: [HEADER_ROWS],
      frozenRows: HEADER_ROWS + 1,
      // A カット番号 / B 使う画像 / C カット名 / D 構図 / E 小物 / F 背景 / G トーン / H NG
      columnWidths: [90, 160, 160, 320, 200, 220, 200, 240],
      wrap: true,
    },
  }];
  if (mode === 'photographer') {
    const lines = cellText(input.requestText, 20000).split('\n').map((l) => [cellText(l)]);
    sheets.push({ name: SHEET_REQUEST, rows: lines.length ? lines : [['']], format: { columnWidths: [640], wrap: true } });
  }
  return { title: shootSheetTitle(input.productCode, mode), sheets };
}

/** キーの順を固定した JSON (hash を安定させる) */
function canonical(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonical).join(',')}]`;
  return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(',')}}`;
}

/**
 * 「作ったときの材料」の hash。今の材料と比べて違えば「LP構成が変わりました → 撮影指示書を更新」を出す。
 * 宛先 (依頼文の 1 行目) は入れない — 宛先を変えただけで全商品が「更新が要る」にならないように
 */
export function shootSheetMaterialHash({ productCode, productName, shootMode, folderUrl, cuts }) {
  const body = canonical({
    v: 1, productCode: cellText(productCode), productName: cellText(productName), shootMode: str(shootMode),
    folderUrl: cellText(folderUrl), cuts: normalizeCuts(cuts),
  });
  return createHash('sha256').update(body, 'utf8').digest('hex');
}

/**
 * 撮影依頼文 (2 タブ目に入れる)。画面の buildShootRequestText (detail.ejs の @image-flow) と同じ文面。
 * smoke が同じ入力で両方を呼んで、1 文字も違わないことを確かめている
 */
export function shootRequestBody({ mention, productName, sheetUrl, folderUrl }) {
  const lines = [];
  const m = str(mention).trim();
  if (m) lines.push(m);
  lines.push('お世話になっています。', '下記商品の商品撮影をお願いします。', '',
    '【商品名】' + (str(productName).trim() || '(商品名が未入力です)'), '',
    '【撮影指示書】', str(sheetUrl).trim() || '(未登録)', '',
    '【商品画像フォルダ】', str(folderUrl).trim() || '(未登録)', '',
    'よろしくお願いいたします。');
  return lines.join('\n');
}

// ─── LP構成 (⑦形式の Markdown) から要撮影のカットを拾う (PR-B・C がマージされるまでの最小版) ───

/** 画像ブロックの `## 見出し` の中身。次の H1/H2 まで (H3 以下の小見出しは中身に含める — lp-lint と同じ見方) */
export function sectionText(block, headingRe) {
  const lines = str(block).replace(/\r\n?/g, '\n').split('\n');
  const out = [];
  let on = false;
  let found = false;
  for (const line of lines) {
    const h = /^(#{1,2})\s+(.+?)\s*$/.exec(line);
    if (h) {
      if (on) break;   // 最初に当たった見出しだけ
      if (headingRe.test(h[2])) { on = true; found = true; }
      continue;
    }
    if (on) out.push(line);
  }
  return found ? out.join('\n').trim() : '';
}

/** 撮影が要るかを見る語: 「撮影」。ただし「撮影不要」「撮影済み」「撮影しない」は要らない側 */
export function mentionsShoot(text) {
  const s = str(text).replace(/撮影(?:不要|済み?|しない)/g, '');
  return /撮影/.test(s);
}

/** 使用素材から撮るカットの名前を取る (「撮影: 使用シーン」「撮影（粉末アップ）」など)。取れなければ null */
export function cutNameFrom(materialText) {
  const m = /撮影\s*[:：（(]\s*([^\n・／/,、）)]+)/.exec(str(materialText));
  return m ? m[1].trim() : null;
}

// 欄ごとの見出し (先に書いたものを優先)。B・C の形 (構図・小物・トーン) と ⑦ の固定見出しのどちらでも拾う
const FIELD_HEADINGS = {
  composition: [/^構図/, /^商品配置$/, /^(詳細レイアウト|レイアウト)$/],
  props: [/^小物/, /^装飾[・･]演出$/],
  background: [/^背景/],
  tone: [/トーン/, /^使用カラー$/],
  ng: [/^NG/],
};
const firstSection = (block, res) => {
  for (const re of res) {
    const t = sectionText(block, re);
    if (t) return t;
  }
  return '';
};

/** 画像ブロックの使用素材に撮影が出てくるか (編集版・AI の判定が無いときの推定) */
export function blockMentionsShoot(blockText) {
  return mentionsShoot(sectionText(blockText, /^使用素材$/));
}

/**
 * 画像ブロック (⑦ の `# N枚目｜名前` の下) から、撮影指示書の 1 カットぶんを見出しで拾う。
 * AI の撮影判定 (PR-C) が無い画像を埋める最小版
 * @param {string} blockText  ブロックの中身 (見出しの行は含めなくてよい)
 * @param {{label: string, name?: string}} o  label = 使う画像 (N枚目｜名前)
 */
export function cutFromBlock(blockText, { label, name = '' }) {
  const block = str(blockText);
  const material = sectionText(block, /^使用素材$/);
  return {
    label,
    cut: cutNameFrom(material) || cellText(name, 100) || label,
    composition: firstSection(block, FIELD_HEADINGS.composition),
    props: firstSection(block, FIELD_HEADINGS.props),
    background: firstSection(block, FIELD_HEADINGS.background),
    tone: firstSection(block, FIELD_HEADINGS.tone),
    ng: firstSection(block, FIELD_HEADINGS.ng),
    role: sectionText(block, /^画像の役割$/),
    title: sectionText(block, /^メイン見出し$/),
  };
}

const imgLabel = (no, name) => {
  const n = cellText(name, 100);
  return `${Number.isInteger(no) ? no + '枚目' : '?枚目'}${n ? '｜' + n : ''}`;
};

/**
 * ⑦形式の LP構成から、`## 使用素材` に「撮影」が出てくる画像を要撮影のカットとして拾う (編集版も AI の判定も無いときの推定)。
 * 構成が読めなければ空配列 (指示書は空の表で作れる)
 */
export function cutsFromComposeText(outputText) {
  let doc;
  try { doc = parseConstructionDoc(str(outputText)); } catch { return []; }
  const cuts = [];
  for (const im of (doc && doc.images) || []) {
    const block = str(im.rawBlockText);
    if (!blockMentionsShoot(block)) continue;
    cuts.push(cutFromBlock(block, { label: imgLabel(im.no, im.name), name: im.name }));
  }
  return normalizeCuts(cuts);
}

/** AI の構成のままの画像の uid (a0, a1 …) → AI の構成での番号。追加した画像 (n…) などは null */
export function aiNoOfUid(uid) {
  const m = /^a(\d{1,2})$/.exec(str(uid));
  return m ? Number(m[1]) : null;
}

/**
 * 撮影指示書のカットを、いまの LP構成の並び (PR-B) と AI の撮影判定 (PR-C) から組む (純粋関数)。
 *   - 要撮影か: 編集版の「要撮影」(人が直した値) が正本 (hasEditShoot)。編集版が無ければ AI の needs_shoot、
 *     AI の判定も無ければ使用素材に「撮影」が出てくるかで推定
 *   - カットの中身 (カット名・構図・小物・背景・トーン・NG): AI の判定のその画像 (needs_shoot で中身があるもの)。
 *     無い画像 (人が要撮影にした・追加した画像) はブロックの見出しから拾う
 *   🚨 AI の判定は「AI の構成での番号」で付いている。編集版で並べ替え・追加した画像は番号がずれるので、
 *      **今の番号ではなく元の画像 (uid の a<元の番号>) で引く**。追加した画像 (n…) には AI の判定は無い
 * @param {{slots: Array<{uid, no, name, role, title, lines: string[], shoot?: boolean}>, hasEditShoot: boolean,
 *          aiImages: Array<{no, needs_shoot, cut, composition, props, background, tone, ng}>|null}} o
 */
export function cutsFromSlots({ slots, hasEditShoot, aiImages }) {
  const aiByNo = new Map((Array.isArray(aiImages) ? aiImages : []).map((x) => [x.no, x]));
  const cuts = [];
  for (const [i, sl] of (Array.isArray(slots) ? slots : []).entries()) {
    const block = (Array.isArray(sl.lines) ? sl.lines : []).join('\n');
    const origNo = aiNoOfUid(sl.uid);
    const ai = origNo != null ? aiByNo.get(origNo) || null : null;
    const needs = hasEditShoot ? sl.shoot === true : (aiImages ? !!(ai && ai.needs_shoot) : blockMentionsShoot(block));
    if (!needs) continue;
    const no = Number.isInteger(sl.no) ? sl.no : i;
    const fromBlock = cutFromBlock(block, { label: imgLabel(no, sl.name), name: sl.name });
    const useAi = ai && ai.needs_shoot && str(ai.cut).trim();
    cuts.push({
      ...fromBlock,
      ...(useAi ? { cut: ai.cut, composition: ai.composition, props: ai.props, background: ai.background, tone: ai.tone, ng: ai.ng } : {}),
      role: str(sl.role) || fromBlock.role,
      title: str(sl.title) || fromBlock.title,
    });
  }
  return normalizeCuts(cuts);
}

/**
 * 撮影指示書を作れない理由 (画面のボタンと API で同じ判定)。null なら作れる
 * @param {{shootMode, folderId, configured: boolean}} o
 */
export function shootSheetBlockReason({ shootMode, folderId, configured }) {
  if (!SHOOT_SHEET_MODES[shootMode]) return '撮影判定が「社内撮影」か「カメラマン撮影」のときに作れます';
  if (!folderId) return '画像フォルダ (Drive のフォルダの URL) が無いので、置き場がありません。画像タブの「画像フォルダから自動セット」に入れてください';
  if (!configured) return 'Google のサービスアカウント (GOOGLE_SERVICE_ACCOUNT_KEY) が未設定なので作れません。管理者に連絡してください';
  return null;
}

/** スプレッドシートの URL (ID から) */
export function spreadsheetUrl(id) {
  return `https://docs.google.com/spreadsheets/d/${id}/edit`;
}
