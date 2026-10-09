/**
 * 撮影指示書 (スプレッドシート) の中身を組む純粋関数 (画像制作の新フロー PR-D・2026-10-09)。
 * 設計 = 共有ドライブ システム設計/商品ハブ_画像制作の新フロー_設計_20261008.md §3.4
 * 形 = スタッフが使っている「新商品初動判定」仕様書 (Ver1.3.11・2026-10-08) の撮影依頼書
 *   (「撮影依頼書スプレッドシート表示ルール」「撮影依頼書表示簡略化」「撮影依頼書連携データ」)。
 *   **仕様書はよく変わるので、表示の並びは SHEET_LAYOUT の 1 か所にまとめてある。** 変わったらそこを直す
 *
 * ここは Google にも DB にも触らない。入力 (商品・撮影の種類・概要・カット) → 出力 (ファイル名とシートごとの行) だけ。
 * 書き込みは services/sheets-writer.js、DB と材料集め (shootSheetCutsFor) は services/shoot-sheet-service.js。
 *
 * 🚨 材料は AI の出力 (LP構成・撮影判定) と人の入力。セルは文字 (stringValue) で書く = 数式として評価させない
 *    (sheets-writer が固定している)。先頭に `'` を付ける方式は採らない (`'` がそのまま見えてしまう)
 */
import { createHash } from 'node:crypto';

import { parseConstructionDoc } from './lp-parser.js';

export const SHOOT_SHEET_MODES = { inhouse: '社内撮影', photographer: 'カメラマン撮影' };
/** 仕様書の撮影判定の表記 (②・③) */
export const SHOOT_JUDGEMENT_LABELS = { inhouse: '② 社内撮影', photographer: '③ カメラマン撮影' };
/** 1 タブ目 / 2 タブ目の名前。更新のときはこの名前のタブだけを書き換える (人が足したタブには触らない) */
export const SHEET_MAIN = '撮影依頼書';
export const SHEET_REQUEST = '依頼文';
export const MANAGED_SHEETS = [SHEET_MAIN, SHEET_REQUEST];

/**
 * カット 1 つの欄 = 仕様書の「撮影依頼書連携データ」の撮影カット (No 以外) + こちらで足した欄。
 * 表示しない欄 (内部連携) も持つ — 材料の hash に入る。AI の撮影判定が仕様書の形 (v2・PR-C2) なら、AI のカットがそのまま埋める
 * (lib/lp-shoot.js の SHOOT_V2_CUT_FIELDS は同じ名前。違うのは使う LP 画像だけ: AI は番号の配列 lp_image_nos、ここは表示の lp_image)
 *   priority 優先度 (必須／推奨) / expression_type 撮影表現タイプ / variation 撮影対象バリエーション /
 *   target 撮影対象 (色・種類名＋点数) / content 撮影内容 / purpose 撮影目的 / finish 構図・完成イメージ /
 *   usage 使用用途 / open_required 開封要否 / reference_theme 参考イメージ (AI 参考画像用の短いテーマ名・表示しない)
 *   lp_image 使う LP 画像 (任意。LP に無い実写素材のカットもある) / notice 注意 / required_notice 必ず出す表示 (「実物への貼付不可」など)
 */
export const CUT_FIELDS = ['priority', 'expression_type', 'variation', 'target', 'content', 'purpose', 'finish', 'usage',
  'open_required', 'reference_theme', 'lp_image', 'notice', 'required_notice'];
/** 概要の欄 = 仕様書の撮影依頼書連携データの上部 (撮影判定／撮影担当／開封要否／撮影用送付対象／撮影目的／完成イメージ／使用用途／判定の結論) */
export const SUMMARY_FIELDS = ['judgement', 'shooter', 'open_required', 'send_targets', 'purpose', 'finish', 'usage', 'conclusion'];
export const MAX_CUTS = 40;
export const CUT_FIELD_MAX = 2000;
/** Sheets の 1 セルの上限は 50,000 文字。依頼文などもこれより短く切る */
const CELL_MAX = 5000;

/**
 * ⭐ 人向け撮影依頼書の表示の定義 (ここ 1 か所)。仕様書 Ver1.3.11 の「撮影依頼書スプレッドシート表示ルール」:
 *   上部は「商品コード／商品名／撮影担当／撮影用送付対象／撮影カット数」の概要だけ。
 *   各カットは「カットNo.／撮影内容／撮影対象／完成イメージ／参考イメージ画像」だけ。
 *   撮影表現タイプ・撮影対象バリエーション・撮影目的・使用用途・開封要否 (内部連携) はシートに出さない。
 *   参考イメージ欄は画像だけ (説明文・テーマ名は出さない。参考画像がまだ無いので空欄)。
 *   社内撮影は「必須／推奨」を出す (撮る／撮らないを社内で決める)。カメラマン撮影は出さない (全カット必須で確定)。
 *   「注意」は AI の撮影判定の NG (PR-C) を出すため、値があるときだけの行 (仕様書には無い。要らなければ消す)
 */
export const SHEET_LAYOUT = {
  spec: '新商品初動判定 Ver1.3.11 (2026-10-08)',
  title: '撮影依頼書',
  summary: [
    { label: '商品コード', value: (x) => x.productCode },
    { label: '商品名', value: (x) => x.productName },
    { label: '撮影担当', value: (x) => x.summary.shooter || SHOOT_SHEET_MODES[x.shootMode] },
    { label: '撮影用送付対象', value: (x) => x.summary.send_targets },
    { label: '撮影カット数', value: (x) => `${x.cuts.length}カット` },
  ],
  cutHeading: (c, x) => (x.shootMode === 'inhouse' && c.priority ? `カット${c.no}（${c.priority}）` : `カット${c.no}`),
  cut: [
    { label: '撮影内容', value: (c) => c.content },
    { label: '撮影対象', value: (c) => c.target },
    { label: '完成イメージ', value: (c) => c.finish },
    { label: '参考イメージ', value: () => '' },
    { label: '注意', value: (c) => c.notice, onlyIfValue: true },
  ],
  columnWidths: [140, 640],
};

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

/**
 * カットを決まった形にそろえる (欠けた欄は空文字・長さを切る)。no は 1 からの連番を振り直す。
 * 必ず出す表示 (required_notice・例「実物への貼付不可」) が撮影内容にも完成イメージにも無ければ、完成イメージの後ろに足す
 * (仕様書「補修シート類・撮影依頼書表示ルール」: カメラマン向け表示では撮影内容か完成イメージのどちらかに必ず含める)
 */
export function normalizeCuts(cuts) {
  return (Array.isArray(cuts) ? cuts : []).slice(0, MAX_CUTS).map((c, i) => {
    const o = { no: i + 1 };
    for (const f of CUT_FIELDS) o[f] = cellText(c && c[f], CUT_FIELD_MAX);
    if (o.required_notice && !o.content.includes(o.required_notice) && !o.finish.includes(o.required_notice)) {
      o.finish = o.finish ? `${o.finish}（${o.required_notice}）` : o.required_notice;
    }
    return o;
  });
}

/** 概要をそろえる (欠けた欄は空文字) */
export function normalizeSummary(summary) {
  const o = {};
  for (const f of SUMMARY_FIELDS) o[f] = cellText(summary && summary[f], CUT_FIELD_MAX);
  return o;
}

/**
 * カット数の確認 (シートの概要の下に出す)。仕様書: カット数は別画像として納品される実カット数。
 * ③カメラマン撮影は最低 5 カット・5 カット単位 (5・10・15)。足りないときにカットを作るのは AI 側なので、ここは知らせるだけ
 */
export function cutCountWarning(shootMode, n) {
  if (!n) return '撮るカットがまだありません。LP構成 (要撮影の画像) を確かめるか、このシートに書き足してください';
  if (shootMode === 'photographer' && n < 5) return `カメラマン撮影は最低 5 カットです (いま ${n} カット)。カットを足してから依頼してください`;
  if (shootMode === 'photographer' && n % 5 !== 0) return `カメラマン撮影は 5 カット単位 (5・10・15) です (いま ${n} カット)。足すか減らすかを決めてから依頼してください`;
  return null;
}

/**
 * 撮影指示書の中身。
 * @param {{productCode, productName, shootMode: 'inhouse'|'photographer', folderUrl, summary, cuts, requestText}} input
 * @returns {{title: string, sheets: Array<{name, rows: string[][], format}>}}
 *   rows は文字列だけの 2 次元配列 (stringValue で書く)。format は sheets-writer の書式指定
 */
export function buildShootSheet(input) {
  const mode = input && input.shootMode;
  if (!SHOOT_SHEET_MODES[mode]) throw new Error('撮影の種類は 社内撮影 / カメラマン撮影 のどちらかです');
  const L = SHEET_LAYOUT;
  const x = {
    productCode: cellText(input.productCode), productName: cellText(input.productName), shootMode: mode,
    summary: normalizeSummary(input.summary), cuts: normalizeCuts(input.cuts),
  };
  const rows = [[L.title]];
  const bold = [0];
  const shaded = [];
  for (const it of L.summary) rows.push([it.label, cellText(it.value(x))]);
  const warn = cutCountWarning(mode, x.cuts.length);
  if (warn) { bold.push(rows.length); rows.push(['確認', warn]); }
  for (const c of x.cuts) {
    rows.push([]);
    bold.push(rows.length); shaded.push(rows.length);
    rows.push([L.cutHeading(c, x)]);
    for (const it of L.cut) {
      const v = cellText(it.value(c, x));
      if (it.onlyIfValue && !v) continue;
      rows.push([it.label, v]);
    }
  }
  const sheets = [{
    name: SHEET_MAIN,
    rows,
    format: { boldRows: bold, shadedRows: shaded, frozenRows: 0, columnWidths: L.columnWidths, wrap: true },
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
 * 表示しない欄 (内部連携) も入れる。宛先 (依頼文の 1 行目) は入れない — 宛先を変えただけで全商品が「更新が要る」にならないように
 */
export function shootSheetMaterialHash({ productCode, productName, shootMode, folderUrl, summary, cuts }) {
  const body = canonical({
    v: 2, productCode: cellText(productCode), productName: cellText(productName), shootMode: str(shootMode),
    folderUrl: cellText(folderUrl), summary: normalizeSummary(summary), cuts: normalizeCuts(cuts),
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

// 欄ごとの見出し (先に書いたものを優先)。⑦ の固定見出しと、構図・小物・トーンの見出しのどちらでも拾う。
// 完成イメージは仕様書で「1 文程度」なので、長い 詳細レイアウト・使用カラー (HEX の並び) は拾わない
const FIELD_HEADINGS = {
  composition: [/^構図/, /^商品配置$/],
  props: [/^小物/, /^装飾[・･]演出$/],
  background: [/^背景/],
  tone: [/トーン/],
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
 * 構図・小物・背景・トーンを「完成イメージ」1 欄に寄せる (仕様書の人向け表示は完成イメージだけ)。
 * C2 (AI が仕様書の「構図・完成イメージ」を直接出す) までのつなぎ
 */
export function joinFinish({ composition, props, background, tone } = {}) {
  return [cellText(composition), props ? `小物：${cellText(props)}` : '', background ? `背景：${cellText(background)}` : '',
    tone ? `トーン：${cellText(tone)}` : ''].filter(Boolean).join('／');
}

/**
 * 画像ブロック (⑦ の `# N枚目｜名前` の下) から、撮影指示書の 1 カットぶんを見出しで拾う。
 * AI の撮影判定 (PR-C) が無い画像を埋める最小版
 * @param {string} blockText  ブロックの中身 (見出しの行は含めなくてよい)
 * @param {{label: string, name?: string}} o  label = 使う LP 画像 (N枚目｜名前)
 */
export function cutFromBlock(blockText, { label, name = '' }) {
  const block = str(blockText);
  const material = sectionText(block, /^使用素材$/);
  return {
    priority: '必須',
    lp_image: label,
    content: cutNameFrom(material) || cellText(name, 100) || label,
    finish: joinFinish({
      composition: firstSection(block, FIELD_HEADINGS.composition), props: firstSection(block, FIELD_HEADINGS.props),
      background: firstSection(block, FIELD_HEADINGS.background), tone: firstSection(block, FIELD_HEADINGS.tone),
    }),
    notice: firstSection(block, FIELD_HEADINGS.ng),
  };
}

const imgLabel = (no, name) => {
  const n = cellText(name, 100);
  return `${Number.isInteger(no) ? no + '枚目' : '?枚目'}${n ? '｜' + n : ''}`;
};

/**
 * ⑦形式の LP構成から、`## 使用素材` に「撮影」が出てくる画像を要撮影のカットとして拾う (編集版も AI の判定も無いときの推定)。
 * 構成が読めなければ空配列 (指示書は空で作れる)
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

/** AI の撮影判定 (v2) のカット 1 つ → 撮影指示書のカット。項目名は同じなので写すだけ。lp_image (表示) は呼び手が今の並びから作る */
function cutFromAi(c, lpImage) {
  const o = {};
  for (const f of CUT_FIELDS) o[f] = f === 'lp_image' ? lpImage : str(c && c[f]);
  return o;
}

/**
 * 撮影指示書のカットを、いまの LP構成の並び (PR-B) と AI の撮影判定 (PR-C / PR-C2) から組む (純粋関数)。
 *   - 要撮影か: 編集版の「要撮影」(人が直した値) が正本 (hasEditShoot)。編集版が無ければ AI の needs_shoot、
 *     AI の判定も無ければ使用素材に「撮影」が出てくるかで推定
 *   - カットの中身:
 *     - AI の判定が仕様書の形 (v2・aiCuts あり・PR-C2): AI のカットをそのまま使う (項目名は CUT_FIELDS と同じ)。
 *       要撮影の画像ごとに、その画像を使う AI のカット (lp_image_nos に元の番号があるもの) を AI の順で並べ、
 *       最後に LP に無いカット (lp_image_nos が []) を足す。1 つのカットを複数の画像で使うときは最初の画像のところに 1 回だけ。
 *       要撮影でなくなった画像 (人が「撮影不要」にした・消した) だけを使うカットは載せない
 *     - AI の判定が PR-C の形 (v1): その画像の cut → 撮影内容 / composition・props・background・tone → 完成イメージ (joinFinish) /
 *       ng → 注意 に対応づけ、ほか (撮影対象・優先度・表現タイプ・バリエーション・目的・用途・開封・参考テーマ) は空欄 (優先度だけ「必須」)
 *     AI のカットが無い要撮影の画像 (人が要撮影にした・追加した画像) はブロックの見出しから拾う
 *   🚨 AI の判定は「AI の構成での番号」で付いている。編集版で並べ替え・追加した画像は番号がずれるので、
 *      **今の番号ではなく元の画像 (uid の a<元の番号>) で引く**。追加した画像 (n…) には AI の判定は無い
 * @param {{slots: Array<{uid, no, name, lines: string[], shoot?: boolean}>, hasEditShoot: boolean,
 *          aiImages: Array<{no, needs_shoot, cut, composition, props, background, tone, ng}>|null,
 *          aiCuts?: Array<{lp_image_nos: number[]} & Record<string, string>>|null}} o
 */
export function cutsFromSlots({ slots, hasEditShoot, aiImages, aiCuts = null }) {
  const aiByNo = new Map((Array.isArray(aiImages) ? aiImages : []).map((x) => [x.no, x]));
  const list = Array.isArray(slots) ? slots : [];
  const v2 = Array.isArray(aiCuts) ? aiCuts.filter((c) => c && Array.isArray(c.lp_image_nos)) : null;
  // 元の番号 → いまの表示 (N枚目｜名前)。AI のカットの lp_image に使う (並べ替えても元の画像で引く)
  const labelOfOrig = new Map();
  list.forEach((sl, i) => {
    const origNo = aiNoOfUid(sl.uid);
    if (origNo != null) labelOfOrig.set(origNo, imgLabel(Number.isInteger(sl.no) ? sl.no : i, sl.name));
  });
  const needsOf = (sl) => {
    const origNo = aiNoOfUid(sl.uid);
    const ai = origNo != null ? aiByNo.get(origNo) || null : null;
    return hasEditShoot ? sl.shoot === true : (aiImages ? !!(ai && ai.needs_shoot) : blockMentionsShoot((Array.isArray(sl.lines) ? sl.lines : []).join('\n')));
  };
  // 🚨 表示に出すのは「いま要撮影」の画像だけ (人が片方だけ「撮影不要」にした共有カットに、撮影不要の画像を出さない。
  //    出すと材料の hash も変わらず「LP構成が変わりました」も出ない — Codex PR-C2 名指し2 M)
  const labelOfNeeded = new Map([...labelOfOrig].filter(([n]) => list.some((sl) => aiNoOfUid(sl.uid) === n && needsOf(sl))));
  const lpImageOf = (c) => c.lp_image_nos.map((n) => labelOfNeeded.get(n)).filter(Boolean).join('・');
  const used = new Set();
  const cuts = [];
  for (const [i, sl] of list.entries()) {
    const block = (Array.isArray(sl.lines) ? sl.lines : []).join('\n');
    const origNo = aiNoOfUid(sl.uid);
    const ai = origNo != null ? aiByNo.get(origNo) || null : null;
    if (!needsOf(sl)) continue;
    const no = Number.isInteger(sl.no) ? sl.no : i;
    const fromBlock = cutFromBlock(block, { label: imgLabel(no, sl.name), name: sl.name });
    if (v2) {
      const mine = origNo != null ? v2.filter((c) => c.lp_image_nos.includes(origNo)) : [];
      if (!mine.length) { cuts.push(fromBlock); continue; }
      for (const c of mine) {
        if (used.has(c)) continue;
        used.add(c);
        cuts.push(cutFromAi(c, lpImageOf(c)));
      }
      continue;
    }
    const useAi = ai && ai.needs_shoot && str(ai.cut).trim();
    cuts.push(useAi ? { ...fromBlock, content: ai.cut, finish: joinFinish(ai), notice: ai.ng } : fromBlock);
  }
  // LP に無いが必要な実写 (仕様書: LP構成を正として追認せず、抜けている実写素材を撮影候補にする)
  if (v2) for (const c of v2) if (c.lp_image_nos.length === 0 && !used.has(c)) cuts.push(cutFromAi(c, ''));
  return normalizeCuts(cuts);
}

/**
 * AI の概要 (撮影用送付対象・開封要否・撮影目的・完成イメージ・使用用途・判定の結論) を撮影指示書にそのまま使ってよいか (PR-C2)。
 * 概要は AI が「自分が要撮影とした画像」のために決めたもの。人が編集版で要撮影を変えた (AI と違う画像を要撮影にした・
 * 要撮影を外した・足した画像を要撮影にした) なら、送付対象などが今のカットと合わない — 違う商品を撮影先へ送りかねない
 * (Codex PR-C2 名指し4 M)。編集版が無い (= 要撮影は AI の値) なら使ってよい
 * @param {{slots: Array<{uid, shoot?: boolean}>, hasEditShoot: boolean, aiImages: Array<{no, needs_shoot}>|null}} o
 */
export function aiSummaryStillValid({ slots, hasEditShoot, aiImages }) {
  if (!hasEditShoot) return true;
  const aiNeeded = new Set((Array.isArray(aiImages) ? aiImages : []).filter((x) => x && x.needs_shoot).map((x) => x.no));
  const nowNeeded = new Set();
  for (const sl of Array.isArray(slots) ? slots : []) {
    if (sl.shoot !== true) continue;
    const n = aiNoOfUid(sl.uid);
    if (n == null) return false;   // 人が足した画像を要撮影にした (AI の概要はこの画像を知らない)
    nowNeeded.add(n);
  }
  return nowNeeded.size === aiNeeded.size && [...nowNeeded].every((n) => aiNeeded.has(n));
}
/** AI の概要を使えないときの撮影指示書の概要 (送付対象は人が決める) */
export const SUMMARY_NEEDS_REVIEW = Object.freeze({
  send_targets: '要確認（LP構成の要撮影を人が直したので、AI が決めた撮影用送付対象は使っていません）',
});

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
