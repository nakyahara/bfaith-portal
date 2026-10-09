/**
 * LP 構成の lint — 設計 §6。正本は**仕様書の「出力形式」タブ**。
 *
 * 2 段構え:
 *   A. テキストの検査 (1〜12)  … 仕様書が「必須」「厳守」と書いているルールをそのまま見る
 *   B. パース結果の検査 (13〜19) … 出力を **lp-tool の本番パーサー** (lib/lp-parser.js) に通して、
 *      その結果を見る。テキストが整っていても、**人が lp-tool に貼って読めなければ意味がない**
 *      (Codex R2 #2「Schema では意味ずれは潰れない」への対応)。
 *
 * 🚨 ここが**正本**。実行役 (Claude) の自己申告の lint は参考値で、
 *    `accepted` を受け取るかどうかはこの関数の結果で決める (設計 §6)。
 *
 * 使うのは 2 箇所:
 *   - `lib/lp-compose.js` の submitResult (accepted の可否)
 *   - service-api の `/lp-compose/jobs/:id/lint` (実行役が出す前に自分で直すため)
 */
import { parseConstructionDoc, isBlank } from './lp-parser.js';

/** 冒頭の 3 見出し (仕様書「出力形式」の “最終ヘッダー”)。この順で固定 */
export const HEADER_LINES = [
  '# LP制作システム V2.1',
  '## ⑦ AI画像生成プロンプト',
  '### AI画像生成プロンプト 出力テンプレート V2.2',
];

/** 共通ブロック (仕様書「出力形式」の “共通ブロック”)。見出し名・順序固定 */
export const COMMON_BLOCKS = ['共通生成条件', '共通使用カラー', '商品再現ルール'];

/** 終端ブロック (同 “終端ブロック”)。全画像の後にこの順 */
export const TAIL_BLOCKS = ['共通NG事項', '共通生成後チェック'];

/** 画像内の固定見出し 15 個 (同 “画像内固定見出し”)。表記・順序を固定 */
export const IMAGE_HEADINGS = [
  '画像の役割', '目的', 'メイン見出し', 'サブ見出し', '本文',
  'バッジ・補足', '商品配置', '背景・シーン', '装飾・演出',
  '使用カラー', '使用素材', '詳細レイアウト', '生成指示', 'NG事項', '生成後チェック',
];

/**
 * そのうち **パーサーが名前付きの項目として拾うもの** (lp-parser.js の parseImageBlock)。
 * 🚨 ここに無い見出しは、パーサーでは「その他項目」のバケツ (`badgeSupplement`) に落ちる。
 *    = 仕様書が先に進んでパーサーが追いついていない印なので、検査 19 が警告を出す。
 *    この一覧がパーサーの実態と合っているかは
 *    `scripts/test-ph-lp-compose-lint.mjs` がパーサーを**実際に叩いて**確かめる
 *    (手で書いた一覧が古くなっても、そこで壊れて気づける)。
 */
export const PARSER_KNOWN_HEADINGS = [
  '画像の役割', '目的', 'メイン見出し', 'サブ見出し', '本文',
  '商品配置', '背景・シーン', '装飾・演出',
  '使用カラー', '詳細レイアウト', '生成指示', 'NG事項', '生成後チェック',
];

/** 省略表現 (同 “省略禁止”) */
const OMISSION_PHRASES = ['以下同様', '画像2以降も同じ', '前画像に準ずる', '必要枚数分同様', '同様に展開'];

/** 出力禁止 (同 “出力禁止”)。①〜⑥ は**行頭に出てきたとき**だけ見る (本文中の丸数字で誤検知しない) */
const FORBIDDEN_SECTION_RE = /^#{0,6}\s*[①②③④⑤⑥]/m;
const FORBIDDEN_WORDS = ['総評', '最終まとめ', '必要なら次に'];

/** 旧表記 (同 “表記揺れ禁止”)。見出しとして単独で出てきたときだけ弾く */
const LEGACY_HEADINGS = [
  { label: '## 役割', re: /^#{1,6}\s*役割\s*$/m },
  { label: '## 使用カラー（HEX）', re: /^#{1,6}\s*使用カラー\s*[（(]\s*HEX\s*[）)]\s*$/m },
  { label: '## NG', re: /^#{1,6}\s*NG\s*$/m },
];

/** 画像枚数の上下限 (0枚目 + 1〜9 = 2〜10)。サーバが強制する (設計 §6 A-12) */
export const MIN_IMAGES = 2;
export const MAX_IMAGES = 10;

/**
 * 🚨 見出しの**階層も見る** (codex exec review P2)。
 *    `#{1,6}` で見ていたときは、`## 0枚目｜サムネイル` のように階層が違っても
 *    検査 4〜7 を通ってしまった。仕様書は `# N枚目｜役割名` と階層ごと決めている
 *    (lp-tool の取込互換のため)。
 */
export const IMAGE_HEADING_RE = /^#(?!#)\s*([0-9０-９]+)\s*枚目\s*[｜|]\s*(.+?)\s*$/;
/** `# 共通…` のブロック見出し (H1 ちょうど) */
export const BLOCK_HEADING_RE = /^#(?!#)\s*(.+?)\s*$/;
/**
 * 画像ブロックの中の `## 見出し` (H2 ちょうど)。
 * 🚨 `###` を拾わない — 「詳細レイアウト」の中には
 *    `### キャンバス構成` のような小見出しが入る (仕様書のテンプレートそのもの)。
 *    `^##` だけだと `### X` を「## 見出し `# X`」として数えてしまい、検査 7 が誤って落ちる
 */
export const SUB_HEADING_RE = /^##(?!#)\s*(.+?)\s*$/;
// ↑ 見出しの決まり (IMAGE / BLOCK / SUB) と sameHeading は lib/lp-edit.js (構成の確認・修正) もブロックの切り出しに使う (export)

const toHalfWidth = (s) => String(s).replace(/[０-９]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xFEE0));
const normalize = (s) => String(s == null ? '' : s).replace(/\r\n?/g, '\n');
/** 「・」と半角中黒の差で落とさない (パーサーも両方許している) */
export const sameHeading = (a, b) => String(a).replace(/[･・]/g, '・').trim() === String(b).replace(/[･・]/g, '・').trim();

/**
 * 構成の全文を検査する。
 *
 * @param {string} output ⑦ の全文
 * @param {{productName?: string|null}} opts productName = その draft の商品名 (検査 17)
 * @returns {{ok: boolean, checks: object, errors: Array, warnings: Array, parsed: object|null}}
 */
export function lintComposition(output, { productName = null } = {}) {
  const text = normalize(output);
  const lines = text.split('\n');
  const errors = [];
  const warnings = [];
  const checks = {};
  const fail = (id, label, detail) => { checks[id] = false; errors.push({ id, label, detail }); };
  const pass = (id) => { checks[id] = true; };
  const warn = (id, label, detail) => { warnings.push({ id, label, detail }); };

  if (!text.trim()) {
    return { ok: false, checks: { 1: false }, parsed: null, warnings,
      errors: [{ id: 1, label: '空', detail: '構成の本文が空です' }] };
  }

  // ─── A. テキストの検査 ──────────────────────────────────

  // 1. テンプレート版の明示
  if (text.includes('AI画像生成プロンプト 出力テンプレート V2.2')) pass(1);
  else fail(1, 'テンプレート版', '「AI画像生成プロンプト 出力テンプレート V2.2」が出力に含まれていません');

  // 2. 冒頭 3 見出し (空行は飛ばして、最初の 3 つの中身がこの順)
  const nonEmpty = lines.filter((l) => l.trim() !== '').map((l) => l.trim());
  const headOk = HEADER_LINES.every((h, i) => nonEmpty[i] === h);
  if (headOk) pass(2);
  else fail(2, '冒頭の3見出し', `冒頭は ${HEADER_LINES.join(' → ')} の順で固定です (実際: ${nonEmpty.slice(0, 3).join(' / ') || 'なし'})`);

  // 画像見出しの位置を先に取る (3・8 の「画像より前/後」を見るのに要る)
  const imageHeads = [];
  lines.forEach((l, i) => {
    const m = l.match(IMAGE_HEADING_RE);
    if (m) imageHeads.push({ index: i, no: Number(toHalfWidth(m[1])), name: m[2], raw: l.trim() });
  });
  const firstImageLine = imageHeads.length ? imageHeads[0].index : lines.length;
  const lastImageLine = imageHeads.length ? imageHeads[imageHeads.length - 1].index : -1;

  // 3. 共通ブロックが画像より前に、この順で
  const blockLine = (name, from, to) => lines.findIndex((l, i) => {
    if (i < from || i >= to) return false;
    const m = l.match(BLOCK_HEADING_RE);
    return !!m && sameHeading(m[1], name);
  });
  const commonAt = COMMON_BLOCKS.map((n) => blockLine(n, 0, firstImageLine));
  if (commonAt.every((i) => i >= 0) && commonAt.every((v, i, a) => i === 0 || a[i - 1] < v)) pass(3);
  else {
    fail(3, '共通ブロック', `画像より前に ${COMMON_BLOCKS.map((b) => `# ${b}`).join(' → ')} をこの順で出してください`);
  }

  // 4 / 5. 最初が 0枚目｜サムネイル、次が 1枚目｜FV
  if (imageHeads[0] && imageHeads[0].no === 0 && sameHeading(imageHeads[0].name, 'サムネイル')) pass(4);
  else fail(4, '0枚目', `最初の画像見出しは「# 0枚目｜サムネイル」です (実際: ${imageHeads[0]?.raw || 'なし'})`);
  if (imageHeads[1] && imageHeads[1].no === 1 && sameHeading(imageHeads[1].name, 'FV')) pass(5);
  else fail(5, '1枚目', `次の画像見出しは「# 1枚目｜FV」です (実際: ${imageHeads[1]?.raw || 'なし'})`);

  // 6. N が 0 から連番
  const seqOk = imageHeads.length > 0 && imageHeads.every((h, i) => h.no === i);
  if (seqOk) pass(6);
  else fail(6, '画像番号の連番', `画像見出しの番号は 0 から連番です (実際: ${imageHeads.map((h) => h.no).join(', ') || 'なし'})`);

  // 7. 各画像に 15 の固定見出しがこの表記・この順
  const blocks = imageHeads.map((h, i) => ({
    ...h,
    body: lines.slice(h.index + 1, i + 1 < imageHeads.length ? imageHeads[i + 1].index : lines.length),
  }));
  // 最後の画像の後ろにある終端ブロックは、画像の中身に数えない
  if (blocks.length) {
    const last = blocks[blocks.length - 1];
    const tailAt = last.body.findIndex((l) => {
      const m = l.match(BLOCK_HEADING_RE);
      return !!m && TAIL_BLOCKS.some((t) => sameHeading(m[1], t));
    });
    if (tailAt >= 0) last.body = last.body.slice(0, tailAt);
  }
  const headingBad = [];
  const unknownHeadings = [];
  for (const b of blocks) {
    const got = b.body.map((l) => l.match(SUB_HEADING_RE)).filter(Boolean).map((m) => m[1].trim());
    if (got.length !== IMAGE_HEADINGS.length || !IMAGE_HEADINGS.every((h, i) => sameHeading(got[i], h))) {
      headingBad.push(`${b.no}枚目 (実際: ${got.join('／') || 'なし'})`);
    }
    for (const g of got) {
      if (!PARSER_KNOWN_HEADINGS.some((k) => sameHeading(k, g))) unknownHeadings.push(`${b.no}枚目「${g}」`);
    }
  }
  if (!headingBad.length && blocks.length) pass(7);
  else fail(7, '画像内の固定見出し', `15 見出しをこの表記・この順で出してください: ${IMAGE_HEADINGS.join('／')} — ${headingBad.join(' / ') || '画像がありません'}`);

  // 8. 終端が 共通NG事項 → 共通生成後チェック (最後の画像より後ろ)。
  //    🚨 **これで文書が終わること**まで見る (codex exec review P2)。
  //    並びと順だけを見ていたときは、`# 共通生成後チェック` の後ろに
  //    `# おまけ` のようなブロックが続いても通った。仕様書は「最後まで完全出力」として
  //    この 2 つを終端に指定しているし、パーサーもその中身を最後の共通ブロックに吸い込む。
  const tailAt = TAIL_BLOCKS.map((n) => blockLine(n, lastImageLine + 1, lines.length));
  const orderOk = lastImageLine >= 0 && tailAt.every((i) => i >= 0) && tailAt[0] < tailAt[1];
  // 最後の終端ブロックより後ろに、別の H1 ブロックが無いこと
  const afterTail = orderOk
    ? lines.slice(tailAt[1] + 1).map((l) => l.match(BLOCK_HEADING_RE)).filter(Boolean).map((m) => m[1])
    : [];
  if (orderOk && afterTail.length === 0) pass(8);
  else if (orderOk) {
    fail(8, '終端ブロック', `# ${TAIL_BLOCKS[1]} で終わります。後ろに別のブロックがあります: ${afterTail.map((h) => `# ${h}`).join(' / ')}`);
  } else {
    fail(8, '終端ブロック', `全画像の後に ${TAIL_BLOCKS.map((b) => `# ${b}`).join(' → ')} をこの順で出してください`);
  }

  // 9. 旧表記が無い
  const legacy = LEGACY_HEADINGS.filter((h) => h.re.test(text)).map((h) => h.label);
  if (!legacy.length) pass(9);
  else fail(9, '旧表記', `旧い見出しが残っています: ${legacy.join(' / ')}`);

  // 10. 省略表現が無い
  const omitted = OMISSION_PHRASES.filter((p) => text.includes(p));
  if (!omitted.length) pass(10);
  else fail(10, '省略表現', `全画像を完全展開してください (見つかった表現: ${omitted.join(' / ')})`);

  // 11. ①〜⑥・総評・最終まとめ・「必要なら次に〜」が無い
  const forbidden = FORBIDDEN_WORDS.filter((w) => text.includes(w));
  if (FORBIDDEN_SECTION_RE.test(text)) forbidden.unshift('①〜⑥ の見出し');
  if (!forbidden.length) pass(11);
  else fail(11, '出力禁止', `⑦ の本文だけを返してください (見つかったもの: ${forbidden.join(' / ')})`);

  // 12. 画像枚数
  if (imageHeads.length >= MIN_IMAGES && imageHeads.length <= MAX_IMAGES) pass(12);
  else fail(12, '画像枚数', `画像は ${MIN_IMAGES}〜${MAX_IMAGES} 枚です (0枚目 + 1〜9。実際: ${imageHeads.length} 枚)`);

  // ─── B. パース結果の検査 ────────────────────────────────
  // テキストが通っても、lp-tool の本番パーサーが読めなければ意味がない

  let parsed = null;
  try { parsed = parseConstructionDoc(text); } catch (e) {
    fail(13, 'パース', `lp-tool のパーサーが読めませんでした: ${String(e?.message || e).slice(0, 200)}`);
    return { ok: false, checks, errors, warnings, parsed: null };
  }

  // 16. templateVersion は V2.2 だけ (未知の版は fail-closed・設計 §6-C)
  if (parsed.templateVersion === 'V2.2') pass(16);
  else fail(16, 'テンプレート版 (パーサー)', `パーサーが V2.2 と判定しませんでした (${parsed.templateVersion})。未知の版は人が確認するまで止めます`);

  // 13. パーサーが見ている枚数が、見出しの枚数と一致する
  if (parsed.images.length === imageHeads.length) pass(13);
  else fail(13, '枚数の一致', `見出しは ${imageHeads.length} 枚ですが、パーサーは ${parsed.images.length} 枚と読みました`);

  // 15. パーサー側の no も 0 から連番
  if (parsed.images.length && parsed.images.every((im, i) => im.no === i)) pass(15);
  else fail(15, '画像番号 (パーサー)', `パーサーが読んだ番号が 0 からの連番になっていません (${parsed.images.map((i) => i.no).join(', ') || 'なし'})`);

  // 14. 各画像の必須項目が非空
  const IMAGE_FIELDS = [
    ['imageRole', '画像の役割'], ['purpose', '目的'], ['mainCopy', 'メイン見出し'],
    ['detailedLayout', '詳細レイアウト'], ['generationInstruction', '生成指示'],
    ['ngItems', 'NG事項'], ['postCheck', '生成後チェック'],
  ];
  const emptyFields = [];
  for (const im of parsed.images) {
    for (const [key, label] of IMAGE_FIELDS) {
      if (isBlank(im[key])) emptyFields.push(`${im.no}枚目の${label}`);
    }
  }
  if (!emptyFields.length && parsed.images.length) pass(14);
  else fail(14, '画像の必須項目', `パーサーが読み取れませんでした: ${emptyFields.join(' / ') || '画像がありません'}`);

  // 17. 商品名が draft と一致する (別商品の内容が混ざっていない)
  if (productName == null || String(productName).trim() === '') {
    checks[17] = null;   // 比べる相手が無い (検査しない)
  } else if (parsed.common.productName && sameProduct(parsed.common.productName, productName)) {
    pass(17);
  } else {
    fail(17, '商品名', `構成の「## 商品」が draft の商品名と違います (構成: ${parsed.common.productName || '未取得'} / draft: ${productName})`);
  }

  // 18. 共通NG / 生成後チェックが非空
  const commonEmpty = [];
  if (isBlank(parsed.common.commonNG)) commonEmpty.push('共通NG事項');
  if (isBlank(parsed.common.confirmationItems)) commonEmpty.push('共通生成後チェック');
  if (!commonEmpty.length) pass(18);
  else fail(18, '終端ブロックの中身', `パーサーが読み取れませんでした: ${commonEmpty.join(' / ')}`);

  // 19. パーサーが構造として知らない見出し → **警告** (落とさない)
  //     仕様書が先に進んでパーサーが追いついていない状態の監視 (設計 §6 の注記)
  if (unknownHeadings.length) {
    const uniq = [...new Set(unknownHeadings.map((u) => u.replace(/^\d+枚目/, '')))];
    warn(19, 'パーサーが知らない見出し',
      `仕様書にはあるがパーサーが構造として拾わない見出しがあります (その他項目に落ちます): ${uniq.join(' / ')}`);
  }
  checks[19] = unknownHeadings.length === 0;

  // パーサー自身の警告も残す (測定のときに「何が取れなかったか」を読むため)
  for (const w of parsed.warnings || []) warn(0, 'パーサーの警告', w);

  return { ok: errors.length === 0, checks, errors, warnings, parsed };
}

/**
 * 商品名の一致 (検査 17)。
 *
 * 🚨 **部分一致では弾けない** (codex exec review P1)。
 *    以前は「片方がもう片方を含んでいれば同じ」にしていたが、それだと
 *    draft が `オイル` のとき `指板メンテナンスオイル` が通ってしまう = **別商品が通る**。
 *    検査 17 の目的は「別商品の内容が混ざっていないか」なので、そこが抜けると意味が無い。
 *
 * かわりに **容量・サイズ・入数だけを落として、残りが同じかを見る**。
 * 実運用で揺れるのはそこだけ (構成側が「ハッカ油スプレー」、draft が「ハッカ油スプレー 100ml」)。
 */
export function sameProduct(a, b) {
  const x = productIdentity(a), y = productIdentity(b);
  return !!x && !!y && x === y;
}

/** 容量・サイズ・入数を落とした「商品の本体」。比較用 */
export function productIdentity(name) {
  let t = String(name == null ? '' : name)
    // 全角英数を半角に
    .replace(/[Ａ-Ｚａ-ｚ０-９]/g, (c) => String.fromCharCode(c.charCodeAt(0) - 0xFEE0))
    .toLowerCase()
    // 括弧は区切りとして扱う (「ハッカ油スプレー（100ml）」)
    .replace(/[（）()「」『』【】[\]]/g, ' ');
  // 🚨 末尾の容量・サイズ・入数だけを落とす。**単位が付いているものだけ**
  //    (単位なしの数字まで落とすと「WD-40」と「WD-50」が同じになる)。
  //
  //    区切りが無くても落とす — 「ハッカ油スプレー100ml」も「Oil100ml」も
  //    EC でありふれた書き方で、落とさないと容量を省いた構成と一致しなくなる。
  //    ただし**ASCII 英数字の直後に来る「1 文字の ASCII 単位」だけは落とさない** —
  //    「RX100M」と「RX200M」がどちらも「rx」になると別商品が通ってしまうから (codex exec review P2 x2)。
  //    ↑ これでも「RX100MM」のような型番は潰れる。そこまでは見分けない (段階1 は人が必ず読む)。
  //
  //    SAFE   = 2 文字以上 / 非 ASCII の単位。型番の末尾と見間違えにくい
  //    AMBIG  = 1 文字の ASCII 単位。区切りか日本語の直後にあるときだけ単位とみなす
  const UNITS_SAFE = 'ml|cc|kg|mg|oz|mm|cm|インチ|inch|個入|本入|枚入|個|本|枚|袋|包|錠|粒|セット|set|pcs|pack|パック|入';
  const UNITS_AMBIG = 'l|g|m|p';
  const NUM = `(?:[x×]\\s*)?\\d+(?:[.,]\\d+)?\\s*`;
  const TAIL = new RegExp(
    '(?:'
    + `(?:(?<=[^\\x00-\\x7F])|[\\s_/・,、]+)${NUM}(?:${UNITS_SAFE}|${UNITS_AMBIG})`
    + '|'
    + `${NUM}(?:${UNITS_SAFE})`
    + `)\\s*$`, 'i');
  let prev;
  do { prev = t; t = t.replace(TAIL, ''); } while (t !== prev);
  return t.replace(/[\s　]+/g, '');
}

/** lint の結果を DB に入れる形にする (lint_json)。errors / warnings は件数と中身を残す */
export function lintSummary(r) {
  return {
    ok: r.ok,
    checks: r.checks,
    errors: r.errors.map((e) => ({ id: e.id, label: e.label, detail: String(e.detail).slice(0, 300) })),
    warnings: r.warnings.map((w) => ({ id: w.id, label: w.label, detail: String(w.detail).slice(0, 300) })).slice(0, 30),
  };
}
