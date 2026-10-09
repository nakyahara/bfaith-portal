/**
 * AI の撮影判定 (画像制作の新フロー PR-C・2026-10-09)。
 *
 * 正本 = AI_reference『商品ハブ_画像制作の新フロー_設計_20261008.md』§3.2・§3.3 とスタッフ要望 ②
 * (「仮LP構成と撮影判定を作る」1 ボタン → 撮影判定 3 択に「AIのおすすめ」と理由)。
 *
 * LP 構成を書く実行役 (miniPC の Claude) に、構成と一緒に「撮影が要るか」も出させる。
 * ここは DB を持たない純粋な部分だけ (指示文・形の検査)。保存と読み口は lib/lp-compose.js。
 *
 * 守りたいこと:
 *   ① **⑦形式のテキストには足さない。** 足すと lint (lib/lp-lint.js) と lp-parser
 *      (配信元と 1 バイトも違わない写し) に触る。結果の送信に別の欄 (shoot_json) を足す
 *   ② **構成の指示文 (PRODUCT_ANALYSIS_INSTRUCTION) は変えない。** スタッフの ChatGPT 定型文と共有の正本で、
 *      段階1 の測定 (AI とスタッフのくらべっこ) は「同じ指示文」が前提。撮影判定の指示は別の定数にして packet に入れる
 *   ③ **撮影判定が壊れていても構成は巻き込まない。** 形が違えば撮影判定だけ「AI の判定なし」にする
 *   ④ 形は**厳しく**見る (足りない・余計な・食い違う、はどれも「判定なし」)。
 *      人が押して決める材料なので、半端に読んで別の意味に取るより「無い」と出すほうが安全。
 *      実行役は出す前に `./phlp lint --shoot` で同じ検査を何度でも受けられる (AI 枠を使わない)
 *
 * PR-C2 (2026-10-09): 判定の決まりをスタッフの仕様書「新商品初動判定」に沿わせた。決まりはコードに書き写さず、
 * 仕様書そのもの (ph_lp_specs の kind = initial_judge) を packet で固めて AI に渡す。形は v2 (validateShootJudgementV2)。
 * 仕様書を取り込む前の依頼は今までどおり v1 (SHOOT_JUDGE_INSTRUCTION)
 */
import { parseConstructionDoc } from './lp-parser.js';
import { MAX_IMAGES as LINT_MAX_IMAGES } from './lp-lint.js';

/** 撮影判定の 3 択 (db.js の SHOOT_MODES と同じ値。ここで db.js を読まないのは純粋関数のままにするため) */
export const SHOOT_RECOMMENDATIONS = ['none', 'inhouse', 'photographer'];
/** 画像ごとの撮影指示 (撮影指示書 PR-D の材料)。この順で保存する */
export const SHOOT_CUT_FIELDS = ['cut', 'composition', 'props', 'background', 'tone', 'ng'];
export const SHOOT_REASON_MAX = 400;
export const SHOOT_FIELD_MAX = 300;
/** 送られてきた JSON の上限 (文字)。10 枚 × 6 項目 × 300 字 + 理由でも収まる */
export const SHOOT_RAW_MAX = 30_000;

/**
 * 撮影判定の指示文 (PR-C の簡単な決まり・形 v1)。packet に入れて受付時に固定する (packet_hash で守られる)。
 * 仕様書「新商品初動判定」が取り込まれていれば、そちら (SHOOT_SPEC_INSTRUCTION・形 v2) を使う (PR-C2)。
 * これは取り込む前の依頼のためのもの (取り込む前に壊れない)。
 * 🚨 PRODUCT_ANALYSIS_INSTRUCTION (構成の指示文) とは**別**。構成の書き方には触れない。
 * 考え方は スタッフのラフ (reasonText) と「新商品初動判定」の定型文
 * (既存素材で足りる / 図解・AI で作れる / 追加撮影が要る) に合わせた。
 */
export const SHOOT_JUDGE_INSTRUCTION = [
  '【撮影判定】(LP構成とは別に出してください)',
  '⑦の構成を書き終えたら、その構成の画像を作るのに撮影が要るかを判定し、JSON で出してください。',
  '⑦の本文には何も足さないでください。撮影判定のために構成を変えないでください。',
  '',
  '判定の材料:',
  '- 添付の商品画像と素材画像 (商品の画像フォルダの中のフォルダにあった画像)',
  '- ⑦の各画像の「使用素材」「商品配置」「背景・シーン」',
  '',
  '画像ごとに、次のどれに当たるかを考えてください:',
  '- 今ある素材 (商品画像・素材画像) で作れる → needs_shoot: false',
  '- AI のイメージ画像や図解で代わりにできる (実物の写りが要らない背景・イメージ・説明図など) → needs_shoot: false',
  '- 実物を撮らないと作れない (商品を使っている様子・質感・中身・サイズ感など、商品そのものの見え方が要るのに素材が無い) → needs_shoot: true',
  '',
  '全体のおすすめ (recommended):',
  '- "none" (撮影不要) … needs_shoot が true の画像が 1 つも無い',
  '- "inhouse" (社内撮影) … 撮影が要るが、卓上・スマホ・自然光で撮れる簡単なカット',
  '- "photographer" (カメラマン撮影) … 料理のスタイリング・質感の作り込み・モデル・大がかりなセットが要るカット',
  '',
  'reason: 人が読んで撮影判定を決めるための 1〜3 文 (200 字ほどまで)。どの画像に何の写真が無いか、なぜその撮影方法かを書く。',
  '  例: 「3枚目 の写真 (使用シーン) がありません。卓上の簡単なカットなので社内撮影で足ります。」',
  '',
  'images: ⑦の画像見出し (# N枚目｜…) の N ごとに 1 つずつ、全部の画像について書く:',
  '- no: N の数字 / needs_shoot: true か false',
  '- needs_shoot が true の画像は、撮影指示書の材料として cut (カット名)・composition (構図)・props (小物)・background (背景)・tone (トーン・光)・ng (撮ってはいけないこと) を書く。cut と composition は必ず書く',
  '- needs_shoot が false の画像は cut 以下を空文字 "" にする',
  '',
  '- どの画像も 8 つのキー (no・needs_shoot・cut・composition・props・background・tone・ng) を全部書く。文字の前後に空白や改行を入れない',
  '',
  '形 (この形以外のキーを足さない):',
  '{"recommended":"none|inhouse|photographer","reason":"…","images":[{"no":0,"needs_shoot":false,"cut":"","composition":"","props":"","background":"","tone":"","ng":""}, …]}',
  '- recommended が "none" なら needs_shoot が true の画像を入れない。"inhouse" か "photographer" なら 1 つ以上入れる',
  '- 材料に無い素材を「ある」ことにしない (素材画像に無い写真を前提にしない)',
].join('\n');

const isPlainObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v)
  && Object.getPrototypeOf(v) === Object.prototype;
/** 改行とタブ以外の制御文字 (画面にそのまま出す文字なので、壊れた出力を通さない) */
// eslint-disable-next-line no-control-regex
const CONTROL_RE = /[\u0000-\u0008\u000b-\u001f\u007f]/;

/**
 * 構成 (⑦の全文) の画像番号。読めなければ null。
 * lp-image (buildImagePlan) と同じく lp-tool の本番パーサーで読む (画像の数え方を 1 つにする)
 */
export function compositionImageNos(outputText) {
  try {
    const doc = parseConstructionDoc(String(outputText || ''));
    const nos = (doc?.images || []).map((im) => im?.no);
    return nos.length && nos.every((n) => Number.isInteger(n)) ? nos : null;
  } catch {
    return null;
  }
}

/**
 * 実行役が送ってきた撮影判定を検査して、保存する形にする。
 *
 * @param raw 送られてきた値 (HTTP の JSON をそのまま)
 * @param {{imageNos?: number[]|null}} opts imageNos = 構成の画像番号。渡せば「全部の画像に 1 つずつ」を照らす
 * @returns {{ok:true, value:object}|{ok:false, errors:string[]}}
 *   value = { recommended, reason, images: [{ no, needs_shoot, cut, composition, props, background, tone, ng }] } (no の昇順)
 */
export function validateShootJudgement(raw, { imageNos = null } = {}) {
  const errors = [];
  const err = (m) => { if (errors.length < 20) errors.push(m); };
  let size;
  try { size = JSON.stringify(raw)?.length; } catch { size = undefined; }
  if (size === undefined) return { ok: false, errors: ['撮影判定を JSON にできません'] };
  if (size > SHOOT_RAW_MAX) return { ok: false, errors: [`撮影判定が大きすぎます (${SHOOT_RAW_MAX} 文字まで)`] };
  if (!isPlainObject(raw)) return { ok: false, errors: ['撮影判定は {recommended, reason, images} の形のオブジェクトです'] };

  for (const k of Object.keys(raw)) {
    if (!['recommended', 'reason', 'images'].includes(k)) err(`知らないキーがあります: ${k.slice(0, 40)}`);
  }
  // 🚨 trim や小文字化をしてから受けない (送られた値と保存する値を食い違わせない・lp-compose の exact と同じ作法)
  const rec = raw.recommended;
  if (typeof rec !== 'string' || !SHOOT_RECOMMENDATIONS.includes(rec)) {
    err(`recommended は ${SHOOT_RECOMMENDATIONS.join(' / ')} のどれかです`);
  }
  // 🚨 前後の空白も直さずに受けない (送られた値と保存する値を同じにする・Codex PR-C 名指し1 M)
  const reason = typeof raw.reason === 'string' ? raw.reason : null;
  if (reason === null) err('reason は文字列です');
  else if (!reason.trim()) err('reason が空です (人が読んで決めるための理由を書いてください)');
  else if (reason !== reason.trim()) err('reason の前後に空白・改行があります');
  else if (reason.length > SHOOT_REASON_MAX) err(`reason は ${SHOOT_REASON_MAX} 文字までです (${reason.length} 文字)`);
  else if (CONTROL_RE.test(reason)) err('reason に制御文字があります');

  const images = [];
  if (!Array.isArray(raw.images)) {
    err('images は配列です');
  } else if (raw.images.length === 0) {
    err('images が空です (構成の画像ごとに 1 つずつ書いてください)');
  } else if (raw.images.length > LINT_MAX_IMAGES) {
    err(`images は ${LINT_MAX_IMAGES} 個までです (${raw.images.length} 個)`);
  } else {
    raw.images.forEach((im, i) => {
      const at = `images[${i}]`;
      if (!isPlainObject(im)) { err(`${at} はオブジェクトです`); return; }
      for (const k of Object.keys(im)) {
        if (!['no', 'needs_shoot', ...SHOOT_CUT_FIELDS].includes(k)) err(`${at} に知らないキーがあります: ${k.slice(0, 40)}`);
      }
      // 番号は数の整数だけ ("3" や 3.0 以外の小数を同じ画像に畳まない)
      if (!Number.isInteger(im.no) || im.no < 0 || im.no > 99) { err(`${at}.no は 0〜99 の整数です`); return; }
      if (images.some((x) => x.no === im.no)) { err(`${at}.no = ${im.no} が 2 回あります`); return; }
      if (typeof im.needs_shoot !== 'boolean') { err(`${at}.needs_shoot は true か false です`); return; }
      const row = { no: im.no, needs_shoot: im.needs_shoot };
      // 🚨 6 項目は全部要る (欠けたものを '' で補わない)。前後の空白も直さずに受けない (Codex PR-C 名指し1 M)
      for (const f of SHOOT_CUT_FIELDS) {
        const v = im[f];
        if (typeof v !== 'string') { err(`${at}.${f} は文字列です (撮影が要らない画像は "")`); row[f] = ''; continue; }
        if (v !== v.trim()) err(`${at}.${f} の前後に空白・改行があります`);
        if (v.length > SHOOT_FIELD_MAX) err(`${at}.${f} は ${SHOOT_FIELD_MAX} 文字までです`);
        if (CONTROL_RE.test(v)) err(`${at}.${f} に制御文字があります`);
        row[f] = v;
      }
      // 撮影が要る画像は、撮影指示書に何を撮るかが要る (空のカットを指示書に並べない)
      if (im.needs_shoot && (!row.cut || !row.composition)) err(`${at} (${im.no}枚目) は撮影が要るので cut と composition を書いてください`);
      // 撮影が要らない画像に撮影指示が書いてあるのは食い違い (どちらを信じて指示書に載せるか決められない)
      if (!im.needs_shoot && SHOOT_CUT_FIELDS.some((f) => row[f] !== '')) err(`${at} (${im.no}枚目) は撮影が要らないので cut 以下を "" にしてください`);
      images.push(row);
    });
  }

  // 🚨 おすすめと画像ごとの要否が食い違うものは受けない (どちらを信じて人に見せるか決められない)
  if (errors.length === 0) {
    const shoots = images.filter((x) => x.needs_shoot).length;
    if (rec === 'none' && shoots > 0) err(`recommended が none (撮影不要) なのに、撮影が要る画像が ${shoots} 枚あります`);
    if (rec !== 'none' && shoots === 0) err(`recommended が ${rec} なのに、撮影が要る画像がありません`);
  }
  // 構成の画像と 1 対 1 か (足りない画像は撮影指示書から抜け、余計な番号は存在しない画像を指す)
  if (errors.length === 0 && imageNos) {
    const want = [...new Set(imageNos)].sort((a, b) => a - b);
    const got = images.map((x) => x.no).sort((a, b) => a - b);
    const missing = want.filter((n) => !got.includes(n));
    const extra = got.filter((n) => !want.includes(n));
    if (missing.length) err(`構成の ${missing.map((n) => n + '枚目').join('・')} の撮影判定がありません`);
    if (extra.length) err(`構成に無い画像の撮影判定があります: ${extra.map((n) => n + '枚目').join('・')}`);
  }
  if (errors.length) return { ok: false, errors };
  return {
    ok: true,
    value: { recommended: rec, reason, images: images.slice().sort((a, b) => a.no - b.no) },
  };
}

/**
 * 構成の本文と照らして検査する (保存と `./phlp lint --shoot` が同じ判定を使う)。
 * 構成を読めなければ「照らせない」として受けない。
 *
 * format = どの形で受けるか (PR-C2)。1 = PR-C の簡単な決まりの形 / 2 = 仕様書「新商品初動判定」の形 /
 * 'auto' = 送られた形で見分ける。
 * 依頼ごとの形は packet で決まる (shootFormatOfPacket)。🚨 依頼と違う形は受けない —
 * 仕様書を渡した依頼に簡単な決まりの判定が返ってきたら、仕様書で判定したものではない
 */
export function validateShootForComposition(raw, outputText, { format = 'auto' } = {}) {
  const nos = compositionImageNos(outputText);
  if (!nos) return { ok: false, errors: ['構成の画像見出しを読めないので、撮影判定を照らせません'] };
  const got = shootFormatOfRaw(raw);
  if (format === 1 && got === 2) return { ok: false, errors: ['この依頼の撮影判定は {recommended, reason, images} の形です (cuts などは付けない)'] };
  if (format === 2 && got !== 2) return { ok: false, errors: ['この依頼の撮影判定は仕様書「新商品初動判定」の形です (cuts・open_required・send_targets などが要ります。shoot_instruction を読み直してください)'] };
  return got === 2 ? validateShootJudgementV2(raw, { imageNos: nos }) : validateShootJudgement(raw, { imageNos: nos });
}

// ─── 仕様書「新商品初動判定」で判定した形 (v2・画像制作の新フロー PR-C2・2026-10-09) ─────────────
//
// 決まり (誰が撮るか・何カットか・開封するか …) は**コードに書き写さない**。仕様書そのものを取り込んで AI に渡す
// (段階1 の LP制作システム仕様書と同じ作法・仕様書が正本)。スタッフが仕様書を頻繁に直すので、
// 決まりをコードに持つと仕様書が変わるたびにコードを直すことになる。
// ここで見るのは**形**だけ: キー・型・列挙値・撮影判定とカットの食い違い・構成の画像と 1 対 1。
// 仕様書の運用ルール (カメラマンは 5 カット単位 など) は強制せず、警告 (shootWarnings) として出すだけ。
//
// 🚨 項目名は撮影指示書 (PR-D・lib/shoot-sheet.js) の CUT_FIELDS / SUMMARY_FIELDS とそろえる
//    (撮影指示書がカット・概要をそのまま受け取れるように)。違うのは 2 つだけ:
//    - 撮影判定は recommended (none|inhouse|photographer・PR-C から)。撮影指示書の judgement (「② 社内撮影」) と撮影担当 shooter は
//      ここから決まるので AI には書かせない (仕様書: 撮影担当は商品単位で 1 つに統一)
//    - 使う LP 画像は番号の配列 lp_image_nos (AI は ⑦ の番号で書く)。撮影指示書の lp_image (「2枚目｜…」の表示) は
//      いまの構成の並びから撮影指示書の側で作る (人が並べ替えても元の画像で引ける)

/** 撮影判定 → 撮影担当 (仕様書: 撮影担当は商品単位で 1 つに統一 = 撮影判定から決まる。AI には書かせない) */
export const SHOOTER_LABELS = { none: 'なし', inhouse: '社内撮影', photographer: 'カメラマン撮影' };
/** 撮影判定 → 仕様書の表記 (撮影指示書の SHOOT_JUDGEMENT_LABELS と同じ。① は撮影指示書を作らないので向こうには無い) */
export const SHOOT_JUDGEMENT_TEXT = { none: '① 追加撮影不要', inhouse: '② 社内撮影', photographer: '③ カメラマン撮影' };
/** 開封要否 (商品全体) */
export const SHOOT_OPEN_VALUES = ['不要', '必要', '一部必要'];
/** 開封要否 (カットごと。仕様書「各撮影カットにも『開封要否：不要／必要』」) */
export const SHOOT_CUT_OPEN_VALUES = ['不要', '必要'];
export const SHOOT_PRIORITY_VALUES = ['必須', '推奨'];
/** 撮影表現タイプ */
export const SHOOT_EXPRESSION_VALUES = ['物撮り', '使用イメージ', '物撮り＋使用イメージ'];
/** 撮影対象バリエーション */
export const SHOOT_VARIATION_VALUES = ['代表1色', '指定複数色', '全色個別', '全色同時'];
/**
 * 概要の文字項目 (キー → 仕様書の項目名)。撮影判定 (recommended)・開封要否 (open_required)・判定の結論 (conclusion) は別に見る。
 * 撮影なし (none) のときは "" (仕様書: ①は連携データを出さない)、撮影ありのときは空にしない
 */
export const SHOOT_V2_SUMMARY_FIELDS = [
  ['send_targets', '撮影用送付対象'], ['purpose', '撮影目的'], ['finish', '完成イメージ'], ['usage', '使用用途'],
];
/** 撮影判定 (v2) の上のキー。この順で保存する */
export const SHOOT_V2_KEYS = ['recommended', 'conclusion', 'open_required', ...SHOOT_V2_SUMMARY_FIELDS.map(([k]) => k), 'cuts', 'images'];
/**
 * カット 1 つ (仕様書の【撮影カット_START】〜【撮影カット_END】の 1 ブロック) の項目。キー → 仕様書の項目名。
 * この順で保存する (仕様書の並び + 商品ハブで足した lp_image_nos・notice・required_notice。名前は撮影指示書の CUT_FIELDS と同じ)
 */
export const SHOOT_V2_CUT_FIELDS = [
  ['no', 'No'], ['priority', '優先度'], ['expression_type', '撮影表現タイプ'], ['variation', '撮影対象バリエーション'],
  ['target', '撮影対象'], ['content', '撮影内容'], ['purpose', '撮影目的'], ['finish', '構図・完成イメージ'],
  ['usage', '使用用途'], ['open_required', '開封要否'], ['reference_theme', '参考イメージ'],
  ['lp_image_nos', '使う LP 画像の番号'], ['notice', '注意'], ['required_notice', '必ず出す表示'],
];
const V2_CUT_ENUMS = {
  priority: SHOOT_PRIORITY_VALUES, expression_type: SHOOT_EXPRESSION_VALUES, variation: SHOOT_VARIATION_VALUES, open_required: SHOOT_CUT_OPEN_VALUES,
};
/** 空にしてよいカットの文字項目 (参考画像が無ければ空欄でよい = 仕様書「参考イメージ記載ルール」/ 注意・必ず出す表示は無ければ空) */
const V2_CUT_OPTIONAL = new Set(['reference_theme', 'notice', 'required_notice']);
/** v2 の判定の結論は「なぜこの担当・撮影内容なのか」まで書くので v1 より長く取る */
export const SHOOT_V2_CONCLUSION_MAX = 600;
/** カットの上限。撮影指示書 (PR-D) の MAX_CUTS と同じ (全色個別を実カット数に展開しても収まる) */
export const SHOOT_V2_MAX_CUTS = 40;
/** v2 の JSON の上限 (文字)。40 カット × 14 項目でも、ふつうの長さなら収まる */
export const SHOOT_V2_RAW_MAX = 60_000;

/** 送られてきた撮影判定がどちらの形か。cuts か open_required を持っていれば 2 (仕様書の形)。それ以外は 1 */
export function shootFormatOfRaw(raw) {
  return isPlainObject(raw) && ('cuts' in raw || 'open_required' in raw) ? 2 : 1;
}
/** 依頼 (packet) が求める形。仕様書「新商品初動判定」を固めた依頼は 2、それ以外 (取り込む前・PR-C の依頼) は 1 */
export function shootFormatOfPacket(packet) {
  return packet && packet.shoot_spec ? 2 : 1;
}

/** 文字の項目を 1 つ見る (前後の空白・長さ・制御文字)。問題が無ければ null */
function textProblem(v, { max, required, at }) {
  if (typeof v !== 'string') return `${at} は文字列です`;
  if (v !== v.trim()) return `${at} の前後に空白・改行があります`;
  if (required && !v) return `${at} が空です`;
  if (v.length > max) return `${at} は ${max} 文字までです (${v.length} 文字)`;
  if (CONTROL_RE.test(v)) return `${at} に制御文字があります`;
  return null;
}

/**
 * 仕様書「新商品初動判定」で判定した撮影判定 (v2) を検査して、保存する形にする。
 * v1 (validateShootJudgement) と同じく**厳しく**見る (欠け・余計・食い違いはどれも「判定なし」。直して受けない)
 * @param {{imageNos?: number[]|null}} opts imageNos = 構成の画像番号。渡せば images が「全部の画像に 1 つずつ」かを照らす
 * @returns {{ok:true, value:object}|{ok:false, errors:string[]}}
 */
export function validateShootJudgementV2(raw, { imageNos = null } = {}) {
  const errors = [];
  const err = (m) => { if (errors.length < 20) errors.push(m); };
  let size;
  try { size = JSON.stringify(raw)?.length; } catch { size = undefined; }
  if (size === undefined) return { ok: false, errors: ['撮影判定を JSON にできません'] };
  if (size > SHOOT_V2_RAW_MAX) return { ok: false, errors: [`撮影判定が大きすぎます (${SHOOT_V2_RAW_MAX} 文字まで)`] };
  if (!isPlainObject(raw)) return { ok: false, errors: [`撮影判定は {${SHOOT_V2_KEYS.join(', ')}} の形のオブジェクトです`] };
  for (const k of Object.keys(raw)) if (!SHOOT_V2_KEYS.includes(k)) err(`知らないキーがあります: ${k.slice(0, 40)}`);
  for (const k of SHOOT_V2_KEYS) if (!(k in raw)) err(`${k} がありません (どの項目も省略しない)`);

  const rec = raw.recommended;
  if (typeof rec !== 'string' || !SHOOT_RECOMMENDATIONS.includes(rec)) err(`recommended は ${SHOOT_RECOMMENDATIONS.join(' / ')} のどれかです`);
  const shooting = rec === 'inhouse' || rec === 'photographer';
  const p = textProblem(raw.conclusion, { max: SHOOT_V2_CONCLUSION_MAX, required: true, at: 'conclusion (判定の結論)' });
  if (p) err(p);
  if (typeof raw.open_required !== 'string' || !SHOOT_OPEN_VALUES.includes(raw.open_required)) err(`open_required (開封要否) は ${SHOOT_OPEN_VALUES.join(' / ')} のどれかです`);
  for (const [k, label] of SHOOT_V2_SUMMARY_FIELDS) {
    const q = textProblem(raw[k], { max: SHOOT_FIELD_MAX, required: false, at: `${k} (${label})` });
    if (q) { err(q); continue; }
    // 🚨 撮影判定と概要の食い違い: 撮影なしなのに送付対象がある / 撮影ありなのに送付対象が無い は、どちらを信じて指示書を作るか決められない
    if (rec === 'none' && raw[k] !== '') err(`recommended が none (追加撮影不要) なので ${k} (${label}) は "" です`);
    if (shooting && raw[k] === '') err(`撮影するので ${k} (${label}) を書いてください`);
  }

  const cuts = [];
  if (!Array.isArray(raw.cuts)) err('cuts は配列です');
  else if (raw.cuts.length > SHOOT_V2_MAX_CUTS) err(`cuts は ${SHOOT_V2_MAX_CUTS} 個までです (${raw.cuts.length} 個)`);
  else {
    const keys = SHOOT_V2_CUT_FIELDS.map(([k]) => k);
    raw.cuts.forEach((c, i) => {
      const at = `cuts[${i}]`;
      if (!isPlainObject(c)) { err(`${at} はオブジェクトです`); return; }
      for (const k of Object.keys(c)) if (!keys.includes(k)) err(`${at} に知らないキーがあります: ${k.slice(0, 40)}`);
      // 番号は 1 からの連番 (カット 1 つ = 別画像として撮る 1 枚。仕様書のカット数の数え方は AI が仕様書に従う)
      if (c.no !== i + 1) err(`${at}.no は ${i + 1} です (1 からの連番)`);
      const row = { no: c.no };
      for (const [k, label] of SHOOT_V2_CUT_FIELDS) {
        if (k === 'no') continue;
        const v = c[k];
        // 🚨 欠けを "" / [] で補わない (v1 と同じ。送られた値と保存する値を同じにする)
        if (!(k in c)) { err(`${at}.${k} (${label}) がありません (無い値は ${k === 'lp_image_nos' ? '[]・LP に無いカットは []' : '""'})`); continue; }
        if (k === 'lp_image_nos') {
          const okArr = Array.isArray(v) && v.length <= LINT_MAX_IMAGES && v.every((n) => Number.isInteger(n) && n >= 0 && n <= 99)
            && new Set(v).size === v.length;
          if (!okArr) { err(`${at}.lp_image_nos (${label}) は ⑦ の画像の番号 (整数・重複なし) の配列です (LP に無いカットは [])`); continue; }
          row[k] = v.slice();
          continue;
        }
        if (V2_CUT_ENUMS[k]) {
          if (typeof v !== 'string' || !V2_CUT_ENUMS[k].includes(v)) err(`${at}.${k} (${label}) は ${V2_CUT_ENUMS[k].join(' / ')} のどれかです`);
          row[k] = typeof v === 'string' ? v : '';
          continue;
        }
        const q = textProblem(v, { max: SHOOT_FIELD_MAX, required: !V2_CUT_OPTIONAL.has(k), at: `${at}.${k} (${label})` });
        if (q) err(q);
        row[k] = typeof v === 'string' ? v : '';
      }
      cuts.push(row);
    });
  }

  const images = [];
  if (!Array.isArray(raw.images)) err('images は配列です');
  else if (raw.images.length === 0) err('images が空です (構成の画像ごとに 1 つずつ書いてください)');
  else if (raw.images.length > LINT_MAX_IMAGES) err(`images は ${LINT_MAX_IMAGES} 個までです (${raw.images.length} 個)`);
  else {
    raw.images.forEach((im, i) => {
      const at = `images[${i}]`;
      if (!isPlainObject(im)) { err(`${at} はオブジェクトです`); return; }
      for (const k of Object.keys(im)) if (!['no', 'needs_shoot'].includes(k)) err(`${at} に知らないキーがあります: ${k.slice(0, 40)} (撮影の中身は cuts に書く)`);
      if (!Number.isInteger(im.no) || im.no < 0 || im.no > 99) { err(`${at}.no は 0〜99 の整数です`); return; }
      if (images.some((x) => x.no === im.no)) { err(`${at}.no = ${im.no} が 2 回あります`); return; }
      if (typeof im.needs_shoot !== 'boolean') { err(`${at}.needs_shoot は true か false です`); return; }
      images.push({ no: im.no, needs_shoot: im.needs_shoot });
    });
  }

  // 🚨 撮影判定とカットの食い違いは受けない (仕様書: ①は撮影カットなし / ②③は撮影依頼書を作る = カットが要る)
  if (errors.length === 0) {
    if (rec === 'none' && cuts.length > 0) err(`recommended が none (追加撮影不要) なのに、撮影カットが ${cuts.length} 個あります`);
    if (shooting && cuts.length === 0) err(`recommended が ${rec} なのに、撮影カットがありません`);
    const shoots = images.filter((x) => x.needs_shoot).length;
    if (rec === 'none' && shoots > 0) err(`recommended が none (追加撮影不要) なのに、撮影が要る画像が ${shoots} 枚あります`);
    // 画像とカットは両向きで合わせる (Codex PR-C2 名指し1 M):
    //   カットを使う画像は「撮影が要る」/ 撮影が要る画像には、それを使うカットが 1 つ以上ある
    //   (片方だけだと、撮影指示書にカットが載らない要撮影の画像や、要らない画像のためのカットができる)
    for (const c of cuts) {
      for (const n of c.lp_image_nos) {
        const im = images.find((x) => x.no === n);
        if (!im) err(`カット ${c.no} の lp_image_nos の ${n} は images にありません`);
        else if (!im.needs_shoot) err(`カット ${c.no} は ${n}枚目 に使うのに、${n}枚目 の needs_shoot が false です`);
      }
    }
    for (const im of images) {
      if (im.needs_shoot && !cuts.some((c) => c.lp_image_nos.includes(im.no))) err(`${im.no}枚目 は needs_shoot が true なのに、${im.no}枚目 に使うカットがありません (lp_image_nos に ${im.no} を入れる)`);
    }
  }
  if (errors.length === 0 && imageNos) {
    const want = [...new Set(imageNos)].sort((a, b) => a - b);
    const got = images.map((x) => x.no).sort((a, b) => a - b);
    const missing = want.filter((n) => !got.includes(n));
    const extra = got.filter((n) => !want.includes(n));
    if (missing.length) err(`構成の ${missing.map((n) => n + '枚目').join('・')} の撮影判定がありません`);
    if (extra.length) err(`構成に無い画像の撮影判定があります: ${extra.map((n) => n + '枚目').join('・')}`);
  }
  if (errors.length) return { ok: false, errors };
  const value = {};
  for (const k of SHOOT_V2_KEYS) {
    if (k === 'cuts') value.cuts = cuts;
    else if (k === 'images') value.images = images.slice().sort((a, b) => a.no - b.no);
    else value[k] = raw[k];
  }
  return { ok: true, value };
}

/**
 * 仕様書の運用ルールのうち、**強制はせず知らせるだけ**のもの (画面と `./phlp lint --shoot` に出す)。
 * 🚨 ここに決まりを増やさない — 仕様書が変わるたびにコードを直すことになる。判定は AI が仕様書に従ってする。
 * いまは「カメラマン撮影は 5 カット単位」(仕様書の最終チェック・担当者向け手順 5) だけ。v1 (PR-C の形) には出さない
 * (撮影指示書のシートにも同じ知らせが出る = lib/shoot-sheet.js の cutCountWarning)
 */
export function shootWarnings(value) {
  const out = [];
  if (!value || !Array.isArray(value.cuts) || value.format === 1) return out;
  const n = value.cuts.length;
  if (value.recommended === 'photographer' && n % 5 !== 0) {
    out.push(`カメラマン撮影は 5 カット単位です (仕様書「新商品初動判定」) が、${n} カットです`);
  }
  return out;
}

const blankV1Cut = () => ({ cut: '', composition: '', props: '', background: '', tone: '', ng: '' });
const blankV2Cut = () => Object.fromEntries(SHOOT_V2_CUT_FIELDS.map(([k]) => [k, k === 'lp_image_nos' ? [] : '']));

/**
 * 検査を通った撮影判定 (v1 / v2 どちらでも) を、読み口 (latestShootJudgement) の形にそろえる。
 *   - format (1|2)・recommended・reason (判定の結論 = 画面が出す文)
 *   - summary: 撮影指示書の SUMMARY_FIELDS の形 {judgement, shooter, open_required, send_targets, purpose, finish, usage, conclusion}
 *   - cuts: 撮影指示書の CUT_FIELDS の形 (+ no・lp_image_nos)。cut_count = その数
 *   - images: 画像ごと {no, needs_shoot, cut, composition, props, background, tone, ng} (PR-C の形のまま。v1 の撮影指示書の材料)。
 *     v2 では cut・composition に、その画像を使う最初のカットの 撮影内容・構図・完成イメージ を入れ、ほかは ""
 *   - warnings: shootWarnings
 * v1 (PR-C の形で保存済みの行) は v2 の形に寄せる: 撮影が要る画像を 1 カットずつ (撮影内容 = cut・完成イメージ = composition・
 * 注意 = ng) にし、v1 に無い項目は ""
 */
export function shootReadModel(value) {
  if (!value) return null;
  const v2 = Array.isArray(value.cuts);
  let cuts;
  let images;
  if (v2) {
    cuts = value.cuts.map((c) => ({ ...c, lp_image_nos: c.lp_image_nos.slice() }));
    images = value.images.map((im) => {
      const c = im.needs_shoot ? cuts.find((x) => x.lp_image_nos.includes(im.no)) : null;
      return { no: im.no, needs_shoot: im.needs_shoot, ...blankV1Cut(), ...(c ? { cut: c.content, composition: c.finish } : {}) };
    });
  } else {
    images = value.images.map((im) => ({ ...im }));
    cuts = images.filter((im) => im.needs_shoot).map((im, i) => ({
      ...blankV2Cut(), no: i + 1, content: im.cut, finish: im.composition, notice: im.ng, lp_image_nos: [im.no],
    }));
  }
  const summary = {
    judgement: SHOOT_JUDGEMENT_TEXT[value.recommended] || '',
    shooter: SHOOTER_LABELS[value.recommended] || '',
    open_required: v2 ? value.open_required : '',
    ...Object.fromEntries(SHOOT_V2_SUMMARY_FIELDS.map(([k]) => [k, v2 ? value[k] : ''])),
    conclusion: v2 ? value.conclusion : value.reason,
  };
  const model = {
    format: v2 ? 2 : 1,
    recommended: value.recommended,
    reason: summary.conclusion,
    summary,
    cuts,
    cut_count: cuts.length,
    images,
  };
  model.warnings = shootWarnings(model);
  return model;
}

/**
 * 撮影判定の指示文 (仕様書「新商品初動判定」を取り込んだ依頼の分)。packet に入れて受付時に固定する。
 * 🚨 判定の決まりはここに書かない (仕様書が正本)。ここに書くのは「どの材料を使うか」と「JSON の形」だけ。
 *    仕様書は claim で shoot-spec-<ID>.md に落ちる (phlp)
 */
const quoted = (xs) => xs.map((x) => `"${x}"`).join(' / ');
export const SHOOT_SPEC_INSTRUCTION = [
  '【撮影判定】(LP構成とは別に出してください・仕様書「新商品初動判定」で判定します)',
  '⑦の構成を書き終えて検品と直しが終わったら、仕様書「新商品初動判定」の判定ロジックに従って、この商品の撮影判定と撮影依頼書連携データを決め、JSON で出してください。',
  '⑦の本文には何も足さないでください。撮影判定のために構成を変えないでください。',
  '',
  '仕様書: claim の shoot_spec.file (shoot-spec-<ID>.md)。全タブがテキストになっています。大きいので Read を分けて (offset を進めて) 最後まで読んでください。',
  '- 判定のやり方 (判定順序・撮影区分・開封判定・使用イメージ・バリエーション・撮影担当・カット数の数え方・撮影用送付対象・LP に無いが必要な実写 など) は仕様書の「システム本文」に従ってください。この指示と仕様書の判定のやり方が食い違うときは仕様書が正本です',
  '- ただし出すのは下の JSON だけです。仕様書の「出力形式」の 1〜9 の分析文と【撮影依頼書連携データ_START】のブロックは出さないでください (同じ中身を JSON の項目に入れます)',
  '',
  '仕様書の「入力テンプレート」の材料との対応:',
  '- 商品情報 = claim の packet.product_info と packet.color_variations',
  '- 商品画像・既存素材 = 添付の商品画像と素材画像 (自分で見たもの。素材画像は商品の画像フォルダの中のフォルダにあった画像)',
  '- LP制作管理シート (LP構成・AI生成用プロンプト) = 書き終えた ⑦ (out-<ID>.md)',
  '- Amazon商品URL・商品画像フォルダURL は渡していません (見ていないものを見たことにしない)',
  '',
  'JSON の項目 (仕様書の「撮影依頼書連携データ」との対応。この形以外のキーを足さない・どの項目も省略しない):',
  '- recommended: 撮影判定。"none" = ①追加撮影不要 / "inhouse" = ②社内撮影 / "photographer" = ③カメラマン撮影 (撮影担当はここから決まるので書かない)',
  `- conclusion: 判定の結論 (なぜこの担当・撮影内容なのか。人が読んで撮影判定を決める。${SHOOT_V2_CONCLUSION_MAX} 字まで)`,
  `- open_required: 開封要否 (${quoted(SHOOT_OPEN_VALUES)})`,
  '- send_targets: 撮影用送付対象 / purpose: 撮影目的 / finish: 完成イメージ / usage: 使用用途',
  '- cuts: 撮影カット一覧。1 カット = 1 つ (【撮影カット_START】〜【撮影カット_END】の 1 ブロックに当たる):',
  `  - no: 1 からの連番 / priority: 優先度 (${quoted(SHOOT_PRIORITY_VALUES)}) / expression_type: 撮影表現タイプ (${quoted(SHOOT_EXPRESSION_VALUES)})`,
  `  - variation: 撮影対象バリエーション (${quoted(SHOOT_VARIATION_VALUES)})`,
  '  - target: 撮影対象 / content: 撮影内容 (短い見出し) / purpose: 撮影目的 / finish: 構図・完成イメージ / usage: 使用用途',
  `  - open_required: 開封要否 (${quoted(SHOOT_CUT_OPEN_VALUES)}) / reference_theme: 参考イメージ (AI 参考画像を作るための短いテーマ名。無ければ "")`,
  '  - lp_image_nos: このカットの写真を使う ⑦ の画像の番号 (# N枚目 の N) の配列。LP に無いが必要なカットは []',
  '  - notice: 撮影者への注意 (無ければ "") / required_notice: 撮影依頼書に必ず出す表示 (仕様書が必ず書けという表示。無ければ "")',
  '- images: ⑦の画像見出し (# N枚目｜…) の N ごとに 1 つずつ、全部の画像について {"no": N, "needs_shoot": true か false}',
  '  (needs_shoot = その画像を作るのに追加撮影の写真が要るか)',
  '',
  'サーバが形を検査します (./phlp lint --shoot で先に確かめられます):',
  '- recommended が "none" なら cuts は [] で、send_targets・purpose・finish・usage は ""、needs_shoot はすべて false',
  '- "inhouse" か "photographer" なら cuts を 1 つ以上書き、send_targets・purpose・finish・usage は空にしない',
  '- 画像とカットは両向きで合わせる: lp_image_nos に入れた画像は needs_shoot を true にし、needs_shoot が true の画像はどれかのカットの lp_image_nos に入れる',
  `- 文字は ${SHOOT_FIELD_MAX} 字まで (conclusion は ${SHOOT_V2_CONCLUSION_MAX} 字まで)。文字の前後に空白や改行を入れない。無い値は "" (lp_image_nos だけは [])`,
  '- 材料に無い素材を「ある」ことにしない。色名・種類名が分からなければ「要確認」と書く',
  '- lint の warnings は仕様書の運用ルールの知らせです (通らないわけではありません)。仕様書を読み直して、直すべきなら直してください',
  '',
  '形:',
  '{"recommended":"inhouse","conclusion":"…","open_required":"不要","send_targets":"…","purpose":"…","finish":"…","usage":"…",'
    + '"cuts":[{"no":1,"priority":"必須","expression_type":"物撮り","variation":"代表1色","target":"…","content":"…","purpose":"…","finish":"…","usage":"…",'
    + '"open_required":"不要","reference_theme":"…","lp_image_nos":[2],"notice":"","required_notice":""}],'
    + '"images":[{"no":0,"needs_shoot":false}, …]}',
].join('\n');
