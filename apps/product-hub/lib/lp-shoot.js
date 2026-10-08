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
 * 撮影判定の指示文。packet に入れて受付時に固定する (packet_hash で守られる)。
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
  const reason = typeof raw.reason === 'string' ? raw.reason.trim() : null;
  if (reason === null) err('reason は文字列です');
  else if (!reason) err('reason が空です (人が読んで決めるための理由を書いてください)');
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
      for (const f of SHOOT_CUT_FIELDS) {
        const v = im[f];
        if (v === undefined) { row[f] = ''; continue; }
        if (typeof v !== 'string') { err(`${at}.${f} は文字列です`); row[f] = ''; continue; }
        const t = v.trim();
        if (t.length > SHOOT_FIELD_MAX) err(`${at}.${f} は ${SHOOT_FIELD_MAX} 文字までです`);
        if (CONTROL_RE.test(t)) err(`${at}.${f} に制御文字があります`);
        row[f] = t;
      }
      // 撮影が要る画像は、撮影指示書に何を撮るかが要る (空のカットを指示書に並べない)
      if (im.needs_shoot && (!row.cut || !row.composition)) err(`${at} (${im.no}枚目) は撮影が要るので cut と composition を書いてください`);
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
 * 構成を読めなければ「照らせない」として受けない
 */
export function validateShootForComposition(raw, outputText) {
  const nos = compositionImageNos(outputText);
  if (!nos) return { ok: false, errors: ['構成の画像見出しを読めないので、撮影判定を照らせません'] };
  return validateShootJudgement(raw, { imageNos: nos });
}
