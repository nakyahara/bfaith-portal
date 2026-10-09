/**
 * LP 画像の生成 — 段階2 (2026-10-04 中原さん「構成だけだと分からない。実際に画像を作ってみないと」)
 *
 * 正本 = AI_reference『商品ハブ_LP構成と画像の自動生成_検討_20260929.md』§2・§5・§6。
 * 構成ができた (done・実モデル一致) 依頼の ⑦ を画像ごとに gpt-image-2.5 で作り、
 * 商品の画像フォルダの中の「AI初稿」に保存する。**人がボタンを押したときだけ** (中原さん 2026-10-04「全部おすすめ」)。
 *
 * 守りたいこと:
 *   ① **従量課金の上限を fail-closed で守る** (検討 §6 層2)。呼ぶ前に見込み額を台帳 (ph_ai_usage) に取り置き、
 *      当月の合計 (推定の実額 + 取り置き + 結果不明の見込み) が「上限 − 安全幅」を超えるなら呼ばない。上限は Render に書く (書かれていなければ作らない)
 *   ② **品質段はサーバが固定**。xhigh / max は使わせない (medium の 16 倍・検討 §5)
 *   ③ 機能フラグ (PH_LP_IMAGE_ENABLED=1) と専用キー (OPENAI_LP_IMAGE_API_KEY) がそろわなければ動かない。
 *      キーは問い合わせハブの OPENAI_API_KEY と**名前を分ける** (検討 §6 層1)。同じキーを入れれば共用で始められ、
 *      後から専用キーに替えるのは Render の設定 1 つで済む (中原さん「共用で」2026-10-04)
 *   ④ prompt と参考画像は**受付時に固定** (作り直すときは新しい job)。参考画像は Drive の更新日時も固定し、
 *      作る前に照らす (差し替わっていたら作らない)。1 依頼 (draft) に動いている job は 1 つ
 *   ⑤ 取り置きは安全側の見込み額 (品質段・参考画像の枚数・prompt のバイト数から・#1612 R1〜R3)。保証された上限ではないので、
 *      月の上限の手前に安全幅を残す。
 *      請求されないと言い切れる失敗 (401 / 403 / 404 / 429) だけ 0 円。内容で断られた (moderation) を含むほかの失敗・
 *      通信断・5xx・再起動・usage の無い成功は**取り置き額のまま** (少なく数えない)
 *   ⑥ 🚨 **Render の 1 台 (1 インスタンス) で動かす前提**。台帳も claim も product-hub の SQLite (1 台のディスク) が正本。
 *      product-hub 全体がもともとこの前提 (DB を別のインスタンスと共有できない)。台数を増やすなら台帳を中央の DB に移すこと
 *
 * DB とロジックはここ。Drive / OpenAI / 画面は router.js から差し込む (試験で差し替えられるように)。
 */
import { parseConstructionDoc } from './lp-parser.js';
import { logEvent, SHOOT_MODE_LABELS, MATERIAL_STATUS_LABELS } from '../db.js';
// 構成の本文は「効いている構成」(人が直した編集版があればそれ・画像制作の新フロー PR-B) を読む
import { composeTextOf, latestDoneComposeJob, effectiveCompose, imageStaleFor } from './lp-edit.js';

export const LP_IMAGE_MODELS = ['gpt-image-2.5-flare', 'gpt-image-2.5-sunburst'];
// xhigh / max は使わない (検討 §5: max は medium の 16 倍・作り込むほどデザイン修正が増える)
export const LP_IMAGE_QUALITIES = ['low', 'medium', 'high'];
export const DEFAULT_LP_IMAGE_MODEL = 'gpt-image-2.5-flare';   // ChatGPT の既定と同じ系統・速い
export const DEFAULT_LP_IMAGE_QUALITY = 'medium';
export const LP_IMAGE_SIZE = '1200x1200';                      // 仕様書の「1200×1200px／正方形」(16 の倍数なので API がそのまま受ける)
export const MAX_LP_IMAGES = 8;                                // 検討 §4: 高 = 全枚数 (上限 8)
/**
 * 何枚まで作るか (検討 §4: 高 = 全枚数 (上限 8) / 低・激低 = 1 枚目のみ・#1612 R3 Medium)。
 * 重要度に「（重要度：高）」が付くものだけ全部。低・激低・**まだ決まっていない**ものは 1 枚目だけ (お金を使いすぎない側)
 */
export function imageLimitForPriority(priority) {
  return /（重要度：高）/.test(String(priority || '')) ? MAX_LP_IMAGES : 1;
}
export const MAX_REFS = 4;                                     // API の上限 (1 回に参考画像 4 枚まで)
export const MAX_PRODUCT_REFS = 2;                             // 商品の写真 (白抜き → TOP) は 2 枚まで。残りは使う素材
/** 推奨の月の上限 (中原さん 2026-10-04「全部おすすめ」・見込み約 1,020 円の約 3 倍)。**既定値ではない** — Render に書く値 */
export const RECOMMENDED_MONTHLY_BUDGET_JPY = 3000;
// $/1M tokens (検討 §5.1・2026-09 の公開価格)
const PRICE = { textIn: 5, imageIn: 8, out: 30 };
/**
 * 円換算 (請求はドル)。**取り置きも確定もこの 1 つの値**で数える (#1612 R3 High: 取り置き 170・確定 160 と分けていたら、
 * 成功のたびに為替の余裕が空いて上限を超えられた)。検討 §5 の 150 円より高め = 少なく数えない。cost_jpy もこの換算の推定値
 */
const USD_JPY = 170;
const USD_JPY_RESERVE = USD_JPY;
/**
 * 取り置きの上限トークン (#1612 R1 High: 一律 15 円だと実費が上回れば上限を突破できた)。
 * 出力: 検討 §5.1 のベンチ (1024px で low 196 / medium 439 / high 1,756) を 1200px の面積 (×1.37) で伸ばし、さらに 3 倍以上の余裕。
 * 入力: 参考画像 1 枚 (1024px) は 2 枚で約 3,000 tok の実測の 4 倍で 1 枚 6,000。
 * 文は **UTF-8 のバイト数**で見る (トークン数がバイト数を超えることはない・#1612 R2 High)。
 * それでも保証された上限ではないので、月の上限の手前に安全幅を取る (lpImageConfig の spendable)
 */
const OUT_TOKENS_CEIL = { low: 2_000, medium: 4_000, high: 12_000 };
const IMAGE_IN_TOKENS_PER_REF_CEIL = 6_000;
/** 月の上限の手前に取る安全幅 (5%・最低 100 円)。取り置きを超える請求がまれにあっても、上限は超えない */
const BUDGET_SAFETY_RATE = 0.05;
const BUDGET_SAFETY_MIN_JPY = 100;
/** 1 枚の取り置き額 (**整数の円・切り上げ**)。品質段が知らない値なら null (= 作らない) */
export function reserveJpy({ quality, refs = 0, promptBytes = 0 }) {
  const out = OUT_TOKENS_CEIL[quality];
  if (!out) return null;
  const usd = (Math.max(0, promptBytes) * PRICE.textIn
    + Math.max(0, refs) * IMAGE_IN_TOKENS_PER_REF_CEIL * PRICE.imageIn + out * PRICE.out) / 1_000_000;
  return Math.ceil(usd * USD_JPY_RESERVE);
}
const PROMPT_MAX = 30_000;
/** 作っている画像の期限 (分)。段階ごとに延ばす。切れたものだけ片付ける */
const LEASE_MIN = 15;
const IDEMPOTENCY_KEY_RE = /^[A-Za-z0-9_.:-]{8,80}$/;
const DRIVE_ID_RE = /^[-\w]{10,200}$/;

const trim = (v, max) => {
  const s = v == null ? '' : String(v).trim();
  return max && s.length > max ? s.slice(0, max) : s;
};
const exact = (v, re) => {
  if (typeof v !== 'string') return null;
  const m = re.exec(v);
  return m && m[0] === v ? v : null;
};
const posInt = (v) => {
  const n = typeof v === 'number' ? v : (exact(String(v ?? ''), /^[1-9]\d*$/) ? Number(v) : NaN);
  return Number.isSafeInteger(n) && n > 0 ? n : null;
};

/** JST の YYYY-MM (月の上限は日本の暦月で区切る) */
export function jstMonth(now = Date.now()) {
  return new Date(now + 9 * 3600_000).toISOString().slice(0, 7);
}

/**
 * 設定。**どれか 1 つでも読めなければ使えない** (黙って既定に戻さない・#1591 R2 と同じ作法)。
 * 未設定 (空) のモデル・品質段・上限は既定値。キーと機能フラグは必須
 */
export function lpImageConfig(env = process.env) {
  const enabled = env.PH_LP_IMAGE_ENABLED === '1';
  const hasKey = !!(env.OPENAI_LP_IMAGE_API_KEY && String(env.OPENAI_LP_IMAGE_API_KEY).trim());
  const pick = (v, list, def) => (v == null || v === '' ? def : (list.includes(v) ? v : null));
  const model = pick(env.PH_LP_IMAGE_MODEL, LP_IMAGE_MODELS, DEFAULT_LP_IMAGE_MODEL);
  const quality = pick(env.PH_LP_IMAGE_QUALITY, LP_IMAGE_QUALITIES, DEFAULT_LP_IMAGE_QUALITY);
  const rawBudget = env.PH_LP_MONTHLY_BUDGET_JPY;
  // 🚨 上限が書かれていなければ作らない (検討 §6 層2・中原さん 2026-10-04「書いていなければ作らない」)
  const budget = exact(String(rawBudget ?? ''), /^[1-9]\d{0,5}$/) ? Number(rawBudget) : null;
  let error = null;
  if (!enabled) error = '画像の生成は止めてあります (Render の PH_LP_IMAGE_ENABLED)';
  else if (!hasKey) error = '画像の生成のキーがありません (Render の OPENAI_LP_IMAGE_API_KEY)';
  else if (!model) error = '画像のモデルの設定が読めません (PH_LP_IMAGE_MODEL)';
  else if (!quality) error = '画像の品質段の設定が読めません (PH_LP_IMAGE_QUALITY・low / medium / high だけ)';
  else if (!budget) error = (rawBudget == null || rawBudget === '')
    ? `月の上限が書かれていません (Render の PH_LP_MONTHLY_BUDGET_JPY・推奨 ${RECOMMENDED_MONTHLY_BUDGET_JPY})`
    : '月の上限の設定が読めません (PH_LP_MONTHLY_BUDGET_JPY・円の整数)';
  // 呼んでよい上限 = 月の上限 − 安全幅。取り置きは保証された上限ではない (OpenAI は使用量を返事で知らせるだけ) ので、
  // ぎりぎりでは呼ばない (#1612 R2 High)
  const safety = budget ? Math.max(BUDGET_SAFETY_MIN_JPY, Math.ceil(budget * BUDGET_SAFETY_RATE)) : null;
  return { enabled, usable: !error, error, model, quality, budget, safety, spendable: budget ? Math.max(0, budget - safety) : null, size: LP_IMAGE_SIZE };
}

/**
 * 返事の usage から円を出す (検討 §5.1 の単価・**整数の円・切り上げ**)。
 * 🚨 内訳がそろって数が合うときだけ使う (#1612 R4 High)。内訳が無い・合わない・負の数・整数でない usage は null
 *    (= 取り置き額のまま)。内訳が無いのを安い単価で数えると、取り置きが不当に戻って上限を超えて呼べた
 */
export function costJpyFromUsage(usage) {
  if (!usage || typeof usage !== 'object') return null;
  const det = usage.input_tokens_details;
  if (!det || typeof det !== 'object') return null;
  const n = (v) => (Number.isSafeInteger(v) && v >= 0 ? v : null);
  const out = n(usage.output_tokens), inAll = n(usage.input_tokens), img = n(det.image_tokens), txt = n(det.text_tokens);
  if (out == null || inAll == null || img == null || txt == null || img + txt !== inAll) return null;
  // 画像ができたのに出力 0 は usage が壊れている。0 円で確定すると取り置きが全部戻ってしまう (Codex PR-E 名指し2 M)
  if (out === 0) return null;
  const usd = (txt * PRICE.textIn + img * PRICE.imageIn + out * PRICE.out) / 1_000_000;
  return Math.ceil(usd * USD_JPY);
}

/** 当月の使った額 = 実額 (charged) + 取り置き (reserved) + 結果不明の見込み (unknown) */
export function monthUsage(db, now = Date.now()) {
  const month = jstMonth(now);
  const r = db.prepare(`SELECT COALESCE(SUM(CASE
      WHEN status = 'charged' THEN COALESCE(cost_jpy, est_jpy)
      WHEN status IN ('reserved','unknown') THEN est_jpy ELSE 0 END), 0) AS jpy
    FROM ph_ai_usage WHERE kind = 'lp_image' AND month = ?`).get(month);
  return { month, used_jpy: Math.ceil(Number(r.jpy)) };
}

const sectionOf = (block, heading) => {
  const m = new RegExp('(?:^|\\n)## ' + heading + '\\n([\\s\\S]*?)(?=\\n## |$)').exec(String(block || ''));
  return m ? m[1].trim() : '';
};

/**
 * 構成 (⑦ の全文) と packet から、作る画像の一覧を組む。**受付時に 1 回だけ** (prompt と参考画像を固定する)。
 * prompt = 共通の決まり (生成条件・カラー・商品再現ルール・NG) + その画像の部分 (個別生成プロンプト = 画像ブロック全文)。
 * 参考画像 = 商品の写真 (白抜き → TOP の順に 2 枚まで) + その画像の「使用素材」に名前の出てくる素材 (合わせて 4 枚まで)
 * @returns {{images: Array<{seq,no,name,prompt,refs}>, error: string|null}}
 */
export function buildImagePlan({ outputText, packet, quality = DEFAULT_LP_IMAGE_QUALITY, refTimes = null, maxImages = MAX_LP_IMAGES }) {
  let doc;
  try { doc = parseConstructionDoc(String(outputText || '')); } catch (e) { return { images: [], error: '構成を読み取れません: ' + String(e?.message || e).slice(0, 200) }; }
  const imgs = (doc?.images || []).filter((im) => trim(im?.individualPrompt || im?.rawBlockText));
  if (!imgs.length) return { images: [], error: '構成に画像のブロックがありません' };
  const c = doc.common || {};
  const commonParts = [
    ['共通生成条件', c.commonConditions], ['共通使用カラー', c.commonColors],
    ['商品再現ルール', c.reproductionRules], ['共通NG事項', c.commonNG],
  ].filter(([, v]) => trim(v)).map(([h, v]) => `# ${h}\n${trim(v)}`).join('\n\n');
  const packetImgs = Array.isArray(packet?.images) ? packet.images : [];
  const products = packetImgs.filter((im) => !/^material:/.test(String(im?.role || ''))).slice(0, MAX_PRODUCT_REFS);
  const materials = packetImgs.filter((im) => /^material:/.test(String(im?.role || '')));
  // modified_time = Drive の更新日時。作る前に照らし、差し替わっていたら作らない (#1612 R1 Medium)
  // refTimes = 受付のときに Drive から取り直した更新日時 (packet の日時より優先。packet の日時は空のことがある・#1612 R2 Medium)
  const ref = (im) => ({ file_id: im.file_id, role: im.role || null, name: im.name || null, folder: im.folder || null,
    modified_time: (refTimes && refTimes[im.file_id]) || im.modified_time || null });
  // 1 枚だけのときは「1枚目」(FV) を作る (無ければ最初のブロック)。サムネイル (0 枚目) は対象にしない
  const limit = Math.max(1, Math.min(MAX_LP_IMAGES, Number(maxImages) || 1));
  const chosen = limit === 1 ? [imgs.find((im) => im.no === 1) || imgs[0]] : imgs.slice(0, limit);
  const out = chosen.map((im, i) => {
    const block = trim(im.individualPrompt || im.rawBlockText);
    // その画像の「使用素材」に名前 (または 場所/名前) が出てくる素材だけ参考に渡す
    const used = sectionOf(block, '使用素材');
    const usedMats = materials.filter((m) => {
      const name = trim(m?.name);
      const full = (m?.folder ? m.folder + '/' : '') + name;
      return name && (used.includes(full) || used.includes(name));
    });
    const refs = [...products.map(ref), ...usedMats.map(ref)].slice(0, MAX_REFS);
    const title = `${im.no === 0 ? '0枚目' : (im.no != null ? im.no + '枚目' : (i + 1) + '番目')}｜${trim(im.name, 60)}`;
    const prompt = [
      'EC の商品ページ (LP) に載せる画像を 1 枚作ってください。下の「共通の決まり」と「この画像の指示」に従います。',
      refs.length ? `参考画像: ${refs.map((r, k) => `${k + 1}枚目 = ${/^material:/.test(String(r.role || '')) ? '素材 (' + (r.folder ? r.folder + '/' : '') + (r.name || '') + ')' : '実物の商品画像'}`).join('・')}。実物の商品画像の形・ラベル・色・文字は描き直さずにそのまま使ってください。` : '',
      '',
      '【共通の決まり】', commonParts || '(なし)',
      '',
      `【この画像の指示: ${title}】`, block,
    ].filter((x) => x !== '').join('\n').slice(0, PROMPT_MAX);
    return { seq: i + 1, no: Number.isInteger(im.no) ? im.no : null, name: trim(im.name, 100) || null, prompt, refs,
      est_jpy: reserveJpy({ quality, refs: refs.length, promptBytes: Buffer.byteLength(prompt, 'utf8') }) };
  });
  return { images: out, error: null };
}

/**
 * いちばん新しい「できた」LP 構成 (done・実モデル一致) と、その実モデル確認。
 * 🚨 LP構成の一覧 (lib/lp-edit.js の「効いている構成」) と同じ構成を見る (画像制作の新フロー PR-B)。
 *    いちばん新しい依頼そのものを見ていたときは、作り直しが失敗・確認待ちのあいだ、人が直した前の構成から
 *    画像を作れなかった (Codex PR-B 名指し H)
 */
function latestComposeJob(db, draftId) {
  return latestDoneComposeJob(db, draftId);
}

/** 依頼のキーの形 (二重クリック対策のキー)。router が Drive を読む前に確かめる */
export const validImageRequestKey = (key) => !!exact(key, IDEMPOTENCY_KEY_RE);

/**
 * 受付の前に Drive の更新日時を取り直すファイル = **実際に参考に渡す画像だけ** (計画の refs の和集合・#1612 R3 Medium)。
 * 使わない画像が消えていても受付を止めない
 */
export function imageRefCandidates(db, draft, env = process.env) {
  const cj = latestComposeJob(db, posInt(draft?.id));
  if (!cj || !trim(composeTextOf(db, cj))) return [];
  const cfg = lpImageConfig(env);
  const plan = buildImagePlan({ outputText: composeTextOf(db, cj), packet: safeJson(cj.packet_json), quality: cfg.quality || DEFAULT_LP_IMAGE_QUALITY, maxImages: imageLimitForPriority(draft?.image_priority) });
  return [...new Set(plan.images.flatMap((im) => im.refs.map((r) => r.file_id)).filter((id) => exact(String(id || ''), DRIVE_ID_RE)))];
}

/** 動いている (queued / running) 画像の job */
const activeImageJob = (db, draftId) => db.prepare(`SELECT * FROM ph_lp_image_jobs
  WHERE draft_id = ? AND status IN ('queued','running') ORDER BY id DESC LIMIT 1`).get(draftId) || null;

/**
 * 押せない理由 (画面のボタンと API で同じ判定)。null なら押せる
 * @param folderId 商品の画像フォルダ (drive_folder_url から取った ID)
 */
export function imageBlockReason(db, { draft, folderId, env = process.env, now = Date.now() } = {}) {
  const cfg = lpImageConfig(env);
  if (!cfg.usable) return cfg.error;
  const draftId = posInt(draft?.id);
  if (!draftId) return '商品の ID が不正です';
  const cj = latestComposeJob(db, draftId);
  if (!cj || cj.status !== 'done' || cj.model_check !== 'match' || !trim(composeTextOf(db, cj))) {
    return '先に「🤖 構成をAIに作らせる」で構成を作ってください (できた構成から画像を作ります)';
  }
  if (!exact(folderId, DRIVE_ID_RE)) return '画像フォルダ (Driveリンク) が無いので、保存先がありません (画像タブで画像フォルダを設定してください)';
  { const sg = shootGateReason(db, draftId); if (sg) return sg; }   // 撮影判定と素材 (画像制作の新フロー PR-E)
  if (activeImageJob(db, draftId)) return 'この商品の画像をいま作っています';
  const plan = buildImagePlan({ outputText: composeTextOf(db, cj), packet: safeJson(cj.packet_json), quality: cfg.quality, maxImages: imageLimitForPriority(draft?.image_priority) });
  if (plan.error) return plan.error;
  if (plan.images.some((im) => im.est_jpy == null)) return '画像の品質段の設定が読めません';
  const { used_jpy } = monthUsage(db, now);
  // 取り置き (安全側の見込み額) の合計で見る
  const need = Math.ceil(plan.images.reduce((a, im) => a + im.est_jpy, 0));
  if (used_jpy + need > cfg.spendable) {
    return `今月の上限 (${cfg.budget.toLocaleString()} 円・安全幅 ${cfg.safety} 円を残す) を超えるおそれがあるので作れません (今月 ${Math.round(used_jpy)} 円・この商品の取り置き 約 ${need} 円)`;
  }
  return null;
}

const safeJson = (s) => { try { return JSON.parse(s); } catch { return null; } };

/**
 * 画像を作る依頼を受け付ける。prompt と参考画像はここで固定する。
 * 同じキーの再送は前の依頼を返す (二重クリック・通信のやり直しで増やさない)
 * @returns {{ok:true, job, created:boolean}|{code, error}}
 */
export function requestImageJob(db, { draft, folderId, idempotencyKey, actor, refTimes = null, expectedJobId, env = process.env, now = Date.now() } = {}) {
  const key = exact(idempotencyKey, IDEMPOTENCY_KEY_RE);
  if (!key) return { code: 'bad_request', error: 'idempotency_key の形が不正です (英数記号 8〜80 文字)' };
  const draftId = posInt(draft?.id);
  if (!draftId) return { code: 'bad_request', error: '商品の ID が不正です' };
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const prior = db.prepare('SELECT * FROM ph_lp_image_jobs WHERE draft_id = ? AND idempotency_key = ?').get(draftId, key);
    if (prior && prior.regen_of_image_id != null) return { code: 'bad_request', error: 'この idempotency_key は 1 枚の作り直しで使われています' };
    if (prior) return { ok: true, job: prior, created: false };
    { const cf = fullJobConflict(db, draftId, expectedJobId); if (cf) return { code: 'conflict', error: cf }; }   // 古いタブ (PR-E・Codex 名指し3 H)
    const blocked = imageBlockReason(db, { draft, folderId, env, now });
    if (blocked) return { code: activeImageJob(db, draftId) ? 'already_running' : 'not_ready', error: blocked };
    const cfg = lpImageConfig(env);
    const cj = latestComposeJob(db, draftId);
    const plan = buildImagePlan({ outputText: composeTextOf(db, cj), packet: safeJson(cj.packet_json), quality: cfg.quality, refTimes, maxImages: imageLimitForPriority(draft?.image_priority) });
    // 🚨 参考画像は全部、更新日時を固定する (作る前に照らせないものを通さない・#1612 R2 Medium)
    if (plan.images.some((im) => im.refs.some((r) => !r.modified_time))) {
      return { code: 'not_ready', error: '参考画像の更新日時を Drive から取れませんでした (もう一度押してください)' };
    }
    const id = Number(db.prepare(`INSERT INTO ph_lp_image_jobs
      (draft_id, compose_job_id, idempotency_key, status, model, quality, size, folder_id, requested_by, created_at)
      VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?)`)
      .run(draftId, cj.id, key, cfg.model, cfg.quality, cfg.size, folderId, trim(actor, 120) || 'unknown', nowS).lastInsertRowid);
    const ins = db.prepare(`INSERT INTO ph_lp_images (image_job_id, seq, no, name, prompt, refs_json, est_jpy, status) VALUES (?, ?, ?, ?, ?, ?, ?, 'queued')`);
    for (const im of plan.images) ins.run(id, im.seq, im.no, im.name, im.prompt, JSON.stringify(im.refs), im.est_jpy);
    logEvent(db, draftId, 'lp_image_requested', `画像 ${plan.images.length} 枚 (${cfg.model} / ${cfg.quality}・依頼 ${id})`, trim(actor, 120) || 'unknown');
    return { ok: true, job: db.prepare('SELECT * FROM ph_lp_image_jobs WHERE id = ?').get(id), created: true };
  }).immediate();
}

/** 画面に出す状態 (ポーリングで叩かれるので軽く) */
export function imageStateFor(db, { draft, folderId, env = process.env, now = Date.now() } = {}) {
  const cfg = lpImageConfig(env);
  const draftId = posInt(draft?.id);
  const { month, used_jpy } = monthUsage(db, now);
  // 画面の「生成した画像」の元 = いちばん新しい「全部作る」依頼 (1 枚ずつの作り直しは cards の版として重ねる)
  const job = draftId ? db.prepare('SELECT * FROM ph_lp_image_jobs WHERE draft_id = ? AND regen_of_image_id IS NULL ORDER BY id DESC LIMIT 1').get(draftId) : null;
  const images = job ? db.prepare(`SELECT id, seq, no, name, status, drive_file_id, error, cost_jpy FROM ph_lp_images
    WHERE image_job_id = ? ORDER BY seq`).all(job.id) : [];
  const cj = draftId ? latestComposeJob(db, draftId) : null;
  const planned = cj && cj.status === 'done' && cj.model_check === 'match' && trim(composeTextOf(db, cj))
    ? buildImagePlan({ outputText: composeTextOf(db, cj), packet: safeJson(cj.packet_json), quality: cfg.quality || DEFAULT_LP_IMAGE_QUALITY, maxImages: imageLimitForPriority(draft?.image_priority) }).images : [];
  return {
    enabled: cfg.enabled,
    usable: cfg.usable,
    model: cfg.model, quality: cfg.quality,
    budget_jpy: cfg.budget, month, used_jpy,
    planned_count: planned.length,
    // 押したときに取り置く額 (安全側の見込み・保証ではない)。実際はふつうもっと安い (検討 §5: 1 枚 数円)
    planned_reserve_jpy: Math.ceil(planned.reduce((a, im) => a + (im.est_jpy || 0), 0)),
    blocked: draftId ? imageBlockReason(db, { draft, folderId, env, now }) : '商品の ID が不正です',
    job: job ? {
      id: job.id, status: job.status, model: job.model, quality: job.quality, error: job.error,
      created_at: job.created_at, completed_at: job.completed_at, folder_id: job.folder_id,
      compose_job_id: job.compose_job_id,
      cost_jpy: images.reduce((a, im) => a + (Number(im.cost_jpy) || 0), 0),   // 1 枚ずつ整数の円 (切り上げ済み)
      images,
      ai_folder_id: aiFolderOf(db, job),   // 「Drive で開く」(AI初稿 フォルダ)
    } : null,
    // 画像制作の新フロー PR-E: 作れる条件のチェックリスト・1 枚ずつの版と確認
    ...imageFlowState(db, { draft, job, folderId, env, now }),
  };
}

// ─── 画像制作の新フロー PR-E (2026-10-09 スタッフ要望 ⑤・設計 §3.6) ─────────
// 撮影が不要ならそのまま作る / 撮影が要るなら素材が揃ってから。できた画像を 1 枚ずつ「確認」「作り直す」。
// 作り直しは **受付で固めた prompt と参考画像** (ph_lp_images の行) を写した新しい job (1 枚だけ)。構成は読み直さない。
// 予算は全部作るときと同じ台帳 (作る係の claimNext が 1 枚分を取り置き、実額で確定する) を通す。

const PENDING = ['queued', 'running'];

/**
 * 「全部作る」を押した画面が見ていた最新の依頼 (全部作る・1 枚の作り直しのどちらも・無ければ null = latest_job_id) と、
 * 今のいちばん新しい依頼が違うなら理由 (古いタブ・2 人同時。ほかの人が 1 枚作り直した後の古い画面も含む・Codex 名指し5 M)。
 * expectedJobId が undefined (送ってこない古い画面・試験) なら見ない。同じキーの再送はこれより先に前の依頼を返す
 */
export function fullJobConflict(db, draftId, expectedJobId) {
  if (expectedJobId === undefined) return null;
  const cur = db.prepare('SELECT id FROM ph_lp_image_jobs WHERE draft_id = ? ORDER BY id DESC LIMIT 1').get(posInt(draftId));
  if ((cur ? cur.id : null) === (expectedJobId === null ? null : posInt(expectedJobId))) return null;
  return 'ほかの人 (または別の画面) が先に画像を作りました。画面を読み直してから、もう一度作るか決めてください';
}

/**
 * 「Drive で開く」の AI初稿 フォルダ。全部作ったときに用意できなかった (Drive の失敗) 後で、作り直しが用意したら
 * そちら (同じ画像フォルダの中のもの) を使う — Codex PR-E R1 P2
 */
function aiFolderOf(db, job) {
  if (job.ai_folder_id) return job.ai_folder_id;
  const r = db.prepare(`SELECT ai_folder_id FROM ph_lp_image_jobs WHERE draft_id = ? AND folder_id IS ? AND ai_folder_id IS NOT NULL
    AND regen_of_image_id IN (SELECT id FROM ph_lp_images WHERE image_job_id = ?) ORDER BY id DESC LIMIT 1`).get(job.draft_id, job.folder_id, job.id);
  return r ? r.ai_folder_id : null;
}
const SHOOT_NEEDED = new Set(['inhouse', 'photographer']);

/**
 * 撮影の段取りから見た「作れない理由」。null なら作ってよい (画面のボタン・全部作る API・作り直す API で同じ判定)。
 *   撮影判定 (shoot_mode) がまだ → 作らない (撮影するかどうか決まっていないのに作ると、撮った後で作り直しになる)
 *   撮影が要る (社内撮影 / カメラマン撮影) → 撮影・素材ステータスが「素材完了」になってから
 *   撮影不要 → 素材は今あるもので作る
 * 文は「人が次に何をすればいいか」が分かる形にする
 */
export function shootGateReason(db, draftId) {
  const ip = db.prepare('SELECT shoot_mode, material_status FROM draft_image_production WHERE draft_id = ?').get(posInt(draftId)) || {};
  const mode = ip.shoot_mode ?? null;
  if (!mode) return '撮影判定がまだです。「2 仮LP構成と撮影判定」で 撮影不要 / 社内撮影 / カメラマン撮影 を選んでください';
  if (SHOOT_NEEDED.has(mode) && ip.material_status !== 'ready') {
    return `撮影判定が「${SHOOT_MODE_LABELS[mode]}」なので、撮影した素材が揃ってから作ります。素材を画像フォルダに入れたら、管理項目の「撮影・素材ステータス」を「素材完了」にしてください (いま: ${MATERIAL_STATUS_LABELS[ip.material_status] || '未設定'})`;
  }
  return null;
}

/**
 * 画面のチェックリスト (ラフ「上のチェックがそろうと押せます」)。押せるかどうかの正本は imageBlockReason —
 * ここは人に「どこまで済んだか」を見せるだけ
 */
export function imageChecklist(db, draftId) {
  const id = posInt(draftId);
  const cj = id ? latestComposeJob(db, id) : null;
  const ip = (id && db.prepare('SELECT shoot_mode, material_status FROM draft_image_production WHERE draft_id = ?').get(id)) || {};
  const composed = !!cj && cj.status === 'done' && cj.model_check === 'match' && !!trim(composeTextOf(db, cj));
  const items = [{ key: 'compose', ok: composed && !!ip.shoot_mode, text: '仮LP構成と撮影判定ができている' }];
  if (SHOOT_NEEDED.has(ip.shoot_mode)) {
    items.push({ key: 'material', ok: ip.material_status === 'ready', text: '撮影した素材が揃った (撮影・素材ステータスが「素材完了」)' });
  }
  return items;
}

/** 全部作る依頼 (作り直しではない) で、動いているもの */
const activeFullJob = (db, draftId) => db.prepare(`SELECT id FROM ph_lp_image_jobs
  WHERE draft_id = ? AND regen_of_image_id IS NULL AND status IN ('queued','running') LIMIT 1`).get(draftId) || null;

/** 画像 1 枚 (全部作ったときの行 = 元) と、その作り直しの版を古い順に。[0] が元 */
function versionsOf(db, rootId) {
  return db.prepare(`SELECT i.*, j.draft_id, j.requested_by, j.created_at AS requested_at, j.status AS job_status
    FROM ph_lp_images i JOIN ph_lp_image_jobs j ON j.id = i.image_job_id
    WHERE i.id = ? OR j.regen_of_image_id = ? ORDER BY i.id`).all(rootId, rootId);
}

/**
 * 作り直しを受け付けられない理由 (null なら作れる)。画面の「再生成」ボタンと API で同じ判定。
 * @param root  元の行 (ph_lp_images と job の列: model / quality / folder_id / draft_id …)
 * @param head  いちばん新しい版 (作っている途中・失敗も含む)
 */
function regenBlockReason(db, { root, head, folderId, env, now }) {
  const cfg = lpImageConfig(env);
  if (!cfg.usable) return cfg.error;
  const latestFull = db.prepare('SELECT id FROM ph_lp_image_jobs WHERE draft_id = ? AND regen_of_image_id IS NULL ORDER BY id DESC LIMIT 1').get(root.draft_id);
  if (!latestFull || latestFull.id !== root.image_job_id) return 'あとで全部作り直したので、この画像は古くなっています (画面を読み直してください)';
  if (activeFullJob(db, root.draft_id)) return 'この商品の画像をいま全部作っています (終わってから作り直せます)';
  if (PENDING.includes(head.status)) return 'この画像をいま作り直しています';
  if (!exact(folderId, DRIVE_ID_RE)) return '画像フォルダ (Driveリンク) が無いので、保存先がありません (画像タブで画像フォルダを設定してください)';
  const sg = shootGateReason(db, root.draft_id);
  if (sg) return sg;
  // 受付のときのモデル・品質段・取り置き額で作る (作る係と同じ確かめ)。読めなければ作らない
  if (!LP_IMAGE_MODELS.includes(root.model) || !LP_IMAGE_QUALITIES.includes(root.quality) || !(Number(root.est_jpy) > 0)) {
    return 'この画像の依頼のモデル・品質段・取り置き額が読めないので作り直せません (「もう一度画像を作る (全部)」で作ってください)';
  }
  // 今月の使った額 + まだ取り置いていない待ちの画像 (どの商品も) + この 1 枚の取り置き。作る係も掴むときにもう一度見る
  const { used_jpy } = monthUsage(db, now);
  const waiting = Number(db.prepare(`SELECT COALESCE(SUM(i.est_jpy), 0) AS s FROM ph_lp_images i
    JOIN ph_lp_image_jobs j ON j.id = i.image_job_id WHERE i.status = 'queued' AND j.status IN ('queued','running')
      AND NOT EXISTS (SELECT 1 FROM ph_ai_usage u WHERE u.kind = 'lp_image' AND u.ref_id = i.id)`).get().s) || 0;   // 取り置き済みは used_jpy に入っている (二重に数えない)
  const est = Number(root.est_jpy);
  if (used_jpy + waiting + est > cfg.spendable) {
    return `今月の上限 (${cfg.budget.toLocaleString()} 円・安全幅 ${cfg.safety} 円を残す) を超えるおそれがあるので作り直せません (今月 ${Math.round(used_jpy)} 円${waiting ? '・待ちの画像 ' + waiting + ' 円' : ''}・この 1 枚の取り置き 約 ${est} 円)`;
  }
  return null;
}

/** 元の行を job の列と一緒に (draft と、作り直しではない依頼の行であることも確かめる) */
const rootRow = (db, rootId) => db.prepare(`SELECT i.*, j.draft_id, j.model, j.quality, j.size, j.folder_id, j.ai_folder_id, j.compose_job_id
  FROM ph_lp_images i JOIN ph_lp_image_jobs j ON j.id = i.image_job_id WHERE i.id = ? AND j.regen_of_image_id IS NULL`).get(rootId) || null;

/** 画面に渡す 1 枚の行の id から、元の行の id (作り直しの版なら job の regen_of_image_id) */
function rootIdOf(db, imageId, draftId) {
  const r = db.prepare(`SELECT i.id, j.draft_id, j.regen_of_image_id FROM ph_lp_images i
    JOIN ph_lp_image_jobs j ON j.id = i.image_job_id WHERE i.id = ?`).get(imageId);
  if (!r || r.draft_id !== draftId) return null;
  return r.regen_of_image_id ?? r.id;
}

/**
 * 1 枚の画面の形。head = いちばん新しい版 (途中・失敗を含む) / current = いちばん新しい「できた」版
 * checked = 今出している版を人が確認した (作っている途中は確認済みにしない)
 */
function cardOf(db, root, { folderId, env, now }) {
  const vs = versionsOf(db, root.id);
  const head = vs[vs.length - 1];
  const current = [...vs].reverse().find((v) => v.status === 'done' && v.drive_file_id) || null;
  const pending = PENDING.includes(head.status);
  return {
    root_id: root.id, seq: root.seq, no: root.no, name: root.name,
    head_id: head.id, head_version: head.version || 1, head_status: head.status,
    // 新しい版が作れなかったときの理由 (前の版の画像はそのまま出す)
    head_error: head.status !== 'done' ? (head.error || null) : null,
    current: current ? {
      id: current.id, version: current.version || 1, drive_file_id: current.drive_file_id, completed_at: current.completed_at,
      checked_at: current.checked_at || null, checked_by: current.checked_by || null,
    } : null,
    checked: !!current && !!current.checked_at && !pending,
    pending,
    versions: vs.filter((v) => v.status === 'done' && v.drive_file_id).length,
    regen_est_jpy: Number(root.est_jpy) || 0,
    regen_blocked: regenBlockReason(db, { root, head, folderId, env, now }),
  };
}

/** imageStateFor に足す分 (チェックリスト・カード・確認済みの数・作り直し中か・構成が変わったか) */
function imageFlowState(db, { draft, job, folderId, env, now }) {
  const draftId = posInt(draft?.id);
  const cards = [];
  if (job) {
    for (const im of db.prepare('SELECT id FROM ph_lp_images WHERE image_job_id = ? ORDER BY seq').all(job.id)) {
      const root = rootRow(db, im.id);
      if (root) cards.push(cardOf(db, root, { folderId, env, now }));
    }
  }
  return {
    // 画面が見ている最新の依頼 (全部作る・1 枚の作り直しとも)。「全部作る」に expected_job_id として添える
    latest_job_id: draftId ? (db.prepare('SELECT MAX(id) AS m FROM ph_lp_image_jobs WHERE draft_id = ?').get(draftId).m ?? null) : null,
    checklist: draftId ? imageChecklist(db, draftId) : [],
    cards,
    checked_count: cards.filter((c) => c.checked).length,
    // 確認の対象 = できた画像 (作れなかった 1 枚は数に入れない)
    checkable_count: cards.filter((c) => c.current).length,
    // 画面に出す出来ぐあい: 全部作る依頼の状態ではなく、カードの最新の版で見る (失敗した 1 枚を作り直せたら「できました」・Codex 名指し1 M)。
    // 依頼の行 (job.status) は記録のまま変えない
    flow_status: !job ? null : PENDING.includes(job.status) ? job.status
      : cards.length && cards.every((c) => c.current) ? 'done' : cards.some((c) => c.current) ? 'partial' : 'failed',
    // この画像にかかった額 (全部作る + 1 枚ずつの作り直し)
    flow_cost_jpy: job ? Number(db.prepare(`SELECT COALESCE(SUM(i.cost_jpy), 0) AS s FROM ph_lp_images i JOIN ph_lp_image_jobs j ON j.id = i.image_job_id
      WHERE j.id = ? OR j.regen_of_image_id IN (SELECT id FROM ph_lp_images WHERE image_job_id = ?)`).get(job.id, job.id).s) || 0 : 0,
    regen_running: cards.some((c) => c.pending && c.head_version > 1),
    // 画像を作った後で構成を作り直した・直した (もう一度全部作ると反映される)。決め方は LP構成の一覧と同じ (lp-edit の imageStaleFor)
    compose_changed: !!job && !!draftId && imageStaleFor(db, draftId, effectiveCompose(db, draftId)),
  };
}

/**
 * 1 枚だけ作り直す依頼を受け付ける。prompt・参考画像・取り置き額は**元の行から写す** (構成を読み直さない)。
 * 同じキーの再送は前の依頼を返す (二重クリック・通信のやり直しで増やさない)。
 * @param imageId 画面が出していた「いちばん新しい版」の行の id。もう新しい版がある (別の人・別のタブが先に作り直した) なら断る
 * @returns {{ok:true, job, created:boolean}|{code, error}}
 */
export function requestImageRegen(db, { draft, imageId, folderId, idempotencyKey, actor, env = process.env, now = Date.now() } = {}) {
  const key = exact(idempotencyKey, IDEMPOTENCY_KEY_RE);
  if (!key) return { code: 'bad_request', error: 'idempotency_key の形が不正です (英数記号 8〜80 文字)' };
  const draftId = posInt(draft?.id);
  if (!draftId) return { code: 'bad_request', error: '商品の ID が不正です' };
  const imgId = posInt(imageId);
  if (!imgId) return { code: 'bad_request', error: '画像の ID が不正です' };
  const nowS = new Date(now).toISOString();
  const who = trim(actor, 120) || 'unknown';
  return db.transaction(() => {
    const rootId = rootIdOf(db, imgId, draftId);
    const root = rootId ? rootRow(db, rootId) : null;
    if (!root) return { code: 'not_found', error: 'この商品の画像が見つかりません (画面を読み直してください)' };
    const prior = db.prepare('SELECT * FROM ph_lp_image_jobs WHERE draft_id = ? AND idempotency_key = ?').get(draftId, key);
    if (prior) {
      if (prior.regen_of_image_id !== root.id) return { code: 'bad_request', error: 'この idempotency_key は別の依頼で使われています' };
      return { ok: true, job: prior, created: false };
    }
    const vs = versionsOf(db, root.id);
    const head = vs[vs.length - 1];
    if (head.id !== imgId) {
      return { code: 'conflict', error: 'ほかの人 (または別の画面) がこの画像をもう作り直しています。画面を読み直してから押してください' };
    }
    const blocked = regenBlockReason(db, { root, head, folderId, env, now });
    if (blocked) return { code: PENDING.includes(head.status) || activeFullJob(db, draftId) ? 'already_running' : 'not_ready', error: blocked };
    const version = Math.max(...vs.map((v) => Number(v.version) || 1)) + 1;
    // 保存先は今の画像フォルダ。元と同じなら「AI初稿」も同じフォルダを使う (違えば作る係が用意する)
    const aiFolder = root.folder_id === folderId ? (root.ai_folder_id || null) : null;
    const id = Number(db.prepare(`INSERT INTO ph_lp_image_jobs
      (draft_id, compose_job_id, idempotency_key, status, model, quality, size, folder_id, ai_folder_id, requested_by, created_at, regen_of_image_id)
      VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?)`)
      .run(draftId, root.compose_job_id, key, root.model, root.quality, root.size, folderId, aiFolder, who, nowS, root.id).lastInsertRowid);
    db.prepare(`INSERT INTO ph_lp_images (image_job_id, seq, no, name, prompt, refs_json, est_jpy, status, version)
      VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', ?)`).run(id, root.seq, root.no, root.name, root.prompt, root.refs_json, root.est_jpy, version);
    // 作り直したら確認は外す (前の版を確認したことを、新しい版の確認に持ち越さない)
    db.prepare(`UPDATE ph_lp_images SET checked_at = NULL, checked_by = NULL
      WHERE id = ? OR image_job_id IN (SELECT id FROM ph_lp_image_jobs WHERE regen_of_image_id = ?)`).run(root.id, root.id);
    logEvent(db, draftId, 'lp_image_regen_requested',
      `画像 ${imageLabel(root)} を作り直し (v${version}・依頼 ${id}・取り置き 約 ${root.est_jpy} 円)`, who);
    return { ok: true, job: db.prepare('SELECT * FROM ph_lp_image_jobs WHERE id = ?').get(id), created: true };
  }).immediate();
}

const imageLabel = (im) => (im.no != null ? im.no + '枚目' : im.seq + '番目') + (im.name ? '｜' + trim(im.name, 40) : '');

/**
 * 1 枚の「確認」を付ける / 外す (誰がいつ)。確認するのは**画面に出していた版** (imageId) — もう新しい版が
 * できている・作り直している途中なら断る (古い画面で、見ていない新しい版を確認済みにしない)
 * @returns {{ok:true, changed:boolean}|{code, error}}
 */
export function setImageChecked(db, { draft, imageId, checked, actor, now = Date.now() } = {}) {
  const draftId = posInt(draft?.id);
  if (!draftId) return { code: 'bad_request', error: '商品の ID が不正です' };
  const imgId = posInt(imageId);
  if (!imgId) return { code: 'bad_request', error: '画像の ID が不正です' };
  if (typeof checked !== 'boolean') return { code: 'bad_request', error: 'checked は true / false で指定してください' };
  const who = trim(actor, 120) || 'unknown';
  return db.transaction(() => {
    const rootId = rootIdOf(db, imgId, draftId);
    const root = rootId ? rootRow(db, rootId) : null;
    if (!root) return { code: 'not_found', error: 'この商品の画像が見つかりません (画面を読み直してください)' };
    const latestFull = db.prepare('SELECT id FROM ph_lp_image_jobs WHERE draft_id = ? AND regen_of_image_id IS NULL ORDER BY id DESC LIMIT 1').get(draftId);
    if (!latestFull || latestFull.id !== root.image_job_id) return { code: 'conflict', error: 'あとで全部作り直したので、この画像は古くなっています (画面を読み直してください)' };
    const vs = versionsOf(db, root.id);
    const head = vs[vs.length - 1];
    const current = [...vs].reverse().find((v) => v.status === 'done' && v.drive_file_id) || null;
    if (PENDING.includes(head.status)) return { code: 'conflict', error: 'この画像はいま作り直しています (できてから確認してください)' };
    if (!current) return { code: 'conflict', error: 'まだできていない画像は確認できません' };
    if (current.id !== imgId) return { code: 'conflict', error: 'この画像は作り直されています。画面を読み直して、新しい版を見てから確認してください' };
    if (!!current.checked_at === checked) return { ok: true, changed: false };
    db.prepare('UPDATE ph_lp_images SET checked_at = ?, checked_by = ? WHERE id = ?')
      .run(checked ? new Date(now).toISOString() : null, checked ? who : null, current.id);
    logEvent(db, draftId, checked ? 'lp_image_checked' : 'lp_image_unchecked',
      `画像 ${imageLabel(root)} (v${current.version || 1}) を${checked ? '確認済みにした' : '確認済みから戻した'}`, who);
    return { ok: true, changed: true };
  }).immediate();
}

// ─── 作る係 (Render のプロセスの中で 1 枚ずつ) ───────────────────

/**
 * 作る係。router.js が本物の部品 (OpenAI・Drive) を渡して作る。試験は偽物を渡す。
 * @param deps.getDB        () => db
 * @param deps.generate     async ({apiKey, model, quality, size, prompt, refs:[{buf, mime, filename}]}) => {buf, usage}
 *                          失敗は Error に status (HTTP) / code / transient (結果が分からない) を付けて throw
 * @param deps.fetchRef     async (fileId) => {buf, mime}      参考画像 (商品の写真・素材) を Drive から
 * @param deps.ensureFolder async (parentId) => folderId       商品の画像フォルダの中の「AI初稿」
 * @param deps.upload       async ({folderId, name, buf}) => fileId
 * @param deps.env / deps.now / deps.sleep
 */
/** 止まった Promise で作る係が止まり続けないよう、部品の呼び出しに時間の上限を付ける (#1612 R2 Medium) */
function withTimeout(p, ms, label) {
  let t;
  return Promise.race([
    Promise.resolve(p).finally(() => clearTimeout(t)),
    new Promise((_, rej) => { t = setTimeout(() => rej(Object.assign(new Error(`${label} が ${Math.round(ms / 1000)} 秒で終わりませんでした`), { timeout: true })), ms); }),
  ]);
}
const STEP_TIMEOUT_MS = { ref: 60_000, folder: 60_000, generate: 300_000, upload: 180_000 };

export function createLpImageWorker(deps) {
  const env = () => deps.env || process.env;
  const now = () => (deps.now ? deps.now() : Date.now());
  const sleep = deps.sleep || ((ms) => new Promise((r) => setTimeout(r, ms)));
  // 部品ごとの時間の上限 (試験は短くできる)
  const T = { ...STEP_TIMEOUT_MS, ...(deps.timeouts || {}) };
  // このプロセスの印。作っている画像に付け、期限 (lease) が切れたものだけを片付ける (#1612 R1 Medium)
  const token = deps.token || `w-${process.pid}-${Math.random().toString(36).slice(2, 10)}`;
  let running = false;
  let again = false;
  let wakeTimer = null;
  const setTimer = deps.setTimer || ((fn, ms) => setTimeout(fn, ms));
  const clearTimer = deps.clearTimer || ((t) => clearTimeout(t));

  const leaseUntil = () => new Date(now() + LEASE_MIN * 60_000).toISOString();
  /** 作っている間、段階ごとに期限を延ばす (生成 4 分 × やり直し 2 回 + Drive でも切れないように) */
  const renew = (db, id) => db.prepare(`UPDATE ph_lp_images SET lease_until = ? WHERE id = ? AND status = 'running' AND claimed_by = ?`)
    .run(leaseUntil(), id, token).changes === 1;

  /**
   * 期限の切れた「作っている途中」の画像を失敗にする (Render の再起動などで止まったもの)。
   * 🚨 期限内のものは触らない — 入れ替え中の古いプロセスがまだ作っているかもしれない。
   * 費用は取り置き額のまま (請求されたものとして)。自動では作り直さない
   */
  function recover() {
    const db = deps.getDB();
    const nowS = new Date(now()).toISOString();
    db.transaction(() => {
      for (const im of db.prepare(`SELECT id FROM ph_lp_images WHERE status = 'running' AND (lease_until IS NULL OR lease_until < ?)`).all(nowS)) {
        const ch = db.prepare(`UPDATE ph_lp_images SET status = 'failed', error = ?, completed_at = ?
          WHERE id = ? AND status = 'running' AND (lease_until IS NULL OR lease_until < ?)`)
          .run('途中で止まりました (Render の再起動など)。費用は取り置き額のまま数えています', nowS, im.id, nowS).changes;
        if (ch) {
          db.prepare(`UPDATE ph_ai_usage SET status = 'unknown', finished_at = ?, error = ? WHERE kind = 'lp_image' AND ref_id = ? AND status = 'reserved'`)
            .run(nowS, 'interrupted', im.id);
        }
      }
    }).immediate();
    finalizeJobs(db);
  }

  function finalizeJobs(db) {
    const nowS = new Date(now()).toISOString();
    for (const j of db.prepare(`SELECT id, draft_id FROM ph_lp_image_jobs WHERE status IN ('queued','running')`).all()) {
      const c = db.prepare(`SELECT
          SUM(status IN ('queued','running')) AS pending, SUM(status = 'done') AS done, COUNT(*) AS total
        FROM ph_lp_images WHERE image_job_id = ?`).get(j.id);
      if (Number(c.pending) > 0) continue;
      const status = Number(c.done) === Number(c.total) && Number(c.total) > 0 ? 'done' : Number(c.done) > 0 ? 'partial' : 'failed';
      const ch = db.prepare(`UPDATE ph_lp_image_jobs SET status = ?, completed_at = COALESCE(completed_at, ?) WHERE id = ? AND status IN ('queued','running')`)
        .run(status, nowS, j.id).changes;
      if (ch) logEvent(db, j.draft_id, 'lp_image_' + status, `画像 ${c.done}/${c.total} 枚 (依頼 ${j.id})`, 'ph-lp-image');
    }
  }

  /** 残りの画像を全部 skipped にする (上限・キーが使えない・保存できない・設定が外れた) */
  function skipRest(db, jobId, reason) {
    const nowS = new Date(now()).toISOString();
    db.prepare(`UPDATE ph_lp_images SET status = 'skipped', error = ?, completed_at = ? WHERE image_job_id = ? AND status = 'queued'`)
      .run(reason, nowS, jobId);
    db.prepare(`UPDATE ph_lp_image_jobs SET error = COALESCE(error, ?) WHERE id = ?`).run(reason, jobId);
  }

  /** 次の 1 枚を掴む (queued → running)。予算もここで取り置く (同じトランザクション) */
  function claimNext(db) {
    const nowS = new Date(now()).toISOString();
    return db.transaction(() => {
      const im = db.prepare(`SELECT i.*, j.draft_id, j.model, j.quality, j.size, j.folder_id, j.ai_folder_id
        FROM ph_lp_images i JOIN ph_lp_image_jobs j ON j.id = i.image_job_id
        WHERE i.status = 'queued' AND j.status IN ('queued','running') ORDER BY j.id, i.seq LIMIT 1`).get();
      if (!im) return null;
      const cfg = lpImageConfig(env());
      if (!cfg.usable) { skipRest(db, im.image_job_id, '途中で止めました: ' + cfg.error); return { skipped: true }; }
      // 撮影の段取りも掴むときにもう一度見る (受付の後で「撮影が要る」に変わった・素材完了を外した。PR-E・Codex 名指し1 M)
      { const sg = shootGateReason(db, im.draft_id); if (sg) { skipRest(db, im.image_job_id, '途中で止めました: ' + sg); return { skipped: true }; } }
      // 🚨 依頼のときのモデル・品質段で作る (途中で設定が変わっても混ぜない)。読めない値・取り置き額が無いなら止める
      if (!LP_IMAGE_MODELS.includes(im.model) || !LP_IMAGE_QUALITIES.includes(im.quality) || !(Number(im.est_jpy) > 0)) {
        skipRest(db, im.image_job_id, '依頼のモデル・品質段・取り置き額が読めません'); return { skipped: true };
      }
      const est = Number(im.est_jpy);
      const { month, used_jpy } = monthUsage(db, now());
      if (used_jpy + est > cfg.spendable) {
        skipRest(db, im.image_job_id, `今月の上限 (${cfg.budget.toLocaleString()} 円・安全幅 ${cfg.safety} 円を残す) に達したので止めました`);
        return { skipped: true };
      }
      const ch = db.prepare(`UPDATE ph_lp_images SET status = 'running', started_at = ?, claimed_by = ?, lease_until = ?
        WHERE id = ? AND status = 'queued'`).run(nowS, token, leaseUntil(), im.id).changes;
      if (ch !== 1) return { skipped: true };
      db.prepare(`UPDATE ph_lp_image_jobs SET status = 'running', started_at = COALESCE(started_at, ?) WHERE id = ? AND status = 'queued'`).run(nowS, im.image_job_id);
      const usageId = Number(db.prepare(`INSERT INTO ph_ai_usage (kind, month, draft_id, ref_id, model, quality, status, est_jpy, created_at)
        VALUES ('lp_image', ?, ?, ?, ?, ?, 'reserved', ?, ?)`)
        .run(month, im.draft_id, im.id, im.model, im.quality, est, nowS).lastInsertRowid);
      return { im, usageId, est };
    }).immediate();
  }

  function finishUsage(db, usageId, { status, cost = null, usage = null, error = null }) {
    const det = usage?.input_tokens_details || {};
    db.prepare(`UPDATE ph_ai_usage SET status = ?, cost_jpy = ?, in_text_tokens = ?, in_image_tokens = ?, out_tokens = ?, error = ?, finished_at = ?
      WHERE id = ? AND status = 'reserved'`)
      .run(status, cost, Number.isFinite(Number(det.text_tokens)) ? Number(det.text_tokens) : null,
        Number.isFinite(Number(det.image_tokens)) ? Number(det.image_tokens) : null,
        Number.isFinite(Number(usage?.output_tokens)) ? Number(usage.output_tokens) : null,
        error ? String(error).slice(0, 300) : null, new Date(now()).toISOString(), usageId);
  }

  /** 自分が掴んでいる画像だけを確定する (片付けで失敗にされた後なら何もしない) */
  function finishImage(db, id, fields) {
    return db.prepare(`UPDATE ph_lp_images SET status = ?, drive_file_id = ?, error = ?, cost_jpy = ?, completed_at = ?
      WHERE id = ? AND status = 'running' AND claimed_by = ?`)
      .run(fields.status, fields.drive_file_id || null, fields.error ? String(fields.error).slice(0, 500) : null,
        fields.cost ?? null, new Date(now()).toISOString(), id, token).changes === 1;
  }

  /**
   * 呼ぶ直前の確かめ (1 回の呼び出しごと・即時トランザクション)。
   *   - この画像をまだ自分が掴んでいて、台帳の取り置きが残っていること。期限切れで片付けられた (recover) 後に
   *     古い作る係が戻ってきても呼ばない → { lost: true } (片付けた側が記録済みなので何も書かない・Codex 名指し4 H)
   *   - 掴んでから JST の月をまたいだら、取り置きを新しい月へ移す (Codex 名指し2 M)
   *   - 今の月の使った額 (この取り置きを含む) が「上限 − 安全幅」を超えていたら呼ばない (掴んだ後で、ほかの画像の実額が
   *     取り置きを超えて上限に達した・Codex 名指し4 H) → 止める理由の文
   * @returns {null|string|{lost:true}}
   */
  function preCallCheck(db, c) {
    return db.transaction(() => {
      const mine = db.prepare(`SELECT 1 FROM ph_lp_images WHERE id = ? AND status = 'running' AND claimed_by = ?`).get(c.im.id, token);
      const u = db.prepare('SELECT month FROM ph_ai_usage WHERE id = ? AND status = ?').get(c.usageId, 'reserved');
      if (!mine || !u) return { lost: true };
      const month = jstMonth(now());
      const moved = u.month !== month;
      if (moved) db.prepare('UPDATE ph_ai_usage SET month = ? WHERE id = ?').run(month, c.usageId);
      const cfg = lpImageConfig(env());
      if (!cfg.usable) return '途中で止めました: ' + cfg.error;
      const { used_jpy } = monthUsage(db, now());   // この取り置きを含む
      if (used_jpy > cfg.spendable) return `今月の上限 (${cfg.budget.toLocaleString()} 円・安全幅 ${cfg.safety} 円を残す) に達したので止めました${moved ? ' (作る途中で月が変わりました)' : ''}`;
      return null;
    }).immediate();
  }

  /** まだ呼んでいない段階の失敗 = 請求なし */
  function failBeforeCall(db, c, why, { stopRest = false } = {}) {
    finishUsage(db, c.usageId, { status: 'failed', cost: 0, error: why });
    finishImage(db, c.im.id, { status: 'failed', error: why });
    if (stopRest) skipRest(db, c.im.image_job_id, why);
  }

  async function processOne(db, c) {
    const { im, usageId, est } = c;
    // 1) 「AI初稿」フォルダを**作る前に**用意する (1 依頼に 1 回・#1612 R1 Medium)。
    //    書き込めないのに作ってから気づくと、全部の画像にお金を払ってから全部保存できない
    let aiFolder = db.prepare('SELECT ai_folder_id FROM ph_lp_image_jobs WHERE id = ?').get(im.image_job_id).ai_folder_id;
    if (!aiFolder) {
      try {
        aiFolder = await withTimeout(deps.ensureFolder(im.folder_id), T.folder, '「AI初稿」の用意');
        if (!exact(String(aiFolder || ''), DRIVE_ID_RE)) throw new Error('フォルダの ID が返ってきませんでした');
        db.prepare('UPDATE ph_lp_image_jobs SET ai_folder_id = COALESCE(ai_folder_id, ?) WHERE id = ?').run(aiFolder, im.image_job_id);
        aiFolder = db.prepare('SELECT ai_folder_id FROM ph_lp_image_jobs WHERE id = ?').get(im.image_job_id).ai_folder_id;
      } catch (e) {
        failBeforeCall(db, c, '画像フォルダの中に「AI初稿」を作れません (Drive の権限・共有を確かめてください): ' + String(e?.message || e).slice(0, 160), { stopRest: true });
        return;
      }
    }
    renew(db, im.id);
    // 2) 参考画像。受付時の更新日時と違えば (差し替わっていたら) 作らない (#1612 R1 Medium)。失敗 = まだ呼んでいないので請求なし
    const refs = [];
    try {
      const list = safeJson(im.refs_json) || [];
      for (const [k, r] of list.entries()) {
        // 更新日時の無い参考画像は受付で断っている。ここでも無ければ照らせないので作らない (fail-closed)
        if (!r.modified_time) throw Object.assign(new Error('参考画像の更新日時がありません'), { changed: true });
        const got = await withTimeout(deps.fetchRef(r.file_id, { expectedModifiedTime: r.modified_time }), T.ref, '参考画像の取得');
        refs.push({ buf: got.buf, mime: got.mime || 'image/jpeg', filename: `ref-${k + 1}.${/png/.test(got.mime || '') ? 'png' : (/webp/.test(got.mime || '') ? 'webp' : 'jpg')}` });
      }
    } catch (e) {
      const why = e?.changed ? '参考画像が依頼のあとで差し替わりました (新しい参考画像で作るときは「もう一度画像を作る (全部)」を押してください)'
        : '参考画像を Drive から取れませんでした: ' + String(e?.message || e).slice(0, 200);
      failBeforeCall(db, c, why);
      return;
    }
    renew(db, im.id);
    // 3) 生成 (混んでいる 429 だけ少し待って 2 回まで)
    let result = null;
    for (let attempt = 0; ; attempt++) {
      // 掴んでから呼ぶまで (参考画像・429 の待ち) に、片付けられた・月をまたいだ・上限に達した なら呼ばない (preCallCheck)
      const pre = preCallCheck(db, c);
      if (pre && pre.lost) return;   // もう自分の画像ではない (片付けられた)。呼ばない
      if (pre) { failBeforeCall(db, c, pre, { stopRest: true }); return; }
      // 撮影の段取りを呼ぶたびの直前にもう一度見る (参考画像を取っている間・429 の待ちの間に変わったら、呼ばずに 0 円で止める・Codex 名指し1 M / 名指し3 M)
      { const sg = shootGateReason(db, im.draft_id); if (sg) { failBeforeCall(db, c, '途中で止めました: ' + sg, { stopRest: true }); return; } }
      try {
        result = await withTimeout(deps.generate({
          apiKey: env().OPENAI_LP_IMAGE_API_KEY, model: im.model, quality: im.quality, size: im.size, prompt: im.prompt, refs,
        }), T.generate, '画像の生成');
        break;
      } catch (e) {
        // 時間切れ = 向こうで作られたか分からない
        if (e?.timeout) e.transient = true;
        const status = Number(e?.status) || 0;
        if (status === 429 && attempt < 2) { await sleep(20_000 * (attempt + 1)); renew(db, im.id); continue; }
        // 🚨 請求されないと言い切れるのは、処理の前に断られるものだけ (キー・権限・モデルが無い・混雑)。
        //    内容で断られた (moderation) は出力の後で止まることもあるので、ほかの失敗と同じく取り置き額のまま (#1612 R1 High)
        const noCharge = !e?.transient && [401, 403, 404, 429].includes(status) && e?.code !== 'moderation_blocked';
        finishUsage(db, usageId, { status: noCharge ? 'failed' : 'unknown', cost: noCharge ? 0 : null, error: `${status || 'net'} ${e?.code || ''} ${e?.message || ''}` });
        const why = e?.code === 'moderation_blocked' ? '画像の AI に内容で断られました (moderation)。費用は取り置き額のまま数えています'
          : status === 401 || status === 403 ? 'OpenAI のキーが使えません (Render の OPENAI_LP_IMAGE_API_KEY)'
          : status === 429 ? 'OpenAI が混んでいて作れませんでした (少し時間をおいてもう一度押してください)'
          : `画像を作れませんでした (${status || '通信'}: ${String(e?.message || '').slice(0, 160)})`;
        finishImage(db, im.id, { status: 'failed', error: why });
        if (status === 401 || status === 403 || status === 404) skipRest(db, im.image_job_id, why);
        return;
      }
    }
    renew(db, im.id);
    // 4) 費用を確定 (作れた = 請求された)。usage が無ければ取り置き額のまま
    const cost = costJpyFromUsage(result.usage) ?? est;
    // 取り置きを超えても実額のまま正直に記録する (次の 1 枚からは上限で止まる)。めったに無いはずなので記録に残す
    finishUsage(db, usageId, { status: 'charged', cost, usage: result.usage, error: cost > est ? `取り置き ${est} 円を超えた (${cost} 円)` : null });
    if (cost > est) console.warn('[product-hub] lp-image: 取り置きを超えた', { image: im.id, est, cost });
    // 5) Drive の「AI初稿」に保存。失敗したら残りは作らない (保存できないのにお金を使い続けない・#1612 R1 Medium)
    try {
      const draft = db.prepare('SELECT ne_code FROM product_drafts WHERE id = ?').get(im.draft_id) || {};
      // 作り直した版は _v2, _v3… を付けた新しいファイル (前の版は消さない・PR-E)
      const ver = Number(im.version) > 1 ? '_v' + Number(im.version) : '';
      const name = `${trim(draft.ne_code, 60) || 'draft' + im.draft_id}_AI初稿_${im.image_job_id}_${String(im.seq).padStart(2, '0')}${im.no != null ? '_' + im.no + '枚目' : ''}${ver}.png`;
      const fileId = await withTimeout(deps.upload({ folderId: aiFolder, name, buf: result.buf }), T.upload, 'Drive への保存');
      // 返ってきた ID が Drive の ID の形でなければ保存できていない (#1612 R2 Low)
      if (!exact(String(fileId || ''), DRIVE_ID_RE)) throw new Error('保存したファイルの ID が返ってきませんでした');
      finishImage(db, im.id, { status: 'done', drive_file_id: fileId, cost });
    } catch (e) {
      const why = '作れましたが Drive に保存できませんでした: ' + String(e?.message || e).slice(0, 200);
      finishImage(db, im.id, { status: 'failed', cost, error: why });
      skipRest(db, im.image_job_id, 'Drive に保存できないので残りは作っていません');
    }
  }

  /** 残っている画像を順に作る。動いている間に呼ばれたら、終わってからもう一周する */
  async function kick() {
    if (running) { again = true; return; }
    running = true;
    try {
      do {
        again = false;
        const db = deps.getDB();
        recover();   // 期限の切れた途中のものを先に片付ける
        for (;;) {
          const c = claimNext(db);
          if (!c) break;
          if (c.skipped) { finalizeJobs(db); continue; }
          try { await processOne(db, c); }
          catch (e) {
            // ここに来るのは想定外 (部品の作りの誤り)。費用は取り置き額のまま・画像は失敗
            try {
              finishUsage(db, c.usageId, { status: 'unknown', error: 'worker: ' + (e?.message || e) });
              finishImage(db, c.im.id, { status: 'failed', error: '想定外の失敗: ' + String(e?.message || e).slice(0, 200) });
            } catch { /* 記録もできないときは recover が拾う */ }
          }
          finalizeJobs(db);
        }
        finalizeJobs(db);
      } while (again);
      try { scheduleWake(deps.getDB()); } catch { /* 次に起こされたときに拾う */ }
    } finally {
      running = false;
    }
  }

  /**
   * 作っている途中の画像が残っていたら、いちばん早い期限の少し後に 1 回だけ起こす (#1612 R4 Medium)。
   * 再起動の直後は前のプロセスの期限内なので片付けられない。定期の見回りは置かず、この 1 回で拾う
   */
  function scheduleWake(db) {
    if (wakeTimer) { clearTimer(wakeTimer); wakeTimer = null; }
    const r = db.prepare(`SELECT MIN(lease_until) AS t FROM ph_lp_images WHERE status = 'running' AND lease_until IS NOT NULL`).get();
    if (!r?.t) return null;
    const ms = Math.max(1_000, Date.parse(r.t) - now() + 5_000);
    wakeTimer = setTimer(() => { wakeTimer = null; kick().catch(() => {}); }, ms);
    return ms;
  }

  return { kick, recover, isRunning: () => running, token, scheduleWake };
}

// ─── 本物の部品 ─────────────────────────────────────────

/**
 * OpenAI Images API。参考画像があれば /v1/images/edits (multipart・image[] 最大 4 枚)、無ければ /v1/images/generations。
 * 出典: developers.openai.com/api/docs/guides/image-generation (2026-10-04 確認)
 */
export async function openaiGenerateImage({ apiKey, model, quality, size, prompt, refs = [] }, { fetchImpl = fetch, timeoutMs = 240_000 } = {}) {
  if (!apiKey) throw Object.assign(new Error('キーがありません'), { status: 401 });
  let res;
  try {
    if (refs.length) {
      const form = new FormData();
      form.append('model', model);
      form.append('prompt', prompt);
      form.append('size', size);
      form.append('quality', quality);
      form.append('output_format', 'png');
      form.append('n', '1');
      for (const r of refs.slice(0, MAX_REFS)) form.append('image[]', new Blob([r.buf], { type: r.mime || 'image/jpeg' }), r.filename || 'ref.jpg');
      res = await fetchImpl('https://api.openai.com/v1/images/edits', {
        method: 'POST', headers: { Authorization: `Bearer ${apiKey}` }, body: form, signal: AbortSignal.timeout(timeoutMs),
      });
    } else {
      res = await fetchImpl('https://api.openai.com/v1/images/generations', {
        method: 'POST',
        headers: { Authorization: `Bearer ${apiKey}`, 'Content-Type': 'application/json' },
        body: JSON.stringify({ model, prompt, size, quality, output_format: 'png', n: 1 }),
        signal: AbortSignal.timeout(timeoutMs),
      });
    }
  } catch (e) {
    // 通信断・時間切れ = 向こうで作られたか分からない
    throw Object.assign(new Error(String(e?.message || e)), { transient: true });
  }
  let body = null;
  try { body = await res.json(); } catch { body = null; }
  if (!res.ok) {
    throw Object.assign(new Error(String(body?.error?.message || `HTTP ${res.status}`).slice(0, 300)), {
      status: res.status, code: body?.error?.code || null, transient: res.status >= 500,
    });
  }
  const b64 = body?.data?.[0]?.b64_json;
  if (!b64) throw Object.assign(new Error('画像が返ってきませんでした'), { transient: true });
  return { buf: Buffer.from(b64, 'base64'), usage: body.usage || null };
}
