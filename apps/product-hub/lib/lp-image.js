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
import { logEvent } from '../db.js';
// 構成の本文は「効いている構成」(人が直した編集版があればそれ・画像制作の新フロー PR-B) を読む
import { composeTextOf } from './lp-edit.js';

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

/** いちばん新しい LP 構成の依頼 (draft ごと) と、その実モデル確認 */
function latestComposeJob(db, draftId) {
  return db.prepare(`SELECT j.*, g.model_check FROM ph_lp_compose_jobs j
    LEFT JOIN ph_lp_compose_generations g ON g.job_id = j.id
    WHERE j.draft_id = ? ORDER BY j.id DESC LIMIT 1`).get(draftId) || null;
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
export function requestImageJob(db, { draft, folderId, idempotencyKey, actor, refTimes = null, env = process.env, now = Date.now() } = {}) {
  const key = exact(idempotencyKey, IDEMPOTENCY_KEY_RE);
  if (!key) return { code: 'bad_request', error: 'idempotency_key の形が不正です (英数記号 8〜80 文字)' };
  const draftId = posInt(draft?.id);
  if (!draftId) return { code: 'bad_request', error: '商品の ID が不正です' };
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const prior = db.prepare('SELECT * FROM ph_lp_image_jobs WHERE draft_id = ? AND idempotency_key = ?').get(draftId, key);
    if (prior) return { ok: true, job: prior, created: false };
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
  const job = draftId ? db.prepare('SELECT * FROM ph_lp_image_jobs WHERE draft_id = ? ORDER BY id DESC LIMIT 1').get(draftId) : null;
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
    } : null,
  };
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
      const why = e?.changed ? '参考画像が依頼のあとで差し替わりました (作り直すときはもう一度押してください)'
        : '参考画像を Drive から取れませんでした: ' + String(e?.message || e).slice(0, 200);
      failBeforeCall(db, c, why);
      return;
    }
    renew(db, im.id);
    // 3) 生成 (混んでいる 429 だけ少し待って 2 回まで)
    let result = null;
    for (let attempt = 0; ; attempt++) {
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
      const name = `${trim(draft.ne_code, 60) || 'draft' + im.draft_id}_AI初稿_${im.image_job_id}_${String(im.seq).padStart(2, '0')}${im.no != null ? '_' + im.no + '枚目' : ''}.png`;
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
