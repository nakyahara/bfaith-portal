/**
 * LP 構成の確認・修正 — 画像制作の新フロー PR-B (2026-10-09・スタッフ要望 ③)。
 * 正本 = AI_reference『商品ハブ_画像制作の新フロー_設計_20261008.md』§3.1。
 *
 * 画像を作る前に、LP の各画像に載せる文字 (役割・見出し・キャッチコピー・本文) を人が確かめて直し、
 * 画像の順番の入れ替え・追加・削除をする。直した構成で AI 画像を作る。
 *
 * 守りたいこと:
 *   ① **⑦形式のテキストを正本のままにする** (設計 §3.1)。画像生成 (buildImagePlan)・lint・lp-tool はみな ⑦ を読む。
 *      直した構成も ⑦ に書き戻して保存し、新しい形式を増やさない
 *   ② **AI の生の出力 (ph_lp_compose_jobs.output_text) は消さない・書き換えない** — 段階1 の測定 (AI の構成と人の構成の
 *      くらべっこ) の材料。直した版は別の表 ph_lp_compose_edits に**追記だけ**する
 *   ③ 書き戻すのは各ブロックの `## 画像の役割` `## メイン見出し` `## サブ見出し` `## 本文` の中身だけ。
 *      構図・素材・トーン・NG はブロックのまま持ち越す (ラフも「画面で直すのは画像に載せる文字だけ」)。
 *      共通の部分 (先頭の決まり・末尾の共通NG 等) は 1 文字も触らない。`# N枚目｜名前` は並び順に振り直す
 *   ④ **サーバの lint (lp-lint.js) を通ったときだけ**保存する。lp-parser.js (配信元の写し) は直さず、
 *      ブロックの切り出しは lint の見出しの決まりで行い、パーサの読み (rawBlockText) と食い違えば直させない
 *   ⑤ 古いタブ・2 人同時で上書きしない (画面が見ていた base_job_id / base_edit_id が今と違えば断る)
 *
 * 「効いている構成」= いちばん新しい LP構成 job (status done・実モデル一致) の、いちばん新しい編集版。
 * 編集版が無ければ AI の output_text。AI に作り直させて新しい job ができたら、古い job への編集版は使わない
 * (base_job_id が違うので自然に外れる。古い構成への直しを新しい構成に混ぜない)。
 */
import { parseConstructionDoc } from './lp-parser.js';
import {
  lintComposition, IMAGE_HEADINGS, TAIL_BLOCKS, MIN_IMAGES, MAX_IMAGES,
  IMAGE_HEADING_RE, BLOCK_HEADING_RE, SUB_HEADING_RE, sameHeading,
} from './lp-lint.js';
import { logEvent } from '../db.js';

/** 画面で直す 4 項目 (画面の名前 → ⑦ の見出し)。設計 §3.1 の対応表 */
export const EDIT_FIELDS = [
  ['role', '画像の役割', '役割'],
  ['title', 'メイン見出し', '見出し'],
  ['copy', 'サブ見出し', 'キャッチコピー'],
  ['body', '本文', '本文'],
];
/** 1 項目の長さの上限 (画像に載せる文字なので、長すぎるものは入れ間違い) */
export const FIELD_MAX = { role: 60, title: 120, copy: 300, body: 1500 };
/** TOP (0枚目) と FV (1枚目) は位置固定。並べ替え・追加・削除できるのはそれ以外 (point) だけ */
export const FIXED_KINDS = ['top', 'fv'];
/** 枚数の上下限は lint に合わせる (0枚目 + 1〜9) */
export { MIN_IMAGES, MAX_IMAGES };
/** 「なし」は画面では空欄で見せる (⑦ では空の見出しを置かずに「なし」と書く決まり・fixture もそう) */
const NONE = 'なし';
const UID_RE = /^[a-z][A-Za-z0-9_-]{0,39}$/;
/** 画面が新しく足した画像の uid (n で始まる)。それ以外の知らない uid は受け付けない */
const NEW_UID_RE = /^n[A-Za-z0-9_-]{1,39}$/;
// eslint-disable-next-line no-control-regex
const CONTROL_RE = /[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/;

const normalize = (s) => String(s == null ? '' : s).replace(/\r\n?/g, '\n');
/** 画面に出す値 (前後の空白を落とし、「なし」は空欄) */
const shownValue = (key, v) => {
  const t = normalize(v).trim();
  return key !== 'role' && t === NONE ? '' : t;
};
export const slotKind = (i) => (i === 0 ? 'top' : i === 1 ? 'fv' : 'point');
/** 画面の番号 (TOP / 1枚目 / 2枚目 …) */
export const slotLabel = (i) => (i === 0 ? 'TOP' : `${i}枚目`);

/**
 * ⑦ の全文を「共通の頭 / 画像ブロック / 共通の尻」に切る。
 * 切り方は lint と同じ見出しの決まり (H1 の `# N枚目｜名前`、尻は最後の画像の後ろの `# 共通NG事項` から)。
 * そのうえで **パーサの読み (rawBlockText) と 1 枚ずつ照らす** — 食い違う構成は、書き戻すと
 * lp-tool での読まれ方が変わるおそれがあるので直させない
 * @returns {{ok:true, head:string[], tail:string[], blocks:Array<{no,name,headingLine,lines}>}|{ok:false, error}}
 */
export function splitComposition(text) {
  const src = normalize(text);
  if (!src.trim()) return { ok: false, error: '構成の本文が空です' };
  const lines = src.split('\n');
  const heads = [];
  lines.forEach((l, i) => {
    const m = l.match(IMAGE_HEADING_RE);
    if (m) heads.push({ index: i, name: m[2] });
  });
  if (!heads.length) return { ok: false, error: '構成に「# N枚目｜名前」の画像ブロックがありません' };
  const lastHead = heads[heads.length - 1].index;
  let tailAt = lines.findIndex((l, i) => {
    if (i <= lastHead) return false;
    const m = l.match(BLOCK_HEADING_RE);
    return !!m && TAIL_BLOCKS.some((t) => sameHeading(m[1], t));
  });
  if (tailAt < 0) tailAt = lines.length;
  const blocks = heads.map((h, k) => ({
    no: k,
    name: h.name,
    headingLine: lines[h.index],
    lines: lines.slice(h.index + 1, k + 1 < heads.length ? heads[k + 1].index : tailAt),
  }));
  let parsed;
  try { parsed = parseConstructionDoc(src); } catch (e) {
    return { ok: false, error: '構成をパーサが読めません: ' + String(e?.message || e).slice(0, 160) };
  }
  // 直せるのは V2.2 の形だけ (4 見出し・15 見出しの決まりは V2.2 のもの。V2.1 は lint も通らない)
  if (parsed && parsed.templateVersion !== 'V2.2') return { ok: false, error: 'テンプレート V2.2 の構成だけ画面で直せます' };
  if (!parsed || parsed.images.length !== blocks.length) {
    return { ok: false, error: `画像の枚数がパーサの読みと合いません (見出し ${blocks.length} 枚 / パーサ ${parsed ? parsed.images.length : 0} 枚)` };
  }
  for (const [k, b] of blocks.entries()) {
    if (parsed.images[k].rawBlockText !== b.lines.join('\n').trim()) {
      return { ok: false, error: `${k}枚目のブロックの切れ目がパーサの読みと合いません (画面では直せない形です)` };
    }
  }
  return { ok: true, head: lines.slice(0, heads[0].index), tail: lines.slice(tailAt), blocks };
}

/** ブロックの中の `## 見出し` (H2 ちょうど) の位置。中身 = 見出しの次の行から、次の H2 の前まで */
function sectionsOf(lines) {
  const at = [];
  lines.forEach((l, i) => {
    const m = l.match(SUB_HEADING_RE);
    if (m) at.push({ name: m[1].trim(), index: i });
  });
  return at.map((s, k) => ({ ...s, end: k + 1 < at.length ? at[k + 1].index : lines.length }));
}
const sectionValue = (lines, heading) => {
  const s = sectionsOf(lines).find((x) => sameHeading(x.name, heading));
  return s ? lines.slice(s.index + 1, s.end).join('\n') : null;
};

/** ブロックから画面の 4 項目を読む */
export function fieldsOfBlock(lines) {
  const out = {};
  for (const [key, heading] of EDIT_FIELDS) out[key] = shownValue(key, sectionValue(lines, heading));
  return out;
}

/**
 * ⑦ の全文 → 画像の並び (slots)。0枚目 = top (サムネイル)、1枚目 = fv、ほか = point。
 * uid / 要撮影はここでは付けない (effectiveCompose が付ける)
 */
export function readComposition(text) {
  const sp = splitComposition(text);
  if (!sp.ok) return sp;
  return {
    ok: true, head: sp.head, tail: sp.tail,
    slots: sp.blocks.map((b, i) => ({ kind: slotKind(i), no: i, ...b, ...fieldsOfBlock(b.lines) })),
  };
}

/** point の見出し名 (`# N枚目｜名前` の名前) を役割から作る。1 行目だけ・区切り記号は外す */
export function headingNameFromRole(role) {
  const first = normalize(role).split('\n')[0].replace(/[｜|]/g, '／').replace(/\s+/g, ' ').trim();
  return first.slice(0, 40) || '画像';
}

/** 4 項目のうち、変わった見出しの中身だけを差し替える (変わっていない見出しは 1 文字も触らない) */
export function applyFields(lines, values, current) {
  let out = lines.slice();
  for (const [key, heading] of EDIT_FIELDS) {
    if (values[key] === undefined || shownValue(key, values[key]) === shownValue(key, current[key])) continue;
    const s = sectionsOf(out).find((x) => sameHeading(x.name, heading));
    if (!s) continue;   // 15 見出しの揃わないブロックは lint が落とす
    const v = shownValue(key, values[key]);
    const body = (v === '' ? NONE : v).split('\n');
    out = [...out.slice(0, s.index + 1), ...body, '', ...out.slice(s.end)];
  }
  return out;
}

/**
 * 追加した画像の雛形 (15 見出しを埋めた形・ラフの newSlot 相当)。
 * 画像の大きさ・色・NG は共通の決まりに従わせる (この画像だけの数値は書かない)
 */
export function newSlotLines({ role = '', title = '', copy = '', body = '' } = {}) {
  const v = (key, x) => { const t = shownValue(key, x); return t === '' ? NONE : t; };
  const sec = {
    '画像の役割': v('role', role) === NONE ? '特長' : v('role', role),
    '目的': '見出し・キャッチコピー・本文の内容を 1 枚で伝える。',
    'メイン見出し': v('title', title),
    'サブ見出し': v('copy', copy),
    '本文': v('body', body),
    'バッジ・補足': NONE,
    '商品配置': '提供された実物商品画像を中央に配置する。',
    '背景・シーン': '共通生成条件の全体トーンに合わせる。',
    '装飾・演出': '最小限。',
    '使用カラー': '共通使用カラーに従う。',
    '使用素材': '提供された実物商品画像',
    '詳細レイアウト': '共通生成条件の画像サイズ。上部に見出しとキャッチコピー、中央〜下部に商品。外周に安全余白を確保。',
    '生成指示': '見出し→キャッチコピー→本文の順に読める配置にする。実物商品画像を正素材とし、商品・パッケージ・ラベル・色・形状を描き直さない。',
    'NG事項': '共通NG事項に従う。商品形状・ラベルの変更、未確認情報の追加、文字の詰め込み。',
    '生成後チェック': '共通生成後チェックに従う。見出しの誤字、商品形状、ラベル、スマホ視認性を確認。',
  };
  const out = [''];
  for (const h of IMAGE_HEADINGS) out.push(`## ${h}`, ...String(sec[h]).split('\n'), '');
  return out;
}

/** slots → ⑦ の全文。頭と尻は元のまま、ブロックは並び順につなぐ */
export function buildComposition({ head, tail, slots }) {
  return [...head, ...slots.flatMap((s) => [s.headingLine, ...s.lines]), ...tail].join('\n');
}

const errOf = (uid, no, field, message) => ({ uid, no, field, message });

/**
 * 画面から来た 1 項目の値を確かめる (変えたものだけ)。
 * 🚨 行頭の `#` は見出しと区別できなくなる (パーサは `#` 1 つから見出しとして読み、ブロックの切れ目がずれる) ので受けない
 */
function fieldError(key, v) {
  const label = EDIT_FIELDS.find((f) => f[0] === key)[2];
  if (CONTROL_RE.test(v)) return `${label}に使えない制御文字が入っています`;
  if (v.length > FIELD_MAX[key]) return `${label}は ${FIELD_MAX[key]} 文字までです (いま ${v.length} 文字)`;
  if (/^\s*#/m.test(v)) return `${label}の行の先頭に # は使えません (見出しと区別できなくなります)`;
  if (key === 'role' && v === '') return '役割を入れてください';
  return null;
}

/**
 * 画面から来た並び (submitted) を今の構成 (cur) に当てて、組み直した全文を作る。lint はまだ見ない。
 * submitted = [{uid, role, title, copy, body, shoot}, ...並び順]
 * @returns {{ok:true, text, slots, summary}|{ok:false, code:'invalid', errors:Array, error}}
 */
export function composeEdit(cur, submitted) {
  const errors = [];
  const fail = () => ({ ok: false, code: 'invalid', errors, error: errors.map((e) => (e.no != null ? slotLabel(e.no) + ': ' : '') + e.message).join(' / ') });
  if (!Array.isArray(submitted)) { errors.push(errOf(null, null, null, '画像の並び (slots) がありません')); return fail(); }
  if (submitted.length < MIN_IMAGES || submitted.length > MAX_IMAGES) {
    errors.push(errOf(null, null, null, `画像は ${MIN_IMAGES}〜${MAX_IMAGES} 枚です (TOP + 1〜${MAX_IMAGES - 1}枚目。いま ${Array.isArray(submitted) ? submitted.length : 0} 枚)`));
    return fail();
  }
  const byUid = new Map(cur.slots.map((s) => [s.uid, s]));
  const seen = new Set();
  for (const [i, x] of submitted.entries()) {
    if (!x || typeof x !== 'object') { errors.push(errOf(null, i, null, '画像の形が不正です')); continue; }
    if (typeof x.uid !== 'string' || !UID_RE.test(x.uid)) { errors.push(errOf(null, i, 'uid', '画像の印 (uid) の形が不正です')); continue; }
    if (seen.has(x.uid)) errors.push(errOf(x.uid, i, 'uid', '同じ画像が 2 回入っています'));
    seen.add(x.uid);
    for (const [key] of EDIT_FIELDS) {
      if (typeof x[key] !== 'string') errors.push(errOf(x.uid, i, key, `${key} は文字で送ってください`));
    }
    if (typeof x.shoot !== 'boolean') errors.push(errOf(x.uid, i, 'shoot', '撮影不要 / 要撮影 (shoot) は true / false で送ってください'));
  }
  if (errors.length) return fail();
  // TOP と FV は位置固定・削除不可。今の構成の TOP / FV がそのまま 0 / 1 番目にあること
  const top = cur.slots[0], fv = cur.slots[1];
  if (submitted[0].uid !== top.uid) errors.push(errOf(submitted[0].uid, 0, 'uid', 'TOP (0枚目) は位置が固定です。動かす・消すことはできません'));
  if (submitted[1].uid !== fv.uid) errors.push(errOf(submitted[1].uid, 1, 'uid', '1枚目 (FV) は位置が固定です。動かす・消すことはできません'));
  for (const [i, x] of submitted.entries()) {
    if (i < 2) continue;
    const known = byUid.get(x.uid);
    if (known && known.kind !== 'point') errors.push(errOf(x.uid, i, 'uid', `${known.kind === 'top' ? 'TOP' : '1枚目 (FV)'} は位置が固定です`));
    else if (!known && !NEW_UID_RE.test(x.uid)) errors.push(errOf(x.uid, i, 'uid', 'この画像は今の構成にありません (ほかの人が直したかもしれません。読み直してください)'));
  }
  if (errors.length) return fail();

  const slots = [];
  const summary = { moved: false, added: 0, removed: 0, textChanged: 0, shootChanged: 0 };
  for (const [i, x] of submitted.entries()) {
    const known = byUid.get(x.uid) || null;
    const values = {};
    for (const [key] of EDIT_FIELDS) values[key] = shownValue(key, x[key]);
    // 変えた項目 (と追加した画像の全部) だけ確かめる。AI が書いたまま触っていない項目は通す
    for (const [key] of EDIT_FIELDS) {
      if (known && values[key] === shownValue(key, known[key])) continue;
      const msg = fieldError(key, values[key]);
      if (msg) errors.push(errOf(x.uid, i, key, msg));
    }
    const kind = slotKind(i);
    const lines = known ? applyFields(known.lines, values, known) : newSlotLines(values);
    const roleChanged = !known || values.role !== shownValue('role', known.role);
    const name = kind === 'top' ? 'サムネイル' : kind === 'fv' ? 'FV'
      : (roleChanged ? headingNameFromRole(values.role) : known.name);
    // 番号も名前も変わらなければ見出しの行は元のまま (読む→そのまま書くで元と同じ)
    const headingLine = known && known.no === i && known.name === name ? known.headingLine : `# ${i}枚目｜${name}`;
    if (!known) summary.added += 1;
    else {
      if (EDIT_FIELDS.some(([key]) => values[key] !== shownValue(key, known[key]))) summary.textChanged += 1;
      if (known.shoot !== x.shoot) summary.shootChanged += 1;
    }
    slots.push({ uid: x.uid, kind, no: i, name, headingLine, lines, ...values, shoot: x.shoot });
  }
  if (errors.length) return fail();
  const keptOrder = slots.filter((s) => byUid.has(s.uid)).map((s) => s.uid);
  summary.removed = cur.slots.filter((s) => !seen.has(s.uid)).length;
  summary.moved = keptOrder.join('\n') !== cur.slots.filter((s) => seen.has(s.uid)).map((s) => s.uid).join('\n');
  return { ok: true, text: buildComposition({ head: cur.head, tail: cur.tail, slots }), slots, summary };
}

// ─── DB ────────────────────────────────────────────────

const posInt = (v) => {
  const n = typeof v === 'number' ? v : (/^[1-9]\d*$/.test(String(v ?? '')) ? Number(v) : NaN);
  return Number.isSafeInteger(n) && n > 0 ? n : null;
};

/** いちばん新しい「できた」LP構成 (done・実モデル一致・本文あり) */
export function latestDoneComposeJob(db, draftId) {
  const id = posInt(draftId);
  if (!id) return null;
  return db.prepare(`SELECT j.id, j.draft_id, j.status, j.output_text, j.created_at, j.completed_at, g.model_check
    FROM ph_lp_compose_jobs j JOIN ph_lp_compose_generations g ON g.job_id = j.id
    WHERE j.draft_id = ? AND j.status = 'done' AND g.model_check = 'match'
      AND j.output_text IS NOT NULL AND TRIM(j.output_text) <> ''
    ORDER BY j.id DESC LIMIT 1`).get(id) || null;
}

/** その構成 (job) への、いちばん新しい編集版 */
export function latestEditFor(db, jobId) {
  const id = posInt(jobId);
  return id ? db.prepare('SELECT * FROM ph_lp_compose_edits WHERE base_job_id = ? ORDER BY id DESC LIMIT 1').get(id) || null : null;
}

/**
 * その構成 (job) の効いている本文 = いちばん新しい編集版、無ければ AI の output_text。
 * lp-image が「いちばん新しい依頼」を自分の条件で選んだあと、本文を読むところで使う
 */
export function composeTextOf(db, job) {
  if (!job) return '';
  const e = latestEditFor(db, job.id);
  return e ? e.output_text : (job.output_text || '');
}

/**
 * 効いている構成 (どの job・どの編集版か・本文)。構成がまだ無ければ null
 * @returns {{job_id, edit_id, text, source:'ai'|'edit', edited_by, edited_at}|null}
 */
export function effectiveComposeText(db, draftId) {
  const job = latestDoneComposeJob(db, draftId);
  if (!job) return null;
  const edit = latestEditFor(db, job.id);
  return {
    job_id: job.id, edit_id: edit ? edit.id : null,
    text: edit ? edit.output_text : job.output_text,
    source: edit ? 'edit' : 'ai',
    edited_by: edit ? edit.edited_by : null, edited_at: edit ? edit.created_at : null,
  };
}

const safeJson = (s) => { try { return JSON.parse(s); } catch { return null; } };

/**
 * 効いている構成を画像の並びにして返す (uid と要撮影つき)。
 * uid: AI の構成は a0, a1 …、編集版は保存したときの uid (slots_json)。要撮影も slots_json から
 * @returns {null|{job, edit, text, error?, head?, tail?, slots?}}
 */
export function effectiveCompose(db, draftId) {
  const job = latestDoneComposeJob(db, draftId);
  if (!job) return null;
  const edit = latestEditFor(db, job.id);
  const text = edit ? edit.output_text : job.output_text;
  const r = readComposition(text);
  if (!r.ok) return { job, edit, text, error: r.error };
  const meta = edit ? safeJson(edit.slots_json) : null;
  const metaOk = Array.isArray(meta) && meta.length === r.slots.length
    && meta.every((m, i) => m && UID_RE.test(String(m.uid || '')) && m.kind === r.slots[i].kind)
    && new Set(meta.map((m) => m.uid)).size === meta.length;
  const slots = r.slots.map((s, i) => ({
    ...s,
    uid: metaOk ? meta[i].uid : (edit ? `e${edit.id}x${i}` : `a${i}`),
    shoot: metaOk ? meta[i].shoot === true : false,
  }));
  return { job, edit, text, head: r.head, tail: r.tail, slots };
}

/**
 * 画像を作ったあとで構成が変わったか (「生成後に構成が変わりました」の印)。
 * いちばん新しい画像の依頼が、どの構成のどの版で作られたかを時刻で割り出して、いまの本文と比べる
 * (ph_lp_image_jobs は画像生成 (PR-E) の持ち場なので列は足さない)
 */
export function imageStaleFor(db, draftId, eff) {
  if (!eff || !eff.job) return false;
  const img = db.prepare('SELECT compose_job_id, created_at FROM ph_lp_image_jobs WHERE draft_id = ? ORDER BY id DESC LIMIT 1').get(posInt(draftId));
  if (!img) return false;
  if (img.compose_job_id !== eff.job.id) return true;
  const then = db.prepare(`SELECT output_text FROM ph_lp_compose_edits WHERE base_job_id = ? AND created_at <= ?
    ORDER BY id DESC LIMIT 1`).get(eff.job.id, img.created_at);
  return (then ? then.output_text : eff.job.output_text) !== eff.text;
}

/** 画面 (GET と保存の応答・詳細画面の最初の表示) に出す状態 */
export function editStateFor(db, draft, { canEdit = false } = {}) {
  const base = { ok: true, can_edit: !!canEdit, max_images: MAX_IMAGES, min_images: MIN_IMAGES };
  const eff = effectiveCompose(db, draft?.id);
  if (!eff) return { ...base, available: false, reason: 'LP構成がまだできていません (「🤖 構成をAIに作らせる」で作ると、ここで直せます)' };
  const common = {
    base_job_id: eff.job.id, base_edit_id: eff.edit ? eff.edit.id : null,
    edited_by: eff.edit ? eff.edit.edited_by : null, edited_at: eff.edit ? eff.edit.created_at : null,
    image_stale: imageStaleFor(db, draft.id, eff),
  };
  if (eff.error) return { ...base, ...common, available: false, reason: 'この構成は画面では直せません: ' + eff.error };
  return {
    ...base, ...common, available: true,
    slots: eff.slots.map((s) => ({ uid: s.uid, kind: s.kind, no: s.no, name: s.name, role: s.role, title: s.title, copy: s.copy, body: s.body, shoot: s.shoot })),
  };
}

/**
 * 保存する。**サーバの lint を通ったときだけ**。画面が見ていた版 (base_job_id / base_edit_id) が今と違えば conflict
 * @returns {{ok:true, changed:boolean, edit_id}|{ok:false, code:'not_ready'|'conflict'|'invalid'|'lint', error, errors?}}
 */
export function saveEdit(db, { draft, baseJobId, baseEditId, slots, actor, now = Date.now() } = {}) {
  const draftId = posInt(draft?.id);
  if (!draftId) return { ok: false, code: 'invalid', error: '商品の ID が不正です' };
  const wantJob = posInt(baseJobId);
  const wantEdit = baseEditId === null || baseEditId === undefined ? null : posInt(baseEditId);
  if (!wantJob || (baseEditId != null && !wantEdit)) return { ok: false, code: 'invalid', error: 'base_job_id / base_edit_id の形が不正です' };
  return db.transaction(() => {
    const cur = effectiveCompose(db, draftId);
    if (!cur) return { ok: false, code: 'not_ready', error: 'LP構成がまだできていません' };
    const curEdit = cur.edit ? cur.edit.id : null;
    // 🚨 古いタブ・2 人同時: 画面が見ていた版から変わっていたら上書きしない
    if (cur.job.id !== wantJob || curEdit !== wantEdit) {
      return {
        ok: false, code: 'conflict',
        error: cur.job.id !== wantJob
          ? 'LP構成が新しく作り直されています。画面を読み直してから直してください (いまの直しは新しい構成には入りません)'
          : 'ほかの人 (か別のタブ) が先に LP構成を保存しています。画面を読み直してから直してください',
      };
    }
    if (cur.error) return { ok: false, code: 'not_ready', error: 'この構成は画面では直せません: ' + cur.error };
    const r = composeEdit(cur, slots);
    if (!r.ok) return r;
    // 🚨 lint はサーバが正本。商品名 (検査 17) は見ない — 頭 (## 商品) は画面で触れず、AI の構成を受け取ったときに
    //    見てある。後から商品名を直した商品で、文字を直しただけの保存が断られないように
    const lint = lintComposition(r.text, { productName: null });
    if (!lint.ok) {
      return {
        ok: false, code: 'lint', errors: lint.errors.map((e) => ({ uid: null, no: null, field: null, message: String(e.detail || e.label).slice(0, 300) })),
        error: 'LP構成のきまり (lint) に合いません: ' + lint.errors.map((e) => String(e.detail || e.label).slice(0, 160)).join(' / '),
      };
    }
    // 書き戻した文を読み直して、画面の値と同じに読めること (パーサの読みとの食い違いも splitComposition が見る)
    const back = readComposition(r.text);
    const same = back.ok && back.slots.length === r.slots.length && back.slots.every((b, i) =>
      EDIT_FIELDS.every(([key]) => b[key] === r.slots[i][key]));
    if (!same) {
      return { ok: false, code: 'invalid', error: '書き戻した構成を読み直すと、画面の値と食い違います (入れた文字の形を見直してください)' + (back.ok ? '' : ': ' + back.error) };
    }
    const shootSame = r.slots.length === cur.slots.length && r.slots.every((s, i) => s.uid === cur.slots[i].uid && s.shoot === cur.slots[i].shoot);
    if (r.text === cur.text && shootSame) return { ok: true, changed: false, edit_id: curEdit };
    const meta = r.slots.map((s) => ({
      uid: s.uid, kind: s.kind, name: s.name, role: s.role, title: s.title, copy: s.copy, body: s.body, shoot: s.shoot,
      // 元のブロック (見出しの行と中身)。撮影指示書 (PR-D) が構図・素材を読む
      block: [s.headingLine, ...s.lines].join('\n').trim(),
    }));
    const who = String(actor || 'unknown').slice(0, 120);
    const id = Number(db.prepare(`INSERT INTO ph_lp_compose_edits (draft_id, base_job_id, slots_json, output_text, edited_by, created_at)
      VALUES (?, ?, ?, ?, ?, ?)`).run(draftId, cur.job.id, JSON.stringify(meta), r.text, who, new Date(now).toISOString()).lastInsertRowid);
    const sm = r.summary;
    const parts = [];
    if (sm.moved) parts.push('並べ替え');
    if (sm.added) parts.push(`追加 ${sm.added}`);
    if (sm.removed) parts.push(`削除 ${sm.removed}`);
    if (sm.textChanged) parts.push(`文字 ${sm.textChanged} 枚`);
    if (sm.shootChanged) parts.push(`撮影の要否 ${sm.shootChanged} 枚`);
    logEvent(db, draftId, 'lp_compose_edited',
      `LP構成を保存 (${r.slots.length}枚・構成 ${cur.job.id}・編集版 ${id})${parts.length ? ': ' + parts.join(' / ') : ''}`, who);
    return { ok: true, changed: true, edit_id: id };
  }).immediate();
}
