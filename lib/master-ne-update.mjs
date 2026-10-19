/**
 * master-ne-update.mjs — 画面「NEへ送る」(AI_reference CompanyDB構想/25・2026-10-19 の方針・migration 0094) の読む / 頼む / 受け取る
 *
 * 中原さん 10/19: 「シンプルに自由に編集できて、取り込み時に照合して差分がある部分を CSV で 1 日 1 回アップ・都度アップも」「NE にあるないとか CSV をどうたらこうたらとかめっちゃわかりにくい」
 *   → 画面は「自社で直したけれど、まだ NE と違う所」の一覧 (商品・欄・NE の今の値 → 自社の値) とボタン 1 つだけ。ファイル番号・状態の名前・鍵・予約の言葉は出さない。
 *     止めている欄だけ、ふつうの言葉で 1 行 (WHY_WORDS)。送った欄は翌朝の照合で NE と同じになれば一覧から消える
 * ボタン: いつも「CSV を作る (NE にアップする用)」(その場で作る = ops.ne_upd_make_download → ops.ne_upd_download で 1 つずつ受け取る・渡す前に DB が今の差と照らす) =
 *   いちばん使う形 (中原さんが NE の画面で手でアップ = API の回数に入らない)。miniPC の API の口が開いている (worker.upload_ready) ときだけ「今すぐ NE へ送る」も (頼みを積む)
 * 🚨 差を決めるのも CSV を作るのも DB の関数 (アプリから値・列名・バイト列を渡さない)。ここは形を確かめて関数を呼び、言葉にするだけ
 * 🚨 関数が無い (0094 の前)・実行権が無い (ロールの流し直しの前) = 画面は「まだ使えません」・商品の画面の 1 行は出さない
 */
import crypto from 'node:crypto';
import { MasterWriteError } from './master-write.mjs';

export const OVERVIEW_FN = 'ops.ne_upd_overview()';
export const SKU_FN = 'ops.ne_upd_sku(bigint)';
/** 画面の欄の名前 (種類:欄) */
export const COL_WORDS = Object.freeze({
  'products:name': '名前', 'products:handling': '取扱', 'products:tax_rate': '税率', 'products:standard_price_jpy': '売価', 'products:cost': '原価',
  'products:primary_supplier': '代表の仕入先', 'sets:name': 'セットの名前', 'sets:standard_price_jpy': 'セットの売価',
});
/** 止めている欄のふつうの言葉 (1 行) */
export const WHY_WORDS = Object.freeze({
  not_open: 'いまは送れません (Company DB の保存がまだ開いていません)',
  ne_not_today: '今朝の NE の値がまだ届いていません (朝の照合が終わると送れます)',
  decided: 'マスタの判断で「NE と違ったままでよい」などに決めてあるので送りません',
  decision_other_value: 'マスタの判断で、NE に別の値を入れると決めてあります (マスタの判断で決め直してください)',
  new_product: '新商品の登録の途中です (NE 登録の CSV で送ります)',
  deleting: 'NE から消している途中なので送りません',
  handling_csv: 'NE で取扱中止にした後なので、取扱は送りません (NE の画面で確かめてください)',
  ne_code_unknown: '今朝の NE にこの商品コードが見つかりません (NE の画面で確かめてください)',
  ne_collided: 'NE に大文字・小文字だけ違う商品コードが 2 つあります (NE の画面で 1 つにしてください)',
  case_unverified: 'NE の商品コードの書き方が 1 つに決まりません (大文字・小文字の直しは NE での確かめが終わってから送ります。急ぐときは NE の画面で直してください)',
  lz_many: 'ロジザードに大文字・小文字だけ違う商品 ID が 2 つあります (ロジザードでそろえてください)',
  lz_old: 'ロジザードの商品の一覧が古いので待っています (明日の朝に送れます)',
  name_blank: '名前が空なので送りません', empty_word: '名前が「empty」なので送りません (NE では「消す」の意味)', name_control: '名前に改行などの見えない文字があるので送りません',
  name_astral: '名前に絵文字などがあるので送りません (NE の画面で)', name_length: '名前が 255 字を超えるので送りません', name_is_code: '名前が商品コードと同じなので送りません',
  handling_value: '取扱の値が「取扱中 / 取扱中止」でないので送りません', tax_value: '税率が 10% / 8% でないので送りません',
  yen_range: '金額が空・0・小数なので送りません (NE の画面で)', supplier_format: '代表の仕入先が 1 つの 4 桁のコードでないので送りません', no_value: '値が空なので送りません',
});
export const SENDING_WORDS = 'この欄は送っている途中です (CSV を NE に取り込めば、明日の朝に一覧から消えます)';
export const STUCK_WORDS = '2 回送っても NE が変わりませんでした。NE の画面で直してください';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const jsonOf = (v) => (typeof v === 'string' ? JSON.parse(v) : v);
const yen = (v) => `${Number(v).toLocaleString('ja-JP')} 円`;

/** 関数があって、この接続のロールが実行できるか ('ok' / 'not_applied' / 'no_privilege') */
export async function neUpdReady(db, fn = OVERVIEW_FN) {
  if (!db) return 'not_applied';
  return (await db.query(`select case when to_regprocedure($1::text) is null then 'not_applied'
    when has_function_privilege(to_regprocedure($1::text)::oid, 'execute') then 'ok' else 'no_privilege' end as s`, [fn])).rows[0].s;
}

/** 欄の値を人の言葉に (照合の D2 の正規化の値) */
export function valueText(col, v) {
  if (v == null) return '空';
  switch (col) {
    case 'handling': return v === 'active' ? '取扱中' : v === 'discontinued' ? '取扱中止' : '不明';
    case 'tax_rate': return Number(v) === 0.1 ? '10%' : Number(v) === 0.08 ? '8%' : `${Number(v) * 100}%`;
    case 'standard_price_jpy': case 'cost': return yen(v);
    case 'primary_supplier': return Array.isArray(v) ? (v.length ? v.join('・') : '空') : String(v);
    default: return String(v);
  }
}

/** 一覧の 1 行を画面の形に (状態の名前は出さない = note だけ) */
export function rowOf(u) {
  const spelling = u.role === 'spelling';
  const what = spelling ? 'コードの大文字・小文字' : (COL_WORDS[`${u.kind}:${u.col}`] || u.col);
  const from = spelling ? (u.ne_spelling || '?') : valueText(u.col, u.ne_value);
  const to = spelling ? (u.ne_code || u.lz_spelling || '?') : valueText(u.col, u.cdb_value);
  const note = u.status === 'sending' ? SENDING_WORDS : u.status === 'stuck' ? STUCK_WORDS : u.status === 'blocked' ? (WHY_WORDS[u.why] || 'いまは送れません') : '';
  const codeFix = !spelling && u.spelling_fix && u.ne_spelling && u.ne_code ? `コードの書き方も ${u.ne_code} にそろえます` : '';
  return { sku_id: u.sku_id, code: u.code, name: u.name, what, from, to, note, codeFix, waiting: u.status === 'send', changed_by: u.changed_by || null, changed_at: u.changed_at || null };
}

/** 画面の一覧。戻り値 = null (読めない) / { ready: false, why } / { ready: true, rows, sendable, neAt, uploadReady, autoOn, problems } */
export async function readNeUpdate(db) {
  if (!db) return null;
  try {
    const r0 = await neUpdReady(db);
    if (r0 !== 'ok') return { ready: false, why: r0 === 'not_applied' ? 'Company DB にまだこの仕組みが入っていません (migration 0094 の前)' : 'Company DB の権限の流し直しの前です' };
    const o = jsonOf((await db.query(`select ops.ne_upd_overview() as r`)).rows[0].r);
    const rows = (o.units || []).map(rowOf);
    return {
      ready: true, ok: !!o.ok, rows, sendable: rows.filter((r) => r.waiting).length,
      problems: (o.problems || []).map((p) => p.text), neAt: o.ne ? o.ne.products_started_at : null,
      uploadReady: !!(o.worker && o.worker.upload_ready), autoOn: !!(o.worker && o.worker.auto_on), waitingNow: !!o.waiting,
      lastSends: (o.sends || []).filter((s) => s.how !== 'download').slice(0, 3).map((s) => ({ at: s.built_at || s.requested_at, how: s.how, state: s.state, rows: s.rows, reason: s.reason })),
      // 今日作った CSV (人が受け取るもの・新しい順・5 つまで) = 受け取りに失敗した・閉じてしまったときにもう一度ダウンロードできる (番号は画面に出さない・渡す前に DB が今の差と照らす)
      todayFiles: (o.sends || []).flatMap((s) => (s.files || []).filter((f) => f.downloadable).map((f) => ({ file_id: String(f.file_id), kind: f.kind, line_count: f.line_count,
        at: s.built_at || s.requested_at, received: !!f.downloaded_at }))).filter((f, i, all) => !f.received || all.slice(0, i).filter((x) => x.received).length < 5),   // まだ受け取っていないものは全部・受け取ったものは新しい 5 つ
    };
  } catch (e) {
    console.error(`[master-ne-update] 一覧を読めない: ${String((e && e.message) || e).slice(0, 200)}`);
    return { ready: false, why: '一覧を読めませんでした (少し待って開き直してください)' };
  }
}

/** 商品の画面の 1 行の材料 = その SKU の NE と違う欄の数 (関数が無い・読めない = null = 出さない) */
export async function readSkuNeDiff(db, skuId) {
  if (!db || !/^\d{1,19}$/.test(String(skuId ?? ''))) return null;
  try {
    if ((await neUpdReady(db, SKU_FN)) !== 'ok') return null;
    const r = jsonOf((await db.query(`select ops.ne_upd_sku($1::bigint) as r`, [String(skuId)])).rows[0].r);
    const units = (r.units || []).filter((u) => u.role === 'change');
    return { count: units.length, sending: units.filter((u) => u.status === 'sending').length, blocked: units.filter((u) => u.status === 'blocked' || u.status === 'stuck').length };
  } catch (e) {
    console.error(`[master-ne-update] 商品の差を読めない: ${String((e && e.message) || e).slice(0, 200)}`);
    return null;
  }
}

const bad = (message, reason = 'invalid_input') => new MasterWriteError(400, reason, message);
const actorOf = (a) => {
  const s = String(a ?? '').trim().toLowerCase();
  if (!s || s.length > 320 || /[\x00-\x1f\x7f]/.test(s)) throw bad('人 (ログインのメール) が無い');
  return s;
};
const ridOf = (x) => {
  const s = String(x ?? '').trim().toLowerCase();
  if (!UUID_RE.test(s)) throw bad('押した番号 (request_id) の形が違う。画面を開き直してください');
  return s;
};
/** DB の関数の誤り (「理由: 文」) → 画面の言葉 */
function fnError(e) {
  if (e instanceof MasterWriteError) return e;
  const m = /^([a-z_]+): ([\s\S]*)$/.exec(String((e && e.message) || ''));
  if (m && e.code && e.code !== 'XX000') {
    const status = { invalid_input: 400, not_found: 404, request_id_reused: 409, file_old: 409, not_download: 409, file_stale: 409 }[m[1]] || 409;
    const words = { file_old: '前の日に作ったファイルは渡しません。もう一度「CSV を作る」を押してください', request_id_reused: 'もう一度画面を開き直してください',
      not_download: 'このファイルは渡せません。もう一度「CSV を作る」を押してください' }[m[1]] || m[2];
    return new MasterWriteError(status, m[1], words);
  }
  return e;
}
async function ready(db, fn) {
  const s = await neUpdReady(db, fn);
  if (s !== 'ok') throw new MasterWriteError(409, 'not_applied', s === 'not_applied' ? 'Company DB にまだこの仕組みが入っていません' : 'Company DB の権限の流し直しの前です');
}

/** 「今すぐ NE へ送る」= 頼みを積む (miniPC の送り手が数分の中に拾って作って上げる) */
export async function requestNeSendNow(db, { actor, requestId }) {
  const a = actorOf(actor);
  const rid = ridOf(requestId);
  await ready(db, 'ops.ne_upd_request_now(uuid, text)');
  try { return jsonOf((await db.query('select ops.ne_upd_request_now($1::uuid, $2) as r', [rid, a])).rows[0].r); } catch (e) { throw fnError(e); }
}

/** 「CSV を作る (NE にアップする用)」= その場で作る。戻り値 = { state, files: [{ file_id, kind, line_count }] } (バイト列は downloadNeUpdateFile で) */
export async function makeNeUpdateCsv(db, { actor, requestId }) {
  const a = actorOf(actor);
  const rid = ridOf(requestId);
  await ready(db, 'ops.ne_upd_make_download(uuid, text)');
  let r;
  try { r = jsonOf((await db.query('select ops.ne_upd_make_download($1::uuid, $2) as r', [rid, a])).rows[0].r); } catch (e) { throw fnError(e); }
  return { state: r.state, reason: r.reason || null, left: r.left || 0, files: (r.files || []).map((f) => ({ file_id: String(f.file_id), kind: f.kind, line_count: f.line_count })) };
}

/** 受け取り (作った日だけ)。sha256 を確かめてから渡す */
export async function downloadNeUpdateFile(db, { actor, fileId }) {
  const a = actorOf(actor);
  if (!/^\d{1,19}$/.test(String(fileId ?? ''))) throw bad('ファイルの番号の形が違う');
  await ready(db, 'ops.ne_upd_download(text, bigint)');
  let r;
  try { r = jsonOf((await db.query('select ops.ne_upd_download($1, $2::bigint) as r', [a, String(fileId)])).rows[0].r); } catch (e) { throw fnError(e); }
  const bytes = Buffer.from(String(r.bytes_b64 || ''), 'base64');
  if (crypto.createHash('sha256').update(bytes).digest('hex') !== r.sha256) throw new MasterWriteError(500, 'sha_mismatch', 'ファイルの中身が記録と違うので渡しません');
  return { file_name: r.file_name, sha256: r.sha256, bytes };
}

/** NE の実物の試しの結果 (名簿 MASTER_DECISION_APPROVERS の人だけ・設計 25 §2.9) */
export const VERIFY_TARGETS = Object.freeze([...Object.keys(COL_WORDS).map((k) => k.split(':')), ['products', 'code_case']]);
export async function recordNeUpdateVerified(db, { actor, kind, col, entry = 'bulk', result, sha256 = null, note = null }) {
  const a = actorOf(actor);
  if (!VERIFY_TARGETS.some(([k, c]) => k === kind && c === col)) throw bad('種類・欄が違う');
  if (!['bulk', 'api'].includes(entry)) throw bad('入口は bulk / api');
  if (!['ok', 'ng'].includes(result)) throw bad('結果は ok / ng');
  if (sha256 != null && sha256 !== '' && !/^[0-9a-f]{64}$/.test(String(sha256))) throw bad('試しのファイルの sha256 の形が違う');
  if (note != null && (typeof note !== 'string' || note.length > 1000)) throw bad('メモは 1,000 字まで');
  await ready(db, 'ops.ne_upd_record_verified(text, text, text, text, text, text, text)');
  try {
    return jsonOf((await db.query('select ops.ne_upd_record_verified($1, $2, $3, $4, $5, $6, $7) as r', [a, kind, col, entry, result, sha256 || null, note || null])).rows[0].r);
  } catch (e) { throw fnError(e); }
}
