/**
 * logizard-barcode.mjs — ロジザードに送るバーコード (新商品をロジザードに初めて載せるときの 1 本) の「値」
 *   (設計 = AI_reference CompanyDB構想/23 v4・中原さん 10/13 夜「FNSKU ってどこに入力するの? 新商品登録の画面に作ってくれよ。
 *    ここで入れてすぐにロジザードと NE に登録するからさ」「逆に Amazon に出品するにはいらない」)
 *
 * 🚨 バーコードは 4 つを混ぜない (23 §2): ① 識別子 (JAN・FNSKU = core.external_ids) ② AMC 商品ラベルの印字 (発注アプリ) ③ 入庫の BC シール (入荷アプリ)
 *    ④ ロジザードに送るバーコード (= ここ)。④ は人が登録のときに打つ値 (手元の FNSKU か JAN) か「空欄」(ロジザードが番号を付ける)。
 *    **Amazon の出品・Amazon SKU の対応とは切り離す** = Amazon のデータが無くても入れる・保存する・CSV に出す。
 *    Company DB の Amazon の FNSKU・JAN と違う = 参考の印だけ (断らない)。ほかの商品の値と同じ = 断る
 * 置き場 = ops.sku_logizard_barcode_rules (0078 + 🆕 0080 の value・追記だけ・SKU ごとに一番新しい行が今の値)。
 *   書くのは security definer の関数 ops.set_sku_logizard_barcode (master_edit だけ)
 *   0078 の「決め方」の行 (auto・値の無い fnsku / jan) = 値が無い = もう一度入れる (legacy)
 */
import { COMPANY_ID, MasterWriteError, janValid } from './master-write.mjs';

/** 種類 → 画面の言葉 */
export const LZ_KIND_WORDS = Object.freeze({ fnsku: 'FNSKU', jan: 'JAN', lz_assign: '空欄 (ロジザードが番号を付ける)' });
export const LZ_BARCODE_VIA = Object.freeze({ register: '新商品の登録', sku_page: '商品の画面' });
/** 打つ FNSKU の形 = X00 + 英数字 7 字 (10 文字・大文字にそろえる) */
export const FNSKU_RE = /^X00[0-9A-Z]{7}$/;
/** Company DB の Amazon の FNSKU (参考の照らし合わせ) の形 = Amazon のラベル (X0 + 英数字 8 字)。B0… (ASIN の形) は数えない */
const AMZ_FNSKU_RE = /^X0[0-9A-Z]{8}$/;
const JAN_RE = /^(\d{8}|\d{13})$/;
/** 送ってよい JAN (8 か 13 桁・チェック数字が合う) */
export const lzJanOk = (v) => JAN_RE.test(String(v)) && janValid(String(v));
/** 読むときの形 (前後の空白を外し・全角を半角に・英字を大文字に) */
export const normLzValue = (v) => String(v ?? '').normalize('NFKC').trim().toUpperCase();
/** CSV のバーコードの列に書いてよい値か (FNSKU か JAN の形・空欄は '') */
export const lzBarcodeValueOk = (v) => v === '' || FNSKU_RE.test(v) || lzJanOk(v);
/** 🆕 #1719 Codex R1 High 2: ロジザードのバーコードマスタの写しと照らせたか (lz_copy_state) → 画面の言葉 */
export const LZ_COPY_STATE_WORDS = Object.freeze({
  checked: 'ロジザードの写しと照らした', unavailable: 'ロジザードの写しを読めない', empty: 'ロジザードの写しが空', stale: 'ロジザードの写しが古い',
});
/**
 * 🆕 #1719 Codex R2 High (人の手順で防ぐ): 照らせていない値に出す手順。商品の画面・登録の結果・つかいかたで同じ文
 *   (ロジザードへの登録は今は GAS の H 列 = 入れる人がロジザードの商品マスタで確かめる)
 */
export const LZ_UNCHECKED_ASK = 'ロジザードの写しと照らせていません。GAS の H 列に入れる前に、ロジザードの商品マスタで同じバーコードが無いか確かめてください';
/** 照らせていない印の一言 (照らせた・値が無い = null)。asOf = 写しの時刻 (日本時間の文字・無い = なし) */
export function lzCopyNote(state, asOf) {
  if (!state || state === 'checked') return null;
  return `${LZ_UNCHECKED_ASK} (${LZ_COPY_STATE_WORDS[state] || state}・写しの時刻 ${asOf || 'なし'})`;
}
/** CSV の行を作らない理由 (lz-csv.mjs はこれ以外を断る) */
export const LZ_BARCODE_HOLDS = Object.freeze(['lz_barcode_missing']);

const bad = (message, field = 'logizard_barcode', extra = {}) => new MasterWriteError(400, 'invalid_input', message, { field, ...extra });

/** 打った値 → { kind: fnsku | jan, value }。形が違う = 何が違うかの一言 (problem) */
export function lzValueShape(raw) {
  const v = normLzValue(raw);
  if (!v) return { problem: '値がありません' };
  if (FNSKU_RE.test(v)) return { kind: 'fnsku', value: v };
  if (/^\d+$/.test(v)) {
    if (v.length !== 8 && v.length !== 13) return { problem: `JAN は 8 けたか 13 けたです (いま ${v.length} けた)` };
    if (!janValid(v)) return { problem: 'JAN の最後の数字 (チェック数字) が合いません。打ちまちがいかも' };
    return { kind: 'jan', value: v };
  }
  if (/^X0/.test(v)) return { problem: 'FNSKU は X00 + 英数字 7 字の 10 文字です' };
  return { problem: 'FNSKU (X00 で始まる 10 文字) か JAN (8 / 13 けたの数字) を入れてください' };
}

/**
 * 画面から来たバーコード (登録・商品の画面)。{ kind: 'value', value } / { kind: 'lz_assign' } → { kind: fnsku | jan | lz_assign, value | null }。
 * 送らない (undefined / null) = null (呼び手が「送っていない」として扱う)。形が違う = 400
 */
export function lzBarcodeIn(v, { field = 'logizard_barcode', label = '' } = {}) {
  if (v === undefined || v === null) return null;
  const at = label ? `${label}: ` : '';
  if (typeof v !== 'object' || Array.isArray(v)) throw bad(`${at}ロジザードのバーコードの入れ方が違う。画面を開き直してください`, field);
  if (v.kind === 'lz_assign') {
    if (v.value != null && v.value !== '') throw bad(`${at}「空欄 (ロジザードが番号を付ける)」と値の両方があります。どちらか一方にしてください`, field);
    return { kind: 'lz_assign', value: null };
  }
  if (v.kind !== 'value') throw bad(`${at}ロジザードのバーコードの入れ方が違う。画面を開き直してください`, field);
  const s = lzValueShape(v.value);
  if (s.problem) throw bad(`${at}ロジザードのバーコード: ${s.problem}`, field);
  return { kind: s.kind, value: s.value };
}

/** 🆕 #1719 R1 High 2: ロジザードの写しと照らした結果 (router = lib/lz-mirror.mjs lzCopyCheck)。形が違う = 読めなかった扱い (null = 呼び手が渡さない) */
export function lzCopyIn(v) {
  if (v == null || typeof v !== 'object') return null;
  if (!['checked', 'unavailable', 'empty', 'stale'].includes(v.state)) return null;
  const asOf = typeof v.as_of === 'string' && !Number.isNaN(Date.parse(v.as_of)) ? v.as_of : null;
  return { state: v.state, as_of: asOf };
}
/** 0080 (値の列と書く関数) があるか = 名前で確かめる (例外で判断しない) */
//   🆕 #1719 Codex R4 Medium (当てる順 = アプリ → 0080 → ロールの流し直し): 0080 の後・ロールの流し直しの前は master_edit に実行権が無い =
//   欄を出さない・値を求めない (登録は今までどおり続く)。master_edit のロールが無い DB も出さない
export async function lzBarcodeReady(db) {
  return (await db.query(`select coalesce((select pg_catalog.has_function_privilege(r.oid, p.fn, 'EXECUTE') from pg_catalog.pg_roles r
      where r.rolname = 'master_edit'), false) as ok
    from (select to_regprocedure('ops.set_sku_logizard_barcode(uuid, text, bigint, text, text, text, bigint, text, timestamp with time zone)') as fn) p
    where p.fn is not null`)).rows[0]?.ok === true;
}

/** 参考の FNSKU (Amazon 日本・この SKU 1 個だけの出品・消えていない)。Amazon との照らし合わせは参考だけ (断らない) */
const AMZ_FNSKU_SQL = `
  select lc.sku_id::text as sku_id, e.external_value as value, l.listing_code from core.listing_components lc
    join core.listings l on l.company_id = lc.company_id and l.listing_id = lc.listing_id and l.mall = 'amazon' and l.status is distinct from 'deleted'
    join core.external_ids e on e.company_id = l.company_id and e.entity_type = 'listing' and e.entity_id = l.listing_id
                            and e.system = 'amazon' and e.id_kind = 'fnsku' and e.valid_to is null
   where lc.company_id = $1 and lc.sku_id = any($2::bigint[]) and lc.qty = 1
     and (select count(*) from core.listing_components x where x.company_id = l.company_id and x.listing_id = l.listing_id) = 1
   order by lc.sku_id, e.external_value`;
const JAN_SQL = `
  select s.sku_id::text as sku_id, e.external_value as value from core.skus s
    join core.external_ids e on e.company_id = s.company_id and e.entity_type = 'product' and e.entity_id = s.product_id
                            and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null
   where s.company_id = $1 and s.sku_id = any($2::bigint[]) order by s.sku_id, e.external_value`;

/** 参考の印 (違えば一言・断らない) */
export function lzReferenceNotes(cur, ref) {
  const notes = [];
  if (!cur || !cur.value) return notes;
  const fn = (ref.fnskus || []).map((x) => normLzValue(x.value)).filter((x) => AMZ_FNSKU_RE.test(x));
  const jn = (ref.jans || []).map((x) => normLzValue(x.value));
  if (cur.kind === 'fnsku' && fn.length && !fn.includes(cur.value)) notes.push(`参考: Amazon 日本の出品の FNSKU (${fn.join('・')}) と違います (ロジザードには打った値を送ります)`);
  if (cur.kind === 'jan' && jn.length && !jn.includes(cur.value)) notes.push(`参考: この商品の JAN (${jn.join('・')}) と違います (ロジザードには打った値を送ります)`);
  return notes;
}

/**
 * SKU ごとの今のロジザードのバーコード (読むだけ)。skuIds = sku_id の配列 (文字)。ready = 0080 があるか (null = ここで確かめる)
 * @returns {Map<string, { kind, value, rule_row, via, actor_id, created_at, legacy, issued: { export_id }|null, ref: { fnskus, jans }, notes }>}
 *   kind = null = まだ入れていない (行が無い・0078 の値の無い行 = legacy)
 */
export async function readLogizardBarcodes(db, skuIds, { ready = null } = {}) {
  const ids = [...new Set((skuIds || []).map(String))];
  const out = new Map();
  if (!ids.length) return out;
  const has = ready == null ? await lzBarcodeReady(db) : ready;
  const rows = new Map(), issued = new Map();
  if (has) {
    // 🚨 並びは表の列で (出す名前の rule_row は文字 = '10' < '9' になる)
    for (const r of (await db.query(`select distinct on (x.sku_id) x.sku_id::text as sku_id, x.rule_row::text as rule_row, x.rule, x.value, x.via, x.actor_id, x.created_at::text as created_at,
        x.lz_copy_state, to_char(x.lz_copy_as_of at time zone 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI') as lz_copy_as_of
        from ops.sku_logizard_barcode_rules x where x.company_id = $1 and x.sku_id = any($2::bigint[]) order by x.sku_id, x.rule_row desc`, [COMPANY_ID, ids])).rows) rows.set(r.sku_id, r);
    // ロジザード登録の CSV に出した記録 (2 本目の帳面・表が無い DB = 出していない)
    if ((await db.query(`select to_regclass('ops.lz_new_export_items') is not null as ok`)).rows[0].ok) {
      for (const r of (await db.query(`select sku_id::text as sku_id, export_id::text as export_id from ops.lz_new_export_items where state = 'issued' and sku_id = any($1::bigint[])`, [ids])).rows) issued.set(r.sku_id, { export_id: r.export_id });
    }
  }
  const fn = new Map(), jan = new Map();
  for (const r of (await db.query(AMZ_FNSKU_SQL, [COMPANY_ID, ids])).rows) { if (!fn.has(r.sku_id)) fn.set(r.sku_id, []); fn.get(r.sku_id).push({ value: r.value, listing_code: r.listing_code }); }
  for (const r of (await db.query(JAN_SQL, [COMPANY_ID, ids])).rows) { if (!jan.has(r.sku_id)) jan.set(r.sku_id, []); jan.get(r.sku_id).push({ value: r.value }); }
  for (const id of ids) {
    const r = rows.get(id) || null;
    const ok = !!r && (r.rule === 'lz_assign' || ((r.rule === 'fnsku' || r.rule === 'jan') && r.value));
    const cur = ok ? { kind: r.rule, value: r.rule === 'lz_assign' ? null : r.value } : null;
    const ref = { fnskus: fn.get(id) || [], jans: jan.get(id) || [] };
    out.set(id, {
      kind: cur ? cur.kind : null, value: cur ? cur.value : null, rule_row: r ? r.rule_row : null, legacy: !!r && !ok,
      via: r ? r.via : null, actor_id: r ? r.actor_id : null, created_at: r ? r.created_at : null,
      issued: issued.get(id) || null, ref, notes: lzReferenceNotes(cur, ref),
      // 🆕 #1719 R1 High 2: 写しと照らせたか (値が無い = null)。照らせていない = unchecked の一言
      lz_copy_state: cur && cur.value ? r.lz_copy_state : null, lz_copy_as_of: cur && cur.value ? r.lz_copy_as_of : null,
      unchecked: cur && cur.value ? lzCopyNote(r.lz_copy_state, r.lz_copy_as_of) : null,
    });
  }
  return out;
}

/** その値を持っているほかの商品 (Company DB = ops.lzbc_value_holder・画面のその場の確かめ)。無い = null */
export async function lzHolderInDb(db, skuId, value) {
  return (await db.query('select ops.lzbc_value_holder($1::bigint, $2) as h', [skuId == null ? '0' : String(skuId), value])).rows[0].h || null;
}

/**
 * 🆕 #1719 Codex R7 Medium: 1 つの取引で値を 2 つ以上書く (まとまりの登録) ときは、書く前に値の鍵を「そろえた値の並べ替えた順」で全部取る。
 *   鍵は ops.set_sku_logizard_barcode の中と同じ (hashtextextended('ops.lzbc:' || 値, 0)・取引の終わりまで)。
 *   子の順に関数の中で取ると、[A, B] と [B, A] の 2 つの登録が互いに待ってデッドロックする (先に全部取る = 後の登録は待ってから 409 lzbc_taken)
 */
export async function lockLzValuesInTx(db, values) {
  const vs = [...new Set((values || []).filter((v) => typeof v === 'string' && v))].sort();
  for (const v of vs) await db.query("select pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.lzbc:' || $1::text, 0))", [v]);
  return vs;
}
/**
 * 値を書く (呼び手の取引の中で・0080 の関数を 1 回)。戻り値 = 関数の答え { ok, sku_id, kind, value, changed, rule_row, replayed? }
 * DB の断り (「key: 文」) → MasterWriteError (stale・lz_issued・lzbc_taken・request_id_reused = 409 / not_single・cancelled・invalid_input = 400)
 */
export async function setLzBarcodeInTx(db, { requestId, actor, skuId, kind, value = null, via, seenRow = null, label = '', copy = null }) {
  const at = label ? `${label}: ` : '';
  // 値があるとき = ロジザードの写しと照らした結果 (copy = lz-mirror.mjs lzCopyCheck の答え・渡さない = 読めなかった扱い)。空欄 = 要らない
  //   🆕 #1719 R2 Medium 1: checked / stale は時刻が要る (無い = 読めなかった扱い)・unavailable / empty は時刻を付けない (DB の関数が組み合わせを断る)
  let st = value == null ? null : (copy && copy.state) || 'unavailable';
  if ((st === 'checked' || st === 'stale') && !(copy && copy.as_of)) st = 'unavailable';
  const asOf = st === 'checked' || st === 'stale' ? copy.as_of : null;
  try {
    return (await db.query('select ops.set_sku_logizard_barcode($1::uuid, $2, $3::bigint, $4, $5, $6, $7::bigint, $8, $9::timestamptz) as r',
      [requestId, actor, String(skuId), kind, value, via, seenRow == null ? null : String(seenRow), st, asOf])).rows[0].r;
  } catch (e) {
    const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(String((e && e.message) || ''));
    const key = m ? m[1] : '';
    const f = { field: 'logizard_barcode', ...(label ? { code: label } : {}) };
    if (key === 'stale') throw new MasterWriteError(409, 'stale', `${at}ロジザードのバーコードを、画面を開いた後にほかの人が直しました。何も保存していません。画面を開き直してください`, f);
    if (key === 'lz_issued') throw new MasterWriteError(409, 'lz_issued', `${at}${m[2]}。何も保存していません`, f);
    if (key === 'lzbc_taken') throw new MasterWriteError(409, 'lzbc_taken', `${at}${m[2]}。何も保存していません`, f);
    if (key === 'not_single' || key === 'cancelled') throw new MasterWriteError(400, key, `${at}${m[2]}。何も保存していません`, f);
    if (key === 'request_id_reused') throw new MasterWriteError(409, 'request_id_reused', '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください', f);
    if (key === 'invalid_input') throw new MasterWriteError(400, 'invalid_input', `${at}ロジザードのバーコードが DB の決まりに合いません (${m[2]})。何も保存していません`, f);
    throw e;
  }
}

/**
 * 商品の画面に出すもの (読むだけ)。cur = 商品の画面の cur (sku_id・sku_kind・registration)。canEdit = 名簿 (editorGate) の答え
 *   { show, ready, canWrite, why, kind, value, rule_row, legacy, issued, ref, notes, via, actor_id, created_at, error }
 */
export async function lzBarcodeView(db, cur, { canEdit = false } = {}) {
  if (!cur || cur.sku_kind !== 'single') return { show: false };
  if (!db) return { show: true, error: 'Company DB につながらないので出せません' };
  try {
    const ready = await lzBarcodeReady(db);
    if (!ready) return { show: true, ready: false, canWrite: false, why: 'Company DB に 0080 (ロジザードのバーコードの値) がまだ入っていないか、権限の流し直しの前です (入ると入れられます)', kind: null, value: null, ref: { fnskus: [], jans: [] }, notes: [] };
    const info = (await readLogizardBarcodes(db, [String(cur.sku_id)], { ready })).get(String(cur.sku_id));
    const state = cur.registration ? cur.registration.state : null;
    const why = state === 'cancelled' ? '登録をやめた商品です'
      : info.issued ? `ロジザード登録の CSV (ファイル #${info.issued.export_id}) に出したので直せません (ロジザードの画面で直す・CSV を取り消せば直せる)`
        : !canEdit ? 'マスタの入力の名簿にない人は見るだけです' : '';
    return { show: true, ready, canWrite: !why, why, ...info };
  } catch (e) {
    console.error(`[master-edit] ロジザードのバーコードを読めない: ${e && e.message}`);
    return { show: true, error: e && e.code === '42501' ? '画面のロールにロジザードのバーコードを読む権限が無い (ロールを流し直す)' : 'ロジザードのバーコードを読めません' };
  }
}
