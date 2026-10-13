/**
 * logizard-barcode.mjs — ロジザードに送るバーコード (新商品をロジザードに初めて載せるときの 1 本) の決め方と答え
 *   (設計 = AI_reference CompanyDB構想/23・中原さんの決定 2026-10-13 夜「JAN コードとロジザードに送るバーコード情報は別。
 *    商品のバーコードには Amazon の FNSKU を記載している場合もある」= FNSKU があれば FNSKU・無ければ JAN・JAN も無ければ空欄 (ロジザードが番号を付ける))
 *
 * 🚨 バーコードは 4 つを混ぜない (23 §2): ① 識別子 (JAN・FNSKU = core.external_ids) ② AMC 商品ラベルの印字 (発注アプリ) ③ 入庫の BC シール (入荷アプリ)
 *    ④ ロジザードに送るバーコード (= ここ)。④ は ① から「作る」だけ (値の写しを持たない・① に書き戻さない)
 * 持つのは決め方 (rule) と、候補が複数のときに人が選んだ 1 本 (pinned) だけ = ops.sku_logizard_barcode_rules (0078・追記だけ・SKU ごとに最新の 1 行)。
 *   行が無い = 決め方なし (この機能の前の商品・NE で直接作った商品) = 画面は auto で計算した参考を出す (no_rule)・③c-2 の CSV では要確認 (23 §5)。
 *   値は CSV を作るとき (今は商品の画面がその場で) Company DB の今の FNSKU・JAN から決める (FNSKU は出品の後に付く = 登録の時点ではほぼ無い)
 * 書くのは security definer の関数 ops.set_sku_logizard_barcode_rule (master_edit だけ)。単品・やめていない商品ならいつでも (使うのはロジザードに初めて載せる時だけ)
 */
import { COMPANY_ID, MasterWriteError, janValid } from './master-write.mjs';

/** 決め方 (画面の言葉)。並び = 画面の順 */
export const LZ_BARCODE_RULES = Object.freeze({
  auto: '自動 (FNSKU → JAN → 空欄)',
  fnsku: 'FNSKU',
  jan: 'JAN',
  lz_assign: '空欄 (ロジザードが番号を付ける)',
});
export const LZ_BARCODE_RULE_HINTS = Object.freeze({
  auto: 'FNSKU があれば FNSKU・無ければ JAN・JAN も無ければ空欄。値はロジザードに載せる CSV を作るときに決まります (おすすめ)',
  fnsku: '商品に Amazon の FNSKU が付いている。FNSKU が付くまでロジザードに載せずに待ちます · 候補が 2 つ以上なら 1 本を選ぶ',
  jan: '商品のバーコードは JAN (FNSKU があっても JAN を送る)。JAN が入るまで待ちます · 候補が 2 つ以上なら 1 本を選ぶ',
  lz_assign: '商品にバーコードが無い。ロジザードが番号を付けます (値札で刷って貼る)',
});
export const LZ_BARCODE_DEFAULT = 'auto';
/** 答えの状態 → 画面の言葉 */
export const LZ_BARCODE_STATES = Object.freeze({ fnsku: 'FNSKU', jan: 'JAN', blank: '空欄 (ロジザードが番号を付ける)', ambiguous: '要確認', waiting: '待つ' });
/** 決め方の行の「どこから」 */
export const LZ_BARCODE_VIA = Object.freeze({ register: '新商品の登録', sku_page: '商品の画面' });

/** FNSKU の形 (Amazon のラベル = X0 + 英数字 8 字)。ASIN と同じ形 (B0…) = メーカーのバーコードのまま出す出品 = FNSKU と見ない */
export const FNSKU_RE = /^X0[0-9A-Z]{8}$/;
const JAN_RE = /^(\d{8}|\d{13})$/;
/** 送ってよい JAN (8 か 13 桁・チェック数字が合う)。ロジザードが付けた番号が観測で JAN に入っていても、チェック数字が合わなければ数えない */
export const lzJanOk = (v) => JAN_RE.test(String(v)) && janValid(String(v));
/** 読むときの形 (前後の空白を外し・全角を半角に・英字を大文字に)。候補・選んだ 1 本・CSV で同じ形で比べる */
export const normLzValue = (v) => String(v ?? '').normalize('NFKC').trim().toUpperCase();
/** CSV のバーコードの列に書いてよい値か (FNSKU か JAN の形・空欄は '') */
export const lzBarcodeValueOk = (v) => v === '' || FNSKU_RE.test(v) || lzJanOk(v);

const bad = (message, field = 'logizard_barcode') => new MasterWriteError(400, 'invalid_input', message, { field });
/** 画面から来た決め方。送らない (undefined / null / '') = null (呼び手が「送っていない」として扱う)。知らない値 = 400 */
export function lzBarcodeRuleIn(v) {
  if (v === undefined || v === null || v === '') return null;
  if (typeof v !== 'string' || !Object.prototype.hasOwnProperty.call(LZ_BARCODE_RULES, v)) {
    throw bad('ロジザードのバーコードは 自動 / FNSKU / JAN / 空欄 から選んでください');
  }
  return v;
}
/** 選んだ 1 本 (fnsku / jan のときだけ・形まで)。送らない = null */
export function lzBarcodePinnedIn(v, rule) {
  if (v === undefined || v === null || v === '') return null;
  const s = typeof v === 'string' ? normLzValue(v) : '';
  if (rule === 'fnsku' && FNSKU_RE.test(s)) return s;
  if (rule === 'jan' && lzJanOk(s)) return s;
  throw bad(rule === 'fnsku' || rule === 'jan' ? `選んだ ${rule === 'fnsku' ? 'FNSKU' : 'JAN'} の形が違います` : '1 本を選べるのは 決め方が FNSKU か JAN のときだけです');
}

/**
 * 決め方 + 候補 → 答え (純粋・画面と CSV で同じ)。
 * @param {string|null} rule  auto / fnsku / jan / lz_assign (null = 行が無い = 決め方なし → auto で計算した参考・no_rule = true)
 * @param {{ fnskus: Array<{ value: string }>, jans: Array<{ value: string }> }} c  候補 (readLogizardBarcodes の candidates の形)
 * @param {string|null} pinned  候補が複数のときに人が選んだ 1 本 (fnsku / jan のときだけ)
 * @returns {{ rule, pinned, no_rule, state: 'fnsku'|'jan'|'blank'|'ambiguous'|'waiting', value: string|null, reason: string|null }}
 *   value = CSV のバーコードの列に書く値 (空欄 = '')・要確認 / 待つ = null (行を作らない)
 */
export function pickLogizardBarcode(rule, c, pinned = null) {
  const r = rule == null ? LZ_BARCODE_DEFAULT : rule;
  if (!Object.prototype.hasOwnProperty.call(LZ_BARCODE_RULES, r)) throw new Error(`知らない決め方: ${r}`);
  const pin = (r === 'fnsku' || r === 'jan') && pinned ? normLzValue(pinned) : null;
  const uniq = (list) => [...new Set((list || []).map((x) => normLzValue(x.value)))].sort();
  const fn = uniq(c && c.fnskus).filter((v) => FNSKU_RE.test(v));
  const jan = uniq(c && c.jans).filter((v) => lzJanOk(v));
  const out = (state, value, reason = null) => ({ rule: r, pinned: pin, no_rule: rule == null, state, value, reason });
  const word = (kind) => (kind === 'fnsku' ? 'FNSKU' : 'JAN');
  const one = (list, kind) => {
    if (pin && kind === r) {
      // 選んだ 1 本 = 今の候補にあれば それ。無い (出品が消えた・JAN を外した) = 要確認 (黙ってほかの値にしない)
      return list.includes(pin) ? out(kind, pin) : out('ambiguous', null, `選んだ ${word(kind)} ${pin} が今の候補 (${list.join('・') || 'なし'}) にありません。選び直してください`);
    }
    if (list.length === 1) return out(kind, list[0]);
    if (list.length > 1) return out('ambiguous', null, `${word(kind)}が ${list.length} つ (${list.join('・')}) あり、どれを送るか決められません。決め方を ${word(kind)} にして 1 本を選んでください`);
    return null;
  };
  if (r === 'lz_assign') return out('blank', '');
  if (r === 'fnsku') return one(fn, 'fnsku') || out('waiting', null, 'FNSKU がまだありません (Amazon 日本の出品ができて FNSKU が付くまで待ちます)');
  if (r === 'jan') return one(jan, 'jan') || out('waiting', null, 'JAN がまだありません (商品の画面で JAN を入れるまで待ちます)');
  // auto: FNSKU → JAN → 空欄。FNSKU が 2 つ以上 = 要確認 (JAN に落とさない = 商品に FNSKU が付いているかもしれない)
  return one(fn, 'fnsku') || one(jan, 'jan') || out('blank', '');
}

/**
 * FNSKU の候補 (Amazon 日本・出品の構成がこの SKU 1 個だけ・消えた出品は除く)。AMC ラベル (amc-label.mjs) の読み方 + 日本だけ。
 *   同じ FNSKU が 2 つの出品に付くことは無い (0002 の ux_external_ids_active = 有効な (system, id_kind, 値) は全体で 1 つ) = ほかの商品との重なりは出品の構成 (n_comp) で見る
 */
const FNSKU_SQL = `
  select lc.sku_id::text as sku_id, e.external_value as value, l.listing_code, l.status, lc.qty::int as qty,
         (select count(*)::int from core.listing_components x where x.listing_id = l.listing_id) as n_comp
    from core.listing_components lc
    join core.listings l on l.company_id = lc.company_id and l.listing_id = lc.listing_id and l.mall = 'amazon'
    join core.external_ids e on e.company_id = l.company_id and e.entity_type = 'listing' and e.entity_id = l.listing_id
                            and e.system = 'amazon' and e.id_kind = 'fnsku' and e.valid_to is null
   where lc.company_id = $1 and lc.sku_id = any($2::bigint[])
   order by lc.sku_id, e.external_value, l.listing_code`;
const JAN_SQL = `
  select s.sku_id::text as sku_id, e.external_value as value from core.skus s
    join core.external_ids e on e.company_id = s.company_id and e.entity_type = 'product' and e.entity_id = s.product_id
                            and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null
   where s.company_id = $1 and s.sku_id = any($2::bigint[])
   order by s.sku_id, e.external_value`;

/** 0078 (決め方の表と関数) があるか = 名前で確かめる (例外で判断しない) */
export async function lzBarcodeReady(db) {
  return (await db.query(`select to_regclass('ops.sku_logizard_barcode_rules') is not null
      and to_regprocedure('ops.set_sku_logizard_barcode_rule(uuid, text, bigint, text, text, text, bigint)') is not null as ok`)).rows[0].ok === true;
}

/**
 * SKU ごとの今の決め方・候補・答え (読むだけ)。skuIds = sku_id の配列 (文字)。ready = 0078 があるか (null = ここで確かめる)
 * @returns {Map<string, { rule, pinned, rule_row, via, actor_id, created_at, candidates: { fnskus, jans, excluded }, pick }>}
 *   rule = null・rule_row = null = 行が無い (決め方なし・pick は auto で計算した参考 = no_rule)。excluded = 候補にしない FNSKU・JAN と理由
 */
export async function readLogizardBarcodes(db, skuIds, { ready = null } = {}) {
  const ids = [...new Set((skuIds || []).map(String))];
  const out = new Map();
  if (!ids.length) return out;
  const has = ready == null ? await lzBarcodeReady(db) : ready;
  const rules = new Map();
  if (has) {
    for (const r of (await db.query(`select distinct on (sku_id) sku_id::text as sku_id, rule_row::text as rule_row, rule, pinned, via, actor_id, created_at::text as created_at
        from ops.sku_logizard_barcode_rules where company_id = $1 and sku_id = any($2::bigint[]) order by sku_id, rule_row desc`, [COMPANY_ID, ids])).rows) rules.set(r.sku_id, r);
  }
  const fn = new Map(), jan = new Map();
  for (const r of (await db.query(FNSKU_SQL, [COMPANY_ID, ids])).rows) { if (!fn.has(r.sku_id)) fn.set(r.sku_id, []); fn.get(r.sku_id).push(r); }
  for (const r of (await db.query(JAN_SQL, [COMPANY_ID, ids])).rows) { if (!jan.has(r.sku_id)) jan.set(r.sku_id, []); jan.get(r.sku_id).push({ value: r.value }); }
  for (const id of ids) {
    const fnskus = [], jans = [], excluded = [];
    for (const r of fn.get(id) || []) {
      const value = normLzValue(r.value);
      const why = r.status === 'deleted' ? '出品が消えた' : r.n_comp > 1 ? 'ほかの商品と組んだ出品' : r.qty !== 1 ? `まとめ売り (${r.qty} 個)`
        : !FNSKU_RE.test(value) ? 'Amazon のラベルの形 (X0…) でない (メーカーのバーコードのまま出す出品)' : null;
      (why ? excluded : fnskus).push({ kind: 'fnsku', value, listing_code: r.listing_code, ...(why ? { why } : {}) });
    }
    for (const j of jan.get(id) || []) {
      const value = normLzValue(j.value);
      if (lzJanOk(value)) jans.push({ kind: 'jan', value });
      else excluded.push({ kind: 'jan', value, why: 'チェック数字が合わない (ロジザードが付けた番号かもしれない)' });
    }
    const candidates = { fnskus, jans, excluded };
    const row = rules.get(id) || null;
    out.set(id, {
      rule: row ? row.rule : null, pinned: row ? row.pinned : null, rule_row: row ? row.rule_row : null, via: row ? row.via : null,
      actor_id: row ? row.actor_id : null, created_at: row ? row.created_at : null,
      candidates, pick: pickLogizardBarcode(row ? row.rule : null, candidates, row ? row.pinned : null),
    });
  }
  return out;
}

/**
 * ロジザード向けの新商品の CSV の項目に足す形 (lz-csv.mjs の NEW の barcode / barcode_hold)。
 * 要確認・待つ・決め方なし (③c-2 の約束 = 黙って auto にしない) = 行を作らない理由 (barcode_hold)
 */
export function lzCsvBarcodeOf(pick) {
  if (!pick) return {};
  if (pick.no_rule) return { barcode: null, barcode_hold: 'lz_barcode_no_rule' };
  if (pick.state === 'ambiguous' || pick.state === 'waiting') return { barcode: null, barcode_hold: pick.state === 'ambiguous' ? 'lz_barcode_ambiguous' : 'lz_barcode_waiting' };
  return { barcode: pick.value ?? '' };
}

/**
 * 決め方を書く (呼び手の取引の中で・0078 の関数を 1 回)。戻り値 = 関数の答え { ok, sku_id, rule, pinned, changed, rule_row?, replayed? }
 * seenRow = 画面を開いたときの一番新しい行番号 (行が無かった = null。新商品の登録 = null)
 * DB の断り (「key: 文」) → MasterWriteError (stale・pinned_not_candidate・request_id_reused = 409 / not_single・cancelled・invalid_input = 400)
 */
export async function setLzBarcodeRuleInTx(db, { requestId, actor, skuId, rule, pinned = null, via, seenRow = null }) {
  try {
    return (await db.query('select ops.set_sku_logizard_barcode_rule($1::uuid, $2, $3::bigint, $4, $5, $6, $7::bigint) as r',
      [requestId, actor, String(skuId), rule, pinned, via, seenRow == null ? null : String(seenRow)])).rows[0].r;
  } catch (e) {
    const m = /^([a-z_]+):\s*([\s\S]*)$/.exec(String((e && e.message) || ''));
    const key = m ? m[1] : '';
    if (key === 'stale') throw new MasterWriteError(409, 'stale', 'ロジザードのバーコードの決め方を、画面を開いた後にほかの人が直しました。何も保存していません。画面を開き直してください', { field: 'logizard_barcode' });
    if (key === 'pinned_not_candidate') throw new MasterWriteError(409, 'pinned_not_candidate', `選んだバーコードが今の候補にありません (DB: ${m[2]})。何も保存していません。画面を開き直してください`, { field: 'logizard_barcode' });
    if (key === 'not_single' || key === 'cancelled') throw new MasterWriteError(400, key, `${m[2]}。何も保存していません`, { field: 'logizard_barcode' });
    if (key === 'request_id_reused') throw new MasterWriteError(409, 'request_id_reused', '同じ保存の番号 (request_id) で違う中身が来ました。画面を開き直してください', { field: 'logizard_barcode' });
    if (key === 'invalid_input') throw new MasterWriteError(400, 'invalid_input', `ロジザードのバーコードの決め方が DB の決まりに合いません (${m[2]})。何も保存していません`, { field: 'logizard_barcode' });
    throw e;
  }
}

/**
 * 商品の画面に出すもの (読むだけ)。cur = 商品の画面の cur (sku_id・sku_kind・registration)。canEdit = 名簿 (editorGate) の答え
 *   { show, ready, canWrite, why, rule, pinned, rule_row, via, actor_id, created_at, candidates, pick, answers: { 決め方: 答え (選んだ 1 本なし) }, error }
 *   単品でない = show: false。読めない = error (画面は欄を出して理由を出す)
 */
export async function lzBarcodeView(db, cur, { canEdit = false } = {}) {
  if (!cur || cur.sku_kind !== 'single') return { show: false };
  if (!db) return { show: true, error: 'Company DB につながらないので出せません' };
  try {
    const ready = await lzBarcodeReady(db);
    const info = (await readLogizardBarcodes(db, [String(cur.sku_id)], { ready })).get(String(cur.sku_id));
    const state = cur.registration ? cur.registration.state : null;
    const why = !ready ? 'Company DB に 0078 (ロジザードのバーコードの決め方) がまだ入っていません (入ると直せます)'
      : state === 'cancelled' ? '登録をやめた商品です' : !canEdit ? 'マスタの入力の名簿にない人は見るだけです' : '';
    const answers = Object.fromEntries(Object.keys(LZ_BARCODE_RULES).map((r) => [r, pickLogizardBarcode(r, info.candidates, null)]));
    return { show: true, ready, canWrite: !why, why, ...info, answers };
  } catch (e) {
    console.error(`[master-edit] ロジザードのバーコードを読めない: ${e && e.message}`);
    return { show: true, error: e && e.code === '42501' ? '画面のロールに決め方・出品・JAN を読む権限が無い (ロールを流し直す)' : 'ロジザードのバーコードの決め方・候補を読めません' };
  }
}
