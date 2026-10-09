/**
 * order-settings.js — 発注の設定を書く「1 つの部品」(10/9 中原さん: master-edit の登録・商品の画面からも入れたい)
 *
 * 発注の設定 = 発注アプリ (warehouse-mirror.db・Render の DATA_DIR) の 3 つの表:
 *   po_product_attrs     商品ごと: 発注ロット (order_lot)・発注条件グループ・原料グループ・容量/個・ケースグループ・ケースロット
 *   po_order_conditions  発注条件グループ (仕入先ごとの最低数量 / 最低金額 など)
 *   po_material_groups   原料グループ
 * 正本は発注アプリのまま (AI_reference CompanyDB構想/10 D-44)。書く入口は 2 つ:
 *   ① 発注アプリのマスタ管理 (/apps/purchase-orders/admin) と発注画面のグループの紐付け
 *   ② マスタの入力 (/apps/master-edit) の新商品の登録・商品の画面
 * どちらもこの部品だけを通る = 確かめ・書き込み・記録 (po_audit_log に だれが・どの画面で・前と後) が 1 か所 = 入口が 2 つでもずれない。
 *
 * 先に読んだ値の確かめ (seen): 画面を開いたときの updated_at (行が無かった = null) を送る。その間にほかの人が直していたら 409 stale (何も書かない)。
 *   発注画面のグループの紐付け (一部の列だけ直す) は seen.fields = 画面が見ていた列の値で確かめる。
 *   新商品の登録の 2 つめの書き込み (Company DB の登録が通った後) だけは 'overwrite' (新しいコード = 前の値は無い・同じ request_id のやり直しは同じ値の上書き)。
 * 1 回の書き込み = 1 つの即時の取引 (BEGIN IMMEDIATE): 新しいグループを作る → 商品の行 → 記録。途中で断ったら何も残らない。
 *
 * 発注ロット (order_lot) = 発注のすすめる数をこの数の倍数にそろえる数 (logic.js computeProduct の N)。
 *   前は NE の goods_lot (「発注ロット単位」) を商品管理リスト経由で使っていた。NE の値を一度だけ写す (scripts/copy-ne-order-lot.mjs) まで
 *   po_settings.order_lot_source は 'ne' (今までどおり NE の値)。写したら 'app' = 発注アプリの order_lot だけを見る (NE の値は使わない)。
 */
import { getDB, normSupplierCode, normProductCode } from './db.js';
import { audit, getSetting } from './ledger.js';

/** 商品ごとの列 (po_product_attrs) */
export const ATTR_FIELDS = Object.freeze(['order_lot', 'condition_id', 'material_group_id', 'capacity_per_unit', 'case_group', 'case_lot']);
/** 「紐付け」の列 (発注ロットだけの行は「未紐付け」のまま = 発注ロットを写しただけで紐付け済みに見せない) */
export const LINK_FIELDS = Object.freeze(['condition_id', 'material_group_id', 'capacity_per_unit', 'case_group', 'case_lot']);
export const FIELD_LABELS = Object.freeze({
  order_lot: '発注ロット', condition_id: '発注条件グループ', material_group_id: '原料グループ',
  capacity_per_unit: '容量/個', case_group: 'ケースグループ', case_lot: 'ケースロット',
});
/** どの画面から書いたか (記録の via) */
export const VIA = Object.freeze({
  'po-admin': '発注アプリのマスタ管理',
  'po-bind': '発注アプリの発注画面 (グループの紐付け)',
  'master-edit:new': 'マスタの入力 (新商品の登録)',
  'master-edit:sku': 'マスタの入力 (商品の画面)',
});
/** マスタの入力から新しく作るときの発注条件の種類 (logic.js evaluateCondition が自動で判定できるものだけ) */
export const CONDITION_TYPES = Object.freeze([
  { type: '金額', unit: '円', label: '金額 (円) 以上' },
  { type: '数量', unit: '個', label: '数量 (個) 以上' },
  { type: '数量', unit: 'ケース', label: 'ケース数 以上' },
  { type: '上限', unit: '個', label: '数量 (個) まで' },
  { type: 'ケース入数かつ金額', unit: '円', label: 'ケースの倍数 かつ 金額 (円) 以上' },
  { type: 'ロット倍率', unit: '倍', label: '発注ロットの n 倍ずつ' },
  { type: 'ロット倍率以上', unit: '倍', label: '発注ロットの n 倍以上' },
]);
export const ORDER_LOT_SOURCE_KEY = 'order_lot_source';
export const MAX_ORDER_LOT = 1000000;

/** 発注のすすめる数に使うロットの出どころ: 'ne' (NE の値・写す前) / 'app' (発注アプリの order_lot・写した後) */
export function orderLotSource() {
  return getSetting(ORDER_LOT_SOURCE_KEY) === 'app' ? 'app' : 'ne';
}

export class OrderSettingsError extends Error {
  constructor(status, reason, message, extra = {}) {
    super(message);
    this.name = 'OrderSettingsError';
    this.status = status;
    this.reason = reason;
    this.extra = extra;
  }
}
const bad = (message, field, reason = 'invalid_input') => new OrderSettingsError(400, reason, message, { field });

// ─── 入力の形 (DB を読まない) ───
const CTRL_RE = new RegExp(`[\\x00-\\x1f\\x7f${String.fromCharCode(0x2028, 0x2029)}]`);
const blank = (v) => v == null || (typeof v === 'string' && v.trim() === '');
/** 全角の数字・小数点・カンマを半角に (画面で全角のまま打っても通す) */
const half = (s) => String(s).replace(/[０-９．，]/g, (c) => (c === '，' ? ',' : String.fromCharCode(c.charCodeAt(0) - 0xFEE0)));

function textIn(v, { label, field, max = 100 }) {
  if (blank(v)) return null;
  if (typeof v !== 'string' && typeof v !== 'number') throw bad(`${label}は文字で入れてください`, field);
  const s = String(v).trim();
  if (CTRL_RE.test(s)) throw bad(`${label}に使えない文字 (改行など) があります`, field);
  if (s.length > max) throw bad(`${label}は ${max} 字までです`, field);
  return s;
}
/** グループの ID。発注アプリの画面は「ID — 名前」で選ぶ = ID に ' — ' は使わない */
function idIn(v, { label, field }) {
  const s = textIn(v, { label, field, max: 64 });
  if (s != null && s.includes(' — ')) throw bad(`${label}に「 — 」は使えません`, field);
  return s;
}
function numIn(v, { label, field, gt0 = false, int = false, max = 1e9 }) {
  if (blank(v)) return null;
  if (typeof v !== 'number' && typeof v !== 'string') throw bad(`${label}は数で入れてください`, field);
  const s = half(v).trim().replace(/,/g, '');
  if (!/^\d+(\.\d+)?$/.test(s)) throw bad(`${label}は${gt0 ? ' 0 より大きい' : ' 0 以上の'}数で入れてください`, field);
  const n = Number(s);
  if (!Number.isFinite(n) || n > max) throw bad(`${label}が大きすぎます (${max.toLocaleString('ja-JP')} まで)`, field);
  if (int && !Number.isInteger(n)) throw bad(`${label}は整数で入れてください`, field);
  if (gt0 ? !(n > 0) : n < 0) throw bad(`${label}は${gt0 ? ' 1 以上' : ' 0 以上'}で入れてください`, field);
  return n;
}

const PARSE = {
  order_lot: (v) => numIn(v, { label: '発注ロット', field: 'order_lot', gt0: true, int: true, max: MAX_ORDER_LOT }),
  condition_id: (v) => idIn(v, { label: '発注条件グループ', field: 'condition_id' }),
  material_group_id: (v) => idIn(v, { label: '原料グループ', field: 'material_group_id' }),
  capacity_per_unit: (v) => numIn(v, { label: '容量/個', field: 'capacity_per_unit', gt0: true }),
  case_group: (v) => textIn(v, { label: 'ケースグループ', field: 'case_group', max: 100 }),
  case_lot: (v) => numIn(v, { label: 'ケースロット', field: 'case_lot', gt0: true, max: MAX_ORDER_LOT }),
};

/** 直す列だけ (本文に無い列は今の値のまま) */
export function parseAttrPatch(raw) {
  if (raw == null) return {};
  if (typeof raw !== 'object' || Array.isArray(raw)) throw bad('発注の設定の入れ方が違う', 'patch');
  const out = {};
  for (const [k, v] of Object.entries(raw)) {
    if (!Object.hasOwn(PARSE, k)) throw bad(`「${k}」は発注の設定の項目ではありません`, k);
    if (v === undefined) continue;
    out[k] = PARSE[k](v);
  }
  return out;
}

const typeOf = (type, unit) => CONDITION_TYPES.find((t) => t.type === type && t.unit === unit) || null;

/** マスタの入力から新しく作る発注条件グループ (仕入先は代表の仕入先 = 呼び手が入れる) */
export function parseNewCondition(raw) {
  if (raw == null) return null;
  if (typeof raw !== 'object' || Array.isArray(raw)) throw bad('新しい発注条件グループの入れ方が違う', 'new_condition');
  const f = (k) => `new_condition.${k}`;
  const id = idIn(raw.condition_id, { label: '新しい発注条件グループの ID', field: f('condition_id') });
  if (!id) throw bad('新しい発注条件グループの ID を入れてください', f('condition_id'));
  const name = textIn(raw.display_name, { label: '新しい発注条件グループの名前', field: f('display_name') });
  if (!name) throw bad('新しい発注条件グループの名前を入れてください', f('display_name'));
  const t = typeOf(String(raw.condition_type ?? ''), String(raw.unit ?? ''));
  if (!t) throw bad('発注条件の種類を一覧から選んでください', f('condition_type'));
  const lotKind = t.unit === '倍';
  const value = numIn(raw.condition_value, { label: '発注条件の値', field: f('condition_value'), gt0: lotKind, int: lotKind, max: 1e9 });
  if (value == null) throw bad('発注条件の値を入れてください', f('condition_value'));
  return {
    condition_id: id, display_name: name, condition_type: t.type, unit: t.unit, condition_value: value,
    maker_name: textIn(raw.maker_name, { label: 'メーカー名', field: f('maker_name') }),
  };
}

/** マスタの入力から新しく作る原料グループ */
export function parseNewMaterial(raw) {
  if (raw == null) return null;
  if (typeof raw !== 'object' || Array.isArray(raw)) throw bad('新しい原料グループの入れ方が違う', 'new_material');
  const f = (k) => `new_material.${k}`;
  const id = idIn(raw.group_id, { label: '新しい原料グループの ID', field: f('group_id') });
  if (!id) throw bad('新しい原料グループの ID を入れてください', f('group_id'));
  const name = textIn(raw.name, { label: '新しい原料グループの名前', field: f('name') });
  if (!name) throw bad('新しい原料グループの名前を入れてください', f('name'));
  return {
    group_id: id, name,
    min_order_qty: numIn(raw.min_order_qty, { label: '最低発注量', field: f('min_order_qty') }),
    unit: textIn(raw.unit, { label: '単位', field: f('unit'), max: 20 }),
  };
}

// ─── 比べる ───
const norm = (v) => (v == null || v === '' ? null : v);
/** 同じ値か (数は数で・空と null は同じ) */
export function sameVal(a, b) {
  const x = norm(a); const y = norm(b);
  if (x == null || y == null) return x == null && y == null;
  const nx = Number(x); const ny = Number(y);
  if (typeof x === 'number' || typeof y === 'number') return Number.isFinite(nx) && Number.isFinite(ny) && nx === ny;
  return String(x) === String(y);
}
export const pickAttrs = (r) => (r ? Object.fromEntries(ATTR_FIELDS.map((k) => [k, r[k] ?? null])) : null);
/** 紐付け (グループ・容量・ケースのどれか) があるか。発注ロットだけの行は紐付けではない */
export const hasLinkage = (a) => !!a && LINK_FIELDS.some((k) => a[k] != null && a[k] !== '');

// ─── 時刻 (先に読んだ値の印) ───
/** 書いた時刻。前の updated_at と同じミリ秒にならないように 1ms 進める (先に読んだ値の確かめが同じ印で見逃さない) */
function nextStamp(prev) {
  let t = Date.now();
  const p = prev ? Date.parse(prev) : NaN;
  if (Number.isFinite(p) && p >= t) t = p + 1;
  return new Date(t).toISOString();
}

function staleError(what, cur) {
  return new OrderSettingsError(409, 'stale',
    `ほかの人 (か別の画面) が${what}を先に直しました。何も保存していません。画面を開き直して今の値を見てから、もう一度保存してください`,
    { current: cur ?? null, updated_at: cur ? cur.updated_at ?? null : null });
}
const seenRequired = () => new OrderSettingsError(428, 'seen_required', '画面を開いたときの値 (seen) がありません。画面を開き直してから保存してください (ほかの人の変更を上書きしないため)');

/** seen = { updated_at: 文字 | null (行が無かった) } / { fields: { 列: 値 } } / 'overwrite' (新商品の登録だけ) */
function checkSeen(seen, cur, what, { allowOverwrite = false } = {}) {
  if (seen === 'overwrite') {
    if (!allowOverwrite) throw seenRequired();
    return;
  }
  if (!seen || typeof seen !== 'object' || Array.isArray(seen)) throw seenRequired();
  if (Object.hasOwn(seen, 'updated_at')) {
    const was = seen.updated_at == null || seen.updated_at === '' ? null : String(seen.updated_at);
    const now = cur ? cur.updated_at ?? null : null;
    if (was !== now) throw staleError(what, cur);
    return;
  }
  if (seen.fields && typeof seen.fields === 'object' && !Array.isArray(seen.fields)) {
    for (const [k, v] of Object.entries(seen.fields)) {
      if (!ATTR_FIELDS.includes(k)) throw bad(`seen の「${k}」は発注の設定の項目ではありません`, 'seen');
      if (!sameVal(cur ? cur[k] : null, v)) throw staleError(what, cur);
    }
    return;
  }
  throw seenRequired();
}

// ─── 読む ───
const attrsRow = (db, key) => db.prepare('SELECT * FROM po_product_attrs WHERE product_key=?').get(key) || null;
const condRow = (db, id) => db.prepare('SELECT * FROM po_order_conditions WHERE condition_id=?').get(id) || null;
const matRow = (db, id) => db.prepare('SELECT * FROM po_material_groups WHERE group_id=?').get(id) || null;

const sameCondition = (ex, nc, supplierCode) => normSupplierCode(ex.supplier_code) === normSupplierCode(supplierCode)
  && ex.display_name === nc.display_name && ex.condition_type === nc.condition_type && sameVal(ex.condition_value, nc.condition_value)
  && sameVal(ex.unit, nc.unit) && sameVal(ex.maker_name, nc.maker_name);
const sameMaterial = (ex, nm) => ex.name === nm.name && sameVal(ex.min_order_qty, nm.min_order_qty) && sameVal(ex.unit, nm.unit);

function actorOf(input) {
  const actor = String(input.actor ?? '').trim();
  if (!actor || actor.length > 320 || CTRL_RE.test(actor)) throw bad('保存する人が分からない (ログインし直してください)', 'actor');
  const actorType = input.actorType || 'user';
  const via = String(input.via || '');
  if (!Object.hasOwn(VIA, via)) throw bad(`書いた画面 (via) が分からない: ${via}`, 'via');
  const requestId = input.requestId == null || input.requestId === '' ? null : String(input.requestId).slice(0, 200);
  return { actor, actorType, via, requestId };
}

/**
 * 商品の発注の設定を書く (新しいグループを作るのも同じ取引で)。
 * input = {
 *   code: 商品コード (打ったとおり。鍵は前後の空白を除いた小文字),
 *   patch: { order_lot?, condition_id?, material_group_id?, capacity_per_unit?, case_group?, case_lot? } (本文に無い列は今のまま),
 *   newCondition?: { condition_id, display_name, condition_type, unit, condition_value, maker_name? } (作ってからその ID を選ぶ),
 *   newMaterial?: { group_id, name, min_order_qty?, unit? },
 *   seen: { updated_at } | { fields } | 'overwrite' (新商品の登録だけ),
 *   supplierCode?: 代表の仕入先 (マスタの入力から = 渡す。発注条件グループはこの仕入先のものだけ選べる・新しく作るグループの仕入先) / undefined = 確かめない (発注アプリの画面),
 *   actor, actorType?, via ('po-admin' | 'po-bind' | 'master-edit:new' | 'master-edit:sku'), requestId?,
 * }
 * opts.dryRun = 全部確かめるが書かない (新商品の登録で Company DB に書く前に確かめる)。
 * 戻り値 = { ok: true, changed, row (書いた後の行 | null), created: { condition, material }, dryRun? }
 */
export function writeOrderSettings(input, { dryRun = false } = {}) {
  const who = actorOf(input || {});
  const code = textIn(input.code, { label: '商品コード', field: 'code', max: 60 });
  if (!code) throw bad('商品コードがありません', 'code');
  const key = normProductCode(code);
  const patch = parseAttrPatch(input.patch);
  const newCondition = parseNewCondition(input.newCondition);
  const newMaterial = parseNewMaterial(input.newMaterial);
  if (newCondition) patch.condition_id = newCondition.condition_id;
  if (newMaterial) patch.material_group_id = newMaterial.group_id;
  const checkSupplier = input.supplierCode !== undefined;
  const supplier = checkSupplier ? normSupplierCode(input.supplierCode) : null;
  if (newCondition && checkSupplier && !supplier) throw bad('代表の仕入先が無いので、発注条件グループを作れません (先に代表の仕入先を)', 'new_condition');
  const allowOverwrite = who.via === 'master-edit:new';

  const db = getDB();
  const run = () => {
    const cur = attrsRow(db, key);
    checkSeen(input.seen, cur, `この商品 (${code}) の発注の設定`, { allowOverwrite });
    const created = { condition: false, material: false };
    // 1. 新しいグループ (同じ中身が既にある = 前の同じ登録のやり直し = 作らずにそれを使う。中身が違う = 409)
    if (newCondition) {
      const ex = condRow(db, newCondition.condition_id);
      if (ex && !sameCondition(ex, newCondition, supplier)) {
        throw new OrderSettingsError(409, 'group_id_taken', `発注条件グループの ID「${newCondition.condition_id}」はもうあります (中身が違う)。別の ID にするか、「今あるグループ」から選んでください。何も保存していません`, { field: 'new_condition.condition_id' });
      }
      if (!ex) {
        const now = new Date().toISOString();
        db.prepare(`INSERT INTO po_order_conditions (condition_id, supplier_code, maker_name, display_name, condition_type, condition_value, unit, created_at, updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?)`)
          .run(newCondition.condition_id, supplier || null, newCondition.maker_name, newCondition.display_name, newCondition.condition_type, newCondition.condition_value, newCondition.unit, now, now);
        audit(db, { actorType: who.actorType, actor: who.actor, action: 'po_condition_write', resource: `condition:${newCondition.condition_id}`, requestId: who.requestId,
          detail: { via: who.via, for_product: code, before: null, after: { ...newCondition, supplier_code: supplier || null } } });
        created.condition = true;
      }
    }
    if (newMaterial) {
      const ex = matRow(db, newMaterial.group_id);
      if (ex && !sameMaterial(ex, newMaterial)) {
        throw new OrderSettingsError(409, 'group_id_taken', `原料グループの ID「${newMaterial.group_id}」はもうあります (中身が違う)。別の ID にするか、「今あるグループ」から選んでください。何も保存していません`, { field: 'new_material.group_id' });
      }
      if (!ex) {
        const now = new Date().toISOString();
        db.prepare('INSERT INTO po_material_groups (group_id, name, min_order_qty, unit, created_at, updated_at) VALUES (?,?,?,?,?,?)')
          .run(newMaterial.group_id, newMaterial.name, newMaterial.min_order_qty, newMaterial.unit, now, now);
        audit(db, { actorType: who.actorType, actor: who.actor, action: 'po_material_write', resource: `material:${newMaterial.group_id}`, requestId: who.requestId,
          detail: { via: who.via, for_product: code, before: null, after: newMaterial } });
        created.material = true;
      }
    }
    // 2. 変える列の確かめ (変えた列だけ = 前からの値は止めない。存在しないグループへの紐付けは静かに欠落するので断る)
    const before = pickAttrs(cur);
    const merged = { ...(before || Object.fromEntries(ATTR_FIELDS.map((k) => [k, null]))), ...patch };
    const changing = (k) => Object.hasOwn(patch, k) && !sameVal(before ? before[k] : null, patch[k]);
    if (changing('condition_id') && merged.condition_id) {
      const c = condRow(db, merged.condition_id);
      if (!c) throw bad(`発注条件グループが未登録です: ${merged.condition_id} (発注アプリにありません)`, 'condition_id');
      if (checkSupplier && normSupplierCode(c.supplier_code) !== supplier) {
        throw bad(supplier
          ? `発注条件グループ「${c.display_name || c.condition_id}」は代表の仕入先 (${supplier}) のグループではありません (仕入先 ${normSupplierCode(c.supplier_code) || 'なし'})`
          : '代表の仕入先が無いので、発注条件グループを選べません (先に代表の仕入先を)', 'condition_id');
      }
    }
    if (changing('material_group_id') && merged.material_group_id && !matRow(db, merged.material_group_id)) {
      throw bad(`原料グループが未登録です: ${merged.material_group_id} (発注アプリにありません)`, 'material_group_id');
    }
    const changed = ATTR_FIELDS.some((k) => !sameVal(before ? before[k] : null, merged[k]));
    // 3. 書く (変わらない = 書かない = updated_at も進めない)。行が無くて全部空 = 何も作らない
    if (!changed || (!cur && ATTR_FIELDS.every((k) => merged[k] == null))) {
      return { ok: true, changed: false, row: cur, created };
    }
    const stamp = nextStamp(cur ? cur.updated_at : null);
    if (cur) {
      db.prepare(`UPDATE po_product_attrs SET ${ATTR_FIELDS.map((k) => `${k}=?`).join(', ')}, updated_at=? WHERE product_key=?`)
        .run(...ATTR_FIELDS.map((k) => merged[k]), stamp, key);
    } else {
      db.prepare(`INSERT INTO po_product_attrs (product_key, product_code, ${ATTR_FIELDS.join(', ')}, created_via, created_at, updated_at)
                  VALUES (?,?,${ATTR_FIELDS.map(() => '?').join(',')},?,?,?)`)
        .run(key, code, ...ATTR_FIELDS.map((k) => merged[k]), who.via, stamp, stamp);
    }
    audit(db, { actorType: who.actorType, actor: who.actor, action: 'po_attrs_write', resource: `attrs:${key}`, requestId: who.requestId,
      detail: { via: who.via, code, before, after: pickAttrs(merged), created_groups: created } });
    return { ok: true, changed: true, row: attrsRow(db, key), created };
  };
  if (!dryRun) return db.transaction(run).immediate();
  // 確かめだけ = 同じ取引で全部やってから巻き戻す (確かめの決まりが書き込みとずれない)
  const DRY = 'po-order-settings-dry-run';
  try {
    db.transaction(() => { const out = run(); const e = new Error(DRY); e.dry = out; throw e; }).immediate();
  } catch (e) {
    if (e && e.message === DRY && e.dry) return { ...e.dry, dryRun: true };
    throw e;
  }
  throw new Error('unreachable');
}

/**
 * 商品の紐付けを外す (発注アプリのマスタ管理の「削除」)。紐付けの列 (グループ・容量・ケース) を空にする。
 * 発注ロットは残す (消すと発注のすすめる数が出なくなる)。発注ロットも空なら行ごと消す
 */
export function clearProductLinks(input) {
  const who = actorOf(input || {});
  const key = normProductCode(input.code);
  if (!key) throw bad('商品コードがありません', 'code');
  const db = getDB();
  return db.transaction(() => {
    const cur = attrsRow(db, key);
    if (!cur) return { ok: true, deleted: 0, kept: false };
    checkSeen(input.seen, cur, `この商品 (${cur.product_code}) の発注の設定`);
    const before = pickAttrs(cur);
    let deleted = 0; let kept = false;
    if (cur.order_lot == null) {
      deleted = db.prepare('DELETE FROM po_product_attrs WHERE product_key=?').run(key).changes;
    } else {
      db.prepare(`UPDATE po_product_attrs SET ${LINK_FIELDS.map((k) => `${k}=NULL`).join(', ')}, updated_at=? WHERE product_key=?`).run(nextStamp(cur.updated_at), key);
      kept = true;
    }
    audit(db, { actorType: who.actorType, actor: who.actor, action: 'po_attrs_unlink', resource: `attrs:${key}`, requestId: who.requestId,
      detail: { via: who.via, code: cur.product_code, before, after: kept ? { ...before, ...Object.fromEntries(LINK_FIELDS.map((k) => [k, null])) } : null } });
    return { ok: true, deleted, kept };
  }).immediate();
}

// ─── グループ (発注アプリのマスタ管理の発注条件グループ・原料グループのタブ) ───
const GROUPS = {
  conditions: {
    table: 'po_order_conditions', pk: 'condition_id', label: '発注条件グループ', resource: 'condition', action: 'po_condition_write',
    cols: ['condition_id', 'supplier_code', 'maker_name', 'display_name', 'condition_type', 'condition_value', 'unit'],
    refCol: 'condition_id',
  },
  materials: {
    table: 'po_material_groups', pk: 'group_id', label: '原料グループ', resource: 'material', action: 'po_material_write',
    cols: ['group_id', 'name', 'min_order_qty', 'unit'],
    refCol: 'material_group_id',
  },
};

/**
 * 発注条件グループ・原料グループを 1 行書く (マスタ管理のタブの「保存」「追加」)。row は router の MASTER_DEFS が形を確かめた行。
 * seen.updated_at = 開いたときの印 (追加 = null = まだ無いこと)
 */
export function writeGroup(kind, row, input) {
  const g = GROUPS[kind];
  if (!g) throw bad(`グループの種類が違う: ${kind}`, 'kind');
  const who = actorOf(input || {});
  const id = row && row[g.pk];
  if (!id) throw bad(`${g.label}の ID がありません`, g.pk);
  if (String(id).includes(' — ')) throw bad(`${g.label}の ID に「 — 」は使えません`, g.pk);
  const db = getDB();
  return db.transaction(() => {
    const cur = db.prepare(`SELECT * FROM ${g.table} WHERE ${g.pk}=?`).get(id) || null;
    checkSeen(input.seen, cur, `${g.label}「${id}」`);
    const before = cur ? Object.fromEntries(g.cols.map((k) => [k, cur[k] ?? null])) : null;
    const after = Object.fromEntries(g.cols.map((k) => [k, row[k] ?? null]));
    const changed = !before || g.cols.some((k) => !sameVal(before[k], after[k]));
    if (!changed) return { ok: true, changed: false, row: cur };
    const stamp = nextStamp(cur ? cur.updated_at : null);
    if (cur) {
      db.prepare(`UPDATE ${g.table} SET ${g.cols.filter((k) => k !== g.pk).map((k) => `${k}=?`).join(', ')}, updated_at=? WHERE ${g.pk}=?`)
        .run(...g.cols.filter((k) => k !== g.pk).map((k) => after[k]), stamp, id);
    } else {
      db.prepare(`INSERT INTO ${g.table} (${g.cols.join(', ')}, created_at, updated_at) VALUES (${g.cols.map(() => '?').join(',')}, ?, ?)`)
        .run(...g.cols.map((k) => after[k]), stamp, stamp);
    }
    audit(db, { actorType: who.actorType, actor: who.actor, action: g.action, resource: `${g.resource}:${id}`, requestId: who.requestId, detail: { via: who.via, before, after } });
    return { ok: true, changed: true, row: db.prepare(`SELECT * FROM ${g.table} WHERE ${g.pk}=?`).get(id) };
  }).immediate();
}

/** グループを消す。商品から使われている = 400 (先に紐付けを外す)。seen.updated_at = 開いたときの印 */
export function deleteGroup(kind, id, input) {
  const g = GROUPS[kind];
  if (!g) throw bad(`グループの種類が違う: ${kind}`, 'kind');
  const who = actorOf(input || {});
  const db = getDB();
  return db.transaction(() => {
    const cur = db.prepare(`SELECT * FROM ${g.table} WHERE ${g.pk}=?`).get(id) || null;
    if (!cur) return { ok: true, deleted: 0 };
    checkSeen(input.seen, cur, `${g.label}「${id}」`);
    const n = db.prepare(`SELECT COUNT(*) c FROM po_product_attrs WHERE ${g.refCol}=?`).get(id).c;
    if (n) throw bad(`商品紐付け ${n} 件から参照されています。先に紐付けを外してください`, g.pk, 'in_use');
    const deleted = db.prepare(`DELETE FROM ${g.table} WHERE ${g.pk}=?`).run(id).changes;
    audit(db, { actorType: who.actorType, actor: who.actor, action: `${g.action.replace(/_write$/, '')}_delete`, resource: `${g.resource}:${id}`, requestId: who.requestId,
      detail: { via: who.via, before: Object.fromEntries(g.cols.map((k) => [k, cur[k] ?? null])), after: null } });
    return { ok: true, deleted };
  }).immediate();
}

// ─── 画面 (マスタの入力) に見せる値 ───
/** NE の発注ロット (商品管理リストの公開の回 = 写す前に使っている値・写した後は「NE と違う」の知らせに使う)。無い = null */
function neLotOf(db, key) {
  try {
    const r = db.prepare(`SELECT r.発注ロット単位 AS lot FROM mirror_pml_snapshot_rows r
      WHERE r.run_id = (SELECT run_id FROM mirror_pml_published WHERE id = 1) AND lower(trim(r.商品コード)) = ? LIMIT 1`).get(key);
    return r && r.lot != null && Number(r.lot) > 0 ? Number(r.lot) : null;
  } catch { return null; }
}

/**
 * マスタの入力の画面 (新商品の登録・商品の画面) に出す値。code = null (新商品) / 商品コード。
 * { ok, lotSource, row (今の値 + updated_at | null), neLot, conditions: [{ id, name, supplier, type, value, unit }], materials: [{ id, name, min, unit }], types }
 * 読めない = { ok: false, error }
 */
export function readOrderSettingsForScreen(code = null) {
  try {
    const db = getDB();
    const key = code == null ? null : normProductCode(code);
    const cur = key ? attrsRow(db, key) : null;
    return {
      ok: true,
      lotSource: orderLotSource(),
      row: cur ? { ...pickAttrs(cur), updated_at: cur.updated_at } : null,
      neLot: key ? neLotOf(db, key) : null,
      conditions: db.prepare('SELECT * FROM po_order_conditions ORDER BY condition_id').all().map((c) => ({
        id: c.condition_id, name: c.display_name || '', supplier: normSupplierCode(c.supplier_code), maker: c.maker_name || '',
        type: c.condition_type, value: c.condition_value, unit: c.unit || '',
      })),
      materials: db.prepare('SELECT * FROM po_material_groups ORDER BY group_id').all().map((m) => ({ id: m.group_id, name: m.name || '', min: m.min_order_qty, unit: m.unit || '' })),
      types: CONDITION_TYPES,
    };
  } catch (e) {
    console.error(`[po-order-settings] 発注アプリの設定を読めない: ${e && e.message}`);
    return { ok: false, error: '発注アプリの設定を読めません' };
  }
}
