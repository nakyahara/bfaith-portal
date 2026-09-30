/**
 * master-register-grants.mjs — 新商品の登録 (0051・Company DB構想 14 ⑤-2a) で画面と運用のロールに渡す権限 (PR #1566 Codex R1 H2)
 *
 * 🚨 権限の境界: master_edit (画面) と master_ops (運用の手の操作) には、登録の状態の表と履歴 (ops.master_registrations / ops.master_registration_events /
 *    ops.master_registration_backfill) の INSERT / UPDATE / DELETE を渡さない。書くのは security definer の関数だけで、ここで渡すのはその実行権だけ
 *    (GUC ops.registration_protocol を立てても 42501 = scripts/test-master-register.mjs の迂回の試験)
 * TODO (⑤-1 の rebase のとき): scripts/company-db/create-master-edit-roles.mjs (⑤-1 R1 の画面だけのロール) の文の一覧に
 *    masterRegisterRoleStatements() を足す (同じ取引で流す)。⑤-1 R2 でロールが master_edit / master_ops / observer になったら名前をそろえる
 *
 * master_edit (画面 D = lib/master-register.mjs・カードの知らせの取り込み = lib/product-hub-outbox.mjs):
 *   読む: 登録の状態 (画面に出す)・backfill の印・カードの知らせ・NE の元のコード (コードの確かめ)・変更の記録 (消したコードの確かめ)
 *   書く: 新商品の SKU・商品・原価・仕入先・構成の依頼・カードの知らせ (登録と取り込みの結果の列だけ)・保存の記録・変更の記録 (0026 のトリガーは呼び手の権限)
 *   実行: ops.create_sku_registration (状態 draft を作る)・ops.sku_registration_problem (SKU を足した commit の確かめ = 呼び手の権限の trigger から呼ぶ)
 * master_ops (切替の日の手の操作): backfill の計画を見る・backfill を流す・登録をやめる (cancelled)
 * 夜間ロード (表の持ち主のロール) は持ち主なので何も渡さない
 */
export const REGISTER_SELECT = [
  'core.skus', 'core.products', 'core.sku_components', 'core.sku_costs', 'core.suppliers', 'core.supplier_skus',
  'events.master_change_events',
  'ops.master_cutover_state', 'ops.master_edit_requests', 'ops.sku_component_requests',
  'ops.master_registrations', 'ops.master_registration_backfill', 'ops.product_hub_outbox', 'ops.master_ne_codes',
];
export const REGISTER_WRITE = [
  ['insert (company_id, display_code, name, sales_class, status, expiry_managed, inbound_date_managed, created_by_type, created_by_id)', 'core.products'],
  ['insert (company_id, product_id, sku_kind, code, name, tax_rate, tax_class, handling, standard_price_jpy, shipping_code, shipping_method, shipping_cost_jpy, reorder_months, set_sales_class_override, handling_own, created_by_type, created_by_id), update (version)', 'core.skus'],
  ['insert', 'core.sku_costs'],
  ['insert', 'core.supplier_skus'],
  ['insert', 'ops.sku_component_requests'],
  ['insert', 'ops.master_edit_requests'],
  ['insert', 'events.master_change_events'],
  ['insert (company_id, sku_id, kind, schema_version, payload, payload_hash, request_id, created_by), update (status, attempts, last_error, lease_owner, leased_until, result, done_at, updated_at)', 'ops.product_hub_outbox'],
];
export const REGISTER_EXECUTE = ['ops.create_sku_registration(bigint, text, text, text)', 'ops.sku_registration_problem(bigint, text)'];
export const OPS_EXECUTE = ['ops.registration_backfill_plan()', 'ops.backfill_sku_registrations(integer, text, text, text)', 'ops.transition_sku_registration(bigint, text, text, text, text, jsonb, text)'];
export const OPS_SELECT = ['ops.master_registrations', 'ops.master_registration_events', 'ops.master_registration_backfill'];

const ident = (s) => { if (!/^[a-z_][a-z0-9_]*$/.test(s)) throw new Error(`識別子が不正: ${s}`); return s; };

/** 流す文の一覧 (ロールはもうある前提。create-master-edit-roles.mjs の後ろに足す) */
export function masterRegisterRoleStatements({ editRole = 'master_edit', opsRole = 'master_ops' } = {}) {
  const e = ident(editRole), o = ident(opsRole);
  const s = [];
  for (const sc of ['core', 'ops', 'events']) s.push(`grant usage on schema ${sc} to ${e}`);
  for (const t of REGISTER_SELECT) s.push(`grant select on ${t} to ${e}`);
  for (const [priv, t] of REGISTER_WRITE) s.push(`grant ${priv} on ${t} to ${e}`);
  s.push(`grant usage on sequence core.master_version_seq to ${e}`);   // version の既定値・0026 のトリガーの nextval (呼び手の権限)
  for (const f of REGISTER_EXECUTE) s.push(`grant execute on function ${f} to ${e}`);
  s.push(`grant usage on schema ops to ${o}`);
  for (const t of OPS_SELECT) s.push(`grant select on ${t} to ${o}`);
  for (const f of OPS_EXECUTE) s.push(`grant execute on function ${f} to ${o}`);
  // 渡さないものを念のため外す (表の DML は持ち主と security definer の関数だけ)
  for (const r of [e, o]) {
    s.push(`revoke insert, update, delete, truncate on ops.master_registrations, ops.master_registration_events, ops.master_registration_backfill from ${r}`);
    s.push(`revoke execute on function ops.quarantine_unregistered_skus(text) from ${r}`);
  }
  s.push(`revoke execute on function ${OPS_EXECUTE[1]}, ${OPS_EXECUTE[2]} from ${e}`);
  return s;
}
