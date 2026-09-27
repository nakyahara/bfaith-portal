-- 0036: 代表関係 (親子) の帰属と DB の守り (2026-09-27。Company DB構想 10 §6.1.1 D3 の契約 v3。Codex D3-R0 = High 3 / Medium 4・D3-R1 = High 1 / Medium 3)
--
-- なぜ: 代表関係 (色違い・サイズ違いの子の商品 → 親の商品 = 名札) は夜間ロードが NE の代表商品コードから**付けるだけ**で、NE で外れても外れない。
--   外せるようにするには「誰がその親子を決めたか」が要る (人が決めた親子・帰属の分からない親子を夜間ロードが付け替えたり外したりしない)。
-- なにを:
--   1. core.products.parent_set_by (load / manual / null) = 今の親子を誰が決めたか
--        親あり × load   = 夜間ロードが NE の代表から付けた (付け替え・外してよい)
--        親あり × manual = 人が付けた (触らない)          親なし × manual = 人が外した (付けない)
--        親あり × null   = 帰属が不明 (触らない = 付け替えも外しもしない)   親なし × null = 親なし (付ける)
--      🚨 「帰属 load なら親あり」の CHECK は付けない: バックアップの復元は親 (自己参照) を一旦 null で入れて後で埋める
--   2. 既存の親の帰属 (backfill) は**行ごとの証拠だけ**: 変更の記録 (0026) で、その子の親の最後の変更が夜間ロード (company_db_load) で
--      new_value = 今の親の行だけ load。残り (記録の前 = 2026-09-10〜09-24 に付いた親) は null (保護)。
--      「他の口の記録が無い」「今の NE と一致する」は証拠にしない (Codex D3-R0 H2)。残りを load に移すかは中原さんの明示の判断で別に行う
--   3. DB の守り (trigger): parent_product_id / parent_set_by を変える取引は ① 約束の印 core.parent_protocol = '1' と
--      ② 親子の鍵 (pg_advisory_xact_lock(core.parent_lock_key()) の排他) の両方が要る。無ければ例外 parent_protocol_required。
--      = 0036 の後に古いコードの夜間ロードが走っても、保護した親を黙って付け替えられない (取引ごと失敗して知らせる。Codex D3-R0 H3)。
--      これからの書き手 (ポータルの付け外し) も鍵を取らないと書けない = 直列化を DB が強制する (D-R1)。
--      🚨 書き手は取引の鍵 (xact) を使い、**商品の行を更新・ロックする前**に取る (鍵 → 行の順。Codex D3-R1 M2)。
--         DB が確かめるのは「この接続が今、固定の鍵を排他で持っている」ことまで (取引の鍵と接続の鍵は pg_locks で見分けられない。R1 M3)
--      バックアップの復元は user trigger を止めて戻す (dump.mjs) ので当たらない
--   4. ops.load_decisions の section に variation_parents (夜間ロードの親子の判断と保持状態。照合の ① が確かめる)

alter table core.products add column parent_set_by text check (parent_set_by in ('load', 'manual'));
comment on column core.products.parent_set_by is '今の親子 (parent_product_id) を誰が決めたか: load = 夜間ロードが NE の代表から / manual = 人 (親なし × manual = 人が外した) / null = 帰属が不明 (親あり) または親なし。0036';

-- 親子の鍵 (固定の bigint。classid = 1・objid = 410342739)
create function core.parent_lock_key() returns bigint language sql immutable as $$ select 4705310036::bigint $$;

-- この接続が今、親子の鍵を排他で持っているか (bigint の形 = objsubid 1・今の DB・この接続・取れている・ExclusiveLock。共有の鍵・整数 2 つの形・別の鍵・ほかの接続の鍵は数えない)
create function core.holds_parent_lock() returns boolean language sql stable as $$
  select exists (
    select 1 from pg_catalog.pg_locks l
    where l.locktype = 'advisory'
      and l.database = (select d.oid from pg_catalog.pg_database d where d.datname = pg_catalog.current_database())
      and l.pid = pg_catalog.pg_backend_pid()
      and l.granted
      and l.mode = 'ExclusiveLock'
      and l.objsubid = 1
      and ((l.classid::bigint << 32) | l.objid::bigint) = core.parent_lock_key())
$$;

create function core.guard_product_parent() returns trigger language plpgsql as $$
begin
  if tg_op = 'UPDATE' and new.parent_product_id is not distinct from old.parent_product_id and new.parent_set_by is not distinct from old.parent_set_by then
    return new;   -- 親子が変わらない UPDATE (名前・状態など) は見ない
  end if;
  if tg_op = 'INSERT' and new.parent_product_id is null and new.parent_set_by is null then
    return new;   -- 親も帰属も無い行の追加 (夜間ロードが作る商品・名札) は見ない
  end if;
  -- 🚨 IS DISTINCT FROM で比べる (未設定の NULL も拒む。Codex D3-R1 M3)
  if pg_catalog.current_setting('core.parent_protocol', true) is distinct from '1' or not core.holds_parent_lock() then
    raise exception 'parent_protocol_required: 親子 (parent_product_id / parent_set_by) を変える取引は set_config(''core.parent_protocol'', ''1'', true) と pg_advisory_xact_lock(core.parent_lock_key()) が要る (product %)', new.product_id
      using errcode = 'P0001';
  end if;
  return new;
end $$;

create trigger trg_products_parent_guard_upd before update of parent_product_id, parent_set_by on core.products
  for each row execute function core.guard_product_parent();
create trigger trg_products_parent_guard_ins before insert on core.products
  for each row execute function core.guard_product_parent();

-- 既存の親の帰属 (行ごとの証拠だけ)。この migration も印と鍵を付けて書く (変更の記録に source_system = migration_0036 が残る)
select pg_catalog.set_config('core.parent_protocol', '1', true), pg_catalog.pg_advisory_xact_lock(core.parent_lock_key());
select pg_catalog.set_config('core.actor_type', 'system', true), pg_catalog.set_config('core.source_system', 'migration_0036', true),
       pg_catalog.set_config('core.reason', 'D3: 帰属の backfill (変更の記録で、夜間ロードが今の親を付け、その後に誰も変えていないと証明できる親子だけ load)', true);
with ev as (
  select e.entity_id as product_id, e.source_system,
         case when e.operation = 'UPDATE' then e.new_value else e.new_value -> 'parent_product_id' end as new_parent,
         row_number() over (partition by e.entity_id order by e.event_id desc) as rn
  from events.master_change_events e
  where e.entity_type = 'product' and e.entity_id is not null
    and ((e.operation = 'UPDATE' and e.attribute = 'parent_product_id')
      or (e.operation = 'INSERT' and e.new_value ? 'parent_product_id' and e.new_value -> 'parent_product_id' <> 'null'::jsonb))
)
update core.products p set parent_set_by = 'load'
from ev
where ev.product_id = p.product_id and ev.rn = 1
  and ev.source_system = 'company_db_load'
  and p.parent_product_id is not null
  and jsonb_typeof(ev.new_parent) = 'number' and (ev.new_parent #>> '{}')::bigint = p.parent_product_id;

-- 夜間ロードの判断の記録に親子の section を足す
alter table ops.load_decisions drop constraint load_decisions_section_check;
alter table ops.load_decisions add constraint load_decisions_section_check
  check (section in ('skus', 'sku_costs', 'set_components', 'primary_suppliers', 'variation_parents'));
