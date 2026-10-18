-- 0087 (0084 = メーカー #1739・0085 = 入出庫の履歴 #1741・0086 = 販売数のグラフ #1742 の次。migrate は欠番を断る): 商品 (単品) の梱包した状態の 縦・横・高さ・重さ (中原さん 2026-10-17)
--   中原さん「マスタの入力に 梱包した状態の 縦・横・高さ・重さ を入れたい。登録の時は必須にしない。
--             この情報は FBA 納品のアプリとか、定形外の発送方法で情報をもっていたはずだから集約して」
--
-- 考え方 (0003 の属性の観測と解決に合わせる):
--   - 出どころの値 (FBA 納品の重さ・定形外の重さと厚み・Amazon のカタログ…) = core.product_physicals (0003・scope = package) の行のまま。
--     夜間ロード (apps/company-db/load/engine.mjs の 11.) が postage.db / fba-box.db から毎晩足している表。ここでは変えない (表も trigger も)
--   - 人が入れた値 = 🆕 core.product_packages (商品ごとに 1 行・軸ごとに空でよい)。夜間ロードは触らない
--     🚨 core.product_physicals に人の行を足さない: 夜間ロードが触った商品の有効行 (is_effective) を規則 v1 で選び直す = 規則に無い出どころの行は有効でなくなる・
--        規則の版 v1 は使い始めたので規則を足せない (0003 の trg_rules_used_version)
--   - まとめた値 = 🆕 core.v_product_packages (軸ごとに 人 → 出どころの順位 → 新しい順 で 1 つ)。FBA 納品・定形外・利益の計算はこれを読めばよい
--   - 出どころの順位 = core.package_source_rank (人 0・実測 1・Amazon のカタログ 2・Yahoo 3・仕入先 4・そのほか 5・定形外の見込み 8)。
--     実測 1・Amazon 2・Yahoo 3 は 0008 の規則 v1 (package_weight_g) と同じ並び。規則に無い出どころは画面の参考のために後ろに並べる
--   - 単品だけ (セットは構成品から = 持たない)。登録で必須にしない (「値か入力なし」にも数えない)
-- 単位: 縦・横・高さ = mm (整数・画面は cm で小数 1 けた)・重さ = g (整数)。0003 の core.product_physicals と同じ
--
-- なにを:
--   1. core.product_packages = 人が入れた今の値 (1 行 = 1 商品・無い = 人の値なし・4 つとも空の行は持たない)
--   2. ops.product_package_events = 入れた・変えた・外したの記録 (追記だけ・request_id と商品で一意)
--   3. ops.set_product_package(...) = 人の値を入れる / 変える / 外す (新商品の登録・まとまりの登録 = 登録と同じ取引 / 商品の画面 = 保存と同じ取引)。
--      守りはメーカー (0084 ops.set_product_maker) と同じ: 単品・会社 1・やめていない・切替の段階 new_open・門を通った保存 / 登録の取引の中・
--      記録の request_id・人 = 約束 (保存 = ops.begin_master_write・登録 = その SKU の sku_create) と同じ・今の値が画面の見ていたもの (最後の記録の番号) と違う = version_conflict
--   4. core.package_source_rank(出どころ) と core.v_product_packages (まとめた値)
-- 🚨 既にある表・関数は変えない (表・関数・ビューを足すだけ)。security definer = search_path = pg_catalog, pg_temp・public の実行権を外す。
--    画面のロール (master_edit) の権限 (表・ビュー・core.product_physicals の select・書く関数の実行) は scripts/company-db/create-master-edit-roles.mjs を流し直して付ける
--    (流すまで画面は欄を出さない・保存に値が来たら 503 db_not_ready)

-- ═══════════ 1. 人の値・記録 ═══════════
create table core.product_packages (
  product_id bigint primary key references core.products,
  company_id smallint not null default 1 references core.companies,
  length_mm  integer check (length_mm between 1 and 3000),
  width_mm   integer check (width_mm between 1 and 3000),
  height_mm  integer check (height_mm between 1 and 3000),
  weight_g   integer check (weight_g between 1 and 100000),
  set_by     text not null,
  set_via    text not null check (set_via in ('register', 'variation', 'sku_page')),
  request_id uuid not null,
  set_at     timestamptz not null default now(),
  constraint ck_product_packages_any check (length_mm is not null or width_mm is not null or height_mm is not null or weight_g is not null),
  foreign key (company_id, product_id) references core.products (company_id, product_id)
);
comment on table core.product_packages is '🆕 0087: 商品 (単品の商品の行) の梱包した状態の 縦・横・高さ (mm)・重さ (g) の人の値 (行が無い = 人の値なし)。夜間ロードは触らない。書くのは ops.set_product_package だけ。出どころの値は core.product_physicals・まとめた値は core.v_product_packages';

create table ops.product_package_events (
  event_id   bigint generated always as identity primary key,
  company_id smallint not null default 1 references core.companies,
  product_id bigint not null references core.products,
  sku_id     bigint not null references core.skus,
  from_value jsonb,   -- 前の人の値 { length_mm, width_mm, height_mm, weight_g } (無かった = null)
  to_value   jsonb,   -- 後の人の値 (外した = null)
  via        text not null check (via in ('register', 'variation', 'sku_page')),
  actor_id   text not null,
  request_id uuid not null,
  reason     text,
  db_user    text not null,
  created_at timestamptz not null default now(),
  constraint ck_ppe_changed check (from_value is distinct from to_value),
  unique (request_id, product_id)
);
create index ix_product_package_events_product on ops.product_package_events (product_id, event_id desc);
comment on table ops.product_package_events is '🆕 0087: 梱包の 縦・横・高さ・重さ の人の値を入れた・変えた・外した記録 (追記だけ・書くのは ops.set_product_package だけ)。商品の最後の記録の番号 = 画面が見ていた版';
revoke all on core.product_packages, ops.product_package_events from public;

-- 守り: 書くのは関数の中だけ (印 ops.package_protocol)・記録は追記だけ・TRUNCATE できない
create function ops.guard_package_tables() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_table_name = 'product_package_events' and tg_op <> 'INSERT' then
    raise exception 'product_package_events: 記録は変えない・消さない (追記だけ)' using errcode = '42501';
  end if;
  if coalesce(pg_catalog.current_setting('ops.package_protocol', true), '') is distinct from '1' then
    raise exception '%: 梱包の大きさ・重さの表は ops.set_product_package の中でだけ書く', tg_table_name using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function ops.guard_package_tables() from public;
create trigger trg_product_packages_guard before insert or update or delete on core.product_packages for each row execute function ops.guard_package_tables();
create trigger trg_product_package_events_guard before insert or update or delete on ops.product_package_events for each row execute function ops.guard_package_tables();
create trigger trg_product_packages_no_truncate before truncate on core.product_packages for each statement execute function core.reject_mutation();
create trigger trg_product_package_events_no_truncate before truncate on ops.product_package_events for each statement execute function core.reject_mutation();

-- ═══════════ 2. 人の値を入れる ═══════════
/**
 * p_length_mm / p_width_mm / p_height_mm / p_weight_g = 入れる人の値 (軸ごとに null = その軸は人の値なし・4 つとも null = 人の値を外す)・
 * p_seen_event_id = 画面が見ていた最後の記録の番号 (無い = null。新商品の登録 = いつも null)・p_via = register / variation / sku_page・p_reason = 記録に残す理由
 * 戻り値 = { ok, sku_id, product_id, from, to, changed, event_id }
 * 断る: invalid_input (形・範囲) / before_cutover / not_single (セット・例外・商品の行の無い SKU) / cancelled (登録をやめた) /
 *       master_write_session_required (門を通った保存・登録の取引の外) / version_conflict (今の人の値が画面の見ていたものと違う)
 */
create function ops.set_product_package(p_request_id uuid, p_actor_id text, p_sku_id bigint, p_length_mm integer, p_width_mm integer, p_height_mm integer, p_weight_g integer,
    p_seen_event_id bigint, p_via text, p_reason text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  s         record;
  v_cur     core.product_packages;
  v_last    bigint;
  v_from    jsonb;
  v_to      jsonb;
  v_event   bigint;
  v_sess    ops.master_write_sessions;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, p_reason) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール)・理由 (200 字まで) の形が違う' using errcode = '22023';
  end if;
  if p_via is null or p_via not in ('register', 'variation', 'sku_page') then raise exception 'invalid_input: どこから は register / variation / sku_page' using errcode = '22023'; end if;
  if p_via in ('register', 'variation') and p_seen_event_id is not null then raise exception 'invalid_input: 新商品の登録に「見ていた記録」は無い' using errcode = '22023'; end if;
  if (p_length_mm is not null and p_length_mm not between 1 and 3000) or (p_width_mm is not null and p_width_mm not between 1 and 3000)
     or (p_height_mm is not null and p_height_mm not between 1 and 3000) then
    raise exception 'invalid_input: 縦・横・高さは 1〜3000 mm (0.1〜300 cm)' using errcode = '22023';
  end if;
  if p_weight_g is not null and p_weight_g not between 1 and 100000 then raise exception 'invalid_input: 重さは 1〜100000 g' using errcode = '22023'; end if;
  if (select c.phase from ops.master_cutover_state c where c.id = 1) is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が new_open でない' using errcode = 'P0001';
  end if;
  select k.sku_id, k.company_id, k.sku_kind, k.product_id, r.state as reg into s
    from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if s.sku_id is null or s.company_id <> 1 then raise exception 'invalid_input: SKU % が Company DB に無い', p_sku_id using errcode = '22023'; end if;
  if s.sku_kind is distinct from 'single' or s.product_id is null then
    raise exception 'not_single: 梱包の大きさ・重さは単品の商品だけ (SKU %・セットは構成品から)', p_sku_id using errcode = '22023';
  end if;
  if s.reg = 'cancelled' then raise exception 'cancelled: 登録をやめた商品 (SKU %) には書かない', p_sku_id using errcode = '22023'; end if;
  -- 守り: 門を通った保存・登録の取引の中だけ (0084 のメーカーと同じ)。
  --   sku_page = この取引の今の約束 (ops.begin_master_write = sku_edit・この SKU) / register・variation = この取引でこの SKU を登録した約束 (sku_create) の行
  --   (約束の行がこの取引の中で作られたこと = created_at ≧ この取引の始まり も見る = バックアップから戻した DB で古い約束を今のものと見ない)
  if p_via = 'sku_page' then
    v_sess := ops.current_master_write_session();
    if v_sess.session_id is null or v_sess.operation is distinct from 'sku_edit' or v_sess.sku_id is distinct from p_sku_id
       or v_sess.request_id is distinct from p_request_id or v_sess.actor_id is distinct from p_actor_id then   -- 記録の request_id・人 = 保存の約束と同じ (#0087 Codex R1 M)
      raise exception 'master_write_session_required: 商品の画面の梱包の大きさ・重さは、同じ取引でこの SKU の保存 (ops.begin_master_write) を始めた後だけ' using errcode = '42501';
    end if;
  elsif not exists (select 1 from ops.master_write_sessions w where w.txid = pg_catalog.txid_current() and w.created_at >= pg_catalog.now() and w.operation = 'sku_create' and w.sku_id = p_sku_id
                      and w.request_id = p_request_id and w.actor_id = p_actor_id) then   -- 登録の約束の request_id (まとまり = 子の番号)・人と同じ (#0087 Codex R1 M)
    raise exception 'master_write_session_required: 新商品の梱包の大きさ・重さは、その SKU を登録した同じ取引の中だけ (ops.register_new_sku の後)' using errcode = '42501';
  end if;
  -- 鍵: SKU の鍵 → 商品の行 (商品の画面の保存・登録の取引はもう持っている = 同じ取引で取り直しても待たない)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  perform 1 from core.products p where p.product_id = s.product_id for update;
  v_last := (select max(e.event_id) from ops.product_package_events e where e.product_id = s.product_id);
  if v_last is distinct from p_seen_event_id then
    raise exception 'version_conflict: 画面を開いた後にこの商品の梱包の大きさ・重さが変わった (今の記録 %・開いたとき %)', coalesce(v_last::text, 'なし'), coalesce(p_seen_event_id::text, 'なし') using errcode = '40001';
  end if;
  select * into v_cur from core.product_packages x where x.product_id = s.product_id;
  v_from := case when v_cur.product_id is null then null else pg_catalog.jsonb_build_object('length_mm', v_cur.length_mm, 'width_mm', v_cur.width_mm, 'height_mm', v_cur.height_mm, 'weight_g', v_cur.weight_g) end;
  v_to := case when coalesce(p_length_mm, p_width_mm, p_height_mm, p_weight_g) is null then null
               else pg_catalog.jsonb_build_object('length_mm', p_length_mm, 'width_mm', p_width_mm, 'height_mm', p_height_mm, 'weight_g', p_weight_g) end;
  if v_from is not distinct from v_to then
    return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', s.product_id::text, 'from', v_from, 'to', v_to, 'changed', false, 'event_id', v_last::text);
  end if;
  perform pg_catalog.set_config('ops.package_protocol', '1', true);
  if v_to is null then
    delete from core.product_packages x where x.product_id = s.product_id;
  else
    insert into core.product_packages (product_id, company_id, length_mm, width_mm, height_mm, weight_g, set_by, set_via, request_id)
      values (s.product_id, 1, p_length_mm, p_width_mm, p_height_mm, p_weight_g, p_actor_id, p_via, p_request_id)
      on conflict (product_id) do update set length_mm = excluded.length_mm, width_mm = excluded.width_mm, height_mm = excluded.height_mm, weight_g = excluded.weight_g,
        set_by = excluded.set_by, set_via = excluded.set_via, request_id = excluded.request_id, set_at = pg_catalog.now();
  end if;
  insert into ops.product_package_events (company_id, product_id, sku_id, from_value, to_value, via, actor_id, request_id, reason, db_user)
    values (1, s.product_id, p_sku_id, v_from, v_to, p_via, p_actor_id, p_request_id, p_reason, v_db_user) returning event_id into v_event;
  perform pg_catalog.set_config('ops.package_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'product_id', s.product_id::text, 'from', v_from, 'to', v_to, 'changed', true, 'event_id', v_event::text);
end $$;
revoke all on function ops.set_product_package(uuid, text, bigint, integer, integer, integer, integer, bigint, text, text) from public;

-- ═══════════ 3. まとめた値 ═══════════
/**
 * 出どころの順位 (小さいほど先)。人 (core.product_packages) はいつも 0 = いちばん先。lib/master-package.mjs の SOURCE_RANK と同じ (試験で照らす)
 *   measured = 実測 (定形外の pm_skus の実測・FBA 納品の実測)・amazon_catalog = Amazon のカタログ (FBA 納品の重さの参照・SP-API)・yahoo・
 *   supplier = 仕入先の値 (定形外の pm_skus)・postage_estimate = 定形外の見込み (測っていない)。知らない出どころ = 5
 */
create function core.package_source_rank(p_source text) returns smallint language sql immutable set search_path = pg_catalog, pg_temp as $$
  select (case p_source when 'human' then 0 when 'measured' then 1 when 'amazon_catalog' then 2 when 'yahoo' then 3 when 'supplier' then 4
                        when 'postage_estimate' then 8 else 5 end)::smallint
$$;
-- 🚨 public の実行権は外さない: ビューの中の関数は読む人の権限で呼ばれる (外すと core.v_product_packages を読めない)。値を返すだけの部品 (表を読まない・書かない)

/**
 * 商品ごとのまとめた 梱包の 縦・横・高さ (mm)・重さ (g)。軸ごとに 1 つ: 人の値 → 出どころの順位 → 観測の新しい順 → 行の番号の大きい順。
 *   出どころの値 = core.product_physicals (scope = package) の、出どころ × 参照 (source_ref) ごとの最新の行 (先の時刻の行は使わない = 夜間ロードと同じ)。
 *   *_source = human / 出どころ (source_system)・*_ref = 参照 (source_ref)・*_at = 観測の時刻 (人 = 入れた時刻)
 *   🚨 security_invoker にしない = 読む人には core.product_physicals の権限が無くてもよい (このビューの select だけ渡す)
 */
create view core.v_product_packages as
with latest as not materialized (
  select distinct on (ph.product_id, ph.source_system, coalesce(ph.source_ref, ''))
         ph.product_id, ph.source_system, ph.source_ref, ph.length_mm, ph.width_mm, ph.height_mm, ph.weight_g, ph.observed_at, ph.product_physical_id
    from core.product_physicals ph
   where ph.scope = 'package' and ph.observed_at <= pg_catalog.now()
   order by ph.product_id, ph.source_system, coalesce(ph.source_ref, ''), ph.observed_at desc, ph.product_physical_id desc
), cand as not materialized (
  -- 軸ごとに縦に並べる (出どころの行も人の行も 1 回だけ読む = 商品の絞り込みが元の表まで届く・#0087 Codex R1 M)
  select l.product_id, x.axis, x.v, l.source_system as src, l.source_ref as ref, l.observed_at as at, l.product_physical_id as id
    from latest l cross join lateral (values ('length_mm'::text, l.length_mm), ('width_mm', l.width_mm), ('height_mm', l.height_mm), ('weight_g', l.weight_g)) as x(axis, v)
   where x.v is not null
  union all
  select h.product_id, x.axis, x.v, 'human', null, h.set_at, null
    from core.product_packages h cross join lateral (values ('length_mm'::text, h.length_mm), ('width_mm', h.width_mm), ('height_mm', h.height_mm), ('weight_g', h.weight_g)) as x(axis, v)
   where x.v is not null
), win as not materialized (
  select distinct on (c.product_id, c.axis) c.product_id, c.axis, c.v, c.src, c.ref, c.at
    from cand c
   order by c.product_id, c.axis, core.package_source_rank(c.src), c.at desc, c.id desc nulls first
)
select w.product_id,
       max(w.v) filter (where w.axis = 'length_mm')::integer as length_mm, max(w.src) filter (where w.axis = 'length_mm') as length_source,
       max(w.ref) filter (where w.axis = 'length_mm') as length_ref, max(w.at) filter (where w.axis = 'length_mm') as length_at,
       max(w.v) filter (where w.axis = 'width_mm')::integer as width_mm, max(w.src) filter (where w.axis = 'width_mm') as width_source,
       max(w.ref) filter (where w.axis = 'width_mm') as width_ref, max(w.at) filter (where w.axis = 'width_mm') as width_at,
       max(w.v) filter (where w.axis = 'height_mm')::integer as height_mm, max(w.src) filter (where w.axis = 'height_mm') as height_source,
       max(w.ref) filter (where w.axis = 'height_mm') as height_ref, max(w.at) filter (where w.axis = 'height_mm') as height_at,
       max(w.v) filter (where w.axis = 'weight_g')::integer as weight_g, max(w.src) filter (where w.axis = 'weight_g') as weight_source,
       max(w.ref) filter (where w.axis = 'weight_g') as weight_ref, max(w.at) filter (where w.axis = 'weight_g') as weight_at
  from win w
 group by w.product_id;
comment on view core.v_product_packages is '🆕 0087: 商品ごとのまとめた梱包の 縦・横・高さ (mm)・重さ (g)。軸ごとに 人 → 出どころの順位 (core.package_source_rank) → 新しい順。FBA 納品・定形外・利益の計算はここを読む';
revoke all on core.v_product_packages from public;
