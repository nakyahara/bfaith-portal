-- 0050: マスタ入力画面の土台 (2026-09-30。Company DB構想 14「マスタ入力画面」§3・§6 ⑤-1 / 10 §2・§8。Codex 設計レビュー ⑤-R0・R1 の契約)
--
-- なぜ: 既にある商品・セットの値を Company DB で人が直す画面 (apps/master-edit・Render だけ) を作る。書く先は Company DB だけ。
--   変更の記録と version は 0026 のトリガーが書く。ここで足すのは「切替の段階」「セットに置く列」「保存 1 回の記録」「セットの構成の依頼」「原価の期間の重なりの守り」。
-- なにを:
--   1. ops.master_cutover_state (1 行) = 切替の段階。legacy_open (今 = 古い入口が正) → frozen (古い入口を止めた) → company_owner (持ち主が Company DB) → new_open (新しい画面で保存できる)。
--      一方向・1 段ずつだけ (ops.set_master_cutover_phase = 鍵 → 行 → 記録)。変えた記録は ops.master_cutover_events (追記だけ)。
--      🚨 読めない = 閉じている (fail-closed。lib/master-cutover.mjs)。新しい画面の保存 = 段階 new_open **かつ** 持ち主表の列が company **かつ** env MASTER_EDIT_OPEN = 1
--   2. core.skus.set_sales_class_override (1〜4・null) = セットの売上分類を人が決めた値 (導けないときだけ画面が許す)。セットは products の行を持たない (0002:70) ので SKU に置く
--      core.skus.handling_own ('active' / 'discontinued'・null) = セット自身の取扱区分。skus.handling は「セット自身 + 構成品」から導いた値。null = まだ人が決めていない
--      🚨 sku_kind = 'set' だけ、の CHECK は付けない: 夜間ロードが NE の種類替えを写したときに 1 行の CHECK で夜間ロード全体を止めない
--   3. ops.master_edit_requests = 保存 1 回 = 1 行 (request_id = 画面が作る UUID)。🚨 保存と同じ取引で done (結果つき) を書く = 「処理中」のまま残る行は無い。
--      巻き戻った保存は取引の後に failed (誤りつき) を残す。追記だけ。同じ request_id + 同じ中身 = 残した結果・誤りを返す / 中身が違う = 409
--   4. ops.sku_component_requests = セットの構成を変えたい依頼。core.sku_components = 最後に確かめた構成 (画面は書かない)。セットごとに開いている依頼は 1 つ
--      (新しい依頼は同じ取引で前の依頼を閉じる)。閉じ方 = applied (NE の構成の観測が依頼と同じ = 構成品・数量・並び・行の数まで。lib/master-write.mjs の
--      promoteComponentRequest が同じ取引で core.sku_components へ上げ・導く値を計算し直し・依頼を閉じる) / cancelled (取り下げ・置き換え)
--   5. core.sku_costs の期間の重なりの守り (この画面と構成の依頼の昇格 = source_system が portal_master_edit / ne_observation の書き込みだけ)。
--      期間は今までどおり両端を含む [valid_from, valid_to] (0049 の読み手も同じ)。
--      🚨 全部の書き手には掛けない: 今の夜間ロードは同じ日に 2 回流すと「今日始まった行を今日で閉じる」= 1 日重なる (⑤-2 で直す)・0049 の読み手は重なりを
--         前提に「その日を覆う 1 行」を選んでいる・試験の材料も重なりを作る = 表全体の排他制約 (exclusion) にすると今の夜間ロードと試験が止まる
-- 🚨 この migration は商品の値を何も変えない (列は全部 null で足す・切替の段階は legacy_open から)

-- 1. 切替の段階
create table ops.master_cutover_state (
  id          smallint primary key default 1 check (id = 1),
  phase       text not null check (phase in ('legacy_open', 'frozen', 'company_owner', 'new_open')),
  changed_at  timestamptz not null default now(),
  changed_by  text not null check (length(changed_by) > 0),
  note        text check (note is null or length(note) <= 500)
);
insert into ops.master_cutover_state (id, phase, changed_by, note) values (1, 'legacy_open', 'migration_0050', '最初 = 古い入口 (NE・/register) が正');
comment on table ops.master_cutover_state is '商品マスタの切替の段階 (0050)。legacy_open → frozen → company_owner → new_open の一方向。変えるのは ops.set_master_cutover_phase だけ';

create table ops.master_cutover_events (
  event_id    bigint generated always as identity primary key,
  from_phase  text not null,
  to_phase    text not null,
  actor       text not null check (length(actor) > 0),
  note        text check (note is null or length(note) <= 500),
  changed_at  timestamptz not null default now()
);
select core.make_append_only('ops', 'master_cutover_events');

create function ops.guard_master_cutover_state() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'master_cutover_state は消さない' using errcode = 'P0001'; end if;
  if coalesce(pg_catalog.current_setting('ops.cutover_protocol', true), '') is distinct from '1' then
    raise exception '切替の段階は ops.set_master_cutover_phase でだけ変える' using errcode = 'P0001';
  end if;
  return new;
end $$;
create trigger trg_master_cutover_state_guard before update or delete on ops.master_cutover_state
  for each row execute function ops.guard_master_cutover_state();

-- 1 段だけ進める (飛ばさない・戻さない)。鍵 → 行 (for update) → 記録 → 状態
create function ops.set_master_cutover_phase(p_to text, p_actor text, p_note text default null) returns jsonb
  language plpgsql as $$
declare
  v_order text[] := array['legacy_open', 'frozen', 'company_owner', 'new_open'];
  v_from  text;
begin
  perform pg_advisory_xact_lock(hashtext('ops.master_cutover'));
  if p_actor is null or length(p_actor) = 0 then raise exception 'invalid_input: 誰が変えたか (actor) が要る' using errcode = '22023'; end if;
  if not (p_to = any(v_order)) then raise exception 'invalid_input: 知らない段階 %', p_to using errcode = '22023'; end if;
  select phase into v_from from ops.master_cutover_state where id = 1 for update;
  if not found then raise exception 'no_state: 切替の段階の行が無い' using errcode = 'P0001'; end if;
  if array_position(v_order, p_to) <> array_position(v_order, v_from) + 1 then
    raise exception 'one_way: 段階は % から % に進めない (1 段ずつ・戻さない)', v_from, p_to using errcode = 'P0001';
  end if;
  perform pg_catalog.set_config('ops.cutover_protocol', '1', true);
  update ops.master_cutover_state set phase = p_to, changed_at = now(), changed_by = p_actor, note = p_note where id = 1;
  perform pg_catalog.set_config('ops.cutover_protocol', '', true);
  insert into ops.master_cutover_events (from_phase, to_phase, actor, note) values (v_from, p_to, p_actor, p_note);
  return jsonb_build_object('from', v_from, 'to', p_to);
end $$;
revoke all on function ops.set_master_cutover_phase(text, text, text) from public;

-- 2. セットに置く列
alter table core.skus
  add column set_sales_class_override smallint check (set_sales_class_override between 1 and 4),
  add column handling_own text check (handling_own in ('active', 'discontinued'));
comment on column core.skus.set_sales_class_override is 'セットの売上分類を人が決めた値 (1〜4)。null = 構成品から導く (lib/master-set-rules.js)。0050';
comment on column core.skus.handling_own is 'セット自身の取扱区分 (人が決めた値)。skus.handling はセット自身 + 構成品から導いた値。null = まだ人が決めていない。0050';

-- 3. 保存 1 回の記録 (追記だけ)
create table ops.master_edit_requests (
  request_id   uuid primary key,
  company_id   smallint not null default 1 references core.companies,
  operation    text not null check (operation in ('sku_edit')),
  target_code  text not null check (length(target_code) between 1 and 60),
  sku_id       bigint references core.skus (sku_id),
  actor_id     text not null check (length(actor_id) between 1 and 320),
  payload_hash text not null check (payload_hash ~ '^[0-9a-f]{64}$'),
  status       text not null check (status in ('done', 'failed')),
  result       jsonb check (result is null or jsonb_typeof(result) = 'object'),
  error        jsonb check (error is null or jsonb_typeof(error) = 'object'),
  started_at   timestamptz not null,
  finished_at  timestamptz not null default clock_timestamp(),
  constraint ck_mer_done check ((status = 'done') = (result is not null)),
  constraint ck_mer_failed check ((status = 'failed') = (error is not null)),
  constraint ck_mer_time check (finished_at >= started_at)
);
create index ix_master_edit_requests_sku on ops.master_edit_requests (sku_id, started_at desc);
select core.make_append_only('ops', 'master_edit_requests');
comment on table ops.master_edit_requests is 'マスタ入力画面の保存 1 回 = 1 行 (0050)。done は保存と同じ取引・failed は巻き戻った後。追記だけ';

-- 4. セットの構成の依頼
create table ops.sku_component_requests (
  component_request_id bigint generated always as identity primary key,
  company_id      smallint not null default 1 references core.companies,
  set_sku_id      bigint not null references core.skus (sku_id),
  rows            jsonb not null check (jsonb_typeof(rows) = 'array' and jsonb_array_length(rows) between 1 and 20),   -- [{sku_id, code, qty, sort}] sort = 1〜n
  rows_hash       text not null check (rows_hash ~ '^[0-9a-f]{64}$'),
  base_rows       jsonb not null check (jsonb_typeof(base_rows) = 'array'),   -- 依頼したときの core.sku_components [{sku_id, code, qty, sort}]
  reason          text check (reason is null or length(reason) <= 200),
  requested_by    text not null check (length(requested_by) > 0),
  edit_request_id uuid not null,   -- ops.master_edit_requests の request_id (同じ取引の最後に書くので FK は付けない)
  status          text not null default 'open' check (status in ('open', 'applied', 'cancelled')),
  created_at      timestamptz not null default now(),
  closed_at       timestamptz,
  closed_by       text,
  close_reason    text check (close_reason in ('matched', 'withdrawn', 'superseded')),
  applied_run     text,          -- 上げた NE の観測の回
  applied_observed_at timestamptz,   -- 上げた NE の観測の時刻 (依頼より後)
  constraint ck_scr_open check ((status = 'open') = (closed_at is null) and (status = 'open') = (close_reason is null) and (status = 'open') = (closed_by is null)),
  constraint ck_scr_applied check ((status = 'applied') = (close_reason = 'matched') and (status = 'applied') = (applied_observed_at is not null)),
  constraint ck_scr_applied_after check (applied_observed_at is null or applied_observed_at > created_at)
);
create unique index ux_sku_component_requests_open on ops.sku_component_requests (set_sku_id) where status = 'open';
create index ix_sku_component_requests_set on ops.sku_component_requests (set_sku_id, created_at desc);
comment on table ops.sku_component_requests is 'セットの構成を変えたい依頼 (0050)。画面は core.sku_components を書かない。NE の観測が同じになったら lib/master-write.mjs の promoteComponentRequest が上げて閉じる';

create function ops.guard_sku_component_requests() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'sku_component_requests は消さない (取り下げは cancelled)' using errcode = 'P0001'; end if;
  if (new.component_request_id, new.company_id, new.set_sku_id, new.rows, new.rows_hash, new.base_rows, new.reason, new.requested_by, new.edit_request_id, new.created_at)
     is distinct from (old.component_request_id, old.company_id, old.set_sku_id, old.rows, old.rows_hash, old.base_rows, old.reason, old.requested_by, old.edit_request_id, old.created_at) then
    raise exception 'sku_component_requests の依頼の中身は書き換えない (閉じて新しく依頼する)' using errcode = 'P0001';
  end if;
  if old.status <> 'open' then raise exception '閉じた依頼 (%) は変えない', old.status using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_sku_component_requests_guard before update or delete on ops.sku_component_requests
  for each row execute function ops.guard_sku_component_requests();

-- 5. 原価の期間の重なり (両端を含む [valid_from, valid_to]。valid_to = null = ずっと)。この画面と昇格の書き込みだけ見る (上の 🚨)
create function core.guard_sku_cost_overlap() returns trigger language plpgsql as $$
begin
  if coalesce(pg_catalog.current_setting('core.source_system', true), '') not in ('portal_master_edit', 'ne_observation') then return null; end if;
  if exists (select 1 from core.sku_costs o
              where o.sku_id = new.sku_id and o.sku_cost_id <> new.sku_cost_id
                and o.valid_from <= coalesce(new.valid_to, 'infinity'::date) and new.valid_from <= coalesce(o.valid_to, 'infinity'::date)) then
    raise exception 'sku_cost_overlap: SKU % の原価の期間が重なる (% 〜 %)', new.sku_id, new.valid_from, coalesce(new.valid_to::text, '') using errcode = '23P01';
  end if;
  return null;
end $$;
create trigger trg_sku_costs_no_overlap after insert or update of sku_id, valid_from, valid_to on core.sku_costs
  for each row execute function core.guard_sku_cost_overlap();

do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_cutover_state, ops.master_cutover_events, ops.master_edit_requests, ops.sku_component_requests to watcher';
  end if;
end $$;
