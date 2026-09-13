-- 0014 発注 (Phase 7。08 §5。D6)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/08_残りドメインのテーブル設計_20260913.md』§5 (草案 v4 = Codex 3 巡 + PR のレビュー)。
--   元 = 発注管理アプリの台帳 (apps/purchase-orders/db.js。warehouse-mirror.db の po_orders / po_order_items / po_item_events)。D-9 = a (NE は正本のまま。
--   2026-07-13 以降の発注はこのアプリで行い、注残の正本 = po_* 台帳)。Company DB は同じ形で持ち、夜間の loader が mirror から直接読む (取込は次の PR)。
--   🚨 元の台帳と「同じ列・同じ規則・同じ式」で持つ (突合できるように):
--     - ヘッダ: status は draft / issued の 2 値、閉鎖は closed_at (null = オープン。再オープンで null)、po_number は発行時に採番 (後から付くので鍵にしない)、
--       追跡の境界 (po_settings.tracking_started_at) より前に発行した PO は legacy = 残数に意味が無い → is_tracked (loader が issued_at >= 境界 で決める)、
--       origin = migration (NE 発注残の移行 PO。ne_slip_no と send_blocked が必須) / supplement (確定後の追加分。parent がある)
--     - 明細: qty > 0、分納の次回予定 (remainder_disposition = awaiting_delivery ⇔ next_expected_date + qty / awaiting_confirmation ⇔ next_action_date / null ⇔ 全部 null)
--     - イベント (append-only): 4 種 (receipt / shortage / cancel / reversal) と元の CHECK を移植。残数超過は trigger で拒む (明細を for update)。逆仕訳は一致だけ・1 回だけ。
--       legacy の PO・draft の PO にはイベントを入れない (元も拒む)
--     - mart.v_purchase_order_open = 元の v_po_item_balance と同じ式 (received / shortage / cancelled / cutoff / remaining)
--     - mart.v_purchase_backorder_by_sku = 元の v_ledger_backorder_by_product と同じ条件 (issued・tracked・open・残 > 0)
-- 🚨 0004〜0013 の表には触らない。

-- ─── ヘッダ (発注 1 件 = po_orders 1 行) ───
create table core.purchase_orders (
  purchase_order_id  bigint generated always as identity primary key,
  company_id         smallint not null references core.companies,
  source_ref         text not null,                   -- 'po_orders:<id>'
  po_number          text,                            -- 発行時に採番 'PO-YYYY-NNNN' (後から付くので鍵にしない)
  supplier_id        bigint,                          -- core.suppliers (コードで解決。当たらなければ null のまま = DQ)
  supplier_code      text not null,                   -- 元の仕入先コード (解決できなくても残す)
  supplier_name      text,
  status             text not null check (status in ('draft','issued')),
  issued_at          timestamptz,
  closed_at          timestamptz,                     -- null = オープン。再オープンで null
  is_tracked         boolean not null default false,  -- issued_at >= 追跡の境界 (po_settings.tracking_started_at)。false = legacy (残数に意味が無い)
  origin             text check (origin is null or origin in ('migration','supplement')),
  send_blocked       boolean not null default false,  -- メール・発注書出力の対象外 (移行 PO の誤発注防止)
  ne_slip_no         text,                            -- NE 発注伝票番号 (移行 PO の冪等キー)
  parent_purchase_order_id bigint,                    -- 追加発注 (origin = supplement) の元 PO
  requested_date     date,                            -- 希望納期 (ヘッダ)
  pml_as_of_date     date,
  note               text,
  source_updated_at  timestamptz not null,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (company_id, source_ref),
  unique (company_id, purchase_order_id),
  foreign key (company_id, supplier_id) references core.suppliers (company_id, supplier_id),
  foreign key (company_id, parent_purchase_order_id) references core.purchase_orders (company_id, purchase_order_id),
  constraint ck_po_issued_at check ((status = 'issued') = (issued_at is not null)),
  constraint ck_po_closed_only_issued check (closed_at is null or status = 'issued'),
  constraint ck_po_tracked_only_issued check (not is_tracked or status = 'issued'),
  constraint ck_po_migration_attrs check (origin is distinct from 'migration' or (nullif(trim(ne_slip_no), '') is not null and send_blocked)),   -- 元の origin rules
  constraint ck_po_parent_is_supplement check (parent_purchase_order_id is null or origin is not distinct from 'supplement'),   -- 🚨 origin が null のとき CHECK が null で通らないよう is not distinct from
  constraint ck_po_not_own_parent check (parent_purchase_order_id is null or parent_purchase_order_id <> purchase_order_id)
);
create unique index ux_purchase_orders_po_number on core.purchase_orders (company_id, po_number) where po_number is not null;
create unique index ux_purchase_orders_ne_slip on core.purchase_orders (company_id, ne_slip_no) where ne_slip_no is not null;
create index ix_purchase_orders_supplier on core.purchase_orders (company_id, supplier_id, status);
create index ix_purchase_orders_open on core.purchase_orders (company_id, issued_at desc) where status = 'issued' and closed_at is null;
create trigger trg_purchase_orders_touch before update on core.purchase_orders for each row execute function core.touch_updated_at();

-- ─── 明細 (po_order_items 1 行) ───
create table core.purchase_order_lines (
  purchase_order_line_id bigint generated always as identity primary key,
  company_id         smallint not null references core.companies,
  purchase_order_id  bigint not null references core.purchase_orders on delete cascade,
  source_ref         text not null,                   -- 'po_order_items:<id>'
  product_key        text not null,                   -- 元の product_key (発注の単位。同じ PO に同じ鍵は 1 行)
  product_code       text not null,                   -- 元の product_code
  product_name       text,
  sku_id             bigint,                          -- core.skus (コードで解決)
  unresolved_code    text,                            -- 解決できなかった元のコード
  qty                integer not null check (qty > 0),
  unit_cost_jpy      bigint check (unit_cost_jpy >= 0),
  condition_id       text,                            -- 発注時点の発注条件グループ
  requested_date     date,                            -- 明細の希望納期 (確定時のスナップショット)
  promised_date      date,                            -- 仕入先の回答納期 (最新値)
  next_expected_date date,                            -- 分納の次回入荷予定日
  next_expected_qty  integer check (next_expected_qty > 0),
  next_action_date   date,                            -- 確認中の期限
  remainder_disposition text check (remainder_disposition is null or remainder_disposition in ('awaiting_delivery','awaiting_confirmation')),
  source_updated_at  timestamptz not null,
  created_at timestamptz not null default now(), created_by_type text not null default 'system', created_by_id text,
  updated_at timestamptz not null default now(),
  unique (company_id, source_ref),
  unique (purchase_order_id, product_key),
  unique (purchase_order_id, purchase_order_line_id),
  constraint ck_po_lines_resolved check (sku_id is not null or unresolved_code is not null),
  -- 元の trigger と同じ規則: 残数の扱いと次回予定は組で決まる
  -- 🚨 null になり得る列の比較は is not distinct from (= だと null で CHECK が通る)
  constraint ck_po_lines_disposition check (
       (remainder_disposition is null and next_expected_date is null and next_expected_qty is null and next_action_date is null)
    or (remainder_disposition is not distinct from 'awaiting_delivery' and next_expected_date is not null and next_expected_qty is not null and next_action_date is null)
    or (remainder_disposition is not distinct from 'awaiting_confirmation' and next_action_date is not null and next_expected_date is null and next_expected_qty is null)),
  foreign key (company_id, purchase_order_id) references core.purchase_orders (company_id, purchase_order_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id)
);
create index ix_po_lines_sku on core.purchase_order_lines (sku_id) where sku_id is not null;
create index ix_po_lines_unresolved on core.purchase_order_lines (company_id, unresolved_code) where unresolved_code is not null;
create trigger trg_purchase_order_lines_touch before update on core.purchase_order_lines for each row execute function core.touch_updated_at();

-- ─── 数量イベント (po_item_events 1 行。append-only) ───
create table events.purchase_order_events (
  event_id          bigint generated always as identity primary key,
  company_id        smallint not null references core.companies,
  occurred_at       timestamptz not null,             -- 元の recorded_at
  recorded_at       timestamptz not null default now(),
  actor_type        text not null check (actor_type in ('human','ai','system','external')),   -- 元 user → human / ai_agent → ai / system → system / migration → external
  actor_id          text,
  source_actor_type text check (source_actor_type is null or source_actor_type in ('user','system','ai_agent','migration')),   -- 元の値
  source_system     text not null,                    -- 'purchase_orders'
  source_ref        text,                             -- 'po_item_events:<id>'
  reason_code       text,
  reason_text       text,                             -- 元の note
  idempotency_key   text not null unique,             -- 'po_item_events:<id>'
  reverses_event_id bigint references events.purchase_order_events,
  payload           jsonb,
  purchase_order_id bigint not null,
  purchase_order_line_id bigint not null,
  event_type        text not null check (event_type in ('receipt','shortage','cancel','reversal')),
  qty               integer not null check (qty > 0),
  effective_date    date not null,
  receipt_source    text check (receipt_source is null or receipt_source in ('manual','logizard','migration')),
  inbound_ref       text,                             -- 'po_inbound_items:<id>' (logizard の入荷)
  foreign key (company_id, purchase_order_id) references core.purchase_orders (company_id, purchase_order_id),
  foreign key (purchase_order_id, purchase_order_line_id) references core.purchase_order_lines (purchase_order_id, purchase_order_line_id),
  -- 元の CHECK を移植 (null になり得る列の比較は is null で明示)
  constraint ck_po_events_reversal_ref        check ((event_type = 'reversal') = (reverses_event_id is not null)),
  constraint ck_po_events_receipt_source      check ((event_type = 'receipt') = (receipt_source is not null)),
  constraint ck_po_events_receipt_inbound     check (event_type <> 'receipt' or ((receipt_source = 'logizard') = (inbound_ref is not null))),
  constraint ck_po_events_non_receipt_inbound check (event_type = 'receipt' or inbound_ref is null),
  constraint ck_po_events_receipt_cancel_reason check (event_type not in ('receipt','cancel') or reason_code is null),
  constraint ck_po_events_shortage_reason     check (event_type <> 'shortage' or (reason_code is not null and reason_code in ('supplier_shortage','own_decision','cutoff','other'))),
  constraint ck_po_events_shortage_other      check (event_type <> 'shortage' or reason_code is distinct from 'other' or nullif(trim(reason_text), '') is not null),
  constraint ck_po_events_reversal_reason     check (event_type <> 'reversal' or (reason_code is not distinct from 'correction' and nullif(trim(reason_text), '') is not null))   -- 🚨 reason_code null で通らないよう
);
create index ix_purchase_order_events_line on events.purchase_order_events (purchase_order_line_id, event_type);
create index ix_purchase_order_events_inbound on events.purchase_order_events (inbound_ref) where inbound_ref is not null;
create unique index ux_purchase_order_events_reverses on events.purchase_order_events (reverses_event_id) where reverses_event_id is not null;

-- 明細の「使った数量」(取り消されていない receipt + shortage + cancel の合計)。呼ぶ側が明細を for update でロックしてから使う
create or replace function events.po_line_used_qty(p_line_id bigint) returns integer language sql stable as $$
  select coalesce(sum(e.qty), 0)::integer
    from events.purchase_order_events e
   where e.purchase_order_line_id = p_line_id and e.event_type in ('receipt','shortage','cancel')
     and not exists (select 1 from events.purchase_order_events r where r.reverses_event_id = e.event_id);
$$;

-- 残数超過の拒否 (明細を for update = READ COMMITTED でロック後の文は最新の commit を見る) / 逆仕訳は元イベントと 明細・数量・業務日付 が一致 / legacy・draft の PO には入れない
create or replace function events.check_po_event() returns trigger language plpgsql as $$
declare
  t record;
  po record;
  ordered integer;
  used integer;
begin
  select l.qty into ordered from core.purchase_order_lines l where l.purchase_order_line_id = new.purchase_order_line_id for update;
  if ordered is null then raise exception 'purchase_order_line % not found', new.purchase_order_line_id; end if;
  select status, is_tracked into po from core.purchase_orders where purchase_order_id = new.purchase_order_id;
  if po.status <> 'issued' or not po.is_tracked then
    raise exception 'events are only for issued & tracked purchase orders (po % is % / tracked=%)', new.purchase_order_id, po.status, po.is_tracked;
  end if;
  if new.event_type = 'reversal' then
    if new.reverses_event_id is null then return new; end if;   -- CHECK (ck_po_events_reversal_ref) に拒ませる
    select * into t from events.purchase_order_events where event_id = new.reverses_event_id;
    if t is null then raise exception 'reversal target % not found', new.reverses_event_id; end if;
    if t.event_type = 'reversal' then raise exception 'cannot reverse a reversal (%)', t.event_id; end if;
    if t.purchase_order_line_id <> new.purchase_order_line_id or t.qty <> new.qty or t.effective_date <> new.effective_date then
      raise exception 'reversal must match line/qty/effective_date of event %', t.event_id;
    end if;
    return new;
  end if;
  used := events.po_line_used_qty(new.purchase_order_line_id);
  if used + new.qty > ordered then
    raise exception 'event exceeds open qty: ordered % used % new %', ordered, used, new.qty;
  end if;
  return new;
end
$$;
create trigger trg_po_events_check before insert on events.purchase_order_events for each row execute function events.check_po_event();
select core.make_append_only('events', 'purchase_order_events');

-- 発注数を減らすときも、有効イベントの合計より下げられない (同じ明細ロック。R3 #8)
create or replace function core.check_po_line_qty() returns trigger language plpgsql as $$
declare
  used integer;
begin
  if new.qty >= old.qty then return new; end if;
  perform 1 from core.purchase_order_lines where purchase_order_line_id = new.purchase_order_line_id for update;
  used := events.po_line_used_qty(new.purchase_order_line_id);
  if new.qty < used then raise exception 'qty % is below used qty % of line %', new.qty, used, new.purchase_order_line_id; end if;
  return new;
end
$$;
create trigger trg_po_lines_qty_check before update of qty on core.purchase_order_lines for each row execute function core.check_po_line_qty();

-- ─── 残数 (元の v_po_item_balance と同じ式) ───
create or replace view mart.v_purchase_order_open as
select l.company_id, l.purchase_order_id, l.purchase_order_line_id, l.sku_id, l.product_key, po.status, po.is_tracked, po.closed_at,
       l.qty as ordered_qty,
       coalesce(sum(e.qty) filter (where e.event_type = 'receipt'), 0)::integer as received_qty,
       coalesce(sum(e.qty) filter (where e.event_type = 'shortage'), 0)::integer as shortage_qty,
       coalesce(sum(e.qty) filter (where e.event_type = 'cancel'), 0)::integer as cancelled_qty,
       coalesce(sum(e.qty) filter (where e.event_type = 'shortage' and e.reason_code = 'cutoff'), 0)::integer as cutoff_qty,
       (l.qty - coalesce(sum(e.qty) filter (where e.event_type in ('receipt','shortage','cancel')), 0))::integer as remaining_qty,
       l.next_expected_date, l.next_expected_qty, l.next_action_date, l.remainder_disposition
  from core.purchase_order_lines l
  join core.purchase_orders po on po.purchase_order_id = l.purchase_order_id
  left join events.purchase_order_events e
    on e.purchase_order_line_id = l.purchase_order_line_id and e.event_type <> 'reversal'
   and not exists (select 1 from events.purchase_order_events r where r.reverses_event_id = e.event_id)
 group by l.company_id, l.purchase_order_id, l.purchase_order_line_id, l.sku_id, l.product_key, po.status, po.is_tracked, po.closed_at, l.qty,
          l.next_expected_date, l.next_expected_qty, l.next_action_date, l.remainder_disposition;

-- ─── 商品別の注残 (元の v_ledger_backorder_by_product と同じ条件 = issued・tracked・open・残 > 0。移行 PO を含む) ───
create or replace view mart.v_purchase_backorder_by_sku as
select b.company_id, b.product_key, min(b.sku_id) as sku_id, sum(b.remaining_qty)::integer as backorder_qty, count(*)::integer as open_lines
  from mart.v_purchase_order_open b
 where b.status = 'issued' and b.is_tracked and b.closed_at is null and b.remaining_qty > 0
 group by b.company_id, b.product_key;
