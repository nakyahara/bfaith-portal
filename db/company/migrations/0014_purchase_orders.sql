-- 0014 発注 (Phase 7。08 §5。D6)
--
-- 設計の正本 = AI_reference『システム設計/CompanyDB構想/08_残りドメインのテーブル設計_20260913.md』§5 (草案 v4 = Codex 3 巡 + PR #1330 のレビュー)。
--   元 = 発注管理アプリの台帳 (apps/purchase-orders/db.js。warehouse-mirror.db の po_orders / po_order_items / po_item_events / po_settings)。D-9 = a (NE は正本のまま。
--   2026-07-13 以降の発注はこのアプリで行い、注残の正本 = po_* 台帳)。Company DB は同じ形で持ち、夜間の loader が mirror から直接読む (取込は次の PR)。
--   🚨 元の台帳と「同じ列・同じ規則・同じ式」で持つ (突合できるように)。元の SQLite の trigger をそのまま移植する:
--     - ヘッダ: status は draft / issued の 2 値、閉鎖は closed_at (null = オープン。イベントから導出 = 全明細の残数 0 で閉じ、逆仕訳で残数が戻れば開く)、
--       po_number は発行時に採番 (後から付くので鍵にしない)、tracking_mode ('tracked' = 発行時に固定される業務属性) と 追跡の境界 (po_settings.tracking_started_at → core.purchase_order_settings)
--       は**別々に持つ** (元: イベント許可 = issued かつ issued_at >= 境界 / 注残の集計 = さらに tracking_mode = 'tracked')、
--       origin = migration (ne_slip_no + send_blocked 必須。ne_slip_no は移行 PO だけ) / supplement (parent 必須・親は issued)、仕入先ごとに draft は 1 件、
--       **発行済みの発行属性・明細は変えられない・消せない・足せない** (数量減 = 取消イベント、数量増 = 新規発注)
--     - 明細: qty > 0、unit_cost は元の REAL のまま numeric (小数の単価がある)、分納の次回予定の組 (remainder_disposition ⇔ next_*)
--     - イベント (append-only): 4 種 (receipt / shortage / cancel / reversal) と元の CHECK を移植。対象 = issued かつ 境界以後。通常イベントは閉鎖済みには入らない・残数超過は拒む。
--       逆仕訳は一致だけ・1 回だけ。登録後にヘッダの closed_at を再計算 (元の trg_po_events_closure)。closed_at の直接更新は残数と矛盾しない範囲だけ (元の closed_guard)
--     - mart.v_purchase_order_open = 元の v_po_item_balance と同じ式 / mart.v_purchase_backorder_by_sku = 元の v_ledger_backorder_by_product と同じ条件
--   取込 (loader) の経路: `set local core.po_maintenance = 'on'` で不変・発行ゲート・開閉の guard・境界の不変を外して 履歴を写し (ヘッダ closed_at = null → 明細 → イベントを元の順に 1 文ずつ →
--   最後に元の closed_at)、commit の前に core.assert_purchase_orders_consistent() を通す (境界がある / 元の台帳の最終状態と矛盾していないことを機械で確かめてから公開する)。
--   イベントの CHECK・残数超過・対象範囲は保守経路でも外さない。
--   🚨 移植していない規則 (元台帳側で検証済みのイベントだけを写す、という契約): logizard 入荷 (po_inbound_items) の実在・superseded・ignore・商品/仕入先の一致・割当合計 ≤ 入荷良品数
--     (入荷の表は Company DB に無い。inbound_ref は参照として持つだけ)、po_item_history (明細の変更履歴)、メール送信の遷移。
-- 🚨 0004〜0013 の表には触らない。null になり得る列の CHECK は is not distinct from (= だと null で通る)。

-- ─── 追跡の境界 (元の po_settings.tracking_started_at。loader が写す。無ければイベントは入らない = 未設定を黙って通さない) ───
create table core.purchase_order_settings (
  company_id           smallint primary key references core.companies,
  tracking_started_at  timestamptz not null,           -- これ以後に発行した PO だけ残数を管理する (元: 変更不可)
  note                 text,
  updated_at           timestamptz not null default now()
);
create trigger trg_purchase_order_settings_touch before update on core.purchase_order_settings for each row execute function core.touch_updated_at();
-- 境界は初回確定後は不変 (元 trg_po_settings_boundary_lock_upd/del: 変えると既存 PO の tracked / legacy 判定が遡って変わる)。保守経路だけ外す (全件の整合性検査を伴う専用手順)
create or replace function core.check_po_settings_lock() returns trigger language plpgsql as $$
begin
  if current_setting('core.po_maintenance', true) = 'on' then return coalesce(new, old); end if;
  raise exception 'tracking_started_at is immutable (移行境界は変更・削除不可。保守経路で全件検査を伴って直す)';
end
$$;
create trigger trg_po_settings_lock before update or delete on core.purchase_order_settings for each row execute function core.check_po_settings_lock();
create or replace function core.po_tracking_started_at(p_company_id smallint) returns timestamptz language plpgsql stable as $$
declare v timestamptz;
begin
  select tracking_started_at into v from core.purchase_order_settings where company_id = p_company_id;
  if v is null then raise exception 'core.purchase_order_settings.tracking_started_at is not set for company % (copy po_settings first)', p_company_id; end if;
  return v;
end
$$;
-- 保守経路 (loader) か
create or replace function core.po_maintenance() returns boolean language sql stable as $$
  select current_setting('core.po_maintenance', true) = 'on';
$$;

-- ─── ヘッダ (発注 1 件 = po_orders 1 行) ───
create table core.purchase_orders (
  purchase_order_id  bigint generated always as identity primary key,
  company_id         smallint not null references core.companies,
  source_ref         text not null,                   -- 'po_orders:<id>'
  po_number          text,                            -- 発行時に採番 'PO-YYYY-NNNN' (後から付くので鍵にしない)
  supplier_id        bigint,                          -- core.suppliers (コードで解決。当たらなければ null のまま = DQ)
  supplier_code      text not null,                   -- 元の仕入先コード (解決できなくても残す)
  supplier_name      text not null,
  status             text not null check (status in ('draft','issued')),
  issued_at          timestamptz,
  closed_at          timestamptz,                     -- null = オープン。イベントから導出 (全明細の残数 0 で閉じる)
  tracking_mode      text check (tracking_mode is null or tracking_mode = 'tracked'),   -- 発行時に固定される業務属性 (元の値のまま)
  origin             text check (origin is null or origin in ('migration','supplement')),
  send_blocked       boolean not null default false,  -- メール・発注書出力の対象外 (移行 PO の誤発注防止)
  ne_slip_no         text,                            -- NE 発注伝票番号 (移行 PO だけ)
  parent_purchase_order_id bigint,                    -- 追加発注 (origin = supplement) の元 PO (issued であること = trigger)
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
  -- 元の ORIGIN_RULES (行の中で決まる分)
  constraint ck_po_migration_attrs check (origin is distinct from 'migration' or (nullif(trim(ne_slip_no), '') is not null and send_blocked)),
  constraint ck_po_ne_slip_only_migration check (ne_slip_no is null or origin is not distinct from 'migration'),
  constraint ck_po_supplement_parent check ((origin is not distinct from 'supplement') = (parent_purchase_order_id is not null)),
  constraint ck_po_not_own_parent check (parent_purchase_order_id is null or parent_purchase_order_id <> purchase_order_id)
);
create unique index ux_purchase_orders_po_number on core.purchase_orders (company_id, po_number) where po_number is not null;
create unique index ux_purchase_orders_ne_slip on core.purchase_orders (company_id, ne_slip_no) where ne_slip_no is not null;
create unique index ux_purchase_orders_one_draft on core.purchase_orders (company_id, supplier_code) where status = 'draft';   -- 元: 仕入先ごとに draft は同時に 1 件
create index ix_purchase_orders_supplier on core.purchase_orders (company_id, supplier_id, status);
create index ix_purchase_orders_open on core.purchase_orders (company_id, issued_at desc) where status = 'issued' and closed_at is null;
create trigger trg_purchase_orders_touch before update on core.purchase_orders for each row execute function core.touch_updated_at();

-- 親 PO は存在して issued (元の ORIGIN_RULES の行をまたぐ分)
create or replace function core.check_po_parent() returns trigger language plpgsql as $$
declare st text;
begin
  if new.parent_purchase_order_id is null then return new; end if;
  if new.parent_purchase_order_id = new.purchase_order_id then raise exception 'origin rules: purchase order % cannot be its own parent', new.purchase_order_id; end if;
  select status into st from core.purchase_orders where purchase_order_id = new.parent_purchase_order_id and company_id = new.company_id;
  if st is distinct from 'issued' then raise exception 'origin rules: parent purchase order % does not exist or is not issued', new.parent_purchase_order_id; end if;
  return new;
end
$$;
create trigger trg_po_parent_check before insert or update of parent_purchase_order_id, origin on core.purchase_orders for each row execute function core.check_po_parent();

-- 発行のゲート (元 trg_po_orders_issue_gate_ins/upd): 境界が決まった後は issued の直接 INSERT を拒む (正規経路 = draft で作って明細を入れて issued に上げる)。
-- draft → issued は po_number (形式 PO-YYYY-NNNN = 元の issue_gate) / issued_at / tracking_mode = 'tracked' / 明細 1 つ以上 が必要 (明細は for update で取ってから数える = 発行と明細の変更が並行しても矛盾が残らない)。
-- 発行済み PO の発行属性は変えられない・消せない (元の trg_po_orders_issued_immutable / no_delete)。保守経路 (loader) は外す
create or replace function core.check_po_issued_immutable() returns trigger language plpgsql as $$
begin
  if core.po_maintenance() then return coalesce(new, old); end if;
  if tg_op = 'INSERT' then
    if new.status = 'issued' and exists (select 1 from core.purchase_order_settings s where s.company_id = new.company_id) then
      raise exception 'issue gate: 発行は draft → issued 経由のみ (issued の直接 INSERT は不可)';
    end if;
    return new;
  end if;
  if tg_op = 'DELETE' then
    if old.status = 'issued' then raise exception 'issued purchase order % is immutable (削除不可)', old.purchase_order_id; end if;
    return old;
  end if;
  if old.status <> 'issued' and new.status = 'issued' then
    -- 明細を先にロックしてから数える (発行と明細の変更・削除が並行しても、明細ゼロの issued や発行後の数量変更が残らない。ロック順 = 親 PO (この UPDATE) → 明細 = イベントと同じ)
    perform 1 from core.purchase_order_lines l where l.purchase_order_id = new.purchase_order_id for update;
    if new.po_number is null or new.po_number !~ '^PO-[0-9]{4}-[0-9]{4,}$' or new.issued_at is null or new.tracking_mode is distinct from 'tracked'
       or not exists (select 1 from core.purchase_order_lines l where l.purchase_order_id = new.purchase_order_id) then
      raise exception 'issue gate: po_number (PO-YYYY-NNNN) / issued_at / tracking_mode = tracked が揃っていないか明細がありません (po %)', new.purchase_order_id;
    end if;
  end if;
  if old.status = 'issued' and (
       new.status is distinct from old.status or new.issued_at is distinct from old.issued_at or new.po_number is distinct from old.po_number
    or new.tracking_mode is distinct from old.tracking_mode or new.requested_date is distinct from old.requested_date or new.supplier_code is distinct from old.supplier_code
    or new.origin is distinct from old.origin or new.ne_slip_no is distinct from old.ne_slip_no or new.parent_purchase_order_id is distinct from old.parent_purchase_order_id) then
    raise exception 'issued purchase order % is immutable (発行済み PO の発行属性は変更不可)', old.purchase_order_id;
  end if;
  return new;
end
$$;
create trigger trg_po_issued_immutable before insert or update or delete on core.purchase_orders for each row execute function core.check_po_issued_immutable();

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
  unit_cost          numeric(14,4) check (unit_cost >= 0),   -- 元の REAL のまま (小数の単価がある。円の整数列にしない)
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
  -- 元の plan rules: 残数の扱いと次回予定は組で決まる
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

-- 発行済み PO の明細は 足せない・変えられない (数量・商品・単価・希望納期・条件)・消せない (元の trg_po_items_issued_*)。保守経路は外す。sku_id / unresolved_code (Render の解決) と 次回予定は変えてよい
-- 🚨 ロック順は 親 PO → 明細 (イベント・発行と同じ) に揃える:
--    INSERT (と移入) = 明細の行はまだ無いので、親 PO を for share で先に取ってから状態を見る (発行 UPDATE と並行したら片方が待ち、後の側が「明細ゼロ / 発行済み」で失敗する)
--    UPDATE / DELETE = 明細の行ロックは trigger の前に取られている (発行側が明細を for update で取るので、発行の後に来た変更は fresh な親の状態 = issued を見て失敗する)。ここで親を待つと逆順 = deadlock なので親はロックしない
create or replace function core.check_po_line_issued_immutable() returns trigger language plpgsql as $$
declare st text; st_new text;
begin
  if core.po_maintenance() then return coalesce(new, old); end if;
  -- 移動元 (OLD の PO) と移動先 (NEW の PO) を別々に見る (移動先だけ見ると、発行済み明細を draft へ移して数量を変えられる)
  if tg_op = 'INSERT' then
    select status into st from core.purchase_orders where purchase_order_id = new.purchase_order_id for share;   -- 親を先に取る (発行と並行させない)
  else
    select status into st from core.purchase_orders where purchase_order_id = old.purchase_order_id;
  end if;
  if tg_op = 'UPDATE' and new.purchase_order_id is distinct from old.purchase_order_id then
    select status into st_new from core.purchase_orders where purchase_order_id = new.purchase_order_id for share;   -- 移入先も同じ
    if st_new = 'issued' then raise exception 'issued purchase order % is immutable (発行済み PO への明細の移入は不可)', new.purchase_order_id; end if;
  end if;
  if st is distinct from 'issued' then return coalesce(new, old); end if;
  if tg_op = 'INSERT' then raise exception 'issued purchase order % is immutable (発行済み PO への明細追加は不可。追加発注は新規 PO で)', new.purchase_order_id; end if;
  if tg_op = 'DELETE' then raise exception 'issued purchase order line % is immutable (削除不可)', old.purchase_order_line_id; end if;
  if new.qty is distinct from old.qty or new.purchase_order_id is distinct from old.purchase_order_id or new.product_code is distinct from old.product_code
     or new.product_key is distinct from old.product_key or new.product_name is distinct from old.product_name or new.unit_cost is distinct from old.unit_cost
     or new.requested_date is distinct from old.requested_date or new.condition_id is distinct from old.condition_id then
    raise exception 'issued purchase order line % is immutable (発行済み明細の数量・商品・単価は変更不可。数量減 = 取消イベント)', old.purchase_order_line_id;
  end if;
  return new;
end
$$;
create trigger trg_po_line_issued_immutable before insert or update or delete on core.purchase_order_lines for each row execute function core.check_po_line_issued_immutable();

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
  -- 元の CHECK を移植 (null になり得る列の比較は is not distinct from / is null で明示)
  constraint ck_po_events_reversal_ref        check ((event_type = 'reversal') = (reverses_event_id is not null)),
  constraint ck_po_events_receipt_source      check ((event_type = 'receipt') = (receipt_source is not null)),
  constraint ck_po_events_receipt_inbound     check (event_type <> 'receipt' or ((receipt_source is not distinct from 'logizard') = (inbound_ref is not null))),
  constraint ck_po_events_non_receipt_inbound check (event_type = 'receipt' or inbound_ref is null),
  constraint ck_po_events_receipt_cancel_reason check (event_type not in ('receipt','cancel') or reason_code is null),
  constraint ck_po_events_shortage_reason     check (event_type <> 'shortage' or (reason_code is not null and reason_code in ('supplier_shortage','own_decision','cutoff','other'))),
  constraint ck_po_events_shortage_other      check (event_type <> 'shortage' or reason_code is distinct from 'other' or nullif(trim(reason_text), '') is not null),
  constraint ck_po_events_reversal_reason     check (event_type <> 'reversal' or (reason_code is not distinct from 'correction' and nullif(trim(reason_text), '') is not null))
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
-- PO の残数の合計 (全明細)
create or replace function core.po_remaining_total(p_po_id bigint) returns integer language sql stable as $$
  select coalesce(sum(l.qty - events.po_line_used_qty(l.purchase_order_line_id)), 0)::integer from core.purchase_order_lines l where l.purchase_order_id = p_po_id;
$$;

-- イベントの検査 (保守経路でも外さない): 対象 = issued かつ 境界以後 (元 trg_po_events_scope) / 通常イベントは閉鎖済みには入らない・残数超過は拒む (元 trg_po_events_normal_check、明細を for update) /
-- 逆仕訳は元イベントと 明細・数量・業務日付 が一致 (元 trg_po_events_reversal_check)
create or replace function events.check_po_event() returns trigger language plpgsql as $$
declare
  t record;
  po record;
  ordered integer;
  used integer;
begin
  -- ロックの順 = 親 PO → 明細 (同じ PO の別明細への並行イベントを直列化し、閉鎖の再計算 (AFTER の別文 = 新しいスナップショット) が相手の commit を見られるようにする。R2 #2)
  select status, issued_at, closed_at, company_id into po from core.purchase_orders where purchase_order_id = new.purchase_order_id for update;
  if po is null then raise exception 'purchase_order % not found', new.purchase_order_id; end if;
  select l.qty into ordered from core.purchase_order_lines l where l.purchase_order_line_id = new.purchase_order_line_id and l.purchase_order_id = new.purchase_order_id for update;
  if ordered is null then raise exception 'purchase_order_line % not found in po %', new.purchase_order_line_id, new.purchase_order_id; end if;
  if po.status is distinct from 'issued' or po.issued_at < core.po_tracking_started_at(po.company_id) then
    raise exception 'event scope: only issued purchase orders at or after tracking_started_at accept events (po % is %, issued_at %)', new.purchase_order_id, po.status, po.issued_at;
  end if;
  if new.event_type = 'reversal' then
    if new.reverses_event_id is null then return new; end if;   -- CHECK に拒ませる
    select * into t from events.purchase_order_events where event_id = new.reverses_event_id;
    if t is null then raise exception 'reversal target % not found', new.reverses_event_id; end if;
    if t.event_type = 'reversal' then raise exception 'cannot reverse a reversal (%)', t.event_id; end if;
    if t.purchase_order_line_id <> new.purchase_order_line_id or t.qty <> new.qty or t.effective_date <> new.effective_date then
      raise exception 'reversal must match line/qty/effective_date of event %', t.event_id;
    end if;
    return new;
  end if;
  if po.closed_at is not null then raise exception 'event scope: purchase order % is closed (訂正は逆仕訳で)', new.purchase_order_id; end if;
  used := events.po_line_used_qty(new.purchase_order_line_id);
  if used + new.qty > ordered then
    raise exception 'event exceeds open qty: ordered % used % new %', ordered, used, new.qty;
  end if;
  return new;
end
$$;
create trigger trg_po_events_check before insert on events.purchase_order_events for each row execute function events.check_po_event();
-- 登録後にヘッダの closed_at を再計算 (元 trg_po_events_closure): 全明細の残数 0 なら閉じる (時刻は最初に閉じたときのまま)、そうでなければ開く
create or replace function events.recompute_po_closure() returns trigger language plpgsql as $$
begin
  update core.purchase_orders po
     set closed_at = case when core.po_remaining_total(po.purchase_order_id) = 0 then coalesce(po.closed_at, now()) else null end
   where po.purchase_order_id = new.purchase_order_id;
  return null;
end
$$;
create trigger trg_po_events_closure after insert on events.purchase_order_events for each row execute function events.recompute_po_closure();
select core.make_append_only('events', 'purchase_order_events');

-- closed_at の直接更新の guard (元 trg_po_orders_closed_guard): 閉鎖時刻の改変は不可 / draft・明細の無い PO は閉じられない / 残数があれば閉じられない / 全消込済みは開けない。保守経路は外す
create or replace function core.check_po_closed_guard() returns trigger language plpgsql as $$
declare remaining integer; has_lines boolean;
begin
  if core.po_maintenance() then return new; end if;
  if new.closed_at is not distinct from old.closed_at then return new; end if;
  if old.closed_at is not null and new.closed_at is not null then raise exception 'closed_at guard: 閉鎖時刻の改変は不可 (po %)', old.purchase_order_id; end if;
  select exists (select 1 from core.purchase_order_lines l where l.purchase_order_id = old.purchase_order_id) into has_lines;
  remaining := core.po_remaining_total(old.purchase_order_id);
  if new.closed_at is not null and (old.status <> 'issued' or not has_lines) then raise exception 'closed_at guard: draft or empty purchase order % cannot be closed', old.purchase_order_id; end if;
  if new.closed_at is not null and remaining > 0 then raise exception 'closed_at guard: purchase order % still has remaining qty %', old.purchase_order_id, remaining; end if;
  if new.closed_at is null and has_lines and remaining = 0 then raise exception 'closed_at guard: fully consumed purchase order % cannot be reopened (訂正は逆仕訳で)', old.purchase_order_id; end if;
  return new;
end
$$;
create trigger trg_po_closed_guard before update of closed_at on core.purchase_orders for each row execute function core.check_po_closed_guard();

-- 取込の後の整合性検査 (loader が commit の前に呼ぶ): 境界がある / 各明細の有効イベント合計 ≤ 発注数 / イベントを持つ PO は issued かつ 境界以後 / closed_at ⇔ (issued かつ 明細あり かつ 残数 0)。矛盾があれば例外 (件数と例を出す)
create or replace function core.assert_purchase_orders_consistent(p_company_id smallint) returns integer language plpgsql stable as $$
declare n integer; bad record;
begin
  if not exists (select 1 from core.purchase_order_settings s where s.company_id = p_company_id)
     and exists (select 1 from core.purchase_orders po where po.company_id = p_company_id and po.status = 'issued') then
    raise exception 'core.purchase_order_settings has no tracking_started_at for company % (issued purchase orders exist)', p_company_id;
  end if;
  -- 各明細の有効イベントの合計 ≤ 発注数 (保守経路で発注数を減らしたときの残数マイナスを見つける)
  select count(*) into n from core.purchase_order_lines l where l.company_id = p_company_id and events.po_line_used_qty(l.purchase_order_line_id) > l.qty;
  if n > 0 then raise exception '% purchase order lines have used qty above ordered qty for company %', n, p_company_id; end if;
  -- イベントを持つ PO は issued かつ 境界以後 (保守経路で境界や発行属性を動かしたときの対象外れを見つける)
  select count(*) into n from core.purchase_orders po where po.company_id = p_company_id
     and exists (select 1 from events.purchase_order_events e where e.purchase_order_id = po.purchase_order_id)
     and (po.status <> 'issued' or po.issued_at < (select s.tracking_started_at from core.purchase_order_settings s where s.company_id = p_company_id));
  if n > 0 then raise exception '% purchase orders have events but are not issued at or after tracking_started_at for company %', n, p_company_id; end if;
  select count(*) into n from core.purchase_orders po where po.company_id = p_company_id
     and (po.closed_at is not null) <> (po.status = 'issued' and exists (select 1 from core.purchase_order_lines l where l.purchase_order_id = po.purchase_order_id) and core.po_remaining_total(po.purchase_order_id) = 0);
  if n > 0 then
    select po.purchase_order_id, po.source_ref, po.closed_at into bad from core.purchase_orders po where po.company_id = p_company_id
       and (po.closed_at is not null) <> (po.status = 'issued' and exists (select 1 from core.purchase_order_lines l where l.purchase_order_id = po.purchase_order_id) and core.po_remaining_total(po.purchase_order_id) = 0)
     order by po.purchase_order_id limit 1;
    raise exception '% purchase orders have closed_at inconsistent with remaining qty (e.g. % / % closed_at=%)', n, bad.purchase_order_id, bad.source_ref, bad.closed_at;
  end if;
  return (select count(*)::integer from core.purchase_orders where company_id = p_company_id);
end
$$;

-- ─── 残数 (元の v_po_item_balance と同じ式) ───
create or replace view mart.v_purchase_order_open as
select l.company_id, l.purchase_order_id, l.purchase_order_line_id, l.sku_id, l.product_key, po.status, po.tracking_mode, po.issued_at, po.closed_at,
       (po.status = 'issued' and po.issued_at >= s.tracking_started_at) as in_tracking_window,
       l.qty as ordered_qty,
       coalesce(sum(e.qty) filter (where e.event_type = 'receipt'), 0)::integer as received_qty,
       coalesce(sum(e.qty) filter (where e.event_type = 'shortage'), 0)::integer as shortage_qty,
       coalesce(sum(e.qty) filter (where e.event_type = 'cancel'), 0)::integer as cancelled_qty,
       coalesce(sum(e.qty) filter (where e.event_type = 'shortage' and e.reason_code = 'cutoff'), 0)::integer as cutoff_qty,
       (l.qty - coalesce(sum(e.qty) filter (where e.event_type in ('receipt','shortage','cancel')), 0))::integer as remaining_qty,
       l.next_expected_date, l.next_expected_qty, l.next_action_date, l.remainder_disposition
  from core.purchase_order_lines l
  join core.purchase_orders po on po.purchase_order_id = l.purchase_order_id
  left join core.purchase_order_settings s on s.company_id = po.company_id
  left join events.purchase_order_events e
    on e.purchase_order_line_id = l.purchase_order_line_id and e.event_type <> 'reversal'
   and not exists (select 1 from events.purchase_order_events r where r.reverses_event_id = e.event_id)
 group by l.company_id, l.purchase_order_id, l.purchase_order_line_id, l.sku_id, l.product_key, po.status, po.tracking_mode, po.issued_at, po.closed_at, s.tracking_started_at, l.qty,
          l.next_expected_date, l.next_expected_qty, l.next_action_date, l.remainder_disposition;

-- ─── 商品別の注残 (元の v_ledger_backorder_by_product と同じ条件 = issued・tracking_mode = 'tracked'・open・残 > 0・境界以後。移行 PO を含む) ───
create or replace view mart.v_purchase_backorder_by_sku as
select b.company_id, b.product_key, min(b.sku_id) as sku_id, sum(b.remaining_qty)::integer as backorder_qty, count(*)::integer as open_lines
  from mart.v_purchase_order_open b
 where b.status = 'issued' and b.tracking_mode = 'tracked' and b.closed_at is null and b.remaining_qty > 0 and b.in_tracking_window
 group by b.company_id, b.product_key;
