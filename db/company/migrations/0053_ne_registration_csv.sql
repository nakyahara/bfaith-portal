-- 0052: 新商品の NE 登録の CSV・JAN の変更の記録・仕入先の登録の状態 (2026-10-01。Company DB構想 14 §10 契約 v3 H5・H6・Medium 3 / §9 v2 M1・M5 / 10 §6.2・§3.2)
-- 🚨 前提 = ⑤-1 (feat/sor5-input) の 0050_master_edit.sql と ⑤-2a (feat/sor5-2-new) の 0051_master_registrations.sql だけ (と 0049 までの本流)。
--    番号: 0051 の次。先に別の migration が main に入ったら番号を付け直す (中身は番号に依らない)
--
-- なぜ:
--   A (H5) 新商品を NE に登録する CSV は、既にある商品の値を直す CSV (0040 の fix_ne / to_ne) と別の物にする。
--      作ったファイル (export)・商品ごと (item)・CSV の行・取り込みの申告 (attempt)・翌朝の照合の確かめ (check) を分けて持ち、
--      商品ごとに built (作った) → issued (配った) → import_declared (取り込んだと申告) → verified (NE で全部の列が合った) / partial (NE にあるが違う列がある) /
--      failed (取り込めなかった = 申告の後の完全な取得に無い・全部だめと申告) / superseded (人が「使わない」にした) と進める。
--      配った (issued) 後は、その商品の NE に送る欄を直せない (lib/master-write.mjs が 409)。直すには人が「使わない」にして、NE で何をしたか (直し方) を書く
--   B (H6) JAN (core.external_ids の system = 'jan') を画面で直せるようにする前に、足す・外す (有効期間の終わり) を変更の記録 (0026 と同じ関数) に残し、
--      値の書き換え・物理の削除を拒み、JAN が変わったらその商品の SKU の version も変える (編集の印・CSV の予約が古くなる)
--   C (M1・M5・契約 v3 Medium 3) 新しい仕入先のコードの決まりと「NE に登録した」の申告の状態。確かめる前の仕入先は代表の仕入先に選べない。
--      取引停止は active = false (物理の削除はしない)。代表の仕入先に使っている間は止められない (同じ取引で付け替えれば止められる)
-- なにを:
--   A. ops.ne_reg_exports / ne_reg_attempts / ne_reg_export_items / ne_reg_export_rows / ne_reg_checks (+ 中身は変えない・一方向の trigger)
--      🚨 権限の境界 (⑤-2a と同じ・#1566 R1 H2 / H3): 画面のロール (master_edit) にこれらの表の INSERT / UPDATE / DELETE を渡さない。
--         書くのは下の security definer の関数だけ (search_path = pg_catalog, pg_temp・名前は全部 schema つき・一時の表を使わない・public の実行権なし):
--           ops.ne_reg_build (作る: コード・状態・NE にもう無いこと・構成品・実機の確かめの門・CSV の byte 列を関数が照らし直す。
--             🚨 行と確かめる値は鍵の後に ops.ne_reg_canonical が Company DB の今の値から作り直し、送られたものと完全に同じときだけ作る・
--             印とハッシュ 4 つは関数が計算する (#1571 Codex R1 High 1)) /
--           ops.ne_reg_issue (配る) / ops.ne_reg_declare (取り込んだと申告: sha256 はファイルの記録と同じ・登録の状態 draft → ne_pending) /
--           ops.ne_reg_supersede (使わない) / ops.ne_reg_guard_on_save (保存の取引: 配った後 = 知らせる・作っただけ = 使わない) /
--           ops.ne_reg_record_verified (実機の確かめ = ops.ne_csv_verified に col 'new_registration'・試しのファイルの商品が全部 verified のときだけ ok) /
--           翌朝の照合 = watch_writer の 3 段 (#1571 Codex R1 High 2): ops.record_ne_registration_observations (NE の完全な取得の観測を残す) →
--             ops.seal_ne_registration_run (回が最後まで終わった受け取り) → ops.record_ne_registration_check(回の番号) (受け取りと残した観測から verified / partial / failed)
--      ops.transition_sku_registration (0051) を create or replace: ne_pending・ne_confirmed の根拠は関数が自分でこの表から読んで鍵を取る
--        (呼び手の渡す export_id・hash・受け取りの JSON は信じない = 渡したら拒む)。distributable / available は ④ まで not_ready のまま
--   B. events.master_change_events の entity_type に 'external_id' (CHECK は NOT VALID で足す = VALIDATE は後の手順・#1571 R1 Low)。core.external_ids の JAN の行に 記録・守り・SKU の version (security definer) の trigger。
--      表の持ち主でないロール (画面) が書けるのは商品の JAN の行だけ (trigger)
--   C. ops.supplier_registrations (新しい仕入先だけ。行が無い = 前からある仕入先)。状態を書くのは security definer の関数
--      (ops.create_supplier_registration / ops.declare_supplier_in_ne) だけ + 守りの trigger (取引停止・物理の削除・確かめる前の代表)
--   D. 0050 の画面のロールの書き込みの約束 (#1563 R3 M2) との関係:
--      ・上の関数 (ops.ne_reg_* と仕入先の関数) は、呼び手が画面のロール master_edit なら同じ取引の ops.begin_master_write の行が要る (ops.reg_write_session)。
--        誰が (と ops.ne_reg_build の request_id) はその行から取る (引数と違えば 42501 = 偽れない)。ほかのロール (表の持ち主・運用) は引数のまま
--      ・画面のロールが直接書く表は core.external_ids (商品の JAN) だけ: 0050 の trg_master_edit_guard (同じ取引の行・段階 new_open) +
--        trg_master_edit_jan_owner (始めたときの持ち主表で external_ids.jan が company)。変更の記録の誰が・request_id は 0050 の関数がその行から取る
--      ・確かめる前の仕入先を代表にしない守り (core.guard_primary_supplier_registered) は、呼び手が master_edit なら設定 (core.source_system) に依らず見る
--      ・切替の前提 (ops.master_cutover_prereq_checks) は足さない (⑤-2b の表は切替の日に空でよい)
-- 🚨 前提の migration は 0050 (⑤-1) と 0051 (⑤-2a) だけ (と 0049 までの本流)
-- 🚨 この migration は商品・仕入先の値を何も変えない

-- ═══════════ A. 新商品の NE 登録の CSV ═══════════
create table ops.ne_reg_exports (
  export_id        bigint generated always as identity primary key,
  company_id       smallint not null default 1 references core.companies,
  kind             text not null check (kind in ('products', 'sets')),
  schema_version   text not null check (schema_version ~ '^ne-reg-(single|set)-v[0-9]+$'),
  header           text not null check (header ~ '^[a-z_]+(,[a-z_]+)*$'),
  encoding         text not null check (encoding in ('utf8')),
  trial            boolean not null,   -- 実機で確かめていない形 = 試し用 (行の上限つき)。決めるのは ops.ne_reg_build
  item_count       integer not null check (item_count between 1 and 1000),
  row_count        integer not null check (row_count between 1 and 1000),
  aggregate_token  text not null check (aggregate_token ~ '^[0-9a-f]{64}$'),   -- 商品ごとの印 (item_token) をまとめたハッシュ (関数が計算)
  payload_hash     text not null check (payload_hash ~ '^[0-9a-f]{64}$'),      -- 形の版・見出し・商品ごとの確かめる値と CSV の行のハッシュ (関数が計算)
  sha256           text not null check (sha256 ~ '^[0-9a-f]{64}$'),            -- 配る byte 列 (関数が計算する)
  file_bytes       bytea not null,
  request_id       uuid not null unique,   -- 作る操作 1 回 (同じ番号 + 同じ中身 = 同じファイル)
  ne_codes_run     text not null,          -- 「NE にまだ無い」を確かめた照合の回 (ops.master_ne_code_mark)
  cost_day         date not null,          -- 原価を見た東京の日 (行の原価はこの日の原価・関数が今の値から作る)
  created_by       text not null check (length(created_by) > 0),
  created_at       timestamptz not null default now(),
  state            text not null default 'built' check (state in ('built', 'issued', 'declared', 'closed')),
  issued_at        timestamptz,   -- 最初に配った時刻 (動かさない)
  issued_by        text,
  declared_at      timestamptz,   -- 最初の申告 (動かさない)
  declared_by      text,
  closed_at        timestamptz,
  closed_by        text,
  close_reason     text check (close_reason in ('superseded', 'rejected_all', 'finished')),
  constraint ck_nre_issued check ((state in ('issued', 'declared')) <= (issued_at is not null and issued_by is not null)),
  constraint ck_nre_declared check ((state = 'declared') <= (declared_at is not null and declared_by is not null)),
  constraint ck_nre_closed check ((state = 'closed') = (closed_at is not null and closed_by is not null and close_reason is not null))
);
create index ix_ne_reg_exports_created on ops.ne_reg_exports (created_at desc);
comment on table ops.ne_reg_exports is '新商品の NE 登録の CSV のファイル 1 つ = 1 行 (0052)。中身 (byte 列・ハッシュ・印) は変えない・消さない。書くのは ops.ne_reg_* の関数だけ';

create table ops.ne_reg_attempts (
  attempt_id   bigint generated always as identity primary key,
  export_id    bigint not null references ops.ne_reg_exports (export_id),
  sha256       text not null check (sha256 ~ '^[0-9a-f]{64}$'),   -- 申告した人が確かめたファイルの sha256 (ファイルの記録と同じであること)
  declared_by  text not null check (length(declared_by) > 0),
  declared_at  timestamptz not null default now(),
  imported_at  timestamptz,   -- NE に取り込んだ時刻 (人が書く・任意)
  result       text not null check (result in ('ok', 'partial', 'rejected_all')),
  ne_message   text check (ne_message is null or length(ne_message) <= 1000),   -- NE の「商品一括登録の履歴」のメッセージ (一部失敗でも状態は「処理成功」= メッセージで選ぶ。10 §6.2)
  note         text check (note is null or length(note) <= 500),
  constraint ck_nra_imported check (imported_at is null or imported_at <= declared_at)
);
select core.make_append_only('ops', 'ne_reg_attempts');

create table ops.ne_reg_export_items (
  item_id          bigint generated always as identity primary key,
  export_id        bigint not null references ops.ne_reg_exports (export_id),
  sku_id           bigint not null references core.skus (sku_id),
  code_norm        text not null check (length(code_norm) > 0),
  ne_code          text not null check (ne_code ~ '^[a-z0-9_-]{1,30}$'),   -- 新しいコードは小文字だけ (中原さん 2026-10-01)
  sku_kind         text not null check (sku_kind in ('single', 'set')),
  item_token       text not null check (item_token ~ '^[0-9a-f]{64}$'),   -- 作ったときの印 (SKU・商品の版・登録の状態・snapshot_hash。関数が計算)
  expected         jsonb not null check (jsonb_typeof(expected) = 'object'),   -- NE で確かめる値 (照合の正規化の形)
  snapshot_hash    text not null check (snapshot_hash ~ '^[0-9a-f]{64}$'),
  row_from         integer not null check (row_from >= 1),
  row_to           integer not null,
  state            text not null default 'built' check (state in ('built', 'issued', 'import_declared', 'verified', 'partial', 'failed', 'superseded')),
  state_changed_at timestamptz not null default now(),
  state_changed_by text not null check (length(state_changed_by) > 0),
  attempt_id       bigint references ops.ne_reg_attempts (attempt_id),   -- 取り込んだと申告した試み
  verified_run     text,
  verified_at      timestamptz,
  failed_reason    text check (failed_reason in ('rejected_all', 'not_in_ne')),
  superseded_reason     text check (superseded_reason is null or length(superseded_reason) between 1 and 500),
  superseded_correction text check (superseded_correction is null or length(superseded_correction) between 1 and 500),
  unique (export_id, sku_id),
  constraint ck_nri_rows check (row_to >= row_from),
  constraint ck_nri_declared check ((state in ('import_declared', 'verified', 'partial')) <= (attempt_id is not null)),
  constraint ck_nri_verified check ((state = 'verified') = (verified_run is not null and verified_at is not null)),
  constraint ck_nri_failed check ((state = 'failed') = (failed_reason is not null)),
  constraint ck_nri_superseded check ((state = 'superseded') = (superseded_reason is not null and superseded_correction is not null))
);
-- 生きている (まだ終わっていない) ファイルは SKU ごとに 1 つ = 同じ商品を 2 つのファイルで登録しない
create unique index ux_ne_reg_items_live on ops.ne_reg_export_items (sku_id) where state in ('built', 'issued', 'import_declared', 'partial');
create index ix_ne_reg_items_export on ops.ne_reg_export_items (export_id);
create index ix_ne_reg_items_code on ops.ne_reg_export_items (code_norm);
comment on table ops.ne_reg_export_items is '新商品の NE 登録の CSV の商品ごと (0052)。built → issued → import_declared → verified / partial / failed、どこからでも superseded (人)';

create table ops.ne_reg_export_rows (
  export_id  bigint not null references ops.ne_reg_exports (export_id),
  row_no     integer not null check (row_no >= 1),   -- 見出しを除いた行の番号
  item_id    bigint not null references ops.ne_reg_export_items (item_id),
  cells      jsonb not null check (jsonb_typeof(cells) = 'array'),
  primary key (export_id, row_no)
);
select core.make_append_only('ops', 'ne_reg_export_rows');

create table ops.ne_reg_checks (
  check_id       bigint generated always as identity primary key,
  compare_run_id text not null references ops.master_compare_runs (compare_run_id),
  item_id        bigint not null references ops.ne_reg_export_items (item_id),
  sku_id         bigint not null references core.skus (sku_id),
  fetched_at     timestamptz,   -- NE の完全な取得の時刻 (単品 = 商品の取得・セット = セットの取得)
  outcome        text not null check (outcome in ('verified', 'partial', 'failed', 'waiting', 'in_ne_undeclared')),
  detail         jsonb not null check (jsonb_typeof(detail) = 'object'),
  recorded_at    timestamptz not null default now(),
  unique (compare_run_id, item_id)
);
select core.make_append_only('ops', 'ne_reg_checks');
comment on table ops.ne_reg_checks is '翌朝の照合 ② の完全な取得で、新規登録の商品を確かめた記録 (0052)。書くのは ops.record_ne_registration_check だけ';

-- 実機の確かめ (0040 の ops.ne_csv_verified を広げる): 新規登録の形は col = 'new_registration'・converter_version = 形の版・reg_export_id = 確かめに使った試しのファイル
alter table ops.ne_csv_verified add column reg_export_id bigint references ops.ne_reg_exports (export_id);

-- 守り: ファイルの中身は変えない・消さない。状態は一方向 (built → issued → declared → closed / built・issued → closed)
create function ops.guard_ne_reg_exports() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'ne_reg_exports は消さない' using errcode = 'P0001'; end if;
  if (new.export_id, new.company_id, new.kind, new.schema_version, new.header, new.encoding, new.trial, new.item_count, new.row_count, new.aggregate_token,
      new.payload_hash, new.sha256, new.file_bytes, new.request_id, new.ne_codes_run, new.cost_day, new.created_by, new.created_at)
     is distinct from
     (old.export_id, old.company_id, old.kind, old.schema_version, old.header, old.encoding, old.trial, old.item_count, old.row_count, old.aggregate_token,
      old.payload_hash, old.sha256, old.file_bytes, old.request_id, old.ne_codes_run, old.cost_day, old.created_by, old.created_at) then
    raise exception 'ne_reg_exports の中身は書き換えない (状態の列だけ)' using errcode = 'P0001';
  end if;
  if old.state = 'closed' and new is distinct from old then raise exception '閉じたファイルは変えない' using errcode = 'P0001'; end if;
  if new.state <> old.state and not ((old.state || '>' || new.state) = any (array['built>issued', 'issued>declared', 'built>closed', 'issued>closed', 'declared>closed'])) then
    raise exception 'ファイルの状態は % から % に進めない', old.state, new.state using errcode = 'P0001';
  end if;
  if old.issued_at is not null and (new.issued_at, new.issued_by) is distinct from (old.issued_at, old.issued_by) then raise exception '最初に配った記録は動かさない' using errcode = 'P0001'; end if;
  if old.declared_at is not null and (new.declared_at, new.declared_by) is distinct from (old.declared_at, old.declared_by) then raise exception '最初の申告の記録は動かさない' using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_ne_reg_exports_guard before update or delete on ops.ne_reg_exports for each row execute function ops.guard_ne_reg_exports();
create trigger trg_ne_reg_exports_no_truncate before truncate on ops.ne_reg_exports for each statement execute function core.reject_mutation();

create function ops.ne_reg_item_transition_allowed(p_from text, p_to text) returns boolean language sql immutable as $$
  select (p_from || '>' || p_to) = any (array[
    'built>issued', 'built>superseded',
    'issued>import_declared', 'issued>failed', 'issued>superseded',
    'import_declared>verified', 'import_declared>partial', 'import_declared>failed', 'import_declared>superseded',
    'partial>verified', 'partial>failed', 'partial>superseded'])
$$;

create function ops.guard_ne_reg_items() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'ne_reg_export_items は消さない' using errcode = 'P0001'; end if;
  if tg_op = 'INSERT' then
    if new.state <> 'built' then raise exception '商品ごとの行は built で作る' using errcode = 'P0001'; end if;
    return new;
  end if;
  if (new.item_id, new.export_id, new.sku_id, new.code_norm, new.ne_code, new.sku_kind, new.item_token, new.expected, new.snapshot_hash, new.row_from, new.row_to)
     is distinct from
     (old.item_id, old.export_id, old.sku_id, old.code_norm, old.ne_code, old.sku_kind, old.item_token, old.expected, old.snapshot_hash, old.row_from, old.row_to) then
    raise exception 'ne_reg_export_items の中身は書き換えない (状態の列だけ)' using errcode = 'P0001';
  end if;
  if old.state in ('verified', 'failed', 'superseded') and new is distinct from old then raise exception '終わった商品 (%) は変えない', old.state using errcode = 'P0001'; end if;
  if new.state <> old.state and not ops.ne_reg_item_transition_allowed(old.state, new.state) then
    raise exception '商品の状態は % から % に進めない', old.state, new.state using errcode = 'P0001';
  end if;
  if old.attempt_id is not null and new.attempt_id is distinct from old.attempt_id then raise exception '申告の試みは付け替えない' using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_ne_reg_items_guard before insert or update or delete on ops.ne_reg_export_items for each row execute function ops.guard_ne_reg_items();
create trigger trg_ne_reg_items_no_truncate before truncate on ops.ne_reg_export_items for each statement execute function core.reject_mutation();

-- 申告の sha256 はファイルの記録と同じであること (違うファイルを取り込んだ申告を受けない)
create function ops.guard_ne_reg_attempts() returns trigger language plpgsql as $$
begin
  if not exists (select 1 from ops.ne_reg_exports e where e.export_id = new.export_id and e.sha256 = new.sha256) then
    raise exception 'sha256_mismatch: 申告の sha256 がファイル % の記録と違う', new.export_id using errcode = '22023';
  end if;
  return new;
end $$;
create trigger trg_ne_reg_attempts_sha before insert on ops.ne_reg_attempts for each row execute function ops.guard_ne_reg_attempts();

-- 照合が NE の値を送る商品 (配った・申告した・一部違う = まだ終わっていない)。watcher が読む
create view ops.v_ne_reg_targets as
  select i.item_id, i.export_id, i.sku_id, i.code_norm, i.sku_kind, i.state
    from ops.ne_reg_export_items i
   where i.state in ('issued', 'import_declared', 'partial');
comment on view ops.v_ne_reg_targets is '翌朝の照合 ② が NE の完全な取得の値を送る新規登録の商品 (0052)';

-- CSV のセル 1 つの書き方 (lib/master-reg-csv.mjs の regQuote と同じ: カンマ・引用符・前後の空白 (半角・全角) を含むときだけ引用符で囲む)。
-- 制御文字 (改行・タブほか) のセルは書かない (呼び手が null を受けて拒む)
create function ops.ne_reg_csv_cell(p text) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case when p ~ '[\x01-\x1f\x7f\u2028\u2029]' then null
              when p ~ '[",]' or p ~ '^[ 　]' or p ~ '[ 　]$' then '"' || pg_catalog.replace(p, '"', '""') || '"'
              else p end
$$;

-- 目標 (expected = 照合で確かめる値) が配る行のセルと同じ中身か (呼び手の目標を信じない = 配るものと違う値で「確かめた」にしない)。null = 合う / 合わない列の名前
--   単品 (行 1 つ): 名前・仕入先 (4 桁)・原価・売価・税率 (10 / 8)・取扱区分 (0 / 1)・代表 (empty = 親なし / NE の書き方の小文字 = 親のコード)
--   セット (構成品ごとに 1 行): 名前・売価・税率 (全部の行で同じ) と 構成品 (順に コードの小文字・数量)
create function ops.ne_reg_expected_problem(p_kind text, p_expected jsonb, p_rows jsonb) returns text
  language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  v  jsonb := p_expected -> 'values';
  c  jsonb;
  ch jsonb;
  n  integer := pg_catalog.jsonb_array_length(p_rows);
  i  integer;
begin
  if pg_catalog.jsonb_typeof(v) is distinct from 'object' then return 'values'; end if;
  if pg_catalog.jsonb_typeof(v -> 'price') is distinct from 'number' then return 'price'; end if;
  if p_kind = 'products' then
    if (p_expected ->> 'kind') is distinct from 'single' or n <> 1 then return 'kind'; end if;
    c := p_rows -> 0;
    if (v ->> 'name') is distinct from (c ->> 1) then return 'name'; end if;
    if (v ->> 'supplier') is distinct from pg_catalog.lower(c ->> 2) then return 'supplier'; end if;
    if pg_catalog.jsonb_typeof(v -> 'cost') is distinct from 'number' or (v ->> 'cost') is distinct from (c ->> 3) then return 'cost'; end if;
    if (v ->> 'price') is distinct from (c ->> 4) then return 'price'; end if;
    if not (((v -> 'tax_rate') = '0.1'::jsonb and (c ->> 5) = '10') or ((v -> 'tax_rate') = '0.08'::jsonb and (c ->> 5) = '8')) then return 'tax_rate'; end if;
    if not (((v ->> 'handling') = 'active' and (c ->> 6) = '0') or ((v ->> 'handling') = 'discontinued' and (c ->> 6) = '1')) then return 'handling'; end if;
    if not ((coalesce(pg_catalog.jsonb_typeof(v -> 'parent'), 'null') = 'null' and (c ->> 7) = 'empty')
         or (pg_catalog.jsonb_typeof(v -> 'parent') = 'string' and (c ->> 7) <> 'empty' and (v ->> 'parent') = pg_catalog.lower(c ->> 7))) then return 'parent'; end if;
    return null;
  end if;
  if (p_expected ->> 'kind') is distinct from 'set' then return 'kind'; end if;
  if pg_catalog.jsonb_typeof(v -> 'children') is distinct from 'array' or pg_catalog.jsonb_array_length(v -> 'children') <> n then return 'children'; end if;
  for i in 0 .. n - 1 loop
    c := p_rows -> i;
    ch := v -> 'children' -> i;
    if (v ->> 'name') is distinct from (c ->> 1) then return 'name'; end if;
    if (v ->> 'price') is distinct from (c ->> 2) then return 'price'; end if;
    if (c ->> 3) is distinct from (p_rows -> 0 ->> 3) or (c ->> 3) not in ('10', '8') then return 'tax_rate'; end if;
    if (ch ->> 'code_norm') is distinct from pg_catalog.lower(c ->> 4) or pg_catalog.jsonb_typeof(ch -> 'qty') is distinct from 'number'
       or (ch ->> 'qty') is distinct from (c ->> 5) then return 'children'; end if;
  end loop;
  return null;
end $$;

-- 目標 (expected) と NE の観測を比べる (単品 = 名前・仕入先・原価・売価・税率・取扱区分・代表 / セット = 名前・売価・構成品と数量と行の数)。
-- 観測の列 = { st: 'ok' | 'no_value' | 'invalid', v }。st が ok でない列は合わない (比べられない = 一致にしない)。
-- JAN (単品)・税率と並び (セット) は NE の取得に無い = 比べない (not_compared に書く)
create function ops.ne_reg_compare(p_expected jsonb, p_obs jsonb) returns jsonb language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  v_kind   text := p_expected ->> 'kind';
  v_cols   jsonb := '{}'::jsonb;
  v_ok     boolean := true;
  k        text;
  e        jsonb;
  o        jsonb;
  ok_k     boolean;
  v_exp    text[];
  v_obs    text[];
  v_bad    integer;
begin
  if (p_obs ->> 'kind') is distinct from v_kind then
    return pg_catalog.jsonb_build_object('ok', false, 'kind', pg_catalog.jsonb_build_object('expected', v_kind, 'observed', p_obs -> 'kind'));
  end if;
  foreach k in array (case when v_kind = 'single' then array['name', 'supplier', 'cost', 'price', 'tax_rate', 'handling', 'parent'] else array['name', 'price'] end) loop
    e := coalesce(p_expected -> 'values' -> k, 'null'::jsonb);
    o := p_obs -> 'cols' -> k;
    ok_k := (o ->> 'st') = 'ok' and coalesce(o -> 'v', 'null'::jsonb) = e;
    v_cols := v_cols || pg_catalog.jsonb_build_object(k, pg_catalog.jsonb_build_object('ok', ok_k, 'expected', e, 'observed', o));
    v_ok := v_ok and ok_k;
  end loop;
  if v_kind = 'set' then
    -- 構成品と数量を「コード|数量」の並べた配列にして比べる (行の数も同じであること・並びは NE の取得に無い = 比べない)
    select coalesce(pg_catalog.array_agg(t.k order by t.k), '{}') into v_exp
      from (select (c ->> 'code_norm') || '|' || ((c ->> 'qty')::numeric)::bigint::text as k
              from pg_catalog.jsonb_array_elements(coalesce(p_expected -> 'values' -> 'children', '[]'::jsonb)) c) t;
    select pg_catalog.count(*) filter (where t.bad), coalesce(pg_catalog.array_agg(t.k order by t.k), '{}') into v_bad, v_obs
      from (select (c ->> 'st' is distinct from 'ok' or pg_catalog.jsonb_typeof(c -> 'v') is distinct from 'number') as bad,
                   (c ->> 'code_norm') || '|' || case when pg_catalog.jsonb_typeof(c -> 'v') = 'number' then ((c ->> 'v')::numeric)::bigint::text else '?' end as k
              from pg_catalog.jsonb_array_elements(coalesce(p_obs -> 'children', '[]'::jsonb)) c) t;
    ok_k := v_bad = 0 and v_exp = v_obs;
    v_cols := v_cols || pg_catalog.jsonb_build_object('children', pg_catalog.jsonb_build_object('ok', ok_k, 'expected', pg_catalog.to_jsonb(v_exp), 'observed', pg_catalog.to_jsonb(v_obs)));
    v_ok := v_ok and ok_k;
  end if;
  return pg_catalog.jsonb_build_object('ok', v_ok, 'cols', v_cols,
    'not_compared', case when v_kind = 'single' then '["jan"]'::jsonb else '["tax_rate", "order"]'::jsonb end);
end $$;

/**
 * 呼び手が画面のロール master_edit なら、同じ取引で ops.begin_master_write をした後だけ (0050 の約束・#1563 R3 M2。0051 の ops.create_sku_registration と同じ)。
 * 誰が・request_id はその行から取る: 引数があって違えば 42501 (偽れない)。ほかのロール (表の持ち主・運用) = 引数のまま。
 * 戻り値 { actor, request_id, bound (行から取った = true) }。🚨 security definer = 画面のロールに ops.master_write_sessions を読ませない
 */
create function ops.reg_write_session(p_actor text, p_request_id uuid default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_sess    ops.master_write_sessions%rowtype;
begin
  if v_db_user is distinct from 'master_edit' then
    return pg_catalog.jsonb_build_object('actor', p_actor, 'request_id', p_request_id, 'bound', false);
  end if;
  select * into v_sess from ops.master_write_sessions s where s.txid = pg_catalog.txid_current();
  if not found then
    raise exception 'master_write_session_required: 画面のロールの書き込みは、同じ取引で ops.begin_master_write をした後だけ' using errcode = '42501';
  end if;
  if (p_actor is not null and p_actor is distinct from v_sess.actor_id) or (p_request_id is not null and p_request_id is distinct from v_sess.request_id) then
    raise exception 'master_write_session_mismatch: 誰が・request_id が ops.begin_master_write のときと違う' using errcode = '42501';
  end if;
  return pg_catalog.jsonb_build_object('actor', v_sess.actor_id, 'request_id', v_sess.request_id, 'bound', true);
end $$;
revoke all on function ops.reg_write_session(text, uuid) from public;

/** 切替の段階が new_open でなければ止める (関数の中の保険。画面の門 = 段階・持ち主表のハッシュ・MASTER_EDIT_OPEN は lib が先に見る) */
create function ops.ne_reg_assert_open() returns void language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_phase text;
begin
  -- 段階を変える取引 (ops.set_master_cutover_phase = 排他) と並ぶ (lib/master-cutover.mjs の CUTOVER_SHARED_LOCK_SQL と同じ鍵。呼び手が先に取っていれば待たない)
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.master_cutover'));
  select s.phase into v_phase from ops.master_cutover_state s where s.id = 1;
  if v_phase is distinct from 'new_open' then
    raise exception 'before_cutover: 切替の段階が % (new_open でない) = 新商品の NE 登録の CSV はまだ使えない', coalesce(v_phase, '読めない') using errcode = 'P0001';
  end if;
end $$;
revoke all on function ops.ne_reg_assert_open() from public;

/** SKU ごとの鍵 (sku_id の順・lib/master-write.mjs の SKU_LOCK_SQL と同じ鍵) → CSV の鍵。同じ取引で先に取っていれば待たない */
create function ops.ne_reg_lock_skus(p_sku_ids bigint[]) returns void language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_id bigint;
begin
  for v_id in select distinct x from pg_catalog.unnest(coalesce(p_sku_ids, '{}'::bigint[])) as t(x) where x is not null order by 1 loop
    perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || v_id::text, 0));
  end loop;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_csv'));
end $$;
revoke all on function ops.ne_reg_lock_skus(bigint[]) from public;

/** JAN の形 (8 桁か 13 桁 + チェック数字) = lib/master-write.mjs の janValid と同じ決まり */
create function ops.jan_check_ok(p text) returns boolean language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  n integer;
  s integer := 0;
  i integer;
begin
  if p is null or p !~ '^([0-9]{8}|[0-9]{13})$' then return false; end if;
  n := pg_catalog.length(p);
  -- チェック数字の前の桁から左へ 3・1・3・… の重み
  for i in 1 .. n - 1 loop
    s := s + pg_catalog.substr(p, n - i, 1)::integer * (case when i % 2 = 1 then 3 else 1 end);
  end loop;
  return (10 - s % 10) % 10 = pg_catalog.substr(p, n, 1)::integer;
end $$;

/** 商品名を NE の CSV に書けるか (空・前後の空白・「empty」・制御文字・4 バイトの文字・255 字より長い = だめ)。apps/master-decisions/ne-csv.mjs の nameCell と同じ向き */
create function ops.ne_reg_name_ok(p text) returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$
  select p is not null and p <> '' and pg_catalog.char_length(p) <= 255
     and pg_catalog.ascii(pg_catalog.left(p, 1)) not in (9, 10, 13, 32, 160, 12288, 65279)
     and pg_catalog.ascii(pg_catalog.right(p, 1)) not in (9, 10, 13, 32, 160, 12288, 65279)
     and pg_catalog.lower(pg_catalog.btrim(p)) <> 'empty'
     and not exists (select 1 from pg_catalog.regexp_split_to_table(p, '') c
                      where pg_catalog.ascii(c) < 32 or pg_catalog.ascii(c) between 127 and 159 or pg_catalog.ascii(c) in (8232, 8233) or pg_catalog.ascii(c) > 65535)
$$;

/**
 * 1 つの SKU の新規登録の CSV の行と NE で確かめる値を、Company DB の今の値 (core.* / ops.*) から作る (呼び手の値を信じない・#1571 Codex R1 High 1)。
 * lib/master-reg-csv.mjs の regMaterialOf と同じ決まり (lib が先に同じものを作って送る = 関数は同じかを見るだけ)。
 *   単品 (1 行) = コード・名前・代表の仕入先 (4 桁・1 つだけ・申告済み)・p_day の原価 (COMPLETE / OVERRIDDEN・1 円以上の整数)・標準売価・税率 (10 / 8)・
 *                 取扱区分 (0 / 1)・代表 (親なし = empty / NE の元のコードの書き方)・JAN (有効な 1 つ・チェック数字 / 無ければ empty)
 *   セット (構成品ごとに 1 行) = 開いている構成の依頼の構成 (無ければ今の構成)・構成品の税率から導いた税率 (低い方)・構成品の NE の書き方・数量 1〜999
 * 戻り値 { cells, expected, blockers: [理由] }。呼び手は SKU (と構成品) の鍵の後に呼ぶ
 */
create function ops.ne_reg_canonical(p_sku_id bigint, p_day date) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  v_blk    text[] := '{}';
  v_price  bigint;
  v_tax    numeric;
  v_taxc   text;
  v_hand   text;
  v_cost   record;
  v_costv  bigint;
  v_nsup   integer;
  v_sup    record;
  v_supc   text;
  v_par    text;
  v_parc   text := 'empty';
  v_pr     record;
  v_jans   text[];
  v_cells  jsonb := '[]'::jsonb;
  v_exp    jsonb;
  v_req    jsonb;
  r        record;
  v_kids   jsonb := '[]'::jsonb;
  v_n      integer := 0;
  v_ntax   integer := 0;
  v_badtax boolean := false;
begin
  select k.sku_id, k.code, k.code_norm, k.sku_kind, k.name, k.tax_rate, k.handling, k.standard_price_jpy, k.product_id, p.parent_product_id, pp.display_code as parent_code
    into s
    from core.skus k left join core.products p on p.product_id = k.product_id left join core.products pp on pp.product_id = p.parent_product_id
   where k.sku_id = p_sku_id;
  if not found then return pg_catalog.jsonb_build_object('blockers', pg_catalog.jsonb_build_array('商品が無い')); end if;
  if not ops.ne_reg_name_ok(s.name) then v_blk := v_blk || '名前を CSV に書けない'::text; end if;
  if s.standard_price_jpy is null or s.standard_price_jpy <> pg_catalog.trunc(s.standard_price_jpy::numeric) or s.standard_price_jpy not between 1 and 999999999 then
    v_blk := v_blk || '標準売価が無い (1〜999,999,999 円)'::text;
  else
    v_price := s.standard_price_jpy::bigint;
  end if;
  if s.sku_kind = 'single' then
    v_tax := case when s.tax_rate = 0.1 then 0.1 when s.tax_rate = 0.08 then 0.08 end;
    v_taxc := case when v_tax = 0.1 then '10' when v_tax = 0.08 then '8' end;
    if v_taxc is null then v_blk := v_blk || '税率が無い (8% か 10%)'::text; end if;
    v_hand := case s.handling when 'active' then '0' when 'discontinued' then '1' end;
    if v_hand is null then v_blk := v_blk || '取扱区分が 取扱中 / 中止 でない'::text; end if;
    -- p_day の原価 (lib/master-write.mjs の costAsOfJoin と同じ選び方)
    select y.cost_jpy, y.cost_status into v_cost from core.sku_costs y
     where y.sku_id = p_sku_id and y.valid_from <= p_day and (y.valid_to is null or y.valid_to >= p_day)
     order by y.valid_from desc, y.created_at desc, y.sku_cost_id desc limit 1;
    if v_cost.cost_status is null or v_cost.cost_status not in ('COMPLETE', 'OVERRIDDEN') or v_cost.cost_jpy is null
       or v_cost.cost_jpy <> pg_catalog.trunc(v_cost.cost_jpy) or v_cost.cost_jpy < 1 then
      v_blk := v_blk || '原価が無い (今日の原価・1 円以上)'::text;
    else
      v_costv := v_cost.cost_jpy::bigint;
    end if;
    select pg_catalog.count(*) into v_nsup from core.supplier_skus x where x.sku_id = p_sku_id and x.is_primary;
    if v_nsup <> 1 then
      v_blk := v_blk || '代表の仕入先が決まっていない'::text;
    else
      select su.supplier_id, su.code, rg.state as reg_state into v_sup
        from core.supplier_skus x join core.suppliers su on su.supplier_id = x.supplier_id
        left join ops.supplier_registrations rg on rg.supplier_id = su.supplier_id
       where x.sku_id = p_sku_id and x.is_primary;
      v_supc := core.canonical_supplier_code(v_sup.code);
      if v_supc is null or v_supc !~ '^[0-9]{4}$' then v_blk := v_blk || '代表の仕入先のコードが NE の形 (4 桁の数字) でない'::text; v_supc := null; end if;
      if v_sup.reg_state is not null and v_sup.reg_state <> 'ne_confirmed' then v_blk := v_blk || '代表の仕入先の「NE に登録した」の申告がまだ'::text; end if;
    end if;
    -- 代表 (親): なし = empty / あり = 名札か商品の NE の元の書き方 (名札を先に)
    if s.parent_product_id is not null then
      v_par := core.norm_code(coalesce(s.parent_code, ''));
      select c.state, c.ne_code into v_pr from ops.master_ne_codes c where c.code_norm = v_par and c.kind in ('rep', 'product')
       order by (c.kind = 'rep') desc limit 1;
      if coalesce(v_par, '') = '' or v_pr.state is distinct from 'ok' then v_blk := v_blk || '代表 (親) の NE の書き方が確かめられない'::text;
      else v_parc := v_pr.ne_code; end if;
    end if;
    select coalesce(pg_catalog.array_agg(e.external_value order by e.external_id_row), '{}') into v_jans from core.external_ids e
     where s.product_id is not null and e.entity_type = 'product' and e.entity_id = s.product_id and e.system = 'jan' and e.id_kind = 'jan' and e.valid_to is null;
    if pg_catalog.cardinality(v_jans) > 1 then v_blk := v_blk || 'JAN が 2 つ以上ある (NE には 1 つ)'::text; end if;
    if exists (select 1 from pg_catalog.unnest(v_jans) j where not ops.jan_check_ok(j)) then v_blk := v_blk || 'JAN のチェック数字が合わない'::text; end if;
    v_cells := pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_array(s.code, s.name, coalesce(v_supc, ''), coalesce(v_costv::text, ''), coalesce(v_price::text, ''),
      coalesce(v_taxc, ''), coalesce(v_hand, ''), v_parc, coalesce(v_jans[1], 'empty')));
    v_exp := pg_catalog.jsonb_build_object('kind', 'single', 'values', pg_catalog.jsonb_build_object('name', s.name, 'supplier', v_supc, 'cost', v_costv, 'price', v_price,
      'tax_rate', v_tax, 'handling', s.handling, 'parent', v_par));
  elsif s.sku_kind = 'set' then
    -- 構成 = 開いている構成の依頼 (新しいセットは core.sku_components が空) か、今の構成 (並び = 依頼の sort / 今の sort_order・コード)
    select q.rows into v_req from ops.sku_component_requests q where q.set_sku_id = p_sku_id and q.status = 'open';
    for r in
      select k.sku_id, k.code, k.code_norm, k.tax_rate, x.qty, rg.state as reg_state, nc.state as nc_state, nc.ne_code
        from (select (e ->> 'sku_id')::bigint as sku_id, (e ->> 'qty')::numeric as qty, (e ->> 'sort')::numeric as srt, null::text as cn
                from pg_catalog.jsonb_array_elements(coalesce(v_req, '[]'::jsonb)) e where v_req is not null
              union all
              select c.child_sku_id, c.qty::numeric, c.sort_order::numeric, null::text from core.sku_components c where v_req is null and c.parent_sku_id = p_sku_id) x
        left join core.skus k on k.sku_id = x.sku_id
        left join ops.master_registrations rg on rg.sku_id = x.sku_id
        left join ops.master_ne_codes nc on nc.kind = 'product' and nc.code_norm = k.code_norm
       order by x.srt, k.code_norm
    loop
      v_n := v_n + 1;
      if r.code is null then v_blk := v_blk || '構成品の SKU が無い'::text; continue; end if;
      if r.tax_rate is null or r.tax_rate not in (0.1, 0.08) then v_badtax := true; elsif r.tax_rate = 0.08 then v_ntax := v_ntax + 1; end if;
      if r.reg_state is null or r.reg_state not in ('ne_confirmed', 'distributable', 'available') then v_blk := v_blk || ('構成品 ' || r.code || ' が NE 確認済みでない'); end if;
      if r.nc_state is distinct from 'ok' then v_blk := v_blk || ('構成品 ' || r.code || ' の NE の書き方が確かめられない'); end if;
      if r.qty is null or r.qty <> pg_catalog.trunc(r.qty) or r.qty not between 1 and 999 then v_blk := v_blk || ('構成品 ' || r.code || ' の数量'); end if;
      v_cells := v_cells || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_array(s.code, s.name, coalesce(v_price::text, ''), '%TAX%', coalesce(r.ne_code, ''),
        coalesce(r.qty::bigint::text, '')));
      v_kids := v_kids || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code_norm', r.code_norm, 'qty', r.qty::bigint));
    end loop;
    if v_n = 0 then v_blk := v_blk || '構成品が無い'::text; end if;
    -- セットの税率 = 構成品の税率が全部分かるときだけ・混ざれば低い方 (lib/master-set-rules.js の deriveSetTaxCdb)
    v_taxc := case when v_badtax or v_n = 0 then null when v_ntax > 0 then '8' else '10' end;
    if v_taxc is null then v_blk := v_blk || 'セットの税率が決まらない (構成品の税率)'::text; end if;
    select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_set(c, '{3}', pg_catalog.to_jsonb(coalesce(v_taxc, ''))) order by o), '[]'::jsonb) into v_cells
      from pg_catalog.jsonb_array_elements(v_cells) with ordinality as t(c, o);
    v_exp := pg_catalog.jsonb_build_object('kind', 'set', 'values', pg_catalog.jsonb_build_object('name', s.name, 'price', v_price, 'children', v_kids));
  else
    v_blk := v_blk || '例外の SKU は NE に登録しない'::text;
  end if;
  return pg_catalog.jsonb_build_object('cells', v_cells, 'expected', v_exp, 'blockers', pg_catalog.to_jsonb(v_blk));
end $$;
revoke all on function ops.ne_reg_canonical(bigint, date) from public;

/**
 * 作る。p = { request_id, actor, kind, schema_version, header, ne_codes_run, cost_day (原価を見る東京の日),
 *             items: [{ sku_id, expected, rows: [[セル, ...], ...] }] }、p_bytes = 配る byte 列。
 * 関数が照らし直すもの (呼び手の判断を信じない):
 *   段階 new_open・形 (種類 × 版 × 見出し・在庫の列が無い)・同じ request_id = 同じファイル (種類・商品・作る人が違えば拒む)・
 *   商品ごと: SKU がある・種類が合う・コードの形・登録の状態 (下書き / NE 登録待ち)・生きているファイルが無い・NE の元のコード (最新の照合の回) に無い・
 *     🚨 行と確かめる値 = 鍵の後に Company DB の今の値から関数が作ったもの (ops.ne_reg_canonical) と完全に同じ (#1571 Codex R1 High 1)・
 *   byte 列 = 見出し + 行 (UTF-8・CRLF・最後の行にも CRLF) を関数が組み直したものと同じ・sha256 は関数が計算・
 *   印とハッシュ 4 つ (商品ごとの snapshot_hash・item_token / ファイルの payload_hash・aggregate_token) は関数が今の値から計算する (呼び手は送らない = 送れば拒む)・
 *   実機の確かめの門 (最後の記録が ok でなければ試し用 = 5 行まで)・1,000 行まで
 *   原価を見る日 (cost_day) はサーバーの東京の今日 - 1 日より前は拒む (古い原価を選ばせない。先の日付は試験の時計のため受ける = 本物の原価の行から選ぶだけ)
 */
create function ops.ne_reg_build(p jsonb, p_bytes bytea) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_rid      uuid;
  v_actor    text := pg_catalog.btrim(coalesce(p ->> 'actor', ''));
  v_kind     text := p ->> 'kind';
  v_schema   text := p ->> 'schema_version';
  v_header   text := p ->> 'header';
  v_day      date;
  v_cols     text[];
  v_prev     record;
  v_ids      bigint[];
  v_want     bigint[];
  it         jsonb;
  rw         jsonb;
  v_sku      record;
  v_reg      text;
  v_mark     text;
  v_latest   text;
  v_nrows    integer := 0;
  v_nitems   integer := 0;
  v_lines    text[] := '{}';
  v_text     text;
  v_bytes    bytea;
  v_sha      text;
  v_verified boolean;
  v_trial    boolean;
  v_export   bigint;
  v_item     bigint;
  v_row_no   integer := 0;
  v_from     integer;
  v_canon    jsonb;
  v_canons   jsonb := '[]'::jsonb;   -- [{ sku_id, code_norm, expected, cells, snapshot_hash, item_token }]
  v_snap     text;
  v_token    text;
  v_payload  text;
  v_agg      text;
  c          jsonb;
begin
  if v_actor = '' or pg_catalog.length(v_actor) > 320 then raise exception 'invalid_input: 作る人 (actor) が要る' using errcode = '22023'; end if;
  begin v_rid := (p ->> 'request_id')::uuid; exception when others then raise exception 'invalid_input: request_id の形が違う' using errcode = '22023'; end;
  if v_rid is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  -- 画面のロール = 同じ取引で ops.begin_master_write をした後だけ・誰が と request_id はその行と同じ (D)
  v_actor := ops.reg_write_session(v_actor, v_rid) ->> 'actor';
  if not ((v_kind = 'products' and v_schema ~ '^ne-reg-single-v[0-9]+$' and v_header like 'syohin_code,%')
       or (v_kind = 'sets' and v_schema ~ '^ne-reg-set-v[0-9]+$' and v_header like 'set_syohin_code,%')) then
    raise exception 'invalid_input: 種類・形の版・見出しが合わない (% / % / %)', v_kind, v_schema, v_header using errcode = '22023';
  end if;
  v_cols := pg_catalog.string_to_array(v_header, ',');
  if v_cols && array['zaiko_su', 'yoyaku_zaiko_su', 'nyusyukko_riyu', 'visible_flg'] then
    raise exception 'invalid_input: 在庫の列は出さない (%)', v_header using errcode = '22023';
  end if;
  -- 形の版ごとの見出し (版を上げたらここも ops.ne_reg_canonical も足す)
  if not ((v_schema = 'ne-reg-single-v1' and v_header = 'syohin_code,syohin_name,sire_code,genka_tnk,baika_tnk,tax_rate,toriatukai_kbn,daihyo_syohin_code,jan_code')
       or (v_schema = 'ne-reg-set-v1' and v_header = 'set_syohin_code,set_syohin_name,set_baika_tnk,tax_rate,syohin_code,suryo')) then
    raise exception 'invalid_input: 形の版 % の見出しが違う (%)', v_schema, v_header using errcode = '22023';
  end if;
  if pg_catalog.jsonb_typeof(p -> 'items') is distinct from 'array' or pg_catalog.jsonb_array_length(p -> 'items') not between 1 and 1000 then
    raise exception 'invalid_input: items は 1〜1000 件' using errcode = '22023';
  end if;
  -- 印・ハッシュは関数が計算する (呼び手の値は受けない)
  if p ? 'aggregate_token' or p ? 'payload_hash'
     or exists (select 1 from pg_catalog.jsonb_array_elements(p -> 'items') i where pg_catalog.jsonb_typeof(i) <> 'object' or i ? 'item_token' or i ? 'snapshot_hash') then
    raise exception 'caller_hash: 印とハッシュ (aggregate_token・payload_hash・item_token・snapshot_hash) は関数が計算する = 送らない' using errcode = '22023';
  end if;
  begin v_day := (p ->> 'cost_day')::date; exception when others then v_day := null; end;
  if v_day is null or v_day < (pg_catalog.now() at time zone 'Asia/Tokyo')::date - 1 then
    raise exception 'invalid_input: 原価を見る日 (cost_day) が無い / 東京の今日より前' using errcode = '22023';
  end if;
  perform ops.ne_reg_assert_open();
  -- 同じ request_id = 同じファイル (種類・商品・作る人が違えば拒む)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.ne_reg_request:' || v_rid::text, 0));
  select e.export_id, e.kind, e.created_by, e.state, e.sha256, e.trial into v_prev from ops.ne_reg_exports e where e.request_id = v_rid;
  if found then
    select pg_catalog.array_agg(distinct (i ->> 'sku_id')::bigint order by (i ->> 'sku_id')::bigint) into v_want from pg_catalog.jsonb_array_elements(p -> 'items') i;
    if v_prev.kind is distinct from v_kind or v_prev.created_by is distinct from v_actor
       or v_want is distinct from (select pg_catalog.array_agg(x.sku_id order by x.sku_id) from ops.ne_reg_export_items x where x.export_id = v_prev.export_id) then
      raise exception 'request_id_reused: 同じ番号 (request_id) で違う中身' using errcode = '23505';
    end if;
    return pg_catalog.jsonb_build_object('export_id', v_prev.export_id, 'state', v_prev.state, 'sha256', v_prev.sha256, 'trial', v_prev.trial, 'replayed', true);
  end if;
  -- 鍵: 商品と構成品 (今の構成 + 開いている構成の依頼) の SKU (sku_id の順) → CSV の鍵 → NE の元のコード (共有・照合の書き手と並ぶ)
  select pg_catalog.array_agg(distinct x) into v_ids from (
    select (i ->> 'sku_id')::bigint as x from pg_catalog.jsonb_array_elements(p -> 'items') i
    union all
    select c2.child_sku_id from pg_catalog.jsonb_array_elements(p -> 'items') i join core.sku_components c2 on c2.parent_sku_id = (i ->> 'sku_id')::bigint where v_kind = 'sets'
    union all
    select (e ->> 'sku_id')::bigint from pg_catalog.jsonb_array_elements(p -> 'items') i
      join ops.sku_component_requests q on q.set_sku_id = (i ->> 'sku_id')::bigint and q.status = 'open'
      cross join lateral pg_catalog.jsonb_array_elements(q.rows) e where v_kind = 'sets') t;
  perform ops.ne_reg_lock_skus(v_ids);
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  -- NE の元のコードは最新の照合の回のもの (呼び手の見た回と同じ)
  select m.compare_run_id into v_mark from ops.master_ne_code_mark m where m.id = 1;
  select r.compare_run_id into v_latest from ops.master_compare_runs r order by r.observed_at desc, r.compare_run_id desc limit 1;
  if v_mark is null or v_mark is distinct from v_latest or v_mark is distinct from (p ->> 'ne_codes_run') then
    raise exception 'ne_codes_stale: NE の元のコードが最新の照合の回のものでない (印 % / 最新 % / 画面 %)', v_mark, v_latest, p ->> 'ne_codes_run' using errcode = 'P0001';
  end if;
  v_lines := array[v_header];
  for it in select * from pg_catalog.jsonb_array_elements(p -> 'items') loop
    v_nitems := v_nitems + 1;
    select s.sku_id, s.code, s.code_norm, s.sku_kind, s.version, s.product_id, (select pr.version from core.products pr where pr.product_id = s.product_id) as product_version
      into v_sku from core.skus s where s.sku_id = (it ->> 'sku_id')::bigint for share;
    if not found then raise exception 'not_ready: SKU % が無い', it ->> 'sku_id' using errcode = 'P0001'; end if;
    if v_sku.sku_kind is distinct from (case v_kind when 'products' then 'single' else 'set' end) then
      raise exception 'not_ready: % は % でない', v_sku.code, v_kind using errcode = 'P0001';
    end if;
    if v_sku.code !~ '^[a-z0-9_-]{1,30}$' then raise exception 'not_ready: % は新しいコードの形でない', v_sku.code using errcode = 'P0001'; end if;
    select r.state into v_reg from ops.master_registrations r where r.sku_id = v_sku.sku_id for share;
    if v_reg is null or v_reg not in ('draft', 'ne_pending') then
      raise exception 'not_ready: % の登録の状態が % (下書き・NE 登録待ちだけ)', v_sku.code, coalesce(v_reg, 'なし') using errcode = 'P0001';
    end if;
    if exists (select 1 from ops.ne_reg_export_items x where x.sku_id = v_sku.sku_id and x.state in ('built', 'issued', 'import_declared', 'partial')) then
      raise exception 'not_ready: % にはまだ終わっていないファイルがある', v_sku.code using errcode = 'P0001';
    end if;
    if exists (select 1 from ops.master_ne_codes c2 where c2.kind = 'product' and c2.code_norm = v_sku.code_norm) then
      raise exception 'already_in_ne: コード % は NE にもうある = 新規登録しない (同じ登録か確かめる / 別のコード)', v_sku.code using errcode = 'P0001';
    end if;
    -- 🚨 行と確かめる値 = 鍵の後に関数が今の値から作ったものと完全に同じ (High 1)
    v_canon := ops.ne_reg_canonical(v_sku.sku_id, v_day);
    if pg_catalog.jsonb_array_length(v_canon -> 'blockers') > 0 then
      raise exception 'not_ready: % を CSV にできない (%)', v_sku.code, (select pg_catalog.string_agg(b, '・') from pg_catalog.jsonb_array_elements_text(v_canon -> 'blockers') b)
        using errcode = 'P0001';
    end if;
    if pg_catalog.jsonb_typeof(it -> 'rows') is distinct from 'array' or (it -> 'rows') is distinct from (v_canon -> 'cells') then
      raise exception 'not_canonical: % の行が Company DB の今の値から作った行と違う', v_sku.code using errcode = '22023';
    end if;
    if (it -> 'expected') is distinct from (v_canon -> 'expected') then
      raise exception 'not_canonical: % の確かめる値が Company DB の今の値と違う', v_sku.code using errcode = '22023';
    end if;
    if exists (select 1 from pg_catalog.jsonb_array_elements(v_canon -> 'cells') r2 where pg_catalog.jsonb_array_length(r2) <> pg_catalog.array_length(v_cols, 1)) then
      raise exception 'invalid_input: % の行のセルの数が見出しと違う', v_sku.code using errcode = '22023';
    end if;
    for rw in select * from pg_catalog.jsonb_array_elements(v_canon -> 'cells') loop
      v_nrows := v_nrows + 1;
      if exists (select 1 from pg_catalog.jsonb_array_elements_text(rw) c3 where ops.ne_reg_csv_cell(c3) is null) then
        raise exception 'invalid_input: % の行に書けない文字 (制御文字) がある', v_sku.code using errcode = '22023';
      end if;
      select pg_catalog.string_agg(ops.ne_reg_csv_cell(c4.value #>> '{}'), ',' order by c4.ordinality) into v_text
        from pg_catalog.jsonb_array_elements(rw) with ordinality c4;
      v_lines := v_lines || v_text;
    end loop;
    -- 照合で確かめる値 = 配る行のセル (二重の確かめ)
    if ops.ne_reg_expected_problem(v_kind, v_canon -> 'expected', v_canon -> 'cells') is not null then
      raise exception 'expected_mismatch: % の確かめる値 (%) が CSV の行と違う', v_sku.code, ops.ne_reg_expected_problem(v_kind, v_canon -> 'expected', v_canon -> 'cells') using errcode = '22023';
    end if;
    -- 商品ごとの印 (関数が計算する): snapshot = 確かめる値と行 / item_token = 版 + 登録の状態 + snapshot
    v_snap := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object('expected', v_canon -> 'expected', 'cells', v_canon -> 'cells')::text, 'UTF8')), 'hex');
    v_token := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object('v', 'nrt-1', 'sku', pg_catalog.jsonb_build_array(v_sku.sku_id, v_sku.version),
      'product', pg_catalog.jsonb_build_array(v_sku.product_id, v_sku.product_version), 'registration', v_reg, 'snapshot', v_snap)::text, 'UTF8')), 'hex');
    v_canons := v_canons || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('sku_id', v_sku.sku_id, 'code_norm', v_sku.code_norm, 'code', v_sku.code, 'sku_kind', v_sku.sku_kind,
      'expected', v_canon -> 'expected', 'cells', v_canon -> 'cells', 'snapshot_hash', v_snap, 'item_token', v_token));
  end loop;
  if v_nrows > 1000 then raise exception 'too_many: 1 つのファイルに 1,000 行まで (% 行)', v_nrows using errcode = '22023'; end if;
  -- byte 列 = 関数が組み直した CSV と同じ (UTF-8・BOM なし・CRLF・最後の行にも CRLF)
  v_bytes := pg_catalog.convert_to(pg_catalog.array_to_string(v_lines, E'\r\n') || E'\r\n', 'UTF8');
  if p_bytes is distinct from v_bytes then raise exception 'bytes_mismatch: 配る byte 列が行から組み直した CSV と違う' using errcode = '22023'; end if;
  v_sha := pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex');
  -- ファイルの印 (関数が計算する): payload = 形の版・見出し・商品ごとの確かめる値と行 / aggregate = 商品ごとの item_token
  v_payload := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object('schema', v_schema, 'header', v_header,
    'items', (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('code', x -> 'code_norm', 'expected', x -> 'expected', 'cells', x -> 'cells') order by o)
                from pg_catalog.jsonb_array_elements(v_canons) with ordinality t(x, o)))::text, 'UTF8')), 'hex');
  v_agg := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.jsonb_build_object('schema', v_schema,
    'tokens', (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(x -> 'code_norm', x -> 'item_token') order by o)
                 from pg_catalog.jsonb_array_elements(v_canons) with ordinality t(x, o)))::text, 'UTF8')), 'hex');
  -- 実機の確かめの門 (種類 × 形の版 × 見出し・最後の記録が ok)。確かめていない = 試し用 (5 行まで)
  select coalesce((select v.result = 'ok' from ops.ne_csv_verified v
                    where v.kind = v_kind and v.col = 'new_registration' and v.encoding = 'utf8' and v.header = v_header and v.converter_version = v_schema
                    order by v.verified_id desc limit 1), false) into v_verified;
  v_trial := not v_verified;
  if v_trial and v_nrows > 5 then
    raise exception 'trial_limit: 形 % は実機でまだ確かめていないので、試し用 = 5 行まで (% 行)', v_schema, v_nrows using errcode = 'P0001';
  end if;
  insert into ops.ne_reg_exports (kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes,
                                  request_id, ne_codes_run, cost_day, created_by)
    values (v_kind, v_schema, v_header, 'utf8', v_trial, v_nitems, v_nrows, v_agg, v_payload, v_sha, v_bytes, v_rid, v_mark, v_day, v_actor)
    returning export_id into v_export;
  for c in select * from pg_catalog.jsonb_array_elements(v_canons) loop
    v_from := v_row_no + 1;
    insert into ops.ne_reg_export_items (export_id, sku_id, code_norm, ne_code, sku_kind, item_token, expected, snapshot_hash, row_from, row_to, state_changed_by)
      values (v_export, (c ->> 'sku_id')::bigint, c ->> 'code_norm', c ->> 'code', c ->> 'sku_kind', c ->> 'item_token', c -> 'expected', c ->> 'snapshot_hash',
              v_from, v_from + pg_catalog.jsonb_array_length(c -> 'cells') - 1, v_actor)
      returning item_id into v_item;
    for rw in select * from pg_catalog.jsonb_array_elements(c -> 'cells') loop
      v_row_no := v_row_no + 1;
      insert into ops.ne_reg_export_rows (export_id, row_no, item_id, cells) values (v_export, v_row_no, v_item, rw);
    end loop;
  end loop;
  return pg_catalog.jsonb_build_object('export_id', v_export, 'state', 'built', 'sha256', v_sha, 'trial', v_trial, 'rows', v_nrows, 'items', v_nitems, 'replayed', false,
    'payload_hash', v_payload, 'aggregate_token', v_agg);
end $$;
revoke all on function ops.ne_reg_build(jsonb, bytea) from public;

/** ファイルと商品を鍵つきで読む (商品の SKU の鍵 → CSV の鍵 → ファイルの行 for update)。無ければ例外 */
create function ops.ne_reg_lock_export(p_export_id bigint) returns ops.ne_reg_exports
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  e ops.ne_reg_exports%rowtype;
begin
  perform ops.ne_reg_lock_skus((select pg_catalog.array_agg(i.sku_id) from ops.ne_reg_export_items i where i.export_id = p_export_id));
  select * into e from ops.ne_reg_exports x where x.export_id = p_export_id for update;
  if not found then raise exception 'not_found: ファイル % が無い', p_export_id using errcode = 'P0002'; end if;
  perform 1 from ops.ne_reg_export_items i where i.export_id = p_export_id order by i.item_id for update;
  return e;
end $$;
revoke all on function ops.ne_reg_lock_export(bigint) from public;

/** 配る (built → issued・商品も)。もう配った = そのまま。使わない商品・登録をやめた商品が 1 つでもある = 配らない (残りも使わないにして閉じる = 作り直す) */
create function ops.ne_reg_issue(p_export_id bigint, p_actor text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  e      ops.ne_reg_exports%rowtype;
  v_gone text[];
begin
  if coalesce(pg_catalog.btrim(p_actor), '') = '' then raise exception 'invalid_input: 配る人 (actor) が要る' using errcode = '22023'; end if;
  p_actor := ops.reg_write_session(p_actor) ->> 'actor';   -- 画面のロール = ops.begin_master_write の行から (D)
  perform ops.ne_reg_assert_open();
  e := ops.ne_reg_lock_export(p_export_id);
  if e.state = 'closed' then raise exception 'closed: ファイル % は閉じている (%)', p_export_id, e.close_reason using errcode = 'P0001'; end if;
  if e.state <> 'built' then return pg_catalog.jsonb_build_object('export_id', p_export_id, 'state', e.state, 'already', true); end if;
  select pg_catalog.array_agg(i.ne_code order by i.item_id) into v_gone
    from ops.ne_reg_export_items i left join ops.master_registrations r on r.sku_id = i.sku_id
   where i.export_id = p_export_id and (i.state <> 'built' or r.state is null or r.state not in ('draft', 'ne_pending'));
  if v_gone is not null then
    update ops.ne_reg_export_items set state = 'superseded', superseded_reason = pg_catalog.left('配る前に使えなくなった商品がある (' || pg_catalog.array_to_string(v_gone, '・') || ')', 500),
           superseded_correction = 'まだ配っていない (NE には何もしていない)。作り直す', state_changed_at = pg_catalog.now(), state_changed_by = p_actor
     where export_id = p_export_id and state = 'built';
    update ops.ne_reg_exports set state = 'closed', closed_at = pg_catalog.now(), closed_by = p_actor, close_reason = 'superseded' where export_id = p_export_id;
    return pg_catalog.jsonb_build_object('export_id', p_export_id, 'state', 'closed', 'refused', true, 'reason', 'item_superseded', 'codes', pg_catalog.to_jsonb(v_gone));
  end if;
  update ops.ne_reg_export_items set state = 'issued', state_changed_at = pg_catalog.now(), state_changed_by = p_actor where export_id = p_export_id and state = 'built';
  update ops.ne_reg_exports set state = 'issued', issued_at = pg_catalog.now(), issued_by = p_actor where export_id = p_export_id;
  return pg_catalog.jsonb_build_object('export_id', p_export_id, 'state', 'issued', 'already', false);
end $$;
revoke all on function ops.ne_reg_issue(bigint, text) from public;

/**
 * 取り込んだと申告する。sha256 = ファイルの記録と同じ (trigger も見る)・申告した人・時刻 (関数の now)・結果・NE のメッセージ。
 * ok / partial = 商品 issued → import_declared・登録の状態 draft → ne_pending (ops.transition_sku_registration が、この試みの記録を自分で読む) /
 * rejected_all = 商品 failed・ファイルを閉じる。申告したファイルにもう一度 = 試みを足すだけ
 */
create function ops.ne_reg_declare(p_export_id bigint, p_actor text, p_sha256 text, p_result text, p_ne_message text, p_imported_at timestamptz, p_note text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  e        ops.ne_reg_exports%rowtype;
  v_att    bigint;
  v_at     timestamptz;
  v_sku    bigint;
  v_moved  text[] := '{}';
  v_code   text;
  v_sess   jsonb;
begin
  if coalesce(pg_catalog.btrim(p_actor), '') = '' then raise exception 'invalid_input: 申告する人 (actor) が要る' using errcode = '22023'; end if;
  if coalesce(p_sha256, '') !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: sha256 (64 桁) が要る' using errcode = '22023'; end if;
  if p_result is null or p_result not in ('ok', 'partial', 'rejected_all') then raise exception 'invalid_input: 結果は ok / partial / rejected_all' using errcode = '22023'; end if;
  -- 画面のロール = ops.begin_master_write の行から 誰が・request_id (D。request_id は登録の状態の履歴に残す)
  v_sess := ops.reg_write_session(p_actor);
  p_actor := v_sess ->> 'actor';
  perform ops.ne_reg_assert_open();
  e := ops.ne_reg_lock_export(p_export_id);
  if e.sha256 <> p_sha256 then raise exception 'sha256_mismatch: sha256 がファイル % の記録と違う', p_export_id using errcode = 'P0001'; end if;
  if e.state = 'built' then raise exception 'not_issued: 先に配る (ダウンロード)' using errcode = 'P0001'; end if;
  if e.state = 'closed' then raise exception 'closed: ファイル % は閉じている (%)', p_export_id, e.close_reason using errcode = 'P0001'; end if;
  if p_imported_at is not null and (p_imported_at > pg_catalog.now() or p_imported_at < e.issued_at - interval '1 minute') then
    raise exception 'invalid_input: 取り込んだ時刻が配った時刻と今の間でない' using errcode = '22023';
  end if;
  insert into ops.ne_reg_attempts (export_id, sha256, declared_by, imported_at, result, ne_message, note)
    values (p_export_id, p_sha256, p_actor, p_imported_at, p_result, p_ne_message, p_note) returning attempt_id, declared_at into v_att, v_at;
  if e.state = 'declared' then
    return pg_catalog.jsonb_build_object('state', 'declared', 'attempt_id', v_att, 'first_declared_at', e.declared_at, 'again', true);
  end if;
  if p_result = 'rejected_all' then
    update ops.ne_reg_export_items set state = 'failed', failed_reason = 'rejected_all', state_changed_at = pg_catalog.now(), state_changed_by = p_actor
     where export_id = p_export_id and state = 'issued';
    update ops.ne_reg_exports set state = 'closed', closed_at = pg_catalog.now(), closed_by = p_actor, close_reason = 'rejected_all' where export_id = p_export_id;
    return pg_catalog.jsonb_build_object('state', 'closed', 'attempt_id', v_att);
  end if;
  update ops.ne_reg_export_items set state = 'import_declared', attempt_id = v_att, state_changed_at = pg_catalog.now(), state_changed_by = p_actor
   where export_id = p_export_id and state = 'issued';
  update ops.ne_reg_exports set state = 'declared', declared_at = v_at, declared_by = p_actor where export_id = p_export_id;
  for v_sku, v_code in select i.sku_id, i.ne_code from ops.ne_reg_export_items i join ops.master_registrations r on r.sku_id = i.sku_id
                        where i.export_id = p_export_id and i.state = 'import_declared' and r.state = 'draft' order by i.sku_id loop
    perform ops.transition_sku_registration(v_sku, 'ne_pending', 'human', p_actor, 'NE に新規登録の CSV を取り込んだ', '{}'::jsonb, v_sess ->> 'request_id');
    v_moved := v_moved || v_code;
  end loop;
  return pg_catalog.jsonb_build_object('state', 'declared', 'attempt_id', v_att, 'ne_pending', pg_catalog.to_jsonb(v_moved));
end $$;
revoke all on function ops.ne_reg_declare(bigint, text, text, text, text, timestamptz, text) from public;

/** 使わないにする (人)。理由と NE で何をしたか (直し方) が要る。まだ終わっていない商品を superseded・ファイルを閉じる。登録の状態は変えない */
create function ops.ne_reg_supersede(p_export_id bigint, p_actor text, p_reason text, p_correction text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  e    ops.ne_reg_exports%rowtype;
  v_n  integer;
begin
  if coalesce(pg_catalog.btrim(p_actor), '') = '' then raise exception 'invalid_input: 操作する人 (actor) が要る' using errcode = '22023'; end if;
  if coalesce(pg_catalog.btrim(p_reason), '') = '' or coalesce(pg_catalog.btrim(p_correction), '') = '' then
    raise exception 'invalid_input: 理由と NE で何をしたか (直し方) が要る' using errcode = '22023';
  end if;
  p_actor := ops.reg_write_session(p_actor) ->> 'actor';   -- 画面のロール = ops.begin_master_write の行から (D)
  perform ops.ne_reg_assert_open();
  e := ops.ne_reg_lock_export(p_export_id);
  if e.state = 'closed' then raise exception 'closed: ファイル % はもう閉じている (%)', p_export_id, e.close_reason using errcode = 'P0001'; end if;
  update ops.ne_reg_export_items set state = 'superseded', superseded_reason = pg_catalog.left(p_reason, 500), superseded_correction = pg_catalog.left(p_correction, 500),
         state_changed_at = pg_catalog.now(), state_changed_by = p_actor
   where export_id = p_export_id and state in ('built', 'issued', 'import_declared', 'partial');
  get diagnostics v_n = row_count;
  update ops.ne_reg_exports set state = 'closed', closed_at = pg_catalog.now(), closed_by = p_actor, close_reason = 'superseded' where export_id = p_export_id;
  return pg_catalog.jsonb_build_object('state', 'closed', 'superseded', v_n);
end $$;
revoke all on function ops.ne_reg_supersede(bigint, text, text, text) from public;

/**
 * 保存 (lib/master-write.mjs) の取引の中で呼ぶ (SKU の鍵と CSV の鍵の後): この SKU (と含むセット) の生きている新規登録の CSV。
 * 配った後 (issued / import_declared / partial) が 1 つでもある = 何もしないで知らせる (呼び手が 409) /
 * 作っただけ (built) だけ = そのファイルを閉じる (中の商品は全部 superseded = 作り直す)
 */
create function ops.ne_reg_guard_on_save(p_sku_ids bigint[], p_actor text, p_what text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_issued  jsonb;
  v_exports bigint[];
begin
  if coalesce(pg_catalog.btrim(p_actor), '') = '' then raise exception 'invalid_input: 保存する人 (actor) が要る' using errcode = '22023'; end if;
  p_actor := ops.reg_write_session(p_actor) ->> 'actor';   -- 画面のロール = 保存の ops.begin_master_write の行から (D)
  select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('export_id', i.export_id::text, 'sku_id', i.sku_id::text, 'code', s.code, 'state', i.state) order by i.item_id)
    into v_issued
    from ops.ne_reg_export_items i join core.skus s on s.sku_id = i.sku_id
   where i.sku_id = any (coalesce(p_sku_ids, '{}')) and i.state in ('issued', 'import_declared', 'partial');
  if v_issued is not null then return pg_catalog.jsonb_build_object('issued', v_issued, 'superseded', '[]'::jsonb); end if;
  select pg_catalog.array_agg(distinct i.export_id) into v_exports from ops.ne_reg_export_items i where i.sku_id = any (coalesce(p_sku_ids, '{}')) and i.state = 'built';
  if v_exports is null then return pg_catalog.jsonb_build_object('issued', '[]'::jsonb, 'superseded', '[]'::jsonb); end if;
  update ops.ne_reg_export_items set state = 'superseded', superseded_reason = pg_catalog.left('配る前に ' || coalesce(p_what, '') || 'を直した', 500),
         superseded_correction = 'まだ配っていない (NE には何もしていない)。作り直す', state_changed_at = pg_catalog.now(), state_changed_by = p_actor
   where export_id = any (v_exports) and state = 'built';
  update ops.ne_reg_exports set state = 'closed', closed_at = pg_catalog.now(), closed_by = p_actor, close_reason = 'superseded' where export_id = any (v_exports) and state = 'built';
  return pg_catalog.jsonb_build_object('issued', '[]'::jsonb, 'superseded', (select pg_catalog.jsonb_agg(x::text order by x) from pg_catalog.unnest(v_exports) x));
end $$;
revoke all on function ops.ne_reg_guard_on_save(bigint[], text, text) from public;

/**
 * 実機で確かめた結果を残す (種類 × 形の版 × 見出し)。ok = 同じ形の試しのファイル (p_export_id) の商品が全部 NE で確かめ済み (verified) のときだけ / ng はいつでも
 */
create function ops.ne_reg_record_verified(p_actor text, p_kind text, p_schema text, p_header text, p_result text, p_note text, p_export_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  e     ops.ne_reg_exports%rowtype;
  v_bad integer;
  v_id  bigint;
begin
  if coalesce(pg_catalog.btrim(p_actor), '') = '' then raise exception 'invalid_input: 確かめた人 (actor) が要る' using errcode = '22023'; end if;
  if p_kind is null or p_kind not in ('products', 'sets') then raise exception 'invalid_input: 種類は products / sets' using errcode = '22023'; end if;
  if p_result is null or p_result not in ('ok', 'ng') then raise exception 'invalid_input: 結果は ok / ng' using errcode = '22023'; end if;
  if p_result = 'ok' and p_export_id is null then raise exception 'invalid_input: ok は確かめに使った試しのファイルの番号が要る' using errcode = '22023'; end if;
  p_actor := ops.reg_write_session(p_actor) ->> 'actor';   -- 画面のロール = ops.begin_master_write の行から (D)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_csv'));
  if p_export_id is not null then
    select * into e from ops.ne_reg_exports x where x.export_id = p_export_id for share;
    if not found then raise exception 'not_found: ファイル % が無い', p_export_id using errcode = 'P0002'; end if;
    if e.kind <> p_kind or e.schema_version <> p_schema or e.header <> p_header then
      raise exception 'export_mismatch: ファイルの種類・形の版・見出しが今の形と違う' using errcode = 'P0001';
    end if;
    if p_result = 'ok' then
      select pg_catalog.count(*) filter (where i.state <> 'verified') into v_bad from ops.ne_reg_export_items i where i.export_id = p_export_id;
      if v_bad > 0 or not exists (select 1 from ops.ne_reg_export_items i where i.export_id = p_export_id) then
        raise exception 'not_verified: ファイル % の商品がまだ全部 NE で確かめ済みになっていない', p_export_id using errcode = 'P0001';
      end if;
    end if;
  end if;
  insert into ops.ne_csv_verified (export_id, kind, col, encoding, header, converter_version, result, note, verified_by, reg_export_id)
    values (null, p_kind, 'new_registration', 'utf8', p_header, p_schema, p_result, p_note, p_actor, p_export_id) returning verified_id into v_id;
  return pg_catalog.jsonb_build_object('verified_id', v_id);
end $$;
revoke all on function ops.ne_reg_record_verified(text, text, text, text, text, text, bigint) from public;

-- 翌朝の照合 ② の新規登録の確かめ (#1571 Codex R1 High 2): 呼び手の JSON を確かめに使わない。3 段 (どれも watch_writer・security definer の関数だけ):
--   1. ops.record_ne_registration_observations = 回ごとに 1 回、取得の世代・時刻・原本のハッシュ・確かめ待ちの商品・観測を残す (DB が観測のハッシュを計算)
--   2. ops.seal_ne_registration_run = 回が最後まで終わった受け取り (観測のハッシュ = 1. と同じ・結果の JSON の sha256)。受け取りの後は観測を足せない
--   3. ops.record_ne_registration_check(回の番号) = 受け取りと残した観測を関数が自分で読んで確かめる (観測のハッシュを数え直して受け取りと同じときだけ)
-- 🚨 3 つの表は追記だけ (変えない・消さない)。照合の書き手 (apps/company-db/master-compare/run.mjs) は 1. → (基準・結果の JSON) → 2. → 完了の証跡 → 3. の順
create table ops.ne_reg_compare_runs (
  compare_run_id    text primary key references ops.master_compare_runs (compare_run_id),
  fetch_generation  text not null check (fetch_generation ~ '^[A-Za-z0-9_.:-]{1,120}$'),   -- 材料の世代 (照合が読んだ今朝の世代)
  products_at       timestamptz not null,   -- NE の完全な取得の時刻 (単品)
  sets_at           timestamptz not null,   -- NE の完全な取得の時刻 (セット)
  products_rev      text not null check (length(products_rev) between 1 and 80),
  sets_rev          text not null check (length(sets_rev) between 1 and 80),
  raw_hash          text not null check (raw_hash ~ '^[0-9a-f]{64}$'),
  absence_trusted   boolean not null,       -- 取得で落ちた行が無い = 「無い」を信じてよい
  target_codes      text[] not null,        -- 確かめ待ちの商品 (照合が読んだ時)
  observation_count integer not null check (observation_count >= 0),
  observation_hash  text not null check (observation_hash ~ '^[0-9a-f]{64}$'),   -- 関数が計算 (観測をコードの順に並べた jsonb の sha256)
  recorded_at       timestamptz not null default pg_catalog.clock_timestamp()
);
select core.make_append_only('ops', 'ne_reg_compare_runs');
create table ops.ne_reg_compare_observations (
  compare_run_id text not null references ops.ne_reg_compare_runs (compare_run_id),
  code_norm      text not null check (length(code_norm) between 1 and 80),
  observation    jsonb not null check (jsonb_typeof(observation) = 'object'),
  primary key (compare_run_id, code_norm)
);
select core.make_append_only('ops', 'ne_reg_compare_observations');
create table ops.ne_reg_compare_receipts (
  compare_run_id   text primary key references ops.ne_reg_compare_runs (compare_run_id),
  observation_hash text not null check (observation_hash ~ '^[0-9a-f]{64}$'),
  evidence_sha256  text not null check (evidence_sha256 ~ '^[0-9a-f]{64}$'),   -- 照合の結果の JSON の sha256 (miniPC の証跡と照らせる)
  sealed_at        timestamptz not null default pg_catalog.clock_timestamp()
);
select core.make_append_only('ops', 'ne_reg_compare_receipts');
comment on table ops.ne_reg_compare_receipts is '照合の回が最後まで終わった受け取り (0052・#1571 R1 High 2)。これがある回だけ ops.record_ne_registration_check が確かめる';
-- 受け取りの後の回には観測を足せない (持ち主のロールでも)
create function ops.guard_ne_reg_compare_observations() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if exists (select 1 from ops.ne_reg_compare_receipts r where r.compare_run_id = new.compare_run_id) then
    raise exception 'sealed_run: 受け取りの後の回 % に観測は足せない', new.compare_run_id using errcode = 'P0001';
  end if;
  return new;
end $$;
create trigger trg_ne_reg_compare_observations_sealed before insert on ops.ne_reg_compare_observations for each row execute function ops.guard_ne_reg_compare_observations();

/** 残した観測のハッシュ (コードの順に並べた jsonb の sha256)。観測が無い回 = 空の配列のハッシュ */
create function ops.ne_reg_observation_hash(p_run text) returns text language sql stable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce((select pg_catalog.jsonb_agg(o.observation order by o.code_norm collate "C")
    from ops.ne_reg_compare_observations o where o.compare_run_id = p_run), '[]'::jsonb)::text, 'UTF8')), 'hex')
$$;
revoke all on function ops.ne_reg_observation_hash(text) from public;

/** 観測 1 つの形 (どれか違えば理由・よければ null)。列 = { st: ok | no_value | invalid, v } */
create function ops.ne_reg_observation_problem(o jsonb) returns text language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  k text;
begin
  if pg_catalog.jsonb_typeof(o) is distinct from 'object' or coalesce(o ->> 'code_norm', '') = '' then return 'code_norm'; end if;
  if pg_catalog.jsonb_typeof(o -> 'present') is distinct from 'boolean' or pg_catalog.jsonb_typeof(o -> 'trusted') is distinct from 'boolean' then return 'present・trusted'; end if;
  if (o -> 'present') = 'false'::jsonb then return null; end if;
  if (o ->> 'kind') is null or (o ->> 'kind') not in ('single', 'set') or pg_catalog.jsonb_typeof(o -> 'cols') is distinct from 'object' then return 'kind・cols'; end if;
  for k in select pg_catalog.jsonb_object_keys(o -> 'cols') loop
    if pg_catalog.jsonb_typeof(o -> 'cols' -> k) is distinct from 'object' or (o -> 'cols' -> k ->> 'st') is null or (o -> 'cols' -> k ->> 'st') not in ('ok', 'no_value', 'invalid') then
      return 'cols.' || k;
    end if;
  end loop;
  if (o ->> 'kind') = 'set' and (pg_catalog.jsonb_typeof(o -> 'children') is distinct from 'array'
      or exists (select 1 from pg_catalog.jsonb_array_elements(o -> 'children') c where pg_catalog.jsonb_typeof(c) <> 'object' or coalesce(c ->> 'code_norm', '') = ''
                  or (c ->> 'st') is null or (c ->> 'st') not in ('ok', 'no_value', 'invalid'))) then
    return 'children';
  end if;
  return null;
end $$;

/**
 * 1. 観測を残す (照合 ② の回ごとに 1 回)。p = { compare_run_id, fetch: { generation_id, products_rev, sets_rev, raw_hash }, products_at, sets_at,
 *    absence_trusted, targets: [code_norm], observations: [{ code_norm, present, trusted, kind, cols, children }] }
 * 確かめること: 照合の回がある・取得の時刻が照合の回の時刻 (+5 分) より後でない・確かめ待ちの商品は新規登録の CSV の商品で重ならない・観測は確かめ待ちの商品ごとにちょうど 1 つ・形。
 * 同じ回の 2 回目 = 中身 (観測のハッシュと取得) が同じなら何もしない・違えば拒む。戻り値 { state, observation_hash, observations }
 */
create function ops.record_ne_registration_observations(p jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_run   text := p ->> 'compare_run_id';
  f       jsonb := p -> 'fetch';
  v_pat   timestamptz;
  v_sat   timestamptz;
  v_tgts  text[];
  v_runat timestamptz;
  v_prev  ops.ne_reg_compare_runs%rowtype;
  v_hash  text;
  v_bad   text;
  v_n     integer;
begin
  if v_run is null or v_run !~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'invalid_input: compare_run_id の形が違う: %', v_run using errcode = '22023'; end if;
  select m.observed_at into v_runat from ops.master_compare_runs m where m.compare_run_id = v_run;
  if not found then raise exception 'unknown_run: 照合の回の記録が無い: %', v_run using errcode = '23503'; end if;
  if pg_catalog.jsonb_typeof(f) is distinct from 'object' or coalesce(f ->> 'generation_id', '') !~ '^[A-Za-z0-9_.:-]{1,120}$'
     or coalesce(f ->> 'raw_hash', '') !~ '^[0-9a-f]{64}$' or coalesce(pg_catalog.length(f ->> 'products_rev'), 0) not between 1 and 80
     or coalesce(pg_catalog.length(f ->> 'sets_rev'), 0) not between 1 and 80 then
    raise exception 'invalid_input: 取得の世代 (fetch = generation_id・products_rev・sets_rev・raw_hash) が要る' using errcode = '22023';
  end if;
  if not ops.cutover_is_ts(p ->> 'products_at') or not ops.cutover_is_ts(p ->> 'sets_at') then raise exception 'invalid_input: products_at / sets_at が読めない' using errcode = '22023'; end if;
  v_pat := (p ->> 'products_at')::timestamptz;
  v_sat := (p ->> 'sets_at')::timestamptz;
  -- 取得は照合の回より前 (回の時刻 = 判断の台帳に書いた照合の始まり。5 分の幅)
  if v_pat > v_runat + interval '5 minutes' or v_sat > v_runat + interval '5 minutes' then
    raise exception 'invalid_input: 取得の時刻が照合の回 (%) より後になっている', v_runat using errcode = '22023';
  end if;
  if pg_catalog.jsonb_typeof(p -> 'absence_trusted') is distinct from 'boolean' then raise exception 'invalid_input: absence_trusted (true / false) が要る' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(p -> 'targets') is distinct from 'array' or pg_catalog.jsonb_typeof(p -> 'observations') is distinct from 'array'
     or exists (select 1 from pg_catalog.jsonb_array_elements(p -> 'targets') t where pg_catalog.jsonb_typeof(t) <> 'string' or (t #>> '{}') = '') then
    raise exception 'invalid_input: targets・observations が配列でない' using errcode = '22023';
  end if;
  select coalesce(pg_catalog.array_agg(t order by t collate "C"), '{}'), pg_catalog.count(distinct t) into v_tgts, v_n from pg_catalog.jsonb_array_elements_text(p -> 'targets') t;
  if v_n <> pg_catalog.cardinality(v_tgts) then raise exception 'invalid_input: targets が重なる' using errcode = '22023'; end if;
  -- 確かめ待ちの商品 = 新規登録の CSV の商品 (知らないコードの観測を残さない)
  if exists (select 1 from pg_catalog.unnest(v_tgts) t where not exists (select 1 from ops.ne_reg_export_items i where i.code_norm = t)) then
    raise exception 'invalid_input: targets に新規登録の CSV の商品でないコードがある' using errcode = '22023';
  end if;
  -- 観測 = 確かめ待ちの商品ごとにちょうど 1 つ・形
  if (select coalesce(pg_catalog.array_agg(o ->> 'code_norm' order by o ->> 'code_norm' collate "C"), '{}') from pg_catalog.jsonb_array_elements(p -> 'observations') o) is distinct from v_tgts then
    raise exception 'invalid_input: 観測が確かめ待ちの商品ごとにちょうど 1 つでない' using errcode = '22023';
  end if;
  select pg_catalog.min(x.problem) into v_bad from (select ops.ne_reg_observation_problem(o) as problem from pg_catalog.jsonb_array_elements(p -> 'observations') o) x;
  if v_bad is not null then raise exception 'invalid_input: 観測の形が違う (%)', v_bad using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.ne_reg_compare_run:' || v_run, 0));
  v_hash := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(coalesce((select pg_catalog.jsonb_agg(o order by o ->> 'code_norm' collate "C")
    from pg_catalog.jsonb_array_elements(p -> 'observations') o), '[]'::jsonb)::text, 'UTF8')), 'hex');
  select * into v_prev from ops.ne_reg_compare_runs r where r.compare_run_id = v_run;
  if found then
    if v_prev.observation_hash = v_hash and v_prev.fetch_generation = (f ->> 'generation_id') and v_prev.raw_hash = (f ->> 'raw_hash') then
      return pg_catalog.jsonb_build_object('state', 'unchanged', 'observation_hash', v_hash, 'observations', v_prev.observation_count);
    end if;
    raise exception 'run_conflict: 照合の回 % の観測はもう残っていて中身が違う', v_run using errcode = '23505';
  end if;
  insert into ops.ne_reg_compare_runs (compare_run_id, fetch_generation, products_at, sets_at, products_rev, sets_rev, raw_hash, absence_trusted, target_codes, observation_count, observation_hash)
    values (v_run, f ->> 'generation_id', v_pat, v_sat, f ->> 'products_rev', f ->> 'sets_rev', f ->> 'raw_hash', (p ->> 'absence_trusted')::boolean, v_tgts,
            pg_catalog.jsonb_array_length(p -> 'observations'), v_hash);
  insert into ops.ne_reg_compare_observations (compare_run_id, code_norm, observation)
    select v_run, o ->> 'code_norm', o from pg_catalog.jsonb_array_elements(p -> 'observations') o;
  if ops.ne_reg_observation_hash(v_run) is distinct from v_hash then raise exception 'observation_hash_mismatch: 残した観測のハッシュが違う' using errcode = 'P0001'; end if;
  return pg_catalog.jsonb_build_object('state', 'written', 'observation_hash', v_hash, 'observations', pg_catalog.jsonb_array_length(p -> 'observations'));
end $$;
revoke all on function ops.record_ne_registration_observations(jsonb) from public;

/** 2. 受け取り (回が最後まで終わった)。観測のハッシュ = 残した観測から数え直したもの = 1. のときと同じ。同じ回の 2 回目 = 同じなら何もしない・違えば拒む */
create function ops.seal_ne_registration_run(p_run text, p_observation_hash text, p_evidence_sha256 text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  h      ops.ne_reg_compare_runs%rowtype;
  v_prev ops.ne_reg_compare_receipts%rowtype;
begin
  if coalesce(p_observation_hash, '') !~ '^[0-9a-f]{64}$' or coalesce(p_evidence_sha256, '') !~ '^[0-9a-f]{64}$' then
    raise exception 'invalid_input: 観測のハッシュ・結果の sha256 (64 桁) が要る' using errcode = '22023';
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.ne_reg_compare_run:' || coalesce(p_run, ''), 0));
  select * into h from ops.ne_reg_compare_runs r where r.compare_run_id = p_run;
  if not found then raise exception 'no_observations: 照合の回 % の観測が残っていない (先に 1. を)', p_run using errcode = 'P0001'; end if;
  if h.observation_hash is distinct from p_observation_hash or ops.ne_reg_observation_hash(p_run) is distinct from h.observation_hash then
    raise exception 'observation_hash_mismatch: 照合の回 % の観測のハッシュが違う', p_run using errcode = 'P0001';
  end if;
  select * into v_prev from ops.ne_reg_compare_receipts r where r.compare_run_id = p_run;
  if found then
    if v_prev.evidence_sha256 = p_evidence_sha256 then return pg_catalog.jsonb_build_object('state', 'unchanged', 'compare_run_id', p_run); end if;
    raise exception 'run_conflict: 照合の回 % の受け取りはもうあって結果の sha256 が違う', p_run using errcode = '23505';
  end if;
  insert into ops.ne_reg_compare_receipts (compare_run_id, observation_hash, evidence_sha256) values (p_run, p_observation_hash, p_evidence_sha256);
  return pg_catalog.jsonb_build_object('state', 'sealed', 'compare_run_id', p_run);
end $$;
revoke all on function ops.seal_ne_registration_run(text, text, text) from public;

/**
 * 3. 確かめる (回の番号だけ・1 回 = 1 つの取引)。受け取りのある回だけ。観測・取得の時刻・「無い」を信じてよいかは関数が残した記録から読む
 *    (残した観測のハッシュを数え直して、受け取りと 1. のときと同じであること = 受け取りの後に変わっていない)。
 * 生きている商品 (issued / import_declared / partial) ごとに:
 *   issued (配ったが申告していない) で NE にある = in_ne_undeclared を残すだけ (申告が要る = 状態は変えない)
 *   申告の前の取得・信用できない観測 = waiting / 申告の後の完全な取得に無い = failed (not_in_ne・「無い」を信じてよいときだけ)
 *   ある = 全部の列が合えば verified (+ 登録の状態 ne_pending → ne_confirmed = ops.transition_sku_registration がこの確かめの記録を自分で読む)・違う列があれば partial
 * 同じ回の同じ商品は 1 回だけ (再送 = 何もしない)。鍵の順 = 回の鍵 → SKU ごと (sku_id の順) → CSV の鍵 → 行
 */
create function ops.record_ne_registration_check(p_run text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  h          ops.ne_reg_compare_runs%rowtype;
  rc         ops.ne_reg_compare_receipts%rowtype;
  it         record;
  v_obs      jsonb;
  v_fetched  timestamptz;
  v_cmp      jsonb;
  v_out      text;
  v_counts   jsonb := '{}'::jsonb;
  v_reg      text;
begin
  if p_run is null or p_run !~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'invalid_input: compare_run_id の形が違う: %', p_run using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_reg_check'));
  select * into rc from ops.ne_reg_compare_receipts r where r.compare_run_id = p_run;
  if not found then raise exception 'not_sealed: 照合の回 % は最後まで終わった受け取りが無い = 確かめない', p_run using errcode = 'P0001'; end if;
  select * into h from ops.ne_reg_compare_runs r where r.compare_run_id = p_run;
  if h.observation_hash is distinct from rc.observation_hash or ops.ne_reg_observation_hash(p_run) is distinct from rc.observation_hash then
    raise exception 'receipt_mismatch: 照合の回 % の観測が受け取りと違う', p_run using errcode = 'P0001';
  end if;
  -- 鍵: SKU ごと (sku_id の順・保存と同じ鍵) → CSV の鍵
  perform ops.ne_reg_lock_skus((select pg_catalog.array_agg(t.sku_id) from ops.v_ne_reg_targets t
                                 where t.code_norm in (select o.code_norm from ops.ne_reg_compare_observations o where o.compare_run_id = p_run)));
  for it in select i.*, e.declared_at as export_declared_at
              from ops.ne_reg_export_items i join ops.ne_reg_exports e on e.export_id = i.export_id
             where i.state in ('issued', 'import_declared', 'partial')
               and i.code_norm in (select o.code_norm from ops.ne_reg_compare_observations o where o.compare_run_id = p_run)
             order by i.sku_id for update of i loop
    if exists (select 1 from ops.ne_reg_checks c where c.compare_run_id = p_run and c.item_id = it.item_id) then continue; end if;
    select o.observation into v_obs from ops.ne_reg_compare_observations o where o.compare_run_id = p_run and o.code_norm = it.code_norm;
    v_fetched := case when it.sku_kind = 'set' then h.sets_at else h.products_at end;
    v_cmp := null;
    if it.state = 'issued' then
      if (v_obs -> 'present') = 'true'::jsonb then v_out := 'in_ne_undeclared'; else continue; end if;
    elsif it.export_declared_at is null or v_fetched <= it.export_declared_at then
      v_out := 'waiting';
    elsif (v_obs -> 'trusted') is distinct from 'true'::jsonb then
      v_out := 'waiting';
    elsif (v_obs -> 'present') is distinct from 'true'::jsonb then
      v_out := case when h.absence_trusted then 'failed' else 'waiting' end;
    else
      v_cmp := ops.ne_reg_compare(it.expected, v_obs);
      v_out := case when (v_cmp -> 'ok') = 'true'::jsonb then 'verified' else 'partial' end;
    end if;
    insert into ops.ne_reg_checks (compare_run_id, item_id, sku_id, fetched_at, outcome, detail)
      values (p_run, it.item_id, it.sku_id, v_fetched, v_out,
              pg_catalog.jsonb_build_object('state_before', it.state, 'compare', v_cmp, 'present', v_obs -> 'present', 'trusted', v_obs -> 'trusted',
                'fetch_generation', h.fetch_generation, 'raw_hash', h.raw_hash, 'evidence_sha256', rc.evidence_sha256));
    if v_out = 'verified' then
      update ops.ne_reg_export_items set state = 'verified', verified_run = p_run, verified_at = pg_catalog.now(), state_changed_at = pg_catalog.now(), state_changed_by = 'ne_compare'
       where item_id = it.item_id;
      select r.state into v_reg from ops.master_registrations r where r.sku_id = it.sku_id;
      if v_reg = 'ne_pending' then
        perform ops.transition_sku_registration(it.sku_id, 'ne_confirmed', 'system', 'ne_compare', null, '{}'::jsonb, null);
      end if;
    elsif v_out = 'partial' and it.state = 'import_declared' then
      update ops.ne_reg_export_items set state = 'partial', state_changed_at = pg_catalog.now(), state_changed_by = 'ne_compare' where item_id = it.item_id;
    elsif v_out = 'failed' then
      update ops.ne_reg_export_items set state = 'failed', failed_reason = 'not_in_ne', state_changed_at = pg_catalog.now(), state_changed_by = 'ne_compare' where item_id = it.item_id;
    end if;
    v_counts := v_counts || pg_catalog.jsonb_build_object(v_out, coalesce((v_counts ->> v_out)::integer, 0) + 1);
  end loop;
  -- 全部の商品が終わったファイルは閉じる
  update ops.ne_reg_exports e set state = 'closed', closed_at = pg_catalog.now(), closed_by = 'ne_compare', close_reason = 'finished'
   where e.state in ('issued', 'declared')
     and not exists (select 1 from ops.ne_reg_export_items i where i.export_id = e.export_id and i.state in ('built', 'issued', 'import_declared', 'partial'));
  return pg_catalog.jsonb_build_object('compare_run_id', p_run, 'counts', v_counts);
end $$;
revoke all on function ops.record_ne_registration_check(text) from public;

/**
 * 0051 の状態の関数を置き換える (引数は同じ)。ne_pending・ne_confirmed の根拠は、関数が 0052 の記録から自分で読んで鍵を取る
 * (呼び手の渡す根拠の JSON は信じない = 渡したら拒む caller_evidence)。distributable / available は ④ まで not_ready のまま。
 *   ne_pending   ← この SKU の新規登録の CSV の品目が import_declared・その試み (結果 ok / partial・sha256 = ファイルの記録) がある (人)
 *   ne_confirmed ← ne_pending から: この SKU の照合の確かめ (ops.ne_reg_checks) が verified・その品目が verified で同じ照合の回・回の記録がある (system)
 *                  quarantined から: NE で見つけた商品の照合の結果の表はまだ無い = not_ready
 */
create or replace function ops.transition_sku_registration(p_sku_id bigint, p_to text, p_actor_type text, p_actor_id text,
                                                           p_reason text default null, p_evidence jsonb default '{}'::jsonb, p_request_id text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  r        ops.master_registrations%rowtype;
  v_ev     jsonb := coalesce(p_evidence, '{}'::jsonb);
  v_rec    record;
  v_event  bigint;
begin
  if p_actor_type is null or p_actor_type not in ('human', 'system') then raise exception 'invalid_input: actor_type は human か system' using errcode = '22023'; end if;
  if p_actor_id is null or pg_catalog.length(p_actor_id) = 0 then raise exception 'invalid_input: 誰が (actor_id) が要る' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(v_ev) <> 'object' then raise exception 'invalid_input: 根拠 (evidence) は object' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  select * into r from ops.master_registrations x where x.sku_id = p_sku_id for update;
  if not found then raise exception 'no_registration: SKU % に状態の行が無い (使えない商品)', p_sku_id using errcode = 'P0001'; end if;
  if not ops.registration_transition_allowed(r.state, p_to) then
    raise exception 'one_way: 状態は % から % に進めない', r.state, p_to using errcode = 'P0001';
  end if;
  if p_to in ('distributable', 'available') then
    raise exception 'not_ready: % への根拠 (%) の表がまだ無い = 進めない (④ で作る)', p_to, case p_to when 'distributable' then '配る世代' else '場所ごとの受け取り' end
      using errcode = 'P0001';
  end if;
  if p_to in ('ne_pending', 'ne_confirmed') then
    -- NE で見つけた商品 (要確認) を NE 確認済みにする根拠 (照合の結果の表) はまだ無い = 開いていない (呼び手の根拠の有無より先に)
    if p_to = 'ne_confirmed' and r.state = 'quarantined' then
      raise exception 'not_ready: NE で見つけた商品を NE 確認済みにする根拠 (照合の結果の表) はまだ無い' using errcode = 'P0001';
    end if;
    if v_ev <> '{}'::jsonb then
      raise exception 'caller_evidence: % の根拠は関数が記録 (新規登録の CSV・照合の確かめ) から読む。呼び手の根拠は受けない', p_to using errcode = '22023';
    end if;
    if p_to = 'ne_pending' then
      if p_actor_type <> 'human' then raise exception 'no_evidence: NE 登録待ちは人の申告 (取り込んだ) で' using errcode = '22023'; end if;
      select i.item_id, i.export_id, a.attempt_id, a.sha256, a.result, a.declared_by, a.declared_at into v_rec
        from ops.ne_reg_export_items i
        join ops.ne_reg_exports e on e.export_id = i.export_id
        join ops.ne_reg_attempts a on a.attempt_id = i.attempt_id and a.export_id = i.export_id
       where i.sku_id = p_sku_id and i.state = 'import_declared' and e.state = 'declared' and a.sha256 = e.sha256 and a.result in ('ok', 'partial')
       order by i.item_id desc limit 1
       for share of i, e;
      if not found then
        raise exception 'no_evidence: SKU % に、取り込んだと申告した新規登録の CSV の品目が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('export_id', v_rec.export_id, 'item_id', v_rec.item_id, 'attempt_id', v_rec.attempt_id, 'sha256', v_rec.sha256,
                                            'result', v_rec.result, 'declared_by', v_rec.declared_by, 'declared_at', v_rec.declared_at);
    else
      if p_actor_type <> 'system' then raise exception 'no_evidence: NE 確認済みは翌朝の照合 (system) で' using errcode = '22023'; end if;
      select c.check_id, c.compare_run_id, c.item_id, i.export_id, c.fetched_at into v_rec
        from ops.ne_reg_checks c
        join ops.ne_reg_export_items i on i.item_id = c.item_id and i.sku_id = c.sku_id
       where c.sku_id = p_sku_id and c.outcome = 'verified' and i.state = 'verified' and i.verified_run = c.compare_run_id
         and exists (select 1 from ops.master_compare_runs m where m.compare_run_id = c.compare_run_id)
       order by c.check_id desc limit 1
       for share of i;
      if not found then
        raise exception 'no_evidence: SKU % に、NE の完全な取得で全部の列が合った確かめ (verified) が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('compare_run_id', v_rec.compare_run_id, 'check_id', v_rec.check_id, 'item_id', v_rec.item_id, 'export_id', v_rec.export_id,
                                            'fetched_at', v_rec.fetched_at, 'matched', true);
    end if;
  else
    -- cancelled = 人が理由を書いてだけ
    if p_actor_type <> 'human' or coalesce(pg_catalog.length(pg_catalog.btrim(p_reason)), 0) = 0 then raise exception 'no_evidence: やめるのは人が理由を書いてだけ' using errcode = '22023'; end if;
  end if;
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  update ops.master_registrations set state = p_to, state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
    values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
  return pg_catalog.jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
end $$;
revoke all on function ops.transition_sku_registration(bigint, text, text, text, text, jsonb, text) from public;

-- ═══════════ B. JAN の変更の記録 (H6) ═══════════
-- 🚨 NOT VALID で足す (#1571 Codex R1 Low): 前からの行を全部読んで確かめるのを、この migration の取引 (強い鍵) の中でしない。
--    前からの行は前の CHECK (external_id を含まない狭い集合) を満たしている = 新しい CHECK も満たす。新しい行は足した時から確かめる。
--    後の手順 (本番に 0052 を流した後・別の取引で・書き込みを止めない SHARE UPDATE EXCLUSIVE の鍵):
--      alter table events.master_change_events validate constraint master_change_events_entity_type_check;
alter table events.master_change_events drop constraint master_change_events_entity_type_check;
alter table events.master_change_events add constraint master_change_events_entity_type_check
  check (entity_type in ('product', 'sku', 'supplier', 'supplier_sku', 'sku_component', 'sku_cost', 'listing', 'listing_component', 'external_id')) not valid;

-- JAN の行 (system = 'jan'): 変えてよいのは有効期間の終わり (valid_to) を null から時刻にする (外す) ことだけ。消さない
create function core.guard_jan_external_ids() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'JAN の行は消さない (外すときは valid_to)' using errcode = 'P0001'; end if;
  -- 生成列 (external_norm) は BEFORE の trigger ではまだ計算されていない = 比べない (external_value が同じなら同じ)
  if (to_jsonb(new) - 'valid_to' - 'external_norm') is distinct from (to_jsonb(old) - 'valid_to' - 'external_norm') then
    raise exception 'JAN の行は書き換えない (外して新しい行を足す)' using errcode = 'P0001';
  end if;
  if old.valid_to is not null and new.valid_to is distinct from old.valid_to then raise exception '外した JAN の行は戻さない・動かさない' using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_external_ids_jan_guard before update on core.external_ids for each row when (old.system = 'jan') execute function core.guard_jan_external_ids();
create trigger trg_external_ids_jan_no_delete before delete on core.external_ids for each row when (old.system = 'jan') execute function core.guard_jan_external_ids();

-- 表の持ち主でないロール (画面 = master_edit) が書けるのは商品の JAN の行だけ (列の権限は create-master-edit-roles.mjs が絞る。行はここで絞る)
--   表の持ち主 (夜間ロード・migration) とそのメンバーは今までどおり (0050 の core.guard_sku_cost_overlap と同じ見分け方)。UPDATE は前の行も JAN であること
create function core.guard_external_ids_writer() returns trigger language plpgsql as $$
declare
  v_jan_new boolean;
  v_jan_old boolean;
begin
  if pg_catalog.pg_has_role(current_user, (select c.relowner from pg_catalog.pg_class c where c.oid = tg_relid), 'USAGE') then
    return case when tg_op = 'DELETE' then old else new end;
  end if;
  if tg_op <> 'DELETE' then v_jan_new := new.system = 'jan' and new.id_kind = 'jan' and new.entity_type = 'product'; end if;
  if tg_op <> 'INSERT' then v_jan_old := old.system = 'jan' and old.id_kind = 'jan' and old.entity_type = 'product'; end if;
  if tg_op = 'DELETE' or v_jan_new is not true or (tg_op = 'UPDATE' and v_jan_old is not true) then
    raise exception 'external_id_writer: ロール % が書けるのは商品の JAN の行 (足す・外す) だけ', current_user using errcode = '42501';
  end if;
  return new;
end $$;
create trigger trg_external_ids_writer before insert or update or delete on core.external_ids for each row execute function core.guard_external_ids_writer();

-- 画面のロール (master_edit) の JAN の書き込み (D・0050 の約束 #1563 R3 M2): 0050 の守り (同じ取引で ops.begin_master_write の後・段階 new_open) +
-- JAN の持ち主 (始めたときの持ち主表の external_ids.jan) が company。0050 の ops.master_edit_owner_keys は core.external_ids を知らない = 持ち主はここで見る
create trigger trg_master_edit_guard before insert or update or delete on core.external_ids for each row execute function ops.guard_master_edit_write();
-- 🚨 security definer = 画面のロールに ops.master_write_sessions を読ませない。名前の順 (trg_master_edit_guard の後) に動く
create function core.guard_master_edit_jan_owner() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_own     jsonb;
begin
  if v_db_user is distinct from 'master_edit' then return case when tg_op = 'DELETE' then old else new end; end if;
  select s.ownership into v_own from ops.master_write_sessions s where s.txid = pg_catalog.txid_current();
  if not found then
    raise exception 'master_write_session_required: 画面のロールの JAN の書き込みは、同じ取引で ops.begin_master_write を呼んだ後だけ' using errcode = '42501';
  end if;
  if (v_own ->> 'external_ids.jan') is distinct from 'company' then
    raise exception 'owner_not_company: external_ids.jan の持ち主が company でない (core.external_ids)' using errcode = '42501';
  end if;
  return case when tg_op = 'DELETE' then old else new end;
end $$;
revoke all on function core.guard_master_edit_jan_owner() from public;
create trigger trg_master_edit_jan_owner before insert or update or delete on core.external_ids for each row execute function core.guard_master_edit_jan_owner();

-- 記録 (0026 と同じ関数 = 0050 から security definer): 足す = INSERT の 1 行 / 外す = valid_to の UPDATE。誰が・request_id・理由は取引の set_config
-- (画面のロール master_edit は 0050 の関数が ops.begin_master_write の行から取る = 設定では偽れない)
create trigger trg_external_ids_jan_audit after insert or update on core.external_ids for each row when (new.system = 'jan')
  execute function core.audit_master_change('external_id', 'external_id_row');

-- JAN が変わったら、その商品の SKU (と商品) の version も変える = 編集の印・CSV の予約の版が古くなる。
-- security definer = 画面のロールに skus / products の version の update を渡さない (0050 の core.bump_parent_version と同じ)
create function core.bump_jan_owner_version() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if tg_op = 'UPDATE' and new.valid_to is not distinct from old.valid_to then return null; end if;
  perform pg_catalog.set_config('core.version_bump', 'on', true);
  if new.entity_type = 'product' then
    update core.products set version = version where product_id = new.entity_id;
    update core.skus set version = version where product_id = new.entity_id;
  elsif new.entity_type = 'sku' then
    update core.skus set version = version where sku_id = new.entity_id;
  end if;
  perform pg_catalog.set_config('core.version_bump', '', true);
  return null;
end $$;
revoke all on function core.bump_jan_owner_version() from public;
create trigger trg_external_ids_jan_bump after insert or update on core.external_ids for each row when (new.system = 'jan')
  execute function core.bump_jan_owner_version();

-- ═══════════ C. 仕入先の登録の状態 (M1・M5・契約 v3 Medium 3) ═══════════
-- 新しい仕入先 (この道で作った) だけに行を作る。行が無い = 前からある仕入先 (NE・発注アプリから来た = 今までどおり使える)
create table ops.supplier_registrations (
  supplier_id      bigint primary key references core.suppliers (supplier_id),
  company_id       smallint not null default 1 references core.companies,
  state            text not null check (state in ('ne_pending', 'ne_confirmed')),
  created_by       text not null check (length(created_by) > 0),
  created_at       timestamptz not null default now(),
  declared_by      text,
  declared_at      timestamptz,   -- 「NE に登録した」と申告した時刻
  evidence         jsonb check (evidence is null or jsonb_typeof(evidence) = 'object'),   -- { ne_screen: 'NE の仕入先の画面', ne_code, note }
  constraint ck_sr_declared check ((state = 'ne_confirmed') = (declared_by is not null and declared_at is not null and evidence is not null))
);
comment on table ops.supplier_registrations is '新しい仕入先の「NE に登録した」の状態 (0052)。行が無い = 前からある仕入先。ne_pending の仕入先は代表の仕入先に選べない。書くのは関数だけ';

create table ops.supplier_registration_events (
  event_id    bigint generated always as identity primary key,
  supplier_id bigint not null references core.suppliers (supplier_id),
  from_state  text,
  to_state    text not null,
  actor       text not null check (length(actor) > 0),
  evidence    jsonb,
  recorded_at timestamptz not null default now()
);
select core.make_append_only('ops', 'supplier_registration_events');

-- 書くのは下の関数だけ (関数が取引の中だけ印を立てる)。DELETE はいつでも拒む
create function ops.guard_supplier_registrations() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception '仕入先の登録の状態は消さない' using errcode = 'P0001'; end if;
  if coalesce(pg_catalog.current_setting('ops.supplier_registration_protocol', true), '') is distinct from '1' then
    raise exception '仕入先の登録の状態は ops の関数 (create_supplier_registration / declare_supplier_in_ne) でだけ書く' using errcode = 'P0001';
  end if;
  if tg_op = 'INSERT' then
    if new.state <> 'ne_pending' then raise exception '新しい仕入先の状態は ne_pending で作る' using errcode = 'P0001'; end if;
    return new;
  end if;
  if (new.supplier_id, new.company_id, new.created_by, new.created_at) is distinct from (old.supplier_id, old.company_id, old.created_by, old.created_at) then
    raise exception '仕入先の登録の行の仕入先・作った記録は変えない' using errcode = 'P0001';
  end if;
  if old.state = 'ne_confirmed' and new is distinct from old then raise exception '確かめた仕入先の状態は戻さない' using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_supplier_registrations_guard before insert or update or delete on ops.supplier_registrations for each row execute function ops.guard_supplier_registrations();
create trigger trg_supplier_registrations_no_truncate before truncate on ops.supplier_registrations for each statement execute function core.reject_mutation();

/** 新しい仕入先の状態の行を作る (ne_pending)。仕入先を作ったのと同じ取引の中でだけ (前からある仕入先を「新しい」にしない) */
create function ops.create_supplier_registration(p_supplier_id bigint, p_actor text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_company smallint;
begin
  if coalesce(pg_catalog.btrim(p_actor), '') = '' then raise exception 'invalid_input: 誰が (actor) が要る' using errcode = '22023'; end if;
  p_actor := ops.reg_write_session(p_actor) ->> 'actor';   -- 画面のロールに渡すとき (⑥) も ops.begin_master_write の行から (D)
  select s.company_id into v_company from core.suppliers s
   where s.supplier_id = p_supplier_id and s.xmin = pg_catalog.pg_current_xact_id()::xid and s.created_at = pg_catalog.now();
  if not found then raise exception 'not_new_supplier: 状態の行は仕入先を作った取引の中でだけ作る (仕入先 %)', p_supplier_id using errcode = 'P0001'; end if;
  perform pg_catalog.set_config('ops.supplier_registration_protocol', '1', true);
  insert into ops.supplier_registrations (supplier_id, company_id, state, created_by) values (p_supplier_id, v_company, 'ne_pending', p_actor);
  perform pg_catalog.set_config('ops.supplier_registration_protocol', '', true);
  insert into ops.supplier_registration_events (supplier_id, from_state, to_state, actor) values (p_supplier_id, null, 'ne_pending', p_actor);
  return pg_catalog.jsonb_build_object('supplier_id', p_supplier_id, 'state', 'ne_pending');
end $$;
revoke all on function ops.create_supplier_registration(bigint, text) from public;

/** 「NE に登録した」と申告する (ne_pending → ne_confirmed)。NE の仕入先の画面で見たコードが Company DB のコードと同じ (4 桁に揃えて) であること */
create function ops.declare_supplier_in_ne(p_supplier_id bigint, p_actor text, p_ne_code text, p_note text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_code  text;
  v_state text;
  v_ev    jsonb;
begin
  if coalesce(pg_catalog.btrim(p_actor), '') = '' then raise exception 'invalid_input: 誰が (actor) が要る' using errcode = '22023'; end if;
  p_actor := ops.reg_write_session(p_actor) ->> 'actor';   -- 画面のロールに渡すとき (⑥) も ops.begin_master_write の行から (D)
  select s.code into v_code from core.suppliers s where s.supplier_id = p_supplier_id for share;
  if not found then raise exception 'not_found: 仕入先 % が無い', p_supplier_id using errcode = 'P0002'; end if;
  select r.state into v_state from ops.supplier_registrations r where r.supplier_id = p_supplier_id for update;
  if v_state is null then raise exception 'not_new_supplier: 仕入先 % は前からある仕入先 (申告は要らない)', v_code using errcode = 'P0001'; end if;
  if v_state = 'ne_confirmed' then return pg_catalog.jsonb_build_object('supplier_id', p_supplier_id, 'state', 'ne_confirmed', 'already', true); end if;
  if core.canonical_supplier_code(pg_catalog.btrim(coalesce(p_ne_code, ''))) is distinct from core.canonical_supplier_code(v_code) then
    raise exception 'ne_code_mismatch: NE で見たコード % が Company DB のコード % と違う', p_ne_code, v_code using errcode = '22023';
  end if;
  v_ev := pg_catalog.jsonb_build_object('ne_screen', 'NE の仕入先の画面', 'ne_code', pg_catalog.btrim(p_ne_code), 'note', p_note);
  perform pg_catalog.set_config('ops.supplier_registration_protocol', '1', true);
  update ops.supplier_registrations set state = 'ne_confirmed', declared_by = p_actor, declared_at = pg_catalog.now(), evidence = v_ev where supplier_id = p_supplier_id;
  perform pg_catalog.set_config('ops.supplier_registration_protocol', '', true);
  insert into ops.supplier_registration_events (supplier_id, from_state, to_state, actor, evidence) values (p_supplier_id, 'ne_pending', 'ne_confirmed', p_actor, v_ev);
  return pg_catalog.jsonb_build_object('supplier_id', p_supplier_id, 'state', 'ne_confirmed');
end $$;
revoke all on function ops.declare_supplier_in_ne(bigint, text, text, text) from public;

-- 仕入先: 物理の削除は「同じコード (正規化の後) の行がほかにある = 二重を寄せる (core.merge_duplicate_suppliers)」ときだけ。止めるのは active = false
-- 取引停止 (active → false) は、代表の仕入先に使っている間は拒む (同じ取引で付け替えた後なら通る)
create function core.guard_suppliers_lifecycle() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then
    if not exists (select 1 from core.suppliers o where o.company_id = old.company_id and o.supplier_id <> old.supplier_id
                    and core.canonical_supplier_code(o.code) = core.canonical_supplier_code(old.code)) then
      raise exception 'supplier_delete: 仕入先 % は消さない (取引停止 = active を false に)', old.code using errcode = 'P0001';
    end if;
    if exists (select 1 from ops.supplier_registrations r where r.supplier_id = old.supplier_id) then
      raise exception 'supplier_delete: 新しく作った仕入先 % は消さない', old.code using errcode = 'P0001';
    end if;
    return old;
  end if;
  if old.active and not new.active and exists (select 1 from core.supplier_skus x where x.supplier_id = new.supplier_id and x.is_primary) then
    raise exception 'supplier_in_use: 仕入先 % は代表の仕入先に使っている商品がある (先に付け替える)', new.code using errcode = 'P0001';
  end if;
  return new;
end $$;
create trigger trg_suppliers_lifecycle before update of active or delete on core.suppliers for each row execute function core.guard_suppliers_lifecycle();

-- 確かめる前 (ne_pending) の仕入先は、この画面の書き込み (呼び手が画面のロール master_edit・または source_system = portal_master_edit) で代表の仕入先にできない。
-- 夜間ロード (NE の商品の仕入先コード) は止めない (NE に登録されている = NE の値)。画面のロールは設定 (core.source_system) を付けなくても見る (D)
create function core.guard_primary_supplier_registered() returns trigger language plpgsql as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
begin
  if not new.is_primary then return new; end if;
  if tg_op = 'UPDATE' and old.is_primary then return new; end if;
  if v_db_user is distinct from 'master_edit' and coalesce(pg_catalog.current_setting('core.source_system', true), '') <> 'portal_master_edit' then return new; end if;
  if exists (select 1 from ops.supplier_registrations r where r.supplier_id = new.supplier_id and r.state <> 'ne_confirmed') then
    raise exception 'supplier_not_confirmed: 仕入先 % は「NE に登録した」の申告がまだなので代表にできない', new.supplier_id using errcode = 'P0001';
  end if;
  return new;
end $$;
create trigger trg_supplier_skus_primary_registered before insert or update of is_primary on core.supplier_skus for each row execute function core.guard_primary_supplier_registered();

-- 見張りは読むだけ・照合は確かめの関数だけ。画面・運用のロールの権限は scripts/company-db/create-master-edit-roles.mjs (migration の後に流し直す)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.ne_reg_exports, ops.ne_reg_attempts, ops.ne_reg_export_items, ops.ne_reg_export_rows, ops.ne_reg_checks, ops.v_ne_reg_targets, ops.supplier_registrations, ops.supplier_registration_events,
      ops.ne_reg_compare_runs, ops.ne_reg_compare_observations, ops.ne_reg_compare_receipts to watcher';
  end if;
  if exists (select 1 from pg_roles where rolname = 'watch_writer') then
    execute 'grant usage on schema ops to watch_writer';
    execute 'grant execute on function ops.record_ne_registration_observations(jsonb), ops.seal_ne_registration_run(text, text, text), ops.record_ne_registration_check(text) to watch_writer';
  end if;
end $$;
