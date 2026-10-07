-- 0040: NE に取り込む CSV の記録 (2026-09-27。Company DB構想 10 §6.1.1「③b NE に取り込む CSV の契約 v3」。Codex ③b-R0・R1)
--
-- なぜ: 判断の画面 (D2') で「NE を直す」(fix_ne) と承認した差を、NE の一括登録で取り込む CSV にする。
--   どのファイルに・どの承認を・どの値で入れたか、取り込んだと誰がいつ申告したか、を残す = 翌朝の照合で行ごとに「届いたか」を確かめる。
-- なにを:
--   ops.ne_csv_exports     = 作ったファイル 1 つ = 1 行 (種類 products / sets・照合の列・CSV の見出し・変換の版・文字コード・配る byte 列と sha256・
--                            作った時の今日の照合の回・状態 made / checked / declared / void)
--   ops.ne_csv_export_rows = ファイルの行 (出どころ fix_ne / to_ne・承認の出来事・指紋・SKU・列・子・CSV に書いたコード・固定した目標・CSV に書いた文字・有効な予約か)
--                            🚨 有効な予約 (reserved) は (SKU・列・子) ごとに 1 つ (部分の一意の索引) = 同じ差を 2 つのファイルに入れない
--   ops.ne_csv_attempts    = 取込の試み (申告した人・時刻・結果 ok / rejected_all / partial・メモ)
--   ops.ne_csv_verified    = 実機で確かめた (種類・列・文字コード・見出し・変換の版) の組。確かめていない組は「試し用」(5 行まで) だけ作れる
-- 書くのはポータル (Render・持ち主の役) の API だけ。watcher は読むだけ。
-- 行・ファイルの中身 (目標・CSV の文字・byte 列など) は書き換えない・消さない (trigger)。変えてよいのは状態の列だけ

create table ops.ne_csv_exports (
  export_id         bigint generated always as identity primary key,
  kind              text not null check (kind in ('products', 'sets')),
  col               text not null check (col in ('name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier', 'parent')),
  ne_column         text not null check (ne_column ~ '^[a-z_]{1,40}$'),
  converter_version text not null,
  encoding          text not null check (encoding in ('utf8', 'sjis')),
  trial             boolean not null,
  row_count         integer not null check (row_count between 1 and 1000),
  sha256            text not null check (sha256 ~ '^[0-9a-f]{64}$'),
  file_bytes        bytea not null,
  compare_run_id    text not null check (compare_run_id ~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$'),
  created_by        text not null check (length(created_by) > 0),
  created_at        timestamptz not null default now(),
  state             text not null default 'made' check (state in ('made', 'checked', 'declared', 'void')),
  checked_at        timestamptz,
  checked_run       text,
  checked_by        text,
  declared_at       timestamptz,   -- 最初の申告 (申告のし直しでは動かさない)
  declared_by       text,
  void_at           timestamptz,
  void_reason       text,
  void_by           text,
  constraint ck_ne_csv_checked check ((state in ('checked', 'declared')) <= (checked_at is not null and checked_run is not null)),
  constraint ck_ne_csv_declared check ((state = 'declared') = (declared_at is not null)),   -- 申告したファイルは void にしない
  constraint ck_ne_csv_void check ((state = 'void') = (void_at is not null))
);
create index ix_ne_csv_exports_created on ops.ne_csv_exports (created_at desc);

create table ops.ne_csv_export_rows (
  row_id            bigint generated always as identity primary key,
  export_id         bigint not null references ops.ne_csv_exports (export_id),
  source            text not null check (source in ('fix_ne', 'to_ne')),
  approved_event_id bigint references ops.master_decision_events (event_id),
  fingerprint       text check (fingerprint ~ '^[0-9a-f]{64}$'),
  code_norm         text not null check (length(code_norm) > 0),
  col               text not null,
  child             text,
  ne_code           text not null check (ne_code ~ '^[a-z0-9_-]{1,30}$'),
  target            jsonb not null,
  cell              text not null,
  cdb_version       bigint,
  evidence          jsonb,
  prev_row_id       bigint references ops.ne_csv_export_rows (row_id),   -- 同じ承認を前に入れたファイルの行 (作り直し)
  reserved          boolean not null default true,
  released_at       timestamptz,
  release_reason    text check (release_reason in ('confirmed', 'not_reflected', 'needs_look', 'superseded', 'void')),
  constraint ck_ne_csv_row_fix_ne check (source <> 'fix_ne' or (approved_event_id is not null and fingerprint is not null)),
  constraint ck_ne_csv_row_to_ne check (source <> 'to_ne' or (cdb_version is not null and evidence is not null)),
  constraint ck_ne_csv_row_release check (reserved = (released_at is null) and reserved = (release_reason is null))
);
-- 有効な予約は (SKU・列・子) ごとに 1 つ (承認の出来事が変わっても二重に予約しない。Codex ③b-R1 H3)
create unique index ux_ne_csv_rows_reserved on ops.ne_csv_export_rows (code_norm, col, coalesce(child, '')) where reserved;
create index ix_ne_csv_rows_export on ops.ne_csv_export_rows (export_id);
create index ix_ne_csv_rows_event on ops.ne_csv_export_rows (approved_event_id);

create table ops.ne_csv_attempts (
  attempt_id   bigint generated always as identity primary key,
  export_id    bigint not null references ops.ne_csv_exports (export_id),
  declared_by  text not null check (length(declared_by) > 0),
  declared_at  timestamptz not null,
  result       text not null check (result in ('ok', 'rejected_all', 'partial')),
  note         text check (note is null or length(note) <= 500)
);
select core.make_append_only('ops', 'ne_csv_attempts');

create table ops.ne_csv_verified (
  verified_id       bigint generated always as identity primary key,
  export_id         bigint references ops.ne_csv_exports (export_id),   -- 確かめに使ったファイル (任意)
  kind              text not null check (kind in ('products', 'sets')),
  col               text not null,
  encoding          text not null check (encoding in ('utf8', 'sjis')),
  header            text not null,
  converter_version text not null,
  result            text not null check (result in ('ok', 'ng')),
  note              text check (note is null or length(note) <= 500),
  verified_by       text not null check (length(verified_by) > 0),
  verified_at       timestamptz not null default now()
);
select core.make_append_only('ops', 'ne_csv_verified');

-- 中身は書き換えない・消さない。変えてよいのは状態の列だけ
create function ops.guard_ne_csv_exports() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'ne_csv_exports は消さない' using errcode = 'P0001'; end if;
  if (new.export_id, new.kind, new.col, new.ne_column, new.converter_version, new.encoding, new.trial, new.row_count, new.sha256, new.file_bytes,
      new.compare_run_id, new.created_by, new.created_at)
     is distinct from
     (old.export_id, old.kind, old.col, old.ne_column, old.converter_version, old.encoding, old.trial, old.row_count, old.sha256, old.file_bytes,
      old.compare_run_id, old.created_by, old.created_at) then
    raise exception 'ne_csv_exports の中身は書き換えない (状態の列だけ)' using errcode = 'P0001';
  end if;
  if old.state = 'void' and new is distinct from old then raise exception 'void のファイルは変えない' using errcode = 'P0001'; end if;
  if old.state = 'declared' and new.state <> 'declared' then raise exception '申告したファイルの状態は戻さない' using errcode = 'P0001'; end if;
  if old.declared_at is not null and new.declared_at is distinct from old.declared_at then raise exception '最初の申告の時刻は動かさない' using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_ne_csv_exports_guard before update or delete on ops.ne_csv_exports for each row execute function ops.guard_ne_csv_exports();

create function ops.guard_ne_csv_rows() returns trigger language plpgsql as $$
begin
  if tg_op = 'DELETE' then raise exception 'ne_csv_export_rows は消さない' using errcode = 'P0001'; end if;
  if (new.row_id, new.export_id, new.source, new.approved_event_id, new.fingerprint, new.code_norm, new.col, new.child, new.ne_code, new.target, new.cell, new.cdb_version, new.evidence, new.prev_row_id)
     is distinct from
     (old.row_id, old.export_id, old.source, old.approved_event_id, old.fingerprint, old.code_norm, old.col, old.child, old.ne_code, old.target, old.cell, old.cdb_version, old.evidence, old.prev_row_id) then
    raise exception 'ne_csv_export_rows の中身は書き換えない (予約の列だけ)' using errcode = 'P0001';
  end if;
  if not old.reserved and new is distinct from old then raise exception '外した予約は変えない・戻さない (作り直す)' using errcode = 'P0001'; end if;
  return new;
end $$;
create trigger trg_ne_csv_rows_guard before update or delete on ops.ne_csv_export_rows for each row execute function ops.guard_ne_csv_rows();

do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.ne_csv_exports, ops.ne_csv_export_rows, ops.ne_csv_attempts, ops.ne_csv_verified to watcher';
  end if;
end $$;
