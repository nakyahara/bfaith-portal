-- 0081 (仮の番号・マージの時に付け替える): ロジザード登録の CSV (logizard_bc_upload.csv) を出す帳面 (AI_reference CompanyDB構想/23 v4 §5'・2 本目)
--   今の GAS の「新規商品マスタを CSV 出力」(人が H 列 = バーコードなどを手で入れる) をポータルの画面に置き換える。GAS は CSV が使えないときの予備として残す
--
-- なにを:
--   1. ops.lz_new_exports / ops.lz_new_export_items = 出した CSV の帳面 (ファイルの byte 列・ハッシュ・行 = SKU・値・有効期限区分・入荷日管理フラグ・照らした写しの時刻)。
--      出した (issued) 品目は SKU ごとに 1 つ・同じ値も 1 つ。書くのは下の関数だけ (印 ops.lz_new_export_protocol)・取り消す以外は変えない・消さない
--   2. ops.set_sku_logizard_barcode を作り直す = CSV に出した商品のバーコードは直さない (lz_issued) を SKU の鍵の下で確かめる (#1719 Codex R1 Medium 1)
--   3. ops.lz_new_export_issue (CSV を出す = 帳面に書く・品目ごとに SKU の鍵 → 値の鍵の下で今の値・有効期限・入荷日・重なり・照らした写しをもう一度確かめ、
--      CSV の中身を DB が品目から作る = #1721 R1 High 1) /
--      ops.lz_new_export_void (取り消す・理由が要る)
--   E 列 (有効期限区分) = 有効期限の管理 あり 02 / なし 01・F 列 (入荷日管理フラグ) = 入荷日の管理 あり 1 / なし・入力なし 0 (中原さんの決定 Q1 a)
--   🚨 値のある品目は、保存のときの結果にかかわらず、出すときに新しい写し (36 時間の中) で照らせたときだけ出す (DB は写しの時刻を確かめる・#1721 R1 High 2)。
--      ops.lzbc_value_clash で重なりのある値 (ほかの商品の JAN・Amazon の FNSKU・ロジザードのバーコード) は出さない
-- 🚨 既にある行は変えない (表と関数を足す・書く関数を作り直す)。security definer = 一時の表を使わない・search_path = pg_catalog, pg_temp・public の実行権を外す

-- ═══════════ 1. CSV の帳面 ═══════════
create table ops.lz_new_exports (
  export_id      bigint generated always as identity primary key,
  company_id     smallint not null default 1 references core.companies,
  request_id     uuid not null unique,
  actor_id       text not null check (pg_catalog.length(actor_id) between 1 and 320),
  db_user        text not null,
  created_at     timestamptz not null default pg_catalog.clock_timestamp(),
  schema_version text not null check (schema_version = 'lz-bc-v1'),
  file_name      text not null check (file_name = 'logizard_bc_upload.csv'),
  file_text      text not null,             -- 🆕 #1721 R1 High 1: CSV の中身 = DB が確かめ直した品目から作った文字 (見出し・CRLF・最後に改行なし)。
                                            --   Shift_JIS にするのはダウンロードのとき (アプリの lz-csv.mjs の encodeText = GAS と同じ変換・PGlite には文字の変換が無い)
  text_sha256    text not null check (text_sha256 ~ '^[0-9a-f]{64}$'),   -- file_text (UTF-8) のハッシュ
  request_hash   text not null check (request_hash ~ '^[0-9a-f]{64}$'),  -- 🆕 #1721 R1 Medium: 要求の品目 (sku_id:rule_row を sku_id の順) のハッシュ = 押し直しの比べ
  row_count      integer not null check (row_count between 1 and 2000),
  lz_list_as_of  timestamptz,               -- ロジザードの商品の一覧の写しの時刻 (まだ無いの判定)
  lz_bc_as_of    timestamptz,               -- ロジザードのバーコードマスタの写しの時刻 (値の照らし直し)
  state          text not null default 'issued' check (state in ('issued', 'voided')),
  voided_at      timestamptz,
  voided_by      text,
  void_reason    text check (void_reason is null or pg_catalog.length(void_reason) between 1 and 200),
  constraint ck_lz_new_exports_void check ((state = 'voided') = (voided_at is not null and voided_by is not null and void_reason is not null))
);
comment on table ops.lz_new_exports is '🆕 0081: ロジザード登録の CSV (logizard_bc_upload.csv) を出した記録 (CompanyDB構想 23 v4)。byte 列とハッシュは変えない。書くのは ops.lz_new_export_issue / _void だけ';

create table ops.lz_new_export_items (
  export_id    bigint not null references ops.lz_new_exports,
  company_id   smallint not null default 1,
  sku_id       bigint not null,
  code         text not null,
  rule_row     bigint not null references ops.sku_logizard_barcode_rules,
  kind         text not null check (kind in ('fnsku', 'jan', 'lz_assign')),
  value        text,
  expiry_code  text not null check (expiry_code in ('01', '02')),
  inbound_code text not null check (inbound_code in ('0', '1')),
  rechecked    boolean not null default false,   -- 保存のときは写しと照らせていなかった値を、出すときに照らし直した
  state        text not null default 'issued' check (state in ('issued', 'voided')),
  primary key (export_id, sku_id),
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  constraint ck_lz_new_items_value check ((kind = 'lz_assign') = (value is null))
);
create unique index ux_lz_new_items_issued_sku on ops.lz_new_export_items (sku_id) where state = 'issued';
create unique index ux_lz_new_items_issued_value on ops.lz_new_export_items (value) where state = 'issued' and value is not null;
comment on table ops.lz_new_export_items is '🆕 0081: ロジザード登録の CSV の行。出した (issued) 品目は SKU ごとに 1 つ・同じ値も 1 つ (同じ商品・同じバーコードを 2 回出さない)。取り消すと voided';

create function ops.guard_lz_new_exports() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if coalesce(pg_catalog.current_setting('ops.lz_new_export_protocol', true), '') is distinct from '1' then
    raise exception 'lz_new_export: ロジザード登録の CSV の帳面は ops.lz_new_export_issue / _void の中でだけ書く' using errcode = '42501';
  end if;
  if tg_op = 'DELETE' then raise exception 'lz_new_export: 帳面の行は消さない' using errcode = '42501'; end if;
  if tg_op = 'UPDATE' then
    -- 変えてよいのは「出した → 取り消した」だけ (中身・byte 列・ハッシュは変えない)
    if old.state <> 'issued' or new.state <> 'voided' then raise exception 'lz_new_export: 変えてよいのは出した → 取り消した だけ' using errcode = '42501'; end if;
    if tg_table_name = 'lz_new_exports' then
      if (new.export_id, new.request_id, new.actor_id, new.created_at, new.file_text, new.text_sha256, new.request_hash, new.row_count, new.lz_list_as_of, new.lz_bc_as_of)
         is distinct from (old.export_id, old.request_id, old.actor_id, old.created_at, old.file_text, old.text_sha256, old.request_hash, old.row_count, old.lz_list_as_of, old.lz_bc_as_of) then
        raise exception 'lz_new_export: ファイルの中身は変えない' using errcode = '42501';
      end if;
    else
      if (new.export_id, new.sku_id, new.code, new.rule_row, new.kind, new.value, new.expiry_code, new.inbound_code, new.rechecked)
         is distinct from (old.export_id, old.sku_id, old.code, old.rule_row, old.kind, old.value, old.expiry_code, old.inbound_code, old.rechecked) then
        raise exception 'lz_new_export: 行の中身は変えない' using errcode = '42501';
      end if;
    end if;
  end if;
  return new;
end $$;
revoke all on function ops.guard_lz_new_exports() from public;
create trigger trg_lz_new_exports_guard before insert or update or delete on ops.lz_new_exports for each row execute function ops.guard_lz_new_exports();
create trigger trg_lz_new_export_items_guard before insert or update or delete on ops.lz_new_export_items for each row execute function ops.guard_lz_new_exports();
create trigger trg_lz_new_exports_no_truncate before truncate on ops.lz_new_exports for each statement execute function core.reject_mutation();
create trigger trg_lz_new_export_items_no_truncate before truncate on ops.lz_new_export_items for each statement execute function core.reject_mutation();

-- ═══════════ 2. 値を書く関数を作り直す (CSV に出した後は直さない) ═══════════
create or replace function ops.set_sku_logizard_barcode(p_request_id uuid, p_actor_id text, p_sku_id bigint, p_kind text, p_value text, p_via text, p_seen_row bigint,
                                               p_lz_state text, p_lz_as_of timestamptz) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  s         record;
  v_prev    ops.sku_logizard_barcode_rules;
  v_cur     ops.sku_logizard_barcode_rules;
  v_holder  text;
  v_row     bigint;
  v_req     ops.sku_logizard_barcode_requests;
  v_res     jsonb;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, null) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール) が要る' using errcode = '22023';
  end if;
  if p_via is null or p_via not in ('register', 'sku_page') then raise exception 'invalid_input: どこから は register / sku_page' using errcode = '22023'; end if;
  if p_kind = 'lz_assign' then
    if p_value is not null then raise exception 'invalid_input: 空欄 (ロジザードが番号を付ける) に値は付けない' using errcode = '22023'; end if;
  elsif p_kind = 'fnsku' then
    if p_value is null or p_value !~ '^X00[0-9A-Z]{7}$' then raise exception 'invalid_input: FNSKU は X00 + 英数字 7 字 (大文字・10 文字)' using errcode = '22023'; end if;
  elsif p_kind = 'jan' then
    if not ops.lzbc_jan_ok(p_value) then raise exception 'invalid_input: JAN は 8 か 13 桁の数字で、チェック数字が合うもの' using errcode = '22023'; end if;
  else
    raise exception 'invalid_input: 種類は fnsku / jan / lz_assign' using errcode = '22023';
  end if;
  if (p_value is null) <> (p_lz_state is null) or (p_lz_state is not null and p_lz_state not in ('checked', 'unavailable', 'empty', 'stale'))
     or (p_lz_state is null and p_lz_as_of is not null) then
    raise exception 'invalid_input: 値があるときはロジザードの写しと照らした結果 (checked / unavailable / empty / stale) が要る・空欄には要らない' using errcode = '22023';
  end if;
  if p_lz_state in ('unavailable', 'empty') and p_lz_as_of is not null then
    raise exception 'invalid_input: 写しを読めない・空 (%) に写しの時刻は付けない', p_lz_state using errcode = '22023';
  end if;
  if p_lz_state = 'checked' and (p_lz_as_of is null or p_lz_as_of <= pg_catalog.clock_timestamp() - interval '36 hours 5 minutes'
                                 or p_lz_as_of > pg_catalog.clock_timestamp() + interval '5 minutes') then
    raise exception 'invalid_input: 照らした (checked) は 36 時間より新しい写しの時刻が要る (時刻 %)', coalesce(p_lz_as_of::text, 'なし') using errcode = '22023';
  end if;
  if p_lz_state = 'stale' and (p_lz_as_of is null or p_lz_as_of > pg_catalog.clock_timestamp() - interval '35 hours 55 minutes') then
    raise exception 'invalid_input: 古い (stale) は 36 時間より前の写しの時刻が要る (時刻 %)', coalesce(p_lz_as_of::text, 'なし') using errcode = '22023';
  end if;
  -- SKU の鍵 → 値の鍵 (同じ値を 2 つの商品に同時に付けない)。この後の文は鍵の後の姿を見る (read committed)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  if p_value is not null then perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.lzbc:' || p_value, 0)); end if;
  -- 押し直し (同じ request_id・同じ SKU) = 前の答え。🆕 #1719 R5: 答えの控え (書かなかった回も) を先に見る
  select * into v_req from ops.sku_logizard_barcode_requests x where x.request_id = p_request_id and x.sku_id = p_sku_id;
  if v_req.request_id is not null then
    if v_req.actor_id is distinct from p_actor_id or v_req.rule is distinct from p_kind or v_req.value is distinct from p_value or v_req.via is distinct from p_via
       or v_req.seen_row is distinct from p_seen_row then
      raise exception 'request_id_reused: request_id % はほかの人・中身で使われている', p_request_id using errcode = '23505';
    end if;
    return v_req.result || pg_catalog.jsonb_build_object('replayed', true);
  end if;
  -- 🆕 #1719 Codex R7 Low: 控えの無い行 (0078 の決め方の行など) と同じ request_id = 同じ要求か確かめられない = 再送として扱わず断る
  select * into v_prev from ops.sku_logizard_barcode_rules r where r.request_id = p_request_id and r.sku_id = p_sku_id;
  if v_prev.rule_row is not null then
    raise exception 'request_id_reused: request_id % は前の行 (答えの控えが無い) で使われている', p_request_id using errcode = '23505';
  end if;
  select k.sku_id, k.company_id, k.sku_kind, r.state as reg into s from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if s.sku_id is null or s.company_id <> 1 then raise exception 'invalid_input: SKU % が Company DB に無い', p_sku_id using errcode = '22023'; end if;
  if s.sku_kind is distinct from 'single' then raise exception 'not_single: ロジザードのバーコードは単品だけ (SKU %)', p_sku_id using errcode = '22023'; end if;
  if s.reg = 'cancelled' then raise exception 'cancelled: 登録をやめた商品 (SKU %) には書かない', p_sku_id using errcode = '22023'; end if;
  -- 🆕 0081 (#1719 Codex R1 Medium 1): ロジザード登録の CSV に出した商品は直さない = SKU の鍵の下で確かめる (CSV を出す関数も同じ SKU の鍵を取る = 同時でも片方が待つ)。
  --   取り消す (ops.lz_new_export_void) と直せる
  if exists (select 1 from ops.lz_new_export_items i where i.sku_id = p_sku_id and i.state = 'issued') then
    raise exception 'lz_issued: ロジザード登録の CSV (ファイル #%) に出したので直せない (ロジザードの画面で直す・CSV を取り消せば直せる)',
      (select i.export_id from ops.lz_new_export_items i where i.sku_id = p_sku_id and i.state = 'issued' limit 1) using errcode = '23505';
  end if;
  select * into v_cur from ops.sku_logizard_barcode_rules r where r.sku_id = p_sku_id order by r.rule_row desc limit 1;
  if v_cur.rule_row is distinct from p_seen_row then
    raise exception 'stale: 画面を開いた後にほかの人がバーコードを直した (今の行 %・開いたときの行 %)', coalesce(v_cur.rule_row::text, 'なし'), coalesce(p_seen_row::text, 'なし') using errcode = '40001';
  end if;
  if p_value is not null then
    v_holder := ops.lzbc_value_holder(p_sku_id, p_value);
    if v_holder is not null then raise exception 'lzbc_taken: バーコード % はほかの商品 (%) が使っている', p_value, v_holder using errcode = '23505'; end if;
  end if;
  -- 今の一番新しい行と同じ (種類・値・写しと照らした結果) = 書かない (照らせなかった値を照らせた = 書く)
  if v_cur.rule_row is not null and v_cur.rule = p_kind and v_cur.value is not distinct from p_value and v_cur.lz_copy_state is not distinct from p_lz_state then
    -- 書かない = 残っている行の時刻 (#1719 R3 Low: 今回渡された時刻を返すと、画面と DB の時刻が食い違う)。🆕 #1719 R5: 書かなかった回も答えの控えに残す (再送を見分ける)
    v_res := pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'kind', p_kind, 'value', p_value, 'lz_copy_state', v_cur.lz_copy_state, 'lz_copy_as_of', v_cur.lz_copy_as_of,
      'changed', false, 'rule_row', v_cur.rule_row::text);
  else
    perform pg_catalog.set_config('ops.logizard_barcode_protocol', '1', true);
    insert into ops.sku_logizard_barcode_rules (company_id, sku_id, rule, value, via, actor_id, request_id, db_user, lz_copy_state, lz_copy_as_of)
      values (1, p_sku_id, p_kind, p_value, p_via, p_actor_id, p_request_id, v_db_user, p_lz_state, p_lz_as_of) returning rule_row into v_row;
    v_res := pg_catalog.jsonb_build_object('ok', true, 'sku_id', p_sku_id::text, 'kind', p_kind, 'value', p_value, 'lz_copy_state', p_lz_state, 'lz_copy_as_of', p_lz_as_of,
      'changed', true, 'rule_row', v_row::text);
  end if;
  perform pg_catalog.set_config('ops.logizard_barcode_protocol', '1', true);
  insert into ops.sku_logizard_barcode_requests (request_id, sku_id, actor_id, rule, value, via, seen_row, result) values (p_request_id, p_sku_id, p_actor_id, p_kind, p_value, p_via, p_seen_row, v_res);
  perform pg_catalog.set_config('ops.logizard_barcode_protocol', '', true);
  return v_res;
end $$;
revoke all on function ops.set_sku_logizard_barcode(uuid, text, bigint, text, text, text, bigint, text, timestamp with time zone) from public;

-- ═══════════ 3. CSV を出す・取り消す ═══════════
/** CSV の 1 つのセル (カンマ・"・改行を含む値だけ "…" で囲み、中の " は "" に重ねる = apps/master-decisions/lz-csv.mjs の cellBytes と同じ) */
create function ops.lz_csv_cell(p text) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case when coalesce(p, '') ~ '[",\r\n]' then '"' || pg_catalog.replace(p, '"', '""') || '"' else coalesce(p, '') end
$$;
revoke all on function ops.lz_csv_cell(text) from public;

/**
 * ロジザード登録の CSV を帳面に書く。🆕 #1721 R1 High 1: CSV の中身は DB が確かめ直した品目から作る (アプリの byte 列は受け取らない = 直に呼んでも別の中身を残せない)。
 *   中身 = 見出し + 品目の行 (商品コードの順・C の並び)・CRLF・最後に改行なし。列 = 商品ID (コード)・商品名・検索名称 (= 名前)・仕入単価 (今の原価・無い = 0)・
 *   有効期限区分 (02 / 01)・入荷日管理フラグ (1 / 0)・取引先コード (代表の仕入先・1〜3 桁は 4 桁に 0 埋め)・バーコード (空欄 = '')。Shift_JIS はダウンロードのときにアプリが付ける
 * p_items = [{ sku_id, code, rule_row, kind, value, expiry_code, inbound_code }] (1〜2000)。鍵 = SKU の鍵 (sku_id の順) → 値の鍵 (値の順) = 書く関数と同じ順。品目ごとに確かめる:
 *   単品・会社 1・やめていない・コードが今の SKU のコード・rule_row がその SKU の一番新しい行で種類と値が同じ・値が今の決まりに合う・
 *   有効期限区分 = 有効期限の管理 (あり 02 / なし 01)・入荷日管理フラグ = 入荷日の管理 (あり 1 / なし・入力なし 0)・代表の仕入先がある・まだ出していない・
 *   🆕 #1721 R2: 新商品の登録から来た商品だけ (ops.master_registrations.origin = new_entry)・p_lz_list_as_of (商品の一覧の写しの時刻) が 36 時間の中 (未来は 5 分まで)・
 *   🚨 既知の限界 (#1721 R2): 写しの中身 (ロジザードにもうある商品・ほかの持ち主のバーコード) は Render の SQLite = DB は見られない = アプリ (lib/lz-new-export.mjs) が確かめる。
 *     DB が確かめるのは時刻の新しさまで。この関数を呼べる master_edit の資格情報はアプリのサーバーだけが持つ (直に呼んで写しを偽るのは資格情報を持つ人だけ)
 *   値がほかの商品と重ならない (ops.lzbc_value_clash)・🆕 #1721 R1 High 2: 値のある品目は、保存のときの結果にかかわらず p_lz_bc_as_of (今照らした写しの時刻) が 36 時間の中 (未来は 5 分まで)。
 *   1 つでも違う = invalid_input / stale / lz_issued / lzbc_taken / lz_unchecked (何も書かない)
 * 同じ request_id の押し直し = 前の答え (人・要求の品目のハッシュが違う = request_id_reused)
 */
create function ops.lz_new_export_issue(p_request_id uuid, p_actor_id text, p_items jsonb, p_lz_list_as_of timestamptz, p_lz_bc_as_of timestamptz) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_hash    text;
  v_prev    ops.lz_new_exports;
  v_n       integer;
  v_id      bigint;
  it        jsonb;
  v_sid     bigint;
  k         record;
  c         record;
  v_cur     ops.sku_logizard_barcode_rules;
  v_fresh   boolean;
  v_re      jsonb := '{}'::jsonb;   -- 照らし直した品目 (保存のときは照らせていなかった・sku_id → true)
  v_lines   jsonb := '[]'::jsonb;   -- [{ code, line }]
  v_text    text;
begin
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, null) is not null or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール) が要る' using errcode = '22023';
  end if;
  if pg_catalog.jsonb_typeof(p_items) is distinct from 'array' then raise exception 'invalid_input: 品目は配列' using errcode = '22023'; end if;
  v_n := pg_catalog.jsonb_array_length(p_items);
  if v_n not between 1 and 2000 then raise exception 'invalid_input: 品目は 1〜2000 件' using errcode = '22023'; end if;
  for it in select x from pg_catalog.jsonb_array_elements(p_items) x loop
    if pg_catalog.jsonb_typeof(it -> 'sku_id') is distinct from 'string' or (it ->> 'sku_id') !~ '^[1-9][0-9]{0,17}$'
       or pg_catalog.jsonb_typeof(it -> 'rule_row') is distinct from 'string' or (it ->> 'rule_row') !~ '^[1-9][0-9]{0,17}$' then
      raise exception 'invalid_input: 品目の形が違う' using errcode = '22023';
    end if;
  end loop;
  -- 要求のハッシュ = sku_id:rule_row を sku_id の順に「,」でつなぐ (lib/lz-new-export.mjs lzRequestHash と同じ)
  v_hash := pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to((select pg_catalog.string_agg((x ->> 'sku_id') || ':' || (x ->> 'rule_row'), ',' order by (x ->> 'sku_id')::bigint, (x ->> 'rule_row')::bigint)
    from pg_catalog.jsonb_array_elements(p_items) x), 'UTF8')), 'hex');
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.lz_new_export:' || p_request_id::text, 0));
  select * into v_prev from ops.lz_new_exports e where e.request_id = p_request_id;
  if v_prev.export_id is not null then
    if v_prev.actor_id is distinct from p_actor_id or v_prev.request_hash is distinct from v_hash then
      raise exception 'request_id_reused: request_id % はほかの人・中身で使われている', p_request_id using errcode = '23505';
    end if;
    return pg_catalog.jsonb_build_object('ok', true, 'export_id', v_prev.export_id::text, 'text_sha256', v_prev.text_sha256, 'rows', v_prev.row_count, 'replayed', true);
  end if;
  -- 🆕 #1721 R2: ロジザードの商品の一覧の写し (まだ無いの判定) の時刻 = 36 時間の中・未来は 5 分まで (古い写しで作らない)
  if p_lz_list_as_of is null or p_lz_list_as_of <= pg_catalog.clock_timestamp() - interval '36 hours'
     or p_lz_list_as_of > pg_catalog.clock_timestamp() + interval '5 minutes' then
    raise exception 'lz_list_stale: ロジザードの商品の一覧の写しの時刻 (%) が 36 時間の中でない', coalesce(p_lz_list_as_of::text, 'なし') using errcode = '40001';
  end if;
  if (select pg_catalog.count(distinct (x ->> 'sku_id')) from pg_catalog.jsonb_array_elements(p_items) x) <> v_n then
    raise exception 'invalid_input: 同じ SKU が 2 回ある' using errcode = '22023';
  end if;
  if (select pg_catalog.count(*) from pg_catalog.jsonb_array_elements(p_items) x where x ->> 'value' is not null)
     <> (select pg_catalog.count(distinct x ->> 'value') from pg_catalog.jsonb_array_elements(p_items) x where x ->> 'value' is not null) then
    raise exception 'invalid_input: 同じバーコードが 2 つの商品にある' using errcode = '22023';
  end if;
  -- SKU の鍵 (sku_id の順) → 値の鍵 (値の順) = ops.set_sku_logizard_barcode・ops.edit_sku_jan と同じ順
  for v_sid in select (x ->> 'sku_id')::bigint from pg_catalog.jsonb_array_elements(p_items) x order by 1 loop
    perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || v_sid::text, 0));
  end loop;
  for k in select distinct x ->> 'value' as v from pg_catalog.jsonb_array_elements(p_items) x where x ->> 'value' is not null order by 1 loop
    perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.lzbc:' || k.v, 0));
  end loop;
  v_fresh := p_lz_bc_as_of is not null and p_lz_bc_as_of > pg_catalog.clock_timestamp() - interval '36 hours'
             and p_lz_bc_as_of <= pg_catalog.clock_timestamp() + interval '5 minutes';
  for it in select x from pg_catalog.jsonb_array_elements(p_items) x loop
    select s.sku_id, s.company_id, s.code, s.name, s.sku_kind, p.expiry_managed, p.inbound_date_managed, g.state as reg, g.origin,
           (select sc.cost_jpy::text from core.sku_costs sc where sc.sku_id = s.sku_id and sc.valid_to is null order by sc.valid_from desc limit 1) as cost,
           (select sp.code from core.supplier_skus x join core.suppliers sp on sp.supplier_id = x.supplier_id where x.sku_id = s.sku_id and x.is_primary limit 1) as supplier
      into k
      from core.skus s left join core.products p on p.product_id = s.product_id left join ops.master_registrations g on g.sku_id = s.sku_id
     where s.sku_id = (it ->> 'sku_id')::bigint;
    if k.sku_id is null or k.company_id <> 1 or k.sku_kind is distinct from 'single' or k.reg = 'cancelled' then
      raise exception 'invalid_input: % は単品で使える商品でない', coalesce(it ->> 'code', it ->> 'sku_id') using errcode = '22023';
    end if;
    -- 🆕 #1721 R2: 出すのは新商品の登録から来た商品だけ (前からある商品・NE で見つけた商品はロジザードにある前提)
    if k.origin is distinct from 'new_entry' then
      raise exception 'invalid_input: % は新商品の登録から来た商品でない (登録の状態の origin = %)', k.code, coalesce(k.origin, 'なし') using errcode = '22023';
    end if;
    if k.code is distinct from (it ->> 'code') then raise exception 'stale: % のコードが変わった', it ->> 'code' using errcode = '40001'; end if;
    select * into v_cur from ops.sku_logizard_barcode_rules r where r.sku_id = k.sku_id order by r.rule_row desc limit 1;
    if v_cur.rule_row is null or v_cur.rule_row::text is distinct from (it ->> 'rule_row') or v_cur.rule is distinct from (it ->> 'kind') or v_cur.value is distinct from (it ->> 'value') then
      raise exception 'stale: % のバーコードが画面を開いた後に変わった', k.code using errcode = '40001';
    end if;
    if not ((v_cur.rule = 'lz_assign' and v_cur.value is null) or (v_cur.rule = 'fnsku' and v_cur.value ~ '^X00[0-9A-Z]{7}$') or (v_cur.rule = 'jan' and ops.lzbc_jan_ok(v_cur.value))) then
      raise exception 'invalid_input: % のバーコードが入っていない (値か空欄を入れる)', k.code using errcode = '22023';
    end if;
    if (it ->> 'expiry_code') is distinct from (case when k.expiry_managed then '02' else '01' end)
       or (it ->> 'inbound_code') is distinct from (case when k.inbound_date_managed then '1' else '0' end) then
      raise exception 'stale: % の有効期限・入荷日の管理が画面を開いた後に変わった', k.code using errcode = '40001';
    end if;
    if k.supplier is null then raise exception 'invalid_input: % に代表の仕入先が無い', k.code using errcode = '22023'; end if;
    if exists (select 1 from ops.lz_new_export_items i where i.sku_id = k.sku_id and i.state = 'issued') then
      raise exception 'lz_issued: % はもうロジザード登録の CSV に出した', k.code using errcode = '23505';
    end if;
    if v_cur.value is not null then
      select x.kind, x.code into c from ops.lzbc_value_clash(k.sku_id, v_cur.value) x limit 1;
      if c.code is not null then
        raise exception 'lzbc_taken: % のバーコード % はほかの商品 (%) の % と重なっている', k.code, v_cur.value, c.code,
          case c.kind when 'fnsku' then 'Amazon の FNSKU' when 'jan' then 'JAN' else 'ロジザードのバーコード' end using errcode = '23505';
      end if;
      -- 🆕 #1721 R1 High 2: 値のある品目は、保存のときの結果にかかわらず今の写しで照らす (照らした写しの時刻が 36 時間の中)
      if not v_fresh then
        raise exception 'lz_unchecked: % のバーコードを今のロジザードの写しと照らせない (36 時間の中の新しい写しで照らすまで出さない)', k.code using errcode = '40001';
      end if;
      if v_cur.lz_copy_state is distinct from 'checked' then v_re := v_re || pg_catalog.jsonb_build_object(k.sku_id::text, true); end if;
    end if;
    v_lines := v_lines || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code', k.code, 'line', pg_catalog.concat_ws(',',
      ops.lz_csv_cell(k.code), ops.lz_csv_cell(k.name), ops.lz_csv_cell(k.name), ops.lz_csv_cell(coalesce(k.cost, '0')),
      ops.lz_csv_cell(case when k.expiry_managed then '02' else '01' end), ops.lz_csv_cell(case when k.inbound_date_managed then '1' else '0' end),
      ops.lz_csv_cell(case when k.supplier ~ '^[0-9]{1,3}$' then pg_catalog.lpad(k.supplier, 4, '0') else k.supplier end), ops.lz_csv_cell(coalesce(v_cur.value, '')))));
  end loop;
  v_text := '商品ID,商品名,検索名称,仕入単価,有効期限区分,入荷日管理フラグ,取引先コード,バーコード' || E'\r\n'
    || (select pg_catalog.string_agg(l ->> 'line', E'\r\n' order by (l ->> 'code') collate "C") from pg_catalog.jsonb_array_elements(v_lines) l);
  perform pg_catalog.set_config('ops.lz_new_export_protocol', '1', true);
  insert into ops.lz_new_exports (company_id, request_id, actor_id, db_user, schema_version, file_name, file_text, text_sha256, request_hash, row_count, lz_list_as_of, lz_bc_as_of)
    values (1, p_request_id, p_actor_id, v_db_user, 'lz-bc-v1', 'logizard_bc_upload.csv', v_text, pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(v_text, 'UTF8')), 'hex'),
            v_hash, v_n, p_lz_list_as_of, p_lz_bc_as_of) returning export_id into v_id;
  insert into ops.lz_new_export_items (export_id, company_id, sku_id, code, rule_row, kind, value, expiry_code, inbound_code, rechecked)
    select v_id, 1, (x ->> 'sku_id')::bigint, x ->> 'code', (x ->> 'rule_row')::bigint, x ->> 'kind', x ->> 'value', x ->> 'expiry_code', x ->> 'inbound_code',
           v_re ? (x ->> 'sku_id')
      from pg_catalog.jsonb_array_elements(p_items) x;
  perform pg_catalog.set_config('ops.lz_new_export_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'export_id', v_id::text, 'text_sha256', (select e.text_sha256 from ops.lz_new_exports e where e.export_id = v_id), 'rows', v_n);
end $$;
revoke all on function ops.lz_new_export_issue(uuid, text, jsonb, timestamptz, timestamptz) from public;

/** 出した CSV を取り消す (ロジザードに取り込めなかった・作り直す)。理由 1〜200 字。もう取り消した = already。取り消すと品目の SKU はもう一度出せる・値を直せる */
create function ops.lz_new_export_void(p_actor_id text, p_export_id bigint, p_reason text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_reason text := pg_catalog.btrim(coalesce(p_reason, ''));
  e ops.lz_new_exports;
  v_sid bigint;
begin
  if ops.reg_actor_problem(p_actor_id, v_reason) is not null or pg_catalog.length(v_reason) = 0 or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール) と理由 (200 字まで・改行なし) が要る' using errcode = '22023';
  end if;
  -- SKU の鍵 (sku_id の順) = 出す関数・書く関数と同じ (取り消しと値の直しが重なっても順に)
  for v_sid in select i.sku_id from ops.lz_new_export_items i where i.export_id = p_export_id order by 1 loop
    perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || v_sid::text, 0));
  end loop;
  select * into e from ops.lz_new_exports x where x.export_id = p_export_id for update;
  if e.export_id is null then raise exception 'not_found: ファイル #% が無い', p_export_id using errcode = '22023'; end if;
  if e.state = 'voided' then return pg_catalog.jsonb_build_object('ok', true, 'export_id', e.export_id::text, 'already', true); end if;
  perform pg_catalog.set_config('ops.lz_new_export_protocol', '1', true);
  update ops.lz_new_export_items set state = 'voided' where export_id = p_export_id and state = 'issued';
  update ops.lz_new_exports set state = 'voided', voided_at = pg_catalog.clock_timestamp(), voided_by = p_actor_id, void_reason = v_reason where export_id = p_export_id;
  perform pg_catalog.set_config('ops.lz_new_export_protocol', '', true);
  return pg_catalog.jsonb_build_object('ok', true, 'export_id', e.export_id::text);
end $$;
revoke all on function ops.lz_new_export_void(text, bigint, text) from public;
