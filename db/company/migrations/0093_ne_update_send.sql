-- 0093 (仮の番号・マージの時に付け替える): NE へ送る = 自社で直したけれど、まだ今朝の NE と違う欄だけを CSV にして NE に上げる
--   (AI_reference CompanyDB構想/25 の PR-2・2026-10-19 の方針の変更 = 中原さん「シンプルに自由に編集できて、取り込み時に照合して差分がある部分を CSV で 1 日 1 回アップ・都度アップも」)
--
-- なぜ: 商品の画面で原価・売価などを直しても、NE には翌朝の照合 → マスタの判断 → 列ごとの CSV を通さないと届かない (遅い・手間・直せない時間が長い)。
--   方針 (10/19): 編集は自由 (出ている CSV で保存を止めない)・毎朝の照合の後に自動で 1 回・画面の「今すぐ送る」で何度でも・翌朝 NE と同じになったものは差から消える。
-- なにを:
--   1. ops.ne_upd_sends (送る 1 回 = 自動 auto / 今すぐ now / CSV で受け取る download)・ops.ne_upd_files (NE に上げるファイル = 単品 / セット・列の組ごと)・
--      ops.ne_upd_rows (ファイルの行 = SKU・欄・CSV の文字・翌朝に比べる期待の値)・ops.ne_upd_verified (NE の実物の試しの結果 = 空欄で変わらないと確かめた列)・
--      ops.ne_upd_workers (miniPC の送り手の生存と設定)。表へは下の関数の中でだけ書ける (印 ops.ne_upd_protocol)・行は書き換えない・消さない
--   2. ops.ne_upd_units(SKU の配列 | null) = 差の一覧 (読むだけ)。単位 = (SKU・欄)。送る = 次の全部:
--      - 欄が対象の 8 つ (単品 = 名前・取扱・税率・売価・原価・代表の仕入先 / セット = 名前・売価)・DB の active でその欄の持ち主が company・段階 new_open・持ち主表のハッシュ = 段階の記録
--      - 今朝の NE の値 (0083 の ops.master_ne_values・その種類の取得の始めが今日 (JST)・値と書き方が同じ回) が ok で、区分が同じで、Company DB の今の値 (D2 の正規化) と違う
--      - その欄の今の値を最後に入れたのが人 (ポータル = source_system portal_master_edit) = 夜間ロード・NE の観測が入れた「わざと残した差」は出さない
--      - その変更の後に、マスタの判断で「差を残す・CDB を直す・入力を直す・仕様」の承認か却下が無い (まだ終わっていない fix_ne の承認は目標 = 今の値のときだけ)
--      - 新商品の途中 (draft / ne_pending / cancelled・NE 登録の CSV の生きた品目) でない・NE から消す依頼の途中 (0072 の live) でない
--      - 値が CSV に書ける (ne-csv.mjs の列の表と同じ決まり)・コードの書き方が決まる (設計 25 §2.4: NE の書き方 / ロジザードの書き方 = 正)
--      - もう送った (今朝の取得の後に作ったファイルに同じ値・同じ書き方で入っている) = 「送っている途中」(翌朝の照合で NE と同じになれば一覧から消える)
--      - 2 回 NE が受け付けたのに、その後の朝もまだ違う = 「NE が変わらない」(自動では送らない・NE の画面で)
--      コードの書き方だけを直す行 (NE の書き方 ≠ ロジザードの書き方・値の差が無い単品) = 原価 → 売価 → 名前の順で NE と Company DB が同じ欄を 1 つ入れて作る (role = spelling)
--   3. ops.ne_upd_build(how, 人 / 送り手) = 鍵の下で差を作り直してファイルを作る (DB が今の値から作る = アプリから値・列名・バイト列を受け取らない)。
--      自動 (auto) = 100 行を超える・ふだんの 5 倍を超える日は作らない (held)。今すぐ (now) = 画面の頼み (waiting) を拾う。前に上げたファイルを NE がまだ処理中なら待つ (busy)
--   4. 送り手 (miniPC・watch_writer): ops.ne_upd_file_result (上げた que_id・NE の受付の結果)・ops.ne_upd_queued_files・ops.ne_upd_worker_seen
--   5. 画面 (master_edit): ops.ne_upd_overview・ops.ne_upd_sku・ops.ne_upd_request_now (今すぐ送るの頼み)・ops.ne_upd_make_download / ops.ne_upd_download (CSV で受け取る)・
--      ops.ne_upd_record_verified (NE の実物の試しの結果)
-- ファイルの形 (設計 25 §2.3): 1 行 = 1 SKU・見出し = コードの列 + その行の組の欄 (列の表の順)・UTF-8・BOM なし・CRLF・1 ファイル 1,000 行まで。
--   空欄のセル (その SKU で変えていない欄) は、NE の実物で「空欄 = 変えない」を確かめた欄 (ops.ne_upd_verified の最後が ok) だけ。確かめていない欄は空欄にしない (組ごとにファイルを分ける)
-- 鍵の順: ops.ne_upd (排他) → NE のコードの共有 → NE の値の共有 → ロジザードの書き方の共有。record_ne_values (CSV → NE のコード共有 → NE の値の排他)・record_ne_codes・
--   record_lz_codes はこの鍵を取らない = 輪にならない。保存 (SKU の鍵 → CSV の鍵) とは鍵を共有しない (編集は自由・読んだ後に直した値は次に送る差に入る)
-- 🚨 security definer = search_path = pg_catalog, pg_temp・名前は全部 schema つき・一時の表なし・public の実行権なし。実行権は下の 9. で渡す (ロールの流し直しを待たない) +
--    scripts/company-db/create-master-edit-roles.mjs (master_edit)・create-watch-roles.mjs (watch_writer) も同じ
-- 🚨 既にある表・関数は変えない (表と関数を足すだけ)。NE に上げるのは miniPC の送り手だけ (env NE_UPDATE_UPLOAD=1・自動は NE_UPDATE_AUTO=1 も・最初は止めて出す)

-- ═══════════ 0. 前提 ═══════════
do $$ begin
  if pg_catalog.to_regclass('ops.master_ne_values_mark') is null or pg_catalog.to_regclass('ops.master_lz_codes_mark') is null then
    raise exception '0093 の前提: 今朝の NE の値・ロジザードの書き方 (0083) が無い';
  end if;
  if pg_catalog.to_regclass('ops.ne_del_items') is null or pg_catalog.to_regclass('ops.ne_reg_export_items') is null or pg_catalog.to_regclass('ops.master_registrations') is null then
    raise exception '0093 の前提: 登録の状態 (0052)・NE 登録の CSV (0053)・NE から消す CSV (0072) が無い';
  end if;
  if pg_catalog.to_regprocedure('ops.master_ownership_active_map()') is null or pg_catalog.to_regprocedure('ops.ownership_hash(jsonb)') is null then
    raise exception '0093 の前提: DB の active の持ち主 (0055 / 0058) が無い';
  end if;
  if pg_catalog.to_regclass('ops.ne_upd_sends') is not null then raise exception '0093: ops.ne_upd_sends がもうある (別の migration が同じ名前を使った?)'; end if;
end $$;

-- ═══════════ 1. 表 ═══════════
create table ops.ne_upd_sends (
  send_id             bigint generated always as identity primary key,
  how                 text not null check (how in ('auto', 'now', 'download')),
  request_id          uuid not null unique,
  requested_by        text not null check (pg_catalog.length(requested_by) between 1 and 320),
  requested_at        timestamptz not null default pg_catalog.clock_timestamp(),
  -- waiting = 今すぐ送るの頼み (送り手を待つ) / built = ファイルを作った (download はここまで) / sending = NE に上げた (受付の結果待ち) / sent = NE が全部受け付けた /
  -- failed = 上げられなかった・NE が断った / held = 作らなかった (多すぎ・今朝の NE の値がまだ ほか) / empty = 送る差が無かった
  state               text not null check (state in ('waiting', 'built', 'sending', 'sent', 'failed', 'held', 'empty')),
  reason              text check (reason is null or pg_catalog.length(reason) <= 500),
  ne_run              text,
  products_started_at timestamptz,
  sets_started_at     timestamptz,
  lz_exported_at      timestamptz,
  row_count           integer check (row_count is null or row_count >= 0),
  line_count          integer check (line_count is null or line_count >= 0),
  left_count          integer check (left_count is null or left_count >= 0),   -- 上限で今回に入れなかった行
  built_at            timestamptz,
  finished_at         timestamptz,
  host                text check (host is null or pg_catalog.length(host) <= 100),
  summary             jsonb,
  constraint ck_nus_built check ((state in ('waiting', 'held', 'empty')) or built_at is not null)
);
create index ix_ne_upd_sends_state on ops.ne_upd_sends (state, send_id);
comment on table ops.ne_upd_sends is '🆕 0093 (設計 25 PR-2・10/19 の方針): NE へ送る 1 回。書くのは ops.ne_upd_* の関数だけ';

create table ops.ne_upd_files (
  file_id        bigint generated always as identity primary key,
  send_id        bigint not null references ops.ne_upd_sends (send_id),
  kind           text not null check (kind in ('products', 'sets')),
  header         text not null check (pg_catalog.length(header) between 1 and 300),
  cols           text[] not null,
  converter_version text not null,
  file_bytes     bytea not null,
  sha256         text not null check (sha256 ~ '^[0-9a-f]{64}$'),
  line_count     integer not null check (line_count between 1 and 1000),
  -- made = 作った / queued = NE に上げた (que_id あり・受付の結果待ち) / ok = NE が全部受け付けた (que_status 2) / failed = 上げられなかった・NE が断った (-1)
  state          text not null default 'made' check (state in ('made', 'queued', 'ok', 'failed')),
  que_id         text check (que_id is null or que_id ~ '^[0-9]{1,20}$'),
  que_status     integer,
  que_message    text check (que_message is null or pg_catalog.length(que_message) <= 2000),
  uploaded_at    timestamptz,
  finished_at    timestamptz,
  downloaded_at  timestamptz,
  downloaded_by  text,
  download_count integer not null default 0 check (download_count >= 0),
  created_at     timestamptz not null default pg_catalog.clock_timestamp(),
  constraint ck_nuf_sha check (sha256 = pg_catalog.encode(pg_catalog.sha256(file_bytes), 'hex')),
  constraint ck_nuf_queued check ((state = 'queued') <= (que_id is not null)),
  constraint ck_nuf_download check ((downloaded_at is null) = (download_count = 0))
);
create index ix_ne_upd_files_send on ops.ne_upd_files (send_id);
create index ix_ne_upd_files_state on ops.ne_upd_files (state) where state in ('made', 'queued');

create table ops.ne_upd_rows (
  row_id     bigint generated always as identity primary key,
  file_id    bigint not null references ops.ne_upd_files (file_id),
  send_id    bigint not null references ops.ne_upd_sends (send_id),
  sku_id     bigint not null references core.skus (sku_id),
  code_norm  text not null,
  ne_code    text not null check (ne_code ~ '^[A-Za-z0-9_-]{1,30}$' and pg_catalog.lower(ne_code) = code_norm),
  col        text not null check (col in ('name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier')),
  role       text not null check (role in ('change', 'spelling')),
  cell       text not null,
  expect     jsonb not null,   -- 翌朝比べる値 (照合の D2 の正規化・版は norm_version)
  norm_version smallint not null,
  ne_before  jsonb,            -- 作った時の今朝の NE の値
  evidence   jsonb not null check (pg_catalog.jsonb_typeof(evidence) = 'object')
);
create index ix_ne_upd_rows_unit on ops.ne_upd_rows (code_norm, col, row_id);
create index ix_ne_upd_rows_file on ops.ne_upd_rows (file_id);

-- NE の実物の試し (設計 25 §2.9): 「空欄のセル = NE は変えない」を人が NE の画面で見た結果 (種類・欄ごと)。最後が ok の欄だけ空欄にしてよい
create table ops.ne_upd_verified (
  verified_id   bigint generated always as identity primary key,
  kind          text not null check (kind in ('products', 'sets')),
  -- code_case = 大文字・小文字だけ違う書き方 (ロジザードの書き方) の行で、NE が今ある商品のコードを書き換える (新しい商品を作らない) のを確かめた (設計 25 §5 の 3)
  col           text not null check (col in ('name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier', 'code_case')),
  entry         text not null check (entry in ('bulk', 'api')),   -- bulk = NE の画面の一括登録 / api = API のアップロード
  result        text not null check (result in ('ok', 'ng')),
  trial_sha256  text check (trial_sha256 is null or trial_sha256 ~ '^[0-9a-f]{64}$'),
  note          text check (note is null or pg_catalog.length(note) <= 1000),
  verified_by   text not null check (pg_catalog.length(verified_by) between 1 and 320),
  verified_at   timestamptz not null default pg_catalog.clock_timestamp(),
  constraint ck_nuv_kind_col check (kind = 'products' or col in ('name', 'standard_price_jpy'))   -- セットの書き方は直さない (code_case は単品だけ)
);
select core.make_append_only('ops', 'ne_upd_verified');

-- miniPC の送り手の生存と設定 (画面が「今すぐ送る」を出してよいか = 送り手が 15 分の中に生きていて、上げてよい設定のとき)
create table ops.ne_upd_workers (
  host          text primary key check (pg_catalog.length(host) between 1 and 100),
  seen_at       timestamptz not null,
  upload_on     boolean not null,
  auto_on       boolean not null,
  version       text check (version is null or pg_catalog.length(version) <= 100)
);

-- 表の守り: 書くのは 0093 の関数の中だけ (印 ops.ne_upd_protocol = '1')・消さない・行と中身は書き換えない (状態・結果の列だけ)
create function ops.guard_ne_upd_write() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_keep text[];
begin
  if tg_op = 'DELETE' then raise exception 'ne_upd_write: % の行は消さない', tg_table_name using errcode = '42501'; end if;
  if coalesce(pg_catalog.current_setting('ops.ne_upd_protocol', true), '') is distinct from '1' then
    raise exception 'ne_upd_write: % は NE へ送る関数 (ops.ne_upd_*) の中でだけ書く', tg_table_name using errcode = '42501';
  end if;
  if tg_op = 'UPDATE' then
    if tg_table_name = 'ne_upd_rows' then raise exception 'ne_upd_write: 行は書き換えない' using errcode = '42501'; end if;
    v_keep := case tg_table_name
      when 'ne_upd_sends' then array['state', 'reason', 'ne_run', 'products_started_at', 'sets_started_at', 'lz_exported_at', 'row_count', 'line_count', 'left_count', 'built_at', 'finished_at', 'host', 'summary']
      when 'ne_upd_files' then array['state', 'que_id', 'que_status', 'que_message', 'uploaded_at', 'finished_at', 'downloaded_at', 'downloaded_by', 'download_count']
      when 'ne_upd_workers' then array['seen_at', 'upload_on', 'auto_on', 'version']
      else array[]::text[] end;
    if (pg_catalog.to_jsonb(new) - v_keep) is distinct from (pg_catalog.to_jsonb(old) - v_keep) then
      raise exception 'ne_upd_write: % の中身は書き換えない (状態・結果の列だけ)', tg_table_name using errcode = '42501';
    end if;
    if tg_table_name = 'ne_upd_files' then
      -- 終わったファイルは戻さない・que_id と最初のダウンロードは一度入れたら変えない
      if old.state in ('ok', 'failed') and new.state is distinct from old.state then raise exception 'ne_upd_write: 終わったファイルの状態は戻さない' using errcode = '42501'; end if;
      if old.que_id is not null and new.que_id is distinct from old.que_id then raise exception 'ne_upd_write: que_id は変えない' using errcode = '42501'; end if;
      if old.downloaded_at is not null and (new.downloaded_at, new.downloaded_by) is distinct from (old.downloaded_at, old.downloaded_by) then
        raise exception 'ne_upd_write: 最初のダウンロードの記録は変えない' using errcode = '42501';
      end if;
    end if;
    if tg_table_name = 'ne_upd_sends' then
      if old.state in ('sent', 'failed', 'held', 'empty') and new.state is distinct from old.state then
        raise exception 'ne_upd_write: 終わった回の状態は戻さない' using errcode = '42501';
      end if;
    end if;
  end if;
  return new;
end $$;
revoke all on function ops.guard_ne_upd_write() from public;
create trigger trg_ne_upd_sends_guard before insert or update or delete on ops.ne_upd_sends for each row execute function ops.guard_ne_upd_write();
create trigger trg_ne_upd_files_guard before insert or update or delete on ops.ne_upd_files for each row execute function ops.guard_ne_upd_write();
create trigger trg_ne_upd_rows_guard before insert or update or delete on ops.ne_upd_rows for each row execute function ops.guard_ne_upd_write();
create trigger trg_ne_upd_workers_guard before insert or update or delete on ops.ne_upd_workers for each row execute function ops.guard_ne_upd_write();
create trigger trg_ne_upd_verified_guard before insert on ops.ne_upd_verified for each row execute function ops.guard_ne_upd_write();
create trigger trg_ne_upd_sends_no_truncate before truncate on ops.ne_upd_sends for each statement execute function core.reject_mutation();
create trigger trg_ne_upd_files_no_truncate before truncate on ops.ne_upd_files for each statement execute function core.reject_mutation();
create trigger trg_ne_upd_rows_no_truncate before truncate on ops.ne_upd_rows for each statement execute function core.reject_mutation();

-- ═══════════ 2. 列の表と CSV の書き方 (apps/master-decisions/ne-csv.mjs の COLUMNS と同じ・試験で照らす) ═══════════
/** 対象の欄: 種類・欄・NE の見出し・並び・持ち主のキー (lib/master-write.mjs の SINGLE_FIELDS / SET_FIELDS の keys と同じ)・変更の記録の列 */
create function ops.ne_upd_cols() returns table (kind text, col text, header text, ord integer, keys text[], attr text)
  language sql immutable set search_path = pg_catalog, pg_temp as $$
  select * from (values
    ('products', 'name', 'syohin_name', 1, array['skus.name', 'products.name'], 'name'),
    ('products', 'handling', 'toriatukai_kbn', 2, array['skus.handling', 'products.status'], 'handling'),
    ('products', 'tax_rate', 'tax_rate', 3, array['skus.tax_rate', 'skus.tax_class'], 'tax_rate'),
    ('products', 'standard_price_jpy', 'baika_tnk', 4, array['skus.standard_price'], 'standard_price_jpy'),
    ('products', 'cost', 'genka_tnk', 5, array['sku_costs'], null),
    ('products', 'primary_supplier', 'sire_code', 6, array['supplier_skus.is_primary'], null),
    ('sets', 'name', 'set_syohin_name', 1, array['skus.name'], 'name'),
    ('sets', 'standard_price_jpy', 'set_baika_tnk', 2, array['skus.standard_price'], 'standard_price_jpy')) as t(kind, col, header, ord, keys, attr)
$$;
revoke all on function ops.ne_upd_cols() from public;

/** CSV の 1 つのセルの文字 (ne-csv.mjs の COLUMNS の cell と同じ決まり)。{ ok, cell } / { ok: false, why } */
create function ops.ne_upd_cell(p_col text, p_value jsonb, p_code_norm text) returns jsonb
  language plpgsql immutable set search_path = pg_catalog, pg_temp as $$
declare
  v text;
  n numeric;
begin
  if p_value is null then return '{"ok": false, "why": "no_value"}'::jsonb; end if;
  if p_col = 'name' then
    if pg_catalog.jsonb_typeof(p_value) <> 'string' then return '{"ok": false, "why": "name_blank"}'::jsonb; end if;
    v := p_value #>> '{}';
    if v = '' or v <> pg_catalog.btrim(v) then return '{"ok": false, "why": "name_blank"}'::jsonb; end if;
    if pg_catalog.lower(v) = 'empty' then return '{"ok": false, "why": "empty_word"}'::jsonb; end if;
    if v ~ '[[:cntrl:]]' or v ~ ('[' || pg_catalog.chr(127) || '-' || pg_catalog.chr(159) || pg_catalog.chr(8232) || pg_catalog.chr(8233) || ']') then
      return '{"ok": false, "why": "name_control"}'::jsonb;
    end if;
    if v ~ ('[' || pg_catalog.chr(65536) || '-' || pg_catalog.chr(1114111) || ']') then return '{"ok": false, "why": "name_astral"}'::jsonb; end if;
    if pg_catalog.char_length(v) > 255 then return '{"ok": false, "why": "name_length"}'::jsonb; end if;
    if core.norm_code(v) = p_code_norm then return '{"ok": false, "why": "name_is_code"}'::jsonb; end if;
    return pg_catalog.jsonb_build_object('ok', true, 'cell', v);
  elsif p_col = 'handling' then
    if p_value = '"active"'::jsonb then return '{"ok": true, "cell": "0"}'::jsonb; end if;
    if p_value = '"discontinued"'::jsonb then return '{"ok": true, "cell": "1"}'::jsonb; end if;
    return '{"ok": false, "why": "handling_value"}'::jsonb;
  elsif p_col = 'tax_rate' then
    if pg_catalog.jsonb_typeof(p_value) = 'number' and (p_value #>> '{}')::numeric = 0.1 then return '{"ok": true, "cell": "10"}'::jsonb; end if;
    if pg_catalog.jsonb_typeof(p_value) = 'number' and (p_value #>> '{}')::numeric = 0.08 then return '{"ok": true, "cell": "8"}'::jsonb; end if;
    return '{"ok": false, "why": "tax_value"}'::jsonb;
  elsif p_col in ('standard_price_jpy', 'cost') then
    if pg_catalog.jsonb_typeof(p_value) <> 'number' then return '{"ok": false, "why": "yen_range"}'::jsonb; end if;
    n := (p_value #>> '{}')::numeric;
    if n <> pg_catalog.trunc(n) or n < 1 or n > 999999999 then return '{"ok": false, "why": "yen_range"}'::jsonb; end if;
    return pg_catalog.jsonb_build_object('ok', true, 'cell', n::bigint::text);
  elsif p_col = 'primary_supplier' then
    if pg_catalog.jsonb_typeof(p_value) <> 'array' or pg_catalog.jsonb_array_length(p_value) <> 1 or pg_catalog.jsonb_typeof(p_value -> 0) <> 'string'
       or (p_value ->> 0) !~ '^[0-9]{4}$' then
      return '{"ok": false, "why": "supplier_format"}'::jsonb;
    end if;
    return pg_catalog.jsonb_build_object('ok', true, 'cell', p_value ->> 0);
  end if;
  return '{"ok": false, "why": "col_not_csv"}'::jsonb;
end $$;
revoke all on function ops.ne_upd_cell(text, jsonb, text) from public;

/** CSV のセルの書き方 (ne-csv.mjs の quote と同じ): カンマ・引用符・前後の空白を含むときだけ引用符で囲む */
create function ops.ne_upd_quote(p text) returns text language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case when p ~ '[",]' or p <> pg_catalog.btrim(p) then '"' || pg_catalog.replace(p, '"', '""') || '"' else p end
$$;
revoke all on function ops.ne_upd_quote(text) from public;

/** Company DB の今の値 → 照合の D2 の正規化 (apps/company-db/master-compare/baseline.mjs の cdbValue と同じ) の文字の部分 (前後の空白・全角の空白を除いて空 = null) */
create function ops.ne_upd_text(p text) returns jsonb language sql immutable set search_path = pg_catalog, pg_temp as $$
  select coalesce(pg_catalog.to_jsonb(nullif(pg_catalog.btrim(p, ' ' || pg_catalog.chr(9) || pg_catalog.chr(10) || pg_catalog.chr(11) || pg_catalog.chr(12) || pg_catalog.chr(13)
    || pg_catalog.chr(160) || pg_catalog.chr(12288) || pg_catalog.chr(65279)), '')), 'null'::jsonb)
$$;
revoke all on function ops.ne_upd_text(text) from public;
/** 金額の正規化 (0・空 = 値なし) */
create function ops.ne_upd_yen(p numeric) returns jsonb language sql immutable set search_path = pg_catalog, pg_temp as $$
  select case when p is null or p = 0 then 'null'::jsonb else pg_catalog.to_jsonb(p) end
$$;
revoke all on function ops.ne_upd_yen(numeric) from public;

-- ═══════════ 3. 差の一覧 (読むだけ) ═══════════
/**
 * 差の一覧。p_sku_ids = その SKU だけ (商品の画面) / null = 全部。戻り値 = {
 *   ok (送ってよい状態か), problems: [{ reason, text }], ne: { run, products_started_at, sets_started_at, products_today, sets_today }, lz: { exported_at, fresh },
 *   blank_ok: { products: [欄], sets: [欄] } (空欄にしてよい欄),
 *   units: [{ sku_id, code, code_norm, kind (products / sets), col, role (change / spelling), name, ne_value, cdb_value, cell, ne_code, ne_spelling, lz_spelling, spelling_fix,
 *             status (send / sending / stuck / blocked), why, changed_by, changed_at, sent_count }] }
 * why (blocked): ne_not_today / not_open / decided / decision_other_value / new_product / deleting / handling_csv (取扱中止の CSV (0072) を作った後の取扱) /
 *   ne_code_unknown / ne_collided / case_unverified (NE の書き方が 2 つ・大文字・小文字の直しを NE の実物で確かめる前) / lz_many / lz_old / <セルの理由>
 */
create function ops.ne_upd_units(p_sku_ids bigint[]) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  m        ops.master_ne_values_mark;
  v_code_run text;
  v_today  date := core.jst_date(pg_catalog.clock_timestamp());
  v_own    jsonb := ops.master_ownership_active_map();
  v_phase  text;
  v_hash   text;
  v_lz_at  timestamptz;
  v_lz_fresh boolean;
  v_case_ok boolean;
  v_p_today boolean;
  v_s_today boolean;
  v_open   boolean;
  v_problems jsonb := '[]'::jsonb;
  v_blank  jsonb;
  v_units  jsonb;
begin
  select * into m from ops.master_ne_values_mark k where k.id = 1;
  select k.compare_run_id into v_code_run from ops.master_ne_code_mark k where k.id = 1;
  select s.phase, s.owner_hash into v_phase, v_hash from ops.master_cutover_state s where s.id = 1;
  select k.exported_at into v_lz_at from ops.master_lz_codes_mark k where k.id = 1;
  v_lz_fresh := v_lz_at is not null and v_lz_at > pg_catalog.clock_timestamp() - interval '36 hours';
  -- 大文字・小文字の直し (NE の書き方 → ロジザードの書き方) は、NE の実物で「今ある商品のコードを書き換える (新しい商品を作らない)」を確かめた後だけ (設計 25 §5 の 3)。
  --   確かめる前 = NE の今の書き方で送る (書き方が 2 つ = 決まらない = 送らない)・書き方だけの行も作らない
  v_case_ok := coalesce((select v.result = 'ok' from ops.ne_upd_verified v where v.kind = 'products' and v.col = 'code_case' order by v.verified_id desc limit 1), false);
  v_open := v_phase = 'new_open' and v_hash is not distinct from ops.ownership_hash(v_own);
  if not v_open then
    v_problems := v_problems || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_open', 'text', 'Company DB の保存がまだ開いていません (切替の段階)'));
  end if;
  if m.id is null then
    v_problems := v_problems || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_ne_values', 'text', '今朝の NE の値がまだ Company DB にありません (毎朝の照合のあとに入ります)'));
  elsif m.code_mark_run is distinct from m.compare_run_id or v_code_run is distinct from m.compare_run_id then
    v_problems := v_problems || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'ne_codes_other_run', 'text', 'NE の値と NE のコードの書き方が同じ朝のものではありません (次の照合を待つ)'));
  end if;
  v_p_today := m.id is not null and core.jst_date(m.products_started_at) = v_today;
  v_s_today := m.id is not null and core.jst_date(m.sets_started_at) = v_today;
  if m.id is not null and not v_p_today and not v_s_today then
    v_problems := v_problems || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'ne_not_today', 'text', '今朝の NE の取得がまだです (比べる NE の値が今日のものではない)'));
  end if;
  -- 空欄にしてよい欄 (種類・欄ごとに最後の試しの記録が ok)
  select pg_catalog.jsonb_build_object('products', coalesce(pg_catalog.jsonb_agg(x.col order by x.col) filter (where x.kind = 'products' and x.result = 'ok'), '[]'::jsonb),
                                       'sets', coalesce(pg_catalog.jsonb_agg(x.col order by x.col) filter (where x.kind = 'sets' and x.result = 'ok'), '[]'::jsonb))
    into v_blank
    from (select distinct on (v.kind, v.col) v.kind, v.col, v.result from ops.ne_upd_verified v where v.col <> 'code_case' order by v.kind, v.col, v.verified_id desc) x;

  with cols as (select * from ops.ne_upd_cols()),
  sk as (
    select k.sku_id, k.code, k.code_norm, k.sku_kind, case k.sku_kind when 'single' then 'products' else 'sets' end as kind,
           k.name, k.handling, k.tax_rate, k.standard_price_jpy
      from core.skus k
     where k.company_id = 1 and k.sku_kind in ('single', 'set') and (p_sku_ids is null or k.sku_id = any (p_sku_ids))
  ),
  cur as (
    select s.sku_id, s.code, s.code_norm, s.sku_kind, s.kind, s.name as sku_name, c.col, c.ord, c.keys, c.attr,
           case c.col
             when 'name' then ops.ne_upd_text(s.name)
             when 'handling' then coalesce(ops.ne_upd_text(s.handling), 'null'::jsonb)
             when 'tax_rate' then coalesce(pg_catalog.to_jsonb(s.tax_rate::numeric), 'null'::jsonb)
             when 'standard_price_jpy' then ops.ne_upd_yen(s.standard_price_jpy::numeric)
             when 'cost' then ops.ne_upd_yen(co.cost_jpy::numeric)
             when 'primary_supplier' then coalesce(ps.codes, '[]'::jsonb)
           end as cdb0,
           case c.attr when 'name' then pg_catalog.to_jsonb(s.name) when 'handling' then pg_catalog.to_jsonb(s.handling)
                       when 'tax_rate' then pg_catalog.to_jsonb(s.tax_rate) when 'standard_price_jpy' then pg_catalog.to_jsonb(s.standard_price_jpy) end as raw,
           co.sku_cost_id, ps.supplier_id as primary_supplier_id, ps.n as primary_n
      from sk s join cols c on c.kind = s.kind
      left join core.sku_costs co on c.col = 'cost' and co.sku_id = s.sku_id and co.valid_to is null
      left join lateral (select pg_catalog.jsonb_agg(sup.code_norm order by sup.code_norm) as codes, pg_catalog.min(x.supplier_id) as supplier_id, pg_catalog.count(*) as n
                           from core.supplier_skus x join core.suppliers sup on sup.supplier_id = x.supplier_id
                          where c.col = 'primary_supplier' and x.sku_id = s.sku_id and x.is_primary) ps on true
     where not exists (select 1 from pg_catalog.unnest(c.keys) k where (v_own ->> k) is distinct from 'company')
  ),
  -- 取扱の空は 'unknown' (baseline.mjs の cdbValue)
  cur2 as (select u.*, case when u.col = 'handling' and u.cdb0 = 'null'::jsonb then '"unknown"'::jsonb else u.cdb0 end as cdb from cur u),
  d as (
    select u.*, v.state as ne_state, v.value as ne, v.ne_kind
      from cur2 u join ops.master_ne_values v on v.code_norm = u.code_norm and v.col = u.col
     where v.ne_kind = case u.sku_kind when 'single' then 'single' else 'set' end
  ),
  -- 単品のコードの書き方 (設計 25 §2.4)。セットは NE の書き方だけ
  sp as (
    select s.sku_id, s.code_norm, s.kind, nc.state as nc_state, nc.ne_code as x, nc.spellings as nc_spellings, lz.state as lz_state, lz.lz_code as l,
           case
             when nc.state is null or nc.state = 'invalid' then null
             when s.kind = 'sets' then case when nc.state = 'ok' then nc.ne_code end
             when nc.state = 'ok' and v_case_ok and v_lz_fresh and lz.state = 'one' then lz.lz_code
             when nc.state = 'ok' and v_case_ok and v_lz_fresh and lz.state = 'many' then null
             when nc.state = 'ok' then nc.ne_code
             when nc.state = 'collided' and v_case_ok and v_lz_fresh and lz.state = 'one' then lz.lz_code
             else null
           end as code_to,
           case
             when nc.state is null or nc.state = 'invalid' then 'ne_code_unknown'
             when s.kind = 'sets' and nc.state <> 'ok' then 'ne_collided'
             when s.kind = 'sets' then null
             when nc.state = 'ok' and v_case_ok and v_lz_fresh and lz.state = 'many' then 'lz_many'
             when nc.state = 'ok' then null
             when nc.state = 'collided' and not v_case_ok then 'case_unverified'
             when nc.state = 'collided' and not v_lz_fresh then 'lz_old'
             when nc.state = 'collided' and coalesce(lz.state, '') <> 'one' then 'ne_collided'
             else null
           end as code_why
      from sk s
      left join ops.master_ne_codes nc on nc.code_norm = s.code_norm and nc.kind = 'product'
      left join ops.master_lz_codes lz on lz.code_norm = s.code_norm
  ),
  -- SKU の止め (新商品の途中・NE から消す依頼の途中)
  blk as (
    select s.sku_id,
           case when r.state in ('draft', 'ne_pending', 'cancelled')
                  or exists (select 1 from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state in ('built', 'issued', 'import_declared', 'partial')) then 'new_product'
                when exists (select 1 from ops.ne_del_items i where i.sku_id = s.sku_id and i.action = 'delete' and i.state = 'live') then 'deleting' end as why
      from sk s left join ops.master_registrations r on r.sku_id = s.sku_id
  ),
  -- 値の差がある単位 + その欄の今の値を最後に入れた変更の記録
  ch as (
    select d.*, e.event_id as ev_id, e.source_system as ev_source, e.actor_id as ev_actor, e.recorded_at as ev_at, e.new_value as ev_new, e.operation as ev_op
      from d
      left join lateral (
        select x.* from (
          (select e1.* from events.master_change_events e1
            where d.attr is not null and e1.entity_type = 'sku' and e1.entity_id = d.sku_id and e1.attribute = d.attr
            order by e1.recorded_at desc, e1.event_id desc limit 1)
          union all
          (select e2.* from events.master_change_events e2
            where d.col = 'cost' and d.sku_cost_id is not null and e2.entity_type = 'sku_cost' and e2.entity_id = d.sku_cost_id
            order by e2.recorded_at desc, e2.event_id desc limit 1)
          union all
          (select e3.* from events.master_change_events e3
            where d.col = 'primary_supplier' and d.primary_n = 1 and e3.entity_type = 'supplier_sku'
              and (e3.entity_key::text) = pg_catalog.jsonb_build_object('supplier_id', d.primary_supplier_id, 'sku_id', d.sku_id)::text
            order by e3.recorded_at desc, e3.event_id desc limit 1)) x
        order by x.recorded_at desc, x.event_id desc limit 1) e on true
     where d.ne_state = 'ok' and d.ne is distinct from d.cdb
  ),
  -- 人がポータルで入れた今の値だけ (夜間ロード・NE の観測の値 = わざと残した差 は出さない)
  human as (
    select c.* from ch c
     where c.ev_source = 'portal_master_edit' and (c.attr is null or c.ev_new is not distinct from c.raw)
  ),
  -- その変更の後のマスタの判断 (単位ごとの最新)
  dec as (
    select h.sku_id, h.col, x.kind as dec_kind, x.resolution, x.target, x.done
      from human h
      join lateral (
        select e.kind, e.resolution, e.target, e.created_at,
               exists (select 1 from ops.master_decision_events d2 where d2.kind = 'action_done' and d2.approved_event_id = e.event_id) as done
          from ops.master_decision_events e join ops.master_decision_candidates c on c.fingerprint = e.fingerprint
         where c.code_norm = h.code_norm and c.col = h.col and c.child is null and e.kind in ('approved', 'rejected', 'revoked')
         order by e.event_id desc limit 1) x on x.created_at > h.ev_at
  ),
  chg as (
    select h.sku_id, h.code, h.code_norm, h.kind, h.col, h.ord, 'change'::text as role, h.sku_name, h.ne, h.cdb, h.ev_actor, h.ev_at,
           ops.ne_upd_cell(h.col, h.cdb, h.code_norm) as cellr, sp.code_to, sp.code_why, sp.x, sp.l,
           coalesce(b.why, case when h.col = 'handling' and exists (select 1 from ops.ne_del_items i where i.sku_id = h.sku_id and i.action = 'handling' and i.created_at > h.ev_at)
                                then 'handling_csv' end) as blk_why,
           case when dc.dec_kind = 'rejected' or (dc.dec_kind = 'approved' and dc.resolution <> 'fix_ne') then 'decided'
                when dc.dec_kind = 'approved' and dc.resolution = 'fix_ne' and not dc.done and (dc.target -> 'value') is distinct from h.cdb then 'decision_other_value' end as dec_why
      from human h join sp on sp.sku_id = h.sku_id join blk b on b.sku_id = h.sku_id
      left join dec dc on dc.sku_id = h.sku_id and dc.col = h.col
  ),
  -- 送った記録 (今朝のその種類の取得の後に作った = 送っている途中 / 前に NE が受け付けた回の数)
  chg2 as (
    select c.*, (c.cellr ->> 'cell') as cell,
           exists (select 1 from ops.ne_upd_rows r join ops.ne_upd_files f on f.file_id = r.file_id join ops.ne_upd_sends s on s.send_id = r.send_id
                    where r.code_norm = c.code_norm and r.col = c.col and r.expect = c.cdb and r.ne_code = c.code_to
                      and s.built_at >= case c.kind when 'products' then m.products_started_at else m.sets_started_at end
                      and (f.state in ('queued', 'ok') or (f.state = 'made' and s.how <> 'download' and f.created_at > pg_catalog.clock_timestamp() - interval '30 minutes')
                           or f.downloaded_at is not null)) as in_flight,
           (select pg_catalog.count(distinct r.send_id) from ops.ne_upd_rows r join ops.ne_upd_files f on f.file_id = r.file_id join ops.ne_upd_sends s on s.send_id = r.send_id
             where r.code_norm = c.code_norm and r.col = c.col and r.expect = c.cdb and r.ne_code = c.code_to and f.state = 'ok'
               and s.built_at < case c.kind when 'products' then m.products_started_at else m.sets_started_at end) as sent_ok
      from chg c
  ),
  chg3 as (
    select c.*,
           case when not v_open then 'not_open'
                when (c.kind = 'products' and not v_p_today) or (c.kind = 'sets' and not v_s_today) or m.code_mark_run is distinct from m.compare_run_id
                     or v_code_run is distinct from m.compare_run_id then 'ne_not_today'
                when c.dec_why is not null then c.dec_why
                when c.blk_why is not null then c.blk_why
                when c.code_why is not null or c.code_to is null then coalesce(c.code_why, 'ne_code_unknown')
                when (c.cellr ->> 'ok') <> 'true' then c.cellr ->> 'why' end as why
      from chg2 c
  ),
  -- コードの書き方だけを直す行 (単品・値の差で送る行が無い SKU)。欄 = 原価 → 売価 → 名前 の順で、今朝の NE と Company DB が同じで CSV に書ける最初の欄
  carrier_sku as (
    select sp.* from sp where sp.kind = 'products' and sp.code_to is not null and sp.l is not null and sp.code_to is distinct from sp.x
       and not exists (select 1 from chg3 c where c.sku_id = sp.sku_id and c.why is null)
  ),
  car as (
    select distinct on (cs.sku_id) cs.sku_id, d.code, d.code_norm, d.kind, d.col, d.ord, 'spelling'::text as role, d.sku_name, d.ne, d.cdb, null::text as ev_actor, null::timestamptz as ev_at,
           ops.ne_upd_cell(d.col, d.cdb, d.code_norm) as cellr, cs.code_to, cs.code_why, cs.x, cs.l, b.why as blk_why, null::text as dec_why
      from carrier_sku cs join d on d.sku_id = cs.sku_id join blk b on b.sku_id = cs.sku_id
     where d.ne_state = 'ok' and d.ne = d.cdb and d.col in ('cost', 'standard_price_jpy', 'name') and (ops.ne_upd_cell(d.col, d.cdb, d.code_norm) ->> 'ok') = 'true'
     order by cs.sku_id, case d.col when 'cost' then 1 when 'standard_price_jpy' then 2 else 3 end
  ),
  car2 as (
    select c.*, (c.cellr ->> 'cell') as cell,
           exists (select 1 from ops.ne_upd_rows r join ops.ne_upd_files f on f.file_id = r.file_id join ops.ne_upd_sends s on s.send_id = r.send_id
                    where r.code_norm = c.code_norm and r.ne_code = c.code_to and s.built_at >= m.products_started_at
                      and (f.state in ('queued', 'ok') or (f.state = 'made' and s.how <> 'download' and f.created_at > pg_catalog.clock_timestamp() - interval '30 minutes')
                           or f.downloaded_at is not null)) as in_flight,
           (select pg_catalog.count(distinct r.send_id) from ops.ne_upd_rows r join ops.ne_upd_files f on f.file_id = r.file_id join ops.ne_upd_sends s on s.send_id = r.send_id
             where r.code_norm = c.code_norm and r.ne_code = c.code_to and f.state = 'ok' and s.built_at < m.products_started_at) as sent_ok,
           case when not v_open then 'not_open'
                when not v_p_today or m.code_mark_run is distinct from m.compare_run_id or v_code_run is distinct from m.compare_run_id then 'ne_not_today'
                when c.blk_why is not null then c.blk_why end as why
      from car c
  ),
  allu as (
    select c.sku_id, c.code, c.code_norm, c.kind, c.col, c.ord, c.role, c.sku_name, c.ne, c.cdb, c.cell, c.code_to, c.x, c.l, c.ev_actor, c.ev_at, c.in_flight, c.sent_ok, c.why from chg3 c
    union all
    select c.sku_id, c.code, c.code_norm, c.kind, c.col, c.ord, c.role, c.sku_name, c.ne, c.cdb, c.cell, c.code_to, c.x, c.l, c.ev_actor, c.ev_at, c.in_flight, c.sent_ok, c.why from car2 c
  )
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object(
           'sku_id', a.sku_id::text, 'code', a.code, 'code_norm', a.code_norm, 'kind', a.kind, 'col', a.col, 'ord', a.ord, 'role', a.role, 'name', a.sku_name,
           'ne_value', a.ne, 'cdb_value', a.cdb, 'cell', case when a.why is null then a.cell end, 'ne_code', case when a.why is null then a.code_to end,
           'ne_spelling', a.x, 'lz_spelling', a.l, 'spelling_fix', a.code_to is not null and a.code_to is distinct from a.x,
           'status', case when a.why is not null then 'blocked' when a.in_flight then 'sending' when a.sent_ok >= 2 then 'stuck' else 'send' end,
           'why', a.why, 'changed_by', a.ev_actor, 'changed_at', a.ev_at, 'sent_count', a.sent_ok)
         order by a.code_norm, a.kind, a.ord), '[]'::jsonb)
    into v_units
    from allu a;

  return pg_catalog.jsonb_build_object('ok', v_open and m.id is not null and m.code_mark_run is not distinct from m.compare_run_id and v_code_run is not distinct from m.compare_run_id
                                              and (v_p_today or v_s_today),
    'problems', v_problems,
    'ne', case when m.id is null then null else pg_catalog.jsonb_build_object('run', m.compare_run_id, 'products_started_at', m.products_started_at, 'sets_started_at', m.sets_started_at,
            'products_today', v_p_today, 'sets_today', v_s_today, 'norm_version', m.norm_version) end,
    'lz', pg_catalog.jsonb_build_object('exported_at', v_lz_at, 'fresh', v_lz_fresh),
    'blank_ok', v_blank, 'units', v_units);
end $$;
revoke all on function ops.ne_upd_units(bigint[]) from public;

-- ═══════════ 4. ファイルを作る ═══════════
/** 自動の上限 (行)。これを超える朝は作らない (held)。ふだん (この 14 日の自動の平均) の 5 倍を超える朝も (20 行より多いとき) */
create function ops.ne_upd_auto_limit() returns integer language sql immutable set search_path = pg_catalog, pg_temp as $$ select 100 $$;
revoke all on function ops.ne_upd_auto_limit() from public;
/** NE の一括登録の 1 ファイルの行の上限 */
create function ops.ne_upd_file_limit() returns integer language sql immutable set search_path = pg_catalog, pg_temp as $$ select 1000 $$;
revoke all on function ops.ne_upd_file_limit() from public;

/** 送り手 (miniPC) の前のファイルを NE がまだ処理中か (上げた・受付の結果待ち / 作って 30 分の中でまだ上げていない) */
create function ops.ne_upd_busy() returns boolean language sql stable set search_path = pg_catalog, pg_temp as $$
  select exists (select 1 from ops.ne_upd_files f join ops.ne_upd_sends s on s.send_id = f.send_id
                  where s.how <> 'download' and (f.state = 'queued' or (f.state = 'made' and f.created_at > pg_catalog.clock_timestamp() - interval '30 minutes')))
$$;
revoke all on function ops.ne_upd_busy() from public;

/**
 * 回 (send) の中で差を作り直してファイルを作る (取引の中・鍵の後・印の中で呼ぶ部品)。戻り値 = 回の状態と files (bytes_b64 つき)
 *   p_cap = 自動の上限を使うか (auto)。上限の外 = held (何も作らない)。今すぐ・受け取り = 1 ファイル 1,000 行まで (残りは left_count = 次の回)
 */
create function ops._ne_upd_fill(p_send_id bigint, p_cap boolean, p_host text) returns jsonb
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  u        jsonb := ops.ne_upd_units(null);
  v_rows   jsonb;
  v_n      integer;
  v_avg    numeric;
  v_limit  integer := ops.ne_upd_file_limit();
  v_blank  jsonb := u -> 'blank_ok';
  g        record;
  v_file   bigint;
  v_lines  text[];
  v_header text;
  v_bytes  bytea;
  v_total_lines integer := 0;
  v_total_rows  integer := 0;
  v_left   integer := 0;
  v_files  jsonb := '[]'::jsonb;
  v_reason text;
  v_cols   text[];
  r        record;
begin
  if (u ->> 'ok') is distinct from 'true' then
    v_reason := coalesce((select pg_catalog.string_agg(p ->> 'text', '・') from pg_catalog.jsonb_array_elements(u -> 'problems') p), '送れる状態でない');
    update ops.ne_upd_sends set state = 'held', reason = pg_catalog.left(v_reason, 500), host = p_host, finished_at = pg_catalog.clock_timestamp(),
           summary = pg_catalog.jsonb_build_object('problems', u -> 'problems') where send_id = p_send_id;
    return pg_catalog.jsonb_build_object('send_id', p_send_id::text, 'state', 'held', 'reason', v_reason, 'files', '[]'::jsonb);
  end if;
  select pg_catalog.jsonb_agg(x order by x ->> 'code_norm', x ->> 'kind', (x ->> 'ord')::integer) into v_rows
    from pg_catalog.jsonb_array_elements(u -> 'units') x where x ->> 'status' = 'send';
  v_n := coalesce(pg_catalog.jsonb_array_length(v_rows), 0);
  update ops.ne_upd_sends set ne_run = u #>> '{ne,run}', products_started_at = (u #>> '{ne,products_started_at}')::timestamptz, sets_started_at = (u #>> '{ne,sets_started_at}')::timestamptz,
         lz_exported_at = (u #>> '{lz,exported_at}')::timestamptz, host = p_host where send_id = p_send_id;
  if v_n = 0 then
    update ops.ne_upd_sends set state = 'empty', row_count = 0, line_count = 0, finished_at = pg_catalog.clock_timestamp() where send_id = p_send_id;
    return pg_catalog.jsonb_build_object('send_id', p_send_id::text, 'state', 'empty', 'files', '[]'::jsonb);
  end if;
  if p_cap then
    select pg_catalog.avg(s.row_count) into v_avg from ops.ne_upd_sends s
     where s.how = 'auto' and s.state in ('built', 'sending', 'sent') and s.row_count > 0 and s.requested_at > pg_catalog.clock_timestamp() - interval '14 days';
    if v_n > ops.ne_upd_auto_limit() then
      v_reason := pg_catalog.format('差が %s 行あり、自動の上限 (%s 行) を超えたので送りませんでした (画面で中身を見て「今すぐ送る」で送れます)', v_n, ops.ne_upd_auto_limit());
    elsif v_avg is not null and v_n > 20 and v_n > v_avg * 5 then
      v_reason := pg_catalog.format('差が %s 行あり、ふだん (この 14 日の平均 %s 行) の 5 倍を超えたので送りませんでした (画面で中身を見て「今すぐ送る」で送れます)', v_n, pg_catalog.round(v_avg, 1));
    end if;
    if v_reason is not null then
      update ops.ne_upd_sends set state = 'held', reason = v_reason, row_count = v_n, finished_at = pg_catalog.clock_timestamp() where send_id = p_send_id;
      return pg_catalog.jsonb_build_object('send_id', p_send_id::text, 'state', 'held', 'reason', v_reason, 'rows', v_n, 'files', '[]'::jsonb);
    end if;
  end if;
  -- 組: (種類・確かめていない欄の組) ごと。組の中の行は、確かめた欄だけが空欄になる (設計 25 §2.3)。SKU の行の欄 = その SKU の送る欄
  for g in
    with per_sku as (
      select x ->> 'sku_id' as sku_id, x ->> 'kind' as kind, x ->> 'ne_code' as ne_code, x ->> 'code_norm' as code_norm,
             pg_catalog.array_agg(x ->> 'col' order by (x ->> 'ord')::integer) as cols,
             pg_catalog.array_agg(x ->> 'col' order by (x ->> 'ord')::integer) filter (where not ((v_blank -> (x ->> 'kind')) ? (x ->> 'col'))) as hard
        from pg_catalog.jsonb_array_elements(v_rows) x group by 1, 2, 3, 4
    )
    select p.kind, coalesce(p.hard, '{}'::text[]) as hard, pg_catalog.array_agg(p.sku_id order by p.code_norm) as skus
      from per_sku p group by p.kind, coalesce(p.hard, '{}'::text[]) order by p.kind, coalesce(p.hard, '{}'::text[])
  loop
    -- この組の見出しの欄 = 組の SKU の欄の全部 (列の表の順)
    select pg_catalog.array_agg(c.col order by c.ord) into v_cols from ops.ne_upd_cols() c
     where c.kind = g.kind and exists (select 1 from pg_catalog.jsonb_array_elements(v_rows) x where x ->> 'kind' = g.kind and (x ->> 'sku_id') = any (g.skus) and x ->> 'col' = c.col);
    v_header := (case g.kind when 'products' then 'syohin_code' else 'set_syohin_code' end) || ','
      || (select pg_catalog.string_agg(c.header, ',' order by c.ord) from ops.ne_upd_cols() c where c.kind = g.kind and c.col = any (v_cols));
    -- 1,000 行ごとのファイル
    for r in
      select ((t.o - 1) / v_limit) as part, pg_catalog.array_agg(t.sku order by t.o) as skus
        from pg_catalog.unnest(g.skus) with ordinality as t(sku, o) group by 1 order by 1
    loop
      if not p_cap and r.part >= 1 then   -- 今すぐ・受け取り = 組ごとに 1,000 行まで (残りは次の回)
        v_left := v_left + pg_catalog.cardinality(r.skus);
        continue;
      end if;
      select pg_catalog.array_agg(line order by o) into v_lines from (
        select t.o, ops.ne_upd_quote(min(x ->> 'ne_code')) || ',' || (
                 select pg_catalog.string_agg(coalesce(ops.ne_upd_quote(y ->> 'cell'), ''), ',' order by c.ord)
                   from ops.ne_upd_cols() c left join pg_catalog.jsonb_array_elements(v_rows) y on y ->> 'sku_id' = t.sku and y ->> 'kind' = g.kind and y ->> 'col' = c.col
                  where c.kind = g.kind and c.col = any (v_cols)) as line
          from pg_catalog.unnest(r.skus) with ordinality as t(sku, o) join pg_catalog.jsonb_array_elements(v_rows) x on x ->> 'sku_id' = t.sku and x ->> 'kind' = g.kind
         group by t.o, t.sku) q;
      v_bytes := pg_catalog.convert_to(v_header || pg_catalog.chr(13) || pg_catalog.chr(10) || pg_catalog.array_to_string(v_lines, pg_catalog.chr(13) || pg_catalog.chr(10))
                                       || pg_catalog.chr(13) || pg_catalog.chr(10), 'UTF8');
      insert into ops.ne_upd_files (send_id, kind, header, cols, converter_version, file_bytes, sha256, line_count)
        values (p_send_id, g.kind, v_header, v_cols, 'ne-upd-v1', v_bytes, pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex'), pg_catalog.cardinality(v_lines))
        returning file_id into v_file;
      insert into ops.ne_upd_rows (file_id, send_id, sku_id, code_norm, ne_code, col, role, cell, expect, norm_version, ne_before, evidence)
        select v_file, p_send_id, (x ->> 'sku_id')::bigint, x ->> 'code_norm', x ->> 'ne_code', x ->> 'col', x ->> 'role', x ->> 'cell', x -> 'cdb_value',
               coalesce((u #>> '{ne,norm_version}')::smallint, 1), x -> 'ne_value',
               pg_catalog.jsonb_build_object('ne_run', u #>> '{ne,run}', 'ne_spelling', x -> 'ne_spelling', 'lz_spelling', x -> 'lz_spelling', 'lz_exported_at', u #> '{lz,exported_at}',
                                             'changed_by', x -> 'changed_by', 'changed_at', x -> 'changed_at', 'sent_count', x -> 'sent_count')
          from pg_catalog.jsonb_array_elements(v_rows) x where x ->> 'kind' = g.kind and (x ->> 'sku_id') = any (r.skus);
      v_total_lines := v_total_lines + pg_catalog.cardinality(v_lines);
      v_total_rows := v_total_rows + (select pg_catalog.count(*) from ops.ne_upd_rows z where z.file_id = v_file);
      v_files := v_files || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('file_id', v_file::text, 'kind', g.kind, 'header', v_header,
        'line_count', pg_catalog.cardinality(v_lines), 'sha256', pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex'), 'bytes_b64', pg_catalog.encode(v_bytes, 'base64')));
    end loop;
  end loop;
  update ops.ne_upd_sends set state = 'built', row_count = v_total_rows, line_count = v_total_lines, left_count = v_left, built_at = pg_catalog.clock_timestamp(),
         summary = (select pg_catalog.jsonb_build_object('cols', coalesce(pg_catalog.jsonb_object_agg(t.k, t.n), '{}'::jsonb)) from (
                      select case when z.role = 'spelling' then 'spelling' else f.kind || ':' || z.col end as k, pg_catalog.count(*) as n
                        from ops.ne_upd_rows z join ops.ne_upd_files f on f.file_id = z.file_id
                       where z.send_id = p_send_id group by 1) t)
   where send_id = p_send_id;
  return pg_catalog.jsonb_build_object('send_id', p_send_id::text, 'state', 'built', 'rows', v_total_rows, 'lines', v_total_lines, 'left', v_left, 'files', v_files,
    'summary', (select s.summary from ops.ne_upd_sends s where s.send_id = p_send_id));
end $$;
revoke all on function ops._ne_upd_fill(bigint, boolean, text) from public;

/** 鍵 (ops.ne_upd 排他 → NE のコード共有 → NE の値共有 → ロジザードの書き方共有) を取って印を立てる (取引の中・書く関数の最初) */
create function ops._ne_upd_begin() returns void language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_upd'));
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_values'));
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.lz_codes'));
  perform pg_catalog.set_config('ops.ne_upd_protocol', '1', true);
end $$;
revoke all on function ops._ne_upd_begin() from public;

/**
 * 送り手 (miniPC・watch_writer) がファイルを作る。p_how:
 *   auto = 毎朝の照合の後の自動 (同じ今朝の NE の回で 2 回目は作らない = 前の答え)。上限の外 = held。前のファイルを NE がまだ処理中 = held (busy)
 *   now  = 画面の「今すぐ送る」の頼み (waiting) をいちばん古いものから 1 つ。無い = { state: 'nothing' }。前のファイルを NE がまだ処理中 = { state: 'busy' } (頼みは残す)
 * 戻り値 = { send_id, state (built / held / empty / nothing / busy), files: [{ file_id, kind, header, line_count, sha256, bytes_b64 }] }
 */
create function ops.ne_upd_build(p_how text, p_host text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_send bigint;
  v_run  text;
  v_prev record;
  v_r    jsonb;
begin
  if p_how is null or p_how not in ('auto', 'now') then raise exception 'invalid_input: how は auto / now' using errcode = '22023'; end if;
  if coalesce(pg_catalog.btrim(p_host), '') = '' or pg_catalog.length(p_host) > 100 or p_host ~ '[[:cntrl:]]' then raise exception 'invalid_input: 送り手の名前が要る' using errcode = '22023'; end if;
  perform ops._ne_upd_begin();
  if p_how = 'auto' then
    select k.compare_run_id into v_run from ops.master_ne_values_mark k where k.id = 1;
    select s.send_id, s.state, s.reason into v_prev from ops.ne_upd_sends s where s.how = 'auto' and s.ne_run is not distinct from v_run and v_run is not null order by s.send_id desc limit 1;
    if v_prev.send_id is not null then
      perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
      return pg_catalog.jsonb_build_object('send_id', v_prev.send_id::text, 'state', v_prev.state, 'reason', v_prev.reason, 'replayed', true, 'files',
        coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('file_id', f.file_id::text, 'kind', f.kind, 'header', f.header, 'line_count', f.line_count, 'sha256', f.sha256,
                    'state', f.state, 'bytes_b64', case when f.state = 'made' then pg_catalog.encode(f.file_bytes, 'base64') end) order by f.file_id)
                   from ops.ne_upd_files f where f.send_id = v_prev.send_id), '[]'::jsonb));
    end if;
    insert into ops.ne_upd_sends (how, request_id, requested_by, state, ne_run, host) values ('auto', pg_catalog.gen_random_uuid(), p_host, 'waiting', v_run, p_host) returning send_id into v_send;
    if ops.ne_upd_busy() then
      update ops.ne_upd_sends set state = 'held', reason = '前に送ったファイルを NE がまだ受け付け中なので、今朝の自動は送りませんでした (画面の「今すぐ送る」で送れます)',
             finished_at = pg_catalog.clock_timestamp() where send_id = v_send;
      perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
      return pg_catalog.jsonb_build_object('send_id', v_send::text, 'state', 'held', 'reason', 'busy', 'files', '[]'::jsonb);
    end if;
    v_r := ops._ne_upd_fill(v_send, true, p_host);
  else
    select s.send_id into v_send from ops.ne_upd_sends s where s.how = 'now' and s.state = 'waiting' order by s.send_id limit 1 for update;
    if v_send is null then perform pg_catalog.set_config('ops.ne_upd_protocol', '', true); return '{"state": "nothing", "files": []}'::jsonb; end if;
    if ops.ne_upd_busy() then perform pg_catalog.set_config('ops.ne_upd_protocol', '', true); return pg_catalog.jsonb_build_object('send_id', v_send::text, 'state', 'busy', 'files', '[]'::jsonb); end if;
    v_r := ops._ne_upd_fill(v_send, false, p_host);
  end if;
  perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
  return v_r;
end $$;
revoke all on function ops.ne_upd_build(text, text) from public;

/**
 * 送り手が上げた結果を残す。p_que_id = NE のアップロードの que_id (上げられなかった = null)・p_que_status = NE の受付の状態 (2 = 全部成功 / 1 = 処理中 / 0 = 待ち / -1 = 失敗 / null = 上げられなかった)。
 *   made → queued (que_id) → ok (2) / failed (-1・上げられなかった)。回の全部のファイルが終わったら回を sent (全部 ok) / failed (1 つでも失敗) に
 */
create function ops.ne_upd_file_result(p_file_id bigint, p_que_id text, p_que_status integer, p_message text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  f ops.ne_upd_files;
  v_state text;
  v_left integer;
  v_bad integer;
  v_send_state text;
begin
  if p_que_id is not null and p_que_id !~ '^[0-9]{1,20}$' then raise exception 'invalid_input: que_id の形が違う' using errcode = '22023'; end if;
  if p_que_status is not null and p_que_status not in (-1, 0, 1, 2) then raise exception 'invalid_input: que_status は -1 / 0 / 1 / 2' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_upd'));
  perform pg_catalog.set_config('ops.ne_upd_protocol', '1', true);
  select * into f from ops.ne_upd_files x where x.file_id = p_file_id for update;
  if f.file_id is null then raise exception 'not_found: ファイル % が無い', p_file_id using errcode = 'P0002'; end if;
  if (select s.how from ops.ne_upd_sends s where s.send_id = f.send_id) = 'download' then raise exception 'invalid_input: 受け取りのファイルは NE に上げない' using errcode = '22023'; end if;
  if f.state in ('ok', 'failed') then
    perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
    return pg_catalog.jsonb_build_object('file_id', f.file_id::text, 'state', f.state, 'already', true);
  end if;
  v_state := case when p_que_status = 2 then 'ok' when p_que_status = -1 or (p_que_id is null and coalesce(f.que_id, '') = '') then 'failed' else 'queued' end;
  update ops.ne_upd_files set state = v_state, que_id = coalesce(que_id, p_que_id), que_status = p_que_status, que_message = pg_catalog.left(p_message, 2000),
         uploaded_at = coalesce(uploaded_at, case when p_que_id is not null then pg_catalog.clock_timestamp() end),
         finished_at = case when v_state in ('ok', 'failed') then pg_catalog.clock_timestamp() end
   where file_id = p_file_id;
  select pg_catalog.count(*) filter (where x.state in ('made', 'queued')), pg_catalog.count(*) filter (where x.state = 'failed') into v_left, v_bad from ops.ne_upd_files x where x.send_id = f.send_id;
  v_send_state := case when v_left > 0 then 'sending' when v_bad > 0 then 'failed' else 'sent' end;
  update ops.ne_upd_sends set state = v_send_state, finished_at = case when v_left = 0 then pg_catalog.clock_timestamp() end,
         reason = case when v_bad > 0 then pg_catalog.left(coalesce(reason || ' / ', '') || coalesce(p_message, 'NE に上げられなかった'), 500) else reason end
   where send_id = f.send_id and state in ('built', 'sending');
  perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
  return pg_catalog.jsonb_build_object('file_id', f.file_id::text, 'state', v_state, 'send_state', v_send_state);
end $$;
revoke all on function ops.ne_upd_file_result(bigint, text, integer, text) from public;

/** 送り手: NE の受付の結果を待っているファイル (queued) と、作ったがまだ上げていないファイル (made) */
create function ops.ne_upd_queued_files() returns jsonb language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('file_id', f.file_id::text, 'send_id', f.send_id::text, 'kind', f.kind, 'state', f.state, 'que_id', f.que_id,
           'created_at', f.created_at, 'uploaded_at', f.uploaded_at, 'bytes_b64', case when f.state = 'made' then pg_catalog.encode(f.file_bytes, 'base64') end) order by f.file_id), '[]'::jsonb)
    from ops.ne_upd_files f join ops.ne_upd_sends s on s.send_id = f.send_id where s.how <> 'download' and f.state in ('made', 'queued')
$$;
revoke all on function ops.ne_upd_queued_files() from public;

/** 送り手の生存と設定 (画面が「今すぐ送る」を出してよいか) */
create function ops.ne_upd_worker_seen(p_host text, p_upload boolean, p_auto boolean, p_version text) returns void
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if coalesce(pg_catalog.btrim(p_host), '') = '' or pg_catalog.length(p_host) > 100 or p_host ~ '[[:cntrl:]]' then raise exception 'invalid_input: 送り手の名前が要る' using errcode = '22023'; end if;
  perform pg_catalog.set_config('ops.ne_upd_protocol', '1', true);
  insert into ops.ne_upd_workers (host, seen_at, upload_on, auto_on, version) values (p_host, pg_catalog.clock_timestamp(), coalesce(p_upload, false), coalesce(p_auto, false), pg_catalog.left(p_version, 100))
    on conflict (host) do update set seen_at = excluded.seen_at, upload_on = excluded.upload_on, auto_on = excluded.auto_on, version = excluded.version;
  perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
end $$;
revoke all on function ops.ne_upd_worker_seen(text, boolean, boolean, text) from public;

-- ═══════════ 5. 画面 (master_edit) ═══════════
/** 送り手の様子 (15 分の中に見えた送り手のうち、上げてよい設定のもの) */
create function ops._ne_upd_worker_state() returns jsonb language sql stable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.jsonb_build_object(
    'upload_ready', exists (select 1 from ops.ne_upd_workers w where w.upload_on and w.seen_at > pg_catalog.clock_timestamp() - interval '15 minutes'),
    'auto_on', exists (select 1 from ops.ne_upd_workers w where w.auto_on and w.seen_at > pg_catalog.clock_timestamp() - interval '36 hours'),
    'seen_at', (select pg_catalog.max(w.seen_at) from ops.ne_upd_workers w))
$$;
revoke all on function ops._ne_upd_worker_state() from public;

/** 画面の一覧 (読むだけ・バイト列は出さない): 差の一覧 + この 14 日の回 (ファイルつき) + 送り手の様子 + 実機の試しの記録 */
create function ops.ne_upd_overview() returns jsonb language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  u jsonb := ops.ne_upd_units(null);
begin
  return u || pg_catalog.jsonb_build_object(
    'today', core.jst_date(pg_catalog.clock_timestamp()),
    'worker', ops._ne_upd_worker_state(),
    'waiting', exists (select 1 from ops.ne_upd_sends s where s.how = 'now' and s.state = 'waiting'),
    'sends', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('send_id', s.send_id::text, 'how', s.how, 'state', s.state, 'reason', s.reason,
        'requested_by', s.requested_by, 'requested_at', s.requested_at, 'built_at', s.built_at, 'finished_at', s.finished_at, 'rows', s.row_count, 'lines', s.line_count,
        'left', s.left_count, 'summary', s.summary,
        'files', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('file_id', f.file_id::text, 'kind', f.kind, 'header', f.header, 'line_count', f.line_count,
                   'state', f.state, 'que_status', f.que_status, 'que_message', f.que_message, 'downloaded_at', f.downloaded_at,
                   'downloadable', s.how = 'download' and core.jst_date(f.created_at) = core.jst_date(pg_catalog.clock_timestamp())) order by f.file_id)
                   from ops.ne_upd_files f where f.send_id = s.send_id), '[]'::jsonb)) order by s.send_id desc)
      from (select * from ops.ne_upd_sends x where x.requested_at > pg_catalog.clock_timestamp() - interval '14 days' order by x.send_id desc limit 50) s), '[]'::jsonb),
    'verified', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('kind', v.kind, 'col', v.col, 'entry', v.entry, 'result', v.result, 'verified_by', v.verified_by,
        'verified_at', v.verified_at, 'note', v.note) order by v.verified_id desc) from ops.ne_upd_verified v), '[]'::jsonb));
end $$;
revoke all on function ops.ne_upd_overview() from public;

/** 商品の画面: その SKU の差 (送る・送っている途中・止まっている) */
create function ops.ne_upd_sku(p_sku_id bigint) returns jsonb language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select ops.ne_upd_units(array[p_sku_id]) || pg_catalog.jsonb_build_object('worker', ops._ne_upd_worker_state())
$$;
revoke all on function ops.ne_upd_sku(bigint) from public;

/** 人 (アプリが言う人。DB では確かめられない) の形 */
create function ops._ne_upd_actor_ok(p text) returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$
  select p is not null and pg_catalog.length(pg_catalog.btrim(p)) between 1 and 320 and p !~ '[[:cntrl:]]'
$$;
revoke all on function ops._ne_upd_actor_ok(text) from public;

/**
 * 画面の「今すぐ NE へ送る」= 頼みを積む (送り手 = miniPC が数分の中に拾って作って上げる)。同じ request_id = 前の答え。
 *   もう待っている頼みがある = その頼みを返す (2 つ目を積まない)。戻り値 = { send_id, state, replayed?, joined? }
 */
create function ops.ne_upd_request_now(p_request_id uuid, p_actor text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  s ops.ne_upd_sends;
begin
  if p_request_id is null or not ops._ne_upd_actor_ok(p_actor) then raise exception 'invalid_input: request_id と人 (ログインのメール) が要る' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_upd'));
  select * into s from ops.ne_upd_sends x where x.request_id = p_request_id;
  if s.send_id is not null then
    if s.how <> 'now' or s.requested_by <> p_actor then raise exception 'request_id_reused: request_id % はほかの操作・人で使われている', p_request_id using errcode = '23505'; end if;
    return pg_catalog.jsonb_build_object('send_id', s.send_id::text, 'state', s.state, 'replayed', true);
  end if;
  select * into s from ops.ne_upd_sends x where x.how = 'now' and x.state = 'waiting' order by x.send_id limit 1;
  if s.send_id is not null then return pg_catalog.jsonb_build_object('send_id', s.send_id::text, 'state', s.state, 'joined', true); end if;
  perform pg_catalog.set_config('ops.ne_upd_protocol', '1', true);
  insert into ops.ne_upd_sends (how, request_id, requested_by, state) values ('now', p_request_id, p_actor, 'waiting') returning * into s;
  perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
  return pg_catalog.jsonb_build_object('send_id', s.send_id::text, 'state', s.state);
end $$;
revoke all on function ops.ne_upd_request_now(uuid, text) from public;

/**
 * 画面の「CSV で受け取る」(送り手が止めてある・人が自分で NE に取り込む) = その場で差を作り直してファイルを作る (同じ request_id = 前の答え)。
 *   バイト列はここでは返さない (ops.ne_upd_download で 1 つずつ・作った日だけ)
 */
create function ops.ne_upd_make_download(p_request_id uuid, p_actor text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  s ops.ne_upd_sends;
  v_r jsonb;
begin
  if p_request_id is null or not ops._ne_upd_actor_ok(p_actor) then raise exception 'invalid_input: request_id と人 (ログインのメール) が要る' using errcode = '22023'; end if;
  perform ops._ne_upd_begin();
  select * into s from ops.ne_upd_sends x where x.request_id = p_request_id;
  if s.send_id is not null then
    if s.how <> 'download' or s.requested_by <> p_actor then raise exception 'request_id_reused: request_id % はほかの操作・人で使われている', p_request_id using errcode = '23505'; end if;
    perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
    return pg_catalog.jsonb_build_object('send_id', s.send_id::text, 'state', s.state, 'reason', s.reason, 'replayed', true,
      'files', coalesce((select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('file_id', f.file_id::text, 'kind', f.kind, 'header', f.header, 'line_count', f.line_count) order by f.file_id)
                           from ops.ne_upd_files f where f.send_id = s.send_id), '[]'::jsonb));
  end if;
  insert into ops.ne_upd_sends (how, request_id, requested_by, state) values ('download', p_request_id, p_actor, 'waiting') returning * into s;
  v_r := ops._ne_upd_fill(s.send_id, false, null);
  perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
  return v_r || pg_catalog.jsonb_build_object('files', coalesce((select pg_catalog.jsonb_agg(f - 'bytes_b64') from pg_catalog.jsonb_array_elements(v_r -> 'files') f), '[]'::jsonb));
end $$;
revoke all on function ops.ne_upd_make_download(uuid, text) from public;

/** 受け取りのファイルのバイト列 (作った JST の日だけ・最初のダウンロードの人と時刻と回数を残す)。戻り値 = { file_id, kind, file_name, sha256, bytes_b64 } */
create function ops.ne_upd_download(p_actor text, p_file_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  f ops.ne_upd_files;
  v_how text;
begin
  if not ops._ne_upd_actor_ok(p_actor) then raise exception 'invalid_input: 人 (ログインのメール) が要る' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_upd'));
  select * into f from ops.ne_upd_files x where x.file_id = p_file_id for update;
  if f.file_id is null then raise exception 'not_found: ファイル % が無い', p_file_id using errcode = 'P0002'; end if;
  select s.how into v_how from ops.ne_upd_sends s where s.send_id = f.send_id;
  if v_how <> 'download' then raise exception 'not_download: このファイルは NE に直接上げるもの (受け取りのファイルでない)' using errcode = 'P0001'; end if;
  if core.jst_date(f.created_at) <> core.jst_date(pg_catalog.clock_timestamp()) then
    raise exception 'file_old: 前の日に作ったファイルは渡しません (今の値と違うかもしれない)。もう一度「CSV で受け取る」を押してください' using errcode = 'P0001';
  end if;
  perform pg_catalog.set_config('ops.ne_upd_protocol', '1', true);
  update ops.ne_upd_files set downloaded_at = coalesce(downloaded_at, pg_catalog.clock_timestamp()), downloaded_by = coalesce(downloaded_by, p_actor), download_count = download_count + 1
   where file_id = p_file_id returning * into f;
  update ops.ne_upd_sends set state = 'sent', finished_at = coalesce(finished_at, pg_catalog.clock_timestamp()) where send_id = f.send_id and state = 'built'
     and not exists (select 1 from ops.ne_upd_files x where x.send_id = f.send_id and x.downloaded_at is null);
  perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
  return pg_catalog.jsonb_build_object('file_id', f.file_id::text, 'kind', f.kind, 'sha256', f.sha256, 'line_count', f.line_count, 'bytes_b64', pg_catalog.encode(f.file_bytes, 'base64'),
    'file_name', pg_catalog.format('ne-update-%s-%s-%s.csv', f.kind, pg_catalog.to_char(f.created_at at time zone 'Asia/Tokyo', 'YYYYMMDD'), f.file_id));
end $$;
revoke all on function ops.ne_upd_download(text, bigint) from public;

/** NE の実物の試しの結果を残す (設計 25 §2.9・人が NE の画面で見た値)。名簿 (MASTER_DECISION_APPROVERS) は画面が確かめる */
create function ops.ne_upd_record_verified(p_actor text, p_kind text, p_col text, p_entry text, p_result text, p_trial_sha256 text, p_note text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_id bigint;
begin
  if not ops._ne_upd_actor_ok(p_actor) then raise exception 'invalid_input: 人 (ログインのメール) が要る' using errcode = '22023'; end if;
  if not exists (select 1 from ops.ne_upd_cols() c where c.kind = p_kind and c.col = p_col) and (p_kind, p_col) is distinct from ('products', 'code_case') then
    raise exception 'invalid_input: 種類・欄が違う' using errcode = '22023';
  end if;
  perform pg_catalog.set_config('ops.ne_upd_protocol', '1', true);
  insert into ops.ne_upd_verified (kind, col, entry, result, trial_sha256, note, verified_by) values (p_kind, p_col, p_entry, p_result, p_trial_sha256, nullif(p_note, ''), p_actor)
    returning verified_id into v_id;
  perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
  return pg_catalog.jsonb_build_object('verified_id', v_id::text);
end $$;
revoke all on function ops.ne_upd_record_verified(text, text, text, text, text, text, text) from public;

-- ═══════════ 9. 権限 (migration の中で渡す = ロールの流し直しを待たない。後から作るロールは create-master-edit-roles.mjs / create-watch-roles.mjs が同じ grant を付ける) ═══════════
--   master_edit (画面) = 一覧・商品の差・今すぐの頼み・受け取り・試しの記録の実行だけ / watch_writer (miniPC の送り手) = 作る・結果・待ち・生存・一覧の実行だけ / 表の権限はだれにも渡さない
do $$ begin
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'master_edit') then
    execute 'grant usage on schema ops to master_edit';
    execute 'grant execute on function ops.ne_upd_overview(), ops.ne_upd_sku(bigint), ops.ne_upd_request_now(uuid, text), ops.ne_upd_make_download(uuid, text), '
         || 'ops.ne_upd_download(text, bigint), ops.ne_upd_record_verified(text, text, text, text, text, text, text) to master_edit';
  end if;
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'watch_writer') then
    execute 'grant usage on schema ops to watch_writer';
    execute 'grant execute on function ops.ne_upd_build(text, text), ops.ne_upd_file_result(bigint, text, integer, text), ops.ne_upd_queued_files(), '
         || 'ops.ne_upd_worker_seen(text, boolean, boolean, text), ops.ne_upd_overview() to watch_writer';
  end if;
end $$;
