-- 0083 (仮の番号・マージの時に付け替える): 今朝の NE の値の表とロジザードの書き方の表 (AI_reference CompanyDB構想/25 v3 §2.5・§3 の PR-1 = 影・画面なし)
--
-- なぜ: 「NE 更新の CSV」(設計 25) は「今朝の NE の値 ≠ Company DB の今の値」の列を出し、翌朝「NE の値 = 期待の値」で届いたかを確かめる。
--   今の Company DB には今朝の NE の値を全部の (商品・列) で持つ表が無い (候補の print と最後に一致した値 0033 だけ)。
--   商品コードの大文字・小文字を直す行の「正しい書き方 = ロジザードの書き方」も、毎朝の lz-daily がロジザードの全件から作った一覧を Company DB に残していない
--   (Render の写し lz-mirror.mjs はバーコードマスタ由来 = 書き方の正に使えない・設計 25 R1 H4)。
-- なにを (PR-1 = 表と書く関数だけ。読む関数・CSV・画面は PR-2 / PR-3):
--   1. ops.master_ne_values      = 今朝の NE の値 (code_norm・列 → 状態 ok / unknown / invalid・値 = 照合の D2 の正規化 (baseline.mjs の neValue)・NE の区分 single / set)
--      ops.master_ne_values_mark = 今の中身の印 (1 行): 照合の回・単品とセットの取得の始め / 完了の時刻・通し番号・取得の件数・取得の版・正規化の版・中身のハッシュ
--      ops.master_ne_values_runs = 受け付けた回の記録 (追記だけ) = 「配った日より後に始まった完全な取得が照合で記録された」を後から言える (設計 25 R3 M1)
--      ops.record_ne_values(p)   = 照合 ② (miniPC・watch_writer) が 1 回 = 1 つの取引で全部を入れ替えて印を進める
--   2. ops.master_lz_codes       = ロジザードの書き方 (code_norm → 書き方の一覧・状態 one / many / invalid。one のときだけ lz_code)
--      ops.master_lz_codes_mark  = 今の中身の印 (1 行): lz-daily の回・ロジザードの全件の書き出しの時刻 (取り終えた時刻)・成功の印の時刻・CSV の sha256・行数
--      ops.master_lz_codes_runs  = 受け付けた書き出しの記録 (追記だけ)
--      ops.record_lz_codes(p)    = 毎朝の lz-daily (miniPC・watch_writer) が、ロジザードの全件の一覧の確かめを全部通った回だけ送る。書き方の組み分けは DB が自分でする (core.norm_code)
-- 受ける条件 (設計 25 §2.5・R1 H5・R2 M5・R2 H5):
--   NE の値: 照合の回が照合の回の表にある・商品とセットの取得の件数が取得の契約の式に合う (fetched = write_attempts + 落とした行・stored ≦ write_attempts)・
--     照合が読んだ生の行の数 = 取得の stored_rows・正規化した単位の数 = 生の行のコードの数 (単品 = 商品の表のコード − セットの表と重なるコード・セット = セットの表のコード)・
--     どのコードも区分の列が全部そろう・正規化の版が知っている版・空の一覧は断る・取得の時刻が照合の回より先 (10 分の余裕)・
--     取得の始めと通し番号と照合の回が今の印より新しい (戻らない)・同じ取得の始めで通し番号が違う = 断る・同じ回の再送はハッシュが同じなら何もしない (違えば断る)
--   ロジザードの書き方: 書き出しの時刻 (取り終えた時刻) が今の印より前 (戻る)・同じ書き出しで中身が違う・36 時間より前・未来は 5 分まで・空 = 断る。
--     成功の印は書き出しの後 15 分以内・印の日付 = 書き出しの JST の日付 (lz-daily と同じ確かめを DB でももう一度)。同じ書き出し・同じ中身の再送 = 何もしない
-- 鍵の順 (設計 25 §2.5 R2 H3): record_ne_values = CSV の鍵 (排他) → NE のコードの共有の鍵 → NE の値の排他の鍵 → 行。
--   PR-2 はこの同じ取引の中 (表を入れ替えた直後) で配ったファイルの行を確かめる (settle) = 確かめる回の値が入れ替わる前に必ず読む。PR-1 では CSV の行には触らない。
--   record_ne_codes (0041) は今のまま (NE のコードの排他の鍵だけ = CSV の鍵を待たない) = 輪にならない。照合は record_ne_codes → record_ne_values の順に呼ぶ。
--   record_lz_codes = ロジザードの書き方の排他の鍵だけ。
-- 🚨 security definer = search_path = pg_catalog, pg_temp・名前は全部 schema つき・一時の表を使わない・public の実行権なし (0053 以降の決まり)。
--    表へ直接は書けない (関数の中の印 ops.ne_values_protocol / ops.lz_codes_protocol が無いと trigger が断る・持ち主のロールでも)。
--    watch_writer は書く関数の実行だけ・watcher は表を読むだけ・master_edit (画面のロール) には何も渡さない (読むのは PR-2 の関数だけ = 0053 の境と同じ)
-- 🚨 既にある表・関数は変えない (表と関数を足すだけ)。関数が無い間・権限が無い間は、照合・lz-daily は今までどおり動き、この 2 つを書かない (アプリが関数と実行権を確かめてから呼ぶ)

-- ═══════════ 0. 前提 ═══════════
do $$ begin
  if pg_catalog.to_regclass('ops.master_compare_runs') is null then raise exception '0083 の前提: ops.master_compare_runs (0034) が無い'; end if;
  if pg_catalog.to_regclass('ops.master_ne_code_mark') is null or pg_catalog.to_regprocedure('ops.record_ne_codes(jsonb)') is null then
    raise exception '0083 の前提: NE のコードの書き方 (0041 の ops.master_ne_code_mark / ops.record_ne_codes) が無い';
  end if;
  if pg_catalog.to_regprocedure('core.norm_code(text)') is null or pg_catalog.to_regprocedure('core.jst_date(timestamptz)') is null
     or pg_catalog.to_regprocedure('core.make_append_only(text, text)') is null then
    raise exception '0083 の前提: core.norm_code / core.jst_date / core.make_append_only (0001) が無い';
  end if;
  if pg_catalog.to_regclass('ops.master_ne_values') is not null or pg_catalog.to_regclass('ops.master_lz_codes') is not null then
    raise exception '0083: ops.master_ne_values / ops.master_lz_codes がもうある (別の migration が同じ名前を使った?)';
  end if;
end $$;

-- ═══════════ 1. 表を直接書かせない印 (statement の trigger・行の数によらず 1 回) ═══════════
--   tg_argv[0] = 関数の中で立てる印の名前。印が無い INSERT / UPDATE / DELETE / TRUNCATE は持ち主のロールでも断る
create function ops.guard_master_ne_lz() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if coalesce(pg_catalog.current_setting(tg_argv[0], true), '') is distinct from '1' then
    raise exception 'ne_lz_guard: % は書く関数 (ops.record_ne_values / ops.record_lz_codes) の中でだけ書く', tg_table_name using errcode = '42501';
  end if;
  return null;
end $$;
revoke all on function ops.guard_master_ne_lz() from public;

-- ═══════════ 2. 今朝の NE の値 ═══════════
create table ops.master_ne_values (
  code_norm  text not null check (pg_catalog.length(code_norm) between 1 and 200),
  col        text not null check (col in ('name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier')),
  ne_kind    text not null check (ne_kind in ('single', 'set')),   -- NE の区分 (セットの表にある = set / 商品の表にだけある = single)
  state      text not null check (state in ('ok', 'unknown', 'invalid')),
  reason     text check (reason in ('raw_unknown', 'collided', 'int_blocked')),
  value      jsonb,   -- state = ok のときだけ (JSON の null = NE に値なし も ok の値)
  primary key (code_norm, col),
  constraint ck_mnv_kind_col check (ne_kind = 'single' or col in ('name', 'standard_price_jpy')),
  constraint ck_mnv_reason check ((state = 'unknown') = (reason is not null)),
  -- 値の形 = baseline.mjs の neValue の形 (名前 = 文字 / null・取扱 = active / discontinued / unknown・税率 = 0.1 / 0.08 / null・売価と原価 = 正の数 / null・仕入先 = 0 か 1 つの文字の配列)
  constraint ck_mnv_value check (case
    when state <> 'ok' then value is null
    when value is null then false
    when col = 'name' then pg_catalog.jsonb_typeof(value) in ('string', 'null')
    when col = 'handling' then pg_catalog.jsonb_typeof(value) = 'string' and (value #>> '{}') in ('active', 'discontinued', 'unknown')
    when col = 'tax_rate' then case when pg_catalog.jsonb_typeof(value) = 'null' then true when pg_catalog.jsonb_typeof(value) = 'number' then (value #>> '{}')::numeric in (0.1, 0.08) else false end
    when col in ('standard_price_jpy', 'cost') then case when pg_catalog.jsonb_typeof(value) = 'null' then true when pg_catalog.jsonb_typeof(value) = 'number' then (value #>> '{}')::numeric > 0 else false end
    when col = 'primary_supplier' then case when pg_catalog.jsonb_typeof(value) <> 'array' then false
      else pg_catalog.jsonb_array_length(value) = 0 or (pg_catalog.jsonb_array_length(value) = 1 and pg_catalog.jsonb_typeof(value -> 0) = 'string') end
    else false end)
);
comment on table ops.master_ne_values is '🆕 0083 (設計 25 PR-1): 今朝の NE の値 (照合 ② が読んだ完全な取得・D2 の正規化)。書くのは ops.record_ne_values だけ (1 回 = 全部を入れ替え)。値・書き方・印は同じ回のときだけ使う (master_ne_values_mark と master_ne_code_mark の回)';

-- 印 (1 行) と受け付けた回 (追記だけ) は同じ列
create table ops.master_ne_values_runs (
  compare_run_id       text primary key references ops.master_compare_runs (compare_run_id),
  observed_at          timestamptz not null,   -- 照合の回の表の値 (入力の時刻は使わない)
  norm_version         smallint not null,
  fetch_fingerprint    text not null check (fetch_fingerprint ~ '^[0-9a-f]{64}$'),
  products_started_at  timestamptz not null,   -- 単品の取得の始め (= 完了の印の時刻 = raw の世代)。「今日の値」はこの時刻の JST の日で決める (セットはセットの時刻)
  products_finished_at timestamptz not null,
  products_rev         bigint not null check (products_rev >= 0),
  sets_started_at      timestamptz not null,
  sets_finished_at     timestamptz not null,
  sets_rev             bigint not null check (sets_rev >= 0),
  fetch_counts         jsonb not null,         -- 送られた取得の件数 (products / sets)
  clean                jsonb not null,         -- DB が件数から決めた「落とした行・重なりが無い」(products / sets の真偽)
  content_hash         text not null check (content_hash ~ '^[0-9a-f]{64}$'),
  counts               jsonb not null,
  code_mark_run        text,                   -- 書いたときの NE のコードの印の回 (この回と同じなら値と書き方が同じ回)
  recorded_at          timestamptz not null default pg_catalog.clock_timestamp(),
  constraint ck_mnvr_times check (products_started_at <= products_finished_at and sets_started_at <= sets_finished_at)
);
comment on table ops.master_ne_values_runs is '🆕 0083: ops.record_ne_values が受け付けた回 (追記だけ)。どの照合の回が、いつ始まった完全な取得を記録したか';
create table ops.master_ne_values_mark (
  id smallint primary key default 1 check (id = 1),
  like ops.master_ne_values_runs including constraints including defaults
);
alter table ops.master_ne_values_mark add constraint fk_mnvm_run foreign key (compare_run_id) references ops.master_ne_values_runs (compare_run_id);
comment on table ops.master_ne_values_mark is '🆕 0083: ops.master_ne_values の中身はどの回か (1 行)。ops.record_ne_values だけが進める';

create trigger trg_guard_mnv before insert or update or delete or truncate on ops.master_ne_values for each statement execute function ops.guard_master_ne_lz('ops.ne_values_protocol');
create trigger trg_guard_mnvm before insert or update or delete or truncate on ops.master_ne_values_mark for each statement execute function ops.guard_master_ne_lz('ops.ne_values_protocol');
create trigger trg_guard_mnvr before insert or update or delete or truncate on ops.master_ne_values_runs for each statement execute function ops.guard_master_ne_lz('ops.ne_values_protocol');
select core.make_append_only('ops', 'master_ne_values_runs');

-- 取得の 1 つの側 (products / sets) を確かめて、DB の形にする (security definer の関数の中から持ち主の権限で呼ぶ・だれにも渡さない)。
--   件数 = 0 以上の整数 (JSON の数・小数・指数なし)。時刻 = ISO の UTC・ミリ秒 (取得の件数の started_at / finished_at の形)。
--   式: fetched_rows = write_attempts + dropped_no_code + dropped_missing_fields・stored_rows ≦ write_attempts・照合が読んだ生の行 (raw_rows) = stored_rows
create function ops._nev_fetch_side(p jsonb, p_side text, p_observed timestamptz) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  k text;
  v jsonb;
  n jsonb := '{}'::jsonb;
  v_s timestamptz;
  v_f timestamptz;
begin
  if pg_catalog.jsonb_typeof(p) is distinct from 'object' then raise exception 'invalid_input: fetch.% が無い', p_side using errcode = '22023'; end if;
  foreach k in array (array['complete_rev', 'fetched_rows', 'write_attempts', 'stored_rows', 'dropped_no_code', 'dropped_missing_fields', 'raw_rows', 'codes']
    || case when p_side = 'products' then array['set_overlap'] else array[]::text[] end) loop
    v := p -> k;
    if v is null or pg_catalog.jsonb_typeof(v) <> 'number' or (v #>> '{}') !~ '^[0-9]{1,15}$' then
      raise exception 'invalid_input: fetch.%.% は 0 以上の整数', p_side, k using errcode = '22023';
    end if;
    n := n || pg_catalog.jsonb_build_object(k, (v #>> '{}')::bigint);
  end loop;
  foreach k in array array['started_at', 'finished_at'] loop
    if coalesce(p ->> k, '') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$' then
      raise exception 'invalid_input: fetch.%.% は ISO の UTC (ミリ秒) の時刻', p_side, k using errcode = '22023';
    end if;
  end loop;
  begin
    v_s := (p ->> 'started_at')::timestamptz; v_f := (p ->> 'finished_at')::timestamptz;
  exception when others then raise exception 'invalid_input: fetch.% の時刻を読めない', p_side using errcode = '22023';
  end;
  if v_s > v_f then raise exception 'invalid_input: fetch.% の完了が始めより前', p_side using errcode = '22023'; end if;
  -- 取得は照合の前に終わっている (照合の回の時刻より 10 分を超えて後 = 別の取得・時計の狂い)
  if v_f > p_observed + interval '10 minutes' then raise exception 'invalid_input: fetch.% の完了が照合の回より後', p_side using errcode = '22023'; end if;
  if (n ->> 'fetched_rows')::bigint <> (n ->> 'write_attempts')::bigint + (n ->> 'dropped_no_code')::bigint + (n ->> 'dropped_missing_fields')::bigint then
    raise exception 'fetch_counts_mismatch: fetch.% の件数の式が合わない (fetched_rows ≠ write_attempts + 落とした行)', p_side using errcode = '22023';
  end if;
  if (n ->> 'stored_rows')::bigint > (n ->> 'write_attempts')::bigint then
    raise exception 'fetch_counts_mismatch: fetch.% の stored_rows が write_attempts より多い', p_side using errcode = '22023';
  end if;
  if (n ->> 'raw_rows')::bigint <> (n ->> 'stored_rows')::bigint then
    raise exception 'fetch_counts_mismatch: fetch.% の読んだ生の行 % ≠ 取得の stored_rows %', p_side, n ->> 'raw_rows', n ->> 'stored_rows' using errcode = '22023';
  end if;
  return n || pg_catalog.jsonb_build_object('started_at', p ->> 'started_at', 'finished_at', p ->> 'finished_at',
    'clean', (n ->> 'dropped_no_code')::bigint = 0 and (n ->> 'dropped_missing_fields')::bigint = 0 and (n ->> 'write_attempts')::bigint = (n ->> 'stored_rows')::bigint);
end $$;
revoke all on function ops._nev_fetch_side(jsonb, text, timestamptz) from public;

/**
 * 今朝の NE の値を入れ替える (照合 ② の最後・watch_writer・1 回 = 1 つの取引)。
 * p = { compare_run_id, norm_version, fetch: { fingerprint, products: {...}, sets: {...} }, entries: [{ code_norm, col, ne_kind, state, reason?, value? }] }
 *   products / sets = 取得の件数 (complete_rev・fetched_rows・write_attempts・stored_rows・dropped_no_code・dropped_missing_fields・started_at・finished_at)
 *     + 照合が読んだ生の行の数 (raw_rows)・そのコードの数 (codes)・(単品だけ) セットの表と重なるコードの数 (set_overlap)
 * 戻り値 = { state: written | unchanged, rows, ... }。断る = 例外 (呼び手は失敗として残すだけ = 照合は失敗にしない)
 */
create function ops.record_ne_values(p jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_run   text := p ->> 'compare_run_id';
  v_at    timestamptz;
  v_nv    integer;
  v_fp    text := p #>> '{fetch,fingerprint}';
  fp      jsonb;
  fs      jsonb;
  v_n     integer;
  v_bad   integer;
  v_dup   integer;
  v_single integer;
  v_set   integer;
  v_hash  text;
  v_code_run text;
  v_counts jsonb;
  m       ops.master_ne_values_mark%rowtype;
begin
  -- 鍵の順 (設計 25 §2.5): CSV の鍵 → NE のコードの共有の鍵 → NE の値の排他の鍵 (印が無い初回も・検査の前に)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_csv'));
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_values'));
  if pg_catalog.jsonb_typeof(p) is distinct from 'object' then raise exception 'invalid_input: 入力が object でない' using errcode = '22023'; end if;
  if v_run is null or v_run !~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'invalid_input: compare_run_id の形が違う: %', v_run using errcode = '22023'; end if;
  -- 時刻は照合の回の表の値 (入力の時刻は使わない・0041 と同じ)
  select r.observed_at into v_at from ops.master_compare_runs r where r.compare_run_id = v_run;
  if not found then raise exception 'unknown_run: 照合の回の記録が無い: %', v_run using errcode = '23503'; end if;
  -- 正規化の版 = apps/company-db/master-compare/baseline.mjs の BASELINE_NORM_VERSION (上げたらここも同じ migration で足す)
  if pg_catalog.jsonb_typeof(p -> 'norm_version') is distinct from 'number' or (p ->> 'norm_version') !~ '^[0-9]{1,4}$' then raise exception 'invalid_input: norm_version が無い' using errcode = '22023'; end if;
  v_nv := (p ->> 'norm_version')::integer;
  if v_nv not in (1) then raise exception 'norm_version_rejected: 正規化の版 % は受け付けない', v_nv using errcode = '22023'; end if;
  if v_fp is null or v_fp !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: fetch.fingerprint (取得の版) の形が違う' using errcode = '22023'; end if;
  fp := ops._nev_fetch_side(p #> '{fetch,products}', 'products', v_at);
  fs := ops._nev_fetch_side(p #> '{fetch,sets}', 'sets', v_at);
  if (fp ->> 'set_overlap')::bigint > (fp ->> 'codes')::bigint then raise exception 'invalid_input: fetch.products.set_overlap が codes より多い' using errcode = '22023'; end if;
  if (fp ->> 'codes')::bigint = 0 then raise exception 'empty: 単品の取得のコードが 0 (空の一覧は受け付けない)' using errcode = '22023'; end if;

  if pg_catalog.jsonb_typeof(p -> 'entries') is distinct from 'array' then raise exception 'invalid_input: entries が配列でない' using errcode = '22023'; end if;
  v_n := pg_catalog.jsonb_array_length(p -> 'entries');
  if v_n = 0 then raise exception 'empty: entries が空 (空の一覧は受け付けない)' using errcode = '22023'; end if;
  -- 1 行ずつの形 (code_norm は正規化の後の形・列と区分の組・状態と値)
  select pg_catalog.count(*) filter (where pg_catalog.jsonb_typeof(x) <> 'object'
           or pg_catalog.jsonb_typeof(x -> 'code_norm') is distinct from 'string' or pg_catalog.length(x ->> 'code_norm') not between 1 and 200
           or core.norm_code(x ->> 'code_norm') is distinct from (x ->> 'code_norm')
           or (x ->> 'ne_kind') is null or (x ->> 'ne_kind') not in ('single', 'set')
           or (x ->> 'col') is null or (x ->> 'col') not in ('name', 'handling', 'tax_rate', 'standard_price_jpy', 'cost', 'primary_supplier')
           or ((x ->> 'ne_kind') = 'set' and (x ->> 'col') not in ('name', 'standard_price_jpy'))
           or (x ->> 'state') is null or (x ->> 'state') not in ('ok', 'unknown', 'invalid')
           or ((x ->> 'state') = 'ok') <> (x ? 'value')   -- ok の行だけ値を持つ (ok でない行は value の鍵ごと送らない)
           or ((x ->> 'state') = 'unknown') <> ((x ->> 'reason') is not null)),
         pg_catalog.count(distinct ((x ->> 'code_norm'), (x ->> 'col')))
    into v_bad, v_dup
    from pg_catalog.jsonb_array_elements(p -> 'entries') x;
  if v_bad > 0 then raise exception 'invalid_input: 形の違う行が % 行', v_bad using errcode = '22023'; end if;
  if v_dup <> v_n then raise exception 'invalid_input: (code_norm, col) の重なりが % 行', v_n - v_dup using errcode = '22023'; end if;
  -- どのコードも区分が 1 つ・その区分の列が全部そろう (単品 6 列・セット 2 列 = 落とした列が無い)
  select pg_catalog.count(*) into v_bad from (
    select x ->> 'code_norm' as c, pg_catalog.count(distinct x ->> 'ne_kind') as kinds, pg_catalog.min(x ->> 'ne_kind') as kind, pg_catalog.count(*) as cols
      from pg_catalog.jsonb_array_elements(p -> 'entries') x group by 1) g
    where g.kinds <> 1 or g.cols <> case g.kind when 'single' then 6 else 2 end;
  if v_bad > 0 then raise exception 'incomplete: 区分の列がそろわない (または区分が 2 つ) のコードが %', v_bad using errcode = '22023'; end if;
  -- 正規化した単位の数 = 照合が読んだ生の行のコードの数 (単品 = 商品の表のコード − セットの表と重なるコード・セット = セットの表のコード。設計 25 R2 M5)
  select pg_catalog.count(distinct x ->> 'code_norm') filter (where x ->> 'ne_kind' = 'single'), pg_catalog.count(distinct x ->> 'code_norm') filter (where x ->> 'ne_kind' = 'set')
    into v_single, v_set from pg_catalog.jsonb_array_elements(p -> 'entries') x;
  if v_single <> (fp ->> 'codes')::bigint - (fp ->> 'set_overlap')::bigint or v_set <> (fs ->> 'codes')::bigint then
    raise exception 'units_mismatch: 単位の数 (単品 %・セット %) が生の行のコードの数 (単品 %・セット %) と合わない', v_single, v_set,
      (fp ->> 'codes')::bigint - (fp ->> 'set_overlap')::bigint, fs ->> 'codes' using errcode = '22023';
  end if;
  -- 中身のハッシュ (行の順に依らない・取得の印と正規化の版も入れる)
  select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(pg_catalog.concat_ws(E'\n', 'nev1', v_nv::text, v_fp,
      fp ->> 'started_at', fp ->> 'complete_rev', fs ->> 'started_at', fs ->> 'complete_rev',
      coalesce(pg_catalog.string_agg(pg_catalog.concat_ws('|', x ->> 'code_norm', x ->> 'col', x ->> 'ne_kind', x ->> 'state', coalesce(x ->> 'reason', ''),
        case when x ->> 'state' = 'ok' then (x -> 'value')::text else '' end), E'\n' order by (x ->> 'code_norm') collate "C", (x ->> 'col') collate "C"), '')), 'UTF8')), 'hex')
    into v_hash from pg_catalog.jsonb_array_elements(p -> 'entries') x;

  select * into m from ops.master_ne_values_mark k where k.id = 1;
  if found then
    if m.compare_run_id = v_run then
      if m.content_hash = v_hash then return pg_catalog.jsonb_build_object('state', 'unchanged', 'rows', v_n); end if;
      raise exception 'run_conflict: 同じ回 % の中身が違う', v_run using errcode = '23505';
    end if;
    if (v_at, v_run) <= (m.observed_at, m.compare_run_id) then
      raise exception 'stale_run: % は今の印の回 % より新しくない', v_run, m.compare_run_id using errcode = '22023';
    end if;
    -- 取得は戻らない (単品・セットのそれぞれ)。同じ取得の始めで通し番号が違う = 別の取得 = 断る
    if (fp ->> 'started_at')::timestamptz < m.products_started_at or (fs ->> 'started_at')::timestamptz < m.sets_started_at
       or (fp ->> 'complete_rev')::bigint < m.products_rev or (fs ->> 'complete_rev')::bigint < m.sets_rev then
      raise exception 'stale_fetch: 取得 (単品 % / セット %) が今の印の取得 (単品 % / セット %) より古い', fp ->> 'started_at', fs ->> 'started_at', m.products_started_at, m.sets_started_at using errcode = '22023';
    end if;
    if ((fp ->> 'started_at')::timestamptz = m.products_started_at and (fp ->> 'complete_rev')::bigint <> m.products_rev)
       or ((fs ->> 'started_at')::timestamptz = m.sets_started_at and (fs ->> 'complete_rev')::bigint <> m.sets_rev) then
      raise exception 'fetch_conflict: 同じ取得の始めで通し番号が違う' using errcode = '23505';
    end if;
  end if;
  select k.compare_run_id into v_code_run from ops.master_ne_code_mark k where k.id = 1;

  perform pg_catalog.set_config('ops.ne_values_protocol', '1', true);
  delete from ops.master_ne_values;
  insert into ops.master_ne_values (code_norm, col, ne_kind, state, reason, value)
    select x ->> 'code_norm', x ->> 'col', x ->> 'ne_kind', x ->> 'state', x ->> 'reason', case when x ->> 'state' = 'ok' then x -> 'value' end
      from pg_catalog.jsonb_array_elements(p -> 'entries') x;
  select pg_catalog.jsonb_build_object('rows', v_n, 'single', v_single, 'set', v_set,
           'ok', pg_catalog.count(*) filter (where v.state = 'ok'), 'unknown', pg_catalog.count(*) filter (where v.state = 'unknown'), 'invalid', pg_catalog.count(*) filter (where v.state = 'invalid'))
    into v_counts from ops.master_ne_values v;
  insert into ops.master_ne_values_runs (compare_run_id, observed_at, norm_version, fetch_fingerprint, products_started_at, products_finished_at, products_rev,
      sets_started_at, sets_finished_at, sets_rev, fetch_counts, clean, content_hash, counts, code_mark_run)
    values (v_run, v_at, v_nv, v_fp, (fp ->> 'started_at')::timestamptz, (fp ->> 'finished_at')::timestamptz, (fp ->> 'complete_rev')::bigint,
      (fs ->> 'started_at')::timestamptz, (fs ->> 'finished_at')::timestamptz, (fs ->> 'complete_rev')::bigint,
      pg_catalog.jsonb_build_object('products', fp - 'clean', 'sets', fs - 'clean'), pg_catalog.jsonb_build_object('products', fp -> 'clean', 'sets', fs -> 'clean'),
      v_hash, v_counts, v_code_run);
  delete from ops.master_ne_values_mark;
  insert into ops.master_ne_values_mark select 1, r.* from ops.master_ne_values_runs r where r.compare_run_id = v_run;
  -- (PR-2: ここで配った NE 更新の CSV の行を、この回の値で確かめる = 同じ取引・同じ鍵の下)
  perform pg_catalog.set_config('ops.ne_values_protocol', '', true);
  return pg_catalog.jsonb_build_object('state', 'written', 'rows', v_n, 'counts', v_counts, 'code_mark_same_run', v_code_run is not distinct from v_run,
    'clean', pg_catalog.jsonb_build_object('products', fp -> 'clean', 'sets', fs -> 'clean'));
end $$;
revoke all on function ops.record_ne_values(jsonb) from public;

-- ═══════════ 3. ロジザードの書き方 ═══════════
create table ops.master_lz_codes (
  code_norm  text primary key check (pg_catalog.length(code_norm) between 1 and 200),
  state      text not null check (state in ('one', 'many', 'invalid')),   -- one = 書き方が 1 つで NE のコードに使える形 / many = 2 つ以上 / invalid = 1 つだが使えない形
  lz_code    text,   -- one のときだけ
  spellings  jsonb not null check (pg_catalog.jsonb_typeof(spellings) = 'array' and pg_catalog.jsonb_array_length(spellings) >= 1),
  constraint ck_mlc_state check ((state = 'one') = (lz_code is not null)),
  constraint ck_mlc_code check (lz_code is null or (lz_code ~ '^[A-Za-z0-9_-]{1,30}$' and pg_catalog.lower(lz_code) = code_norm)),
  constraint ck_mlc_many check ((state = 'many') = (pg_catalog.jsonb_array_length(spellings) >= 2))
);
comment on table ops.master_lz_codes is '🆕 0083 (設計 25 PR-1): ロジザードの書き方 (毎朝の lz-daily がロジザードの全件の一覧から送った商品 ID を DB が core.norm_code で組み分け)。書くのは ops.record_lz_codes だけ。削除フラグの付いた ID も入る';

create table ops.master_lz_codes_runs (
  lz_daily_run_id text primary key check (lz_daily_run_id ~ '^lzd_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$'),
  exported_at     timestamptz not null unique,   -- ロジザードの全件の書き出しを保存した時刻 (auto-shohin-csv.js の保存 = 取り終えた時刻)。「今日の書き方」= 36 時間の中
  stamp_at        timestamptz not null,           -- 成功の印 (shohin-last-success.txt) を書いた時刻
  csv_sha256      text not null check (csv_sha256 ~ '^[0-9a-f]{64}$'),
  csv_rows        integer not null check (csv_rows >= 1),
  content_hash    text not null check (content_hash ~ '^[0-9a-f]{64}$'),
  counts          jsonb not null,
  recorded_at     timestamptz not null default pg_catalog.clock_timestamp(),
  constraint ck_mlcr_stamp check (stamp_at >= exported_at and stamp_at <= exported_at + interval '15 minutes')
);
comment on table ops.master_lz_codes_runs is '🆕 0083: ops.record_lz_codes が受け付けたロジザードの書き出し (追記だけ)';
create table ops.master_lz_codes_mark (
  id smallint primary key default 1 check (id = 1),
  like ops.master_lz_codes_runs including constraints including defaults
);
alter table ops.master_lz_codes_mark add constraint fk_mlcm_run foreign key (lz_daily_run_id) references ops.master_lz_codes_runs (lz_daily_run_id);
comment on table ops.master_lz_codes_mark is '🆕 0083: ops.master_lz_codes の中身はどの書き出しか (1 行)。ops.record_lz_codes だけが進める';

create trigger trg_guard_mlc before insert or update or delete or truncate on ops.master_lz_codes for each statement execute function ops.guard_master_ne_lz('ops.lz_codes_protocol');
create trigger trg_guard_mlcm before insert or update or delete or truncate on ops.master_lz_codes_mark for each statement execute function ops.guard_master_ne_lz('ops.lz_codes_protocol');
create trigger trg_guard_mlcr before insert or update or delete or truncate on ops.master_lz_codes_runs for each statement execute function ops.guard_master_ne_lz('ops.lz_codes_protocol');
select core.make_append_only('ops', 'master_lz_codes_runs');

/**
 * ロジザードの書き方を入れ替える (毎朝の lz-daily・watch_writer・1 回 = 1 つの取引)。
 * p = { lz_daily_run_id, export: { exported_at, stamp_at, stamp_date, csv_sha256, rows }, ids: [ロジザードの商品 ID (全件・削除フラグの付いたものも)] }
 *   exported_at / stamp_at = ISO の UTC (ミリ秒まで)・stamp_date = 成功の印の中身 (JST の YYYY-MM-DD)・rows = 一覧の行数 (= ids の数)
 * 書き方の組み分け (one / many / invalid) は DB が core.norm_code で決める (送られた組み分けを信じない)
 */
create function ops.record_lz_codes(p jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_run   text := p ->> 'lz_daily_run_id';
  e       jsonb := p -> 'export';
  v_exp   timestamptz;
  v_stamp timestamptz;
  v_now   timestamptz := pg_catalog.clock_timestamp();
  v_rows  integer;
  v_n     integer;
  v_bad   integer;
  v_dup   integer;
  v_hash  text;
  v_counts jsonb;
  m       ops.master_lz_codes_mark%rowtype;
begin
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.lz_codes'));
  if pg_catalog.jsonb_typeof(p) is distinct from 'object' or pg_catalog.jsonb_typeof(e) is distinct from 'object' then raise exception 'invalid_input: 入力 / export が object でない' using errcode = '22023'; end if;
  if v_run is null or v_run !~ '^lzd_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'invalid_input: lz_daily_run_id の形が違う: %', v_run using errcode = '22023'; end if;
  if coalesce(e ->> 'exported_at', '') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,3})?Z$'
     or coalesce(e ->> 'stamp_at', '') !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,3})?Z$' then
    raise exception 'invalid_input: export.exported_at / stamp_at は ISO の UTC の時刻' using errcode = '22023';
  end if;
  begin v_exp := (e ->> 'exported_at')::timestamptz; v_stamp := (e ->> 'stamp_at')::timestamptz;
  exception when others then raise exception 'invalid_input: export の時刻を読めない' using errcode = '22023'; end;
  if coalesce(e ->> 'csv_sha256', '') !~ '^[0-9a-f]{64}$' then raise exception 'invalid_input: export.csv_sha256 の形が違う' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(e -> 'rows') is distinct from 'number' or (e ->> 'rows') !~ '^[0-9]{1,9}$' then raise exception 'invalid_input: export.rows は 0 以上の整数' using errcode = '22023'; end if;
  v_rows := (e ->> 'rows')::integer;
  -- 新しさ (設計 25 R2 H5): 未来は 5 分まで・36 時間より前は断る (昨日の書き出しを読み直して今日の書き方にしない)
  if v_exp > v_now + interval '5 minutes' then raise exception 'future_export: ロジザードの書き出し % が未来', v_exp using errcode = '22023'; end if;
  if v_exp < v_now - interval '36 hours' then raise exception 'stale_export: ロジザードの書き出し % が 36 時間より前', v_exp using errcode = '22023'; end if;
  -- 成功の印 = 書き出しの後 15 分以内・中身 = 書き出しの JST の日付 (lz-daily の確かめと同じ = その回が保存 → 転送 → 印まで終わった)
  if v_stamp < v_exp or v_stamp > v_exp + interval '15 minutes' then raise exception 'export_not_confirmed: 成功の印が書き出しの後 15 分の中に無い' using errcode = '22023'; end if;
  if (e ->> 'stamp_date') is distinct from pg_catalog.to_char(core.jst_date(v_exp), 'YYYY-MM-DD') then
    raise exception 'export_not_confirmed: 成功の印の日付 % が書き出しの JST の日付と違う', e ->> 'stamp_date' using errcode = '22023';
  end if;
  if pg_catalog.jsonb_typeof(p -> 'ids') is distinct from 'array' then raise exception 'invalid_input: ids が配列でない' using errcode = '22023'; end if;
  v_n := pg_catalog.jsonb_array_length(p -> 'ids');
  if v_n = 0 or v_rows = 0 then raise exception 'empty: ロジザードの商品 ID が 0 (空の一覧は受け付けない)' using errcode = '22023'; end if;
  if v_n <> v_rows then raise exception 'rows_mismatch: 商品 ID の数 % ≠ 一覧の行数 %', v_n, v_rows using errcode = '22023'; end if;
  select pg_catalog.count(*) filter (where pg_catalog.jsonb_typeof(x) <> 'string' or pg_catalog.btrim(x #>> '{}') = '' or pg_catalog.length(x #>> '{}') > 200
           or coalesce(core.norm_code(x #>> '{}'), '') = '' or pg_catalog.length(core.norm_code(x #>> '{}')) > 200),
         pg_catalog.count(distinct x #>> '{}')
    into v_bad, v_dup from pg_catalog.jsonb_array_elements(p -> 'ids') x;
  if v_bad > 0 then raise exception 'invalid_input: 使えない商品 ID が % 件 (文字でない・空・200 字超)', v_bad using errcode = '22023'; end if;
  if v_dup <> v_n then raise exception 'invalid_input: 同じ商品 ID が % 件 (一覧は ID ごとに 1 行)', v_n - v_dup using errcode = '22023'; end if;
  select pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to('lzc1' || E'\n' || pg_catalog.string_agg(x #>> '{}', E'\n' order by (x #>> '{}') collate "C"), 'UTF8')), 'hex')
    into v_hash from pg_catalog.jsonb_array_elements(p -> 'ids') x;

  select * into m from ops.master_lz_codes_mark k where k.id = 1;
  if found then
    if m.exported_at = v_exp then
      if m.csv_sha256 = (e ->> 'csv_sha256') and m.content_hash = v_hash then return pg_catalog.jsonb_build_object('state', 'unchanged', 'ids', v_n, 'lz_daily_run_id', m.lz_daily_run_id); end if;
      raise exception 'run_conflict: 同じ書き出し % で中身が違う', v_exp using errcode = '23505';
    end if;
    if v_exp < m.exported_at then raise exception 'stale_export: 書き出し % は今の印の書き出し % より前', v_exp, m.exported_at using errcode = '22023'; end if;
  end if;
  if exists (select 1 from ops.master_lz_codes_runs r where r.lz_daily_run_id = v_run) then
    raise exception 'run_conflict: lz-daily の回 % はもう別の書き出しを送った', v_run using errcode = '23505';
  end if;

  perform pg_catalog.set_config('ops.lz_codes_protocol', '1', true);
  delete from ops.master_lz_codes;
  insert into ops.master_lz_codes (code_norm, state, lz_code, spellings)
    select g.code_norm,
           case when g.n >= 2 then 'many' when g.one ~ '^[A-Za-z0-9_-]{1,30}$' and pg_catalog.lower(g.one) = g.code_norm then 'one' else 'invalid' end,
           case when g.n = 1 and g.one ~ '^[A-Za-z0-9_-]{1,30}$' and pg_catalog.lower(g.one) = g.code_norm then g.one end,
           g.spellings
      from (select core.norm_code(x #>> '{}') as code_norm, pg_catalog.count(*) as n, pg_catalog.min(x #>> '{}') as one,
                   pg_catalog.jsonb_agg(x #>> '{}' order by (x #>> '{}') collate "C") as spellings
              from pg_catalog.jsonb_array_elements(p -> 'ids') x group by 1) g;
  select pg_catalog.jsonb_build_object('ids', v_n, 'codes', pg_catalog.count(*), 'one', pg_catalog.count(*) filter (where c.state = 'one'),
           'many', pg_catalog.count(*) filter (where c.state = 'many'), 'invalid', pg_catalog.count(*) filter (where c.state = 'invalid'))
    into v_counts from ops.master_lz_codes c;
  insert into ops.master_lz_codes_runs (lz_daily_run_id, exported_at, stamp_at, csv_sha256, csv_rows, content_hash, counts)
    values (v_run, v_exp, v_stamp, e ->> 'csv_sha256', v_rows, v_hash, v_counts);
  delete from ops.master_lz_codes_mark;
  insert into ops.master_lz_codes_mark select 1, r.* from ops.master_lz_codes_runs r where r.lz_daily_run_id = v_run;
  perform pg_catalog.set_config('ops.lz_codes_protocol', '', true);
  return pg_catalog.jsonb_build_object('state', 'written', 'counts', v_counts);
end $$;
revoke all on function ops.record_lz_codes(jsonb) from public;

-- ═══════════ 4. 権限 (0041 / 0068 と同じ形)。ロールを後から作る DB は scripts/company-db/create-watch-roles.mjs が同じ grant を付ける ═══════════
--   watch_writer = 書く関数 2 つの実行だけ (表は読めない・書けない) / watcher = 表を読むだけ (関数は実行できない) / master_edit (画面) = 何も渡さない
do $$ begin
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'watch_writer') then
    execute 'grant usage on schema ops to watch_writer';
    execute 'grant execute on function ops.record_ne_values(jsonb) to watch_writer';
    execute 'grant execute on function ops.record_lz_codes(jsonb) to watch_writer';
  end if;
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.master_ne_values, ops.master_ne_values_mark, ops.master_ne_values_runs, ops.master_lz_codes, ops.master_lz_codes_mark, ops.master_lz_codes_runs to watcher';
  end if;
end $$;
