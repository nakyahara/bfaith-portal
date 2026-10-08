-- 0064: 新しい商品コードに大文字も使える (2026-10-08 夜 中原さんの答え 9 = b。Company DB構想 20 §③「コードの決まり」・§⑩ PR-1)
-- 🚨 前提 = 0058 (ops.new_sku_code_problem・ops.ne_reg_build の今の版)・0053 (ops.ne_reg_export_items)。どちらも本番に適用済み。番号はマージの順で振り直す (中身は番号に依らない)
--
-- なぜ:
--   NE の「選択肢つき商品の登録」は子のコード = 代表 + 選択肢番号 (例 hakama-WH-90) で、大文字を使う。NE のコードは大文字・小文字を区別する (0041)。
--   今の新しいコードの決まりは小文字だけ (中原さん 2026-10-01「新規は大文字を禁止」) = 10/8 夜の答えで「大文字も使える」に変えた。
-- 決まり (中の鍵 = norm・外へ = 原文):
--   形 = ^[A-Za-z0-9_-]{1,30}$・set- で始めない (大文字小文字を問わず)・前後の空白なし (形の正規表現が空白を通さない)。
--   重なりの確かめ (Company DB の SKU・product の display_code・NE で見たコード・消した SKU のコード) は今までどおり norm (core.norm_code = 小文字) で見る。
--   CSV・NE・カード・ロジザードには打ったとおり (core.skus.code = 原文) を書く。新しいコードの鍵 (core.new_code:<norm>) も norm のまま
-- なにを:
--   1. ops.ne_reg_export_items.ne_code の CHECK (0053 = 小文字だけ) を「^[A-Za-z0-9_-]{1,30}$ かつ 小文字にすると code_norm」に広げる (0041 が ne_csv_export_rows に入れた形と同じ)。
--      前からの行は小文字だけ = ne_code = code = code_norm (形の確かめの後に作った品目) = 新しい CHECK を満たす (満たさない行があれば先に止める)
--   2. ops.new_sku_code_problem (0058 の版) を create or replace: 形の行だけを変える (scripts/test-master-register.mjs の [G-0064] が 0058 の本文との差を機械で確かめる)
--   3. ops.ne_reg_build (0058 の版) を create or replace: コードの形の確かめの行だけを変える (同じ試験)
-- 変えないもの: 登録の関数 ops.register_new_sku (形は ops.new_sku_code_problem に任せている)・NE の取得 (コードは小文字で保存・元の書き方は 0041 の ops.master_ne_codes)・
--   照合 ② の確かめ (code_norm で結ぶ)・ロジザード用 CSV / 入荷予定 (0041 の元の書き方で書く = NE に入る前は出さない)
-- 🚨 security definer の関数は search_path = pg_catalog, pg_temp・名前は全部 schema つき・一時の表を使わない (0058 の本文のまま)。
--    create or replace は今の権限を残す (画面のロール master_edit の実行権は scripts/company-db/create-master-edit-roles.mjs のまま = ロールの script は流し直さなくてよい)
-- 🚨 この migration は商品・SKU・登録・CSV の値を何も変えない (変わるのは次の新商品の登録から)

-- ─── 1. NE 登録の CSV の品目のコード (原文) の CHECK を広げる ───
do $$
declare
  v_bad bigint;
begin
  select pg_catalog.count(*) into v_bad from ops.ne_reg_export_items i
   where not (i.ne_code ~ '^[A-Za-z0-9_-]{1,30}$' and pg_catalog.lower(i.ne_code) = i.code_norm);
  if v_bad > 0 then
    raise exception '0064: NE 登録の CSV の品目で、コード (ne_code) を小文字にすると code_norm と合わない行が % 行ある = CHECK を広げる前に調べる', v_bad;
  end if;
end $$;
alter table ops.ne_reg_export_items drop constraint ne_reg_export_items_ne_code_check;
alter table ops.ne_reg_export_items add constraint ck_nri_ne_code check (ne_code ~ '^[A-Za-z0-9_-]{1,30}$' and lower(ne_code) = code_norm);

-- ─── 2. 新しいコードの決まり (0058 の版の置き換え・形の行だけ) ───
create or replace function ops.new_sku_code_problem(p_code text) returns text
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_norm text;
begin
  -- 🆕 0064 (中原さん 10/8 夜): 大文字も使える (形 = ^[A-Za-z0-9_-]{1,30}$)・set- は大文字小文字を問わず断る。重なりの確かめは下のとおり norm (小文字) のまま
  if p_code is null or p_code !~ '^[A-Za-z0-9_-]{1,30}$' or p_code ~* '^set-' then return 'code_shape'; end if;
  v_norm := core.norm_code(p_code);
  if exists (select 1 from core.skus where company_id = 1 and code_norm = v_norm) then return 'code_taken'; end if;
  if exists (select 1 from core.products where company_id = 1 and core.norm_code(display_code) = v_norm) then return 'code_is_rep'; end if;
  if ops.ne_code_seen(v_norm) then return 'code_in_ne'; end if;   -- 🆕 0058: 今の NE のコードと、前に NE で見たコード (今朝の取得で欠けても)・kind を限らない
  if exists (select 1 from events.master_change_events
              where entity_type = 'sku' and operation = 'DELETE' and core.norm_code(old_value ->> 'code') = v_norm) then return 'code_used_before'; end if;
  return null;
end $$;
revoke all on function ops.new_sku_code_problem(text) from public;

-- ─── 3. NE 登録の CSV を作る (0058 の版の置き換え・コードの形の行だけ) ───
create or replace function ops.ne_reg_build(p jsonb, p_bytes bytea) returns jsonb
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
  v_result   jsonb;
  c          jsonb;
begin
  if v_actor = '' or pg_catalog.length(v_actor) > 320 then raise exception 'invalid_input: 作る人 (actor) が要る' using errcode = '22023'; end if;
  begin v_rid := (p ->> 'request_id')::uuid; exception when others then raise exception 'invalid_input: request_id の形が違う' using errcode = '22023'; end;
  if v_rid is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(v_actor, p ->> 'reason') is not null then raise exception 'invalid_input: 作る人・理由の形が違う' using errcode = '22023'; end if;
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
  -- 🆕 0058 §3.10 (PR-2 Codex R4): 鍵の順 = request の鍵 → 許可の共有の鍵 → 段階の共有の鍵 → マスタの書き込みの共有の鍵 → SKU → CSV (アプリと同じ・直接呼んでも同じ)
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.ne_reg_request:' || v_rid::text, 0));
  perform ops._new_entry_lease_shared_locks();
  perform ops.reg_write_gate(p -> 'ownership');   -- 段階の共有の鍵 → マスタの書き込みの共有の鍵 → 段階・持ち主表
  -- 同じ request_id = 同じファイル (種類・商品・作る人が違えば拒む)
  select e.export_id, e.kind, e.created_by, e.state, e.sha256, e.trial into v_prev from ops.ne_reg_exports e where e.request_id = v_rid;
  if found then
    select pg_catalog.array_agg(distinct (i ->> 'sku_id')::bigint order by (i ->> 'sku_id')::bigint) into v_want from pg_catalog.jsonb_array_elements(p -> 'items') i;
    if v_prev.kind is distinct from v_kind or v_prev.created_by is distinct from v_actor
       or v_want is distinct from (select pg_catalog.array_agg(x.sku_id order by x.sku_id) from ops.ne_reg_export_items x where x.export_id = v_prev.export_id) then
      raise exception 'request_id_reused: 同じ番号 (request_id) で違う中身' using errcode = '23505';
    end if;
    return pg_catalog.jsonb_build_object('export_id', v_prev.export_id, 'state', v_prev.state, 'sha256', v_prev.sha256, 'trial', v_prev.trial, 'replayed', true);
  end if;
  -- 🆕 0058 (v13 §3.8): 同じ request_id・同じ中身の replay (上) は許可が要らない。新しい export を作る前だけ許可を確かめる
  -- 単品の CSV = 単品の許可・セットの CSV = セットの許可 (今は出す道が無い = DB の境界でも閉じたまま・#1644 Codex R1 Medium 2)
  perform ops._require_new_entry_lease(case when v_kind = 'products' then 'single' else 'set' end);
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
    -- 🆕 0064: 大文字も (CSV には打ったとおりの書き方を書く・品目の ne_code の CHECK も同じ形)
    if v_sku.code !~ '^[A-Za-z0-9_-]{1,30}$' then raise exception 'not_ready: % は新しいコードの形でない', v_sku.code using errcode = 'P0001'; end if;
    select r.state into v_reg from ops.master_registrations r where r.sku_id = v_sku.sku_id for share;
    if v_reg is null or v_reg not in ('draft', 'ne_pending') then
      raise exception 'not_ready: % の登録の状態が % (下書き・NE 登録待ちだけ)', v_sku.code, coalesce(v_reg, 'なし') using errcode = 'P0001';
    end if;
    if exists (select 1 from ops.ne_reg_export_items x where x.sku_id = v_sku.sku_id and x.state in ('built', 'issued', 'import_declared', 'partial')) then
      raise exception 'not_ready: % にはまだ終わっていないファイルがある', v_sku.code using errcode = 'P0001';
    end if;
    if ops.ne_code_seen(v_sku.code_norm) then   -- 🆕 0058: 今の NE のコードと前に NE で見たコード・商品のコードも代表のコードも (登録の時と同じ範囲・#1640 R5)
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
  -- 約束 (reg_csv_build・相手 = これから作るファイルの番号と商品・構成品の SKU = 関数が決めた)。番号を先に振る = 約束に書ける (⑤-2a の登録と同じ)
  v_export := pg_catalog.nextval(pg_catalog.pg_get_serial_sequence('ops.ne_reg_exports', 'export_id')::regclass);
  perform ops.open_reg_write('reg_csv_build', v_rid, v_actor, p ->> 'reason', p -> 'ownership', null, null, null, v_payload,
    pg_catalog.jsonb_build_object('export_id', v_export::text, 'sku_ids', pg_catalog.to_jsonb(v_ids), 'kind', v_kind));
  insert into ops.ne_reg_exports (export_id, kind, schema_version, header, encoding, trial, item_count, row_count, aggregate_token, payload_hash, sha256, file_bytes,
                                  request_id, ne_codes_run, cost_day, created_by)
    overriding system value
    values (v_export, v_kind, v_schema, v_header, 'utf8', v_trial, v_nitems, v_nrows, v_agg, v_payload, v_sha, v_bytes, v_rid, v_mark, v_day, v_actor);
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
  v_result := pg_catalog.jsonb_build_object('export_id', v_export::text, 'state', 'built', 'sha256', v_sha, 'trial', v_trial, 'rows', v_nrows, 'items', v_nitems, 'replayed', false,
    'payload_hash', v_payload, 'aggregate_token', v_agg);
  perform ops.close_reg_write(v_result, 'reg-csv #' || v_export);
  return v_result;
end $$;
revoke all on function ops.ne_reg_build(jsonb, bytea) from public;
