-- 0097 (0096 = NE へ送るの 1 ファイル (#1762) の次。同じころの NE 更新の CSV (fix/ne-update-one-file) と番号はマージの順に付け替える。migrate は欠番を断る): 新商品の NE 登録の CSV を「単品 1 つのファイル」にまとめる =
--   ふつうの単品と、色違い・サイズ違いのまとまり (ポータルで作ったまとまり) の子を同じ単品のファイル (ne-reg-single-v2) に入れられるようにする
--   (2026-10-20 中原さん「まとまりごとでアップしないといけないから面倒。ボタンをワンクリックしたら CSV が作成できるようにしてほしい」)
--
-- なぜ入れてよいか (NE の動き・AI_reference CompanyDB構想/20 v7 §⑤):
--   ・単品の版 (ne-reg-single-v2) とまとまりの版 (ne-reg-variation-v1) は、見出し・列の中身の決まり (ops.ne_reg_canonical) が同じ。代表商品コードの列は
--     どちらの版でも ops.ne_reg_canonical が決める (NE にある代表 = NE の書き方 / ポータルで作ったまとまり = 予約のコード)。版で行は変わらない
--   ・NE の CSV に代表 (親) の行は無い (代表商品コードは子の行の 1 つの列)。NE は、まだ知らない代表商品コードの子も CSV で受けた
--     (本番 10/19 のまとまりのファイル #9〜#14 = 翌朝の照合で代表まで 3 者一致)。= 行の順 (親を先に) の決まりは要らない。アプリはまとまりの子を並べて書く
--   ・「まとまりで 1 ファイル」(0067 の variation_incomplete・中原さんの決定 10/11) の中身 = まとまりの NE 登録待ちの子を全部同じファイルに入れる、は残す
--     (単品の版でも、ポータルで作ったまとまりの子を入れたら、そのまとまりの NE 登録待ちの子を全部入れる = 下で確かめる)。1 つのファイルに 2 つ以上のまとまり・ふつうの単品を入れてよい
-- なにを (前の最後の版を写して 🆕 0097 の行だけ変えた・鍵・許可・形の版・印・ハッシュ・約束・権限・security definer・search_path は同じ):
--   1. ops.reg_csv_one_file() = 印 (アプリが to_regprocedure で「0097 の後の DB か」を見る。前の DB = アプリはまとまりを今までどおりまとまりの版のファイルに分ける)
--   2. ops.ne_reg_build (0067 の版): 単品の版の variation_file_required の断りを外し、入れたポータルのまとまりごとに variation_incomplete を確かめる。
--      まとまりの版 (variation-v1) の決まり (まとまりの子だけ・1 つのまとまり・全部の子) は変えない (前に作ったファイルの再送・API はそのまま)
-- 変えない: 形の版の決まり (ops.ne_reg_schema_rule)・行と確かめる値 (ops.ne_reg_canonical)・翌朝の確かめ (3 者一致)・配る・申告・使わない・代表 (親) の門 (0095 の trigger)・
--   「まだ終わっていないファイルがある」・NE にもうあるコード・1 ファイル 1,000 行
-- 🚨 この migration は値を何も変えない (関数を作り直すだけ)。create or replace = 持ち主・master_edit の実行権・public から外した実行権はそのまま
--   (create-master-edit-roles.mjs の流し直しは要らない)

-- ═══════════ 1. 印 ═══════════
create or replace function ops.reg_csv_one_file() returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$ select true $$;
comment on function ops.reg_csv_one_file() is '0097: 新商品の NE 登録の CSV の単品のファイルに、ポータルで作ったまとまりの子も入れられる DB の印 (アプリが to_regprocedure で見る)';
revoke all on function ops.reg_csv_one_file() from public;

-- ═══════════ 2. NE 登録の CSV を作る (0067 の版から・引数は同じ) ═══════════
/**
 * 0067 と同じ (鍵の順・許可・形の版の決まり・NE のコード・大文字のコードの確かめ・行と確かめる値の照らし直し・印とハッシュ・約束) で、変えたのは単品の版のまとまりの子だけ:
 *   🆕 0097: 単品の版 (parent = ne_code) にポータルで作ったまとまり (予約 source = portal) の子を入れてよい。入れたまとまりごとに、そのまとまりの NE 登録待ちの子
 *     (下書き / NE 登録待ちで生きているファイルの無い単品) を全部入れる (variation_incomplete = まとまりで 1 ファイル・0067 と同じ文)。
 *     まとまりの版 (parent = group = ne-reg-variation-v1) は 0067 のまま (全部がまとまりの子・1 つのまとまり・全部の子)
 */
create or replace function ops.ne_reg_build(p jsonb, p_bytes bytea) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_rid      uuid;
  v_actor    text := pg_catalog.btrim(coalesce(p ->> 'actor', ''));
  v_kind     text := p ->> 'kind';
  v_schema   text := p ->> 'schema_version';
  v_header   text := p ->> 'header';
  v_rule     jsonb;
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
  v_vpar     bigint;   -- 🆕 0067: 品目の親
  v_group    bigint;   -- 🆕 0067: まとまりの版のまとまり
  v_left     text;     -- 🆕 0067: まとまりの NE 登録待ちの子でファイルに入っていないもの
  v_groups   bigint[] := '{}';   -- 🆕 0097: 確かめるまとまり (単品の版に入れたポータルのまとまり + まとまりの版のまとまり)
  v_g        bigint;   -- 🆕 0097
begin
  if v_actor = '' or pg_catalog.length(v_actor) > 320 then raise exception 'invalid_input: 作る人 (actor) が要る' using errcode = '22023'; end if;
  begin v_rid := (p ->> 'request_id')::uuid; exception when others then raise exception 'invalid_input: request_id の形が違う' using errcode = '22023'; end;
  if v_rid is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(v_actor, p ->> 'reason') is not null then raise exception 'invalid_input: 作る人・理由の形が違う' using errcode = '22023'; end if;
  -- 🆕 0065: 形の版の決まりは ops.ne_reg_schema_rule の 1 か所 (種類・見出し・作れる版か・試し用の門)
  v_rule := ops.ne_reg_schema_rule(v_schema);
  if v_rule is null or (v_rule ->> 'kind') is distinct from v_kind or (v_rule ->> 'header') is distinct from v_header then
    raise exception 'invalid_input: 種類・形の版・見出しが合わない (% / % / %)', v_kind, v_schema, v_header using errcode = '22023';
  end if;
  if (v_rule -> 'buildable') is distinct from 'true'::jsonb then
    raise exception 'schema_not_buildable: 形の版 % では新しいファイルを作らない (%)', v_schema, v_rule ->> 'why' using errcode = 'P0001';
  end if;
  v_cols := pg_catalog.string_to_array(v_header, ',');
  if v_cols && array['zaiko_su', 'yoyaku_zaiko_su', 'nyusyukko_riyu', 'visible_flg'] then
    raise exception 'invalid_input: 在庫の列は出さない (%)', v_header using errcode = '22023';
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
  select e.export_id, e.kind, e.schema_version, e.header, e.created_by, e.state, e.sha256, e.trial into v_prev from ops.ne_reg_exports e where e.request_id = v_rid;
  if found then
    select pg_catalog.array_agg(distinct (i ->> 'sku_id')::bigint order by (i ->> 'sku_id')::bigint) into v_want from pg_catalog.jsonb_array_elements(p -> 'items') i;
    -- 🆕 0067 (#1677 Codex R1 Medium 1): 形の版と見出しも同じときだけ前のファイル (single-v2 と variation-v1 はどちらも products)
    if v_prev.kind is distinct from v_kind or v_prev.schema_version is distinct from v_schema or v_prev.header is distinct from v_header or v_prev.created_by is distinct from v_actor
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
    -- 🆕 0067: 代表の列の版の決まり (まとまりの版 = 全部が同じまとまりの子 / 🆕 0097: 単品の版 = ポータルで作ったまとまりの子も入れてよい・そのまとまりの子は全部)
    select pp.parent_product_id into v_vpar from core.products pp where pp.product_id = v_sku.product_id;
    if (v_rule ->> 'parent') = 'group' then
      if v_vpar is null then raise exception 'not_ready: % はまとまりの子でない (形の版 % はまとまりの子だけ)', v_sku.code, v_schema using errcode = 'P0001'; end if;
      if v_group is null then v_group := v_vpar;
      elsif v_group <> v_vpar then raise exception 'variation_mixed: 1 つのファイルは 1 つのまとまりの子だけ (%)', v_sku.code using errcode = 'P0001'; end if;
    elsif v_vpar is not null and exists (select 1 from ops.variation_group_codes g where g.group_product_id = v_vpar and g.source = 'portal') then
      if not (v_vpar = any (v_groups)) then v_groups := v_groups || v_vpar; end if;   -- 🆕 0097: 下でそのまとまりの NE 登録待ちの子が全部入っているかを確かめる
    end if;
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
  -- 🆕 0067: まとまりの版 = そのまとまりの NE 登録待ちの子 (下書き / NE 登録待ち・生きているファイルなし) を全部入れる (まとまりで 1 ファイル)
  --   🆕 0097: 単品の版に入れたポータルのまとまりも、まとまりごとに同じ確かめ (1 つのファイルに 2 つ以上のまとまり・ふつうの単品があってよい)
  if v_group is not null then v_groups := v_groups || v_group; end if;
  foreach v_g in array v_groups loop
    select pg_catalog.string_agg(k.code, '・' order by k.code_norm) into v_left
      from core.products cp join core.skus k on k.product_id = cp.product_id and k.sku_kind = 'single' join ops.master_registrations mr on mr.sku_id = k.sku_id
     where cp.parent_product_id = v_g and mr.state in ('draft', 'ne_pending')
       and not exists (select 1 from ops.ne_reg_export_items x where x.sku_id = k.sku_id and x.state in ('built', 'issued', 'import_declared', 'partial'))
       and not (k.sku_id = any (v_ids));
    if v_left is not null then
      raise exception 'variation_incomplete: まとまりの NE 登録待ちの子 (%) もこのファイルに入れる (まとまりで 1 ファイル)', v_left using errcode = 'P0001';
    end if;
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
  -- 実機の確かめの門 (種類 × 形の版 × 見出し・最後の記録が ok)。確かめていない = 試し用 (5 行まで)。🆕 0065: 門の無い版 (single-v2・variation-v1) は試し用にしない
  select coalesce((select v.result = 'ok' from ops.ne_csv_verified v
                    where v.kind = v_kind and v.col = 'new_registration' and v.encoding = 'utf8' and v.header = v_header and v.converter_version = v_schema
                    order by v.verified_id desc limit 1), false) into v_verified;
  v_trial := (v_rule -> 'trial_gate') = 'true'::jsonb and not v_verified;
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
