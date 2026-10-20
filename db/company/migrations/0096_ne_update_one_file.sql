-- 0096: NE へ送る (0094) の CSV を、単品 1 つ・セット 1 つにまとめる (2026-10-20 中原さん「CSVの数が多すぎない？1つにまとめてよ。
--   セット商品と単品は2のCSVに分ける必要があるけど、それ以外は共通でアップできるはずだよ」・10/20 の本番 = 123 欄が 5 つのファイル)
--
-- なぜ分かれていたか: 0094 は「空欄のセル = NE は変えない」を NE の実物で確かめるまで空欄を作らない = その SKU が変えた欄の組ごとにファイルを分けた。
-- どうまとめるか: ファイルの見出し = コードの列 + その種類で変える欄の全部 (列の表の順)。各行は全部の欄を埋める:
--   - 変えた欄 = Company DB の値 (今までどおり・ops.ne_upd_rows に行が残る)
--   - 変えていない欄 (埋めるセル) =
--       (a) 空欄 = NE の実物で「空欄 = 変えない」を確かめた欄 (ops.ne_upd_verified の入口ごとの最後が ok) だけ
--       (b) 今朝の NE の値 (ops.master_ne_values・ops.ne_upd_cell と同じセルの書き方) = NE には今ある値をもう一度書く = 変わらない
--       (c) 今朝の取得の後にこの画面の CSV でその欄を出した (送っている途中) = 出した値 (= 自社の今の値) をもう一度 = 前の CSV を朝の値に戻さない
--     埋められない = 今朝の NE の値が無い・不明・CSV に書けない (空・0・名前 = コード など)・取扱が「取扱中止」(NE の 取扱中止 / ﾒｰｶｰ取扱中止 の
--       どちらかが分からない = 1 を書くと ﾒｰｶｰ取扱中止 (2) を変えてしまう)・今朝の後に別の値を出した・マスタの判断の CSV (0040) や取扱中止の CSV (0072) が
--       この 2 日にその欄を NE に書いたかもしれない
--   - 埋められない行は黙って落とさない: その行 (SKU) だけを 2 つ目のファイルへ (見出し = その行たちの変える欄)。そこでも埋められない行は、最後の手として
--     その SKU が変えた欄だけのファイル (埋めるセルなし = 0094 と同じ形) にする。= どの欄も送れる・NE の値を推し量って書くことはしない
--   - 埋めたセルはファイルに残す (ops.ne_upd_files.fill)・受け取り (ops.ne_upd_download) の前に、今の状態でもう一度同じ値になるかを DB が確かめる (違えば渡さない)。
--     「出した」は今朝の取得の後か今日 (JST) の分を見る (同じ日にもう一度取得しても、その前に今日出した CSV はまだ取り込んでいないかもしれない・Codex 0096 R1 H2)
--   - 今日もう出したほかの CSV が、受け取る CSV で直す欄を別の値で埋めている = 受け取りの答え (older) で画面が「前の CSV は取り込まない」と知らせる
--     (その CSV を後に取り込むと直しが古い値に戻る。手元のファイルは DB では止められない。戻っても翌朝の照合で差がまた出て、もう一度送る・Codex 0096 R1 H1)
-- 🚨 残る危うさ: 今朝の取得の後に NE の画面 (やポータルの外の CSV) で同じ欄を直していたら、この CSV はその欄を今朝の値に戻す。
--    NE はもう Company DB の下流 (13 キーの NE の画面は止めた・17 §4.2) なので受け入れる。画面に 1 行だけ書く
-- 変えない: 差の一覧 (ops.ne_upd_units)・鍵の順 (ops._ne_upd_begin)・毎朝の上限 (100 欄・5 倍)・1 ファイル 1,000 行・コードの書き方 (行ごとに 1 つ = 書き方でファイルは分かれない)・
--   同じ request_id = 前の答え・権限 (create or replace = 持ち主・実行権はそのまま)
-- なにを: 1. ops.ne_upd_files.fill (埋めたセル・書き換えない) 2. ops.ne_upd_one_file() (印・アプリが「0096 の後か」を見る)
--   3. ops._ne_upd_filler (1 つの埋めるセル) 4. ops._ne_upd_emit (1 つの組のファイルを 1,000 行ずつ作る) 5. ops._ne_upd_fill (0094 の版から: 組 = 種類ごとに 1 つ)
--   6. ops.ne_upd_download (0094 の版から: 埋めたセルの確かめを足した)
-- 🚨 security definer = search_path = pg_catalog, pg_temp・名前は全部 schema つき・一時の表なし・public の実行権なし

-- ═══════════ 0. 前提 ═══════════
do $$ begin
  if pg_catalog.to_regprocedure('ops._ne_upd_fill(bigint, boolean, text, boolean)') is null or pg_catalog.to_regclass('ops.ne_upd_files') is null then
    raise exception '0096 の前提: NE へ送る (0094) が無い';
  end if;
  if pg_catalog.to_regclass('ops.ne_csv_export_rows') is null or pg_catalog.to_regclass('ops.ne_del_items') is null then
    raise exception '0096 の前提: NE に取り込む CSV (0040)・NE から消す CSV (0072) が無い';
  end if;
end $$;

-- ═══════════ 1. 埋めたセル ═══════════
-- [{ sku_id, col, cell (null = 空欄), src (blank = 確かめた空欄 / ne = 今朝の NE の値 / sent = 今朝の後に出した値) }]。0096 の前のファイル = []
--   (書き換えない = ops.guard_ne_upd_write の「状態・結果の列だけ」に入れない)
alter table ops.ne_upd_files add column fill jsonb not null default '[]'::jsonb check (pg_catalog.jsonb_typeof(fill) = 'array');
comment on column ops.ne_upd_files.fill is '🆕 0096: 埋めたセル (その SKU が変えていない欄) = 確かめた空欄 / 今朝の NE の値 / 今朝の後に出した値。受け取りの前に DB がもう一度確かめる';

-- ═══════════ 2. 印 ═══════════
create or replace function ops.ne_upd_one_file() returns boolean language sql immutable set search_path = pg_catalog, pg_temp as $$ select true $$;
comment on function ops.ne_upd_one_file() is '0096: NE へ送る CSV = 単品 1 つ・セット 1 つ (埋めるセル) の DB の印 (アプリが to_regprocedure で見る)';
revoke all on function ops.ne_upd_one_file() from public;

-- ═══════════ 3. 1 つの埋めるセル ═══════════
/**
 * その SKU が変えていない欄のセル。戻り値 = { ok: true, cell (null = 空欄), src } / { ok: false, why }
 *   p_blank = 空欄にしてよい欄 (その種類・入口)・p_since = 今朝のその種類の取得の始め・p_sending = 送っている途中の欄 { "sku_id:col": 自社の今の値 }
 *   why: other_csv (マスタの判断の CSV・取扱中止の CSV がこの 2 日にこの欄を NE に書いたかもしれない) / sent_other (今朝の後に別の値を出した) /
 *        ne_unknown (今朝の NE の値が無い・不明・区分が違う) / ne_handling_stop (NE は取扱中止か ﾒｰｶｰ取扱中止 = どちらか分からない) / ne_<セルの理由>
 */
create function ops._ne_upd_filler(p_kind text, p_sku_id bigint, p_code_norm text, p_col text, p_blank jsonb, p_since timestamptz, p_sending jsonb) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v   ops.master_ne_values;
  v_x jsonb;
  v_c text;
  c   jsonb;
begin
  if coalesce(p_blank, '[]'::jsonb) ? p_col then return '{"ok": true, "cell": null, "src": "blank"}'::jsonb; end if;
  if p_since is null then return '{"ok": false, "why": "ne_unknown"}'::jsonb; end if;
  -- ポータルのほかの CSV がこの欄を NE に書いたかもしれない (今朝の取得より後に取り込んだら、今朝の値は古い) = 埋めない
  if exists (select 1 from ops.ne_csv_export_rows r join ops.ne_csv_exports e on e.export_id = r.export_id
              where r.code_norm = p_code_norm and r.col = p_col and r.child is null and e.kind = p_kind
                and (r.reserved or e.created_at > p_since - interval '2 days')) then
    return '{"ok": false, "why": "other_csv"}'::jsonb;
  end if;
  if p_col = 'handling' and exists (select 1 from ops.ne_del_items i where i.sku_id = p_sku_id and i.created_at > p_since - interval '2 days') then
    return '{"ok": false, "why": "other_csv"}'::jsonb;
  end if;
  select * into v from ops.master_ne_values x where x.code_norm = p_code_norm and x.col = p_col;
  if v.code_norm is null or v.state <> 'ok' or v.ne_kind <> (case p_kind when 'products' then 'single' else 'set' end) then
    return '{"ok": false, "why": "ne_unknown"}'::jsonb;
  end if;
  -- 今朝の取得の後・今日 (JST) にこの画面の CSV でこの欄を出した (NE に上げた・人が受け取った) = NE は今朝の値か出した値のどちらか
  --   (同じ日にもう一度取得した後も、その前に今日出した CSV をまだ取り込んでいないかもしれない = 今日の分は見る・Codex 0096 R1 H2)
  select r.expect, r.cell into v_x, v_c from ops.ne_upd_rows r join ops.ne_upd_files f on f.file_id = r.file_id
   where r.code_norm = p_code_norm and r.col = p_col and f.kind = p_kind and ops.ne_upd_file_out(f.state, f.claimed_at, f.downloaded_at)
     and coalesce(f.downloaded_at, f.uploaded_at, f.claimed_at)
         >= least(p_since, (core.jst_date(pg_catalog.clock_timestamp())::timestamp at time zone 'Asia/Tokyo'))
   order by coalesce(f.downloaded_at, f.uploaded_at, f.claimed_at) desc, r.row_id desc limit 1;
  if v_x is not null and v_x is distinct from v.value then
    -- 出した値が自社の今の値 (送っている途中) = その値をもう一度 (どちらの CSV を後に取り込んでも NE は同じ値になる)
    if (coalesce(p_sending, '{}'::jsonb) -> (p_sku_id::text || ':' || p_col)) is not distinct from v_x then
      return pg_catalog.jsonb_build_object('ok', true, 'cell', v_c, 'src', 'sent');
    end if;
    return '{"ok": false, "why": "sent_other"}'::jsonb;
  end if;
  -- NE の取扱区分は 3 つ (取扱中 0・取扱中止 1・ﾒｰｶｰ取扱中止 2)。今朝の値は 1 と 2 をどちらも discontinued で持つ = 1 を書くと 2 を変えてしまう
  if p_col = 'handling' and v.value is distinct from '"active"'::jsonb then
    return case when v.value = '"discontinued"'::jsonb then '{"ok": false, "why": "ne_handling_stop"}'::jsonb else '{"ok": false, "why": "ne_handling_value"}'::jsonb end;
  end if;
  c := ops.ne_upd_cell(p_col, v.value, p_code_norm);
  if (c ->> 'ok') is distinct from 'true' then return pg_catalog.jsonb_build_object('ok', false, 'why', 'ne_' || coalesce(c ->> 'why', 'value')); end if;
  return pg_catalog.jsonb_build_object('ok', true, 'cell', c ->> 'cell', 'src', 'ne');
end $$;
revoke all on function ops._ne_upd_filler(text, bigint, text, text, jsonb, timestamptz, jsonb) from public;

-- ═══════════ 4. 1 つの組のファイル ═══════════
/**
 * 見出し = コードの列 + p_cols (列の表の順)。p_skus の行を 1,000 行ずつのファイルにして、ファイルと行 (変えた欄) を残す。戻り値 = [{ file_id, kind, header, line_count, rows, sha256, bytes_b64? }]
 *   p_rows = 送る単位 (ops.ne_upd_units の units の send)・p_fill = { sku_id: { col: { cell, src } } } = 埋めるセル
 *   🚨 どのセルも「変えた欄」か「埋めるセル」のどちらか (どちらも無い = 黙って空欄にしない = 止める)
 */
create function ops._ne_upd_emit(p_send_id bigint, p_kind text, p_cols text[], p_skus text[], p_rows jsonb, p_fill jsonb, p_u jsonb, p_host text, p_upload boolean) returns jsonb
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  v_limit  integer := ops.ne_upd_file_limit();
  v_header text;
  v_lines  text[];
  v_bytes  bytea;
  v_file   bigint;
  v_n      integer;
  v_files  jsonb := '[]'::jsonb;
  r        record;
begin
  if p_kind not in ('products', 'sets') or coalesce(pg_catalog.cardinality(p_cols), 0) = 0 or coalesce(pg_catalog.cardinality(p_skus), 0) = 0 then
    raise exception 'ne_upd_layout: 組が空' using errcode = 'P0001';
  end if;
  if exists (select 1 from pg_catalog.unnest(p_skus) t(sku), pg_catalog.unnest(p_cols) c(col)
              where not exists (select 1 from pg_catalog.jsonb_array_elements(p_rows) y where y ->> 'sku_id' = t.sku and y ->> 'kind' = p_kind and y ->> 'col' = c.col)
                and not (coalesce(p_fill -> t.sku, '{}'::jsonb) ? c.col)) then
    raise exception 'ne_upd_layout: 変えた欄でも埋めるセルでもないセルがある (空欄にしない)' using errcode = 'P0001';
  end if;
  v_header := (case p_kind when 'products' then 'syohin_code' else 'set_syohin_code' end) || ','
    || (select pg_catalog.string_agg(c.header, ',' order by c.ord) from ops.ne_upd_cols() c where c.kind = p_kind and c.col = any (p_cols));
  for r in
    select ((t.o - 1) / v_limit) as part, pg_catalog.array_agg(t.sku order by t.o) as skus
      from pg_catalog.unnest(p_skus) with ordinality as t(sku, o) group by 1 order by 1
  loop
    select pg_catalog.array_agg(q.line order by q.o) into v_lines from (
      select t.o, ops.ne_upd_quote((select pg_catalog.min(x ->> 'ne_code') from pg_catalog.jsonb_array_elements(p_rows) x where x ->> 'sku_id' = t.sku and x ->> 'kind' = p_kind)) || ',' || (
               select pg_catalog.string_agg(coalesce(ops.ne_upd_quote(coalesce(own.cell, p_fill #>> array[t.sku, c.col, 'cell'])), ''), ',' order by c.ord)
                 from ops.ne_upd_cols() c
                 left join lateral (select y ->> 'cell' as cell from pg_catalog.jsonb_array_elements(p_rows) y
                                     where y ->> 'sku_id' = t.sku and y ->> 'kind' = p_kind and y ->> 'col' = c.col limit 1) own on true
                where c.kind = p_kind and c.col = any (p_cols)) as line
        from pg_catalog.unnest(r.skus) with ordinality as t(sku, o)) q;
    v_bytes := pg_catalog.convert_to(v_header || pg_catalog.chr(13) || pg_catalog.chr(10) || pg_catalog.array_to_string(v_lines, pg_catalog.chr(13) || pg_catalog.chr(10))
                                     || pg_catalog.chr(13) || pg_catalog.chr(10), 'UTF8');
    insert into ops.ne_upd_files (send_id, kind, header, cols, converter_version, file_bytes, sha256, line_count, claimed_by, claimed_at, fill)
      values (p_send_id, p_kind, v_header, p_cols, 'ne-upd-v2', v_bytes, pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex'), pg_catalog.cardinality(v_lines),
              case when p_upload then p_host end, case when p_upload then pg_catalog.clock_timestamp() end,
              (select coalesce(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('sku_id', t.sku, 'col', k.key, 'cell', k.value -> 'cell', 'src', k.value ->> 'src') order by t.o, k.key), '[]'::jsonb)
                 from pg_catalog.unnest(r.skus) with ordinality as t(sku, o), pg_catalog.jsonb_each(coalesce(p_fill -> t.sku, '{}'::jsonb)) k
                where k.key = any (p_cols)))
      returning file_id into v_file;
    insert into ops.ne_upd_rows (file_id, send_id, sku_id, code_norm, ne_code, col, role, cell, expect, norm_version, ne_before, evidence)
      select v_file, p_send_id, (x ->> 'sku_id')::bigint, x ->> 'code_norm', x ->> 'ne_code', x ->> 'col', x ->> 'role', x ->> 'cell', x -> 'cdb_value',
             coalesce((p_u #>> '{ne,norm_version}')::smallint, 1), x -> 'ne_value',
             pg_catalog.jsonb_build_object('ne_run', p_u #>> '{ne,run}', 'ne_spelling', x -> 'ne_spelling', 'lz_spelling', x -> 'lz_spelling', 'lz_exported_at', p_u #> '{lz,exported_at}',
                                           'changed_by', x -> 'changed_by', 'changed_at', x -> 'changed_at', 'sent_count', x -> 'sent_count')
        from pg_catalog.jsonb_array_elements(p_rows) x where x ->> 'kind' = p_kind and (x ->> 'sku_id') = any (r.skus) and (x ->> 'col') = any (p_cols);
    get diagnostics v_n = row_count;
    v_files := v_files || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('file_id', v_file::text, 'kind', p_kind, 'header', v_header,
      'line_count', pg_catalog.cardinality(v_lines), 'rows', v_n, 'sha256', pg_catalog.encode(pg_catalog.sha256(v_bytes), 'hex'),
      'bytes_b64', case when p_upload then pg_catalog.encode(v_bytes, 'base64') end));
  end loop;
  return v_files;
end $$;
revoke all on function ops._ne_upd_emit(bigint, text, text[], text[], jsonb, jsonb, jsonb, text, boolean) from public;

-- ═══════════ 5. 回の中で差を作り直してファイルを作る (0094 の版から: 組 = 種類ごとに 1 つ) ═══════════
/**
 * (0094 と同じ) p_cap = 毎朝の自動の上限を使うか・p_upload = API で上げるファイル (claimed) / 人が受け取るファイル
 * 🆕 0096 の組: 種類ごとに、見出し = その種類で変える欄の全部。どの行も埋めるセル (ops._ne_upd_filler) で全部の欄を埋める。
 *   埋められない行 = 2 つ目のファイル (見出し = その行たちの変える欄) → それでも埋められない行 = その SKU が変えた欄だけの組 (埋めるセルなし)。
 *   1 つ目に入れなかった行は summary.apart に (コード・欄・理由)
 */
create or replace function ops._ne_upd_fill(p_send_id bigint, p_cap boolean, p_host text, p_upload boolean) returns jsonb
  language plpgsql set search_path = pg_catalog, pg_temp as $$
declare
  u        jsonb := ops.ne_upd_units(null, case when p_upload then 'api' else 'bulk' end);
  v_rows   jsonb;
  v_n      integer;
  v_avg    numeric;
  v_blank  jsonb := u #> array['blank_ok', case when p_upload then 'api' else 'bulk' end];
  v_sending jsonb;
  v_kind   text;
  v_since  timestamptz;
  v_skus   jsonb;      -- { sku_id: { code_norm, code, cols: [変える欄] } }
  v_left   text[];
  v_fit    text[];
  v_cols   text[];
  v_fill   jsonb;
  v_one    jsonb;
  v_apart  jsonb := '[]'::jsonb;
  v_tier   integer;
  v_ok     boolean;
  v_f      jsonb;
  v_files  jsonb := '[]'::jsonb;
  v_total_lines integer := 0;
  v_total_rows  integer := 0;
  v_reason text;
  s        text;
  c        text;
  g        record;
begin
  if (u ->> 'ok') is distinct from 'true' then
    v_reason := coalesce((select pg_catalog.string_agg(p ->> 'text', '・') from pg_catalog.jsonb_array_elements(u -> 'problems') p), '作れる状態でない');
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
    select pg_catalog.avg(s0.row_count) into v_avg from ops.ne_upd_sends s0
     where s0.how = 'auto' and s0.state in ('built', 'sending', 'sent', 'unknown') and s0.row_count > 0 and s0.requested_at > pg_catalog.clock_timestamp() - interval '14 days';
    if v_n > ops.ne_upd_auto_limit() then
      v_reason := pg_catalog.format('差が %s 欄あり、毎朝の上限 (%s 欄) を超えたので、今朝は止めました (画面「NEへ送る」で中身を見て CSV を作れます)', v_n, ops.ne_upd_auto_limit());
    elsif v_avg is not null and v_n > 20 and v_n > v_avg * 5 then
      v_reason := pg_catalog.format('差が %s 欄あり、ふだん (この 14 日の平均 %s 欄) の 5 倍を超えたので、今朝は止めました (画面「NEへ送る」で中身を見て CSV を作れます)', v_n, pg_catalog.round(v_avg, 1));
    end if;
    if v_reason is not null then
      update ops.ne_upd_sends set state = 'held', reason = v_reason, row_count = v_n, finished_at = pg_catalog.clock_timestamp() where send_id = p_send_id;
      return pg_catalog.jsonb_build_object('send_id', p_send_id::text, 'state', 'held', 'reason', v_reason, 'rows', v_n, 'files', '[]'::jsonb);
    end if;
  end if;
  -- 送っている途中の欄 (今朝の後に出した値 = 自社の今の値) = 埋めるセルをその値にする
  select coalesce(pg_catalog.jsonb_object_agg((x ->> 'sku_id') || ':' || (x ->> 'col'), x -> 'cdb_value'), '{}'::jsonb) into v_sending
    from pg_catalog.jsonb_array_elements(u -> 'units') x where x ->> 'status' = 'sending' and x ->> 'role' = 'change';

  foreach v_kind in array array['products', 'sets'] loop
    select pg_catalog.jsonb_object_agg(t.sku_id, pg_catalog.jsonb_build_object('code_norm', t.code_norm, 'code', t.code, 'cols', t.cols)),
           pg_catalog.array_agg(t.sku_id order by t.code_norm, t.sku_id)
      into v_skus, v_left
      from (select x ->> 'sku_id' as sku_id, pg_catalog.min(x ->> 'code_norm') as code_norm, pg_catalog.min(x ->> 'code') as code,
                   pg_catalog.jsonb_agg(x ->> 'col' order by (x ->> 'ord')::integer) as cols
              from pg_catalog.jsonb_array_elements(v_rows) x where x ->> 'kind' = v_kind group by 1) t;
    continue when v_left is null;
    v_since := case v_kind when 'products' then (u #>> '{ne,products_started_at}')::timestamptz else (u #>> '{ne,sets_started_at}')::timestamptz end;
    v_tier := 0;
    while coalesce(pg_catalog.cardinality(v_left), 0) > 0 loop
      v_tier := v_tier + 1;
      v_fit := '{}'::text[];
      if v_tier <= 2 then
        -- 見出しの欄 = 残っている行の変える欄の全部 (列の表の順)
        select pg_catalog.array_agg(k.col order by k.ord) into v_cols from ops.ne_upd_cols() k
         where k.kind = v_kind and exists (select 1 from pg_catalog.unnest(v_left) t(sku) where (v_skus #> array[t.sku, 'cols']) ? k.col);
        v_fill := '{}'::jsonb;
        foreach s in array v_left loop
          v_ok := true;
          v_one := '{}'::jsonb;
          foreach c in array v_cols loop
            continue when (v_skus #> array[s, 'cols']) ? c;
            v_f := ops._ne_upd_filler(v_kind, s::bigint, v_skus #>> array[s, 'code_norm'], c, coalesce(v_blank -> v_kind, '[]'::jsonb), v_since, v_sending);
            if (v_f ->> 'ok') is distinct from 'true' then
              v_ok := false;
              if v_tier = 1 then
                v_apart := v_apart || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('kind', v_kind, 'code', v_skus #>> array[s, 'code'], 'col', c, 'why', v_f ->> 'why'));
              end if;
              exit;
            end if;
            v_one := v_one || pg_catalog.jsonb_build_object(c, v_f - 'ok');
          end loop;
          if v_ok then
            v_fit := v_fit || s;
            v_fill := v_fill || pg_catalog.jsonb_build_object(s, v_one);
          end if;
        end loop;
        if pg_catalog.cardinality(v_fit) > 0 then
          v_files := v_files || ops._ne_upd_emit(p_send_id, v_kind, v_cols, v_fit, v_rows, v_fill, u, p_host, p_upload);
          v_left := array(select t.sku from pg_catalog.unnest(v_left) with ordinality as t(sku, o) where t.sku <> all (v_fit) order by t.o);
          continue;
        end if;
      end if;
      -- 最後の手: 残りの行は、その SKU が変えた欄だけの組 (埋めるセルなし = 0094 と同じ形)
      for g in
        select (v_skus #> array[t.sku, 'cols'])::text as cols_json, pg_catalog.array_agg(t.sku order by t.o) as skus
          from pg_catalog.unnest(v_left) with ordinality as t(sku, o) group by 1 order by 1
      loop
        select pg_catalog.array_agg(e order by o) into v_cols from pg_catalog.jsonb_array_elements_text(g.cols_json::jsonb) with ordinality as z(e, o);
        v_files := v_files || ops._ne_upd_emit(p_send_id, v_kind, v_cols, g.skus, v_rows, '{}'::jsonb, u, p_host, p_upload);
      end loop;
      exit;
    end loop;
  end loop;

  select coalesce(pg_catalog.sum((f ->> 'line_count')::integer), 0), coalesce(pg_catalog.sum((f ->> 'rows')::integer), 0) into v_total_lines, v_total_rows
    from pg_catalog.jsonb_array_elements(v_files) f;
  if v_total_rows <> v_n then
    raise exception 'ne_upd_layout: ファイルに入れた欄 (%) が送る欄 (%) と合わない', v_total_rows, v_n using errcode = 'P0001';
  end if;
  update ops.ne_upd_sends set state = 'built', row_count = v_total_rows, line_count = v_total_lines, left_count = 0, built_at = pg_catalog.clock_timestamp(),
         summary = (select pg_catalog.jsonb_build_object('cols', coalesce(pg_catalog.jsonb_object_agg(t.k, t.n), '{}'::jsonb)) from (
                      select case when z.role = 'spelling' then 'spelling' else f.kind || ':' || z.col end as k, pg_catalog.count(*) as n
                        from ops.ne_upd_rows z join ops.ne_upd_files f on f.file_id = z.file_id
                       where z.send_id = p_send_id group by 1) t)
                   || case when pg_catalog.jsonb_array_length(v_apart) > 0
                           then pg_catalog.jsonb_build_object('apart', (select pg_catalog.jsonb_agg(a) from (select a from pg_catalog.jsonb_array_elements(v_apart) a limit 50) z),
                                                              'apart_count', pg_catalog.jsonb_array_length(v_apart))
                           else '{}'::jsonb end
   where send_id = p_send_id;
  return pg_catalog.jsonb_build_object('send_id', p_send_id::text, 'state', 'built', 'rows', v_total_rows, 'lines', v_total_lines, 'left', 0,
    'files', (select coalesce(pg_catalog.jsonb_agg(f - 'rows'), '[]'::jsonb) from pg_catalog.jsonb_array_elements(v_files) f),
    'summary', (select s0.summary from ops.ne_upd_sends s0 where s0.send_id = p_send_id));
end $$;
revoke all on function ops._ne_upd_fill(bigint, boolean, text, boolean) from public;

-- ═══════════ 6. 受け取り (0094 の版から: 埋めたセルの確かめを足した) ═══════════
/** 受け取りのファイルのバイト列 (作った JST の日だけ・最初のダウンロードの人と時刻と回数を残す)。戻り値 = { file_id, kind, file_name, sha256, bytes_b64 } */
create or replace function ops.ne_upd_download(p_actor text, p_file_id bigint) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  f ops.ne_upd_files;
  u jsonb;
  v_bad integer;
  v_blank_bad integer := 0;
  v_fill_bad integer := 0;
  v_since timestamptz;
  v_sending jsonb;
  v_g jsonb;
  v_older jsonb;
  r2 record;
begin
  if not ops._ne_upd_actor_ok(p_actor) then raise exception 'invalid_input: 人 (ログインのメール) が要る' using errcode = '22023'; end if;
  perform ops._ne_upd_begin();
  select * into f from ops.ne_upd_files x where x.file_id = p_file_id for update;
  if f.file_id is null then raise exception 'not_found: ファイル % が無い', p_file_id using errcode = 'P0002'; end if;
  -- API で上げるファイル (送り手の claimed) は人に渡さない (2 つの道で同じファイルを NE に入れない)。毎朝の自動で作った人が受け取る CSV は渡す
  if f.claimed_by is not null then raise exception 'not_download: このファイルは送り手が NE に直接上げるもの (人が受け取るファイルでない)' using errcode = 'P0001'; end if;
  if core.jst_date(f.created_at) <> core.jst_date(pg_catalog.clock_timestamp()) then
    raise exception 'file_old: 前の日に作ったファイルは渡しません (今の値と違うかもしれない)。もう一度「CSV を作る」を押してください' using errcode = 'P0001';
  end if;
  -- 渡す前に、ファイルの行が今もそのまま送る差か確かめる (作った後に値を直した・ほかの CSV でもう出した・止める理由が出た・NE が同じになった = 渡さない・Codex PR-2 R2 High 1)。
  --   もう受け取ったファイルのもう一度のダウンロードは、その行が「送っている途中」で同じ値のときだけ
  u := ops.ne_upd_units((select pg_catalog.array_agg(distinct r.sku_id) from ops.ne_upd_rows r where r.file_id = f.file_id), 'bulk');
  if (u ->> 'ok') is distinct from 'true' then
    raise exception 'file_stale: いまは渡せません (%)。もう一度「CSV を作る」を押してください',
      coalesce((select pg_catalog.string_agg(p ->> 'text', '・') from pg_catalog.jsonb_array_elements(u -> 'problems') p), '今朝の NE の値が無い') using errcode = 'P0001';
  end if;
  select pg_catalog.count(*) into v_bad from ops.ne_upd_rows r
   where r.file_id = f.file_id and not exists (
     select 1 from pg_catalog.jsonb_array_elements(u -> 'units') x
      where x ->> 'code_norm' = r.code_norm and x ->> 'col' = r.col and x ->> 'role' = r.role and (x -> 'cdb_value') = r.expect and x ->> 'ne_code' = r.ne_code
        and ((f.downloaded_at is null and x ->> 'status' = 'send') or (f.downloaded_at is not null and x ->> 'status' = 'sending')));
  if v_bad > 0 then
    raise exception 'file_stale: このファイルを作った後に値が変わったか、ほかの CSV でもう出した欄があります (% 行)。もう一度「CSV を作る」を押してください', v_bad using errcode = 'P0001';
  end if;
  -- 🆕 0096: 埋めたセル (見出しにあってその SKU が変えていない欄) を今の状態でもう一度決めて、ファイルと同じか (Codex PR-2 R3 H3 の空欄の確かめを広げた)
  --   空欄 = 今も NE の実物で確かめた欄 / 今朝の NE の値・出した値 = 今も同じ値 (今朝の値が入れ替わった・その後にほかの CSV でその欄を出した = 渡さない = 新しい方を朝の値に戻さない)
  v_since := case f.kind when 'products' then (u #>> '{ne,products_started_at}')::timestamptz else (u #>> '{ne,sets_started_at}')::timestamptz end;
  select coalesce(pg_catalog.jsonb_object_agg((x ->> 'sku_id') || ':' || (x ->> 'col'), x -> 'cdb_value'), '{}'::jsonb) into v_sending
    from pg_catalog.jsonb_array_elements(u -> 'units') x where x ->> 'status' = 'sending' and x ->> 'role' = 'change';
  for r2 in
    select s.sku_id, s.code_norm, c.col,
           (select e from pg_catalog.jsonb_array_elements(f.fill) e where e ->> 'sku_id' = s.sku_id::text and e ->> 'col' = c.col limit 1) as e
      from (select distinct r.sku_id, r.code_norm from ops.ne_upd_rows r where r.file_id = f.file_id) s, pg_catalog.unnest(f.cols) c(col)
     where not exists (select 1 from ops.ne_upd_rows r where r.file_id = f.file_id and r.sku_id = s.sku_id and r.col = c.col)
  loop
    if r2.e is null or coalesce(pg_catalog.jsonb_typeof(r2.e -> 'cell'), 'null') = 'null' then
      -- 空欄 (0096 の前のファイルは埋めたセルの記録が無い = 空欄)
      if not (coalesce(u #> array['blank_ok', 'bulk', f.kind], '[]'::jsonb) ? r2.col) then v_blank_bad := v_blank_bad + 1; end if;
    else
      v_g := ops._ne_upd_filler(f.kind, r2.sku_id, r2.code_norm, r2.col, '[]'::jsonb, v_since, v_sending);
      if (v_g ->> 'ok') is distinct from 'true' or (v_g ->> 'cell') is distinct from (r2.e ->> 'cell') then v_fill_bad := v_fill_bad + 1; end if;
    end if;
  end loop;
  if v_blank_bad > 0 then
    raise exception 'file_stale: 空欄にした欄の確かめが取り消されました。もう一度「CSV を作る」を押してください' using errcode = 'P0001';
  end if;
  if v_fill_bad > 0 then
    raise exception 'file_stale: このファイルを作った後に、変えていない欄の NE の値が変わったか、ほかの CSV でその欄を出しました (% か所)。もう一度「CSV を作る」を押してください', v_fill_bad
      using errcode = 'P0001';
  end if;
  -- 🆕 0096 (Codex 0096 R1 H1): 今日もう出した (受け取った・上げた) ほかの CSV が、このファイルで直す欄を「埋めるセル」として別の値で書いている =
  --   このファイルの後にその CSV を取り込むと、直しが古い値に戻る (手元のファイルは DB では止められない)。渡すが、画面で「前の CSV は取り込まない」と知らせる
  --   (取り込んでしまっても、翌朝の照合で差がまた出て、もう一度送る = Company DB の値は失われない)
  select coalesce(pg_catalog.jsonb_agg(z.o order by z.at), '[]'::jsonb) into v_older from (
    select distinct on (o.file_id) pg_catalog.jsonb_build_object('file_id', o.file_id::text, 'kind', o.kind, 'at', coalesce(o.downloaded_at, o.uploaded_at, o.claimed_at)) as o,
           coalesce(o.downloaded_at, o.uploaded_at, o.claimed_at) as at
      from ops.ne_upd_rows r
      join ops.ne_upd_files o on o.file_id <> r.file_id and o.kind = f.kind and ops.ne_upd_file_out(o.state, o.claimed_at, o.downloaded_at)
                             and core.jst_date(o.created_at) = core.jst_date(pg_catalog.clock_timestamp())
      cross join lateral pg_catalog.jsonb_array_elements(o.fill) e
     where r.file_id = f.file_id and e ->> 'sku_id' = r.sku_id::text and e ->> 'col' = r.col
       and pg_catalog.jsonb_typeof(e -> 'cell') = 'string' and (e ->> 'cell') is distinct from r.cell
     order by o.file_id) z;
  perform pg_catalog.set_config('ops.ne_upd_protocol', '1', true);
  update ops.ne_upd_files set downloaded_at = coalesce(downloaded_at, pg_catalog.clock_timestamp()), downloaded_by = coalesce(downloaded_by, p_actor), download_count = download_count + 1
   where file_id = p_file_id returning * into f;
  update ops.ne_upd_sends set state = 'sent', finished_at = coalesce(finished_at, pg_catalog.clock_timestamp()) where send_id = f.send_id and state = 'built'
     and not exists (select 1 from ops.ne_upd_files x where x.send_id = f.send_id and x.downloaded_at is null);
  perform pg_catalog.set_config('ops.ne_upd_protocol', '', true);
  return pg_catalog.jsonb_build_object('file_id', f.file_id::text, 'kind', f.kind, 'sha256', f.sha256, 'line_count', f.line_count, 'bytes_b64', pg_catalog.encode(f.file_bytes, 'base64'),
    'file_name', pg_catalog.format('ne-update-%s-%s-%s.csv', f.kind, pg_catalog.to_char(f.created_at at time zone 'Asia/Tokyo', 'YYYYMMDD'), f.file_id),
    'older', v_older);
end $$;
revoke all on function ops.ne_upd_download(text, bigint) from public;

-- ═══════════ 7. 権限 ═══════════
-- 画面の印 (ops.ne_upd_one_file) だけ master_edit に (to_regprocedure と has_function_privilege で見る)。ほかの新しい関数は中の部品 = だれにも渡さない
do $$ begin
  if exists (select 1 from pg_catalog.pg_roles where rolname = 'master_edit') then
    execute 'grant execute on function ops.ne_upd_one_file() to master_edit';
  end if;
end $$;
