-- 0057: 注文明細の解き直し (reresolve) を「上限つきの batch + cursor + skip した注文の retry の表」にする (D-60 PR 1b-0r・2026-10-04)
--   設計 = AI_reference CompanyDB構想/13 §3.10「reresolve の batch の契約」(v3.12・Codex R-D60-v3-7〜12 / 判定 R-D60-v3-13 = 1b-0r の実装 Go・
--   条件 = L1 (high-water + 1 の bigint の上限) と L2 (cursor の行の CHECK (cycle_through_order_id is null or cycle_started_at is not null)))
--
-- なぜ: 0024 の core.reresolve_order_lines(smallint, text, date) は ① 期間の終わりが無い (p_since = null で全履歴) ② 件数の上限が無い
--   ③ 一時の表 4 つ (TEMP) を使う。D-60 で TEMP を外し (PR 1b)、重い入口に上限を付ける前に、上限つきの新しい署名を足す。
--
-- 🚨 この migration は夜間ロードの動きを変えない:
--   - 旧い 3 引数 core.reresolve_order_lines(smallint, text, date) は **そのまま残す** (中身も権限も変えない)。夜間ロード (apps/company-db/load/engine.mjs 8b) は
--     今までどおり to_regprocedure('core.reresolve_order_lines(smallint, text, date)') で旧い署名を呼ぶ。新しい署名へ切り替えるのは PR 1b-0e
--     (本番の件数を読むだけの SQL で確かめた後)。旧い 3 引数を DROP するのは PR 1b (夜間ロードが移った後)。
--   - 新しい署名は引数が 4 つ以上 (p_until に既定値が無い) = 3 引数の呼び出しは今までどおり旧い関数に解決する (曖昧にならない)。
--
-- なにを:
--   1. 表 3 つ (ops・主キーは自然の鍵 = sequence なし・バックアップは restore (今の dump.mjs の既定のまま)・runtime (= 今は持ち主) が書く・watcher は読む)
--      ops.reresolve_retry_orders    = skip locked で飛ばした注文 (関数がその batch の中で upsert・lock を取って処理したら消す・呼び手は書かない)
--      ops.reresolve_backlog_windows = 夜の窓の予約と持ち越し (固定の [p_since, p_until) と cursor・1b-0e の夜の回し方が使う)
--      ops.reresolve_retry_cursor    = retry の表の cursor と周回の high-water (夜をまたいで持つ・CHECK 3 つ)
--   2. core.reresolve_order_lines(p_company, p_mall, p_since, p_until, p_after_order_id, p_max_orders, p_max_lines) = 窓 [p_since, p_until) の 1 batch
--      core.reresolve_order_lines_retry(p_company, p_mall, p_after_order_id, p_before_order_id, p_max_orders, p_max_lines) = retry の表の 1 batch
--      core._reresolve_order_batch(p_company, p_mall, p_order_ids, p_retry) = 上の 2 つの共通の部品 (lock → 解く → touch → retry の表)
--   戻り (2 つとも同じ) = 今の 4 列 (candidates・resolved・orders_touched・orders_skipped_locked = 名前と意味は 0024 と同じ) +
--     has_more・next_after_order_id・orders_examined・stop_reason ('done' / 'max_orders' / 'max_lines')・skipped_order_ids (昇順)
--
-- 契約 (設計 13 §3.10 のとおり):
--   - 窓 = [p_since, p_until) (注文日 order_date_jst・終わりを含まない)・両方必須・p_since < p_until・p_until − p_since ≦ 62 日。違えば 22023
--   - 上限 = p_max_orders 1〜5,000 (既定 2,000)・p_max_lines 1〜20,000 (既定 10,000)。範囲の外・null は 22023。p_after_order_id は 0 以上 (null は 22023)
--   - 1 batch = order_id > p_after_order_id の候補を order_id の昇順に、注文の数が p_max_orders に、未解決の明細の累計が p_max_lines に達する手前まで。
--     1 つ目の注文は明細の数に関わらず必ず含める (必ず前に進む)。上限を超えても例外にしない (has_more = true で次の batch へ)
--   - cursor = next_after_order_id = この batch に含めて lock を試した最後の注文 (処理した注文と skip した注文の両方)。候補が無ければ p_after_order_id のまま
--   - max_lines で「次を含めると上限を超える」と分かった次の注文と、max_orders の +1 件目は含めない = orders_examined・cursor・skip のどれにも数えない
--   - skip locked で取れなかった注文は、その batch の中で retry の表へ (窓の関数 = upsert で last_skipped_at を進める・attempts は増やさない /
--     retry の関数 = attempts + 1・last_skipped_at)。lock を取れた注文の retry の行はその場で消す (解けたかどうかに関係なく)
--   - 候補を読んだ後に消えた注文 (core.orders に無い) は skip に数えない (retry の行も消す = 永久に残らない)
--   - retry の関数の p_before_order_id = null なら上限なし・あれば order_id < p_before_order_id だけ・p_before_order_id ≦ p_after_order_id は 22023。
--     夜の回し方 (1b-0e) は周回の high-water + 1 を渡す。🚨 high-water が bigint の最大値 (9223372036854775807) なら + 1 は溢れる = null を渡す (L1・
--     apps/company-db/load/reresolve-batch.mjs の beforeOrderIdFor が BigInt で計算する)
--   - ロックの順と再確認は 0024 と同じ (注文 = order_id の順に skip locked → 明細 = order_line_id の順に for update →
--     update の時に「まだ未解決で元のコードが同じ」を再確認 → 当たった注文の updated_at を core.touch_force で進める)
--   - 一時の表は使わない。中間は配列の変数 (要素は p_max_orders + 1 ≦ 5,001 個の bigint / integer) と、窓の注文の materialized の CTE
--     (窓 ≦ 62 日の注文の id = work_mem を超えたら一時のファイル = temp_file_limit が縛る)
--   - 明細の更新は候補の CTE と結合しない (1 つの UPDATE の WHERE で行の今のコードを解く) = 表の統計に依らず、読むのは lock した注文の明細だけ
--   - search_path = pg_catalog, pg_temp に固定し、表と関数は schema で修飾する (SECURITY INVOKER のまま)。work_mem = 4MB・hash_mem_multiplier = 2 (0057 の
--     relink / merge (PR #1605) と同じ枠・関数の中だけ・出るときに戻る)。関数の SET は中で発火する trigger (core.touch_updated_at_unless_seq_only・FK) にも効く
--     = trigger の本体は pg_catalog の関数 (to_jsonb・current_setting・now) だけ = 影響なし
--   - 権限 = 3 つの関数は PUBLIC の EXECUTE を外す (作った同じ取引で・0056 の約束「これから作る重い関数は作った直後に REVOKE」)。持ち主は持ち主として呼べる。
--     runtime を持ち主から分けるのは PR 1b (そのとき runtime に EXECUTE と 3 つの表の DML を付ける・設計の RUNTIME_DML_DENY には入れない)。
--     watcher は 3 つの表の SELECT だけ (関数は呼べない)
--   - heavy-entry-manifest.mjs = 新しい 2 つと部品を guard_later (public: false) に足す (設計の分け = Gw・門は PR 3a)
--
-- 同時に 2 本の夜間ロードが同じ会社 × モールを流すことは想定しない (PR 4a の共通の lock company_db_heavy)。流れても値は壊れない
--   (相手が lock した注文は skip → retry の表 → 次の晩に lock を取って処理して消す = 余分な 1 回の確かめだけ)。
-- 2 回流すと create table / create function が 42P07 / 42723 で止まる (migration の実行器は 1 回だけ流す)。

-- ─── 1. 表 ───
create table ops.reresolve_retry_orders (
  company_id        smallint not null,
  mall              text not null,
  order_id          bigint not null check (order_id > 0),
  first_skipped_at  timestamptz not null default now(),   -- 最初に skip された時刻 (⚠️ = 7 日より前・attempts に関係なく)
  last_skipped_at   timestamptz not null default now(),
  attempts          integer not null default 0 check (attempts >= 0),   -- retry の関数で lock を取れなかった回数 (窓の関数の skip では増やさない)
  primary key (company_id, mall, order_id)
);
comment on table ops.reresolve_retry_orders is 'reresolve で skip locked で飛ばした注文 (D-60 1b-0r)。core.reresolve_order_lines / _retry がその batch の中で書き、lock を取って処理したら消す。呼び手は書かない';

create table ops.reresolve_backlog_windows (
  company_id      smallint not null,
  mall            text not null,
  p_since         date not null,
  p_until         date not null,
  after_order_id  bigint not null default 0 check (after_order_id >= 0),   -- 窓の cursor (次の batch の p_after_order_id)
  reserved_at     timestamptz not null default now(),
  saved_at        timestamptz,                                              -- 1 晩の上限で止まって cursor を残した時刻
  primary key (company_id, mall, p_since, p_until),
  constraint ck_reresolve_backlog_window check (p_since < p_until and p_until - p_since <= 62)
);
comment on table ops.reresolve_backlog_windows is 'reresolve の夜の窓の予約と持ち越し (D-60 1b-0r)。固定の [p_since, p_until) と cursor。流し終えた窓の行は消す';

create table ops.reresolve_retry_cursor (
  company_id              smallint not null,
  mall                    text not null,
  after_order_id          bigint not null default 0 check (after_order_id >= 0),   -- retry の表の cursor (夜をまたいで持つ)
  cycle_through_order_id  bigint check (cycle_through_order_id > 0),             -- 今の周回の high-water (null = 周回の外 = 次の晩の始めに新しい周回)
  cycle_started_at        timestamptz,                                             -- 新しい周回の始めにだけ入れる (周回の途中・終えた晩は変えない)
  cycles_completed        integer not null default 0 check (cycles_completed >= 0),
  updated_at              timestamptz not null default now(),
  primary key (company_id, mall),
  constraint ck_reresolve_cursor_outside_cycle check (cycle_through_order_id is not null or after_order_id = 0),
  constraint ck_reresolve_cursor_within_cycle check (cycle_through_order_id is null or after_order_id <= cycle_through_order_id),
  constraint ck_reresolve_cursor_cycle_started check (cycle_through_order_id is null or cycle_started_at is not null)   -- R-D60-v3-13 L2 (⚠️ ④ を fail-open にしない)
);
comment on table ops.reresolve_retry_cursor is 'reresolve の retry の表の cursor と周回の high-water (D-60 1b-0r・v3.12)。夜の本体の取引の中で for update で読んで更新する';

-- ─── 2. 共通の部品: 注文の batch を lock して解く ───
--   p_order_ids = 昇順・重なりなし・≦ 5,000 (呼ぶのは下の 2 つの関数だけ)。p_retry = retry の関数から (skip で attempts + 1) か窓の関数から (skip で upsert)
--   戻り = candidates (lock を取れた注文の未解決の明細の数)・resolved・orders_touched・skipped_order_ids (lock を取れなかった注文・昇順)・gone_order_ids (core.orders に無い)
create function core._reresolve_order_batch(p_company smallint, p_mall text, p_order_ids bigint[], p_retry boolean)
returns table (candidates integer, resolved integer, orders_touched integer, skipped_order_ids bigint[], gone_order_ids bigint[])
language plpgsql volatile
set search_path = pg_catalog, pg_temp set work_mem = '4MB' set hash_mem_multiplier = 2
as $$
declare
  v_locked bigint[];
  v_done   bigint[];
begin
  if p_company is null or p_mall is null or p_retry is null or p_order_ids is null or array_ndims(p_order_ids) > 1 or cardinality(p_order_ids) > 5000 then
    raise exception 'reresolve_bad_batch: 会社・モール・注文の配列 (1 次元・≦ 5,000) が要る' using errcode = '22023';
  end if;
  candidates := 0; resolved := 0; orders_touched := 0; skipped_order_ids := '{}'; gone_order_ids := '{}';
  if cardinality(p_order_ids) = 0 then return next; return; end if;

  -- ① 注文を order_id の順に lock (skip locked = 受け口の chunk が持つ注文は待たない = deadlock にしない)
  select coalesce(array_agg(x.order_id order by x.order_id), '{}') into v_locked
    from (select o.order_id from core.orders o
           where o.order_id = any(p_order_ids) and o.company_id = p_company and o.mall = p_mall
           order by o.order_id
             for update of o skip locked) x;
  -- ② 取れなかった注文 = 今もある (別の書き手が持っている = skip) か、消えた (gone = skip に数えない)
  select coalesce(array_agg(u.id order by u.id) filter (where e.ok), '{}'), coalesce(array_agg(u.id order by u.id) filter (where not e.ok), '{}')
    into skipped_order_ids, gone_order_ids
    from unnest(p_order_ids) u(id)
    cross join lateral (select exists (select 1 from core.orders o where o.order_id = u.id and o.company_id = p_company and o.mall = p_mall) as ok) e
   where u.id <> all (v_locked);

  if cardinality(v_locked) > 0 then
    -- ③ 明細を order_line_id の順に lock してから候補を確定 (注文の lock を持っている = 受け口はこの注文の明細に触れない)
    perform 1 from core.order_lines l
      where l.order_id = any(v_locked) and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null
      order by l.order_line_id
        for update of l;
    select count(*)::integer into candidates from core.order_lines l
     where l.order_id = any(v_locked) and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null;
    -- ④ 当たった明細だけ更新。🚨 候補の CTE と明細を結合しない = 1 つの UPDATE の WHERE で行そのものの今のコードを解く
    --    (CTE の列には統計が無い・統計の古い明細の表では入れ子のループ × 全表 = 件数の 2 乗になりうる = 使い捨ての PG で 1 batch 20 秒を見た。
    --     読むのは lock した注文の明細だけ (ix_order_lines_current の order_id = any)。lock を持っている = 行の今の値 = 0024 の「まだ未解決で元のコードが同じ」の再確認と同じ)
    --    resolve_listing_id (STABLE) は当たった行で 2 回呼ぶ (WHERE と SET・同じ文の中では同じ値)
    with upd as (
      update core.order_lines l
         set listing_id = core.resolve_listing_id(p_company, p_mall, l.unresolved_code), unresolved_code = null
       where l.order_id = any(v_locked) and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null
         and core.resolve_listing_id(p_company, p_mall, l.unresolved_code) is not null
      returning l.order_id
    )
    select count(*)::integer, coalesce(array_agg(distinct u.order_id), '{}') into resolved, v_done from upd u;
    -- ⑤ 実際に更新した明細の注文だけ updated_at を進める (翌朝の売上日次の作り直しに乗る・0024 と同じ保守経路)
    perform set_config('core.touch_force', 'on', true);
    update core.orders o set updated_at = now() where o.order_id = any(v_done);
    get diagnostics orders_touched = row_count;
    perform set_config('core.touch_force', 'off', true);
  end if;

  -- ⑥ retry の表: lock を取れた注文と消えた注文の行は消す / 取れなかった注文は書く (呼び手は書かない)
  delete from ops.reresolve_retry_orders t
   where t.company_id = p_company and t.mall = p_mall and t.order_id = any(v_locked || gone_order_ids);
  if cardinality(skipped_order_ids) > 0 then
    if p_retry then
      update ops.reresolve_retry_orders t set attempts = t.attempts + 1, last_skipped_at = now()
       where t.company_id = p_company and t.mall = p_mall and t.order_id = any(skipped_order_ids);
    else
      insert into ops.reresolve_retry_orders as t (company_id, mall, order_id)
        select p_company, p_mall, s.id from unnest(skipped_order_ids) s(id) order by s.id
        on conflict (company_id, mall, order_id) do update set last_skipped_at = now();
    end if;
  end if;
  return next;
end $$;
comment on function core._reresolve_order_batch(smallint, text, bigint[], boolean) is 'reresolve の 1 batch の共通の部品 (D-60 1b-0r)。注文 (skip locked) → 明細 → 解く → touch → retry の表。呼ぶのは core.reresolve_order_lines (7 引数) と core.reresolve_order_lines_retry だけ';

-- ─── 3. 窓の 1 batch ───
create function core.reresolve_order_lines(p_company smallint, p_mall text, p_since date, p_until date,
  p_after_order_id bigint default 0, p_max_orders integer default 2000, p_max_lines integer default 10000)
returns table (candidates integer, resolved integer, orders_touched integer, orders_skipped_locked integer,
  has_more boolean, next_after_order_id bigint, orders_examined integer, stop_reason text, skipped_order_ids bigint[])
language plpgsql volatile
set search_path = pg_catalog, pg_temp set work_mem = '4MB' set hash_mem_multiplier = 2
as $$
declare
  v_ids   bigint[];
  v_ns    integer[];
  v_n     integer := 0;
  v_lines bigint := 0;
  v_reason text := 'done';
  v_batch bigint[];
  h record;
begin
  if p_company is null or p_mall is null or p_since is null or p_until is null or p_after_order_id is null or p_max_orders is null or p_max_lines is null then
    raise exception 'reresolve_bad_args: 会社・モール・p_since・p_until・p_after_order_id・上限は null にできない' using errcode = '22023';
  end if;
  if p_since >= p_until or p_until - p_since > 62 then
    raise exception 'reresolve_bad_window: 窓 [%, %) は p_since < p_until かつ 62 日まで', p_since, p_until using errcode = '22023';
  end if;
  if p_max_orders not between 1 and 5000 or p_max_lines not between 1 and 20000 or p_after_order_id < 0 then
    raise exception 'reresolve_bad_limits: p_max_orders は 1〜5000・p_max_lines は 1〜20000・p_after_order_id は 0 以上 (% / % / %)', p_max_orders, p_max_lines, p_after_order_id using errcode = '22023';
  end if;

  -- 候補 = 窓の中で未解決の明細がある注文を order_id の順に p_max_orders + 1 件まで (+1 件目は has_more の判定だけ)。明細の数は絞った後に数える
  --   🚨 窓の注文は materialized の CTE で ix_orders_date (company_id, order_date_jst) の範囲から読む = 読む量は窓 (≦ 62 日) の注文の数で決まる。
  --      CTE にしないと planner が「主キーの順に読んで LIMIT で早く止まる」を選びうる (order_id は日付とほぼ同じ順 = 窓より古い全部の注文を読んでから窓に着く)
  with w as materialized (
    select o.order_id from core.orders o
     where o.company_id = p_company and o.mall = p_mall and o.order_date_jst >= p_since and o.order_date_jst < p_until
       and o.order_id > p_after_order_id
       and exists (select 1 from core.order_lines l
                    where l.order_id = o.order_id and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null)
  )
  select coalesce(array_agg(c.order_id order by c.order_id), '{}'), coalesce(array_agg(c.n order by c.order_id), '{}')
    into v_ids, v_ns
    from (select k.order_id,
                 (select count(*)::integer from core.order_lines l
                   where l.order_id = k.order_id and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null) as n
            from (select w.order_id from w order by w.order_id limit p_max_orders + 1) k) c;

  -- batch に含める数 (1 つ目は必ず・注文の数 / 明細の累計の上限の手前まで)
  for i in 1 .. cardinality(v_ids) loop
    if v_n >= p_max_orders then v_reason := 'max_orders'; exit; end if;
    if v_n > 0 and v_lines + v_ns[i] > p_max_lines then v_reason := 'max_lines'; exit; end if;
    v_n := v_n + 1; v_lines := v_lines + v_ns[i];
  end loop;
  v_batch := v_ids[1:v_n];

  select * into h from core._reresolve_order_batch(p_company, p_mall, v_batch, false);
  candidates := h.candidates; resolved := h.resolved; orders_touched := h.orders_touched;
  orders_skipped_locked := cardinality(h.skipped_order_ids); skipped_order_ids := h.skipped_order_ids;
  orders_examined := v_n;
  next_after_order_id := case when v_n > 0 then v_batch[v_n] else p_after_order_id end;
  stop_reason := v_reason; has_more := (v_reason <> 'done');
  return next;
end $$;
comment on function core.reresolve_order_lines(smallint, text, date, date, bigint, integer, integer) is
  '出品に当たらなかった注文明細を窓 [p_since, p_until) (≦ 62 日) で 1 batch だけ解き直す (D-60 1b-0r)。上限 = 注文 p_max_orders (≦ 5000)・未解決の明細 p_max_lines (≦ 20000)。has_more なら next_after_order_id で続ける。skip した注文は retry の表へ。旧い 3 引数 (0024) は夜間ロードが 1b-0e で移るまで残す';

-- ─── 4. retry の表の 1 batch ───
create function core.reresolve_order_lines_retry(p_company smallint, p_mall text, p_after_order_id bigint default 0, p_before_order_id bigint default null,
  p_max_orders integer default 2000, p_max_lines integer default 10000)
returns table (candidates integer, resolved integer, orders_touched integer, orders_skipped_locked integer,
  has_more boolean, next_after_order_id bigint, orders_examined integer, stop_reason text, skipped_order_ids bigint[])
language plpgsql volatile
set search_path = pg_catalog, pg_temp set work_mem = '4MB' set hash_mem_multiplier = 2
as $$
declare
  v_ids   bigint[];
  v_ns    integer[];
  v_n     integer := 0;
  v_lines bigint := 0;
  v_reason text := 'done';
  v_batch bigint[];
  h record;
begin
  if p_company is null or p_mall is null or p_after_order_id is null or p_max_orders is null or p_max_lines is null then
    raise exception 'reresolve_bad_args: 会社・モール・p_after_order_id・上限は null にできない (p_before_order_id だけ null = 上限なし)' using errcode = '22023';
  end if;
  if p_max_orders not between 1 and 5000 or p_max_lines not between 1 and 20000 or p_after_order_id < 0 then
    raise exception 'reresolve_bad_limits: p_max_orders は 1〜5000・p_max_lines は 1〜20000・p_after_order_id は 0 以上 (% / % / %)', p_max_orders, p_max_lines, p_after_order_id using errcode = '22023';
  end if;
  if p_before_order_id is not null and p_before_order_id <= p_after_order_id then
    raise exception 'reresolve_bad_range: p_before_order_id (%) は p_after_order_id (%) より大きく (high-water が bigint の最大値なら null)', p_before_order_id, p_after_order_id using errcode = '22023';
  end if;

  select coalesce(array_agg(c.order_id order by c.order_id), '{}'), coalesce(array_agg(c.n order by c.order_id), '{}')
    into v_ids, v_ns
    from (select k.order_id,
                 (select count(*)::integer from core.order_lines l
                   where l.order_id = k.order_id and l.removed_at is null and l.listing_id is null and l.sku_id is null and l.unresolved_code is not null) as n
            from (select t.order_id from ops.reresolve_retry_orders t
                   where t.company_id = p_company and t.mall = p_mall and t.order_id > p_after_order_id
                     and (p_before_order_id is null or t.order_id < p_before_order_id)
                   order by t.order_id
                   limit p_max_orders + 1) k) c;

  for i in 1 .. cardinality(v_ids) loop
    if v_n >= p_max_orders then v_reason := 'max_orders'; exit; end if;
    if v_n > 0 and v_lines + v_ns[i] > p_max_lines then v_reason := 'max_lines'; exit; end if;
    v_n := v_n + 1; v_lines := v_lines + v_ns[i];
  end loop;
  v_batch := v_ids[1:v_n];

  select * into h from core._reresolve_order_batch(p_company, p_mall, v_batch, true);
  candidates := h.candidates; resolved := h.resolved; orders_touched := h.orders_touched;
  orders_skipped_locked := cardinality(h.skipped_order_ids); skipped_order_ids := h.skipped_order_ids;
  orders_examined := v_n;
  next_after_order_id := case when v_n > 0 then v_batch[v_n] else p_after_order_id end;
  stop_reason := v_reason; has_more := (v_reason <> 'done');
  return next;
end $$;
comment on function core.reresolve_order_lines_retry(smallint, text, bigint, bigint, integer, integer) is
  'reresolve の retry の表 (skip した注文) を order_id の順に 1 batch だけ流す (D-60 1b-0r)。p_before_order_id = 周回の high-water + 1 (high-water が bigint の最大値なら null)。窓の外の古い注文も流す';

-- ─── 5. 権限 ───
revoke execute on function core._reresolve_order_batch(smallint, text, bigint[], boolean) from public;
revoke execute on function core.reresolve_order_lines(smallint, text, date, date, bigint, integer, integer) from public;
revoke execute on function core.reresolve_order_lines_retry(smallint, text, bigint, bigint, integer, integer) from public;
do $$
begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant select on ops.reresolve_retry_orders, ops.reresolve_backlog_windows, ops.reresolve_retry_cursor to watcher';
  end if;
end $$;
