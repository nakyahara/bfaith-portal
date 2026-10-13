-- 0076: 「NE から消えた → やめる」(0070・0072) の「3 日続けて無い」を、中原さんが決めた SKU だけ期限つきで短くする明示の許可 (2026-10-13 夜 中原さんの決定 B =
--       色違いの登録 (代表の正本の切替 = widen) を 10/15 から 10/14 に 1 日前倒し)
-- 🚨 前提 = 0072 (ops.sku_gone_from_ne_check の今の版 = 0070 の版 + 🆕 0072 の行)。0073〜0075 はこの関数に触らない
--
-- なぜ: widen には代表のずれ 0 件が要る。今のずれ 18 件 (drift-list = 全部 parent_incomparable・NE の代表なし・not_in_ne・登録 available) は 0070 の
--   「NE から消えた」(完全な NE の取得に JST の取得の日ごとに 1・3 日続けて無い) でやめる (cancelled) 予定だが、今は全部 1 / 3 日 (ほかの止まる理由は無い)。
--   1 日前倒しのため、中原さんが決めた SKU だけ、続けて無い日数の下限を 3 → 1 (〜3) にする。**ほかの決まりは全部そのまま**
--   (完全で信用できる取得・一番新しい取得が 36 時間の中・その取得にも無い・今の NE のコードの台帳にも無い・単品かセット・生きた NE 登録の CSV なし・
--   構成品 / 構成の依頼 / 出品の構成 (削除済みも) / 親に使われていない・セット自身の構成の依頼なし)
-- なにを:
--   1. ops.sku_gone_from_ne_day_overrides (許可: SKU・必要な日数 1〜3・だれ・理由・作った時刻・期限 (作った時刻 + 1〜14 日)・request_id・DB のロール)。
--      追記だけ (変えない・消さない・TRUNCATE しない)・書くのは下の関数だけ (取引の中の印)。期限切れの許可は効かない (= 3 日に戻る)。同じ SKU に何件あっても
--      効くのは期限の中の一番新しい許可 1 つ
--   2. ops.add_sku_gone_day_overrides(request_id, 人, 理由, コードの配列, 日数, 有効な日数) = 許可を足す (security definer・運用のロール master_ops だけに実行・
--      1〜100 件を 1 つの取引で全部か何も無いか・コードは Company DB の業務で使える単品 / セット・同じ request_id の押し直し = 前の答え・違う中身 = request_id_reused)
--   3. ops.sku_gone_from_ne_check (0072 の版から写して 🆕 0076 の行だけ足した): 期限の中の許可がある SKU は「3 日」の代わりに許可の日数。
--      答えと根拠 (evidence) に day_override (許可の番号・日数・ふだんの日数・だれ・理由・時刻・期限) を足す = CLI の計画のハッシュ・登録の状態の履歴に許可の事実が入る
--   4. ops.sku_gone_day_override_active(sku_id) = 効いている許可 (読むだけ・部品・だれにも渡さない)
-- 🚨 この migration は商品・登録の状態の値を何も変えない (許可の表は空)。許可を足す・やめるのは中原さんの OK の後に CLI で
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp・public の実行権を外す。
--    ロール (scripts/company-db/create-master-edit-roles.mjs) の流し直しで master_ops に 2. の実行を足す

-- ═══════════ 1. 許可の表 ═══════════
create table ops.sku_gone_from_ne_day_overrides (
  override_id   bigint generated always as identity primary key,
  company_id    smallint not null default 1 references core.companies,
  sku_id        bigint not null,
  required_days integer not null check (required_days between 1 and 3),
  actor_id      text not null check (pg_catalog.length(actor_id) between 1 and 320),
  reason        text not null check (pg_catalog.length(reason) between 1 and 200),
  request_id    uuid not null,
  db_user       text not null,
  created_at    timestamptz not null default pg_catalog.clock_timestamp(),
  expires_at    timestamptz not null,
  foreign key (company_id, sku_id) references core.skus (company_id, sku_id),
  unique (request_id, sku_id),
  constraint ck_sgdo_expires check (expires_at > created_at and expires_at <= created_at + interval '14 days')
);
create index ix_sgdo_sku on ops.sku_gone_from_ne_day_overrides (sku_id, created_at desc);
comment on table ops.sku_gone_from_ne_day_overrides is '「NE から消えた → やめる」の続けて無い日数の下限を、中原さんが決めた SKU だけ期限つきで短くする許可 (0076)。追記だけ・書くのは ops.add_sku_gone_day_overrides だけ';

create function ops.guard_sku_gone_day_overrides() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if tg_op <> 'INSERT' then raise exception 'gone_day_override: 許可の行は変えない・消さない (追記だけ)' using errcode = '42501'; end if;
  if coalesce(pg_catalog.current_setting('ops.gone_day_override_protocol', true), '') is distinct from '1' then
    raise exception 'gone_day_override: 許可は ops.add_sku_gone_day_overrides の中でだけ書く' using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_sku_gone_day_overrides() from public;
create trigger trg_sku_gone_day_overrides_guard before insert or update or delete on ops.sku_gone_from_ne_day_overrides
  for each row execute function ops.guard_sku_gone_day_overrides();
create trigger trg_sku_gone_day_overrides_no_truncate before truncate on ops.sku_gone_from_ne_day_overrides
  for each statement execute function core.reject_mutation();

-- ═══════════ 4. 効いている許可 (読むだけ・部品) ═══════════
/** その SKU に今効いている許可 (期限の中の一番新しい 1 つ)。無い = 全部の列が null の行 */
create function ops.sku_gone_day_override_active(p_sku_id bigint) returns ops.sku_gone_from_ne_day_overrides
  language sql stable security definer set search_path = pg_catalog, pg_temp as $$
  select o.* from ops.sku_gone_from_ne_day_overrides o
   where o.sku_id = p_sku_id and o.company_id = 1 and o.expires_at > pg_catalog.clock_timestamp()
   order by o.created_at desc, o.override_id desc limit 1
$$;
revoke all on function ops.sku_gone_day_override_active(bigint) from public;

-- ═══════════ 2. 許可を足す ═══════════
/**
 * 許可を足す (1 つの取引・1〜100 件・全部か何も無いか)。p_codes = Company DB の商品コード (code_norm で探す)・p_days = 必要な日数 (1〜3)・
 *   p_valid_days = 期限までの日数 (1〜14)。人 (メール)・理由 (200 字まで・改行なし) が要る。
 *   どのコードも: Company DB にある・会社 1・業務で使える (available)・単品かセット。1 つでも違えば invalid_input (何も書かない)。
 *   同じ request_id の押し直し = 前の答え (replayed)・違う人・中身で同じ request_id = request_id_reused
 *   戻り値 = { ok, request_id, required_days, expires_at, overrides: [{ override_id, sku_id, code }] }
 * 呼べるロール = master_ops (CLI scripts/company-db/master-gone-day-override.mjs) だけ。🚨 人 (actor_id) は呼び手が言う値 (DB では確かめられない)。DB が残すのは db_user
 */
create function ops.add_sku_gone_day_overrides(p_request_id uuid, p_actor_id text, p_reason text, p_codes text[], p_days integer, p_valid_days integer) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_reason  text := pg_catalog.btrim(coalesce(p_reason, ''));
  v_now     timestamptz := pg_catalog.clock_timestamp();
  v_bad     text;
  v_prev    jsonb;
  v_rows    jsonb;
  v_n       integer;
begin
  if p_request_id is null then raise exception 'invalid_input: request_id が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, v_reason) is not null or pg_catalog.length(v_reason) = 0 or p_actor_id !~ '^[^[:space:]@]+@[^[:space:]@]+$' then
    raise exception 'invalid_input: 人 (メール) と理由 (200 字まで・改行なし) が要る' using errcode = '22023';
  end if;
  if p_days is null or p_days not between 1 and 3 then raise exception 'invalid_input: 日数は 1〜3' using errcode = '22023'; end if;
  if p_valid_days is null or p_valid_days not between 1 and 14 then raise exception 'invalid_input: 有効な日数は 1〜14' using errcode = '22023'; end if;
  if p_codes is null or pg_catalog.cardinality(p_codes) not between 1 and 100 or pg_catalog.array_position(p_codes, null) is not null then
    raise exception 'invalid_input: コードは 1〜100 件' using errcode = '22023';
  end if;
  if (select pg_catalog.count(distinct core.norm_code(c)) from pg_catalog.unnest(p_codes) c) <> pg_catalog.cardinality(p_codes) then
    raise exception 'invalid_input: 同じコードが 2 回ある' using errcode = '22023';
  end if;
  -- 押し直し (同じ request_id) = 前の答え。中身 (人・理由・日数・期限の日数・コードの集合) が違えば request_id_reused
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.sku_gone_day_override:' || p_request_id::text, 0));
  if exists (select 1 from ops.sku_gone_from_ne_day_overrides o where o.request_id = p_request_id) then
    if exists (select 1 from ops.sku_gone_from_ne_day_overrides o where o.request_id = p_request_id
                and (o.actor_id is distinct from p_actor_id or o.reason is distinct from v_reason or o.required_days is distinct from p_days
                     or o.expires_at - o.created_at is distinct from pg_catalog.make_interval(days => p_valid_days)))
       or (select pg_catalog.array_agg(k.code_norm order by k.code_norm) from ops.sku_gone_from_ne_day_overrides o join core.skus k on k.sku_id = o.sku_id where o.request_id = p_request_id)
          is distinct from (select pg_catalog.array_agg(core.norm_code(c) order by core.norm_code(c)) from pg_catalog.unnest(p_codes) c) then
      raise exception 'request_id_reused: request_id % はほかの人・中身で使われている', p_request_id using errcode = '23505';
    end if;
    select pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'required_days', p_days, 'replayed', true,
             'expires_at', pg_catalog.min(o.expires_at),
             'overrides', pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('override_id', o.override_id::text, 'sku_id', o.sku_id::text, 'code', k.code) order by k.code_norm))
      into v_prev from ops.sku_gone_from_ne_day_overrides o join core.skus k on k.sku_id = o.sku_id where o.request_id = p_request_id;
    return v_prev;
  end if;
  select pg_catalog.string_agg(x.c || ' (' || x.why || ')', '・' order by x.c) into v_bad
    from (select c,
                 case when k.sku_id is null then 'Company DB に無い' when k.company_id <> 1 then '会社 1 でない'
                      when r.state is distinct from 'available' then '登録の状態が ' || coalesce(r.state, '行が無い')
                      when k.sku_kind not in ('single', 'set') then '例外の SKU' end as why
            from pg_catalog.unnest(p_codes) c
            left join core.skus k on k.company_id = 1 and k.code_norm = core.norm_code(c)
            left join ops.master_registrations r on r.sku_id = k.sku_id) x
   where x.why is not null;
  if v_bad is not null then raise exception 'invalid_input: 許可を足せないコード: %', pg_catalog.left(v_bad, 1500) using errcode = '22023'; end if;
  perform pg_catalog.set_config('ops.gone_day_override_protocol', '1', true);
  with ins as (
    insert into ops.sku_gone_from_ne_day_overrides (company_id, sku_id, required_days, actor_id, reason, request_id, db_user, created_at, expires_at)
      select 1, k.sku_id, p_days, p_actor_id, v_reason, p_request_id, v_db_user, v_now, v_now + pg_catalog.make_interval(days => p_valid_days)
        from pg_catalog.unnest(p_codes) c join core.skus k on k.company_id = 1 and k.code_norm = core.norm_code(c)
      returning override_id, sku_id)
  select pg_catalog.count(*)::integer, pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('override_id', ins.override_id::text, 'sku_id', ins.sku_id::text, 'code', k.code) order by k.code_norm)
    into v_n, v_rows from ins join core.skus k on k.sku_id = ins.sku_id;
  perform pg_catalog.set_config('ops.gone_day_override_protocol', '', true);
  if v_n <> pg_catalog.cardinality(p_codes) then raise exception 'invalid_input: 許可の行の数が合わない (%/%)', v_n, pg_catalog.cardinality(p_codes) using errcode = '22023'; end if;
  return pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'required_days', p_days, 'expires_at', v_now + pg_catalog.make_interval(days => p_valid_days),
    'overrides', v_rows);
end $$;
revoke all on function ops.add_sku_gone_day_overrides(uuid, text, text, text[], integer, integer) from public;

-- ═══════════ 3. やめてよいかの確かめ (0072 の版から写して 🆕 0076 の行だけ足した) ═══════════
/**
 * SKU を「NE から消えた」でやめてよいか (0070・0072 の契約のまま)。🆕 0076: 期限の中の許可 (ops.sku_gone_from_ne_day_overrides) がある SKU は、
 *   続けて無い日数の下限 = 許可の日数 (ふだん = ops.ne_gone_required_runs() = 3)。ほかの決まりは全部そのまま。答えと根拠に day_override を足す
 */
create or replace function ops.sku_gone_from_ne_check(p_sku_id bigint) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  a        ops.master_ne_absence;
  g        ops.master_parent_gate_results;
  f        ops.master_parent_gate_results;
  m        ops.master_ne_code_mark;
  am       ops.master_ne_absence_mark;
  v_req    integer := ops.ne_gone_required_runs();
  v_hours  integer := ops.ne_gone_fresh_hours();
  v_p      jsonb := '[]'::jsonb;
  v_streak integer := 0;
  v_txt    text;
  v_ok     boolean;
  v_ovr    ops.sku_gone_from_ne_day_overrides;   -- 🆕 0076: 効いている日数の許可
  v_res    jsonb;                                -- 🆕 0076
  v_dov    jsonb;                                -- 🆕 0076
begin
  select k.sku_id, k.company_id, k.code, k.code_norm, k.sku_kind, k.product_id, r.state as reg into s
    from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id where k.sku_id = p_sku_id;
  if not found then
    return pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'code', null, 'ok', false,
      'problems', pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_sku', 'text', 'Company DB にその SKU が無い')));
  end if;
  -- 🆕 0076 ここから: 中原さんが決めた SKU の日数の許可 (期限の中の一番新しい 1 つ) = 続けて無い日数の下限を許可の日数に (ほかの決まりはそのまま)
  v_ovr := ops.sku_gone_day_override_active(s.sku_id);
  if v_ovr.override_id is not null then v_req := v_ovr.required_days; end if;
  -- 🆕 0076 ここまで
  if s.company_id <> 1 then v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_company', 'text', '会社 1 の SKU でない')); end if;
  if s.reg is distinct from 'available' then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_available',
      'text', pg_catalog.format('登録の状態が %s (やめられるのは「業務で使える」(available) の商品だけ)', coalesce(s.reg, '行が無い'))));
  end if;
  if s.sku_kind is distinct from 'single' then
    if s.sku_kind is distinct from 'set' then   -- 🆕 0072: セットも「NE から消えた」でやめられる (決定 10/12 Q4 b・やめるときに構成を外す)
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'not_single', 'text', pg_catalog.format('%s はこの操作でやめない (単品だけ)', case s.sku_kind when 'set' then 'セット' else '例外の SKU' end)));
    end if;   -- 🆕 0072
  end if;
  -- NE の完全な取得 (照合 ② の記録の回) に続けて無いか。🆕 #1683 Codex R1 High 1: 「一番新しい」= 数えた一番新しい取得 (取得の時刻の順・ops.master_ne_absence_mark)・
  --   新しさ = 実際の取得の完了の時刻 (単品とセットの古い方)。記録の時刻 (created_at)・記録の順 (result_id) では決めない
  select * into am from ops.master_ne_absence_mark x where x.id = 1;
  select * into g from ops.master_parent_gate_results x where x.result_id = am.result_id;
  if g.result_id is null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'no_trusted_fetch', 'text', '完全で信用できる NE の取得の記録 (照合 ② の代表の数え) がまだ無い'));
  else
    if least(g.products_complete_at, g.setproducts_complete_at) < pg_catalog.clock_timestamp() - pg_catalog.make_interval(hours => v_hours) then
      v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'fetch_old',
        'text', pg_catalog.format('一番新しい完全な NE の取得 (照合の回 %s・取得の完了 %s) が %s 時間より古い (今朝の照合 ② を待つ)', g.compare_run_id,
          pg_catalog.to_char(least(g.products_complete_at, g.setproducts_complete_at) at time zone 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI'), v_hours)));
    end if;
    select * into a from ops.master_ne_absence x where x.company_id = 1 and x.code_norm = s.code_norm;
    if a.code_norm is null or a.last_absent_result_id <> g.result_id then
      v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'in_ne_latest',
        'text', pg_catalog.format('一番新しい完全な NE の取得 (照合の回 %s) にある (か、無いと言えない) = NE から消えていない', g.compare_run_id)));
    else
      v_streak := a.streak;
      select * into f from ops.master_parent_gate_results x where x.result_id = a.first_absent_result_id;
      if a.streak < v_req then
        v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'absent_short',
          'text', pg_catalog.format('完全な NE の取得に続けて無いのが %s 日 (%s 日で、やめられる・1 日に何回取得しても 1 日)', a.streak, v_req)));
      end if;
    end if;
  end if;
  -- 今の NE のコードの台帳 (0041・最新の照合の回の入れ替え) にあれば断る (安全側の守りだけ)
  select * into m from ops.master_ne_code_mark x where x.id = 1;
  if exists (select 1 from ops.master_ne_codes c where c.code_norm = s.code_norm) then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'in_ne_codes',
      'text', pg_catalog.format('今の NE のコードの台帳 (照合の回 %s) にある', coalesce(m.compare_run_id, '不明'))));
  end if;
  -- 使われていないか
  select pg_catalog.string_agg('#' || i.export_id::text || ' ' || i.state, '・' order by i.item_id) into v_txt
    from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state in ('built', 'issued', 'import_declared', 'partial');
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'live_file', 'text', pg_catalog.format('終わっていない NE 登録の CSV の品目がある (%s)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select p.code from core.sku_components c join core.skus p on p.sku_id = c.parent_sku_id where c.child_sku_id = s.sku_id order by p.code limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'set_component', 'text', pg_catalog.format('ほかのセットの構成品になっている (%s)', v_txt)));
  end if;
  select pg_catalog.string_agg(x.code, '・' order by x.code) into v_txt
    from (select distinct p.code from ops.sku_component_requests q join core.skus p on p.sku_id = q.set_sku_id
           where q.status = 'open' and exists (select 1 from pg_catalog.jsonb_array_elements(q.rows) e where (e ->> 'sku_id') = s.sku_id::text) order by p.code limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'component_request', 'text', pg_catalog.format('開いている構成の依頼に入っている (セット %s)', v_txt)));
  end if;
  -- 🆕 0072 ここから: セット自身の開いている構成の依頼 = 先に閉じる (やめるときに構成を外す・依頼が残ると夜間の昇格で構成が戻る)
  if s.sku_kind = 'set' and exists (select 1 from ops.sku_component_requests q where q.set_sku_id = s.sku_id and q.status = 'open') then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'own_component_request', 'text', 'このセットの開いている構成の依頼がある (先に構成の依頼を閉じる)'));
  end if;
  -- 🆕 0072 ここまで
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select l.mall || ':' || l.listing_code as w from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = s.sku_id and l.status <> 'deleted' order by 1 limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'listing_component', 'text', pg_catalog.format('販路の出品の構成に使われている (%s)', v_txt)));
  end if;
  -- 🆕 #1683 Codex R3 High: 削除済み (status = deleted) の出品に残る構成も断る = 後で出品が戻る (夜間ロードが active に戻す) と、やめた商品が使われてしまう。
  --   どの出品か分かるように出す (先に構成を外す = Amazon の対応の墓標・モールの対応を直す)
  select pg_catalog.string_agg(x.w, '・' order by x.w) into v_txt
    from (select l.mall || ':' || l.listing_code as w from core.listing_components lc join core.listings l on l.listing_id = lc.listing_id
           where lc.sku_id = s.sku_id and l.status = 'deleted' order by 1 limit 6) x;
  if v_txt is not null then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'listing_component_deleted',
      'text', pg_catalog.format('削除済みの出品 %s の構成に入っている (出品が戻るとやめた商品が使われる = 先にその出品の構成から外す)', v_txt)));
  end if;
  if s.product_id is not null and exists (select 1 from core.products c where c.parent_product_id = s.product_id and c.product_id <> s.product_id) then
    v_p := v_p || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('reason', 'parent_of_others', 'text', 'この商品がほかの商品の代表 (親) になっている'));
  end if;
  v_ok := pg_catalog.jsonb_array_length(v_p) = 0;
  -- 🆕 0076 ここから: 許可があるとき = 答えと根拠 (evidence) に day_override を足す (CLI の計画のハッシュ・登録の状態の履歴に許可の事実が入る)。式は下の return と同じ
  if v_ovr.override_id is not null then
    v_res := pg_catalog.jsonb_build_object('sku_id', s.sku_id::text, 'code', s.code, 'ok', v_ok, 'problems', v_p, 'reg_state', s.reg, 'sku_kind', s.sku_kind,
      'required_runs', v_req, 'fresh_hours', v_hours, 'streak', v_streak,
      'latest', case when g.result_id is null then null else pg_catalog.jsonb_build_object('result_id', g.result_id::text, 'compare_run_id', g.compare_run_id,
          'products_complete_at', g.products_complete_at, 'setproducts_complete_at', g.setproducts_complete_at, 'fetch_day', am.fetch_day) end,
      'absent_since', case when f.result_id is null then null else pg_catalog.jsonb_build_object('result_id', f.result_id::text, 'compare_run_id', f.compare_run_id,
          'products_complete_at', f.products_complete_at, 'day', a.first_absent_day) end,
      'evidence', case when v_ok then pg_catalog.jsonb_build_object('rule', 'gone_from_ne_v2', 'required_days', v_req, 'streak_days', v_streak,
          'first_absent', pg_catalog.jsonb_build_object('result_id', f.result_id::text, 'compare_run_id', f.compare_run_id, 'products_complete_at', f.products_complete_at, 'day', a.first_absent_day),
          'last_absent', pg_catalog.jsonb_build_object('result_id', g.result_id::text, 'compare_run_id', g.compare_run_id, 'products_complete_at', g.products_complete_at,
            'setproducts_complete_at', g.setproducts_complete_at, 'day', a.last_absent_day, 'ne_generation_id', g.ne_generation_id),
          'ne_codes_run', m.compare_run_id) end);
    v_dov := pg_catalog.jsonb_build_object('override_id', v_ovr.override_id::text, 'required_days', v_ovr.required_days, 'normal_days', ops.ne_gone_required_runs(),
      'actor_id', v_ovr.actor_id, 'reason', v_ovr.reason, 'request_id', v_ovr.request_id::text, 'created_at', v_ovr.created_at, 'expires_at', v_ovr.expires_at);
    return v_res || pg_catalog.jsonb_build_object('day_override', v_dov)
      || case when v_ok then pg_catalog.jsonb_build_object('evidence', (v_res -> 'evidence') || pg_catalog.jsonb_build_object('day_override', v_dov)) else '{}'::jsonb end;
  end if;
  -- 🆕 0076 ここまで
  return pg_catalog.jsonb_build_object('sku_id', s.sku_id::text, 'code', s.code, 'ok', v_ok, 'problems', v_p, 'reg_state', s.reg, 'sku_kind', s.sku_kind,
    'required_runs', v_req, 'fresh_hours', v_hours, 'streak', v_streak,
    'latest', case when g.result_id is null then null else pg_catalog.jsonb_build_object('result_id', g.result_id::text, 'compare_run_id', g.compare_run_id,
        'products_complete_at', g.products_complete_at, 'setproducts_complete_at', g.setproducts_complete_at, 'fetch_day', am.fetch_day) end,
    'absent_since', case when f.result_id is null then null else pg_catalog.jsonb_build_object('result_id', f.result_id::text, 'compare_run_id', f.compare_run_id,
        'products_complete_at', f.products_complete_at, 'day', a.first_absent_day) end,
    'evidence', case when v_ok then pg_catalog.jsonb_build_object('rule', 'gone_from_ne_v2', 'required_days', v_req, 'streak_days', v_streak,
        'first_absent', pg_catalog.jsonb_build_object('result_id', f.result_id::text, 'compare_run_id', f.compare_run_id, 'products_complete_at', f.products_complete_at, 'day', a.first_absent_day),
        'last_absent', pg_catalog.jsonb_build_object('result_id', g.result_id::text, 'compare_run_id', g.compare_run_id, 'products_complete_at', g.products_complete_at,
          'setproducts_complete_at', g.setproducts_complete_at, 'day', a.last_absent_day, 'ne_generation_id', g.ne_generation_id),
        'ne_codes_run', m.compare_run_id) end);
end $$;
revoke all on function ops.sku_gone_from_ne_check(bigint) from public;

-- ─── 権限。master_ops には scripts/company-db/create-master-edit-roles.mjs の流し直しで付ける (表は読ませない・関数の実行だけ) ───
--   watcher = 表を読むだけ (ops の全部の表の select = create-watch-roles.mjs の既定の権限)
