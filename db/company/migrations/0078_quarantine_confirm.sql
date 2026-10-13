-- 0078: 要確認 (quarantined = NE で直接作られた商品) を「確認済み」(ne_confirmed = NE 確認済み) にする DB の道 (2026-10-13・Company DB構想 22 の Q-1)
--       中原さんの決定 10/13 夜 = Q1〜Q5 全部 a (Q2 a = 推奨保有月数が要る・Q3 a = NE と違う欄はマスタの判断で決めてから・Q4 a = 人が見て押す (自動にしない))
-- 🚨 番号は仮置き (マージの順で付け替える・中身は番号に依らない)。前提 = 0073 (ops.transition_sku_registration の今の版)・0072 (約束の操作の CHECK の今の版)・
--    0067 (ops.v_ne_reg_targets の今の版・まとまりの予約 ops.variation_group_codes・鍵の部品)・0065 / 0067 (ops.ne_reg_canonical・ops.ne_reg_compare)・
--    0070 (ops.ne_gone_fresh_hours)・0053 (照合 ② の観測・封・ops.reg_write_gate・ops.close_reg_write)・0032 / 0034 (判断の台帳)
--
-- なぜ: 要確認の商品 (本番 10/13 夕 = 42 件) を「確認済み」にする道が無い (0052 の地図に quarantined > ne_confirmed はあるが、関数が not_ready で止めていた)。
--   原価・仕入先が空のまま、新しいセットの構成品・NE 登録の CSV の構成品に使えない
-- なにを:
--   1. 照合 ② の確かめ待ち (ops.v_ne_reg_targets) に要確認を全部 (単品もセットも・親の有無・持ち主を問わず) 入れる = 毎朝の照合 ② が要確認の NE の観測を残して封をする。
--      今の「親の無い単品・products.parent が company のときだけ」(0067・代表の採用の根拠) は含まれる = 壊れない。新しい定期実行は作らない (今の daily-sync の照合の中)。
--      コードが 80 字を超える SKU は入れない (観測の表の CHECK = 80 字まで。入れると照合の朝の観測の記録が全部止まる) = 確かめで「観測が無い」と出る
--   2. ops.quarantine_confirm_check(SKU) = 確認済みにしてよいかの表 (読むだけ・security definer)。返す形 = { sku_id, code, ok, items: [{ key, ok, cdb, ne, text, fix }],
--      evidence (ok のときだけ), check_hash }。fix = 直し方の組 (one = 1 件の画面で直す / ne_fill = NE の値で埋める / decision = マスタの判断で決める /
--      after_compare = 明日の照合の後に)。確かめること (設計 22 §3):
--        ① 必須の欄 (新商品の登録 0060・0061 と同じ): 単品 = 名前 (コードのままでない)・標準売価・税率と税区分・取扱・売上分類 (1〜4)・推奨保有月数 (Q2 a)・
--           代表の仕入先 (ちょうど 1 つ・NE の形)・今日の原価 (決まった値・1 円以上)。セット = 名前・標準売価・構成品 (1 つ以上・全部 NE 確認済み以降)・
--           開いている構成の依頼が無い・税率と取扱と売上分類 = 構成品から導いた値 (売上分類は上書きでも)・推奨保有月数・今日の原価 (構成品の合計 set_calc か例外原価)
--        ② NE と合っている: 最新の封をした照合 ② の回 = NE の元のコードの印の回・取得が ops.ne_gone_fresh_hours() 時間の中。その回の観測に、ある・信用できる・区分が同じ。
--           欄ごと (名前・仕入先・原価・売価・税率・取扱・代表 / 名前・売価・構成の子ごと): 今の社内の値 = その回の観測 か、違うなら「今の社内の値」に結びついた
--           承認済みの判断 (その回に見えた候補で、承認した組 print.c ↔ print.n = 今の社内の値 ↔ 今の NE の値・差を残す / NE を直す / 社内を直す・
--           直す承認の完了の後にまた出た = 再発はだめ) があるときだけ (Q3 a)。照合の後に値を変えた (候補が無い)・承認の後に社内か NE の値が変わった = だめ
--           (明日の照合の後に) (#1715 Codex R1 High・R2 High 1 / 2)
--        ③ 代表 (色違いのまとまり): 親のある単品 = まとまりの記録 (予約) がある。親の無い単品で NE に代表がある = 先に代表の採用
--        ④ そのほか: 生きている NE 登録の CSV の品目・NE から消す依頼が無い
--      人が見る欄 (名前と構成の数・売上分類) は画面の「見たら ✓」(DB は ✓ の有無で断らない)
--   3. ops.quarantine_confirm_list(コード | null) = 一覧 (読むだけ・null = 要確認の全部 (500 件を超えたら too_many)・コードは 500 件まで) = 一括の inspect・毎朝のまとめ (Q-3・Q-4) の材料
--   4. ops.confirm_quarantined_sku(request_id, 人, 理由, 持ち主表, SKU, check_hash) = 確認済みにする (security definer・1 件 = 1 取引)。
--      同じ request_id の押し直し = 前の答え。鍵: 段階 → マスタの書き込み (共有) → SKU → 親子 (共有) → NE のコード (共有) → (親があれば) まとまりのコード → まとまり →
--      登録の状態の行 (for update) → 構成品の登録の状態の行 (for share) → 判断の候補の行 (for share)。鍵の後に 2. をもう一度: ok でない = not_confirmable (理由を全部)・
--      check_hash が今の表と違う (画面を開いた後に値か照合が変わった) = stale。約束 (操作 quarantine_confirm・この関数だけが書く) → 登録の状態 → 保存の記録 done
--   5. ops.transition_sku_registration (0073 の版を写して 🆕 0078 のかたまりだけ足した): quarantined → ne_confirmed は約束 quarantine_confirm の中・同じ SKU・人・request_id・
--      人 (human) のときだけ。根拠は呼び手から受けず、DB が 2. の表から作る (照合の回・観測のハッシュ・check_hash・使った判断・まとまり)。2. が ok で、check_hash が
--      約束の check_hash と同じときだけ。運用のロール・持ち主のロールが直に呼んでも断る (0070 の「NE から消えた」と同じ型)。🚨 0073 の本文の not_ready の行は残る
--      (このかたまりが先に終わる = 届かない) = 0073 との差はこのかたまりだけ (試験が機械で比べる)
--   6. 約束の操作 (ops.master_write_sessions・ops.master_edit_requests の CHECK) に quarantine_confirm (0072 の版から写して 1 つ足しただけ)。
--      この関数は商品の値 (core.*) を書かない = ops.master_write_allowed に行は足さない (値を埋めるのは今の保存 saveSku = Q-2・Q-3)
-- 変えない: 地図 (ops.registration_transition_allowed)・0070 の「NE から消えた」(available だけが対象 = ne_confirmed は NE から消えてもその道でやめられない =
--   設計 22 §2 の既にある穴・④ で直す)・Amazon の構成 (0073)・代表の門 (0068)・夜間ロード (登録の状態に触らない)
-- 🚨 この migration は商品・登録の状態の値を何も変えない (確かめ待ちの view が広がるだけ = 次の朝の照合 ② から要確認の観測が残る)
-- 🚨 security definer の関数 = 一時の表を使わない・search_path = pg_catalog, pg_temp (名前は全部 schema つき)・public の実行権を外す。
--    ロール (scripts/company-db/create-master-edit-roles.mjs) の流し直しで master_edit (画面・名簿 MASTER_EDITORS の人 = Q1 a) に 2. 3. 4. の実行を足す。
--    watcher = 2. 3. の実行 (読むだけ・下の do で付ける・create-watch-roles.mjs の流し直しでも付く)
--   7. (#1715 Codex R3) 判断の承認の時の組 (ops.master_decision_approval_prints・承認の出来事の後の trigger が書く) = 確認は承認の時の組と今の組を照らす

-- ═══════════ 1. 照合 ② の確かめ待ちに要確認を全部 (0067 の版から写して、要確認の行の条件だけ替えた) ═══════════
create or replace view ops.v_ne_reg_targets as
  select i.item_id, i.export_id, i.sku_id, i.code_norm, i.sku_kind, i.state
    from ops.ne_reg_export_items i
   where i.state in ('issued', 'import_declared', 'partial')
  union all
  -- 🆕 0078: NE で直接作られた商品 (quarantined) を全部 (単品もセットも・親の有無・products.parent の持ち主を問わず) = 翌朝の照合が NE の観測を残す
  --   (確認済みにする根拠 = ops.quarantine_confirm_check・代表の採用の根拠 = 0067 の部分集合も含む)。コードが 80 字を超える SKU は入れない (観測の表の CHECK)
  select null::bigint, null::bigint, s.sku_id, s.code_norm, s.sku_kind, 'quarantined'::text
    from core.skus s join ops.master_registrations r on r.sku_id = s.sku_id
   where r.state = 'quarantined' and s.sku_kind in ('single', 'set') and pg_catalog.length(s.code_norm) between 1 and 80
     -- #1715 Codex R1 M2: 上の枝 (生きている NE 登録の CSV の品目) にある SKU は重ねない (同じ SKU を 2 行にしない)
     and not exists (select 1 from ops.ne_reg_export_items i where i.sku_id = s.sku_id and i.state in ('issued', 'import_declared', 'partial'));
comment on view ops.v_ne_reg_targets is '翌朝の照合 ② が NE の完全な取得の値を送る商品 (0053・0063 から配っただけの品目も・0078 から要確認 (quarantined) の単品とセットの全部)';

-- ═══════════ 1b. 承認の時の組 (#1715 Codex R3 High) ═══════════
-- 判断の承認 (ops.master_decision_events の approved) を書いた時の候補の print の組 (社内の値 c・NE の値 n) を、承認の出来事ごとに残す。
--   書くのは承認の出来事の insert の後の trigger だけ (どの書き手も = 判断の画面 apps/master-decisions/decide.mjs の applyDecisions・持ち主の手)。
--   確認 (ops.quarantine_confirm_check) は「承認の時の組 = 今の組 (今日の社内の値 ↔ 最新の観測の値)」のときだけ承認を効かせる = 候補の行ではなく承認に結びつける。
--   組の記録が無い承認は効かせない。
-- 🚨 前からある承認 (0078 の前) = 下で、その候補の今の print から入れる。候補の print は 0032 の trg_mdc_guard が作った時から変えさせない (不変の列) =
--    承認の時の print と同じ (この表を作る取引の中で入れる)
create table ops.master_decision_approval_prints (
  event_id    bigint primary key references ops.master_decision_events (event_id),
  fingerprint text not null references ops.master_decision_candidates (fingerprint),
  print_c     jsonb not null,   -- 承認の時の社内の値 (候補の print.c・無い = JSON の null)
  print_n     jsonb not null,   -- 承認の時の NE の値 (候補の print.n・無い = JSON の null)
  recorded_at timestamptz not null default pg_catalog.clock_timestamp()
);
select core.make_append_only('ops', 'master_decision_approval_prints');
comment on table ops.master_decision_approval_prints is '判断の承認の時の候補の組 (社内の値・NE の値) (0078・#1715 Codex R3)。書くのは承認の出来事の後の trigger だけ。追記だけ';
create function ops.guard_master_decision_approval_prints() returns trigger language plpgsql set search_path = pg_catalog, pg_temp as $$
begin
  if coalesce(pg_catalog.current_setting('ops.approval_print_protocol', true), '') is distinct from '1' then
    raise exception '% は承認の出来事の trigger (ops.record_master_decision_approval_print) の中でだけ書く', tg_table_name using errcode = '42501';
  end if;
  return new;
end $$;
revoke all on function ops.guard_master_decision_approval_prints() from public;
create trigger trg_mdap_guard before insert on ops.master_decision_approval_prints for each row execute function ops.guard_master_decision_approval_prints();
create function ops.record_master_decision_approval_print() returns trigger
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
begin
  if new.kind = 'approved' then
    perform pg_catalog.set_config('ops.approval_print_protocol', '1', true);
    insert into ops.master_decision_approval_prints (event_id, fingerprint, print_c, print_n)
      select new.event_id, new.fingerprint, coalesce(c.print -> 'c', 'null'::jsonb), coalesce(c.print -> 'n', 'null'::jsonb)
        from ops.master_decision_candidates c where c.fingerprint = new.fingerprint;
    perform pg_catalog.set_config('ops.approval_print_protocol', '', true);
  end if;
  return null;
end $$;
revoke all on function ops.record_master_decision_approval_print() from public;
create trigger trg_mde_approval_print after insert on ops.master_decision_events for each row execute function ops.record_master_decision_approval_print();
-- 前からある承認 (候補の print は不変 = 承認の時と同じ)
select pg_catalog.set_config('ops.approval_print_protocol', '1', true);
insert into ops.master_decision_approval_prints (event_id, fingerprint, print_c, print_n)
  select e.event_id, e.fingerprint, coalesce(c.print -> 'c', 'null'::jsonb), coalesce(c.print -> 'n', 'null'::jsonb)
    from ops.master_decision_events e join ops.master_decision_candidates c on c.fingerprint = e.fingerprint
   where e.kind = 'approved'
  on conflict (event_id) do nothing;
select pg_catalog.set_config('ops.approval_print_protocol', '', true);

-- ═══════════ 2. 確認済みにしてよいかの表 (読むだけ) ═══════════
/** 表の 1 行 ({ key, ok, cdb, ne, text, fix })。fix = 直し方の組 (ok = null) */
create function ops.quarantine_confirm_item(p_key text, p_ok boolean, p_cdb jsonb, p_ne jsonb, p_text text, p_fix text) returns jsonb
  language sql immutable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('key', p_key, 'ok', coalesce(p_ok, false), 'cdb', coalesce(p_cdb, 'null'::jsonb), 'ne', coalesce(p_ne, 'null'::jsonb),
    'text', p_text, 'fix', case when coalesce(p_ok, false) then null else p_fix end))
$$;
revoke all on function ops.quarantine_confirm_item(text, boolean, jsonb, jsonb, text, text) from public;

/**
 * 要確認の商品を確認済み (NE 確認済み) にしてよいか。何も書かない (watcher の読むだけの取引でも動く = 鍵・行の鍵を取らない)。
 * 戻り値 = { sku_id (文字), code, sku_kind, reg_state, ok, items: [{ key, ok, cdb, ne, text, fix }], evidence (ok のときだけ), check_hash }
 *   key: state / code_length / kind / name / price / tax / handling / sales_class / reorder_months / supplier / cost / parent (単品) /
 *        components / component_request (セット) / ne_run / ne_observed / ne.<列> (照合 ② の答え・列 = name supplier cost price tax_rate handling parent / name price children・
 *        ne.other = 列に当たらない候補) / live_file
 *   check_hash = 表 (items)・根拠・SKU と商品の version・今日 (東京) のハッシュ = 画面が見た表と、押した時の表が同じか (確認の関数が比べる)
 */
create function ops.quarantine_confirm_check(p_sku_id bigint) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  s        record;
  v_today  date := (pg_catalog.now() at time zone 'Asia/Tokyo')::date;
  v_hours  integer := ops.ne_gone_fresh_hours();
  v_it     jsonb := '[]'::jsonb;
  v_cost   record;
  v_nsup   integer;
  v_sup    record;
  v_supc   text;
  v_txt    text;
  v_n      integer;
  v_rates  numeric[];
  v_class  integer[];
  v_tax    numeric;
  v_tclass text;
  v_hand   text;
  v_stop   boolean;
  v_sales  integer;
  v_all    boolean;
  v_sum    numeric;
  v_bad    text;
  v_run    text;
  v_mark   text;
  h        ops.ne_reg_compare_runs;
  v_rc     ops.ne_reg_compare_receipts;
  v_fresh  boolean := false;
  v_obs    jsonb;
  v_obsok  boolean := false;
  v_exp    jsonb;
  v_cmp    jsonb;
  c        record;
  v_dcol   text;
  v_unres  jsonb := '{}'::jsonb;   -- 列 → [効かない候補の文]
  v_appr   jsonb := '{}'::jsonb;   -- 列 (構成は 'children:<子>') → 今の社内の値に結びついた承認済みの判断
  v_cands  jsonb := '{}'::jsonb;   -- 列 → その回に候補があった
  v_cur    jsonb;                  -- 今の社内の値 (照合の読み方 = 候補の print.c と同じ形)
  v_kids   jsonb;                  -- 今の構成 (子の code_norm → 数量)
  v_tied   boolean;
  v_ccur   jsonb;                  -- 候補の列の今の社内の値
  v_ncur   jsonb;                  -- 候補の列の今の NE の値 (最新の封をした回の観測)
  v_diff   text[];
  v_used   jsonb := '[]'::jsonb;   -- 根拠: 使った判断・写した列
  v_cols   text[];
  k        text;
  e        jsonb;
  o        jsonb;
  v_same   boolean;
  v_gcode  text;
  v_ok     boolean;
  v_ev     jsonb;
  v_hash   text;
begin
  select k1.sku_id, k1.company_id, k1.code, k1.code_norm, k1.sku_kind, k1.name, k1.tax_rate, k1.tax_class, k1.handling, k1.handling_own, k1.standard_price_jpy,
         k1.reorder_months, k1.set_sales_class_override, k1.product_id, k1.version as sku_version, p.version as product_version, p.sales_class, p.parent_product_id,
         r.state as reg
    into s
    from core.skus k1 left join core.products p on p.product_id = k1.product_id left join ops.master_registrations r on r.sku_id = k1.sku_id
   where k1.sku_id = p_sku_id;
  if not found or s.company_id <> 1 then
    return pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'code', null, 'ok', false,
      'items', ops.quarantine_confirm_item('state', false, null, null, 'Company DB (会社 1) にその SKU が無い', 'one'), 'evidence', null, 'check_hash', null);
  end if;
  -- ─── 状態・区分 ───
  v_it := v_it || ops.quarantine_confirm_item('state', s.reg is not distinct from 'quarantined', pg_catalog.to_jsonb(s.reg), null,
    case when s.reg is not distinct from 'quarantined' then '要確認 (NE で直接作られた商品)'
         else pg_catalog.format('登録の状態が %s (確認済みにできるのは要確認だけ)', coalesce(s.reg, '行が無い')) end, 'one');
  -- #1715 Codex R1 L1: コードが 80 字を超える = 照合 ② の確かめ待ちに入らない (観測の表の CHECK) = いつまでも観測が来ない → 専用の行で直し方を出す
  v_it := v_it || ops.quarantine_confirm_item('code_length', pg_catalog.length(s.code_norm) between 1 and 80, pg_catalog.to_jsonb(pg_catalog.length(s.code_norm)), null,
    case when pg_catalog.length(s.code_norm) between 1 and 80 then 'コードの長さ (照合できる)'
         else pg_catalog.format('コードが %s 字 (80 字を超える) = 照合 ② で NE と照らせない = 確認済みにできない (NE と社内で短いコードの商品を作り直し、この要確認はやめる)', pg_catalog.length(s.code_norm)) end, 'one');
  v_it := v_it || ops.quarantine_confirm_item('kind', s.sku_kind in ('single', 'set'), pg_catalog.to_jsonb(s.sku_kind), null,
    case when s.sku_kind in ('single', 'set') then case s.sku_kind when 'single' then '単品' else 'セット' end else '例外の SKU は確認済みにしない' end, 'one');

  -- ─── ② NE と合っている: 最新の封をした照合の回と観測 (必須の欄の「NE の値で埋める」の材料にも使う) ───
  select m.compare_run_id into v_run from ops.ne_reg_compare_receipts rc join ops.master_compare_runs m on m.compare_run_id = rc.compare_run_id
   order by m.observed_at desc, m.compare_run_id desc limit 1;
  select x.compare_run_id into v_mark from ops.master_ne_code_mark x where x.id = 1;
  select * into h from ops.ne_reg_compare_runs x where x.compare_run_id = v_run;
  select * into v_rc from ops.ne_reg_compare_receipts x where x.compare_run_id = v_run;
  v_fresh := coalesce(v_run is not null and v_run = v_mark and h.compare_run_id is not null
             and least(h.products_at, h.sets_at) >= pg_catalog.now() - pg_catalog.make_interval(hours => v_hours), false);
  if v_run is not null then
    select x.observation into v_obs from ops.ne_reg_compare_observations x where x.compare_run_id = v_run and x.code_norm = s.code_norm;
  end if;
  v_obsok := coalesce(v_fresh and v_obs is not null and (v_obs -> 'present') = 'true'::jsonb and (v_obs -> 'trusted') = 'true'::jsonb and (v_obs ->> 'kind') is not distinct from s.sku_kind, false);
  -- 今の Company DB の値を NE の観測と同じ形に (0065 / 0067 の ops.ne_reg_canonical = 新商品の NE 登録の CSV と同じ作り方)
  v_exp := ops.ne_reg_canonical(p_sku_id, v_today) -> 'expected';
  if v_obsok and v_exp is not null then v_cmp := ops.ne_reg_compare(v_exp, v_obs); end if;
  v_it := v_it || ops.quarantine_confirm_item('ne_run', v_fresh,
    case when v_run is null then null else pg_catalog.jsonb_build_object('compare_run_id', v_run, 'ne_codes_run', v_mark,
      'fetched_at', case when h.compare_run_id is null then null else least(h.products_at, h.sets_at) end) end, null,
    case when v_run is null then '封をした照合 ② の回がまだ無い (明日の照合の後に)'
         when v_run is distinct from v_mark then pg_catalog.format('最新の封をした照合の回 (%s) と NE の元のコードの回 (%s) が違う (照合の途中・少し待つか明日の照合の後に)', v_run, coalesce(v_mark, 'なし'))
         when not v_fresh then pg_catalog.format('最新の封をした照合の回 (%s) の NE の取得が %s 時間より古い (明日の照合の後に)', v_run, v_hours)
         else pg_catalog.format('照合の回 %s (NE の取得 %s)', v_run, pg_catalog.to_char(least(h.products_at, h.sets_at) at time zone 'Asia/Tokyo', 'YYYY-MM-DD HH24:MI')) end,
    'after_compare');
  v_it := v_it || ops.quarantine_confirm_item('ne_observed', v_obsok, pg_catalog.to_jsonb(s.sku_kind), v_obs -> 'kind',
    case when not v_fresh then '照合の回が使えない (上)'
         when v_obs is null then pg_catalog.format('照合の回 %s に %s の NE の観測が無い (照合 ② の確かめ待ちに入った後の照合をまだ待っている = 明日の照合の後に)', v_run, s.code)
         when (v_obs -> 'present') is distinct from 'true'::jsonb then 'NE の完全な取得に無い (NE から消えた・コードが違う)'
         when (v_obs -> 'trusted') is distinct from 'true'::jsonb then 'NE の取得のこのコードを信用できない (正規化の衝突・取込の整合 = 明日の照合の後に)'
         when (v_obs ->> 'kind') is distinct from s.sku_kind then pg_catalog.format('区分が NE (%s) と違う', coalesce(v_obs ->> 'kind', '?'))
         else 'NE にある・信用できる・区分が同じ' end,
    case when not v_fresh or v_obs is null or (v_obs -> 'trusted') is distinct from 'true'::jsonb then 'after_compare' else 'one' end);

  -- ─── ① 必須の欄 (新商品の登録と同じ決まり) ───
  v_it := v_it || ops.quarantine_confirm_item('name', ops.ne_reg_name_ok(s.name) and core.norm_code(s.name) is distinct from s.code_norm, pg_catalog.to_jsonb(s.name), v_obs -> 'cols' -> 'name',
    case when not ops.ne_reg_name_ok(s.name) then '名前が無い・書けない文字がある'
         when core.norm_code(s.name) = s.code_norm then '名前が商品コードのまま'
         else '名前がある' end,
    case when (v_obs -> 'cols' -> 'name' ->> 'st') = 'ok' then 'ne_fill' else 'one' end);
  v_it := v_it || ops.quarantine_confirm_item('price',
    s.standard_price_jpy is not null and s.standard_price_jpy = pg_catalog.trunc(s.standard_price_jpy::numeric) and s.standard_price_jpy between 1 and 999999999,
    pg_catalog.to_jsonb(s.standard_price_jpy), v_obs -> 'cols' -> 'price',
    case when s.standard_price_jpy is not null and s.standard_price_jpy = pg_catalog.trunc(s.standard_price_jpy::numeric) and s.standard_price_jpy between 1 and 999999999
         then '標準売価がある' else '標準売価が無い (1〜999,999,999 円の整数)' end,
    case when (v_obs -> 'cols' -> 'price' ->> 'st') = 'ok' then 'ne_fill' else 'one' end);
  v_it := v_it || ops.quarantine_confirm_item('reorder_months', s.reorder_months is not null and s.reorder_months between 0 and 60, pg_catalog.to_jsonb(s.reorder_months), null,
    case when s.reorder_months is null then '推奨保有月数が空 (0〜60 を入れる・新商品と同じ必須の欄 = 中原さんの決定 Q2 a)' else '推奨保有月数がある' end, 'one');
  -- 今日 (東京) の原価 (lib/master-write.mjs の costAsOfJoin・ops.ne_reg_canonical と同じ選び方)
  select y.sku_cost_id, y.cost_jpy, y.cost_source, y.cost_status into v_cost from core.sku_costs y
   where y.sku_id = p_sku_id and y.valid_from <= v_today and (y.valid_to is null or y.valid_to >= v_today)
   order by y.valid_from desc, y.created_at desc, y.sku_cost_id desc limit 1;

  if s.sku_kind = 'single' then
    v_it := v_it || ops.quarantine_confirm_item('tax', (s.tax_rate = 0.1 and s.tax_class = 'STANDARD_10') or (s.tax_rate = 0.08 and s.tax_class = 'REDUCED_8'),
      pg_catalog.jsonb_build_object('tax_rate', s.tax_rate, 'tax_class', s.tax_class), v_obs -> 'cols' -> 'tax_rate',
      case when (s.tax_rate = 0.1 and s.tax_class = 'STANDARD_10') or (s.tax_rate = 0.08 and s.tax_class = 'REDUCED_8') then '税率と税区分が合う'
           else '税率 (8% / 10%) と税区分が合わない・空' end,
      case when (v_obs -> 'cols' -> 'tax_rate' ->> 'st') = 'ok' then 'ne_fill' else 'one' end);
    v_it := v_it || ops.quarantine_confirm_item('handling', s.handling in ('active', 'discontinued'), pg_catalog.to_jsonb(s.handling), v_obs -> 'cols' -> 'handling',
      case when s.handling in ('active', 'discontinued') then '取扱がある' else '取扱が 取扱中 / 中止 でない (分からない)' end,
      case when (v_obs -> 'cols' -> 'handling' ->> 'st') = 'ok' then 'ne_fill' else 'one' end);
    v_it := v_it || ops.quarantine_confirm_item('sales_class', s.sales_class between 1 and 4, pg_catalog.to_jsonb(s.sales_class), null,
      case when s.sales_class between 1 and 4 then '売上分類がある' else '売上分類が空 (1〜4 を入れる・NE に無い = 人が決める)' end, 'one');
    -- 代表の仕入先 (ちょうど 1 つ・NE の形 = 4 桁の数字・ポータルで作った仕入先は「NE に登録した」の申告の後)
    select pg_catalog.count(*) into v_nsup from core.supplier_skus x where x.sku_id = p_sku_id and x.is_primary;
    -- 行が無くても読む (record に列の形を入れる = 下の式が「まだ値の無い record」で止まらない)
    select su.supplier_id, su.code, rg.state as reg_state into v_sup
      from core.supplier_skus x join core.suppliers su on su.supplier_id = x.supplier_id left join ops.supplier_registrations rg on rg.supplier_id = su.supplier_id
     where x.sku_id = p_sku_id and x.is_primary order by su.supplier_id limit 1;
    if v_nsup = 1 then v_supc := core.canonical_supplier_code(v_sup.code); end if;
    v_it := v_it || ops.quarantine_confirm_item('supplier', v_nsup = 1 and v_supc ~ '^[0-9]{4}$' and (v_sup.reg_state is null or v_sup.reg_state = 'ne_confirmed'),
      case when v_nsup = 1 then pg_catalog.to_jsonb(v_supc) end, v_obs -> 'cols' -> 'supplier',
      case when v_nsup = 0 then '代表の仕入先が無い' when v_nsup > 1 then pg_catalog.format('代表の仕入先が %s つある', v_nsup)
           when v_supc is null or v_supc !~ '^[0-9]{4}$' then '代表の仕入先のコードが NE の形 (4 桁の数字) でない'
           when v_sup.reg_state is not null and v_sup.reg_state <> 'ne_confirmed' then '代表の仕入先の「NE に登録した」の申告がまだ'
           else '代表の仕入先がある' end,
      case when v_nsup = 0 and (v_obs -> 'cols' -> 'supplier' ->> 'st') = 'ok' then 'ne_fill' else 'one' end);
    v_it := v_it || ops.quarantine_confirm_item('cost', v_cost.cost_status in ('COMPLETE', 'OVERRIDDEN') and v_cost.cost_jpy >= 1,
      case when v_cost.sku_cost_id is null then null else pg_catalog.jsonb_build_object('cost_jpy', v_cost.cost_jpy, 'source', v_cost.cost_source, 'status', v_cost.cost_status) end,
      v_obs -> 'cols' -> 'cost',
      case when v_cost.sku_cost_id is null then pg_catalog.format('今日 (%s) の原価が無い', v_today)
           when v_cost.cost_status not in ('COMPLETE', 'OVERRIDDEN') then pg_catalog.format('今日の原価が決まった値でない (%s)', v_cost.cost_status)
           when v_cost.cost_jpy < 1 then '今日の原価が 0 円'
           else '今日の原価がある' end,
      case when v_cost.sku_cost_id is null and (v_obs -> 'cols' -> 'cost' ->> 'st') = 'ok' then 'ne_fill' else 'one' end);
    -- ③ 代表 (色違いのまとまり): 親がある = まとまりの記録 (予約) がある / 親が無い = NE に代表があれば先に代表の採用
    if s.parent_product_id is not null then
      select g.code into v_gcode from ops.variation_group_codes g where g.company_id = 1 and g.group_product_id = s.parent_product_id;
      v_it := v_it || ops.quarantine_confirm_item('parent', v_gcode is not null, pg_catalog.jsonb_build_object('group_product_id', s.parent_product_id::text, 'group_code', v_gcode),
        v_obs -> 'cols' -> 'parent',
        case when v_gcode is null then 'まとまりの子だが、まとまりの記録 (予約) が無い (先にまとまりの記録を作る = once-variation-group-from-ne.mjs)'
             else pg_catalog.format('まとまり %s の子 (記録がある)', v_gcode) end, 'one');
    else
      v_ok := not coalesce((v_obs -> 'cols' -> 'parent' ->> 'st') = 'ok' and pg_catalog.jsonb_typeof(v_obs -> 'cols' -> 'parent' -> 'v') = 'string'
                   and coalesce(v_obs -> 'cols' -> 'parent' ->> 'v', '') <> '' and core.norm_code(v_obs -> 'cols' -> 'parent' ->> 'v') <> s.code_norm, false);
      v_it := v_it || ops.quarantine_confirm_item('parent', v_ok, null, v_obs -> 'cols' -> 'parent',
        case when v_ok then '代表 (親) なし' else pg_catalog.format('NE に代表 (%s) がある = 先に代表の採用 (1 回だけ)', v_obs -> 'cols' -> 'parent' ->> 'v') end, 'one');
    end if;
  elsif s.sku_kind = 'set' then
    -- 構成品 (1 つ以上・全部 NE 確認済み以降)・開いている構成の依頼が無い
    select pg_catalog.count(*), pg_catalog.string_agg(case when rg.state is null or rg.state not in ('ne_confirmed', 'distributable', 'available')
                                                         then k2.code || ' (' || coalesce(rg.state, '状態なし') || ')' end, '・' order by c2.sort_order, k2.code),
           pg_catalog.array_agg(k2.tax_rate), pg_catalog.array_agg(p2.sales_class::integer), pg_catalog.bool_or(k2.handling = 'discontinued'),
           pg_catalog.bool_and(coalesce(y2.cost_jpy, 0) > 0), pg_catalog.sum(y2.cost_jpy * c2.qty)
      into v_n, v_bad, v_rates, v_class, v_stop, v_all, v_sum
      from core.sku_components c2 join core.skus k2 on k2.sku_id = c2.child_sku_id left join core.products p2 on p2.product_id = k2.product_id
      left join ops.master_registrations rg on rg.sku_id = c2.child_sku_id
      left join lateral (select case when y.cost_status in ('COMPLETE', 'OVERRIDDEN') then y.cost_jpy end as cost_jpy from core.sku_costs y
                          where y.sku_id = c2.child_sku_id and y.valid_from <= v_today and (y.valid_to is null or y.valid_to >= v_today)
                          order by y.valid_from desc, y.created_at desc, y.sku_cost_id desc limit 1) y2 on true
     where c2.parent_sku_id = p_sku_id;
    v_it := v_it || ops.quarantine_confirm_item('components', v_n >= 1 and v_bad is null,
      (select pg_catalog.jsonb_agg(pg_catalog.jsonb_build_object('code', k2.code, 'qty', c2.qty, 'state', rg.state) order by c2.sort_order, k2.code)
         from core.sku_components c2 join core.skus k2 on k2.sku_id = c2.child_sku_id left join ops.master_registrations rg on rg.sku_id = c2.child_sku_id where c2.parent_sku_id = p_sku_id),
      v_obs -> 'children',
      case when v_n = 0 then '構成品が無い' when v_bad is not null then pg_catalog.format('構成品 %s が NE 確認済みでない (先に構成品を確認済みに)', v_bad)
           else pg_catalog.format('構成品 %s 品 (全部 NE 確認済み以降)', v_n) end, 'one');
    select pg_catalog.count(*) into v_n from ops.sku_component_requests q where q.set_sku_id = p_sku_id and q.status = 'open';
    v_it := v_it || ops.quarantine_confirm_item('component_request', v_n = 0, null, null,
      case when v_n = 0 then '開いている構成の依頼は無い' else '開いている構成の依頼がある (先に片付ける)' end, 'one');
    -- 税率 (lib/master-set-rules.js の deriveSetTaxCdb = 1 つでも 8% / 10% でない = 決まらない・混ざれば低い方 + MIXED)
    if v_rates is null or exists (select 1 from pg_catalog.unnest(v_rates) x where x is null or x not in (0.1, 0.08)) then v_tax := null; v_tclass := null;
    elsif (select pg_catalog.count(distinct x) from pg_catalog.unnest(v_rates) x) = 1 then v_tax := v_rates[1]; v_tclass := case when v_tax = 0.08 then 'REDUCED_8' else 'STANDARD_10' end;
    else v_tax := 0.08; v_tclass := 'MIXED'; end if;
    v_it := v_it || ops.quarantine_confirm_item('tax', v_tax is not null and s.tax_rate = v_tax and s.tax_class = v_tclass,
      pg_catalog.jsonb_build_object('tax_rate', s.tax_rate, 'tax_class', s.tax_class), pg_catalog.jsonb_build_object('derived_tax_rate', v_tax, 'derived_tax_class', v_tclass),
      case when v_tax is null then '構成品の税率が決まらない (構成品の税率を先に)' when s.tax_rate = v_tax and s.tax_class = v_tclass then '税率が構成品から導いた値と同じ'
           else pg_catalog.format('税率が構成品から導いた値 (%s / %s) と違う (構成品から計算)', v_tax, v_tclass) end, 'one');
    -- 取扱 (deriveSetHandlingCdb: セット自身の取扱 handling_own が無い = 構成品が止まっていれば止める・そうでなければ今の値 / ある = どちらかが止まっていれば止める)
    v_hand := case when s.handling_own is null then case when coalesce(v_stop, false) then 'discontinued' else s.handling end
                   when s.handling_own = 'discontinued' or coalesce(v_stop, false) then 'discontinued' else 'active' end;
    v_it := v_it || ops.quarantine_confirm_item('handling', s.handling in ('active', 'discontinued') and s.handling = v_hand,
      pg_catalog.jsonb_build_object('handling', s.handling, 'handling_own', s.handling_own), pg_catalog.jsonb_build_object('derived', v_hand),
      case when s.handling not in ('active', 'discontinued') or s.handling is null then '取扱が 取扱中 / 中止 でない (分からない)'
           when s.handling <> v_hand then pg_catalog.format('取扱が構成品から導いた値 (%s) と違う (構成品から計算)', v_hand) else '取扱が構成品から導いた値と同じ' end, 'one');
    -- 売上分類 (上書き = 1〜4 か、構成品の MIN (1 つでも空・輸出 4 と 1〜3 の混在 = 導けない))
    if v_class is null or exists (select 1 from pg_catalog.unnest(v_class) x where x is null or x not between 1 and 4)
       or (4 = any(v_class) and exists (select 1 from pg_catalog.unnest(v_class) x where x <> 4)) then v_sales := null;
    else select pg_catalog.min(x) into v_sales from pg_catalog.unnest(v_class) x; end if;
    v_it := v_it || ops.quarantine_confirm_item('sales_class', s.set_sales_class_override between 1 and 4 or v_sales is not null,
      pg_catalog.jsonb_build_object('override', s.set_sales_class_override, 'from_components', v_sales), null,
      case when s.set_sales_class_override between 1 and 4 then '売上分類の上書きがある' when v_sales is not null then '売上分類を構成品から導ける'
           else '構成品から売上分類を導けない (上書きを入れる)' end, 'one');
    -- 今日の原価 = 構成品の合計 (set_calc / COMPLETE・円に丸めて同じ) か例外原価 (manual / OVERRIDDEN)
    --   #1715 Codex R1 M1・R2 M1: どちらの分岐も 1 円以上 (0 円は通さない = 単品と同じ)
    v_ok := v_cost.cost_jpy >= 1
            and ((v_cost.cost_source = 'set_calc' and v_cost.cost_status = 'COMPLETE' and coalesce(v_all, false) and v_cost.cost_jpy = pg_catalog.round(pg_catalog.round(v_sum, 2), 0))
                 or (v_cost.cost_source = 'manual' and v_cost.cost_status = 'OVERRIDDEN'));
    v_it := v_it || ops.quarantine_confirm_item('cost', v_ok,
      case when v_cost.sku_cost_id is null then null else pg_catalog.jsonb_build_object('cost_jpy', v_cost.cost_jpy, 'source', v_cost.cost_source, 'status', v_cost.cost_status) end,
      pg_catalog.jsonb_build_object('derived_cost_jpy', case when coalesce(v_all, false) then pg_catalog.round(pg_catalog.round(v_sum, 2), 0) end),
      case when v_ok then '今日の原価がある (構成品の合計か例外原価)'
           when v_cost.sku_cost_id is null then pg_catalog.format('今日 (%s) の原価が無い (構成品から計算・例外原価)', v_today)
           when not coalesce(v_all, false) then '構成品の原価がそろっていない (合計できない = 先に構成品の原価か、例外原価)'
           when v_cost.cost_jpy < 1 then '今日の原価が 0 円 (1 円以上)'
           else '今日の原価が構成品の合計と違う (構成品から計算し直す・例外原価)' end, 'one');
  end if;

  -- ─── ② 欄ごと = 今の社内の値と封をした回の NE の観測が同じ / 違うなら「今の社内の値」に結びついた承認済みの判断 (Q3 a・#1715 Codex R1 High) ───
  --   判断の候補は照合 ② が見た差 (社内の値 = 候補の print.c・NE の値 = 候補の print.n)。承認が効くのは、承認した組 (print.c ↔ print.n) = 今の組
  --   (今の社内の値 ↔ 最新の封をした回の観測の値・照合の読み方と同じ形) のときだけ (#1715 Codex R2 High 1) = 照合の後に社内の値を変えた (候補が無い)・
  --   承認の後に社内を第三の値に変えた・NE の値が変わった (同じ候補が次の回にも見えていても) = 通さない (明日の照合の後に、新しい差をマスタの判断で)
  if v_obsok then
    -- 今の社内の値 (apps/company-db/master-compare/compare-load.mjs の読み方 = 候補の print.c と同じ形)
    v_cur := pg_catalog.jsonb_build_object('name', s.name, 'standard_price_jpy', s.standard_price_jpy, 'tax_rate', s.tax_rate, 'handling', s.handling, 'kind', s.sku_kind,
      'exists', true,
      'cost', v_cost.cost_jpy,   -- #1715 Codex R2 High 2: 今日の原価 (必須・期待値と同じ行)。照合の print.c (期間の終わりの無い行) と違う行なら承認は結びつかない (通さない側)
      'primary_supplier', coalesce((select pg_catalog.jsonb_agg(su.code_norm order by su.code_norm) from core.supplier_skus x join core.suppliers su on su.supplier_id = x.supplier_id
                                     where x.sku_id = p_sku_id and x.is_primary), '[]'::jsonb),
      'parent', (select core.norm_code(pp.display_code) from core.products pp where pp.product_id = s.parent_product_id));
    select pg_catalog.jsonb_object_agg(k2.code_norm, c2.qty) into v_kids from core.sku_components c2 join core.skus k2 on k2.sku_id = c2.child_sku_id where c2.parent_sku_id = p_sku_id;
    v_kids := coalesce(v_kids, '{}'::jsonb);
    for c in
      select x.fingerprint, x.col, x.child, x.cls, x.reason_kind, coalesce(x.print -> 'c', 'null'::jsonb) as pc, coalesce(x.print -> 'n', 'null'::jsonb) as pn,
             l.event_id, l.kind as dkind, l.resolution, ap.print_c as ac, ap.print_n as an,
             exists (select 1 from ops.master_decision_events d where d.kind = 'action_done' and d.approved_event_id = l.event_id) as done
        from ops.master_decision_candidates x
        left join lateral (select e1.event_id, e1.kind, e1.resolution from ops.master_decision_events e1
                            where e1.fingerprint = x.fingerprint and e1.kind in ('approved', 'rejected', 'revoked') order by e1.event_id desc limit 1) l on true
        left join ops.master_decision_approval_prints ap on ap.event_id = l.event_id and ap.fingerprint = x.fingerprint   -- 承認の時の組 (#1715 Codex R3 High)
       where x.company_id = 1 and x.code_norm = s.code_norm
         and exists (select 1 from ops.master_decision_observations ob where ob.fingerprint = x.fingerprint and ob.compare_run_id = v_run)
       order by x.col, x.child nulls first, x.fingerprint
    loop
      v_dcol := case c.col when 'name' then 'name' when 'primary_supplier' then 'supplier' when 'cost' then 'cost' when 'standard_price_jpy' then 'price'
                           when 'tax_rate' then 'tax_rate' when 'handling' then 'handling' when 'parent' then 'parent' when 'components' then 'children' else 'other' end;
      v_ccur := case when c.col = 'components' then coalesce(v_kids -> c.child, '"(無い)"'::jsonb) else v_cur -> c.col end;
      -- 今の NE の値 (最新の封をした回の観測を照合の print.n と同じ形に: 仕入先 = [値]・構成の子 = 数量か "(無い)"・値なし = null・読めない = 結びつけない)
      v_ncur := case
        when c.col = 'components' then (select case when z.ch is null then '"(無い)"'::jsonb when (z.ch ->> 'st') = 'ok' then z.ch -> 'v' end
                                          from (select (select x2 from pg_catalog.jsonb_array_elements(coalesce(v_obs -> 'children', '[]'::jsonb)) x2 where x2 ->> 'code_norm' = c.child limit 1) as ch) z)
        when c.col = 'kind' then v_obs -> 'kind'
        when c.col = 'exists' then 'true'::jsonb
        else (select case when (oc ->> 'st') = 'no_value' then 'null'::jsonb
                          when (oc ->> 'st') <> 'ok' then null
                          when c.col = 'primary_supplier' then case when oc -> 'v' = 'null'::jsonb then 'null'::jsonb else pg_catalog.jsonb_build_array(oc -> 'v') end
                          when c.col = 'parent' and pg_catalog.jsonb_typeof(oc -> 'v') = 'string' and core.norm_code(oc ->> 'v') = s.code_norm then 'null'::jsonb
                          when c.col = 'parent' and pg_catalog.jsonb_typeof(oc -> 'v') = 'string' then pg_catalog.to_jsonb(core.norm_code(oc ->> 'v'))
                          else coalesce(oc -> 'v', 'null'::jsonb) end
                from (select v_obs -> 'cols' -> (case c.col when 'standard_price_jpy' then 'price' when 'primary_supplier' then 'supplier' else c.col end) as oc) y)
      end;
      -- #1715 Codex R3 High: 照らすのは承認の時の組 (承認の出来事に結びついた記録)。候補の行の今の print ではない。記録が無い承認は効かせない
      v_tied := coalesce(c.ac = v_ccur and c.an = v_ncur, false);
      if c.dkind = 'approved' and c.resolution in ('accept_difference', 'fix_ne', 'fix_cdb') and not (c.resolution in ('fix_ne', 'fix_cdb') and c.done) and v_tied then
        -- 承認済みで、承認した差の社内の値 = 今の社内の値 (構成は子ごと)
        v_appr := v_appr || pg_catalog.jsonb_build_object(v_dcol || coalesce(':' || c.child, ''), pg_catalog.jsonb_build_object('fingerprint', c.fingerprint, 'col', c.col,
          'child', c.child, 'how', 'approved', 'event_id', c.event_id::text, 'resolution', c.resolution));
      else
        v_unres := v_unres || pg_catalog.jsonb_build_object(v_dcol, coalesce(v_unres -> v_dcol, '[]'::jsonb) || pg_catalog.jsonb_build_array(
          pg_catalog.format('%s%s (%s%s)', c.col, coalesce(':' || c.child, ''), c.reason_kind,
            case when c.dkind is null then '・まだ決めていない'
                 when c.dkind <> 'approved' then '・最新の判断が ' || c.dkind
                 when c.resolution not in ('accept_difference', 'fix_ne', 'fix_cdb') then '・解決 ' || c.resolution || ' はこの確認では数えない'
                 when c.resolution in ('fix_ne', 'fix_cdb') and c.done then '・直す承認の完了の後にまた出た'
                 when c.ac is null then '・承認の時の組の記録が無い (マスタの判断で決め直す)'
                 when c.ac is distinct from v_ccur then pg_catalog.format('・承認の後に社内の値が変わった (照合の時 %s・今 %s)', c.ac, coalesce(v_ccur::text, '読めない'))
                 else pg_catalog.format('・承認の後に NE の値が変わった (承認の時 %s・今 %s)', c.an, coalesce(v_ncur::text, '読めない')) end)));
      end if;
      v_cands := v_cands || pg_catalog.jsonb_build_object(v_dcol, true);
    end loop;
    v_cols := case when s.sku_kind = 'single' then array['name', 'supplier', 'cost', 'price', 'tax_rate', 'handling', 'parent'] else array['name', 'price', 'children'] end;
    if v_unres ? 'other' then v_cols := v_cols || 'other'::text; end if;
    foreach k in array v_cols loop
      e := case when k = 'other' then null else coalesce(v_exp -> 'values' -> k, 'null'::jsonb) end;
      o := case when k = 'children' then v_obs -> 'children' when k = 'other' then null else v_obs -> 'cols' -> k end;
      v_diff := null;
      if k = 'children' then
        -- 違う子 (社内の今の構成の数量 ↔ その回の観測。読めない子も違う) = 子ごとに、今の構成に結びついた承認済みの判断が要る
        select pg_catalog.array_agg(coalesce(a.cn, b.cn) order by coalesce(a.cn, b.cn)) into v_diff
          from (select t.key as cn, t.value as qty from pg_catalog.jsonb_each(v_kids) t) a
          full join (select ch ->> 'code_norm' as cn, ch from pg_catalog.jsonb_array_elements(coalesce(o, '[]'::jsonb)) ch) b on b.cn = a.cn
         where b.cn is null or a.cn is null or (b.ch ->> 'st') is distinct from 'ok' or (b.ch -> 'v') is distinct from a.qty;
        v_same := v_diff is null;
        v_ok := v_same or not exists (select 1 from pg_catalog.unnest(v_diff) dc where not (v_appr ? ('children:' || dc)));
      elsif k = 'other' then
        v_same := false; v_ok := false;
      elsif k = 'parent' then
        -- 代表: NE の「自分自身」= 親なし (照合 ② と同じ)・コードは norm で比べる
        v_same := case when (o ->> 'st') = 'ok' and pg_catalog.jsonb_typeof(o -> 'v') = 'string' and coalesce(o ->> 'v', '') <> '' and core.norm_code(o ->> 'v') <> s.code_norm
                       then pg_catalog.jsonb_typeof(e) = 'string' and core.norm_code(o ->> 'v') = (e #>> '{}')
                       else e = 'null'::jsonb and (o ->> 'st') in ('ok', 'no_value') end;
        v_same := coalesce(v_same, false);
        v_ok := v_same or (v_appr ? k);
      else
        v_same := coalesce(((o ->> 'st') = 'ok' and coalesce(o -> 'v', 'null'::jsonb) = e) or ((o ->> 'st') = 'no_value' and e = 'null'::jsonb), false);
        v_ok := v_same or (v_appr ? k);
      end if;
      if v_ok and not v_same then
        v_used := v_used || coalesce((select pg_catalog.jsonb_agg(x.value order by x.key) from pg_catalog.jsonb_each(v_appr) x where x.key = k or x.key like k || ':%'), '[]'::jsonb);
      elsif v_same and v_cands ? k then
        v_used := v_used || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('col', k, 'how', 'copied'));
      end if;
      v_it := v_it || ops.quarantine_confirm_item('ne.' || k, v_ok, e, o,
        case when v_same then case when v_cands ? k then '社内の値を NE の値に合わせた (明日の照合で差が消える)' else 'NE と同じ' end
             when v_ok then 'NE と違うが、今の社内の値でマスタの判断で決めてある'
             when v_unres ? k then pg_catalog.format('NE と違う (照合の回 %s) = マスタの判断で決める: %s', v_run,
                                     (select pg_catalog.string_agg(t, '・') from pg_catalog.jsonb_array_elements_text(v_unres -> k) t))
             when k = 'children' then pg_catalog.format('構成が NE と違う子 (%s) に、今の構成でのマスタの判断が無い (照合の後に構成を変えた = 明日の照合の後に)', pg_catalog.array_to_string(v_diff, '・'))
             else pg_catalog.format('NE (照合の回 %s) と違う。照合の後に社内の値を変えた = 明日の照合の後にマスタの判断で', v_run) end,
        case when k not in ('children', 'other') and e = 'null'::jsonb and (o ->> 'st') = 'ok' then 'ne_fill'
             when v_unres ? k and not exists (select 1 from pg_catalog.jsonb_array_elements_text(v_unres -> k) t where t like '%承認の後に%の値が変わった%') then 'decision'
             else 'after_compare' end);
    end loop;
  end if;

  -- ─── ④ そのほか: 生きている NE 登録の CSV の品目・NE から消す依頼 ───
  select pg_catalog.string_agg('#' || i.export_id::text || ' ' || i.state, '・' order by i.item_id) into v_txt
    from ops.ne_reg_export_items i where i.sku_id = p_sku_id and i.state in ('built', 'issued', 'import_declared', 'partial');
  if exists (select 1 from ops.ne_del_items d where d.sku_id = p_sku_id and d.state = 'live') then
    v_txt := coalesce(v_txt || '・', '') || 'NE から消す依頼の途中';
  end if;
  v_it := v_it || ops.quarantine_confirm_item('live_file', v_txt is null, null, null,
    case when v_txt is null then '生きている NE 登録の CSV・NE から消す依頼は無い' else pg_catalog.format('終わっていない CSV・依頼がある (%s)', v_txt) end, 'one');

  v_ok := not exists (select 1 from pg_catalog.jsonb_array_elements(v_it) x where (x -> 'ok') is distinct from 'true'::jsonb);
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('v', 'quarantine-confirm-v1', 'sku_id', p_sku_id::text, 'day', v_today::text,
    'sku_version', s.sku_version::text, 'product_version', s.product_version::text, 'cost_id', v_cost.sku_cost_id::text, 'run', v_run,
    'observation_hash', case when v_obs is null then null else pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(v_obs::text, 'UTF8')), 'hex') end,
    'items', v_it, 'used', v_used));
  if v_ok then
    v_ev := pg_catalog.jsonb_build_object('rule', 'quarantine_confirm_v1', 'check_hash', v_hash, 'compare_run_id', v_run, 'ne_codes_run', v_mark,
      'receipt_observation_hash', v_rc.observation_hash, 'evidence_sha256', v_rc.evidence_sha256, 'fetch_generation', h.fetch_generation,
      'products_at', h.products_at, 'sets_at', h.sets_at,
      'observation_hash', pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(v_obs::text, 'UTF8')), 'hex'),
      'decisions', v_used, 'group_product_id', s.parent_product_id::text, 'group_code', v_gcode, 'cost_id', v_cost.sku_cost_id::text, 'day', v_today::text);
  end if;
  return pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'code', s.code, 'sku_kind', s.sku_kind, 'reg_state', s.reg, 'ok', v_ok, 'items', v_it,
    'evidence', v_ev, 'check_hash', v_hash);
end $$;
revoke all on function ops.quarantine_confirm_check(bigint) from public;

-- ═══════════ 3. 一覧 (読むだけ・一括の inspect・毎朝のまとめの材料) ═══════════
/**
 * p_codes = 確かめるコード (500 件まで・code_norm で探す・渡した順は code_norm の順) / null = 要確認 (quarantined) の全部 (コードの順・500 件を超えたら too_many)。
 * 戻り値 = [ops.quarantine_confirm_check の答え (無いコード = { sku_id: null, code, ok: false, items: [state = no_sku] })]
 */
create function ops.quarantine_confirm_list(p_codes text[]) returns jsonb
  language plpgsql stable security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_out jsonb;
begin
  if p_codes is not null and (pg_catalog.cardinality(p_codes) > 500 or exists (select 1 from pg_catalog.unnest(p_codes) c where c is null or pg_catalog.length(c) not between 1 and 200)) then
    raise exception 'invalid_input: コードは 1〜200 字で 500 件まで' using errcode = '22023';
  end if;
  if p_codes is null then
    -- #1715 Codex R1 M3: 全部 = 500 件まで (超えたら断る = コードを 500 件ずつ渡す)。1 件ずつ確かめを呼ぶ = 件数に比例して重い
    if (select pg_catalog.count(*) from core.skus k join ops.master_registrations r on r.sku_id = k.sku_id where k.company_id = 1 and r.state = 'quarantined') > 500 then
      raise exception 'too_many: 要確認が 500 件を超える = コード (500 件まで) を渡して分けて読む' using errcode = '22023';
    end if;
    select coalesce(pg_catalog.jsonb_agg(ops.quarantine_confirm_check(k.sku_id) order by k.code_norm), '[]'::jsonb) into v_out
      from core.skus k join ops.master_registrations r on r.sku_id = k.sku_id where k.company_id = 1 and r.state = 'quarantined';
  else
    select coalesce(pg_catalog.jsonb_agg(case when k.sku_id is null
             then pg_catalog.jsonb_build_object('sku_id', null, 'code', c.code, 'ok', false, 'evidence', null, 'check_hash', null,
                    'items', ops.quarantine_confirm_item('state', false, null, null, 'Company DB にそのコードの SKU が無い', 'one'))
             else ops.quarantine_confirm_check(k.sku_id) end order by core.norm_code(c.code), c.code), '[]'::jsonb) into v_out
      from (select distinct on (core.norm_code(u.v)) u.v as code from pg_catalog.unnest(p_codes) u(v) order by core.norm_code(u.v), u.v) c
      left join core.skus k on k.company_id = 1 and k.code_norm = core.norm_code(c.code);
  end if;
  return v_out;
end $$;
revoke all on function ops.quarantine_confirm_list(text[]) from public;

-- ═══════════ 6. 約束の操作 (0072 の CHECK の版から写して quarantine_confirm を 1 つ足しただけ) ═══════════
alter table ops.master_write_sessions drop constraint ck_mws_operation;
alter table ops.master_write_sessions add constraint ck_mws_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel',
  'sku_draft_cancel',
  'ne_del_build', 'ne_del_withdraw', 'ne_handling_build',
  'quarantine_confirm'));
alter table ops.master_edit_requests drop constraint ck_mer_operation;
alter table ops.master_edit_requests add constraint ck_mer_operation check (operation in ('sku_edit', 'sku_create',
  'reg_csv_build', 'reg_csv_issue', 'reg_csv_declare', 'reg_csv_supersede', 'reg_csv_verified', 'jan_edit', 'supplier_create', 'supplier_declare', 'supplier_deactivate',
  'amazon_map_save', 'amazon_map_delete',
  'variation_batch_open', 'variation_batch_close', 'variation_child_cancel', 'variation_label_edit', 'parent_adopt_ne',
  'sku_gone_cancel',
  'sku_draft_cancel',
  'ne_del_build', 'ne_del_withdraw', 'ne_handling_build',
  'quarantine_confirm'));

-- ═══════════ 5. 状態を 1 つ進める (0073 の版を写して、🆕 0078 のかたまりだけ足した) ═══════════
/**
 * 0073 の版と同じ (ne_pending・ne_confirmed (ne_pending から)・cancelled の道・0070 / 0071 / 0073 の守り)。足したのは 🆕 0078 のかたまり:
 *   quarantined → ne_confirmed = 約束 quarantine_confirm (ops.confirm_quarantined_sku だけが書く) の中・同じ SKU・人・request_id・人 (human) のときだけ。
 *   呼び手の根拠は受けない。ops.quarantine_confirm_check をもう一度呼び、ok で check_hash が約束の check_hash と同じときだけ。根拠 = その表の evidence (DB が作る)
 */
create or replace function ops.transition_sku_registration(p_sku_id bigint, p_to text, p_actor_type text, p_actor_id text,
                                                           p_reason text default null, p_evidence jsonb default '{}'::jsonb, p_request_id text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  r        ops.master_registrations%rowtype;
  v_ev     jsonb := coalesce(p_evidence, '{}'::jsonb);
  v_rec    record;
  v_event  bigint;
  v_sess   ops.master_write_sessions;   -- 🆕 0070
  v_chk    jsonb;                       -- 🆕 0070
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;   -- 🆕 0071
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
  -- 🆕 0078 ここから: 要確認 (NE で直接作られた商品) → NE 確認済み = 「確認済みにする」の約束 (ops.confirm_quarantined_sku・操作 quarantine_confirm) の中だけ・
  --   同じ SKU・人・request_id・人 (human)。呼び手の根拠は受けない。確かめ (ops.quarantine_confirm_check) をもう一度: ok で、check_hash が約束の check_hash と同じときだけ。
  --   根拠は DB が確かめの表から作る。運用のロール・持ち主のロールが直に呼んでも断る (0070 の「NE から消えた」と同じ型)。下の 0073 の not_ready の行には届かない
  if p_to = 'ne_confirmed' and r.state = 'quarantined' then
    v_sess := ops.current_master_write_session();
    if v_sess.session_id is null or v_sess.operation is distinct from 'quarantine_confirm' or v_sess.sku_id is distinct from p_sku_id
       or v_sess.actor_id is distinct from p_actor_id or v_sess.request_id::text is distinct from p_request_id or p_actor_type is distinct from 'human' then
      raise exception 'confirm_session_required: 要確認を NE 確認済みにするのは「確認済みにする」の操作 (ops.confirm_quarantined_sku) の中だけ' using errcode = '42501';
    end if;
    if v_ev <> '{}'::jsonb then
      raise exception 'caller_evidence: 確認済みの根拠は DB が確かめの表から作る。呼び手の根拠は受けない' using errcode = '22023';
    end if;
    v_chk := ops.quarantine_confirm_check(p_sku_id);
    if (v_chk ->> 'ok') is distinct from 'true' then
      raise exception 'not_confirmable: %', (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'items') x
                                              where (x -> 'ok') is distinct from 'true'::jsonb) using errcode = 'P0001';
    end if;
    if (v_chk ->> 'check_hash') is distinct from (v_sess.versions ->> 'check_hash') then
      raise exception 'stale: 確かめの表が約束の時と違う (読み直して押し直す)' using errcode = 'P0001';
    end if;
    v_ev := v_chk -> 'evidence';
    perform pg_catalog.set_config('ops.registration_protocol', '1', true);
    update ops.master_registrations set state = p_to, state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
    perform pg_catalog.set_config('ops.registration_protocol', '', true);
    insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
      values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
    return pg_catalog.jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
  end if;
  -- 🆕 0078 ここまで
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
    if p_to = 'ne_pending' and p_actor_type = 'human' then
      select i.item_id, i.export_id, a.attempt_id, a.sha256, a.result, a.declared_by, a.declared_at into v_rec
        from ops.ne_reg_export_items i
        join ops.ne_reg_exports e on e.export_id = i.export_id
        join ops.ne_reg_attempts a on a.attempt_id = i.attempt_id and a.export_id = i.export_id
       where i.sku_id = p_sku_id and i.state in ('import_declared', 'partial') and e.state = 'declared' and a.sha256 = e.sha256 and a.result in ('ok', 'partial')   -- 🆕 0063: 申告を結んだ partial も
       order by i.item_id desc limit 1
       for share of i, e;
      if not found then
        raise exception 'no_evidence: SKU % に、取り込んだと申告した新規登録の CSV の品目が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('export_id', v_rec.export_id, 'item_id', v_rec.item_id, 'attempt_id', v_rec.attempt_id, 'sha256', v_rec.sha256,
                                            'result', v_rec.result, 'declared_by', v_rec.declared_by, 'declared_at', v_rec.declared_at);
    elsif p_to = 'ne_pending' then
      -- 🆕 0063: 申告をしなかった品目を、配った後の NE の完全な取得で照合が確かめた (system・ne_compare だけ)
      if p_actor_id <> 'ne_compare' then raise exception 'no_evidence: 申告なしの NE 登録待ちは翌朝の照合 (ne_compare) の確かめで' using errcode = '22023'; end if;
      select c.check_id, c.compare_run_id, c.item_id, i.export_id, c.fetched_at, e.issued_at into v_rec
        from ops.ne_reg_checks c
        join ops.ne_reg_export_items i on i.item_id = c.item_id and i.sku_id = c.sku_id
        join ops.ne_reg_exports e on e.export_id = i.export_id
       where c.sku_id = p_sku_id and c.outcome = 'verified' and i.state = 'verified' and i.verified_run = c.compare_run_id and i.attempt_id is null
         and e.issued_at is not null and c.fetched_at > e.issued_at
         and ops.ne_reg_auto_block(i.item_id) = ''   -- 自動で確かめてよい品目だけ (単品・JAN を送っていない・配った時の許可の印がある)
         and exists (select 1 from ops.master_compare_runs m where m.compare_run_id = c.compare_run_id)
       order by c.check_id desc limit 1
       for share of i;
      if not found then
        raise exception 'no_evidence: SKU % に、配った後の NE の完全な取得で全部の列が合った確かめ (verified・申告なし) が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('compare_run_id', v_rec.compare_run_id, 'check_id', v_rec.check_id, 'item_id', v_rec.item_id, 'export_id', v_rec.export_id,
                                            'fetched_at', v_rec.fetched_at, 'issued_at', v_rec.issued_at, 'matched', true, 'declared', false);
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
    -- 🆕 0070 ここから: 業務で使える (available) → やめた = 「NE から消えた」の約束の中だけ・DB の確かめが ok・根拠は DB が作る
    if r.state = 'available' then
      v_sess := ops.current_master_write_session();
      if v_sess.session_id is null or v_sess.operation is distinct from 'sku_gone_cancel' or v_sess.sku_id is distinct from p_sku_id
         or v_sess.actor_id is distinct from p_actor_id or v_sess.request_id::text is distinct from p_request_id then
        raise exception 'gone_session_required: 業務で使える商品をやめるのは「NE から消えた」の操作 (ops.cancel_sku_gone_from_ne) の中だけ' using errcode = '42501';
      end if;
      if v_ev <> '{}'::jsonb then
        raise exception 'caller_evidence: NE から消えた根拠は DB が完全な NE の取得の記録から作る。呼び手の根拠は受けない' using errcode = '22023';
      end if;
      v_chk := ops.sku_gone_from_ne_check(p_sku_id);
      if (v_chk ->> 'ok') is distinct from 'true' then
        raise exception 'not_gone_from_ne: %', (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'problems') x) using errcode = 'P0001';
      end if;
      v_ev := v_chk -> 'evidence';
    end if;
    -- 🆕 0070 ここまで
    -- 🆕 0071 ここから: 下書き・NE 登録待ち → やめた = 下書きを消す (ops.cancel_sku_draft・約束 sku_draft_cancel)・まとまりの子の廃止 (ops.cancel_variation_child・約束 variation_child_cancel)
    --   の中だけ (同じ SKU・人・request_id)。運用のロール (master_ops) が直に呼んでも断る = 消せる条件 (一度配った・NE に現れた・使われている) と片付けを迂回しない。
    --   表の持ち主のロール (とそのメンバー = migration・手の復旧) は約束を求めない (表を直接書けるロール = 守りの境界はログインのロール)
    if r.state in ('draft', 'ne_pending')
       and not pg_catalog.pg_has_role(v_db_user, (select c.relowner from pg_catalog.pg_class c where c.oid = 'ops.master_registrations'::pg_catalog.regclass), 'MEMBER') then
      v_sess := ops.current_master_write_session();
      if v_sess.session_id is null or v_sess.operation not in ('sku_draft_cancel', 'variation_child_cancel') or v_sess.sku_id is distinct from p_sku_id
         or v_sess.actor_id is distinct from p_actor_id or v_sess.request_id::text is distinct from p_request_id then
        raise exception 'draft_cancel_session_required: 下書き・NE 登録待ちをやめるのは「下書きを消す」(ops.cancel_sku_draft)・「子の廃止」(ops.cancel_variation_child) の中だけ' using errcode = '42501';
      end if;
    end if;
    -- 🆕 0071 ここまで
    -- 🆕 0073 ここから: 要確認 (NE で直接作った商品) → やめた = 販路の出品の構成 (Amazon SKU の対応など・削除済みの出品も) に使われていれば断る。
    --   0073 から要確認も Amazon の構成に使える = やめると有効な対応に「やめた」構成品が残る (0070 の NE から消えた・0071 の下書きを消す・0072 の NE から消す・
    --   0073 の子の廃止と同じ決まり)。先にその出品の構成から外す
    if r.state = 'quarantined' and exists (select 1 from core.listing_components lc where lc.sku_id = p_sku_id) then
      raise exception 'listing_component: 要確認の商品 % は販路の出品の構成に使われている (%) = 先にその出品の構成から外す',
        (select k.code from core.skus k where k.sku_id = p_sku_id),
        (select pg_catalog.string_agg(x.w, '・' order by x.w) from (select distinct l.mall || ':' || l.listing_code as w from core.listing_components lc
            join core.listings l on l.listing_id = lc.listing_id where lc.sku_id = p_sku_id order by 1 limit 6) x)
        using errcode = 'P0001';
    end if;
    -- 🆕 0073 ここまで
  end if;
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  update ops.master_registrations set state = p_to, state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
    values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
  return pg_catalog.jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
end $$;

-- ═══════════ 4. 確認済みにする関数 ═══════════
/**
 * 要確認 (quarantined) の商品を確認済み (NE 確認済み = ne_confirmed) にする。1 件ずつ・1 つの取引。理由は無くてよい (200 字まで・改行なし)。
 *   p_check_hash = 画面が見た確かめの表のハッシュ (ops.quarantine_confirm_check の check_hash)。押した時の表と違う = stale (読み直して押し直す)
 *   同じ request_id の押し直し = 前の答え (replayed)。違う操作・人・SKU・理由・表で同じ request_id = request_id_reused
 *   鍵: 段階 (共有) → マスタの書き込み (共有・夜間ロードと並ぶ) → SKU → 親子 (共有・代表の採用と並ぶ) → NE のコード (共有・照合の入れ替えと並ぶ) →
 *       (親があれば) まとまりのコード → まとまり → 登録の状態の行 (for update) → 構成品の登録の状態の行 (for share = 構成品を同時にやめない) →
 *       判断の候補の行 (for share = 判断の画面の取り消しと並ぶ)
 *   段階 new_open・持ち主表 (呼び手) が段階の記録と同じ (ops.reg_write_gate)。確かめが ok でない = not_confirmable (だめな行の文を全部)・ハッシュが違う = stale
 *   約束 (0051 の ops.master_write_sessions・操作 quarantine_confirm・versions に check_hash・この関数だけが書く) → 登録の状態 (ops.transition_sku_registration が約束と
 *   確かめをもう一度見る) → 保存の記録 done (ops.close_reg_write)。戻り値 = { ok, request_id, code, sku_id, from, state, event_id, check_hash, evidence }
 * 呼べるロール = master_edit (画面・名簿 MASTER_EDITORS の人 = 中原さんの決定 Q1 a)。🚨 人 (actor_id) は呼び手が言う値 (DB では確かめられない)
 * 商品の値 (core.*) は書かない (値を埋めるのは今の保存 = Q-2・Q-3)
 */
create function ops.confirm_quarantined_sku(p_request_id uuid, p_actor_id text, p_reason text, p_ownership jsonb, p_sku_id bigint, p_check_hash text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v_db_user text := case when coalesce(pg_catalog.current_setting('role', true), 'none') <> 'none' then pg_catalog.current_setting('role', true) else session_user::text end;
  v_reason  text := nullif(pg_catalog.btrim(coalesce(p_reason, '')), '');
  v_hash    text;
  v_prev    record;
  s         record;
  v_chk     jsonb;
  v_phase   text;
  v_id      uuid := pg_catalog.gen_random_uuid();
  v_tr      jsonb;
  v_result  jsonb;
begin
  if p_request_id is null or p_sku_id is null then raise exception 'invalid_input: request_id と SKU が要る' using errcode = '22023'; end if;
  if ops.reg_actor_problem(p_actor_id, v_reason) is not null then
    raise exception 'invalid_input: 人・理由 (200 字まで・改行なし) の形が違う' using errcode = '22023';
  end if;
  if coalesce(p_check_hash, '') !~ '^[0-9a-f]{64}$' then
    raise exception 'invalid_input: 確かめの表のハッシュ (check_hash・64 桁の 16 進) が要る (画面で表を読んでから押す)' using errcode = '22023';
  end if;
  perform ops.reg_write_gate(p_ownership, '{}'::text[]);   -- 段階 (共有) → マスタの書き込み (共有) → 段階 new_open・持ち主表 = 段階の記録
  -- #1715 Codex R3 M1: 同じ request_id の押し直しを直列に (保存 lib/master-write.mjs の REQUEST_LOCK_SQL と同じ鍵) → その後に前の結果を読む = 同時の押し直しも前の答え
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('ops.master_edit_request:' || p_request_id::text, 0));
  v_hash := ops.reg_hash(pg_catalog.jsonb_build_object('op', 'quarantine_confirm', 'sku_id', p_sku_id::text, 'reason', v_reason, 'check_hash', p_check_hash));
  select x.operation, x.actor_id, x.status, x.result, x.payload_hash, x.sku_id into v_prev from ops.master_edit_requests x where x.request_id = p_request_id;
  if found then
    if v_prev.operation is distinct from 'quarantine_confirm' or v_prev.actor_id is distinct from p_actor_id or v_prev.status is distinct from 'done'
       or v_prev.payload_hash is distinct from v_hash or v_prev.sku_id is distinct from p_sku_id then
      raise exception 'request_id_reused: request_id % はほかの操作・人・商品・理由・表で使われている', p_request_id using errcode = '23505';
    end if;
    return v_prev.result || '{"replayed": true}'::jsonb;
  end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  perform pg_catalog.pg_advisory_xact_lock_shared(core.parent_lock_key());
  perform pg_catalog.pg_advisory_xact_lock_shared(pg_catalog.hashtext('ops.ne_codes'));
  select k.code, k.code_norm, p.parent_product_id into s
    from core.skus k left join core.products p on p.product_id = k.product_id where k.sku_id = p_sku_id and k.company_id = 1;
  if not found then raise exception 'not_found: SKU % が無い', p_sku_id using errcode = 'P0002'; end if;
  if s.parent_product_id is not null then perform ops._variation_lock_existing_group(s.parent_product_id); end if;
  perform 1 from ops.master_registrations x where x.sku_id = p_sku_id for update;
  perform 1 from ops.master_registrations x where x.sku_id in (select c.child_sku_id from core.sku_components c where c.parent_sku_id = p_sku_id) order by x.sku_id for share;
  perform 1 from ops.master_decision_candidates x where x.company_id = 1 and x.code_norm = s.code_norm order by x.fingerprint for share;
  if (select x.state from ops.master_registrations x where x.sku_id = p_sku_id) is distinct from 'quarantined' then
    raise exception 'not_quarantined: % の登録の状態が % (確認済みにできるのは要確認だけ)', s.code,
      coalesce((select x.state from ops.master_registrations x where x.sku_id = p_sku_id), 'なし') using errcode = 'P0001';
  end if;
  v_chk := ops.quarantine_confirm_check(p_sku_id);
  if (v_chk ->> 'ok') is distinct from 'true' then
    raise exception 'not_confirmable: % は確認済みにできない (%)', s.code, (select pg_catalog.string_agg(x ->> 'text', ' / ') from pg_catalog.jsonb_array_elements(v_chk -> 'items') x
                                                                       where (x -> 'ok') is distinct from 'true'::jsonb) using errcode = 'P0001';
  end if;
  if (v_chk ->> 'check_hash') is distinct from p_check_hash then
    raise exception 'stale: % の確かめの表が画面で見た時と違う (値か照合が変わった = 読み直して押し直す)', s.code using errcode = 'P0001';
  end if;
  if (ops.current_master_write_session()).session_id is not null then
    raise exception 'master_write_session_exists: この取引ではもう書き込みを始めている' using errcode = '55000';
  end if;
  select x.phase into v_phase from ops.master_cutover_state x where x.id = 1;
  insert into ops.master_write_sessions (session_id, txid, request_id, operation, sku_id, derived_sku_ids, target_product_ids, edit_token, payload_hash, versions,
                                         actor_id, reason, source_system, db_user, phase, owner_hash, ownership)
    values (v_id, pg_catalog.txid_current(), p_request_id, 'quarantine_confirm', p_sku_id, '{}', '{}', pg_catalog.repeat('0', 64),
            v_hash, pg_catalog.jsonb_build_object('sku_id', p_sku_id::text, 'check_hash', p_check_hash), p_actor_id, v_reason, 'portal_master_edit', v_db_user, v_phase,
            ops.ownership_hash(p_ownership), p_ownership);
  perform pg_catalog.set_config('ops.master_write_session', v_id::text, true);
  v_tr := ops.transition_sku_registration(p_sku_id, 'ne_confirmed', 'human', p_actor_id, v_reason, '{}'::jsonb, p_request_id::text);
  v_result := pg_catalog.jsonb_build_object('ok', true, 'request_id', p_request_id::text, 'code', s.code, 'sku_id', p_sku_id::text, 'from', 'quarantined', 'state', 'ne_confirmed',
    'event_id', v_tr ->> 'event_id', 'check_hash', p_check_hash, 'evidence', v_chk -> 'evidence');
  perform ops.close_reg_write(v_result, s.code);
  return v_result;
end $$;
revoke all on function ops.confirm_quarantined_sku(uuid, text, text, jsonb, bigint, text) from public;

-- ─── 権限 (0070 と同じ形)。master_edit には scripts/company-db/create-master-edit-roles.mjs の流し直しで付ける (表は増やさない・関数の実行だけ) ───
--   watcher = 確かめと一覧 (読むだけ) の実行。だれにも渡さない = ops.quarantine_confirm_item (確かめの中の部品)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'grant execute on function ops.quarantine_confirm_check(bigint) to watcher';
    execute 'grant execute on function ops.quarantine_confirm_list(text[]) to watcher';
    execute 'grant select on ops.master_decision_approval_prints to watcher';
  end if;
end $$;
