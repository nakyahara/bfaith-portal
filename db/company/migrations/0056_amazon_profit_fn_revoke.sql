-- 0056 Amazon の利益の重い関数を、誰からも直接呼べないようにする (D-60 の緊急の権限の封鎖 = PR 1a。2026-10-03)
--   Codex R-D60-v3-4 H2 / R-D60-v3-5「最初の PR の範囲」の PR 1a
--
-- 何が起きたか: 2026-10-01 00:10、0049 / 0050 の利益の関数で 93 日分を計算して本番の Postgres (1GB) を落とした。
--   受け口 GET /apps/company-db/sync/amazon-profit/daily・/totals は #1570 で 503 (DB に接続しない) にしたが、
--   DB に直接つなげば (持ち主・watcher・PUBLIC) 今も呼べる。
--   関数の既定は PUBLIC EXECUTE。`alter default privileges … in schema …` は既定の PUBLIC を消せない (schema ごとの既定は全体の既定に「足す」だけ)。
--
-- やること: 下の 10 の関数 (署名ごと) の EXECUTE を **全員から** 外す = PUBLIC・watcher・profit_reader (あれば)・持ち主 (migration を流す Render の default user) と、
--   今の権限の表 (proacl) に載っている全部の役割。最後に「10 の関数の権限の表が空」を確かめ、違えば例外 = この取引ごと巻き戻す (fail-closed)。
--   🚨 持ち主自身から外しても効く (PostgreSQL 18.4 の実機で確かめた: 持ち主が呼ぶと 42501)。ただし持ち主は **付け直せる** (持ち主は常に GRANT の権限を持つ)
--      = この封鎖は「うっかり・ほかの接続から呼べない」まで。呼べるように戻すには、人が明示の GRANT を書く必要がある。
--      持ち主を専用の NOLOGIN の役割に移すのは PR 1b (別の管理主体が profit_definer と migration の deployer を用意する)。この PR では役割を作らない・持ち主も移さない。
--   🚨 TEMP の権限は変えない (持ち主の relink・reresolve_order_lines・merge_duplicate_suppliers が一時の表を使う)。監査の結果を出すだけ (heavy-entry-manifest.mjs --verify)
--   🚨 この 10 以外の重い入口 (mart.finance_daily_range・ad_efficiency・sku_activity ほか) は、署名つきの棚卸し scripts/company-db/heavy-entry-manifest.mjs に
--      「guard_later (正当な呼び手がいる = 後の PR で共通の lock に参加させるか外す)」と理由つきで記録し、権限は変えない = 今も DB に直接つなげば呼べる (PR 1a の範囲の外)
--   🚨 外した関数を create or replace するときは、持ち主にも EXECUTE が無いので、関数の検査 (validator) が 42501 で止まる (実機で確かめた)。
--      直すときは同じ取引で `grant execute … to current_user` → create or replace → この migration と同じに全員から外す → 空を確かめる (README の約束)
--   🚨 これから作る関数は、作った直後に同じ取引で署名ごとに REVOKE する (全体の既定 = PUBLIC EXECUTE は変えない = PR 1b)。試験 test-company-db-profit-fn-revoke.mjs が
--      重い入口の規則 (mart の関数・期間や件数の引数・本体が閉じた関数を呼ぶ) で pg_proc と manifest を突き合わせ、分けていない関数があれば落ちる
--
-- 閉じる関数 (D-60 の重い計算と、その内部の部品。全部 SECURITY INVOKER = 呼び手の権限で動く・呼び手は中で呼ぶ関数の EXECUTE も要る):
--   0049: mart.amazon_profit_daily_range / mart.amazon_profit_day_totals_range (公開の 2 つ)・mart._amazon_profit_totals・mart._amazon_profit_rows (0050 で差し替え)・
--         mart._amazon_profit_finance_days・mart._amazon_profit_ad_days・mart._amazon_profit_ad_children・mart._amazon_easy_ship_alloc・mart.amazon_profit_assert_args
--   0047 / 0050: mart.finance_daily_sku_range (日 × 正規化 SKU の財務。呼ぶのは 0049 の利益の関数だけ。watcher は使っていない = 0047 / 0050 の GRANT も外す)
-- 閉じない (権限は前のまま): 軽い関数 = core.finance_coverage_state・core.finance_month_settled・core.finance_policy_fingerprint・core.finance_policy_snapshot (coverage の受け口・見張り)・
--   mart.amazon_profit_composition_audit_since()・mart.amazon_account_fee_tax_rate(text) (定数) / 重いが正当な呼び手がいる = mart.finance_daily_range (受け口 GET /order-finance/daily) ほか (manifest の guard_later)
-- 呼び手の調べ (2026-10-03): アプリ (router・ingest・watch・miniPC の送り手・measure) で閉じる関数を呼ぶ所は無い (受け口は 503 で関数を呼ばない)。
--   閉じる関数を呼ぶ関数は閉じる関数だけ。SECURITY DEFINER の呼び手も無い (あっても定義者 = 持ち主の EXECUTE が無いので 42501 になる)。呼ぶのは試験だけ (PGlite = superuser)
-- 2 回流しても同じ (revoke は何度でも同じ・確かめも同じ)。表・関数の中身は変えない。利益の値は計算しない。

do $$
declare
  v_sigs text[] := array[
    'mart.amazon_profit_daily_range(smallint, text, text, date, date)',
    'mart.amazon_profit_day_totals_range(smallint, text, text, date, date)',
    'mart._amazon_profit_totals(smallint, text, text, date, date)',
    'mart._amazon_profit_rows(smallint, text, text, date, date, mart.amazon_profit_finance_day[], mart.amazon_profit_ad_day[], mart.amazon_profit_ad_child[], mart.amazon_easy_ship_alloc_row[])',
    'mart._amazon_profit_finance_days(smallint, text, text, date, date)',
    'mart._amazon_profit_ad_days(smallint, text, text, date, date)',
    'mart._amazon_profit_ad_children(smallint, text, text, date, date)',
    'mart._amazon_easy_ship_alloc(smallint, text, text, date, date)',
    'mart.amazon_profit_assert_args(smallint, text, text, date, date)',
    'mart.finance_daily_sku_range(smallint, text, text, date, date)'
  ];
  v_sig  text;
  v_oid  oid;
  v_role text;
  v_bad  text;
begin
  foreach v_sig in array v_sigs loop
    v_oid := v_sig::pg_catalog.regprocedure;   -- 無ければここで例外 (署名の書き違いを黙って飛ばさない)
    execute pg_catalog.format('revoke execute on function %s from public', v_sig);
    -- 今の権限の表に載っている全部の役割 (watcher・持ち主・ほか) + 持ち主 + watcher・profit_reader (あれば)。grant option を持つ役割から先に配られた分も cascade で外す
    for v_role in
      select a.grantee::pg_catalog.regrole::text from pg_catalog.pg_proc p cross join lateral pg_catalog.aclexplode(p.proacl) a where p.oid = v_oid and a.grantee <> 0
      union select p.proowner::pg_catalog.regrole::text from pg_catalog.pg_proc p where p.oid = v_oid
      union select r.rolname::pg_catalog.regrole::text from pg_catalog.pg_roles r where r.rolname in ('watcher', 'profit_reader')
    loop
      execute pg_catalog.format('revoke execute on function %s from %s cascade', v_sig, v_role);
    end loop;
  end loop;

  -- 確かめ: 10 の関数の権限の表が空 (null = 既定 = 持ち主 + PUBLIC も不可)。違えば例外 = この取引ごと巻き戻す
  --   (持ち主でない役割で流した = revoke が「何も外せない」の警告だけで通る、を止める)
  select pg_catalog.string_agg(p.oid::pg_catalog.regprocedure::text || ' = ' || coalesce(p.proacl::text, '(既定 = 持ち主 + PUBLIC)'), ' / ')
    into v_bad
    from pg_catalog.pg_proc p
   where p.oid = any (select s::pg_catalog.regprocedure::oid from pg_catalog.unnest(v_sigs) s)
     and (p.proacl is null or pg_catalog.cardinality(p.proacl) > 0);
  if v_bad is not null then
    raise exception 'd60_revoke_incomplete: 重い関数の EXECUTE が残った (%)。関数の持ち主の役割で流す', v_bad using errcode = '42501';
  end if;
end $$;
