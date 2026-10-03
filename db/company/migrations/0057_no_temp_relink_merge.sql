-- 0057: 一時の表 (TEMP) を使う関数のうち 2 つを、一時の表を使わない形に書き直す (D-60 v3.6 の PR 1b-0 の一部・2026-10-04)
--   設計 = AI_reference CompanyDB構想/13 §3.10「PR 1b-0」と「明示の一時の表 (TEMP) の hard bound」(Codex R-D60-v3-6 H1)。Codex R-D60-v3-7 の判定 =
--   「relink_shipments_bulk と merge_duplicate_suppliers の TEMP なし化と比較の試験は着手してよい。reresolve_order_lines は止める」
--
-- なぜ: `temp_file_limit` は明示の一時の表を縛らない。TEMP を持つ LOGIN の役割は関数の外から任意の大きさの一時の表を作れる = PR 1b で TEMP の権限を
--   superuser でない全部の LOGIN の役割から外す。その前に、正当な呼び手が使う関数から一時の表を無くしておく (外した後も今までどおり動くように)。
--
-- 形 (Codex PR #1605 R1 High・R2 High / Medium への対応):
--   🚨 中間の結果を **変数に溜めない** (R1)。一時の表だった所は、文の中の CTE・並べ替え・hash (work_mem (hash は × hash_mem_multiplier) を超えたら一時のファイルに逃げる) にする。
--      変数に残すのは relink の数 3 つと、merge の「寄せる行 → 残す行」の対応 (bigint の配列 2 つ・仕入先 ≦ 5,000 行を同じ snapshot で数えてから作る = 約 80 KB が上限) だけ。
--   🚨 「列ごとに空でない最初の値」を array_agg(...)[1] で取らない (R2 High = array_agg はまとまりの値を全部 1 つの配列にする = バイト数の上限が無い)。
--      代わりに 窓関数で まとまりの中の順 (0027 と同じ順の鍵) に「空でない値の数」を数え (count(列) over w)、最初の 1 行 (数 = 1 かつ空でない) だけを
--      min(列) filter (...) で拾う = 集約の状態は 1 つの値だけ。窓の並べ替えと窓の行の置き場 (tuplestore) は work_mem を超えたら一時のファイルに逃げる。
--      → backend の heap の増え分 = work_mem の桁 + 1 行の値の大きさ (件数・まとまりの幅・文字の長さに比例しない)。AFTER の行 trigger の待ち行列は変えた行の数に比例する (0027 も同じ)
--   🚨 枠を明示する = どちらの関数にも work_mem = 4MB・hash_mem_multiplier = 2 を付ける (関数の中だけ・出るときに戻る。呼び手の work_mem に依らない)
--   🚨 文の中で「前の CTE を待つ」恒真の条件 ((select count(*) from 前) >= 0) は使わない (R2 Medium = PostgreSQL の契約ではない)。順は次の 2 つだけで作る:
--      (a) 別の文 (PL/pgSQL の文は順に流れる) (b) データの依存 (後の段が前の DML の RETURNING の出力を集約して使う = 集約は まとまりの行を全部読むまで値を出せない)
--
-- なにを (署名・戻り値・結果は変えない。CREATE OR REPLACE = 持ち主・EXECUTE の権限の表はそのまま):
--   1. core.relink_shipments_bulk(smallint, bigint, integer) (0017 の置き換え)
--      0017 の 2 つの文 (候補を一時の表 _relink_cand2 に lock して入れる → 結ぶ) を、一時の表なしの 2 つの文にする:
--        文 1 = 候補を shipment_id の順に p_limit 件 for update (skip locked にしない) で lock し、数と最後の番号だけを変数に (0017 の 1 つ目の文と同じ行・同じ順の lock)
--        文 2 = (p_after, 最後の番号] の未結合の伝票を CTE (for update・照合用の鍵を列にする・0017 の教訓 = 式で結合しない) にして orders と列どうしの等結合で結び、
--               結んだ組 (shipment_id, order_id) の CTE から UPDATE する (新しい snapshot = 0017 の 2 つ目の文と同じく、lock を待った後に commit された注文が見える)
--      文 1 の候補は全部 lock を持っている = 文 2 でも変わらない → 文 2 の範囲の未結合の伝票 = 文 1 の候補 + 「文 1 の後に commit されて範囲に入った伝票」。
--      🚨 0017 との違い (同時に動くときだけ・試験で固定): ① 文 1 の後に commit されて (p_after, 最後の番号] に入った未結合の伝票も結ぶ (0017 は結ばずに cursor を越える =
--         次の先頭からの走査 (relink_rescan) まで残る。linked に入り examined には入らない。その伝票は文 2 で lock する) ② 店舗 (ne_shops) は文 2 の版を使う (0017 は文 1 の版を一時の表に写していた)。
--         ほかの同時の場合 (lock を待った相手が注文を足した・注文の鍵を変えた / 消した・伝票を直した) は 0017 と同じ。
--      🚨 関数に enable_nestloop = off・enable_mergejoin = off を付ける (= hash join を強く選ばせる。off は完全な禁止ではない = ほかの結合の方法が無ければ
--         planner は入れ子のループも選ぶ。設定は関数の中だけ・出るときに戻る。関数の中で発火する trigger・呼ばれる関数にも効く = 下の「trigger」):
--         0017 は一時の表を analyze して候補の本当の件数と列の分布を planner に渡していた。CTE は渡せない (CTE の列には統計が無い = 選択度は既定値) →
--         (a) 統計の無い / 古い orders では入れ子のループで 0021 の索引 (company_id, mall, scope_key, 日付 / 更新時刻) を引いて mall_order_no を Filter で見る
--         (b) work_mem が大きい (32MB) と、その索引の並びを使う merge join を (mall, scope_key) だけの鍵で選び mall_order_no を Join Filter で見る
--         = どちらも 候補 × 同じモールの全部の注文 = 件数の 2 乗 (使い捨ての PG で測った: (a) 注文 300k・統計なし 5,000 件 = 1 回 36 秒・100,000 件は 60 秒で打ち切り /
--         (b) 100,000 件・注文番号 1,000 文字・統計あり・work_mem 32MB = 9 分を超えても終わらない)。hash join は等号の条件を全部 hash の鍵に使う = 件数に比例
--      上限は今までどおり p_limit ≦ 100,000 (関数の中で強制)。戻り値 = linked・examined・last_id (候補 0 件なら null) = 0017 と同じ
--   2. core.merge_duplicate_suppliers() (0027 の置き換え)
--      0027 の一時の表 3 つ → ① _sup_merge = 上限つきの配列の変数 2 つ (寄せる行・残す行) ② _ss_merged・_dl_merged = 1 つの文:
--      「寄せる行を消す (DELETE … RETURNING で消した行の値を返す)」→ 残す行 (文の始めの snapshot) と消した行を まとまり (残す行, 商品) / (文書, 残す行) ごとに
--      0027 と同じ優先 (残す行 → 寄せる行の supplier_id の順) で列ごとに空でない最初の値にまとめる → **MERGE 1 つ** で 残す行を直す (WHEN MATCHED) / 足す (WHEN NOT MATCHED)
--      = 直すと足すは 1 つの DML (R2 Medium)。消すのが先 = まとめた値は消した行 (RETURNING) の集約 = そのまとまりの寄せる行を全部消してから出る
--      (代表の印 (ux_supplier_skus_primary = SKU ごとに代表は最大 1 つ) がぶつかる相手は同じまとまりの寄せる行だけ = 0027 の「消す → 直す → 足す」の前提を同じく守る)
--      MERGE は BEFORE INSERT の trigger を本当に足す行だけに発火する (INSERT … ON CONFLICT は直す行にも BEFORE INSERT を発火する = version の通し番号を余計に進める → 使わない)
--      🚨 MERGE の足すとき、文の snapshot の後に別の取引が同じ (残す行, 商品) / (文書, 残す行) を足して commit していたら 23505 になる (R2 Medium)
--         → その文だけ例外の塊 (subtransaction) で巻き戻し、新しい snapshot でやり直す (3 回まで。ほかの 23505 はやり直しても同じなので 3 回目で同じ誤りを返す)。
--         やり直しの後は相手の行を残す行として 0027 と同じ優先でまとめる (= 相手が先に commit した直列の順と同じ結果。0027 は相手の値を寄せる行の値で上書きした)。
--         文書の紐付けは仕入先への FK が無い = 窓が開く → やり直しで吸収する (試験で固定 = 違い ④)。
--         仕入先ごとの商品は、今の表では窓が開かない (Codex R3 Medium に試験で答えた): 残す仕入先に行を足す取引の FK の検査 (残す仕入先の行を FOR KEY SHARE) は、
--         merge の最初の文 (残す仕入先を直す UPDATE) が取った行の lock とぶつかって merge の commit まで待つ。その UPDATE は鍵の列を変えないが、FOR NO KEY UPDATE ではなく
--         鍵の lock (FOR UPDATE 相当) になる = core.suppliers に BEFORE UPDATE の行 trigger (touch・version・lifecycle) があり、一意の鍵 (company_id, code_norm) の
--         code_norm が生成列 (stored) = PostgreSQL は trigger が何を変えるか分からないので生成列を全部「変える列」に数え、鍵を変える UPDATE として trigger の前に行を lock する
--         (0027 も同じ文で同じ lock)。→ Codex R3 の競合 (寄せる行の商品の行を持つ相手を merge が待つ間に、相手が残す行へ同じ SKU を足す) は 相手が merge を待つ = deadlock
--         (1 本だけ 40P01) = 0027 と同じ。lock の強さとその理由 (trigger を外すと止めない) も試験で固定 (崩れたら試験が落ちる = 見直す)。
--         🚨 前提が崩れたとき (trigger を外す・PostgreSQL の版で変わる) だけ窓が開き、やり直しで吸収する = 違い ⑤ (潜在): 巻き戻した MERGE が直す / 足すときに使った
--         version の通し番号 (core.master_version_seq。直す 1・足す 2 = 既定値と BEFORE INSERT) は戻らない = 0027 より余計に進む。監査は巻き戻る (二重にならない)・各行の version は 1 つ
--         (version は「読んだ時と同じか」だけに使う = 番号の飛びは害が無い。試験の DB で trigger を外して固定)
--      文の順番は 0027 と同じ (仕入先を補う → 仕入先ごとの商品 → 発注・外部 ID → 文書の紐付け → 寄せた仕入先を消す → コードを揃える → 二重が残れば raise)
--      監査 (AFTER の行 trigger) は文の終わりに発火する。0027 は 消す・直す・足す が別の文 = 監査の並びは 消す…直す…足す。この形は 消す… の後、直すと足すが MERGE の行の順に混ざる
--      (1 行の変更の中の並び (列の名前の順) は同じ。行どうしの並びは 0027 でも契約ではない)
--      🚨 0027 との違い (同時に動くときだけ・試験で固定): ③ 消した行そのもの (lock を待った後の最新の版) の値でまとめる (0027 は直す前の値で上書きすることがあった)
--         ④ 上の 23505 のやり直し (文書の紐付け) ⑤ (潜在) 仕入先ごとの商品の 23505 のやり直しと version の通し番号の余分 (今の表では lock で起きない)
--      🆕 上限 = 仕入先 (core.suppliers の全部の行) ≦ 5,000 (超えたら 54000 で止まる = 何も変えない)。本番は約 40〜80 行
--      relink と同じ理由で enable_nestloop = off・enable_mergejoin = off (CTE の列に統計が無い = (残す行, 商品) の一部の鍵だけの結合を選ばせない)
--   どちらも search_path を pg_catalog, pg_temp に固定し、表と関数は schema で修飾する (SECURITY INVOKER のまま)
--   trigger: 関数の SET (planner の設定・work_mem) は、関数の中で発火する trigger と、それが呼ぶ関数にも効く。発火するのは
--     core.shipments (touch_updated_at_unless_seq_only・FK の検査) / core.suppliers (version・監査・touch・lifecycle・FK) / core.supplier_skus (version・監査・touch・
--     master_edit の guard・登録 CSV の guard・代表の仕入先の guard・FK) / docs.document_links (FK) / core.purchase_orders (touch・発行の guard 3 つ・FK) /
--     core.external_ids (writer・JAN の guard 3 つ・JAN の監査と version・FK)。結合があるのは ops.guard_master_edit_write (商品の親の輪の確かめ) と
--     ops.guard_reg_csv_live (SKU 数個の引き) だけ = どちらも画面のロール master_edit のときだけ (ほかの呼び手は最初の行で返る)。ほかは 1 つの表を引くか早く返すだけ。
--     一覧は試験 (scripts/test-company-db-no-temp-pg.mjs) で固定 (増えたら試験が落ちる = 見直す)
-- 変えないこと:
--   🚨 core.reresolve_order_lines (0024・一時の表 4 つ) には触らない (Codex R-D60-v3-7 = 上限の単位・cursor・期間の端・戻り値を直してから)
--   🚨 TEMP の権限は外さない (外すのは PR 1b)。EXECUTE の権限も変えない (merge_duplicate_suppliers を PUBLIC・watcher・runtime から外すのは PR 1b = 設計の「重い入口の分け」の表)
--   表・索引・trigger・データは変えない (関数の差し替えだけ)。2 回流しても同じ
--   🚨 本番 (temp_file_limit = -1 = 一時のファイルが無制限) には、temp_file_limit が有限になるまで当てない (Codex R2 Medium。README の 0057 の節)
-- 試験 = scripts/test-company-db-no-temp-pg.mjs (使い捨ての本物の PostgreSQL で、0017 / 0027 の版と 1 行も違わないこと・2 接続の同時の試験・TEMP の権限の無い役割で呼べること)
--   メモリ = scripts/company-db/measure-no-temp-mem.mjs (backend のピークのメモリ・一時のファイルを 0017 / 0027 とこの版で測る。最大のまとまりの幅 (仕入先 5,000 → 1) も。README の 0057 の節に表)

create or replace function core.relink_shipments_bulk(p_company_id smallint, p_after bigint default 0, p_limit integer default 20000)
returns table (linked integer, examined integer, last_id bigint)
language plpgsql set search_path = pg_catalog, pg_temp set enable_nestloop = off set enable_mergejoin = off set work_mem = '4MB' set hash_mem_multiplier = 2 as $$
declare
  v_linked integer := 0;
  v_examined integer := 0;
  v_last bigint := null;
begin
  if p_limit is null or p_limit <= 0 or p_limit > 100000 then raise exception 'p_limit must be 1..100000'; end if;
  -- 文 1: 候補 = 未結合の伝票を shipment_id の順に p_limit 件 lock する (for update。skip locked にしない = 飛ばした伝票を「完了」にしない)。
  --   変数に残すのは数と最後の番号だけ。店舗 (ne_shops) が無い / mall が null (対象外の店) の伝票も候補に数える (examined) が結ばれない (0016 / 0017 と同じ)
  select count(*)::integer, max(c.shipment_id) into v_examined, v_last
    from (select s.shipment_id
            from core.shipments s
           where s.company_id = p_company_id and s.order_id is null and s.ne_order_no is not null and s.shop_code is not null and s.shipment_id > coalesce(p_after, 0)
           order by s.shipment_id limit p_limit
           for update of s) c;
  if v_examined > 0 then
    -- 文 2: (p_after, 最後の番号] の未結合の伝票を結ぶ (新しい snapshot)。照合用の鍵は CTE の列 (0017 の教訓 = 式で結合しない)。
    --   CTE の列は 0017 の一時の表と同じ 4 つだけ (伝票の番号を写さない = 一時のファイルを増やさない)。
    --   CTE も for update = 文 1 の候補は lock を持っている (待たない・変わらない)。文 1 の後に範囲に入った伝票も ここで lock して最新の版で鍵を作る
    --   = UPDATE までに誰も直せない (CTE の値と伝票がずれない)。並べ替えない (文 1 の候補の lock の順は文 1 で決まっている。並べ替えは一時のファイルを増やす)
    --   結んだ組 (伝票, 注文) を別の CTE (j) にしてから UPDATE する = UPDATE の FROM の行は (shipment_id, order_id) だけ
    --   (FROM に c・orders を直接書くと、EvalPlanQual のために両方の行全体 (長い注文番号を 2 つ) を運び、hash の batch が一時のファイルに大きく逃げた)
    with c as materialized (
      select s.shipment_id, n.mall, n.scope_key, n.order_no_prefix || s.ne_order_no as mall_order_no
        from core.shipments s
        join core.ne_shops n on n.company_id = s.company_id and n.shop_code = s.shop_code and n.mall is not null
       where s.company_id = p_company_id and s.order_id is null and s.ne_order_no is not null and s.shop_code is not null
         and s.shipment_id > coalesce(p_after, 0) and s.shipment_id <= v_last
       for update of s
    ), j as materialized (
      select c.shipment_id, o.order_id
        from c
        join core.orders o on o.company_id = p_company_id and o.mall = c.mall and o.scope_key = c.scope_key and o.mall_order_no = c.mall_order_no
    )
    update core.shipments s set order_id = j.order_id
      from j
     where s.shipment_id = j.shipment_id and s.order_id is null;
    get diagnostics v_linked = row_count;
  end if;
  linked := v_linked; examined := v_examined; last_id := v_last;
  return next;
end
$$;

create or replace function core.merge_duplicate_suppliers()
returns table (merged_suppliers integer, supplier_skus_after integer, renamed_codes integer)
language plpgsql set search_path = pg_catalog, pg_temp set enable_nestloop = off set enable_mergejoin = off set work_mem = '4MB' set hash_mem_multiplier = 2 as $$
declare
  c_max_suppliers constant integer := 5000;
  c_max_tries constant integer := 3;
  v_suppliers bigint;
  v_drop bigint[];      -- 寄せる行 (消す supplier_id)。要素は 5,000 未満 (下の文で同じ snapshot の仕入先を数えてから作る)
  v_keep bigint[];      -- v_drop と同じ位置の残す行
  v_merged integer;
  v_skus integer;
  v_renamed integer;
begin
  -- 寄せる行 → 残す行。仕入先の数と同じ snapshot で数え、上限を超えたら配列を作らない (d が空) = 競合中に足されても 5,000 を超えない
  with c as materialized (
    select supplier_id, company_id, code, core.canonical_supplier_code(code) as canon from core.suppliers
  ), keeper as (
    select distinct on (company_id, canon) company_id, canon, supplier_id as keep_id
    from c
    order by company_id, canon, (code = canon) desc, supplier_id
  ), n as (
    select count(*) as n from c
  ), d as (
    select c.supplier_id as drop_id, k.keep_id
    from c join keeper k on k.company_id = c.company_id and k.canon = c.canon
    cross join n
    where c.supplier_id <> k.keep_id and n.n <= c_max_suppliers
  )
  select (select n.n from n), array_agg(d.drop_id order by d.drop_id), array_agg(d.keep_id order by d.drop_id)
    into v_suppliers, v_drop, v_keep
  from d;
  if v_suppliers > c_max_suppliers then
    raise exception 'merge_duplicate_suppliers: 仕入先が % 行 (上限 % 行) = 一度にまとめない (人が確かめる)', v_suppliers, c_max_suppliers using errcode = '54000';
  end if;
  v_merged := coalesce(pg_catalog.cardinality(v_drop), 0);

  if v_merged > 0 then
    -- 仕入先: 名前・発注方法・リードタイム・有効・連絡先を寄せる行から補う (残す行が空のときだけ)。
    --   寄せる行 (supplier_id の順) で条件に合う最初の値 = 窓で「条件に合う行の数」を数え、数 = 1 の行だけを拾う (array_agg で全部を配列にしない)
    with m as (
      select * from unnest(v_drop, v_keep) as u(drop_id, keep_id)
    ), r as (
      select m.keep_id, s.supplier_id, s.name, s.code, s.order_method, s.lead_time_days, s.email_to, s.email_cc, s.contact_name, s.fax_number, s.relay_to, s.order_memo, s.active,
             count(*) filter (where s.name is distinct from s.code) over w as n_name,
             count(s.order_method) over w as n_om, count(s.lead_time_days) over w as n_lt, count(s.email_to) over w as n_to, count(s.email_cc) over w as n_cc,
             count(s.contact_name) over w as n_cn, count(s.fax_number) over w as n_fax, count(s.relay_to) over w as n_relay, count(s.order_memo) over w as n_memo
      from m join core.suppliers s on s.supplier_id = m.drop_id
      window w as (partition by m.keep_id order by s.supplier_id rows between unbounded preceding and current row)
    ), agg as (
      select keep_id,
             min(name) filter (where n_name = 1 and name is distinct from code) as real_name,
             min(order_method) filter (where n_om = 1 and order_method is not null) as om,
             min(lead_time_days) filter (where n_lt = 1 and lead_time_days is not null) as lt,
             min(email_to) filter (where n_to = 1 and email_to is not null) as email_to,
             min(email_cc) filter (where n_cc = 1 and email_cc is not null) as email_cc,
             min(contact_name) filter (where n_cn = 1 and contact_name is not null) as contact_name,
             min(fax_number) filter (where n_fax = 1 and fax_number is not null) as fax_number,
             min(relay_to) filter (where n_relay = 1 and relay_to is not null) as relay_to,
             min(order_memo) filter (where n_memo = 1 and order_memo is not null) as order_memo,
             bool_or(active) as any_active
      from r
      group by keep_id
    )
    update core.suppliers k set
      name = case when k.name = k.code and a.real_name is not null then a.real_name else k.name end,
      order_method = coalesce(k.order_method, a.om),
      lead_time_days = coalesce(k.lead_time_days, a.lt),
      email_to = coalesce(k.email_to, a.email_to),
      email_cc = coalesce(k.email_cc, a.email_cc),
      contact_name = coalesce(k.contact_name, a.contact_name),
      fax_number = coalesce(k.fax_number, a.fax_number),
      relay_to = coalesce(k.relay_to, a.relay_to),
      order_memo = coalesce(k.order_memo, a.order_memo),
      active = k.active or a.any_active
    from agg a
    where k.supplier_id = a.keep_id;

    -- 仕入先ごとの商品: (残す行, 商品) ごとに 残す行 → 寄せる行 (supplier_id 順) の優先で列ごとに空でない最初の値。代表の印はどれかが代表なら代表
    --   1 つの文: 消す (del) → まとめる (r・g = 0027 の _ss_merged。残す行 (文の始めの snapshot) と del が返した消した行から) → MERGE で直す / 足す
    for v_try in 1 .. c_max_tries loop
      begin
        with m as (
          select * from unnest(v_drop, v_keep) as u(drop_id, keep_id)
        ), del as (
          delete from core.supplier_skus x using m where x.supplier_id = m.drop_id
          returning m.keep_id, x.supplier_id, x.sku_id, x.company_id, x.vendor_code, x.order_unit, x.stock_units_per_order_unit, x.min_order_qty, x.order_multiple,
                    x.unit_cost_jpy, x.lead_time_days, x.active, x.is_primary, x.created_at, x.created_by_type, x.created_by_id
        ), grp as (
          select x.supplier_id as keep_id, false as is_drop, x.supplier_id, x.sku_id, x.company_id, x.vendor_code, x.order_unit, x.stock_units_per_order_unit, x.min_order_qty,
                 x.order_multiple, x.unit_cost_jpy, x.lead_time_days, x.active, x.is_primary, x.created_at, x.created_by_type, x.created_by_id
          from core.supplier_skus x
          where x.supplier_id = any(v_keep)
          union all
          select d.keep_id, true, d.supplier_id, d.sku_id, d.company_id, d.vendor_code, d.order_unit, d.stock_units_per_order_unit, d.min_order_qty,
                 d.order_multiple, d.unit_cost_jpy, d.lead_time_days, d.active, d.is_primary, d.created_at, d.created_by_type, d.created_by_id
          from del d
        ), r as (
          -- まとまりの中の順 = 0027 の array_agg の order by (is_drop, supplier_id)。(残す行, 商品) の中で supplier_id は重ならない = 順は 1 つに決まる
          select grp.*,
                 row_number() over w as rn,
                 count(vendor_code) over w as n_vc, count(order_unit) over w as n_ou, count(stock_units_per_order_unit) over w as n_su,
                 count(min_order_qty) over w as n_moq, count(order_multiple) over w as n_om, count(unit_cost_jpy) over w as n_uc, count(lead_time_days) over w as n_lt
          from grp
          window w as (partition by keep_id, sku_id order by is_drop, supplier_id rows between unbounded preceding and current row)
        ), g as (
          select keep_id, sku_id, min(company_id) as company_id,
                 min(vendor_code) filter (where n_vc = 1 and vendor_code is not null) as vendor_code,
                 min(order_unit) filter (where n_ou = 1 and order_unit is not null) as order_unit,
                 min(stock_units_per_order_unit) filter (where n_su = 1 and stock_units_per_order_unit is not null) as stock_units_per_order_unit,
                 min(min_order_qty) filter (where n_moq = 1 and min_order_qty is not null) as min_order_qty,
                 min(order_multiple) filter (where n_om = 1 and order_multiple is not null) as order_multiple,
                 min(unit_cost_jpy) filter (where n_uc = 1 and unit_cost_jpy is not null) as unit_cost_jpy,
                 min(lead_time_days) filter (where n_lt = 1 and lead_time_days is not null) as lead_time_days,
                 bool_or(active) as active,
                 bool_or(is_primary) as is_primary,
                 min(created_at) as created_at,
                 min(created_by_type) filter (where rn = 1) as created_by_type,
                 min(created_by_id) filter (where rn = 1) as created_by_id
          from r
          group by keep_id, sku_id
        )
        merge into core.supplier_skus k
        using g on k.supplier_id = g.keep_id and k.sku_id = g.sku_id
        when matched and (k.vendor_code, k.order_unit, k.stock_units_per_order_unit, k.min_order_qty, k.order_multiple, k.unit_cost_jpy, k.lead_time_days, k.active, k.is_primary)
                         is distinct from (g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple, g.unit_cost_jpy, g.lead_time_days, g.active, g.is_primary) then
          update set vendor_code = g.vendor_code, order_unit = g.order_unit, stock_units_per_order_unit = g.stock_units_per_order_unit,
                     min_order_qty = g.min_order_qty, order_multiple = g.order_multiple, unit_cost_jpy = g.unit_cost_jpy,
                     lead_time_days = g.lead_time_days, active = g.active, is_primary = g.is_primary
        when not matched then
          insert (company_id, supplier_id, sku_id, vendor_code, order_unit, stock_units_per_order_unit, min_order_qty, order_multiple,
                  unit_cost_jpy, lead_time_days, active, is_primary, created_at, created_by_type, created_by_id)
          values (g.company_id, g.keep_id, g.sku_id, g.vendor_code, g.order_unit, g.stock_units_per_order_unit, g.min_order_qty, g.order_multiple,
                  g.unit_cost_jpy, g.lead_time_days, g.active, g.is_primary, g.created_at, g.created_by_type, g.created_by_id);
        exit;
      exception when unique_violation then
        -- 文の snapshot の後に別の取引が同じ (残す行, 商品) を足して commit した = この文を巻き戻し、新しい snapshot でやり直す (ほかの 23505 は c_max_tries 回目に同じ誤りを返す)
        if v_try >= c_max_tries then raise; end if;
      end;
    end loop;

    -- 発注・外部 ID
    update core.purchase_orders p set supplier_id = m.keep_id from unnest(v_drop, v_keep) as m(drop_id, keep_id) where p.supplier_id = m.drop_id;
    update core.external_ids e set entity_id = m.keep_id from unnest(v_drop, v_keep) as m(drop_id, keep_id) where e.entity_type = 'supplier' and e.entity_id = m.drop_id;

    -- 文書の紐付け: 1 つの文で 消す → まとめる (0027 の _dl_merged) → MERGE で直す / 足す (上と同じ形・同じやり直し)
    for v_try in 1 .. c_max_tries loop
      begin
        with m as (
          select * from unnest(v_drop, v_keep) as u(drop_id, keep_id)
        ), del as (
          delete from docs.document_links l using m where l.entity_type = 'supplier' and l.entity_id = m.drop_id
          returning l.document_id, m.keep_id, l.entity_id, l.link_role, l.created_at
        ), grp as (
          select l.document_id, l.entity_id as keep_id, false as is_drop, l.entity_id, l.link_role, l.created_at
          from docs.document_links l
          where l.entity_type = 'supplier' and l.entity_id = any(v_keep)
          union all
          select d.document_id, d.keep_id, true, d.entity_id, d.link_role, d.created_at from del d
        ), r as (
          select grp.*, count(link_role) over w as n_role
          from grp
          window w as (partition by document_id, keep_id order by is_drop, entity_id rows between unbounded preceding and current row)
        ), g as (
          select document_id, keep_id,
                 min(link_role) filter (where n_role = 1 and link_role is not null) as link_role,
                 min(created_at) as created_at
          from r group by document_id, keep_id
        )
        merge into docs.document_links l
        using g on l.document_id = g.document_id and l.entity_type = 'supplier' and l.entity_id = g.keep_id
        when matched and l.link_role is distinct from g.link_role then
          update set link_role = g.link_role
        when not matched then
          insert (document_id, entity_type, entity_id, link_role, created_at) values (g.document_id, 'supplier', g.keep_id, g.link_role, g.created_at);
        exit;
      exception when unique_violation then
        if v_try >= c_max_tries then raise; end if;
      end;
    end loop;

    delete from core.suppliers s using unnest(v_drop) as m(drop_id) where s.supplier_id = m.drop_id;
  end if;

  update core.suppliers set code = core.canonical_supplier_code(code) where code <> core.canonical_supplier_code(code);
  get diagnostics v_renamed = row_count;

  if exists (select 1 from core.suppliers group by company_id, core.canonical_supplier_code(code) having count(*) > 1) then
    raise exception 'merge_duplicate_suppliers: 仕入先コードを揃えても二重が残っている';
  end if;
  select count(*) into v_skus from core.supplier_skus;
  return query select v_merged, v_skus, v_renamed;
end $$;
