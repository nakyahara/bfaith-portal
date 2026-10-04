-- migrate:concurrent-index
-- D-60 PR 3a-i の index (負荷の数え上げの段が index だけで上限 + 1 行目で止まるため)・5 つの file の 1/5 = core.listings (④a)
--   1 file = 1 表 (設計 §3.10 の推し = 途中で落ちたときの再開の範囲と容量の判定を 1 表に収める)。5 つ = listings → external_ids → sku_costs → master_change_events → logizard_inventory_obs
--   設計の正本 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』(v40・D-60 v3.15) の
--   付録 B (B.2 段のパイプライン・B.3 許す plan の木) と §3.10「migrate の runner の契約 (3a-i)」。runner = scripts/company-db/migrate.mjs (#1606)
--
-- 🚨 番号は仮 (今の master の次の空きから 5 つの連番)。PR #1605・PR #1607 の後にマージし、5 つの組 (この .sql と横の .expect.json) を同じ順のまま
--    その時の次の空きへ git mv で付け替える (本文と expect.json は番号を書かない・checksum も番号を含まない・試験は名前 d60_load_count_idx_<表> で探す)。
--    runner は欠番を許さない = 仮の番号は今の次の空きにした
-- 🚨 runner の契約: 1 行目の印 = 取引の外で 1 文ずつ (CREATE INDEX CONCURRENTLY)・許す文は create index concurrently if not exists と drop index concurrently if exists だけ。
--    流す前に全部の文と横の expect.json (使い捨ての PG 18 で流して --index-expect で作った物) を検査・空き容量 (Render のメトリクス) が足りない / 読めない = 流さない。
--    作った後に indisvalid・indisready・indislive と正規化した catalog の属性が expect.json と一致したときだけ記録。
--    途中で落ちた = この file は記録しない (前の file = 別の表は記録済みのまま) → もう一度流して完成させる (作り済みの valid は飛ばし、
--    invalid は drop index concurrently してから作り直す・runbook = migrate.mjs の CONCURRENT_INDEX_RUNBOOK)。
--    古い runner (lock なし) で流すと CREATE INDEX CONCURRENTLY cannot run inside a transaction block で失敗する (黙って通らない)。
--    PGlite (試験) は同じ文から concurrently を外し、ふつうの取引で流す (属性の検証は同じ)。
-- 🚨 RENDER_PG_HOST_MAPPING.confirmed = true の PR (Render の回答 = 設計 §5 の英文の質問 14) の後にマージする
--    (前だと本番の CLI の容量の読み手が照合を通さず、次の本番の migrate が全部この file で止まる)
--
-- ④a 出品 (直接の一致) = core.listings (company_id, mall, listing_norm)
--    … 今の一意 (mall, shop_code, listing_norm) は間に shop_code があり company_id も無い
-- (既存の index で足りる段 = ① ix_order_finance_daily_date / ② ad_spend_daily_pkey / ③ order_finance_daily_pkey / ⑤ listing_components_pkey /
--  ⑦ ix_sku_cost_observed_sku / ⑧ ix_master_change_events_entity / (b)-1 ix_ad_spend_daily_unlinked = 5 つの file のどれでも作らない)
-- 試験 = scripts/test-company-db-d60-load-count-indexes-pg.mjs (本物の PG 18 の runner の legacy の道・PGlite の道・expect.json との一致・門の GUC の下の 14 段の計画の形)

create index concurrently if not exists ix_listings_company_mall_norm
  on core.listings (company_id, mall, listing_norm);
