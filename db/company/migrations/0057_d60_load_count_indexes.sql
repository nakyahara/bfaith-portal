-- migrate:concurrent-index
-- D-60 PR 3a-i の index 6 つ (負荷の数え上げの段が index だけで上限 + 1 行目で止まるため)
--   設計の正本 = AI_reference『システム設計/CompanyDB構想/13_Amazon利益のmart_設計_20260930.md』(v40・D-60 v3.15) の
--   付録 B (B.2 段のパイプライン・B.3 許す plan の木) と §3.10「migrate の runner の契約 (3a-i)」。runner = scripts/company-db/migrate.mjs (#1606)
--
-- 🚨 番号は仮 (今の master の次の空き)。PR #1605 (0057)・PR #1607 (0058) の後にマージし、番号はその時の次の空きに付け替える
--    (この file と横の <番号>_d60_load_count_indexes.expect.json の 2 つを git mv するだけ = この本文と expect.json は番号を書かない・checksum も番号を含まない。
--     runner は欠番を許さない = 0059 を先に置くと 0057 / 0058 が無い間は全部の migrate が止まるので、仮の番号は今の次の空きにした)
-- 🚨 runner の契約: 1 行目の印 = 取引の外で 1 文ずつ (CREATE INDEX CONCURRENTLY)・許す文は create index concurrently if not exists と drop index concurrently if exists だけ
--    (名前・schema・表は引用しない名前・using btree だけ・with / tablespace なし)。流す前に全部の文を検査・空き容量 (Render のメトリクス) が足りない / 読めない = 流さない。
--    作った後に indisvalid・indisready・indislive と正規化した catalog の属性 (横の expect.json = 使い捨ての PG 18 で流して --index-expect で作った物) が一致したときだけ記録。
--    途中で落ちた = 記録しない → もう一度流すと作り済みの valid は飛ばし、invalid は drop index concurrently してから作り直す (runbook = migrate.mjs の CONCURRENT_INDEX_RUNBOOK)。
--    古い runner (lock なし) で流すと CREATE INDEX CONCURRENTLY cannot run inside a transaction block で失敗する (黙って通らない)。
--    PGlite (試験) は同じ文から concurrently を外し、ふつうの取引で流す (属性の検証は同じ)。
-- 🚨 RENDER_PG_HOST_MAPPING.confirmed = false の間 (Render の回答 = 設計 §5 の英文の質問 14 の前) は、本番の CLI の容量の読み手が照合を通さない = この file は本番で流れない。
--    → この file は Render の回答 (host の対応を確かめて confirmed = true にする PR) の後にマージする (先にマージすると、次の本番の migrate が全部 HOST_MAPPING_UNCONFIRMED で止まる)
--
-- 段との対応 (付録 B.2 の表・本文の SQL の文字は付録 B が正本 = PR 3a で apps/company-db/profit/load-count.mjs に同じ文を置く):
--   ④a 出品 (直接の一致)      = core.listings          (company_id, mall, listing_norm)  … 今の一意 (mall, shop_code, listing_norm) は間に shop_code があり company_id も無い
--   ④b 出品 (別名・広告だけ)  = core.external_ids      (company_id, system, external_norm) where entity_type = 'listing' and valid_to is null
--                                                        … core.resolve_listing_id の向き (正規化した target → entity_id)。今の ux_external_ids_active は間に id_kind がある
--   ⑥ / ⑥b 原価の候補 / 全体 = core.sku_costs         (company_id, sku_id, valid_from)  … 今は有効な行だけの部分 index (ux_sku_costs_active) = 履歴を引けない
--   ⑨a 監査 (構成・今の側)    = events.master_change_events の CASE の式 (entity_type = 'listing_component' の entity_key ->> 'listing_id')
--   ⑨b 監査 (構成・旧い側)    = events.master_change_events の CASE の式 (listing_component の UPDATE で attribute = 'listing_id' の old_value #>> '{}')
--                                … ⑨a / ⑨b は文に entity_type の等号を書かない = ix_master_change_events_entity (entity_type, …) + Filter を選べない (設計 v3.8)。
--                                  式は付録 B の本文の式と同じ (比べるのは catalog の pg_get_expr)・部分 index の条件 (<式> is not null) は本文の = any(…) (strict) から導ける
--   (b)-2 raw.purge_superseded_observations の count = raw.logizard_inventory_observations (observed_at)
--   (既存の index で足りる段 = ① ix_order_finance_daily_date / ② ad_spend_daily_pkey / ③ order_finance_daily_pkey / ⑤ listing_components_pkey /
--    ⑦ ix_sku_cost_observed_sku / ⑧ ix_master_change_events_entity / (b)-1 ix_ad_spend_daily_unlinked = この file では作らない)
-- 試験 = scripts/test-company-db-d60-load-count-indexes-pg.mjs (本物の PG 18 の runner の legacy の道・PGlite の道・expect.json との一致・門の GUC の下の 14 段の計画の形)

create index concurrently if not exists ix_listings_company_mall_norm
  on core.listings (company_id, mall, listing_norm);

create index concurrently if not exists ix_external_ids_listing_alias
  on core.external_ids (company_id, system, external_norm)
  where entity_type = 'listing' and valid_to is null;

create index concurrently if not exists ix_sku_costs_company_sku_from
  on core.sku_costs (company_id, sku_id, valid_from);

create index concurrently if not exists ix_master_change_events_component_listing
  on events.master_change_events ((case when entity_type = 'listing_component' then entity_key ->> 'listing_id' end))
  where (case when entity_type = 'listing_component' then entity_key ->> 'listing_id' end) is not null;

create index concurrently if not exists ix_master_change_events_component_old_listing
  on events.master_change_events ((case when entity_type = 'listing_component' and operation = 'UPDATE' and attribute = 'listing_id' then old_value #>> '{}' end))
  where (case when entity_type = 'listing_component' and operation = 'UPDATE' and attribute = 'listing_id' then old_value #>> '{}' end) is not null;

create index concurrently if not exists ix_logizard_inventory_obs_observed_at
  on raw.logizard_inventory_observations (observed_at);
