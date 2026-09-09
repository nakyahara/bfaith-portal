-- 0008 参照データの初期値 (会社・倉庫・属性の解決規則 v1)。データ移行 (商品・SKU) は scripts/company-db/load-*.mjs で行う
--
-- 🚨 ここに載せるのは「無いと表が使えない」最小限だけ。人・商品・仕入先はロードで入れる。

insert into core.companies (company_id, name, kind) values
  (1, 'B-Faith', 'parent'),
  (2, 'いろは', 'subsidiary')
on conflict (company_id) do nothing;

insert into core.warehouses (warehouse_id, company_id, code, name, kind) values
  (1, 1, 'MAIN', '自社倉庫 (本館 + いろは棟)', 'own'),
  (2, 1, 'FBA_JP', 'Amazon FBA (日本)', 'fba'),
  (3, 1, 'FBA_US', 'Amazon FBA (米国)', 'fba'),
  (9, 1, 'VIRTUAL', '仮想 (台車・ZZZ など)', 'virtual')
on conflict (warehouse_id) do nothing;

insert into core.rule_versions (rule_version, note) values ('v1', '06 §3.6 の初期規則 (2026-09-09)') on conflict do nothing;

-- 属性の解決規則 v1 (06 §3.6。priority が小さいほど優先。同じ属性・同じ包装範囲の中だけで比べる)
insert into core.attribute_resolution_rules (attribute, packaging_scope, source_system, priority, rule_version) values
  ('jan', 'item', 'product_hub', 1, 'v1'), ('jan', 'item', 'ne', 2, 'v1'), ('jan', 'item', 'logizard', 3, 'v1'),
  ('jan', 'item', 'amazon_catalog', 4, 'v1'), ('jan', 'item', 'rakuten', 5, 'v1'), ('jan', 'item', 'yahoo', 6, 'v1'),
  ('jan', 'item', 'fba_sheet_import', 7, 'v1'), ('jan', 'item', 'notion_import', 8, 'v1'),
  ('brand', 'item', 'product_hub', 1, 'v1'), ('brand', 'item', 'amazon_catalog', 2, 'v1'), ('brand', 'item', 'yahoo', 3, 'v1'), ('brand', 'item', 'qoo10', 4, 'v1'),
  ('manufacturer', 'item', 'product_hub', 1, 'v1'), ('manufacturer', 'item', 'amazon_catalog', 2, 'v1'),
  ('package_weight_g', 'package', 'measured', 1, 'v1'), ('package_weight_g', 'package', 'amazon_catalog', 2, 'v1'), ('package_weight_g', 'package', 'yahoo', 3, 'v1'),
  ('package_length_mm', 'package', 'measured', 1, 'v1'), ('package_length_mm', 'package', 'amazon_catalog', 2, 'v1'),
  ('package_width_mm', 'package', 'measured', 1, 'v1'), ('package_width_mm', 'package', 'amazon_catalog', 2, 'v1'),
  ('package_height_mm', 'package', 'measured', 1, 'v1'), ('package_height_mm', 'package', 'amazon_catalog', 2, 'v1'),
  ('unit_count', 'item', 'product_hub', 1, 'v1'), ('unit_count', 'item', 'manual', 2, 'v1'), ('unit_count', 'item', 'amazon_catalog', 3, 'v1'), ('unit_count', 'item', 'rakuten', 4, 'v1'),
  ('net_content', 'item', 'product_hub', 1, 'v1'), ('net_content', 'item', 'notion_import', 2, 'v1'), ('net_content', 'item', 'amazon_catalog', 3, 'v1'),
  ('release_date', 'item', 'product_hub', 1, 'v1'), ('release_date', 'item', 'yahoo', 2, 'v1'), ('release_date', 'item', 'amazon_catalog', 3, 'v1')
on conflict do nothing;
