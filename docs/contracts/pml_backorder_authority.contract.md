# データ契約: 注残 (発注残) の正本 — 2026-07-13 切替

## 背景

2026-07-13 から発注は bfaith-portal の発注管理アプリ (`apps/purchase-orders`, `po_*` テーブル) で行い、
**NE (ネクストエンジン) には発注を登録しない**。そのため NE 由来の注残は以後更新されず、
**ゼロになるのではなく古い値がそのまま残り続ける (legacy)**。

## 契約

| 用途 | 使うもの |
|---|---|
| 業務上の注残の正本 (商品別) | **`v_ledger_backorder_by_product`** (product_key → backorder_qty)。集計ロジックはここに一本化 |
| PML (商品管理リスト) 行を読む全アプリ | **`v_pml_rows_authoritative`** (published PML + 台帳注残の合成。`注残数` 列 = 台帳値、`ne_backorder_qty` = 参考のNE legacy値) |
| GAS → Google シート出力 | `/apps/warehouse-mirror/api/pml/published` (注残数をサーバ側で台帳値へ差替済み。`backorder_source: 'app_ledger'`) |

**禁止**: `mirror_pml_snapshot_rows.注残数` / NE由来 `発注残数` を「現在の注残」として直接参照すること。
(mirror はミラーの原則どおり NE が言う値を保持し続ける — 改変・センチネル化はしない。消費層で差し替える)

このリポジトリの静的契約テスト (`apps/purchase-orders/scripts/smoke.mjs` の「注残SSoT契約」) が、
許可リスト外のコードによる `mirror_pml_snapshot_rows` + `注残数` の直接参照を検出して落とす。

許可リスト (直接参照してよい場所):
- `apps/warehouse/**` (miniPC 側。raw/snapshot の生成元)
- `apps/warehouse-mirror/**` (ingest 検証・GAS endpoint の差替実装そのもの)
- `apps/purchase-orders/db.js` (正本ビューの定義) / `apps/purchase-orders/logic.js` (NEオーバーレイの表示用)
- `apps/product-management-list/router.js` (ロールバック fallback)

## ロールバック

`po_settings` の `backorder_source='ne'` (DB直、UIなし) で GAS 出力・商品管理リスト表示は NE 由来値に戻る
(発注アプリの要発注判定も同じ設定で NE 値に戻る)。`v_pml_rows_authoritative` は常に台帳値。

## NE に残っている旧伝票の注残

- アプリへ移行済み: 移行PO 43件 (`origin='migration'`, `ne_slip_number` で追跡可能)
- 意図的未移行: 2026-07-12 突合時の古いゾンビ注残 (69品目ほか)。必要になれば NE 発注残一覧を
  期間指定なしで再エクスポート → 発注残ページの初期取込へ (取込済み伝票は自動スキップ)
- **NE と アプリの注残を合算してはならない** (二重計上)

## 監視

- `GET /apps/purchase-orders/api/ledger/integrity` の warnings `backorder_not_in_pml`:
  台帳注残があるのに published PML の商品へ JOIN できない件数 (商品コード改廃・正規化不一致の検知)
