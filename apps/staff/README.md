# スタッフマスタ — apps/staff

「人」の正本。アプリごとの作業者リスト (picking `pk_workers` / inquiry-hub `staff_members` / product-hub `ph_staff`) とは毛色が違う表で、
将来の勤怠・シフト (`staff_shifts` / `staff_attendance`、未作成) の親になる。2026-09-01 中原さん方針で新設 (まず形だけ)。

- DB: `staff.db` (DATA_DIR)。render-backup の対象 (vacuum)。倉庫ミラーには混ぜない
- 画面: `/apps/staff/` (**管理者のみ**)。追加・行ごと編集 (楽観ロック)・無効化/再有効化。削除はしない (他アプリの履歴が `staff.id` を参照)
- 初期データ: `seed/initial-staff.json` (13名。空のときだけ投入・`staff_no` 冪等)。`staff_no` が YYYYMMDD 形式なら `joined_on` に写す
- 他アプリからの参照: 同一プロセスは `./db.js` を import (`listTapCandidates()` = 名前タップ候補 `{staff_id, staff_no, name}`)。
  別マシン (miniPC の picking/packing) は `GET /apps/staff/export` (`Authorization: Bearer $STAFF_EXPORT_TOKEN`。env 未設定なら 404) — 同期側は次の PR

## staff テーブル

| 列 | 内容 |
|---|---|
| `id` | 内部ID (連番・不変)。他アプリ・勤怠・シフトはこれを参照 |
| `staff_no` | スタッフ管理番号 (人が読む番号。`0001`〜 と 入社日 `YYYYMMDD` が混在する運用のため TEXT・一意) |
| `display_name` / `short_name` | 正式表記 / 名前タップ用の短い表記 (任意) |
| `kana` / `kind` | よみ / 区分 (`employee` 社員・`part_time`・`contractor` 外注・`iroha`・`other`) |
| `portal_email` | ポータルログインとの紐付け (任意・小文字) |
| `joined_on` / `left_on` / `active` | 入社日・退職日・有効 (無効化で `left_on` を自動セット、再有効化でクリア) |
| `sort` / `note` / `version` | 並び・メモ・楽観ロック |

`staff_audit` = 変更履歴 (append-only: create / update / deactivate / reactivate / seed、before/after JSON、actor)。

## 使っているアプリ

| アプリ | 状態 |
|---|---|
| 入荷受付チェック (`apps/inbound-check`) | ✅ 直接参照 (worker_code = staff_no、events.staff_id) |
| ピッキング・梱包 (miniPC `pk_workers`) | ⬜ 次の PR: `pk_workers.staff_id` + daily-sync に `/export` 取得ステップ |
| 問い合わせ管理 / 商品登録 | ⬜ `staff_id` 列の追加は後回し (事務スタッフは倉庫と集合が違う) |

## テスト

```
node scripts/test-staff.mjs     # 32 項目 (seed・追加検証・楽観ロック・無効化・監査・inbound-check からの参照)
```
