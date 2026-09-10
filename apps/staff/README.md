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

`staff_roles` = **役割** (どの現場の名前タップに出すか。1人が複数持てる):

| role | ラベル | 効果 |
|---|---|---|
| `warehouse` | 倉庫作業 | ピッキング・梱包・入荷受付チェックの名前タップに**出る** |
| `office` | 事務 | 倉庫系アプリには出ない (事務担当を現場の一覧に並べない — 中原さん 2026-09-01) |
| `iroha` | いろは現場 | いろは在庫化・FBA箱詰め (iPad) の名前タップに**出る**。役割に関係なく社員は `/export` に出す (消えると取込側が警告する)。**区分 `iroha` (利用者) だけ `/export` に出さない** |

`listTapCandidates()` は既定で `warehouse` だけを返す。全員が欲しい呼び出し側は `{ role: null }` を渡す。
役割は管理画面のチェックボックスで即保存 (行の「保存」とは独立)。

`staff_audit` = 変更履歴 (append-only: create / update / deactivate / reactivate / seed、before/after JSON、actor)。

## 職員PIN (2026-09-10 共通化)

いろは在庫化 / FBA箱詰め がアプリごとに持っていた職員PIN を、この表に 1 つにした (`pin_hash` / `pin_salt` / `pin_fails` / `pin_lock_until` / `pin_set_at`)。

- `setStaffPin(id, pin, actor)` / `verifyStaffPin(id, pin)` (5 回失敗で 10 分ロック・DB 永続)。区分 `iroha` (利用者) は持てない
- 🚨 `pin_salt` は **方式込みの文字列** (`staff-pin:<hex>`)。旧アプリのハッシュ (`iroha-pin:<hex>` / `fbx-pin:<hex>`) をそのまま持ち越して照合できる
- 読み出し (`getStaff` / `listStaff` / `/export`) は `pin_hash` / `pin_salt` を**絶対に出さない** (`PUBLIC_COLS`)。`pin_set` (有無) だけ
- 設定は 管理画面 (`POST /api/staff/:id/pin`) と、各アプリの iPad の名簿画面 (職員PIN ゲート) の両方から

## いろはの名簿 = この表の鏡 (`roster-link.js`)

いろは在庫化 `f_iroha_workers` / FBA箱詰め `fbx_workers` は表を残したまま (作業の記録がその id を指すため)、`staff_id` で紐付けた**鏡**になった。
名前・区分 (kind=iroha → 利用者 / それ以外 → 職員)・有効 (active かつ 役割 iroha)・並び・PIN の有無 を `staff_meta.roster_rev` が進んだときだけ写す。

- アプリからの「追加」= ここに作る (番号は `IROHA-001`〜 自動採番・あとで直せる) + 役割 iroha。「無効」= 役割 iroha を外すだけ (退職はここで)
- 起動時の移行 (`migrateLegacyRoster`): 名前が完全一致 (空白無視・NFKC) する人が 1 人だけなら紐付け、それ以外は新規。利用者は kind=iroha の人としか一致させない。PIN はハッシュのまま持ち越す
- 間違えた紐付けは 各アプリの管理画面「スタッフマスタ (紐付け直し)」で直す (`relinkRosterWorker`。元の行の役割 iroha を外し、自動採番の行で他に役割が無ければ無効に。PIN は引き継ぐ)

## 使っているアプリ

| アプリ | 状態 |
|---|---|
| 入荷受付チェック (`apps/inbound-check`) | ✅ 直接参照 (worker_code = staff_no、events.staff_id) |
| いろは在庫化 (`apps/iroha-work`) / FBA箱詰め (`apps/fba-box`) | ✅ 鏡 (`roster-link.js`、2026-09-10)。名簿・職員PIN を共通化。役割 `iroha` の人が名前タップに出る |
| ピッキング・梱包 (miniPC `pk_workers`) | ✅ 同期あり: `apps/picking/staff-sync.js` が `/export` を取得して `pk_workers` へ反映 (picking-drive-poller に相乗り・1時間ごと)。詳細は `apps/picking/README-staff-sync.md` |
| 問い合わせ管理 / 商品登録 | ⬜ `staff_id` 列の追加は後回し (事務スタッフは倉庫と集合が違う) |

## テスト

```
node scripts/test-staff.mjs               # seed・追加検証・楽観ロック・無効化・監査・inbound-check からの参照
node scripts/test-staff-roster-link.mjs   # 職員PIN・役割 iroha・export の絞り込み・鏡 (写す/移行/追加/無効/紐付け直し)
```
