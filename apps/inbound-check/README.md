# 入荷受付チェック (iPad) — apps/inbound-check

紙の「入荷受付伝票」を iPad に置き換えるアプリ。ロジザードに登録済みの入荷受付伝票 (AR番号) を CSV で取り込み、
届いた行を1タップで消し込み、伝票ごと・全体で「何件中何件確認したか」を見せる。横に入庫情報管理の入数・行き先 (ピックロケ / 直接ピック / 保管荷姿 / いろは) を出す。

- 設計正本: AI_reference『システム設計/入荷受付チェック_要件定義_20260901.md』(v1.1)
- **やらないこと**: 発注書 (PO) との連動・発注残の表示や消込 (上流=発注管理の入荷予定タブ、下流=発注管理 P14 に既にある)。ロジザードへの書き戻し
- **設計の軸**: 「現物が届いたかの確認台帳」に徹する。在庫計上はロジザード、消込は P14

## URL

| パス | 誰が | 内容 |
|---|---|---|
| `/apps/inbound-check/` | 登録端末 (iPad) or ポータルセッション | 作業画面 (PWA) |
| `/apps/inbound-check/api/state` | 同上 | 一覧の状態 (5秒ポーリング) |
| `/apps/inbound-check/api/lines/check` `/uncheck` | 同上 | 1タップ確認 / 取消 |
| `/apps/inbound-check/admin` | ポータルセッション (アプリ権限) | 管理画面。CSV 取込はアプリ利用者全員 |
| `/apps/inbound-check/admin/devices` `/workers` `/history(.csv)` | admin のみ | 端末登録・作業者・履歴 |
| `/apps/inbound-check/device/exit` (POST) | 端末 | 端末Cookie を外す |

server.js では `requireAppAccess` を掛けずに mount する (端末Cookie を通すため)。認可は router 内で全ルートに掛ける (manifest.json だけ素通し)。

## データの流れ (PR1 = 手動取込)

```
ロジザード 入荷状況照会 [FA04_01] (当日〜7日前・受付済) → CSV エクスポート (CA04001_*.csv, Shift-JIS, 58列)
  → 管理画面「取り込む」 (file_modified = ブラウザ File.lastModified を CSV 生成時刻として送る)
  → f_inbound_check_batches (active は常に1件) / slips / lines / line_state (全行 unchecked から)
  → iPad: GET /api/state で f_inbound_info (入数等) と mirror_logizard_stock (ピックロケ) を商品単位で結合して表示
```

- 明細キー = `AR番号|入荷管理行番号|入荷管理詳細行番号`。商品IDはキーにしない
- **取込は fail-closed**: 必須列欠落 / 列数不一致 / 数値でない予定数 / AR空 / 明細キー重複 / 同一ハッシュ / 生成時刻が active より古い → 拒否して active を据え置く。**0件は正常** (行数の前回比ガードは置かない)
- **確認状態はバッチ単位** (毎朝リセット)。前バッチで確認済みだった行は「前回 … が確認済み」を薄字で出す (状態は引き継がない)
- **同時操作**: `UPDATE line_state … WHERE status='unchecked' AND (expect_version IS NULL OR version=?)` の原子的条件付き UPDATE。負けたら 409 `conflict` + 現在状態。旧 batch_id の操作は 409 `stale_batch`。画面は 409 を受けたら最新状態に置き換える
- events は append-only。取消は `reverted_event_id` で打ち消した確認を指す
- ピックロケ = `mirror_logizard_stock` を商品×(ブロック略称-ロケ) で集約し、`P3F*` を在庫数順に最大3件。P3F が無ければ他ブロック先頭1件を「保管」。在庫ゼロ商品は「ロケ未取得」(商品マスタ側の値は PR3 で補完)
- 保持期間: superseded バッチとその子 = 365日、取込ログ = 90日 (取込時に掃除)

## 作業者 (名前タップ) = スタッフマスタ

自前の作業者表は持たない。名前タップの候補は `apps/staff` (staff.db) の**有効かつ倉庫作業 (warehouse) の役割を持つスタッフ**で (事務担当は出ない)、`worker_code` = スタッフ管理番号 (`staff_no`)、表示名 = 短い表記 (無ければ正式表記)。確認イベントには `worker` (表示名) と `staff_id` を記録する。追加・無効化は `/apps/staff/` (管理者)。

## 端末 (iPad) の登録

倉庫の iPad で管理者としてログイン → 管理画面「この端末を登録」 → トークンは httpOnly Cookie `ic_device` (path=/apps/inbound-check、400日) としてその端末だけに渡り、**同時に管理者セッションを破棄**する (共用端末に管理者ログインを残さない)。⚠ ホーム画面に追加した PWA から開いて登録する (Safari 本体と Cookie 保存領域が別)。

## テスト

```
node scripts/test-inbound-check.mjs [CA04001_*.csv]        # DB 層 + CSV パーサ (60 項目)
node scripts/smoke-inbound-check-http.mjs [CA04001_*.csv]  # server.js を起動して HTTP 経路 (38 項目)
```

## 次 (PR2 / PR3)

- PR2: miniPC の Playwright で FA04_01 → CSV を 8:30 / 12:00 に自動エクスポート → rclone → Drive → Render 取込 (`source='auto'`)。jobs-registry 登録 + dead-man ping + 失敗 GChat
- PR3: 在庫ゼロ商品のピックロケ補完 (商品マスタ)、実機で決まった表示の手直し、Stream Deck の紙印刷を障害時のみに降格
- **PR1 の受け入れ条件**: 2日分の実 CSV で「検品済みの伝票が受付済の検索から消える」ことを確認する (要件定義 §3.1)
