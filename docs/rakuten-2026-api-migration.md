# 楽天ウェブサービス 2026 API 移行手順書

**作成日**: 2026-05-07
**期限**: 2026-05-13 までに完了 (2026-05-14 から旧 API 完全停止)
**Branch**: `feature/rakuten-2026-api-migration`

## 背景

楽天ウェブサービス (公開 API、IchibaItem/Search 等) は新ダッシュボードへ完全移行中。

- **2026-02-10**: 新ダッシュボード公開、新規アプリ登録開始
- **2026-02-10〜2026-05-13**: 移行期間 (旧/新両方稼働)
- **2026-05-14 以降**: 旧ドメイン (`app.rakuten.co.jp`) と旧 API バージョン (`Search/20220601` 等) **恒久停止**

## コード変更内容 (実施済み、commit に含まれる)

### bfaith-portal (このリポジトリ)

| ファイル | 変更内容 |
|---|---|
| `apps/ranking-checker/auto-check.js` | endpoint `Search/20220601` → `Search/20260401` |
| `apps/ranking-checker/router.js` | endpoint `Search/20220601` → `Search/20260401` |
| `.env.example` | `RAKUTEN_AFFILIATE_ID` を追加、コメント追記 |

**注**: 既に新ドメイン (`openapi.rakuten.co.jp`) / `applicationId` + `accessKey` 認証 / `Referer` ヘッダー対応済だったため、変更は endpoint version のみ。

### G ドライブ (別管理) — 同様に対応済み

`g:\共有ドライブ\AI_reference\システム設計\` 配下:

| ファイル | 変更内容 |
|---|---|
| `楽天タイトル最適化/rakuten_title_tool.py` L124 | endpoint 更新 |
| `楽天タイトル最適化/仕様書.md` L64 | ドキュメント更新 |
| `rakuten-ranking-checker/auto_check.js` L19 | endpoint 更新 |
| `rakuten-ranking-checker/auto_check.py` L27 | endpoint 更新 |
| `rakuten-ranking-checker/server.js` L18 | endpoint 更新 |
| `rakuten-ranking-checker/server.py` L25 | endpoint 更新 |

## 中原さんが実施する作業

### Step 1: 新ダッシュボードでアプリ登録 (実施済み)

✅ `https://webservice.rakuten.co.jp/` にログイン → 新アプリ作成

発行された 3 つのキー:
- **applicationId** (UUID 形式)
- **accessKey** (`pk_` で始まる)
- **affiliateId** (オプション)

### Step 2: 各環境の `.env` を更新

#### 2-A. bfaith-portal (Render Singapore)

**Render dashboard → Environment** で以下を更新:

```env
RAKUTEN_APP_ID=<新 applicationId、UUID>
RAKUTEN_ACCESS_KEY=<新 accessKey、pk_ で始まる>
RAKUTEN_AFFILIATE_ID=<新 affiliateId>  # 新規追加
RAKUTEN_SHOP_CODE=b-faith              # 既存のまま
```

更新後 → **Manual Deploy** で反映。

#### 2-B. miniPC bfaith-portal (もしランキングチェッカー Runner が動いていれば)

`C:\tools\rankcheck-runner\.env` (or 該当 path) を編集:

```env
RAKUTEN_APP_ID=<新 applicationId>
RAKUTEN_ACCESS_KEY=<新 accessKey>
RAKUTEN_AFFILIATE_ID=<新 affiliateId>
```

更新後 → 次回 13:00 JST の Task Scheduler 実行で反映。

#### 2-C. 楽天タイトル最適化ツール (中原さん PC ローカル)

`g:\共有ドライブ\AI_reference\システム設計\楽天タイトル最適化\.env` を編集:

```env
RAKUTEN_APP_ID=<新 applicationId>
RAKUTEN_ACCESS_KEY=<新 accessKey>
```

#### 2-D. スタンドアロン版順位チェッカー (稼働 PC が特定できていれば)

`g:\共有ドライブ\AI_reference\システム設計\rakuten-ranking-checker\.env` (or 該当 PC ローカル) を編集:

```env
RAKUTEN_APP_ID=<新 applicationId>
RAKUTEN_ACCESS_KEY=<新 accessKey>
```

⚠️ **稼働 PC が不明** (memory: `project_rakuten_ranking_checker.md` Phase 3 段階で「ローカル版稼働 PC 未特定」)。bfaith-portal 統合版が動いていれば、こちらは未稼働の可能性。要確認。

### Step 3: 動作確認

#### 3-A. bfaith-portal 順位チェッカー

Render dashboard で deploy 完了後:

```
https://bfaith-portal.onrender.com/apps/ranking-checker/
```

→ 「今すぐチェック」を押して 5 件ほど成功すれば OK。失敗時はログを確認:
- 401/403: applicationId / accessKey が間違っている可能性
- 400: Referer ヘッダー / 許可された Web サイト設定 ミスの可能性
- 429: レート制限 (1.5 秒以上に拡張)

#### 3-B. 楽天タイトル最適化ツール

中原さん PC で実行:

```powershell
python -X utf8 "g:\共有ドライブ\AI_reference\システム設計\楽天タイトル最適化\rakuten_title_tool.py" "テスト"
```

→ `output/rakuten_data.csv` が生成されれば OK。

## 既知の制限・注意

### 許可された Web サイト

新ダッシュボードで以下を登録済み:
- `bfaith-portal.onrender.com`
- `*.bfaith-wh.uk`
- `127.0.0.1`

→ コード側の `Referer` ヘッダーは `https://rakuten.co.jp/` を送っているが、新仕様では **登録した Web サイトのドメインを Referer に送る必要がある可能性**。429 でなく 400 が返ったらここを疑う。修正候補:

```javascript
// auto-check.js / router.js
'Origin': 'https://bfaith-portal.onrender.com',
'Referer': 'https://bfaith-portal.onrender.com/',
```

### レート制限

- 旧 API: 1 秒 1 リクエスト目安
- 新 API: **1.5 秒以上推奨** (公式ガイドライン)
- 現コードは `API_DELAY = 1100` (1.1 秒) のまま
- 429 が頻発したら 1500 ms に拡張する

### 影響を受けない API

以下は今回の移行と無関係 (別ドメイン / 別認証):
- 楽天 RMS API (`api.rms.rakuten.co.jp`) — `apps/warehouse/rakuten-orders.js`
- 楽天 RMS Service Secret 認証 — `apps/warehouse/rakuten-rms-service.js`
- 楽天サジェスト API (`rdc-api-catalog-gateway-api.rakuten.co.jp/SUI/...`) — `rakuten_title_tool.py` の `fetch_suggest_keywords`

## ロールバック手順

万が一新キーで動かない場合、5/13 までは旧キーが使えるので:

1. `.env` を旧 `RAKUTEN_APP_ID` に戻す
2. コード側の endpoint を `Search/20220601` に戻す (このコミットを revert)

```bash
git revert <この feature branch のマージコミット>
```

ただし 5/14 以降は **必ず新キーで動かす必要がある**。

## チェックリスト

- [x] コード修正 (8 ファイル) commit
- [ ] 中原さん: bfaith-portal Render env 更新
- [ ] 中原さん: 楽天タイトル最適化ツール .env 更新
- [ ] 中原さん: スタンドアロン版順位チェッカー稼働確認 + .env 更新 (該当する場合)
- [ ] 中原さん: bfaith-portal 動作確認 (順位チェッカー手動実行)
- [ ] 中原さん: 楽天タイトル最適化ツール動作確認
- [ ] 5/14 以降の朝 (例: 5/15 9:00) のログで 429/401/400 が出ていないことを確認

## 関連 memory

- `project_rakuten_ranking_checker.md`
- `project_rakuten_title_tool.md`
- `feedback_rakuten_title.md`
