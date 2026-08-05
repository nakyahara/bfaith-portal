# jobs-monitor — 定期実行の見張り役

「**時間までに成功の報告 (ping) が来なければ鳴らす**」dead-man 方式の監視。
2026-07〜08 に実際に起きた「Yahoo統計6日間停止」「スカウト2度の無音停止」
「OAuth期限切れ (警告は出ていたが毎朝の通知に埋もれた)」への恒久対策。

## ⭐絶対ルール (今後作るものすべて)

> **定期実行・期限つき人手作業・一時物 (あとで消すもの) を新設・変更するときは、
> 実装前に `config/jobs-registry.mjs` へ1エントリ足す。**
> 未登録のスケジュールを直接作らない。temporary_asset には remove_by (撤去期限) が必須。

- 登録するとデプロイだけで監視・毎朝サマリ・期限管理が自動で付いてくる
- 台帳に無い id から ping が来ると毎朝のサマリが「登録するか止めるか」を要求する
  (**不明なまま残る、という状態を許さない**)
- 新しい定期実行を独立タスクにする前に、**既存ランナー (daily-sync / fetch-all) に
  1ステップとして載せられないか先に検討する** (入口の数自体を増やさない)。
  独立にするのは時間帯・障害影響・実行環境が合わない場合だけ

## 仕組み (3層)

```
ジョブ (miniPC/どこでも) ──成功時に ping──▶ Render jobs-monitor (判定)
                                              │ 締切超過 → GChat 要対応スペース
                                              │ 毎朝08:50 「今日の要対応」1本
GAS (Google側=両方から独立) ──/health を10分ごと監視──┘ 死んでいたら通知
```

- **判定は Render** — miniPC が丸ごと死んでも検知できる
- **見張りの見張りは GAS** — Render が死んでも検知できる (追加サービス契約なし)
- 通知は **GCHAT_WEBHOOK_JOBS (要対応専用スペース) のみ**。日次レポートの流れに混ぜない
  (混ぜたことが OAuth 失効見逃しの直接原因)

## 監視のセマンティクス

- 見るのは「**ok ping が締切内に来たか**」だけ。締切は**予定時刻基準 (アンカー方式)**:
  `直近の anchor_hour_jst:anchor_minute_jst + grace_hours` までに成功が来なければ late。
  「最終成功 + 期間」基準だと遅れて成功するたびに締切がドリフトするので採らない
- **ping が一度も来ないままでも、監視開始 (firstSeen) を起点に締切超過へ昇格する**
  (配線忘れ・ジョブ自体が死んでいるケースを永遠の「未初期化」で無音にしない)
- `status=fail` は記録のみ (失敗の中身はジョブ自身の GChat 通知が担当。二重に鳴らさない)
- ⭐**`status=partial` = 「動いたが完走していない」** — 1回では終わらない長時間バッチ用。
  台帳に `partial_max_days` があるジョブだけが使える。partial は**その日の締切は満たすが
  完走にはならない**。未完走は `partial_max_days` 回までは許容し、**それを超えると**
  `stalled` (🟠) で要対応に出る (`max` = 許容する最大回数なので、7 なら8回目で鳴る)。
  これが無いと、実行時間の上限で毎回中断されるバッチは
  「毎朝鳴りっぱなし」か「永遠に完走しなくても緑」の二択にしかならない
  (2026-08-02 に product-idea-scout で実際に前者が起きた)
- P1/P2 の締切超過 → 即時通知 + 再通知 (P1=6h / P2=24h) + 復旧通知。
  **通知は送信に成功したときだけ「通知済み」になる** (送信失敗は次の5分で再試行)
- P3 / TMP / 期限接近 / 未初期化 → 毎朝 08:50 のサマリのみ
- サマリは**毎日必ず1本**届く (「✅すべて正常」の日も)。時刻ちょうどの cron でなく
  「時刻を過ぎた最初の評価」で送るので、その時刻に Render が再起動していても欠落しない
- `/health` は評価ループだけでなく**通知経路の健全性**も見る (webhook未設定・直近送信失敗・
  サマリが26時間出ていない・台帳エラー → 503)。「判定できるが誰にも伝わらない」を GAS が検知できる

## API

| エンドポイント | 認証 | 用途 |
|---|---|---|
| `POST /apps/jobs-monitor/ping/:id?status=ok\|fail\|start\|partial[&note=...]` | Bearer `JOBS_MONITOR_TOKEN` | ジョブからの報告 (`note` は200文字まで・サマリに表示) |
| `GET /apps/jobs-monitor/health` | なし (ok と評価時刻のみ返す) | GAS 見張り用 |
| `GET /apps/jobs-monitor/status` | Bearer | 全評価JSON (デバッグ・将来の一覧画面) |

ping はどの環境からでも1リクエスト:

```powershell
# miniPC ランナー (bat/ps1) の末尾に1行:
powershell -NoProfile -ExecutionPolicy Bypass -File C:\Users\bfaith\bfaith-portal\scripts\jobs-monitor\ping.ps1 -Id <job-id> -Status ok

# 長時間バッチが時間切れで中断したとき (完走ではないが動いてはいる):
powershell -NoProfile -ExecutionPolicy Bypass -File ...\ping.ps1 -Id <job-id> -Status partial -Note "remaining 484"
```

```bash
curl -m 15 -s -X POST -H "Authorization: Bearer $TOKEN" \
  "https://bfaith-portal.onrender.com/apps/jobs-monitor/ping/<job-id>?status=ok"
```

ping ヘルパーは**絶対に本体を失敗させない** (常に exit 0 / 未設定なら無言でスキップ)。

## 環境変数 (Render)

| 変数 | 値 |
|---|---|
| `JOBS_MONITOR_ENABLED` | `1` (Render のみ。miniPC には設定しない — 同じ server.js を共有しているため) |
| `JOBS_MONITOR_TOKEN` | ping/status 用 Bearer (miniPC .env と同じ値を設定) |
| `GCHAT_WEBHOOK_JOBS` | 要対応スペースの webhook。**既定の GCHAT_WEBHOOK にフォールバックしない** |
| `JOBS_DIGEST_CRON` | サマリ時刻 (既定 `50 23 * * *` UTC = JST 08:50) |

## デプロイ手順 (初回)

1. Render に上記 env を設定 → 再起動 → ログに `[server] jobs-monitor mounted`
2. miniPC `bfaith-portal\.env` に `JOBS_MONITOR_TOKEN=` (同じ値) を追記
3. miniPC の各ランナー末尾に ping 1行を追加 (対象は台帳の scheduled_job 全11本)
4. human_obligation の初期化: 直近の実施日を ping で刻む
   (yahoo-oauth-reauth = 2026-08-01 再認可済 / aupay-api-key-rotation = 直近ローテ日)
5. GAS 見張りを設置 (`scripts/gas/jobs-monitor-watchdog.gs` の冒頭コメント参照)
6. 翌朝 08:50 のサマリが届くことを確認

## テスト

```
node apps/jobs-monitor/test-evaluate.js           # 判定ロジック + 台帳バリデータ
node apps/jobs-monitor/test-ping-local.js         # Render内ジョブの ping ヘルパー
node apps/jobs-monitor/test-schedule-inventory.js # ⭐未登録スケジュールの検出
```

`test-schedule-inventory.js` は **コード上の `cron.schedule` / `setInterval` を全部数え、
台帳 id か除外理由の宣言を強制する**。新しく定期実行を書くとこのテストが落ちるので、

- 業務ジョブ → 台帳に登録して ping を配線し `job:` を書く
- そうでない (プロセス内観測・ロック延命・CLI 等) → `exempt:` に理由を書く

のどちらかを通らないとマージできない。**「新設したら台帳に足す」を人の規律から機械の検査へ
移すためのもの。** 2026-08-01 の棚卸しが miniPC だけを見ていて Render 内 cron を
カテゴリごと取りこぼした (6本が4ヶ月無監視) 再発防止 = PR #710 / #712。

## 今後 (PR-2 以降)

- **data_freshness**: 「ジョブが動いたか」でなく「warehouse.db のデータが新しいか」を監視
  (Yahoo の "毎日起動していたが認証切れで空振り" はこれで捕まえる)。
  ⭐**今の dead-man では、ジョブが正常終了しながら中身が空だった場合を捕まえられない。
  これが設計上の最大の残穴。**
- **実構成の自動収集**: miniPC の Task Scheduler 一覧を毎日送り、台帳との差分
  (未登録タスク / 消えたタスク) をサマリに出す。
  Render 側はコードが正なので `test-schedule-inventory.js` が同じ役目を果たすが、
  miniPC 側は「誰かが手で作ったタスク」を拾えないため実構成の収集が要る
- ポータルに一覧画面 (`/status` を表示するだけ)
