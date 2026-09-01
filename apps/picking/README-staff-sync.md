# スタッフマスタ同期 (miniPC picking/packing ← Render apps/staff)

「人」の正本は Render の **スタッフマスタ** (`apps/staff` / staff.db)。miniPC は別プロセス・別 DB なので、
読み取り専用の export を取得して `pk_workers` に反映する。**packing も同じ `pk_workers` を読む**ので同時に揃う。

```
Render /apps/staff/export  ──(Bearer STAFF_EXPORT_TOKEN)──▶  miniPC apps/picking/staff-sync.js
                                                                      │
   picking-drive-poller の tick に相乗り (1時間に1回)  ────────────────┘
                                                                      ▼
                                                          picking.db pk_workers
                                                          (ピッキング/梱包の名前タップ)
```

## 決めごと

| 論点 | どうするか | なぜ |
|---|---|---|
| `pk_workers.code` (w01…) | **変えない**。`staff_id` / `staff_no` / `source` を足して紐づける | 作業実績が参照する不変キー |
| 初回の紐付け | **名前一致** (staff の短い表記 → 正式表記の順)。以後は `staff_id` で照合 | どちらの表で改名しても追従できる |
| **同名が絡むとき** | export 側に同じ表示名が2人 → **全体を拒否** / pk_workers 側に同名の未紐付けが2行 → **その人だけ触らず警告** | 別人に紐づく事故を防ぐ (同姓の社員がいる) |
| 既存の `staff_no` と受信値が違う | **全体を拒否** (staff を作り直して id が再採番された兆候) | そのまま進めると別人に改名・無効化が起きる |
| export の中身が壊れている (id が非整数・重複、staff_no 空/重複、名前空) | **全体を拒否** (件数と理由を記録) | 部分欠損を「退職」と誤解しない |
| 古い応答が後着 (`generated_at` が前回成功より古い) | 適用しない | 手動と自動が重なったときに巻き戻らない |
| 取り込む対象 | **倉庫作業 (`warehouse`) の役割を持つ有効なスタッフだけ** | 事務担当を現場の名前タップに並べない。倉庫→事務に変わった人は無効化され、戻せば復活する。⚠ `roles` を持たない古い export は全員 倉庫とみなす (役割が届かないだけで名前タップが空になる事故を作らない) |
| staff にいて pk_workers にいない | 追加 (既存の採番 w+連番、code 衝突は回避)。ただし**無効なスタッフ・事務だけの人は生やさない** | 退職者・事務担当を名前タップに出さない |
| staff で無効化 | pk_workers も無効化 (名前タップから消える) | 正本はスタッフマスタ |
| **pk_workers にしかいない人** | **触らない**。`unmatched` として管理画面に「スタッフマスタに登録してください」と出す | 勝手に無効化すると現場が自分を選べなくなる |
| 過去の実績 | 遡って書き換えない (`pk_batches.worker` 等は打刻時点の表示名) | 履歴を書き換えない |
| 画面から手で無効化した人 | 次の同期でスタッフマスタが有効なら**復活する** | 正本はスタッフマスタ。外したいならスタッフマスタ側で無効化する |
| 取得失敗 / 0件 / 有効数が**前回成功時の有効スタッフ数**の半分未満 | **適用せず前回の `pk_workers` を保つ** (fail-closed) | Render の一時的な不調で現場の名前が全部消える事故を作らない。基準は「前回成功時の値」で、一時要員 (local) の数に左右されない |
| 紐付け済みなのに export から消えた | 無効化せず **`❗…がスタッフマスタから消えています`** と強く警告 | 削除異常を「ただの未登録」と混ぜない |

## env (miniPC `C:\tools\bfaith-picking\.env`)

| env | 必須 | 内容 |
|---|---|---|
| `STAFF_EXPORT_TOKEN` | ✅ | Render 側と同じ値。**未設定なら同期しない** (現状維持・管理画面に警告) |
| `STAFF_SYNC_URL` | – | 既定 `https://bfaith-portal.onrender.com` |
| `PICKING_STAFF_SYNC_INTERVAL_MIN` | – | 既定 60 (分) |

Render 側にも同じ `STAFF_EXPORT_TOKEN` が要る (未設定だと `/apps/staff/export` は 404 = 経路ごと無効)。

## 運用

- 状態: `/apps/picking/admin/devices` の「作業者マスタ」節に**最終同期**(件数・追加/紐付け/改名/無効化・失敗理由・警告)が出る
- 手動同期: 同画面の「👥 スタッフマスタから今すぐ同期」(管理者)。自動と重なってもプロセス内 mutex で1回にまとまる
- 実行タイミング: `picking-drive-poller` の tick から**待たずに**起動する (最大20秒の HTTP が2分周期と heartbeat を遅らせない)。
  Drive 取得の成否とは独立 (Drive 障害中も人の増減は反映される)。失敗時は5分後に再試行、成功時は1時間後
- 監視: 親の `picking-drive-poller` (heartbeat・台帳登録済み) に含める。独立したスケジュールタスクは作らない
- ログ: `PickingServer.out.log` の `[picking-staff-sync]`

## テスト

```
node scripts/test-picking-staff-sync.mjs   # 56 項目 (紐付け・追加・改名・無効化・入力検証・同名/identity conflict・世代の巻き戻り・mutex・fail-closed)
node scripts/test-staff-sync-e2e.mjs       # 11 項目 (server.js を起動して実 HTTP で Render→miniPC を通す)
```
