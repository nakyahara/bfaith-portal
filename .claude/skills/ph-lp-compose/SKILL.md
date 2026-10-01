---
name: ph-lp-compose
description: product-hub の「構成をAIに作らせる」の実行役 — claim → 商品画像を見る → ⑦AI画像生成プロンプトを書く → lint → Codex検品 → 反映 → 書き戻す。「LP構成を作って」「ph-lp-compose」で起動。ランナー (scripts/ph-nightly/run-lp-compose.ps1) からも同じ手順で走る
---

# product-hub LP 構成の AI 生成 (段階1・2026-10-01)

スタッフが詳細画面で「🤖 構成をAIに作らせる」を押した商品について、
**LP制作システムの ⑦ AI画像生成プロンプト**を書いて書き戻す。書き戻した構成は画面に出るだけで、
**人がコピーして lp-tool に貼る**ところまでは変わらない (人の確認なしに先へ進むことはない)。

正本 = AI_reference『商品ハブ_LP構成AI生成_段階1設計_20260930.md』。

**段階1 の目的は機能ではなく測定。**「AI の構成は、スタッフが ChatGPT で作っているものと比べて使えるか」を
10 件で判定する。だから**ごまかさない** — 材料が足りなければ無理に書かず、素直に `fail` する。

## 前提 — シェルで使うのは `./phlp` と `./phlpreview` だけ

作業ディレクトリ (`C:\tools\ph-nightly\work`) に固定機能 CLI `./phlp` と検品ラッパー `./phlpreview` がある。
**HTTP・トークン・URL 制限・ファイル名制限はすべて phlp の中**。
🚨 トークンファイル (`~/.claude/secrets/...`) を**読まない・表示しない**。
🚨 `curl` / `node` / `python` / `codex` を直接使わない (無人実行では deny)。

```
./phlp queue                                  仕事があるか
./phlp claim   --run RUN_ID                   1 件 claim (材料 + 仕様書を spec-ID.md に落とす)
./phlp images  ID                             商品画像を img-ID-1.jpg … に落とす
                                              (枚数は claim の packet で決まる。落とせない枚があれば失敗する)
./phlp reserve ID                             🚨 **構成を書き始める前に必ず**
./phlp result  ID --accepted --file out-ID.md --lint lint-ID.json --rounds N
./phlp result  ID --rejected --reason-file reason-ID.txt --lint lint-ID.json --rounds N
./phlp fail    ID --code CODE --message "…"   予約の**前**だけ
./phlp release ID --reason "…"                予約の**前**だけ (一時障害)
./phlp clean   ID                             一時ファイルを消す (rm は使えない)
```

## 不変条件 (終了時に必ず成り立つこと)

- claim した依頼は **done (result --accepted) / failed (result --rejected または fail) / released** の
  どれかで終わる。中途半端に lease を残さない
- 🚨 **`./phlp reserve` の後に失敗したら、`fail` ではなく `result --rejected`** を出す。
  これを守らないと、サーバ側で「AI を呼んだのに結果が来ない」= 成否不明 (`needs_review`) になり、
  人が確認するまで止まる
- 一時ファイルは**どの結末でも**依頼を離れる前に `./phlp clean ID` で消す (`rm` は使えない)
- **1 件ずつ**。1 回の実行で 1 件だけ処理して終わる (段階1 は人がボタンを押す運用)

## 手順

### 1. claim

```bash
RUN_ID="lp-$RANDOM$RANDOM"
./phlp queue                  # claimable が 0 なら何もせず終了
./phlp claim --run "$RUN_ID"
```

応答に出るもの:
- `job_id` … 以降これを ID として使う
- `spec.file` … **仕様書の全文** (`spec-<ID>.md`)。LP制作システムの全タブがテキストになっている
- `packet.name` / `packet.product_info` / `packet.color_variations` … 商品の材料
- `packet.images` … 商品画像の枚数

`job: null` なら仕事なし。終了する。

### 2. 商品画像を落として**見る**

```bash
./phlp images <ID>
```

`img-<ID>-1.jpg` … が作業ディレクトリに落ちる。**Read ツールで全部見る。**
商品の形・ラベルの文字・色・容量表記・バリエーションを自分の目で確かめる
(仕様書の「商品再現ルール」は、見ていないと書けない)。

画像が 0 枚なら `./phlp fail <ID> --code IMAGES_UNAVAILABLE --message "商品画像がありません"`。

**見たことを `seen-<ID>.md` に書く** (これが検品の材料②になる)。
Codex は画像を見られないので、**書いていないことは裏取りの無い記述として扱われる**。
見たままを書く — 形状・色・ラベルの実際の文字・容量表記・点数・バリエーション。
**推測を混ぜない**。読めない文字は「読めない」と書く。

```
img-12-1.jpg: 透明の PET ボトル、白ラベル。ラベルに「ハッカ油スプレー 100mL」と緑字。
ポンプ式キャップ。裏面の成分表記は角度のせいで読めない。
```

### 3. 仕様書を読む

`spec-<ID>.md` を Read で読む。特に見るタブ:

- **出力形式** … 最終出力の形 (必須・禁止・取込互換)
- **AIプロンプトV2.2** … 出力テンプレートそのもの
- **システム本文** … ①商品分析〜⑥制作指示書の考え方

🚨 **仕様書が読めない / 期待するタブが無いときは書かない。**
`./phlp fail <ID> --code SPEC_UNREADABLE --message "…"`。

### 4. 予約してから書く

```bash
./phlp reserve <ID>
```

🚨 **ここを通さずに書き始めない。** サーバは予約していない生成の結果を受け取らない。
そして**ここから先の失敗は `result --rejected`**。

書くもの = 仕様書の「出力形式」に従った ⑦ の全文を `out-<ID>.md` に。

**内部の ①〜⑥ は考えるが出さない。** 最終出力は ⑦ だけ。
材料は「claim で来た商品情報・カラバリ」と「自分の目で見た商品画像」だけ。
**裏取りできない事実を書かない** — 効果・数値・認証・受賞を作らない。
分かっていない項目は仕様書の決まりに従って「未確認」「要確認」とし、
そもそも商品に存在しない項目は**見出しごと省く**。

### 5. lint (機械で見る)

仕様書の「出力形式」に書いてある必須ルールを自分で確かめ、結果を `lint-<ID>.json` に書く:

```json
{"ok": true, "checks": {"header": true, "thumbnail_first": true, "image_count": 8,
 "fixed_headings": true, "tail_blocks": true, "forbidden_words": []}, "notes": []}
```

最低限ここを見る (仕様書が正本。食い違ったら仕様書に従う):

1. 出力内に `AI画像生成プロンプト 出力テンプレート V2.2` が入っている
2. 冒頭が `# LP制作システム V2.1` → `## ⑦ AI画像生成プロンプト` → `### AI画像生成プロンプト 出力テンプレート V2.2`
3. `# 共通生成条件` → `# 共通使用カラー` → `# 商品再現ルール` がこの順にある
4. 最初の画像見出しが **`# 0枚目｜サムネイル`**、次が `# 1枚目｜FV`
5. すべての画像見出しが `# N枚目｜役割名` で、N が 0 から連番
6. 各画像に **15 の固定見出し**がこの表記・この順にある
   (画像の役割／目的／メイン見出し／サブ見出し／本文／バッジ・補足／商品配置／背景・シーン／
    装飾・演出／使用カラー／使用素材／詳細レイアウト／生成指示／NG事項／生成後チェック)
7. 終端が `# 共通NG事項` → `# 共通生成後チェック`
8. 禁止表記が無い: `## 役割` (単独) / `## 使用カラー（HEX）` / `## NG` (単独)
9. 省略表現が無い: 「以下同様」「画像2以降も同じ」「前画像に準ずる」「必要枚数分同様」
10. ①〜⑥・総評・最終まとめ・前置き・「必要なら次に〜」が出ていない

**lint が通らなければ自分で直す。** 直してから次へ。

### 6. Codex で検品 → 反映 (最大 2 巡)

`_lp_review_<ID>.md` に `out-<ID>.md` の全文を書き出してから:

```bash
./phlpreview <ID>
```

渡るのはこの 3 つ (ラッパーが組み立てる。自分で貼る必要は無い):
材料① claim の商品情報 ∕ 材料② `seen-<ID>.md` ∕ 構成案 `_lp_review_<ID>.md`。
🚨 `seen-<ID>.md` が無いと検品は始まらない (見ずに書いた構成案を通さないため)。

Codex は「**画像生成器として実行できるか**」を見る (具体性・事実との食い違い・作り話・トンマナ・重複)。
critical / high の指摘があれば `out-<ID>.md` を直して、`_lp_review_<ID>.md` を作り直してもう 1 回。

🚨 **2 巡で critical / high が消えなければ、そこで打ち切って `rejected`**:

```bash
./phlp result <ID> --rejected --reason-file reason-<ID>.txt --lint lint-<ID>.json --rounds 2
```

`reason-<ID>.txt` には「何が通らなかったか」を具体的に書く (1000 字まで)。
**これは失敗ではなく測定結果。** 正直に書く。

### 7. 書き戻す

```bash
./phlp result <ID> --accepted --file out-<ID>.md --lint lint-<ID>.json --rounds 1
./phlp clean  <ID>
```

🚨 **`--lint` は accepted に必須**で、中身の `ok` が `true` でなければ通らない (CLI もサーバも断る)。

画像の証跡 (どの画像を実際に見たか) は **サーバが配ったときに自分で記録している**。
自分で書く必要はないし、**書いても使われない** — `imgs-<ID>.json` を手で作っても通らない。
`./phlp images` を実際に通していないと `--accepted` は受け取られない。
🚨 **1 枚でも欠けていたら `--accepted` は通らない**。途中の枚で取得に失敗したら、
予約の前なら `fail --code IMAGES_UNAVAILABLE`、後なら `result --rejected` を出す。

## 失敗のときのコード (予約の前だけ)

| code | いつ |
|---|---|
| `SPEC_UNREADABLE` | 仕様書が読めない / 期待するタブが無い |
| `MATERIAL_TOO_THIN` | 商品情報が商品名程度しかなく、書くと憶測になる |
| `IMAGES_UNAVAILABLE` | 商品画像が 0 枚 / 取得できない |
| `OTHER` | その他 |

**一時的な障害** (タイムアウト・429・Render 502 が続く) は `fail` ではなく `./phlp release`。

## やってはいけないこと

- 🚨 トークンファイルを読む・表示する
- 🚨 `curl` / `node` / `python` / `codex` を直接使う
- 🚨 `./phlp reserve` を通さずに構成を書き始める
- 🚨 予約の後に `fail` / `release` を使う (→ `result --rejected`)
- 🚨 材料に無い事実 (効果・数値・認証・受賞) を書く
- 🚨 lint が通らないまま `--accepted` を出す (CLI とサーバの両方で止まる)
- 🚨 `imgs-<ID>.json` を手で作って「画像を見たこと」にする (サーバの記録を見るので通らない)
- 一時ファイルを残したまま依頼を離れる
