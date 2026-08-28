# -*- coding: utf-8 -*-
"""
商品コピー機械リント (copy_lint.py)
タイトル・キャッチコピー・説明文の 文字数 と 薬機法/景表法/モール規約NGワード を決定的にチェックする。
/rakuten-title スキル(Claude Code)の Step「機械リント」から呼ばれる。

使い方:
  python -X utf8 copy_lint.py <copy.json>

入力JSON (キーは任意。あるものだけチェック):
  {
    "code":          "商品コード (表示用)",
    "rakuten_title": "楽天タイトル",
    "yahoo_title":   "Yahoo!タイトル",
    "headline":      "キャッチコピー (Yahoo!ヘッドライン兼用)",
    "caption":       "商品説明文",
    "notes":         "注意書き"
  }

終了コード: 0=合格(WARNのみ含む) / 1=NGあり / 2=入力エラー
NG  = 出品前に必ず修正 (薬機法・景表法・文字数超過)
WARN= 人が判断 (公式ソースに記載があれば可、など)

※NGワードは「文字列が含まれるか」の機械判定。文脈は見ない。
  誤検知は人の判断で通してよいが、その判断はレビュー記録に残すこと。
"""
import json
import re
import sys
import unicodedata

# ─── 文字数ルール ───────────────────────────────────────
# 楽天タイトル: 全角127文字(255バイト)上限 / 社内ルール80文字以上
# Yahoo!タイトル: 65文字上限 (RYS YAHOO_TITLE_MAX_LEN と同値) / 社内ルール60文字以上
# キャッチコピー: Yahoo!ヘッドラインに使うため30文字上限(Yahoo仕様)
# 説明文: 500文字超は要確認 (モール別上限は出品時に再検証)
LIMITS = {
    "rakuten_title": {"max": 127, "min_soft": 80, "max_bytes": 255},
    "yahoo_title":   {"max": 65,  "min_soft": 60},
    "headline":      {"max": 30},
    "caption":       {"max_soft": 500},
    "notes":         {"max_soft": 500},
}

# ─── NGワード (薬機法・景表法・モール規約) ──────────────────
# NG: 含まれていたら出品不可として失敗させる
NG_PATTERNS = [
    # 病名・症状の治癒効果 (薬機法)
    (r"治る|治す|治療|完治|治癒", "薬機法: 治癒効果の標榜は医薬品のみ"),
    (r"アトピー|糖尿病|高血圧|花粉症|インフルエンザ|認知症|うつ病|がんに|癌に", "薬機法: 病名への効果を連想させる"),
    (r"殺菌効果|消炎|抗炎症", "薬機法: 医薬品・医薬部外品の効能 (該当商品でなければ不可)"),
    (r"痩せる|脂肪燃焼|デトックス", "薬機法: 痩身効果の標榜"),
    (r"若返り|アンチエイジング", "薬機法: 老化防止の標榜 (化粧品の効能範囲外)"),
    (r"脱毛|発毛|育毛", "薬機法: 医薬部外品・医療の効能 (該当商品でなければ不可)"),
    (r"免疫力(アップ|向上|を高める)", "薬機法: 免疫への効果の標榜"),
    (r"血行促進|血流改善", "薬機法: 身体機能への効果 (該当区分商品でなければ不可)"),
    (r"副作用(なし|ゼロ|がない)", "薬機法: 安全性の保証"),
    # 景表法 (優良誤認・有利誤認)
    (r"No\.?1|ナンバーワン|日本一|世界一|業界一", "景表法: 根拠資料なしのNo.1表示は優良誤認"),
    (r"最安|最高級|最強|日本初|世界初|業界初", "景表法: 最上級・初回性の表示は根拠必須"),
    (r"永久|絶対|完全に|100[%％](効|安全)", "景表法: 断定的保証"),
]

# WARN: 人が判断 (公式ソースに記載があれば可 / 表現を弱めるか検討)
WARN_PATTERNS = [
    (r"美白", "楽天で厳格運用。医薬部外品でメーカー公式が謳う場合のみ可"),
    (r"効果|効く", "効能の断定に見えないか文脈確認 (「〜に効く」はNG寄り)"),
    (r"改善|予防", "身体への効能文脈なら薬機法NG。物品の話(収納の改善等)なら可"),
    (r"安心|安全", "安全性の保証に見えないか確認。「保護者の見守りのもと」等へ言い換え検討"),
    (r"無添加|無着色|無香料|パラベンフリー", "何が無添加かの明示必須(楽天規約)。公式表記がある場合のみ"),
    (r"オーガニック", "公式ソースに記載がある場合のみ (認証なしは優良誤認)"),
    (r"知育|学習効果", "公式ソースに記載がある場合のみ (教育効果の裏取り)"),
    (r"送料無料", "実際の送料設定と一致しているか確認"),
    (r"ポイント\d*倍?", "ポイント訴求はタイトル不可 (モール規約)"),
    (r"抗菌|除菌", "試験根拠・公式表記の確認 (雑品の範囲か)"),
    (r"医薬部外品|薬用", "実際に医薬部外品の承認があるか確認"),
    (r"ランキング\d*位|受賞", "実績の根拠 (いつ・どこで) を説明文に明示"),
]


def width_len(s):
    """全角=2, 半角=1 のバイト数換算 (楽天の255バイト制限用)"""
    return sum(2 if unicodedata.east_asian_width(c) in ("F", "W", "A") else 1 for c in s)


def check_field(name, text, findings):
    lim = LIMITS.get(name, {})
    n = len(text)

    if "max" in lim and n > lim["max"]:
        findings.append(("NG", name, f"文字数超過: {n}文字 > 上限{lim['max']}文字"))
    if "max_bytes" in lim and width_len(text) > lim["max_bytes"]:
        findings.append(("NG", name, f"バイト数超過: {width_len(text)} > 上限{lim['max_bytes']} (全角2/半角1換算)"))
    if "min_soft" in lim and n < lim["min_soft"]:
        findings.append(("WARN", name, f"文字数不足: {n}文字 < 目標{lim['min_soft']}文字 (検索KWを追加推奨)"))
    if "max_soft" in lim and n > lim["max_soft"]:
        findings.append(("WARN", name, f"長文: {n}文字 > 目安{lim['max_soft']}文字 (モール上限を出品時に確認)"))

    for pat, why in NG_PATTERNS:
        for m in re.finditer(pat, text):
            findings.append(("NG", name, f"NGワード「{m.group(0)}」 — {why}"))
    for pat, why in WARN_PATTERNS:
        for m in re.finditer(pat, text):
            findings.append(("WARN", name, f"要確認「{m.group(0)}」 — {why}"))


def main():
    if len(sys.argv) < 2:
        print("usage: python -X utf8 copy_lint.py <copy.json>")
        return 2
    try:
        with open(sys.argv[1], encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        print(f"入力エラー: {e}")
        return 2

    findings = []
    checked = []
    for field in ("rakuten_title", "yahoo_title", "headline", "caption", "notes"):
        text = data.get(field)
        if text:
            checked.append(f"{field}({len(text)}文字)")
            check_field(field, str(text), findings)

    code = data.get("code", "?")
    print(f"=== copy_lint: {code} ===")
    print("checked:", ", ".join(checked) if checked else "(なし)")
    ng = [f for f in findings if f[0] == "NG"]
    warn = [f for f in findings if f[0] == "WARN"]
    for level, field, msg in ng + warn:
        print(f"  [{level}] {field}: {msg}")
    if not findings:
        print("  問題なし")
    print(f"result: {'FAIL' if ng else 'PASS'} (NG={len(ng)}, WARN={len(warn)})")
    return 1 if ng else 0


if __name__ == "__main__":
    sys.exit(main())
