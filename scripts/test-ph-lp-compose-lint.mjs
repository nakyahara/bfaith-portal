/**
 * LP 構成の lint とパーサーのテスト (PR1-c・設計 §6)。
 *
 * 見るもの:
 *   ① 移植したパーサーが**配信元と 1 バイトも違わない** (写しの範囲を sha256 で固定)
 *   ② `PARSER_KNOWN_HEADINGS` が**パーサーの実態と合っている** — 手で書いた一覧なので、
 *      パーサーを実際に叩いて確かめる。合わなくなったら検査 19 が嘘をつくことになる
 *   ③ 検査 1〜19 が、通るものを通し、落ちるものを落とす
 *   ④ **fixture の契約テスト** (設計 §6-C) — 配信元が変わったらここが壊れて気づく
 */
import { parseConstructionDoc, PARSER_SOURCE_SHA256, PARSER_SOURCE_URL } from '../apps/product-hub/lib/lp-parser.js';
import {
  lintComposition, sameProduct, lintSummary,
  IMAGE_HEADINGS, PARSER_KNOWN_HEADINGS, HEADER_LINES,
} from '../apps/product-hub/lib/lp-lint.js';
import { fixture, compositionFor, FIXTURES } from './fixtures/lp-compose/index.mjs';
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const HERE = path.dirname(fileURLToPath(import.meta.url));
let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const PRODUCT = 'ハッカ油スプレー 100ml';
const good = () => compositionFor(PRODUCT);
const lintOf = (text, name = PRODUCT) => lintComposition(text, { productName: name });

console.log('① 移植したパーサーは配信元と 1 バイトも違わない');
{
  const src = fs.readFileSync(path.join(HERE, '..', 'apps', 'product-hub', 'lib', 'lp-parser.js'), 'utf8').replace(/\r\n/g, '\n');
  const lines = src.split('\n');
  const from = lines.findIndex((l) => l.startsWith('//>>>'));
  const to = lines.findIndex((l) => l.startsWith('//<<<'));
  ok(from > 0 && to > from, '写しの範囲を示す //>>> と //<<< がある');
  // 改行で区切って結び直すと最後の改行が落ちる。配信元のファイルは改行で終わるので戻す
  const copied = lines.slice(from + 1, to).join('\n') + '\n';
  const hash = crypto.createHash('sha256').update(copied, 'utf8').digest('hex');
  eq(hash, PARSER_SOURCE_SHA256,
    `🚨 写しの sha256 が取得時と同じ (違ったら、こちらで直したか配信元を取り直した。${PARSER_SOURCE_URL} と diff を取る)`);
}

console.log('② PARSER_KNOWN_HEADINGS がパーサーの実態と合っている');
{
  // 15 見出しそれぞれに別々の目印を置いて、**パーサーが名前付きの項目として拾うのはどれか**を実測する。
  // 手で書いた一覧が古くなると検査 19 が嘘をつくので、一覧ではなくパーサーを信じて突き合わせる。
  const body = IMAGE_HEADINGS.map((h, i) => `## ${h}\nSENTINEL${i}`).join('\n\n');
  const doc = [...HEADER_LINES, '', '# 0枚目｜サムネイル', '', body].join('\n');
  const img = parseConstructionDoc(doc).images[0];
  // individualPrompt と rawBlockText は V2.2 ではブロック全文 (= どの見出しも入っている) なので除く。
  // badgeSupplement は「その他項目」のバケツそのものなのでこれも除く
  const WHOLE_BLOCK = ['no', 'name', 'individualPrompt', 'rawBlockText', 'badgeSupplement'];
  const named = Object.entries(img).filter(([k]) => !WHOLE_BLOCK.includes(k));
  const detected = IMAGE_HEADINGS.filter((h, i) =>
    named.some(([, v]) => typeof v === 'string' && v.includes(`SENTINEL${i}`)));
  eq(detected, PARSER_KNOWN_HEADINGS, '🚨 パーサーが構造として拾う見出しの一覧が、lp-lint.js の定数と一致する');
  const bucket = IMAGE_HEADINGS.filter((h) => !PARSER_KNOWN_HEADINGS.includes(h));
  eq(bucket, ['バッジ・補足', '使用素材'],
    '🚨 いま落ちているのは「バッジ・補足」と「使用素材」(設計 §6 の注記どおり。増えたら仕様書かパーサーが動いた)');
}

console.log('③ 正しい構成は通る');
{
  const r = lintOf(good());
  ok(r.ok, 'fixture (3枚) は lint を通る');
  for (let i = 1; i <= 18; i++) ok(r.checks[i] === true || r.checks[i] === null, `  検査 ${i} が通る`);
  ok(r.warnings.some((w) => w.id === 19), '🚨 検査 19 は警告として出る (落とさない)');
  ok(r.errors.length === 0, 'エラーは無い');
  const spec = lintComposition(fixture(FIXTURES.specExample), { productName: 'FingerBoard Care Oil 指板メンテナンスオイル 60ml' });
  ok(spec.ok, '🚨 仕様書「出力例」から組んだ構成も通る (lint が仕様書より厳しくない)');
}

console.log('④ 検査 1〜12 — テキストの検査');
{
  const t = (f) => f(good());
  eq(lintOf(t((s) => s.replace('### AI画像生成プロンプト 出力テンプレート V2.2', '### AI画像生成プロンプト'))).checks[1],
    false, '1: テンプレート版の記載が無ければ落ちる');
  eq(lintOf(t((s) => `よろしくお願いします。\n\n${s}`)).checks[2], false, '2: 前置きが付いたら落ちる');
  eq(lintOf(t((s) => s.replace('# 共通使用カラー', '# 使用カラー共通'))).checks[3], false, '3: 共通ブロックの名前が違えば落ちる');
  eq(lintOf(t((s) => s.replace('# 0枚目｜サムネイル', '# 0枚目｜TOP'))).checks[4], false, '4: 0枚目がサムネイルでなければ落ちる');
  eq(lintOf(t((s) => s.replace('# 1枚目｜FV', '# 1枚目｜ファーストビュー'))).checks[5], false, '5: 1枚目が FV でなければ落ちる');
  eq(lintOf(t((s) => s.replace('# 2枚目｜使用シーン', '# 3枚目｜使用シーン'))).checks[6], false, '6: 番号が飛べば落ちる');
  eq(lintOf(t((s) => s.replace('## 使用素材\n提供された実物商品画像\n\n## 詳細レイアウト', '## 詳細レイアウト'))).checks[7],
    false, '7: 固定見出しが 1 つ欠ければ落ちる');
  eq(lintOf(t((s) => s.replace('## 目的\n検索結果', '## ねらい\n検索結果'))).checks[7], false, '7: 表記が違っても落ちる');
  eq(lintOf(t((s) => s.replace(/# 共通生成後チェック[\s\S]*$/, ''))).checks[8], false, '8: 終端ブロックが欠ければ落ちる');
  eq(lintOf(t((s) => s.replace('## 画像の役割', '## 役割'))).checks[9], false, '9: 旧表記「## 役割」で落ちる');
  eq(lintOf(t((s) => s.replace('## 使用カラー\n#FFFFFF', '## 使用カラー（HEX）\n#FFFFFF'))).checks[9], false, '9: 「使用カラー（HEX）」で落ちる');
  eq(lintOf(t((s) => s.replace('最小限。', '以下同様。'))).checks[10], false, '10: 「以下同様」で落ちる');
  eq(lintOf(t((s) => `${s}\n\n## 総評\nよくできています。`)).checks[11], false, '11: 総評が付いたら落ちる');
  eq(lintOf(t((s) => `${s}\n\n① 商品分析\n…`)).checks[11], false, '11: ①〜⑥ の見出しで落ちる');
  ok(lintOf(good()).checks[11] === true, '11: 本文に ⑦ があるだけでは落ちない (必須ヘッダーのため)');
  // 12: 0枚目だけ = 1 枚 → 下限割れ
  const oneOnly = good().replace(/# 1枚目｜FV[\s\S]*?(?=# 共通NG事項)/, '');
  eq(lintOf(oneOnly).checks[12], false, '12: 画像が 1 枚なら落ちる (2〜10 枚)');
}

console.log('⑤ 検査 13〜19 — パース結果の検査');
{
  eq(lintOf(fixture(FIXTURES.legacyV21)).checks[16], false, '16: V2.1 は落ちる (未知/旧版は fail-closed)');
  const r21 = lintOf(fixture(FIXTURES.legacyV21));
  ok(!r21.ok, '🚨 V2.1 の構成は受け取らない');
  // 14: 必須項目が空
  const emptyNg = good().replace('## NG事項\nテキスト20％超過、枠線、商品改変、商品より目立つ文字・装飾。', '## NG事項\n');
  eq(lintOf(emptyNg).checks[14], false, '14: NG事項が空なら落ちる');
  // 17: 別商品
  eq(lintOf(good(), 'ギター用 指板オイル').checks[17], false, '🚨 17: 別商品の名前なら落ちる (内容の取り違え)');
  eq(lintOf(good(), 'ハッカ油スプレー').checks[17], true, '17: 容量違いの表記ゆれは通す');
  eq(lintOf(good(), null).checks[17], null, '17: 比べる相手が無ければ検査しない');
  // 18: 終端ブロックの中身が空
  const emptyTail = good().replace(/# 共通NG事項\n[\s\S]*?(?=# 共通生成後チェック)/, '# 共通NG事項\n\n');
  eq(lintOf(emptyTail).checks[18], false, '18: 共通NG事項が空なら落ちる');
  // 19 は警告であって、ok を落とさない
  const r = lintOf(good());
  eq(r.checks[19], false, '19: 既知のズレがあるので false');
  ok(r.ok, '🚨 19 が false でも ok は true (警告であって失格ではない)');
}

console.log('⑥ sameProduct — 表記ゆれは通し、別商品は弾く');
{
  ok(sameProduct('ハッカ油スプレー 100ml', 'ハッカ油スプレー 100ml'), '完全一致');
  ok(sameProduct('ハッカ油スプレー', 'ハッカ油スプレー 100ml'), '容量の有無');
  ok(sameProduct('ハッカ油スプレー１００ｍｌ', 'ハッカ油スプレー100ml'), '全角・半角');
  ok(sameProduct('ハッカ油スプレー （100ml）', 'ハッカ油スプレー 100ml'), '括弧・空白');
  ok(!sameProduct('ハッカ油スプレー', '指板メンテナンスオイル'), '🚨 別商品は弾く');
  ok(sameProduct('ハッカ油スプレー', 'ハッカ油スプレー 3 枚'), '入数の有無');
  // 🚨 以前は「片方がもう片方を含む」で通していたので、この 3 つが通っていた (codex exec review P1)
  ok(!sameProduct('オイル', '指板メンテナンスオイル'), '🚨 一般的な語の部分一致では通さない');
  ok(!sameProduct('ハッカ油', 'ハッカ油クリーム'), '🚨 前方一致でも別商品は弾く');
  ok(!sameProduct('ハッカ油スプレー', 'ハッカ油スプレー 詰替'), '🚨 容量以外の語が付けば別商品');
  // 単位の無い数字は落とさない (型番を潰すと別商品が通ってしまう)
  ok(!sameProduct('WD-40', 'WD-50'), '🚨 型番の数字は容量扱いしない');
  ok(!sameProduct('', 'ハッカ油'), '空は一致としない');
  ok(!sameProduct('ハッカ油', ''), '空は一致としない (逆)');
}

console.log('⑥b 見出しの階層 (codex exec review P2)');
{
  // 🚨 `## 0枚目｜…` は仕様書の形ではない。以前は #{1,6} で見ていたので通っていた
  const h2 = good().replace(/^# (\d+枚目)/gm, '## $1');
  const r = lintOf(h2);
  ok(!r.ok, '🚨 画像見出しが ## なら落ちる');
  eq(r.checks[4], false, '  4 が落ちる');
  const h2b = good().replace('# 共通NG事項', '## 共通NG事項');
  eq(lintOf(h2b).checks[8], false, '🚨 終端ブロックが ## なら落ちる');
  const h2c = good().replace('# 共通生成条件', '## 共通生成条件');
  eq(lintOf(h2c).checks[3], false, '🚨 共通ブロックが ## なら落ちる');

  // 🚨 「詳細レイアウト」の中の ### は仕様書のテンプレートそのもの。
  //    これを ## と数えてしまうと、正しい出力が検査 7 で落ちる
  const withSub = good().replace(
    '## 詳細レイアウト\n1200×1200px。商品領域を最大化し、テキスト要素20％以下、枠線なし。外周に安全余白を確保。',
    '## 詳細レイアウト\n\n### キャンバス構成\n- 1200×1200px\n- 商品領域を最大化\n\n### 配置\n- テキスト要素20％以下\n- 枠線は使用しない');
  ok(withSub !== good(), '  (差し替えが実際に起きている)');
  const rs = lintOf(withSub);
  eq(rs.checks[7], true, '🚨 詳細レイアウトの中の ### は固定見出しに数えない');
  ok(rs.ok, '  ### 入りでも lint を通る');
}

console.log('⑦ fixture の契約テスト — パース結果を固定する (設計 §6-C)');
{
  // 🚨 ここが壊れたら「直す」のではなく、**配信元のパーサーが変わったのか仕様書が変わったのか**を
  //    先に確かめる。fixture を書き換えて通すのは最後の手段。
  const p = parseConstructionDoc(fixture(FIXTURES.hakka3));
  eq(p.templateVersion, 'V2.2', 'hakka3: V2.2 と判定する');
  eq(p.images.length, 3, 'hakka3: 3 枚');
  eq(p.images.map((i) => i.no), [0, 1, 2], 'hakka3: 番号は 0,1,2');
  eq(p.images.map((i) => i.name), ['サムネイル', 'FV', '使用シーン'], 'hakka3: 役割名');
  eq(p.common.productName, 'ハッカ油スプレー 100ml', 'hakka3: 商品名を読む');
  eq(p.common.imageSize, '1200×1200px／正方形', 'hakka3: 画像サイズを読む');
  eq(p.common.baseColor, '#FAF9F6', 'hakka3: ベース背景を読む');
  eq(p.images[1].mainCopy, '夏のベタつく空気に、ひと吹き。', 'hakka3: 1枚目のメイン見出し');
  eq(p.images[1].imageRole, 'FV／商品理解', 'hakka3: 1枚目の画像の役割');
  ok(p.images[2].generationInstruction.includes('商品を持って使っている状態'), 'hakka3: 2枚目の生成指示');
  ok(p.images[0].individualPrompt.includes('## 生成後チェック'), '🚨 individualPrompt は画像ブロック全文 (= 完成プロンプト)');
  eq(p.common.reproductionRules ? p.common.reproductionRules.split('\n')[0] : null, '## 使用商品', 'hakka3: 商品再現ルールを読む');

  const e = parseConstructionDoc(fixture(FIXTURES.specExample));
  eq(e.templateVersion, 'V2.2', 'spec例: V2.2');
  eq(e.images.length, 2, 'spec例: 2 枚');
  eq(e.images.map((i) => i.no), [0, 1], 'spec例: 番号は 0,1');
  eq(e.common.productName, 'FingerBoard Care Oil 指板メンテナンスオイル 60ml', 'spec例: 商品名');
  ok(e.images[0].postCheck.includes('全バリエーション掲載'), 'spec例: 0枚目の生成後チェック');

  const l = parseConstructionDoc(fixture(FIXTURES.legacyV21));
  eq(l.templateVersion, 'V2.1', '旧版: V2.1 と判定する');
  eq(l.images.length, 2, '旧版: 2 枚');
  eq(l.images[0].imageRole, null, '🚨 旧版では V2.2 の項目が取れない (だから lint が落とす)');
}

console.log('⑧ lintSummary — 保存する形');
{
  const s = lintSummary(lintOf(fixture(FIXTURES.legacyV21)));
  ok(s.ok === false, 'ok が入る');
  ok(Array.isArray(s.errors) && s.errors.length > 0, 'errors が入る');
  ok(s.errors.every((e) => typeof e.id === 'number' && e.detail.length <= 300), '1 件ずつ id と 300 字までの説明');
  ok(s.warnings.length <= 30, '警告は 30 件まで (DB を肥らせない)');
  ok(JSON.stringify(s).length < 100_000, 'LINT_MAX に収まる');
}

console.log('⑨ 壊れた入力で例外を漏らさない');
{
  for (const bad of ['', '   ', null, undefined, '# 見出しだけ', 'a'.repeat(50_000)]) {
    let r;
    try { r = lintOf(bad); } catch (e) { r = { threw: String(e.message) }; }
    ok(r && r.ok === false, `${JSON.stringify(String(bad).slice(0, 12))} は例外ではなく ok:false`);
  }
}

console.log(fail ? `\n❌ ${pass} 件成功 / ${fail} 件失敗` : `\n✅ ${pass} 件成功 / 0 件失敗`);
process.exit(fail ? 1 : 0);
