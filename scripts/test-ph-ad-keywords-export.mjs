import { temporaryTestRoot } from './test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * SP広告KW — 候補の正規化とコピー本文 (apps/product-hub/lib/ad-keywords-export.js)
 * 実行: node scripts/test-ph-ad-keywords-export.mjs
 */
const m = await import('../apps/product-hub/lib/ad-keywords-export.js');

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };
const eq = (a, b, l) => ok(JSON.stringify(a) === JSON.stringify(b), `${l} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

console.log('[1] normalizeKeyword');
{
  eq(m.normalizeKeyword('  ハッカ油 　 スプレー  '), 'ハッカ油 スプレー', '空白 (全角含む) を 1 つに寄せる');
  eq(m.normalizeKeyword('a\x00b'), 'a b', '制御文字は空白に');
  eq(m.normalizeKeyword(''), null, '空は null');
  eq(m.normalizeKeyword('x'.repeat(81)), null, '81 文字は null (Amazon の上限 80)');
  eq(m.normalizeKeyword('ハッカ油', { seed: 'ハッカ油' }), null, '種そのものは null');
  eq(m.normalizeKeyword('ハッカ油', { seed: 'はっか油' }), 'ハッカ油', '別の語は残す');
}

console.log('[2] dedupeKeywords');
{
  const r = m.dedupeKeywords([{ keyword: 'Hakka', source: 'base' }, { keyword: 'hakka', source: 'hiragana:は' }, { keyword: 'ひば', source: 'base' }]);
  eq(r.map((x) => x.keyword), ['Hakka', 'ひば'], '大小文字違いは 1 つ (先に出た出典を残す)');
  eq(r[0].source, 'base', '出典は先勝ち');
}

console.log('[3] buildKeywordCopy: 種類ごと・マッチタイプごとに分ける');
{
  const c = m.buildKeywordCopy([
    { keyword: 'ハッカ油 スプレー', match_type: 'exact' },
    { keyword: 'ハッカ油 虫除け', match_type: 'phrase' },
    { keyword: 'ハッカ油 スプレー', match_type: 'exact' },   // 重複
    { keyword: 'ハッカ油 効果', match_type: 'exact' },
    { keyword: 'ハッカ油 ?', match_type: 'unknown' },         // 型の無い採用
    { keyword: '', match_type: 'broad' },
  ]);
  eq(c.blocks.map((b) => [b.match_type, b.count]), [['exact', 2], ['phrase', 1]], 'exact 2・phrase 1・broad は空なので出さない');
  eq(c.blocks[0].text, 'ハッカ油 スプレー\nハッカ油 効果', '1 行 1 語・重複なし');
  eq(c.total, 3, '合計');
  ok(!JSON.stringify(c).includes('unknown'), '型の無い採用は黙って exact に寄せない (落とす)');
  eq(m.buildKeywordCopy([]).blocks, [], '採用が無ければ block も無い');
}

console.log('[4] exportSnapshot は同じ採否から同じ中身');
{
  const a = [{ keyword: 'a b', match_type: 'exact' }, { keyword: 'c', match_type: 'phrase' }];
  eq(m.exportSnapshot(a), m.exportSnapshot([...a]), '同じ入力 → 同じ固定版');
  eq(m.exportSnapshot(a).kind, 'ad_copy', '種類が付く (PR2-C から ad_copy = 語 + 商品ターゲット)');
}

console.log('[5] 商品ターゲット (ASIN) はキーワードと別ブロック (PR2-C)');
{
  const adopted = [
    { keyword: 'ハッカ油 スプレー', match_type: 'exact', kind: 'kw' },
    { keyword: 'B0ABCDEFGH', kind: 'asin' },
    { keyword: 'b0abcdefgh', kind: 'asin' },            // 重複 (大小文字)
    { keyword: 'B0ZZZZZZZZ', kind: 'asin' },
    { keyword: 'not-an-asin', kind: 'asin' },           // 形式違いは落とす
    { keyword: 'B0QQQQQQQQ', match_type: 'exact', kind: 'asin' },   // kind=asin に match_type が付いていてもキーワードには混ぜない
  ];
  const t = m.buildProductTargetCopy(adopted);
  eq([t.match_type, t.count, t.text], ['product_targets', 3, 'B0ABCDEFGH\nB0ZZZZZZZZ\nB0QQQQQQQQ'], '1 行 1 ASIN・重複と形式違いは落とす');
  const kw = m.buildKeywordCopy(adopted);
  eq(kw.blocks.map((b) => [b.match_type, b.count]), [['exact', 1]], 'キーワードのブロックに ASIN は混ざらない');
  const snap = m.exportSnapshot(adopted);
  eq([snap.kind, snap.keyword_total, snap.target_total, snap.total], ['ad_copy', 1, 3, 4], '固定版に 語 と 商品ターゲット が別々に数えられる');
  eq(snap.blocks.map((b) => b.match_type), ['exact', 'product_targets'], 'ブロックは キーワード → 商品ターゲット の順');
  eq(m.buildProductTargetCopy([{ keyword: 'x', match_type: 'exact' }]), null, 'kind の無い採用は商品ターゲットにしない');
  eq(m.COPY_BLOCK_JA.product_targets, '商品ターゲット (ASIN)', 'ブロックの表示名');
}

console.log(`\n${pass} PASS / ${fail} FAIL`);
process.exitCode = fail ? 1 : 0;
