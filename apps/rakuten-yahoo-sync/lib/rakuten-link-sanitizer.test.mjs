/**
 * R16 (2026-07-04 中原さん指示・重大): 楽天リンク根絶のテスト。
 *
 * 実例 (中原さん報告): AA0203 の salesDescription 内「セット商品」「関連商品」テーブルに
 * item.rakuten.co.jp への <a> が入っており、 Yahoo 側に楽天誘導リンクとして登録されていた
 * (Yahoo!ショッピング規約違反)。
 *
 * 実行: node --test apps/rakuten-yahoo-sync/lib/rakuten-link-sanitizer.test.mjs
 */
import { test } from 'node:test';
import assert from 'node:assert/strict';

process.env.YAHOO_SELLER_ID = 'b-faith';
process.env.RAKUTEN_SHOP_SLUG = 'b-faith';

const {
  classifyRakutenHref,
  scrubRakutenUrlsFromText,
  findRakutenResidueInFields,
  isRakutenFamilyHost,
  stripEcmpanBlocks,
} = await import('./rakuten-link-sanitizer.js');
const { sanitizeProductHtml } = await import('./html-sanitize.js');
const { validateYahooEditItemFields, YahooFieldValidationError } = await import('./yahoo-edititem-validator.js');

// ── classifyRakutenHref ──

test('自店商品リンク → Yahoo ストア URL に書換 (code は小文字化)', () => {
  const c = classifyRakutenHref('https://item.rakuten.co.jp/b-faith/AA0203/');
  assert.equal(c.kind, 'rewrite');
  assert.equal(c.yahooUrl, 'https://store.shopping.yahoo.co.jp/b-faith/aa0203.html');
});

test('自店商品リンク (query/fragment 付き) も書換', () => {
  const c = classifyRakutenHref('https://item.rakuten.co.jp/b-faith/tp-react/?s-id=top#review');
  assert.equal(c.kind, 'rewrite');
  assert.equal(c.yahooUrl, 'https://store.shopping.yahoo.co.jp/b-faith/tp-react.html');
});

test('bouncer 形式 (percent-encode 埋込) も自店商品なら書換', () => {
  const c = classifyRakutenHref(
    'https://shopping.yahoo.co.jp/stores/wrap/bouncer.html?dest_path=https%3A%2F%2Fitem.rakuten.co.jp%2Fb-faith%2FAA0203-2%2F'
  );
  assert.equal(c.kind, 'rewrite');
  assert.equal(c.yahooUrl, 'https://store.shopping.yahoo.co.jp/b-faith/aa0203-2.html');
});

test('Codex R1 High: %2E エンコード (ドットまでエンコード) もすり抜けさせない', () => {
  const c = classifyRakutenHref(
    'https://shopping.yahoo.co.jp/stores/wrap/bouncer.html?dest_path=https%3A%2F%2Fitem%2Erakuten%2Eco%2Ejp%2Fb-faith%2FAA0203%2F'
  );
  assert.equal(c.kind, 'rewrite');
  assert.equal(c.yahooUrl, 'https://store.shopping.yahoo.co.jp/b-faith/aa0203.html');
});

test('Codex R1 High: 二重エンコードも反復 decode で検知', () => {
  // %253A = %3A の二重エンコード
  const doubled = 'https://example.com/r?u=https%253A%252F%252Fitem%252Erakuten%252Eco%252Ejp%252Fb-faith%252Fxyz%252F';
  const c = classifyRakutenHref(doubled);
  assert.equal(c.kind, 'rewrite');
  assert.equal(c.yahooUrl, 'https://store.shopping.yahoo.co.jp/b-faith/xyz.html');
  const r = findRakutenResidueInFields({ additional1: doubled });
  assert.equal(r.length, 1, 'residue backstop も二重エンコードを検知');
});

test('Codex R2 High: 不正 % (%ZZ) が混在しても %2E エンコード楽天ドメインを検知', () => {
  const href = 'https://shopping.yahoo.co.jp/bouncer.html?bad=%ZZ&dest_path=https%3A%2F%2Fitem%2Erakuten%2Eco%2Ejp%2Fb-faith%2FAA0203%2F';
  const c = classifyRakutenHref(href);
  assert.equal(c.kind, 'rewrite');
  assert.equal(c.yahooUrl, 'https://store.shopping.yahoo.co.jp/b-faith/aa0203.html');
  const r = findRakutenResidueInFields({ additional1: href });
  assert.equal(r.length, 1, 'residue backstop も不正%混在形を検知');
});

test('他店の楽天商品リンク → remove', () => {
  const c = classifyRakutenHref('https://item.rakuten.co.jp/other-shop/xyz123/');
  assert.equal(c.kind, 'remove');
});

test('楽天検索/店舗トップ/CDN → remove', () => {
  assert.equal(classifyRakutenHref('https://search.rakuten.co.jp/search/mall/foo/').kind, 'remove');
  assert.equal(classifyRakutenHref('https://www.rakuten.ne.jp/gold/b-faith/').kind, 'remove');
  assert.equal(classifyRakutenHref('https://tshop.r10s.jp/b-faith/cabinet/x.jpg').kind, 'remove');
});

test('楽天と無関係な URL → not_rakuten (触らない)', () => {
  assert.equal(classifyRakutenHref('https://store.shopping.yahoo.co.jp/b-faith/foo.html').kind, 'not_rakuten');
  assert.equal(classifyRakutenHref('https://www.google.com/').kind, 'not_rakuten');
});

test('YAHOO_SELLER_ID 未設定でも楽天リンクは残さない (自店商品でも remove)', () => {
  const c = classifyRakutenHref('https://item.rakuten.co.jp/b-faith/AA0203/', { sellerId: '' });
  // opts.sellerId='' は falsy → env fallback。 env を消して検証
  const saved = process.env.YAHOO_SELLER_ID;
  delete process.env.YAHOO_SELLER_ID;
  try {
    const c2 = classifyRakutenHref('https://item.rakuten.co.jp/b-faith/AA0203/');
    assert.equal(c2.kind, 'remove');
  } finally {
    process.env.YAHOO_SELLER_ID = saved;
  }
  assert.equal(c.kind, 'rewrite'); // env あり経路は書換
});

test('isRakutenFamilyHost: subdomain 一致 / 部分文字列は不一致', () => {
  assert.equal(isRakutenFamilyHost('item.rakuten.co.jp'), true);
  assert.equal(isRakutenFamilyHost('rakuten.co.jp'), true);
  assert.equal(isRakutenFamilyHost('notrakuten.co.jp'), false);
  assert.equal(isRakutenFamilyHost('rakuten.co.jp.evil.com'), false);
});

// ── sanitizeProductHtml 統合 (AA0203 実例) ──

const AA0203_FIXTURE = `
<table><tbody>
 <tr><th><b>＼ お得に買える！セット商品／</b></th></tr>
 <tr><td><a target="_blank" href="https://item.rakuten.co.jp/b-faith/AA0203-2/"><img src="https://image.rakuten.co.jp/b-faith/cabinet/set2.jpg"></a></td></tr>
 <tr><td><a target="_blank" href="https://item.rakuten.co.jp/b-faith/AA0203-3/"><img src="https://image.rakuten.co.jp/b-faith/cabinet/set3.jpg"></a></td></tr>
</tbody></table>
<table><tbody>
 <tr><th><b>＼ よく一緒に購入されている商品 ／</b></th></tr>
 <tr><td><a target="_blank" href="https://item.rakuten.co.jp/b-faith/0726-001963/">関連商品はこちら</a></td></tr>
 <tr><td><a href="https://search.rakuten.co.jp/search/mall/b-faith/">その他の商品を検索</a></td></tr>
</tbody></table>`;

test('sanitize: 自店商品 <a> は Yahoo ストア URL に書換され楽天ドメインが消える', () => {
  const out = sanitizeProductHtml(AA0203_FIXTURE);
  assert.ok(out.includes('https://store.shopping.yahoo.co.jp/b-faith/aa0203-2.html'), 'セット商品1が書換');
  assert.ok(out.includes('https://store.shopping.yahoo.co.jp/b-faith/aa0203-3.html'), 'セット商品2が書換');
  assert.ok(out.includes('https://store.shopping.yahoo.co.jp/b-faith/0726-001963.html'), '関連商品が書換');
  assert.ok(!/rakuten|r10s/i.test(out), `楽天ドメイン完全消滅: ${out}`);
});

test('sanitize: 書換不能な楽天リンクは unwrap (テキストは残しリンクは消える)', () => {
  const out = sanitizeProductHtml(AA0203_FIXTURE);
  assert.ok(out.includes('その他の商品を検索'), '検索リンクのテキストは残る');
  assert.ok(!out.includes('search.rakuten'), '検索リンクの href は消える');
});

test('sanitize: 楽天と無関係な <a href> は従来通り保持', () => {
  const out = sanitizeProductHtml('<a href="https://store.shopping.yahoo.co.jp/b-faith/x.html">既存リンク</a>');
  assert.ok(out.includes('href="https://store.shopping.yahoo.co.jp/b-faith/x.html"'));
});

test('sanitize: テキストノード内の生楽天 URL (リンクでない文字列) もスクラブされる', () => {
  const out = sanitizeProductHtml(
    '<p>詳細は https://item.rakuten.co.jp/b-faith/AA0203/ を参照。検索は https://search.rakuten.co.jp/mall/x/ で。</p>'
  );
  assert.ok(out.includes('https://store.shopping.yahoo.co.jp/b-faith/aa0203.html'), '自店商品は書換');
  assert.ok(!/rakuten/i.test(out), `楽天 URL 消滅: ${out}`);
});

// ── R18: ECMPAN ブロック除去 + カテゴリ URL 誤書換の回帰 (GONESH-no4-2 実例) ──

const GONESH_ECMPAN_FIXTURE = '<!--ECMPAN--><a target="_blank" href="https://shopping.yahoo.co.jp/stores/wrap/bouncer.html?dest_path=https%3A%2F%2Fitem.rakuten.co.jp%2Fb-faith%2Fc%2F0000000220%2F">アロマスプレー・香料</a> ＞ <a target="_blank" href="https://shopping.yahoo.co.jp/stores/wrap/bouncer.html?dest_path=https%3A%2F%2Fitem.rakuten.co.jp%2Fb-faith%2Fc%2F0000000305%2F">ルームフレグランス</a><br><br><!--/ECMPAN-->本文はここから';

test('R18: ECMPAN ブロックは丸ごと移行対象外 (GONESH-no4-2 実例)', () => {
  const out = sanitizeProductHtml(GONESH_ECMPAN_FIXTURE);
  assert.ok(!out.includes('アロマスプレー・香料'), 'パンくずテキスト消滅');
  assert.ok(!out.includes('ECMPAN'), 'マーカー消滅');
  assert.ok(!/rakuten/i.test(out), '楽天ドメイン消滅');
  assert.ok(out.includes('本文はここから'), 'ブロック外の本文は残る');
});

test('R18: stripEcmpanBlocks 単体 — 対ブロック除去 + stray マーカーも除去', () => {
  assert.equal(stripEcmpanBlocks('a<!--ECMPAN-->x<!--/ECMPAN-->b'), 'ab');
  assert.equal(stripEcmpanBlocks('a<!-- ECMPAN -->x<!-- /ECMPAN -->b'), 'ab', '空白入りマーカー');
  assert.equal(stripEcmpanBlocks('a<!--ECMPAN-->閉じ忘れb'), 'a閉じ忘れb', 'stray はマーカーのみ除去');
  assert.equal(stripEcmpanBlocks(''), '');
  assert.equal(stripEcmpanBlocks(null), '');
});

test('R18: 楽天カテゴリページ URL (/c/NNNN/) は商品と誤認せず remove (旧実装は c.html に誤書換)', () => {
  assert.equal(classifyRakutenHref('https://item.rakuten.co.jp/b-faith/c/0000000220/').kind, 'remove');
  const bouncer = classifyRakutenHref(
    'https://shopping.yahoo.co.jp/stores/wrap/bouncer.html?dest_path=https%3A%2F%2Fitem.rakuten.co.jp%2Fb-faith%2Fc%2F0000000220%2F'
  );
  assert.equal(bouncer.kind, 'remove');
});

test('R18: HTML コメントは全除去 (コメント内の楽天 URL も residue に残らない)', () => {
  const out = sanitizeProductHtml('<p>本文</p><!-- https://item.rakuten.co.jp/b-faith/x/ メモ -->');
  assert.ok(!out.includes('<!--'), out);
  assert.ok(!/rakuten/i.test(out));
  assert.ok(out.includes('本文'));
});

// ── scrubRakutenUrlsFromText (explanation プレーンテキスト経路) ──

test('text scrub: 自店商品 URL → Yahoo URL、 その他楽天 URL → 削除、 無関係 URL → 保持', () => {
  const s = scrubRakutenUrlsFromText(
    '詳細は https://item.rakuten.co.jp/b-faith/AA0203/ を参照。検索は https://search.rakuten.co.jp/mall/x/ から。公式 https://example.com/a も。'
  );
  assert.ok(s.includes('https://store.shopping.yahoo.co.jp/b-faith/aa0203.html'));
  assert.ok(!/rakuten/i.test(s), `楽天 URL 消滅: ${s}`);
  assert.ok(s.includes('https://example.com/a'));
});

// ── findRakutenResidueInFields + validator backstop ──

test('residue 検知: percent-encode された bouncer 形式も検知する', () => {
  const r = findRakutenResidueInFields({
    caption: 'ok text',
    additional1: '<a href="https://shopping.yahoo.co.jp/bouncer.html?dest_path=https%3A%2F%2Fitem.rakuten.co.jp%2Fb-faith%2FAA0203%2F">x</a>',
  });
  assert.equal(r.length, 1);
  assert.equal(r[0].field, 'additional1');
});

test('residue 検知: clean fields は空配列', () => {
  const r = findRakutenResidueInFields({
    name: '楽天ふるさと納税ではなく通常商品', // 日本語の「楽天」はドメインではないので OK
    caption: '<a href="https://store.shopping.yahoo.co.jp/b-faith/x.html">y</a>',
    price: 1000,
  });
  assert.deepEqual(r, []);
});

test('validator: 楽天ドメイン残存で YahooFieldValidationError (fail-closed)', () => {
  assert.throws(
    () => validateYahooEditItemFields({ item_code: 'aa0203', caption: 'see https://item.rakuten.co.jp/b-faith/x/' }),
    (e) => e instanceof YahooFieldValidationError && /rakuten_link_residue/.test(e.message)
  );
});

test('validator: clean fields は通過', () => {
  validateYahooEditItemFields({ item_code: 'aa0203', caption: '<p>通常説明</p>' });
});
