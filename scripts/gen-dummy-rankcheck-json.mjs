#!/usr/bin/env node
/**
 * 開発・テスト用: 模擬 ranking-checker.json を生成する。
 *
 * Usage:
 *   node scripts/gen-dummy-rankcheck-json.mjs [products] [days] [out]
 *   node scripts/gen-dummy-rankcheck-json.mjs 20 10 data-test/ranking-checker.json
 */
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

const N = parseInt(process.argv[2] || '20', 10);
const DAYS = parseInt(process.argv[3] || '10', 10);
const OUT = process.argv[4] || 'data-test/ranking-checker.json';

function jstDateOffset(delta) {
  const d = new Date(Date.now() + 9 * 60 * 60 * 1000);
  d.setUTCDate(d.getUTCDate() + delta);
  return d.toISOString().slice(0, 10);
}

function randRank() {
  const r = Math.random();
  if (r < 0.1) return null;       // 10% 圏外
  if (r < 0.13) return 'error';   // 3% API失敗
  return Math.floor(Math.random() * 100) + 1;
}

const products = [];
for (let i = 0; i < N; i++) {
  const code = `DUMMY-${String(i).padStart(4, '0')}`;
  const hasComp2 = i % 3 === 0;
  const hasAmazon = i % 2 === 0;
  const history = [];
  for (let d = DAYS - 1; d >= 0; d--) {
    const entry = {
      date: jstDateOffset(-d),
      own_rank: randRank(),
      competitor1_rank: randRank(),
      competitor2_rank: hasComp2 ? randRank() : null,
    };
    if (hasAmazon) entry.amazon_own_rank = randRank();
    history.push(entry);
  }
  products.push({
    id: crypto.randomUUID(),
    keyword: `ダミーキーワード${i}`,
    product_code: code,
    own_url: `https://item.rakuten.co.jp/b-faith/${code}/`,
    yahoo_url: `https://store.shopping.yahoo.co.jp/b-faith01/${code}.html`,
    amazon_url: hasAmazon ? `https://www.amazon.co.jp/dp/B000${String(i).padStart(6, '0')}` : '',
    amazon_asin: hasAmazon ? `B000${String(i).padStart(6, '0')}` : '',
    competitor1_url: `https://item.rakuten.co.jp/competitor1/item-${i}/`,
    competitor2_url: hasComp2 ? `https://item.rakuten.co.jp/competitor2/item-${i}/` : '',
    review_count: Math.floor(Math.random() * 500),
    history,
  });
}

const dir = path.dirname(OUT);
if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
fs.writeFileSync(OUT, JSON.stringify({ products }, null, 2), 'utf-8');
console.log(`[gen-dummy] ${OUT} に ${N} 商品 × ${DAYS} 日分 書き出し完了`);
console.log(`[gen-dummy] サイズ: ${(fs.statSync(OUT).size / 1024).toFixed(1)} KB`);
