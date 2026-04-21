/**
 * 楽天順位チェッカー共通ヘルパー。
 *
 * Phase 3 (Render から auto-check.js を削除する) 準備として、
 * scheduler.js / csv-export.js / rankcheck-service.js などが必要とする純粋関数を
 * auto-check.js から切り出した場所。auto-check.js は runAutoCheck の実装ファイルで、
 * miniPC / dev fallback でしか使われなくなっても、ここが残れば UI側が壊れない。
 *
 * ここには副作用あり関数 (log) も含む。`log` は auto-check.js 時代から
 * DATA_DIR/ranking-checker.log に追記する挙動を保つ。
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, '..', '..', 'data');
const LOG_FILE = path.join(DATA_DIR, 'ranking-checker.log');

export function log(msg) {
  const ts = new Date().toISOString().replace('T', ' ').replace(/\.\d+Z$/, '');
  const line = `[${ts}] ${msg}`;
  console.log('[RankCheck]', msg);
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFileSync(LOG_FILE, line + '\n', 'utf-8');
  } catch {}
}

export function today() {
  const d = new Date(Date.now() + 9 * 60 * 60 * 1000);
  return `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
}

// ── URL helpers ──

export function normalizeUrl(url) {
  if (!url) return '';
  url = url.trim().replace(/^https?:\/\//, '');
  if (url.startsWith('www.')) url = url.slice(4);
  url = url.split('?')[0].split('#')[0].replace(/\/+$/, '');
  return url.toLowerCase();
}

export function urlsMatch(a, b) { return normalizeUrl(a) === normalizeUrl(b); }

export function codeToRakutenUrl(code) { return `https://item.rakuten.co.jp/b-faith/${code.replace(/\/+$/, '')}/`; }
export function codeToYahooUrl(code) { return `https://store.shopping.yahoo.co.jp/b-faith01/${code.replace(/\/+$/, '')}.html`; }

export function codeFromRakutenUrl(url) {
  const m = (url || '').match(/item\.rakuten\.co\.jp\/b-faith\/([^/?#]+)/i);
  return m ? m[1] : '';
}

export function getRakutenUrl(product) {
  return product.own_url || (product.product_code ? codeToRakutenUrl(product.product_code) : '');
}
export function getYahooUrl(product) {
  return product.yahoo_url || (product.product_code ? codeToYahooUrl(product.product_code) : '');
}
export function getAmazonAsin(product) {
  if (product.amazon_asin) return product.amazon_asin;
  const url = product.amazon_url || '';
  if (url) {
    const m = url.match(/\/(?:dp|gp\/product)\/([A-Z0-9]{10})/i);
    return m ? m[1] : '';
  }
  return '';
}

export const RANK_ERROR = -1;

export { LOG_FILE, DATA_DIR };
