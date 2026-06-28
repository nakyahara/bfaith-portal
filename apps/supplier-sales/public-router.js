/**
 * 仕入れ先向け売れ筋共有 — 公開ルーター（ログイン不要・トークンURL）
 *
 *  GET /share/supplier/:token   仕入先がログインなしで自社商品の売れ筋を見る
 *
 * server.js で requireAuth/requireAppAccess の「外側」、/share にマウントする。
 *
 * セキュリティ（Codex レビュー反映）:
 *  - トークンは推測困難（256bit）。DB にはハッシュのみ。失効・期限チェックあり。
 *  - 検索エンジンに載らないよう noindex / no-store ヘッダを必ず付与。
 *  - 簡易レート制限（IP 単位、メモリ）。総当たり緩和。
 *  - 原価・粗利は一切出さない（販売個数・売上のみ）。
 *  - SKU 名寄せできなかった売上は公開ページには出さない（fail-closed、aggregate 側で除外済）。
 *  - ログにトークン全文を出さない。
 */
import express from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { getSupplierSummary, MALL_LABELS } from './aggregate.js';
import { resolveActiveToken, getSupplierName } from './share-db.js';

const router = express.Router();

// ── 簡易レート制限（IP 単位、60秒で 60 リクエストまで）──
const RATE_WINDOW_MS = 60_000;
const RATE_MAX = 60;
const hits = new Map(); // ip -> { count, resetAt }

const RATE_MAP_CAP = 10_000;

function rateLimited(ip) {
  const now = Date.now();
  let rec = hits.get(ip);
  if (!rec || now > rec.resetAt) {
    rec = { count: 0, resetAt: now + RATE_WINDOW_MS };
    hits.set(ip, rec);
  }
  rec.count += 1;
  // マップ肥大化防止: 上限超過で期限切れを一掃し、それでも超過なら全消去
  // （レート窓は短いので全消去しても実害は軽微）。ユニークIP洪水での無限増加を防ぐ。
  if (hits.size > RATE_MAP_CAP) {
    for (const [k, v] of hits) if (now > v.resetAt) hits.delete(k);
    if (hits.size > RATE_MAP_CAP) hits.clear();
  }
  return rec.count > RATE_MAX;
}

function setNoIndexHeaders(res) {
  res.setHeader('X-Robots-Tag', 'noindex, nofollow, noarchive');
  res.setHeader('Cache-Control', 'private, no-store, max-age=0');
  res.setHeader('Referrer-Policy', 'no-referrer');
}

router.get('/supplier/:token', (req, res) => {
  setNoIndexHeaders(res);
  const ip = req.ip || req.socket?.remoteAddress || 'unknown';
  if (rateLimited(ip)) {
    return res.status(429).send('アクセスが多すぎます。しばらくしてから再度お試しください。');
  }

  const raw = req.params.token;
  // トークン形式の事前チェック（base64url 43文字相当）。形式不正は探索とみなし即 404。
  if (!/^[A-Za-z0-9_-]{20,128}$/.test(raw || '')) {
    return res.status(404).send('ページが見つかりません。');
  }

  let token;
  try {
    token = resolveActiveToken(raw);
  } catch (e) {
    console.error('[supplier-sales/public] token resolve error:', e.message);
    return res.status(503).send('準備中です。しばらくしてから再度お試しください。');
  }
  if (!token) {
    // 無効・失効・期限切れをまとめて 404（存在有無を漏らさない）
    return res.status(404).send('このリンクは無効か、有効期限が切れています。');
  }

  let summary, supplierName;
  try {
    summary = getSupplierSummary(getMirrorDB(), token.supplierCode);
    supplierName = getSupplierName(token.supplierCode) || token.supplierCode;
  } catch (e) {
    console.error('[supplier-sales/public] summary error:', e.message);
    return res.status(503).send('準備中です。しばらくしてから再度お試しください。');
  }

  res.render('supplier-sales-public', {
    title: `${supplierName} 様 販売実績`,
    supplierName,
    mallLabels: MALL_LABELS,
    windows: summary.windows,
    products: summary.products,
    totals: summary.totals,
  });
});

export default router;
