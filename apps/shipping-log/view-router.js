/**
 * 出荷件数ダッシュボード (shipping-log の画面側)
 *
 * mirror_shipments_daily (miniPC の f_shipments_daily = NE受注ベースの派生) を読んで
 * 「日ごとの出荷件数」をモール別・配送方法別に見せる。件数の定義は伝票1件 = 発送1件。
 * Amazon Easy Ship は配送方法名 'AES' で入るので、モール=Amazon × 配送方法=AES で数えられる。
 *
 * router.js (GAS 取込 API、Bearer 認証) とは別に session 認証配下へ mount する。
 */
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getMirrorDB } from '../warehouse-mirror/db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = express.Router();

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;

function jstDate(offsetDays = 0) {
  const t = Date.now() + 9 * 3600 * 1000 + offsetDays * 86400 * 1000;
  return new Date(t).toISOString().slice(0, 10);
}

/** クエリの期間を検証して既定 (直近30日) を埋める */
function parseRange(q) {
  const to = DATE_RE.test(String(q.to || '')) ? q.to : jstDate(0);
  const from = DATE_RE.test(String(q.from || '')) ? q.from : jstDate(-29);
  if (from > to) return { from: to, to: from };
  return { from, to };
}

/**
 * 集計本体。日 × モール × 配送方法の明細と、画面が使う集計軸をまとめて返す。
 * slips は「出荷確定した伝票すべて」、cancelled_slips はその内数。
 * basis='shipped' … 出荷確定した伝票すべて = 発送1件として数える (既定)
 * basis='valid'   … そこからキャンセル扱いを引いた数
 */
function loadVolume({ from, to, basis, malls, methods }) {
  const db = getMirrorDB();
  const countExpr = basis === 'valid' ? '(slips - cancelled_slips)' : 'slips';

  const where = ['ship_date >= ?', 'ship_date <= ?'];
  const params = [from, to];
  if (malls && malls.length) {
    where.push(`shop_code IN (${malls.map(() => '?').join(',')})`);
    params.push(...malls);
  }
  if (methods && methods.length) {
    where.push(`delivery_id IN (${methods.map(() => '?').join(',')})`);
    params.push(...methods);
  }
  const whereSql = where.join(' AND ');

  const rows = db.prepare(`
    SELECT ship_date, shop_code, shop_name, platform, delivery_id, delivery_name,
           slips, cancelled_slips, ${countExpr} AS n
    FROM mirror_shipments_daily
    WHERE ${whereSql}
    ORDER BY ship_date, shop_code, delivery_id
  `).all(...params);

  const byDate = new Map();
  const byMall = new Map();
  const byMethod = new Map();
  let total = 0;
  let cancelled = 0;
  for (const r of rows) {
    const mall = r.shop_name || `店舗${r.shop_code}`;
    const method = r.delivery_name || '(未設定)';
    total += r.n;
    cancelled += r.cancelled_slips;
    if (!byDate.has(r.ship_date)) byDate.set(r.ship_date, { date: r.ship_date, total: 0, malls: {}, methods: {} });
    const d = byDate.get(r.ship_date);
    d.total += r.n;
    d.malls[mall] = (d.malls[mall] || 0) + r.n;
    d.methods[method] = (d.methods[method] || 0) + r.n;
    byMall.set(mall, (byMall.get(mall) || 0) + r.n);
    byMethod.set(method, (byMethod.get(method) || 0) + r.n);
  }

  const sortDesc = (m) => [...m.entries()].sort((a, b) => b[1] - a[1]).map(([name, n]) => ({ name, n }));
  const days = [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));

  return {
    from, to, basis: basis === 'valid' ? 'valid' : 'shipped',
    total, cancelled,
    days,
    rows,
    malls: sortDesc(byMall),
    methods: sortDesc(byMethod),
    avgPerDay: days.length ? Math.round(total / days.length) : 0,
  };
}

/** 絞り込み用の選択肢 (期間に関係なく全期間から) */
function loadOptions() {
  const db = getMirrorDB();
  const malls = db.prepare(`
    SELECT shop_code, COALESCE(shop_name, '店舗' || shop_code) AS shop_name, SUM(slips) AS n
    FROM mirror_shipments_daily GROUP BY shop_code ORDER BY n DESC
  `).all();
  const methods = db.prepare(`
    SELECT delivery_id, COALESCE(delivery_name, '(未設定)') AS delivery_name, SUM(slips) AS n
    FROM mirror_shipments_daily GROUP BY delivery_id ORDER BY n DESC
  `).all();
  const range = db.prepare('SELECT MIN(ship_date) AS min_date, MAX(ship_date) AS max_date, MAX(synced_at) AS synced_at FROM mirror_shipments_daily').get();
  return { malls, methods, ...range };
}

const asArray = (v) => (v == null ? [] : (Array.isArray(v) ? v : [v])).map(String).filter(Boolean).slice(0, 50);

router.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'volume.html'));
});

router.get('/api/options', (req, res) => {
  try {
    res.json({ ok: true, ...loadOptions() });
  } catch (e) {
    res.status(503).json({ ok: false, error: String(e.message || e) });
  }
});

router.get('/api/volume', (req, res) => {
  try {
    const { from, to } = parseRange(req.query);
    const { rows, ...summary } = loadVolume({
      from, to, basis: String(req.query.basis || 'shipped'),
      malls: asArray(req.query.mall), methods: asArray(req.query.method),
    });
    res.json({ ok: true, ...summary }); // 明細は CSV 側で返す (画面は集計だけで足りる)
  } catch (e) {
    res.status(503).json({ ok: false, error: String(e.message || e) });
  }
});

/** 表そのままの CSV (Excel で開く前提なので BOM 付き) */
router.get('/api/volume.csv', (req, res) => {
  try {
    const { from, to } = parseRange(req.query);
    // 画面の絞り込みをそのまま反映した明細を出す (loadVolume が同じ条件で引いた rows を使う)
    const data = loadVolume({
      from, to, basis: String(req.query.basis || 'shipped'),
      malls: asArray(req.query.mall), methods: asArray(req.query.method),
    });
    const esc = (v) => {
      const s = String(v ?? '');
      return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const lines = ['出荷日,モール,配送方法,件数,うちキャンセル'];
    for (const r of data.rows) {
      lines.push([
        r.ship_date, r.shop_name || `店舗${r.shop_code}`, r.delivery_name || '(未設定)',
        r.n, r.cancelled_slips,
      ].map(esc).join(','));
    }
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="shipments_${data.from}_${data.to}.csv"`);
    res.send('﻿' + lines.join('\r\n'));
  } catch (e) {
    res.status(503).send(String(e.message || e));
  }
});

export default router;
