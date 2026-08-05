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
const MAX_RANGE_DAYS = 1096; // 3年。これ以上は画面で扱えないので 400 で断る

/** 実在する日付か (2026-99-99 / 2026-02-31 を弾く) */
function isRealDate(s) {
  if (!DATE_RE.test(String(s || ''))) return false;
  const d = new Date(`${s}T00:00:00Z`);
  return !Number.isNaN(d.getTime()) && d.toISOString().slice(0, 10) === s;
}

function jstDate(offsetDays = 0) {
  const t = Date.now() + 9 * 3600 * 1000 + offsetDays * 86400 * 1000;
  return new Date(t).toISOString().slice(0, 10);
}

class BadRequest extends Error {}

/** クエリの期間を検証して既定 (直近30日) を埋める。不正なら 400 で返す */
function parseRange(q) {
  const rawFrom = q.from == null || q.from === '' ? null : String(q.from);
  const rawTo = q.to == null || q.to === '' ? null : String(q.to);
  if (rawFrom !== null && !isRealDate(rawFrom)) throw new BadRequest(`from が不正です: ${rawFrom}`);
  if (rawTo !== null && !isRealDate(rawTo)) throw new BadRequest(`to が不正です: ${rawTo}`);
  let from = rawFrom || jstDate(-29);
  let to = rawTo || jstDate(0);
  if (from > to) [from, to] = [to, from];
  const days = (Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86400000 + 1;
  if (days > MAX_RANGE_DAYS) throw new BadRequest(`期間が長すぎます (${MAX_RANGE_DAYS}日まで)`);
  return { from, to };
}

/**
 * 集計本体。日 or 月 × モール × 配送方法の明細と、画面が使う集計軸をまとめて返す。
 * slips は「出荷確定した伝票すべて」、cancelled_slips はその内数。
 * basis='shipped' … 出荷確定した伝票すべて = 発送1件として数える (既定)
 * basis='valid'   … そこからキャンセル扱いを引いた数
 * granularity='day' | 'month' … 集計の粒度。month のときは各期間に
 *   「出荷があった日数 (稼働日)」と「1日あたりの平均」を付ける
 *   (休業日を含めた暦日で割ると実感とズレるため、分母は出荷実績のある日)
 */
function loadVolume({ from, to, basis, malls, methods, granularity }) {
  const db = getMirrorDB();
  const isMonth = granularity === 'month';
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

  const byBucket = new Map();
  const byMall = new Map();
  const byMethod = new Map();
  const workDays = new Map(); // bucket → 出荷があった日の集合 (月の1日平均の分母)
  let total = 0;
  let cancelled = 0;
  for (const r of rows) {
    const mall = r.shop_name || `店舗${r.shop_code}`;
    const method = r.delivery_name || '(未設定)';
    const key = isMonth ? r.ship_date.slice(0, 7) : r.ship_date;
    total += r.n;
    cancelled += r.cancelled_slips;
    if (!byBucket.has(key)) byBucket.set(key, { date: key, total: 0, malls: {}, methods: {} });
    const d = byBucket.get(key);
    d.total += r.n;
    d.malls[mall] = (d.malls[mall] || 0) + r.n;
    d.methods[method] = (d.methods[method] || 0) + r.n;
    byMall.set(mall, (byMall.get(mall) || 0) + r.n);
    byMethod.set(method, (byMethod.get(method) || 0) + r.n);
    if (isMonth) {
      if (!workDays.has(key)) workDays.set(key, new Set());
      workDays.get(key).add(r.ship_date);
    }
  }

  const sortDesc = (m) => [...m.entries()].sort((a, b) => b[1] - a[1]).map(([name, n]) => ({ name, n }));
  const days = [...byBucket.values()].sort((a, b) => a.date.localeCompare(b.date));
  if (isMonth) {
    for (const d of days) {
      d.work_days = workDays.get(d.date).size;
      d.avg_per_day = d.work_days ? Math.round(d.total / d.work_days) : 0;
    }
  }

  // 全体の日数 (月別表示でも「1日あたり」の分母は実際に出荷があった日数)
  const totalWorkDays = isMonth
    ? days.reduce((a, d) => a + d.work_days, 0)
    : days.length;

  return {
    from, to, basis: basis === 'valid' ? 'valid' : 'shipped',
    granularity: isMonth ? 'month' : 'day',
    total, cancelled,
    days,
    rows,
    malls: sortDesc(byMall),
    methods: sortDesc(byMethod),
    workDays: totalWorkDays,
    avgPerDay: totalWorkDays ? Math.round(total / totalWorkDays) : 0,
    avgPerBucket: days.length ? Math.round(total / days.length) : 0,
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
      granularity: String(req.query.granularity || 'day'),
      malls: asArray(req.query.mall), methods: asArray(req.query.method),
    });
    res.json({ ok: true, ...summary }); // 明細は CSV 側で返す (画面は集計だけで足りる)
  } catch (e) {
    if (e instanceof BadRequest) return res.status(400).json({ ok: false, error: String(e.message) });
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
      granularity: String(req.query.granularity || 'day'),
      malls: asArray(req.query.mall), methods: asArray(req.query.method),
    });
    // Excel の数式インジェクション対策: 先頭が = + - @ (と制御文字) のセルは ' を付けて無害化する。
    // モール名・配送方法名は NE 由来の外部データなので、そのまま出すと式として評価されうる。
    const esc = (v) => {
      let s = String(v ?? '');
      if (/^[=+\-@\t\r]/.test(s)) s = `'${s}`;
      return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
    };
    if (data.granularity === 'month') {
      // 月別表示のときは月粒度に畳んで出す (画面と同じ粒度でないと突き合わせられない)
      const agg = new Map();
      for (const r of data.rows) {
        const mall = r.shop_name || `店舗${r.shop_code}`;
        const method = r.delivery_name || '(未設定)';
        const key = `${r.ship_date.slice(0, 7)}${mall}${method}`;
        if (!agg.has(key)) agg.set(key, { ym: r.ship_date.slice(0, 7), mall, method, n: 0, cancelled: 0, days: new Set() });
        const a = agg.get(key);
        a.n += r.n;
        a.cancelled += r.cancelled_slips;
        a.days.add(r.ship_date);
      }
      const lines = ['出荷月,モール,配送方法,件数,うちキャンセル,出荷があった日数,1日あたり平均'];
      for (const a of [...agg.values()].sort((x, y) => x.ym.localeCompare(y.ym) || x.mall.localeCompare(y.mall) || x.method.localeCompare(y.method))) {
        lines.push([a.ym, a.mall, a.method, a.n, a.cancelled, a.days.size,
          a.days.size ? Math.round(a.n / a.days.size) : 0].map(esc).join(','));
      }
      res.setHeader('Content-Type', 'text/csv; charset=utf-8');
      res.setHeader('Content-Disposition', `attachment; filename="shipments_monthly_${data.from}_${data.to}.csv"`);
      return res.send('﻿' + lines.join('\r\n'));
    }
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
    if (e instanceof BadRequest) return res.status(400).send(String(e.message));
    res.status(503).send(String(e.message || e));
  }
});

export default router;
