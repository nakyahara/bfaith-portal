/**
 * 郵便料金判定 (postage) — 画面 + API (PR1a)
 *
 * PR1a の役割は「マスタを育てる」こと。印字はまだしない (PR1b)。
 * 画面が答えるのは 3 つだけ:
 *   1. いま何割が自動で確定できるか
 *   2. 次に何を測れば一番効くか
 *   3. 取り込んだ表のどこが壊れているか
 */
import express from 'express';
import multer from 'multer';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';
import {
  getDB, initPostageDB, getTariffVersionFor, getBands, jstToday,
} from './db.js';
import { importWeightFile } from './import.js';
import { coverageReport, warehouseAvailable } from './coverage.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const UPLOAD_DIR = path.join(DATA_DIR, 'uploads', 'postage');
fs.mkdirSync(UPLOAD_DIR, { recursive: true });

const router = express.Router();
const view = (name) => path.join(__dirname, 'views', name);
// 重量表は 1MB もあれば足りる (実データは 44KB)。無制限にしない。
// 拡張子も絞る (ExcelJS に何でも渡さない)
const upload = multer({
  dest: UPLOAD_DIR,
  limits: { fileSize: 2 * 1024 * 1024, files: 1 },
  fileFilter: (_req, file, cb) => {
    if (!/\.(xlsx|csv)$/i.test(file.originalname || '')) {
      return cb(new Error('取り込めるのは .xlsx か .csv です'));
    }
    cb(null, true);
  },
});

initPostageDB();

// ─── CSRF 二段ガード (更新系。inquiry-hub / product-links と同じ) ───
router.use('/api/', (req, res, next) => {
  if (['GET', 'HEAD', 'OPTIONS'].includes(req.method)) return next();
  const origin = req.headers.origin;
  if (origin) {
    let host = null;
    try { host = new URL(origin).host; } catch { /* 不正 Origin は不一致として拒否 */ }
    if (!host || host !== req.headers.host) return res.status(403).json({ ok: false, error: 'origin_mismatch' });
  }
  // multipart (取込) は Content-Type で守れないぶん、Origin か Referer を必須にする。
  // マスタ反映は料金判定そのものを変える操作なので、ここだけ緩くしない
  if (req.path === '/import') {
    if (!origin && !req.headers.referer) {
      return res.status(403).json({ ok: false, error: 'origin_required' });
    }
    if (!origin && req.headers.referer) {
      let rhost = null;
      try { rhost = new URL(req.headers.referer).host; } catch { /* 不正 Referer は不一致として拒否 */ }
      if (!rhost || rhost !== req.headers.host) return res.status(403).json({ ok: false, error: 'origin_mismatch' });
    }
    return next();
  }
  if (!/^application\/json\b/i.test(String(req.headers['content-type'] || ''))) {
    return res.status(415).json({ ok: false, error: 'Content-Type は application/json にしてください' });
  }
  next();
});
router.use(express.json({ limit: '256kb' }));

const actorOf = (req) => (req.session?.email || req.session?.displayName || 'unknown');

// 既定の集計期間 = 直近90日。少なすぎると季節の偏りで判断を誤る
function defaultSince() {
  const d = new Date(Date.now() + 9 * 3600 * 1000);
  d.setUTCDate(d.getUTCDate() - 90);
  return d.toISOString().slice(0, 10);
}

// ─────────────────────────── 画面 ───────────────────────────
router.get('/', (req, res) => {
  const db = getDB();
  const since = String(req.query.since || defaultSince()).slice(0, 10);
  const until = req.query.until ? String(req.query.until).slice(0, 10) : null;

  const report = coverageReport({ since, until });
  const tariff = getTariffVersionFor(jstToday());
  const materials = db.prepare('SELECT * FROM pm_materials ORDER BY active DESC, material_code').all();
  const overheads = db.prepare('SELECT * FROM pm_overheads ORDER BY code').all();
  const settings = Object.fromEntries(db.prepare('SELECT key, value FROM pm_settings').all().map((r) => [r.key, r.value]));
  const skuStats = db.prepare(`
    SELECT COUNT(*) total,
           SUM(CASE WHEN unit_weight_g IS NOT NULL THEN 1 ELSE 0 END) with_weight,
           SUM(CASE WHEN thickness_mm  IS NOT NULL THEN 1 ELSE 0 END) with_thickness,
           SUM(CASE WHEN default_material_code IS NOT NULL THEN 1 ELSE 0 END) with_material
      FROM pm_skus`).get();
  const lastRun = db.prepare('SELECT * FROM pm_import_runs ORDER BY import_run_id DESC LIMIT 1').get() || null;
  const issues = lastRun
    ? db.prepare(`SELECT * FROM pm_import_issues WHERE import_run_id=?
                   ORDER BY CASE severity WHEN 'error' THEN 0 ELSE 1 END, row_no LIMIT 200`).all(lastRun.import_run_id)
    : [];

  res.render(view('index.ejs'), {
    report, tariff, bands: tariff ? getBands(tariff.tariff_version_id) : [],
    materials, overheads, settings, skuStats, lastRun, issues,
    since, until: until || '', warehouseOk: warehouseAvailable(),
    username: req.session?.email, displayName: req.session?.displayName,
  });
});

// 不足リストの CSV (現場に配って埋めてもらうため)
router.get('/missing.csv', (req, res) => {
  const since = String(req.query.since || defaultSince()).slice(0, 10);
  const report = coverageReport({ since });
  if (!report.available) return res.status(503).send('warehouse.db が読めません');
  const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = ['優先,出現通数,商品コード,商品名,足りないもの'];
  report.missingSkus.forEach((m, i) => {
    lines.push([i + 1, m.count, esc(m.sku_code), esc(m.name || ''), esc(m.needs)].join(','));
  });
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="postage_missing_${jstToday()}.csv"`);
  res.send('﻿' + lines.join('\r\n'));   // Excel で開くので BOM をつける
});

// ─────────────────────────── API ───────────────────────────

/** 重量表の取込。既定は dry-run (検証だけ)。apply=1 で実際に入れる。 */
router.post('/api/import', upload.single('file'), async (req, res) => {
  if (!req.file) return res.status(400).json({ ok: false, error: 'ファイルが選ばれていません' });
  const dryRun = String(req.body?.apply || '') !== '1';
  try {
    const r = await importWeightFile(req.file.path, { dryRun, actor: actorOf(req) });
    res.json({ ok: true, ...r, issues: r.issues.slice(0, 300) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  } finally {
    // 取り込んだら生ファイルは残さない (個人情報は無いが、置きっぱなしにしない)
    fs.rm(req.file.path, { force: true }, () => {});
  }
});

/** 資材の自重・外寸。ここが埋まると「不明」が一気に減る。 */
router.post('/api/materials/:code', (req, res) => {
  const { code } = req.params;
  const db = getDB();
  const cur = db.prepare('SELECT * FROM pm_materials WHERE material_code=?').get(code);
  if (!cur) return res.status(404).json({ ok: false, error: '資材が見つかりません' });

  // 0 は許さない。欠測は空欄 (NULL)。0 を実測値として持つと最安区分に倒れる
  const pick = (k) => {
    if (!(k in req.body)) return cur[k];
    const v = req.body[k];
    if (v === '' || v === null) return null;
    const n = Number(v);
    if (!Number.isFinite(n) || n <= 0) throw new Error(`${k} は 0 より大きい数値で入れてください (未計測なら空欄)`);
    return n;
  };
  try {
    const tare = pick('tare_weight_g');
    const L = pick('outer_length_mm');
    const W = pick('outer_width_mm');
    // 外寸が両方入ったときだけ「測った」と見なす。片方だけでは判定に使えない
    const verified = (L !== null && W !== null) ? 1 : 0;
    db.prepare(`UPDATE pm_materials SET tare_weight_g=?, outer_length_mm=?, outer_width_mm=?,
      dims_verified=?, note=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now'), updated_by=?
      WHERE material_code=?`)
      .run(tare, L, W, verified, req.body.note ?? cur.note, actorOf(req), code);
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

/** 商品1件の重さ・厚みを画面から埋める (Excel を開かずにその場で直せるように)。 */
router.post('/api/skus/:sku', (req, res) => {
  const sku = String(req.params.sku || '').normalize('NFKC').trim().toLowerCase();
  if (!sku) return res.status(400).json({ ok: false, error: '商品コードが空です' });
  const db = getDB();
  // 0 は許さない (欠測は空欄)。0g・0mm の実物は無く、入力ミスが最安区分として確定してしまう
  const num = (v) => {
    if (v === '' || v === null || v === undefined) return null;
    const n = Number(v);
    if (!Number.isFinite(n) || n <= 0) throw new Error('0 より大きい数値で入れてください (未計測なら空欄)');
    return n;
  };
  try {
    const w = num(req.body.unit_weight_g);
    const t = num(req.body.thickness_mm);
    const m = req.body.default_material_code || null;
    if (m && !db.prepare('SELECT 1 FROM pm_materials WHERE material_code=?').get(m)) {
      return res.status(400).json({ ok: false, error: '資材コードが不正です' });
    }
    db.prepare(`
      INSERT INTO pm_skus (sku_code, display_name, unit_weight_g, thickness_mm, default_material_code,
                           material_source, weight_source, updated_at, updated_by)
      VALUES (?,?,?,?,?, CASE WHEN ? IS NULL THEN NULL ELSE 'explicit' END, 'measured',
              strftime('%Y-%m-%dT%H:%M:%SZ','now'), ?)
      ON CONFLICT(sku_code) DO UPDATE SET
        unit_weight_g         = COALESCE(excluded.unit_weight_g,         pm_skus.unit_weight_g),
        thickness_mm          = COALESCE(excluded.thickness_mm,          pm_skus.thickness_mm),
        default_material_code = COALESCE(excluded.default_material_code, pm_skus.default_material_code),
        display_name          = COALESCE(pm_skus.display_name,           excluded.display_name),
        updated_at = excluded.updated_at, updated_by = excluded.updated_by
    `).run(sku, req.body.display_name || null, w, t, m, m, actorOf(req));
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

/** 境界マージン。運用が落ち着くまで触れるようにしておく (率ではなくグラム/ミリ)。 */
router.post('/api/settings', (req, res) => {
  const db = getDB();
  const allowed = { boundary_margin_g: [0, 100], thickness_margin_mm: [0, 20] };
  try {
    for (const [k, v] of Object.entries(req.body || {})) {
      if (!(k in allowed)) continue;
      const n = Number(v);
      const [lo, hi] = allowed[k];
      if (!Number.isFinite(n) || n < lo || n > hi) throw new Error(`${k} は ${lo}〜${hi} で入れてください`);
      db.prepare(`UPDATE pm_settings SET value=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now'), updated_by=? WHERE key=?`)
        .run(String(n), actorOf(req), k);
    }
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

/** 単票の試算 (デバッグ・確認用)。印字はまだしない。 */
router.post('/api/judge', async (req, res) => {
  const { judge } = await import('./engine.js');
  const { buildContext } = await import('./coverage.js');
  const lines = Array.isArray(req.body?.lines) ? req.body.lines : [];
  const ctx = buildContext(req.body?.date);
  res.json({ ok: true, result: judge({ lines }, ctx) });
});

export default router;
