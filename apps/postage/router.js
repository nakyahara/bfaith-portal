/**
 * 郵便料金判定 (postage) — 画面 + API
 *
 * 役割は「マスタを育てる」こと + 判定ログを見せること。判定 API 本体は judge-router.js。
 * 画面は3つ:
 *   /            カバー率 — いま何割が確定できるか / 次に何を測れば効くか / 取り込んだ表のどこが壊れているか
 *   /skus        商品マスタ — 商品ごとに重さ・厚み・資材を登録する。入れたその場で判定を見せる
 *   /decisions   判定ログ — ランチャーに返した印字文言。不明 (人が測るもの) を上に出す
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
import {
  coverageReport, warehouseAvailable, availableSources, SOURCE_LABELS, buildContext, lookupProductName,
} from './coverage.js';
import { searchSkus, countByStatus, previewOne, skuStatus, STATUS_LABELS, FILTER_LABELS } from './skus.js';
import { listDecisions, STATUS_LABELS as DECISION_STATUS_LABELS } from './judge-service.js';
import { mirrorAvailable } from './composition.js';

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
// 実在する YYYY-MM-DD だけ受ける。壊れた値 (?since=xxxxxxxxxx) で日付計算が RangeError にならないように既定へ戻す
function ymdOr(v, fallback) {
  const s = String(v || '').slice(0, 10);
  if (!/^\d{4}-\d{2}-\d{2}$/.test(s)) return fallback;
  const t = Date.parse(`${s}T00:00:00Z`);
  return Number.isFinite(t) && new Date(t).toISOString().slice(0, 10) === s ? s : fallback;
}

// 出荷実績の出どころ。指定が無ければ自動 (warehouse があればそれ、無ければ packing-dispatch)
function sourceOf(req) {
  const s = String(req.query.source || 'auto');
  return Object.prototype.hasOwnProperty.call(SOURCE_LABELS, s) ? s : 'auto';
}

// ─────────────────────────── 画面 ───────────────────────────
router.get('/', (req, res) => {
  const db = getDB();
  const since = ymdOr(req.query.since, defaultSince());
  const until = ymdOr(req.query.until, null);
  const source = sourceOf(req);

  const report = coverageReport({ since, until, source });
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
    since, until: until || '', source, sources: availableSources(), sourceLabels: SOURCE_LABELS,
    warehouseOk: warehouseAvailable(), mirrorOk: mirrorAvailable(),
    username: req.session?.email, displayName: req.session?.displayName,
  });
});

// 不足リストの CSV (現場に配って埋めてもらうため)
router.get('/missing.csv', (req, res) => {
  const since = ymdOr(req.query.since, defaultSince());
  const report = coverageReport({ since, source: sourceOf(req) });
  if (!report.available) return res.status(503).send('出荷実績 (warehouse.db / packing-dispatch) が読めません');
  const esc = (v) => `"${String(v ?? '').replace(/"/g, '""')}"`;
  const lines = ['優先,出現通数,商品コード,商品名,足りないもの'];
  report.missingSkus.forEach((m, i) => {
    lines.push([i + 1, m.count, esc(m.sku_code), esc(m.name || ''), esc(m.needs)].join(','));
  });
  res.setHeader('Content-Type', 'text/csv; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="postage_missing_${jstToday()}.csv"`);
  res.send('﻿' + lines.join('\r\n'));   // Excel で開くので BOM をつける
});

// 商品マスタ — 商品ごとに重さ・厚み・資材を登録する画面。
// 一括取込 (Excel) では届かないもの (新商品・打ち間違いの修正) をここで入れる。
router.get('/skus', (req, res) => {
  const db = getDB();
  const q = String(req.query.q || '').slice(0, 100);
  const filter = Object.prototype.hasOwnProperty.call(FILTER_LABELS, req.query.filter) ? req.query.filter : 'incomplete';
  const limit = 50;
  // 1.5 や Infinity が素通りすると、表示上のページ番号と取得位置がズレる
  const nPage = Number(req.query.page);
  let page = Number.isFinite(nPage) && nPage >= 1 ? Math.floor(nPage) : 1;
  const { total: preTotal } = searchSkus({ q, filter, limit: 1, offset: 0 });
  const pages = Math.max(Math.ceil(preTotal / limit), 1);
  if (page > pages) page = pages;
  const { rows, total } = searchSkus({ q, filter, limit, offset: (page - 1) * limit });

  // 判定プレビューは全行ぶん作るのでマスタは1回だけ読む
  const ctx = buildContext();
  const list = rows.map((r) => ({ ...r, status: skuStatus(r), preview: previewOne(r.sku_code, ctx) }));

  res.render(view('skus.ejs'), {
    list, total, page, limit, q, filter,
    counts: countByStatus(),
    // 新規登録の選択肢は使用中の資材だけ。ただし一覧の各行では
    // 「いまその商品に設定されている資材」も選択肢に入れないと、
    // 使用停止にした資材の商品を保存したときに資材が黙って消える
    materials: db.prepare('SELECT * FROM pm_materials WHERE active=1 ORDER BY material_code').all(),
    materialNames: Object.fromEntries(
      db.prepare('SELECT material_code, display_name, active FROM pm_materials').all()
        .map((m) => [m.material_code, { name: m.display_name, active: !!m.active }]),
    ),
    statusLabels: STATUS_LABELS, filterLabels: FILTER_LABELS,
    warehouseOk: warehouseAvailable() || mirrorAvailable(),
    username: req.session?.email, displayName: req.session?.displayName,
  });
});

// 判定ログ — ランチャーが送ってきた伝票に何を返したか (印字した文言そのもの)。
// 「不明」を上に出す: 集荷前に人が測って訂正する対象がこれ
router.get('/decisions', (req, res) => {
  const date = /^\d{4}-\d{2}-\d{2}$/.test(String(req.query.date || '')) ? String(req.query.date) : jstToday();
  const data = listDecisions({ date });
  res.render(view('decisions.ejs'), {
    ...data, statusLabels: DECISION_STATUS_LABELS,
    judgeKeySet: !!process.env.POSTAGE_JUDGE_KEY, mirrorOk: mirrorAvailable(),
    username: req.session?.email, displayName: req.session?.displayName,
  });
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
    const T = pick('thickness_mm');
    if (T !== null && T > 100) throw new Error('厚みが大きすぎます (mm で入れてください。封筒なら 1〜2)');
    // 外寸が両方入ったときだけ「測った」と見なす。片方だけでは判定に使えない
    const verified = (L !== null && W !== null) ? 1 : 0;
    db.prepare(`UPDATE pm_materials SET tare_weight_g=?, outer_length_mm=?, outer_width_mm=?, thickness_mm=?,
      dims_verified=?, note=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now'), updated_by=?
      WHERE material_code=?`)
      .run(tare, L, W, T, verified, req.body.note ?? cur.note, actorOf(req), code);
    res.json({ ok: true });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

/**
 * 商品1件の登録・更新。
 *
 * mode='fill' (既定): 送った値だけ入れ、空欄は既存を残す。
 *   カバー率画面の「その場で入れる」用。うっかり消さないため。
 * mode='replace': 送った内容をそのまま反映する。空欄は **消す**。
 *   商品マスタ画面の編集用。打ち間違いを直せないと使い物にならない。
 */
router.post('/api/skus/:sku', (req, res) => {
  const sku = String(req.params.sku || '').normalize('NFKC').trim().toLowerCase();
  if (!sku) return res.status(400).json({ ok: false, error: '商品コードが空です' });
  if (sku.length > 120) return res.status(400).json({ ok: false, error: '商品コードが長すぎます' });
  const db = getDB();
  const replace = String(req.body.mode || 'fill') === 'replace';
  // 0 は許さない (欠測は空欄)。0g・0mm の実物は無く、入力ミスが最安区分として確定してしまう
  const num = (v, label) => {
    if (v === '' || v === null || v === undefined) return null;
    const n = Number(v);
    if (!Number.isFinite(n) || n <= 0) throw new Error(`${label}は 0 より大きい数値で入れてください (未計測なら空欄)`);
    return n;
  };
  try {
    const w = num(req.body.unit_weight_g, '重さ');
    const t = num(req.body.thickness_mm, '厚み');
    const m = req.body.default_material_code || null;
    const name = typeof req.body.display_name === 'string' ? req.body.display_name.trim().slice(0, 300) : null;
    if (m && !db.prepare('SELECT 1 FROM pm_materials WHERE material_code=?').get(m)) {
      return res.status(400).json({ ok: false, error: '資材コードが不正です' });
    }
    const merge = replace
      ? `unit_weight_g = excluded.unit_weight_g,
         thickness_mm  = excluded.thickness_mm,
         default_material_code = excluded.default_material_code,
         material_source = CASE WHEN excluded.default_material_code IS NULL THEN NULL ELSE 'explicit' END,
         weight_source   = CASE WHEN excluded.unit_weight_g IS NULL THEN NULL ELSE 'measured' END,
         display_name  = COALESCE(excluded.display_name, pm_skus.display_name)`
      : `unit_weight_g = COALESCE(excluded.unit_weight_g, pm_skus.unit_weight_g),
         thickness_mm  = COALESCE(excluded.thickness_mm,  pm_skus.thickness_mm),
         default_material_code = COALESCE(excluded.default_material_code, pm_skus.default_material_code),
         material_source = COALESCE(pm_skus.material_source,
                                    CASE WHEN excluded.default_material_code IS NULL THEN NULL ELSE 'explicit' END),
         weight_source   = CASE WHEN COALESCE(excluded.unit_weight_g, pm_skus.unit_weight_g) IS NULL
                                THEN NULL ELSE COALESCE(pm_skus.weight_source, 'measured') END,
         display_name  = COALESCE(pm_skus.display_name, excluded.display_name)`;
    db.prepare(`
      INSERT INTO pm_skus (sku_code, display_name, unit_weight_g, thickness_mm, default_material_code,
                           material_source, weight_source, updated_at, updated_by)
      VALUES (@sku, @name, @w, @t, @m,
              CASE WHEN @m IS NULL THEN NULL ELSE 'explicit' END,
              CASE WHEN @w IS NULL THEN NULL ELSE 'measured' END,
              strftime('%Y-%m-%dT%H:%M:%SZ','now'), @actor)
      ON CONFLICT(sku_code) DO UPDATE SET
        ${merge},
        updated_at = excluded.updated_at, updated_by = excluded.updated_by
    `).run({ sku, name, w, t, m, actor: actorOf(req) });
    // 入れた値が効いているかをその場で見せる (1個だけ送ったらいくらか)
    res.json({ ok: true, preview: previewOne(sku, buildContext()) });
  } catch (e) {
    res.status(400).json({ ok: false, error: e.message });
  }
});

/** 資材の新規追加。実物が出てきたとき (厚紙封など) にここから足す。 */
router.post('/api/materials', (req, res) => {
  const db = getDB();
  // 配列やオブジェクトが String() で "a,b" / "[object Object]" として通らないよう、型を要求する
  const str = (v, label, max) => {
    if (v === undefined || v === null || v === '') return '';
    if (typeof v !== 'string') throw new Error(`${label}は文字で入れてください`);
    const t = v.normalize('NFKC').trim();
    if (t.length > max) throw new Error(`${label}は ${max} 文字以内にしてください`);
    return t;
  };
  try {
    const code = str(req.body.material_code, '資材コード', 40).toLowerCase();
    const name = str(req.body.display_name, '資材の名前', 100);
    if (!/^[a-z0-9_]{2,40}$/.test(code)) {
      return res.status(400).json({ ok: false, error: '資材コードは半角英小文字・数字・_ の 2〜40 文字にしてください' });
    }
    if (!name) return res.status(400).json({ ok: false, error: '資材の名前を入れてください' });

    // 桁を1つ間違えた値を黙って受けない (封筒の自重が 5000g になることは無い)
    const num = (v, label, max) => {
      if (v === '' || v === null || v === undefined) return null;
      if (typeof v !== 'string' && typeof v !== 'number') throw new Error(`${label}は数値で入れてください`);
      const n = Number(v);
      if (!Number.isFinite(n) || n <= 0) throw new Error(`${label}は 0 より大きい数値で入れてください (未計測なら空欄)`);
      if (n > max) throw new Error(`${label}が大きすぎます (${max} 以下で入れてください)`);
      return n;
    };
    const tare = num(req.body.tare_weight_g, '自重', 4000);          // 定形外の上限が 4kg
    const L = num(req.body.outer_length_mm, '長辺', 1000);           // 規格外の長辺上限が 600mm
    const W = num(req.body.outer_width_mm, '短辺', 1000);
    const T = num(req.body.thickness_mm, '厚み', 100);               // 資材そのものの厚み (封筒 1〜2mm)
    db.prepare(`INSERT INTO pm_materials
      (material_code, display_name, tare_weight_g, outer_length_mm, outer_width_mm, thickness_mm, dims_verified, note, updated_at, updated_by)
      VALUES (?,?,?,?,?,?,?,?, strftime('%Y-%m-%dT%H:%M:%SZ','now'), ?)`)
      .run(code, name, tare, L, W, T, (L !== null && W !== null) ? 1 : 0,
        str(req.body.note, 'メモ', 300) || null, actorOf(req));
    res.json({ ok: true });
  } catch (e) {
    // 同じコードを同時に足したときは 400 ではなく 409 で返す
    if (/UNIQUE constraint failed|PRIMARY KEY/i.test(e.message)) {
      return res.status(409).json({ ok: false, error: 'その資材コードは既にあります' });
    }
    res.status(400).json({ ok: false, error: e.message });
  }
});

/** 商品名の補完 (過去の出荷実績から引く)。無ければ手で入れてもらう。 */
router.get('/api/skus/:sku/lookup', (req, res) => {
  const sku = String(req.params.sku || '').normalize('NFKC').trim();
  res.json({ ok: true, display_name: sku ? lookupProductName(sku) : null });
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

/**
 * 1通あたりの固定加算 (送り状シールなど) の重さ。
 * 2026-08-30 実測 0.5g で種を入れたが、2026-09-05 に中原さんが 2g と確定 → 画面から直せるようにした。
 * 0 は許す (加算しない、という明示)。50g を超える固定加算は入力ミス
 */
router.post('/api/overheads/:code', (req, res) => {
  const db = getDB();
  const cur = db.prepare('SELECT * FROM pm_overheads WHERE code=?').get(req.params.code);
  if (!cur) return res.status(404).json({ ok: false, error: '固定加算が見つかりません' });
  const v = req.body?.weight_g;
  const n = Number(v);
  if (v === '' || v === null || v === undefined || (typeof v !== 'string' && typeof v !== 'number') || !Number.isFinite(n) || n < 0 || n > 50) {
    return res.status(400).json({ ok: false, error: '重さは 0〜50 g で入れてください' });
  }
  db.prepare(`UPDATE pm_overheads SET weight_g=?, updated_at=strftime('%Y-%m-%dT%H:%M:%SZ','now'), updated_by=? WHERE code=?`)
    .run(n, actorOf(req), req.params.code);
  res.json({ ok: true, weight_g: n });
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
