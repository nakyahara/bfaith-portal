/**
 * api-router.mjs — 梱包指示の配りの口 (Render → miniPC の梱包アプリ・中原さん 2026-10-23・Company DB の 0101・lib/packing-guide.mjs)
 *
 * 正 = Render の Company DB。miniPC の梱包アプリ (apps/packing/guide-sync.js) がここから取りに来て、読むだけの写しを持つ
 * (取れなくても前の写しで出し続ける)。人が書くのは商品の画面 (apps/master-edit) と、梱包の画面のカメラ (2 段目・miniPC のサーバーが代わりに送る)。
 *
 * 載せ方 (server.js): Render (PORTAL_VARIANT = render) のときだけ。セッションの外 (miniPC のサーバーが鍵で来る) =
 *   app.use('/apps/packing-guide-api', router)。共通の JSON の読み取りは通さない (鍵を確かめてから読む)
 * 鍵: env PACKING_GUIDE_KEY (この口だけの鍵・miniPC の梱包アプリの .env と同じ値)。ヘッダ x-packing-guide-key。未設定 = 503 (fail-closed)
 * 接続: env COMPANY_DB_MASTER_EDIT_URL (商品の画面と同じロール master_edit = 梱包指示の表の select だけ使う)。無い = 503
 *   GET /manifest         指示のある単品の全部 { ok, generated_at, guides: [{ code, sku_id, name, note, event_id, updated_at, updated_by, photos: [{ photo_id, sha256, position, caption, width, height }] }] }
 *   GET /photos/:id       写真の中身 (JPEG)。番号ごとに中身は変わらない
 */
import express from 'express';
import crypto from 'node:crypto';
import { openPgClient, pgAdapter } from '../../scripts/company-db/migrate.mjs';
import * as PGD from '../../lib/packing-guide.mjs';

const router = express.Router();
let pgClientFactory = openPgClient;
export function __setPgClientFactory(fn) { pgClientFactory = fn || openPgClient; }

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  return ab.length === bb.length && crypto.timingSafeEqual(ab, bb);
}
router.use((req, res, next) => {
  const expected = process.env.PACKING_GUIDE_KEY;
  if (!expected) return res.status(503).json({ ok: false, error: 'PACKING_GUIDE_KEY が未設定なので配れません (fail-closed)' });
  const got = req.headers['x-packing-guide-key'];
  if (typeof got !== 'string' || !timingSafeEq(got, expected)) return res.status(401).json({ ok: false, error: 'x-packing-guide-key が違います' });
  res.set('Cache-Control', 'no-store');
  next();
});

async function withDb(res, fn) {
  const url = process.env.COMPANY_DB_MASTER_EDIT_URL;
  if (!url) return res.status(503).json({ ok: false, error: 'COMPANY_DB_MASTER_EDIT_URL が未設定です' });
  let client;
  try {
    client = await pgClientFactory(url, { application_name: 'packing-guide-api', connectionTimeoutMillis: 10_000 });
    if (client.on) client.on('error', (e) => console.error(`[packing-guide-api] 接続のエラー: ${e.message}`));
    await client.query(`set statement_timeout = '20s'`);
    const db = pgAdapter(client);
    if (!(await PGD.packingGuideReady(db))) return res.status(503).json({ ok: false, error: 'Company DB の準備 (0101・ロールの流し直し) がまだです', reason: 'db_not_ready' });
    await fn(db);
  } catch (e) {
    console.error(`[packing-guide-api] ${e && e.stack || e}`);
    if (!res.headersSent) res.status(503).json({ ok: false, error: 'Company DB を読めませんでした' });
  } finally {
    if (client) { try { await client.end(); } catch { /* */ } }
  }
}

router.get('/manifest', (req, res) => withDb(res, async (db) => {
  res.json({ ok: true, generated_at: new Date().toISOString(), guides: await PGD.listGuidesForExport(db) });
}));

router.get('/photos/:id', (req, res) => withDb(res, async (db) => {
  const p = await PGD.readPackingPhoto(db, req.params.id);
  if (!p) return res.status(404).json({ ok: false, error: '写真がありません' });
  res.set('X-Content-Sha256', p.sha256);
  res.type('image/jpeg').send(p.image);
}));

export default router;
