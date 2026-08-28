/**
 * 新商品企画スカウト (apps/product-scout) router
 *
 * URL:
 *   GET  /apps/product-scout/                     — 1画面 (信号 + 工程表 + テーマ一覧)
 *   GET  /apps/product-scout/concepts/:id         — テーマ1件 (JSON。画面が詳細を開くのに使う)
 *   POST /apps/product-scout/concepts/:id/decision— 採否を記録 (追記のみ)
 *   POST /apps/product-scout/ingest               — miniPC からの取り込み (x-sync-key)
 *
 * 認証: 画面と採否は server.js の requireAppAccess('product-scout') (社内ログイン)。
 *       ingest だけはバッチが叩くので MIRROR_SYNC_KEY で認証する (セッションを持てないため)。
 *
 * ⭐設計方針は AI_reference『新商品企画スカウト_出口設計_20260828.md』を参照。
 *   要点: 正本はここ1箇所 / 採否は追記のみ / 進捗は「100%」でなく「下限%」で出す。
 */
import { Router } from 'express';
import express from 'express';
import path from 'path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'url';
import {
  ingestSnapshot, getLatestSnapshot, listCategories, listConcepts, countConcepts,
  getConcept, recordDecision, REASON_CODES,
} from './db.js';
import { productScoutInitError } from '../warehouse-mirror/db.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();
// ingest は社内ログインを掛けない別 mount にするため、独立した Router にする
// (メインの router を /apps/product-scout/ingest に mount するとパスが二重になって当たらない)
export const ingestRouter = Router();

router.use('/public', express.static(path.join(__dirname, 'public'), { maxAge: '1h', index: false }));

/** テーブルの初期化に失敗していたら、壊れた画面を出さずに理由を見せる */
function guardTables(req, res, next) {
  if (productScoutInitError) {
    return res.status(503).json({ error: 'テーブル初期化に失敗しています', detail: productScoutInitError });
  }
  return next();
}

// ─────────────────────────────────────────────────────────────
// 取り込み (miniPC → Render)
// ─────────────────────────────────────────────────────────────
// ⚠️セッションを持てないバッチからの呼び出しなので、共有鍵で認証する。
//   鍵が未設定なら通さない (fail-closed)。素通りさせると誰でも画面の中身を差し替えられる。
ingestRouter.post('/', express.json({ limit: '32mb' }), guardTables, (req, res) => {
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) return res.status(503).json({ error: 'MIRROR_SYNC_KEY 未設定' });
  const got = String(req.headers['x-sync-key'] || '');
  const a = Buffer.from(got);
  const b = Buffer.from(key);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return res.status(401).json({ error: 'unauthorized' });
  }
  const payload = req.body;
  if (!payload || !Array.isArray(payload.concepts)) {
    return res.status(400).json({ error: 'concepts 配列が必要です' });
  }
  try {
    const r = ingestSnapshot(payload);
    console.log(`[product-scout] 取り込み ${r.concepts}テーマ / ${r.categories}カテゴリ (${r.snapshotId})`);
    res.json({ ok: true, ...r });
  } catch (e) {
    console.error('[product-scout] 取り込み失敗:', e.message);
    res.status(500).json({ error: e.message });
  }
});

// ─────────────────────────────────────────────────────────────
// 画面
// ─────────────────────────────────────────────────────────────

/**
 * 全体の信号を作る。
 * ⭐「ジョブが成功したか」ではなく「仕事が前に進んでいるか」で判定する。
 *   正常終了しているのに仕事が無い (= 次のカテゴリが未投入) を緑にしたせいで、
 *   2026-08-07〜27 の20日間、誰も止まっていることに気づかなかった。
 */
function buildSignal(snapshot, categories, counts) {
  if (!snapshot) {
    return { level: 'gray', title: 'まだ取り込みがありません',
      detail: 'miniPC で node concepts.js → node push.js を実行すると、ここに出ます' };
  }
  const notStarted = categories.filter((c) => c.state === 'not_started');
  const incomplete = categories.filter((c) => c.complete === 0);
  const remaining = categories.reduce((n, c) => n + (c.remaining || 0), 0);
  const ageH = (Date.now() - Date.parse(snapshot.ingested_at)) / 3600000;
  // ⚠️「残件があるか」だけで動いていると判断してはいけない。
  //   Keepa が返さない数件はいつまでも残るので、それを「収集中」と呼ぶと
  //   止まっていても永久に緑になる (直したはずの空回りが別の形で戻る)。
  //   実際に products.jsonl が伸びた時刻で見る。
  const progressH = snapshot.last_progress_at
    ? (Date.now() - Date.parse(snapshot.last_progress_at)) / 3600000 : null;

  if (ageH > 48) {
    return { level: 'red', title: 'データが更新されていません',
      detail: `最後の取り込みは ${Math.floor(ageH / 24)}日前。miniPC の毎日14:00タスクが動いているか確認してください` };
  }
  if (remaining > 0 && progressH !== null && progressH > 48) {
    return { level: 'red', title: '収集が止まっています',
      detail: `残り${remaining.toLocaleString()}件あるのに ${Math.floor(progressH / 24)}日間なにも取得できていません。products.log を確認してください` };
  }
  if (remaining === 0 && notStarted.length) {
    return { level: 'yellow', title: '収集する対象がありません (次のカテゴリ未投入)',
      detail: `未投入: ${notStarted.map((c) => c.name).join(' / ')}` };
  }
  if (remaining === 0 && !notStarted.length) {
    return { level: 'yellow', title: '全カテゴリ収集済み — 次の探索対象が決まっていません',
      detail: '1周が終わりました。config.json に次のカテゴリを足すか、探索方針を決め直してください' };
  }
  const active = categories.filter((c) => (c.remaining || 0) > 0).map((c) => c.name);
  return { level: 'green', title: `収集中: ${active.join(' / ')} (残り${remaining.toLocaleString()}件)`,
    detail: `審査待ち ${counts.undecidedPass}件` + (incomplete.length ? ` ／ ⚠️分母が不完全なカテゴリ ${incomplete.length}件` : '') };
}

router.get('/', guardTables, (req, res) => {
  const snapshot = getLatestSnapshot();
  const categories = snapshot ? listCategories(snapshot.snapshot_id) : [];
  const counts = countConcepts();
  const gate = ['pass', 'unknown', 'fail', 'all'].includes(req.query.gate) ? req.query.gate : 'pass';
  const status = ['undecided', 'adopt', 'reject', 'hold', 'all'].includes(req.query.status)
    ? req.query.status : 'undecided';
  const concepts = listConcepts({ gate, status, limit: 60 });

  res.render(path.join(__dirname, 'views/index'), {
    username: req.session?.email,
    displayName: req.session?.displayName,
    snapshot, categories, counts, concepts, gate, status,
    reasonCodes: REASON_CODES,
    signal: buildSignal(snapshot, categories, counts),
  });
});

router.get('/concepts/:id', guardTables, (req, res) => {
  const c = getConcept(String(req.params.id));
  if (!c) return res.status(404).json({ error: 'not found' });
  res.json(c);
});

router.post('/concepts/:id/decision', express.json({ limit: '64kb' }), guardTables, (req, res) => {
  try {
    const r = recordDecision({
      conceptId: String(req.params.id),
      decision: String(req.body?.decision || ''),
      reasonCode: req.body?.reasonCode || null,
      comment: req.body?.comment || null,
      recheckCondition: req.body?.recheckCondition || null,
      decidedBy: req.session?.email || 'unknown',
    });
    res.json({ ok: true, ...r });
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

export default router;
