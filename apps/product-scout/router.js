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
  getConcept, recordDecision, countMatching, REASON_CODES,
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
    // ⚠️詳細 (SQLite の表名やスタック) は返さない。サーバログにだけ残す
    console.error('[product-scout] テーブル初期化エラー:', productScoutInitError);
    return res.status(503).json({ error: 'テーブル初期化に失敗しています (ログを確認してください)' });
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
  // ⚠️共有DB (他20アプリが使う) に1トランザクションで書き込むので、
  //   異常な大きさのものを受けるとその間ほかのアプリの書き込みを塞ぐ。
  //   バッチ不具合や鍵の誤設定で巻き添えにしないよう、入口で桁を確かめる。
  const MAX_CONCEPTS = 20000;   // 実測 1,988件。1桁の余裕を持たせた上限
  const MAX_CATEGORIES = 200;
  if (payload.concepts.length > MAX_CONCEPTS) {
    return res.status(413).json({ error: `テーマが多すぎます (${payload.concepts.length} > ${MAX_CONCEPTS})` });
  }
  if (Array.isArray(payload.collection) && payload.collection.length > MAX_CATEGORIES) {
    return res.status(413).json({ error: `カテゴリが多すぎます (${payload.collection.length} > ${MAX_CATEGORIES})` });
  }
  if (!payload.generatedAt || Number.isNaN(Date.parse(payload.generatedAt))) {
    return res.status(400).json({ error: 'generatedAt が日時として読めません' });
  }
  if (payload.lastProgressAt && Number.isNaN(Date.parse(payload.lastProgressAt))) {
    // 画面の信号がこの値で判定するので、読めない値を黙って入れさせない
    return res.status(400).json({ error: 'lastProgressAt が日時として読めません' });
  }
  const badConcept = payload.concepts.find((c) => !c || typeof c.categoryPath !== 'string'
    || typeof c.form !== 'string' || !c.categoryPath || !c.form);
  if (badConcept) {
    return res.status(400).json({ error: 'categoryPath と form は必須です (テーマの安定キーに使うため)' });
  }
  try {
    const r = ingestSnapshot(payload);
    console.log(`[product-scout] 取り込み ${r.concepts}テーマ / ${r.categories}カテゴリ (${r.snapshotId})`);
    res.json({ ok: true, ...r });
  } catch (e) {
    // 未認証でも到達しうる経路なので、内部の詳細は返さず追跡IDだけ返す
    const traceId = crypto.randomBytes(4).toString('hex');
    console.error('[product-scout] 取り込み失敗 (' + traceId + '):', e.message);
    res.status(500).json({ error: '取り込みに失敗しました', traceId });
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
  const progressMs = snapshot.last_progress_at ? Date.parse(snapshot.last_progress_at) : NaN;
  const progressH = Number.isFinite(progressMs) ? (Date.now() - progressMs) / 3600000 : null;

  if (ageH > 48) {
    return { level: 'red', title: 'データが更新されていません',
      detail: `最後の取り込みは ${Math.floor(ageH / 24)}日前。miniPC の毎日14:00タスクが動いているか確認してください` };
  }
  // ⚠️前進時刻が無い・読めないときに緑へ抜けさせない。
  //   「判定できない」を「問題なし」にするのは、まさに20日間の空回りを隠した思考なので、
  //   分からないなら黄色にして人に見に行かせる。
  if (remaining > 0 && progressH === null) {
    return { level: 'yellow', title: '進んでいるか判定できません',
      detail: `残り${remaining.toLocaleString()}件ありますが、最終前進時刻が取り込まれていません (バッチが古い可能性)` };
  }
  if (remaining > 0 && progressH > 48) {
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

const PAGE_SIZE = 40;

router.get('/', guardTables, (req, res) => {
  const snapshot = getLatestSnapshot();
  const snapshotId = snapshot ? snapshot.snapshot_id : null;
  const categories = snapshot ? listCategories(snapshotId) : [];
  const counts = countConcepts(snapshotId);
  const gate = ['pass', 'unknown', 'fail', 'all'].includes(req.query.gate) ? req.query.gate : 'pass';
  const status = ['undecided', 'adopt', 'reject', 'hold', 'all'].includes(req.query.status)
    ? req.query.status : 'undecided';
  // 約2,000テーマあるので、上位だけ見て終わりにできない。ページで送れるようにする
  const page = Math.max(1, Math.min(500, Number(req.query.page) || 1));
  const offset = (page - 1) * PAGE_SIZE;
  const total = countMatching({ snapshotId, gate, status });
  const concepts = listConcepts({ snapshotId, gate, status, limit: PAGE_SIZE, offset });

  res.render(path.join(__dirname, 'views/index'), {
    username: req.session?.email,
    displayName: req.session?.displayName,
    snapshot, categories, counts, concepts, gate, status,
    page, pageSize: PAGE_SIZE, total, totalPages: Math.max(1, Math.ceil(total / PAGE_SIZE)),
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
      // 判断者が取れないなら記録しない (db.js が401にする)。
      // 'unknown' で通すと、認証まわりが変わったときに監査性が静かに失われる
      decidedBy: req.session?.email || null,
    });
    res.json({ ok: true, ...r });
  } catch (e) {
    res.status(e.status || 500).json({ error: e.message });
  }
});

export default router;
