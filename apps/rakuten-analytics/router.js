/**
 * rakuten-analytics router — 楽天分析ツール (売上・広告・利益・検索順位の統合管理)
 *
 * 要件定義: g:/共有ドライブ/AI_reference/システム設計/楽天統合管理ダッシュボード_要件定義_20260706.md
 *
 * 設計原則 (§9):
 *   - Render 完結: warehouse-mirror.db 読み取りのみ (集計 SQL は queries.js に分離)
 *   - 精度ラベル: 速報 (受注ベース) / 推定 (mall_fee 10%一律) / 確定 (月次仕訳書) を API が明示
 *   - API は /api/v1/ prefix (将来拡張用)
 *   - 認可は server.js 側 requireAppAccess('rakuten-analytics')
 *   - 既存アプリ (rakuten-accounting 等) とは独立。mirror の読み取り参照のみ (§9-4)
 *
 * P1 スコープ: アプリ骨格 + 概要タブ (タイル4枚 + トレンド)。
 * 取込 (P2) / 広告 (P3) / 利益分析 (P4) / 売れ筋 (P5) / 検索順位 (P6) / 診断・設定 (P7) は準備中タブ。
 */
import { Router } from 'express';
import multer from 'multer';
import { resolvePeriod, getOverview, getTrend } from './queries.js';
import { importFiles, getImportStatus, getImportLog } from './import.js';

const router = Router();

// CSV/zip アップロード (メモリ保持。RPP日別CSVは数MB想定、zipは30MBまで)。
// files=4 でリクエストあたりの最大メモリを 120MB に制限 (Codex R2 High — memoryStorage の
// 総量はストリーム中に止められないため件数側で抑える)。UI が 4件ずつ自動分割送信する
const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 30 * 1024 * 1024, files: 4 },
});

// 500 を JSON で返す共通 wrapper (API は画面から fetch されるため HTML error page を返さない)
// エラー詳細 (SQL/テーブル名等) はログのみ。クライアントには固定文言 (Codex R1 Medium)
function api(handler) {
  return (req, res) => {
    try {
      res.json(handler(req));
    } catch (e) {
      console.error(`[rakuten-analytics] ${req.method} ${req.originalUrl}: ${e.stack || e.message}`);
      res.status(500).json({ error: 'サーバー内部エラー (詳細はサーバーログ参照)' });
    }
  };
}

router.get('/', (req, res) => {
  res.render('rakuten-analytics', {
    username: req.session?.email,
    displayName: req.session?.displayName,
  });
});

// ─── 概要 ───
router.get('/api/v1/overview', api(() => getOverview()));

router.get('/api/v1/trend', api((req) => {
  const { from, to } = resolvePeriod(req.query.preset, req.query.from, req.query.to);
  const granularity = ['day', 'week', 'month'].includes(req.query.granularity) ? req.query.granularity : 'day';
  return getTrend(from, to, granularity);
}));

// ─── 取込 (P2): RPPレポートCSV/zip アップロード ───
router.post('/api/v1/import', (req, res) => {
  upload.array('files', 4)(req, res, (err) => {
    if (err) {
      const msg = err.code === 'LIMIT_FILE_SIZE' ? 'ファイルが大きすぎます (30MBまで)'
        : err.code === 'LIMIT_FILE_COUNT' || err.code === 'LIMIT_UNEXPECTED_FILE' ? '1回のアップロードは4ファイルまでです (画面からは自動分割されます)'
        : `アップロード失敗: ${err.message}`;
      return res.status(400).json({ error: msg });
    }
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({ error: 'ファイルを選択してください (CSV または zip)' });
    }
    // リクエスト総量ガード (memoryStorage のため。files=4 × 30MB = 120MB と整合)
    const totalBytes = req.files.reduce((s, f) => s + f.buffer.length, 0);
    if (totalBytes > 120 * 1024 * 1024) {
      return res.status(400).json({ error: '合計サイズが大きすぎます (120MBまで)。分けてアップロードしてください' });
    }
    try {
      res.json(importFiles(req.files, req.session?.email || ''));
    } catch (e) {
      console.error(`[rakuten-analytics] POST /api/v1/import: ${e.stack || e.message}`);
      res.status(500).json({ error: 'サーバー内部エラー (詳細はサーバーログ参照)' });
    }
  });
});

router.get('/api/v1/import/status', api(() => getImportStatus()));
router.get('/api/v1/import/log', api((req) => ({ items: getImportLog(req.query.limit) })));

export default router;
