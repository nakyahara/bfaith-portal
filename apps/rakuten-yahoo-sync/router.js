/**
 * RakutenYahooSync (RYS) — bfaith-portal app router (Phase E-1 skeleton)
 *
 * 設計原則 (Codex Phase E R3/R4 確定):
 *   - 楽天 RMS は miniPC proxy 経由 (mercari-sync 同型、 Render に楽天キーを置かない)
 *   - Notion は Render 直接 (RYS_NOTION_TOKEN、 公開 API)
 *   - Yahoo OAuth は既存 vps-proxy 経由
 *   - secret 値は UI / DB / log に出さない (set?:true/false + length のみ)
 *   - RYS state は専用 SQLite (E-2 以降)、 warehouse-mirror.db は read-only 参照
 *   - 実 publish は RYS_PUBLISH_ENABLED=0 default の kill-switch で gate
 *
 * E-1 (本ファイル): skeleton + env health dashboard のみ
 *   - GET /            ダッシュボード (env 充足状況 + 次の step ナビ)
 *   - GET /api/health  env 充足チェック (JSON)
 */

import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { inspectEnvStatus } from './env-check.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

function renderView(res, viewName, data = {}) {
  res.render(path.join(__dirname, 'views', viewName), data);
}

// ───────────────── 画面 ─────────────────

router.get('/', (_req, res) => {
  const status = inspectEnvStatus();
  renderView(res, 'dashboard', {
    status,
    phase: 'E-1 (skeleton)',
  });
});

// ───────────────── API ─────────────────

router.get('/api/health', (_req, res) => {
  res.json(inspectEnvStatus());
});

export default router;
