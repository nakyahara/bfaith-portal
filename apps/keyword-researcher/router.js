/**
 * Amazon キーワードリサーチャー — Express Router
 * サジェスト取得 + 広告API連携（キーワード推奨・入札額）
 */
import { Router } from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { getSuggestions } from './suggest.js';
import {
  getKeywordRecommendations,
  getThemeRecommendations,
  testConnection,
  isConfigured,
} from './ads-api.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = Router();

// ── ページ配信 ──
router.get('/', (req, res) => {
  res.sendFile(path.join(__dirname, 'views', 'index.html'));
});

// ── API: サジェスト取得 ──
router.post('/api/suggest', async (req, res) => {
  try {
    const { seed, hiragana = true, alphabet = false, depth = 1, delayMs = 200 } = req.body;
    if (!seed || typeof seed !== 'string') {
      return res.status(400).json({ error: 'seed（キーワード）は必須です' });
    }

    const result = await getSuggestions(seed.trim(), {
      hiragana,
      alphabet,
      depth: Math.min(depth, 2),
      delayMs: Math.max(delayMs, 100),
    });

    res.json(result);
  } catch (err) {
    console.error('[KeywordResearcher] サジェスト取得エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: ASINからキーワード推奨取得 ──
router.post('/api/keyword-recommendations', async (req, res) => {
  try {
    if (!isConfigured()) {
      return res.status(400).json({ error: 'Amazon Ads APIが未設定です。/ads-auth/start で認証してください。' });
    }

    const { asins, maxRecommendations = 100 } = req.body;
    if (!Array.isArray(asins) || asins.length === 0) {
      return res.status(400).json({ error: 'asins（ASIN配列）は必須です' });
    }

    const keywords = await getKeywordRecommendations(asins, maxRecommendations);

    res.json({
      asins,
      total: keywords.length,
      keywords,
    });
  } catch (err) {
    console.error('[KeywordResearcher] キーワード推奨取得エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: ASINからテーマベース推奨取得（入札額付き） ──
router.post('/api/theme-recommendations', async (req, res) => {
  try {
    if (!isConfigured()) {
      return res.status(400).json({ error: 'Amazon Ads APIが未設定です。' });
    }

    const { asins } = req.body;
    if (!Array.isArray(asins) || asins.length === 0) {
      return res.status(400).json({ error: 'asins（ASIN配列）は必須です' });
    }

    const themes = await getThemeRecommendations(asins);

    // テーマからフラットなキーワードリストに展開
    const keywords = [];
    for (const theme of themes) {
      for (const kw of theme.keywords) {
        keywords.push({ ...kw, theme: theme.theme });
      }
    }

    res.json({
      asins,
      total: keywords.length,
      themes: themes.length,
      keywords,
    });
  } catch (err) {
    console.error('[KeywordResearcher] テーマ推奨取得エラー:', err.message);
    res.status(500).json({ error: err.message });
  }
});

// ── API: Ads API 接続テスト ──
router.get('/api/ads-status', async (req, res) => {
  if (!isConfigured()) {
    return res.json({ configured: false, message: 'Ads API未設定' });
  }
  const result = await testConnection();
  res.json({ configured: true, ...result });
});

// ── API: CSV出力（入札額対応） ──
router.post('/api/export-csv', (req, res) => {
  try {
    const { keywords } = req.body;
    if (!Array.isArray(keywords) || keywords.length === 0) {
      return res.status(400).json({ error: 'keywords は必須です' });
    }

    const hasBid = keywords.some(kw => kw.bid != null);
    const header = hasBid
      ? 'キーワード,マッチタイプ,推奨入札額,ソース'
      : 'キーワード,ソース';

    const rows = keywords.map(kw => {
      if (hasBid) {
        return [
          `"${kw.keyword}"`,
          kw.matchType || '',
          kw.bid || '',
          kw.source || kw.theme || '',
        ].join(',');
      }
      return [
        `"${kw.keyword}"`,
        kw.source || '',
      ].join(',');
    });

    const csv = '\uFEFF' + header + '\n' + rows.join('\n');
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="keyword-research-${Date.now()}.csv"`);
    res.send(csv);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

export default router;
