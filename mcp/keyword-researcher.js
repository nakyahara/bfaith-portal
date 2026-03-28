#!/usr/bin/env node
/**
 * Amazon キーワードリサーチャー MCP Server
 * Claude Code から直接呼び出し可能
 *
 * ツール:
 *   - amazon_suggest: キーワードサジェスト取得（五十音/アルファベット掛け合わせ対応）
 *   - amazon_keyword_recommendations: ASINからキーワード推奨取得（Ads API）
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from '@modelcontextprotocol/sdk/types.js';

import { getSuggestions } from '../apps/keyword-researcher/suggest.js';
import {
  getKeywordRecommendations,
  getThemeRecommendations,
  isConfigured,
} from '../apps/keyword-researcher/ads-api.js';

const server = new Server(
  { name: 'amazon-keyword-researcher', version: '2.0.0' },
  { capabilities: { tools: {} } },
);

// ── ツール一覧 ──
server.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'amazon_suggest',
      description: 'Amazonのサジェスト（検索候補）を取得します。五十音・アルファベット掛け合わせで網羅的に取得可能。広告キーワード候補の調査に使います。',
      inputSchema: {
        type: 'object',
        properties: {
          seed: { type: 'string', description: 'シードキーワード（例: イヤホン bluetooth）' },
          hiragana: { type: 'boolean', description: '五十音掛け合わせ（デフォルト: true）', default: true },
          alphabet: { type: 'boolean', description: 'アルファベット掛け合わせ（デフォルト: false）', default: false },
          depth: { type: 'number', description: '深掘り階層数 1-2（デフォルト: 1）', default: 1 },
        },
        required: ['seed'],
      },
    },
    {
      name: 'amazon_keyword_recommendations',
      description: 'Amazon Ads APIを使い、ASINから広告キーワード推奨を取得します。推奨入札額・マッチタイプ付き。広告に登録すべきキーワードの判断に使います。',
      inputSchema: {
        type: 'object',
        properties: {
          asins: {
            type: 'array',
            items: { type: 'string' },
            description: '対象ASIN（最大10個）',
          },
          maxRecommendations: {
            type: 'number',
            description: '最大取得件数（デフォルト: 100）',
            default: 100,
          },
        },
        required: ['asins'],
      },
    },
  ],
}));

// ── ツール実行 ──
server.setRequestHandler(CallToolRequestSchema, async (request) => {
  const { name, arguments: args } = request.params;

  try {
    if (name === 'amazon_suggest') {
      const result = await getSuggestions(args.seed, {
        hiragana: args.hiragana ?? true,
        alphabet: args.alphabet ?? false,
        depth: Math.min(args.depth ?? 1, 2),
        delayMs: 200,
      });
      return {
        content: [{
          type: 'text',
          text: JSON.stringify(result, null, 2),
        }],
      };
    }

    if (name === 'amazon_keyword_recommendations') {
      if (!isConfigured()) {
        return {
          content: [{
            type: 'text',
            text: 'Amazon Ads APIが未設定です。.envにADS_CLIENT_ID, ADS_CLIENT_SECRET, ADS_REFRESH_TOKEN, ADS_PROFILE_IDを設定してください。',
          }],
          isError: true,
        };
      }

      const asins = args.asins.slice(0, 10);
      const max = args.maxRecommendations || 100;

      // キーワード推奨とテーマ推奨を並列取得
      const [keywords, themes] = await Promise.all([
        getKeywordRecommendations(asins, max),
        getThemeRecommendations(asins),
      ]);

      // テーマからフラットなキーワードリストに展開
      const themeKeywords = [];
      for (const theme of themes) {
        for (const kw of theme.keywords) {
          themeKeywords.push({ ...kw, source: `theme:${theme.theme}` });
        }
      }

      // 重複除去してマージ
      const seen = new Set();
      const merged = [];
      for (const kw of keywords) {
        const key = `${kw.keyword}|${kw.matchType}`;
        if (!seen.has(key)) {
          seen.add(key);
          merged.push({ ...kw, source: 'keyword-recommendation' });
        }
      }
      for (const kw of themeKeywords) {
        const key = `${kw.keyword}|${kw.matchType}`;
        if (!seen.has(key)) {
          seen.add(key);
          merged.push(kw);
        }
      }

      // 入札額降順
      merged.sort((a, b) => (b.bid || 0) - (a.bid || 0));

      return {
        content: [{
          type: 'text',
          text: JSON.stringify({
            asins,
            total: merged.length,
            keywords: merged,
          }, null, 2),
        }],
      };
    }

    return {
      content: [{ type: 'text', text: `Unknown tool: ${name}` }],
      isError: true,
    };
  } catch (err) {
    return {
      content: [{ type: 'text', text: `Error: ${err.message}` }],
      isError: true,
    };
  }
});

// ── 起動 ──
const transport = new StdioServerTransport();
await server.connect(transport);
