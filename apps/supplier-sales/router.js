/**
 * 仕入れ先向け売れ筋共有 — 社内管理ルーター（要ログイン / requireAppAccess）
 *
 *  GET  /                         管理画面（仕入先一覧・トークン発行・プレビュー）
 *  GET  /api/suppliers            仕入先コード一覧（表示名・商品数・有効トークン数）
 *  POST /api/supplier-name        仕入先コード→表示名 の登録/更新
 *  GET  /api/summary?code=        指定仕入先の商品別売上サマリ（プレビュー）
 *  GET  /api/summary.csv?code=    同 CSV ダウンロード
 *  GET  /api/health               未解決率（名寄せ健全性）
 *  GET  /api/tokens?code=         トークン一覧
 *  POST /api/tokens               トークン発行（生トークンを 1 度だけ返す）
 *  POST /api/tokens/:id/revoke    トークン失効
 */
import express from 'express';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { getSupplierSummary, getUnresolvedStats, MALL_LABELS } from './aggregate.js';
import {
  listSuppliers, upsertSupplierName, getSupplierName,
  listTokens, createToken, revokeToken,
} from './share-db.js';

const router = express.Router();

router.get('/', (req, res) => {
  res.render('supplier-sales', {
    title: '仕入れ先 売れ筋共有',
    displayName: req.session?.displayName || req.session?.email || '',
  });
});

router.get('/api/suppliers', (req, res) => {
  try {
    res.json({ ok: true, suppliers: listSuppliers() });
  } catch (e) {
    console.error('[supplier-sales] suppliers error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/api/health', (req, res) => {
  try {
    res.json({ ok: true, health: getUnresolvedStats(getMirrorDB()) });
  } catch (e) {
    console.error('[supplier-sales] health error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.post('/api/supplier-name', (req, res) => {
  try {
    const code = String(req.body?.code || '').trim();
    const name = String(req.body?.name || '').trim();
    const memo = req.body?.memo ? String(req.body.memo) : null;
    if (!code || !name) return res.status(400).json({ ok: false, error: '仕入先コードと表示名は必須です' });
    upsertSupplierName(code, name, memo);
    res.json({ ok: true });
  } catch (e) {
    console.error('[supplier-sales] supplier-name error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/api/summary', (req, res) => {
  try {
    const code = String(req.query.code || '').trim();
    if (!code) return res.status(400).json({ ok: false, error: 'code は必須です' });
    const summary = getSupplierSummary(getMirrorDB(), code);
    res.json({
      ok: true,
      supplierName: getSupplierName(code) || code,
      mallLabels: MALL_LABELS,
      ...summary,
    });
  } catch (e) {
    console.error('[supplier-sales] summary error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/api/summary.csv', (req, res) => {
  try {
    const code = String(req.query.code || '').trim();
    if (!code) return res.status(400).send('code は必須です');
    const { products, windows } = getSupplierSummary(getMirrorDB(), code);
    const name = getSupplierName(code) || code;
    const header = ['商品コード', '商品名', '7日販売数', '7日売上', '30日販売数', '30日売上', '30日日平均', '最終販売日'];
    const lines = [header.join(',')];
    for (const p of products) {
      lines.push([
        p.ne_code, csvCell(p.product_name),
        p.units7, p.sales7, p.units30, p.sales30,
        p.avgPerDay30.toFixed(2), p.lastSold || '',
      ].join(','));
    }
    const cutoff = windows ? windows.cutoff : '';
    // UTF-8 BOM 付き（Excel で文字化けしないよう）
    const csv = '﻿' + lines.join('\r\n') + '\r\n';
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition',
      `attachment; filename="supplier_sales_${encodeURIComponent(name)}_${cutoff}.csv"`);
    res.send(csv);
  } catch (e) {
    console.error('[supplier-sales] csv error:', e.message);
    res.status(500).send('CSV 生成に失敗しました');
  }
});

router.get('/api/tokens', (req, res) => {
  try {
    const code = req.query.code ? String(req.query.code).trim() : null;
    res.json({ ok: true, tokens: listTokens(code) });
  } catch (e) {
    console.error('[supplier-sales] tokens error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.post('/api/tokens', (req, res) => {
  try {
    const supplierCode = String(req.body?.code || '').trim();
    const label = req.body?.label ? String(req.body.label).trim() : null;
    const expiresAt = req.body?.expiresAt ? String(req.body.expiresAt).trim() : null;
    if (!supplierCode) return res.status(400).json({ ok: false, error: 'code は必須です' });
    // 表示名が未登録のトークン発行はブロック（公開ページに仕入先名を出すため）
    if (!getSupplierName(supplierCode)) {
      return res.status(400).json({ ok: false, error: '先に仕入先の表示名を登録してください' });
    }
    const { id, rawToken } = createToken({
      supplierCode, label, expiresAt,
      createdBy: req.session?.email || null,
    });
    // 生トークンはここでのみ返す（DB はハッシュのみ保持、再表示不可）
    res.json({ ok: true, id, rawToken, sharePath: `/share/supplier/${rawToken}` });
  } catch (e) {
    console.error('[supplier-sales] create token error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.post('/api/tokens/:id/revoke', (req, res) => {
  try {
    const id = Number(req.params.id);
    if (!Number.isInteger(id)) return res.status(400).json({ ok: false, error: '不正な id' });
    revokeToken(id);
    res.json({ ok: true });
  } catch (e) {
    console.error('[supplier-sales] revoke error:', e.message);
    res.status(500).json({ ok: false, error: e.message });
  }
});

function csvCell(s) {
  let v = String(s ?? '');
  // CSV インジェクション対策: =,+,-,@ 始まりは Excel 等で式評価されるため ' を前置
  if (/^[=+\-@]/.test(v)) v = `'${v}`;
  return /[",\r\n]/.test(v) ? `"${v.replace(/"/g, '""')}"` : v;
}

export default router;
