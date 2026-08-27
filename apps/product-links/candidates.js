/**
 * 商品リンク台帳 — 取込候補 (ph_link_import_candidates) の DB 層。
 *
 * Drive 走査 / Notion 画像DB / CSV はすべてここを経由し、人が accept したときだけ本表 (ph_product_links) へ入る。
 *   - 候補の同一性 = (source_system, provider, external_id)。再走査で同じフォルダを二重に候補化しない
 *   - 推定 (inferred_ne_code) は「商品マスタに完全一致」だけ exact。前方一致は prefix、無ければ none
 *   - 自動 accept = exact かつ 台帳に同じリンク (商品 × 正規化URL) が既にある (product-hub と同じフォルダ) → duplicate として閉じる
 *     (要件定義 §5: 「external_id 一致 + ne_code 一致なら自動 accepted」。新規行は作らず既存行へ由来を足す)
 */
import { analyzeUrl, normalizeCode, isSafeHttpUrl, upsertLink, loadCatalog, validationError, LINK_TYPES, PURPOSES } from './db.js';

const NOW = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";

/** PR2 で足した列 (label = CSV のラベルを採用時に引き継ぐ)。冪等 ALTER。DB 接続ごとに確認 (Codex PR2 R1 M) */
const ensuredDbs = new WeakSet();
function ensureColumns(db) {
  if (ensuredDbs.has(db)) return;
  const cols = new Set(db.prepare('PRAGMA table_info(ph_link_import_candidates)').all().map((c) => c.name));
  if (!cols.has('label')) {
    try { db.exec('ALTER TABLE ph_link_import_candidates ADD COLUMN label TEXT'); }
    catch (e) { if (!/duplicate column/i.test(String(e?.message || ''))) throw e; }
  }
  ensuredDbs.add(db);
}

/** バッチ ID (時刻 + 乱数。同一 ms の衝突を避ける — [[feedback-unique-id-generation]]) */
export function newBatchId(prefix) {
  return `${prefix}_${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}_${Math.random().toString(36).slice(2, 8)}`;
}

/** フォルダ名 `商品コード_商品名` から候補コードを切り出す (先頭の `_` まで。無ければ全体) */
export function codeFromFolderName(name) {
  const s = String(name || '').replace(/　/g, ' ').trim();
  const m = s.match(/^([^_\s]+)_/);
  return normalizeCode(m ? m[1] : s.split(' ')[0]);
}

/** 商品マスタ照合 → { inferred, confidence } */
export function inferCode(catalog, rawCode) {
  const code = normalizeCode(rawCode);
  if (!code) return { inferred: null, confidence: 'none' };
  if (catalog.some((r) => r.code === code)) return { inferred: code, confidence: 'exact' };
  const pref = catalog.filter((r) => r.code.startsWith(code));
  if (pref.length === 1) return { inferred: pref[0].code, confidence: 'prefix' };
  return { inferred: null, confidence: 'none' };
}

/**
 * 候補を投入する。既存 (同 source×provider×external_id) はスキップ。
 * exact かつ台帳に同じリンクが既にある → その場で duplicate (人の手を煩わせない)。
 * 返す: { inserted, skipped, duplicate }
 */
export function addCandidates(db, { batchId, source, items, actor = null }) {
  ensureColumns(db);
  const catalog = loadCatalog(db);
  // 候補の同一性 = (source, provider, external_id)。Drive fileId / Canva DESIGN_ID が取れない URL は
  // provider='url', external_id=正規化URL にして同じ UNIQUE 制約に乗せる (NULL は UNIQUE で重複を止められない — Codex PR2 R1 M)
  const ins = db.prepare(`
    INSERT OR IGNORE INTO ph_link_import_candidates
      (import_batch_id, source_system, raw_code, raw_name, raw_url, provider, external_id, link_type, purpose, inferred_ne_code, confidence, label)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  let inserted = 0; let skipped = 0; let duplicate = 0;
  db.transaction(() => {
    for (const it of items) {
      const url = String(it.url || '').trim();
      if (!isSafeHttpUrl(url)) { skipped++; continue; }
      const a = analyzeUrl(url);
      const provider = a.provider || 'url';
      const externalId = a.external_id || a.normalized_url;
      const linkType = LINK_TYPES.includes(it.link_type) ? it.link_type : (a.link_type_hint || 'other');
      const purpose = it.purpose && PURPOSES.includes(it.purpose) ? it.purpose : null;
      const { inferred, confidence } = inferCode(catalog, it.raw_code);
      const label = it.label ? String(it.label).trim().slice(0, 100) || null : null;
      // 候補行を先に確保。既に候補がある (pending / rejected / accepted どれでも) ものは何もしない
      // (Codex PR2 R1 High: 却下済みの候補を再走査で本表に由来追加してしまわない)
      const r = ins.run(batchId, source, it.raw_code || null, it.raw_name || null, url, provider, externalId, linkType, purpose, inferred, confidence, label);
      if (r.changes === 0) { skipped++; continue; }
      const candId = Number(r.lastInsertRowid);
      // 今回新規に入った候補だけ: exact で本表に同じリンク (商品 × 正規化URL) があれば由来を足して duplicate で閉じる
      if (confidence === 'exact') {
        const existing = db.prepare('SELECT id FROM ph_product_links WHERE ne_code = ? AND normalized_url = ? AND deleted_at IS NULL').get(inferred, a.normalized_url);
        if (existing) {
          upsertLink(db, { neCode: inferred, linkType, purpose, url, label, source, sourceEntityId: it.source_entity_id || batchId, createdBy: actor });
          db.prepare(`UPDATE ph_link_import_candidates SET resolution = 'duplicate', accepted_link_id = ?, resolved_ne_code = ?, resolved_by = ?, resolved_at = ${NOW} WHERE id = ?`)
            .run(existing.id, inferred, actor ? `auto:${actor}` : 'auto', candId);
          duplicate++;
          continue;
        }
      }
      inserted++;
    }
  })();
  return { inserted, skipped, duplicate };
}

export function listCandidates(db, { resolution = 'pending', source = null, limit = 500 } = {}) {
  ensureColumns(db);
  const where = ['1=1']; const args = [];
  if (resolution && resolution !== 'all') { where.push('resolution = ?'); args.push(resolution); }
  if (source) { where.push('source_system = ?'); args.push(source); }
  args.push(limit);
  const rows = db.prepare(`
    SELECT * FROM ph_link_import_candidates WHERE ${where.join(' AND ')}
    ORDER BY CASE confidence WHEN 'exact' THEN 0 WHEN 'prefix' THEN 1 ELSE 2 END, raw_code, id LIMIT ?
  `).all(...args);
  const nameOf = new Map(loadCatalog(db).map((r) => [r.code, r.name]));
  for (const r of rows) r.inferred_name = r.inferred_ne_code ? (nameOf.get(r.inferred_ne_code) || '') : '';
  return rows;
}

export function candidateCounts(db) {
  ensureColumns(db);
  const rows = db.prepare('SELECT resolution, COUNT(*) AS c FROM ph_link_import_candidates GROUP BY resolution').all();
  const out = { pending: 0, accepted: 0, rejected: 0, duplicate: 0 };
  for (const r of rows) out[r.resolution] = r.c;
  return out;
}

/** 候補を採用 → 本表へ upsert (由来 = 候補の source_system)。ne_code は人が上書きできる */
export function acceptCandidate(db, id, { neCode, purpose, label, actor }) {
  ensureColumns(db);
  return db.transaction(() => {
    const c = db.prepare('SELECT * FROM ph_link_import_candidates WHERE id = ?').get(id);
    if (!c) return null;
    if (c.resolution !== 'pending') throw validationError('この候補は処理済みです');
    const code = normalizeCode(neCode || c.inferred_ne_code);
    if (!code) throw validationError('商品コードを指定してください');
    const product = loadCatalog(db).find((r) => r.code === code);
    if (!product) throw validationError(`商品コード ${code} は商品マスタにありません`);
    const pu = purpose !== undefined ? (purpose || null) : c.purpose;
    // 状態遷移を先に取る (pending のときだけ)。同時採用は片方が changes=0 で止まる (Codex PR2 R1 M)
    const claimed = db.prepare(`UPDATE ph_link_import_candidates SET resolution = 'accepted', resolved_ne_code = ?, resolved_by = ?, resolved_at = ${NOW} WHERE id = ? AND resolution = 'pending'`)
      .run(code, actor || null, id);
    if (claimed.changes !== 1) throw validationError('この候補は処理済みです');
    const r = upsertLink(db, {
      neCode: code, linkType: c.link_type, purpose: pu, url: c.raw_url, label: label || c.label || null, productName: product.name,
      source: c.source_system, sourceEntityId: `${c.import_batch_id}:${c.id}`, createdBy: actor,
    });
    db.prepare('UPDATE ph_link_import_candidates SET accepted_link_id = ? WHERE id = ?').run(r.id, id);
    return { link_id: r.id, created: r.created, ne_code: code };
  })();
}

export function rejectCandidate(db, id, { actor }) {
  const r = db.prepare(`UPDATE ph_link_import_candidates SET resolution = 'rejected', resolved_by = ?, resolved_at = ${NOW} WHERE id = ? AND resolution = 'pending'`).run(actor || null, id);
  return r.changes > 0;
}

/**
 * exact 候補をまとめて採用。対象は **画面が見せている id 集合** (人が確認した集合と同じものだけ採用する —
 * Codex PR2 R1 M: 一覧の 500 件制限の外や、古い画面から全件採用してしまわない)。
 * 渡された id のうち pending かつ exact のものだけ処理する
 */
export function acceptExactByIds(db, { ids, actor }) {
  const list = [...new Set((ids || []).map(Number).filter((n) => Number.isInteger(n) && n > 0))].slice(0, 1000);
  let accepted = 0; let failed = 0; let skipped = 0;
  for (const id of list) {
    const c = db.prepare("SELECT resolution, confidence FROM ph_link_import_candidates WHERE id = ?").get(id);
    if (!c || c.resolution !== 'pending' || c.confidence !== 'exact') { skipped++; continue; }
    try { acceptCandidate(db, id, { actor }); accepted++; } catch (e) { failed++; console.error(`[product-links] accept ${id} failed:`, e.message); }
  }
  return { accepted, failed, skipped, total: list.length };
}

/**
 * CSV テキスト → items。ヘッダ行: ne_code,url[,link_type][,purpose][,label]。区切りはカンマ/タブ。
 * 引用符つき値は最小限に対応 (URL や商品名にカンマが入るケース)
 */
export function parseCsvItems(text) {
  const lines = String(text || '').replace(/^﻿/, '').split(/\r?\n/).map((l) => l.trim()).filter(Boolean);
  if (lines.length === 0) return [];
  const split = (l) => {
    const out = []; let cur = ''; let q = false;
    const sep = l.includes('\t') ? '\t' : ',';
    for (let i = 0; i < l.length; i++) {
      const ch = l[i];
      if (ch === '"') { if (q && l[i + 1] === '"') { cur += '"'; i++; } else q = !q; }
      else if (ch === sep && !q) { out.push(cur); cur = ''; }
      else cur += ch;
    }
    out.push(cur);
    return out.map((s) => s.trim());
  };
  const head = split(lines[0]).map((h) => h.toLowerCase());
  const idx = (names) => head.findIndex((h) => names.includes(h));
  const iCode = idx(['ne_code', '商品コード', 'code']);
  const iUrl = idx(['url', 'リンク']);
  const iType = idx(['link_type', '種類']);
  const iPurpose = idx(['purpose', '用途']);
  const iLabel = idx(['label', 'ラベル']);
  const iName = idx(['name', '商品名']);
  if (iCode < 0 || iUrl < 0) throw validationError('CSV のヘッダに ne_code (商品コード) と url が必要です');
  const items = [];
  for (const l of lines.slice(1)) {
    const c = split(l);
    if (!c[iUrl]) continue;
    items.push({
      raw_code: c[iCode] || null, url: c[iUrl], raw_name: iName >= 0 ? c[iName] : null,
      link_type: iType >= 0 ? c[iType] : null, purpose: iPurpose >= 0 ? c[iPurpose] : null, label: iLabel >= 0 ? c[iLabel] : null,
    });
  }
  return items;
}
