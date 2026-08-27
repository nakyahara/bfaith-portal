/**
 * 商品リンク台帳 (product-links) — DB 層
 *
 * 正本 = AI_reference『システム設計/商品リンク台帳_要件定義_20260827.md』(v0.1)
 *
 * 設計 (Codex R1/R2 反映):
 *   - warehouse-mirror.db 同居 (product-hub と同じ DB) → product-hub の保存と同一トランザクションで写せる
 *   - 台帳は「検索用集約台帳」。URL の入力元の正本は product-hub / Notion / 手動、
 *     商品名・区分・セット構成の正本は mirror_products / mirror_set_components (台帳は母集合を持たない)
 *   - 1 リンク : 多由来 (ph_product_link_sources)。同じリンクが product-hub と Drive 走査の両方から
 *     確認される事実を失わない (削除判定にも要る — Codex R2)
 *   - 同一判定は URL 文字列でなく normalized_url (Canva = DESIGN_ID / Drive = fileId)
 *   - 自動同期 (product_hub 由来) は人の判断 (is_primary / hidden / manual 行) を上書きしない
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { parseDriveLink } from '../product-hub/lib/drive-link.js';

export const LINK_TYPES = ['canva', 'drive_folder', 'drive_file', 'gdoc', 'other'];
export const LINK_TYPE_LABELS = {
  canva: 'Canva', drive_folder: '商品フォルダ', drive_file: 'Driveファイル', gdoc: 'Googleドキュメント', other: 'その他',
};
// 用途 (中原さん 8/27 確定の 7 値)。drive_folder は用途を持たない (NULL)
export const PURPOSES = ['top_image', 'detail_image', 'a_plus', 'variation', 'photo_instruction', 'production_doc', 'other'];
export const PURPOSE_LABELS = {
  top_image: 'TOP画像', detail_image: '詳細画像', a_plus: 'A+', variation: 'バリエーション',
  photo_instruction: '撮影指示', production_doc: '制作リンクDoc', other: 'その他',
};
export const SOURCE_SYSTEMS = ['product_hub', 'notion_image', 'notion_master', 'drive_scan', 'csv', 'manual', 'generated'];
export const SOURCE_LABELS = {
  product_hub: '商品登録ハブ', notion_image: 'Notion画像DB', notion_master: 'Notion商品マスター', drive_scan: 'Drive走査',
  csv: 'CSV取込', manual: '手入力', generated: '自動生成',
};

const NOW = "strftime('%Y-%m-%dT%H:%M:%fZ','now')";

let initialized = false;

export function initProductLinksDB() {
  const db = getMirrorDB();
  if (initialized) return db;
  db.exec(`
    CREATE TABLE IF NOT EXISTS ph_product_links (
      id                    INTEGER PRIMARY KEY AUTOINCREMENT,
      ne_code               TEXT NOT NULL,
      link_type             TEXT NOT NULL CHECK (link_type IN ('canva','drive_folder','drive_file','gdoc','other')),
      purpose               TEXT CHECK (purpose IS NULL OR purpose IN
                              ('top_image','detail_image','a_plus','variation','photo_instruction','production_doc','other')),
      url                   TEXT NOT NULL,
      normalized_url        TEXT NOT NULL,
      provider              TEXT,
      external_id           TEXT,
      label                 TEXT,
      product_name_snapshot TEXT,
      is_primary            INTEGER NOT NULL DEFAULT 0 CHECK (is_primary IN (0,1)),
      hidden                INTEGER NOT NULL DEFAULT 0 CHECK (hidden IN (0,1)),
      created_by            TEXT,
      created_at            TEXT NOT NULL DEFAULT (${NOW}),
      updated_at            TEXT NOT NULL DEFAULT (${NOW}),
      deleted_at            TEXT,
      deleted_by            TEXT
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_ppl_link ON ph_product_links(ne_code, link_type, normalized_url) WHERE deleted_at IS NULL;
    CREATE UNIQUE INDEX IF NOT EXISTS ux_ppl_primary ON ph_product_links(ne_code, purpose) WHERE is_primary = 1 AND deleted_at IS NULL;
    CREATE INDEX IF NOT EXISTS idx_ppl_code ON ph_product_links(ne_code);
    CREATE INDEX IF NOT EXISTS idx_ppl_ext ON ph_product_links(provider, external_id);

    CREATE TABLE IF NOT EXISTS ph_product_link_sources (
      link_id           INTEGER NOT NULL REFERENCES ph_product_links(id) ON DELETE CASCADE,
      source_system     TEXT NOT NULL CHECK (source_system IN
                          ('product_hub','notion_image','notion_master','drive_scan','csv','manual','generated')),
      source_entity_id  TEXT NOT NULL DEFAULT '',
      source_updated_at TEXT,
      last_synced_at    TEXT,
      detached_at       TEXT,
      PRIMARY KEY (link_id, source_system, source_entity_id)
    );
    CREATE INDEX IF NOT EXISTS idx_ppls_source ON ph_product_link_sources(source_system, source_entity_id);

    -- 取込候補 (PR2 で使う。Drive 走査 / Notion / CSV は必ずここを経由し、人が accept したときだけ本表へ)
    CREATE TABLE IF NOT EXISTS ph_link_import_candidates (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      import_batch_id  TEXT NOT NULL,
      source_system    TEXT NOT NULL,
      raw_code         TEXT,
      raw_name         TEXT,
      raw_url          TEXT NOT NULL,
      provider         TEXT,
      external_id      TEXT,
      link_type        TEXT NOT NULL,
      purpose          TEXT,
      inferred_ne_code TEXT,
      confidence       TEXT NOT NULL CHECK (confidence IN ('exact','prefix','none')),
      resolution       TEXT NOT NULL DEFAULT 'pending' CHECK (resolution IN ('pending','accepted','rejected','duplicate')),
      accepted_link_id INTEGER REFERENCES ph_product_links(id),
      resolved_ne_code TEXT,
      resolved_by      TEXT,
      resolved_at      TEXT,
      created_at       TEXT NOT NULL DEFAULT (${NOW})
    );
    CREATE UNIQUE INDEX IF NOT EXISTS ux_plic_ext ON ph_link_import_candidates(source_system, provider, external_id)
      WHERE external_id IS NOT NULL;
    CREATE INDEX IF NOT EXISTS idx_plic_res ON ph_link_import_candidates(resolution);
  `);
  initialized = true;
  return db;
}

export function getDB() {
  return initProductLinksDB();
}

// ─── 正規化 ───

/** 商品コード: product-hub と同じ LOWER(TRIM())。全角空白は先に半角へ */
export function normalizeCode(code) {
  return String(code ?? '').replace(/　/g, ' ').trim().toLowerCase();
}

const CANVA_DESIGN_RE = /^https?:\/\/(?:www\.)?canva\.com\/design\/([A-Za-z0-9_-]{6,})(?:\/|$|\?|#)/i;

/**
 * URL → { provider, external_id, normalized_url, link_type_hint }
 * - Canva /design/{DESIGN_ID}/{TOKEN}/edit → DESIGN_ID だけで同一 (TOKEN はアクセス経路であって識別子ではない)。
 *   /templates/ や DESIGN_ID を取れないものは同一化しない (Codex R2)
 * - Google Drive → fileId (folders/ file/d/ open?id= を parseDriveLink で吸収)
 * - それ以外 → scheme+host 小文字化・fragment 除去・末尾 / 除去
 */
export function analyzeUrl(raw) {
  const url = String(raw ?? '').trim();
  if (!url) return null;
  const canva = url.match(CANVA_DESIGN_RE);
  if (canva && !/\/design\/[^/]+\/copy\b/i.test(url)) {
    return { provider: 'canva', external_id: canva[1], normalized_url: `canva:${canva[1]}`, link_type_hint: 'canva' };
  }
  const drive = parseDriveLink(url);
  if (drive && drive.type !== 'unknown' && /drive\.google\.com|docs\.google\.com/i.test(url)) {
    const isDoc = /docs\.google\.com\/document/i.test(url);
    return {
      provider: 'google_drive',
      external_id: drive.id,
      normalized_url: `gdrive:${drive.id}`,
      link_type_hint: drive.type === 'folder' ? 'drive_folder' : (isDoc ? 'gdoc' : 'drive_file'),
    };
  }
  const gdoc = url.match(/^https?:\/\/docs\.google\.com\/document\/d\/([-\w]{10,})/i);
  if (gdoc) {
    return { provider: 'google_drive', external_id: gdoc[1], normalized_url: `gdrive:${gdoc[1]}`, link_type_hint: 'gdoc' };
  }
  let norm = url.replace(/#.*$/, '').replace(/\/+$/, '');
  norm = norm.replace(/^(https?:\/\/[^/]+)/i, (m) => m.toLowerCase());
  const hint = /canva\.com/i.test(url) ? 'canva' : 'other';
  return { provider: null, external_id: null, normalized_url: norm, link_type_hint: hint };
}

/** 検索語の正規化: NFKC (全角英数→半角) ・小文字・空白畳み込み */
export function normalizeText(s) {
  return String(s ?? '').normalize('NFKC').toLowerCase().replace(/[\s　]+/g, ' ').trim();
}

// ─── 書き込み ───

/**
 * 由来つき upsert (自動同期・取込 accept・手入力の共通入口)。
 * 既存行 (同 ne_code × link_type × normalized_url・未削除) があれば由来だけ足す (人の判断は触らない)。
 * 返す: { id, created: boolean }
 */
export function upsertLink(db, {
  neCode, linkType, purpose = null, url, label = null, productName = null,
  source, sourceEntityId = '', sourceUpdatedAt = null, createdBy = null,
}) {
  const ne_code = normalizeCode(neCode);
  if (!ne_code) throw new Error('ne_code が空です');
  if (!LINK_TYPES.includes(linkType)) throw new Error(`link_type が不正です: ${linkType}`);
  if (purpose !== null && !PURPOSES.includes(purpose)) throw new Error(`purpose が不正です: ${purpose}`);
  if (!SOURCE_SYSTEMS.includes(source)) throw new Error(`source_system が不正です: ${source}`);
  const a = analyzeUrl(url);
  if (!a) throw new Error('url が空です');
  const effPurpose = linkType === 'drive_folder' ? null : purpose;
  const existing = db.prepare(`
    SELECT id FROM ph_product_links WHERE ne_code = ? AND link_type = ? AND normalized_url = ? AND deleted_at IS NULL
  `).get(ne_code, linkType, a.normalized_url);
  let id; let created = false;
  if (existing) {
    id = existing.id;
    // 由来が product_hub なら label/purpose を埋めるだけ (空のときだけ)。人が付けた値は残す
    db.prepare(`
      UPDATE ph_product_links SET
        purpose = COALESCE(purpose, ?), label = COALESCE(label, ?),
        product_name_snapshot = COALESCE(product_name_snapshot, ?),
        updated_at = ${NOW}
      WHERE id = ?
    `).run(effPurpose, label, productName, id);
  } else {
    const r = db.prepare(`
      INSERT INTO ph_product_links
        (ne_code, link_type, purpose, url, normalized_url, provider, external_id, label, product_name_snapshot, created_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(ne_code, linkType, effPurpose, String(url).trim(), a.normalized_url, a.provider, a.external_id, label, productName, createdBy);
    id = Number(r.lastInsertRowid);
    created = true;
  }
  db.prepare(`
    INSERT INTO ph_product_link_sources (link_id, source_system, source_entity_id, source_updated_at, last_synced_at, detached_at)
    VALUES (?, ?, ?, ?, ${NOW}, NULL)
    ON CONFLICT(link_id, source_system, source_entity_id) DO UPDATE SET
      source_updated_at = excluded.source_updated_at, last_synced_at = excluded.last_synced_at, detached_at = NULL
  `).run(id, source, String(sourceEntityId ?? ''), sourceUpdatedAt);
  return { id, created };
}

/**
 * ある由来 (source × entity) から、いま確認できたリンク以外を「この由来からは外れた」にする。
 * 他の由来が残っていなければ deleted_at を入れる (Codex R2: 由来が消えた ≠ 即削除。手動/採用済みは残す)。
 * 返す: detached 件数
 */
export function detachStaleSources(db, { source, sourceEntityId, keepLinkIds }) {
  const keep = new Set(keepLinkIds.map(Number));
  const rows = db.prepare(`
    SELECT s.link_id FROM ph_product_link_sources s
    WHERE s.source_system = ? AND s.source_entity_id = ? AND s.detached_at IS NULL
  `).all(source, String(sourceEntityId ?? ''));
  let n = 0;
  for (const r of rows) {
    if (keep.has(Number(r.link_id))) continue;
    db.prepare(`UPDATE ph_product_link_sources SET detached_at = ${NOW} WHERE link_id = ? AND source_system = ? AND source_entity_id = ?`)
      .run(r.link_id, source, String(sourceEntityId ?? ''));
    const alive = db.prepare(`SELECT COUNT(*) AS c FROM ph_product_link_sources WHERE link_id = ? AND detached_at IS NULL`).get(r.link_id).c;
    if (alive === 0) {
      db.prepare(`UPDATE ph_product_links SET deleted_at = ${NOW}, deleted_by = ?, updated_at = ${NOW} WHERE id = ? AND deleted_at IS NULL`)
        .run(`auto:${source}`, r.link_id);
    }
    n++;
  }
  return n;
}

export function setPrimary(db, id, on) {
  const row = db.prepare('SELECT * FROM ph_product_links WHERE id = ? AND deleted_at IS NULL').get(id);
  if (!row) return false;
  db.transaction(() => {
    if (on) {
      if (row.purpose) {
        db.prepare(`UPDATE ph_product_links SET is_primary = 0, updated_at = ${NOW} WHERE ne_code = ? AND purpose = ? AND is_primary = 1 AND deleted_at IS NULL`)
          .run(row.ne_code, row.purpose);
      }
      db.prepare(`UPDATE ph_product_links SET is_primary = 1, updated_at = ${NOW} WHERE id = ?`).run(id);
    } else {
      db.prepare(`UPDATE ph_product_links SET is_primary = 0, updated_at = ${NOW} WHERE id = ?`).run(id);
    }
  })();
  return true;
}

export function updateLinkMeta(db, id, { label, purpose, hidden }) {
  const row = db.prepare('SELECT * FROM ph_product_links WHERE id = ? AND deleted_at IS NULL').get(id);
  if (!row) return false;
  const sets = []; const args = [];
  if (label !== undefined) { sets.push('label = ?'); args.push(label || null); }
  if (purpose !== undefined && row.link_type !== 'drive_folder') {
    if (purpose !== null && !PURPOSES.includes(purpose)) throw new Error('purpose が不正です');
    // 用途を変えると primary の一意 (ne_code × purpose) と衝突しうる → 変更時は primary を外す
    sets.push('purpose = ?', 'is_primary = 0'); args.push(purpose);
  }
  if (hidden !== undefined) { sets.push('hidden = ?'); args.push(hidden ? 1 : 0); }
  if (sets.length === 0) return true;
  db.prepare(`UPDATE ph_product_links SET ${sets.join(', ')}, updated_at = ${NOW} WHERE id = ?`).run(...args, id);
  return true;
}

export function softDeleteLink(db, id, by) {
  const r = db.prepare(`UPDATE ph_product_links SET deleted_at = ${NOW}, deleted_by = ?, updated_at = ${NOW} WHERE id = ? AND deleted_at IS NULL`).run(by || null, id);
  return r.changes > 0;
}

// ─── 読み取り ───

const LINK_SELECT = `
  SELECT l.*,
    (SELECT group_concat(s.source_system) FROM ph_product_link_sources s WHERE s.link_id = l.id AND s.detached_at IS NULL) AS sources
  FROM ph_product_links l
  WHERE l.deleted_at IS NULL
`;

/** 複数コードのリンクを一括取得 → Map<ne_code, rows[]> (primary → 用途 → 新しい順) */
export function linksByCodes(db, codes) {
  const map = new Map();
  const list = [...new Set(codes.map(normalizeCode).filter(Boolean))];
  if (list.length === 0) return map;
  const CH = 400;
  for (let i = 0; i < list.length; i += CH) {
    const chunk = list.slice(i, i + CH);
    const rows = db.prepare(`${LINK_SELECT} AND l.ne_code IN (${chunk.map(() => '?').join(',')})
      ORDER BY l.is_primary DESC, l.link_type = 'drive_folder' DESC, l.purpose, l.updated_at DESC`).all(...chunk);
    for (const r of rows) {
      r.sources = r.sources ? r.sources.split(',') : [];
      if (!map.has(r.ne_code)) map.set(r.ne_code, []);
      map.get(r.ne_code).push(r);
    }
  }
  return map;
}

export function getLink(db, id) {
  const r = db.prepare(`${LINK_SELECT} AND l.id = ?`).get(id);
  if (r) r.sources = r.sources ? r.sources.split(',') : [];
  return r || null;
}

/** mirror_products が同居しているか (smoke など mirror 無し環境でも落ちないように) */
function hasTable(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

/**
 * 検索母集合 = mirror_products 全件 (+ product-hub にしか無いドラフト)。
 * 日本語の全半角ゆれは SQL では正規化できないので、母集合 (~5,000 件) を読み JS 側で NFKC 照合する。
 */
let _catalogCache = { at: 0, rows: null };
export function loadCatalog(db, { maxAgeMs = 60_000 } = {}) {
  const now = Date.now();
  if (_catalogCache.rows && now - _catalogCache.at < maxAgeMs) return _catalogCache.rows;
  const byCode = new Map();
  if (hasTable(db, 'mirror_products')) {
    const rows = db.prepare(`SELECT 商品コード AS code, 商品名 AS name, 商品区分 AS kind, 取扱区分 AS handling, セット構成品数 AS set_count FROM mirror_products`).all();
    for (const r of rows) {
      const code = normalizeCode(r.code);
      if (!code) continue;
      byCode.set(code, {
        code, display_code: String(r.code).trim(), name: r.name || '', kind: r.kind || '', handling: r.handling || '',
        is_set: r.kind === 'セット' || (Number(r.set_count) > 0), draft_id: null, jan: null, in_mirror: true,
      });
    }
  }
  if (hasTable(db, 'product_drafts')) {
    const drafts = db.prepare('SELECT id, ne_code, name, jan_code, status FROM product_drafts').all();
    for (const d of drafts) {
      const code = normalizeCode(d.ne_code);
      if (!code) continue;
      const cur = byCode.get(code);
      if (cur) { cur.draft_id = d.id; cur.jan = d.jan_code || null; cur.draft_status = d.status; }
      else {
        byCode.set(code, {
          code, display_code: String(d.ne_code).trim(), name: d.name || '', kind: '', handling: '', is_set: false,
          draft_id: d.id, jan: d.jan_code || null, draft_status: d.status, in_mirror: false,
        });
      }
    }
  }
  if (hasTable(db, 'draft_set_members')) {
    // product-hub で派生したセット (mirror にまだ無い) はセット扱い
    const sets = db.prepare('SELECT DISTINCT p.ne_code FROM draft_set_members m JOIN product_drafts p ON p.id = m.set_draft_id').all();
    for (const s of sets) { const c = byCode.get(normalizeCode(s.ne_code)); if (c) c.is_set = true; }
  }
  const rows = [...byCode.values()];
  for (const r of rows) { r._name_n = normalizeText(r.name); r._code_n = r.code; }
  _catalogCache = { at: now, rows };
  return rows;
}
export function invalidateCatalogCache() { _catalogCache = { at: 0, rows: null }; }

/**
 * 検索。q の各語 (空白区切り) が 商品コード前方一致 / 商品名部分一致 / JAN 完全一致 のどれかに当たる行 (AND)。
 * onlyMissing = リンク未登録だけ。返す: { total, rows: [{...catalog, links: [], members: [{code,name,qty,links}]}] }
 */
export function searchProducts(db, { q = '', onlyMissing = false, onlySet = null, limit = 100 } = {}) {
  const catalog = loadCatalog(db);
  const terms = normalizeText(q).split(' ').filter(Boolean);
  let rows = catalog.filter((r) => {
    if (onlySet === true && !r.is_set) return false;
    if (onlySet === false && r.is_set) return false;
    return terms.every((t) => r._code_n.startsWith(t) || r._name_n.includes(t) || (r.jan && r.jan === t));
  });
  const linkMap = linksByCodes(db, rows.map((r) => r.code));
  if (onlyMissing) rows = rows.filter((r) => !(linkMap.get(r.code) || []).some((l) => !l.hidden));
  // 並び: リンクあり → コード。語が無ければ「最近更新されたリンクの商品」を先頭に
  const lastOf = (code) => (linkMap.get(code) || []).reduce((m, l) => (l.updated_at > m ? l.updated_at : m), '');
  rows.sort((a, b) => {
    if (terms.length === 0) { const d = lastOf(b.code).localeCompare(lastOf(a.code)); if (d !== 0) return d; }
    const ha = linkMap.has(a.code) ? 0 : 1; const hb = linkMap.has(b.code) ? 0 : 1;
    if (ha !== hb) return ha - hb;
    return a.code.localeCompare(b.code);
  });
  const total = rows.length;
  rows = rows.slice(0, limit);
  // セット構成 (mirror_set_components 正本・product-hub 派生は draft_set_members で補完) と構成単品のリンク (参照表示)
  const setCodes = rows.filter((r) => r.is_set).map((r) => r.code);
  const members = membersOf(db, setCodes);
  const memberCodes = [...members.values()].flat().map((m) => m.code);
  const memberLinks = linksByCodes(db, memberCodes);
  const nameOf = new Map(catalog.map((r) => [r.code, r.name]));
  const out = rows.map((r) => ({
    code: r.code, display_code: r.display_code, name: r.name, kind: r.kind, handling: r.handling, is_set: r.is_set,
    draft_id: r.draft_id, draft_status: r.draft_status || null, jan: r.jan, in_mirror: r.in_mirror,
    links: linkMap.get(r.code) || [],
    members: (members.get(r.code) || []).map((m) => ({
      code: m.code, name: m.name || nameOf.get(m.code) || '', qty: m.qty, links: memberLinks.get(m.code) || [],
    })),
  }));
  return { total, rows: out };
}

/** セット構成 → Map<set_code, [{code, name, qty}]> */
export function membersOf(db, setCodes) {
  const map = new Map();
  const list = [...new Set(setCodes.map(normalizeCode).filter(Boolean))];
  if (list.length === 0) return map;
  if (hasTable(db, 'mirror_set_components')) {
    const rows = db.prepare(`
      SELECT LOWER(TRIM(セット商品コード)) AS set_code, LOWER(TRIM(構成商品コード)) AS code, 構成商品名 AS name, 数量 AS qty
      FROM mirror_set_components WHERE LOWER(TRIM(セット商品コード)) IN (${list.map(() => '?').join(',')})
      ORDER BY 構成商品コード
    `).all(...list);
    for (const r of rows) { if (!map.has(r.set_code)) map.set(r.set_code, []); map.get(r.set_code).push({ code: r.code, name: r.name, qty: r.qty }); }
  }
  if (hasTable(db, 'draft_set_members')) {
    const rows = db.prepare(`
      SELECT LOWER(TRIM(p.ne_code)) AS set_code, LOWER(TRIM(m.member_ne_code)) AS code, m.qty
      FROM draft_set_members m JOIN product_drafts p ON p.id = m.set_draft_id
      WHERE LOWER(TRIM(p.ne_code)) IN (${list.map(() => '?').join(',')}) ORDER BY m.sort, m.member_ne_code
    `).all(...list);
    for (const r of rows) {
      if (map.has(r.set_code)) continue; // mirror が正。無いときだけ補完
      if (!map.has(r.set_code)) map.set(r.set_code, []);
      map.get(r.set_code).push({ code: r.code, name: null, qty: r.qty });
    }
  }
  return map;
}

export function stats(db) {
  const links = db.prepare(`SELECT COUNT(*) AS c, COUNT(DISTINCT ne_code) AS products FROM ph_product_links WHERE deleted_at IS NULL`).get();
  const catalog = loadCatalog(db);
  const active = catalog.filter((r) => !r.handling || r.handling === '取扱中').length;
  return { links: links.c, products_with_links: links.products, catalog: catalog.length, catalog_active: active };
}
