/**
 * product-hub (商品登録一元化) router。
 * 要件定義: AI_reference『システム設計/商品登録一元化_要件定義_20260703.md』P1 スコープ。
 *   - ドラフト一覧/詳細編集 (サムネ付き)
 *   - 参考URL必須ゲート (公式URL等が揃うまで「生成待ち」に進めない)
 *   - Notion カード自動作成 + 未作成バナー + リトライ
 * mount: server.js で /apps/product-hub (requireAppAccess 配下)
 */
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';

import crypto from 'crypto';

import {
  getDB, logEvent, gateReasons, demoteIfGateBroken, applyFolderImport,
  claimGenerationDrafts, generationClaimError, releaseGenerationClaim, acquireGenerationWriteLock,
  extractAsin, saveSpKeywordSnapshot, loadSpKeywordSnapshot,
  upsertDraftYahoo, upsertImageProduction, listGenerationQueue, isNotionImported, isNeCodeUniqueEnforced, isKnownImageFileId,
  DRAFT_STATUSES, STATUS_LABELS, AI_OUTPUT_KINDS,
} from './db.js';
import { parseDriveLink, thumbnailUrl, fileViewUrl, THUMB_WIDTHS, DRIVE_FILE_ID_PATTERN } from './lib/drive-link.js';
import { attemptCardCreation, retryPendingCards, pendingCardCount, syncCardLinks, isNotionCardEnabled } from './services/notion-card.js';
import { importFromNotion, parseNeCodes, MAX_IMPORT_CODES } from './services/notion-import.js';
import { resolveVariationGroup, resolveVariationGroupsBatch, effectiveHasVariation, mirrorReady, resolveNeDefaults, getNeCost } from './lib/variation.js';
import { regroupToRepCode, regroupBlockReason } from './services/regroup.js';
import { registerByCodes, syncNewProducts, intakeStatus, MAX_REGISTER_CODES } from './services/new-product-intake.js';
import {
  transferImagesToCabinet, buildItemPayload, registerHidden, parseAttributes,
  setItemVisibility, SHIPPING_METHOD_GROUPS,
  fetchGenreAttributes, getCachedGenreAttributes, listDriveFolderImages, fetchShopCategoryTree, syncShopCategoriesToRms, shopCategorySyncState, buildDescriptionPreview,
  getDriveThumbnail, SHIPPING_BANNER_LOCATIONS, COMMON_TRAILING_BANNERS, cabinetImageUrl, effectiveShippingForDraft,
} from './services/rakuten-listing.js';
import { assignImageSlots, MAX_IMAGE_SLOTS } from './lib/folder-import.js';
import { fetchGenreChildren, suggestGenreByName, genrePathOf } from './lib/ichiba-genre.js';
import { computeProfit, TAKE_RATE } from './lib/profit.js';
import { listSpManualKeywordsByAsin } from '../keyword-researcher/ads-api.js';
import {
  PRODUCT_TYPES, CATEGORY_LABELS, CATEGORY_LABELS_BY_TYPE, adResponsibility, validatePageInfo,
  buildPageInfoHtml, mapNeShippingToRakuten, listShippingMethodMap, saveShippingMethodMap,
} from './lib/page-info.js';
import {
  parseShopCategoryText, replaceShopCategories, countActiveShopCategories,
  listShopCategoriesForDraft, selectedShopCategoryPaths, setDraftShopCategories,
  sanitizeShopCategoryIds, MAX_SHOP_CATEGORY_LINES, MAX_DRAFT_SHOP_CATEGORIES, flattenCategoryTrees, shopCategorySnapshotHash,
  suggestShopCategories, canAutoApplyShopCategory, countSelectableShopCategories,
  shopCategoriesNeverSaved, isAutoApplyRequestValid,
} from './lib/shop-categories.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = express.Router();
router.use(express.json({ limit: '512kb' }));

const view = (name) => path.join(__dirname, 'views', name);
const actorOf = (req) => req.session?.email || req.session?.displayName || null;

// ステータス遷移の許可表 (§3)。on_hold/excluded へは全ステータスから退避可
const TRANSITIONS = {
  draft: ['ready_for_ai'],
  ready_for_ai: ['draft', 'review'],
  review: ['approved', 'draft'],
  approved: ['listed', 'review'],
  listed: ['expanded'],
  expanded: [],
  on_hold: ['draft'],
  excluded: ['draft'],
};
const ESCAPE_STATUSES = ['on_hold', 'excluded'];

function canTransition(from, to) {
  if (!DRAFT_STATUSES.includes(to)) return false;
  if (ESCAPE_STATUSES.includes(to)) return from !== to;
  return (TRANSITIONS[from] || []).includes(to);
}

// 金額 sanitize (非有限→null、0〜1兆 clamp、整数化)。
// 売価に負数は無意味なので 0 に clamp — DB の CHECK (0..1e12) と必ず整合させる (Codex R2 medium)
function sanitizeMoney(v) {
  if (v == null || v === '') return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  const clamped = Math.max(0, Math.min(1e12, n));
  return Math.round(clamped);
}

function isHttpUrl(s) {
  if (typeof s !== 'string') return false;
  const t = s.trim();
  return /^https?:\/\/\S+$/i.test(t);
}

function cleanText(v, maxLen = 2000) {
  if (v == null) return null;
  const s = String(v).trim();
  if (s === '') return null;
  return s.length > maxLen ? s.slice(0, maxLen) : s;
}

function loadDraftOr404(req, res) {
  const id = Number.parseInt(req.params.id, 10);
  if (!Number.isInteger(id) || id <= 0) {
    res.status(400).json({ ok: false, error: 'invalid id' });
    return null;
  }
  const db = getDB();
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  if (!draft) {
    res.status(404).json({ ok: false, error: 'draft not found' });
    return null;
  }
  return draft;
}

// 出品カードで選択済みの楽天配送方法グループ名 (未選択なら null)
function rakutenShippingLabelOf(db, draftId) {
  const rk = db.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(draftId);
  const g = rk?.shipping_method_group != null ? String(rk.shipping_method_group).trim() : '';
  return g && SHIPPING_METHOD_GROUPS[g] ? SHIPPING_METHOD_GROUPS[g] : null;
}

// ─── 画面 ───────────────────────────────────────────────

router.get('/', (req, res) => {
  const db = getDB();
  const statusFilter = DRAFT_STATUSES.includes(req.query.status) ? req.query.status : null;

  const counts = {};
  for (const s of DRAFT_STATUSES) counts[s] = 0;
  for (const row of db.prepare('SELECT status, COUNT(*) AS c FROM product_drafts GROUP BY status').all()) {
    if (counts[row.status] != null) counts[row.status] = row.c;
  }

  const where = statusFilter ? 'WHERE d.status = ?' : '';
  const params = statusFilter ? [statusFilter] : [];
  const drafts = db.prepare(`
    SELECT d.*,
      (SELECT drive_file_id FROM draft_images i WHERE i.draft_id = d.id ORDER BY i.sort, i.id LIMIT 1) AS first_image_id
    FROM product_drafts d ${where}
    ORDER BY d.updated_at DESC
    LIMIT 500
  `).all(...params);
  // NE 判定は一覧では 2 クエリで一括解決する (行ごとに引くと N+1 になる)
  const variations = resolveVariationGroupsBatch(db, drafts.map((d) => d.ne_code));
  for (const d of drafts) {
    d.thumb = d.first_image_id ? thumbnailUrl(d.first_image_id, 160) : null;
    d.variation = variations.get(String(d.ne_code || '').trim().toLowerCase())
      || { kind: 'unknown', groupKey: d.ne_code, memberCount: 0, isChild: false };
  }

  res.render(view('index.ejs'), {
    title: '商品登録ハブ',
    displayName: req.session?.displayName || req.session?.email || '',
    drafts, counts, statusFilter,
    statuses: DRAFT_STATUSES, statusLabels: STATUS_LABELS,
    notionPending: pendingCardCount(),
    maxImportCodes: MAX_IMPORT_CODES,
    maxRegisterCodes: MAX_REGISTER_CODES,
    intake: intakeStatus(),
    notionCardEnabled: isNotionCardEnabled(),
    isAdmin: req.session?.role === 'admin',
    shopCategoryCount: countActiveShopCategories(db),
    maxShopCategoryLines: MAX_SHOP_CATEGORY_LINES,
  });
});

router.get('/new', (req, res) => {
  res.render(view('new.ejs'), {
    title: '新規商品ドラフト',
    displayName: req.session?.displayName || req.session?.email || '',
  });
});

router.get('/detail/:id', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  const yahoo = db.prepare('SELECT * FROM draft_yahoo WHERE draft_id = ?').get(draft.id) || null;
  const imageProduction = db.prepare('SELECT * FROM draft_image_production WHERE draft_id = ?').get(draft.id) || null;
  const refs = db.prepare('SELECT * FROM draft_reference_urls WHERE draft_id = ? ORDER BY sort, id').all(draft.id);
  const images = db.prepare('SELECT * FROM draft_images WHERE draft_id = ? ORDER BY sort, id').all(draft.id);
  for (const img of images) {
    img.thumb = thumbnailUrl(img.drive_file_id, 320);
    img.view_url = img.drive_url || fileViewUrl(img.drive_file_id);
  }
  const specs = db.prepare('SELECT * FROM draft_specs WHERE draft_id = ? ORDER BY sort, id').all(draft.id);
  const aiRows = db.prepare('SELECT * FROM draft_ai_outputs WHERE draft_id = ?').all(draft.id);
  const aiOutputs = {};
  for (const k of AI_OUTPUT_KINDS) aiOutputs[k] = aiRows.find((r) => r.kind === k) || null;
  const events = db.prepare('SELECT * FROM draft_events WHERE draft_id = ? ORDER BY id DESC LIMIT 30').all(draft.id);

  const nextStatuses = (TRANSITIONS[draft.status] || [])
    .concat(ESCAPE_STATUSES.filter((s) => s !== draft.status));

  // NE の代表商品コードでバリエーション判定 (NE が正・DB には焼かない)
  const variation = resolveVariationGroup(db, draft.ne_code, { draftId: draft.id });
  const hasVariation = effectiveHasVariation(variation, draft);
  const regroup = regroupBlockReason(db, draft, variation);
  // 外したSKUが「どのページにも載っていない」状態を見えるようにする (Codex high-2 の可視化)。
  // 別ページ化は手動 (中原さん方針) なので、未登録なら未割当として明示する
  for (const m of variation.excludedMembers) {
    m.ownDraftId = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?')
      .get(String(m.商品コード).trim().toLowerCase())?.id || null;
  }

  // 利益シミュレーション (Notion 数式の移植)。原価・送料 (配送方法から自動算出済) ・税率は NE から
  const neCost = getNeCost(db, draft.ne_code);
  const simTaxPercent = (() => {
    const t = String(yahoo?.tax_rate ?? '').trim().match(/^(\d+)/);
    if (t) return Number(t[1]);
    return neCost?.taxPercent ?? 10;
  })();
  const profitSim = neCost ? computeProfit({
    price: draft.price, costExTax: neCost.costExTax,
    taxPercent: simTaxPercent, shippingCost: neCost.shippingCost,
  }) : null;

  const pageInfo = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(draft.id) || null;
  // 発送方法: NE 配送方法 → 楽天グループ (マップ表 + 名前一致の推測)
  const neShipping = mapNeShippingToRakuten(db, neCost?.shippingMethod);
  const pageInfoHtml = buildPageInfoHtml({
    productName: draft.name, info: pageInfo,
    shippingLabel: pageInfo?.shipping_label_override
      || (rakutenShippingLabelOf(db, draft.id) ?? neShipping.label),
  });

  const rakuten = db.prepare('SELECT * FROM draft_rakuten WHERE draft_id = ?').get(draft.id) || null;
  // 出品時に商品画像の末尾へ自動追加される店舗共通バナー (画像タブのプレビュー用)。
  // 配送方法の解決は buildItemPayload と同じ関数 (アプリ指定 > NEマッピング)
  const effShipGroup = effectiveShippingForDraft(db, draft.ne_code, rakuten?.shipping_method_group).group;
  const trailingBanners = [
    ...(SHIPPING_BANNER_LOCATIONS[effShipGroup]
      ? [{ location: SHIPPING_BANNER_LOCATIONS[effShipGroup], label: `配送: ${SHIPPING_METHOD_GROUPS[effShipGroup]}` }]
      : []),
    ...COMMON_TRAILING_BANNERS,
  ].map((b) => ({ ...b, url: cabinetImageUrl(b.location) }));
  // ジャンル属性辞書 (取得済みならUIに必須件数と自動フォームの材料を渡す)
  const genreDict = rakuten?.genre_id ? getCachedGenreAttributes(db, rakuten.genre_id) : null;
  const cabinetImages = db.prepare('SELECT * FROM draft_cabinet_images WHERE draft_id = ? ORDER BY id').all(draft.id);

  res.render(view('detail.ejs'), {
    title: `商品ドラフト #${draft.id}`,
    displayName: req.session?.displayName || req.session?.email || '',
    draft, refs, images, specs, aiOutputs, events, yahoo, imageProduction,
    gate: gateReasons(db, draft),
    nextStatuses,
    statusLabels: STATUS_LABELS,
    aiKinds: AI_OUTPUT_KINDS,
    variation, hasVariation, regroup,
    rakuten, cabinetImages, genreDict,
    shopCatSyncState: shopCategorySyncState(db, draft.id, rakuten),
    thumbnailUrl, fileViewUrl,
    neCost, profitSim, simTaxPercent, profitTakeRate: TAKE_RATE,
    pageInfo, pageInfoHtml, neShipping,
    productTypes: PRODUCT_TYPES, categoryLabels: CATEGORY_LABELS,
    categoryLabelsByType: CATEGORY_LABELS_BY_TYPE,
    adResponsibilityText: adResponsibility(),
    shopCategories: listShopCategoriesForDraft(db, draft.id),
    shippingGroups: SHIPPING_METHOD_GROUPS,
    trailingBanners,
  });
});

// ─── API: ドラフト作成/更新 ───────────────────────────────

router.post('/api/drafts', async (req, res) => {
  const name = cleanText(req.body?.name, 300);
  const neCode = cleanText(req.body?.ne_code, 100);
  const officialUrl = cleanText(req.body?.official_url, 1000);
  if (!name) return res.status(400).json({ ok: false, error: '商品名は必須です' });
  if (!neCode) return res.status(400).json({ ok: false, error: 'NE商品コードは必須です' });
  if (officialUrl && !isHttpUrl(officialUrl)) {
    return res.status(400).json({ ok: false, error: '公式ページURLの形式が不正です (http/https)' });
  }
  const amazonUrlCreate = cleanText(req.body?.amazon_url, 1000);
  if (amazonUrlCreate && !isHttpUrl(amazonUrlCreate)) {
    return res.status(400).json({ ok: false, error: 'Amazon URLの形式が不正です (http/https)' });
  }

  const db = getDB();

  // fail-closed (Codex medium): 判定基盤が壊れている状態で子SKUを登録すると、
  // 代表コードにまとめられないドラフト + Notion カードができ、カード作成後は regroup 禁止で
  // 自動修復できない。NE に「その商品コードが無い」のは正常 (新商品) なので通す。
  if (!mirrorReady(db)) {
    return res.status(503).json({ ok: false, error: 'NE商品マスタを参照できないため登録できません (時間をおいて再試行してください)' });
  }
  if (!isNeCodeUniqueEnforced()) {
    return res.status(503).json({
      ok: false,
      error: '商品コードの重複 (大文字小文字違い) があるため登録を停止しています。管理者にご連絡ください',
    });
  }

  // 既定でまとめる (2026-07-25 中原さん方針): 子SKUのコードで登録されたら、
  // 確認を挟まず代表商品コード (= 楽天1ページ) に正規化する。人の作業を最小にするため。
  // 明示的にバリエーションから外された SKU は対象外 (別ページにしたい意図なので、そのまま登録)。
  // 判定と INSERT は 1 トランザクション: 分けると restore と競合して二重掲載しうる (Codex high-1)
  const created = db.transaction(() => {
    const vari = resolveVariationGroup(db, neCode, { withMembers: false });
    const groupedFrom = (vari.kind === 'variation' && vari.isChild) ? neCode : null;
    const effectiveCode = groupedFrom ? vari.groupKey : neCode;
    try {
      const info = db.prepare(`
        INSERT INTO product_drafts (ne_code, name, official_url, price, jan_code, asin, amazon_url, created_by)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(effectiveCode, name, officialUrl, sanitizeMoney(req.body?.price), cleanText(req.body?.jan_code, 20),
        cleanText(req.body?.asin, 20), amazonUrlCreate, actorOf(req));
      const id = Number(info.lastInsertRowid);
      logEvent(db, id, 'created', effectiveCode, actorOf(req));
      if (groupedFrom) logEvent(db, id, 'grouped_on_create', `${groupedFrom} → ${effectiveCode}`, actorOf(req));
      // NE の税率を初期値として入れる (中原さん決定: 初期値だけ。以降はアプリで編集)。
      // **作成と同じトランザクション**に入れる: 分けると失敗時に「ドラフトはあるが税率なし」で
      // 再送すると商品コード重複 409 になり復旧しづらい (Codex high-1)。
      // ⚠️ NE 税率 0%/想定外値は入れない (誤った税率が Yahoo へ流れる方が危険) → taxToPercent が null を返す
      // 画面側と同じ **入力されたコード** で引く。effectiveCode (正規化後の代表コード) で引くと、
      // 代表コード行が NE に無い場合 (365種中338種) に画面では出て登録時は空、というズレになる
      const neDefaults = resolveNeDefaults(db, neCode);
      if (neDefaults?.taxPercent) {
        upsertDraftYahoo(db, id, { tax_rate: `${neDefaults.taxPercent}%` });
      }
      return { id, effectiveCode, groupedFrom, memberCount: vari.memberCount };
    } catch (e) {
      if (!String(e.message).includes('UNIQUE')) throw e;
      const existing = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?')
        .get(String(effectiveCode).trim().toLowerCase());
      // 正規化した結果ぶつかった場合は「なぜ別コードのドラフトに当たったか」を説明する
      return {
        conflict: groupedFrom
          ? `${groupedFrom} は代表商品コード ${effectiveCode} のバリエーションです。${effectiveCode} のドラフトが既にあります`
          : `商品コード ${effectiveCode} のドラフトは既に存在します`,
        existingId: existing?.id,
      };
    }
  })();
  if (created.conflict) {
    return res.status(409).json({ ok: false, error: created.conflict, existingId: created.existingId });
  }
  const { id: draftId, effectiveCode, groupedFrom } = created;

  // §5: 登録と同時に Notion カード作成 (失敗しても登録は成功。バナー+リトライで回収)
  const notion = await attemptCardCreation(draftId, { actor: actorOf(req) });
  res.json({
    ok: true, id: draftId, notion,
    ne_code: effectiveCode,
    grouped: groupedFrom ? { from: groupedFrom, to: effectiveCode, memberCount: created.memberCount } : null,
  });
});

router.post('/api/drafts/:id', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();

  const name = cleanText(req.body?.name, 300) || draft.name;
  const officialUrl = req.body?.official_url !== undefined ? cleanText(req.body.official_url, 1000) : draft.official_url;
  if (officialUrl && !isHttpUrl(officialUrl)) {
    return res.status(400).json({ ok: false, error: '公式ページURLの形式が不正です (http/https)' });
  }
  const driveFolderUrl = req.body?.drive_folder_url !== undefined ? cleanText(req.body.drive_folder_url, 1000) : draft.drive_folder_url;
  if (driveFolderUrl && !isHttpUrl(driveFolderUrl)) {
    return res.status(400).json({ ok: false, error: '画像フォルダURLの形式が不正です (http/https)' });
  }

  // has_variation は NE を正とする方針なので、**NE で判定できる商品は DB に焼かない** (Codex high-3)。
  // クライアント側だけの制御だと、後で NE から消えたときに「焼かれた NE 値」が手入力値として復活する
  const neInfo = resolveVariationGroup(getDB(), draft.ne_code, { withMembers: false });
  const neDecides = neInfo.kind === 'variation' || neInfo.kind === 'single';
  const hasVariationValue = (!neDecides && req.body?.has_variation !== undefined)
    ? (req.body.has_variation ? 1 : 0)
    : draft.has_variation;

  const amazonUrl = req.body?.amazon_url !== undefined ? cleanText(req.body.amazon_url, 1000) : draft.amazon_url;
  if (amazonUrl && !isHttpUrl(amazonUrl)) {
    return res.status(400).json({ ok: false, error: 'Amazon URLの形式が不正です (http/https)' });
  }
  db.prepare(`
    UPDATE product_drafts SET
      name = ?, official_url = ?, price = ?, jan_code = ?, has_variation = ?,
      drive_folder_url = ?, memo = ?, asin = ?, amazon_url = ?, own_brand = ?,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    WHERE id = ?
  `).run(
    name,
    officialUrl,
    req.body?.price !== undefined ? sanitizeMoney(req.body.price) : draft.price,
    req.body?.jan_code !== undefined ? cleanText(req.body.jan_code, 20) : draft.jan_code,
    hasVariationValue,
    driveFolderUrl,
    req.body?.memo !== undefined ? cleanText(req.body.memo, 4000) : draft.memo,
    req.body?.asin !== undefined ? cleanText(req.body.asin, 20) : draft.asin,
    amazonUrl,
    req.body?.own_brand !== undefined ? (req.body.own_brand ? 1 : 0) : draft.own_brand,
    draft.id,
  );
  logEvent(db, draft.id, 'updated', null, actorOf(req));
  // ゲート必須項目 (公式URL等) を消したら ready_for_ai を draft に自動差し戻し (Codex R1 high)
  const demoted = demoteIfGateBroken(db, draft.id, actorOf(req));
  // 既存 Notion カードへ URL 項目を再同期 (fail-soft — 失敗しても保存は成功のまま)
  const notionSync = await syncCardLinks(draft.id, { actor: actorOf(req) });
  res.json({ ok: true, demoted: demoted || undefined, notion_sync: notionSync.outcome });
});

// ─── API: 参考URL / 画像 / 仕様表 / AI出力 ─────────────────

router.post('/api/drafts/:id/refs', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const url = cleanText(req.body?.url, 1000);
  if (!url || !isHttpUrl(url)) return res.status(400).json({ ok: false, error: 'URLの形式が不正です (http/https)' });
  const db = getDB();
  const info = db.prepare('INSERT INTO draft_reference_urls (draft_id, url) VALUES (?, ?)').run(draft.id, url);
  res.json({ ok: true, id: info.lastInsertRowid });
});

router.post('/api/drafts/:id/refs/:refId/delete', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  db.prepare('DELETE FROM draft_reference_urls WHERE id = ? AND draft_id = ?')
    .run(Number.parseInt(req.params.refId, 10) || 0, draft.id);
  res.json({ ok: true });
});

router.post('/api/drafts/:id/images', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const raw = cleanText(req.body?.url, 1000);
  if (!raw) return res.status(400).json({ ok: false, error: 'DriveリンクまたはファイルIDを入力してください' });
  const parsed = parseDriveLink(raw);
  if (!parsed) return res.status(400).json({ ok: false, error: 'Google Driveのリンクとして解釈できませんでした' });
  if (parsed.type === 'folder') {
    return res.status(400).json({
      ok: false,
      error: 'フォルダリンクです。フォルダ一括取り込みは今後対応予定なので、画像ファイル個別のリンクを貼ってください (フォルダURLは基本情報の「画像フォルダ」欄へ)',
    });
  }
  const db = getDB();
  const maxSort = db.prepare('SELECT COALESCE(MAX(sort), -1) AS m FROM draft_images WHERE draft_id = ?').get(draft.id).m;
  try {
    const info = db.prepare(`
      INSERT INTO draft_images (draft_id, drive_file_id, drive_url, sort) VALUES (?, ?, ?, ?)
    `).run(draft.id, parsed.id, isHttpUrl(raw) ? raw : null, maxSort + 1);
    res.json({ ok: true, id: info.lastInsertRowid, thumb: thumbnailUrl(parsed.id, 320) });
  } catch (e) {
    if (String(e.message).includes('UNIQUE')) {
      return res.status(409).json({ ok: false, error: 'この画像は既に追加されています' });
    }
    throw e;
  }
});

router.post('/api/drafts/:id/images/:imageId/delete', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  db.prepare('DELETE FROM draft_images WHERE id = ? AND draft_id = ?')
    .run(Number.parseInt(req.params.imageId, 10) || 0, draft.id);
  // 最後の画像を消したら ready_for_ai を draft に自動差し戻し (Codex R1 high)
  const demoted = demoteIfGateBroken(db, draft.id, actorOf(req));
  res.json({ ok: true, demoted: demoted || undefined });
});

// 画像サムネイルプロキシ: Drive のサムネイルを SA 経由で取得してアプリから返す。
// drive.google.com/thumbnail 直リンクはブラウザの Google セッション頼みで、
// サードパーティ Cookie ブロックや authuser 不一致 (複数アカウント) で 403 になるため。
router.get('/api/thumb/:fileId', async (req, res) => {
  const fileId = String(req.params.fileId || '');
  if (!DRIVE_FILE_ID_PATTERN.test(fileId)) return res.status(400).json({ ok: false, error: 'invalid file id' });
  // product-hub に画像として登録済みの ID だけ取得を許す
  // (SA は Drive を広く読めるため、無制限だと任意 ID を覗ける confused-deputy になる)
  if (!isKnownImageFileId(getDB(), fileId)) return res.status(404).json({ ok: false, error: 'unknown image' });
  const w = Number.parseInt(String(req.query.w || ''), 10);
  const width = THUMB_WIDTHS.includes(w) ? w : 320;
  try {
    const { buf } = await getDriveThumbnail(fileId, width);
    res.set('Content-Type', 'image/jpeg'); // sharp 再エンコード済み (service 側で固定)
    res.set('X-Content-Type-Options', 'nosniff');
    res.set('Cache-Control', 'private, max-age=86400');
    res.send(buf);
  } catch (e) {
    // 詳細はログのみ。<img> は壊れ表示のままにする (SA 権限なしはフォルダ取込時に別途エラーが出る)
    const upstream = e?.code || e?.response?.status;
    const status = upstream === 404 ? 404 : upstream === 403 ? 403 : 502;
    console.error(`[product-hub] thumb proxy failed (${status}):`, fileId, String(e?.message || e).slice(0, 300));
    res.status(status).json({ ok: false, error: 'thumbnail unavailable' });
  }
});

// 画像フォルダ一括取り込み: フォルダ内の「<商品コード>_番号」ファイルをスロットへ自動セット。
// _00 = 白抜き背景 (whiteBgImage) / _01〜_20 = 楽天商品画像 1〜20 (並び順 = sort)。
// 番号付き画像が1枚でも見つかったら draft_images は全置き換え (フォルダが正)。
router.post('/api/drafts/:id/images/import-folder', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const raw = cleanText(req.body?.url, 1000) || draft.drive_folder_url;
  if (!raw) return res.status(400).json({ ok: false, error: '画像フォルダのURLを入力してください' });
  if (!isHttpUrl(raw)) return res.status(400).json({ ok: false, error: 'URLの形式が不正です' });
  const parsed = parseDriveLink(raw);
  if (!parsed || parsed.type === 'file') {
    return res.status(400).json({ ok: false, error: 'Drive のフォルダリンクを貼ってください (https://drive.google.com/drive/folders/…)' });
  }
  let files;
  try {
    files = await listDriveFolderImages(parsed.id);
  } catch (e) {
    return res.status(502).json({
      ok: false,
      error: 'フォルダ一覧の取得に失敗しました。フォルダがサービスアカウントに共有されているか確認してください ('
        + String(e.message || e).slice(0, 200) + ')',
    });
  }
  const assigned = assignImageSlots(files, draft.ne_code);
  if (!assigned.codeMatched) {
    return res.status(400).json({
      ok: false,
      error: `「${draft.ne_code}_番号」で始まる画像が見つかりませんでした。ファイル名を「${draft.ne_code}_00 (白抜き)」「${draft.ne_code}_01〜_${MAX_IMAGE_SLOTS}」の形式にしてください`,
      skipped: assigned.skipped, conflicts: assigned.conflicts,
    });
  }
  // 同一番号の重複はどれが正か判断できないので、DBを触る前に止める (Codex R1 medium)
  if (assigned.conflicts.length > 0) {
    return res.status(400).json({
      ok: false,
      error: '同じ番号のファイルが複数あります。フォルダを整理してからやり直してください',
      skipped: assigned.skipped, conflicts: assigned.conflicts,
    });
  }
  if (!assigned.whiteBg && assigned.slots.length === 0) {
    return res.status(400).json({
      ok: false,
      error: `_00〜_${MAX_IMAGE_SLOTS} の番号付き画像が見つかりませんでした`,
      skipped: assigned.skipped, conflicts: assigned.conflicts,
    });
  }
  const db = getDB();
  const warnings = [];
  if (assigned.missingNums.length > 0) {
    warnings.push(`_${assigned.missingNums.map((n) => String(n).padStart(2, '0')).join(' / _')} が欠番です。楽天へは番号順に詰めて登録されます`);
  }
  if (!assigned.whiteBg) {
    const existingWb = db.prepare('SELECT white_bg_drive_file_id FROM draft_rakuten WHERE draft_id = ?').get(draft.id)?.white_bg_drive_file_id;
    if (existingWb) warnings.push(`フォルダに ${draft.ne_code}_00 (白抜き背景) が見つかりません。白抜き背景は以前の設定のまま残っています`);
  }
  // 入力されたフォルダURLは基本情報にも保存 (次回はワンクリックで再取込できる)
  applyFolderImport(db, draft.id, assigned, { folderUrl: raw, currentFolderUrl: draft.drive_folder_url });
  logEvent(db, draft.id, 'images_imported_from_folder',
    `画像${assigned.slots.length}枚 / 白抜き${assigned.whiteBg ? 'あり' : 'なし'}`, actorOf(req));
  // 置換で必須項目が壊れた場合は ready_for_ai を差し戻す (個別削除と同じ扱い)
  const demoted = demoteIfGateBroken(db, draft.id, actorOf(req));
  res.json({ ok: true, ...assigned, warnings, demoted: demoted || undefined });
});

router.post('/api/drafts/:id/specs', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const key = cleanText(req.body?.spec_key, 100);
  const value = cleanText(req.body?.spec_value, 500);
  if (!key) return res.status(400).json({ ok: false, error: '項目名を入力してください' });
  const db = getDB();
  const maxSort = db.prepare('SELECT COALESCE(MAX(sort), -1) AS m FROM draft_specs WHERE draft_id = ?').get(draft.id).m;
  const info = db.prepare('INSERT INTO draft_specs (draft_id, spec_key, spec_value, sort) VALUES (?, ?, ?, ?)')
    .run(draft.id, key, value, maxSort + 1);
  res.json({ ok: true, id: info.lastInsertRowid });
});

router.post('/api/drafts/:id/specs/:specId/delete', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  db.prepare('DELETE FROM draft_specs WHERE id = ? AND draft_id = ?')
    .run(Number.parseInt(req.params.specId, 10) || 0, draft.id);
  res.json({ ok: true });
});

router.post('/api/drafts/:id/ai-outputs', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const kind = String(req.body?.kind || '');
  if (!AI_OUTPUT_KINDS.includes(kind)) return res.status(400).json({ ok: false, error: 'invalid kind' });
  const content = cleanText(req.body?.content, 50000);
  const db = getDB();
  db.prepare(`
    INSERT INTO draft_ai_outputs (draft_id, kind, content, edited_by_human)
    VALUES (?, ?, ?, 1)
    ON CONFLICT(draft_id, kind) DO UPDATE SET content = excluded.content, edited_by_human = 1
  `).run(draft.id, kind, content);
  logEvent(db, draft.id, 'ai_output_edited', kind, actorOf(req));
  res.json({ ok: true });
});

// ─── API: Yahoo 向け追記項目 / 画像制作 (P1.5) ─────────────

router.post('/api/drafts/:id/yahoo', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  const b = req.body || {};
  // カテゴリIDは識別子: 完全一致の数字のみ受理 (Codex R1 medium: parseInt は "123abc" を通す)
  let catId;
  if (b.yahoo_category_id === undefined) catId = undefined;
  else if (b.yahoo_category_id === '' || b.yahoo_category_id === null) catId = null;
  else if (/^\d+$/.test(String(b.yahoo_category_id).trim())) catId = Number.parseInt(String(b.yahoo_category_id).trim(), 10);
  else return res.status(400).json({ ok: false, error: 'Yahoo!カテゴリIDは数値で入力してください' });
  // 金額: 非空の不正値は既存値クリアでなく 400 (Codex R1 low)
  for (const [key, label] of [['yahoo_price', 'Yahoo!売価'], ['yahoo_price_sagawa', 'Yahoo!売価(佐川)']]) {
    const v = b[key];
    if (v !== undefined && v !== null && String(v).trim() !== '' && !Number.isFinite(Number(v))) {
      return res.status(400).json({ ok: false, error: `${label}は数値で入力してください` });
    }
  }
  upsertDraftYahoo(db, draft.id, {
    yahoo_price: b.yahoo_price !== undefined ? sanitizeMoney(b.yahoo_price) : undefined,
    yahoo_price_sagawa: b.yahoo_price_sagawa !== undefined ? sanitizeMoney(b.yahoo_price_sagawa) : undefined,
    delivery_label: b.delivery_label !== undefined ? cleanText(b.delivery_label, 100) : undefined,
    tax_rate: b.tax_rate !== undefined ? cleanText(b.tax_rate, 20) : undefined,
    yahoo_category_id: catId,
    yahoo_path: b.yahoo_path !== undefined ? cleanText(b.yahoo_path, 500) : undefined,
  });
  logEvent(db, draft.id, 'yahoo_updated', null, actorOf(req));
  res.json({ ok: true });
});

router.post('/api/drafts/:id/image-production', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  if (!draft.own_brand) {
    return res.status(400).json({ ok: false, error: '画像制作情報は自社商品のみ登録できます (基本情報で「自社商品」をONにしてください)' });
  }
  const db = getDB();
  const b = req.body || {};
  const urlVal = b.camera_instruction_url !== undefined ? cleanText(b.camera_instruction_url, 1000) : undefined;
  if (urlVal && !isHttpUrl(urlVal)) {
    return res.status(400).json({ ok: false, error: '撮影指示URLの形式が不正です (http/https)' });
  }
  const clean = (v, len) => (v !== undefined ? cleanText(v, len) : undefined);
  upsertImageProduction(db, draft.id, {
    status: clean(b.status, 100),
    importance_tier: clean(b.importance_tier, 100),
    production_type: clean(b.production_type, 100),
    aplus_content: clean(b.aplus_content, 100),
    aplus_related: clean(b.aplus_related, 2000),
    camera_instruction_url: urlVal,
    shipping_status: clean(b.shipping_status, 100),
    reference_collection: clean(b.reference_collection, 100),
    designer: clean(b.designer, 100),
    page_composer: clean(b.page_composer, 100),
    request_text: clean(b.request_text, 10000),
  });
  logEvent(db, draft.id, 'image_production_updated', null, actorOf(req));
  res.json({ ok: true });
});

// ─── API: ステータス遷移 (§4 ゲート) ───────────────────────

router.post('/api/drafts/:id/status', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const to = String(req.body?.to || '');
  if (!canTransition(draft.status, to)) {
    return res.status(400).json({ ok: false, error: `${STATUS_LABELS[draft.status] || draft.status} から ${STATUS_LABELS[to] || to} へは遷移できません` });
  }
  const db = getDB();
  // review へ進むときも再チェック (ready_for_ai 到達後に必須項目が壊された場合のすり抜け防止)
  if (to === 'ready_for_ai' || to === 'review') {
    const reasons = gateReasons(db, draft);
    if (reasons.length > 0) {
      return res.status(400).json({ ok: false, error: '必須項目が未入力です', reasons });
    }
  }
  db.prepare(`
    UPDATE product_drafts SET status = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?
  `).run(to, draft.id);
  logEvent(db, draft.id, 'status_changed', `${draft.status} -> ${to}`, actorOf(req));
  res.json({ ok: true, status: to });
});

// ─── API: Notion カードリトライ ───────────────────────────

router.post('/api/drafts/:id/notion-retry', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const result = await attemptCardCreation(draft.id, { actor: actorOf(req) });
  res.json({ ok: result.outcome === 'created' || result.outcome === 'adopted_existing' || result.outcome === 'already_created', result });
});

router.post('/api/notion-retry-all', async (req, res) => {
  const results = await retryPendingCards({ actor: actorOf(req) });
  const failed = results.filter((r) => r.outcome === 'failed');
  res.json({ ok: failed.length === 0, tried: results.length, failed: failed.length, results });
});

// ─── API: 楽天出品 (P3) ──────────────────────────────────

// ジャンルID・商品属性・メーカー型番の保存 (手入力 — 中原さん決定)
router.post('/api/drafts/:id/rakuten', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  const genreId = cleanText(req.body?.genre_id, 20);
  // genre_only: ジャンルIDだけ更新 (AI提案/ツリー反映用 — 編集途中の他フィールドを巻き込まない)
  if (req.body?.genre_only === true) {
    if (!genreId || !/^\d+$/.test(genreId)) {
      return res.status(400).json({ ok: false, error: 'ジャンルIDは数字で入力してください' });
    }
    db.prepare(`
      INSERT INTO draft_rakuten (draft_id, genre_id) VALUES (?, ?)
      ON CONFLICT(draft_id) DO UPDATE SET
        genre_id = excluded.genre_id,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(draft.id, genreId);
    logEvent(db, draft.id, 'rakuten_genre_set', genreId, actorOf(req));
    return res.json({ ok: true });
  }

  if (genreId && !/^\d+$/.test(genreId)) {
    return res.status(400).json({ ok: false, error: 'ジャンルIDは数字で入力してください' });
  }
  // attributes は [{name, value}] で受けて [{name, values:[..]}] に正規化して保存
  let attributesJson = null;
  if (Array.isArray(req.body?.attributes)) {
    const rows = req.body.attributes
      .map((a) => ({ name: cleanText(a?.name, 100), value: cleanText(a?.value, 300) }))
      .filter((a) => a.name || a.value);
    for (const a of rows) {
      if (!a.name || !a.value) {
        return res.status(400).json({ ok: false, error: '属性は「属性名」と「値」の両方を入力してください (不要な行は両方空に)' });
      }
    }
    attributesJson = JSON.stringify(rows.map((a) => ({ name: a.name, values: [a.value] })));
    if (parseAttributes(attributesJson) === null) {
      return res.status(400).json({ ok: false, error: '属性の形式が不正です' });
    }
  }
  // 店舗内カテゴリ (複数選択)。フィールドが来たときだけ入れ替える
  let shopCategoryIds = null;
  if (req.body?.shop_category_ids !== undefined) {
    const sanitized = sanitizeShopCategoryIds(req.body.shop_category_ids);
    if (sanitized.error === 'not_array') {
      return res.status(400).json({ ok: false, error: '店舗内カテゴリの形式が不正です' });
    }
    if (sanitized.error === 'invalid_id') {
      return res.status(400).json({ ok: false, error: '店舗内カテゴリの指定が不正です' });
    }
    if (sanitized.error === 'too_many') {
      return res.status(400).json({ ok: false, error: `店舗内カテゴリは最大 ${MAX_DRAFT_SHOP_CATEGORIES} 件までです` });
    }
    const ids = sanitized.ids;
    // 検証条件は画面の選択肢と同じ = 有効 or このドラフトが既に選択中 (Codex R1 medium)
    if (countSelectableShopCategories(db, draft.id, ids) !== ids.length) {
      return res.status(400).json({ ok: false, error: '選べない店舗内カテゴリが指定されました。画面を再読み込みしてください' });
    }
    shopCategoryIds = ids;
  }
  // 配送・納期 (2026-07-27 仕様: 公開に必要な情報はアプリで持つ)
  const shippingGroup = cleanText(req.body?.shipping_method_group, 10);
  if (shippingGroup && !SHIPPING_METHOD_GROUPS[shippingGroup]) {
    return res.status(400).json({ ok: false, error: '配送方法の指定が不正です' });
  }
  let postageIncluded = null;
  if (req.body?.postage_included === '1' || req.body?.postage_included === 1) postageIncluded = 1;
  else if (req.body?.postage_included === '0' || req.body?.postage_included === 0) postageIncluded = 0;
  const deliveryDateId = cleanText(req.body?.normal_delivery_date_id, 10);
  if (deliveryDateId && !/^\d+$/.test(deliveryDateId)) {
    return res.status(400).json({ ok: false, error: '納期情報IDは数字で入力してください' });
  }
  // 白抜き背景画像 (Drive リンク)
  const whiteBgRaw = cleanText(req.body?.white_bg_url, 1000);
  let whiteBgFileId = null;
  if (whiteBgRaw) {
    const parsed = parseDriveLink(whiteBgRaw);
    if (!parsed || parsed.type === 'folder') {
      return res.status(400).json({ ok: false, error: '白抜き背景画像は Drive のファイルリンクを貼ってください' });
    }
    whiteBgFileId = parsed.id;
  }

  const articleNumber = cleanText(req.body?.article_number, 100);
  db.transaction(() => {
    db.prepare(`
      INSERT INTO draft_rakuten (draft_id, genre_id, attributes_json, article_number,
        shipping_method_group, postage_included, normal_delivery_date_id,
        white_bg_drive_file_id, white_bg_drive_url)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(draft_id) DO UPDATE SET
        genre_id = excluded.genre_id,
        attributes_json = excluded.attributes_json,
        article_number = excluded.article_number,
        shipping_method_group = excluded.shipping_method_group,
        postage_included = excluded.postage_included,
        normal_delivery_date_id = excluded.normal_delivery_date_id,
        white_bg_drive_file_id = excluded.white_bg_drive_file_id,
        white_bg_drive_url = excluded.white_bg_drive_url,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(draft.id, genreId, attributesJson, articleNumber,
      shippingGroup, postageIncluded, deliveryDateId, whiteBgFileId, whiteBgRaw);
    if (shopCategoryIds !== null) {
      setDraftShopCategories(db, draft.id, shopCategoryIds);
      // 店舗内カテゴリが確定した記録 (AI 初期設定の「一度だけ」判定に使う。0件保存も「人が外した」意思表示)。
      // 選択の入れ替えと同一トランザクションに置く (Codex R2: マーカーと実体の整合)
      logEvent(db, draft.id, 'shop_categories_saved', `${shopCategoryIds.length}件`, actorOf(req));
    }
  })();
  logEvent(db, draft.id, 'rakuten_fields_saved',
    [genreId, shopCategoryIds !== null ? `店舗内カテゴリ${shopCategoryIds.length}件` : null].filter(Boolean).join(' / ') || null,
    actorOf(req));
  res.json({ ok: true });
});

// 店舗内カテゴリ一覧を RMS から直接同期する (2026-08-02、Category API 2.0)。
// 貼り付け取り込みと同じ replaceShopCategories を使うので激減ガードもそのまま効く。
router.post('/api/shop-categories/sync', async (req, res) => {
  if (req.session?.role !== 'admin') {
    return res.status(403).json({ ok: false, error: '店舗内カテゴリの同期は管理者のみです' });
  }
  let fetched;
  try {
    // force 再送 (too_few の確認後) は取り直さない — 人が見て承認した内容をそのまま適用する
    // (再取得すると確認した件数と実際に入る内容がズレる。Codex R1)
    fetched = await fetchShopCategoryTree({ force: req.body?.refresh === true && req.body?.force !== true });
  } catch (e) {
    return res.status(502).json({ ok: false, error: `楽天への接続に失敗しました: ${String(e.message || e).slice(0, 200)}` });
  }
  if (!fetched.ok) return res.status(502).json({ ok: false, error: fetched.error });
  // 複数カテゴリセット (メガプラン) は同名パスで categoryId が割れるため、黙って1つに決めない
  if (fetched.trees.length > 1) {
    return res.status(400).json({
      ok: false,
      error: `カテゴリセットが ${fetched.trees.length} 個あります。どのセットを使うか決める必要があるため、自動同期を中止しました (中原さんに相談してください)`,
    });
  }
  const { rows, duplicates, skipped, truncated } = flattenCategoryTrees(fetched.trees);
  if (truncated) {
    return res.status(502).json({
      ok: false,
      error: `店舗内カテゴリが上限 (${MAX_SHOP_CATEGORY_LINES} 件) を超えました。一部だけ取り込むとマスタが欠けるため中止しました`,
    });
  }
  if (rows.length === 0) {
    return res.status(502).json({ ok: false, error: '楽天から店舗内カテゴリを1件も取得できませんでした' });
  }
  // 承認 (force) は「人が件数を見て確認した、その内容」にだけ効かせる (Codex R2 high)。
  // 確認と再送の間に再取得が走って内容が変わっていたら適用せず、やり直してもらう
  const snapshot = shopCategorySnapshotHash(rows);
  const isApproval = req.body?.force === true;
  if (isApproval && req.body?.expectedSnapshot !== snapshot) {
    return res.status(409).json({
      ok: false, error: 'changed',
      message: '確認したあとに楽天側の内容が変わりました。取り込みからやり直してください',
    });
  }
  const r = replaceShopCategories(getDB(), rows, { force: isApproval });
  if (r.error === 'too_few') {
    return res.status(400).json({
      ok: false, error: 'too_few', active: r.active, incoming: r.incoming, expectedSnapshot: snapshot,
      message: `取得した件数 (${r.incoming}件) が現在の一覧 (${r.active}件) の半分未満です。楽天側でカテゴリを整理したのでなければ、取得が不完全な可能性があります`,
    });
  }
  res.json({
    ok: true, imported: rows.length, duplicates, skipped: skipped.length,
    skippedDetail: skipped.slice(0, 20),
    active: r.active, deactivated: r.deactivated, source: 'rms',
  });
});

// 店舗内カテゴリ一覧の取り込み (貼り付け・全置き換え)。マスタ全体に影響するので admin 限定。
// 通常は上の /sync (RMS から直接) を使う。こちらは API が使えないときのフォールバック。
router.post('/api/shop-categories/import', (req, res) => {
  if (req.session?.role !== 'admin') {
    return res.status(403).json({ ok: false, error: '店舗内カテゴリ一覧の取り込みは管理者のみです' });
  }
  const parsed = parseShopCategoryText(req.body?.text);
  if (!parsed.ok) return res.status(400).json({ ok: false, error: parsed.error });
  if (parsed.rows.length === 0) {
    return res.status(400).json({ ok: false, error: 'カテゴリが1件も読み取れませんでした' });
  }
  const r = replaceShopCategories(getDB(), parsed.rows, { force: req.body?.force === true });
  if (r.error === 'too_few') {
    // 貼り付けミスでマスタが激減する事故ガード (High-4)。UI 側が再確認のうえ force で再送する
    return res.status(400).json({
      ok: false, error: 'too_few', active: r.active, incoming: r.incoming,
      message: `取り込み件数 (${r.incoming}件) が現在の一覧 (${r.active}件) の半分未満です。貼り付け漏れがないか確認してください`,
    });
  }
  res.json({ ok: true, imported: parsed.rows.length, duplicates: parsed.duplicates, active: r.active, deactivated: r.deactivated });
});

// Drive 画像 → R-Cabinet 転送 (冪等。転送済みはスキップ)
router.post('/api/drafts/:id/rakuten/transfer-images', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  try {
    const r = await transferImagesToCabinet(draft.id, { actor: actorOf(req) });
    if (r.error === 'no_images') {
      return res.status(400).json({ ok: false, error: '商品画像 (Driveリンク) を先に追加してください' });
    }
    res.json(r);
  } catch (e) {
    console.error('[product-hub] cabinet transfer failed:', e);
    res.status(502).json({ ok: false, error: String(e.message || e).slice(0, 300) });
  }
});

// 送信内容のプレビュー (dry run — RMS には送らない)
// ジャンルツリー: 子ジャンル一覧 (IchibaGenre/Search。genre_id=0 が最上位)。
// RMS ナビゲーションAPI は root 以外の子を返さないため公開APIを使う (ID体系は同一)
router.get('/api/rakuten/genre-children', async (req, res) => {
  const genreId = cleanText(req.query?.genre_id, 12) || '0';
  try {
    const r = await fetchGenreChildren(genreId);
    if (!r.ok) return res.status(r.needsSetup ? 503 : 502).json({ ok: false, error: r.error, needsSetup: r.needsSetup === true });
    res.json({ ok: true, current: r.current, parents: r.parents, children: r.children, path: genrePathOf(r) });
  } catch (e) {
    const status = e?.statusCode === 503 ? 503 : 502;
    res.status(status).json({ ok: false, error: String(e.message || e).slice(0, 200) });
  }
});

// ジャンル候補の自動提案 (AI初期値): 商品名で楽天市場を検索し上位ヒットのジャンル多数決
router.get('/api/drafts/:id/rakuten/genre-suggest', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  try {
    const r = await suggestGenreByName(draft.name);
    if (!r.ok) return res.status(502).json({ ok: false, error: r.error });
    // 候補のフルパスは RMS 属性辞書 (miniPC 経由・DB キャッシュあり) で補完する。
    // 旧ホストのジャンルAPIに依存しない + 末端でないジャンルは 404 になるので自然に落ちる
    const db = getDB();
    // miniPC 障害時に画面表示を道連れにしない (Codex high):
    //   ①鮮度を問わず DB キャッシュを先に使う ②未キャッシュ分だけ短い timeout (10s) で並列取得
    const candidates = await Promise.all((r.candidates || []).map(async (c) => {
      const cached = getCachedGenreAttributes(db, c.genreId);
      if (cached) return { genreId: c.genreId, count: c.count, path: cached.genrePath || cached.genreName || c.genreId };
      try {
        const g = await fetchGenreAttributes(db, c.genreId, { timeoutMs: 10_000 });
        if (g.ok) return { genreId: c.genreId, count: c.count, path: g.genre.genrePath || g.genre.genreName || c.genreId };
      } catch (_) { /* miniPC 不達などはその候補だけ落とす */ }
      return null;
    }));
    res.json({ ok: true, candidates: candidates.filter(Boolean) });
  } catch (e) {
    const status = e?.statusCode === 503 ? 503 : 502;
    res.status(status).json({ ok: false, error: String(e.message || e).slice(0, 200) });
  }
});

// 店舗内カテゴリの AI 初期候補 (2026-08-02): ジャンルフルパス+商品名とカテゴリパスの語彙マッチ。
// 外部 API なし (マスタとキャッシュ済みジャンル辞書だけで即答)。
// GET (提案) と POST (自動適用の直前検証) の両方から呼ぶ = 同じ材料・同じ計算を保証する
function computeShopCategoryCandidates(db, draft) {
  const cats = listShopCategoriesForDraft(db, draft.id);
  if (cats.length === 0) return null;
  const genreId = db.prepare('SELECT genre_id FROM draft_rakuten WHERE draft_id = ?').get(draft.id)?.genre_id;
  const genrePath = genreId ? (getCachedGenreAttributes(db, genreId)?.genrePath || '') : '';
  return suggestShopCategories(cats, { name: draft.name, genrePath });
}

router.get('/api/drafts/:id/rakuten/shop-category-suggest', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  const candidates = computeShopCategoryCandidates(db, draft);
  if (candidates === null) return res.json({ ok: true, candidates: [], reason: 'no_master' });
  // 自動セットは「まだ一度も店舗内カテゴリが保存されていないドラフト」だけ。
  // これが無いと、人が意図的に全部外してもページを開き直すたびに AI が入れ直してしまう
  // (最終判定は POST 側のトランザクション内でもう一度行う)
  const neverSaved = shopCategoriesNeverSaved(db, draft.id);
  // 自動適用の可否はサーバーで確定させる (画面側にしきい値ロジックを持たせない)
  res.json({
    ok: true, candidates,
    everSaved: !neverSaved,
    autoApply: neverSaved && canAutoApplyShopCategory(candidates),
  });
});

// 店舗内カテゴリだけの部分保存 (AI自動適用・候補ボタン用。draft_rakuten の他カラムは触らない)
router.post('/api/drafts/:id/shop-categories', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const sanitized = sanitizeShopCategoryIds(req.body?.ids);
  if (sanitized.error) {
    return res.status(400).json({ ok: false, error: `店舗内カテゴリの指定が不正です (${sanitized.error})` });
  }
  const db = getDB();
  const ids = sanitized.ids;
  // 検証条件は画面の選択肢 (listShopCategoriesForDraft) と同じ = 有効 or 既に選択中 (Codex R1 medium)
  if (countSelectableShopCategories(db, draft.id, ids) !== ids.length) {
    return res.status(400).json({ ok: false, error: '選べない店舗内カテゴリが指定されました。画面を再読み込みしてください' });
  }
  // AI の自動適用 (人の操作を伴わない書き込み) だけは、トランザクション内で
  // 「まだ一度も保存されていない」ことを再確認する (Codex R2 high: GET と POST の間に
  // 人が「全部外して保存」していると、遅れて届いた自動適用がその意思を上書きしてしまう)。
  // 候補ボタン (ai_candidate) は人の明示操作なのでこの制約の対象外
  const isAutoApply = req.body?.source === 'ai';
  let skipped = null;
  db.transaction(() => {   // 全削除→INSERT の途中失敗で空にしない (Codex R1 low)
    if (isAutoApply) {
      if (!shopCategoriesNeverSaved(db, draft.id)) { skipped = 'already_saved'; return; }
      // 候補取得後にジャンルが変わっていないか (Codex R3 high: 旧ジャンル由来の候補を保存すると
      // 保存イベントが立ち、新ジャンルでの再提案まで塞いでしまう)
      if (!isAutoApplyRequestValid(computeShopCategoryCandidates(db, draft) || [], ids)) {
        skipped = 'stale_suggestion';
        return;
      }
    }
    setDraftShopCategories(db, draft.id, ids);
    logEvent(db, draft.id, 'shop_categories_saved',
      `${ids.length}件${isAutoApply ? ' (AI初期設定)' : ''}`, actorOf(req));
  })();
  if (skipped) return res.json({ ok: true, skipped });
  res.json({ ok: true, count: ids.length });
});

// ジャンル属性辞書の取得 (Genre API)。取得結果は ph_genre_attributes にキャッシュされ、
// buildItemPayload の事前検証 (IE1002防止・必須属性チェック・カタログID自動付与) に効く
router.get('/api/rakuten/genre-attributes', async (req, res) => {
  const genreId = cleanText(req.query?.genre_id, 12);
  if (!genreId) return res.status(400).json({ ok: false, error: 'genre_id が必要です' });
  try {
    const r = await fetchGenreAttributes(getDB(), genreId, { force: req.query?.refresh === '1' });
    if (!r.ok) {
      if (r.notFound) {
        return res.status(404).json({ ok: false, error: 'このジャンルIDは見つかりません (末端ジャンルのIDか確認してください)' });
      }
      return res.status(502).json({ ok: false, error: r.error || 'ジャンル情報の取得に失敗しました' });
    }
    res.json({ ok: true, genre: r.genre, cached: r.cached === true });
  } catch (e) {
    console.error('[product-hub] genre-attributes failed:', e);
    res.status(502).json({ ok: false, error: 'ジャンル情報の取得に失敗しました (miniPC 接続を確認)' });
  }
});

// 楽天の説明文3欄 (PC用商品説明文/スマホ用/PC用販売説明文) の最終形プレビュー (2026-08-03)。
// 登録要件が未達でも現時点の素材で組み立てる (「楽天と同じ項目をアプリにも」への回答)
router.get('/api/drafts/:id/rakuten/desc-preview', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const r = buildDescriptionPreview(getDB(), draft.id);
  if (!r.ok) return res.status(404).json(r);
  res.json(r);
});

router.get('/api/drafts/:id/rakuten/preview', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  // プレビューでも辞書の鮮度を回復 (24h 以内なら通信なし。失敗しても検証スキップで続行)
  const rkRow = db.prepare('SELECT genre_id FROM draft_rakuten WHERE draft_id = ?').get(draft.id);
  if (rkRow?.genre_id && /^\d+$/.test(String(rkRow.genre_id).trim())) {
    try { await fetchGenreAttributes(db, String(rkRow.genre_id).trim()); } catch (_) { /* best-effort */ }
  }
  const built = buildItemPayload(db, draft.id);
  if (!built.ok) return res.json({ ok: false, reasons: built.reasons });
  res.json({
    ok: true, manageNumber: String(draft.ne_code).toLowerCase(), payload: built.payload,
    // 店舗内カテゴリは RMS payload に含まれない (Category API が別 = miniPC 対応待ち)。
    // 公開時に RMS 画面で設定するものとしてプレビューに添える
    shopCategories: selectedShopCategoryPaths(db, draft.id),
  });
});

// 非公開 (倉庫指定) で楽天に登録。公開は人が RMS 画面で行う
router.post('/api/drafts/:id/rakuten/register', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  if (req.body?.confirm !== true) {
    return res.status(400).json({ ok: false, error: 'confirm が必要です' });
  }
  try {
    const r = await registerHidden(draft.id, { actor: actorOf(req) });
    if (!r.ok) return res.status(400).json({ ok: false, error: r.error || (r.reasons || []).join(' / '), reasons: r.reasons });
    res.json(r);
  } catch (e) {
    console.error('[product-hub] rakuten register failed:', e);
    res.status(502).json({ ok: false, error: String(e.message || e).slice(0, 300) });
  }
});

// 店舗内カテゴリ (お店の棚) を RMS へ反映 (2026-08-02、item-mappings API)。
// 登録時に自動で走るが、失敗したときや棚を選び直したときに手動で再実行する
router.post('/api/drafts/:id/rakuten/sync-shop-categories', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  try {
    const r = await syncShopCategoriesToRms(draft.id, { actor: actorOf(req) });
    if (!r.ok) return res.status(400).json({ ok: false, error: r.error });
    res.json(r);
  } catch (e) {
    console.error('[product-hub] shop-category mapping failed:', e);
    res.status(502).json({ ok: false, error: String(e.message || e).slice(0, 300) });
  }
});

// 公開/非公開の切り替え (アプリから登録した商品のみ。miniPC の visibility ルート経由)
router.post('/api/drafts/:id/rakuten/visibility', async (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  if (req.body?.confirm !== true) {
    return res.status(400).json({ ok: false, error: 'confirm が必要です' });
  }
  // 公開操作は fail-closed に: hide が boolean でない (欠落・"true" 等) なら拒否 (Codex R1 High-3)
  if (typeof req.body?.hide !== 'boolean') {
    return res.status(400).json({ ok: false, error: 'hide (true/false) が必要です' });
  }
  try {
    const r = await setItemVisibility(draft.id, { hide: req.body.hide, actor: actorOf(req) });
    if (!r.ok) return res.status(400).json(r);
    res.json(r);
  } catch (e) {
    console.error('[product-hub] rakuten visibility failed:', e);
    res.status(502).json({ ok: false, error: String(e.message || e).slice(0, 300) });
  }
});

// ─── API: 商品ページ表記 (化粧品・食品) ──────────────────

router.post('/api/drafts/:id/page-info', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const db = getDB();
  const b = req.body || {};
  const productType = Object.keys(PRODUCT_TYPES).includes(b.product_type) ? b.product_type : 'general';
  const categoryLabel = cleanText(b.category_label, 50);
  if (categoryLabel && !CATEGORY_LABELS.includes(categoryLabel)) {
    return res.status(400).json({ ok: false, error: '商品区分は選択肢から選んでください' });
  }
  const originType = ['日本製', '海外製'].includes(b.origin_type) ? b.origin_type : null;
  const vals = {
    product_type: productType,
    content_volume: cleanText(b.content_volume, 200),
    size_text: cleanText(b.size_text, 300),
    ingredients: cleanText(b.ingredients, 3000),
    usage_notes: cleanText(b.usage_notes, 2000),
    origin_type: originType,
    origin_country: cleanText(b.origin_country, 100),
    category_label: categoryLabel,
    seller_name: cleanText(b.seller_name, 200),
    importer_name: cleanText(b.importer_name, 200),
    food_name: cleanText(b.food_name, 200),
    food_ingredients: cleanText(b.food_ingredients, 3000),
    food_expiry: cleanText(b.food_expiry, 200),
    food_storage: cleanText(b.food_storage, 300),
  };
  // リビジョン (#691 自動保存): 同一ページロード (save_token 一致) 内で seq が進んでいない
  // リクエスト = 追い越された古い保存。到着順に依存させず古い方を破棄する
  // (トークン無し・別ページロード・別ユーザーは従来どおり後勝ち)
  const saveToken = cleanText(b.save_token, 64);
  const saveSeq = Number.isSafeInteger(b.save_seq) && b.save_seq >= 0 ? b.save_seq : null;
  const existing = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(draft.id);
  const isStale = !!existing && saveToken !== null && saveSeq !== null
    && existing.save_token === saveToken && existing.save_seq !== null && existing.save_seq >= saveSeq;
  // 自動保存が同じ内容を送ってきたときは、書き込みも履歴 (draft_events) も増やさない
  const unchanged = !!existing && Object.entries(vals).every(([k, v]) => (existing[k] ?? null) === v);
  let info;
  if (isStale) {
    info = existing;
  } else if (unchanged) {
    // seq だけは前進させる (以後さらに古いリクエストが来ても stale 判定できるように)
    if (saveToken !== null && saveSeq !== null) {
      db.prepare('UPDATE draft_page_info SET save_token = ?, save_seq = ? WHERE draft_id = ?')
        .run(saveToken, saveSeq, draft.id);
    }
    info = existing;
  } else {
    db.prepare(`
      INSERT INTO draft_page_info (
        draft_id, product_type, content_volume, size_text, ingredients, usage_notes,
        origin_type, origin_country, category_label, seller_name, importer_name,
        food_name, food_ingredients, food_expiry, food_storage, save_token, save_seq
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(draft_id) DO UPDATE SET
        product_type = excluded.product_type,
        content_volume = excluded.content_volume, size_text = excluded.size_text,
        ingredients = excluded.ingredients, usage_notes = excluded.usage_notes,
        origin_type = excluded.origin_type, origin_country = excluded.origin_country,
        category_label = excluded.category_label, seller_name = excluded.seller_name,
        importer_name = excluded.importer_name,
        food_name = excluded.food_name, food_ingredients = excluded.food_ingredients,
        food_expiry = excluded.food_expiry, food_storage = excluded.food_storage,
        save_token = excluded.save_token, save_seq = excluded.save_seq,
        updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(
      draft.id, vals.product_type,
      vals.content_volume, vals.size_text, vals.ingredients, vals.usage_notes,
      vals.origin_type, vals.origin_country,
      vals.category_label, vals.seller_name, vals.importer_name,
      vals.food_name, vals.food_ingredients, vals.food_expiry, vals.food_storage,
      saveToken, saveSeq,
    );
    logEvent(db, draft.id, 'page_info_saved', productType, actorOf(req));
    info = db.prepare('SELECT * FROM draft_page_info WHERE draft_id = ?').get(draft.id);
  }
  const neShip = mapNeShippingToRakuten(db, getNeCost(db, draft.ne_code)?.shippingMethod);
  res.json({
    ok: true,
    ...(isStale ? { stale: true } : {}),
    warnings: validatePageInfo(info),
    html: buildPageInfoHtml({
      productName: draft.name, info,
      shippingLabel: rakutenShippingLabelOf(db, draft.id) ?? neShip.label,
    }),
  });
});

// NE配送方法 ↔ 楽天配送方法グループのマッピング (admin)
router.get('/api/shipping-method-map', (req, res) => {
  if (req.session?.role !== 'admin') return res.status(403).json({ ok: false, error: 'admin のみ操作できます' });
  res.json({ ok: true, rows: listShippingMethodMap(getDB()), groups: SHIPPING_METHOD_GROUPS });
});

router.post('/api/shipping-method-map', (req, res) => {
  if (req.session?.role !== 'admin') return res.status(403).json({ ok: false, error: 'admin のみ操作できます' });
  if (!Array.isArray(req.body?.entries) || req.body.entries.length > 200) {
    return res.status(400).json({ ok: false, error: 'entries (最大200件) が必要です' });
  }
  const saved = saveShippingMethodMap(getDB(), req.body.entries);
  logEvent(getDB(), 0, 'shipping_map_saved', `${saved}件`, actorOf(req));
  res.json({ ok: true, saved });
});

// ─── API: バリエーション ────────────────────────────────

// NE 判定のプレビュー (新規登録画面が入力中に問い合わせる。読み取り専用)
router.get('/api/variation', (req, res) => {
  const code = cleanText(req.query?.ne_code, 100);
  if (!code) return res.status(400).json({ ok: false, error: 'ne_code が必要です' });
  const db = getDB();
  const info = resolveVariationGroup(db, code);
  res.json({
    ok: true,
    kind: info.kind, groupKey: info.groupKey, isChild: info.isChild,
    memberCount: info.memberCount,
    members: info.members.slice(0, 60).map((m) => ({ code: m.商品コード, name: m.商品名, status: m.取扱区分 })),
    // 新規登録画面が初期値として使う (商品名・税率のみ)
    neDefaults: resolveNeDefaults(db, code),
  });
});

// SKU をこのドラフト (楽天1ページ) のバリエーションから外す。
// 外した SKU を別ページにしたい場合は、そのコードで新規登録する (自動まとめはスキップされる)。
router.post('/api/drafts/:id/variation/exclude', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const code = cleanText(req.body?.ne_code, 100);
  if (!code) return res.status(400).json({ ok: false, error: 'ne_code が必要です' });
  const db = getDB();
  // 判定・所属確認・INSERT・監査ログを 1 トランザクションに閉じる (Codex medium)
  const r = db.transaction(() => {
    const v = resolveVariationGroup(db, draft.ne_code, { draftId: draft.id });
    if (v.kind !== 'variation') {
      return { code: 400, error: 'このドラフトはバリエーションではありません' };
    }
    // ドラフト自身の商品コード (代表コード) は外せない
    if (code.trim().toLowerCase() === String(draft.ne_code).trim().toLowerCase()) {
      return { code: 400, error: 'このドラフト自身の商品コードは外せません' };
    }
    // グループに属さないコードを外したことにしない (入力ミス・古い画面からの誤操作を弾く)
    const inGroup = [...v.members, ...v.excludedMembers]
      .some((m) => String(m.商品コード).trim().toLowerCase() === code.trim().toLowerCase());
    if (!inGroup) {
      return { code: 409, error: `${code} はこのバリエーションに含まれていません。画面を再読み込みしてください` };
    }
    try {
      db.prepare('INSERT INTO draft_variation_exclusions (draft_id, ne_code, actor) VALUES (?, ?, ?)')
        .run(draft.id, code, actorOf(req));
    } catch (e) {
      // SKU 単位のグローバル UNIQUE。既に外れているなら成功扱い (冪等)
      if (/UNIQUE/i.test(String(e && e.message))) return { code: 200, already: true };
      throw e;
    }
    logEvent(db, draft.id, 'variation_sku_excluded', code, actorOf(req));
    return { code: 200 };
  })();
  if (r.code !== 200) return res.status(r.code).json({ ok: false, error: r.error });
  res.json({ ok: true });
});

// 外した SKU をバリエーションに戻す
router.post('/api/drafts/:id/variation/restore', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const code = cleanText(req.body?.ne_code, 100);
  if (!code) return res.status(400).json({ ok: false, error: 'ne_code が必要です' });
  const db = getDB();
  // 「単独ドラフトの有無を見る」と「除外を消す」を 1 トランザクションにする (Codex high-1)。
  // 分けると、確認と削除の間に同じ SKU の単独ドラフトが作られ、代表ページと単独ページの
  // 両方に同じ SKU が載る (二重掲載) 余地が残る
  const r = db.transaction(() => {
    const separate = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?')
      .get(code.trim().toLowerCase());
    if (separate) {
      return {
        code: 409,
        error: `${code} は単独のドラフト (#${separate.id}) として登録されています。先にそちらを削除してください`,
      };
    }
    const info = db.prepare('DELETE FROM draft_variation_exclusions WHERE draft_id = ? AND LOWER(TRIM(ne_code)) = ?')
      .run(draft.id, code.trim().toLowerCase());
    if (info.changes > 0) logEvent(db, draft.id, 'variation_sku_restored', code, actorOf(req));
    return { code: 200 };
  })();
  if (r.code !== 200) return res.status(r.code).json({ ok: false, error: r.error });
  res.json({ ok: true });
});

// 子SKUで作られたドラフトを代表商品コードにまとめる (人が確認してから実行する)。
// 判定・照合・更新・監査ログは services/regroup.js が 1 トランザクションで行う
router.post('/api/drafts/:id/regroup', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  const r = regroupToRepCode(getDB(), draft.id, {
    expectedFrom: cleanText(req.body?.expected_from, 100),
    expectedTo: cleanText(req.body?.expected_to, 100),
    actor: actorOf(req),
  });
  if (r.code !== 200) return res.status(r.code).json({ ok: false, error: r.error });
  res.json({ ok: true, ne_code: r.ne_code });
});

// ─── API: NE商品コードからの一括登録 / 新商品の自動取込 ──────

// 商品コードを貼り付けて一括でドラフトを作る。バリエーションは代表コード単位にまとまる。
// Notion カードは作らない (2026-07-25 方針)。
router.post('/api/register-codes', (req, res) => {
  const codes = parseNeCodes(req.body?.codes);
  if (codes.length === 0) return res.status(400).json({ ok: false, error: '商品コードを1件以上入力してください' });
  if (codes.length > MAX_REGISTER_CODES) {
    return res.status(400).json({ ok: false, error: `一度に登録できるのは ${MAX_REGISTER_CODES} 件までです (指定: ${codes.length} 件)` });
  }
  const r = registerByCodes(codes, { actor: actorOf(req), dryRun: req.body?.dry_run === true });
  if (!r.ok) {
    const msg = {
      mirror_unavailable: 'NE商品マスタを参照できません (時間をおいて再試行してください)',
      ne_unique_not_enforced: '商品コードの重複 (大文字小文字違い) があるため登録を停止しています。管理者にご連絡ください',
      no_codes: '商品コードを1件以上入力してください',
      too_many_codes: `一度に登録できるのは ${MAX_REGISTER_CODES} 件までです`,
    }[r.error] || r.error;
    return res.status(400).json({ ok: false, error: msg });
  }
  res.json(r);
});

// 新商品の自動取込を手動で走らせる (cron と同じ処理)。
// 初回シード = カットオフの確定は影響が大きいので admin 限定 (Codex 補足指摘)
router.post('/api/intake/run', (req, res) => {
  if (req.session?.role !== 'admin') {
    return res.status(403).json({ ok: false, error: '自動取込の実行は管理者のみです' });
  }
  const r = syncNewProducts({ dryRun: req.body?.dry_run === true, actor: actorOf(req) });
  if (!r.ok) {
    const msg = {
      mirror_unavailable: 'NE商品マスタを参照できません',
      mirror_empty: 'NE商品マスタが空です (miniPCからの同期待ち)',
      mirror_too_small: `NE商品マスタの件数が少なすぎます (${r.singles}件 < ${r.minRequired}件)。同期途中の可能性があるため実行しません`,
      ne_unique_not_enforced: '商品コードの重複 (大文字小文字違い) があるため停止しています',
    }[r.error] || r.error;
    return res.status(400).json({ ok: false, error: msg });
  }
  res.json(r);
});

// ─── API: Notion 取り込み (検証用の選択インポート) ──────────
// 商品コード指定で Notion 既存カードを取り込む。**読み取り専用** (Notion へ書き戻さない)。
// 取り込んだ行は source='notion_import' で印が付き、notion-card.js の同期経路から外れる。

router.post('/api/notion-import', async (req, res) => {
  const codes = parseNeCodes(req.body?.codes);
  if (codes.length === 0) {
    return res.status(400).json({ ok: false, error: '商品コードを1件以上入力してください' });
  }
  if (codes.length > MAX_IMPORT_CODES) {
    return res.status(400).json({ ok: false, error: `一度に取り込めるのは ${MAX_IMPORT_CODES} 件までです (指定: ${codes.length} 件)` });
  }
  try {
    const { results, summary } = await importFromNotion(codes, { actor: actorOf(req) });
    res.json({ ok: summary.failed === 0, summary, results });
  } catch (e) {
    // Notion env 未設定 (fail-closed) 等。取り込みは検証機能なので落ちても他機能に影響させない。
    // 詳細はサーバーログに残し、クライアントには内部情報を返さない (Codex R1 low-7)
    console.error('[product-hub] notion-import failed:', e);
    res.status(500).json({ ok: false, error: '取り込みに失敗しました (詳細はサーバーログを確認してください)' });
  }
});

// 取り込んだテストデータの掃除。**取り込み由来だけ**削除可 (ポータル起点の商品は消させない)。
// Notion 側のカードには一切触らない (ポータル DB の行を消すだけ)。
router.post('/api/drafts/:id/delete', (req, res) => {
  const draft = loadDraftOr404(req, res);
  if (!draft) return;
  if (!isNotionImported(draft)) {
    return res.status(400).json({ ok: false, error: '削除できるのはNotion取り込み由来のドラフトだけです' });
  }
  const db = getDB();
  // draft_events は append-only (削除 trigger) なので消さない。孤児として監査ログに残す
  db.prepare('DELETE FROM product_drafts WHERE id = ?').run(draft.id);
  res.json({ ok: true, deleted: draft.ne_code });
});

export default router;

// ─── service-api (P2 スキル接続用、トークン認証) ─────────────
// 中原さんの PC の Claude Code (/rakuten-title スキル) がここを叩く。
// 認証: Authorization: Bearer <PH_SERVICE_TOKEN>。env 未設定なら 503 (fail-closed)。
// mount: server.js で /apps/product-hub/service-api (セッション認証の外)

function requireServiceToken(req, res, next) {
  const expected = process.env.PH_SERVICE_TOKEN;
  if (!expected || !expected.trim()) {
    return res.status(503).json({ ok: false, error: 'PH_SERVICE_TOKEN not configured (fail-closed)' });
  }
  const got = String(req.headers.authorization || '').replace(/^Bearer\s+/i, '');
  const a = Buffer.from(got);
  const b = Buffer.from(expected.trim());
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) {
    return res.status(401).json({ ok: false, error: 'unauthorized' });
  }
  next();
}

export const serviceApiRouter = express.Router();
serviceApiRouter.use(express.json({ limit: '1mb' }));
serviceApiRouter.use(requireServiceToken);

// 生成待ち一覧 (AI 生成の材料つき、読み取り専用 = プレビュー用)
serviceApiRouter.get('/generation-queue', (req, res) => {
  const db = getDB();
  res.json({ ok: true, drafts: listGenerationQueue(db) });
});

// 生成対象の claim (2026-08-03、Codex設計相談 Critical 対応)。
// 取得と排他を1回で行う: ready_for_ai かつ 未claim/lease切れ の draft を run_id で押さえて材料付きで返す。
// 定期実行と手動、前夜のハングした実行が同じ draft を二重生成するのを防ぐ (lease 30分)
serviceApiRouter.post('/generation-queue/claim', async (req, res) => {
  const runId = cleanText(req.body?.run_id, 100);
  if (!runId) return res.status(400).json({ ok: false, error: 'run_id が必要です (例: 20260803-2200-a1b2)' });
  let limit = Number.parseInt(req.body?.limit, 10);
  if (!Number.isInteger(limit) || limit < 1) limit = 2;
  if (limit > 10) limit = 10;   // 1回の実行で押さえられる上限 (Codex: サーバー側上限)
  const db = getDB();
  const r = claimGenerationDrafts(db, runId, { limit });
  for (const d of Array.isArray(r.claimed) ? r.claimed : []) {
    logEvent(db, d.id, 'generation_claimed', runId, 'service-api');
  }
  // 中原さんが SP広告に「マニュアルで」設定しているキーワードを材料に添付 (2026-08-04)。
  // 人が選定・運用してきた実キーワードだけを参照する — 推奨KWやオートターゲティングの
  // プレースホルダは使わない (中原さん指示「マニュアルでなければ参照しない」)。
  // アカウント全体を30分キャッシュで一括取得するので claim ごとの外部呼び出しは高々1回。
  // fail-soft: env 未設定 (null)・API 失敗・タイムアウトでも claim は成功させる。
  // タイムアウト後も裏の取得は続きキャッシュに入る (次回 claim で使われるだけで無害)
  const claimedList = Array.isArray(r.claimed) ? r.claimed : [];
  if (claimedList.some((d) => extractAsin(d))) {
    // 全件取得は5〜10分かかる (実測) → スナップショット優先で応答は待たせない (Codex R3 high):
    // ①有効なスナップショットがあれば即時それを使う ②裏で最新化 (fire-and-forget・成功時に保存)
    // ③スナップショットが無い初回だけ、15s を上限に生取得を待ってみる (それでも無ければ今回は無しで続行)
    let byAsin = loadSpKeywordSnapshot(db);
    let kwError = null;
    const live = listSpManualKeywordsByAsin();
    live.then((m) => { if (m) try { saveSpKeywordSnapshot(getDB(), m); } catch (_) {} })
        .catch((e) => {
          // 無音で握り潰さない (Codex R2: 運用で「なぜKWが付かないか」を追えるように)
          console.error('[product-hub] SP広告KWの取得に失敗:', String(e?.message || e).slice(0, 200));
        });
    if (!byAsin) {
      try {
        byAsin = await Promise.race([
          live,
          new Promise((_, rej) => setTimeout(() => rej(new Error('sp_kw_timeout')), 15_000)),
        ]);
      } catch (e) {
        kwError = String(e.message || e).slice(0, 120);
      }
    }
    if (byAsin) {
      for (const d of claimedList) {
        const asin = extractAsin(d);
        const kws = asin ? byAsin.get(asin) : null;
        const arr = kws ? (Array.isArray(kws) ? kws : [...kws]) : null;
        if (arr && arr.length > 0) d.sp_keywords = arr.slice(0, 50);
      }
    } else if (kwError) {
      for (const d of claimedList) d.sp_keywords_error = kwError;
    }
  }
  res.json({ ok: true, run_id: runId, lease_until: r.leaseUntil, drafts: r.claimed });
});

// claim の解放 (生成を断念した draft を他の実行がすぐ拾えるように)
serviceApiRouter.post('/drafts/:id/release', (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  const runId = cleanText(req.body?.run_id, 100);
  if (!Number.isInteger(id) || id <= 0 || !runId) {
    return res.status(400).json({ ok: false, error: 'draft id と run_id が必要です' });
  }
  const db = getDB();
  const released = releaseGenerationClaim(db, id, runId);
  if (released) logEvent(db, id, 'generation_released', `${runId}: ${cleanText(req.body?.reason, 300) || '理由未記載'}`, 'service-api');
  res.json({ ok: true, released });
});

// AI 生成結果の一括書き戻し + status ready_for_ai → review
// body: { run_id, outputs: { rakuten_title: "...", ... }, model_note?, advance? }
// ⚠️ run_id 必須 (claim 済みの実行だけが書ける)。status が ready_for_ai でなければ 409
//    (人がレビュー中の文章への上書き防止 — Codex設計相談 Critical/High 対応)
serviceApiRouter.post('/drafts/:id/ai-outputs', (req, res) => {
  const id = Number.parseInt(req.params.id, 10);
  const db = getDB();
  const draft = Number.isInteger(id) && id > 0 ? db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id) : null;
  if (!draft) return res.status(404).json({ ok: false, error: 'draft not found' });

  const runId = cleanText(req.body?.run_id, 100);
  const claimErr = generationClaimError(draft, runId);
  if (claimErr) return res.status(409).json({ ok: false, error: claimErr });

  const outputs = req.body?.outputs;
  if (!outputs || typeof outputs !== 'object' || Array.isArray(outputs)) {
    return res.status(400).json({ ok: false, error: 'outputs (object) is required' });
  }
  const badKinds = Object.keys(outputs).filter((k) => !AI_OUTPUT_KINDS.includes(k));
  if (badKinds.length > 0) {
    return res.status(400).json({ ok: false, error: `invalid kinds: ${badKinds.join(', ')}` });
  }
  // 空出力での review 遷移を防ぐ (Codex R1 high): 1件以上 + 全 content 非空を必須にする
  const cleaned = {};
  for (const [kind, content] of Object.entries(outputs)) {
    const c = cleanText(content, 50000);
    if (!c) return res.status(400).json({ ok: false, error: `outputs.${kind} が空です` });
    cleaned[kind] = c;
  }
  if (Object.keys(cleaned).length === 0) {
    return res.status(400).json({ ok: false, error: 'outputs が空です' });
  }
  const wantAdvance = req.body?.advance === true; // truthy でなく厳密比較 (Codex R1 high)
  // review へ進めるのは6項目そろったときだけ (Codex: 部分生成は原則書かない。
  // advance=false での部分保存は再生成・試運転用に許す)
  if (wantAdvance) {
    const missing = AI_OUTPUT_KINDS.filter((k) => !cleaned[k]);
    if (missing.length > 0) {
      return res.status(400).json({ ok: false, error: `advance には全${AI_OUTPUT_KINDS.length}項目が必要です (不足: ${missing.join(', ')})` });
    }
  }
  const modelNote = cleanText(req.body?.model_note, 200);

  const write = db.transaction(() => {
    // 書き込み権の原子的な再取得 (Codex R2 high): 事前チェックと書き込みの間に
    // lease切れ・再claimが起きていたら、1バイトも書かずに中断する
    if (!acquireGenerationWriteLock(db, id, runId)) {
      return { conflict: true };
    }
    const skippedHumanEdited = [];
    for (const [kind, content] of Object.entries(cleaned)) {
      // 人が編集済みの項目は AI で上書きしない (Codex: 人編集の保護)
      const info = db.prepare(`
        INSERT INTO draft_ai_outputs (draft_id, kind, content, generated_at, model_note, edited_by_human)
        VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'), ?, 0)
        ON CONFLICT(draft_id, kind) DO UPDATE SET
          content = excluded.content,
          generated_at = excluded.generated_at,
          model_note = excluded.model_note,
          edited_by_human = 0
        WHERE draft_ai_outputs.edited_by_human = 0
      `).run(id, kind, content, modelNote);
      if (info.changes === 0) skippedHumanEdited.push(kind);
    }
    logEvent(db, id, 'ai_generated', `${runId}: ${Object.keys(cleaned).join(',')}`, 'service-api');
    // CAS: changes を見て実際に遷移した時だけ advanced/ログ (Codex R1 medium: 監査ログを嘘にしない)
    let advanced = false;
    if (wantAdvance) {
      const info = db.prepare(`
        UPDATE product_drafts SET status = 'review', updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ? AND status = 'ready_for_ai' AND generation_claim_run_id = ?
      `).run(id, runId);
      if (info.changes === 1) {
        logEvent(db, id, 'status_changed', 'ready_for_ai -> review (ai_generated)', 'service-api');
        advanced = true;
      }
    }
    // 書き終えたら claim は解放 (成功・部分保存どちらも。以降の再claimを妨げない)
    releaseGenerationClaim(db, id, runId);
    return { advanced, skippedHumanEdited };
  });
  const r = write();
  if (r.conflict) {
    return res.status(409).json({ ok: false, error: 'claim が失効しています (別の実行が引き継いだ可能性)。何も書き込んでいません' });
  }
  res.json({
    ok: true,
    written: Object.keys(cleaned).length - r.skippedHumanEdited.length,
    skipped_human_edited: r.skippedHumanEdited.length ? r.skippedHumanEdited : undefined,
    advanced: r.advanced,
  });
});
