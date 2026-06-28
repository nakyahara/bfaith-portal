/**
 * Phase E-14 (2026-06-28): 楽天 RMS → Notion master 新規 page 作成 service。
 *
 * 設計 (Codex R1〜R7 stop):
 *   - 重複作成防止: DB CAS lock (notion_page_create_requests) + Notion pre-check (findPageByManageNumber) の 2 段
 *   - lease_token + deadlineAt で workers 競合と長時間 create を防ぐ
 *   - finalize CAS は lease_token 一致時のみ書込、 不一致は row 不変 + 'lease_lost' report
 *   - 各 step (pre-check / 楽天 fetch / create / finalize) で remaining = deadlineAt - now を渡す
 *   - dryRun=true は完全 read-only (DB 書込・Notion 作成なし)
 */
import crypto from 'crypto';

import { createPage, findPageByManageNumber } from '../lib/notion-client.js';
import { fetchItemDetailsBulkDetailed } from '../lib/rakuten-rms-proxy.js';
import { truncateYahooTitle, convertTaxRate, pickRepresentativePrice } from '../lib/rakuten-to-notion-draft.js';

const LEASE_TTL_MS = 90_000;       // 90s
const CREATE_BUDGET_MS = 45_000;   // 45s (lease の 50%、 worst case 余裕)
const CREATE_ATTEMPT_CAP_MS = 10_000;
const RETRY_BUFFER_MS = 2_000;     // Retry-After 後の次 attempt 最低時間

const sleep = (ms) => new Promise((r) => setTimeout(r, Math.max(0, ms)));

function newLeaseToken() {
  return crypto.randomUUID();
}

function nowIso() {
  return new Date().toISOString();
}

function leaseExpiresAtIso() {
  return new Date(Date.now() + LEASE_TTL_MS).toISOString();
}

/**
 * 1 SKU の処理 (claim → pre-check → fetch → create → finalize)。
 * dryRun=true なら DB 書込・Notion 作成なしで preview のみ。
 */
async function processSku({ db, manageNumber, dryRun, rakutenItem }) {
  if (dryRun) {
    if (!rakutenItem) return { itemCode: manageNumber, outcome: 'would_skip_rakuten_fetch_failed' };
    const title = rakutenItem.itemName || rakutenItem.title;
    if (!title) return { itemCode: manageNumber, outcome: 'would_skip_no_itemName' };
    return {
      itemCode: manageNumber,
      outcome: 'would_create',
      preview: {
        name: title,
        yahoo_title: truncateYahooTitle(title),
        price: pickRepresentativePrice(rakutenItem),
        tax_rate: convertTaxRate(rakutenItem.payment?.taxRate),
      },
    };
  }

  // 1. claim CAS — INSERT で 'running' を直接取得 (失敗 status のみ奪取可能、 completed は弾く)
  const leaseToken = newLeaseToken();
  const claimResult = db.prepare(`
    INSERT INTO notion_page_create_requests (rakuten_manage_number, status, lease_token, lease_expires_at, attempt)
    VALUES (?, 'running', ?, ?, 1)
    ON CONFLICT(rakuten_manage_number) DO UPDATE SET
      status = 'running',
      lease_token = excluded.lease_token,
      lease_expires_at = excluded.lease_expires_at,
      attempt = attempt + 1,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
    WHERE status = 'failed'
       OR (status = 'running' AND lease_expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
  `).run(manageNumber, leaseToken, leaseExpiresAtIso());

  if (claimResult.changes === 0) {
    // 既に completed or 他 worker が running (lease 有効) → skip
    const existing = db.prepare(`SELECT status, notion_page_id FROM notion_page_create_requests WHERE rakuten_manage_number = ?`).get(manageNumber);
    return { itemCode: manageNumber, outcome: 'skip_locked', existingStatus: existing?.status, existingPageId: existing?.notion_page_id };
  }

  const deadlineAt = Date.now() + CREATE_BUDGET_MS;
  const remaining = () => Math.max(0, deadlineAt - Date.now());

  // 2. Notion pre-check (findPageByManageNumber)
  try {
    if (remaining() <= 0) throw new Error('deadline_exceeded:before_precheck');
    const existing = await findPageByManageNumber(manageNumber, {
      cfg: { timeoutMs: Math.min(remaining(), 10_000) },
      maxRetries: 0,
    });
    if (existing?.id) {
      finalizeSuccess(db, manageNumber, leaseToken, existing.id);
      return { itemCode: manageNumber, outcome: 'already_exists_in_notion', notionPageId: existing.id };
    }
  } catch (e) {
    finalizeFailed(db, manageNumber, leaseToken, `precheck_failed: ${e.message}`);
    return { itemCode: manageNumber, outcome: 'precheck_failed', error: e.message };
  }

  // 3. 楽天 fetch チェック (caller が bulk で取得済の rakutenItem を使う想定だが、 単体 fallback も可)
  if (!rakutenItem) {
    finalizeFailed(db, manageNumber, leaseToken, 'rakuten_fetch_failed');
    return { itemCode: manageNumber, outcome: 'rakuten_fetch_failed' };
  }

  const itemName = rakutenItem.itemName || rakutenItem.title;
  if (!itemName) {
    finalizeFailed(db, manageNumber, leaseToken, 'rakuten_itemName_missing');
    return { itemCode: manageNumber, outcome: 'rakuten_itemName_missing' };
  }

  // 4. properties 構築
  const yahooTitle = truncateYahooTitle(itemName);
  const price = pickRepresentativePrice(rakutenItem);
  const taxRate = convertTaxRate(rakutenItem.payment?.taxRate);
  const properties = {
    'Name': { title: [{ type: 'text', text: { content: itemName } }] },
    '商品コード': { rich_text: [{ type: 'text', text: { content: manageNumber } }] },
  };
  if (yahooTitle) properties['Yahoo!タイトル'] = { rich_text: [{ type: 'text', text: { content: yahooTitle } }] };
  if (price != null) properties['売価'] = { number: price };
  if (taxRate) properties['税率'] = { select: { name: taxRate } };
  properties['Status'] = { select: { name: '⓪新規商品_高島' } };

  // 5. createPage with budget
  let createdPage;
  try {
    createdPage = await createPageWithBudget(properties, { deadlineAt });
  } catch (e) {
    finalizeFailed(db, manageNumber, leaseToken, `create_failed: ${e.message}`);
    return { itemCode: manageNumber, outcome: 'create_failed', error: e.message };
  }

  // 6. finalize CAS
  const ok = finalizeSuccess(db, manageNumber, leaseToken, createdPage.id);
  if (!ok) {
    // lease 喪失 (他 worker が finalize 済み or lease 期限切れ後別 worker 取った)
    return { itemCode: manageNumber, outcome: 'lease_lost', notionPageId: createdPage.id };
  }

  // 7. notion_overrides cache 即反映 (best-effort、 失敗しても次回 sync で吸収)
  try {
    db.prepare(`
      INSERT OR IGNORE INTO notion_overrides
        (rakuten_manage_number, yahoo_title, yahoo_price, notion_tax_rate, raw_properties_json,
         source_hash, notion_page_id, source_updated_at, sync_run_id, synced_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
    `).run(
      manageNumber, yahooTitle || null, price ?? null, taxRate || null,
      JSON.stringify({ created_by_phase_e14: true }),
      crypto.createHash('sha256').update(`${manageNumber}:${createdPage.id}`).digest('hex'),
      createdPage.id,
      createdPage.last_edited_time || nowIso(),
      'phase_e14_create',
    );
  } catch (_) { /* best-effort */ }

  return { itemCode: manageNumber, outcome: 'created', notionPageId: createdPage.id, properties: { yahoo_title: yahooTitle, price, tax_rate: taxRate } };
}

async function createPageWithBudget(properties, { deadlineAt }) {
  // Codex Phase E-14 R8 H-1: timeout / network error / 5xx 後に Notion 側で page が
  //   作成済みのケースで同一 SKU の重複 page を作るリスクがある。 POST に idempotency key
  //   が無いので、 retry は **429 (Retry-After 明示の rate limit) のみ** に限定。
  //   timeout / network error / 5xx は即 failed → 次回実行時に findPageByManageNumber
  //   pre-check で拾う (= 重複防止)。
  let attempt = 0;
  let lastErr = null;
  while (Date.now() < deadlineAt) {
    attempt += 1;
    const remaining = deadlineAt - Date.now();
    const attemptTimeout = Math.min(remaining, CREATE_ATTEMPT_CAP_MS);
    try {
      return await createPage(properties, {
        cfg: { timeoutMs: attemptTimeout },
        maxRetries: 0,
      });
    } catch (e) {
      lastErr = e;
      // 429 (rate limit) のみ retry。 それ以外は即 failed (重複作成防止)
      if (e.status !== 429) break;
      const retryAfterMs = e.retryAfterMs ?? 1000;
      const projectedNext = Date.now() + retryAfterMs + RETRY_BUFFER_MS;
      if (projectedNext >= deadlineAt) break;
      await sleep(retryAfterMs);
    }
  }
  throw new Error(`createPage exhausted (attempt=${attempt}): ${lastErr?.message || 'deadline_exceeded'}`);
}

function finalizeSuccess(db, manageNumber, leaseToken, notionPageId) {
  const r = db.prepare(`
    UPDATE notion_page_create_requests
       SET status = 'completed', notion_page_id = ?, error_message = NULL,
           updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
     WHERE rakuten_manage_number = ?
       AND lease_token = ?
       AND status = 'running'
  `).run(notionPageId, manageNumber, leaseToken);
  return r.changes > 0;
}

function finalizeFailed(db, manageNumber, leaseToken, errorMessage) {
  const r = db.prepare(`
    UPDATE notion_page_create_requests
       SET status = 'failed', error_message = ?,
           updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
     WHERE rakuten_manage_number = ?
       AND lease_token = ?
       AND status = 'running'
  `).run(errorMessage, manageNumber, leaseToken);
  return r.changes > 0;
}

/**
 * Phase E-14 main entry: 楽天 RMS → Notion master 新規 page 作成。
 *
 * @param {Database} db
 * @param {object} opts
 * @param {boolean} [opts.dryRun=true]
 * @param {string[]} [opts.itemCodes]  指定時はその item_code のみ対象
 * @param {number} [opts.limit=100]
 */
export async function createNotionPagesFromRakuten(db, { dryRun = true, itemCodes = null, limit = 100 } = {}) {
  // 1. eligible SKU 抽出
  const itemCodesFilter = Array.isArray(itemCodes) && itemCodes.length > 0;
  const sql = `
    SELECT mc.rakuten_manage_number AS manage_number, mc.item_code, mc.rakuten_title
    FROM migration_candidates mc
    LEFT JOIN notion_overrides no ON no.rakuten_manage_number = mc.rakuten_manage_number
    LEFT JOIN notion_page_create_requests req ON req.rakuten_manage_number = mc.rakuten_manage_number
    WHERE no.notion_page_id IS NULL
      AND mc.status = 'candidate'
      AND COALESCE(mc.rakuten_manage_number, '') != ''
      AND (
        req.status IS NULL
        OR req.status = 'failed'
        OR (req.status = 'running' AND req.lease_expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
      )
      ${itemCodesFilter ? `AND mc.item_code IN (${Array(itemCodes.length).fill('?').join(',')})` : ''}
    ORDER BY mc.rakuten_manage_number
    LIMIT ?
  `;
  const params = itemCodesFilter ? [...itemCodes, limit] : [limit];
  const rows = db.prepare(sql).all(...params);

  if (rows.length === 0) {
    return { totalScanned: 0, results: [] };
  }

  // 2. 楽天 RMS bulk fetch
  const manageNumbers = rows.map((r) => r.manage_number);
  let rakutenItems = [];
  let rakutenFailed = [];
  try {
    const r = await fetchItemDetailsBulkDetailed(manageNumbers);
    rakutenItems = r.items || [];
    rakutenFailed = r.failed || [];
  } catch (e) {
    return { totalScanned: rows.length, results: [], error: `rakuten_rms_failed: ${e.message}` };
  }
  const rakutenByMn = new Map(rakutenItems.map((it) => [it.manageNumber, it]));

  // 3. 各 SKU 処理
  const results = [];
  for (const r of rows) {
    const rakutenItem = rakutenByMn.get(r.manage_number);
    const result = await processSku({ db, manageNumber: r.manage_number, dryRun, rakutenItem });
    results.push(result);
  }

  return { totalScanned: rows.length, results };
}
