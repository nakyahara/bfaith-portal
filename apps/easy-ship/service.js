/**
 * easy-ship の業務ロジック。
 * SKU → Easy Ship 梱包サイズ (寸法プリセット) マスターの管理と、Chrome拡張向け照会。
 *
 * 照合ルール (easy-ship-helper 仕様):
 * - SKUは前後trim+**常に大文字小文字を同一視**して完全一致 (2026-08-04 仕様として固定)
 * - SKUの一意性は照合設定に関わらず常に大小無視 (DBの LOWER(sku) 一意インデックス)
 */
import { getDB, utcNow } from './db.js';
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { CSV_COLUMNS, CsvParseError, parseCsv, parseCsvBool, sanitizeExcelCell, toCsv } from './csv.js';

export class EsError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

// SKUの大文字小文字は常に同一視する (2026-08-04 仕様として固定。設定切替は廃止)

function isUniqueViolation(e) {
  return typeof e?.code === 'string' && e.code.startsWith('SQLITE_CONSTRAINT');
}

function rowToApi(r) {
  return {
    id: r.id,
    sku: r.sku,
    packageSizeCode: r.package_size_code,
    packageSizeLabel: r.package_size_label,
    amazonOptionValue: r.amazon_option_value,
    isActive: r.is_active === 1,
    note: r.note,
    createdAt: r.created_at,
    updatedAt: r.updated_at,
  };
}

function rowToLookup(r) {
  return {
    sku: r.sku,
    packageSizeCode: r.package_size_code,
    packageSizeLabel: r.package_size_label,
    amazonOptionValue: r.amazon_option_value,
  };
}

// ---------- バリデーション ----------

function str(v) {
  return typeof v === 'string' ? v : '';
}

/** Excelで数式として解釈される先頭文字 (CSVインジェクション対策: 入力時点で拒否する) */
function hasFormulaPrefix(v) {
  return /^\s*[=+@-]/.test(v) || /^[\t\r]/.test(v);
}

/** 登録・更新の入力検証。不正なら EsError(400) */
export function validateInput(body) {
  const sku = str(body.sku).trim();
  const packageSizeCode = str(body.packageSizeCode).trim();
  const packageSizeLabel = str(body.packageSizeLabel).trim();
  const amazonOptionValue = str(body.amazonOptionValue).trim();
  const noteRaw = body.note == null ? '' : str(body.note);
  const isActive = body.isActive === undefined ? true : body.isActive === true;

  for (const [name, v] of [
    ['SKU', sku],
    ['梱包サイズコード', packageSizeCode],
    ['梱包サイズ名', packageSizeLabel],
    ['Amazon option value', amazonOptionValue],
    ['備考', noteRaw],
  ]) {
    if (hasFormulaPrefix(v)) {
      throw new EsError(
        400,
        'VALIDATION_ERROR',
        `${name} が数式として解釈される文字 (= + @ など) で始まっています。先頭に別の文字を付けてください`,
      );
    }
  }

  if (!sku || sku.length > 100) throw new EsError(400, 'VALIDATION_ERROR', 'SKUは1〜100文字で入力してください');
  if (!packageSizeCode || packageSizeCode.length > 50) {
    throw new EsError(400, 'VALIDATION_ERROR', '梱包サイズコードは1〜50文字で入力してください');
  }
  if (!packageSizeLabel || packageSizeLabel.length > 100) {
    throw new EsError(400, 'VALIDATION_ERROR', '梱包サイズ名は1〜100文字で入力してください');
  }
  if (amazonOptionValue.length > 200) {
    throw new EsError(400, 'VALIDATION_ERROR', 'Amazon option value は200文字以内です');
  }
  if (noteRaw.length > 500) throw new EsError(400, 'VALIDATION_ERROR', '備考は500文字以内です');
  return {
    sku,
    packageSizeCode,
    packageSizeLabel,
    amazonOptionValue,
    isActive,
    note: noteRaw.trim() === '' ? null : noteRaw,
  };
}

// ---------- 照会 (拡張向け) ----------

/** SKU検索 (常に大小無視の完全一致)。見つからなければ null */
export function findBySku(rawSku) {
  const sku = str(rawSku).trim();
  if (!sku) return null;
  return (
    getDB().prepare('SELECT * FROM es_package_size_master WHERE LOWER(sku) = LOWER(?)').get(sku) ??
    null
  );
}

/** 重複チェック用: 照合設定に関わらず常に大小無視 (DB一意制約と同じ基準) */
function findBySkuForDup(rawSku) {
  const sku = str(rawSku).trim();
  if (!sku) return null;
  return (
    getDB().prepare('SELECT * FROM es_package_size_master WHERE LOWER(sku) = LOWER(?)').get(sku) ??
    null
  );
}

export function lookupOne(rawSku) {
  const row = findBySku(rawSku);
  if (!row) throw new EsError(404, 'SKU_NOT_FOUND', 'SKUが登録されていません');
  if (row.is_active !== 1) throw new EsError(404, 'SKU_INACTIVE', 'このSKUは無効化されています');
  return rowToLookup(row);
}

export function bulkLookup(skus) {
  if (!Array.isArray(skus) || skus.length === 0 || skus.length > 200) {
    throw new EsError(400, 'VALIDATION_ERROR', 'skus は1〜200件の配列で指定してください');
  }
  const found = [];
  const notFound = [];
  const inactive = [];
  const seen = new Set();
  for (const raw of skus) {
    const sku = str(raw).trim();
    if (!sku || sku.length > 100) continue;
    const key = sku.toLowerCase(); // SKUは常に大小無視
    if (seen.has(key)) continue;
    seen.add(key);
    const row = findBySku(sku);
    if (!row) notFound.push(sku);
    else if (row.is_active !== 1) inactive.push(sku);
    else found.push(rowToLookup(row));
  }
  return { found, notFound, inactive };
}

// ---------- 管理 (CRUD) ----------

/**
 * SKU→商品名の解決 (warehouse-mirror の mirror_products。easy-shipのSKUは基本NE商品コード)。
 * 表示専用の付加情報のため fail-soft: mirror未初期化・照会失敗時は空Mapを返し一覧表示を止めない。
 * Amazon別名SKU (商品コードと不一致) は見つからず null 表示になる (仕様)。
 */
function productNamesFor(skus) {
  const out = new Map();
  const codes = [...new Set(skus.map((s) => str(s).trim().toLowerCase()).filter(Boolean))];
  if (codes.length === 0) return out;
  try {
    const ph = codes.map(() => '?').join(',');
    const rows = getMirrorDB()
      .prepare(
        `SELECT lower(trim(商品コード)) AS code, 商品名 AS name
           FROM mirror_products
          WHERE lower(trim(商品コード)) IN (${ph})`,
      )
      .all(...codes);
    for (const r of rows) out.set(r.code, r.name ?? null);
  } catch {
    // mirror未初期化 (テスト等)・スキーマ差異は無視 (商品名なしで一覧は成立する)
  }
  return out;
}

/** 商品名の部分一致で mirror_products から商品コード (lower) を引く。fail-soft で空配列 */
function productCodesByName(q) {
  try {
    // LIKEのワイルドカードを literal 化 (packing-dispatch searchRules と同方式)
    const safe = q.replace(/[\\%_]/g, '\\$&').toLowerCase();
    return getMirrorDB()
      .prepare(
        `SELECT lower(trim(商品コード)) AS code FROM mirror_products
          WHERE lower(trim(商品名)) LIKE ? ESCAPE '\\' AND 商品コード IS NOT NULL
          LIMIT 500`,
      )
      .all(`%${safe}%`)
      .map((r) => r.code);
  } catch {
    return [];
  }
}

export function listMaster(query = {}) {
  const page = Math.max(1, Number.parseInt(query.page, 10) || 1);
  const perPage = Math.min(100, Math.max(1, Number.parseInt(query.perPage, 10) || 50));
  const q = str(query.q).trim();
  const active = ['true', 'false'].includes(query.active) ? query.active : 'all';
  const sort = query.sort === 'sku' ? 'sku' : 'updated_at';
  const order = query.order === 'asc' ? 'ASC' : query.order === 'desc' ? 'DESC' : sort === 'sku' ? 'ASC' : 'DESC';

  const where = [];
  const params = [];
  if (q) {
    // SKU部分一致に加え、商品名部分一致 (mirror_products) でもヒットさせる。
    // LIKEワイルドカード (% _ \) は literal 化し、商品名検索側と semantics を揃える
    const safeQ = q.replace(/[\\%_]/g, '\\$&');
    const nameCodes = productCodesByName(q);
    if (nameCodes.length > 0) {
      const ph = nameCodes.map(() => '?').join(',');
      where.push(`(sku COLLATE NOCASE LIKE ? ESCAPE '\\' OR lower(trim(sku)) IN (${ph}))`);
      params.push(`%${safeQ}%`, ...nameCodes);
    } else {
      where.push(`sku COLLATE NOCASE LIKE ? ESCAPE '\\'`);
      params.push(`%${safeQ}%`);
    }
  }
  if (active !== 'all') {
    where.push('is_active = ?');
    params.push(active === 'true' ? 1 : 0);
  }
  const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
  const db = getDB();
  const total = db
    .prepare(`SELECT COUNT(*) AS c FROM es_package_size_master ${whereSql}`)
    .get(...params).c;
  const items = db
    .prepare(
      `SELECT * FROM es_package_size_master ${whereSql} ORDER BY ${sort} ${order} LIMIT ? OFFSET ?`,
    )
    .all(...params, perPage, (page - 1) * perPage)
    .map(rowToApi);
  // 表示用の商品名を付与 (見つからないSKUは null)
  const names = productNamesFor(items.map((i) => i.sku));
  for (const item of items) {
    item.productName = names.get(item.sku.trim().toLowerCase()) ?? null;
  }
  return { items, total, page, perPage };
}

export function createMaster(body, userEmail) {
  const input = validateInput(body);
  const dup = findBySkuForDup(input.sku);
  if (dup) {
    throw new EsError(409, 'DUPLICATE_SKU', `SKU "${input.sku}" は既に登録されています (大文字小文字違いを含む)`);
  }
  const now = utcNow();
  try {
    const info = getDB()
      .prepare(
        `INSERT INTO es_package_size_master
           (sku, package_size_code, package_size_label, amazon_option_value, is_active, note, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        input.sku,
        input.packageSizeCode,
        input.packageSizeLabel,
        input.amazonOptionValue,
        input.isActive ? 1 : 0,
        input.note,
        now,
        now,
      );
    logAdmin(userEmail, 'admin_create', 'ok', input.sku, null);
    return rowToApi(getDB().prepare('SELECT * FROM es_package_size_master WHERE id = ?').get(info.lastInsertRowid));
  } catch (e) {
    // 事前チェックをすり抜けた並行作成の最終防衛 (DBのLOWER一意インデックス)
    if (isUniqueViolation(e)) {
      throw new EsError(409, 'DUPLICATE_SKU', 'SKUが既に登録されています (大文字小文字違いを含む)');
    }
    throw e;
  }
}

export function updateMaster(id, body, userEmail) {
  const input = validateInput(body);
  const db = getDB();
  const current = db.prepare('SELECT * FROM es_package_size_master WHERE id = ?').get(id);
  if (!current) throw new EsError(404, 'NOT_FOUND', '対象のデータが見つかりません');
  const dup = findBySkuForDup(input.sku);
  if (dup && dup.id !== current.id) {
    throw new EsError(409, 'DUPLICATE_SKU', `SKU "${input.sku}" は既に登録されています (大文字小文字違いを含む)`);
  }
  try {
    db.prepare(
      `UPDATE es_package_size_master SET
         sku = ?, package_size_code = ?, package_size_label = ?, amazon_option_value = ?,
         is_active = ?, note = ?, updated_at = ?
       WHERE id = ?`,
    ).run(
      input.sku,
      input.packageSizeCode,
      input.packageSizeLabel,
      input.amazonOptionValue,
      input.isActive ? 1 : 0,
      input.note,
      utcNow(),
      id,
    );
  } catch (e) {
    if (isUniqueViolation(e)) {
      throw new EsError(409, 'DUPLICATE_SKU', 'SKUが既に登録されています (大文字小文字違いを含む)');
    }
    throw e;
  }
  logAdmin(userEmail, 'admin_update', 'ok', input.sku, null);
  return rowToApi(db.prepare('SELECT * FROM es_package_size_master WHERE id = ?').get(id));
}

/** 既定は無効化 (論理削除)。hard=true で物理削除 */
export function removeMaster(id, hard, userEmail) {
  const db = getDB();
  const current = db.prepare('SELECT * FROM es_package_size_master WHERE id = ?').get(id);
  if (!current) throw new EsError(404, 'NOT_FOUND', '対象のデータが見つかりません');
  if (hard) {
    db.prepare('DELETE FROM es_package_size_master WHERE id = ?').run(id);
    logAdmin(userEmail, 'admin_delete', 'ok', current.sku, null);
    return { deleted: true };
  }
  db.prepare('UPDATE es_package_size_master SET is_active = 0, updated_at = ? WHERE id = ?').run(
    utcNow(),
    id,
  );
  logAdmin(userEmail, 'admin_deactivate', 'ok', current.sku, null);
  return rowToApi(db.prepare('SELECT * FROM es_package_size_master WHERE id = ?').get(id));
}

// ---------- CSVインポート / エクスポート ----------

/**
 * mode='preview' はDBを変更しない。mode='commit' はエラー行が1件でもあれば
 * 既定で全件ブロック (applied=false, blocked=true)。allowPartial=true のときだけ正常行を反映。
 */
export function importCsv(csvText, mode, allowPartial, userEmail) {
  if (typeof csvText !== 'string' || csvText.length === 0 || csvText.length > 2_000_000) {
    throw new EsError(400, 'VALIDATION_ERROR', 'CSVが空か大きすぎます');
  }
  if (!['preview', 'commit'].includes(mode)) {
    throw new EsError(400, 'VALIDATION_ERROR', 'mode は preview | commit');
  }
  let rows;
  try {
    rows = parseCsv(csvText);
  } catch (e) {
    if (e instanceof CsvParseError) throw new EsError(400, 'INVALID_CSV', e.message);
    throw e;
  }
  if (rows.length === 0) throw new EsError(400, 'INVALID_CSV', 'CSVにデータがありません');
  const header = rows[0].map((h) => h.trim());
  for (const col of ['sku', 'package_size_code', 'package_size_label']) {
    if (!header.includes(col)) {
      throw new EsError(400, 'INVALID_CSV', `ヘッダー行に必須列 "${col}" がありません。想定: ${CSV_COLUMNS.join(',')}`);
    }
  }
  const get = (row, col) => {
    const i = header.indexOf(col);
    return i >= 0 ? (row[i] ?? '') : '';
  };

  /**
   * 行の検証と適用計画の作成。
   * commit時は BEGIN IMMEDIATE トランザクション内から呼ぶことで、
   * 判定(既存行の読み取り)と適用の間に別プロセスの書き込みが割り込まないようにする。
   */
  const analyze = () => {
  const results = [];
  const plans = [];
  const seen = new Set(); // ファイル内重複はDB制約に合わせて常に大小無視

  for (let i = 1; i < rows.length; i++) {
    const raw = rows[i];
    const line = i + 1;
    const isActive = parseCsvBool(get(raw, 'is_active'));
    if (isActive === null) {
      results.push({ line, sku: (raw[0] ?? '').trim(), action: 'error', message: 'is_active は true/false/1/0/空 のいずれか', raw });
      continue;
    }
    let input;
    try {
      input = validateInput({
        sku: get(raw, 'sku'),
        packageSizeCode: get(raw, 'package_size_code'),
        packageSizeLabel: get(raw, 'package_size_label'),
        amazonOptionValue: get(raw, 'amazon_option_value'),
        isActive,
        note: get(raw, 'note'),
      });
    } catch (e) {
      results.push({ line, sku: (raw[0] ?? '').trim(), action: 'error', message: e.message, raw });
      continue;
    }
    const key = input.sku.toLowerCase();
    if (seen.has(key)) {
      results.push({ line, sku: input.sku, action: 'error', message: '同一CSV内でSKUが重複しています (大文字小文字違いを含む)', raw });
      continue;
    }
    seen.add(key);
    const existing = findBySkuForDup(input.sku);
    if (!existing) {
      plans.push({ action: 'create', input });
      results.push({ line, sku: input.sku, action: 'create' });
    } else {
      const unchanged =
        existing.sku === input.sku &&
        existing.package_size_code === input.packageSizeCode &&
        existing.package_size_label === input.packageSizeLabel &&
        existing.amazon_option_value === input.amazonOptionValue &&
        (existing.is_active === 1) === input.isActive &&
        (existing.note ?? null) === input.note;
      if (unchanged) {
        results.push({ line, sku: input.sku, action: 'skip', message: '変更なし' });
      } else {
        plans.push({ action: 'update', input, existingId: existing.id });
        results.push({ line, sku: input.sku, action: 'update' });
      }
    }
  }
  return { results, plans };
  };

  const db = getDB();
  let results;
  let applied = false;
  let blocked = false;

  if (mode === 'preview') {
    results = analyze().results;
  } else {
    // commit: 判定と適用を同一の BEGIN IMMEDIATE トランザクションで行う
    // (Renderデプロイ時の新旧プロセス重複など、判定と適用の間の割り込み書き込みを防ぐ)
    db.transaction(() => {
      const plan = analyze();
      results = plan.results;
      const errorCount = results.filter((r) => r.action === 'error').length;
      if (errorCount > 0 && !allowPartial) {
        blocked = true;
        return;
      }
      const now = utcNow();
      const ins = db.prepare(
        `INSERT INTO es_package_size_master
           (sku, package_size_code, package_size_label, amazon_option_value, is_active, note, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      );
      const upd = db.prepare(
        `UPDATE es_package_size_master SET
           sku = ?, package_size_code = ?, package_size_label = ?, amazon_option_value = ?,
           is_active = ?, note = ?, updated_at = ?
         WHERE id = ?`,
      );
      try {
        for (const p2 of plan.plans) {
          const p = p2.input;
          if (p2.action === 'create') {
            ins.run(p.sku, p.packageSizeCode, p.packageSizeLabel, p.amazonOptionValue, p.isActive ? 1 : 0, p.note, now, now);
          } else {
            upd.run(p.sku, p.packageSizeCode, p.packageSizeLabel, p.amazonOptionValue, p.isActive ? 1 : 0, p.note, now, p2.existingId);
          }
        }
      } catch (e) {
        // BEGIN IMMEDIATE内では通常起こらないが、万一の一意制約違反は409へ (500にしない)
        if (isUniqueViolation(e)) {
          throw new EsError(409, 'DUPLICATE_SKU', 'SKUが既に登録されています (大文字小文字違いを含む)');
        }
        throw e;
      }
      applied = true;
    }).immediate();
  }

  const errorCount = results.filter((r) => r.action === 'error').length;
  const summary = {
    mode,
    created: results.filter((r) => r.action === 'create').length,
    updated: results.filter((r) => r.action === 'update').length,
    skipped: results.filter((r) => r.action === 'skip').length,
    errors: errorCount,
    applied,
    ...(blocked ? { blocked: true } : {}),
    rows: results,
  };
  if (applied) {
    logAdmin(
      userEmail,
      'admin_import',
      'ok',
      null,
      `created=${summary.created} updated=${summary.updated} skipped=${summary.skipped} errors=${summary.errors}`,
    );
  }
  return summary;
}

/** excelMode=true で数式インジェクション対策の ' 前置 (再インポート不可の閲覧用) */
export function exportCsv(excelMode) {
  const cell = (v) => (excelMode ? sanitizeExcelCell(v) : v);
  const items = getDB().prepare('SELECT * FROM es_package_size_master ORDER BY sku ASC').all();
  const rows = [
    [...CSV_COLUMNS],
    ...items.map((r) => [
      cell(r.sku),
      cell(r.package_size_code),
      cell(r.package_size_label),
      cell(r.amazon_option_value),
      r.is_active === 1 ? 'true' : 'false',
      cell(r.note ?? ''),
    ]),
  ];
  const bom = String.fromCharCode(0xfeff); // Excelでの文字化け防止
  return bom + toCsv(rows);
}

// ---------- 拡張からの自動登録 ----------

/** 表示名から梱包サイズコードを導出 (「メール便…」→mail /「NNサイズ…」→NN / 不明→auto) */
export function deriveSizeCode(label) {
  if (/^メール便/.test(label)) return 'mail';
  const m = label.match(/^(\d+)サイズ/);
  return m ? m[1] : 'auto';
}

/**
 * セラーセントラルで担当者が手動選択したサイズをマスターへ自動登録する (Chrome拡張の学習機能)。
 * - 既存SKUは大小文字違いを含め**絶対に上書きしない** (created:false を返すだけ)
 * - amazonOptionValue (実画面で選択された option の UUID) を必須にする
 *   (照合tier1が常に効く状態でのみ登録を受け付ける)
 * - 検証は createMaster と同一 (validateInput + 大小無視の重複チェック + DB一意制約)
 */
export function autoRegisterFromExt(body) {
  const label = str(body?.packageSizeLabel).trim();
  const uuid = str(body?.amazonOptionValue).trim();
  if (!uuid) {
    throw new EsError(400, 'VALIDATION_ERROR', 'amazonOptionValue が必要です');
  }
  try {
    const item = createMaster(
      {
        sku: body?.sku,
        packageSizeCode: deriveSizeCode(label),
        packageSizeLabel: label,
        amazonOptionValue: uuid,
        isActive: true,
        note: 'セラーセントラル初回手動選択から自動登録 (拡張)',
      },
      'extension:auto-register',
    );
    return { created: true, item };
  } catch (e) {
    if (e instanceof EsError && e.code === 'DUPLICATE_SKU') {
      return { created: false, reason: 'already_exists' };
    }
    throw e;
  }
}

// ---------- 操作ログ ----------

function logAdmin(userEmail, action, result, sku, message) {
  try {
    getDB()
      .prepare(
        `INSERT INTO es_operation_logs (sku, action, result, message, user_identifier, browser_identifier, page_url, created_at)
         VALUES (?, ?, ?, ?, ?, NULL, NULL, ?)`,
      )
      .run(sku, action, result, message, userEmail ?? null, utcNow());
  } catch (e) {
    console.error('[easy-ship] operation_logs への書き込みに失敗:', e);
  }
}

/** URLからクエリ・フラグメントを除去 (個人情報・トークン混入防止) */
function sanitizePageUrl(raw) {
  if (typeof raw !== 'string' || !raw) return null;
  try {
    const u = new URL(raw);
    return `${u.origin}${u.pathname}`.slice(0, 300);
  } catch {
    return null;
  }
}

/**
 * Chrome拡張からの操作ログ。
 * 注文番号は env EASY_SHIP_ALLOW_ORDER_ID_LOGGING='1' のときのみmessageに付記 (既定: 破棄)。
 */
export function addExtLogs(entries) {
  if (!Array.isArray(entries) || entries.length === 0 || entries.length > 100) {
    throw new EsError(400, 'VALIDATION_ERROR', 'entries は1〜100件の配列で指定してください');
  }
  const allowOrderId = process.env.EASY_SHIP_ALLOW_ORDER_ID_LOGGING === '1';
  const db = getDB();
  const now = utcNow();
  const ins = db.prepare(
    `INSERT INTO es_operation_logs (sku, action, result, message, user_identifier, browser_identifier, page_url, created_at)
     VALUES (?, ?, ?, ?, NULL, ?, ?, ?)`,
  );
  let saved = 0;
  db.transaction(() => {
    for (const e of entries) {
      const action = str(e?.action).trim().slice(0, 50);
      const result = str(e?.result).trim().slice(0, 50);
      if (!action || !result) continue;
      let message = str(e?.message).slice(0, 500) || null;
      if (allowOrderId && str(e?.orderId)) {
        message = `${message ?? ''} [order=${str(e.orderId).slice(0, 50)}]`.trim().slice(0, 500);
      }
      ins.run(
        str(e?.sku).slice(0, 100) || null,
        action,
        result,
        message,
        str(e?.browserIdentifier).slice(0, 100) || null,
        sanitizePageUrl(e?.pageUrl),
        now,
      );
      saved++;
    }
  })();
  return { saved };
}
