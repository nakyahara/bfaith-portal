/**
 * 入庫情報管理 — データ層 (Render 完結)
 *
 * warehouse-mirror.db の f_inbound_info / f_inbound_origin を扱う。
 * 接続は warehouse-mirror の getMirrorDB() を共有 (WAL / busy_timeout=5000)。
 * 新商品の自動追加は mirror_products (m_products のミラー) を参照する。
 *
 * ミニPC は一切使わない。
 *
 * 照合キー: code_key = lower(trim(商品コード))。
 *   実データ検証 (2026-07-21) で Excel と NE の商品コードに大文字小文字違いが
 *   1123 件あったため、表示用の元表記 (商品コード) と照合キーを分離して持つ。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';

export function utcIsoNow() {
  return new Date().toISOString();
}

// ─── 商品コード正規化 ───
// JS の String.prototype.trim() は全角スペース (U+3000) も除去する
// (Yahoo 検索KW取込の全角スペース PK 衝突と同種の揺れ対策)。
export function normCode(code) {
  return String(code == null ? '' : code).trim();
}
export function codeKey(code) {
  return normCode(code).toLowerCase();
}

// ─── 入数の検証 (null 可 / 0〜1億の整数のみ) ───
// 戻り値: { ok: true, value } | { ok: false }
export function parseIrisu(v) {
  if (v == null || v === '') return { ok: true, value: null };
  const n = typeof v === 'number' ? v : Number(String(v).trim());
  if (!Number.isFinite(n) || !Number.isInteger(n) || n < 0 || n > 100000000) {
    return { ok: false };
  }
  return { ok: true, value: n };
}

// UI で編集可能なテキスト列 (入数以外)。列名は旧スプレッドシートのヘッダーそのまま。
const TEXT_FIELDS = ['商品名', '入庫時BCシール貼りフラグ', '直接ピックロケ保管', 'BF保管荷姿', 'いろは在庫化作業有無', 'memo'];
const MAX_TEXT_LEN = 500;

function cleanText(v) {
  if (v == null) return null;
  const s = String(v).trim();
  if (s === '') return null;
  return s.slice(0, MAX_TEXT_LEN);
}

// ─── ヘッダー統計 ───
export function stats() {
  const db = getMirrorDB();
  const row = db.prepare(`
    SELECT COUNT(*) AS total,
           SUM(CASE WHEN 入数 IS NULL THEN 1 ELSE 0 END) AS missing_irisu,
           SUM(CASE WHEN source = 'auto' THEN 1 ELSE 0 END) AS auto_new
      FROM f_inbound_info
  `).get();
  const mirrorCount = db.prepare('SELECT COUNT(*) AS c FROM mirror_products').get().c;
  const schedCount = db.prepare('SELECT COUNT(*) AS c FROM f_inbound_schedule').get().c;
  return { ...row, mirror_products: mirrorCount, schedule_count: schedCount };
}

// ─── 一覧 (サーバー側ページング + 検索 + フィルタ) ───
// filter: 'all' | 'missing' (入数未記入) | 'new' (source='auto' = cron 追加後に未編集)
//         | 'scheduled' (入荷予定 = Drive nefuda.csv に載っている商品のみ)
export function listInbound({ q = '', filter = 'all', offset = 0, limit = 100 } = {}) {
  const db = getMirrorDB();
  const conds = [];
  const params = [];
  const term = String(q || '').trim();
  if (term) {
    conds.push('(商品コード LIKE ? OR 商品名 LIKE ?)');
    const like = `%${term}%`;
    params.push(like, like);
  }
  if (filter === 'missing') conds.push('入数 IS NULL');
  else if (filter === 'new') conds.push("source = 'auto'");
  else if (filter === 'scheduled') conds.push('code_key IN (SELECT code_key FROM f_inbound_schedule)');
  const where = conds.length ? `WHERE ${conds.join(' AND ')}` : '';
  const total = db.prepare(`SELECT COUNT(*) AS c FROM f_inbound_info ${where}`).get(...params).c;
  const lim = Math.min(Math.max(1, Number(limit) || 100), 500);
  const off = Math.max(0, Number(offset) || 0);
  const rows = db.prepare(`
    SELECT code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ, 直接ピックロケ保管,
           BF保管荷姿, いろは在庫化作業有無, memo, source, version, updated_at, updated_by
      FROM f_inbound_info ${where}
     ORDER BY (source = 'auto') DESC, code_key
     LIMIT ? OFFSET ?
  `).all(...params, lim, off);
  return { total, rows, offset: off, limit: lim };
}

// ─── 1行更新 (UI から)。人が触った行は source='manual' に倒して「新着」から外す ───
// expectedVersion: 楽観ロック (Codex R1 Medium / R2 Medium)。編集開始時点の version を必須で渡し、
//   条件付き UPDATE (WHERE version = ?) の changes で判定する。updated_at 比較だと
//   同一ミリ秒衝突と check-then-update の競合窓が残るため、整数 version を正とする。
export function updateInbound(key, fields, user, expectedVersion) {
  const db = getMirrorDB();
  const k = codeKey(key);
  const ver = Number(expectedVersion);
  if (!Number.isInteger(ver) || ver < 1) return { ok: false, error: 'version_required' };

  // 商品名は空にしない (誤クリアで一覧が読めなくなるため)
  if ('商品名' in fields && cleanText(fields.商品名) == null) {
    return { ok: false, error: 'empty_name' };
  }

  const sets = ["source = 'manual'", 'version = version + 1', 'updated_at = ?', 'updated_by = ?'];
  const params = [utcIsoNow(), cleanText(user)];
  if ('入数' in fields) {
    const irisu = parseIrisu(fields.入数);
    if (!irisu.ok) return { ok: false, error: 'invalid_irisu' };
    sets.unshift('入数 = ?');
    params.unshift(irisu.value);
  }
  for (const f of TEXT_FIELDS) {
    if (f in fields) {
      sets.unshift(`${f} = ?`);
      params.unshift(cleanText(fields[f]));
    }
  }
  const info = db.prepare(
    `UPDATE f_inbound_info SET ${sets.join(', ')} WHERE code_key = ? AND version = ?`
  ).run(...params, k, ver);
  if (info.changes === 0) {
    const exists = db.prepare('SELECT 1 FROM f_inbound_info WHERE code_key = ?').get(k);
    return { ok: false, error: exists ? 'conflict' : 'not_found' };
  }
  return { ok: true };
}

// ─── 手動追加 (mirror_products に実在するコードのみ) ───
export function addManual(code, user) {
  const db = getMirrorDB();
  const c = normCode(code);
  if (!c) return { ok: false, error: 'empty_code' };
  const k = codeKey(c);
  if (db.prepare('SELECT 1 FROM f_inbound_info WHERE code_key = ?').get(k)) {
    return { ok: false, error: 'duplicate' };
  }
  const prod = db.prepare(
    'SELECT 商品コード, 商品名 FROM mirror_products WHERE lower(trim(商品コード)) = ? LIMIT 1'
  ).get(k);
  if (!prod) return { ok: false, error: 'not_in_master' };
  const now = utcIsoNow();
  try {
    db.prepare(`
      INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, source, created_at, updated_at, updated_by)
      VALUES (?, ?, ?, 'manual', ?, ?, ?)
    `).run(k, normCode(prod.商品コード), prod.商品名, now, now, cleanText(user));
  } catch (e) {
    // check-then-insert の隙間で同時追加された場合 (Codex R1 Low): PK 違反は duplicate として返す
    if (String(e.code || '').startsWith('SQLITE_CONSTRAINT')) return { ok: false, error: 'duplicate' };
    throw e;
  }
  return { ok: true, code: prod.商品コード };
}

// ─── 1行削除 (誤登録の取り消し用。履歴は持たない単純マスタなのでハード削除) ───
export function deleteInbound(key) {
  const db = getMirrorDB();
  const info = db.prepare('DELETE FROM f_inbound_info WHERE code_key = ?').run(codeKey(key));
  return { ok: info.changes > 0 };
}

// ─── 新商品の自動追加 (cron / 手動「今すぐ取込」共用) ───
// mirror_products の 単品・取扱中 のうち f_inbound_info 未登録コードを INSERT する。
// フィルタ根拠 (2026-07-21 実データ検証):
//   - Excel 4897件中、セット商品は 1 件のみ (セット 2162 件はほぼ全て対象外) → 単品のみ
//   - 商品区分='例外' (89件) も旧シートに 1 件も無し → 対象外
//   - 取扱中止/ﾒｰｶｰ取扱中止は旧シートに履歴として残っているが新規追加はしない
// fail-safe: mirror_products が空 (= miniPC からの同期前 / 同期失敗) の時は何もしない。
export function syncNewProducts() {
  const db = getMirrorDB();
  const mirrorCount = db.prepare('SELECT COUNT(*) AS c FROM mirror_products').get().c;
  if (mirrorCount === 0) {
    return { ok: false, error: 'mirror_empty', inserted: 0 };
  }
  const candidates = db.prepare(`
    SELECT p.商品コード AS code, p.商品名 AS name
      FROM mirror_products p
     WHERE p.商品区分 = '単品'
       AND p.取扱区分 = '取扱中'
       AND lower(trim(p.商品コード)) NOT IN (SELECT code_key FROM f_inbound_info)
  `).all();
  const now = utcIsoNow();
  const ins = db.prepare(`
    INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, source, created_at, updated_at)
    VALUES (?, ?, ?, 'auto', ?, ?)
    ON CONFLICT(code_key) DO NOTHING
  `);
  let inserted = 0;
  db.transaction(() => {
    // mirror_products 内にも lower(trim) 重複があり得るため ON CONFLICT DO NOTHING で吸収
    for (const c of candidates) {
      const k = codeKey(c.code);
      if (!k) continue;
      inserted += ins.run(k, normCode(c.code), c.name, now, now).changes;
    }
  })();
  return { ok: true, inserted, mirror_products: mirrorCount };
}

// ─── Excel 取込 (移行/再取込)。rows は router 側で Excel からパース済み ───
// mode: 'dry_run' なら書き込まずに件数レポートのみ。
// 上書きポリシー: Excel の値で全列上書き (移行ツールとしての位置付け。通常運用は UI 編集)。
export function importInboundRows(rows, { dryRun = false, user = null } = {}) {
  const db = getMirrorDB();
  const report = { total: rows.length, inserted: 0, updated: 0, skipped: 0, warnings: [] };
  const now = utcIsoNow();
  const sel = db.prepare('SELECT code_key FROM f_inbound_info WHERE code_key = ?');
  const ins = db.prepare(`
    INSERT INTO f_inbound_info
      (code_key, 商品コード, 商品名, 入数, 入庫時BCシール貼りフラグ, 直接ピックロケ保管,
       BF保管荷姿, いろは在庫化作業有無, source, created_at, updated_at, updated_by)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'excel', ?, ?, ?)
  `);
  const upd = db.prepare(`
    UPDATE f_inbound_info
       SET 商品コード = ?, 商品名 = ?, 入数 = ?, 入庫時BCシール貼りフラグ = ?,
           直接ピックロケ保管 = ?, BF保管荷姿 = ?, いろは在庫化作業有無 = ?,
           source = 'excel', version = version + 1, updated_at = ?, updated_by = ?
     WHERE code_key = ?
  `);

  const apply = () => {
    const seen = new Set();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const c = normCode(r.商品コード);
      const k = codeKey(c);
      if (!k) {
        report.skipped++;
        continue;
      }
      if (seen.has(k)) {
        report.skipped++;
        if (report.warnings.length < 20) report.warnings.push(`行${r.rowNumber ?? i + 2}: 商品コード重複 (${c})`);
        continue;
      }
      seen.add(k);
      const irisu = parseIrisu(r.入数);
      if (!irisu.ok) {
        report.skipped++;
        if (report.warnings.length < 20) report.warnings.push(`行${r.rowNumber ?? i + 2}: 入数が整数でない (${c}: ${r.入数})`);
        continue;
      }
      const vals = [
        cleanText(r.商品名),
        irisu.value,
        cleanText(r.入庫時BCシール貼りフラグ),
        cleanText(r.直接ピックロケ保管),
        cleanText(r.BF保管荷姿),
        cleanText(r.いろは在庫化作業有無),
      ];
      if (sel.get(k)) {
        if (!dryRun) upd.run(c, ...vals, now, cleanText(user), k);
        report.updated++;
      } else {
        if (!dryRun) ins.run(k, c, ...vals, now, now, cleanText(user));
        report.inserted++;
      }
    }
  };
  if (dryRun) apply();
  else db.transaction(apply)();
  return report;
}

// ─── ブック単位の一括取込 (入数マスタ + 原産国 を単一トランザクションで) ───
// Codex R1 Medium: シートごとに別トランザクションだと原産国側の失敗時に
// 入数マスタだけ更新済みの半端な状態が残るため、本取込は外側で1トランザクションに束ねる。
// (importInboundRows / importOriginRows 内の db.transaction は better-sqlite3 が
//  savepoint として入れ子実行するので、どちらかが throw すれば全体が rollback される)
export function importWorkbook(irisuRows, originRows, { dryRun = false, user = null } = {}) {
  const run = () => ({
    irisu: importInboundRows(irisuRows, { dryRun, user }),
    origin: originRows ? importOriginRows(originRows, { dryRun, user }) : null,
  });
  if (dryRun) return run();
  const db = getMirrorDB();
  return db.transaction(run)();
}

// ─── 入荷予定 (nefuda.csv) : full-replace + f_inbound_info への自動追加 ───
// rows: [{ 商品コード, 商品名, バーコード, 有効期限 }] (nefuda-fetch.js がパース済み)
// 置換とマスタ自動追加を単一トランザクションで行う。
// マスタ自動追加: 入荷予定に載っているのに f_inbound_info に無いコードは「実際に入荷してくる」ため
//   区分に関わらず追加する (商品名は mirror_products を正とし、無ければ CSV の商品名)。
//   mirror_products に無いコードは not_in_master として返し、UI で注意表示する (追加はする)。
export function replaceSchedule(rows, { filename = null, fileModifiedTime = null, user = null } = {}) {
  const db = getMirrorDB();
  const now = utcIsoNow();
  const insSched = db.prepare(`
    INSERT INTO f_inbound_schedule (code_key, 商品コード, 商品名, バーコード, 有効期限, created_at)
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(code_key) DO NOTHING
  `);
  const selInfo = db.prepare('SELECT 1 FROM f_inbound_info WHERE code_key = ?');
  const selProd = db.prepare('SELECT 商品コード, 商品名 FROM mirror_products WHERE lower(trim(商品コード)) = ? LIMIT 1');
  const insInfo = db.prepare(`
    INSERT INTO f_inbound_info (code_key, 商品コード, 商品名, source, created_at, updated_at)
    VALUES (?, ?, ?, 'auto', ?, ?)
  `);
  const result = { schedule_rows: 0, duplicates: 0, added_to_master: 0, not_in_master: [] };
  db.transaction(() => {
    db.prepare('DELETE FROM f_inbound_schedule').run();
    for (const r of rows) {
      const c = normCode(r.商品コード);
      const k = codeKey(c);
      if (!k) continue;
      const info = insSched.run(k, c, cleanText(r.商品名), cleanText(r.バーコード), cleanText(r.有効期限), now);
      if (info.changes === 0) {
        result.duplicates++;
        continue;
      }
      result.schedule_rows++;
      if (!selInfo.get(k)) {
        const prod = selProd.get(k);
        if (!prod && result.not_in_master.length < 20) result.not_in_master.push(c);
        insInfo.run(k, prod ? normCode(prod.商品コード) : c, prod ? prod.商品名 : cleanText(r.商品名), now, now);
        result.added_to_master++;
      }
    }
    db.prepare(`
      INSERT INTO f_inbound_schedule_state (id, fetched_at, row_count, file_modified_time, filename, fetched_by)
      VALUES (1, ?, ?, ?, ?, ?)
      ON CONFLICT(id) DO UPDATE SET
        fetched_at = excluded.fetched_at, row_count = excluded.row_count,
        file_modified_time = excluded.file_modified_time, filename = excluded.filename,
        fetched_by = excluded.fetched_by
    `).run(now, result.schedule_rows, fileModifiedTime, cleanText(filename), cleanText(user));
  })();
  return { ok: true, fetched_at: now, ...result };
}

// 入荷予定の最終取得状態 (未取得なら null)
export function scheduleState() {
  const db = getMirrorDB();
  return db.prepare('SELECT * FROM f_inbound_schedule_state WHERE id = 1').get() || null;
}

// ─── 原産国: 一覧 ───
export function listOrigin() {
  const db = getMirrorDB();
  return db.prepare(`
    SELECT id, 管理コード, 識別番号, 商品コード, 商品名, 産地, 画像有無, version, updated_at, updated_by
      FROM f_inbound_origin
     ORDER BY COALESCE(管理コード, ''), 識別番号, code_key
  `).all();
}

// ─── 原産国: Excel 取込 (管理コード + 商品コード で upsert) ───
export function importOriginRows(rows, { dryRun = false, user = null } = {}) {
  const db = getMirrorDB();
  const report = { total: rows.length, inserted: 0, updated: 0, skipped: 0, warnings: [] };
  const now = utcIsoNow();
  const sel = db.prepare(
    "SELECT id FROM f_inbound_origin WHERE COALESCE(管理コード, '') = ? AND code_key = ?"
  );
  const ins = db.prepare(`
    INSERT INTO f_inbound_origin (管理コード, 識別番号, 商品コード, code_key, 商品名, 産地, 画像有無, created_at, updated_at, updated_by)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  const upd = db.prepare(`
    UPDATE f_inbound_origin
       SET 識別番号 = ?, 商品コード = ?, 商品名 = ?, 産地 = ?, 画像有無 = ?,
           version = version + 1, updated_at = ?, updated_by = ?
     WHERE id = ?
  `);
  const apply = () => {
    const seen = new Set();
    for (let i = 0; i < rows.length; i++) {
      const r = rows[i];
      const c = normCode(r.商品コード);
      const k = codeKey(c);
      if (!k) {
        report.skipped++;
        continue;
      }
      const kanri = cleanText(r.管理コード);
      const uniKey = `${kanri ?? ''} ${k}`;
      if (seen.has(uniKey)) {
        report.skipped++;
        if (report.warnings.length < 20) report.warnings.push(`行${r.rowNumber ?? i + 2}: 管理コード+商品コード重複 (${c})`);
        continue;
      }
      seen.add(uniKey);
      const vals = [cleanText(r.識別番号), c, cleanText(r.商品名), cleanText(r.産地), r.画像有無 ? 1 : 0];
      const hit = sel.get(kanri ?? '', k);
      if (hit) {
        if (!dryRun) upd.run(...vals, now, cleanText(user), hit.id);
        report.updated++;
      } else {
        if (!dryRun) ins.run(kanri, vals[0], c, k, vals[2], vals[3], vals[4], now, now, cleanText(user));
        report.inserted++;
      }
    }
  };
  if (dryRun) apply();
  else db.transaction(apply)();
  return report;
}

// ─── 原産国: 1行更新 / 追加 / 削除 ───
export function upsertOrigin(fields, user) {
  const db = getMirrorDB();
  const c = normCode(fields.商品コード);
  if (!c) return { ok: false, error: 'empty_code' };
  const k = codeKey(c);
  const kanri = cleanText(fields.管理コード);
  const now = utcIsoNow();
  const vals = {
    識別番号: cleanText(fields.識別番号),
    商品名: cleanText(fields.商品名),
    産地: cleanText(fields.産地),
    画像有無: fields.画像有無 ? 1 : 0,
  };
  const id = fields.id != null ? Number(fields.id) : null;
  try {
    if (id != null && Number.isInteger(id)) {
      // 楽観ロック (Codex R1 Medium / R2 Medium): version 必須の条件付き UPDATE で判定
      const ver = Number(fields.expected_version);
      if (!Number.isInteger(ver) || ver < 1) return { ok: false, error: 'version_required' };
      const info = db.prepare(`
        UPDATE f_inbound_origin
           SET 管理コード = ?, 識別番号 = ?, 商品コード = ?, code_key = ?, 商品名 = ?, 産地 = ?, 画像有無 = ?,
               version = version + 1, updated_at = ?, updated_by = ?
         WHERE id = ? AND version = ?
      `).run(kanri, vals.識別番号, c, k, vals.商品名, vals.産地, vals.画像有無, now, cleanText(user), id, ver);
      if (info.changes === 0) {
        const exists = db.prepare('SELECT 1 FROM f_inbound_origin WHERE id = ?').get(id);
        return { ok: false, error: exists ? 'conflict' : 'not_found' };
      }
      return { ok: true, id };
    }
    const info = db.prepare(`
      INSERT INTO f_inbound_origin (管理コード, 識別番号, 商品コード, code_key, 商品名, 産地, 画像有無, created_at, updated_at, updated_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(kanri, vals.識別番号, c, k, vals.商品名, vals.産地, vals.画像有無, now, now, cleanText(user));
    return { ok: true, id: info.lastInsertRowid };
  } catch (e) {
    // UNIQUE(管理コード, code_key) 違反は INSERT/UPDATE どちらの経路でも duplicate として返す
    // (check-then-insert の競合窓を作らないため、事前 SELECT でなく制約違反で判定する)
    if (String(e.code || '').startsWith('SQLITE_CONSTRAINT')) return { ok: false, error: 'duplicate' };
    throw e;
  }
}

export function deleteOrigin(id) {
  const db = getMirrorDB();
  const n = Number(id);
  if (!Number.isInteger(n)) return { ok: false };
  const info = db.prepare('DELETE FROM f_inbound_origin WHERE id = ?').run(n);
  return { ok: info.changes > 0 };
}
