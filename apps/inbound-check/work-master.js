/**
 * いろは作業仕様マスタ (f_iroha_work_master) — 旧「作業内容管理マスター」スプレッドシートの DB 化 (PR2)
 *
 * 設計 = Codex設計相談R1 (Downloads『在庫化カード置き換え_Codex設計相談R1_20260902.md』質問2):
 *  - 持つのは「いろは作業に固有の属性」だけ: 資材 / 収納容器 / 容器あたり数量 / 工程数 / 備考
 *  - 商品名・仕入先・取扱区分は mirror_products を JOIN で引く (旧シートの IMPORTRANGE 列は保存しない)
 *  - **在庫化必要FLG は廃止** (中原さん 2026-09-02)。在庫化要否の正本は f_inbound_info.いろは在庫化作業有無
 *    (荷受け時のその場選択で育つ)。xlsx に FLG 列があっても完全に読み飛ばす
 *  - 旧「入数」は units_per_container と命名して意味を固定 (f_inbound_info.入数 = 仕入箱入数 とは別概念。
 *    統合しない — 値が一致していても同一概念とは判断しない)
 *  - 全て空だった5列 (作業動画URL・作業拠点初期値・外部委託対象・作業工程・単価) は持ち込まない
 *
 * 取込は xlsx を管理画面からアップロード。既定 dry-run (検証レポートのみ)、apply=1 で本取込。
 */
import ExcelJS from 'exceljs';
import { getDB } from './db.js';

const utcNow = () => new Date().toISOString();
export const SHEET_NAME = '作業内容管理マスター';

export function codeKeyOf(code) {
  return String(code == null ? '' : code).trim().toLowerCase();
}

function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

// ─── xlsx の読み取り ───

/** セル値を文字列に解決する。IMPORTRANGE 等の数式セルは計算結果 (result) を使う */
function resolveCell(v) {
  if (v == null) return '';
  if (typeof v === 'object') {
    if (v.result !== undefined && v.result !== null) return String(v.result);
    if (Array.isArray(v.richText)) return v.richText.map((t) => t.text).join('');
    if (v.text != null) return String(v.text);
    if (v.formula || v.sharedFormula) return '';   // 数式だけで結果なし
    return '';
  }
  return String(v);
}

function parseIntOrNull(s) {
  const t = String(s ?? '').trim();
  if (t === '') return { ok: true, value: null };
  const n = Number(t);
  if (!Number.isInteger(n) || n < 0 || n > 100_000_000) return { ok: false, value: null };
  return { ok: true, value: n };
}

/**
 * 「作業内容管理マスター」シートを読み、正規化した行と検証問題を返す。
 * 必須ヘッダー: 商品コード。任意: 資材・収納容器・入数・工程数・備考 (それ以外の列は読み飛ばす)。
 * 重複コードは先勝ち (後の行は duplicates に記録)。
 */
export async function parseWorkMasterXlsx(buffer) {
  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(buffer);
  const ws = wb.getWorksheet(SHEET_NAME);
  if (!ws) {
    const names = wb.worksheets.map((w) => w.name).join(' / ');
    throw new Error(`シート「${SHEET_NAME}」がありません (あるシート: ${names})`);
  }
  // 細工された xlsx (高圧縮で展開後が巨大) の暴走ガード。実ファイルは ~5,100行×15列
  if (ws.rowCount > 50_000 || ws.columnCount > 100) {
    throw new Error(`シートが大きすぎます (${ws.rowCount}行×${ws.columnCount}列。上限 50,000行×100列)`);
  }
  const header = new Map();   // 列名 → 列番号
  ws.getRow(1).eachCell({ includeEmpty: false }, (c, col) => {
    const name = resolveCell(c.value).trim();
    if (name && !header.has(name)) header.set(name, col);
  });
  // ⭐在庫化必要FLG は廃止 (中原さん 2026-09-02:「在庫化必要FLGはもうつかわない」)。
  //   正本は f_inbound_info.いろは在庫化作業有無 (荷受け時のその場選択で育つ)。
  //   列があれば突合レポート/seed に使えるが、無い xlsx も取り込める (必須は商品コードのみ)
  for (const required of ['商品コード']) {
    if (!header.has(required)) throw new Error(`ヘッダーに「${required}」がありません`);
  }
  const col = (name) => header.get(name) || null;
  const cols = {
    code: col('商品コード'), material: col('資材'),
    container: col('収納容器'), units: col('入数'), process: col('工程数') || col('工数数'),
    note: col('備考'),
  };

  const rows = [];
  const seen = new Map();   // codeKey → 行番号 (先勝ち)
  const issues = { duplicates: [], badUnits: [], badProcess: [], numericCode: [], emptyCode: 0 };
  let dataRows = 0;
  ws.eachRow((row, rn) => {
    if (rn === 1) return;
    const get = (c) => (c ? resolveCell(row.getCell(c).value).trim() : '');
    // 商品コードが数値セルだと Excel の表示形式 (先頭ゼロ等) が失われて別SKU化する (Codex PR2 #3)。
    // 黙って String() せず検証エラーにする — 列を文字列形式に直してから取り込み直してもらう
    const rawCode = cols.code ? row.getCell(cols.code).value : null;
    const rawCodeResolved = rawCode && typeof rawCode === 'object' ? rawCode.result : rawCode;
    if (typeof rawCodeResolved === 'number') {
      dataRows++;
      issues.numericCode.push({ row: rn, value: String(rawCodeResolved) });
      return;
    }
    const code = get(cols.code);
    const material = get(cols.material);
    const container = get(cols.container);
    const unitsRaw = get(cols.units);
    const processRaw = get(cols.process);
    const note = get(cols.note);
    if (!code && !material && !container && !unitsRaw && !processRaw && !note) return; // 対象列が全空の行
    dataRows++;
    if (!code) { issues.emptyCode++; return; }
    const k = codeKeyOf(code);
    if (seen.has(k)) { issues.duplicates.push({ code, row: rn, first: seen.get(k) }); return; }
    seen.set(k, rn);

    const units = parseIntOrNull(unitsRaw);
    if (!units.ok) issues.badUnits.push({ code, row: rn, value: unitsRaw });
    const proc = parseIntOrNull(processRaw);
    if (!proc.ok) issues.badProcess.push({ code, row: rn, value: processRaw });

    rows.push({
      code, codeKey: k,
      material: material || null, container: container || null,
      units: units.ok ? units.value : null, processCount: proc.ok ? proc.value : null,
      note: note || null,
    });
  });
  return { rows, issues, dataRows };
}

/**
 * 検証エラーの総数。1件でもあれば**本取込は拒否**する (Codex PR2 High-1)。
 * 「入数 abc」のような不正値を null として取り込むと、既存の 180 を黙って消してしまう。
 * dry-run のレポートで場所が分かるので、xlsx 側を直してから取り込み直してもらう
 */
export function importIssueCount(issues) {
  return (issues.duplicates?.length || 0)
    + (issues.badUnits?.length || 0) + (issues.badProcess?.length || 0)
    + (issues.numericCode?.length || 0) + (issues.emptyCode || 0);
}

// ─── 本取込 (全置換 upsert+delete) ───

/** xlsx に無い既存行 (=取込で削除される行)。dry-run の予告と本取込の両方で使う */
export function computeDeletions(rows) {
  const db = getDB();
  const incoming = new Set(rows.map((r) => r.codeKey));
  const gone = db.prepare('SELECT code_key, 商品コード AS code FROM f_iroha_work_master').all()
    .filter((r) => !incoming.has(r.code_key));
  // codes は**全件**返す (削除予定を全部確認できないと「予告」にならない — Codex PR2-R3)。
  // 画面側が先頭50件+折りたたみで表示する
  return { count: gone.length, codes: gone.map((g) => g.code), keys: gone.map((g) => g.code_key) };
}

export function applyWorkMaster(rows, { user = null } = {}) {
  const db = getDB();
  const now = utcNow();
  const sel = db.prepare('SELECT * FROM f_iroha_work_master WHERE code_key = ?');
  const ins = db.prepare(`INSERT INTO f_iroha_work_master
    (code_key, 商品コード, material_code, storage_container, units_per_container, process_count, note, version, updated_at, updated_by)
    VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)`);
  const upd = db.prepare(`UPDATE f_iroha_work_master
    SET 商品コード = ?, material_code = ?, storage_container = ?, units_per_container = ?, process_count = ?, note = ?,
        version = version + 1, updated_at = ?, updated_by = ? WHERE code_key = ?`);
  const counts = { inserted: 0, updated: 0, unchanged: 0, deleted: 0 };
  const tx = db.transaction(() => {
    for (const r of rows) {
      const cur = sel.get(r.codeKey);
      if (!cur) {
        ins.run(r.codeKey, r.code, r.material, r.container, r.units, r.processCount, r.note, now, user);
        counts.inserted++;
      } else if (
        cur.material_code !== r.material || cur.storage_container !== r.container ||
        cur.units_per_container !== r.units || cur.process_count !== r.processCount || cur.note !== r.note ||
        cur.商品コード !== r.code
      ) {
        upd.run(r.code, r.material, r.container, r.units, r.processCount, r.note, now, user, r.codeKey);
        counts.updated++;
      } else {
        counts.unchanged++;
      }
    }
    // ⭐取込 = マスタ全体の置き換え。xlsx に無い既存行は削除する — upsert だけだと廃止した
    //   作業仕様が残り、以後のカードに誤って載り続ける (Codex PR2-R2 High)。
    //   削除予定は dry-run のレポート (wouldDelete) で先に見せる
    const gone = computeDeletions(rows);
    if (gone.keys.length > 0) {
      const del = db.prepare('DELETE FROM f_iroha_work_master WHERE code_key = ?');
      for (const k of gone.keys) del.run(k);
      counts.deleted = gone.keys.length;
    }
  });
  tx.immediate();
  return counts;
}

export function logWorkMasterImport({ actor, fileName, ok, message }) {
  const db = getDB();
  db.prepare(`INSERT INTO f_inbound_check_import_log (at, actor, source, file_name, ok, message)
    VALUES (?, ?, 'work_master_xlsx', ?, ?, ?)`).run(utcNow(), actor || null, fileName || null, ok ? 1 : 0, message || null);
}

// ─── 管理画面: 検索・編集 ───

export function workMasterStats() {
  const db = getDB();
  const g = (sql) => db.prepare(sql).get();
  const t = g('SELECT COUNT(*) n, MAX(updated_at) m FROM f_iroha_work_master');
  const filled = g(`SELECT COUNT(*) n FROM f_iroha_work_master
    WHERE material_code IS NOT NULL OR storage_container IS NOT NULL OR units_per_container IS NOT NULL OR process_count IS NOT NULL`);
  return { total: t?.n || 0, filled: filled?.n || 0, lastUpdatedAt: t?.m || null };
}

export function searchWorkMaster(q, limit = 50) {
  const db = getDB();
  const like = `%${String(q || '').trim()}%`;
  return db.prepare(`SELECT w.*, p.商品名 AS product_name, p.取扱区分 AS handling
    FROM f_iroha_work_master w
    LEFT JOIN mirror_products p ON LOWER(TRIM(p.商品コード)) = w.code_key
    WHERE w.商品コード LIKE ? OR w.code_key LIKE ? OR p.商品名 LIKE ?
    ORDER BY w.商品コード LIMIT ?`).all(like, like.toLowerCase(), like, Math.max(1, Math.min(200, limit)));
}

const EDIT_FIELDS = ['material_code', 'storage_container', 'units_per_container', 'process_count', 'note', 'video_url', 'size_class'];

export function updateWorkMasterRow(key, fields, user, expectVersion) {
  const db = getDB();
  const k = codeKeyOf(key);
  const ver = Number(expectVersion);
  if (!Number.isInteger(ver) || ver < 1) return { ok: false, error: 'version_required', message: 'version が必要です' };
  const sets = [];
  const params = [];
  for (const f of EDIT_FIELDS) {
    if (!(f in fields)) continue;
    let v = fields[f];
    if (f === 'units_per_container' || f === 'process_count') {
      const p = parseIntOrNull(v);
      if (!p.ok) return { ok: false, error: 'bad_number', message: `${f} は 0 以上の整数か空にしてください` };
      v = p.value;
    } else {
      v = String(v ?? '').trim() || null;
      // 動画リンクは http(s) のみ (javascript: 等を画面のリンクにしない)
      if (f === 'video_url' && v != null && !/^https?:\/\/\S+$/i.test(v)) {
        return { ok: false, error: 'bad_url', message: '作り方動画は http(s) のリンクを入れてください' };
      }
      if (v != null && v.length > 500) return { ok: false, error: 'too_long', message: `${f} が長すぎます (500文字まで)` };
    }
    sets.push(`${f} = ?`);
    params.push(v);
  }
  if (sets.length === 0) return { ok: false, error: 'no_fields', message: '更新する項目がありません' };
  const r = db.prepare(`UPDATE f_iroha_work_master
    SET ${sets.join(', ')}, version = version + 1, updated_at = ?, updated_by = ?
    WHERE code_key = ? AND version = ?`).run(...params, utcNow(), user || null, k, ver);
  if (r.changes === 0) {
    const exists = db.prepare('SELECT version FROM f_iroha_work_master WHERE code_key = ?').get(k);
    if (!exists) return { ok: false, error: 'not_found', message: 'その商品コードはマスタにありません' };
    return { ok: false, error: 'conflict', message: '他の人が先に更新しました (画面を読み込み直してください)', currentVersion: exists.version };
  }
  return { ok: true, row: db.prepare('SELECT * FROM f_iroha_work_master WHERE code_key = ?').get(k) };
}

/** 手動で1行足す (取込に無い新商品用)。mirror_products に居る商品だけ許可 (打ち間違い防止) */
export function addWorkMasterRow(code, user) {
  const db = getDB();
  const c = String(code || '').trim();
  if (!c) return { ok: false, error: 'empty_code', message: '商品コードを入力してください' };
  const k = codeKeyOf(c);
  const prod = db.prepare('SELECT 商品コード FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ? LIMIT 1').get(k);
  if (!prod) return { ok: false, error: 'not_in_master', message: '商品マスタ (mirror_products) に無い商品コードです' };
  try {
    db.prepare(`INSERT INTO f_iroha_work_master (code_key, 商品コード, version, updated_at, updated_by)
      VALUES (?, ?, 1, ?, ?)`).run(k, prod.商品コード, utcNow(), user || null);
  } catch (e) {
    if (String(e.code || '').startsWith('SQLITE_CONSTRAINT')) return { ok: false, error: 'duplicate', message: '既に登録されています' };
    throw e;
  }
  return { ok: true, code: prod.商品コード };
}
