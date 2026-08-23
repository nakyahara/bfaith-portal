/**
 * ロケーション動線マスタ (面マスタ) — NEXTサインの土台。
 *
 * 設計書: AI_reference/システム設計/ピッキングNEXTサイン_要件定義_20260823.md
 *
 * 「面」= 1本の棚の片側 (表/裏/片面)。ピッキングフロア(P3F)は一筆書きなので、面を歩く順
 * (seq_no) に並べ、各面の連の範囲を持てば、ロケ8桁 → 面 → 歩行上の位置 が機械的に決まる。
 * 2026-08-23 現場確認で 45面/28棚/503マス (apps/picking/location-faces.default.csv)。
 *
 * - pk_lines / 表示順 (seq) には一切触れない。このマスタは「NEXTの見せ方」にだけ使う
 * - 取込は CSV (UTF-8/Shift_JIS・日本語ヘッダ)。検証 → 全置換 → 履歴 (CSV全文+sha256) を残す
 *   (間違った版を入れても前の版のCSVを取り出して入れ直せる)
 * - 45行なのでメモリに載せ、取込があれば再読込する
 * - マスタが空なら同梱CSVで初期化する (miniPCデプロイで手順を増やさない)
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { parseCsv, decodeCp932 } from '../packing-dispatch/csv.js';
import { getDB, utcNow } from './db.js';
import { PkError } from './service.js';
import core from './next-sign-core.cjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
export const DEFAULT_CSV_PATH = path.join(__dirname, 'location-faces.default.csv');

// CSV列 (日本語ヘッダ) ↔ 内部コード。人が Excel で直して再取込できる形にする
export const CSV_HEADERS = ['面番号', 'ブロック', '列', '連from', '連to', '面の種類', '棚ID', '前の面からの動き', '信頼', '向き', 'メモ'];
const FACE_KIND_JA = { '表': 'front', '裏': 'back', '続き': 'cont', '片面': 'single' };
const MOVE_JA = {
  '開始': 'start', '折り返す': 'forward_turn', '振り向く': 'turn_around', '階段を回る': 'around_stairs',
  '階段の向こう': 'across_stairs', '吊り下げ': 'hang', '移動': 'move',
};
const DIR_JA = { '左': 'left', '右': 'right', '': 'left' };
const inv = (o) => Object.fromEntries(Object.entries(o).map(([k, v]) => [v, k]));
const FACE_KIND_TO_JA = inv(FACE_KIND_JA);
const MOVE_TO_JA = inv(MOVE_JA);

/** CSV (Buffer) → 文字列。BOM付きUTF-8 (Excel/本システムの出力) → UTF-8 → それ以外は Shift_JIS とみなす。 */
export function decodeFacesCsv(buffer) {
  const buf = Buffer.isBuffer(buffer) ? buffer : Buffer.from(buffer || '');
  if (buf.length === 0) throw new PkError(400, 'empty_file', 'ファイルが空です');
  if (buf.length >= 3 && buf[0] === 0xef && buf[1] === 0xbb && buf[2] === 0xbf) return buf.slice(3).toString('utf8');
  const utf = buf.toString('utf8');
  return utf.includes('�') ? decodeCp932(buf) : utf;
}

/** CSV (Buffer) → 面の配列。 */
export function parseFacesCsv(buffer) {
  const text = decodeFacesCsv(buffer);
  const state = {};
  const table = parseCsv(text, state).filter((r) => !(r.length === 1 && r[0] === ''));
  if (state.unclosedQuote) throw new PkError(400, 'broken_csv', 'CSVの引用符が閉じていません');
  if (table.length < 2) throw new PkError(400, 'no_rows', 'データ行がありません');
  const header = table[0].map((h) => String(h).replace(/^﻿/, '').trim());
  const missing = CSV_HEADERS.filter((h) => !header.includes(h) && !['メモ', '向き', '信頼'].includes(h));
  if (missing.length > 0) throw new PkError(400, 'missing_columns', `必須列がありません: ${missing.join(', ')}`);
  const rows = table.slice(1).map((cells) => Object.fromEntries(header.map((h, i) => [h, String(cells[i] ?? '').trim()])));

  const faces = rows.map((r, i) => {
    const rowNo = i + 2;
    const bad = (msg) => { throw new PkError(400, 'bad_row', `行${rowNo}: ${msg}`); };
    const seq = Number(r['面番号']);
    if (!Number.isInteger(seq) || seq <= 0) bad(`面番号「${r['面番号']}」が正の整数ではありません`);
    const block = String(r['ブロック'] || '').trim().toUpperCase();
    if (!/^[A-Z0-9]{2,6}$/.test(block)) bad(`ブロック「${r['ブロック']}」の形式が不正です`);
    const colRaw = String(r['列'] || '').trim();
    if (!/^\d{1,3}$/.test(colRaw)) bad(`列「${colRaw}」は1〜3桁の数字にしてください`);
    const col = colRaw.padStart(3, '0');
    const from = Number(r['連from']), to = Number(r['連to']);
    if (!Number.isInteger(from) || !Number.isInteger(to) || from <= 0 || to < from) bad(`連from/連to (${r['連from']}〜${r['連to']}) が不正です`);
    const kindJa = String(r['面の種類'] || '').trim();
    if (!(kindJa in FACE_KIND_JA)) bad(`面の種類「${kindJa}」は 表/裏/続き/片面 のどれかにしてください`);
    const rack = String(r['棚ID'] || '').trim();
    if (!rack) bad('棚IDが空です');
    const moveJa = String(r['前の面からの動き'] || '').trim();
    if (!(moveJa in MOVE_JA)) bad(`前の面からの動き「${moveJa}」は ${Object.keys(MOVE_JA).join('/')} のどれかにしてください`);
    const relRaw = String(r['信頼'] ?? '1').trim();
    if (!['0', '1', ''].includes(relRaw)) bad(`信頼「${relRaw}」は 1 か 0 にしてください`);
    const dirJa = String(r['向き'] ?? '').trim();
    if (!(dirJa in DIR_JA)) bad(`向き「${dirJa}」は 左 か 右 にしてください`);
    return {
      seq_no: seq, block, col, ren_from: from, ren_to: to,
      face_kind: FACE_KIND_JA[kindJa], rack_id: rack, move_in: MOVE_JA[moveJa],
      reliable: relRaw === '0' ? 0 : 1, direction: DIR_JA[dirJa],
      storage_kind: moveJa === '吊り下げ' ? 'pocket' : 'shelf',
      note: String(r['メモ'] || '').trim() || null,
    };
  });
  return faces;
}

/**
 * 構造検証。errors があれば取込不可、warnings は取り込めるが確認を促す。
 * 通し位置 (slot) もここで振る。
 */
export function validateFaces(faces, { knownLocations = null } = {}) {
  const errors = [], warnings = [];
  const sorted = [...faces].sort((a, b) => a.seq_no - b.seq_no);

  // 面番号は 1..N の連番
  sorted.forEach((f, i) => { if (f.seq_no !== i + 1) errors.push(`面番号が連番ではありません (面${f.seq_no} が ${i + 1} 番目)`); });
  if (sorted.length > 0 && sorted[0].move_in !== 'start') errors.push('面1 の「前の面からの動き」は 開始 にしてください');
  sorted.slice(1).forEach((f) => { if (f.move_in === 'start') errors.push(`面${f.seq_no}: 「開始」は面1だけです`); });

  // 同じ列の面は 連1 から隙間なく並び、面番号も連続する
  const byCol = new Map();
  for (const f of sorted) {
    const k = `${f.block}-${f.col}`;
    if (!byCol.has(k)) byCol.set(k, []);
    byCol.get(k).push(f);
  }
  for (const [k, list] of byCol) {
    list.sort((a, b) => a.ren_from - b.ren_from);
    if (list[0].ren_from !== 1) errors.push(`${k}: 連1 から始まっていません (最初の面が連${list[0].ren_from})`);
    for (let i = 1; i < list.length; i++) {
      if (list[i].ren_from !== list[i - 1].ren_to + 1) errors.push(`${k}: 連${list[i - 1].ren_to} と 連${list[i].ren_from} の間に隙間/重なりがあります`);
      if (list[i].seq_no !== list[i - 1].seq_no + 1) errors.push(`${k}: 同じ列の面は面番号も連続させてください (面${list[i - 1].seq_no} → 面${list[i].seq_no})`);
    }
    const kinds = list.map((f) => f.face_kind).join(',');
    if (list.length === 1 && !['single', 'front', 'back'].includes(kinds)) errors.push(`${k}: 面が1つなら 面の種類は 片面 (または表/裏) にしてください`);
    if (list.length >= 2 && (list[0].face_kind !== 'front' || list[1].face_kind !== 'back')) errors.push(`${k}: 面が複数なら 表 → 裏 (→ 続き) の順にしてください`);
    if (list.length >= 3 && list.slice(2).some((f) => f.face_kind !== 'cont')) errors.push(`${k}: 3つ目以降の面は 続き にしてください`);
  }

  // 棚ID: 2面まで。2面なら面番号が隣り合い、後の面は 折り返す
  const byRack = new Map();
  for (const f of sorted) { if (!byRack.has(f.rack_id)) byRack.set(f.rack_id, []); byRack.get(f.rack_id).push(f); }
  for (const [rack, list] of byRack) {
    const sameCol = new Set(list.map((f) => `${f.block}-${f.col}`)).size === 1;
    if (!sameCol && list.length > 2) errors.push(`棚ID ${rack}: 列をまたぐ棚は2面までです`);
    if (list.length === 2 && list[1].seq_no !== list[0].seq_no + 1) errors.push(`棚ID ${rack}: 同じ棚の2面は面番号を隣り合わせにしてください`);
  }
  for (let i = 1; i < sorted.length; i++) {
    const prev = sorted[i - 1], f = sorted[i];
    if (f.move_in === 'forward_turn' && f.rack_id !== prev.rack_id) errors.push(`面${f.seq_no}: 「折り返す」なのに前の面 (面${prev.seq_no}) と棚IDが違います`);
    if (f.move_in === 'turn_around' && f.rack_id === prev.rack_id) errors.push(`面${f.seq_no}: 「振り向く」なのに前の面と同じ棚IDです (同じ棚の裏なら 折り返す)`);
    if (f.storage_kind === 'pocket' && f.face_kind !== 'single') warnings.push(`面${f.seq_no}: 吊り下げは 片面 として登録してください`);
  }

  // 通し位置
  let slot = 0;
  const withSlots = sorted.map((f) => {
    const len = f.ren_to - f.ren_from + 1;
    const out = { ...f, slot_from: slot + 1, slot_to: slot + len };
    slot += len;
    return out;
  });

  // 実績のロケがマスタに載るか (警告)
  if (knownLocations) {
    const idx = core.buildIndex(withSlots);
    const miss = new Map();
    for (const { block, location, count } of knownLocations) {
      if (!core.locate(idx, block, location)) {
        const k = `${block}-${String(location).slice(0, 3)}`;
        miss.set(k, (miss.get(k) || 0) + (count || 1));
      }
    }
    for (const [k, c] of [...miss].sort((a, b) => b[1] - a[1]).slice(0, 20)) warnings.push(`実績のロケ ${k}-xxx がマスタに載りません (${c}明細)`);
    if (miss.size > 20) warnings.push(`…他 ${miss.size - 20} 列`);
  }
  return { ok: errors.length === 0, errors, warnings, faces: withSlots, totalSlots: slot };
}

/** pk_lines にある P3F ロケを (block, location, count) で列挙 (検証の突合用)。 */
export function knownLocationsFromLines() {
  try {
    return getDB().prepare(`
      SELECT block, location, COUNT(*) count FROM pk_lines
      WHERE block LIKE 'P3F%' AND length(location)=8 GROUP BY block, location
    `).all();
  } catch { return []; }
}

let cache = null;   // { importId, index, faces }

function currentImportId() {
  return getDB().prepare('SELECT MAX(id) id FROM pk_location_face_imports').get()?.id || 0;
}

/** 面マスタ (配列・seq_no順)。 */
export function listFaces() {
  return getDB().prepare('SELECT * FROM pk_location_faces ORDER BY seq_no').all();
}

/** 検索インデックス (キャッシュ。取込で id が進んだら再読込)。 */
export function getFaceIndex() {
  const id = currentImportId();
  if (!cache || cache.importId !== id) {
    const faces = listFaces();
    cache = { importId: id, faces, index: core.buildIndex(faces) };
  }
  return cache.index;
}

/**
 * 取込 (全置換)。検証エラーがあれば投げる。履歴に CSV 全文を残す。
 * @returns {{importId, faceCount, totalSlots, warnings}}
 */
export function importFaces(buffer, { actor, filename = null } = {}) {
  const faces = parseFacesCsv(buffer);
  const v = validateFaces(faces, { knownLocations: knownLocationsFromLines() });
  if (!v.ok) throw new PkError(400, 'invalid_faces', `検証エラー:\n${v.errors.join('\n')}`);
  const db = getDB();
  const sha = crypto.createHash('sha256').update(buffer).digest('hex');
  // 履歴はデコード済み (UTF-8) で残す。Shift_JIS の原本をそのまま utf8 で読むと化けて復元できない (Codexレビュー P1)
  const csvText = decodeFacesCsv(buffer);
  const run = db.transaction(() => {
    const r = db.prepare(`
      INSERT INTO pk_location_face_imports (imported_at, actor, filename, sha256, face_count, total_slots, csv_text)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(utcNow(), actor || 'system', filename, sha, v.faces.length, v.totalSlots, csvText);
    const importId = Number(r.lastInsertRowid);
    db.prepare('DELETE FROM pk_location_faces').run();
    const ins = db.prepare(`
      INSERT INTO pk_location_faces (seq_no, block, col, ren_from, ren_to, face_kind, rack_id, move_in,
        reliable, direction, storage_kind, slot_from, slot_to, note, import_id)
      VALUES (@seq_no, @block, @col, @ren_from, @ren_to, @face_kind, @rack_id, @move_in,
        @reliable, @direction, @storage_kind, @slot_from, @slot_to, @note, @import_id)
    `);
    for (const f of v.faces) ins.run({ ...f, import_id: importId });
    return importId;
  });
  const importId = run();
  cache = null;
  return { importId, faceCount: v.faces.length, totalSlots: v.totalSlots, warnings: v.warnings };
}

/** 取込履歴 (新しい順)。csv_text は含めない。 */
export function listFaceImports(limit = 20) {
  return getDB().prepare(`
    SELECT id, imported_at, actor, filename, sha256, face_count, total_slots
    FROM pk_location_face_imports ORDER BY id DESC LIMIT ?
  `).all(limit);
}

export function getFaceImportCsv(id) {
  return getDB().prepare('SELECT filename, csv_text FROM pk_location_face_imports WHERE id = ?').get(id) || null;
}

/** 現在のマスタを CSV (UTF-8 BOM・日本語ヘッダ) に戻す (Excel で直して再取込する用)。 */
export function exportFacesCsv(faces = listFaces()) {
  // Excel の数式インジェクション対策: = + - @ やタブ/改行で始まる値は先頭に ' を付けて文字列にする (Codexレビュー P2)
  const q = (v) => {
    let t = String(v ?? '');
    if (/^[=+\-@\t\r]/.test(t)) t = `'${t}`;
    return `"${t.replace(/"/g, '""')}"`;
  };
  const lines = [CSV_HEADERS.map(q).join(',')];
  for (const f of faces) {
    lines.push([
      f.seq_no, f.block, f.col, f.ren_from, f.ren_to, FACE_KIND_TO_JA[f.face_kind] || f.face_kind, f.rack_id,
      MOVE_TO_JA[f.move_in] || f.move_in, f.reliable, f.direction === 'right' ? '右' : '左', f.note || '',
    ].map(q).join(','));
  }
  return '﻿' + lines.join('\r\n') + '\r\n';
}

/** マスタが空なら同梱CSVで初期化する (起動時に1回)。同梱CSVが無い/壊れている場合は警告だけ出して続行。 */
export function ensureLocationFacesSeeded() {
  try {
    const n = getDB().prepare('SELECT COUNT(*) c FROM pk_location_faces').get().c;
    if (n > 0) return false;
    if (!fs.existsSync(DEFAULT_CSV_PATH)) { console.warn('[picking] 面マスタ初期CSVが見つかりません:', DEFAULT_CSV_PATH); return false; }
    const r = importFaces(fs.readFileSync(DEFAULT_CSV_PATH), { actor: 'seed', filename: path.basename(DEFAULT_CSV_PATH) });
    console.log(`[picking] 面マスタを初期化しました: ${r.faceCount}面 / ${r.totalSlots}マス`);
    return true;
  } catch (e) {
    console.warn('[picking] 面マスタの初期化に失敗 (NEXTサインはコード表示のみになります):', e.message);
    return false;
  }
}
