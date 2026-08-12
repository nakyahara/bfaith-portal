/**
 * inquiry-hub テンプレート/Q&A — メールディーラーのエクスポートCSV取込 + 一覧/CRUDロジック
 *
 * 取込元 (メールディーラー https://support.maildealer.jp/ のエクスポート機能、UTF-8 CSV):
 *   - テンプレート: ヘッダ「テンプレートID,テンプレート名,件名,本文（上）,本文（下）,To,From,...,テンプレートグループ,登録日,最終更新日」
 *   - Q&A:        ヘッダ「Q&A ID,カテゴリID,カテゴリ名,件名,質問内容,回答,備考,担当者,登録日,最終更新日,公開状況」
 *
 * 取込は external_id (メールディーラー側ID) をキーに UPSERT (再取込しても重複しない・CSV側の値で上書き)。
 * 手動追加分 (external_id NULL) には触らない。削除は論理削除 (is_active=0) のみ。
 */
import { getDB, toUtcIso } from './db.js';

/**
 * RFC4180厳格CSVパース (引用符内の改行・"" エスケープ・CRLF/LF/CR改行対応)。
 * 構文不正 (セル途中の引用符 / 閉じ引用符後の余分な文字 / 未クローズの引用符) は
 * 黙って壊れた行を作らず throw する (Codex R1 high: 静かなデータ破損の防止)。
 */
export function parseCsv(text, { maxRows = 100000 } = {}) {
  const s = String(text).replace(new RegExp('^\\uFEFF'), ''); // BOM除去
  const rows = [];
  let row = [], field = '', i = 0;
  // state: start=セル先頭 / quoted=引用符内 / afterQuote=閉じ引用符直後 / raw=非引用セル
  let state = 'start';
  // sawAny: この行に区切り文字・引用符・文字が1つでも出たか。
  // 物理的な空行だけを無視し、',,' や '"",""' のような「空セルを明示した行」は
  // 行として残す (列数チェックに乗せる。Codex R2 medium)
  let sawAny = false;
  const n = s.length;
  const endField = () => { row.push(field); field = ''; state = 'start'; };
  const endRow = () => {
    endField();
    if (sawAny) rows.push(row);
    row = []; sawAny = false;
    if (rows.length > maxRows) throw new Error(`CSVの行数が上限 (${maxRows}) を超えています`);
  };
  while (i < n) {
    const c = s[i];
    if (state === 'quoted') {
      if (c === '"') {
        if (s[i + 1] === '"') { field += '"'; i += 2; continue; }
        state = 'afterQuote'; i++; continue;
      }
      field += c; i++; continue;
    }
    if (c === '"') {
      if (state === 'start') { state = 'quoted'; sawAny = true; i++; continue; }
      throw new Error(`CSV解析エラー (${rows.length + 1}行目付近): セル途中に引用符があります`);
    }
    if (c === ',') { endField(); sawAny = true; i++; continue; }
    if (c === '\n') { endRow(); i++; continue; }
    if (c === '\r') { endRow(); i += s[i + 1] === '\n' ? 2 : 1; continue; }
    if (state === 'afterQuote') throw new Error(`CSV解析エラー (${rows.length + 1}行目付近): 閉じ引用符の後に余分な文字があります`);
    state = 'raw'; field += c; sawAny = true; i++;
  }
  if (state === 'quoted') throw new Error('CSV解析エラー: 引用符が閉じていません');
  endRow(); // 最終行 (改行なしEOF)。物理空行なら sawAny=false で無視される
  return rows;
}

/** ヘッダ行から列名→indexのマップを作る (前後空白は無視)。必須列が無ければthrow */
function headerMap(headerRow, requiredCols) {
  const map = {};
  headerRow.forEach((h, i) => { const k = String(h).trim(); if (k && !(k in map)) map[k] = i; });
  for (const col of requiredCols) {
    if (!(col in map)) throw new Error(`CSVに必須列「${col}」がありません。メールディーラーのエクスポートCSVか確認してください`);
  }
  return map;
}

/** メールディーラーの日時 '2021/07/08 07:13:21' (JST) → 正準UTC。空・不正はnull */
function mdDate(v) {
  const s = String(v || '').trim();
  const m = s.match(/^(\d{4})\/(\d{1,2})\/(\d{1,2})[ T](\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!m) return null;
  const pad = n => String(n).padStart(2, '0');
  try { return toUtcIso(`${m[1]}-${pad(m[2])}-${pad(m[3])}T${pad(m[4])}:${m[5]}:${m[6] || '00'}+09:00`); }
  catch { return null; }
}

const cell = (row, map, col) => String(row[map[col]] ?? '').trim();

/** テンプレートCSV取込。戻り値 { total, inserted, updated, skipped, errors[] } */
export function importTemplatesCsv(csvText) {
  const rows = parseCsv(csvText);
  if (!rows.length) throw new Error('CSVが空です');
  const map = headerMap(rows[0], ['テンプレートID', 'テンプレート名', '本文（上）', 'テンプレートグループ']);
  const db = getDB();
  const result = { total: rows.length - 1, inserted: 0, updated: 0, skipped: 0, errors: [] };

  const upsert = db.prepare(`INSERT INTO reply_templates
      (external_id, template_name, subject, template_body, body_bottom, keywords, notes,
       sort_order, category, source_created_at, source_updated_at, updated_at)
    VALUES (@external_id, @template_name, @subject, @template_body, @body_bottom, @keywords, @notes,
       @sort_order, @category, @source_created_at, @source_updated_at, strftime('%Y-%m-%dT%H:%M:%SZ','now'))
    ON CONFLICT(external_id) WHERE external_id IS NOT NULL DO UPDATE SET
      template_name = excluded.template_name, subject = excluded.subject,
      template_body = excluded.template_body, body_bottom = excluded.body_bottom,
      keywords = excluded.keywords, notes = excluded.notes,
      sort_order = excluded.sort_order, category = excluded.category,
      source_created_at = excluded.source_created_at, source_updated_at = excluded.source_updated_at,
      is_active = 1, updated_at = excluded.updated_at`);

  const width = rows[0].length;
  db.transaction(() => {
    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      if (r.length !== width) { result.skipped++; result.errors.push(`${i + 1}行目: 列数不一致 (${r.length}列/期待${width}列)`); continue; }
      const externalId = cell(r, map, 'テンプレートID');
      const name = cell(r, map, 'テンプレート名');
      const body = String(r[map['本文（上）']] ?? ''); // 本文は trim しない (整形を保持)
      if (!externalId || !name) { result.skipped++; result.errors.push(`${i + 1}行目: ID または テンプレート名が空`); continue; }
      if (!body.trim()) { result.skipped++; result.errors.push(`${i + 1}行目 (${name}): 本文（上）が空`); continue; }
      const kw = [cell(r, map, '重要キーワード'), cell(r, map, 'キーワード')].filter(Boolean).join(' / ') || null;
      const values = {
        external_id: externalId,
        template_name: name,
        subject: cell(r, map, '件名') || null,
        template_body: body,
        body_bottom: (map['本文（下）'] != null && String(r[map['本文（下）']] ?? '').trim()) ? String(r[map['本文（下）']]) : null,
        keywords: kw,
        notes: cell(r, map, '備考') || null,
        sort_order: Number(cell(r, map, '表示順序')) || null,
        category: cell(r, map, 'テンプレートグループ') || null,
        source_created_at: mdDate(cell(r, map, '登録日')),
        source_updated_at: mdDate(cell(r, map, '最終更新日')),
      };
      // 手動CRUDと同じ長さ制限を取込にも適用 (Codex R1 medium: 制限迂回で巨大レコード化するのを防ぐ)
      const over = fieldOverLimit(values, {
        template_name: [200, 'テンプレート名'], subject: [500, '件名'], template_body: [50000, '本文（上）'],
        body_bottom: [50000, '本文（下）'], keywords: [500, 'キーワード'], notes: [1000, '備考'], category: [200, 'グループ'],
      });
      if (over) { result.skipped++; result.errors.push(`${i + 1}行目 (${name}): ${over}`); continue; }
      const existed = db.prepare('SELECT 1 FROM reply_templates WHERE external_id = ?').get(externalId);
      upsert.run(values);
      if (existed) result.updated++; else result.inserted++;
    }
  })();
  return result;
}

/** 長さ制限チェック。超過があれば「◯◯が長すぎます」を返す (なければnull) */
function fieldOverLimit(values, limits) {
  for (const [key, [max, label]] of Object.entries(limits)) {
    const v = values[key];
    if (v != null && String(v).length > max) return `${label}が長すぎます (${max}文字まで)`;
  }
  return null;
}

/** Q&A CSV取込。戻り値 { total, inserted, updated, skipped, errors[] } */
export function importQaCsv(csvText) {
  const rows = parseCsv(csvText);
  if (!rows.length) throw new Error('CSVが空です');
  const map = headerMap(rows[0], ['Q&A ID', 'カテゴリ名', '件名', '回答']);
  const db = getDB();
  const result = { total: rows.length - 1, inserted: 0, updated: 0, skipped: 0, errors: [] };

  const upsert = db.prepare(`INSERT INTO qa_entries
      (external_id, category, title, question, answer, notes, staff, is_published,
       source_created_at, source_updated_at, updated_at)
    VALUES (@external_id, @category, @title, @question, @answer, @notes, @staff, @is_published,
       @source_created_at, @source_updated_at, strftime('%Y-%m-%dT%H:%M:%SZ','now'))
    ON CONFLICT(external_id) WHERE external_id IS NOT NULL DO UPDATE SET
      category = excluded.category, title = excluded.title, question = excluded.question,
      answer = excluded.answer, notes = excluded.notes, staff = excluded.staff,
      is_published = excluded.is_published,
      source_created_at = excluded.source_created_at, source_updated_at = excluded.source_updated_at,
      is_active = 1, updated_at = excluded.updated_at`);

  const width = rows[0].length;
  db.transaction(() => {
    for (let i = 1; i < rows.length; i++) {
      const r = rows[i];
      if (r.length !== width) { result.skipped++; result.errors.push(`${i + 1}行目: 列数不一致 (${r.length}列/期待${width}列)`); continue; }
      const externalId = cell(r, map, 'Q&A ID');
      const title = cell(r, map, '件名');
      const answer = String(r[map['回答']] ?? '');
      if (!externalId || !title) { result.skipped++; result.errors.push(`${i + 1}行目: Q&A ID または 件名が空`); continue; }
      if (!answer.trim()) { result.skipped++; result.errors.push(`${i + 1}行目 (${title}): 回答が空`); continue; }
      const values = {
        external_id: externalId,
        category: cell(r, map, 'カテゴリ名') || null,
        title,
        question: (map['質問内容'] != null && String(r[map['質問内容']] ?? '').trim()) ? String(r[map['質問内容']]) : null,
        answer,
        notes: cell(r, map, '備考') || null,
        staff: cell(r, map, '担当者') || null,
        is_published: cell(r, map, '公開状況') === '非公開' ? 0 : 1,
        source_created_at: mdDate(cell(r, map, '登録日')),
        source_updated_at: mdDate(cell(r, map, '最終更新日')),
      };
      const over = fieldOverLimit(values, {
        title: [300, '件名'], category: [200, 'カテゴリ名'], question: [50000, '質問内容'],
        answer: [50000, '回答'], notes: [1000, '備考'], staff: [200, '担当者'],
      });
      if (over) { result.skipped++; result.errors.push(`${i + 1}行目 (${title}): ${over}`); continue; }
      const existed = db.prepare('SELECT 1 FROM qa_entries WHERE external_id = ?').get(externalId);
      upsert.run(values);
      if (existed) result.updated++; else result.inserted++;
    }
  })();
  return result;
}

const likeEsc = s => String(s).replace(/[\\%_]/g, c => '\\' + c);

/** テンプレート一覧 (q: {category, q})。カテゴリ順+表示順 */
export function listTemplates(q = {}) {
  const db = getDB();
  const where = ['is_active = 1'];
  const params = [];
  if (q.category) { where.push('category = ?'); params.push(String(q.category)); }
  const kw = String(q.q || '').trim();
  if (kw) {
    const like = `%${likeEsc(kw)}%`;
    where.push(`(template_name LIKE ? ESCAPE '\\' OR subject LIKE ? ESCAPE '\\'
      OR template_body LIKE ? ESCAPE '\\' OR keywords LIKE ? ESCAPE '\\' OR category LIKE ? ESCAPE '\\')`);
    for (let k = 0; k < 5; k++) params.push(like);
  }
  const rows = db.prepare(`SELECT * FROM reply_templates WHERE ${where.join(' AND ')}
    ORDER BY category IS NULL, category, COALESCE(sort_order, 999999), template_name`).all(...params);
  const categories = db.prepare(`SELECT category, COUNT(*) AS c FROM reply_templates
    WHERE is_active = 1 AND category IS NOT NULL GROUP BY category ORDER BY category`).all();
  return { rows, categories };
}

/** テンプレート1件 (返信エディタの「テンプレートを本文へ反映」用)。無効・不存在は null */
export function getTemplate(id) {
  if (!Number.isInteger(id)) return null;
  return getDB().prepare('SELECT * FROM reply_templates WHERE id = ? AND is_active = 1').get(id) || null;
}

/** Q&A一覧 (q: {category, q}) */
export function listQa(q = {}) {
  const db = getDB();
  const where = ['is_active = 1'];
  const params = [];
  if (q.category) { where.push('category = ?'); params.push(String(q.category)); }
  const kw = String(q.q || '').trim();
  if (kw) {
    const like = `%${likeEsc(kw)}%`;
    where.push(`(title LIKE ? ESCAPE '\\' OR question LIKE ? ESCAPE '\\'
      OR answer LIKE ? ESCAPE '\\' OR notes LIKE ? ESCAPE '\\' OR category LIKE ? ESCAPE '\\')`);
    for (let k = 0; k < 5; k++) params.push(like);
  }
  const rows = db.prepare(`SELECT * FROM qa_entries WHERE ${where.join(' AND ')}
    ORDER BY category IS NULL, category, title`).all(...params);
  const categories = db.prepare(`SELECT category, COUNT(*) AS c FROM qa_entries
    WHERE is_active = 1 AND category IS NOT NULL GROUP BY category ORDER BY category`).all();
  return { rows, categories };
}
