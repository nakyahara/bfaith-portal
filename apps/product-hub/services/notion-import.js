/**
 * Notion 商品マスター → product-hub 取り込み (検証用の選択インポート)。
 *
 * 方針 (中原さん 2026-07-25):
 *   Notion で作られた既存カードは **Notion 側で運用**し、今後の新商品はアプリ起点。
 *   ただし「アプリで既存商品を扱えるか」をテスト検証したいので、商品コード指定で一部だけ取り込む。
 *
 * 設計原則:
 *   - **読み取り専用**: 取り込んだ行は source='notion_import' で印を付け、Notion へ一切書き戻さない。
 *     (notion-card.js の syncCardLinks / attemptCardCreation がこのフラグで抜ける)
 *     これが無いと、公式URL が空のまま「基本情報を保存」した瞬間に Notion 側の
 *     メーカーページURL / amazon販売ページ が { url: null } でクリアされ、実データを壊す。
 *   - 商品コード = 楽天 manage_number = Yahoo item_code = NE商品コード (field_mapping 20260527)。
 *     product_drafts.ne_code と同一キーなので突合はそのまま。
 *   - ポータル起点 (source='portal') の行は取り込みで上書きしない (conflict として弾く)。
 *   - Notion の運用ステータス (⓪新規商品_高島 等) は product_drafts.status の 8 状態と別軸なので、
 *     status は 'draft' 固定にし、原文は source_notion_status に保持する。
 *
 * 関連: RYS lib/notion-property.js (プロパティ抽出は既存実装を流用、二重定義しない)
 */
import crypto from 'node:crypto';
import { findPageByManageNumber, queryDatabaseAll, notionRequest, getConfig } from '../../rakuten-yahoo-sync/lib/notion-client.js';
import { extractTitle, extractRow } from '../../rakuten-yahoo-sync/lib/notion-property.js';
import { getDB, logEvent, upsertDraftYahoo, SOURCE_NOTION_IMPORT } from '../db.js';

/** 1 リクエストあたりの上限。Notion は 1 コードにつき 1 query なので欲張らない */
export const MAX_IMPORT_CODES = 50;

/** Notion url 型プロパティ (notion-property.js に無いのでここで持つ) */
function extractUrl(prop) {
  if (!prop || prop.type !== 'url') return null;
  const v = typeof prop.url === 'string' ? prop.url.trim() : '';
  return v === '' ? null : v;
}

/**
 * 「バリエーション有無」select → 0/1。
 * 選択肢の表記ゆれ (有/あり/有り) を吸収し、判別できない値は 0 (単品扱い) に倒す。
 * 取り込みは検証用途であり、誤って 1 にすると出品形状の判断を誤らせるため安全側に倒している。
 */
function toHasVariation(v) {
  if (v == null) return 0;
  return /^(あり|有|有り|yes|true|1)$/i.test(String(v).trim()) ? 1 : 0;
}

/** DB の CHECK (0..1e12) と整合させる整数化。router.js の sanitizeMoney と同じ意味論 */
function toMoney(v) {
  if (v == null) return null;
  const n = Number(v);
  if (!Number.isFinite(n)) return null;
  return Math.round(Math.max(0, Math.min(1e12, n)));
}

function toIntOrNull(v) {
  if (v == null) return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.round(n) : null;
}

/** 入力文字列 (改行/カンマ区切り) → 正規化済み商品コード配列 (重複除去・順序保持) */
export function parseNeCodes(input) {
  const raw = Array.isArray(input) ? input : String(input == null ? '' : input).split(/[\r\n,、\t]+/);
  const seen = new Set();
  const out = [];
  for (const r of raw) {
    const code = String(r == null ? '' : r).trim();
    if (code === '' || seen.has(code)) continue;
    seen.add(code);
    out.push(code);
  }
  return out;
}

/**
 * Notion page → 取り込み用の平坦な値に変換する pure function (テスト可能にするため分離)。
 * @returns {object|null} 商品コードが取れない page は null
 */
export function buildImportRecord(page) {
  const row = extractRow(page);
  if (!row) return null;
  const props = page.properties || {};
  return {
    ne_code: row.rakuten_manage_number,
    // Name (title) が空のカードもあり得る。商品名は NOT NULL なので商品コードで代替する
    name: extractTitle(props['Name']) || row.rakuten_manage_number,
    price: toMoney(row.yahoo_price),
    jan_code: row.yahoo_jan,
    has_variation: toHasVariation(row.notion_has_variation),
    official_url: extractUrl(props['メーカーページURL']),
    amazon_url: extractUrl(props['amazon販売ページ']),
    notion_page_id: page.id,
    notion_status: row.notion_status,
    yahoo: {
      yahoo_price: toMoney(row.yahoo_price),
      yahoo_price_sagawa: toMoney(row.yahoo_price_sagawa),
      delivery_label: row.notion_delivery_label,
      tax_rate: row.notion_tax_rate,
      yahoo_category_id: toIntOrNull(row.notion_product_category),
      yahoo_path: row.notion_path,
    },
    // AI 出力スロットへの割り当て (§4 スロット構造)。
    // HTML_商品説明文 は対応スロットが無いので取り込まない → 結果に skipped_fields で明示する
    aiOutputs: {
      yahoo_title: row.yahoo_title,
      desc_catch: row.yahoo_headline,
      desc_features: row.yahoo_caption_text,
    },
    skipped_fields: row.yahoo_caption_html ? ['HTML_商品説明文'] : [],
  };
}

/**
 * 取り込み結果を DB に書く。
 * **照会・判定・書き込みを 1 トランザクションに閉じる** (Codex R1 high-3 / medium-4):
 *   - 既存行の取得を transaction 外でやると、判定後に portal 行へ変わった行を上書きしうる
 *   - UPDATE の WHERE にも source 条件を入れ、changes===1 を確認する (二重ガード)
 *   - 同時実行で INSERT が UNIQUE 衝突したら再照会して updated / conflict_portal に正規化する
 * @returns {{ outcome: 'imported'|'updated'|'conflict_portal'|'conflict_page_id', draftId?: number }}
 */
function persist(db, rec, actor) {
  const run = db.transaction(() => {
    const existing = db.prepare('SELECT id, source, notion_page_id FROM product_drafts WHERE ne_code = ?').get(rec.ne_code);

    if (existing) {
      // ポータル起点の商品を Notion の値で塗り潰さない (どちらが正か曖昧になる事故を防ぐ)
      if (existing.source !== SOURCE_NOTION_IMPORT) {
        return { outcome: 'conflict_portal', draftId: existing.id };
      }
      // 同じ商品コードに別の Notion ページが紐づいた = Notion 側の重複か検索ゆらぎ。
      // 黙って付け替えると別カードの内容で上書きするので、人が見るまで止める (Codex R1 medium-5)
      if (existing.notion_page_id && existing.notion_page_id !== rec.notion_page_id) {
        return { outcome: 'conflict_page_id', draftId: existing.id };
      }
      const info = db.prepare(`
        UPDATE product_drafts SET
          name = ?, price = ?, jan_code = ?, has_variation = ?,
          official_url = ?, amazon_url = ?,
          notion_page_id = ?, notion_card_status = 'created', notion_card_error = NULL,
          source_notion_status = ?,
          imported_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'),
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ? AND source = ?
      `).run(
        rec.name, rec.price, rec.jan_code, rec.has_variation,
        rec.official_url, rec.amazon_url, rec.notion_page_id, rec.notion_status,
        existing.id, SOURCE_NOTION_IMPORT,
      );
      if (info.changes !== 1) return { outcome: 'conflict_portal', draftId: existing.id };
      writeChildren(db, existing.id, rec);
      logEvent(db, existing.id, 'notion_reimported', rec.ne_code, actor);
      return { outcome: 'updated', draftId: existing.id };
    }

    let draftId;
    try {
      const info = db.prepare(`
        INSERT INTO product_drafts (
          ne_code, name, status, price, jan_code, has_variation,
          official_url, amazon_url, notion_page_id, notion_card_status,
          source, source_notion_status, created_by, imported_at
        ) VALUES (?, ?, 'draft', ?, ?, ?, ?, ?, ?, 'created', ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      `).run(
        rec.ne_code, rec.name, rec.price, rec.jan_code, rec.has_variation,
        rec.official_url, rec.amazon_url, rec.notion_page_id,
        SOURCE_NOTION_IMPORT, rec.notion_status, actor || null,
      );
      draftId = Number(info.lastInsertRowid);
    } catch (e) {
      // 同時実行で先に入った行がある → 再照会して結果を正規化 (API を冪等に保つ)
      if (!/UNIQUE/i.test(String(e && e.message))) throw e;
      const raced = db.prepare('SELECT id, source FROM product_drafts WHERE ne_code = ?').get(rec.ne_code);
      if (!raced) throw e;
      return { outcome: raced.source === SOURCE_NOTION_IMPORT ? 'updated' : 'conflict_portal', draftId: raced.id };
    }

    writeChildren(db, draftId, rec);
    logEvent(db, draftId, 'notion_imported', rec.ne_code, actor);
    return { outcome: 'imported', draftId };
  });
  return run();
}

/**
 * 子テーブル (draft_yahoo / draft_ai_outputs) を Notion の現在値に一致させる。
 * Notion が正なので、**空になった項目は NULL で上書きする** (スキップすると古い値が残り、
 * 再取り込みしても Notion に収束しない — Codex R1 medium-6)。
 */
function writeChildren(db, draftId, rec) {
  upsertDraftYahoo(db, draftId, rec.yahoo);
  const upsertAi = db.prepare(`
    INSERT INTO draft_ai_outputs (draft_id, kind, content, generated_at, model_note)
    VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'), 'notion_import')
    ON CONFLICT(draft_id, kind) DO UPDATE SET
      content = excluded.content,
      generated_at = excluded.generated_at,
      model_note = excluded.model_note
  `);
  for (const [kind, content] of Object.entries(rec.aiOutputs)) {
    const v = content == null || String(content).trim() === '' ? null : String(content);
    upsertAi.run(draftId, kind, v);
  }
}

/**
 * 商品コード配列を Notion から取り込む。
 * 1 コードずつ直列に処理する (notion-client 側の rate limiter が pacing する)。
 * 1 件の失敗で全体を止めない — 結果配列で個別に返す。
 */
export async function importFromNotion(neCodes, { actor = null, finder = findPageByManageNumber } = {}) {
  const codes = parseNeCodes(neCodes);
  if (codes.length === 0) return { results: [], summary: emptySummary() };
  if (codes.length > MAX_IMPORT_CODES) {
    throw new Error(`一度に取り込めるのは ${MAX_IMPORT_CODES} 件までです (指定: ${codes.length} 件)`);
  }

  const db = getDB();
  const results = [];
  for (const code of codes) {
    try {
      const page = await finder(code);
      if (!page || !page.id) {
        results.push({ ne_code: code, outcome: 'not_found' });
        continue;
      }
      const rec = buildImportRecord(page);
      if (!rec) {
        results.push({ ne_code: code, outcome: 'failed', error: 'Notion カードに商品コードがありません' });
        continue;
      }
      // 検索したコードと返ってきたカードのコードが違うなら書かない (Codex R1 medium-5)。
      // Notion 側の重複や検索ゆらぎで、指定と別の商品を上書きする事故を防ぐ
      if (rec.ne_code !== code) {
        results.push({
          ne_code: code, outcome: 'failed',
          error: `Notionから別の商品コード (${rec.ne_code}) が返りました。取り込みを中止しました`,
        });
        continue;
      }
      const persisted = persist(db, rec, actor);
      results.push({
        ne_code: rec.ne_code,
        outcome: persisted.outcome,
        draftId: persisted.draftId,
        notion_status: rec.notion_status,
        skipped_fields: rec.skipped_fields,
      });
    } catch (e) {
      results.push({ ne_code: code, outcome: 'failed', error: truncate(e) });
    }
  }
  return { results, summary: summarize(results) };
}

/** ステータス一括移植の対象: Status が ①〜⑥ で始まる選択肢 (2026-08-25 中原さん指示) */
export const MIGRATE_STATUS_RE = /^[①②③④⑤⑥]/;

/**
 * Notion 商品マスターの Status ①〜⑥ の商品のうち、**このアプリにカードが無い商品だけ**を移植する
 * (2026-08-25 中原さん指示)。既にある商品は由来 (portal / notion_import) を問わず触らない =
 * already_exists で報告のみ。dryRun (既定) は書き込みせず対象一覧だけ返す — 実行は明示が必要。
 * Notion の select filter は equals しか無いので、DB スキーマから選択肢名を列挙して OR で問い合わせる。
 */
/** 1回の実行で取り込む上限 (同期HTTPを長引かせない)。残りは deferred で報告し、再実行で続きから入る */
export const MAX_MIGRATE_PER_RUN = 300;

export async function importByNotionStatus({
  actor = null, dryRun = true, expectedSnapshot = null, maxPerRun = MAX_MIGRATE_PER_RUN,
  query = queryDatabaseAll, request = notionRequest, config = getConfig,
} = {}) {
  const cfg = config();
  const schema = await request(`/databases/${cfg.databaseId}`, { method: 'GET', cfg });
  const statusProp = schema?.properties?.Status;
  const options = (statusProp?.select?.options || statusProp?.status?.options || [])
    .map((o) => (o && o.name ? String(o.name) : null)).filter(Boolean);
  const targets = options.filter((n) => MIGRATE_STATUS_RE.test(n));
  if (targets.length === 0) {
    throw new Error('Notion の Status に ①〜⑥ で始まる選択肢が見つかりません');
  }
  const filterType = statusProp?.type === 'status' ? 'status' : 'select';
  const { pages } = await query({
    filter: { or: targets.map((name) => ({ property: 'Status', [filterType]: { equals: name } })) },
    // ①〜⑥全対象の一括照会。既定の50ページ (5,000件) だと超えた時点で機能ごと使えなくなる (Codex R1 medium)
    maxPages: 200,
  });

  // ── 分類 (書き込みなし)。dryRun / 実行で同じ判定を通る ──
  const db = getDB();
  const results = [];
  const toImport = []; // { rec, result } — would_import の実体
  const seen = new Set();
  for (const page of pages) {
    const rec = buildImportRecord(page);
    if (!rec) {
      results.push({
        ne_code: '(商品コードなし)', outcome: 'failed',
        error: `Notion カードに商品コードがありません (page ${page?.id || '?'})`,
      });
      continue;
    }
    const key = rec.ne_code.trim().toLowerCase();
    // Notion 側の重複カード: 最初の 1 枚だけを対象にし、2 枚目以降は取り込まず報告する
    if (seen.has(key)) {
      results.push({
        ne_code: rec.ne_code, outcome: 'duplicate', notion_status: rec.notion_status,
        error: '同じ商品コードのカードが Notion に複数あります',
      });
      continue;
    }
    seen.add(key);
    const existing = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(key);
    if (existing) {
      results.push({ ne_code: rec.ne_code, outcome: 'already_exists', draftId: existing.id, notion_status: rec.notion_status });
      continue;
    }
    const result = { ne_code: rec.ne_code, outcome: 'would_import', notion_status: rec.notion_status };
    results.push(result);
    toImport.push({ rec, result });
  }

  // プレビューと実行を同じ対象集合に固定する (Codex R1 high): 対象の**保存内容全体**
  // (buildImportRecord の結果 = 商品名・売価・JAN・URL・Status・Yahoo欄・AI出力まで) のハッシュを返し、
  // 実行時に一致しなければ書き込まず再プレビューを要求する。コード+ページIDだけだと、
  // プレビュー後に値が書き換わった商品を見ていないまま保存してしまう (Codex R2 high)。
  // rec はオブジェクトリテラル構築でキー順が固定なので JSON.stringify は決定的
  const snapshot = crypto.createHash('sha256')
    .update(toImport.map(({ rec }) => JSON.stringify(rec)).sort().join('\n'))
    .digest('hex').slice(0, 32);

  if (!dryRun) {
    if (expectedSnapshot !== snapshot) {
      const e = new Error('プレビュー後に Notion 側の対象が変わりました。もう一度「対象を確認」からやり直してください');
      e.code = 'snapshot_mismatch';
      throw e;
    }
    let processed = 0;
    for (const t of toImport) {
      if (processed >= maxPerRun) { t.result.outcome = 'deferred'; continue; }
      processed += 1;
      try {
        const persisted = persist(db, t.rec, actor);
        t.result.outcome = persisted.outcome;
        t.result.draftId = persisted.draftId;
        t.result.skipped_fields = t.rec.skipped_fields;
      } catch (e) {
        t.result.outcome = 'failed';
        t.result.error = truncate(e);
      }
    }
  }
  return { statuses: targets, total: pages.length, snapshot, results, summary: summarize(results) };
}

function truncate(e) {
  const msg = e && e.message ? String(e.message) : String(e);
  return msg.length > 500 ? `${msg.slice(0, 500)}…` : msg;
}

function emptySummary() {
  return {
    imported: 0, updated: 0, not_found: 0, conflict_portal: 0, conflict_page_id: 0, failed: 0,
    already_exists: 0, would_import: 0, duplicate: 0, deferred: 0,
  };
}

export function summarize(results) {
  const s = emptySummary();
  for (const r of results) {
    if (s[r.outcome] != null) s[r.outcome] += 1;
  }
  return s;
}
