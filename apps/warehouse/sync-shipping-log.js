/**
 * sync-shipping-log.js — 出荷実績ログの吸い上げ + Notion出荷カード担当者スナップショット
 *
 * shipping-log P2 (Notion突合)。毎朝 daily-sync から実行し、2フェーズで動く:
 *
 *  Phase 1: Render (bfaith-portal) の GET /apps/shipping-log/api/export から
 *           伝票行 (sl_shipping_slips) とピッキング合計 (sl_picking_batches) を
 *           直近 N 日分取得し、warehouse.db の shipping_slips / shipping_picking_batches へ
 *           INSERT OR REPLACE (源泉が append-only なので replace は同値上書き)。
 *
 *  Phase 2: Notion 出荷カードDBの現在状態を取得し、バッチ (出荷_XX) ごとの
 *           ピッキング/梱包担当者を shipping_batch_staff に日次スナップショット保存。
 *           work_date はカードの日付プロパティ (設定時) か「JST昨日」(07:00実行時点で
 *           前日作業の最終状態が残っている前提)。
 *
 *  突合は VIEW v_shipping_slips_staff (ship_date=work_date AND folder_name=batch_no) で行う。
 *
 * env:
 *   RENDER_MIRROR_URL          : 既存 (default https://bfaith-portal.onrender.com/apps/mirror)。
 *                                /apps/mirror を剥がして base URL にする
 *   MIRROR_READ_TOKEN          : Render read-only token (Phase 1 必須。無ければ exit 1)
 *   NOTION_TOKEN               : Notion integration token (未設定なら Phase 2 はスキップ=成功扱い)
 *   SHIPPING_NOTION_DB_ID      : 出荷カードDB ID (default c87194a151f44108a21a5514c932c27b)
 *   SHIPPING_NOTION_PROP_BATCH : 出荷No を持つプロパティ名 (未設定なら Phase 2 は
 *                                プロパティ一覧を表示してスキップ — 初回マッピング用)
 *   SHIPPING_NOTION_PROP_PACKER: 梱包担当者プロパティ名 (people 想定)
 *   SHIPPING_NOTION_PROP_PICKER: ピッキング担当者プロパティ名 (任意)
 *   SHIPPING_NOTION_PROP_DATE  : 作業日プロパティ名 (任意。無ければ JST昨日)
 *   SHIPPING_NOTION_PROP_STATUS: ステータスプロパティ名 (任意)
 *
 * 使い方:
 *   node apps/warehouse/sync-shipping-log.js [days]   … 通常実行 (default 7日分)
 *   node apps/warehouse/sync-shipping-log.js --inspect … Notion DBのプロパティ構成と
 *                                                       サンプルカードを表示 (書き込みなし)
 */
import 'dotenv/config';
import { initDB, getDB } from './db.js';

const RENDER_BASE = (process.env.RENDER_MIRROR_URL || 'https://bfaith-portal.onrender.com/apps/mirror')
  .replace(/\/apps\/mirror\/?$/, '');
const NOTION_API = 'https://api.notion.com/v1';
const NOTION_VERSION = '2022-06-28';
const NOTION_DB_ID = process.env.SHIPPING_NOTION_DB_ID || 'c87194a151f44108a21a5514c932c27b';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Date を JST (UTC+9) の YYYY-MM-DD に変換 */
function toJstDate(d) {
  const jst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return jst.toISOString().slice(0, 10);
}

function ensureTables(db) {
  db.exec(`
    CREATE TABLE IF NOT EXISTS shipping_slips (
      slip_no TEXT PRIMARY KEY,         -- 出荷伝票NO (ロジザード SPxxx)
      ship_date TEXT NOT NULL,          -- 出荷日 (JST)
      folder_name TEXT NOT NULL,        -- 出荷_XX (バッチ)
      mgmt_no TEXT,                     -- 管理番号 (NE伝票番号)
      mall_order_no TEXT,               -- モール注文番号
      source_file TEXT,                 -- 抽出元 納品書PDF
      synced_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_shipping_slips_date ON shipping_slips(ship_date);
    CREATE INDEX IF NOT EXISTS idx_shipping_slips_mgmt ON shipping_slips(mgmt_no);

    CREATE TABLE IF NOT EXISTS shipping_picking_batches (
      ship_date TEXT NOT NULL,
      folder_name TEXT NOT NULL,
      source_file TEXT NOT NULL,        -- ピッキングリストPDF
      total_qty INTEGER NOT NULL,       -- ピッキング総合計 (GAS検算+サーバ再検算済み)
      pages INTEGER NOT NULL,
      page_totals TEXT NOT NULL,        -- JSON配列
      work_date_on_list TEXT,
      printed_at TEXT,
      synced_at TEXT NOT NULL,
      PRIMARY KEY (ship_date, folder_name, source_file)
    );

    CREATE TABLE IF NOT EXISTS shipping_batch_staff (
      work_date TEXT NOT NULL,          -- 作業日 (JST)
      batch_no TEXT NOT NULL,           -- 出荷_XX に正規化したバッチ名
      packer TEXT,                      -- 梱包担当者 (複数は「、」区切り)
      picker TEXT,                      -- ピッキング担当者 (同上)
      card_status TEXT,                 -- カードのステータス (取れれば)
      notion_page_id TEXT,
      props_summary TEXT,               -- プロパティ要約JSON (マッピング調整・デバッグ用)
      synced_at TEXT NOT NULL,
      PRIMARY KEY (work_date, batch_no)
    );

    CREATE VIEW IF NOT EXISTS v_shipping_slips_staff AS
    SELECT s.slip_no, s.ship_date, s.folder_name, s.mgmt_no, s.mall_order_no,
           st.packer, st.picker, st.card_status, st.notion_page_id
    FROM shipping_slips s
    LEFT JOIN shipping_batch_staff st
      ON st.work_date = s.ship_date AND st.batch_no = s.folder_name;
  `);
}

// ─── Phase 1: Render export 吸い上げ ───

async function fetchExport(from, to) {
  const token = process.env.MIRROR_READ_TOKEN;
  if (!token) throw new Error('MIRROR_READ_TOKEN が未設定です (miniPC .env に追加してください)');
  const url = `${RENDER_BASE}/apps/shipping-log/api/export?from=${from}&to=${to}`;
  const res = await fetch(url, {
    headers: { 'x-read-token': token },
    signal: AbortSignal.timeout(60000),
  });
  const text = await res.text();
  if (!res.ok) throw new Error(`export HTTP ${res.status}: ${text.slice(0, 200)}`);
  const body = JSON.parse(text);
  if (body.ok !== true) throw new Error(`export ok!==true: ${text.slice(0, 200)}`);
  return body;
}

function upsertExport(db, body) {
  const now = new Date().toISOString();
  const slipStmt = db.prepare(`
    INSERT OR REPLACE INTO shipping_slips
      (slip_no, ship_date, folder_name, mgmt_no, mall_order_no, source_file, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
  `);
  const pickStmt = db.prepare(`
    INSERT OR REPLACE INTO shipping_picking_batches
      (ship_date, folder_name, source_file, total_qty, pages, page_totals, work_date_on_list, printed_at, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);
  return db.transaction(() => {
    for (const s of body.slips) {
      slipStmt.run(s.slip_no, s.ship_date, s.folder_name, s.mgmt_no, s.mall_order_no, s.source_file, now);
    }
    for (const p of body.picking) {
      pickStmt.run(p.ship_date, p.folder_name, p.source_file, p.total_qty, p.pages,
        p.page_totals, p.work_date_on_list, p.printed_at, now);
    }
    return { slips: body.slips.length, picking: body.picking.length };
  })();
}

// ─── Phase 2: Notion 出荷カード ───

async function notionFetch(pathname, options = {}, attempt = 1) {
  const res = await fetch(`${NOTION_API}${pathname}`, {
    ...options,
    headers: {
      'Authorization': `Bearer ${process.env.NOTION_TOKEN}`,
      'Notion-Version': NOTION_VERSION,
      'Content-Type': 'application/json',
      ...(options.headers || {}),
    },
    signal: AbortSignal.timeout(30000),
  });
  if ((res.status === 429 || res.status >= 500) && attempt < 3) {
    await sleep(500 * 2 ** attempt);
    return notionFetch(pathname, options, attempt + 1);
  }
  const text = await res.text();
  if (!res.ok) throw new Error(`Notion ${pathname} HTTP ${res.status}: ${text.slice(0, 200)}`);
  return JSON.parse(text);
}

async function notionQueryAll(dbId) {
  const pages = [];
  let cursor = undefined;
  do {
    const body = await notionFetch(`/databases/${dbId}/query`, {
      method: 'POST',
      body: JSON.stringify({ page_size: 100, ...(cursor ? { start_cursor: cursor } : {}) }),
    });
    pages.push(...body.results);
    cursor = body.has_more ? body.next_cursor : undefined;
    if (cursor) await sleep(350); // Notion rate limit (3req/s) に余裕を持たせる
  } while (cursor);
  return pages;
}

/** Notion プロパティ値 → プレーン文字列 (突合・要約の共通変換) */
function plainValue(prop) {
  if (!prop) return null;
  switch (prop.type) {
    case 'title': return prop.title.map((t) => t.plain_text).join('') || null;
    case 'rich_text': return prop.rich_text.map((t) => t.plain_text).join('') || null;
    case 'number': return prop.number != null ? String(prop.number) : null;
    case 'select': return prop.select ? prop.select.name : null;
    case 'status': return prop.status ? prop.status.name : null;
    case 'multi_select': return prop.multi_select.map((s) => s.name).join('、') || null;
    case 'people': return prop.people.map((p) => p.name || p.id).join('、') || null;
    case 'date': return prop.date ? prop.date.start : null;
    case 'checkbox': return String(prop.checkbox);
    default: return null;
  }
}

/** カードの全プロパティを {名前: 値} に要約 (マッピング調整用に保存) */
function summarizeProps(properties) {
  const out = {};
  for (const [name, prop] of Object.entries(properties)) {
    const v = plainValue(prop);
    if (v != null && v !== '') out[name] = v;
  }
  return out;
}

/** YYYY-MM-DD 形式かつ実在日か (2026-02-30 等を弾く) */
function isRealDate(ymd) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(ymd)) return false;
  const [y, m, d] = ymd.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

/** 出荷No表記ゆれ (「1」「01」「出荷_01」「出荷01」) → 「出荷_01」に正規化 */
function normalizeBatchNo(raw) {
  if (raw == null) return null;
  const m = String(raw).match(/(\d{1,2})\s*$/);
  if (!m) return null;
  return `出荷_${m[1].padStart(2, '0')}`;
}

async function syncNotionStaff(db) {
  if (!process.env.NOTION_TOKEN) {
    return { skipped: 'NOTION_TOKEN 未設定 (中原さんの env 設定待ち)' };
  }
  const propBatch = process.env.SHIPPING_NOTION_PROP_BATCH;
  const propPacker = process.env.SHIPPING_NOTION_PROP_PACKER;
  const pages = await notionQueryAll(NOTION_DB_ID);

  // 取得対象 (BATCH/PACKER) の env が未設定、または指定名がカードに実在しないなら
  // プロパティ一覧を出して Phase 2 全体をスキップ (Codex R1 high: 設定漏れのまま走ると
  // 既存スナップショットを NULL で置換しかねない。誤推測より明示設定を待つ)
  const propDate = process.env.SHIPPING_NOTION_PROP_DATE;
  const propPicker = process.env.SHIPPING_NOTION_PROP_PICKER;
  const propStatus = process.env.SHIPPING_NOTION_PROP_STATUS;
  const propNames = new Set();
  for (const p of pages) Object.keys(p.properties || {}).forEach((n) => propNames.add(n));
  // 設定されているプロパティ名は任意項目も含め全部存在検証 (Codex R4/R5: タイプミスは
  // 黙ってNULL同期になるより設定エラーとして止める)
  const missing = [propBatch, propPacker, propDate, propPicker, propStatus]
    .filter(Boolean).filter((n) => pages.length > 0 && !propNames.has(n));
  if (!propBatch || !propPacker || missing.length > 0) {
    console.log(`[Notion] カード${pages.length}件。プロパティ: ${[...propNames].join(' / ')}`);
    if (pages[0]) console.log(`[Notion] サンプル: ${JSON.stringify(summarizeProps(pages[0].properties))}`);
    return {
      skipped: missing.length > 0
        ? `プロパティ「${missing.join('」「')}」がカードに存在しません (env の名前を確認)`
        : 'SHIPPING_NOTION_PROP_BATCH / SHIPPING_NOTION_PROP_PACKER 未設定 (↑の一覧から env を設定してください)',
    };
  }
  const defaultWorkDate = toJstDate(new Date(Date.now() - 24 * 60 * 60 * 1000)); // JST昨日

  // 同一 (work_date, batch_no) のカードが複数ある場合は最終更新が新しい方を採用
  // (Codex R1 medium: API返却順で結果が変わる非決定性を排除)
  const byKey = new Map();
  let noBatch = 0;
  let noDate = 0;
  for (const page of pages) {
    const props = page.properties || {};
    const batchNo = normalizeBatchNo(plainValue(props[propBatch]));
    if (!batchNo) { noBatch++; continue; }
    // DATE設定時: 日付が空・不正なカードは昨日へフォールバックせず除外 (Codex R4 medium)。
    // DATE未設定時のみ「カード使い回し運用」とみなし JST昨日 を割り当てる
    const workDate = propDate
      ? String(plainValue(props[propDate]) || '').slice(0, 10)
      : defaultWorkDate;
    if (!isRealDate(workDate)) { noDate++; continue; } // 実在日検証 (Codex R5 medium)
    const key = `${workDate}::${batchNo}`;
    // 第一キー=最終更新時刻、同時刻は page.id で決定的にタイブレーク (Codex R2 low)
    const prev = byKey.get(key);
    const cmp = String(page.last_edited_time || '').localeCompare(String(prev?.page.last_edited_time || ''))
      || String(page.id).localeCompare(String(prev?.page.id || ''));
    if (!prev || cmp > 0) byKey.set(key, { workDate, batchNo, page });
  }
  const dupes = pages.length - noBatch - noDate - byKey.size;
  if (dupes > 0) console.warn(`[Notion] 同一(作業日,バッチ)のカード重複 ${dupes}件 → 最終更新が新しい方を採用`);
  // 日付プロパティ未設定は「バッチカードを毎日使い回す運用」を想定して JST昨日 に割り当てる。
  // その運用ならバッチごとにカードは1枚のはず。重複が出る = 日別カードが蓄積するDBであり、
  // 全カードを昨日に潰すと誤った日付で JOIN される (Codex R3 medium) → 明示設定を求めて中断
  if (!propDate && dupes > 0) {
    return { skipped: `同一バッチのカードが複数あります (${dupes}件)。日別カードDBのようなので SHIPPING_NOTION_PROP_DATE (作業日プロパティ名) を設定してください` };
  }

  const now = new Date().toISOString();
  // 担当者はいったん取れた値を優先保持 (COALESCE)。同日の再実行でカードが
  // 翌日用にリセットされていても、取得済みの担当者を NULL で潰さない
  const stmt = db.prepare(`
    INSERT INTO shipping_batch_staff
      (work_date, batch_no, packer, picker, card_status, notion_page_id, props_summary, synced_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(work_date, batch_no) DO UPDATE SET
      packer = COALESCE(excluded.packer, packer),
      picker = COALESCE(excluded.picker, picker),
      card_status = COALESCE(excluded.card_status, card_status),
      notion_page_id = excluded.notion_page_id,
      props_summary = excluded.props_summary,
      synced_at = excluded.synced_at
  `);
  let saved = 0;
  db.transaction(() => {
    for (const { workDate, batchNo, page } of byKey.values()) {
      const props = page.properties || {};
      stmt.run(
        workDate, batchNo,
        plainValue(props[propPacker]),
        propPicker ? plainValue(props[propPicker]) : null,
        propStatus ? plainValue(props[propStatus]) : null,
        page.id, JSON.stringify(summarizeProps(props)), now
      );
      saved++;
    }
  })();
  return { saved, noBatch, noDate, total: pages.length };
}

async function inspectNotion() {
  if (!process.env.NOTION_TOKEN) {
    console.log('NOTION_TOKEN 未設定のため inspect できません');
    return;
  }
  const meta = await notionFetch(`/databases/${NOTION_DB_ID}`, { method: 'GET' });
  console.log(`DB: ${meta.title?.map((t) => t.plain_text).join('') || '(無題)'}`);
  for (const [name, prop] of Object.entries(meta.properties || {})) {
    console.log(`  - ${name} (${prop.type})`);
  }
  const pages = await notionQueryAll(NOTION_DB_ID);
  console.log(`カード数: ${pages.length}`);
  for (const p of pages.slice(0, 5)) {
    console.log(`  ${p.id}: ${JSON.stringify(summarizeProps(p.properties))}`);
  }
}

// ─── main ───

const WATERMARK_KEY = 'shipping_log_last_synced_to';

/** YYYY-MM-DD + n日 (日付のみのUTC演算なのでTZ安全) */
function addDays(ymd, n) {
  return new Date(Date.parse(`${ymd}T00:00:00Z`) + n * 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

async function main() {
  if (process.argv.includes('--inspect')) {
    await inspectNotion();
    return;
  }
  const days = Math.min(Math.max(parseInt(process.argv[2], 10) || 7, 1), 31);
  const to = toJstDate(new Date());
  let from = toJstDate(new Date(Date.now() - (days - 1) * 24 * 60 * 60 * 1000));

  await initDB();
  const db = getDB();
  ensureTables(db);

  // 水位管理 (Codex R1 medium: 固定7日窓だと8日以上の停止で恒久欠落)。
  // 前回成功時の to が窓より古ければそこまで窓を拡張する。水位日当日も再取得
  // (隔離リトライで過去 ship_date の行が後から増えるため。取込は冪等なので安全)
  const wm = db.prepare(`SELECT value FROM sync_meta WHERE key = ?`).get(WATERMARK_KEY)?.value;
  if (wm && /^\d{4}-\d{2}-\d{2}$/.test(wm) && wm < from) from = wm;

  // Phase 1 (必須): 失敗は exit 1 → daily-sync ❌ + retry。/export の31日制限に合わせ分割
  const p1 = { slips: 0, picking: 0 };
  for (let cursor = from; cursor <= to; cursor = addDays(cursor, 31)) {
    const chunkEnd = addDays(cursor, 30) < to ? addDays(cursor, 30) : to;
    const r = upsertExport(db, await fetchExport(cursor, chunkEnd));
    p1.slips += r.slips;
    p1.picking += r.picking;
  }
  db.prepare(`INSERT OR REPLACE INTO sync_meta (key, value, updated_at) VALUES (?, ?, ?)`)
    .run(WATERMARK_KEY, to, new Date().toISOString());

  // Phase 2: Notion 未設定はスキップ (成功扱い)、API エラーは exit 1 (retry で復旧)
  const p2 = await syncNotionStaff(db);
  const p2notes = [p2.noBatch ? `出荷No不明${p2.noBatch}件` : '', p2.noDate ? `作業日不明${p2.noDate}件` : '']
    .filter(Boolean).join('、');
  const p2msg = p2.skipped
    ? `Notion: ⏭️ ${p2.skipped}`
    : `Notion: カード${p2.total}件→担当者${p2.saved}件保存` + (p2notes ? ` (${p2notes})` : '');

  // 最終行が daily-sync の summary になる
  console.log(`伝票${p1.slips}件 / ピッキング${p1.picking}件 (${from}〜${to}) / ${p2msg}`);
}

main().catch((e) => {
  console.error('[sync-shipping-log] FATAL:', e.message);
  process.exit(1);
});
