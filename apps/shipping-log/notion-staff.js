/**
 * 出荷実績ログ — Notion出荷カード担当者スナップショット (shipping-log P2)
 *
 * Notion の出荷カードDB (バッチ=出荷_XX ごとのカードに梱包/ピッキング担当者が載っている)
 * から担当者を取得し、warehouse-mirror.db の sl_batch_staff へ日次スナップショット保存する。
 * 伝票 (sl_shipping_slips) とは v_sl_slips_staff (ship_date×folder_name) で突合できる。
 *
 * Render 完結の原則 (miniPC は既存モールAPIの取得のみ、集計/保存は Render):
 * GAS の伝票取込 (18:16 JST) と同じ日の夜にカードの最終状態を写す。
 *
 * env (Render):
 *   NOTION_TOKEN                : Notion integration token (giftset-assembly と共用の既存 env)。
 *                                 integration を出荷カードDBに接続しておくこと
 *   SHIPPING_NOTION_DB_ID       : 出荷カードDB ID (default c87194a151f44108a21a5514c932c27b)
 *   SHIPPING_NOTION_PROP_BATCH  : 出荷No を持つプロパティ名 (必須。未設定なら同期せず
 *                                 プロパティ一覧を返す = 初回マッピング用 fail-closed)
 *   SHIPPING_NOTION_PROP_PACKER : 梱包担当者プロパティ名 (必須。people 想定)
 *   SHIPPING_NOTION_PROP_PICKER : ピッキング担当者プロパティ名 (任意)
 *   SHIPPING_NOTION_PROP_DATE   : 作業日プロパティ名 (任意。未設定=カード使い回し運用と
 *                                 みなし JST当日 を割り当てる。日別カードDBなら必須。
 *                                 created_time 型も可 — UTC→JST変換して日付化する)
 *   SHIPPING_NOTION_PROP_STATUS : ステータスプロパティ名 (任意)
 *   SHIPPING_STAFF_LOOKBACK_DAYS: 取得窓 (created_time基準、default 7日。日別カードが
 *                                 蓄積するDBを全件走査しないため)
 *
 * 実DB確認済み (2026-07-19、integration「Claude Code」で読み取り実証):
 *   DB「スタッフ用デイリー業務」= 日別カード (毎日20枚前後、出荷_XXごと1枚)。推奨env:
 *   BATCH=名前 (title、「出荷_18」「出荷_1　パフ済」形式。select「出荷No」は入力率5割で不採用) /
 *   PACKER=梱包担当者 (select) / PICKER=ピッキング担当者 (select) /
 *   DATE=作成日時 (created_time) / STATUS=ステータス (select)
 *   SHIPPING_STAFF_CRON_ENABLED : 'true'|'1' で日次 cron 起動 (Dark Launch、default OFF)
 *   SHIPPING_STAFF_CRON         : cron 式 (default '30 19 * * *' = 毎日19:30 JST。
 *                                 GAS 18:16 の後、カードが翌日用にリセットされる前)
 *   SHIPPING_LOG_GCHAT_WEBHOOK  : cron 失敗時の通知先 (任意)
 *
 * 手動実行/確認は router.js の POST /sync-staff (ingest token) /
 * GET /staff-props / /recent-staff (read token) から。
 */
import cron from 'node-cron';
import { upsertStaffRows } from './db.js';

const NOTION_API = 'https://api.notion.com/v1';
const NOTION_VERSION = '2022-06-28';
const DEFAULT_DB_ID = 'c87194a151f44108a21a5514c932c27b';
const DEFAULT_CRON = '30 19 * * *'; // JST (schedule 時に timezone 指定)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Date を JST (UTC+9) の YYYY-MM-DD に変換 */
function toJstDate(d) {
  return new Date(d.getTime() + 9 * 60 * 60 * 1000).toISOString().slice(0, 10);
}

// ─── Notion API ───

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
    await sleep(500 * 2 ** attempt); // 指数バックオフ (giftset-assembly notion.js と同方針)
    return notionFetch(pathname, options, attempt + 1);
  }
  const text = await res.text();
  if (!res.ok) throw new Error(`Notion ${pathname} HTTP ${res.status}: ${text.slice(0, 200)}`);
  return JSON.parse(text);
}

/**
 * 取得窓の起点: 「JST暦日で lookbackDays 日分」の最古日 00:00 JST を UTC ISO で返す。
 * 現在時刻から引くだけだと境界日が時刻で分断され、作業日単位の置換 (delete→insert) が
 * 境界日の朝分を削って部分集合で上書きしてしまう (Codex high)。
 */
export function lookbackSinceIso(lookbackDays, now = new Date()) {
  const oldestJstDate = toJstDate(new Date(now.getTime() - (lookbackDays - 1) * 24 * 60 * 60 * 1000));
  return new Date(Date.parse(`${oldestJstDate}T00:00:00+09:00`)).toISOString();
}

/**
 * カード取得。直近 lookbackDays 日 (created_time 基準、JST暦日単位) に絞る —
 * スタッフ用デイリー業務DBは日別カードが毎日20枚前後追加され累計3,000枚超
 * (2026-07-19実測) のため、全件走査は毎晩30リクエスト超+増え続ける。
 * 担当者の追記・訂正は数日内なので直近窓で十分
 * (置換も取得できた作業日単位なので過去の履歴は壊れない)。
 */
async function notionQueryAll(dbId, lookbackDays) {
  const since = lookbackSinceIso(lookbackDays);
  const pages = [];
  let cursor;
  do {
    const body = await notionFetch(`/databases/${dbId}/query`, {
      method: 'POST',
      body: JSON.stringify({
        page_size: 100,
        filter: { timestamp: 'created_time', created_time: { on_or_after: since } },
        ...(cursor ? { start_cursor: cursor } : {}),
      }),
    });
    pages.push(...body.results);
    cursor = body.has_more ? body.next_cursor : undefined;
    if (cursor) await sleep(350); // Notion rate limit (3req/s) に余裕を持たせる
  } while (cursor);
  return pages;
}

// ─── プロパティ変換 (pure、テスト対象) ───

/** Notion プロパティ値 → プレーン文字列 */
export function plainValue(prop) {
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
    case 'created_time': return prop.created_time || null;
    case 'last_edited_time': return prop.last_edited_time || null;
    default: return null;
  }
}

/** カードの全プロパティを {名前: 値} に要約 (マッピング調整用に保存) */
export function summarizeProps(properties) {
  const out = {};
  for (const [name, prop] of Object.entries(properties || {})) {
    const v = plainValue(prop);
    if (v != null && v !== '') out[name] = v;
  }
  return out;
}

/**
 * 出荷No表記ゆれ → 「出荷_01」に正規化。受理は 1〜99 で:
 *  - 「出荷」接頭辞つき: 出荷_1 / 出荷01 / 「出荷_1　パフ済」のような後ろメモつき (実DBに存在)
 *  - 数字のみ (完全一致): 1 / 01
 * 「棚卸 1」「返品_02」「緊急在庫」等の無関係カードは受理しない (Codex medium)
 */
export function normalizeBatchNo(raw) {
  if (raw == null) return null;
  const s = String(raw).trim();
  const m = s.match(/^出荷[_＿]?(\d{1,2})(?:[\s　].*)?$/) || s.match(/^(\d{1,2})$/);
  if (!m) return null;
  const n = Number(m[1]);
  if (n < 1) return null;
  return `出荷_${String(n).padStart(2, '0')}`;
}

/** YYYY-MM-DD 形式かつ実在日か (2026-02-30 等を弾く) */
function isRealDate(ymd) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(ymd)) return false;
  const [y, m, d] = ymd.split('-').map(Number);
  const dt = new Date(Date.UTC(y, m - 1, d));
  return dt.getUTCFullYear() === y && dt.getUTCMonth() === m - 1 && dt.getUTCDate() === d;
}

function envConfig() {
  return {
    propBatch: process.env.SHIPPING_NOTION_PROP_BATCH,
    propPacker: process.env.SHIPPING_NOTION_PROP_PACKER,
    propPicker: process.env.SHIPPING_NOTION_PROP_PICKER,
    propDate: process.env.SHIPPING_NOTION_PROP_DATE,
    propStatus: process.env.SHIPPING_NOTION_PROP_STATUS,
  };
}

/**
 * カード群 → sl_batch_staff 行に変換する (pure)。設定不備は書き込まず skipped を返す
 * (Codex R1/R4: 誤ったプロパティ推測・タイプミスのまま走って NULL/誤日付を保存しない)。
 * @returns {{ skipped: string, props?: string[] } | { rows: Array<object>, noBatch: number, noDate: number, dupes: number }}
 */
export function buildStaffRows(pages, cfg, defaultWorkDate) {
  const propNames = new Set();
  for (const p of pages) Object.keys(p.properties || {}).forEach((n) => propNames.add(n));
  // 設定されているプロパティ名は任意項目も含め全部存在検証 (タイプミスは黙ってNULLになるより設定エラーで止める)
  const missing = [cfg.propBatch, cfg.propPacker, cfg.propPicker, cfg.propDate, cfg.propStatus]
    .filter(Boolean).filter((n) => pages.length > 0 && !propNames.has(n));
  if (!cfg.propBatch || !cfg.propPacker || missing.length > 0) {
    return {
      skipped: missing.length > 0
        ? `プロパティ「${missing.join('」「')}」がカードに存在しません (env の名前を確認)`
        : 'SHIPPING_NOTION_PROP_BATCH / SHIPPING_NOTION_PROP_PACKER 未設定 (GET /staff-props で一覧を確認して env を設定してください)',
      props: [...propNames],
    };
  }

  // 同一 (work_date, batch_no) は最終更新が新しいカードを採用 (同時刻は page.id でタイブレーク)
  const byKey = new Map();
  let noBatch = 0;
  let noDate = 0;
  for (const page of pages) {
    const props = page.properties || {};
    const batchNo = normalizeBatchNo(plainValue(props[cfg.propBatch]));
    if (!batchNo) { noBatch++; continue; }
    // DATE設定時: 日付が空・不正なカードはフォールバックさせず除外。
    // DATE未設定時のみ「カード使い回し運用」とみなし JST当日 を割り当てる。
    // created_time/last_edited_time 型 (スタッフ用デイリー業務DBの「作成日時」等) は
    // UTC ISO なので JST に変換してから日付化 (slice だと朝9時前作成が前日にずれる)
    let workDate = defaultWorkDate;
    if (cfg.propDate) {
      const dp = props[cfg.propDate];
      const v = plainValue(dp);
      workDate = (dp?.type === 'created_time' || dp?.type === 'last_edited_time')
        ? (v ? toJstDate(new Date(v)) : '')
        : String(v || '').slice(0, 10);
    }
    if (!isRealDate(workDate)) { noDate++; continue; }
    const key = `${workDate}::${batchNo}`;
    const prev = byKey.get(key);
    const cmp = String(page.last_edited_time || '').localeCompare(String(prev?.page.last_edited_time || ''))
      || String(page.id).localeCompare(String(prev?.page.id || ''));
    if (!prev || cmp > 0) byKey.set(key, { workDate, batchNo, page });
  }
  const dupes = pages.length - noBatch - noDate - byKey.size;
  // 日付プロパティ未設定で重複が出る = 日別カードが蓄積するDB。全カードを当日に潰すと
  // 誤った日付で JOIN されるため明示設定を求めて中断
  if (!cfg.propDate && dupes > 0) {
    return { skipped: `同一バッチのカードが複数あります (${dupes}件)。日別カードDBのようなので SHIPPING_NOTION_PROP_DATE (作業日プロパティ名) を設定してください` };
  }

  const rows = [...byKey.values()].map(({ workDate, batchNo, page }) => {
    const props = page.properties || {};
    return {
      work_date: workDate,
      batch_no: batchNo,
      packer: plainValue(props[cfg.propPacker]),
      picker: cfg.propPicker ? plainValue(props[cfg.propPicker]) : null,
      card_status: cfg.propStatus ? plainValue(props[cfg.propStatus]) : null,
      notion_page_id: page.id,
      props_summary: JSON.stringify(summarizeProps(props)),
    };
  });
  return { rows, noBatch, noDate, dupes };
}

// ─── 同期本体 ───

let syncRunning = false;

/**
 * Notion → sl_batch_staff 同期1回分。cron と POST /sync-staff から呼ばれる。
 * 設定不備は { ok: true, skipped } (エラーではなくセットアップ待ち)。
 */
export async function runStaffSync() {
  if (syncRunning) return { ok: false, error: 'already_running' };
  syncRunning = true;
  try {
    if (!process.env.NOTION_TOKEN) {
      return { ok: true, skipped: 'NOTION_TOKEN 未設定' };
    }
    const dbId = process.env.SHIPPING_NOTION_DB_ID || DEFAULT_DB_ID;
    const lookbackDays = Math.min(Math.max(parseInt(process.env.SHIPPING_STAFF_LOOKBACK_DAYS, 10) || 7, 1), 60);
    const pages = await notionQueryAll(dbId, lookbackDays);
    const built = buildStaffRows(pages, envConfig(), toJstDate(new Date()));
    if (built.skipped) {
      console.warn(`[shipping-staff] スキップ: ${built.skipped}`);
      return { ok: true, skipped: built.skipped, total: pages.length, props: built.props };
    }
    // 日別カード運用 (DATE明示) はカードが正 → NULLも反映。使い回しカード運用のみ
    // COALESCE保護 (リセットで実績が消えないように)
    const saved = upsertStaffRows(built.rows, { preserveNonNull: !envConfig().propDate });
    const msg = `カード${pages.length}件→担当者${saved}件保存` +
      (built.noBatch ? ` / 出荷No不明${built.noBatch}件` : '') +
      (built.noDate ? ` / 作業日不明${built.noDate}件` : '') +
      (built.dupes ? ` / 重複${built.dupes}件は最新更新を採用` : '');
    console.log(`[shipping-staff] ${msg}`);
    return { ok: true, saved, total: pages.length, noBatch: built.noBatch, noDate: built.noDate, dupes: built.dupes };
  } finally {
    syncRunning = false;
  }
}

/** 初回マッピング用: DBのプロパティ構成とサンプルカードを返す (書き込みなし) */
export async function fetchStaffProps() {
  if (!process.env.NOTION_TOKEN) {
    return { ok: false, error: 'NOTION_TOKEN 未設定' };
  }
  const dbId = process.env.SHIPPING_NOTION_DB_ID || DEFAULT_DB_ID;
  const meta = await notionFetch(`/databases/${dbId}`, { method: 'GET' });
  const properties = Object.entries(meta.properties || {}).map(([name, p]) => ({ name, type: p.type }));
  const q = await notionFetch(`/databases/${dbId}/query`, {
    method: 'POST',
    body: JSON.stringify({ page_size: 5 }),
  });
  return {
    ok: true,
    title: meta.title?.map((t) => t.plain_text).join('') || '(無題)',
    properties,
    samples: (q.results || []).map((p) => summarizeProps(p.properties)),
  };
}

async function notifyFailure(text) {
  const webhook = process.env.SHIPPING_LOG_GCHAT_WEBHOOK;
  if (!webhook) return;
  try {
    await fetch(webhook, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ text }),
      signal: AbortSignal.timeout(10000),
    });
  } catch (e) {
    console.error('[shipping-staff] GChat通知失敗:', e.message);
  }
}

/** SHIPPING_STAFF_CRON_ENABLED='true'|'1' のときだけ schedule する (Dark Launch、inquiry-hub と同型) */
export function startShippingStaffCron() {
  const enabled = process.env.SHIPPING_STAFF_CRON_ENABLED;
  if (enabled !== 'true' && enabled !== '1') {
    console.log('[shipping-staff] SHIPPING_STAFF_CRON_ENABLED が未設定/false のためスケジュールしない (Dark Launch)');
    return null;
  }
  const expr = process.env.SHIPPING_STAFF_CRON || DEFAULT_CRON;
  if (!cron.validate(expr)) {
    console.error(`[shipping-staff] SHIPPING_STAFF_CRON が不正: ${expr}`);
    return null;
  }
  const task = cron.schedule(expr, () => {
    runStaffSync().catch(async (e) => {
      console.error('[shipping-staff] cron 失敗:', e.message);
      await notifyFailure(`⚠️ *出荷カード担当者同期 失敗*\n${e.message}`);
    });
  }, { timezone: 'Asia/Tokyo' });
  console.log(`[shipping-staff] スケジュール開始: '${expr}' (Asia/Tokyo)`);
  return task;
}
