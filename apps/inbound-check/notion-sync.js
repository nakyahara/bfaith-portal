/**
 * 入荷受付チェック — Notion 作業カードの reconcile (状態収束) sweep
 *
 * f_inbound_check_destinations (行き先台帳) を正として、Notion 側を期待状態へ収束させる:
 *   active + iroha  → 有効なカードが1枚ある
 *   cancelled       → カードなし (未送信のまま取消) または既存カードがステータス「取消」
 *
 * 実行タイミング (中原さん 2026-09-02 決定):
 *   1日1回 夕方 (既定 17:30 JST) の一括送信 + 管理画面「今すぐ送る」ボタン。
 *   都度送信にしないのは、当日中のやり直し (取消→再確認) を送信前に収束させるため。
 *   それでも「確認済み行が翌日に引き継がれた後のやり直し」等で送信後の取消は起こり得るので、
 *   取消の反映 (ステータス→取消) は同じ sweep が保険として行う (Codex設計相談R1)。
 *
 * 設計の要点 (Codex設計相談R1 = Downloads『在庫化カード置き換え_Codex設計相談R1_20260902.md』):
 *   - outbox 方式: 送信状態は台帳の行そのものに持つ (notion_page_id / notion_synced_at / …)。
 *     Render のデプロイ再起動で今日の分を送り損ねても、次の sweep が拾う
 *   - 二重カード防止: カードに destination_id を必ず入れ、作成前に同 ID のカードを検索して回収
 *   - 作成成功と取消反映は別の状態列 (notion_synced_at を取消時に上書きしない)
 *   - 4xx (スキーマ不整合等) は無限リトライしない — ブロックして管理画面に出す。再送は人がボタンで
 *   - 取消反映時に元ステータスを記録し、未着手以外だった行は管理画面の要確認一覧に出す
 *     (「人が動かしていたら触らない」はしない — 取消済みの作業指示が有効に見える方が危険)
 */
import { getDB, workDateJst } from './db.js';
import {
  isNotionConfigured, ensureCardSchema, findCardByDestinationId,
  createCard, getCardState, setCardStatus,
} from './notion.js';
import { recordPing } from '../jobs-monitor/store.js';
import { normSupplierCode } from '../purchase-orders/db.js';

export const JOB_ID = 'inbound-check-notion-cards';
const CANCELLED_STATUS = '取消';
// 4xx で止めた行の目印。人が「再送」するまで自動では触らない
const BLOCKED_UNTIL = '9999-12-31T00:00:00.000Z';
const RETRY_DELAY_MS = 30 * 60 * 1000;   // 一時エラーの再試行間隔 (次の sweep か手動ボタンで拾う)
const CREATE_INTERVAL_MS = 350;          // Notion レートリミット (3req/s) 対策。GAS と同じ
const SWEEP_LIMIT = 300;                 // 1回の sweep で扱う行数の上限 (通常は1日数十行)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const utcNow = () => new Date().toISOString();

function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

// ─── 送信対象の抽出 ───

/** まだカードになっていない いろは行き (取消済み・ブロック中・再試行待ちは除く) */
export function collectUnsent(db, now = utcNow(), limit = SWEEP_LIMIT) {
  return db.prepare(`SELECT * FROM f_inbound_check_destinations
    WHERE destination = 'iroha' AND cancelled_at IS NULL AND notion_page_id IS NULL
      AND (notion_next_retry_at IS NULL OR notion_next_retry_at <= ?)
    ORDER BY id LIMIT ?`).all(now, limit);
}

/** カードは作ったが、その後取り消された行 (Notion 側へ「取消」を反映する対象) */
export function collectCancelPending(db, now = utcNow(), limit = SWEEP_LIMIT) {
  return db.prepare(`SELECT * FROM f_inbound_check_destinations
    WHERE notion_page_id IS NOT NULL AND cancelled_at IS NOT NULL AND notion_cancelled_at IS NULL
      AND (notion_next_retry_at IS NULL OR notion_next_retry_at <= ?)
    ORDER BY id LIMIT ?`).all(now, limit);
}

// ─── カード内容の組み立て ───

/**
 * sweep 1回分の参照データをまとめて引く。
 * ⚠1行ずつ LOWER(TRIM(...)) で照合するとインデックスが効かず全表スキャン×行数になるので、
 *   30日分・対象表ごとに1クエリで Map を作る (mirror_sales_daily は数百万行になり得る)。
 */
export function buildEnrichContext(db) {
  const ctx = { products: new Map(), suppliers: new Map(), sales30: new Map(), freeStock: new Map() };
  if (tableExists(db, 'mirror_products')) {
    for (const r of db.prepare('SELECT 商品コード AS code, 仕入先コード AS sup, 取扱区分 AS handling FROM mirror_products').all()) {
      const k = String(r.code || '').trim().toLowerCase();
      if (k) ctx.products.set(k, { supplierCode: r.sup, handling: r.handling });
    }
  }
  if (tableExists(db, 'po_suppliers')) {
    for (const r of db.prepare('SELECT supplier_code, supplier_name FROM po_suppliers').all()) {
      ctx.suppliers.set(String(r.supplier_code), r.supplier_name);
    }
  }
  if (tableExists(db, 'mirror_sales_daily')) {
    // GAS は販売実績シートの「30日販売数合計」を使っていた。ここは自社ミラーの直近30日合計
    const rows = db.prepare(`SELECT LOWER(TRIM(商品コード)) AS k, SUM(数量) AS q
      FROM mirror_sales_daily
      WHERE データ種別 = 'by_product' AND 日付 >= date('now', '-30 day')
      GROUP BY LOWER(TRIM(商品コード))`).all();
    for (const r of rows) if (r.k) ctx.sales30.set(r.k, Number(r.q) || 0);
  }
  if (tableExists(db, 'mirror_logizard_stock')) {
    // フリー在庫 = 在庫数 − 引当数。GAS の zenzaiko.csv は品質を見ていなかったが、
    // 不良品在庫は外部に預けられる数ではないので良品に絞る (意図的な改善)
    const rows = db.prepare(`SELECT LOWER(TRIM(商品ID)) AS k, SUM(在庫数 - 引当数) AS free
      FROM mirror_logizard_stock WHERE 品質区分名 = '良品'
      GROUP BY LOWER(TRIM(商品ID))`).all();
    for (const r of rows) if (r.k) ctx.freeStock.set(r.k, Number(r.free) || 0);
  }
  return ctx;
}

/** 外部出しOK数 (GAS calcExternalAllowance_ と同じ式: フリー在庫 − ceil(30日販売数/30×14日)) */
export function calcExternal(sales30, freeStock) {
  const r = { sales30: sales30 ?? null, freeStock: freeStock ?? null, externalOk: null };
  if (r.sales30 == null && r.freeStock == null) return r;
  const s = r.sales30 ?? 0;
  if (r.freeStock != null) {
    const keep = Math.ceil((s / 30) * 14);
    r.externalOk = Math.max(r.freeStock - keep, 0);
  }
  return r;
}

/** 行のバーコード (取込行から引く。バッチが保持期間で消えていたら null) */
function barcodeFor(db, row) {
  const r = db.prepare('SELECT barcode FROM f_inbound_check_lines WHERE batch_id = ? AND line_key = ?')
    .get(row.batch_id, row.line_key);
  return r?.barcode || null;
}

/**
 * Notion プロパティを組み立てる。schemaProps (実在するプロパティ名の集合) に無い項目は送らない
 * (プロパティが改名・削除されていても、その項目だけ落ちて送信自体は成功する)。
 */
export function buildCardProperties(row, { barcode, product, supplierName, ext }, schemaProps) {
  const props = {};
  const put = (name, value) => { if (schemaProps.has(name)) props[name] = value; };
  const text = (s) => [{ text: { content: String(s).slice(0, 1900) } }];

  props['名前'] = { title: [{ text: { content: row.product_name || row.product_id || '名称不明' } }] };
  put('ステータス', { select: { name: '未着手' } });
  put('商品コード', { rich_text: text(row.product_id || '') });
  put('数量', { number: Number(row.actual_qty ?? row.planned_qty) || 0 });
  if (row.work_date) put('入庫日', { date: { start: row.work_date } });
  put('入荷管理番号', { rich_text: text(row.ar_no || '') });
  if (barcode) put('バーコード', { rich_text: text(barcode) });
  // 取引先: 入荷受付CSVの取引先は常に 0002/BF で使えない → 商品マスタの仕入先から引く
  if (supplierName) put('取引先', { select: { name: supplierName } });
  const supNum = product ? parseInt(product.supplierCode, 10) : NaN;
  if (!Number.isNaN(supNum)) put('仕入先', { number: supNum });
  if (product?.handling) put('取扱区分', { select: { name: product.handling } });
  // このカードは「いろはで在庫化する」と人が判断した行だけから作られる
  put('在庫化必要FLG', { checkbox: true });
  put('作業拠点', { select: { name: 'いろは' } });
  if (row.expiry_date) put('有効期限', { rich_text: text(row.expiry_date) });
  put('destination_id', { number: row.id });

  // 過去30日販売数 / 外部出し目安 (GAS と同じ文言)
  put('過去30日販売数', { rich_text: text(ext.sales30 != null ? `${ext.sales30}個` : '販売実績なし') });
  if (ext.externalOk != null) {
    put('外部出し目安', { rich_text: text(ext.externalOk === 0 ? '外部施設NG' : `外部施設に${ext.externalOk}個まで預けてOK`) });
  } else if (ext.sales30 == null) {
    put('外部出し目安', { rich_text: text('販売実績なし') });
  }
  // ⚠「入数」は送らない: 旧マスターの入数 (いろは容器あたり数) と f_inbound_info の入数 (仕入箱入数) は
  //   意味が違う疑いがあり、間違った意味の数字を現場に見せない (Codex設計相談R1 質問2-3。PR2 の
  //   f_iroha_work_master 整備後に units_per_container を載せる)
  return props;
}

// ─── 状態の記録 ───

function markSent(db, id, pageId, payload) {
  // notion_page_id IS NULL ガード: sweep 中に別経路で埋まっていたら上書きしない
  return db.prepare(`UPDATE f_inbound_check_destinations
    SET notion_page_id = ?, notion_synced_at = ?, notion_payload = ?,
        notion_error = NULL, notion_next_retry_at = NULL, notion_attempt_count = COALESCE(notion_attempt_count, 0) + 1
    WHERE id = ? AND notion_page_id IS NULL`).run(pageId, utcNow(), payload, id).changes;
}

function markSendError(db, id, e) {
  const permanent = [400, 401, 403, 404].includes(e.status);
  const retryAt = permanent ? BLOCKED_UNTIL : new Date(Date.now() + RETRY_DELAY_MS).toISOString();
  db.prepare(`UPDATE f_inbound_check_destinations
    SET notion_error = ?, notion_next_retry_at = ?, notion_attempt_count = COALESCE(notion_attempt_count, 0) + 1
    WHERE id = ?`).run(String(e.message || e).slice(0, 300), retryAt, id);
  return permanent;
}

function markCancelled(db, id, prevStatus) {
  db.prepare(`UPDATE f_inbound_check_destinations
    SET notion_cancelled_at = ?, notion_cancelled_prev_status = ?, notion_cancel_error = NULL, notion_next_retry_at = NULL
    WHERE id = ?`).run(utcNow(), prevStatus || null, id);
}

function markCancelError(db, id, e) {
  const permanent = [400, 401, 403, 404].includes(e.status);
  const retryAt = permanent ? BLOCKED_UNTIL : new Date(Date.now() + RETRY_DELAY_MS).toISOString();
  db.prepare(`UPDATE f_inbound_check_destinations
    SET notion_cancel_error = ?, notion_next_retry_at = ?
    WHERE id = ?`).run(String(e.message || e).slice(0, 300), retryAt, id);
  return permanent;
}

/** 管理画面の「再送」: ブロック・再試行待ちを解除する (次の sweep / ボタンで拾われる) */
export function resetNotionRow(id) {
  return getDB().prepare(`UPDATE f_inbound_check_destinations
    SET notion_next_retry_at = NULL, notion_error = NULL, notion_cancel_error = NULL
    WHERE id = ?`).run(Number(id)).changes;
}

// ─── sweep 本体 ───

let running = false;

/**
 * reconcile を1回実行する。cron (毎日夕方) と管理画面ボタンの両方から呼ばれる。
 * @returns {{ok:boolean, sent?:number, recovered?:number, cancelled?:number, errors?:number, error?:string}}
 */
export async function runNotionSweep({ actor = 'manual' } = {}) {
  if (!isNotionConfigured()) {
    const r = { ok: false, error: 'not_configured', message: 'Notion 連携が未設定です (NOTION_TOKEN / INBOUND_CHECK_NOTION_DB_ID)' };
    // cron から呼ばれたのに未設定 = 「動いているべきものが動いていない」なので fail を残す
    if (actor === 'cron') ping('fail', 'env未設定 (INBOUND_CHECK_NOTION_DB_ID)');
    return r;
  }
  if (running) return { ok: false, error: 'already_running', message: '送信処理が既に動いています' };
  running = true;
  try {
    const db = getDB();
    const summary = { sent: 0, recovered: 0, cancelled: 0, errors: 0, skipped: 0, blocked: 0 };
    const schemaProps = await ensureCardSchema();
    const ctx = buildEnrichContext(db);

    // ① 未送信の いろは行き → カード作成
    for (const row of collectUnsent(db)) {
      // API を待っている間に取り消されているかもしれない — 直前にもう一度見る
      const fresh = db.prepare('SELECT cancelled_at, notion_page_id FROM f_inbound_check_destinations WHERE id = ?').get(row.id);
      if (!fresh || fresh.cancelled_at || fresh.notion_page_id) { summary.skipped++; continue; }
      try {
        // 回収: 前回「作成成功したのに page_id を保存できなかった」カードが残っていないか
        const existing = await findCardByDestinationId(row.id);
        const key = String(row.code_key || row.product_id || '').trim().toLowerCase();
        const product = ctx.products.get(key) || null;
        const supplierName = product ? (ctx.suppliers.get(normSupplierCode(product.supplierCode)) || null) : null;
        const ext = calcExternal(ctx.sales30.get(key) ?? null, ctx.freeStock.get(key) ?? null);
        const props = buildCardProperties(row, { barcode: barcodeFor(db, row), product, supplierName, ext }, schemaProps);
        if (existing) {
          markSent(db, row.id, existing.id, JSON.stringify({ recovered: true }));
          summary.recovered++;
        } else {
          const { id: pageId } = await createCard(props);
          markSent(db, row.id, pageId, JSON.stringify(props));
          summary.sent++;
          await sleep(CREATE_INTERVAL_MS);
        }
      } catch (e) {
        summary.errors++;
        if (markSendError(db, row.id, e)) summary.blocked++;
        console.warn(`[inbound-check notion] 送信失敗 dest#${row.id} ${row.product_id}: ${e.message}`);
      }
    }

    // ② 送信後に取り消された行 → カードを「取消」へ (①の最中に取り消された分も同じ sweep で拾う)
    for (const row of collectCancelPending(db)) {
      try {
        const state = await getCardState(row.notion_page_id);
        if (state.archived) {
          // 誰かが Notion 側でアーカイブ済み — もう有効な作業指示として見えないので収束とみなす
          markCancelled(db, row.id, state.status ? `${state.status} (アーカイブ済み)` : '(アーカイブ済み)');
        } else if (state.status === CANCELLED_STATUS) {
          markCancelled(db, row.id, CANCELLED_STATUS);
        } else {
          await setCardStatus(row.notion_page_id, CANCELLED_STATUS);
          markCancelled(db, row.id, state.status || '(不明)');
          await sleep(CREATE_INTERVAL_MS);
        }
        summary.cancelled++;
      } catch (e) {
        summary.errors++;
        if (markCancelError(db, row.id, e)) summary.blocked++;
        console.warn(`[inbound-check notion] 取消反映失敗 dest#${row.id}: ${e.message}`);
      }
    }

    const note = `作成${summary.sent} 回収${summary.recovered} 取消反映${summary.cancelled} 失敗${summary.errors}` +
      (summary.blocked ? ` (うち要対応${summary.blocked})` : '');
    console.log(`[inbound-check notion] sweep (${actor}): ${note}`);
    // 失敗が1件でもあれば fail (「正常終了したが仕事が残っている」を ok にしない)
    ping(summary.errors > 0 ? 'fail' : 'ok', note);
    return { ok: summary.errors === 0, ...summary, note };
  } catch (e) {
    console.error('[inbound-check notion] sweep 失敗:', e);
    ping('fail', String(e.message).slice(0, 180));
    return { ok: false, error: 'sweep_failed', message: e.message };
  } finally {
    running = false;
  }
}

function ping(status, note) {
  try { recordPing(JOB_ID, status, note, Date.now()); }
  catch (e) { console.error('[inbound-check notion] ping failed:', e.message); }
}

// ─── 管理画面用の状態 ───

export function notionStatusForAdmin() {
  const db = getDB();
  const now = utcNow();
  const todayStart = `${workDateJst()}T00:00:00`; // JST の今日 (synced_at は UTC なので比較は目安表示用)
  const one = (sql, ...p) => db.prepare(sql).get(...p)?.n ?? 0;
  const unsent = one(`SELECT COUNT(*) n FROM f_inbound_check_destinations
    WHERE destination = 'iroha' AND cancelled_at IS NULL AND notion_page_id IS NULL
      AND (notion_next_retry_at IS NULL OR notion_next_retry_at <= ?)`, now);
  const blocked = db.prepare(`SELECT id, product_id, product_name, notion_error, notion_cancel_error, notion_next_retry_at
    FROM f_inbound_check_destinations
    WHERE notion_next_retry_at IS NOT NULL AND notion_next_retry_at > ?
    ORDER BY id DESC LIMIT 10`).all(now);
  const cancelPending = one(`SELECT COUNT(*) n FROM f_inbound_check_destinations
    WHERE notion_page_id IS NOT NULL AND cancelled_at IS NOT NULL AND notion_cancelled_at IS NULL`);
  const sentRecent = one(`SELECT COUNT(*) n FROM f_inbound_check_destinations
    WHERE notion_synced_at IS NOT NULL AND notion_synced_at >= datetime('now', '-1 day')`);
  const lastSyncedAt = db.prepare('SELECT MAX(notion_synced_at) m FROM f_inbound_check_destinations').get()?.m || null;
  // 要確認: 取消を反映したとき、カードが未着手以外だった (現場が動かしていた) 行。30日分
  const attention = db.prepare(`SELECT id, product_id, product_name, actual_qty, work_date,
      cancel_reason, cancelled_at, notion_cancelled_at, notion_cancelled_prev_status
    FROM f_inbound_check_destinations
    WHERE notion_cancelled_prev_status IS NOT NULL AND notion_cancelled_prev_status <> '未着手'
      AND notion_cancelled_at >= datetime('now', '-30 day')
    ORDER BY notion_cancelled_at DESC LIMIT 20`).all();
  return {
    configured: isNotionConfigured(),
    dbIdTail: (process.env.INBOUND_CHECK_NOTION_DB_ID || '').slice(-6),
    unsent, cancelPending, sentRecent, lastSyncedAt, blocked, attention, todayStart,
  };
}
