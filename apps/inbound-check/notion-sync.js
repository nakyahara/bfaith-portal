/**
 * 入荷受付チェック — Notion 作業カードの reconcile (状態収束) sweep
 *
 * f_inbound_check_destinations (行き先台帳) を正として、Notion 側を期待状態へ収束させる:
 *   active + iroha  → 有効なカードが1枚ある
 *   cancelled       → カードなし (未送信のまま取消) または既存カードがステータス「取消」
 *
 * 実行タイミング (中原さん 2026-09-02 決定):
 *   - 新規カードの送信 = 1日1回 夕方 (既定 17:30 JST) の一括 + 管理画面「今すぐ送る」。
 *     都度送信にしないのは、当日中のやり直し (確認→取消→再確認) を送信前に収束させるため
 *   - 取消の反映・エラー行の再試行だけは 30 分巡回 (既存 Drive cron に相乗り・mode='retry')。
 *     「取消済みの作業指示が有効に見え続ける」時間を短くする (Codex R1 #5 #6)
 *
 * 設計の要点 (Codex設計相談R1 + コードレビューR1 = Downloads『在庫化カード置き換え_*_20260902.md』):
 *   - outbox: 送信状態は台帳の行に持つ。Render 再起動で送り損ねても次の sweep が拾う
 *   - 二重カード防止: 送信前に notion_dedupe_key (行ごとの永続ランダムキー) を**先に DB へ保存**し、
 *     カードにも「台帳キー」として入れる。作成前に同キーで検索して回収する。
 *     行ID (AUTOINCREMENT) はDB作り直しで振り直されるので回収キーにしない (R1 #8)
 *   - markSent は「未取消・未送信」の条件付き UPDATE。0件なら競合として扱い、
 *     作った/回収したカードを取り漏らさない (R1 #3 #4)
 *   - 「作成成功→記録前に停止→取消」で孤立したカードも、取消済み行の台帳キー検索で回収して
 *     「取消」へ収束させる (R1 #1)
 *   - 多重実行は プロセス内フラグ + SQLite lease の二段 (R1 #2)。lease は**行を処理するたびに延長**し、
 *     失っていたら即中断する — 長時間 sweep 中に期限が切れて別実行が侵入するのを防ぐ (R2 #2)
 *   - カード作成 POST は HTTP 層で自動再試行しない。曖昧な失敗 (タイムアウト/5xx/429) は
 *     台帳キーで**再検索してから**作り直す — 「実は成功していた」の二重作成防止 (R2 #1)
 *   - 4xx (429/409 以外) は自動再試行しない — ブロックして管理画面の「再送」で解除 (R1 #7)。
 *     一時エラーは 30 分後から再試行対象 (30分巡回が拾う)
 *   - 取消反映時に元ステータスを記録し、未着手以外だった行は管理画面の要確認一覧に出す
 */
import crypto from 'crypto';
import { getDB } from './db.js';
import {
  isNotionConfigured, ensureCardSchema, findCardsByDedupeKey,
  createCard, getCardState, setCardStatus, DEDUPE_PROP,
} from './notion.js';
import { recordPing } from '../jobs-monitor/store.js';
import { normSupplierCode } from '../purchase-orders/db.js';

export const JOB_ID = 'inbound-check-notion-cards';
const CANCELLED_STATUS = '取消';
// 4xx で止めた行の目印。人が「再送」するまで自動では触らない
const BLOCKED_UNTIL = '9999-12-31T00:00:00.000Z';
const RETRY_DELAY_MS = 30 * 60 * 1000;   // 一時エラーの再試行間隔 (30分巡回 mode='retry' が拾う)
const CREATE_INTERVAL_MS = 350;          // Notion レートリミット (3req/s) 対策。GAS と同じ
const SWEEP_LIMIT = 300;                 // 1回の sweep で扱う行数の上限 (通常は1日数十行)
const LEASE_MS = 10 * 60 * 1000;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const utcNow = () => new Date().toISOString();

// 再試行しても直らない 4xx (429=rate limit と 409=conflict は一時扱い) — Codex R1 #7
const isPermanentStatus = (s) => Number.isInteger(s) && s >= 400 && s < 500 && s !== 429 && s !== 409;

function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

// ─── 対象行の抽出 ───

/** まだカードになっていない いろは行き。retryOnly=true なら一度失敗した行だけ (30分巡回用) */
export function collectUnsent(db, { now = utcNow(), limit = SWEEP_LIMIT, retryOnly = false } = {}) {
  return db.prepare(`SELECT * FROM f_inbound_check_destinations
    WHERE destination = 'iroha' AND cancelled_at IS NULL AND notion_page_id IS NULL
      AND (notion_next_retry_at IS NULL OR notion_next_retry_at <= ?)
      ${retryOnly ? 'AND COALESCE(notion_attempt_count, 0) > 0' : ''}
    ORDER BY id LIMIT ?`).all(now, limit);
}

/** カードは作ったが、その後取り消された行 (Notion 側へ「取消」を反映する対象) */
export function collectCancelPending(db, now = utcNow(), limit = SWEEP_LIMIT) {
  return db.prepare(`SELECT * FROM f_inbound_check_destinations
    WHERE notion_page_id IS NOT NULL AND cancelled_at IS NOT NULL AND notion_cancelled_at IS NULL
      AND (notion_next_retry_at IS NULL OR notion_next_retry_at <= ?)
    ORDER BY id LIMIT ?`).all(now, limit);
}

/**
 * 取消済みなのに page_id が無い行 (Codex R1 #1)。
 * 「作成は成功したが記録前に落ち、その後取り消された」カードが Notion に孤立している可能性がある。
 * 台帳キーで1回だけ検索し、見つかれば「取消」へ収束、無ければ「カード未作成」で終端にする。
 * 台帳キーが無い行はカードを作りようがなかった行なので、API を叩かず終端。
 */
export function collectOrphanCancelled(db, now = utcNow(), limit = SWEEP_LIMIT) {
  return db.prepare(`SELECT * FROM f_inbound_check_destinations
    WHERE cancelled_at IS NOT NULL AND notion_page_id IS NULL AND notion_cancelled_at IS NULL
      AND (notion_next_retry_at IS NULL OR notion_next_retry_at <= ?)
    ORDER BY id LIMIT ?`).all(now, limit);
}

/** 行の永続ランダムキー。**カード作成の前に** DB へ保存する (作成後のクラッシュでも回収できる) */
export function ensureDedupeKey(db, row) {
  if (row.notion_dedupe_key) return row.notion_dedupe_key;
  const key = `d${row.id}-${crypto.randomBytes(6).toString('hex')}`;
  db.prepare('UPDATE f_inbound_check_destinations SET notion_dedupe_key = ? WHERE id = ? AND notion_dedupe_key IS NULL')
    .run(key, row.id);
  // 同時実行で別プロセスが先に付けた場合はそちらが正
  return db.prepare('SELECT notion_dedupe_key FROM f_inbound_check_destinations WHERE id = ?').get(row.id)?.notion_dedupe_key || key;
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
 * Notion プロパティを組み立てる。names (実在するプロパティ名の集合) に無い項目は送らない
 * (プロパティが改名・削除されていても、その項目だけ落ちて送信自体は成功する)。
 */
export function buildCardProperties(row, { barcode, product, supplierName, ext, dedupeKey }, names) {
  const props = {};
  const put = (name, value) => { if (names.has(name)) props[name] = value; };
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
  put(DEDUPE_PROP, { rich_text: text(dedupeKey) });

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

/**
 * 送信成功の記録。**未取消・未送信のときだけ** 書く (Codex R1 #3)。
 * 戻り値 changes=0 は「待っている間に取り消された/別実行が先に記録した」— 呼び元が収束させる。
 */
function markSent(db, id, pageId, payload) {
  return db.prepare(`UPDATE f_inbound_check_destinations
    SET notion_page_id = ?, notion_synced_at = ?, notion_payload = ?,
        notion_error = NULL, notion_next_retry_at = NULL, notion_attempt_count = COALESCE(notion_attempt_count, 0) + 1
    WHERE id = ? AND destination = 'iroha' AND cancelled_at IS NULL AND notion_page_id IS NULL`)
    .run(pageId, utcNow(), payload, id).changes;
}

/** 取消済み行などへ page_id だけ紐付ける (取消フェーズが「取消」へ倒すための足がかり) */
function attachPage(db, id, pageId, payload) {
  return db.prepare(`UPDATE f_inbound_check_destinations
    SET notion_page_id = ?, notion_synced_at = COALESCE(notion_synced_at, ?), notion_payload = COALESCE(notion_payload, ?)
    WHERE id = ? AND notion_page_id IS NULL`).run(pageId, utcNow(), payload || null, id).changes;
}

function markSendError(db, id, message, permanent) {
  const retryAt = permanent ? BLOCKED_UNTIL : new Date(Date.now() + RETRY_DELAY_MS).toISOString();
  db.prepare(`UPDATE f_inbound_check_destinations
    SET notion_error = ?, notion_next_retry_at = ?, notion_attempt_count = COALESCE(notion_attempt_count, 0) + 1
    WHERE id = ?`).run(String(message).slice(0, 300), retryAt, id);
  return permanent;
}

function markCancelled(db, id, prevStatus) {
  db.prepare(`UPDATE f_inbound_check_destinations
    SET notion_cancelled_at = ?, notion_cancelled_prev_status = ?, notion_cancel_error = NULL, notion_next_retry_at = NULL
    WHERE id = ?`).run(utcNow(), prevStatus || null, id);
}

function markCancelError(db, id, message, permanent) {
  const retryAt = permanent ? BLOCKED_UNTIL : new Date(Date.now() + RETRY_DELAY_MS).toISOString();
  db.prepare(`UPDATE f_inbound_check_destinations
    SET notion_cancel_error = ?, notion_next_retry_at = ?
    WHERE id = ?`).run(String(message).slice(0, 300), retryAt, id);
  return permanent;
}

/** 管理画面の「再送」: ブロック・再試行待ちを解除する (次の sweep / ボタンで拾われる) */
export function resetNotionRow(id) {
  return getDB().prepare(`UPDATE f_inbound_check_destinations
    SET notion_next_retry_at = NULL, notion_error = NULL, notion_cancel_error = NULL
    WHERE id = ?`).run(Number(id)).changes;
}

// ─── 多重実行の防止 (プロセス内フラグ + SQLite lease。Codex R1 #2) ───

let running = false;

function acquireLease(db, holder, now = Date.now()) {
  const tx = db.transaction(() => {
    const cur = db.prepare('SELECT holder, expires_at FROM f_inbound_check_notion_lease WHERE id = 1').get();
    if (cur && cur.holder !== holder && Date.parse(cur.expires_at) > now) return false;
    db.prepare(`INSERT INTO f_inbound_check_notion_lease (id, holder, expires_at) VALUES (1, ?, ?)
      ON CONFLICT(id) DO UPDATE SET holder = excluded.holder, expires_at = excluded.expires_at`)
      .run(holder, new Date(now + LEASE_MS).toISOString());
    return true;
  });
  return tx.immediate();
}

function releaseLease(db, holder) {
  try { db.prepare('DELETE FROM f_inbound_check_notion_lease WHERE id = 1 AND holder = ?').run(holder); }
  catch (e) { console.warn('[inbound-check notion] lease 解放失敗 (期限で自然回収されます):', e.message); }
}

/** lease の延長。自分がもう holder でなければ false (=別実行に引き継がれた → 即中断する。R2 #2) */
function touchLease(db, holder, now = Date.now()) {
  return db.prepare('UPDATE f_inbound_check_notion_lease SET expires_at = ? WHERE id = 1 AND holder = ?')
    .run(new Date(now + LEASE_MS).toISOString(), holder).changes === 1;
}

/**
 * カードを1枚作る (最大3回)。POST 自体は1回ずつで、失敗のたびに台帳キーで再検索してから
 * やり直す — 1回目が「実は成功していた」場合に2枚目を作らない (Codex R2 #1)。
 * @returns {{ id, recoveredAfterError?: boolean, duplicates?: number }}
 */
async function createCardSafe(props, dedupeKey) {
  const MAX = 3;
  let lastErr;
  for (let attempt = 1; attempt <= MAX; attempt++) {
    try {
      return await createCard(props);
    } catch (e) {
      if (isPermanentStatus(e.status)) throw e;   // 4xx は作られていないと確定できる
      lastErr = e;
      await sleep(500 * attempt);
      const found = await findCardsByDedupeKey(dedupeKey);
      if (found.length > 0) {
        return { id: found[0].id, recoveredAfterError: true, duplicates: found.length > 1 ? found.length : 0 };
      }
      // 見つからない = 本当に作られていない → もう一度 POST してよい
    }
  }
  throw lastErr;
}

// ─── sweep 本体 ───

/** page_id のあるカードを「取消」へ倒す (取消フェーズ・孤立回収の共通処理) */
async function convergeCancelledCard(db, rowId, pageId) {
  const state = await getCardState(pageId);
  if (state.archived) {
    // 誰かが Notion 側でアーカイブ済み — もう有効な作業指示として見えないので収束とみなす
    markCancelled(db, rowId, state.status ? `${state.status} (アーカイブ済み)` : '(アーカイブ済み)');
  } else if (state.status === CANCELLED_STATUS) {
    markCancelled(db, rowId, CANCELLED_STATUS);
  } else {
    await setCardStatus(pageId, CANCELLED_STATUS);
    markCancelled(db, rowId, state.status || '(不明)');
    await sleep(CREATE_INTERVAL_MS);
  }
}

/**
 * reconcile を1回実行する。
 * @param {object} opts
 * @param {string} opts.actor  'cron' | 'cron-retry' | 利用者メール
 * @param {string} opts.mode   'full' = 新規送信も行う (17:30・手動ボタン) /
 *                             'retry' = 取消反映・失敗行の再試行だけ (30分巡回。新規は送らない)
 */
export async function runNotionSweep({ actor = 'manual', mode = 'full' } = {}) {
  if (!isNotionConfigured()) {
    // cron から呼ばれたのに未設定 = 「動いているべきものが動いていない」なので fail を残す。
    // 30分巡回 (retry) では静かに帰る (毎回鳴らすと通知疲れになる)
    if (actor === 'cron') ping('fail', 'env未設定 (INBOUND_CHECK_NOTION_DB_ID)');
    return { ok: false, error: 'not_configured', message: 'Notion 連携が未設定です (NOTION_TOKEN / INBOUND_CHECK_NOTION_DB_ID)' };
  }
  if (running) return { ok: false, error: 'already_running', message: '送信処理が既に動いています' };
  running = true;
  const db = getDB();
  const holder = `${process.pid}:${crypto.randomBytes(3).toString('hex')}`;
  try {
    if (!acquireLease(db, holder)) {
      return { ok: false, error: 'already_running', message: '別の実行が進行中です (数分後にやり直してください)' };
    }
    const summary = { sent: 0, recovered: 0, raced: 0, cancelled: 0, errors: 0, skipped: 0, blocked: 0 };
    // 行を処理するたびに lease を延長する。失っていたら (期限切れで別実行が始まった) 即中断 — R2 #2
    const guard = () => {
      if (!touchLease(db, holder)) {
        const e = new Error('lease を失いました (別の実行に引き継がれています)');
        e.code = 'LEASE_LOST';
        throw e;
      }
    };
    let schema;
    try {
      schema = await ensureCardSchema();
    } catch (e) {
      // スキーマ全体の問題は行に書かず、sweep 1回の失敗として報告する (Codex R1 #9)
      console.error('[inbound-check notion] スキーマ確認失敗:', e.message);
      if (mode === 'full') ping('fail', String(e.message).slice(0, 180));
      return { ok: false, error: e.code === 'NOTION_SCHEMA_MISMATCH' ? 'schema_mismatch' : 'schema_error', message: e.message };
    }

    // ① 孤立カードの回収 (取消済み・page_id なし。Codex R1 #1) — 取消を最優先で片付ける
    for (const row of collectOrphanCancelled(db)) {
      guard();
      try {
        if (!row.notion_dedupe_key) { markCancelled(db, row.id, '(カード未作成)'); continue; }
        const found = await findCardsByDedupeKey(row.notion_dedupe_key);
        if (found.length === 0) { markCancelled(db, row.id, '(カード未作成)'); continue; }
        attachPage(db, row.id, found[0].id, null);
        await convergeCancelledCard(db, row.id, found[0].id);
        // 万一 複数枚あれば全部「取消」に倒す (取消済みの指示を1枚も有効に見せない)
        for (const extra of found.slice(1)) {
          try { await setCardStatus(extra.id, CANCELLED_STATUS); }
          catch (e2) { console.warn(`[inbound-check notion] 余分カードの取消失敗 ${extra.id}: ${e2.message}`); }
        }
        summary.cancelled++;
      } catch (e) {
        summary.errors++;
        if (markCancelError(db, row.id, e.message, isPermanentStatus(e.status))) summary.blocked++;
        console.warn(`[inbound-check notion] 孤立回収失敗 dest#${row.id}: ${e.message}`);
      }
    }

    // ② 送信後に取り消された行 → カードを「取消」へ (新規作成より先に。Codex R1 #6)
    const cancelPhase = async () => {
      for (const row of collectCancelPending(db)) {
        guard();
        try {
          await convergeCancelledCard(db, row.id, row.notion_page_id);
          summary.cancelled++;
        } catch (e) {
          summary.errors++;
          if (markCancelError(db, row.id, e.message, isPermanentStatus(e.status))) summary.blocked++;
          console.warn(`[inbound-check notion] 取消反映失敗 dest#${row.id}: ${e.message}`);
        }
      }
    };
    await cancelPhase();

    // ③ 未送信の いろは行き → カード作成 (retry モードでは「一度失敗した行」だけ)
    const unsent = collectUnsent(db, { retryOnly: mode !== 'full' });
    const ctx = unsent.length > 0 ? buildEnrichContext(db) : null;
    for (const row of unsent) {
      guard();
      // API を待っている間に取り消されているかもしれない — 直前にもう一度見る
      const fresh = db.prepare('SELECT cancelled_at, notion_page_id FROM f_inbound_check_destinations WHERE id = ?').get(row.id);
      if (!fresh || fresh.cancelled_at || fresh.notion_page_id) { summary.skipped++; continue; }
      try {
        // ⭐キーを DB に保存してから Notion を触る (作成成功→記録前に落ちても次回回収できる)
        const dedupeKey = ensureDedupeKey(db, row);
        const found = await findCardsByDedupeKey(dedupeKey);
        let pageId;
        let payload;
        let wasRecovered = false;
        let duplicates = found.length > 1 ? found.length : 0;
        if (found.length > 0) {
          pageId = found[0].id;
          payload = JSON.stringify({ recovered: true });
          wasRecovered = true;
        } else {
          const key = String(row.code_key || row.product_id || '').trim().toLowerCase();
          const product = ctx.products.get(key) || null;
          const supplierName = product ? (ctx.suppliers.get(normSupplierCode(product.supplierCode)) || null) : null;
          const ext = calcExternal(ctx.sales30.get(key) ?? null, ctx.freeStock.get(key) ?? null);
          const props = buildCardProperties(row, { barcode: barcodeFor(db, row), product, supplierName, ext, dedupeKey }, schema.names);
          const created = await createCardSafe(props, dedupeKey);
          pageId = created.id;
          wasRecovered = !!created.recoveredAfterError;
          duplicates = created.duplicates || duplicates;
          payload = wasRecovered ? JSON.stringify({ recovered: true }) : JSON.stringify(props);
          await sleep(CREATE_INTERVAL_MS);
        }
        const ch = markSent(db, row.id, pageId, payload);
        if (ch === 1) {
          if (wasRecovered) summary.recovered++; else summary.sent++;
        } else {
          // 記録できなかった = 待っている間に状態が変わった (Codex R1 #3 #4)。取り漏らさず収束させる
          const cur = db.prepare('SELECT cancelled_at, notion_page_id FROM f_inbound_check_destinations WHERE id = ?').get(row.id);
          if (cur && cur.notion_page_id === pageId) {
            if (wasRecovered) summary.recovered++; else summary.sent++;   // 別実行が同じカードを記録済み
          } else if (cur && !cur.notion_page_id) {
            attachPage(db, row.id, pageId, payload);   // 取消済みでも紐付ける → ②の再実行が「取消」へ倒す
            summary.raced++;
          } else {
            // 台帳には別のカードが記録されている — 今回のカードが孤立した疑い。人に見せる
            summary.errors++;
            summary.blocked++;
            markSendError(db, row.id,
              `二重カードの疑い: 台帳=${cur?.notion_page_id || '?'} / 今回=${pageId}。Notion 側を確認して片方を整理してください`, true);
            console.error(`[inbound-check notion] 二重カード疑い dest#${row.id}: db=${cur?.notion_page_id} new=${pageId}`);
          }
        }
        if (duplicates > 1) {
          // 同じ台帳キーのカードが複数 — 自動では消さず、人に見える形で止める (Codex R2 #1)
          summary.errors++;
          summary.blocked++;
          markSendError(db, row.id,
            `台帳キー ${dedupeKey} のカードが ${duplicates} 枚あります。Notion 側で余分な方を整理してから「再送」してください`, true);
          console.error(`[inbound-check notion] 二重カード検出 dest#${row.id}: ${duplicates}枚`);
        }
      } catch (e) {
        summary.errors++;
        if (markSendError(db, row.id, e.message, isPermanentStatus(e.status))) summary.blocked++;
        console.warn(`[inbound-check notion] 送信失敗 dest#${row.id} ${row.product_id}: ${e.message}`);
      }
    }

    // ④ 送信中に取り消された分を同じ sweep で拾う (②の再実行。通常 0 件)
    if (summary.sent + summary.raced > 0) await cancelPhase();

    const note = `作成${summary.sent} 回収${summary.recovered} 取消反映${summary.cancelled} 失敗${summary.errors}`
      + (summary.raced ? ` 競合${summary.raced}` : '')
      + (summary.blocked ? ` (うち要対応${summary.blocked})` : '');
    if (mode === 'full' || summary.sent + summary.recovered + summary.cancelled + summary.errors + summary.raced > 0) {
      console.log(`[inbound-check notion] sweep (${actor}/${mode}): ${note}`);
    }
    // 失敗が1件でもあれば fail (「正常終了したが仕事が残っている」を ok にしない)。
    // dead-man ping は 17:30 の一括 (full) だけ — 30分巡回で毎回 ok を打つと
    // 「一括送信が止まっているのに監視は緑」になる
    if (mode === 'full') ping(summary.errors > 0 ? 'fail' : 'ok', note);
    return { ok: summary.errors === 0, ...summary, note };
  } catch (e) {
    console.error('[inbound-check notion] sweep 失敗:', e);
    if (mode === 'full') ping('fail', String(e.message).slice(0, 180));
    return { ok: false, error: 'sweep_failed', message: e.message };
  } finally {
    releaseLease(db, holder);
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
  const one = (sql, ...p) => db.prepare(sql).get(...p)?.n ?? 0;
  const unsent = one(`SELECT COUNT(*) n FROM f_inbound_check_destinations
    WHERE destination = 'iroha' AND cancelled_at IS NULL AND notion_page_id IS NULL
      AND (notion_next_retry_at IS NULL OR notion_next_retry_at <= ?)`, now);
  // 「再試行待ち (一時エラー・自動で再試行される)」と「要対応 (4xx・再送ボタンが必要)」は別物 (Codex R1 #10)
  const waitingRetry = one(`SELECT COUNT(*) n FROM f_inbound_check_destinations
    WHERE notion_next_retry_at IS NOT NULL AND notion_next_retry_at > ? AND notion_next_retry_at < '9999'`, now);
  const blocked = db.prepare(`SELECT id, product_id, product_name, notion_error, notion_cancel_error
    FROM f_inbound_check_destinations
    WHERE notion_next_retry_at >= '9999'
    ORDER BY id DESC LIMIT 10`).all();
  const cancelPending = one(`SELECT COUNT(*) n FROM f_inbound_check_destinations
    WHERE cancelled_at IS NOT NULL AND notion_cancelled_at IS NULL
      AND (notion_page_id IS NOT NULL OR notion_dedupe_key IS NOT NULL)`);
  const sentRecent = one(`SELECT COUNT(*) n FROM f_inbound_check_destinations
    WHERE notion_synced_at IS NOT NULL AND notion_synced_at >= datetime('now', '-1 day')`);
  const lastSyncedAt = db.prepare('SELECT MAX(notion_synced_at) m FROM f_inbound_check_destinations').get()?.m || null;
  // 要確認: 取消を反映したとき、カードが未着手以外だった (現場が動かしていた) 行。30日分
  const attention = db.prepare(`SELECT id, product_id, product_name, actual_qty, work_date,
      cancel_reason, cancelled_at, notion_cancelled_at, notion_cancelled_prev_status
    FROM f_inbound_check_destinations
    WHERE notion_cancelled_prev_status IS NOT NULL
      AND notion_cancelled_prev_status NOT IN ('未着手', '(カード未作成)')
      AND notion_cancelled_at >= datetime('now', '-30 day')
    ORDER BY notion_cancelled_at DESC LIMIT 20`).all();
  return {
    configured: isNotionConfigured(),
    dbIdTail: (process.env.INBOUND_CHECK_NOTION_DB_ID || '').slice(-6),
    unsent, waitingRetry, cancelPending, sentRecent, lastSyncedAt, blocked, attention,
  };
}
