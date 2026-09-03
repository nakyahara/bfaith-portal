/**
 * いろは在庫化作業アプリ — 一覧の組み立て
 *
 * Notion キャッシュ (カード) に、自社 DB の参照情報を重ねる:
 *   - 作業仕様 = f_iroha_work_master (最新値。カード作成時のスナップショットより優先)
 *   - 販売/在庫 = mirror_sales_daily / mirror_logizard_stock → 優先度 (残り在庫日数)
 *   - 商品画像 = pk_product_images (picking のキャッシュ)
 *
 * 優先度 (要件定義 §5 / Codex R1 Q4):
 *   在庫日数 = フリー在庫 ÷ (30日販売数/30)。**理由を必ず付ける**。
 *   データ欠損をゼロで代用しない — 「在庫データなし」「新商品」はそれぞれ別の表示にする。
 */
import { buildEnrichContext } from '../inbound-check/notion-sync.js';
import { productImageMap } from '../inbound-check/db.js';
import { queueEnsureImages } from '../picking/images.js';
import { getDB, listCache, activeSessionsByPage, activeSessionsByTask, estimateByProduct, workSecondsByTask } from './db.js';
import { mediaByPage, mediaByTask, photosByCodeKey } from './media.js';
import { STATUSES, LIST_STATUSES } from './notion-read.js';
import { OPEN_STATUSES, STATUS_LABEL, TRANSITIONS, HOLD_REASONS, HOLD_LABEL, CLOSE_REASONS, CLOSE_LABEL, statusLabel } from './tasks.js';
import { listOpenTasks, listFacilities, listClosedTasks, countClosedTasks, getTask } from './tasks-db.js';

// 「急ぎ」の線引き: 在庫切れ、または残り在庫日数がこれ以下
export const URGENT_DAYS = 3;

// 販売/在庫/仕入先などの参照マップは重い (mirror_sales_daily の30日集計など) ので数分キャッシュ
const ENRICH_TTL_MS = 5 * 60 * 1000;
let enrichCache = null; // { at, ctx }

function enrichContext() {
  if (enrichCache && Date.now() - enrichCache.at < ENRICH_TTL_MS) return enrichCache.ctx;
  const ctx = buildEnrichContext(getDB());
  enrichCache = { at: Date.now(), ctx };
  return ctx;
}

/** テスト・強制更新用 */
export function clearEnrichCache() { enrichCache = null; }

const keyOf = (code) => String(code || '').trim().toLowerCase();

/** 優先度 (kind: urgent > new > normal > unknown > calm。label は画面へそのまま出す理由) */
export function priorityOf(sales30, freeStock) {
  if (sales30 == null && freeStock == null) return { kind: 'new', label: '新商品', days: null };
  const s = Number(sales30) || 0;
  if (s > 0) {
    if (freeStock == null) return { kind: 'unknown', label: '在庫データなし', days: null };
    if (freeStock <= 0) return { kind: 'urgent', label: '在庫切れ', days: 0 };
    const days = freeStock / (s / 30);
    const label = `残り${days < 10 ? days.toFixed(1) : String(Math.round(days))}日分`;
    return { kind: days <= URGENT_DAYS ? 'urgent' : 'normal', label, days };
  }
  return { kind: 'calm', label: '30日販売なし', days: null };
}

const PRIORITY_RANK = { urgent: 0, new: 1, normal: 2, unknown: 3, calm: 4 };

/**
 * 作業仕様。f_iroha_work_master があればそれ (最新)、無ければカード作成時の
 * スナップショット (Notion プロパティ) で代用し、source で区別する。
 * missing = 現場が作業を始めるのに足りない項目 (⚠未登録バッジの根拠)
 */
export function masterOf(wm, props) {
  return mergeMaster(wm, {
    material_code: props['資材セットID'] || null, storage_container: props['収納容器'] || null,
    units_per_container: props['入数'] ?? null, process_count: props['工程数'] ?? null, note: props['備考'] || null,
  });
}

/** 同上。アプリ正本のカードはカード側の値が作成時スナップショット (正規化済みの形) */
export function masterOfTask(wm, snapshot) {
  const s = snapshot || {};
  return mergeMaster(wm, {
    material_code: s.material_code || null, storage_container: s.storage_container || null,
    units_per_container: s.units_per_container ?? null, process_count: s.process_count ?? null, note: s.note || null,
  });
}

function mergeMaster(wm, card) {
  // ⭐項目単位でカード値へフォールバックする (Codex PR4 #1: マスタ行が「動画だけ」でも、
  //   カードに載っている資材・入数の表示を消さない)。version はマスタ行の有無で決まる
  const m = wm
    ? { source: 'master', version: wm.version,
        material_code: wm.material_code || card.material_code, storage_container: wm.storage_container || card.storage_container,
        units_per_container: wm.units_per_container ?? card.units_per_container,
        process_count: wm.process_count ?? card.process_count, note: wm.note || card.note,
        video_url: wm.video_url || null }
    : { source: 'card', version: null, ...card, video_url: null };
  const missing = [];
  if (!m.material_code) missing.push('資材');
  if (!m.storage_container) missing.push('保管箱');   // 画面の呼び名は「保管箱」(中原さん 2026-09-03。旧「入れもの」)
  if (m.units_per_container == null) missing.push('入数');
  if (m.process_count == null) missing.push('工程');
  m.missing = missing;
  return m;
}

/**
 * その場登録の権限判定 (要件 §7 と FB③ の折衷。2026-09-02 実装判断):
 *   - **空欄を埋める**だけの変更 = 作業者なら誰でも (新商品で現場が止まらないように。履歴に残る)
 *   - **入っている値の変更・削除** = 職員のみ (マスタ編集は職員権限 — 要件 §7)
 * @returns {fills: string[], overwrites: string[]} 変更が既存値を書き換えるかの内訳
 */
export function classifyMasterEdit(row, fields) {
  const fills = [];
  const overwrites = [];
  for (const [f, nv] of Object.entries(fields)) {
    const cur = row ? row[f] : null;
    const curEmpty = cur == null || String(cur).trim() === '';
    const next = nv == null ? '' : String(nv).trim();
    if (curEmpty && next !== '') fills.push(f);
    else if (!curEmpty && next !== String(cur).trim()) overwrites.push(f);
    // curEmpty && next==='' (空→空) は変更なし扱い
  }
  return { fills, overwrites };
}

/**
 * 一覧を組み立てて優先度順に並べる。
 * 並び = 急ぎ (在庫日数昇順) → 新商品 → 通常 (在庫日数昇順) → データなし → 販売なし、
 * 同順位は入庫日の古い順 (要件定義 §5)。タブごとの絞り込みは画面側で行う。
 */
/** 新しい順の候補 (photosByCodeKey) から、自分以外で直近に撮ったカード1件ぶんを取り出す。
 *  own = 自分のカードの識別子 (Notion 正本は page_id、アプリ正本は 't'+task_id) */
export function previousPhotosOf(candidates, own, limit = 3) {
  const key = (p) => (p.card != null ? p.card : p.page_id);   // 旧形式 (page_id だけ) の呼び出しも通す
  const others = candidates.filter(p => key(p) !== own);
  if (others.length === 0) return [];
  const lastCard = key(others[0]);   // 先頭 = いちばん最近撮った写真 → そのカード
  return others.filter(p => key(p) === lastCard).slice(0, limit);
}

export function buildList() {
  const rows = listCache();
  const ctx = enrichContext();
  const images = productImageMap(rows.map(r => r.product_code));
  const activeMap = activeSessionsByPage();
  const estimates = estimateByProduct();
  const mediaMap = mediaByPage();
  const prevPhotos = photosByCodeKey();
  const zStock = stockMapByPrefix(rows.map((r) => keyOf(r.product_code)), 'Z');

  const cards = rows.map((r) => {
    let props = {};
    try { props = JSON.parse(r.payload || '{}'); } catch { /* 壊れた payload は素の表示になるだけ */ }
    const k = keyOf(r.product_code);
    const sales30 = k ? (ctx.sales30.get(k) ?? null) : null;
    const freeStock = k ? (ctx.freeStock.get(k) ?? null) : null;
    return {
      id: r.page_id,          // 画面はモードによらず id でカードを指す (アプリ正本では task.id)
      page_id: r.page_id,
      status: r.status,
      status_label: r.status,
      title: r.title,
      product_code: r.product_code,
      url: r.url,
      last_edited_time: r.last_edited_time,
      qty: props['数量'] ?? null,
      arrival: props['入庫日'] || null,
      ar_no: props['入荷管理番号'] || null,
      barcode: props['バーコード'] || null,
      expiry: props['有効期限'] || null,
      supplier: props['取引先'] || null,
      handling: props['取扱区分'] || null,
      sales_text: props['過去30日販売数'] || null,
      external_text: props['外部出し目安'] || null,
      image_url: images.get(k) || null,
      master: masterOf(k ? ctx.workMaster.get(k) : null, props),
      live: { sales30, free_stock: freeStock },
      priority: priorityOf(sales30, freeStock),
      plan_hours: planHours(props['数量'], masterOf(k ? ctx.workMaster.get(k) : null, props).process_count),
      boxes: neededBoxes(props['数量'], masterOf(k ? ctx.workMaster.get(k) : null, props).units_per_container,
        k && zStock.get(k) ? zStock.get(k).stock : 0, k && zStock.get(k) ? zStock.get(k).allocated : 0),
      z_stock: k && zStock.get(k) ? zStock.get(k).stock : null,
      z_allocated: k && zStock.get(k) ? zStock.get(k).allocated : null,
      z_free: k && zStock.get(k) ? zStock.get(k).stock - zStock.get(k).allocated : null,
      z_at: k && zStock.get(k) ? zStock.get(k).captured : null,
      loc_kind: 'Z',
      loc_stock: k && zStock.get(k) ? zStock.get(k).stock : null,
      loc_allocated: k && zStock.get(k) ? zStock.get(k).allocated : null,
      loc_free: k && zStock.get(k) ? zStock.get(k).stock - zStock.get(k).allocated : null,
      loc_at: k && zStock.get(k) ? zStock.get(k).captured : null,
      // 作業時間: いま作業中の人 + 過去の実測 (カード単位合計の平均。1回だけなら「前回」表示)
      active: activeMap.get(r.page_id) || [],
      estimate: (k && estimates.get(k)) || null,
      media: mediaMap.get(r.page_id) || [],
      // 「前回の完成形」= 同じ商品コードで**直近に写真を撮った他の1カード**の写真 (最大3枚)。
      // 複数カードの写真を混ぜない・古いカードを開いても「いちばん最近の完成形」を見せる (Codex R1 #4)。
      // 次に同じ商品を作る人への見本 (中原さん 2026-09-03: 写真は証拠ではなく見本)
      previous_photos: previousPhotosOf(k ? (prevPhotos.get(k) || []) : [], r.page_id),
    };
  });

  cards.sort((a, b) => {
    const ra = PRIORITY_RANK[a.priority.kind] ?? 9;
    const rb = PRIORITY_RANK[b.priority.kind] ?? 9;
    if (ra !== rb) return ra - rb;
    const da = a.priority.days ?? Infinity;
    const db_ = b.priority.days ?? Infinity;
    if (da !== db_) return da - db_;
    const aa = a.arrival || '9999-99-99';
    const ab = b.arrival || '9999-99-99';
    if (aa !== ab) return aa < ab ? -1 : 1;
    return String(a.title).localeCompare(String(b.title), 'ja');
  });

  // 画像がまだ無い商品は裏で解決を仕掛けておく (次に開いたとき出る)
  const missingImg = [...new Set(cards.filter(c => c.product_code && !c.image_url).map(c => c.product_code))];
  if (missingImg.length > 0) {
    try { queueEnsureImages(missingImg, 'いろは作業アプリ'); } catch (e) { console.warn('[iroha-work] 画像解決を飛ばしました:', e.message); }
  }

  return { mode: 'notion', cards, statuses: LIST_STATUSES, changeTargets: STATUSES };
}

/**
 * タスク行 → 画面のカード。一覧・ボード・下見・履歴の詳細で同じ形 (buildList と同じ cards[] の形なので画面は共通)。
 * 違いは識別子 (id = task.id) と状態 (status = 英語の値・status_label = 表示) と、拠点・保留理由・「今日やる」を持つこと。
 * rows に closed が混ざっていてもそのまま作る (履歴の詳細用)。並べ替えと画像の取り寄せは呼び出し側
 */
function buildTaskCards(rows) {
  const ctx = enrichContext();
  const images = productImageMap(rows.map(r => r.product_code));
  const activeMap = activeSessionsByTask();
  const estimates = estimateByProduct();
  const mediaMap = mediaByTask();
  const prevPhotos = photosByCodeKey();
  const zStock = stockMapByPrefix(rows.map((r) => keyOf(r.product_code)), 'Z');
  // 外部 (羅針盤・ワークセンター) に出したカードは Y ロケを見せる。その拠点のカードの商品だけ引く
  const yStock = stockMapByPrefix(rows.filter((r) => stockLocOf(r.facility_code) === 'Y').map((r) => keyOf(r.product_code)), 'Y');
  const today = jstToday();

  const cards = rows.map((r) => {
    let props = {};
    try { props = JSON.parse(r.payload || '{}'); } catch { /* 壊れた payload は素の表示になるだけ */ }
    let snapshot = null;
    try { snapshot = r.master_snapshot ? JSON.parse(r.master_snapshot) : null; } catch { /* 同上 */ }
    const k = keyOf(r.product_code);
    const sales30 = k ? (ctx.sales30.get(k) ?? null) : null;
    const freeStock = k ? (ctx.freeStock.get(k) ?? null) : null;
    const master = masterOfTask(k ? ctx.workMaster.get(k) : null, snapshot);
    const z = k ? zStock.get(k) : null;
    // 画面に出す在庫: 拠点で Z か Y かが変わる (必要保管箱は Notion の式のまま Z を使う)
    const locKind = stockLocOf(r.facility_code);
    const loc = k ? (locKind === 'Y' ? yStock.get(k) : z) : null;
    return {
      id: r.id,
      page_id: r.notion_page_id,          // Notion 時代の証跡 (詳細のリンク用。無ければ null)
      status: r.status,                   // 値 (not_started …)
      status_label: statusLabel(r),       // 表示 (「保留 · ラベル待ち」など)
      facility_code: r.facility_code,
      hold_reason_code: r.hold_reason_code,
      hold_reason_note: r.hold_reason_note,
      planned_date: r.planned_date,
      today: r.planned_date === today,
      external_ready: !!r.external_ready,
      version: r.version,
      migration_review: !!r.migration_review,
      cancellation_requested_at: r.cancellation_requested_at,
      title: r.product_name || '(名称なし)',
      product_code: r.product_code,
      url: r.notion_page_id ? `https://www.notion.so/${String(r.notion_page_id).replace(/-/g, '')}` : null,
      qty: r.qty ?? null,
      arrival: r.arrival_date || null,
      ar_no: r.ar_no || null,
      barcode: r.barcode || null,
      expiry: r.expiry || null,
      supplier: r.supplier || null,
      handling: r.handling || null,
      sales_text: props['過去30日販売数'] || null,
      external_text: props['外部出し目安'] || null,
      image_url: images.get(k) || null,
      master,
      live: { sales30, free_stock: freeStock },
      priority: priorityOf(sales30, freeStock),
      active: activeMap.get(r.id) || [],
      estimate: (k && estimates.get(k)) || null,
      // 想定作業時間・必要保管箱 (Notion の計算式をそのまま。ボードでは状態ごとに合計する)
      plan_hours: planHours(r.qty, master.process_count),
      boxes: neededBoxes(r.qty, master.units_per_container, z ? z.stock : 0, z ? z.allocated : 0),
      // Z ロケ (一時保管) の在庫。在庫ミラーは毎時更新で、画面は 60 秒ごとに取り直すので自動で新しくなる
      z_stock: z ? z.stock : null,
      z_allocated: z ? z.allocated : null,
      z_free: z ? z.stock - z.allocated : null,
      z_at: z ? z.captured : null,
      loc_kind: locKind,
      loc_stock: loc ? loc.stock : null,
      loc_allocated: loc ? loc.allocated : null,
      loc_free: loc ? loc.stock - loc.allocated : null,
      loc_at: loc ? loc.captured : null,
      media: mediaMap.get(r.id) || [],
      previous_photos: previousPhotosOf(k ? (prevPhotos.get(k) || []) : [], `t${r.id}`),
    };
  });
  return { cards, today };
}

/** 1 枚だけ (下見・履歴の詳細)。終了したタスクも返す。無ければ null */
export function buildTaskCard(id) {
  const t = getTask(id);
  if (!t) return null;
  return buildTaskCards([t]).cards[0] || null;
}

/**
 * 一覧 (アプリ正本 = f_iroha_tasks)。
 * 終了 (closed) は含めない (履歴画面で見る — 中原さん 2026-09-03「完了が溜まる一方なのを何とかしたい」)
 */
export function buildTaskList({ facility = null } = {}) {
  const rows = listOpenTasks({ facility });
  const { cards, today } = buildTaskCards(rows);

  // 並び: 今日やる → 優先度 (急ぎ→新商品→通常→データなし→販売なし) → 在庫日数 → 入庫が古い順
  cards.sort((a, b) => {
    if (a.today !== b.today) return a.today ? -1 : 1;
    const ra = PRIORITY_RANK[a.priority.kind] ?? 9;
    const rb = PRIORITY_RANK[b.priority.kind] ?? 9;
    if (ra !== rb) return ra - rb;
    const da = a.priority.days ?? Infinity;
    const db_ = b.priority.days ?? Infinity;
    if (da !== db_) return da - db_;
    const aa = a.arrival || '9999-99-99';
    const ab = b.arrival || '9999-99-99';
    if (aa !== ab) return aa < ab ? -1 : 1;
    return String(a.title).localeCompare(String(b.title), 'ja');
  });

  const missingImg = [...new Set(cards.filter(c => c.product_code && !c.image_url).map(c => c.product_code))];
  if (missingImg.length > 0) {
    try { queueEnsureImages(missingImg, 'いろは作業アプリ'); } catch (e) { console.warn('[iroha-work] 画像解決を飛ばしました:', e.message); }
  }

  return {
    mode: 'app',
    cards,
    statuses: OPEN_STATUSES.map(s => ({ value: s, label: STATUS_LABEL[s] })),
    transitions: TRANSITIONS,
    holdReasons: HOLD_REASONS.map(v => ({ value: v, label: HOLD_LABEL[v] })),
    closeReasons: CLOSE_REASONS.map(v => ({ value: v, label: CLOSE_LABEL[v] })),
    facilities: listFacilities(),
    today,
  };
}

/**
 * 履歴 (終了したタスク)。一覧には出さないが DB には残す — 中原さん 2026-09-03「完了が溜まる一方なのを何とかしたい」→
 * 期間と検索で絞って見る画面のためのデータ。作業時間の合計も出す (次の目安になる)
 */
export function buildHistory({ from = null, to = null, q = null, limit = 200 } = {}) {
  const lim = Math.max(1, Math.min(500, Number(limit) || 200));
  const rows = listClosedTasks({ from, to, q, limit: lim });
  const secs = workSecondsByTask(rows.map((r) => r.id));
  const facilities = listFacilities(true);
  const facName = (code) => (facilities.find((f) => f.code === code) || {}).name || code;
  return {
    rows: rows.map((r) => {
      const w = secs.get(r.id) || null;
      return {
        id: r.id, title: r.product_name || '(名称なし)', product_code: r.product_code, qty: r.qty ?? null,
        arrival: r.arrival_date || null, ar_no: r.ar_no || null,
        facility_code: r.facility_code, facility_name: facName(r.facility_code),
        close_reason: r.close_reason, status_label: statusLabel(r),
        closed_at: r.closed_at, closed_by: r.closed_by,
        work_seconds: w ? w.seconds : 0, workers: w ? w.people : 0,
      };
    }),
    total: countClosedTasks({ from, to, q }),
    limit: lim, from: from || null, to: to || null, q: q || null,
  };
}

/**
 * 想定作業時間 (時間)。Notion の計算式そのまま: round(数量 × 工程数 × 5秒 / 3600 × 10) / 10
 * (1 工程あたり 5 秒。中原さん 2026-09-03 提示)
 */
export function planHours(qty, processCount) {
  const q = Number(qty);
  const p = Number(processCount);
  if (!Number.isFinite(q) || !Number.isFinite(p) || q <= 0 || p <= 0) return null;
  const seconds = q * p * 5;
  // 掛け算で桁があふれたら出さない (Infinity を画面や JSON に出さない — Codex FB R1)
  if (!Number.isFinite(seconds) || seconds > Number.MAX_SAFE_INTEGER) return null;
  return Math.round(seconds / 3600 * 10) / 10;
}

/**
 * 必要保管箱。Notion の計算式そのまま (中原さん 2026-09-03 提示):
 *   入数が無ければ空。Z ロケに在庫があればその引当を引いた数、無ければカードの数量を入数で割る。
 *   割り切れれば「N箱」、余りが出れば「N箱+余り」
 */
export function neededBoxes(qty, unitsPerContainer, zStock = 0, zAllocated = 0) {
  const per = Number(unitsPerContainer);
  if (!Number.isSafeInteger(per) || per <= 0) return null;
  const z = Number(zStock) || 0;
  const base = z > 0 ? z - (Number(zAllocated) || 0) : Number(qty);
  // 整数で数えられる範囲だけ (小数・桁あふれは箱数を保証できないので出さない — Codex FB R1)
  if (!Number.isSafeInteger(base) || base < 0) return null;
  const boxes = Math.floor(base / per);
  const rest = base % per;
  return rest === 0 ? `${boxes}箱` : `${boxes}箱+${rest}`;
}

/**
 * ロケ別の在庫。商品コードごとに 在庫数・引当数 を合計する (良品だけ。不良品は外に出せない)。
 *   Z = 一時保管 (手元にある)。Notion 時代の Z在庫数 / Z引当数 に当たる
 *   Y = 出荷禁止 (外部施設に出している分)。外に預けると Z→Y、戻ると Y→Z (中原さん)
 * 見分けは「ロケ または ブロック略称 がその文字ではじまる」
 */
function stockMapByPrefix(codeKeys, prefix) {
  const db = getDB();
  const map = new Map();
  const keys = [...new Set((codeKeys || []).filter(Boolean))];
  if (keys.length === 0) return map;
  if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'mirror_logizard_stock'").get()) return map;
  const like = `${prefix}%`;
  // 画面に出すカードの商品だけを数える (在庫ミラーは数千〜数万行。一覧を開くたびに全表を舐めない — Codex FB R1)
  for (let i = 0; i < keys.length; i += 400) {
    const chunk = keys.slice(i, i + 400);
    const rows = db.prepare(`SELECT LOWER(TRIM(商品ID)) AS k, SUM(在庫数) AS zaiko, SUM(引当数) AS hikiate,
        MAX(captured_at) AS captured, COUNT(*) AS locs
      FROM mirror_logizard_stock
      WHERE LOWER(TRIM(商品ID)) IN (${chunk.map(() => '?').join(',')})
        AND (ロケ LIKE ? OR ブロック略称 LIKE ?) AND 品質区分名 = '良品'
      GROUP BY LOWER(TRIM(商品ID))`).all(...chunk, like, like);
    for (const r of rows) if (r.k) map.set(r.k, { stock: Number(r.zaiko) || 0, allocated: Number(r.hikiate) || 0, captured: r.captured || null, locs: r.locs });
  }
  return map;
}

/**
 * その拠点のカードで見せる在庫のロケ (中原さん 2026-09-03)。
 * 羅針盤・ワークセンターに出したものは Y ロケ (出荷禁止 = 外に出している分) に移るので、Y を見せる。
 * それ以外 (いろは・ジョブサポ・リハス) は手元の Z ロケ
 */
const Y_FACILITIES = new Set(['rashinban', 'workcenter']);
export const stockLocOf = (facilityCode) => (Y_FACILITIES.has(facilityCode) ? 'Y' : 'Z');

/** JST の今日 (YYYY-MM-DD)。「今日やる」の判定に使う */
export function jstToday(now = new Date()) {
  return new Date(now.getTime() + 9 * 3600 * 1000).toISOString().slice(0, 10);
}
