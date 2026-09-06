/**
 * いろは在庫化作業アプリ — 一覧の組み立て
 *
 * Notion キャッシュ (カード) に、自社 DB の参照情報を重ねる:
 *   - 作業仕様 = f_iroha_work_master (最新値。カード作成時のスナップショットより優先)
 *   - 販売/在庫 = mirror_sales_daily / mirror_logizard_stock → 優先度 (残り在庫日数)
 *   - 商品画像 = pk_product_images (picking のキャッシュ)
 *
 * 優先度 (要件定義 §5 / §AA。中原さん 2026-09-06 に定義し直し):
 *   在庫日数 = **出荷できる在庫** ÷ (30日販売数/30)。
 *   出荷できる在庫 = フリー在庫の合計 − Z ロケ (在庫化待ち) − Y ロケ (外部施設に出している分)。
 *   Z・Y はまだ売り物になっていないので数えない。この日数が 1 週間以下 = **急ぎ**
 *   (= 1 週間以内に Z ロケの在庫を使わないといけない商品)。
 *   **理由を必ず付ける**。データ欠損をゼロで代用しない — 「在庫データなし」「新商品」はそれぞれ別の表示にする。
 */
import { buildEnrichContext } from '../inbound-check/notion-sync.js';
import { productImageMap } from '../inbound-check/db.js';
import { queueEnsureImages } from '../picking/images.js';
import { getDB, listCache, activeSessionsByPage, activeSessionsByTask, estimateByProduct, workSecondsByTask, finishedSessionsOfTask } from './db.js';
import { mediaByPage, mediaByTask, photosByCodeKey } from './media.js';
import { STATUSES, LIST_STATUSES } from './notion-read.js';
import { OPEN_STATUSES, STATUS_LABEL, TRANSITIONS, BLOCK_REASONS, BLOCK_LABEL, BLOCK_BUTTON, CLOSE_REASONS, CLOSE_LABEL, statusLabel, blockLabel } from './tasks.js';
import { listOpenTasks, listFacilities, listClosedTasks, countClosedTasks, getTask } from './tasks-db.js';

/**
 * ⭐「急ぎ」の線引き (中原さん 2026-09-06)。
 * 過去 30 日の売上と**出荷できる自社在庫**を比べて、**1 週間以内に Z ロケ (在庫化ロケ) の在庫を
 * 使わないといけない**商品 = 急ぎ。出荷できる在庫が 0 (在庫切れ) も同じ扱い。
 * 「在庫が少ない」も同じ計算で決める (明日どれをやるかの並びと同じものさしにする)
 */
export const URGENT_DAYS = 7;
/**
 * ⭐新商品 (売上も在庫も分からない) の扱い (中原さん 2026-09-06)。
 * そのままだと日数が出せず後ろに沈むので、**10 日で在庫がなくなる商品**とみなして並べる。
 * 表示は「新商品」のまま (作った日数を画面に出して誤解させない)
 */
export const NEW_PRODUCT_DAYS = 10;

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

/**
 * 優先度。**shippableFree = 出荷できる在庫** (Z ロケ・Y ロケを除いたフリー在庫) で計算する。
 *   kind  = urgent / new / normal / unknown / calm。label は画面へそのまま出す理由
 *   days  = 出荷できる在庫が何日もつか (出せないときは null)
 *   sort_days = 並びに使う日数。新商品は 10 日とみなす。分からないもの (null) は最後
 */
export function priorityOf(sales30, shippableFree) {
  if (sales30 == null && shippableFree == null) return { kind: 'new', label: '新商品', days: null, sort_days: NEW_PRODUCT_DAYS };
  const s = Number(sales30) || 0;
  if (s > 0) {
    if (shippableFree == null) return { kind: 'unknown', label: '在庫データなし', days: null, sort_days: null };
    // 出せる在庫が無い = 次に売れる分は Z ロケの在庫化待ちしかない
    if (shippableFree <= 0) return { kind: 'urgent', label: '在庫切れ', days: 0, sort_days: 0 };
    const days = shippableFree / (s / 30);
    const label = `残り${days < 10 ? days.toFixed(1) : String(Math.round(days))}日分`;
    return { kind: days <= URGENT_DAYS ? 'urgent' : 'normal', label, days, sort_days: days };
  }
  return { kind: 'calm', label: '30日販売なし', days: null, sort_days: null };
}

/**
 * 出荷できる在庫 = フリー在庫の合計 − Z ロケ − Y ロケ (どちらもまだ売り物になっていない)。
 * ミラーが取れていなければ null (0 で代用しない)
 */
export function shippableFreeOf(totalFree, z, y) {
  if (totalFree == null) return null;
  const n = Number(totalFree);
  if (!Number.isFinite(n)) return null;      // 数にならない在庫は「分からない」(0 で代用しない)
  // ミラーの一時的なずれ (他ロケの引当超過など) で負にしない。「出せる在庫 -5個」を画面に出さない (Codex R1 #6)
  return Math.max(0, n - pendingFreeOf(z, y));
}
/** 在庫化待ち (Z + Y) のフリー在庫 */
export function pendingFreeOf(z, y) {
  const f = (loc) => (loc ? (Number(loc.stock) || 0) - (Number(loc.allocated) || 0) : 0);
  return f(z) + f(y);
}

// 同じ日数のときの並び順 (日数が出ない unknown / calm を分けるためにも使う)
const PRIORITY_RANK = { urgent: 0, new: 1, normal: 2, unknown: 3, calm: 4 };

/**
 * ⭐優先度の比較 (一覧・ボード・明日の計画で共通)。
 * 在庫が少ない順 = sort_days の昇順。新商品は 10 日 (§AA)、日数の出ないものは最後。
 * 同じ日数なら kind の順 (急ぎ → 新商品 → 通常 → データなし → 販売なし)
 */
/** 並びに使う在庫日数。数にならないもの (null・NaN) は最後に回す — 比較結果を NaN にしない (Codex R1 #3) */
const sortDaysOf = (c) => {
  const v = c?.priority?.sort_days;
  if (v == null) return Infinity;          // 日数が出ないもの (Number(null) は 0 なので先に弾く)
  const n = Number(v);
  return Number.isFinite(n) ? n : Infinity;
};

export function comparePriority(a, b) {
  const da = sortDaysOf(a);
  const db_ = sortDaysOf(b);
  if (da !== db_) return da - db_;
  const ra = PRIORITY_RANK[a.priority?.kind] ?? 9;
  const rb = PRIORITY_RANK[b.priority?.kind] ?? 9;
  return ra - rb;
}

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
        // 期限シールはマスタだけが持つ (カードには無い項目)。
        // 大きさ (size_class) は廃止 (中原さん 2026-09-06 — §AA)。列は残っているが画面へ渡さない
        video_url: wm.video_url || null,
        expiry_seal: wm.expiry_seal == null ? null : Number(wm.expiry_seal) }
    : { source: 'card', version: null, ...card, video_url: null, expiry_seal: null };
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
  const codeKeys = rows.map((r) => keyOf(r.product_code));
  const propsByPage = new Map();
  for (const r of rows) {
    let p = {};
    try { p = JSON.parse(r.payload || '{}'); } catch { /* 壊れた payload は素の表示になるだけ */ }
    propsByPage.set(r.page_id, p);
  }
  // ⭐Z (在庫化待ち)・Y (外部施設)・全ロケ を 1 回で数える — 在庫日数は「出荷できる在庫」で出す (§AA)
  const stocks = stockByCode(codeKeys);
  // Notion 正本のカードは f_iroha_tasks に居ないので、カード自身の入庫日を材料として渡す (Codex R2)
  const arrivals = arrivalHistory(codeKeys,
    rows.map((r) => ({ k: keyOf(r.product_code), ymd: (propsByPage.get(r.page_id) || {})['入庫日'] || null })));
  const mirrorAt = mirrorCapturedAt();

  const cards = rows.map((r) => {
    const props = propsByPage.get(r.page_id) || {};
    const k = keyOf(r.product_code);
    const sales30 = k ? (ctx.sales30.get(k) ?? null) : null;
    const st = (k && stocks.get(k)) || NO_STOCK;
    const zLoc = st.z;
    // 全ロケの合計は在庫ミラーから直接数える (Z/Y と同じ 1 回の読み取り。ミラーに行が無ければ null)
    const freeStock = st.total ? st.total.stock - st.total.allocated : null;
    const shippable = shippableFreeOf(freeStock, zLoc, st.y);
    const arrival = props['入庫日'] || null;
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
      arrival,
      // ⭐🌱「はじめての商品」= この商品の入荷実績が今回より前に無い (§AA)。判定材料が無ければ null
      first_time: arrivals.firstTime(k, arrival),
      ar_no: props['入荷管理番号'] || null,
      barcode: props['バーコード'] || null,
      expiry: props['有効期限'] || null,
      supplier: props['取引先'] || null,
      handling: props['取扱区分'] || null,
      sales_text: props['過去30日販売数'] || null,
      external_text: props['外部出し目安'] || null,
      image_url: images.get(k) || null,
      master: masterOf(k ? ctx.workMaster.get(k) : null, props),
      // free_stock = 全ロケの合計、shippable_free = そこから在庫化待ち (Z+Y) を引いた「いま出せる分」
      live: { sales30, free_stock: freeStock, shippable_free: shippable, pending_free: pendingFreeOf(zLoc, st.y) },
      priority: priorityOf(sales30, shippable),
      plan_hours: planHours(props['数量'], masterOf(k ? ctx.workMaster.get(k) : null, props).process_count),
      boxes: neededBoxes(props['数量'], masterOf(k ? ctx.workMaster.get(k) : null, props).units_per_container,
        zLoc ? zLoc.stock : 0, zLoc ? zLoc.allocated : 0),
      boxes_calc: neededBoxesCalc(props['数量'], masterOf(k ? ctx.workMaster.get(k) : null, props).units_per_container,
        zLoc ? zLoc.stock : 0, zLoc ? zLoc.allocated : 0),
      // Z ロケ行が無ければ「Z に 0」(ミラーが取れている限り)。取れていなければ null (監修 B-8)
      ...locFields(zLoc, zLoc, 'Z', mirrorAt),
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

  cards.sort(comparePlanOrder);   // アプリ正本の一覧・ボード・明日の計画と同じ並び (§AA・Codex R1 #2)

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
 * rows に closed が混ざっていてもそのまま作る (履歴の詳細用)。並べ替えと画像の取り寄せは呼び出し側。
 * readOnly = 下見・履歴。DB を一切変えない (写真の修復印も付けない・対象カードの写真だけ引く — Codex PR1 R8)
 */
function buildTaskCards(rows, { readOnly = false } = {}) {
  const ctx = enrichContext();
  const images = productImageMap(rows.map(r => r.product_code));
  const activeMap = activeSessionsByTask();
  const estimates = estimateByProduct();
  const mediaMap = mediaByTask(readOnly ? { ids: rows.map((r) => r.id), repair: false } : undefined);
  const prevPhotos = photosByCodeKey();
  const codeKeys = rows.map((r) => keyOf(r.product_code));
  // ⭐Z (在庫化待ち)・Y (外部施設)・全ロケ を 1 回で数える。Y は**全カードぶん**引く —
  //   表示 (羅針盤・ワークセンターのカードは Y を見せる) だけでなく「出荷できる在庫」の計算にも使う (§AA)
  const stocks = stockByCode(codeKeys);
  const mirrorAt = mirrorCapturedAt();
  // 大きさ (嵩) = 配送方法。明日どれをやるかの並びに**だけ**使う (画面には出さない — §AA)
  const sizes = sizeMapByCode(codeKeys);
  // 入荷実績 (🌱「はじめての商品」の判定。§AA)
  const arrivals = arrivalHistory(codeKeys);
  const today = jstToday();
  const tomorrow = jstTomorrow(today);

  const cards = rows.map((r) => {
    let props = {};
    try { props = JSON.parse(r.payload || '{}'); } catch { /* 壊れた payload は素の表示になるだけ */ }
    let snapshot = null;
    try { snapshot = r.master_snapshot ? JSON.parse(r.master_snapshot) : null; } catch { /* 同上 */ }
    const k = keyOf(r.product_code);
    const sales30 = k ? (ctx.sales30.get(k) ?? null) : null;
    const master = masterOfTask(k ? ctx.workMaster.get(k) : null, snapshot);
    const st = (k && stocks.get(k)) || NO_STOCK;
    // 全ロケの合計は在庫ミラーから直接数える (Z/Y と同じ 1 回の読み取り。ミラーに行が無ければ null)
    const freeStock = st.total ? st.total.stock - st.total.allocated : null;
    const z = st.z;
    const y = st.y;
    // 画面に出す在庫: 拠点で Z か Y かが変わる (必要保管箱は Notion の式のまま Z を使う)
    const locKind = stockLocOf(r.facility_code);
    const loc = locKind === 'Y' ? y : z;
    const shippable = shippableFreeOf(freeStock, z, y);
    return {
      id: r.id,
      page_id: r.notion_page_id,          // Notion 時代の証跡 (詳細のリンク用。無ければ null)
      status: r.status,                   // 値 (not_started …)
      status_label: statusLabel(r),       // 表示 (「終了 · 棚入完了」など)
      facility_code: r.facility_code,
      // ⭐止まっている理由の札 (要件 §Y-2 = 案A)。進捗とは別の軸。null = 止まっていない
      blocked: r.blocked_reason ? { reason: r.blocked_reason, label: BLOCK_LABEL[r.blocked_reason] || r.blocked_reason,
        note: r.blocked_note || null, at: r.blocked_at || null, by: r.blocked_by || null } : null,
      blocked_label: blockLabel(r),
      // ⭐できた数と中断メモ (要件 §Y)。done_qty は NULL = まだ数えていない (0 と区別する)
      done_qty: r.done_qty ?? null,
      hold_memo: r.hold_memo || null,
      planned_date: r.planned_date,
      today: r.planned_date === today,
      // ⭐3 軸のうちの「いつ」。planned_date (実日付) から出す — today / tomorrow / over (やり残し) / later / null (未定)
      when: r.status === 'closed' ? null : whenOf(r.planned_date, today, tomorrow),
      // ⭐大きさ (嵩) = 配送方法から見なす。**並びに使うだけで画面には出さない** (中原さん 2026-09-06 — §AA)
      size_rank: (k && sizes.get(k)) ? sizes.get(k).rank : null,
      external_ready: !!r.external_ready,
      version: r.version,
      migration_review: !!r.migration_review,
      cancellation_requested_at: r.cancellation_requested_at,
      title: r.product_name || '(名称なし)',
      product_code: r.product_code,
      url: r.notion_page_id ? `https://www.notion.so/${String(r.notion_page_id).replace(/-/g, '')}` : null,
      qty: r.qty ?? null,
      arrival: r.arrival_date || null,
      // ⭐🌱「はじめての商品」= この商品の入荷実績が今回より前に無い (§AA)。判定材料が無ければ null
      first_time: arrivals.firstTime(k, r.arrival_date || null),
      ar_no: r.ar_no || null,
      barcode: r.barcode || null,
      expiry: r.expiry || null,
      supplier: r.supplier || null,
      handling: r.handling || null,
      sales_text: props['過去30日販売数'] || null,
      external_text: props['外部出し目安'] || null,
      image_url: images.get(k) || null,
      master,
      // free_stock = 全ロケの合計、shippable_free = そこから在庫化待ち (Z+Y) を引いた「いま出せる分」
      live: { sales30, free_stock: freeStock, shippable_free: shippable, pending_free: pendingFreeOf(z, y) },
      priority: priorityOf(sales30, shippable),
      active: activeMap.get(r.id) || [],
      estimate: (k && estimates.get(k)) || null,
      // 想定作業時間・必要保管箱 (Notion の計算式をそのまま。ボードでは状態ごとに合計する)
      plan_hours: planHours(r.qty, master.process_count),
      boxes: neededBoxes(r.qty, master.units_per_container, z ? z.stock : 0, z ? z.allocated : 0),
      boxes_calc: neededBoxesCalc(r.qty, master.units_per_container, z ? z.stock : 0, z ? z.allocated : 0),
      // Z ロケ (一時保管) の在庫。在庫ミラーは毎時更新で、画面は 60 秒ごとに取り直すので自動で新しくなる。
      // ⭐行が無い = その商品は Z に 0 個 (ミラーが取れている限り)。取れていなければ null (監修 B-8)
      ...locFields(z, loc, locKind, mirrorAt),
      media: mediaMap.get(r.id) || [],
      previous_photos: previousPhotosOf(k ? (prevPhotos.get(k) || []) : [], `t${r.id}`),
    };
  });
  return { cards, today };
}

/**
 * 「明日の計画」画面のデータ (職員だけが開く。要件 §W-3 / §AA)。
 *   candidates = まだ予定の無い未着手カード。**おすすめ順**に並べ、1 から順の `rank` を付ける
 *                (在庫が少ない → 入荷が古い → 大きい。大きさは画面に出さない)
 *   tomorrow   = 明日やる分。拠点ごとの内訳もつける
 *   carry_over = やり残し (今日より前の予定で、まだ終わっていない)。**自動では動かさない**
 *   recommend  = 上から順に足して 1 日の目安 (4〜6h) に届くまでの件数と時間。画面はここに区切り線を引く
 * 画面は 4〜6 時間を目安に選ぶ。超えても入れられる (ハードな上限にしない)
 */
export function buildPlan({ readOnly = false } = {}) {
  const today = jstToday();
  const tomorrow = jstTomorrow(today);
  const { cards } = buildTaskCards(listOpenTasks({}), { readOnly });
  const byWhen = (w) => cards.filter((c) => c.when === w).sort(comparePlanOrder);
  const tomorrowCards = byWhen('tomorrow');
  const facilities = listFacilities();
  const byFacility = facilities.map((f) => {
    const list = tomorrowCards.filter((c) => c.facility_code === f.code);
    return { code: f.code, name: f.name, ...sumPlanHours(list) };
  }).filter((x) => x.count > 0);
  // 予定の無い未着手だけを候補に出す。止まっているもの (資材不足・ラベル待ち) は札が外れるまで混ぜない (要件 §W)
  const candidates = cards.filter((c) => c.when == null && c.status === 'not_started' && !c.blocked)
    .sort(comparePlanOrder)
    // ⭐推奨の順位 (中原さん 2026-09-06「そのほうが作業しやすい」)。1 が「まずこれ」
    .map((c, i) => ({ ...c, rank: i + 1 }));
  const totals = sumPlanHours(tomorrowCards);
  const targetHours = { min: 4, max: 6 };
  return {
    today_ymd: today,
    tomorrow_ymd: tomorrow,
    candidates,
    recommend: recommendCut(candidates, totals.hours, targetHours.max),
    tomorrow: tomorrowCards,
    carry_over: byWhen('over'),
    totals,
    by_facility: byFacility,
    unassigned_count: tomorrowCards.filter((c) => !c.facility_code).length,
    facilities,
    // 目安 (画面のゲージの帯)。超えても入れられる
    target_hours: targetHours,
  };
}

/**
 * おすすめの区切り = 候補を上から足していって、1 日の目安 (max) に届くまでの件数。
 * すでに目安を超えていれば 0 件 (区切り線を引かない)。想定時間の分からないカードは 0 時間として数える
 * (足しても目安を超えないので、区切りより上に残る)。
 * ⭐その「時間不明」が何件混じったかも返す。0 時間で数えた分を「1 日の目安」と言い切らない (Codex R1 #4)
 */
export function recommendCut(candidates, plannedHours = 0, maxHours = 6) {
  const room = Math.round((maxHours - (Number(plannedHours) || 0)) * 10) / 10;
  if (!(room > 0)) return { count: 0, hours: 0, room: 0, unknown_hours_count: 0 };
  let count = 0;
  let hours = 0;
  let unknown = 0;
  for (const c of candidates) {
    const n = Number(c.plan_hours);
    const isKnown = c.plan_hours != null && Number.isFinite(n);
    const h = isKnown ? n : 0;
    if (count > 0 && hours + h > room) break;    // 1 件目は目安を超えていても入れる (何も勧めないと画面が無言になる)
    hours += h;
    count += 1;
    if (!isKnown) unknown += 1;
  }
  return { count, hours: Math.round(hours * 10) / 10, room, unknown_hours_count: unknown };
}

/** 商品画像がまだ無いカードの取り寄せを頼む (一覧・単票の共通処理。失敗しても表示は続ける) */
function queueMissingImages(cards) {
  const missingImg = [...new Set(cards.filter(c => c.product_code && !c.image_url).map(c => c.product_code))];
  if (missingImg.length === 0) return;
  try { queueEnsureImages(missingImg, 'いろは作業アプリ'); } catch (e) { console.warn('[iroha-work] 画像解決を飛ばしました:', e.message); }
}

/**
 * 1 枚だけ (下見・履歴の詳細)。終了したタスクも返す。無ければ null。
 * 一覧と違い、そのカードの**終わった作業** (work_history) も付ける — 詳細でしか使わないので 1 件ずつ引く
 */
export function buildTaskCard(id, { queueImages = true, readOnly = false } = {}) {
  const t = getTask(id);
  if (!t) return null;
  const card = buildTaskCards([t], { readOnly }).cards[0] || null;
  if (!card) return null;
  card.work_history = finishedSessionsOfTask(t.id);
  // ⭐下見・履歴 (読むだけ) では取り寄せない。開くだけで画像キューの DB が変わると
  //   「読むだけの画面では何も書かない」という境界が崩れる (Codex PR1 R7)
  if (queueImages) queueMissingImages([card]);
  return card;
}

/**
 * 一覧 (アプリ正本 = f_iroha_tasks)。
 * 終了 (closed) は含めない (履歴画面で見る — 中原さん 2026-09-03「完了が溜まる一方なのを何とかしたい」)
 */
export function buildTaskList({ facility = null, readOnly = false } = {}) {
  const rows = listOpenTasks({ facility });
  const { cards, today } = buildTaskCards(rows, { readOnly });

  // 並び: 「今日やる」だけ一覧の先頭に来て、そこから先は明日の計画・ボードとまったく同じ並び。
  // 画面ごとにものさしを変えない (§AA。以前は一覧だけ大きさと id を見ておらず順が食い違っていた — Codex R1 #2)
  cards.sort((a, b) => {
    if (a.today !== b.today) return a.today ? -1 : 1;
    return comparePlanOrder(a, b);
  });

  // 読むだけ (下見) では画像の取り寄せも起こさない — 開くだけで DB が変わらない (Codex PR1 R7 / R8)
  if (!readOnly) queueMissingImages(cards);

  // 上のゲージ用。明日やる分の件数と合計時間 (工程数の無いカードは 0 で足さず別に数える — 要件 §W-3)
  const tomorrowPlan = sumPlanHours(cards.filter((c) => c.when === 'tomorrow'));

  return {
    mode: 'app',
    tomorrow_plan: tomorrowPlan,
    today_ymd: today,
    cards,
    statuses: OPEN_STATUSES.map(s => ({ value: s, label: STATUS_LABEL[s] })),
    transitions: TRANSITIONS,
    // 止まっている理由 (案A)。button = 利用者が押すボタンの言い方 (「ラベルが足りない」)、label = 札の言い方 (「ラベル待ち」)
    blockReasons: BLOCK_REASONS.map(v => ({ value: v, label: BLOCK_LABEL[v], button: BLOCK_BUTTON[v] })),
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
  const c = neededBoxesCalc(qty, unitsPerContainer, zStock, zAllocated);
  if (!c) return null;
  return c.rest === 0 ? `${c.full}箱` : `${c.full}箱+${c.rest}`;
}
/**
 * 必要保管箱の中身。full = いっぱいになる箱、rest = 余り、boxes = **人が用意する箱の数** (余りがあれば +1)。
 * Notion の式 (上) は「1箱+80」= いっぱい 1 + 余り 80 と読むが、用意するのは 2 箱 (監修 PR-D 2026-09-05。表示は画面側 boxesText)
 */
export function neededBoxesCalc(qty, unitsPerContainer, zStock = 0, zAllocated = 0) {
  const per = Number(unitsPerContainer);
  if (!Number.isSafeInteger(per) || per <= 0) return null;
  const z = Number(zStock) || 0;
  const base = z > 0 ? z - (Number(zAllocated) || 0) : Number(qty);
  // 整数で数えられる範囲だけ (小数・桁あふれは箱数を保証できないので出さない — Codex FB R1)
  if (!Number.isSafeInteger(base) || base < 0) return null;
  const full = Math.floor(base / per);
  const rest = base % per;
  return { full, rest, per, base, boxes: full + (rest > 0 ? 1 : 0) };
}

/**
 * ロケ別の在庫。商品コードごとに 在庫数・引当数 を合計する (良品だけ。不良品は外に出せない)。
 *   Z = 一時保管 (手元にある)。Notion 時代の Z在庫数 / Z引当数 に当たる
 *   Y = 出荷禁止 (外部施設に出している分)。外に預けると Z→Y、戻ると Y→Z (中原さん)
 * 見分けは「ロケ または ブロック略称 がその文字ではじまる」
 */
/**
 * 大きさ (嵩) — 商品の**配送方法**で簡易的に見なす (中原さん 2026-09-04・要件 §W-1 / §AA)。
 * 定形外 ＜ ネコポス ＜ ゆうパケットポスト ＜ 50 サイズ ＜ 60 サイズ。
 * 大きい物から片づける理由 = 在庫化待ちの荷物が置き場スペースを取るので、大きい物を先に減らすと
 * 在庫化スペースに残っている商品を後から探しやすくなる。**体積の厳密さは要らない** (並び順にだけ使う)。
 * ⭐2026-09-06: この値は**画面に出さない** (並びの中だけで効かせる)。職員が手で登録する項目も廃止
 */
// ⚠上から順に見る。**大きいものを先に**判定する (「宅急便50サイズ」は 60 の規則に当たらないこと)
const SIZE_RULES = [
  // 発払い = サイズ指定のないヤマト宅急便。当社では 50 サイズに収まらないものに使う = いちばん大きい扱い
  // (梱包機振り分け apps/packing-dispatch の段階も 定形外1 < ネコポス2 < ゆうパケットパフ3 < 宅急便50=4 < 発払い5)
  { rank: 5, label: '60サイズ以上',       re: /60\s*サイズ|発払|宅急便コンパクト/ },
  { rank: 4, label: '50サイズ',           re: /50\s*サイズ|宅急便/ },
  { rank: 3, label: 'ゆうパケットポスト', re: /ゆうパケット|ゆうパック|クリックポスト|レターパック/ },
  { rank: 2, label: 'ネコポス',           re: /ネコポス|メール便/ },
  { rank: 1, label: '定形外',             re: /定形外|定形/ },
];
/** 配送方法の文字列 → { rank, label }。分からなければ null (並びは最後・画面は「大きさ 不明」) */
export function sizeOfShipping(text) {
  const t = String(text || '').trim();
  if (!t) return null;
  for (const r of SIZE_RULES) if (r.re.test(t)) return { rank: r.rank, label: r.label };
  return null;
}

/**
 * 商品コード → 大きさ。**配送方法だけで決める** (中原さん 2026-09-06 — §AA)。
 * 分からなければ「不明」= 並びは最後。職員がその場で登録する「大きさ」の項目は廃止した
 * (f_iroha_work_master.size_class の列と既存の値は残してあるが、もう読まない)
 */
function sizeMapByCode(codeKeys) {
  const db = getDB();
  const map = new Map();
  const keys = [...new Set((codeKeys || []).filter(Boolean))];
  if (keys.length === 0) return map;
  if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'mirror_products'").get()) return map;
  for (let i = 0; i < keys.length; i += 400) {
    const chunk = keys.slice(i, i + 400);
    const rows = db.prepare(`SELECT LOWER(TRIM(商品コード)) AS k, 配送方法 AS m FROM mirror_products
      WHERE LOWER(TRIM(商品コード)) IN (${chunk.map(() => '?').join(',')})`).all(...chunk);
    for (const r of rows) { const s = r.k ? sizeOfShipping(r.m) : null; if (s) map.set(r.k, s); }
  }
  return map;
}

/**
 * ⭐入荷実績 (🌱「はじめての商品」の判定。中原さん 2026-09-06 — §AA)。
 * 「はじめての商品」= **過去に入荷したことがない商品**。作業実績 (実測時間) の有無ではない。
 * 実績の証拠は 2 つ。どちらかに今回より古い入荷があれば「はじめて」ではない:
 *   ① 過去のカード (f_iroha_tasks。取り込んだ Notion の分と、アプリで作った分。終了したカードも数える)
 *   ② ロジザード在庫の入荷日 (カードが残っていない古い入荷を拾う)
 * どちらの材料も無ければ null = **分からない** (札を出さない)。0 件を「はじめて」と読み替えない。
 * ⭐材料の有無は**商品ごと**に見る。よその商品に実績があっても、この商品の材料が 0 件なら分からない
 *   (日付が読めない行しか無い場合も材料なし扱い。読めない = 入荷が無かった証拠にはならない — Codex R1 #1)
 * seed = いま画面に出すカード自身の入庫日。Notion 正本ではカードが f_iroha_tasks に無いので、
 *   これが無いと「はじめての商品」が判定材料 0 件 = 分からない になってしまう (Codex R2)
 */
function arrivalHistory(codeKeys, seed = []) {
  const db = getDB();
  const first = new Map();       // 商品コード → いちばん古い入荷日 (YYYY-MM-DD)
  const put = (k, d) => {
    const ymd = normalizeYmd(d);
    if (!k || !ymd) return;      // 日付が読めない行は材料に数えない
    const cur = first.get(k);
    if (!cur || ymd < cur) first.set(k, ymd);
  };
  const keys = [...new Set((codeKeys || []).filter(Boolean))];
  for (const s of seed || []) put(s.k, s.ymd);     // 画面に出すカード自身の入荷 (アプリ正本では f_iroha_tasks から重ねて入る)
  const has = (t) => !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(t);
  if (keys.length > 0 && has('f_iroha_tasks')) {
    // カードは多くて数千行なので商品コードで絞らずまとめて集計する (1 回の GROUP BY)。
    // ⭐取消 (close_reason = 'cancelled') は入荷そのものが取り下げられたカードなので実績に数えない。
    //   「在庫化対象外」は**荷物は届いている**ので数える
    const rows = db.prepare(`SELECT LOWER(TRIM(product_code)) AS k, MIN(arrival_date) AS a FROM f_iroha_tasks
      WHERE product_code IS NOT NULL AND arrival_date IS NOT NULL AND TRIM(arrival_date) <> ''
        AND (close_reason IS NULL OR close_reason <> 'cancelled') GROUP BY 1`).all();
    for (const r of rows) put(r.k, r.a);
  }
  if (keys.length > 0 && has('mirror_logizard_stock')
      && db.prepare('PRAGMA table_info(mirror_logizard_stock)').all().some((c) => c.name === '入荷日')) {
    // 在庫ミラーは数万行。画面に出すカードの商品だけ引く (正規化キーの索引を使う)
    for (let i = 0; i < keys.length; i += 400) {
      const chunk = keys.slice(i, i + 400);
      const rows = db.prepare(`SELECT LOWER(TRIM(商品ID)) AS k, MIN(入荷日) AS a FROM mirror_logizard_stock
        WHERE LOWER(TRIM(商品ID)) IN (${chunk.map(() => '?').join(',')})
          AND 入荷日 IS NOT NULL AND TRIM(入荷日) <> '' GROUP BY 1`).all(...chunk);
      for (const r of rows) put(r.k, r.a);
    }
  }
  return {
    /** @returns true = はじめての商品 / false = 入荷実績あり / null = 分からない (札を出さない) */
    firstTime(codeKey, arrival) {
      if (!codeKey) return null;
      const f = first.get(codeKey);
      if (!f) return null;                       // この商品の判定材料が 0 件 = 分からない (札を出さない)
      const mine = normalizeYmd(arrival);
      if (!mine) return false;                   // 今回の入庫日が分からない: 記録がある以上「はじめて」とは言えない
      return !(f < mine);                        // 今回より古い入荷があれば「はじめて」ではない
    },
  };
}
/** 'YYYY/M/D' '2026年9月3日' なども 'YYYY-MM-DD' に揃える (取込元で書き方が違っても比べられるように) */
function normalizeYmd(v) {
  const m = /(\d{4})\D{0,2}(\d{1,2})\D{0,2}(\d{1,2})/.exec(String(v || ''));
  if (!m) return null;
  return `${m[1]}-${String(m[2]).padStart(2, '0')}-${String(m[3]).padStart(2, '0')}`;
}

/**
 * 「いつやるか」— planned_date (実日付) から決める (要件 §W-2)。日付は JST。
 * 深夜の書き換えバッチは作らない。日付が変わるだけで「明日」が「今日」になる
 */
export function whenOf(plannedDate, today = jstToday(), tomorrow = null) {
  if (!plannedDate) return null;
  const d = String(plannedDate);
  if (d === today) return 'today';
  if (d === (tomorrow || jstTomorrow(today))) return 'tomorrow';
  if (d > today) return 'later';
  return 'over';   // 今日より前で、まだ終わっていない = やり残し
}
/** JST の明日 (YYYY-MM-DD) */
export function jstTomorrow(today = jstToday()) {
  return new Date(Date.parse(`${today}T00:00:00Z`) + 86400000).toISOString().slice(0, 10);
}

/**
 * 「明日はどれをやるか」を決めるときの並び = **おすすめの順** (中原さん 2026-09-04 / 2026-09-06・§W-3 / §AA)。
 *   ① 在庫が少ない順 = 出荷できる在庫が何日もつか (急ぎが先頭。新商品は 10 日とみなす)
 *   ② 入荷日が古い順
 *   ③ 大きさが大きい順 (置き場が空くので。配送方法で見なす・画面には出さない)
 * 同点は id。**同じ入力なら必ず同じ順**になる (画面とサーバーで並びがぶれない)
 */
export function comparePlanOrder(a, b) {
  const p = comparePriority(a, b);
  if (p !== 0) return p;
  const aa = a.arrival || '9999-99-99';
  const ab = b.arrival || '9999-99-99';
  if (aa !== ab) return aa < ab ? -1 : 1;
  const sa = a.size_rank ?? -1;
  const sb = b.size_rank ?? -1;
  if (sa !== sb) return sb - sa;       // 大きいほど先
  return compareId(a.id, b.id);
}

/**
 * 最後の決着に使う id。アプリ正本は数値 (task.id)、Notion 正本は page_id の文字列なので
 * 両方を同じ規則で比べる (数どうしは数として、そうでなければ文字列として)。Codex R1 #2
 */
function compareId(x, y) {
  // ⭐比べ方が相手によって変わると順序が循環する (10 > '2' > '11x' > 10)。まず**種類**で分けてから、
  //   同じ種類どうしだけ比べる。0 = 数として読める / 1 = それ以外の文字列 / 2 = 無い (Codex R2)
  const num = (v) => (typeof v === 'number' && Number.isFinite(v) ? v
    : (typeof v === 'string' && /^\d+$/.test(v.trim()) ? Number(v) : null));
  const nx = num(x);
  const ny = num(y);
  const gx = x == null ? 2 : (nx != null ? 0 : 1);
  const gy = y == null ? 2 : (ny != null ? 0 : 1);
  if (gx !== gy) return gx - gy;
  if (gx === 2) return 0;
  if (gx === 0 && nx !== ny) return nx - ny;
  const sx = String(x);                        // '2' と '02' のように数が同じでも元の文字列で決着させる
  const sy = String(y);
  return sx < sy ? -1 : sx > sy ? 1 : 0;
}

/** 明日やる分の合計 (ゲージ用)。工程数の無いカードは 0 で足さず「時間不明」として数える */
export function sumPlanHours(cards) {
  let hours = 0;
  let unknown = 0;
  for (const c of cards) {
    if (c.plan_hours == null) unknown += 1;
    else hours += c.plan_hours;
  }
  return { hours: Math.round(hours * 10) / 10, count: cards.length, unknown_hours_count: unknown };
}

/**
 * 商品コード → ロケの種類ごとの在庫 (良品だけ)。**1 回の読み取りでまとめて数える**:
 *   z     = 在庫化待ち (手元。ロケ か ブロック略称 が Z ではじまる)
 *   y     = 外部施設に出している分 (同 Y)
 *   total = 全ロケの合計
 * ⭐ここを 1 本のクエリにしているのは、「全ロケの合計」と「Z/Y」が**別の時点の値**にならないため。
 *   出荷できる在庫 = total − z − y なので、片方だけ新しいと在庫日数がその瞬間だけ大きく狂う
 *   (enrichContext の 5 分キャッシュを混ぜていたときの問題)。
 *   Z と Y の両方に当たる行は Z として 1 回だけ数える (二重に引かない)
 * 画面に出すカードの商品だけを数える (在庫ミラーは数千〜数万行。一覧を開くたびに全表を舐めない — Codex FB R1)
 */
function stockByCode(codeKeys) {
  const db = getDB();
  const map = new Map();
  const keys = [...new Set((codeKeys || []).filter(Boolean))];
  if (keys.length === 0) return map;
  if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'mirror_logizard_stock'").get()) return map;
  const zero = () => ({ stock: 0, allocated: 0, captured: null });
  for (let i = 0; i < keys.length; i += 400) {
    const chunk = keys.slice(i, i + 400);
    const rows = db.prepare(`SELECT LOWER(TRIM(商品ID)) AS k,
        CASE WHEN ロケ LIKE 'Z%' OR ブロック略称 LIKE 'Z%' THEN 'z'
             WHEN ロケ LIKE 'Y%' OR ブロック略称 LIKE 'Y%' THEN 'y'
             ELSE 'other' END AS kind,
        SUM(在庫数) AS zaiko, SUM(引当数) AS hikiate, MAX(captured_at) AS captured
      FROM mirror_logizard_stock
      WHERE LOWER(TRIM(商品ID)) IN (${chunk.map(() => '?').join(',')}) AND 品質区分名 = '良品'
      GROUP BY 1, 2`).all(...chunk);
    for (const r of rows) {
      if (!r.k) continue;
      const cur = map.get(r.k) || { z: null, y: null, total: zero() };
      const part = { stock: Number(r.zaiko) || 0, allocated: Number(r.hikiate) || 0, captured: r.captured || null };
      if (r.kind === 'z' || r.kind === 'y') cur[r.kind] = part;
      cur.total.stock += part.stock;
      cur.total.allocated += part.allocated;
      if (!cur.total.captured || (part.captured && part.captured > cur.total.captured)) cur.total.captured = part.captured;
      map.set(r.k, cur);
    }
  }
  return map;
}
/** stockByCode の 1 商品ぶん (行が無い商品でも同じ形で扱えるように) */
const NO_STOCK = { z: null, y: null, total: null };

/**
 * 在庫ミラーが「取れている」か = 1 行でもあれば、その取得時刻 (MAX(captured_at))。無ければ null。
 * ⭐その商品の Z ロケ行が無い = 「取れていない」ではなく「Z に在庫 0」(監修 B-8: 0 なら P ロケから持ってくる、と現場の判断が違う)。
 *   本当に取れていないのは、ミラーの表が無い・空のときだけ
 */
function mirrorCapturedAt() {
  const db = getDB();
  if (!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'mirror_logizard_stock'").get()) return null;
  const r = db.prepare('SELECT MAX(captured_at) AS captured FROM mirror_logizard_stock').get();
  return (r && r.captured) || null;
}
/** ロケ別の在庫の表示値。行があればその値、ミラーが取れていれば 0 (時刻はミラーの最新)、取れていなければ null (「まだ取れていません」) */
function locView(loc, mirrorAt) {
  if (loc) return { stock: loc.stock, allocated: loc.allocated, free: loc.stock - loc.allocated, at: loc.captured };
  if (mirrorAt) return { stock: 0, allocated: 0, free: 0, at: mirrorAt };
  return { stock: null, allocated: null, free: null, at: null };
}
/** カードに乗せる Z ロケ・表示ロケの在庫 (z_* は必要保管箱の式が使う Z、loc_* は拠点で Z か Y に変わる表示用) */
function locFields(z, loc, locKind, mirrorAt) {
  const zv = locView(z, mirrorAt);
  const lv = locView(loc, mirrorAt);
  return { z_stock: zv.stock, z_allocated: zv.allocated, z_free: zv.free, z_at: zv.at,
    loc_kind: locKind, loc_stock: lv.stock, loc_allocated: lv.allocated, loc_free: lv.free, loc_at: lv.at };
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
