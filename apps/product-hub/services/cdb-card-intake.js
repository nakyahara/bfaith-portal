/**
 * cdb-card-intake.js — Company DB の「新商品の登録」の知らせ (ops.product_hub_outbox) から出品カード (product_drafts) を作る
 * (Company DB構想 14 §9 H7・§10 契約 v3 Medium 1・§11 の追加の要望。Postgres 側は lib/product-hub-outbox.mjs)
 *
 * 1 つの知らせ = SQLite の 1 つの取引で:
 *   product_drafts (ne_code = 商品コード・名前・売価・公式ページ URL・ASIN・Amazon URL・cdb_sku_id) + 参考 URL (draft_reference_urls) +
 *   セット商品を作るか (draft_set_decisions) + Yahoo!向け追記 (draft_yahoo) + 楽天の配送方法 (draft_rakuten.shipping_method_group) + 取り込みの記録 (ph_cdb_card_events)
 * 冪等:
 *   - 同じ知らせをもう一度 (event_id が記録にある・衝突でない) = 記録の答えを返すだけ
 *   - cdb_sku_id のカードがもうある = 「結んであった」(linked)。増やさない
 *   - 同じ商品コードのカード (cdb_sku_id なし・別の SKU) がもうある = 衝突 (conflict)。増やさない・直さない・記録する (人が決める)
 *     人が決める = 「既存のカードをこの商品に結ぶ」(linkCdbCardToExisting・PR #1566 R1 M6) か、古いカードを片付けて「もう一度」
 * 🚨 切替の段階が frozen 以降に、このファイル (と ⑤-2a のコード) でカードを作る道はこの取り込みだけ (知らせは new_open の登録でしか書かれない)。
 *    product-hub の古い作成の道 (/api/drafts・NE の一括登録・自動取込) を frozen から閉じるのは ⑤-3
 * 発送方法: Company DB の送料コードの方法 (送料の表の小分類区分名称 = NE の配送方法と同じ名前) を ph_shipping_method_map で楽天の配送方法グループへ。
 *   対応が無い = 楽天の配送方法は空のまま・shipping_status = unmapped (ボードのカードに「要確認」)。🚨 名前が似ているだけで推し量らない
 * Yahoo!: 画面 D で配送方法を入れたときだけ delivery_label を入れる (楽天の配送方法と違えば「ヤフーだけ別」= shipping_override 1)。
 *   税率は入れない (Company DB の税率を見せる = 14 §5 #6・⑤-3)
 * 🚨 Drive の画像フォルダの自動作成など外への呼び出しはしない (知らせの取り込みは何度でも流せる形にする)
 */
import { getDB, logEvent, upsertDraftYahoo } from '../db.js';
import { ALL_SHIPPING_METHOD_GROUPS } from '../lib/shipping-groups.js';
import { SET_DECISION_REASONS } from '../lib/set-decision.js';

/** 取り込める payload の版 (Company DB の lib/product-hub-outbox.mjs の CARD_SCHEMA_VERSION) */
export const CARD_SCHEMAS = Object.freeze(['ph-card-v1']);
const ACTOR = 'auto:cdb-register';

/** 送料コードの方法 → 楽天の配送方法グループ (保存した対応だけ・推し量らない) */
export function mapCdbShipping(db, shipping) {
  const method = String(shipping?.method ?? '').trim();
  const code = shipping?.code == null ? null : String(shipping.code);
  if (!method) return { status: 'unmapped', group: null, code, method: null, label: null };
  const row = db.prepare('SELECT rakuten_group FROM ph_shipping_method_map WHERE ne_label = ?').get(method);
  const g = row && row.rakuten_group ? String(row.rakuten_group).trim() : '';
  if (g && ALL_SHIPPING_METHOD_GROUPS[g]) return { status: 'mapped', group: g, code, method, label: ALL_SHIPPING_METHOD_GROUPS[g] };
  return { status: 'unmapped', group: null, code, method, label: null };
}

function fail(message) { return Object.assign(new Error(message), { code: 'CDB_CARD_INVALID' }); }

/** payload の最低限の形 (Company DB で確かめ済みの値だが、SQLite に入れる前にもう一度) */
function checkPayload(event) {
  if (!CARD_SCHEMAS.includes(event?.schema_version)) throw fail(`知らない payload の版: ${event?.schema_version}`);
  const p = event.payload || {};
  if (p.schema !== event.schema_version) throw fail('payload の版が知らせの版と違う');
  const skuId = Number(p.cdb_sku_id);
  if (!Number.isSafeInteger(skuId) || skuId <= 0) throw fail(`cdb_sku_id が不正: ${p.cdb_sku_id}`);
  const code = String(p.code ?? '').trim();
  if (!/^[a-z0-9_-]{1,30}$/.test(code)) throw fail(`商品コードが不正: ${p.code}`);
  const name = String(p.name ?? '').trim();
  if (!name) throw fail('名前が空');
  const eventId = String(event.event_id ?? '');
  if (!/^[0-9a-f-]{36}$/.test(eventId)) throw fail(`event_id が不正: ${event.event_id}`);
  return { p, skuId, code, name, eventId };
}

/**
 * 知らせを 1 つ取り込む (同期・better-sqlite3)。戻り値 { outcome: created | linked | conflict, draft_id, conflict_draft_id?, shipping_status?, message?, replayed? }
 * 失敗 (SQLite の誤り・不正な payload) は throw = Postgres 側で failed (後でもう一度)
 */
export function applyCdbCardEvent(event, { db = getDB() } = {}) {
  const { p, skuId, code, name, eventId } = checkPayload(event);
  return db.transaction(() => {
    const seen = db.prepare('SELECT * FROM ph_cdb_card_events WHERE event_id = ?').get(eventId);
    if (seen && seen.outcome !== 'conflict') {
      return { outcome: seen.outcome, draft_id: seen.draft_id, shipping_status: seen.shipping_status, replayed: true };
    }
    const record = (outcome, draftId, conflictId, ship) => db.prepare(`
      INSERT INTO ph_cdb_card_events (event_id, cdb_sku_id, ne_code, outcome, draft_id, conflict_draft_id, shipping_status, shipping_code, shipping_method)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET outcome = excluded.outcome, draft_id = excluded.draft_id, conflict_draft_id = excluded.conflict_draft_id,
        shipping_status = excluded.shipping_status, shipping_code = excluded.shipping_code, shipping_method = excluded.shipping_method,
        applied_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(eventId, skuId, code, outcome, draftId, conflictId, ship?.status ?? null, ship?.code ?? null, ship?.method ?? null);
    // もう結んであるカード (同じ SKU)
    const linked = db.prepare('SELECT id FROM product_drafts WHERE cdb_sku_id = ?').get(skuId);
    if (linked) {
      record('linked', linked.id, null, null);
      return { outcome: 'linked', draft_id: linked.id };
    }
    // 同じ商品コードのカード (別に作られていた) = 増やさない・記録する
    const same = db.prepare('SELECT id, cdb_sku_id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(code);
    if (same) {
      record('conflict', null, same.id, null);
      logEvent(db, same.id, 'cdb_card_conflict', `Company DB の新商品 ${code} (SKU ${skuId}) と同じ商品コードのカード`, ACTOR);
      return { outcome: 'conflict', draft_id: null, conflict_draft_id: same.id,
        message: `同じ商品コード ${code} のカード (#${same.id}) が product-hub にもうあります。増やしていません (どちらを使うか決めてください)` };
    }
    const ship = mapCdbShipping(db, p.shipping);
    const price = Number.isSafeInteger(Number(p.price)) ? Number(p.price) : null;
    const info = db.prepare(`
      INSERT INTO product_drafts (ne_code, name, official_url, price, asin, amazon_url, created_by, cdb_sku_id)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(code, name, p.official_url || null, price, p.asin || null, p.amazon_url || null, p.created_by || ACTOR, skuId);
    const draftId = Number(info.lastInsertRowid);
    const refs = Array.isArray(p.reference_urls) ? p.reference_urls.filter((u) => typeof u === 'string' && /^https?:\/\/\S+$/i.test(u)) : [];
    refs.forEach((u, i) => db.prepare('INSERT INTO draft_reference_urls (draft_id, url, sort) VALUES (?, ?, ?)').run(draftId, u, i));
    // セット商品を作るか: 作らない = none (理由つき) / 保留 = hold / 作る = hold + 「作る予定」 (セットは product-hub の「セット商品を作る」で作る。
    // 作っていないのに「セットを作成」を記録すると⑤が閉じる = product-hub の set-decision API と同じく create は記録しない)
    const sd = p.set_decision;
    if (sd && p.kind === 'single') {
      if (sd.decision === 'none' && SET_DECISION_REASONS[sd.reason_code]) {
        db.prepare('INSERT INTO draft_set_decisions (draft_id, decision, reason_code, reason_text, decided_by) VALUES (?, ?, ?, ?, ?)')
          .run(draftId, 'none', sd.reason_code, sd.reason_text || null, p.created_by || ACTOR);
      } else if (sd.decision === 'hold' || sd.decision === 'create') {
        const text = sd.decision === 'create' ? `作る予定 (新商品の登録で「作る」)${sd.reason_text ? `: ${sd.reason_text}` : ''}` : (sd.reason_text || null);
        db.prepare('INSERT INTO draft_set_decisions (draft_id, decision, reason_code, reason_text, decided_by) VALUES (?, ?, NULL, ?, ?)')
          .run(draftId, 'hold', text, p.created_by || ACTOR);
      }
    }
    // 楽天の配送方法 (対応があるときだけ)
    if (ship.group) db.prepare('INSERT INTO draft_rakuten (draft_id, shipping_method_group) VALUES (?, ?)').run(draftId, ship.group);
    // Yahoo!向け追記
    const y = p.yahoo;
    if (y) {
      const delivery = y.delivery_label ? String(y.delivery_label) : null;
      upsertDraftYahoo(db, draftId, {
        yahoo_price: y.price ?? null,
        yahoo_price_sagawa: y.price_sagawa ?? null,
        delivery_label: delivery,
        shipping_override: delivery && delivery !== ship.label ? 1 : 0,
        yahoo_category_id: y.category_id ?? null,
        yahoo_path: y.path ?? null,
      });
    }
    record('created', draftId, null, ship);
    logEvent(db, draftId, 'created_from_cdb', `Company DB の新商品の登録 (SKU ${skuId}・${p.kind === 'set' ? 'セット' : '単品'})`, p.created_by || ACTOR);
    if (ship.status === 'unmapped') {
      logEvent(db, draftId, 'cdb_shipping_unmapped', `送料コード ${ship.code ?? '—'} (${ship.method ?? '方法なし'}) に楽天の配送方法の対応がありません (要確認)`, ACTOR);
    }
    return { outcome: 'created', draft_id: draftId, shipping_status: ship.status };
  })();
}

/**
 * 衝突を人が解く = 既存のカード (同じ商品コード) をこの Company DB の SKU に結ぶ (SQLite の 1 つの取引・PR #1566 R1 M6)。
 * 取引の中で衝突をもう一度確かめる: カードがある・商品コードが同じ・ほかの SKU に結ばれていない・この SKU がほかのカードに結ばれていない。
 * もうこの SKU に結ばれている = 何もしない (冪等・already)。確かめに落ちたら throw (Company DB の知らせは conflict のまま)。
 * カードの値 (売価・URL など) は変えない (結ぶだけ)。戻り値 { outcome: 'linked', draft_id, already }
 */
export function linkCdbCardToExisting(event, { draftId, actor = ACTOR, db = getDB() } = {}) {
  const { skuId, code, eventId } = checkPayload(event);
  const id = Number(draftId);
  if (!Number.isSafeInteger(id) || id <= 0) throw fail(`結ぶカードの番号が不正: ${draftId}`);
  return db.transaction(() => {
    const d = db.prepare('SELECT id, ne_code, cdb_sku_id FROM product_drafts WHERE id = ?').get(id);
    if (!d) throw fail(`カード #${id} がもうありません`);
    if (String(d.ne_code || '').trim().toLowerCase() !== code) throw fail(`カード #${id} の商品コード (${d.ne_code}) が ${code} と違います`);
    if (d.cdb_sku_id != null && Number(d.cdb_sku_id) !== skuId) throw fail(`カード #${id} はもう別の商品 (SKU ${d.cdb_sku_id}) に結ばれています`);
    const other = db.prepare('SELECT id FROM product_drafts WHERE cdb_sku_id = ? AND id <> ?').get(skuId, id);
    if (other) throw fail(`この商品はもう別のカード #${other.id} に結ばれています`);
    const already = d.cdb_sku_id != null;
    if (!already) {
      const u = db.prepare(`UPDATE product_drafts SET cdb_sku_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ? AND cdb_sku_id IS NULL`).run(skuId, id);
      if (u.changes !== 1) throw fail(`カード #${id} をちょうどほかの処理が変えました`);
      logEvent(db, id, 'cdb_card_linked', `Company DB の新商品 ${code} (SKU ${skuId}) に結んだ (人が決めた)`, actor);
    }
    db.prepare(`
      INSERT INTO ph_cdb_card_events (event_id, cdb_sku_id, ne_code, outcome, draft_id, conflict_draft_id)
      VALUES (?, ?, ?, 'linked', ?, NULL)
      ON CONFLICT(event_id) DO UPDATE SET outcome = 'linked', draft_id = excluded.draft_id, conflict_draft_id = NULL,
        applied_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(eventId, skuId, code, id);
    return { outcome: 'linked', draft_id: id, already };
  })();
}

/**
 * ボードのカードに「発送方法 要確認」を出す draft の id (取り込みで対応が無かった・まだ楽天の配送方法が空のもの)。
 * 表が無い (初期化の前) = 空
 */
export function cdbShippingCheckIds(db = getDB()) {
  try {
    return new Set(db.prepare(`
      SELECT e.draft_id AS id FROM ph_cdb_card_events e
      LEFT JOIN draft_rakuten r ON r.draft_id = e.draft_id
      WHERE e.outcome = 'created' AND e.shipping_status = 'unmapped' AND e.draft_id IS NOT NULL
        AND COALESCE(TRIM(r.shipping_method_group), '') = ''
    `).all().map((r) => r.id));
  } catch {
    return new Set();
  }
}
