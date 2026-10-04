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
 *   - 同じ商品コード (LOWER(TRIM(ne_code))) のカードが 2 枚以上 = どれか決められない衝突 (ambiguous)。結ぶ道も出さない (片付けてから「もう一度」)
 *   - 前に作った・結んだ答え (記録・cdb_sku_id のカード) を使い回すのも、同じコードのカードがそのカード 1 枚だけのときだけ。
 *     SQLite で作った後に Postgres を done にできず、その間に同じコードのカードが増えた = 使い回さず ambiguous (知らせを done にしない・#1566 Codex R3 Medium 2)
 *     🚨 product-hub の正規化した ne_code の一意 (idx_product_drafts_ne_norm) は、前からの重なりがあると張れない (db.js) = 一意に頼らず、
 *        取引の中で同じコードのカードを全部数える (PR #1566 Codex R2 Medium)
 * 🚨 切替の段階が frozen 以降に、このファイル (と ⑤-2a のコード) でカードを作る道はこの取り込みだけ (知らせは new_open の登録でしか書かれない)。
 *    product-hub の古い作成の道 (/api/drafts・NE の一括登録・自動取込) を frozen から閉じるのは ⑤-3
 * 発送方法: Company DB の送料コードの方法 (送料の表の小分類区分名称 = NE の配送方法と同じ名前) を ph_shipping_method_map で楽天の配送方法グループへ。
 *   対応が無い = 楽天の配送方法は空のまま・shipping_status = unmapped (ボードのカードに「要確認」)。🚨 名前が似ているだけで推し量らない
 * Yahoo!: 画面 D で配送方法を入れたときだけ delivery_label を入れる (楽天の配送方法と違えば「ヤフーだけ別」= shipping_override 1)。
 *   税率は入れない (Company DB の税率を見せる = 14 §5 #6・⑤-3)
 * 🚨 Drive の画像フォルダの自動作成など外への呼び出しはしない (知らせの取り込みは何度でも流せる形にする)
 */
import { getDB, logEvent, upsertDraftYahoo } from '../db.js';
import { SHIPPING_METHOD_GROUPS } from '../lib/shipping-groups.js';
import { SET_DECISION_REASONS } from '../lib/set-decision.js';

/** 取り込める payload の版 (Company DB の lib/product-hub-outbox.mjs の CARD_SCHEMA_VERSION) */
export const CARD_SCHEMAS = Object.freeze(['ph-card-v1']);
const ACTOR = 'auto:cdb-register';

/**
 * 送料コードの方法 → 楽天の配送方法グループ (保存した対応だけ・推し量らない)。
 * 🚨 対応先は画面で選べる配送方法 (SHIPPING_METHOD_GROUPS = 「現在使用不可」を除く) だけ。使えないグループへの対応 = 要確認 (仮レビュー L4)。
 *    送料の表の小分類区分名称 と ph_shipping_method_map.ne_label (mirror_products.配送方法) が同じ名前か = 本番の値で数える
 *    (apps/product-hub/scripts/shipping-map-report.mjs・読むだけ)
 */
export function mapCdbShipping(db, shipping) {
  const method = String(shipping?.method ?? '').trim();
  const code = shipping?.code == null ? null : String(shipping.code);
  if (!method) return { status: 'unmapped', group: null, code, method: null, label: null };
  const row = db.prepare('SELECT rakuten_group FROM ph_shipping_method_map WHERE ne_label = ?').get(method);
  const g = row && row.rakuten_group ? String(row.rakuten_group).trim() : '';
  if (g && SHIPPING_METHOD_GROUPS[g]) return { status: 'mapped', group: g, code, method, label: SHIPPING_METHOD_GROUPS[g] };
  return { status: 'unmapped', group: null, code, method, label: null };
}

function fail(message) { return Object.assign(new Error(message), { code: 'CDB_CARD_INVALID' }); }

/**
 * payload の最低限の形 (Company DB で確かめ済みの値だが、SQLite に入れる前にもう一度)。
 * SKU = 知らせの行の sku_id (event.sku_id = Company DB が振った番号・lib/product-hub-outbox.mjs の claim が返す)。payload には SKU の番号を入れない (0052)
 */
function checkPayload(event) {
  if (!CARD_SCHEMAS.includes(event?.schema_version)) throw fail(`知らない payload の版: ${event?.schema_version}`);
  const p = event.payload || {};
  if (p.schema !== event.schema_version) throw fail('payload の版が知らせの版と違う');
  const skuId = Number(event.sku_id);
  if (!/^[1-9][0-9]{0,15}$/.test(String(event.sku_id ?? '')) || !Number.isSafeInteger(skuId)) throw fail(`知らせの SKU の番号が不正: ${event.sku_id}`);
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
    // 同じ商品コードのカードを最初に全部数える (一意の index が無い DB でも 2 枚以上を 1 枚と見ない。前の答えを使い回す前にも見る)
    const ids = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ? ORDER BY id').all(code).map((x) => x.id);
    const seen = db.prepare('SELECT * FROM ph_cdb_card_events WHERE event_id = ?').get(eventId);
    const record = (outcome, draftId, conflictId, ship) => db.prepare(`
      INSERT INTO ph_cdb_card_events (event_id, cdb_sku_id, ne_code, outcome, draft_id, conflict_draft_id, shipping_status, shipping_code, shipping_method)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET outcome = excluded.outcome, draft_id = excluded.draft_id, conflict_draft_id = excluded.conflict_draft_id,
        shipping_status = excluded.shipping_status, shipping_code = excluded.shipping_code, shipping_method = excluded.shipping_method,
        applied_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(eventId, skuId, code, outcome, draftId, conflictId, ship?.status ?? null, ship?.code ?? null, ship?.method ?? null);
    /** どれか決められない衝突 (知らせは done にしない = 人が 1 枚に片付けてから「もう一度」)。mine = この知らせ / SKU のカード (同じコードの中に無ければ足して見せる) */
    const ambiguous = (mine = null) => {
      const show = [...new Set([...ids, ...(mine == null ? [] : [mine])])].sort((a, b) => a - b);
      record('conflict', null, null, null);
      for (const id of show) logEvent(db, id, 'cdb_card_conflict', `Company DB の新商品 ${code} (SKU ${skuId}) と同じ商品コードのカードが ${ids.length} 枚 (どれか決められない)`, ACTOR);
      return { outcome: 'conflict', draft_id: null, conflict_draft_id: null, conflict_draft_ids: show, ambiguous: true,
        message: `同じ商品コード ${code} のカードが product-hub に ${ids.length} 枚 (${ids.map((x) => '#' + x).join('・') || 'なし'}${mine != null && !ids.includes(mine) ? `・この商品のカード #${mine} はコードが違う` : ''}) あります。どれに結ぶか決められないので、増やしても結んでもいません (product-hub で 1 枚に片付けてから「もう一度作る」)` };
    };
    // 前に取り込んだ知らせ (作った・結んだ) = そのカードが同じコードのただ 1 枚のときだけ同じ答え
    if (seen && seen.outcome !== 'conflict') {
      if (ids.length === 1 && ids[0] === seen.draft_id) {
        return { outcome: seen.outcome, draft_id: seen.draft_id, shipping_status: seen.shipping_status, replayed: true };
      }
      return ambiguous(seen.draft_id);
    }
    // もう結んであるカード (同じ SKU) = そのカードが同じコードのただ 1 枚のときだけ「結んであった」
    const linked = db.prepare('SELECT id FROM product_drafts WHERE cdb_sku_id = ?').get(skuId);
    if (linked) {
      if (ids.length === 1 && ids[0] === linked.id) {
        record('linked', linked.id, null, null);
        return { outcome: 'linked', draft_id: linked.id };
      }
      return ambiguous(linked.id);
    }
    // 同じ商品コードのカード (別に作られていた) = 増やさない・記録する
    if (ids.length > 1) return ambiguous();
    if (ids.length === 1) {
      record('conflict', null, ids[0], null);
      logEvent(db, ids[0], 'cdb_card_conflict', `Company DB の新商品 ${code} (SKU ${skuId}) と同じ商品コードのカード`, ACTOR);
      return { outcome: 'conflict', draft_id: null, conflict_draft_id: ids[0],
        message: `同じ商品コード ${code} のカード (#${ids[0]}) が product-hub にもうあります。増やしていません (どちらを使うか決めてください)` };
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
    // セットの構成品 (仮レビュー L3)。Company DB の構成の依頼と同じ並び・数量。product-hub のセットの派生 (parent_draft_id) にはしない = 工程は単品と同じ
    if (p.kind === 'set') writeSetMembers(db, draftId, p.components);
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

/** セットの構成品をカードに (同じ品は 1 行・並び = 入れた順)。前からある行は消さない (INSERT OR IGNORE) */
function writeSetMembers(db, draftId, components) {
  const list = Array.isArray(components) ? components : [];
  list.forEach((c, i) => {
    const code = String(c?.code ?? '').trim().toLowerCase();
    const qty = Number(c?.qty);
    if (!code || !Number.isInteger(qty) || qty < 1 || qty > 999) return;
    db.prepare('INSERT OR IGNORE INTO draft_set_members (set_draft_id, member_ne_code, qty, sort) VALUES (?, ?, ?, ?)').run(draftId, code, qty, i);
  });
}

/**
 * 結ぶときにカードの空の欄を画面 D の値で埋める (仮レビュー L2)。入っている欄は変えない = not_applied に (欄・カードの値・画面 D の値) を返す。
 * 戻り値 { applied: [欄の名前], not_applied: [{ field, label, card, entered }] }
 */
function fillEmptyCardFields(db, id, p) {
  const applied = []; const notApplied = [];
  const d = db.prepare('SELECT price, official_url, asin, amazon_url FROM product_drafts WHERE id = ?').get(id);
  const empty = (v) => v == null || String(v).trim() === '';
  const one = (field, label, cardVal, entered, write) => {
    if (entered == null || entered === '') return;
    if (empty(cardVal)) { write(); applied.push(label); } else if (String(cardVal) !== String(entered)) notApplied.push({ field, label, card: cardVal, entered });
  };
  const price = Number.isSafeInteger(Number(p.price)) ? Number(p.price) : null;
  for (const [col, label, val] of [['price', '売価', price], ['official_url', '公式ページ URL', p.official_url || null], ['asin', 'ASIN', p.asin || null], ['amazon_url', 'Amazon URL', p.amazon_url || null]]) {
    one(col, label, d[col], val, () => db.prepare(`UPDATE product_drafts SET ${col} = ? WHERE id = ? AND (${col} IS NULL OR TRIM(${col}) = '')`).run(val, id));
  }
  // 参考 URL: 無い URL だけ足す (前からある URL は消さない・並びの後ろに)
  const refs = Array.isArray(p.reference_urls) ? p.reference_urls.filter((u) => typeof u === 'string' && /^https?:\/\/\S+$/i.test(u)) : [];
  const have = new Set(db.prepare('SELECT url FROM draft_reference_urls WHERE draft_id = ?').all(id).map((r) => r.url));
  let sort = Number(db.prepare('SELECT COALESCE(MAX(sort), -1) AS s FROM draft_reference_urls WHERE draft_id = ?').get(id).s);
  const added = refs.filter((u) => !have.has(u));
  for (const u of added) db.prepare('INSERT INTO draft_reference_urls (draft_id, url, sort) VALUES (?, ?, ?)').run(id, u, ++sort);
  if (added.length) applied.push(`参考 URL ${added.length} 件`);
  // セット商品を作るか: 判断がまだ無いときだけ
  const sd = p.set_decision;
  if (sd && p.kind === 'single') {
    const has = db.prepare('SELECT 1 FROM draft_set_decisions WHERE draft_id = ? LIMIT 1').get(id);
    if (has) notApplied.push({ field: 'set_decision', label: 'セット商品を作るか', card: '判断あり', entered: sd.decision });
    else {
      if (sd.decision === 'none' && SET_DECISION_REASONS[sd.reason_code]) {
        db.prepare('INSERT INTO draft_set_decisions (draft_id, decision, reason_code, reason_text, decided_by) VALUES (?, ?, ?, ?, ?)').run(id, 'none', sd.reason_code, sd.reason_text || null, p.created_by || ACTOR);
      } else if (sd.decision === 'hold' || sd.decision === 'create') {
        const text = sd.decision === 'create' ? `作る予定 (新商品の登録で「作る」)${sd.reason_text ? `: ${sd.reason_text}` : ''}` : (sd.reason_text || null);
        db.prepare('INSERT INTO draft_set_decisions (draft_id, decision, reason_code, reason_text, decided_by) VALUES (?, ?, NULL, ?, ?)').run(id, 'hold', text, p.created_by || ACTOR);
      }
      applied.push('セット商品を作るか');
    }
  }
  if (p.kind === 'set') {
    const n = db.prepare('SELECT COUNT(*) AS c FROM draft_set_members WHERE set_draft_id = ?').get(id).c;
    if (n === 0) { writeSetMembers(db, id, p.components); applied.push('セットの構成品'); } else notApplied.push({ field: 'set_members', label: 'セットの構成品', card: `${n} 行`, entered: (p.components || []).map((c) => `${c.code}×${c.qty}`).join(', ') });
  }
  // 楽天の配送方法 (対応があるとき・カードが空のとき)
  const ship = mapCdbShipping(db, p.shipping);
  const rk = db.prepare('SELECT shipping_method_group FROM draft_rakuten WHERE draft_id = ?').get(id);
  if (ship.group) {
    if (!rk) { db.prepare('INSERT INTO draft_rakuten (draft_id, shipping_method_group) VALUES (?, ?)').run(id, ship.group); applied.push('楽天の配送方法'); } else one('shipping_method_group', '楽天の配送方法', rk.shipping_method_group, ship.group, () => db.prepare('UPDATE draft_rakuten SET shipping_method_group = ? WHERE draft_id = ?').run(ship.group, id));
  }
  // Yahoo! (欄ごと・空のときだけ)
  const y = p.yahoo;
  if (y) {
    const cur = db.prepare('SELECT * FROM draft_yahoo WHERE draft_id = ?').get(id) || {};
    const patch = {};
    for (const [col, key, label] of [['yahoo_price', 'price', 'Yahoo!売価'], ['yahoo_price_sagawa', 'price_sagawa', 'Yahoo!売価 (佐川)'], ['delivery_label', 'delivery_label', 'Yahoo!の配送方法'],
      ['yahoo_category_id', 'category_id', 'Yahoo!カテゴリID'], ['yahoo_path', 'path', 'Yahoo!path']]) {
      one(col, label, cur[col], y[key] ?? null, () => { patch[col] = y[key]; });
    }
    if (patch.delivery_label) patch.shipping_override = patch.delivery_label !== (ship.label ?? null) ? 1 : 0;
    if (Object.keys(patch).length) upsertDraftYahoo(db, id, patch);
  }
  return { applied, not_applied: notApplied };
}

/**
 * 衝突を人が解く = 既存のカード (同じ商品コード) をこの Company DB の SKU に結ぶ (SQLite の 1 つの取引・PR #1566 R1 M6)。
 * 取引の中で衝突をもう一度確かめる: カードがある・商品コードが同じ・ほかの SKU に結ばれていない・この SKU がほかのカードに結ばれていない。
 * もうこの SKU に結ばれている = 何もしない (冪等・already)。確かめに落ちたら throw (Company DB の知らせは conflict のまま)。
 * カードの空の欄は画面 D の値で埋める・入っている欄は変えない (仮レビュー L2)。
 * 戻り値 { outcome: 'linked', draft_id, already, applied: [欄], not_applied: [{ field, label, card, entered }] }
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
    // 同じ商品コードのカードが、このカードの 1 枚だけか (一意の index が無い DB でも = 取引の中で全部数える。PR #1566 Codex R2 Medium)
    const same = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ? ORDER BY id').all(code).map((x) => x.id);
    if (same.length !== 1 || same[0] !== id) {
      throw Object.assign(fail(`同じ商品コード ${code} のカードが product-hub に ${same.length} 枚 (${same.map((x) => '#' + x).join('・')}) あります。どれに結ぶか決められないので結びません (1 枚に片付けてから)`),
        { ambiguous: true, draft_ids: same });
    }
    const other = db.prepare('SELECT id FROM product_drafts WHERE cdb_sku_id = ? AND id <> ?').get(skuId, id);
    if (other) throw fail(`この商品はもう別のカード #${other.id} に結ばれています`);
    const already = d.cdb_sku_id != null;
    if (!already) {
      const u = db.prepare(`UPDATE product_drafts SET cdb_sku_id = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ? AND cdb_sku_id IS NULL`).run(skuId, id);
      if (u.changes !== 1) throw fail(`カード #${id} をちょうどほかの処理が変えました`);
      logEvent(db, id, 'cdb_card_linked', `Company DB の新商品 ${code} (SKU ${skuId}) に結んだ (人が決めた)`, actor);
    }
    const fill = already ? { applied: [], not_applied: [] } : fillEmptyCardFields(db, id, event.payload || {});
    if (fill.applied.length || fill.not_applied.length) {
      logEvent(db, id, 'cdb_card_link_fields', `埋めた: ${fill.applied.join('・') || 'なし'} / 入っていたので変えなかった: ${fill.not_applied.map((x) => x.label).join('・') || 'なし'}`, actor);
    }
    db.prepare(`
      INSERT INTO ph_cdb_card_events (event_id, cdb_sku_id, ne_code, outcome, draft_id, conflict_draft_id)
      VALUES (?, ?, ?, 'linked', ?, NULL)
      ON CONFLICT(event_id) DO UPDATE SET outcome = 'linked', draft_id = excluded.draft_id, conflict_draft_id = NULL,
        applied_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(eventId, skuId, code, id);
    return { outcome: 'linked', draft_id: id, already, applied: fill.applied, not_applied: fill.not_applied };
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
