/**
 * cdb-group-intake.js — Company DB のまとまり (色違い・サイズ違い) の知らせ (group_snapshot / ph-group-v1) から、まとまりで 1 枚のカードを作る・置き換える
 * (Company DB構想 20 v7 §⑤ の「出品カード」・§③ の revision の定義・§⑩ の PR-4。Postgres 側 = lib/product-hub-outbox.mjs の runGroupOutbox)
 *
 * 知らせ = 毎回「今のまとまりの全部」の完全なスナップショット (差分の知らせは無い・設計 R3 High 3)。1 つの知らせ = SQLite の 1 つの取引で:
 *   - まとまりのカード (product_drafts.cdb_group_product_id = まとまりの商品の番号) が
 *       ある → revision が今より大きいときだけ、子・軸・選択肢を全部置き換える (replaced)。小さい / 同じ = 何もしない (stale = done)
 *       無い → 同じコード (LOWER(TRIM(ne_code)) = まとまりのコード) のカードを数える:
 *         0 枚 = 新しいカードを作る (created)
 *         1 枚 = そのカードがほかに結ばれていない (まとまりにも SKU にも。単品の代表のまとまりは代表の SKU のカードなら可) → 結ぶ (linked・「要確認」)
 *                ほかに結ばれている → 衝突 (conflict)。増やさない・直さない・結ばない (人が決める)
 *         2 枚以上 = どれか決められない衝突 (conflict・ambiguous)
 *   - 廃止した子・スナップショットから消えた子・消えた選択肢は消さずに無効 (active = 0)。カード・mirror との一致・出品は有効な子だけで見る
 *   - 置き換えでまとまりの名前・軸の名前・選択肢名・子が変わった = カードに「要確認」(人が確かめて消す)。モールの出品は自動で変えない (§④)
 * 冪等: 同じ知らせをもう一度 (event_id が記録にある・衝突でない・カードがまだ結ばれている) = 記録の答えを返すだけ
 * 🚨 カードの欄 (名前・売価・画像…) は作るときだけ Company DB の値で入れる。置き換え・結ぶときは変えない (カードはページの値を自分で持つ)
 * 🚨 衝突のときに、まとまりや子を自動で作り直さない (§③)。Drive などの外への呼び出しはしない (何度でも流せる形)
 */
import { getDB, logEvent, upsertDraftYahoo } from '../db.js';
import { mapCdbShipping } from './cdb-card-intake.js';
import { groupSnapshotShapeProblem, GROUP_SCHEMA_VERSION, GROUP_SNAPSHOT_KIND } from '../../../lib/product-hub-outbox.mjs';

/** 取り込めるまとまりの知らせの版 (lib/product-hub-outbox.mjs の GROUP_SCHEMA_VERSION) */
export const GROUP_SCHEMAS = Object.freeze([GROUP_SCHEMA_VERSION]);
const ACTOR = 'auto:cdb-group';
const norm = (v) => (v == null ? '' : String(v).trim().toLowerCase());

function fail(message) { return Object.assign(new Error(message), { code: 'CDB_GROUP_INVALID' }); }

/** 知らせの形 (Company DB で確かめ済みだが、SQLite に入れる前にもう一度。lib と同じ決まり) */
function checkGroupEvent(event) {
  if (!GROUP_SCHEMAS.includes(event?.schema_version)) throw fail(`知らないまとまりの知らせの版: ${event?.schema_version}`);
  if (event.kind != null && event.kind !== GROUP_SNAPSHOT_KIND) throw fail(`まとまりの知らせの種類が違う: ${event.kind}`);
  if (event.entity_kind != null && event.entity_kind !== 'variation_group') throw fail(`まとまりの知らせではない: ${event.entity_kind}`);
  const eventId = String(event.event_id ?? '');
  if (!/^[0-9a-f-]{36}$/.test(eventId)) throw fail(`event_id が不正: ${event.event_id}`);
  const p = event.payload;
  const prob = groupSnapshotShapeProblem(p);
  if (prob) throw fail(`まとまりの知らせ (ph-group-v1) の形が違う: ${prob}`);
  const gid = Number(p.group.product_id);
  if (!Number.isSafeInteger(gid) || gid <= 0) throw fail(`まとまりの番号が不正: ${p.group.product_id}`);
  if (event.group_product_id != null && String(event.group_product_id) !== p.group.product_id) throw fail('知らせのまとまりの番号と payload の番号が違う');
  if (event.revision != null && Number(event.revision) !== p.revision) throw fail('知らせの revision と payload の revision が違う');
  // コードの形は ph-group-v1 の決まり (1〜255 字・前後の空白・制御文字なし) だけで見る: 今あるまとまり (夜間ロードの札) の子のコードは
  // NE の元のコード = 新しいコードの形 (^[A-Za-z0-9_-]{1,30}$) より広いことがある。カードの ne_code = まとまりのコードを打ったとおり (原文)・鍵は小文字
  const code = p.group.code;
  for (const c of p.children) if (!Number.isSafeInteger(Number(c.sku_id))) throw fail(`子の SKU の番号が不正: ${c.sku_id}`);
  for (const c of p.cancelled_children) if (!Number.isSafeInteger(Number(c.sku_id))) throw fail(`廃止した子の SKU の番号が不正: ${c.sku_id}`);
  const repSku = p.group.sku_id == null ? null : Number(p.group.sku_id);
  if (repSku != null && !Number.isSafeInteger(repSku)) throw fail(`代表の SKU の番号が不正: ${p.group.sku_id}`);
  return { p, eventId, gid, revision: p.revision, code, codeKey: norm(code), repSku };
}

/** スナップショットの子・軸・選択肢を SQLite に (置き換え)。前の姿との違い (要確認の文) を返す */
function writeSnapshot(db, draftId, ev, { first }) {
  const { p } = ev;
  const changes = [];
  const prevAxes = new Map(db.prepare('SELECT axis, name FROM ph_cdb_group_axes WHERE draft_id = ?').all(draftId).map((a) => [a.axis, a.name]));
  const prevOpts = new Map(db.prepare('SELECT axis, code_key, name, active FROM ph_cdb_group_options WHERE draft_id = ?').all(draftId).map((o) => [`${o.axis}:${o.code_key}`, o]));
  const prevKids = new Map(db.prepare('SELECT cdb_sku_id, code, name, price, active FROM ph_cdb_group_children WHERE draft_id = ?').all(draftId).map((c) => [Number(c.cdb_sku_id), c]));
  const prevGroup = db.prepare('SELECT group_name FROM ph_cdb_groups WHERE draft_id = ?').get(draftId);

  // まとまり (名前・共通の欄)
  if (!first && prevGroup && prevGroup.group_name !== p.group.name) changes.push(`まとまりの名前 ${prevGroup.group_name} → ${p.group.name}`);
  db.prepare(`
    INSERT INTO ph_cdb_groups (draft_id, group_code, group_name, group_kind, rep_sku_id, common_json, event_id)
    VALUES (@d, @code, @name, @kind, @rep, @common, @ev)
    ON CONFLICT(draft_id) DO UPDATE SET group_code = excluded.group_code, group_name = excluded.group_name, group_kind = excluded.group_kind,
      rep_sku_id = excluded.rep_sku_id, common_json = excluded.common_json, event_id = excluded.event_id,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `).run({ d: draftId, code: p.group.code, name: p.group.name, kind: p.group.kind, rep: ev.repSku, common: JSON.stringify(p.common), ev: ev.eventId });

  // 軸 (0〜2 つ・全部置き換え。軸の数は登録の後に変わらない決まり = 変われば要確認)
  if (!first) {
    for (const a of p.axes) if (prevAxes.has(a.axis) && prevAxes.get(a.axis) !== a.name) changes.push(`軸の名前 ${prevAxes.get(a.axis)} → ${a.name}`);
    if (prevAxes.size !== p.axes.length) changes.push(`軸の数 ${prevAxes.size} → ${p.axes.length}`);
  }
  db.prepare('DELETE FROM ph_cdb_group_axes WHERE draft_id = ?').run(draftId);
  for (const a of p.axes) db.prepare('INSERT INTO ph_cdb_group_axes (draft_id, axis, name) VALUES (?, ?, ?)').run(draftId, a.axis, a.name);

  // 選択肢 (有効な選択肢 = スナップショット・消えたものは無効の印)
  const seenOpt = new Set();
  let addedOpts = 0;
  for (const o of p.options) {
    const key = `${o.axis}:${norm(o.code)}`;
    seenOpt.add(key);
    const prev = prevOpts.get(key);
    if (!first && prev && prev.active === 1 && prev.name !== o.name) changes.push(`社内の選択肢名が変わった ${o.code} ${prev.name} → ${o.name}`);
    if (!first && (!prev || prev.active !== 1)) addedOpts += 1;
    db.prepare(`
      INSERT INTO ph_cdb_group_options (draft_id, axis, code, code_key, name, sort, active) VALUES (?, ?, ?, ?, ?, ?, 1)
      ON CONFLICT(draft_id, axis, code_key) DO UPDATE SET code = excluded.code, name = excluded.name, sort = excluded.sort, active = 1
    `).run(draftId, o.axis, o.code, norm(o.code), o.name, o.sort);
  }
  let goneOpts = 0;
  for (const [key, o] of prevOpts) {
    if (seenOpt.has(key) || o.active !== 1) continue;
    goneOpts += 1;
    db.prepare('UPDATE ph_cdb_group_options SET active = 0 WHERE draft_id = ? AND axis = ? AND code_key = ?').run(draftId, o.axis, o.code_key);
  }
  if (addedOpts) changes.push(`選択肢が ${addedOpts} 件増えた`);
  if (goneOpts) changes.push(`選択肢が ${goneOpts} 件なくなった`);

  // 子 (有効な子 = スナップショットの children・廃止した子 = cancelled_children・どちらにも無い前の子 = 無効 (removed))
  const seenKid = new Set();
  let added = 0; let changedKids = 0;
  p.children.forEach((c, i) => {
    const sid = Number(c.sku_id);
    seenKid.add(sid);
    const prev = prevKids.get(sid);
    if (!first) {
      if (!prev || prev.active !== 1) added += 1;
      else if (prev.code !== c.code || prev.name !== c.name || (prev.price ?? null) !== (c.price ?? null)) changedKids += 1;
    }
    db.prepare(`
      INSERT INTO ph_cdb_group_children (draft_id, cdb_sku_id, code, code_key, name, price, choice1, choice2, jans_json, sort, active, inactive_reason)
      VALUES (@d, @sid, @code, @key, @name, @price, @c1, @c2, @jans, @sort, 1, NULL)
      ON CONFLICT(draft_id, cdb_sku_id) DO UPDATE SET code = excluded.code, code_key = excluded.code_key, name = excluded.name, price = excluded.price,
        choice1 = excluded.choice1, choice2 = excluded.choice2, jans_json = excluded.jans_json, sort = excluded.sort, active = 1, inactive_reason = NULL
    `).run({ d: draftId, sid, code: c.code, key: norm(c.code), name: c.name, price: c.price, c1: c.choices['1'] ?? null, c2: c.choices['2'] ?? null,
      jans: JSON.stringify(c.jans), sort: i });
  });
  let cancelled = 0;
  for (const c of p.cancelled_children) {
    const sid = Number(c.sku_id);
    seenKid.add(sid);
    const prev = prevKids.get(sid);
    if (!first && (!prev || prev.active === 1)) cancelled += 1;
    db.prepare(`
      INSERT INTO ph_cdb_group_children (draft_id, cdb_sku_id, code, code_key, name, sort, active, inactive_reason)
      VALUES (@d, @sid, @code, @key, NULL, 0, 0, 'cancelled')
      ON CONFLICT(draft_id, cdb_sku_id) DO UPDATE SET code = excluded.code, code_key = excluded.code_key, active = 0, inactive_reason = 'cancelled'
    `).run({ d: draftId, sid, code: c.code, key: norm(c.code) });
  }
  let removed = 0;
  for (const [sid, k] of prevKids) {
    if (seenKid.has(sid) || k.active !== 1) continue;
    removed += 1;
    db.prepare(`UPDATE ph_cdb_group_children SET active = 0, inactive_reason = 'removed' WHERE draft_id = ? AND cdb_sku_id = ?`).run(draftId, sid);
  }
  if (added) changes.push(`子が ${added} 件増えた`);
  if (cancelled) changes.push(`子を ${cancelled} 件廃止した`);
  if (removed) changes.push(`子が ${removed} 件まとまりから外れた`);
  if (changedKids) changes.push(`子の名前・売価が ${changedKids} 件変わった`);
  return changes;
}

/** 「要確認」を足す (前の要確認が残っていれば後ろにつなぐ = 人が確かめるまで消えない) */
function addAttention(db, draftId, text) {
  const cur = db.prepare('SELECT attention FROM ph_cdb_groups WHERE draft_id = ?').get(draftId)?.attention || '';
  const next = cur ? `${cur} / ${text}` : text;
  db.prepare(`UPDATE ph_cdb_groups SET attention = ?, attention_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE draft_id = ?`).run(next.slice(0, 2000), draftId);
}

/** 作るときだけ: カードの欄を Company DB の値で (共通の欄・売価は子が全部同じときだけ) */
function createCard(db, ev) {
  const { p } = ev;
  const m = p.common;
  const prices = [...new Set(p.children.map((c) => c.price))];
  const price = prices.length === 1 && Number.isSafeInteger(prices[0]) ? prices[0] : null;
  const ship = mapCdbShipping(db, m.shipping);
  const info = db.prepare(`
    INSERT INTO product_drafts (ne_code, name, official_url, price, asin, amazon_url, has_variation, created_by, cdb_group_product_id, cdb_group_revision)
    VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, ?)
  `).run(ev.code, p.group.name, m.official_url || null, price, m.asin || null, m.amazon_url || null, p.created_by || ACTOR, ev.gid, ev.revision);
  const draftId = Number(info.lastInsertRowid);
  const refs = (m.reference_urls || []).filter((u) => typeof u === 'string' && /^https?:\/\/\S+$/i.test(u));
  refs.forEach((u, i) => db.prepare('INSERT INTO draft_reference_urls (draft_id, url, sort) VALUES (?, ?, ?)').run(draftId, u, i));
  if (ship.group) db.prepare('INSERT INTO draft_rakuten (draft_id, shipping_method_group) VALUES (?, ?)').run(draftId, ship.group);
  const y = m.yahoo;
  if (y) {
    const delivery = y.delivery_label ? String(y.delivery_label) : null;
    upsertDraftYahoo(db, draftId, {
      yahoo_price: y.price ?? null, yahoo_price_sagawa: y.price_sagawa ?? null, delivery_label: delivery,
      shipping_override: delivery && delivery !== ship.label ? 1 : 0, yahoo_category_id: y.category_id ?? null, yahoo_path: y.path ?? null,
    });
  }
  return { draftId, ship };
}

/**
 * まとまりの知らせを 1 つ取り込む (同期・better-sqlite3)。
 * 戻り値 { outcome: created | replaced | linked | stale | conflict, draft_id, revision, conflict_draft_id?, conflict_draft_ids?, ambiguous?, message?, replayed? }
 * 失敗 (形が違う・SQLite の誤り) は throw = Postgres 側で failed (後でもう一度)
 */
export function applyCdbGroupEvent(event, { db = getDB() } = {}) {
  const ev = checkGroupEvent(event);
  const { p, eventId, gid, revision, code, codeKey } = ev;
  return db.transaction(() => {
    const record = (outcome, draftId, conflictId = null) => db.prepare(`
      INSERT INTO ph_cdb_group_events (event_id, cdb_group_product_id, revision, group_code, outcome, draft_id, conflict_draft_id) VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET outcome = excluded.outcome, draft_id = excluded.draft_id, conflict_draft_id = excluded.conflict_draft_id,
        applied_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
    `).run(eventId, gid, revision, code, outcome, draftId, conflictId);
    const bound = db.prepare('SELECT id, ne_code, cdb_group_revision FROM product_drafts WHERE cdb_group_product_id = ?').get(gid);
    // 同じ知らせをもう一度 = 記録の答え (カードがまだこのまとまりに結ばれているときだけ)
    const seen = db.prepare('SELECT * FROM ph_cdb_group_events WHERE event_id = ?').get(eventId);
    if (seen && seen.outcome !== 'conflict' && bound && bound.id === seen.draft_id) {
      return { outcome: seen.outcome, draft_id: seen.draft_id, revision: seen.revision, replayed: true };
    }
    if (bound) {
      const cur = Number(bound.cdb_group_revision ?? 0);
      if (revision <= cur) {
        // 古い / 同じ revision = 何もしない (順番が逆に届いても安全・同じ revision は 1 回だけ効く)
        record('stale', bound.id);
        return { outcome: 'stale', draft_id: bound.id, revision: cur, message: `カードは revision ${cur} (知らせは ${revision}) = 何もしない` };
      }
      const changes = writeSnapshot(db, bound.id, ev, { first: false });
      db.prepare(`UPDATE product_drafts SET cdb_group_revision = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?`).run(revision, bound.id);
      if (changes.length) {
        addAttention(db, bound.id, `revision ${revision}: ${changes.join('・')}`);
        logEvent(db, bound.id, 'cdb_group_changed', `まとまり ${code} が revision ${cur} → ${revision} (${changes.join('・')})。モールの出品は自動で変えていません (要確認)`, ACTOR);
      } else {
        logEvent(db, bound.id, 'cdb_group_replaced', `まとまり ${code} が revision ${cur} → ${revision} (子・軸・選択肢の違いなし)`, ACTOR);
      }
      record('replaced', bound.id);
      return { outcome: 'replaced', draft_id: bound.id, revision, changes };
    }
    // このまとまりのカードはまだ無い = 同じコードのカードを全部数える (一意の index に頼らない)
    const same = db.prepare('SELECT id, ne_code, cdb_sku_id, cdb_group_product_id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ? ORDER BY id').all(codeKey);
    if (same.length > 1) {
      record('conflict', null);
      for (const d of same) logEvent(db, d.id, 'cdb_group_conflict', `Company DB のまとまり ${code} (番号 ${gid}) と同じ商品コードのカードが ${same.length} 枚 (どれか決められない)`, ACTOR);
      return { outcome: 'conflict', draft_id: null, conflict_draft_id: null, conflict_draft_ids: same.map((d) => d.id), ambiguous: true, revision,
        message: `同じ商品コード ${code} のカードが product-hub に ${same.length} 枚 (${same.map((d) => '#' + d.id).join('・')}) あります。どれに結ぶか決められないので、増やしても結んでもいません (product-hub で 1 枚に片付けてから「もう一度」)` };
    }
    if (same.length === 1) {
      const d = same[0];
      const otherGroup = d.cdb_group_product_id != null;
      const otherSku = d.cdb_sku_id != null && !(p.group.kind === 'single' && ev.repSku != null && Number(d.cdb_sku_id) === ev.repSku);
      if (otherGroup || otherSku) {
        record('conflict', null, d.id);
        const why = otherGroup ? `別のまとまり (番号 ${d.cdb_group_product_id})` : `別の商品 (SKU ${d.cdb_sku_id})`;
        logEvent(db, d.id, 'cdb_group_conflict', `Company DB のまとまり ${code} (番号 ${gid}) と同じ商品コードのカード (${why} に結ばれている)`, ACTOR);
        return { outcome: 'conflict', draft_id: null, conflict_draft_id: d.id, revision,
          message: `同じ商品コード ${code} のカード (#${d.id}) が product-hub にあり、${why}に結ばれています。増やしても結んでもいません (どちらを使うか決めてください)` };
      }
      // 今あるカード (今ある楽天のページ・単品の代表のカード) に結ぶ = スナップショットで全部置き換え・「要確認」
      const u = db.prepare(`UPDATE product_drafts SET cdb_group_product_id = ?, cdb_group_revision = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
        WHERE id = ? AND cdb_group_product_id IS NULL`).run(gid, revision, d.id);
      if (u.changes !== 1) throw fail(`カード #${d.id} をちょうどほかの処理が変えました`);
      writeSnapshot(db, d.id, ev, { first: true });
      addAttention(db, d.id, `revision ${revision}: 今あるカードを Company DB のまとまり ${code} に結んだ (子 ${p.children.length} 件・確かめてください)`);
      logEvent(db, d.id, 'cdb_group_linked', `Company DB のまとまり ${code} (番号 ${gid}・revision ${revision}・子 ${p.children.length} 件) に今あるカードを結んだ。カードの欄は変えていません`, ACTOR);
      record('linked', d.id);
      return { outcome: 'linked', draft_id: d.id, revision };
    }
    const { draftId, ship } = createCard(db, ev);
    writeSnapshot(db, draftId, ev, { first: true });
    record('created', draftId);
    logEvent(db, draftId, 'created_from_cdb_group', `Company DB のまとまり ${code} (番号 ${gid}・revision ${revision}・子 ${p.children.length} 件${p.axes.length ? `・軸 ${p.axes.map((a) => a.name).join(' × ')}` : ''})`, p.created_by || ACTOR);
    if (ship.status === 'unmapped') {
      logEvent(db, draftId, 'cdb_shipping_unmapped', `送料コード ${ship.code ?? '—'} (${ship.method ?? '方法なし'}) に楽天の配送方法の対応がありません (要確認)`, ACTOR);
    }
    return { outcome: 'created', draft_id: draftId, revision, shipping_status: ship.status };
  })();
}

/** 「要確認」を人が確かめて消す。戻り値 { ok, cleared } */
export function ackCdbGroupAttention(draftId, { actor = null, expected = null, db = getDB() } = {}) {
  return db.transaction(() => {
    const g = db.prepare('SELECT attention FROM ph_cdb_groups WHERE draft_id = ?').get(Number(draftId));
    if (!g) return { ok: false, error: 'まとまりのカードではありません' };
    if (!g.attention) return { ok: true, cleared: false };
    // 画面が見ていた文と違う = その間に新しい知らせが来た (消さない = 開き直してもらう)
    if (expected != null && String(expected) !== g.attention) return { ok: false, error: '要確認の中身が変わりました。画面を開き直してから確かめてください', changed: true };
    db.prepare('UPDATE ph_cdb_groups SET attention = NULL, attention_at = NULL WHERE draft_id = ?').run(Number(draftId));
    logEvent(db, Number(draftId), 'cdb_group_attention_ack', `確かめた: ${g.attention}`.slice(0, 1000), actor);
    return { ok: true, cleared: true };
  })();
}
