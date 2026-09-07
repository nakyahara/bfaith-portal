/**
 * 商品別 想定利益 — 世代の受け口 (Render 側)
 *
 * 正本 = §7.5 / §15-7
 *
 * 🚨 既存の mirror push (DELETE → INSERT の全置換) には乗らない。
 *    全置換は受信中に一瞬テーブルが空になり、その瞬間に画面を開くと何も出ない。
 *    ここは **世代を作り切ってからポインタを切り替える**。
 *
 * 契約:
 *   POST /generations/:id/chunks  … 行を分割して受け取る (checksum つき・冪等)
 *   POST /publish                 … 同一tx で「ポインタ更新 + published + 前世代 superseded」
 *   GET  /published               … 公開中の世代を返す (送信側が読み戻して確認する)
 *
 * 逆転防止: seq が現在公開中以下の publish は 409 (実行Aの遅い転送が実行Bの後に届いても戻らない)
 * 冪等: 同じ chunk の再送は checksum 一致なら短絡、同じ世代の再 publish は 200
 */
import { Router } from 'express';
import crypto from 'crypto';
import { getExpectedProfitDB } from './db.js';
import { hashGeneration } from './generation-hash.js';
import { nowIso } from './util.js';

const router = Router();

function requireSyncKey(req, res, next) {
  const key = process.env.MIRROR_SYNC_KEY;
  if (!key) {
    if (process.env.ALLOW_INSECURE_MIRROR_SYNC === '1') return next();
    return res.status(503).json({ error: 'mirror_sync_key_unset' });
  }
  const provided = req.headers['x-sync-key'] || req.query.sync_key;
  if (provided !== key) return res.status(401).json({ error: 'invalid_sync_key' });
  next();
}

/** チャンクの checksum (送信側と同じ計算) */
export function chunkChecksum(rows) {
  return crypto.createHash('sha256').update(JSON.stringify(rows)).digest('hex');
}

/**
 * 受信した行を世代へ入れる (冪等)。
 * 同じ checksum のチャンクが既に入っていれば何もしない。
 */
export function receiveChunk(db, { generationId, seq, chunkIndex, checksum, rows, manifest }) {
  const existing = db.prepare('SELECT * FROM expected_profit_generation WHERE generation_id = ?').get(generationId);
  const prior = db.prepare('SELECT * FROM expected_profit_chunk WHERE generation_id = ? AND chunk_index = ?')
    .get(generationId, chunkIndex);

  if (existing && (existing.remote_status === 'published' || existing.remote_status === 'superseded')) {
    // 🚨 公開済み・旧公開世代は不変にする。
    //    ただし「同じ内容の再送」だけは 200 で受ける。公開成功後に応答を失った送信側が、
    //    チャンクからやり直しても publish まで到達できるようにするため (Codex R4-7)
    if (prior && prior.checksum === checksum) {
      return { ok: true, idempotent: true, chunkIndex, received: 0, note: 'already_published_same_chunk' };
    }
    return { ok: false, status: 409, error: 'generation_already_published' };
  }
  // 🚨 同じ index を別内容で上書きさせない (混ざった世代を作らせない — Codex R4-5)
  if (prior && prior.checksum !== checksum) {
    return { ok: false, status: 409, error: 'chunk_conflict', chunkIndex, storedChecksum: prior.checksum };
  }
  if (!existing) {
    db.prepare(`INSERT INTO expected_profit_generation
      (generation_id, seq, built_at, local_status, remote_status, row_count, ok_count,
       incomplete_count, rank_eligible_count, content_hash, malls_included, malls_degraded)
      VALUES (?, ?, ?, 'validated', 'sending', ?, ?, ?, ?, ?, ?, ?)`)
      .run(generationId, seq, manifest?.built_at || nowIso(), manifest?.row_count ?? null,
        manifest?.ok_count ?? null, manifest?.incomplete_count ?? null,
        manifest?.rank_eligible_count ?? null, manifest?.content_hash ?? null,
        JSON.stringify(manifest?.malls_included || []), JSON.stringify(manifest?.malls_degraded || []));
  }

  const calc = chunkChecksum(rows);
  if (calc !== checksum) return { ok: false, status: 400, error: 'checksum_mismatch', expected: checksum, actual: calc };

  // 冪等: 同じ行が既に入っているなら数だけ返す
  const already = db.prepare(`SELECT COUNT(*) n FROM mart_listing_expected_profit
                              WHERE generation_id = ?`).get(generationId).n;

  const insert = db.prepare(`INSERT OR REPLACE INTO mart_listing_expected_profit
    (generation_id, mall, shop_id, mall_item_key, ne_code, product_name, sales_class, fulfillment,
     listing_status, price_incl_tax, price_ex_tax, postage_revenue_ex_tax, revenue_ex_tax, tax_rate,
     cost_ex_tax, cost_method, shipping_code, shipping_method, shipping_fee_ex_tax, shipping_work_ex_tax,
     shipping_material_ex_tax, shipping_labor_ex_tax, shipping_total_ex_tax, fba_fee_ex_tax,
     referral_fee_ex_tax, closing_fee_ex_tax, per_item_fee_ex_tax, fee_total_ex_tax, fee_rate_display,
     fee_breakdown, expected_profit, expected_margin_rate, listing_enum_status, listing_enum_valid_until,
     price_status, price_valid_until, fee_status, fee_valid_until, cost_status, cost_valid_until,
     shipping_master_status, shipping_master_valid_until, shipping_revenue_status, scenario_fit,
     calculation_status, incomplete_reason, rank_eligible, rank_exclusion_reason, expense_scope_version,
     input_snapshot, formula_version, scenario_version, fee_rate_version, code_version, price_run_id, built_at)
    VALUES
     (@generation_id, @mall, @shop_id, @mall_item_key, @ne_code, @product_name, @sales_class, @fulfillment,
      @listing_status, @price_incl_tax, @price_ex_tax, @postage_revenue_ex_tax, @revenue_ex_tax, @tax_rate,
      @cost_ex_tax, @cost_method, @shipping_code, @shipping_method, @shipping_fee_ex_tax, @shipping_work_ex_tax,
      @shipping_material_ex_tax, @shipping_labor_ex_tax, @shipping_total_ex_tax, @fba_fee_ex_tax,
      @referral_fee_ex_tax, @closing_fee_ex_tax, @per_item_fee_ex_tax, @fee_total_ex_tax, @fee_rate_display,
      @fee_breakdown, @expected_profit, @expected_margin_rate, @listing_enum_status, @listing_enum_valid_until,
      @price_status, @price_valid_until, @fee_status, @fee_valid_until, @cost_status, @cost_valid_until,
      @shipping_master_status, @shipping_master_valid_until, @shipping_revenue_status, @scenario_fit,
      @calculation_status, @incomplete_reason, @rank_eligible, @rank_exclusion_reason, @expense_scope_version,
      @input_snapshot, @formula_version, @scenario_version, @fee_rate_version, @code_version, @price_run_id, @built_at)`);
  const recordChunk = db.prepare(`INSERT OR REPLACE INTO expected_profit_chunk
    (generation_id, chunk_index, checksum, row_count, received_at) VALUES (?, ?, ?, ?, ?)`);
  const tx = db.transaction((list) => {
    for (const r of list) insert.run({ ...r, generation_id: generationId });
    recordChunk.run(generationId, chunkIndex, checksum, list.length, nowIso());
  });
  tx(rows);

  const now = db.prepare('SELECT COUNT(*) n FROM mart_listing_expected_profit WHERE generation_id = ?').get(generationId).n;
  return { ok: true, chunkIndex, received: rows.length, before: already, total: now };
}

/**
 * 世代を公開する。
 * 🚨 ポインタ更新・世代状態・前世代の superseded を **同一トランザクション**で行う。
 * 🚨 seq が現在公開中以下なら 409 (逆転公開の防止)
 */
export function publishGeneration(db, { generationId, seq, manifest }) {
  const gen = db.prepare('SELECT * FROM expected_profit_generation WHERE generation_id = ?').get(generationId);
  if (!gen) return { ok: false, status: 404, error: 'generation_not_found' };

  // 🚨 要求の seq を信用しない。保存済み世代の seq を使う (Codex R4-6)。
  //    要求と食い違うなら、送信側と受信側で世代が一致していない
  if (seq != null && Number(seq) !== Number(gen.seq)) {
    return { ok: false, status: 400, error: 'seq_mismatch', requested: seq, stored: gen.seq };
  }
  const effectiveSeq = Number(gen.seq);

  // 🚨 読取・判定・更新をすべて1つのトランザクションに入れる。
  //    判定と更新の間に別接続の公開が割り込むと、逆転防止をすり抜ける
  let outcome;
  const tx = db.transaction(() => {
    const pointer = db.prepare('SELECT * FROM expected_profit_publish_pointer WHERE id = 1').get();
    // 冪等: 同じ世代が既に公開中なら 200 (確認応答が失われた再送)
    if (pointer && pointer.generation_id === generationId) {
      outcome = { ok: true, idempotent: true, generationId, seq: pointer.seq };
      return;
    }
    // 逆転防止 (保存済み seq で比べる)
    if (pointer && effectiveSeq <= pointer.seq) {
      outcome = { ok: false, status: 409, error: 'seq_not_newer', publishedSeq: pointer.seq, attemptedSeq: effectiveSeq };
      return;
    }
    // 🚨 照合の基準は「受信時に保存した manifest」。要求の値を優先しない (Codex R5-2)。
    //    要求を優先すると、content_hash: '' を送るだけで照合を飛ばせてしまう
    const expected = gen.row_count;
    const expectedHash = gen.content_hash;
    if (manifest?.row_count != null && Number(manifest.row_count) !== Number(expected)) {
      outcome = { ok: false, status: 400, error: 'manifest_row_count_mismatch', stored: expected, requested: manifest.row_count };
      return;
    }
    if (manifest?.content_hash != null && manifest.content_hash !== expectedHash) {
      outcome = { ok: false, status: 400, error: 'manifest_hash_mismatch', stored: expectedHash, requested: manifest.content_hash };
      return;
    }
    // 件数の照合
    const actual = db.prepare('SELECT COUNT(*) n FROM mart_listing_expected_profit WHERE generation_id = ?').get(generationId).n;
    if (expected != null && actual !== expected) {
      outcome = { ok: false, status: 400, error: 'row_count_mismatch', expected, actual };
      return;
    }
    if (actual === 0) { outcome = { ok: false, status: 400, error: 'no_rows' }; return; }
    // 🚨 ハッシュが無い世代は公開しない (照合を飛ばさせない)
    if (!expectedHash) { outcome = { ok: false, status: 400, error: 'content_hash_missing' }; return; }
    const actualHash = generationContentHash(db, generationId);
    if (actualHash !== expectedHash) {
      outcome = { ok: false, status: 400, error: 'content_hash_mismatch', expected: expectedHash, actual: actualHash };
      return;
    }
    const previousId = pointer?.generation_id || null;
    db.prepare(`INSERT INTO expected_profit_publish_pointer (id, generation_id, seq, published_at)
                VALUES (1, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET generation_id = excluded.generation_id,
                  seq = excluded.seq, published_at = excluded.published_at`)
      .run(generationId, effectiveSeq, nowIso());
    db.prepare("UPDATE expected_profit_generation SET remote_status = 'published' WHERE generation_id = ?")
      .run(generationId);
    if (previousId) {
      db.prepare("UPDATE expected_profit_generation SET remote_status = 'superseded' WHERE generation_id = ?")
        .run(previousId);
    }
    outcome = { ok: true, generationId, seq: effectiveSeq, supersededId: previousId };
  });
  tx();
  return outcome;
}

/** 保存済みの行から内容ハッシュを再計算する (送信側と同じ関数を通す) */
export function generationContentHash(db, generationId) {
  return hashGeneration(db, generationId);
}

/** 公開中の世代 (送信側が読み戻して確認する) */
export function getPublished(db) {
  const p = db.prepare('SELECT * FROM expected_profit_publish_pointer WHERE id = 1').get();
  if (!p) return null;
  const gen = db.prepare('SELECT * FROM expected_profit_generation WHERE generation_id = ?').get(p.generation_id);
  return {
    generation_id: p.generation_id,
    seq: p.seq,
    published_at: p.published_at,
    built_at: gen?.built_at || null,
    row_count: gen?.row_count ?? null,
    rank_eligible_count: gen?.rank_eligible_count ?? null,
    malls_degraded: gen?.malls_degraded ? JSON.parse(gen.malls_degraded) : [],
  };
}

/** 古い世代を消す (公開中と直前の1つは残す — §15-11) */
export function pruneGenerations(db, keep = 7) {
  const pointer = db.prepare('SELECT * FROM expected_profit_publish_pointer WHERE id = 1').get();
  const rows = db.prepare('SELECT generation_id FROM expected_profit_generation ORDER BY seq DESC').all();
  const protectedIds = new Set([pointer?.generation_id].filter(Boolean));
  const toDelete = rows.slice(keep).map(r => r.generation_id).filter(id => !protectedIds.has(id));
  if (toDelete.length === 0) return { deleted: 0 };
  const tx = db.transaction((ids) => {
    const delRows = db.prepare('DELETE FROM mart_listing_expected_profit WHERE generation_id = ?');
    const delGen = db.prepare('DELETE FROM expected_profit_generation WHERE generation_id = ?');
    for (const id of ids) { delRows.run(id); delGen.run(id); }
  });
  tx(toDelete);
  return { deleted: toDelete.length };
}

// ────────────────────────────────────────────────────────────
// HTTP
// ────────────────────────────────────────────────────────────

router.post('/generations/:id/chunks', requireSyncKey, (req, res) => {
  try {
    const db = getExpectedProfitDB();
    const r = receiveChunk(db, {
      generationId: req.params.id,
      seq: req.body?.seq,
      chunkIndex: req.body?.chunk_index,
      checksum: req.body?.checksum,
      rows: req.body?.rows || [],
      manifest: req.body?.manifest,
    });
    res.status(r.ok ? 200 : (r.status || 400)).json(r);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.post('/publish', requireSyncKey, (req, res) => {
  try {
    const db = getExpectedProfitDB();
    const r = publishGeneration(db, {
      generationId: req.body?.generation_id,
      seq: req.body?.seq,
      manifest: req.body?.manifest,
    });
    res.status(r.ok ? 200 : (r.status || 400)).json(r);
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

router.get('/published', requireSyncKey, (req, res) => {
  try {
    res.json({ ok: true, published: getPublished(getExpectedProfitDB()) });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

export default router;
