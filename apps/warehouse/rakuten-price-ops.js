/**
 * 楽天 価格更新の「計画」と「受領台帳」 (価格一括改定ツール M2)
 *
 * ここは miniPC 側。実際に楽天へ書き込む唯一の経路 (Render に楽天キーを置かない原則)。
 * ルート本体 (rakuten-rms-service.js) から使う。判断のロジックはここに切り出して単体で試せるようにする。
 *
 * 🚨M0 実測をそのまま設計に落としている:
 *   ・GET の standardPrice は**文字列** ("1000") → 照合は整数化してから
 *   ・PATCH の variants キーに**存在しない SKU を書くと新規SKU作成**と解釈される
 *     → 事前 GET で実在を確かめ、実在する SKU にだけ送る
 *   ・**0円が 204 で通る** → 正の整数円だけ受ける
 *   ・成功は **204**。200/201 を成功にしない
 *
 * 受領台帳 (rakuten_price_ops):
 *   Render 側の監査とは別に、実行主体 (miniPC) 側にも残す (要件 F6 の2層目)。
 *   operation_id が来たら「受領」を先に記録し、結果が出たら追記する。
 *   ★同じ operation_id の再送は**実行し直さない**。前回結果を返す (冪等)。
 *   結果が残っていない受領済み ID = 送信済みか不明 → unknown を返し、人が照会して決める。
 */
import path from 'node:path';
import fs from 'node:fs';
import crypto from 'node:crypto';
import Database from 'better-sqlite3';

/** 楽天APIの許容上限。0円は 204 で通ってしまうので下限は 1 円 */
export const MAX_PRICE = 999_999_999;
/** 1リクエストで送れる SKU 数の上限 (1商品まとめて送るが、無制限にはしない) */
export const MAX_SKUS_PER_REQUEST = 100;

/** 文字列でも数値でも、整数円として読めた時だけ数値を返す */
export function toIntPrice(v) {
  if (typeof v === 'number') return Number.isInteger(v) ? v : null;
  if (typeof v !== 'string') return null;
  const s = v.trim();
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

export function isValidPrice(v) {
  return Number.isInteger(v) && v > 0 && v <= MAX_PRICE;
}

/**
 * 送る内容を決める。楽天へのリクエストは作るが送らない (テストできるように)。
 *
 * @param {object} item      GET /items/manage-numbers/:mn の応答 (variants を持つ)
 * @param {object} req
 * @param {Record<string, number>} req.expected 更新前価格 (SKU管理番号 → 整数円)
 * @param {Record<string, number>} req.prices   新価格 (SKU管理番号 → 整数円)
 * @returns {{ok:true, patch:object, applied:object}
 *          |{ok:true, noop:true, reason:string}
 *          |{ok:false, code:string, message:string, detail?:object}}
 */
export function planPriceUpdate(item, { expected, prices }) {
  const variants = item?.variants && typeof item.variants === 'object' ? item.variants : null;
  if (!variants) return { ok: false, code: 'NO_VARIANTS', message: 'SKU情報が取得できませんでした' };

  const skus = Object.keys(prices || {});
  if (skus.length === 0) return { ok: false, code: 'EMPTY_PRICES', message: 'prices が空です' };
  if (skus.length > MAX_SKUS_PER_REQUEST) {
    return { ok: false, code: 'TOO_MANY_SKUS', message: `SKU が多すぎます (${skus.length} > ${MAX_SKUS_PER_REQUEST})` };
  }

  // expected は必須。「今いくらか」を確かめずに書き換えない (要件 F4 楽観ロック)
  const missingExpected = skus.filter((s) => !Object.prototype.hasOwnProperty.call(expected || {}, s));
  if (missingExpected.length > 0) {
    return { ok: false, code: 'EXPECTED_REQUIRED', message: `expected がありません: ${missingExpected.join(', ')}` };
  }

  // 🚨存在しない SKU を送ると「新規SKU作成」と解釈される (M0実測)。実在を先に確かめる
  const unknownSkus = skus.filter((s) => !Object.prototype.hasOwnProperty.call(variants, s));
  if (unknownSkus.length > 0) {
    return {
      ok: false, code: 'SKU_NOT_FOUND',
      message: `商品に存在しない SKU があります: ${unknownSkus.join(', ')} (このまま送ると新規SKUが作られます)`,
      detail: { unknownSkus, availableSkus: Object.keys(variants) },
    };
  }

  const badPrices = skus.filter((s) => !isValidPrice(prices[s]));
  if (badPrices.length > 0) {
    return {
      ok: false, code: 'INVALID_PRICE',
      message: `価格は 1〜${MAX_PRICE} の整数円で指定してください (0円不可): ${badPrices.join(', ')}`,
    };
  }

  // 楽観ロック: ライブ価格が expected と整数円で完全一致しなければ書き換えない
  const conflicts = [];
  for (const s of skus) {
    const live = toIntPrice(variants[s]?.standardPrice);
    if (live == null) {
      conflicts.push({ sku: s, expected: expected[s], live: null, reason: '現在価格を整数円として読めません' });
      continue;
    }
    if (live !== toIntPrice(expected[s])) {
      conflicts.push({ sku: s, expected: expected[s], live, reason: '現在価格が想定と違います' });
    }
  }
  if (conflicts.length > 0) {
    return { ok: false, code: 'CONFLICT', message: '更新前価格が一致しませんでした', detail: { conflicts } };
  }

  // すでに同じ価格なら送らない (再送で無駄に楽天を叩かない)
  const changed = skus.filter((s) => toIntPrice(variants[s].standardPrice) !== prices[s]);
  if (changed.length === 0) {
    return { ok: true, noop: true, reason: 'すべて同じ価格です (更新不要)' };
  }

  const patchVariants = {};
  const applied = {};
  for (const s of changed) {
    patchVariants[s] = { standardPrice: prices[s] };
    applied[s] = prices[s];
  }
  return { ok: true, patch: { variants: patchVariants }, applied };
}

// ─── 受領台帳 ───

let opsDb = null;

export function getPriceOpsDb(dataDir = process.env.DATA_DIR || path.join(process.cwd(), 'data')) {
  if (opsDb) return opsDb;
  fs.mkdirSync(dataDir, { recursive: true });
  opsDb = new Database(path.join(dataDir, 'rakuten-price-ops.db'));
  opsDb.pragma('journal_mode = WAL');
  opsDb.pragma('busy_timeout = 5000');
  createPriceOpsTables(opsDb);
  return opsDb;
}

/** DDL 単体 (テストが自前の DB ハンドルに対して呼べるように) */
export function createPriceOpsTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_price_ops (
    operation_id   TEXT PRIMARY KEY,
    run_id         TEXT,
    manage_number  TEXT NOT NULL,
    request_json   TEXT NOT NULL,
    request_hash   TEXT,                  -- 同じ operation_id で別の依頼が来ていないかの照合用
    received_at    TEXT NOT NULL,
    result_state   TEXT,                  -- 'applied' | 'noop' | 'conflict' | 'failed'
    result_json    TEXT,
    completed_at   TEXT
  )`);
  // 既存DBへの列追加 (再起動で自動的に整う)
  try { db.exec('ALTER TABLE rakuten_price_ops ADD COLUMN request_hash TEXT'); } catch { /* 既にある */ }
  db.exec('CREATE INDEX IF NOT EXISTS idx_rpo_run ON rakuten_price_ops(run_id)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_rpo_mn ON rakuten_price_ops(manage_number, received_at)');
  return db;
}

/**
 * 受領を記録する。
 * @returns {{fresh:true} | {fresh:false, row:object}} fresh=false なら同じ operation_id が既にある
 */
export function receiveOperation(db, { operationId, runId, manageNumber, request }) {
  const hash = requestHash({ manageNumber, request });
  // ★SELECT→INSERT だと同時受領で主キー違反になる。INSERT の結果で受領できたかを判断する
  const info = db.prepare(`INSERT INTO rakuten_price_ops
      (operation_id, run_id, manage_number, request_json, request_hash, received_at)
      VALUES (?,?,?,?,?,?)
      ON CONFLICT(operation_id) DO NOTHING`)
    .run(operationId, runId ?? null, manageNumber, JSON.stringify(request), hash, new Date().toISOString());
  if (info.changes === 1) return { fresh: true };

  const row = getOperation(db, operationId);
  if (!row) return { fresh: false, reused: true, row: null };
  // ★同じ operation_id で**別の依頼**が来た = ID の使い回し/衝突。前回結果を返すと
  //   「更新していないのに成功」と記録される。実行も replay もせず拒否する。
  //   request_hash 列を足す前の行は hash が NULL なので、保存してある依頼から計算し直して比べる
  //   (NULL を「一致」とみなすと、古い行に対しては検査が丸ごと効かない)
  const storedHash = row.request_hash || requestHash({
    manageNumber: row.manage_number,
    request: safeParse(row.request_json),
  });
  if (storedHash !== hash) return { fresh: false, reused: true, row };
  return { fresh: false, row };
}

function safeParse(json) {
  try { return JSON.parse(json || 'null'); } catch { return null; }
}

/** 依頼の同一性を見るためのハッシュ (キー順に依存しない正規形から作る) */
export function requestHash({ manageNumber, request }) {
  const norm = (o) => Object.fromEntries(
    Object.keys(o || {}).sort().map((k) => [k, toIntPrice(o[k]) ?? o[k]])
  );
  const canonical = JSON.stringify({
    mn: String(manageNumber || '').trim().toLowerCase(),
    expected: norm(request?.expected),
    prices: norm(request?.prices),
  });
  return crypto.createHash('sha256').update(canonical).digest('hex').slice(0, 32);
}

/** 結果を記録する (受領済みの行に追記する。上書きはしない) */
export function completeOperation(db, operationId, state, result) {
  db.prepare(`UPDATE rakuten_price_ops
      SET result_state = ?, result_json = ?, completed_at = ?
      WHERE operation_id = ? AND result_state IS NULL`)
    .run(state, JSON.stringify(result ?? null), new Date().toISOString(), operationId);
}

export function getOperation(db, operationId) {
  return db.prepare('SELECT * FROM rakuten_price_ops WHERE operation_id = ?').get(operationId) || null;
}

/**
 * 同じ operation_id が来たときの扱い。
 *   結果あり  → 前回結果をそのまま返す (実行しない)
 *   結果なし  → 送信済みか分からない。★実行し直さない (二重更新を作らない)
 */
export function replayOf(row) {
  if (row.result_state) {
    return { replay: true, state: row.result_state, result: JSON.parse(row.result_json || 'null') };
  }
  return {
    replay: true, state: 'unknown',
    result: {
      message: '同じ operation_id を受領済みですが結果が残っていません。'
        + '楽天へ送信済みか不明なため実行しません。商品の現在価格を確認してから判断してください。',
      receivedAt: row.received_at,
    },
  };
}
