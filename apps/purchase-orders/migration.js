/**
 * purchase-orders NE発注残 初期取込 (移行)
 *
 * NE (ネクストエンジン) の発注残エクスポートCSV (発注伝票番号/商品コード/発注数/注残計/発行日/仕入先cd) を
 * 読み、NE伝票ごとに移行PO (origin='migration', send_blocked=1, ne_slip_number=NE伝票番号) を issued で作成する。
 *
 *  - 対象はオープン明細のみ (注残計>0)。注残0の行は全行検証 (伝票属性整合・重複) を通した後にスキップ
 *    (発注残エクスポートに通常含まれない防御。完了済み明細の履歴移行はしない — 履歴の正本はNE側に残る)
 *  - 明細 qty = NEの発注数。取込前の入庫済分 (発注数−注残計) は source='migration' の入荷イベントで
 *    消し込み、取込直後の残数 = NEの注残計 に一致させる (以後の消込はアプリの入庫取込/手動イベントで行う)
 *  - 冪等性: ne_slip_number の UNIQUE + BEGIN IMMEDIATE 内での取込済み判定 (取込済み伝票はスキップ報告。
 *    同じCSVの再取込・同時取込でも二重登録にならない)
 *  - commit=false はプレビューのみ (書込なし)。プレビュー応答の fileHash (CSVのSHA-256) を確定時に
 *    expectedHash として要求し、プレビュー後にファイルが差し替わっていたら拒否する
 *  - NEの発行日は issued_at にできない (移行境界より過去だと legacy 扱いになりイベント登録不可) ため、
 *    issued_at=取込時刻とし、NE発行日・伝票番号は note に残す
 *  - 移行POは発注済み扱いの二重カウントを避けるため、発注提案の「発注済み」バッジ (loadRecentIssued) と
 *    月次上限集計から除外される (数量はNE由来でPMLの注残数に反映済み)
 *  - origin/ne_slip_number/send_blocked の組合せは db.js のトリガ (trg_po_orders_migration_attrs_*) と
 *    checkLedgerIntegrity が保証する
 */
import { createHash } from 'crypto';
import { getDB, normSupplierCode, normProductCode } from './db.js';
import { decodeCsvBuffer, parseCsv } from './inbound.js';
import { loadPmlMerged } from './logic.js';
import { ensureTrackingStarted, nextPoNumber, appendPoItemEvent, audit, jstToday, isYmd } from './ledger.js';

const nowIso = () => new Date().toISOString();
const trimS = v => String(v == null ? '' : v).trim();

/** NEの日付表記 (YYYY/M/D・YYYY-MM-DD) → YYYY-MM-DD。不正なら null */
function toYmd(s) {
  const m = trimS(s).match(/^(\d{4})[/-](\d{1,2})[/-](\d{1,2})$/);
  if (!m) return null;
  const d = `${m[1]}-${m[2].padStart(2, '0')}-${m[3].padStart(2, '0')}`;
  return isYmd(d) ? d : null;
}

const MAX_ROWS = 5000;

/** CSVを解析して伝票単位のグループに正規化する (構造エラーは全体拒否) */
function parseNeCsv(buffer) {
  const rows = parseCsv(decodeCsvBuffer(buffer));
  if (rows.length < 2) throw new Error('データ行がありません');
  if (rows.length - 1 > MAX_ROWS) throw new Error(`明細が多すぎます (${rows.length - 1}行 > 上限${MAX_ROWS})`);
  const header = rows[0].map(s => trimS(s).replace(/^\uFEFF/, ''));
  const idx = {};
  for (const col of ['発注伝票番号', '商品コード', '商品名', '発注数', '注残計', '発行日', '仕入先cd']) {
    idx[col] = header.indexOf(col);
    if (idx[col] === -1) throw new Error(`NE発注残CSVの見出しが見つかりません: ${col} (NEの発注残エクスポートCSVをそのまま入れてください)`);
  }
  const iDue = header.indexOf('予定納期');

  const errors = [];
  const warnings = [];
  const slips = new Map(); // slip → { slip, supplier, neDate, items[], seenKeys }
  for (let n = 1; n < rows.length; n++) {
    const r = rows[n];
    const line = `行${n + 1}`;
    const slip = trimS(r[idx['発注伝票番号']]);
    const code = trimS(r[idx['商品コード']]);
    const name = trimS(r[idx['商品名']]);
    const qty = Number(trimS(r[idx['発注数']]).replace(/,/g, ''));
    const rem = Number(trimS(r[idx['注残計']]).replace(/,/g, ''));
    const neDate = toYmd(r[idx['発行日']]);
    const supplier = normSupplierCode(r[idx['仕入先cd']]);
    if (!slip) { errors.push(`${line}: 発注伝票番号が空です`); continue; }
    if (!code) { errors.push(`${line}: 商品コードが空です`); continue; }
    if (!Number.isInteger(qty) || qty <= 0) { errors.push(`${line} (${code}): 発注数が正の整数ではありません: ${trimS(r[idx['発注数']])}`); continue; }
    if (!Number.isInteger(rem) || rem < 0) { errors.push(`${line} (${code}): 注残計が0以上の整数ではありません: ${trimS(r[idx['注残計']])}`); continue; }
    if (rem > qty) { errors.push(`${line} (${code}): 注残計${rem}が発注数${qty}を超えています`); continue; }
    if (!neDate) { errors.push(`${line} (${code}): 発行日が日付ではありません: ${trimS(r[idx['発行日']])}`); continue; }
    if (!supplier) { errors.push(`${line} (${code}): 仕入先cdが空です`); continue; }
    let due = null;
    if (iDue !== -1 && trimS(r[iDue])) {
      due = toYmd(r[iDue]);
      if (!due) { errors.push(`${line} (${code}): 予定納期が日付ではありません: ${trimS(r[iDue])}`); continue; }
    }
    // 伝票ヘッダ属性 (仕入先・発行日) の整合と明細重複は、注残0でスキップする行も含めて全行検証する
    let g = slips.get(slip);
    if (!g) { g = { slip, supplier, neDate, items: [], seenKeys: new Set() }; slips.set(slip, g); }
    if (g.supplier !== supplier) { errors.push(`伝票${slip}: 仕入先cdが混在しています (${g.supplier} / ${supplier})`); continue; }
    if (g.neDate !== neDate) { errors.push(`伝票${slip}: 発行日が混在しています (${g.neDate} / ${neDate})`); continue; }
    const key = normProductCode(code);
    if (g.seenKeys.has(key)) { errors.push(`伝票${slip}: 商品コードが重複しています: ${code}`); continue; }
    g.seenKeys.add(key);
    if (rem === 0) { warnings.push(`${line} (${code}): 注残0のためスキップ (オープン明細のみ移行)`); continue; }
    g.items.push({ code, key, name, qty, rem, due });
  }
  if (errors.length) {
    throw new Error(`取込を中止しました (${errors.length}件):\n` + errors.slice(0, 10).join('\n') + (errors.length > 10 ? `\n…他${errors.length - 10}件` : ''));
  }
  // 全明細が注残0だった伝票は対象外
  const openSlips = [...slips.values()].filter(g => g.items.length > 0).map(({ seenKeys, ...g }) => g);
  if (!openSlips.length) throw new Error('取込対象の伝票がありません' + (warnings.length ? ` (注残0スキップ ${warnings.length}件)` : ''));
  return { slips: openSlips, warnings };
}

/**
 * 現在のDB状態で取込計画を確定する (commit時は BEGIN IMMEDIATE 内から呼ばれ、ロック取得後の状態が正)。
 * @returns {plan, skipped} plan=作成対象、skipped=取込済み伝票
 * @throws 仕入先未登録・draft競合・取込済み伝票との仕入先不一致
 */
function buildPlan(db, slips, warnings) {
  const errors = [];

  // 仕入先マスタ必須 (仕入先名の解決元。未登録なら先にマスタ登録してもらう)
  const supplierNames = new Map();
  for (const s of new Set(slips.map(g => g.supplier))) {
    const row = db.prepare('SELECT name FROM po_suppliers WHERE supplier_code=?').get(s);
    if (!row) errors.push(`仕入先コード ${s} がマスタ未登録です (マスタ管理→仕入先 で登録してから取り込んでください)`);
    else supplierNames.set(s, row.name);
  }
  if (errors.length) throw new Error(errors.join('\n'));

  // 取込済み伝票 (ne_slip_number 一致) はスキップ。仕入先まで違う場合はデータ異常として拒否
  const exists = db.prepare('SELECT po_number, supplier_code FROM po_orders WHERE ne_slip_number=?');
  const skipped = [];
  const todo = [];
  for (const g of slips) {
    const ex = exists.get(g.slip);
    if (!ex) { todo.push(g); continue; }
    if (normSupplierCode(ex.supplier_code) !== g.supplier) {
      errors.push(`伝票${g.slip}: 取込済みPO (${ex.po_number}) と仕入先が一致しません (取込済:${ex.supplier_code} / CSV:${g.supplier})`);
    } else {
      skipped.push({ slip: g.slip, poNumber: ex.po_number });
    }
  }
  if (errors.length) throw new Error(errors.join('\n'));

  // 同一仕入先の作業中draftがあると取込用の一時draftが作れない (仕入先ごとdraft1件のUNIQUE)
  const draftOf = db.prepare("SELECT id FROM po_orders WHERE supplier_code=? AND status='draft'");
  for (const s of new Set(todo.map(g => g.supplier))) {
    if (draftOf.get(s)) errors.push(`仕入先 ${supplierNames.get(s) || s} に作業中の発注draftがあります。先に発注確定するか、ワークスペースでカートを空にして保存してから取り込んでください`);
  }
  if (errors.length) throw new Error(errors.join('\n'));

  // PMLスナップショットから原価・商品名を補完 (PML外の商品はCSVの商品名+単価未設定で取り込む)
  const pmlByKey = new Map();
  try {
    const { rows: pmlRows } = loadPmlMerged();
    for (const r of pmlRows) pmlByKey.set(normProductCode(r['商品コード']), r);
  } catch { /* PML未同期でも移行取込自体は可能 (原価は未設定になる) */ }

  const plan = todo.map(g => ({
    ...g,
    supplierName: supplierNames.get(g.supplier),
    items: g.items.map(it => {
      const p = pmlByKey.get(it.key);
      if (!p) warnings.push(`伝票${g.slip} ${it.code}: PMLに存在しない商品のため単価未設定で取り込みます`);
      else if (normSupplierCode(p['仕入先']) !== g.supplier) warnings.push(`伝票${g.slip} ${it.code}: PML上の仕入先 (${p['仕入先'] || '未設定'}) と伝票の仕入先が一致しません`);
      return { ...it, name: (p && p['商品名']) || it.name, cost: p && p['原価'] != null ? Number(p['原価']) : null };
    }),
  }));
  return { plan, skipped };
}

function summarize(commit, fileHash, plan, skipped, warnings) {
  return {
    commit: !!commit,
    fileHash,
    orders: plan.length,
    items: plan.reduce((s, g) => s + g.items.length, 0),
    totalRemaining: plan.reduce((s, g) => s + g.items.reduce((t, it) => t + it.rem, 0), 0),
    receiptEvents: plan.reduce((s, g) => s + g.items.filter(it => it.qty > it.rem).length, 0),
    skipped,
    warnings,
    slips: plan.map(g => ({
      slip: g.slip, supplier: g.supplier, supplierName: g.supplierName, neDate: g.neDate,
      items: g.items.length,
      remaining: g.items.reduce((t, it) => t + it.rem, 0),
      knownAmount: g.items.reduce((t, it) => t + (it.cost != null ? Math.round(it.cost * it.rem) : 0), 0),
    })),
  };
}

/**
 * NE発注残CSVを取り込む。
 * @param {Buffer} buffer - アップロードされたCSV
 * @param {boolean} commit - false=プレビューのみ (書込なし)
 * @param {string|null} expectedHash - commit時必須。プレビュー応答の fileHash (プレビューしたCSVと同一かの検証)
 * @returns {commit, fileHash, orders, items, totalRemaining, receiptEvents, skipped, warnings, slips, created?}
 * @throws 構造エラー (見出し不足・数値不正・伝票属性混在・仕入先未登録・draft競合・ハッシュ不一致 等) は全体拒否
 */
export function importNeBackorderCsv({ buffer, commit = false, expectedHash = null, actor = null }) {
  const fileHash = createHash('sha256').update(buffer).digest('hex');
  if (commit && expectedHash !== fileHash) {
    throw new Error('プレビューしたCSVと内容が一致しません。ファイルを選び直してもう一度プレビューしてください');
  }
  const { slips, warnings } = parseNeCsv(buffer);
  const db = getDB();

  if (!commit) {
    const { plan, skipped } = buildPlan(db, slips, warnings);
    return summarize(false, fileHash, plan, skipped, warnings);
  }

  // ── 書込 (全伝票を1つの BEGIN IMMEDIATE。取込済み判定・draft判定もロック取得後の状態で行う) ──
  return db.transaction(() => {
    const { plan, skipped } = buildPlan(db, slips, warnings);
    const now = nowIso();
    // 初回発行前の取込でも境界を確定させる (取込POは issued_at=now >= 境界 で tracked になる)
    ensureTrackingStarted(now);
    const insHeader = db.prepare(`INSERT INTO po_orders
      (supplier_code, supplier_name, status, note, pml_as_of_date, created_at, updated_at, origin, send_blocked, ne_slip_number)
      VALUES (?,?,'draft',?,NULL,?,?,'migration',1,?)`);
    const insItem = db.prepare(`INSERT INTO po_order_items
      (order_id, product_code, product_key, product_name, qty, unit_cost, condition_id, requested_date)
      VALUES (?,?,?,?,?,?,?,?)`);
    const toIssued = db.prepare(`UPDATE po_orders SET status='issued', issued_at=?, po_number=?, tracking_mode='tracked', updated_at=? WHERE id=?`);
    const itemIdOf = db.prepare('SELECT id FROM po_order_items WHERE order_id=? AND product_key=?');
    const condOf = db.prepare('SELECT condition_id FROM po_product_attrs WHERE product_key=?');
    const created = [];
    for (const g of plan) {
      // 発行済みPOへの明細INSERTはDBトリガが無条件拒否するため、issueOrder と同様に draft で作ってから issued へ遷移
      const note = `NE移行: 伝票${g.slip} (NE発行日 ${g.neDate})`;
      const info = insHeader.run(g.supplier, g.supplierName, note, now, now, g.slip);
      const orderId = Number(info.lastInsertRowid);
      for (const it of g.items) {
        const a = condOf.get(it.key);
        insItem.run(orderId, it.code, it.key, it.name, it.qty, it.cost, (a && a.condition_id) || null, it.due);
      }
      const poNumber = nextPoNumber(db);
      toIssued.run(now, poNumber, now, orderId);
      // 取込前入庫分の消込 (残数 = NEの注残計 に一致させる)。appendPoItemEvent はネストsavepointとして実行される
      for (const it of g.items.filter(x => x.qty > x.rem)) {
        appendPoItemEvent({
          orderItemId: itemIdOf.get(orderId, it.key).id, eventType: 'receipt', qty: it.qty - it.rem,
          source: 'migration', note: `NE発注残初期取込: 取込前の入庫済分 (NE発注数${it.qty}−注残${it.rem})`,
          effectiveDate: jstToday(), actorType: 'migration', actor,
        });
      }
      created.push({ slip: g.slip, poNumber, orderId });
    }
    audit(db, { actorType: 'user', actor, action: 'ne_backorder_import', resource: 'orders:migration',
      detail: { fileHash, orders: created.length, skipped: skipped.length, warningCount: warnings.length } });
    return { ...summarize(true, fileHash, plan, skipped, warnings), created };
  }).immediate();
}
