/**
 * 自社商品バーコードラベル管理 (po_barcode_labels)
 *
 * 業務背景 (中原さん 2026-07-15):
 *   自社商品 (PML 売上分類='1') はアメージングクラフト (AMC) が製造・ラベル印刷するため、
 *   AmazonのFNSKUバーコードを商品ラベルに直接印字してもらう (FBA納品時のラベル貼りを省く)。
 *   過去商品は未印字が多く、発注の都度どの商品が印字済みかを確認して印字依頼している。
 *   旧管理はExcel「BFバーコード管理_既存商品.xlsx」→ CSV取込で移行し、以後このアプリが正本。
 *
 * 対象商品 = PML(+NEオーバーレイ) で 仕入先=AMC かつ 売上分類='1' かつ 商品区分≠'セット'。
 *   po_barcode_labels に行が無い対象商品 = 「未登録」(旧Excel管理漏れ相当)。
 *   行はあるが対象から外れた商品 = orphan (理由付きで返す。取扱中止は対象のまま=ラベル情報は生きている)
 */
import { createHash } from 'crypto';
import { getDB, normSupplierCode, normProductCode } from './db.js';
import { loadPmlMerged, loadPmlMergedFor } from './logic.js';
import { getSetting, audit } from './ledger.js';

const nowIso = () => new Date().toISOString();
const trimS = v => String(v == null ? '' : v).trim();

/** 対象仕入先 (既定=AMC 正規化コード'1')。移設が必要になったら po_settings barcode_target_supplier で変更可 (UIなし) */
export function barcodeTargetSupplier() {
  return normSupplierCode(getSetting('barcode_target_supplier') || '1');
}

export const BARCODE_STATUSES = ['printed', 'requested', 'not_needed', 'unset'];
export const BARCODE_STATUS_LABELS = {
  printed: '設定済み', requested: '依頼中', not_needed: '不要', unset: '未設定',
  unregistered: '未登録', // 仮想状態 (行なし)。DBには保存しない
};

/** バーコード値から種別を推定。FNSKU=X0始まり10桁英数 / JAN=8or13桁数字 / 空=null / その他=unknown */
export function detectBarcodeType(value) {
  const v = trimS(value);
  if (!v) return null;
  if (/^X0[0-9A-Z]{8}$/i.test(v)) return 'fnsku';
  if (/^\d{8}$/.test(v) || /^\d{13}$/.test(v)) return 'jan';
  return 'unknown';
}

/** PML の 1 行が対象か。対象 = null / 対象外 = 理由のキー (set / not_amc / not_own_product)。loadBarcodeTargets と barcodeTargetOf の共通 */
function targetReasonOfRow(r, supplier) {
  if (String(r['商品区分'] || '').trim() === 'セット') return 'set';
  if (normSupplierCode(r['仕入先']) !== supplier) return 'not_amc';
  if (String(r['売上分類'] == null ? '' : r['売上分類']).trim() !== '1') return 'not_own_product';
  return null;
}

/**
 * バーコード管理対象の商品一覧を PML(+NEオーバーレイ) から取得。
 * 返値: { pub, rows (PML全行=orphan理由の算出等で再利用可), targets: Map<product_key, {code, name, active, vendorCode}> }
 * PML未同期 (pub=null) のときは targets=空Map (呼び元で「未同期」と表示する)
 */
export function loadBarcodeTargets() {
  const supplier = barcodeTargetSupplier();
  const { pub, rows } = loadPmlMerged();
  const targets = new Map();
  if (!pub) return { pub: null, rows: [], targets };
  for (const r of rows) {
    if (targetReasonOfRow(r, supplier)) continue;
    const key = normProductCode(r['商品コード']);
    if (!key) continue;
    targets.set(key, {
      code: String(r['商品コード']).trim(),
      name: r['商品名'] || '',
      active: String(r['取扱区分'] || '') === '取扱中',
    });
  }
  // 対応表の先方管理番号 (AMCへの印字依頼で使う識別子)
  const db = getDB();
  for (const m of db.prepare('SELECT product_key, vendor_code FROM po_vendor_code_map WHERE supplier_code=?').all(supplier)) {
    const t = targets.get(m.product_key);
    if (t) t.vendorCode = m.vendor_code;
  }
  return { pub, rows, targets };
}

function rowToLabel(r) {
  return {
    key: r.product_key, code: r.product_code, name: r.product_name || '',
    status: r.status, barcodeValue: r.barcode_value || '', barcodeType: r.barcode_type || null,
    vendorCodeHint: r.vendor_code_hint || '', note: r.note || '',
    source: r.source, version: r.version, updatedAt: r.updated_at, updatedBy: r.updated_by || '',
  };
}

/** 全ラベル行を Map<product_key, label> で */
export function loadLabelMap() {
  const db = getDB();
  const m = new Map();
  for (const r of db.prepare('SELECT * FROM po_barcode_labels').all()) m.set(r.product_key, rowToLabel(r));
  return m;
}

/**
 * 管理画面用の一覧。
 * targets: 対象商品全件 (label=null なら未登録) / orphans: ラベル行はあるが対象外 (理由付き)
 */
export function listBarcodeOverview() {
  const { pub, rows: pmlRows, targets } = loadBarcodeTargets();
  const labels = loadLabelMap();
  const supplier = barcodeTargetSupplier();
  const rows = [];
  for (const [key, t] of targets) {
    rows.push({ key, code: t.code, name: t.name, active: t.active, vendorCode: t.vendorCode || null, label: labels.get(key) || null });
  }
  // 表示順: 未登録→未設定→依頼中→設定済み→不要 (対応が必要な順)、同状態内は商品コード順
  const rank = { unregistered: 0, unset: 1, requested: 2, printed: 3, not_needed: 4 };
  rows.sort((a, b) => (rank[a.label ? a.label.status : 'unregistered'] - rank[b.label ? b.label.status : 'unregistered'])
    || a.code.localeCompare(b.code));
  // orphan = ラベル行があるが今の対象定義から外れた商品 (理由を明示、Codex設計相談)。
  // PML行は loadBarcodeTargets で読んだものを再利用 (二重読み回避、Codex R1 Low)
  const orphans = [];
  if (pub) {
    const pmlByKey = new Map();
    for (const r of pmlRows) pmlByKey.set(normProductCode(r['商品コード']), r);
    for (const [key, label] of labels) {
      if (targets.has(key)) continue;
      const r = pmlByKey.get(key);
      let reason = 'missing_from_pml';
      if (r) {
        if (String(r['商品区分'] || '').trim() === 'セット') reason = 'became_set';
        else if (normSupplierCode(r['仕入先']) !== supplier) reason = 'supplier_changed';
        else reason = 'sales_class_changed';
      }
      orphans.push({ ...label, orphanReason: reason });
    }
    orphans.sort((a, b) => a.code.localeCompare(b.code));
  }
  const summary = { unregistered: 0, unset: 0, requested: 0, printed: 0, not_needed: 0, orphans: orphans.length, targets: rows.length };
  for (const r of rows) summary[r.label ? r.label.status : 'unregistered']++;
  return { pmlSynced: !!pub, supplierCode: supplier, targets: rows, orphans, summary };
}

/**
 * 1件更新 (upsert + 楽観ロック + 監査イベント)。
 *   version=null → 新規作成 (既に行があれば409)
 *   version=数値 → 一致時のみ更新 (不一致=他画面で更新済み → 409。currentを返すので画面で再読込)
 * 返値: { label } / 競合時は Error に status=409 と current を付けて throw
 */
export function upsertLabel({ productKey, productCode, status, barcodeValue, note, version, actor, source = 'manual' }) {
  const key = normProductCode(productKey);
  if (!key) throw new Error('商品コードが空です');
  if (!BARCODE_STATUSES.includes(status)) throw new Error(`状態が不正です: ${status}`);
  const value = trimS(barcodeValue);
  if (value.length > 64) throw new Error('バーコード値が長すぎます (64文字まで)');
  const noteV = trimS(note);
  if (noteV.length > 500) throw new Error('備考が長すぎます (500文字まで)');
  const db = getDB();
  return db.transaction(() => {
    const cur = db.prepare('SELECT * FROM po_barcode_labels WHERE product_key=?').get(key);
    const now = nowIso();
    if (version == null) {
      if (cur) { const e = new Error('この商品は既に登録されています (画面が古い可能性)。再読込してください'); e.status = 409; e.current = rowToLabel(cur); throw e; }
      db.prepare(`INSERT INTO po_barcode_labels
        (product_key, product_code, product_name, status, barcode_value, barcode_type, vendor_code_hint, note, source, version, created_at, updated_at, updated_by)
        VALUES (?,?,?,?,?,?,?,?,?,1,?,?,?)`)
        .run(key, trimS(productCode) || key, null, status, value || null, detectBarcodeType(value), null, noteV || null, source, now, now, actor || null);
    } else {
      const v = Number(version);
      if (!Number.isSafeInteger(v) || v < 1) throw new Error('versionが不正です');
      if (!cur) { const e = new Error('この商品の登録が見つかりません (削除された可能性)。再読込してください'); e.status = 409; e.current = null; throw e; }
      const r = db.prepare(`UPDATE po_barcode_labels
        SET status=?, barcode_value=?, barcode_type=?, note=?, source=?, version=version+1, updated_at=?, updated_by=?
        WHERE product_key=? AND version=?`)
        .run(status, value || null, detectBarcodeType(value), noteV || null, source, now, actor || null, key, v);
      if (r.changes === 0) {
        const latest = db.prepare('SELECT * FROM po_barcode_labels WHERE product_key=?').get(key);
        const e = new Error('他の画面で先に更新されています。最新の状態を確認してからやり直してください');
        e.status = 409; e.current = latest ? rowToLabel(latest) : null;
        throw e;
      }
    }
    db.prepare(`INSERT INTO po_barcode_label_events (product_key, at, actor, source, from_status, to_status, barcode_value, note)
      VALUES (?,?,?,?,?,?,?,?)`)
      .run(key, now, actor || null, source, cur ? cur.status : null, status, value || null, noteV || null);
    return { label: rowToLabel(db.prepare('SELECT * FROM po_barcode_labels WHERE product_key=?').get(key)) };
  })();
}

// ─── 1 商品だけ (マスタの入力の商品の画面・Company DB構想 21 A1) ───

/** 対象外の理由 (画面の言葉)。対象の決め方は loadBarcodeTargets と同じ (商品管理リスト + NE オーバーレイ) */
export const BARCODE_TARGET_REASONS = Object.freeze({
  pml_not_synced: '商品管理リストがまだ同期されていないので、対象か分かりません',
  missing_from_pml: '商品管理リストにまだ出ていません (NE に登録した翌朝から対象になります)',
  set: 'セットです (ラベルは中身の単品ごと)',
  not_amc: '仕入先が AMC (アメージングクラフト) ではありません',
  not_own_product: '自社商品 (売上分類 1) ではありません',
});
/** どの画面から書いたか (po_audit_log の via)。発注アプリのマスタ管理のポップオーバーは今までどおり events だけ (記録の形を変えない) */
export const BARCODE_VIA = Object.freeze({ 'master-edit:sku': 'マスタの入力 (商品の画面)' });

/**
 * 1 商品の対象の判定と今の行。対象の決め方は発注アプリと同じ (loadBarcodeTargets)。
 * { pmlSynced, target, reason (対象外の理由のキー・対象 = null), code, name, vendorCode, label (今の行・無ければ null) }
 */
export function barcodeTargetOf(productCode) {
  // 商品管理リスト全体は読まない = その商品の行だけ (公開の世代 + NE オーバーレイ・loadPmlMergedFor)。決め方は loadBarcodeTargets と同じ (targetReasonOfRow)
  const key = normProductCode(productCode);
  const db = getDB();
  const supplier = barcodeTargetSupplier();
  const { pub, rows } = loadPmlMergedFor(key);
  const cur = key ? db.prepare('SELECT * FROM po_barcode_labels WHERE product_key=?').get(key) : null;
  const label = cur ? rowToLabel(cur) : null;
  if (!pub) return { pmlSynced: false, target: false, reason: 'pml_not_synced', code: trimS(productCode), name: '', vendorCode: null, label };
  // 重なり (大文字小文字だけ違う行) があっても loadBarcodeTargets と同じ = どれかが対象なら対象 (後の行の名前)
  const hits = rows.filter((r) => !targetReasonOfRow(r, supplier));
  if (hits.length) {
    const t = hits[hits.length - 1];
    const m = db.prepare('SELECT vendor_code FROM po_vendor_code_map WHERE supplier_code=? AND product_key=?').get(supplier, key);
    return { pmlSynced: true, target: true, reason: null, code: String(t['商品コード']).trim(), name: t['商品名'] || '', vendorCode: m ? m.vendor_code : null, label };
  }
  const r = rows[0];
  return { pmlSynced: true, target: false, reason: r ? targetReasonOfRow(r, supplier) : 'missing_from_pml', code: r ? String(r['商品コード']).trim() : trimS(productCode), name: r ? r['商品名'] || '' : '', vendorCode: null, label };
}

const labelCols = (l) => (l ? { status: l.status, barcode_value: l.barcodeValue || null, barcode_type: l.barcodeType || null, note: l.note || null, version: l.version } : null);

/**
 * マスタの入力から 1 件書く = upsertLabel (version の楽観ロック・events) + po_audit_log (だれ・どの画面・前と後・値の出どころ) を 1 つの即時の取引で。
 *   対象かどうかも同じ取引の中で確かめ直す (対象外 = 409 not_target・何も書かない)。
 *   pick = 値の出どころ (呼び手が Company DB で確かめた候補・手入力の理由・今の値のまま・空)。記録にそのまま残す
 * 誤り = Error (status 409 = 版が違う・対象外 / それ以外 = 400)。e.reason・e.current
 */
export function writeLabelAudited({ productCode, status, barcodeValue, note, version, actor, via, requestId = null, pick = null }) {
  if (!Object.hasOwn(BARCODE_VIA, String(via || ''))) throw Object.assign(new Error(`書いた画面 (via) が分からない: ${via}`), { status: 400, reason: 'invalid_input' });
  const db = getDB();
  return db.transaction(() => {
    const t = barcodeTargetOf(productCode);
    if (!t.target) {
      throw Object.assign(new Error(`AMC 商品ラベルの対象ではありません: ${BARCODE_TARGET_REASONS[t.reason] || t.reason}`), { status: 409, reason: 'not_target', target_reason: t.reason, current: t.label });
    }
    const before = t.label;
    let r;
    try {
      r = upsertLabel({ productKey: t.code, productCode: t.code, status, barcodeValue, note, version, actor, source: 'manual' });
    } catch (e) {
      if (e.status === 409) e.reason = 'stale';
      else { e.status = 400; e.reason = e.reason || 'invalid_input'; }
      throw e;
    }
    audit(db, { actorType: 'user', actor: actor || null, action: 'barcode_label_write', resource: `barcode:${normProductCode(t.code)}`, requestId,
      detail: { via, code: t.code, before: labelCols(before), after: labelCols(r.label), pick } });
    return { label: r.label, before };
  }).immediate();
}

/** 戻したときのメモの印 (前に行が無かった = 「未設定 + 要確認」の行を残す) */
export const RECHECK_NOTE = '要確認: マスタの入力の保存を、書いた後の確かめで戻しました';

/**
 * 書いた後の読み直しで Company DB の値が変わっていた = その書き込みを戻す (要確認)。2 つの DB をまたぐ取引は作らない代わり (Codex #1701 R1 H2)。
 *   version = 書いた後の版 (その後にだれかが直していたら戻さない = 409 のまま記録だけ)。
 *   before = 書く前の行 (無かった = null → 状態「未設定」・値なし・メモ = RECHECK_NOTE の行にする = 依頼中 / 印字済みのまま残さない)。
 *   po_audit_log に barcode_label_revert (戻した) / barcode_label_revert_failed (版が違って戻せなかった) と理由を残す。
 * 返値 = { reverted, label (今の行) }
 */
export function revertLabelAudited({ productCode, version, before, actor, via, requestId = null, why, detail = null }) {
  if (!Object.hasOwn(BARCODE_VIA, String(via || ''))) throw Object.assign(new Error(`書いた画面 (via) が分からない: ${via}`), { status: 400, reason: 'invalid_input' });
  const db = getDB();
  const key = normProductCode(productCode);
  return db.transaction(() => {
    const cur = db.prepare('SELECT * FROM po_barcode_labels WHERE product_key=?').get(key);
    const to = before ? { status: before.status, value: before.barcodeValue || '', note: before.note || '' } : { status: 'unset', value: '', note: RECHECK_NOTE };
    let r = null;
    try {
      r = upsertLabel({ productKey: key, productCode: cur ? cur.product_code : productCode, status: to.status, barcodeValue: to.value, note: to.note, version, actor, source: 'manual' });
    } catch (e) {
      if (e.status !== 409) throw e;
    }
    const after = r ? r.label : (cur ? rowToLabel(db.prepare('SELECT * FROM po_barcode_labels WHERE product_key=?').get(key)) : null);
    audit(db, { actorType: 'user', actor: actor || null, action: r ? 'barcode_label_revert' : 'barcode_label_revert_failed', resource: `barcode:${key}`, requestId,
      detail: { via, code: productCode, why, recheck: detail, before: labelCols(cur ? rowToLabel(cur) : null), after: labelCols(after) } });
    return { reverted: !!r, label: after };
  }).immediate();
}

/** 状態変更履歴 */
export function listLabelEvents(productKey, limit = 50) {
  const db = getDB();
  return db.prepare('SELECT * FROM po_barcode_label_events WHERE product_key=? ORDER BY id DESC LIMIT ?')
    .all(normProductCode(productKey), limit);
}

// ─── CSV取込 (旧Excel移行用。upsert方式 — CSVに無い行は消さない・手動編集の上書きは件数と一覧で警告) ───

const STATUS_JA = new Map([
  ['設定済み', 'printed'], ['設定済', 'printed'],
  ['依頼中', 'requested'],
  ['不要', 'not_needed'], ['入れる必要なし', 'not_needed'],
  ['未設定', 'unset'], ['', 'unset'],
]);

/**
 * バーコード管理CSVの取込。列: 商品コード(必須), 状態(必須: 設定済み/依頼中/不要/未設定),
 * バーコード値(任意。FNSKU/JAN), 備考(任意), AMC管理番号(任意=vendor_code_hint)
 * commit=false: プレビュー (書込なし、fileHash+stateHash返却)
 * commit=true: fileHash (同一ファイル) と stateHash (プレビュー時のDB状態=対象行のversion集合) の
 *   両方の一致を要求。プレビュー後に誰かがポップオーバーで手動更新していたら stateHash が変わり409
 *   → プレビューからやり直し (CSV取込に楽観ロックを迂回させない、Codex R1 High)
 */
export function importBarcodeCsv({ rows, fileHash, commit, expectedHash, expectedStateHash, actor }) {
  if (rows.length < 2) throw new Error('データ行がありません');
  const header = rows[0].map(s => trimS(s));
  const iCode = header.indexOf('商品コード');
  const iStatus = header.indexOf('状態');
  if (iCode === -1 || iStatus === -1) {
    throw new Error('見出し「商品コード」「状態」が必要です (バーコードラベル取込用CSVを入れてください)');
  }
  const iValue = header.findIndex(h => h === 'バーコード値' || h === 'バーコード' || h === 'FNSKU' || h === 'FNSKUorJAN');
  const iNote = header.indexOf('備考');
  const iVendor = header.findIndex(h => h === 'AMC管理番号' || h === 'AMC様商品コード' || h === '先方管理番号');
  const db = getDB();

  // 現在のDB状態を読み、CSVとの差分計画+stateHash (対象行のversion集合) を作る。
  // commit時はこの関数をトランザクション内で再実行する — 読取り・照合・書込みが同一スナップショットになり、
  // 「commit読取り直後に別接続が書く」隙間がない (same行スキップのすり抜け防止、Codex R2 High)
  const buildPlan = () => {
    const existing = new Map();
    for (const r of db.prepare('SELECT * FROM po_barcode_labels').all()) existing.set(r.product_key, r);
    const errors = [];
    const seen = new Set();
    const plan = []; // { key, code, status, value, note, vendorHint, action: 'new'|'update'|'same', manualOverwrite, baseVersion }
    for (let n = 1; n < rows.length; n++) {
      const code = trimS(rows[n][iCode]);
      if (!code) continue;
      const key = normProductCode(code);
      if (seen.has(key)) { errors.push(`行${n + 1}: 商品コード ${code} がCSV内で重複しています`); continue; }
      seen.add(key);
      const stRaw = trimS(rows[n][iStatus]);
      const status = STATUS_JA.get(stRaw);
      if (!status) { errors.push(`行${n + 1} (${code}): 状態「${stRaw}」が不正です (設定済み/依頼中/不要/未設定)`); continue; }
      const value = iValue === -1 ? '' : trimS(rows[n][iValue]);
      if (value.length > 64) { errors.push(`行${n + 1} (${code}): バーコード値が長すぎます (64文字まで)`); continue; }
      const note = iNote === -1 ? '' : trimS(rows[n][iNote]);
      // 黙って切り捨てない (プレビュー集計だけ見てcommitすると末尾が失われる、Codex R1 Medium)
      if (note.length > 500) { errors.push(`行${n + 1} (${code}): 備考が長すぎます (500文字まで)`); continue; }
      const vendorHint = iVendor === -1 ? '' : trimS(rows[n][iVendor]);
      const cur = existing.get(key);
      let action = 'new';
      if (cur) {
        const sameCore = cur.status === status && trimS(cur.barcode_value) === value && trimS(cur.note) === note
          && trimS(cur.vendor_code_hint) === vendorHint;
        action = sameCore ? 'same' : 'update';
      }
      plan.push({
        key, code, status, value, note, vendorHint, action,
        // 手動編集済み行の上書きは目視確認対象 (CSVは移行時の一括投入用。画面編集が新しい可能性)
        manualOverwrite: !!(cur && cur.source === 'manual' && action === 'update'),
        fromStatus: cur ? cur.status : null,
        baseVersion: cur ? cur.version : 0, // 0=未登録
      });
    }
    if (errors.length) throw new Error(`CSVにエラーがあります (${errors.length}件):\n` + errors.slice(0, 10).join('\n') + (errors.length > 10 ? '\n…' : ''));
    const counts = {
      total: plan.length,
      new: plan.filter(p => p.action === 'new').length,
      update: plan.filter(p => p.action === 'update').length,
      same: plan.filter(p => p.action === 'same').length,
      manualOverwrite: plan.filter(p => p.manualOverwrite).length,
    };
    const manualList = plan.filter(p => p.manualOverwrite).slice(0, 20)
      .map(p => `${p.code}: ${BARCODE_STATUS_LABELS[p.fromStatus]}→${BARCODE_STATUS_LABELS[p.status]}`);
    // stateHash = CSV対象行の現在version集合。プレビュー後にどれか1行でも手動更新されたら変わる
    const stateHash = createHash('sha256')
      .update(plan.map(p => `${p.key}:${p.baseVersion}`).sort().join('|')).digest('hex');
    return { plan, counts, manualList, stateHash };
  };

  if (!commit) {
    const { plan, counts, manualList, stateHash } = buildPlan();
    return { committed: false, fileHash, stateHash, counts, manualList,
      statusCounts: { printed: plan.filter(p => p.status === 'printed').length, requested: plan.filter(p => p.status === 'requested').length,
        not_needed: plan.filter(p => p.status === 'not_needed').length, unset: plan.filter(p => p.status === 'unset').length } };
  }
  if (!expectedHash || expectedHash !== fileHash) {
    throw new Error('プレビューしたCSVと内容が異なります。もう一度プレビューからやり直してください');
  }
  // 書込み前提の再読取り→stateHash照合→書込みを IMMEDIATE トランザクションで一体化
  // (deferredだと読取り中は共有ロックのままで、照合と書込みの間に他接続の書込みが挟まり得る)
  return db.transaction(() => {
    const { plan, counts, manualList, stateHash } = buildPlan();
    if (!expectedStateHash || expectedStateHash !== stateHash) {
      const e = new Error('プレビュー後に対象商品の登録が別の画面で更新されています。もう一度プレビューして内容を確認してからやり直してください');
      e.status = 409;
      throw e;
    }
    if (counts.new + counts.update === 0) throw new Error('取り込む変更が0件です (すべて登録済みと同一)');
    const now = nowIso();
    const ins = db.prepare(`INSERT INTO po_barcode_labels
      (product_key, product_code, product_name, status, barcode_value, barcode_type, vendor_code_hint, note, source, version, created_at, updated_at, updated_by)
      VALUES (?,?,?,?,?,?,?,?,'csv',1,?,?,?)`);
    // UPDATEは読取り時versionを条件に (同一txn内なので必ず一致するはず。防御的二重化)
    const upd = db.prepare(`UPDATE po_barcode_labels
      SET product_code=?, status=?, barcode_value=?, barcode_type=?, vendor_code_hint=?, note=?, source='csv', version=version+1, updated_at=?, updated_by=?
      WHERE product_key=? AND version=?`);
    const ev = db.prepare(`INSERT INTO po_barcode_label_events (product_key, at, actor, source, from_status, to_status, barcode_value, note)
      VALUES (?,?,?,'csv',?,?,?,?)`);
    for (const p of plan) {
      if (p.action === 'same') continue;
      if (p.action === 'new') {
        ins.run(p.key, p.code, null, p.status, p.value || null, detectBarcodeType(p.value), p.vendorHint || null, p.note || null, now, now, actor || null);
      } else {
        const r = upd.run(p.code, p.status, p.value || null, detectBarcodeType(p.value), p.vendorHint || null, p.note || null, now, actor || null, p.key, p.baseVersion);
        if (r.changes === 0) {
          const e = new Error(`取込中に ${p.code} が別の画面で更新されました。全件取り消しました — プレビューからやり直してください`);
          e.status = 409;
          throw e; // txn全体をロールバック
        }
      }
      ev.run(p.key, now, actor || null, p.fromStatus, p.status, p.value || null, p.note || null);
    }
    return { committed: true, fileHash, stateHash, counts, manualList };
  }).immediate();
}

/**
 * 発注残明細・ワークスペース商品への barcode 添付用データ。
 * 返値: { has(key), get(key) } — 対象商品なら {status, value, note, version} (未登録は status='unregistered')、対象外なら null
 */
export function barcodeAttacher() {
  const { pub, targets } = loadBarcodeTargets();
  const labels = pub ? loadLabelMap() : new Map();
  return {
    pmlSynced: !!pub,
    of(productKey) {
      const key = normProductCode(productKey);
      if (!pub || !targets.has(key)) return null;
      const l = labels.get(key);
      return l
        ? { status: l.status, value: l.barcodeValue || '', note: l.note || '', version: l.version, code: l.code }
        : { status: 'unregistered', value: '', note: '', version: null, code: targets.get(key).code };
    },
  };
}
