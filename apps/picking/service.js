/**
 * picking 業務ロジック — CS03002 (ピッキングリストCSV) の解析と取込。
 *
 * CS03002 = ロジザードZERO カンタン引当 [PS03/FS03_07] 実行時に出力されるCSV。
 *   - Shift-JIS / 全列ダブルクォート / 150列 / 1行 = 出荷伝票 × SKU の明細
 *   - 1ファイル = 1引当実行 = トータルピッキングバッチ番号 (TB…) 1つ
 * 列仕様の正本: AI_reference/システム設計/ピッキング支援システム_要件定義_20260811.md §4
 *
 * 列は名前で引く (ロジザードの列追加・順序変更に耐える)。必須列が無ければ fail-closed。
 */
import crypto from 'node:crypto';
import { parseCsv, decodeCp932 } from '../packing-dispatch/csv.js';
import { getDB, getBatch, getBatchByTbNo, listLines, utcNow, jstToday } from './db.js';
import { suggestPatterns } from './patterns.js';
import { getImageMap, queueEnsureImages } from './images.js';

/** 業務エラー。router が status + message に変換する。 */
export class PkError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

// CS03002 の必須列 (名前で参照する列のみ列挙。他の列は保持しない)
const REQUIRED_COLUMNS = [
  '出荷指示日', 'ブロック略称', 'ロケーション', '商品ID', '商品名', '出荷指示数', '出荷引当数',
  'ピッキングNO', '出荷伝票NO', '荷主出荷NO', 'バーコード',
  '送り状発行ソフト名', '配送方法名', 'トータルピッキングバッチ番号',
];

/**
 * CS03002 のバッファを解析して取込プレビュー用の構造を返す。DB には触れない。
 * qty は常に「出荷引当数」(そのロケーションから取る数)。「出荷指示数」ではない点に注意
 * (指示数は分割引当の各行に明細総数が繰り返されるため、ピッキング数量には使えない)。
 * @returns {{
 *   tbNo, instructDate, invoiceSoft, deliveryMethod, composition,
 *   lines: [{location, block, sku, productName, barcode, qty}],   // 集約済み・ロケ昇順
 *   slipLines: [{slipNo, pickingNo, neSlipNo, sku, qty, location}],
 *   slipCount, totalQty, qtyWarnings: string[], suggestions: string[]
 * }}
 * @throws {PkError} 400 — 形式不正 (列欠落・数量不正・複数TB等)
 */
export function parseCs03002(buffer) {
  if (!buffer || buffer.length === 0) throw new PkError(400, 'empty_file', 'ファイルが空です');
  const state = {};
  const rows = parseCsv(decodeCp932(buffer), state);
  if (state.unclosedQuote) {
    throw new PkError(400, 'broken_csv', 'CSVの引用符が閉じていません (ファイルが壊れている可能性があります)');
  }
  if (rows.length === 0) throw new PkError(400, 'no_rows', 'データ行がありません');

  const header = rows[0];
  const col = {};
  const dupCols = new Set();
  header.forEach((name, i) => {
    if (name in col) dupCols.add(name);
    else col[name] = i;
  });
  const missing = REQUIRED_COLUMNS.filter((name) => !(name in col));
  if (missing.length > 0) {
    throw new PkError(400, 'missing_columns',
      `必須列がありません: ${missing.join(', ')}。CS03002 (ピッキングリストCSV) か確認してください`);
  }
  // 参照する列名が重複していたら、どちらを読むべきか判断できないため fail-closed (Codex R1 low)
  const dupRequired = REQUIRED_COLUMNS.filter((name) => dupCols.has(name));
  if (dupRequired.length > 0) {
    throw new PkError(400, 'duplicate_columns',
      `同名の列が複数あります: ${dupRequired.join(', ')}。CSVの形式が変わった可能性があります`);
  }

  const dataRows = rows.slice(1).filter((r) => !(r.length === 1 && r[0] === ''));
  if (dataRows.length === 0) throw new PkError(400, 'no_rows', 'データ行がありません');
  const badRows = [];
  dataRows.forEach((r, i) => { if (r.length !== header.length) badRows.push(i + 2); });
  if (badRows.length > 0) {
    throw new PkError(400, 'ragged_rows',
      `列数が揃わない行があります (行 ${badRows.slice(0, 5).join(', ')}${badRows.length > 5 ? ' …' : ''})`);
  }

  const slipLines = [];
  for (const [i, r] of dataRows.entries()) {
    const rowNo = i + 2;
    const get = (name) => (r[col[name]] ?? '').trim();
    const instructQty = Number(get('出荷指示数'));
    if (!Number.isInteger(instructQty) || instructQty <= 0) {
      throw new PkError(400, 'bad_qty', `行${rowNo}: 出荷指示数「${get('出荷指示数')}」が正の整数ではありません`);
    }
    // ピッキングで取る数 = 出荷引当数 (そのロケーションからの引当数)。
    // 出荷指示数は伝票明細のトータルで、1明細が複数ロケに分割引当されると
    // 各行に同じ総数が繰り返される (実測 2026-08-20 出荷_11: 9個が8+1に分割され、
    // 指示数は両行とも9 → 指示数を使うと各ロケで取りすぎる)
    const qty = Number(get('出荷引当数'));
    if (!Number.isInteger(qty) || qty <= 0) {
      throw new PkError(400, 'bad_alloc_qty', `行${rowNo}: 出荷引当数「${get('出荷引当数')}」が正の整数ではありません`);
    }
    if (!get('ロケーション')) throw new PkError(400, 'no_location', `行${rowNo}: ロケーションが空です`);
    if (!get('商品ID')) throw new PkError(400, 'no_sku', `行${rowNo}: 商品IDが空です`);
    if (!get('出荷伝票NO')) throw new PkError(400, 'no_slip', `行${rowNo}: 出荷伝票NOが空です`);
    // TBが空の行を黙って有効TBに混ぜると、別引当・壊れ行の数量を取り込んでしまう (Codex R1 high)
    if (!get('トータルピッキングバッチ番号')) {
      throw new PkError(400, 'no_tb_no', `行${rowNo}: トータルピッキングバッチ番号が空です`);
    }
    slipLines.push({
      tbNo: get('トータルピッキングバッチ番号'),
      instructDate: get('出荷指示日'),
      block: get('ブロック略称'),
      location: get('ロケーション'),
      instructQty,                          // 出荷指示数 (伝票明細の総数。クロスチェック用)
      instructLineNo: get('出荷指示行NO'),  // 分割行を同一明細にまとめるキー (任意列)
      sku: get('商品ID'),
      productName: get('商品名'),
      barcode: get('バーコード'),
      qty,
      pickingNo: get('ピッキングNO'),
      slipNo: get('出荷伝票NO'),
      neSlipNo: get('荷主出荷NO'),
      invoiceSoft: get('送り状発行ソフト名'),
      deliveryMethod: get('配送方法名'),
    });
  }

  // 1ファイル = 1引当 = 1バッチ。TB番号は1回の引当でも複数振られることがある
  // (実測 2026-08-12: 出荷_01=2個・別の引当=13個。昨日のサンプル1個は偶然)。
  // そのためTB単体ではなく「ソート済みTB一覧の組」をバッチの識別キーにする
  // (同じ引当のCSVなら常に同じ組になる。順序はソートで正規化)
  const tbNos = [...new Set(slipLines.map((l) => l.tbNo))].sort();

  // 数量クロスチェック: 同一明細 (伝票×SKU×出荷指示行NO) の出荷引当数の合計は
  // 出荷指示数と一致するはず (分割引当 8+1=9 で実測確認 2026-08-20)。
  // ズレは列解釈の変化・部分引当の兆候なので、取込は止めず警告として返す。
  // 出荷指示行NO が無い (列ごと欠落 or 値が空) 行は、同一伝票×SKUの別明細と
  // 分割行を区別できず誤警告になるためチェック対象にしない (Codex R1 medium)
  const QTY_WARNINGS_MAX = 20;   // ログ・応答・画面に載せるため上限を置く (Codex R1 low)
  const qtyWarnings = [];
  let qtyWarningCount = 0;       // 実件数 (qtyWarnings は上限で省略される — Codex R2 low)
  {
    const byInstruct = new Map();
    for (const l of slipLines) {
      if (!l.instructLineNo) continue;
      const key = `${l.slipNo}\u0000${l.sku}\u0000${l.instructLineNo}`;
      if (!byInstruct.has(key)) byInstruct.set(key, []);
      byInstruct.get(key).push(l);
    }
    for (const group of byInstruct.values()) {
      const instructSet = [...new Set(group.map((l) => l.instructQty))];
      const allocSum = group.reduce((s, l) => s + l.qty, 0);
      if (instructSet.length !== 1 || allocSum !== instructSet[0]) {
        const g = group[0];
        qtyWarningCount++;
        if (qtyWarnings.length < QTY_WARNINGS_MAX) {
          qtyWarnings.push(`${g.slipNo} × ${g.sku}: 出荷指示数${instructSet.join('/')}に対し出荷引当数合計${allocSum}`);
        } else if (qtyWarnings.length === QTY_WARNINGS_MAX) {
          qtyWarnings.push(`…他にも不一致があります (先頭${QTY_WARNINGS_MAX}件のみ表示)`);
        }
      }
    }
    if (qtyWarningCount > 0) {
      console.warn(`[picking-import] 数量クロスチェック不一致 ${qtyWarningCount}件 (TB=${tbNos.join(',')}): ${qtyWarnings.join(' / ')}`);
    }
  }

  // ロケーション×SKU で集約 (トータルピッキング)。
  // 表示順 = ブロック昇順 → ロケーション昇順 → SKU昇順。
  // 紙のトータルピッキングリストPDFと実物突合済み (2026-08-12 出荷_01):
  // 紙は P3FA→P3FB→P3FD→P3FF とブロックでまず並び、その中がロケーション順
  const aggMap = new Map();
  for (const l of slipLines) {
    const key = `${l.location}\u0000${l.sku}`;
    const cur = aggMap.get(key);
    if (cur) {
      cur.qty += l.qty;
    } else {
      aggMap.set(key, {
        location: l.location, block: l.block, sku: l.sku,
        productName: l.productName, barcode: l.barcode, qty: l.qty,
      });
    }
  }
  const cmp = (x, y) => (x < y ? -1 : x > y ? 1 : 0);
  const lines = [...aggMap.values()].sort((a, b) =>
    cmp(a.block || '', b.block || '') || cmp(a.location, b.location) || cmp(a.sku, b.sku));

  const composition = classifyComposition(slipLines);
  // 共通項目は先頭行決め打ちにせず distinct を取る (Codex R1 medium)。
  // LINEギフトの引当は複数の配送方法・送り状ソフトが正当に混在するため、混在=エラーにはせず
  // 全値を保持して表示・推定の両方に使う
  const distinct = (key) => [...new Set(slipLines.map((l) => l[key]).filter(Boolean))];
  const invoiceSofts = distinct('invoiceSoft');
  const deliveryMethods = distinct('deliveryMethod');
  const instructDates = distinct('instructDate');
  const preview = {
    tbNo: tbNos.join(','),   // 識別キー (pk_batches.tb_no UNIQUE)。表示側は先頭+件数に整形する
    tbCount: tbNos.length,
    instructDate: instructDates.map(formatDate8).join(' / '),
    invoiceSoft: invoiceSofts.join(' / '),
    deliveryMethod: deliveryMethods.join(' / '),
    composition,
    lines,
    slipLines,
    slipCount: new Set(slipLines.map((l) => l.slipNo)).size,
    totalQty: slipLines.reduce((s, l) => s + l.qty, 0),
    qtyWarnings,
    qtyWarningCount,
    csvSha256: crypto.createHash('sha256').update(buffer).digest('hex'),
  };
  preview.suggestions = suggestPatterns({ invoiceSofts, deliveryMethods, composition });
  return preview;
}

/**
 * 数量構成の判定 (引当パターンの3分類と同じ軸)。
 *   単品        = 全伝票が 1明細 × 1個
 *   1SKU複数個  = 全伝票が 1SKU (数量2以上を含む)
 *   アソート    = 全伝票が 複数SKU
 *   混在        = 上記が混ざる (まとめ引当)
 */
export function classifyComposition(slipLines) {
  const bySlip = new Map();
  for (const l of slipLines) {
    if (!bySlip.has(l.slipNo)) bySlip.set(l.slipNo, []);
    bySlip.get(l.slipNo).push(l);
  }
  const kinds = new Set();
  for (const lines of bySlip.values()) {
    const skus = new Set(lines.map((l) => l.sku));
    const totalQty = lines.reduce((s, l) => s + l.qty, 0);
    if (skus.size >= 2) kinds.add('アソート');
    else if (totalQty >= 2) kinds.add('1SKU複数個');
    else kinds.add('単品');
  }
  if (kinds.size === 1) return [...kinds][0];
  return '混在';
}

/** '20260811' → '2026-08-11'。形式外はそのまま返す (表示用なので落とさない)。 */
function formatDate8(s) {
  return /^\d{8}$/.test(s) ? `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}` : s;
}

/** ロケーション表示形式 (確定 2026-08-11): ブロック + 8桁を 3-3-2 区切り。例 P3FB-002-016-04 */
export function formatLocation(block, location) {
  const digits = /^\d{8}$/.test(location)
    ? `${location.slice(0, 3)}-${location.slice(3, 6)}-${location.slice(6, 8)}`
    : location;
  return block ? `${block}-${digits}` : digits;
}

/**
 * Driveファイル名から出荷フォルダ名を導出する。
 * 運用ルール (2026-08-12 中原さん): 出荷_no フォルダに `ピッキングリストデータ_出荷XX.csv` で保存。
 * 例: 'ピッキングリストデータ_出荷03.csv' → '出荷_03' / 該当なしは null (手入力に任せる)
 */
export function deriveFolderName(filename) {
  const m = String(filename || '').match(/出荷_?(\d{1,2})/);
  return m ? `出荷_${m[1].padStart(2, '0')}` : null;
}

/**
 * 出荷指示日が今日を含まないか (前日ファイルの取り込み事故ガード)。
 * 出荷Noは毎日1から再利用されるため、Driveに残った前日の同名ファイルを
 * 翌朝取り込むと出荷済みバッチを再ピックしてしまう。警告表示に使う (ブロックはしない)。
 * instructDate は 'YYYY-MM-DD' または 'YYYY-MM-DD / YYYY-MM-DD' (混在時)。
 */
export function isStaleInstructDate(instructDate, today = jstToday()) {
  const dates = String(instructDate || '').split(' / ').filter(Boolean);
  return dates.length > 0 && dates.every((d) => d !== today);
}

/**
 * 取込の確定。tb_no が冪等キー。
 *   - 既存なし → 新規作成
 *   - 既存あり (ready・overwrite=true) → 明細を入れ替えて更新 (取り込み直し)
 *   - 既存あり (ready・overwrite なし) → 409 duplicate (画面が上書き確認を出す)
 *   - 既存あり (ready 以外) → 409 already_started (作業開始後の上書きは不可)
 * @returns {{batchId, replaced}}
 */
export function importBatch(preview, { hikiateClass, classSource, folderName, overwrite }, actor) {
  const cls = String(hikiateClass ?? '').trim();
  // 引当分類の出どころ。'txt'=Driveの引当パターンtxt (確か) / 'suggested'=CSVからの推定 (要確認) /
  // 'manual'=人が選んだ。⭐未指定は null (出所不明) にする。'manual' に丸めると、呼び出し側が
  // 渡し忘れたときに「人が確認済み」を騙って警告対象から外れてしまう (Codexレビュー)
  const src = ['txt', 'suggested', 'manual'].includes(classSource) ? classSource : null;
  const folder = String(folderName ?? '').trim() || null;
  if (!cls) throw new PkError(400, 'no_class', '引当分類を選択してください');
  // 管理者入力にも長さ上限を置く (Codex R1 low)。パターン名の最長は約60文字
  if (cls.length > 100) throw new PkError(400, 'class_too_long', '引当分類が長すぎます (100文字まで)');
  if (folder && folder.length > 50) throw new PkError(400, 'folder_too_long', 'フォルダ名が長すぎます (50文字まで)');
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    const existing = getBatchByTbNo(preview.tbNo);
    let batchId;
    let replaced = false;
    if (existing) {
      // 同一CSV・同一分類・同一フォルダの再confirmは「応答が届かなかった再送」なので
      // 成功済み結果を返す (Codex R1 medium)。フォルダ名が違う要求は再送ではなく変更なので
      // 通常の duplicate/overwrite 経路に落とす (Codex R2 medium)
      if (existing.csv_sha256 === preview.csvSha256 && existing.hikiate_class === cls
          && (existing.folder_name ?? null) === folder) {
        return { batchId: existing.id, replaced: false, replayed: true };
      }
      if (existing.status !== 'ready') {
        throw new PkError(409, 'already_started',
          `バッチ ${preview.tbNo} は既に作業が始まっています (${existing.status})。取り込み直しはできません`);
      }
      if (!overwrite) {
        throw new PkError(409, 'duplicate',
          `バッチ ${preview.tbNo} は取込済みです (${existing.hikiate_class} / ${existing.line_count}明細)`);
      }
      db.prepare('DELETE FROM pk_lines WHERE batch_id = ?').run(existing.id);
      db.prepare('DELETE FROM pk_slip_lines WHERE batch_id = ?').run(existing.id);
      db.prepare(`
        UPDATE pk_batches SET hikiate_class=?, class_source=?, folder_name=?, work_date=?, instruct_date=?,
          composition=?, delivery_method=?, invoice_soft=?,
          line_count=?, slip_count=?, total_qty=?, csv_sha256=?, imported_by=?, updated_at=?
        WHERE id=?
      `).run(cls, src, folder, jstToday(), preview.instructDate,
        preview.composition, preview.deliveryMethod, preview.invoiceSoft,
        preview.lines.length, preview.slipCount, preview.totalQty, preview.csvSha256,
        actor, now, existing.id);
      batchId = existing.id;
      replaced = true;
    } else {
      const info = db.prepare(`
        INSERT INTO pk_batches
          (tb_no, hikiate_class, class_source, folder_name, work_date, instruct_date, composition,
           delivery_method, invoice_soft, line_count, slip_count, total_qty,
           status, csv_sha256, imported_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?)
      `).run(preview.tbNo, cls, src, folder, jstToday(),
        preview.instructDate, preview.composition, preview.deliveryMethod, preview.invoiceSoft,
        preview.lines.length, preview.slipCount, preview.totalQty, preview.csvSha256,
        actor, now, now);
      batchId = Number(info.lastInsertRowid);
    }

    // 取込の監査ログ (追記型)。上書きは変更前の集計値も残す (Codex R1 medium)
    db.prepare(`
      INSERT INTO pk_import_logs
        (batch_id, tb_no, action, csv_sha256, hikiate_class, folder_name,
         line_count, slip_count, total_qty, before_json, actor, at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(batchId, preview.tbNo, replaced ? 'overwrite' : 'create', preview.csvSha256, cls, folder,
      preview.lines.length, preview.slipCount, preview.totalQty,
      replaced ? JSON.stringify({
        hikiate_class: existing.hikiate_class, folder_name: existing.folder_name,
        line_count: existing.line_count, slip_count: existing.slip_count,
        total_qty: existing.total_qty, csv_sha256: existing.csv_sha256,
      }) : null, actor, now);

    const insLine = db.prepare(`
      INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `);
    preview.lines.forEach((l, i) => {
      insLine.run(batchId, i + 1, l.location, l.block || null, l.sku,
        l.productName || null, l.barcode || null, l.qty);
    });

    const insSlip = db.prepare(`
      INSERT INTO pk_slip_lines (batch_id, slip_no, picking_no, ne_slip_no, sku, qty, location)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `);
    for (const l of preview.slipLines) {
      insSlip.run(batchId, l.slipNo, l.pickingNo || null, l.neSlipNo || null, l.sku, l.qty, l.location);
    }
    return { batchId, replaced };
  })();
}

// ═══ 作業イベント (PR2) ═══════════════════════════════════════════════
//
// スマホ作業画面からの全操作はイベントとして POST され、ここで適用する。
//   - 冪等: op_id (端末生成 ms+乱数)。同一 op_id + 同一内容の再送は保存済み結果を返す。
//     内容が違う op_id の使い回しは 409 (sw_operations と同思想)
//   - 順序: 端末側がキューを直列で送る前提。サーバーは現在状態と矛盾する適用を拒否する
//   - 時刻: サーバー時刻を正とする (db.js の方針)。端末の発生時刻は payload に保持し分析用
//
// イベント種別 (PR2): start / next / back / complete は next の最終行で自動。
// shortage / pause / resume / cancel は PR4 で追加する。

const WORK_EVENTS = ['start', 'next', 'back', 'takeover', 'shortage', 'shortage_open', 'shortage_cancel', 'pause', 'resume', 'cancel'];

// 中断理由 (要件§5.6。shipping-work の保留理由マスタと粒度を揃えた最小セット)
export const PAUSE_REASONS = ['休憩', '他作業への応援', 'その他'];

/** 作業画面の現在状態。currentSeq = 次にピックする明細 (null = 全て完了)。 */
export function getWorkState(batchId) {
  const batch = getBatch(batchId);
  if (!batch) throw new PkError(404, 'not_found', 'バッチが見つかりません');
  const rawLines = listLines(batchId);
  const images = getImageMap(rawLines.map((l) => l.sku));
  // 取込直後の解決キューが再起動等で消えていても、画面を開いたときに自己修復する
  // (キャッシュに行が無いSKUだけ再キュー。行があるSKUは ensureImagesFor 側のTTLに従う)
  const missing = rawLines
    .map((l) => String(l.sku ?? '').trim().toLowerCase())
    .filter((sku) => sku && !images.has(sku));
  if (missing.length > 0) queueEnsureImages(missing, `batch:${batchId}`);
  const lines = rawLines.map((l) => ({
    ...l,
    locationLabel: formatLocation(l.block, l.location),
    imageUrl: images.get(String(l.sku ?? '').trim().toLowerCase())?.url || null,
  }));
  const pending = lines.filter((l) => l.status === 'pending');
  // 明細ごとの「最後に完了させた next の op_id」。back の取り消し対象指定 (CAS) に使う
  const lastNextOps = {};
  for (const e of getDB().prepare(
    "SELECT line_seq, op_id FROM pk_events WHERE batch_id=? AND event IN ('next','shortage') ORDER BY id"
  ).all(batchId)) {
    lastNextOps[e.line_seq] = e.op_id;
  }
  return {
    batch,
    lines,
    currentSeq: pending.length > 0 ? pending[0].seq : null,
    doneCount: lines.length - pending.length,
    lastNextOps,
  };
}

/**
 * 欠品対応セッションを閉じる。開いていた区間 (open_at〜at) を paused_total_sec に加算する
 * (= 中断と同じ扱いで実働から除外。サマリ/ボード/フロア/Notion の実働秒は既存計算のまま)。
 * 別の明細で開いていた/開いていなければ、列を消すだけ。
 */
function closeShortageSession(db, batch, lineSeq, at, now) {
  // その明細で開いているときだけ閉じる (二重タップ・別明細のセッションは触らない — Codex R2)
  if (!batch.shortage_open_at || batch.shortage_open_seq !== lineSeq) return;
  const sec = Math.max(0, Math.round((Date.parse(at) - Date.parse(batch.shortage_open_at)) / 1000));
  db.prepare(`UPDATE pk_batches SET paused_total_sec = paused_total_sec + ?,
    shortage_open_at=NULL, shortage_open_seq=NULL, updated_at=? WHERE id=?`).run(sec, now, batch.id);
  // 明細単位の所要時間 (done_at - shown_at: 作業者別/分類別/ボード) からも同じ秒数を除く。
  // shown_at を後ろへずらす = 「欠品対応の分だけ表示が遅く始まった」とみなす (Codex R2 critical)
  if (sec > 0) {
    db.prepare(`UPDATE pk_lines SET shown_at = strftime('%Y-%m-%dT%H:%M:%SZ', shown_at, '+' || ? || ' seconds')
      WHERE batch_id=? AND seq=? AND shown_at IS NOT NULL`).run(sec, batch.id, lineSeq);
  }
}

/**
 * 端末の発生時刻を「now以前・24時間以内」にクランプして採用する (中断時間の計測用)。
 * 通常イベントの計測はサーバー時刻が正のままで、pause/resume だけこの値を使う。
 */
function clampedEventTime(clientAt, now) {
  const t = Date.parse(clientAt || '');
  const nowMs = Date.parse(now);
  if (!Number.isFinite(t)) return now;
  if (t > nowMs || t < nowMs - 24 * 3600 * 1000) return now;
  return new Date(t).toISOString().slice(0, 19) + 'Z';
}

function eventResult(batchId, transition = null) {
  const s = getWorkState(batchId);
  return {
    batchStatus: s.batch.status,
    currentSeq: s.currentSeq,
    doneCount: s.doneCount,
    lineCount: s.lines.length,
    startedAt: s.batch.started_at,
    finishedAt: s.batch.finished_at,
    // バッチ状態の遷移が起きたときだけ入る: started / completed / reopened。
    // router がこれを見て Notion カードを動かす (replayed のときは動かさない)
    transition,
  };
}

/**
 * 作業イベントの適用。1イベント = 1トランザクション。
 * @returns {{replayed?: boolean, ...eventResult}}
 */
export function applyEvent(batchId, { opId, event, lineSeq, clientAt, undoOpId, shortageQty, pauseReason, altBlock, altLocation, altQty, remaining }, worker) {
  if (!opId || typeof opId !== 'string' || opId.length > 64) {
    throw new PkError(400, 'bad_op_id', 'op_id が不正です');
  }
  if (!WORK_EVENTS.includes(event)) {
    throw new PkError(400, 'bad_event', `不明なイベントです: ${event}`);
  }
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    // 冪等: 同一 op_id は同一内容の再送のみ成功扱い
    const prev = db.prepare('SELECT * FROM pk_events WHERE op_id = ?').get(opId);
    if (prev) {
      const prevPayload = prev.payload_json ? JSON.parse(prev.payload_json) : {};
      if (prev.batch_id === batchId && prev.worker === worker
          && prev.event === event && (prev.line_seq ?? null) === (lineSeq ?? null)
          && (prevPayload.undoOpId ?? null) === (undoOpId ?? null)
          && (prevPayload.shortageQty ?? null) === (shortageQty ?? null)
          && (prevPayload.pauseReason ?? null) === (pauseReason ?? null)
          && (prevPayload.altBlock ?? null) === (altBlock ?? null)
          && (prevPayload.altLocation ?? null) === (altLocation ?? null)
          && (prevPayload.altQty ?? null) === (altQty ?? null)
          && (prevPayload.remaining ?? null) === (remaining ?? null)) {
        return { replayed: true, ...JSON.parse(prev.result_json) };
      }
      throw new PkError(409, 'op_conflict', '同じ操作IDが別の内容で使われています');
    }

    const batch = getBatch(batchId);
    if (!batch) throw new PkError(404, 'not_found', 'バッチが見つかりません');
    if (batch.validity !== 'valid' || batch.status === 'cancelled') {
      throw new PkError(409, 'batch_invalid', 'このバッチは取消されています');
    }
    // 🔴ピッキング漏れバッチ: 梱包側でタスクが取下げられていたら操作させず、その場で畳む
    // (一覧のreconcileを経ずに /work を直接開いている端末への即時ガード — Codexレビュー)
    if (batch.origin === 'repick' && batch.pack_task_id && batch.status !== 'done') {
      let ts = null;
      try {
        ts = db.prepare('SELECT status FROM pk_pack_tasks WHERE id = ?').get(batch.pack_task_id)?.status ?? null;
      } catch { ts = null; }   // pk_pack_tasks が無い環境は無視
      if (ts === 'cancelled') {
        // ここで UPDATE しても throw でトランザクションごとロールバックされる (Codex 2巡目)。
        // 操作は409で拒否し、実際の取消は一覧表示時の reconcileRepickBatches が行う
        throw new PkError(409, 'repick_cancelled', 'この再ピック依頼は梱包側で取下げられました (一覧に戻ると整理されます)');
      }
    }
    // 🔴再ピックバッチの back/cancel: 1階が受け取り済みなら戻せない (タスクは終端・現物は1階にある)
    if (batch.origin === 'repick' && batch.pack_task_id && (event === 'back' || event === 'cancel')) {
      let ts = null;
      try { ts = db.prepare('SELECT status FROM pk_pack_tasks WHERE id = ?').get(batch.pack_task_id)?.status ?? null; } catch { ts = null; }
      if (ts === 'received') throw new PkError(409, 'already_received', '梱包側が受け取り済みのため戻せません');
    }

    let transition = null;   // started / completed / reopened (Notion連携のトリガ)
    if (event === 'takeover') {
      // 担当者の交代 (選び間違い・実際の引き継ぎ)。作業中・中断中のみ。
      // 排他 (taken) の唯一の正規の突破口で、イベントに交代の記録が残る
      if (batch.status !== 'picking' && batch.status !== 'paused') {
        throw new PkError(409, 'not_picking', `作業中ではないため交代できません (${batch.status})`);
      }
      if (batch.worker !== worker) {
        db.prepare('UPDATE pk_batches SET worker=?, updated_at=? WHERE id=?').run(worker, now, batchId);
        transition = 'takeover';   // Notionのピッキング担当者を追従させる (ステータスは現状維持)
      }
    } else if (event === 'pause') {
      // 中断: 中断時間はピッキング時間から除外する (要件§5.6)
      if (batch.status !== 'picking') {
        throw new PkError(409, 'not_picking', `作業中ではないため中断できません (${batch.status})`);
      }
      if (batch.worker !== worker) throw new PkError(409, 'taken', `このバッチは ${batch.worker} が作業中です`);
      const reason = PAUSE_REASONS.includes(pauseReason) ? pauseReason : 'その他';
      // pause/resume の時刻だけは端末の発生時刻 (clientAt) を採用する (クランプ付き)。
      // オフラインで pause→resume を積むと、再接続時に連続でサーバーへ届くため、
      // 受信時刻基準では実際30分の中断が数秒になってしまう (Codex PR4 high)。
      // 未来・24時間より過去は now に丸め、細工や壊れた端末時計の影響を限定する
      db.prepare(`UPDATE pk_batches SET status='paused', pause_started_at=?, pause_reason=?, updated_at=? WHERE id=?`)
        .run(clampedEventTime(clientAt, now), reason, now, batchId);
    } else if (event === 'resume') {
      if (batch.status !== 'paused') {
        throw new PkError(409, 'not_paused', `中断中ではありません (${batch.status})`);
      }
      if (batch.worker !== worker) throw new PkError(409, 'taken', `このバッチは ${batch.worker} が中断中です (交代してから再開してください)`);
      const resumeAt = clampedEventTime(clientAt, now);
      const pausedSec = Math.max(0, Math.round((Date.parse(resumeAt) - Date.parse(batch.pause_started_at || resumeAt)) / 1000));
      db.prepare(`
        UPDATE pk_batches SET status='picking', paused_total_sec = paused_total_sec + ?,
          pause_started_at=NULL, pause_reason=NULL, updated_at=? WHERE id=?
      `).run(pausedSec, now, batchId);
    } else if (event === 'cancel') {
      // 誤開始の取消: バッチを未着手に戻し、明細の進捗も初期化する。
      // pk_events は残るので「取消があった」ことは追える (計測からは除外される)
      if (batch.status !== 'picking' && batch.status !== 'paused') {
        throw new PkError(409, 'not_picking', `作業中ではないため取消できません (${batch.status})`);
      }
      if (batch.worker !== worker) throw new PkError(409, 'taken', `このバッチは ${batch.worker} が作業中です`);
      // v2 PR2: 欠品明細の後始末 (配賦・依頼・展開済みタスク・梱包側の保留) を明細ごとに。
      // 着手済みの「後で取りに行く」が1つでもあれば取消できない (throw でトランザクションごと巻き戻る)
      for (const l of db.prepare("SELECT seq FROM pk_lines WHERE batch_id=? AND status='shortage'").all(batchId)) {
        undoShortageSideEffects(db, batchId, l.seq, now);
      }
      db.prepare(`
        UPDATE pk_batches SET status='ready', worker=NULL, started_at=NULL, finished_at=NULL,
          paused_total_sec=0, pause_started_at=NULL, pause_reason=NULL, updated_at=? WHERE id=?
      `).run(now, batchId);
      db.prepare(`
        UPDATE pk_lines SET status='pending', shown_at=NULL, done_at=NULL, shortage_qty=NULL,
          alt_block=NULL, alt_location=NULL, alt_qty=NULL, remaining_qty=NULL, remaining=NULL WHERE batch_id=?
      `).run(batchId);
      db.prepare('UPDATE pk_batches SET shortage_open_at=NULL, shortage_open_seq=NULL WHERE id=?').run(batchId);
    } else if (event === 'start') {
      if (batch.status === 'picking') {
        if (batch.worker !== worker) {
          throw new PkError(409, 'taken', `このバッチは ${batch.worker} が作業中です`);
        }
        // 同一作業者の再開 (リロード・端末復帰)。状態はそのまま返す
      } else if (batch.status === 'done') {
        throw new PkError(409, 'already_done', 'このバッチは完了済みです');
      } else {
        // ready → picking
        const res = db.prepare(`
          UPDATE pk_batches SET status='picking', worker=?, started_at=COALESCE(started_at, ?), updated_at=?
          WHERE id=? AND status='ready'
        `).run(worker, now, now, batchId);
        if (res.changes === 0) throw new PkError(409, 'not_startable', `状態 ${batch.status} からは開始できません`);
        transition = 'started';
      }
      // 先頭の未完了明細に表示時刻を刻む (初回のみ)
      db.prepare(`
        UPDATE pk_lines SET shown_at = COALESCE(shown_at, ?)
        WHERE batch_id = ? AND seq = (SELECT MIN(seq) FROM pk_lines WHERE batch_id = ? AND status = 'pending')
      `).run(now, batchId, batchId);
    } else {
      // next は picking 中のみ。back は「最終明細のnextで完了した直後の誤タップ」を取り消せる
      // 必要があるため、done からも受け付けてバッチを picking に戻す (Codex PR2-R1)。
      // これが無いと、オフラインキュー [next(最終), back] の後半が 409 になりキューごと破棄される
      const backFromDone = event === 'back' && batch.status === 'done';
      if (batch.status !== 'picking' && !backFromDone) {
        throw new PkError(409, 'not_picking', `バッチが作業中ではありません (${batch.status})`);
      }
      if (batch.worker !== worker) {
        throw new PkError(409, 'taken', `このバッチは ${batch.worker} が作業中です`);
      }
      if (!Number.isInteger(lineSeq) || lineSeq < 1) {
        throw new PkError(400, 'bad_line_seq', 'line_seq が不正です');
      }
      const line = db.prepare('SELECT * FROM pk_lines WHERE batch_id=? AND seq=?').get(batchId, lineSeq);
      if (!line) throw new PkError(404, 'line_not_found', `明細 ${lineSeq} がありません`);

      if (event === 'shortage_open' || event === 'shortage_cancel') {
        // 欠品フローv2: 「⚠ 欠品」を押した瞬間から判断確定までを欠品対応セッションとして
        // 計測から除外する (探しに行く時間で個人スピードが落ちない — 中原さん指示 2026-08-26)。
        // open は既に同じ明細で開いていればそのまま (開き直し・再送で開始時刻を後ろにずらさない)
        const cur = db.prepare(
          "SELECT MIN(seq) s FROM pk_lines WHERE batch_id=? AND status='pending'"
        ).get(batchId).s;
        if (cur !== lineSeq) {
          throw new PkError(409, 'out_of_order', `明細 ${lineSeq} は現在の対象 (${cur ?? 'なし'}) ではありません`);
        }
        if (event === 'shortage_open') {
          if (batch.shortage_open_seq !== lineSeq) {
            db.prepare('UPDATE pk_batches SET shortage_open_at=?, shortage_open_seq=?, updated_at=? WHERE id=?')
              .run(clampedEventTime(clientAt, now), lineSeq, now, batchId);
          }
        } else {
          closeShortageSession(db, batch, lineSeq, clampedEventTime(clientAt, now), now);
        }
      } else if (event === 'next' || event === 'shortage') {
        // 表示中の明細 (= 最小の pending) 以外への操作は、キュー順序が壊れている兆候なので拒否
        const cur = db.prepare(
          "SELECT MIN(seq) s FROM pk_lines WHERE batch_id=? AND status='pending'"
        ).get(batchId).s;
        if (cur !== lineSeq) {
          throw new PkError(409, 'out_of_order', `明細 ${lineSeq} は現在の対象 (${cur ?? 'なし'}) ではありません`);
        }
        if (event === 'shortage') {
          // 欠品: 数量は 1〜指示数 (未指定は全量欠品)。一部欠品 = 取れた分だけ取って残りを欠品。
          // v2: 他ロケで確保した数 (alt_qty) と、残りをどうするか (remaining: later/none) を同時に記録する。
          // 旧クライアント (alt無し・remaining無し) は「残り全量=欠品確定 (none)」として扱う
          const q = shortageQty == null ? line.qty : Number(shortageQty);
          if (!Number.isInteger(q) || q < 1 || q > line.qty) {
            throw new PkError(400, 'bad_shortage_qty', `欠品数量は1〜${line.qty}で指定してください`);
          }
          const a = altQty == null ? 0 : Number(altQty);
          if (!Number.isInteger(a) || a < 0 || a > q) {
            throw new PkError(400, 'bad_alt_qty', `他ロケで確保した数は0〜${q}で指定してください`);
          }
          const altLoc = a > 0 ? String(altLocation || '').trim().slice(0, 40) : null;
          if (a > 0 && !altLoc) throw new PkError(400, 'bad_alt_location', '確保したロケーションを指定してください');
          const altBlk = a > 0 ? (String(altBlock || '').trim().slice(0, 20) || null) : null;
          const remQty = q - a;
          const rem = remQty > 0 ? (remaining || 'none') : null;
          if (rem != null && !['later', 'none'].includes(rem)) {
            throw new PkError(400, 'bad_remaining', '残りの扱いは later か none です');
          }
          // 🔴再ピックバッチ (梱包からの依頼・自分の「後で」の受け皿) の中で「後で取りに行く」は使えない。
          // 受注明細 (pk_slip_lines) が無いので配賦できず、依頼が pending_binding のまま迷子になる
          // (9/1 に実発生。例外処理監査 A-3)。取れなければ「どこにもない」= 在庫なしとして1階へ伝える
          if (batch.origin === 'repick' && rem === 'later') {
            throw new PkError(400, 'later_in_repick',
              '再ピックでは「後で取りに行く」は使えません。取れなければ「どこにもない」を選んでください');
          }
          db.prepare(`UPDATE pk_lines SET status='shortage', done_at=?, shortage_qty=?,
              alt_block=?, alt_location=?, alt_qty=?, remaining_qty=?, remaining=?
            WHERE batch_id=? AND seq=?`)
            .run(now, q, altBlk, altLoc, a > 0 ? a : null, remQty, rem, batchId, lineSeq);
          closeShortageSession(db, batch, lineSeq, clampedEventTime(clientAt, now), now);
          // v2 PR2: 残りを受注に配賦し (梱包の 🕒/❌ バッジの元)、「後で」は依頼として積む。
          // 梱包タスクへの展開はトランザクション外 (bindPendingLaterRequests — router/reconcile/ポーラー)。
          // 再ピックバッチには受注明細が無いので配賦しない (在庫なしは syncRepickTask がタスク側へ伝える)
          if (batch.origin !== 'repick') {
            recordShortageAllocations(db, batch, line, lineSeq, { remQty, rem, worker }, now);
          }
        } else {
          db.prepare("UPDATE pk_lines SET status='done', done_at=? WHERE batch_id=? AND seq=?")
            .run(now, batchId, lineSeq);
          // 欠品シートを開いたまま「次へ」は画面上できないが、セッションが残っていれば閉じる
          if (batch.shortage_open_seq === lineSeq) closeShortageSession(db, batch, lineSeq, now, now);
        }
        const nextSeq = db.prepare(
          "SELECT MIN(seq) s FROM pk_lines WHERE batch_id=? AND status='pending'"
        ).get(batchId).s;
        if (nextSeq != null) {
          // shown_at = 「直近にこの明細が表示対象になった時刻」。done_at - shown_at が
          // その明細の実表示時間になる (back で往復しても他明細の時間が混入しない。
          // 初回表示がいつだったかは pk_events の履歴から復元できる — Codex PR2-R1 medium)
          db.prepare('UPDATE pk_lines SET shown_at = ? WHERE batch_id=? AND seq=?')
            .run(now, batchId, nextSeq);
        } else {
          // 最終明細の完了 = バッチ完了
          db.prepare("UPDATE pk_batches SET status='done', finished_at=?, updated_at=? WHERE id=?")
            .run(now, now, batchId);
          transition = 'completed';
        }
      } else if (event === 'back') {
        // 直前に処理した明細 (完了 or 欠品) を取り消して戻る
        if (line.status !== 'done' && line.status !== 'shortage') {
          throw new PkError(409, 'not_done', `明細 ${lineSeq} は完了していないため戻れません`);
        }
        // 戻れるのは「処理済みの中で最大の seq」だけ (途中の明細に飛び戻ると順序が壊れる)
        const lastDone = db.prepare(
          "SELECT MAX(seq) s FROM pk_lines WHERE batch_id=? AND status IN ('done','shortage')"
        ).get(batchId).s;
        if (lastDone !== lineSeq) {
          throw new PkError(409, 'out_of_order', `戻れるのは直前の明細 (${lastDone}) だけです`);
        }
        // CAS: 「どの完了を取り消すか」を undo_op_id で特定する (Codex PR2-R2 high)。
        // これが無いと、同一作業者の別端末に残っていた古い back が、再完了後に到着して
        // 新しい完了まで取り消してしまう
        const lastNextOp = db.prepare(
          "SELECT op_id FROM pk_events WHERE batch_id=? AND event IN ('next','shortage') AND line_seq=? ORDER BY id DESC LIMIT 1"
        ).get(batchId, lineSeq)?.op_id ?? null;
        if (!undoOpId || undoOpId !== lastNextOp) {
          throw new PkError(409, 'stale_back',
            '取り消し対象の完了が見つからないか、別の操作で上書きされています');
        }
        // v2 PR2: 欠品の後始末 (配賦・依頼・展開済みタスク・保留)。着手済みなら back 自体を拒否
        if (line.status === 'shortage') undoShortageSideEffects(db, batchId, lineSeq, now);
        // shown_at は now に更新 (この瞬間から再表示。再作業時間に前の明細の時間を混ぜない)
        db.prepare(`UPDATE pk_lines SET status='pending', done_at=NULL, shortage_qty=NULL, shown_at=?,
            alt_block=NULL, alt_location=NULL, alt_qty=NULL, remaining_qty=NULL, remaining=NULL
          WHERE batch_id=? AND seq=?`)
          .run(now, batchId, lineSeq);
        if (backFromDone) {
          // 完了直後の取り消し: バッチを作業中に戻す
          db.prepare("UPDATE pk_batches SET status='picking', finished_at=NULL, updated_at=? WHERE id=?")
            .run(now, batchId);
          transition = 'reopened';
        }
      }
    }

    const result = eventResult(batchId, transition);
    db.prepare(`
      INSERT INTO pk_events (op_id, batch_id, worker, event, line_seq, payload_json, result_json, at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(opId, batchId, worker, event, lineSeq ?? null,
      (clientAt || undoOpId || shortageQty != null || pauseReason || altQty != null || remaining)
        ? JSON.stringify({ clientAt, undoOpId, shortageQty, pauseReason, altBlock, altLocation, altQty, remaining }) : null,
      JSON.stringify(result), now);
    return result;
  })();
}

/**
 * 日次サマリ (要件§5.7: まずログを正しく蓄積し、素朴な集計を見せる)。
 * 有効時間 = (finished - started) - 中断合計。完了バッチのみ集計に採用。
 */
export function getDailySummary(workDate) {
  const db = getDB();
  const batches = db.prepare(`
    SELECT * FROM pk_batches
    WHERE work_date = ? AND validity = 'valid' AND status != 'cancelled'
      AND origin != 'repick'   -- 🔴ピッキング漏れは計測対象外 (中原さん指示 2026-08-21)
    ORDER BY id
  `).all(workDate);

  const activeSec = (b) => {
    if (!b.started_at || !b.finished_at) return null;
    return Math.max(0, Math.round((Date.parse(b.finished_at) - Date.parse(b.started_at)) / 1000) - (b.paused_total_sec || 0));
  };

  const done = batches.filter((b) => b.status === 'done');
  const total = {
    batchCount: batches.length,
    doneCount: done.length,
    lineCount: done.reduce((s, b) => s + b.line_count, 0),
    qty: done.reduce((s, b) => s + b.total_qty, 0),
    activeSec: done.reduce((s, b) => s + (activeSec(b) || 0), 0),
  };
  total.secPerLine = total.lineCount > 0 ? total.activeSec / total.lineCount : null;
  total.linesPerHour = total.secPerLine ? 3600 / total.secPerLine : null;

  const groupBy = (key) => {
    const m = new Map();
    for (const b of done) {
      const k = b[key] || '(不明)';
      if (!m.has(k)) m.set(k, { key: k, batches: 0, lines: 0, qty: 0, activeSec: 0 });
      const g = m.get(k);
      g.batches++; g.lines += b.line_count; g.qty += b.total_qty; g.activeSec += activeSec(b) || 0;
    }
    return [...m.values()].map((g) => ({
      ...g,
      secPerLine: g.lines > 0 ? g.activeSec / g.lines : null,
    })).sort((a, b) => b.lines - a.lines);
  };

  // 欠品の担当者はバッチの最終担当者ではなく「欠品操作をした時点の作業者」(pk_events) を使う
  // (途中で takeover があると帰属がずれる — Codex PR4 medium)。
  // なお作業者別の時間集計はバッチの最終担当者に全量計上する簡略化 (交代は稀・Notion運用と同等)
  const shortages = db.prepare(`
    SELECT l.*, b.folder_name, b.hikiate_class,
      (SELECT e.worker FROM pk_events e
        WHERE e.batch_id = l.batch_id AND e.line_seq = l.seq AND e.event = 'shortage'
        ORDER BY e.id DESC LIMIT 1) AS worker
    FROM pk_lines l JOIN pk_batches b ON b.id = l.batch_id
    WHERE b.work_date = ? AND b.validity = 'valid' AND l.status = 'shortage'
      AND COALESCE(l.remaining_qty, l.shortage_qty, 1) > 0   -- 他ロケで全量確保した分は欠品に数えない (Q3・2026-08-26)
    ORDER BY b.id, l.seq
  `).all(workDate).map((l) => ({ ...l, locationLabel: formatLocation(l.block, l.location) }));

  return { workDate, total, byWorker: groupBy('worker'), byClass: groupBy('hikiate_class'), shortages, batches };
}

// ═══════════════════════════════════════════════════════════════════════════
// 作業実績の統計 (30日ローリング) — 倉庫の掲示モニターと管理画面が使う
//
// getDailySummary との違い:
//   - 期間が「当日1日」ではなく「直近30日」(中原さん指定 2026-08-17)
//   - 作業者の帰属が「バッチの最終担当者に全量計上」ではなく **明細1件ごと**
//     (pk_events の next/shortage を打った本人。交代しても正しく分かれる)
//   - 「速い/遅い」は生の秒/明細ではなく **引当分類の基準秒に対する比** で見る
//     (AES《単品》10.9秒 と AES《1SKU複数個》23.7秒 では倍違うため、
//      重い分類を引いた人が遅く見えてしまう)
// ═══════════════════════════════════════════════════════════════════════════

/** 集計の開始下限。これ以前はテスト運用のため除外する (中原さん指定 2026-08-17)。 */
export const STATS_MIN_DATE = process.env.PICKING_STATS_MIN_DATE || '2026-08-15';
/** 既定の集計窓 (日)。当日を含む直近N日。 */
export const STATS_WINDOW_DAYS = Number(process.env.PICKING_STATS_WINDOW_DAYS) || 30;
/**
 * 明細1件の所要秒がこれを超えたら「放置」とみなして集計から除外する。
 * 作業画面を開いたまま別の用事に行って戻ってから「次へ」を押すと、その1件に
 * 何分も乗って平均が壊れるため (中断⏸を使えば正しく除外されるが、実運用では押し忘れる)。
 */
export const STATS_OUTLIER_SEC = Number(process.env.PICKING_STATS_OUTLIER_SEC) || 180;
/**
 * ランキングを実数として扱う最低明細数。これ未満は「参考」扱いにする (数字が暴れるため)。
 * 実測 (8/15〜8/17) の1人1日あたりは数十〜400明細。半日だけ応援に入った人を弾かない 30 とする。
 */
export const STATS_MIN_LINES = Number(process.env.PICKING_STATS_MIN_LINES) || 30;
/**
 * 引当分類ごとの比較で実数として扱う最低明細数。分類別は1人あたりの母数が小さくなるため
 * 総合 (STATS_MIN_LINES) より緩くする。これ未満は参考値。
 */
export const STATS_MIN_CLASS_LINES = Number(process.env.PICKING_STATS_MIN_CLASS_LINES) || 10;

/** 'YYYY-MM-DD' に日数を足す (UTC基準で計算するのでJST日付文字列にそのまま使える)。 */
export function shiftDate(dateStr, days) {
  const d = new Date(`${dateStr}T00:00:00Z`);
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

/** 集計期間 (until を含む直近 days 日。ただし STATS_MIN_DATE より前には遡らない)。 */
export function statsRange(until = jstToday(), days = STATS_WINDOW_DAYS) {
  const rawSince = shiftDate(until, -(Math.max(1, days) - 1));
  const since = rawSince < STATS_MIN_DATE ? STATS_MIN_DATE : rawSince;
  return { since, until, days, clamped: rawSince < STATS_MIN_DATE };
}

/** 掲示用の作業者表示名。email 形式はローカル部だけにする (モニターに社内メールを出さない)。 */
export function displayWorkerName(worker) {
  const s = String(worker ?? '').trim();
  if (!s) return '(不明)';
  const at = s.indexOf('@');
  return at > 0 ? s.slice(0, at) : s;
}

/**
 * 期間内の「完了バッチの明細」を1件ずつ返す。作業者は pk_events から明細単位で解決する。
 * shown_at/done_at は PR2 で「直近表示時刻」を刻んでいるので、back で往復しても
 * 他の明細の時間が混入しない (= この差がその明細の実所要時間)。
 */
export function loadStatsLines(since, until) {
  return getDB().prepare(`
    SELECT b.work_date, b.hikiate_class, b.id AS batch_id, l.seq, l.status, l.shortage_qty, l.remaining_qty,
      CAST(ROUND((julianday(l.done_at) - julianday(l.shown_at)) * 86400) AS INTEGER) AS sec,
      COALESCE(
        (SELECT e.worker FROM pk_events e
          WHERE e.batch_id = l.batch_id AND e.line_seq = l.seq
            AND e.event IN ('next','shortage')
          ORDER BY e.id DESC LIMIT 1),
        b.worker
      ) AS worker
    FROM pk_lines l
    JOIN pk_batches b ON b.id = l.batch_id
    WHERE b.work_date >= ? AND b.work_date <= ?
      AND b.validity = 'valid' AND b.status = 'done'
      AND b.origin != 'repick'
      AND l.shown_at IS NOT NULL AND l.done_at IS NOT NULL
  `).all(since, until);
}

/**
 * 作業実績の統計。
 *
 * @param {{until?: string, days?: number}} opts
 * @returns {{
 *   since, until, days, clamped, minLines, minClassLines, outlierSec,
 *   total: {lines, sec, secPerLine, linesPerHour, batches, workers, days, excluded},
 *   baseline: [{key, lines, sec, avgSec, workerCount,          // 引当分類の基準秒
 *     workers: [{worker, name, lines, sec, secPerLine, index, provisional}]}],  // 分類内の速い順
 *   workers: [{worker, name, lines, sec, secPerLine, expectedSec, index, provisional,
 *              batches, days, shortages, excluded, classes: [...]}],
 *   byDate: [{date, lines, sec, secPerLine, workers}]
 * }}
 */
export function getPickingStats({ until = jstToday(), days = STATS_WINDOW_DAYS, lineRows = null } = {}) {
  const range = statsRange(until, days);
  // lineRows = 呼び出し側で読んだ明細 (ボードは getMissStats と同じ窓を使うので1回の読みを共有 — Codex)
  const rows = lineRows ?? loadStatsLines(range.since, range.until);

  // ── 外れ値の仕分け ──
  // 捨てた件数は全体と作業者別の両方で出す。除外は「放置の多い人ほど悪い記録が消える」
  // 非対称性を持つので、誰の分をどれだけ捨てたかが見えないと不公平を検知できない
  // (Codexレビュー medium)。0秒は除外しない — 同一ロケの連続ピックで実際に起きる
  const kept = [];
  const excludedByWorker = new Map();
  let excludedLines = 0;
  let excludedSec = 0;
  for (const r of rows) {
    const sec = Number(r.sec);
    if (!Number.isFinite(sec) || sec < 0 || sec > STATS_OUTLIER_SEC) {
      excludedLines++;
      if (Number.isFinite(sec) && sec > 0) excludedSec += sec;
      const k = r.worker || '(不明)';
      excludedByWorker.set(k, (excludedByWorker.get(k) || 0) + 1);
      continue;
    }
    kept.push({ ...r, sec });
  }

  // ── 引当分類ごとの基準秒 (期間内の全員の平均) ──
  // ⚠ 基準は本人の実績も含む相対値 (Codexレビュー medium)。固定基準や「本人を除く平均」も
  // 検討したが、①稼働がまだ数日で固定基準を置ける実績が無い ②除いた側の母数が薄いと
  // かえって暴れる ため、当面は全体平均に対する相対値と割り切り、画面にその旨を明記する。
  // その分類を1人しか担当していない場合、その人の指数は構造的に100付近になる
  // (baseline の workers 数を画面に出して見分けられるようにしてある)。
  const classMap = new Map();
  for (const r of kept) {
    const key = r.hikiate_class || '(不明)';
    if (!classMap.has(key)) classMap.set(key, { key, lines: 0, sec: 0, workers: new Set() });
    const c = classMap.get(key);
    c.lines++; c.sec += r.sec; c.workers.add(r.worker);
  }
  const baselineRaw = [...classMap.values()]
    .map((c) => ({ key: c.key, lines: c.lines, sec: c.sec, avgSec: c.sec / c.lines, workerCount: c.workers.size }))
    .sort((a, b) => b.lines - a.lines);
  const baselineByKey = new Map(baselineRaw.map((c) => [c.key, c.avgSec]));

  // ── 引当分類 × 作業者 (「この分類は誰が速いか」— 中原さん要望 2026-08-17) ──
  // 分類によって速さが根本的に違う (AES《単品》11.9秒 vs AES《1SKU複数個》23.5秒) ので、
  // 総合順位だけでなく分類ごとの比較を出す。指数は「その分類の平均 ÷ 本人の実測」=
  // 同じ分類同士の比較なので、重さの補正を挟まない素直な比率になる。
  // ⚠ 分類平均は明細の加重平均 (= 実際の総時間ベース) であり、処理量の多い人へ基準が寄る
  //   (Codexレビュー)。分類内の順位づけ自体は素の秒/明細で行うため順位には影響しない。
  //   指数は「平均との差の目安」の補助表示と割り切る (作業者等重み平均は母数の薄い人に
  //   引きずられるため採用しない)
  const classWorkerMap = new Map();
  for (const r of kept) {
    const ck = r.hikiate_class || '(不明)';
    const wk = r.worker || '(不明)';
    if (!classWorkerMap.has(ck)) classWorkerMap.set(ck, new Map());
    const m = classWorkerMap.get(ck);
    if (!m.has(wk)) m.set(wk, { worker: wk, name: displayWorkerName(wk), lines: 0, sec: 0 });
    const e = m.get(wk);
    e.lines++; e.sec += r.sec;
  }

  const baseline = baselineRaw.map((c) => ({
    ...c,
    workers: [...(classWorkerMap.get(c.key)?.values() ?? [])]
      .map((w) => ({
        worker: w.worker,
        name: w.name,
        lines: w.lines,
        sec: w.sec,
        secPerLine: w.sec / w.lines,
        // 分類内の相対 (100 = その分類の平均どおり・大きいほど速い)
        index: w.sec > 0 ? Math.round((c.avgSec * w.lines / w.sec) * 100) : null,
        provisional: w.lines < STATS_MIN_CLASS_LINES,
      }))
      .sort((a, b) => {
        if (a.provisional !== b.provisional) return a.provisional ? 1 : -1;
        return a.secPerLine - b.secPerLine;   // 速い順
      }),
  }));

  // ── 作業者別 ──
  const workerMap = new Map();
  for (const r of kept) {
    const key = r.worker || '(不明)';
    if (!workerMap.has(key)) {
      workerMap.set(key, {
        worker: key, name: displayWorkerName(key),
        lines: 0, sec: 0, expectedSec: 0, shortages: 0,
        batches: new Set(), days: new Set(), classes: new Map(),
      });
    }
    const w = workerMap.get(key);
    w.lines++;
    w.sec += r.sec;
    w.expectedSec += baselineByKey.get(r.hikiate_class || '(不明)') ?? r.sec;
    if (r.status === 'shortage' && (r.remaining_qty ?? r.shortage_qty ?? 1) > 0) w.shortages++;   // 他ロケで全量確保は数えない
    w.batches.add(r.batch_id);
    w.days.add(r.work_date);
    const ck = r.hikiate_class || '(不明)';
    if (!w.classes.has(ck)) w.classes.set(ck, { key: ck, lines: 0, sec: 0, expectedSec: 0 });
    const c = w.classes.get(ck);
    c.lines++; c.sec += r.sec; c.expectedSec += baselineByKey.get(ck) ?? r.sec;
  }

  // 全明細が外れ値だった作業者は kept に1件も残らず一覧から消えてしまう
  // (= 除外件数も誰にも見えない)。行だけは残して除外件数を出す (Codexレビュー medium)
  for (const key of excludedByWorker.keys()) {
    if (workerMap.has(key)) continue;
    workerMap.set(key, {
      worker: key, name: displayWorkerName(key),
      lines: 0, sec: 0, expectedSec: 0, shortages: 0,
      batches: new Set(), days: new Set(), classes: new Map(),
    });
  }

  const workers = [...workerMap.values()].map((w) => ({
    worker: w.worker,
    name: w.name,
    lines: w.lines,
    sec: w.sec,
    secPerLine: w.lines > 0 ? w.sec / w.lines : null,   // 全件除外の作業者は 0 除算になる
    expectedSec: w.expectedSec,
    // 速さ指数: 100 = 全体平均どおり。大きいほど速い (期待時間 ÷ 実測時間)
    index: w.sec > 0 ? Math.round((w.expectedSec / w.sec) * 100) : null,
    provisional: w.lines < STATS_MIN_LINES,   // 母数不足 = 参考値
    batches: w.batches.size,
    days: w.days.size,
    shortages: w.shortages,
    excluded: excludedByWorker.get(w.worker) || 0,   // 外れ値として捨てた明細数
    classes: [...w.classes.values()]
      .map((c) => ({ ...c, secPerLine: c.sec / c.lines, index: c.sec > 0 ? Math.round((c.expectedSec / c.sec) * 100) : null }))
      .sort((a, b) => b.lines - a.lines),
  })).sort((a, b) => {
    // 参考値は下へ。同区分内は速さ指数の降順 → 明細数の降順
    if (a.provisional !== b.provisional) return a.provisional ? 1 : -1;
    if ((b.index ?? 0) !== (a.index ?? 0)) return (b.index ?? 0) - (a.index ?? 0);
    return b.lines - a.lines;
  });

  // ── 日別 (推移) ──
  const dateMap = new Map();
  for (const r of kept) {
    if (!dateMap.has(r.work_date)) dateMap.set(r.work_date, { date: r.work_date, lines: 0, sec: 0, workers: new Set() });
    const d = dateMap.get(r.work_date);
    d.lines++; d.sec += r.sec; d.workers.add(r.worker);
  }
  const byDate = [...dateMap.values()]
    .map((d) => ({ date: d.date, lines: d.lines, sec: d.sec, secPerLine: d.sec / d.lines, workers: d.workers.size }))
    .sort((a, b) => a.date.localeCompare(b.date));

  const totalSec = kept.reduce((s, r) => s + r.sec, 0);
  const total = {
    lines: kept.length,
    sec: totalSec,
    secPerLine: kept.length > 0 ? totalSec / kept.length : null,
    linesPerHour: totalSec > 0 ? (kept.length / totalSec) * 3600 : null,
    batches: new Set(kept.map((r) => r.batch_id)).size,
    workers: workerMap.size,
    days: dateMap.size,
    excluded: { lines: excludedLines, sec: excludedSec },
  };

  return {
    ...range,
    minLines: STATS_MIN_LINES,
    minClassLines: STATS_MIN_CLASS_LINES,
    outlierSec: STATS_OUTLIER_SEC,
    total,
    baseline,   // 引当分類ごとの基準秒 + その分類の作業者内訳 (誰が速いか)
    workers,
    byDate,
  };
}

/**
 * 掲示モニター用の当日進捗 (30日統計と違い「今どうなっているか」を出す)。
 * 取込済みバッチの消化状況と、いま作業中の人が分かれば現場の目的は足りる。
 */
export function getTodayProgress(workDate = jstToday()) {
  const db = getDB();
  const batches = db.prepare(`
    SELECT id, status, worker, hikiate_class, folder_name, line_count, started_at, finished_at, paused_total_sec
    FROM pk_batches
    WHERE work_date = ? AND validity = 'valid' AND status != 'cancelled'
      AND origin != 'repick'
    ORDER BY id
  `).all(workDate);

  const doneLines = db.prepare(`
    SELECT COUNT(*) AS n FROM pk_lines l JOIN pk_batches b ON b.id = l.batch_id
    WHERE b.work_date = ? AND b.validity = 'valid' AND b.status != 'cancelled'
      AND b.origin != 'repick' AND l.status != 'pending'
  `).get(workDate).n;

  const totalLines = batches.reduce((s, b) => s + b.line_count, 0);
  const done = batches.filter((b) => b.status === 'done');
  const active = batches
    .filter((b) => b.status === 'picking' || b.status === 'paused')
    .map((b) => ({
      id: b.id,
      folder: b.folder_name,
      hikiateClass: b.hikiate_class,
      name: displayWorkerName(b.worker),
      paused: b.status === 'paused',
    }));

  const activeSec = done.reduce((s, b) => (
    b.started_at && b.finished_at
      ? s + Math.max(0, Math.round((Date.parse(b.finished_at) - Date.parse(b.started_at)) / 1000) - (b.paused_total_sec || 0))
      : s
  ), 0);
  const doneLineCount = done.reduce((s, b) => s + b.line_count, 0);

  return {
    workDate,
    batchCount: batches.length,
    doneCount: done.length,
    totalLines,
    doneLines,
    remainingLines: Math.max(0, totalLines - doneLines),
    secPerLine: doneLineCount > 0 ? activeSec / doneLineCount : null,
    active,
  };
}

// ═══ 🔴ピッキング漏れバッチ (2026-08-21 中原さん指示) ═══════════════════════
// 梱包からの再ピック依頼 (不足・品違い — pk_pack_tasks kind='repick') を、タスク一覧では
// なく通常のピッキングバッチとして生成する。ロケ・依頼元 (出荷_XX #伝票)・依頼者を表示し、
// 普段のピッキング画面で消化する。origin='repick' は計測 (サマリ/統計/ボード/フロア) の対象外。

/**
 * 再ピックタスク → ピッキング漏れバッチを生成 (タスク1つ=バッチ1つ・tb_no で冪等)。
 * pk_batches / pk_lines は picking 所有のため、packing からはこの関数を呼ぶ (要件§7.1)。
 * @param task pk_pack_tasks の行 (id/sku/product_name/req_qty/location/block/folder_name/slip_seq/requested_by)
 */
/**
 * 欠品記録の後始末 (back と バッチ取消 cancel で共用・欠品フローv2 PR2)。
 * 配賦を消し、「後で取りに行く」依頼を取り下げ、そのために作った未着手タスクを取消し、
 * そのために保留にした梱包伝票を (他に生きた repick が無ければ) 戻す。
 * 🚨 ピッカーが既に対応を始めている (自前タスクまたは合流先が requested/cancelled 以外) なら
 *    PkError 409 later_in_progress で**操作そのものを拒否**する — 黙って戻すと、取りに行った商品と
 *    取り消された記録がズレて二重ピックになる。呼び出し側のトランザクション内で使う (throw で巻き戻る)。
 */
function undoShortageSideEffects(db, batchId, lineSeq, now) {
  const lr = db.prepare(`SELECT * FROM pk_later_requests
    WHERE batch_id=? AND line_seq=? AND status IN ('pending_binding','requested')
    ORDER BY id DESC LIMIT 1`).get(batchId, lineSeq);
  if (lr) {
    let tasks = [];
    let mergedBusy = false;
    try {
      // cancelled 以外は全部見る — received (梱包者が受け取った後) や unavailable も
      // 「対応が始まった」に含める (Codex High: 受領後に欠品記録だけ消せていた)
      tasks = db.prepare(`SELECT * FROM pk_pack_tasks
        WHERE later_request_id=? AND status != 'cancelled'`).all(lr.id);
      // 梱包側の再ピックへ合流していた分も、その合流先が着手済みなら同じ扱い
      // (全量合流だと自前のタスクが無く、ここを見ないと着手後に back できてしまう — Codex R7)
      const mergedIds = String(lr.merged_task_ids || '').split(',').map((x) => Number(x)).filter((x) => x > 0);
      if (mergedIds.length > 0) {
        mergedBusy = db.prepare(`SELECT 1 FROM pk_pack_tasks
          WHERE id IN (${mergedIds.map(() => '?').join(',')}) AND status NOT IN ('requested','cancelled')`)
          .get(...mergedIds) != null;
      }
    } catch { /* packing無効環境 */ }
    if (mergedBusy || tasks.some((t) => t.status !== 'requested')) {
      throw new PkError(409, 'later_in_progress',
        '「後で取りに行く」分は既に対応が始まっているため取り消せません');
    }
    for (const t of tasks) {
      // 未着手のみ取消 (CAS)。pk_pack_tasks は packing 所有だが、
      // 未着手の依頼の取り下げは依頼者 (ピッカー) の操作として許す
      db.prepare("UPDATE pk_pack_tasks SET status='cancelled', updated_at=? WHERE id=? AND status='requested'")
        .run(now, t.id);
      if (t.slip_seq != null) {
        // この依頼のために保留にした梱包伝票は、他に生きた repick が無ければ戻す
        const other = db.prepare(`SELECT 1 FROM pk_pack_tasks
          WHERE batch_id=? AND slip_seq=? AND kind='repick'
            AND status IN ('requested','claimed','fulfilled')`).get(t.batch_id, t.slip_seq);
        if (!other) {
          db.prepare(`UPDATE pk_pack_slips SET status='pending', hold_reason=NULL
            WHERE batch_id=? AND seq=? AND status='held' AND hold_reason='repick'`)
            .run(t.batch_id, t.slip_seq);
        }
      }
    }
    db.prepare("UPDATE pk_later_requests SET status='cancelled', updated_at=? WHERE id=?")
      .run(now, lr.id);
    // タスクは依頼と1対1なので、兄弟依頼 (別ロケ明細由来) のタスクには触らない
  }
  db.prepare('DELETE FROM pk_shortage_allocations WHERE batch_id=? AND line_seq=?')
    .run(batchId, lineSeq);
  // 1階へ出した欠品バナー (🕒/❌) も閉じる (取り消した欠品が1階に残らない)。同一トランザクション・失敗は伝播
  resolveFloorAlertsByRef(`alloc:${batchId}:${lineSeq}:`, { prefix: true, dbh: db });
}

/**
 * 欠品の残り (remQty) を受注に配賦する (欠品フローv2 PR2・要件§4.4)。
 * ルール (Q2既定) = **ピッキング順の後ろの受注から不足扱い** (先に取れた分は前の受注へ)。
 * 受注の順序 = pk_slip_lines の登録順 (CSV = 納品書順) → 後ろ = id の降順。
 * 同一明細の再欠品 (back後のやり直し) で二重にならないよう、必ず作り直す。
 */
function recordShortageAllocations(db, batch, line, lineSeq, { remQty, rem, worker }, now) {
  db.prepare('DELETE FROM pk_shortage_allocations WHERE batch_id=? AND line_seq=?').run(batch.id, lineSeq);
  if (rem == null || remQty <= 0) return;
  // 受注単位に集約する (同一受注に同じSKUの行が複数あると、行ごとに控除が重複して
  //   本来使える数量まで「使用済み」に見える — Codex Medium)。順序は最後に出た行で決める
  const slipRows = db.prepare(`SELECT ne_slip_no, SUM(qty) AS qty, MAX(id) AS last_id FROM pk_slip_lines
    WHERE batch_id=? AND LOWER(TRIM(sku))=LOWER(TRIM(?)) AND ne_slip_no IS NOT NULL
    GROUP BY ne_slip_no ORDER BY last_id DESC`).all(batch.id, line.sku);
  // 🚨 同一SKUが複数ロケ (複数の pk_lines) にある場合、別の明細の欠品が既に同じ受注へ
  //    配賦していることがある。控除しないと「注文1個に欠品2個」を作る (Codex High)
  const taken = new Map();
  for (const r of db.prepare(`SELECT ne_slip_no, SUM(qty) AS q FROM pk_shortage_allocations
      WHERE batch_id=? AND LOWER(TRIM(sku))=LOWER(TRIM(?)) AND line_seq != ?
      GROUP BY ne_slip_no`).all(batch.id, line.sku, lineSeq)) {
    taken.set(r.ne_slip_no, r.q);
  }
  const ins = db.prepare(`INSERT INTO pk_shortage_allocations
    (batch_id, line_seq, sku, ne_slip_no, qty, kind, created_at) VALUES (?,?,?,?,?,?,?)`);
  let left = remQty;
  for (const r of slipRows) {
    if (left <= 0) break;
    const avail = r.qty - (taken.get(r.ne_slip_no) || 0);
    if (avail <= 0) continue;
    const take = Math.min(left, avail);
    ins.run(batch.id, lineSeq, line.sku, r.ne_slip_no, take, rem, now);
    left -= take;
  }
  if (left > 0) {
    // 受注に結べない残り (伝票明細の欠けた旧データ等)。欠品自体の記録はサマリが持つ
    console.warn(`[picking] 欠品の配賦が ${left}個ぶん受注に結べません (batch=${batch.id} seq=${lineSeq} ${line.sku})`);
  }
  if (rem === 'later') {
    db.prepare(`INSERT INTO pk_later_requests
      (batch_id, line_seq, sku, product_name, qty, from_block, from_location, requested_by, status, created_at, updated_at)
      VALUES (?,?,?,?,?,?,?,?, 'pending_binding', ?, ?)`)
      .run(batch.id, lineSeq, line.sku, line.product_name || null, remQty,
        line.block || null, line.location || null, worker, now, now);
  }
}

/** 通知・画面用: その明細の配賦先 (受注×数量)。テーブル未作成環境では空。 */
export function listShortageAllocations(batchId, lineSeq) {
  try {
    return getDB().prepare(`SELECT id, ne_slip_no, qty, kind, created_at FROM pk_shortage_allocations
      WHERE batch_id=? AND line_seq=? ORDER BY id`).all(batchId, lineSeq);
  } catch { return []; }
}

/**
 * 欠品バナー (pk_floor_alerts.ref_key) のキー = 配賦 1 行。配賦 ID を含める = 戻る→再欠品で作り直された配賦は
 * 別キーになり、1階が閉じた古いバナーと同一秒でも衝突しない (Codex R2 High)。packing 側の解決もこれを使う。
 * @param {{batch_id:number, line_seq:number, ne_slip_no:string, id:number}} a 配賦行
 */
export function shortageAllocRefKey(a) {
  return `alloc:${a.batch_id}:${a.line_seq}:${a.ne_slip_no}:${a.id}`;
}

/**
 * 「後で取りに行く」依頼を、取込済みの梱包バッチへ展開する (欠品フローv2 PR2・要件§4.3)。
 * 展開 = 配賦先の伝票ごとに pk_pack_tasks(kind='repick') を作り、伝票を保留にする。
 * その先は**既存の再ピック機構がそのまま動く**:
 *   reconcileRepickBatches ② がタスクから「🕒後で取りに行く」1行バッチをピッキング一覧に出し、
 *   完了→梱包ヘッダー緑バナー→「受け取った」で伝票の保留が解ける。
 * 呼び出し: 欠品直後 (router)・picking一覧表示 (reconcile)・packingポーラー。冪等。
 */
export function bindPendingLaterRequests() {
  const db = getDB();
  let pend = [];
  try {
    pend = db.prepare(`SELECT lr.*, b.tb_no FROM pk_later_requests lr
      JOIN pk_batches b ON b.id = lr.batch_id
      WHERE lr.status = 'pending_binding'`).all();
  } catch { return 0; }   // v11未適用環境
  let bound = 0;
  const now = utcNow();
  for (const lr of pend) {
    try { if (bindLaterRequest(db, lr, now)) bound++; }
    catch (e) { console.warn(`[picking] later依頼の展開失敗 (id=${lr.id}): ${e.message}`); }
  }
  return bound;
}

function bindLaterRequest(db, lr, now) {
  return db.transaction(() => {
    // 並行呼び出し (一覧表示とポーラー) の二重展開ガード
    const cur = db.prepare('SELECT status FROM pk_later_requests WHERE id=?').get(lr.id);
    if (!cur || cur.status !== 'pending_binding') return false;
    const pb = db.prepare(`SELECT * FROM pk_pack_batches
      WHERE tb_key=? AND status != 'cancelled' ORDER BY id DESC LIMIT 1`).get(lr.tb_no);
    if (!pb) return false;   // 梱包が未取込 — 次回の呼び出しで追いつく
    const allocs = db.prepare(`SELECT ne_slip_no, SUM(qty) AS qty FROM pk_shortage_allocations
      WHERE batch_id=? AND line_seq=? AND kind='later' GROUP BY ne_slip_no`).all(lr.batch_id, lr.line_seq);
    const insTask = db.prepare(`INSERT INTO pk_pack_tasks
      (batch_id, slip_seq, kind, sku, product_name, req_qty, location, block, folder_name,
       status, requested_by, later_request_id, created_at, updated_at)
      VALUES (?, ?, 'repick', ?, ?, ?, ?, ?, ?, 'requested', ?, ?, ?, ?)`);
    let covered = 0;
    const mergedTaskIds = new Set();
    for (const a of allocs) {
      const slip = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND ne_slip_no=?')
        .get(pb.id, a.ne_slip_no);
      // 伝票が見つからない → 伝票なしタスク側へ回す (下)。
      // 🚨 完了済み (done) は**保留へ戻す**対象 — bind より先に梱包が完了していた場合、
      //    そのまま出荷すると商品が足りない箱が出る (Codex High)。
      //    梱包側の「不足かも？」も done 伝票を held に戻す仕様で、これと同じ扱い
      // 🚨 別の理由で保留中 (配送方法変更 shipping_change 等) の伝票は触らない。
      //    hold_reason を repick に書き換えると、受領時に pending へ戻って元の保留を迂回する
      //    (Codex High)。その分は伝票なしタスクへ回す (取りには行く)
      const bindable = ['pending', 'done'].includes(slip?.status)
        || (slip?.status === 'held' && slip.hold_reason === 'repick');
      if (!slip || !bindable) continue;
      // 同じ伝票×SKUの生きた repick が既にあれば (梱包側が先に「不足かも？」を出した等)、
      // それは同一の物理不足なので合流する — ただし**数量で**判断する (Codex High)。
      // 既存が1個・今回3個なら差分2個のタスクを作る。1個で伝票を解除できてしまうのを防ぐ。
      // 二重に同数を作ると1行バッチが2本出て同じ商品を二度取りに行くので、差分だけ
      // ⭐タスクは依頼ごとに1対1 (later_request_id で出自を持つ)。
      //   配賦の段階で「受注の注文数を超えない」ことは保証済み (recordShortageAllocations の
      //   taken 控除) なので、別ロケ由来の依頼どうしが同じ伝票へ配賦していても数量は重ならない。
      //   → 各依頼は自分の配賦ぶんだけ作ればよく、兄弟依頼のタスクを「既にある」と見なさない
      //     (見なすと後続の数量が落ちる・back の着手判定が出自とズレる・received 後に二重に
      //      取りに行く — Codex R4/R5)
      const own = db.prepare(`SELECT COALESCE(SUM(req_qty), 0) AS q FROM pk_pack_tasks
        WHERE later_request_id=? AND slip_seq=? AND status != 'cancelled'`).get(lr.id, slip.seq).q;
      // 梱包側が自分で出した再ピック (later_request_id IS NULL) と同じ伝票×SKU = 同一の物理不足。
      // 二重に取りに行かないよう、**最初に展開する依頼だけ**がその数量ぶんを合流 (差し引く)。
      // 2件目以降の依頼は差し引かない (余分に取りに行く側に倒す。余りは棚へ戻せる)
      // 「先に展開済みの兄弟依頼」は**タスクの有無ではなく依頼の記録**で見る。
      // 1件目が梱包側タスクに全量吸収されるとタスクが残らず、2件目も同じ数量を差し引いて
      // 合計が足りなくなる (Codex R6)。展開済み = pk_later_requests.status='requested' で、
      // 同じ受注へ配賦しているもの
      const siblingBound = db.prepare(`SELECT 1 FROM pk_later_requests r
        JOIN pk_shortage_allocations al
          ON al.batch_id = r.batch_id AND al.line_seq = r.line_seq AND al.kind = 'later'
        WHERE r.batch_id=? AND r.id != ? AND r.status = 'requested'
          AND LOWER(TRIM(r.sku))=LOWER(TRIM(?)) AND al.ne_slip_no=?`)
        .get(lr.batch_id, lr.id, lr.sku, a.ne_slip_no);
      const packerTasks = siblingBound ? [] : db.prepare(`SELECT id, req_qty, status FROM pk_pack_tasks
        WHERE batch_id=? AND kind='repick' AND LOWER(TRIM(sku))=LOWER(TRIM(?)) AND slip_seq=?
          AND later_request_id IS NULL AND status IN ('requested','claimed','fulfilled') ORDER BY id`)
        .all(pb.id, lr.sku, slip.seq);
      const packerQty = packerTasks.reduce((s, t) => s + t.req_qty, 0);
      const merged = Math.min(a.qty, packerQty);
      // 合流した先を依頼に記録する — 合流先が着手済みなら back を拒否するため (Codex R7)。
      // 全量合流だと自前のタスクが残らず、back の判定材料がこれしか無い。
      // ⭐記録するのは合流数量ぶんに達するまでのタスクだけ。未着手 (requested) を優先して
      //   割り当てる — 賄えるのに着手済みのタスクまで記録すると back/再取込が恒久的に 409 になる (Codex R9)
      let remain = merged;
      const ordered = [...packerTasks].sort((x, y) =>
        ((x.status === 'requested' ? 0 : 1) - (y.status === 'requested' ? 0 : 1)) || (x.id - y.id));
      for (const t of ordered) {
        if (remain <= 0) break;
        mergedTaskIds.add(t.id);
        remain -= t.req_qty;
      }
      const need = a.qty - own - merged;
      if (need > 0) {
        insTask.run(pb.id, slip.seq, lr.sku, lr.product_name, need,
          lr.from_location || null, lr.from_block || null, pb.folder_name, lr.requested_by, lr.id, now, now);
      }
      // 商品が無いまま梱包を完了できないよう保留に (done は done_at=NULL で戻す・held は不変)
      db.prepare(`UPDATE pk_pack_slips SET status='held', hold_reason='repick', done_at=NULL WHERE id=?`)
        .run(slip.id);
      // 合流した既存タスクは back で取り消さない (依頼主が別のため) — 意図的
      covered += a.qty;
    }
    if (covered < lr.qty) {
      // 受注に結べない残り。取りに行くこと自体は必要なので、伝票なしのタスクで一覧に出す。
      // 重複判定は**この依頼由来**の伝票なしタスクに限る (別の明細の依頼は別の物理不足)
      const existingLoose = db.prepare(`SELECT COALESCE(SUM(req_qty), 0) AS q FROM pk_pack_tasks
        WHERE batch_id=? AND kind='repick' AND slip_seq IS NULL AND later_request_id=?
          AND status != 'cancelled'`).get(pb.id, lr.id).q;
      const need = (lr.qty - covered) - existingLoose;
      if (need > 0) {
        insTask.run(pb.id, null, lr.sku, lr.product_name, need,
          lr.from_location || null, lr.from_block || null, pb.folder_name, lr.requested_by, lr.id, now, now);
      }
    }
    db.prepare("UPDATE pk_later_requests SET status='requested', merged_task_ids=?, updated_at=? WHERE id=?")
      .run(mergedTaskIds.size ? [...mergedTaskIds].join(',') : null, now, lr.id);
    return true;
  })();
}

/**
 * 梱包バッチの再取込 (overwrite) 前処理: このバッチへ展開済みの「後で取りに行く」を
 * 依頼 (pending_binding) へ戻す (欠品フローv2 PR2・Codex High)。
 *
 * タスクの slip_seq は再取込で**別のお客さまの伝票**を指し得る不安定な参照。
 * 展開済みタスクを残したまま伝票を作り直すと、無関係の伝票が保留/解除される。
 * → 未着手タスクを取消して依頼へ戻し、再取込後に bindPendingLaterRequests が
 *   新しい伝票に対して展開し直す (伝票の保留は overwrite が全削除→再作成するので触らない)。
 * 🚨 ピッカーが既に動いている (claimed/fulfilled) 分があれば e.code='later_in_progress' を
 *   投げて**再取込側を止める** (取りに行った商品の行き先が消えるため)。
 * packing importPackBatch のトランザクション内から呼ばれる (同一 SQLite)。
 */
export function resetLaterBindingsForPackBatch(db, packBatchId) {
  // 起点は**依頼** (タスクではなく)。全量合流した依頼は自前のタスクを持たないので、
  // タスク起点だと見落として合流先が着手中でも再取込が通る (Codex R8)
  let lrs = [];
  try {
    lrs = db.prepare(`SELECT r.* FROM pk_later_requests r
      JOIN pk_batches b ON b.id = r.batch_id
      JOIN pk_pack_batches pb ON pb.tb_key = b.tb_no
      WHERE pb.id = ? AND r.status = 'requested'`).all(packBatchId);
  } catch { return 0; }   // v11/v16未適用環境
  if (lrs.length === 0) return 0;
  const ids = lrs.map((r) => r.id);
  const ph = ids.map(() => '?').join(',');
  // 自前タスク: cancelled 以外は全部 — unavailable (梱包開始前でもピッカーが「在庫なし」にできる) や
  // received も、古い slip_seq を抱えたまま残るので再取込の妨げになる
  const own = db.prepare(`SELECT * FROM pk_pack_tasks
    WHERE later_request_id IN (${ph}) AND status != 'cancelled'`).all(...ids);
  // 合流先 (梱包側の再ピック) の着手も同じ扱い
  const mergedIds = [...new Set(lrs.flatMap((r) => String(r.merged_task_ids || '').split(',')
    .map((x) => Number(x)).filter((x) => x > 0)))];
  const mergedBusy = mergedIds.length === 0 ? [] : db.prepare(`SELECT * FROM pk_pack_tasks
    WHERE id IN (${mergedIds.map(() => '?').join(',')}) AND status NOT IN ('requested','cancelled')`).all(...mergedIds);
  const busy = [...own.filter((t) => t.status !== 'requested'), ...mergedBusy];
  if (busy.length > 0) {
    const e = new Error(`「後で取りに行く」の対応が始まっています (${[...new Set(busy.map((t) => t.sku))].join(', ')})。完了 (受け取った) まで進めてから再取込してください`);
    e.code = 'later_in_progress';
    throw e;
  }
  const now = utcNow();
  for (const t of own) {
    db.prepare("UPDATE pk_pack_tasks SET status='cancelled', updated_at=? WHERE id=? AND status='requested'")
      .run(now, t.id);
  }
  // 依頼を展開前に戻す。合流の記録も消す (再取込後の伝票に対して判断し直すため)
  db.prepare(`UPDATE pk_later_requests SET status='pending_binding', merged_task_ids=NULL, updated_at=?
    WHERE id IN (${ph}) AND status='requested'`).run(now, ...ids);
  return lrs.length;
}

/**
 * ↩ 棚戻しの戻し先候補 (例外処理監査 PR-4・D-2)。
 *   picked = 取った場所 (依頼元バッチのピックロケ。無ければ確定時に入れたロジザード候補 = location_source 'stock')
 *   rows   = ロジザードの在庫ロケ (良品・フリー在庫の多い順・同一ロケはまとめる)。picked と同じロケは除く
 * 在庫参照が未設定・障害のときは rows=[] で fetched=false (画面は「取った場所」と手入力だけ出す — fail-soft)
 * @param task pk_pack_tasks の行 (sku/location/block/location_source)
 */
export async function returnCandidates(task, { fetchFn = fetch, now = new Date(), maxRows = 8 } = {}) {
  const sl = await import('./stock-locations.js');
  const picked = task?.location
    ? { block: task.block || null, location: task.location, label: formatLocation(task.block, task.location), source: task.location_source || 'picked' }
    : null;
  const base = { ok: true, picked, rows: [], fetched: false, stale: true, stamp: null, truncated: 0, configured: sl.stockLookupConfigured() };
  if (!base.configured || !task?.sku) return base;
  const data = await fetchStockLocationsSafe(sl, task.sku, fetchFn);
  const c = sl.listStockCandidates(data, { groupByLocation: true, maxRows, now });
  const same = (r) => picked && String(r.block || '') === String(picked.block || '')
    && sl.normalizeLocationDigits(r.location) === sl.normalizeLocationDigits(picked.location);
  return { ...base, rows: c.rows.filter((r) => !same(r)), fetched: c.fetched, stale: c.stale, stamp: c.stamp, truncated: c.truncated || 0 };
}
async function fetchStockLocationsSafe(sl, sku, fetchFn) {
  try { return await sl.fetchStockLocations(sku, fetchFn); } catch { return null; }
}

export function createRepickBatch(task) {
  const db = getDB();
  const now = utcNow();
  const tbNo = `REPICK-${task.id}`;
  return db.transaction(() => {
    const existing = db.prepare('SELECT id FROM pk_batches WHERE tb_no = ?').get(tbNo);
    if (existing) return { batchId: existing.id, existed: true };
    const originRef = `${task.folder_name || '-'}${task.slip_seq ? ` #${task.slip_seq}` : ''}`;
    // folder_name は入れない (Notionカード・shipping-log 突合を誤爆させない)。依頼元は origin_ref
    const info = db.prepare(`
      INSERT INTO pk_batches (tb_no, hikiate_class, folder_name, work_date, instruct_date, composition,
        delivery_method, invoice_soft, line_count, slip_count, total_qty, status, validity,
        csv_sha256, imported_by, created_at, updated_at, origin, origin_ref, requested_by, pack_task_id)
      VALUES (?, 'ピッキング漏れ', NULL, ?, NULL, '単品', NULL, NULL, 1, 1, ?, 'ready', 'valid',
        ?, ?, ?, ?, 'repick', ?, ?, ?)
    `).run(tbNo, jstToday(), task.req_qty, `repick-task-${task.id}`,
      task.requested_by || 'packing', now, now, originRef, task.requested_by || null, task.id);
    const batchId = Number(info.lastInsertRowid);
    db.prepare(`
      INSERT INTO pk_lines (batch_id, seq, location, block, sku, product_name, barcode, qty)
      VALUES (?, 1, ?, ?, ?, ?, NULL, ?)
    `).run(batchId, task.location || '', task.block || null, task.sku, task.product_name || null, task.req_qty);
    return { batchId, existed: false };
  })();
}

/**
 * ピッキング漏れバッチと梱包タスクの整合回復 (一覧表示のたびに軽く同期)。
 * 梱包側でタスクが取消 (バッチ取消・「出てきた」取下げ) されたら、未着手/作業中の
 * 漏れバッチも取消する。pk_pack_tasks は参照のみ (packing 所有)。
 */
export function reconcileRepickBatches() {
  const db = getDB();
  // 「後で取りに行く」依頼の展開 (欠品時に梱包が未取込だった分の追いつき)。
  // 展開されたタスクは直後の②で「🕒後で取りに行く」1行バッチになる
  try { bindPendingLaterRequests(); } catch { /* fail-soft */ }
  // 1階の欠品バナー (🕒/❌) も収束させる (取込前の欠品・障害で作れなかった分の追いつき)
  try { reconcileShortageAlerts(); } catch { /* fail-soft */ }
  try {
    // ①梱包側で取消されたタスクのバッチを畳む
    const rows = db.prepare(`
      SELECT b.id FROM pk_batches b JOIN pk_pack_tasks t ON t.id = b.pack_task_id
      WHERE b.origin = 'repick' AND b.validity = 'valid'
        AND b.status IN ('ready','picking','paused') AND t.status = 'cancelled'
    `).all();
    const now = utcNow();
    for (const r of rows) {
      db.prepare("UPDATE pk_batches SET status='cancelled', validity='invalid', updated_at=? WHERE id=?")
        .run(now, r.id);
    }
    // ②バッチ未生成の再ピックタスクを拾って生成 (Codexレビュー: resolve時の生成が
    // 一時障害で失敗しても、一覧を開くたびにここで必ず収束する。tb_no冪等)
    const missing = db.prepare(`
      SELECT t.* FROM pk_pack_tasks t
      WHERE t.kind = 'repick' AND t.status IN ('requested','claimed')
        AND NOT EXISTS (SELECT 1 FROM pk_batches b WHERE b.pack_task_id = t.id)
    `).all();
    for (const t of missing) {
      try { createRepickBatch(t); } catch (e) { console.warn(`[picking] 漏れバッチ再生成失敗 (task=${t.id}): ${e.message}`); }
    }
    return rows.length + missing.length;
  } catch {
    return 0;   // pk_pack_tasks が無い環境 (packing無効・テスト) では何もしない
  }
}

// ═══ 現場間アラート (ピッキング⇄梱包 — 2026-08-21 中原さん指示) ═══════════════
// バッチ一覧のボタン → 相手システムの全画面ヘッダーにバナー表示。OKで消える。
// テーブルは picking 所有 (pk_floor_alerts)。packing からもこの関数経由で読み書きする

export const FLOOR_ALERT_KINDS = {
  cart:    { direction: 'to_packing', message: '🛒 ピッキングカートを送ってください' },
  trolley: { direction: 'to_packing', message: '🧺 台車を送ってください' },
  lift:    { direction: 'to_packing', message: '🛗 リフトの中身を出してください' },
  unload:  { direction: 'to_picking', message: '📦 ピッキング済みの商品を下してください' },
  repick_done: { direction: 'to_packing', message: null },   // 不足分ピッキング完了 (メッセージは依頼ごとに動的)
  // 3階「在庫なし」→ 1階の全端末へ (例外処理監査 PR-1)。メッセージは依頼ごとに動的・link で対象伝票へ飛ぶ
  stockout: { direction: 'to_packing', message: null },
  // ピッカーの欠品 (🕒 後で取りに行く / ❌ どこにもない) → 1階の全端末へ (例外処理監査 PR-2・Q2 決定 2026-09-05)。
  // 配賦した受注 (伝票) ごとに1本。ref_key='alloc:<batch>:<seq>:<ne_slip_no>' で back / 1階の処理と同時に閉じる
  picking_shortage: { direction: 'to_packing', message: null },
};

/** アラート発報。同種の未確認が生きていれば重ねない (連打・二重依頼の集約)。 */
export function createFloorAlert(kind, requestedBy, customMessage = null, link = null, taskId = null, refKey = null) {
  const def = FLOOR_ALERT_KINDS[kind];
  if (!def) throw new PkError(400, 'bad_kind', '不明なアラート種別です');
  const message = def.message || String(customMessage || '').slice(0, 160);
  if (!message) throw new PkError(400, 'no_message', 'メッセージが必要です');
  const db = getDB();
  // 集約チェック+INSERTは同一トランザクション (Codex: 同時押下の重複防止)。
  // 集約キーは (kind, message) — repick_done は依頼ごとにメッセージが違うため別々に出る。
  // task_id 付き (在庫なし) は (kind, task_id)、ref_key 付き (欠品の配賦) は (kind, ref_key) で集約 = 未解決バナーは1本だけ
  return db.transaction(() => {
    const dup = taskId != null
      ? db.prepare(`SELECT id, message, link FROM pk_floor_alerts
          WHERE kind = ? AND task_id = ? AND acked_at IS NULL AND resolved_at IS NULL`).get(kind, taskId)
      : refKey
        ? db.prepare(`SELECT id, message, link FROM pk_floor_alerts
          WHERE kind = ? AND ref_key = ? AND acked_at IS NULL AND resolved_at IS NULL`).get(kind, refKey)
        : db.prepare(`SELECT id, message, link FROM pk_floor_alerts
          WHERE kind = ? AND message = ? AND acked_at IS NULL AND resolved_at IS NULL AND created_at >= datetime('now', '-4 hours')`).get(kind, message);
    if (dup) {
      // ref_key 付きは内容を更新する (梱包バッチの取込・再取込で伝票番号やリンクが変わる — Codex R1 High)
      const newLink = link ? String(link).slice(0, 200) : null;
      if (refKey && (dup.message !== message || (dup.link || null) !== newLink)) {
        db.prepare('UPDATE pk_floor_alerts SET message=?, link=? WHERE id=?').run(message, newLink, dup.id);
        return { id: dup.id, existed: true, updated: true };
      }
      return { id: dup.id, existed: true };
    }
    const info = db.prepare(`
      INSERT INTO pk_floor_alerts (direction, kind, message, requested_by, created_at, link, task_id, ref_key)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(def.direction, kind, message, requestedBy || null, utcNow(), link ? String(link).slice(0, 200) : null, taskId, refKey);
    return { id: Number(info.lastInsertRowid), existed: false };
  }).immediate();
}

/** 表示対象 (未確認・未解決・4時間以内 — 古いバナーを翌日まで残さない)。 */
export function listFloorAlerts(direction) {
  return getDB().prepare(`
    SELECT id, kind, message, requested_by, created_at, link, task_id, ref_key FROM pk_floor_alerts
    WHERE direction = ? AND acked_at IS NULL AND resolved_at IS NULL AND created_at >= datetime('now', '-4 hours')
    ORDER BY id
  `).all(direction);
}

/**
 * ref_key で紐づくバナーを閉じる (ピッカーの欠品バナー)。prefix=true なら前方一致 (back で明細ぶん全部、
 * 1階の処理で受注ぶん全部)。packing のトランザクション内からは同じ接続 (dbh) で呼ぶ
 */
export function resolveFloorAlertsByRef(refKey, { prefix = false, dbh = null, failSoft = false } = {}) {
  // 業務トランザクションの中 (stockout_ack / receive / back) では例外を伝播させる — 握りつぶすと
  // 業務更新だけ commit されてバナーが残る (Codex R2 Low)。旧環境の表示補助だけ failSoft
  const run = () => {
    const d = dbh || getDB();
    if (prefix) {
      // LIKE のメタ文字 (% _) をエスケープして前方一致だけにする
      const esc = String(refKey).replace(/[\\%_]/g, (c) => `\\${c}`);
      return d.prepare(`UPDATE pk_floor_alerts SET resolved_at=? WHERE ref_key LIKE ? ESCAPE '\\' AND resolved_at IS NULL`)
        .run(utcNow(), `${esc}%`).changes;
    }
    return d.prepare(`UPDATE pk_floor_alerts SET resolved_at=? WHERE ref_key = ? AND resolved_at IS NULL`).run(utcNow(), refKey).changes;
  };
  if (!failSoft) return run();
  try { return run(); } catch { return 0; }
}

/**
 * ピッカーの欠品 (🕒 後で取りに行く / ❌ どこにもない) を1階の全端末へ知らせる (例外処理監査 PR-2)。
 * 配賦した受注 (伝票) ごとに1本。梱包バッチが取込済みなら対象伝票へのリンクを付ける。
 * 他ロケで全量確保した欠品 (remaining なし) は1階に関係ないので出さない。
 * 正本は pk_shortage_allocations。冪等 (ref_key で集約・内容が変われば更新) なので、shortage 直後だけでなく
 * replay・ピッキング一覧の表示・梱包バッチの取込/再取込からも呼んで収束させる (Codex R1 High: 一度きりだと恒久的に漏れる)。
 * @param workerName 省略時はその明細の欠品イベントの担当者
 * @returns 作った/更新したバナーの数
 */
export function announceShortageToPacking(batchId, lineSeq, workerName = null) {
  const db = getDB();
  const line = db.prepare('SELECT * FROM pk_lines WHERE batch_id=? AND seq=?').get(batchId, lineSeq);
  if (!line || line.status !== 'shortage' || !line.remaining) return 0;
  const batch = getBatch(batchId);
  if (!batch || batch.origin === 'repick') return 0;
  const ev = db.prepare(`SELECT worker, at FROM pk_events WHERE batch_id=? AND line_seq=? AND event='shortage' ORDER BY id DESC LIMIT 1`)
    .get(batchId, lineSeq);
  const who = workerName || ev?.worker || batch.worker || '-';
  const when = ev?.at || utcNow();
  let pb = null;
  try {
    // 突合済みの梱包バッチ (pk_batch_id) を優先。無ければ tb_key で
    // 突合済み (pk_batch_id = 自分) を優先。tb_key の代替は「未突合」の梱包バッチだけ — 別の picking バッチに突合済みの
    // ものへリンクすると、梱包側 (pickingBatchIdFor) はその配賦を見せないので「バナーはあるが確認できない」になる (Codex R2)
    pb = db.prepare(`SELECT id FROM pk_pack_batches WHERE pk_batch_id=? AND validity='valid' AND status != 'cancelled' ORDER BY id DESC LIMIT 1`).get(batchId)
      || db.prepare(`SELECT id FROM pk_pack_batches WHERE tb_key=? AND pk_batch_id IS NULL AND validity='valid' AND status != 'cancelled' ORDER BY id DESC LIMIT 1`).get(batch.tb_no)
      || null;
  } catch { pb = null; }
  const jstHm = (iso) => { const t = Date.parse(iso || ''); return Number.isFinite(t) ? new Date(t + 9 * 3600e3).toISOString().slice(11, 16) : ''; };
  // 1階が既に処理したバナー (× で閉じた / 在庫なし確認・受領で resolved) は収束で復活させない。
  // キーに配賦 ID が入っているので、戻る→再欠品で作り直された配賦は別キー = 新しいバナーが出る (同一秒でも — Codex R2)
  const handled = db.prepare(`SELECT 1 FROM pk_floor_alerts
    WHERE kind='picking_shortage' AND ref_key=? AND (resolved_at IS NOT NULL OR acked_at IS NOT NULL) LIMIT 1`);
  let made = 0;
  for (const a of listShortageAllocations(batchId, lineSeq)) {
    const refKey = shortageAllocRefKey({ batch_id: batchId, line_seq: lineSeq, ne_slip_no: a.ne_slip_no, id: a.id });
    if (handled.get(refKey)) continue;
    let slip = null;
    if (pb) {
      try { slip = db.prepare('SELECT seq FROM pk_pack_slips WHERE batch_id=? AND ne_slip_no=?').get(pb.id, a.ne_slip_no) || null; } catch { slip = null; }
    }
    const ref = `${batch.folder_name || '-'}${slip ? ` #${slip.seq}` : ` (NE ${a.ne_slip_no})`}`;
    const name = line.product_name || line.sku;
    const msg = a.kind === 'none'
      ? `❌ ${ref} ${name} ×${a.qty} — どのロケにもありません (在庫なし) — 3階 ${who} ${jstHm(when)}`
      : `🕒 ${ref} ${name} ×${a.qty} — 後で取りに行きます (届いたら受け取り) — 3階 ${who} ${jstHm(when)}`;
    const link = pb ? `/apps/packing/work/${pb.id}${slip ? `?seq=${slip.seq}` : ''}` : null;
    try {
      const r = createFloorAlert('picking_shortage', who, msg, link, null, refKey);
      if (!r.existed || r.updated) made++;
    } catch (e) { console.warn(`[picking] 欠品バナーの発報失敗: ${e.message}`); }
  }
  return made;
}

/**
 * 欠品バナーの収束 (Codex R1 High): 配賦が残っている明細すべてについて announce を呼び直す。
 * 呼び出し = ピッキング一覧の表示 (reconcileRepickBatches) / 梱包バッチの取込・再取込 (importPackBatch の後)。
 * @param {{tbNo?: string, batchId?: number, sinceDays?: number}} opts 絞り込み (省略時は直近2日のバッチ)
 */
export function reconcileShortageAlerts({ tbNo = null, batchId = null, sinceDays = 2 } = {}) {
  const db = getDB();
  let rows = [];
  try {
    rows = db.prepare(`
      SELECT DISTINCT a.batch_id, a.line_seq FROM pk_shortage_allocations a
      JOIN pk_batches b ON b.id = a.batch_id
      WHERE b.validity = 'valid' AND b.status != 'cancelled'
        AND (? IS NULL OR b.tb_no = ?) AND (? IS NULL OR b.id = ?)
        AND (? IS NOT NULL OR ? IS NOT NULL OR b.work_date >= date('now', ?))
    `).all(tbNo, tbNo, batchId, batchId, tbNo, batchId, `-${Math.max(0, sinceDays)} days`);
  } catch { return 0; }
  let n = 0;
  for (const r of rows) {
    try { n += announceShortageToPacking(r.batch_id, r.line_seq); } catch (e) { console.warn(`[picking] 欠品バナーの収束失敗: ${e.message}`); }
  }
  return n;
}

/**
 * タスク由来のバナーを状態遷移と同時に閉じる (在庫なし → 見つかった/届けた/取り消し/1階が確認)。
 * packing からも呼ぶ (picking 所有テーブルへの書き込みはこの関数経由)。fail-soft (テーブル未適用は 0)
 */
export function resolveFloorAlertsByTask(taskId, kind = 'stockout', dbh = null) {
  try {
    // packing のトランザクション内から呼ぶときは同じ接続 (dbh) を使う — 別接続だと書き込みロック待ちになる
    return (dbh || getDB()).prepare(`UPDATE pk_floor_alerts SET resolved_at=?
      WHERE task_id=? AND kind=? AND resolved_at IS NULL`).run(utcNow(), taskId, kind).changes;
  } catch { return 0; }
}

// 🔴再ピックバッチ → 梱包タスクの同期: 「いまのバッチ状態から望ましいタスク状態」を導いて収束させる。
//   ready → requested / picking・paused → claimed / done → 在庫なしの明細があれば unavailable、なければ fulfilled
// イベント名で分岐しない (back/cancel で取り消した欠品が同期されず、後から fulfilled に化ける穴 — Codex R1 High)。
// 終端 (received/cancelled/returned) には触らない
const TASK_SYNC_TERMINAL = new Set(['received', 'cancelled', 'returned']);
const TASK_SYNC_PATHS = {
  'requested>claimed': ['claim'],
  'requested>fulfilled': ['claim', 'fulfill'],
  'requested>unavailable': ['unavailable'],
  'claimed>fulfilled': ['fulfill'],
  'claimed>unavailable': ['unavailable'],
  'claimed>requested': ['reopen'],
  'unavailable>claimed': ['resume'],
  'unavailable>fulfilled': ['fulfill'],
  'unavailable>requested': ['reopen'],
  'fulfilled>claimed': ['resume'],
  'fulfilled>requested': ['reopen'],
  'fulfilled>unavailable': ['resume', 'unavailable'],
};

/**
 * 🔴再ピックバッチの作業イベント → 梱包タスク (pk_pack_tasks) の状態同期 (例外処理監査 PR-1)。
 * router から毎イベント後に呼ぶ (トランザクション外・fail-soft・replay でも呼ぶ = 収束させる)。
 *   - 他ロケで全量確保して完了 → fulfilled (1階の受領待ち)。以前は shortage を全部 unavailable にしていた
 *     ため、他ロケで確保できても「在庫なし」と記録されていた (9/3・9/5 に実発生)
 *   - 残りありで完了 → unavailable + 1階の全端末へ赤バナー (task_id で1本に集約。未解決が無ければ作り直す
 *     = 障害・replay 後の自己修復) + 部分確保の内訳 (fulfilled_qty / unavailable_qty) をタスクに保存
 *   - back / cancel → claimed / requested へ戻し、赤バナーを閉じる
 * @param psvc packing service (無効環境は null → 何もしない)
 * @returns {{actions: string[], unavailable: null|{task, remaining, altQty}, desired: string|null, status: string|null}}
 *          unavailable = 今回 unavailable へ遷移したときだけ (GChat 通知用。replay では付かない)
 */
export function syncRepickTask(batchId, { event = null } = {}, workerName, psvc) {
  const none = { actions: [], unavailable: null, desired: null, status: null };
  const b = getBatch(batchId);
  if (!b || b.origin !== 'repick' || !b.pack_task_id || !psvc?.getTask) return none;
  const taskId = b.pack_task_id;
  let task = null;
  try { task = psvc.getTask(taskId); } catch (e) { console.warn(`[picking] 漏れバッチのタスク読取失敗 (task=${taskId}): ${e.message}`); return none; }
  if (!task) return none;
  const lines = listLines(batchId);
  const stockoutQty = lines.filter((l) => l.status === 'shortage')
    .reduce((s, l) => s + (Number(l.remaining_qty ?? l.shortage_qty ?? 0) || 0), 0);
  const desired = b.status === 'ready' ? 'requested'
    : (b.status === 'picking' || b.status === 'paused') ? 'claimed'
      : b.status === 'done' ? (stockoutQty > 0 ? 'unavailable' : 'fulfilled')
        : null;   // cancelled/invalid は reconcileRepickBatches が畳む
  const actions = [];
  if (desired && !TASK_SYNC_TERMINAL.has(task.status) && task.status !== desired) {
    for (const action of (TASK_SYNC_PATHS[`${task.status}>${desired}`] || [])) {
      try {
        const extra = action === 'unavailable'
          ? { unavailableQty: Math.min(stockoutQty, task.req_qty), fulfilledQty: Math.max(0, task.req_qty - stockoutQty) }
          : {};
        task = psvc.applyTaskAction(taskId, action, workerName, extra);
        actions.push(action);
      } catch (e) {
        // 別経路 (1階の操作等) で先に動いた等。現状態を読み直して判定に使う
        console.warn(`[picking] 漏れバッチのタスク同期 (${action}) 失敗 (task=${taskId}, event=${event}): ${e.message}`);
        try { task = psvc.getTask(taskId) || task; } catch { /* 読めなければ直前の値 */ }
        break;
      }
    }
    if (task.status !== desired) {
      console.warn(`[picking] 漏れバッチのタスク同期が収束しません (task=${taskId} status=${task.status} desired=${desired} event=${event})`);
    }
  }
  let unavailable = null;
  if (task.status === 'unavailable') {
    const unavailableQty = task.unavailable_qty ?? Math.min(stockoutQty || task.req_qty, task.req_qty);
    const fulfilledQty = task.fulfilled_qty ?? Math.max(0, task.req_qty - unavailableQty);
    if (actions.includes('unavailable')) unavailable = { task, remaining: unavailableQty, altQty: fulfilledQty };
    // 1階の全端末に赤バナー (Q2 決定 2026-09-05: バナーで・分かりやすく = 商品名・伝票・数量・誰が)。
    // task_id で集約するので毎回呼んでよい (未解決が残っていれば作らない)
    try {
      const name = task.product_name || task.sku;
      const ref = `${task.folder_name || '-'}${task.slip_seq ? ` #${task.slip_seq}` : ''}`;
      const who = task.claimed_by || workerName;
      const msg = `🚫 在庫なし: ${ref} ${name} ×${unavailableQty}${fulfilledQty > 0 ? ` (${fulfilledQty}個は届けます)` : ''} — 3階 ${who}`;
      const link = task.slip_seq ? `/apps/packing/work/${task.batch_id}?seq=${task.slip_seq}` : `/apps/packing/work/${task.batch_id}`;
      createFloorAlert('stockout', who, msg, link, task.id);
    } catch (e) { console.warn(`[picking] 在庫なしバナーの発報失敗: ${e.message}`); }
  } else {
    resolveFloorAlertsByTask(taskId, 'stockout');   // 在庫なしでなくなった (届けた/戻した) → バナーを閉じる
  }
  return { actions, unavailable, desired, status: task.status };
}

/** OKタップで確認済みに (全端末から消える)。direction=呼び出し側の表示方向 —
 *  相手方向のアラートを勝手に消せない (Codex high)。 */
export function ackFloorAlert(id, worker, direction) {
  if (!['to_packing', 'to_picking'].includes(direction)) {
    throw new PkError(400, 'bad_direction', '方向が不正です');
  }
  const n = getDB().prepare(
    'UPDATE pk_floor_alerts SET acked_at=?, acked_by=? WHERE id=? AND acked_at IS NULL AND direction=?'
  ).run(utcNow(), worker || null, id, direction).changes;
  if (n === 0) throw new PkError(404, 'not_found', 'アラートが見つからないか確認済みです');
  return true;
}

// ═══ ピッキングミス集計 (2026-08-21 中原さん指示: ダッシュボード表示) ═══════════
// 源泉 = pk_pack_incidents (packing所有・参照のみ)。梱包で検知され「ピッキングへ送信」で
// 確定 (status='confirmed') したものだけを数える (取下げ=誤検知は除外)。
// 帰責 = attributed_worker (確定時点のピッキングバッチ担当) — ヒストリカルに保存済み

/**
 * 期間内のミス集計 (作業者×種別)。
 * @returns {{ byWorker: [{worker,total,shortage,excess,wrong_item,qty}], total: {...} }}
 */
/**
 * 期間内のミス集計 (作業者×種別) — **件数より比率** (中原さん指示 2026-08-31)。
 *
 * - 分母 = その作業者が期間内にピッキングした明細数 (完了バッチの明細。欠品で止めた明細も含む。
 *   外れ値除外はしない = 速さ統計と違い「やった量」なので全部数える)
 * - 比率 = 1,000 明細あたりのミス件数 (per1000)。分母が minLines 未満は参考値 (provisional)
 * - **時間軸と帰属は分母と同じ規則** (Codex R1 High×2):
 *     日付 = そのミスが出たピッキングバッチの work_date (梱包で見つかった日ではない)。
 *     作業者 = その SKU の明細を実際にピッキングした人 (pk_events の最後の next/shortage の担当。交代しても
 *     交代前の明細は前任者に付く — loadStatsLines と同じ)。ピッキングバッチを引けないときだけ
 *     確定時の attributed_worker / created_at の JST 日付にフォールバック
 * - 🚨 **欠品とミスは別物**: 梱包で「不足」として確定したものでも、
 *     (a) ピッキング側でその受注×SKU が欠品 (他ロケにも無く「後で取りに行く」/「どこにもない」 = pk_shortage_allocations)、
 *     (b) 配賦記録を持たない古いバッチ (2026-08-31 以前) で、同じバッチの同 SKU が欠品 (残りあり) だった
 *   なら、棚に無かったのであってピッカーの取り忘れではない → `stockout` として数え、ミスから除く
 *
 * @returns {{ since, until, minLines,
 *   byWorker: [{worker, name, lines, total, shortage, excess, wrong_item, qty, stockout, per1000, provisional}],
 *   total: {lines, total, shortage, excess, wrong_item, qty, stockout, per1000} }}
 */
/** ミス率の分母: 期間内の完了バッチ (有効・再ピック以外) の**全明細** — 速さ統計と違い、表示/完了時刻が
 *  無い明細も数える (Codex R5: loadStatsLines は時刻のある明細だけなので分母が小さくなりミス率が過大になる)。
 *  作業者の帰属は loadStatsLines と同じ (その明細の最後の next/shortage の担当 → 無ければバッチ担当)。 */
function loadMissDenominator(since, until) {
  return getDB().prepare(`
    SELECT COALESCE(
        (SELECT e.worker FROM pk_events e
          WHERE e.batch_id = l.batch_id AND e.line_seq = l.seq AND e.event IN ('next','shortage')
          ORDER BY e.id DESC LIMIT 1),
        b.worker
      ) AS worker, COUNT(*) AS n
    FROM pk_lines l
    JOIN pk_batches b ON b.id = l.batch_id
    WHERE b.work_date >= ? AND b.work_date <= ?
      AND b.validity = 'valid' AND b.status = 'done' AND b.origin != 'repick'
    GROUP BY worker
  `).all(since, until);
}

export function getMissStats({ until = jstToday(), days = STATS_WINDOW_DAYS } = {}) {
  const db = getDB();
  const { since, until: to } = statsRange(until, days);
  const empty = () => ({ lines: 0, total: 0, shortage: 0, excess: 0, wrong_item: 0, qty: 0, stockout: 0, per1000: null });
  const map = new Map();
  const ensure = (worker) => {
    if (!map.has(worker)) map.set(worker, { worker, name: displayWorkerName(worker), ...empty() });
    return map.get(worker);
  };
  // 分母: 期間内にピッキングした明細数 (作業者は明細単位の帰属 = 速さ統計と同じ。時刻の無い明細も含む)
  for (const r of loadMissDenominator(since, to)) ensure(r.worker || '(不明)').lines += r.n;

  // 確定ミスを、対応するピッキングバッチ (取込時の突合 pk_batch_id を優先。無ければ tb_no = tb_key の有効な最新・
  // 再ピックバッチは除く — 同じ tb_no の再取込で別バッチへ誤帰属しないため Codex R4) と受注番号つきで読む。
  // packing のテーブルが無い環境 (picking 単独) では incident だけ読む
  let rows = [];
  try {
    rows = db.prepare(`
      SELECT i.id, i.kind, i.qty, i.sku, i.attributed_worker, i.created_at,
        COALESCE(pb.pk_batch_id,
          (SELECT b.id FROM pk_batches b WHERE b.tb_no = pb.tb_key AND b.origin != 'repick' AND b.validity = 'valid'
             ORDER BY b.id DESC LIMIT 1)) AS pk_batch_id,
        s.ne_slip_no
      FROM pk_pack_incidents i
      LEFT JOIN pk_pack_batches pb ON pb.id = i.batch_id
      LEFT JOIN pk_pack_slips s ON s.batch_id = i.batch_id AND s.seq = i.slip_seq
      WHERE i.status = 'confirmed'
      ORDER BY i.id
    `).all();
  } catch (e) {
    if (/no such table: pk_pack_incidents/.test(String(e.message))) {
      rows = [];
    } else if (/no such table/.test(String(e.message))) {
      rows = db.prepare(`SELECT id, kind, qty, sku, attributed_worker, created_at, NULL AS pk_batch_id, NULL AS ne_slip_no
        FROM pk_pack_incidents WHERE status = 'confirmed' ORDER BY id`).all();
    } else {
      throw e;   // それ以外のDB障害は隠さず投げる (Codex: 「ミス0件」という誤った正常値を出さない)
    }
  }
  const qBatch = db.prepare('SELECT work_date, worker FROM pk_batches WHERE id = ?');
  const qLineWorker = db.prepare(`
    SELECT e.worker FROM pk_events e
    JOIN pk_lines l ON l.batch_id = e.batch_id AND l.seq = e.line_seq
    WHERE l.batch_id = ? AND LOWER(TRIM(l.sku)) = ? AND e.event IN ('next','shortage')
    ORDER BY e.id DESC LIMIT 1`);
  const qAlloc = db.prepare(`SELECT 1 FROM pk_shortage_allocations
    WHERE batch_id = ? AND ne_slip_no = ? AND LOWER(TRIM(sku)) = ? LIMIT 1`);
  const qAnyAlloc = db.prepare('SELECT 1 FROM pk_shortage_allocations WHERE batch_id = ? LIMIT 1');
  const qShortLine = db.prepare(`SELECT 1 FROM pk_lines
    WHERE batch_id = ? AND LOWER(TRIM(sku)) = ? AND status = 'shortage' AND COALESCE(remaining_qty, shortage_qty, 0) > 0 LIMIT 1`);
  const jstDateOf = (iso) => new Date(Date.parse(iso) + 9 * 3600 * 1000).toISOString().slice(0, 10);

  const total = empty();
  for (const r of rows) {
    const skuN = String(r.sku ?? '').trim().toLowerCase();
    const pb = r.pk_batch_id != null ? qBatch.get(r.pk_batch_id) : null;
    const day = pb?.work_date ?? jstDateOf(r.created_at);
    if (day < since || day > to) continue;
    let worker = null;
    let stockout = false;
    if (pb) {
      worker = qLineWorker.get(r.pk_batch_id, skuN)?.worker ?? pb.worker ?? null;
      if (r.kind === 'shortage') {
        stockout = (r.ne_slip_no != null && qAlloc.get(r.pk_batch_id, r.ne_slip_no, skuN) != null)
          || (qAnyAlloc.get(r.pk_batch_id) == null && qShortLine.get(r.pk_batch_id, skuN) != null);
      }
    }
    const w = ensure(worker ?? r.attributed_worker ?? '(担当不明)');
    if (stockout) { w.stockout++; total.stockout++; continue; }   // 欠品 = ミスではない
    w[r.kind] = (w[r.kind] || 0) + 1;
    w.total++;
    w.qty += r.qty || 0;
    total[r.kind] = (total[r.kind] || 0) + 1;
    total.total++;
    total.qty += r.qty || 0;
  }
  for (const w of map.values()) {
    total.lines += w.lines;
    w.per1000 = w.lines > 0 ? Math.round((w.total / w.lines) * 10000) / 10 : null;
    w.provisional = w.lines < STATS_MIN_LINES;
  }
  total.per1000 = total.lines > 0 ? Math.round((total.total / total.lines) * 10000) / 10 : null;
  // 並び = 比率の高い順 (分母ゼロは後ろ・件数順)。同率は件数順
  const byWorker = [...map.values()].sort((a, b) => {
    if ((b.per1000 ?? -1) !== (a.per1000 ?? -1)) return (b.per1000 ?? -1) - (a.per1000 ?? -1);
    return b.total - a.total;
  });
  return { since, until: to, minLines: STATS_MIN_LINES, byWorker, total };
}
