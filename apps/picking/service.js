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
export function importBatch(preview, { hikiateClass, folderName, overwrite }, actor) {
  const cls = String(hikiateClass ?? '').trim();
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
        UPDATE pk_batches SET hikiate_class=?, folder_name=?, work_date=?, instruct_date=?,
          composition=?, delivery_method=?, invoice_soft=?,
          line_count=?, slip_count=?, total_qty=?, csv_sha256=?, imported_by=?, updated_at=?
        WHERE id=?
      `).run(cls, folder, jstToday(), preview.instructDate,
        preview.composition, preview.deliveryMethod, preview.invoiceSoft,
        preview.lines.length, preview.slipCount, preview.totalQty, preview.csvSha256,
        actor, now, existing.id);
      batchId = existing.id;
      replaced = true;
    } else {
      const info = db.prepare(`
        INSERT INTO pk_batches
          (tb_no, hikiate_class, folder_name, work_date, instruct_date, composition,
           delivery_method, invoice_soft, line_count, slip_count, total_qty,
           status, csv_sha256, imported_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?)
      `).run(preview.tbNo, cls, folder, jstToday(),
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
          db.prepare(`UPDATE pk_lines SET status='shortage', done_at=?, shortage_qty=?,
              alt_block=?, alt_location=?, alt_qty=?, remaining_qty=?, remaining=?
            WHERE batch_id=? AND seq=?`)
            .run(now, q, altBlk, altLoc, a > 0 ? a : null, remQty, rem, batchId, lineSeq);
          closeShortageSession(db, batch, lineSeq, clampedEventTime(clientAt, now), now);
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
function loadStatsLines(since, until) {
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
export function getPickingStats({ until = jstToday(), days = STATS_WINDOW_DAYS } = {}) {
  const range = statsRange(until, days);
  const rows = loadStatsLines(range.since, range.until);

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
};

/** アラート発報。同種の未確認が生きていれば重ねない (連打・二重依頼の集約)。 */
export function createFloorAlert(kind, requestedBy, customMessage = null) {
  const def = FLOOR_ALERT_KINDS[kind];
  if (!def) throw new PkError(400, 'bad_kind', '不明なアラート種別です');
  const message = def.message || String(customMessage || '').slice(0, 160);
  if (!message) throw new PkError(400, 'no_message', 'メッセージが必要です');
  const db = getDB();
  // 集約チェック+INSERTは同一トランザクション (Codex: 同時押下の重複防止)。
  // 集約キーは (kind, message) — repick_done は依頼ごとにメッセージが違うため別々に出る
  return db.transaction(() => {
    const dup = db.prepare(`
      SELECT id FROM pk_floor_alerts
      WHERE kind = ? AND message = ? AND acked_at IS NULL AND created_at >= datetime('now', '-4 hours')
    `).get(kind, message);
    if (dup) return { id: dup.id, existed: true };
    const info = db.prepare(`
      INSERT INTO pk_floor_alerts (direction, kind, message, requested_by, created_at)
      VALUES (?, ?, ?, ?, ?)
    `).run(def.direction, kind, message, requestedBy || null, utcNow());
    return { id: Number(info.lastInsertRowid), existed: false };
  }).immediate();
}

/** 表示対象 (未確認・4時間以内 — 古いバナーを翌日まで残さない)。 */
export function listFloorAlerts(direction) {
  return getDB().prepare(`
    SELECT id, kind, message, requested_by, created_at FROM pk_floor_alerts
    WHERE direction = ? AND acked_at IS NULL AND created_at >= datetime('now', '-4 hours')
    ORDER BY id
  `).all(direction);
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
export function getMissStats({ until = jstToday(), days = STATS_WINDOW_DAYS } = {}) {
  const db = getDB();
  const since = new Date(Date.parse(`${until}T00:00:00Z`) - (days - 1) * 864e5).toISOString().slice(0, 10);
  // created_at は UTC → JST日付境界に変換して窓を切る (統計画面と同じ期間感覚)
  const startUtc = new Date(`${since}T00:00:00+09:00`).toISOString().slice(0, 19) + 'Z';
  const endUtc = new Date(new Date(`${until}T00:00:00+09:00`).getTime() + 864e5).toISOString().slice(0, 19) + 'Z';
  const empty = { total: 0, shortage: 0, excess: 0, wrong_item: 0, qty: 0 };
  try {
    const rows = db.prepare(`
      SELECT COALESCE(attributed_worker, '(担当不明)') AS worker, kind, COUNT(*) AS cnt, SUM(qty) AS qty
      FROM pk_pack_incidents
      WHERE status = 'confirmed' AND created_at >= ? AND created_at < ?
      GROUP BY worker, kind
    `).all(startUtc, endUtc);
    const map = new Map();
    const total = { ...empty };
    for (const r of rows) {
      if (!map.has(r.worker)) map.set(r.worker, { worker: r.worker, ...empty });
      const w = map.get(r.worker);
      w[r.kind] = (w[r.kind] || 0) + r.cnt;
      w.total += r.cnt;
      w.qty += r.qty || 0;
      total[r.kind] = (total[r.kind] || 0) + r.cnt;
      total.total += r.cnt;
      total.qty += r.qty || 0;
    }
    return { since, until, byWorker: [...map.values()].sort((a, b) => b.total - a.total), total };
  } catch (e) {
    // テーブル未作成 (packing未初期化環境) のみ空扱い。それ以外のDB障害は隠さず投げる
    // (Codex: 包括catchだと障害時に「ミス0件」という誤った正常値を表示してしまう)
    if (/no such table: pk_pack_incidents/.test(String(e.message))) {
      return { since, until, byWorker: [], total: empty };
    }
    throw e;
  }
}
