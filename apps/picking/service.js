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
  '出荷指示日', 'ブロック略称', 'ロケーション', '商品ID', '商品名', '出荷指示数',
  'ピッキングNO', '出荷伝票NO', '荷主出荷NO', 'バーコード',
  '送り状発行ソフト名', '配送方法名', 'トータルピッキングバッチ番号',
];

/**
 * CS03002 のバッファを解析して取込プレビュー用の構造を返す。DB には触れない。
 * @returns {{
 *   tbNo, instructDate, invoiceSoft, deliveryMethod, composition,
 *   lines: [{location, block, sku, productName, barcode, qty}],   // 集約済み・ロケ昇順
 *   slipLines: [{slipNo, pickingNo, neSlipNo, sku, qty, location}],
 *   slipCount, totalQty, suggestions: string[]
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
    const qty = Number(get('出荷指示数'));
    if (!Number.isInteger(qty) || qty <= 0) {
      throw new PkError(400, 'bad_qty', `行${rowNo}: 出荷指示数「${get('出荷指示数')}」が正の整数ではありません`);
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

  // 1ファイル = 1引当 = 1TB が前提。複数TBが混ざるケース (まとめ引当の実データ) は
  // 未採取のため fail-closed (要件§10)。採取できたら分割取込に対応する
  const tbNos = [...new Set(slipLines.map((l) => l.tbNo))];
  if (tbNos.length > 1) {
    throw new PkError(400, 'multiple_tb_no',
      `複数のトータルピッキングバッチ番号が含まれています (${tbNos.join(', ')})。1引当=1ファイルで出力してください`);
  }

  // ロケーション×SKU で集約 (トータルピッキング)。表示順 = ロケーション昇順 → SKU昇順。
  // 紙のトータルピッキングリストPDFと同順である前提 (PR1完了時に実物と突合する — 実装計画§10)
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
  const lines = [...aggMap.values()].sort((a, b) =>
    a.location < b.location ? -1 : a.location > b.location ? 1 :
    a.sku < b.sku ? -1 : a.sku > b.sku ? 1 : 0);

  const composition = classifyComposition(slipLines);
  // 共通項目は先頭行決め打ちにせず distinct を取る (Codex R1 medium)。
  // LINEギフトの引当は複数の配送方法・送り状ソフトが正当に混在するため、混在=エラーにはせず
  // 全値を保持して表示・推定の両方に使う
  const distinct = (key) => [...new Set(slipLines.map((l) => l[key]).filter(Boolean))];
  const invoiceSofts = distinct('invoiceSoft');
  const deliveryMethods = distinct('deliveryMethod');
  const instructDates = distinct('instructDate');
  const preview = {
    tbNo: tbNos[0],
    instructDate: instructDates.map(formatDate8).join(' / '),
    invoiceSoft: invoiceSofts.join(' / '),
    deliveryMethod: deliveryMethods.join(' / '),
    composition,
    lines,
    slipLines,
    slipCount: new Set(slipLines.map((l) => l.slipNo)).size,
    totalQty: slipLines.reduce((s, l) => s + l.qty, 0),
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

const WORK_EVENTS = ['start', 'next', 'back'];

/** 作業画面の現在状態。currentSeq = 次にピックする明細 (null = 全て完了)。 */
export function getWorkState(batchId) {
  const batch = getBatch(batchId);
  if (!batch) throw new PkError(404, 'not_found', 'バッチが見つかりません');
  const lines = listLines(batchId).map((l) => ({
    ...l, locationLabel: formatLocation(l.block, l.location),
  }));
  const pending = lines.filter((l) => l.status === 'pending');
  // 明細ごとの「最後に完了させた next の op_id」。back の取り消し対象指定 (CAS) に使う
  const lastNextOps = {};
  for (const e of getDB().prepare(
    "SELECT line_seq, op_id FROM pk_events WHERE batch_id=? AND event='next' ORDER BY id"
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

function eventResult(batchId) {
  const s = getWorkState(batchId);
  return {
    batchStatus: s.batch.status,
    currentSeq: s.currentSeq,
    doneCount: s.doneCount,
    lineCount: s.lines.length,
    startedAt: s.batch.started_at,
    finishedAt: s.batch.finished_at,
  };
}

/**
 * 作業イベントの適用。1イベント = 1トランザクション。
 * @returns {{replayed?: boolean, ...eventResult}}
 */
export function applyEvent(batchId, { opId, event, lineSeq, clientAt, undoOpId }, worker) {
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
          && (prevPayload.undoOpId ?? null) === (undoOpId ?? null)) {
        return { replayed: true, ...JSON.parse(prev.result_json) };
      }
      throw new PkError(409, 'op_conflict', '同じ操作IDが別の内容で使われています');
    }

    const batch = getBatch(batchId);
    if (!batch) throw new PkError(404, 'not_found', 'バッチが見つかりません');
    if (batch.validity !== 'valid' || batch.status === 'cancelled') {
      throw new PkError(409, 'batch_invalid', 'このバッチは取消されています');
    }

    if (event === 'start') {
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

      if (event === 'next') {
        // 表示中の明細 (= 最小の pending) 以外への next は、キュー順序が壊れている兆候なので拒否
        const cur = db.prepare(
          "SELECT MIN(seq) s FROM pk_lines WHERE batch_id=? AND status='pending'"
        ).get(batchId).s;
        if (cur !== lineSeq) {
          throw new PkError(409, 'out_of_order', `明細 ${lineSeq} は現在の対象 (${cur ?? 'なし'}) ではありません`);
        }
        db.prepare("UPDATE pk_lines SET status='done', done_at=? WHERE batch_id=? AND seq=?")
          .run(now, batchId, lineSeq);
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
        }
      } else if (event === 'back') {
        // 直前に完了した明細を取り消して戻る
        if (line.status !== 'done') {
          throw new PkError(409, 'not_done', `明細 ${lineSeq} は完了していないため戻れません`);
        }
        // 戻れるのは「完了済みの中で最大の seq」だけ (途中の明細に飛び戻ると順序が壊れる)
        const lastDone = db.prepare(
          "SELECT MAX(seq) s FROM pk_lines WHERE batch_id=? AND status='done'"
        ).get(batchId).s;
        if (lastDone !== lineSeq) {
          throw new PkError(409, 'out_of_order', `戻れるのは直前の明細 (${lastDone}) だけです`);
        }
        // CAS: 「どの完了を取り消すか」を undo_op_id で特定する (Codex PR2-R2 high)。
        // これが無いと、同一作業者の別端末に残っていた古い back が、再完了後に到着して
        // 新しい完了まで取り消してしまう
        const lastNextOp = db.prepare(
          "SELECT op_id FROM pk_events WHERE batch_id=? AND event='next' AND line_seq=? ORDER BY id DESC LIMIT 1"
        ).get(batchId, lineSeq)?.op_id ?? null;
        if (!undoOpId || undoOpId !== lastNextOp) {
          throw new PkError(409, 'stale_back',
            '取り消し対象の完了が見つからないか、別の操作で上書きされています');
        }
        // shown_at は now に更新 (この瞬間から再表示。再作業時間に前の明細の時間を混ぜない)
        db.prepare("UPDATE pk_lines SET status='pending', done_at=NULL, shown_at=? WHERE batch_id=? AND seq=?")
          .run(now, batchId, lineSeq);
        if (backFromDone) {
          // 完了直後の取り消し: バッチを作業中に戻す
          db.prepare("UPDATE pk_batches SET status='picking', finished_at=NULL, updated_at=? WHERE id=?")
            .run(now, batchId);
        }
      }
    }

    const result = eventResult(batchId);
    db.prepare(`
      INSERT INTO pk_events (op_id, batch_id, worker, event, line_seq, payload_json, result_json, at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(opId, batchId, worker, event, lineSeq ?? null,
      (clientAt || undoOpId) ? JSON.stringify({ clientAt, undoOpId }) : null,
      JSON.stringify(result), now);
    return result;
  })();
}
