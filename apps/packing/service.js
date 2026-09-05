/**
 * packing 業務ロジック — CS03003 (納品書データCSV) の解析・CS03002突合・取込。
 *
 * 入力 = 納品書_出荷_XX.csv:
 *   別プログラム (伝票出しPC) がロジザードの納品書データCSV (CS03003) を
 *   Drive の各「出荷_XX」フォルダへ毎朝配置する (稼働確認 2026-08-16)。
 *   - Shift-JIS / 全列ダブルクォート / 実測237列 / 1行 = 出荷伝票 × 明細行
 *   - 行順 = 納品書PDFの印字順 (実データ17伝票で完全一致確認 2026-08-15)。並び替えない
 * 列仕様の正本: AI_reference/システム設計/梱包支援システム_要件定義_20260815.md §4
 *
 * 列は名前で引く (ロジザードの列追加・順序変更に耐える)。必須列が無ければ fail-closed。
 * 個人情報は作業画面=納品書PDF同等表示に必要な範囲 (配送先の名前・〒・住所) だけ保存する
 * (中原さん指示 2026-08-16)。電話番号・購入者情報・メールは読まない=保存しない。
 */
import crypto from 'node:crypto';
import { parseCsv, decodeCp932 } from '../packing-dispatch/csv.js';
// 欠品フローv2 PR2: 再取込 (overwrite) 前に、ピッカーの「後で取りに行く」の展開を依頼へ戻す
import { resetLaterBindingsForPackBatch, resolveFloorAlertsByTask, resolveFloorAlertsByRef } from '../picking/service.js';
import {
  getDB, getPackBatchByTbKey, utcNow, jstToday,
} from './db.js';
// 資材の完了スナップショット (循環 import だが実行時参照のみ = ESM で安全)
import {
  onSlipCompleted as materialOnSlipCompleted,
  onSlipCompletionCleared as materialOnSlipCompletionCleared,
} from './materials.js';

/** 業務エラー。router が status + message に変換する。 */
export class PackError extends Error {
  constructor(status, code, message) {
    super(message);
    this.status = status;
    this.code = code;
  }
}

// CS03003 の必須列 (名前で参照する列のみ列挙。他の200列は読まない=保持しない)
// 送り先 (名前・〒・住所) は作業画面=納品書PDF同等表示のため保存する (中原さん指示 2026-08-16)。
// 電話番号・購入者情報・決済方法・金額系は表示不要のため読まない=保存しない
const REQUIRED_COLUMNS = [
  '荷主出荷NO', '出荷伝票NO', 'ピッキングNO', 'トータルピッキングバッチ番号',
  '出荷予定行NO', 'マテハン用BC', '出荷作業日', '取引先名',
  '配送方法ID', '配送方法名', '引当抽出グループ1',
  '配送先名', '配送先郵便番号', '配送先都道府県', '配送先住所1', '配送先住所2', '配送先住所3',
  'サイト受注№', '注文日', '納品書印字ヘッダ1',
  '商品ID', '商品名', '出荷数', 'バーコード', '印字商品名', '送り状備考1',
  'ギフトフラグ', 'ギフトメッセージ', 'のし',
  '納品書ヘッダコメント', '納品書フッタコメント', '倉庫連絡事項', '顧客コメント',
  '配達指定日', '配達時間帯', '箱数', '有効期限', 'ロット',
];

/** '20260816' → '2026-08-16'。形式外はそのまま返す (表示用なので落とさない)。 */
function formatDate8(s) {
  return /^\d{8}$/.test(s) ? `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}` : s;
}

/** SKU比較用の正規化 (feedback: SKUはLOWER(TRIM())で突合する)。保存は原文のまま。 */
const normSku = (s) => String(s ?? '').trim().toLowerCase();

/**
 * 伝票単位の警告バッジ (要件§5.2)。作業画面 (PR2) とバッチ詳細で表示する。
 * @returns {string[]} 'gift' | 'noshi' | 'comment' | 'multi_box' | 'multi_qty' | 'assort' | 'expiry_lot'
 */
export function slipWarns(slip) {
  const warns = [];
  if ((slip.giftFlag && slip.giftFlag !== '0') || slip.giftMessage) warns.push('gift');
  if (slip.noshi) warns.push('noshi');
  // フッタコメントはバッジ判定から除外: 実データではサイト受注番号が毎伝票入る定型欄で、
  // 含めると全伝票に「コメントあり」が付きノイズになる (2026-08-16 実CSVで確認)。表示用には保持する
  if (['header', 'warehouse', 'customer'].some((k) => slip.comments[k])) warns.push('comment');
  if (slip.boxCount > 1) warns.push('multi_box');
  if (new Set(slip.lines.map((l) => normSku(l.sku))).size >= 2) warns.push('assort');
  else if (slip.lines.some((l) => l.qty >= 2)) warns.push('multi_qty');
  if (slip.lines.some((l) => l.expiry || l.lot)) warns.push('expiry_lot');
  return warns;
}

export const WARN_LABELS = {
  gift: '🎁 ギフト', noshi: '🎀 のし', comment: '📝 コメント',
  multi_box: '📦 複数箱', multi_qty: '✕2 複数個', assort: '🧩 アソート',
  expiry_lot: '📅 期限/ロット',
};

/**
 * CS03003 のバッファを解析して取込プレビュー用の構造を返す。DB には触れない。
 * 伝票の並び = CSVの出現順 (= 納品書PDFの束の順)。並び替えは一切しない。
 * @returns {{
 *   tbKey, tbCount, sagyoDate,
 *   slips: [{seq, neSlipNo, slipNo, pickingNo, matehanBc, mall,
 *            deliveryMethodId, deliveryMethod, material, boxCount,
 *            giftFlag, giftMessage, noshi, comments, deliveryDate, deliveryTime,
 *            warns, lines: [{lineNo, sku, productName, qty, barcode, printName, shortName, expiry, lot}]}],
 *   slipCount, lineCount, totalQty, csvSha256
 * }}
 * @throws {PackError} 400 — 形式不正 (列欠落・数量不正・伝票行の非連続等)
 */
export function parseCs03003(buffer) {
  if (!buffer || buffer.length === 0) throw new PackError(400, 'empty_file', 'ファイルが空です');
  const state = {};
  const rows = parseCsv(decodeCp932(buffer), state);
  if (state.unclosedQuote) {
    throw new PackError(400, 'broken_csv', 'CSVの引用符が閉じていません (ファイルが壊れている可能性があります)');
  }
  if (rows.length === 0) throw new PackError(400, 'no_rows', 'データ行がありません');

  const header = rows[0];
  const col = {};
  const dupCols = new Set();
  header.forEach((name, i) => {
    if (name in col) dupCols.add(name);
    else col[name] = i;
  });
  const missing = REQUIRED_COLUMNS.filter((name) => !(name in col));
  if (missing.length > 0) {
    throw new PackError(400, 'missing_columns',
      `必須列がありません: ${missing.join(', ')}。CS03003 (納品書データCSV) か確認してください`);
  }
  const dupRequired = REQUIRED_COLUMNS.filter((name) => dupCols.has(name));
  if (dupRequired.length > 0) {
    throw new PackError(400, 'duplicate_columns',
      `同名の列が複数あります: ${dupRequired.join(', ')}。CSVの形式が変わった可能性があります`);
  }

  const dataRows = rows.slice(1).filter((r) => !(r.length === 1 && r[0] === ''));
  if (dataRows.length === 0) throw new PackError(400, 'no_rows', 'データ行がありません');
  const badRows = [];
  dataRows.forEach((r, i) => { if (r.length !== header.length) badRows.push(i + 2); });
  if (badRows.length > 0) {
    throw new PackError(400, 'ragged_rows',
      `列数が揃わない行があります (行 ${badRows.slice(0, 5).join(', ')}${badRows.length > 5 ? ' …' : ''})`);
  }

  // 伝票ごとに行をまとめる。出現順を seq にする (= 納品書の束の順)。
  // 同一伝票の行が離れて出てくるCSVは「行順=納品書順」の前提が壊れているので fail-closed
  // (要件§4: 取込時の簡易検証は継続する)
  const slipMap = new Map();   // slip_no → slip
  const slipOrder = [];
  let lastSlipNo = null;
  const closedSlips = new Set();
  // 伝票単位フィールド (=受注ヘッダ由来の列) は全行で一致していることを検証する (Codex R1/R2 high:
  // 先頭行だけを信じると、①別TB・別伝票番号が混ざった壊れCSVを先頭行の値で突合・取込してしまう
  // ②2行目だけに「のし」やコメントが入ったCSVで警告が欠落し、梱包者が指示を見落とす)
  const SLIP_CONSTANT_FIELDS = [
    '荷主出荷NO', 'ピッキングNO', 'トータルピッキングバッチ番号',
    '出荷作業日', '配送方法ID', '配送方法名', '箱数',
    'マテハン用BC', '取引先名', '引当抽出グループ1',
    '配送先名', '配送先郵便番号', '配送先都道府県', '配送先住所1', '配送先住所2', '配送先住所3',
    'サイト受注№', '注文日', '納品書印字ヘッダ1',
    'ギフトフラグ', 'ギフトメッセージ', 'のし',
    '納品書ヘッダコメント', '納品書フッタコメント', '倉庫連絡事項', '顧客コメント',
    '配達指定日', '配達時間帯',
  ];
  for (const [i, r] of dataRows.entries()) {
    const rowNo = i + 2;
    const get = (name) => (r[col[name]] ?? '').trim();
    const slipNo = get('出荷伝票NO');
    if (!slipNo) throw new PackError(400, 'no_slip', `行${rowNo}: 出荷伝票NOが空です`);
    if (!get('荷主出荷NO')) throw new PackError(400, 'no_ne_slip', `行${rowNo}: 荷主出荷NOが空です`);
    if (!get('商品ID')) throw new PackError(400, 'no_sku', `行${rowNo}: 商品IDが空です`);
    if (!get('トータルピッキングバッチ番号')) {
      throw new PackError(400, 'no_tb_no', `行${rowNo}: トータルピッキングバッチ番号が空です`);
    }
    const qty = Number(get('出荷数'));
    if (!Number.isInteger(qty) || qty <= 0) {
      throw new PackError(400, 'bad_qty', `行${rowNo}: 出荷数「${get('出荷数')}」が正の整数ではありません`);
    }
    if (slipNo !== lastSlipNo) {
      if (closedSlips.has(slipNo)) {
        throw new PackError(400, 'noncontiguous_slip',
          `行${rowNo}: 伝票 ${slipNo} の明細が離れた位置に分かれています (行順=納品書順の前提が崩れているため取込を中止しました)`);
      }
      if (lastSlipNo != null) closedSlips.add(lastSlipNo);
      lastSlipNo = slipNo;
    }

    let slip = slipMap.get(slipNo);
    if (slip) {
      for (const f of SLIP_CONSTANT_FIELDS) {
        if (get(f) !== slip._raw[f]) {
          throw new PackError(400, 'inconsistent_slip',
            `行${rowNo}: 伝票 ${slipNo} の「${f}」が行によって異なります (${slip._raw[f]} / ${get(f)})。CSVが壊れている可能性があります`);
        }
      }
    }
    if (!slip) {
      const boxRaw = get('箱数');
      const boxCount = boxRaw === '' ? 1 : Number(boxRaw);
      if (!Number.isInteger(boxCount) || boxCount < 1) {
        throw new PackError(400, 'bad_box_count', `行${rowNo}: 箱数「${boxRaw}」が不正です`);
      }
      const comments = {};
      if (get('納品書ヘッダコメント')) comments.header = get('納品書ヘッダコメント');
      if (get('納品書フッタコメント')) comments.footer = get('納品書フッタコメント');
      if (get('倉庫連絡事項')) comments.warehouse = get('倉庫連絡事項');
      if (get('顧客コメント')) comments.customer = get('顧客コメント');
      slip = {
        seq: slipOrder.length + 1,
        neSlipNo: get('荷主出荷NO'),
        slipNo,
        pickingNo: get('ピッキングNO'),
        matehanBc: get('マテハン用BC'),
        mall: get('取引先名'),
        deliveryMethodId: get('配送方法ID'),
        deliveryMethod: get('配送方法名'),
        material: get('引当抽出グループ1'),
        recipientName: get('配送先名'),
        recipientZip: get('配送先郵便番号'),
        recipientPref: get('配送先都道府県'),
        recipientAddr: [get('配送先住所1'), get('配送先住所2'), get('配送先住所3')].filter(Boolean).join(' '),
        siteOrderNo: get('サイト受注№'),
        orderDate: formatDate8(get('注文日')),
        printHeader1: get('納品書印字ヘッダ1'),
        boxCount,
        giftFlag: get('ギフトフラグ'),
        giftMessage: get('ギフトメッセージ'),
        noshi: get('のし'),
        comments,
        deliveryDate: formatDate8(get('配達指定日')),
        deliveryTime: get('配達時間帯') === '指定なし' ? '' : get('配達時間帯'),
        tbNo: get('トータルピッキングバッチ番号'),
        sagyoDate: formatDate8(get('出荷作業日')),
        _raw: Object.fromEntries(SLIP_CONSTANT_FIELDS.map((f) => [f, get(f)])),
        lines: [],
      };
      slipMap.set(slipNo, slip);
      slipOrder.push(slip);
    }
    const lineNoRaw = get('出荷予定行NO');
    slip.lines.push({
      lineNo: /^\d+$/.test(lineNoRaw) ? Number(lineNoRaw) : null,
      sku: get('商品ID'),
      productName: get('商品名'),
      qty,
      barcode: get('バーコード'),
      printName: get('印字商品名'),
      shortName: get('送り状備考1'),
      expiry: get('有効期限'),
      lot: get('ロット'),
    });
  }

  for (const slip of slipOrder) { slip.warns = slipWarns(slip); delete slip._raw; }

  // TB番号は1引当でも複数振られる (picking実測 2026-08-12)。
  // picking と同じ正規化 = ソート済みTB一覧のカンマ結合をバッチ識別キーにする
  // (伝票内の全行一致は上で検証済みなので、伝票単位の集計で全行を代表できる)
  const tbNos = [...new Set(slipOrder.map((s) => s.tbNo))].sort();
  const sagyoDates = [...new Set(slipOrder.map((s) => s.sagyoDate).filter(Boolean))];
  // 作業日の混在 = 別の日のファイルが連結された等、生成が壊れている兆候なので fail-closed
  // (Codex R1 medium。毎朝Drive配置の運用では前日ファイルとの混同が現実的に起きる)
  if (sagyoDates.length > 1) {
    throw new PackError(400, 'mixed_sagyo_date',
      `出荷作業日が混在しています (${sagyoDates.join(' / ')})。ファイルの生成が壊れている可能性があります`);
  }

  return {
    tbKey: tbNos.join(','),
    tbCount: tbNos.length,
    sagyoDate: sagyoDates.join(' / '),
    slips: slipOrder,
    slipCount: slipOrder.length,
    lineCount: slipOrder.reduce((s, x) => s + x.lines.length, 0),
    totalQty: slipOrder.reduce((s, x) => s + x.lines.reduce((a, l) => a + l.qty, 0), 0),
    csvSha256: crypto.createHash('sha256').update(buffer).digest('hex'),
  };
}

/**
 * Driveファイル名から出荷フォルダ名を導出。例: '納品書_出荷_01.csv' → '出荷_01'
 * (picking の deriveFolderName と同規則。該当なしは null = 手入力に任せる)
 */
export function deriveFolderName(filename) {
  const m = String(filename || '').match(/出荷_?(\d{1,2})/);
  return m ? `出荷_${m[1].padStart(2, '0')}` : null;
}

/**
 * 出荷作業日が今日でないか (前日ファイルの取り込み事故ガード)。
 * 出荷Noは毎日1から再利用されるため、Driveに残った前日の同名ファイルを翌朝取り込むと
 * 出荷済み伝票を再梱包してしまう。警告表示に使う (ブロックはしない — picking と同運用)。
 */
export function isStaleSagyoDate(sagyoDate, today = jstToday()) {
  const dates = String(sagyoDate || '').split(' / ').filter(Boolean);
  return dates.length > 0 && dates.every((d) => d !== today);
}

/**
 * CS03002 (ピッキング) との突合 (要件§4: TB一致だけでなく伝票集合・SKU・数量を照合する。
 * 異なる時点のCSVが混ざると、正しく操作しても誤出荷するため)。
 *
 * 比較粒度: 伝票 (出荷伝票NO) × SKU の数量合計。picking 側は pk_slip_lines (集約前の生明細)。
 * @returns {{status: 'ok'|'mismatch'|'no_picking', pkBatchId: number|null, diffs: string[]}}
 */
export function checkPickingMatch(preview) {
  const db = getDB();
  // pk_batches は picking 所有 — 参照JOINのみ (要件§7.1)。存在しない場合 (pickingが
  // まだ初期化されていない開発環境) は no_picking として扱う
  let pk = null;
  try {
    pk = db.prepare(
      "SELECT id FROM pk_batches WHERE tb_no = ? AND validity = 'valid'"
    ).get(preview.tbKey);
  } catch (e) {
    if (!/no such table/.test(e.message)) throw e;
  }
  if (!pk) return { status: 'no_picking', pkBatchId: null, diffs: [] };

  const pickMap = new Map();   // slip_no \0 sku(正規化) → qty
  for (const r of db.prepare(
    'SELECT slip_no, sku, SUM(qty) qty FROM pk_slip_lines WHERE batch_id = ? GROUP BY slip_no, sku'
  ).all(pk.id)) {
    const key = `${r.slip_no}\u0000${normSku(r.sku)}`;
    pickMap.set(key, (pickMap.get(key) || 0) + r.qty);
  }
  const packMap = new Map();
  for (const s of preview.slips) {
    for (const l of s.lines) {
      const key = `${s.slipNo}\u0000${normSku(l.sku)}`;
      packMap.set(key, (packMap.get(key) || 0) + l.qty);
    }
  }

  const diffs = [];
  const fmt = (key) => key.replace('\u0000', ' × ');
  for (const [key, qty] of packMap) {
    const pq = pickMap.get(key);
    if (pq == null) diffs.push(`納品書のみ: ${fmt(key)} (${qty}個) — ピッキング側にない`);
    else if (pq !== qty) diffs.push(`数量不一致: ${fmt(key)} 納品書${qty} / ピッキング${pq}`);
  }
  for (const [key, qty] of pickMap) {
    if (!packMap.has(key)) diffs.push(`ピッキングのみ: ${fmt(key)} (${qty}個) — 納品書側にない`);
  }
  return { status: diffs.length === 0 ? 'ok' : 'mismatch', pkBatchId: pk.id, diffs };
}

/**
 * 取込の確定。tb_key が冪等キー (picking の importBatch と同じ意味論)。
 *   - 既存なし → 新規作成
 *   - 既存あり (同一CSV・同一フォルダ) → 再送成功 (replayed)
 *   - 既存あり (ready・overwrite=true) → 伝票・明細を入れ替えて更新 (旧版はここで superseded)
 *   - 既存あり (ready・overwrite なし) → 409 duplicate
 *   - 既存あり (ready 以外) → 409 already_started (作業開始後の差し替え禁止 — 要件§4)
 * 突合が ok でない場合は matchAck=true、出荷作業日が今日でない場合は dateAck=true
 * (それぞれ別の明示承認) がなければ 409 で拒否する。
 * @returns {{batchId, replaced, replayed?, match}}
 */
export function importPackBatch(preview, { folderName, overwrite, matchAck, dateAck }, actor) {
  const folder = String(folderName ?? '').trim() || null;
  if (folder && folder.length > 50) throw new PackError(400, 'folder_too_long', 'フォルダ名が長すぎます (50文字まで)');
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    // 突合はトランザクション内で行う (プレビュー後に picking 側が変わっても確定時点の状態で判定する)
    const match = checkPickingMatch(preview);

    const existing = getPackBatchByTbKey(preview.tbKey);
    let batchId;
    let replaced = false;
    if (existing && existing.csv_sha256 === preview.csvSha256 && (existing.folder_name ?? null) === folder) {
      // 同一内容の再送 = 応答が届かなかった再confirm。承認の再要求はしない。
      // ただし突合結果は最新に更新する (Codex R1 medium: no_picking 承認後に CS03002 が
      // 取り込まれて ok になっても、旧状態のまま表示され続けるのを防ぐ)。
      // 例外: 保存済みと異なる「非ok」への変化 (status/pk_batch_id/差分内容のいずれか —
      // picking 側が後から変わった) は、承認されていない状態を「承認済み」表示にしないため
      // matchAck を要求し、更新は監査ログに残す (Codex R2/R3 medium)
      const diffsJson = match.diffs.length ? JSON.stringify(match.diffs) : null;
      const matchChanged = existing.match_status !== match.status
        || (existing.pk_batch_id ?? null) !== (match.pkBatchId ?? null)
        || (existing.match_json ?? null) !== diffsJson;
      if (matchChanged) {
        if (match.status !== 'ok' && !matchAck) {
          throw new PackError(409, 'match_' + match.status,
            `ピッキング側の状態が変わり、突合結果が「${existing.match_status}」から変化しています。差分を確認のうえ、承認する場合はチェックを付けて再実行してください`);
        }
        db.prepare(`UPDATE pk_pack_batches SET match_status=?, match_json=?, pk_batch_id=?, updated_at=? WHERE id=?`)
          .run(match.status, diffsJson, match.pkBatchId, now, existing.id);
        db.prepare(`
          INSERT INTO pk_pack_import_logs
            (batch_id, tb_key, action, csv_sha256, folder_name, slip_count, line_count, total_qty,
             match_status, match_acked, before_json, actor, at)
          VALUES (?, ?, 'match_update', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `).run(existing.id, existing.tb_key, existing.csv_sha256, existing.folder_name,
          existing.slip_count, existing.line_count, existing.total_qty,
          match.status, match.status !== 'ok' ? 1 : 0,
          JSON.stringify({ match_status: existing.match_status, match_json: existing.match_json, pk_batch_id: existing.pk_batch_id }),
          actor, now);
      }
      return { batchId: existing.id, replaced: false, replayed: true, match };
    }

    if (match.status !== 'ok' && !matchAck) {
      const head = match.status === 'no_picking'
        ? `ピッキング側にバッチ ${preview.tbKey} が見つかりません (CS03002=ピッキングリストCSVが未取込か、別の時点のCSVです)`
        : `ピッキングと内容が一致しません (${match.diffs.length}件の差分)`;
      throw new PackError(409, 'match_' + match.status,
        `${head}。内容を確認のうえ、承認して取り込む場合はチェックを付けて再実行してください`);
    }
    // 前日ファイルの誤取込ガード (Codex R1 medium: 出荷Noは毎日再利用されるため、警告だけでは
    // 前日CSVをそのまま確定できてしまう)。今日を含まない作業日は明示承認を要求する
    if (isStaleSagyoDate(preview.sagyoDate) && !dateAck) {
      throw new PackError(409, 'stale_sagyo_date',
        `出荷作業日 (${preview.sagyoDate}) が今日ではありません。前日のファイルの可能性があります。承認して取り込む場合はチェックを付けて再実行してください`);
    }

    if (existing) {
      if (existing.status !== 'ready') {
        throw new PackError(409, 'already_started',
          `バッチ ${preview.tbKey} は既に梱包が始まっています (${existing.status})。取り込み直しはできません`);
      }
      if (!overwrite) {
        throw new PackError(409, 'duplicate',
          `バッチ ${preview.tbKey} は取込済みです (${existing.slip_count}伝票)`);
      }
      // 欠品フローv2 PR2: ピッカーの「後で取りに行く」から展開済みのタスクは slip_seq で
      // 伝票を指しており、再取込で別のお客さまを指してしまう → 依頼へ戻して展開し直させる。
      // ピッカーが既に取りに動いている分があれば再取込を止める (Codex High)。
      // ⭐overwrite は status='ready' のバッチにしか許されないので、梱包者自身の
      //   再ピックタスク (作業開始後にしか生まれない) はここに存在し得ない
      try {
        resetLaterBindingsForPackBatch(db, existing.id);
      } catch (e) {
        if (e && e.code === 'later_in_progress') throw new PackError(409, 'later_in_progress', e.message);
        throw e;
      }
      // 旧版の伝票・明細を監査ログ用に丸ごとスナップショットしてから消す (Codex R1 medium:
      // 集計値だけでは、Drive側のCSVも差し替わった後に「上書き前は何が対象だったか」を復元できない)
      existing._snapshotSlips = db.prepare(
        'SELECT * FROM pk_pack_slips WHERE batch_id = ? ORDER BY seq'
      ).all(existing.id).map((s) => ({
        ...s,
        lines: db.prepare('SELECT * FROM pk_pack_lines WHERE slip_id = ? ORDER BY id').all(s.id),
      }));
      db.prepare(`DELETE FROM pk_pack_lines WHERE slip_id IN
        (SELECT id FROM pk_pack_slips WHERE batch_id = ?)`).run(existing.id);
      db.prepare('DELETE FROM pk_pack_slips WHERE batch_id = ?').run(existing.id);
      db.prepare(`
        UPDATE pk_pack_batches SET folder_name=?, work_date=?, sagyo_date=?,
          slip_count=?, line_count=?, total_qty=?, pk_batch_id=?, match_status=?, match_json=?,
          csv_sha256=?, imported_by=?, updated_at=?
        WHERE id=?
      `).run(folder, jstToday(), preview.sagyoDate,
        preview.slipCount, preview.lineCount, preview.totalQty,
        match.pkBatchId, match.status, match.diffs.length ? JSON.stringify(match.diffs) : null,
        preview.csvSha256, actor, now, existing.id);
      batchId = existing.id;
      replaced = true;
    } else {
      const info = db.prepare(`
        INSERT INTO pk_pack_batches
          (tb_key, folder_name, work_date, sagyo_date, slip_count, line_count, total_qty,
           pk_batch_id, match_status, match_json, status, csv_sha256, imported_by, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'ready', ?, ?, ?, ?)
      `).run(preview.tbKey, folder, jstToday(), preview.sagyoDate,
        preview.slipCount, preview.lineCount, preview.totalQty,
        match.pkBatchId, match.status, match.diffs.length ? JSON.stringify(match.diffs) : null,
        preview.csvSha256, actor, now, now);
      batchId = Number(info.lastInsertRowid);
    }

    db.prepare(`
      INSERT INTO pk_pack_import_logs
        (batch_id, tb_key, action, csv_sha256, folder_name, slip_count, line_count, total_qty,
         match_status, match_acked, before_json, actor, at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(batchId, preview.tbKey, replaced ? 'overwrite' : 'create', preview.csvSha256, folder,
      preview.slipCount, preview.lineCount, preview.totalQty,
      match.status, match.status !== 'ok' ? 1 : 0,
      replaced ? JSON.stringify({
        // 旧版のバッチ全列 + 全伝票+明細 (Codex R2 medium: 集計値だけでは復元できない)
        batch: Object.fromEntries(Object.entries(existing).filter(([k]) => k !== '_snapshotSlips')),
        slips: existing._snapshotSlips,
      }) : null, actor, now);

    const insSlip = db.prepare(`
      INSERT INTO pk_pack_slips
        (batch_id, seq, ne_slip_no, slip_no, picking_no, matehan_bc, mall,
         delivery_method_id, delivery_method, material, box_count, warn_json,
         gift_message, noshi, comments_json, delivery_date, delivery_time,
         recipient_name, recipient_zip, recipient_pref, recipient_addr,
         site_order_no, order_date, print_header1)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    const insLine = db.prepare(`
      INSERT INTO pk_pack_lines
        (slip_id, line_no, sku, product_name, qty, barcode, print_name, short_name, expiry, lot)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `);
    for (const s of preview.slips) {
      const slipId = Number(insSlip.run(
        batchId, s.seq, s.neSlipNo, s.slipNo, s.pickingNo || null, s.matehanBc || null,
        s.mall || null, s.deliveryMethodId || null, s.deliveryMethod || null,
        s.material || null, s.boxCount,
        s.warns.length ? JSON.stringify(s.warns) : null,
        s.giftMessage || null, s.noshi || null,
        Object.keys(s.comments).length ? JSON.stringify(s.comments) : null,
        s.deliveryDate || null, s.deliveryTime || null,
        s.recipientName || null, s.recipientZip || null, s.recipientPref || null,
        s.recipientAddr || null, s.siteOrderNo || null, s.orderDate || null,
        s.printHeader1 || null,
      ).lastInsertRowid);
      for (const l of s.lines) {
        insLine.run(slipId, l.lineNo, l.sku, l.productName || null, l.qty,
          l.barcode || null, l.printName || null, l.shortName || null,
          l.expiry || null, l.lot || null);
      }
    }
    return { batchId, replaced, match };
  })();
}

// ═══ 作業イベント (PR2) ═══════════════════════════════════════════════
//
// iPad作業画面 = 納品書PDF同等の1伝票1画面 (中原さん指示 2026-08-16)。
// タップ/スワイプ順送り (要件§5.1)。「次へ」1タップ = 現在伝票の梱包完了宣言 + 次伝票の表示。
//   - 計測の分離 (要件§5.11): packing_completed = pk_pack_slips.done_at /
//     slip_opened = 次伝票の shown_at として記録する (イベント行は 'next' 1行)
//   - 「前へ」= 完了済み伝票の閲覧であり完了取消ではない (クライアント側で表示するだけ。
//     完了取消は理由つき別操作として PR3 で追加する)
//   - 冪等: op_id (端末生成 ms+乱数)。同一 op_id の再送は保存済み結果を返す (picking と同思想)

const LINE_EVENTS = ['line_sort_start', 'line_sort_done', 'line_start', 'line_stop', 'line_done'];
// 伝票単位のイベント (梱包機バッチでは不可 — 伝票状態とライン工程の矛盾を作らない。Codex high)。
// undo はライン専用の段階的取消として別実装、takeover/pause/resume/cancel は両方で使える
const SLIP_ONLY_EVENTS = ['start', 'next', 'jump', 'ship_change', 'reprint', 'label_missing',
  'shortage', 'excess', 'wrong_item', 'found', 'receive', 'stockout_ack'];
// 紙で作業する梱包機ラインでも、伝票を選んでの「依頼」は出せる (2026-08-31 現場意見: MELT/PAS は
// 再ピック依頼・サイズ変更依頼がかけられない)。伝票の完了 (next/jump) と余り/品違いは対象外 —
// 伝票の完了状態はライン工程で管理しないため。保留 (repick) は伝票行に付くがライン工程とは独立
export const LINE_SLIP_REQUEST_EVENTS = ['ship_change', 'reprint', 'label_missing', 'shortage', 'found', 'receive', 'stockout_ack'];
const WORK_EVENTS = ['start', 'next', 'takeover', 'pause', 'resume', 'cancel', 'undo', 'jump', 'ship_change',
  'reprint', 'label_missing', 'shortage', 'excess', 'wrong_item', 'found', 'receive', 'stockout_ack', ...LINE_EVENTS];

/**
 * 梱包機ライン種別 (要件v7 — 中原さん指示 2026-08-18)。
 * 引当分類名の部分一致: PAS-LINE (2つ折り/3つ折り) / MELT-LINE。null = 手梱包 (1伝票1画面)。
 */
export function lineKindOf(hikiateClass) {
  const c = String(hikiateClass || '');
  if (c.includes('PAS-LINE')) return 'pas';
  if (c.includes('MELT-LINE')) return 'melt';
  return null;
}

/**
 * バッチの引当分類と、その**出どころ** (picking pk_batches — 参照のみ・未初期化環境では null)。
 * source='suggested' は Drive の引当パターンtxt が取れず CSV から推定した値で、
 * 実際の分類と違うことがある (2026-09-04: 出荷_17 が《2つ折り》→《3つ折り》)。画面で警告する。
 */
export function batchClassInfo(db, batch) {
  if (!batch?.pk_batch_id) return { name: null, source: null };
  try {
    const r = db.prepare('SELECT hikiate_class, class_source FROM pk_batches WHERE id = ?').get(batch.pk_batch_id);
    return { name: r?.hikiate_class ?? null, source: r?.class_source ?? null };
  } catch {
    return { name: null, source: null };
  }
}

/** バッチの引当分類名 (既存の呼び出し向け)。 */
export function batchHikiateClass(db, batch) {
  if (!batch?.pk_batch_id) return null;
  try {
    return db.prepare('SELECT hikiate_class FROM pk_batches WHERE id = ?')
      .get(batch.pk_batch_id)?.hikiate_class ?? null;
  } catch {
    return null;
  }
}

/** ライン工程の行 (sort/run)。 */
export function listLineRuns(batchId) {
  return getDB().prepare('SELECT * FROM pk_pack_line_runs WHERE batch_id = ? ORDER BY phase DESC').all(batchId);
}

/**
 * 本日 (作業日) ×同ライン種別の流し済み累計 — 梱包機のトータルカウンタとの突合用。
 * 日付が変わるとリセット (中原さん指示 2026-08-18)。
 * @returns { total: 出荷累計 (手動含む), machine: 機械通過累計 (=出荷-手動。カウンタと比較する数),
 *            transferredIn: MELT の仕分けで PAS-LINE へ移した件数 (PAS のみ。total/machine に加算済み) }
 */
export function lineDailyTotal(workDate, kind) {
  try {
    // MELT → PAS の振替 (3つ折り等)。PAS の機械カウンタには乗るが PAS バッチの伝票数には無いので、
    // ここで足さないと「前回までの累計」がカウンタと 1 件ずつズレていく (2026-08-31 現場意見)
    let transferredIn = 0;
    if (kind === 'pas') {
      for (const r of getDB().prepare(`
        SELECT r.to_pas_count, pb.hikiate_class
        FROM pk_pack_line_runs r
        JOIN pk_pack_batches b ON b.id = r.batch_id
        LEFT JOIN pk_batches pb ON pb.id = b.pk_batch_id
        WHERE b.work_date = ? AND r.phase = 'sort' AND r.finished_at IS NOT NULL
          AND r.to_pas_count > 0 AND b.validity = 'valid'
      `).all(workDate)) {
        if (lineKindOf(r.hikiate_class) === 'melt') transferredIn += r.to_pas_count;
      }
    }
    const rows = getDB().prepare(`
      SELECT r.final_count, COALESCE(r.manual_count, 0) AS manual_count, pb.hikiate_class
      FROM pk_pack_line_runs r
      JOIN pk_pack_batches b ON b.id = r.batch_id
      LEFT JOIN pk_batches pb ON pb.id = b.pk_batch_id
      WHERE b.work_date = ? AND r.phase = 'run' AND r.final_count IS NOT NULL AND b.validity = 'valid'
    `).all(workDate);
    let total = 0;
    let machine = 0;
    for (const r of rows) {
      if (lineKindOf(r.hikiate_class) !== kind) continue;
      total += r.final_count;
      machine += r.final_count - r.manual_count;
    }
    return { total: total + transferredIn, machine: machine + transferredIn, transferredIn };
  } catch {
    return { total: 0, machine: 0, transferredIn: 0 };   // pk_batches 未初期化環境
  }
}

// 中断理由 (picking と同じ最小セット)
// '配送変更の入力' = ④⑥フォームを開いている間の自動中断 (計測から除外 — 中原さん指示 2026-08-21)。
// 手動の中断メニューには出さない (work.ejs側でフィルタ)
export const PAUSE_REASONS = ['資材の交換', '休憩', '他作業への応援', '配送変更の入力', 'その他'];
// 完了取消の理由 (監査ログ必須 — 要件§5.1)
export const UNDO_REASONS = ['誤タップ', '入れ間違いの確認', 'その他'];
// ④ 配送方法変更の理由 (要件§5.7)
export const SHIP_CHANGE_REASONS = ['入らない', '資材が違う', 'その他'];
// ④ 提案できる配送方法 (固定リスト — 中原さん指定 2026-08-16)
export const SHIP_CHANGE_METHOD_OPTIONS = [
  '定形外', 'ネコポス', 'ゆうパケットパフ', 'レターパック', '宅急便50サイズ', '宅急便60サイズ',
];
// ④ 特殊依頼: ネコポス二枚出し (中原さん指示 2026-09-02) — 配送方法は変えず、ネコポスの
// 送り状をもう1枚 (2個口) 発行してもらう。扱いは通常の変更依頼と同じ (記録+GChat通知のみ・
// 現物は変更待ち棚)。理由は聞かない (実質「入らない」一択なので画面で省く — 未指定は入らない扱い)
export const SHIP_CHANGE_TWO_LABELS = 'ネコポス二枚出し';

/**
 * 端末の発生時刻を [floor, now] にクランプ (中断時間の計測用)。
 * floor = バッチ開始時刻等 (Codexレビュー medium: 端末時計のズレで開始前の時刻を
 * pause_started_at に入れると、実作業時間以上の中断秒が加算されて計測が壊れる)
 */
/** ライン: 進行中 (開始済み・未停止) の工程行。仕分け→流しの順で高々1行。 */
function inProgressLineRun(db, batchId) {
  return db.prepare(`SELECT * FROM pk_pack_line_runs WHERE batch_id=? AND started_at IS NOT NULL AND finished_at IS NULL
    ORDER BY id DESC LIMIT 1`).get(batchId) || null;
}

function clampedEventTime(clientAt, now, floor = null) {
  const t = Date.parse(clientAt || '');
  const nowMs = Date.parse(now);
  const floorMs = floor ? Date.parse(floor) : nowMs - 24 * 3600 * 1000;
  if (!Number.isFinite(t)) return now;
  if (t > nowMs || t < floorMs) return now;
  return new Date(t).toISOString().slice(0, 19) + 'Z';
}

/** 作業画面の現在状態。currentSeq = 次に梱包する伝票 (null = 全て完了)。 */
export function getWorkState(batchId) {
  const db = getDB();
  const batch = db.prepare('SELECT * FROM pk_pack_batches WHERE id = ?').get(batchId);
  if (!batch) throw new PackError(404, 'not_found', 'バッチが見つかりません');
  const slips = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id = ? ORDER BY seq').all(batchId);
  const linesBySlip = new Map();
  for (const l of db.prepare(`
    SELECT l.* FROM pk_pack_lines l JOIN pk_pack_slips s ON s.id = l.slip_id
    WHERE s.batch_id = ? ORDER BY s.seq, l.id
  `).all(batchId)) {
    if (!linesBySlip.has(l.slip_id)) linesBySlip.set(l.slip_id, []);
    linesBySlip.get(l.slip_id).push(l);
  }
  const full = slips.map((s) => ({
    ...s,
    warns: s.warn_json ? JSON.parse(s.warn_json) : [],
    lines: linesBySlip.get(s.id) || [],
  }));
  // ピッキング欠品の配賦 (欠品フローv2 PR2・picking所有テーブルの参照のみ)。
  // 配賦された伝票だけに 🕒(後で取りに行く)/❌(どこにもない) を出す —
  // 同一SKUの全伝票に出すと、欠品1個で10伝票が保留に見えて出荷が遅れる (要件§4.4)
  try {
    const bySlip = new Map();
    // 商品名 (pk_lines) と ピッカー・時刻 (欠品イベント) も持つ — 1階は名前で商品を見ている (例外処理監査 U-23)
    for (const a of db.prepare(`
      SELECT s.seq, a.sku, a.qty, a.kind, a.created_at, l.product_name,
        (SELECT e.worker FROM pk_events e WHERE e.batch_id = a.batch_id AND e.line_seq = a.line_seq AND e.event = 'shortage'
           ORDER BY e.id DESC LIMIT 1) AS picker
      FROM pk_shortage_allocations a
      JOIN pk_batches pb ON pb.id = a.batch_id
      JOIN pk_pack_slips s ON s.batch_id = ? AND s.ne_slip_no = a.ne_slip_no
      LEFT JOIN pk_lines l ON l.batch_id = a.batch_id AND l.seq = a.line_seq
      WHERE pb.tb_no = ?
      ORDER BY a.id
    `).all(batchId, batch.tb_key)) {
      if (!bySlip.has(a.seq)) bySlip.set(a.seq, []);
      bySlip.get(a.seq).push({ sku: a.sku, qty: a.qty, kind: a.kind, name: a.product_name || null, picker: a.picker || null, at: a.created_at });
    }
    for (const s of full) s.pickingShortages = bySlip.get(s.seq) || [];
  } catch { for (const s of full) s.pickingShortages = []; }   // picking無効環境
  const pending = full.filter((s) => s.status === 'pending');
  // ③ミス候補 (梱包完了サマリで確定/取下げする) と、保留伝票の未解決再ピックタスク状態
  let incidents = [];
  let repickBySlip = {};
  let stockoutBySlip = {};   // 3階「在庫なし」(unavailable) の報告がある保留伝票: seq → [{sku, product_name, req_qty, claimed_by, updated_at}]
  let stockoutAckSeqs = [];  // そのうち「在庫なしを確認」できる伝票
  let stockoutNotifyBySlip = {};   // 出荷保留 (在庫なし) で閉じた伝票の事務通知: seq → 'sent' | 'pending'
  try {
    incidents = db.prepare(
      "SELECT * FROM pk_pack_incidents WHERE batch_id=? AND status='candidate' ORDER BY id"
    ).all(batchId);
    // 伝票単位に集約: 同じ伝票に複数の再ピックがあれば「全部 fulfilled」のときだけ受領可 (Codex)
    for (const t of db.prepare(
      "SELECT slip_seq, status FROM pk_pack_tasks WHERE batch_id=? AND kind='repick' AND status IN ('requested','claimed','fulfilled') ORDER BY id"
    ).all(batchId)) {
      if (t.slip_seq == null) continue;
      const cur = repickBySlip[t.slip_seq];
      repickBySlip[t.slip_seq] = (cur == null || cur === 'fulfilled') ? t.status : cur;
    }
    // 3階が「在庫なし」にした依頼 (例外処理監査 PR-1)。以前はどの画面も読まず、伝票が
    // 「⏳再ピック対応待ち」のまま何日も残った。保留中の伝票だけ = 1階が「在庫なしを確認」する対象
    const heldRepick = new Set(full.filter((s) => s.status === 'held' && s.hold_reason === 'repick').map((s) => s.seq));
    for (const t of db.prepare(
      "SELECT id, slip_seq, sku, product_name, req_qty, unavailable_qty, fulfilled_qty, claimed_by, requested_by, updated_at FROM pk_pack_tasks WHERE batch_id=? AND kind='repick' AND status='unavailable' AND slip_seq IS NOT NULL ORDER BY id"
    ).all(batchId)) {
      if (!heldRepick.has(t.slip_seq)) continue;
      (stockoutBySlip[t.slip_seq] ||= []).push({
        id: t.id, sku: t.sku, product_name: t.product_name, req_qty: t.req_qty,
        unavailable_qty: t.unavailable_qty ?? t.req_qty, fulfilled_qty: t.fulfilled_qty || 0,
        claimed_by: t.claimed_by, requested_by: t.requested_by, updated_at: t.updated_at,
      });
    }
    // ❌ ピッキング時の「どこにもない」(配賦) も、未処理 (pending) の伝票なら1階が「在庫なしを確認」する対象 (PR-2)。
    // 保留 (repick) 中の伝票は上の unavailable 経路で扱う。再ピック依頼が生きている伝票は対象にしない
    const liveRepick = new Set(db.prepare(`SELECT DISTINCT slip_seq FROM pk_pack_tasks WHERE batch_id=? AND kind='repick'
      AND status IN ('requested','claimed','fulfilled','unavailable') AND slip_seq IS NOT NULL`).all(batchId).map((r) => r.slip_seq));
    for (const s of full) {
      if (s.status !== 'pending' || liveRepick.has(s.seq)) continue;
      const none = (s.pickingShortages || []).filter((p) => p.kind === 'none');
      if (none.length === 0) continue;
      stockoutBySlip[s.seq] = none.map((p) => ({
        id: null, source: 'picking', sku: p.sku, product_name: p.name, req_qty: p.qty,
        unavailable_qty: p.qty, fulfilled_qty: 0, claimed_by: p.picker, requested_by: p.picker, updated_at: p.at,
      }));
    }
    // 「在庫なしを確認」できる伝票 = 在庫なし報告あり・再ピック中 (requested/claimed) なし・未送信候補なし・
    // ライン完了件数の確定前 (サーバーの stockout_ack と同じ条件。画面は許可される伝票にだけボタンを出す — Codex R1)
    let lineFinalized = false;
    if (lineKindOf(batchHikiateClass(db, batch))) {
      lineFinalized = db.prepare("SELECT final_count FROM pk_pack_line_runs WHERE batch_id=? AND phase='run'").get(batchId)?.final_count != null;
    }
    const candSeqs = new Set(incidents.filter((i) => i.slip_seq != null && ['shortage', 'wrong_item'].includes(i.kind)).map((i) => i.slip_seq));
    stockoutAckSeqs = Object.keys(stockoutBySlip).map(Number)
      .filter((seq) => !lineFinalized && !candSeqs.has(seq) && (repickBySlip[seq] == null || repickBySlip[seq] === 'fulfilled'));
    // 出荷保留 (在庫なし) で閉じた伝票の事務通知の状態 ('sent' | 'pending')。画面に「通知済み」を固定表示しない (Codex R2)
    for (const r of db.prepare(`SELECT slip_seq, notified_at FROM pk_pack_stockouts
      WHERE batch_id=? AND id IN (SELECT MAX(id) FROM pk_pack_stockouts WHERE batch_id=? GROUP BY slip_seq)`).all(batchId, batchId)) {
      stockoutNotifyBySlip[r.slip_seq] = r.notified_at ? 'sent' : 'pending';
    }
  } catch { /* v4未適用環境 */ }
  return {
    batch,
    slips: full,
    currentSeq: pending.length > 0 ? pending[0].seq : null,
    doneCount: full.filter((s) => s.status === 'done').length,
    incidents,
    repickBySlip,
    stockoutBySlip,
    stockoutAckSeqs,
    stockoutNotifyBySlip,
  };
}

/**
 * 「最後に完了した伝票」= 完了順の正はイベント履歴 (done_at は秒精度で連打時にタイになる)。
 * 現在も done のままの伝票のうち、最新の next イベントの対象を返す。
 */
export function lastDoneSeqOf(batchId) {
  return getDB().prepare(`
    SELECT e.slip_seq FROM pk_pack_events e
    JOIN pk_pack_slips s ON s.batch_id = e.batch_id AND s.seq = e.slip_seq
    WHERE e.batch_id = ? AND e.event = 'next' AND s.status = 'done'
    ORDER BY e.id DESC LIMIT 1
  `).get(batchId)?.slip_seq ?? null;
}

function eventResult(batchId) {
  const s = getWorkState(batchId);
  return {
    batchStatus: s.batch.status,
    currentSeq: s.currentSeq,
    doneCount: s.doneCount,
    slipCount: s.slips.length,
    // ズレ回復ジャンプ (順序外の完了) があると「currentSeqより前=完了」が成り立たないため、
    // 完了済みseqの一覧を明示的に返す (画面はこれで doneSet を再構築する)
    doneSeqs: s.slips.filter((x) => x.status === 'done').map((x) => x.seq),
    heldSeqs: s.slips.filter((x) => x.status === 'held').map((x) => x.seq),
    repickSeqs: s.slips.filter((x) => x.status === 'held' && x.hold_reason === 'repick').map((x) => x.seq),
    repickReadySeqs: Object.entries(s.repickBySlip).filter(([, st]) => st === 'fulfilled').map(([q]) => Number(q)),
    // 3階「在庫なし」の報告がある保留伝票 / 1階が確認して閉じた伝票 (出荷保留・在庫なし)
    repickUnavailableSeqs: Object.keys(s.stockoutBySlip || {}).map(Number),
    stockoutBySlip: s.stockoutBySlip || {},
    stockoutAckSeqs: s.stockoutAckSeqs || [],
    stockoutSeqs: s.slips.filter((x) => x.status === 'cancelled' && x.hold_reason === 'stockout').map((x) => x.seq),
    stockoutNotifyBySlip: s.stockoutNotifyBySlip || {},
    incidents: s.incidents,
    // 完了取消の対象 = 最後に完了した伝票 (完了順の正はイベント履歴 — done_at は秒精度)
    lastDoneSeq: lastDoneSeqOf(batchId),
    worker: s.batch.worker,
    startedAt: s.batch.started_at,
    finishedAt: s.batch.finished_at,
  };
}

/**
 * 作業イベントの適用。1イベント = 1トランザクション。
 * @returns {{replayed?: boolean, ...eventResult}}
 */
export function applyEvent(batchId, { opId, event, slipSeq, clientAt, reason, jumped, proposedMethod, sku, actualSku, actualName, qty, finalCount, manualCount, excludedCount, toPasCount, note }, worker) {
  if (!opId || typeof opId !== 'string' || opId.length > 64) {
    throw new PackError(400, 'bad_op_id', 'op_id が不正です');
  }
  if (!WORK_EVENTS.includes(event)) {
    throw new PackError(400, 'bad_event', `不明なイベントです: ${event}`);
  }
  if (!worker) throw new PackError(400, 'no_worker', '作業者を選択してください');
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    const prev = db.prepare('SELECT * FROM pk_pack_events WHERE op_id = ?').get(opId);
    if (prev) {
      const pp = prev.payload_json ? JSON.parse(prev.payload_json) : {};
      if (prev.batch_id === batchId && prev.worker === worker
          && prev.event === event && (prev.slip_seq ?? null) === (slipSeq ?? null)
          && (pp.reason ?? null) === (reason ?? null) && !!pp.jumped === !!jumped
          && (pp.proposedMethod ?? null) === (proposedMethod ?? null)
          && (pp.sku ?? null) === (sku ?? null) && (pp.actualSku ?? null) === (actualSku ?? null)
          && (pp.qty ?? null) === (qty ?? null)
          && (pp.finalCount ?? null) === (finalCount ?? null)
          && (pp.manualCount ?? null) === (manualCount ?? null)
          && (pp.excludedCount ?? null) === (excludedCount ?? null)
          && (pp.toPasCount ?? null) === (toPasCount ?? null)
          && (pp.note ?? null) === (note ?? null)) {
        return { replayed: true, ...JSON.parse(prev.result_json) };
      }
      throw new PackError(409, 'op_conflict', '同じ操作IDが別の内容で使われています');
    }

    const batch = db.prepare('SELECT * FROM pk_pack_batches WHERE id = ?').get(batchId);
    if (!batch) throw new PackError(404, 'not_found', 'バッチが見つかりません');
    if (batch.validity !== 'valid' || batch.status === 'cancelled') {
      throw new PackError(409, 'batch_invalid', 'このバッチは取消されています');
    }
    const requireOwner = () => {
      if (batch.worker !== worker) throw new PackError(409, 'taken', `このバッチは ${batch.worker} が作業中です`);
    };
    // 梱包機ライン種別 (null=手梱包)。イベント族をサーバー側で相互排他にする (Codex high:
    // 伝票イベントとライン工程の混在は「伝票一部done+line_doneで完了」等の矛盾状態を作る)
    const lineKind = lineKindOf(batchHikiateClass(db, batch));
    // ラインバッチからの伝票単位の依頼 (再ピック/配送変更/再印刷/見つかった/受領)。
    // 工程は担当交代 (仕分け→流し) があるので所有者チェックはせず (作業者名は router で有効名を検証済み)、
    // 状態も取消以外なら受け付ける — 紙の作業は画面の開始/終了と必ずしも同期しないため
    const lineSlipRequest = !!lineKind && LINE_SLIP_REQUEST_EVENTS.includes(event);
    if (lineKind && SLIP_ONLY_EVENTS.includes(event) && !lineSlipRequest) {
      throw new PackError(409, 'line_batch', '梱包機バッチはライン管理画面から操作してください');
    }
    const requireSlipOp = (statuses) => {
      if (lineSlipRequest) return;
      if (!statuses.includes(batch.status)) {
        throw new PackError(409, 'not_packing', `作業中ではありません (${batch.status})`);
      }
      requireOwner();
    };
    if (!lineKind && LINE_EVENTS.includes(event)) {
      throw new PackError(409, 'not_line_batch', '梱包機バッチではありません');
    }
    let taskNotify = null;   // ①②のGChat通知情報 (routerがfail-softで送る。replayでは再送しない)
    let switchedFrom = null; // ライン工程開始での担当交代 (payload に監査記録)
    // 再ピック待ち区間の境界時刻。中断/再開は端末時刻 (クランプ) を使い、中断計測と二重控除しない (Codex)
    let blockedAt = now;

    if (event === 'takeover') {
      // 担当者の交代 (選び間違い・実際の引き継ぎ)。記録が残る唯一の正規突破口。
      // 完了後も可 (中原さん指示 2026-08-21: 選び間違いに完了後に気づいても帰属を直せる)
      if (!['packing', 'paused', 'done'].includes(batch.status)) {
        throw new PackError(409, 'not_packing', `交代できる状態ではありません (${batch.status})`);
      }
      if (batch.worker !== worker) {
        db.prepare('UPDATE pk_pack_batches SET worker=?, updated_at=? WHERE id=?').run(worker, now, batchId);
      }
      if (lineKind) {
        if (batch.status === 'done') {
          // 完了後の修正 = 最終帰属 (流し担当) を直す。仕分け担当の記録は別人の可能性があるため変えない
          db.prepare(`UPDATE pk_pack_line_runs SET worker=?, updated_at=?
            WHERE batch_id=? AND phase='run'`).run(worker, now, batchId);
        } else {
          // ライン工程中の交代は「進行中の工程行」の担当も引き継ぐ (工程途中の操作判定は
          // 工程行の worker で行うため。完了済みの工程の担当記録は変えない)。
          // batch.worker が既に本人でも実行する — 工程行だけ食い違う不整合の修復口 (Codex medium)
          db.prepare(`UPDATE pk_pack_line_runs SET worker=?, updated_at=?
            WHERE batch_id=? AND started_at IS NOT NULL
              AND (finished_at IS NULL OR (phase='run' AND final_count IS NULL))
          `).run(worker, now, batchId);
        }
      }
    } else if (event === 'pause') {
      // 中断: 中断時間は梱包時間から除外する。時刻は端末の発生時刻をクランプ採用
      // (オフラインで pause→resume を積むと受信時刻基準では中断が数秒になる — picking と同じ)
      if (batch.status !== 'packing') {
        throw new PackError(409, 'not_packing', `作業中ではないため中断できません (${batch.status})`);
      }
      requireOwner();
      if (lineKind && !inProgressLineRun(db, batchId)) {
        // ライン: 進行中の工程 (仕分け中 / 流し中) があるときだけ中断できる。
        // 停止後の件数入力待ちや工程間で中断すると差し引く先の工程が無く、計測が壊れる
        throw new PackError(409, 'no_phase', '進行中の工程がありません (中断できるのは仕分け中・流し中のみ)');
      }
      const r = PAUSE_REASONS.includes(reason) ? reason : 'その他';
      const pauseAt = clampedEventTime(clientAt, now, batch.started_at);
      db.prepare(`UPDATE pk_pack_batches SET status='paused', pause_started_at=?, pause_reason=?, updated_at=? WHERE id=?`)
        .run(pauseAt, r, now, batchId);
      blockedAt = pauseAt;
    } else if (event === 'resume') {
      if (batch.status !== 'paused') {
        throw new PackError(409, 'not_paused', `中断中ではありません (${batch.status})`);
      }
      requireOwner();
      const resumeAt = clampedEventTime(clientAt, now, batch.pause_started_at);
      const pausedSec = Math.max(0, Math.round((Date.parse(resumeAt) - Date.parse(batch.pause_started_at || resumeAt)) / 1000));
      db.prepare(`
        UPDATE pk_pack_batches SET status='packing', paused_total_sec = paused_total_sec + ?,
          pause_started_at=NULL, pause_reason=NULL, updated_at=? WHERE id=?
      `).run(pausedSec, now, batchId);
      if (lineKind) {
        // 進行中の工程行にも中断秒を積む (工程の所要時間 = finished - started - paused_total_sec)
        const run = inProgressLineRun(db, batchId);
        if (run) {
          db.prepare('UPDATE pk_pack_line_runs SET paused_total_sec = paused_total_sec + ?, updated_at=? WHERE id=?')
            .run(pausedSec, now, run.id);
        }
      }
      blockedAt = resumeAt;
    } else if (event === 'cancel') {
      // 誤開始の取消: バッチを未着手に戻し、伝票の進捗も初期化する (pk_pack_events は残る)
      if (batch.status !== 'packing' && batch.status !== 'paused') {
        throw new PackError(409, 'not_packing', `作業中ではないため取消できません (${batch.status})`);
      }
      requireOwner();
      db.prepare(`
        UPDATE pk_pack_batches SET status='ready', worker=NULL, started_at=NULL, finished_at=NULL,
          paused_total_sec=0, pause_started_at=NULL, pause_reason=NULL,
          blocked_total_sec=0, blocked_since=NULL, updated_at=? WHERE id=?
      `).run(now, batchId);
      db.prepare("UPDATE pk_pack_slips SET status='pending', hold_reason=NULL, shown_at=NULL, done_at=NULL WHERE batch_id=?")
        .run(batchId);
      // 未対応の配送変更依頼を孤児化させない (Codexレビュー high: 取消後に事務が変更を実施すると
      // 梱包済み内容と配送方法が食い違う)。取消と同時に依頼も cancelled にする
      db.prepare(`
        UPDATE pk_pack_ship_changes SET status='cancelled', office_by='(バッチ取消)', updated_at=?
        WHERE batch_id=? AND status IN ('requested','accepted')
      `).run(now, batchId);
      // 再ピック/棚戻しタスクと未確定のミス候補も同様に後始末 (Codex Phase2 high:
      // 残すと再開後の重複ガードと旧タスクが新しい伝票状態と食い違う)
      // unavailable (3階の在庫なし報告) も取消対象 (Codex R1 High: 残すと再開後の重複ガードが永久に効き再依頼できない)
      for (const t of db.prepare("SELECT id FROM pk_pack_tasks WHERE batch_id=? AND status='unavailable'").all(batchId)) {
        resolveFloorAlertsByTask(t.id, 'stockout', db);
      }
      db.prepare(`
        UPDATE pk_pack_tasks SET status='cancelled', updated_at=?
        WHERE batch_id=? AND status IN ('requested','claimed','fulfilled','unavailable')
      `).run(now, batchId);
      db.prepare(`
        UPDATE pk_pack_incidents SET status='withdrawn', updated_at=?
        WHERE batch_id=? AND status='candidate'
      `).run(now, batchId);
      // 梱包機ライン工程も初期化 (誤開始の取消でやり直せるように。イベント履歴は残る)
      db.prepare('DELETE FROM pk_pack_line_runs WHERE batch_id=?').run(batchId);
    } else if (event === 'undo') {
      // 完了取消 (理由必須・監査ログ=このイベント自体)。対象は「最後に完了した伝票」のみ
      // (途中の伝票に飛び戻ると納品書の束と画面の対応が壊れる)。バッチ完了直後も取り消せる
      if (batch.status !== 'packing' && batch.status !== 'done') {
        throw new PackError(409, 'not_packing', `完了取消できる状態ではありません (${batch.status})`);
      }
      requireOwner();
      if (!UNDO_REASONS.includes(reason)) {
        throw new PackError(400, 'bad_reason', '取消理由を選択してください');
      }
      if (lineKind) {
        // ライン工程の段階的取消 (Codex high: 完了後に修正経路が無いと件数の入力ミスが復旧不能)。
        // 直近の記録から1段ずつ戻す: 流し終了→流し開始→仕分け完了→仕分け開始
        const sort = db.prepare("SELECT * FROM pk_pack_line_runs WHERE batch_id=? AND phase='sort'").get(batchId);
        const run = db.prepare("SELECT * FROM pk_pack_line_runs WHERE batch_id=? AND phase='run'").get(batchId);
        if (run?.final_count != null) {
          // 件数記録の取消 → 入力画面へ戻す (停止時刻は保持)
          db.prepare(`UPDATE pk_pack_line_runs SET final_count=NULL, manual_count=NULL, updated_at=?
            WHERE id=?`).run(now, run.id);
          db.prepare("UPDATE pk_pack_batches SET status='packing', finished_at=NULL, updated_at=? WHERE id=?")
            .run(now, batchId);
        } else if (run?.finished_at) {
          // 停止の取消 → タイマー再開
          db.prepare('UPDATE pk_pack_line_runs SET finished_at=NULL, updated_at=? WHERE id=?').run(now, run.id);
        } else if (run) {
          db.prepare('DELETE FROM pk_pack_line_runs WHERE id=?').run(run.id);
        } else if (sort?.finished_at) {
          db.prepare(`UPDATE pk_pack_line_runs SET finished_at=NULL, final_count=NULL, excluded_count=NULL, to_pas_count=NULL, updated_at=?
            WHERE id=?`).run(now, sort.id);
        } else if (sort) {
          db.prepare('DELETE FROM pk_pack_line_runs WHERE id=?').run(sort.id);
        } else {
          throw new PackError(409, 'nothing_to_undo', '取り消せる記録がありません');
        }
        // 最初の工程開始まで取り消したらバッチを未着手へ戻す (Codex medium: packing のまま
        // 残すと誤開始時刻 started_at が COALESCE で再利用され作業時間が過大になる)
        const left = db.prepare('SELECT COUNT(*) c FROM pk_pack_line_runs WHERE batch_id=?').get(batchId).c;
        if (left === 0) {
          db.prepare(`UPDATE pk_pack_batches SET status='ready', worker=NULL, started_at=NULL, finished_at=NULL,
            paused_total_sec=0, pause_started_at=NULL, pause_reason=NULL, updated_at=? WHERE id=?`)
            .run(now, batchId);
        }
        // 伝票側の undo 処理はライン工程では行わない
        const result0 = eventResult(batchId);
        db.prepare(`
          INSERT INTO pk_pack_events (op_id, batch_id, worker, event, slip_seq, payload_json, result_json, at)
          VALUES (?, ?, ?, ?, NULL, ?, ?, ?)
        `).run(opId, batchId, worker, event, JSON.stringify({ reason, line: true }), JSON.stringify(result0), now);
        return result0;
      }
      const lastSeq = lastDoneSeqOf(batchId);
      if (lastSeq == null) throw new PackError(409, 'nothing_to_undo', '取り消せる完了がありません');
      if (slipSeq != null && slipSeq !== lastSeq) {
        throw new PackError(409, 'out_of_order', `取り消せるのは最後に完了した伝票 (${lastSeq}) だけです`);
      }
      db.prepare("UPDATE pk_pack_slips SET status='pending', done_at=NULL, shown_at=? WHERE batch_id=? AND seq=?")
        .run(now, batchId, lastSeq);
      // 資材の完了スナップショットも解除 (再完了時に取り直す)
      try { materialOnSlipCompletionCleared(db, batchId, lastSeq); } catch { /* fail-soft */ }
      if (batch.status === 'done') {
        db.prepare("UPDATE pk_pack_batches SET status='packing', finished_at=NULL, updated_at=? WHERE id=?")
          .run(now, batchId);
      }
    } else if (event === 'jump') {
      // ズレ回復 (要件§5.1): 紙の束と画面がズレたときの頭出し。状態は変えず記録のみ
      // (頻度を計測し、多発するならスキャン再検討を含め対策を再協議する)
      if (batch.status !== 'packing' && batch.status !== 'paused') {
        throw new PackError(409, 'not_packing', `作業中ではありません (${batch.status})`);
      }
      requireOwner();
    } else if (event === 'ship_change') {
      // ④ 配送方法変更 (簡素化 — 中原さん指示 2026-08-17): 記録+GChat通知 (明細つき) のみ。
      // 伝票の保留や事務側の状態管理はしない — 現物を「変更待ちの棚」へ置く運用のため
      // 放置されず、事務の画面操作 (1工程) も不要。梱包者はそのまま「次へ」で完了してよい。
      // paused も許可 (④⑥フォーム表示中は自動中断で計測を止めるため — 2026-08-21)
      requireSlipOp(['packing', 'paused', 'done']);
      const proposed = String(proposedMethod || '').trim();
      const twoLabels = proposed === SHIP_CHANGE_TWO_LABELS;
      if (!twoLabels && !SHIP_CHANGE_METHOD_OPTIONS.includes(proposed)) {
        throw new PackError(400, 'bad_method', '提案する配送方法を選択してください');
      }
      const reasonVal = twoLabels ? (reason || '入らない') : reason;
      if (!SHIP_CHANGE_REASONS.includes(reasonVal)) {
        throw new PackError(400, 'bad_reason', '理由を選択してください');
      }
      const slip = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, slipSeq);
      if (!slip) throw new PackError(404, 'slip_not_found', `伝票 ${slipSeq} がありません`);
      db.prepare(`
        INSERT INTO pk_pack_ship_changes
          (batch_id, slip_seq, ne_slip_no, folder_name, current_method, proposed_method,
           reason, requested_by, status, updated_at, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'requested', ?, ?)
      `).run(batchId, slipSeq, slip.ne_slip_no, batch.folder_name, slip.delivery_method,
        proposed, reasonVal, worker, now, now);
    } else if (event === 'reprint' || event === 'label_missing') {
      // 🖨 伝票再印刷依頼 (2026-08-21 中原さん指示): 記録+即時通知のみ。伝票状態は変えず、
      // 梱包画面にも痕跡を出さない (理由入力なし)。完了済み伝票でも押せる (配送変更と同様)
      // 📭 label_missing (2026-08-26): 「送り状が束に無かった」の事務通知。経路は再印刷と同一 (kind で区別)
      requireSlipOp(['packing', 'done']);
      const slip = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, slipSeq);
      if (!slip) throw new PackError(404, 'slip_not_found', `伝票 ${slipSeq} がありません`);
      const info = db.prepare(`
        INSERT INTO pk_pack_reprints (batch_id, slip_seq, ne_slip_no, site_order_no, folder_name,
          recipient_name, requested_by, created_at, kind)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      `).run(batchId, slipSeq, slip.ne_slip_no, slip.site_order_no || null, batch.folder_name,
        slip.recipient_name || null, worker, now, event);
      var reprintId = Number(info.lastInsertRowid);   // eventResult後にresultへ載せる (var=分岐外参照)
    } else if (['shortage', 'excess', 'wrong_item', 'found', 'receive', 'stockout_ack'].includes(event)) {
      // ①不足→再ピック / ②余り→棚戻し / 品違い / 見つかった / 受領 / 在庫なしを確認 (要件§5.4〜5.6)。
      // 完了済み伝票への不足/品違いも許可 (誤タップ後の気づき) — バッチ完了済みなら再オープン
      // (ラインバッチは再オープンしない: バッチの完了は件数記録 line_done で決まり、伝票の保留とは独立)
      requireSlipOp(['packing', 'done']);
      taskNotify = applyTaskEvent(db, batch, event, { slipSeq, sku, actualSku, actualName, qty }, worker, now).notify || null;
      if (!lineKind && batch.status === 'done' && ['shortage', 'wrong_item'].includes(event)) {
        db.prepare("UPDATE pk_pack_batches SET status='packing', finished_at=NULL, updated_at=? WHERE id=?")
          .run(now, batchId);
      }
      // 「在庫なしを確認」で閉じた伝票が最後の1枚なら、そのままバッチ完了 (「次へ」を経ないため — Q1 決定 2026-09-05)。
      // 完了条件は next と同じ = 未処理ゼロ + 保留ゼロ
      if (!lineKind && event === 'stockout_ack' && batch.status === 'packing') {
        const c = db.prepare(`SELECT SUM(status='pending') AS p, SUM(status='held') AS h FROM pk_pack_slips WHERE batch_id=?`).get(batchId);
        if ((c.p || 0) === 0 && (c.h || 0) === 0) {
          db.prepare("UPDATE pk_pack_batches SET status='done', finished_at=?, updated_at=? WHERE id=?").run(now, now, batchId);
        }
      }
    } else if (LINE_EVENTS.includes(event)) {
      // 梱包機ライン工程 (PAS/MELT — 紙台帳の置き換え。要件v7)。伝票めくりの代わりに
      // 工程単位で開始/終了と件数を記録する。伝票行は触らない (バッチ完了は line_done で直接)
      const kind = lineKind;
      const getRun = (phase) => db.prepare(
        'SELECT * FROM pk_pack_line_runs WHERE batch_id=? AND phase=?').get(batchId, phase);
      const startBatchIfReady = () => {
        if (batch.status === 'done') throw new PackError(409, 'already_done', 'このバッチは完了済みです');
        if (batch.status === 'paused') {
          // 中断中の完了・開始を許すと pause_* が残ったまま done になり計測が壊れる (Codex high)
          throw new PackError(409, 'not_packing', '中断中です。再開してから操作してください');
        }
        if (batch.status === 'ready') {
          db.prepare(`UPDATE pk_pack_batches SET status='packing', worker=?,
            started_at=COALESCE(started_at, ?), updated_at=? WHERE id=?`).run(worker, now, now, batchId);
        } else if (batch.worker !== worker) {
          // 工程の開始は担当交代を許可 (中原さん指示 2026-08-20: MELTは仕分けと流しが別人のことがある)。
          // 誰がどの工程をやったかは pk_pack_line_runs.worker (工程別) とイベント履歴に残り、
          // 交代自体もこのイベントの payload (switchedFrom) に監査記録する。
          // 工程の途中操作 (仕分け完了・停止・件数記録) はその工程の担当者のみ (requirePhaseWorker)
          switchedFrom = batch.worker;
          db.prepare('UPDATE pk_pack_batches SET worker=?, updated_at=? WHERE id=?').run(worker, now, batchId);
        }
      };
      const requirePackingStatus = () => {
        if (batch.status !== 'packing') {
          throw new PackError(409, 'not_packing', `作業中ではありません (${batch.status})`);
        }
      };
      // 工程途中の操作はその工程の担当者のみ。batch.worker は工程開始・takeover で動くため
      // 判定は工程行の worker と直接比較する (Codexレビュー high: 第三者のtakeoverで
      // 開始者が締め出される/横取りできる穴を塞ぐ)
      const requirePhaseWorker = (row) => {
        if (row.worker !== worker) {
          throw new PackError(409, 'taken', `この工程は ${row.worker} が作業中です (交代は「引き継ぐ」で)`);
        }
      };
      if (event === 'line_sort_start') {
        if (kind !== 'melt') throw new PackError(409, 'not_melt', '仕分け工程は MELT-LINE のみです');
        if (getRun('sort')) throw new PackError(409, 'already_started', '仕分けは開始済みです');
        startBatchIfReady();
        db.prepare(`INSERT INTO pk_pack_line_runs (batch_id, phase, started_at, planned_count, worker, updated_at)
          VALUES (?, 'sort', ?, ?, ?, ?)`).run(batchId, now, batch.slip_count, worker, now);
      } else if (event === 'line_sort_done') {
        const run = getRun('sort');
        if (!run) throw new PackError(409, 'not_started', '仕分けが開始されていません');
        if (run.finished_at) throw new PackError(409, 'already_done', '仕分けは完了済みです');
        requirePackingStatus();
        requirePhaseWorker(run);
        // 入力は「他の方法で出荷する件数」(除外件数)。機械に流す件数 = 伝票数 - 除外 を
        // 自動計算して final_count に持つ (中原さん指示 2026-08-18 — 現場は外した数を数えている)。
        // 未入力は 0 と区別して拒否 (Codex medium: Number(null)===0)
        if (excludedCount == null) throw new PackError(400, 'bad_count', '他の方法で出荷する件数を入力してください (無ければ0)');
        const ex = Number(excludedCount);
        if (!Number.isInteger(ex) || ex < 0 || ex > run.planned_count) {
          throw new PackError(400, 'bad_count', `他の方法で出荷する件数は 0〜${run.planned_count} で入力してください`);
        }
        // 除外の内訳: PAS-LINE へ移した件数 (3つ折り等)。PAS の本日累計 (機械カウンタ突合) に加算する
        // (2026-08-31 現場意見)。未指定は 0 (旧画面・API 互換)
        const tp = toPasCount == null ? 0 : Number(toPasCount);
        if (!Number.isInteger(tp) || tp < 0 || tp > ex) {
          throw new PackError(400, 'bad_count', 'PAS-LINE へ移す件数は「他の方法で出荷する件数」の内数 (0〜' + ex + ') で入力してください');
        }
        db.prepare(`UPDATE pk_pack_line_runs SET finished_at=?, excluded_count=?, to_pas_count=?, final_count=?, note=?, worker=?, updated_at=?
          WHERE id=?`).run(now, ex, tp, run.planned_count - ex, note || run.note || null, worker, now, run.id);
      } else if (event === 'line_start') {
        if (getRun('run')) throw new PackError(409, 'already_started', '機械流しは開始済みです');
        let planned = batch.slip_count;
        if (kind === 'melt') {
          const sort = getRun('sort');
          if (!sort?.finished_at) throw new PackError(409, 'sort_first', '先に仕分けを完了してください');
          planned = sort.final_count;
        }
        startBatchIfReady();
        db.prepare(`INSERT INTO pk_pack_line_runs (batch_id, phase, started_at, planned_count, worker, updated_at)
          VALUES (?, 'run', ?, ?, ?, ?)`).run(batchId, now, planned, worker, now);
      } else if (event === 'line_stop') {
        // 流し終了 = まず時計を止めるだけ (件数はこの後の入力画面で。中原さん指示 2026-08-18:
        // 終了の瞬間に時刻を確定し、それから件数を落ち着いて入力する)
        const run = getRun('run');
        if (!run) throw new PackError(409, 'not_started', '機械流しが開始されていません');
        if (run.finished_at) throw new PackError(409, 'already_done', 'すでに停止済みです');
        requirePackingStatus();
        requirePhaseWorker(run);
        db.prepare('UPDATE pk_pack_line_runs SET finished_at=?, worker=?, updated_at=? WHERE id=?')
          .run(clampedEventTime(clientAt, now, run.started_at), worker, now, run.id);
      } else {   // line_done — 件数の記録 (line_stop 後のみ。入力するまでバッチは完了しない)
        const run = getRun('run');
        if (!run) throw new PackError(409, 'not_started', '機械流しが開始されていません');
        if (!run.finished_at) throw new PackError(409, 'not_stopped', '先に「終了」で時間を止めてください');
        if (run.final_count != null) throw new PackError(409, 'already_done', 'このラインは記録済みです');
        requirePackingStatus();
        requirePhaseWorker(run);
        // 出荷完了件数 (紙台帳の「出荷完了件数」)。うち手動 = 機械を通さず手で流した分。
        // 未入力は 0 と区別して拒否 (Codex medium)
        if (finalCount == null) throw new PackError(400, 'bad_count', '出荷完了件数を入力してください');
        const fc = Number(finalCount);
        const mc = manualCount == null ? 0 : Number(manualCount);
        if (!Number.isInteger(fc) || fc < 0 || fc > run.planned_count) {
          throw new PackError(400, 'bad_count', `完了件数は 0〜${run.planned_count} で入力してください`);
        }
        if (!Number.isInteger(mc) || mc < 0 || mc > fc) {
          throw new PackError(400, 'bad_count', '手動で流した件数は完了件数以下で入力してください');
        }
        db.prepare(`UPDATE pk_pack_line_runs SET final_count=?, manual_count=?, note=?, worker=?, updated_at=?
          WHERE id=?`).run(fc, mc, note || run.note || null, worker, now, run.id);
        db.prepare("UPDATE pk_pack_batches SET status='done', finished_at=?, updated_at=? WHERE id=?")
          .run(run.finished_at, now, batchId);
      }
    } else if (event === 'start') {
      if (batch.status === 'packing') {
        if (batch.worker !== worker) {
          throw new PackError(409, 'taken', `このバッチは ${batch.worker} が作業中です`);
        }
        // 同一作業者の再開 (リロード・端末復帰)。状態はそのまま返す
      } else if (batch.status === 'done') {
        throw new PackError(409, 'already_done', 'このバッチは完了済みです');
      } else {
        const res = db.prepare(`
          UPDATE pk_pack_batches SET status='packing', worker=?, started_at=COALESCE(started_at, ?), updated_at=?
          WHERE id=? AND status='ready'
        `).run(worker, now, now, batchId);
        if (res.changes === 0) throw new PackError(409, 'not_startable', `状態 ${batch.status} からは開始できません`);
      }
      // 先頭の未完了伝票に表示時刻 (slip_opened) を刻む (初回のみ)
      db.prepare(`
        UPDATE pk_pack_slips SET shown_at = COALESCE(shown_at, ?)
        WHERE batch_id = ? AND seq = (SELECT MIN(seq) FROM pk_pack_slips WHERE batch_id = ? AND status = 'pending')
      `).run(now, batchId, batchId);
    } else {
      // next: 基本は現在表示中の伝票 (= 最小の pending) のみ完了できる。
      // 例外 = ズレ回復ジャンプ後 (jumped): 任意の pending 伝票を完了できる。
      // 飛ばされた伝票は pending のまま残り、全伝票 done になるまでバッチは完了しない
      // (= 保留し忘れ・飛ばし忘れがバッチ末尾で必ず検知される完了ガード — 要件§5.1)
      if (batch.status !== 'packing') {
        throw new PackError(409, 'not_packing', `バッチが作業中ではありません (${batch.status})`);
      }
      requireOwner();
      if (!Number.isInteger(slipSeq) || slipSeq < 1) {
        throw new PackError(400, 'bad_slip_seq', 'slip_seq が不正です');
      }
      const cur = db.prepare(
        "SELECT MIN(seq) s FROM pk_pack_slips WHERE batch_id=? AND status='pending'"
      ).get(batchId).s;
      if (cur !== slipSeq) {
        if (!jumped) {
          throw new PackError(409, 'out_of_order', `伝票 ${slipSeq} は現在の対象 (${cur ?? 'なし'}) ではありません`);
        }
        const st = db.prepare('SELECT status FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, slipSeq);
        if (!st) throw new PackError(404, 'slip_not_found', `伝票 ${slipSeq} がありません`);
        if (st.status !== 'pending') {
          throw new PackError(409, 'not_pending', `伝票 ${slipSeq} は処理済みです (${st.status})`);
        }
      }
      // packing_completed (現在伝票) — done_at がその記録
      db.prepare("UPDATE pk_pack_slips SET status='done', done_at=? WHERE batch_id=? AND seq=?")
        .run(now, batchId, slipSeq);
      // 資材の表示観測ログ: 完了時点の判定をスナップショット固定 (v10 未適用環境は無視)
      try { materialOnSlipCompleted(db, batchId, slipSeq, now); } catch { /* fail-soft */ }
      // slip_opened (次伝票) — 紙の束は前から順なので「完了した伝票の次以降で最初の pending」を
      // 優先し、無ければ先頭の pending (飛ばした伝票へ戻る)
      const nextSeq = db.prepare(
        'SELECT MIN(seq) s FROM pk_pack_slips WHERE batch_id=? AND status=\'pending\' AND seq > ?'
      ).get(batchId, slipSeq).s
        ?? db.prepare("SELECT MIN(seq) s FROM pk_pack_slips WHERE batch_id=? AND status='pending'").get(batchId).s;
      if (nextSeq != null) {
        db.prepare('UPDATE pk_pack_slips SET shown_at = ? WHERE batch_id=? AND seq=?')
          .run(now, batchId, nextSeq);
      } else {
        // バッチ完了条件: 未処理ゼロ + 保留 (配送変更待ち等) ゼロ (要件§5.11 完了ガード)
        const held = db.prepare(
          "SELECT COUNT(*) c FROM pk_pack_slips WHERE batch_id=? AND status='held'"
        ).get(batchId).c;
        if (held === 0) {
          db.prepare("UPDATE pk_pack_batches SET status='done', finished_at=?, updated_at=? WHERE id=?")
            .run(now, now, batchId);
        }
      }
    }

    // 「ピッキングミスで梱包できない待ち時間」の区間更新 (中原さん指示 2026-08-23: 梱包時間に含めない)。
    // 伝票状態が動くイベントの後に毎回評価する (手梱包バッチのみ。ライン工程は対象外)
    if (!lineKind) syncBlockedState(db, batchId, blockedAt);
    const result = eventResult(batchId);
    // eventResult はこのイベント行の INSERT より前に走るため、lastDoneSeq (イベント履歴由来) に
    // いま完了させた伝票が反映されない。next のときはここで上書きする
    if (event === 'next') result.lastDoneSeq = slipSeq;
    if ((event === 'reprint' || event === 'label_missing') && typeof reprintId !== 'undefined') result.reprintId = reprintId;
    const payload = (clientAt || reason || jumped || proposedMethod || sku || actualSku || qty != null
        || finalCount != null || manualCount != null || excludedCount != null || toPasCount != null || note || switchedFrom)
      ? JSON.stringify({ clientAt, reason, jumped: jumped || undefined, proposedMethod: proposedMethod || undefined,
          sku: sku || undefined, actualSku: actualSku || undefined, actualName: actualName || undefined, qty: qty ?? undefined,
          finalCount: finalCount ?? undefined, manualCount: manualCount ?? undefined,
          excludedCount: excludedCount ?? undefined, toPasCount: toPasCount ?? undefined, note: note || undefined,
          switchedFrom: switchedFrom || undefined }) : null;
    db.prepare(`
      INSERT INTO pk_pack_events (op_id, batch_id, worker, event, slip_seq, payload_json, result_json, at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(opId, batchId, worker, event, slipSeq ?? null, payload, JSON.stringify(result), now);
    // taskNotify は保存済み結果に含めない = replay の再送で通知が重複しない
    return taskNotify ? { ...result, taskNotify } : result;
  })();
}

/**
 * 「ピッキングミスで梱包できない待ち時間」の計測 (中原さん指示 2026-08-23: 梱包時間に含めない)。
 * 条件 = 作業中 (packing) かつ 未処理ゼロ かつ 保留あり (= 再ピック待ちで手が止まっている)。
 * 中断 (paused) 中は中断側で除外するので、status != packing では区間を閉じて二重控除しない。
 * 実装は中断と同型: blocked_since (区間開始) / blocked_total_sec (累計)。
 */
function syncBlockedState(db, batchId, at) {
  const b = db.prepare('SELECT status, blocked_since FROM pk_pack_batches WHERE id = ?').get(batchId);
  if (!b) return;
  // 保留理由は再ピックに限定 (計測ルールを直接表現 — 将来の別種保留を巻き込まない。Codex)
  const c = db.prepare(`
    SELECT SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
           SUM(CASE WHEN status = 'held' AND hold_reason = 'repick' THEN 1 ELSE 0 END) AS held
    FROM pk_pack_slips WHERE batch_id = ?
  `).get(batchId);
  const shouldBlock = b.status === 'packing' && (c.pending || 0) === 0 && (c.held || 0) > 0;
  if (shouldBlock && !b.blocked_since) {
    db.prepare('UPDATE pk_pack_batches SET blocked_since = ? WHERE id = ?').run(at, batchId);
  } else if (!shouldBlock && b.blocked_since) {
    const sec = Math.max(0, Math.round((Date.parse(at) - Date.parse(b.blocked_since)) / 1000));
    db.prepare('UPDATE pk_pack_batches SET blocked_total_sec = blocked_total_sec + ?, blocked_since = NULL WHERE id = ?')
      .run(sec, batchId);
  }
}

/** バッチの実働秒 = (finished - started) - 中断合計 - 再ピック待ち合計。未完了は null。 */
export function batchActiveSec(b) {
  if (!b.started_at || !b.finished_at) return null;
  return Math.max(0, Math.round((Date.parse(b.finished_at) - Date.parse(b.started_at)) / 1000)
    - (b.paused_total_sec || 0) - (b.blocked_total_sec || 0));
}

// ═══ 日次サマリ (⑦計測の集計・要件§5.11) ═══════════════════════════════

/**
 * 指定作業日のサマリ。
 *   - バッチ単位: 実働 = (finished - started) - 中断合計 - 再ピック待ち合計 (梱包できない時間)
 *   - 伝票単位: done_at - shown_at (表示〜完了)。伝票表示中の中断は現状含まれる
 *     (中断はバッチ単位で記録のため。除外精度が必要になったらイベントから差し引く)
 *   - ズレ回復 (jump)・完了取消 (undo) の回数も集計 (要件§5.1: 頻度を計測し多発なら再協議)
 */
export function getDailySummary(workDate) {
  const db = getDB();
  const batches = db.prepare(`
    SELECT * FROM pk_pack_batches
    WHERE work_date = ? AND validity = 'valid' AND status != 'cancelled'
    ORDER BY id
  `).all(workDate);

  const activeSec = batchActiveSec;

  const done = batches.filter((b) => b.status === 'done');
  const total = {
    batchCount: batches.length,
    doneCount: done.length,
    slipCount: done.reduce((s, b) => s + b.slip_count, 0),
    qty: done.reduce((s, b) => s + b.total_qty, 0),
    activeSec: done.reduce((s, b) => s + (activeSec(b) || 0), 0),
    blockedSec: done.reduce((s, b) => s + (b.blocked_total_sec || 0), 0),   // 除外した再ピック待ち
  };
  total.secPerSlip = total.slipCount > 0 ? total.activeSec / total.slipCount : null;
  total.slipsPerHour = total.secPerSlip ? 3600 / total.secPerSlip : null;

  // 作業者別 (バッチの最終担当者に全量計上する簡略化 — picking と同じ)
  const byWorker = new Map();
  for (const b of done) {
    const k = b.worker || '(不明)';
    if (!byWorker.has(k)) byWorker.set(k, { key: k, batches: 0, slips: 0, qty: 0, activeSec: 0 });
    const g = byWorker.get(k);
    g.batches++; g.slips += b.slip_count; g.qty += b.total_qty; g.activeSec += activeSec(b) || 0;
  }
  const workers = [...byWorker.values()].map((g) => ({
    ...g, secPerSlip: g.slips > 0 ? g.activeSec / g.slips : null,
  })).sort((a, b) => b.slips - a.slips);

  // 伝票単位の実測 (表示〜完了秒)。バッチ横断の中央値・平均
  const slipSecs = db.prepare(`
    SELECT (unixepoch(s.done_at) - unixepoch(s.shown_at)) AS sec
    FROM pk_pack_slips s JOIN pk_pack_batches b ON b.id = s.batch_id
    WHERE b.work_date = ? AND b.validity = 'valid' AND s.status = 'done'
      AND s.done_at IS NOT NULL AND s.shown_at IS NOT NULL
      AND (unixepoch(s.done_at) - unixepoch(s.shown_at)) BETWEEN 0 AND 1800
    ORDER BY sec
  `).all(workDate).map((r) => r.sec);
  const slipStats = slipSecs.length > 0 ? {
    count: slipSecs.length,
    median: slipSecs[Math.floor(slipSecs.length / 2)],
    avg: slipSecs.reduce((a, x) => a + x, 0) / slipSecs.length,
  } : null;

  // 例外操作の頻度 (jump=紙ズレ回復・undo=完了取消)
  const opCounts = {};
  for (const r of db.prepare(`
    SELECT e.event, COUNT(*) c FROM pk_pack_events e
    JOIN pk_pack_batches b ON b.id = e.batch_id
    WHERE b.work_date = ? AND e.event IN ('jump','undo','cancel','takeover','pause')
    GROUP BY e.event
  `).all(workDate)) {
    opCounts[r.event] = r.c;
  }

  // ③ 確定済みピッキングミス (帰責=picking担当) と未確定候補 (要件§5.6)
  let incidents = { confirmed: [], byWorker: [], candidateCount: 0 };
  try {
    incidents.confirmed = db.prepare(`
      SELECT i.*, b.folder_name FROM pk_pack_incidents i
      JOIN pk_pack_batches b ON b.id = i.batch_id
      WHERE b.work_date = ? AND i.status = 'confirmed' ORDER BY i.id
    `).all(workDate);
    const m = new Map();
    for (const i of incidents.confirmed) {
      const k = i.attributed_worker || '(担当不明)';
      if (!m.has(k)) m.set(k, { key: k, count: 0, shortage: 0, excess: 0, wrong_item: 0 });
      const g = m.get(k);
      g.count++; g[i.kind]++;
    }
    incidents.byWorker = [...m.values()].sort((a, b) => b.count - a.count);
    incidents.candidateCount = db.prepare(`
      SELECT COUNT(*) c FROM pk_pack_incidents i
      JOIN pk_pack_batches b ON b.id = i.batch_id
      WHERE b.work_date = ? AND i.status = 'candidate'
    `).get(workDate).c;
  } catch { /* v4未適用環境 */ }

  return {
    workDate, total, workers, slipStats, opCounts, incidents,
    batches: batches.map((b) => ({ ...b, activeSec: activeSec(b) })),
  };
}

// ═══ ①再ピック / ②棚戻し / ③ミス記録 (要件§5.4〜5.6・Phase 2) ═══════════

// タスク状態機械 (要件の状態を実運用の最小操作に簡約):
//   repick: requested → claimed (対応する) → fulfilled (持って行った) → received (梱包者が受領)
//   return: requested → claimed → returned (棚に戻した)
//   例外: unavailable (在庫なし等 → エスカレーション) / cancelled (見つかった・取下げ)
const TASK_TRANSITIONS = {
  claim: { from: ['requested'], to: 'claimed' },
  // unavailable → fulfill も許す (例外処理監査 PR-1): 「在庫なし」にした後で他ロケで見つけた・届けた、を記録できる
  // (以前は取り消せず、9/5 にピッカーが /tasks で在庫なし→直後にバッチで確保しても記録が在庫なしのままだった)
  fulfill: { from: ['claimed', 'unavailable'], to: null },   // to は kind で決まる (下で解決)
  unavailable: { from: ['requested', 'claimed'], to: 'unavailable' },
  // 🔴再ピックバッチの back (完了/在庫なしを取り消して作業中へ) / cancel (未着手へ) の同期用 (Codex R1 High:
  // これが無いと取り消した欠品が同期されず、後から fulfilled に化けた)。received は終端で戻さない
  resume: { from: ['unavailable', 'fulfilled'], to: 'claimed' },
  reopen: { from: ['claimed', 'unavailable', 'fulfilled'], to: 'requested' },
  cancel: { from: ['requested', 'claimed', 'unavailable'], to: 'cancelled' },
};

/** バッチの pk_lines (picking所有・参照のみ) から SKU の参考ロケを引く。 */
function lookupLocation(pkBatchId, sku) {
  if (!pkBatchId) return { location: null, block: null };
  try {
    const row = getDB().prepare(
      'SELECT location, block FROM pk_lines WHERE batch_id = ? AND LOWER(TRIM(sku)) = ? LIMIT 1'
    ).get(pkBatchId, normSku(sku));
    return { location: row?.location ?? null, block: row?.block ?? null };
  } catch {
    return { location: null, block: null };
  }
}

function insertTask(db, batch, { slipSeq, kind, sku, productName, qty, incidentId }, worker, now) {
  // 業務キー (伝票×SKU×種別) の重複依頼ガード (要件§5.4-6)
  // 3階が「在庫なし」にした依頼 (unavailable) も生きている扱い — 以前は数えず、1階が同じ商品を再依頼して
  // 🔴バッチがもう1本立った (9/5 出荷_55)。伝票の「在庫なしを確認」で閉じるまで再依頼させない
  const dup = db.prepare(`
    SELECT id, status FROM pk_pack_tasks
    WHERE batch_id=? AND kind=? AND LOWER(TRIM(sku))=? AND (slip_seq IS ? OR slip_seq = ?)
      AND status IN ('requested','claimed','fulfilled','unavailable')
  `).get(batch.id, kind, normSku(sku), slipSeq ?? null, slipSeq ?? -1);
  if (dup?.status === 'unavailable') {
    throw new PackError(409, 'stockout_reported', `3階から「在庫なし」の報告があります (${sku})。伝票の「在庫なしを確認」で閉じてください`);
  }
  if (dup) throw new PackError(409, 'dup_task', `同じ依頼が既にあります (${kind} ${sku})`);
  const loc = lookupLocation(batch.pk_batch_id, sku);
  return Number(db.prepare(`
    INSERT INTO pk_pack_tasks
      (batch_id, slip_seq, kind, sku, product_name, req_qty, location, block, folder_name,
       status, requested_by, incident_id, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'requested', ?, ?, ?, ?)
  `).run(batch.id, slipSeq ?? null, kind, sku, productName ?? null, qty,
    loc.location, loc.block, batch.folder_name, worker, incidentId ?? null, now, now).lastInsertRowid);
}

function insertIncident(db, batch, { slipSeq, kind, sku, actualSku, actualName, qty }, worker, now) {
  return Number(db.prepare(`
    INSERT INTO pk_pack_incidents
      (batch_id, slip_seq, kind, sku, actual_sku, actual_name, qty, status, detected_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'candidate', ?, ?, ?)
  `).run(batch.id, slipSeq ?? null, kind, sku, actualSku ?? null, actualName ?? null, qty, worker, now, now).lastInsertRowid);
}

/** 伝票内のSKU明細を引く (数量上限の検証用)。 */
function slipLineOf(db, batchId, slipSeq, sku) {
  return db.prepare(`
    SELECT l.* FROM pk_pack_lines l
    JOIN pk_pack_slips s ON s.id = l.slip_id
    WHERE s.batch_id=? AND s.seq=? AND LOWER(TRIM(l.sku))=?
  `).get(batchId, slipSeq, normSku(sku));
}

/**
 * 伝票を『出荷保留 (在庫なし)』として閉じる (在庫なしを確認の共通部分)。
 * 伝票は 'cancelled' + hold_reason='stockout' (= この束では梱包しない。保留ではないのでバッチ完了を妨げない)。
 * 事務通知は outbox (pk_pack_stockouts) に同一トランザクションで積む。送れたときだけ notified_at、未送信はポーラーが再送。
 * 1階へ出ていた欠品バナー (🕒/❌・受注単位) も閉じる
 */
function closeSlipAsStockout(db, batch, slip, items, worker, now) {
  db.prepare("UPDATE pk_pack_slips SET status='cancelled', hold_reason='stockout', done_at=NULL WHERE id=?").run(slip.id);
  resolveFloorAlertsByRef(`alloc:%:${slip.ne_slip_no}`, { prefix: false, dbh: db, like: true });
  const stockoutId = Number(db.prepare(`INSERT INTO pk_pack_stockouts
    (batch_id, slip_seq, ne_slip_no, site_order_no, recipient_name, folder_name, items_json, worker, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
    .run(batch.id, slip.seq, slip.ne_slip_no, slip.site_order_no || null, slip.recipient_name || null,
      batch.folder_name, JSON.stringify(items), worker, now).lastInsertRowid);
  return {
    notify: {
      kind: 'stockout', stockoutId, slipSeq: slip.seq, neSlipNo: slip.ne_slip_no, siteOrderNo: slip.site_order_no || null,
      recipientName: slip.recipient_name || null, folder: batch.folder_name, items,
    },
  };
}

/**
 * ❌ ピッキング時の「どこにもない」(pk_shortage_allocations kind='none') がある未処理の伝票を、
 * 再ピック依頼を経ずに『出荷保留 (在庫なし)』で閉じる (PR-2)。
 * 条件: 未処理 (pending)・この伝票に生きた再ピック依頼が無い・未送信の候補が無い・配賦 none が1つ以上
 */
function ackPickingStockout(db, batch, slip, worker, now) {
  const live = db.prepare(`SELECT COUNT(*) c FROM pk_pack_tasks WHERE batch_id=? AND slip_seq=? AND kind='repick'
    AND status IN ('requested','claimed','fulfilled','unavailable')`).get(batch.id, slip.seq).c;
  if (live > 0) throw new PackError(409, 'repick_in_progress', 'この伝票には再ピック依頼があります (届いたら「受け取った」・出てきたら「見つかった」)');
  const cands = db.prepare(`SELECT COUNT(*) c FROM pk_pack_incidents
    WHERE batch_id=? AND slip_seq=? AND status='candidate' AND kind IN ('shortage','wrong_item')`).get(batch.id, slip.seq).c;
  if (cands > 0) throw new PackError(409, 'candidates_remain', '未送信の不足候補があります。先に「ピッキングへ送信」するか、見つかったなら取り下げてください');
  let none = [];
  try {
    none = db.prepare(`
      SELECT a.sku, a.qty, a.created_at, l.product_name,
        (SELECT e.worker FROM pk_events e WHERE e.batch_id = a.batch_id AND e.line_seq = a.line_seq AND e.event = 'shortage'
           ORDER BY e.id DESC LIMIT 1) AS picker
      FROM pk_shortage_allocations a
      JOIN pk_batches pb ON pb.id = a.batch_id
      LEFT JOIN pk_lines l ON l.batch_id = a.batch_id AND l.seq = a.line_seq
      WHERE pb.tb_no = ? AND a.ne_slip_no = ? AND a.kind = 'none'
      ORDER BY a.id`).all(batch.tb_key, slip.ne_slip_no);
  } catch { none = []; }
  if (none.length === 0) throw new PackError(409, 'no_stockout', '3階からの「在庫なし」の報告がありません');
  const items = none.map((a) => ({ sku: a.sku, name: a.product_name, qty: a.qty, delivered: 0, claimedBy: a.picker, at: a.created_at }));
  return closeSlipAsStockout(db, batch, slip, items, worker, now);
}

/**
 * 不足 (①) / 余り (②) / 品違い / 見つかった / 受領 のイベント本体。
 * applyEvent から呼ばれる (op_id 冪等・トランザクションは呼び出し側)。
 * @returns {{notify?: object}} router が fail-soft でGChat通知に使う情報
 */
export function applyTaskEvent(db, batch, event, { slipSeq, sku, actualSku, actualName, qty }, worker, now) {
  const batchId = batch.id;
  if (event === 'shortage' || event === 'wrong_item') {
    if (!Number.isInteger(slipSeq) || slipSeq < 1) throw new PackError(400, 'bad_slip_seq', 'slip_seq が不正です');
    const line = slipLineOf(db, batchId, slipSeq, sku);
    if (!line) throw new PackError(404, 'sku_not_in_slip', `伝票 ${slipSeq} にSKU ${sku} がありません`);
    const q = qty == null ? line.qty : Number(qty);
    if (!Number.isInteger(q) || q < 1 || q > line.qty) {
      throw new PackError(400, 'bad_qty', `数量は1〜${line.qty}で指定してください`);
    }
    const slip = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, slipSeq);
    // 梱包機ライン (紙作業) は終了画面が無く不足を1件ずつ即送信するため、既に保留 (repick) の伝票にも
    // 追加の不足 (別SKU) を記録できる (Codex R1 High: 同一伝票で2品目が不足すると2件目を出せなかった)。
    // 手梱包は従来どおり (保留中は画面側でまとめて扱う)
    const lineKind = lineKindOf(batchHikiateClass(db, batch));
    const heldRepick = slip && slip.status === 'held' && slip.hold_reason === 'repick';
    if (!slip || !(slip.status === 'pending' || slip.status === 'done' || (lineKind && heldRepick))) {
      throw new PackError(409, 'not_pending', `伝票 ${slipSeq} は保留/取消済みです`);
    }
    // 同じ伝票×SKU の生きた依頼 (在庫なし報告を含む) があれば二重依頼として拒否 — 手梱包も同じ
    // (候補だけ作れて送信時に初めて 409 になる「送れない候補」を作らない — Codex R1 Medium)
    const dupTask = db.prepare(`SELECT id, status FROM pk_pack_tasks
      WHERE batch_id=? AND slip_seq=? AND kind='repick' AND LOWER(TRIM(sku))=LOWER(TRIM(?)) AND status IN ('requested','claimed','fulfilled','unavailable')`)
      .get(batchId, slipSeq, line.sku);
    if (dupTask?.status === 'unavailable') {
      throw new PackError(409, 'stockout_reported', `伝票 ${slipSeq} の ${line.sku} は3階から「在庫なし」の報告があります。「在庫なしを確認」で閉じてください`);
    }
    if (dupTask) throw new PackError(409, 'dup_task', `伝票 ${slipSeq} の ${line.sku} は既に依頼済みです`);
    // ❌ ピッキング時に「どこにもない」と配賦された商品 (PR-2): 再依頼しても3階に無い → 「在庫なしを確認」へ
    // (9/5 出荷_15 #94: 3階が「どこにもない」と記録した商品を1階が再依頼し、🔴バッチが立った)
    try {
      const noneAlloc = db.prepare(`SELECT 1 FROM pk_shortage_allocations a JOIN pk_batches pb ON pb.id = a.batch_id
        WHERE pb.tb_no = ? AND a.ne_slip_no = ? AND a.kind = 'none' AND LOWER(TRIM(a.sku)) = LOWER(TRIM(?)) LIMIT 1`)
        .get(batch.tb_key, slip.ne_slip_no, line.sku);
      if (noneAlloc && event === 'shortage') {
        throw new PackError(409, 'stockout_reported',
          `伝票 ${slipSeq} の ${line.sku} は3階が「どのロケにもない」と記録しています。「在庫なしを確認」で出荷保留にしてください`);
      }
    } catch (e) { if (e instanceof PackError) throw e; /* picking無効環境 */ }
    if (lineKind) {
      // ラインは同一伝票に複数SKUの候補を持てるので、同じSKUの未送信候補もタップの重複として拒否
      const dupInc = db.prepare(`SELECT id FROM pk_pack_incidents
        WHERE batch_id=? AND slip_seq=? AND LOWER(TRIM(sku))=LOWER(TRIM(?)) AND kind IN ('shortage','wrong_item') AND status='candidate'`)
        .get(batchId, slipSeq, line.sku);
      if (dupInc) throw new PackError(409, 'dup_task', `伝票 ${slipSeq} の ${line.sku} は既に依頼済みです`);
    }
    // ⭐候補方式 (中原さん指示 2026-08-17): 記録時はタスクを発行しない。
    // 「途中で足りないと思っても最後までやると山の下から出てくる」ため、
    // ピッキングへの送信は梱包終了時の候補一覧からまとめて行う (dispatchIncident)
    if (event === 'shortage') {
      insertIncident(db, batch, { slipSeq, kind: 'shortage', sku: line.sku, qty: q }, worker, now);
      db.prepare(`UPDATE pk_pack_slips SET status='held', hold_reason='repick', done_at=NULL WHERE id=?`).run(slip.id);
      return {};
    }
    // wrong_item: 期待SKU+実SKUのペアを候補として保持 (要件§5.6)。
    // 実SKUは記録時に在庫検索で特定 (router で形式+実在検証・名前はサーバー由来 — 2026-08-23)
    const act = String(actualSku || '').trim();
    if (!act) throw new PackError(400, 'bad_actual_sku', '実際に入っていた商品を検索で特定してください');
    if (act.length > 80) throw new PackError(400, 'bad_actual_sku', 'SKUが不正です');
    const aName = actualName ? String(actualName).trim().slice(0, 120) || null : null;
    insertIncident(db, batch, { slipSeq, kind: 'wrong_item', sku: line.sku, actualSku: act, actualName: aName, qty: q }, worker, now);
    db.prepare(`UPDATE pk_pack_slips SET status='held', hold_reason='repick', done_at=NULL WHERE id=?`).run(slip.id);
    return {};
  }

  if (event === 'excess') {
    const s = String(sku || '').trim();
    const q = Number(qty);
    if (!s) throw new PackError(400, 'bad_sku', 'SKUを指定してください');
    if (!Number.isInteger(q) || q < 1 || q > 999) throw new PackError(400, 'bad_qty', '数量が不正です');
    insertIncident(db, batch, { slipSeq: slipSeq ?? null, kind: 'excess', sku: s, qty: q }, worker, now);
    return {};
  }

  if (event === 'stockout_ack') {
    // 🚫 在庫なしを確認 (Q1 決定 2026-09-05 = 案a): 3階が「在庫なし」と報告した保留伝票を、
    // 1階の梱包者が確認して『出荷保留 (在庫なし)』として閉じる。事務へ通知し (router)、バッチは完了できる。
    // 以前は unavailable を読む画面が無く、伝票が「⏳再ピック対応待ち」のまま何日も残った (出荷_01 8/30・出荷_21 9/1)
    if (!Number.isInteger(slipSeq) || slipSeq < 1) throw new PackError(400, 'bad_slip_seq', 'slip_seq が不正です');
    const slip = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, slipSeq);
    // ❌ ピッキング時の「どこにもない」(配賦) がある未処理の伝票 (PR-2): 再ピック依頼を経ずにそのまま出荷保留にできる
    // (以前は3階が「どこにもない」と言った商品を1階が再依頼し、3階が二度呼ばれていた — 9/5 出荷_15 #94)
    if (slip && slip.status === 'pending') return ackPickingStockout(db, batch, slip, worker, now);
    if (!slip || slip.status !== 'held' || slip.hold_reason !== 'repick') {
      throw new PackError(409, 'not_held', `伝票 ${slipSeq} は再ピック保留ではありません`);
    }
    if (lineKindOf(batchHikiateClass(db, batch))) {
      // ライン完了件数の確定後に伝票を減らすと、ライン記録 (出荷完了 n 件) と出荷対象がずれる (Codex R1 Medium)
      const run = db.prepare("SELECT final_count FROM pk_pack_line_runs WHERE batch_id=? AND phase='run'").get(batchId);
      if (run?.final_count != null) {
        throw new PackError(409, 'line_already_finalized', 'ライン完了件数の確定後です。先に「直前の記録を取り消す」で完了記録を戻してください');
      }
    }
    const tasks = db.prepare(`SELECT * FROM pk_pack_tasks WHERE batch_id=? AND slip_seq=? AND kind='repick'
      AND status IN ('requested','claimed','fulfilled','unavailable') ORDER BY id`).all(batchId, slipSeq);
    if (tasks.some((t) => t.status === 'requested' || t.status === 'claimed')) {
      throw new PackError(409, 'repick_in_progress',
        'まだ再ピック中の商品があります (届いたら「受け取った」・手元で出てきたら「見つかった」)');
    }
    const cands = db.prepare(`SELECT COUNT(*) c FROM pk_pack_incidents
      WHERE batch_id=? AND slip_seq=? AND status='candidate' AND kind IN ('shortage','wrong_item')`).get(batchId, slipSeq).c;
    if (cands > 0) {
      throw new PackError(409, 'candidates_remain', '未送信の不足候補があります。先に「ピッキングへ送信」するか、見つかったなら取り下げてください');
    }
    const na = tasks.filter((t) => t.status === 'unavailable');
    if (na.length === 0) throw new PackError(409, 'no_stockout', '3階からの「在庫なし」の報告がありません');
    // 届いている分 (fulfilled) は受領扱い、在庫なし分は「確認済み」として閉じる (cancelled + close_reason='stockout')。
    // unavailable のまま残すと、後から fulfill できたり重複ガードに永久に掛かったりする (Codex R1 High)
    for (const t of tasks) {
      if (t.status === 'fulfilled') {
        db.prepare("UPDATE pk_pack_tasks SET status='received', updated_at=? WHERE id=?").run(now, t.id);
      } else if (t.status === 'unavailable') {
        db.prepare("UPDATE pk_pack_tasks SET status='cancelled', close_reason='stockout', updated_at=? WHERE id=?").run(now, t.id);
        resolveFloorAlertsByTask(t.id, 'stockout', db);
      }
    }
    const items = na.map((t) => ({
      sku: t.sku, name: t.product_name, qty: t.unavailable_qty ?? t.req_qty, delivered: t.fulfilled_qty || 0,
      claimedBy: t.claimed_by || t.requested_by, at: t.updated_at,
    }));
    return closeSlipAsStockout(db, batch, slip, items, worker, now);
  }

  if (event === 'found' || event === 'receive') {
    // 保留 (repick) の解除。found=後から出てきた (依頼取下げ+候補も取下げ) / receive=再ピック品を受領
    if (!Number.isInteger(slipSeq) || slipSeq < 1) throw new PackError(400, 'bad_slip_seq', 'slip_seq が不正です');
    const slip = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, slipSeq);
    if (!slip || slip.status !== 'held' || slip.hold_reason !== 'repick') {
      throw new PackError(409, 'not_held', `伝票 ${slipSeq} は再ピック保留ではありません`);
    }
    const open = db.prepare(`
      SELECT * FROM pk_pack_tasks WHERE batch_id=? AND slip_seq=? AND kind='repick'
        AND status IN ('requested','claimed','fulfilled')
    `).all(batchId, slipSeq);
    // 3階が「在庫なし」にした依頼 (未確認)。found はこれも取り下げる = 在庫なし報告の後に手元で出てきた
    const unavailable = db.prepare(`SELECT * FROM pk_pack_tasks WHERE batch_id=? AND slip_seq=? AND kind='repick'
      AND status='unavailable'`).all(batchId, slipSeq);
    if (event === 'found') {
      // 商品が出てきた: この伝票の候補 (不足/品違い) を取り下げる。
      // 送信済みなら repick タスクも取消 (棚戻しタスクは残す=間違い品が実在する場合)。
      // sku 指定あり = その商品だけ (ラインは同一伝票に複数SKUの依頼を許すため — Codex R2)。
      // 他に候補/未完了タスクが残っていれば保留は解除しない。
      // unavailable も対象 (Codex R1 High: 対象外だと伝票だけ pending に戻り、在庫なし報告と重複ガードが残る)
      const actionable = [...open, ...unavailable];
      const skuN = sku ? normSku(sku) : null;
      const targets = skuN ? actionable.filter((t) => normSku(t.sku) === skuN) : actionable;
      for (const t of targets) {
        db.prepare("UPDATE pk_pack_tasks SET status='cancelled', updated_at=? WHERE id=?").run(now, t.id);
        if (t.status === 'unavailable') resolveFloorAlertsByTask(t.id, 'stockout', db);
      }
      db.prepare(`
        UPDATE pk_pack_incidents SET status='withdrawn', updated_at=?
        WHERE batch_id=? AND slip_seq=? AND status='candidate' AND kind IN ('shortage','wrong_item')
          AND (? IS NULL OR LOWER(TRIM(sku)) = ?)
      `).run(now, batchId, slipSeq, skuN, skuN);
      // 送信済み (confirmed) も、いま取り消した依頼の分だけ取り下げる (Q5 決定 2026-09-05)。
      // ラインは候補を経ず即送信するため「送ったあとに見つかった」が confirmed のまま残り、
      // ピッカーのミス率に数えられていた。⭐対象は取り消したタスクに紐づく候補だけ —
      // 既に受領済み (本当に足りなかった) の記録まで消さない
      const incIds = targets.map((x) => x.incident_id).filter((x) => x != null);
      if (incIds.length > 0) {
        db.prepare(`UPDATE pk_pack_incidents SET status='withdrawn', updated_at=?
          WHERE id IN (${incIds.map(() => '?').join(',')}) AND status='confirmed'`).run(now, ...incIds);
      }
      if (skuN) {
        const remainCands = db.prepare(`SELECT COUNT(*) c FROM pk_pack_incidents
          WHERE batch_id=? AND slip_seq=? AND status='candidate' AND kind IN ('shortage','wrong_item')`).get(batchId, slipSeq).c;
        if (actionable.length - targets.length > 0 || remainCands > 0) return {};   // 他の商品の依頼が残る → 保留のまま
      }
    } else {
      // 3階が「在庫なし」にした商品があれば受領では解除しない — 「在庫なしを確認」で閉じる (届いた分はその中で受領扱い)
      if (unavailable.length > 0) {
        throw new PackError(409, 'stockout_reported',
          '3階から「在庫なし」の報告がある商品があります。「在庫なしを確認」で伝票を閉じてください (届いた分はその中で受領扱いになります)');
      }
      // 未送信の候補 (不足/品違い) が残っていれば受領できない — その商品はまだ依頼すら出ていない
      // (Codex R2 High: 2品目の候補が未送信のまま1品目の受領で保留が解けていた)
      const cands = db.prepare(`SELECT COUNT(*) c FROM pk_pack_incidents
        WHERE batch_id=? AND slip_seq=? AND status='candidate' AND kind IN ('shortage','wrong_item')`).get(batchId, slipSeq).c;
      if (cands > 0) {
        throw new PackError(409, 'repick_not_ready',
          '未送信の不足候補があります。先に「ピッキングへ送信」するか、見つかったなら取り下げてください');
      }
      // receive は状態機械どおり fulfilled のみ (Codex Phase2 high: ピッカー未対応のまま
      // 保留解除できると、商品が無いのに梱包を再開してしまう)。
      // 依頼未送信 (候補のみ) の段階では受領は成立しない — 「見つかった」を使う
      if (open.length === 0) {
        throw new PackError(409, 'repick_not_ready',
          '再ピック依頼がまだ送信されていません (商品が見つかった場合は「見つかった」を使ってください)');
      }
      const notReady = open.filter((t) => t.status !== 'fulfilled');
      if (notReady.length > 0) {
        throw new PackError(409, 'repick_not_ready',
          '再ピックがまだ完了していません (商品が手元で見つかった場合は「見つかった」を使ってください)');
      }
      for (const t of open) {
        db.prepare("UPDATE pk_pack_tasks SET status='received', updated_at=? WHERE id=?").run(now, t.id);
      }
    }
    // 保留解除と同時に伝票の表示時刻を打ち直す = 待っていた時間をこの伝票の梱包時間に入れない
    // (受領後は画面がこの伝票へ自動ジャンプする — 2026-08-23)
    db.prepare("UPDATE pk_pack_slips SET status='pending', hold_reason=NULL, shown_at=? WHERE id=?").run(now, slip.id);
    // 🕒 の欠品バナー (受注単位) はここで片付く
    resolveFloorAlertsByRef(`alloc:%:${slip.ne_slip_no}`, { like: true, dbh: db });
    return {};
  }
  throw new PackError(400, 'bad_event', `不明なタスクイベント: ${event}`);
}

// ─── タスクの実行側操作 (picking のキュー画面から呼ばれる更新API) ───

/**
 * バッチ一覧のカード用: 欠品まわりの件数 (例外処理監査 PR-2・U-14「どのバッチが待ちを抱えているか一覧で分からない」)。
 *   stockoutWait = 1階の「在庫なしを確認」待ち (未処理の伝票に ❌ / 保留伝票に 3階の unavailable)
 *   repickWait   = 3階の再ピック待ち (保留 repick で在庫なし報告なし)
 *   later        = 🕒 後で取りに行く (保留のうち依頼が動いているもの) — repickWait に含まれる件数の内訳
 *   closed       = 出荷保留 (在庫なし) で閉じた伝票
 */
export function shortageSummaryFor(batch) {
  const db = getDB();
  const out = { stockoutWait: 0, repickWait: 0, closed: 0 };
  try {
    out.closed = db.prepare(`SELECT COUNT(*) c FROM pk_pack_slips WHERE batch_id=? AND status='cancelled' AND hold_reason='stockout'`).get(batch.id).c;
    const unavailableSeqs = new Set(db.prepare(`SELECT DISTINCT slip_seq FROM pk_pack_tasks WHERE batch_id=? AND kind='repick' AND status='unavailable' AND slip_seq IS NOT NULL`).all(batch.id).map((r) => r.slip_seq));
    const liveSeqs = new Set(db.prepare(`SELECT DISTINCT slip_seq FROM pk_pack_tasks WHERE batch_id=? AND kind='repick' AND status IN ('requested','claimed','fulfilled','unavailable') AND slip_seq IS NOT NULL`).all(batch.id).map((r) => r.slip_seq));
    const noneSeqs = new Set(db.prepare(`SELECT s.seq FROM pk_shortage_allocations a JOIN pk_batches pb ON pb.id = a.batch_id
      JOIN pk_pack_slips s ON s.batch_id = ? AND s.ne_slip_no = a.ne_slip_no
      WHERE pb.tb_no = ? AND a.kind = 'none' AND s.status = 'pending'`).all(batch.id, batch.tb_key).map((r) => r.seq));
    for (const seq of noneSeqs) if (!liveSeqs.has(seq)) out.stockoutWait++;
    for (const s of db.prepare(`SELECT seq FROM pk_pack_slips WHERE batch_id=? AND status='held' AND hold_reason='repick'`).all(batch.id)) {
      if (unavailableSeqs.has(s.seq)) out.stockoutWait++; else out.repickWait++;
    }
  } catch { /* picking無効環境 */ }
  return out;
}

/** 未完了タスク。kind を指定するとその種別だけ (棚戻し専用キュー用 — 再ピックは 🔴バッチに一本化)。 */
export function listOpenTasks({ kind = null } = {}) {
  return getDB().prepare(`
    SELECT t.*, b.folder_name AS batch_folder FROM pk_pack_tasks t
    JOIN pk_pack_batches b ON b.id = t.batch_id
    WHERE t.status IN ('requested','claimed') AND (? IS NULL OR t.kind = ?)
    ORDER BY t.id
  `).all(kind, kind);
}

/**
 * 再ピック完了 (fulfilled) で梱包側の受領待ちのタスク — 梱包ヘッダーの受領バナー用 (2026-08-23)。
 * 旧: picking が完了時に「通知バナー (OK=既読のみ)」を出していたが、受領と混同されるため
 * タスク状態そのものから導出する (受領すれば自然に消える)。
 */
export function listRepickReady() {
  try {
    const rows = getDB().prepare(`
      SELECT t.id, t.batch_id, t.slip_seq, t.sku, t.product_name, t.req_qty, t.requested_by, t.claimed_by,
             t.status, t.updated_at, b.folder_name
      FROM pk_pack_tasks t JOIN pk_pack_batches b ON b.id = t.batch_id
      WHERE t.kind = 'repick' AND t.slip_seq IS NOT NULL
        AND t.status IN ('requested', 'claimed', 'fulfilled') AND b.status IN ('packing', 'paused')
      ORDER BY t.updated_at DESC, t.id DESC
    `).all();
    // 伝票単位に集約し、未解決タスクが全部 fulfilled の伝票だけ (受領処理の条件と同じ — Codex)
    const bySlip = new Map();
    for (const r of rows) {
      const k = `${r.batch_id}:${r.slip_seq}`;
      if (!bySlip.has(k)) {
        bySlip.set(k, { batch_id: r.batch_id, slip_seq: r.slip_seq, folder_name: r.folder_name,
          requested_by: r.requested_by, updated_at: r.updated_at, tasks: [], ready: true });
      }
      const g = bySlip.get(k);
      g.tasks.push({ id: r.id, sku: r.sku, product_name: r.product_name, req_qty: r.req_qty, claimed_by: r.claimed_by, status: r.status });
      if (r.status !== 'fulfilled') g.ready = false;
    }
    return [...bySlip.values()].filter((g) => g.ready).map(({ ready, ...g }) => g);
  } catch { return []; }
}

export function countOpenTasks({ kind = null } = {}) {
  try {
    return getDB().prepare(
      "SELECT COUNT(*) c FROM pk_pack_tasks WHERE status IN ('requested','claimed') AND (? IS NULL OR kind = ?)"
    ).get(kind, kind).c;
  } catch { return 0; }
}

/**
 * タスク操作 (claim / fulfill / unavailable / cancel)。
 * fulfill は kind で終端が変わる: repick→fulfilled (梱包者の受領待ち) / return→returned (完了)
 * @returns 更新後のタスク行 (+ unavailable 時は _notifyUnavailable)
 */
export function applyTaskAction(taskId, action, worker, { unavailableQty = null, fulfilledQty = null } = {}) {
  const t = TASK_TRANSITIONS[action];
  if (!t) throw new PackError(400, 'bad_action', `不明な操作: ${action}`);
  if (!worker) throw new PackError(400, 'no_worker', '作業者を選択してください');
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    const task = db.prepare('SELECT * FROM pk_pack_tasks WHERE id = ?').get(taskId);
    if (!task) throw new PackError(404, 'not_found', 'タスクが見つかりません');
    if (!t.from.includes(task.status)) {
      throw new PackError(409, 'bad_transition', `${task.status} から ${action} はできません`);
    }
    const to = action === 'fulfill' ? (task.kind === 'repick' ? 'fulfilled' : 'returned') : t.to;
    const sets = ['status=?', 'claimed_by=COALESCE(claimed_by, ?)', 'updated_at=?'];
    const params = [to, worker, now];
    // 部分確保の内訳 (5個中2個は他ロケで確保・3個は在庫なし) を保存する (Codex R1 High: 通知と現物が食い違う)
    if (action === 'unavailable') {
      sets.push('unavailable_qty=?', 'fulfilled_qty=?');
      params.push(unavailableQty ?? task.req_qty, fulfilledQty ?? 0);
    } else if (action === 'resume' || action === 'reopen') {
      sets.push('unavailable_qty=NULL', 'fulfilled_qty=NULL');
    } else if (action === 'fulfill') {
      sets.push('unavailable_qty=NULL', 'fulfilled_qty=?');
      params.push(task.req_qty);
    }
    db.prepare(`UPDATE pk_pack_tasks SET ${sets.join(', ')} WHERE id=?`).run(...params, taskId);
    const updated = db.prepare('SELECT * FROM pk_pack_tasks WHERE id = ?').get(taskId);
    if (action === 'unavailable') {
      // 在庫なし → 1階が「在庫なしを確認」して閉じる (Q1 決定)。GChat は呼び出し側 (router) が送る
      updated._notifyUnavailable = true;
    }
    return updated;
  })();
}

/** タスク1行 (参照用。picking の同期・API の種別チェックで使う)。 */
export function getTask(taskId) {
  return getDB().prepare('SELECT * FROM pk_pack_tasks WHERE id = ?').get(taskId) || null;
}

// ─── 🚫 出荷保留 (在庫なし) 通知の outbox (pk_pack_stockouts) ───
// router (確認直後) とポーラー (再送) が同じ行を同時に送らないよう、送信前に claimed_at で「送信中」の印を付ける
// (Codex R2 Medium)。印は10分で失効 = 送信中に落ちても再送される。webhook に冪等キーが無いので
// 「成功→印を付ける前に落ちた」だけは重複し得る (at-least-once)

/**
 * 送信権を取る。true = この呼び出し側が送ってよい / false = 未通知でない or 他方が送信中。
 * ⚠ 時刻列は ISO ('…T…Z') なので datetime() で正規化してから比較する — 文字列のままだと 'T' > ' ' で
 *   同日中は失効判定が常に偽になり、送信中に落ちた行が翌日まで再送されない (Codex R3)
 */
export function claimStockoutNotify(id) {
  return getDB().prepare(`UPDATE pk_pack_stockouts SET claimed_at=?
    WHERE id=? AND notified_at IS NULL AND (claimed_at IS NULL OR datetime(claimed_at) < datetime('now', '-10 minutes'))`)
    .run(utcNow(), id).changes === 1;
}

/** 送信結果を記録する。sent=true で通知済み。false は webhook 未設定、error は失敗理由。印 (claimed_at) は外す。 */
export function markStockoutNotify(id, sent, error = null) {
  const db = getDB();
  if (sent) {
    db.prepare('UPDATE pk_pack_stockouts SET notified_at=?, notify_error=NULL, claimed_at=NULL WHERE id=?').run(utcNow(), id);
  } else {
    db.prepare('UPDATE pk_pack_stockouts SET notify_error=?, claimed_at=NULL WHERE id=?')
      .run(String(error || 'webhook未設定').slice(0, 200), id);
  }
}

/** 未通知の outbox 行 (送信中でないもの)。期間で切らない = 何日経っても送れたときに届く (Codex R2 High)。 */
export function listPendingStockoutNotifies(limit = 3) {
  return getDB().prepare(`SELECT * FROM pk_pack_stockouts
    WHERE notified_at IS NULL AND (claimed_at IS NULL OR datetime(claimed_at) < datetime('now', '-10 minutes'))
    ORDER BY id LIMIT ?`).all(limit);
}

/** 2日以上未通知のまま滞留している件数 (監視用ログ)。 */
export function countStaleStockoutNotifies() {
  return getDB().prepare(`SELECT COUNT(*) c FROM pk_pack_stockouts
    WHERE notified_at IS NULL AND datetime(created_at) < datetime('now', '-2 days')`).get().c;
}

// ─── ③ 候補の確定 / 取下げ (梱包完了サマリから) ───

export function listIncidents(batchId, status = null) {
  const db = getDB();
  return status
    ? db.prepare('SELECT * FROM pk_pack_incidents WHERE batch_id=? AND status=? ORDER BY id').all(batchId, status)
    : db.prepare('SELECT * FROM pk_pack_incidents WHERE batch_id=? ORDER BY id').all(batchId);
}

/**
 * ミス候補の確定/取下げ (⭐候補方式 — 中原さん指示 2026-08-17)。
 *   confirm = 「ピッキングへ送信」: ミス確定 (pk_batches.worker へ帰責) と同時に
 *             再ピック/棚戻しタスクを発行する。梱包を一通り終えた時点 (未処理ゼロ) でのみ可
 *             — 「途中で足りないと思っても最後までやると出てくる」ため途中送信を防ぐ (要件§5.6)
 *   withdraw = 取下げ (出てきた・誤記録)。作業中でも可。対象伝票に他の候補が無ければ保留も解除
 * @returns {{...inc, dispatchedTasks: [{kind, sku, name, qty, folder, slipSeq}]}}
 */
/** 商品コード→ロジザード商品名 (picking pk_lines 参照のみ・最新行)。無ければ null。 */
function logizardNameOf(db, sku) {
  try {
    // 通常取込行のみ参照 (Codex: 再ピックバッチ自身の行を拾うと、フォールバックした
    // 納品書名が次回から「ロジザード名」と誤認されるループになる)
    return db.prepare(`
      SELECT l.product_name FROM pk_lines l
      JOIN pk_batches b ON b.id = l.batch_id
      WHERE l.sku = ? AND l.product_name IS NOT NULL AND b.origin != 'repick'
      ORDER BY l.rowid DESC LIMIT 1
    `).get(sku)?.product_name ?? null;
  } catch {
    return null;   // picking未初期化環境
  }
}

export function resolveIncident(incidentId, decision, actor, expectBatchId = null, { actualSku = null, actualName = null } = {}) {
  if (!['confirm', 'withdraw'].includes(decision)) {
    throw new PackError(400, 'bad_decision', 'decision が不正です');
  }
  const db = getDB();
  const now = utcNow();
  return db.transaction(() => {
    const inc = db.prepare('SELECT * FROM pk_pack_incidents WHERE id = ?').get(incidentId);
    if (!inc) throw new PackError(404, 'not_found', '候補が見つかりません');
    if (expectBatchId != null && inc.batch_id !== expectBatchId) {
      throw new PackError(404, 'not_found', '候補が見つかりません (バッチ不一致)');
    }
    if (inc.status !== 'candidate') {
      // 現在の status を同梱 — 再送側は「要求した decision と同じ結果か」で成功/競合を見分ける (Codex R5)
      const e = new PackError(409, 'already_resolved', `既に${inc.status === 'confirmed' ? '送信' : '取下げ'}済みです`);
      e.body = { status: inc.status };
      throw e;
    }
    const batch = db.prepare('SELECT * FROM pk_pack_batches WHERE id = ?').get(inc.batch_id);
    // 梱包機ライン (紙作業) は伝票の完了を追わない → 「一通り終えてから」の条件と担当者一致は課さない
    // (工程ごとに担当が替わる。作業者名は router で有効名を検証済み — 2026-08-31)
    // 緩和はライン画面が作る不足候補 (shortage) に限る (Codex R1: 余り/品違いまで誰でも確定できる範囲にしない)
    const lineShortage = !!lineKindOf(batchHikiateClass(db, batch)) && inc.kind === 'shortage';
    // 担当者チェック (Codex high: 任意の作業者名で他人のバッチの候補を送信・帰責できてしまう)
    if (!lineShortage && batch.worker && batch.worker !== actor) {
      throw new PackError(409, 'taken', `このバッチは ${batch.worker} が担当です`);
    }
    let attributed = null;
    const dispatchedTasks = [];
    if (decision === 'confirm') {
      const pendingCount = db.prepare(
        "SELECT COUNT(*) c FROM pk_pack_slips WHERE batch_id=? AND status='pending'"
      ).get(inc.batch_id).c;
      // 許可状態を明示: 完了済み or 「作業中かつ未処理ゼロ」(=終了画面)。paused等からは不可 (Codex medium)
      const allowed = lineShortage || batch.status === 'done' || (batch.status === 'packing' && pendingCount === 0);
      if (!allowed) {
        throw new PackError(409, 'batch_not_done',
          '送信は梱包を一通り終えてからできます (途中で出てくることがあるため)。先に残りの伝票を進めてください');
      }
      // 品違いは「実際に入っていた商品」を特定してから送信 (終了画面の商品検索で指定 —
      // 中原さん指示 2026-08-21)。特定作業はバッチ完了打刻の後のため梱包計測には入らない
      if (inc.kind === 'wrong_item') {
        const sku = String(actualSku ?? inc.actual_sku ?? '').trim();
        if (!sku) throw new PackError(400, 'actual_sku_required', '間違って入っていた商品を検索で特定してから送信してください');
        if (sku.length > 80) throw new PackError(400, 'bad_sku', 'SKUが不正です');
        if (sku !== inc.actual_sku || (actualName && actualName !== inc.actual_name)) {
          db.prepare('UPDATE pk_pack_incidents SET actual_sku=?, actual_name=COALESCE(?, actual_name), updated_at=? WHERE id=?')
            .run(sku, actualName ? String(actualName).slice(0, 120) : null, now, incidentId);
          inc.actual_sku = sku;
          if (actualName) inc.actual_name = String(actualName).slice(0, 120);
        }
      }
      try {
        attributed = batch?.pk_batch_id
          ? db.prepare('SELECT worker FROM pk_batches WHERE id = ?').get(batch.pk_batch_id)?.worker ?? null
          : null;
      } catch { attributed = null; }
      // 種別に応じてタスクを発行 (ここが「ピッキングへの送信」)。
      // 商品名は商品コード→ロジザード商品名 (picking pk_lines参照 = 通常ピッキングと同じ表記)。
      // NEの印字商品名はセット表記 (「10本入り【1個】…」) で個数が誤読されるため使わない
      // (中原さん指示 2026-08-21)。picking側に無いSKUのみ納品書の名前へフォールバック
      const name = logizardNameOf(db, inc.sku)
        ?? (inc.slip_seq != null
          ? (slipLineOf(db, inc.batch_id, inc.slip_seq, inc.sku)?.print_name
            ?? slipLineOf(db, inc.batch_id, inc.slip_seq, inc.sku)?.product_name ?? null)
          : null);
      if (inc.kind === 'shortage' || inc.kind === 'wrong_item') {
        insertTask(db, batch, { slipSeq: inc.slip_seq, kind: 'repick', sku: inc.sku, productName: name, qty: inc.qty, incidentId: inc.id }, actor, now);
        dispatchedTasks.push({ kind: 'repick', sku: inc.sku, name, qty: inc.qty, folder: batch.folder_name, slipSeq: inc.slip_seq });
      }
      if (inc.kind === 'wrong_item') {
        const aName = (actualName ? String(actualName).slice(0, 120) : null)
          ?? inc.actual_name ?? logizardNameOf(db, inc.actual_sku);
        insertTask(db, batch, { slipSeq: inc.slip_seq, kind: 'return', sku: inc.actual_sku, productName: aName, qty: inc.qty, incidentId: inc.id }, actor, now);
        dispatchedTasks.push({ kind: 'return', sku: inc.actual_sku, name: aName, qty: inc.qty, folder: batch.folder_name, slipSeq: inc.slip_seq });
      }
      if (inc.kind === 'excess') {
        insertTask(db, batch, { slipSeq: inc.slip_seq, kind: 'return', sku: inc.sku, productName: null, qty: inc.qty, incidentId: inc.id }, actor, now);
        dispatchedTasks.push({ kind: 'return', sku: inc.sku, qty: inc.qty, folder: batch.folder_name, slipSeq: inc.slip_seq });
      }
    }
    db.prepare(`
      UPDATE pk_pack_incidents SET status=?, attributed_worker=?, confirmed_by=?, updated_at=? WHERE id=?
    `).run(decision === 'confirm' ? 'confirmed' : 'withdrawn', attributed, actor, now, incidentId);
    if (decision === 'withdraw' && inc.slip_seq != null) {
      // 取下げで、その伝票に他の候補も未解決の再ピックタスクも無ければ保留を解除して梱包に戻す
      // (送信済みの再ピックが残っていれば現物未受領 — 受領まで保留のまま。Codex)
      const remain = db.prepare(`
        SELECT COUNT(*) c FROM pk_pack_incidents
        WHERE batch_id=? AND slip_seq=? AND status='candidate' AND kind IN ('shortage','wrong_item') AND id != ?
      `).get(inc.batch_id, inc.slip_seq, inc.id).c;
      const openRepick = db.prepare(`
        SELECT COUNT(*) c FROM pk_pack_tasks
        WHERE batch_id=? AND slip_seq=? AND kind='repick' AND status IN ('requested','claimed','fulfilled')
      `).get(inc.batch_id, inc.slip_seq).c;
      if (remain === 0 && openRepick === 0) {
        // 保留解除 = 梱包再開。表示時刻も打ち直す (待ちをこの伝票の時間に入れない)
        db.prepare(`
          UPDATE pk_pack_slips SET status='pending', hold_reason=NULL, shown_at=?
          WHERE batch_id=? AND seq=? AND status='held' AND hold_reason='repick'
        `).run(now, inc.batch_id, inc.slip_seq);
      }
    }
    // 取下げで伝票が pending に戻ると再ピック待ち区間が終わる (applyEvent 外の状態変更 — Codex)
    syncBlockedState(db, inc.batch_id, now);
    return { ...inc, status: decision === 'confirm' ? 'confirmed' : 'withdrawn', attributed_worker: attributed, dispatchedTasks };
  })();
}
