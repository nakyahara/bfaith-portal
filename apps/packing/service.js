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
import {
  getDB, getPackBatchByTbKey, utcNow, jstToday,
} from './db.js';

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
const SLIP_ONLY_EVENTS = ['start', 'next', 'jump', 'ship_change', 'reprint',
  'shortage', 'excess', 'wrong_item', 'found', 'receive'];
const WORK_EVENTS = ['start', 'next', 'takeover', 'pause', 'resume', 'cancel', 'undo', 'jump', 'ship_change',
  'reprint', 'shortage', 'excess', 'wrong_item', 'found', 'receive', ...LINE_EVENTS];

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

/** バッチの引当分類名 (picking pk_batches — 参照のみ・未初期化環境では null)。 */
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
 * @returns { total: 出荷累計 (手動含む), machine: 機械通過累計 (=出荷-手動。カウンタと比較する数) }
 */
export function lineDailyTotal(workDate, kind) {
  try {
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
    return { total, machine };
  } catch {
    return { total: 0, machine: 0 };   // pk_batches 未初期化環境
  }
}

// 中断理由 (picking と同じ最小セット)
export const PAUSE_REASONS = ['休憩', '他作業への応援', 'その他'];
// 完了取消の理由 (監査ログ必須 — 要件§5.1)
export const UNDO_REASONS = ['誤タップ', '入れ間違いの確認', 'その他'];
// ④ 配送方法変更の理由 (要件§5.7)
export const SHIP_CHANGE_REASONS = ['入らない', '資材が違う', 'その他'];
// ④ 提案できる配送方法 (固定リスト — 中原さん指定 2026-08-16)
export const SHIP_CHANGE_METHOD_OPTIONS = [
  '定形外', 'ネコポス', 'ゆうパケットパフ', 'レターパック', '宅急便50サイズ', '宅急便60サイズ',
];

/**
 * 端末の発生時刻を [floor, now] にクランプ (中断時間の計測用)。
 * floor = バッチ開始時刻等 (Codexレビュー medium: 端末時計のズレで開始前の時刻を
 * pause_started_at に入れると、実作業時間以上の中断秒が加算されて計測が壊れる)
 */
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
  const pending = full.filter((s) => s.status === 'pending');
  // ③ミス候補 (梱包完了サマリで確定/取下げする) と、保留伝票の未解決再ピックタスク状態
  let incidents = [];
  let repickBySlip = {};
  try {
    incidents = db.prepare(
      "SELECT * FROM pk_pack_incidents WHERE batch_id=? AND status='candidate' ORDER BY id"
    ).all(batchId);
    for (const t of db.prepare(
      "SELECT slip_seq, status FROM pk_pack_tasks WHERE batch_id=? AND kind='repick' AND status IN ('requested','claimed','fulfilled')"
    ).all(batchId)) {
      if (t.slip_seq != null) repickBySlip[t.slip_seq] = t.status;
    }
  } catch { /* v4未適用環境 */ }
  return {
    batch,
    slips: full,
    currentSeq: pending.length > 0 ? pending[0].seq : null,
    doneCount: full.filter((s) => s.status === 'done').length,
    incidents,
    repickBySlip,
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
export function applyEvent(batchId, { opId, event, slipSeq, clientAt, reason, jumped, proposedMethod, sku, actualSku, qty, finalCount, manualCount, excludedCount, note }, worker) {
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
    if (lineKind && SLIP_ONLY_EVENTS.includes(event)) {
      throw new PackError(409, 'line_batch', '梱包機バッチはライン管理画面から操作してください');
    }
    if (!lineKind && LINE_EVENTS.includes(event)) {
      throw new PackError(409, 'not_line_batch', '梱包機バッチではありません');
    }
    let taskNotify = null;   // ①②のGChat通知情報 (routerがfail-softで送る。replayでは再送しない)
    let switchedFrom = null; // ライン工程開始での担当交代 (payload に監査記録)

    if (event === 'takeover') {
      // 担当者の交代 (選び間違い・実際の引き継ぎ)。作業中・中断中のみ・記録が残る唯一の正規突破口
      if (batch.status !== 'packing' && batch.status !== 'paused') {
        throw new PackError(409, 'not_packing', `作業中ではないため交代できません (${batch.status})`);
      }
      if (batch.worker !== worker) {
        db.prepare('UPDATE pk_pack_batches SET worker=?, updated_at=? WHERE id=?').run(worker, now, batchId);
      }
      // ライン工程中の交代は「進行中の工程行」の担当も引き継ぐ (工程途中の操作判定は
      // 工程行の worker で行うため。完了済みの工程の担当記録は変えない)。
      // batch.worker が既に本人でも実行する — 工程行だけ食い違う不整合の修復口 (Codex medium)
      if (lineKind) {
        db.prepare(`UPDATE pk_pack_line_runs SET worker=?, updated_at=?
          WHERE batch_id=? AND started_at IS NOT NULL
            AND (finished_at IS NULL OR (phase='run' AND final_count IS NULL))
        `).run(worker, now, batchId);
      }
    } else if (event === 'pause') {
      // 中断: 中断時間は梱包時間から除外する。時刻は端末の発生時刻をクランプ採用
      // (オフラインで pause→resume を積むと受信時刻基準では中断が数秒になる — picking と同じ)
      if (batch.status !== 'packing') {
        throw new PackError(409, 'not_packing', `作業中ではないため中断できません (${batch.status})`);
      }
      requireOwner();
      const r = PAUSE_REASONS.includes(reason) ? reason : 'その他';
      db.prepare(`UPDATE pk_pack_batches SET status='paused', pause_started_at=?, pause_reason=?, updated_at=? WHERE id=?`)
        .run(clampedEventTime(clientAt, now, batch.started_at), r, now, batchId);
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
    } else if (event === 'cancel') {
      // 誤開始の取消: バッチを未着手に戻し、伝票の進捗も初期化する (pk_pack_events は残る)
      if (batch.status !== 'packing' && batch.status !== 'paused') {
        throw new PackError(409, 'not_packing', `作業中ではないため取消できません (${batch.status})`);
      }
      requireOwner();
      db.prepare(`
        UPDATE pk_pack_batches SET status='ready', worker=NULL, started_at=NULL, finished_at=NULL,
          paused_total_sec=0, pause_started_at=NULL, pause_reason=NULL, updated_at=? WHERE id=?
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
      db.prepare(`
        UPDATE pk_pack_tasks SET status='cancelled', updated_at=?
        WHERE batch_id=? AND status IN ('requested','claimed','fulfilled')
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
          db.prepare(`UPDATE pk_pack_line_runs SET finished_at=NULL, final_count=NULL, excluded_count=NULL, updated_at=?
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
      // 放置されず、事務の画面操作 (1工程) も不要。梱包者はそのまま「次へ」で完了してよい
      if (batch.status !== 'packing' && batch.status !== 'done') {
        throw new PackError(409, 'not_packing', `作業中ではありません (${batch.status})`);
      }
      requireOwner();
      const proposed = String(proposedMethod || '').trim();
      if (!SHIP_CHANGE_METHOD_OPTIONS.includes(proposed)) {
        throw new PackError(400, 'bad_method', '提案する配送方法を選択してください');
      }
      if (!SHIP_CHANGE_REASONS.includes(reason)) {
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
        proposed, reason, worker, now, now);
    } else if (event === 'reprint') {
      // 🖨 伝票再印刷依頼 (2026-08-21 中原さん指示): 記録+即時通知のみ。伝票状態は変えず、
      // 梱包画面にも痕跡を出さない (理由入力なし)。完了済み伝票でも押せる (配送変更と同様)
      if (batch.status !== 'packing' && batch.status !== 'done') {
        throw new PackError(409, 'not_packing', `作業中ではありません (${batch.status})`);
      }
      requireOwner();
      const slip = db.prepare('SELECT * FROM pk_pack_slips WHERE batch_id=? AND seq=?').get(batchId, slipSeq);
      if (!slip) throw new PackError(404, 'slip_not_found', `伝票 ${slipSeq} がありません`);
      const info = db.prepare(`
        INSERT INTO pk_pack_reprints (batch_id, slip_seq, ne_slip_no, site_order_no, folder_name,
          recipient_name, requested_by, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      `).run(batchId, slipSeq, slip.ne_slip_no, slip.site_order_no || null, batch.folder_name,
        slip.recipient_name || null, worker, now);
      var reprintId = Number(info.lastInsertRowid);   // eventResult後にresultへ載せる (var=分岐外参照)
    } else if (['shortage', 'excess', 'wrong_item', 'found', 'receive'].includes(event)) {
      // ①不足→再ピック / ②余り→棚戻し / 品違い / 見つかった / 受領 (要件§5.4〜5.6)。
      // 完了済み伝票への不足/品違いも許可 (誤タップ後の気づき) — バッチ完了済みなら再オープン
      if (batch.status !== 'packing' && batch.status !== 'done') {
        throw new PackError(409, 'not_packing', `作業中ではありません (${batch.status})`);
      }
      requireOwner();
      taskNotify = applyTaskEvent(db, batch, event, { slipSeq, sku, actualSku, qty }, worker, now).notify || null;
      if (batch.status === 'done' && ['shortage', 'wrong_item'].includes(event)) {
        db.prepare("UPDATE pk_pack_batches SET status='packing', finished_at=NULL, updated_at=? WHERE id=?")
          .run(now, batchId);
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
        db.prepare(`UPDATE pk_pack_line_runs SET finished_at=?, excluded_count=?, final_count=?, note=?, worker=?, updated_at=?
          WHERE id=?`).run(now, ex, run.planned_count - ex, note || run.note || null, worker, now, run.id);
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

    const result = eventResult(batchId);
    // eventResult はこのイベント行の INSERT より前に走るため、lastDoneSeq (イベント履歴由来) に
    // いま完了させた伝票が反映されない。next のときはここで上書きする
    if (event === 'next') result.lastDoneSeq = slipSeq;
    if (event === 'reprint' && typeof reprintId !== 'undefined') result.reprintId = reprintId;
    const payload = (clientAt || reason || jumped || proposedMethod || sku || actualSku || qty != null
        || finalCount != null || manualCount != null || excludedCount != null || note || switchedFrom)
      ? JSON.stringify({ clientAt, reason, jumped: jumped || undefined, proposedMethod: proposedMethod || undefined,
          sku: sku || undefined, actualSku: actualSku || undefined, qty: qty ?? undefined,
          finalCount: finalCount ?? undefined, manualCount: manualCount ?? undefined,
          excludedCount: excludedCount ?? undefined, note: note || undefined,
          switchedFrom: switchedFrom || undefined }) : null;
    db.prepare(`
      INSERT INTO pk_pack_events (op_id, batch_id, worker, event, slip_seq, payload_json, result_json, at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(opId, batchId, worker, event, slipSeq ?? null, payload, JSON.stringify(result), now);
    // taskNotify は保存済み結果に含めない = replay の再送で通知が重複しない
    return taskNotify ? { ...result, taskNotify } : result;
  })();
}

// ═══ 日次サマリ (⑦計測の集計・要件§5.11) ═══════════════════════════════

/**
 * 指定作業日のサマリ。
 *   - バッチ単位: 実働 = (finished - started) - 中断合計
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

  const activeSec = (b) => {
    if (!b.started_at || !b.finished_at) return null;
    return Math.max(0, Math.round((Date.parse(b.finished_at) - Date.parse(b.started_at)) / 1000) - (b.paused_total_sec || 0));
  };

  const done = batches.filter((b) => b.status === 'done');
  const total = {
    batchCount: batches.length,
    doneCount: done.length,
    slipCount: done.reduce((s, b) => s + b.slip_count, 0),
    qty: done.reduce((s, b) => s + b.total_qty, 0),
    activeSec: done.reduce((s, b) => s + (activeSec(b) || 0), 0),
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
  fulfill: { from: ['claimed'], to: null },   // to は kind で決まる (下で解決)
  unavailable: { from: ['requested', 'claimed'], to: 'unavailable' },
  cancel: { from: ['requested', 'claimed'], to: 'cancelled' },
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
  const dup = db.prepare(`
    SELECT id FROM pk_pack_tasks
    WHERE batch_id=? AND kind=? AND LOWER(TRIM(sku))=? AND (slip_seq IS ? OR slip_seq = ?)
      AND status IN ('requested','claimed','fulfilled')
  `).get(batch.id, kind, normSku(sku), slipSeq ?? null, slipSeq ?? -1);
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

function insertIncident(db, batch, { slipSeq, kind, sku, actualSku, qty }, worker, now) {
  return Number(db.prepare(`
    INSERT INTO pk_pack_incidents
      (batch_id, slip_seq, kind, sku, actual_sku, qty, status, detected_by, created_at, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, 'candidate', ?, ?, ?)
  `).run(batch.id, slipSeq ?? null, kind, sku, actualSku ?? null, qty, worker, now, now).lastInsertRowid);
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
 * 不足 (①) / 余り (②) / 品違い / 見つかった / 受領 のイベント本体。
 * applyEvent から呼ばれる (op_id 冪等・トランザクションは呼び出し側)。
 * @returns {{notify?: object}} router が fail-soft でGChat通知に使う情報
 */
export function applyTaskEvent(db, batch, event, { slipSeq, sku, actualSku, qty }, worker, now) {
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
    if (!slip || (slip.status !== 'pending' && slip.status !== 'done')) {
      throw new PackError(409, 'not_pending', `伝票 ${slipSeq} は保留/取消済みです`);
    }
    // ⭐候補方式 (中原さん指示 2026-08-17): 記録時はタスクを発行しない。
    // 「途中で足りないと思っても最後までやると山の下から出てくる」ため、
    // ピッキングへの送信は梱包終了時の候補一覧からまとめて行う (dispatchIncident)
    if (event === 'shortage') {
      insertIncident(db, batch, { slipSeq, kind: 'shortage', sku: line.sku, qty: q }, worker, now);
      db.prepare(`UPDATE pk_pack_slips SET status='held', hold_reason='repick', done_at=NULL WHERE id=?`).run(slip.id);
      return {};
    }
    // wrong_item: 期待SKU+実SKUのペアを候補として保持 (要件§5.6)
    const act = String(actualSku || '').trim();
    if (!act) throw new PackError(400, 'bad_actual_sku', '実際に入っていた商品 (SKU) を指定してください');
    insertIncident(db, batch, { slipSeq, kind: 'wrong_item', sku: line.sku, actualSku: act, qty: q }, worker, now);
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
    if (event === 'found') {
      // 商品が出てきた: この伝票の候補 (不足/品違い) を取り下げる。
      // 送信済みなら repick タスクも取消 (棚戻しタスクは残す=間違い品が実在する場合)
      for (const t of open) {
        db.prepare("UPDATE pk_pack_tasks SET status='cancelled', updated_at=? WHERE id=?").run(now, t.id);
      }
      db.prepare(`
        UPDATE pk_pack_incidents SET status='withdrawn', updated_at=?
        WHERE batch_id=? AND slip_seq=? AND status='candidate' AND kind IN ('shortage','wrong_item')
      `).run(now, batchId, slipSeq);
    } else {
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
    db.prepare("UPDATE pk_pack_slips SET status='pending', hold_reason=NULL WHERE id=?").run(slip.id);
    return {};
  }
  throw new PackError(400, 'bad_event', `不明なタスクイベント: ${event}`);
}

// ─── タスクの実行側操作 (picking のキュー画面から呼ばれる更新API) ───

export function listOpenTasks() {
  return getDB().prepare(`
    SELECT t.*, b.folder_name AS batch_folder FROM pk_pack_tasks t
    JOIN pk_pack_batches b ON b.id = t.batch_id
    WHERE t.status IN ('requested','claimed')
    ORDER BY t.id
  `).all();
}

export function countOpenTasks() {
  try {
    return getDB().prepare(
      "SELECT COUNT(*) c FROM pk_pack_tasks WHERE status IN ('requested','claimed')"
    ).get().c;
  } catch { return 0; }
}

/**
 * タスク操作 (claim / fulfill / unavailable / cancel)。
 * fulfill は kind で終端が変わる: repick→fulfilled (梱包者の受領待ち) / return→returned (完了)
 * @returns 更新後のタスク行 (+ unavailable 時は _notifyUnavailable)
 */
export function applyTaskAction(taskId, action, worker) {
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
    db.prepare('UPDATE pk_pack_tasks SET status=?, claimed_by=COALESCE(claimed_by, ?), updated_at=? WHERE id=?')
      .run(to, worker, now, taskId);
    const updated = { ...task, status: to, claimed_by: task.claimed_by || worker };
    if (action === 'unavailable') {
      // 在庫なし等 → 出荷可否は責任者判断へ (Q4未回答のため当面GChatエスカレーションのみ)
      updated._notifyUnavailable = true;
    }
    return updated;
  })();
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
      throw new PackError(409, 'already_resolved', `既に${inc.status === 'confirmed' ? '送信' : '取下げ'}済みです`);
    }
    const batch = db.prepare('SELECT * FROM pk_pack_batches WHERE id = ?').get(inc.batch_id);
    // 担当者チェック (Codex high: 任意の作業者名で他人のバッチの候補を送信・帰責できてしまう)
    if (batch.worker && batch.worker !== actor) {
      throw new PackError(409, 'taken', `このバッチは ${batch.worker} が担当です`);
    }
    let attributed = null;
    const dispatchedTasks = [];
    if (decision === 'confirm') {
      const pendingCount = db.prepare(
        "SELECT COUNT(*) c FROM pk_pack_slips WHERE batch_id=? AND status='pending'"
      ).get(inc.batch_id).c;
      // 許可状態を明示: 完了済み or 「作業中かつ未処理ゼロ」(=終了画面)。paused等からは不可 (Codex medium)
      const allowed = batch.status === 'done' || (batch.status === 'packing' && pendingCount === 0);
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
        if (sku !== inc.actual_sku) {
          db.prepare('UPDATE pk_pack_incidents SET actual_sku=?, updated_at=? WHERE id=?').run(sku, now, incidentId);
          inc.actual_sku = sku;
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
          ?? logizardNameOf(db, inc.actual_sku);
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
      // 取下げで、その伝票に他の候補が無ければ保留を解除して梱包に戻す
      const remain = db.prepare(`
        SELECT COUNT(*) c FROM pk_pack_incidents
        WHERE batch_id=? AND slip_seq=? AND status='candidate' AND kind IN ('shortage','wrong_item') AND id != ?
      `).get(inc.batch_id, inc.slip_seq, inc.id).c;
      if (remain === 0) {
        db.prepare(`
          UPDATE pk_pack_slips SET status='pending', hold_reason=NULL
          WHERE batch_id=? AND seq=? AND status='held' AND hold_reason='repick'
        `).run(inc.batch_id, inc.slip_seq);
      }
    }
    return { ...inc, status: decision === 'confirm' ? 'confirmed' : 'withdrawn', attributed_worker: attributed, dispatchedTasks };
  })();
}
