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
    const key = `${r.slip_no} ${normSku(r.sku)}`;
    pickMap.set(key, (pickMap.get(key) || 0) + r.qty);
  }
  const packMap = new Map();
  for (const s of preview.slips) {
    for (const l of s.lines) {
      const key = `${s.slipNo} ${normSku(l.sku)}`;
      packMap.set(key, (packMap.get(key) || 0) + l.qty);
    }
  }

  const diffs = [];
  const fmt = (key) => key.replace(' ', ' × ');
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

const WORK_EVENTS = ['start', 'next', 'takeover', 'pause', 'resume', 'cancel', 'undo', 'jump'];

// 中断理由 (picking と同じ最小セット)
export const PAUSE_REASONS = ['休憩', '他作業への応援', 'その他'];
// 完了取消の理由 (監査ログ必須 — 要件§5.1)
export const UNDO_REASONS = ['誤タップ', '入れ間違いの確認', 'その他'];

/** 端末の発生時刻を「now以前・24時間以内」にクランプ (中断時間の計測用。picking と同じ)。 */
function clampedEventTime(clientAt, now) {
  const t = Date.parse(clientAt || '');
  const nowMs = Date.parse(now);
  if (!Number.isFinite(t)) return now;
  if (t > nowMs || t < nowMs - 24 * 3600 * 1000) return now;
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
  return {
    batch,
    slips: full,
    currentSeq: pending.length > 0 ? pending[0].seq : null,
    doneCount: full.filter((s) => s.status === 'done').length,
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
export function applyEvent(batchId, { opId, event, slipSeq, clientAt, reason, jumped }, worker) {
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
          && (pp.reason ?? null) === (reason ?? null) && !!pp.jumped === !!jumped) {
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

    if (event === 'takeover') {
      // 担当者の交代 (選び間違い・実際の引き継ぎ)。作業中・中断中のみ・記録が残る唯一の正規突破口
      if (batch.status !== 'packing' && batch.status !== 'paused') {
        throw new PackError(409, 'not_packing', `作業中ではないため交代できません (${batch.status})`);
      }
      if (batch.worker !== worker) {
        db.prepare('UPDATE pk_pack_batches SET worker=?, updated_at=? WHERE id=?').run(worker, now, batchId);
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
        .run(clampedEventTime(clientAt, now), r, now, batchId);
    } else if (event === 'resume') {
      if (batch.status !== 'paused') {
        throw new PackError(409, 'not_paused', `中断中ではありません (${batch.status})`);
      }
      requireOwner();
      const resumeAt = clampedEventTime(clientAt, now);
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
      db.prepare("UPDATE pk_pack_slips SET status='pending', shown_at=NULL, done_at=NULL WHERE batch_id=?")
        .run(batchId);
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
        db.prepare("UPDATE pk_pack_batches SET status='done', finished_at=?, updated_at=? WHERE id=?")
          .run(now, now, batchId);
      }
    }

    const result = eventResult(batchId);
    // eventResult はこのイベント行の INSERT より前に走るため、lastDoneSeq (イベント履歴由来) に
    // いま完了させた伝票が反映されない。next のときはここで上書きする
    if (event === 'next') result.lastDoneSeq = slipSeq;
    const payload = (clientAt || reason || jumped)
      ? JSON.stringify({ clientAt, reason, jumped: jumped || undefined }) : null;
    db.prepare(`
      INSERT INTO pk_pack_events (op_id, batch_id, worker, event, slip_seq, payload_json, result_json, at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(opId, batchId, worker, event, slipSeq ?? null, payload, JSON.stringify(result), now);
    return result;
  })();
}
