/**
 * バーコードマスタ (ロジザード エクスポート[FM08_01] 種類=バーコード) の取込。
 *
 * 中原さん 2026-09-06:「粟国の塩のバーコード情報がなかった。共有ドライブのバーコードマスタ.csv は
 * 常に最新のバーコード情報を入れているから、ここを参照するかこの情報をサーバーにあげる仕組みを作って」
 *
 * ⭐これが**バーコードの正本**になる。今まで Render 側にはバーコードの完全なマスタが無く、
 *   入荷受付伝票の明細・在庫ミラー・入荷予定 (7日分) から拾うしかなかったため、
 *   「入荷したことがなく在庫も無い商品」は値札を出せなかった (粟国の塩がこれ)。
 *
 * 列 (CP932):  商品ID / 商品名 / 検索名称 / バーコード / 有効区分
 *   - **1商品に複数のバーコードが載る** (JAN と Amazon の FNSKU が別行など)。全部持つ:
 *     値札に刷るのは代表1つだが、📷 カメラや検索ではどのバーコードからでも商品を引きたい
 *   - 代表 = JAN (数字だけ) を優先し、同じ種別なら CSV に先に出てくるもの
 *   - 有効区分の意味はロジザード側の設定に依存するので**値で判断しない**。
 *     取り込んだ内訳を管理画面に出して、中原さんが実データを見て決められるようにする
 *     (商品マスタの有効期限区分と同じ進め方)
 *
 * fail-closed (product-master.js と同じ考え方):
 *   - 必須列 (商品ID / バーコード) が無ければ拒否。列名が変わったのを黙って通さない
 *   - 壊れた CP932・列数不一致は拒否
 *   - 0行は拒否。マスタが空になることは無く、空を通すと全商品のバーコードが消える
 */
import { getDB } from './db.js';
import { decodeCsvBuffer, parseCsv } from './csv.js';

const ID_COL = '商品ID';
const BC_COL = 'バーコード';
const NAME_COL = '商品名';
const KUBUN_COL = '有効区分';

/** 数字だけ = JAN / 英字を含む英数字 = FNSKU / それ以外は値札に刷れないので採らない */
export function barcodeTypeOf(v) {
  const s = String(v == null ? '' : v).trim();
  if (!s || s.length > 40) return null;
  if (/^[0-9]+$/.test(s)) return 'jan';
  if (/^[A-Za-z0-9]+$/.test(s) && /[A-Za-z]/.test(s)) return 'fnsku';
  return null;
}

/**
 * バーコードマスタ CSV を読む。
 * @returns {{rows: Array<{barcode, barcode_type, code_key, product_id, product_name, kubun, rank}>,
 *            header: string[], kubunCounts: object, products: number, skipped: number}}
 * @throws {Error} 検証に落ちたら (error.code = 'bad_csv')
 */
export function parseBarcodeMasterCsv(buffer) {
  const bad = (msg) => { const e = new Error(msg); e.code = 'bad_csv'; throw e; };
  if (!buffer || buffer.length === 0) bad('ファイルが空です');
  let text;
  try {
    text = decodeCsvBuffer(buffer);
  } catch (e) {
    bad(`Shift-JIS として読めません (${e.message})`);
  }
  let rows;
  try {
    rows = parseCsv(text);
  } catch (e) {
    bad(e.message);
  }
  if (rows.length === 0) bad('中身がありません');
  const header = rows[0].map(h => String(h || '').trim());
  for (const col of [ID_COL, BC_COL]) {
    if (!header.includes(col)) {
      bad(`必須列「${col}」がありません (実際の列: ${header.slice(0, 12).join(' / ')}${header.length > 12 ? ' …' : ''})`);
    }
  }
  const dup = header.filter((h, i) => h && header.indexOf(h) !== i);
  if (dup.length) bad(`列名が重複しています: ${[...new Set(dup)].join(', ')}`);
  const iId = header.indexOf(ID_COL);
  const iBc = header.indexOf(BC_COL);
  const iName = header.indexOf(NAME_COL);
  const iKubun = header.indexOf(KUBUN_COL);

  const out = [];
  const kubunCounts = {};
  const seenBarcode = new Set();
  const byProduct = new Map();   // code_key → その商品の行 (代表を選ぶため)
  let skipped = 0;
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    if (row.length === 1 && String(row[0] || '').trim() === '') continue;   // 末尾の空行
    if (row.length !== header.length) {
      bad(`${r + 1} 行目の列数が違います (ヘッダ ${header.length} 列 / この行 ${row.length} 列)`);
    }
    const productId = String(row[iId] || '').trim();
    const barcode = String(row[iBc] || '').trim();
    if (!productId || !barcode) continue;          // どちらか無い行は読み飛ばす
    const type = barcodeTypeOf(barcode);
    if (!type) { skipped++; continue; }            // 値札に刷れない形 (記号入りなど) は積まない
    if (seenBarcode.has(barcode)) continue;        // 同じバーコードが2度出たら先勝ち
    seenBarcode.add(barcode);
    const kubun = iKubun >= 0 ? String(row[iKubun] || '').trim() : '';
    kubunCounts[kubun || '(空欄)'] = (kubunCounts[kubun || '(空欄)'] || 0) + 1;
    const rec = {
      barcode,
      barcode_type: type,
      code_key: productId.toLowerCase(),
      product_id: productId,
      product_name: iName >= 0 ? String(row[iName] || '').trim() : '',
      kubun,
      rank: 0,
    };
    out.push(rec);
    const list = byProduct.get(rec.code_key) || [];
    list.push(rec);
    byProduct.set(rec.code_key, list);
  }
  if (out.length === 0) bad('バーコードが1件もありません (マスタが空になることは無いため取込を中止しました)');
  // 商品ごとの代表 (rank=0) を決める: JAN を優先、同じ種別なら CSV に先に出てきたもの
  for (const list of byProduct.values()) {
    const jan = list.find(x => x.barcode_type === 'jan');
    const head = jan || list[0];
    let n = 1;
    for (const x of list) x.rank = x === head ? 0 : n++;
  }
  return { rows: out, header, kubunCounts, products: byProduct.size, skipped };
}

/**
 * 取り込んで f_inbound_check_barcode_master を入れ替える。
 *
 * ⭐**全量置換**。マスタに無くなったバーコードは消す (ロジザード側で消したものを引き続き刷らない)。
 *   1つのトランザクションでやるので、途中で落ちても前の中身のまま残る。
 *   手入力の控え (f_inbound_check_barcodes の manual) はここでは触らない —
 *   マスタに載った商品は resolveBarcode がマスタを先に見るので、自然に置き換わる。
 */
export function importBarcodeMaster(buffer, { actor = null } = {}) {
  const parsed = parseBarcodeMasterCsv(buffer);
  const db = getDB();
  const now = new Date().toISOString();
  const by = String(actor || 'logizard').trim() || 'logizard';

  const stats = {
    total: parsed.rows.length, products: parsed.products, skipped: parsed.skipped,
    kubunCounts: parsed.kubunCounts, added: 0, removed: 0,
  };
  db.transaction(() => {
    const before = new Set(db.prepare('SELECT barcode FROM f_inbound_check_barcode_master').all().map(r => r.barcode));
    db.prepare('DELETE FROM f_inbound_check_barcode_master').run();
    const ins = db.prepare(`INSERT INTO f_inbound_check_barcode_master
      (barcode, code_key, product_id, product_name, barcode_type, kubun, rank, updated_at, updated_by)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`);
    for (const r of parsed.rows) {
      ins.run(r.barcode, r.code_key, r.product_id, r.product_name || null, r.barcode_type, r.kubun || null, r.rank, now, by);
      if (!before.has(r.barcode)) stats.added++;
      before.delete(r.barcode);
    }
    stats.removed = before.size;
  }).immediate();
  return { ok: true, ...stats };
}

/** 何件取り込めているか (管理画面の表示用) */
export function barcodeMasterStatus() {
  const db = getDB();
  const r = db.prepare(`SELECT COUNT(*) AS total, COUNT(DISTINCT code_key) AS products, MAX(updated_at) AS at
    FROM f_inbound_check_barcode_master`).get();
  return { total: r?.total || 0, products: r?.products || 0, updatedAt: r?.at || null };
}
