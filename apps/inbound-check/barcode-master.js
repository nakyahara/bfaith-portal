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
 *   - 代表 (rank=0) = **チェックデジットまで正しい GTIN** (JAN-8/UPC-A/JAN-13/GTIN-14) を最優先、
 *     次に FNSKU、最後にその他の数字列 (社内コードなど)。同じ格なら CSV に先に出てくるもの
 *   - 有効区分の意味はロジザード側の設定に依存するので**値で判断しない** (まだ確かめていない)。
 *     取り込んだ内訳を管理画面に出して、中原さんが実データを見て決められるようにする
 *     (商品マスタの有効期限区分と同じ進め方)
 *
 * fail-closed (product-master.js と同じ考え方 + 全量置換ゆえの安全弁):
 *   - 必須列 (商品ID / 商品名 / バーコード / 有効区分) が無ければ拒否。列名が変わったのを黙って通さない
 *   - 壊れた CP932・列数不一致は拒否
 *   - 0行は拒否。マスタが空になることは無く、空を通すと全商品のバーコードが消える
 *   - **同じバーコードが別の商品に付いていたら拒否** (どちらの商品か決められない = 誤った商品を刷る)
 *   - 🚨**前回よりバーコードが大きく減っていたら拒否** (出力が途中で切れた CSV を全量置換すると、
 *     正常なマスタがごっそり消える)。意図した削減なら管理画面で「減っても取り込む」を選ぶ
 */
import { getDB } from './db.js';
import { decodeCsvBuffer, parseCsv } from './csv.js';

const ID_COL = '商品ID';
const BC_COL = 'バーコード';
const NAME_COL = '商品名';
const KUBUN_COL = '有効区分';

/** 前回より何割まで減ってよいか。これを超えて減ったら取り込まない (人が承認すれば通る) */
export const SHRINK_LIMIT = 0.2;
/** 安全弁を効かせる下限。これ未満しか持っていないときは比べても意味がない (初回・試験用DB) */
const SHRINK_MIN_ROWS = 50;

/**
 * GTIN (JAN-8 / UPC-A / JAN-13 / GTIN-14) としてチェックデジットまで正しいか。
 * 商品につける正規のバーコードはこれを満たす。社内で振った数字列 (桁数が違う等) と区別して、
 * 値札に刷る代表を選ぶときに優先する
 */
export function isValidGtin(v) {
  const s = String(v == null ? '' : v).trim();
  if (!/^\d+$/.test(s)) return false;
  if (![8, 12, 13, 14].includes(s.length)) return false;
  const d = s.split('').map(Number);
  const check = d.pop();
  let sum = 0;
  // 右から 3,1,3,1… の重み (GS1 共通)
  for (let i = d.length - 1, w = 3; i >= 0; i--, w = w === 3 ? 1 : 3) sum += d[i] * w;
  return (10 - (sum % 10)) % 10 === check;
}

/** 数字だけ = JAN / 英字を含む英数字 = FNSKU / それ以外は値札に刷れないので採らない */
export function barcodeTypeOf(v) {
  const s = String(v == null ? '' : v).trim();
  if (!s || s.length > 40) return null;
  if (/^[0-9]+$/.test(s)) return 'jan';
  if (/^[A-Za-z0-9]+$/.test(s) && /[A-Za-z]/.test(s)) return 'fnsku';
  return null;
}

/** 代表を選ぶときの格 (小さいほど先): 正しい GTIN → FNSKU → その他の数字列 */
function grade(rec) {
  if (rec.gtin_ok) return 0;
  if (rec.barcode_type === 'fnsku') return 1;
  return 2;
}

/**
 * バーコードマスタ CSV を読む。
 * @returns {{rows, header, kubunCounts, products, invalidBarcodes, blankRows, conflicts}}
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
  // 🚨 5列そろっていることまで見る。列が減ったのを黙って通すと「区分不明のまま本番で使う」ことになる
  for (const col of [ID_COL, NAME_COL, BC_COL, KUBUN_COL]) {
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
  const byBarcode = new Map();   // barcode → code_key (別商品への付け替えを見つける)
  const byProduct = new Map();   // code_key → その商品の行 (代表を選ぶため)
  const conflicts = [];
  let invalidBarcodes = 0, blankRows = 0;
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    if (row.length === 1 && String(row[0] || '').trim() === '') continue;   // 末尾の空行
    if (row.length !== header.length) {
      bad(`${r + 1} 行目の列数が違います (ヘッダ ${header.length} 列 / この行 ${row.length} 列)`);
    }
    const productId = String(row[iId] || '').trim();
    const barcode = String(row[iBc] || '').trim();
    if (!productId || !barcode) { blankRows++; continue; }   // 空欄は数える (出力が途中で切れたのを見つけるため)
    const type = barcodeTypeOf(barcode);
    if (!type) { invalidBarcodes++; continue; }              // 値札に刷れない形 (記号入りなど) は積まない
    const codeKey = productId.toLowerCase();
    const already = byBarcode.get(barcode);
    if (already !== undefined) {
      // 同じ商品の同じバーコードが2度出るのは無害 (先勝ち)。**別の商品**なら決められないので取込ごと拒否
      if (already !== codeKey) conflicts.push({ barcode, a: already, b: codeKey });
      continue;
    }
    byBarcode.set(barcode, codeKey);
    const kubun = String(row[iKubun] || '').trim();
    kubunCounts[kubun || '(空欄)'] = (kubunCounts[kubun || '(空欄)'] || 0) + 1;
    const rec = {
      barcode,
      barcode_type: type,
      gtin_ok: type === 'jan' && isValidGtin(barcode),
      code_key: codeKey,
      product_id: productId,
      product_name: String(row[iName] || '').trim(),
      kubun,
      rank: 0,
    };
    out.push(rec);
    const list = byProduct.get(codeKey) || [];
    list.push(rec);
    byProduct.set(codeKey, list);
  }
  if (conflicts.length) {
    const s = conflicts.slice(0, 5).map(c => `${c.barcode} (${c.a} / ${c.b})`).join(' , ');
    bad(`同じバーコードが別の商品に付いています ${conflicts.length} 件: ${s}${conflicts.length > 5 ? ' …' : ''}`
      + ' — どちらの商品か決められないため取り込みません');
  }
  if (out.length === 0) bad('バーコードが1件もありません (マスタが空になることは無いため取込を中止しました)');
  // 商品ごとの代表 (rank=0): 正しい GTIN → FNSKU → その他の数字。同じ格なら CSV の順
  for (const list of byProduct.values()) {
    const sorted = [...list].sort((a, b) => grade(a) - grade(b));
    const head = sorted[0];
    let n = 1;
    for (const x of list) x.rank = x === head ? 0 : n++;
  }
  return { rows: out, header, kubunCounts, products: byProduct.size, invalidBarcodes, blankRows, conflicts: 0 };
}

/**
 * 取り込んで f_inbound_check_barcode_master を入れ替える。
 *
 * ⭐**全量置換**。マスタに無くなったバーコードは消す (ロジザード側で消したものを引き続き刷らない)。
 *   1つの書込みトランザクション (BEGIN IMMEDIATE) でやるので、途中で落ちても前の中身のまま残り、
 *   同時に走った取込とも重ならない。
 *
 * @param {object} o
 *   - sourceModifiedAt: この CSV の Drive 上の更新時刻。**取り込み済みのものより古ければコミットしない**
 *     (cron と手動が並んだとき、遅れて着いた古い世代でマスタを巻き戻さないため)
 *   - allowShrink: 前回より大きく減っていても取り込む (人が承認したとき)
 */
export function importBarcodeMaster(buffer, { actor = null, sourceModifiedAt = null, allowShrink = false } = {}) {
  const parsed = parseBarcodeMasterCsv(buffer);
  const db = getDB();
  const now = new Date().toISOString();
  const by = String(actor || 'logizard').trim() || 'logizard';

  const stats = {
    total: parsed.rows.length, products: parsed.products,
    invalidBarcodes: parsed.invalidBarcodes, blankRows: parsed.blankRows,
    kubunCounts: parsed.kubunCounts, added: 0, removed: 0,
  };
  return db.transaction(() => {
    const beforeRows = db.prepare('SELECT barcode FROM f_inbound_check_barcode_master').all();
    const before = new Set(beforeRows.map(r => r.barcode));
    // 🚨 世代の追い越し: 遅れて着いた古い CSV で新しいマスタを巻き戻さない
    const prevAt = getMeta(db, 'barcode_master_source_modified_at');
    if (sourceModifiedAt && prevAt && Date.parse(sourceModifiedAt) < Date.parse(prevAt)) {
      return { ok: false, error: 'stale_source', message: `もっと新しいバーコードマスタ (${prevAt}) が取り込み済みのため、この ${sourceModifiedAt} の内容は使いません` };
    }
    // 🚨 急に減っていたら取り込まない (出力が途中で切れた CSV の全量置換で正常なマスタが消える)
    if (!allowShrink && before.size >= SHRINK_MIN_ROWS && parsed.rows.length < before.size * (1 - SHRINK_LIMIT)) {
      return {
        ok: false, error: 'shrink_guard', before: before.size, after: parsed.rows.length, blankRows: parsed.blankRows,
        message: `バーコードが ${before.size} 件 → ${parsed.rows.length} 件 と ${Math.round((1 - parsed.rows.length / before.size) * 100)}% 減っています`
          + `${parsed.blankRows ? ` (商品IDかバーコードが空の行が ${parsed.blankRows} 件ありました)` : ''}。`
          + 'CSV の出力が途中で切れていないか確認してください。意図した減少なら「件数が減っても取り込む」を選んでください',
      };
    }
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
    if (sourceModifiedAt) setMeta(db, 'barcode_master_source_modified_at', sourceModifiedAt);
    setMeta(db, 'barcode_master_imported_at', now);
    return { ok: true, ...stats };
  }).immediate();
}

// ─── 取り込んだ世代の記録 (小さな key-value。表を増やさずここだけで使う) ───
function ensureMeta(db) {
  db.exec('CREATE TABLE IF NOT EXISTS f_inbound_check_barcode_meta (key TEXT PRIMARY KEY, value TEXT, updated_at TEXT NOT NULL)');
}
function getMeta(db, key) {
  ensureMeta(db);
  return db.prepare('SELECT value FROM f_inbound_check_barcode_meta WHERE key = ?').get(key)?.value || null;
}
function setMeta(db, key, value) {
  ensureMeta(db);
  db.prepare(`INSERT INTO f_inbound_check_barcode_meta (key, value, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at`)
    .run(key, value, new Date().toISOString());
}

/** 何件取り込めているか (管理画面の表示用) */
export function barcodeMasterStatus() {
  const db = getDB();
  const r = db.prepare(`SELECT COUNT(*) AS total, COUNT(DISTINCT code_key) AS products, MAX(updated_at) AS at
    FROM f_inbound_check_barcode_master`).get();
  return {
    total: r?.total || 0, products: r?.products || 0, updatedAt: r?.at || null,
    sourceModifiedAt: getMeta(db, 'barcode_master_source_modified_at'),
  };
}
