/**
 * いろは在庫化作業アプリ — 🚚 入荷予定 (中原さん 2026-09-08)
 *
 * 「入荷予定が見れるようにしてほしい。仕入先コード0001だけ商品名数量、いろは在庫化区分が見れたらいい」
 *
 * 出どころ = **入荷受付チェック (apps/inbound-check) の active バッチ**。倉庫の iPad が見ているのと同じ1枚。
 *   miniPC の auto-nyuka.js が ロジザード 入荷状況照会[FA04_01] を「受付済 / 当日から7日前まで」で出した CSV を
 *   Drive 経由で Render が30分おきに取り込んでいる。
 *   ⚠ **ロジザード側の検索範囲が「当日から7日前まで」**なので、ここに出るのは今日届く分と直近に受け付けた分だけ。
 *      もっと先の予定は入らない (画面にもそう書く)。範囲を変えたいときは miniPC 側 (step-nyuka.js の daterange) から。
 *
 * 仕入先の絞り込み: 入荷受付CSVに**弊社の仕入先コードは載っていない** (取引先ID/取引先名はロジザードの取引先で、
 *   ネクストエンジンの仕入先コードとは別体系) ので、商品マスタ mirror_products.仕入先コード で引く。
 *   '0001' (NE の生コード) と '1' (発注管理の正規形) の揺れは normSupplierCode を通して吸収する
 *   — inbound-check/db.js の supplierNameMap・iroha-work/task-intake.js と同じ考え方。
 *
 * いろは在庫化区分 = 入庫情報管理 f_inbound_info.いろは在庫化作業有無 (有り / 無し / 未記入 / 人が入れた値)。
 *
 * ⭐読むだけのモジュール。ここからは何も書かない (正本は ロジザード と 入庫情報管理)。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { getActiveBatch, workDateJst } from '../inbound-check/db.js';
import { normSupplierCode } from '../purchase-orders/db.js';

/**
 * 見せる仕入先 (ネクストエンジンの生コード)。0001 = アメージングクラフト様。
 * 増やすときはここに足す — 画面・API は配列のまま扱う
 */
export const SUPPLIER_CODES = Object.freeze(['0001']);

const trimS = (v) => String(v == null ? '' : v).trim();
const tableExists = (db, name) => !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);

/**
 * 仕入先コードの正規形 (先頭ゼロを外した形)。
 * normSupplierCode は数字を parseInt するので、2^53 を超える数字コードは丸められて
 * 別のコードと同じ形になりうる。そういうコードは正規化せず生のまま突き合わせる
 * (inbound-check/db.js supplierNameMap と同じ用心)
 */
function normSupplierSafe(v) {
  const s = trimS(v);
  if (!s) return null;
  if (/^\d+$/.test(s) && !Number.isSafeInteger(Number(s))) return s;
  return normSupplierCode(s) || null;
}

function eachChunk(keys, size, fn) {
  for (let i = 0; i < keys.length; i += size) fn(keys.slice(i, i + size));
}

// ─── いろは在庫化区分の読み方 (inbound-check/db.js resolveDestination と同じ表記ゆれの扱い) ───
const IROHA_YES = '有り';
const IROHA_NO = '無し';
const NA_VALUES = new Set(['', '－', '-', 'ー', '―']);   // 「未記入」と同じ扱いにする表記

/**
 * @returns {{label: string, kind: 'yes'|'no'|'unknown'|'other'}}
 *   other = 「状況による」等、人が意図して入れた値。画面はその文字をそのまま出す (勝手に有り/無しへ寄せない)
 */
function irohaOf(raw) {
  const v = trimS(raw);
  if (v === IROHA_YES) return { label: IROHA_YES, kind: 'yes' };
  if (v === IROHA_NO) return { label: IROHA_NO, kind: 'no' };
  if (NA_VALUES.has(v)) return { label: '未記入', kind: 'unknown' };
  return { label: v, kind: 'other' };
}

/** 照合キー (入荷受付・入庫情報と同じ規則 = lower(trim)。JS 側で作る) */
const codeKeyOf = (v) => trimS(v).toLowerCase();

/** 出すべき仕入先 (正規形) の集合 */
const wantedSuppliers = () => new Set(SUPPLIER_CODES.map(normSupplierSafe).filter(Boolean));

/**
 * 同じ code_key に商品マスタの行が 2 つ以上あったとき、どれを採るかの順 (小さいほど先)。
 * 商品コードは大文字小文字違いで別の行として入りうる (`商品コード` の UNIQUE は大小を区別する)。
 * SQLite が返す順に任せると、片方が 0001・片方が別の仕入先のとき「日によって出たり出なかったりする」
 * 一覧になるので、ここで決め方をはっきりさせる (Codex R1 P2)。
 */
function masterRank(row, want) {
  const s = normSupplierSafe(row.supplier_code);
  if (s && want.has(s)) return 0;   // 出すべき仕入先の行
  if (s) return 1;                  // 別の仕入先だが仕入先は入っている
  return 2;                         // 仕入先コードが空
}

/**
 * 商品マスタ (仕入先コード・商品名・取扱区分) を code_key で引く Map。
 *
 * 🚨 SQLite の lower() は **ASCII 限定**で、JS の toLowerCase() (= code_key の作り方) と食い違う
 *    (全角英字を小文字にできない。warehouse-mirror/db.js が f_inbound_info の CHECK を張らない理由と同じ)。
 *    ここで取りこぼすと「仕入先が分からない」扱いになり、仕入先 0001 の商品でも一覧から**黙って消える**。
 *    そこで 3 段で拾う:
 *      ① ASCII 版のキー … LOWER(TRIM(商品コード)) IN (code_key)   … 実データはほぼこれで当たる
 *      ② 生の商品ID     … TRIM(商品コード) IN (商品ID)            … 全角コードで表記が同じもの
 *      ③ 非ASCII の商品コードだけ読んで JS で突き合わせる          … 全角コードで大小が違うもの
 *         (ＡＭＣ-Ｚ と ａｍｃ-ｚ。①②のどちらでも当たらない — Codex R3 P2)
 *    ③ は「非ASCII を含む商品コードを引き当てられなかったとき」だけ走る = 実データではまず走らない。
 *
 * 🚨 同じ code_key の行が 2 つ以上あるときは masterRank の順で決める (毎回同じ行を選ぶ)。
 *    仕入先が食い違っている (0001 と別の仕入先が両方ある) ことは呼び元へ伝えて画面に出す —
 *    黙ってどちらかに決めない。商品マスタを直すのは人の仕事
 *
 * @returns {Map<string, {code, supplier_code, name, handling, supplier_conflict: boolean}>}
 */
const NON_ASCII = /[^\x20-\x7E]/;
const MASTER_SELECT = `SELECT 商品コード AS code, 仕入先コード AS supplier_code, 商品名 AS name, 取扱区分 AS handling
      FROM mirror_products`;

function productMasterMap(db, lines) {
  const map = new Map();
  if (!tableExists(db, 'mirror_products')) return map;
  const wanted = new Set();   // 探している code_key (JS の規則)
  const probes = new Set();   // SQL の IN に渡す値 (①ASCII 版のキー + ②生の商品ID)
  for (const l of lines) {
    const key = codeKeyOf(l.code_key);
    if (!key) continue;
    wanted.add(key);
    probes.add(key);
    const raw = trimS(l.product_id);
    if (raw) probes.add(raw);
  }
  if (!wanted.size) return map;
  const want = wantedSuppliers();
  const seen = new Map();   // code_key → 見つけた仕入先 (正規形) の集合。食い違いの検出用
  const consider = (r) => {
    const k = codeKeyOf(r.code);
    if (!wanted.has(k)) return;   // ASCII 版のキーだけ当たった別商品を拾わない
    const s = normSupplierSafe(r.supplier_code);
    if (s) {
      if (!seen.has(k)) seen.set(k, new Set());
      seen.get(k).add(s);
    }
    const cur = map.get(k);
    if (!cur) { map.set(k, r); return; }
    const d = masterRank(r, want) - masterRank(cur, want);
    // 同じ順位なら商品コードの文字順で決める (SQLite の返す順に左右されない)
    if (d < 0 || (d === 0 && trimS(r.code) < trimS(cur.code))) map.set(k, r);
  };
  // ①② 1 チャンクで束縛する値は placeholder 2 組ぶん = 600 (SQLite の上限 999 に余裕を残す)
  eachChunk([...probes], 300, (part) => {
    const ph = part.map(() => '?').join(',');
    for (const r of db.prepare(`${MASTER_SELECT} WHERE LOWER(TRIM(商品コード)) IN (${ph}) OR TRIM(商品コード) IN (${ph})`)
      .all(...part, ...part)) consider(r);
  });
  // ③ 非ASCII の商品コードで引けなかったものが残っているときだけ
  if ([...wanted].some((k) => !map.has(k) && NON_ASCII.test(k))) {
    for (const r of db.prepare(`${MASTER_SELECT} WHERE 商品コード GLOB '*[^ -~]*'`).all()) consider(r);
  }
  for (const [k, sups] of seen) {
    const m = map.get(k);
    if (m) m.supplier_conflict = sups.size > 1;
  }
  return map;
}

/** 入庫情報管理の「いろは在庫化作業有無」を code_key で引く Map */
function irohaInfoMap(db, keys) {
  const map = new Map();
  if (!keys.length || !tableExists(db, 'f_inbound_info')) return map;
  eachChunk(keys, 400, (part) => {
    const ph = part.map(() => '?').join(',');
    for (const r of db.prepare(`SELECT code_key, いろは在庫化作業有無 AS iroha FROM f_inbound_info WHERE code_key IN (${ph})`).all(...part)) {
      map.set(r.code_key, r.iroha);
    }
  });
  return map;
}

/**
 * 仕入先名 (見出し用)。
 *
 * 🚨 コードの持ち方が 2 系統ある (inbound-check/db.js supplierNameMap と同じ事情):
 *      supplier_share_master.仕入先コード … mirror_products と同じ体系 = NE の生コード ('0001')
 *      po_suppliers.supplier_code         … 発注管理の正規形 ('1')
 *    発注管理側は **生コードの完全一致を先に**見る。いきなり正規形にすると、'0001' と '1' が
 *    別の会社として両方登録されているときに、絞り込んでいるのと違う会社の名前を見出しに出す (Codex R2 P2)。
 *    同じ正規形に別名が 2 つ以上ぶら下がっていたら、どちらか分からないので**名前を出さない**。
 */
function supplierNameOf(db, code) {
  const raw = trimS(code);
  if (!raw) return null;
  // ① 仕入先向け売れ筋共有の表示名 (mirror_products と同じ体系なので、ここが最も確か)
  if (tableExists(db, 'supplier_share_master')) {
    const r = db.prepare('SELECT 表示名 AS name FROM supplier_share_master WHERE trim(仕入先コード) = ?').get(raw);
    if (r && trimS(r.name)) return trimS(r.name);
  }
  // ② 発注管理の仕入先マスタ (数十件なので読み切って JS で突き合わせる — 正規化は SQL では書けない)
  if (tableExists(db, 'po_suppliers')) {
    const rows = db.prepare('SELECT supplier_code, name FROM po_suppliers').all()
      .map((r) => ({ code: trimS(r.supplier_code), name: trimS(r.name) }))
      .filter((r) => r.code && r.name);
    const exact = rows.find((r) => r.code === raw);
    if (exact) return exact.name;
    const key = normSupplierSafe(raw);
    if (key) {
      const names = new Set(rows.filter((r) => normSupplierSafe(r.code) === key).map((r) => r.name));
      if (names.size === 1) return [...names][0];   // 2 つ以上あれば決められないので出さない
    }
  }
  return null;
}

const emptyTotals = () => ({ products: 0, qty: 0, iroha_products: 0, iroha_qty: 0 });

/**
 * 入荷予定の一覧 (仕入先 SUPPLIER_CODES に絞る)。
 *
 * 入荷予定日は**伝票 (AR番号) 単位** (f_inbound_check_slips.planned_date)。明細には日付が無い。
 *
 * 同じ商品が同じ入荷予定日に複数の明細に載ることがある (伝票が分かれている・行が分かれている)。
 * いろはが見たいのは「何がいくつ来るか」なので **入荷予定日 × 商品でまとめて数量を足す**。
 * 何行をまとめたか (lines) と伝票番号 (ar_nos) は画面の補足に残す。
 *
 * @returns {{supplier, batch, rows, totals, day_stale}}
 */
export function listInboundPlan() {
  const batch = getActiveBatch();   // ← inbound-check 側のテーブルもここで冪等に作られる
  const db = getMirrorDB();         //   (同じ warehouse-mirror.db の同じ接続)
  const supplier = {
    codes: [...SUPPLIER_CODES],
    name: supplierNameOf(db, SUPPLIER_CODES[0]),
  };
  if (!batch) {
    return { supplier, batch: null, rows: [], totals: emptyTotals(), day_stale: false };
  }
  const lines = db.prepare(`
    SELECT l.code_key, l.product_id, l.product_name, l.planned_qty, l.seq, l.ar_no, s.planned_date
      FROM f_inbound_check_lines l
      LEFT JOIN f_inbound_check_slips s ON s.batch_id = l.batch_id AND s.ar_no = l.ar_no
     WHERE l.batch_id = ?
     ORDER BY l.seq`).all(batch.id);
  const keys = [...new Set(lines.map((l) => codeKeyOf(l.code_key)).filter(Boolean))];
  const master = productMasterMap(db, lines);
  const iroha = irohaInfoMap(db, keys);
  const want = wantedSuppliers();

  const grouped = new Map();
  for (const l of lines) {
    const key = codeKeyOf(l.code_key);
    const m = master.get(key) || null;
    if (!want.has(normSupplierSafe(m && m.supplier_code))) continue;
    const day = trimS(l.planned_date) || null;
    const gk = `${day || ''} ${key}`;
    const cur = grouped.get(gk);
    if (cur) {
      cur.qty += l.planned_qty;
      cur.lines += 1;
      if (!cur.ar_nos.includes(l.ar_no)) cur.ar_nos.push(l.ar_no);
      continue;
    }
    const ir = irohaOf(iroha.get(key));
    grouped.set(gk, {
      planned_date: day,
      product_code: trimS(l.product_id) || null,
      // 商品名はロジザードの明細を先に (現物の箱に貼ってあるのと同じ表記)。空なら商品マスタで補う
      product_name: trimS(l.product_name) || trimS(m && m.name) || null,
      qty: l.planned_qty,
      lines: 1,
      ar_nos: [l.ar_no],
      iroha: ir.label,
      iroha_kind: ir.kind,
      // 商品マスタに同じ商品コードの行が 2 つあり、仕入先が食い違っている (Codex R1 P2)。
      // 黙って片方に決めず画面に出す — 直すのは人の仕事
      supplier_conflict: !!(m && m.supplier_conflict),
      handling: trimS(m && m.handling) || null,
    });
  }
  // 早く届く順 → 商品名。入荷予定日が空の行は末尾へ (日付が分からないものを先頭に出さない)
  const rows = [...grouped.values()].sort((a, b) =>
    (a.planned_date ? 0 : 1) - (b.planned_date ? 0 : 1)
    || String(a.planned_date || '').localeCompare(String(b.planned_date || ''))
    || String(a.product_name || '').localeCompare(String(b.product_name || ''), 'ja')
    || String(a.product_code || '').localeCompare(String(b.product_code || '')));

  const totals = emptyTotals();
  for (const r of rows) {
    totals.products += 1;
    totals.qty += r.qty;
    if (r.iroha_kind === 'yes') { totals.iroha_products += 1; totals.iroha_qty += r.qty; }
  }
  return {
    supplier,
    batch: {
      imported_at: batch.imported_at,
      work_date: batch.work_date || null,
      source: batch.source,
      file_name: batch.file_name || null,
      row_count: batch.row_count,
      carried_from: batch.carried_from || null,
    },
    rows,
    totals,
    // 本日の取込がまだ来ていない = 前の日の一覧を見ている。
    //   work_date が今日でない … いろは側は繰り越し (rollOverWorkDate) を呼ばないので、まずここで分かる
    //   carried_from がある     … 倉庫の iPad が先に開いて work_date を今日へ繰り越した後 (中身は前の日のまま)
    day_stale: !!((batch.work_date && batch.work_date !== workDateJst()) || batch.carried_from),
  };
}
