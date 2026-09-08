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
/** 同じ取得日の中の並び順 (中原さん 2026-09-09「あり、空欄、なしの順」)。小さいほど上 */
const IROHA_ORDER = Object.freeze({ yes: 0, unknown: 1, other: 2, no: 3 });

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
 *    ③ は「探しているキーに非ASCII があるとき」だけ走る = 実データではまず走らない。
 *    ②が当たっていても飛ばさない (別の仕入先の表記だけ拾って終わると、0001 の行を取り逃す — Codex R4 P2)。
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
  // ③ 探しているキーに非ASCII が 1 つでもあれば走らせる。
  //    ②で「たまたま同じ表記の行」が当たっていても飛ばさない — その行が別の仕入先で、
  //    大小違いの行が 0001 だと、飛ばした瞬間にその商品が消えて食い違いも報せられない (Codex R4 P2)
  if ([...wanted].some((k) => NON_ASCII.test(k))) {
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

/** 取り込んでから何日ぶんまで出すか (中原さん 2026-09-09「過去五日間だけ」)。
 *  元の CSV 自体が「当日から7日前まで」なので、ここを 7 より大きくしても増えない */
const PAST_DAYS = 5;

const emptyTotals = () => ({ products: 0, qty: 0, iroha_products: 0, iroha_qty: 0,
  arrived_products: 0, arrived_qty: 0, old_products: 0, old_qty: 0 });

/**
 * もう届いた明細 (line_key の集合)。
 *
 * 「届いた」の正本 = **倉庫の iPad が確認を確定したときに立つ行き先の台帳** `f_inbound_check_destinations`。
 *   - 確定と同じトランザクションで 1 行入り、やり直し (reopen) と 伝票から消えた取込 で cancelled_at が入る
 *   - **取込バッチに紐づかない**ので、3 日前に確認した明細も引ける (line_state の確認は業務日ごとに戻るため使えない)
 *   - actual_qty = 数えた実数。0 = 「確認したが 1 個も来なかった」なので**届いた扱いにしない**
 *     (列は後付けなので、それ以前の行は NULL = 確認できた分として扱う)
 *
 * ⚠ ロジザードの「受付数」(f_inbound_check_lines.received_qty) は使わない。入荷受付CSV は
 *    ステータス=受付済 で出しており、受付数は伝票を受け付けた時点の数で「現物が届いたか」を表さない。
 *
 * 念のため、いま開いている取込の「確認ずみ・1 個以上数えた」行も足す。台帳と二重の見張りにしておくと、
 * 行き先が付かないまま確認だけされた行 (画面を通さない経路) も取りこぼさない。
 */
function arrivedLineKeys(db, batchId, lines) {
  const arrived = new Set();
  for (const r of db.prepare(`SELECT line_key FROM f_inbound_check_line_state
    WHERE batch_id = ? AND status = 'checked' AND found_qty > 0`).all(batchId)) arrived.add(r.line_key);
  if (!tableExists(db, 'f_inbound_check_destinations')) return arrived;
  const keys = [...new Set(lines.map((l) => trimS(l.line_key)).filter(Boolean))];
  eachChunk(keys, 400, (part) => {
    const ph = part.map(() => '?').join(',');
    for (const r of db.prepare(`SELECT DISTINCT line_key FROM f_inbound_check_destinations
      WHERE cancelled_at IS NULL AND (actual_qty IS NULL OR actual_qty > 0) AND line_key IN (${ph})`).all(...part)) {
      arrived.add(r.line_key);
    }
  });
  return arrived;
}

/** 'YYYY-MM-DD' を days 日ずらす (JST の日付文字列のまま計算する) */
function shiftDate(ymd, days) {
  const m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(trimS(ymd));
  if (!m) return null;
  const d = new Date(Date.UTC(Number(m[1]), Number(m[2]) - 1, Number(m[3])));
  d.setUTCDate(d.getUTCDate() + days);
  return d.toISOString().slice(0, 10);
}

/**
 * 明細ごとの「取得日」= その明細を初めて取り込んだ日 (JST の 'YYYY-MM-DD')。
 *
 * ⭐**入荷予定日は使わない** (中原さん 2026-09-09「入庫予定日は正確に入れてないから、
 *   それより取り込んだ日付の方が欲しい」)。ロジザード側で予定日を正確に入れていないので、
 *   予定日を軸にすると「遅れている / 古い」の判断がそのまま狂う。
 *
 * 取込は毎回**全置換**なので、同じ明細を含むいちばん古いバッチの取込日 = その明細の取得日。
 * work_date ではなく imported_at から出す — work_date は繰り越し (rollOverWorkDate) で今日へ書き換わるため。
 *
 * 🚨 明細のキーは **line_key だけでなく code_key も**見る (Codex P1)。伝票の明細は商品が差し替わることが
 *    あり (取込側も product_changed として確認をやり直させる)、line_key だけで見ると新しい商品が
 *    前の商品の古い日付を引き継いで、取り込んだ当日に「5 日より前」として消える。
 *
 * さかのぼるのは LOOKBACK_DAYS 日ぶんだけ。それより前に載った明細は、どのみち PAST_DAYS で
 * 出さない側に落ちるので、正確な初日を知る必要がない (バッチは 365 日ぶん残るので、全部見ると重い)。
 */
const LOOKBACK_DAYS = 60;

function firstSeenMap(db, lines, today) {
  const map = new Map();
  const keys = [...new Set(lines.map((l) => trimS(l.line_key)).filter(Boolean))];
  if (!keys.length) return map;
  const since = shiftDate(today, -LOOKBACK_DAYS) || '0000-01-01';
  eachChunk(keys, 400, (part) => {
    const ph = part.map(() => '?').join(',');
    const rows = db.prepare(`SELECT l.line_key AS k, l.code_key AS c, MIN(date(b.imported_at, '+9 hours')) AS day
      FROM f_inbound_check_lines l
      JOIN f_inbound_check_batches b ON b.id = l.batch_id
     WHERE l.line_key IN (${ph}) AND date(b.imported_at, '+9 hours') >= ?
     GROUP BY l.line_key, l.code_key`).all(...part, since);
    for (const r of rows) if (r.day) map.set(`${r.k} ${r.c}`, r.day);
  });
  return map;
}

/**
 * 入荷予定の一覧 (仕入先 SUPPLIER_CODES に絞る)。
 *
 * ⭐軸は**取得日** (firstSeenMap)。ロジザードの入荷予定日は現場で正確に
 *   入れていないので、画面にも出さないし、並び順・切り捨ての判断にも使わない
 *   (中原さん 2026-09-09)。
 *
 * 同じ商品が同じ日に複数の明細に載ることがある (伝票が分かれている・行が分かれている)。
 * いろはが見たいのは「何がいくつ来るか」なので **取得日 × 商品でまとめて数量を足す**。
 * 何行をまとめたか (lines) と伝票番号 (ar_nos) は画面の補足に残す。
 *
 * ⭐出さないもの (中原さん 2026-09-09):
 *   - **もう届いた明細** … 倉庫が確認を確定したもの (arrivedLineKeys)。いろは行きならこの時点で
 *     📋 作業 のカードになっているので、入荷予定に残すと同じものが 2 か所に出る
 *   - **取り込んでから PAST_DAYS 日より前の未着** … 古いものがいつまでも居座らないように切る
 *   どちらも件数は totals に残して画面に理由を出す (黙って減らさない)
 *
 * @returns {{supplier, batch, rows, totals, day_stale, past_days, today}}
 */
export function listInboundPlan() {
  const batch = getActiveBatch();   // ← inbound-check 側のテーブルもここで冪等に作られる
  const db = getMirrorDB();         //   (同じ warehouse-mirror.db の同じ接続)
  const supplier = {
    codes: [...SUPPLIER_CODES],
    name: supplierNameOf(db, SUPPLIER_CODES[0]),
  };
  const today = workDateJst();
  if (!batch) {
    return { supplier, batch: null, rows: [], totals: emptyTotals(), day_stale: false, past_days: PAST_DAYS, today };
  }
  const lines = db.prepare(`
    SELECT line_key, code_key, product_id, product_name, planned_qty, seq, ar_no
      FROM f_inbound_check_lines WHERE batch_id = ? ORDER BY seq`).all(batch.id);
  const keys = [...new Set(lines.map((l) => codeKeyOf(l.code_key)).filter(Boolean))];
  const master = productMasterMap(db, lines);
  const iroha = irohaInfoMap(db, keys);
  const arrived = arrivedLineKeys(db, batch.id, lines);
  const firstSeen = firstSeenMap(db, lines, today);
  const want = wantedSuppliers();
  const oldest = shiftDate(today, -PAST_DAYS);   // これより前に取り込んだものは出さない

  // 出すもの / 届いたので出さないもの / 古すぎて出さないもの を同じまとめ方で数える
  const buckets = { rows: new Map(), arrived: new Map(), old: new Map() };
  for (const l of lines) {
    const key = codeKeyOf(l.code_key);
    const m = master.get(key) || null;
    if (!want.has(normSupplierSafe(m && m.supplier_code))) continue;
    // さかのぼり切れなかった明細 (LOOKBACK_DAYS より前から載っている) は「古い」側へ
    const day = firstSeen.get(`${trimS(l.line_key)} ${l.code_key}`) || null;
    // 届いたか → 古すぎるか の順に見る (届いた分は古くても「届いた」と数えたい)
    const bucket = arrived.has(trimS(l.line_key)) ? buckets.arrived
      : (!day || (oldest && day < oldest)) ? buckets.old
        : buckets.rows;
    // まとめる単位のキー。区切りは NUL — 商品コードに空白が入っていても日付との境目が曖昧にならない
    const gk = `${day || ''} ${key}`;
    const cur = bucket.get(gk);
    if (cur) {
      cur.qty += l.planned_qty;
      cur.lines += 1;
      if (!cur.ar_nos.includes(l.ar_no)) cur.ar_nos.push(l.ar_no);
      continue;
    }
    const ir = irohaOf(iroha.get(key));
    bucket.set(gk, {
      // ⭐この明細を取り込んだ日 (JST)。入荷予定日ではない
      fetched_on: day,
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
  // ⭐先に取り込んだ日から順に並べ、**同じ取得日の中を** いろは在庫化区分 の順で並べる
  //   (中原さん 2026-09-09「あり、空欄、なしの順で。同じ取得日内で」)。
  //   「状況による」等 (人が入れた値) は決まっていないもの寄りなので 未記入 の次に置く
  const rows = [...buckets.rows.values()].sort((a, b) =>
    String(a.fetched_on || '').localeCompare(String(b.fetched_on || ''))
    || IROHA_ORDER[a.iroha_kind] - IROHA_ORDER[b.iroha_kind]
    || String(a.product_name || '').localeCompare(String(b.product_name || ''), 'ja')
    || String(a.product_code || '').localeCompare(String(b.product_code || '')));

  const totals = emptyTotals();
  for (const r of rows) {
    totals.products += 1;
    totals.qty += r.qty;
    if (r.iroha_kind === 'yes') { totals.iroha_products += 1; totals.iroha_qty += r.qty; }
  }
  for (const [name, bucket] of [['arrived', buckets.arrived], ['old', buckets.old]]) {
    for (const r of bucket.values()) {
      totals[`${name}_products`] += 1;
      totals[`${name}_qty`] += r.qty;
    }
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
    past_days: PAST_DAYS,
    today,
    // 本日の取込がまだ来ていない = 前の日の一覧を見ている。
    //   work_date が今日でない … いろは側は繰り越し (rollOverWorkDate) を呼ばないので、まずここで分かる
    //   carried_from がある     … 倉庫の iPad が先に開いて work_date を今日へ繰り越した後 (中身は前の日のまま)
    day_stale: !!((batch.work_date && batch.work_date !== today) || batch.carried_from),
  };
}
