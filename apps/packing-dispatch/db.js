/**
 * 梱包機振り分け・配送方法判定 — データ層 / 判定エンジン (Render 完結)
 *
 * NE→ロジザード CSV を取り込み、配送方法と梱包機マーカー(第91列)を判定して再出力する。
 * 永続マスタ(pd_*) と 揮発 staging(pd_import_*) は warehouse-mirror.db に同居し、
 * mirror_products(NE 商品マスタのミラー) と直接 JOIN して未登録チェックを行う。
 * ミニPC は一切使わない (API 不要)。
 *
 * 設計根拠: project_packing_machine_routing メモリ + Codex 4ラウンド (critical/high なし)。
 *  - 連動列(34/17/76/92/91)は finalizeLine() で必ず一括導出 (途中で34だけ書かない)
 *  - shipping_rule は区間モデル(qty_min,qty_max NULL=無限)、保存時にトランザクション内で連続性検証
 *  - アソート学習はヘッダ+明細、combo_key=正規化明細のSHA-256+version
 *  - 未登録/不整合は fail-safe で必ず row_status='要判断' (B1)、NE CSV ヘッダは fail-closed 検証 (B3)
 */
import crypto from 'node:crypto';
import { getMirrorDB } from '../warehouse-mirror/db.js';

// ───────────────────────── 定数 ─────────────────────────

export const EXPECTED_COL_COUNT = 93;

// CSV 列インデックス(0始まり)。NE→ロジザード CSV 実データで確認済み。
export const COL = {
  shop: 1,            // ショップ名
  orderNo: 2,         // 注文番号 (モール注文番号・伝票グルーピングキー)
  uketsuke: 20,       // 受注番号/伝票番号 (1伝票1値の連番。ヘッダ表記は顧客idだが実態はNE受注番号)
  recipient: 4,       // 配送先名
  pref: 6,            // 配送先都道府県
  carrierId: 17,      // 配送会社id          ← 連動列
  deliveryDate: 18,   // 配送指定日 (表示用)
  deliveryTime: 19,   // 配送時間帯 (表示用)
  shippingMethod: 34, // 配送方法            ← 連動列(出力主)
  productName: 56,    // 品名 (商品名・表示用)
  productCode: 57,    // 形式/型番 (=NE商品コード。品番(55)は空でこちらに入る)
  colorName: 59,      // 色名
  sizeName: 61,       // サイズ名
  qty: 63,            // 受注数
  neCarrierId: 76,    // ne配送会社id        ← 連動列
  packing: 91,        // 自動梱包機使用      ← 連動列(梱包機マーカー)
  yamatoBill: 92,     // 配送方法別ヤマト請求顧客コード ← 連動列
};

// ヘッダ厳密検証用のアンカー列(この位置にこの名前が無ければ fail-closed)
const HEADER_ANCHORS = {
  1: 'ショップ名', 2: '注文番号', 6: '配送先都道府県', 17: '配送会社id',
  34: '配送方法', 55: '品番', 63: '受注数', 76: 'ne配送会社id',
  91: '自動梱包機使用', 92: '配送方法別ヤマト請求顧客コード',
};

// 配送方法コードテーブルの初期値 (実データ実測。code→{name_csv,17,76,92,rank,locked,nekopos})
const SHIPPING_METHODS = [
  // code, name_csv, carrier_id, ne_carrier_id, yamato_bill_code, rank, is_locked, is_nekopos
  ['nekopos',     'ヤマト(ネコポス)',             '99', '28', '0661557433',  2, 0, 1],
  ['yupacketpuff','ゆうパケットパフ',             '07', '34', '',            3, 0, 0],
  ['takkyu50',    'ヤマト宅急便【50サイズ専用】', '01', '24', '066155743311',4, 0, 0],
  ['hatsubarai',  'ヤマト(発払い)B2v6',           '12', '20', '0661557433',  5, 0, 0],
  ['teikeigai',   '定形外郵便',                   '99', '41', '',            1, 0, 0],
  ['letterpack',  'レターパック500',             '07', '31', '',            null, 0, 0], // 階層外: 沖縄北海道特例
  ['aes',         'AES',                         '19', '71', '',            null, 1, 0], // 変更対象外(lock)
];

// 梱包機コードテーブル。name_csv が CSV 第91列に書く値 (ロジザード取込時に変換)。
const PACKING_MACHINES = [
  ['pasline3', 'pasline3つ折り', 1],
  ['pasline2', 'pasline2つ折り', 2],
  ['meltline', 'meltline',       3],
  ['manual',   '手動出荷',       9],
];

// ショップ名→mall_group。Amazon 系のみ amazon、それ以外(楽天/Yahoo/LINE/メルカリ/auPay/Qoo10…)は rakuten を参照。
const SHOP_MALL_MAP = [
  ['雑貨イズムAmazon店',     'amazon'],
  ['雑貨イズム楽天市場店',   'rakuten'],
  ['雑貨イズムYahoo!店',     'yahoo'],   // Yahoo は個別保持 (無ければ rakuten にフォールバック)
  ['LINE ギフト',            'rakuten'],
  ['雑貨イズムメルカリshops', 'rakuten'],
  ['雑貨イズムauPay!店',     'rakuten'],
  ['雑貨イズムQoo10店',      'rakuten'],
];
const MALL_GROUPS = ['amazon', 'rakuten', 'yahoo'];
const DEFAULT_MALL_GROUP = 'rakuten'; // 未知ショップ・ルール無し時のフォールバック

const COMBO_KEY_VERSION = 1;
const NEKOPOS_PACKING = ['pasline3', 'pasline2', 'meltline']; // ネコポスで許される梱包機
const HOKKAIDO_OKINAWA = ['北海道', '沖縄県', '沖縄']; // 地域特例

// ───────────────────────── 共通ユーティリティ ─────────────────────────

export function utcIsoNow() { return new Date().toISOString(); }

// 正規化: 文字列化→trim→NFKC→連続空白除去→NULL/''統一。全キー(sku/combo/都道府県)で共用。
export function norm(s) {
  if (s == null) return '';
  let v = String(s).normalize('NFKC').trim().replace(/\s+/g, ' ');
  return v;
}
function normKeyPart(s) { return norm(s).toLowerCase(); }

// SKU キー: 商品コード::色::サイズ (色サイズ未設定は空)。比較は小文字化で揺れ吸収。
export function buildSkuKey(productCode, colorName, sizeName) {
  return [normKeyPart(productCode), normKeyPart(colorName), normKeyPart(sizeName)].join('::');
}
export function normProductCode(code) { return normKeyPart(code); }

// ───────────────────────── スキーマ ─────────────────────────

let schemaReady = false;
export function ensureSchema() {
  const db = getMirrorDB();
  if (schemaReady) return db;

  db.exec(`
    CREATE TABLE IF NOT EXISTS pd_shipping_method (
      code TEXT PRIMARY KEY,
      name_csv TEXT NOT NULL,
      carrier_id TEXT NOT NULL,
      ne_carrier_id TEXT NOT NULL,
      yamato_bill_code TEXT,
      rank INTEGER,
      is_locked INTEGER NOT NULL DEFAULT 0,
      is_nekopos INTEGER NOT NULL DEFAULT 0
    );
    CREATE TABLE IF NOT EXISTS pd_packing_machine (
      code TEXT PRIMARY KEY,
      name_csv TEXT NOT NULL,
      sort_order INTEGER
    );
    CREATE TABLE IF NOT EXISTS pd_shop_mall_map (
      shop_name TEXT PRIMARY KEY,
      mall_group TEXT NOT NULL
    );
    CREATE TABLE IF NOT EXISTS pd_shipping_rule (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      sku_key TEXT NOT NULL,
      product_code TEXT NOT NULL,
      mall_group TEXT NOT NULL,
      qty_min INTEGER NOT NULL,
      qty_max INTEGER,
      shipping_method_code TEXT NOT NULL REFERENCES pd_shipping_method(code),
      packing_machine_code TEXT NOT NULL REFERENCES pd_packing_machine(code),
      updated_at TEXT, updated_by TEXT,
      UNIQUE (sku_key, mall_group, qty_min),
      CHECK (qty_min >= 1),
      CHECK (qty_max IS NULL OR qty_max >= qty_min),
      CHECK ( packing_machine_code='manual' OR shipping_method_code='nekopos' )
    );
    CREATE INDEX IF NOT EXISTS idx_pd_rule_pc ON pd_shipping_rule(product_code);
    CREATE INDEX IF NOT EXISTS idx_pd_rule_lookup ON pd_shipping_rule(sku_key, mall_group);

    CREATE TABLE IF NOT EXISTS pd_assort_decision (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      combo_key TEXT NOT NULL,
      combo_key_version INTEGER NOT NULL DEFAULT 1,
      combo_detail TEXT,
      shipping_method_code TEXT NOT NULL REFERENCES pd_shipping_method(code),
      packing_machine_code TEXT NOT NULL REFERENCES pd_packing_machine(code),
      is_active INTEGER NOT NULL DEFAULT 1,
      decided_by TEXT, decided_at TEXT,
      CHECK ( packing_machine_code='manual' OR shipping_method_code='nekopos' )
    );
    CREATE UNIQUE INDEX IF NOT EXISTS idx_pd_assort_active
      ON pd_assort_decision(combo_key, combo_key_version) WHERE is_active=1;
    CREATE TABLE IF NOT EXISTS pd_assort_decision_item (
      decision_id INTEGER NOT NULL REFERENCES pd_assort_decision(id),
      sku_key TEXT NOT NULL,
      qty INTEGER NOT NULL,
      PRIMARY KEY (decision_id, sku_key)
    );

    -- アソート学習の利用ログ (非PII)。combo_key と NE注文番号 を紐付け、注文番号から学習を辿れるようにする。
    -- 住所/氏名/電話は持たない。古いものは purge。
    CREATE TABLE IF NOT EXISTS pd_assort_usage (
      combo_key TEXT NOT NULL,
      order_no TEXT NOT NULL,      -- 注文番号 (モール)
      uketsuke_no TEXT,           -- 受注番号/伝票番号 (NE連番)
      shop_name TEXT,
      applied_at TEXT,
      PRIMARY KEY (combo_key, order_no)
    );
    CREATE INDEX IF NOT EXISTS idx_pd_usage_order ON pd_assort_usage(order_no);
    CREATE INDEX IF NOT EXISTS idx_pd_usage_uke ON pd_assort_usage(uketsuke_no);

    CREATE TABLE IF NOT EXISTS pd_import_batch (
      batch_id TEXT PRIMARY KEY,
      filename TEXT,
      uploaded_by TEXT, uploaded_at TEXT,
      status TEXT NOT NULL DEFAULT 'classifying',
      row_count INTEGER, order_count INTEGER,
      header_json TEXT,
      expires_at TEXT
    );
    CREATE TABLE IF NOT EXISTS pd_import_line (
      batch_id TEXT NOT NULL REFERENCES pd_import_batch(batch_id),
      row_no INTEGER NOT NULL,
      order_no TEXT, shop_name TEXT, mall_group TEXT,
      product_code TEXT, sku_key TEXT, qty INTEGER, pref TEXT,
      order_type TEXT,
      shipping_method_code TEXT, packing_machine_code TEXT,
      row_status TEXT, reason_code TEXT,
      raw_cols TEXT,
      PRIMARY KEY (batch_id, row_no)
    );
    CREATE INDEX IF NOT EXISTS idx_pd_line_order ON pd_import_line(batch_id, order_no);

    CREATE TABLE IF NOT EXISTS pd_audit_log (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      ts TEXT, who TEXT, action TEXT, target TEXT, detail TEXT
    );
  `);

  // コードテーブルの seed (INSERT OR IGNORE: 既存は壊さない)
  const insSM = db.prepare(`INSERT OR IGNORE INTO pd_shipping_method
    (code,name_csv,carrier_id,ne_carrier_id,yamato_bill_code,rank,is_locked,is_nekopos)
    VALUES (?,?,?,?,?,?,?,?)`);
  for (const r of SHIPPING_METHODS) insSM.run(...r);
  const insPM = db.prepare(`INSERT OR IGNORE INTO pd_packing_machine (code,name_csv,sort_order) VALUES (?,?,?)`);
  for (const r of PACKING_MACHINES) insPM.run(...r);
  const insMap = db.prepare(`INSERT OR IGNORE INTO pd_shop_mall_map (shop_name,mall_group) VALUES (?,?)`);
  for (const r of SHOP_MALL_MAP) insMap.run(...r);

  schemaReady = true;
  return db;
}

function audit(action, target, detail, who) {
  try {
    getMirrorDB().prepare(`INSERT INTO pd_audit_log (ts,who,action,target,detail) VALUES (?,?,?,?,?)`)
      .run(utcIsoNow(), who || null, action, target || null, detail ? JSON.stringify(detail) : null);
  } catch (e) { console.error('[packing] audit', e.message); }
}

// ───────────────────────── マスタ参照(キャッシュ) ─────────────────────────

function shippingMethodMap() {
  const db = ensureSchema();
  const m = new Map();
  for (const r of db.prepare(`SELECT * FROM pd_shipping_method`).all()) m.set(r.code, r);
  return m;
}
function mallGroupOf(shopName) {
  const db = ensureSchema();
  const row = db.prepare(`SELECT mall_group FROM pd_shop_mall_map WHERE shop_name=?`).get(norm(shopName));
  return row ? row.mall_group : DEFAULT_MALL_GROUP;
}

// ───────────────────────── 連動列の一括導出 (純関数の核) ─────────────────────────

/**
 * shipping_method_code + packing_machine_code から CSV 連動5列の書き込み値を一括導出。
 * ネコポス以外は梱包機を必ず manual に矯正 (整合保証)。途中で34だけ書く経路を作らないための単一窓口。
 * @returns {{ shipping_method_code, packing_machine_code, cols: {34,17,76,91,92} }}
 */
export function finalizeLine(shipping_method_code, packing_machine_code, smMap = shippingMethodMap()) {
  const sm = smMap.get(shipping_method_code);
  if (!sm) throw new Error('未知の配送方法コード: ' + shipping_method_code);
  let pm = packing_machine_code;
  if (shipping_method_code !== 'nekopos' || !NEKOPOS_PACKING.includes(pm)) pm = 'manual';
  const db = ensureSchema();
  const pmRow = db.prepare(`SELECT name_csv FROM pd_packing_machine WHERE code=?`).get(pm);
  return {
    shipping_method_code,
    packing_machine_code: pm,
    cols: {
      [COL.shippingMethod]: sm.name_csv,
      [COL.carrierId]: sm.carrier_id,
      [COL.neCarrierId]: sm.ne_carrier_id,
      [COL.yamatoBill]: sm.yamato_bill_code || '',
      [COL.packing]: pmRow ? pmRow.name_csv : '手動出荷',
    },
  };
}

// 上位ルール: rank 最大の配送方法code (rank が NULL の階層外は除外)
function upperShipping(codes, smMap) {
  let best = null, bestRank = -1;
  for (const c of codes) {
    const sm = smMap.get(c);
    if (!sm || sm.rank == null) continue;
    if (sm.rank > bestRank) { bestRank = sm.rank; best = c; }
  }
  return best;
}

// ───────────────────────── 区間引き当て(②) ─────────────────────────

/**
 * sku_key×mall_group×qty で配送ルールを引く。mall_group 一致が無ければ rakuten(default) を参照。
 * @returns rule行 or null(未登録/帯の穴 → 呼び出し側で要判断)
 */
function lookupRule(sku_key, mall_group, qty) {
  const db = ensureSchema();
  const q = `SELECT * FROM pd_shipping_rule
    WHERE sku_key=? AND mall_group=? AND qty_min<=? AND (qty_max IS NULL OR qty_max>=?)
    ORDER BY qty_min DESC LIMIT 1`;
  let r = db.prepare(q).get(sku_key, mall_group, qty, qty);
  if (!r && mall_group !== DEFAULT_MALL_GROUP) {
    r = db.prepare(q).get(sku_key, DEFAULT_MALL_GROUP, qty, qty);
  }
  return r || null;
}

// product_code に①件でも有効ルールがあるか (未登録判定の補助)
function hasAnyRule(product_code) {
  const db = ensureSchema();
  return !!db.prepare(`SELECT 1 FROM pd_shipping_rule WHERE product_code=? LIMIT 1`).get(product_code);
}

// ───────────────────────── アソート学習(③) ─────────────────────────

// combo_key: 正規化明細[{sku_key,qty}]を sku_key 昇順ソート→JSON→SHA-256
export function comboKeyOf(items) {
  const norm2 = items
    .map((i) => ({ s: i.sku_key, q: Number(i.qty) }))
    .filter((i) => i.s && i.q > 0)
    .sort((a, b) => (a.s < b.s ? -1 : a.s > b.s ? 1 : 0));
  const json = JSON.stringify(norm2);
  return crypto.createHash('sha256').update(json).digest('hex');
}
function lookupAssort(combo_key) {
  const db = ensureSchema();
  return db.prepare(
    `SELECT * FROM pd_assort_decision WHERE combo_key=? AND combo_key_version=? AND is_active=1 LIMIT 1`
  ).get(combo_key, COMBO_KEY_VERSION) || null;
}

/**
 * アソート判断の確定(学習登録)。同一 combo_key の既存 active は is_active=0 にして履歴を残す(SCD2)。承認なし(誰でも確定)。
 */
export function saveAssortDecision({ items, combo_detail, shipping_method_code, packing_machine_code }, user) {
  const db = ensureSchema();
  const combo_key = comboKeyOf(items);
  const fin = finalizeLine(shipping_method_code, packing_machine_code); // 整合矯正
  const now = utcIsoNow();
  const tx = db.transaction(() => {
    db.prepare(`UPDATE pd_assort_decision SET is_active=0 WHERE combo_key=? AND combo_key_version=? AND is_active=1`)
      .run(combo_key, COMBO_KEY_VERSION);
    const info = db.prepare(`INSERT INTO pd_assort_decision
      (combo_key,combo_key_version,combo_detail,shipping_method_code,packing_machine_code,is_active,decided_by,decided_at)
      VALUES (?,?,?,?,?,1,?,?)`).run(
      combo_key, COMBO_KEY_VERSION, combo_detail ? JSON.stringify(combo_detail) : null,
      fin.shipping_method_code, fin.packing_machine_code, user || null, now);
    const did = info.lastInsertRowid;
    const insItem = db.prepare(`INSERT OR IGNORE INTO pd_assort_decision_item (decision_id,sku_key,qty) VALUES (?,?,?)`);
    // sku_key で合算してから登録
    const merged = new Map();
    for (const it of items) {
      const k = it.sku_key; if (!k) continue;
      merged.set(k, (merged.get(k) || 0) + Number(it.qty || 0));
    }
    for (const [k, q] of merged) insItem.run(did, k, q);
  });
  tx();
  audit('assort_decide', combo_key, { shipping_method_code: fin.shipping_method_code, packing_machine_code: fin.packing_machine_code }, user);
  return { combo_key };
}

// ───────────────────────── 判定エンジン(1注文の分類) ─────────────────────────

/**
 * 1伝票(同一 shop+注文番号)の明細群を判定。
 * lines: [{ sku_key, product_code, qty, pref, ... }]  (同一伝票)
 * @returns { shipping_method_code, packing_machine_code, row_status, reason_code, order_type }
 *   row_status: 'auto' | '要判断'
 */
export function classifyOrder(lines, smMap = shippingMethodMap()) {
  // 同一 SKU を合算した正規化中間
  const bySku = new Map();
  for (const l of lines) {
    if (!l.sku_key || !(l.qty > 0)) continue; // qty<=0/空は除外
    const cur = bySku.get(l.sku_key) || { sku_key: l.sku_key, product_code: l.product_code, qty: 0 };
    cur.qty += l.qty;
    bySku.set(l.sku_key, cur);
  }
  const skus = [...bySku.values()];
  const mall_group = lines[0]?.mall_group || DEFAULT_MALL_GROUP;
  const pref = norm(lines[0]?.pref);
  const isRegionSpecial = HOKKAIDO_OKINAWA.includes(pref);

  // (1) AES lock: いずれかの行が AES(現行配送方法) なら触らない
  const curMethodCodes = lines.map((l) => l.cur_method_code).filter(Boolean);
  if (curMethodCodes.includes('aes')) {
    return { order_type: 'aes', shipping_method_code: 'aes', packing_machine_code: 'manual', row_status: 'auto', reason_code: 'aes_locked' };
  }

  if (skus.length === 0) {
    return { order_type: 'empty', shipping_method_code: null, packing_machine_code: null, row_status: '要判断', reason_code: 'no_valid_line' };
  }

  let result;
  if (skus.length === 1) {
    // (2) 単品 / 同一商品複数
    const s = skus[0];
    if (!hasAnyRule(s.product_code)) {
      result = { order_type: 'single', shipping_method_code: null, packing_machine_code: null, row_status: '要判断', reason_code: 'unregistered' };
    } else {
      const rule = lookupRule(s.sku_key, mall_group, s.qty);
      if (!rule) {
        result = { order_type: 'single', shipping_method_code: null, packing_machine_code: null, row_status: '要判断', reason_code: 'qty_gap' };
      } else {
        result = { order_type: 'single', shipping_method_code: rule.shipping_method_code, packing_machine_code: rule.packing_machine_code, row_status: 'auto', reason_code: null };
      }
    }
  } else {
    // (3) アソート: ③照会 → ヒットで自動 / 初出は上位ルールでサジェスト + 要判断
    const combo_key = comboKeyOf(skus);
    const hit = lookupAssort(combo_key);
    if (hit) {
      result = { order_type: 'assort', combo_key, shipping_method_code: hit.shipping_method_code, packing_machine_code: hit.packing_machine_code, row_status: 'auto', reason_code: 'assort_learned' };
    } else {
      // 各SKUの単品配送方法→上位を提案。未登録/穴があれば要判断のまま提案だけ。
      const codes = [];
      for (const s of skus) {
        const r = lookupRule(s.sku_key, mall_group, s.qty);
        if (r) codes.push(r.shipping_method_code);
      }
      const suggest = upperShipping(codes, smMap);
      result = { order_type: 'assort', combo_key, shipping_method_code: null, packing_machine_code: 'manual',
        suggest_shipping_method_code: suggest || null, row_status: '要判断', reason_code: 'assort_new' };
    }
  }

  // (4) 地域特例: 沖縄・北海道 → letterpack 上書き(梱包機は manual に落ちる)
  if (isRegionSpecial && result.row_status === 'auto' && result.shipping_method_code && result.shipping_method_code !== 'aes') {
    result = { ...result, shipping_method_code: 'letterpack', packing_machine_code: 'manual', reason_code: 'region_letterpack' };
  } else if (isRegionSpecial && result.row_status === '要判断') {
    result = { ...result, suggest_shipping_method_code: 'letterpack' };
  }
  return result;
}

// ───────────────────────── アソート学習の利用ログ(注文番号⇄combo) ─────────────────────────

// combo_key と 注文番号/受注番号 を紐付け(非PII)。同一(combo,注文番号)は上書き。
export function recordAssortUsage({ combo_key, order_no, uketsuke_no, shop_name }) {
  if (!combo_key || !order_no) return;
  const db = ensureSchema();
  db.prepare(`INSERT INTO pd_assort_usage (combo_key, order_no, uketsuke_no, shop_name, applied_at)
    VALUES (?,?,?,?,?)
    ON CONFLICT(combo_key, order_no) DO UPDATE SET uketsuke_no=excluded.uketsuke_no, shop_name=excluded.shop_name, applied_at=excluded.applied_at`)
    .run(combo_key, order_no, uketsuke_no || null, shop_name || null, utcIsoNow());
}

// 注文番号 or 受注番号 で combo_key を引く
export function comboKeysByOrderRef(q) {
  const db = ensureSchema();
  const like = `%${norm(q)}%`;
  return db.prepare(`SELECT DISTINCT combo_key FROM pd_assort_usage WHERE order_no LIKE ? OR uketsuke_no LIKE ? LIMIT 200`).all(like, like).map(r => r.combo_key);
}

// アクティブな学習1件 (combo_key) を明細・利用注文つきで取得
export function getAssortByCombo(combo_key) {
  const db = ensureSchema();
  const d = db.prepare(`SELECT * FROM pd_assort_decision WHERE combo_key=? AND combo_key_version=? AND is_active=1`).get(combo_key, COMBO_KEY_VERSION);
  if (!d) return null;
  const items = db.prepare(`SELECT sku_key, qty FROM pd_assort_decision_item WHERE decision_id=?`).all(d.id);
  const usage = db.prepare(`SELECT order_no, uketsuke_no, shop_name, applied_at FROM pd_assort_usage WHERE combo_key=? ORDER BY applied_at DESC LIMIT 50`).all(combo_key);
  return { ...d, items, usage };
}

// 30日より古い利用ログを掃除
export function purgeOldUsage() {
  const db = ensureSchema();
  const cutoff = new Date(Date.now() - 30 * 86400 * 1000).toISOString();
  return db.prepare(`DELETE FROM pd_assort_usage WHERE applied_at < ?`).run(cutoff).changes;
}

// ───────────────────────── 未登録チェック(取扱中×非セット×②未登録) ─────────────────────────

export function listUnregistered() {
  const db = ensureSchema();
  // セット商品は対象外。商品区分='セット' だけでは NE 上 単品扱いのセットを取りこぼすため、
  // 「セット構成品数>0」と「セット構成品マスタに親として存在」も併せて除外する (3シグナル)。
  // NOT EXISTS で NULL 罠回避。商品コードは比較前に lower(trim()) で正規化整合。
  return db.prepare(`
    SELECT mp.商品コード AS product_code, mp.商品名 AS product_name
      FROM mirror_products mp
     WHERE mp.取扱区分 = '取扱中'
       AND mp.商品区分 <> 'セット'
       AND COALESCE(mp.セット構成品数, 0) = 0
       AND NOT EXISTS (
         SELECT 1 FROM mirror_set_components sc WHERE sc.セット商品コード = mp.商品コード
       )
       AND NOT EXISTS (
         SELECT 1 FROM pd_shipping_rule r
          WHERE r.product_code = lower(trim(mp.商品コード))
       )
     ORDER BY mp.商品コード
     LIMIT 2000
  `).all();
}

// ミラー鮮度 (最終同期時刻があれば返す。無ければ null)
export function mirrorFreshness() {
  const db = ensureSchema();
  try {
    const row = db.prepare(`SELECT MAX(updated_at) AS last FROM mirror_sync_status`).get();
    return row ? row.last : null;
  } catch { return null; }
}

export { COMBO_KEY_VERSION, shippingMethodMap, mallGroupOf, MALL_GROUPS, DEFAULT_MALL_GROUP };
