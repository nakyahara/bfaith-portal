/**
 * purchase-orders 発注計算エンジン
 *
 * 旧スプレッドシート「発注対象商品」の数式を基礎にする (在庫の定義のみ変更、下記※):
 *   在庫月数 L      = (総在庫数 + 注残数) / 30日販売数合計   (販売0なら0)
 *     ※在庫は引当済みを含む総在庫 (旧シートは引当なしだったが、FBAに送る前の
 *       引当済み準備在庫が集計から漏れて過剰発注になるため変更。中原さん指示 2026-08-03。
 *       日中のロジザード在庫CSV「在庫数(引当数を含む)」とも整合する)
 *   在庫定数 O      = M<=1→0.5, 1<M<=2→1, 2<M<=3→2, M>3→3   (M=推奨保有月数)
 *   目標月数 P      = M + O
 *   発注対象 (v2)   = 取扱区分='取扱中' かつ V > 0 かつ M > 0 かつ S + B <= M × V
 *     ※旧式 (v1) は 0 < L <= M。在庫0・注残0 で L=0 になると「下限」を割って要発注から消え、
 *       売れていて先に切れた商品ほど掘り起こしに落ちていた (2026-08-30 全数調査: 203件・月204万円)。
 *       v2 は V>0 かつ S+B>0 の領域で v1 と同値。差分は S+B=0 のみ = 要件定義原文
 *       「掘り起こし = 在庫も注残もゼロ / 販売実績消失」に合わせた修正 (中原さん決定 2026-08-30)。
 *       M>0 ガードが無いと S+B=0 <= 0×V が真になり M未設定の在庫0商品まで要発注化するため必須 (Codex R1)。
 *       設定 target_rule='v1' で旧式へ即時ロールバックできる。
 *   推奨発注量      = lots = (P×V − (S+B))/N を N(発注ロット単位)で丸め  (= (P-L)×V/N と同値)
 *                     lots > 1 → ROUND(lots)*N / lots <= 1 → ROUNDUP(lots)*N (最低1ロット)
 *   掘り起こし対象 (v2) = 取扱中 かつ 在庫0 かつ 注残0 かつ 販売0 (販売実績消失。中原さん定義 2026-07-14:
 *     他社の値下げで価格が合わず仕入を控えた商品を、一定期間後に再販できるか調べるためのステータス)
 *     ※旧シートは取扱中止も掘り起こしに含めていたが、本アプリは「取扱中」のみに絞る (ノイズ除去)
 *     ※在庫があって販売0の商品は掘り起こしではない → ついで買い候補の末尾に回す
 *     ※v1 は販売を見ずに 在庫0・注残0 を掘り起こしにしていた (売れている欠品品もここに落ちていた)
 *   在庫月数 L      = 販売0 のとき null (「在庫月数を定義できない」。在庫0で販売ありの本当の0ヶ月と区別する、Codex R2)
 *   推奨保有月数 M 未設定 (<=0) で売れている商品 = holdMonthsMissing (設定不備。要発注にしない)
 *   セット商品 (商品区分='セット') は対象外 = 一覧にも出さない (在庫・発注は構成品側で管理)
 *
 * データソース: mirror_pml_snapshot_rows (published run) + po_* マスタ。
 */
import { getDB, normSupplierCode, normProductCode } from './db.js';
import { getSetting, audit } from './ledger.js';

const PML_COLS = [
  '商品コード', '商品名', '仕入先', '取扱区分', '商品区分', '売上分類',
  '総在庫数', 'FBA在庫数', '注残数', '販売数7日_合計', '販売数30日_合計',
  '発注ロット単位', '推奨保有月数', '売価', '原価', '最終仕入日', '登録日',
];

const num = v => {
  if (v == null || v === '') return 0;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
};

/** 在庫定数 (推奨保有月数に応じた上乗せバッファ月数) — 旧シート IFS を忠実移植 */
export function stockConstant(m) {
  if (m <= 1) return 0.5;
  if (m <= 2) return 1;
  if (m <= 3) return 2;
  return 3;
}

/** 要発注判定ルール (po_settings target_rule)。既定 'v2'。'v1' = 旧式 (0 < L <= M) へのロールバック用 */
export function targetRule() {
  return getSetting('target_rule') === 'v1' ? 'v1' : 'v2';
}

/**
 * 1商品の発注判定。旧シートの1行分。backOrderOverride を渡すとNE CSVの注残数の代わりに使う (アプリ台帳の残数)。
 * rule: 'v2' (既定) | 'v1'。両ルールの判定 (targetV1/targetV2) を常に返し、ルール切替の影響件数を観測できるようにする。
 */
export function computeProduct(r, backOrderOverride, rule = 'v2') {
  const S = num(r['総在庫数']); // 引当済み含む (FBA送付前の準備在庫も自社在庫として数える)
  const B = backOrderOverride != null ? backOrderOverride : num(r['注残数']);
  const V = num(r['販売数30日_合計']);
  const M = num(r['推奨保有月数']);
  const N = num(r['発注ロット単位']);
  const active = String(r['取扱区分'] || '') === '取扱中';
  const salesDefined = V > 0;
  // 在庫+注残が負 (NE の引当超過・取込異常) は「在庫0」として扱う。v1 は L<0 で黙って対象外にしていたが、
  // 実態は欠品なので v2 では対象にし、推奨量は 0 起点で計算する (負をそのまま使うと推奨量が膨らむ、Codex PR1-R1 Low)
  const stockNegative = S + B < 0;
  const SB = stockNegative ? 0 : S + B;
  const stockMonths = salesDefined ? SB / V : null; // L (販売0は未定義)
  const targetV1 = active && salesDefined && stockMonths > 0 && stockMonths <= M;
  const targetV2 = active && salesDefined && M > 0 && SB <= M * V;
  const isTarget = rule === 'v1' ? targetV1 : targetV2;
  // 売れているのに推奨保有月数が未設定 → どのルールでも要発注に出ない設定不備 (管理画面で埋める)
  const holdMonthsMissing = active && salesDefined && !(M > 0);
  // 推奨量は v2 判定で常に計算する (v1 ⊂ v2 なので v1 の対象商品も同じ式)。
  // recQtyV2 はルール切替の影響額 (ruleStats) 用で、現在のルールに依存させない (Codex PR1-R1 Medium)
  let recQtyV2 = null;
  if (targetV2 && N > 0) {
    const P = M + stockConstant(M);
    const lots = (P * V - SB) / N; // = (P − L) × V / N。target なら SB <= M×V なので必ず正 (最低1ロット)
    recQtyV2 = lots > 1 ? Math.round(lots) * N : Math.ceil(lots) * N;
  }
  const recQty = isTarget ? recQtyV2 : null;
  return {
    code: r['商品コード'],
    key: normProductCode(r['商品コード']),
    name: r['商品名'] || '',
    supplierCode: normSupplierCode(r['仕入先']),
    active,
    salesClass: r['売上分類'] == null ? '' : String(r['売上分類']),
    stock: S,
    backOrder: B,
    sales30: V,
    sales7: num(r['販売数7日_合計']),
    lot: N,
    holdMonths: M,
    price: num(r['売価']),
    cost: num(r['原価']),
    lastPurchase: r['最終仕入日'] || '',
    stockMonths,
    salesDefined,
    isTarget,
    targetV1,
    targetV2,
    targetReason: isTarget ? ['total'] : [],
    holdMonthsMissing,
    stockNegative,
    recQty,
    recQtyV2,
    // 掘り起こし = 取扱中かつ在庫0・注残0・販売0 (仕入を控えた商品の再販調査。販売0でも在庫があれば対象外。
    // v2 では売れている欠品品は要発注へ行くので掘り起こしには落とさない)
    isHorikoshi: active && S === 0 && B === 0 && (rule === 'v1' || !salesDefined),
  };
}

/** published PML を読む (pub+rows を1トランザクションで) */
export function loadPml() {
  const db = getDB();
  return db.transaction(() => {
    const pub = db.prepare('SELECT * FROM mirror_pml_published WHERE id=1').get();
    if (!pub) return { pub: null, rows: [] };
    const rows = db.prepare(
      `SELECT ${PML_COLS.join(', ')} FROM mirror_pml_snapshot_rows WHERE run_id=?`
    ).all(pub.run_id);
    return { pub, rows };
  })();
}

// ─── ロジザード在庫 mirror 自動反映 (2026-08-17 中原さん指示) ───
// mirror_logizard_stock は miniPC 毎時取込 (9〜18時) の複製 (同じ warehouse-mirror.db 内)。
// captured_at が進んでいたら、手動CSV取込と同じ「在庫数のみ上書き」のオーバーレイを自動で作り直す。
// 発注画面を開けば常に最新 (最大1時間前) の在庫で計算される — 手動アップロードは miniPC 停止時の
// バックアップとして残す (新しい方が勝つ: captured_at 比較の後勝ち)。
//
// やらないこと (意図的):
// - bumpCycleReset しない — ✅発注確定済み・×非表示のサイクルは人がデータ更新したときだけ進める
//   (毎時リセットされたら発注管理の目印が全部消えるため)
// - po_product_code_canonical を更新しない — mirror の商品IDは小文字化済みで
//   ロジザード登録表記 (大小) が失われている (表記の正はCSV手動取込・NE取込のみ)

/** mirror_logizard_stock の最新 captured_at (表が無い環境=init fail-soft でも落とさない)。
 *  MAXは文字列順だが、mirror は毎時「全置換」で全行が同一 captured_at (単一世代) かつ
 *  書込み元 (miniPC sync-to-render.js) は toISOString 固定のため表記ゆれは混在しない (Codex LZM-R3 Low回答) */
export function logizardMirrorLatestCapture(db) {
  try {
    const r = db.prepare('SELECT MAX(captured_at) AS c FROM mirror_logizard_stock').get();
    return r && r.c ? r.c : null;
  } catch { return null; }
}

let lzMirrorLastCheckMs = 0;

/** 時刻文字列 a が b より新しいか (Date.parse可能ならepoch比較、不能なら文字列比較にフォールバック。
 *  ISO表記ゆれ (+09:00等) でも辞書順に頼らない — Codex LZM-R1 Low) */
function newerThan(a, b) {
  const ta = Date.parse(a), tb = Date.parse(b);
  if (Number.isFinite(ta) && Number.isFinite(tb)) return ta > tb;
  return String(a) > String(b);
}

/**
 * mirror が進んでいたら在庫オーバーレイを自動更新する (computeAll の頭で毎回呼ぶ)。
 * チェック自体は MAX(captured_at) 1本 + 60秒throttle で軽い。失敗しても発注画面は落とさない
 * (fail-soft: 古いオーバーレイのまま表示され、鮮度表示の⚠️で気づける)。
 *
 * 夜間〜翌朝の挙動 (意図した設計・Codex LZM-R1 High回答): 前日18時のmirrorオーバーレイは
 * 翌朝のPML同期 (src_ne_products_synced_at) で applied=false になり「朝同期」の在庫へ戻る。
 * 朝のNE在庫の方が18時のロジザード在庫より新しいので、これが正 (既存オーバーレイ思想と同じ)。
 * 9時以降の毎時更新で新しい captured_at が届いたら自動反映が再開する。
 */
export function maybeRefreshFromLogizardMirror() {
  const intervalMs = Number(process.env.PO_LZ_MIRROR_CHECK_INTERVAL_MS ?? 60000);
  if (intervalMs > 0 && Date.now() - lzMirrorLastCheckMs < intervalMs) return;
  lzMirrorLastCheckMs = Date.now();
  try {
    const db = getDB();
    // 世代チェック〜集計〜置換を単一の即時トランザクションで行う (mirror世代・meta・suppression を
    // 同一スナップショットで読む。チェックと書込みの間に毎時push/手動取込/解除が割り込んで
    // 旧capに新世代の集計を紐付けたり負数検査を迂回したりしない — Codex LZM-R1/R3 Medium)。
    // 全体で実測50ms未満・60秒throttle付きのためロック保持は問題にならない
    db.transaction(() => {
    const cap = logizardMirrorLatestCapture(db);
    if (!cap) return; // mirror未着 (初回push前・表なし)
    // 適用してよい状態か
    const shouldApply = () => {
      // mirror が朝のPML同期より古いなら適用しない — 朝のNE在庫の方が新しい (overlay無し=初回や
      // 解除後でも、前日18時のmirrorで今朝の在庫を上書きしない。Codex LZM-R2 High)
      const pubSync = db.prepare('SELECT src_ne_products_synced_at FROM mirror_pml_published WHERE id=1').get();
      if (pubSync && pubSync.src_ne_products_synced_at && !newerThan(cap, pubSync.src_ne_products_synced_at)) return false;
      const meta = db.prepare('SELECT * FROM po_ne_overlay_meta WHERE id=1').get();
      // 後勝ち: 手動取込 (captured_at=取込時刻) の方が新しい間は上書きしない
      const metaCap = meta ? (meta.captured_at || meta.uploaded_at) : null;
      if (metaCap && !newerThan(cap, metaCap)) return false;
      // 「CSV取込を解除」直後は同じ世代では復活させない (次の毎時更新で自動再開)
      const suppressed = getSetting('po_lz_mirror_suppress_capture');
      if (suppressed && !newerThan(cap, suppressed)) return false;
      // NEマスタCSV (全項目上書き) が有効に適用されている間はスキップ — 在庫だけの自動反映で
      // その日の原価・取扱区分の上書きを消さない。翌朝のPML同期で applied=false になったら再開
      if (meta && (meta.source == null || meta.source === 'ne')) {
        const pub = db.prepare('SELECT src_ne_products_synced_at FROM mirror_pml_published WHERE id=1').get();
        const a = Date.parse(meta.uploaded_at);
        const b = pub && pub.src_ne_products_synced_at ? Date.parse(pub.src_ne_products_synced_at) : NaN;
        const neActive = !(Number.isFinite(a) && Number.isFinite(b) && a < b); // loadPmlMerged の applied と同じ判定
        if (neActive) return false;
      }
      return true;
    };
    if (!shouldApply()) return;
    // 手動CSV経路の stock<0 拒否と同等の防衛 (mirror受信側は負数を拒否しない)。SUM後だとロケ間で
    // 相殺されて見逃すため行単位で検査し、良品以外の行の負数も見逃さない (検証が除外より先 — Codex LZM-R2 Medium)
    const negRow = db.prepare('SELECT 商品ID FROM mirror_logizard_stock WHERE 在庫数 < 0 OR 引当数 < 0 LIMIT 1').get();
    if (negRow) {
      console.warn(`[po] logizard mirror自動反映をスキップ: 負数在庫の行あり (${negRow.商品ID}・mirrorデータ異常の疑い)`);
      return;
    }
    // 集計はSQL一発 (手動CSV経路と同じルール: 良品のみ・商品IDごとにロケ横断SUM)
    const agg = db.prepare(`SELECT 商品ID AS code, SUM(在庫数) AS stock, SUM(引当数) AS alloc
      FROM mirror_logizard_stock WHERE 品質区分名='良品' GROUP BY 商品ID`).all();
    const byKey = new Map();
    for (const r of agg) {
      const key = normProductCode(r.code);
      if (!key) continue;
      const cur = byKey.get(key) || { code: r.code, stock: 0, alloc: 0 };
      cur.stock += r.stock;
      cur.alloc += r.alloc == null ? 0 : r.alloc;
      byKey.set(key, cur);
    }
    // 異常データガード: mirror が極端に小さい (途中欠け等) 場合に大量の在庫0上書きをしない。
    // 手動経路のプレビュー確認に相当する安全弁 (実データ約2,900商品に対して既定500)
    const minProducts = Math.max(1, Number(process.env.PO_LZ_MIRROR_MIN_PRODUCTS) || 500);
    if (byKey.size < minProducts) {
      console.warn(`[po] logizard mirror自動反映をスキップ: 商品数${byKey.size} < 下限${minProducts} (mirror欠損の疑い)`);
      return;
    }
    // CSVに行が無い取扱中商品は「売り切れ=在庫0」で上書き (手動経路と同じ前提: 自社在庫は全てロジザードにある)
    const pmlActive = new Map(loadPml().rows.filter(r => String(r['取扱区分'] || '') === '取扱中')
      .map(r => [normProductCode(r['商品コード']), String(r['商品コード']).trim()]));
    const zeroFill = [...pmlActive].filter(([k]) => !byKey.has(k));
    const now = new Date().toISOString();
    db.prepare('DELETE FROM po_ne_overlay_rows').run();
    const ins = db.prepare('INSERT INTO po_ne_overlay_rows (product_key, product_code, 在庫数, 引当数) VALUES (?,?,?,?)');
    for (const [key, v] of byKey) ins.run(key, v.code, v.stock, v.alloc);
    for (const [key, code] of zeroFill) ins.run(key, code, 0, null);
    db.prepare(`INSERT INTO po_ne_overlay_meta (id, uploaded_at, row_count, filename, source, captured_at)
                VALUES (1,?,?,NULL,'logizard_mirror',?)
                ON CONFLICT(id) DO UPDATE SET uploaded_at=excluded.uploaded_at, row_count=excluded.row_count,
                  filename=excluded.filename, source=excluded.source, captured_at=excluded.captured_at`)
      .run(now, byKey.size + zeroFill.length, cap);
    audit(db, { actorType: 'system', actor: 'lz-mirror-auto', action: 'logizard_mirror_refresh', resource: 'ne_overlay',
      detail: { captured_at: cap, products: byKey.size, zeroFill: zeroFill.length } });
    }).immediate(); // 即時write lock取得 (読み→書きのロック昇格レースを避ける。markCycleFbaJobDone と同方式)
  } catch (e) {
    console.error('[po] logizard mirror自動反映エラー (発注画面は継続):', e.message);
  }
}

/** NE商品マスタCSVオーバーレイをロード */
export function loadNeOverlay() {
  const db = getDB();
  const meta = db.prepare('SELECT * FROM po_ne_overlay_meta WHERE id=1').get() || null;
  const rows = new Map();
  if (meta) {
    for (const r of db.prepare('SELECT * FROM po_ne_overlay_rows').all()) rows.set(r.product_key, r);
  }
  return { meta, rows };
}

/**
 * published PML に NEオーバーレイ (日中手動DLのNE商品マスタCSV) をマージして返す。
 * 上書き対象: 取扱区分 / 仕入先 / 原価 / 売価 / 発注ロット単位 / 最終仕入日 / 注残数(=NE発注残数) /
 *             総在庫数 (= NE在庫数 + PML.FBA在庫数。NE在庫数もロジザードCSVの
 *             「在庫数(引当数を含む)」も引当込みなので、朝のPML値と意味が揃う)
 * 据え置き: FBA在庫数・販売数・推奨保有月数 (PMLのまま)。
 * overlay が朝のNE同期より古い場合は自動で無視する (applied=false)。
 */
export function loadPmlMerged() {
  const db = getDB();
  return db.transaction(() => {
    const { pub, rows } = loadPml();
    const ov = loadNeOverlay();
    let overlay = null;
    let applied = false;
    if (ov.meta) {
      applied = true;
      if (pub && pub.src_ne_products_synced_at) {
        const a = Date.parse(ov.meta.uploaded_at);
        const b = Date.parse(pub.src_ne_products_synced_at);
        if (!Number.isNaN(a) && !Number.isNaN(b) && a < b) applied = false; // 朝同期の方が新しい
      }
      overlay = { uploaded_at: ov.meta.uploaded_at, row_count: ov.meta.row_count, filename: ov.meta.filename, source: ov.meta.source || 'ne', captured_at: ov.meta.captured_at || null, applied, mergedCount: 0 };
    }
    const merged = !applied ? rows : rows.map(r => {
      const o = ov.rows.get(normProductCode(r['商品コード']));
      if (!o) return r;
      overlay.mergedCount++;
      return {
        ...r,
        '取扱区分': o.取扱区分 != null && o.取扱区分 !== '' ? o.取扱区分 : r['取扱区分'],
        '仕入先': o.仕入先コード != null && o.仕入先コード !== '' ? o.仕入先コード : r['仕入先'],
        '原価': o.原価 != null ? o.原価 : r['原価'],
        '売価': o.売価 != null ? o.売価 : r['売価'],
        '発注ロット単位': o.発注ロット単位 != null ? o.発注ロット単位 : r['発注ロット単位'],
        '最終仕入日': o.最終仕入日 != null && o.最終仕入日 !== '' ? o.最終仕入日 : r['最終仕入日'],
        '注残数': o.発注残数 != null ? o.発注残数 : r['注残数'],
        '総在庫数': o.在庫数 != null ? o.在庫数 + num(r['FBA在庫数']) : r['総在庫数'],
      };
    });
    return { pub, rows: merged, overlay };
  })();
}

/** マスタ一式をロード */
export function loadMasters() {
  const db = getDB();
  const suppliers = new Map();
  for (const s of db.prepare('SELECT * FROM po_suppliers').all()) {
    suppliers.set(normSupplierCode(s.supplier_code), s);
  }
  const conditions = db.prepare('SELECT * FROM po_order_conditions ORDER BY condition_id').all();
  const materialGroups = new Map();
  for (const g of db.prepare('SELECT * FROM po_material_groups').all()) {
    materialGroups.set(g.group_id, g);
  }
  const attrs = new Map();
  for (const a of db.prepare('SELECT * FROM po_product_attrs').all()) {
    attrs.set(a.product_key, a);
  }
  const selectable = new Map();
  for (const s of db.prepare('SELECT * FROM po_selectable_products').all()) {
    selectable.set(s.product_key, s);
  }
  return { suppliers, conditions, materialGroups, attrs, selectable };
}

/** 選べるセット構成商品の既定最低在庫 (min_stock 未設定時) */
export const SELECTABLE_DEFAULT_MIN = 10;

/** 直近 issuedDays 日以内に発注確定済みの商品 → { product_key: {orderId, issuedAt, qty} } */
export function loadRecentIssued(issuedDays = 14) {
  const db = getDB();
  const since = new Date(Date.now() - issuedDays * 86400000).toISOString();
  // 移行PO (NE発注残初期取込) は除外: その数量はNE由来でPMLの注残数に反映済みのため、
  // 「発注済み (NE反映待ち)」バッジを付けると二重カウントになる
  const rows = db.prepare(`
    SELECT i.product_key, i.qty, o.id AS order_id, o.issued_at
    FROM po_order_items i JOIN po_orders o ON o.id = i.order_id
    WHERE o.status = 'issued' AND o.issued_at >= ? AND (o.origin IS NULL OR o.origin <> 'migration')
    ORDER BY o.issued_at ASC
  `).all(since);
  const map = new Map();
  for (const r of rows) map.set(r.product_key, { orderId: r.order_id, issuedAt: r.issued_at, qty: r.qty });
  return map;
}

/**
 * アプリ台帳の商品別発注残 (残数>0のオープン発注のみ、移行PO含む) → Map(product_key → 残数計)。
 * NE CSVの「発注残数」の代わりに要発注判定へ使う: 発注確定した瞬間に注残へ反映され、
 * NE CSV/朝同期の取込を待たずに要発注から消える (二重発注防止)。
 */
export function loadLedgerBackorders() {
  const db = getDB();
  // 集計ロジックは v_ledger_backorder_by_product に一本化 (PML正本ビュー v_pml_rows_authoritative・
  // 整合性検査と同一定義。tracked の正は issued_at >= tracking_started_at、Codex サイクルR1 High / SSoT化 2026-07-13)
  const map = new Map();
  for (const r of db.prepare('SELECT product_key, backorder_qty FROM v_ledger_backorder_by_product').all()) {
    map.set(r.product_key, r.backorder_qty);
  }
  return map;
}

/**
 * 商品別オープン注残の希望納期内訳 → Map(product_key → [{date:'YYYY-MM-DD', qty}] 日付昇順)。
 * 母集合は loadLedgerBackorders と同じ (v_ledger_backorder_requested_dates)。納期未指定の注残は含まれない。
 */
export function loadBackorderRequestedDates() {
  const db = getDB();
  const map = new Map();
  for (const r of db.prepare(
    'SELECT product_key, requested_date, remaining_qty FROM v_ledger_backorder_requested_dates ORDER BY requested_date'
  ).all()) {
    let arr = map.get(r.product_key);
    if (!arr) map.set(r.product_key, arr = []);
    arr.push({ date: r.requested_date, qty: r.remaining_qty });
  }
  return map;
}

/**
 * 全体計算。仕入先別に 要発注 / ついで買い候補 / 掘り起こし を仕分けする。
 * 戻り値の products は PML 全行の computeProduct 結果 (attrs 情報を付与済み)。
 * 注残はアプリ台帳 (loadLedgerBackorders) を正とする — NE CSVの発注残数は使わない。
 */
export function computeAll() {
  // 毎時mirrorが進んでいたら在庫オーバーレイを先に自動更新 (60秒throttle+fail-soft。read txの外で書く)
  maybeRefreshFromLogizardMirror();
  // PML(+NEオーバーレイ)・マスタ・直近発注を1つの read transaction で読む (途中の書き込みと混在させない、Codex R2 Low)
  const db = getDB();
  const { pub, rows, overlay, masters, recentIssued, ledgerZan, boDates, useLedgerZan, rule } = db.transaction(() => ({
    ...loadPmlMerged(), masters: loadMasters(), recentIssued: loadRecentIssued(), ledgerZan: loadLedgerBackorders(),
    boDates: loadBackorderRequestedDates(),
    // 既定 'app' = アプリ台帳。設定 backorder_source='ne' で旧挙動 (NE CSVの発注残数) へ戻せる
    useLedgerZan: getSetting('backorder_source') !== 'ne',
    rule: targetRule(),
  }))();
  const products = [];
  const bySupplier = new Map();
  // ルール切替の観測: v2 で新たに要発注になった (v1 では出なかった) 商品の件数と推奨額。v1 ⊂ v2 なので減る側は無い
  const ruleStats = { rule, addedCount: 0, addedAmount: 0, holdMonthsMissing: 0 };
  for (const r of rows) {
    // セット商品は発注・在庫管理の対象外 (在庫は構成品側)。全商品情報にも出さない (中原さん 2026-07-14)
    if (String(r['商品区分'] || '').trim() === 'セット') continue;
    const p = computeProduct(r, useLedgerZan ? (ledgerZan.get(normProductCode(r['商品コード'])) || 0) : undefined, rule);
    if (p.holdMonthsMissing) ruleStats.holdMonthsMissing++;
    if (p.targetV2 && !p.targetV1) {
      ruleStats.addedCount++;
      ruleStats.addedAmount += (p.recQtyV2 || 0) * p.cost; // v1 で動作中でも v2 の影響額が見える
    }
    const a = masters.attrs.get(p.key);
    p.conditionId = a ? (a.condition_id || '') : '';
    p.materialGroupId = a ? (a.material_group_id || '') : '';
    p.capacityPerUnit = a ? (a.capacity_per_unit || null) : null;
    p.caseGroup = a ? (a.case_group || '') : '';
    p.caseLot = a ? (a.case_lot || null) : null;
    const ri = recentIssued.get(p.key);
    p.recentIssued = ri || null;
    // 注残の希望納期内訳 (台帳由来)。注残0の商品や納期未指定の発注しかない商品は null
    const bd = boDates.get(p.key);
    p.backOrderDates = (p.backOrder > 0 && bd && bd.length) ? bd : null;
    // 選べる◯種セット構成商品: 在庫を切らさない前提のため、在庫+注残 が最低在庫以下なら要発注扱い
    const sel = masters.selectable.get(p.key);
    const selMin = sel ? (sel.min_stock != null ? sel.min_stock : SELECTABLE_DEFAULT_MIN) : null;
    p.selectableLow = (sel && p.active && (p.stock + p.backOrder) <= selMin)
      ? { sets: sel.set_names || '', minStock: selMin } : null;
    // 通常式で推奨発注が出ない場合 (販売30日=0等) は「最低在庫の2倍まで補充」をロット切上げで推奨 (Codex P8-1)
    if (p.selectableLow && !p.recQty) {
      const lot = p.lot > 0 ? p.lot : 1;
      const need = Math.max(0, selMin * 2 - (p.stock + p.backOrder));
      p.recQty = need > 0 ? Math.ceil(need / lot) * lot : lot;
    }
    products.push(p);
    if (!p.supplierCode) continue;
    let g = bySupplier.get(p.supplierCode);
    if (!g) {
      const sm = masters.suppliers.get(p.supplierCode);
      g = {
        code: p.supplierCode,
        name: sm ? sm.name : '',
        memo: sm ? (sm.order_memo || '') : '',
        targets: [], candidates: [], horikoshi: [],
      };
      bySupplier.set(p.supplierCode, g);
    }
    if (p.isTarget || p.selectableLow) g.targets.push(p);
    else if (p.isHorikoshi) g.horikoshi.push(p);
    // 在庫あり販売0 (stockMonths=0) もついで買い候補に含める (掘り起こしから外れた死に筋の受け皿。
    // どのリストにも載らないとワークスペース検索・カート追加から消えてしまう)
    else if (p.active) g.candidates.push(p);
  }
  // 在庫月数の昇順。販売0 (stockMonths=null) は末尾へ
  const byMonths = (a, b) => ((a.stockMonths == null) - (b.stockMonths == null)) || (a.stockMonths - b.stockMonths);
  for (const g of bySupplier.values()) {
    g.targets.sort(byMonths);
    g.candidates.sort(byMonths);
    g.horikoshi.sort((a, b) => (b.lastPurchase || '').localeCompare(a.lastPurchase || ''));
    g.estAmount = Math.round(g.targets.reduce((s, p) => s + (p.recQty || 0) * p.cost, 0));
  }
  ruleStats.addedAmount = Math.round(ruleStats.addedAmount);
  return { pub, overlay, products, bySupplier, masters, ruleStats };
}

/**
 * 発注条件の充足評価。旧GAS参照ツール (発注条件マスタ) の全条件タイプを自動判定する。
 *   数量[個]           Σ数量 ≧ 条件値
 *   金額[円]           Σ(数量×原価) ≧ 条件値
 *   上限               Σ数量 ≦ 条件値 (仕入先の出荷制限。例: びわこふきん300枚まで)
 *   数量[ケース]       Σ(数量÷ケース入数) ≧ 条件値。ケース入数 = attrsのケースロット、無ければ発注ロット単位
 *   ケース入数かつ金額  金額下限 + ケースグループごとに Σ数量がケースロットの倍数 (ジョンズブレンド:
 *                      同グループ内は香り違いを混載できるが、ケース入数ちょうどで発注する)
 *   ロット倍率 n倍     各商品の発注数が (発注ロット単位×n) の倍数 (パシーマ: 2ロット単位で発注)
 *   ロット倍率以上 n倍  各商品の発注数が (発注ロット単位×n) 以上 (ノアテック: 最低2ロットから)
 * items: [{key, qty, cost, lot?, caseLot?, caseGroup?}] — 発注内容 (lot=発注ロット単位)。
 * lot/ケース情報が無い商品は判定不能として auto.unknown / auto.noSize に件数・キーを載せ、met は下げない
 * (データ不備で発注を止めない。バッジで人間に知らせる)。
 */
export function evaluateCondition(cond, memberKeys, items) {
  const inGroup = items.filter(i => memberKeys.has(i.key));
  const sumQty = inGroup.reduce((s, i) => s + i.qty, 0);
  const sumAmount = Math.round(inGroup.reduce((s, i) => s + i.qty * (i.cost || 0), 0));
  const active = inGroup.filter(i => i.qty > 0);
  let auto = null; // null=自動判定不可 (未知の条件タイプ → 条件文を表示して人間が判断)
  if (cond.condition_type === '数量' && (cond.unit === '個' || !cond.unit)) {
    auto = { current: sumQty, required: cond.condition_value, met: sumQty >= cond.condition_value, kind: 'qty' };
  } else if (cond.condition_type === '数量' && cond.unit === 'ケース') {
    let cases = 0; const noSize = [];
    for (const i of active) {
      const size = i.caseLot || i.lot || 0;
      if (size > 0) cases += i.qty / size;
      else noSize.push(i.key);
    }
    cases = Math.round(cases * 100) / 100;
    auto = { current: cases, required: cond.condition_value, met: cases >= cond.condition_value, kind: 'cases', noSize };
  } else if (cond.condition_type === '金額') {
    auto = { current: sumAmount, required: cond.condition_value, met: sumAmount >= cond.condition_value, kind: 'amount' };
  } else if (cond.condition_type === '上限') {
    auto = { current: sumQty, required: cond.condition_value, met: sumQty <= cond.condition_value, kind: 'cap' };
  } else if (cond.condition_type === 'ケース入数かつ金額') {
    // ケースグループごとに合算 (グループ無しの商品は単品でケースロット判定)
    const byGroup = new Map(); const misaligned = []; const noInfo = [];
    for (const i of active) {
      if (i.caseGroup) {
        let g = byGroup.get(i.caseGroup);
        if (!g) { g = { qty: 0, lot: 0 }; byGroup.set(i.caseGroup, g); }
        g.qty += i.qty;
        if (i.caseLot > 0) g.lot = Math.max(g.lot, i.caseLot);
      } else if (i.caseLot > 0) {
        if (i.qty % i.caseLot !== 0) misaligned.push({ group: i.key, qty: i.qty, lot: i.caseLot });
      } else noInfo.push(i.key);
    }
    for (const [name, g] of byGroup) {
      if (g.lot > 0) { if (g.qty % g.lot !== 0) misaligned.push({ group: name, qty: g.qty, lot: g.lot }); }
      else noInfo.push(name);
    }
    const amountMet = sumAmount >= cond.condition_value;
    auto = { current: sumAmount, required: cond.condition_value, met: amountMet && misaligned.length === 0, kind: 'caseAmount', amountMet, misaligned, noInfo };
  } else if (cond.condition_type === 'ロット倍率') {
    const off = []; const unknown = [];
    for (const i of active) {
      const step = (i.lot || 0) * cond.condition_value;
      if (!(step > 0)) { unknown.push(i.key); continue; }
      if (i.qty % step !== 0) off.push({ key: i.key, qty: i.qty, step });
    }
    auto = { current: sumQty, required: cond.condition_value, met: off.length === 0, kind: 'lotMultiple', off, unknown };
  } else if (cond.condition_type === 'ロット倍率以上') {
    const off = []; const unknown = [];
    for (const i of active) {
      const min = (i.lot || 0) * cond.condition_value;
      if (!(min > 0)) { unknown.push(i.key); continue; }
      if (i.qty < min) off.push({ key: i.key, qty: i.qty, min });
    }
    auto = { current: sumQty, required: cond.condition_value, met: off.length === 0, kind: 'lotMin', off, unknown };
  }
  return { sumQty, sumAmount, auto };
}
