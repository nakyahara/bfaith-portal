/**
 * NE (ネクストエンジン) の 代表商品コード からバリエーションを判定する。
 *
 * 実データ調査 (2026-07-25, warehouse.db raw_ne_products 4,916件):
 *   - 代表商品コードが空 = 2,786件 (56.7%) … 単品
 *   - 代表商品コードあり = 2,130件 … バリエーションの子SKU
 *   - 代表コードは約365種類。**うち338種類 (93%) は商品として実在しない** =
 *     代表商品コードは「親商品の行」ではなく **グループキーの文字列**。
 *     実際の値も rooms / aromamist20 / ws100 のように楽天の管理番号相当。
 *   → product-hub は「楽天の1ページ = 1ドラフト」なので、
 *     **グループキー = 代表商品コードがあればそれ、無ければ商品コード自身** と定義する。
 *
 * ⚠️ NE データにはノイズがある (自動でまとめず、人に確認させる理由):
 *   - 代表コード `10` に mercari01〜50「メルカリ訳アリ品」が50件 (バリエーションではない)
 *   - `pokemonpatch2` に pokemonpatch-系 と pokemonpatch2-系 の2系統が混在
 *   - 代表コードがあるのに1SKUしかないグループが35件
 *
 * 照合は LOWER(TRIM()) で行う ([[feedback-sku-case-normalization]])。
 */

/** @typedef {'unknown'|'single'|'variation'|'conflict'|'detached'} VariationKind */

/**
 * mirror_products.消費税率 は **小数** (0.1 = 10%) で持つ。
 * (aupay/linegift/amazon-accounting が揃って `* 100` している / 書き込み側は `/ 100`)
 * ただし取り込み経路によっては百分率が入る余地があるので両方を吸収する。
 */
export const ALLOWED_TAX_PERCENTS = [8, 10];

export function taxToPercent(v) {
  if (v == null) return null;
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return null; // 0 は「未設定」として扱う (下記 §初期値 参照)
  const pct = n < 1 ? n * 100 : n;
  // 大小だけでは 1 (=100% or 1%) や 0.8 (=80% or 0.8%) を判別できない (Codex low)。
  // これは「新規出品の初期値」なので、現行の消費税率だけを許可し、
  // それ以外は推測せず null (= 人が設定する) に倒す。
  // ⚠️ 丸めてから照合すると 9.6 → 10% のように誤った値が通る (Codex R2 high) ので厳密一致。
  //    0.1*100 = 10.000000000000002 になる浮動小数の誤差だけを許容する
  return ALLOWED_TAX_PERCENTS.find((p) => Math.abs(pct - p) < 1e-6) ?? null;
}

/**
 * 新規登録時の初期値として NE 商品マスタから引く値 (中原さん決定 2026-07-25)。
 *   商品名 … 初期値として入れる。以降はアプリが正 (売り場向けに整えるため)
 *   税率   … 初期値として入れる。以降はアプリで編集
 * 配送方法・在庫数・JAN は対象外 (配送方法=アプリ内設定 / 在庫=登録時0で後から在庫連携 / JAN=手入力)。
 * @returns {{name: string|null, taxPercent: number|null}|null} NE に無ければ null
 */
export function getNeDefaults(db, neCode) {
  const code = String(neCode == null ? '' : neCode).trim();
  if (!code || !mirrorReady(db)) return null;
  let rows;
  try {
    rows = db.prepare('SELECT 商品名, 消費税率 FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ?').all(norm(code));
  } catch (_) {
    return null;
  }
  if (rows.length === 0) return null;

  // 正規化後に重複する行がありうる (kind='conflict' と同じ状況)。
  // `.get()` で任意の1行を採ると、行ごとに税率が違うとき不定な値が Yahoo 初期値になる
  // (Codex R3 high) → **全行が一致する項目だけ**採用し、割れていたら null にする
  // null も比較に含める: 除外すると「10% / 未設定」を一致とみなして
  // 片方の値を採ってしまう (Codex R4 high)。全行が完全に同じときだけ採用する
  const agreed = (pick) => {
    const vals = new Set(rows.map(pick));
    if (vals.size !== 1) return null;
    return [...vals][0] ?? null;
  };
  const name = agreed((r) => (r.商品名 && String(r.商品名).trim() !== '' ? String(r.商品名).trim() : null));
  const taxPercent = agreed((r) => taxToPercent(r.消費税率));
  return { name, taxPercent };
}

/**
 * 画面 (GET /api/variation) と登録 (POST /api/drafts) で **同じ規則**で初期値を決める。
 * 別々に書くと、代表コードの行が無く子SKU行だけある NE データで
 * 「画面には出るが登録時は空」というズレが出る (Codex medium-4)。
 * 代表コード行を優先し、**項目ごとに**空なら入力コード側で補う。
 */
export function resolveNeDefaults(db, inputCode) {
  const v = resolveVariationGroup(db, inputCode, { withMembers: false });
  const primary = getNeDefaults(db, v.groupKey);
  const fallback = getNeDefaults(db, inputCode);
  if (!primary && !fallback) return null;
  return {
    name: primary?.name ?? fallback?.name ?? null,
    taxPercent: primary?.taxPercent ?? fallback?.taxPercent ?? null,
  };
}

/**
 * 利益シミュレーション用の NE 実費 (原価・送料・税率)。mirror_products から引く。
 * ⚠️ 送料は「配送方法の値から自動算出」(中原さん 2026-07-28) — NE 側で配送方法→送料の
 * 導出が済んだ値が mirror_products.送料 に入っている (例: ネコポス→237円)。
 * どの配送方法由来かを表示できるよう shippingMethod も返す。
 * 正規化重複で値が割れたら採用しない (getNeDefaults と同じ方針)。無ければ null
 *
 * バリエーションは**子SKUの行から集計**する (2026-08-24 中原さん要望)。
 * 代表商品コードの93%は商品として実在しない = 自身の行だけ見ると原価が引けない。
 *   - 全SKUで原価が同じ → その値を costExTax に採用
 *   - SKUごとに違う → costExTax は null にし costVaries=true (売価をSKU別に設定する運用)
 * draftId を渡すと、そのドラフトでバリエーションから外した SKU は集計に含めない。
 */
export function getNeCost(db, neCode, { draftId = null } = {}) {
  const code = String(neCode == null ? '' : neCode).trim();
  if (!code || !mirrorReady(db)) return null;
  let rows;
  let source = 'self';
  try {
    const v = resolveVariationGroup(db, code, { withMembers: false, draftId });
    if (v.kind === 'variation') {
      const excluded = excludedSet(db, draftId);
      rows = db.prepare(`
        SELECT 商品コード, 原価, 送料, 配送方法, 消費税率 FROM mirror_products
        WHERE LOWER(TRIM(代表商品コード)) = ? ORDER BY 商品コード
      `).all(norm(v.groupKey)).filter((r) => !excluded.has(norm(r.商品コード)));
      source = 'members';
    }
    if (!rows || rows.length === 0) {
      rows = db.prepare('SELECT 商品コード, 原価, 送料, 配送方法, 消費税率 FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ?').all(norm(code));
      source = 'self';
    }
  } catch (_) {
    return null;
  }
  if (rows.length === 0) return null;
  const agreed = (pick) => {
    const vals = new Set(rows.map(pick));
    if (vals.size !== 1) return null;
    return [...vals][0] ?? null;
  };
  const num = (v) => (v == null || !Number.isFinite(Number(v)) ? null : Number(v));
  // null (未設定) も1種として数える: 「660円 / 未設定」の混在を「全SKU同じ」とは扱わない
  const distinctCosts = new Set(rows.map((r) => num(r.原価)));
  return {
    costExTax: num(agreed((r) => r.原価)),
    shippingCost: num(agreed((r) => r.送料)),
    shippingMethod: agreed((r) => (r.配送方法 && String(r.配送方法).trim() !== '' ? String(r.配送方法).trim() : null)),
    taxPercent: taxToPercent(agreed((r) => r.消費税率)),
    source, // 'members' = バリエーション子SKUから集計 / 'self' = 自身の行
    costVaries: source === 'members' && distinctCosts.size > 1,
    skuCosts: source === 'members'
      ? rows.map((r) => ({ code: String(r.商品コード).trim(), costExTax: num(r.原価), shippingCost: num(r.送料) }))
      : [],
  };
}

/**
 * その商品コードが「どこかのドラフトでバリエーションから外された」ものか。
 * 外した SKU を別ページにしたい場合はそのコードで新規登録する運用なので、
 * ここに載っているコードは自動まとめの対象から除く (2026-07-25 中原さん方針)。
 */
export function isDetached(db, neCode) {
  try {
    return !!db.prepare('SELECT 1 FROM draft_variation_exclusions WHERE LOWER(TRIM(ne_code)) = ?')
      .get(norm(neCode));
  } catch (_) {
    return false; // テーブル未作成でも判定を止めない
  }
}

/** 指定ドラフトの除外のうち、今そのグループに属している SKU の件数だけ数える */
function countExcludedInGroup(db, draftId, groupKey) {
  if (!draftId) return 0;
  try {
    return db.prepare(`
      SELECT COUNT(*) AS c FROM draft_variation_exclusions e
      JOIN mirror_products p ON LOWER(TRIM(p.商品コード)) = LOWER(TRIM(e.ne_code))
      WHERE e.draft_id = ? AND LOWER(TRIM(p.代表商品コード)) = ?
    `).get(draftId, norm(groupKey)).c;
  } catch (_) {
    return 0;
  }
}

/** 指定ドラフトで外されている SKU コード (正規化済み) の集合 */
function excludedSet(db, draftId) {
  if (!draftId) return new Set();
  try {
    return new Set(
      db.prepare('SELECT ne_code FROM draft_variation_exclusions WHERE draft_id = ?')
        .all(draftId).map((r) => norm(r.ne_code))
    );
  } catch (_) {
    return new Set();
  }
}

/** IN 句のバインド上限 (SQLite 既定 999) に当たらないための分割単位 (Codex low-7) */
const IN_CHUNK = 400;

function chunk(arr, size) {
  const out = [];
  for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
  return out;
}

const norm = (v) => (v == null ? '' : String(v).trim().toLowerCase());

/**
 * mirror_products を読めるか。
 * **表示は fail-soft・登録は fail-closed** に使い分ける (Codex medium):
 * ここが false のまま子SKUで登録すると、代表コードにまとめられないドラフト+Notionカードが
 * でき、カード作成後は regroup 禁止なので自動修復できない。
 */
export function mirrorReady(db) {
  try {
    db.prepare('SELECT 1 FROM mirror_products LIMIT 1').get();
    return true;
  } catch (_) {
    return false;
  }
}

/**
 * 1 商品コードのバリエーション判定。
 * @returns {{
 *   kind: VariationKind, groupKey: string, repCode: string|null,
 *   isChild: boolean, memberCount: number, members: object[], found: boolean
 * }}
 *   kind='unknown'   … NE に無い (これから登録する新商品など)。手動フラグへフォールバック
 *   kind='single'    … NE にあり代表コード無し = 単品
 *   kind='variation' … バリエーション。groupKey が楽天1ページに相当する
 *   isChild=true     … 渡されたコードが子SKUで、groupKey と異なる (まとめる候補)
 */
export function resolveVariationGroup(db, neCode, { withMembers = true, draftId = null } = {}) {
  const code = String(neCode == null ? '' : neCode).trim();
  const empty = {
    kind: 'unknown', groupKey: code, repCode: null,
    isChild: false, memberCount: 0, members: [], excludedMembers: [], found: false,
  };
  if (!code || !mirrorReady(db)) return empty;

  // 明示的にバリエーションから外された SKU は単独ページとして扱う (グループへ引き戻さない)
  if (isDetached(db, code)) {
    return { ...empty, kind: 'detached', groupKey: code, found: true };
  }

  // 正規化後に複数行ヒットする = mirror 側の表記ゆらぎ重複。
  // どちらのグループに属するか決められないので人に確認させる (Codex low-8)
  const selves = db.prepare(
    'SELECT 商品コード, 代表商品コード FROM mirror_products WHERE LOWER(TRIM(商品コード)) = ?'
  ).all(norm(code));
  if (selves.length > 1) {
    return { ...empty, kind: 'conflict', found: true, memberCount: selves.length };
  }
  const self = selves[0] || null;

  const rep = self && self.代表商品コード && String(self.代表商品コード).trim() !== ''
    ? String(self.代表商品コード).trim()
    : null;
  // 代表コードがあればそれがグループキー。無ければ自分自身がキー候補
  const groupKey = rep || code;

  const memberCount = db.prepare(
    'SELECT COUNT(*) AS c FROM mirror_products WHERE LOWER(TRIM(代表商品コード)) = ?'
  ).get(norm(groupKey)).c;

  if (memberCount === 0) {
    // 代表コードに紐づく行が無い = 単品 (NE に本人が居る場合) / 不明 (居ない場合)
    return { ...empty, kind: self ? 'single' : 'unknown', groupKey: code, found: !!self };
  }

  // このドラフトで外された SKU は一覧から分離する (外した記録は残して復帰できるようにする)
  const excluded = excludedSet(db, draftId);
  const all = withMembers
    ? db.prepare(`
        SELECT 商品コード, 商品名, 取扱区分, 在庫数, 標準売価
        FROM mirror_products WHERE LOWER(TRIM(代表商品コード)) = ?
        ORDER BY 商品コード
      `).all(norm(groupKey))
    : [];
  const members = all.filter((m) => !excluded.has(norm(m.商品コード)));
  const excludedMembers = all.filter((m) => excluded.has(norm(m.商品コード)));

  return {
    kind: 'variation',
    groupKey,
    repCode: rep,
    // 入力コードがグループキーと違う = 子SKU を入力された (まとめる候補)
    isChild: norm(code) !== norm(groupKey),
    // 外した分を引いた実効SKU数 (楽天1ページに載る数)。
    // excluded.size をそのまま引くと、mirror 側で代表コードが変わった古い除外行まで
    // 差し引いて実数より小さくなる (Codex low) → グループ内の除外だけ数える
    memberCount: withMembers ? members.length : Math.max(0, memberCount - countExcludedInGroup(db, draftId, groupKey)),
    members,
    excludedMembers,
    found: !!self,
  };
}

/**
 * 一覧向けの一括判定 (N+1 回避)。members は返さない。
 * @returns {Map<string, {kind: VariationKind, groupKey: string, memberCount: number, isChild: boolean}>}
 *   キーは正規化済み (LOWER(TRIM())) の商品コード
 */
export function resolveVariationGroupsBatch(db, neCodes) {
  const result = new Map();
  const codes = [...new Set((neCodes || []).map((c) => String(c == null ? '' : c).trim()).filter(Boolean))];
  if (codes.length === 0 || !mirrorReady(db)) {
    for (const c of codes) result.set(norm(c), { kind: 'unknown', groupKey: c, memberCount: 0, isChild: false });
    return result;
  }

  const selfByCode = new Map();
  const dupCodes = new Set(); // 正規化後に重複する = どのグループか決められない
  for (const part of chunk(codes.map(norm), IN_CHUNK)) {
    const ph = part.map(() => '?').join(',');
    const rows = db.prepare(
      `SELECT 商品コード, 代表商品コード FROM mirror_products WHERE LOWER(TRIM(商品コード)) IN (${ph})`
    ).all(...part);
    for (const r of rows) {
      const k = norm(r.商品コード);
      if (selfByCode.has(k)) dupCodes.add(k);
      else selfByCode.set(k, r);
    }
  }

  // 各コードのグループキーを決めてから、キー単位で件数をまとめて引く
  const groupKeyOf = new Map();
  for (const c of codes) {
    const s = selfByCode.get(norm(c));
    const rep = s && s.代表商品コード && String(s.代表商品コード).trim() !== '' ? String(s.代表商品コード).trim() : null;
    groupKeyOf.set(norm(c), rep || c);
  }
  const keys = [...new Set([...groupKeyOf.values()].map(norm))];
  const counts = new Map();
  for (const part of chunk(keys, IN_CHUNK)) {
    const kph = part.map(() => '?').join(',');
    for (const r of db.prepare(`
      SELECT LOWER(TRIM(代表商品コード)) AS k, COUNT(*) AS c
      FROM mirror_products WHERE LOWER(TRIM(代表商品コード)) IN (${kph})
      GROUP BY LOWER(TRIM(代表商品コード))
    `).all(...part)) counts.set(r.k, r.c);
  }

  // 外された SKU の集合 (一覧でも「単独ページ」として扱う)
  const detached = new Set();
  try {
    for (const part of chunk(codes.map(norm), IN_CHUNK)) {
      const ph = part.map(() => '?').join(',');
      for (const r of db.prepare(
        `SELECT DISTINCT LOWER(TRIM(ne_code)) k FROM draft_variation_exclusions WHERE LOWER(TRIM(ne_code)) IN (${ph})`
      ).all(...part)) detached.add(r.k);
    }
  } catch (_) { /* テーブル未作成でも一覧を止めない */ }

  for (const c of codes) {
    if (detached.has(norm(c))) {
      result.set(norm(c), { kind: 'detached', groupKey: c, memberCount: 0, isChild: false });
      continue;
    }
    if (dupCodes.has(norm(c))) {
      result.set(norm(c), { kind: 'conflict', groupKey: c, memberCount: 0, isChild: false });
      continue;
    }
    const key = groupKeyOf.get(norm(c));
    const cnt = counts.get(norm(key)) || 0;
    const self = selfByCode.get(norm(c));
    if (cnt === 0) {
      result.set(norm(c), { kind: self ? 'single' : 'unknown', groupKey: c, memberCount: 0, isChild: false });
    } else {
      result.set(norm(c), { kind: 'variation', groupKey: key, memberCount: cnt, isChild: norm(c) !== norm(key) });
    }
  }
  return result;
}

/**
 * 画面表示用の has_variation。**NE を正とし、NE に無い商品だけ手入力値へフォールバック**
 * (中原さん決定 2026-07-25)。DB の has_variation は焼き直さないので、
 * NE 側でバリエーション構成が変わっても常に最新が映る。
 */
export function effectiveHasVariation(info, draft) {
  if (info && info.kind === 'variation') return { value: info.memberCount > 1, source: 'ne' };
  // detached = バリエーションから外した単独ページ。single と同じく「なし」で確定
  if (info && (info.kind === 'single' || info.kind === 'detached')) return { value: false, source: 'ne' };
  return { value: !!(draft && draft.has_variation), source: 'manual' };
}

/**
 * 利益シミュレーションで選べる「配送方法 → 配送費」の一覧 (2026-09-04 中原さん要望)。
 *
 * 背景: 利益の配送費は NE の送料 (= NE が配送方法から自動算出した値) をそのまま使うので、
 * 画面で配送方法を変えても金額が動かなかった。「この商品をどの配送方法で送るか」で
 * 利益は変わるので、その場で試算できるようにする。
 *
 * ⚠️ 楽天の配送方法グループ (8種) と NE の配送方法 (サイズ別) は**粒度が違う**。
 *    楽天「宅急便50サイズ以下」は NE の 50/60/80/100/120 サイズ (479〜945円) のどれにもなる。
 *    だから選択肢は **NE の配送方法**で持つ (送料が一意に決まる方)。
 *
 * 実データ (2026-09-04 実測・取扱中7,239件): 配送方法ごとの送料はほぼ1種類。
 * 宅急便50/60/80 だけ 2 種類 (端数違い) あるので**最頻値**を採る。
 */
export function listNeShippingOptions(db) {
  try {
    const rows = db.prepare(`
      SELECT TRIM(配送方法) AS method, 送料 AS cost, COUNT(*) AS n
      FROM mirror_products
      WHERE 取扱区分 = '取扱中'
        AND 配送方法 IS NOT NULL AND TRIM(配送方法) != ''
        AND 送料 IS NOT NULL AND 送料 >= 0
      GROUP BY TRIM(配送方法), 送料
    `).all();
    // 配送方法ごとに「一番多く使われている送料」を代表値にする。
    // 同数のときは**高い方**を採る (利益を実際より良く見せない側に倒す)
    const best = new Map(); // method -> { cost, n, total }
    for (const r of rows) {
      const method = String(r.method).trim();
      const cur = best.get(method);
      const total = (cur?.total || 0) + r.n;
      const wins = !cur || r.n > cur.n || (r.n === cur.n && Number(r.cost) > cur.cost);
      if (wins) best.set(method, { cost: Number(r.cost), n: r.n, total });
      else best.set(method, { ...cur, total });
    }
    // 送料の安い順に並べる (2026-09-04 中原さん要望)。利用数順だと金額がバラバラに並んで
    // 探しにくい。同額なら配送方法名で揃えて、並びが実行のたびに変わらないようにする
    return [...best.entries()]
      .map(([method, v]) => ({ method, cost: v.cost, count: v.total }))
      // localeCompare は別の文字列でも 0 を返すことがある (合成済み「ガ」と「カ+結合濁点」など)。
      // その場合は挿入順に引きずられるので、最後にコード単位で決める (Codex R1 P2)
      .sort((a, b) => a.cost - b.cost
        || a.method.localeCompare(b.method, 'ja')
        || (a.method < b.method ? -1 : a.method > b.method ? 1 : 0));
  } catch (_) {
    return []; // mirror 未同期でも画面は出す
  }
}

/**
 * 楽天の配送方法グループ → NE の配送方法名にかかる語 (2026-09-04)。
 * 利益シミュレーションで**候補を上に集めるためだけ**の目安で、これで確定はしない
 * (最終的に人が選ぶ)。掲載HTMLに出る mapNeShippingToRakuten の割当とは別物 —
 * あちらは誤ると商品ページの表記が誤るので部分一致を避けている
 */
export const RAKUTEN_GROUP_NE_HINTS = {
  '1': ['定形'],          // 定形外 → 定形内 / 定形外規格内 / 定形外規格外
  '3': ['飛脚'],          // 飛脚宅配便
  '4': ['宅急便'],        // 宅急便
  '5': ['ネコポス'],
  '6': ['クリックポスト'],
  '7': ['宅急便'],        // ヤマト運輸宅急便
  '8': ['宅急便'],        // 宅急便50サイズ以下
  '9': ['ゆうパケット'],  // ゆうパケットパフ
};

/**
 * 利益シミュレーションの配送方法の選択肢 (2026-09-04)。
 *
 * 🚨 **その商品がいま使っている配送方法だけは、代表値ではなく実際の NE 送料**にする。
 * 代表値 (最頻値) で上書きすると、開いただけで利益額が実際と違う数字に変わってしまう
 * (例: この商品は宅急便60サイズ 544円だが、最頻値は 538円 — Codex R1 P1)。
 * 一覧に無い配送方法 (その商品しか使っていない等) も、実送料で先頭に足す。
 */
export function profitShipChoices(options, neMethod, neShippingCost) {
  const cur = String(neMethod ?? '').trim();
  // ⚠️ Number(null) は 0。null を先に弾かないと「送料0円」として選択肢に残り、
  //    利益を過大に見せる (profit.js の computeProfit と同じ注意)
  const actual = (neShippingCost == null || neShippingCost === '') ? NaN : Number(neShippingCost);
  const hasActual = Number.isFinite(actual) && actual >= 0;
  const list = (options || []).map((o) => ({ ...o, isCurrent: cur !== '' && o.method === cur }));
  if (hasActual && !list.some((o) => o.isCurrent)) {
    // 配送方法名が分からなくても送料があれば、その実値を先頭に置く。
    // 置かないと画面が先頭候補を初期選択して、開いただけで利益額が代表送料に変わる
    // (バリエーションで送料は一致・配送方法名だけ割れている場合など — Codex R3 P1)
    list.unshift({
      method: cur || 'NEの登録送料 (配送方法は未設定)',
      cost: actual, count: 0, isCurrent: true,
    });
  } else if (cur && !list.some((o) => o.isCurrent)) {
    list.unshift({ method: cur, cost: null, count: 0, isCurrent: true });
  }
  return list
    .map((o) => (o.isCurrent && hasActual ? { ...o, cost: actual } : o))
    .filter((o) => o.cost != null);
}
