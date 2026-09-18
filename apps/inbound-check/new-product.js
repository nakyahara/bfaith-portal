/**
 * 入荷受付チェック — 「この商品は新商品か」の判定 (2026-09-18 中原さん指示)
 *
 * 目的: 初めて入ってくる商品は、現場で**パッケージ裏面のラベルを撮っておく**。
 *   撮っておかないと、商品登録 (product-hub) の「基本情報入力」が
 *   「パッケージ裏面の確認待ち」(CHECKING_REASONS の package_label) で止まる。
 *   成分表示・原材料・内容量は実物を見ないと埋まらず、入荷を逃すと次の入荷まで待つことになるため。
 *
 * 中原さんの定義 (2026-09-18):
 *   新商品 = **NE の商品登録日が過去3週間以内** かつ **一度も入庫履歴がない**
 *
 * どのデータで判定するか (2026-09-18 に本番データで実在を確認):
 *   ①登録日   = mirror_products.new_product_launch_date
 *              miniPC で NE 商品マスタの 作成日 (goods_creation_date) から作られ、Render へ毎日
 *              全件置換で同期される。単品×取扱中 3,772 件すべてに値が入っている (充足率 100%)
 *              ⚠miniPC 側で人が new_product_launch_date を手で設定していればそちらが優先される
 *                (rebuild-m-products.js resolveLaunchDate)。発売日として手で直した商品は
 *                「登録日」ではなくその日付で判定されるが、運用上ほぼ使われていない
 *   ②入庫履歴 = 次のどれか1つでも痕跡があれば「入庫したことがある」
 *              (a) 商品管理リスト snapshot の 最終仕入日 (NE の goods_last_time_supplied_date)。
 *                  ロジザードの入庫 → NE 仕入計上 で入る日付で、一番直接的な証拠。
 *                  4,979 件中 4,380 件に値があり、599 件が「一度も仕入なし」
 *              (b) ロジザード在庫ミラーにその商品の行がある (在庫ゼロでも行は残る)
 *              (c) このアプリの過去の確認実績 (f_inbound_check_destinations)
 *
 * 🚨**「データが無い」を「新商品ではない」に読み替えない** (feedback: 取れなかったと0を分ける)。
 *   商品マスタのミラーがまだ無い・その商品コードが見つからない・登録日が空 — これらは
 *   `unknown` を返す。unknown は撮影を**必須にしない**(入荷作業を止めない)が、画面には
 *   「新商品か判定できません」と出して手で撮れるようにする。
 *   逆に「登録日は分かるが入庫の痕跡がまったく無い」は new に倒す — 余分に1枚撮るだけで済み、
 *   撮り逃すと次の入荷まで取り返せないため (安全側は方向で効きめが違う)。
 */
import { jstDateStr } from '../../lib/jst-date.js';

/** 新商品とみなす登録日からの日数 (中原さん: 過去3週間) */
export const NEW_PRODUCT_WINDOW_DAYS = 21;

const norm = (v) => String(v == null ? '' : v).trim().toLowerCase();
const blank = (v) => String(v == null ? '' : v).trim() === '';

function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

/**
 * 任意形式の日付文字列を YYYY-MM-DD へ。読めなければ null。
 * NE の 作成日 は "2026-09-16 18:23:46" / "2026/09/16" のどちらも来る。
 * 実在しない日付 (2026-02-31・2026-13-01) は null に倒す (誤った日付で判定しない)。
 */
export function normalizeDate(raw) {
  if (raw == null) return null;
  const m = String(raw).match(/^(\d{4})[-/](\d{1,2})[-/](\d{1,2})/);
  if (!m) return null;
  const [y, mo, d] = [Number(m[1]), Number(m[2]), Number(m[3])];
  if (!y || !mo || !d) return null;
  const probe = new Date(Date.UTC(y, mo - 1, d));
  if (probe.getUTCFullYear() !== y || probe.getUTCMonth() !== mo - 1 || probe.getUTCDate() !== d) return null;
  return `${m[1]}-${String(mo).padStart(2, '0')}-${String(d).padStart(2, '0')}`;
}

/**
 * 登録日が「今日から windowDays 以内」か。
 * 比較は JST の暦日どうし (時刻を持ち込まない)。未来日付は誤入力とみなして対象外。
 * @returns {boolean|null} 日付が読めなければ null (= 判定できない)
 */
export function isWithinWindow(launchDate, today = jstDateStr(), windowDays = NEW_PRODUCT_WINDOW_DAYS) {
  const l = normalizeDate(launchDate);
  const t = normalizeDate(today);
  if (!l || !t) return null;
  const days = Math.round((Date.parse(`${t}T00:00:00Z`) - Date.parse(`${l}T00:00:00Z`)) / 86400000);
  if (days < 0) return false;          // 未来日付 = 誤入力。新商品扱いにしない
  return days <= windowDays;
}

/**
 * 1回の画面表示ぶんをまとめて引く。
 * ⚠1行ずつ LOWER(TRIM(...)) で照合するとインデックスが効かないので、対象表ごとに1クエリで
 *   Map を作る (enrich.js と同じ考え方)。mirror_pml_snapshot_rows は全商品ぶんある。
 *
 * @param {import('better-sqlite3').Database} db
 * @param {string[]} codeKeys 判定したい商品コード (小文字化済みでなくてもよい)
 * @param {{today?: string}} opts
 * @returns {Map<string, {verdict:'new'|'not_new'|'unknown', launch_date:string|null, reason:string}>}
 */
export function buildNewProductContext(db, codeKeys, { today = jstDateStr() } = {}) {
  const keys = [...new Set((codeKeys || []).map(norm).filter(Boolean))];
  const out = new Map();
  if (keys.length === 0) return out;

  // ⚠この関数は一覧 (getState) から呼ばれ、iPad が**5秒ごとに**取りに来る。
  //   表を丸ごと JS の Map にすると、そのたびに mirror_products 7千行・ロジザード在庫 数万行を
  //   読むことになるので、**聞かれた商品コードだけ** IN で引く (SQLite の上限があるので 500 ずつ)。
  //   prefix = IN より前に来るプレースホルダの値 (run_id など)
  const chunked = (sql, fn, ...prefix) => {
    for (let i = 0; i < keys.length; i += 500) {
      const chunk = keys.slice(i, i + 500);
      const stmt = db.prepare(sql.replace('@IN@', chunk.map(() => '?').join(',')));
      for (const r of stmt.all(...prefix, ...chunk)) fn(r);
    }
  };

  // ① 登録日 (mirror_products)。表ごと無い / 空 = miniPC 同期前 → 全部 unknown に倒す
  //    ⭐「聞いた商品が1件も無い」と「ミラーが空」は別物。空かどうかは件数で確かめる
  const launch = new Map();
  let mirrorReady = false;
  if (tableExists(db, 'mirror_products')) {
    mirrorReady = db.prepare('SELECT EXISTS (SELECT 1 FROM mirror_products) AS e').get().e === 1;
    chunked(`SELECT LOWER(TRIM(商品コード)) AS k, new_product_launch_date AS d
      FROM mirror_products WHERE LOWER(TRIM(商品コード)) IN (@IN@)`, (r) => {
      if (r.k) launch.set(r.k, r.d || null);
    });
  }

  // ② 入庫の痕跡
  //   (a) 商品管理リストの 最終仕入日 (公開中の run のみ)。未公開でも他の材料で判定を続ける
  const supplied = new Map();   // code_key → 最終仕入日 (空文字 = 一度も仕入なし)
  let pmlReady = false;
  if (tableExists(db, 'mirror_pml_snapshot_rows') && tableExists(db, 'mirror_pml_published')) {
    const pub = db.prepare('SELECT run_id FROM mirror_pml_published WHERE id = 1').get();
    if (pub && pub.run_id) {
      pmlReady = db.prepare('SELECT EXISTS (SELECT 1 FROM mirror_pml_snapshot_rows WHERE run_id = ?) AS e').get(pub.run_id).e === 1;
      chunked(`SELECT LOWER(TRIM(商品コード)) AS k, 最終仕入日 AS d, 登録日 AS reg
        FROM mirror_pml_snapshot_rows WHERE run_id = ? AND LOWER(TRIM(商品コード)) IN (@IN@)`, (r) => {
        if (!r.k) return;
        supplied.set(r.k, r.d || '');
        // mirror_products に登録日が無い商品の控え (PML は NE の 作成日 をそのまま持っている)
        if (!launch.get(r.k) && r.reg) launch.set(r.k, r.reg);
      }, pub.run_id);
    }
  }
  //   (b) ロジザード在庫ミラーに行があるか (在庫ゼロでも行は残る = 一度は入庫している)
  const inStock = new Set();
  if (tableExists(db, 'mirror_logizard_stock')) {
    chunked(`SELECT DISTINCT LOWER(TRIM(商品ID)) AS k FROM mirror_logizard_stock
      WHERE LOWER(TRIM(商品ID)) IN (@IN@)`, (r) => { if (r.k) inStock.add(r.k); });
  }
  //   (c) このアプリで過去に確認した実績 (取り消していないもの)。
  //       同じ日に2回届いたとき (ロジザード・NE がまだ追いついていない) を拾う
  const checkedBefore = new Set();
  if (tableExists(db, 'f_inbound_check_destinations')) {
    chunked(`SELECT DISTINCT LOWER(TRIM(product_id)) AS k FROM f_inbound_check_destinations
      WHERE cancelled_at IS NULL AND LOWER(TRIM(product_id)) IN (@IN@)`, (r) => { if (r.k) checkedBefore.add(r.k); });
  }

  for (const k of keys) {
    // 商品マスタのミラーそのものが来ていない → 判定材料が無い。止めない
    if (!mirrorReady) {
      out.set(k, { verdict: 'unknown', launch_date: null, reason: '商品マスタがまだ届いていないため判定できません' });
      continue;
    }
    const d = normalizeDate(launch.get(k));
    if (!d) {
      out.set(k, {
        verdict: 'unknown',
        launch_date: null,
        reason: launch.has(k) ? '商品マスタに登録日が入っていないため判定できません'
          : 'この商品コードが商品マスタに無いため判定できません',
      });
      continue;
    }
    const within = isWithinWindow(d, today);
    if (!within) {
      out.set(k, { verdict: 'not_new', launch_date: d, reason: `登録日 ${d} (${NEW_PRODUCT_WINDOW_DAYS}日より前)` });
      continue;
    }
    // 登録日は新しい。入庫したことがあるか
    const lastSupplied = supplied.get(k);
    if (pmlReady && !blank(lastSupplied)) {
      out.set(k, { verdict: 'not_new', launch_date: d, reason: `最終仕入日 ${String(lastSupplied).slice(0, 10)} (入庫済み)` });
      continue;
    }
    if (inStock.has(k)) {
      out.set(k, { verdict: 'not_new', launch_date: d, reason: 'ロジザードに在庫の記録があります (入庫済み)' });
      continue;
    }
    if (checkedBefore.has(k)) {
      out.set(k, { verdict: 'not_new', launch_date: d, reason: 'このアプリで過去に受け入れ済みです' });
      continue;
    }
    out.set(k, { verdict: 'new', launch_date: d, reason: `登録日 ${d} / 入庫履歴なし` });
  }
  return out;
}

/** 1件だけ判定する (API から使う。画面は buildNewProductContext でまとめて引く) */
export function judgeNewProduct(db, codeKey, opts = {}) {
  const m = buildNewProductContext(db, [codeKey], opts);
  return m.get(norm(codeKey)) || { verdict: 'unknown', launch_date: null, reason: '判定できません' };
}
