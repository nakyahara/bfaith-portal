/**
 * view-prefs.mjs — 商品・セットの一覧の「列の設定」(出す列・並び) を人ごとに覚える (10/8 中原さん「会社の PC と家の PC で同じ」= PR1)
 *
 * 置き場 = Render の warehouse-mirror.db (SQLite) の表 master_edit_view_prefs (ログインのメール 1 つに 1 行)。
 *   前例: 発注アプリの仕入先カードの非表示 (localStorage は PC 間で共有されない → サーバーに。apps/purchase-orders/router.js)・
 *   画面の設定の表 dashboard_settings (apps/warehouse-mirror/db.js)。Company DB には置かない (画面の見せ方だけ = migrate は要らない)。
 * 🚨 読む・書くのは自分の行だけ: 鍵 = セッションのメール (小文字・前後の空白を除く)。API は人の名前を受けない (router.mjs)。
 * 読めない (表が無い・DB が開いていない・壊れた JSON) = いつもの列 (画面は止めない)。保存できない = 503 (画面は「保存できませんでした」)。
 * いつもの列と同じ設定を保存 = 行を消す (後でいつもの列を変えたとき、その人にも効く)
 */
import { DEFAULT_VIEW, normalizeView, sameView, parseViewInput } from './list-columns.mjs';

export const VIEW_PREFS_TABLE = 'master_edit_view_prefs';
const DDL = `CREATE TABLE IF NOT EXISTS ${VIEW_PREFS_TABLE} (
  email       TEXT PRIMARY KEY,
  prefs_json  TEXT NOT NULL,
  updated_at  TEXT NOT NULL
)`;

/** 本番 = warehouse-mirror.db (使うときに読む = この画面の試験は SQLite 無しでも動く)。試験は差し替える */
async function defaultDb() {
  const { getMirrorDB } = await import('../warehouse-mirror/db.js');
  return getMirrorDB();
}
let dbProvider = defaultDb;
export function __setViewPrefsDbProvider(fn) { dbProvider = fn || defaultDb; ready = new WeakSet(); }
let ready = new WeakSet();
async function openDb() {
  const db = await dbProvider();
  if (!db) throw new Error('列の設定の置き場を開けません');
  if (!ready.has(db)) { db.exec(DDL); ready.add(db); }
  return db;
}

/** 鍵 = ログインのメール (小文字・前後の空白を除く)。無い = null */
export const prefsKeyOf = (email) => {
  const k = String(email ?? '').trim().toLowerCase();
  return k && k.length <= 320 ? k : null;
};
const copyDefault = () => ({ order: [...DEFAULT_VIEW.order], shown: [...DEFAULT_VIEW.shown] });

/**
 * その人の列の設定。{ ok: true, view: { order, shown }, saved: bool } / 読めない = { ok: false, error, view: いつもの列, saved: false }
 */
export async function readViewPrefs(email) {
  const key = prefsKeyOf(email);
  if (!key) return { ok: true, view: copyDefault(), saved: false };
  try {
    const db = await openDb();
    const row = db.prepare(`SELECT prefs_json FROM ${VIEW_PREFS_TABLE} WHERE email = ?`).get(key);
    if (!row) return { ok: true, view: copyDefault(), saved: false };
    let v = null;
    try { v = normalizeView(JSON.parse(row.prefs_json)); } catch { v = null; }
    if (!v) return { ok: true, view: copyDefault(), saved: false };   // 壊れた行 = いつもの列 (次の保存で書き直す)
    return { ok: true, view: v, saved: true };
  } catch (e) {
    console.error(`[master-edit] 列の設定を読めない: ${e && e.message}`);
    return { ok: false, error: '列の設定を読めません (いつもの列で出しています)', view: copyDefault(), saved: false };
  }
}

/**
 * その人の列の設定を保存。input = 画面が送ってきた { order, shown } (parseViewInput で確かめる = 誤りは ViewPrefsInputError)。
 * いつもの列と同じ = 行を消す。戻り値 = { view, saved }
 */
export async function saveViewPrefs(email, input, { nowMs = Date.now() } = {}) {
  const key = prefsKeyOf(email);
  if (!key) throw Object.assign(new Error('ログインのメールが無いので保存できません'), { status: 403 });
  const view = parseViewInput(input);
  const db = await openDb();
  if (sameView(view, DEFAULT_VIEW)) {
    db.prepare(`DELETE FROM ${VIEW_PREFS_TABLE} WHERE email = ?`).run(key);
    return { view, saved: false };
  }
  db.prepare(`INSERT INTO ${VIEW_PREFS_TABLE} (email, prefs_json, updated_at) VALUES (?, ?, ?)
    ON CONFLICT(email) DO UPDATE SET prefs_json = excluded.prefs_json, updated_at = excluded.updated_at`)
    .run(key, JSON.stringify({ v: 1, order: view.order, shown: view.shown }), new Date(nowMs).toISOString());
  return { view, saved: true };
}
