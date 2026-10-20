/**
 * board-theme.js — 工程ボードの背景 (黒 / 白) を人ごとに覚える (2026-10-19 中原さん要望「白背景、黒背景と切り替えれる設定を。アカウントごとに」)
 *
 * 置き場 = product-hub の DB (warehouse-mirror.db) の表 ph_board_prefs (ログインのメール 1 つに 1 行・DDL は db.js)。
 *   前例: master-edit の列の設定 (apps/master-edit/view-prefs.mjs)。localStorage だと会社と家の PC で揃わないのでサーバーに置く。
 * 🚨 読む・書くのは自分の行だけ: 鍵 = セッションのメール (小文字・前後の空白を除く)。API は人の名前を受けない (router.js)。
 * 読めない (表が無い・壊れた値) = いつもの黒 (画面は止めない)。
 * いつもの黒を選んだ = 行を消す (後でいつもの色を変えたとき、その人にも効く)
 */

export const BOARD_THEMES = ['dark', 'light'];
// 10/13 に黒へ変えた (#1718)。白を選んだ人だけ白に戻す
export const DEFAULT_BOARD_THEME = 'dark';

/** 鍵 = ログインのメール (小文字・前後の空白を除く)。無い = null */
export const prefsKeyOf = (email) => {
  const k = String(email ?? '').trim().toLowerCase();
  return k && k.length <= 320 ? k : null;
};

/** その人のボードの背景。'dark' | 'light' (読めない・未設定 = いつもの黒) */
export function boardThemeOf(db, email) {
  const key = prefsKeyOf(email);
  if (!key) return DEFAULT_BOARD_THEME;
  try {
    const row = db.prepare('SELECT theme FROM ph_board_prefs WHERE email = ?').get(key);
    return row && BOARD_THEMES.includes(row.theme) ? row.theme : DEFAULT_BOARD_THEME;
  } catch (e) {
    console.error(`[product-hub] ボードの背景の設定を読めない: ${e && e.message}`);
    return DEFAULT_BOARD_THEME;
  }
}

/**
 * その人のボードの背景を保存。知らない値 = status 400 / メールが無い = status 403 のエラー。
 * いつもの黒 = 行を消す。戻り値 = 保存した背景
 */
export function saveBoardTheme(db, email, theme, { nowMs = Date.now() } = {}) {
  const key = prefsKeyOf(email);
  if (!key) throw Object.assign(new Error('ログインのメールが無いので保存できません'), { status: 403 });
  if (!BOARD_THEMES.includes(theme)) throw Object.assign(new Error('背景の値が不正です'), { status: 400 });
  if (theme === DEFAULT_BOARD_THEME) {
    db.prepare('DELETE FROM ph_board_prefs WHERE email = ?').run(key);
  } else {
    db.prepare(`INSERT INTO ph_board_prefs (email, theme, updated_at) VALUES (?, ?, ?)
      ON CONFLICT(email) DO UPDATE SET theme = excluded.theme, updated_at = excluded.updated_at`)
      .run(key, theme, new Date(nowMs).toISOString());
  }
  return theme;
}
