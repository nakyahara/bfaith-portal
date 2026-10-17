/**
 * lz-bc-import/lz-state.js — ロジザードの毎日の商品マスタの取込の状態 (apps/logizard-import-state) を見て、「ロジザードに取り込む」を止めるか決める
 *
 * 止める (積まない・渡さない・押させない):
 *   - 取込の鍵が生きている (毎晩の取込・少数件の試験・確かめのやり直しが動いている)
 *   - 取込の途中 (importing) か、取り込んだ後の確かめの前 (imported_unverified) = 毎晩の取込の前後の書き出しの比べに、新商品の取込が混ざらないように
 *   - 手の取込が開いている (人が別のアカウントでロジザードに取り込んでいる途中)
 *   - 状態を読めない = 止める (分からないときは押さない)
 * 止めない: まだ初期化していない (毎晩の取込を始める前)・止めてある (halted)・止まった回 (unknown・partial・verify_failed) = 新商品の取込とは別の話
 */
import * as IS from '../logizard-import-state/store.js';

let db = null;
let reader = (now) => {
  db ||= IS.openImportStateDb();
  return IS.getStatus(db, { now, events: 0 });
};
/** 試験で差し替える ((now) => status | throw) */
export function __setImportStateReader(fn) { reader = fn || ((now) => { db ||= IS.openImportStateDb(); return IS.getStatus(db, { now, events: 0 }); }); }

/** @returns {null | { code: string, message: string }} */
export function importStateBlock(now = Date.now()) {
  let st;
  try { st = reader(now); } catch (e) {
    console.error(`[lz-bc-import] ロジザードの取込の状態を読めない: ${e && e.message}`);
    return { code: 'lz_state_unreadable', message: 'ロジザードの毎日の商品マスタの取込の状態を読めないので、取り込みません' };
  }
  if (!st || !st.initialized) return null;
  if (st.lock) return { code: 'lz_import_locked', message: `ロジザードの毎日の商品マスタの取込が動いています (鍵 ${st.lock.purpose || ''}・${st.lock.expires_at ? new Date(st.lock.expires_at).toISOString() : ''} まで)。終わってから押してください` };
  if (st.state === 'importing' || st.state === 'imported_unverified') return { code: 'lz_import_in_progress', message: `ロジザードの毎日の商品マスタの取込が途中です (状態 ${st.state})。確かめが済んでから押してください` };
  if (st.manual && st.manual.open) return { code: 'lz_manual_open', message: 'ロジザードの毎日の商品マスタの「手の取込」が開いています。終えてから押してください' };
  return null;
}
