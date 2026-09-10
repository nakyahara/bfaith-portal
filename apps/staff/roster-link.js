/**
 * いろはの現場の名簿 (いろは在庫化 `f_iroha_workers` / FBA箱詰め `fbx_workers`) を、スタッフマスタ (staff.db) の**鏡**にする。
 *
 * 2026-09-10 中原さん: 「いろはの利用者を会社のスタッフ名簿に入れてよい。PIN も共通化」
 *
 * 作り (なぜ鏡なのか):
 *   - 各アプリの表はそのまま残す。作業の記録 (セッション・投入・イベント) が **その表の id** で人を指しているため、
 *     id を staff.id に差し替えると履歴が全部壊れる。表に `staff_id` を足して「どの人か」だけをスタッフマスタに寄せる
 *   - 名前・区分 (利用者/職員)・有効・並び・PIN の有無 は スタッフマスタから写す (syncRoster)。
 *     写すのは staff_meta.roster_rev が進んだときだけ (毎回 全行を比べない)
 *   - 区分の対応: staff.kind = 'iroha' → 利用者 (member) / それ以外 → 職員 (staff)
 *   - 有効の対応: staff.active かつ 役割 iroha を持つ → 有効。**アプリからの「無効」は役割を外すだけ**
 *     (FBA箱詰めの iPad から社員を退職扱いにしてはいけない。退職はスタッフマスタの管理画面で)
 *   - PIN はスタッフマスタに 1 つ (setStaffPin / verifyStaffPin)。鏡には pin_set (有無) だけ
 *
 * 既存データの移行 (migrateLegacyRoster — 起動時に 1 回、以後は何もしない):
 *   - staff_id が空の行 = 旧名簿。**名前が完全一致 (空白無視・NFKC) する人が 1 人だけ**いれば紐付け、
 *     それ以外はスタッフマスタに新しく作る (番号は IROHA-001〜 の自動採番。あとで管理画面で直せる)。
 *     利用者は kind='iroha' の人としか一致させない (同名の社員に利用者を紐付けない)
 *   - 設定済みの PIN はハッシュのまま持ち越す (塩に方式を焼き込む: `iroha-pin:<salt>` / `fbx-pin:<salt>`)
 *   - 紐付け先を間違えたときは 管理画面の「紐付け直し」(relinkRosterWorker) で直す
 */
import {
  listStaff, getStaff, createStaff, setStaffRoles, setStaffActive, getRosterRev, nameKey, tapName,
  nextGeneratedStaffNo, importStaffPinHash, getStaffDB, IROHA_ROLE,
} from './db.js';

const utcNow = () => new Date().toISOString();

/** 鏡の表に要る列を足す (staff_id = スタッフマスタの id / pin_set = PIN の有無)。各アプリの createTables から */
export function ensureMirrorColumns(db, table) {
  const cols = new Set(db.prepare(`PRAGMA table_info(${table})`).all().map((c) => c.name));
  if (!cols.has('staff_id')) db.exec(`ALTER TABLE ${table} ADD COLUMN staff_id INTEGER`);
  if (!cols.has('pin_set')) db.exec(`ALTER TABLE ${table} ADD COLUMN pin_set INTEGER NOT NULL DEFAULT 0`);
  db.exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_${table}_staff ON ${table}(staff_id)`);
}

/** スタッフマスタの 1 人 → 鏡の行に写す値 */
function desiredRow(s) {
  return {
    display_name: tapName(s),
    worker_type: s.kind === 'iroha' ? 'member' : 'staff',
    active: s.active && (s.roles || []).includes(IROHA_ROLE) ? 1 : 0,
    sort_order: Number(s.sort) || 0,
    pin_set: s.pin_set ? 1 : 0,
  };
}

/**
 * スタッフマスタから写す。state = アプリごとの { rev } (前回写した世代)。
 * rev が同じなら何もしない。force = 世代に関係なく写す (移行直後・DB 差し替え時)
 */
export function syncRoster(db, table, state, { force = false } = {}) {
  const rev = getRosterRev();
  if (!force && state.rev === rev) return { synced: false, rev };
  const staff = listStaff({ includeInactive: true });
  const locals = db.prepare(`SELECT id, staff_id, display_name, worker_type, active, sort_order, pin_set FROM ${table} WHERE staff_id IS NOT NULL`).all();
  const byStaff = new Map(locals.map((l) => [l.staff_id, l]));
  const ins = db.prepare(`INSERT INTO ${table} (staff_id, display_name, worker_type, active, sort_order, pin_set, created_at, created_by)
    VALUES (?, ?, ?, ?, ?, ?, ?, 'staff-sync')`);
  const upd = db.prepare(`UPDATE ${table} SET display_name = ?, worker_type = ?, active = ?, sort_order = ?, pin_set = ? WHERE id = ?`);
  let inserted = 0, updated = 0;
  db.transaction(() => {
    for (const s of staff) {
      const want = desiredRow(s);
      const local = byStaff.get(s.id);
      if (!local) {
        if (!want.active) continue;   // 役割の無い人・退職者を新しく生やさない
        ins.run(s.id, want.display_name, want.worker_type, want.active, want.sort_order, want.pin_set, utcNow());
        inserted++;
        continue;
      }
      if (local.display_name !== want.display_name || local.worker_type !== want.worker_type || local.active !== want.active
        || local.sort_order !== want.sort_order || local.pin_set !== want.pin_set) {
        upd.run(want.display_name, want.worker_type, want.active, want.sort_order, want.pin_set, local.id);
        updated++;
      }
    }
  })();
  state.rev = rev;
  return { synced: true, rev, inserted, updated };
}

/**
 * 旧名簿 (staff_id が空の行) をスタッフマスタへ移す。冪等 (staff_id が入った行は二度と触らない)。
 * @returns {{linked: Array, created: Array}} 何をどう扱ったか (起動ログ・テスト用)
 */
export function migrateLegacyRoster(db, table, { saltPrefix, appLabel }) {
  const legacy = db.prepare(`SELECT * FROM ${table} WHERE staff_id IS NULL ORDER BY id`).all();
  const out = { linked: [], created: [] };
  if (legacy.length === 0) return out;
  const today = utcNow().slice(0, 10);
  const taken = () => new Set(db.prepare(`SELECT staff_id FROM ${table} WHERE staff_id IS NOT NULL`).all().map((r) => r.staff_id));
  for (const w of legacy) {
    const key = nameKey(w.display_name);
    const used = taken();
    let cands = listStaff({ includeInactive: true })
      .filter((s) => !used.has(s.id))
      .filter((s) => nameKey(s.display_name) === key || (s.short_name && nameKey(s.short_name) === key));
    if (w.worker_type === 'member') cands = cands.filter((s) => s.kind === 'iroha');
    let s, how;
    if (cands.length === 1) {
      s = cands[0]; how = 'linked';
    } else {
      // 0 人 = 新しく作る。2 人以上 = どちらか決められないので新しく作る (紐付け直しは管理画面から)
      s = createWithRetry({
        display_name: w.display_name, kind: w.worker_type === 'member' ? 'iroha' : null,
        note: `${appLabel} の名簿から移行 (${today})${cands.length > 1 ? ' ⚠同名が複数いたため紐付けず新規' : ''}`,
      }, 'roster-migrate');
      how = 'created';
      if (!w.active) setStaffActive(s.id, false, 'roster-migrate', { expectVersion: s.version });
    }
    // 役割 iroha は「いま有効な行」にだけ付ける (無効だった人を鏡で有効に戻さない)
    if (w.active && !(s.roles || []).includes(IROHA_ROLE)) setStaffRoles(s.id, [...(s.roles || []), IROHA_ROLE], 'roster-migrate');
    // PIN をハッシュのまま持ち越す (先に共通化された方があれば、そちらを正とする)
    let pin = 'none';
    if (w.pin_hash && w.pin_salt) {
      const r = importStaffPinHash(s.id, { pinHash: w.pin_hash, pinSalt: `${saltPrefix}${w.pin_salt}` }, `roster-migrate:${appLabel}`);
      pin = r.ok ? (r.kept ? 'kept-existing' : 'carried') : 'failed';
    }
    db.prepare(`UPDATE ${table} SET staff_id = ?, pin_hash = NULL, pin_salt = NULL, pin_fails = 0, pin_lock_until = NULL WHERE id = ?`).run(s.id, w.id);
    out[how].push({ localId: w.id, name: w.display_name, staffId: s.id, staffNo: s.staff_no, pin });
  }
  return out;
}

/** 自動採番の番号が同時に取られたときだけ 1 回やり直す */
function createWithRetry(fields, actor) {
  for (let i = 0; i < 2; i++) {
    try {
      return createStaff({ ...fields, staff_no: nextGeneratedStaffNo() }, actor);
    } catch (e) {
      if (i === 1 || !/既に使われています/.test(e.message)) throw e;
    }
  }
  throw new Error('unreachable');
}

/**
 * アプリ (iPad の名簿・管理画面) からの追加 = スタッフマスタに作って役割 iroha を付け、鏡に写す。
 * 返り値は旧 addWorker と同じ形 ({ok, id} = 鏡の行の id)
 */
export function addRosterWorker(db, table, state, { displayName, workerType, actor, appLabel }) {
  const name = String(displayName || '').trim();
  if (!name || name.length > 30) return { ok: false, error: 'bad_name', message: '名前は1〜30文字で入力してください' };
  if (workerType !== 'member' && workerType !== 'staff') return { ok: false, error: 'bad_type', message: '区分は 利用者 / 職員 のどちらかです' };
  syncRoster(db, table, state);
  const dup = db.prepare(`SELECT id FROM ${table} WHERE display_name = ? AND active = 1`).get(name);
  if (dup) return { ok: false, error: 'duplicate', message: `「${name}」は既に登録されています` };
  const s = createWithRetry({ display_name: name, kind: workerType === 'member' ? 'iroha' : null, note: `${appLabel} から追加` }, actor || appLabel);
  setStaffRoles(s.id, [IROHA_ROLE], actor || appLabel);
  syncRoster(db, table, state, { force: true });
  const local = db.prepare(`SELECT id FROM ${table} WHERE staff_id = ?`).get(s.id);
  return { ok: true, id: local.id, staffId: s.id, staffNo: s.staff_no };
}

/**
 * アプリからの 有効/無効 = 役割 iroha の付け外し (スタッフマスタの active は触らない)。
 * @returns {{ok:true}} | {{ok:false, error:'not_found'|'retired', message}}
 */
export function setRosterWorkerActive(db, table, state, localId, active, actor) {
  const local = db.prepare(`SELECT id, staff_id FROM ${table} WHERE id = ?`).get(Number(localId));
  if (!local) return { ok: false, error: 'not_found', message: '作業者が見つかりません' };
  if (!local.staff_id) {   // 移行前の行 (通常は無い)。鏡でないので直接
    db.prepare(`UPDATE ${table} SET active = ? WHERE id = ?`).run(active ? 1 : 0, local.id);
    return { ok: true };
  }
  const s = getStaff(local.staff_id);
  if (!s) return { ok: false, error: 'not_found', message: 'スタッフマスタに該当する人がいません (管理画面で紐付け直してください)' };
  if (active && !s.active) {
    return { ok: false, error: 'retired', message: `${s.display_name} はスタッフマスタで無効 (退職) になっています。スタッフマスタの管理画面で有効に戻してください` };
  }
  const roles = new Set(s.roles || []);
  if (active) roles.add(IROHA_ROLE); else roles.delete(IROHA_ROLE);
  setStaffRoles(s.id, [...roles], actor);
  syncRoster(db, table, state, { force: true });
  return { ok: true };
}

/**
 * 紐付け直し (管理者)。移行で別人・重複に紐付いたときの直し方。
 *   - 対象の staff に役割 iroha を付け (鏡の行が有効なら)、鏡の staff_id を差し替える
 *   - 元の staff から役割 iroha を外す。元が自動採番 (IROHA-…) の行で他に役割が無ければ無効にする (重複の後始末)
 *   - PIN: 元にあって先に無ければ持ち越す (同じ人なので)
 */
export function relinkRosterWorker(db, table, state, { localId, staffId, actor }) {
  const local = db.prepare(`SELECT id, staff_id, active, display_name FROM ${table} WHERE id = ?`).get(Number(localId));
  if (!local) return { ok: false, error: 'not_found', message: '作業者が見つかりません' };
  const target = getStaff(staffId);
  if (!target) return { ok: false, error: 'not_found', message: 'スタッフマスタにその人がいません' };
  if (target.id === local.staff_id) return { ok: true, unchanged: true };
  const other = db.prepare(`SELECT id, display_name FROM ${table} WHERE staff_id = ? AND id <> ?`).get(target.id, local.id);
  if (other) return { ok: false, error: 'already_linked', message: `${target.display_name} は既に「${other.display_name}」に紐付いています` };
  const old = local.staff_id ? getStaff(local.staff_id) : null;
  if (local.active && !(target.roles || []).includes(IROHA_ROLE)) setStaffRoles(target.id, [...(target.roles || []), IROHA_ROLE], actor);
  let pin = 'none';
  if (old && old.pin_set && !target.pin_set) {
    const raw = getStaffDB().prepare('SELECT pin_hash, pin_salt FROM staff WHERE id = ?').get(old.id);
    const r = importStaffPinHash(target.id, { pinHash: raw.pin_hash, pinSalt: raw.pin_salt }, `${actor} (relink)`);
    pin = r.ok ? 'carried' : 'failed';
  }
  db.prepare(`UPDATE ${table} SET staff_id = ? WHERE id = ?`).run(target.id, local.id);
  if (old) {
    const rest = (old.roles || []).filter((r) => r !== IROHA_ROLE);
    setStaffRoles(old.id, rest, actor);
    if (rest.length === 0 && /^IROHA-\d+$/.test(old.staff_no) && old.active) {
      setStaffActive(old.id, false, `${actor} (relink)`, { expectVersion: getStaff(old.id).version });
    }
  }
  syncRoster(db, table, state, { force: true });
  return { ok: true, pin, from: old ? { id: old.id, staff_no: old.staff_no, display_name: old.display_name } : null };
}

/** 管理画面の「紐付け直し」の選択肢 (無効な人も出す — いま紐付いている先が無効でも表示できるように) */
export function listStaffForLink() {
  return listStaff({ includeInactive: true }).map((s) => ({ id: s.id, staff_no: s.staff_no, display_name: s.display_name, short_name: s.short_name, kind: s.kind, roles: s.roles, active: s.active }));
}
