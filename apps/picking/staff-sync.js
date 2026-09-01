/**
 * スタッフマスタ同期 (miniPC picking/packing ← Render apps/staff)
 *
 * 中原さん方針 (2026-09-01):「人の正本は staff.db に1つ。各アプリはそこから引く」。
 * miniPC は Render と別プロセス・別DBなので、読み取り専用 export を取得して pk_workers に反映する
 * (packing も同じ pk_workers を読むので同時に揃う)。設計の要点は apps/picking/README-staff-sync.md。
 *
 * 事故を作らないための決めごと (Codex R5 反映):
 *  - pk_workers.code (w01…) は**変えない** — 作業実績が参照する不変キー。staff_id / staff_no を足して紐づける
 *  - 初回の紐付けは名前一致。**同名が絡むときは紐付けない** (別人に紐づく事故を防ぐ):
 *      · export 側に同じ表示名が2人以上 → 全体を拒否 (データが曖昧なので直してもらう)
 *      · pk_workers 側に同名の未紐付けが2行以上 → その人だけ触らず conflicts に出す
 *  - 既存の staff_no と受信 staff_no が食い違う → **全体を拒否** (staff DB 再作成で id が再採番された兆候。
 *    そのまま進めると別人に改名・無効化が起きる)
 *  - 入力検証: id は正整数・重複なし / staff_no 非空・重複なし / 表示名非空。1件でも壊れていたら全体を拒否
 *    (部分欠損を「退職」と誤解しない)
 *  - fail-closed: 取得失敗 / 0件 / 有効数が**前回成功時の有効スタッフ数**の半分未満 / generated_at の巻き戻り → 適用しない
 *  - pk_workers にしかいない人は触らない (unlinked_local = 警告のみ)。紐付け済みなのに export から消えた人は
 *    missing_linked (異常) として区別して強く出す
 *  - 過去の実績 (pk_batches.worker 等) は打刻時点の表示名。改名しても遡らない (履歴を書き換えない)
 *
 * env: STAFF_SYNC_URL (既定 https://bfaith-portal.onrender.com) / STAFF_EXPORT_TOKEN (未設定なら同期しない)
 */
import { getDB } from './db.js';

const utcNow = () => new Date().toISOString();
const DEFAULT_BASE = 'https://bfaith-portal.onrender.com';
const FETCH_TIMEOUT_MS = 20_000;

export function isStaffSyncConfigured() {
  return !!process.env.STAFF_EXPORT_TOKEN;
}

export function getStaffSyncState() {
  const row = getDB().prepare('SELECT * FROM pk_staff_sync_state WHERE id = 1').get() || null;
  if (row) {
    try { row.unmatchedNames = row.unmatched ? JSON.parse(row.unmatched) : []; } catch { row.unmatchedNames = []; }
    if (!Array.isArray(row.unmatchedNames)) row.unmatchedNames = [];
  }
  return row;
}

function saveState(s) {
  const prev = getDB().prepare('SELECT active_staff_count, last_generated_at FROM pk_staff_sync_state WHERE id = 1').get();
  getDB().prepare(`INSERT INTO pk_staff_sync_state
      (id, synced_at, ok, staff_count, linked, added, renamed, deactivated, unmatched, generated_at, error, active_staff_count, last_generated_at)
    VALUES (1, @synced_at, @ok, @staff_count, @linked, @added, @renamed, @deactivated, @unmatched, @generated_at, @error, @active_staff_count, @last_generated_at)
    ON CONFLICT(id) DO UPDATE SET synced_at = @synced_at, ok = @ok, staff_count = @staff_count, linked = @linked,
      added = @added, renamed = @renamed, deactivated = @deactivated, unmatched = @unmatched,
      generated_at = @generated_at, error = @error, active_staff_count = @active_staff_count,
      last_generated_at = @last_generated_at`).run({
    synced_at: utcNow(), ok: s.ok ? 1 : 0, staff_count: s.staffCount ?? null, linked: s.linked ?? null,
    added: s.added ?? null, renamed: s.renamed ?? null, deactivated: s.deactivated ?? null,
    unmatched: s.warnings ? JSON.stringify(s.warnings) : null, generated_at: s.generatedAt ?? null,
    error: s.error ?? null,
    // 成功時だけ「次回の判定基準」を進める (失敗で基準が緩むと激減を見逃す)
    active_staff_count: s.ok ? (s.activeStaffCount ?? null) : (prev?.active_staff_count ?? null),
    last_generated_at: s.ok ? (s.generatedAt ?? prev?.last_generated_at ?? null) : (prev?.last_generated_at ?? null),
  });
}

/** Render から取得 (認証は Bearer)。失敗は例外 */
export async function fetchStaffExport(fetchFn = fetch) {
  const token = process.env.STAFF_EXPORT_TOKEN;
  if (!token) throw new Error('STAFF_EXPORT_TOKEN 未設定');
  const base = (process.env.STAFF_SYNC_URL || DEFAULT_BASE).replace(/\/+$/, '');
  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), FETCH_TIMEOUT_MS);
  let res;
  try {
    res = await fetchFn(`${base}/apps/staff/export`, { headers: { Authorization: `Bearer ${token}` }, signal: ac.signal });
  } finally {
    clearTimeout(timer);
  }
  if (!res.ok) {
    throw new Error(res.status === 404
      ? 'Render 側の STAFF_EXPORT_TOKEN が未設定です (/apps/staff/export が無効)'
      : `HTTP ${res.status}`);
  }
  const json = await res.json();
  if (!json || json.ok !== true || !Array.isArray(json.staff)) throw new Error('応答の形式が不正です');
  return json;
}

const norm = s => String(s == null ? '' : s).trim();
const tapName = s => norm(s.short_name) || norm(s.display_name);
const isActive = s => s.active === 1 || s.active === true;

/**
 * 名前の照合キー。**空白 (半角/全角) を無視**して比べる。
 * 🚨 2026-09-01 実障害: picking の既存作業者は「中原大輔」、スタッフマスタは「中原 大輔」で、
 * 完全一致の照合では紐付かず10名が二重登録された (現場の名前タップが23名になった)。
 * 姓名の間の空白は同じ人の表記ゆれなので、照合では落とす。NFKC で全角英数の揺れも吸収する。
 */
const nameKey = s => norm(s).normalize('NFKC').replace(/[\s　]+/g, '');

/** id は正整数 (数字文字列も受ける)。それ以外は null */
function staffId(v) {
  const n = typeof v === 'number' ? v : (/^\d+$/.test(norm(v)) ? Number(norm(v)) : NaN);
  return Number.isSafeInteger(n) && n > 0 ? n : null;
}

/** export の中身を検証。壊れていたら理由を返す (fail-closed) */
function validateStaff(rows) {
  const errs = [];
  const ids = new Set(), nos = new Set(), tapNames = new Map();
  const staff = [];
  rows.forEach((r, i) => {
    const id = staffId(r.id);
    const no = norm(r.staff_no);
    const name = norm(r.display_name);
    const label = `${i + 1}件目 (${no || '番号なし'} ${name || '名前なし'})`;
    if (id == null) { errs.push(`${label}: id が正の整数でない`); return; }
    if (!no) { errs.push(`${label}: staff_no が空`); return; }
    if (!name) { errs.push(`${label}: display_name が空`); return; }
    if (ids.has(id)) { errs.push(`${label}: id ${id} が重複`); return; }
    if (nos.has(no)) { errs.push(`${label}: staff_no ${no} が重複`); return; }
    ids.add(id); nos.add(no);
    const s = { ...r, id, staff_no: no, display_name: name };
    // 名前一致の紐付けに使う表示名。有効な人だけ衝突を見る (退職者の同名は紐付け対象にならない)
    if (isActive(s)) tapNames.set(tapName(s), (tapNames.get(tapName(s)) || 0) + 1);
    staff.push(s);
  });
  const dupNames = [...tapNames.entries()].filter(([, c]) => c > 1).map(([n]) => n);
  if (dupNames.length) errs.push(`スタッフマスタに同じ表示名が複数あります: ${dupNames.join('、')} (短い表記で区別してください)`);
  return { staff, errs };
}

/**
 * 取得済みデータを pk_workers に反映 (単一トランザクション)。
 * @returns {ok:true, staffCount, activeStaffCount, linked, added, renamed, deactivated, warnings[]}
 *        | {ok:false, error, skipped:true}
 */
export function applyStaffExport(payload, { fetchedAt = null } = {}) {
  const db = getDB();
  const generatedAt = norm(payload.generated_at) || fetchedAt || utcNow();
  const prev = db.prepare('SELECT active_staff_count, last_generated_at FROM pk_staff_sync_state WHERE id = 1').get();

  const fail = (error) => {
    const r = { ok: false, skipped: true, error, generatedAt };
    saveState(r);
    return r;
  };

  // ① 世代の巻き戻り (手動と自動が重なって古い応答が後着した場合)
  if (prev?.last_generated_at && Date.parse(generatedAt) < Date.parse(prev.last_generated_at)) {
    return fail(`取得したデータ (${generatedAt}) が前回 (${prev.last_generated_at}) より古いため適用しません`);
  }
  // ② 入力検証 (1件でも壊れていたら全体を拒否)
  const { staff, errs } = validateStaff(payload.staff);
  if (errs.length) return fail(`スタッフマスタの内容が不正なため適用しません: ${errs.slice(0, 3).join(' / ')}${errs.length > 3 ? ` ほか${errs.length - 3}件` : ''}`);
  if (staff.length === 0) return fail('スタッフが0件のため適用しません (前回の作業者を保持)');
  const activeStaff = staff.filter(isActive);
  // ③ 激減ガード: 前回**成功時の有効スタッフ数**と比べる (local 一時要員を含む現在値と比べない)
  if (prev?.active_staff_count > 0 && activeStaff.length * 2 < prev.active_staff_count) {
    return fail(`有効スタッフ ${activeStaff.length} 名が前回 ${prev.active_staff_count} 名の半分未満のため適用しません (前回を保持)`);
  }

  const workers = db.prepare('SELECT code, name, sort, active, staff_id, staff_no, source FROM pk_workers').all();
  // ④ identity conflict: 同じ staff_id なのに staff_no が違う = staff DB 再作成で id が再採番された兆候
  const byStaffId = new Map(workers.filter(w => w.staff_id != null).map(w => [w.staff_id, w]));
  for (const s of staff) {
    const w = byStaffId.get(s.id);
    if (w && norm(w.staff_no) && norm(w.staff_no) !== s.staff_no) {
      return fail(`スタッフの対応が食い違っています (${w.code} は ${w.staff_no} でしたが ${s.staff_no} が来ました)。`
        + 'スタッフマスタを作り直した可能性があるため適用しません — 管理者に連絡してください');
    }
  }

  const result = db.transaction(() => {
    // 名前一致の候補 (未紐付けのみ)。照合キーは空白を無視する (表記ゆれ対策)。
    // 同じキーの行が2つ以上ある名前は「曖昧」として紐付けに使わない
    const nameCount = new Map();
    for (const w of workers) {
      if (w.staff_id != null) continue;
      const k = nameKey(w.name);
      if (k) nameCount.set(k, (nameCount.get(k) || 0) + 1);
    }
    const byName = new Map();
    for (const w of workers) {
      if (w.staff_id != null) continue;
      const k = nameKey(w.name);
      if (k && nameCount.get(k) === 1) byName.set(k, w);
    }
    const ambiguous = new Set([...nameCount.entries()].filter(([, c]) => c > 1).map(([n]) => n));

    const link = db.prepare("UPDATE pk_workers SET staff_id = ?, staff_no = ?, source = 'staff' WHERE code = ?");
    const rename = db.prepare('UPDATE pk_workers SET name = ? WHERE code = ?');
    const setActive = db.prepare('UPDATE pk_workers SET active = ? WHERE code = ?');
    const setNo = db.prepare('UPDATE pk_workers SET staff_no = ? WHERE code = ?');

    // code は既存を一度 Set 化して最大番号から採番 (行ごとの COUNT/EXISTS を避ける)
    const usedCodes = new Set(workers.map(w => w.code));
    let seq = workers.reduce((m, w) => { const n = /^w(\d+)$/.exec(w.code); return n ? Math.max(m, Number(n[1])) : m; }, 0);
    const nextCode = () => { do { seq++; } while (usedCodes.has(`w${String(seq).padStart(2, '0')}`)); const c = `w${String(seq).padStart(2, '0')}`; usedCodes.add(c); return c; };

    let linked = 0, added = 0, renamed = 0, deactivated = 0;
    const touched = new Set();
    const conflicts = [];

    for (const s of staff) {
      const name = tapName(s);
      const active = isActive(s) ? 1 : 0;
      let w = byStaffId.get(s.id);
      if (!w) {
        // 曖昧な名前 (pk_workers に同名が複数) は触らない — 別人に紐づく事故を防ぐ
        if (ambiguous.has(nameKey(name)) || ambiguous.has(nameKey(s.display_name))) {
          conflicts.push(`⚠ ${s.staff_no} ${s.display_name}: 同じ名前の作業者が複数いるため紐付けを保留しました (どちらかの名前を直してください)`);
          continue;
        }
        w = byName.get(nameKey(name)) || byName.get(nameKey(s.display_name));
        if (w) {
          link.run(s.id, s.staff_no, w.code);
          byName.delete(nameKey(w.name));
          w = { ...w, staff_id: s.id, staff_no: s.staff_no };
          byStaffId.set(s.id, w);
          linked++;
        }
      }
      if (!w) {
        if (!active) continue;   // 退職者を新規に生やさない
        const code = nextCode();
        db.prepare("INSERT INTO pk_workers (code, name, sort, active, staff_id, staff_no, source) VALUES (?, ?, ?, 1, ?, ?, 'staff')")
          .run(code, name, s.sort ?? seq, s.id, s.staff_no);
        touched.add(code);
        added++;
        continue;
      }
      touched.add(w.code);
      // ⭐**空白の有無だけの違いでは改名しない**: 現場の表記 (「中原大輔」) で過去の作業実績が
      //   記録されているため、「中原 大輔」に変えると実績の集計が新旧で分断される。
      //   スタッフマスタで実質的に名前が変わったとき (「中原」「星さん」等) だけ追従する。
      //   表記を完全に揃えたいときはスタッフマスタ側の「短い表記」に現場の表記を入れる
      if (nameKey(w.name) !== nameKey(name)) { rename.run(name, w.code); renamed++; }
      if (w.active !== active) { setActive.run(active, w.code); if (!active) deactivated++; }
      if (norm(w.staff_no) !== s.staff_no) setNo.run(s.staff_no, w.code);
    }

    // 触らなかった有効行の内訳。紐付け済みなのに export に無い = 異常 (削除された) として区別する
    const leftover = db.prepare('SELECT code, name, active, source, staff_id, staff_no FROM pk_workers WHERE active = 1').all()
      .filter(w => !touched.has(w.code));
    const missingLinked = leftover.filter(w => w.staff_id != null)
      .map(w => `❗ ${w.name} (${w.code} / ${w.staff_no || 'staff_id ' + w.staff_id}) がスタッフマスタから消えています — 確認してください`);
    const unlinkedLocal = leftover.filter(w => w.staff_id == null).map(w => `${w.name} (この画面で追加)`);
    const warnings = [
      ...conflicts,
      ...missingLinked,
      ...(unlinkedLocal.length ? [`スタッフマスタ未登録: ${unlinkedLocal.join('、')}`] : []),
    ];
    return {
      ok: true, staffCount: staff.length, activeStaffCount: activeStaff.length,
      linked, added, renamed, deactivated, warnings, generatedAt,
      // 互換 (既存の呼び出し・表示が unmatched を見ている場合)
      unmatched: warnings,
    };
  }).immediate();

  saveState(result);
  return result;
}

// 手動 (管理画面) と自動 (poller) が重ならないようにするプロセス内 mutex
let _running = null;

/** 取得 → 反映。env 未設定なら skip (状態は書かない) */
export async function syncStaff({ fetchFn = fetch } = {}) {
  if (!isStaffSyncConfigured()) return { ok: false, skipped: true, error: 'STAFF_EXPORT_TOKEN 未設定' };
  if (_running) return _running;   // 実行中なら同じ結果を待つ (二重取得・後着の適用を避ける)
  _running = (async () => {
    let payload;
    try {
      payload = await fetchStaffExport(fetchFn);
    } catch (e) {
      const r = { ok: false, skipped: true, error: `取得に失敗: ${e.message}` };
      try { saveState(r); } catch { /* 状態保存の失敗で本体を落とさない */ }
      return r;
    }
    try {
      return applyStaffExport(payload, { fetchedAt: utcNow() });
    } catch (e) {
      // 制約違反等でトランザクションが落ちた場合も「失敗した事実」を残す (黙って緑にしない)
      const r = { ok: false, skipped: true, error: `反映に失敗: ${e.message}` };
      try { saveState(r); } catch { /* 同上 */ }
      return r;
    }
  })();
  try {
    return await _running;
  } finally {
    _running = null;
  }
}
