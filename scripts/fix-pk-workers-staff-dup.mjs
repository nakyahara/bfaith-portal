/**
 * 復旧: スタッフマスタ同期の二重登録を片付ける (2026-09-01 の実障害)
 *
 * 何が起きたか:
 *   picking の既存作業者は「中原大輔」(空白なし)、スタッフマスタは「中原 大輔」(空白あり) で、
 *   初回同期の名前照合が完全一致だったため紐付かず、同じ人が2行ずつ登録された
 *   (現場の名前タップが 13 → 23 名になった)。
 *
 * このスクリプトがすること:
 *   同期が作った行 (source='staff') のうち、
 *     ① 空白を無視した名前が同じ既存行 (source='local') がある
 *     ② その staff 行に**作業実績が1件も無い** (pk_batches / pk_pack_events 等で名前が使われていない)
 *   を満たすものだけを削除し、staff_id / staff_no を既存の local 行へ付け替える。
 *   実績のある行は絶対に消さない (1件でもあれば中止して報告する)。
 *
 * 実行:
 *   node scripts/fix-pk-workers-staff-dup.mjs            # 確認のみ (何も書き換えない)
 *   node scripts/fix-pk-workers-staff-dup.mjs --apply    # 実行
 *   DATA_DIR を picking.db のあるディレクトリに向けること (miniPC: C:\tools\bfaith-picking\data)
 */
import path from 'path';
import fs from 'fs';
import Database from 'better-sqlite3';

const APPLY = process.argv.includes('--apply');
const DATA_DIR = process.env.DATA_DIR || path.join(process.cwd(), 'data');
const DB_FILE = path.join(DATA_DIR, 'picking.db');
if (!fs.existsSync(DB_FILE)) {
  console.error(`picking.db が見つかりません: ${DB_FILE} (DATA_DIR を指定してください)`);
  process.exit(1);
}
const db = new Database(DB_FILE);
db.pragma('foreign_keys = ON');

const nameKey = s => String(s == null ? '' : s).trim().normalize('NFKC').replace(/[\s　]+/g, '');

/** その名前が作業実績で使われている件数 (テーブルが無い版でも落ちないように存在を見てから数える) */
function usageOf(name) {
  const targets = [
    ['pk_batches', 'worker'],
    ['pk_events', 'worker'],
    ['pk_pack_events', 'worker'],
    ['pk_pack_batches', 'worker'],
  ];
  let total = 0;
  const detail = {};
  for (const [table, col] of targets) {
    const exists = db.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?").get(table);
    if (!exists) continue;
    const hasCol = db.prepare(`PRAGMA table_info(${table})`).all().some(c => c.name === col);
    if (!hasCol) continue;
    const c = db.prepare(`SELECT COUNT(*) c FROM ${table} WHERE ${col} = ?`).get(name).c;
    if (c) detail[table] = c;
    total += c;
  }
  return { total, detail };
}

const workers = db.prepare('SELECT code, name, sort, active, staff_id, staff_no, source FROM pk_workers ORDER BY code').all();
const locals = workers.filter(w => w.source !== 'staff');
const staffRows = workers.filter(w => w.source === 'staff');

// 空白無視キー → local 行 (同キーが2行以上ある場合は曖昧なので触らない)
const localByKey = new Map();
const localDupKeys = new Set();
for (const w of locals) {
  const k = nameKey(w.name);
  if (!k) continue;
  if (localByKey.has(k)) { localDupKeys.add(k); continue; }
  localByKey.set(k, w);
}

const plan = [];
const blocked = [];
for (const s of staffRows) {
  const k = nameKey(s.name);
  if (localDupKeys.has(k)) { blocked.push({ staff: s, why: '同名の既存行が複数あるため自動では判断できない' }); continue; }
  const local = localByKey.get(k);
  if (!local) continue;                       // 既存行が無い = 正しく新規追加された人
  if (local.staff_id != null && local.staff_id !== s.staff_id) {
    blocked.push({ staff: s, why: `既存行 ${local.code} は別の staff_id=${local.staff_id} に紐付いている` });
    continue;
  }
  const u = usageOf(s.name);
  if (u.total > 0) { blocked.push({ staff: s, why: `実績が ${u.total} 件あるため削除できない (${JSON.stringify(u.detail)})` }); continue; }
  plan.push({ staff: s, local, localUsage: usageOf(local.name).total });
}

console.log(`picking.db = ${DB_FILE}`);
console.log(`作業者 ${workers.length} 名 (同期由来 ${staffRows.length} / 既存 ${locals.length})\n`);
if (plan.length === 0 && blocked.length === 0) {
  console.log('二重登録は見つかりませんでした。何もしません。');
  process.exit(0);
}
console.log(`■ 片付ける対象 (${plan.length} 件): 同期が作った実績ゼロの行を消し、既存行に紐付けを移す`);
for (const p of plan) {
  console.log(`  - 削除 ${p.staff.code} 「${p.staff.name}」(実績0) → 残す ${p.local.code} 「${p.local.name}」(実績 ${p.localUsage} 件) に staff_id=${p.staff.staff_id} / ${p.staff.staff_no} を移す`);
}
if (blocked.length) {
  console.log(`\n■ 触らないもの (${blocked.length} 件) — 人が判断してください`);
  for (const b of blocked) console.log(`  - ${b.staff.code} 「${b.staff.name}」: ${b.why}`);
}
if (!APPLY) {
  console.log('\n(確認のみ。実行するには --apply を付けてください)');
  process.exit(0);
}

const run = db.transaction(() => {
  for (const p of plan) {
    // 先に staff 行を消してから付け替える (staff_id の一意索引に引っかからないように)
    db.prepare('DELETE FROM pk_workers WHERE code = ?').run(p.staff.code);
    db.prepare("UPDATE pk_workers SET staff_id = ?, staff_no = ?, source = 'staff', active = 1 WHERE code = ?")
      .run(p.staff.staff_id, p.staff.staff_no, p.local.code);
  }
  // 次回の同期が「消えた紐付け」と誤解しないよう、判定基準は据え置きにする (状態は次の同期で更新される)
});
run.immediate();

const after = db.prepare('SELECT COUNT(*) c FROM pk_workers WHERE active = 1').get().c;
console.log(`\n✅ ${plan.length} 件を片付けました。有効な作業者は ${after} 名になりました。`);
console.log('管理画面 (/apps/picking/admin/devices) で名前タップの一覧を確認してください。');
