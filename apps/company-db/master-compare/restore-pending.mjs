/**
 * restore-pending.mjs — 照合 ② の反映待ちの台帳を、ある回の全件 JSON から作り直す (人が原因を直した後に 1 回だけ)
 *
 * いつ: 朝の要約に「⚠️ ②: 反映待ちの台帳が使えない (write_failed / untrusted: previous_write_failed / write_interrupted)」が出たとき。
 *   ディスク・権限などの原因を直してから、**台帳の保存に失敗した回 (または最後に正常に走った回) の全件 JSON** を指定する。
 *   その回の ne.pending_entries (書こうとした台帳の中身 = 反映待ちの始まり) から新しい台帳を作る = 期限を後ろへずらさない
 * 使い方 (miniPC): node apps/company-db/master-compare/restore-pending.mjs --from <全件 JSON のパス> [--data-dir D]
 *   全件 JSON は DATA_DIR/cdb-master-compare/<日付>/<compare_run_id>.json。daily-sync・retry が走っていない時間に流す
 */
import fs from 'node:fs';
import { restoreLedger } from './pending.mjs';
import { RESULT_DIR, makeCompareRunId } from './run.mjs';

const args = process.argv.slice(2);
const get = (k) => { const i = args.indexOf(k); return i >= 0 ? args[i + 1] : null; };
const from = get('--from');
const dataDir = (get('--data-dir') || process.env.DATA_DIR || '').trim();
if (!from || !dataDir) { console.error('使い方: node restore-pending.mjs --from <全件 JSON> [--data-dir D]'); process.exit(2); }
const result = JSON.parse(fs.readFileSync(from, 'utf8'));
const r = restoreLedger(dataDir, RESULT_DIR, { result, compareRunId: makeCompareRunId(new Date()) });
console.log(`台帳を作り直した: ${r.entries} 件 (元 = ${r.from}) → 版 ${r.compare_run_id}`);
