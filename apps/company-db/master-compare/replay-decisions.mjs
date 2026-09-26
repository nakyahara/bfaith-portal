/**
 * replay-decisions.mjs — 照合 ② の判断の候補を、ある回の全件 JSON から台帳 (Company DB) に入れ直す (台帳を書けなかった回の復旧。D1 契約 v3)
 *
 * 全件 JSON の ne.decisions (指紋・指紋の元・選べる解決・意味の版) を**そのまま**使う (再計算しない)。同じ回を二重に数えない・最後に見た日時を巻き戻さない (関数が守る)。
 * 完了 (action_done) は入れ直さない (到達が続いていれば次の照合がまた書く)
 * 使い方 (miniPC): node -r dotenv/config apps/company-db/master-compare/replay-decisions.mjs --from <全件 JSON>
 *   env: COMPANY_DB_WATCH_WRITER_URL (watch_writer)
 */
import fs from 'node:fs';
import { writeDecisions, connectDecisionWriter } from './decisions.mjs';

const args = process.argv.slice(2);
const i = args.indexOf('--from');
const from = i >= 0 ? args[i + 1] : null;
const url = (process.env.COMPANY_DB_WATCH_WRITER_URL || '').trim();
if (!from || !url) { console.error('使い方: node -r dotenv/config replay-decisions.mjs --from <全件 JSON> (env COMPANY_DB_WATCH_WRITER_URL)'); process.exit(2); }
const res = JSON.parse(fs.readFileSync(from, 'utf8'));
const obs = res.ne && res.ne.decisions_observed;
if (!obs || !obs.compare_run_id || !obs.observed_at || !Array.isArray(res.ne.decisions)) { console.error('全件 JSON に ne.decisions / ne.decisions_observed が無い (0032 の後の照合の JSON を指定する)'); process.exit(2); }
const w = await connectDecisionWriter(url);
try {
  const x = await writeDecisions(w.db, { compareRunId: obs.compare_run_id, observedAt: obs.observed_at, decisions: res.ne.decisions, done: [] });
  console.log(`入れ直した: 新しい観測 ${x.candidates} 件 (候補 ${res.ne.decisions.length} 件・回 ${obs.compare_run_id})`);
} finally { await w.close(); }
process.exitCode = 0;
setTimeout(() => process.exit(0), 5000).unref();
