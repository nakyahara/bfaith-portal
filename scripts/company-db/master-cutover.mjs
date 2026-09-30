#!/usr/bin/env node
/**
 * master-cutover.mjs — 商品マスタの切替の段階を見る・1 段進める (手の操作。Company DB構想 10 §8 切替日の手順・0050・lib/master-cutover.mjs)
 *
 * 使い方:
 *   COMPANY_DB_URL=postgres://... node scripts/company-db/master-cutover.mjs --status
 *   COMPANY_DB_URL=postgres://... node scripts/company-db/master-cutover.mjs --to frozen --actor someone@b-faith.biz [--note "理由"] --yes
 * 段階: legacy_open → frozen → company_owner → new_open (1 段ずつ・戻さない。DB の関数が守る)
 * 🚨 --to は切替日の手順書の順番でだけ使う (定期実行にしない)。--yes が無ければ何もしない
 * 終了コード: 0 = 成功 / 1 = 失敗 / 2 = 引数不正
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { readCutoverPhase, advanceCutoverPhase, CUTOVER_PHASES, PHASE_LABELS } from '../../lib/master-cutover.mjs';

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const url = getArg('--url') || process.env.COMPANY_DB_URL;
  if (!url) { console.error('COMPANY_DB_URL (または --url) が要る'); process.exit(2); }
  const to = getArg('--to');
  if (!args.includes('--status') && !to) { console.error('--status か --to <段階> を付ける'); process.exit(2); }
  if (to && !CUTOVER_PHASES.includes(to)) { console.error(`--to は ${CUTOVER_PHASES.join(' / ')} のどれか`); process.exit(2); }
  const client = await openPgClient(url, { application_name: 'master-cutover' });
  try {
    const db = pgAdapter(client);
    const s = await readCutoverPhase(db);
    console.log(s.readable ? `今の段階: ${s.phase} = ${PHASE_LABELS[s.phase]} (${s.changed_at} ${s.changed_by})` : `読めない (= 閉じている扱い): ${s.error}`);
    if (to) {
      const actor = getArg('--actor');
      if (!actor) { console.error('--actor (進める人のメール) が要る'); process.exitCode = 2; }
      else if (!args.includes('--yes')) console.log(`--yes が無いので進めない (${s.phase} → ${to} の予定)`);
      else {
        const r = await advanceCutoverPhase(db, { to, actor, note: getArg('--note') });
        console.log(`進めた: ${r.from} → ${r.to}`);
      }
    }
  } catch (e) {
    console.error(`失敗: ${e.message}`);
    process.exitCode = 1;
  } finally { await client.end(); }
}
