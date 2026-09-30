#!/usr/bin/env node
/**
 * master-cutover.mjs — 商品マスタの切替の段階を見る・証拠つきで 1 段進める (手の操作。Company DB構想 10 §8 切替日の手順・0050・lib/master-cutover.mjs)
 *
 * 使い方:
 *   node -r dotenv/config scripts/company-db/master-cutover.mjs --status
 *   node -r dotenv/config scripts/company-db/master-cutover.mjs --to frozen --actor someone@b-faith.biz --evidence evidence.json [--note "理由"] --yes
 * 接続 = env COMPANY_DB_MASTER_OPS_URL (運用のロール master_ops = 段階を進める関数を実行できるだけ。create-master-edit-roles.mjs が作る) か --url
 * 段階: legacy_open → frozen → company_owner → new_open (1 段ずつ・戻さない。DB の関数が門と証拠を確かめる):
 *   → frozen:        証拠 { drain: { done: true, checked_by, checked_at }, manual_entries_stopped: [{ entry, stopped_by, stopped_at }, ...] }
 *                    + render・minipc が古い入口の門を持つ版で起動した記録 (⑤-3 の門が書く = ⑤-3 を配るまで進めない)
 *   → company_owner: 証拠 { owner_hash } + 全部の場所が frozen の後にそのハッシュで起動した記録
 *   → new_open:      証拠 { owner_hash } (同じもの) + 全部の場所が company_owner の後にそのハッシュで起動した記録
 * 🚨 --to は切替日の手順書の順番でだけ使う (定期実行にしない)。--yes が無ければ何もしない
 * 終了コード: 0 = 成功 / 1 = 失敗 / 2 = 引数不正
 */
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient, pgAdapter } from './migrate.mjs';
import { readCutoverPhase, advanceCutoverPhase, CUTOVER_PHASES, PHASE_LABELS } from '../../lib/master-cutover.mjs';

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  const url = getArg('--url') || process.env.COMPANY_DB_MASTER_OPS_URL;
  if (!url) { console.error('COMPANY_DB_MASTER_OPS_URL (または --url) が要る'); process.exit(2); }
  const to = getArg('--to');
  if (!args.includes('--status') && !to) { console.error('--status か --to <段階> を付ける'); process.exit(2); }
  if (to && !CUTOVER_PHASES.includes(to)) { console.error(`--to は ${CUTOVER_PHASES.join(' / ')} のどれか`); process.exit(2); }
  let evidence = null;
  if (to) {
    const file = getArg('--evidence');
    if (!file) { console.error('--evidence <証拠の JSON ファイル> が要る'); process.exit(2); }
    try { evidence = JSON.parse(fs.readFileSync(file, 'utf8')); } catch (e) { console.error(`証拠の JSON が読めない: ${e.message}`); process.exit(2); }
  }
  const client = await openPgClient(url, { application_name: 'master-cutover' });
  try {
    const db = pgAdapter(client);
    const s = await readCutoverPhase(db);
    console.log(s.readable ? `今の段階: ${s.phase} = ${PHASE_LABELS[s.phase]} (${s.changed_at} ${s.changed_by}${s.owner_hash ? ` 持ち主表 ${s.owner_hash.slice(0, 12)}…` : ''})` : `読めない (= 閉じている扱い): ${s.error}`);
    if (to) {
      const actor = getArg('--actor');
      if (!actor) { console.error('--actor (進める人のメール) が要る'); process.exitCode = 2; }
      else if (!args.includes('--yes')) console.log(`--yes が無いので進めない (${s.phase} → ${to} の予定)`);
      else {
        const r = await advanceCutoverPhase(db, { to, actor, evidence, note: getArg('--note') });
        console.log(`進めた: ${r.from} → ${r.to} (確かめた場所: ${(r.acks || []).map((a) => `${a.host}@${a.build_id}`).join(', ') || 'なし'})`);
      }
    }
  } catch (e) {
    console.error(`失敗: ${e.message}`);
    process.exitCode = 1;
  } finally { await client.end(); }
}
