#!/usr/bin/env node
/**
 * master-cutover.mjs — 商品マスタの切替の段階を見る・証拠つきで 1 段進める (手の操作。Company DB構想 10 §8 切替日の手順・0050・lib/master-cutover.mjs)
 *
 * 使い方:
 *   node -r dotenv/config scripts/company-db/master-cutover.mjs --status
 *   node -r dotenv/config scripts/company-db/master-cutover.mjs --to frozen --actor someone@b-faith.biz --evidence evidence.json [--note "理由"] --yes
 * 接続 = env COMPANY_DB_MASTER_OPS_URL (運用のロール master_ops = 段階を進める関数を実行できるだけ。create-master-edit-roles.mjs が作る) か --url
 * 段階: legacy_open → frozen → company_owner → new_open (1 段ずつ・戻さない。DB の関数 ops.set_master_cutover_phase が門の記録と証拠を確かめる):
 *   共通の証拠 { expected_builds: { render: [build_id, ...], minipc: [...] }, manifest_hash, owner_hash }
 *     + 門の記録 (ops.master_legacy_gate_acks・古い入口の門 = ⑤-3 が起動時と定期に書く・ログインは場所ごと) が、各場所に 15 分以内に 1 つ以上あり、
 *       直近 15 分に記録した全部の実体 (instance_id) の build が expected_builds にあり、manifest_hash・owner_hash が証拠と同じ。
 *       24 時間以内に記録があるのに最後の記録が 15 分より前の実体 (黙っている) があれば進めない = 止めた実体は stopped の記録を書く (⑤-3 の CLI・正しく終わるとき)
 *   → frozen:        + manual_entries_stopped: [{ id, by, at }, ...] (id の集まり = manifest の手の入口 kind='manual' と完全に同じ)
 *                    + drain: { done: true, checked_by, checked_at }。owner_hash = いまの持ち主表 (全部 load) のハッシュ
 *   → company_owner: owner_hash = 新しい持ち主表のハッシュ。門の記録は frozen に入った後・phase_seen = 'frozen'・処理中 0
 *   → new_open:      owner_hash は company_owner と同じ。門の記録は company_owner に入った後・phase_seen = 'company_owner'・処理中 0
 *   ops.master_cutover_prereq_problems(from, to) が問題を返したら、どの段階も進めない (後の PR が条件を足す口)
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
