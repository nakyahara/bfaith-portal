#!/usr/bin/env node
/**
 * master-legacy-instance.mjs — 門の記録のプロセス (instance) を見る・止まったプロセスに「止めた」を書く (手の操作。⑤-3・中間レビューの続き)
 *
 * なぜ: 段階を進める DB の関数 (⑤-1) は「今までに記録を書いたプロセスで、15 分以内の記録も『止めた』も無いもの (何日前でも)」があれば進めない
 *   (黙って止まった = 古い版のまま動いているかもしれない)。ふつうは server.js が止まるとき (SIGTERM / SIGINT) に「止めた」を書くが、
 *   落ちた・電源が切れた・2 秒で書けなかったプロセスは残る → 人が確かめて (本当に止まっている) ここで「止めた」を書く。
 * 使い方:
 *   node -r dotenv/config scripts/company-db/master-legacy-instance.mjs --list                  # 最後が「止めた」でないプロセス (何日前でも = 段階を止めうる) と、24 時間以内に「止めた」を書いたプロセスの、最後の記録・新しいか・止めたか
 *   node -r dotenv/config scripts/company-db/master-legacy-instance.mjs --stop --host minipc --instance <名札> --reason "再起動で消えた" --yes
 *     書くのは、その場所の門のログイン (COMPANY_DB_MASTER_GATE_RENDER_URL / _MINIPC_URL) = ⑤-1 の関数が場所と役が同じかを確かめる
 *     🚨 15 分以内に記録があるプロセス (= 動いているかもしれない) は拒む。本当に止まったのを確かめたときだけ --force (中間レビュー 2 回目 Low)
 *        動いているプロセスを「止めた」にすると、段階を進める門がそのプロセス (古い版かもしれない) を見落とす
 * 見る接続先: COMPANY_DB_MASTER_OPS_URL (運用のロール) → COMPANY_DB_WATCH_URL (照会用)。
 * 🚨 定期実行にしない (人が止まったのを確かめてから)。--yes が無ければ書かない。終了コード 0 = 成功 / 1 = 失敗 / 2 = 引数
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient } from './migrate.mjs';
import { ackLegacyGates, gateUrlFor, GATE_URL_ENV } from '../../lib/master-legacy-gate.mjs';

/**
 * プロセスごとの最後の記録。出すのは「最後が『止めた』でない (何日前の記録でも)」か「hours 時間以内」のプロセス
 * (⑤-1 は、記録を書いたことのあるプロセスは全部「新しい記録」か「止めた」を求める = 年齢では外れない。#1563 R3)
 */
export async function listInstances(client, { hours = 24 } = {}) {
  return (await client.query(`select * from (
      select distinct on (host, instance_id) host, instance_id, build_id, phase_seen, inflight_count, acked_at, acked_at::text as acked_at_text, stopped, stopped_reason, session_role,
          acked_at >= clock_timestamp() - make_interval(mins => ops.master_cutover_ack_fresh_minutes()) as fresh
        from ops.master_legacy_gate_acks
        order by host, instance_id, acked_at desc, ack_id desc) last
      where not last.stopped or last.acked_at >= clock_timestamp() - make_interval(hours => $1)
      order by host, instance_id`, [hours])).rows.map(({ acked_at_text, ...r }) => ({ ...r, acked_at: acked_at_text }));
}

/** そのプロセスの最後の記録 (無ければ null)。見る接続先は --list と同じ */
async function latestAckOf({ host, instance, env }) {
  const url = String(env.COMPANY_DB_MASTER_OPS_URL || '').trim() || String(env.COMPANY_DB_WATCH_URL || '').trim();
  if (!url) throw Object.assign(new Error('15 分以内の記録が無いかを確かめる接続先 (COMPANY_DB_MASTER_OPS_URL か COMPANY_DB_WATCH_URL) が無い (確かめずに書くなら --force)'), { code: 1 });
  const c = await openPgClient(url, { application_name: 'master-legacy-instance' });
  try {
    return (await c.query(`select acked_at::text as acked_at, stopped,
        acked_at >= clock_timestamp() - make_interval(mins => ops.master_cutover_ack_fresh_minutes()) as fresh
      from ops.master_legacy_gate_acks where host = $1 and instance_id = $2 order by acked_at desc, ack_id desc limit 1`, [host, instance])).rows[0] || null;
  } finally { await c.end(); }
}

export async function markStopped({ host, instance, reason, env = process.env, connect = null, force = false, latest = latestAckOf }) {
  if (!['render', 'minipc'].includes(host)) throw Object.assign(new Error('--host は render か minipc'), { code: 2 });
  if (!instance || !/^[A-Za-z0-9_.:-]{1,100}$/.test(instance)) throw Object.assign(new Error('--instance (名札: 英数字と _.:- で 100 字まで) が要る'), { code: 2 });
  if (!reason || !String(reason).trim()) throw Object.assign(new Error('--reason (なぜ止まったと言えるか) が要る'), { code: 2 });
  if (String(reason).trim().length > 200) throw Object.assign(new Error('--reason は 200 字まで (0051 の約束)'), { code: 2 });
  if (!connect && !gateUrlFor(host, env)) throw Object.assign(new Error(`${GATE_URL_ENV[host]} が無い (その場所の門のログインで書く)`), { code: 1 });
  if (!force) {
    const last = await latest({ host, instance, env });
    if (last && last.fresh && !last.stopped) {
      throw Object.assign(new Error(`${host}/${instance} は 15 分以内 (${last.acked_at}) に記録がある = 動いているかもしれない。止まったのを確かめたなら --force`), { code: 1 });
    }
  }
  return ackLegacyGates({ host, env, connect, stopped: true, stoppedReason: String(reason), instance });
}

const isMain = process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const args = process.argv.slice(2);
  const getArg = (f) => { const i = args.indexOf(f); return i >= 0 && i < args.length - 1 ? args[i + 1] : null; };
  try {
    if (args.includes('--list')) {
      const url = String(process.env.COMPANY_DB_MASTER_OPS_URL || '').trim() || String(process.env.COMPANY_DB_WATCH_URL || '').trim();
      if (!url) { console.error('COMPANY_DB_MASTER_OPS_URL か COMPANY_DB_WATCH_URL が要る'); process.exitCode = 2; }
      else {
        const c = await openPgClient(url, { application_name: 'master-legacy-instance' });
        try {
          const rows = await listInstances(c, { hours: Number(getArg('--hours') || 24) });
          for (const r of rows) console.log(`${r.host}\t${r.instance_id}\t最後 ${r.acked_at}\t${r.stopped ? `止めた (${r.stopped_reason})` : r.fresh ? '新しい' : '⚠️ 黙っている = 15 分より前で「止めた」も無い (段階を進められない。止まったのを確かめて --stop)'}\t段階 ${r.phase_seen}\t書きかけ ${r.inflight_count}\tbuild ${String(r.build_id).slice(0, 12)}`);
          if (!rows.length) console.log('(止めていないプロセスも、24 時間以内に止めたプロセスも無い)');
        } finally { await c.end(); }
      }
    } else if (args.includes('--stop')) {
      if (!args.includes('--yes')) { console.log('--yes が無いので書かない (確かめてから --yes を付ける)'); }
      else {
        const r = await markStopped({ host: getArg('--host'), instance: getArg('--instance'), reason: getArg('--reason'), force: args.includes('--force') });
        console.log(`${r.state}: ${r.detail}`);
        process.exitCode = r.state === 'stopped' ? 0 : 1;
      }
    } else { console.error('--list か --stop を付ける'); process.exitCode = 2; }
  } catch (e) {
    console.error(`失敗: ${e.message}`);
    process.exitCode = e.code === 2 ? 2 : 1;
  }
}
