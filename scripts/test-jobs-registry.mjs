/**
 * 定期実行の台帳 (config/jobs-registry.mjs) の形の検査
 * 実行: node scripts/test-jobs-registry.mjs
 *
 * 🚨 きっかけ (2026-10-02): `runbook` に `'C:\tools\ph-nightly\logs\...'` と
 *    **シングルクォートの中で 1 本のバックスラッシュ**を書いていたため、`\t` が JS の
 *    エスケープ (タブ) として消え、監視を読む人に `C:<TAB>oolsph-nightlylogs...` と出ていた。
 *    Windows のパスは台帳のほぼ全エントリに出てくるので、次の人も同じ落とし方をする。
 *    = 目で見るのではなく機械で止める。
 *
 * ここは**台帳の形だけ**を見る。「ジョブが実際に動いているか」は jobs-monitor の仕事。
 */
import { JOBS_REGISTRY } from '../config/jobs-registry.mjs';

let pass = 0, fail = 0;
const ok = (c, l) => { if (c) { pass++; console.log(`  ✓ ${l}`); } else { fail++; console.log(`  ✗ ${l}`); } };

const TYPES = ['scheduled_job', 'heartbeat', 'human_obligation', 'temporary_asset'];
const IMPORTANCE = ['P1', 'P2', 'P3', 'TMP'];

console.log('① id は重複しない');
{
  const ids = JOBS_REGISTRY.map((j) => j.id);
  const dup = ids.filter((x, i) => ids.indexOf(x) !== i);
  ok(dup.length === 0, `id の重複が無い${dup.length ? ' — ' + [...new Set(dup)].join(', ') : ''}`);
  ok(ids.every((x) => typeof x === 'string' && x.length > 0), 'id が全部ある');
}

console.log('② type と importance が決まった値');
{
  const badType = JOBS_REGISTRY.filter((j) => !TYPES.includes(j.type)).map((j) => `${j.id}(${j.type})`);
  ok(badType.length === 0, `type が決まった値${badType.length ? ' — ' + badType.join(', ') : ''}`);
  const badImp = JOBS_REGISTRY.filter((j) => !IMPORTANCE.includes(j.importance)).map((j) => `${j.id}(${j.importance})`);
  ok(badImp.length === 0, `importance が決まった値${badImp.length ? ' — ' + badImp.join(', ') : ''}`);
}

console.log('③ 型ごとに要る項目がある');
{
  const miss = [];
  for (const j of JOBS_REGISTRY) {
    for (const k of ['owner', 'purpose', 'where', 'runbook']) {
      if (typeof j[k] !== 'string' || !j[k].trim()) miss.push(`${j.id}.${k}`);
    }
    // heartbeat は経過時間で生死を見るので max_age_hours が無いと判定できない
    if (j.type === 'heartbeat' && !(Number(j.max_age_hours) > 0)) miss.push(`${j.id}.max_age_hours`);
    if (j.type === 'human_obligation' && !(Number(j.period_hours) > 0)) miss.push(`${j.id}.period_hours`);
    if (j.type === 'temporary_asset' && !j.remove_by) miss.push(`${j.id}.remove_by`);
  }
  ok(miss.length === 0, `要る項目がそろっている${miss.length ? ' — ' + miss.join(', ') : ''}`);
}

console.log('④ 🚨 文字列に制御文字が紛れていない (バックスラッシュの書き損じ)');
{
  // '\t' '\b' '\f' '\v' '\0' — Windows のパスを 1 本のバックスラッシュで書くとこうなる
  const CTRL = /[\t\b\f\v\0\x01-\x08\x0b\x0c\x0e-\x1f]/;
  const bad = [];
  for (const j of JOBS_REGISTRY) {
    for (const [k, v] of Object.entries(j)) {
      if (typeof v === 'string' && CTRL.test(v)) bad.push(`${j.id}.${k}`);
    }
  }
  ok(bad.length === 0,
    `制御文字が入っていない${bad.length ? ' — ' + bad.join(', ') + ' (Windows のパスは \\\\ で書く)' : ''}`);
}

console.log('⑤ 🚨 Windows のパスの区切りが消えていない');
{
  // 単独の英字 + コロン の後ろが区切りでない = バックスラッシュが消えた跳。
  //   'C:\tools'  → 	 がタブになる (検査 ④ でも捕まる)
  //   'C:\ph-nightly' → \p はそのまま消える → 'C:ph-nightly' (検査 ④ では捕まらない)
  // 直前が英字のものは除く — `env:` `HH:MM` などを拾わないため。
  const EATEN = new RegExp('(?<![A-Za-z])[A-Za-z]:(?![\\\\/])');
  const bad = [];
  for (const j of JOBS_REGISTRY) {
    for (const [k, v] of Object.entries(j)) {
      if (typeof v === 'string' && EATEN.test(v)) bad.push(`${j.id}.${k} → ${JSON.stringify((v.match(EATEN) || [''])[0])}`);
    }
  }
  ok(bad.length === 0,
    `ドライブ名の後ろの区切りが生きている${bad.length ? ' — ' + bad.join(' / ') + ' (Windows のパスは \\\\ で書く)' : ''}`);
}

console.log('⑥ LP構成の AI 生成が載っている (段階1)');
{
  const j = JOBS_REGISTRY.find((x) => x.id === 'ph-lp-compose');
  ok(!!j, '台帳にある (載っていないものは監視されない)');
  if (j) {
    ok(j.type === 'heartbeat', '1 分おきなので heartbeat');
    ok(Number(j.max_age_hours) === 1, 'ping が 1 時間途切れたら止まっている');
    ok(j.runbook.includes('lp-compose.log'), 'runbook にログの置き場が書いてある');
    ok(j.where.includes('PH_LP_COMPOSE_ENABLED'), 'フラグが要ることが書いてある');
  }
}

console.log(fail ? `\n❌ ${pass} 件成功 / ${fail} 件失敗` : `\n✅ ${pass} 件成功 / 0 件失敗`);
process.exit(fail ? 1 : 0);
