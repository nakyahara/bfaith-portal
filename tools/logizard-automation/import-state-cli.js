/**
 * import-state-cli.js — ロジザードの取込の状態を人が見る・直す (マスタ正本切替 ③c-1b-1・契約 v3 H5・H6)
 *
 *   node import-state-cli.js status
 *   node import-state-cli.js init    --by <名前> --local <この PC の印のファイル> [--note "…"]      … 最初の 1 回だけ (ポータル + この PC の印)
 *   node import-state-cli.js adopt   --by <名前> --local <ファイル> --note "…" [--replace]         … もう 1 台の PC がポータルの識別子を印にする
 *   node import-state-cli.js recover --by <名前> --local <ファイル> --note "何を確かめたか"          … 消失からの復旧 (識別子を作り直す。もう 1 台は adopt --replace)
 *   node import-state-cli.js halt    --by <名前> --reason "…"                                   … 自動の取込を止める (戻し方の手の ③ の前)
 *   node import-state-cli.js resume  --by <名前> --note "…" --halt-revision <番号>              … 再開 (未解決の取込が無いときだけ。番号 = status の halt_revision = 見た止めと今の止めが同じときだけ)
 *   node import-state-cli.js resolve --by <名前> --run <実行 ID> --outcome imported|not_imported|partial --note "…" [--partial-ok] [--repaired]
 *        … ロジザードのインポート履歴を確かめてから。--partial-ok = 対象外の列に差が無く、差のある商品が全部次の夜の対象にあると確かめた
 * env: LZ_LOCK_TOKEN・LZ_IMPORT_STATE_URL (このフォルダの .env か、呼ぶ側の env)
 * 終了コード: 0 = できた, 1 = 断られた・届かない
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { createImportStateClient, readLocalInit, writeLocalInit } from './import-state-client.js';

function parse(argv) {
  const out = { cmd: argv[0] || 'status' };
  for (let i = 1; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a === '--by') out.by = next();
    else if (a === '--local') out.local = next();
    else if (a === '--note') out.note = next();
    else if (a === '--reason') out.reason = next();
    else if (a === '--run') out.run = next();
    else if (a === '--outcome') out.outcome = next();
    else if (a === '--replace') out.replace = true;
    else if (a === '--partial-ok') out.partialOk = true;
    else if (a === '--repaired') out.repaired = true;
    else if (a === '--halt-revision') out.haltRevision = next();
    else throw new Error(`知らない引数: ${a}`);
  }
  return out;
}

export async function main(argv, { client = null, log = console.log } = {}) {
  const a = parse(argv);
  const c = client || createImportStateClient();
  const need = (k) => { if (!a[k]) throw new Error(`--${k} が要る`); return a[k]; };
  switch (a.cmd) {
    case 'status': { const s = await c.status(20); log(JSON.stringify(s, null, 1)); return s; }
    case 'init': {
      const local = need('local');
      if (fs.existsSync(local)) throw new Error(`この PC の印がすでにある (${local}) = 初期化しない`);
      const r = await c.init({ by: need('by'), note: a.note || null });
      writeLocalInit(local, { initId: r.init_id, by: a.by, note: 'init' });
      log(`✅ 初期化 ${r.init_id} (この PC の印 = ${local})`); return r;
    }
    case 'adopt': {
      const local = need('local'); need('note');
      const s = await c.status(0);
      if (!s.initialized) throw new Error('ポータルがまだ初期化されていない');
      const cur = readLocalInit(local);
      if (cur && !a.replace) throw new Error(`この PC の印がすでにある (${cur.init_id})。書き換えるなら --replace (人が確かめてから)`);
      writeLocalInit(local, { initId: s.init_id, by: need('by'), note: `adopt: ${a.note}`, replace: !!cur });
      log(`✅ この PC の印 = ${s.init_id} (${local})`); return { init_id: s.init_id };
    }
    case 'recover': {
      const local = need('local');
      const r = await c.recover({ by: need('by'), note: need('note') });
      writeLocalInit(local, { initId: r.init_id, by: a.by, note: `recover: ${a.note}`, replace: fs.existsSync(local) });
      log(`✅ 作り直した ${r.init_id}${r.halted ? ' (自動の取込は止めたまま = 確かめてから resume)' : ''}。もう 1 台の PC は adopt --replace`); return r;
    }
    case 'halt': { const r = await c.halt({ by: need('by'), reason: need('reason') }); log('✅ 自動の取込を止めた'); return r; }
    case 'resume': {
      // 見た止めの番号 (status の halt_revision) が要る = 見た後に止め直されていたら断る (Codex #1542 R2 High)
      const hr = need('haltRevision');
      if (!/^[0-9]{1,12}$/.test(String(hr))) throw new Error('--halt-revision は番号 (status の halt_revision)');
      const r = await c.resume({ by: need('by'), note: need('note'), expected_halt_revision: Number(hr) }); log('✅ 自動の取込を再開した'); return r;
    }
    case 'resolve': {
      const r = await c.resolve({ by: need('by'), run_id: need('run'), outcome: need('outcome'), note: need('note'),
        partial_check: a.partialOk ? { non_target_unchanged: true, all_in_next_csv: true } : null, repaired: !!a.repaired });
      log(`✅ 解除した (${a.run} → ${r.state})。自動の取込を再開するなら resume`); return r;
    }
    default: throw new Error(`知らないコマンド: ${a.cmd}`);
  }
}

const isMain = (() => { try { return path.resolve(process.argv[1] || '').toLowerCase() === fileURLToPath(import.meta.url).toLowerCase(); } catch { return false; } })();
if (isMain) {
  try {
    // このフォルダの .env (LZ_LOCK_TOKEN など) を読む。無くても env から
    try { const { loadEnv } = await import('./logizard-common.js'); if (fs.existsSync(path.join(path.dirname(fileURLToPath(import.meta.url)), '.env'))) loadEnv(); } catch { /* */ }
    await main(process.argv.slice(2));
  } catch (e) {
    console.error(`❌ ${e.code ? `${e.code}: ` : ''}${e.message}`);
    process.exitCode = 1;
  }
}
