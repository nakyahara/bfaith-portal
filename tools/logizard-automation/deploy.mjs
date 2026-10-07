/**
 * deploy.mjs — ロジザードの自動化の正本 (このフォルダ) を、各 PC の C:\tools\logizard-automation へ写す
 *   (マスタ正本切替 ③c-1b-0・中原さんの答え L-10 = 正本はリポジトリ)
 *
 * 使い方 (その PC で、リポジトリを最新にしてから):
 *   node tools/logizard-automation/deploy.mjs --pc minipc                 … 何が変わるかを見るだけ (既定)
 *   node tools/logizard-automation/deploy.mjs --pc minipc --apply         … 写す (前のファイルは deploy-backup/<実行 ID>/ に残す)
 *   node tools/logizard-automation/deploy.mjs --pc minipc --check         … 写したものから変わっていないか・リポジトリより古くないか (違えば exit 1)
 *   node tools/logizard-automation/deploy.mjs --pc minipc --rollback <実行 ID>  … その回の前に戻す
 *   --target <dir> で写す先を変える (既定 C:\tools\logizard-automation)
 *
 * 決まり:
 *   - 写すのはコミット済みの中身だけ (このフォルダに未コミットの変更がある = 断る)。どのコミットを写したかを DEPLOYED.json に残す
 *   - 写す・戻すあいだは、ロジザードの自動化の鍵 (logs/ のセッションの鍵 2 つ) を自分で持つ。すでにある (動いている) = 断る
 *   - 1 ファイルずつ 一時ファイル → rename。写した後に sha256 を読み直して確かめる。途中で失敗 = それまでに替えたものを戻す (戻せなかったものは名前と理由を出す)
 *   - 戻せるのはいちばん新しい回だけ (2 回前へ = 1 回ずつ)。その回の後に写す先で直されていたら戻さない
 *   - .env・logs・out・downloads など manifest に無いものには触らない
 * 終了コード: 0 = できた / 変わりなし、1 = 断った・失敗・ずれがある
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

export const SRC_DIR = path.dirname(fileURLToPath(import.meta.url));
export const DEFAULT_TARGET = 'C:\\tools\\logizard-automation';
export const DEPLOYED_FILE = 'DEPLOYED.json';
export const BACKUP_DIR = 'deploy-backup';
export const RUNNING_LOCKS = ['logizard-session.lock', 'logizard-barcode.lock'];

const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
const readOrNull = (p) => { try { return fs.readFileSync(p); } catch (e) { if (e.code === 'ENOENT') return null; throw e; } };
export const makeDeployId = (now = new Date()) => `dep_${now.toISOString().replace(/[-:.]/g, '')}_${crypto.randomBytes(3).toString('hex')}`;

/** リポジトリの状態 (コミットと、このフォルダの未コミットの変更) */
export function realGit(srcDir) {
  const run = (args) => execFileSync('git', ['-C', srcDir, ...args], { encoding: 'utf8' }).trim();
  return { commit: run(['rev-parse', 'HEAD']), dirty: run(['status', '--porcelain', '--', '.']) !== '' };
}

export function readManifest(srcDir) {
  const m = JSON.parse(fs.readFileSync(path.join(srcDir, 'manifest.json'), 'utf8'));
  if (!m || !m.pcs || typeof m.pcs !== 'object') throw new Error('manifest.json の形が違う');
  return m;
}

/** 写す先の今のファイルと、正本を比べる */
export function plan({ srcDir, target, files }) {
  return files.map((name) => {
    if (path.basename(name) !== name) throw new Error(`manifest のファイル名が不正: ${name}`);
    const src = fs.readFileSync(path.join(srcDir, name));
    const dst = readOrNull(path.join(target, name));
    const status = dst == null ? 'new' : sha256(dst) === sha256(src) ? 'same' : 'changed';
    return { name, status, src_sha256: sha256(src), dst_sha256: dst == null ? null : sha256(dst) };
  });
}

function writeAtomicReal(file, buf) {
  const tmp = path.join(path.dirname(file), `.${path.basename(file)}.deploy-${process.pid}-${crypto.randomBytes(3).toString('hex')}.tmp`);
  fs.writeFileSync(tmp, buf, { flag: 'wx' });
  try { fs.renameSync(tmp, file); } catch (e) { try { fs.unlinkSync(tmp); } catch { /* */ } throw e; }
}

/**
 * 写す・戻すあいだ、ロジザードの自動化の鍵 (logs/ のセッションの鍵 2 つ) を**自分で持つ** (Codex #1512 R1 High)。
 * 形は logizard-common.js の acquireLock と同じ ({ pid, token, startedAt })。ほかの処理は「別の処理が実行中」で始まらない
 * (00:20 の bat は最大 10 分待つ)。1 つでもすでにある = 動いている (か異常終了の残り) = 断る。
 * @returns {{ ok: true, release: () => void } | { ok: false, busy: string }}
 */
export function holdLocks(target, { now = new Date() } = {}) {
  const dir = path.join(target, 'logs');
  fs.mkdirSync(dir, { recursive: true });
  const token = crypto.randomUUID();
  const held = [];
  const release = () => {
    for (const p of held) {
      try { const cur = JSON.parse(fs.readFileSync(p, 'utf8')); if (cur && cur.token === token) fs.unlinkSync(p); } catch { /* 無い・読めない = 触らない */ }
    }
  };
  for (const name of RUNNING_LOCKS) {
    const p = path.join(dir, name);
    try {
      fs.writeFileSync(p, JSON.stringify({ pid: process.pid, token, startedAt: now.toISOString(), by: 'deploy.mjs' }), { flag: 'wx' });
      held.push(p);
    } catch (e) {
      release();
      if (e.code === 'EEXIST') return { ok: false, busy: name };
      throw e;
    }
  }
  return { ok: true, release };
}

/**
 * 1 回分。戻り値 = { ok, action, reason?, plan?, deployId?, ... }
 * @param {object} p
 * @param {'minipc'|'streamdeck'} p.pc
 * @param {'plan'|'apply'|'check'|'rollback'} p.action
 * @param {object} [p.hooks]  試験で差し替える (readBack = 写した後の読み直し・writeAtomic = 1 ファイルを書く)
 */
export function deploy({ srcDir = SRC_DIR, target = DEFAULT_TARGET, pc, action = 'plan', rollbackId = null, git = realGit, now = new Date(), log = () => {}, hooks = {} }) {
  const readBack = hooks.readBack || ((file) => fs.readFileSync(file));
  const writeAtomic = hooks.writeAtomic || writeAtomicReal;
  const manifest = readManifest(srcDir);
  const files = manifest.pcs[pc];
  if (!Array.isArray(files) || !files.length) return { ok: false, action, reason: `manifest に PC「${pc}」が無い` };
  if (!fs.existsSync(target) || !fs.statSync(target).isDirectory()) return { ok: false, action, reason: `写す先が無い: ${target}` };
  const deployedPath = path.join(target, DEPLOYED_FILE);
  const readDeployed = () => { try { return JSON.parse(fs.readFileSync(deployedPath, 'utf8')); } catch { return null; } };

  if (action === 'check') {
    const p = plan({ srcDir, target, files });
    const rec = readDeployed();
    const drift = rec ? files.filter((n) => rec.files?.[n] !== (p.find((x) => x.name === n).dst_sha256)) : files;
    const outdated = p.filter((x) => x.status !== 'same').map((x) => x.name);
    const ok = !!rec && rec.pc === pc && !drift.length && !outdated.length;
    return { ok, action, deployed: rec ? { deploy_id: rec.deploy_id, commit: rec.commit, at: rec.at } : null, drift, outdated,
      reason: ok ? null : !rec ? 'まだ写していない (DEPLOYED.json が無い)' : rec.pc !== pc ? `別の PC として写した記録 (${rec.pc})` : drift.length ? '写した後に写す先で変わった' : 'リポジトリのほうが新しい' };
  }

  let g = null;
  if (action === 'plan' || action === 'apply') {
    g = git(srcDir);
    if (g.dirty) return { ok: false, action, reason: 'tools/logizard-automation に未コミットの変更がある (コミット済みの中身だけを写す)' };
  }
  if (action === 'plan') return { ok: true, action, commit: g.commit, plan: plan({ srcDir, target, files }) };
  if (action !== 'apply' && action !== 'rollback') return { ok: false, action, reason: `知らない action: ${action}` };

  // ── ここから先 (写す・戻す) は、ロジザードの自動化の鍵を持ったまま ──
  const lock = holdLocks(target, { now });
  if (!lock.ok) return { ok: false, action, reason: `ロジザードの自動化が動いている (${lock.busy})。終わってから (動いていないのに残っている = 中の pid を確かめて消す)` };
  try {
    return action === 'rollback' ? doRollback() : doApply();
  } finally {
    lock.release();
  }

  /**
   * 替えたものを前に戻し (読み直して確かめる)、**全部戻せたときだけ**足したものを消す (Codex #1512 R2 High)。
   * 替えたものが 1 つでも戻らない = 新しい版の呼び手が残る = 足したもの (その呼び手が読む部品) を消さずに残す (消すと次の定時が ERR_MODULE_NOT_FOUND)。
   * @returns {{ failed: Array<{ name, error }>, kept: string[] }}  kept = 戻しきれなかったので残した足したもの
   */
  function restore(names, { replacedSet, bdir }) {
    const failed = [];
    for (const name of names.filter((n) => replacedSet.has(n))) {
      const file = path.join(target, name);
      try {
        const want = fs.readFileSync(path.join(bdir, name));
        writeAtomic(file, want);
        if (sha256(fs.readFileSync(file)) !== sha256(want)) throw new Error('戻した後の中身が違う');
      } catch (e) { failed.push({ name, error: String(e && e.message).slice(0, 160) }); }
    }
    const addedNames = names.filter((n) => !replacedSet.has(n));
    if (failed.length) return { failed, kept: addedNames };
    for (const name of addedNames) {
      const file = path.join(target, name);
      try {
        try { fs.unlinkSync(file); } catch (e) { if (e.code !== 'ENOENT') throw e; }
        if (fs.existsSync(file)) throw new Error('消せない');
      } catch (e) { failed.push({ name, error: String(e && e.message).slice(0, 160) }); }
    }
    return { failed, kept: [] };
  }
  function failText(r, bdir) { return `${r.failed.map((x) => `${x.name} (${x.error})`).join('・')}${r.kept.length ? `・消さずに残した = ${r.kept.join('・')}` : ''}。前のファイル = ${bdir}`; }

  function doRollback() {
    // 戻せるのは**いちばん新しい回だけ** (古い回を戻すと、その後の回の変更と混ざる。Codex #1512 R1 Medium)。2 回前へ = 1 回ずつ戻す
    const rec = readDeployed();
    if (!rec || rec.deploy_id !== rollbackId) return { ok: false, action, reason: `戻せるのはいちばん新しい回だけ (今 = ${rec ? rec.deploy_id : 'なし'}・指定 = ${rollbackId})` };
    const bdir = path.join(target, BACKUP_DIR, rollbackId);
    let b;
    try { b = JSON.parse(fs.readFileSync(path.join(bdir, 'backup.json'), 'utf8')); } catch { return { ok: false, action, reason: `戻す記録が無い: ${rollbackId}` }; }
    // その回が写した中身から変わっていたら戻さない (人の直しを消さない)。
    // ただし前の戻しが途中で止まった続き = もう戻した (前のファイルと同じ)・もう消した は変わったと見ない (戻し直しを続けられる。Codex #1512 R2 High)
    const touched = [...b.replaced, ...b.added];
    const drift = touched.filter((n) => {
      const cur = readOrNull(path.join(target, n));
      if (b.replaced.includes(n)) return !cur || (sha256(cur) !== rec.files[n] && sha256(cur) !== sha256(fs.readFileSync(path.join(bdir, n))));
      return cur != null && sha256(cur) !== rec.files[n];
    });
    if (drift.length) return { ok: false, action, reason: `写した後に写す先で変わっている (${drift.join('・')})。中身を見てから`, drift };
    const r = restore(touched, { replacedSet: new Set(b.replaced), bdir });
    if (r.failed.length) return { ok: false, action, reason: `戻しきれなかった: ${failText(r, bdir)} (直してからもう一度 --rollback ${rollbackId})`, restore_failed: r.failed, kept: r.kept, backup: bdir };
    if (b.prev_deployed != null) writeAtomic(deployedPath, Buffer.from(b.prev_deployed, 'utf8'));
    else { try { fs.unlinkSync(deployedPath); } catch (e) { if (e.code !== 'ENOENT') throw e; } }
    log(`↩️ ${rollbackId} の前に戻した (戻した ${b.replaced.length}・消した ${b.added.length})`);
    return { ok: true, action, deployId: rollbackId, restored: b.replaced, removed: b.added };
  }

  function doApply() {
    const p = plan({ srcDir, target, files });   // 鍵を持ってから比べ直す
    const todo = p.filter((x) => x.status !== 'same');
    const deployId = makeDeployId(now);
    const prevDeployed = readOrNull(deployedPath);
    let prevRec = null;
    if (prevDeployed) { try { prevRec = JSON.parse(prevDeployed.toString('utf8')); } catch { /* */ } }
    // 前の回が途中で失敗して戻しきれていない = 先にその回を戻す (混ざった状態の上に写さない。Codex #1512 R3)
    if (prevRec && prevRec.state === 'failed_partial') return { ok: false, action, reason: `前の回 (${prevRec.deploy_id}) が途中で失敗したまま。先に --rollback ${prevRec.deploy_id}` };
    if (!todo.length && prevRec && prevRec.pc === pc && prevRec.commit === g.commit) return { ok: true, action, commit: g.commit, plan: p, unchanged: true };
    // 前のファイルを残す (替えるものだけ + 前の DEPLOYED.json)
    const bdir = path.join(target, BACKUP_DIR, deployId);
    fs.mkdirSync(bdir, { recursive: true });
    const replaced = todo.filter((x) => x.status === 'changed').map((x) => x.name);
    const added = todo.filter((x) => x.status === 'new').map((x) => x.name);
    for (const name of replaced) fs.writeFileSync(path.join(bdir, name), fs.readFileSync(path.join(target, name)), { flag: 'wx' });
    fs.writeFileSync(path.join(bdir, 'backup.json'), JSON.stringify({ deploy_id: deployId, replaced, added, prev_deployed: prevDeployed ? prevDeployed.toString('utf8') : null }, null, 1), { flag: 'wx' });
    const done = [];
    try {
      for (const x of todo) {
        const buf = fs.readFileSync(path.join(srcDir, x.name));
        done.push(x.name);   // 書こうとした時点で戻す対象 (途中で落ちた一時ファイルは writeAtomic が消す)
        writeAtomic(path.join(target, x.name), buf);
        const back = readBack(path.join(target, x.name));
        if (sha256(back) !== x.src_sha256) throw new Error(`写した後の中身が違う: ${x.name}`);
      }
      const rec = { deploy_id: deployId, commit: g.commit, pc, at: now.toISOString(), files: Object.fromEntries(p.map((x) => [x.name, x.src_sha256])), backup: path.join(BACKUP_DIR, deployId), replaced, added };
      writeAtomic(deployedPath, Buffer.from(JSON.stringify(rec, null, 1), 'utf8'));
    } catch (e) {
      // それまでに替えたものを戻す。戻せなかったものは名前・理由・前のファイルの場所を返す (Codex #1512 R1 Medium)
      const r = restore(done, { replacedSet: new Set(replaced), bdir });
      const why = String(e && e.message).slice(0, 200);
      if (r.failed.length) {
        // 戻しきれない = 新旧が混ざったまま。「途中で失敗した回」として記録を残す = 原因を直してから同じ実行 ID で --rollback できる (Codex #1512 R3)
        let recNote = '';
        try {
          const failedRec = { deploy_id: deployId, state: 'failed_partial', error: why, commit: g.commit, pc, at: now.toISOString(),
            files: Object.fromEntries(p.map((x) => [x.name, x.src_sha256])), backup: path.join(BACKUP_DIR, deployId), replaced, added };
          writeAtomic(deployedPath, Buffer.from(JSON.stringify(failedRec, null, 1), 'utf8'));
          recNote = `。原因を直してから --rollback ${deployId}`;
        } catch (we) { recNote = `。記録も書けなかった (${String(we && we.message).slice(0, 120)}) = 前のファイルから手で戻す`; }
        return { ok: false, action, deployId, reason: `写す途中で失敗 (${why})・戻しきれなかった: ${failText(r, bdir)}${recNote}`, restore_failed: r.failed, kept: r.kept, backup: bdir };
      }
      return { ok: false, action, deployId, reason: `写す途中で失敗して戻した (${why})`, restore_failed: [] };
    }
    log(`✅ ${deployId}: 替えた ${replaced.length}・足した ${added.length}・同じ ${p.length - todo.length} (commit ${g.commit.slice(0, 8)})`);
    return { ok: true, action, deployId, commit: g.commit, plan: p, replaced, added };
  }
}

export function parseArgs(argv) {
  const out = { pc: null, target: DEFAULT_TARGET, action: 'plan', rollbackId: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--pc') out.pc = argv[++i];
    else if (a === '--target') out.target = argv[++i];
    else if (a === '--apply') out.action = 'apply';
    else if (a === '--check') out.action = 'check';
    else if (a === '--rollback') { out.action = 'rollback'; out.rollbackId = argv[++i]; }
    else throw new Error(`知らない引数: ${a}`);
  }
  if (!out.pc) throw new Error('--pc minipc|streamdeck が要る');
  return out;
}

const fold = (x) => (process.platform === 'win32' ? x.toLowerCase() : x);
const isMain = (() => { try { return !!process.argv[1] && fold(fs.realpathSync.native(process.argv[1])) === fold(fs.realpathSync.native(fileURLToPath(import.meta.url))); } catch { return false; } })();
if (isMain) {
  try {
    const a = parseArgs(process.argv.slice(2));
    const r = deploy({ ...a, log: (m) => console.log(m) });
    if (r.plan && a.action !== 'apply') for (const x of r.plan) console.log(`  ${x.status.padEnd(7)} ${x.name}`);
    console.log(JSON.stringify({ ok: r.ok, action: r.action, reason: r.reason ?? null, deployId: r.deployId ?? null, commit: r.commit ?? r.deployed?.commit ?? null,
      drift: r.drift, outdated: r.outdated, replaced: r.replaced, added: r.added, unchanged: r.unchanged ?? false }));
    process.exitCode = r.ok ? 0 : 1;
  } catch (e) {
    console.error(`❌ ${String(e && e.message)}`);
    process.exitCode = 1;
  }
}
