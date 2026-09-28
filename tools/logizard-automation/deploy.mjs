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
 *   - 写す先でロジザードの自動化が動いている (logs/ のセッションの鍵がある) = 断る (--force-running で押し切れる)
 *   - 1 ファイルずつ 一時ファイル → rename。写した後に sha256 を読み直して確かめる。途中で失敗 = それまでに替えたものを戻す
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

function writeAtomic(file, buf) {
  const tmp = path.join(path.dirname(file), `.${path.basename(file)}.deploy-${process.pid}-${crypto.randomBytes(3).toString('hex')}.tmp`);
  fs.writeFileSync(tmp, buf, { flag: 'wx' });
  try { fs.renameSync(tmp, file); } catch (e) { try { fs.unlinkSync(tmp); } catch { /* */ } throw e; }
}

/**
 * 1 回分。戻り値 = { ok, action, reason?, plan?, deployId?, ... }
 * @param {object} p
 * @param {'minipc'|'streamdeck'} p.pc
 * @param {'plan'|'apply'|'check'|'rollback'} p.action
 */
export function deploy({ srcDir = SRC_DIR, target = DEFAULT_TARGET, pc, action = 'plan', rollbackId = null, forceRunning = false, git = realGit, now = new Date(), log = () => {}, readBack = (file) => fs.readFileSync(file) }) {
  const manifest = readManifest(srcDir);
  const files = manifest.pcs[pc];
  if (!Array.isArray(files) || !files.length) return { ok: false, action, reason: `manifest に PC「${pc}」が無い` };
  if (!fs.existsSync(target) || !fs.statSync(target).isDirectory()) return { ok: false, action, reason: `写す先が無い: ${target}` };
  const deployedPath = path.join(target, DEPLOYED_FILE);

  if (action === 'check') {
    const p = plan({ srcDir, target, files });
    let rec = null;
    try { rec = JSON.parse(fs.readFileSync(deployedPath, 'utf8')); } catch { /* 無い・読めない */ }
    const drift = rec ? files.filter((n) => rec.files?.[n] !== (p.find((x) => x.name === n).dst_sha256)) : files;
    const outdated = p.filter((x) => x.status !== 'same').map((x) => x.name);
    const ok = !!rec && rec.pc === pc && !drift.length && !outdated.length;
    return { ok, action, deployed: rec ? { deploy_id: rec.deploy_id, commit: rec.commit, at: rec.at } : null, drift, outdated,
      reason: ok ? null : !rec ? 'まだ写していない (DEPLOYED.json が無い)' : rec.pc !== pc ? `別の PC として写した記録 (${rec.pc})` : drift.length ? '写した後に写す先で変わった' : 'リポジトリのほうが新しい' };
  }

  const running = RUNNING_LOCKS.filter((n) => fs.existsSync(path.join(target, 'logs', n)));
  if (running.length && !forceRunning) return { ok: false, action, reason: `ロジザードの自動化が動いている (${running.join('・')})。終わってから` };

  if (action === 'rollback') {
    const dir = path.join(target, BACKUP_DIR, String(rollbackId || ''));
    const recPath = path.join(dir, 'backup.json');
    if (!rollbackId || !/^dep_[0-9TZ]+_[0-9a-f]{6}$/.test(rollbackId) || !fs.existsSync(recPath)) return { ok: false, action, reason: `戻す記録が無い: ${rollbackId}` };
    const b = JSON.parse(fs.readFileSync(recPath, 'utf8'));
    for (const name of b.replaced) writeAtomic(path.join(target, name), fs.readFileSync(path.join(dir, name)));
    for (const name of b.added) { try { fs.unlinkSync(path.join(target, name)); } catch (e) { if (e.code !== 'ENOENT') throw e; } }
    if (b.prev_deployed != null) writeAtomic(deployedPath, Buffer.from(b.prev_deployed, 'utf8'));
    else { try { fs.unlinkSync(deployedPath); } catch (e) { if (e.code !== 'ENOENT') throw e; } }
    log(`↩️ ${rollbackId} の前に戻した (戻した ${b.replaced.length}・消した ${b.added.length})`);
    return { ok: true, action, deployId: rollbackId, restored: b.replaced, removed: b.added };
  }

  const g = git(srcDir);
  if (g.dirty) return { ok: false, action, reason: 'tools/logizard-automation に未コミットの変更がある (コミット済みの中身だけを写す)' };
  const p = plan({ srcDir, target, files });
  if (action === 'plan') return { ok: true, action, commit: g.commit, plan: p };
  if (action !== 'apply') return { ok: false, action, reason: `知らない action: ${action}` };

  const todo = p.filter((x) => x.status !== 'same');
  const deployId = makeDeployId(now);
  const prevDeployed = readOrNull(deployedPath);
  if (!todo.length && prevDeployed) {
    let rec = null; try { rec = JSON.parse(prevDeployed.toString('utf8')); } catch { /* */ }
    if (rec && rec.pc === pc && rec.commit === g.commit) return { ok: true, action, commit: g.commit, plan: p, unchanged: true };
  }
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
      writeAtomic(path.join(target, x.name), buf);
      done.push(x.name);
      const back = readBack(path.join(target, x.name));   // 写した後に読み直す (試験では差し替える)
      if (sha256(back) !== x.src_sha256) throw new Error(`写した後の中身が違う: ${x.name}`);
    }
    const rec = { deploy_id: deployId, commit: g.commit, pc, at: now.toISOString(), files: Object.fromEntries(p.map((x) => [x.name, x.src_sha256])), backup: path.join(BACKUP_DIR, deployId), replaced, added };
    writeAtomic(deployedPath, Buffer.from(JSON.stringify(rec, null, 1), 'utf8'));
  } catch (e) {
    // それまでに替えたものを戻す
    for (const name of done) {
      try {
        if (replaced.includes(name)) writeAtomic(path.join(target, name), fs.readFileSync(path.join(bdir, name)));
        else fs.unlinkSync(path.join(target, name));
      } catch { /* 戻せなかったものは理由に出す */ }
    }
    return { ok: false, action, reason: `写す途中で失敗して戻した: ${String(e && e.message).slice(0, 200)}`, deployId };
  }
  log(`✅ ${deployId}: 替えた ${replaced.length}・足した ${added.length}・同じ ${p.length - todo.length} (commit ${g.commit.slice(0, 8)})`);
  return { ok: true, action, deployId, commit: g.commit, plan: p, replaced, added };
}

export function parseArgs(argv) {
  const out = { pc: null, target: DEFAULT_TARGET, action: 'plan', rollbackId: null, forceRunning: false };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--pc') out.pc = argv[++i];
    else if (a === '--target') out.target = argv[++i];
    else if (a === '--apply') out.action = 'apply';
    else if (a === '--check') out.action = 'check';
    else if (a === '--rollback') { out.action = 'rollback'; out.rollbackId = argv[++i]; }
    else if (a === '--force-running') out.forceRunning = true;
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
