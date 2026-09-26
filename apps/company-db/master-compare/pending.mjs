/**
 * pending.mjs — 照合 ② の「反映待ちの期限の台帳」(Company DB構想 10 §6.1.1 C2 v5-2・v6-3。Codex C2-R1 ② / C2-R2 M2)
 *
 * 何を持つか: 反映待ちの単位 (案件キー × 列 (構成は子) × 目標値のハッシュ) ごとに「目標値が最初に Render に届いた世代と時刻」。
 *   期限 = その後の最初の夜間ロード。再送・作り直しで始まりを動かさない (「朝 B → 夜の再送で A → 翌朝 B」でも B の始まりは最初の日のまま)
 * 置き場所: DATA_DIR/cdb-master-compare/pending/
 *   pending_<compare_run_id>.json = 版 (format・compare_run_id・prev = 前の版の compare_run_id と sha256・entries)
 *   HEAD.json = 最新の版 (compare_run_id・sha256)。版 → HEAD の順に、一時ファイル → rename で書く
 * 🚨 信用できないとき (HEAD の版が無い・ハッシュ違い・形が違う・HEAD が無いのに版がある) = untrusted:
 *   HEAD を進めない・版も書かない・期限を作り直さない (照合 ② は反映待ちの判定を blocked にする。直すのは人 = runbook)
 *   初めての導入 = HEAD が無く、版が 1 つも無いときだけ
 */
import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';

export const PENDING_FORMAT = 'pl-v1';
export const PENDING_DIRNAME = 'pending';
export const PENDING_KEEP_DAYS = 35;
const LOCK_STALE_MS = 2 * 3600 * 1000;
const VERSION_RE = /^pending_(mc_\d{8}T\d{9}Z_[0-9a-f]{6})\.json$/;
const sha256 = (buf) => crypto.createHash('sha256').update(buf).digest('hex');
export const pendingDir = (dataDir, resultDir) => path.join(dataDir, resultDir, PENDING_DIRNAME);

/** 1 件の形 (読み戻したときに確かめる) */
const isEntry = (e) => e && typeof e === 'object' && typeof e.unit === 'string' && typeof e.key === 'string' && typeof e.col === 'string'
  && typeof e.target_hash === 'string' && typeof e.start_generation === 'string' && typeof e.start_at === 'string' && Number.isFinite(Date.parse(e.start_at));

function readVersion(dir, compareRunId, wantSha) {
  const file = path.join(dir, `pending_${compareRunId}.json`);
  let buf;
  try { buf = fs.readFileSync(file); } catch { return { ok: false, reason: 'version_missing', compare_run_id: compareRunId }; }
  if (wantSha && sha256(buf) !== wantSha) return { ok: false, reason: 'version_hash_mismatch', compare_run_id: compareRunId };
  let v;
  try { v = JSON.parse(buf.toString('utf8')); } catch { return { ok: false, reason: 'version_not_json', compare_run_id: compareRunId }; }
  if (!v || v.format !== PENDING_FORMAT || v.compare_run_id !== compareRunId || !Array.isArray(v.entries) || !v.entries.every(isEntry)
    || !(v.prev === null || (v.prev && typeof v.prev.compare_run_id === 'string' && typeof v.prev.sha256 === 'string'))) {
    return { ok: false, reason: 'version_malformed', compare_run_id: compareRunId };
  }
  return { ok: true, version: v, sha256: sha256(buf) };
}

/**
 * 台帳を読む。戻り値 = { state: 'initial' | 'ok' | 'untrusted', reason, head, entries: Map(unit → entry) }
 * @param {string} dataDir
 * @param {string} resultDir  照合の結果の置き場所の名前 (run.mjs の RESULT_DIR)
 */
export function readLedger(dataDir, resultDir) {
  const dir = pendingDir(dataDir, resultDir);
  const entries = new Map();
  const versions = fs.existsSync(dir) ? fs.readdirSync(dir).filter((f) => VERSION_RE.test(f)) : [];
  const headFile = path.join(dir, 'HEAD.json');
  if (!fs.existsSync(headFile)) {
    if (versions.length) return { state: 'untrusted', reason: 'head_missing_with_versions', head: null, entries };
    return { state: 'initial', reason: null, head: null, entries };
  }
  let head;
  try { head = JSON.parse(fs.readFileSync(headFile, 'utf8')); } catch { return { state: 'untrusted', reason: 'head_not_json', head: null, entries }; }
  if (!head || typeof head.compare_run_id !== 'string' || typeof head.sha256 !== 'string') return { state: 'untrusted', reason: 'head_malformed', head: null, entries };
  const cur = readVersion(dir, head.compare_run_id, head.sha256);
  if (!cur.ok) return { state: 'untrusted', reason: cur.reason, head, entries };
  // 1 つ前の版も存在とハッシュを確かめる (鎖が切れていないか)
  if (cur.version.prev) {
    const prev = readVersion(dir, cur.version.prev.compare_run_id, cur.version.prev.sha256);
    if (!prev.ok) return { state: 'untrusted', reason: `prev_${prev.reason}`, head, entries };
  }
  for (const e of cur.version.entries) entries.set(e.unit, e);
  return { state: 'ok', reason: null, head, entries };
}

function writeAtomic(file, text) {
  const tmp = `${file}.${process.pid}.${Date.now()}.tmp`;
  fs.writeFileSync(tmp, text);
  fs.renameSync(tmp, file);
}

/**
 * 排他 (pending/.lock)。中身 (自分の印) を書いた一時ファイルを link で .lock にする = 中身の無い .lock が見える瞬間が無い (Codex #1464 R1 Medium 4)。
 * 古さは .lock の mtime で見る (2 時間より前なら捨てて 1 回だけ取り直す)。解放は .lock の印が自分のときだけ消す。取れなければ null
 */
export function acquireLock(dir, { now = Date.now() } = {}) {
  fs.mkdirSync(dir, { recursive: true });
  const lock = path.join(dir, '.lock');
  const token = `${process.pid}-${crypto.randomBytes(6).toString('hex')}`;
  const tmp = path.join(dir, `.lock.${token}.tmp`);
  fs.writeFileSync(tmp, JSON.stringify({ token, pid: process.pid, at: new Date(now).toISOString() }));
  try {
    for (let attempt = 0; attempt < 2; attempt++) {
      try {
        fs.linkSync(tmp, lock);
        return () => {
          try { if (JSON.parse(fs.readFileSync(lock, 'utf8')).token === token) fs.rmSync(lock, { force: true }); } catch { /* 自分の印か分からなければ消さない */ }
        };
      } catch (e) {
        if (e.code !== 'EEXIST') throw e;
        let mtime = null;
        try { mtime = fs.statSync(lock).mtimeMs; } catch { mtime = null; }
        if (attempt === 0 && mtime != null && now - mtime > LOCK_STALE_MS) { try { fs.rmSync(lock, { force: true }); } catch { /* */ } continue; }
        return null;
      }
    }
    return null;
  } finally { try { fs.rmSync(tmp, { force: true }); } catch { /* */ } }
}

/**
 * 新しい版を書き、HEAD を進める (台帳が initial / ok のときだけ呼ぶ)。
 * @returns {{ compare_run_id, sha256, file }}
 */
export function writeLedger(dataDir, resultDir, { compareRunId, ledger, entries, now = new Date() }) {
  if (ledger.state !== 'ok' && ledger.state !== 'initial') throw new Error(`台帳が信用できないときは書かない (${ledger.state})`);
  const dir = pendingDir(dataDir, resultDir);
  fs.mkdirSync(dir, { recursive: true });
  const version = { format: PENDING_FORMAT, compare_run_id: compareRunId, written_at: now.toISOString(),
    prev: ledger.head ? { compare_run_id: ledger.head.compare_run_id, sha256: ledger.head.sha256 } : null,
    entries: [...entries].sort((a, b) => a.unit.localeCompare(b.unit)) };
  const text = JSON.stringify(version);
  const file = path.join(dir, `pending_${compareRunId}.json`);
  writeAtomic(file, text);
  const head = { compare_run_id: compareRunId, sha256: sha256(Buffer.from(text, 'utf8')) };
  writeAtomic(path.join(dir, 'HEAD.json'), JSON.stringify(head));
  prunePending(dir, head, { now });
  return { ...head, file };
}

/** HEAD と、HEAD から 2 つ前までの版は消さない。それより古い版は 35 日で消す (全件 JSON の掃除とは別) */
export function prunePending(dir, head, { now = new Date(), keepDays = PENDING_KEEP_DAYS } = {}) {
  const keep = new Set([head.compare_run_id]);
  let cur = head.compare_run_id;
  for (let i = 0; i < 2 && cur; i++) {
    try { const v = JSON.parse(fs.readFileSync(path.join(dir, `pending_${cur}.json`), 'utf8')); cur = v.prev ? v.prev.compare_run_id : null; if (cur) keep.add(cur); } catch { cur = null; }
  }
  const cutoff = now.getTime() - keepDays * 86400000;
  for (const f of fs.readdirSync(dir)) {
    const m = f.match(VERSION_RE); if (!m || keep.has(m[1])) continue;
    const t = m[1].match(/^mc_(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})(\d{3})Z/);
    const ms = t ? Date.UTC(+t[1], +t[2] - 1, +t[3], +t[4], +t[5], +t[6], +t[7]) : NaN;
    if (Number.isFinite(ms) && ms < cutoff) { try { fs.rmSync(path.join(dir, f), { force: true }); } catch { /* */ } }
  }
}
