/**
 * linegift-orders.js — LINEギフト 受注 API 取得 (Phase 1 A-1、ミニPC側)
 *
 * 設計書: g:/共有ドライブ/AI_reference/システム設計/LINEギフトPhase1設計書_v0.5_20260515.md
 * Codex 5 ラウンドレビュー済 (R5 で critical/high なし、A-1 GO)
 *
 * 機能:
 *   - LINEギフト 受注 API (shop-mall.line.me/shop/api/1/order/search) で全件取得 (~3ヶ月限界)
 *   - OAuth refresh 完全 atomic 化 (one-time refresh_token を取りこぼさない: file lock + secure staging)
 *   - raw_linegift_orders に 1注文1商品単位で UPSERT (40列、PII含む)
 *   - 90日境界 frozen horizon 管理 (last_seen_at + is_frozen_after_horizon)
 *   - fail-closed: HTTP 401 / pagination欠落 / 必須キー欠落 / schema drift / fetched>0 inserted=0
 *
 * 既存 raw_linegift_orders (バグ持ち、~10,000行 item_code 空文字) は初回実行時 legacy 表に rename される。
 * 旧 mall-orders.js の fetchLineGift は廃止済み (daily-sync の cron 経路も本スクリプトに切替済み)。
 *
 * 使い方:
 *   node apps/warehouse/linegift-orders.js              直近の API 全件 (~2,400 注文 / ~3ヶ月) を取得
 *   node apps/warehouse/linegift-orders.js --dry-run    取得のみ、DB 保存しない (動作確認用)
 *
 * env:
 *   LINEGIFT_CLIENT_ID     OAuth client_id (LINEギフト管理画面)
 *   LINEGIFT_CLIENT_SECRET OAuth client_secret
 *   LINEGIFT_ACCESS_TOKEN  現在の access_token
 *   LINEGIFT_REFRESH_TOKEN 現在の refresh_token (one-time)
 */
import 'dotenv/config';
import fs from 'node:fs';
import path from 'node:path';
import { initDB, getDB, updateSyncMeta } from './db.js';

const API_HOST = 'https://shop-mall.line.me';
const TOKEN_REFRESH_URL = 'https://gift-shop-cms.line.biz/api/v1/oauth2/token/refresh';
const PAGE_SIZE = 100;
const FETCH_INTERVAL_MS = 600;
const FROZEN_HORIZON_DAYS = 14;

// 認証 + 状態管理ファイル (chmod 600 相当、Render に sync しない、.gitignore 必須)
const PROJECT_DIR = path.resolve(new URL('../..', import.meta.url).pathname.replace(/^\/([A-Z]:)/, '$1'));
const ENV_FILE = path.join(PROJECT_DIR, '.env');
const STAGING_FILE = path.join(PROJECT_DIR, 'data', 'linegift-token-staging.json');
const HISTORY_FILE = path.join(PROJECT_DIR, 'data', 'linegift-token-history.jsonl');
const LOCK_FILE = path.join(PROJECT_DIR, 'data', 'linegift-token.lock');

function nowJstIso() {
  const d = new Date(Date.now() + 9 * 3600 * 1000);
  return d.toISOString().replace('Z', '+09:00');
}
function unixToJstIso(u) {
  if (!u) return null;
  const d = new Date(u * 1000 + 9 * 3600 * 1000);
  return d.toISOString().replace('Z', '+09:00');
}
function unixToJstDate(u) {
  if (!u) return null;
  return new Date(u * 1000 + 9 * 3600 * 1000).toISOString().slice(0, 10);
}
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

// ─── atomic write helper (Codex Round 2/3 反映) ───
// temp file → fsync → rename → parent dir fsync (durable atomic replace)
function atomicWriteJson(filepath, obj, mode = 0o600) {
  const dir = path.dirname(filepath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const tmpPath = `${filepath}.tmp.${process.pid}`;
  const fd = fs.openSync(tmpPath, 'w', mode);
  try {
    fs.writeSync(fd, JSON.stringify(obj, null, 2));
    fs.fsyncSync(fd);
  } finally {
    fs.closeSync(fd);
  }
  fs.renameSync(tmpPath, filepath);
  // parent dir fsync (新名前を durable 化)
  try {
    const dirFd = fs.openSync(dir, 'r');
    try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); }
  } catch { /* Windows では dir fsync 不要/不可、無視 */ }
}

function atomicWriteEnv(updates) {
  let env = fs.readFileSync(ENV_FILE, 'utf-8');
  for (const [k, v] of Object.entries(updates)) {
    const re = new RegExp(`^${k}=.*$`, 'm');
    if (re.test(env)) env = env.replace(re, `${k}=${v}`);
    else env += (env.endsWith('\n') ? '' : '\n') + `${k}=${v}\n`;
  }
  const tmpPath = `${ENV_FILE}.tmp.${process.pid}`;
  const fd = fs.openSync(tmpPath, 'w', 0o600);
  try { fs.writeSync(fd, env); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
  fs.renameSync(tmpPath, ENV_FILE);
  try {
    const dirFd = fs.openSync(path.dirname(ENV_FILE), 'r');
    try { fs.fsyncSync(dirFd); } finally { fs.closeSync(dirFd); }
  } catch { /* ignore */ }
}

function appendAudit(entry) {
  if (!fs.existsSync(path.dirname(HISTORY_FILE))) fs.mkdirSync(path.dirname(HISTORY_FILE), { recursive: true });
  const line = JSON.stringify({ timestamp: nowJstIso(), ...entry }) + '\n';
  fs.appendFileSync(HISTORY_FILE, line);
}

// ─── lock (O_EXCL + owner token 検証 + TTL 60min 経過で stale 回収、Codex R1 #6 / R2 #1) ───
const LOCK_STALE_TTL_MS = 60 * 60 * 1000;  // 60分。daily-sync の最大 timeout より長く

let myLockToken = null;  // 自分が取得した lock の owner token

function acquireLock() {
  if (!fs.existsSync(path.dirname(LOCK_FILE))) fs.mkdirSync(path.dirname(LOCK_FILE), { recursive: true });
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      // Codex R3 #1: durable lock — temp file に fsync 済の完全内容を書いてから link/rename で atomic に lock 確立
      // 直接 openSync('wx') + writeSync だと書込中クラッシュで空 lock が残り、parse 失敗で恒久停止する
      const tmpPath = `${LOCK_FILE}.tmp.${process.pid}.${Date.now()}.${Math.random().toString(36).slice(2, 8)}`;
      const token = `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
      const payload = JSON.stringify({ pid: process.pid, started_at: nowJstIso(), started_at_ms: Date.now(), owner_token: token });
      const tfd = fs.openSync(tmpPath, 'w', 0o600);
      try {
        fs.writeSync(tfd, payload);
        fs.fsyncSync(tfd);
      } finally {
        fs.closeSync(tfd);
      }
      try {
        // link は dst が存在すれば EEXIST を返す → atomic な O_EXCL 相当
        fs.linkSync(tmpPath, LOCK_FILE);
        try { fs.unlinkSync(tmpPath); } catch { /* ignore */ }
      } catch (linkErr) {
        // link 失敗 (大抵 EEXIST = 既存 lock あり) → tmp を片付けて既存判定へ
        try { fs.unlinkSync(tmpPath); } catch { /* ignore */ }
        if (linkErr.code !== 'EEXIST') {
          // Windows 等で link が EPERM/EOPNOTSUPP の場合は rename で代替 (atomic でなくなるが概ね同じ動作)
          // ただし既存 lock を上書きしないため、再度 wx open でフォールバック
          try {
            const fd = fs.openSync(LOCK_FILE, 'wx');
            try { fs.writeSync(fd, payload); fs.fsyncSync(fd); } finally { fs.closeSync(fd); }
            myLockToken = token;
            return true;
          } catch (fbErr) {
            if (fbErr.code === 'EEXIST') {
              throw Object.assign(new Error('EEXIST'), { code: 'EEXIST' });
            }
            throw fbErr;
          }
        }
        throw Object.assign(new Error('EEXIST'), { code: 'EEXIST' });
      }
      myLockToken = token;
      return true;
    } catch (e) {
      if (e.code !== 'EEXIST') throw e;
      // 既存 lock を読んで stale 判定
      let existing = null;
      let parseFailed = false;
      try { existing = JSON.parse(fs.readFileSync(LOCK_FILE, 'utf-8')); }
      catch { parseFailed = true; }

      // Codex R3 #1: parse 失敗時も mtime fallback で stale 判定 (lock poisoning 回避)
      if (parseFailed) {
        let mtimeMs;
        try { mtimeMs = fs.statSync(LOCK_FILE).mtimeMs; }
        catch { console.error('[linegift] lock 取得失敗 (既存 lock の stat も読めず、手動確認要)'); return false; }
        const ageMs = Date.now() - mtimeMs;
        if (ageMs > LOCK_STALE_TTL_MS && attempt === 0) {
          console.warn(`[linegift] 破損 lock 検出 (parse 失敗、mtime age=${(ageMs/60000).toFixed(1)}min) → 強制削除して再取得`);
          try { fs.unlinkSync(LOCK_FILE); } catch (e2) { console.error(`[linegift] 破損 lock 削除失敗: ${e2.message}`); return false; }
          continue;
        }
        console.error(`[linegift] lock 取得失敗 (parse 失敗、mtime age=${(ageMs/60000).toFixed(1)}min < TTL=${LOCK_STALE_TTL_MS/60000}min、しばらく待って再試行)`);
        return false;
      }

      const ageMs = existing.started_at_ms ? Date.now() - existing.started_at_ms : null;
      if (ageMs == null) {
        console.error(`[linegift] lock 取得失敗: 旧フォーマット lock (started_at_ms 無し) — 手動削除要 pid=${existing.pid}`);
        return false;
      }
      if (ageMs > LOCK_STALE_TTL_MS && attempt === 0) {
        console.warn(`[linegift] stale lock 検出 (age=${(ageMs/60000).toFixed(1)}min, pid=${existing.pid}, token=${(existing.owner_token || '?').slice(0, 12)}...) → 強制解放して再取得`);
        // stale 削除も owner_token 比較で安全化: 読んだ瞬間の token と削除直前の token が一致する場合のみ削除
        try {
          const recheck = JSON.parse(fs.readFileSync(LOCK_FILE, 'utf-8'));
          if (recheck.owner_token === existing.owner_token && recheck.started_at_ms === existing.started_at_ms) {
            fs.unlinkSync(LOCK_FILE);
          } else {
            console.warn('[linegift] stale 判定中に lock が入れ替わった → 削除中止');
            return false;
          }
        } catch (e2) {
          console.error(`[linegift] stale lock 削除失敗: ${e2.message}`);
          return false;
        }
        continue;  // 再取得試行
      }
      console.error(`[linegift] lock 取得失敗: 既存 lock pid=${existing.pid} started_at=${existing.started_at} age=${(ageMs/60000).toFixed(1)}min (TTL=${LOCK_STALE_TTL_MS/60000}min)`);
      return false;
    }
  }
  return false;
}
function releaseLockOnce() {
  // 1 回試行。戻り値: 'released' (削除成功 or already gone or 不一致), 'transient' (リトライ価値あり), 'unsafe' (parse 失敗等で TTL に委譲)
  if (!myLockToken) return 'released';
  let raw;
  try { raw = fs.readFileSync(LOCK_FILE, 'utf-8'); }
  catch (e) {
    if (e.code === 'ENOENT') { myLockToken = null; return 'released'; }
    return 'transient';
  }
  let cur;
  try { cur = JSON.parse(raw); }
  catch { return 'unsafe'; }
  if (cur.owner_token !== myLockToken) {
    console.warn(`[linegift] releaseLock: owner_token 不一致 (mine=${myLockToken.slice(0, 12)}... vs file=${(cur.owner_token || '?').slice(0, 12)}...) — 自分の lock は既に失われている、token clear`);
    myLockToken = null;
    return 'released';
  }
  try {
    fs.unlinkSync(LOCK_FILE);
    myLockToken = null;
    return 'released';
  } catch (e) {
    if (e.code === 'ENOENT') { myLockToken = null; return 'released'; }
    return 'transient';
  }
}

function releaseLock() {
  // Codex R2 #1 / R3 #2 / R4 #2: 自分の owner_token と一致する場合のみ削除 +
  //   一過性失敗には bounded retry。caller (catch / signal handler) が直後に exit するため
  //   「次回 release で再試行」は成立しない → ここで完結させる。
  if (!myLockToken) return;
  const MAX_ATTEMPTS = 3;
  for (let i = 0; i < MAX_ATTEMPTS; i++) {
    const r = releaseLockOnce();
    if (r === 'released') return;
    if (r === 'unsafe') {
      console.warn('[linegift] releaseLock: lock parse 失敗 — TTL stale 回収に委譲');
      return;
    }
    // transient — 短い同期 sleep で retry (Atomics.wait で sync sleep)
    if (i < MAX_ATTEMPTS - 1) {
      const sab = new SharedArrayBuffer(4);
      const ia = new Int32Array(sab);
      Atomics.wait(ia, 0, 0, 100);  // 100ms 同期 sleep
    }
  }
  console.warn(`[linegift] releaseLock: ${MAX_ATTEMPTS}回 retry 後も release 失敗 — TTL ${LOCK_STALE_TTL_MS/60000}min 経過後に stale 回収される`);
}

// プロセス異常終了時の cleanup (Codex R1 #6 / R2 #1)
// myLockToken の有無で「自分が lock を持ってるか」を判定 (lockHeld 二重管理は廃止)
function installSignalHandlers() {
  let cleaningUp = false;  // 多重発火防止 (handler が複数 signal で同時発火しても 1 回だけ release)
  const handler = (sig) => {
    if (cleaningUp) return;
    cleaningUp = true;
    if (myLockToken) {
      console.error(`[linegift] ${sig} 受信 → lock を解放して exit`);
      releaseLock();
    }
    process.exit(sig === 'uncaughtException' || sig === 'unhandledRejection' ? 1 : 130);
  };
  process.on('SIGTERM', () => handler('SIGTERM'));
  process.on('SIGINT', () => handler('SIGINT'));
  process.on('SIGHUP', () => handler('SIGHUP'));
  process.on('uncaughtException', (err) => {
    console.error(`[linegift] uncaughtException: ${err.message}\n${err.stack}`);
    handler('uncaughtException');
  });
  process.on('unhandledRejection', (reason) => {
    console.error(`[linegift] unhandledRejection: ${reason}`);
    handler('unhandledRejection');
  });
}

// ─── OAuth refresh atomic (Codex Round 1〜5、設計書 §9) ───

/**
 * 起動時 staging file 復旧チェック (Codex Round 4-5 反映)
 * - kind='post_refresh_unsaved' → 手動復旧モードを案内、exit 1
 * - kind='pre_refresh_backup' → 直前 refresh 中断、staging の旧 token を表示、exit 1
 *   ※ 注意: refresh API がプロバイダ側で成功していたが (1)→(3) durable write 完了前に異常終了した場合、
 *      staging の旧 refresh_token は既にプロバイダ側でローテート済 = 無効の可能性あり
 * - kind='post_refresh_saved' → 単に purge (前回の audit 完了忘れ)
 */
function recoverStagingIfPresent() {
  if (!fs.existsSync(STAGING_FILE)) return;
  let staging;
  try { staging = JSON.parse(fs.readFileSync(STAGING_FILE, 'utf-8')); } catch (e) {
    console.error(`[linegift] staging file が壊れてる (${e.message}) → 手動確認要`);
    process.exit(1);
  }
  if (staging.kind === 'post_refresh_saved') {
    console.log('[linegift] 前回 audit 完了忘れの staging 検出 → purge');
    fs.unlinkSync(STAGING_FILE);
    return;
  }
  if (staging.kind === 'post_refresh_unsaved') {
    // Codex R3 #3 / R4 #1: .env 書込後 〜 post_refresh_saved 更新前の SIGTERM での誤停止を回避。
    // 比較は .env ファイルを直読みで行う (process.env は dotenv が override しないため、
    // winsw/systemd 等の親環境変数が混入してると process.env が .env の実体と乖離して誤 purge する)。
    const stagingAcc = staging.new_access_token;
    const stagingRef = staging.new_refresh_token;
    let envFileAcc = null;
    let envFileRef = null;
    try {
      const envRaw = fs.readFileSync(ENV_FILE, 'utf-8');
      // 簡易 parse: KEY=VALUE 各行、CRLF/末尾空白除去
      for (const line of envRaw.split(/\r?\n/)) {
        const m = line.match(/^([A-Z0-9_]+)=(.*)$/);
        if (!m) continue;
        const v = m[2].trim().replace(/^["']|["']$/g, '');
        if (m[1] === 'LINEGIFT_ACCESS_TOKEN') envFileAcc = v;
        else if (m[1] === 'LINEGIFT_REFRESH_TOKEN') envFileRef = v;
      }
    } catch (e) {
      console.error(`[linegift] post_refresh_unsaved 検出だが .env 読込失敗: ${e.message} → 安全側で手動介入要求`);
      process.exit(1);
    }
    const accMatch = envFileAcc && stagingAcc && envFileAcc === stagingAcc;
    const refMatch = envFileRef && stagingRef && envFileRef === stagingRef;
    if (accMatch && refMatch) {
      console.warn('[linegift] post_refresh_unsaved 検出だが .env ファイルが staging と一致 → 自動 purge して継続 (refresh は既に commit 済)');
      try { fs.unlinkSync(STAGING_FILE); } catch (e) { console.warn(`[linegift] staging purge 失敗 (実害なし): ${e.message}`); }
      return;
    }
    console.error('[linegift] ★復旧モード★: refresh 成功後 .env 書き戻し前にクラッシュした痕跡あり');
    console.error(`  staging.new_access_token (先頭8文字): ${(stagingAcc || '').slice(0, 8)}...`);
    console.error(`  staging.new_refresh_token (先頭8文字): ${(stagingRef || '').slice(0, 8)}...`);
    console.error(`  .env(file) LINEGIFT_ACCESS_TOKEN  一致: ${accMatch}`);
    console.error(`  .env(file) LINEGIFT_REFRESH_TOKEN 一致: ${refMatch}`);
    console.error('  対応: 上記 new_access_token / new_refresh_token を .env に手動転記してから再実行');
    console.error(`  staging file: ${STAGING_FILE}`);
    process.exit(1);
  }
  if (staging.kind === 'pre_refresh_backup') {
    console.error('[linegift] ★復旧モード★: refresh 中断 (pre_refresh_backup 残存)');
    console.error('  ⚠️ 注意: refresh API がプロバイダ側で成功していたが durable write 完了前のクラッシュなら、');
    console.error('     staging の旧 refresh_token は既に無効化されている可能性あり (LINEギフト 401 連発)');
    console.error(`  staging.access_token (先頭8文字): ${(staging.access_token || '').slice(0, 8)}...`);
    console.error(`  staging.refresh_token (先頭8文字): ${(staging.refresh_token || '').slice(0, 8)}...`);
    console.error('  対応: 一旦 .env に転記して再実行 → 401 連発なら手動 auth code 取得');
    console.error(`  staging file: ${STAGING_FILE}`);
    process.exit(1);
  }
  console.error(`[linegift] 未知の staging kind=${staging.kind} → 手動確認要`);
  process.exit(1);
}

/**
 * Token refresh — atomic 設計 (設計書 §9 (1)〜(8))
 * 戻り値: 新 access_token
 */
async function refreshAccessToken() {
  const clientId = process.env.LINEGIFT_CLIENT_ID;
  const clientSecret = process.env.LINEGIFT_CLIENT_SECRET;
  const refreshToken = process.env.LINEGIFT_REFRESH_TOKEN;
  if (!clientId || !clientSecret || !refreshToken) {
    throw new Error('LINEGIFT_CLIENT_ID / CLIENT_SECRET / REFRESH_TOKEN 未設定');
  }

  // (1) 旧 token を staging に書き出し (pre_refresh_backup)
  atomicWriteJson(STAGING_FILE, {
    saved_at: nowJstIso(),
    kind: 'pre_refresh_backup',
    access_token: process.env.LINEGIFT_ACCESS_TOKEN,
    refresh_token: refreshToken,
  });

  // (2) refresh API 呼び出し
  const res = await fetch(TOKEN_REFRESH_URL, {
    method: 'POST',
    headers: { 'Accept': 'application/json', 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      grant_type: 'refresh_token',
      client_id: clientId,
      client_secret: clientSecret,
      refresh_token: refreshToken,
    }).toString(),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`refresh API failed: HTTP ${res.status} ${text.slice(0, 300)}`);
  }
  const td = await res.json();
  if (!td.access_token) {
    throw new Error(`refresh API returned no access_token: ${JSON.stringify(td)}`);
  }

  // (3) 新 token を staging に上書き (post_refresh_unsaved) — ★最重要 durable write 必須
  atomicWriteJson(STAGING_FILE, {
    saved_at: nowJstIso(),
    kind: 'post_refresh_unsaved',
    new_access_token: td.access_token,
    new_refresh_token: td.refresh_token || refreshToken,
    prev_refresh_token: refreshToken,
  });

  // (4)(5) .env を atomic 書き戻し
  const updates = { LINEGIFT_ACCESS_TOKEN: td.access_token };
  if (td.refresh_token) updates.LINEGIFT_REFRESH_TOKEN = td.refresh_token;
  atomicWriteEnv(updates);
  process.env.LINEGIFT_ACCESS_TOKEN = td.access_token;
  if (td.refresh_token) process.env.LINEGIFT_REFRESH_TOKEN = td.refresh_token;

  // (6) staging を 'post_refresh_saved' にマーク
  atomicWriteJson(STAGING_FILE, {
    saved_at: nowJstIso(),
    kind: 'post_refresh_saved',
    new_token_prefix: td.access_token.slice(0, 8),
  });

  // (7) audit log
  appendAudit({
    event: 'token_refresh_success',
    prev_token_prefix: (refreshToken || '').slice(0, 8),
    new_access_token_prefix: td.access_token.slice(0, 8),
    new_refresh_token_prefix: (td.refresh_token || '').slice(0, 8),
  });

  // (8) staging purge
  try { fs.unlinkSync(STAGING_FILE); } catch (e) {
    console.warn(`[linegift] staging purge 失敗 (実害なし、次回起動時に自動 purge): ${e.message}`);
  }

  console.log('[linegift] OAuth refresh OK (.env 書き戻し済、audit 記録済)');
  return td.access_token;
}

// ─── 受注 API 取得 ───

async function fetchOrdersAllPages(accessToken) {
  const orders = [];
  let page = 1;
  let firstPaging = null;
  let firstTotalPages = null;
  while (true) {
    const url = `${API_HOST}/shop/api/1/order/search?access_token=${encodeURIComponent(accessToken)}&page=${page}&per_page=${PAGE_SIZE}`;
    const res = await fetch(url);
    if (res.status === 401) {
      throw new Error('HTTP 401 — token expired or invalid');
    }
    if (!res.ok) {
      throw new Error(`HTTP ${res.status}: ${(await res.text()).slice(0, 300)}`);
    }
    const data = await res.json();

    // Codex R1 #5 / R2 #2: paging を Number() 正規化して意味的異常のみ fatal に。
    // typeof === 'number' 厳格判定だと "24" のような numeric string 化で常時停止してしまう (R2 高)
    const totalPages = Number(data.paging?.total_pages);
    const totalEntries = Number(data.paging?.total_entries);
    if (page === 1) {
      if (!data.paging) {
        throw new Error('schema drift: 1ページ目に paging が含まれない');
      }
      if (!Number.isInteger(totalPages) || totalPages < 0) {
        throw new Error(`schema drift: 1ページ目の paging.total_pages が integer でない/負数 (got ${JSON.stringify(data.paging.total_pages)})`);
      }
      if (!Number.isInteger(totalEntries) || totalEntries < 0) {
        throw new Error(`schema drift: 1ページ目の paging.total_entries が integer でない/負数 (got ${JSON.stringify(data.paging.total_entries)})`);
      }
      firstPaging = { ...data.paging, total_pages: totalPages, total_entries: totalEntries };
      firstTotalPages = totalPages;
      // total_entries=0 は正常 (受注ゼロ件) なので OK、early exit
      if (totalEntries === 0) break;
    } else {
      // 2ページ目以降も schema drift 検知 (途中で paging 消えたら異常)
      if (!data.paging || !Number.isInteger(totalPages) || totalPages < 0) {
        throw new Error(`schema drift: page=${page} で paging.total_pages 不正 (got ${JSON.stringify(data.paging?.total_pages)})`);
      }
      if (totalPages !== firstTotalPages) {
        throw new Error(`schema drift: total_pages がページ間で変動 (1ページ目=${firstTotalPages}, ${page}ページ目=${totalPages})`);
      }
    }

    const pageOrders = data.orders || [];
    if (pageOrders.length === 0) break;
    orders.push(...pageOrders);
    process.stdout.write(`\r[linegift] fetch page ${page}/${totalPages}, total ${orders.length}/${totalEntries}`);
    if (page >= totalPages) break;
    page++;
    await sleep(FETCH_INTERVAL_MS);
  }
  process.stdout.write('\n');
  return { orders, paging: firstPaging };
}

// ─── DDL (legacy rename + 新表 create) ───

function ensureTables(db) {
  // 旧表 (item_code 等空文字バグ持ち、9,640行) を一度だけ legacy 化
  const cols = db.prepare("PRAGMA table_info(raw_linegift_orders)").all().map(c => c.name);
  if (cols.length > 0 && !cols.includes('sku_code')) {
    // 旧スキーマ検出 (sku_code 列なし) → タイムスタンプ付き legacy 退避
    // (Codex R1 #4: 固定名は再実行で衝突 → 旧 raw を DROP しちゃう事故あり、必ず unique 退避先で fail-closed)
    // (Codex R2 #3: ms + pid 付加で衝突確率を実質ゼロに)
    const tsTag = new Date().toISOString().replace(/[-:T.Z]/g, '').slice(0, 17); // YYYYMMDDhhmmss + ms 先頭3桁 (UTC)
    const legacyName = `raw_linegift_orders_legacy_buggy_${tsTag}_${process.pid}`;
    const exists = db.prepare(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`).get(legacyName);
    if (exists) {
      throw new Error(`[linegift] FATAL: legacy 退避先 ${legacyName} が既存 (1秒以内の再実行?) — 手動確認要。raw_linegift_orders は触らず停止`);
    }
    console.log(`[linegift] 旧スキーマ raw_linegift_orders (item_code 空文字バグ持ち) を検出 → ${legacyName} に rename`);
    db.exec(`ALTER TABLE raw_linegift_orders RENAME TO ${legacyName}`);
  }
  db.exec(`CREATE TABLE IF NOT EXISTS raw_linegift_orders (
    -- PK
    order_id          TEXT NOT NULL PRIMARY KEY,    -- 1注文1商品 (V1 verify) で order_id 単一 PK

    -- 注文レベル: 基本
    status            TEXT,
    user_name         TEXT,                         -- LINE ID (PII)
    -- 売上 / 手数料 (税込・送料込み込み、中原さん 2026-05-15 確定)
    selling_price     REAL,                         -- 売価 (LINEギフトの販売価格、税込)
    fee               REAL,                         -- モール手数料 (API 実額、税込、~12.97%)

    -- 商品 (item.code/name) - 親軸分析用
    item_id           INTEGER,
    parent_item_code  TEXT,                         -- = item.code (親商品)
    parent_item_name  TEXT,
    -- variant (variation.code/name) - SKU 主キー
    variation_id      INTEGER,
    sku_code          TEXT,                         -- = variation.code (★ SKU 主キー、m_products と 100% マッチ)
    sku_name          TEXT,                         -- = variation.name
    stock_count       INTEGER,                      -- 数量 (V1: 全件 1)

    -- 送料 (将来 API 拡張用、現状 NULL/0、中原さん方針 2026-05-15)
    shipping_fee      REAL,                         -- 将来 LINEギフト API が送料 field 提供したら自動的にここに入る

    -- タイムスタンプ (unix UTC + JST 派生)
    bought_on_unix    INTEGER,                      -- 注文時刻 (UTC)
    bought_at_jst     TEXT,                         -- ISO8601 JST
    bought_date_jst   TEXT,                         -- 'YYYY-MM-DD' JST
    payed_on_unix     INTEGER,
    payed_at_jst      TEXT,
    payed_date_jst    TEXT,
    delivered_on_unix INTEGER,
    delivered_at_jst  TEXT,
    delivered_date_jst TEXT,
    received_on_unix  INTEGER,
    received_at_jst   TEXT,
    received_date_jst TEXT,                         -- ★ recognized_on (whitelist=received で集計の基準日)
    payment_deadline_unix         INTEGER,
    delivery_deadline_unix        INTEGER,
    delivery_by_hand_deadline_unix INTEGER,
    receive_deadline_unix         INTEGER,

    -- 配送
    is_delivery_by_hand     INTEGER,
    delivery_code           TEXT,                   -- 追跡番号
    delivery_agent          TEXT,                   -- 配送業者
    shipment_date           TEXT,
    pre_order_delivery_start INTEGER,

    -- 配送先 PII (受取人 = ギフト宛先)
    address_from_name TEXT,
    address_first_name TEXT,
    address_last_name TEXT,
    address_zip       TEXT,
    address_tel       TEXT,
    address_tel_hyphen TEXT,
    address_pref      TEXT,
    address_city      TEXT,
    address_address   TEXT,

    -- 90日境界の凍結管理 (Codex Round 1#1 + Round 2-5、設計書 §12)
    -- ※ DEFAULT は使わず INSERT 時に必ず JST ISO を node 側で指定 (Codex Round 2 #5)
    first_seen_at         TEXT NOT NULL,  -- raw に最初に出現した時刻 (JST ISO)
    last_seen_at          TEXT NOT NULL,  -- ★最後に API レスポンスに含まれた時刻 (JST ISO)、freeze 判定の単一基準
    last_api_snapshot_at  TEXT NOT NULL,  -- 直前の API run の取得時刻 (JST ISO、毎 run 全行に同値)
    is_frozen_after_horizon INTEGER NOT NULL DEFAULT 0,  -- 1 = finalization_horizon (14日) 超過、UPDATE/DELETE 禁止

    -- メタ
    synced_at         TEXT NOT NULL
  )`);
  db.exec('CREATE INDEX IF NOT EXISTS idx_linegift_received_date ON raw_linegift_orders(received_date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_linegift_bought_date ON raw_linegift_orders(bought_date_jst)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_linegift_status ON raw_linegift_orders(status)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_linegift_sku_code ON raw_linegift_orders(sku_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_linegift_parent_item_code ON raw_linegift_orders(parent_item_code)');
  db.exec('CREATE INDEX IF NOT EXISTS idx_linegift_frozen ON raw_linegift_orders(is_frozen_after_horizon)');
}

// ─── DB 投入 (UPSERT、first_seen_at は新規時のみ、last_seen_at は毎回更新) ───

const INSERT_SQL = `
INSERT INTO raw_linegift_orders (
  order_id, status, user_name, selling_price, fee,
  item_id, parent_item_code, parent_item_name,
  variation_id, sku_code, sku_name, stock_count,
  shipping_fee,
  bought_on_unix, bought_at_jst, bought_date_jst,
  payed_on_unix, payed_at_jst, payed_date_jst,
  delivered_on_unix, delivered_at_jst, delivered_date_jst,
  received_on_unix, received_at_jst, received_date_jst,
  payment_deadline_unix, delivery_deadline_unix, delivery_by_hand_deadline_unix, receive_deadline_unix,
  is_delivery_by_hand, delivery_code, delivery_agent, shipment_date, pre_order_delivery_start,
  address_from_name, address_first_name, address_last_name, address_zip, address_tel, address_tel_hyphen,
  address_pref, address_city, address_address,
  first_seen_at, last_seen_at, last_api_snapshot_at, is_frozen_after_horizon,
  synced_at
) VALUES (
  @order_id, @status, @user_name, @selling_price, @fee,
  @item_id, @parent_item_code, @parent_item_name,
  @variation_id, @sku_code, @sku_name, @stock_count,
  @shipping_fee,
  @bought_on_unix, @bought_at_jst, @bought_date_jst,
  @payed_on_unix, @payed_at_jst, @payed_date_jst,
  @delivered_on_unix, @delivered_at_jst, @delivered_date_jst,
  @received_on_unix, @received_at_jst, @received_date_jst,
  @payment_deadline_unix, @delivery_deadline_unix, @delivery_by_hand_deadline_unix, @receive_deadline_unix,
  @is_delivery_by_hand, @delivery_code, @delivery_agent, @shipment_date, @pre_order_delivery_start,
  @address_from_name, @address_first_name, @address_last_name, @address_zip, @address_tel, @address_tel_hyphen,
  @address_pref, @address_city, @address_address,
  @first_seen_at, @last_seen_at, @last_api_snapshot_at, @is_frozen_after_horizon,
  @synced_at
)
ON CONFLICT(order_id) DO UPDATE SET
  -- frozen 行は UPDATE 禁止 (§12)
  status                    = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.status ELSE excluded.status END,
  user_name                 = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.user_name ELSE excluded.user_name END,
  selling_price             = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.selling_price ELSE excluded.selling_price END,
  fee                       = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.fee ELSE excluded.fee END,
  item_id                   = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.item_id ELSE excluded.item_id END,
  parent_item_code          = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.parent_item_code ELSE excluded.parent_item_code END,
  parent_item_name          = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.parent_item_name ELSE excluded.parent_item_name END,
  variation_id              = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.variation_id ELSE excluded.variation_id END,
  sku_code                  = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.sku_code ELSE excluded.sku_code END,
  sku_name                  = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.sku_name ELSE excluded.sku_name END,
  stock_count               = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.stock_count ELSE excluded.stock_count END,
  shipping_fee              = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.shipping_fee ELSE excluded.shipping_fee END,
  bought_on_unix            = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.bought_on_unix ELSE excluded.bought_on_unix END,
  bought_at_jst             = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.bought_at_jst ELSE excluded.bought_at_jst END,
  bought_date_jst           = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.bought_date_jst ELSE excluded.bought_date_jst END,
  payed_on_unix             = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.payed_on_unix ELSE excluded.payed_on_unix END,
  payed_at_jst              = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.payed_at_jst ELSE excluded.payed_at_jst END,
  payed_date_jst            = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.payed_date_jst ELSE excluded.payed_date_jst END,
  delivered_on_unix         = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.delivered_on_unix ELSE excluded.delivered_on_unix END,
  delivered_at_jst          = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.delivered_at_jst ELSE excluded.delivered_at_jst END,
  delivered_date_jst        = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.delivered_date_jst ELSE excluded.delivered_date_jst END,
  received_on_unix          = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.received_on_unix ELSE excluded.received_on_unix END,
  received_at_jst           = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.received_at_jst ELSE excluded.received_at_jst END,
  received_date_jst         = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.received_date_jst ELSE excluded.received_date_jst END,
  payment_deadline_unix         = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.payment_deadline_unix         ELSE excluded.payment_deadline_unix         END,
  delivery_deadline_unix        = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.delivery_deadline_unix        ELSE excluded.delivery_deadline_unix        END,
  delivery_by_hand_deadline_unix = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.delivery_by_hand_deadline_unix ELSE excluded.delivery_by_hand_deadline_unix END,
  receive_deadline_unix         = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.receive_deadline_unix         ELSE excluded.receive_deadline_unix         END,
  is_delivery_by_hand           = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.is_delivery_by_hand           ELSE excluded.is_delivery_by_hand           END,
  delivery_code             = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.delivery_code             ELSE excluded.delivery_code             END,
  delivery_agent            = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.delivery_agent            ELSE excluded.delivery_agent            END,
  shipment_date             = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.shipment_date             ELSE excluded.shipment_date             END,
  pre_order_delivery_start  = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.pre_order_delivery_start  ELSE excluded.pre_order_delivery_start  END,
  address_from_name         = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.address_from_name         ELSE excluded.address_from_name         END,
  address_first_name        = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.address_first_name        ELSE excluded.address_first_name        END,
  address_last_name         = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.address_last_name         ELSE excluded.address_last_name         END,
  address_zip               = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.address_zip               ELSE excluded.address_zip               END,
  address_tel               = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.address_tel               ELSE excluded.address_tel               END,
  address_tel_hyphen        = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.address_tel_hyphen        ELSE excluded.address_tel_hyphen        END,
  address_pref              = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.address_pref              ELSE excluded.address_pref              END,
  address_city              = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.address_city              ELSE excluded.address_city              END,
  address_address           = CASE WHEN raw_linegift_orders.is_frozen_after_horizon = 1 THEN raw_linegift_orders.address_address           ELSE excluded.address_address           END,
  -- first_seen_at は新規時のみ設定 (UPSERT で温存)
  -- last_seen_at は frozen でも更新 (= API で再観測されたという事実を記録)、ただし frozen 行が API に出ること自体が異常 (DQ check 11)
  last_seen_at              = excluded.last_seen_at,
  last_api_snapshot_at      = excluded.last_api_snapshot_at,
  -- is_frozen_after_horizon は raw 一括 UPDATE で別途切替、ここでは触らない
  synced_at                 = excluded.synced_at
`;

function insertOrders(db, apiOrders, snapshotAt) {
  const ins = db.prepare(INSERT_SQL);
  let inserted = 0, skippedInvalid = 0;
  const skippedExamples = [];

  const tx = db.transaction(() => {
    for (const o of apiOrders) {
      // 必須キー欠落チェック (fail-closed)
      // Codex R1 #7 / R2 #4: PII (user_name, address_*) は ログ出力禁止
      // 未知キー名にも PII が乗る可能性があるため whitelist した既知キーの有無のみ集約
      if (!o.id) {
        skippedInvalid++;
        if (skippedExamples.length < 5) {
          const knownKeys = ['id', 'status', 'item', 'variation', 'selling_price', 'fee', 'bought_on', 'received_on'];
          const presentKnown = knownKeys.filter(k => k in o);
          const missingKnown = knownKeys.filter(k => !(k in o));
          skippedExamples.push({
            reason: 'id missing',
            present_known_keys: presentKnown,
            missing_known_keys: missingKnown,
            unknown_key_count: Object.keys(o).filter(k => !knownKeys.includes(k)).length,
            status: o.status || null,
          });
        }
        continue;
      }
      const orderId = String(o.id);
      const skuCode = (o.variation?.code || '').toString().trim();
      const itemCode = (o.item?.code || '').toString().trim();
      if (!skuCode && !itemCode) {
        skippedInvalid++;
        if (skippedExamples.length < 5) skippedExamples.push({ reason: 'variation.code AND item.code both empty', order_id: orderId, status: o.status || null });
        continue;
      }
      if (!(o.selling_price > 0)) {
        skippedInvalid++;
        if (skippedExamples.length < 5) skippedExamples.push({ reason: 'selling_price <= 0 or missing', order_id: orderId, selling_price: o.selling_price, status: o.status || null });
        continue;
      }

      const row = {
        order_id: orderId,
        status: o.status || null,
        user_name: o.user_name || null,
        selling_price: o.selling_price,
        fee: o.fee != null ? o.fee : null,
        item_id: o.item?.id != null ? o.item.id : null,
        parent_item_code: itemCode || null,
        parent_item_name: o.item?.name || null,
        variation_id: o.variation?.id != null ? o.variation.id : null,
        sku_code: skuCode || itemCode,  // sku_code 主キー、欠損時は item.code に fallback
        sku_name: o.variation?.name || null,
        stock_count: o.stock_count != null ? o.stock_count : 1,
        shipping_fee: o.shipping_fee != null ? o.shipping_fee : null,  // 将来 API 拡張で値が入る、現状 NULL
        bought_on_unix: o.bought_on || null,
        bought_at_jst: unixToJstIso(o.bought_on),
        bought_date_jst: unixToJstDate(o.bought_on),
        payed_on_unix: o.payed_on || null,
        payed_at_jst: unixToJstIso(o.payed_on),
        payed_date_jst: unixToJstDate(o.payed_on),
        delivered_on_unix: o.delivered_on || null,
        delivered_at_jst: unixToJstIso(o.delivered_on),
        delivered_date_jst: unixToJstDate(o.delivered_on),
        received_on_unix: o.received_on || null,
        received_at_jst: unixToJstIso(o.received_on),
        received_date_jst: unixToJstDate(o.received_on),
        payment_deadline_unix: o.payment_deadline || null,
        delivery_deadline_unix: o.delivery_deadline || null,
        delivery_by_hand_deadline_unix: o.delivery_by_hand_deadline || null,
        receive_deadline_unix: o.receive_deadline || null,
        is_delivery_by_hand: o.is_delivery_by_hand != null ? o.is_delivery_by_hand : null,
        delivery_code: o.delivery_code || null,
        delivery_agent: o.delivery_agent || null,
        shipment_date: o.shipment_date || null,
        pre_order_delivery_start: o.pre_order_delivery_start != null ? o.pre_order_delivery_start : null,
        address_from_name: o.address?.from_name || null,
        address_first_name: o.address?.first_name || null,
        address_last_name: o.address?.last_name || null,
        address_zip: o.address?.zip || null,
        address_tel: o.address?.tel || null,
        address_tel_hyphen: o.address?.tel_hyphen || null,
        address_pref: o.address?.pref || null,
        address_city: o.address?.city || null,
        address_address: o.address?.address || null,
        first_seen_at: snapshotAt,
        last_seen_at: snapshotAt,
        last_api_snapshot_at: snapshotAt,
        is_frozen_after_horizon: 0,
        synced_at: snapshotAt,
      };
      ins.run(row);
      inserted++;
    }
  });
  tx();
  return { inserted, skippedInvalid, skippedExamples };
}

// ─── frozen 切替 (§12 設計、Step 3.5 freeze first) ───
function applyHorizonFreeze(db) {
  // last_seen_at は '2026-05-15T00:30:00+09:00' 形式 (JST ISO)。
  // SQLite の date(...) に通すと UTC 正規化されて JST 境界で 1 日早くずれる (Codex R1 #3)
  // → substr(...,1,10) で先頭 10 文字 (YYYY-MM-DD) を JST date 文字列として直接比較。
  // 比較対象の watermark は node 側で JST 計算した文字列を bind。
  const watermark = (() => {
    const d = new Date(Date.now() + 9 * 3600 * 1000 - FROZEN_HORIZON_DAYS * 86400 * 1000);
    return d.toISOString().slice(0, 10);
  })();
  const result = db.prepare(`
    UPDATE raw_linegift_orders
    SET is_frozen_after_horizon = 1
    WHERE is_frozen_after_horizon = 0
      AND substr(last_seen_at, 1, 10) < ?
  `).run(watermark);
  return { changes: result.changes, watermark };
}

// ─── fail-closed 評価 ───
function evaluateFetchResult(label, fetched, inserted, skippedInvalid, totalAvailable) {
  const skipRatio = fetched > 0 ? skippedInvalid / fetched : 0;
  console.log(`[linegift] ${label}: fetched=${fetched} inserted=${inserted} skipped_invalid=${skippedInvalid} (skip_ratio=${(skipRatio * 100).toFixed(1)}%${totalAvailable != null ? `, totalAvailable=${totalAvailable}` : ''})`);
  if (totalAvailable != null && totalAvailable > 0 && fetched < totalAvailable) {
    console.error(`[linegift] FATAL: pagination 欠落 — fetched=${fetched} < totalAvailable=${totalAvailable}`);
    return 'fatal';
  }
  if (fetched > 0 && inserted === 0) {
    console.error(`[linegift] FATAL: fetched=${fetched} だが inserted=0 (API 障害 / schema drift の可能性)`);
    return 'fatal';
  }
  if (skipRatio > 0.05) {
    console.error(`[linegift] FATAL: skip_ratio=${(skipRatio * 100).toFixed(1)}% > 5%`);
    return 'fatal';
  }
  return 'ok';
}

// ─── メイン ───

async function main() {
  const isDryRun = process.argv.includes('--dry-run');

  // 0. signal handler 設置 (lock leak 防止、Codex R1 #6)
  installSignalHandlers();

  // 1. 起動時 staging 復旧チェック
  recoverStagingIfPresent();

  // 2. lock 取得 (重複実行防止 + stale 60min TTL 自動回収)
  if (!acquireLock()) {
    console.error('[linegift] lock 取得失敗 → exit 1 (重複起動防止)');
    process.exit(1);
  }
  try {
    if (!isDryRun) await initDB();
    const db = !isDryRun ? getDB() : null;
    if (db) ensureTables(db);

    // 3. token refresh
    let accessToken = process.env.LINEGIFT_ACCESS_TOKEN;
    if (!accessToken) throw new Error('LINEGIFT_ACCESS_TOKEN 未設定 — 初回 auth code 取得要');
    try {
      accessToken = await refreshAccessToken();
    } catch (e) {
      console.error(`[linegift] token refresh 失敗: ${e.message}`);
      throw e;
    }

    // 4. 受注 API 全件取得
    console.log('[linegift] 受注取得開始 (API 直近 ~3ヶ月)');
    const { orders, paging } = await fetchOrdersAllPages(accessToken);
    console.log(`[linegift] 取得完了: ${orders.length} 注文 (paging: ${JSON.stringify(paging)})`);

    if (isDryRun) {
      console.log('[linegift] --dry-run 指定: DB 保存スキップ、終了');
      releaseLock();
      return;
    }

    // 5. DB 投入 (UPSERT、frozen 行は更新スキップ)
    const snapshotAt = nowJstIso();
    const { inserted, skippedInvalid, skippedExamples } = insertOrders(db, orders, snapshotAt);

    // 6. fail-closed 判定
    const verdict = evaluateFetchResult('fetchLineGift', orders.length, inserted, skippedInvalid, paging?.total_entries);
    if (skippedExamples.length > 0) {
      console.log('[linegift] skip 例 (先頭5):');
      for (const e of skippedExamples) console.log(`  ${JSON.stringify(e)}`);
    }
    if (verdict === 'fatal') {
      releaseLock();
      process.exit(1);
    }

    // 7. frozen 切替 (last_seen_at < today-14d を frozen=1 に、§12)
    const { changes: frozenChanges, watermark: frozenWatermark } = applyHorizonFreeze(db);
    if (frozenChanges > 0) console.log(`[linegift] frozen 切替: ${frozenChanges} 行を is_frozen_after_horizon=1 に (watermark=${frozenWatermark})`);
    else console.log(`[linegift] frozen 切替対象なし (watermark=${frozenWatermark})`);

    updateSyncMeta('linegift_last_sync', snapshotAt);
    console.log(`[linegift] 完了: inserted=${inserted} / skipped=${skippedInvalid} / frozen_changes=${frozenChanges}`);

    releaseLock();
  } catch (e) {
    releaseLock();
    console.error(`[linegift] 致命的エラー: ${e.message}\n${e.stack}`);
    process.exit(1);
  }
}

main();
