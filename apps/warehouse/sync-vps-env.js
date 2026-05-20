/**
 * VPS環境変数同期 — ミニPC .env を真の情報源として VPS .env に反映する
 *
 * 妻が月末にau PAY APIキーをローテーションしたとき、ミニPCの .env を編集するだけで
 * 翌朝の daily-sync で自動的にVPSへ反映される。VPSに直接ログインする必要なし。
 *
 * 動作:
 *   1. ミニPC .env から SYNC_KEYS を読む
 *   2. VPS の /home/rocky/.env を ssh で取得して比較
 *   3. 差分があれば VPS .env の該当行を sed で書き換え + aupay-proxy 再起動
 *   4. 同一なら何もせず終了
 *
 * 使い方:
 *   node apps/warehouse/sync-vps-env.js          # daily-sync から呼ばれる
 *   node apps/warehouse/sync-vps-env.js --dry    # 差分検知のみ、書込まず
 */
import 'dotenv/config';
import { spawnSync } from 'child_process';

const VPS_USER = 'rocky';
const VPS_HOST = '133.167.122.198';
const SSH_KEY = process.env.VPS_SSH_KEY || 'C:/Users/bfaith/.ssh/id_ed25519_vps';
const VPS_ENV_PATH = '/home/rocky/.env';
const VPS_SERVICE = 'aupay-proxy';

// 必須同期 key (ミニPC .env が真の値、VPS .env はミラー。ミニPC .env に無いとエラー)
// au PAY API キーは月次でローテーション。YAHOO_TOKEN_MINT_SECRET は /yahoo/access-token 専用 secret。
const SYNC_KEYS = [
  'AUPAY_API_KEY',
  'YAHOO_TOKEN_MINT_SECRET',
];

// 任意同期 key (ミニPC .env にあるときだけ同期。rotation 用 _NEXT や timeout override 等、常設しない)
const OPTIONAL_SYNC_KEYS = [
  'YAHOO_TOKEN_MINT_SECRET_NEXT',
  'YAHOO_TOKEN_REFRESH_TIMEOUT_MS',
];

const DRY_RUN = process.argv.includes('--dry');

function ts() { return new Date().toISOString().slice(0, 19); }

// Windows OpenSSH は stdout を返した後もプロセスが残ることがある (FDリーク既知問題)。
// timeout で kill されても、ちゃんと結果が返っていれば成功とみなすため、コマンド末尾にマーカーを付けて判定する。
function ssh(command) {
  const fullCmd = `${command} && echo __SYNC_OK__`;
  const r = spawnSync('ssh', [
    '-i', SSH_KEY,
    '-o', 'BatchMode=yes',
    '-o', 'StrictHostKeyChecking=no',
    '-o', 'ConnectTimeout=10',
    '-T', '-n',
    `${VPS_USER}@${VPS_HOST}`,
    fullCmd,
  ], { encoding: 'utf-8', timeout: 15000, stdio: ['ignore', 'pipe', 'pipe'] });

  const stdout = r.stdout || '';
  const stderr = r.stderr || '';

  if (stderr.includes('Permission denied') || stderr.includes('Could not resolve') || stderr.includes('Connection refused')) {
    throw new Error(`SSH失敗: ${stderr.split('\n')[0]}`);
  }
  if (!stdout.includes('__SYNC_OK__')) {
    throw new Error(`SSHコマンド失敗: ${command} | stderr: ${stderr.slice(0, 200)} | stdout: ${stdout.slice(-200)}`);
  }
  return stdout.replace(/__SYNC_OK__\s*$/, '').trimEnd();
}

function parseEnv(content) {
  const map = {};
  for (const line of content.split('\n')) {
    const m = line.match(/^([A-Z_][A-Z0-9_]*)=(.*)$/);
    if (m) map[m[1]] = m[2];
  }
  return map;
}

async function main() {
  console.log(`[${ts()}] [vps-env-sync] 開始${DRY_RUN ? ' (dry-run)' : ''}`);

  // ミニPC .env から取得 (必須 key)
  const localValues = {};
  for (const key of SYNC_KEYS) {
    const v = process.env[key];
    if (!v) {
      throw new Error(`[vps-env-sync] ミニPC .env に ${key} が未設定`);
    }
    localValues[key] = v;
  }
  // 任意 key はミニPC .env にあるときだけ同期対象に含める
  const syncKeys = [...SYNC_KEYS];
  for (const key of OPTIONAL_SYNC_KEYS) {
    if (process.env[key]) {
      localValues[key] = process.env[key];
      syncKeys.push(key);
    }
  }

  // VPS .env 取得
  let vpsContent;
  try {
    vpsContent = ssh(`cat ${VPS_ENV_PATH}`);
  } catch (e) {
    throw new Error(`[vps-env-sync] VPS .env 取得失敗 (SSH/権限を確認): ${e.message.split('\n')[0]}`);
  }
  const vpsValues = parseEnv(vpsContent);

  // 差分検知
  const diffs = syncKeys.filter(k => localValues[k] !== vpsValues[k]);

  if (diffs.length === 0) {
    console.log(`[${ts()}] [vps-env-sync] 差分なし: ${syncKeys.join(', ')}`);
    return;
  }

  console.log(`[${ts()}] [vps-env-sync] 差分検知: ${diffs.join(', ')}`);
  for (const key of diffs) {
    const local = localValues[key];
    const vps = vpsValues[key] || '(未設定)';
    // 値はマスクしてログ出し
    const mask = (s) => s ? `${s.slice(0, 6)}…${s.slice(-4)} (${s.length}文字)` : s;
    console.log(`  ${key}: VPS ${mask(vps)} -> ミニPC ${mask(local)}`);
  }

  if (DRY_RUN) {
    console.log(`[${ts()}] [vps-env-sync] dry-run のため反映スキップ`);
    return;
  }

  // VPS .env を書き換え (該当キーのみ sed で置換)
  for (const key of diffs) {
    const value = localValues[key];
    // 値に英数字以外が含まれていないか念のため検証 (sedの区切り対策)
    if (!/^[A-Za-z0-9_\-./:=+]*$/.test(value)) {
      throw new Error(`[vps-env-sync] ${key} に sed で安全に扱えない文字が含まれている。手動更新してください`);
    }
    ssh(`grep -q '^${key}=' ${VPS_ENV_PATH} && sed -i 's|^${key}=.*|${key}=${value}|' ${VPS_ENV_PATH} || echo '${key}=${value}' >> ${VPS_ENV_PATH}`);
    console.log(`[${ts()}] [vps-env-sync] VPS .env: ${key} 更新完了`);
  }

  // aupay-proxy 再起動
  ssh(`sudo systemctl restart ${VPS_SERVICE}`);
  console.log(`[${ts()}] [vps-env-sync] ${VPS_SERVICE} 再起動完了`);

  // 反映確認: 実際にプロキシ経由で auPay API を1件叩いて新キーで認証通るか確認
  await new Promise(r => setTimeout(r, 4000));
  const proxyUrl = process.env.AUPAY_PROXY_URL || `http://${VPS_HOST}:8080`;
  const proxySecret = process.env.AUPAY_PROXY_SECRET;
  const today = new Date().toISOString().slice(0, 10).replace(/-/g, '');
  const testUrl = `${proxyUrl}/wmshopapi/searchTradeInfoListProc?shopId=${process.env.AUPAY_SHOP_ID || '54318092'}&totalCount=1&startCount=1&startDate=${today}&endDate=${today}&dateType=0`;
  const res = await fetch(testUrl, { headers: { 'X-Proxy-Secret': proxySecret } });
  if (res.status !== 200) {
    throw new Error(`[vps-env-sync] proxy 疎通NG: HTTP ${res.status}`);
  }
  const body = await res.text();
  if (!body.includes('<status>0</status>')) {
    const errMatch = body.match(/<error>.*?<message>([^<]+)<\/message>/);
    throw new Error(`[vps-env-sync] proxy 疎通NG: auPay APIエラー ${errMatch ? errMatch[1] : body.slice(0, 200)}`);
  }

  console.log(`[${ts()}] [vps-env-sync] 完了: ${diffs.length}件更新、proxy疎通OK`);
}

main().catch(e => {
  console.error(`[${ts()}] [vps-env-sync] エラー: ${e.message}`);
  process.exit(1);
});
