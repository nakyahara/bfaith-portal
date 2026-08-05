/**
 * 定期実行の棚卸しテスト — 「台帳に載っていないスケジュールが増えたら落ちる」
 * 実行: node apps/jobs-monitor/test-schedule-inventory.js
 *
 * ⭐これが無いと同じ事故が再発する。
 *   2026-08-01 の棚卸しは miniPC の Task Scheduler だけを見ていたため、Render 内の
 *   node-cron がカテゴリごと台帳から漏れ、6本が「止まっても誰も気づかない」まま4ヶ月動いた
 *   (2026-08-05 に外部からの指摘で発覚)。
 *   「新設したら台帳に足す」は人の規律に頼るルールなので、破られても静かに通ってしまう。
 *   ここでコード上の全スケジュールを機械的に数え、台帳 id か除外理由の宣言を強制する。
 *
 * 新しく cron.schedule / setInterval を書いたらこのテストが落ちる。直し方は2つ:
 *   (a) 業務ジョブ → config/jobs-registry.mjs に登録し、ping を配線して job: を書く
 *   (b) 業務ジョブでない (プロセス内観測・ロック延命・CLI等) → exempt: に理由を書く
 * どちらの場合も「何も考えずに通す」道は用意しない。
 */
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import { JOBS_REGISTRY } from '../../config/jobs-registry.mjs';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../..');
const SCAN_DIRS = ['apps', 'lib', 'scripts'];
const SCAN_FILES = ['server.js'];
const CALL_RE = /(?:cron\.schedule|setInterval)\s*\(/g;

/**
 * 既知のスケジュール一覧。値は「台帳の id」か「監視対象外の理由」。
 * count は同一ファイル内の呼び出し回数 (増減したら落ちる = 見落としを防ぐ)。
 */
const KNOWN = {
  // ── Render 内の業務ジョブ (台帳登録済み) ──
  'apps/fba-replenishment/router.js': { count: 1, job: 'fba-daily-sync' },
  'apps/inbound-info/sync-job.js': { count: 1, job: 'inbound-info-daily' },
  'apps/mgmt-accounting/router.js': { count: 1, job: 'mgmt-auto-sync' },
  'apps/purchase-orders/email.js': { count: 0, job: 'po-email-dispatcher' }, // setTimeout 再帰なので呼び出しは数えない
  'apps/warehouse/healthcheck.js': { count: 1, job: 'warehouse-healthcheck' },
  'apps/product-hub/intake-cron.js': { count: 1, job: 'ph-ne-intake' },
  'apps/ranking-checker/scheduler.js': { count: 2, job: 'rankcheck-csv-export' }, // 自動チェック(既定OFF) + CSV出力

  // ── 監視の本体 ──
  'apps/jobs-monitor/notify-job.js': { count: 1, exempt: '監視ループ自身 (これが止まれば GAS の見張りが鳴る)' },

  // ── Dark Launch (既定OFF)。点火するときに台帳へ登録する ──
  'apps/profit-analysis/notify-job.js': { count: 1, exempt: 'Dark Launch (INVENTORY_NOTIFY_ENABLED)。点火時に要登録' },
  'apps/profit-analysis/margin-alert-job.js': { count: 1, exempt: 'Dark Launch (MARGIN_ALERT_ENABLED)。点火時に要登録' },
  'apps/biz-ops-overview/notify-job.js': { count: 1, exempt: 'Dark Launch (SALES_NOTIFY_ENABLED)。点火時に要登録' },
  'apps/ai-insights/notify-job.js': { count: 1, exempt: 'Dark Launch (AI_INSIGHTS_NOTIFY_ENABLED)。点火時に要登録' },
  'apps/rakuten-yahoo-sync/services/rys-cron.js': { count: 1, exempt: 'Dark Launch (RYS_FULL_SYNC_CRON_ENABLED)。点火時に要登録' },
  'apps/shipping-log/notion-staff.js': { count: 1, exempt: 'Dark Launch (SHIPPING_STAFF_CRON_ENABLED)。点火時に要登録' },
  'apps/inquiry-hub/sync/cron.js': { count: 3, exempt: 'Dark Launch (INQUIRY_HUB_*_CRON_ENABLED)。点火時に要登録' },
  'apps/render-backup/backup-render.js': { count: 2, exempt: 'Dark Launch (RENDER_BACKUP_CRON_ENABLED)。点火時に要登録' },

  // ── 業務ジョブではないもの ──
  'apps/observability/metrics.js': { count: 1, exempt: 'プロセス内観測 (event-loop lag / heap)。業務データを触らない' },
  'apps/observability/disk-watch.js': { count: 1, exempt: 'プロセス内観測 (DATA_DIR 使用率)。業務データを触らない' },
  'apps/mercari-sync/router.js': { count: 1, exempt: 'メモリ上の一時ジョブの TTL 掃除。止まってもデータは壊れない' },
  'apps/warehouse/job-manager.js': { count: 1, exempt: '実行中ジョブの後片付け。単発ジョブに追随するだけ' },
  'apps/warehouse/job-locks.js': { count: 1, exempt: 'ロックの期限切れ掃除' },
  'apps/warehouse/fba-service.js': { count: 1, exempt: 'オンデマンド実行の進捗ポーリング (人の操作起点)' },
  'apps/warehouse-mirror/build-lock.js': { count: 2, exempt: 'ビルド中ロックの延命 heartbeat (処理の一部)' },
  'apps/warehouse-mirror/build-linegift-analytics-mart.js': { count: 1, exempt: '長時間処理内の進捗ログ' },
  'apps/warehouse-mirror/generate-gift-season-occurrences.js': { count: 1, exempt: '長時間処理内の進捗ログ' },
  'scripts/ai-insights/runner-lib.js': { count: 1, exempt: 'CLI ランナー内の進捗表示' },
  'server.js': { count: 1, exempt: 'PERF_LOG=1 のときだけのデバッグ計測' },

  // ── miniPC 側で動く (Render では起動しない) ──
  'apps/warehouse/auto-import.js': { count: 1, exempt: 'miniPC の取込ウォッチャ。台帳では warehouse-daily-sync 側で見ている' },

  // ── 起動していない死にコード ──
  'apps/profit-calculator/price-scheduler.js': { count: 3, exempt: 'server.js の呼び出しがコメントアウト済み = 未起動 (2026-03-30〜)' },
};

let pass = 0;
const fails = [];
function ok(cond, label) { if (cond) pass++; else fails.push(label); }

/** ディレクトリを再帰的に走査して .js/.mjs を集める (node_modules とテスト類は除く) */
function collect(dir, out = []) {
  for (const e of fs.readdirSync(path.join(ROOT, dir), { withFileTypes: true })) {
    const rel = `${dir}/${e.name}`;
    if (e.isDirectory()) {
      if (e.name === 'node_modules' || e.name === '.git') continue;
      collect(rel, out);
    } else if (/\.(js|mjs)$/.test(e.name) && !/test|smoke/i.test(rel)) {
      out.push(rel);
    }
  }
  return out;
}

const files = [...SCAN_FILES, ...SCAN_DIRS.flatMap((d) => collect(d))];
const found = new Map();
for (const rel of files) {
  const src = fs.readFileSync(path.join(ROOT, rel), 'utf8');
  const n = (src.match(CALL_RE) || []).length;
  if (n > 0) found.set(rel, n);
}

// 1. 未申告のスケジュールが無いこと (= 新設したのに台帳も除外理由も書いていない)
for (const [rel, n] of found) {
  const known = KNOWN[rel];
  ok(known !== undefined,
    `未申告のスケジュール: ${rel} (${n}箇所)\n`
    + '     → 業務ジョブなら config/jobs-registry.mjs に登録して ping を配線し、\n'
    + '       そうでなければこのテストの KNOWN に exempt 理由を書いてください');
  if (!known) continue;
  ok(known.count === n,
    `${rel}: スケジュールの数が変わりました (宣言 ${known.count} / 実際 ${n})。増えた分の扱いを決めてください`);
}

// 2. KNOWN の掃除漏れが無いこと (消したファイルが残っていると棚卸しが嘘になる)
for (const rel of Object.keys(KNOWN)) {
  const n = found.get(rel) ?? 0;
  const declared = KNOWN[rel].count;
  ok(n > 0 || declared === 0, `KNOWN に残骸: ${rel} は今スケジュールを持ちません。行を削除してください`);
}

// 3. job: に書いた id が実在すること
const registered = new Set(JOBS_REGISTRY.map((e) => e.id));
for (const [rel, v] of Object.entries(KNOWN)) {
  if (!v.job) continue;
  ok(registered.has(v.job), `${rel}: job "${v.job}" が台帳にありません`);
}

// 4. どちらか一方だけを持つこと (両方 or どちらも無い = 判断が曖昧なまま通っている)
for (const [rel, v] of Object.entries(KNOWN)) {
  ok(!!v.job !== !!v.exempt, `${rel}: job と exempt はどちらか一方だけにしてください`);
}

if (fails.length) {
  console.error(`❌ ${fails.length} 件失敗 / ${pass} 件成功`);
  for (const f of fails) console.error(`  - ${f}`);
  process.exitCode = 1;
} else {
  console.log(`✅ 全 ${pass} 件パス (スケジュール ${found.size} ファイル / 台帳 ${JOBS_REGISTRY.length} 件)`);
}
