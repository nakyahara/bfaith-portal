/**
 * 発注設定マスタ 初期移行（一度きり）
 *
 * 現行スプレッドシート「商品管理リスト」の CSV から「推奨保有在庫」(= 推奨保有月数の係数) を
 * m_reorder_setting に投入する。CSV の列構成（ヘッダー名で自動判定、無ければ既定位置）:
 *   0: 商品コード ... 19: 推奨保有在庫(係数)
 * ※ この CSV はヘッダー/商品名セルに引用符内改行・カンマを含むため RFC4180 準拠で解析する。
 *
 * 冪等: INSERT OR REPLACE。初回移行専用。値域 0〜60、範囲外/非数値はスキップ。
 *
 * 使い方:
 *   node apps/warehouse/migrate-reorder-setting-initial.js --csv="path/to/商品管理リスト.csv" [--dry-run]
 */
import fs from 'fs';
import iconv from 'iconv-lite';
import { getDB, initDB } from './db.js';
import { legacyCliGate, runWithLegacyCliLock, closeLegacyGatePool } from '../../lib/master-legacy-gate.mjs';

const REORDER_MONTHS_MAX = 60;
function parseMonths(v) {
  if (v === undefined || v === null || String(v).trim() === '') return null;
  const n = Number(String(v).trim());
  if (!Number.isFinite(n) || n < 0 || n > REORDER_MONTHS_MAX) return null;
  return Math.round(n * 10) / 10;
}

// RFC4180 準拠の最小 CSV パーサ（引用符内のカンマ・改行・"" エスケープに対応）
function parseCsv(text) {
  const rows = [];
  let row = [], field = '', inQuotes = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (inQuotes) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; }
        else inQuotes = false;
      } else field += c;
    } else {
      if (c === '"') inQuotes = true;
      else if (c === ',') { row.push(field); field = ''; }
      else if (c === '\r') { /* skip */ }
      else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
      else field += c;
    }
  }
  if (field !== '' || row.length) { row.push(field); rows.push(row); }
  return rows;
}

function detectCol(header, candidates, fallback) {
  const idx = header.findIndex(h => candidates.some(c => (h || '').includes(c)));
  return idx >= 0 ? idx : fallback;
}

async function main() {
  // 🚨 推奨保有月数 (m_reorder_setting) はマスタ = 古い入口の門を通す (Company DB構想 14 §9 M2・契約 v3 H1・PR #1565 R1)。
  //    切替の段階が legacy_open のときだけ今までどおり。frozen 以降・段階が読めない = 引数・ファイルの検査より前・DB を開く前に終了コード 3 (--dry-run も)。
  //    書く直前にもう一度読む
  if (!(await legacyCliGate('cli:migrate-reorder-setting-initial.js'))) return;
  const args = process.argv.slice(2);
  const csvPath = args.find(a => a.startsWith('--csv='))?.split('=').slice(1).join('=');
  const dryRun = args.includes('--dry-run');
  // 🚨 process.exit は使わない (段階を読んだ PostgreSQL の接続の後に呼ぶと Windows で終了コード 127 になることがある。中間レビュー 2 回目 Low)
  if (!csvPath) { console.error('--csv=... が必要です'); process.exitCode = 2; return; }
  if (!fs.existsSync(csvPath)) { console.error(`CSVが見つかりません: ${csvPath}`); process.exitCode = 2; return; }

  // UTF-8(BOM可) / Shift_JIS 自動判定
  //   まず UTF-8 として解釈し、置換文字(U+FFFD)が多ければ cp932 とみなす。
  //   (高バイト数だけで判定すると UTF-8(BOMなし) の日本語を誤って cp932 扱いしてしまう)
  const raw = fs.readFileSync(csvPath);
  let text;
  if (raw[0] === 0xEF && raw[1] === 0xBB && raw[2] === 0xBF) {
    text = raw.slice(3).toString('utf-8');
  } else {
    const utf8 = raw.toString('utf-8');
    const replacementCount = (utf8.match(/�/g) || []).length;
    text = replacementCount > 3 ? iconv.decode(raw, 'cp932') : utf8;
  }

  const records = parseCsv(text).filter(r => r.some(c => (c || '').trim() !== ''));
  if (!records.length) { console.error('行がありません'); process.exitCode = 2; return; }

  const header = records[0].map(h => (h || '').replace(/\s+/g, '').trim());
  const colSku = detectCol(header, ['商品コード'], 0);
  const colMonths = detectCol(header, ['推奨保有'], 19);
  console.log(`列判定: 商品コード=col${colSku} (${header[colSku]}) / 推奨保有=col${colMonths} (${header[colMonths]})`);

  await initDB();
  const db = getDB();
  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);
  const getName = db.prepare('SELECT 商品名 FROM m_products WHERE 商品コード = ? COLLATE NOCASE');
  const stmt = db.prepare('INSERT OR REPLACE INTO m_reorder_setting (sku, 推奨保有月数, 商品名, updated_by, synced_at) VALUES (?, ?, ?, ?, ?)');

  let imported = 0, skipped = 0, invalid = 0;
  const apply = db.transaction((dataRows) => {
    for (const row of dataRows) {
      const sku = (row[colSku] || '').toLowerCase().trim();
      if (!sku) { skipped++; continue; }
      const months = parseMonths(row[colMonths]);
      if (months === null) { invalid++; skipped++; continue; }
      if (!dryRun) {
        const name = getName.get(sku)?.商品名 || '';
        stmt.run(sku, months, name, 'initial-migration', now);
      }
      imported++;
    }
  });
  // 書くところ = 段階の鍵を共有で持ったまま読み直し、legacy_open のときだけ書く (段階を変える関数は書き終わるまで待つ)
  const { ran } = await runWithLegacyCliLock('cli:migrate-reorder-setting-initial.js', () => apply(records.slice(1)));
  if (!ran) return;

  console.log(`${dryRun ? '[DRY-RUN] ' : ''}投入 ${imported} / スキップ ${skipped} (無効 ${invalid}) / 全 ${records.length - 1} 行`);
}

// 終わり方: 接続を閉じてから自然に終わる (process.exit を呼ばない)。終了コードは process.exitCode
main().catch(e => { console.error(e); process.exitCode = 1; }).finally(() => closeLegacyGatePool());
