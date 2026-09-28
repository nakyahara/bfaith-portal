import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * test-amazon-finance-months.js — 日次の財務を毎朝どの月について作り直すか (amazon-finance-months.js) の試験
 *
 * 2026-09-28: 5 月の日次の財務が半分欠けていた (月末をまたぐ決済が翌月に届いたのに、前月を作り直さなかった)。
 * 「当月 + 直近 35 日に決済の行が入った月」になっていること:
 *   ① 翌月の 21 日以降に届いた前月の決済でも前月を作り直す (旧「20 日まで」では落ちた)
 *   ② 5 月の穴の再現: 6/3 に 5 月の行が入った → 6 月の朝は 5 月も作り直す
 *   ③ 35 日より前に入った月は作り直さない / 当月は決済が無くても必ず / 当月より先の月 (日付の誤り) は作らない / 新しい月から
 *   ④ 35 日の境目 ⑤ やり残しの持ち越し・月を決められないときは 当月 + 前月 + やり残し で warn・壊れたファイル (消さずに名前を変えて残し、片付くまで warn) ⑥ 取り直しで入った時刻が動かない (Codex #1514 R1・R2)
 *
 * 実行: node apps/warehouse/test-amazon-finance-months.js (daily-sync 冒頭でも実行)。本番 DB には触れない (一時 DATA_DIR)
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'finance-months-test-'));
process.env.DATA_DIR = tmpDir;
const { initDB, getDB } = await import('./db.js');
const { financeMonthsToBuild, pickFinanceMonths, FINANCE_DIRTY_DAYS, DIRTY_MONTHS_SQL } = await import('./amazon-finance-months.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
await initDB();
const db = getDB();
let n = 0;
const line = (ym, ingestedAt) => db.prepare(`INSERT INTO raw_amazon_settlement_lines (physical_line_hash, business_line_key, source_document_id, source_file_hash, source_path, source_line_no, source_layer, parser_version, source_settlement_id, year_month_int, posted_date_utc, posted_datetime_jst, economic_date, transaction_type, currency, ingest_run_id, observed_at, ingested_at)
  VALUES (?, 'k', 'D', 'h', 'p', ?, 'sp_api_v2', 'v2.0.0', 'S', ?, ?, ?, ?, 'Order', 'JPY', 'r', ?, ?)`).run(`ph-${++n}`, n, ym, `${String(ym).slice(0, 4)}-${String(ym).slice(4)}-10T00:00:00+00:00`, `${String(ym).slice(0, 4)}-${String(ym).slice(4)}-10 09:00:00`, `${String(ym).slice(0, 4)}-${String(ym).slice(4)}-10`, ingestedAt, ingestedAt);
const at = (iso) => new Date(iso);

ok(FINANCE_DIRTY_DAYS === 35, '既定は 35 日');
// ① 前月の決済が翌月 25 日に届いた (旧の「20 日まで」では前月を作り直さなかった)
line(202608, '2026-09-25 22:00:00');
ok(JSON.stringify(financeMonthsToBuild(db, { currentMonth: '2026-09', now: at('2026-09-26T00:00:00Z') })) === JSON.stringify(['2026-09', '2026-08']), '翌月 25 日に届いた前月の決済 → 前月も作り直す');
// ② 5 月の穴の再現: 6/3 に 5 月の行 → 6 月の朝 (6/10) は 5 月も / 7/9 (35 日を過ぎた) は作らない
line(202605, '2026-06-02 22:02:04');
ok(financeMonthsToBuild(db, { currentMonth: '2026-06', now: at('2026-06-10T00:00:00Z') }).includes('2026-05'), '5 月の穴の再現: 6/3 に 5 月の行が入った → 6/10 の朝は 5 月も作り直す');
ok(!financeMonthsToBuild(db, { currentMonth: '2026-07', now: at('2026-07-09T00:00:00Z') }).includes('2026-05'), '35 日より前に入った月は作り直さない');
// ③ 当月は決済が無くても必ず・当月より先の月は作らない・新しい月から
line(203001, '2026-09-20 00:00:00');
line(202607, '2026-09-20 00:00:00');
const m = financeMonthsToBuild(db, { currentMonth: '2026-10', now: at('2026-10-01T00:00:00Z') });
ok(m[0] === '2026-10' && !m.includes('2030-01'), `当月が先頭・当月より先の月 (日付の誤り) は作らない (${m.join(', ')})`);
ok(JSON.stringify(m) === JSON.stringify(['2026-10', '2026-08', '2026-07']), `ほかの月は新しい月から (${m.join(', ')})`);
let threw = false; try { financeMonthsToBuild(db, { currentMonth: '2026-9' }); } catch { threw = true; }
ok(threw, '当月の書き方が違えば止める (daily-sync は 当月 + 前月 に戻す)');
// 入った時刻の索引で引く (無いと本番で 443 万行を毎朝全部読んで 97 秒)
const plan = db.prepare(`EXPLAIN QUERY PLAN ${DIRTY_MONTHS_SQL}`).all('2026-01-01 00:00:00').map((r) => r.detail).join(' / ');
ok(/idx_settle_lines_ingested/.test(plan), `入った時刻の索引で引く (${plan})`);
// DATA_DIR から開く (daily-sync が呼ぶ形)
ok(pickFinanceMonths(tmpDir, { currentMonth: '2026-09', now: at('2026-09-26T00:00:00Z') }).join(',') === '2026-09,2026-08,2026-07', 'DATA_DIR の warehouse.db を読み取り専用で開いて決める');

// ④ 35 日の境目: ちょうど 35 日前は入る・その 1 秒前は入らない
const NOW = at('2026-11-20T00:00:00Z');
line(202610, '2026-10-16 00:00:00');   // ちょうど 35 日前
line(202609, '2026-10-15 23:59:59');   // 35 日と 1 秒前
const edge = financeMonthsToBuild(db, { currentMonth: '2026-11', now: NOW });
ok(edge.includes('2026-10') && !edge.includes('2026-09'), `35 日の境目: ちょうど 35 日前は入る・1 秒前は入らない (${edge.join(', ')})`);

// ⑤ 計画 (planFinanceMonths): やり残しを持ち越す / 月を決められなければ 当月 + 前月 + やり残し に戻って warn (Codex #1514 R1)
const { planFinanceMonths, writePendingMonths, readPendingMonths, PENDING_FILE } = await import('./amazon-finance-months.js');
let p = planFinanceMonths(tmpDir, { currentMonth: '2026-11', now: NOW });
ok(JSON.stringify(p.months) === JSON.stringify(['2026-11', '2026-10']) && !p.warn, `やり残しが無い朝 = 当月 + 直近 35 日の月・warn なし (${p.months.join(', ')})`);
writePendingMonths(tmpDir, ['2026-03', '2026-10', '2026-03']);
p = planFinanceMonths(tmpDir, { currentMonth: '2026-11', now: NOW });
ok(JSON.stringify(p.months) === JSON.stringify(['2026-11', '2026-10', '2026-03']) && !p.warn, `🚨 やり残し (35 日を過ぎた 3 月) も持ち越す・重なりは 1 回 (${p.months.join(', ')})`);
p = planFinanceMonths(tmpDir, { currentMonth: '2026-11', now: NOW, pick: () => { throw new Error('no such index: idx_settle_lines_ingested'); } });
ok(JSON.stringify(p.months) === JSON.stringify(['2026-11', '2026-10', '2026-03']) && p.warn && /決められない/.test(p.notes.join()), `🚨 月を決められない = 当月 + 前月 + やり残し に戻り warn (daily-sync で ⚠️) (${p.months.join(', ')} / ${p.notes.join(' / ')})`);
writePendingMonths(tmpDir, []);
ok(readPendingMonths(tmpDir).months.length === 0, '作り直しと送信が通った月はやり残しから消える');
fs.writeFileSync(path.join(tmpDir, PENDING_FILE), '{壊れた');
p = planFinanceMonths(tmpDir, { currentMonth: '2026-11', now: NOW });
ok(p.warn && /読めない/.test(p.notes.join()) && p.months[0] === '2026-11', `やり残しのファイルが壊れていれば warn (月は決める) (${p.notes.join(' / ')})`);
// 🚨 読めなかった朝の回の終わり: 壊れたファイルは消さずに名前を変えて残す + 今回の失敗は新しいファイルへ / 翌朝も warn が続く / 人が片付けたら消える (Codex #1514 R2)
writePendingMonths(tmpDir, ['2026-11'], { now: at('2026-11-20T08:00:00Z') });
const corrupt = fs.readdirSync(tmpDir).filter((x) => /^amazon-finance-pending.corrupt-/.test(x));
ok(corrupt.length === 1 && fs.readFileSync(path.join(tmpDir, corrupt[0]), 'utf8') === '{壊れた', `壊れたファイルは消さずに名前を変えて残す (${corrupt.join(', ')})`);
ok(JSON.stringify(readPendingMonths(tmpDir).months) === JSON.stringify(['2026-11']), '今回失敗した月は新しいやり残しのファイルへ');
p = planFinanceMonths(tmpDir, { currentMonth: '2026-12', now: at('2026-12-01T00:00:00Z') });
ok(p.warn && /読めなかったやり残しのファイルが残っている/.test(p.notes.join()) && p.months.includes('2026-11'), `翌朝も warn が続く + 新しいやり残し (2026-11) は作り直す (${p.notes.join(' / ')})`);
writePendingMonths(tmpDir, [], { now: at('2026-12-01T08:00:00Z') });
ok(fs.existsSync(path.join(tmpDir, corrupt[0])), '読めるファイルを書き直しても、残した corrupt ファイルは人が片付けるまで消さない');
fs.unlinkSync(path.join(tmpDir, corrupt[0]));
ok(!planFinanceMonths(tmpDir, { currentMonth: '2026-12', now: at('2026-12-02T00:00:00Z') }).warn, '人が片付けたら warn は消える');

// ⑥ 同じレポートを取り直しても、決済の行の入った時刻は動かない (INSERT OR IGNORE) = 新しい決済が入った月だけが 35 日のあいだ対象
const { prepareReportTsv, ingestSettlement } = await import('./fetch-amazon-settlements.js');
const tsv = ['settlement-id\tsettlement-start-date\tsettlement-end-date\tdeposit-date\ttotal-amount\tcurrency\ttransaction-type\torder-id\tposted-date\tsku\tprice-type\tprice-amount',
  'S-RE\t2026-11-01T00:00:00+00:00\t2026-11-15T00:00:00+00:00\t2026-11-17T00:00:00+00:00\t100.00\tJPY\t\t\t\t\t\t',
  'S-RE\t\t\t\t\tJPY\tOrder\tO1\t2026-11-05T00:00:00+00:00\tA\tPrincipal\t100.00'].join('\n') + '\n';
const p1 = prepareReportTsv(tsv, 'R-RE', 'run1');
for (const r of p1.lineRows) r.ingested_at = '2026-11-16 00:00:00';
ingestSettlement(db, p1.headerRow, p1.lineRows, p1.ctx);
const p2 = prepareReportTsv(tsv, 'R-RE', 'run2');
for (const r of p2.lineRows) r.ingested_at = '2026-12-30 00:00:00';   // 翌月末に同じレポートを取り直した
ingestSettlement(db, p2.headerRow, p2.lineRows, p2.ctx);
ok(db.prepare(`SELECT GROUP_CONCAT(ingested_at) g FROM raw_amazon_settlement_lines WHERE source_settlement_id = 'S-RE'`).get().g === '2026-11-16 00:00:00', '同じレポートを取り直しても入った時刻は最初のまま');
ok(!financeMonthsToBuild(db, { currentMonth: '2027-01', now: at('2027-01-05T00:00:00Z') }).includes('2026-11'), '取り直しだけでは 35 日を過ぎた月を作り直さない (新しい決済が入った月だけ)');

console.log(failed ? `\n❌ ${failed} 件 失敗` : '\n=== 作り直す月の試験 ALL PASS ===');
process.exit(failed ? 1 : 0);
