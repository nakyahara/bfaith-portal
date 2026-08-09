/**
 * tracking-runner.js の純粋関数 (JST日付 / 通知本文) のテスト。
 *   node apps/fba-replenishment/tests/test-tracking-runner.mjs
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'fba-runner-test-'));
// SP-APIの疎通はしない。missingEnv を通すためだけのダミー
for (const k of ['SP_API_CLIENT_ID', 'SP_API_CLIENT_SECRET', 'SP_API_REFRESH_TOKEN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']) {
  process.env[k] = process.env[k] || 'dummy-for-test';
}

const { jstYmd, formatSummary, runTrackingJob } = await import('../tracking-runner.js');
const store = await import('../tracking-store.js');
const { _internal } = await import('../tracking-service.js');

let pass = 0;
const t = (name, fn) => { fn(); pass++; console.log(`  ok  ${name}`); };
console.log('tracking-runner');

t('JST日付: UTC深夜でも日本の日付になる (toISOStringの罠)', () => {
  assert.equal(jstYmd(new Date('2026-08-07T13:00:00Z')), '20260807'); // 22:00 JST
  assert.equal(jstYmd(new Date('2026-08-07T15:30:00Z')), '20260808'); // 翌日 00:30 JST
  assert.equal(jstYmd(new Date('2026-08-06T23:00:00Z')), '20260807');
});

const base = { runId: 'r', commit: true, severity: 'ok', openShipments: 0, expectYmd: '20260807', blocked: [], excluded: [], skipped: [], registered: [], failed: [], note: [] };

t('中断時は「登録は一切していません」と明記する', () => {
  const s = formatSummary({ ...base, severity: 'blocked', blocked: ['個口合計 5 と輸送箱 6 が一致しません'] });
  assert.match(s, /🚨/);
  assert.match(s, /登録は一切していません/);
  assert.match(s, /個口合計 5 と輸送箱 6/);
  assert.match(s, /翌日 Seller Central 画面から手入力/);
});

t('成功時は納品ごとに箱数と照合方法を出す', () => {
  const s = formatSummary({ ...base, severity: 'ok', registered: [{ shipmentConfirmationId: 'FBA15GGL5J2X', fcCode: 'HND2', countBoxes: 5, matchedBy: '納品番号' }] });
  assert.match(s, /✅/);
  assert.match(s, /FBA15GGL5J2X \(HND2\) 5箱  照合=納品番号/);
});

t('期限切れは「画面から手入力してください」を添える', () => {
  const s = formatSummary({ ...base, severity: 'warn', failed: [{ shipmentConfirmationId: 'FBA-A', error: '編集期限を過ぎています', needsManual: true }] });
  assert.match(s, /⚠️/);
  assert.match(s, /画面から手入力してください/);
});

t('🚨除外は黙って捨てず件数を必ず出す', () => {
  const s = formatSummary({ ...base, excluded: [{ fcCode: 'AMRC', 件数: 4, reason: 'FBA以外' }] });
  assert.match(s, /除外 4件/);
  assert.match(s, /AMRC/);
});

t('🚨納品が無い日 (CSV未設置) は情報どまり — 毎晩の誤警報を出さない', () => {
  const s = formatSummary({ ...base, severity: 'info', note: ['本日の出荷実績CSVはまだ置かれていません。FBA納品が無い日なら正常です'] });
  assert.match(s, /^ℹ️/);
  assert.doesNotMatch(s, /🚨/);
  assert.doesNotMatch(s, /登録は一切していません/);
});

t('対象の納品が無いだけなら警告どまり (中断にしない)', () => {
  const s = formatSummary({ ...base, severity: 'warn', note: ['追跡番号の未登録な納品が見つかりません'] });
  assert.match(s, /^⚠️/);
  assert.doesNotMatch(s, /🚨/);
});

t('箱数不一致は中断 (severityで見分ける)', () => {
  const s = formatSummary({ ...base, severity: 'blocked', blocked: ['個口合計 5 と輸送箱 6 が一致しません'] });
  assert.match(s, /^🚨/);
  assert.match(s, /登録は一切していません/);
});

t('🚨投入待ちの納品が無い日は情報どまり — 置きっぱなしCSVで赤を出さない', () => {
  const s = formatSummary({ ...base, severity: 'info', openShipments: 0,
    note: ['追跡番号の投入を待っている納品はありません (納品が無い日、すでに画面で入力済み、またはラベル未発行)'] });
  assert.match(s, /^ℹ️/);
  assert.doesNotMatch(s, /🚨/);
  assert.doesNotMatch(s, /登録は一切していません/);
});

t('🚨投入待ちがあるのにCSVが古いときは赤 + 対象を並べる', () => {
  const s = formatSummary({ ...base, severity: 'blocked', openShipments: 2, blocked: [
    'CSVの出荷日が期待 (20260808) と一致しません → 20260807:19件。古いCSVが残っていないか、出力時の日付指定を確認してください',
    '追跡番号待ちの納品: FBA15GGL5J2X(HND2) 5箱 / FBA15GGLDVMG(XHD4) 14箱',
  ] });
  assert.match(s, /^🚨/);
  assert.match(s, /FBA15GGL5J2X\(HND2\) 5箱/);
});

t('🚨投入待ちがあるのにCSVが無いときは、何をすべきか書く', () => {
  const s = formatSummary({ ...base, severity: 'blocked', openShipments: 1, blocked: [
    '追跡番号待ちの納品が 1件あるのに、出荷実績CSVが取得できません: ファイルが見つかりません',
    '対象: FBA15GGL5J2X(HND2) 5箱',
    'iS-2の「出荷実績印刷・CSV出力」から本日分を fukutsu_tuiseki.csv として保存してください',
  ] });
  assert.match(s, /出荷実績印刷・CSV出力/);
  assert.match(s, /fukutsu_tuiseki\.csv/);
});

t('プレビューはタイトルで分かる', () => {
  assert.match(formatSummary({ ...base, commit: false }), /追跡番号\(プレビュー\)/);
  assert.doesNotMatch(formatSummary({ ...base, commit: true }), /プレビュー/);
});

// ── 2巡目レビューの修正 ──────────────────────────────
t('🚨受理されたか断定できない例外は indeterminate にする (再送で二重投入しない)', () => {
  const c = _internal.classifyPutError;
  // Amazonが明確に拒否したもの = 確定失敗
  assert.equal(c({ code: 'BadRequest', message: 'is not in a state where tracking details can be provided' }).indeterminate, false);
  assert.equal(c({ code: 'InvalidInput', message: 'validation error detected' }).indeterminate, false);
  // 通信起因 = 受理された可能性がある
  assert.equal(c({ code: 'ETIMEDOUT', message: 'socket hang up' }).indeterminate, true);
  assert.equal(c({ code: 'ECONNRESET', message: 'read ECONNRESET' }).indeterminate, true);
  assert.equal(c(new Error('Internal Server Error')).indeterminate, true);
  assert.equal(c(undefined).indeterminate, true);
});

await (async () => {
  const name = '🚨投入記録に読めない行があれば中断する (pendingを見失って再送しない)';
  fs.appendFileSync(store.storePath(), '{壊れたJSON' + String.fromCharCode(10), 'utf-8');
  const s = await runTrackingJob({ file: 'これは読まれない.csv' });
  assert.equal(s.severity, 'blocked', JSON.stringify(s));
  assert.ok(s.blocked.some((b) => b.includes('投入記録に読めない行があります')), s.blocked.join(' / '));
  fs.writeFileSync(store.storePath(), '', 'utf-8');
  pass++; console.log(`  ok  ${name}`);
})();

await (async () => {
  const name = 'ロックは実行後に解放される (次の実行が止まらない)';
  const s = await runTrackingJob({ file: 'これは読まれない.csv' });
  assert.ok(s.severity, JSON.stringify(s));
  assert.equal(store.acquireLock('after').ok, true, '前の実行のロックが残っていない');
  store.releaseLock('after');
  pass++; console.log(`  ok  ${name}`);
})();

fs.rmSync(process.env.DATA_DIR, { recursive: true, force: true });

console.log(`\n${pass} 件すべて通過`);
