import { temporaryTestRoot } from '../../scripts/test-temp-dir.mjs';
await temporaryTestRoot(import.meta.url);
/**
 * ゆうパケットパフ 追跡番号CSV取込 — ゆうプリR → ゆうプリクラウド 移行 (2026-09-18) のテスト。
 *   node apps/packing-dispatch/test-tracking-import-yupuri-cloud.mjs
 *
 * 守りたいこと: クラウド出力 (UTF-8 BOM付き / 全項目ダブルクォート / 1列目 発送予定日が空) を
 * 取り込んでも、旧ゆうプリR形式 (Shift-JIS / 引用符なし) と同じ結果になること。
 * 列はヘッダ名で引いているので並びが増減しても効くが、それを実物の形で固定する。
 * DATA_DIR を一時ディレクトリに向ける (本番mirrorには触れない)。
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import iconv from 'iconv-lite';

process.env.DATA_DIR = fs.mkdtempSync(path.join(os.tmpdir(), 'pd-yupuri-cloud-test-'));

const { initMirrorDB } = await import('../warehouse-mirror/db.js');
initMirrorDB();

const { ensureSchema } = await import('./db.js');
const { importTrackingCsv } = await import('./service.js');
const db = ensureSchema();

let passed = 0;
const t = (name, fn) => { fn(); passed++; console.log(`  ok: ${name}`); };

const HEADER = ['発送予定日', 'お客様側管理番号', 'お問い合わせ番号', 'お届け先 郵便番号',
  'お届け先 住所', 'お届け先 住所２', 'お届け先 住所３', 'お届け先 名称', 'お届け先 電話番号'];

// 旧: ゆうプリR 出荷履歴 (Shift-JIS / 引用符なし / 1列目に発送予定日)
function buildLegacyCsv(rows) {
  const lines = [HEADER.join(',')];
  for (const r of rows) {
    lines.push(['20260917', r.ne, r.track, '5640000', '大阪府吹田市', '1-2-3', '', 'テスト 太郎', '0600000000'].join(','));
  }
  return iconv.encode(lines.join('\r\n') + '\r\n', 'cp932');
}

// 新: ゆうプリクラウド 送り状データダウンロード (UTF-8 BOM付き / 全項目ダブルクォート / 1列目は空・住所2,3も常に空)
function buildCloudCsv(rows) {
  const q = (v) => `"${String(v)}"`;
  const lines = [HEADER.map(q).join(',')];
  for (const r of rows) {
    lines.push(['', r.ne, r.track, '5640000', '大阪府吹田市1-2-3', '', '', 'テスト 太郎', '0600000000'].map(q).join(','));
  }
  return Buffer.concat([Buffer.from([0xEF, 0xBB, 0xBF]), Buffer.from(lines.join('\r\n') + '\r\n', 'utf8')]);
}

// 伝票を仕込む (出力済みで追跡番号待ちの状態)。配送方法は「出力時にゆうパケパフだった」で揃える。
function seed(neList, { method = 'yupacketpuff' } = {}) {
  const ins = db.prepare(`INSERT OR REPLACE INTO pd_shipment_tracking
    (ne_uketsuke_no, shipping_method_code, shop_name, order_no, sync_status, method_at_export, added_at)
    VALUES (?,?,?,?, 'pending', ?, ?)`);
  for (const ne of neList) ins.run(ne, method, 'テスト店', `ORDER-${ne}`, method, '2026-09-19T00:00:00Z');
}
const trackingOf = (ne) => db.prepare(`SELECT tracking_no, tracking_source, shipping_method_code, sync_status
  FROM pd_shipment_tracking WHERE ne_uketsuke_no=?`).get(ne);

// ─── 本題: クラウド形式がそのまま通る ───

t('クラウド形式 (UTF-8 BOM / 全項目クォート / 1列目が空) を取り込んで追跡番号が紐付く', () => {
  seed(['1000001', '1000002']);
  const res = importTrackingCsv({
    source: 'yupacketpuff',
    buffer: buildCloudCsv([{ ne: '1000001', track: '123456789012' }, { ne: '1000002', track: '123456789013' }]),
    filename: '送り状データダウンロード_202609191500.csv',
  }, 'test');
  assert.equal(res.row_count, 2);
  assert.equal(res.matched_count, 2, '2件とも紐付く');
  assert.equal(res.unmatched_count, 0);
  assert.equal(res.conflict_count, 0);
  assert.equal(trackingOf('1000001').tracking_no, '123456789012');
  assert.equal(trackingOf('1000001').tracking_source, 'yupacketpuff');
  assert.equal(trackingOf('1000002').tracking_no, '123456789013');
});

t('1列目 (発送予定日) が空でも行は捨てられない — 旧形式と同じ結果になる', () => {
  seed(['2000001']);
  const cloud = importTrackingCsv({ source: 'yupacketpuff',
    buffer: buildCloudCsv([{ ne: '2000001', track: '223456789012' }]), filename: 'cloud.csv' }, 'test');
  const afterCloud = trackingOf('2000001');

  seed(['2000002']);
  const legacy = importTrackingCsv({ source: 'yupacketpuff',
    buffer: buildLegacyCsv([{ ne: '2000002', track: '223456789013' }]), filename: 'legacy.csv' }, 'test');
  const afterLegacy = trackingOf('2000002');

  assert.deepEqual(
    { row: cloud.row_count, matched: cloud.matched_count, unmatched: cloud.unmatched_count },
    { row: legacy.row_count, matched: legacy.matched_count, unmatched: legacy.unmatched_count },
    '件数の内訳が旧形式と一致する');
  assert.equal(afterCloud.tracking_source, afterLegacy.tracking_source);
  assert.equal(afterCloud.sync_status, afterLegacy.sync_status);
  assert.equal(afterCloud.tracking_no, '223456789012');
  assert.equal(afterLegacy.tracking_no, '223456789013');
});

t('BOM がヘッダ1列目に混入して「お客様側管理番号が無い」にならない', () => {
  seed(['3000001']);
  const buf = buildCloudCsv([{ ne: '3000001', track: '323456789012' }]);
  assert.deepEqual([...buf.subarray(0, 3)], [0xEF, 0xBB, 0xBF], '前提: BOM 付きで作っている');
  const res = importTrackingCsv({ source: 'yupacketpuff', buffer: buf, filename: 'bom.csv' }, 'test');
  assert.equal(res.matched_count, 1);
});

t('BOM なしの UTF-8 (全項目クォート) でも通る', () => {
  seed(['3100001']);
  const withBom = buildCloudCsv([{ ne: '3100001', track: '331456789012' }]);
  const res = importTrackingCsv({ source: 'yupacketpuff', buffer: withBom.subarray(3), filename: 'nobom.csv' }, 'test');
  assert.equal(res.matched_count, 1);
  assert.equal(trackingOf('3100001').tracking_no, '331456789012');
});

t('クラウド形式でも配送方法の矯正が効く (出力時ネコポス → 実際はゆうパケパフ)', () => {
  seed(['4000001'], { method: 'nekopos' });
  const res = importTrackingCsv({ source: 'yupacketpuff',
    buffer: buildCloudCsv([{ ne: '4000001', track: '423456789012' }]), filename: 'method.csv' }, 'test');
  assert.equal(res.matched_count, 1);
  assert.equal(res.method_corrected_count, 1, '実キャリア (CSV) に合わせて配送方法を書き換える');
  assert.equal(res.method_change_count, 1, '配送方法変更ログに載る');
  assert.equal(trackingOf('4000001').shipping_method_code, 'yupacketpuff');
});

t('クラウド形式でも既存の追跡番号と食い違えば競合として止まる (黙って上書きしない)', () => {
  seed(['5000001']);
  importTrackingCsv({ source: 'yupacketpuff',
    buffer: buildCloudCsv([{ ne: '5000001', track: '523456789012' }]), filename: 'c1.csv' }, 'test');
  const res = importTrackingCsv({ source: 'yupacketpuff',
    buffer: buildCloudCsv([{ ne: '5000001', track: '523456789099' }]), filename: 'c2.csv' }, 'test');
  assert.equal(res.conflict_count, 1);
  assert.equal(trackingOf('5000001').tracking_no, '523456789012', '既存の番号は書き換えない');
  assert.equal(trackingOf('5000001').sync_status, 'conflict');
});

t('同じクラウドファイルの二重取込は今までどおり検出される', () => {
  seed(['6000001']);
  const buf = buildCloudCsv([{ ne: '6000001', track: '623456789012' }]);
  importTrackingCsv({ source: 'yupacketpuff', buffer: buf, filename: '送り状データダウンロード_202609191500.csv' }, 'test');
  const again = importTrackingCsv({ source: 'yupacketpuff', buffer: buf, filename: '送り状データダウンロード_202609191500.csv' }, 'test');
  assert.equal(again.duplicate, true);
});

t('ヘッダが想定と違えば黙って0件成功にせずエラーにする', () => {
  const broken = Buffer.from('﻿"日付","伝票","番号"\r\n"","1000001","123456789012"\r\n', 'utf8');
  assert.throws(() => importTrackingCsv({ source: 'yupacketpuff', buffer: broken, filename: 'broken.csv' }, 'test'),
    /解析に失敗/);
});

console.log(`\n✅ ${passed} tests passed`);
