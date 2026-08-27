/**
 * test-reprint-manifest.mjs — 送り状自動印刷 P0/P1 の検証 (2026-08-27)
 *   - verifyManifest: 「出力ページ→注文番号」の対応表を信用してよいかの判定 (fail-closed)
 *   - pageHasContent: 抜き出したページが明白に空でないかの構造チェック
 * 実行: node apps/packing/tests/test-reprint-manifest.mjs
 */
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { PDFDocument, StandardFonts } from 'pdf-lib';

const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'packing-manifest-test-'));
process.env.DATA_DIR = tmpDir;

const { verifyManifest, pageHasContent, isSorterFailureText, MIN_INK_RATIO } = await import('../reprint-pdf.js');

let failed = 0;
const ok = (cond, label) => { console.log(`${cond ? '✅' : '❌'} ${label}`); if (!cond) failed++; };
const eq = (a, b, label) => ok(JSON.stringify(a) === JSON.stringify(b), `${label} (期待 ${JSON.stringify(b)} / 実際 ${JSON.stringify(a)})`);

const SHA = 'a'.repeat(64);
const ORDER = '249-4104623-8581459';
const INVOICES = [{ name: '納品書_20.pdf', modified_time: '2026-08-27T00:31:00.000Z' }];
const baseManifest = (over = {}) => ({
  version: 1,
  generated_at: '2026-08-27T09:41:00+09:00',
  folder_name: '出荷_20',
  invoice_files: INVOICES.map((f) => ({ name: f.name, modified_time: f.modified_time })),
  output_pdf: 'AES送り状_並び替え済.pdf',
  output_pdf_sha256: SHA,
  page_count: 3,
  pages: [
    { page: 1, order_number: '249-1111111-1111111', shipping_number: 'DA1', ink_ratio: 0.12 },
    { page: 2, order_number: ORDER, shipping_number: 'DA2', ink_ratio: 0.134 },
    { page: 3, order_number: '249-3333333-3333333', shipping_number: 'DA3', ink_ratio: 0.09 },
  ],
  unmatched_orders: [],
  ...over,
});
const ctx = (over = {}) => ({
  folderName: '出荷_20', siteOrderNo: ORDER, pdfSha256: SHA,
  pdfPageCount: 3, invoiceFiles: INVOICES, ...over,
});

console.log('── verifyManifest: 通る場合 ──');
{
  const v = verifyManifest(baseManifest(), ctx());
  eq({ ok: v.ok, page: v.page }, { ok: true, page: 1 }, '注文番号の完全一致で 2ページ目 (0始まり=1)');
  const v2 = verifyManifest(baseManifest(), ctx({ siteOrderNo: ` ${ORDER} ` }));
  eq(v2.ok, true, '前後の空白は無視して一致');
}

console.log('\n── verifyManifest: 止める場合 (fail-closed) ──');
{
  const reason = (m, c) => verifyManifest(m, c ?? ctx()).reason;
  ok(verifyManifest(null, ctx()).ok === false, 'manifestなし');
  ok(/失効/.test(reason(baseManifest({ invalid: true }))), '失効manifest (並び替えが失敗した回)');
  ok(/version/.test(reason(baseManifest({ version: 2 }))), '未知のversion');
  ok(/一致しません/.test(reason(baseManifest(), ctx({ pdfSha256: 'b'.repeat(64) }))),
    'PDFのsha256不一致 (作り直された)');
  ok(/出荷フォルダ/.test(reason(baseManifest(), ctx({ folderName: '出荷_99' }))),
    '別の出荷フォルダのmanifest');
  ok(/ありません/.test(reason(baseManifest(), ctx({ siteOrderNo: '249-0000000-0000000' }))),
    '注文番号がmanifestに無い');
  ok(/2件/.test(reason(baseManifest({
    pages: [
      { page: 1, order_number: ORDER, ink_ratio: 0.1 },
      { page: 2, order_number: ORDER, ink_ratio: 0.1 },
    ],
    page_count: 2,
  }), ctx({ pdfPageCount: 2 }))), '同じ注文番号が複数ページ (どちらか確定できない)');
  ok(/合いません/.test(reason(baseManifest({
    pages: [{ page: 1, order_number: ORDER, ink_ratio: 0.1 }], page_count: 3,
  }))), 'pagesの件数がpage_countと合わない');
  ok(/範囲外/.test(reason(baseManifest({
    pages: [{ page: 9, order_number: ORDER, ink_ratio: 0.1 }], page_count: 1,
  }), ctx({ pdfPageCount: 1 }))), 'ページ番号が範囲外');
  ok(/重複/.test(reason(baseManifest({
    pages: [
      { page: 1, order_number: ORDER, ink_ratio: 0.1 },
      { page: 1, order_number: '249-3333333-3333333', ink_ratio: 0.1 },
    ], page_count: 2,
  }), ctx({ pdfPageCount: 2 }))), 'ページ番号の重複');
  ok(/PDFのページ数/.test(reason(baseManifest(), ctx({ pdfPageCount: 5 }))),
    '送り状PDFの実ページ数と合わない');
  // 欠けたメタは「スキップ」ではなく不合格 (Codexレビュー指摘3)
  ok(/出荷フォルダ名/.test(reason(baseManifest({ folder_name: undefined }))), 'folder_nameなし');
  ok(/生成時刻/.test(reason(baseManifest({ generated_at: 'ぐちゃぐちゃ' }))), 'generated_atが不正');
  ok(/納品書の記録/.test(reason(baseManifest({ invoice_files: [] }))), 'manifestに納品書の記録なし');
  ok(/今の納品書/.test(reason(baseManifest(), ctx({
    invoiceFiles: [{ name: '納品書_20.pdf', modified_time: '2026-08-27T05:00:00.000Z' }],
  }))), '納品書が差し替わっている (作り直しが必要)');
  ok(/世代を確認/.test(reason(baseManifest(), ctx({ invoiceFiles: [] }))), '納品書が1件も無い');
  // NaN/Infinity/範囲外の ink_ratio は比較が素通りするので明示的に弾く (Codexレビュー指摘1)
  for (const v of ['abc', Infinity, -1, 2, NaN]) {
    ok(/ink_ratioが不正/.test(reason(baseManifest({
      pages: [{ page: 1, order_number: ORDER, ink_ratio: v }], page_count: 1,
    }), ctx({ pdfPageCount: 1 }))), `ink_ratio=${String(v)} は不正として弾く`);
  }
  ok(/しきい値の設定/.test(reason(baseManifest(), ctx({ minInkRatio: NaN }))), 'しきい値が不正なら止める');
  ok(/判定できません/.test(reason(baseManifest({
    pages: [{ page: 1, order_number: ORDER, ink_ratio: null }], page_count: 1,
  }), ctx({ pdfPageCount: 1 }))), 'ink_ratioなし (白紙か確かめられない)');
  // 二重失効失敗への歯止め: 失敗の記録がmanifestより新しい = 並び替えが失敗した状態
  ok(/エラーの記録/.test(reason(baseManifest(), ctx({
    manifestModifiedMs: 1000, errorTxt: { modifiedMs: 2000, isFailure: true },
  }))), '失敗の記録がmanifestより新しい');
  // 並び替えは**成功時にもエラーtxtを「解消済み」で上書きする**。時刻だけで判定すると
  // 一度でも失敗したフォルダは永久に不合格になり、新経路が一度も有効にならない
  ok(verifyManifest(baseManifest(), ctx({
    manifestModifiedMs: 1000, errorTxt: { modifiedMs: 2000, isFailure: false },
  })).ok, '解消済みの記録は新しくても通す (成功時に上書きされるため)');
  ok(verifyManifest(baseManifest(), ctx({
    manifestModifiedMs: 2000, errorTxt: { modifiedMs: 1000, isFailure: true },
  })).ok, '失敗の記録でも古ければ通す (その後に成功している)');
  ok(/判別できません/.test(reason(baseManifest(), ctx({
    manifestModifiedMs: 1000, errorTxt: { modifiedMs: 2000, isFailure: null },
  }))), '内容を判別できないエラーtxtは不合格');
  ok(/更新時刻/.test(reason(baseManifest(), ctx({
    manifestModifiedMs: 1000, errorTxt: { modifiedMs: null, isFailure: true },
  }))), 'エラーtxtの更新時刻が不正なら不合格');
  // 納品書メタの欠損を空文字で埋めて一致させない
  ok(/世代を確認/.test(reason(baseManifest(), ctx({
    invoiceFiles: [{ name: '納品書_20.pdf', modified_time: 'ぐちゃぐちゃ' }],
  }))), '納品書の更新時刻が不正なら不合格');
  ok(/納品書の記録/.test(reason(baseManifest({
    invoice_files: [{ name: '', modified_time: '' }],
  }))), 'manifest側の納品書メタが空なら不合格');
  // 型を緩く受理しない
  ok(/version/.test(reason(baseManifest({ version: '1' }))), 'versionが文字列 "1" は不合格');
  ok(/page_countが不正/.test(reason(baseManifest({ page_count: '3' }))), 'page_countが文字列は不合格');
  ok(/ink_ratioが不正/.test(reason(baseManifest({
    pages: [{ page: 1, order_number: ORDER, ink_ratio: '0.5' }], page_count: 1,
  }), ctx({ pdfPageCount: 1 }))), 'ink_ratioが文字列は不合格');
  ok(/pagesが壊れて/.test(reason(baseManifest({ pages: [null], page_count: 1 }))), 'pagesにnullがあれば不合格');
  // ⭐梱包バッチの取込時刻との前後比較は使わない: 並び替えと取込のどちらが先かは運用で
  // 変わるため、常に「古い」判定になって新経路が一度も有効にならない事故になり得る
  // (Codexレビュー指摘5)。世代は「manifestが今の納品書から作られたか」で見る
}

console.log('\n── verifyManifest: 白紙 ──');
{
  const v = verifyManifest(baseManifest({
    pages: [{ page: 1, order_number: ORDER, ink_ratio: 0 }], page_count: 1,
  }), ctx({ pdfPageCount: 1 }));
  eq({ ok: v.ok, reason: v.reason }, { ok: false, reason: 'blank' }, 'ink_ratio=0 は blank');
  const v2 = verifyManifest(baseManifest({
    pages: [{ page: 1, order_number: ORDER, ink_ratio: MIN_INK_RATIO / 2 }], page_count: 1,
  }), ctx({ pdfPageCount: 1 }));
  eq(v2.reason, 'blank', 'しきい値未満も blank');
  const v3 = verifyManifest(baseManifest({
    pages: [{ page: 1, order_number: ORDER, ink_ratio: MIN_INK_RATIO }], page_count: 1,
  }), ctx({ pdfPageCount: 1 }));
  eq(v3.ok, true, 'しきい値ちょうどは通す');
}

console.log('\n── isSorterFailureText: 失敗の記録か解消済みか ──');
{
  // 並び替えツールは BOM付きUTF-8/CRLF で書く (build_error_txt / build_resolved_txt)
  eq(isSorterFailureText('﻿❌ AES送り状の並び替えに失敗しました\r\n原因:\r\n'), true, '失敗通知');
  eq(isSorterFailureText('﻿✅ 解消済み (AES送り状_並び替え済.pdf を出力しました)'), false, '解消済み');
  eq(isSorterFailureText(''), null, '空は判別不能');
  eq(isSorterFailureText('なにか別の文言'), null, '想定外の文言は判別不能');
}

console.log('\n── pageHasContent: 抜き出したページの構造チェック ──');
{
  const blank = await PDFDocument.create();
  blank.addPage([283, 425]);
  ok(pageHasContent(blank.getPage(0)) === false, '真っ白なページは中身なしと判定');

  const withText = await PDFDocument.create();
  const page = withText.addPage([283, 425]);
  page.drawText('LABEL', { size: 12, font: await withText.embedFont(StandardFonts.Helvetica) });
  ok(pageHasContent(withText.getPage(0)) === true, '文字があるページは中身あり');

  // AES送り状は画像1枚のページ — 画像XObjectを持つページも中身ありになること
  const withImage = await PDFDocument.create();
  const p2 = withImage.addPage([283, 425]);
  // 1x1 の PNG (黒)
  const png = Buffer.from(
    'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
    'base64');
  const img = await withImage.embedPng(png);
  p2.drawImage(img, { x: 0, y: 0, width: 283, height: 425 });
  ok(pageHasContent(withImage.getPage(0)) === true, '画像1枚のページは中身あり');
}

try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch { /* 無視 */ }
console.log(`\n${failed === 0 ? '✅ 全テスト PASS' : `❌ ${failed} 件失敗`}`);
process.exitCode = failed === 0 ? 0 : 1;
