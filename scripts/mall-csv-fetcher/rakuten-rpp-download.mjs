/**
 * 楽天RPP 全商品レポート 自動ダウンロード (P1本実装の第一歩)
 *
 * 流れ:
 *   1. ログイン (lib-rakuten-login) してRPPレポート画面へ
 *   2. 集計単位=商品別 / 集計期間=全期間で表示 を選択 (全期間は常にデータありで空振りしない)
 *   3. 「全商品レポートダウンロード」を押す → 非同期でレポート生成
 *   4. 「ダウンロード履歴」で生成完了を待ち、ZIPをダウンロードして downloads/ に保存
 *
 * ⚠️まだ検証段階。フォーム/履歴のDOMを診断出力しつつ best-effort で操作する。
 *   セレクタが合わなければログの [DOM:*] を見て候補を追記する (ログインと同じ進め方)。
 *
 * ⚠️「5のつく日だけ広告」等でデータが無い期間は空になり得る → 空=正常終了扱い (障害にしない)。
 *   全期間指定なら常に全履歴が入るので空問題は回避できる。
 *
 * 実行: node scripts/mall-csv-fetcher/rakuten-rpp-download.mjs
 */

import { mkdir } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import { openContext, ensureRmsLogin, tryClick, dumpControls, dumpLinks, safeHost } from './lib-rakuten-login.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DL_DIR = join(__dirname, 'downloads');
const OUT_DIR = join(__dirname, 'spike-output');

const REPORTS_URL = 'https://ad.rms.rakuten.co.jp/rpp/reports';
const HISTORY_URL = 'https://ad.rms.rakuten.co.jp/rpp/reports/history'; // 推定。違えば画面のタブから遷移

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `rpp_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] rpp_${label}  url=${page.url()}`);
}

/** ラベル文字列でラジオ/チェックを選ぶ (Rakutenのラジオはラベルクリックで反応することが多い) */
async function pickByLabel(page, text, label) {
  const cands = [
    `label:has-text("${text}")`,
    `text="${text}"`,
    `//label[contains(., "${text}")]`,
  ];
  return tryClick(page, cands, `${label}=${text}`, 8000);
}

async function main() {
  await mkdir(DL_DIR, { recursive: true });
  await mkdir(OUT_DIR, { recursive: true });

  const context = await openContext();
  const page = context.pages()[0] || await context.newPage();

  // ダイアログ(確認/alert)が出たら承認して進める
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

  try {
    console.log('=== 楽天RPP 全商品レポート 自動DL ===');
    // ⚠️RPP深いURLへの直行はsystem_errorになる。RMSメインメニューから遷移してセッション確立が必要。
    await ensureRmsLogin(page);
    await snap(page, '0_mainmenu');
    await dumpLinks(page, 'mainmenu');

    // RPPへ: メインメニューから「検索連動型広告(RPP)」等のリンク/メニューをたどる。
    // 正確な導線はLINKSダンプで確認して調整。まずは候補を best-effort でクリック。
    console.log('[nav] RPPプロモーションメニューへ遷移を試行');
    const toRpp = await tryClick(page, [
      'a:has-text("RPP")', 'a:has-text("検索連動型広告")',
      'a:has-text("プロモーション")', 'a:has-text("広告")',
    ], 'RPPメニュー', 8000);
    if (toRpp) {
      await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
      await snap(page, '0b_after_rpp_nav');
      await dumpLinks(page, 'rpp-landing');
      // パフォーマンスレポートへ
      await tryClick(page, [
        'a:has-text("パフォーマンスレポート")', 'text=パフォーマンスレポート',
      ], 'パフォーマンスレポート', 8000);
      await page.waitForLoadState('networkidle', { timeout: 20000 }).catch(() => {});
    }
    await snap(page, '0c_reports');
    console.log(`[nav] 現在地: ${page.url()}`);

    // --- フォーム状態を診断出力 ---
    console.log('[form] RPPレポート画面のフォームを確認');
    await dumpControls(page, 'reports-form');

    // --- 集計単位=商品別 / 集計期間=全期間で表示 ---
    // (全期間は日付入力不要かつ常にデータあり。まずここでパイプラインを通す)
    await pickByLabel(page, '商品別', '集計単位');
    await pickByLabel(page, '全期間で表示', '集計期間');
    await page.waitForTimeout(1000); // ボタン活性化待ち
    await snap(page, '1_form_set');
    await dumpControls(page, 'reports-after-set'); // ボタンのdisabledが外れたか確認

    // --- 全商品レポートダウンロードを押す (非同期生成) ---
    const clicked = await tryClick(page, [
      'button:has-text("全商品レポートダウンロード")',
      'text=全商品レポートダウンロード',
      'input[value="全商品レポートダウンロード"]',
    ], '全商品レポートDL', 10000);
    if (!clicked) {
      console.warn('  [warn] DLボタンを押せず。上の [DOM:reports-after-set] のボタン状態(disabled)を確認して条件を追加すること');
    }
    await page.waitForTimeout(3000);
    await snap(page, '2_after_request');

    // --- ダウンロード履歴で生成完了を待ってZIP取得 ---
    console.log('[history] ダウンロード履歴で生成完了を待つ');
    // タブ「ダウンロード履歴」をクリック (URL直行が効かない場合に備え両方試す)
    const wentHistory = await tryClick(page, ['text=ダウンロード履歴', 'a:has-text("ダウンロード履歴")'], 'ダウンロード履歴タブ', 6000);
    if (!wentHistory) await page.goto(HISTORY_URL, { waitUntil: 'domcontentloaded' }).catch(() => {});
    await page.waitForTimeout(1500);
    await snap(page, '3_history');
    await dumpControls(page, 'history');

    // 生成完了をポーリング (最大5分)。完了行のダウンロードリンクを押してZIP取得。
    const deadline = Date.now() + 5 * 60 * 1000;
    let saved = null;
    while (Date.now() < deadline && !saved) {
      // 最新行の「ダウンロード」リンク/ボタンを探す
      const dlLink = page.locator('a:has-text("ダウンロード"), button:has-text("ダウンロード")').first();
      const ready = await dlLink.isVisible().catch(() => false);
      if (ready) {
        try {
          const [download] = await Promise.all([
            page.waitForEvent('download', { timeout: 60000 }),
            dlLink.click(),
          ]);
          const fname = download.suggestedFilename() || `rpp_all_products_${Date.now()}.zip`;
          const dest = join(DL_DIR, fname);
          await download.saveAs(dest);
          saved = dest;
          console.log(`  ✅ ダウンロード成功: ${dest}`);
        } catch (e) {
          console.log(`  [retry] DLリンク押下でZIP取得できず (${e.message})。生成中の可能性→待機`);
        }
      } else {
        console.log('  [wait] まだ生成中 (ダウンロードリンク未出現)。30秒後に再確認');
      }
      if (!saved) {
        await page.waitForTimeout(30000);
        await page.reload({ waitUntil: 'domcontentloaded' }).catch(() => {});
      }
    }

    await snap(page, '4_final');
    if (saved) {
      console.log(`\n✅ 完了: ${saved}`);
      console.log('  → 次: このZIPをRender取込APIへPOSTする処理を追加 (P1後半)');
    } else {
      console.log('\n❓ ZIP未取得。spike-output の rpp_*.png と [DOM:history] を確認してセレクタ調整が必要');
    }
  } catch (err) {
    if (String(err.message).startsWith('2FA_REQUIRED')) {
      console.error(`\n⚠️ ${err.message}`);
      console.error('  → 本番ではここでGChat通知を出し、その日は手動DLで埋める (大原則の手動フォールバック)');
    } else {
      console.error('[ERROR]', err.message);
    }
    await snap(page, 'error');
    process.exitCode = 1;
  } finally {
    if (process.env.HEADLESS !== '1') {
      console.log('\n目視用にブラウザを120秒開いたままにします。Ctrl+Cで終了可。');
      await page.waitForTimeout(120000).catch(() => {});
    }
    await context.close();
  }
}

main();
