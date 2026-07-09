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
const HISTORY_URL = 'https://ad.rms.rakuten.co.jp/rpp/download'; // 実測: ダウンロード履歴

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `rpp_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] rpp_${label}  url=${page.url()}`);
}

/** ラジオをIDで確実に選択 (check→ダメならラベル相当をクリック) */
async function checkRadio(page, idSel, label) {
  const el = page.locator(idSel).first();
  try {
    if (await el.count()) {
      await el.check({ timeout: 5000, force: true });
      console.log(`  [radio] ${label}: ${idSel} を選択`);
      return true;
    }
  } catch { /* fallthrough */ }
  // フォールバック: ラベルテキストをクリック
  return tryClick(page, [`label:has-text("${label}")`, `text="${label}"`], `${label}(label)`, 5000);
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
    // まずRMSに正しくログイン (mainmenu /rms 到達)
    await ensureRmsLogin(page);
    await snap(page, '0_mainmenu');

    // 新規ログイン直後は深いURL直行が通る(実証済)。system_errorなら再ログインして直行リトライ。
    const gotoReports = async () => {
      await page.goto(REPORTS_URL, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
      await page.waitForTimeout(2000);
    };
    console.log('[nav] RPPレポートへ直行');
    await gotoReports();
    if (/system_error/i.test(page.url())) {
      console.log('[nav] system_error → 再ログインして直行リトライ');
      await ensureRmsLogin(page);
      await gotoReports();
    }
    await snap(page, '0c_reports');
    console.log(`[nav] 現在地: ${page.url()}`);
    if (/system_error/i.test(page.url()) || safeHost(page.url()) !== 'ad.rms.rakuten.co.jp') {
      throw new Error(`RPPレポート画面に到達できず: ${page.url()}`);
    }

    // --- フォーム状態を診断出力 ---
    console.log('[form] RPPレポート画面のフォームを確認');
    await dumpControls(page, 'reports-form');

    // --- 集計単位=商品別 / 集計期間=全期間で表示 ---
    // ラジオIDは実測: rdReportTypeItem=商品別 / rdPeriodAll=全期間で表示。
    // (全期間は日付入力不要かつ常にデータあり。まずここでパイプラインを通す)
    await checkRadio(page, '#rdReportTypeItem', '商品別');
    await checkRadio(page, '#rdPeriodAll', '全期間で表示');
    await page.waitForTimeout(1500); // ボタン活性化待ち
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
    const wentHistory = await tryClick(page, ['text=ダウンロード履歴', 'a:has-text("ダウンロード履歴")'], 'ダウンロード履歴タブ', 6000);
    if (!wentHistory) await page.goto(HISTORY_URL, { waitUntil: 'domcontentloaded' }).catch(() => {});
    await page.waitForTimeout(2000);
    await snap(page, '3_history');

    // 先頭行(=今リクエストした最新行)が「完了」になり、テキストがちょうど「ダウンロード」の
    // リンクが出るまでポーリング (生成が長いので最大15分)。誤って見出し/タブを押さないよう厳密に。
    const deadline = Date.now() + 15 * 60 * 1000;
    let saved = null;
    while (Date.now() < deadline && !saved) {
      const row1 = await page.evaluate(() => {
        const tr = document.querySelector('table tbody tr') || document.querySelectorAll('table tr')[1];
        if (!tr) return null;
        const dl = [...tr.querySelectorAll('a')].find((a) => a.textContent.trim() === 'ダウンロード');
        return { text: (tr.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 140), hasDownload: !!dl };
      }).catch(() => null);
      console.log(`  [poll] 先頭行: ${row1 ? row1.text : '(行なし)'}`);

      if (row1 && row1.hasDownload && /完了/.test(row1.text)) {
        // 先頭行の「ダウンロード」アンカーをその場で特定してマーク+href/HTMLを取得
        const info = await page.evaluate(() => {
          const tr = document.querySelector('table tbody tr') || document.querySelectorAll('table tr')[1];
          const a = tr && [...tr.querySelectorAll('a')].find((x) => x.textContent.trim() === 'ダウンロード');
          if (!a) return null;
          a.setAttribute('data-autodl', '1');
          return { href: a.href || '', onclick: a.getAttribute('onclick') || '', html: a.outerHTML.slice(0, 180) };
        }).catch(() => null);
        console.log(`  [dl-anchor] ${JSON.stringify(info)}`);

        // まずマークしたアンカーを普通にクリック → ダメなら href 直接遷移でダウンロード
        const trySave = async (trigger) => {
          const [download] = await Promise.all([
            page.waitForEvent('download', { timeout: 90000 }),
            trigger(),
          ]);
          const fname = download.suggestedFilename() || `rpp_all_products_${Date.now()}.zip`;
          const dest = join(DL_DIR, fname);
          await download.saveAs(dest);
          return dest;
        };
        try {
          saved = await trySave(() => page.locator('[data-autodl="1"]').click({ timeout: 15000 }));
          console.log(`  ✅ ダウンロード成功(click): ${saved}`);
        } catch (e) {
          console.log(`  [retry] clickでDL不可 (${String(e.message).split('\n')[0]})`);
          if (info && /^https?:/.test(info.href)) {
            try {
              saved = await trySave(() => page.evaluate((h) => { window.location.href = h; }, info.href));
              console.log(`  ✅ ダウンロード成功(href遷移): ${saved}`);
            } catch (e2) {
              console.log(`  [retry] href遷移もDL不可 (${String(e2.message).split('\n')[0]})`);
            }
          }
        }
      } else if (row1 && /待機中|生成中|処理中/.test(row1.text)) {
        console.log('  [wait] 生成中 (待機中)。20秒後に更新して再確認');
      } else {
        console.log('  [wait] 完了リンク未検出。20秒後に更新して再確認');
      }

      if (!saved) {
        await page.waitForTimeout(20000);
        // 「更新」ボタンで最新状態に (無ければリロード)
        const refreshed = await tryClick(page, ['text=更新', 'button:has-text("更新")'], '更新', 4000);
        if (!refreshed) await page.reload({ waitUntil: 'domcontentloaded' }).catch(() => {});
        await page.waitForTimeout(1500);
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
