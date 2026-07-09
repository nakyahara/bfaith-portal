/**
 * 楽天RPP レポート自動ダウンロード (mall-csv-fetcher P1)
 *
 * 毎晩2レポートを取得して incoming/ に置く (取込は import-rakuten-ads-rpp.js が行う):
 *   1. 商品別 × 月ごと (全商品レポートDL)     → fact_rakuten_ads_rpp (月次×SKU)
 *   2. すべての広告 × 日ごと (この条件でDL)   → fact_rakuten_ads_rpp_daily (日次合計)
 *
 * 対象月 = 今月+先月 (月初3日までは前々月も)。⭐実測: 期間は「1ヶ月以内」制約があるため
 * 月ごとに1リクエストずつDLする。過去分は変動する (不正クリック控除、720h遡及) ため
 * 毎回同月を取り直して UPSERT。
 * ⚠️「全期間で表示」は指定日付範囲の1バケツ集計であり all-time ではない (実測 2026-07-09)。
 * ⚠️「月ごとに表示」選択時は期間入力が YYYY-MM の月入力に変わる (placeholder=Select start/end)。
 *
 * ⭐業務仕様: 広告は「5のつく日」等スポットのみ出稿 → 期間にデータが無いことがあり得る。
 *   空=正常終了扱い (障害アラートにしない。report_fetch_log に status='empty' で記録)。
 *
 * Codex高対応:
 *   ①今回リクエストしたレポートだけを識別してDL (履歴の先頭行スナップショット比較 —
 *     リクエスト前の先頭行と異なる新しい行が「完了」になるのを待つ。古いZIP誤取得防止)
 *   ②フォーム検証 (ラジオ・日付を設定後に実際の状態を読み戻して確認。違う条件のデータを
 *     黙って取らない)
 *   ③.env fail-fast (assertLoginEnv)
 *
 * 出力:
 *   - downloads/ に常時保存 (デバッグ/手動確認用)
 *   - WAREHOUSE_DATA_DIR (または DATA_DIR) 設定時: <DATA_DIR>/incoming/rakuten-ads/ へコピー
 *     + warehouse.db report_fetch_log に記録 (fail-soft: DB不調でもDL自体は成功扱い)
 *
 * 実行: node scripts/mall-csv-fetcher/rakuten-rpp-download.mjs
 *   env: HEADLESS=1 で headless / RPP_REPORTS=item,daily で対象絞り込み (デフォルト両方)
 */

import { mkdir, copyFile, readFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { dirname, join, basename } from 'node:path';
import { openContext, ensureRmsLogin, tryClick, dumpControls, safeHost, assertLoginEnv } from './lib-rakuten-login.mjs';
import { initRunLog, sendGChat, buildErrorReport } from './lib-notify.mjs';

const __dirname = dirname(fileURLToPath(import.meta.url));
const DL_DIR = join(__dirname, 'downloads');
const OUT_DIR = join(__dirname, 'spike-output');

const REPORTS_URL = 'https://ad.rms.rakuten.co.jp/rpp/reports';
const HISTORY_URL = 'https://ad.rms.rakuten.co.jp/rpp/download'; // 実測: ダウンロード履歴

const WAREHOUSE_DATA_DIR = (process.env.WAREHOUSE_DATA_DIR || process.env.DATA_DIR || '').trim();
const INCOMING_DIR = WAREHOUSE_DATA_DIR ? join(WAREHOUSE_DATA_DIR, 'incoming', 'rakuten-ads') : null;

// ─── 対象月 (JST): 今月+先月 (月初3日までは前々月も)。720h遡及+月替わりカバー ───
// ⭐実測 (2026-07-09): RPPレポートは「※期間は1ヶ月以内に絞ってください」の制約があり
// 複数月を1回でDLできない → 月ごとに1リクエストずつループする。
// 「月ごとに表示」選択時は日付欄が YYYY-MM の月入力に変わる (placeholder=Select start/end)。
function jstNow() { return new Date(Date.now() + 9 * 3600 * 1000); }
function computeTargetMonths() {
  const now = jstNow();
  const y = now.getUTCFullYear(), m = now.getUTCMonth(), d = now.getUTCDate();
  const yesterday = new Date(Date.UTC(y, m, d - 1)); // 集計は昨日まで
  const iso = (dt) => dt.toISOString().slice(0, 10);
  const backs = d <= 3 ? [2, 1, 0] : [1, 0]; // 古い月から順に
  const months = [];
  for (const back of backs) {
    const first = new Date(Date.UTC(y, m - back, 1));
    if (first > yesterday) continue; // 月初1日実行時の「今月」はデータなし → スキップ
    const last = new Date(Date.UTC(y, m - back + 1, 0));
    months.push({
      ym: iso(first).slice(0, 7),
      from: iso(first),
      to: iso(last < yesterday ? last : yesterday),
    });
  }
  return months;
}

async function snap(page, label) {
  await page.screenshot({ path: join(OUT_DIR, `rpp_${label}.png`), fullPage: true }).catch(() => {});
  console.log(`  [snap] rpp_${label}  url=${page.url()}`);
}

/** ラジオをIDで確実に選択 (check→ダメならラベル相当をクリック) */
async function checkRadio(page, idSels, labels) {
  for (const idSel of idSels) {
    const el = page.locator(idSel).first();
    try {
      if (await el.count()) {
        await el.check({ timeout: 5000, force: true });
        console.log(`  [radio] ${labels[0]}: ${idSel} を選択`);
        return true;
      }
    } catch { /* 次候補 */ }
  }
  for (const label of labels) {
    if (await tryClick(page, [`label:has-text("${label}")`, `text="${label}"`], `${label}(label)`, 4000)) return true;
  }
  return false;
}

/** ラジオが実際に選択されているか読み戻し検証 (Codex高②) */
async function verifyRadioChecked(page, idSels) {
  return page.evaluate((sels) => {
    for (const s of sels) {
      const el = document.querySelector(s);
      if (el && el.checked) return s;
    }
    return null;
  }, idSels).catch(() => null);
}

/**
 * 期間入力欄に値をセット (Codex高②: 設定後に読み戻して検証)。
 * 実測DOM (2026-07-09): 日付欄は id/name 無しの text input で placeholder="Select start" /
 * "Select end" のみ。「月ごとに表示」選択時は値が YYYY-MM (月単位)、
 * 「日ごと/全期間」時は YYYY-MM-DD。区切りは '-'。
 * startVal/endVal は呼び出し側が粒度に合わせて渡す (YYYY-MM or YYYY-MM-DD)。
 */
async function setPeriodInputs(page, startVal, endVal) {
  const info = await page.evaluate(() => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const isDateish = (v) => /^\d{4}[/-]\d{1,2}([/-]\d{1,2})?$/.test((v || '').trim());
    const els = [...document.querySelectorAll('input[type="text"], input:not([type])')].filter(vis);
    // placeholder が第一候補 (実測: Select start / Select end)。値が日付/月形式のものも拾う
    const dateEls = els.filter((el) =>
      /select\s*(start|end)/i.test(el.placeholder || '')
      || isDateish(el.value)
      || /date|start|end|from|to/i.test(el.name + ' ' + el.id));
    return dateEls.slice(0, 4).map((el, i) => {
      el.setAttribute('data-autodl-date', String(i));
      return { i, placeholder: el.placeholder || '', value: el.value || '' };
    });
  }).catch(() => []);
  console.log(`  [date] 期間入力候補: ${JSON.stringify(info)}`);
  if (info.length < 2) {
    throw new Error(`FORM_VERIFY: 期間入力欄を特定できず (候補${info.length}件)。[DOM:reports-form] を確認してセレクタ追記が必要`);
  }
  // placeholder に start/end があればそれで開始/終了を割り当て (無ければ出現順)
  const startIdx = info.find((x) => /start/i.test(x.placeholder))?.i ?? 0;
  const endIdx = info.find((x) => /end/i.test(x.placeholder) && x.i !== startIdx)?.i ?? (startIdx === 0 ? 1 : 0);

  // 既存値の区切り文字に合わせる (実測は '-')
  const sep = (info[0].value || '').includes('/') ? '/' : '-';
  const fmt = (iso) => iso.split('-').join(sep);

  const fillOne = async (idx, val, label) => {
    const el = page.locator(`[data-autodl-date="${idx}"]`).first();
    await el.click({ timeout: 5000 }).catch(() => {});
    await el.fill(val, { timeout: 5000 });
    // datepicker が開いたまま残らないよう ESC + blur
    await page.keyboard.press('Escape').catch(() => {});
    await el.evaluate((e) => e.blur()).catch(() => {});
    console.log(`  [date] ${label} = ${val}`);
  };
  await fillOne(startIdx, fmt(startVal), '開始');
  await fillOne(endIdx, fmt(endVal), '終了');
  await page.waitForTimeout(800);

  // 読み戻し検証 (datepicker に上書きされていないか)
  const got = await page.evaluate(([si, ei]) => ({
    start: document.querySelector(`[data-autodl-date="${si}"]`)?.value || '',
    end: document.querySelector(`[data-autodl-date="${ei}"]`)?.value || '',
  }), [startIdx, endIdx]).catch(() => ({ start: '', end: '' }));
  const norm = (v) => v.trim().replace(/[/-]/g, '-').split('-').map((x, i) => i === 0 ? x : x.padStart(2, '0')).join('-');
  if (norm(got.start) !== norm(startVal)) {
    throw new Error(`FORM_VERIFY: 期間が設定できていません (期待 ${startVal}〜${endVal} / 実際 ${got.start}〜${got.end})`);
  }
  if (norm(got.end) !== norm(endVal)) {
    // ⭐実測 (2026-07-10 05:32 本番初回): 早朝は「昨日まで」の集計境界がまだ前日に達しておらず、
    // datepicker が終了日を選択可能上限 (例: 7/9指定→7/8) にクランプする。これはRMS側の
    // データ境界で正常 → 「同月内・開始日以降への短縮」だけ許容して続行 (毎日再取得UPSERTで
    // 翌朝埋まるため欠落しない)。それ以外の不一致は従来通りエラー
    const isClampDown = got.end
      && norm(got.end) < norm(endVal)
      && norm(got.end) >= norm(startVal)
      && norm(got.end).slice(0, 7) === norm(endVal).slice(0, 7);
    if (!isClampDown) {
      throw new Error(`FORM_VERIFY: 期間が設定できていません (期待 ${startVal}〜${endVal} / 実際 ${got.start}〜${got.end})`);
    }
    console.warn(`  [date] ⚠ 終了日がRMS側上限でクランプ: 指定 ${endVal} → 実際 ${got.end} (早朝は前日分未集計。この範囲で続行、残りは翌朝の再取得で埋まる)`);
    return { endUsed: norm(got.end) };
  }
  console.log(`  [date] 検証OK: ${got.start} 〜 ${got.end}`);
  return { endUsed: norm(got.end) };
}

/** DLボタンの状態を調べる: 'enabled' | 'disabled' | 'missing' */
async function buttonState(page, texts) {
  return page.evaluate((btnTexts) => {
    const vis = (el) => !!(el.offsetParent || el.getClientRects().length);
    const all = [...document.querySelectorAll('button, input[type=submit], input[type=button], a[role=button]')].filter(vis);
    for (const t of btnTexts) {
      const el = all.find((b) => ((b.innerText || b.value || '').trim()).includes(t));
      if (el) return el.disabled || el.getAttribute('aria-disabled') === 'true' ? 'disabled' : 'enabled';
    }
    return 'missing';
  }, texts).catch(() => 'missing');
}

/** ダウンロード履歴の先頭行テキストを取得 (今回レポ識別用スナップショット) */
async function historyFirstRowText(page) {
  await page.goto(HISTORY_URL, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(2000);
  return page.evaluate(() => {
    const tr = document.querySelector('table tbody tr') || document.querySelectorAll('table tr')[1];
    return tr ? (tr.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 200) : '';
  }).catch(() => '');
}

/** 履歴で「今回の新しい行」が完了になるのを待ってDL (Codex高①: 古いZIP誤取得防止)。
 *  新しさ (リクエスト前の先頭行と差分) に加えて、行テキストにレポート種別キーワードが
 *  含まれることを要求 (別ジョブ/別レポートの行を誤取得しない — Codex R1 High #2)。
 *  履歴行の表記が想定と違いキーワードが一致しない場合は掴まずタイムアウト → エラーで
 *  手動確認へ (RPP_LOOSE_MATCH=1 でキーワード照合を無効化できる緊急脱出口)。 */
async function collectFromHistory(page, prevFirstRow, keywords, timeoutMs = 15 * 60 * 1000) {
  const looseMatch = process.env.RPP_LOOSE_MATCH === '1';
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const row1 = await page.evaluate(() => {
      const tr = document.querySelector('table tbody tr') || document.querySelectorAll('table tr')[1];
      if (!tr) return null;
      const dl = [...tr.querySelectorAll('a')].find((a) => a.textContent.trim() === 'ダウンロード');
      return { text: (tr.innerText || '').replace(/\s+/g, ' ').trim().slice(0, 200), hasDownload: !!dl };
    }).catch(() => null);
    console.log(`  [poll] 先頭行: ${row1 ? row1.text.slice(0, 140) : '(行なし)'}`);

    const isNewRow = row1 && row1.text && row1.text !== prevFirstRow;
    if (row1 && !isNewRow) {
      console.log('  [poll] 先頭行がリクエスト前と同一 (今回の行はまだ未出現)。待機');
    }
    const typeMatches = looseMatch || (row1 && keywords.some((k) => row1.text.includes(k)));
    if (row1 && isNewRow && !typeMatches) {
      console.log(`  [poll] 先頭行が期待レポート種別 (${keywords.join('/')}) と不一致。掴まずに待機 (誤取得防止。表記が違うだけなら RPP_LOOSE_MATCH=1)`);
    }

    if (row1 && isNewRow && typeMatches && row1.hasDownload && /完了/.test(row1.text)) {
      // 先頭行の「ダウンロード」アンカーをその場で特定してマーク (href/onclick無しのJSイベント駆動)
      const info = await page.evaluate(() => {
        const tr = document.querySelector('table tbody tr') || document.querySelectorAll('table tr')[1];
        const a = tr && [...tr.querySelectorAll('a')].find((x) => x.textContent.trim() === 'ダウンロード');
        if (!a) return null;
        a.setAttribute('data-autodl', '1');
        return { href: a.href || '' };
      }).catch(() => null);

      const trySave = async (trigger) => {
        const [download] = await Promise.all([
          page.waitForEvent('download', { timeout: 90000 }),
          trigger(),
        ]);
        return download;
      };
      try {
        return await trySave(() => page.locator('[data-autodl="1"]').click({ timeout: 15000 }));
      } catch (e) {
        console.log(`  [retry] clickでDL不可 (${String(e.message).split('\n')[0]})`);
        if (info && /^https?:/.test(info.href)) {
          return await trySave(() => page.evaluate((h) => { window.location.href = h; }, info.href));
        }
        throw e;
      }
    }

    await page.waitForTimeout(20000);
    const refreshed = await tryClick(page, ['text=更新', 'button:has-text("更新")'], '更新', 4000);
    if (!refreshed) await page.reload({ waitUntil: 'domcontentloaded' }).catch(() => {});
    await page.waitForTimeout(1500);
  }
  throw new Error('HISTORY_TIMEOUT: レポート生成完了を待ちきれず (15分)。ダウンロード履歴を手動確認してください');
}

// ─── report_fetch_log (fail-soft: DB不調でもDL自体は失敗にしない) ───
async function logFetch(entry) {
  if (!WAREHOUSE_DATA_DIR) {
    console.log(`  [fetch-log] WAREHOUSE_DATA_DIR 未設定のためDB記録スキップ (${entry.report_type}: ${entry.status})`);
    return;
  }
  try {
    const { default: Database } = await import('better-sqlite3');
    const { ensureRppTables } = await import('../../apps/warehouse/rakuten-ads-rpp-lib.js');
    const dbPath = join(WAREHOUSE_DATA_DIR, 'warehouse.db');
    const db = new Database(dbPath);
    try {
      db.pragma('busy_timeout = 5000');
      ensureRppTables(db);
      db.prepare(`INSERT INTO report_fetch_log
        (fetched_at, mall, report_type, period_from, period_to, file_name, file_sha256, file_bytes, status, message)
        VALUES (?, 'rakuten', ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(new Date().toISOString(), entry.report_type, entry.period_from, entry.period_to,
             entry.file_name || null, entry.file_sha256 || null, entry.file_bytes ?? null,
             entry.status, String(entry.message || '').slice(0, 500));
    } finally {
      db.close();
    }
  } catch (e) {
    console.warn(`  ⚠ [fetch-log] DB記録失敗 (DLは成功扱い): ${e.message}`);
  }
}

/** DL結果を downloads/ に保存し、incoming/ へコピー + fetch log 記録 */
async function persistDownload(download, reportType, range) {
  // suggestedFilename は商品別/日次で同名になり得る + 再実行で同名 → 上書き事故防止のため
  // レポート種別+期間+時刻を含む一意名にする (Codex R1 High #1)。拡張子は元名から引き継ぐ
  const suggested = download.suggestedFilename() || '';
  const ext = /\.(zip|csv)$/i.exec(suggested)?.[0]?.toLowerCase() || '.zip';
  const ts = new Date().toISOString().replace(/[:.]/g, '').replace('Z', '');
  const dest = join(DL_DIR, `rpp_${reportType}_${range.from}_${range.to}_${ts}${ext}`);
  await download.saveAs(dest);
  console.log(`  ✅ ダウンロード成功: ${dest} (元名: ${suggested || '(なし)'})`);

  const buf = await readFile(dest);
  const sha256 = createHash('sha256').update(buf).digest('hex');

  if (INCOMING_DIR) {
    await mkdir(INCOMING_DIR, { recursive: true });
    const incomingDest = join(INCOMING_DIR, basename(dest));
    await copyFile(dest, incomingDest);
    console.log(`  → incoming投入: ${incomingDest}`);
  } else {
    console.log('  ⚠ WAREHOUSE_DATA_DIR 未設定のため incoming/ 投入スキップ (手動で置くか env を設定)');
  }

  await logFetch({
    report_type: reportType, period_from: range.from, period_to: range.to,
    file_name: basename(dest), file_sha256: sha256, file_bytes: buf.length,
    status: 'ok', message: '',
  });
  return dest;
}

/** レポート1種類×1ヶ月分のリクエスト〜取得。戻り値: 'ok' | 'empty'
 *  range: {ym, from, to}。期間入力は spec.periodMode に応じて月(YYYY-MM) or 日(YYYY-MM-DD) */
async function fetchOneReport(page, spec, range) {
  console.log(`\n--- ${spec.label} ${range.ym} (${range.from} 〜 ${range.to}) ---`);

  // 今回レポ識別 (Codex高①): リクエスト前に履歴の先頭行をスナップショット
  const prevFirstRow = await historyFirstRowText(page);
  console.log(`  [ident] リクエスト前の履歴先頭行: ${prevFirstRow.slice(0, 100) || '(なし)'}`);

  // レポート画面でフォーム設定
  await page.goto(REPORTS_URL, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
  await page.waitForTimeout(2000);
  if (/system_error/i.test(page.url()) || safeHost(page.url()) !== 'ad.rms.rakuten.co.jp') {
    await ensureRmsLogin(page);
    await page.goto(REPORTS_URL, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
    await page.waitForTimeout(2000);
  }
  if (safeHost(page.url()) !== 'ad.rms.rakuten.co.jp') {
    throw new Error(`RPPレポート画面に到達できず: ${page.url()}`);
  }
  await dumpControls(page, `reports-form-${spec.key}`);

  // 集計単位 + 集計期間 (Codex高②: 読み戻し検証)
  if (!(await checkRadio(page, spec.unitRadio, spec.unitLabels))) {
    throw new Error(`FORM_VERIFY: 集計単位「${spec.unitLabels[0]}」を選択できず`);
  }
  if (!(await checkRadio(page, spec.periodRadio, spec.periodLabels))) {
    throw new Error(`FORM_VERIFY: 集計期間「${spec.periodLabels[0]}」を選択できず`);
  }
  await page.waitForTimeout(1000);
  const unitOk = await verifyRadioChecked(page, spec.unitRadio);
  const periodOk = await verifyRadioChecked(page, spec.periodRadio);
  if (!unitOk || !periodOk) {
    // ID未特定でlabelクリックのみ成功したケースは検証不能 → 警告して続行はしない (誤条件DL防止)
    throw new Error(`FORM_VERIFY: ラジオ選択を確認できず (unit=${unitOk} period=${periodOk})。[DOM:reports-form-${spec.key}] のID実測が必要`);
  }

  // 期間入力 (検証込み)。「月ごと」は月入力 YYYY-MM (実測)、「日ごと」は YYYY-MM-DD。
  // 終了日がRMS側上限でクランプされた場合は実際の範囲 (endUsed) を記録に使う
  if (spec.periodMode === 'month') {
    await setPeriodInputs(page, range.ym, range.ym);
  } else {
    const { endUsed } = await setPeriodInputs(page, range.from, range.to);
    if (endUsed && endUsed !== range.to) range = { ...range, to: endUsed };
  }
  await page.waitForTimeout(1500); // ボタン活性化待ち
  await snap(page, `${spec.key}_${range.ym}_form_set`);

  // DLボタン: disabled のままなら「期間にデータなし」= 空正常 (スポット出稿仕様)
  const state = await buttonState(page, spec.dlButtonTexts);
  if (state === 'missing') {
    await dumpControls(page, `reports-nobutton-${spec.key}`);
    throw new Error(`DLボタンが見つからず: ${spec.dlButtonTexts.join('/')}`);
  }
  if (state === 'disabled') {
    // フォーム検証 (ラジオ・日付の読み戻し) は上で通過済みなので、不活性の主因は
    // 「期間にデータなし」(スポット出稿仕様) — ただし権限/生成上限等の可能性も残るため
    // 断定せず記録して正常終了 (Codex R1 Medium: empty が連続したら手動DLで確認すること)
    console.log('  [empty] DLボタンが不活性。フォーム検証済みのため「期間にデータなし」と判断 (空=正常終了)');
    if (spec.key === 'item_monthly') {
      console.warn('  ⚠ 商品別×先月〜は通常データがあるはず。翌日も empty なら権限/生成上限/画面変更を疑い手動DLで確認');
    }
    await logFetch({
      report_type: spec.reportType, period_from: range.from, period_to: range.to,
      status: 'empty', message: 'DLボタン不活性 (期間にデータなし想定。連続する場合は権限/生成上限も疑う)',
    });
    return 'empty';
  }

  // 「この条件でDL」が同期DL (即ファイル) の可能性に備え、クリック前から download イベントを
  // 待ち受ける (クリック後に waitForEvent すると同期DLを取り逃がす — Codex R1 High #2)。
  const directDownloadPromise = page.waitForEvent('download', { timeout: 25000 }).catch(() => null);
  const clicked = await tryClick(page, spec.dlButtonTexts.map((t) => `text=${t}`), spec.label + ' DL', 10000);
  if (!clicked) throw new Error(`DLボタンを押せず: ${spec.dlButtonTexts.join('/')}`);

  let download = await directDownloadPromise;
  if (download) {
    console.log('  [direct] 同期ダウンロードを検出');
  } else {
    // 非同期生成 → ダウンロード履歴で「今回の行」(新規 + 種別一致) の完了を待つ
    await snap(page, `${spec.key}_after_request`);
    console.log('[history] ダウンロード履歴で生成完了を待つ');
    await page.goto(HISTORY_URL, { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(() => {});
    await page.waitForTimeout(2000);
    download = await collectFromHistory(page, prevFirstRow, spec.historyKeywords);
  }

  await persistDownload(download, spec.reportType, range);
  return 'ok';
}

// ─── レポート定義 ───
// ラジオIDは全て実測済み (2026-07-09 [DOM:reports-form] ダンプ):
//   集計単位: rdReportTypeAllAds / rdReportTypeCampaign / rdReportTypeItem / rdReportTypeKeyword
//   集計期間: rdPeriodAll / rdPeriodMonth / rdPeriodDay
const REPORT_SPECS = [
  {
    key: 'item_monthly',
    label: '商品別 × 月ごと (全商品レポート)',
    reportType: 'rpp_item_monthly',
    periodMode: 'month', // 期間入力は YYYY-MM (「月ごと」選択で月入力に変わる、実測)
    unitRadio: ['#rdReportTypeItem'],
    unitLabels: ['商品別'],
    periodRadio: ['#rdPeriodMonth'],
    periodLabels: ['月ごとに表示', '月ごと'],
    dlButtonTexts: ['全商品レポートダウンロード', '全商品レポートDL'],
    historyKeywords: ['商品'],
  },
  {
    key: 'all_daily',
    label: 'すべての広告 × 日ごと (日次広告費)',
    reportType: 'rpp_all_daily',
    periodMode: 'day',
    unitRadio: ['#rdReportTypeAllAds'],
    unitLabels: ['すべての広告', 'すべて'],
    periodRadio: ['#rdPeriodDay'],
    periodLabels: ['日ごとに表示', '日ごと'],
    dlButtonTexts: ['この条件でダウンロード', 'この条件でDL'],
    // 履歴行の実表記は未実測。一致しない場合は poll ログの行テキストを見て追記
    // (それまでは RPP_LOOSE_MATCH=1 で回避可)
    historyKeywords: ['すべて'],
  },
];

async function main() {
  const runLog = initRunLog('rakuten-rpp'); // console出力をlogs/へtee (secretマスク付き)
  // Codex高③: .env fail-fast (ブラウザを開く前に検知)。webhook設定済みなら通知してから落ちる
  try { assertLoginEnv(); } catch (e) {
    console.error(`⚠️ ${e.message}`);
    await sendGChat(buildErrorReport({
      mall: 'rakuten', logPath: runLog.logPath,
      failures: [{ reportType: 'rpp(起動前)', error: e.message }],
      repro: 'scripts/mall-csv-fetcher/.env を確認 (記入は中原さん)',
    }), 'rakuten-rpp');
    // fetch直後の process.exit() は Windows で libuv assertion crash を起こし
    // 終了コードが化ける (実機 2026-07-09) → exitCode で自然終了させる
    process.exitCode = 2;
    return;
  }
  await mkdir(DL_DIR, { recursive: true });
  await mkdir(OUT_DIR, { recursive: true });

  const targets = (process.env.RPP_REPORTS || 'item,daily').split(',').map((s) => s.trim());
  const specs = REPORT_SPECS.filter((s) =>
    (s.key === 'item_monthly' && targets.includes('item')) || (s.key === 'all_daily' && targets.includes('daily')));
  if (specs.length === 0) { console.error('FATAL: RPP_REPORTS に item / daily を指定してください'); process.exitCode = 2; return; }

  const months = computeTargetMonths();
  const context = await openContext();
  const page = context.pages()[0] || await context.newPage();
  page.on('dialog', (d) => { console.log(`  [dialog] ${d.message()}`); d.accept().catch(() => {}); });

  const outcomes = [];
  try {
    console.log('=== 楽天RPP レポート自動DL ===');
    console.log(`対象: ${specs.map((s) => s.label).join(' / ')}`);
    console.log(`対象月: ${months.map((m) => m.ym).join(', ')} (※期間は1ヶ月以内制約のため月ごとにDL)`);
    await ensureRmsLogin(page);

    const failures = [];
    for (const spec of specs) {
      for (const month of months) {
        try {
          const status = await fetchOneReport(page, spec, month);
          outcomes.push({ spec: spec.key, ym: month.ym, status });
        } catch (e) {
          // 1件の失敗で全体を止めない: 同レポートの残月のみスキップし、別レポートは続行
          console.error(`✗ ${spec.label} ${month.ym}: ${e.message}`);
          const screenshot = join(OUT_DIR, `rpp_${spec.key}_${month.ym}_error.png`);
          await snap(page, `${spec.key}_${month.ym}_error`);
          await logFetch({
            report_type: spec.reportType, period_from: month.from, period_to: month.to,
            status: 'error', message: e.message,
          });
          outcomes.push({ spec: spec.key, ym: month.ym, status: 'error', error: e.message });
          failures.push({
            reportType: spec.reportType, ym: month.ym, from: month.from, to: month.to,
            error: e.message, url: page.url(), screenshot,
          });
          if (String(e.message).startsWith('2FA_REQUIRED')) throw e; // ログイン切れは全レポート共倒れ → 即通知へ
          break; // 同レポートの他の月も同じ原因で落ちる可能性大 → 無駄打ちしない (別レポートは試す)
        }
      }
    }

    const failed = outcomes.filter((o) => o.status === 'error');
    console.log(`\n=== summary: ${outcomes.map((o) => `${o.spec}:${o.ym}=${o.status}`).join(' / ')} ===`);
    if (failed.length > 0) {
      console.log('  → 失敗分は手動DLで埋められます (取込は incoming/ に置くだけ。大原則の手動フォールバック)');
      await sendGChat(buildErrorReport({
        mall: 'rakuten', outcomes, failures, logPath: runLog.logPath,
        repro: `cd ${process.cwd()}; $env:RPP_REPORTS='${targets.join(',')}'; node scripts/mall-csv-fetcher/rakuten-rpp-download.mjs`,
      }), 'rakuten-rpp');
      process.exitCode = 1;
    } else {
      console.log('  → 取込は次回 daily-sync (import-rakuten-ads-rpp.js) が実行');
    }
  } catch (err) {
    const is2fa = String(err.message).startsWith('2FA_REQUIRED');
    if (is2fa) {
      console.error(`\n⚠️ ${err.message}`);
      console.error('  → その日は手動DLで埋める (大原則の手動フォールバック)');
    } else {
      console.error('[ERROR]', err.message);
    }
    await snap(page, 'error');
    await sendGChat(buildErrorReport({
      mall: 'rakuten', outcomes, logPath: runLog.logPath,
      failures: [{
        reportType: is2fa ? '2FA_REQUIRED (全レポート停止)' : 'rpp(共通処理)',
        error: err.message, url: page.url(), screenshot: join(OUT_DIR, 'rpp_error.png'),
      }],
      repro: is2fa
        ? 'miniPCで $env:MANUAL=1; node scripts/mall-csv-fetcher/rakuten-login-spike.mjs → 手動ログイン+「信頼できる端末」登録 (14日ごと)'
        : 'node scripts/mall-csv-fetcher/rakuten-rpp-download.mjs',
    }), 'rakuten-rpp');
    process.exitCode = 1;
  } finally {
    if (process.env.HEADLESS !== '1') {
      console.log('\n目視用にブラウザを120秒開いたままにします。Ctrl+Cで終了可。');
      await page.waitForTimeout(120000).catch(() => {});
    }
    await context.close().catch(() => {});
  }
}

main();
