/**
 * SKUマスタ → 商品コード変換テーブル 直接追記
 *
 * 用途:
 *   bfaith-portal「マスタ登録」(m_sku_master) に登録済みで、このスプレッドシートの
 *   「商品コード変換テーブル」シート (A列に SKU) にまだ載っていない SKU を、
 *   毎朝 1 回検出して **「商品コード変換テーブル」本体に直接追記** する。
 *
 * 追記時の列マッピング:
 *   A列: seller_sku
 *   C列: 商品名
 *   D列: NE商品コード
 *   E列: 数量
 *   B列, F列以降: 触らない (既存セル保護)
 *
 * セット商品 (1 SKU が複数 NE コード) は components.length 行に展開して書き込む。
 * 同じ A列 SKU が既存にあれば skip (重複防止)。
 *
 * セットアップ (Script Properties):
 *   MIRROR_API_BASE     = https://bfaith-portal.onrender.com/apps/mirror
 *   MIRROR_READ_TOKEN   = (Render env の値と同じ)
 *   SOURCE_SHEET_NAME   = 商品コード変換テーブル        (差分判定 + 追記先 を兼ねる)
 *   SOURCE_SKU_COLUMN   = 1                              (SKU が入っている列番号、A列なら 1)
 *   SOURCE_HEADER_ROWS  = 1                              (ヘッダ行数。0 なら全行を SKU として扱う)
 *   WRITE_MODE          = dry_run                        (★必須★ dry_run | live。最初は必ず dry_run)
 *
 * トリガー:
 *   関数: runDailyCheck / 時間主導型 / 日付ベース / 午前7時〜8時
 *   (miniPC daily-sync 07:00 完了後に動かす)
 *
 * 安全装置:
 *   - WRITE_MODE=dry_run のときは Logger.log でシミュレートのみ、シート無変更
 *   - LockService で多重起動防止
 *   - mirror freshness (sku_master 最終同期 26h 超え) なら throw
 *   - レスポンス契約検証 (since_days = 7、items 配列)
 *   - 重複チェック: 既に A列にある seller_sku は skip
 *   - 一度に追記する行数の上限 (ML_MAX_NEW_ROWS) を超えたら throw (誤爆防止)
 *
 * Codex review (PR #124 round 1〜5) の方針:
 *   - WAREHOUSE_API_KEY は絶対に Script Properties に入れない (Render の read-only token のみ)
 *   - 失敗黙殺禁止 — throw で Apps Script 実行履歴のエラー欄に出す
 */

const SINCE_DAYS = 7;
const MIRROR_FRESHNESS_MAX_HOURS = 26;
const ML_MAX_NEW_ROWS = 500; // 1 回の実行で追記する行数の上限。超えたら throw

function runDailyCheck() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(30 * 1000)) {
    throw new Error('LockService: 他の実行が走っているためスキップ');
  }
  try {
    main_();
  } finally {
    lock.releaseLock();
  }
}

function main_() {
  const props = PropertiesService.getScriptProperties();
  const base = props.getProperty('MIRROR_API_BASE');
  const token = props.getProperty('MIRROR_READ_TOKEN');
  const sourceSheetName = props.getProperty('SOURCE_SHEET_NAME') || '商品コード変換テーブル';
  const sourceSkuColumn = parseInt(props.getProperty('SOURCE_SKU_COLUMN') || '1', 10);
  const sourceHeaderRows = parseInt(props.getProperty('SOURCE_HEADER_ROWS') || '1', 10);
  const writeMode = (props.getProperty('WRITE_MODE') || 'dry_run').toLowerCase();

  if (!base) throw new Error('Script Property MIRROR_API_BASE が未設定');
  if (!token) throw new Error('Script Property MIRROR_READ_TOKEN が未設定');
  if (!(sourceSkuColumn >= 1)) throw new Error('SOURCE_SKU_COLUMN は 1 以上の整数');
  if (writeMode !== 'dry_run' && writeMode !== 'live') {
    throw new Error('WRITE_MODE は dry_run か live のどちらか: ' + writeMode);
  }

  // 1. Render mirror から「直近7日に登録/更新された SKU + components」を取得
  const url = base.replace(/\/+$/, '') + '/api/sku-master/recent-missing-candidates';
  const res = UrlFetchApp.fetch(url, {
    method: 'get',
    headers: { 'x-read-token': token },
    muteHttpExceptions: true,
    followRedirects: false,
  });
  const code = res.getResponseCode();
  const body = res.getContentText();
  if (code !== 200) {
    throw new Error('Mirror API error: HTTP ' + code + ' / body=' + body.slice(0, 300));
  }
  const data = JSON.parse(body);

  // 2. レスポンス契約検証 (server 側 drift 検知 — Codex review medium #4)
  //    destructive 操作 (シート本体追記) のため厳しめに型まで検証する
  if (data.since_days !== SINCE_DAYS) {
    throw new Error('Mirror endpoint contract drift: since_days=' + data.since_days + ' (expected ' + SINCE_DAYS + ')');
  }
  if (!Array.isArray(data.items)) {
    throw new Error('Mirror endpoint contract drift: items is not array');
  }
  if (typeof data.count !== 'number' || data.count !== data.items.length) {
    throw new Error('Mirror endpoint contract drift: count=' + data.count + ' != items.length=' + data.items.length);
  }
  for (let i = 0; i < data.items.length; i++) {
    const it = data.items[i];
    if (!it || typeof it !== 'object') throw new Error('items[' + i + '] is not object');
    if (typeof it.seller_sku !== 'string' || it.seller_sku.length === 0) {
      throw new Error('items[' + i + '].seller_sku is not non-empty string');
    }
    if (it['商品名'] !== null && typeof it['商品名'] !== 'string') {
      throw new Error('items[' + i + '].商品名 is not string|null');
    }
    if (typeof it.source_updated_at !== 'string') {
      throw new Error('items[' + i + '].source_updated_at is not string');
    }
    if (!Array.isArray(it.components)) {
      throw new Error('items[' + i + '].components is not array');
    }
    for (let j = 0; j < it.components.length; j++) {
      const c = it.components[j];
      if (!c || typeof c !== 'object') throw new Error('items[' + i + '].components[' + j + '] is not object');
      if (typeof c.ne_code !== 'string' || c.ne_code.length === 0) {
        throw new Error('items[' + i + '].components[' + j + '].ne_code is not non-empty string');
      }
      if (!Number.isInteger(c.quantity) || c.quantity <= 0) {
        throw new Error('items[' + i + '].components[' + j + '].quantity is not positive integer');
      }
    }
  }

  // 3. mirror freshness 検証 (両系列を見る)
  const masterSync = data.mirror_sku_master_synced_at;
  const overallSync = data.mirror_last_synced_at;
  if (!masterSync) throw new Error('mirror_sku_master_synced_at が空 — Render mirror に sku_master が未投入の可能性');
  if (!overallSync) throw new Error('mirror_last_synced_at が空 — Render mirror の同期メタが未取得');
  const masterAge = ageHours_(masterSync);
  const overallAge = ageHours_(overallSync);
  if (masterAge === null) throw new Error('mirror_sku_master_synced_at がパース不能: ' + masterSync);
  if (overallAge === null) throw new Error('mirror_last_synced_at がパース不能: ' + overallSync);
  if (masterAge > MIRROR_FRESHNESS_MAX_HOURS) {
    throw new Error('Mirror が stale (sku_master): 最終同期 ' + masterSync + ' (' + masterAge.toFixed(1) + 'h 前)');
  }
  if (overallAge > MIRROR_FRESHNESS_MAX_HOURS) {
    throw new Error('Mirror が stale (全体): 最終同期 ' + overallSync + ' (' + overallAge.toFixed(1) + 'h 前)');
  }

  const masterItems = data.items;

  // 4. シート側「商品コード変換テーブル」既存 (seller_sku, ne_code) ペアの集合を作る
  //    Codex review high #2: 重複ガードを SKU 単位 → (sku, ne_code) ペア単位にして
  //    セット商品の partial write / component 変更時に欠けた行を補完できるようにする
  //    NE_COLUMN は固定 4 (D列、CSV 仕様より)
  const NE_COLUMN = 4;
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sourceSheet = ss.getSheetByName(sourceSheetName);
  if (!sourceSheet) throw new Error('参照シートが見つからない: ' + sourceSheetName);
  const initialLastRow = sourceSheet.getLastRow();
  const existingPairs = new Set(); // 'sku|ne' 形式
  // A 列に値がある最終行 (1-indexed)。書き込みはここの直後から始める。
  // 初期値はヘッダ末尾、A 列がデータ無し (ヘッダのみ) なら header+1 から書く。
  // getLastRow() ベースで書くと、F 列等が A 列より下まで埋まってる場合に「A 列の途中」へ
  // 飛んで書かれるので採用しない (中原さん指示 2026-05-15)。
  let lastARow = sourceHeaderRows;
  if (initialLastRow > sourceHeaderRows) {
    const colsToRead = Math.max(sourceSkuColumn, NE_COLUMN);
    const range = sourceSheet.getRange(sourceHeaderRows + 1, 1, initialLastRow - sourceHeaderRows, colsToRead);
    const vals = range.getValues();
    for (let i = 0; i < vals.length; i++) {
      const row = vals[i];
      const sku = normalizeSku_(row[sourceSkuColumn - 1]);
      if (!sku) continue;
      const ne = normalizeSku_(row[NE_COLUMN - 1]);
      // ne が空でも 'sku|' で記録 → 「SKU 単独行 (NE 未記入)」も重複扱いにする
      // (中原さんが手動で SKU だけ入れて NE を後で埋める運用も想定)
      existingPairs.add(sku + '|' + ne);
      // この行は A 列に値あり。lastARow を更新 (A 列の最終データ行を追跡)
      lastARow = sourceHeaderRows + 1 + i;
    }
  }

  // 5. 差分計算 + components 展開
  //    Codex review critical: components 欠落 (mirror 不整合) は skip + warn、空 D/E 行を書かない
  //    Codex review high #1: 重複判定は (sku, ne_code) ペア単位
  const skippedNoComponents = []; // mirror 不整合 SKU
  const skippedEmptyNe = [];      // ne_code 空の component
  const newRows = [];             // {sku, name, ne_code, quantity} 形式
  for (const it of masterItems) {
    const skuNorm = normalizeSku_(it.seller_sku);
    if (!skuNorm) continue;
    if (it.components.length === 0) {
      // mirror_sku_master に居るが mirror_sku_resolved (source=master) に component 行が無い
      // = mirror 不整合 (sync 中途半端 / 別 DB 分裂 等)。空 D/E 行を書くと後で SKU 単独で skip され
      // 自動修復しないので、ここで止めて warn を出す。
      skippedNoComponents.push(it.seller_sku);
      continue;
    }
    for (const c of it.components) {
      const neNorm = normalizeSku_(c.ne_code);
      if (!neNorm) {
        skippedEmptyNe.push(it.seller_sku + '/' + c.ne_code);
        continue;
      }
      const pairKey = skuNorm + '|' + neNorm;
      if (existingPairs.has(pairKey)) continue;
      // 中原さんが SKU 単独で入れてた行 ('sku|') がある場合も pair が違うので追加する
      // (NE を後付けする運用なら、後で重複行が出るが手動で消せる範囲)
      newRows.push({
        sku: String(it.seller_sku),
        name: String(it['商品名'] ?? ''),
        ne_code: String(c.ne_code),
        quantity: c.quantity,
      });
      // Codex round 2 high #1: mirror が同じ (sku, ne) を重複返却したり、
      // 同一実行内で既に積んだペアを再度積まないように existingPairs に追加
      existingPairs.add(pairKey);
    }
  }

  if (newRows.length > ML_MAX_NEW_ROWS) {
    throw new Error('追記行数 ' + newRows.length + ' が上限 ' + ML_MAX_NEW_ROWS + ' 超過。誤爆防止のため停止');
  }

  // 6. 書き込み (dry_run なら log のみ、live ならシート追記)
  const summary = {
    write_mode: writeMode,
    fetched_master_skus: masterItems.length,
    existing_pairs_in_sheet: existingPairs.size,
    a_column_last_row: lastARow,
    new_rows_to_write: newRows.length,
    skipped_no_components: skippedNoComponents.length,
    skipped_empty_ne: skippedEmptyNe.length,
    mirror_sku_master_synced_at: masterSync,
  };
  console.log(JSON.stringify(summary));

  if (skippedNoComponents.length > 0) {
    console.warn('[WARN] components 欠落 SKU を skip (mirror_sku_resolved に source=master 行が無い、mirror 不整合の疑い): ' + skippedNoComponents.join(', '));
  }
  if (skippedEmptyNe.length > 0) {
    console.warn('[WARN] ne_code 空の component を skip: ' + skippedEmptyNe.join(', '));
  }

  if (newRows.length === 0) {
    console.log('追記なし、終了');
    return;
  }

  // 着地行 + 安全装置: dry_run / live 共通で実行
  //   Codex round 1 medium #1 反映: dry_run でも着地の健全性を確認したい運用意図のため、
  //   safety check を writeMode 分岐の前に実行する。
  //   - 着地行: A 列の最終データ行 + 1 (中原さん指示 2026-05-15)
  //     getLastRow() ベースだと F 列等が A 列より下まで埋まってる場合に A 列が中抜けで
  //     書かれてしまう (前回 incident: 5 行が想定外位置に追記された) ため A 列ベースに変更
  //   - 安全装置: 着地行範囲 (startRow〜startRow+numRows-1, A〜E) が空か再確認。
  //     人手 append / 別 GAS との競合で既存値があったら throw (誤上書き防止)。
  //   - 注意: GAS は getRange/getValues と setValues が別 RPC。
  //     **「確認時点で既存値があれば throw」しか保証できない** (RPC 間の極短時間の競合は検知不可)。
  //     完全排他は Apps Script API の制約で不可能、運用 (トリガー時刻分離) で補う。
  const startRow = lastARow + 1;
  const numRows = newRows.length;

  // 安全装置 (dry_run / live 共通)
  const safetyVals = sourceSheet.getRange(startRow, 1, numRows, 5).getValues();
  for (let i = 0; i < numRows; i++) {
    for (let j = 0; j < 5; j++) {
      const v = safetyVals[i][j];
      if (v !== '' && v !== null) {
        throw new Error(
          '安全装置発動: 書き込み予定の行 ' + (startRow + i) + ' / 列 ' + (j + 1) +
          ' に既存値 "' + String(v).slice(0, 80) + '" あり。' +
          '人手編集 / 別 GAS / 着地行計算ミス の可能性、安全のため中止。dry_run / live 共通で検知。'
        );
      }
    }
  }

  if (writeMode === 'dry_run') {
    console.log('[DRY-RUN] 着地行 = ' + startRow + ' (A 列の最終データ行 ' + lastARow + ' の次)');
    console.log('[DRY-RUN] 安全装置 OK (着地行範囲 A〜E は空セル、確認時点)');
    console.log('[DRY-RUN] 以下の行を追記する予定 (実際には書き込まない):');
    for (const r of newRows) {
      console.log('  A=' + r.sku + ' | C=' + r.name + ' | D=' + r.ne_code + ' | E=' + r.quantity);
    }
    console.log('[DRY-RUN] 本番に切り替えるには Script Property WRITE_MODE=live にしてください');
    return;
  }

  // live 書き込み:
  //   A:E 一括 setValues (原子性、Codex round 2 high #2 で確定)
  //   B 列には空文字 '' が入る。新規追記行は元々空のため既存値破壊なし。
  //   B 列の用途は CSV 仕様の「asin」(中原さんが手動入力)、新規追加時に空でよい運用。
  //   「B 列に既定値や数式を予約しない」運用ルールは README に明記。
  const matrix = newRows.map(r => [r.sku, '', r.name, r.ne_code, r.quantity]);
  sourceSheet.getRange(startRow, 1, numRows, 5).setValues(matrix);
  console.log('[LIVE] ' + numRows + ' 行を行 ' + startRow + ' から追記しました (A/C/D/E に値、B は空文字、F+ 不変)');
}

function normalizeSku_(v) {
  if (v === null || v === undefined) return '';
  return String(v).replace(/　/g, ' ').trim().toLowerCase();
}

/** 'YYYY-MM-DD HH:MM:SS' (UTC) を Date に */
function parseSyncedAt_(s) {
  if (!s) return null;
  const m = String(s).match(/^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})/);
  if (!m) return null;
  return new Date(Date.UTC(+m[1], +m[2] - 1, +m[3], +m[4], +m[5], +m[6]));
}

function ageHours_(s) {
  const d = parseSyncedAt_(s);
  if (!d) return null;
  return (Date.now() - d.getTime()) / 36e5;
}
