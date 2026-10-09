/**
 * Google スプレッドシートを作って書く (汎用。撮影指示書 PR-D・デザイナー修正依頼書 PR-F で使う)。2026-10-09
 *
 * - 置き場 = Drive の指定フォルダ (共有ドライブ)。files.create で mimeType = スプレッドシートを作る
 * - 中身の差し替え = 指定したタブの中身を消して書き直す (1 回の batchUpdate)。**ファイルは作り直さない = URL は変わらない**
 * - 書式 = 太字の行・網かけの行・列幅・折り返し・固定行 (spreadsheets.batchUpdate)
 *
 * 🚨 値は必ず userEnteredValue.stringValue で書く (呼び手が選べないようにしてある = RAW と同じく文字のまま)。
 *    材料は AI の出力と人の入力なので、`=IMPORTXML(...)` `=HYPERLINK(...)` のような値を数式として評価させない
 * 🚨 中身の差し替えは spreadsheets.batchUpdate 1 回 (途中で失敗しても、前の版のまま = 空の指示書を残さない)
 *
 * 認証: 既存のサービスアカウント (env GOOGLE_SERVICE_ACCOUNT_KEY・base64 JSON)。スコープは drive
 *   (Sheets API は drive スコープでも呼べる。フォルダへの作成に drive が要るので 1 つにまとめる)。
 *   SA が置き場の共有ドライブに「コンテンツ管理者」以上で入っていないと 403 になる。
 * クライアント ({ drive, sheets }) は差し込み式。試験は偽物を渡す (実際の Google には繋がない)
 */
import { google } from 'googleapis';

const SPREADSHEET_MIME = 'application/vnd.google-apps.spreadsheet';
// 1 呼び出しの上限。画面のボタンから同期で待つので長くしすぎない
const GOOGLE_TIMEOUT_MS = 20_000;
const ERROR_MAX_LEN = 300;
// Drive の ID として妥当な文字だけ (クエリ文字列に埋めるので検査する)
const DRIVE_ID_RE = /^[A-Za-z0-9_-]{5,200}$/;
// appProperties のキー・値に使える文字 (クエリ文字列に埋めるので検査する)
const APP_PROP_RE = /^[A-Za-z0-9_.-]{1,100}$/;

/**
 * { drive, sheets } を返す。env が無ければ null (fail-closed: 呼び手は「未設定なので作れません」を出す)。
 * ⚠️ 鍵が壊れていると JSON.parse が throw する — 呼び出し側の try の中で呼ぶこと
 */
export function getSheetsWriteClients(env = process.env) {
  const keyBase64 = env.GOOGLE_SERVICE_ACCOUNT_KEY;
  if (!keyBase64) return null;
  const credentials = JSON.parse(Buffer.from(keyBase64, 'base64').toString('utf-8'));
  const auth = new google.auth.GoogleAuth({ credentials, scopes: ['https://www.googleapis.com/auth/drive'] });
  return { drive: google.drive({ version: 'v3', auth }), sheets: google.sheets({ version: 'v4', auth }) };
}

/** Google の失敗を、画面に出せる日本語の理由にする (どこを直せばいいかが分かる文) */
export function explainGoogleError(e) {
  if (e?.code === 'tab_conflict') return e.message;
  const status = Number(e?.code || e?.status || e?.response?.status) || null;
  const raw = String(e?.message || e || '');
  const msg = raw.length > ERROR_MAX_LEN ? raw.slice(0, ERROR_MAX_LEN) + '…' : raw;
  if (/has not been used|SERVICE_DISABLED|is disabled|accessNotConfigured/i.test(raw)) {
    return `Google Sheets API (または Drive API) がサービスアカウントのプロジェクトで有効になっていません。管理者が Google Cloud で有効にしてください (${msg})`;
  }
  if (status === 401) return `Google のサービスアカウントで認証できませんでした (鍵を確認してください: ${msg})`;
  if (status === 403 || /insufficient|permission/i.test(raw)) {
    return `書き込みの権限がありません。サービスアカウントを商品フォルダのある共有ドライブに「コンテンツ管理者」以上で追加してください (${msg})`;
  }
  if (status === 404) return `フォルダまたはファイルが見つかりません。画像フォルダの URL と、サービスアカウントから見える場所かを確認してください (${msg})`;
  if (status === 429) return `Google の呼び出し回数の上限に当たりました。少し待ってからもう一度押してください (${msg})`;
  if (/timeout|ETIMEDOUT|ECONNRESET|ENOTFOUND|EAI_AGAIN|socket hang up/i.test(raw)) {
    return `Google に繋がりませんでした (時間切れ・通信の失敗)。少し待ってからもう一度押してください (${msg})`;
  }
  return `Google の処理で失敗しました: ${msg}`;
}

/**
 * フォルダの中に、appProperties (key=value) の付いたスプレッドシートがあれば返す。
 * 「前に作ったけれど DB に書く前に止まった」ファイルを拾い直して、二重に作らないため。
 * 名前では探さない — 人が同じ名前で手作りしたシートを上書きしないように
 * @returns {Promise<{id: string}|null>}
 */
export async function findSpreadsheetByAppProperty({ drive }, { folderId, key, value }) {
  if (!DRIVE_ID_RE.test(String(folderId || ''))) throw new Error('フォルダの ID が不正です');
  if (!APP_PROP_RE.test(String(key || '')) || !APP_PROP_RE.test(String(value || ''))) throw new Error('appProperties の形が不正です');
  const r = await drive.files.list({
    q: `'${folderId}' in parents and trashed = false and mimeType = '${SPREADSHEET_MIME}' and appProperties has { key='${key}' and value='${value}' }`,
    fields: 'files(id, name)',
    pageSize: 5,
    supportsAllDrives: true,
    includeItemsFromAllDrives: true,
    orderBy: 'createdTime',
  }, { timeout: GOOGLE_TIMEOUT_MS });
  const hit = (r?.data?.files || [])[0];
  return hit && hit.id ? { id: hit.id } : null;
}

/**
 * 前に作ったファイルがまだ使えるか (ごみ箱に入っていない・同じフォルダにある)。
 * 消された (404) なら usable=false。それ以外の失敗は throw (権限・通信の失敗で作り直さない)
 */
export async function spreadsheetUsable({ drive }, { fileId, folderId }) {
  if (!DRIVE_ID_RE.test(String(fileId || ''))) return { usable: false, reason: 'invalid_id' };
  let meta;
  try {
    meta = await drive.files.get({ fileId, fields: 'id, trashed, parents, mimeType', supportsAllDrives: true }, { timeout: GOOGLE_TIMEOUT_MS });
  } catch (e) {
    if (Number(e?.code || e?.status || e?.response?.status) === 404) return { usable: false, reason: 'not_found' };
    throw e;
  }
  const d = meta?.data || {};
  if (d.trashed) return { usable: false, reason: 'trashed' };
  if (d.mimeType && d.mimeType !== SPREADSHEET_MIME) return { usable: false, reason: 'not_spreadsheet' };
  if (folderId && Array.isArray(d.parents) && !d.parents.includes(folderId)) return { usable: false, reason: 'moved' };
  return { usable: true, reason: null };
}

/**
 * フォルダの中に空のスプレッドシートを作る。権限はフォルダから継承 (リンク共有は付けない)
 * @returns {Promise<{id: string}>}
 */
export async function createSpreadsheetInFolder({ drive }, { folderId, title, appProperties = {} }) {
  if (!DRIVE_ID_RE.test(String(folderId || ''))) throw new Error('フォルダの ID が不正です');
  const r = await drive.files.create({
    requestBody: { name: String(title), mimeType: SPREADSHEET_MIME, parents: [folderId], appProperties },
    fields: 'id',
    supportsAllDrives: true,
  }, { timeout: GOOGLE_TIMEOUT_MS });
  const id = r?.data?.id;
  if (!id) throw new Error('作ったスプレッドシートの ID が返ってきませんでした');
  return { id };
}

/** 新しいスプレッドシートに最初からあるタブの名前 (言語ごと) */
const DEFAULT_TAB_RE = /^(シート|Sheet)\s*1$/;

/** 書き込み係が作ったタブの印 (developer metadata のキー)。この印のあるタブだけを「自分のタブ」として消す・書き換える */
export const OWNED_TAB_KEY = 'phOwnedTab';

/** 人が作った同じ名前のタブがあるので書けない (黙って上書きしない) */
export class TabConflictError extends Error {
  constructor(name) {
    super(`スプレッドシートに、人が作った「${name}」というタブがあります。そのタブの名前を変えてから、もう一度押してください (上書きしないため)`);
    this.code = 'tab_conflict';
  }
}

/**
 * 式のセル (PR-F のデザイナー修正依頼書の `=IMAGE("…")` など)。**呼び手が自分で組んだ式だけ**に使う —
 * AI の出力・人の入力をそのまま入れない (それは文字のセル = 普通の string で渡す)
 */
export const formulaCell = (formula) => {
  const f = String(formula || '');
  if (!f.startsWith('=')) throw new Error('式は = で始めてください');
  return { __formula: f };
};
/** 1 セル。文字は stringValue = 文字のまま (数式・数値として解釈させない)。formulaCell() だけが式になる */
const cellData = (c) => (c && typeof c === 'object' && typeof c.__formula === 'string'
  ? { userEnteredValue: { formulaValue: c.__formula } }
  : { userEnteredValue: { stringValue: c == null ? '' : String(c) } });
const rowData = (row) => ({ values: row.map(cellData) });
/**
 * 書かないセル (PR-F のデザイナー修正依頼書: 人が書いている修正指示のセル)。行にこれがあるタブは、
 * タブ全体を消してから書く代わりに、KEEP 以外のセルだけを (前の広さ clearRows × clearCols まで) 書き直す = KEEP のセルはいまの値のまま
 */
export const KEEP_CELL = Object.freeze({ __keep: true });
const isKeep = (c) => !!c && typeof c === 'object' && c.__keep === true;

/**
 * スプレッドシートの中身を差し替える。
 * 🚨 タブの足し引き・値の消去と書き込み・書式を **spreadsheets.batchUpdate 1 回** にまとめる。
 *    batchUpdate は 1 回の中の要求をまとめて適用する (どれかが失敗すれば全部が適用されない) ので、
 *    「消した後の書き込みで失敗して、済の指示書が空になる」が起きない (Codex PR-D 名指し High)
 * 🚨 値は userEnteredValue.stringValue で書く = 数式として評価させない (`=IMPORTXML(...)` も文字のまま)。
 *    式にしたいセルだけ formulaCell('=IMAGE("…")') で渡す (呼び手が自分で組んだ式に限る)
 *
 * タブの持ち主: 書き込み係が足したタブには developer metadata の印 (OWNED_TAB_KEY) を付ける。
 *   - 書くタブが既にあり、印が無い (人が作った同じ名前のタブ) → TabConflictError (上書きしない。作ったばかりのファイルでも)
 *   - removeTabs のタブは、印があるときだけ消す (人が作った同じ名前のタブは消さない)
 *   - fresh (この呼び出しで作ったばかり) のときだけ、最初からある「シート1」などを消す
 * @param {{sheets, drive}} clients
 * @param {object} o
 * @param {string} o.spreadsheetId
 * @param {string} [o.title]  ファイル名 (違っていれば付け直す。撮影の種類を変えたとき)
 * @param {Array<{name: string, rows: string[][], format?: object}>} o.tabs  書くタブ (この順に並べる)
 * @param {string[]} [o.removeTabs]  あれば消すタブ名 (自分が作るタブのうち、今回は要らないもの)
 * @param {boolean} [o.fresh]  この呼び出しで作ったばかりのファイル (最初からある空の「シート1」を消す)
 * @param {Function} [o.beforeWrite]  batchUpdate を送る直前に呼ぶ確認 ({ existing: [{sheetId, title, owned}] } を渡す。throw すれば送らない)。
 *   呼び手が「待っている間に材料・持ち主が変わっていないか」を見る口 (Codex PR-D 名指し2 High)
 */
export async function writeSpreadsheet({ sheets, drive }, { spreadsheetId, title, tabs, removeTabs = [], fresh = false, beforeWrite = null, onWritten = null }) {
  const opt = { timeout: GOOGLE_TIMEOUT_MS };
  const got = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets(properties(sheetId,title,index,gridProperties(rowCount,columnCount)),developerMetadata(metadataKey,metadataValue))',
  }, opt);
  const existing = (got?.data?.sheets || []).map((s) => ({
    ...(s.properties || {}),
    owned: (s.developerMetadata || []).some((m) => m && m.metadataKey === OWNED_TAB_KEY),
  }));
  const byTitle = new Map(existing.map((p) => [p.title, p]));
  const wanted = new Set(tabs.map((t) => t.name));
  let nextId = Math.max(0, ...existing.map((p) => Number(p.sheetId) || 0)) + 1;

  const requests = [];
  const ids = new Map();
  // 1. 足りないタブを足す (sheetId はこちらで決める = 同じ batchUpdate の中で続けて書ける)。印も付ける
  for (const t of tabs) {
    const width = Math.max(1, ...t.rows.map((r) => r.length));
    const cur = byTitle.get(t.name);
    if (cur) {
      // 作ったばかりでも、印の無い同じ名前のタブは人が作ったもの (作った直後に足された) — 上書きしない (Codex PR-D 名指し4 M)
      if (!cur.owned) throw new TabConflictError(t.name);
      ids.set(t.name, cur.sheetId);
      // 行・列が足りなければ広げる (人が行を消していても書ける)
      const g = cur.gridProperties || {};
      if (Number(g.rowCount) && g.rowCount < t.rows.length) requests.push({ appendDimension: { sheetId: cur.sheetId, dimension: 'ROWS', length: t.rows.length - g.rowCount } });
      if (Number(g.columnCount) && g.columnCount < width) requests.push({ appendDimension: { sheetId: cur.sheetId, dimension: 'COLUMNS', length: width - g.columnCount } });
      continue;
    }
    const sheetId = nextId++;
    ids.set(t.name, sheetId);
    requests.push({ addSheet: { properties: { sheetId, title: t.name, gridProperties: { rowCount: Math.max(100, t.rows.length + 20), columnCount: Math.max(26, width) } } } });
    requests.push({ createDeveloperMetadata: { developerMetadata: {
      metadataKey: OWNED_TAB_KEY, metadataValue: t.name, location: { sheetId }, visibility: 'DOCUMENT',
    } } });
  }
  // 2. 値: タブの中身を消してから A1 から書く (前の版の行が残らない)。書式もまっさらにしてから付け直す
  tabs.forEach((t, index) => {
    const sheetId = ids.get(t.name);
    const f = t.format || {};
    const width = Math.max(1, ...t.rows.map((r) => r.length));
    if (!t.rows.some((r) => r.some(isKeep))) {
      requests.push({ updateCells: { range: { sheetId }, fields: 'userEnteredValue,userEnteredFormat' } });
      requests.push({ updateCells: { start: { sheetId, rowIndex: 0, columnIndex: 0 }, rows: t.rows.map(rowData), fields: 'userEnteredValue' } });
    } else {
      // KEEP のあるタブ: 書式だけまっさらにし、値は KEEP 以外のセルを行ごとのひと続きで書く (空のセルは '' = 消す)。
      // 前の版の広さ (clearRows × clearCols) まで書くので、前の行・列は残らない (KEEP のセルだけが残る)
      requests.push({ updateCells: { range: { sheetId }, fields: 'userEnteredFormat' } });
      const h = Math.max(t.rows.length, Number(t.clearRows) || 0);
      const w = Math.max(width, Number(t.clearCols) || 0);
      for (let r = 0; r < h; r++) {
        const row = t.rows[r] || [];
        let seg = null;
        for (let c = 0; c <= w; c++) {
          const cell = c < w ? row[c] : KEEP_CELL;
          if (!isKeep(cell)) { (seg = seg || { c, cells: [] }).cells.push(cellData(cell)); continue; }
          if (seg) requests.push({ updateCells: { start: { sheetId, rowIndex: r, columnIndex: seg.c }, rows: [{ values: seg.cells }], fields: 'userEnteredValue' } });
          seg = null;
        }
      }
    }
    requests.push({ updateSheetProperties: { properties: { sheetId, index, gridProperties: { frozenRowCount: Number(f.frozenRows) || 0 } }, fields: 'index,gridProperties.frozenRowCount' } });
    requests.push({ repeatCell: {
      range: { sheetId },
      cell: { userEnteredFormat: { wrapStrategy: f.wrap ? 'WRAP' : 'OVERFLOW_CELL', verticalAlignment: 'TOP' } },
      fields: 'userEnteredFormat',
    } });
    for (const row of f.boldRows || []) {
      requests.push({ repeatCell: {
        range: { sheetId, startRowIndex: row, endRowIndex: row + 1, startColumnIndex: 0, endColumnIndex: width },
        cell: { userEnteredFormat: { textFormat: { bold: true } } },
        fields: 'userEnteredFormat.textFormat.bold',
      } });
    }
    for (const row of f.shadedRows || []) {
      requests.push({ repeatCell: {
        range: { sheetId, startRowIndex: row, endRowIndex: row + 1, startColumnIndex: 0, endColumnIndex: width },
        cell: { userEnteredFormat: { backgroundColor: { red: 0.93, green: 0.95, blue: 0.97 } } },
        fields: 'userEnteredFormat.backgroundColor',
      } });
    }
    // 行の高さ (PR-F: 画像を出す行を高くする)。[{ start, end, px }] (start 以上 end 未満の行)
    for (const rh of f.rowHeights || []) {
      requests.push({ updateDimensionProperties: {
        range: { sheetId, dimension: 'ROWS', startIndex: Number(rh.start) || 0, endIndex: Number(rh.end) || (Number(rh.start) || 0) + 1 },
        properties: { pixelSize: Number(rh.px) || 21 },
        fields: 'pixelSize',
      } });
    }
    (f.columnWidths || []).forEach((px, i) => {
      requests.push({ updateDimensionProperties: {
        range: { sheetId, dimension: 'COLUMNS', startIndex: i, endIndex: i + 1 },
        properties: { pixelSize: Number(px) || 100 },
        fields: 'pixelSize',
      } });
    });
  });
  // 3. 要らないタブを消す (足した後 = 最後の 1 枚を消して失敗しない)。印のある自分のタブだけ。
  //    作ったばかりなら、最初からある空のタブ (sheetId 0 の「シート1 / Sheet1」) も。それ以外の印の無いタブは
  //    作ったばかりでも消さない (作った直後に人が足したタブを消さない — Codex PR-D 名指し2 L)
  for (const p of existing) {
    if (wanted.has(p.title)) continue;
    const initialTab = fresh && !p.owned && Number(p.sheetId) === 0 && DEFAULT_TAB_RE.test(String(p.title || ''));
    if (initialTab || (p.owned && removeTabs.includes(p.title))) requests.push({ deleteSheet: { sheetId: p.sheetId } });
  }
  if (beforeWrite) beforeWrite({ existing: existing.map((p) => ({ sheetId: p.sheetId, title: p.title, owned: p.owned })) });
  await sheets.spreadsheets.batchUpdate({ spreadsheetId, requestBody: { requests } }, opt);
  if (onWritten) onWritten();

  // 4. ファイル名 (違うときだけ)。中身とは別の呼び出しだが、失敗しても中身は新しい版のまま (名前だけ古い)
  if (title && drive) {
    const meta = await drive.files.get({ fileId: spreadsheetId, fields: 'name', supportsAllDrives: true }, opt);
    if (meta?.data?.name !== title) {
      await drive.files.update({ fileId: spreadsheetId, requestBody: { name: title }, fields: 'id', supportsAllDrives: true }, opt);
    }
  }
}

/**
 * 書き込み係が作ったタブ (印つき) の今の値を読む (PR-F: 作り直す前に、人が書いた修正指示を読み戻す)。
 * 値は画面に見えている文字 (FORMATTED_VALUE・式のセルは式の結果 = =IMAGE は空)。render: 'FORMULA' なら式のセルは式そのもの。
 * @returns {Promise<{exists: false}|{exists: true, owned: boolean, values: string[][]|null}>}
 *   owned=false (人が作った同じ名前のタブ) は読まない (書くときに writeSpreadsheet が TabConflictError で止める)
 */
export async function readOwnedTabValues({ sheets }, { spreadsheetId, name, range = 'A1:Z2000', render = 'FORMATTED_VALUE' }) {
  const opt = { timeout: GOOGLE_TIMEOUT_MS };
  const got = await sheets.spreadsheets.get({
    spreadsheetId,
    fields: 'sheets(properties(sheetId,title),developerMetadata(metadataKey,metadataValue))',
  }, opt);
  const tab = (got?.data?.sheets || []).find((s) => s?.properties?.title === name);
  if (!tab) return { exists: false };
  const owned = (tab.developerMetadata || []).some((m) => m && m.metadataKey === OWNED_TAB_KEY);
  if (!owned) return { exists: true, owned: false, values: null };
  // タブ名は自分で決めた名前 (引用符を含まない) なので、そのまま範囲に入れる
  const tabRef = `'${String(name).replace(/'/g, "''")}'`;
  const r = await sheets.spreadsheets.values.get({
    // range: null = タブ全体 (使っている範囲を全部返す)
    spreadsheetId, range: range ? `${tabRef}!${range}` : tabRef, valueRenderOption: render === 'FORMULA' ? 'FORMULA' : 'FORMATTED_VALUE', majorDimension: 'ROWS',
  }, opt);
  return { exists: true, owned: true, values: Array.isArray(r?.data?.values) ? r.data.values : [] };
}
