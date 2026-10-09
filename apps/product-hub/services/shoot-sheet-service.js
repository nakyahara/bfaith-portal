/**
 * 撮影指示書 (スプレッドシート) をポータルが自分で作る・上書きする (画像制作の新フロー PR-D・2026-10-09)。
 * 設計 = 共有ドライブ システム設計/商品ハブ_画像制作の新フロー_設計_20261008.md §3.4
 *
 *   材料を集める (shootSheetCutsFor) → 中身を組む (lib/shoot-sheet.js) → Google に書く (sheets-writer.js)
 *   → できたときだけ DB に URL・ファイル ID・材料の hash を 1 トランザクションで書く (db.recordShootSheet)
 *
 * 冪等:
 *   - 前に作ったファイル (shoot_sheet_file_id) が使えれば、そのファイルを上書きする (URL は変わらない)
 *   - DB に書く前に止まった場合も、作ったファイルには appProperties (phShootSheetDraft = 商品の ID) を付けてあるので、
 *     次に押したときフォルダの中から拾い直す (2 つ作らない)。名前では探さない (人が手作りした同名のシートを上書きしない)
 *   - 同じ商品への同時の押下は 1 本だけ (DB の印 = プロセスをまたいでも効く。2 本目は 409)
 * 競合: 作り始めたときの「版」(撮影判定・画像フォルダ・商品名・LP構成・指示書の URL) を、作る前・書く直前・記録するときに比べ、
 *   違えば止める (古い材料で上書きしない・後から終わったほうが黙って勝たない)
 * 失敗しても camera_instruction_url は書かない (Google の処理が全部通ったときだけ記録する)。
 *   中身の差し替えは 1 回の batchUpdate なので、途中で失敗しても前の版のまま (空の指示書を残さない)
 */
import {
  getDB, logEvent, recordShootSheet, getShootMention, shootSheetRevision, assertShootSheetRevision,
  acquireShootSheetLease, releaseShootSheetLease, assertShootSheetLease,
} from '../db.js';
import { parseDriveLink } from '../lib/drive-link.js';
import { effectiveCompose } from '../lib/lp-edit.js';
import { latestShootJudgement } from '../lib/lp-compose.js';
import {
  buildShootSheet, cutsFromComposeText, cutsFromSlots, shootSheetMaterialHash, shootRequestBody, shootSheetBlockReason,
  spreadsheetUrl, MANAGED_SHEETS, SHOOT_SHEET_MODES,
} from '../lib/shoot-sheet.js';
import {
  getSheetsWriteClients, explainGoogleError, findSpreadsheetByAppProperty, spreadsheetUsable,
  createSpreadsheetInFolder, writeSpreadsheet,
} from './sheets-writer.js';

const APP_PROP_KEY = 'phShootSheetDraft';

// Google のクライアントの作り方。試験は偽物に差し替える (実際の Google には繋がない)
let clientFactory = null;
/** 試験用: () => ({ drive, sheets }) | null を渡す。null で本物に戻す */
export function __setShootSheetClientsForTest(fn) { clientFactory = fn; }
function makeClients() {
  return clientFactory ? clientFactory() : getSheetsWriteClients();
}
/** サービスアカウントが設定されているか (画面のボタンを押せるか。鍵の中身までは見ない) */
export function shootSheetConfigured() {
  return clientFactory ? !!clientFactory() : !!process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
}

/**
 * ⭐ 撮影指示書の材料 (カット) の差し込み口。**材料を変えるときはこの関数の中身だけを替える**
 * (画面の「LP構成が変わりました」(材料の hash) も API もこの関数しか見ていない)。
 *
 * 構成 = 効いている構成 (PR-B の lp-edit の effectiveCompose: いちばん新しいできた構成の、いちばん新しい編集版。
 *   編集版が無ければ AI の構成)。その画像の並びで、
 *   - 要撮影か = 編集版の slots_json の shoot (人が直した値) が正本。編集版が無ければ AI の撮影判定 (PR-C の
 *     latestShootJudgement・その構成のもので、実モデル一致のときだけ) の needs_shoot。AI の判定も無ければ
 *     ブロックの `## 使用素材` に「撮影」が出てくるかで推定
 *   - カットの中身 = AI の撮影判定 (元の画像の番号で引く = 並べ替え・追加してもずれない)。無い画像はブロックの見出しから拾う
 *   (組み立ては lib/shoot-sheet.js の cutsFromSlots。純粋関数で試している)
 * 構成がパーサで読めない (画面で直せない) ときは、本文から推定する最小版 (cutsFromComposeText) で組む
 * @returns {{cuts: Array, source: 'lp'|'none', composeJobId: number|null, editId: number|null}}
 */
export function shootSheetCutsFor(db, draft) {
  const eff = effectiveCompose(db, draft?.id);
  if (!eff) return { cuts: [], source: 'none', composeJobId: null, editId: null };
  const editId = eff.edit ? eff.edit.id : null;
  if (eff.error || !Array.isArray(eff.slots)) {
    return { cuts: cutsFromComposeText(eff.text), source: 'lp', composeJobId: eff.job.id, editId };
  }
  // 編集版の要撮影が読めたか (slots_json が壊れていると、lp-edit は e<編集版>x<番号> の uid を振って shoot=false にする)
  const hasEditShoot = !!eff.edit && eff.slots.every((sl) => !/^e\d+x\d+$/.test(String(sl.uid)));
  // AI の撮影判定は、効いている構成と同じ構成 (job) のものだけ使う (新しい構成を AI が作り直している途中なら使わない)
  const judge = latestShootJudgement(db, draft.id);
  const aiImages = judge && judge.available && judge.job_id === eff.job.id ? judge.images : null;
  return { cuts: cutsFromSlots({ slots: eff.slots, hasEditShoot, aiImages }), source: 'lp', composeJobId: eff.job.id, editId };
}

const folderIdOf = (draft) => {
  const p = parseDriveLink(draft?.drive_folder_url);
  return p && p.type === 'folder' ? p.id : null;
};
const materialOf = (draft, shootMode, cuts) => ({
  productCode: draft?.ne_code || '', productName: draft?.name || '', shootMode,
  folderUrl: String(draft?.drive_folder_url || '').trim(), cuts,
});

/**
 * 詳細画面に出す状態 (ボタンの出し分け・「更新が要る」)。
 * ours = 指示書の URL 欄が、ポータルが作ったファイルを指している (手で貼った URL ではない)
 */
export function shootSheetStateFor(db, draft, ip) {
  const shootMode = ip?.shoot_mode ?? null;
  const fileId = ip?.shoot_sheet_file_id || null;
  const url = String(ip?.camera_instruction_url || '').trim();
  const ours = !!fileId && url === spreadsheetUrl(fileId);
  const blocked = shootSheetBlockReason({ shootMode, folderId: folderIdOf(draft), configured: shootSheetConfigured() });
  let cuts = [];
  let source = 'none';
  try { ({ cuts, source } = shootSheetCutsFor(db, draft)); } catch (e) { console.error('[product-hub] 撮影指示書の材料:', e?.message || e); }
  // 材料が変わったか: LP構成から作ったものだけ比べる (API で渡されたカットで作ったものは比べようがない)
  const stale = ours && ip?.shoot_sheet_source === 'auto' && !!SHOOT_SHEET_MODES[shootMode]
    && ip?.shoot_sheet_hash !== shootSheetMaterialHash(materialOf(draft, shootMode, cuts));
  return {
    ours, url: ours ? url : null, manualUrl: !ours && url ? url : null,
    at: ours ? ip?.shoot_sheet_at || null : null, by: ours ? ip?.shoot_sheet_by || null : null,
    stale, blocked, cutsCount: cuts.length, cutLabels: cuts.map((c) => `${c.label} ${c.cut}`.trim()), source,
  };
}

/**
 * 撮影指示書を作る (無ければ) / 上書きする (あれば)。reject しない (結果は outcome で返す)
 * @param {object} o
 * 材料は必ず shootSheetCutsFor (サーバが LP構成から組む)。呼び手からカットは受けない
 *   (古い画面・直接の呼び出しで、正本の LP構成と違う中身を書かせない — Codex PR-D 名指し3 M)
 * @param {string|null} [o.mention]  その回だけの宛先 (検査済み)。null / 省略ならいつもの宛先
 * @param {string|null} [o.replaceManualUrl]  手で貼った URL を置き換えてよい、と画面で確かめたときの「その URL」。
 *   今の URL と同じときだけ置き換える (確かめた後に別の URL に貼り替えられていたら、もう一度聞く — Codex PR-D 名指し M)
 * @param {string|null} [o.overwriteFileId]  DB に記録する前に止まった撮影指示書 (拾い直したファイル) を上書きしてよい、と
 *   画面で確かめたときの、そのファイルの ID
 * @returns {Promise<{ok: true, url, created: boolean, cuts: number, source}|{ok: false, status: number, code: string, error: string, manual_url?: string}>}
 */
export async function createOrUpdateShootSheet(draftId, { mention = null, actor = null, replaceManualUrl = null, overwriteFileId = null, db = getDB() } = {}) {
  const id = Number(draftId);
  // 作っている最中の印 (DB)。同じ商品の 2 本目は 409 (二重押し・2 人同時・プロセスをまたいでも)
  let token = null;
  try { token = acquireShootSheetLease(db, id); } catch (e) {
    console.error('[product-hub] 撮影指示書の印:', e);
    return { ok: false, status: 500, code: 'error', error: '撮影指示書を作れませんでした (サーバーの失敗。Render のログを確認してください)' };
  }
  if (!token) return { ok: false, status: 409, code: 'busy', error: 'いまこの商品の撮影指示書を作っています。終わるまで待ってください' };
  try {
    return await run(db, id, { mention, actor, replaceManualUrl, overwriteFileId, token });
  } catch (e) {
    // ここに来るのは DB の失敗など想定外のもの。Google の失敗は run の中で理由にしている
    console.error('[product-hub] 撮影指示書:', e);
    return { ok: false, status: 500, code: 'error', error: '撮影指示書を作れませんでした (サーバーの失敗。Render のログを確認してください)' };
  } finally {
    try { releaseShootSheetLease(db, id, token); } catch (_) { /* 期限が来れば取り直せる */ }
  }
}

/**
 * いまの材料の hash (LP構成から拾うときだけ)。版 (shootSheetRevision) は LP構成の依頼・編集版の動きを見るが、
 * 実モデル確認・AI の撮影判定の切り替わりまでは見ないので、材料そのものも比べる (Codex PR-D 名指し2 M)
 */
function autoMaterialHashNow(db, id) {
  const d = db.prepare('SELECT id, ne_code, name, drive_folder_url FROM product_drafts WHERE id = ?').get(id);
  const ip = db.prepare('SELECT shoot_mode FROM draft_image_production WHERE draft_id = ?').get(id) || {};
  return shootSheetMaterialHash(materialOf(d, ip.shoot_mode ?? null, shootSheetCutsFor(db, d).cuts));
}

async function run(db, id, { mention, actor, replaceManualUrl, overwriteFileId, token }) {
  const draft = db.prepare('SELECT id, ne_code, name, drive_folder_url FROM product_drafts WHERE id = ?').get(id);
  if (!draft) return { ok: false, status: 404, code: 'not_found', error: '商品が見つかりません' };
  const ip = db.prepare('SELECT shoot_mode, camera_instruction_url, shoot_sheet_file_id FROM draft_image_production WHERE draft_id = ?').get(id) || {};
  // 作り始めたときの版。Google に書く直前と記録するときに、これと比べる
  const revision = shootSheetRevision(db, id);
  const shootMode = ip.shoot_mode ?? null;
  const folderId = folderIdOf(draft);
  let clients = null;
  let keyError = null;
  try { clients = makeClients(); } catch (e) { keyError = e; }
  if (keyError) return { ok: false, status: 503, code: 'not_configured', error: `Google のサービスアカウントの鍵 (GOOGLE_SERVICE_ACCOUNT_KEY) が読めません。管理者に連絡してください (${String(keyError?.message || keyError).slice(0, 120)})` };
  const blocked = shootSheetBlockReason({ shootMode, folderId, configured: !!clients });
  if (blocked) return { ok: false, status: clients ? 400 : 503, code: clients ? 'blocked' : 'not_configured', error: blocked };

  const prevUrl = String(ip.camera_instruction_url || '').trim();
  const prevFileId = ip.shoot_sheet_file_id || null;
  const ours = !!prevFileId && prevUrl === spreadsheetUrl(prevFileId);
  // 手で貼った指示書の URL を黙って差し替えない。画面で確かめてから、確かめた URL を添えて送り直してもらう
  if (prevUrl && !ours && replaceManualUrl !== prevUrl) {
    return { ok: false, status: 409, code: 'manual_url', manual_url: prevUrl,
      error: `撮影指示書の URL に、手で貼ったもの (${prevUrl.slice(0, 120)}) が入っています。自動で作る撮影指示書に置き換えますか？ (手で貼ったスプレッドシートは消しません。URL の欄だけ差し替えます)` };
  }

  const material = { cuts: shootSheetCutsFor(db, draft).cuts, source: 'auto' };
  const mat = materialOf(draft, shootMode, material.cuts);
  const hash = shootSheetMaterialHash(mat);
  const mentionUsed = typeof mention === 'string' ? mention : getShootMention(db).value;
  // 書く直前と記録するときの確認: 版・材料 (LP構成から拾うとき)・印がまだ自分のものか
  const stillSame = () => {
    assertShootSheetRevision(db, id, revision);
    if (autoMaterialHashNow(db, id) !== hash) {
      throw Object.assign(new Error('作っている間に LP構成 (AI の撮影判定・直した構成) が変わりました。画面を読み直して、もう一度押してください (指示書の URL は変えていません)'), { code: 'shoot_sheet_conflict' });
    }
    assertShootSheetLease(db, id, token);
  };

  let fileId = null;
  let created = false;
  let recovered = false;
  try {
    // 1. 前に作ったファイルが使えればそれ (消された・ごみ箱・別フォルダに移ったなら作り直す)
    if (prevFileId && (await spreadsheetUsable(clients, { fileId: prevFileId, folderId })).usable) fileId = prevFileId;
    // 2. DB に書く前に止まったファイルを拾い直す (既にあるファイル = 人が足したタブがありうるので fresh にしない)
    if (!fileId) {
      const found = await findSpreadsheetByAppProperty(clients, { folderId, key: APP_PROP_KEY, value: String(id) });
      if (found) { fileId = found.id; recovered = true; }
    }
    // 3. 無ければ作る (作る前にも版を見る = 待っている間に変わっていれば作らない)
    if (!fileId) {
      assertShootSheetRevision(db, id, revision);
      const c = await createSpreadsheetInFolder(clients, { folderId, title: buildShootSheet({ ...mat, requestText: '' }).title, appProperties: { [APP_PROP_KEY]: String(id) } });
      fileId = c.id; created = true;
    }
    const url = spreadsheetUrl(fileId);
    const requestText = shootRequestBody({ mention: mentionUsed, productName: mat.productName, sheetUrl: url, folderUrl: mat.folderUrl });
    const built = buildShootSheet({ ...mat, requestText });
    // 書く直前 (タブの様子を読んだ後・送る直前) にもう一度見る (Google を待っている間に変わった材料で、既にある指示書を上書きしない)
    stillSame();
    await writeSpreadsheet(clients, {
      spreadsheetId: fileId, title: built.title, tabs: built.sheets, fresh: created,
      beforeWrite: ({ existing }) => {
        // 拾い直したファイル (DB に記録する前に止まったもの) に、もう指示書のタブ (印つき) があれば、人が直しているかもしれない。
        // 画面は「作成」と出しているので上書きの確認が出ていない → 確かめてから送り直してもらう (Codex PR-D 名指し3 M)
        if (recovered && overwriteFileId !== fileId && existing.some((t) => t.owned)) {
          throw Object.assign(new Error(`画像フォルダに、前に作りかけた撮影指示書があります (${spreadsheetUrl(fileId)})。今の LP構成で上書きしますか？ (スプレッドシートで直した内容は消えます)`),
            { code: 'recovered_file', fileId });
        }
        stillSame();
      },
      // 自分が作るタブのうち今回は要らないもの (カメラマン撮影 → 社内撮影 にしたときの「依頼文」)。印のあるタブだけ消える
      removeTabs: MANAGED_SHEETS.filter((n) => !built.sheets.some((t) => t.name === n)),
    });
  } catch (e) {
    if (e?.code === 'shoot_sheet_conflict') return { ok: false, status: 409, code: 'conflict', error: e.message };
    if (e?.code === 'recovered_file') return { ok: false, status: 409, code: 'recovered_file', file_id: e.fileId, error: e.message };
    const reason = explainGoogleError(e);
    try { logEvent(db, id, 'shoot_sheet_failed', reason.slice(0, 500), actor); } catch (_) { /* 記録の失敗で結果を変えない */ }
    if (e?.code === 'tab_conflict') return { ok: false, status: 409, code: 'tab_conflict', error: reason };
    return { ok: false, status: 502, code: 'google', error: `撮影指示書を作れませんでした: ${reason}` };
  }

  const url = spreadsheetUrl(fileId);
  try {
    recordShootSheet(db, id, { url, fileId, hash, source: material.source, actor, created: created || fileId !== prevFileId, expectedRevision: revision, verify: stillSame });
  } catch (e) {
    if (e?.code === 'shoot_sheet_conflict') return { ok: false, status: 409, code: 'conflict', error: e.message };
    throw e;
  }
  return { ok: true, url, created, cuts: material.cuts.length, source: material.source };
}
