/**
 * デザイナー修正依頼書 (スプレッドシート) をポータルが作る・最新の画像で作り直す (画像制作の新フロー PR-F・2026-10-09・スタッフ要望 ⑥)。
 *
 *   材料を集める (生成した画像 = lp-image の imageStateFor の cards・役割と見出し = 画像を作ったときの構成)
 *   → 画像ファイルに「リンクを知っている人は閲覧可」を付ける (=IMAGE() で出すため・2026-10-09 中原さん決定 A)
 *   → 中身を組む (lib/designer-sheet.js。作り直すときは今のシートの「修正指示」を読み戻して同じ画像の行に戻す)
 *   → Google に書く (sheets-writer.js・batchUpdate 1 回)
 *   → できたときだけ DB に URL・ファイル ID・画像の並びの hash を記録 (ph_designer_sheets)
 *   → 依頼書から外れた古い版の画像の公開を外す (ポータルが付けたものだけ)
 *
 * 作法は撮影指示書 (services/shoot-sheet-service.js・PR-D) と同じ:
 *   - 同じ商品への同時の押下は 1 本だけ (DB の印 = プロセスをまたいでも効く。2 本目は 409)
 *   - 画面が見ていた依頼書 (seen_file_id) と画像の並び (seen_images_hash) が今と違えば 409 (古いタブ・ほかの人が先に作った)
 *   - 作り始めたときの「版」を、作る前・書く直前・記録するときに比べ、違えば止める (待っている間に変わった材料で書かない)
 *   - 前に作ったファイルが使えればそれを上書き (URL は変わらない)。DB に書く前に止まったファイルは appProperties の印
 *     (phDesignerSheetDraft = 商品の ID) で拾い直す。名前では探さない (人が手作りした同じ名前のシートを上書きしない)
 *   - appProperties のキー・タブ名は撮影指示書と別 (撮影指示書を拾い直さない・タブを取り違えない)
 *
 * 公開 (リンク共有) の決まり:
 *   - 公開するのは**依頼書に載せる画像ファイル (各画像の最新のできた版) だけ**。古い版・素材フォルダの写真・フォルダは公開しない
 *   - 共有ドライブの設定で社外共有が禁止されていると permissions.create が断られる → **依頼書を作らずに** 理由を返す
 *     (この回で付けた公開は外す)
 *   - ポータルが付けた公開は ph_designer_sheet_shares に記録する。前から付いていた公開 (人が付けた) は記録しない = 外さない
 *   - 作り直して依頼書から外れた古い版は、記録できた後で公開を外す (外せなければ記録に理由を残し、次の作成で外し直す)
 */
import { getDB, logEvent } from '../db.js';
import { parseDriveLink } from '../lib/drive-link.js';
import { fieldsOfBlock, readComposition } from '../lib/lp-edit.js';
import { imageStateFor } from '../lib/lp-image.js';
import { spreadsheetUrl } from '../lib/shoot-sheet.js';
import {
  buildDesignerSheet, readBackNotes, designerImagesHash, designerSheetBlockReason, designerSheetTitle, DESIGNER_TAB,
} from '../lib/designer-sheet.js';
import {
  getSheetsWriteClients, explainGoogleError, findSpreadsheetByAppProperty, spreadsheetUsable,
  createSpreadsheetInFolder, writeSpreadsheet, readOwnedTabValues,
} from './sheets-writer.js';

const APP_PROP_KEY = 'phDesignerSheetDraft';
/** lp-image の prompt の上限 (PROMPT_MAX)。これに達している prompt は途中で切れている */
const PROMPT_MAX_LEN = 30_000;
const LEASE_MS = 180_000;
const GOOGLE_TIMEOUT_MS = 20_000;
export const DESIGNER_SHEET_FORBIDDEN = 'デザイナー修正依頼書を作れるのは 画像登録者・画像作成承認者 の担当者か管理者だけです';
/** 共有ドライブの設定でリンク共有ができないとき (画面にそのまま出す) */
export const SHARE_BLOCKED_MESSAGE = '共有ドライブの設定でリンク共有ができません。画像を =IMAGE() で表示するには、AI 初稿の画像ファイルを「リンクを知っている人は閲覧可」にする必要があります。共有ドライブの「共有の設定」で、メンバー以外 (リンクを知っている全員) への共有が許可されているかを管理者が確かめてください。依頼書は作っていません';

// Google のクライアントの作り方。試験は偽物に差し替える (実際の Google には繋がない)
let clientFactory = null;
/** 試験用: () => ({ drive, sheets }) | null を渡す。null で本物に戻す */
export function __setDesignerSheetClientsForTest(fn) { clientFactory = fn; }
function makeClients() { return clientFactory ? clientFactory() : getSheetsWriteClients(); }
/** サービスアカウントが設定されているか (画面のボタンを押せるか。鍵の中身までは見ない) */
export function designerSheetConfigured() {
  return clientFactory ? !!clientFactory() : !!process.env.GOOGLE_SERVICE_ACCOUNT_KEY;
}

const folderIdOf = (draft) => {
  const p = parseDriveLink(draft?.drive_folder_url);
  return p && p.type === 'folder' ? p.id : null;
};
const rowOf = (db, id) => db.prepare('SELECT * FROM ph_designer_sheets WHERE draft_id = ?').get(Number(id)) || null;
const safeJson = (s) => { try { return JSON.parse(s); } catch { return null; } };

/** 依頼書に載せる画像 (カードの最新のできた版)。TOP から順 = 作った順 (seq) */
function currentImagesOf(cards) {
  return (cards || []).filter((c) => c.current && c.current.drive_file_id).slice().sort((a, b) => a.seq - b.seq).map((c) => ({
    root_id: c.root_id, current_id: c.current.id, drive_file_id: c.current.drive_file_id, version: c.current.version || 1,
    no: c.no, seq: c.seq, name: c.name,
  }));
}

/**
 * 画像の役割と見出し = **その画像を作ったときの指示** (受付で固めた prompt の「この画像の指示」= 構成のブロック) から読む。
 * 🚨 「いまの構成」から引かない — 画像を作った後で構成を並べ替え・追加・削除すると、同じ番号でも別の画像の役割になる。
 *    構成の編集版を時刻で割り出すのも、同じミリ秒の保存で取り違える (Codex PR-F 名指し1 低)。
 *    prompt は受付で固めたもので、1 枚の作り直しも同じ prompt を写す (lp-image の requestImageRegen) ので、画像と必ず合う
 * @returns {Map<number, {role, title}>}  元の行の ID → 役割と見出し (読めなければ入れない = 呼び手は画像の名前で代える)
 */
function rolesFromPrompts(db, rootIds) {
  const out = new Map();
  const marker = '【この画像の指示: ';
  let byNo = null;
  for (const rid of rootIds) {
    const im = db.prepare(`SELECT i.prompt, i.no, i.seq, i.name, j.compose_job_id, j.created_at FROM ph_lp_images i JOIN ph_lp_image_jobs j ON j.id = i.image_job_id WHERE i.id = ?`).get(rid) || {};
    const prompt = im.prompt || '';
    // lp-image の buildImagePlan が付けた見出しと同じ形 (番号｜名前 60 文字) を探す。共通の決まりの中に例として
    // 「【この画像の指示: …】」が書かれていても取り違えない (Codex PR-F 名指し7 低)
    const head = `${marker}${im.no === 0 ? '0枚目' : (im.no != null ? im.no + '枚目' : im.seq + '番目')}｜${String(im.name || '').trim().slice(0, 60)}】\n`;
    const hi = prompt.indexOf(head);
    const i = hi >= 0 ? hi : -1;
    const nl = i >= 0 ? prompt.indexOf('\n', i) : -1;
    // 上限 (lp-image の PROMPT_MAX = 30,000 文字) まであるなら途中で切れているかもしれない (役割の途中・見出しの前で切れる) → 下の代わりの引き方
    if (nl >= 0 && prompt.length < PROMPT_MAX_LEN) {
      const f = fieldsOfBlock(prompt.slice(nl + 1).split('\n'));
      out.set(rid, { role: f.role || '', title: f.title || '' });
      continue;
    }
    // prompt が上限 (30,000 文字) で切れて「この画像の指示」が無い (共通の決まりがとても長い構成) →
    // 受付のときに効いていた構成 (その時刻までのいちばん新しい編集版・無ければ AI の構成) から番号で引く (Codex PR-F 名指し2 中)
    if (!byNo) byNo = rolesAtRequest(db, im);
    const hit = byNo.get(im.no);
    if (hit) out.set(rid, hit);
  }
  return out;
}

/** 受付のときに効いていた構成の、画像番号 → 役割と見出し (prompt から読めないときの代わり) */
function rolesAtRequest(db, { compose_job_id: jobId, created_at: at } = {}) {
  const out = new Map();
  if (!jobId) return out;
  const edit = db.prepare('SELECT output_text FROM ph_lp_compose_edits WHERE base_job_id = ? AND created_at <= ? ORDER BY id DESC LIMIT 1').get(jobId, at);
  const text = edit ? edit.output_text : db.prepare('SELECT output_text FROM ph_lp_compose_jobs WHERE id = ?').get(jobId)?.output_text;
  const r = text ? readComposition(text) : null;
  if (r && r.ok) for (const sl of r.slots) out.set(sl.no, { role: sl.role || '', title: sl.title || '' });
  return out;
}

/** この商品の画像ファイル → 元の行の ID (全部作った行・作り直しの版とも)。読み戻しで「その行の画像は管理番号の画像か」を照らす */
function rootOfFileMap(db, draftId) {
  const m = new Map();
  for (const r of db.prepare(`SELECT i.id, i.drive_file_id, j.regen_of_image_id FROM ph_lp_images i JOIN ph_lp_image_jobs j ON j.id = i.image_job_id
    WHERE j.draft_id = ? AND i.drive_file_id IS NOT NULL`).all(draftId)) m.set(String(r.drive_file_id), r.regen_of_image_id ?? r.id);
  return m;
}

/** 画像の材料 (画面と API で同じ)。lpState があればそれを使う (詳細画面のポーリングで二度数えない) */
function materialOf(db, draft, lpState = null) {
  const st = lpState || imageStateFor(db, { draft, folderId: folderIdOf(draft) });
  const job = st.job || null;
  const cards = st.cards || [];
  const images = currentImagesOf(cards);
  return { job, cards, images, hash: job ? designerImagesHash({ jobId: job.id, images }) : null };
}

/**
 * 詳細画面に出す状態 (ボタンの出し分け・「最新の画像で作り直す」)。lp-image の状態 (GET /lp-images) に入れて返す
 * (画像ができた・作り直したのをポーリングで拾って、ボタンを押せるようにする)
 */
export function designerSheetStateFor(db, draft, { lpState = null, canEdit = false } = {}) {
  const row = rowOf(db, draft?.id);
  const m = materialOf(db, draft, lpState);
  const exists = !!(row && row.file_id);
  const blocked = designerSheetBlockReason({ configured: designerSheetConfigured(), folderId: folderIdOf(draft), job: m.job, cards: m.cards });
  // 依頼書に載っていないのに公開したままの画像 (作成前でも数える = 作成が途中で止まった分 — Codex PR-F 名指し2 高)
  const unrevoked = draft?.id ? sharesOutside(db, Number(draft.id), recordedFiles(db, Number(draft.id))).length : 0;
  return {
    can_edit: !!canEdit,
    exists,
    url: exists ? row.url : null,
    file_id: exists ? row.file_id : null,
    title: designerSheetTitle(draft?.ne_code),
    at: exists ? (row.updated_at || row.created_at) : null,
    by: exists ? (row.updated_by || row.created_by) : null,
    count: exists ? row.image_count : null,
    // 作った後で画像を作り直した (載せた版と今の最新の版が違う)・前回の書き込みが記録まで届かなかった・
    // 依頼書から外れた画像の公開を外せていない (外すのに失敗した・外す前に止まった)。どれも「作り直す」で直る
    stale: exists && (!!row.writing_at || (!!m.hash && row.images_hash !== m.hash) || unrevoked > 0),
    interrupted: exists && !!row.writing_at,
    // 依頼書に載っていないのに公開したままの画像の数 (Codex PR-F 名指し1 高: 外せなかったら、次に押すまで気づけない → 画面に出す)
    unrevoked,
    blocked,
    images_hash: m.hash,
    total: m.cards.length,
    done: m.images.length,
  };
}

// ─── 作っている最中の印 (撮影指示書 PR-D の acquireShootSheetLease と同じ作法・表は別) ─────────

function acquireLease(db, id, { ms = LEASE_MS, now = Date.now() } = {}) {
  const token = `${now.toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
  return db.transaction(() => {
    db.prepare('INSERT OR IGNORE INTO ph_designer_sheets (draft_id) VALUES (?)').run(id);
    const r = db.prepare(`UPDATE ph_designer_sheets SET lease_token = ?, lease_until = ?
      WHERE draft_id = ? AND (lease_until IS NULL OR lease_until < ?)`).run(token, new Date(now + ms).toISOString(), id, new Date(now).toISOString());
    return r.changes === 1 ? token : null;
  })();
}
/** 印がまだ自分のものか (期限内) を見て延ばす。違えば conflict (期限切れの後に別の処理が取り直した — 古い処理が書き戻さない) */
function assertLease(db, id, token, { ms = LEASE_MS, now = Date.now() } = {}) {
  const r = db.prepare(`UPDATE ph_designer_sheets SET lease_until = ? WHERE draft_id = ? AND lease_token = ? AND lease_until >= ?`)
    .run(new Date(now + ms).toISOString(), id, token, new Date(now).toISOString());
  if (r.changes !== 1) {
    throw Object.assign(new Error('時間がかかりすぎたため、このデザイナー修正依頼書の作成は取りやめました (ほかの処理が始まっています)。画面を読み直してから、もう一度押してください'), { code: 'designer_conflict' });
  }
}
function releaseLease(db, id, token) {
  db.prepare('UPDATE ph_designer_sheets SET lease_token = NULL, lease_until = NULL WHERE draft_id = ? AND lease_token = ?').run(id, token);
}

/**
 * 版 = 置き場 (画像フォルダ)・ファイル名 (商品コード)・記録してある依頼書のファイル・載せる画像の並び・いちばん新しい画像の依頼。
 * 作り始めたときと違えば書かない / 記録しない (Google を待っている間に、ほかの人が画像を作り直した・フォルダを変えた)
 */
function revisionOf(db, id) {
  const d = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id) || {};
  const row = rowOf(db, id) || {};
  const m = materialOf(db, d);
  const lastJob = db.prepare('SELECT MAX(id) AS m FROM ph_lp_image_jobs WHERE draft_id = ?').get(id)?.m ?? null;
  return JSON.stringify([d.ne_code ?? null, d.name ?? null, d.drive_folder_url ?? null, row.file_id ?? null, m.hash, lastJob]);
}
const CONFLICT_MESSAGE = '作っている間に、画像 (作り直し・全部作り直し)・画像フォルダ・商品コード・依頼書のどれかが変わりました。画面を読み直して、もう一度押してください';

// ─── 画像ファイルの公開 (リンクを知っている人は閲覧可) ─────────

const statusOf = (e) => Number(e?.code || e?.status || e?.response?.status) || null;
const reasonOf = (e) => String(e?.errors?.[0]?.reason || e?.response?.data?.error?.errors?.[0]?.reason || '');

/**
 * 公開できなかった理由 (画面に出す文)。
 * Drive API の文書 (Handle errors) では、共有の設定で断られたときは 400 invalidSharingRequest「ACL change not allowed」。
 * 共有ドライブの「メンバー以外への共有を許可しない」でも 403 になる報告があるので、権限の不足 (サービスアカウントが
 * コンテンツ管理者でない = insufficientFilePermissions 等) と通信・回数の失敗のほかは「共有ドライブの設定で」とみなす
 */
export function explainShareError(e) {
  const status = statusOf(e);
  const reason = reasonOf(e);
  const raw = String(e?.message || e || '').slice(0, 300);
  if (status === 404) return `依頼書に載せる画像のファイルが Drive に見つかりません (消された・移された)。その画像を「再生成」してから押してください (${raw})`;
  if (['insufficientFilePermissions', 'teamDriveMembershipRequired', 'appNotAuthorizedToFile'].includes(reason) || status === 401 || status === 429 || !status) return explainGoogleError(e);
  // 回数の上限 (403 でも返る) は一時的なもの — 少し待てば通る (Codex PR-F 名指し7 低)
  if (/rateLimitExceeded|userRateLimitExceeded|sharingRateLimitExceeded|dailyLimitExceeded|quotaExceeded/.test(reason)) {
    return `Google の呼び出し回数の上限に当たりました。少し待ってからもう一度押してください (${reason}: ${raw})`;
  }
  if (status === 400 || status === 403) return `${SHARE_BLOCKED_MESSAGE} (${reason || status}: ${raw})`;
  return explainGoogleError(e);
}

/**
 * ファイルの権限を全部読む (共有ドライブでは 1 ページ 100 件まで。続きは nextPageToken — Codex PR-F 名指し4 高:
 * 先頭のページだけでは anyone の公開を見落とし、付いているのに「無い」と見て記録を閉じていた)
 */
async function listAllPermissions(drive, fileId) {
  const out = [];
  let pageToken;
  for (let page = 0; page < 50; page++) {
    const r = await drive.permissions.list({ fileId, supportsAllDrives: true, pageSize: 100, fields: 'nextPageToken, permissions(id,type,role)', ...(pageToken ? { pageToken } : {}) }, { timeout: GOOGLE_TIMEOUT_MS });
    out.push(...(r?.data?.permissions || []));
    pageToken = r?.data?.nextPageToken;
    if (!pageToken) return out;
  }
  throw new Error('権限の一覧が多すぎて読み切れませんでした');
}
const closeShare = (db, rowId, why) => db.prepare(`UPDATE ph_designer_sheet_shares SET revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), revoke_error = ? WHERE id = ?`).run(why, rowId);

/**
 * ファイルに「リンクを知っている人は閲覧可」を付ける。
 * 🚨 付ける**前に**「付けようとしている」行 (permission_id = NULL) を記録する (Codex PR-F 名指し1 高):
 *    付けた直後に止まった・記録の書き込みで失敗した、でも次の回・外すときに「ポータルが付けた公開」と分かる
 *    (記録の無い公開 = 人が付けたもの、と見て外さないので、先に記録しないと外せない公開が残る)。
 * もう公開されていれば付けない。そのとき、ポータルが ID を記録した公開ならポータルのもの、それ以外は人が付けたもの (外さない)。
 * 🚨 前の回の「付けようとしていた行」(ID が NULL) は、いまある公開と結び付けない (Codex PR-F 名指し4 / base P2:
 *    止まった後に人が公開を付けたのかもしれず、ポータルが付けたと証明できない)。持ち主の分からない公開として閉じ、履歴に残す
 * @returns {Promise<number|null>}  この回で記録した行の ID。付けなかったら null
 */
async function shareImage(db, drive, { draftId, fileId, imageId, actor }) {
  const perms = await listAllPermissions(drive, fileId);
  const hit = perms.find((p) => p && p.type === 'anyone');
  const mine = db.prepare('SELECT * FROM ph_designer_sheet_shares WHERE draft_id = ? AND drive_file_id = ? AND revoked_at IS NULL ORDER BY id DESC LIMIT 1').get(draftId, fileId);
  if (mine && !mine.permission_id) {
    closeShare(db, mine.id, hit ? 'unknown_owner' : 'not_shared');
    if (hit) unknownOwnerEvent(db, draftId, fileId, actor);
  }
  if (hit) return null;
  // 記録はあるのに公開が無い (人が Drive で外した) → その記録は外れたものとして閉じる
  if (mine && mine.permission_id) closeShare(db, mine.id, 'gone');
  const sid = Number(db.prepare(`INSERT INTO ph_designer_sheet_shares (draft_id, drive_file_id, permission_id, image_id, shared_by) VALUES (?, ?, NULL, ?, ?)`)
    .run(draftId, fileId, imageId, actor).lastInsertRowid);
  // allowFileDiscovery: false = 検索には出さない (リンクを知っている人だけ)。通知メールは anyone には送られない。
  // 失敗したら (返事が来なかった = 付いたか分からない) 行は ID なしのまま残り、外すときに「持ち主の分からない公開」として扱う
  const r = await drive.permissions.create({
    fileId, supportsAllDrives: true, fields: 'id',
    requestBody: { type: 'anyone', role: 'reader', allowFileDiscovery: false },
  }, { timeout: GOOGLE_TIMEOUT_MS });
  if (r?.data?.id) db.prepare('UPDATE ph_designer_sheet_shares SET permission_id = ? WHERE id = ?').run(String(r.data.id), sid);
  return sid;
}
function unknownOwnerEvent(db, draftId, fileId, actor) {
  try {
    logEvent(db, draftId, 'designer_sheet_share_unknown', `画像 ${fileId} に「リンクを知っている人は閲覧可」の公開があります。前の作成が途中で止まった後なので、ポータルが付けたものか分かりません (外していません)。要らなければ Drive で外してください`, actor);
  } catch (_) { /* 記録の失敗で止めない */ }
}

/**
 * 公開を外す (記録してある、ポータルが付けた = API が返した ID のあるものだけ)。
 * ID が NULL の行 (付ける返事が来なかった・付ける前後に止まった) は、いまある公開と結び付けない
 * (その間に人が付けたのかもしれない — Codex PR-F 名指し4・6 / base R4)。公開が無ければ閉じ、あれば
 * 「持ち主の分からない公開」として閉じて履歴に残す (外さない)
 */
async function revokeShares(db, drive, shares) {
  let revoked = 0;
  for (const s of shares) {
    try {
      let permissionId = s.permission_id;
      if (!permissionId) {
        const anyone = (await listAllPermissions(drive, s.drive_file_id)).find((p) => p && p.type === 'anyone');
        if (!anyone) { closeShare(db, s.id, 'not_shared'); revoked += 1; continue; }
        closeShare(db, s.id, 'unknown_owner');
        unknownOwnerEvent(db, s.draft_id, s.drive_file_id, null);
        continue;
      }
      await drive.permissions.delete({ fileId: s.drive_file_id, permissionId, supportsAllDrives: true }, { timeout: GOOGLE_TIMEOUT_MS });
      closeShare(db, s.id, null);
      revoked += 1;
    } catch (e) {
      if (statusOf(e) === 404) {
        // 404 は「その権限が無い」のほかに「ファイルが見えない」でも返る (Codex PR-F 名指し4 中)。一覧を読めて、その権限が無いと分かったときだけ閉じる。
        // ファイルごと見えない (サービスアカウントが入れなくなった) ときは、公開が残っているかもしれないので閉じない
        // (外せていないまま = 画面に出し、商品の削除も止め、片付けが外し直す — Codex PR-F base R5 P1)
        let gone = false;
        try { gone = !(await listAllPermissions(drive, s.drive_file_id)).some((p) => p && (p.id === s.permission_id || p.type === 'anyone')); }
        catch (_) { gone = false; }
        if (gone) { closeShare(db, s.id, 'not_found'); revoked += 1; continue; }
        db.prepare('UPDATE ph_designer_sheet_shares SET revoke_error = ? WHERE id = ?').run('権限の削除が 404 (画像ファイルが見えない・公開が残っている)。Drive で画像の共有を確かめてください', s.id);
      } else {
        // 外せなければ記録に残す (公開のまま)。次に作ったときに外し直す
        db.prepare('UPDATE ph_designer_sheet_shares SET revoke_error = ? WHERE id = ?').run(explainGoogleError(e).slice(0, 300), s.id);
      }
    }
  }
  return revoked;
}

/** 公開中 (外していない) の記録のうち、keep (ファイル ID の集合) に入らないもの */
const sharesOutside = (db, id, keep) => db.prepare('SELECT * FROM ph_designer_sheet_shares WHERE draft_id = ? AND revoked_at IS NULL ORDER BY id')
  .all(id).filter((s) => !keep.has(s.drive_file_id));
/** 記録してある依頼書に載っている画像のファイル (公開を残すもの) */
const recordedFiles = (db, id) => new Set((safeJson(rowOf(db, id)?.images_json) || []).map((im) => String(im.drive_file_id)));

/**
 * 商品を消す前に、その商品の画像に付けた公開を全部外す (Codex PR-F 名指し2 高: 商品が消えると画面から外せなくなる)。
 * 公開が無ければ Google に触らない。@returns {Promise<{ok: boolean, remaining: number, error?: string}>}
 */
export async function revokeAllDesignerShares(draftId, { db = getDB() } = {}) {
  const id = Number(draftId);
  const active = sharesOutside(db, id, new Set());
  if (!active.length) return { ok: true, remaining: 0 };
  let clients = null;
  try { clients = makeClients(); } catch (_) { clients = null; }
  if (!clients) return { ok: false, remaining: active.length, error: 'Google のサービスアカウントが使えないので、デザイナー修正依頼書のために公開した画像の公開を外せません' };
  await revokeShares(db, clients.drive, active);
  const remaining = sharesOutside(db, id, new Set()).length;
  return remaining ? { ok: false, remaining, error: `デザイナー修正依頼書のために公開した画像 ${remaining} 枚の公開を外せませんでした (少し待ってからもう一度押してください)` } : { ok: true, remaining: 0 };
}

/**
 * 依頼書に載っていないのに公開したままの画像を片付ける (Codex PR-F 名指し3 高: プロセスが止まると catch が動かず、
 * 押されるまで公開が残る)。サーバーの起動のあと (router.js) と、その後 30 分おきに呼ぶ (外すのに一度失敗した公開も、押されなくても外れる)。
 * 作っている最中 (印が期限内) の商品と、書いている途中で止まった (writing_at が立ったまま = シートが新しい画像を
 * 出しているかもしれない) 商品は触らない (後者は画面に「作り直しが要る」が出て、作り直すと片付く)
 * @returns {Promise<{drafts: number, revoked: number}>}
 */
export async function sweepDesignerShares({ db = getDB(), now = null } = {}) {
  let clients = null;
  try { clients = makeClients(); } catch (_) { clients = null; }
  if (!clients) return { drafts: 0, revoked: 0 };
  const ids = db.prepare(`SELECT DISTINCT draft_id FROM ph_designer_sheet_shares WHERE revoked_at IS NULL`).all().map((r) => r.draft_id);
  let drafts = 0;
  let revoked = 0;
  for (const id of ids) {
    const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
    // 残す公開 = 記録の依頼書の画像。書いている途中で止まった (writing_at) 商品は、シートが送った画像を出しているかもしれないので
    // 送った画像 (writing_images_json) も残す — 両方のほかは、どちらの版のシートにも載っていない (Codex PR-F 名指し5・7 高)
    const keepOf = () => {
      const keep = recordedFiles(db, id);
      const row = rowOf(db, id);   // その都度読み直す (印を取る前後で変わりうる — Codex PR-F 名指し6 中)
      if (row && row.writing_at) for (const f of safeJson(row.writing_images_json) || []) keep.add(String(f));
      return keep;
    };
    const left = sharesOutside(db, id, keepOf());
    if (!left.length) continue;
    // 商品が消えている (記録だけ残った) ときは印の行が作れない (外部キー) ので、印なしで外す
    const exists = !!draft;
    // 印は商品ごとに今の時刻で取る (前の商品で時間がかかっても、取った瞬間から期限切れにしない — Codex PR-F 名指し6 中)
    const token = exists ? acquireLease(db, id, { now: now ?? Date.now() }) : 'none';
    if (!token) continue;
    try {
      revoked += await revokeShares(db, clients.drive, sharesOutside(db, id, keepOf()));
      drafts += 1;
    } catch (e) {
      console.error('[product-hub] デザイナー修正依頼書の公開の片付け:', id, e?.message || e);
    } finally {
      if (exists) { try { releaseLease(db, id, token); } catch (_) { /* 期限が来れば取り直せる */ } }
    }
  }
  return { drafts, revoked };
}

// ─── 作る / 作り直す ─────────

/**
 * デザイナー修正依頼書を作る (無ければ) / 最新の画像で作り直す (あれば)。reject しない (結果は outcome で返す)
 * @param {object} o
 * @param {string} o.seenFileId  画面を開いたときの依頼書のファイル ID (無ければ空)。今と違えば 409 (古い画面)
 * @param {string} o.seenImagesHash  画面が見ていた画像の並び (designer_sheet.images_hash)。今と違えば 409
 * @returns {Promise<{ok: true, url, created, count, carried, orphaned, revoked}|{ok: false, status, code, error}>}
 */
export async function createOrUpdateDesignerSheet(draftId, { actor = null, seenFileId, seenImagesHash, db = getDB() } = {}) {
  const id = Number(draftId);
  let token = null;
  try { token = acquireLease(db, id); } catch (e) {
    console.error('[product-hub] デザイナー修正依頼書の印:', e);
    return { ok: false, status: 500, code: 'error', error: 'デザイナー修正依頼書を作れませんでした (サーバーの失敗。Render のログを確認してください)' };
  }
  if (!token) return { ok: false, status: 409, code: 'busy', error: 'いまこの商品のデザイナー修正依頼書を作っています。終わるまで待ってください' };
  try {
    return await run(db, id, { actor, seenFileId, seenImagesHash, token });
  } catch (e) {
    console.error('[product-hub] デザイナー修正依頼書:', e);
    return { ok: false, status: 500, code: 'error', error: 'デザイナー修正依頼書を作れませんでした (サーバーの失敗。Render のログを確認してください)' };
  } finally {
    try { releaseLease(db, id, token); } catch (_) { /* 期限が来れば取り直せる */ }
  }
}

async function run(db, id, { actor, seenFileId, seenImagesHash, token }) {
  const draft = db.prepare('SELECT * FROM product_drafts WHERE id = ?').get(id);
  if (!draft) return { ok: false, status: 404, code: 'not_found', error: '商品が見つかりません' };
  const folderId = folderIdOf(draft);
  let clients = null;
  let keyError = null;
  try { clients = makeClients(); } catch (e) { keyError = e; }
  if (keyError) return { ok: false, status: 503, code: 'not_configured', error: `Google のサービスアカウントの鍵 (GOOGLE_SERVICE_ACCOUNT_KEY) が読めません。管理者に連絡してください (${String(keyError?.message || keyError).slice(0, 120)})` };
  const m = materialOf(db, draft);
  const blocked = designerSheetBlockReason({ configured: !!clients, folderId, job: m.job, cards: m.cards });
  if (blocked) return { ok: false, status: clients ? 409 : 503, code: clients ? 'not_ready' : 'not_configured', error: blocked };

  const row = rowOf(db, id) || {};
  // 古い画面: 開いたときの依頼書 (別のタブ・ほかの人が先に作った) や画像の並び (ほかの人が作り直した) が今と違う。Google に触る前に止める
  if (typeof seenFileId !== 'string' || seenFileId !== String(row.file_id || '')) {
    return { ok: false, status: 409, code: 'stale_screen', error: 'ほかの人 (または別の画面) がデザイナー修正依頼書を作りました。画面を読み直してから押してください' };
  }
  if (typeof seenImagesHash !== 'string' || seenImagesHash !== m.hash) {
    return { ok: false, status: 409, code: 'images_changed', error: '画面を開いた後で画像が変わりました (作り直し・全部作り直し)。画面を読み直して、載せる画像を確かめてから押してください' };
  }
  const revision = revisionOf(db, id);
  const stillSame = () => {
    if (revisionOf(db, id) !== revision) throw Object.assign(new Error(CONFLICT_MESSAGE), { code: 'designer_conflict' });
    assertLease(db, id, token);
  };
  const roles = rolesFromPrompts(db, m.images.map((im) => im.root_id));
  const images = m.images.map((im) => ({ ...im, role: roles.get(im.root_id)?.role || im.name || '', title: roles.get(im.root_id)?.title || '' }));
  const keepFiles = new Set(images.map((im) => String(im.drive_file_id)));

  // 1. 画像を公開する (依頼書を作る前。断られたら依頼書を作らない = 中途半端なシートを残さない)
  try {
    for (const im of images) {
      await shareImage(db, clients.drive, { draftId: id, fileId: im.drive_file_id, imageId: im.current_id, actor });
      // 1 枚ごとに印の期限を延ばす (8 枚 × Google の待ちで 3 分を超えても、別の処理に印を取られない — Codex PR-F 名指し1 中)
      assertLease(db, id, token);
    }
  } catch (e) {
    // 記録してある依頼書に載っていない公開は全部外す (作らなかった依頼書のために公開したままにしない)。
    // この回のものに限らない — 前の回が公開した直後に止まった分も、ここで片付く (Codex PR-F 名指し2 高)
    await revokeShares(db, clients.drive, sharesOutside(db, id, recordedFiles(db, id)));
    if (e?.code === 'designer_conflict') return { ok: false, status: 409, code: 'conflict', error: e.message };
    const reason = explainShareError(e);
    try { logEvent(db, id, 'designer_sheet_failed', reason.slice(0, 500), actor); } catch (_) { /* 記録の失敗で結果を変えない */ }
    const shareBlocked = reason.startsWith(SHARE_BLOCKED_MESSAGE);
    return { ok: false, status: shareBlocked ? 409 : 502, code: shareBlocked ? 'share_blocked' : 'google', error: shareBlocked ? reason : `デザイナー修正依頼書を作れませんでした: ${reason}` };
  }

  // 2. 置き場のファイル: 前に作ったもの → 印で拾い直す → 無ければ作る
  let fileId = null;
  let created = false;
  let wrote = false;
  let sending = false;
  let built = null;
  try {
    const prevFileId = row.file_id || null;
    // 画像フォルダを変えた (前の依頼書は別のフォルダ) ときは、新しいフォルダに作り、前の依頼書から修正指示を読んで引き継ぐ
    // (前の依頼書は消さない — Codex PR-F 名指し6 高)
    let notesFrom = null;
    if (prevFileId) {
      const u = await spreadsheetUsable(clients, { fileId: prevFileId, folderId });
      if (u.usable) fileId = prevFileId;
      else if (u.reason === 'moved') notesFrom = prevFileId;
    }
    if (!fileId) {
      const found = await findSpreadsheetByAppProperty(clients, { folderId, key: APP_PROP_KEY, value: String(id) });
      if (found) fileId = found.id;
    }
    if (!fileId) {
      stillSame();
      const c = await createSpreadsheetInFolder(clients, { folderId, title: designerSheetTitle(draft.ne_code), appProperties: { [APP_PROP_KEY]: String(id) } });
      fileId = c.id; created = true;
    }
    // 3. 人が書いた修正指示を読み戻す (作ったばかりなら無い)。読めない形 (見出しを消した) なら上書きしない
    let previous = null;
    // 範囲はタブ全体 (A1:Z2000 のように区切ると、その外へ動かした修正指示を「無い」と見て消す — Codex PR-F 名指し2 高)
    // 修正指示を読むファイル = 書くファイル (作ったばかりなら、フォルダを変える前の依頼書。無ければ読まない)
    const src = created ? notesFrom : fileId;
    const readNow = () => readOwnedTabValues(clients, { spreadsheetId: src, name: DESIGNER_TAB, render: 'FORMULA', range: null });
    let firstRead = null;
    if (src) {
      let cur = await readNow();
      if (cur.ambiguous) {
        throw Object.assign(new Error(`「${DESIGNER_TAB}」の印の付いたタブが ${cur.ambiguous.length} 枚あります (${cur.ambiguous.join('・')}。タブをコピーした?)。どれが今の依頼書か分からないので作り直していません。要らないほうのタブを消してから押してください`), { code: 'unreadable' });
      }
      // 人がタブの名前を変えた (書くファイルのタブ): 名前を元に戻して、同じタブに書く (コピーを作らない = 古いタブへの書き込みを落とさない — Codex PR-F 名指し6 中)
      if (cur.renamedFrom && src === fileId) {
        await clients.sheets.spreadsheets.batchUpdate({ spreadsheetId: fileId, requestBody: { requests: [
          { updateSheetProperties: { properties: { sheetId: cur.sheetId, title: DESIGNER_TAB }, fields: 'title' } },
        ] } }, { timeout: GOOGLE_TIMEOUT_MS });
        cur = await readNow();
      }
      if (cur.exists && cur.owned) {
        const files = rootOfFileMap(db, id);
        const back = readBackNotes(cur.values, { rootOfFile: (fid) => files.get(String(fid)) ?? null });
        if (!back.ok) throw Object.assign(new Error(back.error), { code: 'unreadable' });
        // 前のフォルダの依頼書から読んだ (書くのは新しいファイル) ときは、セルに触らずに残す (KEEP) は使えない
        previous = src !== fileId ? { ...back, layout: null } : back;
        firstRead = JSON.stringify([cur.renamedFrom || null, cur.values]);
      }
    }
    built = buildDesignerSheet({ productCode: draft.ne_code, productName: draft.name, images, previous });
    // 🚨 書く直前にもう一度読み、読み戻した後にシートが書き換えられていたら (修正指示を書いた・行を並べ替えた・列を足した) 書かない
    //    (Codex PR-F 名指し3 高: 読み戻した座標のまま KEEP したり書き直したりすると、修正指示が消える・別の画像に付く)。
    //    ここから送るまで (Google の 2 往復) の間の編集は、同じ行のままの修正指示なら KEEP で残る (見送りの残り)
    if (firstRead) {
      const again = await readNow();
      if (JSON.stringify([again.renamedFrom || null, again.values]) !== firstRead) {
        throw Object.assign(new Error('作り直している間に、デザイナー修正依頼書が書き換えられました (修正指示を書いた・行や列を動かした)。書いた内容は消していません。シートを触り終えてから、もう一度押してください'), { code: 'sheet_changed' });
      }
    }
    stillSame();
    await writeSpreadsheet(clients, {
      spreadsheetId: fileId, title: built.title, tabs: built.tabs, fresh: created,
      beforeWrite: () => {
        stillSame();
        sending = true;
        // 送る画像の一覧も記録する (返事が来ずに止まったら、シートはこの画像を出しているかもしれない = 片付けで残す — Codex PR-F 名指し7 高)。
        // 前の回も書いている途中で止まっていたら、その一覧も残す (どちらがシートに載っているか分からない)
        const prevWriting = row.writing_at ? (safeJson(rowOf(db, id)?.writing_images_json) || []) : [];
        const sendingFiles = [...new Set([...prevWriting, ...keepFiles])];
        db.prepare(`UPDATE ph_designer_sheets SET writing_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), writing_images_json = ? WHERE draft_id = ?`).run(JSON.stringify(sendingFiles), id);
      },
      onWritten: () => { wrote = true; },
    });
  } catch (e) {
    // 書く前に止まったら、記録してある依頼書に載っていない公開は外す (書いた後なら、シートが新しい画像を出しているので外さない)。
    // 送った後の失敗 (返事が来なかった) は、Google 側で書けているかもしれないので、今回載せた画像の公開は残す (Codex PR-F 名指し5 中)
    // Google が断った (400/401/403/404/409/429 = 適用されていないと分かる) なら、送らなかったのと同じ (書いている印も前のまま — Codex PR-F 名指し6 高)
    if (sending && !wrote && [400, 401, 403, 404, 409, 429].includes(statusOf(e))) {
      sending = false;
      if (!row.writing_at) db.prepare('UPDATE ph_designer_sheets SET writing_at = NULL, writing_images_json = NULL WHERE draft_id = ?').run(id);
    }
    if (!wrote) {
      const keep = recordedFiles(db, id);
      if (sending) for (const f of keepFiles) keep.add(f);
      await revokeShares(db, clients.drive, sharesOutside(db, id, keep));
    }
    if (e?.code === 'designer_conflict') return { ok: false, status: 409, code: 'conflict', error: e.message };
    if (e?.code === 'unreadable') return { ok: false, status: 409, code: 'unreadable', error: e.message };
    if (e?.code === 'sheet_changed') return { ok: false, status: 409, code: 'sheet_changed', error: e.message };
    const reason = explainGoogleError(e);
    try { logEvent(db, id, 'designer_sheet_failed', reason.slice(0, 500), actor); } catch (_) { /* 記録の失敗で結果を変えない */ }
    if (e?.code === 'tab_conflict') return { ok: false, status: 409, code: 'tab_conflict', error: reason };
    return { ok: false, status: 502, code: 'google', error: `デザイナー修正依頼書を作れませんでした: ${reason}` };
  }

  // 4. 記録 (1 トランザクション・版と印を同じ中で見る)
  const url = spreadsheetUrl(fileId);
  const isNew = created || fileId !== (row.file_id || null);
  try {
    db.transaction(() => {
      stillSame();
      db.prepare(`UPDATE ph_designer_sheets SET file_id = ?, url = ?, images_hash = ?, images_json = ?, image_count = ?,
          created_at = CASE WHEN ? OR created_at IS NULL THEN strftime('%Y-%m-%dT%H:%M:%fZ','now') ELSE created_at END,
          created_by = CASE WHEN ? OR created_by IS NULL THEN ? ELSE created_by END,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now'), updated_by = ?, writing_at = NULL, writing_images_json = NULL
        WHERE draft_id = ?`).run(fileId, url, m.hash,
        JSON.stringify(images.map((im) => ({ root_id: im.root_id, current_id: im.current_id, drive_file_id: im.drive_file_id, version: im.version }))),
        images.length, isNew ? 1 : 0, isNew ? 1 : 0, actor, actor, id);
      logEvent(db, id, isNew ? 'designer_sheet_created' : 'designer_sheet_updated',
        `デザイナー修正依頼書を${isNew ? '作成' : '作り直し'} (${images.length} 枚・修正指示 ${built.carried} 件を残した${built.orphaned ? '・行き先の無い修正指示 ' + built.orphaned + ' 件を下に残した' : ''}): ${url}`, actor);
    })();
  } catch (e) {
    if (e?.code === 'designer_conflict') return { ok: false, status: 409, code: 'conflict', error: e.message };
    throw e;
  }

  // 5. 依頼書から外れた古い版の公開を外す (失敗しても依頼書はできている。記録に残して次に外し直す)
  let revoked = 0;
  try { revoked = await revokeShares(db, clients.drive, sharesOutside(db, id, keepFiles)); }
  catch (e) { console.error('[product-hub] デザイナー修正依頼書の公開を外す:', e?.message || e); }
  return { ok: true, url, created: isNew, count: images.length, carried: built.carried, orphaned: built.orphaned, kept: built.kept, revoked };
}
