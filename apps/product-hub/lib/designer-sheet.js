/**
 * デザイナー修正依頼書 (スプレッドシート) の中身を組む — 画像制作の新フロー PR-F (2026-10-09・スタッフ要望 ⑥)。
 *
 * 今まで: AI で作った画像を 1 枚ずつスプレッドシートに貼って、横に修正箇所を書いていた。
 * これから: ポータルが「生成した全画像を TOP から順に貼ったシート」を商品の画像フォルダに作る。人は「修正指示」の列に書くだけ。
 *
 * 画像の貼り方 (2026-10-09 中原さん決定 A): AI 初稿の画像ファイルに「リンクを知っている人は閲覧可」を付けて `=IMAGE()` で出す。
 *   (Sheets API はセルに画像ファイルそのものを貼れない。B = Apps Script は採らない)
 *
 * 守りたいこと:
 *   ① **人が書いた「修正指示」を消さない**。作り直すときは今のシートの修正指示を読み戻し、同じ画像 (管理番号 = 画像の元の行の ID)
 *      の行に戻す。行き先の無い修正指示 (画像が無くなった・全部作り直して別の画像になった・管理番号を消された) は
 *      シートの下の「前の依頼書の修正指示」に残す (別の画像の行に黙って付け替えない = 付け間違いより、残すほうを取る)
 *   ② 値は文字のセル (sheets-writer が stringValue で書く)。式は `=IMAGE("…")` と `=HYPERLINK("…")` だけで、
 *      URL はこちらで組む (Drive のファイル ID は形を検査してから埋める = 引用符で式を壊させない)
 *   ③ 見出しの行 (「修正指示」の列) が見つからないシートは、読めないので**上書きしない** (呼び手が止める)
 *
 * ここは純粋関数だけ (DB・Google に触らない)。材料集めと Google への書き込みは services/designer-sheet-service.js
 */
import { createHash } from 'node:crypto';

/** 書くタブの名前 (撮影指示書 PR-D の「撮影依頼書」「依頼文」とは別の名前) */
export const DESIGNER_TAB = 'デザイナー修正依頼';
/** 列 (左から)。読み戻しは見出しの文字で列を探す (人が列を足しても読める) */
export const HEADERS = ['画像番号・役割', 'AI生成画像', '修正指示', '版', '画像を開く', '管理番号 (消さないでください)'];
const H_LABEL = HEADERS[0];
const H_IMAGE = HEADERS[1];
const H_NOTE = HEADERS[2];
const H_VERSION = HEADERS[3];
const H_KEY_PREFIX = '管理番号';
/** 行き先の無い修正指示を残す場所の見出し (この行より下は「前の依頼書の修正指示」) */
export const ORPHAN_HEADING = '前の依頼書の修正指示 (対応する画像が無くなったもの。消していません — 要らなければ修正指示の欄を空にしてください)';
const ORPHAN_HEADING_HEAD = '前の依頼書の修正指示';
/** 見出しより上の行 (タイトル・使い方)。見出しは 3 行目 */
const TOP_ROWS = 2;
/** 画像の行の高さ (px)。1200×1200 の画像が見分けられる大きさ */
export const IMAGE_ROW_PX = 300;
const CELL_MAX = 5000;
// Drive のファイル ID (式に埋めるので形を検査する。lp-image の DRIVE_ID_RE と同じ)
const DRIVE_ID_RE = /^[-\w]{10,200}$/;
const KEY_RE = /^img-([1-9]\d{0,15})$/;
// 画像の列の式から、出している画像ファイルの ID を読む (imageFormulaUrl の形)
const IMAGE_FILE_RE = /lh3\.googleusercontent\.com\/d\/([-\w]{10,200})/;
/** 書かないセル (services/sheets-writer.js の KEEP_CELL と同じ形)。人がいま書いている修正指示に触らない */
const KEEP = { __keep: true };
/** 人の書いた修正指示は切らない (Sheets の 1 セルの上限 50,000 文字まで) */
const NOTE_MAX = 50_000;
/** 式のセル (services/sheets-writer.js の formulaCell と同じ形。lib は googleapis を読み込まないので形だけ合わせる) */
const formula = (f) => ({ __formula: f });

const str = (v) => (v == null ? '' : String(v));
/** セルに入れる文字 (制御文字を除き、長さを切る) */
// eslint-disable-next-line no-control-regex
const cellText = (v, max = CELL_MAX) => str(v).replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F\u007F]/g, '').slice(0, max);

/** ファイル名 `デザイナー修正依頼書_<商品コード>` (ファイル名に使えない文字は寄せる・撮影指示書と同じ寄せ方) */
export function designerSheetTitle(productCode) {
  // eslint-disable-next-line no-control-regex
  const code = str(productCode).replace(/[\u0000-\u001f\u007f]/g, '').replace(/[\\/]/g, '／').replace(/[:*?"<>|]/g, '_').trim().slice(0, 80);
  return `デザイナー修正依頼書_${code || '(商品コードなし)'}`;
}

/**
 * `=IMAGE()` に渡す画像の URL。
 *
 * 一次情報で確かめたこと (2026-10-09):
 *   - Google のヘルプ「IMAGE 関数」(support.google.com/docs/answer/3093333):
 *     「Important: You can only use URLs that aren't hosted at drive.google.com.」
 *     → `drive.google.com/uc?export=view&id=…` も `drive.google.com/thumbnail?id=…` も IMAGE では**決まりの外**
 *   - Google の Developer Relations (Justin Poehnelt, 2024-01-11「Embed images from Google Drive in your website」):
 *     `drive.google.com/uc?export=view&id=…` は 403 を返すようになった (サードパーティ Cookie の廃止と関係)
 *   - Drive API の files.thumbnailLink: 「A short-lived link … Typically lasts on the order of hours」→ シートに置き続ける URL にできない
 * → drive.google.com 以外で、ファイル ID だけから組めて、リンク共有 (anyone・reader) のファイルを出せるのは
 *   `https://lh3.googleusercontent.com/d/<ID>` (Drive が画像を配信する Google のホスト)。
 *   ⚠️ この形は Google の文書に載っていない (使っている人の報告だけ)。**本番で表示されるかはデプロイ後に確かめる** (PR の「デプロイ後に」)。
 *   表示されなかったときに直すのはこの 1 か所だけ。画像が出なくても、隣の「画像を開く」(Drive の閲覧画面) から開ける
 */
export function imageFormulaUrl(fileId) {
  const id = str(fileId);
  if (!DRIVE_ID_RE.test(id)) throw new Error('画像のファイル ID が不正です');
  return `https://lh3.googleusercontent.com/d/${id}`;
}
/** Drive の閲覧画面 (画像が出なかったときの逃げ道。共有ドライブのメンバーなら必ず開ける) */
export function driveViewUrl(fileId) {
  const id = str(fileId);
  if (!DRIVE_ID_RE.test(id)) throw new Error('画像のファイル ID が不正です');
  return `https://drive.google.com/file/d/${id}/view`;
}

/** 画像の番号 (画面の画像カードと同じ: 0枚目 (TOP) / N枚目。番号が無ければ作った順) */
export function imageLabel({ no, seq } = {}) {
  if (no === 0) return '0枚目 (TOP)';
  if (Number.isInteger(no)) return `${no}枚目`;
  return `${Number(seq) || '?'}番目`;
}

/** 管理番号 (行と画像を結ぶ鍵)。画像の元の行 (全部作ったときの行) の ID — 作り直しても (v2, v3…) 変わらない */
export const imageKey = (rootId) => `img-${Number(rootId)}`;

/**
 * 依頼書に載せる画像の並びの hash (作ったときの値と今の値が違えば「最新の画像で作り直す」を出す)。
 * 中身 = どの「全部作る」依頼か + 画像ごとの (元の行・いま載せる版の行・ファイル)。役割の文字は入れない
 * (役割は画像を作ったときの構成から引くので、画像が同じなら変わらない)
 */
export function designerImagesHash({ jobId, images }) {
  const body = JSON.stringify({ v: 1, job: Number(jobId) || null,
    images: (images || []).map((im) => [Number(im.root_id), Number(im.current_id), str(im.drive_file_id)]) });
  return createHash('sha256').update(body, 'utf8').digest('hex');
}

/**
 * 作れない理由 (null なら作れる)。画面のボタンと API で同じ判定。
 * 「全部の画像ができてから」= 全部作る依頼が終わっていて、どの画像も最新のできた版があり、作り直し中のものが無い
 * @param {{configured: boolean, folderId: string|null, job: object|null, cards: Array}} o  cards = lp-image の imageStateFor().cards
 */
export function designerSheetBlockReason({ configured, folderId, job, cards }) {
  if (!configured) return 'Google のサービスアカウント (GOOGLE_SERVICE_ACCOUNT_KEY) が設定されていないので作れません。管理者に連絡してください';
  if (!folderId) return '画像フォルダ (Driveリンク) が無いので、置き場がありません (画像タブで画像フォルダを設定してください)';
  const list = Array.isArray(cards) ? cards : [];
  if (!job || !list.length) return '先に「AI画像生成」で画像を作ってください (全部の画像ができてから作れます)';
  if (job.status === 'queued' || job.status === 'running') return 'いま画像を作っています。全部できてから作れます';
  if (list.some((c) => c.pending)) return '作り直している画像があります。できてから作れます';
  const missing = list.filter((c) => !c.current || !c.current.drive_file_id);
  if (missing.length) return `作れなかった画像があります (${missing.map((c) => imageLabel(c)).join('・')})。「再生成」で作ってから押してください`;
  return null;
}

/**
 * 今のシート (タブの値) から、人が書いた修正指示を読み戻す。
 * 列は見出しの文字で探す (人が列を足しても・並べ替えても読める)。
 * 値は FORMULA で読む (画像の列の =IMAGE("…/d/<ファイル ID>") を読んで、その行に出ている画像を知るため)。
 *
 * 🚨 管理番号のセルだけを信じない (Codex PR-F 名指し1 高): 管理番号は人が書き換えられるので、
 *    その行の =IMAGE が出している画像ファイルが、管理番号の画像 (の版のどれか) のものかを rootOfFile で照らす。
 *    合わなければ (管理番号を入れ替えた・別の番号を書いた・画像のセルを消した) その行の修正指示は「行き先なし」に残す
 *    (別の画像の行に付けない)。行ごと並べ替えた (管理番号と画像が一緒に動いた) ときは合うので、同じ画像の行に戻る
 * @param {string[][]|null} values  タブの値 (FORMULA)。null = タブが無い (作ったばかり・人がタブを消した)
 * @param {{rootOfFile?: (fileId: string) => number|null}} [opts]  画像ファイル → 元の行の ID (この商品の画像だけ)。無ければ照らさない
 * @returns {{ok: true, byKey: Map<number, {note, version, label, row}>, orphans: Array<{label, note, version}>, layout: object|null}|{ok: false, error: string}}
 *   byKey = 管理番号の画像の行 (元の行の ID → 修正指示・何行目か)。orphans = 行き先の無いもの (前の「前の依頼書の修正指示」の行も含む)。
 *   修正指示が空の行は byKey / orphans に入れない (layout.rowOfKey には入れる = その行の場所は分かる)。
 *   layout = 見出しの行・列の位置と、管理番号が確かめられた行 (作り直しで修正指示のセルに触らずに済むかを決める)
 */
export function readBackNotes(values, { rootOfFile = null } = {}) {
  const byKey = new Map();
  const orphans = [];
  if (values == null) return { ok: true, byKey, orphans, layout: null };
  const rows = Array.isArray(values) ? values.map((r) => (Array.isArray(r) ? r.map(str) : [])) : [];
  // 見出しの行 = 「修正指示」「管理番号」「AI生成画像」がそろった行 (修正指示の本文に「修正指示」と書かれた行を見出しと取り違えない — Codex PR-F 名指し4 中)
  const headerIdx = rows.findIndex((r) => r.some((c) => c.trim() === H_NOTE) && r.some((c) => c.trim().startsWith(H_KEY_PREFIX)) && r.some((c) => c.trim() === H_IMAGE));
  if (headerIdx < 0) {
    // 人が書いたものがあるかもしれないのに、どこが修正指示か分からない → 上書きしない
    if (rows.some((r) => r.some((c) => c.trim()))) {
      return { ok: false, error: `デザイナー修正依頼書の「${H_NOTE}」の見出しの行が見つかりません (見出しを消した・書き換えた)。書いた内容を消さないため、作り直していません。見出しの行を元に戻すか、要らなければタブ「${DESIGNER_TAB}」の名前を変えてから押してください` };
    }
    return { ok: true, byKey, orphans, layout: null };
  }
  const head = rows[headerIdx];
  // 見出しが 2 つある列 (人が同じ名前の列を足した) は、どちらが本物か分からない → 上書きしない (Codex PR-F 名指し2 高)
  const dup = [(c) => c === H_NOTE, (c) => c.startsWith(H_KEY_PREFIX), (c) => c === H_IMAGE, (c) => c === H_VERSION]
    .find((pred) => head.filter((c) => pred(c.trim())).length > 1);
  if (dup) {
    return { ok: false, error: `デザイナー修正依頼書の見出しの行に、同じ名前の列が 2 つあります (「${H_NOTE}」「${H_KEY_PREFIX}」「${H_IMAGE}」「${H_VERSION}」は 1 つずつ)。書いた内容を消さないため、作り直していません。足した列の見出しの名前を変えてから押してください` };
  }
  const col = (pred, fallback) => { const i = head.findIndex((c) => pred(c.trim())); return i >= 0 ? i : fallback; };
  const noteCol = col((c) => c === H_NOTE, -1);
  const keyCol = col((c) => c.startsWith(H_KEY_PREFIX), -1);
  const verCol = col((c) => c === H_VERSION, -1);
  const labelCol = col((c) => c === H_LABEL, 0);
  const imageCol = col((c) => c === H_IMAGE, -1);
  // 管理番号が確かめられた行 (元の行の ID → 何行目か。最初の 1 行だけ)
  const rowOfKey = new Map();
  let inOrphans = false;
  rows.forEach((r, idx) => {
    if (idx <= headerIdx) return;
    const label = str(r[labelCol]);
    if (label.startsWith(ORPHAN_HEADING_HEAD)) { inOrphans = true; return; }
    const note = str(r[noteCol]);
    const version = verCol >= 0 ? str(r[verCol]) : '';
    const m = !inOrphans && keyCol >= 0 ? KEY_RE.exec(str(r[keyCol]).trim()) : null;
    let id = m ? Number(m[1]) : null;
    if (id && rootOfFile) {
      const fm = imageCol >= 0 ? IMAGE_FILE_RE.exec(str(r[imageCol])) : null;
      if (!fm || rootOfFile(fm[1]) !== id) id = null;
    }
    // 同じ管理番号が 2 行 (人が行をコピーした) → 2 つ目からは行き先なしに残す (片方を黙って捨てない)
    const first = id && !rowOfKey.has(id);
    if (first) rowOfKey.set(id, idx);
    if (!note.trim()) return;
    if (first) byKey.set(id, { note, version, label, row: idx });
    else orphans.push({ label, note, version });
  });
  return { ok: true, byKey, orphans, layout: { headerIdx, noteCol, rowOfKey, rowCount: rows.length, colCount: Math.max(0, ...rows.map((r) => r.length)) } };
}

const verNo = (s) => { const m = /^v(\d+)/.exec(str(s).trim()); return m ? Number(m[1]) : null; };
const noteVerNo = (s) => { const m = /修正指示は v(\d+)/.exec(str(s)); return m ? Number(m[1]) : null; };

/**
 * 版の欄。修正指示が前の版の画像に書かれたもの (書いた後で画像を作り直した) なら、そう添える
 * (指示が新しい画像にも当てはまるかは人が見る)。
 * 前のシートの版の欄が「今と同じ版」なら、前に添えた注記をそのまま引き継ぐ (人が注記を消せば消えたまま)
 */
export function versionCell(version, prev) {
  const v = Number(version) || 1;
  let noteVer = null;
  if (prev && str(prev.note).trim()) {
    const oldVer = verNo(prev.version);
    const oldNoteVer = noteVerNo(prev.version);
    noteVer = oldVer === v ? oldNoteVer : (oldNoteVer ?? oldVer);
  }
  return noteVer && noteVer !== v ? `v${v} (修正指示は v${noteVer} の画像に書いたもの)` : `v${v}`;
}

/**
 * シートの中身を組む。
 * @param {object} o
 * @param {string} o.productCode
 * @param {string} o.productName
 * @param {Array<{root_id, current_id, no, seq, role, title, version, drive_file_id}>} o.images  TOP から順 (作った順)
 * @param {{byKey: Map, orphans: Array, layout: object|null}|null} [o.previous]  readBackNotes の結果 (作り直すとき)
 * @returns {{title: string, tabs: Array<{name, rows, format, clearRows?, clearCols?}>, carried: number, orphaned: number, kept: number}}
 *   carried = 同じ画像の行に戻した修正指示の数 / orphaned = 「前の依頼書の修正指示」に残した数 /
 *   kept = 修正指示のセルに触らずに残した行の数 (下)
 *
 * 🚨 読み戻してから Google に送るまでの間 (数秒) に人が書いた修正指示を消さない (Codex PR-F 名指し1 高):
 *    前のシートで、その画像が**同じ行**・修正指示が**同じ列**にある (見出しの位置も同じ) なら、その修正指示のセルは書かない
 *    (KEEP = 送らない = いまシートにある値のまま)。1 枚だけ作り直した、のようなよくある作り直しはこれで、書いている途中の
 *    修正指示も残る。画像が増えた・減った・並びが変わった・全部作り直したときは行が動くので、読み戻した値で書き直す
 *    (そのときだけ、読み戻した後の数秒に書いた分は残らない — 画面で「作り直しの間はシートを書かないでください」と出す)
 */
export function buildDesignerSheet({ productCode, productName, images, previous = null }) {
  const byKey = previous?.byKey instanceof Map ? previous.byKey : new Map();
  const layout = previous?.layout || null;
  // 前のシートと見出しの行・修正指示の列が同じ (人が列や見出しを動かしていない) ときだけ、セルに触らずに残せる
  const sameFrame = !!layout && layout.headerIdx === TOP_ROWS && layout.noteCol === HEADERS.indexOf(H_NOTE);
  let kept = 0;
  const used = new Set();
  const rows = [
    [cellText(`デザイナー修正依頼書　${str(productCode)}　${str(productName)}`)],
    ['「修正指示」の列に書いてください。ポータルの「最新の画像で作り直す」を押しても、修正指示は同じ画像 (管理番号) の行に残ります (ほかの列は書き直します)。画像が出ないときは「画像を開く」から見てください'],
    HEADERS.slice(),
  ];
  let carried = 0;
  for (const im of images || []) {
    const id = Number(im.root_id);
    const prev = byKey.get(id) || null;
    if (prev) { used.add(id); carried += 1; }
    // この画像が前のシートでも同じ行にある → 修正指示のセルは書かない (いま人が書いている値のまま)
    const keep = sameFrame && layout.rowOfKey.get(id) === rows.length;
    if (keep) kept += 1;
    const role = cellText(im.role, 200);
    const title = cellText(im.title, 300);
    rows.push([
      cellText(`${imageLabel(im)}${role ? '｜' + role : ''}${title ? '\n' + title : ''}`),
      // 式は自分で組んだ URL だけ (ID は imageFormulaUrl / driveViewUrl が形を検査する)
      formula(`=IMAGE("${imageFormulaUrl(im.drive_file_id)}")`),
      keep ? KEEP : (prev ? cellText(prev.note, NOTE_MAX) : ''),
      versionCell(im.version, prev),
      formula(`=HYPERLINK("${driveViewUrl(im.drive_file_id)}", "画像を開く")`),
      imageKey(id),
    ]);
  }
  const imageEnd = rows.length;
  // 行き先の無い修正指示 (画像が無くなった・全部作り直した・管理番号を消された・前から残っていたもの)
  const orphans = [];
  for (const [id, p] of byKey) if (!used.has(id)) orphans.push({ label: p.label, note: p.note, version: p.version });
  for (const p of previous?.orphans || []) orphans.push(p);
  let orphanHead = null;
  if (orphans.length) {
    rows.push([]);
    orphanHead = rows.length;
    rows.push([ORPHAN_HEADING]);
    for (const p of orphans) rows.push([cellText(p.label), '', cellText(p.note, NOTE_MAX), cellText(p.version, 200), '', '']);
  }
  const format = {
    frozenRows: TOP_ROWS + 1,
    wrap: true,
    boldRows: [0, TOP_ROWS, ...(orphanHead != null ? [orphanHead] : [])],
    shadedRows: [TOP_ROWS, ...(orphanHead != null ? [orphanHead] : [])],
    columnWidths: [220, 320, 380, 150, 90, 130],
    rowHeights: imageEnd > TOP_ROWS + 1 ? [{ start: TOP_ROWS + 1, end: imageEnd, px: IMAGE_ROW_PX }] : [],
  };
  return {
    title: designerSheetTitle(productCode),
    // KEEP のあるときは、前のシートの広さまで (KEEP 以外を) 消してから書く (前の版の行が残らない)
    tabs: [{ name: DESIGNER_TAB, rows, format, ...(kept ? { clearRows: layout.rowCount, clearCols: layout.colCount } : {}) }],
    carried,
    orphaned: orphans.length,
    kept,
  };
}
