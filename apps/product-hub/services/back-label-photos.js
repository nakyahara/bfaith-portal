/**
 * 入荷受付チェックで撮った「パッケージ裏面ラベル」の写真を、商品登録の画面で使う
 * (2026-09-18 中原さん指示)。
 *
 * 背景: 「基本情報入力」は、成分表示・原材料・内容量など **実物を見ないと埋まらない** 項目が
 *   あると「パッケージ裏面の確認待ち」(CHECKING_REASONS.package_label) で止まる。
 *   現物が社内を通るのは入荷のときだけなので、入荷受付チェック (iPad) で裏面を撮っておき、
 *   ここでその写真を見ながら書けるようにする。
 *
 * 写真の正本は入荷受付チェック側の f_inbound_check_back_labels (同じ warehouse-mirror.db)。
 * このファイルは **読むだけ** — 撮る・消すは入荷の現場でしか起きない。
 *
 * ⭐紐づけの規則は1か所 (codeKeysForDraft / groupKeyOfPhotoCode) にまとめる。
 *   一覧・配信の認可・AI文字起こし・ボードのバッジが**同じ規則**を見ないと、
 *   「詳細には出るのにバッジが出ない」「外したはずの SKU の写真が親に混ざる」が起きる。
 *
 *   写真は届いた現物の商品コード (子SKU) で保存される。ドラフトの ne_code は
 *   バリエーションなら **代表商品コード** なので、そのままでは一致しない:
 *     ① ドラフト自身の ne_code
 *     ② その代表コード配下の子SKU。ただし **このドラフトから外した SKU は除く**
 *        (draft_variation_exclusions。外した SKU は単独ページになるので親の写真ではない)
 *     ③ ph_ne_seen_codes (自動取込が「このコードはこのドラフト」と覚えたもの) は **補助**。
 *        取込履歴は ON CONFLICT DO NOTHING で残り続けるので、代表コードが変わった後も
 *        古い紐づけが残る。**いまの商品マスタと矛盾しないときだけ**使う
 */
import { fileViewUrl } from '../lib/drive-link.js';
import { resolveVariationGroup } from '../lib/variation.js';

const norm = (v) => String(v == null ? '' : v).trim().toLowerCase();

function tableExists(db, name) {
  return !!db.prepare("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?").get(name);
}

/**
 * 「まだ見られる写真」の条件 — 入荷受付チェック側の ALIVE と同じ規則。
 *   deleted_at      = 撮り直しで消した
 *   missing_file_at = 実体を失って Drive にも無い (開いても壊れた画像になる)
 * どちらも画面に出さない・数えない。
 */
const ALIVE = 'deleted_at IS NULL AND missing_file_at IS NULL';

/** 入荷受付チェックがまだ一度も動いていない環境では表が無い (機能ごと静かに無効でよい) */
export function backLabelsAvailable(db) {
  return tableExists(db, 'f_inbound_check_back_labels');
}

function publicPhoto(r) {
  return {
    id: r.id,
    product_id: r.product_id,
    product_name: r.product_name || null,
    // Drive へ送る前 (status='stored') でも画面では見られる (配信はサーバーのローカル実体から)
    status: r.status,
    drive_url: r.drive_file_id ? fileViewUrl(r.drive_file_id) : null,
    created_at: r.created_at,
    ar_no: r.ar_no || null,
  };
}

/** その商品コードが、いま商品マスタの上でどのグループに属するか (代表コード or 自分自身) */
function currentGroupOf(db, codeKey) {
  try {
    const r = db.prepare(`SELECT 代表商品コード AS rep FROM mirror_products
      WHERE LOWER(TRIM(商品コード)) = ?`).get(codeKey);
    if (!r) return null;                                   // 商品マスタに無い
    const rep = norm(r.rep);
    return rep || codeKey;
  } catch { return null; }                                 // mirror 未作成
}

/**
 * その商品コードは **このドラフトから** バリエーションを外されたか。
 * ⚠「どれかのドラフトが外したか」では見ない — 旧親で外した SKU がマスタ変更で新親へ移ったとき、
 *   新親から見れば外していないので出すべき。バッジ側 (excluded_by_rep) と同じ範囲にそろえる
 */
function excludedByDraft(db, draftId, codeKey) {
  try {
    return !!db.prepare(`SELECT 1 FROM draft_variation_exclusions
      WHERE draft_id = ? AND LOWER(TRIM(ne_code)) = ?`).get(draftId, codeKey);
  } catch { return false; }
}

/**
 * このドラフトの写真として扱ってよい商品コードの集合。
 * 一覧・配信の認可・AI文字起こしが**必ずこれを通る** (規則を1か所にする)。
 * @returns {string[]} 小文字化した商品コード
 */
export function codeKeysForDraft(db, draft) {
  const keys = new Set();
  const add = (v) => { const k = norm(v); if (k) keys.add(k); };
  if (!draft) return [];
  add(draft.ne_code);
  // ① バリエーションの子SKU。**このドラフトから外した SKU は入らない**
  //    (resolveVariationGroup が draft_variation_exclusions を見て members / excludedMembers に分ける)
  // 🚨**自分がそのグループの代表のときだけ**引き受ける (Codex PR2 R2 #1)。
  //    resolveVariationGroup は子コードからでも「いまの代表」まで遡るので、
  //    このドラフトのコードが別の代表の子になっていると、兄弟SKU の写真まで混ざる
  try {
    const v = resolveVariationGroup(db, draft.ne_code, { withMembers: true, draftId: draft.id });
    if (norm(v.groupKey) === norm(draft.ne_code)) {
      for (const m of (v.members || [])) add(m.商品コード);
    }
  } catch { /* mirror 未作成なら ne_code だけで引く */ }
  // ② 取込履歴は補助。**いまの商品マスタと矛盾しないときだけ**使う
  //    (代表コードが変わった後も履歴は残るので、そのまま信じると別商品の写真が混ざる)
  const self = norm(draft.ne_code);
  try {
    for (const r of db.prepare('SELECT code_key FROM ph_ne_seen_codes WHERE draft_id = ?').all(draft.id)) {
      const k = norm(r.code_key);
      if (!k || keys.has(k)) continue;
      const g = currentGroupOf(db, k);
      // 商品マスタに無いコード (ロジザードにだけある等) は矛盾しようがないので通す。
      // マスタにあるなら、いまの所属がこのドラフトと同じときだけ通す
      if (g === null || (g === self && !excludedByDraft(db, draft.id, k))) add(k);
    }
  } catch { /* 表が無ければ飛ばす */ }
  return [...keys];
}

/**
 * このドラフトに紐づく裏面写真 (新しい順)。
 * @returns {Array<{id, product_id, product_name, status, drive_url, created_at, ar_no}>}
 */
export function backLabelPhotosForDraft(db, draft) {
  if (!draft || !backLabelsAvailable(db)) return [];
  const list = codeKeysForDraft(db, draft);
  if (list.length === 0) return [];
  const rows = [];
  // IN 句は SQLite の上限 (既定 999) があるので分割する (バリエーションが多い商品でも落ちない)
  for (let i = 0; i < list.length; i += 500) {
    const chunk = list.slice(i, i + 500);
    rows.push(...db.prepare(`SELECT * FROM f_inbound_check_back_labels
      WHERE ${ALIVE} AND code_key IN (${chunk.map(() => '?').join(',')})
      ORDER BY id DESC`).all(...chunk));
  }
  rows.sort((a, b) => b.id - a.id);
  return rows.map(publicPhoto);
}

/** その写真がこのドラフトのものか (配信 API の認可。他の商品の写真を覗かせない) */
export function photoBelongsToDraft(db, draft, photoId) {
  const id = Number(photoId);
  if (!Number.isSafeInteger(id) || id <= 0) return false;
  return backLabelPhotosForDraft(db, draft).some((p) => p.id === id);
}

/**
 * 工程ボードのカードに出す「裏面写真があるか」。
 * ⭐ボードは 1 回で最大 800 枚のカードを描くので、カードごとに引かず **1 クエリで Map** にする。
 *   写真の台帳は多くても年に数百行なので、全件を読んでグループキーに寄せるのが一番安い。
 * ⚠寄せ方は codeKeysForDraft と**同じ規則** — 違うとバッジと詳細で食い違う (Codex #3)。
 * @returns {Map<string, number>} グループキー (= ドラフトの ne_code を正規化したもの) → 枚数
 */
export function backLabelCountsByGroup(db) {
  const out = new Map();
  if (!backLabelsAvailable(db)) return out;
  let rows;
  try {
    rows = db.prepare(`
      SELECT b.code_key,
             COUNT(*) AS c,
             LOWER(TRIM(COALESCE(p.代表商品コード, ''))) AS rep,
             CASE WHEN p.商品コード IS NULL THEN 0 ELSE 1 END AS in_master,
             ${/* 🚨除外は「いまの代表のドラフトが外したか」だけを見る (Codex PR2 R2 #2)。
                  どのドラフトの除外でも外すと、旧親で外した SKU がマスタ変更で新親へ移ったとき、
                  詳細には出るのにバッジが出ない */''}
             EXISTS (SELECT 1 FROM draft_variation_exclusions x
                       JOIN product_drafts d ON d.id = x.draft_id
                      WHERE LOWER(TRIM(x.ne_code)) = b.code_key
                        AND LOWER(TRIM(d.ne_code)) = LOWER(TRIM(COALESCE(p.代表商品コード, '')))) AS excluded_by_rep,
             (SELECT LOWER(TRIM(d.ne_code)) FROM ph_ne_seen_codes s
                JOIN product_drafts d ON d.id = s.draft_id
               WHERE s.code_key = b.code_key) AS seen_group
        FROM f_inbound_check_back_labels b
        LEFT JOIN mirror_products p ON LOWER(TRIM(p.商品コード)) = b.code_key
       WHERE b.${ALIVE}
       GROUP BY b.code_key`).all();
  } catch {
    // mirror_products / 除外表がまだ無い環境では、写真のコードそのままで数える
    try {
      rows = db.prepare(`SELECT code_key, COUNT(*) AS c, '' AS rep, 0 AS in_master, 0 AS excluded_by_rep, NULL AS seen_group
        FROM f_inbound_check_back_labels WHERE ${ALIVE} GROUP BY code_key`).all();
    } catch { return out; }
  }
  // ⭐1枚の写真は**2つのカードに出うる** (その商品コード自身のカードと、代表コードのカード)。
  //   codeKeysForDraft と裏表になるよう、当てはまるキー全部に足す —
  //   片方だけにすると「詳細には出るのにバッジが出ない」が起きる (Codex PR2 R2 #2)
  const bump = (key, n) => { const k = norm(key); if (k) out.set(k, (out.get(k) || 0) + n); };
  for (const r of rows) {
    const code = norm(r.code_key);
    if (!code) continue;
    const n = Number(r.c || 0);
    bump(code, n);                                        // そのコード自身のカード
    if (r.rep && !r.excluded_by_rep) bump(r.rep, n);      // 代表コードのカード (外されていなければ)
    // マスタに無いコードだけ、取込履歴のドラフトにも出る (codeKeysForDraft の補助と同じ)
    if (!r.in_master && r.seen_group && norm(r.seen_group) !== code) bump(r.seen_group, n);
  }
  return out;
}
