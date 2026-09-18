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
 * 紐づけの鍵:
 *   写真は届いた現物の商品コード (子SKU) で保存される。ドラフトの ne_code は
 *   バリエーションなら **代表商品コード** なので、そのままでは一致しない。
 *   そこで「写真の商品コード → mirror_products.代表商品コード」を引いてグループキーに寄せる。
 *   加えて ph_ne_seen_codes (自動取込が「このコードはこのドラフト」と記録したもの) も見る。
 */
import { fileViewUrl } from '../lib/drive-link.js';

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

/**
 * このドラフトに紐づく裏面写真 (新しい順)。
 * @returns {Array<{id, product_id, product_name, status, drive_url, created_at, ar_no}>}
 */
export function backLabelPhotosForDraft(db, draft) {
  if (!draft || !backLabelsAvailable(db)) return [];
  const keys = new Set();
  const add = (v) => { const k = norm(v); if (k) keys.add(k); };
  add(draft.ne_code);
  // ① バリエーションの子SKU (代表コード配下)。現物は子SKU で届く
  try {
    for (const r of db.prepare('SELECT 商品コード FROM mirror_products WHERE LOWER(TRIM(代表商品コード)) = ?').all(norm(draft.ne_code))) {
      add(r.商品コード);
    }
  } catch { /* mirror 未作成なら ne_code だけで引く */ }
  // ② 自動取込が「このコードはこのドラフト」と覚えたもの (代表コードに寄せる前の本コード)
  try {
    for (const r of db.prepare('SELECT code_key FROM ph_ne_seen_codes WHERE draft_id = ?').all(draft.id)) add(r.code_key);
  } catch { /* 表が無ければ飛ばす */ }
  if (keys.size === 0) return [];
  const list = [...keys];
  const rows = db.prepare(`SELECT * FROM f_inbound_check_back_labels
    WHERE ${ALIVE} AND code_key IN (${list.map(() => '?').join(',')})
    ORDER BY id DESC`).all(...list);
  return rows.map(publicPhoto);
}

/** その写真がこのドラフトのものか (配信 API の認可。他の商品の写真を覗かせない) */
export function photoBelongsToDraft(db, draft, photoId) {
  const id = Number(photoId);
  if (!Number.isInteger(id)) return false;
  return backLabelPhotosForDraft(db, draft).some((p) => p.id === id);
}

/**
 * 工程ボードのカードに出す「裏面写真があるか」。
 * ⭐ボードは 1 回で最大 800 枚のカードを描くので、カードごとに引かず **1 クエリで Map** にする。
 *   写真の台帳は多くても年に数百行なので、全件を読んで代表コードに寄せるのが一番安い。
 * @returns {Map<string, number>} グループキー (= ドラフトの ne_code を正規化したもの) → 枚数
 */
export function backLabelCountsByGroup(db) {
  const out = new Map();
  if (!backLabelsAvailable(db)) return out;
  let rows;
  try {
    rows = db.prepare(`
      SELECT LOWER(TRIM(COALESCE(NULLIF(TRIM(p.代表商品コード), ''), b.code_key))) AS group_key,
             COUNT(*) AS c
        FROM f_inbound_check_back_labels b
        LEFT JOIN mirror_products p ON LOWER(TRIM(p.商品コード)) = b.code_key
       WHERE b.deleted_at IS NULL AND b.missing_file_at IS NULL
       GROUP BY group_key`).all();
  } catch {
    // mirror_products がまだ無い環境では代表コードに寄せずに数える
    rows = db.prepare(`SELECT code_key AS group_key, COUNT(*) AS c
      FROM f_inbound_check_back_labels WHERE ${ALIVE} GROUP BY code_key`).all();
  }
  for (const r of rows) if (r.group_key) out.set(r.group_key, Number(r.c) || 0);
  return out;
}
