/**
 * 取りこぼしの見張り (2026-09-04 障害の再発防止)。
 *
 * その日、梱包アプリに 出荷_04/07/11/12 が来ていないことに誰も気づけず、現場が
 * 「出荷7が出てこない」と言うまで約2時間かかった。原因 (Drive の束ね検索) は直したが、
 * **取りこぼしても誰も気づかない**という穴はそれとは別に残る。ここはその穴を塞ぐ。
 *
 * 見張るのは2つ。どちらも「ピッキング側が正」で、梱包側/分類がそれに追いついているかを見る。
 *   1. not_imported    ピッキングに取り込まれているのに、梱包に来ていない出荷グループ
 *   2. class_suggested 引当分類が Drive の引当パターンtxt ではなく CSV からの**推定値**で
 *                      確定している (もっともらしい別の分類が入っている可能性)
 *
 * ⭐同じ件で鳴り続けないよう pk_pack_miss_alerts に1件1回だけ記録する。
 *   ただし**送れたときだけ**印を付ける (送信前に印を付けると webhook が落ちていた分が
 *   永久に鳴らなくなる — 資材通知・印刷通知と同じ方針)。
 */
import { getDB, utcNow, jstToday } from './db.js';

// ピッキング取込から梱包取込までの猶予。納品書CSVはピッキングCSVとほぼ同時に置かれ、
// ポーラーは60秒の安定待ち+2分間隔で拾うので通常は5分以内に入る。
// no_picking のリトライ (2分×15回=30分) を見込んでも 40分あれば「来ていない」と断じてよい
const GRACE_MIN = 40;

/**
 * 鳴らすべき取りこぼしを探す (DBを読むだけ・通知はしない)。
 * @returns {{kind, folderName, workDate, detail, alertKey}[]}
 */
export function findMisses({ workDate = jstToday(), nowMs = Date.now(), graceMin = GRACE_MIN } = {}) {
  const db = getDB();
  const out = [];

  // 1) ピッキングにあるのに梱包に無い。picking 側のテーブルが無い環境 (packing 単体) では何もしない
  let notImported = [];
  try {
    notImported = db.prepare(`
      SELECT b.folder_name, b.slip_count, b.created_at, b.status
      FROM pk_batches b
      WHERE b.work_date = ? AND b.folder_name IS NOT NULL
        AND NOT EXISTS (
          SELECT 1 FROM pk_pack_batches p
          WHERE p.folder_name = b.folder_name AND p.work_date = b.work_date
        )
      ORDER BY b.folder_name
    `).all(workDate);
  } catch { return out; }

  for (const r of notImported) {
    // 取り込まれたばかりのものは「まだ来ていないだけ」なので待つ
    const ageMin = (nowMs - Date.parse(`${r.created_at}${r.created_at.endsWith('Z') ? '' : 'Z'}`)) / 60000;
    if (!(ageMin >= graceMin)) continue;
    out.push({
      kind: 'not_imported',
      folderName: r.folder_name,
      workDate,
      detail: `${r.slip_count}伝票 (ピッキングは${Math.round(ageMin)}分前に取込済み・${r.status})`,
      alertKey: `${workDate}:not_imported:${r.folder_name}`,
    });
  }

  // 2) 引当分類が推定値のまま確定している (class_source が無い古い行は対象外)
  let suggested = [];
  try {
    suggested = db.prepare(`
      SELECT folder_name, hikiate_class FROM pk_batches
      WHERE work_date = ? AND folder_name IS NOT NULL AND class_source = 'suggested'
      ORDER BY folder_name
    `).all(workDate);
  } catch { return out; }

  for (const r of suggested) {
    out.push({
      kind: 'class_suggested',
      folderName: r.folder_name,
      workDate,
      detail: r.hikiate_class,
      alertKey: `${workDate}:class_suggested:${r.folder_name}`,
    });
  }
  return out;
}

/** 未通知のものだけに絞る (既に鳴らしたものは返さない)。 */
export function pendingMisses(misses) {
  const db = getDB();
  const done = db.prepare('SELECT alert_key FROM pk_pack_miss_alerts WHERE notified_at IS NOT NULL');
  const sent = new Set(done.all().map((r) => r.alert_key));
  return misses.filter((m) => !sent.has(m.alertKey));
}

/** 送信できたものを記録する (送れたときだけ呼ぶ)。 */
export function markNotified(miss) {
  const now = utcNow();
  getDB().prepare(`
    INSERT INTO pk_pack_miss_alerts (alert_key, kind, work_date, folder_name, detail, notified_at, created_at)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(alert_key) DO UPDATE SET notified_at = excluded.notified_at, detail = excluded.detail
  `).run(miss.alertKey, miss.kind, miss.workDate, miss.folderName, miss.detail ?? null, now, now);
}

/** GChat 本文 (種類ごとにまとめて1通)。 */
export function buildMissText(kind, misses) {
  if (!misses.length) return null;
  const list = misses.map((m) => `・${m.folderName}  ${m.detail || ''}`.trimEnd()).join('\n');
  if (kind === 'not_imported') {
    return '⚠ *梱包アプリに来ていない出荷グループがあります*\n'
      + `${list}\n`
      + 'ピッキングには入っているのに梱包に来ていません。Drive の納品書CSVが取り込めていない可能性があります。\n'
      + '梱包アプリ → 管理 → 取込 で状態を確認してください。';
  }
  return '⚠ *引当分類が推定値のまま確定しています*\n'
    + `${list}\n`
    + 'Drive の引当パターンtxt が取れず、CSVから推定した分類が入っています。'
    + '**実際の分類と違うことがあります** (梱包機の折り方など)。現物と突き合わせてください。';
}

/**
 * 1周期分の見張り。ポーラーから呼ぶ (fail-soft — ここが落ちても取込は止めない)。
 * @param post GChat送信関数 (text => Promise<boolean>)。null なら送らない
 */
export async function missWatchStep(post, opts = {}) {
  const misses = pendingMisses(findMisses(opts));
  if (!misses.length) return { checked: 0, notified: 0 };
  let notified = 0;
  for (const kind of ['not_imported', 'class_suggested']) {
    const group = misses.filter((m) => m.kind === kind);
    if (!group.length) continue;
    const text = buildMissText(kind, group);
    // 送信先が無い環境では「鳴らせなかった」ことがログに残るようにする (黙って握らない)
    if (!post) {
      console.warn(`[packing-miss-watch] 通知先が未設定のため鳴らせません: ${text.split('\n')[0]} (${group.map((m) => m.folderName).join(',')})`);
      continue;
    }
    try {
      if (await post(text)) {
        for (const m of group) markNotified(m);
        notified += group.length;
        console.log(`[packing-miss-watch] 通知: ${kind} ${group.map((m) => m.folderName).join(',')}`);
      }
    } catch (e) {
      console.warn(`[packing-miss-watch] 通知に失敗 (${kind}): ${e.message}`);
    }
  }
  return { checked: misses.length, notified };
}
