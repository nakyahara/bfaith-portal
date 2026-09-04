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
 * ⭐**検知した時点で pk_pack_miss_alerts に行を作る (outbox)**。送信はそのあと。
 *   「送れたときだけ notified_at を書く」だけだと、webhook が落ちたまま日付を跨いだ異常を
 *   永久に見失う (Codexレビュー High)。行が残っていれば何日後でも送れたときに鳴る。
 * ⭐検知は当日だけでなく直近数日を見る。当日だけだと、猶予 (40分) の途中で日を跨いだバッチが
 *   翌日には当日扱いされず、一度も検知されないまま消える。
 * ⭐通知は at-least-once。送信直後・markNotified 前に落ちれば同じ内容がもう一度鳴る。
 *   「鳴らし損ねない」を優先し、まれな重複は許容する (webhook 側に冪等キーが無いため
 *   exactly-once は作れない — Codexレビュー)。
 */
import { getDB, utcNow, jstToday } from './db.js';

// ピッキング取込から梱包取込までの猶予 (分)。納品書CSVはピッキングCSVとほぼ同時に置かれ、
// ポーラーは60秒の安定待ち+2分間隔で拾うので通常は5分以内に入る。
// no_picking のリトライ (2分×15回=30分) を見込んでも 40分あれば「来ていない」と断じてよい。
// ⭐判定の起点は Drive への配置時刻ではなく pk_batches.created_at (ピッキング取込時刻)
const GRACE_MIN = Number(process.env.PACKING_MISS_GRACE_MIN || 40);
// 検知の対象にする日数。当日だけを見ると**猶予の途中で日を跨いだバッチを永久に見失う**
// (23:40 に取り込まれると 0:00 時点でまだ20分 → 翌日からは当日扱いされない — Codexレビュー High)
const LOOKBACK_DAYS = 3;
// 古すぎる取りこぼしは今さら鳴らさない (この機能を入れた日に過去の分が一斉に鳴るのを防ぐ)
const MAX_AGE_MIN = LOOKBACK_DAYS * 24 * 60;
// 1周期に送る未送信行の上限 (溜まっても1通が巨大にならないように)
const FLUSH_LIMIT = 50;

/**
 * 鳴らすべき取りこぼしを探す (DBを読むだけ)。
 * @returns {{kind, folderName, workDate, detail, alertKey}[]}
 */
export function findMisses({ workDate = null, nowMs = Date.now(), graceMin = GRACE_MIN,
  lookbackDays = LOOKBACK_DAYS, maxAgeMin = MAX_AGE_MIN } = {}) {
  const db = getDB();
  const out = [];
  // 単日指定 (テスト・調査用) と、直近数日の範囲指定を両立させる
  const from = workDate || jstToday(new Date(nowMs - lookbackDays * 86400_000));
  const to = workDate || jstToday(new Date(nowMs));

  // 1) ピッキングにあるのに梱包に無い。
  //    ⭐突合キーは pk_batch_id / tb_no (フォルダ名は日をまたいで使い回される表示用の名前で、
  //      同名の別バッチが「来ている」ことにしてしまう — Codexレビュー)
  //    ⭐取消・無効化されたバッチは対象外 (取消の40分後に誤報を出さない — Codexレビュー)
  //    picking 側のテーブルが無い環境 (packing 単体) では何もしない
  let notImported = [];
  try {
    notImported = db.prepare(`
      SELECT b.id, b.folder_name, b.slip_count, b.created_at, b.status, b.work_date
      FROM pk_batches b
      WHERE b.work_date BETWEEN ? AND ? AND b.folder_name IS NOT NULL
        AND b.validity = 'valid' AND b.status != 'cancelled' AND b.origin != 'repick'
        AND NOT EXISTS (
          SELECT 1 FROM pk_pack_batches p
          WHERE (p.pk_batch_id = b.id OR p.tb_key = b.tb_no) AND p.validity = 'valid'
        )
      ORDER BY b.work_date, b.folder_name
    `).all(from, to);
  } catch { return out; }

  for (const r of notImported) {
    // 取り込まれたばかりのものは「まだ来ていないだけ」なので待つ。
    // 逆に古すぎるものは今さら鳴らさない (この機能の導入日に過去分が一斉に鳴らないように)
    const ageMin = (nowMs - Date.parse(`${r.created_at}${r.created_at.endsWith('Z') ? '' : 'Z'}`)) / 60000;
    if (!(ageMin >= graceMin) || ageMin > maxAgeMin) continue;
    out.push({
      kind: 'not_imported',
      pkBatchId: r.id,
      folderName: r.folder_name,
      workDate: r.work_date,
      detail: `${r.slip_count}伝票 (ピッキングは${Math.round(ageMin)}分前に取込済み・${r.status})`,
      alertKey: `${r.work_date}:not_imported:${r.id}`,
    });
  }

  // 2) 引当分類が推定値のまま確定している (class_source が無い古い行は対象外)
  let suggested = [];
  try {
    suggested = db.prepare(`
      SELECT id, folder_name, hikiate_class, work_date FROM pk_batches
      WHERE work_date BETWEEN ? AND ? AND folder_name IS NOT NULL AND class_source = 'suggested'
        AND validity = 'valid' AND status != 'cancelled' AND origin != 'repick'
      ORDER BY work_date, folder_name
    `).all(from, to);
  } catch { return out; }

  for (const r of suggested) {
    out.push({
      kind: 'class_suggested',
      pkBatchId: r.id,
      folderName: r.folder_name,
      workDate: r.work_date,
      detail: r.hikiate_class,
      alertKey: `${r.work_date}:class_suggested:${r.id}`,
    });
  }
  return out;
}

/**
 * 見つけた異常を outbox に記録する (未送信行として残す)。
 * 既にある行は触らない = 一度鳴らしたものを鳴らし直さない。
 */
export function recordMisses(misses) {
  const db = getDB();
  const now = utcNow();
  const ins = db.prepare(`
    INSERT INTO pk_pack_miss_alerts (alert_key, kind, work_date, pk_batch_id, folder_name, detail, attempts, created_at)
    VALUES (?, ?, ?, ?, ?, ?, 0, ?)
    ON CONFLICT(alert_key) DO NOTHING
  `);
  const tx = db.transaction((rows) => {
    for (const m of rows) ins.run(m.alertKey, m.kind, m.workDate, m.pkBatchId ?? null, m.folderName, m.detail ?? null, now);
  });
  tx(misses);
  return misses.length;
}

/**
 * まだ送れていない異常。
 * ⭐日付では切り捨てない。webhook の障害が長引いたときに未送信行を黙って捨てると、
 *   「何日後でも送れたときに鳴る」という outbox の前提が崩れる (Codexレビュー High)。
 *   件数が膨らむ場合に備えて1周期の通知量だけ LIMIT で抑える (残りは次の周期で送られる)。
 */
export function pendingAlerts({ limit = FLUSH_LIMIT } = {}) {
  return getDB().prepare(`
    SELECT alert_key, kind, work_date, folder_name, detail, attempts, last_error
    FROM pk_pack_miss_alerts
    WHERE notified_at IS NULL
    ORDER BY work_date, kind, folder_name
    LIMIT ?
  `).all(limit);
}

/** 送信できたものを記録する (送れたときだけ呼ぶ)。 */
export function markNotified(alertKey) {
  getDB().prepare('UPDATE pk_pack_miss_alerts SET notified_at = ?, last_error = NULL WHERE alert_key = ?')
    .run(utcNow(), alertKey);
}

/** 送信に失敗したことを残す (次の周期でまた試す)。 */
export function markFailed(alertKeys, message) {
  const db = getDB();
  const upd = db.prepare('UPDATE pk_pack_miss_alerts SET attempts = attempts + 1, last_error = ? WHERE alert_key = ?');
  const tx = db.transaction((keys) => { for (const k of keys) upd.run(String(message).slice(0, 200), k); });
  tx(alertKeys);
}

/** GChat 本文 (種類ごとにまとめて1通)。 */
export function buildMissText(kind, misses) {
  if (!misses.length) return null;
  const list = misses.map((m) => `・${m.work_date ?? m.workDate} ${m.folder_name ?? m.folderName}  ${m.detail || ''}`.trimEnd()).join('\n');
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
 * 1周期分の見張り。ポーラーから呼ぶ。
 * ⭐取込 (pollOnce) が失敗した周期でも必ず呼ぶこと。Drive 障害のときこそ鳴らす必要がある
 *   (Codexレビュー High)。
 * @param post GChat送信関数 (text => Promise<boolean>)。null なら送らない (outbox には残る)
 */
export async function missWatchStep(post, opts = {}) {
  recordMisses(findMisses(opts));
  const pending = pendingAlerts(opts);
  if (!pending.length) return { pending: 0, notified: 0 };
  if (!post) {
    // 送信先が無い環境でも「異常を検知した」事実は outbox に残る。黙って握らない
    console.warn(`[packing-miss-watch] 通知先が未設定のため鳴らせません (未送信 ${pending.length}件: ${pending.map((m) => m.folder_name).join(',')})`);
    return { pending: pending.length, notified: 0 };
  }
  let notified = 0;
  for (const kind of ['not_imported', 'class_suggested']) {
    const group = pending.filter((m) => m.kind === kind);
    if (!group.length) continue;
    const text = buildMissText(kind, group);
    try {
      if (await post(text)) {
        for (const m of group) markNotified(m.alert_key);
        notified += group.length;
        console.log(`[packing-miss-watch] 通知: ${kind} ${group.map((m) => m.folder_name).join(',')}`);
      } else {
        // false = 送信先が無い等で送れなかった。例外と同じく失敗として残す (Codexレビュー)
        markFailed(group.map((m) => m.alert_key), '送信されませんでした (webhook 未設定または拒否)');
      }
    } catch (e) {
      markFailed(group.map((m) => m.alert_key), e.message);
      console.warn(`[packing-miss-watch] 通知に失敗 (${kind}): ${e.message}`);
    }
  }
  return { pending: pending.length, notified };
}
