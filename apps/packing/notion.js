/**
 * ⑤ Notion カード自動移動 — 梱包工程 (要件§5.10・恒久機能)。
 *
 * picking/notion.js の createNotionCardSync (共通部品) の梱包インスタンス。
 * 「スタッフ用デイリー業務」DB (picking と同じ) のカード = 出荷_XX×当日 を動かす:
 *   手梱包: 梱包開始 → 「梱包作業中」+梱包担当者 / 全伝票完了 → 「完了」 /
 *           完了取消で再開 → 「梱包作業中」 / 中断・バッチ取消 → カードは触らない (中原さん決定 2026-08-18)
 *
 * - DB (packing) が正本・Notion は投影。失敗しても梱包作業は止めない (fail-soft)
 * - 時間プロパティ (梱包開始/梱包終了/梱包時間(分)/秒/伝票) は Notion 側に存在するものだけ書く
 * - トークン: PACKING_NOTION_TOKEN 優先。未設定なら PICKING_NOTION_TOKEN を共用
 *   (同一DB・同一プロセスのため明示フォールバック。分けたくなったら専用トークンを設定するだけ)
 */
import { createNotionCardSync, DAILY_DB_ID } from '../picking/notion.js';
import { getDB } from './db.js';

export const STATUS_PACKING = '梱包作業中';
export const STATUS_PACK_DONE = '完了';

const sync = createNotionCardSync({
  dbId: DAILY_DB_ID,
  tokenEnvs: ['PACKING_NOTION_TOKEN', 'PICKING_NOTION_TOKEN'],
  logPrefix: 'packing-notion',
  workerPropName: '梱包担当者',
  optionalProps: {
    startProp: { name: '梱包開始', type: 'date' },
    endProp: { name: '梱包終了', type: 'date' },
    minutesProp: { name: '梱包時間(分)', type: 'number' },
    secPerLineProp: { name: '秒/伝票', type: 'number' },
  },
});

/**
 * バッチ行 → Notion へ送る状態 (純関数・テスト対象)。
 * label=null はカードを触らない (ready/cancelled/invalid — 取消しても人の運用に任せる)。
 * activeSec = 実働秒 (中断時間を除外)。
 */
export function packBatchNotionState(batch) {
  if (!batch || batch.validity !== 'valid') return null;
  let label = null;
  if (batch.status === 'packing' || batch.status === 'paused') label = STATUS_PACKING;
  else if (batch.status === 'done') label = STATUS_PACK_DONE;
  else return null;
  const done = batch.status === 'done';
  let activeSec = null;
  if (done && batch.started_at && batch.finished_at) {
    activeSec = Math.max(0, Math.round(
      (Date.parse(batch.finished_at) - Date.parse(batch.started_at)) / 1000
    ) - (batch.paused_total_sec || 0));
  }
  return {
    folderName: batch.folder_name,
    workDate: batch.work_date,
    label,
    workerName: batch.worker,
    times: {
      startedAt: batch.started_at,
      finishedAt: done ? batch.finished_at : null,
      activeSec,
      lineCount: batch.slip_count,
    },
  };
}

/**
 * バッチの現在状態を Notion へ非同期反映 (fire-and-forget・バッチ単位で直列化)。
 * 呼び出し側はイベント適用後に叩くだけ。状態は送信直前に読み直すため到着順の逆転がない。
 */
export function enqueuePackBatchNotionSync(batchId) {
  sync.enqueueBatchSync(batchId, () => {
    const batch = getDB().prepare('SELECT * FROM pk_pack_batches WHERE id = ?').get(batchId);
    return packBatchNotionState(batch);
  });
}
