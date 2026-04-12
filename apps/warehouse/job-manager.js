/**
 * ジョブマネージャー
 * 長時間処理（>10秒）をジョブ化して進捗を追跡する
 */
import { randomUUID } from 'crypto';

// インメモリジョブストア（再起動で消えるが、短期ジョブには十分）
const jobs = new Map();

// 古いジョブを自動クリーンアップ（1時間以上経過）
const CLEANUP_INTERVAL = 10 * 60 * 1000; // 10分ごと
const MAX_AGE = 60 * 60 * 1000;          // 1時間

setInterval(() => {
  const now = Date.now();
  for (const [id, job] of jobs) {
    if (now - job.createdAt > MAX_AGE) {
      jobs.delete(id);
    }
  }
}, CLEANUP_INTERVAL);

/**
 * ジョブを作成して非同期で実行する
 * @param {string} type - ジョブ種別（例: 'fba-replenishment'）
 * @param {Function} fn - async function(updateProgress) { ... return result; }
 * @returns {{ jobId: string, status: string }}
 */
export function createJob(type, fn) {
  const jobId = randomUUID();
  const job = {
    jobId,
    type,
    status: 'running',
    progress: null,
    result: null,
    error: null,
    createdAt: Date.now(),
    updatedAt: Date.now(),
  };
  jobs.set(jobId, job);

  // 非同期で実行
  const updateProgress = (progress) => {
    job.progress = progress;
    job.updatedAt = Date.now();
  };

  fn(updateProgress)
    .then((result) => {
      job.status = 'completed';
      job.result = result;
      job.updatedAt = Date.now();
    })
    .catch((err) => {
      job.status = 'failed';
      job.error = { code: err.code || 'UNKNOWN', message: err.message };
      job.updatedAt = Date.now();
      console.error(`[JobManager] Job ${jobId} (${type}) failed:`, err.message);
    });

  return { jobId, status: 'running' };
}

/**
 * ジョブの状態を取得
 * @param {string} jobId
 * @returns {object|null}
 */
export function getJob(jobId) {
  const job = jobs.get(jobId);
  if (!job) return null;
  return {
    jobId: job.jobId,
    type: job.type,
    status: job.status,
    progress: job.progress,
    result: job.status === 'completed' ? job.result : undefined,
    error: job.status === 'failed' ? job.error : undefined,
    createdAt: new Date(job.createdAt).toISOString(),
    updatedAt: new Date(job.updatedAt).toISOString(),
  };
}

/**
 * 実行中のジョブ一覧
 */
export function listJobs() {
  const result = [];
  for (const job of jobs.values()) {
    result.push({
      jobId: job.jobId,
      type: job.type,
      status: job.status,
      progress: job.progress,
      createdAt: new Date(job.createdAt).toISOString(),
      updatedAt: new Date(job.updatedAt).toISOString(),
    });
  }
  return result.sort((a, b) => b.createdAt.localeCompare(a.createdAt));
}
