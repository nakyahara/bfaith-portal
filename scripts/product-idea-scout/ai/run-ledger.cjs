'use strict';

// W07: AI実行の再開・二重実行防止用の小さな永続台帳。
// プロンプト、応答本文、認証情報、社内原価は保存しない。
const crypto = require('node:crypto');
const fs = require('node:fs');
const path = require('node:path');
const { requireValue: check } = require('./common.cjs');
const { RunBudget } = require('./budget.cjs');

const RUN_ID = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,80}$/;
const INPUT_HASH = /^[a-f0-9]{64}$/;
const TERMINAL = new Set(['completed', 'partial', 'blocked', 'failed']);

function validDeadline(value) { return Number.isFinite(Date.parse(value)); }
function safeRunId(value) { check(typeof value === 'string' && RUN_ID.test(value), 'INVALID_RUN_ID'); return value; }
function statePath(directory, runId) { return path.join(directory, `${safeRunId(runId)}.json`); }
function lockPath(directory, runId) { return path.join(directory, `${safeRunId(runId)}.lock`); }
function readJson(file, missing = null) {
  try { return JSON.parse(fs.readFileSync(file, 'utf8')); }
  catch (error) { if (error.code === 'ENOENT') return missing; const e = new Error('RUN_STATE_CORRUPT'); e.code = 'RUN_STATE_CORRUPT'; throw e; }
}
function writeAtomic(file, value) {
  const temporary = `${file}.${process.pid}.${crypto.randomUUID()}.writing`;
  try {
    fs.writeFileSync(temporary, JSON.stringify(value, null, 2) + '\n', { encoding: 'utf8', flag: 'wx' });
    fs.renameSync(temporary, file);
  } finally {
    try { if (fs.existsSync(temporary)) fs.unlinkSync(temporary); } catch {}
  }
}
function safeResult(result = {}) {
  return {
    status: typeof result.status === 'string' ? result.status : 'UNKNOWN',
    requested_model: typeof result.requested_model === 'string' ? result.requested_model : null,
    actual_model: typeof result.actual_model === 'string' ? result.actual_model : null,
    effort: typeof result.effort === 'string' ? result.effort : null,
    usage: result.usage && typeof result.usage === 'object' ? {
      kind: result.usage.kind || null,
      input_tokens: Number.isFinite(result.usage.input_tokens) ? result.usage.input_tokens : null,
      output_tokens: Number.isFinite(result.usage.output_tokens) ? result.usage.output_tokens : null,
    } : null,
  };
}

class RunSession {
  constructor(ledger, state, lock) { this.ledger = ledger; this.state = state; this.lock = lock; this.closed = false; }
  persist() {
    check(!this.closed, 'RUN_SESSION_CLOSED');
    this.state.updated_at = new Date().toISOString();
    writeAtomic(statePath(this.ledger.directory, this.state.run_id), this.state);
  }
  saveBudget(snapshot) {
    check(snapshot && snapshot.run_id === this.state.run_id && snapshot.deadline === this.state.deadline, 'BUDGET_STATE_MISMATCH');
    check(Array.isArray(snapshot.calls), 'INVALID_BUDGET_STATE');
    this.state.budget = structuredClone(snapshot);
    this.persist();
  }
  budget() { return new RunBudget({ run_id: this.state.run_id, deadline: this.state.deadline, state: this.state.budget }); }
  recordStage(stage, result) {
    check(typeof stage === 'string' && stage, 'INVALID_STAGE');
    const event = { stage, at: new Date().toISOString(), ...safeResult(result) };
    this.state.stage_results = [...this.state.stage_results, event].slice(-40);
    this.persist();
  }
  finish(status, reason = null) {
    check(TERMINAL.has(status), 'INVALID_RUN_STATUS');
    this.state.status = status;
    this.state.finish_reason = typeof reason === 'string' ? reason.slice(0, 120) : null;
    this.state.finished_at = new Date().toISOString();
    this.persist();
    this.release();
  }
  release() {
    if (this.closed) return;
    const current = readJson(lockPath(this.ledger.directory, this.state.run_id));
    if (current?.lock_id === this.lock.lock_id) fs.unlinkSync(lockPath(this.ledger.directory, this.state.run_id));
    this.closed = true;
  }
}

class RunLedger {
  constructor(directory, { now = () => Date.now() } = {}) {
    check(typeof directory === 'string' && path.isAbsolute(directory), 'RUN_DIRECTORY_REQUIRED');
    this.directory = directory; this.now = now;
  }
  inspect(runId) {
    const run = readJson(statePath(this.directory, runId));
    const lock = readJson(lockPath(this.directory, runId));
    return { run, lock };
  }
  acquire({ run_id, target_date, deadline, input_hash, input_version, model_plan = {}, budget_profile = 'default' }) {
    safeRunId(run_id);
    check(/^\d{4}-\d{2}-\d{2}$/.test(target_date), 'INVALID_TARGET_DATE');
    check(validDeadline(deadline), 'DEADLINE_REQUIRED');
    check(typeof input_hash === 'string' && INPUT_HASH.test(input_hash), 'INVALID_INPUT_HASH');
    check(typeof input_version === 'string' && input_version.length <= 80, 'INVALID_INPUT_VERSION');
    fs.mkdirSync(this.directory, { recursive: true });
    const lockFile = lockPath(this.directory, run_id);
    const lock = { lock_id: crypto.randomUUID(), run_id, acquired_at: new Date(this.now()).toISOString(), pid: process.pid };
    try { fs.writeFileSync(lockFile, JSON.stringify(lock) + '\n', { encoding: 'utf8', flag: 'wx' }); }
    catch (error) { if (error.code === 'EEXIST') { const e = new Error('RUN_LOCKED'); e.code = 'RUN_LOCKED'; throw e; } throw error; }
    let state;
    try {
      state = readJson(statePath(this.directory, run_id));
      if (state) {
        check(state.input_hash === input_hash && state.deadline === deadline, 'RUN_INPUT_MISMATCH');
        if (TERMINAL.has(state.status)) { const e = new Error('RUN_FINALIZED'); e.code = 'RUN_FINALIZED'; throw e; }
        if (state.budget?.calls?.some((call) => call.status === 'reserved')) {
          state.status = 'blocked'; state.finish_reason = 'UNRESOLVED_RESERVATION'; state.updated_at = new Date(this.now()).toISOString();
          writeAtomic(statePath(this.directory, run_id), state);
          const e = new Error('RUN_NEEDS_REVIEW'); e.code = 'RUN_NEEDS_REVIEW'; throw e;
        }
      } else {
        const budget = new RunBudget({ run_id, deadline, profile:budget_profile, now: this.now() });
        state = {
          schema_version: 'product-scout-run-ledger-v1', run_id, target_date, deadline, input_hash, input_version,
          model_plan: Object.fromEntries(Object.entries(model_plan).filter(([key, value]) => typeof key === 'string' && typeof value === 'string')),
          status: 'running', started_at: new Date(this.now()).toISOString(), updated_at: new Date(this.now()).toISOString(),
          budget: budget.snapshot(), stage_results: [], finish_reason: null, finished_at: null,
        };
        writeAtomic(statePath(this.directory, run_id), state);
      }
      return new RunSession(this, state, lock);
    } catch (error) {
      try { const current = readJson(lockFile); if (current?.lock_id === lock.lock_id) fs.unlinkSync(lockFile); } catch {}
      throw error;
    }
  }
  recoverStaleLock(runId, { max_age_ms, now = this.now() } = {}) {
    check(Number.isFinite(max_age_ms) && max_age_ms >= 60000, 'INVALID_STALE_LOCK_AGE');
    const file = lockPath(this.directory, runId); const lock = readJson(file);
    check(lock, 'RUN_NOT_LOCKED');
    check(Number.isFinite(Date.parse(lock.acquired_at)) && now - Date.parse(lock.acquired_at) >= max_age_ms, 'LOCK_NOT_STALE');
    fs.unlinkSync(file);
    return { recovered_run_id: runId, previous_lock_id: lock.lock_id };
  }
}
module.exports = { RunLedger, RunSession, safeResult };
