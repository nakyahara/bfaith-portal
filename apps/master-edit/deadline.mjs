/**
 * deadline.mjs — 一覧の CSV・全部コピーの時間の上限 (#1627 Codex R1 M3)。期限はリクエストの始めに作り、参考の値の読み込み・一覧の段・販売数の読み込みの間で確かめる
 */
export class ListTimeoutError extends Error { constructor() { super('一覧の読み出しに時間がかかりすぎました'); this.name = 'ListTimeoutError'; } }
/** 期限 (ms・null = 無し) を過ぎていたら ListTimeoutError */
export function checkDeadline(deadline) { if (deadline != null && Date.now() > deadline) throw new ListTimeoutError(); }
