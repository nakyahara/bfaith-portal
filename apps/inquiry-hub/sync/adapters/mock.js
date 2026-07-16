/**
 * モックアダプター — 同期エンジンの単体テスト用 (設計書§12「アダプターをモック化した同期カーソルのテスト」)
 * 実チャネル (gmail/rakuten/yahoo) と同じ fetchNew 契約を、メモリ上のデータセットで模倣する。
 *
 * data: engine.js のアダプター契約と同じ形の inquiries 配列。各要素に updatedAt (ISO) を持たせ、
 *       fetchNew は sinceIso < updatedAt のものだけを返す (日時ウィンドウ型チャネルの模倣)。
 */
export function createMockAdapter(data = [], opts = {}) {
  const state = {
    calls: 0,
    failAtCall: opts.failAtCall ?? null,   // n回目の呼び出しで throw (部分失敗はさせない = 契約通り全体失敗)
    lastArgs: null,
  };
  return {
    state,
    setData(next) { state.data = next; data = next; },
    async fetchNew({ sinceIso, untilIso, cursor }) {
      state.calls++;
      state.lastArgs = { sinceIso, untilIso, cursor };
      if (state.failAtCall === state.calls) {
        const e = new Error(`mock failure at call ${state.calls}`);
        e.errorType = opts.errorType || 'fetch_failed';
        throw e;
      }
      const items = data.filter(i => !i.updatedAt || i.updatedAt > sinceIso);
      return {
        inquiries: items,
        nextCursor: opts.nextCursor,
        observedUntil: opts.observedUntil,
      };
    },
  };
}
