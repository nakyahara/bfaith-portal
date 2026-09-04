// Drive 一覧取得の回帰テスト (Drive API は呼ばない = モックを注入する)
//   node --test lib/drive-csv.test.mjs
//
// 2026-09-04 の障害 (梱包の納品書CSV 4本が9時間拾われず 144伝票が欠落) の再発防止。
// 「フォルダを OR で束ねて検索しない」「欠けた一覧を成功として返さない」を固定する。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { listDriveFilesAcross, mapWithConcurrency, ACROSS_CONCURRENCY } from './drive-csv.js';

/**
 * files.list を記録するモック。q からフォルダIDを取り出して、そのフォルダのファイルを返す。
 * pagesByFolder を渡すと nextPageToken でページを分けて返す (ページングの検証用)。
 * incomplete は 'F2' (全ページ) または { folderId, page } (そのページだけ) を受ける。
 */
function mockDrive({ filesByFolder = {}, pagesByFolder = null, incomplete = null, failOn = null, delayMs = 0 } = {}) {
  const calls = [];
  let inflight = 0;
  let maxInflight = 0;
  const isIncomplete = (folderId, page) => (
    typeof incomplete === 'string'
      ? incomplete === folderId
      : !!incomplete && incomplete.folderId === folderId && incomplete.page === page
  );
  const drive = {
    files: {
      get: async () => ({ data: { id: 'root', driveId: 'DRIVE1' } }),
      list: async (params) => {
        const folderId = (String(params.q).match(/'([^']+)' in parents/) || [])[1];
        const page = params.pageToken ? Number(String(params.pageToken).replace(/\D/g, '')) : 0;
        calls.push({ q: String(params.q), folderId, page });
        inflight++;
        maxInflight = Math.max(maxInflight, inflight);
        try {
          if (delayMs) await new Promise((r) => setTimeout(r, delayMs));
          if (failOn === folderId) throw new Error(`boom on ${folderId}`);
          const pages = pagesByFolder ? (pagesByFolder[folderId] || [[]]) : [filesByFolder[folderId] || []];
          return {
            data: {
              files: pages[page] || [],
              nextPageToken: page + 1 < pages.length ? `page${page + 1}` : undefined,
              incompleteSearch: isIncomplete(folderId, page),
            },
          };
        } finally {
          inflight--;
        }
      },
    },
  };
  return { drive, calls, stats: () => ({ maxInflight, callCount: calls.length }) };
}

const folder = (n) => ({ folder_id: `F${n}`, name: `出荷_${String(n).padStart(2, '0')}` });
const file = (id, name, modifiedTime) => ({ id, name, modifiedTime, size: '10' });

test('フォルダを OR で束ねない (1フォルダ=1クエリ)', async () => {
  const folders = [folder(1), folder(2), folder(3)];
  const m = mockDrive({
    filesByFolder: {
      F1: [file('a', '納品書_出荷_01.csv', '2026-09-04T01:01:00Z')],
      F2: [file('b', '納品書_出荷_02.csv', '2026-09-04T01:02:00Z')],
      F3: [file('c', '納品書_出荷_03.csv', '2026-09-04T01:03:00Z')],
    },
  });
  const out = await listDriveFilesAcross({ folders, nameContains: '納品書' }, { drive: m.drive });

  assert.equal(m.calls.length, 3, 'フォルダ数と同じ回数だけ files.list を呼ぶ');
  for (const c of m.calls) {
    assert.ok(!/ or /.test(c.q), `クエリに or を含めない: ${c.q}`);
    assert.equal((c.q.match(/in parents/g) || []).length, 1, 'in parents は1つだけ');
    assert.ok(/trashed = false/.test(c.q));
    assert.ok(/name contains '納品書'/.test(c.q));
  }
  assert.deepEqual(out.map((f) => f.file_id), ['c', 'b', 'a'], '更新日時の降順で束ねる');
});

test('どのフォルダのファイルも取りこぼさない (66フォルダ)', async () => {
  const folders = Array.from({ length: 66 }, (_, i) => folder(i + 1));
  const filesByFolder = Object.fromEntries(
    folders.map((f, i) => [f.folder_id, [file(`file${i}`, `納品書_${f.name}.csv`, `2026-09-04T01:${String(i % 60).padStart(2, '0')}:00Z`)]])
  );
  const m = mockDrive({ filesByFolder, delayMs: 1 });
  const out = await listDriveFilesAcross({ folders, nameContains: '納品書' }, { drive: m.drive });

  assert.equal(out.length, 66, '66フォルダすべてのファイルが返る');
  assert.equal(new Set(out.map((f) => f.parent_name)).size, 66);
  assert.ok(m.stats().maxInflight <= ACROSS_CONCURRENCY, `同時実行が ${ACROSS_CONCURRENCY} を超えない (実測 ${m.stats().maxInflight})`);
});

test('parent は問い合わせたフォルダの値を使う (Drive の parents に依存しない)', async () => {
  const m = mockDrive({ filesByFolder: { F7: [file('x', '納品書_出荷_07.csv', '2026-09-04T01:04:43Z')] } });
  const out = await listDriveFilesAcross({ folders: [folder(7)], nameContains: '納品書' }, { drive: m.drive });
  assert.equal(out[0].parent_id, 'F7');
  assert.equal(out[0].parent_name, '出荷_07');
  assert.equal(out[0].modified_time_jst, '2026-09-04 10:04');
});

test('同じ folder_id が重複しても1回しか引かない', async () => {
  const folders = [folder(1), folder(1), folder(2)];
  const m = mockDrive({
    filesByFolder: {
      F1: [file('a', '納品書_出荷_01.csv', '2026-09-04T01:01:00Z')],
      F2: [file('b', '納品書_出荷_02.csv', '2026-09-04T01:02:00Z')],
    },
  });
  const out = await listDriveFilesAcross({ folders, nameContains: '納品書' }, { drive: m.drive });
  assert.equal(m.calls.length, 2);
  assert.deepEqual(out.map((f) => f.file_id), ['b', 'a'], '同じファイルを二重に返さない');
});

test('incompleteSearch=true は成功にしない (欠けた一覧を返さない)', async () => {
  const folders = [folder(1), folder(2)];
  const m = mockDrive({
    filesByFolder: { F1: [file('a', '納品書_出荷_01.csv', '2026-09-04T01:01:00Z')], F2: [] },
    incomplete: 'F2',
  });
  await assert.rejects(
    () => listDriveFilesAcross({ folders, nameContains: '納品書' }, { drive: m.drive }),
    /incompleteSearch/,
  );
});

test('1フォルダの失敗で全体を失敗させる (部分的な一覧を返さない)', async () => {
  const folders = [folder(1), folder(2), folder(3)];
  const m = mockDrive({
    filesByFolder: { F1: [file('a', '納品書_出荷_01.csv', '2026-09-04T01:01:00Z')], F2: [], F3: [] },
    failOn: 'F2',
  });
  await assert.rejects(() => listDriveFilesAcross({ folders, nameContains: '納品書' }, { drive: m.drive }), /boom on F2/);
});

test('mapWithConcurrency: 失敗後は新しいタスクを始めない', async () => {
  const started = [];
  await assert.rejects(() => mapWithConcurrency([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 2, async (n) => {
    started.push(n);
    await new Promise((r) => setTimeout(r, 1));
    if (n === 1) throw new Error('stop');
    return n;
  }), /stop/);
  assert.ok(started.length < 10, `残りを走らせない (開始したのは ${started.length}件)`);
});

test('mapWithConcurrency: 順序を保ち、同時実行数を守る', async () => {
  let inflight = 0;
  let peak = 0;
  const out = await mapWithConcurrency([1, 2, 3, 4, 5, 6, 7], 3, async (n) => {
    inflight++;
    peak = Math.max(peak, inflight);
    await new Promise((r) => setTimeout(r, 1));
    inflight--;
    return n * 2;
  });
  assert.deepEqual(out, [2, 4, 6, 8, 10, 12, 14]);
  assert.equal(peak, 3);
});

test('mapWithConcurrency: 不正な同時実行数でも止まらない (0/負数/NaN/Infinity は1扱い)', async () => {
  for (const limit of [0, -3, NaN, undefined, Infinity]) {
    const out = await mapWithConcurrency([1, 2, 3], limit, async (n) => n + 1);
    assert.deepEqual(out, [2, 3, 4], `limit=${limit}`);
  }
});

test('mapWithConcurrency: falsy な値で reject されても成功にしない', async () => {
  // 停止フラグを firstError と兼用すると、undefined/null の reject をすり抜けて
  // 穴あきの配列を成功として返してしまう (Codexレビュー2巡目)
  for (const bad of [undefined, null, 0, '']) {
    await assert.rejects(
      () => mapWithConcurrency([1, 2, 3], 2, async (n) => { if (n === 1) throw bad; return n; }),
      (e) => e === bad || (e === undefined && bad === undefined),
      `reject(${JSON.stringify(bad)}) を失敗として扱う`,
    );
  }
});

test('mapWithConcurrency: 失敗しても走り出したタスクの完了を待ってから throw する', async () => {
  let finished = 0;
  await assert.rejects(() => mapWithConcurrency([1, 2], 2, async (n) => {
    if (n === 1) throw new Error('fast fail');
    await new Promise((r) => setTimeout(r, 20));
    finished++;
    return n;
  }), /fast fail/);
  assert.equal(finished, 1, '遅いタスクを置き去りにしない');
});

test('mapWithConcurrency: 空配列は空配列', async () => {
  assert.deepEqual(await mapWithConcurrency([], 8, async () => 1), []);
});

test('nextPageToken を辿って全ページ取る', async () => {
  const m = mockDrive({
    pagesByFolder: {
      F1: [
        [file('a', '納品書_1.csv', '2026-09-04T01:03:00Z')],
        [file('b', '納品書_2.csv', '2026-09-04T01:02:00Z')],
        [file('c', '納品書_3.csv', '2026-09-04T01:01:00Z')],
      ],
    },
  });
  const out = await listDriveFilesAcross({ folders: [folder(1)], nameContains: '納品書' }, { drive: m.drive });
  assert.deepEqual(out.map((f) => f.file_id), ['a', 'b', 'c']);
  assert.equal(m.calls.length, 3, '3ページとも取りに行く');
});

test('2ページ目で incompleteSearch が立っても成功にしない', async () => {
  const m = mockDrive({
    pagesByFolder: { F1: [[file('a', '納品書_1.csv', '2026-09-04T01:03:00Z')], [file('b', '納品書_2.csv', '2026-09-04T01:02:00Z')]] },
    incomplete: { folderId: 'F1', page: 1 },
  });
  await assert.rejects(
    () => listDriveFilesAcross({ folders: [folder(1)], nameContains: '納品書' }, { drive: m.drive }),
    /incompleteSearch/,
  );
});
