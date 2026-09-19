// Drive 一覧取得の回帰テスト (Drive API は呼ばない = モックを注入する)
//   node --test lib/drive-csv.test.mjs
//
// 2026-09-04 の障害 (梱包の納品書CSV 4本が9時間拾われず 144伝票が欠落) の再発防止。
// 「フォルダを OR で束ねて検索しない」「欠けた一覧を成功として返さない」を固定する。
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { listDriveFilesAcross, mapWithConcurrency, ACROSS_CONCURRENCY, findDriveFile } from './drive-csv.js';

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

test('mapWithConcurrency: 不正な同時実行数でも止まらない (0/負数/NaN/±Infinity は1扱い)', async () => {
  for (const limit of [0, -3, NaN, undefined, Infinity, -Infinity]) {
    let inflight = 0;
    let peak = 0;
    const out = await mapWithConcurrency([1, 2, 3], limit, async (n) => {
      inflight++;
      peak = Math.max(peak, inflight);
      await new Promise((r) => setTimeout(r, 1));
      inflight--;
      return n + 1;
    });
    assert.deepEqual(out, [2, 3, 4], `limit=${limit}`);
    assert.equal(peak, 1, `limit=${limit} は同時実行1に正規化される`);
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

// ── findDriveFile の latestPrefix (毎回ファイル名が変わる出力から最新1本を採る) ──
// ゆうプリクラウド「送り状データダウンロード_YYYYMMDDhhmm.csv」用 (2026-09-19)。
// 守りたいこと: ①最新を取り違えない ②対象外の名前を拾わない ③欠けた一覧から「最新」を決めない。

const CLOUD = {
  label: 'ゆうパケットパフ',
  folderId: 'F1',
  filename: 'ゆうプリR出荷履歴＿2項目3項目_20250714102302.csv',
  latestPrefix: '送り状データダウンロード_',
};

test('latestPrefix: 接頭辞で始まる .csv の更新日時が最新の1本を採る', async () => {
  const m = mockDrive({
    filesByFolder: {
      F1: [
        file('new', '送り状データダウンロード_202609191500.csv', '2026-09-19T06:00:00Z'),
        file('old', '送り状データダウンロード_202609181137.csv', '2026-09-18T02:37:00Z'),
      ],
    },
  });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'new');
  assert.equal(info.filename, '送り状データダウンロード_202609191500.csv');
});

test('latestPrefix: 旧ゆうプリRの固定名も候補に残る (手で旧形式へ戻す運用に耐える)', async () => {
  const m = mockDrive({
    filesByFolder: {
      F1: [
        file('fixed', CLOUD.filename, '2026-09-19T09:00:00Z'),
        file('cloud', '送り状データダウンロード_202609191500.csv', '2026-09-19T06:00:00Z'),
      ],
    },
  });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'fixed', '固定名の方が新しければそちらを採る');
});

test('latestPrefix: 接頭辞違い・csv以外は拾わない', async () => {
  const m = mockDrive({
    filesByFolder: {
      F1: [
        file('pdf', '送り状データダウンロード_202609191500.pdf', '2026-09-19T08:00:00Z'),
        file('other', '出荷一覧_202609191500.csv', '2026-09-19T07:00:00Z'),
        file('hit', '送り状データダウンロード_202609181137.csv', '2026-09-18T02:37:00Z'),
      ],
    },
  });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'hit');
});

test('latestPrefix: 候補が1本も無ければ見つからないエラー (空成功にしない)', async () => {
  const m = mockDrive({ filesByFolder: { F1: [file('x', '別のファイル.csv', '2026-09-19T08:00:00Z')] } });
  await assert.rejects(() => findDriveFile(CLOUD, { drive: m.drive }), /見つかりません/);
});

test('latestPrefix: 濁点が分解された名前 (NFD) でも同じ接頭辞として拾う', async () => {
  const nfd = '送り状データダウンロード_202609191500.csv'.normalize('NFD');
  const m = mockDrive({ filesByFolder: { F1: [file('nfd', nfd, '2026-09-19T06:00:00Z')] } });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'nfd');
});

test('latestPrefix: 候補が無ければ次ページも辿る', async () => {
  const m = mockDrive({
    pagesByFolder: {
      F1: [
        [file('a', '関係ないファイル.csv', '2026-09-19T09:00:00Z')],
        [file('b', '送り状データダウンロード_202609181137.csv', '2026-09-18T02:37:00Z')],
      ],
    },
  });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'b');
  assert.equal(m.calls.length, 2);
});

test('latestPrefix: 候補より古いものが同じページに出たら打ち切る (以降は辿らない)', async () => {
  const m = mockDrive({
    pagesByFolder: {
      F1: [
        [file('hit', '送り状データダウンロード_202609191500.csv', '2026-09-19T06:00:00Z'),
         file('zzz', '関係ないファイル.csv', '2026-09-18T02:37:00Z')],
        [file('older', '送り状データダウンロード_202609171000.csv', '2026-09-17T01:00:00Z')],
      ],
    },
  });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'hit');
  assert.equal(m.calls.length, 1, 'best より古いものが出た時点で次ページを引かない');
});

// ── Codexレビュー 1巡目 P2: ページ境界をまたぐ同時刻 / 同名同時刻の揺れ ──

test('latestPrefix: 同じ更新日時の候補が次ページに続いても取り違えない', async () => {
  const same = '2026-09-19T06:00:00Z';
  const m = mockDrive({
    pagesByFolder: {
      F1: [
        [file('old', '送り状データダウンロード_202609181137.csv', same)],
        [file('new', '送り状データダウンロード_202609191500.csv', same)],
      ],
    },
  });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'new', '同時刻ならファイル名 (末尾が日時) の新しい方');
  assert.equal(m.calls.length, 2, '同時刻が続く間は次ページも見る');
});

test('latestPrefix: 同名・同時刻の別ファイルがあっても返却順で選択が変わらない', async () => {
  const same = '2026-09-19T06:00:00Z';
  const name = '送り状データダウンロード_202609191500.csv';
  const pick = async (order) => {
    const m = mockDrive({ filesByFolder: { F1: order.map((id) => file(id, name, same)) } });
    return (await findDriveFile(CLOUD, { drive: m.drive })).file_id;
  };
  assert.equal(await pick(['A', 'B']), await pick(['B', 'A']),
    '返却順が入れ替わっても同じ1本 (DL前後の再確認が空振りしない)');
});

test('latestPrefix: 同時刻の候補が尽きずページ上限に達したら「確定できない」で止める', async () => {
  const same = '2026-09-19T06:00:00Z';
  const pages = Array.from({ length: 12 }, (_, i) =>
    [file(`f${i}`, `送り状データダウンロード_20260919150${i}.csv`, same)]);
  const m = mockDrive({ pagesByFolder: { F1: pages } });
  await assert.rejects(() => findDriveFile(CLOUD, { drive: m.drive }), /ページ探しても最新の1本を確定できません/);
});

// ── Codexレビュー 2巡目 P2/P3: 更新日時の欠落・探索の境界 ──

test('latestPrefix: 更新日時の無いファイルを「古い」と見なして探索を打ち切らない', async () => {
  const same = '2026-09-19T06:00:00Z';
  const m = mockDrive({
    pagesByFolder: {
      F1: [
        [file('old', '送り状データダウンロード_202609181137.csv', same),
         file('nomtime', '関係ないファイル.csv', undefined)],
        [file('new', '送り状データダウンロード_202609191500.csv', same)],
      ],
    },
  });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'new', '日時欠落は打ち切りの根拠にしない');
});

test('latestPrefix: 候補自身の更新日時が無ければ黙って選ばず中断する', async () => {
  const m = mockDrive({
    filesByFolder: {
      F1: [
        file('nomtime', '送り状データダウンロード_202609191500.csv', undefined),
        file('ok', '送り状データダウンロード_202609181137.csv', '2026-09-18T02:37:00Z'),
      ],
    },
  });
  await assert.rejects(() => findDriveFile(CLOUD, { drive: m.drive }), /更新日時がありません/);
});

test('latestPrefix: 空ページを挟んでも辿り続ける', async () => {
  const m = mockDrive({
    pagesByFolder: { F1: [[], [], [file('hit', '送り状データダウンロード_202609181137.csv', '2026-09-18T02:37:00Z')]] },
  });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'hit');
  assert.equal(m.calls.length, 3);
});

test('latestPrefix: 上限ちょうど (10ページ目) で確定できれば成功する', async () => {
  const same = '2026-09-19T06:00:00Z';
  const pages = Array.from({ length: 10 }, (_, i) =>
    [file(`f${i}`, `送り状データダウンロード_2026091915${String(i).padStart(2, '0')}.csv`, same)]);
  const m = mockDrive({ pagesByFolder: { F1: pages } });
  const info = await findDriveFile(CLOUD, { drive: m.drive });
  assert.equal(info.file_id, 'f9', '最後のページまで見て名前が最大のものを採る');
  assert.equal(m.calls.length, 10);
});

test('latestPrefix: 候補を見つけた後のページで incompleteSearch が立ったら成功にしない', async () => {
  const same = '2026-09-19T06:00:00Z';
  const m = mockDrive({
    pagesByFolder: {
      F1: [
        [file('old', '送り状データダウンロード_202609181137.csv', same)],
        [file('new', '送り状データダウンロード_202609191500.csv', same)],
      ],
    },
    incomplete: { folderId: 'F1', page: 1 },
  });
  await assert.rejects(() => findDriveFile(CLOUD, { drive: m.drive }), /incompleteSearch/);
});

test('latestPrefix: incompleteSearch=true は成功にしない (古いファイルを最新として取り込まない)', async () => {
  const m = mockDrive({
    filesByFolder: { F1: [file('old', '送り状データダウンロード_202609181137.csv', '2026-09-18T02:37:00Z')] },
    incomplete: 'F1',
  });
  await assert.rejects(() => findDriveFile(CLOUD, { drive: m.drive }), /incompleteSearch/);
});

test('latestPrefix なしの設定は従来どおり name= の完全一致で引く', async () => {
  const m = mockDrive({ filesByFolder: { F1: [file('y', 'ネコ・60サイズ31項目4項目_発行済データ.csv', '2026-09-19T06:00:00Z')] } });
  const cfg = { label: 'ヤマト B2', folderId: 'F1', filename: 'ネコ・60サイズ31項目4項目_発行済データ.csv' };
  const info = await findDriveFile(cfg, { drive: m.drive });
  assert.equal(info.file_id, 'y');
  assert.match(m.calls[0].q, /^name = '/, '固定名の設定は name= 検索のまま');
});
