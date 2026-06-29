import { test } from 'node:test';
import assert from 'node:assert/strict';

import { parseCategoryResponse, buildPathFromCurrent } from './yahoo-category-client.js';

// ── buildPathFromCurrent ──
test('buildPathFromCurrent: 祖先 Title.Medium を : 連結、 ルート(Depth=0)除外', () => {
  const current = {
    Id: '43494',
    Path: {
      Category: [
        { Id: '1', Depth: '0', Title: { Medium: 'すべて' } },
        { Id: '2498', Depth: '1', Title: { Medium: '生活雑貨・日用品' } },
        { Id: '43494', Depth: '2', Title: { Medium: '掃除用品' } },
      ],
    },
  };
  assert.equal(buildPathFromCurrent(current), '生活雑貨・日用品:掃除用品');
});

test('buildPathFromCurrent: Id=1 もルート扱いで除外', () => {
  const current = {
    Path: {
      Category: [
        { Id: '1', Title: { Medium: 'すべて' } },
        { Id: '100', Title: { Medium: '食品・飲料' } },
      ],
    },
  };
  assert.equal(buildPathFromCurrent(current), '食品・飲料');
});

test('buildPathFromCurrent: Depth 順がバラバラでも昇順ソート', () => {
  const current = {
    Path: {
      Category: [
        { Id: '43494', Depth: '2', Title: { Medium: '掃除用品' } },
        { Id: '1', Depth: '0', Title: { Medium: 'すべて' } },
        { Id: '2498', Depth: '1', Title: { Medium: '生活雑貨・日用品' } },
      ],
    },
  };
  assert.equal(buildPathFromCurrent(current), '生活雑貨・日用品:掃除用品');
});

test('buildPathFromCurrent: Path 単一オブジェクト(配列でない)も拾う', () => {
  const current = { Path: { Category: { Id: '100', Depth: '1', Title: { Medium: '食品・飲料' } } } };
  assert.equal(buildPathFromCurrent(current), '食品・飲料');
});

test('buildPathFromCurrent: Path 無しは null', () => {
  assert.equal(buildPathFromCurrent({ Id: '1' }), null);
  assert.equal(buildPathFromCurrent(null), null);
});

test('buildPathFromCurrent: Title が文字列でも拾う', () => {
  const current = { Path: { Category: [{ Id: '100', Depth: '1', Title: '食品・飲料' }] } };
  assert.equal(buildPathFromCurrent(current), '食品・飲料');
});

test('buildPathFromCurrent: Path.Category が numeric-key オブジェクトでも配列化 (Codex R1 Medium)', () => {
  const current = {
    Path: {
      Category: {
        0: { Id: '1', Depth: '0', Title: { Medium: 'すべて' } },
        1: { Id: '2498', Depth: '1', Title: { Medium: '生活雑貨・日用品' } },
        2: { Id: '43494', Depth: '2', Title: { Medium: '掃除用品' } },
      },
    },
  };
  assert.equal(buildPathFromCurrent(current), '生活雑貨・日用品:掃除用品');
});

// ── parseCategoryResponse ──
test('parseCategoryResponse: Current + Children を正規化', () => {
  const json = {
    ResultSet: {
      Result: {
        Categories: {
          Current: {
            Id: '2498', ParentId: '1',
            Title: { Short: '生活', Medium: '生活雑貨・日用品', Long: '生活雑貨・日用品すべて' },
            Path: {
              Category: [
                { Id: '1', Depth: '0', Title: { Medium: 'すべて' } },
                { Id: '2498', Depth: '1', Title: { Medium: '生活雑貨・日用品' } },
              ],
            },
          },
          Children: {
            Category: [
              { Id: '43494', ParentId: '2498', Title: { Medium: '掃除用品' } },
              { Id: '43495', ParentId: '2498', Title: { Medium: '洗濯用品' } },
            ],
          },
        },
      },
    },
  };
  const { current, children } = parseCategoryResponse(json);
  assert.equal(current.categoryId, 2498);
  assert.equal(current.parentId, 1);
  assert.equal(current.titleMedium, '生活雑貨・日用品');
  assert.equal(current.path, '生活雑貨・日用品');
  assert.equal(children.length, 2);
  assert.equal(children[0].categoryId, 43494);
  assert.equal(children[0].titleMedium, '掃除用品');
});

test('parseCategoryResponse: Children 単一オブジェクトも配列化', () => {
  const json = {
    ResultSet: { Result: { Categories: {
      Current: { Id: '100', Title: { Medium: '食品' } },
      Children: { Category: { Id: '101', ParentId: '100', Title: { Medium: '飲料' } } },
    } } },
  };
  const { children } = parseCategoryResponse(json);
  assert.equal(children.length, 1);
  assert.equal(children[0].categoryId, 101);
});

test('parseCategoryResponse: Categories 無しは空', () => {
  const { current, children } = parseCategoryResponse({ ResultSet: {} });
  assert.equal(current, null);
  assert.deepEqual(children, []);
});

test('parseCategoryResponse: ResultSet が配列ラップでも拾う', () => {
  const json = {
    ResultSet: [{ Result: { Categories: { Current: { Id: '5', Title: { Medium: 'X' } }, Children: {} } } }],
  };
  const { current } = parseCategoryResponse(json);
  assert.equal(current.categoryId, 5);
});
