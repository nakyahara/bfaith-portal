/**
 * 画像フォルダ一括取り込みのスロット割当 (pure function)。
 *
 * 命名規則: <商品コード>_<番号>.<拡張子>
 *   _00        → 白抜き背景画像 (whiteBgImage)
 *   _01〜_20   → 楽天商品画像スロット 1〜20 (images[] の並び順。欠番は詰めて登録される)
 *   _21以上    → 楽天上限超えのため対象外 (警告として報告)
 *   番号なし    → 対象外 (警告として報告)
 *
 * プレフィックス (番号より前の部分) が商品コードと一致するファイルだけを採用する
 * (fail-closed。Codex R1 high: 誤ったフォルダURLで別商品の画像に全置換される事故を防ぐ)。
 * 一致ゼロのときは codeMatched: false を返し、呼び出し側がエラーにする。
 */

export const MAX_IMAGE_SLOTS = 20;

/** ファイル名から { base, num } を抽出。番号付きでなければ null */
export function parseNumberedName(name) {
  const m = String(name || '').match(/^(.*)_(\d{1,3})\.[a-zA-Z0-9]+$/);
  if (!m) return null;
  return { base: m[1], num: Number.parseInt(m[2], 10) };
}

const norm = (s) => String(s || '').trim().toLowerCase();

/**
 * @param {Array<{id: string, name: string, mimeType?: string}>} files
 * @param {string} neCode 商品コード (プレフィックス一致で採用を判定)
 * @returns {{
 *   whiteBg: {id, name}|null,
 *   slots: Array<{slot: number, id: string, name: string}>,  // slot は 1〜20、slot 昇順
 *   skipped: Array<{name: string, reason: string}>,
 *   conflicts: Array<{num: number, names: string[]}>,        // 同一番号の重複
 *   missingNums: number[],   // 1〜最終スロットの間の欠番 (楽天へは詰めて登録される)
 *   codeMatched: boolean,    // 商品コード一致の番号付きファイルが1枚でもあったか
 * }}
 */
export function assignImageSlots(files, neCode) {
  const skipped = [];
  const numbered = [];
  for (const f of files || []) {
    if (f.mimeType && !String(f.mimeType).startsWith('image/')) {
      skipped.push({ name: f.name, reason: '画像ファイルではありません' });
      continue;
    }
    const parsed = parseNumberedName(f.name);
    if (!parsed) {
      skipped.push({ name: f.name, reason: '末尾に _番号 がありません' });
      continue;
    }
    numbered.push({ ...parsed, id: f.id, name: f.name });
  }

  // 商品コード一致のファイルだけを採用 (fail-closed)
  const code = norm(neCode);
  const matched = numbered.filter((f) => norm(f.base) === code);
  for (const f of numbered) {
    if (norm(f.base) !== code) skipped.push({ name: f.name, reason: `商品コード (${neCode}) と先頭が一致しません` });
  }
  if (matched.length === 0) {
    return { whiteBg: null, slots: [], skipped, conflicts: [], missingNums: [], codeMatched: false };
  }

  // 番号ごとにグループ化して重複検出
  const byNum = new Map();
  for (const f of matched) {
    if (!byNum.has(f.num)) byNum.set(f.num, []);
    byNum.get(f.num).push(f);
  }

  let whiteBg = null;
  const slots = [];
  const conflicts = [];
  for (const [num, group] of [...byNum.entries()].sort((a, b) => a[0] - b[0])) {
    if (group.length > 1) {
      conflicts.push({ num, names: group.map((g) => g.name) });
      continue;
    }
    const f = group[0];
    if (num === 0) {
      whiteBg = { id: f.id, name: f.name };
    } else if (num <= MAX_IMAGE_SLOTS) {
      slots.push({ slot: num, id: f.id, name: f.name });
    } else {
      skipped.push({ name: f.name, reason: `_${num} は楽天の上限 (_01〜_${MAX_IMAGE_SLOTS}) を超えています` });
    }
  }

  // 欠番 (conflictでセットされなかった番号も欠番として知らせる)
  const present = new Set(slots.map((s) => s.slot));
  const lastSlot = slots.length > 0 ? slots[slots.length - 1].slot : 0;
  const missingNums = [];
  for (let n = 1; n < lastSlot; n++) if (!present.has(n)) missingNums.push(n);

  return { whiteBg, slots, skipped, conflicts, missingNums, codeMatched: true };
}
