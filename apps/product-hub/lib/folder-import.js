/**
 * 画像フォルダ一括取り込みのスロット割当 (pure function)。
 *
 * 命名規則: <商品コード>_<番号 or top>.<拡張子>
 *   _00        → 白抜き背景画像 (whiteBgImage)
 *   _top       → **商品画像1 (楽天のTOP画像/サムネイル)** — 2026-08-08 スタッフ指摘で追加
 *   _01〜_19   → 商品画像 2〜20 (**_top が枠1を取るので1つずれる**)
 *   _20以上    → 楽天上限超えのため対象外 (警告として報告)
 *   番号なし    → 対象外 (警告として報告)
 *
 * プレフィックス (番号より前の部分) が商品コードと一致するファイルだけを採用する
 * (fail-closed。Codex R1 high: 誤ったフォルダURLで別商品の画像に全置換される事故を防ぐ)。
 * 一致ゼロのときは codeMatched: false を返し、呼び出し側がエラーにする。
 */

export const MAX_IMAGE_SLOTS = 20;
/** ファイル名に使える最大番号。_top が商品画像1を取るので _19 = 商品画像20 が上限 */
export const MAX_NUMBERED_IMAGE = MAX_IMAGE_SLOTS - 1;

/**
 * ファイル名から { base, kind, num, label } を抽出。命名規則に合わなければ null。
 *   kind: 'white' (_00) | 'top' (_top) | 'num' (_01〜)
 *   label: 画面表示・警告文で使うファイル名の末尾 ('_top' / '_02')
 */
export function parseImageFileName(name) {
  const m = String(name || '').match(/^(.*)_(\d{1,3}|[Tt][Oo][Pp])\.[a-zA-Z0-9]+$/);
  if (!m) return null;
  const tag = m[2].toLowerCase();
  if (tag === 'top') return { base: m[1], kind: 'top', num: null, label: '_top' };
  const num = Number.parseInt(tag, 10);
  if (num === 0) return { base: m[1], kind: 'white', num: 0, label: '_00' };
  return { base: m[1], kind: 'num', num, label: `_${String(num).padStart(2, '0')}` };
}

/** 楽天の商品画像スロット番号 (1始まり)。_top=1 / _01=2 / _NN=NN+1。白抜きは null */
export function slotOfParsedName(parsed) {
  if (!parsed) return null;
  if (parsed.kind === 'top') return 1;
  if (parsed.kind === 'num') return parsed.num + 1;
  return null;
}

const norm = (s) => String(s || '').trim().toLowerCase();

/**
 * @param {Array<{id: string, name: string, mimeType?: string, modifiedTime?: string}>} files
 * @param {string} neCode 商品コード (プレフィックス一致で採用を判定)
 * @returns {{
 *   whiteBg: {id, name, modifiedTime}|null,
 *   slots: Array<{slot: number, id: string, name: string, label: string, modifiedTime: string|null}>, // slot 昇順 (1=TOP画像)
 *   skipped: Array<{name: string, reason: string}>,
 *   conflicts: Array<{label: string, names: string[]}>,   // 同じ枠を指すファイルの重複
 *   missingLabels: string[],  // 欠けているファイル名の末尾 ('_top' / '_03'。楽天へは詰めて登録される)
 *   codeMatched: boolean,     // 商品コード一致のファイルが1枚でもあったか
 * }}
 */
export function assignImageSlots(files, neCode) {
  const skipped = [];
  const parsed = [];
  for (const f of files || []) {
    if (f.mimeType && !String(f.mimeType).startsWith('image/')) {
      skipped.push({ name: f.name, reason: '画像ファイルではありません' });
      continue;
    }
    const p = parseImageFileName(f.name);
    if (!p) {
      skipped.push({ name: f.name, reason: '末尾が _top / _番号 ではありません' });
      continue;
    }
    parsed.push({ ...p, id: f.id, name: f.name, modifiedTime: f.modifiedTime || null });
  }

  // 商品コード一致のファイルだけを採用 (fail-closed)
  const code = norm(neCode);
  const matched = parsed.filter((f) => norm(f.base) === code);
  for (const f of parsed) {
    if (norm(f.base) !== code) skipped.push({ name: f.name, reason: `商品コード (${neCode}) と先頭が一致しません` });
  }
  if (matched.length === 0) {
    return { whiteBg: null, slots: [], skipped, conflicts: [], missingLabels: [], codeMatched: false };
  }

  // 枠ごとにグループ化して重複検出 (白抜きは 'white' キー、商品画像は slot 番号)
  const byKey = new Map();
  for (const f of matched) {
    const key = f.kind === 'white' ? 'white' : slotOfParsedName(f);
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(f);
  }

  let whiteBg = null;
  const slots = [];
  const conflicts = [];
  for (const [key, group] of byKey.entries()) {
    // 上限超えは重複していても skipped 扱い (Codex R2 low: 対象外番号の重複で全体を止めない)
    if (key !== 'white' && key > MAX_IMAGE_SLOTS) {
      for (const g of group) {
        skipped.push({ name: g.name, reason: `${g.label} は楽天の上限 (_top と _01〜_${MAX_NUMBERED_IMAGE}) を超えています` });
      }
      continue;
    }
    if (group.length > 1) {
      conflicts.push({ label: group[0].label, names: group.map((g) => g.name) });
      continue;
    }
    const f = group[0];
    if (key === 'white') {
      whiteBg = { id: f.id, name: f.name, modifiedTime: f.modifiedTime };
    } else {
      slots.push({ slot: key, id: f.id, name: f.name, label: f.label, modifiedTime: f.modifiedTime });
    }
  }
  slots.sort((a, b) => a.slot - b.slot);

  // 欠番 (conflict でセットされなかった枠も欠番として知らせる)。
  // 枠1 = _top なので、_top が無ければ必ず先頭に出る = 「TOP画像が未設定」の警告になる
  const present = new Set(slots.map((s) => s.slot));
  const lastSlot = slots.length > 0 ? slots[slots.length - 1].slot : 0;
  const missingLabels = [];
  for (let n = 1; n < lastSlot; n++) {
    if (!present.has(n)) missingLabels.push(n === 1 ? '_top' : `_${String(n - 1).padStart(2, '0')}`);
  }

  return { whiteBg, slots, skipped, conflicts, missingLabels, codeMatched: true };
}
