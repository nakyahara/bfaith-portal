/**
 * 資材の持ち方 (2026-10-01 中原さん要望)。
 *
 * きっかけ: 「在庫化のとき商品を 10 個ずつ袋に入れる。その小分けの袋も資材として記録したい」。
 * それまで資材は `f_iroha_work_master.material_code` の 1 列しか持てなかった。
 *
 * 決めごと (Codex 設計相談 2026-10-01):
 *   - **正本は `materials_json`** (オブジェクトの配列)。`material_code` は 1 件目の写し (互換用)。
 *     両方を別々に書けるようにすると二重正本になって必ずずれるので、**書き込みは必ずこのモジュールを通す**
 *   - `materials_json` が NULL = **まだ新しい形に移っていない行** → 読むときに `material_code` から 1 件にする。
 *     `'[]'` = **資材なし (人が消した)**。この 2 つを混同しない
 *   - 並び順に意味がある。**先頭が主資材** (= material_code に写る)
 *   - 小分け袋の数 (`units_per_pack`) は**その資材に紐づける**。
 *     🚨 保管箱の入数 (`units_per_container`) とは別物。入数は必要な保管箱の数の計算に使われているので、
 *        小分けの数をそこへ混ぜると箱数が狂う (neededBoxesCalc)
 *   - 同じ資材の重複は正規化キーで落とす。文字は入力どおりに残す (表示は人が入れた表記)
 *
 * このファイルは**純粋関数だけ** (DB も Express も触らない)。
 * 🚨 lib/ に置くのは置き場所の好みではない。入荷検品 (inbound-check) が いろは (iroha-work) から
 *    import してよいのは `createTaskForDestination` **だけ**という決まりがあり (scripts/test-inbound-check.mjs [H]、
 *    中原さん 2026-09-09「削除は絶対にダメ」)、apps/iroha-work/ に置くとその決まりを破る。
 *    どちらのアプリにも属さない純粋な決めごとなので lib/ が正しい置き場。
 */

/**
 * 資材の上限。🚨 **画面 (apps/iroha-work/views/index.html の MAT_MAX) と同じ数にすること** —
 * ずれると「API で 3 件保存できるのに画面は 2 件までと言う」状態になる (Codex 2026-10-01)。
 * 増やすときは両方を直す (テストが食い違いを見張っている)
 */
export const MAX_MATERIALS = 2;

/** 資材の使い道。normal = ふつうに使う / inner_pack = 小分け袋として使う (何個ずつ入れるかを持てる) */
export const MATERIAL_USAGES = ['normal', 'inner_pack'];

/** 小分けの数の上限 (打ち間違いを通さないための歯止め。現場の実際は 10 個ずつ程度) */
export const MAX_UNITS_PER_PACK = 100000;

const MAX_CODE_LEN = 100;

/** 比較用の正規化: NFKC (全角英数・全角空白→半角) + 連続空白を1つ + trim + 大文字化。表示は入力どおり (Codex R1 #3) */
export function normalizeOptionCode(code) {
  return String(code == null ? '' : code).normalize('NFKC').replace(/\s+/g, ' ').trim().toUpperCase();
}

/**
 * 表示用の整形 (連続空白を1つ・trim。文字種は変えない)。
 * 🚨 **文字列と数値以外は空にする** — 壊れたスナップショットや細工された JSON で
 *    `{code:'D-8'}` のような入れ子が来たとき、String() すると画面に `[object Object]` が出る
 *    (外部施設の画面にも出てしまう。2026-09 のテストが見張っている境界)
 */
export function displayCode(code) {
  if (code == null) return '';
  if (typeof code !== 'string' && typeof code !== 'number') return '';
  if (typeof code === 'number' && !Number.isFinite(code)) return '';
  return String(code).replace(/\s+/g, ' ').trim();
}

/** 0 以上の整数か (null = 未登録は通す) */
function intOrNull(v, max) {
  if (v == null || v === '') return { ok: true, value: null };
  const n = Number(v);
  if (!Number.isInteger(n) || n < 1 || n > max) return { ok: false, value: null };
  return { ok: true, value: n };
}

/**
 * 1 件ぶんの正規化。文字列でも {code, usage, units_per_pack} でも受ける。
 * @returns {{ok: true, material: object} | {ok: false, error: string, message: string}}
 */
function canonicalizeOne(input) {
  const src = typeof input === 'string' ? { code: input } : (input && typeof input === 'object' ? input : null);
  if (!src) return { ok: false, error: 'bad_material', message: '資材の指定が読めません' };
  const code = displayCode(src.code);
  if (!code) return { ok: false, error: 'bad_material', message: '資材の名前を入れてください' };
  if (code.length > MAX_CODE_LEN) return { ok: false, error: 'too_long', message: `資材の名前が長すぎます (${MAX_CODE_LEN}文字まで)` };
  const usage = src.usage == null || src.usage === '' ? 'normal' : String(src.usage);
  if (!MATERIAL_USAGES.includes(usage)) {
    return { ok: false, error: 'bad_usage', message: '資材の使い道は「ふつう」「小分け袋」のどちらかです' };
  }
  const per = intOrNull(src.units_per_pack, MAX_UNITS_PER_PACK);
  if (!per.ok) return { ok: false, error: 'bad_units_per_pack', message: `小分けの数は 1〜${MAX_UNITS_PER_PACK} の整数で入れてください` };
  // 🚨 小分け袋でない資材に数を残さない (使い道を戻したのに古い数が残ると、画面に「10個ずつ」が出たままになる)
  const material = { code, usage };
  if (usage === 'inner_pack' && per.value != null) material.units_per_pack = per.value;
  return { ok: true, material };
}

/**
 * 画面・API から来た資材の指定を、保存してよい形にそろえる。
 * 重複は先勝ちで落とし、件数の上限を見る。
 *
 * 🚨 **strict を使い分ける** (Codex 2026-10-01):
 *   - `strict: true` … **人の入力を保存するとき** (保存 API・マスタの書き込み)。
 *     読めない行 (`{}` / code が入れ子 / 空の code) は**捨てずにエラー**。
 *     捨てると「資材を全部消す」指示に化けたり、「変更なし」で黙って消えたりする
 *   - 既定 (寛容) … **DB やカードのスナップショットを読むとき**。
 *     壊れた行は落として読み進める (表示を止めない)
 * @param {Array|string|null} input
 * @param {{strict?: boolean}} [opts]
 * @returns {{ok: true, materials: object[]} | {ok: false, error: string, message: string}}
 */
export function canonicalizeMaterials(input, { strict = false } = {}) {
  if (input == null || input === '') return { ok: true, materials: [] };
  const list = Array.isArray(input) ? input : [input];
  const out = [];
  const seen = new Set();
  for (const item of list) {
    const unreadable = item == null || item === '' || (typeof item === 'object' && !displayCode(item.code));
    if (unreadable) {
      if (strict) return { ok: false, error: 'bad_material', message: '資材の指定が読めません (名前を選び直してください)' };
      continue;   // 読むだけのときは落として進む
    }
    const r = canonicalizeOne(item);
    if (!r.ok) return r;
    const key = normalizeOptionCode(r.material.code);
    if (seen.has(key)) continue;   // 同じ資材を 2 回選んだときは 1 つにまとめる
    seen.add(key);
    out.push(r.material);
  }
  if (out.length > MAX_MATERIALS) {
    return { ok: false, error: 'too_many_materials', message: `資材は ${MAX_MATERIALS} つまでです` };
  }
  return { ok: true, materials: out };
}

/**
 * DB の materials_json を読む。壊れていたら null (= 未移行と同じ扱いにして material_code へ落とす)。
 * 読み出しでも正規化を通す (手で DB をいじった値・古い形をそのまま画面へ出さない)
 */
export function parseMaterialsJson(json) {
  if (json == null || json === '') return null;
  let v;
  try { v = JSON.parse(json); } catch (_) { return null; }
  if (!Array.isArray(v)) return null;
  const r = canonicalizeMaterials(v);
  return r.ok ? r.materials : null;
}

/**
 * マスタ行 (または作成時スナップショット) から資材の配列を作る。
 *   materials_json がある        → それ
 *   無い (未移行) + material_code → 1 件だけの配列
 *   どちらも無い                 → []
 * @param {{materials_json?: string|null, materials?: object[]|null, material_code?: string|null}|null} row
 */
export function materialsOf(row) {
  if (!row) return [];
  // すでに配列で持っている (API の戻り・スナップショット) ときはそれを正規化して使う
  if (Array.isArray(row.materials)) {
    const r = canonicalizeMaterials(row.materials);
    return r.ok ? r.materials : [];
  }
  // '[]' (資材なし) は [] が返るのでここで確定する。null = 未移行 **または壊れた JSON** →
  // 🚨 その場合は material_code へ落とす (壊れた値で現場の資材を消さない。写真も出したい)
  const parsed = parseMaterialsJson(row.materials_json);
  if (parsed) return parsed;
  const one = displayCode(row.material_code);
  return one ? [{ code: one, usage: 'normal' }] : [];
}

/**
 * その行が**資材について答えを持っているか** (= materials_json がちゃんと読めるか)。
 * 🚨 `'[]'` (人が資材なしにした) は **true**。これを「空だから未登録」と同じ扱いにすると、
 *    カード (作成時スナップショット) に残っている古い資材が復活する (Codex 2026-10-01 high)
 */
export function hasMaterialsDecision(row) {
  return !!row && parseMaterialsJson(row.materials_json) !== null;
}

/** 保存する JSON 文字列。[] も '[]' として残す (「資材なし」と「未移行」を区別するため null にしない) */
export function serializeMaterials(list) {
  const arr = (list || []).map((m) => {
    // キーの順番を固定する (順番だけ違う JSON で version が進まないように)
    const o = { code: m.code };
    if (m.usage && m.usage !== 'normal') o.usage = m.usage;
    if (m.usage === 'inner_pack' && m.units_per_pack != null) o.units_per_pack = m.units_per_pack;
    return o;
  });
  return JSON.stringify(arr);
}

/** 1 件目の資材コード (= material_code に写す値)。無ければ null */
export function primaryMaterialCode(list) {
  const first = (list || [])[0];
  return first ? first.code : null;
}

/** 中身が同じか (正規化してから比べる。空白やキー順の違いで version を進めない) */
export function sameMaterials(a, b) {
  return serializeMaterials(a || []) === serializeMaterials(b || []);
}

/**
 * 古い画面からの `material_code` だけの更新を、いまの配列に重ねる。
 * 🚨 2 件目以降を消さない (古い iPad が 1 件目を直しただけで小分け袋の指定が消えてはいけない)。
 * @param {object[]} current いまの配列
 * @param {string|null} code 新しい 1 件目 ('' / null = 資材なしにする)
 */
export function withPrimaryCode(current, code) {
  const list = (current || []).map((m) => ({ ...m }));
  const next = displayCode(code);
  if (!next) {
    // 1 件目を消す = 2 件目が繰り上がる。「資材なし」にしたいときは materials を空で送ってもらう
    return list.slice(1);
  }
  // 同じ資材が 2 件目以降にあれば、そちらを落としてから先頭に置く (重複を作らない)
  const key = normalizeOptionCode(next);
  // 🚨 同じ資材が 2 件目以降にあるときは、**その資材の指定 (小分け袋・何個ずつ) ごと**先頭へ動かす。
  //    1 件目の指定を残して名前だけ差し替えると、袋の「10 個ずつ」が消える (Codex 2026-10-01)
  const existing = list.find((m) => normalizeOptionCode(m.code) === key);
  const rest = list.slice(1).filter((m) => normalizeOptionCode(m.code) !== key);
  const base = existing || list[0];
  const head = base ? { ...base, code: next } : { code: next, usage: 'normal' };
  return [head, ...rest];
}

/** 画面・通知の文字列 ("D-8 ＋ 313ビニール袋 (10個ずつ)")。ログと GChat で使う */
export function materialsText(list) {
  return (list || []).map((m) => (m.usage === 'inner_pack'
    ? `${m.code}${m.units_per_pack != null ? ` (${m.units_per_pack}個ずつ)` : ' (小分け袋)'}`
    : m.code)).join(' ＋ ');
}

/**
 * 足りない項目 (⚠未登録バッジの根拠)。
 *   資材が 0 件                        → '資材'
 *   小分け袋なのに何個ずつか無い      → '小分け数'
 * 2 件目そのものは任意なので、無くても足りない扱いにしない
 */
export function materialsMissing(list) {
  const out = [];
  if ((list || []).length === 0) out.push('資材');
  else if ((list || []).some((m) => m.usage === 'inner_pack' && m.units_per_pack == null)) out.push('小分け数');
  return out;
}
