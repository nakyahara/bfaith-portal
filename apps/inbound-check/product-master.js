/**
 * ロジザード商品マスタ (エクスポート[FM08_01] 種類=商品 / パターン=デフォルト) の取込。
 *
 * ⭐目的は1つだけ: **「期限管理あり/なし」の正本を取ること**。
 *   入荷受付CSV (FA04_01) には期限管理の設定が出てこないため、それまでは
 *   「在庫データに有効期限が入っていれば期限管理商品」と推定していた。
 *   在庫ゼロの商品は在庫ミラーに行が無く推定できないので、商品マスタから取り直す
 *   (中原さん 2026-09-01:「エクスポート[FM08_01]の商品のデフォルトをDLして
 *    有効期限区分を参照したら有効期限情報は取れる」)。
 *
 * fail-closed:
 *   - 必須列 (商品ID / 有効期限区分) が無ければ拒否。列名が変わったのを黙って通さない
 *   - 壊れた CP932・列数不一致は拒否 (csv.js と同じ考え方)
 *   - 0行は拒否。商品マスタが空になることは無く、空を通すと全商品の期限管理が消える
 *
 * 期限管理の判定は「区分の値そのもの」を見る。ロジザード側の表記が
 * 「0:管理しない / 1:賞味期限 …」なのか「なし/あり」なのかは環境依存なので、
 * **見つかった値の内訳を必ず結果に含める** (初回の実データで中原さんが確認できるように)。
 */
import { getDB } from './db.js';
import { decodeCsvBuffer, parseCsv } from './csv.js';

const NAME_COL = '商品ID';
const KUBUN_COL = '有効期限区分';

// ⭐実データで確定 (2026-09-01)。ロジザードの有効期限区分は**ゼロ埋め2桁のコード**で、
//   01 = 無し / 02 = 有効期限あり。在庫データ (mirror_logizard_stock の有効期限) と
//   突き合わせて 2,875 件を照合し、**例外ゼロ**で一致した:
//     在庫に有効期限が入っている商品 → 全部 02 (142件)
//     在庫に有効期限が無い商品       → 全部 01 (2,733件)
//   数値コードは先頭ゼロを外して見るので、01 でも 1 でも同じ扱いになる。
//   03 以降 (製造日・消費期限など別の期限種別) が増えても「管理する」に入る。
const NOT_MANAGED_RE = /^(なし|無し|無|しない|管理しない|対象外|-|－)$/;

export function isExpiryManagedValue(v) {
  const s = String(v == null ? '' : v).trim();
  if (s === '') return false;                    // 空欄 = 未設定 = 管理しない
  if (/^\d+$/.test(s)) return Number(s) >= 2;    // 00/0/01/1 = 無し、02 以降 = 期限あり
  // 数字でない表記 (「有り」「賞味期限」等) は、はっきり「無し」と読めるものだけ除く。
  // 迷う値は「管理する」に倒す — 期限を聞かずに通してしまう方が危ないため
  return !NOT_MANAGED_RE.test(s);
}

/**
 * 商品マスタ CSV を読む。
 * @returns {{rows: Array<{code_key, product_id, kubun, managed}>, header: string[], kubunCounts: object}}
 * @throws {Error} 検証に落ちたら (error.code = 'bad_csv')
 */
export function parseProductMasterCsv(buffer) {
  const bad = (msg) => { const e = new Error(msg); e.code = 'bad_csv'; throw e; };
  if (!buffer || buffer.length === 0) bad('ファイルが空です');
  let text;
  try {
    text = decodeCsvBuffer(buffer);
  } catch (e) {
    bad(`Shift-JIS として読めません (${e.message})`);
  }
  let rows;
  try {
    rows = parseCsv(text);
  } catch (e) {
    bad(e.message);
  }
  if (rows.length === 0) bad('中身がありません');
  const header = rows[0].map(h => String(h || '').trim());
  for (const col of [NAME_COL, KUBUN_COL]) {
    if (!header.includes(col)) {
      bad(`必須列「${col}」がありません (実際の列: ${header.slice(0, 12).join(' / ')}${header.length > 12 ? ' …' : ''})`);
    }
  }
  const dup = header.filter((h, i) => h && header.indexOf(h) !== i);
  if (dup.length) bad(`列名が重複しています: ${[...new Set(dup)].join(', ')}`);
  const iId = header.indexOf(NAME_COL);
  const iKubun = header.indexOf(KUBUN_COL);

  const out = [];
  const kubunCounts = {};
  const seen = new Set();
  for (let r = 1; r < rows.length; r++) {
    const row = rows[r];
    if (row.length === 1 && String(row[0] || '').trim() === '') continue;   // 末尾の空行
    if (row.length !== header.length) {
      bad(`${r + 1} 行目の列数が違います (ヘッダ ${header.length} 列 / この行 ${row.length} 列)`);
    }
    const productId = String(row[iId] || '').trim();
    if (!productId) continue;                     // 商品IDが無い行は読み飛ばす (集計行など)
    const codeKey = productId.toLowerCase();
    if (seen.has(codeKey)) continue;              // 同じ商品が2度出たら先勝ち
    seen.add(codeKey);
    const kubun = String(row[iKubun] || '').trim();
    kubunCounts[kubun || '(空欄)'] = (kubunCounts[kubun || '(空欄)'] || 0) + 1;
    out.push({ code_key: codeKey, product_id: productId, kubun, managed: isExpiryManagedValue(kubun) });
  }
  if (out.length === 0) bad('商品が1件もありません (商品マスタが空になることは無いため取込を中止しました)');
  return { rows: out, header, kubunCounts };
}

/**
 * 取り込んで f_inbound_check_product_flags を更新する。
 *
 * ⭐**ロジザードの値を正とする** (CLAUDE.md「正本優先」)。人が手で直した値も上書きするが、
 *   黙って消さずに「手動設定を何件上書きしたか」を返す。手動はあくまで商品マスタを
 *   取り込めるようになるまでの応急処置だったため。
 */
export function importProductMaster(buffer, { actor = null } = {}) {
  const parsed = parseProductMasterCsv(buffer);
  const db = getDB();
  const now = new Date().toISOString();
  const by = String(actor || 'logizard').trim() || 'logizard';

  const sel = db.prepare('SELECT expiry_managed, source FROM f_inbound_check_product_flags WHERE code_key = ?');
  const ins = db.prepare(`INSERT INTO f_inbound_check_product_flags (code_key, expiry_managed, source, updated_at, updated_by)
    VALUES (?, ?, 'logizard', ?, ?)
    ON CONFLICT(code_key) DO UPDATE SET expiry_managed = excluded.expiry_managed,
      source = 'logizard', updated_at = excluded.updated_at, updated_by = excluded.updated_by`);

  const stats = { total: parsed.rows.length, managed: 0, changed: 0, overroteManual: 0, kubunCounts: parsed.kubunCounts };
  db.transaction(() => {
    for (const r of parsed.rows) {
      if (r.managed) stats.managed++;
      const cur = sel.get(r.code_key);
      if (cur) {
        const was = !!cur.expiry_managed;
        if (was !== r.managed) {
          stats.changed++;
          if (cur.source === 'manual') stats.overroteManual++;
        }
      } else if (r.managed) {
        stats.changed++;   // 新しく「期限管理あり」になった
      }
      ins.run(r.code_key, r.managed ? 1 : 0, now, by);
    }
  }).immediate();
  return { ok: true, ...stats };
}

/** 商品マスタから何件取り込めているか (管理画面の表示用) */
export function productMasterStatus() {
  const db = getDB();
  const r = db.prepare(`SELECT COUNT(*) AS total, SUM(expiry_managed) AS managed, MAX(updated_at) AS at
    FROM f_inbound_check_product_flags WHERE source = 'logizard'`).get();
  return { total: r?.total || 0, managed: r?.managed || 0, updatedAt: r?.at || null };
}
