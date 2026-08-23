/**
 * ne-products 向けの service-api (packing standalone → warehouse ローカル呼び出し)。
 *
 * 梱包画面の商品名表示用: NEから毎日取り込む商品マスタ (raw_ne_products) の
 * 「社内の単品商品名」を商品コードで一括解決する (中原さん指示 2026-08-22 —
 * 納品書CSVの印字商品名はセット品受注でモールのSEO長文になるため使わない)。
 *
 * 🚨絶対ルール (中原さん 2026-08-23): **セット商品の名前は決して使わない。必ず単品の商品名。**
 *   NEは受注時点でセットを単品コードに分解して納品書CSVを作るので、本来ここに来る
 *   コードは単品のはず。それでも構造で保証するため:
 *   - raw_ne_set_products に親として1行でもあるコードは「セット」。構成単品へ展開し、
 *     セット商品自身の raw_ne_products 名は決して返さない (構成行が壊れていても単品扱いしない)
 *   - 展開が少しでも失敗したセット (構成単品の名前欠落・循環・入れ子の深さ超過・不正な数量)
 *     は部分結果を返さず unresolved に入れる → 呼び出し側は CSV名 (セット名の可能性) へ
 *     フォールバックせず「商品名未解決」を表示する
 *
 * - 認証は mount 側の serviceAuth (CF Access サービストークン + SERVICE_TOKEN Bearer)
 * - 読み取り専用
 */
import { Router } from 'express';
import { getDB } from './db.js';

const CHUNK = 500;        // SQLite のバインド変数上限を考慮
const MAX_CODES = 1000;   // 呼び出し側 (packing ne-names.js MAX_PER_CALL) と同値
const MAX_DEPTH = 3;      // セットの入れ子展開の上限 (展開回数)。超えたセットは unresolved

function chunked(arr, n) {
  const out = [];
  for (let i = 0; i < arr.length; i += n) out.push(arr.slice(i, i + n));
  return out;
}

/**
 * セット親の行 (lower親コード → [{child, qty}])。
 * 親の「存在判定」と構成取得を分けない代わりに、構成行が不正 (child空・数量不正) でも
 * 行として返す → 呼び出し側で「セットだが壊れている」と判定できる (単品にフォールバックしない)。
 */
function setRowsOf(db, codes) {
  const map = new Map();
  for (const chunk of chunked(codes, CHUNK)) {
    const rows = db.prepare(`
      SELECT TRIM(セット商品コード) AS parent, 商品コード AS child, 数量 AS qty
      FROM raw_ne_set_products
      WHERE TRIM(セット商品コード) COLLATE NOCASE IN (${chunk.map(() => '?').join(',')})
    `).all(...chunk);
    for (const r of rows) {
      const k = String(r.parent).toLowerCase();
      if (!map.has(k)) map.set(k, []);
      map.get(k).push({ child: String(r.child ?? '').trim(), qty: r.qty });
    }
  }
  return map;
}

/** 単品の商品名 (lowerコード → 商品名)。COLLATE NOCASE = このリポジトリの既存JOINと同じ流儀。 */
function productNamesOf(db, codes) {
  const map = new Map();
  for (const chunk of chunked(codes, CHUNK)) {
    const rows = db.prepare(`
      SELECT 商品コード AS code, 商品名 AS name FROM raw_ne_products
      WHERE 商品コード COLLATE NOCASE IN (${chunk.map(() => '?').join(',')})
        AND 商品名 IS NOT NULL AND TRIM(商品名) != ''
    `).all(...chunk);
    for (const r of rows) map.set(String(r.code).trim().toLowerCase(), String(r.name).trim());
  }
  return map;
}

/**
 * 商品コード → 単品の商品名 を一括解決する (純関数・テスト用にexport)。
 *   - 単品コード: raw_ne_products の商品名。未登録なら何も返さない (呼び出し側がCSV名へ)
 *   - セット商品コード: 構成単品へ展開し、単品名を ' / ' で連結。components に [{sku, name, qty}]。
 *     展開に少しでも失敗したら names に入れず unresolved に入れる (部分結果は返さない)
 * @returns {{ names: Record<string,string>, components: Record<string, Array<{sku:string,name:string,qty:number}>>, unresolved: string[] }}
 */
export function resolveSingleNames(db, codesIn) {
  // 入力正規化: 空白trim・空除外・大小文字違いは同一視 (先勝ち)
  const byLower = new Map();
  for (const c of codesIn || []) {
    const s = String(c ?? '').trim();
    if (s && !byLower.has(s.toLowerCase())) byLower.set(s.toLowerCase(), s);
  }
  const codes = [...byLower.values()].slice(0, MAX_CODES);
  const names = {};
  const components = {};
  const unresolved = [];
  if (codes.length === 0) return { names, components, unresolved };

  const failed = new Set();   // lower root
  const leaves = new Map();   // root → [{code, qty}]
  let frontier = codes.map((c) => ({ root: c, code: c, qty: 1, path: new Set([c.toLowerCase()]) }));
  // depth = これまでの展開回数。MAX_DEPTH 回展開した後も「まだセット」なら深さ超過
  for (let depth = 0; depth <= MAX_DEPTH && frontier.length > 0; depth++) {
    const rows = setRowsOf(db, [...new Set(frontier.map((f) => f.code))]);
    const next = [];
    for (const f of frontier) {
      const rk = f.root.toLowerCase();
      if (failed.has(rk)) continue;
      const cs = rows.get(f.code.toLowerCase());
      if (!cs) {   // セット親ではない = 単品
        if (!leaves.has(f.root)) leaves.set(f.root, []);
        leaves.get(f.root).push({ code: f.code, qty: f.qty });
        continue;
      }
      if (depth === MAX_DEPTH) { failed.add(rk); continue; }   // 入れ子が深すぎる
      for (const c of cs) {
        const qty = c.qty;   // INTEGER列そのまま。文字列・小数・NULL・0以下は不正
        if (!c.child || typeof qty !== 'number' || !Number.isSafeInteger(qty) || qty <= 0) { failed.add(rk); break; }
        const ck = c.child.toLowerCase();
        if (f.path.has(ck)) { failed.add(rk); break; }   // 循環 (A→B→A)
        const total = f.qty * qty;
        if (!Number.isSafeInteger(total)) { failed.add(rk); break; }   // 入れ子の掛け算で桁あふれ
        next.push({ root: f.root, code: c.child, qty: total, path: new Set([...f.path, ck]) });
      }
    }
    frontier = next;
  }

  const leafCodes = [...new Set(
    codes.filter((c) => !failed.has(c.toLowerCase())).flatMap((c) => (leaves.get(c) || []).map((l) => l.code)),
  )];
  const nm = productNamesOf(db, leafCodes);
  for (const root of codes) {
    const rk = root.toLowerCase();
    if (!failed.has(rk)) {
      const ls = leaves.get(root) || [];
      const expanded = !(ls.length === 1 && ls[0].code.toLowerCase() === rk);   // セット展開が起きた
      const merged = new Map();   // 同じ単品が複数経路で出たら数量を合算
      let overflow = false;
      for (const l of ls) {
        const k = l.code.toLowerCase();
        const m = merged.get(k);
        if (m) {
          m.qty += l.qty;
          if (!Number.isSafeInteger(m.qty)) overflow = true;
        } else {
          merged.set(k, { sku: l.code, qty: l.qty });
        }
      }
      const parts = [...merged.values()].map((p) => ({ sku: p.sku, name: nm.get(p.sku.toLowerCase()) || null, qty: p.qty }));
      if (!overflow && parts.length > 0 && parts.every((p) => p.name)) {
        names[root] = parts.map((p) => p.name).join(' / ');
        if (expanded) components[root] = parts;
        continue;
      }
      if (!expanded) continue;   // 単品の未登録 → 何も返さない (CSVの商品名列=単品名へ)
      failed.add(rk);            // セットなのに構成単品の名前が1つでも欠ける → 部分結果を返さない
    }
    unresolved.push(root);
  }
  return { names, components, unresolved };
}

const router = Router();

router.post('/names', (req, res) => {
  const codes = Array.isArray(req.body?.codes) ? req.body.codes : [];
  res.json({ ok: true, ...resolveSingleNames(getDB(), codes) });
});

export default router;
