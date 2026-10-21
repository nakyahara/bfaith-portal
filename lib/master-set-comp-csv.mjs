/**
 * master-set-comp-csv.mjs — 今あるセットの中身 (構成) の依頼を NE に入れる「セットの中身の CSV」(2026-10-21 中原さん「近道を作って」・PR-0b)
 *
 * 流れ (構成の持ち主は NE (load) のまま = core.sku_components は今までどおり夜間ロードが NE から書く):
 *   商品の画面で構成を変える = 構成の依頼 (ops.sku_component_requests) → ここで CSV (足す・数を変える) / NE の画面 (外す・入れ替える) →
 *   翌朝の NE の取得 → その夜の夜間ロードが core を NE の中身にし、依頼を閉じる (lib/master-write.mjs の closeRequestOnLoad)
 * NE の決まり (公開マニュアル・AI_reference CompanyDB構想/10 §6.2):
 *   セット商品情報 CSV で構成品を足す・数量を変えられる (数量を変えるときは syohin_code も入れる)。構成品を外す・入れ替えるは CSV ではできない (NE の画面で)
 * ファイル = 見出し set_syohin_code,syohin_code,suryo・UTF-8 (BOM なし)・CRLF (新商品の NE 登録の CSV と同じ書き方 = lib/master-reg-csv.mjs の buildRegCsv)。
 *   依頼の構成の全部の行を書く (変えない構成品も依頼の数量で書く = NE が「書いた行だけにする」動きでも「足す・数を変える」動きでも依頼の中身に近づく。
 *   外す行は書けない = NE の画面で)。セットのコード・構成品のコードは NE の書き方 (0041 の元の書き方 = ops.ne_reg_ne_codes)
 * 入れないセット (理由を画面に): 足す・数を変えるが無い (外すだけ = NE の画面で)・NE の書き方が分からない・構成品が NE 確認済みでない・
 *   新しいセット (NE 登録の前 = 新商品の NE 登録の CSV で送る)
 * 🚨 DB には記録しない (ファイルは開いている依頼からいつでも作り直せる・数量は足し算でなく値)。届いたかは翌朝の NE の取得と夜の依頼の閉じ方で決まる。読むだけ (書き込みは無い)
 *   🚨 古いファイルは取り込まない (PR-0b Codex R1 High): 作った後に依頼を直した・取り下げた・NE で直したなら、前のファイルは前の中身 = 取り込むと戻る (依頼が閉じた後なら夜間ロードがその中身を入れる)。
 *     画面・つかいかた・保存の後の一言で「作り直す・前のファイルは捨てる」と出す。ファイル名に作った時刻
 * 🚨 作る前に確かめる (PR-0b Codex R1 Medium): NE のコードの台帳が今朝の照合の回 (= 最新の照合の回が今日) のものか・構成品が今も単品か
 */
import crypto from 'node:crypto';

import { neCodeLookup, buildRegCsv } from './master-reg-csv.mjs';
import { todayRun } from '../apps/master-decisions/ne-csv.mjs';

export const SET_COMP_HEADER = Object.freeze(['set_syohin_code', 'syohin_code', 'suryo']);
const NE_CONFIRMED = new Set(['ne_confirmed', 'distributable', 'available']);
const NEW_SET_STATES = new Set(['draft', 'ne_pending']);
/** 入れない理由の言葉 (画面の 1 行) */
export const SET_COMP_WHY = Object.freeze({
  remove_only: '外す・入れ替えだけなので、NE の画面で直してください (CSV では外せません)',
  ne_code: 'NE の商品コードの書き方が分からないので CSV に入れません (朝の照合の後にもう一度)',
  child_not_in_ne: '構成品がまだ NE にありません (NE確認済みになってから)',
  child_not_single: '構成品がセットか例外の商品になっています (セットの入れ子はできません。商品の画面で依頼を直してください)',
  ne_codes_old: '今朝の NE の取り込みと照合がまだ終わっていないので、CSV に入れません (照合の後にもう一度)',
  new_set: '新しいセット (NE 登録の前) なので、NE 登録の CSV で送ります',
});

/**
 * 開いている構成の依頼 (セット) の一覧と、CSV に入れる行。戻り値 { items: [...], csvSets, ready } (表が無い DB = ready false)
 *   item = { code, name, sku_id, request_id, created_at, requested_by, rows: [{ code, qty }], current: [{ code, qty }], add, qty, remove, in_csv, why, cells }
 */
export async function readSetCompRequests(db, { nowMs = Date.now() } = {}) {
  const has = (await db.query("select to_regclass('ops.sku_component_requests') is not null as ok")).rows[0].ok === true;
  if (!has) return { ready: false, items: [], csvSets: 0 };
  const reqs = (await db.query(`select q.component_request_id::text as request_id, q.set_sku_id::text as sku_id, q.rows, q.created_at::text as created_at, q.requested_by,
        s.code, s.code_norm, s.name, r.state as reg_state
      from ops.sku_component_requests q join core.skus s on s.sku_id = q.set_sku_id left join ops.master_registrations r on r.sku_id = s.sku_id
     where q.status = 'open' and s.sku_kind = 'set' and coalesce(r.state, 'available') <> 'cancelled'
     order by s.code_norm`)).rows;
  if (!reqs.length) return { ready: true, items: [], csvSets: 0 };
  const setIds = reqs.map((x) => x.sku_id);
  const cur = (await db.query(`select c.parent_sku_id::text as set_id, c.child_sku_id::text as sku_id, k.code, k.code_norm, c.qty, c.sort_order
      from core.sku_components c join core.skus k on k.sku_id = c.child_sku_id where c.parent_sku_id = any($1::bigint[]) order by c.parent_sku_id, c.sort_order, k.code_norm`, [setIds])).rows;
  const childIds = [...new Set(reqs.flatMap((x) => x.rows.map((r) => String(r.sku_id))))];
  const kids = new Map((await db.query(`select k.sku_id::text as id, k.code, k.code_norm, k.sku_kind, r.state from core.skus k left join ops.master_registrations r on r.sku_id = k.sku_id
     where k.sku_id = any($1::bigint[])`, [childIds])).rows.map((x) => [x.id, x]));
  const nc = await neCodeLookup(db, [...reqs.map((x) => x.code), ...[...kids.values()].map((k) => k.code)]);
  // NE のコードの台帳 = 今朝の照合の回 (最新の照合の回が今日・台帳の印がその回) のときだけ CSV に入れる (古い台帳の書き方で送らない)
  const tr = await todayRun(db, nowMs);
  const codesCurrent = !!tr.today && !!nc.run && nc.run === tr.run;
  const items = [];
  for (const q of reqs) {
    const now = cur.filter((c) => c.set_id === q.sku_id);
    const nowBy = new Map(now.map((c) => [c.sku_id, Number(c.qty)]));
    const rows = [...q.rows].sort((a, b) => Number(a.sort) - Number(b.sort)).map((r) => ({ sku_id: String(r.sku_id), code: kids.get(String(r.sku_id))?.code ?? r.code, qty: Number(r.qty) }));
    const want = new Set(rows.map((r) => r.sku_id));
    const add = rows.filter((r) => !nowBy.has(r.sku_id)).map((r) => ({ code: r.code, qty: r.qty }));
    const qty = rows.filter((r) => nowBy.has(r.sku_id) && nowBy.get(r.sku_id) !== r.qty).map((r) => ({ code: r.code, from: nowBy.get(r.sku_id), to: r.qty }));
    const remove = now.filter((c) => !want.has(c.sku_id)).map((c) => ({ code: c.code, qty: Number(c.qty) }));
    let why = null;
    let cells = [];
    // 新しいセット = 登録の状態が下書き・NE 登録待ち (NE 登録の CSV で送る)。今の構成が空でも NE にあるセット (要確認など) は今あるセットとして扱う (全部が「足す」)
    if (NEW_SET_STATES.has(q.reg_state)) why = 'new_set';
    else if (!add.length && !qty.length) why = 'remove_only';
    else if (!codesCurrent) why = 'ne_codes_old';
    else {
      const sp = await nc.get(`product|${q.code_norm}`);
      const kidCells = [];
      for (const r of rows) {
        const k = kids.get(r.sku_id);
        if (!k || k.sku_kind !== 'single') { why = 'child_not_single'; break; }
        if (!NE_CONFIRMED.has(k.state ?? 'available')) { why = 'child_not_in_ne'; break; }
        const ks = await nc.get(`product|${k.code_norm}`);
        if (!ks || ks.state !== 'ok') { why = 'ne_code'; break; }
        kidCells.push([ks.ne_code, r.qty]);
      }
      if (!why && (!sp || sp.state !== 'ok')) why = 'ne_code';
      if (!why) cells = kidCells.map(([kc, n]) => [sp.ne_code, kc, n]);
    }
    items.push({ code: q.code, name: q.name, sku_id: q.sku_id, request_id: q.request_id, created_at: q.created_at, requested_by: q.requested_by,
      rows: rows.map(({ code, qty: n }) => ({ code, qty: n })), current: now.map((c) => ({ code: c.code, qty: Number(c.qty) })), add, qty, remove,
      in_csv: !why, why, why_text: why ? SET_COMP_WHY[why] : null, manual_remove: remove.length > 0, cells });
  }
  return { ready: true, items, csvSets: items.filter((i) => i.in_csv).length, ne_codes: { run: nc.run, current: codesCurrent } };
}

/** 東京の YYYYMMDD_HHMMSS */
const jstStamp = (now = new Date()) => new Date(now.getTime() + 9 * 3600e3).toISOString().replace(/[-:]/g, '').replace('T', '_').slice(0, 15);

/**
 * CSV を作る (読むだけ・DB に記録しない)。入れるセットが無い = null。戻り値 { file_name, bytes, sha256, sets: [コード], rows (行の数) }
 */
export async function buildSetCompCsv(db, { now = new Date() } = {}) {
  const v = await readSetCompRequests(db, { nowMs: now.getTime() });
  const sets = v.items.filter((i) => i.in_csv);
  if (!sets.length) return null;
  const rows = sets.flatMap((i) => i.cells);
  const f = buildRegCsv({ header: SET_COMP_HEADER }, rows);
  return { file_name: `ne_set_components_${jstStamp(now)}.csv`, bytes: f.bytes, sha256: f.sha256 || crypto.createHash('sha256').update(f.bytes).digest('hex'),
    sets: sets.map((i) => i.code), rows: rows.length };
}
