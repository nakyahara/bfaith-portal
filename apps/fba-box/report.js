/**
 * FBA箱詰め記録 — 本社向けの納品まとめ (中原さん 2026-09-10)
 *
 * いろはが iPad で「作業を終える」と、本社の Google チャットに通知が届き (notify.js)、そのリンク先がこのまとめ
 * (views/report.ejs)。本社はこれを見て
 *   ① 送り状 — 個口数 (箱の数)・箱ごとの資材 (サイズ)・外寸・重さ
 *   ② Seller Central (STA) の輸送箱ラベル — 梱包グループごとの箱・重さ・外寸・Amazon の箱番号
 * を用意する。予定と違う商品は赤く出す (alert)。
 *
 * 読むだけ (DB を書かない)。数字の出どころは getRunState = iPad・出荷前チェック・Excel 出力と同じなので食い違わない。
 */
import { getRunState, listMaterials, SHORTAGE_REASON_JA } from './db.js';

/** 完了判定から外している行 (Excel に無い = プラン外 / Excel の差し替えで外れた)。出荷前チェックと同じ */
const EXCLUDED = new Set(['picking_only', 'retired']);
const parseJson = (s) => { try { return s == null ? null : JSON.parse(s); } catch { return null; } };
const round1 = (n) => Math.round(Number(n) * 10) / 10;

/** 不足の理由 (内訳があれば「破損 2 + 今回は納品しない 3」) */
function reasonJaOf(r) {
  if (!(r.shortage_qty > 0)) return null;
  const arr = parseJson(r.shortage_detail);
  if (Array.isArray(arr) && arr.length) return arr.map((x) => `${SHORTAGE_REASON_JA[x.reason] || x.reason} ${x.qty}`).join(' + ');
  return SHORTAGE_REASON_JA[r.shortage_reason] || r.shortage_reason || null;
}

/**
 * @returns {null | {run, limitKg, totals, groups: Array<{boxes, rows}>, expiries}}
 */
export function buildRunReport(runId) {
  const st = getRunState(runId);
  if (!st) return null;
  const { run, groups, rows, boxes, placements, weightLimits } = st;
  const mats = new Map(listMaterials(true).map((m) => [m.code, m]));
  // 取消した箱と、中身の無い箱 (作業中の回で作っただけの箱) は送らないので出さない
  const live = boxes.filter((b) => b.status !== 'void' && b.total_qty > 0);
  const boxById = new Map(live.map((b) => [b.id, b]));
  const rowById = new Map(rows.map((r) => [r.id, r]));

  const rowExp = new Map();    // row_id → Map(期限 → 個数)
  const rowBox = new Map();    // row_id → Map(box_id → 個数)
  const boxItems = new Map();  // box_id → Map(`row|期限` → {rowId, expiry, qty})
  const add = (m, k, q) => m.set(k, (m.get(k) || 0) + q);
  for (const p of placements) {
    if (!boxById.has(p.box_id)) continue;
    const exp = p.expiry || '';
    if (!rowExp.has(p.row_id)) rowExp.set(p.row_id, new Map());
    add(rowExp.get(p.row_id), exp, p.qty);
    if (!rowBox.has(p.row_id)) rowBox.set(p.row_id, new Map());
    add(rowBox.get(p.row_id), p.box_id, p.qty);
    if (!boxItems.has(p.box_id)) boxItems.set(p.box_id, new Map());
    const bi = boxItems.get(p.box_id);
    const k = `${p.row_id}|${exp}`;
    if (!bi.has(k)) bi.set(k, { rowId: p.row_id, expiry: p.expiry || null, qty: 0 });
    bi.get(k).qty += p.qty;
  }

  const limitG = weightLimits?.limitG ?? null;
  const boxesOut = live.map((b) => {
    const m = mats.get(b.material_code);
    const dims = m && m.width_cm > 0 && m.length_cm > 0 && m.height_cm > 0 ? { w: m.width_cm, l: m.length_cm, h: m.height_cm } : null;
    const kg = Number(b.measured_weight_kg) > 0 ? Number(b.measured_weight_kg) : null;
    return {
      id: b.id, groupId: b.pack_group_id, code: b.box_code, boxNo: b.box_no,
      amazonBoxNo: b.amazon_box_no, amazonName: b.amazon_name, numberShifted: b.amazon_box_no !== b.box_no,
      status: b.status, qty: b.total_qty, kinds: b.sku_count, weightKg: kg,
      material: m ? m.name : (b.material_code || null), dims, sum3: dims ? round1(dims.w + dims.l + dims.h) : null,
      overLimit: b.status === 'closed' && kg != null && limitG != null && kg * 1000 > limitG,
      limitOverrideBy: b.limit_override_by || null, closedBy: b.closed_by || null, closedAt: b.closed_at || null,
      contents: [...(boxItems.get(b.id) || new Map()).values()].map((x) => {
        const r = rowById.get(x.rowId) || {};
        return { planNo: r.plan_no || null, name: r.product_name || r.seller_sku || '', fnsku: r.fnsku || '', sku: r.seller_sku || '', expiry: x.expiry, qty: x.qty };
      }),
    };
  });

  const rowsOut = rows
    // プラン外の行は、箱に入っているときだけ出す (入っていなければ送らない = 本社がやることは無い)
    .filter((r) => !EXCLUDED.has(r.match_state) || r.placed > 0)
    .map((r) => {
      const excluded = EXCLUDED.has(r.match_state);
      const shortage = r.shortage_qty || 0;
      const remaining = excluded ? 0 : Math.max(0, r.planned_qty - r.placed - shortage);
      const diff = r.placed - r.planned_qty;
      return {
        id: r.id, groupId: r.pack_group_id, planNo: r.plan_no, name: r.product_name || r.seller_sku || '', fnsku: r.fnsku, sku: r.seller_sku,
        planned: r.planned_qty, placed: r.placed, shortage, remaining, diff, excluded,
        reasonJa: reasonJaOf(r),
        note: excluded ? (r.match_state === 'picking_only' ? 'STA のプラン (Excel) に無い商品です' : 'Excel の差し替えで外れた商品です') : null,
        // 🟥 予定と違う = 箱に入れた数が予定と違う (不足・未投入)。プラン外なのに入っている行も
        alert: excluded ? r.placed > 0 : diff !== 0,
        expiries: [...(rowExp.get(r.id) || new Map())].filter(([e]) => e).map(([expiry, qty]) => ({ expiry, qty }))
          .sort((a, b) => a.expiry.localeCompare(b.expiry)),
        inBoxes: [...(rowBox.get(r.id) || new Map())].map(([boxId, qty]) => {
          const b = boxById.get(boxId);
          return { code: b.box_code, amazonName: b.amazon_name, boxNo: b.box_no, qty };
        }).sort((a, b) => a.boxNo - b.boxNo),
      };
    });

  const counted = rowsOut.filter((r) => !r.excluded);
  const totals = {
    boxes: boxesOut.length,
    openBoxes: boxesOut.filter((b) => b.status !== 'closed').length,
    weightKg: round1(boxesOut.reduce((a, b) => a + (b.weightKg || 0), 0)),
    noWeight: boxesOut.filter((b) => b.weightKg == null).length,
    noDims: boxesOut.filter((b) => !b.dims).length,
    overLimitBoxes: boxesOut.filter((b) => b.overLimit).length,
    planned: counted.reduce((a, r) => a + r.planned, 0),
    placed: rowsOut.reduce((a, r) => a + r.placed, 0),
    // 🚨 予定 (planned) と比べるのは「予定の商品を入れた数」。プラン外の商品は別に数える (Codex PR #1307 R1 P2: 母集団をそろえる)
    placedInPlan: counted.reduce((a, r) => a + r.placed, 0),
    placedExtra: rowsOut.filter((r) => r.excluded).reduce((a, r) => a + r.placed, 0),
    kinds: rowsOut.filter((r) => r.placed > 0).length,
    diffRows: rowsOut.filter((r) => r.alert).length,
    remaining: counted.reduce((a, r) => a + r.remaining, 0),
  };

  const groupsOut = groups.map((g) => ({
    id: g.id, name: g.sheet_name, displayName: g.display_name, packingGroupId: g.packing_group_id || null, excelAttached: !!g.excel_file_id,
    boxes: boxesOut.filter((b) => b.groupId === g.id),
    rows: rowsOut.filter((r) => r.groupId === g.id),
  })).filter((g) => g.boxes.length > 0 || g.rows.length > 0);

  // 期限一覧 (STA 画面へ転記。Excel には期限を書かない — 要件 F-7b)
  const expiries = rowsOut.flatMap((r) => r.expiries.map((e) => ({ fnsku: r.fnsku, sku: r.sku, name: r.name, expiry: e.expiry, qty: e.qty })))
    .sort((a, b) => (a.fnsku || '').localeCompare(b.fnsku || '') || a.expiry.localeCompare(b.expiry));

  const out = {
    run: { id: run.id, title: run.title, status: run.status, deliveryDate: run.delivery_date || null, doneAt: run.done_at || null, staUploadedAt: run.sta_uploaded_at || null },
    limitKg: limitG != null ? limitG / 1000 : null,
    totals, groups: groupsOut, expiries,
  };
  out.tsv = reportTsv(out);
  return out;
}

/**
 * 表計算に貼る 1 マス。🚨 Excel・picking 由来の文字をそのまま入れない (Codex PR #1307 R1 P2):
 *   タブ・改行は列や行を壊すので空白に / = + - @ で始まる文字は式として動くことがあるので先頭に ' を付ける
 *   (数値はそのまま — マイナスの数も数値として貼る)
 */
export function tsvCell(v) {
  if (v == null) return '';
  if (typeof v === 'number') return String(v);
  const s = String(v).replace(/[\t\r\n]+/g, ' ');
  return /^[=+\-@]/.test(s) ? `'${s}` : s;
}
const tsvTable = (rows) => rows.map((row) => row.map(tsvCell).join('\t')).join('\n');

/** ページのコピー用 (箱の一覧は梱包グループごと・期限一覧)。列の順は管理画面の期限コピーと同じ */
export function reportTsv(rep) {
  const out = {};
  for (const g of rep.groups) {
    if (g.boxes.length === 0) continue;
    out[`box${g.id}`] = tsvTable([['Amazonの箱', '箱札', '資材', '重さkg', '幅cm', '長さcm', '高さcm', '個数'],
      ...g.boxes.map((b) => [b.amazonName, b.code, b.material, b.weightKg, b.dims?.w, b.dims?.l, b.dims?.h, b.qty])]);
  }
  if (rep.expiries.length > 0) out.exp = tsvTable([['FNSKU', 'SKU', '商品', '期限', '個数'], ...rep.expiries.map((e) => [e.fnsku, e.sku, e.name, e.expiry, e.qty])]);
  return out;
}

const pad2 = (n) => String(n).padStart(2, '0');
function jstWhen(d) {
  const j = new Date(d.getTime() + 9 * 3600 * 1000);
  return `${j.getUTCMonth() + 1}/${j.getUTCDate()} ${pad2(j.getUTCHours())}:${pad2(j.getUTCMinutes())}`;
}

/**
 * 完了通知 (Google Chat) の本文。読み手ファースト: 何が終わったか → 数 → 気をつけること → だれが・いつ → リンク。
 * リンクは Google Chat の書式 `<url|文字>`
 */
export function runDoneText(rep, { link = null, doneBy = null, at = new Date() } = {}) {
  const t = rep.totals;
  const lines = [
    `📦 *FBA箱詰めが終わりました* — ${rep.run.title}`,
    `箱 ${t.boxes} 箱 ・ 合計 ${t.weightKg} kg ・ 商品 ${t.kinds} 種類 ${t.placed} 個`,
  ];
  if (t.diffRows > 0) lines.push(`⚠ 予定と違う商品 ${t.diffRows} 件 (予定 ${t.planned} 個 → 予定の商品を箱に入れた ${t.placedInPlan} 個)`);
  if (t.placedExtra > 0) lines.push(`🟥 STA のプランに無い商品が ${t.placedExtra} 個 箱に入っています (送る前に確認してください)`);
  const over = rep.groups.flatMap((g) => g.boxes).filter((b) => b.overLimit);
  if (over.length > 0) lines.push(`🚨 ${rep.limitKg}kg を超えた箱 ${over.length} 箱 (${over.map((b) => `${b.code} ${b.weightKg}kg`).join('、')})`);
  if (t.noDims > 0) lines.push(`📏 外寸が未登録の資材の箱 ${t.noDims} 箱 (送り状・箱ラベルの前に外寸を確認してください)`);
  lines.push(`終えた人: ${doneBy || '—'} ・ ${jstWhen(at)}`);
  lines.push(link ? `→ <${link}|送り状・箱ラベル用の一覧を開く>` : '→ 一覧は FBA箱詰め記録の管理画面から');
  return lines.join('\n');
}
