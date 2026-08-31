/**
 * Notion「商品ページ商品画像登録」DB (画像制作の管理台帳・自社商品のみ) → product-hub 移植。
 * 要件定義 = AI_reference『システム設計/Notion画像DB移植_要件定義_20260826.md』(中原さん判断 5 点確定)。
 *
 * 対象: Status が IMAGE_MIGRATE_STATUSES (構成作成中 / 画像作成中（高島） / 画像確認（田中） / 保留) のカード。
 *
 * 設計原則 (notion-import.js の ①〜⑥ 一括移植 #922 と同型 + Codex R1):
 *   - 読み取り専用。Notion へは一切書き戻さない。移植後は Notion 側の更新を止める (中原さん決定 5)
 *   - admin 限定・プレビュー (dryRun 既定) → 実行。実行はプレビューの snapshot 一致が必須
 *   - **フィールド単位の所有権**: 商品本体 (name/price/JAN…) は source を問わず触らない。
 *     画像関連の明示列だけ「アプリ側が空のときだけ」埋める (portal 起点でも可)。上書きしない
 *   - 商品コードは**原コードのまま** (代表コードへ正規化しない)。NE の子SKU なら detach を記録して
 *     独立ページにする = 画像DBのカード 1 枚 = 楽天 1 ページ (中原さん決定 2: 色ごとに別ページ)
 *   - 「保留」は画像制作だけの保留 (draft_image_production.workflow_state)。商品本流は止めない (決定 1)
 *   - 工程 (画像トラック) は、そのドラフトの画像工程が**全部まっさら** (todo・担当/メモ/期限なし・楽天未登録)
 *     のときだけ書く。人が動かした痕跡があれば工程は書かず報告 (進捗を巻き戻さない)
 *   - own_brand ⟺ image_priority=自社 の不変条件を壊さない: 別の重要度が入っていたら止めて報告 (決定 4)
 *   - 商品マスターに居るのにアプリに無い商品は、先に ①〜⑥ 一括移植 (または選択取り込み) をさせる
 *     (最小情報カードを先に作ると、後から本体情報が補完されないため — Codex R1)
 *   - 移植元カードは台帳 draft_image_notion_imports に 1 カード 1 行 (冪等キー = notion_page_id)
 */
import crypto from 'node:crypto';
import { getConfig, requireEnv, queryDatabaseAll, notionRequest, findPageByManageNumber } from '../../rakuten-yahoo-sync/lib/notion-client.js';
import {
  getDB, logEvent, upsertImageProduction, setImageWorkflowState,
  OWN_BRAND_IMAGE_PRIORITY, SOURCE_NOTION_IMPORT, DETAIL_V2_CODES, SHIPPING_TO_MATERIAL,
} from '../db.js';
import { resolveVariationGroup, isDetached } from '../lib/variation.js';
import { ensureProgress, setStepState } from '../lib/workflow-progress.js';

/** 対象ステータス (Notion の選択肢名そのまま equals で照会する。表記ゆれは吸収しない) */
export const IMAGE_MIGRATE_STATUSES = ['構成作成中', '画像作成中（高島）', '画像確認（田中）', '保留'];

/** 1 回の実行で書き込む上限 (同期 HTTP を長引かせない)。残りは deferred */
export const MAX_IMAGE_MIGRATE_PER_RUN = 100;

/** 画像DB用の Notion 設定。トークンは商品マスターと同じ integration (画像DBにも接続済みを 8/26 実測) */
export function getImageDbConfig() {
  return { ...getConfig(), databaseId: requireEnv('NOTION_IMAGE_DB_ID') };
}

const norm = (v) => (v == null ? '' : String(v).trim().toLowerCase());
const blank = (v) => v == null || String(v).trim() === '';
const clip = (v, n) => (blank(v) ? null : String(v).trim().slice(0, n));

function plain(prop) {
  if (!prop) return null;
  const t = prop.type;
  if (t === 'title' || t === 'rich_text') {
    const s = (prop[t] || []).map((x) => x.plain_text || '').join('').trim();
    return s === '' ? null : s;
  }
  if (t === 'select') return prop.select?.name || null;
  if (t === 'status') return prop.status?.name || null;
  if (t === 'url') return blank(prop.url) ? null : String(prop.url).trim();
  if (t === 'formula') {
    const f = prop.formula || {};
    return blank(f.string) ? null : String(f.string).trim();
  }
  return null;
}

/** http(s) の URL だけ受ける (Notion の url 型は任意文字列を許すため) */
function httpUrl(v, max = 1000) {
  const s = clip(v, max);
  if (!s) return null;
  return /^https?:\/\//i.test(s) ? s : null;
}

/**
 * Notion page → 平坦なレコード (pure。テスト可能にするため分離)。
 * @returns {object|null} 商品コードが無い page は null
 */
export function buildImageRecord(page) {
  const props = page?.properties || {};
  const neCode = clip(plain(props['商品コード']), 100);
  if (!neCode) return null;
  const rec = {
    notion_page_id: page.id,
    ne_code: neCode,
    name: clip(plain(props['Name']), 300) || neCode,
    status: clip(plain(props['Status']), 100),
    drive_folder_url: httpUrl(plain(props['グーグルドライブURL'])),
    amazon_url: httpUrl(plain(props['AmazonURL'])),
    asin: clip(plain(props['ASIN']), 20),
    importance_tier: clip(plain(props['重要商品区分']), 100),
    shipping_status: clip(plain(props['撮影商品発送']), 100),
    camera_instruction_url: httpUrl(plain(props['カメラ撮影指示URL'])),
    canva_url: httpUrl(plain(props['Canva'])),
    request_text: clip(plain(props['依頼文']), 10000),
    designer: clip(plain(props['画像作成担当者']), 100),
  };
  // 再実行時の「同じ内容 / 変わった」判定用。キー順が固定のオブジェクトリテラルなので決定的
  rec.source_hash = crypto.createHash('sha256').update(JSON.stringify(rec)).digest('hex').slice(0, 32);
  return rec;
}

/** 担当者名の照合: 「高島さん」「田中美祐」→ 高島 / 田中 を含む active な人。完全一致 → 部分一致、0件/複数は未割当 */
export function normalizeStaffName(v) {
  return norm(v).replace(/さん$/, '').replace(/[ 　]+/g, '');
}
export function findStaffByName(db, name) {
  const key = normalizeStaffName(name);
  if (!key) return { staff: null, reason: 'no_name' };
  const rows = db.prepare('SELECT id, name FROM ph_staff WHERE active = 1').all()
    .map((r) => ({ ...r, key: normalizeStaffName(r.name) }));
  const exact = rows.filter((r) => r.key === key);
  if (exact.length === 1) return { staff: exact[0], reason: null };
  if (exact.length > 1) return { staff: null, reason: 'ambiguous' };
  const partial = rows.filter((r) => r.key.includes(key) || key.includes(r.key));
  if (partial.length === 1) return { staff: partial[0], reason: null };
  return { staff: null, reason: partial.length > 1 ? 'ambiguous' : 'not_found' };
}

/**
 * Notion Status → 画像トラックの工程状態 (要件定義 §4)。TOP と 詳細の両 kind に同じ状態。
 *   構成作成中     … 依頼 (request) = doing  ← 「依頼を成立させる構成・準備も request 工程に含む」(中原さん決定 3)
 *   画像作成中     … request done / production doing (担当 = 高島)
 *   画像確認       … request・production done / register doing (アプリに画像が既にあれば register done・approve doing)。承認担当 = 田中
 *   保留           … 工程は書かない。画像制作だけ保留 (workflow_state)
 * 撮影依頼中 (詳細のみ) は 撮影商品発送 から: 撮影依頼不要 → skip / 発送手配済み → done / 社内準備 → 段階で doing|done
 * @returns {{ steps: Array<{code:string, state:string}>, hold: boolean, assigneeName: string|null, assigneeStepStage: string|null }}
 */
export function planStepsFor(status, _opts = {}) {
  // 画像工程 v2 (2026-08-26): 詳細系列 ①〜⑨ (TOP 側は ⑤/⑥-2 の完了で自動追随するので書かない)
  //   構成作成中       → ① done / ② 構成 doing
  //   画像作成中（高島） → ①〜④ done / ⑤ デザイン修正 doing (担当 高島)
  //   画像確認（田中）   → ①〜⑤ done / ⑥-1 社内確認 (田中) doing (担当 田中)
  //   保留             → 工程は書かない。画像制作だけ保留
  const out = { steps: [], hold: false, assigneeName: null, assigneeStepCode: null };
  const st = String(status || '');
  if (st === '保留') { out.hold = true; return out; }
  let doingCode = null;
  if (st === '構成作成中') doingCode = 'imgd_compose';
  else if (st.startsWith('画像作成中')) { doingCode = 'imgd_design'; out.assigneeName = '高島'; out.assigneeStepCode = 'imgd_design'; }
  else if (st.startsWith('画像確認')) { doingCode = 'imgd_review_1'; out.assigneeName = '田中'; out.assigneeStepCode = 'imgd_review_1'; }
  else return out; // 対象外のステータスは何も書かない
  const idx = DETAIL_V2_CODES.indexOf(doingCode);
  DETAIL_V2_CODES.forEach((code, i) => {
    if (i < idx) out.steps.push({ code, state: 'done' });
    else if (i === idx) out.steps.push({ code, state: 'doing' });
  });
  return out;
}

/** 画像トラックが「まっさら」か (工程を書いてよい条件)。人が動かした痕跡があれば理由を返す */
export function imageTrackUntouchedReason(db, draftId, { ensure = true } = {}) {
  const id = Number(draftId);
  // プレビュー (ensure=false) は DB を書かない (Codex R4 medium: 工程行の自己修復も書き込み)。
  // 行が 1 つも無い = まだ誰も触っていない、として扱う。実行時 (ensure=true) は行を作ってから判定する
  if (ensure) ensureProgress(db, id);
  // 担当は「役割の既定担当」が ensureProgress で自動で入るので、既定と同じ担当は人の痕跡とみなさない
  const rows = db.prepare(`
    SELECT p.state, p.assignee_id, p.note, p.due_date,
           (SELECT sr.staff_id FROM ph_staff_roles sr WHERE sr.role_code = s.role_code AND sr.is_default = 1 LIMIT 1) AS default_staff_id
    FROM draft_step_progress p
    JOIN ph_steps s ON s.code = p.step_code AND s.active = 1
    WHERE p.draft_id = ? AND s.track = 'image'
  `).all(id);
  if (rows.length === 0) return ensure ? '画像トラックの工程がありません' : null;
  if (rows.some((r) => r.state !== 'todo')) return '画像工程が既に動いています';
  if (rows.some((r) => (r.assignee_id != null && r.assignee_id !== r.default_staff_id) || !blank(r.note) || !blank(r.due_date))) {
    return '画像工程に担当・メモ・期限が入っています';
  }
  const listed = db.prepare('SELECT 1 FROM draft_rakuten WHERE draft_id = ? AND registered_at IS NOT NULL').get(id);
  if (listed) return '楽天登録済みです';
  return null;
}

/** 台帳 */
function ledgerOf(db, pageId) {
  return db.prepare('SELECT * FROM draft_image_notion_imports WHERE notion_page_id = ?').get(pageId) || null;
}

/**
 * 対象カードの分類 (書き込みなし) → dryRun=false なら実行。dryRun / 実行で同じ判定を通る。
 * @returns {Promise<{ statuses, missingStatuses, total, snapshot, results, summary }>}
 */
export async function importImageDbByStatus({
  actor = null, dryRun = true, expectedSnapshot = null, maxPerRun = MAX_IMAGE_MIGRATE_PER_RUN,
  query = queryDatabaseAll, request = notionRequest, config = getImageDbConfig,
  masterFinder = findPageByManageNumber,
  runId = null,
} = {}) {
  const cfg = config();
  const schema = await request(`/databases/${cfg.databaseId}`, { method: 'GET', cfg });
  const statusProp = schema?.properties?.Status;
  const options = (statusProp?.select?.options || statusProp?.status?.options || [])
    .map((o) => (o && o.name ? String(o.name) : null)).filter(Boolean);
  const targets = IMAGE_MIGRATE_STATUSES.filter((n) => options.includes(n));
  const missingStatuses = IMAGE_MIGRATE_STATUSES.filter((n) => !options.includes(n));
  // fail-closed: 選択肢が改名・削除されていたら「0 件成功」にしない
  if (targets.length === 0) {
    throw new Error(`Notion 画像DB の Status に対象の選択肢が見つかりません (${IMAGE_MIGRATE_STATUSES.join(' / ')})`);
  }
  // 実行 (書き込み) は選択肢が全部揃っているときだけ (Codex R2 high: 「保留」だけ改名されると
  // そのカードを欠落させたまま正常終了する)。プレビューは残りだけで見せて欠落を報告する
  if (!dryRun && missingStatuses.length > 0) {
    const e = new Error(`Notion 画像DB の Status に無い選択肢があります (${missingStatuses.join(' / ')})。選択肢名を確認してから実行してください`);
    e.code = 'missing_statuses';
    throw e;
  }
  const filterType = statusProp?.type === 'status' ? 'status' : 'select';
  const { pages } = await query({
    cfg,
    filter: { or: targets.map((name) => ({ property: 'Status', [filterType]: { equals: name } })) },
    maxPages: 50,
  });

  const db = getDB();
  const results = [];
  const planned = []; // { rec, plan, result }
  // 同じ商品コードのカードが複数あるときは**全カード**を止める (Codex R2 medium: 「最初の1枚」は取得順依存)
  const codeCount = new Map();
  const recs = pages.map((page) => ({ page, rec: buildImageRecord(page) }));
  for (const { rec } of recs) if (rec) codeCount.set(norm(rec.ne_code), (codeCount.get(norm(rec.ne_code)) || 0) + 1);

  // 外部照会 (商品マスター) は**先に全部**済ませる。判定ループの途中で await すると、その間に
  // 判定済みのドラフトが変わっても plan (= snapshot) に写らない (Codex R3 medium)。
  // 以降の DB 判定は同期で一気に通す = 判定と snapshot が同じ瞬間の DB を見る
  const masterHits = new Map(); // key -> master page | null | Error
  for (const { rec } of recs) {
    if (!rec) continue;
    const key = norm(rec.ne_code);
    if (codeCount.get(key) > 1 || masterHits.has(key)) continue;
    if (ledgerOf(db, rec.notion_page_id)) continue;
    if (db.prepare('SELECT 1 FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(key)) continue;
    try {
      masterHits.set(key, await masterFinder(rec.ne_code));
    } catch (e) {
      masterHits.set(key, e instanceof Error ? e : new Error(String(e)));
    }
  }

  for (const { page, rec } of recs) {
    if (!rec) {
      results.push({ ne_code: '(商品コードなし)', outcome: 'failed', warnings: [], error: `Notion カードに商品コードがありません (page ${page?.id || '?'})` });
      continue;
    }
    const key = norm(rec.ne_code);
    const result = { ne_code: rec.ne_code, notion_status: rec.status, notion_page_id: rec.notion_page_id, name: rec.name, warnings: [] };
    results.push(result);
    if (codeCount.get(key) > 1) {
      result.outcome = 'duplicate';
      result.error = '同じ商品コードのカードが Notion 画像DB に複数あります。Notion 側で 1 枚にしてから再実行してください';
      continue;
    }

    // 台帳: 同じカードは二度取り込まない。内容が変わっていても追従しない (Notion 側は停止する運用) — 報告のみ
    const led = ledgerOf(db, rec.notion_page_id);
    if (led) {
      result.draftId = led.draft_id;
      result.outcome = led.source_hash === rec.source_hash ? 'already_migrated' : 'source_changed_after_migration';
      continue;
    }

    const existing = db.prepare(`
      SELECT id, source, name, own_brand, image_priority, drive_folder_url, amazon_url, asin
      FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?
    `).get(key);

    if (!existing) {
      // 商品マスターに居る商品は、先に本体を取り込ませる (最小情報カードを先に作らない — Codex R1)
      const master = masterHits.has(key) ? masterHits.get(key) : null;
      if (master instanceof Error) {
        result.outcome = 'failed';
        result.error = `商品マスターの照会に失敗: ${truncate(master)}`;
        continue;
      }
      if (master) {
        result.outcome = 'needs_master_import';
        result.master_status = master?.properties?.Status?.select?.name || master?.properties?.Status?.status?.name || null;
        result.error = '商品マスターにカードがあります。先に「ステータス①〜⑥の一括移植」か「商品コードで取り込み」で本体を入れてください';
        continue;
      }
    } else if (!blank(existing.image_priority) && existing.image_priority !== OWN_BRAND_IMAGE_PRIORITY) {
      // own_brand ⟺ 重要度=自社 の不変条件。別の重要度が入っている商品は自社商品に書き換えない (決定 4)
      result.draftId = existing.id;
      result.outcome = 'brand_priority_conflict';
      result.error = `画像の重要度が「${existing.image_priority}」で登録済みです (自社商品に変えるなら詳細画面で重要度を直してから再実行)`;
      continue;
    } else if (existing.image_priority === OWN_BRAND_IMAGE_PRIORITY && existing.own_brand !== 1) {
      // 逆方向の不整合 (重要度=自社なのに own_brand=0) も素通りさせない (Codex R2 medium)。起動時補正に任せず止めて報告
      result.draftId = existing.id;
      result.outcome = 'brand_priority_conflict';
      result.error = '重要度は自社商品なのに「自社商品」チェックが OFF です (詳細画面で基本情報を保存して整合させてから再実行)';
      continue;
    }

    // バリエーション: 子SKU なら detach して独立ページにする (決定 2)
    const plan = {
      create: !existing, draftId: existing?.id || null, detach: null, fill: {}, ip: {}, steps: [], hold: false,
      holdFrom: null, assignee: null,
    };
    const v = resolveVariationGroup(db, rec.ne_code, { withMembers: false });
    if (v.kind === 'conflict') {
      // NE に同じコードが複数 (表記ゆれ) → どのグループに属するか決められない。detach の要否を誤ると
      // 代表ページに残したまま独立ページも作る二重掲載になるので、何も書かず止める (Codex R2 high)
      result.draftId = existing?.id;
      result.outcome = 'variation_conflict';
      result.error = 'NE 商品マスタに同じ商品コードが複数あります (表記ゆれ)。NE 側を直してから再実行してください';
      continue;
    }
    if (v.kind === 'variation' && v.isChild && !isDetached(db, rec.ne_code)) {
      const rep = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(norm(v.groupKey));
      plan.detach = { groupKey: v.groupKey, repDraftId: rep?.id || null };
      result.warnings.push(`NE では ${v.groupKey} のバリエーション子SKU → 独立ページとして外します${rep ? ` (代表 #${rep.id} の SKU 一覧から外れます)` : ''}`);
    }

    // 商品本体の空欄補完 (画像関連の明示列だけ)
    const cur = existing || {};
    if (rec.drive_folder_url && blank(cur.drive_folder_url)) plan.fill.drive_folder_url = rec.drive_folder_url;
    else if (rec.drive_folder_url && !blank(cur.drive_folder_url) && cur.drive_folder_url.trim() !== rec.drive_folder_url) {
      result.warnings.push('画像フォルダURLがアプリ側と違います (アプリ側を優先・Notion の値は使いません)');
    }
    if (rec.amazon_url && blank(cur.amazon_url)) plan.fill.amazon_url = rec.amazon_url;
    if (rec.asin && blank(cur.asin)) plan.fill.asin = rec.asin;
    if (rec.amazon_url && rec.asin && !rec.amazon_url.toUpperCase().includes(rec.asin.toUpperCase())) {
      result.warnings.push('AmazonURL と ASIN が一致していません');
    }
    if (blank(cur.image_priority)) plan.fill.image_priority = OWN_BRAND_IMAGE_PRIORITY; // own_brand=1 と同時に

    // 画像制作情報の空欄補完
    const ipCur = existing ? (db.prepare('SELECT * FROM draft_image_production WHERE draft_id = ?').get(existing.id) || {}) : {};
    for (const f of ['importance_tier', 'shipping_status', 'camera_instruction_url', 'canva_url', 'request_text', 'designer']) {
      if (rec[f] && blank(ipCur[f])) plan.ip[f] = rec[f];
    }
    if (rec.status && blank(ipCur.status)) plan.ip.status = rec.status;
    // 撮影商品発送 → 撮影・素材ステータス (v2 の安定コード)。空のときだけ
    const mat = rec.shipping_status ? SHIPPING_TO_MATERIAL[rec.shipping_status] : null;
    if (mat && blank(ipCur.material_status)) plan.ip.material_status = mat;

    // 工程
    const sp = planStepsFor(rec.status);
    plan.hold = sp.hold;
    if (sp.hold) {
      // 現在の保留状態を plan (= snapshot) に載せる: プレビュー後に人が解除していたら snapshot 不一致で止まる
      // (Codex R2 high: 無条件に on_hold へ戻すと人の解除を上書きする)
      plan.holdFrom = ipCur.workflow_state || 'active';
      if (plan.holdFrom === 'on_hold') result.warnings.push('既に画像制作が保留中です (そのまま)');
    } else if (sp.steps.length > 0) {
      const untouched = existing ? imageTrackUntouchedReason(db, existing.id, { ensure: false }) : null;
      if (untouched) {
        result.warnings.push(`工程は書きません (${untouched})`);
      } else {
        plan.steps = sp.steps;
        if (sp.assigneeName) {
          const f = findStaffByName(db, sp.assigneeName);
          if (f.staff) plan.assignee = { staffId: f.staff.id, name: f.staff.name, code: sp.assigneeStepCode };
          else result.warnings.push(`担当者「${sp.assigneeName}」を担当者マスタで特定できず未割当 (${f.reason === 'ambiguous' ? '複数該当' : '見つからない'})`);
        }
      }
    }
    if (plan.create) result.warnings.push('商品マスターに無いため最小情報で作成 (売価・JAN・税率は未入力)');

    plan.ledgerOnly = !plan.create && !plan.detach && Object.keys(plan.fill).length === 0
      && Object.keys(plan.ip).length === 0 && plan.steps.length === 0 && !(plan.hold && plan.holdFrom !== 'on_hold');
    result.outcome = plan.create ? 'would_create' : (plan.ledgerOnly ? 'would_ledger_only' : 'would_update');
    result.plan_summary = summarizePlan(plan);
    planned.push({ rec, plan, result });
  }

  // プレビューと実行を同じ対象・同じ内容に固定する (#922 Codex R1/R2 high と同じ考え方)
  const snapshot = crypto.createHash('sha256')
    .update(planned.map(({ rec, plan }) => JSON.stringify({ rec, plan })).sort().join('|'))
    .digest('hex').slice(0, 32);

  if (!dryRun) {
    if (expectedSnapshot !== snapshot) {
      const e = new Error('プレビュー後に Notion 側またはアプリ側の対象が変わりました。もう一度「対象を確認」からやり直してください');
      e.code = 'snapshot_mismatch';
      throw e;
    }
    const run = runId || `img-${Date.now().toString(36)}-${crypto.randomBytes(3).toString('hex')}`;
    let processed = 0;
    for (const t of planned) {
      if (processed >= maxPerRun) { t.result.outcome = 'deferred'; continue; }
      processed += 1;
      try {
        const r = applyPlan(db, t.rec, t.plan, { actor, runId: run });
        t.result.draftId = r.draftId;
        t.result.outcome = t.plan.create ? 'created' : (t.plan.ledgerOnly ? 'ledger_only' : 'updated');
        t.result.warnings.push(...r.warnings);
      } catch (e) {
        t.result.outcome = 'failed';
        t.result.error = truncate(e);
      }
    }
  }
  return {
    statuses: targets, missingStatuses, total: pages.length, snapshot, results, summary: summarize(results),
  };
}

function summarizePlan(plan) {
  const parts = [];
  if (plan.create) parts.push('新規作成');
  if (plan.detach) parts.push('バリエーションから外す');
  const fills = Object.keys(plan.fill).filter((k) => k !== 'image_priority');
  if (fills.length) parts.push(`商品: ${fills.join('/')}`);
  if (plan.fill.image_priority) parts.push('自社商品');
  const ips = Object.keys(plan.ip);
  if (ips.length) parts.push(`画像制作: ${ips.join('/')}`);
  if (plan.hold) parts.push('画像制作を保留');
  if (plan.steps.length) parts.push(`工程 ${plan.steps.length} 件${plan.assignee ? ` (担当 ${plan.assignee.name})` : ''}`);
  return parts.join(' / ') || '変更なし (台帳に記録のみ)';
}

/**
 * 1 カード分の書き込み。**1 トランザクション** (ドラフト作成/補完・画像情報・detach・工程・台帳・イベント)。
 * 途中で失敗したら全部戻す (台帳だけ残る / 工程だけ進む、を作らない)
 */
function applyPlan(db, rec, plan, { actor, runId }) {
  const warnings = [];
  const run = db.transaction(() => {
    let draftId = plan.draftId;
    if (plan.create) {
      // 実行時点での再確認 (プレビュー後に同じコードが作られていたら二重にしない)
      const raced = db.prepare('SELECT id FROM product_drafts WHERE LOWER(TRIM(ne_code)) = ?').get(norm(rec.ne_code));
      if (raced) throw new Error(`プレビュー後に同じ商品コードのドラフト (#${raced.id}) が作られました。再プレビューしてください`);
      const info = db.prepare(`
        INSERT INTO product_drafts (
          ne_code, name, status, own_brand, image_priority, drive_folder_url, amazon_url, asin,
          source, created_by, imported_at
        ) VALUES (?, ?, 'draft', 1, ?, ?, ?, ?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
      `).run(
        rec.ne_code, rec.name, OWN_BRAND_IMAGE_PRIORITY,
        plan.fill.drive_folder_url || null, plan.fill.amazon_url || null, plan.fill.asin || null,
        SOURCE_NOTION_IMPORT, actor || null,
      );
      draftId = Number(info.lastInsertRowid);
      logEvent(db, draftId, 'image_db_imported',
        `Notion 画像DB「${rec.status || ''}」から最小情報で作成 (missing: price, jan_code, tax_rate) page=${rec.notion_page_id}`, actor);
    } else {
      // プレビュー時に「空」と判定した項目が実行時点で埋まっていたら、そのカードは書かずに失敗させる
      // (Codex R3 medium: 黙って据え置くと台帳だけ「移植済み」になり、以後補完されない)
      const nowRow = db.prepare('SELECT drive_folder_url, amazon_url, asin, image_priority, own_brand FROM product_drafts WHERE id = ?').get(draftId);
      if (!nowRow) throw new Error('ドラフトが見つかりません (プレビュー後に削除された可能性)');
      const changedNow = Object.keys(plan.fill).filter((k) => !blank(nowRow[k]));
      if (changedNow.length > 0) throw new Error(`プレビュー後に ${changedNow.join('/')} が入力されました。再プレビューしてください`);
      if (plan.fill.image_priority && nowRow.own_brand !== 0 && nowRow.own_brand !== 1) throw new Error('own_brand が不正です');
      const sets = [];
      const params = { id: draftId };
      for (const [k, v] of Object.entries(plan.fill)) {
        if (k === 'image_priority') continue;
        sets.push(`${k} = @${k}`);
        params[k] = v;
      }
      if (plan.fill.image_priority) {
        sets.push('own_brand = 1', 'image_priority = @image_priority');
        params.image_priority = plan.fill.image_priority;
      }
      if (sets.length > 0) {
        db.prepare(`UPDATE product_drafts SET ${sets.join(', ')}, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = @id`).run(params);
      }
      // 補完するものが無い (台帳に記録だけ) ときはイベントを残さない — 「補完済み」と誤読させない (Codex R2 low)
      if (!plan.ledgerOnly) {
        logEvent(db, draftId, 'image_db_imported',
          `Notion 画像DB「${rec.status || ''}」から画像情報を補完 (${Object.keys(plan.fill).concat(Object.keys(plan.ip)).join(', ') || '工程/保留のみ'}) page=${rec.notion_page_id}`, actor);
      }
    }

    // 画像制作情報 (プレビュー時に空だった項目だけ。実行時点で埋まっていたらカード失敗)
    if (Object.keys(plan.ip).length > 0) {
      const ipCur = db.prepare('SELECT * FROM draft_image_production WHERE draft_id = ?').get(draftId) || {};
      const filledNow = Object.keys(plan.ip).filter((k) => !blank(ipCur[k]));
      if (filledNow.length > 0) throw new Error(`プレビュー後に画像制作情報 (${filledNow.join('/')}) が入力されました。再プレビューしてください`);
      upsertImageProduction(db, draftId, plan.ip);
    }

    // 子SKU → 独立ページ (detach)。代表ドラフトが無いときは自分自身を記録元にする
    if (plan.detach && !isDetached(db, rec.ne_code)) {
      const fromId = plan.detach.repDraftId || draftId;
      db.prepare('DELETE FROM draft_sku_prices WHERE draft_id = ? AND sku_code = ?').run(fromId, norm(rec.ne_code));
      db.prepare('DELETE FROM draft_sku_jans WHERE draft_id = ? AND sku_code = ?').run(fromId, norm(rec.ne_code));
      db.prepare('INSERT INTO draft_variation_exclusions (draft_id, ne_code, actor) VALUES (?, ?, ?)')
        .run(fromId, rec.ne_code, actor || null);
      logEvent(db, fromId, 'variation_sku_excluded', `${rec.ne_code} (Notion 画像DB移植: 色ごとに別ページ)`, actor);
    }

    // 画像制作だけの保留。プレビュー時点の状態と違っていたら (人が解除/保留した) 書かずに失敗させる
    if (plan.hold) {
      const nowState = db.prepare('SELECT workflow_state FROM draft_image_production WHERE draft_id = ?').get(draftId)?.workflow_state || 'active';
      if (nowState !== plan.holdFrom) throw new Error('プレビュー後に画像制作の保留状態が変わりました。再プレビューしてください');
      if (nowState !== 'on_hold') setImageWorkflowState(db, draftId, 'on_hold', { note: 'Notion 画像DB: 保留', actor });
    }

    // 工程 (まっさらなときだけ — 実行時点で再確認。動いていたらカード失敗 = 台帳に載せない)
    if (plan.steps.length > 0) {
      const untouched = imageTrackUntouchedReason(db, draftId);
      if (untouched) {
        throw new Error(`プレビュー後に画像工程が動きました (${untouched})。再プレビューしてください`);
      } else {
        for (const s of plan.steps) {
          const patch = { state: s.state };
          if (plan.assignee && plan.assignee.code === s.code) patch.assignee_id = plan.assignee.staffId;
          // Notion で既に済んでいる段階を写すだけなので ①③ の材料チェックは通さない (bypassGates)。⑥順序も plan の順で満たす
          setStepState(draftId, s.code, patch, actor, { isAdmin: true, bypassGates: true });
        }
      }
    }

    db.prepare(`
      INSERT INTO draft_image_notion_imports (notion_page_id, draft_id, source_ne_code, source_status, drive_folder_url, source_hash, import_run_id)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `).run(rec.notion_page_id, draftId, rec.ne_code, rec.status, rec.drive_folder_url, rec.source_hash, runId);
    return { draftId };
  });
  const r = run();
  return { ...r, warnings };
}

function truncate(e) {
  const m = String((e && e.message) || e || 'unknown');
  return m.length > 200 ? `${m.slice(0, 200)}…` : m;
}

export const IMAGE_OUTCOMES = [
  'would_create', 'would_update', 'would_ledger_only', 'created', 'updated', 'ledger_only',
  'already_migrated', 'source_changed_after_migration',
  'needs_master_import', 'brand_priority_conflict', 'variation_conflict', 'duplicate', 'deferred', 'failed',
];

export function summarize(results) {
  const s = Object.fromEntries(IMAGE_OUTCOMES.map((k) => [k, 0]));
  for (const r of results) s[r.outcome] = (s[r.outcome] || 0) + 1;
  return s;
}
