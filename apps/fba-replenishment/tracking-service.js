/**
 * FBA納品への追跡番号投入 — SP-API Fulfillment Inbound v2024-03-20
 *
 * 2026-08-07 に本番アカウントで実測して確定した事実:
 *   - **shipment一覧の operation は存在しない**。shipments は `GET /inboundPlans/{id}` の
 *     レスポンス本体に入っている (`/inboundPlans/{id}/shipments` は Access denied になるが
 *     これは権限不足ではなく「そんなパスは無い」の意味)
 *   - boxId は `<納品番号>U<6桁連番>` だが **推測生成せず必ず listShipmentBoxes の返り値を使う**
 *   - trackingId は **ハイフン無しの数字**で送る
 *   - 送信ボディに `ltlTrackingDetail` は入れない (SPDのみ)
 *   - PUT は非同期。operationId を `getInboundOperationStatus` で SUCCESS まで追う
 *
 * 🏆**PUT の可否を決めるのは「記入欄 (spdTrackingItems の枠) の有無」** (2026-08-31 確定)。
 *   本番24件の突合で、成功9件はすべて欄あり・失敗15件はすべて欄なし。例外なし。
 *   欄は Seller Central で出荷確定の操作をするとAmazon側に作られる → trackingSlotCount 参照。
 *
 *   ⚠️ 2026-08-07〜08-21 のあいだ「status が効く」「期限が効く」「未発送だから」と
 *   立て続けに誤診した。いずれも**欄の有無と相関する別の何か**を見ていただけだった:
 *     - 期限切れの納品は欄も無いことが多い (出荷確定していないから期限も過ぎる)
 *     - 実際に出していない納品は当然出荷確定もしていない = 欄が無い
 *   期限 (checkDeadline) は無駄なPUTを減らす事前判定として残してあるが、
 *   **拒否の理由ではない**。
 */
import SellingPartner from 'amazon-sp-api';

const V = '/inbound/fba/2024-03-20';
const MARKETPLACE = () => process.env.SP_API_MARKETPLACE_ID || 'A1VC38T7YXB528';

/**
 * 呼び出し間隔。読み取りと書き込みで分ける。
 * SDKの定義では全operationが restore_rate 0.5/秒 だが、実測では読み取りを
 * 25回連続で投げても 10.7秒 (=0.43秒/回) で完走し、429は出なかった
 * (2026-08-09 実測。バースト枠が広い)。
 * 読み取りを2秒間隔にすると一覧だけで50秒かかり、22時の実行が長くなりすぎるので、
 * **読み取りは0.5秒・書き込みは2秒**にする。読み取りは冪等で、429が出てもSDKが再試行する。
 */
const READ_INTERVAL_MS = 500;
const WRITE_INTERVAL_MS = 2000;

/**
 * 何日以内に更新されたプランを見るか。
 * ⭐ACTIVEのまま放置されたプランが478件あり (2026-08-09 実測)、全部の詳細を取ると
 *   1件2秒 × 478 = 約16分かかって定期実行に収まらない。
 *   輸送箱ラベルを発行すればプランは必ず更新されるので、
 *   **直近に更新されていないプランに「今日投入すべき納品」は無い**。
 *   黙って打ち切るのではなく、対象外にした件数を呼び出し側へ返して通知に出す。
 */
const PLAN_WINDOW_DAYS = () => Number(process.env.FBA_TRACKING_PLAN_WINDOW_DAYS || 14);
let lastCallAt = 0;

let spClient = null;
function getClient() {
  if (!spClient) {
    spClient = new SellingPartner({
      region: 'fe',
      refresh_token: process.env.SP_API_REFRESH_TOKEN,
      credentials: {
        SELLING_PARTNER_APP_CLIENT_ID: process.env.SP_API_CLIENT_ID,
        SELLING_PARTNER_APP_CLIENT_SECRET: process.env.SP_API_CLIENT_SECRET,
        AWS_ACCESS_KEY_ID: process.env.AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY: process.env.AWS_SECRET_ACCESS_KEY,
      },
    });
  }
  return spClient;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function call(api_path, method = 'GET', body) {
  const min = method === 'GET' ? READ_INTERVAL_MS : WRITE_INTERVAL_MS;
  const wait = min - (Date.now() - lastCallAt);
  if (wait > 0) await sleep(wait);
  lastCallAt = Date.now();
  return getClient().callAPI(body ? { api_path, method, body } : { api_path, method });
}

/**
 * pageToken を最後まで辿る (#7)。
 * 先頭ページだけ見ていると、プランが20件を超えた日や箱が100個を超えた納品を黙って取りこぼす。
 */
async function callAllPages(basePath, key, { pageSize = 20, max = 50 } = {}) {
  const out = [];
  let token = null;
  for (let i = 0; i < max; i++) {
    const sep = basePath.includes('?') ? '&' : '?';
    const url = `${basePath}${sep}pageSize=${pageSize}` + (token ? `&paginationToken=${encodeURIComponent(token)}` : '');
    const res = await call(url);
    if (Array.isArray(res?.[key])) out.push(...res[key]);
    token = res?.pagination?.nextToken ?? res?.pagination?.token ?? null;
    if (!token) return { items: out, truncated: false };
  }
  // max ページ見ても終わらない = 想定外。黙って打ち切らずに呼び出し側へ伝える
  return { items: out, truncated: true };
}

/**
 * Amazonが「明確に拒否した」と断定できるエラーコード。
 * ⭐これ以外 (タイムアウト・接続断・5xx・応答解析失敗) は
 *   **受理されたのに応答だけ失った可能性がある**ので、確定失敗にしてはいけない。
 *   確定失敗にすると次回の実行で再送され、二重投入になる。
 */
const DEFINITE_REJECT_CODES = new Set([
  'BadRequest', 'InvalidInput', 'Unauthorized', 'Forbidden', 'NotFound', 'AccessDenied', 'QuotaExceeded',
]);

/**
 * エラーの手がかりを、後から原因を追えるだけ残す。
 * 🚨message だけを記録していたため「同じ文言の別原因」を区別できず、真因の特定に3週間かかった
 *   (2026-08-31)。SDKがどの形で投げてくるかは operation により違うので、拾える口を全部見る。
 */
export function describePutError(e) {
  const pick = (...vals) => vals.find((v) => v !== undefined && v !== null && v !== '') ?? null;
  const headers = e?.headers ?? e?.response?.headers ?? {};
  const h = (name) => headers?.[name] ?? headers?.[name.toLowerCase()] ?? null;
  const detail = {
    code: pick(e?.code, e?.error?.code, e?.body?.errors?.[0]?.code),
    httpStatus: pick(e?.statusCode, e?.status, e?.response?.status, e?.response?.statusCode),
    requestId: pick(h('x-amzn-RequestId'), h('x-amzn-requestid'), e?.requestId),
    details: pick(e?.details, e?.error?.details, e?.body?.errors?.[0]?.details),
    errors: Array.isArray(e?.body?.errors) ? e.body.errors : (Array.isArray(e?.errors) ? e.errors : null),
    message: String(e?.message ?? e ?? ''),
  };
  for (const k of Object.keys(detail)) if (detail[k] === null) delete detail[k];
  return detail;
}

function classifyPutError(e) {
  const code = e?.code ?? '';
  const msg = String(e?.message ?? e ?? '');
  // レート超過は「明確に拒否された」が、内容は正しいので時間をおけば通る
  if (code === 'QuotaExceeded') return { indeterminate: false, retryable: true };
  if (DEFINITE_REJECT_CODES.has(code)) return { indeterminate: false, retryable: false };
  // メッセージからも拾う (SDKがcodeを付けずに投げることがある)
  if (/is not in a state where tracking details can be provided|validation error/i.test(msg)) {
    return { indeterminate: false, retryable: false };
  }
  return { indeterminate: true, retryable: false };
}

export function missingEnv() {
  return ['SP_API_CLIENT_ID', 'SP_API_CLIENT_SECRET', 'SP_API_REFRESH_TOKEN', 'AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY']
    .filter((k) => !process.env[k]);
}

/**
 * 期限を過ぎていないか。過ぎていたらAPIは受け付けない (画面からは入るのでリカバリは人手)。
 *
 * ⭐**すべての期限を過ぎたときだけ「期限切れ」**とする (#5)。
 *   readyToShipWindow.end (当日23:59 JST) と editableUntil (翌日09:00 JST) のどちらが
 *   効くのかは未実測。片方でも残っていれば送ってみる。
 *   ここは無駄なPUTを減らすための事前判定にすぎず、可否の最終判断はAmazon側にある。
 *   拒否されても BadRequest が返るだけで実害は無いので、**緩い側に倒すほうが安全**
 *   (厳しくすると、まだ入るはずの納品を人手に回してしまう)。
 */
export function checkDeadline(shipment, now = new Date()) {
  const rts = shipment?.dates?.readyToShipWindow?.end;
  const edit = shipment?.selectedDeliveryWindow?.editableUntil;
  const limits = [rts, edit].filter(Boolean).map((s) => ({ raw: s, t: Date.parse(s) })).filter((x) => !Number.isNaN(x.t));
  if (!limits.length) return { ok: true, limits: [], expired: [], note: '期限の情報が取れませんでした' };
  const expired = limits.filter((x) => x.t <= now.getTime());
  return {
    ok: expired.length < limits.length,
    limits: limits.map((x) => x.raw),
    expired: expired.map((x) => x.raw),
    note: expired.length === limits.length
      ? 'この納品は編集期限をすべて過ぎています。APIでは登録できません (Seller Central画面からは入力できます)'
      : null,
  };
}

/**
 * 追跡番号が**実際に入っている** spdTrackingItems だけを返す。
 * 🚨(2026-08-12 22:00 本番で実測) READY_TO_SHIP の納品でも、spdTrackingItems が
 *   「箱数ぶんの trackingId が空のエントリ」で返ってくることがある (8/12は15箱+3箱の2納品とも)。
 *   エントリの有無 (length) で「登録済み」と判定すると、未登録の納品を照合対象から外してしまい、
 *   CSV側の行が宙に浮いて「対応する納品が見つかりません」で全件中断する。
 *   必ず trackingId の中身で判定すること。
 */
export function realTrackingItems(detail) {
  const items = detail?.trackingDetails?.spdTrackingDetail?.spdTrackingItems ?? [];
  return items.filter((i) => String(i?.trackingId ?? '').trim() !== '');
}

/**
 * 追跡番号の**記入欄** (spdTrackingItems のエントリ) が何個あるか。
 *
 * 🏆(2026-08-31 実測で確定) `PUT trackingDetails` が通るかどうかは status でも期限でもなく、
 *   **この記入欄が存在するか**だけで決まる。欄はAmazon側が用意するもので、
 *   Seller Central で出荷確定の操作をした納品にだけ箱数ぶん作られる。
 *
 *   欄あり → 投入できる / 欄なし (0個) → 何度投げても
 *   `not in a state where tracking details can be provided` で拒否される。
 *
 *   本番24件の突合で例外なし (成功9件はすべて欄あり・失敗15件はすべて欄なし)。
 *   欄の有無は人がいつ画面を操作したかで決まるため**納品プラン単位で揃う**。
 *   これが「日によって全滅したり全成功したりする」ように見えていた正体。
 */
export function trackingSlotCount(detail) {
  return (detail?.trackingDetails?.spdTrackingDetail?.spdTrackingItems ?? []).length;
}

/**
 * 記入欄が「その納品の箱と1対1で揃っているか」。
 * 🚨欄が1つでもあれば投入してよい、にはしない (Codexレビュー 2026-08-31)。
 *   PUTには /boxes 由来の boxId を全件送るので、欄の側と食い違っていれば
 *   一部の箱が宙に浮くか、知らない箱へ書きにいくことになる。書き込みジョブなので
 *   「揃っていることを確認できたときだけ送る」に倒す。
 * @returns {{ok: boolean, reason: string|null}}
 */
export function checkSlotsMatchBoxes(detail, boxIds) {
  const slotIds = (detail?.trackingDetails?.spdTrackingDetail?.spdTrackingItems ?? []).map((i) => String(i?.boxId ?? ''));
  if (!slotIds.length) {
    return { ok: false, reason: 'Amazon側に追跡番号の記入欄がありません (Seller Central で出荷確定の操作が必要)' };
  }
  const a = [...slotIds].sort().join(' ');
  const b = [...boxIds].map(String).sort().join(' ');
  if (a === b) return { ok: true, reason: null };
  return {
    ok: false,
    reason: `Amazon側の記入欄 ${slotIds.length}個 と輸送箱 ${boxIds.length}個 が一致しません。` +
      '取り違えを避けるため自動では投入しません (画面から入力してください)',
  };
}

/**
 * getShipment の生レスポンスから runner に渡す要約を組み立てる。
 * 🚨期限の生データ (dates / selectedDeliveryWindow) を必ず残すこと。
 *   runner は投入直前に checkDeadline(ship) を呼び直すが、ここで落とすと
 *   「期限の情報が取れませんでした→ok」で素通りし、期限切れをAmazonのエラーで
 *   知ることになる (2026-08-11 実測。8/10分をJST 10時に投入したら3件とも
 *   ガードを抜けて "not in a state..." で拒否された)
 */
export function summarizeShipment(plan, shipmentRef, detail, boxes, now = new Date()) {
  const boxIds = boxes.map((x) => x.boxId);
  // ⭐APIが追跡番号を受け付ける状態か (記入欄の有無と、箱との対応)。詳細は trackingSlotCount のコメント
  const slot = checkSlotsMatchBoxes(detail, boxIds);
  return {
    inboundPlanId: plan.inboundPlanId,
    planName: plan.name || '',
    shipmentId: shipmentRef.shipmentId,
    shipmentConfirmationId: detail.shipmentConfirmationId,
    fcCode: detail.destination?.warehouseId ?? '',
    status: detail.status,
    // ⭐boxIdは規則から生成せずAPIの返り値を正とする
    boxIds,
    hasTracking: realTrackingItems(detail).length > 0,
    slots: trackingSlotCount(detail),
    putReady: slot.ok,
    notReadyReason: slot.reason,
    deadline: checkDeadline(detail, now),
    dates: detail.dates ?? null,
    selectedDeliveryWindow: detail.selectedDeliveryWindow ?? null,
  };
}

/**
 * 追跡番号がまだ入っていない納品を探す。
 * ⭐無駄なAPI呼び出しを避けるため、プラン本体の shipments[].status を見てから詳細を取る。
 *
 * 🏆**SHIPPED も必ず対象に入れる** (2026-08-31 に設計を反転):
 *   記入欄 (trackingSlotCount) が立つのは Seller Central で出荷確定の操作をした時で、
 *   その操作で status は SHIPPED になる。READY_TO_SHIP だけを見ていた旧実装は
 *   **「投入できる納品」を除外し、「何度投げても入らない納品」だけを拾っていた**。
 *   実際 8/21・8/28・8/31 の135箱は欄が立っていたのに一度も試されず、
 *   逆に 8/24・8/27 の56箱は欄が無いのに投入を試みて全部 BadRequest になっていた。
 *
 *   旧コメントの「SHIPPED を足すと、人が画面で入力した直後を未登録と誤認して二重投入する」
 *   という懸念自体は残る (値の反映は数時間遅れる)。ただしそれは自前記録 (tracking-store) と
 *   投入直前の再確認で守る側であって、**status で入口を閉じる理由にはならない**。
 *
 * @param {{statuses?: string[]}} [opts]
 * @returns {Promise<{shipments: Array, notReady: Array, errors: string[], scanned: object}>}
 *   shipments … 記入欄があり投入できる納品 (hasTracking 済みのものも含む。呼び出し側で skip)
 *   notReady  … 記入欄が無く、APIでは投入できない納品 (人が画面で出荷確定していない)
 */
export async function findOpenShipments(opts = {}) {
  const wantStatuses = opts.statuses ?? ['READY_TO_SHIP', 'SHIPPED'];
  const windowDays = opts.windowDays ?? PLAN_WINDOW_DAYS();
  const now = opts.now ?? new Date();
  const out = [];
  const notReady = [];
  const arrived = [];
  const errors = [];

  let plans;
  try {
    // ⭐一覧は全ページ取る (並び順を仮定して早期に打ち切ると取りこぼす)。
    //   絞るのは「詳細を取るかどうか」の側で行う
    plans = await callAllPages(`${V}/inboundPlans?status=ACTIVE`, 'inboundPlans', { pageSize: 20, max: 100 });
  } catch (e) {
    // プラン一覧そのものが取れない = 何も判断できない。呼び出し側で中断させる
    throw new Error(`納品プラン一覧を取得できません: ${e?.message ?? e}`);
  }
  if (plans.truncated) errors.push('納品プランが多すぎて全部見られませんでした (ページ上限)');

  const cutoff = now.getTime() - windowDays * 24 * 3600 * 1000;
  const recent = plans.items.filter((p) => {
    const t = Date.parse(p.lastUpdatedAt ?? p.createdAt ?? '');
    return Number.isNaN(t) ? true : t >= cutoff; // 日付が読めないものは念のため見る
  });
  const skipped = plans.items.length - recent.length;

  for (const p of recent) {
    // 🚨ここで例外を握り潰すと「取得に失敗した」が「今日は納品が無い」に化ける (#6)。
    //   件数を集めて呼び出し側へ返し、1件でも失敗していたら人に知らせる
    let full;
    try {
      full = await call(`${V}/inboundPlans/${p.inboundPlanId}`);
    } catch (e) {
      errors.push(`プラン ${p.inboundPlanId} の取得に失敗: ${e?.message ?? e}`);
      continue;
    }
    for (const s of full.shipments ?? []) {
      const base = `${V}/inboundPlans/${p.inboundPlanId}/shipments/${s.shipmentId}`;
      try {
        // 🚨status で先に切り捨てない (Codexレビュー 2026-08-31)。切り捨てると
        //   「到着済みなのに追跡番号が入っていない納品」が誰にも見えないまま消える。
        //   直近の被害 (8/19〜8/28 の約240箱) がまさにこれで、気づいたのは3週間後だった
        const d = await call(base);
        const b = await callAllPages(`${base}/boxes`, 'boxes', { pageSize: 100 });
        if (b.truncated) errors.push(`${d.shipmentConfirmationId}: 輸送箱が多すぎて全部見られませんでした`);
        const sum = summarizeShipment(p, s, d, b.items, now);

        // 登録済みは status を問わず返す。ここで落とすとCSV行が宙に浮いて全件中断する
        if (sum.hasTracking) { out.push(sum); continue; }
        // もう書き込める段階ではない (RECEIVING / CLOSED)。手遅れだが黙って消さない
        if (!wantStatuses.includes(d.status)) { arrived.push(sum); continue; }
        // 🚨記入欄が無い納品を投入対象に混ぜてはいけない。投げれば必ず BadRequest になり、
        //   さらに「対応する納品が見つからない/送り状がCSVに無い」で**その日の全件が中断**する。
        //   人が画面で出荷確定するまでAPIでは手が出ないので、対象から外して名指しで知らせる
        if (sum.putReady) out.push(sum);
        else notReady.push(sum);
      } catch (e) {
        errors.push(`納品 ${s.shipmentId} の取得に失敗: ${e?.message ?? e}`);
      }
    }
  }
  return {
    shipments: out,
    notReady,
    arrived,
    errors,
    scanned: { total: plans.items.length, checked: recent.length, skippedOld: skipped, windowDays },
  };
}

/**
 * PUTの直前に、まだ投入してよい状態かをもう一度確かめる。
 * CSVの解析中に人が画面から入力していることがある (即 SHIPPED になる)。
 * @returns {Promise<{ok: boolean, reason?: string, status?: string}>}
 */
export async function recheckBeforePut(target, { expectStatuses = ['READY_TO_SHIP', 'SHIPPED'] } = {}) {
  const base = `${V}/inboundPlans/${target.inboundPlanId}/shipments/${target.shipmentId}`;
  let d;
  try {
    d = await call(base);
  } catch (e) {
    return { ok: false, reason: `投入直前の確認に失敗しました: ${e?.message ?? e}` };
  }
  if (realTrackingItems(d).length) {
    return { ok: false, status: d.status, reason: 'すでに追跡番号が入っています (この間に誰かが入力した可能性)' };
  }
  // 記入欄が消えた = もうAPIでは受け付けられない。投げれば BadRequest になるだけなので送らない
  if (trackingSlotCount(d) === 0) {
    return {
      ok: false, status: d.status, slots: 0,
      reason: 'Amazon側に追跡番号の記入欄がありません (Seller Central で出荷確定の操作が済んでいない状態)。' +
        'APIでは登録できないため、画面から入力してください',
    };
  }
  if (!expectStatuses.includes(d.status)) {
    return {
      ok: false, status: d.status,
      reason: `状態が ${d.status} に変わりました。この間に画面から入力された可能性があります` +
        ' (入力直後は追跡番号がAPIに現れないため、自動では投入しません)',
    };
  }
  return { ok: true, status: d.status, slots: trackingSlotCount(d) };
}

/**
 * 追跡番号を投入する。
 * @param {{inboundPlanId: string, shipmentId: string}} target
 * @param {Array<{boxId: string, trackingId: string}>} items 箱の**全件**を渡すこと (部分更新のmergeは前提にしない)
 * @returns {Promise<{ok: boolean, operationId?: string, error?: string, verified?: object}>}
 */
export async function putTrackingDetails(target, items) {
  if (!items?.length) return { ok: false, error: '投入する項目がありません' };
  const bad = items.filter((i) => !i.boxId || !/^[0-9]{8,20}$/.test(String(i.trackingId ?? '')));
  if (bad.length) return { ok: false, error: `送信できない項目があります: ${JSON.stringify(bad.slice(0, 3))}` };

  const base = `${V}/inboundPlans/${target.inboundPlanId}/shipments/${target.shipmentId}`;
  // SPD のみ。過去レスポンスに出てくる空の ltlTrackingDetail はコピーしない
  const body = { trackingDetails: { spdTrackingDetail: { spdTrackingItems: items.map((i) => ({ boxId: i.boxId, trackingId: i.trackingId })) } } };

  let res;
  try {
    res = await call(`${base}/trackingDetails`, 'PUT', body);
  } catch (e) {
    const msg = e?.message ?? String(e);
    const c = classifyPutError(e);
    return {
      ok: false,
      error: c.indeterminate
        ? `投入が受理されたか判断できません (${msg})。Seller Central画面で反映を確認してください`
        : msg,
      // ⭐タイムアウトや接続断は「受理後に応答だけ失った」可能性がある。
      //   確定失敗にすると次回の実行で再送され二重投入になるため indeterminate にする
      indeterminate: c.indeterminate,
      retryable: c.retryable,
      errorDetail: describePutError(e),
    };
  }

  // ⭐202相当。operationId が返ったら SUCCESS まで追う (返り値だけで成功と判断しない)
  const operationId = res?.operationId;
  if (!operationId) {
    // 非同期APIなので operationId が返るのが正。返らないのは想定外で、
    // 受理されたかも分からない。成功に倒さない (#6)
    return {
      ok: false, indeterminate: true, retryable: false,
      error: `投入の応答に operationId がありません (${JSON.stringify(res ?? null).slice(0, 200)})。Seller Central画面で反映を確認してください`,
    };
  }
  {
    for (let i = 0; i < 20; i++) {
      await sleep(3000);
      let op;
      try {
        op = await call(`${V}/operations/${operationId}`);
      } catch (e) {
        // 🚨PUTは受け付けられているので、ここで throw すると「Amazonには入ったが記録が無い」
        //   状態を作る (#11)。結果不明として返し、呼び出し側で人に確認させる
        return {
          ok: false, operationId, indeterminate: true, retryable: false,
          error: `投入は受け付けられましたが結果を確認できませんでした (${e?.message ?? e})。Seller Central画面で反映を確認してください`,
        };
      }
      if (op.operationStatus === 'SUCCESS') break;
      if (op.operationStatus === 'FAILED') {
        return { ok: false, operationId, error: `operation FAILED: ${JSON.stringify(op.operationProblems ?? {})}`, retryable: false };
      }
      if (i === 19) {
        return {
          ok: false, operationId, indeterminate: true, retryable: false,
          error: '投入は受け付けられましたが時間内に完了しませんでした。Seller Central画面で反映を確認してください',
        };
      }
    }
  }
  return { ok: true, operationId: operationId ?? null };
}

/**
 * 投入後の確認。
 * ⚠️ trackingDetails の反映には数時間かかるため、**null でも失敗とは限らない**。
 * 成否の判定には使わず、あくまで「見えたら一致しているか」を見る。
 */
export async function verifyTracking(target, items) {
  const base = `${V}/inboundPlans/${target.inboundPlanId}/shipments/${target.shipmentId}`;
  const d = await call(base);
  const got = realTrackingItems(d);
  if (!got.length) return { visible: false, matched: null, status: d.status, note: 'APIにはまだ反映されていません (反映に数時間かかることを確認済み)' };
  const a = got.map((x) => `${x.boxId}=${x.trackingId}`).sort().join(',');
  const b = items.map((x) => `${x.boxId}=${x.trackingId}`).sort().join(',');
  return { visible: true, matched: a === b, status: d.status, got };
}

/** Seller Central のURL (…?wf=wfxxxx…) からもプランIDを受け取れるようにする */
export function extractPlanId(input) {
  const s = String(input ?? '').trim();
  if (!s) return null;
  const m = s.match(/[?&]wf=([^&\s]+)/i);
  const id = m ? decodeURIComponent(m[1]) : s;
  return /^wf[0-9a-f-]{8,}$/i.test(id) ? id : null;
}

/**
 * 1つの納品プランの shipment 一覧を返す (伝票発行CSVを作るため)。
 * findOpenShipments と違い **箱があるものは status を問わず全部返す**。
 * 伝票は「ラベルを作った後・出荷前」に発行するので、READY_TO_SHIP に限ると早すぎて拾えない。
 * @returns {Promise<{planName: string, shipments: Array}>}
 */
export async function getPlanShipments(inboundPlanId) {
  const full = await call(`${V}/inboundPlans/${inboundPlanId}`);
  const out = [];
  for (const s of full.shipments ?? []) {
    const base = `${V}/inboundPlans/${inboundPlanId}/shipments/${s.shipmentId}`;
    const d = await call(base);
    const b = await callAllPages(`${base}/boxes`, 'boxes', { pageSize: 100 });
    if (b.truncated) throw new Error(`${d.shipmentConfirmationId}: 輸送箱が多すぎて全部取得できませんでした`);
    const boxes = b.items;
    out.push({
      shipmentId: s.shipmentId,
      shipmentConfirmationId: d.shipmentConfirmationId,
      fcCode: d.destination?.warehouseId ?? '',
      address: d.destination?.address ?? null,
      status: d.status,
      boxCount: boxes.length,
      hasTracking: realTrackingItems(d).length > 0,
    });
  }
  return { planName: full.name || '', planStatus: full.status, shipments: out };
}

export const _internal = { MARKETPLACE, classifyPutError };
