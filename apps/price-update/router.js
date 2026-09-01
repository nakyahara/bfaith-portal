/**
 * 価格一括改定ツール (price-update) — M1 読み取り専用版
 *
 * 要件定義 v1.0 の M1: 引き当て (信頼度つき) + セット展開 + ライブ設定価格 (楽天/Yahoo)
 * + Amazon スナップショット表示 + 利益プレビュー + 手動更新チェックリスト。
 *
 * 🚨このバージョンはモールへ一切書き込まない。値付けの入力・ガード評価・記録までを行い、
 *   実際の更新 (楽天=M2 / Yahoo=M3) は別 PR で入れる。
 *   ガード評価はここ (サーバ側) で行う — 画面のチェックだけだと API 直叩きで抜けられるため。
 *
 * CSRF: ポータルにトークン機構は無いので inquiry-hub と同じ二段ガード
 *   (非GETは Origin 一致必須 403 + application/json 以外 415) を /api/ に一括適用する。
 */
import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import { loadDimMall } from '../../lib/dim-mall.js';
import { getDB, insertRun, appendEvent, getRun, listRuns, newId, runClaim, recoveryRunsOf, createRecoveryRun } from './db.js';
import { planRecovery, buildRecoveryOperations, RECOVERABLE_STATES } from './recovery.js';
import { buildTargets, listingUrl, normCode, UPDATABLE_MALLS } from './resolve.js';
import { fetchRakutenPrices, fetchYahooPrices, loadAmazonSnapshot } from './live-price.js';
import { evaluateRow, runLimits } from './pricing.js';
import { rakutenShippingLabel, yahooPostageLabel, rakutenShippingName, yahooPostageName } from './shipping-labels.js';
import { executeRun, mallWriteEnabled } from './execute.js';
import { patchItemPrices, fetchItemDetail } from '../rakuten-yahoo-sync/lib/rakuten-rms-proxy.js';
import { loadShippingRates, resolveMallShippingCost } from './shipping-cost.js';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const router = express.Router();
const view = (name) => path.join(__dirname, 'views', name);

/** プレビューのサーバ側スナップショット。実行 (M2) はこれだけを信用する = 画面から行データを再受領しない */
const PREVIEW_TTL_MS = 15 * 60 * 1000;
const previews = new Map(); // previewId → { createdBy, createdAt, expiresAt, codes, rows }

function sweepPreviews(now = Date.now()) {
  for (const [id, p] of previews) if (p.expiresAt <= now) previews.delete(id);
}

// ─── CSRF 二段ガード (inquiry-hub / product-links と同型) ───
router.use('/api/', (req, res, next) => {
  if (['GET', 'HEAD', 'OPTIONS'].includes(req.method)) return next();
  // ★Origin は必須にする (Codex R1 Medium)。他アプリは「あれば照合」だが、このアプリは
  // この先モールへ書き込むので、Origin を落とした要求を通さない (fail-closed)
  const origin = req.headers.origin;
  let host = null;
  try { host = origin ? new URL(origin).host : null; } catch { /* 壊れた Origin は不一致扱い */ }
  if (!host || host !== req.headers.host) {
    return res.status(403).json({ ok: false, error: 'origin_mismatch', message: 'ブラウザから操作してください (Origin ヘッダが必要です)' });
  }
  if (!/^application\/json\b/i.test(String(req.headers['content-type'] || ''))) {
    return res.status(415).json({ ok: false, error: 'Content-Type は application/json にしてください' });
  }
  next();
});
router.use(express.json({ limit: '256kb' }));

function actorOf(req) {
  return req.session?.email || req.session?.displayName || 'unknown';
}
function isAdmin(req) {
  return req.session?.role === 'admin';
}
/**
 * 実行できる人か (要件⑨: 実行は中原さん + 奥様の2名)。
 * ★画面でボタンを隠すだけでは防御にならない。API を直接叩かれてもここで止める。
 * env PRICE_UPDATE_EXECUTORS にメールを並べて指定する (カンマ区切り)。名簿がすべて。
 * ★admin でも名簿に無ければ実行できない。未設定なら誰も実行できない (Codex R2 High)。
 *   「admin なら実行できる」にすると、権限を持つ人が増えた時に黙って実行者も増える。
 */
function executorGate(req) {
  const list = String(process.env.PRICE_UPDATE_EXECUTORS || '')
    .split(',').map((s) => s.trim().toLowerCase()).filter(Boolean);
  if (list.length === 0) {
    return { ok: false, message: '実行できる人がまだ設定されていません (環境変数 PRICE_UPDATE_EXECUTORS にメールを設定してください)。設定されるまで誰も実行できません' };
  }
  const email = String(req.session?.email || '').trim().toLowerCase();
  if (!list.includes(email)) {
    return { ok: false, message: '価格を実際に更新できるのは、実行権限のある人だけです (管理者でも名簿に無ければ実行できません)' };
  }
  return { ok: true, message: null };
}
function canExecute(req) {
  return executorGate(req).ok;
}

function apiError(res, e, where) {
  if (e?.code === 'VALIDATION') return res.status(400).json({ ok: false, error: e.message });
  console.error(`[price-update] ${where}:`, e);
  return res.status(500).json({ ok: false, error: 'サーバーエラーが発生しました' });
}
function validationError(message) {
  const e = new Error(message);
  e.code = 'VALIDATION';
  return e;
}

/** 貼り付け入力を NEコード配列に (改行・カンマ・タブ・全角空白区切り) */
export function parseCodes(input) {
  return [...new Set(String(input || '')
    .split(/[\s,、\r\n\t]+/u)
    .map((s) => s.trim())
    .filter(Boolean))];
}

/**
 * 新売価の厳格パース。★勝手に切り捨てない (Codex R1 High)。
 * 1200.9 を黙って 1200 にすると「入力した値」と「監査に残る値」がずれる。
 * @returns {{ok:true, value:number|null} | {ok:false, raw:any}}
 */
export function parseStrictPrice(v) {
  if (v === null || v === undefined || v === '') return { ok: true, value: null };
  if (typeof v === 'number') return Number.isInteger(v) ? { ok: true, value: v } : { ok: false, raw: v };
  if (typeof v === 'string') {
    const s = v.trim();
    if (s === '') return { ok: true, value: null };
    // 10進の整数表記だけ受ける ("1e3" や "1,000"、小数は拒否)
    if (!/^\d{1,12}$/.test(s)) return { ok: false, raw: v };
    return { ok: true, value: Number(s) };
  }
  return { ok: false, raw: v };
}

/** 行の一意キー (プレビュー内で行を指すため。運用IDではない) */
function rowKeyOf(i) { return `r${i}`; }

/**
 * 引き当て + ライブ価格取得 → プレビュー行を作る。
 * ライブ価格が取れた行だけ confirmed に昇格させる (要件 F1/F2)。
 */
async function buildPreviewRows(db, codes, costOverrides, deps = {}) {
  const dim = loadDimMall(db);
  const rates = loadShippingRates(db);   // 送料マスタ (配送方法ごとの配送関係費合計)
  const { targets, unknownCodes } = buildTargets(db, codes, { costOverrides });

  // モールごとに問い合わせ対象をまとめる (1商品1回。行ごとに叩かない)
  // 楽天は AM/AL/W の別名をまとめて渡す (同じ SKU の別名なので、どれか1つで manageNumber と
  // variant を特定できる — 別名ごとに問い合わせると全部「見つかりません」になる)
  // ★楽天の結果は「NEコード × モール」で引く。色違いは同じ商品管理番号を共有するので、
  //   出品コード (= 管理番号) をキーにすると、同じプレビューに BK と BE を並べた時に片方の価格を
  //   もう片方が上書きする
  const rakutenRowKey = (t, l) => `${normCode(t.neCode)}|${t.rowKind}|${normCode(l.listingCode)}`;
  const rakutenTargets = [];
  const yahooTargets = [];
  const amazonSkus = [];
  for (const t of targets) {
    for (const l of t.listings) {
      if (l.mall === 'rakuten') {
        rakutenTargets.push({
          key: l.listingCode, rowKey: rakutenRowKey(t, l), aliases: l.aliases || [l.listingCode],
          skuAliases: l.skuAliases || [], amAliases: l.amAliases || [],
          manageNumber: l.manageNumber || null, manageNumbers: l.manageNumbers || [],
        });
      }
      else if (l.mall === 'yahoo') yahooTargets.push({ key: l.listingCode, candidates: l.candidates || [l.listingCode] });
      else if (l.mall === 'amazon') amazonSkus.push(l.listingCode);
    }
  }

  const notices = [];
  let rakutenPrices = new Map();
  let yahooPrices = new Map();
  if (rakutenTargets.length > 0) {
    try {
      rakutenPrices = await fetchRakutenPrices(rakutenTargets, deps);
    } catch (e) {
      notices.push(`楽天の設定価格を取得できませんでした: ${e.message}`);
    }
  }
  if (yahooTargets.length > 0) {
    try {
      yahooPrices = await fetchYahooPrices(yahooTargets, deps);
    } catch (e) {
      notices.push(`Yahoo の設定価格を取得できませんでした: ${e.message}`);
    }
  }
  const amazonSnap = loadAmazonSnapshot(db, amazonSkus);
  const fetchedAt = new Date().toISOString();

  const rows = [];
  for (const t of targets) {
    for (const l of t.listings) {
      const key = normCode(l.listingCode);
      let price = null;
      let priceSource = null;
      let priceIsLive = false;
      let confidence = l.confidence;
      let note = null;
      let skuCode = l.skuCode;
      let listingCode = l.listingCode;
      let mallShipping = null;   // モール側の発送設定 (モールで配送方法が違うと売価差の理由になる)
      let url = listingUrl(l.mall, l.listingCode);

      if (!l.listingCode) {
        // 引き当てできなかったモール。「出品が無い」とは書かない (否定を証明できていないため)
        note = 'このモールの出品コードが見つかりませんでした';
      } else if (l.mall === 'rakuten') {
        const p = rakutenPrices.get(rakutenRowKey(t, l));
        if (p?.found) {
          price = p.price; priceSource = '楽天RMS (ライブ)'; priceIsLive = true;
          confidence = 'confirmed'; skuCode = p.skuCode;
          mallShipping = p.shipping ? { ...p.shipping, methodLabel: rakutenShippingLabel(p.shipping.methodGroup),
            methodName: rakutenShippingName(p.shipping.methodGroup) } : null;
          // 表示は実際の商品管理番号に差し替える (別名のままだと楽天の画面で探せない)
          if (p.manageNumber) { listingCode = p.manageNumber; url = listingUrl('rakuten', p.manageNumber); }
        } else {
          note = p?.reason || '設定価格を取得できませんでした';
          if (p?.manageNumber) url = listingUrl('rakuten', p.manageNumber);
        }
      } else if (l.mall === 'yahoo') {
        const p = yahooPrices.get(key);
        if (p?.found) {
          price = p.price; priceSource = 'Yahoo itemInfo (ライブ)'; priceIsLive = true;
          confidence = 'confirmed';
          mallShipping = p.shipping ? { ...p.shipping, postageLabel: yahooPostageLabel(p.shipping.postageSet),
            methodName: yahooPostageName(p.shipping.postageSet) } : null;
          // カラバリは「親の商品コード + 個別商品コード」で登録されている。
          // 当たった実際の商品コードに差し替える (Yahoo の画面で探せるように)
          if (p.itemCode) { listingCode = p.itemCode; url = listingUrl('yahoo', p.itemCode); }
          if (p.skuCode) skuCode = p.skuCode;
        } else {
          note = p?.reason || '設定価格を取得できませんでした';
        }
      } else if (l.mall === 'amazon') {
        const s = amazonSnap.get(key);
        if (s) {
          price = s.price; priceSource = `Amazon 日次スナップショット (${s.dateJst})`;
          url = listingUrl('amazon', l.listingCode, { asin: s.asin });
        }
        note = '更新対象外 (既存の Amazon 価格管理を使う)';
      } else {
        note = '手動更新 (管理画面で直す)';
      }

      // このモールの配送方法に対応する配送関係費 (粗利の計算に使う)。
      // 決められない時は商品マスタの値に戻し、その旨を画面に出す (黙って別の送料を使わない)
      const shipCost = resolveMallShippingCost(rates, {
        mallMethodName: mallShipping?.methodName || null,
        neShippingCode: t.shippingCode,
        neShippingCost: t.shipping,
      });

      rows.push({
        key: rowKeyOf(rows.length),
        neCode: t.neCode,
        productName: t.name,
        rowKind: t.rowKind,
        viaCode: t.viaCode,
        mall: l.mall,
        listingCode,
        skuCode,
        confidence,
        resolveSource: l.source,
        price,
        priceSource,
        priceIsLive,
        priceFetchedAt: priceIsLive ? fetchedAt : null,
        cost: t.cost,
        costSource: t.costSource,
        taxRate: t.taxRate,
        // ★このモールの配送方法で引いた配送関係費 (粗利・原価割れ判定はこれで計算する)
        shipping: shipCost.cost,
        shippingSource: shipCost.source,      // 'mall' = モールの配送方法で引けた / 'product' / 'unknown'
        shippingLabel: shipCost.label,        // 引けた配送方法の名前
        productShipping: t.shipping,          // 商品マスタの送料 (参考表示)
        neDeliveryMethod: t.deliveryMethod,   // NE側の配送方法
        mallShipping,                          // モール側の発送設定 (楽天=配送方法セット / Yahoo=Delivery等)
        feeRate: dim.feeRateOf(l.mall),
        url,
        note,
        // 初期値: 更新可能モールは未選択・未入力。手動モールはチェックリスト行
        selected: false,
        newPrice: null,
        manual: !UPDATABLE_MALLS.includes(l.mall),
      });
    }
  }
  return { rows, unknownCodes, notices, targets };
}

/** 行にガード評価を付ける (サーバ側でのみ行う) */
function evaluateRows(rows, { isRecovery = false } = {}) {
  return rows.map((r) => {
    if (r.manual) {
      return { ...r, evaluation: null };
    }
    const evaluation = evaluateRow({
      mall: r.mall,
      confidence: r.confidence,
      currentPrice: r.price,
      newPrice: r.newPrice,
      cost: r.cost,
      taxRate: r.taxRate,
      shipping: r.shipping,
      feeRate: r.feeRate,
      isRecovery,
    });
    return { ...r, evaluation };
  });
}

// ─── 画面 ───
router.get('/', (req, res) => {
  const db = getDB();
  res.render(view('index.ejs'), {
    title: '価格一括改定',
    displayName: req.session?.displayName || req.session?.email || '',
    isAdmin: isAdmin(req),
    limits: runLimits(),
    runs: listRuns(db, 20),
  });
});

router.get('/runs/:runId', (req, res) => {
  const db = getDB();
  const run = getRun(db, req.params.runId);
  if (!run) return res.status(404).render('forbidden', { username: req.session?.email, displayName: req.session?.displayName });
  res.render(view('run.ejs'), {
    title: `価格一括改定 履歴 ${run.run_id}`,
    displayName: req.session?.displayName || req.session?.email || '',
    isAdmin: isAdmin(req),
    run,
  });
});

// ─── API ───

/** 引き当て + ライブ価格取得。プレビューはサーバ側に持つ (画面から行データを再受領しない) */
router.post('/api/resolve', async (req, res) => {
  try {
    const db = getDB();
    const limits = runLimits();
    const codes = parseCodes(req.body?.codes);
    if (codes.length === 0) throw validationError('NE商品コードを入力してください');
    if (codes.length > limits.maxNeCodes) {
      throw validationError(`一度に扱えるのは ${limits.maxNeCodes} コードまでです (入力 ${codes.length} 件)`);
    }
    const costOverrides = new Map();
    for (const [k, v] of Object.entries(req.body?.costOverrides || {})) {
      const n = Number(v);
      if (Number.isFinite(n) && n >= 0) costOverrides.set(normCode(k), n);
    }

    const { rows, unknownCodes, notices } = await buildPreviewRows(db, codes, costOverrides);
    if (rows.length > limits.maxSkuRows) {
      throw validationError(`対象行が多すぎます (${rows.length} 行 / 上限 ${limits.maxSkuRows} 行)。コードを分けて実行してください`);
    }
    // 1 NEコードからの展開が多すぎる場合も止める (要件 F4)
    const perCode = new Map();
    for (const r of rows) perCode.set(r.neCode, (perCode.get(r.neCode) || 0) + 1);
    for (const [code, n] of perCode) {
      if (n > limits.maxRowsPerNeCode) throw validationError(`${code} の展開が ${n} 行になりました (上限 ${limits.maxRowsPerNeCode} 行)`);
    }

    sweepPreviews();
    const previewId = newId('pup');
    const now = Date.now();
    previews.set(previewId, {
      createdBy: actorOf(req),
      createdAt: now,
      expiresAt: now + PREVIEW_TTL_MS,
      codes,
      costOverrides: Object.fromEntries(costOverrides),
      rows,
    });
    res.json({
      ok: true, previewId, expiresAt: new Date(now + PREVIEW_TTL_MS).toISOString(),
      rows: evaluateRows(rows), unknownCodes, notices, limits,
    });
  } catch (e) {
    apiError(res, e, 'resolve');
  }
});

/** 新売価の入力を反映してガードを再評価 (ライブ価格は取り直さない = プレビュー時点の値で判定) */
router.post('/api/preview', (req, res) => {
  try {
    sweepPreviews();
    const p = previews.get(String(req.body?.previewId || ''));
    if (!p) throw validationError('プレビューの有効期限が切れました。もう一度検索からやり直してください');
    if (p.createdBy !== actorOf(req)) throw validationError('他の人が作ったプレビューは操作できません');
    const inputs = new Map();
    for (const row of Array.isArray(req.body?.rows) ? req.body.rows : []) {
      inputs.set(String(row?.key || ''), row);
    }
    const bad = [];
    const next = p.rows.map((r) => {
      const inp = inputs.get(r.key);
      if (!inp) return r;
      const parsed = parseStrictPrice(inp.newPrice);
      if (!parsed.ok) { bad.push(`${r.mall} ${r.listingCode}: ${JSON.stringify(inp.newPrice)}`); return r; }
      return { ...r, newPrice: parsed.value, selected: !!inp.selected };
    });
    if (bad.length > 0) {
      throw validationError(`新売価は整数円で入力してください (小数・指数表記・カンマは不可): ${bad.join(' / ')}`);
    }
    p.rows = next;
    res.json({ ok: true, rows: evaluateRows(p.rows) });
  } catch (e) {
    apiError(res, e, 'preview');
  }
});

/** プレビューを履歴として記録する (M1 はここまで。実際の更新は M2 以降) */
router.post('/api/runs', (req, res) => {
  try {
    sweepPreviews();
    const db = getDB();
    const p = previews.get(String(req.body?.previewId || ''));
    if (!p) throw validationError('プレビューの有効期限が切れました。もう一度検索からやり直してください');
    if (p.createdBy !== actorOf(req)) throw validationError('他の人が作ったプレビューは操作できません');

    const evaluated = evaluateRows(p.rows);
    // 未解決行 (出品コードを引き当てられなかったモール) は自動では記録しない。
    // 手動更新リストに「コード不明の行」が積み上がっても、チェックのしようがない
    const chosen = evaluated.filter((r) => r.selected || (r.manual && r.confidence !== 'unresolved' && r.listingCode));
    if (chosen.length === 0) throw validationError('記録する行が選ばれていません');

    const note = String(req.body?.note || '').slice(0, 500) || null;
    const operations = chosen.map((r) => ({
      operationId: newId('puo'),
      mall: r.mall,
      neCode: r.neCode,
      rowKind: r.rowKind,
      viaCode: r.viaCode,
      productName: r.productName,
      listingCode: r.listingCode,
      skuCode: r.skuCode,
      confidence: r.confidence,
      priceSource: r.priceSource,
      priceFetchedAt: r.priceFetchedAt,
      expectedCurrentPrice: r.price,
      newPrice: r.newPrice,
      cost: r.cost,
      taxRate: r.taxRate,
      shipping: r.shipping,
      feeRate: r.feeRate,
      guard: r.evaluation ? { blocks: r.evaluation.blocks, warns: r.evaluation.warns, canUpdate: r.evaluation.canUpdate } : null,
      productUrl: r.url,
      // ★ガードに引っかかった行は previewed にしない (Codex R1 High)。
      // M2 の実行候補は previewed だけを見る。ブロック理由つきの行が同じ状態で混ざると、
      // 「引き当て未確定」「原価割れ」の行をそのままモールに送りかねない
      initialState: r.manual ? 'manual_required' : (r.evaluation?.canUpdate ? 'previewed' : 'blocked_preview'),
    }));

    const runId = insertRun(db, {
      createdBy: actorOf(req),
      kind: 'normal',
      note,
      neCodes: p.codes,
      limits: runLimits(),
      operations,
    });
    previews.delete(String(req.body?.previewId || ''));
    res.json({ ok: true, runId });
  } catch (e) {
    apiError(res, e, 'create-run');
  }
});

/**
 * run を実行する (M2)。
 * ★画面から価格を受け取らない。保存済みの run の内容だけを送る
 *   (画面を書き換えて別の値を送る、という抜け道を作らないため)。
 * ★確認文字列を要求する (押し間違いで走らないように)。
 */
router.post('/api/runs/:runId/execute', async (req, res) => {
  try {
    const db = getDB();
    const gate = executorGate(req);
    if (!gate.ok) {
      return res.status(403).json({ ok: false, error: 'forbidden', message: gate.message });
    }
    const run = getRun(db, req.params.runId);
    if (!run) throw validationError('履歴が見つかりません');
    if (String(req.body?.confirm || '') !== '実行する') {
      throw validationError('実行するには確認欄に「実行する」と入力してください');
    }
    const targets = run.operations.filter((o) => o.state === 'previewed');
    if (targets.length === 0) throw validationError('実行できる行がありません (すでに実行済み、またはガードで止まっています)');

    const limits = runLimits();
    const neCodes = new Set(targets.map((o) => o.ne_code));
    if (neCodes.size > limits.maxNeCodes) {
      throw validationError(`一度に実行できるのは ${limits.maxNeCodes} コードまでです (対象 ${neCodes.size} コード)`);
    }
    if (targets.length > limits.maxSkuRows) {
      throw validationError(`対象行が多すぎます (${targets.length} 行 / 上限 ${limits.maxSkuRows} 行)`);
    }

    const out = await executeRun(db, run, {
      actor: actorOf(req),
      client: { patchItemPrices, fetchItemDetail },
    });
    res.json({ ok: true, ...out });
  } catch (e) {
    if (e?.code === 'ALREADY_EXECUTED') return res.status(409).json({ ok: false, error: 'already_executed', message: e.message });
    apiError(res, e, 'execute-run');
  }
});

/** 実行できる状態か (画面がボタンを出すかどうかの判断に使う) */
router.get('/api/runs/:runId/executable', (req, res) => {
  try {
    const run = getRun(getDB(), req.params.runId);
    if (!run) return res.status(404).json({ ok: false, error: 'not_found' });
    const targets = run.operations.filter((o) => o.state === 'previewed');
    const malls = [...new Set(targets.map((o) => o.mall))];
    res.json({
      ok: true,
      targets: targets.length,
      canExecute: canExecute(req),
      canExecuteReason: executorGate(req).message,   // 実行できない理由を画面に出すため
      claim: runClaim(getDB(), run.run_id),   // 既に実行済みならここに誰がいつ実行したかが入る
      gates: Object.fromEntries(malls.map((m) => [m, mallWriteEnabled(m)])),
    });
  } catch (e) {
    apiError(res, e, 'executable');
  }
});

/**
 * 復旧 run を作る (要件 F6・M2-4)。
 * ★戻す先の価格は **監査記録から取る**。画面からは run_id しか受け取らない
 *   (「戻すつもりで別の値を送る」余地を作らない)。
 * ★作るだけでは何も送らない。送信はいつも通り実行API (確認文字列 + claim + 試運転 + 照合) を通る。
 */
router.post('/api/runs/:runId/recovery', async (req, res) => {
  try {
    const db = getDB();
    // 価格を戻すのも価格を変える操作。実行と同じ名簿で守る
    const gate = executorGate(req);
    if (!gate.ok) return res.status(403).json({ ok: false, error: 'forbidden', message: gate.message });

    const source = getRun(db, req.params.runId);
    if (!source) throw validationError('元の履歴が見つかりません');

    const { candidates, skipped } = planRecovery(source);
    if (candidates.length === 0) {
      throw validationError('戻す対象がありません (価格が変わった行がこの履歴にはありません)');
    }

    // いま引き当て直してライブ価格を取る (楽観ロックの基準は「いまモールにある価格」)
    const codes = [...new Set(candidates.map((c) => c.op.ne_code))];
    const { rows, notices } = await buildPreviewRows(db, codes, new Map());
    const evaluate = (row) => evaluateRows([row], { isRecovery: true })[0];
    const { operations, unmatched } = buildRecoveryOperations(candidates, rows, evaluate);
    if (operations.length === 0) {
      throw validationError('戻す対象の出品が今は見つかりません。モールの画面で確認してください');
    }

    // ★「既にあるか調べる → 作る」は1つの取引の中で (同時に2回押されても2本作らない)
    const created = createRecoveryRun(db, {
      sourceRunId: source.run_id,
      allowRepeat: req.body?.confirmRepeat === true,
      runSpec: {
        createdBy: actorOf(req),
        note: `${source.run_id} を元の価格に戻す`,
        neCodes: codes,
        limits: runLimits(),
        operations,
      },
    });
    if (!created.ok) {
      const message = created.code === 'RECOVERY_EXISTS'
        ? `この履歴の復旧 run は既に作ってあります (${created.runId})。そちらを実行してください`
        : `この履歴は既に一度戻しています (${created.runId})。もう一度戻すと、いまの価格を当時の価格に上書きします。それでよければ画面の確認にチェックを入れてください`;
      return res.status(409).json({ ok: false, error: created.code.toLowerCase(), runId: created.runId, message });
    }
    const runId = created.runId;
    // 元の run 側にも「復旧 run を作った」ことを残す (追記のみ)
    appendEvent(db, source.run_id, {
      actor: actorOf(req), event: 'recovery_created',
      detail: { recoveryRunId: runId, rows: operations.length },
    });
    res.json({
      ok: true, runId, rows: operations.length,
      skipped: skipped.map((s) => ({ operationId: s.op.operation_id, neCode: s.op.ne_code, reason: s.reason })),
      unmatched: unmatched.map((u) => ({ operationId: u.op.operation_id, neCode: u.op.ne_code, reason: u.reason })),
      notices,
    });
  } catch (e) {
    apiError(res, e, 'create-recovery');
  }
});

/** 復旧 run を作れるか (画面がボタンを出すかの判断材料) */
router.get('/api/runs/:runId/recoverable', (req, res) => {
  try {
    const db = getDB();
    const run = getRun(db, req.params.runId);
    if (!run) return res.status(404).json({ ok: false, error: 'not_found' });
    const { candidates } = planRecovery(run);
    const existing = recoveryRunsOf(db, run.run_id).map((r) => ({ ...r, executed: !!runClaim(db, r.run_id) }));
    res.json({
      ok: true,
      rows: candidates.length,
      states: RECOVERABLE_STATES,
      canCreate: canExecute(req),
      canCreateReason: executorGate(req).message,
      existing,
    });
  } catch (e) {
    apiError(res, e, 'recoverable');
  }
});

/** 手動更新チェック (Amazon / auPAY / Qoo10)。状態はイベント追記で表す */
router.post('/api/runs/:runId/manual', (req, res) => {
  try {
    const db = getDB();
    const run = getRun(db, req.params.runId);
    if (!run) throw validationError('履歴が見つかりません');
    const operationId = String(req.body?.operationId || '');
    const op = run.operations.find((o) => o.operation_id === operationId);
    if (!op) throw validationError('対象の行が見つかりません');
    if (op.initial_state !== 'manual_required') throw validationError('手動更新の対象行ではありません');
    const done = !!req.body?.done;
    appendEvent(db, run.run_id, {
      operationId,
      actor: actorOf(req),
      event: done ? 'manual_done' : 'manual_required',
      detail: { note: String(req.body?.note || '').slice(0, 200) || null },
    });
    res.json({ ok: true, state: done ? 'manual_done' : 'manual_required' });
  } catch (e) {
    apiError(res, e, 'manual-check');
  }
});

router.get('/api/runs', (req, res) => {
  try {
    res.json({ ok: true, runs: listRuns(getDB(), Math.min(parseInt(req.query.limit, 10) || 50, 200)) });
  } catch (e) {
    apiError(res, e, 'list-runs');
  }
});

router.get('/api/runs/:runId', (req, res) => {
  try {
    const run = getRun(getDB(), req.params.runId);
    if (!run) return res.status(404).json({ ok: false, error: 'not_found' });
    res.json({ ok: true, run });
  } catch (e) {
    apiError(res, e, 'get-run');
  }
});

export default router;
export { buildPreviewRows, evaluateRows, executorGate };
