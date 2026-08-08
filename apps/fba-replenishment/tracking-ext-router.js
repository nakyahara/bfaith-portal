/**
 * FBA納品 → 福山通運の伝票データ の Chrome拡張向けAPI
 * (セッション認証なし・x-api-key 認証。easy-ship / select-set と同パターン)
 *
 * ⭐これがあると人の作業がこう変わる:
 *     旧: Seller Centralを見ながらスプレッドシートに宛先コードと箱数を手入力 → GASでCSV生成
 *     新: Seller Centralの画面でボタンを押すだけでCSVが落ちる
 *   さらに **U列「お客様管理番号」に納品番号が入る** ので、22時の追跡番号投入が
 *   FCコードや箱数からの推測なしに一意で決まる (同一FC宛が複数でも取り違えない)。
 *
 * - トークンは env FBA_TRACKING_EXT_TOKEN (未設定なら 503 fail-closed)
 * - server.js で requireAppAccess 付き本体より先に mount し、グローバル JSON parser から除外する
 * - **読み取りのみ**。SP-APIへの書き込みも福山通運のシステムへのアクセスも行わない
 *   (作ったCSVは人が iS-2 の「CSVエントリー」から取り込む = 従来と同じ公式ルート)
 */
import { Router } from 'express';
import crypto from 'crypto';
import express from 'express';
import { extractPlanId, getPlanShipments as realGetPlanShipments, missingEnv as realMissingEnv } from './tracking-service.js';
import { buildFukutsuCsv } from './fukutsu-csv.js';
import { isRegisteredFc } from './fukutsu-master.js';
import { jstYmd } from './tracking-runner.js';

function timingSafeEq(a, b) {
  const ab = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ab.length !== bb.length) return false;
  return crypto.timingSafeEqual(ab, bb);
}

function requireExtToken(req, res, next) {
  const expected = process.env.FBA_TRACKING_EXT_TOKEN;
  if (!expected) {
    return res.status(503).json({
      success: false,
      error: { code: 'INTERNAL_ERROR', message: 'fba_tracking_ext_token_unset' },
    });
  }
  const provided = req.headers['x-api-key'];
  if (typeof provided !== 'string' || !timingSafeEq(provided, expected)) {
    return res.status(401).json({ success: false, error: { code: 'UNAUTHORIZED', message: 'APIキーが不正です' } });
  }
  next();
}

const ok = (data) => ({ success: true, data });
const fail = (code, message) => ({ success: false, error: { code, message } });

/**
 * ⭐SP-APIへの依存を差し込めるようにしてある。
 *   既定は本物。テストではスタブを渡し、外部APIを叩かずにHTTPの振る舞いだけを確かめる。
 */
export function createTrackingExtRouter(deps = {}) {
  const getPlanShipments = deps.getPlanShipments ?? realGetPlanShipments;
  const missingEnv = deps.missingEnv ?? realMissingEnv;

  const router = Router();
  router.use(requireExtToken);
  // body parser は認証の後 (未認可リクエストに parse させない)
  router.use(express.json({ limit: '64kb' }));

  /** プランIDを取り出し、SP-APIの設定も確認する。問題があればレスポンスを返して null。 */
  async function resolvePlan(req, res) {
    const miss = missingEnv();
    if (miss.length) {
      res.status(503).json(fail('INTERNAL_ERROR', `SP-APIの環境変数が不足しています: ${miss.join(', ')}`));
      return null;
    }
    const planId = extractPlanId(req.query.wf ?? req.query.plan ?? req.query.url);
    if (!planId) {
      res.status(400).json(fail('VALIDATION', '納品プランIDが取れません (Seller CentralのURLか wfxxxx… を渡してください)'));
      return null;
    }
    try {
      const plan = await getPlanShipments(planId);
      if (!plan.shipments.length) {
        res.status(404).json(fail('NOT_FOUND', 'この納品プランには輸送箱のある納品がありません (梱包情報の登録前かもしれません)'));
        return null;
      }
      return { planId, plan };
    } catch (e) {
      res.status(502).json(fail('UPSTREAM', `Amazonから納品情報を取得できません: ${e?.message ?? e}`));
      return null;
    }
  }

  /** 拡張が「何を落とそうとしているか」を先に見せるための下見 */
  router.get('/plan', async (req, res) => {
    const r = await resolvePlan(req, res);
    if (!r) return;
    const { planId, plan } = r;
    res.json(ok({
      inboundPlanId: planId,
      planName: plan.planName,
      planStatus: plan.planStatus,
      出荷日: jstYmd(),
      伝票枚数: plan.shipments.reduce((a, s) => a + s.boxCount, 0),
      shipments: plan.shipments.map((s) => ({
        納品番号: s.shipmentConfirmationId,
        宛先FC: s.fcCode,
        箱数: s.boxCount,
        status: s.status,
        追跡番号: s.hasTracking ? '登録済み' : '未登録',
        宛先: isRegisteredFc(s.fcCode) ? 'マスタ登録済み' : '⚠️ マスタ未登録 (住所を出力します)',
      })),
    }));
  });

  /** 福山通運 iS-2 の「CSVエントリー」に取り込む26列CSVを返す */
  router.get('/plan-csv', async (req, res) => {
    const r = await resolvePlan(req, res);
    if (!r) return;
    const { planId, plan } = r;

    const ymd = /^\d{8}$/.test(String(req.query.date ?? '')) ? String(req.query.date) : jstYmd();
    let built;
    try {
      built = buildFukutsuCsv(plan.shipments, ymd);
    } catch (e) {
      return res.status(422).json(fail('VALIDATION', e?.message ?? String(e)));
    }

    // 拡張が中身を見ずに保存できるよう、要点はヘッダーで返す (本文はCSVそのもの)
    res.setHeader('X-Fukutsu-Rows', String(built.summary.伝票枚数));
    res.setHeader('X-Fukutsu-Shipments', String(built.summary.納品数));
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="fukutu_${planId.slice(0, 10)}_${ymd}.csv"`);
    res.send(built.csv);
  });

  return router;
}

export default createTrackingExtRouter();
