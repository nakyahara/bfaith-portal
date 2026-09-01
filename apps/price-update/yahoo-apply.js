/**
 * yahoo-apply.js — Yahoo の価格を実際に書き換える (M3-1)
 *
 * ★楽天は miniPC が「ロック内で GET → 照合 → PATCH」までやってくれる。Yahoo にその仕組みは無いので、
 *   同じ守りを **ここで自分でやる**:
 *     読む → 記録時の価格と照合 → 送る → 読み直して確かめる → 反映を依頼する
 *
 * ★応答の形は **miniPC の楽天エンドポイントに合わせる**。そうすれば execute.js の
 *   classify() / 試運転 / ブレーカー / 想定外は即停止 がそのまま効く (モールごとに判定を作らない)。
 *
 * 🚨使う API は **商品一括更新 (updateItems)**。editItem は使わない。
 *   実測 (2026-09-01): editItem は全項目上書きで、送らなかった商品説明が消える。
 *   updateItems は「指定された項目だけを更新し、省略された項目は更新しません」(公式・実測で確認済み)。
 *
 * 🚨反映は非同期。updateItems はフロント反映しないので submitItem を呼ぶが、
 *   **その場では終わらない**。実行は「反映を依頼した」まで。フロントに出たかは後から確かめる。
 */
import { fetchYahooItemDetail } from '../rakuten-yahoo-sync/lib/yahoo-detail-proxy.js';

/** VPS プロキシに繋ぐための env (yahoo-detail-proxy と揃える) */
function proxyBase() {
  const v = process.env.YAHOO_PROXY_BASE_URL || process.env.YAHOO_PROXY_URL;
  if (!v || !String(v).trim()) throw new Error('YAHOO_PROXY_BASE_URL (または YAHOO_PROXY_URL) が未設定です');
  return String(v).trim().replace(/\/+$/, '');
}
function proxySecret() {
  const v = process.env.YAHOO_PROXY_SECRET;
  if (!v || !String(v).trim()) throw new Error('YAHOO_PROXY_SECRET が未設定です');
  return String(v).trim();
}

const TIMEOUT_MS = 30_000;

/** 整数円として読めた時だけ数値を返す (読めなければ null) */
export function toIntPrice(v) {
  if (typeof v === 'number') return Number.isSafeInteger(v) ? v : null;
  if (typeof v !== 'string') return null;
  const s = v.trim();
  if (!/^\d+$/.test(s)) return null;
  const n = Number(s);
  return Number.isSafeInteger(n) ? n : null;
}

/**
 * 送ってよい商品か。問題があれば「意味の分かる失敗」として返す形を作る。
 * ★どれも **送る前に** 分かること。分からない状態のまま送らない。
 *
 * @param {object} detail get-item-detail の応答
 * @param {string} itemCode 送ろうとしている商品コード
 * @param {object} expected {[sku]: 円} 記録時の価格
 * @param {object} prices   {[sku]: 円} 新しい価格
 * @returns {{ok:true, currentPrice:number, noop:boolean} | {ok:false, status:number, body:object}}
 */
export function planYahooUpdate(detail, itemCode, expected, prices) {
  const bad = (status, error, message, extra = {}) => ({ ok: false, status, body: { ok: false, error, message, ...extra } });

  if (!detail || detail.ok === false) {
    return bad(404, 'ITEM_NOT_FOUND', `Yahoo でこの商品を取得できませんでした (${itemCode})`);
  }
  // ★取ってきた商品が本当に送ろうとしている商品か (別商品に値付けしない)
  if (String(detail.ItemCode || '').trim().toLowerCase() !== String(itemCode).trim().toLowerCase()) {
    return bad(404, 'ITEM_NOT_FOUND',
      `取得した商品コードが違います (要求 ${itemCode} / 応答 ${detail.ItemCode ?? 'なし'})`);
  }
  // ★セール価格。updateItems は price と sale_price を必ず両方送る決まりで、
  //   空文字を送ると既存のセール価格が消える。入っている商品は触らない
  if (detail.SalePriceReadable !== true) {
    return bad(400, 'SALE_PRICE_UNREADABLE',
      'セール価格が入っているか確かめられませんでした。消してしまう恐れがあるため送りません');
  }
  if (detail.SalePrice !== null && detail.SalePrice !== undefined) {
    return bad(400, 'SALE_PRICE_PRESENT',
      `この商品にはセール価格 (${detail.SalePrice} 円) が入っています。`
      + '価格を送るとセール価格が消えるため、このツールでは更新しません');
  }
  // ★SKU別価格がある商品は M3-1 では対象外 (subcode_price の実機確認がまだ)
  const subs = Array.isArray(detail.SubCodes) ? detail.SubCodes : [];
  if (subs.some((s) => s && s.Price != null)) {
    return bad(400, 'SUBCODE_PRICE_UNSUPPORTED',
      'この商品は個別商品ごとに価格が設定されています。いまの版では更新できません (M3-2 で対応)');
  }
  // ★送る値は「商品1つに1つの価格」だけ。SKU 別の値が渡ってきたら受けない
  const wantKeys = Object.keys(prices || {});
  if (wantKeys.length !== 1) {
    return bad(400, 'MULTIPLE_PRICES',
      `Yahoo は商品ごとに1つの価格しか送りません (受け取った数: ${wantKeys.length})`);
  }
  const key = wantKeys[0];
  const next = toIntPrice(prices[key]);
  if (next === null || next < 1) {
    return bad(400, 'INVALID_PRICE', `送ろうとした価格が整数円ではありません (${prices[key]})`);
  }

  const current = toIntPrice(detail.Price);
  if (current === null) {
    return bad(400, 'CURRENT_PRICE_UNREADABLE', 'いまの価格を整数円として読めません');
  }
  // ★楽観ロック。記録した時の価格と今の価格が違えば送らない (誰かの変更を踏み潰さない)
  const want = toIntPrice(expected?.[key]);
  if (want === null) {
    return bad(400, 'EXPECTED_REQUIRED', '記録時の価格が分からないため送りません');
  }
  if (want !== current) {
    return {
      ok: false, status: 409,
      body: {
        ok: false, state: 'conflict', error: 'CONFLICT',
        message: '現在価格が想定と違います',
        detail: { conflicts: [{ sku: key, expected: want, live: current, reason: '現在価格が想定と違います' }] },
      },
    };
  }
  return { ok: true, currentPrice: current, noop: current === next, sku: key, price: next };
}

/**
 * Yahoo 用のクライアント。execute.js からは楽天と同じ形で呼ばれる。
 * @param {object} deps テスト用の差し替え (getDetail / postUpdate)
 */
export function makeYahooClient(deps = {}) {
  const getDetail = deps.getDetail || ((code) => fetchYahooItemDetail(code));
  const postUpdate = deps.postUpdate || defaultPostUpdate;

  return {
    /**
     * 照合のための再取得。楽天と同じ形 ({ item: { variants: { sku: { standardPrice } } } }) にそろえる。
     * ★Yahoo は商品に1つの価格なので、variants のキーは商品コードそのもの
     */
    async fetchItemDetail(itemCode) {
      const d = await getDetail(itemCode);
      if (!d || d.ok === false || d.Price === null || d.Price === undefined) {
        return { item: null, status: 'not_found' };
      }
      return {
        item: { manageNumber: d.ItemCode, variants: { [d.ItemCode]: { standardPrice: String(d.Price) } } },
        status: 'found',
      };
    },

    /**
     * 価格を送る。応答は miniPC の楽天エンドポイントと同じ形にそろえる
     * (execute.js の classify() をそのまま使うため)。
     */
    async patchItemPrices(itemCode, { expected, prices }) {
      const detail = await getDetail(itemCode);
      const plan = planYahooUpdate(detail, itemCode, expected, prices);
      if (!plan.ok) return { status: plan.status, body: plan.body };

      if (plan.noop) {
        return { status: 200, body: { ok: true, state: 'noop', applied: {} } };
      }
      const res = await postUpdate(itemCode, plan.price, plan.currentPrice);
      // ★VPS が「更新 + 反映依頼」をまとめて行い、JSON で結果を返す
      if (res.status >= 200 && res.status < 300 && res.json && res.json.ok === true) {
        const submits = Array.isArray(res.json.submits) ? res.json.submits : [];
        const publish = {
          requested: res.json.submitted === true,
          ok: submits.length > 0 && submits.every((s) => s.ok),
        };
        // ★反映を依頼できていなければ「終わった」と言わない (Codex R1 High)。
        //   価格は変わっているのに客には見えない状態なので、成功にすると誰も気づかない。
        //   state を付けずに返すと execute 側が「想定外」として **その場で止める** ので、
        //   人が確かめるまで残りを送らない。価格が変わったことは applied に残す
        if (!publish.requested || !publish.ok) {
          return {
            status: 200,
            body: {
              ok: false,
              error: 'PUBLISH_FAILED',
              message: `価格は ${plan.price} 円に変わりましたが、フロント反映を依頼できていません`
                + `${publish.requested ? ' (反映の依頼が失敗)' : ' (反映を依頼していない)'}。`
                + 'Yahoo の管理画面で反映状況を確かめてください',
              applied: { [plan.sku]: plan.price },
              publish,
            },
          };
        }
        return {
          status: 200,
          body: {
            ok: true, state: 'applied',
            applied: { [plan.sku]: plan.price },
            // ★反映は非同期。ここで分かるのは「依頼できたか」まで。
            //   フロントに出たかは後から未反映一覧で確かめる
            publish,
          },
        };
      }
      // ★VPS が「今の価格が想定と違う」と言ってきた時は、楽天と同じ conflict として返す (Codex R4)。
      //   ひとまとめに失敗へ潰すと、誰かの変更とぶつかったのか別の理由なのかが記録から分からなくなる
      if (res.status === 409 && res.json?.error === 'CONFLICT') {
        const c = res.json.conflict || {};
        return {
          status: 409,
          body: {
            ok: false, state: 'conflict', error: 'CONFLICT',
            message: c.reason || '現在価格が想定と違います',
            detail: { conflicts: [{ sku: plan.sku, expected: c.expected ?? plan.currentPrice, live: c.live ?? null, reason: c.reason || '現在価格が想定と違います' }] },
          },
        };
      }
      // 更新そのものが通らなかった。中身が読めるなら「意味の分かる失敗」、読めなければ想定外へ倒す
      const message = String(res.json?.updateBody || res.body || '').slice(0, 300).replace(/\s+/g, ' ');
      if (res.status >= 400 && res.status < 500) {
        return { status: 400, body: { ok: false, error: 'YAHOO_REJECTED', message } };
      }
      return { status: res.status, body: { ok: false, message } };
    },
  };
}

/** VPS の /yahoo/update-items を叩く (更新 + 反映依頼がまとまっている) */
async function defaultPostUpdate(itemCode, price, expectedPrice) {
  const res = await fetch(`${proxyBase()}/yahoo/update-items`, {
    method: 'POST',
    headers: { 'X-Proxy-Secret': proxySecret(), 'Content-Type': 'application/json' },
    body: JSON.stringify({
      // ★公式: price を更新する時は sale_price も必ず送る。うちはセール価格を使っていないので空文字。
      //   「消してよい」と明示する (入っている商品は planYahooUpdate が手前で止めている)
      items: [{ item_code: itemCode, price: String(price), sale_price: '' }],
      clearSalePrice: true,
      // ★VPS 側でも「送る直前に読み直して照合」してもらう (Codex R1 High)。
      //   Render で読んでから送るまでの窓を、あちらのロックの中で閉じる
      expected: { [itemCode]: expectedPrice },
    }),
    signal: AbortSignal.timeout(TIMEOUT_MS),
  });
  const text = await res.text();
  let json = null;
  try { json = JSON.parse(text); } catch { /* エラー時は XML やテキストが返る */ }
  return { status: res.status, body: text, json };
}
