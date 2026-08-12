/**
 * Yahoo!ショッピング 問い合わせ対応漏れチェック — 検知ロジック
 *
 * 目的:
 *   問い合わせ回答率の悪化を防ぐ。返信していても「完了する」(回答済み) 処理を忘れると
 *   回答率に反映されないため、次の2種類を毎朝検知して GChat に通知する:
 *     ① 未返信 (出店者回答待ち)                 … isNoAnswer=true
 *     ② 返信済みだが完了処理されていない        … isNoAnswer=false かつ isCompleted=false
 *   対象はストアクリエイターPro 問い合わせ管理ツールの「通常」(shp) と
 *   「オークション (落札前)」(auc) の両方。
 *
 * データ源: 問い合わせ管理API 質問一覧 (externalTalkList)。
 *   https://developer.yahoo.co.jp/webapi/shopping/question/list.html
 *   - VPSプロキシ (vps-proxy/aupay-proxy.js) の read-only passthrough 経由
 *     (Yahoo!システムAPIは登録済み固定IPからしか呼べない。Bearer+公開鍵署名はVPS側で付与)
 *   - requestFilter=unanswered / answered をサーバー側で絞り、**1ページ目だけ**読む。
 *     件数は summary.topic.count (絞り込み後の総件数) から取るので全ページ走査は不要。
 *     一覧はソート指定の降順 → 1ページ目 (20件) = 最新の該当分 = 通知に載せる分
 *
 * 🚨fail-closed の考え方 (0件と「読めない」を混同しない):
 *   - summary.filter が要求した requestFilter と一致しなければエラー
 *     (旧プロキシがパラメータを転送していない場合、全件が返って summary.filter='all' になる。
 *      そのまま进むと件数を誤読するので必ず止める)
 *   - unanswered の結果に isNoAnswer=false が混ざる、answered の結果に isCompleted=true が
 *     混ざる場合もエラー (フィルタ語義が想定 (相互排他な状態) と違う = 件数を信用できない)
 *   - shp と auc の結果に同じ topicId が出たらエラー (serviceType が効いていない)
 *
 * 個人情報: 通知に載せるのはタイトルと日時のみ。注文者名・マスクIDは扱わない。
 */

/** 1リクエスト/秒のレート制限 (公式)。余裕を持って1.1秒 */
export const SLEEP_MS = 1100;
/** 通知に載せる明細の上限 (カテゴリごと)。超えた分は件数だけ伝える */
export const MAX_SHOW = 10;
/** externalTalkList の1ページ上限 (公式仕様) */
const PAGE_SIZE = 20;
/** ② 完了処理待ちを「要対応」として通知する窓 (日)。窓外の過去分は件数だけ添える */
export const DEFAULT_WINDOW_DAYS = 30;
/** 1照会あたりのページ読み込み上限 (20件×15=300件。1リクエスト/秒なので暴走防止) */
export const MAX_PAGES_PER_QUERY = 15;

export const SERVICE_LABEL = { shp: '通常', auc: 'オークション' };

/**
 * 実行する照会の一覧。
 *   kind 'unanswered'  = ① 未返信 → 顧客の最終投稿 (userPostTime) が新しい順
 *   kind 'uncompleted' = ② 返信済み・完了処理待ち → 店舗の最終返信 (sellerPostTime) が新しい順
 */
export const QUERIES = [
  { serviceType: 'shp', requestFilter: 'unanswered', kind: 'unanswered', sort: 'userPostTime' },
  { serviceType: 'auc', requestFilter: 'unanswered', kind: 'unanswered', sort: 'userPostTime' },
  { serviceType: 'shp', requestFilter: 'answered', kind: 'uncompleted', sort: 'sellerPostTime' },
  { serviceType: 'auc', requestFilter: 'answered', kind: 'uncompleted', sort: 'sellerPostTime' },
];

export function proxyConfig(env = process.env) {
  const base = (env.YAHOO_PROXY_URL || 'http://133.167.122.198:8080').replace(/\/+$/, '');
  const secret = env.YAHOO_PROXY_SECRET || env.AUPAY_PROXY_SECRET || '';
  if (!secret) throw new Error('YAHOO_PROXY_SECRET (または AUPAY_PROXY_SECRET) が未設定です');
  return { base, secret };
}

/** UNIX秒 → 'M/D HH:MM' (JST)。不正は null */
export function fmtJstFromUnixSec(v) {
  const n = Number(v);
  if (!Number.isFinite(n) || n <= 0) return null;
  const t = new Date(n * 1000 + 9 * 3600 * 1000);
  const pad2 = x => String(x).padStart(2, '0');
  return `${t.getUTCMonth() + 1}/${t.getUTCDate()} ${pad2(t.getUTCHours())}:${pad2(t.getUTCMinutes())}`;
}

function contractError(msg) {
  const e = new Error(msg);
  e.errorType = 'contract_violation';
  return e;
}

/**
 * 一覧レスポンス1ページ分を検証する。壊れていたら throw (握り潰さない)。
 * @param {number} start このページの読み込み開始位置 (1始まり)
 * @returns {{count:number, headlines:object[]}}
 */
export function validateListResponse(json, q, start = 1) {
  const label = `${q.requestFilter}/${q.serviceType}`;
  const filter = json?.summary?.filter;
  // 旧プロキシは requestFilter を転送しない → Yahoo!は全件を返し filter='all' になる。
  // ここで止めないと「全問い合わせ数」を「対応漏れ数」と誤読する。
  // ⚠️実測 (2026-08-12 初回実機): Yahoo!は summary.filter を大文字 ('UNANSWERED') で
  //   echo する。要求値と大文字小文字を無視して比較する
  if (String(filter ?? '').toLowerCase() !== q.requestFilter) {
    throw contractError(`summary.filter='${filter}' が要求 '${q.requestFilter}' と一致しません`
      + ` (VPSプロキシが絞り込みパラメータ未対応の可能性。${label})`);
  }
  const count = json?.summary?.topic?.count;
  if (!Number.isSafeInteger(count) || count < 0) {
    throw contractError(`summary.topic.count が読み取れません (${label}: ${JSON.stringify(count)})`);
  }
  const headlines = json?.headlines;
  if (!Array.isArray(headlines)) {
    throw contractError(`headlines が配列でありません (${label})`);
  }
  // count と明細数の整合性 (Codexレビュー High): start 位置と result=20 の要求に対して
  // 明細数は必ず min(count - (start-1), 20) になるはず。count=0 なのに明細がある /
  // count>0 なのに明細が無い ようなレスポンスをそのまま通すと、該当があるのに
  // 「0件=無通知」で正常終了してしまう。矛盾はエラーにする (0件と読み取り不能を混同しない)
  const expected = Math.max(0, Math.min(count - (start - 1), PAGE_SIZE));
  if (headlines.length !== expected) {
    throw contractError(`count=${count} start=${start} に対して明細が ${headlines.length}件 (期待 ${expected}件) です (${label})`);
  }
  for (const h of headlines) {
    if (!h?.topicId) throw contractError(`topicId のない行があります (${label})`);
    if (typeof h.isNoAnswer !== 'boolean' || typeof h.isCompleted !== 'boolean') {
      throw contractError(`isNoAnswer/isCompleted が真偽値でありません (${label}: ${h.topicId})`);
    }
    // uncompleted は sellerPostTime で窓判定・ページ打ち切りをするため、欠落・不正は
    // 「読めない」としてエラーにする (0扱いにすると走査が即打ち切られ、取りこぼしを
    // 「成功」でコミットしてしまう)
    if (q.kind === 'uncompleted') {
      const sec = Number(h.sellerPostTime);
      if (!Number.isFinite(sec) || sec <= 0) {
        throw contractError(`sellerPostTime が不正です (${label}: ${h.topicId} = ${JSON.stringify(h.sellerPostTime)})`);
      }
    }
    // フィルタ語義の検証: unanswered に返信済みが混ざる / answered に未返信・完了済みが
    // 混ざるなら、summary.topic.count を「その分類の件数」として信用できない
    if (q.kind === 'unanswered' && h.isNoAnswer !== true) {
      throw contractError(`unanswered の結果に返信済み (isNoAnswer=false) が混ざっています (${label}: ${h.topicId})`);
    }
    if (q.kind === 'uncompleted' && (h.isNoAnswer !== false || h.isCompleted !== false)) {
      throw contractError(`answered の結果に想定外の状態が混ざっています`
        + ` (${label}: ${h.topicId} isNoAnswer=${h.isNoAnswer} isCompleted=${h.isCompleted})`);
    }
    // serviceType が返ってくる場合は要求と一致すること (無指定転送の検出)
    if (h.serviceType != null && h.serviceType !== q.serviceType) {
      throw contractError(`serviceType='${h.serviceType}' が要求 '${q.serviceType}' と一致しません (${label}: ${h.topicId})`);
    }
  }
  return { count, headlines };
}

/** 一覧行 → 通知に載せる1件 (タイトルと日時のみ。個人情報は持たない) */
export function toItem(h, q) {
  return {
    topicId: String(h.topicId),
    serviceType: q.serviceType,
    title: String(h.title || '(タイトルなし)'),
    userPostTime: Number(h.userPostTime) || null,
    sellerPostTime: h.sellerPostTime == null ? null : Number(h.sellerPostTime) || null,
    sortTime: Number(h[q.sort]) || 0,
  };
}

/**
 * 4照会 (shp/auc × unanswered/answered) を実行して分類する。
 *
 * ② 完了処理待ちには**直近 windowDays 日の窓**を設ける (実測 2026-08-12: 過去分の
 * 完了処理漏れが1,036件あり、全件を毎朝通知すると新しい要対応が埋もれるため)。
 * 窓判定は sellerPostTime (店舗の最終返信) 基準。窓外の件数は olderCount として返し、
 * 通知には件数だけ添える。窓内が必要な分だけページを読み進める (sellerPostTime 降順)。
 * ① 未返信は何日前のものでも要対応なので窓を設けない (件数は summary から取得)。
 *
 * @param {{fetchImpl?:Function, sleepMs?:number, timeoutMs?:number, env?:object,
 *          windowDays?:number, now?:Date, maxPages?:number}} opts
 * @returns {Promise<{
 *   unanswered:{count:number, items:object[]},
 *   uncompleted:{count:number, items:object[], olderCount:number, truncated:boolean},
 *   windowDays:number}>}
 *   unanswered.count = Yahoo!側の総件数 / uncompleted.count = 窓内の件数
 *   items = 新しい順の明細 (unanswered は最大20件/照会)
 */
export async function fetchInquiryStatus(opts = {}) {
  const fetchImpl = opts.fetchImpl ?? fetch;
  const sleepMs = opts.sleepMs ?? SLEEP_MS;
  const timeoutMs = opts.timeoutMs ?? 45000;
  const windowDays = opts.windowDays ?? DEFAULT_WINDOW_DAYS;
  const maxPages = opts.maxPages ?? MAX_PAGES_PER_QUERY;
  const now = opts.now ?? new Date();
  // 入力の検証 (Codexレビュー3巡目 Medium): windowDays=NaN → floorSec=NaN → 全件窓内、
  // maxPages=NaN → 無制限走査 のような静かな誤動作を入口で止める
  if (!Number.isInteger(windowDays) || windowDays < 1) throw new Error(`windowDays が不正です (${windowDays})`);
  if (!Number.isInteger(maxPages) || maxPages < 1) throw new Error(`maxPages が不正です (${maxPages})`);
  if (!(now instanceof Date) || Number.isNaN(now.getTime())) throw new Error('now が不正な Date です');
  const floorSec = Math.floor(now.getTime() / 1000) - windowDays * 86400;
  const { base, secret } = proxyConfig(opts.env ?? process.env);

  const out = {
    unanswered: { count: 0, items: [] },
    uncompleted: { count: 0, items: [], olderCount: 0, truncated: false },
    windowDays,
    maxPages,
  };
  const seen = new Map(); // topicId → query label (serviceType が効いていない事故の検出)
  let requests = 0;

  async function fetchPage(q, start) {
    if (requests > 0) await new Promise(r => setTimeout(r, sleepMs));
    requests++;

    const u = new URL(`${base}/yahoo/externalTalkList`);
    u.searchParams.set('start', String(start));
    u.searchParams.set('result', String(PAGE_SIZE));
    u.searchParams.set('requestFilter', q.requestFilter);
    u.searchParams.set('serviceType', q.serviceType);
    u.searchParams.set('sort', q.sort);
    u.searchParams.set('sortOrder', 'desc');

    const label = `${q.requestFilter}/${q.serviceType}`;
    let res;
    try {
      res = await fetchImpl(u.toString(), {
        headers: { 'X-Proxy-Secret': secret },
        signal: AbortSignal.timeout(timeoutMs),
      });
    } catch (err) {
      const timedOut = err?.name === 'TimeoutError' || err?.name === 'AbortError';
      throw new Error(timedOut
        ? `Yahooプロキシ タイムアウト (${timeoutMs}ms, ${label})`
        : `Yahooプロキシ接続失敗 (${label}): ${err?.message || err}`);
    }
    const text = await res.text();
    if (res.status !== 200) {
      // Yahoo!のエラーはXML <Error><Code/>。コードだけ抜く (問い合わせ本文の露出防止)
      const code = (text.match(/<Code>([^<]{1,40})<\/Code>/) || [])[1];
      throw new Error(`Yahoo API HTTP ${res.status} (${label})${code ? ` code=${code}` : ''}`);
    }
    let json;
    try { json = JSON.parse(text); } catch {
      throw contractError(`Yahoo APIレスポンスがJSONでありません (${label}): ${text.length} bytes`);
    }

    const { count, headlines } = validateListResponse(json, q, start);
    for (const h of headlines) {
      // String() で正規化してから照合 (数値/文字列の揺れで重複検出をすり抜けないように)
      const id = String(h.topicId);
      const prev = seen.get(id);
      if (prev) {
        throw contractError(`同じ topicId が複数の照会に現れました (${prev} と ${label}: ${id})`
          + ' — serviceType の絞り込みが効いていない可能性');
      }
      seen.set(id, label);
    }
    return { count, headlines };
  }

  for (const q of QUERIES) {
    const label = `${q.requestFilter}/${q.serviceType}`;

    if (q.kind === 'unanswered') {
      // 窓なし: 件数は summary から、明細は最新20件で足りる (表示は MAX_SHOW 件)
      const { count, headlines } = await fetchPage(q, 1);
      out.unanswered.count += count;
      out.unanswered.items.push(...headlines.map(h => toItem(h, q)));
      console.log(`[yahoo-inquiry] ${label}: ${count}件`);
      continue;
    }

    // uncompleted: sellerPostTime 降順で読み進め、窓 (floorSec) より古くなったら打ち切る
    let start = 1;
    let total = null;   // 1ページ目の count を基準に固定 (走査中の変動は fail-closed)
    let inWindow = 0;
    let truncated = false;
    for (let page = 1; ; page++) {
      if (page > maxPages) {
        // ここまで読んだ分だけで通知する (打ち切りは通知文で明示する。黙って切らない)
        truncated = true;
        break;
      }
      const { count, headlines } = await fetchPage(q, start);
      // ページ間で総件数が動いたら、どのページも信用できない (走査中に問い合わせが
      // 増減した or レスポンス不正)。翌 retry でやり直す (Codexレビュー3巡目)
      if (total === null) total = count;
      else if (count !== total) {
        throw contractError(`走査中に総件数が変動しました (${label}: ${total} → ${count})。retry で取り直します`);
      }
      let reachedFloor = false;
      for (const h of headlines) {
        const item = toItem(h, q);
        if (item.sortTime < floorSec) { reachedFloor = true; break; }
        out.uncompleted.items.push(item);
        inWindow++;
      }
      if (reachedFloor || headlines.length === 0 || start + headlines.length > count) break;
      start += headlines.length;
    }
    out.uncompleted.count += inWindow;
    if (truncated) {
      // 打ち切り時は「残りが窓内か窓外か」が分からない。未読の窓内案件を
      // olderCount (過去分) に混ぜて過少表示しない (Codexレビュー3巡目 High)
      out.uncompleted.truncated = true;
    } else {
      out.uncompleted.olderCount += Math.max(0, (total ?? 0) - inWindow);
    }
    console.log(`[yahoo-inquiry] ${label}: 窓内(${windowDays}日) ${inWindow}件 / 全体 ${total ?? 0}件${truncated ? ' (上限打ち切り)' : ''}`);
  }

  // shp と auc を混ぜて新しい順に (通知は最新のものから載せる)
  out.unanswered.items.sort((a, b) => b.sortTime - a.sortTime);
  out.uncompleted.items.sort((a, b) => b.sortTime - a.sortTime);
  return out;
}

// ─── GChat 本文 ───

/**
 * 顧客入力 (タイトル) を通知へ載せる前の無害化 (Codexレビュー Medium)。
 * 改行・制御文字で通知のセクション偽装をされない / GChat のメンション記法
 * (<users/all> 等) を発動させない。装飾記法 (*) は見た目だけなので許容
 */
export function sanitizeForChat(s) {
  return String(s || '')
    .replace(/[\u0000-\u001f\u007f]+/g, ' ')            // 改行・タブ含む制御文字 → 空白
    .replace(/<(users|rooms)\//gi, '<\u200b$1/')       // メンション記法をゼロ幅スペースで分断
    .trim();
}

function truncate(s, n) {
  const str = sanitizeForChat(s);
  return str.length > n ? `${str.slice(0, n)}…` : str;
}

function itemLine(it, kind) {
  const tag = SERVICE_LABEL[it.serviceType] || it.serviceType;
  const received = fmtJstFromUnixSec(it.userPostTime) || '-';
  const times = kind === 'uncompleted' && it.sellerPostTime
    ? `受信 ${received} / 返信 ${fmtJstFromUnixSec(it.sellerPostTime)}`
    : `受信 ${received}`;
  return `・[${tag}] ${truncate(it.title, 40)} (${times})`;
}

/**
 * 通知本文を組み立てる。**未返信と窓内の完了処理待ちが両方0件なら null**
 * (該当ゼロは通知しない仕様 — 「通知が来た = 何か対応が必要」と分かるようにする。
 * 実行の生存確認は daily-sync 側が担う)。
 * 窓外の過去分 (olderCount) だけでは通知しない — 毎朝同じ件数を言い続けても
 * 読み手が麻痺するだけなので、要対応が出た通知に件数だけ添える。
 */
export function buildMessage({ unanswered, uncompleted, windowDays = DEFAULT_WINDOW_DAYS, maxPages = MAX_PAGES_PER_QUERY }) {
  const total = unanswered.count + uncompleted.count;
  if (total === 0) return null;

  const lines = [];
  lines.push(`⚠️ *Yahoo!問い合わせに対応が必要なものが${total}件あります*`);

  if (unanswered.count > 0) {
    lines.push('');
    lines.push(`🔴 未返信 (出店者回答待ち): *${unanswered.count}件*`);
    for (const it of unanswered.items.slice(0, MAX_SHOW)) lines.push(itemLine(it, 'unanswered'));
    if (unanswered.count > MAX_SHOW) lines.push(`  …他 ${unanswered.count - MAX_SHOW}件`);
  }

  if (uncompleted.count > 0) {
    lines.push('');
    lines.push(`🟡 返信済みだが「完了する」が押されていない (直近${windowDays}日): *${uncompleted.count}件*`);
    for (const it of uncompleted.items.slice(0, MAX_SHOW)) lines.push(itemLine(it, 'uncompleted'));
    if (uncompleted.count > MAX_SHOW) lines.push(`  …他 ${uncompleted.count - MAX_SHOW}件`);
  }

  if (uncompleted.truncated) {
    lines.push(`  ⚠️ 読み込み上限 (1照会あたり${maxPages * 20}件) で打ち切りました。実際はこれより多い可能性があります`);
  }
  if (uncompleted.olderCount > 0) {
    lines.push('');
    lines.push(`※ このほか ${windowDays}日より前の完了処理漏れが ${uncompleted.olderCount}件あります (過去分はまとめて完了処理を)`);
  }

  lines.push('');
  lines.push('ストアクリエイターPro「問い合わせ管理」で返信・完了処理をしてください');
  return lines.join('\n');
}
