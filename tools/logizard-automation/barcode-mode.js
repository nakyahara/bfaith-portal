/**
 * 入荷バーコード連携 (auto-barcode.js) の起動の決まり (マスタ正本切替 ③c-1b-3a・2026-09-28)
 *
 * 1. 夜の止め: JST 00:00〜01:30 は動かない (始めない・実行ボタンも押さない)。
 *    miniPC の毎日の商品マスタの取込 (00:15〜00:55) と同じ共通アカウントを使うため
 *    (同じ ID で 2 か所からログインするとセッションを追い出し合う)。専用アカウントは作らない (L-14 の見直し 9/28)。
 * 2. 動かすのは **① 新商品の取込 → ② バーコード情報の書き出し だけ** (切替の PR・L-23・③c-1b-3b v4 §7)。
 *    ③ 毎日の商品マスタの取込 (GAS の CSV) はこの道具から外した = 設定に依らない (fail-closed)。
 *    毎日の商品マスタは miniPC の自動 (00:20) が取り込み、自動が止まったときに人が取り込むのはポータルの画面の「手の取込」。
 *    GAS の ③ に戻すのはシステム全体を旧方式に戻すときだけ (台帳 lz-gas-rollback の固定の版を配る)。
 *    .env に前の設定 LOGIZARD_BC_DAILY が残っていても、①② だけ (値は見ない・消してよいと出す)。
 * 3. 引数は --dry だけ (打ち間違いで本番が動かないように、知らない引数は断る)。
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3 (v2 §2・§6 / 契約 v3 / L-11 / L-14 の見直し / L-23 / 2b-2 契約 v3 の切替の PR)
 */

export const NIGHT_BLOCK = Object.freeze({ fromMin: 0, toMin: 90 });   // JST 00:00 以上 01:30 未満

/** JST のその日の 0 時からの分 */
export function jstMinuteOfDay(now = new Date()) {
  const d = new Date(now.getTime() + 9 * 3600 * 1000);
  return d.getUTCHours() * 60 + d.getUTCMinutes();
}

export function inNightBlock(now = new Date()) {
  const m = jstMinuteOfDay(now);
  return m >= NIGHT_BLOCK.fromMin && m < NIGHT_BLOCK.toMin;
}

const hhmm = (now) => {
  const m = jstMinuteOfDay(now);
  return `${String(Math.floor(m / 60)).padStart(2, '0')}:${String(m % 60).padStart(2, '0')}`;
};

export function nightBlockMessage(now = new Date(), where = '') {
  return `${where ? `${where}: ` : ''}いま ${hhmm(now)} (JST)。00:00〜01:30 は動きません `
    + '(miniPC がロジザードの毎日の商品マスタを同じアカウントで扱う時間。同時にログインするとセッションを追い出し合う)。'
    + '01:30 を過ぎてからもう一度押してください。';
}

/** 夜の止めの中なら例外 (実行ボタンの直前・各ステップの前に呼ぶ) */
export function assertOutsideNightBlock(where, now = new Date()) {
  if (inNightBlock(now)) {
    const e = new Error(nightBlockMessage(now, where));
    e.nightBlock = true;
    throw e;
  }
}

// ── ボタンを押す持ち時間 (Codex #1518 R2) ──
// click() は押せるようになるまで待つ = 確かめた後に 00:00 を越えて押しうる。
// → 押すのは「次の 00:00 の CLICK_MARGIN_MS 前」まで。その持ち時間を click の timeout にする (過ぎたら押さずに止まる)。
export const CLICK_MARGIN_MS = 2000;
const DAY_MS = 24 * 3600 * 1000;

/** 次の夜の止め (JST 00:00) までの ms。止めの中 = 0 */
export function msUntilNightBlock(now = new Date()) {
  if (inNightBlock(now)) return 0;
  const msOfDay = ((now.getTime() + 9 * 3600 * 1000) % DAY_MS + DAY_MS) % DAY_MS;
  return DAY_MS - msOfDay;
}

/** 止めの中、または 00:00 の直前 CLICK_MARGIN_MS の内 = もう押さない */
export function nearNightBlock(now = new Date()) {
  return inNightBlock(now) || msUntilNightBlock(now) <= CLICK_MARGIN_MS;
}

export function nightError(where, now = new Date(), cause = '') {
  const near = !inNightBlock(now);
  const e = new Error(nightBlockMessage(now, where) + (near ? ` (00:00 の直前 ${CLICK_MARGIN_MS / 1000} 秒からは押さない)` : '') + (cause ? ` [${cause}]` : ''));
  e.nightBlock = true;
  return e;
}

/** ボタンを押す前に呼ぶ: もう押さない時刻なら例外・押してよければ click の timeout (ms) を返す */
export function clickBudgetMs(where, now = new Date(), maxMs = 30000) {
  if (nearNightBlock(now)) throw nightError(where, now);
  return Math.min(maxMs, msUntilNightBlock(now) - CLICK_MARGIN_MS);
}

/** 押している途中の失敗 (持ち時間切れなど) が夜の止めのせいなら、夜の止めの例外に置き換える */
export function asNightError(e, where, now = new Date()) {
  if (e && e.nightBlock) return e;
  if (nearNightBlock(now)) return nightError(where, now, String(e && e.message || e).split('\n')[0].slice(0, 120));
  return e;
}

const KNOWN_ARGS = new Set(['--dry']);

export const LABEL = '① 新商品の取込 → ② バーコード情報の書き出し (③ 毎日の商品マスタは miniPC の自動が取り込む)';

/**
 * 起動の形を決める (①② だけ。③ は設定に依らず無い)。
 * @returns {{ dry: boolean, label: string, notes: string[] }}  notes = 起動のときに出す注意 (止めない)
 */
export function resolveBarcodeMode({ env = process.env, argv = process.argv.slice(2) } = {}) {
  if (argv.includes('--only-daily')) {
    throw new Error('③ 毎日の商品マスタの取込はこの道具から外しました (切替済み・毎晩 miniPC の自動が取り込む)。自動が止まったときに手で取り込むのは、ポータルの画面の「手の取込」です。');
  }
  const unknown = argv.filter((a) => !KNOWN_ARGS.has(a));
  if (unknown.length) throw new Error(`知らない引数です: ${unknown.join(' ')} (使えるのは --dry だけ)`);
  const notes = [];
  // 前の設定が残っていても ①② だけ (値で ③ を戻せない = fail-closed。現場の ①② は止めない)
  if (Object.prototype.hasOwnProperty.call(env, 'LOGIZARD_BC_DAILY')) notes.push('.env の LOGIZARD_BC_DAILY はもう使いません (③ は切替で外した)。この行は消してかまいません。');
  return { dry: argv.includes('--dry'), label: LABEL, notes };
}
