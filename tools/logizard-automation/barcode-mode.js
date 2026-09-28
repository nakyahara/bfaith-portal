/**
 * 入荷バーコード連携 (auto-barcode.js) の起動の決まり (マスタ正本切替 ③c-1b-3a・2026-09-28)
 *
 * 1. 夜の止め: JST 00:00〜01:30 は動かない (始めない・実行ボタンも押さない)。
 *    miniPC の毎日の商品マスタの取込 (00:15〜00:55) と同じ共通アカウントを使うため
 *    (同じ ID で 2 か所からログインするとセッションを追い出し合う)。専用アカウントは作らない (L-14 の見直し 9/28)。
 * 2. どこまで動かすか: .env の LOGIZARD_BC_DAILY
 *    - 無い / manual = 今までどおり ① 新商品の取込 → ② バーコード情報の書き出し → ③ 毎日の商品マスタの取込
 *    - auto = ①② だけ (③ の CSV を見ない・取り込まない。毎日の商品マスタは miniPC の自動が取り込む = 切替日から)
 * 3. 引数は --dry だけ (打ち間違いで本番が動かないように、知らない引数は断る)。
 *    戻し方の手の ③ (--only-daily) は ③c-1b-3b (まだ無い)。
 *
 * 設計 = AI_reference CompanyDB構想/10 §6.3 (v2 §2・§6 / 契約 v3 / L-11 / L-14 の見直し)
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

const KNOWN_ARGS = new Set(['--dry']);

/**
 * 起動の形を決める。
 * @returns {{ dry: boolean, daily: 'manual' | 'auto', import2: boolean, label: string }}
 */
export function resolveBarcodeMode({ env = process.env, argv = process.argv.slice(2) } = {}) {
  if (argv.includes('--only-daily')) {
    throw new Error('--only-daily (戻し方の手の ③) はまだありません (③c-1b-3b)。今の毎日の商品マスタの取込は、引数なしの起動の ③ です。');
  }
  const unknown = argv.filter((a) => !KNOWN_ARGS.has(a));
  if (unknown.length) throw new Error(`知らない引数です: ${unknown.join(' ')} (使えるのは --dry だけ)`);
  const raw = String(env.LOGIZARD_BC_DAILY ?? '').trim();
  let daily;
  if (raw === '' || raw === 'manual') daily = 'manual';
  else if (raw === 'auto') daily = 'auto';
  else throw new Error(`.env の LOGIZARD_BC_DAILY が不正です: "${raw}" (manual か auto。無ければ manual)`);
  const dry = argv.includes('--dry');
  return {
    dry,
    daily,
    import2: daily === 'manual',
    label: daily === 'manual'
      ? '① 新商品の取込 → ② バーコード情報の書き出し → ③ 毎日の商品マスタの取込'
      : '① 新商品の取込 → ② バーコード情報の書き出し (③ 毎日の商品マスタは miniPC の自動)',
  };
}
