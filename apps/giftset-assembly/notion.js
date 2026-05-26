/**
 * ギフトセット作業カードを Notion に作成 (Render から直接)
 *
 * 環境変数:
 *   NOTION_TOKEN          … Notion インテグレーションのシークレット
 *   GIFTSET_NOTION_DB_ID  … 「ギフトセット作業」DB の ID
 * どちらか未設定なら NOTION_NOT_CONFIGURED を投げる (fail-closed)。
 * ピッキングデータ生成は Notion 未設定でも動くよう、呼び出し側で分離している。
 */
import { manualSourceLabel } from './db.js';

const API_BASE = 'https://api.notion.com/v1';
const NOTION_VERSION = '2022-06-28';

function getConfig() {
  const token = process.env.NOTION_TOKEN;
  const dbId = process.env.GIFTSET_NOTION_DB_ID;
  if (!token || !dbId) {
    const e = new Error('Notion 連携が未設定です (NOTION_TOKEN / GIFTSET_NOTION_DB_ID)');
    e.code = 'NOTION_NOT_CONFIGURED';
    throw e;
  }
  return { token, dbId };
}

export function isNotionConfigured() {
  return !!(process.env.NOTION_TOKEN && process.env.GIFTSET_NOTION_DB_ID);
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const backoff = (a) => 500 * 2 ** (a - 1) + Math.floor(Math.random() * 200); // 指数バックオフ + jitter

async function notionCreatePage(body, token) {
  const MAX = 3; // 429 / 5xx / 瞬断は指数バックオフで再試行
  let lastErr;
  for (let attempt = 1; attempt <= MAX; attempt++) {
    let res;
    try {
      res = await fetch(`${API_BASE}/pages`, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`,
          'Notion-Version': NOTION_VERSION,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify(body),
        signal: AbortSignal.timeout(15000),
      });
    } catch (e) {
      lastErr = e;
      if (attempt < MAX) { await sleep(backoff(attempt)); continue; }
      const err = new Error(`Notion 接続エラー: ${e.message}`);
      err.code = 'NOTION_API_ERROR';
      throw err;
    }
    if (res.ok) {
      const data = await res.json().catch(() => ({}));
      if (data.object === 'error') {
        const err = new Error(`Notion API エラー: ${data.message || res.status}`);
        err.code = 'NOTION_API_ERROR'; err.status = res.status; throw err;
      }
      return data;
    }
    if ((res.status === 429 || res.status >= 500) && attempt < MAX) {
      const ra = Number(res.headers.get('retry-after'));
      await sleep(ra > 0 ? ra * 1000 : backoff(attempt));
      continue;
    }
    const data = await res.json().catch(() => ({}));
    const err = new Error(`Notion API エラー: ${data.message || res.status}`);
    err.code = 'NOTION_API_ERROR'; err.status = res.status; throw err;
  }
  const err = new Error(`Notion API エラー: ${lastErr?.message || 'unknown'}`);
  err.code = 'NOTION_API_ERROR'; throw err;
}

/**
 * 作業カードを1枚作成。
 * @param {object} args
 * @param {object} args.set        getGiftset() の戻り値 (giftset_name, photo_url, video_url, unit_price, components)
 * @param {number} args.qty        個数
 * @param {number|null} args.people 人数
 * @param {string|null} args.dueDate 納期 'YYYY-MM-DD'
 * @param {string} args.requestDate 依頼日 'YYYY-MM-DD'
 * @param {string|null} args.note  メモ
 * @returns {{ url: string, id: string }}
 */
export async function createWorkCard({ set, qty, people, dueDate, requestDate, note }) {
  const { token, dbId } = getConfig();

  // 名前: lot_size > 1 のときはロット数も併記して誤読を防ぐ。
  //   例) 'リラックスギフトセット（10個 = 2ロット）'
  //   この時点では本文側で qty % lotSize の整合チェックを後段で行うが、ここでも防御的に
  //   Math.floor を使わず整数除算 (qty が倍数前提) を信頼する形にする。
  const lotSizeForTitle = Number.isInteger(set.production_lot_size) && set.production_lot_size >= 1
    ? set.production_lot_size : 1;
  const titleText = (lotSizeForTitle > 1 && qty % lotSizeForTitle === 0)
    ? `${set.giftset_name}（${qty}個 = ${qty / lotSizeForTitle}ロット）`
    : `${set.giftset_name}（${qty}個）`;
  const props = {
    '名前': { title: [{ text: { content: titleText } }] },
    'ステータス': { select: { name: '未着手' } },
    '個数': { number: qty },
    '依頼日': { date: { start: requestDate } },
    'ギフトコード': { rich_text: [{ text: { content: set.giftset_code } }] },
  };
  if (Number.isInteger(people) && people > 0) props['人数'] = { number: people };
  if (dueDate) props['納期'] = { date: { start: dueDate } };
  if (set.unit_price != null) props['単価'] = { number: set.unit_price };
  if (set.video_url) props['動画リンク'] = { url: set.video_url };
  if (set.photo_url) {
    props['写真'] = { files: [{ type: 'external', name: '完成見本', external: { url: set.photo_url } }] };
  }
  if (note) props['メモ'] = { rich_text: [{ text: { content: String(note).slice(0, 1900) } }] };

  // ページ本文: 構成品リスト (1ロットあたり / 今回合計)。
  // production_lot_size > 1 のときは、構成品.数量 は「1ロット分」で登録されているため
  // 合計は 数量 × lots ( = qty / lot_size)。lot_size=1 (デフォルト) なら従来通り 数量 × qty。
  const lotSize = Number.isInteger(set.production_lot_size) && set.production_lot_size >= 1
    ? set.production_lot_size : 1;
  // 防御的チェック (Codex review R1 medium #3): 通常は router /api/issue で qty%lotSize=0 を保証。
  // 将来 createWorkCard を別経路から呼んだ場合の安全網。Math.floor で潜在化させない。
  if (qty % lotSize !== 0) {
    const e = new Error(
      `[notion.createWorkCard] qty=${qty} が production_lot_size=${lotSize} の倍数ではありません`
    );
    e.code = 'VALIDATION';
    throw e;
  }
  const lots = qty / lotSize;
  const unitLabel = lotSize > 1 ? `1ロット(${lotSize}個)あたり` : '1セットあたり';
  const heading = `構成品（${unitLabel} / 今回合計）`;
  const children = [
    { object: 'block', type: 'heading_3', heading_3: { rich_text: [{ text: { content: heading } }] } },
    ...set.components.map((c) => ({
      object: 'block', type: 'bulleted_list_item',
      bulleted_list_item: {
        rich_text: [{
          text: { content: `${c.商品名 || c.商品コード}（${c.商品コード}）× ${c.数量}　→ 合計 ${c.数量 * lots}` },
        }],
      },
    })),
  ];
  if (lotSize > 1) {
    children.unshift({
      object: 'block', type: 'callout',
      callout: {
        icon: { type: 'emoji', emoji: '📦' },
        rich_text: [{ text: { content: `1ロット = ${lotSize}個セット。今回 ${qty}個 = ${lots}ロット製造。` } }],
      },
    });
  }

  // マニュアル / 所要時間 / 難易度 をページ本文の冒頭に追加 (set に値が入っているときのみ)。
  // Notion DB に新プロパティを追加せずに済むよう、body の callout として表現する
  // (新プロパティ追加が前提だと、本番 DB の手作業セットアップが必要になり deploy が壊れる)。
  const manualBlocks = [];
  if (set.manual_url) {
    // 表示ラベル: db.js 中心定義 MANUAL_SOURCE_LABELS から取得。
    // enum 範囲外の値が来たら manualSourceLabel が 'マニュアル' に fallback する。
    const srcLabel = manualSourceLabel(set.manual_source);
    manualBlocks.push({
      object: 'block', type: 'callout',
      callout: {
        icon: { type: 'emoji', emoji: '📖' },
        // Notion API は rich_text を配列で受け取り、各要素ごとにテキスト/リンクを表現できる。
        // text に href を付けるとそのテキストがクリック可能リンクになる (Notion 仕様)。
        rich_text: [
          { text: { content: `製造マニュアル (${srcLabel}): ` } },
          { text: { content: set.manual_url, link: { url: set.manual_url } } },
        ],
      },
    });
  }
  const metaParts = [];
  if (Number.isInteger(set.estimated_duration_min) && set.estimated_duration_min > 0) {
    // 1ロット製造の目安時間。lots 倍したのが今回想定時間。
    const totalMin = set.estimated_duration_min * lots;
    metaParts.push(`⏱ 所要時間: 1ロット ${set.estimated_duration_min}分 × ${lots}ロット = ${totalMin}分`);
  }
  if (Number.isInteger(set.difficulty) && set.difficulty >= 1 && set.difficulty <= 5) {
    const stars = '★'.repeat(set.difficulty) + '☆'.repeat(5 - set.difficulty);
    metaParts.push(`🎯 難易度: ${stars} (${set.difficulty}/5)`);
  }
  if (metaParts.length > 0) {
    manualBlocks.push({
      object: 'block', type: 'paragraph',
      paragraph: { rich_text: [{ text: { content: metaParts.join('　') } }] },
    });
  }
  // 冒頭 (lot callout の前) に挿入。順序: 📖 manual → ⏱/🎯 meta → 📦 lot info → 構成品...
  for (let i = manualBlocks.length - 1; i >= 0; i--) {
    children.unshift(manualBlocks[i]);
  }

  const data = await notionCreatePage({
    parent: { database_id: dbId },
    properties: props,
    children,
  }, token);

  return { url: data.url, id: data.id };
}
