/**
 * ポータルトップ (views/dashboard.ejs) に並べる中身を組み立てる。
 *
 * - アプリ一覧そのものは lib/portal-apps.js (registry)
 * - server.js の GET / と scripts/test-portal-top.mjs は、どちらも dashboardLocals() で
 *   テンプレートに渡す値を作る (テンプレートへの渡し忘れをテストで拾えるように)
 * - 絞り込みはブラウザ側 (public/js/portal-top.js)。カードの data-search はここで正規化しておく
 * - 権限の判定は従来どおり allowedApps だけ。ここは「見せ方」しか変えない
 */
import { categories, appGroups, apps, externalLinks, warehouseVariantDashboardApps } from './portal-apps.js';

export const SUMMARY_MAX = 30;      // 一行説明の上限 (文字数)
export const SEARCH_MIN_CARDS = 6;  // これ未満しか見えないユーザー (iPad の現場など) には検索窓と分類ナビを出さない

const STATUSES = new Set(['active', 'coming-soon', 'archived']);
const BADGE_LABELS = { ipad: 'iPad', phone: 'スマホ', readonly: '読み取り専用', admin: '管理者のみ' };

const catById = new Map(categories.map(c => [c.id, c]));
const groupById = new Map(appGroups.map(g => [g.id, g]));

/**
 * 検索用の正規化。全角半角 (NFKC)・大文字小文字・カタカナ/ひらがな・空白のゆれを吸収する。
 * ⚠ public/js/portal-top.js の normalize と同じ規則にすること (テストで突き合わせている)。
 */
export function normalizeForSearch(s) {
  return String(s ?? '').normalize('NFKC').toLowerCase()
    .replace(/[ァ-ヶ]/g, c => String.fromCharCode(c.charCodeAt(0) - 0x60))
    .replace(/\s+/g, '');
}

/**
 * registry の書き方の抜けを返す (空配列なら問題なし)。
 * server.js は起動時に警告として出すだけ (起動は止めない: 止めると全アプリが落ちる)。
 */
export function validateRegistry() {
  const problems = [];
  const seenIds = new Set();
  const iconOwner = new Map();

  const checkCommon = (kind, e) => {
    const label = `${kind} ${e.id || '(id なし)'}`;
    if (!e.id) problems.push(`${label}: id がない`);
    else if (seenIds.has(e.id)) problems.push(`${label}: id が重複している`);
    seenIds.add(e.id);
    if (!e.name) problems.push(`${label}: name がない`);
    if (!catById.has(e.category)) problems.push(`${label}: 知らない category "${e.category}"`);
    if (!e.summary) problems.push(`${label}: summary (一行説明) がない`);
    else if ([...e.summary].length > SUMMARY_MAX) problems.push(`${label}: summary が ${SUMMARY_MAX} 字を超えている`);
    if (!Array.isArray(e.keywords) || e.keywords.length === 0) problems.push(`${label}: keywords がない`);
    return label;
  };
  // トップに 1 枚のカードとして並ぶものどうしでアイコンを重ねない (グループ内のアプリは対象外)
  const checkIcon = (label, icon) => {
    if (!icon) { problems.push(`${label}: icon がない`); return; }
    if (iconOwner.has(icon)) problems.push(`${label}: icon ${icon} が ${iconOwner.get(icon)} と重なっている`);
    else iconOwner.set(icon, label);
  };

  for (const g of appGroups) checkIcon(checkCommon('group', g), g.icon);

  const memberCount = new Map();
  for (const a of apps) {
    const label = checkCommon('app', a);
    if (typeof a.path !== 'string' || !a.path.startsWith('/apps/')) problems.push(`${label}: path は /apps/ で始める`);
    if (!STATUSES.has(a.status)) problems.push(`${label}: 知らない status "${a.status}"`);
    for (const b of a.badges || []) if (!BADGE_LABELS[b]) problems.push(`${label}: 知らない badge "${b}"`);
    if (a.group) {
      const g = groupById.get(a.group.id);
      if (!g) problems.push(`${label}: 知らない group "${a.group.id}"`);
      else if (g.category !== a.category) problems.push(`${label}: group ${g.id} と category が違う`);
      if (!a.group.label) problems.push(`${label}: group.label (ボタン名) がない`);
      memberCount.set(a.group.id, (memberCount.get(a.group.id) || 0) + 1);
    } else {
      checkIcon(label, a.icon);
    }
  }
  for (const g of appGroups) {
    if ((memberCount.get(g.id) || 0) < 2) problems.push(`group ${g.id}: アプリが 2 本未満 (グループにする意味がない)`);
  }

  for (const x of externalLinks) {
    const label = checkCommon('external', x);
    if (typeof x.url !== 'string' || !x.url.startsWith('https://')) problems.push(`${label}: url は https:// で始める`);
    if (!STATUSES.has(x.status)) problems.push(`${label}: 知らない status "${x.status}"`);
    checkIcon(label, x.icon);
  }
  return problems;
}

function statusBadges(status) {
  if (status === 'coming-soon') return [{ kind: 'soon', label: '準備中' }];
  if (status === 'archived') return [{ kind: 'archived', label: 'しまった' }];
  return [];
}

function searchText(parts) {
  return normalizeForSearch(parts.filter(Boolean).join(' '));
}

function appCard(a) {
  const badges = [
    ...(a.badges || []).map(kind => ({ kind, label: BADGE_LABELS[kind] })),
    ...statusBadges(a.status),
  ];
  return {
    kind: 'app',
    id: a.id,
    name: a.name,
    icon: a.icon,
    summary: a.summary,
    href: a.path,
    external: false,
    badges,
    comingSoon: a.status === 'coming-soon',
    archived: a.status === 'archived',
    search: searchText([a.name, a.summary, ...(a.keywords || []), catById.get(a.category)?.name, ...badges.map(b => b.label)]),
  };
}

function externalCard(x) {
  const badges = [{ kind: 'external', label: '外部' }, ...statusBadges(x.status)];
  return {
    kind: 'external',
    id: x.id,
    name: x.name,
    icon: x.icon,
    summary: x.summary,
    href: x.url,
    external: true,
    badges,
    comingSoon: x.status === 'coming-soon',
    archived: x.status === 'archived',
    search: searchText([x.name, x.summary, ...(x.keywords || []), catById.get(x.category)?.name, ...badges.map(b => b.label)]),
  };
}

// group の search にはモール名を入れない。モール名での絞り込みはボタン側 (chips[].search) で当てる
// (「楽天 分析」→ モール別分析カードの「楽天」ボタンだけが光る)
function groupCard(g, members) {
  return {
    kind: 'group',
    id: g.id,
    name: g.name,
    icon: g.icon,
    summary: g.summary,
    href: null,
    external: false,
    badges: [],
    comingSoon: false,
    archived: false,
    search: searchText([g.name, g.summary, ...g.keywords, catById.get(g.category)?.name]),
    chips: members.map(m => ({
      id: m.id,
      label: m.group.label,
      title: m.name,
      href: m.path,
      search: searchText([m.group.label, m.name, m.summary, ...(m.keywords || [])]),
    })),
  };
}

/**
 * allowedApps と variant から、トップに並べる分類とカードを組み立てる。
 *   - allowedApps: '*' (管理者) か、見てよいアプリ id の配列
 *   - variant: 'render' (社内ポータル本体) | 'warehouse' (miniPC のマスタ登録専用)
 * しまった (archived) カードも返すが archived: true が付く (画面では検索のときだけ出す)。
 */
export function buildDashboard({ allowedApps, variant = 'render' }) {
  const isAll = allowedApps === '*';
  const allowed = new Set(Array.isArray(allowedApps) ? allowedApps : []);

  let visibleApps;
  let visibleExternals;
  if (variant === 'warehouse') {
    // 認可は既存ルートの requireAppAccess('warehouse') に揃える (表示と認可をズレさせない)
    visibleApps = warehouseVariantDashboardApps.filter(a => isAll || allowed.has(a.requiresAccess));
    visibleExternals = [];
  } else {
    visibleApps = apps.filter(a => isAll || allowed.has(a.id));
    visibleExternals = isAll ? externalLinks : [];
  }

  const sections = [];
  for (const cat of categories) {
    const cards = [];
    const doneGroups = new Set();
    for (const a of visibleApps) {
      if (a.category !== cat.id) continue;
      if (a.group && a.status !== 'archived') {
        if (doneGroups.has(a.group.id)) continue;
        doneGroups.add(a.group.id);
        const members = visibleApps.filter(m => m.group?.id === a.group.id && m.status !== 'archived');
        // 見られるのが 1 本だけならグループにせず普通のカードで出す
        cards.push(members.length >= 2 ? groupCard(groupById.get(a.group.id), members) : appCard(a));
        continue;
      }
      cards.push(appCard(a));
    }
    for (const x of visibleExternals) {
      if (x.category === cat.id) cards.push(externalCard(x));
    }
    if (cards.length === 0) continue;
    sections.push({ id: cat.id, name: cat.name, cards, visibleCount: cards.filter(c => !c.archived).length });
  }

  const totalCards = sections.reduce((n, s) => n + s.visibleCount, 0);
  return {
    sections,
    totalCards,
    showSearch: totalCards >= SEARCH_MIN_CARDS,
    hasArchived: sections.some(s => s.cards.some(c => c.archived)),
  };
}

/** GET / で dashboard.ejs に渡す値。session は express-session の req.session */
export function dashboardLocals({ session, variant }) {
  return {
    ...buildDashboard({ allowedApps: session.allowedApps, variant }),
    username: session.email,
    displayName: session.displayName,
    role: session.role,
  };
}
