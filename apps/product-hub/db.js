/**
 * product-hub (商品登録一元化) DB 初期化。
 *
 * 設計: 要件定義 = AI_reference『システム設計/商品登録一元化_要件定義_20260703.md』
 *   - warehouse-mirror.db 同居 (inventory-monthly と同じ Pattern)
 *   - 冪等マイグレーション: CREATE TABLE IF NOT EXISTS + PRAGMA table_info → ALTER
 *   - 金額は整数 (円)。REAL 禁止
 *   - draft_events は append-only の監査ログ
 *
 * ステータス (§3 → PR4 2026-08-24 で工程からの導出値に切替):
 *   draft(下書き) → ready_for_ai(生成待ち) → review(レビュー待ち) → approved(承認済み)
 *   → listed(楽天出品済み) → expanded(展開済み)。どこからでも on_hold / excluded へ退避可。
 *   値は lib/workflow-progress.js の recomputeDraftStatus が工程・モール状態から再計算して書く。
 *   手で遷移させるのは 保留/除外/再開 のみ (それ以外の手動遷移 API は廃止)。
 */
import { getMirrorDB } from '../warehouse-mirror/db.js';
import { syncDraftLinks } from '../product-links/sync.js';
import { fileViewUrl } from './lib/drive-link.js';

export const DRAFT_STATUSES = [
  'draft', 'ready_for_ai', 'review', 'approved', 'listed', 'expanded', 'on_hold', 'excluded',
];

/**
 * ドラフトの出自 (§8 「商品コード単位でどちらを正とするか」)。
 *   portal        … ポータル起点の新規商品。ポータルが正 → Notion カードを作成/同期する
 *   notion_import … Notion 既存カードの取り込み。**Notion が正** → ポータルから書き戻さない
 * 中原さん方針 (2026-07-25): 既存カードは Notion 側で運用、新商品はアプリ、検証用に一部だけ取り込む。
 */
/**
 * TOP画像の重要度 (どれぐらい力を入れて作るかの目安。2026-08-24 中原さん指定・Notion の選択肢を踏襲)。
 * value はそのまま DB (product_drafts.image_priority) に保存する。色は Notion のタグ風
 */
export const IMAGE_PRIORITIES = [
  { value: '仕入れ商品（重要度：激低_白抜）', bg: '#ffe4e0', fg: '#9f1239' },
  { value: '仕入商品（重要度：低）', bg: '#fdecec', fg: '#b91c1c' },
  { value: '仕入商品（重要度：高）', bg: '#fce7f3', fg: '#be185d' },
  { value: '取扱先限定商品（重要度：高）', bg: '#f3f0d1', fg: '#6d6413' },
  { value: '自社商品（重要度：高）', bg: '#dbeafe', fg: '#1d4ed8' },
];
export const IMAGE_PRIORITY_VALUES = new Set(IMAGE_PRIORITIES.map((p) => p.value));
// 「自社商品」の重要度は own_brand チェックと連動する (2026-08-24 中原さん要望)
export const OWN_BRAND_IMAGE_PRIORITY = '自社商品（重要度：高）';

/**
 * 画像制作の管理項目 (撮影・素材 / Canva / 依頼文 / 保留 …) を使える商品か (2026-08-31 中原さん)。
 *
 * もとは own_brand (自社商品) だけだったが、**取扱先限定商品 (栃木レザー等) でも
 * 撮影の設定や LP の重要度を決める**ので画面が使えないと困る、という指摘で広げた。
 * 判定は画面とサーバ (image-production / image-hold API) の両方で同じものを使う。
 */
export const IMAGE_PRODUCTION_PRIORITIES = [
  OWN_BRAND_IMAGE_PRIORITY,
  '取扱先限定商品（重要度：高）',
];
export function canUseImageProduction(draft) {
  if (!draft) return false;
  if (draft.own_brand === 1 || draft.own_brand === true) return true;
  return IMAGE_PRODUCTION_PRIORITIES.includes(String(draft.image_priority || '').trim());
}

export const DRAFT_SOURCES = ['portal', 'notion_import'];
export const SOURCE_PORTAL = 'portal';
export const SOURCE_NOTION_IMPORT = 'notion_import';

/**
 * Notion へ書き戻してよい行か。
 * **allow-list (fail-closed)**: portal 起点だけを許可する。
 * 「notion_import でなければ許可」の deny-list だと、source が未知の値や誤記
 * ('notion-import' 等) になった瞬間に書き戻しが通ってしまう (Codex R1 high-1)。
 * ALTER で足した列には CHECK を付けられないため、コード側を fail-closed にして担保する。
 */
export function canWriteToNotion(draft) {
  if (!draft || draft.source !== SOURCE_PORTAL) return false;
  // セット派生の仮コード (SET-xxx-01) でカードを作らせない (Codex R1 high 2026-08-23)。
  // 仮コードのカードが商品マスターに残ると、RYS など Notion を読む側が実在しない
  // 商品コードを掴む。本コードに差し替わってから作る
  if (draft.provisional_code === 1) return false;
  return true;
}

/** Notion 取り込み由来か (削除許可など「取り込みだけ」を対象にする判定用) */
export function isNotionImported(draft) {
  return !!draft && draft.source === SOURCE_NOTION_IMPORT;
}

export const STATUS_LABELS = {
  draft: '下書き',
  ready_for_ai: '生成待ち',
  review: 'レビュー待ち',
  approved: '承認済み',
  listed: '楽天出品済み',
  expanded: '展開済み',
  on_hold: '保留',
  excluded: '除外',
};

// AI 出力スロット (§4/§6: 自由文でなくスロット構造。店舗共通フッターは出品時にシステム結合)
export const AI_OUTPUT_KINDS = [
  'rakuten_title', 'yahoo_title', 'desc_catch', 'desc_features', 'desc_spec', 'desc_notes',
];

/**
 * AI 出力の文字数上限 (モールの物理制約。2026-08-28 夜間自動化に先立ちサーバ側で担保)。
 *   楽天タイトル 127 / Yahoo!タイトル 65 / キャッチコピー 30 (Yahoo!ヘッドライン兼用)
 * 説明文3欄は上限を置かない (モール別上限は出品時に別途検証)。
 * NG ワード検査は入れない — 自社 copy_lint.py と二重管理になる。ここは「超えたら出品できない長さ」だけ。
 */
export const AI_OUTPUT_LIMITS = { rakuten_title: 127, yahoo_title: 65, desc_catch: 30 };

/**
 * 文字数の数え方は **コードポイント数** (`[...s].length`)。copy_lint.py の `len()` と一致させる。
 * `String.length` (UTF-16 単位) だと絵文字・一部の漢字が 2 と数えられ、lint を通った文章が
 * サーバで弾かれる食い違いが出る。検証は cleanText 後の「保存される値」に対して行う。
 * @returns {{ok: true} | {ok: false, error: string, code: 'OUTPUT_TOO_LONG', kind: string, limit: number, actual: number}}
 */
export function validateAiOutputLength(kind, content) {
  const limit = AI_OUTPUT_LIMITS[kind];
  if (!limit || content == null) return { ok: true };
  const actual = [...String(content)].length;
  if (actual <= limit) return { ok: true };
  return {
    ok: false, code: 'OUTPUT_TOO_LONG', kind, limit, actual,
    error: `${kind} が ${limit} 文字を超えています (${actual} 文字)`,
  };
}

/**
 * AI ランナーが draft を「人の確認待ち」にする理由コード (許容値は固定。画面表示用ラベル付き)。
 * 未知のコードは受け付けない — UI が説明できない状態を作らない
 */
export const GENERATION_BLOCK_CODES = {
  PACK_COUNT_MISMATCH: '入数・容量が参照ページと食い違う',
  IDENTITY_UNVERIFIED: '商品の同一性を確認できない',
  SOURCE_UNREACHABLE: '参照ページを取得できない',
  FACTS_TOO_THIN: '裏取りできた事実が少なすぎる',
  OTHER: 'その他',
};
export const GENERATION_BLOCK_REASON_MAX = 1000;

/**
 * 「確認中」の理由 (2026-08-31 中原さん・スタッフ要望)。
 *
 * 工程を進められない原因が **情報待ち** のときに人が立てる印。工程 (列) は動かさず、
 * カードにラベルを出す。列を挟む案を採らなかったのは:
 *   - 本流は直列なので、間に工程を足すと保留でない商品も全部そこを通る
 *   - status の導出は組み込み 5 工程しか見ない (deriveDraftStatus) ので、
 *     新しい列にカードが居ても status は ready_for_ai のままで **夜間 AI が生成してしまう**
 *   - 情報待ちは基本情報入力の直後だけでなく、どの工程でも起きる (列は 1 箇所にしか置けない)
 * 既存の「画像制作の保留」(draft_image_production.workflow_state) と
 * 「AI が止めました」(generation_block_code) と同じ **列を変えずカードに出す** 方式に揃えた。
 *
 * 理由コードを固定にするのは、あとで「何を待って止まっているか」を数えるため
 * (自由記述だけだと集計できない)。補足は checking_note に書く。
 */
// short = 詳細画面のワンタップ用ボタンの文字 (長いと押しにくい)。label = カード・バナーの表示
export const CHECKING_REASONS = [
  { code: 'package_label', short: '裏面を確認', label: 'パッケージ裏面の確認待ち', hint: '自社商品の成分表示・原材料など、実物を見ないと埋まらない項目がある' },
  { code: 'no_web_info', short: 'ウェブに情報なし', label: 'ウェブに情報が無い', hint: 'メーカー・仕入先へ問い合わせ中' },
  { code: 'share_info', short: '仕入れ担当と相談', label: '仕入れ担当と共有・相談中', hint: 'どういう商品なのかを仕入れ担当とすり合わせている' },
  { code: 'other', short: 'その他', label: 'その他', hint: '' },
];
export const CHECKING_REASON_CODES = new Set(CHECKING_REASONS.map((r) => r.code));
export const CHECKING_REASON_LABELS = Object.fromEntries(CHECKING_REASONS.map((r) => [r.code, r.label]));
export const CHECKING_NOTE_MAX = 300;

/** 担当者の区分 (表示順)。外注さんはポータルにログインしないので kind で見分ける */
export const STAFF_KINDS = [
  { value: 'internal', label: '社内' },
  { value: 'outsource', label: '外注' },
  { value: 'iroha', label: 'いろは' },
  { value: 'other', label: 'その他' },
];

/** かんばんのバッジ色。登録順に自動割り当て (管理画面で選び直せる) */
export const STAFF_COLORS = [
  '#2563eb', '#16a34a', '#d97706', '#7c3aed', '#db2777',
  '#0891b2', '#65a30d', '#dc2626', '#4f46e5', '#0d9488',
];

/**
 * 役割の初期値 (中原さん 2026-08-23 の工程定義そのまま)。
 * 人ではなく役割を工程に紐づけるので、担当者が変わっても工程定義は無変更で済む。
 * 担当者 (人) は**シードしない** — 中原さんが管理画面から登録する (2026-08-23 決定)。
 */
export const ROLE_SEEDS = [
  { code: 'registrar', label: '商品登録者', sort: 10 },
  { code: 'image', label: '画像登録者', sort: 20 },
  { code: 'image_approver', label: '画像作成承認者', sort: 25 },   // 2026-08-23 中原さん追加
  { code: 'approver', label: '商品承認者', sort: 30 },
  { code: 'set_planner', label: 'セット商品登録販売企画者', sort: 40 },
];

/**
 * 工程の初期値 (中原さん案 + 合意した修正)。
 *   - main  … かんばん上段。直列に進む本流
 *   - image … 画像トラック。基本情報が入った時点から**並行で**走る
 *             (本流に混ぜると外注の制作リードタイム分だけ全体が延びるため)
 * 「完了」は作業が無いので工程として持たず、かんばんの終端列として画面側で描く。
 * ここはあくまで初期値で、管理画面から表示名・担当ロール・並び順・滞留日数を変えられる。
 */
export const STEP_SEEDS = [
  {
    code: 'basic_info', label: '基本情報入力', track: 'main', role_code: 'registrar', sort: 10,
    description: '商品名・売価・JAN・公式/Amazon URL・バリエーション確認・カテゴリ/属性・ページ表記 (化粧品・食品)',
  },
  {
    code: 'ai_generate', label: 'AI情報入力待ち', track: 'main', role_code: null, sort: 20, stall_days: 3,
    description: 'タイトル・キャッチ・説明文3欄・仕様表を夜間バッチが生成する。担当者を置かない代わりに滞留を警告する',
  },
  {
    code: 'desc_review', label: '商品説明確認', track: 'main', role_code: 'registrar', sort: 30,
    description: 'AI が書いた説明文をレビュー・修正する',
  },
  {
    code: 'title_approve', label: 'タイトル確認', track: 'main', role_code: 'approver', sort: 40,
    description: '出品してよいかの承認ゲート',
  },
  {
    code: 'set_review', label: 'セット商品作成検討', track: 'main', role_code: 'set_planner', sort: 50,
    description: 'セットを作るか判断する。作る場合も派生ドラフトを別に立てるので単品の出品は止めない',
  },
  {
    code: 'listing', label: '出品・展開', track: 'main', role_code: null, sort: 60,
    description: '楽天 / Yahoo / auPAY / メルカリ / Qoo10 をモールごとに進める (担当はモール別に持つ)',
  },
  // 画像トラックは TOP画像 (サムネイル) と商品詳細画像で**別々に**進む (2026-08-24 中原さん:
  // 依頼・制作・承認のタイミングも担当も分かれる。単純な仕入れ商品は詳細画像を作らないこともある)。
  // 工程を kind ごとに複製する方式にしたのは、既存の権限・楽観ロック・イベント記録が
  // 「1 工程 = 1 行」の前提でそのまま使えるため。ラベルは共通にし、画面側で TOP/詳細 の
  // バッジを付けて区別する (ボードでは同じラベル同士が 1 列にまとまる)
  // image_stage はボードで TOP/詳細 の同じ段階を 1 列にまとめるための**安定キー**。
  // ラベルや sort でまとめると、管理画面で片方だけ改名・並べ替えした瞬間に列が分裂する (Codex設計相談)
  // 撮影依頼中 (2026-08-25 中原さん要望)。**商品詳細画像だけ**の段階 — 外部カメラマンへの
  // 撮影依頼は商品詳細画像のみの運用のため、TOP側には作らない (ボードの列には詳細カードだけが載る)。
  // 撮影しない商品 (メーカー素材を使う等) は工程パネルで「対象外」にするか、ボードで先の列へ D&D
  // TOP画像の 4 工程 (img_*_top) は 2026-08-31 に廃止 (RETIRED_TOP_STEP_CODES)。
  // 画像の工程は商品詳細 (LP) の 10 段階に一本化し、TOP は「画像が登録されているか」で見る
  // ── 商品詳細画像 v2 (2026-08-26 現場要望・中原さん決定。要件定義 = AI_reference『画像工程v2_要件定義_20260826.md』) ──
  // 自社商品の詳細画像の流れ ①〜⑨ (⑩完了 = ボードの終端列)。TOP 画像は全商品で作るので TOP 系列は上のまま。
  // 自社商品の TOP は ⑤デザイン修正で作る → ⑤ done で TOP 系列の依頼/制作/登録、⑥中原確認 done で TOP 承認が自動 done
  // (workflow-progress.js setStepState の追随)。skippable=0 の工程は「対象外」にできない。listing_gate=0 の工程
  // (⑦⑧⑨) は楽天出品の前提にしない (⑧は楽天出品そのもの = 出品成功で自動完了)。
  // 旧詳細 5 工程 (img_shoot_detail / img_*_detail) は LEGACY_DETAIL_V1_CODES で active=0 にし、進捗は migrateDetailTrackV2 で写す
  {
    code: 'imgd_request', label: '画像制作の依頼', track: 'image', image_kind: 'detail', image_stage: 'request',
    role_code: 'image', sort: 10, skippable: 0, listing_gate: 1,
    description: '① 新商品が自動で入る。撮影要否を判断して「撮影・素材」を設定し、商品情報 (Amazon やパッケージを見て手入力) を入れる',
  },
  {
    code: 'imgd_compose', label: '構成', track: 'image', image_kind: 'detail', image_stage: 'compose',
    role_code: 'image', sort: 20, skippable: 0, listing_gate: 1,
    description: '② 商品画像の構成を作る',
  },
  {
    code: 'imgd_material', label: '素材待ち', track: 'image', image_kind: 'detail', image_stage: 'material',
    role_code: 'image', sort: 30, stall_days: 7, skippable: 0, listing_gate: 1,
    description: '③ 撮影・社内準備した素材が揃うのを待つ (「撮影・素材」が 素材完了 か 撮影不要 で完了できる)',
  },
  {
    code: 'imgd_ai', label: 'AI制作', track: 'image', image_kind: 'detail', image_stage: 'ai',
    role_code: 'image', sort: 40, skippable: 0, listing_gate: 1,
    description: '④ 構成 + 素材から AI 画像を制作',
  },
  {
    code: 'imgd_design', label: 'デザイン修正', track: 'image', image_kind: 'detail', image_stage: 'design',
    role_code: 'image', sort: 50, stall_days: 7, skippable: 0, listing_gate: 1,
    description: '⑤ AI 画像を修正 + TOP画像制作 (TOP と LP は同時進行で作る)',
  },
  {
    code: 'imgd_review_1', label: '社内確認 (田中)', track: 'image', image_kind: 'detail', image_stage: 'review',
    role_code: 'image', sort: 60, stall_days: 3, skippable: 0, listing_gate: 1,
    description: '⑥-1 画像の責任者の確認。カードの「確認者」はここが未完了なら田中',
  },
  {
    code: 'imgd_review_2', label: '社内確認 (中原)', track: 'image', image_kind: 'detail', image_stage: 'review',
    role_code: 'image_approver', sort: 61, stall_days: 3, skippable: 0, listing_gate: 1,
    description: '⑥-2 最終確認 (田中確認の後にしか完了できない)。完了で楽天出品ゲートが開く',
  },
  {
    code: 'imgd_amazon', label: 'Amazon登録依頼', track: 'image', image_kind: 'detail', image_stage: 'amazon',
    role_code: 'image', sort: 70, stall_days: 7, skippable: 1, listing_gate: 0,
    description: '⑦ 大畑さんへ登録依頼 + 最終デザイン確認。Amazon に出さない商品は「対象外」',
  },
  {
    code: 'imgd_rakuten', label: '楽天登録', track: 'image', image_kind: 'detail', image_stage: 'rakuten',
    role_code: null, sort: 80, skippable: 1, listing_gate: 0,
    description: '⑧ このアプリから楽天に出品すると自動で完了。他モールは本流「出品・展開」のモール別状況で見る',
  },
  {
    code: 'imgd_aplus', label: 'A+登録', track: 'image', image_kind: 'detail', image_stage: 'aplus',
    role_code: 'image', sort: 90, stall_days: 14, skippable: 1, listing_gate: 0,
    description: '⑨ Amazon A+ コンテンツ登録。作らない商品は「対象外」',
  },
];

/** 詳細系列 v1 (2026-08-24〜26) の工程コード。v2 で active=0 (進捗行は残置。migrateDetailTrackV2 が v2 へ写す) */
export const LEGACY_DETAIL_V1_CODES = ['img_shoot_detail', 'img_request_detail', 'img_production_detail', 'img_register_detail', 'img_approve_detail'];
/** 詳細系列 v2 の工程コード (順序どおり) */
export const DETAIL_V2_CODES = ['imgd_request', 'imgd_compose', 'imgd_material', 'imgd_ai', 'imgd_design', 'imgd_review_1', 'imgd_review_2', 'imgd_amazon', 'imgd_rakuten', 'imgd_aplus'];
/** 撮影・素材ステータス (2026-08-26 現場要望の 6 値。DB には安定コードで保存) */
export const MATERIAL_STATUSES = [
  { code: 'not_required', label: '撮影不要' },
  { code: 'not_shipped', label: '商品未発送' },
  { code: 'shipped', label: '商品発送済み' },
  { code: 'shooting', label: '撮影中' },
  { code: 'internal_prep', label: '社内準備' },
  { code: 'ready', label: '素材完了' },
];
export const MATERIAL_STATUS_CODES = new Set(MATERIAL_STATUSES.map((m) => m.code));
export const MATERIAL_STATUS_LABELS = Object.fromEntries(MATERIAL_STATUSES.map((m) => [m.code, m.label]));
/** 旧 Notion 5 値 (shipping_status) → material_status の写像 */
export const SHIPPING_TO_MATERIAL = {
  '撮影依頼不要': 'not_required', '撮影商品未発送': 'not_shipped', '撮影商品発送手配済み': 'shipped',
  '社内準備': 'internal_prep', '社内画質上げる': 'internal_prep',
};
export const IMAGE_TRACK_V2_KEY = 'image_track_v2_migrated_at';

/** kind 分割前の画像工程コード (2026-08-24 移行で active=0 にする。進捗行は残置) */
export const LEGACY_IMAGE_STEP_CODES = ['img_request', 'img_production', 'img_register', 'img_approve'];

/**
 * TOP画像の工程 (2026-08-31 中原さん決定で廃止)。
 *
 * 「LP と TOP画像は基本的に同時進行で制作するため、工程を分けて管理する必要性が低い」
 * 「商品詳細画像と TOP画像が別工程になっているため、実際の制作件数がぱっと見で分かりにくい」
 * → 画像の工程は **商品詳細 (LP) の 10 段階に一本化**し、カードは 1 商品 1 枚にする。
 *
 * TOP画像そのものは楽天出品に必須なので、ゲートは工程ではなく
 * **画像が登録されているか**で見る (中原さんの選択 A。imageTrackBlockReason 参照)。
 * 進捗行は消さずに残す (履歴)。active=0 なので画面・ボードには出ない。
 */
export const RETIRED_TOP_STEP_CODES = ['img_request_top', 'img_production_top', 'img_register_top', 'img_approve_top'];

/** 画像トラックの種別。TOP は楽天出品に必須なので「対象外」にできない (workflow-progress 側で拒否) */
export const IMAGE_KINDS = [
  { code: 'top', label: 'TOP画像' },
  { code: 'detail', label: '商品詳細画像' },
];

let initialized = false;

/**
 * ne_code の正規化 UNIQUE index を張れたか。
 * 既存データに 'ABC'/'abc' の重複があると張れず、その状態では
 * 「代表コードにまとめる」の衝突検出 (LOWER(TRIM()) 照合) が DB 制約に守られない。
 * 起動は止めず (ポータル全体を巻き添えにしない)、**登録系だけ fail-closed** にする (Codex medium)。
 */
let neCodeUniqueEnforced = false;
export function isNeCodeUniqueEnforced() {
  return neCodeUniqueEnforced;
}

/**
 * draft_shop_categories に枠番 (slot) を導入する冪等マイグレーション (2026-08-02)。
 * RMS の「表示先カテゴリ」は 1〜5 の 5 枠 — 既存行はマスタの並び順で採番し、
 * 旧上限 (30) 時代に 6 件以上選んでいた draft は先頭 5 件だけ残す
 * (残すと保存 API と RMS 同期が全部 400 で詰む。Codex R1 high)。
 * 黙って消さず、何を外したかを draft_events に記録する。
 */
export function migrateShopCategorySlots(db) {
  const cols = new Set(db.prepare('PRAGMA table_info(draft_shop_categories)').all().map((c) => c.name));
  if (cols.has('slot')) return false;
  // 列追加〜採番〜切り詰めを単一トランザクションで (Codex R2 high)。
  // 別々に走らせると、列追加直後のクラッシュで「slot 列はあるが全行 slot=1・6件超も残存」の
  // 中途半端な状態になり、再実行判定 (列の有無) が二度と移行を走らせない。
  // SQLite は DDL もトランザクショナルなので、失敗すれば列追加ごとロールバックされる
  db.transaction(() => {
    db.exec('ALTER TABLE draft_shop_categories ADD COLUMN slot INTEGER NOT NULL DEFAULT 1');
    // 既存の選択はマスタの並び順を枠順とみなす (それ以外に順序の手がかりが無い)
    db.exec(`
      UPDATE draft_shop_categories AS d SET slot = (
        SELECT COUNT(*) FROM draft_shop_categories x
        JOIN ph_shop_categories cx ON cx.id = x.shop_category_id
        JOIN ph_shop_categories cd ON cd.id = d.shop_category_id
        WHERE x.draft_id = d.draft_id
          AND (cx.sort_order < cd.sort_order OR (cx.sort_order = cd.sort_order AND cx.id <= cd.id))
      )
    `);
    const overDrafts = db.prepare(
      'SELECT draft_id, COUNT(*) AS c FROM draft_shop_categories GROUP BY draft_id HAVING c > 5'
    ).all();
    for (const o of overDrafts) {
      const dropped = db.prepare(`
        SELECT c.path FROM draft_shop_categories s
        JOIN ph_shop_categories c ON c.id = s.shop_category_id
        WHERE s.draft_id = ? AND s.slot > 5 ORDER BY s.slot
      `).all(o.draft_id).map((r) => r.path);
      db.prepare('DELETE FROM draft_shop_categories WHERE draft_id = ? AND slot > 5').run(o.draft_id);
      db.prepare(`
        INSERT INTO draft_events (draft_id, event, detail, actor)
        VALUES (?, 'shop_categories_trimmed_to_5', ?, 'migration')
      `).run(o.draft_id, `RMSの5枠制限に合わせ ${dropped.length} 件を外しました: ${dropped.join(' ／ ').slice(0, 400)}`);
    }
  })();
  return true;
}

export function initProductHubDB() {
  if (initialized) return getMirrorDB();
  const db = getMirrorDB();

  // 注意: CHECK 制約は CREATE TABLE 時のみ有効。本アプリは PR #1 で新規テーブルとして
  // デプロイされるため既存 DB の retrofit は不要だが、将来 CHECK を変更する場合は
  // テーブル再作成型マイグレーション (swap) が必要 (Codex R2 low)。
  db.exec(`
    CREATE TABLE IF NOT EXISTS product_drafts (
      id                  INTEGER PRIMARY KEY AUTOINCREMENT,
      ne_code             TEXT NOT NULL UNIQUE,
      name                TEXT NOT NULL,
      status              TEXT NOT NULL DEFAULT 'draft'
                          CHECK (status IN ('draft','ready_for_ai','review','approved','listed','expanded','on_hold','excluded')),
      official_url        TEXT,
      price               INTEGER             -- 売価 (税込・円・整数)
                          CHECK (price IS NULL OR (price BETWEEN 0 AND 1000000000000)),
      jan_code            TEXT,
      asin                TEXT,
      amazon_url          TEXT,
      own_brand           INTEGER NOT NULL DEFAULT 0 CHECK (own_brand IN (0, 1)),
      has_variation       INTEGER NOT NULL DEFAULT 0 CHECK (has_variation IN (0, 1)),
      drive_folder_url    TEXT,               -- 商品画像フォルダ (商品コード_商品名 規則を推奨)
      memo                TEXT,
      notion_page_id      TEXT,
      notion_card_status  TEXT NOT NULL DEFAULT 'pending'
                          CHECK (notion_card_status IN ('pending','creating','created','failed')),
      notion_card_error   TEXT,
      notion_card_claim   TEXT,               -- creating 中の claim token (stale 奪取の二重作成防止)
      notion_card_attempts INTEGER NOT NULL DEFAULT 0,
      created_by          TEXT,
      created_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_product_drafts_status ON product_drafts(status);
    CREATE INDEX IF NOT EXISTS idx_product_drafts_notion ON product_drafts(notion_card_status);

    CREATE TABLE IF NOT EXISTS draft_reference_urls (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      url        TEXT NOT NULL,
      sort       INTEGER NOT NULL DEFAULT 0,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_draft_refs_draft ON draft_reference_urls(draft_id);

    CREATE TABLE IF NOT EXISTS draft_images (
      id               INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id         INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      drive_file_id    TEXT NOT NULL,
      drive_url        TEXT,
      sort             INTEGER NOT NULL DEFAULT 0,
      validation_error TEXT,
      created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      UNIQUE(draft_id, drive_file_id)
    );
    CREATE INDEX IF NOT EXISTS idx_draft_images_draft ON draft_images(draft_id);
    ${/* ボードは 1 回の表示で最大 800 商品ぶんの相関サブクエリを撃つ (先頭画像 id / 更新日時 /
         枠1 の有無)。draft_id 単独では ORDER BY sort, id LIMIT 1 を索引だけで解けない (Codex R1 low) */''}
    CREATE INDEX IF NOT EXISTS idx_draft_images_draft_sort_id ON draft_images(draft_id, sort, id);

    CREATE TABLE IF NOT EXISTS draft_specs (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      spec_key   TEXT NOT NULL,
      spec_value TEXT,
      sort       INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_draft_specs_draft ON draft_specs(draft_id);

    CREATE TABLE IF NOT EXISTS draft_ai_outputs (
      id              INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id        INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      kind            TEXT NOT NULL,
      content         TEXT,
      generated_at    TEXT,
      model_note      TEXT,
      edited_by_human INTEGER NOT NULL DEFAULT 0,
      UNIQUE(draft_id, kind)
    );

    -- Yahoo 向け追記項目 (要件定義 §12: RYS notion_overrides と同じ意味論。
    -- 将来 RYS の参照先を Notion → ここへアダプタ方式で切替するための受け皿)
    CREATE TABLE IF NOT EXISTS draft_yahoo (
      draft_id            INTEGER PRIMARY KEY REFERENCES product_drafts(id) ON DELETE CASCADE,
      yahoo_price         INTEGER CHECK (yahoo_price IS NULL OR (yahoo_price BETWEEN 0 AND 1000000000000)),
      yahoo_price_sagawa  INTEGER CHECK (yahoo_price_sagawa IS NULL OR (yahoo_price_sagawa BETWEEN 0 AND 1000000000000)),
      delivery_label      TEXT,
      tax_rate            TEXT,
      yahoo_category_id   INTEGER,
      yahoo_path          TEXT,
      updated_at          TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- 画像制作ワークフロー (要件定義 §13: Notion「商品ページ商品画像登録」の固有項目。自社商品のみ作成)
    CREATE TABLE IF NOT EXISTS draft_image_production (
      draft_id               INTEGER PRIMARY KEY REFERENCES product_drafts(id) ON DELETE CASCADE,
      status                 TEXT,
      importance_tier        TEXT,
      production_type        TEXT,
      aplus_content          TEXT,
      aplus_related          TEXT,
      camera_instruction_url TEXT,
      shipping_status        TEXT,
      reference_collection   TEXT,
      designer               TEXT,
      page_composer          TEXT,
      request_text           TEXT,
      updated_at             TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- バリエーションから外したSKU (2026-07-25 中原さん方針: 既定でまとめ、例外だけ外す)。
    -- NE の代表商品コードは概ね正しいが例外がある (代表コード '10' に無関係な訳アリ品50件など)。
    -- 外した SKU はこのドラフトのSKU一覧から消え、別ページにしたければそのコードで新規登録する
    -- (登録時の自動まとめは、ここに載っているコードではスキップされる)。
    -- 1 SKU は NE の代表商品コードを1つしか持たない = 属するグループ (ドラフト) も1つ。
    -- したがって除外も **SKU 単位でグローバルに一意**にする (Codex high-4)。
    -- draft 単位 UNIQUE だと、A から戻しても B の除外行が残って detached のままになる。
    -- 照合は LOWER(TRIM()) なので UNIQUE も式インデックスで揃える (生の UNIQUE では 'ABC'/' abc ' が別行になる)
    CREATE TABLE IF NOT EXISTS draft_variation_exclusions (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      ne_code    TEXT NOT NULL,
      actor      TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_draft_vari_excl_draft ON draft_variation_exclusions(draft_id);

    -- NE で「見たことがある商品コード」の記録 (2026-07-25: 新商品の自動取込)。
    -- ⚠️ これが無いと「mirror にあって product_drafts に無いコード」= 取扱中3,723件が全部
    --    新商品として流れ込む。初回実行では**全件をここに登録するだけでドラフトは作らず**、
    --    2回目以降に現れた未知コードだけを「今日以降の新商品」として扱う (カットオフ)。
    CREATE TABLE IF NOT EXISTS ph_ne_seen_codes (
      code_key      TEXT PRIMARY KEY,   -- LOWER(TRIM(商品コード))
      ne_code       TEXT NOT NULL,
      draft_id      INTEGER,            -- 取り込んだ先のドラフト (NULL = 初回シード)
      first_seen_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- 楽天出品 (P3、2026-07-26 権限smoke実証後に実装)。
    -- ジャンルID・商品属性は中原さん決定で手入力 (attributes_json = RMS 2.0 の
    -- variants[].attributes と同形 [{name, values:[..]}])。
    CREATE TABLE IF NOT EXISTS draft_rakuten (
      draft_id       INTEGER PRIMARY KEY REFERENCES product_drafts(id) ON DELETE CASCADE,
      genre_id       TEXT,
      attributes_json TEXT,                 -- [{name, values:[string]}]
      -- メーカー型番。RMS 画面では「商品仕様」の中の 1 項目 = API の attributes[メーカー型番]。
      -- 🚨 API の articleNumber (= RMS 画面の「カタログID」) ではない。2026-09-02 まで
      -- ここを articleNumber に送っていて IE0228 で登録が落ちていた (shaganshi)
      article_number TEXT,
      -- カタログID が無いときの理由 (1..6)。カタログID 本体は product_drafts.jan_code
      catalog_id_exemption_reason INTEGER CHECK (catalog_id_exemption_reason BETWEEN 1 AND 6),
      registered_at  TEXT,                  -- 楽天への登録に成功した日時 (2026-08-05〜 公開直行。それ以前は非公開登録)
      last_error     TEXT,                  -- 直近の RMS エラー (人が直す材料)
      -- 2026-07-27 仕様確定: 「アプリが正、RMS手直しは最終手段」— 公開に必要な情報をアプリで持つ
      shipping_method_group  TEXT,          -- variants[].shipping.shippingMethodGroup (店舗の配送方法ID '1'〜'9')
      postage_included       INTEGER,       -- variants[].shipping.postageIncluded (NULL=未設定 / 0=送料別 / 1=送料込み)
      normal_delivery_date_id TEXT,         -- variants[].normalDeliveryDateId (RMS 納期情報ID = リードタイム)
      white_bg_drive_file_id TEXT,          -- 白抜き背景画像 (whiteBgImage) の Drive fileId
      white_bg_drive_url     TEXT,
      published_at   TEXT,                  -- 公開になった日時 (2026-08-05〜 登録時に同時記録。NULL = 非公開)
      shop_categories_synced_at TEXT,       -- 店舗内カテゴリ (item-mappings) を RMS へ反映した日時
      shop_categories_synced_key TEXT,      -- そのとき反映した categoryId の並び (選択を変えたら「未反映」に戻すため)
      shop_categories_error     TEXT,       -- 直近の反映エラー (人が直す材料)
      updated_at     TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- R-Cabinet へ転送済みの画像 (draft_images の Drive 画像 → cabinet location)
    CREATE TABLE IF NOT EXISTS draft_cabinet_images (
      id             INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id       INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      drive_file_id  TEXT NOT NULL,
      cabinet_location TEXT NOT NULL,       -- item images[].location に入れる形 (/dir/file.jpg)
      cabinet_file_id  INTEGER,
      uploaded_at    TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      UNIQUE(draft_id, drive_file_id)
    );
    CREATE INDEX IF NOT EXISTS idx_draft_cabinet_draft ON draft_cabinet_images(draft_id);

    -- SKU画像 (バリエーションページで SKU 選択時に出る画像。2026-08-07 中原さん指示)。
    -- Drive フォルダに「SKUコード」名で置かれたファイルを取り込み、R-Cabinet 転送後に
    -- 楽天の variants[sku].images へ PATCH で紐づける (PATCH は per-SKU マージ = 実測済)
    CREATE TABLE IF NOT EXISTS draft_sku_images (
      draft_id       INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      sku_code       TEXT NOT NULL,          -- LOWER(TRIM()) した SKU 商品コード
      drive_file_id  TEXT NOT NULL,
      file_name      TEXT,
      cabinet_location TEXT,                 -- 転送後に /dir/file.jpg (未転送は NULL)
      cabinet_file_id  INTEGER,
      uploaded_at    TEXT,
      synced_at      TEXT,                   -- RMS の variants[sku].images へ反映した日時
      PRIMARY KEY (draft_id, sku_code)
    );

    -- バリエーションSKUごとの売価 (2026-08-24 中原さん要望)。
    -- NE 原価が SKU で異なる場合に売価も SKU 別に決める。原価が全SKU同じ商品は
    -- draft.price (ページ代表の売価) のままでよく、この表には行を持たない。
    -- P3.5 バリエーション出品の variants[sku].standardPrice の材料になる
    CREATE TABLE IF NOT EXISTS draft_sku_prices (
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      sku_code   TEXT NOT NULL,               -- LOWER(TRIM()) した SKU 商品コード
      price      INTEGER NOT NULL,            -- 売価 (税込・円)
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (draft_id, sku_code)
    );

    -- 楽天の店舗内カテゴリ (お店の棚) マスタ。RMS 画面からの貼り付けで取り込む
    -- (Category API での自動取得/自動紐付けは miniPC service-api にルート追加が必要 = 未実装)。
    -- 全置き換え取り込みでも行は消さず is_active で外す (draft_shop_categories が参照するため)
    CREATE TABLE IF NOT EXISTS ph_shop_categories (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      category_id  TEXT,                   -- RMS上のカテゴリID (貼り付けに含まれていた場合のみ。将来の自動紐付け用)
      path         TEXT NOT NULL,          -- 例: 犬用品 > おやつ > 無添加 (' > ' 区切りに正規化)
      path_key     TEXT NOT NULL UNIQUE,   -- LOWER(path)
      is_active    INTEGER NOT NULL DEFAULT 1 CHECK (is_active IN (0, 1)),
      sort_order   INTEGER NOT NULL DEFAULT 0,
      imported_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- ドラフトが載る店舗内カテゴリ (複数選択)。公開時に RMS 画面で設定する指示として使う
    -- SKU別の JANコード (2026-08-28 中原さん要望: バリエーションありのとき、
    -- ページ代表の JAN 1つだけでは SKU ごとの JAN を控えられない)。
    -- ページ代表の JAN は product_drafts.jan_code (楽天のカタログIDに使う) のまま。
    -- sku_code は LOWER(TRIM()) した SKU 商品コード (draft_sku_prices と同じ規則)
    CREATE TABLE IF NOT EXISTS draft_sku_jans (
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      sku_code   TEXT NOT NULL,
      jan_code   TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (draft_id, sku_code),
      -- 同じ JAN を同じページの2つの SKU に付けない (別商品なのに同一 JAN はモール側で弾かれる)。
      -- API 側でも事前に 409 を返すが、一括取込など別経路からも作らせないための制約
      UNIQUE (draft_id, jan_code)
    );

    -- SKU別の項目選択肢の値 (2026-09-02、カラバリ出品 P3.5)。
    -- 楽天のバリエーションページは variants[sku].selectorValues = {軸キー: 値} が必須で、
    -- 軸名 (「種類」「カラー」) は draft_rakuten.variant_selector_name に 1 つだけ持つ。
    -- 🚨 draft_sku_jans と分けている理由: あちらは jan_code NOT NULL なので、
    --    JAN の無い SKU (カタログIDなしの理由で出す商品) の選択肢値を持てない。
    -- NE 側にバリエーション軸の情報が無いため、値は画面で手入力する (2026-09-02 中原さん決定)
    CREATE TABLE IF NOT EXISTS draft_sku_selector_values (
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      sku_code   TEXT NOT NULL,          -- LOWER(TRIM()) した SKU 商品コード (draft_sku_jans と同じ規則)
      value      TEXT NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (draft_id, sku_code),
      -- 1 軸なので、同じ値を 2 つの SKU に付けると組み合わせが重複して RMS に弾かれる
      UNIQUE (draft_id, value)
    );

    CREATE TABLE IF NOT EXISTS draft_shop_categories (
      draft_id         INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      shop_category_id INTEGER NOT NULL REFERENCES ph_shop_categories(id),
      -- RMS の「表示先カテゴリ 1〜5」に対応する枠番 (2026-08-02)。
      -- 順序に意味がある: 枠1 = item-mappings の mainPluralCategoryId (メインページ)
      slot             INTEGER NOT NULL DEFAULT 1,
      created_at       TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (draft_id, shop_category_id)
    );

    -- 楽天ジャンル属性辞書のキャッシュ (Genre API、2026-07-28 プローブで実証)。
    -- ドラフト間で共有。buildItemPayload の事前検証 (IE1002防止・必須属性チェック) と
    -- 「辞書に カタログID があるジャンルだけ JAN を自動付与」の判定に使う。
    CREATE TABLE IF NOT EXISTS ph_genre_attributes (
      genre_id     TEXT PRIMARY KEY,
      genre_name   TEXT,                    -- 例: 付箋紙
      genre_path   TEXT,                    -- 例: 日用品雑貨… > 文房具… > 付箋紙
      payload_json TEXT NOT NULL,           -- 正規化済み attributes 配列 (JSON)
      fixed_at     TEXT,                    -- RMS 辞書の version.fixedAt
      fetched_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- 商品ページ表記 (化粧品・食品) — 旧「商品ページ詳細ページ作成.xlsm」の移植 (2026-07-29)。
    -- 楽天必須記載事項 (広告文責/メーカーor販売業者名/製造国/商品区分 — テキスト記載必須・画像化不可)
    -- + 食品表示系の項目。HTML は buildPageInfoHtml が xlsm と同じ表形式で生成し説明文に結合する
    CREATE TABLE IF NOT EXISTS draft_page_info (
      draft_id        INTEGER PRIMARY KEY REFERENCES product_drafts(id) ON DELETE CASCADE,
      product_type    TEXT NOT NULL DEFAULT 'general'
                      CHECK (product_type IN ('general','cosmetics','health_food','food')),
      brand_name      TEXT,               -- ブランド名 (2026-08-28 中原さん要望。掲載HTMLの先頭行)
      content_volume  TEXT,               -- 内容量・容量 (例: 50ml / 200g)
      size_text       TEXT,               -- サイズ (例: 縦5cm×横10cm×高さ15cm)
      ingredients     TEXT,               -- 成分/素材/材質 (化粧品=全成分、雑貨=素材)
      usage_notes     TEXT,               -- 使用上の注意
      origin_type     TEXT CHECK (origin_type IN (NULL, '日本製', '海外製')),
      origin_country  TEXT,               -- 原産国名 (海外製のとき。健康食品は必須)
      category_label  TEXT,               -- 商品分類区分 (化粧品/医薬部外品/健康食品/…)
      seller_name     TEXT,               -- 発売元 (メーカー名 or 販売業者名)
      importer_name   TEXT,               -- 輸入者名 (輸入品はメーカー名と両記載が楽天必須)
      food_name       TEXT,               -- 名称 (食品表示)
      food_ingredients TEXT,              -- 原材料名 (食品表示)
      food_expiry     TEXT,               -- 賞味期限 (例: 商品ラベルに記載)
      food_storage    TEXT,               -- 保存方法
      updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- NE の配送方法 ↔ 楽天の発送方法コード (配送方法グループ 1〜9) の紐付け (2026-07-29 中原さん指示)。
    -- 出品カードの配送方法デフォルトと、商品ページ表記の「発送方法」行に使う
    CREATE TABLE IF NOT EXISTS ph_shipping_method_map (
      ne_label        TEXT PRIMARY KEY,   -- NE の配送方法 (例: ネコポス)
      rakuten_group   TEXT,               -- 楽天 配送方法グループID '1'〜'9' (NULL=未割当)
      updated_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    -- ─── ワークフロー: 担当者 / 役割 / 工程 (2026-08-23 中原さん要件) ───────────
    -- 「ステータスごとに誰が何をするか」を見えるようにするための土台。
    -- **役割 (ロール) を人と工程の間に挟む**のが設計の肝 (中原さん指定):
    --   工程「基本情報入力」→ 役割「商品登録者」→ 人 (複数可・既定1人)
    -- 人が入れ替わっても工程定義を触らずに済み、担当者別の集計もできる。
    -- 旧実装は draft_image_production.designer / page_composer の自由入力
    -- (datalist 候補) だったため表記ゆれが起き、集計も絞り込みもできなかった。

    CREATE TABLE IF NOT EXISTS ph_staff (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      name         TEXT NOT NULL,                    -- 表示名 (例: 大川さん)
      kind         TEXT NOT NULL DEFAULT 'internal'
                   CHECK (kind IN ('internal','outsource','iroha','other')),
      portal_email TEXT,                             -- ポータルlog in との紐付け (任意・小文字で保存)
      color        TEXT,                             -- かんばんのバッジ色 (#rrggbb)
      note         TEXT,
      -- 退職・契約終了は物理削除でなく active=0。担当履歴を壊さないため (取り消せない操作を作らない)
      active       INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
      sort         INTEGER NOT NULL DEFAULT 0,
      created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      updated_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    CREATE TABLE IF NOT EXISTS ph_roles (
      code       TEXT PRIMARY KEY,                   -- registrar / image / approver / set_planner …
      label      TEXT NOT NULL,                      -- 商品登録者 / 画像登録者 …
      sort       INTEGER NOT NULL DEFAULT 0,
      -- builtin=1 は工程シードが参照するコード。改名はできるが無効化・削除はさせない
      builtin    INTEGER NOT NULL DEFAULT 0 CHECK (builtin IN (0, 1)),
      active     INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );

    CREATE TABLE IF NOT EXISTS ph_staff_roles (
      staff_id   INTEGER NOT NULL REFERENCES ph_staff(id) ON DELETE CASCADE,
      role_code  TEXT NOT NULL REFERENCES ph_roles(code) ON DELETE CASCADE,
      -- 既定担当 = 新しいドラフトの工程に自動で入る人。役割ごとに 1 人 (下の partial unique index)
      is_default INTEGER NOT NULL DEFAULT 0 CHECK (is_default IN (0, 1)),
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (staff_id, role_code)
    );
    CREATE INDEX IF NOT EXISTS idx_ph_staff_roles_role ON ph_staff_roles(role_code);

    CREATE TABLE IF NOT EXISTS ph_steps (
      code        TEXT PRIMARY KEY,                  -- basic_info / ai_generate / …
      label       TEXT NOT NULL,
      -- main = かんばん上段 (直列に進む本流) / image = 画像トラック (基本情報の後から並行で走る)。
      -- 画像を本流に混ぜると外注のリードタイム分だけ全体が延びるため分けている (中原さん合意 2026-08-23)
      track       TEXT NOT NULL DEFAULT 'main' CHECK (track IN ('main', 'image')),
      -- 画像トラックの種別 (2026-08-24 中原さん: TOP画像と商品詳細画像は別々に進む)。
      -- 工程を kind ごとに複製する方式 — 進捗テーブルの PK・権限・楽観ロックが「1工程=1行」のまま使える
      image_kind  TEXT CHECK (image_kind IN ('top', 'detail')),
      -- ボードで TOP/詳細 の同じ段階を 1 列にまとめる安定キー (ラベル・sort でまとめると改名で分裂する)
      image_stage TEXT,
      role_code   TEXT REFERENCES ph_roles(code),    -- NULL = システム工程 (担当者を置かない。例: AI待ち)
      sort        INTEGER NOT NULL DEFAULT 0,
      stall_days  INTEGER,                           -- この日数を超えて滞留したら警告 (NULL = 警告しない)
      description TEXT,
      builtin     INTEGER NOT NULL DEFAULT 0 CHECK (builtin IN (0, 1)),
      active      INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
      updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_ph_steps_track ON ph_steps(track, sort);

    -- 商品 × 工程の進捗。「いま誰のボールか」の実体。
    -- 行は表示時に自己修復で作る (ensureProgress) — 工程を後から足しても既存ドラフトに行き渡る。
    -- state: todo=未着手 / doing=作業中 / done=完了 / skip=この商品では不要
    CREATE TABLE IF NOT EXISTS draft_step_progress (
      draft_id    INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      step_code   TEXT NOT NULL REFERENCES ph_steps(code),
      state       TEXT NOT NULL DEFAULT 'todo' CHECK (state IN ('todo', 'doing', 'done', 'skip')),
      -- 担当者は工程の既定担当を初期値に入れ、商品ごとに差し替えられる (NULL = 未割り当て)
      assignee_id INTEGER REFERENCES ph_staff(id),
      due_date    TEXT,
      started_at  TEXT,
      done_at     TEXT,
      done_by     TEXT,                          -- 完了操作をした人 (ポータルのログイン)
      note        TEXT,
      -- 楽観ロック用の版数。updated_at (ミリ秒精度) をトークンにすると、同一ミリ秒内の
      -- 連続更新で値が変わらず、古い画面からの上書きを検知できない (Codex R3)
      version     INTEGER NOT NULL DEFAULT 0,
      updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (draft_id, step_code)
    );
    -- 列 (工程) 別・担当者別の絞り込みがかんばんの主クエリなので、両方に索引を張る
    CREATE INDEX IF NOT EXISTS idx_dsp_step ON draft_step_progress(step_code, state);
    CREATE INDEX IF NOT EXISTS idx_dsp_assignee ON draft_step_progress(assignee_id, state);

    -- 商品 × モールの展開状況 (2026-08-23 中原さん: 「出品・展開はモールごとにステータスを作る」)。
    -- 工程「出品・展開」の中身。全モールが done/skip になるとその工程が完了する。
    -- mall コードは lib/mall-status.js の MALLS が正 (CHECK は将来のモール追加を
    -- テーブル再作成なしでできるよう**あえて張らない**。値の検証はコード側)
    CREATE TABLE IF NOT EXISTS draft_mall_status (
      draft_id    INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      mall        TEXT NOT NULL,                  -- rakuten / yahoo / aupay / mercari / qoo10 / linegift
      state       TEXT NOT NULL DEFAULT 'todo' CHECK (state IN ('todo', 'doing', 'done', 'skip')),
      assignee_id INTEGER REFERENCES ph_staff(id),
      listed_at   TEXT,                           -- 掲載できた日時
      item_url    TEXT,                           -- 掲載ページ (確認用)
      note        TEXT,
      version     INTEGER NOT NULL DEFAULT 0,     -- 楽観ロック (工程と同じ方式)
      updated_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (draft_id, mall)
    );
    CREATE INDEX IF NOT EXISTS idx_dms_mall ON draft_mall_status(mall, state);
    CREATE INDEX IF NOT EXISTS idx_dms_assignee ON draft_mall_status(assignee_id, state);

    -- セット商品の構成 (2026-08-23)。工程「セット商品作成検討」で単品から派生させたとき、
    -- 「何を何個まとめたセットか」を持つ。AI に説明文を書かせるときの文脈にも使う
    -- (これが無いと単品のコピーみたいなタイトルになる)
    CREATE TABLE IF NOT EXISTS draft_set_members (
      set_draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      member_ne_code TEXT NOT NULL,
      qty            INTEGER NOT NULL DEFAULT 1 CHECK (qty BETWEEN 1 AND 999),
      sort           INTEGER NOT NULL DEFAULT 0,
      PRIMARY KEY (set_draft_id, member_ne_code)
    );

    -- 自動取込の状態 (シード完了の判定は seen 件数でなくここで行う — Codex critical:
    -- 一括登録も ph_ne_seen_codes に書くため、件数>0 を「シード済み」とすると
    -- シード前に手動登録1件しただけで既存3,723件が全部「新商品」扱いになる)
    CREATE TABLE IF NOT EXISTS ph_intake_state (
      key   TEXT PRIMARY KEY,
      value TEXT
    );

    -- かんばんの手動並び順 (2026-08-28 中原さん要望: 「動かしたカードは自由に順番を変えたい」)。
    -- 既定は「停滞日数の多い順 → 登録順」だが、それだと現場で決めた「今日はこの順でやる」が
    -- 保存されず、動かしても読み直すたびに元へ戻ってしまう。
    -- 1 枚のカード = (view, draft_id, kind) につき 1 行。col は最後に手で置いた列で、
    -- 工程が変わって別の列に出たときは手動順を捨てて既定順に戻す (別の列の並びを
    -- そのまま持ち込むと、置いた覚えのない位置に割り込む)。
    -- kind は画像ビューの種別 (top/detail)。本流ビューは '' (空文字) を使う。
    -- 現場の目安情報なのでロックも版数も持たない (最後に置いた人の並びが正)。
    CREATE TABLE IF NOT EXISTS ph_board_order (
      view       TEXT NOT NULL,                   -- main / image
      draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      kind       TEXT NOT NULL DEFAULT '',        -- image ビューのみ top / detail
      col        TEXT NOT NULL,                   -- 置いた列 (工程コード / 画像ステージ / done)
      sort       INTEGER NOT NULL,
      updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
      PRIMARY KEY (view, draft_id, kind)
    );
    CREATE INDEX IF NOT EXISTS idx_ph_board_order_col ON ph_board_order(view, col, sort);

    CREATE TABLE IF NOT EXISTS draft_events (
      id         INTEGER PRIMARY KEY AUTOINCREMENT,
      draft_id   INTEGER NOT NULL,
      event      TEXT NOT NULL,
      detail     TEXT,
      actor      TEXT,
      created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_draft_events_draft ON draft_events(draft_id);
  `);

  // 既存 DB へのカラム追加 (warehouse-mirror/db.js の addColumnIfMissing と同方針の冪等 ALTER)
  const draftCols = new Set(db.prepare('PRAGMA table_info(product_drafts)').all().map((c) => c.name));
  if (!draftCols.has('notion_card_claim')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN notion_card_claim TEXT');
  }
  // P1.5: 自社商品フラグ + Amazon 識別子 (要件定義 §13)
  if (!draftCols.has('asin')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN asin TEXT');
  }
  if (!draftCols.has('amazon_url')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN amazon_url TEXT');
  }
  if (!draftCols.has('own_brand')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN own_brand INTEGER NOT NULL DEFAULT 0 CHECK (own_brand IN (0, 1))');
  }
  // P2 (2026-08-03): AI生成の取り合い防止 (claim/lease)。
  // 定期実行と手動、あるいは前夜のハングした実行が同じ draft を二重生成しないため (Codex設計相談 Critical)
  if (!draftCols.has('generation_claim_run_id')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN generation_claim_run_id TEXT');
  }
  if (!draftCols.has('generation_claim_until')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN generation_claim_until TEXT');
  }
  // 夜間自動化 (2026-08-28): AI ランナーが「人が見ないと進められない」と判断した draft を
  // 人が解除するまで claim 対象から外す。status は ready_for_ai のまま (工程導出を壊さない・
  // ボードの「AI情報入力待ち」列に残して ⚠ を出す)。release と違い updated_at も進めるので、
  // 解除後は列の末尾に回る。4列は「全部 NULL」か「code/at/by が非NULL」のどちらかで揃える
  // (Codex設計相談 medium: 部分的な不整合を作らない — 書くのは blockGenerationDraft だけ)
  if (!draftCols.has('generation_block_code')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN generation_block_code TEXT');
  }
  if (!draftCols.has('generation_block_reason')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN generation_block_reason TEXT');
  }
  if (!draftCols.has('generation_blocked_at')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN generation_blocked_at TEXT');
  }
  if (!draftCols.has('generation_blocked_by')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN generation_blocked_by TEXT');
  }
  // 確認中 (2026-08-31): 人が「情報待ちで進められない」と立てる印。generation_block と同じく
  // 工程・status は動かさず、ボードのカードにラベルを出して AI 生成キューからだけ外す。
  // **確認中の判定は checking_since**。reason_code は since と必ず対で入る (書くのは
  // set/clearDraftChecking だけ)。note は任意、by はログイン情報が取れないとき NULL になりうる。
  // 解除は 4 列まとめて NULL に戻す — 「reason だけ残る」中途半端な状態を作らない (Codex R1)。
  //
  // 列追加と、AI キューの部分インデックスの作り直しを **1 つの immediate transaction** で行う
  // (Codex R2/R3 medium)。理由は 2 つ:
  //   - CREATE INDEX IF NOT EXISTS は**定義変更を反映しない**ので、旧定義 (checking_since 抜き) が
  //     残っている環境では DROP して作り直す必要がある
  //   - 上の draftCols のようにトランザクション外で「読んでから ALTER」すると、同じ DB を
  //     2 プロセスが同時に起動したとき両方が旧スキーマを見て duplicate column name で落ちる。
  //     BEGIN IMMEDIATE で最初から書きロックを取り、**中で存在確認をやり直す**ことで塞ぐ
  // 注: このファイルの他の ALTER 群は従来どおりトランザクション外。同じ性質を持つが、
  //     まとめて直すのは影響範囲が広いので別途 (ここで足す 4 列だけ先に安全側に倒す)
  const migrateChecking = db.transaction(() => {
    const cols = new Set(db.prepare('PRAGMA table_info(product_drafts)').all().map((c) => c.name));
    if (!cols.has('checking_reason_code')) {
      db.exec('ALTER TABLE product_drafts ADD COLUMN checking_reason_code TEXT');
    }
    if (!cols.has('checking_note')) {
      db.exec('ALTER TABLE product_drafts ADD COLUMN checking_note TEXT');
    }
    if (!cols.has('checking_since')) {
      db.exec('ALTER TABLE product_drafts ADD COLUMN checking_since TEXT');
    }
    if (!cols.has('checking_by')) {
      db.exec('ALTER TABLE product_drafts ADD COLUMN checking_by TEXT');
    }
    // claim の候補 SELECT 用の部分インデックス (キューが大きくなっても updated_at 順の先頭だけ読む)
    const genQueueIdx = db.prepare(`
      SELECT sql FROM sqlite_master WHERE type = 'index' AND name = 'idx_product_drafts_generation_queue'
    `).get();
    if (genQueueIdx && !String(genQueueIdx.sql || '').includes('checking_since')) {
      db.exec('DROP INDEX IF EXISTS idx_product_drafts_generation_queue');
    }
    db.exec(`CREATE INDEX IF NOT EXISTS idx_product_drafts_generation_queue
      ON product_drafts(updated_at)
      WHERE status = 'ready_for_ai' AND generation_block_code IS NULL AND checking_since IS NULL`);
  });
  migrateChecking.immediate();
  // Notion 取り込み (テスト検証用)。source='notion_import' の行は Notion 側が正であり、
  // ポータルから Notion へ書き戻してはならない (既存カードの破壊防止 — notion-card.js のガード参照)。
  //   注意: ALTER で足す列に CHECK は付けられない。そのため書き戻し判定は canWriteToNotion の
  //   allow-list (source==='portal' のみ許可) で fail-closed にしてある。
  if (!draftCols.has('source')) {
    db.exec(`ALTER TABLE product_drafts ADD COLUMN source TEXT NOT NULL DEFAULT 'portal'`);
  }
  if (!draftCols.has('source_notion_status')) {
    // 取り込み時点の Notion Status (⓪新規商品_高島 等)。product_drafts.status とは別軸なので原文保持
    db.exec('ALTER TABLE product_drafts ADD COLUMN source_notion_status TEXT');
  }
  // セット派生 (2026-08-23): どの単品から作ったセットか / 商品コードがまだ仮か。
  // 仮コードのまま楽天に出すと直せない (manage_number は登録後に変えられない) ので、
  // 出品ゲートでこのフラグを見て止める
  if (!draftCols.has('parent_draft_id')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN parent_draft_id INTEGER');
  }
  if (!draftCols.has('provisional_code')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN provisional_code INTEGER NOT NULL DEFAULT 0');
  }
  if (!draftCols.has('imported_at')) {
    db.exec('ALTER TABLE product_drafts ADD COLUMN imported_at TEXT');
  }
  // ページ表記の自動保存 (#691): ページロードごとのトークン + 単調増加 seq。
  // 自動保存とpagehideビーコンの到着順が逆転しても「古いリクエストが新しい保存を
  // 上書きしない」ためのリビジョン (同一トークン内でのみ seq を比較する)
  const pageInfoCols = new Set(db.prepare('PRAGMA table_info(draft_page_info)').all().map((c) => c.name));
  if (!pageInfoCols.has('save_token')) {
    db.exec('ALTER TABLE draft_page_info ADD COLUMN save_token TEXT');
  }
  if (!pageInfoCols.has('save_seq')) {
    db.exec('ALTER TABLE draft_page_info ADD COLUMN save_seq INTEGER');
  }

  // 店舗内カテゴリの枠番 (2026-08-02、RMS「表示先カテゴリ 1〜5」対応)
  // draft_sku_jans の JAN 重複防止 (2026-08-28)。テーブル定義側にも UNIQUE を書いているが、
  // CREATE TABLE IF NOT EXISTS は既存テーブルには効かない。UNIQUE の無い版が残っていても
  // 後から張れるようにする。重複が既にあると張れないので fail-soft (起動は止めない)
  try {
    const sjIndexes = db.prepare('PRAGMA index_list(draft_sku_jans)').all();
    const hasJanUnique = sjIndexes.some((ix) => {
      if (!ix.unique) return false;
      const cols = db.prepare(`PRAGMA index_info(${JSON.stringify(ix.name)})`).all().map((c) => c.name);
      return cols.length === 2 && cols.includes('draft_id') && cols.includes('jan_code');
    });
    if (!hasJanUnique) {
      db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_dsj_draft_jan ON draft_sku_jans(draft_id, jan_code)');
    }
  } catch (e) {
    console.warn('[product-hub] draft_sku_jans の JAN 一意制約を張れませんでした (重複データの可能性):', e.message);
  }

  // ブランド名 (2026-08-28)。既存 DB への冪等追加
  const piCols = new Set(db.prepare('PRAGMA table_info(draft_page_info)').all().map((c) => c.name));
  if (!piCols.has('brand_name')) {
    db.exec('ALTER TABLE draft_page_info ADD COLUMN brand_name TEXT');
  }

  migrateShopCategorySlots(db);

  // 楽天出品仕様 2026-07-27 (配送/納期/白抜き/公開状態)。#629 デプロイ済み DB への冪等 ALTER
  const rkCols = new Set(db.prepare('PRAGMA table_info(draft_rakuten)').all().map((c) => c.name));
  for (const [col, ddl] of [
    ['shipping_method_group', 'TEXT'],
    ['postage_included', 'INTEGER'],
    ['normal_delivery_date_id', 'TEXT'],
    ['white_bg_drive_file_id', 'TEXT'],
    ['white_bg_drive_url', 'TEXT'],
    // Drive の更新日時 (サムネURLの版数。上書き時にブラウザ/サーバーのキャッシュを外す)
    ['white_bg_modified_time', 'TEXT'],
    ['published_at', 'TEXT'],
    // 店舗内カテゴリの RMS 反映 (2026-08-02、item-mappings API)。
    // 商品APIに棚のフィールドが無く登録とは別呼び出しになるため、結果を個別に持つ
    ['shop_categories_synced_at', 'TEXT'],
    ['shop_categories_synced_key', 'TEXT'],
    ['shop_categories_error', 'TEXT'],
    // ボードからの出品 (2026-09-01) の直近の試行。outcome: running=実行中 / failed=失敗 (やり直せる) /
    // unknown=RMS への PUT の結果が確認できなかった (**やり直し禁止** — 実は登録が通っている可能性がある。
    // 人が RMS で確認してから管理者だけが再実行できる) / NULL=成功 or 未実行。
    // last_error だけだと「いつの・どの段階の失敗か」「再実行していいか」が分からない (Codex R1)
    ['listing_outcome', "TEXT CHECK (listing_outcome IN ('running', 'failed', 'unknown'))"],
    ['listing_attempt_at', 'TEXT'],
    // カタログID (= RMS 画面の「カタログID」/ API の articleNumber) が無いときの理由 (2026-09-02)。
    // JAN があれば JAN を送るので使わない。NULL = 未設定 → 送信時は 5 (該当製品コードなし) 扱い
    ['catalog_id_exemption_reason', 'INTEGER CHECK (catalog_id_exemption_reason BETWEEN 1 AND 6)'],
    // カラバリ出品 (2026-09-02)。項目選択肢の軸名 (「種類」「カラー」等)。1 ページ 1 軸まで
    ['variant_selector_name', 'TEXT'],
  ]) {
    if (!rkCols.has(col)) db.exec(`ALTER TABLE draft_rakuten ADD COLUMN ${col} ${ddl}`);
  }

  // Drive 画像の更新日時 (2026-08-08 スタッフ指摘: Drive で画像を上書きしてもサムネが
  // 古いまま表示される)。サムネURLに版数として載せ、ブラウザとサーバーのキャッシュを外す
  const imgCols = new Set(db.prepare('PRAGMA table_info(draft_images)').all().map((c) => c.name));
  if (!imgCols.has('drive_modified_time')) {
    db.exec('ALTER TABLE draft_images ADD COLUMN drive_modified_time TEXT');
  }
  if (!imgCols.has('source_label')) {
    // 取り込み元のファイル名末尾 ('_top' / '_01')。旧データ (NULL) は枠番との対応が
    // 新旧で違う (旧: sort0=_01) ため、UI ではラベルを出さない (Codex medium)
    db.exec('ALTER TABLE draft_images ADD COLUMN source_label TEXT');
  }
  // R-Cabinet 転送履歴にも更新日時を持たせる。Drive で同じファイルを上書きしたとき、
  // 「転送済み」と誤判定して**古い画像のまま楽天へ出す**のを防ぐ (Codex high)
  const cabCols = new Set(db.prepare('PRAGMA table_info(draft_cabinet_images)').all().map((c) => c.name));
  if (!cabCols.has('drive_modified_time')) {
    db.exec('ALTER TABLE draft_cabinet_images ADD COLUMN drive_modified_time TEXT');
  }
  const skuImgCols = new Set(db.prepare('PRAGMA table_info(draft_sku_images)').all().map((c) => c.name));
  if (!skuImgCols.has('drive_modified_time')) {
    db.exec('ALTER TABLE draft_sku_images ADD COLUMN drive_modified_time TEXT');
  }

  // 除外の一意性を「SKU単位グローバル」へ移行する (Codex R2 high)。
  //   CREATE ... IF NOT EXISTS は、旧定義 (draft 単位 UNIQUE) が残っていても何もしない。
  //   旧形のまま動くと「A で外して B から戻せない」不整合が残るので、定義を実測して張り替える。
  try {
    const tableSql = db.prepare(
      `SELECT sql FROM sqlite_master WHERE type='table' AND name='draft_variation_exclusions'`
    ).get()?.sql || '';
    const idxSql = db.prepare(
      `SELECT sql FROM sqlite_master WHERE type='index' AND name='idx_draft_vari_excl_code'`
    ).get()?.sql || null;

    // 旧テーブル制約 UNIQUE(draft_id, ne_code) が残っているか / 期待する式インデックスが無いか
    const hasOldTableUnique = /UNIQUE\s*\(\s*draft_id\s*,\s*ne_code\s*\)/i.test(tableSql);
    const hasWantedIndex = !!idxSql && /LOWER\s*\(\s*TRIM\s*\(\s*ne_code/i.test(idxSql) && /UNIQUE/i.test(idxSql);

    if (hasOldTableUnique || !hasWantedIndex) {
      // テーブル入れ替えは SQLite の手順どおり foreign_keys を一時 OFF にする
      // (ON のままだと DROP/RENAME と INSERT..SELECT が FK 違反で落ちる)。
      // PRAGMA はトランザクション外で切り替える必要がある
      const fkWasOn = db.pragma('foreign_keys', { simple: true }) === 1;
      if (fkWasOn) db.pragma('foreign_keys = OFF');
      try {
        db.transaction(() => {
          // ① 親ドラフトを失った孤児行を先に落とす。
          //    集約を先にやると「最古が孤児・後発が有効」のとき有効な除外まで消える (Codex R3 high)
          db.exec(`
            DELETE FROM draft_variation_exclusions
            WHERE NOT EXISTS (SELECT 1 FROM product_drafts d WHERE d.id = draft_variation_exclusions.draft_id)
          `);
          // ② 残った有効行のうち、正規化後に重複するものは最古の1件だけ残す
          db.exec(`
            DELETE FROM draft_variation_exclusions WHERE id NOT IN (
              SELECT MIN(id) FROM draft_variation_exclusions GROUP BY LOWER(TRIM(ne_code))
            )
          `);
          if (hasOldTableUnique) {
            // テーブル制約は ALTER で外せないので作り直す (swap)。
            // 親を失った孤児行はここで落とす (FK を復帰させるため)
            db.exec(`
              CREATE TABLE draft_variation_exclusions_new (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                draft_id   INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
                ne_code    TEXT NOT NULL,
                actor      TEXT,
                created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
              );
              INSERT INTO draft_variation_exclusions_new (id, draft_id, ne_code, actor, created_at)
                SELECT e.id, e.draft_id, e.ne_code, e.actor, e.created_at
                FROM draft_variation_exclusions e
                WHERE EXISTS (SELECT 1 FROM product_drafts d WHERE d.id = e.draft_id);
              DROP TABLE draft_variation_exclusions;
              ALTER TABLE draft_variation_exclusions_new RENAME TO draft_variation_exclusions;
              CREATE INDEX IF NOT EXISTS idx_draft_vari_excl_draft ON draft_variation_exclusions(draft_id);
            `);
          } else if (idxSql) {
            db.exec('DROP INDEX idx_draft_vari_excl_code');
          }
          db.exec(`CREATE UNIQUE INDEX idx_draft_vari_excl_code
                   ON draft_variation_exclusions(LOWER(TRIM(ne_code)))`);
        })();
        const violations = db.pragma('foreign_key_check(draft_variation_exclusions)');
        if (violations.length > 0) {
          console.warn(`[product-hub] 除外テーブル移行後に FK 違反 ${violations.length} 件`);
        }
      } finally {
        if (fkWasOn) db.pragma('foreign_keys = ON');
      }
    }
  } catch (e) {
    // 起動をポータルごと落とさない。張れていない場合は exclude API の INSERT が
    // 冪等でなくなるだけで、表示・既存機能は動く
    console.warn('[product-hub] 除外テーブルの一意制約移行に失敗:', e.message);
  }

  // バリエーション判定は LOWER(TRIM()) 照合なので、通常の索引が効かない (Codex medium-6)。
  // 式インデックスを張って全走査を避ける。mirror 側の所有テーブルなので失敗しても無視する
  for (const stmt of [
    'CREATE INDEX IF NOT EXISTS idx_mirp_sku_norm ON mirror_products(LOWER(TRIM(商品コード)))',
    'CREATE INDEX IF NOT EXISTS idx_mirp_rep_norm ON mirror_products(LOWER(TRIM(代表商品コード)))',
  ]) {
    try { db.exec(stmt); } catch (e) { console.warn('[product-hub] index skip:', e.message); }
  }

  // ne_code の一意性: DB の UNIQUE は BINARY 比較だが、NE/Notion 突合は LOWER(TRIM()) で行う。
  // 'ABC' と 'abc' が別ドラフトとして共存すると突合が壊れるので正規化 UNIQUE も張る (Codex medium-4)。
  // ⚠️ 既存データが衝突していると CREATE が失敗する → 起動を巻き添えにしないよう事前検査 + try/catch
  try {
    const dup = db.prepare(`
      SELECT COUNT(*) AS c FROM (
        SELECT LOWER(TRIM(ne_code)) k FROM product_drafts GROUP BY LOWER(TRIM(ne_code)) HAVING COUNT(*) > 1
      )
    `).get().c;
    if (dup === 0) {
      db.exec('CREATE UNIQUE INDEX IF NOT EXISTS idx_product_drafts_ne_norm ON product_drafts(LOWER(TRIM(ne_code)))');
      neCodeUniqueEnforced = true;
    } else {
      console.warn(`[product-hub] ne_code の正規化重複 ${dup} 件のため UNIQUE index を張れません (要データ修正)`);
    }
  } catch (e) {
    console.warn('[product-hub] ne_code 正規化 UNIQUE index skip:', e.message);
  }

  // draft_events は append-only (mis-shipment と同じ trigger ガード)
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_draft_events_no_update
    BEFORE UPDATE ON draft_events
    BEGIN SELECT RAISE(ABORT, 'draft_events is append-only'); END;
  `);
  db.exec(`
    CREATE TRIGGER IF NOT EXISTS trg_draft_events_no_delete
    BEFORE DELETE ON draft_events
    BEGIN SELECT RAISE(ABORT, 'draft_events is append-only'); END;
  `);

  // ─── ワークフロー (担当者/役割/工程) の索引とシード ─────────────────
  // 楽観ロックの版数列。PR 途中の状態でデプロイした環境にも足せるよう冪等 ALTER にする
  const dspCols = new Set(db.prepare('PRAGMA table_info(draft_step_progress)').all().map((c) => c.name));
  if (!dspCols.has('version')) {
    try {
      db.exec('ALTER TABLE draft_step_progress ADD COLUMN version INTEGER NOT NULL DEFAULT 0');
    } catch (e) {
      // ポータルと warehouse variant が同時に起動すると、両方が「列なし」と判定して
      // 後発が duplicate column で落ちる。列が既にあるなら成功と同義なので飲み込む
      if (!/duplicate column/i.test(String(e?.message || ''))) throw e;
    }
  }

  for (const stmt of [
    // 担当者名の表記ゆれ防止。「大川さん」と「大川 さん」を別人として登録させない
    'CREATE UNIQUE INDEX IF NOT EXISTS idx_ph_staff_name_norm ON ph_staff(LOWER(TRIM(name)))',
    // 既定担当は役割ごとに 1 人。DB で担保しないと、画面からの付け替えが競合したときに
    // 「既定が 2 人いる役割」ができて、新規ドラフトへの自動割り当てが不定になる
    'CREATE UNIQUE INDEX IF NOT EXISTS idx_ph_staff_roles_default ON ph_staff_roles(role_code) WHERE is_default = 1',
    // 1 つのポータルアカウントを 2 人に紐づけない (「自分の作業」の絞り込みが二重になる)。
    // 未設定 (NULL/空) は何人いてもよいので partial index にする
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_ph_staff_email_norm ON ph_staff(LOWER(TRIM(portal_email)))
       WHERE portal_email IS NOT NULL AND TRIM(portal_email) <> ''`,
  ]) {
    try { db.exec(stmt); } catch (e) { console.warn('[product-hub] workflow index skip:', e.message); }
  }

  // 画像トラックの TOP/詳細 分割 (2026-08-24)。#888 デプロイ済み環境の ph_steps に列を足す
  const stepCols = new Set(db.prepare('PRAGMA table_info(ph_steps)').all().map((c) => c.name));
  for (const [col, ddl] of [
    ['image_kind', "ALTER TABLE ph_steps ADD COLUMN image_kind TEXT CHECK (image_kind IN ('top', 'detail'))"],
    ['image_stage', 'ALTER TABLE ph_steps ADD COLUMN image_stage TEXT'],
    // 2026-08-26 v2: 「対象外」にできるか / 楽天出品ゲートに数えるか を工程属性で持つ (kind 依存をやめる — Codex 設計相談)
    ['skippable', 'ALTER TABLE ph_steps ADD COLUMN skippable INTEGER NOT NULL DEFAULT 1 CHECK (skippable IN (0, 1))'],
    ['listing_gate', 'ALTER TABLE ph_steps ADD COLUMN listing_gate INTEGER NOT NULL DEFAULT 1 CHECK (listing_gate IN (0, 1))'],
  ]) {
    if (!stepCols.has(col)) {
      try { db.exec(ddl); } catch (e) {
        // ポータルと warehouse variant の同時起動レース (version 列の ALTER と同じ扱い)
        if (!/duplicate column/i.test(String(e?.message || ''))) throw e;
      }
    }
  }
  // 詳細画像を作らない商品のフラグ (単純な仕入れ商品は TOP のみ — 2026-08-24 中原さん)。
  // detail 側の工程行を skip に書き換える方式にしなかったのは、done の履歴を壊さず、
  // 複数行更新の競合検出も不要になるため (Codex設計相談)。工程行はそのまま残し、
  // ゲート・ボード・詳細画面がこのフラグを見て detail 側を「対象外」として扱う
  const draftCols2 = new Set(db.prepare('PRAGMA table_info(product_drafts)').all().map((c) => c.name));
  if (!draftCols2.has('detail_images_excluded')) {
    try {
      db.exec('ALTER TABLE product_drafts ADD COLUMN detail_images_excluded INTEGER NOT NULL DEFAULT 0 CHECK (detail_images_excluded IN (0, 1))');
    } catch (e) {
      if (!/duplicate column/i.test(String(e?.message || ''))) throw e;
    }
  }
  // TOP画像をどれぐらい力を入れて作るかの目安 (2026-08-24 中原さん。Notion 画像情報DB の分類を移植)。
  // 自社商品限定の draft_image_production.importance_tier (枚数の目安) とは別物 — こちらは仕入商品にも付ける
  if (!draftCols2.has('image_priority')) {
    try {
      db.exec('ALTER TABLE product_drafts ADD COLUMN image_priority TEXT');
    } catch (e) {
      if (!/duplicate column/i.test(String(e?.message || ''))) throw e;
    }
  }

  // 2026-08-26 Notion 画像DB (商品ページ商品画像登録) 移植 (要件定義 = AI_reference『Notion画像DB移植_要件定義_20260826.md』)。
  //   canva_url       … Notion「Canva」プロパティ (制作中デザインのリンク)
  //   workflow_state  … **画像制作だけの保留** (中原さん決定 8/26)。商品本流の on_hold と違い、
  //                     本流・AI・ボード本流は止めず、画像トラックと楽天出品ゲートだけを閉じる
  //   hold_note       … 保留の理由 (任意)
  const ipCols = new Set(db.prepare('PRAGMA table_info(draft_image_production)').all().map((c) => c.name));
  const ipAlters = [
    ['canva_url', 'ALTER TABLE draft_image_production ADD COLUMN canva_url TEXT'],
    // 2026-08-26 画像工程 v2: 撮影・素材ステータス (安定コード) / 手入力の商品情報 (1.5・定型文ボタンの材料)
    ['material_status', 'ALTER TABLE draft_image_production ADD COLUMN material_status TEXT'],
    ['product_info_text', 'ALTER TABLE draft_image_production ADD COLUMN product_info_text TEXT'],
    ['product_info_updated_at', 'ALTER TABLE draft_image_production ADD COLUMN product_info_updated_at TEXT'],
    ['product_info_updated_by', 'ALTER TABLE draft_image_production ADD COLUMN product_info_updated_by TEXT'],
    ['workflow_state', "ALTER TABLE draft_image_production ADD COLUMN workflow_state TEXT NOT NULL DEFAULT 'active' CHECK (workflow_state IN ('active', 'on_hold'))"],
    ['hold_note', 'ALTER TABLE draft_image_production ADD COLUMN hold_note TEXT'],
  ];
  for (const [col, sql] of ipAlters) {
    if (ipCols.has(col)) continue;
    try {
      db.exec(sql);
    } catch (e) {
      if (!/duplicate column/i.test(String(e?.message || ''))) throw e;
    }
  }
  // 画像DB移植の**移植元カード台帳** (Codex R1: 画像DBは 1 ドラフト : 1 カードでない。
  // draft_image_production に page id を 1 つ置くと、同じ商品の他カードの冪等・監査情報を失う)。
  // notion_page_id が PK = 同じカードを二度取り込まない。source_hash は再実行時の「同じ内容 / 変わった」判定
  db.exec(`
    CREATE TABLE IF NOT EXISTS draft_image_notion_imports (
      notion_page_id   TEXT PRIMARY KEY,
      draft_id         INTEGER NOT NULL REFERENCES product_drafts(id) ON DELETE CASCADE,
      source_ne_code   TEXT NOT NULL,
      source_status    TEXT,
      drive_folder_url TEXT,
      source_hash      TEXT NOT NULL,
      import_run_id    TEXT NOT NULL,
      imported_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
    );
    CREATE INDEX IF NOT EXISTS idx_dini_draft ON draft_image_notion_imports(draft_id);
  `);

  // 役割・工程の初期値。INSERT OR IGNORE なので、管理画面で改名・並べ替え・無効化しても
  // 毎起動で巻き戻らない (code が PK)。ph_steps.role_code は ph_roles を参照するので順序が要る
  const roleSeed = db.prepare('INSERT OR IGNORE INTO ph_roles (code, label, sort, builtin) VALUES (?, ?, ?, 1)');
  const stepSeed = db.prepare(`
    INSERT OR IGNORE INTO ph_steps (code, label, track, image_kind, image_stage, role_code, sort, stall_days, description, builtin, skippable, listing_gate)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
  `);
  // 属性 (skippable / listing_gate) は管理画面で変えられないので、既存行も seed の値に揃える (列追加の後追いを含む)
  const stepAttr = db.prepare('UPDATE ph_steps SET skippable = ?, listing_gate = ? WHERE code = ? AND (skippable != ? OR listing_gate != ?)');
  db.transaction(() => {
    for (const r of ROLE_SEEDS) roleSeed.run(r.code, r.label, r.sort);
    for (const s of STEP_SEEDS) {
      // 既定: 画像 TOP 系列と basic_info / listing は対象外にできない (従来ルールを属性に写す)
      const skippable = s.skippable ?? ((s.track === 'image' && s.image_kind !== 'detail') || s.code === 'basic_info' || s.code === 'listing' ? 0 : 1);
      const gate = s.listing_gate ?? 1;
      stepSeed.run(
        s.code, s.label, s.track, s.image_kind ?? null, s.image_stage ?? null,
        s.role_code ?? null, s.sort, s.stall_days ?? null, s.description ?? null,
        skippable, gate,
      );
      stepAttr.run(skippable, gate, s.code, skippable, gate);
    }
    migrateImageKindSplit(db);
    migrateDetailTrackV2(db);
    retireTopImageSteps(db);
    syncOwnBrandImagePriority(db);
  })();

  initialized = true;
  return db;
}

/**
 * own_brand ⇄ 画像の重要度「自社商品（重要度：高）」の整合 (2026-08-24 連動導入、冪等)。
 * 不変条件: **own_brand=1 と image_priority=自社商品 は常に同値**。
 * 連動導入前の既存データの整合化 + 万一の不整合の自己修復として毎起動で走らせる (Codex R1 medium)。
 *   - 重要度が設定済み → 重要度を正として own_brand を合わせる (重要度の方が後から入った情報)
 *   - 重要度が未設定で own_brand=1 → 自社の重要度は1種類しかないので「自社商品（重要度：高）」を入れる
 */
export function syncOwnBrandImagePriority(db) {
  db.prepare(`
    UPDATE product_drafts SET own_brand = CASE WHEN image_priority = ? THEN 1 ELSE 0 END
    WHERE image_priority IS NOT NULL
      AND own_brand != CASE WHEN image_priority = ? THEN 1 ELSE 0 END
  `).run(OWN_BRAND_IMAGE_PRIORITY, OWN_BRAND_IMAGE_PRIORITY);
  db.prepare(`
    UPDATE product_drafts SET image_priority = ? WHERE image_priority IS NULL AND own_brand = 1
  `).run(OWN_BRAND_IMAGE_PRIORITY);
}

/**
 * 画像トラックの一本 → TOP/詳細 分割の移行 (2026-08-24、冪等)。
 * #888/#890 デプロイ済み環境には旧4工程 (img_request 等) の進捗行があるので:
 *   - TOP 側: 旧行の状態をそのまま引き継ぐ (旧トラックの実体は TOP 中心の作業だったため)
 *   - 詳細側: done を引き継ぐのは**楽天登録済みの商品だけ** (出品済みに「承認して」を出さない #891 方針)。
 *     旧行の done は「画像が 1 枚でもあれば done」の初回推定を含むため、それを根拠に詳細側を
 *     done にすると TOP しか無い商品が出品ゲートを素通りする (Codex R1 critical)。
 *     それ以外は todo で入り、詳細画像を作るか (対象外か) を改めて判断してもらう
 *   - 旧4工程は active=0 (進捗行は残置 — 全クエリが active=1 join なので見えない)
 * 呼び出し元 (シード) のトランザクション内で走る。コピーと無効化の間に別プロセスが
 * 旧行を更新して変更が消えるレースを避ける (Codex設計相談 High)。
 */
export function migrateImageKindSplit(db) {
  const placeholders = LEGACY_IMAGE_STEP_CODES.map(() => '?').join(',');
  const legacyActive = db.prepare(
    `SELECT COUNT(*) AS c FROM ph_steps WHERE code IN (${placeholders}) AND active = 1`
  ).get(...LEGACY_IMAGE_STEP_CODES).c;
  if (legacyActive === 0) return false;   // 新規 DB / 移行済み

  const copyTop = db.prepare(`
    INSERT OR IGNORE INTO draft_step_progress
      (draft_id, step_code, state, assignee_id, due_date, started_at, done_at, done_by, note)
    SELECT draft_id, ? , state, assignee_id, due_date, started_at, done_at, done_by, note
    FROM draft_step_progress WHERE step_code = ?
  `);
  const copyDetail = db.prepare(`
    INSERT OR IGNORE INTO draft_step_progress
      (draft_id, step_code, state, assignee_id, due_date, started_at, done_at, done_by, note)
    SELECT p.draft_id, ?,
      CASE WHEN p.state IN ('done', 'skip')
             AND EXISTS (SELECT 1 FROM draft_rakuten dr WHERE dr.draft_id = p.draft_id AND dr.registered_at IS NOT NULL)
           THEN p.state ELSE 'todo' END,
      p.assignee_id, p.due_date, NULL,
      CASE WHEN p.state IN ('done', 'skip')
             AND EXISTS (SELECT 1 FROM draft_rakuten dr WHERE dr.draft_id = p.draft_id AND dr.registered_at IS NOT NULL)
           THEN COALESCE(p.done_at, strftime('%Y-%m-%dT%H:%M:%fZ','now')) ELSE NULL END,
      CASE WHEN p.state IN ('done', 'skip')
             AND EXISTS (SELECT 1 FROM draft_rakuten dr WHERE dr.draft_id = p.draft_id AND dr.registered_at IS NOT NULL)
           THEN 'migration' ELSE NULL END,
      p.note
    FROM draft_step_progress p WHERE p.step_code = ?
  `);
  let copied = 0;
  const stepExists = db.prepare('SELECT 1 FROM ph_steps WHERE code = ?');
  for (const legacy of LEGACY_IMAGE_STEP_CODES) {
    // コピー先の工程が無ければ写さない (FK 制約違反になる)。
    // TOP の 4 工程は 2026-08-31 に廃止し、詳細 v1 も v2 以降シードされないので、
    // 新規 DB では両方とも「写す先が無い」= 旧工程を無効化するだけになる
    if (stepExists.get(`${legacy}_top`)) copied += copyTop.run(`${legacy}_top`, legacy).changes;
    if (stepExists.get(`${legacy}_detail`)) copied += copyDetail.run(`${legacy}_detail`, legacy).changes;
  }
  db.prepare(`UPDATE ph_steps SET active = 0 WHERE code IN (${placeholders})`).run(...LEGACY_IMAGE_STEP_CODES);
  console.log(`[product-hub] 画像工程を TOP/詳細 に分割しました (進捗コピー ${copied} 行)`);
  return true;
}

/**
 * 詳細系列 v1 (依頼/撮影/制作/登録/承認) → v2 (①〜⑨) の一回きり移行 (2026-08-26)。
 * 要件定義 §3.6 (中原さん決定 4): 楽天登録済み = 全部 done / 未登録 = 旧で最も進んだ段階まで done、
 * 旧「登録/承認」done でも ⑥-1 (田中確認) から再確認 (田中確認済みを DB から証明できないため — Codex R1)。
 * 旧進捗行は残す。新行にはイベントで元工程・元 state を記録。実行済みは ph_intake_state に記録 (再実行しない)。
 * 旧 shipping_status → material_status も、空のときだけ写す。
 * @returns {{ migrated: number, skipped: boolean }}
 */
/**
 * TOP画像の 4 工程を無効化する (2026-08-31 中原さん決定)。
 *
 * 「LP と TOP画像は基本的に同時進行で制作するため、工程を分けて管理する必要性が低い」。
 * 画像の工程は商品詳細 (LP) の 10 段階に一本化し、カードは 1 商品 1 枚にする。
 * TOP画像そのものは楽天出品に必須なので、ゲートは工程ではなく
 * **画像が登録されているか** で見る (imageTrackBlockReason)。
 * 進捗行 (draft_step_progress) は消さない — 誰がどこまでやったかの記録として残す。冪等。
 */
export function retireTopImageSteps(db) {
  // **コードでなく属性で**落とす (Codex R3 high): 管理画面から足したカスタム TOP 工程
  // (step_xxx / image_kind='top' や NULL) が残ると、そこだけ TOP 列・TOP カードが復活する。
  // 画像トラックは商品詳細 (LP) の 1 本に統一したので、detail 以外の画像工程はすべて無効化する
  const info = db.prepare(`
    UPDATE ph_steps SET active = 0
    WHERE track = 'image' AND (image_kind IS NULL OR image_kind <> 'detail') AND active = 1
  `).run();
  // 説明文も直す (Codex R5 low)。シードは INSERT OR IGNORE なので、既存 DB には
  // 「TOP側工程も自動完了」という**もう起きない動作**の説明が残ってしまう
  // **旧文言に完全一致するときだけ**置き換える (Codex R6 low): 「新文言と違えば上書き」だと、
  // 管理画面で書き換えた独自の説明が毎起動で消える
  const fixDesc = db.prepare('UPDATE ph_steps SET description = ? WHERE code = ? AND description = ?');
  for (const [code, oldDesc, newDesc] of [
    ['imgd_design',
      '⑤ AI 画像を修正 + TOP画像制作 (完了で TOP 側の 依頼/制作/登録 も自動で済みになる)',
      '⑤ AI 画像を修正 + TOP画像制作 (TOP と LP は同時進行で作る)'],
    ['imgd_review_2',
      '⑥-2 最終確認 (田中確認の後にしか完了できない)。完了で TOP 側の承認も自動で済みになる = 楽天出品ゲートが開く',
      '⑥-2 最終確認 (田中確認の後にしか完了できない)。完了で楽天出品ゲートが開く'],
  ]) fixDesc.run(newDesc, code, oldDesc);
  if (info.changes > 0) {
    console.log(`[product-hub] TOP画像の工程 ${info.changes} 件を無効化しました (画像工程は詳細に一本化)`);
  }
  return info.changes;
}

export function migrateDetailTrackV2(db) {
  const ph = LEGACY_DETAIL_V1_CODES.map(() => '?').join(',');
  const run = db.transaction(() => {
    // 1. 旧詳細工程を無効化 (新規 DB では行が無いので no-op)
    db.prepare(`UPDATE ph_steps SET active = 0 WHERE code IN (${ph})`).run(...LEGACY_DETAIL_V1_CODES);
    // 2. 旧詳細の進捗を持ち、**まだ v2 へ写していない**ドラフトを写す (ドラフト単位で冪等 — Codex R2 high:
    //    DB 全体のマーカー 1 個だと、抽出漏れ・途中デプロイで写し損ねた商品が永久に確定する)。
    //    「写した」根拠は draft_events の image_track_v2_migrated
    const drafts = db.prepare(`
      SELECT DISTINCT p.draft_id AS id FROM draft_step_progress p
      WHERE p.step_code IN (${ph})
        AND NOT EXISTS (SELECT 1 FROM draft_events e WHERE e.draft_id = p.draft_id AND e.event = 'image_track_v2_migrated')
    `).all(...LEGACY_DETAIL_V1_CODES).map((r) => r.id);
    const oldRows = db.prepare(`SELECT step_code, state FROM draft_step_progress WHERE draft_id = ? AND step_code IN (${ph})`);
    // 楽天登録済みの根拠は 1 表に限らない (Codex R2 high): アプリ経由の登録記録 / 導出 status / モール別状況の楽天 done
    const listedQ = db.prepare(`
      SELECT 1 WHERE EXISTS (SELECT 1 FROM draft_rakuten WHERE draft_id = @id AND registered_at IS NOT NULL)
         OR EXISTS (SELECT 1 FROM product_drafts WHERE id = @id AND status IN ('listed', 'expanded'))
         OR EXISTS (SELECT 1 FROM draft_mall_status WHERE draft_id = @id AND mall = 'rakuten' AND state = 'done')
    `);
    // 既に v2 行が (途中デプロイ等で) todo のまま存在していても期待状態へ揃える
    const ins = db.prepare(`
      INSERT INTO draft_step_progress (draft_id, step_code, state, done_at, done_by)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT(draft_id, step_code) DO UPDATE SET
        state = CASE WHEN draft_step_progress.state = 'todo' AND excluded.state = 'done' THEN 'done' ELSE draft_step_progress.state END,
        done_at = CASE WHEN draft_step_progress.state = 'todo' AND excluded.state = 'done' THEN excluded.done_at ELSE draft_step_progress.done_at END,
        done_by = CASE WHEN draft_step_progress.state = 'todo' AND excluded.state = 'done' THEN excluded.done_by ELSE draft_step_progress.done_by END,
        version = draft_step_progress.version + 1
    `);
    const settled = (st) => st === 'done' || st === 'skip';
    let migrated = 0;
    for (const id of drafts) {
      const old = Object.fromEntries(oldRows.all(id, ...LEGACY_DETAIL_V1_CODES).map((r) => [r.step_code, r.state]));
      const listed = !!listedQ.get({ id });
      let doneUpTo = -1;   // DETAIL_V2_CODES の index。-1 = 何も済んでいない
      let rule;
      if (listed) {
        doneUpTo = DETAIL_V2_CODES.length - 1; rule = '楽天登録済み → 全工程 done';
      } else if (settled(old.img_register_detail) || settled(old.img_approve_detail)) {
        doneUpTo = DETAIL_V2_CODES.indexOf('imgd_design'); rule = '旧 登録/承認 済み → ⑤まで done・⑥-1 (田中確認) から再確認';
      } else if (settled(old.img_production_detail)) {
        doneUpTo = DETAIL_V2_CODES.indexOf('imgd_ai'); rule = '旧 制作 済み → ④まで done・⑤から';
      } else if (settled(old.img_request_detail) || settled(old.img_shoot_detail)) {
        // 旧 撮影依頼中 が済み = 依頼は当然済み (Codex R3 high: 依頼行が無くても拾う)
        doneUpTo = DETAIL_V2_CODES.indexOf('imgd_request'); rule = '旧 依頼/撮影 済み → ①done・②から';
      } else {
        rule = '旧 未着手 → 全工程 todo';
      }
      const now = new Date().toISOString();
      DETAIL_V2_CODES.forEach((code, i) => {
        const st = i <= doneUpTo ? 'done' : 'todo';
        ins.run(id, code, st, st === 'done' ? now : null, st === 'done' ? 'migration_v2' : null);
      });
      // 旧 撮影依頼中: 「対象外」= 撮影不要 / 「完了」= 撮影データ納品済み = 素材完了 (空のときだけ)
      if (old.img_shoot_detail === 'skip' || old.img_shoot_detail === 'done') {
        db.prepare('INSERT OR IGNORE INTO draft_image_production (draft_id) VALUES (?)').run(id);
        db.prepare('UPDATE draft_image_production SET material_status = ? WHERE draft_id = ? AND material_status IS NULL')
          .run(old.img_shoot_detail === 'skip' ? 'not_required' : 'ready', id);
      }
      const oldDesc = LEGACY_DETAIL_V1_CODES.map((c) => `${c}=${old[c] || '-'}`).join(' ');
      logEvent(db, id, 'image_track_v2_migrated', `${rule} (旧: ${oldDesc})`, 'migration_v2');
      migrated += 1;
    }
    // 2b. TOP 系列の並び (管理画面で変えていない = 旧既定値のままなら、v2 の物差しに揃える)
    for (const [code, from, to] of [['img_production_top', 20, 51], ['img_register_top', 30, 52], ['img_approve_top', 40, 62]]) {
      db.prepare('UPDATE ph_steps SET sort = ? WHERE code = ? AND sort = ?').run(to, code, from);
    }
    // 3. 旧 Notion 5 値 → 撮影・素材ステータス (空のときだけ。毎起動で冪等)
    for (const [from, to] of Object.entries(SHIPPING_TO_MATERIAL)) {
      db.prepare('UPDATE draft_image_production SET material_status = ? WHERE material_status IS NULL AND shipping_status = ?').run(to, from);
    }
    // v2 に切り替えた日時 (初回だけ記録。①の「商品情報必須」の境目に使う)
    db.prepare('INSERT OR IGNORE INTO ph_intake_state (key, value) VALUES (?, ?)').run(IMAGE_TRACK_V2_KEY, new Date().toISOString());
    return migrated;
  });
  const migrated = run();
  if (migrated > 0) console.log(`[product-hub] 商品詳細画像の工程を v2 (①〜⑨) へ移行しました (${migrated} 件)`);
  return { migrated, skipped: migrated === 0 };
}

/** 画像工程 v2 に切り替えた日時 (この日時より後に作られた商品は ①の完了に商品情報が必須) */
export function imageTrackV2At(db) {
  return db.prepare('SELECT value FROM ph_intake_state WHERE key = ?').get(IMAGE_TRACK_V2_KEY)?.value || null;
}

export function getDB() {
  return initProductHubDB();
}

/** smoke 専用: 初期化フラグを戻して migration を再実行させる (本番からは呼ばない) */
export function _resetInitForTest() {
  initialized = false;
}

export function logEvent(db, draftId, event, detail, actor) {
  db.prepare(`
    INSERT INTO draft_events (draft_id, event, detail, actor) VALUES (?, ?, ?, ?)
  `).run(draftId, event, detail == null ? null : String(detail), actor || null);
}

/**
 * 生成待ち (ready_for_ai) に進めるための必須条件チェック (§4 参考URL必須ゲート)。
 * @returns {string[]} 不足理由 (空配列 = 進める)
 */
export function gateReasons(db, draft) {
  const reasons = [];
  if (!draft.name || !String(draft.name).trim()) reasons.push('商品名が未入力です');
  if (!draft.ne_code || !String(draft.ne_code).trim()) reasons.push('NE商品コードが未入力です');
  // AI が説明文を書くための「材料」が1つでもあれば通す (中原さん 2026-08-02)。
  // 公式ページが無い商品でも、参考URL や Amazon の販売ページがあれば書ける
  const has = (v) => !!(v && String(v).trim());
  if (!has(draft.official_url) && !has(draft.amazon_url)) {
    const refCount = db.prepare('SELECT COUNT(*) AS c FROM draft_reference_urls WHERE draft_id = ?').get(draft.id).c;
    if (refCount === 0) {
      reasons.push('AIが参照できるURLがありません (公式ページURL / 参考URL / Amazon URL のどれか1つ)');
    }
  }
  // 商品画像の有無はここでは見ない (2026-08-24 中原さん: 画像は画像トラックで並行して作る運用で、
  // 白抜きだけの状態で登録を進めることもある。AIの説明文生成に画像は不要)。
  // 楽天出品側のチェック (buildItemPayload の _top 必須・転送済み判定) は従来どおり効く
  return reasons;
}

/** draft_yahoo の upsert (部分更新: undefined のキーは既存値を維持) */
export function upsertDraftYahoo(db, draftId, fields) {
  const existing = db.prepare('SELECT * FROM draft_yahoo WHERE draft_id = ?').get(draftId) || {};
  const merged = {
    yahoo_price: fields.yahoo_price !== undefined ? fields.yahoo_price : (existing.yahoo_price ?? null),
    yahoo_price_sagawa: fields.yahoo_price_sagawa !== undefined ? fields.yahoo_price_sagawa : (existing.yahoo_price_sagawa ?? null),
    delivery_label: fields.delivery_label !== undefined ? fields.delivery_label : (existing.delivery_label ?? null),
    tax_rate: fields.tax_rate !== undefined ? fields.tax_rate : (existing.tax_rate ?? null),
    yahoo_category_id: fields.yahoo_category_id !== undefined ? fields.yahoo_category_id : (existing.yahoo_category_id ?? null),
    yahoo_path: fields.yahoo_path !== undefined ? fields.yahoo_path : (existing.yahoo_path ?? null),
  };
  db.prepare(`
    INSERT INTO draft_yahoo (draft_id, yahoo_price, yahoo_price_sagawa, delivery_label, tax_rate, yahoo_category_id, yahoo_path)
    VALUES (@draft_id, @yahoo_price, @yahoo_price_sagawa, @delivery_label, @tax_rate, @yahoo_category_id, @yahoo_path)
    ON CONFLICT(draft_id) DO UPDATE SET
      yahoo_price = excluded.yahoo_price,
      yahoo_price_sagawa = excluded.yahoo_price_sagawa,
      delivery_label = excluded.delivery_label,
      tax_rate = excluded.tax_rate,
      yahoo_category_id = excluded.yahoo_category_id,
      yahoo_path = excluded.yahoo_path,
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `).run({ draft_id: draftId, ...merged });
}

export const IMAGE_WORKFLOW_STATES = ['active', 'on_hold'];

/**
 * 画像制作だけの保留 / 解除 (2026-08-26 中原さん決定: Notion 画像DB の「保留」= 画像制作の保留であって
 * 商品そのものの保留ではない)。product_drafts.status は触らない。
 * 行が無ければ作る (INSERT OR IGNORE → UPDATE)。冪等: 同じ状態なら changed=false でイベントも残さない
 */
export function setImageWorkflowState(db, draftId, state, { note = null, actor = null } = {}) {
  const st = String(state || '');
  if (!IMAGE_WORKFLOW_STATES.includes(st)) throw new Error('画像制作の状態の指定が不正です');
  const id = Number(draftId);
  const run = db.transaction(() => {
    db.prepare('INSERT OR IGNORE INTO draft_image_production (draft_id) VALUES (?)').run(id);
    const cur = db.prepare('SELECT workflow_state, hold_note FROM draft_image_production WHERE draft_id = ?').get(id);
    const noteClean = note == null ? null : (String(note).trim().slice(0, 300) || null);
    if (cur.workflow_state === st && (st === 'active' || (cur.hold_note ?? null) === noteClean)) return { changed: false };
    db.prepare(`
      UPDATE draft_image_production
      SET workflow_state = ?, hold_note = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE draft_id = ?
    `).run(st, st === 'on_hold' ? noteClean : null, id);
    logEvent(db, id, st === 'on_hold' ? 'image_hold' : 'image_resume',
      st === 'on_hold' ? (noteClean || '画像制作を保留') : '画像制作の保留を解除', actor);
    return { changed: true };
  });
  return run();
}

/**
 * 「確認中」を立てる / 理由・メモを差し替える (2026-08-31)。
 *
 * status も工程も動かさない。効くのは 2 つだけ:
 *   - ボードのカードにラベルが出て、列の先頭に並ぶ (埋もれない)
 *   - AI 生成キューの候補から外れる (確認中なのに夜間 AI が原稿を書く、を防ぐ)
 * checking_since は **立て直しても進めない** (「何日待っているか」が 0 に戻ると
 * 長引いているカードを見失う)。理由やメモの修正で待ち時間がリセットされない。
 *
 * claim も同時に解放する (Codex R1 medium)。生成中の run が居るまま確認中にすると、
 * lease の 30 分以内に解除された場合だけ古い run が結果を書き戻せてしまう。
 * generation_block も claim を消しているので、そちらとも揃う。
 */
export function setDraftChecking(db, draftId, { reasonCode, note = null, actor = null } = {}) {
  const code = String(reasonCode || '');
  if (!CHECKING_REASON_CODES.has(code)) {
    const e = new Error('確認中の理由の指定が不正です');
    e.status = 400;
    throw e;
  }
  const id = Number(draftId);
  const noteClean = note == null ? null : (String(note).trim().slice(0, CHECKING_NOTE_MAX) || null);
  const run = db.transaction(() => {
    const cur = db.prepare(`
      SELECT checking_reason_code, checking_note, checking_since FROM product_drafts WHERE id = ?
    `).get(id);
    if (!cur) return { changed: false };
    // 冪等: 同じ理由・同じメモなら書かない (イベントログを無意味に増やさない)。
    // ただし claim だけは落とす — 「setDraftChecking を呼べば claim は必ず落ちている」を
    // 関数の契約にするため (Codex R2 low: 何らかの経路で確認中のまま claim が残った状態を
    // 自己修復できるようにする)。updated_at は進めない (中身は変わっていない)
    if (cur.checking_reason_code === code && (cur.checking_note ?? null) === noteClean) {
      db.prepare(`
        UPDATE product_drafts SET generation_claim_run_id = NULL, generation_claim_until = NULL
        WHERE id = ? AND (generation_claim_run_id IS NOT NULL OR generation_claim_until IS NOT NULL)
      `).run(id);
      return { changed: false };
    }
    const wasOn = cur.checking_since != null;
    db.prepare(`
      UPDATE product_drafts
      SET checking_reason_code = ?, checking_note = ?,
          checking_since = COALESCE(checking_since, strftime('%Y-%m-%dT%H:%M:%fZ','now')),
          checking_by = ?,
          generation_claim_run_id = NULL, generation_claim_until = NULL,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ?
    `).run(code, noteClean, actor, id);
    const label = CHECKING_REASON_LABELS[code] || code;
    logEvent(db, id, wasOn ? 'checking_updated' : 'checking_on',
      `${wasOn ? '確認中の理由を変更' : '確認中にした'}: ${label}${noteClean ? ` (${noteClean})` : ''}`, actor);
    return { changed: true };
  });
  return run();
}

/** 「確認中」を外す。4 列まとめて NULL に戻す (中途半端な残り方をさせない) */
export function clearDraftChecking(db, draftId, { actor = null } = {}) {
  const id = Number(draftId);
  const run = db.transaction(() => {
    const cur = db.prepare('SELECT checking_since FROM product_drafts WHERE id = ?').get(id);
    if (!cur || cur.checking_since == null) return { changed: false };
    db.prepare(`
      UPDATE product_drafts
      SET checking_reason_code = NULL, checking_note = NULL, checking_since = NULL, checking_by = NULL,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ?
    `).run(id);
    logEvent(db, id, 'checking_off', '確認中を解除', actor);
    return { changed: true };
  });
  return run();
}

const IMAGE_PRODUCTION_FIELDS = [
  'status', 'importance_tier', 'production_type', 'aplus_content', 'aplus_related',
  'camera_instruction_url', 'shipping_status', 'reference_collection',
  'designer', 'page_composer', 'request_text',
  'canva_url',   // 2026-08-26 Notion 画像DB の「Canva」(制作中デザインのリンク) 移植で追加
  'material_status', 'product_info_text', 'product_info_updated_at', 'product_info_updated_by',   // 画像工程 v2
];

/** draft_image_production の upsert (部分更新)。自社商品のみ呼ぶ想定 (router 側でガード) */
export function upsertImageProduction(db, draftId, fields) {
  const existing = db.prepare('SELECT * FROM draft_image_production WHERE draft_id = ?').get(draftId) || {};
  const merged = {};
  for (const f of IMAGE_PRODUCTION_FIELDS) {
    merged[f] = fields[f] !== undefined ? fields[f] : (existing[f] ?? null);
  }
  db.prepare(`
    INSERT INTO draft_image_production (draft_id, ${IMAGE_PRODUCTION_FIELDS.join(', ')})
    VALUES (@draft_id, ${IMAGE_PRODUCTION_FIELDS.map((f) => `@${f}`).join(', ')})
    ON CONFLICT(draft_id) DO UPDATE SET
      ${IMAGE_PRODUCTION_FIELDS.map((f) => `${f} = excluded.${f}`).join(', ')},
      updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
  `).run({ draft_id: draftId, ...merged });
}

/**
 * 生成待ち (ready_for_ai) の一覧を、AI 生成に必要な材料つきで返す (P2 スキル接続用)。
 * 返す形: [{ id, ne_code, name, official_url, amazon_url, asin, price, jan_code, drive_folder_url,
 *            reference_urls: [], specs: [{key,value}], yahoo: {...}|null, image_count }]
 */
export function listGenerationQueue(db, { limit = 50, ids = null } = {}) {
  // amazon_url / asin も返す: ゲートは「公式URL / 参考URL / Amazon URL のどれか」なので
  // Amazon URL だけで通過した draft の参照元がキューから欠けないように (Codex R1 high)
  // ids 指定時はその draft を直接引く (claim 応答用。一覧の LIMIT に依存させない — Codex R2 medium)
  const drafts = ids
    ? db.prepare(`
        SELECT id, ne_code, name, official_url, amazon_url, asin, price, jan_code, drive_folder_url, own_brand
        FROM product_drafts WHERE status = 'ready_for_ai' AND generation_block_code IS NULL
          AND checking_since IS NULL
          AND id IN (${ids.map(() => '?').join(',') || 'NULL'})
        ORDER BY updated_at ASC
      `).all(...ids)
    : db.prepare(`
        SELECT id, ne_code, name, official_url, amazon_url, asin, price, jan_code, drive_folder_url, own_brand
        FROM product_drafts WHERE status = 'ready_for_ai' AND generation_block_code IS NULL
          AND checking_since IS NULL
        ORDER BY updated_at ASC LIMIT ?
      `).all(limit);
  const refStmt = db.prepare('SELECT url FROM draft_reference_urls WHERE draft_id = ? ORDER BY sort, id');
  const specStmt = db.prepare('SELECT spec_key, spec_value FROM draft_specs WHERE draft_id = ? ORDER BY sort, id');
  const yahooStmt = db.prepare('SELECT yahoo_price, yahoo_price_sagawa, delivery_label, tax_rate, yahoo_category_id, yahoo_path FROM draft_yahoo WHERE draft_id = ?');
  const imgStmt = db.prepare('SELECT COUNT(*) AS c FROM draft_images WHERE draft_id = ?');
  return drafts.map((d) => ({
    ...d,
    reference_urls: refStmt.all(d.id).map((r) => r.url),
    specs: specStmt.all(d.id).map((s) => ({ key: s.spec_key, value: s.spec_value })),
    yahoo: yahooStmt.get(d.id) || null,
    image_count: imgStmt.get(d.id).c,
  }));
}

/**
 * draft から ASIN を取り出す (2026-08-04)。asin 列優先、無ければ amazon_url の /dp/ から抽出。
 * AI 生成の材料として Amazon 広告の推奨キーワードを引くために使う。
 */
export function extractAsin(draft) {
  const direct = String(draft?.asin || '').trim().toUpperCase();
  if (/^[A-Z0-9]{10}$/.test(direct)) return direct;
  const m = String(draft?.amazon_url || '').match(/\/dp\/([A-Z0-9]{10})(?:[/?#]|$)/i);
  return m ? m[1].toUpperCase() : null;
}

/**
 * SP広告マニュアルKWのスナップショット (2026-08-04)。
 * Amazon Ads の全件取得は5〜10分かかり claim のタイムアウト (15s) に間に合わないため、
 * 取得成功時に ph_intake_state へ保存し、以後の claim は即時に使う (stale-while-revalidate)。
 * KW はめったに変わらないので TTL は7日。
 */
export const SP_KW_SNAPSHOT_KEY = 'sp_keywords_snapshot';
export const SP_KW_SNAPSHOT_TTL_MS = 7 * 24 * 60 * 60 * 1000;

export function saveSpKeywordSnapshot(db, byAsin) {
  const obj = {};
  for (const [asin, kws] of byAsin) obj[asin] = [...kws];
  db.prepare(`INSERT INTO ph_intake_state (key, value) VALUES (?, ?)
              ON CONFLICT(key) DO UPDATE SET value = excluded.value`)
    .run(SP_KW_SNAPSHOT_KEY, JSON.stringify({ fetchedAt: new Date().toISOString(), byAsin: obj }));
}

/** @returns {Map<string, string[]>|null} TTL内のスナップショット (無ければ null) */
export function loadSpKeywordSnapshot(db, { ttlMs = SP_KW_SNAPSHOT_TTL_MS } = {}) {
  const row = db.prepare('SELECT value FROM ph_intake_state WHERE key = ?').get(SP_KW_SNAPSHOT_KEY);
  if (!row?.value) return null;
  try {
    const parsed = JSON.parse(row.value);
    const fetchedAt = Date.parse(parsed?.fetchedAt);
    // NaN は比較が常に false になり無期限に受理されてしまう (Codex R2 low) → 明示的に弾く
    if (!Number.isFinite(fetchedAt) || Date.now() - fetchedAt > ttlMs) return null;
    return new Map(Object.entries(parsed.byAsin || {}));
  } catch (_) { return null; }
}

// ─── P2: AI生成の claim/lease (2026-08-03、Codex設計相談の Critical/High 対応) ───

export const GENERATION_LEASE_MINUTES = 30;

/**
 * 生成待ち draft を run_id で claim して材料付きで返す (取得と排他を1回で)。
 * 対象 = status='ready_for_ai' かつ (未claim or lease切れ)。CAS UPDATE の changes=1 だけ採用。
 * ハングした実行の claim は lease (30分) が切れれば別 run が取り直せる。
 */
export function claimGenerationDrafts(db, runId, { limit = 2 } = {}) {
  const now = new Date().toISOString();
  const until = new Date(Date.now() + GENERATION_LEASE_MINUTES * 60_000).toISOString();
  // 人の確認待ち (generation_block_code) と 確認中 (checking_since) は候補から外す。
  // **CAS UPDATE 側にも同じ条件を入れる** — SELECT と UPDATE の隙間で block / 確認中に
  // された draft を掴めてしまう (Codex設計相談 Critical)
  const candidates = db.prepare(`
    SELECT id FROM product_drafts
    WHERE status = 'ready_for_ai' AND generation_block_code IS NULL AND checking_since IS NULL
      AND (generation_claim_until IS NULL OR generation_claim_until < ?)
    ORDER BY updated_at ASC LIMIT ?
  `).all(now, limit);
  const claimed = [];
  for (const c of candidates) {
    const info = db.prepare(`
      UPDATE product_drafts SET generation_claim_run_id = ?, generation_claim_until = ?
      WHERE id = ? AND status = 'ready_for_ai' AND generation_block_code IS NULL AND checking_since IS NULL
        AND (generation_claim_until IS NULL OR generation_claim_until < ?)
    `).run(runId, until, c.id, now);
    if (info.changes === 1) claimed.push(c.id);
  }
  if (claimed.length === 0) return { claimed: [], leaseUntil: until };
  return { claimed: listGenerationQueue(db, { ids: claimed }), leaseUntil: until };
}

/**
 * 書き込み直前の「書き込み権の原子的な再取得」(Codex R2 high)。
 * 事前チェック (generationClaimError) と UPSERT の間に lease 切れ・再claim が起きても、
 * この条件付き UPDATE (changes=1) を通らない限り一切書き込まない。
 * 成功時は lease を延長する (書き込み中の失効を防ぐ)。
 */
export function acquireGenerationWriteLock(db, draftId, runId) {
  const now = new Date().toISOString();
  const until = new Date(Date.now() + GENERATION_LEASE_MINUTES * 60_000).toISOString();
  // block 済みなら書き込み権を渡さない: 同じ run が「出力保存」と「人待ち」を並行して送った場合に
  // block 済みの draft へ文章が入る取り違えを防ぐ (Codex設計相談 Critical)。
  // 確認中も同じ — claim 済みの生成が走っている最中に人が確認中にしたら、その結果は書かせない
  return db.prepare(`
    UPDATE product_drafts SET generation_claim_until = ?
    WHERE id = ? AND status = 'ready_for_ai' AND generation_block_code IS NULL AND checking_since IS NULL
      AND generation_claim_run_id = ? AND generation_claim_until >= ?
  `).run(until, draftId, runId, now).changes === 1;
}

/**
 * 書き込み前の claim 検証 (Codex: claimしていない draft には書けないこと)。
 * @returns {null | string} null = OK / それ以外 = 拒否理由
 */
export function generationClaimError(draft, runId) {
  if (!runId) return 'run_id が必要です (先に claim してください)';
  if (draft.status !== 'ready_for_ai') {
    return `status が ready_for_ai ではありません (${draft.status})。人がレビュー中の可能性があるため書き込みません`;
  }
  // claim 後に人が「確認中」にした場合。書けない理由を具体的に返す
  // (acquireGenerationWriteLock でも弾かれるが、そこだと「claim 切れ」に見えて原因が追えない)
  if (draft.checking_since) {
    return '人が「確認中」にした draft です (情報待ちのため書き込みません)';
  }
  if (draft.generation_claim_run_id !== runId) return 'この draft は別の実行が claim しています';
  if (!draft.generation_claim_until || draft.generation_claim_until < new Date().toISOString()) {
    return 'claim の有効期限が切れています (claim し直してください)';
  }
  return null;
}

/** claim の解放 (生成を断念した draft を他の実行がすぐ拾えるように)。run_id 一致時のみ */
export function releaseGenerationClaim(db, draftId, runId) {
  return db.prepare(`
    UPDATE product_drafts SET generation_claim_run_id = NULL, generation_claim_until = NULL
    WHERE id = ? AND generation_claim_run_id = ?
  `).run(draftId, runId).changes === 1;
}

/**
 * AI ランナーが draft を「人の確認待ち」にする (2026-08-28 夜間自動化)。
 * release との違い: release は「一時的に手放す」(次の claim でまた先頭に戻る) のに対し、
 * これは **人が解除するまで claim 対象から外す**。仕様不一致 (入数の食い違い等) は何度 claim しても
 * 生成できないので、release で回すと毎晩同じ draft を掴んで捨てる無限ループになる。
 *
 * 書けるのは **有効な claim を持つ run だけ**。検証と書き込みは 1 本の条件付き UPDATE で行う
 * (事前に読んでから無条件 UPDATE すると、その間の lease 切れ・解除・別 claim を取りこぼす — Codex Critical)。
 * 同じ run が同じ code で再送してきたら「適用済み」として成功扱い (通信断で応答だけ失った再試行を 409 にしない)。
 * @returns {{result: 'blocked'|'already'|'conflict'|'not_found', error?: string}}
 */
export function blockGenerationDraft(db, draftId, runId, { code, reason }) {
  const id = Number(draftId);
  const by = `ai:${runId}`;
  return db.transaction(() => {
    const now = new Date().toISOString();
    const info = db.prepare(`
      UPDATE product_drafts
      SET generation_block_code = ?, generation_block_reason = ?, generation_blocked_at = ?, generation_blocked_by = ?,
          generation_claim_run_id = NULL, generation_claim_until = NULL,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ? AND status = 'ready_for_ai' AND generation_block_code IS NULL AND checking_since IS NULL
        AND generation_claim_run_id = ? AND generation_claim_until >= ?
    `).run(code, reason, now, by, id, runId, now);
    if (info.changes === 1) {
      logEvent(db, id, 'generation_blocked', `${code}: ${reason}`, by);
      return { result: 'blocked' };
    }
    // checking_since も引く: 確認中の draft を block しようとしたとき、下の generationClaimError が
    // 「人が確認中にした draft です」と具体的な理由を返せる (Codex R2 low)
    const cur = db.prepare(`
      SELECT status, generation_block_code, generation_block_reason, generation_blocked_by,
             generation_claim_run_id, generation_claim_until, checking_since
      FROM product_drafts WHERE id = ?
    `).get(id);
    if (!cur) return { result: 'not_found' };
    if (cur.generation_block_code) {
      // 冪等は「同一操作の再送」に限る: run・code・reason が全部同じときだけ already。
      // reason 違いまで成功扱いにすると、ランナー側のバグで理由が変わったのを隠す (Codex R1 medium)
      if (cur.generation_blocked_by === by && cur.generation_block_code === code && cur.generation_block_reason === reason) {
        return { result: 'already' };
      }
      return { result: 'conflict', error: `すでに人の確認待ちです (${cur.generation_block_code})。上書きしません` };
    }
    return { result: 'conflict', error: generationClaimError(cur, runId) || 'claim が無効です' };
  })();
}

/**
 * 人が「人の確認待ち」を解除する。解除 = claim 対象に戻すだけで、自動で再生成はしない
 * (人が基本情報を直した上で解除する前提。直さずに解除すれば次の夜また同じ理由で止まる = 正しい挙動)。
 * expectedBlockedAt を渡すと楽観ロック: 画面が見ていた block と違う block になっていたら解除しない
 * (Codex設計相談 medium: 長時間開いた画面からの解除で、別の理由の block を消さない)。
 * @returns {{result: 'unblocked'|'not_blocked'|'stale'|'not_found'}}
 */
export function unblockGenerationDraft(db, draftId, { actor = null, expectedBlockedAt = null } = {}) {
  const id = Number(draftId);
  return db.transaction(() => {
    const cur = db.prepare(`
      SELECT generation_block_code, generation_block_reason, generation_blocked_at FROM product_drafts WHERE id = ?
    `).get(id);
    if (!cur) return { result: 'not_found' };
    if (!cur.generation_block_code) return { result: 'not_blocked' };
    const info = db.prepare(`
      UPDATE product_drafts
      SET generation_block_code = NULL, generation_block_reason = NULL, generation_blocked_at = NULL, generation_blocked_by = NULL,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      WHERE id = ? AND generation_block_code IS NOT NULL
        AND (? IS NULL OR generation_blocked_at = ?)
    `).run(id, expectedBlockedAt, expectedBlockedAt);
    if (info.changes !== 1) return { result: 'stale' };
    logEvent(db, id, 'generation_unblocked', `${cur.generation_block_code}: ${cur.generation_block_reason || ''}`.trim(), actor);
    return { result: 'unblocked' };
  })();
}

/**
 * 生成キューの内訳 (夜間バッチの成否判定用)。
 * 「キューが減ったか」で成否を見ると、人待ちだけが残った夜を失敗扱いにしてしまう (Codex設計相談 High)。
 * ランナー/監視は claimable === 0 を「今夜やることは終わった」と読む。blocked は別枠で人に見せる。
 * claim 処理と同じスナップショットではない (観測値) — 厳密な制御には使わない
 */
export function generationQueueSummary(db) {
  const now = new Date().toISOString();
  // 確認中 (checking) は claimable から外す — 実際に claim できない分を「生成待ち」に数えると、
  // キューが減らない理由が読めなくなる。checking は独立した内訳として返す
  const r = db.prepare(`
    SELECT
      SUM(CASE WHEN generation_block_code IS NULL AND checking_since IS NULL AND (generation_claim_until IS NULL OR generation_claim_until < ?) THEN 1 ELSE 0 END) AS claimable,
      SUM(CASE WHEN generation_block_code IS NULL AND checking_since IS NULL AND generation_claim_until >= ? THEN 1 ELSE 0 END) AS leased,
      SUM(CASE WHEN generation_block_code IS NOT NULL THEN 1 ELSE 0 END) AS blocked,
      SUM(CASE WHEN generation_block_code IS NULL AND checking_since IS NOT NULL THEN 1 ELSE 0 END) AS checking
    FROM product_drafts WHERE status = 'ready_for_ai'
  `).get(now, now);
  const blockedByCode = {};
  for (const row of db.prepare(`
    SELECT generation_block_code AS code, COUNT(*) AS c FROM product_drafts
    WHERE status = 'ready_for_ai' AND generation_block_code IS NOT NULL GROUP BY generation_block_code
  `).all()) blockedByCode[row.code] = row.c;
  return {
    claimable: r?.claimable || 0, leased: r?.leased || 0, blocked: r?.blocked || 0,
    checking: r?.checking || 0, blockedByCode,
  };
}

// demoteIfGateBroken は lib/workflow-progress.js へ移設 (PR4):
// status を直接書かず「基本情報入力」工程を差し戻して導出に任せる形に変わったため

/**
 * サムネイルプロキシ (/api/thumb) の取得対象を「product-hub が管理している画像」に限定する。
 * SA は Drive の広い範囲を読めるため、形式チェックだけだと任意の Drive ID を
 * SA 権限で覗ける confused-deputy になる (Codex R1 high)。
 */
export function isKnownImageFileId(db, fileId) {
  return !!imageRefOfFileId(db, fileId);
}

/**
 * 登録済み画像なら { modifiedTime } を返す (未登録は null)。
 * modifiedTime はサムネURLの版数 (?v=) の期待値。**DB の値と一致する v だけ**を
 * キャッシュキーに採用することで、任意の v でキャッシュを汚されるのを防ぐ (Codex low)
 */
export function imageRefOfFileId(db, fileId) {
  if (!fileId) return null;
  return db.prepare(`
    SELECT drive_modified_time AS modifiedTime FROM draft_images WHERE drive_file_id = ?
    UNION ALL
    SELECT white_bg_modified_time FROM draft_rakuten WHERE white_bg_drive_file_id = ?
    UNION ALL
    SELECT drive_modified_time FROM draft_sku_images WHERE drive_file_id = ?
    LIMIT 1
  `).get(fileId, fileId, fileId) || null;
}

/**
 * 画像フォルダ一括取り込みの結果 (assignImageSlots の戻り値) を DB に反映する。
 *   - slots があるときだけ draft_images を全置き換え (sort = スロット番号 - 1)。
 *     白抜きだけ見つかった場合は既存の商品画像を触らない
 *   - whiteBg があれば draft_rakuten の白抜き背景を upsert (他カラムは触らない)
 *   - フォルダURLが基本情報と違えば product_drafts.drive_folder_url も更新
 */
export function applyFolderImport(db, draftId, assigned, { folderUrl = null, currentFolderUrl = null } = {}) {
  db.transaction(() => {
    if (assigned.slots.length > 0) {
      db.prepare('DELETE FROM draft_images WHERE draft_id = ?').run(draftId);
      const ins = db.prepare(
        'INSERT INTO draft_images (draft_id, drive_file_id, drive_url, sort, drive_modified_time, source_label) VALUES (?, ?, ?, ?, ?, ?)',
      );
      for (const s of assigned.slots) ins.run(draftId, s.id, fileViewUrl(s.id), s.slot - 1, s.modifiedTime || null, s.label || null);
    }
    if (assigned.whiteBg) {
      db.prepare(`
        INSERT INTO draft_rakuten (draft_id, white_bg_drive_file_id, white_bg_drive_url, white_bg_modified_time) VALUES (?, ?, ?, ?)
        ON CONFLICT(draft_id) DO UPDATE SET
          white_bg_drive_file_id = excluded.white_bg_drive_file_id,
          white_bg_drive_url = excluded.white_bg_drive_url,
          white_bg_modified_time = excluded.white_bg_modified_time,
          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now')
      `).run(draftId, assigned.whiteBg.id, fileViewUrl(assigned.whiteBg.id), assigned.whiteBg.modifiedTime || null);
    }
    if (folderUrl && folderUrl !== currentFolderUrl) {
      db.prepare(`UPDATE product_drafts SET drive_folder_url = ?, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ','now') WHERE id = ?`)
        .run(folderUrl, draftId);
      // 商品リンク台帳へ同一トランザクションで写す (2026-08-27)
      syncDraftLinks(db, draftId, { actor: 'auto:folder_import' });
    }
  })();
}
