/**
 * LP 構成の AI 生成 — 段階1 (2026-10-01)
 *
 * 正本 = AI_reference『商品ハブ_LP構成AI生成_段階1設計_20260930.md』。
 * 型は SP広告KW の夜間 AI (`lib/ad-kw-ai.js`) の manual を縮小して写した。**新しい型を作らない。**
 * DB は 3 表 (ph_lp_specs / ph_lp_compose_jobs / ph_lp_compose_generations、db.js)。
 * 画面の API と miniPC の実行役用の service-api は router.js。ここは DB とロジックだけ。
 *
 * 段階1 の目的は**機能ではなく測定** — 「AI が作った LP 構成は、スタッフが ChatGPT で
 * 作っているものと比べて使えるか」を、キューも予算台帳も夜間バッチも作らずに確かめる。
 * 画像生成・GAS への送信・夜間実行・撮影依頼書・簡易LP は段階2 以降。
 *
 * 守りたいこと (Codex R1〜R4 の指摘から。番号は設計書 §4.3 / §4.3b):
 *   ① **材料 (packet) は受付時に固定する。** claim は常に同じ packet を返す。
 *      仕様書は「最新版」ではなく受付時の spec_id を引き、hash が合わなければ job を止める
 *      (依頼から claim までに仕様書が差し替わると、packet_hash が同じまま中身が変わる)
 *   ② **AI を呼ぶ前にサーバーで予約する** (generation)。予約後に結果が来なければ needs_review。
 *      成否不明を自動で作り直さない
 *   ③ **AI を呼んだ後の終了は必ず generation の確定を通す** (accepted / rejected)。
 *      job だけ進んで generation が reserved のまま残る経路を作らない。
 *      fail / release は**予約の前だけ**
 *   ④ lease が切れたあとに許すのは generation による結果の復旧だけ
 *   ⑤ 機能フラグ PH_LP_COMPOSE_ENABLED=1 が無ければ受付・claim・reserve を受けない (fail-closed)。
 *      予約済みの結果の復旧は受ける (AI 枠を消費済みなので取りこぼさない)
 *   ⑥ **終端では必ず completed_at を入れる。** measurement_deadline_at (受付 + 3 分) と対にして、
 *      「3 分で終わったか」を後から計算する。lease は 40 分なので、これが無いと
 *      「3 分では終わらなかったが後で成功した」job が所要時間の母数から抜ける
 */
import { createHash, randomBytes } from 'node:crypto';
import { logEvent } from '../db.js';
import { lintComposition, lintSummary } from './lp-lint.js';
import { PRODUCT_ANALYSIS_INSTRUCTION } from './prompt-templates.js';

/*
 * 書き込みを伴うトランザクションは `.immediate()` で回す (コード R11)。
 * better-sqlite3 の既定は DEFERRED で、先頭が SELECT だと書き込みロックを取らない。
 * 別プロセスと同時に走ると「両方とも既存 job なし」と判断したあと片方が SQLITE_BUSY になり、
 * 一意制約違反の救済 (isUniqueViolation) では拾えない。
 * 入れ子の呼び出しは savepoint になるので immediate かどうかは影響しない。
 * 同じ作法が apps/amazon-pricing/db.js にある。
 */

// 2: スタッフの定型文と**同じ指示文** (instruction) を packet に入れた (設計 §5)
export const PACKET_VERSION = 2;
export const PROMPT_VERSION = 'lp-compose-v1';
export const LEASE_MIN = 40;
/** 測定の合格ライン (設計 §7.2)。受付時に created_at + これで deadline を固定する */
export const MEASUREMENT_WINDOW_MIN = 3;
export const MAX_IMAGES = 6;
/** 実行役へ配る商品画像の幅。画面用の THUMB_WIDTHS (160/320) では AI が
 *  ラベル文字や商品形状を判断できない (仕様書の「商品再現ルール」を守れない) */
export const LP_COMPOSE_IMAGE_WIDTH = 1024;
/** ⑦ の全文。V2.2 は画像 1 枚あたり 15 見出しあるので 10 枚で 8 万字程度。余裕を見る */
export const OUTPUT_MAX = 200_000;
export const SPEC_BODY_MAX = 500_000;
export const REASON_MAX = 1000;
/** 1 日の AI 呼び出しの上限。段階1 は 10 件の測定なので小さく始める (env で上げられる) */
const DEFAULT_DAILY_CAP = 20;

export const SPEC_KINDS = ['product_analysis'];
export const JOB_STATUSES = ['queued', 'running', 'done', 'needs_review', 'failed', 'cancelled'];
/** 人が次に何をするかが変わる終端だけ。cancelled は画面に出さない */
export const TERMINAL_STATUSES = ['done', 'needs_review', 'failed', 'cancelled'];

export function lpComposeEnabled() { return process.env.PH_LP_COMPOSE_ENABLED === '1'; }

/**
 * 構成を書く Claude のモデル。**決める場所は Render の PH_LP_COMPOSE_MODEL だけ** (2026-10-02 中原さん)。
 * queue / claim がこの値を返し、miniPC のランナーはそれを `claude --model` に渡す。
 * reserve はこれと違うモデルを断る (別の条件で作ったものが測定に混ざらないように。prompt_version と同じ扱い)。
 * 画面のボタンにも出す。
 * 🚨 以前は実行役の自己申告 (既定値 'claude-opus-5' の決め打ち) を記録していて、実際に動いたモデルと関係が無かった。
 * 🚨 Opus 5.5 は Claude Code 2.1.280 以上が要る (miniPC は 2026-10-02 に 2.1.280 へ上げた)。
 *    [1m] = 1M コンテキスト。仕様書の全文 + 画像 6 枚 + ⑦ の全文で 20 万トークンを超えうる
 */
export const DEFAULT_MODEL = 'claude-opus-5-5[1m]';
const MODEL_RE = /^claude-[a-z0-9]+(?:-[a-z0-9]+){1,6}(?:\[1m\])?$/;
export function lpComposeModel() {
  return exact(process.env.PH_LP_COMPOSE_MODEL, MODEL_RE) || DEFAULT_MODEL;
}
/** 'claude-opus-5-5[1m]' → 'Opus 5.5' / 'claude-haiku-4-5-20251001' → 'Haiku 4.5'。読めない形はそのまま返す */
export function modelLabel(id) {
  const m = /^claude-([a-z]+)((?:-\d{1,2})+)(?:-\d{8})?(?:\[1m\])?$/.exec(String(id || ''));
  if (!m) return String(id || '');
  return m[1].charAt(0).toUpperCase() + m[1].slice(1) + ' ' + m[2].slice(1).split('-').join('.');
}

export function dailyCap() {
  const n = Number.parseInt(process.env.PH_LP_COMPOSE_DAILY_CAP || '', 10);
  return Number.isInteger(n) && n > 0 && n <= 200 ? n : DEFAULT_DAILY_CAP;
}

export function sha256(s) { return createHash('sha256').update(String(s), 'utf8').digest('hex'); }

/** キーの順を固定した JSON (hash を安定させる。ad-kw-ai と同じ考え方) */
export function canonicalJson(v) {
  if (v === null || typeof v !== 'object') return JSON.stringify(v);
  if (Array.isArray(v)) return `[${v.map(canonicalJson).join(',')}]`;
  return `{${Object.keys(v).sort().map((k) => `${JSON.stringify(k)}:${canonicalJson(v[k])}`).join(',')}}`;
}

/** JST の日付 (1 日の上限を数える単位。夜間と手動が同じ境界を使う) */
function jstDay(now) {
  return new Date(now + 9 * 3600_000).toISOString().slice(0, 10);
}

const trim = (v, max) => {
  const s = v == null ? '' : String(v).trim();
  return max && s.length > max ? s.slice(0, max) : s;
};

/**
 * 正の安全な整数か (SQLite の非 STRICT 表は文字列も入るので、入口で正規化する。コード R1 #4)。
 * 🚨 `Number.parseInt` は使わない — "12abc" / "12.9" / "12 " を 12 として通してしまい、
 *    入力と違う draft / spec を指す job を作れる (コード R2 #2)。
 */
const posInt = (v) => {
  if (typeof v === 'number') return Number.isSafeInteger(v) && v > 0 ? v : null;
  // 🚨 trim してから検査しない — " 12" と "12" を同じ 12 に畳むと、入力と保存値が食い違う (コード R3)
  // test() だと末尾に改行が付いた値まで通ってしまう (コード R6)
  if (!exact(v, POS_INT_RE)) return null;
  const n = Number(v);
  return Number.isSafeInteger(n) && n > 0 ? n : null;
};

/** sha256 (16 進小文字 64 桁ちょうど)。証跡・packet_hash・spec_hash の照合に使う */
const SHA256_RE = /^[0-9a-f]{64}$/;
const POS_INT_RE = /^[1-9]\d*$/;
/** Drive の fileId (lib/drive-link.js の DRIVE_FILE_ID_PATTERN と同じ形) */
const DRIVE_FILE_ID_RE = /^[-\w]{10,200}$/;
/** 二重クリック対策のキー。画面が UUID などを作る。空白差で別物にならないよう形を固定する */
const IDEMPOTENCY_KEY_RE = /^[A-Za-z0-9_.:-]{8,80}$/;

/**
 * 識別子・ハッシュは **trim も切り詰めもしない**で形を直接見る (コード R3〜R5)。
 * 正規化して受けると、送られた値と保存・照合に使う値が食い違い、
 * 「別の入力」が「同じ結果の再送」に畳まれる。
 *
 * 🚨 `re.test(v)` では**末尾の改行を弾けない** — JS の `$` は (multiline でなくても)
 *    文字列末尾の `\n` の直前にも一致するので `"abc\n"` が `/^abc$/` を通る。
 *    一致した部分が文字列全体と同じかで確かめる (コード R6)。
 */
const exact = (v, re) => {
  if (typeof v !== 'string') return null;
  const m = re.exec(v);
  return m && m[0] === v ? v : null;
};

/**
 * 実行役から来た JSON を保存できる形にする。
 * 直列化できない・大きすぎるなら `false` を返す (呼び手が bad_request にする)。
 * 壊れた実行役が DB を肥らせたり、例外を API まで漏らしたりしないための入口検査 (コード R1 #5)。
 */
function jsonOrNull(v, max) {
  if (v == null) return null;
  let s;
  try { s = JSON.stringify(v); } catch { return false; }
  if (typeof s !== 'string') return false;
  return s.length > max ? false : s;
}

/** SQLite の一意制約違反か (better-sqlite3 は code に SQLITE_CONSTRAINT_* を入れる) */
const isUniqueViolation = (e) => String(e?.code || '').startsWith('SQLITE_CONSTRAINT');

export const LINT_MAX = 100_000;
export const RECEIPT_IMAGES_MAX = MAX_IMAGES;

// ─── 仕様書のスナップショット ────────────────────────────────

/**
 * 仕様書を取り込む。**追記専用** — 同じ中身なら既存の版を返す (版を無駄に進めない)。
 * body は「AI へ渡す全文」そのもの。hash は body だけでなく**タブ名と順序も含めて**計算する
 * (Codex R4 #4: body が同じでもタブの並びが変われば AI への実効入力が変わりうる)。
 * @returns {{ok:true, spec, created:boolean}|{code:string, error:string}}
 */
export function importSpec(db, { kind, title, body, sheetTitles = [], actor } = {}) {
  const k = trim(kind, 40);
  if (!SPEC_KINDS.includes(k)) return { code: 'bad_kind', error: `仕様書の種類は ${SPEC_KINDS.join(' / ')} だけです` };
  const t = trim(title, 200);
  const b = String(body == null ? '' : body);
  if (!t) return { code: 'bad_request', error: '仕様書の名前が要ります' };
  if (!b.trim()) return { code: 'bad_request', error: '仕様書の中身が空です' };
  if (b.length > SPEC_BODY_MAX) return { code: 'too_large', error: `仕様書が大きすぎます (${SPEC_BODY_MAX} 文字まで)` };
  const titles = Array.isArray(sheetTitles) ? sheetTitles.map((s) => trim(s, 120)).filter(Boolean) : [];
  const hash = sha256(canonicalJson({ body: b, sheet_titles: titles }));
  const a = trim(actor, 120) || 'unknown';
  return db.transaction(() => {
    const hit = db.prepare('SELECT * FROM ph_lp_specs WHERE kind = ? AND hash = ?').get(k, hash);
    if (hit) return { ok: true, spec: hit, created: false };
    let id;
    try {
      id = Number(db.prepare(`INSERT INTO ph_lp_specs (kind, title, body, hash, sheet_titles_json, imported_by)
        VALUES (?, ?, ?, ?, ?, ?)`).run(k, t, b, hash, JSON.stringify(titles), a).lastInsertRowid);
    } catch (e) {
      // 同時に同じ中身を上げた = 相手の行を返す (SELECT→INSERT の競合。コード R1 #3)
      if (!isUniqueViolation(e)) throw e;
      const raced = db.prepare('SELECT * FROM ph_lp_specs WHERE kind = ? AND hash = ?').get(k, hash);
      if (raced) return { ok: true, spec: raced, created: false };
      throw e;
    }
    return { ok: true, spec: db.prepare('SELECT * FROM ph_lp_specs WHERE id = ?').get(id), created: true };
  }).immediate();
}

/** いまの版 (= 最大 id)。無ければ null */
export function latestSpec(db, kind = 'product_analysis') {
  return db.prepare('SELECT * FROM ph_lp_specs WHERE kind = ? ORDER BY id DESC LIMIT 1').get(kind) || null;
}

/** 画面に出す「仕様書: ○○ (YYYY-MM-DD 取込)」。本文は返さない (画面に全文は要らない) */
export function specSummary(db, kind = 'product_analysis') {
  const s = latestSpec(db, kind);
  if (!s) return null;
  return {
    id: s.id, title: s.title, imported_at: s.imported_at, imported_by: s.imported_by,
    hash_short: String(s.hash).slice(0, 12),
    sheet_titles: JSON.parse(s.sheet_titles_json || '[]'),
    chars: s.body.length,
  };
}

// ─── 受付 ────────────────────────────────────────────────

/**
 * 受付時に固定する材料。
 * **文字列はすべて画面と同じ組み立て済みのものを受け取る** — `lib/prompt-templates.js` の
 * composeProductInfo / composeColorVariations が正本で、ここで組み直さない (二重に持たない)。
 */
/**
 * packet に入る商品画像 (空の ID・重複を除いた最大 MAX_IMAGES 枚)。
 * 受付の判定 (requestBlockReason) も**この結果の枚数**で見る — 配列の長さで見ると
 * `[{}]` や空の ID が通り、packet の画像は 0 枚になる (codex exec review #1591 Low)
 */
function normalizeImages(images) {
  // file_id は重複させない (コード R9)。証跡 (receipt.images) 側は重複を禁じているので、
  // packet に同じ画像が 2 回あると「渡した材料」と「見た証跡」が 1 対 1 で対応しなくなる
  const seen = new Set();
  const imgs = [];
  for (const im of Array.isArray(images) ? images : []) {
    const fileId = trim(im?.file_id || im?.drive_file_id, 200);
    if (!fileId || seen.has(fileId)) continue;
    seen.add(fileId);
    imgs.push({
      file_id: fileId,
      // Drive の更新日時。何を見て作ったかを後から辿るために packet に残す (設計 §4.2)
      modified_time: trim(im?.modified_time || im?.drive_modified_time, 40) || null,
    });
    if (imgs.length >= MAX_IMAGES) break;
  }
  return imgs;
}

export function buildPacket({ draft, productInfo, colorVariations, images = [], spec }) {
  const imgs = normalizeImages(images);
  const packet = {
    packet_version: PACKET_VERSION,
    // 🚨 スタッフが ChatGPT に貼る定型文の【実行】と**同じ文**を渡す (設計 §5)。
    //    正本は lib/prompt-templates.js の PRODUCT_ANALYSIS_INSTRUCTION だけ。
    //    段階1 の測定は「同じ入力から作った二つを比べる」のが前提。
    //    指示文が片方だけ違うと、比べているのが「AI の力の差」なのか
    //    「指示文の差」なのか分からなくなる。
    //    packet に入れる = 受付時に固定され、packet_hash で守られる
    instruction: PRODUCT_ANALYSIS_INSTRUCTION,
    draft_id: Number(draft.id),
    ne_code: trim(draft.ne_code, 100),
    name: trim(draft.name, 300),
    product_info: trim(productInfo, 20_000),
    color_variations: trim(colorVariations, 4_000),
    images: imgs,
    spec_kind: spec.kind,
    spec_id: Number(spec.id),
    spec_hash: spec.hash,
  };
  return { packet, hash: sha256(canonicalJson(packet)) };
}

/** 受け付けられる材料が揃っているか (画面のボタンの活性条件と同じ) */
export function requestBlockReason({ draft, productInfo, spec, images }) {
  if (!spec) return '仕様書がまだ取り込まれていません (管理画面から取り込んでください)';
  if (!trim(draft?.name)) return '商品名が未入力です';
  if (!trim(productInfo)) return '「商品情報」か「裏面情報」を入力して保存すると使えます';
  // 🚨 画像が無いと AI は必ず IMAGES_UNAVAILABLE で止まる (スキルの決まり: 見ずに書かない)。
  //    受け付けると 4 分待たせてから失敗する (2026-10-02 の 1 件目 = draft 188)。押す前に止める。
  //    呼び手は必ず images を渡す (渡し忘れ = 配列でない も「無い」と同じに扱う)
  if (normalizeImages(images).length === 0) return '商品画像がありません (画像タブに商品画像を入れると使えます)';
  return null;
}

/**
 * 依頼を受け付ける。**ここで packet を固定する。**
 * 同じ draft + idempotency_key は既存の job を返す (二重クリック・通信リトライで増やさない)。
 * @returns {{ok:true, job, created:boolean}|{code:string, error:string}}
 */
export function requestJob(db, { draft, productInfo, colorVariations, images, spec, idempotencyKey, actor, now = Date.now() } = {}) {
  if (!lpComposeEnabled()) return { code: 'disabled', error: 'PH_LP_COMPOSE_ENABLED が無効です' };
  // 切り詰めない — 81 文字目以降だけ違うキーを同一視すると、別の依頼を既存 job として返す (コード R5)
  const key = exact(idempotencyKey, IDEMPOTENCY_KEY_RE);
  if (!key) return { code: 'bad_request', error: 'idempotency_key の形が不正です (英数記号 8〜80 文字)' };
  // 🚨 ID は入口で正規化して、packet・検索・INSERT・ログで同じ値だけを使う。
  //    SQLite の非 STRICT 表は文字列も受けるので、検証しないと packet と行で ID が食い違う (コード R1 #4)
  const draftId = posInt(draft?.id);
  if (!draftId) return { code: 'bad_request', error: '商品の ID が不正です' };
  const a = trim(actor, 120) || 'unknown';
  const nowS = new Date(now).toISOString();
  const deadline = new Date(now + MEASUREMENT_WINDOW_MIN * 60_000).toISOString();
  return db.transaction(() => {
    // 🚨 **同じキーの再送は、材料の検証より先に**既存の job を返す。しかも
    //    トランザクションの**冒頭**で見る (コード R9・R10)。外で見ると、その後に別の接続が
    //    同じキーで作った job を見落とし、再送側が not_ready / bad_request になる。
    //    画面は「作れなかった」と見えるのに裏では job が動いている、という食い違いが起きる
    const prior = db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE draft_id = ? AND idempotency_key = ?').get(draftId, key);
    if (prior) return { ok: true, job: prior, created: false };
    const blocked = requestBlockReason({ draft, productInfo, spec, images });
    if (blocked) return { code: 'not_ready', error: blocked };
    const specId = posInt(spec?.id);
    if (!specId) return { code: 'bad_request', error: '仕様書の ID が不正です' };
    // 渡された spec を信じず、DB から引き直して種類と hash を照合する (コード R1 #4)
    // hash は**必須**。省略を許すと ID と kind だけで通り、照合の意味が無くなる (コード R2 #3)
    const specRow = db.prepare('SELECT * FROM ph_lp_specs WHERE id = ?').get(specId);
    if (!specRow || specRow.kind !== 'product_analysis' || exact(spec?.hash, SHA256_RE) !== specRow.hash) {
      return { code: 'bad_request', error: '仕様書の版が見つかりません (取り込み直してください)' };
    }
    const { packet, hash } = buildPacket({ draft: { ...draft, id: draftId }, productInfo, colorVariations, images, spec: specRow });
    recoverExpired(db, now);
    const live = db.prepare(`SELECT * FROM ph_lp_compose_jobs WHERE draft_id = ? AND status IN ('queued','running')`).get(draftId);
    if (live) return { code: 'already_running', error: 'この商品の構成をいま作っています', job: live };
    let id;
    try {
      id = Number(db.prepare(`INSERT INTO ph_lp_compose_jobs
        (draft_id, idempotency_key, status, packet_json, packet_hash, packet_version, spec_id, spec_hash,
         requested_by, measurement_deadline_at, created_at, updated_at)
        VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(draftId, key, JSON.stringify(packet), hash, PACKET_VERSION, specId, specRow.hash, a, deadline, nowS, nowS).lastInsertRowid);
    } catch (e) {
      // SELECT と INSERT の間に別の接続が入った (二重クリック・リトライ)。
      // 例外にせず、相手が作った行 / 動いている依頼を返す (コード R1 #3)
      if (!isUniqueViolation(e)) throw e;
      const raced = db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE draft_id = ? AND idempotency_key = ?').get(draftId, key);
      if (raced) return { ok: true, job: raced, created: false };
      const racedLive = db.prepare(`SELECT * FROM ph_lp_compose_jobs WHERE draft_id = ? AND status IN ('queued','running')`).get(draftId);
      if (racedLive) return { code: 'already_running', error: 'この商品の構成をいま作っています', job: racedLive };
      throw e;
    }
    logEvent(db, draftId, 'lp_compose_requested', `依頼 ${id} (仕様書 v${specId})`, a);
    return { ok: true, job: db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(id), created: true };
  }).immediate();
}

// ─── 実行役 (miniPC) ─────────────────────────────────────

// 外部から来た ID も posInt を通す。Number() だと " 12" や "" が別の数値になる (コード R4)
const jobById = (db, id) => { const n = posInt(id); return n ? db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE id = ?').get(n) || null : null; };
const genOf = (db, jobId) => { const n = posInt(jobId); return n ? db.prepare(`SELECT * FROM ph_lp_compose_generations WHERE job_id = ? AND status = 'reserved'`).get(n) || null : null; };

/**
 * lease 切れの job を終端に落とす (completed_at を必ず入れる = 設計 §7.1b・Codex R4 #2)。
 * 🚨 **`status='running' かつ lease_until < now` を UPDATE の条件に入れる** — これが無いと、
 *    SELECT と UPDATE の間に実行役が結果を出して done になった job を、後から needs_review /
 *    failed で塗り潰してしまう (`output_text` は残ったまま status だけ変わる。コード R1 #1 critical)。
 */
function expireJob(db, jobId, status, { code, message, nowS }) {
  return db.prepare(`UPDATE ph_lp_compose_jobs
    SET status = ?, error_code = ?, error = ?, lease_token = NULL, lease_until = NULL,
        updated_at = ?, completed_at = COALESCE(completed_at, ?)
    WHERE id = ? AND status = 'running' AND lease_until < ?`)
    .run(status, code, message, nowS, nowS, posInt(jobId), nowS).changes;
}

/** 終端に落とす (lease が有効な job を、その lease の持ち主が終わらせるとき) */
function finishJob(db, jobId, status, { code = null, message = null, nowS }) {
  return db.prepare(`UPDATE ph_lp_compose_jobs
    SET status = ?, error_code = ?, error = ?, lease_token = NULL, lease_until = NULL,
        updated_at = ?, completed_at = COALESCE(completed_at, ?)
    WHERE id = ? AND status = 'running'`)
    .run(status, code, message, nowS, nowS, posInt(jobId)).changes;
}

/**
 * lease が切れた job を倒す。
 * **予約済みなら needs_review** (AI を呼んだ後に結果を持ち帰れなかった = 成否不明)。
 * 段階1 は人がボタンを押して画面を見ているので、**自動で作り直さない**。
 *
 * 🚨 全体を 1 トランザクションにする。`jobStateFor` (画面のポーリング) からも呼ばれるので、
 *    ここが裸だと 5 秒ごとの読み取りが書き込みと競合する (コード R1 #1)。
 */
export function recoverExpired(db, now = Date.now()) {
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    let n = 0;
    for (const job of db.prepare(`SELECT id FROM ph_lp_compose_jobs WHERE status = 'running' AND lease_until < ?`).all(nowS)) {
      n += genOf(db, job.id)
        ? expireJob(db, job.id, 'needs_review', {
          code: 'lease_expired_after_reserve',
          message: '予約後に結果が届かないまま期限が切れた (成否不明・自動では作り直さない)', nowS,
        })
        : expireJob(db, job.id, 'failed', { code: 'lease_expired', message: '実行役の期限が切れた', nowS });
    }
    return n;
  }).immediate();
}

/** キューの要約 (実行役の「仕事なし」判定・監視用) */
export function queueSummary(db, now = Date.now()) {
  return db.transaction(() => {
    recoverExpired(db, now);
    const c = (sql, ...a) => db.prepare(sql).get(...a).n;
    return {
      enabled: lpComposeEnabled(),
      // ランナーはこれを claude --model に渡す (claude を起動する前に知る必要がある)
      model: lpComposeModel(),
      claimable: c(`SELECT COUNT(*) AS n FROM ph_lp_compose_jobs WHERE status = 'queued'`),
      running: c(`SELECT COUNT(*) AS n FROM ph_lp_compose_jobs WHERE status = 'running'`),
      needs_review: c(`SELECT COUNT(*) AS n FROM ph_lp_compose_jobs WHERE status = 'needs_review'`),
      reserved_today: c('SELECT COUNT(*) AS n FROM ph_lp_compose_generations WHERE reserved_day = ?', jstDay(now)),
      daily_cap: dailyCap(),
      oldest_queued: db.prepare(`SELECT MIN(created_at) AS t FROM ph_lp_compose_jobs WHERE status = 'queued'`).get().t,
    };
  }).immediate();
}

/**
 * 1 件 claim する。応答には**受付時に固定した packet と、その版の仕様書の全文**を入れる。
 * 🚨 仕様書は「最新版」ではなく job の spec_id で引き、spec_hash と照合する。
 *    合わなければ job を failed にして次へ (Codex R3 #3)。
 * @returns {{ok:true, job:object|null}|{code:string, error:string}}
 */
export function claimJob(db, { runnerRunId, now = Date.now() } = {}) {
  if (!lpComposeEnabled()) return { code: 'disabled', error: 'PH_LP_COMPOSE_ENABLED が無効です' };
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    recoverExpired(db, now);
    for (let guard = 0; guard < 50; guard++) {
      const job = db.prepare(`SELECT * FROM ph_lp_compose_jobs WHERE status = 'queued' ORDER BY id LIMIT 1`).get();
      if (!job) return { ok: true, job: null };
      // 🚨 claim は「材料を AI に渡す瞬間」= 材料固定という設計の芯が試される所。
      //    保存済みの hash を信じず、**中身から計算し直して**照合する (コード R7 #1)。
      //    追記専用トリガーや app の経路だけでは、DB を直接いじられたときに気づけない。
      const packetOk = (() => {
        try {
          const p = JSON.parse(job.packet_json);
          if (sha256(canonicalJson(p)) !== job.packet_hash) return false;
          // 🚨 packet と job 行の**突き合わせ**も要る (コード R8 #1)。
          //    それぞれの hash が個別に正しくても、job の spec_id / spec_hash だけ書き換えれば
          //    「受付時とは別の仕様書」を渡せてしまう
          return p.draft_id === job.draft_id
            && p.packet_version === job.packet_version
            && p.spec_id === job.spec_id
            && p.spec_hash === job.spec_hash;
        } catch { return false; }
      })();
      if (!packetOk) {
        db.prepare(`UPDATE ph_lp_compose_jobs
          SET status = 'failed', error_code = 'packet_tampered', error = ?,
              updated_at = ?, completed_at = COALESCE(completed_at, ?)
          WHERE id = ? AND status = 'queued'`)
          .run('受付時に固定した材料が変わっている (もう一度依頼してください)', nowS, nowS, job.id);
        continue;
      }
      // 🚨 古い版の packet を渡さない (codex exec review P2)。
      //    版が上がる = 渡す材料の形が変わった。v1 には instruction が入っていないので、
      //    そのまま渡すと**スタッフと違う指示文で作ったものが測定に混ざる** (設計 §5 / §7.1)。
      //    自動で作り直さない — 人がもう一度ボタンを押す (渡した材料を勝手に差し替えない)。
      if (job.packet_version !== PACKET_VERSION) {
        db.prepare(`UPDATE ph_lp_compose_jobs
          SET status = 'failed', error_code = 'packet_outdated', error = ?,
              updated_at = ?, completed_at = COALESCE(completed_at, ?)
          WHERE id = ? AND status = 'queued'`)
          .run(`渡す材料の形が更新されました (版 ${job.packet_version} → ${PACKET_VERSION})。もう一度依頼してください`, nowS, nowS, job.id);
        continue;
      }
      const spec = db.prepare('SELECT * FROM ph_lp_specs WHERE id = ?').get(job.spec_id);
      const specOk = spec
        && spec.hash === job.spec_hash
        && (() => {
          try {
            return sha256(canonicalJson({ body: spec.body, sheet_titles: JSON.parse(spec.sheet_titles_json || '[]') })) === spec.hash;
          } catch { return false; }
        })();
      if (!specOk) {
        // まだ queued なので status の条件は 'queued'。running を条件にすると 1 行も動かず、
        // 同じ job を掴み続けて claim が空回りする
        db.prepare(`UPDATE ph_lp_compose_jobs
          SET status = 'failed', error_code = 'spec_changed', error = ?,
              updated_at = ?, completed_at = COALESCE(completed_at, ?)
          WHERE id = ? AND status = 'queued'`)
          .run('受付時の仕様書の版が見つからない (取り込み直してからもう一度依頼してください)', nowS, nowS, job.id);
        continue;
      }
      const token = randomBytes(16).toString('hex');
      const until = new Date(now + LEASE_MIN * 60_000).toISOString();
      // 🚨 証跡は claim ごとにリセットする (codex exec review P1)。
      //    残しておくと、前の実行役が画像を落としてから release / 落ちた場合に、
      //    **次の実行役が画像を一度も見ずに accepted を出せてしまう**。
      //    「その出力を書いた実行役が実際に見た」を保つのがこの証跡の意味。
      const ch = db.prepare(`UPDATE ph_lp_compose_jobs
        SET status = 'running', lease_token = ?, lease_until = ?, runner_run_id = ?, claims = claims + 1,
            images_served_json = '[]', updated_at = ?
        WHERE id = ? AND status = 'queued'`)
        .run(token, until, trim(runnerRunId, 80) || null, nowS, job.id).changes;
      if (ch !== 1) continue;   // 誰かに取られた
      return {
        ok: true,
        job: {
          job_id: job.id, draft_id: job.draft_id,
          lease_token: token, lease_until: until,
          packet: JSON.parse(job.packet_json), packet_hash: job.packet_hash,
          prompt_version: PROMPT_VERSION,
          model: lpComposeModel(),
          spec: { id: spec.id, kind: spec.kind, title: spec.title, hash: spec.hash, body: spec.body },
        },
      };
    }
    // 🚨 ここに来た = 50 件続けて掴めなかった。「仕事なし」と同じ顔で返すと、
    //    壊れた job が並んでいるときに正常な依頼が永遠に拾われない (コード R8 #3)。
    //    実行役はすぐ掛け直し、監視には異常として見える形にする
    return { ok: true, job: null, exhausted: true, error: 'claim を 50 回試しても掴めなかった (壊れた依頼が並んでいる可能性)' };
  }).immediate();
}

/** lease が有効な running の job を返す。無効なら理由 */
function liveLease(db, jobId, leaseToken, nowS) {
  const job = jobById(db, jobId);
  if (!job) return { code: 'not_found', error: '依頼がありません' };
  if (job.status !== 'running' || !leaseToken || job.lease_token !== String(leaseToken)) {
    return { code: 'lease_lost', error: 'この実行役の lease ではありません (取り直されたか終了済み)' };
  }
  if (!job.lease_until || job.lease_until < nowS) return { code: 'lease_expired', error: 'lease の期限が切れています' };
  return { job };
}

/**
 * AI を呼ぶ**前**の予約 (1 job 1 回・1 日の上限)。
 * ここを通らずに呼ばれた生成は、結果を受け取らない。
 */
export function reserveGeneration(db, jobId, { leaseToken, model, promptVersion, now = Date.now() } = {}) {
  if (!lpComposeEnabled()) return { code: 'disabled', error: 'PH_LP_COMPOSE_ENABLED が無効です' };
  const m = trim(model, 80), pv = trim(promptVersion, 80);
  if (!m) return { code: 'bad_request', error: 'model が要ります' };
  // 🚨 段階1 は 1 つの prompt を測るのが目的。実行役が旧版や打ち間違いを送ってきたら、
  //    AI 枠を使う前に断る (通すと「別の条件で作ったもの」が測定結果に混ざる。コード R1 #6)
  // trim 前の値で比べる (識別子は正規化せず形を直接見る。コード R3〜R5 と同じ作法・codex exec review #1591 Low)
  if (promptVersion !== PROMPT_VERSION) {
    return { code: 'bad_prompt_version', error: `prompt_version は ${PROMPT_VERSION} です (実行役が古い可能性)` };
  }
  // 同じ理由でモデルも固定する。ランナーは queue で受け取った値で claude を起動し、同じ値を送る
  // 🚨 これは「頼んだモデル」の記録。実際に本回答を書いたモデルはランナーが後から
  //    recordModelCheck で付ける (Claude の申告は使わない・codex exec review #1591 High)
  if (model !== lpComposeModel()) {
    return { code: 'bad_model', error: `model は ${lpComposeModel()} です (実行役が古いか、設定が途中で変わった)` };
  }
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    if (genOf(db, l.job.id)) return { code: 'already_reserved', error: 'この依頼の AI 呼び出しは予約済みです (1 依頼 1 回)' };
    // 段階1 で作り直したいときは新しい job を作る (同じ job を再利用しない)
    if (db.prepare('SELECT COUNT(*) AS n FROM ph_lp_compose_generations WHERE job_id = ?').get(l.job.id).n > 0) {
      return { code: 'already_generated', error: 'この依頼は生成済みです (作り直すときは依頼し直してください)' };
    }
    const day = jstDay(now);
    const used = db.prepare('SELECT COUNT(*) AS n FROM ph_lp_compose_generations WHERE reserved_day = ?').get(day).n;
    if (used >= dailyCap()) return { code: 'daily_cap', error: `今日の AI 呼び出しの上限 (${dailyCap()} 回) に達しました` };
    const gid = Number(db.prepare(`INSERT INTO ph_lp_compose_generations
      (job_id, packet_hash, lease_token, runner_run_id, status, model, prompt_version, reserved_day, reserved_at)
      VALUES (?, ?, ?, ?, 'reserved', ?, ?, ?, ?)`)
      .run(l.job.id, l.job.packet_hash, l.job.lease_token, l.job.runner_run_id, m, pv, day, nowS).lastInsertRowid);
    return { ok: true, generation_id: gid, packet_hash: l.job.packet_hash };
  }).immediate();
}

/**
 * AI を呼んだ**後**の終了。**accepted / rejected のどちらでもここを通す** (設計 §4.3b)。
 * job だけ進んで generation が reserved のまま残る経路を作らない。
 *
 * - `verdict: 'accepted'` … lint も検品も通った → job = done。output を保存する
 * - `verdict: 'rejected'` … lint / 検品が 2 巡で通らなかった → job = failed。理由を残す
 *
 * **確定済み + 同じ payload の再送は保存済みの receipt を返す** (通信断のリトライで二重に書かない)。
 * lease が切れていても受ける — AI 枠は既に消費しているので、結果は取りこぼさない (④)。
 * @returns {{ok:true, status:string, already:boolean}|{code:string, error:string}}
 */
export function submitResult(db, generationId, {
  packetHash, verdict, output = null, lint = null, reviewRounds = null, receipt = null, reason = null, now = Date.now(),
} = {}) {
  const nowS = new Date(now).toISOString();
  const v = trim(verdict, 20);
  if (!['accepted', 'rejected'].includes(v)) return { code: 'bad_request', error: "verdict は accepted か rejected です" };
  const out = output == null ? '' : String(output);
  if (v === 'accepted') {
    if (!out.trim()) return { code: 'bad_request', error: '構成の本文が空です' };
    if (out.length > OUTPUT_MAX) return { code: 'too_large', error: `構成が大きすぎます (${OUTPUT_MAX} 文字まで)` };
  }
  // 🚨 実行役から来た値の検査は **payloadHash を作る前**。canonicalJson は循環参照で
  //    スタックを溢れさせるので、先に JSON にできるかを確かめる (コード R1 #5)
  // 範囲外を黙って null にしない — 未指定と区別できず、別の再送が同じ payloadHash になる (コード R5)
  if (reviewRounds != null && (!Number.isInteger(reviewRounds) || reviewRounds < 0 || reviewRounds > 10)) {
    return { code: 'bad_request', error: 'review_rounds は 0〜10 の整数です' };
  }
  const rounds = reviewRounds == null ? null : reviewRounds;
  const lintJson = jsonOrNull(lint, LINT_MAX);
  if (lintJson === false) return { code: 'bad_request', error: `lint が大きすぎるか JSON にできません (${LINT_MAX} 文字まで)` };
  // 🚨 証跡は**実行役から受け取らない** (codex exec review P1)。
  //    作業ディレクトリに置いた記録は Claude のセッションが Write できるので、
  //    「見ていないのに見たことにする」偽造ができた。サーバが配ったときの記録 (images_served_json) を使う。
  //    receipt が送られてきても無視する (古い実行役との互換のため、形だけは見る)。
  let imgs = null;
  if (receipt?.images != null) {
    if (!Array.isArray(receipt.images)) return { code: 'bad_request', error: 'receipt.images は配列です' };
    if (receipt.images.length > RECEIPT_IMAGES_MAX) {
      return { code: 'bad_request', error: `receipt.images は ${RECEIPT_IMAGES_MAX} 枚までです (${receipt.images.length} 枚)` };
    }
    imgs = [];
    for (const im of receipt.images) {
      // file_id も trim / 切り詰めしない — 別の入力が同じ証跡に畳まれると provenance にならない (コード R4)
      const fileId = exact(im?.file_id, DRIVE_FILE_ID_RE);
      if (!fileId) {
        return { code: 'bad_request', error: 'receipt.images の file_id が Drive の ID の形ではありません' };
      }
      // 🚨 trim / toLowerCase してから検査しない — 大文字や空白混じりを受けて同じ hash に畳むと、
      //    別の入力が同じ payloadHash になり「同じ結果の再送」の判定が狂う (コード R3)
      const hex = exact(im?.sha256, SHA256_RE);
      const bytes = posInt(im?.bytes);
      if (!hex || !bytes) {
        return { code: 'bad_request', error: 'receipt.images は sha256 (16進小文字64桁) と bytes (正の整数) が要ります' };
      }
      // 同じ画像を並べて枚数を水増しさせない (コード R8 #2)
      if (imgs.some((x) => x.file_id === fileId)) {
        return { code: 'bad_request', error: `receipt.images に同じ画像が 2 回あります (${fileId})` };
      }
      imgs.push({ file_id: fileId, sha256: hex, bytes });
    }
  }
  const reasonText = trim(reason, REASON_MAX);
  // 🚨 receipt も hash の対象に入れる。入れないと「画像の証跡だけ違う再送」を
  //    同じ結果と見なして保存済みを返してしまう (コード R2 #1)。finalized_at は毎回変わるので入れない
  const payloadHash = sha256(canonicalJson({
    verdict: v, output: out, lint: lintJson, review_rounds: rounds, reason: reasonText,
  }));
  return db.transaction(() => {
    const gen = db.prepare('SELECT * FROM ph_lp_compose_generations WHERE id = ?').get(posInt(generationId));
    if (!gen) return { code: 'not_found', error: '予約がありません' };
    if (exact(packetHash, SHA256_RE) !== gen.packet_hash) {
      return { code: 'packet_mismatch', error: '材料が予約時と違います (受付時に固定した packet ではありません)' };
    }
    if (gen.status !== 'reserved') {
      // 同じ結果の再送 = 保存済みの receipt を返す (リトライで二重に書かない)
      if (gen.payload_hash === payloadHash) {
        const job = jobById(db, gen.job_id);
        return { ok: true, status: job?.status || gen.status, already: true, receipt: JSON.parse(gen.receipt_json || 'null') };
      }
      return { code: 'already_finalized', error: 'この予約は確定済みです (別の内容では上書きできません)' };
    }
    const job = jobById(db, gen.job_id);
    if (!job) return { code: 'not_found', error: '依頼がありません' };
    // 🚨 確定してよいのは running (通常) と needs_review (lease 切れ後の復旧) だけ。
    //    これが無いと、cancelled や別理由で failed になった job を後から done に戻せてしまう
    //    (コード R1 #2)。lease 切れ後の復旧を受けるのは AI 枠を消費済みだから (設計 ④)。
    if (!['running', 'needs_review'].includes(job.status)) {
      return { code: 'job_finalized', error: `この依頼は ${job.status} で終わっています (結果は受け取れません)` };
    }

    // 🚨 証跡は「渡した材料のうち実際に見たもの」でなければ意味がない。
    //    packet に無い file_id を含む receipt は受け取らない (コード R7 #2)。
    let packetImages = [];
    try { packetImages = JSON.parse(job.packet_json).images || []; } catch { packetImages = []; }
    // 証跡はサーバが配ったときの記録。実行役が送ってきた receipt は使わない
    let served = [];
    try { served = JSON.parse(job.images_served_json || '[]'); } catch { served = []; }
    if (!Array.isArray(served)) served = [];
    // 🚨 **accepted なら packet の画像を全部配っていること** (codex exec review R2 P2 → P1 で全枚に)。
    //    はじめは「1 枚でも配っていればよい」にしていたが、それだと
    //    **途中の枚で落ちた実行役が、残りを見ずに accepted を出せてしまう**。
    //    実行役側 (`./phlp images`) も「1 枚でも取れなければ失敗」に揃えてあるので、
    //    サーバ側も全枚を求める (段階1 は測定が目的。欠けた材料で書いた構成を混ぜない)。
    //    rejected は「作れなかった」ので証跡が無くてよい。
    if (v === 'accepted' && packetImages.length > 0) {
      const seenIds = new Set(served.map((im) => im && im.file_id).filter(Boolean));
      const missing = packetImages.filter((im) => im?.file_id && !seenIds.has(im.file_id)).length;
      if (missing > 0) {
        return {
          code: 'bad_request',
          error: `商品画像 ${packetImages.length} 枚のうち ${missing} 枚を見ていません。全部取得してから構成を書いてください`,
        };
      }
    }
    // 🚨 **lint はサーバが実行する。これが正本** (PR1-c・設計 §6)。
    //    PR1-b までは実行役の自己申告 (`lint.ok`) を信じていたが、
    //    それだと**自分で `{"ok":true}` と書けば何でも通せた**。
    //    実行役が送ってきた lint は参考値として残すだけ (runner_lint)。
    let serverLint = null;
    if (v === 'accepted') {
      let packetName = null;
      try { packetName = JSON.parse(job.packet_json).name || null; } catch { packetName = null; }
      try {
        serverLint = lintSummary(lintComposition(out, { productName: packetName }));
      } catch (e) {
        return { code: 'lint_failed', error: `lint を実行できませんでした: ${String(e?.message || e).slice(0, 200)}` };
      }
      if (!serverLint.ok) {
        // 🚨 これは実行役のバグではなく、**測定結果**。
        //    渡すときに何が足りないかを全部返す (実行役が直せるように)
        return {
          code: 'lint_failed',
          error: `lint を通っていません (${serverLint.errors.length} 件)`,
          lint: serverLint,
        };
      }
    }
    // 保存する lint = サーバの結果を正本にし、実行役の自己申告を横に残す
    // (測定のときに「AI は通っているつもりだったか」を読める)
    let runnerLint = null;
    try { runnerLint = lintJson ? JSON.parse(lintJson) : null; } catch { runnerLint = null; }
    const storedLint = v === 'accepted' ? packStoredLint(serverLint, runnerLint) : lintJson;
    const receiptObj = {
      verdict: v, review_rounds: rounds, model: gen.model, prompt_version: gen.prompt_version,
      finalized_at: nowS,
      // 何を見て作ったか = **サーバが配ったときの記録**。実行役は書き換えられない
      images: served,
    };
    // 🚨 generation と job の両方が 1 行ずつ動いたことを確かめ、片方でも動かなければ throw して
    //    トランザクションごと戻す (中途半端な状態を残さない。コード R1 #2)
    const genCh = db.prepare(`UPDATE ph_lp_compose_generations
      SET status = ?, payload_hash = ?, receipt_json = ?, discard_reason = ?, finalized_at = ?
      WHERE id = ? AND status = 'reserved'`)
      .run(v, payloadHash, JSON.stringify(receiptObj), v === 'rejected' ? reasonText : null, nowS, gen.id).changes;

    const jobCh = v === 'accepted'
      ? db.prepare(`UPDATE ph_lp_compose_jobs
          SET status = 'done', output_text = ?, output_hash = ?, lint_json = ?, review_rounds = ?,
              error_code = NULL, error = NULL, lease_token = NULL, lease_until = NULL,
              updated_at = ?, completed_at = COALESCE(completed_at, ?), finalized_at = ?
          WHERE id = ? AND status IN ('running', 'needs_review')`)
        .run(out, sha256(out), storedLint, rounds, nowS, nowS, nowS, job.id).changes
      : db.prepare(`UPDATE ph_lp_compose_jobs
          SET status = 'failed', lint_json = ?, review_rounds = ?, error_code = 'rejected', error = ?,
              lease_token = NULL, lease_until = NULL,
              updated_at = ?, completed_at = COALESCE(completed_at, ?), finalized_at = ?
          WHERE id = ? AND status IN ('running', 'needs_review')`)
        .run(storedLint, rounds, reasonText || '検品で通らなかった', nowS, nowS, nowS, job.id).changes;

    if (genCh !== 1 || jobCh !== 1) {
      throw new Error(`lp-compose: 結果の確定で行が動かなかった (generation=${genCh} job=${jobCh})`);
    }
    if (v === 'accepted') {
      logEvent(db, job.draft_id, 'lp_compose_done', `依頼 ${job.id} (${rounds ?? '?'} 巡)`, 'ph-lp-compose');
      return { ok: true, status: 'done', already: false, receipt: receiptObj };
    }
    logEvent(db, job.draft_id, 'lp_compose_rejected', `依頼 ${job.id}: ${trim(reason, 200)}`, 'ph-lp-compose');
    return { ok: true, status: 'failed', already: false, receipt: receiptObj };
  }).immediate();
}

/**
 * 生成できなかった報告。**予約の前だけ** (設計 §4.3b)。
 * 呼んだ後の失敗は submitResult(verdict: 'rejected') を使う。
 */
export function failJob(db, jobId, { leaseToken, code, message, now = Date.now() } = {}) {
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    if (genOf(db, l.job.id)) {
      return { code: 'already_reserved', error: '予約後の失敗は result で rejected を出してください (§4.3b)' };
    }
    finishJob(db, l.job.id, 'failed', { code: trim(code, 40) || 'other', message: trim(message, 500), nowS });
    logEvent(db, l.job.draft_id, 'lp_compose_failed', `依頼 ${l.job.id} ${trim(code, 40)}`, 'ph-lp-compose');
    return { ok: true, status: 'failed' };
  }).immediate();
}

/** 一時障害で手放す (queued に戻す)。**予約の前だけ** — 後に許すと二重に呼べてしまう */
export function releaseJob(db, jobId, { leaseToken, reason = null, now = Date.now() } = {}) {
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const l = liveLease(db, jobId, leaseToken, nowS);
    if (l.code) return l;
    if (genOf(db, l.job.id)) {
      return { code: 'already_reserved', error: '予約後は手放せません (成否不明になるため result を出してください)' };
    }
    db.prepare(`UPDATE ph_lp_compose_jobs SET status = 'queued', lease_token = NULL, lease_until = NULL, updated_at = ? WHERE id = ?`)
      .run(nowS, l.job.id);
    if (trim(reason)) logEvent(db, l.job.draft_id, 'lp_compose_released', `依頼 ${l.job.id}: ${trim(reason, 200)}`, 'ph-lp-compose');
    return { ok: true, status: 'queued' };
  }).immediate();
}

/**
 * 実行役へ渡す商品画像の 1 枚を指す。**fileId を外から受けない** —
 * その job の packet に固定済みの images[index] だけを返す (設計 §4.3)。
 * lease が有効な間だけ。`version` は packet に固定した Drive の更新日時
 * (キャッシュのキーに使う。事前照合は段階2 で入れる — 設計 §4.2)。
 * @returns {{ok:true, file_id:string, version:string|null}|{code:string, error:string}}
 */
export function lpComposeImageRef(db, jobId, { leaseToken, index, now = Date.now() } = {}) {
  const nowS = new Date(now).toISOString();
  const l = liveLease(db, jobId, leaseToken, nowS);
  if (l.code) return l;
  const i = Number(index);
  if (!Number.isInteger(i) || i < 0 || i >= MAX_IMAGES) {
    return { code: 'bad_request', error: `画像の番号は 1〜${MAX_IMAGES} です` };
  }
  let images;
  try { images = JSON.parse(l.job.packet_json).images || []; }
  catch { return { code: 'bad_request', error: '材料を読めませんでした' }; }
  const im = images[i];
  if (!im?.file_id) return { code: 'not_found', error: 'その番号の商品画像はありません' };
  return { ok: true, file_id: im.file_id, version: im.modified_time || null };
}

/**
 * 保存する lint を組み立てる。
 *
 * 🚨 **サーバの結果を必ず残す** (codex exec review P2)。
 *    以前は `jsonOrNull(...) || lintJson` と書いていたので、実行役が
 *    LINT_MAX 間近の巨大な lint を送ると合計が上限を超え、
 *    **実行役の自己申告がそのまま正本として保存された**。
 *    入り切らなければ落とすのは**参考値の方** (runner_lint → 警告 → 詳細)。
 */
function packStoredLint(serverLint, runnerLint) {
  const base = { ...serverLint, source: 'server' };
  const tries = [
    { ...base, runner_lint: runnerLint },
    { ...base, runner_lint: null, runner_lint_dropped: true },
    { ...base, warnings: [], runner_lint: null, runner_lint_dropped: true, warnings_dropped: true },
    { ok: base.ok, checks: base.checks, errors: [], warnings: [], source: 'server', truncated: true },
  ];
  for (const cand of tries) {
    const j = jsonOrNull(cand, LINT_MAX);
    if (j !== false) return j;
  }
  // ここまで来ることは無いが、来ても**実行役の申告には戻さない**
  return JSON.stringify({ ok: !!serverLint.ok, source: 'server', truncated: true });
}

/**
 * 構成を lint するだけ (結果は確定しない)。実行役が**出す前に自分で直せる**ように置く。
 *
 * これが無いと、実行役は `result --accepted` を出して断られるまで lint 結果を知れず、
 * その 1 回で generation を使い切ってしまう。**測定の目的は「直せたか」ではなく
 * 「どこまで書けたか」**なので、lint 自体は何度でも回せてよい (AI 枠を消費しない)。
 * @returns {{ok:true, lint:object}|{code:string, error:string}}
 */
export function lintForJob(db, jobId, { leaseToken, output, now = Date.now() } = {}) {
  const nowS = new Date(now).toISOString();
  const l = liveLease(db, posInt(jobId), leaseToken, nowS);
  if (l.code) return l;
  const out = String(output == null ? '' : output);
  if (!out.trim()) return { code: 'bad_request', error: '構成の本文が空です' };
  if (out.length > OUTPUT_MAX) return { code: 'too_large', error: `構成が大きすぎます (${OUTPUT_MAX} 文字まで)` };
  let packetName = null;
  try { packetName = JSON.parse(l.job.packet_json).name || null; } catch { packetName = null; }
  try {
    return { ok: true, lint: lintSummary(lintComposition(out, { productName: packetName })) };
  } catch (e) {
    return { code: 'lint_failed', error: `lint を実行できませんでした: ${String(e?.message || e).slice(0, 200)}` };
  }
}

/**
 * 商品画像を 1 枚配ったことを**サーバが**記録する (codex exec review P1)。
 * 証跡を実行役の作業ディレクトリに置くと、Claude のセッションが Write できてしまい
 * 「画像を見ていないのに見たことにする」偽造ができる。測定の根拠なのでここで持つ。
 * 同じ file_id を 2 回配っても 1 行 (枚数を水増しさせない)。
 *
 * 🚨 記録は**その時点で生きている lease** に紐づける (codex exec review P2)。
 *    紐づけないと、A の lease が切れた直後に B が claim して証跡をリセットしたあとに、
 *    飛んでいた A の取得が完走してここに来て、**B の証跡として入ってしまう**
 *    = B は画像を一度も見ずに accepted を出せる。
 */
export function recordImageServed(db, jobId, { leaseToken, fileId, sha256: hex, bytes, now = Date.now() } = {}) {
  const id = posInt(jobId);
  const f = exact(fileId, DRIVE_FILE_ID_RE);
  const h = exact(hex, SHA256_RE);
  const b = posInt(bytes);
  if (!id || !f || !h || !b) return { code: 'bad_request', error: '記録する画像の指定が不正です' };
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const l = liveLease(db, id, leaseToken, nowS);
    if (l.code) return l;
    const job = l.job;
    let list;
    try { list = JSON.parse(job.images_served_json || '[]'); } catch { list = []; }
    if (!Array.isArray(list)) list = [];
    const hit = list.findIndex((im) => im.file_id === f);
    const row = { file_id: f, sha256: h, bytes: b, served_at: new Date(now).toISOString() };
    if (hit >= 0) list[hit] = row; else list.push(row);
    if (list.length > MAX_IMAGES) list.length = MAX_IMAGES;
    // WHERE にも lease を書く (liveLease で確かめた同じ lease のままであることを DB 側でも固定する)
    const ch = db.prepare(`UPDATE ph_lp_compose_jobs SET images_served_json = ?, updated_at = ?
      WHERE id = ? AND status = 'running' AND lease_token = ?`)
      .run(JSON.stringify(list), nowS, id, String(leaseToken)).changes;
    if (!ch) return { code: 'lease_lost', error: 'この実行役の lease ではありません (取り直されたか終了済み)' };
    return { ok: true, count: list.length };
  }).immediate();
}

/** ランナーが決める run id (PH_LP_RUN_ID)。claim で job に、reserve で generation に写る */
const RUN_ID_RE = /^[A-Za-z0-9_.:-]{1,80}$/;
/** 本回答のモデル (stream-json の assistant.message.model)。[1m] は付かない */
const ACTUAL_MODEL_RE = /^claude-[a-z0-9]+(?:-[a-z0-9]+){1,6}$/;
const baseModel = (m) => String(m || '').replace(/\[1m\]$/, '');
export const MODEL_CHECKS = ['match', 'mismatch', 'unknown'];

/**
 * **実際に本回答を書いたモデル**を generation に付ける (codex exec review #1591 High)。
 * reserve に入るのは「頼んだモデル」だけで、別のモデルが書いても done のまま測定に入っていた。
 *
 * 🚨 呼べるのは miniPC のランナー (service token を持つ PowerShell) だけ。`./phlp` にはこれを呼ぶ
 *    コマンドを**作らない** — Claude の権限は ./phlp と ./phlpreview だけなので、Claude からは届かない。
 *    相手の generation は run id (ランナーが PH_LP_RUN_ID で決めて claim に渡した値) で引く。
 * 🚨 一度付けたら書き換えない (後から「一致」に塗り替えられない)。
 *
 * @param actualModels 本回答 (サブエージェントでない assistant) のモデルの一覧。読めなければ []
 * @returns {{ok:true, updated:number, checks:Array<{generation_id:number, model_check:string}>}|{code, error}}
 */
export function recordModelCheck(db, { runnerRunId, actualModels, now = Date.now() } = {}) {
  const run = exact(runnerRunId, RUN_ID_RE);
  if (!run) return { code: 'bad_request', error: 'runner_run_id の形が不正です' };
  if (!Array.isArray(actualModels) || actualModels.length > 10) {
    return { code: 'bad_request', error: 'actual_models は 10 個までの配列です' };
  }
  const actual = [];
  for (const a of actualModels) {
    if (!exact(a, ACTUAL_MODEL_RE)) return { code: 'bad_request', error: 'actual_models に形の違う値があります' };
    if (!actual.includes(a)) actual.push(a);
  }
  const nowS = new Date(now).toISOString();
  return db.transaction(() => {
    const gens = db.prepare(`SELECT id, model FROM ph_lp_compose_generations
      WHERE runner_run_id = ? AND model_check IS NULL ORDER BY id`).all(run);
    const checks = [];
    for (const g of gens) {
      // 全部が頼んだモデルで「一致」。1 つでも違えば「不一致」。読めなければ「未確認」(一致とは扱わない)
      const check = actual.length === 0 ? 'unknown'
        : actual.every((a) => a === baseModel(g.model)) ? 'match' : 'mismatch';
      const ch = db.prepare(`UPDATE ph_lp_compose_generations
        SET actual_model = ?, model_check = ?, model_checked_at = ?
        WHERE id = ? AND model_check IS NULL`)
        .run(actual.length ? actual.join(',') : null, check, nowS, g.id).changes;
      if (ch === 1) checks.push({ generation_id: g.id, model_check: check });
    }
    return { ok: true, updated: checks.length, checks };
  }).immediate();
}

// ─── 画面 ────────────────────────────────────────────────

/**
 * 詳細画面に出す状態。ポーリングで 5 秒おきに叩かれるので軽く保つ
 * (done のときだけ output_text を返す)。
 */
export function jobStateFor(db, draftId, { now = Date.now() } = {}) {
  recoverExpired(db, now);
  const job = db.prepare('SELECT * FROM ph_lp_compose_jobs WHERE draft_id = ? ORDER BY id DESC LIMIT 1').get(posInt(draftId));
  // いま押したら使われるモデル (ボタンに出す)
  const current = { enabled: lpComposeEnabled(), model: lpComposeModel(), model_label: modelLabel(lpComposeModel()) };
  if (!job) return { ...current, job: null };
  // この依頼で**実際に予約された**モデル。予約前 (待ち・画像なしで止まった等) は null
  const gen = db.prepare(`SELECT model, actual_model, model_check FROM ph_lp_compose_generations
    WHERE job_id = ? ORDER BY id DESC LIMIT 1`).get(job.id);
  const elapsed = Math.max(0, Math.round((Date.parse(job.completed_at || new Date(now).toISOString()) - Date.parse(job.created_at)) / 1000));
  return {
    ...current,
    job: {
      id: job.id,
      model: gen?.model || null,
      model_label: gen?.model ? modelLabel(gen.model) : null,
      // 実際に本回答を書いたモデル (ランナーが後から付ける)。
      // model_check: match / mismatch / unknown。予約済みでまだ付いていなければ null (= 確認中)
      actual_model: gen?.actual_model || null,
      model_check: gen?.model_check || null,
      status: job.status,
      created_at: job.created_at,
      completed_at: job.completed_at,
      elapsed_sec: elapsed,
      // 測定の合格ライン (3 分) に入ったか。終わっていなければ null
      within_deadline: job.completed_at ? job.completed_at <= job.measurement_deadline_at : null,
      requested_by: job.requested_by,
      spec_id: job.spec_id,
      review_rounds: job.review_rounds,
      error_code: job.error_code,
      error: job.error,
      lint: job.lint_json ? JSON.parse(job.lint_json) : null,
      output_text: job.status === 'done' ? job.output_text : null,
      packet_hash: job.packet_hash,
    },
  };
}
