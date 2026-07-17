/**
 * rakuten-review-campaign-lib.js — フォロー/クーポンメール planner (mall-csv-fetcher P2 PR-C1)
 *
 * らくらくーぽん置換 (設計書v1.2 §4 C3) の shadow planner。
 * **このPRには送信コードが一切ない** (SMTP接続もクーポン発行APIも呼ばない)。
 * planner は「らくらくーぽんなら何をいつ送るか」を rakuten_campaign_actions に記録するだけ。
 * shadow 期間中に到達しうる status は planned / suppressed / ready / expired / cancelled のみで、
 * claimed / sent / failed_safe / ambiguous は PR-C4 (送信) まで発生しない。
 *
 * 置換対象の現行運用値 (らくらくーぽん転記、設計書v1.2冒頭):
 *   - フォローメール = 発送日の10日後 12:00 JST
 *   - クーポンメール = レビュー投稿の検知次第 (日次12時台) → 検知後の次の12:00 JST
 *   - 規約: 受注関連メールは発送後3週間以内 → 全action に expires_at (発送日+21日)
 *
 * 設計判断 (Codex 2R確定 + 本PRでの追加判断):
 *   - dedupe_key = sha256("follow:v1:"+注文番号) / sha256("coupon:v1:"+注文番号) UNIQUE。
 *     ※設計書はクーポンを review_key ベースとしていたが、「クーポンは注文単位最大1回」の
 *     不変条件を UNIQUE 制約で構造的に保証するため注文番号ベースに変更
 *     (商品レビュー+ショップレビューの2件で2重付与になる穴を塞ぐ)。
 *     どのレビューが引き金かは trigger_review_url に記録
 *   - 紐付けルール (安全側、設計書§4): 注文にレビューが1件でもあれば (削除済み含む) フォロー停止 /
 *     編集で再付与しない (dedupe_key が構造的に防ぐ) / 削除でフォロー再開しない
 *     (suppressed からの復帰遷移を実装しない) / 注文番号なしレビューは対象外 / fuzzy matching 禁止
 *   - クーポン epoch: 初回 plan 実行時刻を rakuten_campaign_meta に記録し、
 *     それ以前に first_seen のレビュー (バックフィル約47,000件) には action を作らない
 *     (shadow は「今から」の比較。歴史的レビューへの遡及付与はしない)
 *   - フォローは rakuten_order_contacts (発送済みの直近注文のみが入る) が自然な窓になるため epoch 不要。
 *     発送+21日を過ぎた古い contacts 行は expired で入る (突合の分母に残す)
 *   - PII: このテーブル群に宛先メールは入れない (送信直前に contacts から復号するのは PR-C4)。
 *     ログ・GChat には件数のみ (注文番号・レビューURLも出さない)
 *
 * 既知の限界 (shadow 突合 PR-C2 で差分として観測する):
 *   - 発送後キャンセルの検知不可 (searchOrder がキャンセル進行を返さないため contacts が古いまま)
 *   - contacts 取得窓 (発送25日) より古い注文へのレビューは no_contact で抑止
 *     (らくらくーぽん側は全注文にアクセスできるため送る可能性がある)
 */
import crypto from 'node:crypto';

// ─── 運用値 (らくらくーぽん転記) ───
export const FOLLOW_DELAY_DAYS = 10;      // 発送日の10日後
export const EXPIRES_AFTER_SHIP_DAYS = 21; // 受注関連メールは発送後3週間以内 (規約)
export const FOLLOW_TEMPLATE_VERSION = 'follow:v1';
export const COUPON_TEMPLATE_VERSION = 'coupon:v1:either'; // 付与条件=どちらか1件で付与 (shadow突合で実測後に調整)

const DAY_MS = 86400000;

// ─── 時刻ヘルパ (JST壁時計→UTC ISO。feedback_jst_to_iso_string_trap: 文字列組立で月境界事故を防ぐ) ───
/** ISO日時 (オフセット付き) → JST の暦日 'YYYY-MM-DD' */
export function jstDateOf(iso) {
  const t = Date.parse(iso);
  if (!Number.isFinite(t)) return null;
  return new Date(t + 9 * 3600 * 1000).toISOString().slice(0, 10);
}

/** JST暦日 + addDays 日後の 12:00 JST → UTC ISO */
export function jstNoonUtcIso(jstDate, addDays = 0) {
  const t = Date.parse(`${jstDate}T12:00:00+09:00`);
  if (!Number.isFinite(t)) return null;
  return new Date(t + addDays * DAY_MS).toISOString();
}

/** now の次の 12:00 JST (now がちょうど正午なら翌日) → UTC ISO */
export function nextNoonJstAfter(nowIso) {
  const t = Date.parse(nowIso);
  if (!Number.isFinite(t)) return null;
  const jstToday = new Date(t + 9 * 3600 * 1000).toISOString().slice(0, 10);
  const noonToday = Date.parse(`${jstToday}T12:00:00+09:00`);
  return new Date(t < noonToday ? noonToday : noonToday + DAY_MS).toISOString();
}

/** 発送日時 → { scheduledAt (発送10日後 12:00 JST), expiresAt (発送21日後 23:59:59 JST) } */
export function followScheduleFor(shippingIso) {
  const shipDate = jstDateOf(shippingIso);
  if (!shipDate) return null;
  const scheduledAt = jstNoonUtcIso(shipDate, FOLLOW_DELAY_DAYS);
  const expiresAt = new Date(Date.parse(`${shipDate}T23:59:59+09:00`) + EXPIRES_AFTER_SHIP_DAYS * DAY_MS).toISOString();
  return { scheduledAt, expiresAt };
}

/** fact_rakuten_reviews.posted_at ('YYYY-MM-DD HH:MM:SS' JST naive) → UTC ISO */
export function postedAtToUtcIso(postedAt) {
  const t = Date.parse(`${String(postedAt).trim().replace(' ', 'T')}+09:00`);
  return Number.isFinite(t) ? new Date(t).toISOString() : null;
}

export function dedupeKeyFor(actionType, orderNumber) {
  return crypto.createHash('sha256').update(`${actionType}:v1:${String(orderNumber).trim()}`, 'utf8').digest('hex');
}

// ─── DDL ───
export function ensureCampaignTables(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_campaign_actions (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    action_type        TEXT NOT NULL CHECK (action_type IN ('follow','coupon')),
    dedupe_key         TEXT NOT NULL UNIQUE,
    order_number       TEXT NOT NULL,
    trigger_review_url TEXT,
    status             TEXT NOT NULL CHECK (status IN
      ('planned','suppressed','ready','claimed','sent','failed_safe','ambiguous','cancelled','expired')),
    status_reason      TEXT,
    template_version   TEXT NOT NULL,
    scheduled_at       TEXT NOT NULL,
    expires_at         TEXT NOT NULL,
    ready_at           TEXT,
    claim_token        TEXT,
    claimed_at         TEXT,
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
  )`);
  // PR-C4 の必須不変条件 (Codex C1-R1): ready→claimed は「条件付きUPDATE+変更行数=1確認」の
  // CAS で行い、claim と同一トランザクションで ①expires_at > now ②contact 未purge・復号可能
  // ③suppression 未登録 ④coupon は有効レビュー残存 ⑤ownership owner='self' を再評価すること。
  // ready_at は「観測時刻」であり送信予定時刻ではない (planner は朝実行のため正午予定の action は
  // 翌朝 ready になる)。PR-C2 の日次突合は scheduled_at の JST 暦日で集計すること
  db.exec(`CREATE INDEX IF NOT EXISTS idx_rca_status_sched ON rakuten_campaign_actions(status, scheduled_at)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_rca_order ON rakuten_campaign_actions(order_number)`);

  // 送信試行ログ (PR-C4 で書く)。UNIQUE(action_id) = action単位の at-most-once を
  // DB制約で強制する (Codex C1-R1 High: message_id UNIQUE だけでは同一actionに別message_idで
  // 何度でも試行できてしまう)。明確なrejectも自動再送しない=「重複より取りこぼしを選ぶ」。
  // 再送が本当に必要なときは人が action を cancelled にして新 action を明示的に作る
  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_campaign_delivery_attempts (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    action_id    INTEGER NOT NULL UNIQUE REFERENCES rakuten_campaign_actions(id),
    message_id   TEXT NOT NULL UNIQUE,
    attempted_at TEXT NOT NULL,
    outcome      TEXT NOT NULL CHECK (outcome IN ('accepted','rejected','ambiguous')),
    smtp_code    TEXT,
    note         TEXT
  )`);

  // クーポン発行とメール送信の分離 (PR-C3/C4 で書く。メール失敗でもクーポンを再発行しない)
  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_coupon_grants (
    order_number TEXT PRIMARY KEY,
    action_id    INTEGER REFERENCES rakuten_campaign_actions(id),
    coupon_code  TEXT,
    issue_status TEXT NOT NULL CHECK (issue_status IN ('pending','issued','failed')),
    issued_at    TEXT,
    updated_at   TEXT NOT NULL
  )`);

  // 注文の担当者 (shadow中=vendor。cutover後の新規注文=self。PR-C4の送信ゲートが owner='self' を要求)
  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_order_campaign_ownership (
    order_number TEXT PRIMARY KEY,
    owner        TEXT NOT NULL CHECK (owner IN ('vendor','self','none')),
    reason       TEXT,
    decided_at   TEXT NOT NULL
  )`);

  db.exec(`CREATE TABLE IF NOT EXISTS rakuten_campaign_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
  )`);
}

// ─── planner 本体 (1回の実行 = 1トランザクション) ───
/**
 * @param {object} opts { nowIso: テスト用時刻注入, couponEpochOverride: テスト用epoch }
 * @returns 件数サマリ (PII なし)
 */
export function planCampaigns(db, opts = {}) {
  const now = opts.nowIso || new Date().toISOString();
  const counts = {
    followInserted: 0, couponInserted: 0,
    suppressed: 0, expired: 0, cancelled: 0, promotedReady: 0, rescheduled: 0,
    ownershipInserted: 0,
  };

  const insertStmt = db.prepare(`
    INSERT OR IGNORE INTO rakuten_campaign_actions (
      action_type, dedupe_key, order_number, trigger_review_url,
      status, status_reason, template_version, scheduled_at, expires_at,
      ready_at, created_at, updated_at
    ) VALUES (
      @action_type, @dedupe_key, @order_number, @trigger_review_url,
      @status, @status_reason, @template_version, @scheduled_at, @expires_at,
      NULL, @now, @now
    )
  `);
  const setStatusStmt = db.prepare(`
    UPDATE rakuten_campaign_actions
       SET status = @status, status_reason = @reason, ready_at = @ready_at, updated_at = @now
     WHERE id = @id AND status = 'planned'
  `);

  const tx = db.transaction(() => {
    // ── 0. クーポン epoch (初回実行時刻。これ以前の first_seen レビューには action を作らない) ──
    let epoch = db.prepare(`SELECT value FROM rakuten_campaign_meta WHERE key = 'coupon_epoch'`).get()?.value;
    if (!epoch) {
      epoch = opts.couponEpochOverride || now;
      db.prepare(`INSERT INTO rakuten_campaign_meta (key, value) VALUES ('coupon_epoch', ?)`).run(epoch);
    }

    // ── 1. 新規フォロー action (発送済み contacts で action 未作成のもの) ──
    // 発送前 (shipping_datetime NULL) はスキップ = 後日の実行で発送済みになってから作る
    const newFollows = db.prepare(`
      SELECT c.order_number, c.shipping_datetime, c.masked_email_enc, c.masked_email_hash,
             EXISTS(SELECT 1 FROM fact_rakuten_reviews r WHERE r.order_number = c.order_number) AS has_review,
             EXISTS(SELECT 1 FROM rakuten_contact_suppressions s WHERE s.email_hash = c.masked_email_hash) AS is_suppressed
        FROM rakuten_order_contacts c
        LEFT JOIN rakuten_campaign_actions a
               ON a.order_number = c.order_number AND a.action_type = 'follow'
       WHERE a.id IS NULL AND c.shipping_datetime IS NOT NULL
    `).all();
    for (const c of newFollows) {
      const sched = followScheduleFor(c.shipping_datetime);
      if (!sched) continue; // 発送日時が壊れている行は次回に回す (contacts 側で正規化済みのため実質起きない)
      let status = 'planned', reason = null;
      if (c.has_review) { status = 'suppressed'; reason = 'review_exists'; }        // レビュー済ならフォロー停止
      else if (!c.masked_email_enc) { status = 'suppressed'; reason = 'no_email'; }
      else if (c.is_suppressed) { status = 'suppressed'; reason = 'suppression_list'; }
      else if (now >= sched.expiresAt) { status = 'expired'; reason = 'expired_before_plan'; } // 発送21日超の古い注文
      const r = insertStmt.run({
        action_type: 'follow', dedupe_key: dedupeKeyFor('follow', c.order_number),
        order_number: c.order_number, trigger_review_url: null,
        status, status_reason: reason, template_version: FOLLOW_TEMPLATE_VERSION,
        scheduled_at: sched.scheduledAt, expires_at: sched.expiresAt, now,
      });
      if (r.changes > 0) counts.followInserted++;
    }

    // ── 2. planned フォローの再スケジュール (再発送等で最終発送日が変わった場合に追従) ──
    const planned = db.prepare(`
      SELECT a.id, a.scheduled_at, a.expires_at, c.shipping_datetime
        FROM rakuten_campaign_actions a
        JOIN rakuten_order_contacts c ON c.order_number = a.order_number
       WHERE a.action_type = 'follow' AND a.status = 'planned' AND c.shipping_datetime IS NOT NULL
    `).all();
    const reschedStmt = db.prepare(`
      UPDATE rakuten_campaign_actions SET scheduled_at = ?, expires_at = ?, updated_at = ?
       WHERE id = ? AND status = 'planned'
    `);
    for (const a of planned) {
      const sched = followScheduleFor(a.shipping_datetime);
      if (sched && (sched.scheduledAt !== a.scheduled_at || sched.expiresAt !== a.expires_at)) {
        reschedStmt.run(sched.scheduledAt, sched.expiresAt, now, a.id);
        counts.rescheduled++;
      }
    }

    // ── 3. 新規クーポン action (epoch 以降に初観測されたレビューの注文、注文単位最大1回) ──
    const newCoupons = db.prepare(`
      SELECT r.order_number,
             MIN(r.posted_at) AS first_posted,
             (SELECT r2.review_url FROM fact_rakuten_reviews r2
               WHERE r2.order_number = r.order_number AND r2.is_deleted = 0
               ORDER BY r2.posted_at, r2.review_url LIMIT 1) AS trigger_url,
             c.order_number IS NOT NULL AS has_contact,
             c.shipping_datetime, c.masked_email_enc, c.masked_email_hash,
             EXISTS(SELECT 1 FROM rakuten_contact_suppressions s WHERE s.email_hash = c.masked_email_hash) AS is_suppressed
        FROM fact_rakuten_reviews r
        LEFT JOIN rakuten_order_contacts c ON c.order_number = r.order_number
        LEFT JOIN rakuten_campaign_actions a
               ON a.order_number = r.order_number AND a.action_type = 'coupon'
       WHERE r.order_number IS NOT NULL AND r.order_number != ''
         AND r.is_deleted = 0 AND r.first_seen_at >= ? AND a.id IS NULL
         AND NOT EXISTS (
           SELECT 1 FROM fact_rakuten_reviews old
            WHERE old.order_number = r.order_number AND old.first_seen_at < ?
         )
       GROUP BY r.order_number
    `).all(epoch, epoch);
    // NOT EXISTS = epoch判定は注文単位 (Codex C1-R1 High: epoch前レビューがある注文に
    // epoch後の2件目が来ると、vendor付与済み注文へ自作が二重付与する候補を作ってしまう)
    for (const rv of newCoupons) {
      // 期限は発送基準 (規約の3週間)。contacts が無い注文は投稿日時基準の保守値 (どうせ no_contact で抑止)
      const base = rv.shipping_datetime
        ? followScheduleFor(rv.shipping_datetime)
        : { expiresAt: (() => {
            const p = postedAtToUtcIso(rv.first_posted);
            return p ? new Date(Date.parse(p) + EXPIRES_AFTER_SHIP_DAYS * DAY_MS).toISOString() : now;
          })() };
      const scheduledAt = nextNoonJstAfter(now); // 検知次第 → 次の12:00 JST (らくらくーぽんの日次12時台に対応)
      let status = 'planned', reason = null;
      if (!rv.has_contact) { status = 'suppressed'; reason = 'no_contact'; }
      else if (!rv.masked_email_enc) { status = 'suppressed'; reason = 'no_email'; }
      else if (rv.is_suppressed) { status = 'suppressed'; reason = 'suppression_list'; }
      else if (now >= base.expiresAt) { status = 'expired'; reason = 'expired_before_plan'; }
      const r = insertStmt.run({
        action_type: 'coupon', dedupe_key: dedupeKeyFor('coupon', rv.order_number),
        order_number: rv.order_number, trigger_review_url: rv.trigger_url,
        status, status_reason: reason, template_version: COUPON_TEMPLATE_VERSION,
        scheduled_at: scheduledAt, expires_at: base.expiresAt, now,
      });
      if (r.changes > 0) counts.couponInserted++;
    }

    // ── 4a. planned フォロー: レビューが来たら停止 (削除済み含む。削除で再開しない=復帰遷移なし) ──
    const followSuppress = db.prepare(`
      SELECT a.id FROM rakuten_campaign_actions a
       WHERE a.action_type = 'follow' AND a.status = 'planned'
         AND EXISTS(SELECT 1 FROM fact_rakuten_reviews r WHERE r.order_number = a.order_number)
    `).all();
    for (const a of followSuppress) {
      setStatusStmt.run({ id: a.id, status: 'suppressed', reason: 'review_exists', ready_at: null, now });
      counts.suppressed++;
    }

    // ── 4b. planned クーポン: 引き金レビューが全削除されたら取消 (送信前のみ。sent後は関知しない) ──
    const couponCancel = db.prepare(`
      SELECT a.id FROM rakuten_campaign_actions a
       WHERE a.action_type = 'coupon' AND a.status = 'planned'
         AND NOT EXISTS(SELECT 1 FROM fact_rakuten_reviews r
                         WHERE r.order_number = a.order_number AND r.is_deleted = 0)
    `).all();
    for (const a of couponCancel) {
      setStatusStmt.run({ id: a.id, status: 'cancelled', reason: 'review_deleted', ready_at: null, now });
      counts.cancelled++;
    }

    // ── 4c. planned で期限超過 → expired ──
    const expiredRows = db.prepare(`
      SELECT id FROM rakuten_campaign_actions WHERE status = 'planned' AND expires_at <= ?
    `).all(now);
    for (const a of expiredRows) {
      setStatusStmt.run({ id: a.id, status: 'expired', reason: 'expired', ready_at: null, now });
      counts.expired++;
    }

    // ── 4c'. ready のまま期限超過 → expired (ready_at は突合の記録として保持) ──
    // Codex C1-R1 Medium: ready を終端にしない。PR-C4 の送信キューに期限切れが残らないようにする
    const readyExpired = db.prepare(`
      UPDATE rakuten_campaign_actions
         SET status = 'expired', status_reason = 'expired_after_ready', updated_at = ?
       WHERE status = 'ready' AND expires_at <= ?
    `).run(now, now);
    counts.expired += readyExpired.changes;

    // ── 4d. planned で送信予定時刻到来 → 適格性を再評価して ready (shadow では「送ったはず」の記録) ──
    // ready_at が shadow 突合 (PR-C2) の would-send 時刻になる。shadow 中は ready のまま滞留し、
    // PR-C4 の送信ゲート (owner='self' + 期限内) だけが ready → claimed に進める
    const dueRows = db.prepare(`
      SELECT a.id, a.action_type, c.masked_email_enc, c.masked_email_hash, c.purged_at,
             EXISTS(SELECT 1 FROM rakuten_contact_suppressions s WHERE s.email_hash = c.masked_email_hash) AS is_suppressed
        FROM rakuten_campaign_actions a
        LEFT JOIN rakuten_order_contacts c ON c.order_number = a.order_number
       WHERE a.status = 'planned' AND a.scheduled_at <= ?
    `).all(now);
    for (const a of dueRows) {
      let status = 'ready', reason = null, readyAt = now;
      if (!a.masked_email_enc) { status = 'suppressed'; reason = a.purged_at ? 'contact_purged' : 'no_email'; readyAt = null; }
      else if (a.is_suppressed) { status = 'suppressed'; reason = 'suppression_list'; readyAt = null; }
      setStatusStmt.run({ id: a.id, status, reason, ready_at: readyAt, now });
      if (status === 'ready') counts.promotedReady++; else counts.suppressed++;
    }

    // ── 5. ownership (shadow 中に観測した注文は全て vendor 担当として記録) ──
    // action の有無ではなく contacts (=shadow中に受注APIで見えた全注文) を母集合にする
    // (Codex C1-R1 Medium: 未発送などで action 未作成の注文が cutover 時に self 扱いされ
    // vendor と二重送信になる穴を塞ぐ)。action だけあって contacts が無い注文 (no_contact) も合流。
    // PR-C4 の送信ゲートは「ownership 行が無い注文 = 送らない (fail-closed)」とすること
    const r1 = db.prepare(`
      INSERT OR IGNORE INTO rakuten_order_campaign_ownership (order_number, owner, reason, decided_at)
      SELECT c.order_number, 'vendor', 'shadow', ? FROM rakuten_order_contacts c
    `).run(now);
    const r2 = db.prepare(`
      INSERT OR IGNORE INTO rakuten_order_campaign_ownership (order_number, owner, reason, decided_at)
      SELECT DISTINCT a.order_number, 'vendor', 'shadow', ? FROM rakuten_campaign_actions a
    `).run(now);
    counts.ownershipInserted = r1.changes + r2.changes;
  });
  tx();
  return counts;
}

/** 状態サマリ (CLI stats 用。PII なし) */
export function campaignStats(db, nowIso = new Date().toISOString()) {
  const byStatus = db.prepare(`
    SELECT action_type, status, COALESCE(status_reason, '') AS reason, COUNT(*) AS n
      FROM rakuten_campaign_actions
     GROUP BY action_type, status, reason
     ORDER BY action_type, status, reason
  `).all();
  const due24h = db.prepare(`
    SELECT COUNT(*) AS n FROM rakuten_campaign_actions
     WHERE status = 'planned' AND scheduled_at <= ?
  `).get(new Date(Date.parse(nowIso) + DAY_MS).toISOString()).n;
  const readyTodayJst = db.prepare(`
    SELECT COUNT(*) AS n FROM rakuten_campaign_actions WHERE status = 'ready' AND ready_at >= ?
  `).get(new Date(Date.parse(`${jstDateOf(nowIso)}T00:00:00+09:00`)).toISOString()).n;
  const epoch = db.prepare(`SELECT value FROM rakuten_campaign_meta WHERE key = 'coupon_epoch'`).get()?.value || null;
  return { byStatus, due24h, readyTodayJst, epoch };
}
