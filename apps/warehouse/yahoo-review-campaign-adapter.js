/**
 * yahoo-review-campaign-adapter.js — Yahoo 版レビューメール planner の「注文・宛先」供給源 (P2-Y PR-Y-C1)
 *
 * 楽天版の planner (rakuten-review-campaign-lib.js、MALL_TABLES.yahoo で束縛) は `${T.contacts}` を
 *   order_number / order_datetime / shipping_datetime / masked_email_enc / masked_email_hash / purged_at
 * の列で読む。Yahoo は約款第10条 (購入者PIIを保持しない) のため contacts テーブルを持たず、
 * **既存の受注取込 `raw_yahoo_orders` (非PII) の上に同名の VIEW `yahoo_order_contacts` を張る**:
 *   - 注文単位 (raw は明細行なので GROUP BY order_id)
 *   - キャンセル (order_status=4) とソーシャルギフト (social_gift_type != 0) は母集合から外す (送らない)
 *   - shipping_datetime = ship_date (PR-Y-B で取込) を 'YYYY-MM-DDT00:00:00+09:00' に。未発送 (ship_status != 3 or ship_date 無し) は NULL
 *     → planner は発送済みになるまで待ち、発送日の 10 日後 12:00 にフォローを予定する (楽天版と同じ)
 *   - masked_email_enc = '(api)' の固定値 (宛先は sender が送信直前に VPS /yahoo/orderContact で取る)。
 *     masked_email_hash は NULL (計画時点では suppression 照合しない。送信直前に HMAC で照合 = PR-Y-C4)
 *   - purged_at = NULL (保持していないので purge の概念がない)
 * suppression 表 `yahoo_contact_suppressions` はここで作る (email_hmac、鍵は楽天と別)。
 *
 * ⚠️ 制約: raw_yahoo_orders は受注日ベースの窓 (daily-sync は直近 7 日) で再取得されるため、
 * 受注から 8 日以上あとに発送された注文は ship_date が入らず、フォロー対象にならない (取りこぼし側に倒れる)。
 * stats の unshipped_over_7d でその規模を見張り、多ければ PR-Y-C2 で「未発送の古い注文だけ orderInfo で再取得」を足す。
 */
export const YAHOO_ORDER_STATUS_CANCELLED = '4';
export const YAHOO_SHIP_STATUS_SHIPPED = '3';

export function ensureYahooCampaignSources(db) {
  // raw_yahoo_orders は db.js が作る (PR-Y-B で ship_date / social_gift_type を後付け済み)。無い環境 (テスト) では最小形を作る
  const hasRaw = db.prepare(`SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'raw_yahoo_orders'`).get();
  if (!hasRaw) {
    db.exec(`CREATE TABLE raw_yahoo_orders (
      id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL, order_time TEXT, last_update_time TEXT, order_status TEXT,
      pay_status TEXT, ship_status TEXT, total_price REAL, pay_charge REAL, ship_charge REAL, discount REAL, use_point REAL,
      line_id INTEGER, item_id TEXT, title TEXT, sub_code TEXT, unit_price REAL, original_price REAL, quantity INTEGER,
      item_tax_ratio REAL, coupon_discount REAL, synced_at TEXT, ship_date TEXT, social_gift_type TEXT
    )`);
  } else {
    const cols = db.prepare(`PRAGMA table_info(raw_yahoo_orders)`).all().map((c) => c.name);
    for (const c of ['ship_date', 'social_gift_type']) if (!cols.includes(c)) db.exec(`ALTER TABLE raw_yahoo_orders ADD COLUMN ${c} TEXT`);
  }
  // VIEW は定義変更に追従できるよう毎回作り直す (DROP+CREATE。読み取り専用なのでデータは失わない)
  db.exec(`DROP VIEW IF EXISTS yahoo_order_contacts`);
  db.exec(`CREATE VIEW yahoo_order_contacts AS
    SELECT
      o.order_id                                   AS order_number,
      MIN(o.order_time)                            AS order_datetime,
      -- 全明細が出荷完了 (1 行でも未発送なら NULL = 部分発送では送らない — Codex Y-C1 R1 High)
      CASE WHEN SUM(CASE WHEN COALESCE(o.ship_status, '') != '${YAHOO_SHIP_STATUS_SHIPPED}' THEN 1 ELSE 0 END) = 0
            AND MAX(o.ship_date) GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
           THEN MAX(o.ship_date) || 'T00:00:00+09:00' ELSE NULL END AS shipping_datetime,
      '(api)'                                      AS masked_email_enc,
      NULL                                         AS masked_email_hash,
      NULL                                         AS purged_at,
      NULL                                         AS contact_delete_at,
      MAX(o.synced_at)                             AS fetched_at
    FROM raw_yahoo_orders o
    GROUP BY o.order_id
    -- 1 行でもキャンセル/ソーシャルギフトなら注文ごと除外 (Codex Y-C1 R1 High: MAX だと '4' と '5' の併存で素通りする)
    HAVING SUM(CASE WHEN o.order_status = '${YAHOO_ORDER_STATUS_CANCELLED}' THEN 1 ELSE 0 END) = 0
       AND SUM(CASE WHEN COALESCE(o.social_gift_type, '0') NOT IN ('0', '') THEN 1 ELSE 0 END) = 0`);
  db.exec(`CREATE TABLE IF NOT EXISTS yahoo_contact_suppressions (
    email_hash  TEXT PRIMARY KEY,
    reason      TEXT NOT NULL,
    source      TEXT NOT NULL DEFAULT 'manual',
    evidence    TEXT,
    created_at  TEXT NOT NULL,
    released_by TEXT
  )`);
}

/**
 * ship_date バックフィルの対象注文を選ぶ (PR-Y-C2、backfill-yahoo-ship-date.js が使う)。
 *   ①出荷完了 (全明細 ship_status=3) なのに ship_date が無い注文 … 確実に埋まる = vendor 突合に効く
 *   ②未発送・部分発送のまま受注 7 日を超えた注文 … 受注取込の窓 (7日) の外で発送された可能性 (実測 0.3%)
 *      → バックフィルは ship_status も更新するので、部分発送が完了済みに変わったことも拾える
 * 未発送で 7 日以内は翌朝の通常取込で埋まるので引かない (無駄な API 呼び出しを避ける)。
 * キャンセルは除外 (送信対象にならない)。①→②の順、各群は新しい注文から (突合に効くのは直近)。
 */
export function selectShipDateBackfillTargets(db, { days = 30, limit = 60, nowIso = null } = {}) {
  const now = nowIso ? `'${nowIso.replace(/'/g, '')}'` : `'now'`;
  return db.prepare(`
    SELECT order_id FROM (
      SELECT order_id, MIN(order_time) AS ot,
             MAX(CASE WHEN ship_date IS NOT NULL AND ship_date != '' THEN 1 ELSE 0 END) AS has_date,
             SUM(CASE WHEN order_status = '${YAHOO_ORDER_STATUS_CANCELLED}' THEN 1 ELSE 0 END) AS cancelled,
             SUM(CASE WHEN COALESCE(ship_status, '') != '${YAHOO_SHIP_STATUS_SHIPPED}' THEN 1 ELSE 0 END) AS unshipped_lines,
             SUM(CASE WHEN COALESCE(social_gift_type, '0') NOT IN ('0', '') THEN 1 ELSE 0 END) AS gift_lines
        FROM raw_yahoo_orders
       WHERE order_time >= datetime(${now}, ?)
       GROUP BY order_id)
     WHERE cancelled = 0
       AND gift_lines = 0  -- 既知のソーシャルギフトは送信対象外なので引く価値がない (未取得=NULL は対象のまま)
       AND (has_date = 0 OR unshipped_lines > 0)
       AND (unshipped_lines = 0 OR ot < datetime(${now}, '-7 days'))
     ORDER BY (CASE WHEN unshipped_lines = 0 AND has_date = 0 THEN 0 ELSE 1 END), ot DESC
     LIMIT ?`).all(`-${Number(days)} days`, Number(limit)).map((r) => r.order_id);
}

/** 母集合の概況 (PII なし、stats 用) */
export function yahooContactStats(db) {
  const r = db.prepare(`
    SELECT COUNT(*) AS orders,
           SUM(CASE WHEN shipping_datetime IS NOT NULL THEN 1 ELSE 0 END) AS shipped,
           MIN(order_datetime) AS oldest, MAX(order_datetime) AS newest
      FROM yahoo_order_contacts`).get();
  const excluded = db.prepare(`
    SELECT SUM(CASE WHEN cancelled > 0 THEN 1 ELSE 0 END) AS cancelled,
           SUM(CASE WHEN gift > 0 THEN 1 ELSE 0 END) AS social_gift,
           SUM(CASE WHEN cancelled = 0 AND gift = 0 AND unshipped > 0 AND order_time < ? THEN 1 ELSE 0 END) AS unshipped_over_7d,
           SUM(CASE WHEN cancelled = 0 AND gift = 0 AND unshipped = 0 AND has_date = 0 THEN 1 ELSE 0 END) AS shipped_no_date
      FROM (SELECT order_id, MIN(order_time) order_time,
                   SUM(CASE WHEN order_status = '${YAHOO_ORDER_STATUS_CANCELLED}' THEN 1 ELSE 0 END) cancelled,
                   SUM(CASE WHEN COALESCE(social_gift_type, '0') NOT IN ('0', '') THEN 1 ELSE 0 END) gift,
                   SUM(CASE WHEN COALESCE(ship_status, '') != '${YAHOO_SHIP_STATUS_SHIPPED}' THEN 1 ELSE 0 END) unshipped,
                   MAX(CASE WHEN ship_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]' THEN 1 ELSE 0 END) has_date
              FROM raw_yahoo_orders GROUP BY order_id)`).get(new Date(Date.now() - 7 * 86400000).toISOString());
  // unshipped_over_7d = 受注 7 日超で未発送 (raw の再取得窓 7 日を過ぎると ship_date が入らない = 取りこぼし候補、Codex Y-C1 R1 Medium)
  // shipped_no_date  = 出荷完了なのに ship_date 無し (PR-Y-B 以前の取込。再取得窓内なら翌日埋まる)
  return { ...r, ...excluded };
}
