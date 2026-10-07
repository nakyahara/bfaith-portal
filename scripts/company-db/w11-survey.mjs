/**
 * w11-survey.mjs — 見張り W11 (注文と出荷の未リンク) の閾値と除外を決めるための **読み取り専用** の集計 (件数だけ。注文番号・宛先などの中身は出さない)
 *
 * 自社発送の注文 (core.orders.shop_code あり = NE を通る) のうち、注文から N 日以上たったものを
 *   モール × 状態のまとまり × 伝票との結び付き (有効な伝票あり / キャンセルの伝票だけ / 番号の合う伝票はあるが結べていない / 伝票なし)
 * で数える。同梱 (複数の注文 → 1 伝票) でまとめられた側の注文が「伝票なし」に何件出るかを見るのが主な目的。
 *
 * 使い方 (miniPC。照会用のロール COMPANY_DB_WATCH_URL で読む = 書けない):
 *   node -r dotenv/config scripts\company-db\w11-survey.mjs            # 直近 90 日・5 日以上たった注文
 *   node -r dotenv/config scripts\company-db\w11-survey.mjs --days 60 --lag 7
 */
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openPgClient } from './migrate.mjs';

/** 集計の本体 (c = { query }。取引は呼ぶ側) */
export async function survey(c, { days = 90, lag = 5, log = console.log } = {}) {
  const DAYS = days, LAG = lag;
  const show = async (label, sql, params = []) => {
    const t0 = Date.now();
    const rows = (await c.query(sql, params)).rows;
    log(`== ${label} (${rows.length} 行・${Date.now() - t0} ms)`);
    for (const r of rows) log(JSON.stringify(r));
  };
  // 注文 1 件ごとの結び付き (有効な伝票 / キャンセルの伝票だけ / 番号の合う伝票はあるが order_id が空 / 伝票なし)
  const BASE = `
    with o as (
      select o.order_id, o.mall, o.scope_key, o.mall_order_no, o.order_date_jst, o.status,
             case when o.status in ('shipped', 'delivered', 'returned') then 'mall_shipped'
                  when o.status in ('confirmed', 'ready', 'on_hold') then 'mall_not_shipped'
                  when o.status = 'new' then 'new'
                  when o.status = 'cancelled' then 'cancelled'
                  else o.status end as grp
        from core.orders o
       where o.company_id = 1 and o.shop_code is not null
         and o.order_date_jst between current_date - $1::int and current_date - $2::int),
    l as (
      select o.*,
             exists (select 1 from core.shipments s where s.order_id = o.order_id and not s.is_cancelled) as has_active,
             exists (select 1 from core.shipments s where s.order_id = o.order_id) as has_any,
             -- 番号の合う伝票 (結べていない = order_id が空)。索引 ix_shipments_unlinked (company_id, shop_code, ne_order_no) に当たる形 (店舗 → 受注番号 = 注文番号から接頭辞を外したもの)
             exists (select 1 from core.ne_shops n join core.shipments s on s.company_id = n.company_id and s.shop_code = n.shop_code and s.order_id is null
                                    and s.ne_order_no = substr(o.mall_order_no, length(n.order_no_prefix) + 1)
                      where n.company_id = 1 and n.mall = o.mall and n.scope_key = o.scope_key and left(o.mall_order_no, length(n.order_no_prefix)) = n.order_no_prefix) as has_unlinked_slip
        from o),
    k as (
      select l.*, case when has_active then 'linked' when has_any then 'linked_cancelled_only' when has_unlinked_slip then 'slip_not_linked' else 'no_slip' end as link
        from l)`;
  await show(`モール × 状態のまとまり × 結び付き (注文日 ${DAYS} 日前〜${LAG} 日前・自社発送)`,
    `${BASE} select mall, grp, link, count(*)::int as n from k group by 1, 2, 3 order by 1, 2, 3`, [DAYS, LAG]);
  await show('モールでは出荷済みなのに伝票なし: 注文日ごと (直近 30 日ぶん。1 日あたり何件増えるか)',
    `${BASE} select mall, order_date_jst::text as d, count(*)::int as n from k where grp = 'mall_shipped' and link = 'no_slip' and order_date_jst >= current_date - 30 - $2::int group by 1, 2 order by 1, 2`, [DAYS, LAG]);
  await show('モールで未発送 (confirmed / ready / on_hold) かつ伝票なし: 注文からの日数',
    `${BASE} select mall, status, case when current_date - order_date_jst <= 7 then 'a_5-7' when current_date - order_date_jst <= 14 then 'b_8-14' when current_date - order_date_jst <= 30 then 'c_15-30' else 'd_31+' end as age, count(*)::int as n
       from k where grp = 'mall_not_shipped' and link <> 'linked' group by 1, 2, 3 order by 1, 2, 3`, [DAYS, LAG]);
  await show('モールで未発送なのに有効な伝票がある (NE では出荷済み?): 伝票の状態',
    `${BASE} select k.mall, k.status, s.status as slip_status, (s.ship_date_jst is not null) as ship_confirmed, count(*)::int as n
       from k join core.shipments s on s.order_id = k.order_id and not s.is_cancelled where k.grp = 'mall_not_shipped' group by 1, 2, 3, 4 order by 1, 2, 3, 4`, [DAYS, LAG]);
  await show('伝票側: 結べていない伝票の理由 (出荷確定日が同じ期間)',
    `select coalesce(mall, '-') as mall, reason, count(*)::int as n from mart.v_shipments_unlinked where company_id = 1 and ship_date_jst between current_date - $1::int and current_date - $2::int group by 1, 2 order by 1, 2`, [DAYS, LAG]);
  await show('参考: 同じ期間の NE 伝票 (店舗ごと・キャンセル)',
    `select s.shop_code, count(*)::int as slips, count(*) filter (where s.is_cancelled)::int as cancelled, count(*) filter (where s.order_id is null)::int as unlinked
       from core.shipments s where s.company_id = 1 and s.order_date_jst between current_date - $1::int and current_date - $2::int group by 1 order by 1`, [DAYS, LAG]);
}

const isMain = !!process.argv[1] && path.resolve(process.argv[1]).toLowerCase() === fileURLToPath(import.meta.url).toLowerCase();
if (isMain) {
  const args = process.argv.slice(2);
  const opt = (name, def) => { const i = args.indexOf(name); return i >= 0 && args[i + 1] ? Number(args[i + 1]) : def; };
  const days = opt('--days', 90), lag = opt('--lag', 5);
  if (!Number.isInteger(days) || days < 7 || days > 400 || !Number.isInteger(lag) || lag < 1 || lag > 60) throw new Error('--days は 7〜400・--lag は 1〜60');
  const url = (process.env.COMPANY_DB_WATCH_URL || '').trim();
  if (!url) throw new Error('COMPANY_DB_WATCH_URL が無い (照会用のロールで読む)');
  const c = await openPgClient(url);
  try {
    await c.query('begin read only');
    await c.query(`set local statement_timeout = '120s'`);
    await survey(c, { days, lag });
    await c.query('rollback');
  } finally {
    await c.end();
  }
}
