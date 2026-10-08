-- 0061: 新商品の NE 登録の CSV = 「取り込んだと申告」をしなくても、翌朝の照合 ② が配ったファイルの商品を確かめて NE 確認済みにする (2026-10-08。中原さんの決定 a)
-- 🚨 前提 = 0053 (⑤-2b の表と関数)・0058 (配る道の許可)。どちらも本番に適用済み。番号はマージの順で振り直す (中身は番号に依らない)
--
-- なぜ:
--   0053 の流れは 作る (built) → 配る (issued) → 人が NE で取り込む → 人がポータルで「取り込んだと申告」(sha256・結果) → 翌朝の照合が確かめる。
--   申告をしないと、NE に入っていても ops.record_ne_registration_check は in_ne_undeclared を残すだけで、登録は下書きのまま止まる
--   (10/8 本番の test01: 配った・NE で取り込んで NE の画面で見た・申告はしていない)。中原さん「ここは自動でやりたい」。
--   NE の画面のスクレイピングは NE の決まりで禁止 = しない。確かめの材料は今までどおり翌朝の NE の完全な取得だけ。
-- 申告の代わりの結び付け:
--   申告は sha256 で「配ったファイルそのものを取り込んだ」と結んでいた。申告なしでは「配った時刻 (ops.ne_reg_exports.issued_at) より後に取った
--   NE の完全な取得で、全部の列が配った値 (expected = ファイルの行と同じ中身・0053 の ops.ne_reg_expected_problem が作るときに照らした) と合う」で確かめる。
--   全部の列が合う = そのファイルの値が NE に入っている (どの経路で入ったかは問わない = 結果が同じなら NE 確認済みでよい)。
-- 🆕 #1659 Codex R1 High 1・High 2 (自動で確かめる品目を絞る = ops.ne_reg_auto_block):
--   ・NE の完全な取得 (apps/warehouse/ne-api.js の商品・セット商品の取得の項目) に、単品の JAN・セットの税率・セットの行の順は無い = 比べられない。
--     比べられない列に値を送った品目は自動にしない (申告 = 任意のボタンで sha256 を結ぶと、0053 のまま確かめる):
--       単品で JAN の欄が「empty」でない = jan_not_compared / セット (税率は必ず送る・行の順も比べられない) = set_not_compared
--   ・0058 の配った時の許可の印 (ne_reg_exports.lease_id・lease_compare_run_id) が無い、前からの issued の行 = no_issue_lease
--   ・CSV の行が読めない = no_row
--   自動にしない品目が NE にある = in_ne_undeclared (0053 と同じ・状態は変えない) + 答えの needs_declaration (照合の要約に ℹ️)
-- 残る危なさ (受け入れ・取り込みの証明はしない。中原さんの 10/6 の決定 = 「その日に NE の画面で同じコードの商品を直接作る (決まり違反)」と
--   「保存しておいた古い CSV をもう一度取り込む」の危なさを受け入れた、と同じ種類):
--   ・朝の完全な取得の後から配るまでに、別の経路で同じコード・同じ値の商品が NE に作られた
--   ・今のファイルを取り込まずに、前に保存した古いファイルを配った後に取り込んだ
--   ・(0058 の印の無い前からの issued の行 = 上の no_issue_lease で自動にしない)
--   どれも翌朝の観測は同じ形 = 区別できない。比べた全部の列が配った値と同じなので、NE の中身は登録どおり
-- なにを (申告した品目 = import_declared / 申告の後の partial の扱いは 0053 のまま変えない):
--   1. ops.record_ne_registration_check を置き換える。配ったが申告していない品目 (issued・申告なしの partial = attempt_id が null) も確かめる:
--        取得が配った時刻より前 (同じ時刻も) = 比べない (waiting)
--        観測が信用できない (trusted でない) = waiting
--        NE に無い = waiting (申告が無い = 「取り込めなかった」と決めない = failed にしない)。
--          配ってから 3 日 (record_ne_registration_check の v_days) を過ぎた取得でも無い (「無い」を信じてよい取得のとき) = 「取り込まれていないらしい」を
--          結果の not_imported に出す (照合 ② の要約に ℹ️ で 1 行。失敗にはしない・状態も変えない)
--        NE にある = 全部の列が合えば verified、違う列があれば partial
--      verified にしたら、登録の状態を draft → ne_pending → ne_confirmed (どちらも system・ne_compare) に移す (申告した品目は今までどおり ne_pending → ne_confirmed)
--   2. ops.transition_sku_registration を置き換える。足すのは「draft → ne_pending を照合の確かめ (system) で」の道だけ:
--        根拠 = この SKU の照合の確かめ (ops.ne_reg_checks) が verified・その品目が verified で同じ照合の回・申告なし (attempt_id が null)・回の記録がある
--        (関数が自分で表から読む・呼び手の根拠は受けない = 0053 と同じ)。人の申告の道 (human) はそのまま
--   3. 品目の状態の地図 (ops.ne_reg_item_transition_allowed) に issued → verified / issued → partial を足す
--   4. ops.ne_reg_export_items の ck_nri_declared を「import_declared は申告の試みが要る」にゆるめる (verified / partial は申告なしでもよい)。
--      前からの行は前の CHECK (もっと強い) を満たしている = 新しい CHECK も満たす (表は小さい = 確かめの読みは短い)
--   ・ops.ne_reg_checks の outcome の in_ne_undeclared は前からの記録のために残す (新しくは書かない)
--   ・ops.registration_transition_allowed (0052 の地図) は変えない (draft → ne_pending → ne_confirmed は前からある)
-- 🚨 security definer の関数は search_path = pg_catalog, pg_temp・名前は全部 schema つき・一時の表を使わない・public の実行権なし (0053 / 0058 と同じ)。
--    create or replace は今の権限を残す = 下の権限の節は 0053 と同じ grant を流し直すだけ
-- 🚨 鍵の順は 0053 と同じ: 確かめの鍵 (ops.ne_reg_check) → SKU ごと (sku_id の順) → CSV の鍵 → 行 (for update) → 登録の状態 (SKU の鍵は同じ取引で取り直し = 待たない)
-- 🚨 この migration は商品・仕入先・登録の状態の値を何も変えない (変わるのは次の照合の確かめから)

-- ─── 4. 申告なしの verified / partial を許す (import_declared だけ申告の試みが要る) ───
alter table ops.ne_reg_export_items drop constraint ck_nri_declared;
alter table ops.ne_reg_export_items add constraint ck_nri_declared check ((state = 'import_declared') <= (attempt_id is not null));

-- ─── 3. 品目の状態の地図: 配った (issued) から照合の確かめで verified / partial に (申告なし) ───
create or replace function ops.ne_reg_item_transition_allowed(p_from text, p_to text) returns boolean language sql immutable as $$
  select (p_from || '>' || p_to) = any (array[
    'built>issued', 'built>superseded',
    'issued>import_declared', 'issued>verified', 'issued>partial', 'issued>failed', 'issued>superseded',
    'import_declared>verified', 'import_declared>partial', 'import_declared>failed', 'import_declared>superseded',
    'partial>verified', 'partial>failed', 'partial>superseded'])
$$;

-- ─── 0. 申告なしで自動で確かめてよいか (#1659 Codex R1 High 1・High 2)。null = よい / 理由 ───
--   set_not_compared = セット (税率・行の順は NE の取得に無い) / no_issue_lease = 0058 の配った時の許可の印が無い /
--   no_row = CSV の行が読めない / jan_not_compared = 単品の JAN の欄に値を送った (JAN は NE の取得に無い)。品目が無い = no_item
create function ops.ne_reg_auto_block(p_item_id bigint) returns text language sql stable set search_path = pg_catalog, pg_temp as $$
  select coalesce((
    select case
             when i.sku_kind <> 'single' then 'set_not_compared'
             when e.lease_id is null or e.lease_compare_run_id is null then 'no_issue_lease'
             when r.export_id is null or pg_catalog.jsonb_typeof(r.cells) is distinct from 'array' then 'no_row'
             when pg_catalog.array_position(pg_catalog.string_to_array(e.header, ','), 'jan_code') is not null
                  and (r.cells ->> (pg_catalog.array_position(pg_catalog.string_to_array(e.header, ','), 'jan_code') - 1)) is distinct from 'empty' then 'jan_not_compared'
             else '' end
      from ops.ne_reg_export_items i
      join ops.ne_reg_exports e on e.export_id = i.export_id
      left join ops.ne_reg_export_rows r on r.export_id = i.export_id and r.row_no = i.row_from
     where i.item_id = p_item_id), 'no_item')
$$;
revoke all on function ops.ne_reg_auto_block(bigint) from public;

-- ─── 1. 翌朝の照合の確かめ (0053 の置き換え) ───
/**
 * 3. 確かめる (回の番号だけ・1 回 = 1 つの取引)。受け取りのある回だけ。観測・取得の時刻・「無い」を信じてよいかは関数が残した記録から読む
 *    (残した観測のハッシュを数え直して、受け取りと 1. のときと同じであること = 受け取りの後に変わっていない)。
 * 生きている商品 (issued / import_declared / partial) ごとに:
 *   申告あり (import_declared・申告の後の partial = attempt_id がある) = 0053 のまま:
 *     申告の前の取得・信用できない観測 = waiting / 申告の後の完全な取得に無い = failed (not_in_ne・「無い」を信じてよいときだけ) / ある = 比べる
 *   申告なし (issued・申告なしの partial = attempt_id が null。0061):
 *     配った時刻より前の取得・信用できない観測 = waiting / 無い = waiting (failed にしない。配ってから 3 日 (v_days) を過ぎた取得で
 *     「無い」を信じてよいときは not_imported に出す) / ある = 比べる。ただし ops.ne_reg_auto_block が理由を返す品目 (セット・JAN を送った・
 *     配った時の印が無い) は in_ne_undeclared のまま (状態は変えない・needs_declaration に出す = 申告すると 0053 のまま確かめる)
 *   比べる = 全部の列が合えば verified (+ 登録の状態 draft → ne_pending (申告なしのときだけ) → ne_confirmed = ops.transition_sku_registration が
 *     この確かめの記録を自分で読む)・違う列があれば partial
 * 同じ回の同じ商品は 1 回だけ (再送 = 何もしない)。鍵の順 = 確かめの鍵 → SKU ごと (sku_id の順) → CSV の鍵 → 行
 * 戻り値 { compare_run_id, counts: { outcome: 件数 }, not_imported: [{ code, export_id, issued_at, days }], not_imported_days, needs_declaration: [{ code, export_id, reason }] }
 */
create or replace function ops.record_ne_registration_check(p_run text) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  h          ops.ne_reg_compare_runs%rowtype;
  rc         ops.ne_reg_compare_receipts%rowtype;
  it         record;
  v_obs      jsonb;
  v_fetched  timestamptz;
  v_cmp      jsonb;
  v_out      text;
  v_basis    text;
  v_late     boolean;
  v_days     constant integer := 3;   -- 配ってから何日たっても NE に無ければ「取り込まれていないらしい」と知らせるか (要約だけ・失敗にしない)
  v_counts   jsonb := '{}'::jsonb;
  v_missing  jsonb := '[]'::jsonb;
  v_block    text;
  v_needs    jsonb := '[]'::jsonb;
  v_reg      text;
begin
  if p_run is null or p_run !~ '^mc_[0-9]{8}T[0-9]{9}Z_[0-9a-f]{6}$' then raise exception 'invalid_input: compare_run_id の形が違う: %', p_run using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtext('ops.ne_reg_check'));
  select * into rc from ops.ne_reg_compare_receipts r where r.compare_run_id = p_run;
  if not found then raise exception 'not_sealed: 照合の回 % は最後まで終わった受け取りが無い = 確かめない', p_run using errcode = 'P0001'; end if;
  select * into h from ops.ne_reg_compare_runs r where r.compare_run_id = p_run;
  if h.observation_hash is distinct from rc.observation_hash or ops.ne_reg_observation_hash(p_run) is distinct from rc.observation_hash then
    raise exception 'receipt_mismatch: 照合の回 % の観測が受け取りと違う', p_run using errcode = 'P0001';
  end if;
  -- 鍵: SKU ごと (sku_id の順・保存と同じ鍵) → CSV の鍵
  perform ops.ne_reg_lock_skus((select pg_catalog.array_agg(t.sku_id) from ops.v_ne_reg_targets t
                                 where t.code_norm in (select o.code_norm from ops.ne_reg_compare_observations o where o.compare_run_id = p_run)));
  for it in select i.*, e.declared_at as export_declared_at, e.issued_at as export_issued_at
              from ops.ne_reg_export_items i join ops.ne_reg_exports e on e.export_id = i.export_id
             where i.state in ('issued', 'import_declared', 'partial')
               and i.code_norm in (select o.code_norm from ops.ne_reg_compare_observations o where o.compare_run_id = p_run)
             order by i.sku_id for update of i loop
    if exists (select 1 from ops.ne_reg_checks c where c.compare_run_id = p_run and c.item_id = it.item_id) then continue; end if;
    select o.observation into v_obs from ops.ne_reg_compare_observations o where o.compare_run_id = p_run and o.code_norm = it.code_norm;
    v_fetched := case when it.sku_kind = 'set' then h.sets_at else h.products_at end;
    v_cmp := null;
    v_late := false;
    v_block := null;
    -- 申告あり = 申告の時刻から (0053 のまま)・申告なし = 配った時刻から (0061)
    v_basis := case when it.attempt_id is not null then 'declared' else 'issued' end;
    if v_basis = 'declared' then
      if it.export_declared_at is null or v_fetched <= it.export_declared_at then
        v_out := 'waiting';
      elsif (v_obs -> 'trusted') is distinct from 'true'::jsonb then
        v_out := 'waiting';
      elsif (v_obs -> 'present') is distinct from 'true'::jsonb then
        v_out := case when h.absence_trusted then 'failed' else 'waiting' end;
      else
        v_cmp := ops.ne_reg_compare(it.expected, v_obs);
        v_out := case when (v_cmp -> 'ok') = 'true'::jsonb then 'verified' else 'partial' end;
      end if;
    else
      if it.export_issued_at is null or v_fetched <= it.export_issued_at then
        v_out := 'waiting';   -- 配る前の取得 = 比べない
      elsif (v_obs -> 'trusted') is distinct from 'true'::jsonb then
        v_out := 'waiting';
      elsif (v_obs -> 'present') is distinct from 'true'::jsonb then
        v_out := 'waiting';   -- 申告が無い = 「取り込めなかった」と決めない
        v_late := h.absence_trusted and v_fetched > it.export_issued_at + pg_catalog.make_interval(days => v_days);
      else
        v_cmp := ops.ne_reg_compare(it.expected, v_obs);
        v_block := nullif(ops.ne_reg_auto_block(it.item_id), '');
        if v_block is not null then
          v_out := 'in_ne_undeclared';   -- 比べられない列を送った / 配った時の印が無い = 自動にしない (申告すると 0053 のまま確かめる)
        else
          v_out := case when (v_cmp -> 'ok') = 'true'::jsonb then 'verified' else 'partial' end;
        end if;
      end if;
    end if;
    insert into ops.ne_reg_checks (compare_run_id, item_id, sku_id, fetched_at, outcome, detail)
      values (p_run, it.item_id, it.sku_id, v_fetched, v_out,
              pg_catalog.jsonb_build_object('state_before', it.state, 'basis', v_basis, 'compare', v_cmp, 'present', v_obs -> 'present', 'trusted', v_obs -> 'trusted',
                'not_imported', v_late, 'auto_block', v_block, 'fetch_generation', h.fetch_generation, 'raw_hash', h.raw_hash, 'evidence_sha256', rc.evidence_sha256));
    if v_out = 'verified' then
      update ops.ne_reg_export_items set state = 'verified', verified_run = p_run, verified_at = pg_catalog.now(), state_changed_at = pg_catalog.now(), state_changed_by = 'ne_compare'
       where item_id = it.item_id;
      select r.state into v_reg from ops.master_registrations r where r.sku_id = it.sku_id;
      -- 申告なし = 下書きのまま → 照合の確かめで NE 登録待ちを通って NE 確認済みに (根拠は関数がこの確かめの記録から読む)
      if v_reg = 'draft' and v_basis = 'issued' then
        perform ops.transition_sku_registration(it.sku_id, 'ne_pending', 'system', 'ne_compare', null, '{}'::jsonb, null);
        v_reg := 'ne_pending';
      end if;
      if v_reg = 'ne_pending' then
        perform ops.transition_sku_registration(it.sku_id, 'ne_confirmed', 'system', 'ne_compare', null, '{}'::jsonb, null);
      end if;
    elsif v_out = 'partial' and it.state in ('issued', 'import_declared') then
      update ops.ne_reg_export_items set state = 'partial', state_changed_at = pg_catalog.now(), state_changed_by = 'ne_compare' where item_id = it.item_id;
    elsif v_out = 'failed' then
      update ops.ne_reg_export_items set state = 'failed', failed_reason = 'not_in_ne', state_changed_at = pg_catalog.now(), state_changed_by = 'ne_compare' where item_id = it.item_id;
    end if;
    if v_out = 'in_ne_undeclared' then
      v_needs := v_needs || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code', it.ne_code, 'export_id', it.export_id::text, 'reason', v_block));
    end if;
    if v_late then
      v_missing := v_missing || pg_catalog.jsonb_build_array(pg_catalog.jsonb_build_object('code', it.ne_code, 'export_id', it.export_id::text, 'issued_at', it.export_issued_at,
        'days', pg_catalog.floor(pg_catalog.date_part('epoch', v_fetched - it.export_issued_at) / 86400)::integer));
    end if;
    v_counts := v_counts || pg_catalog.jsonb_build_object(v_out, coalesce((v_counts ->> v_out)::integer, 0) + 1);
  end loop;
  -- 全部の商品が終わったファイルは閉じる
  update ops.ne_reg_exports e set state = 'closed', closed_at = pg_catalog.now(), closed_by = 'ne_compare', close_reason = 'finished'
   where e.state in ('issued', 'declared')
     and not exists (select 1 from ops.ne_reg_export_items i where i.export_id = e.export_id and i.state in ('built', 'issued', 'import_declared', 'partial'));
  return pg_catalog.jsonb_build_object('compare_run_id', p_run, 'counts', v_counts, 'not_imported', v_missing, 'not_imported_days', v_days, 'needs_declaration', v_needs);
end $$;
revoke all on function ops.record_ne_registration_check(text) from public;

-- ─── 2. 登録の状態の関数 (0053 の置き換え・引数は同じ) ───
/**
 * ne_pending・ne_confirmed の根拠は、関数が 0053 の記録から自分で読んで鍵を取る (呼び手の渡す根拠の JSON は信じない = 渡したら拒む caller_evidence)。
 * distributable / available は ④ まで not_ready のまま。
 *   ne_pending   ← (人) この SKU の新規登録の CSV の品目が import_declared・その試み (結果 ok / partial・sha256 = ファイルの記録) がある (0053 のまま)
 *                ← (system・0061) 下書きから: この SKU の照合の確かめ (ops.ne_reg_checks) が verified・その品目が verified で同じ照合の回・
 *                  申告なし (attempt_id が null)・回の記録がある = 申告をしなかった品目を照合が確かめた (照合の確かめの中だけが通る道)
 *   ne_confirmed ← ne_pending から: この SKU の照合の確かめ (ops.ne_reg_checks) が verified・その品目が verified で同じ照合の回・回の記録がある (system)
 *                  quarantined から: NE で見つけた商品の照合の結果の表はまだ無い = not_ready
 */
create or replace function ops.transition_sku_registration(p_sku_id bigint, p_to text, p_actor_type text, p_actor_id text,
                                                           p_reason text default null, p_evidence jsonb default '{}'::jsonb, p_request_id text default null) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  r        ops.master_registrations%rowtype;
  v_ev     jsonb := coalesce(p_evidence, '{}'::jsonb);
  v_rec    record;
  v_event  bigint;
begin
  if p_actor_type is null or p_actor_type not in ('human', 'system') then raise exception 'invalid_input: actor_type は human か system' using errcode = '22023'; end if;
  if p_actor_id is null or pg_catalog.length(p_actor_id) = 0 then raise exception 'invalid_input: 誰が (actor_id) が要る' using errcode = '22023'; end if;
  if pg_catalog.jsonb_typeof(v_ev) <> 'object' then raise exception 'invalid_input: 根拠 (evidence) は object' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended('core.sku:' || p_sku_id::text, 0));
  select * into r from ops.master_registrations x where x.sku_id = p_sku_id for update;
  if not found then raise exception 'no_registration: SKU % に状態の行が無い (使えない商品)', p_sku_id using errcode = 'P0001'; end if;
  if not ops.registration_transition_allowed(r.state, p_to) then
    raise exception 'one_way: 状態は % から % に進めない', r.state, p_to using errcode = 'P0001';
  end if;
  if p_to in ('distributable', 'available') then
    raise exception 'not_ready: % への根拠 (%) の表がまだ無い = 進めない (④ で作る)', p_to, case p_to when 'distributable' then '配る世代' else '場所ごとの受け取り' end
      using errcode = 'P0001';
  end if;
  if p_to in ('ne_pending', 'ne_confirmed') then
    -- NE で見つけた商品 (要確認) を NE 確認済みにする根拠 (照合の結果の表) はまだ無い = 開いていない (呼び手の根拠の有無より先に)
    if p_to = 'ne_confirmed' and r.state = 'quarantined' then
      raise exception 'not_ready: NE で見つけた商品を NE 確認済みにする根拠 (照合の結果の表) はまだ無い' using errcode = 'P0001';
    end if;
    if v_ev <> '{}'::jsonb then
      raise exception 'caller_evidence: % の根拠は関数が記録 (新規登録の CSV・照合の確かめ) から読む。呼び手の根拠は受けない', p_to using errcode = '22023';
    end if;
    if p_to = 'ne_pending' and p_actor_type = 'human' then
      select i.item_id, i.export_id, a.attempt_id, a.sha256, a.result, a.declared_by, a.declared_at into v_rec
        from ops.ne_reg_export_items i
        join ops.ne_reg_exports e on e.export_id = i.export_id
        join ops.ne_reg_attempts a on a.attempt_id = i.attempt_id and a.export_id = i.export_id
       where i.sku_id = p_sku_id and i.state = 'import_declared' and e.state = 'declared' and a.sha256 = e.sha256 and a.result in ('ok', 'partial')
       order by i.item_id desc limit 1
       for share of i, e;
      if not found then
        raise exception 'no_evidence: SKU % に、取り込んだと申告した新規登録の CSV の品目が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('export_id', v_rec.export_id, 'item_id', v_rec.item_id, 'attempt_id', v_rec.attempt_id, 'sha256', v_rec.sha256,
                                            'result', v_rec.result, 'declared_by', v_rec.declared_by, 'declared_at', v_rec.declared_at);
    elsif p_to = 'ne_pending' then
      -- 🆕 0061: 申告をしなかった品目を、配った後の NE の完全な取得で照合が確かめた (system・ne_compare だけ)
      if p_actor_id <> 'ne_compare' then raise exception 'no_evidence: 申告なしの NE 登録待ちは翌朝の照合 (ne_compare) の確かめで' using errcode = '22023'; end if;
      select c.check_id, c.compare_run_id, c.item_id, i.export_id, c.fetched_at, e.issued_at into v_rec
        from ops.ne_reg_checks c
        join ops.ne_reg_export_items i on i.item_id = c.item_id and i.sku_id = c.sku_id
        join ops.ne_reg_exports e on e.export_id = i.export_id
       where c.sku_id = p_sku_id and c.outcome = 'verified' and i.state = 'verified' and i.verified_run = c.compare_run_id and i.attempt_id is null
         and e.issued_at is not null and c.fetched_at > e.issued_at
         and ops.ne_reg_auto_block(i.item_id) = ''   -- 自動で確かめてよい品目だけ (単品・JAN を送っていない・配った時の許可の印がある)
         and exists (select 1 from ops.master_compare_runs m where m.compare_run_id = c.compare_run_id)
       order by c.check_id desc limit 1
       for share of i;
      if not found then
        raise exception 'no_evidence: SKU % に、配った後の NE の完全な取得で全部の列が合った確かめ (verified・申告なし) が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('compare_run_id', v_rec.compare_run_id, 'check_id', v_rec.check_id, 'item_id', v_rec.item_id, 'export_id', v_rec.export_id,
                                            'fetched_at', v_rec.fetched_at, 'issued_at', v_rec.issued_at, 'matched', true, 'declared', false);
    else
      if p_actor_type <> 'system' then raise exception 'no_evidence: NE 確認済みは翌朝の照合 (system) で' using errcode = '22023'; end if;
      select c.check_id, c.compare_run_id, c.item_id, i.export_id, c.fetched_at into v_rec
        from ops.ne_reg_checks c
        join ops.ne_reg_export_items i on i.item_id = c.item_id and i.sku_id = c.sku_id
       where c.sku_id = p_sku_id and c.outcome = 'verified' and i.state = 'verified' and i.verified_run = c.compare_run_id
         and exists (select 1 from ops.master_compare_runs m where m.compare_run_id = c.compare_run_id)
       order by c.check_id desc limit 1
       for share of i;
      if not found then
        raise exception 'no_evidence: SKU % に、NE の完全な取得で全部の列が合った確かめ (verified) が無い', p_sku_id using errcode = '22023';
      end if;
      v_ev := pg_catalog.jsonb_build_object('compare_run_id', v_rec.compare_run_id, 'check_id', v_rec.check_id, 'item_id', v_rec.item_id, 'export_id', v_rec.export_id,
                                            'fetched_at', v_rec.fetched_at, 'matched', true);
    end if;
  else
    -- cancelled = 人が理由を書いてだけ
    if p_actor_type <> 'human' or coalesce(pg_catalog.length(pg_catalog.btrim(p_reason)), 0) = 0 then raise exception 'no_evidence: やめるのは人が理由を書いてだけ' using errcode = '22023'; end if;
  end if;
  perform pg_catalog.set_config('ops.registration_protocol', '1', true);
  update ops.master_registrations set state = p_to, state_changed_at = pg_catalog.now(), state_changed_by = p_actor_id where sku_id = p_sku_id;
  perform pg_catalog.set_config('ops.registration_protocol', '', true);
  insert into ops.master_registration_events (company_id, sku_id, from_state, to_state, actor_type, actor_id, reason, evidence, request_id)
    values (r.company_id, p_sku_id, r.state, p_to, p_actor_type, p_actor_id, p_reason, v_ev, p_request_id) returning event_id into v_event;
  return pg_catalog.jsonb_build_object('sku_id', p_sku_id, 'from', r.state, 'to', p_to, 'event_id', v_event);
end $$;
revoke all on function ops.transition_sku_registration(bigint, text, text, text, text, jsonb, text) from public;

comment on view ops.v_ne_reg_targets is '翌朝の照合 ② が NE の完全な取得の値を送る新規登録の商品 (0053・0061 から配っただけ (issued) の商品も確かめる)';

-- ─── 権限 (0053 と同じ形)。create or replace は今の権限を残す = 照合の確かめは watch_writer だけ (流し直し)・新しい部品はだれにも渡さない ───
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watch_writer') then
    execute 'grant execute on function ops.record_ne_registration_check(text) to watch_writer';
  end if;
end $$;
