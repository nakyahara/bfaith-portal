-- 0059 広げる道で listing_components.amazon (Amazon SKU の対応) を足せるようにする (2026-10-07。Amazon SKU の対応の PR-B。計画 = amazon_min_plan.md §2 PR-B・
--   Codex の計画レビュー R2 の Medium 3)
-- 🚨 番号は仮置き (Amazon 財務などほかの PR と取り合いになりうる = マージの直前に空き番号へ付け替える)。積み方: 0058 (広げる道 PR-1) → この 0059
--
-- なぜ: 0058 の広げる道は skus.sku_kind だけを足せた。判定の本体 ops._widen_judge は、足すキーに関係なく区分 (sku_kind) の判断の記録 (held 0 など) と
--   最終形を求める = Amazon の対応だけを足す日に区分の食い違いが 1 件あると止まる (sku_kind はもう company = 夜間ロードは食い違いを held に記録し続ける)。
-- なにを (0058 の関数を create or replace で置き換える。0058 が入った DB の今の動き = sku_kind の守り・許可・照合 ② の close / record・復元の停止 は変えない):
--   1. ops.master_widen_allowed_keys() = {listing_components.amazon, skus.sku_kind}
--   2. ops._widen_judge: 区分の判断の記録 (sku_kind の held・unverifiable・形の版) と最終形 (⑤⑥) は、skus.sku_kind を足す試みのときだけ見る。
--      listing_components.amazon を足す試みのときだけ、消えた対応 (ops.amazon_map_lost_listings()) = 0 件・active の対応が 1 件以上 (移行が済んだ) を見る。
--      ほかの判定 (試み・段階・epoch・足すだけ・ack・手の入口の停止・試みの中の 2 つのロード・材料) は今までどおり全部の試みで見る。
--      check (ops.widen_check_readonly) と widen (ops.widen_master_ownership) は今までどおり同じ本体を呼ぶ = 同じ判定を両方で流し直す
--   3. ops.widen_master_ownership: listing_components.amazon を足すときだけ、写しの証拠に amazon_map (古い表のハッシュ = Company DB のハッシュ・
--      ハッシュを作った行の数) が要る。行の数は DB が鍵の後に数え直した数 (本体の counts) と同じであること (CLI が同じ取引で鍵の後に読む)
--   4. ops.widen_amazon_map_counts(会社) = 本体と widen が使う数 (消えた対応・active の対応・その構成の行)
-- 🚨 全部の関数: search_path = pg_catalog, pg_temp・完全修飾・REVOKE EXECUTE FROM PUBLIC・要るロールだけ grant (0058 と同じ形)。一時の表を使わない
-- 🚨 作らないもの: 古い表 (miniPC の SQLite) のハッシュを DB で作り直すこと (DB は SQLite を読めない = CLI が鍵の後に読んで照らし、DB は数とハッシュの形を照らす)

-- ─── 1. 広げてよいキー (PR-8 (narrow + ロールの分離) の前に広げてよいのは skus.sku_kind と listing_components.amazon) ───
create or replace function ops.master_widen_allowed_keys() returns text[] language sql immutable set search_path = pg_catalog, pg_temp as $$
  select array['listing_components.amazon', 'skus.sku_kind']::text[]
$$;
revoke all on function ops.master_widen_allowed_keys() from public;

-- ─── 4. Amazon SKU の対応の数 (本体と widen が使う・何も書かない・どのロールにも EXECUTE を与えない) ───
--   amazon_map_lost = 変更の記録にあるのに行が無い出品 (0054。会社によらず全部)・amazon_map_active = active の対応・amazon_map_active_components = その構成の行
--   (readCompanyAmazonMapCanon が写しの決まった並べ方を作る行と同じ数え方)
create function ops.widen_amazon_map_counts(p_company_id integer) returns jsonb
  language sql stable set search_path = pg_catalog, pg_temp as $$
  select pg_catalog.jsonb_build_object(
    'amazon_map_lost', (select pg_catalog.count(*) from ops.amazon_map_lost_listings()),
    'amazon_map_active', (select pg_catalog.count(*) from core.amazon_sku_maps m where m.company_id = p_company_id and m.state = 'active'),
    'amazon_map_active_components', (select pg_catalog.count(*) from core.amazon_sku_maps m join core.listing_components c on c.listing_id = m.listing_id
                                      where m.company_id = p_company_id and m.state = 'active'))
$$;
revoke all on function ops.widen_amazon_map_counts(integer) from public;

-- ─── 2. 判定の本体 (0058 の §10 を置き換え)。何も書かない・鍵を取らない・どのロールにも EXECUTE を与えない (DEFINER の 2 つから呼ぶ) ───
-- 戻り値 = { ok, problems: [...], counts: {...}, loads: { recovery, prepared } (commit_seq は文字 = JS の Number にしない), acks: [...] }
create or replace function ops._widen_judge(p_widen_prepare_id uuid, p_company_id integer) returns jsonb
  language plpgsql stable set search_path = pg_catalog, pg_temp as $$
declare
  v_problems text[] := '{}';
  v_counts   jsonb := '{}'::jsonb;
  v_acks     jsonb := '[]'::jsonb;
  v_loads    jsonb := '{}'::jsonb;
  a          ops.master_widen_attempts;
  st         ops.master_ownership_state;
  v_phase    text;
  v_added    text[];
  v_hosts    text[] := ops.master_cutover_required_hosts();
  v_fresh    interval := make_interval(mins => ops.master_cutover_ack_fresh_minutes());
  v_now      timestamptz := clock_timestamp();
  v_host     text;
  v_n        integer;
  k          record;
  v_want     text[];
  v_got      text[];
  v_stop_at  timestamptz;
  c1         ops.master_load_commits;
  c2         ops.master_load_commits;
  v_ncommits bigint;
  m          record;
  v_ts       timestamptz;
  v_skus     jsonb;
  v_kind     jsonb;
  v_unv      jsonb;
  v_shape    jsonb;
  v_affected bigint;
  v_kind_added boolean;   -- 0059: skus.sku_kind を足す試み = 区分の判断の記録と最終形を見る
  v_amz_added  boolean;   -- 0059: listing_components.amazon を足す試み = 消えた対応 0・active の対応 1 件以上を見る
  v_amz      jsonb;
begin
  if p_company_id is distinct from 1 then
    return jsonb_build_object('ok', false, 'problems', jsonb_build_array('unsupported_company: 会社 ' || coalesce(p_company_id::text, 'null') || ' (今は会社 1 だけ)'), 'counts', '{}'::jsonb);
  end if;
  -- 1. 試み・段階・epoch・足すだけ
  select * into a from ops.master_widen_attempts w where w.widen_prepare_id = p_widen_prepare_id;
  if not found then
    return jsonb_build_object('ok', false, 'problems', jsonb_build_array('attempt_missing: 試み ' || coalesce(p_widen_prepare_id::text, 'null') || ' が無い'), 'counts', '{}'::jsonb);
  end if;
  v_kind_added := 'skus.sku_kind' = any(a.added_keys);
  v_amz_added := 'listing_components.amazon' = any(a.added_keys);
  if a.state <> 'prepared' then v_problems := v_problems || format('attempt_not_prepared: 試みの状態が %s', a.state); end if;
  select c.phase into v_phase from ops.master_cutover_state c where c.id = 1;
  if v_phase is distinct from 'new_open' then v_problems := v_problems || format('phase_not_new_open: 段階が %s', coalesce(v_phase, '読めない')); end if;
  select * into st from ops.master_ownership_state s where s.id = 1;
  if not found then
    v_problems := v_problems || 'epoch_missing: 持ち主の epoch の行が無い'::text;
  else
    if st.prepared_hash is distinct from a.prepared_hash or st.prepared_at is distinct from a.prepared_at then
      v_problems := v_problems || 'prepared_changed: prepared が試みのものでない (cancel / 別の prepare)'::text;
    end if;
    if st.active_hash is distinct from a.active_hash then v_problems := v_problems || 'active_changed: prepare の後に active が変わった'::text; end if;
    if ops.ownership_hash(st.active_map) is distinct from st.active_hash then v_problems := v_problems || 'epoch_broken: active のハッシュが中身と違う'::text; end if;
    if st.prepared_map is not null and ops.ownership_hash(st.prepared_map) is distinct from st.prepared_hash then v_problems := v_problems || 'epoch_broken: prepared のハッシュが中身と違う'::text; end if;
  end if;
  if ops.ownership_hash(a.prepared_map) is distinct from a.prepared_hash then v_problems := v_problems || 'attempt_broken: 試みの持ち主表のハッシュが中身と違う'::text; end if;
  select coalesce(array_agg(p.key order by p.key), '{}') into v_added from jsonb_each_text(a.prepared_map) p
   where p.value = 'company' and (st.active_map ->> p.key) is distinct from 'company';
  if v_added is distinct from (select array_agg(x order by x) from unnest(a.added_keys) x) then
    v_problems := v_problems || format('not_additive: 足すキーが試みと違う (今 %s・試み %s)', array_to_string(v_added, ','), array_to_string(a.added_keys, ','));
  end if;
  if exists (select 1 from jsonb_each_text(st.active_map) x where x.value = 'company' and (a.prepared_map ->> x.key) is distinct from 'company') then
    v_problems := v_problems || 'not_additive: prepared が active の company のキーを load に戻す'::text;
  end if;
  if not (a.added_keys <@ ops.master_widen_allowed_keys()) then
    v_problems := v_problems || format('key_not_allowed: 広げてよいのは %s だけ (PR-8 の前)', array_to_string(ops.master_widen_allowed_keys(), ','));
  end if;

  -- 2. 書き手の ack (全部のプロセス・2 版・prepare の後・書きかけ 0・capable ⊇ 足すキー・試みの manifest・active / prepared を見た)
  foreach v_host in array v_hosts loop
    v_n := 0;
    for k in select distinct on (g.instance_id) g.* from ops.master_legacy_gate_acks g where g.host = v_host order by g.instance_id, g.acked_at desc, g.ack_id desc loop
      if k.inflight_count <> 0 or k.oldest_inflight_at is not null then
        v_problems := v_problems || format('ack: %s/%s: 書きかけが %s 件ある', v_host, k.instance_id, k.inflight_count);
      end if;
      if k.stopped then continue; end if;
      if k.acked_at < v_now - v_fresh then
        v_problems := v_problems || format('ack: %s/%s: 黙っている (最後の記録 %s。止めたプロセスなら stopped の記録を書く)', v_host, k.instance_id, k.acked_at);
        continue;
      end if;
      v_n := v_n + 1;
      if k.ack_version <> 2 then v_problems := v_problems || format('ack: %s/%s: 記録が 1 版 (active / prepared を見た証拠が無い = 新しい build の門が要る)', v_host, k.instance_id);
      else
        if k.acked_at <= a.prepared_at then v_problems := v_problems || format('ack: %s/%s: 記録が prepare の前', v_host, k.instance_id); end if;
        if k.active_hash_seen is distinct from a.active_hash then v_problems := v_problems || format('ack: %s/%s: 見た active が違う', v_host, k.instance_id); end if;
        if k.prepared_hash_seen is distinct from a.prepared_hash then v_problems := v_problems || format('ack: %s/%s: 見た prepared が試みのものでない', v_host, k.instance_id); end if;
        if not (a.added_keys <@ k.capable) then v_problems := v_problems || format('ack: %s/%s: build が足すキー (%s) を company にできない', v_host, k.instance_id, array_to_string(a.added_keys, ',')); end if;
      end if;
      if k.manifest_hash is distinct from a.manifest_hash then v_problems := v_problems || format('ack: %s/%s: 古い入口の一覧が試みのものと違う', v_host, k.instance_id); end if;
      if k.phase_seen is distinct from 'new_open' then v_problems := v_problems || format('ack: %s/%s: 見た段階が %s', v_host, k.instance_id, k.phase_seen); end if;
      v_acks := v_acks || jsonb_build_array(jsonb_build_object('ack_id', k.ack_id::text, 'host', k.host, 'instance_id', k.instance_id, 'build_id', k.build_id, 'acked_at', k.acked_at));
    end loop;
    if v_n = 0 then v_problems := v_problems || format('ack: %s: %s 分以内の記録が無い', v_host, ops.master_cutover_ack_fresh_minutes()); end if;
  end loop;

  -- 3. 手の入口の停止 = 足すキーに関係する manual の入口と完全一致 (DB の時刻・prepare の後)
  v_want := ops.widen_required_manual_entries(a.manifest_hash, a.added_keys);
  select coalesce(array_agg(s.entry_id order by s.entry_id), '{}'), max(s.stopped_at) into v_got, v_stop_at
    from ops.master_widen_manual_stops s where s.widen_prepare_id = a.widen_prepare_id;
  if v_got is distinct from v_want then
    v_problems := v_problems || format('manual_stops: 止めた手の入口 (%s) が要る入口 (%s) と同じでない', array_to_string(v_got, ','), array_to_string(v_want, ','));
  end if;
  if exists (select 1 from ops.master_widen_manual_stops s where s.widen_prepare_id = a.widen_prepare_id and s.stopped_at <= a.prepared_at) then
    v_problems := v_problems || 'manual_stops: 停止の記録が prepare の前'::text;
  end if;
  v_stop_at := greatest(coalesce(v_stop_at, a.prepared_at), a.prepared_at);

  -- 4. 試みの中のロード = base より後の commit がちょうど 2 つ (回収 = active → prepared = 試みの prepared・prepared が全体の最新)
  select count(*) into v_ncommits from ops.master_load_commits x where x.commit_seq > a.base_commit_seq;
  v_counts := v_counts || jsonb_build_object('commits_after_base', v_ncommits, 'base_commit_seq', a.base_commit_seq::text);
  if v_ncommits <> 2 then
    v_problems := v_problems || format('loads: 試みの中の commit が %s 個 (ちょうど 2 つ = 回収のロード → prepared のロード。夜間ロードが挟まった / prepared を 2 回流した = 試みを作り直す)', v_ncommits);
  else
    select * into c1 from ops.master_load_commits x where x.commit_seq > a.base_commit_seq order by x.commit_seq asc limit 1;
    select * into c2 from ops.master_load_commits x where x.commit_seq > a.base_commit_seq order by x.commit_seq desc limit 1;
    v_loads := jsonb_build_object('recovery', jsonb_build_object('commit_seq', c1.commit_seq::text, 'run_id', c1.ingest_run_id, 'epoch', c1.epoch),
                                  'prepared', jsonb_build_object('commit_seq', c2.commit_seq::text, 'run_id', c2.ingest_run_id, 'epoch', c2.epoch));
    if c1.epoch <> 'active' or c1.ownership_hash is distinct from a.active_hash then
      v_problems := v_problems || format('loads: 1 つ目 (%s) が回収のロード (epoch active・持ち主 = 今の active) でない (%s)', c1.ingest_run_id, c1.epoch);
    end if;
    if c2.epoch <> 'prepared' or c2.ownership_hash is distinct from a.prepared_hash then
      v_problems := v_problems || format('loads: 2 つ目 (%s) が試みの prepared のロードでない (%s)', c2.ingest_run_id, c2.epoch);
    end if;

    -- 5. ①⑦② 材料 (4 行・matched・ロードの中で世代が同じ・2 つのロードで世代とハッシュが同じ・規則の指紋・取得の時刻)
    select count(*) into v_n from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) and lm.entity in ('products', 'set_components');
    if v_n <> 4 then v_problems := v_problems || format('material: 2 つのロード × products・set_components の 4 行がそろっていない (%s 行)', v_n); end if;
    if exists (select 1 from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) and lm.status <> 'matched') then
      v_problems := v_problems || 'material: matched でない材料がある (世代と中身が合わない / 世代が無い)'::text;
    end if;
    if (select count(distinct lm.generation_id) from ops.load_materials lm where lm.ingest_run_id = c1.ingest_run_id) <> 1
       or (select count(distinct lm.generation_id) from ops.load_materials lm where lm.ingest_run_id = c2.ingest_run_id) <> 1
       or exists (select 1 from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) and lm.generation_id is null) then
      v_problems := v_problems || 'material: 同じロードの中で products と set_components の世代が違う'::text;
    end if;
    if exists (select 1 from ops.load_materials x join ops.load_materials y on y.entity = x.entity
                where x.ingest_run_id = c1.ingest_run_id and y.ingest_run_id = c2.ingest_run_id
                  and (x.generation_id is distinct from y.generation_id or x.content_hash is distinct from y.content_hash)) then
      v_problems := v_problems || 'material: 2 つのロードで材料の世代・ハッシュが違う'::text;
    end if;
    if exists (select 1 from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) and lm.rule_fingerprint is distinct from a.loader_fingerprint) then
      v_problems := v_problems || 'material: ロードの規則の指紋が 2 つで違う / 試みの版 (loader_fingerprint) と違う'::text;
    end if;
    for m in select lm.entity, lm.ingest_run_id, lm.source_complete_at from ops.load_materials lm where lm.ingest_run_id in (c1.ingest_run_id, c2.ingest_run_id) order by lm.ingest_run_id, lm.entity loop
      v_ts := ops.rfc3339_ts(m.source_complete_at);
      if v_ts is null then
        v_problems := v_problems || format('material: %s の取得の時刻 %s が RFC 3339 (明示の offset つき) でない', m.entity, coalesce(m.source_complete_at, 'null'));
      elsif v_ts <= v_stop_at then
        v_problems := v_problems || format('material: %s の取得の時刻 %s が手の入口の停止 (%s) の前', m.entity, m.source_complete_at, v_stop_at);
      elsif v_ts > v_now + interval '5 minutes' then
        v_problems := v_problems || format('material: %s の取得の時刻 %s が DB の今 + 5 分より先', m.entity, m.source_complete_at);
      end if;
    end loop;

    -- 6. ③④ prepared のロードの判断の記録 (section skus の payload.sku_kind・format = sku-kind-v1)。🆕 0059: skus.sku_kind を足す試みのときだけ
    --    (sku_kind がもう company の後に Amazon だけを足す日は、夜間ロードが区分の食い違いを held に記録し続ける = 見ない)
    if v_kind_added then
      select ld.payload into v_skus from ops.load_decisions ld where ld.ingest_run_id = c2.ingest_run_id and ld.section = 'skus';
      v_kind := case when jsonb_typeof(v_skus) = 'object' then v_skus -> 'sku_kind' end;
      if v_kind is null or jsonb_typeof(v_kind) <> 'object' then
        v_problems := v_problems || 'decisions: prepared のロードの判断の記録に sku_kind が無い'::text;
      else
        if (v_kind ->> 'format') is distinct from 'sku-kind-v1' or jsonb_typeof(v_kind -> 'format') is distinct from 'string' then
          v_problems := v_problems || format('decisions: sku_kind の形の版が %s (sku-kind-v1 だけ)', coalesce(v_kind ->> 'format', 'なし'));
        end if;
        if jsonb_typeof(v_kind -> 'held') is distinct from 'array' then
          v_problems := v_problems || 'decisions: sku_kind.held が配列でない'::text;
        elsif exists (select 1 from jsonb_array_elements(v_kind -> 'held') h where jsonb_typeof(h) <> 'string') then
          v_problems := v_problems || 'decisions: sku_kind.held が文字の配列でない'::text;
        elsif jsonb_array_length(v_kind -> 'held') <> 0 then
          v_problems := v_problems || format('decisions: sku_kind.held が %s 件 (区分の食い違いが残っている = 0 だけ)', jsonb_array_length(v_kind -> 'held'));
        end if;
        v_counts := v_counts || jsonb_build_object('held', case when jsonb_typeof(v_kind -> 'held') = 'array' then jsonb_array_length(v_kind -> 'held') end);
        v_unv := v_kind -> 'unverifiable';
        if jsonb_typeof(v_unv) is distinct from 'array' then
          v_problems := v_problems || 'decisions: sku_kind.unverifiable が配列でない'::text;
        elsif exists (select 1 from jsonb_array_elements(v_unv) u
                       where case
                         when jsonb_typeof(u) <> 'object' then true
                         when exists (select 1 from jsonb_object_keys(u) kk where kk not in ('reason', 'raw_code', 'code_norm')) then true
                         when jsonb_typeof(u -> 'reason') is distinct from 'string' or jsonb_typeof(u -> 'raw_code') is distinct from 'string' then true
                         when (u ->> 'reason') not in ('empty_code', 'unknown_kind', 'norm_collision') then true
                         when (u ->> 'reason') = 'empty_code' then not (jsonb_typeof(u -> 'code_norm') = 'null' and core.norm_code(u ->> 'raw_code') = '')
                         when jsonb_typeof(u -> 'code_norm') is distinct from 'string' then true
                         else (u ->> 'code_norm') = '' or core.norm_code(u ->> 'raw_code') is distinct from (u ->> 'code_norm') end) then
          v_problems := v_problems || 'decisions: sku_kind.unverifiable の行の形が違う ({reason, raw_code, code_norm}・core.norm_code(raw_code) = code_norm)'::text;
        else
          select count(distinct s.sku_id) into v_affected from core.skus s
           where s.company_id = p_company_id and s.code_norm in (select u ->> 'code_norm' from jsonb_array_elements(v_unv) u where (u ->> 'reason') <> 'empty_code');
          v_counts := v_counts || jsonb_build_object('unverifiable', jsonb_array_length(v_unv), 'affected_existing_cdb', v_affected);
          if v_affected <> 0 then
            v_problems := v_problems || format('decisions: 区分を確かめられない材料の行が C の SKU %s 件に当たる (NE で直してから試みを作り直す)', v_affected);
          end if;
        end if;
      end if;
    end if;
  end if;

  -- 7. ⑤⑥ 最終形。🆕 0059: skus.sku_kind を足す試みのときだけ (足した後は G19 が commit のときに守る)
  if v_kind_added then
    v_shape := ops.sku_kind_shape_counts(p_company_id);
    v_counts := v_counts || v_shape;
    if (v_shape ->> 'single_product_mismatch')::bigint <> 0 then v_problems := v_problems || format('shape: 区分と product_id の不整合が %s 件', v_shape ->> 'single_product_mismatch'); end if;
    if (v_shape ->> 'non_set_parent_components')::bigint <> 0 then v_problems := v_problems || format('shape: セットでない親の構成が %s 件', v_shape ->> 'non_set_parent_components'); end if;
  end if;

  -- 8. 🆕 0059: Amazon SKU の対応 (listing_components.amazon を足す試みのときだけ) = 消えた対応 0 件・active の対応が 1 件以上 (移行が済んだ)。
  --    古い表と Company DB のハッシュの一致は CLI が鍵の後に読んで照らし、widen が写しの証拠 (amazon_map) の数をこの数と照らす
  if v_amz_added then
    v_amz := ops.widen_amazon_map_counts(p_company_id);
    v_counts := v_counts || v_amz;
    if (v_amz ->> 'amazon_map_lost')::bigint <> 0 then
      v_problems := v_problems || format('amazon_map: 消えた対応が %s 件 (変更の記録にあるのに行が無い = trigger を止めて消された。ops.amazon_map_lost_listings() を見て戻す)', v_amz ->> 'amazon_map_lost');
    end if;
    if (v_amz ->> 'amazon_map_active')::bigint < 1 then
      v_problems := v_problems || 'amazon_map: active の対応が 0 件 (移行 amazon-map-migrate.mjs --apply がまだ = 古い表を移してから)'::text;
    end if;
  end if;

  return jsonb_build_object('ok', coalesce(array_length(v_problems, 1), 0) = 0, 'problems', to_jsonb(v_problems), 'counts', v_counts, 'loads', v_loads, 'acks', v_acks,
    'widen_prepare_id', a.widen_prepare_id, 'added_keys', to_jsonb(a.added_keys), 'stop_at', v_stop_at, 'checked_at', v_now);
end $$;
revoke all on function ops._widen_judge(uuid, integer) from public;

-- ─── 3. widen (apply・0058 の §11 を置き換え)。DB の持ち主だけ。鍵 = epoch の排他 → 段階の排他 → マスタの書き込みの排他 (自分で取ったことを確かめる) → 判定の本体 →
--   写しの証拠 (🆕 Amazon を足すときは amazon_map も) → active ← prepared (prepared を消す) → 試みを widened → 出来事 → 段階の owner_hash → 段階の出来事 (同じ取引・G3)
create or replace function ops.widen_master_ownership(p_widen_prepare_id uuid, p_company_id integer, p_actor text, p_evidence jsonb) returns jsonb
  language plpgsql security definer set search_path = pg_catalog, pg_temp as $$
declare
  v       jsonb;
  a       ops.master_widen_attempts;
  v_seq   text;
  v_ev    jsonb;
  v_am    jsonb;
begin
  if p_company_id is distinct from 1 then
    raise exception 'unsupported_company: 会社 % (今は会社 1 だけ。持ち主の epoch は全社で 1 つ)', p_company_id using errcode = '22023';
  end if;
  if not coalesce(ops.session_is_db_owner(), false) then
    raise exception 'widen_owner_only: 広げる道の操作は DB の持ち主だけ (session_user %)', session_user using errcode = '42501';
  end if;
  if p_actor is null or length(btrim(p_actor)) = 0 or length(p_actor) > 200 then raise exception 'invalid_input: actor (1〜200 字) が要る' using errcode = '22023'; end if;
  perform pg_catalog.pg_advisory_xact_lock(ops.master_ownership_lock_key());
  perform pg_catalog.pg_advisory_xact_lock(hashtext('ops.master_cutover'));
  perform pg_catalog.pg_advisory_xact_lock(core.master_write_lock_key());
  if not (ops.holds_xact_advisory(ops.master_ownership_lock_key(), true) and ops.holds_xact_advisory(hashtext('ops.master_cutover')::bigint, true)
          and ops.holds_xact_advisory(core.master_write_lock_key(), true)) then
    raise exception 'widen_locks: epoch・段階・マスタの書き込みの排他の鍵を持っていない' using errcode = '55000';
  end if;
  perform 1 from ops.master_ownership_state s where s.id = 1 for update;
  select * into a from ops.master_widen_attempts w where w.widen_prepare_id = p_widen_prepare_id for update;
  v := ops._widen_judge(p_widen_prepare_id, p_company_id);
  if not (v ->> 'ok')::boolean then
    raise exception 'widen_rejected: %', (select string_agg(x, ' / ') from jsonb_array_elements_text(v -> 'problems') x) using errcode = 'P0001', detail = v::text;
  end if;
  -- 6. 写しの証拠 (activate と同じ・CLI が miniPC の作り直しと確かめから集める)。読んだロード = 試みの prepared のロード
  v_seq := v #>> '{loads,prepared,commit_seq}';
  if p_evidence is null or jsonb_typeof(p_evidence) <> 'object' then raise exception 'evidence_invalid: 写しの証拠 (object) が要る' using errcode = '22023'; end if;
  if jsonb_typeof(p_evidence -> 'load_commit_seq') not in ('string', 'number') or (p_evidence ->> 'load_commit_seq') is distinct from v_seq then
    raise exception 'evidence_invalid: 写しの世代が読んだロード (%) が試みの prepared のロード (%) でない', p_evidence ->> 'load_commit_seq', v_seq using errcode = '22023';
  end if;
  if coalesce(length(p_evidence ->> 'build_id'), 0) = 0 or coalesce(length(p_evidence ->> 'generation_id'), 0) = 0 then
    raise exception 'evidence_invalid: 作り直し (build_id) と写しの世代 (generation_id) が要る' using errcode = '22023';
  end if;
  -- 🆕 0059: Amazon SKU の対応を足す = 古い表 (miniPC の m_sku_master / m_sku_components) のハッシュ = Company DB の読み直しのハッシュ (CLI が鍵の後に同じ取引で読む)・
  --   ハッシュを作った行の数 = DB がこの鍵の後に数えた数 (本体の counts)。違う = 読んだ後に対応が変わった / 別の DB を読んだ
  if 'listing_components.amazon' = any(a.added_keys) then
    v_am := p_evidence -> 'amazon_map';
    if jsonb_typeof(v_am) is distinct from 'object' or jsonb_typeof(v_am -> 'legacy_hash') is distinct from 'string' or jsonb_typeof(v_am -> 'company_hash') is distinct from 'string'
       or (v_am ->> 'legacy_hash') !~ '^[0-9a-f]{64}$' then
      raise exception 'evidence_invalid: Amazon SKU の対応を足すには、古い表と Company DB のハッシュ (amazon_map.legacy_hash・company_hash = 64 桁の 16 進) が要る' using errcode = '22023';
    end if;
    if (v_am ->> 'company_hash') is distinct from (v_am ->> 'legacy_hash') then
      raise exception 'evidence_invalid: 古い表のハッシュ % が Company DB のハッシュ % と違う (広げない)', left(v_am ->> 'legacy_hash', 12), left(v_am ->> 'company_hash', 12) using errcode = '22023';
    end if;
    if ops.jsonb_nonneg_bigint(v_am -> 'master_rows') is distinct from (v #>> '{counts,amazon_map_active}')::bigint
       or ops.jsonb_nonneg_bigint(v_am -> 'component_rows') is distinct from (v #>> '{counts,amazon_map_active_components}')::bigint then
      raise exception 'evidence_invalid: ハッシュを作った行の数 (対応 %・構成 %) が DB の今 (対応 %・構成 %) と違う (読んだ後に対応が変わった)',
        v_am ->> 'master_rows', v_am ->> 'component_rows', v #>> '{counts,amazon_map_active}', v #>> '{counts,amazon_map_active_components}' using errcode = '22023';
    end if;
  end if;
  v_ev := jsonb_build_object('widen_prepare_id', a.widen_prepare_id, 'evidence', p_evidence, 'loads', v -> 'loads', 'counts', v -> 'counts');
  perform pg_catalog.set_config('ops.widen_protocol', '1', true);
  update ops.master_ownership_state set active_hash = prepared_hash, active_map = prepared_map, activated_at = now(), activated_by = btrim(p_actor), activated_evidence = v_ev,
         prepared_hash = null, prepared_map = null, prepared_at = null, prepared_by = null, updated_at = now() where id = 1;
  update ops.master_widen_attempts set state = 'widened', closed_at = clock_timestamp(), closed_by = btrim(p_actor) where widen_prepare_id = a.widen_prepare_id;
  perform pg_catalog.set_config('ops.widen_protocol', '', true);
  insert into ops.master_ownership_events (action, ownership_hash, ownership, actor, evidence) values ('widen', a.prepared_hash, a.prepared_map, btrim(p_actor), v_ev);
  insert into ops.master_widen_events (widen_prepare_id, action, actor, detail) values (a.widen_prepare_id, 'widen', btrim(p_actor), v || jsonb_build_object('evidence', p_evidence));
  -- 段階は new_open のまま、持ち主表のハッシュだけ付け替える (0051 の守り = 設定 ops.cutover_protocol・0055 の守り = owner_hash = active・prepared が無い)
  perform pg_catalog.set_config('ops.cutover_protocol', '1', true);
  update ops.master_cutover_state set owner_hash = a.prepared_hash, changed_at = now(), changed_by = btrim(p_actor),
         note = left('widen ' || array_to_string(a.added_keys, ',') || ' (' || a.widen_prepare_id::text || ')', 500)
   where id = 1;
  perform pg_catalog.set_config('ops.cutover_protocol', '', true);
  insert into ops.master_cutover_events (from_phase, to_phase, actor, evidence, acks, note)
    values ('new_open', 'new_open', btrim(p_actor), v_ev || jsonb_build_object('action', 'widen', 'owner_hash', a.prepared_hash), coalesce(v -> 'acks', '[]'::jsonb),
            left('widen ' || array_to_string(a.added_keys, ','), 500));
  return jsonb_build_object('widened', true, 'widen_prepare_id', a.widen_prepare_id, 'active_hash', a.prepared_hash, 'added_keys', to_jsonb(a.added_keys), 'loads', v -> 'loads', 'counts', v -> 'counts');
end $$;
revoke all on function ops.widen_master_ownership(uuid, integer, text, jsonb) from public;

-- ─── 権限 (0058 の §15 と同じ形)。create or replace は今の権限を残す = ここは「だれにも渡さない」を明示するだけ ───
--   DB の持ち主だけ = widen / 判定の本体 ops._widen_judge / ops.widen_amazon_map_counts。watcher の読むだけの判定 ops.widen_check_readonly (0058 の grant) はそのまま
--   (watcher の判定も DEFINER の中で本体を呼ぶ = 新しい数も同じ答え)
do $$ begin
  if exists (select 1 from pg_roles where rolname = 'watcher') then
    execute 'revoke all on function ops._widen_judge(uuid, integer) from watcher';
    execute 'revoke all on function ops.widen_amazon_map_counts(integer) from watcher';
    execute 'revoke all on function ops.widen_master_ownership(uuid, integer, text, jsonb) from watcher';
    execute 'grant execute on function ops.widen_check_readonly(uuid, integer) to watcher';
  end if;
end $$;
