-- 0022: 在庫の「増えた / 減った」(inferred) を作った日の印 snapshots.stock_diff_days (Company DB構想 08 §3.2 / §3.3 ②。D2c)
--
--   08 §3.2 の 3 段目 = 完走した日どうしの差 (SKU 単位) → events.inventory_events (confidence = 'inferred')。
--   イベントの表 (0005・0010・0013) は既にある。ここで足すのは **「どの日の差を作り終えたか」の印** だけ。
--
--   なぜ印が要るか: 倉庫が動かない日 (日曜・連休) は差が 0 件 = イベントが 1 行も増えない。
--     「その日のイベントがある = 作り終えた」と読むと、0 件の日を毎回作り直すか、止まっていた日を見落とす (件数を完了の代わりにしない。完了は明示の印)。
--     印はイベントの追記と **同じ取引** で入れる = 「印があるのにイベントが無い」「イベントだけ入って印が無い」は起きない。
--
--   差を作るロジックは DB の関数にしない (apps/company-db/inventory/stock-diff.mjs)。式を直すたびに本番の migrate を要らなくするため。
--   式の版は calc_version (いまは 'lzdiff:v1'。イベントの idempotency_key の頭と同じ) で持ち、版を上げたら同じ日を新しい版でもう一度作れる (主キーに版を含む)。
--
--   status:
--     done    = from_date (= to_date の前日。complete) との差を作った。events = 追記したイベントの数 (0 もある)
--     skipped = 差を作らない日。skip_reason = 'first_day' (それより前の日が無い) / 'prev_not_complete' (前日が missing / partial = 間に取れなかった日がある区間は作らない。08 §3.2)
--   unresolved_changed = 数量が変わったのに SKU が分からず (sku_stock_daily.sku_id が両日とも null)、イベントにできなかった商品コードの数 (events.inventory_events.sku_id は not null)
--
--   inferred のイベントは日次の表から何度でも作り直せる **派生データ**。締めをやり直して日次が変わったら、その区間の inferred のイベントは保守の手順で消して作り直す (README)。
--     作る側は、追記の後に「その区間のイベントの集合 = いまの日次から作った差」を照合し、合わなければ印を付けずに失敗する (古いイベントを残したまま done にしない)。
--     その照合と保守の削除が区間を引けるように、events.inventory_events に (source_system, source_ref) の索引を足す。source_ref = '<scope>:<前日>..<当日>'。
-- 🚨 0001〜0021 の表・関数は変えない (events.inventory_events に索引を 1 つ足すだけ)。

create index if not exists ix_inventory_events_source_ref on events.inventory_events (source_system, source_ref) where source_ref is not null;

create table snapshots.stock_diff_days (
  to_date            date not null,
  source             text not null check (source in ('logizard','ne','fba_jp','fba_us')),
  scope_key          text not null,
  calc_version       text not null check (calc_version <> ''),
  company_id         smallint not null references core.companies,
  from_date          date,
  status             text not null check (status in ('done','skipped')),
  skip_reason        text check (skip_reason in ('first_day','prev_not_complete')),
  events             integer not null default 0 check (events >= 0),
  unresolved_changed integer not null default 0 check (unresolved_changed >= 0),
  created_at         timestamptz not null default now(),
  primary key (to_date, source, scope_key, calc_version),
  foreign key (to_date, source, scope_key) references snapshots.stock_capture_days (snapshot_date, source, scope_key),
  foreign key (from_date, source, scope_key) references snapshots.stock_capture_days (snapshot_date, source, scope_key),
  constraint ck_stock_diff_days_done check ((status = 'done') = (from_date is not null)),
  constraint ck_stock_diff_days_from check (from_date is null or from_date = to_date - 1),
  constraint ck_stock_diff_days_skip check ((status = 'skipped') = (skip_reason is not null)),
  constraint ck_stock_diff_days_skip_counts check (status = 'done' or (events = 0 and unresolved_changed = 0))
);
comment on table snapshots.stock_diff_days is '在庫の日次の差 (events.inventory_events の inferred) を作り終えた日の印。イベントの追記と同じ取引で入る。差が 0 件の日も done で残る';
