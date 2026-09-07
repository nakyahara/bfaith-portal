import io

# ─────────── db.js: 判定を1か所に ───────────
p = 'apps/inbound-check/db.js'
s = io.open(p, encoding='utf-8').read()
old = """  // 前日の一覧を引き継いで作業している (作業はできる。何が起きているかは下の2つで言い分ける)
  const carriedFrom = batch.carried_from && batch.carried_from !== batch.work_date ? batch.carried_from : null;
  // 🚨「内容が同じだった」(正常) と「取りに行けていない」(要調査) を混ぜない (Codex #1231 R1 中)。
  //   本日ぶんの取得を確かめられた = 今日取り込めた or 今日読んで中身が同じだった
  const checkedAt = batch.last_verified_at || null;
  const checkedToday = !carriedFrom
    || !!(checkedAt && workDateJst(new Date(checkedAt)) === workDateJst());
  return {
    batch, slips, lines, day_stale: dayStale, field_options: fieldOptions(),
    carried_from: carriedFrom, import_checked_at: checkedAt, import_checked_today: checkedToday,"""
new = """  // 前日の一覧を引き継いで作業している (作業はできる。何が起きているかは carryStatus が言い分ける)
  const carry = carryStatus(batch);
  return {
    batch, slips, lines, day_stale: dayStale, field_options: fieldOptions(),
    carried_from: carry ? carry.from : null,
    import_checked_at: carry ? carry.checkedAt : null,
    import_checked_today: carry ? carry.checkedToday : true,"""
assert s.count(old) == 1
s = s.replace(old, new)

old = """/**
 * iPad 一覧の状態。active バッチが無ければ { batch:null, slips:[], lines:[] }"""
new = """/**
 * 引き継ぎ中かどうかと、**本日ぶんの取得を確かめられたか**。iPad と管理画面で同じ判定を使う。
 *
 * 🚨「新しい入荷受付が増えていないので CSV の中身が同じ」(正常) と「そもそも取りに行けていない」
 *   (miniPC / rclone / Drive の故障。要調査) は**別の事実**。同じ表示にすると、取得が止まった日も
 *   「新しい受付はありません」と出て静かに気づけなくなる (Codex #1231 R1 中)。
 *   `last_verified_at` = 共有ドライブの CSV を読んで、中身がこのバッチと同じだと確かめた時刻。
 *
 * @returns {null | {from, checkedAt, checkedToday}} 引き継いでいなければ null (= 本日ぶんを取り込めている)
 */
export function carryStatus(batch) {
  if (!batch || !batch.carried_from || batch.carried_from === batch.work_date) return null;
  const at = batch.last_verified_at || null;
  return {
    from: batch.carried_from,
    checkedAt: at,
    checkedToday: !!(at && workDateJst(new Date(at)) === workDateJst()),
  };
}

/**
 * iPad 一覧の状態。active バッチが無ければ { batch:null, slips:[], lines:[] }"""
assert s.count(old) == 1
s = s.replace(old, new)
io.open(p, 'w', encoding='utf-8', newline='').write(s)

# ─────────── router.js: 管理画面へ渡す ───────────
p = 'apps/inbound-check/router.js'
s = io.open(p, encoding='utf-8').read()
s = s.replace("  getState, importCsv, getActiveBatch, rollOverWorkDate, listBatches, listImportLog, listEvents, eventsCsv,",
              "  getState, importCsv, getActiveBatch, rollOverWorkDate, carryStatus, listBatches, listImportLog, listEvents, eventsCsv,", 1)
old = """    active: getActiveBatch(),
    batches: listBatches(30),"""
new = """    active: activeBatch,
    // 引き継ぎ中かどうか + 本日ぶんの取得を確かめられたか (iPad と同じ判定を使う)
    carry: carryStatus(activeBatch),
    batches: listBatches(30),"""
assert s.count(old) == 1
s = s.replace(old, new)
old = """  rollOverWorkDate();
  let drive = null;"""
new = """  rollOverWorkDate();
  const activeBatch = getActiveBatch();
  let drive = null;"""
assert s.count(old) == 1
s = s.replace(old, new)
io.open(p, 'w', encoding='utf-8', newline='').write(s)

# ─────────── iPad ───────────
p = 'apps/inbound-check/views/index.html'
s = io.open(p, encoding='utf-8').read()
old = """      // 本日の取込がまだ来ていない = 前日の一覧を引き継いで作業中。**入力は止めない** (中原さん 2026-09-07)。
      // 取込が来ていないことだけ伝えて、🚚 で取りに行けることを案内する
      if (state.day_stale) banner('warn', '一覧の業務日を今日に更新できませんでした (表示中は ' + esc(b.work_date || '') + ' の一覧です)。「🚚 いま取りに行く」を押してください');
      else if (state.carried_from) banner('info', '本日の取込はまだ来ていません。<b>' + esc(fmtDate(state.carried_from)) + ' の一覧を引き継いで</b>そのまま作業できます (前日の ✅ と数えた数は残っています)。ロジザードに新しい受付を入れたら「🚚 いま取りに行く」を押してください');"""
new = """      // 前日の一覧を引き継いで作業中。**入力は止めない** (中原さん 2026-09-07)。
      // 🚨「新しい受付が増えていないだけ」(正常) と「取りに行けていない」(要調査) は別の話なので言い分ける
      if (state.day_stale) banner('warn', '一覧の業務日を今日に更新できませんでした (表示中は ' + esc(b.work_date || '') + ' の一覧です)。「🚚 いま取りに行く」を押してください');
      else if (state.carried_from && state.import_checked_today) {
        banner('info', '<b>' + esc(fmtDate(state.carried_from)) + ' の一覧を引き継いで</b>作業中です (前日の ✅ と数えた数は残っています)。'
          + '本日 ' + esc(fmtJst(state.import_checked_at)) + ' に確認しましたが <b>新しい入荷受付は増えていません</b>。'
          + 'いま登録したぶんを出すには「🚚 いま取りに行く」を押してください');
      } else if (state.carried_from) {
        banner('warn', '<b>' + esc(fmtDate(state.carried_from)) + ' の一覧を引き継いで</b>作業できます (前日の ✅ と数えた数は残っています)。'
          + 'ただし<b>本日はまだ一覧を取りに行けていません</b> — 「🚚 いま取りに行く」を押してください'
          + (state.import_checked_at ? ' (最後に確認できたのは ' + esc(fmtJst(state.import_checked_at)) + ')' : ''));
      }"""
assert s.count(old) == 1
s = s.replace(old, new)
io.open(p, 'w', encoding='utf-8', newline='').write(s)

# ─────────── 管理画面 ───────────
p = 'apps/inbound-check/views/admin.ejs'
s = io.open(p, encoding='utf-8').read()
old = """        <% if (active.carried_from && active.carried_from !== active.work_date) { %>
          <p class="hint carried">📅 <b>本日 (<%= active.work_date %>) の取込はまだ来ていません</b>。<%= active.carried_from %> に取り込んだ一覧を引き継いで作業中です
            (前日の ✅ と数えた数は残っています)。ロジザードに新しい入荷受付が1件も増えていない日は CSV の中身が変わらないため、これが通常の状態です。
            増えたはずなのに出ないときは「🚚 いま取りに行く」を押してください</p>
        <% } %>"""
new = """        <% if (carry) { %>
          <p class="hint carried<%= carry.checkedToday ? '' : ' ng-carried' %>">📅 <b><%= carry.from %> に取り込んだ一覧を引き継いで作業中</b>です
            (本日 = <%= active.work_date %>。前日の ✅ と数えた数は残っています)。
            <% if (carry.checkedToday) { %>
              本日も共有ドライブの CSV を確認していますが (<span data-jst="<%= carry.checkedAt %>"></span>)、<b>中身が変わっていません</b>
              = ロジザードに新しい入荷受付が1件も増えていない日です。これは通常の状態です
            <% } else { %>
              <b>⚠本日はまだ共有ドライブから一覧を取りに行けていません</b><% if (!carry.checkedAt) { %> (確認できた記録がありません)<% } else { %>
              (最後に確認できたのは <span data-jst="<%= carry.checkedAt %>"></span>)<% } %>。
              miniPC の定時取得 (08:40 / 11:45) と rclone 転送、下の取込履歴に失敗が並んでいないかを確認してください
            <% } %>
          </p>
        <% } %>"""
assert s.count(old) == 1
s = s.replace(old, new)
old = """    p.hint.carried { background: #e7f5ff; color: #1864ab; border-radius: 6px; padding: 8px 10px; }"""
new = """    p.hint.carried { background: #e7f5ff; color: #1864ab; border-radius: 6px; padding: 8px 10px; }
    /* 取りに行けていない日だけ警告色 (中身が変わっていないだけの日は正常なので青のまま) */
    p.hint.carried.ng-carried { background: #fff3bf; color: #7a5c00; }"""
assert s.count(old) == 1
s = s.replace(old, new)
io.open(p, 'w', encoding='utf-8', newline='').write(s)
print('ok')
