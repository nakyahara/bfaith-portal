import io

# ─────────── db.js ───────────
p = 'apps/inbound-check/db.js'
s = io.open(p, encoding='utf-8').read()

old = """  addCol(db, 'f_inbound_check_batches', 'carried_from', 'TEXT');
  addCol(db, 'f_inbound_check_batches', 'carried_at', 'TEXT');"""
new = """  addCol(db, 'f_inbound_check_batches', 'carried_from', 'TEXT');
  addCol(db, 'f_inbound_check_batches', 'carried_at', 'TEXT');
  // 🚨「内容が変わっていない」と「そもそも取りに行けていない」を**別の事実として**持つ (Codex #1231 R1 中)。
  //   前者は正常 (新しい入荷受付が無い日)、後者は取得が壊れている疑い。同じ表示にすると監視精度が落ちる。
  //   last_verified_at = 共有ドライブの CSV を読んで、中身がこのバッチと同じだと確かめた時刻 (UTC)
  //   last_verified_source_at = そのとき読んだ CSV の更新時刻 (Drive の modifiedTime)
  addCol(db, 'f_inbound_check_batches', 'last_verified_at', 'TEXT');
  addCol(db, 'f_inbound_check_batches', 'last_verified_source_at', 'TEXT');"""
assert s.count(old) == 1
s = s.replace(old, new)

# rollover tx を immediate に (別プロセスと競合したとき、読んでから書く DEFERRED は SQLITE_BUSY になる)
old = """    return { from: b.work_date, to: today, batchId: b.id, lines, qty };
  });
  const r = tx();"""
new = """    return { from: b.work_date, to: today, batchId: b.id, lines, qty };
  });
  // 読んでから書くので IMMEDIATE (DEFERRED だと別プロセスと競合したとき SQLITE_BUSY になる)。
  // ただし既にトランザクションの中 (finalizeLine 等) なら savepoint になるので immediate は使えない
  const r = db.inTransaction ? tx() : tx.immediate();"""
assert s.count(old) == 1
s = s.replace(old, new)

# dupResult: active バッチなら「本日読んで、中身は同じだった」を記録する
old = """  const dupResult = dup => {
    const message = `同じ内容のCSVは取込済みです (バッチ#${dup.id}、${dup.imported_at})`;
    logImport(db, { actor, source, fileName, ok: false, batchId: dup.id, message });
    return { ok: false, error: 'duplicate_file', message, batch: dup };
  };"""
new = """  const dupResult = dup => {
    // ⭐**「中身が同じ」は取得が生きている証拠**なので、active バッチに確認時刻を残す (Codex #1231 R1 中)。
    //   これが今日の日付なら「新しい入荷受付が無いだけ」、無ければ「取りに行けていない」= 要調査。
    //   区別しないと、取得が止まった日も「新しい受付はありません」と出て静かに気づけなくなる
    if (dup.status === 'active') {
      db.prepare('UPDATE f_inbound_check_batches SET last_verified_at = ?, last_verified_source_at = ? WHERE id = ?')
        .run(utcNow(), genAt, dup.id);
    }
    const message = `同じ内容のCSVは取込済みです (バッチ#${dup.id}、${dup.imported_at})`;
    logImport(db, { actor, source, fileName, ok: false, batchId: dup.id, message });
    return { ok: false, error: 'duplicate_file', message, batch: getBatch(dup.id) || dup };
  };"""
assert s.count(old) == 1
s = s.replace(old, new)

# getState: 取得できているかを別立てで返す
old = """  // 本日の取込がまだ来ていない = 前日の一覧を引き継いで作業している (作業はできる。取込が来ていないことは伝える)
  const carriedFrom = batch.carried_from && batch.carried_from !== batch.work_date ? batch.carried_from : null;
  return {
    batch, slips, lines, day_stale: dayStale, carried_from: carriedFrom, field_options: fieldOptions(),"""
new = """  // 前日の一覧を引き継いで作業している (作業はできる。何が起きているかは下の2つで言い分ける)
  const carriedFrom = batch.carried_from && batch.carried_from !== batch.work_date ? batch.carried_from : null;
  // 🚨「内容が同じだった」(正常) と「取りに行けていない」(要調査) を混ぜない (Codex #1231 R1 中)。
  //   本日ぶんの取得を確かめられた = 今日取り込めた or 今日読んで中身が同じだった
  const checkedAt = batch.last_verified_at || null;
  const checkedToday = !carriedFrom
    || !!(checkedAt && workDateJst(new Date(checkedAt)) === workDateJst());
  return {
    batch, slips, lines, day_stale: dayStale, field_options: fieldOptions(),
    carried_from: carriedFrom, import_checked_at: checkedAt, import_checked_today: checkedToday,"""
assert s.count(old) == 1
s = s.replace(old, new)
io.open(p, 'w', encoding='utf-8', newline='').write(s)

# ─────────── router.js (管理画面でも繰り越す) ───────────
p = 'apps/inbound-check/router.js'
s = io.open(p, encoding='utf-8').read()
old = """router.get('/admin', requireSession, api(async (req, res) => {
  let drive = null;"""
new = """router.get('/admin', requireSession, api(async (req, res) => {
  // iPad が1台も開かれていない朝でも、管理画面を開けば業務日が今日になる (Codex #1231 R1 中)。
  // ここを飛ばすと「9/5 の一覧を引き継いで作業中」の案内が管理画面にだけ出ない
  rollOverWorkDate();
  let drive = null;"""
assert s.count(old) == 1
s = s.replace(old, new)
io.open(p, 'w', encoding='utf-8', newline='').write(s)
print('ok')
