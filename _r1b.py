import io

p = 'apps/inbound-check/views/products.html'
s = io.open(p, encoding='utf-8').read()

# ── 1. 取り直しの予約をタイマーで持ち、停止で解除する ──
old = """    let scanStream = null, scanTimer = null, scanStarting = false, scanGen = 0, scanCanvas = null, scanWatchTimer = null;"""
new = """    let scanStream = null, scanTimer = null, scanStarting = false, scanGen = 0, scanCanvas = null, scanWatchTimer = null;
    let scanRetryTimer = null;   // 取り直しの予約。**停止したら必ず解除する** (Codex #1239 R1 P1)"""
assert s.count(old) == 1, 'A'
s = s.replace(old, new)

old = """    function stopScan() {
      scanGen++;                                   // 起動待ちの getUserMedia が返ってきても使わせない
      clearTimeout(scanTimer); scanTimer = null;
      clearInterval(scanWatchTimer); scanWatchTimer = null;   // 映像の見張りも必ず止める"""
new = """    /**
     * 取り直しの予約。🚨**予約した時点の世代を覚えておき、途中で止められていたら走らせない**
     * (Codex #1239 R1 P1)。予約だけ生き残ると、閉じたあとにカメラが勝手に開く
     */
    function scheduleRescan(ms) {
      clearTimeout(scanRetryTimer);
      const at = scanGen;
      scanRetryTimer = setTimeout(() => {
        scanRetryTimer = null;
        if (at !== scanGen) return;                // 予約後に止められた / 別の起動が始まった
        startScan();
      }, ms);
    }
    function stopScan() {
      scanGen++;                                   // 起動待ちの getUserMedia が返ってきても使わせない
      clearTimeout(scanRetryTimer); scanRetryTimer = null;    // 予約済みの取り直しも取り消す
      clearTimeout(scanTimer); scanTimer = null;
      clearInterval(scanWatchTimer); scanWatchTimer = null;   // 映像の見張りも必ず止める"""
assert s.count(old) == 1, 'B'
s = s.replace(old, new)

# stopScan 自身が「起動中」の札とボタンを戻す (世代が変わると古い endStarting は何もしないため)
old = """      const v = $('#scanVideo'); if (v) v.srcObject = null;
      $('#scanBox').classList.remove('on');
    }"""
new = """      const v = $('#scanVideo'); if (v) v.srcObject = null;
      $('#scanBox').classList.remove('on');
      // 🚨 世代が変わると古い endStarting() は何もしないので、**停止側で必ず戻す**。
      //    でないと「起動中」の札が掛かったままボタンが押せなくなる (Codex #1239 R1)
      scanStarting = false;
      const b = $('#scanBtn'); if (b) b.disabled = false;
    }"""
assert s.count(old) == 1, 'C'
s = s.replace(old, new)

# ── 2. 古い getUserMedia の失敗が、いまの取り直しに触らないようにする ──
old = """      catch (e) {
        // 条件が強すぎて掴めないだけなら、緩めてもう一度 (画面には出さない)
        if (cameraTry < CAMERA_TRIES.length - 1
            && e && (e.name === 'OverconstrainedError' || e.name === 'NotFoundError' || e.name === 'NotReadableError')) {
          cameraTry++;
          endStarting();
          setTimeout(() => { startScan(); }, 200);
          return;
        }"""
new = """      catch (e) {
        // 🚨 **古い要求が遅れて失敗しても、いまの起動には触らない** (Codex #1239 R1 P1)。
        //    条件を飛ばしたり、閉じたあとにエラーを出したりしないため
        if (gen !== scanGen) return;
        // 条件が強すぎて掴めないだけなら、緩めてもう一度 (画面には出さない)。
        // 次の条件は **この試行の tryIndex + 1**。共有値を足すと、別の起動と取り合って条件が飛ぶ
        if (tryIndex < CAMERA_TRIES.length - 1
            && e && (e.name === 'OverconstrainedError' || e.name === 'NotFoundError' || e.name === 'NotReadableError')) {
          cameraTry = tryIndex + 1;
          endStarting();
          scheduleRescan(200);
          return;
        }"""
assert s.count(old) == 1, 'D'
s = s.replace(old, new)

# ── 3. 診断は「停止する前」に取る (stop() が readyState を ended に変えてしまう) ──
old = """          const heldMs = endedAt !== null ? endedAt : Math.round(now() - startedAt);
          clearInterval(scanWatchTimer); scanWatchTimer = null;
          stopScan();
          // 🚨 つないだ直後に切れたら、**次の (より緩い) 条件**で取り直す。同じ条件を繰り返さない
          if (heldMs < 3000 && cameraTry < CAMERA_TRIES.length - 1) {
            cameraTry++;
            // 取り直しに入れるように「起動中」の札を先に外す (stopScan は札を触らない)
            scanStarting = false;
            const b = $('#scanBtn'); if (b) b.disabled = false;
            banner('warn', 'カメラをつなぎ直しています… (' + (cameraTry + 1) + '/' + CAMERA_TRIES.length + ')');
            setTimeout(() => { startScan(); }, 400);
            return;
          }"""
new = """          const heldMs = endedAt !== null ? endedAt : Math.round(now() - startedAt);
          // 🚨 **診断は止める前に取る**。stopScan() が track.stop() を呼ぶと readyState が
          //    ended に変わり、直前まで live/muted だったのか分からなくなる (Codex #1239 R1 P2)
          const snapshot = diag();
          clearInterval(scanWatchTimer); scanWatchTimer = null;
          stopScan();
          // 🚨 つないだ直後に切れたら、**次の (より緩い) 条件**で取り直す。同じ条件を繰り返さない
          if (heldMs < 3000 && tryIndex < CAMERA_TRIES.length - 1) {
            cameraTry = tryIndex + 1;
            banner('warn', 'カメラをつなぎ直しています… (' + (cameraTry + 1) + '/' + CAMERA_TRIES.length + ')');
            scheduleRescan(400);
            return;
          }"""
assert s.count(old) == 1, 'E'
s = s.replace(old, new)

old = """            + '検索欄を長押し →「テキストをスキャン」でも JAN を読み取れます'
            + '<br><small>' + esc(diag()) + '</small>', { sticky: true });
          return;"""
new = """            + '検索欄を長押し →「テキストをスキャン」でも JAN を読み取れます'
            + '<br><small>' + esc(snapshot) + '</small>', { sticky: true });
          return;"""
assert s.count(old) == 1, 'F'
s = s.replace(old, new)

# 映像が出ないときも、止める前に取る
old = """        if (!BarcodeScanDecide.videoStalled(videoWatch, now())) return;
        stopScan();"""
new = """        if (!BarcodeScanDecide.videoStalled(videoWatch, now())) return;
        const snapshot = diag();   // 止める前に取る (stop() が track の状態を書き換えるため)
        stopScan();"""
assert s.count(old) == 1, 'G'
s = s.replace(old, new)

old = """          + '検索欄を長押し →「テキストをスキャン」でも JAN を読み取れます'
          + '<br><small>' + esc(diag()) + '</small>', { sticky: true });
      }, 500);"""
new = """          + '検索欄を長押し →「テキストをスキャン」でも JAN を読み取れます'
          + '<br><small>' + esc(snapshot) + '</small>', { sticky: true });
      }, 500);"""
assert s.count(old) == 1, 'H'
s = s.replace(old, new)

# ── 4. 診断に自由文字列を出さない (カメラ名・エラー本文は個人情報が混じりうる) ──
old = """        if (playError) bits.push('play=' + (playError.name || playError.message || 'NG'));
        bits.push('条件' + (tryIndex + 1) + '/' + CAMERA_TRIES.length);
        if (track) {
          if (track.label) bits.push(String(track.label).slice(0, 24));
          try {
            const st = track.getSettings ? track.getSettings() : null;
            if (st && st.width) bits.push(st.width + 'x' + st.height);
          } catch (e) { /* 取れなくてよい */ }
        }"""
new = """        // 🚨 自由文字列は出さない (カメラ名やエラー本文に何が入るか保証できない — Codex #1239 R1)。
        //    出すのは **決まった語彙だけ**: エラーは name / カメラは facingMode と解像度
        if (playError) bits.push('play=' + (playError.name || 'NG'));
        bits.push('条件' + (tryIndex + 1) + '/' + CAMERA_TRIES.length);
        if (track) {
          try {
            const st = track.getSettings ? track.getSettings() : null;
            if (st) {
              if (st.facingMode) bits.push(String(st.facingMode));
              if (st.width) bits.push(st.width + 'x' + st.height);
            }
          } catch (e) { /* 取れなくてよい */ }
        }"""
assert s.count(old) == 1, 'I'
s = s.replace(old, new)

# getUserMedia 自体の失敗も name だけ (既存どおり) + 起動元
io.open(p, 'w', encoding='utf-8', newline='').write(s)
print('ok')
