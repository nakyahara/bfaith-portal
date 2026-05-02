/**
 * SKUミラー同期の3点テスト (Codex指摘):
 *   1. masterに登録があれば sku_map を遮断 (master優先)
 *   2. master を削除すると次回 sync で auto に戻る
 *   3. Render 側 mirror_sku_resolved への全件置換 (件数・PK重複・source内訳)
 *
 * miniPC側DBとRender側DBを両方一時ファイルで起動、sync-to-render.js のSELECT部と
 * warehouse-mirror/router.js の受信処理部をライブラリ呼び出しでシミュレートする
 * (httpを介さずDB→DBの形でロジックだけ検証)。
 */
import Database from 'better-sqlite3';
import fs from 'fs';
import path from 'path';
import os from 'os';

// ────────────────────────────────────────────────────────────────────────────
// テスト用DBセットアップ
// ────────────────────────────────────────────────────────────────────────────

const TMP_MINI = path.join(os.tmpdir(), `mirror-test-mini-${process.pid}.db`);
const TMP_RENDER = path.join(os.tmpdir(), `mirror-test-render-${process.pid}.db`);
[TMP_MINI, TMP_RENDER].forEach(p => { if (fs.existsSync(p)) fs.unlinkSync(p); });

const mini = new Database(TMP_MINI);
mini.pragma('foreign_keys = ON');
mini.exec(`
  CREATE TABLE raw_ne_products (商品コード TEXT PRIMARY KEY, 原価 REAL);
  CREATE TABLE sku_map (
    seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, 数量 INTEGER DEFAULT 1,
    asin TEXT, 商品名 TEXT, synced_at TEXT,
    PRIMARY KEY (seller_sku, ne_code)
  );
  CREATE TABLE m_sku_master (
    seller_sku TEXT PRIMARY KEY, 商品名 TEXT NOT NULL,
    created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    created_by TEXT, updated_by TEXT
  );
  CREATE TABLE m_sku_components (
    seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL, 数量 INTEGER NOT NULL DEFAULT 1, sort_order INTEGER NOT NULL DEFAULT 0,
    created_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    updated_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    PRIMARY KEY (seller_sku, ne_code),
    FOREIGN KEY (seller_sku) REFERENCES m_sku_master(seller_sku) ON DELETE CASCADE
  );
  CREATE VIEW v_sku_resolved AS
    SELECT c.seller_sku, c.ne_code, c.数量, 'master' AS source FROM m_sku_components c
    UNION ALL
    SELECT s.seller_sku, s.ne_code, s.数量, 'auto' AS source FROM sku_map s
    WHERE NOT EXISTS (SELECT 1 FROM m_sku_master m WHERE m.seller_sku = s.seller_sku);
`);

const render = new Database(TMP_RENDER);
render.exec(`
  CREATE TABLE mirror_sku_resolved (
    seller_sku TEXT NOT NULL, ne_code TEXT NOT NULL,
    quantity INTEGER NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('master', 'auto')),
    商品名 TEXT, source_updated_at TEXT, synced_at TEXT NOT NULL,
    PRIMARY KEY (seller_sku, ne_code)
  );
`);

// ────────────────────────────────────────────────────────────────────────────
// 同期ロジックのシミュレーション (sync-to-render.js と router.js から抜き出し)
// ────────────────────────────────────────────────────────────────────────────

function buildSkuResolvedPayload(mini) {
  return mini.prepare(`
    SELECT
      v.seller_sku,
      v.ne_code,
      v.数量 AS quantity,
      v.source,
      CASE WHEN v.source = 'master' THEN m.商品名 ELSE NULL END AS 商品名,
      CASE WHEN v.source = 'master' THEN m.updated_at ELSE s.synced_at END AS source_updated_at
    FROM v_sku_resolved v
    LEFT JOIN m_sku_master m ON v.source = 'master' AND v.seller_sku = m.seller_sku
    LEFT JOIN sku_map s      ON v.source = 'auto'   AND v.seller_sku = s.seller_sku AND v.ne_code = s.ne_code
  `).all();
}

function applySkuResolvedToMirror(render, resolved, syncedAt) {
  const tx = render.transaction(() => {
    render.exec('DELETE FROM mirror_sku_resolved');
    const stmt = render.prepare(`INSERT INTO mirror_sku_resolved (
      seller_sku, ne_code, quantity, source, 商品名, source_updated_at, synced_at
    ) VALUES (?,?,?,?,?,?,?)`);
    for (const r of resolved) {
      stmt.run(
        r.seller_sku,
        r.ne_code,
        r.quantity ?? 1,
        r.source,
        r.商品名 ?? null,
        r.source_updated_at ?? null,
        syncedAt
      );
    }
  });
  tx();
}

function syncOnce(mini, render, syncedAt) {
  const payload = buildSkuResolvedPayload(mini);
  applySkuResolvedToMirror(render, payload, syncedAt);
  return payload;
}

// ────────────────────────────────────────────────────────────────────────────
// テスト
// ────────────────────────────────────────────────────────────────────────────

let pass = 0, fail = 0;
function check(label, ok, detail) {
  if (ok) { console.log(`  ✓ ${label}`); pass++; }
  else { console.log(`  ✗ ${label}${detail ? '\n    ' + detail : ''}`); fail++; }
}

// ── テスト1: master登録SKUが sku_map を遮断する ──
console.log('\n[テスト1] master優先 + sku_map fallback の遮断');
mini.exec("INSERT INTO sku_map (seller_sku, ne_code, 数量, synced_at) VALUES ('sku-A', 'ne-old', 1, '2026-01-01')"); // masterに登録される予定
mini.exec("INSERT INTO sku_map (seller_sku, ne_code, 数量, synced_at) VALUES ('sku-B', 'ne-auto', 2, '2026-01-02')"); // masterなし fallback対象
mini.exec("INSERT INTO m_sku_master (seller_sku, 商品名) VALUES ('sku-A', '社内: A弁当')");
mini.exec("INSERT INTO m_sku_components (seller_sku, ne_code, 数量) VALUES ('sku-A', 'ne-new', 1)");

const payload1 = syncOnce(mini, render, '2026-05-02T10:00:00Z');
check('payload件数 = master(1) + auto(1) = 2', payload1.length === 2, `got ${payload1.length}`);

const aRow = payload1.find(r => r.seller_sku === 'sku-A');
check('sku-A は master 由来', aRow?.source === 'master', `got source=${aRow?.source}`);
check('sku-A は ne-new (sku_map の ne-old は遮断)', aRow?.ne_code === 'ne-new', `got ne_code=${aRow?.ne_code}`);
check('sku-A の商品名が反映', aRow?.商品名 === '社内: A弁当', `got ${aRow?.商品名}`);

const bRow = payload1.find(r => r.seller_sku === 'sku-B');
check('sku-B は auto 由来', bRow?.source === 'auto');
check('sku-B の商品名は NULL', bRow?.商品名 === null, `got ${bRow?.商品名}`);

// Render 側に正しく反映
const renderRows1 = render.prepare("SELECT * FROM mirror_sku_resolved ORDER BY seller_sku").all();
check('Render mirror 件数 2', renderRows1.length === 2);
check('mirror sku-A は master', renderRows1.find(r => r.seller_sku === 'sku-A')?.source === 'master');
check('mirror sku-B は auto', renderRows1.find(r => r.seller_sku === 'sku-B')?.source === 'auto');
check('mirror.synced_at が共通', renderRows1.every(r => r.synced_at === '2026-05-02T10:00:00Z'));

// ── テスト2: master を削除すると次回 sync で auto fallback に切り替わる ──
console.log('\n[テスト2] master削除→auto fallback遷移');
mini.exec("DELETE FROM m_sku_master WHERE seller_sku = 'sku-A'"); // CASCADE で components も消える
const masterCntAfter = mini.prepare("SELECT COUNT(*) c FROM m_sku_master WHERE seller_sku='sku-A'").get().c;
const compsCntAfter = mini.prepare("SELECT COUNT(*) c FROM m_sku_components WHERE seller_sku='sku-A'").get().c;
check('master 削除確認', masterCntAfter === 0);
check('CASCADE で components 0件', compsCntAfter === 0);

const payload2 = syncOnce(mini, render, '2026-05-02T11:00:00Z');
const aRow2 = payload2.find(r => r.seller_sku === 'sku-A');
check('sku-A が auto 由来に切り替わった', aRow2?.source === 'auto', `got source=${aRow2?.source}`);
check('sku-A は ne-old (sku_map から復活)', aRow2?.ne_code === 'ne-old', `got ne_code=${aRow2?.ne_code}`);
check('sku-A の商品名が NULL に戻る', aRow2?.商品名 === null);

// Render 側で全件置換が走った結果、新しい syncedAt
const renderRows2 = render.prepare("SELECT * FROM mirror_sku_resolved ORDER BY seller_sku").all();
check('Render 件数は依然2', renderRows2.length === 2);
check('Render sku-A も auto に', renderRows2.find(r => r.seller_sku === 'sku-A')?.source === 'auto');
check('全件 syncedAt が新しい時刻に更新', renderRows2.every(r => r.synced_at === '2026-05-02T11:00:00Z'));

// ── テスト3: Render 側の全件置換 (PK重複なし、source内訳) ──
console.log('\n[テスト3] 全件置換の挙動');
// 複数SKU + セット商品をシードして実行
mini.exec("DELETE FROM sku_map");
mini.exec("DELETE FROM m_sku_components");
mini.exec("DELETE FROM m_sku_master");

// auto only
for (let i = 0; i < 5; i++) {
  mini.exec(`INSERT INTO sku_map (seller_sku, ne_code, 数量, synced_at) VALUES ('auto-${i}', 'ne-${i}', 1, '2026-05-01')`);
}
// セット商品 (master) - 1 SKU = 3 components
mini.exec("INSERT INTO m_sku_master (seller_sku, 商品名) VALUES ('set-001', 'セット弁当 (a+b+c)')");
['a', 'b', 'c'].forEach((ne, i) => {
  mini.exec(`INSERT INTO m_sku_components (seller_sku, ne_code, 数量, sort_order) VALUES ('set-001', 'ne-${ne}', ${i + 1}, ${i})`);
});

const payload3 = syncOnce(mini, render, '2026-05-02T12:00:00Z');
check('payload行数 = auto(5) + master(3) = 8', payload3.length === 8, `got ${payload3.length}`);

const renderRows3 = render.prepare("SELECT * FROM mirror_sku_resolved").all();
check('Render 全件 8', renderRows3.length === 8);

// PK重複なし (better-sqlite3 で INSERT 時に CHECK されるが念のため SELECT で再確認)
const pkSet = new Set(renderRows3.map(r => `${r.seller_sku}|${r.ne_code}`));
check('PK重複なし', pkSet.size === renderRows3.length);

// source 内訳
const masterCnt = renderRows3.filter(r => r.source === 'master').length;
const autoCnt = renderRows3.filter(r => r.source === 'auto').length;
check('master 3件', masterCnt === 3, `got ${masterCnt}`);
check('auto 5件', autoCnt === 5, `got ${autoCnt}`);

// セット商品の数量と sort_order
const setRows = renderRows3.filter(r => r.seller_sku === 'set-001').sort((a, b) => a.ne_code.localeCompare(b.ne_code));
check('set-001 は3行', setRows.length === 3);
check('数量1,2,3 全て格納', setRows.map(r => r.quantity).join(',') === '1,2,3');

// 全件置換確認: 古いsku-A/sku-Bは消えている
const oldRow = render.prepare("SELECT * FROM mirror_sku_resolved WHERE seller_sku IN ('sku-A','sku-B')").all();
check('前回 sync の sku-A/B は完全削除', oldRow.length === 0);

// ── テスト4: 0件payloadでも全件置換される (空に戻る) ──
console.log('\n[テスト4] 0件payload (master/sku_map両方空) で mirror も空');
mini.exec("DELETE FROM sku_map");
mini.exec("DELETE FROM m_sku_components");
mini.exec("DELETE FROM m_sku_master");

const payload4 = syncOnce(mini, render, '2026-05-02T13:00:00Z');
check('payload 0件', payload4.length === 0);
check('Render mirror 0件 (置換クリア)', render.prepare("SELECT COUNT(*) c FROM mirror_sku_resolved").get().c === 0);

// ────────────────────────────────────────────────────────────────────────────
// 終了
// ────────────────────────────────────────────────────────────────────────────
mini.close();
render.close();
[TMP_MINI, TMP_RENDER].forEach(p => {
  if (fs.existsSync(p)) fs.unlinkSync(p);
  ['-wal', '-shm'].forEach(suf => { if (fs.existsSync(p + suf)) fs.unlinkSync(p + suf); });
});

console.log(`\n結果: ${pass} pass / ${fail} fail`);
process.exit(fail > 0 ? 1 : 0);
