/**
 * マスタの配信・取得 (Render → miniPC) のテスト。
 * 全置換なので「消えてはいけないものが消えない」ことを重点的に見る。
 * 一時ディレクトリに DATA_DIR を向けるので本番DBには触れない。
 */
import path from 'path';
import fs from 'fs';
import os from 'os';
import { pathToFileURL } from 'url';

const repo =
  process.argv[2] ||
  path.resolve(
    path.dirname(new URL(import.meta.url).pathname.replace(/^\/(?=[A-Za-z]:)/, '')),
    '../../..',
  );

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), 'ss-master-'));
process.env.DATA_DIR = tmp;
delete process.env.RENDER;
delete process.env.RENDER_MIRROR_URL;

const db = await import(pathToFileURL(path.join(repo, 'apps/select-set/db.js')));
const ms = await import(pathToFileURL(path.join(repo, 'apps/select-set/master-sync.js')));

let pass = 0;
let fail = 0;
const ok = (c, n) => {
  if (c) { pass++; console.log(`  ok - ${n}`); }
  else { fail++; console.log(`  NG - ${n}`); }
};

db.initSelectSetDB();

console.log('=== どちらの役割かの判定 ===');
delete process.env.RENDER; delete process.env.RENDER_MIRROR_URL;
ok(ms.masterMode() === 'standalone', '取得先が無ければ standalone (自前のDBで動く)');
process.env.RENDER_MIRROR_URL = 'https://example.invalid';
ok(ms.masterMode() === 'replica', '取得先があれば replica (Renderから取りに行く)');
process.env.RENDER = 'true';
ok(ms.masterMode() === 'source', 'Render上なら source (ここが正)');
process.env.RENDER = 'tru'; // 打ち間違い
ok(ms.masterMode() === 'replica', 'RENDER の打ち間違いを Render 扱いしない');
delete process.env.RENDER;

console.log('\n=== 配信 (export) ===');
const snap = db.exportMaster();
ok(snap.sets.length === 9 && snap.mappings.length === 214 && snap.omake.length === 10,
  `シード済みの中身をそのまま出せる (sets=${snap.sets.length} mappings=${snap.mappings.length} omake=${snap.omake.length})`);
ok(!!snap.exportedAt, '書き出し日時が入る');
ok(snap.omake[0].rank === 1 && snap.omake[0].product_code === 'nemune-s', 'おまけは順位つきで出る');

console.log('\n=== 取得 (replaceMaster) は全置換 ===');
db.putRmsCache('selectae10-5', [{ displayName: '1本目', selections: [] }], null);
const counts = db.replaceMaster({
  sets: [{ set_code: 'newset-3', label: '新セット', is_active: 1 }],
  mappings: [
    { set_code: 'newset-3', option_text: 'いちご_strawberry10', product_code: 'strawberry10' },
    { set_code: 'newset-3', option_text: '【いちご】strawberry10', product_code: 'strawberry10' },
  ],
  omake: [{ product_code: 'aaa-1' }, { product_code: 'bbb-2' }],
});
ok(db.listSets().length === 1 && db.listSets()[0].set_code === 'newset-3', '古いセットは消えて新しいものだけになる');
ok(db.listMappings('ganesh-select3').length === 0, 'Render側で消えたセットのマッピングも消える');
ok(db.listMappings('newset-3').length === 1, '正規化キーが同じ行は1件にまとまる (括弧違いの重複)');
ok(counts.mappings === 2, '受け取った件数はそのまま報告する (実際に入るのは重複排除後)');
const om = db.listOmake();
ok(om.length === 2 && om[0].rank === 1 && om[0].product_code === 'aaa-1', 'おまけも順位を振り直して全置換');
ok(!!db.getRmsCache('selectae10-5'), '🚨 RMSキャッシュは miniPC 側で育てるものなので消さない');

console.log('\n=== 取得状況の記録 ===');
db.setMeta('master_last_pulled_at', '2026-08-08T01:00:00Z');
ok(db.getMeta('master_last_pulled_at') === '2026-08-08T01:00:00Z', '最終取得日時を保存できる');
db.setMeta('master_last_pulled_at', '2026-08-08T02:00:00Z');
ok(db.getMeta('master_last_pulled_at') === '2026-08-08T02:00:00Z', '上書きできる');
ok(db.getMeta('存在しないキー') === null, '無いキーは null');

console.log('\n=== 取得できないときは前の内容で動く ===');
process.env.RENDER_MIRROR_URL = 'http://127.0.0.1:1'; // 誰も待っていないポート
process.env.MIRROR_SYNC_KEY = 'dummy';
const r = await ms.pullMaster();
ok(r.ok === false && !!r.error, '取得に失敗したら ok=false とエラー内容を返す');
ok(db.listSets().length === 1, '失敗しても既存のマスタを消さない (fail-soft)');
ok(!!db.getMeta('master_last_error'), '失敗の内容を残す');

const st = ms.masterStatus();
ok(st.mode === 'replica' && !!st.remote && st.remote.includes('/apps/select-set/master-api/export'),
  '取得先URLを画面に出せる');

console.log(`\n合計: ${pass} pass / ${fail} NG`);
try { fs.rmSync(tmp, { recursive: true, force: true }); } catch {}
process.exit(fail === 0 ? 0 : 1);
