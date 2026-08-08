/**
 * miniPC で「Renderからマスタを取りに行く」経路を一度だけ実行して結果を出す。
 * 導入時の疎通確認用。秘密の値は出さない。
 *   cd C:\Users\bfaith\bfaith-portal && node apps/select-set/tests/pull-once.mjs
 */
import 'dotenv/config';
import { initSelectSetDB, listMappings, listOmake, listSets } from '../db.js';
import { masterMode, masterStatus, pullMaster } from '../master-sync.js';

initSelectSetDB();
console.log('役割: ' + masterMode());
console.log('取得前: sets=' + listSets({ includeInactive: true }).length
  + ' mappings=' + listMappings().length + ' omake=' + listOmake().length);

const r = await pullMaster();
console.log('取得結果: ' + JSON.stringify(r));
console.log('取得後: sets=' + listSets({ includeInactive: true }).length
  + ' mappings=' + listMappings().length + ' omake=' + listOmake().length);
console.log('状態: ' + JSON.stringify(masterStatus()));
process.exit(r.ok ? 0 : 1);
