/**
 * 試験だけで使う (scripts/test-master-legacy-gate.mjs が CLI を子プロセスで流すとき `node --import` で先に読む)。
 * 子プロセスの古い入口の門が読む切替の段階を env TEST_LEGACY_PHASE で決める
 * (legacy_open / frozen / company_owner / new_open / unreadable / legacy_open_then_frozen = 1 回目だけ legacy_open・2 回目から frozen)。
 * 本番のコードはこのファイルを読まない
 */
import { __setLegacyPhaseReader } from '../lib/master-legacy-gate.mjs';

const phase = process.env.TEST_LEGACY_PHASE;
let n = 0;
if (phase) {
  __setLegacyPhaseReader(async () => {
    n++;
    if (phase === 'unreadable') return { readable: false, phase: null, error: '試験: 段階を読めない' };
    if (phase === 'legacy_open_then_frozen') return { readable: true, phase: n === 1 ? 'legacy_open' : 'frozen' };
    return { readable: true, phase };
  });
}
