/**
 * 試験だけで使う (scripts/test-master-legacy-gate.mjs が CLI を子プロセスで流すとき `node --import` で先に読む)。
 * 子プロセスの古い入口の門が読む切替の段階を env TEST_LEGACY_PHASE で決める
 * (legacy_open / frozen / company_owner / new_open / unreadable / legacy_open_then_frozen = 1 回目だけ legacy_open・2 回目から frozen)。
 * ⑤-3b: 段階が legacy_open 以外のときの持ち主 (C の列) = env TEST_LEGACY_OWNER
 *   (無い・'all' = 全部の列が C / 'none' = 全部 load / 'unreadable' = 持ち主を読めない / それ以外 = カンマ区切りの列のキー)
 * 本番のコードはこのファイルを読まない
 */
import { __setLegacyPhaseReader } from '../lib/master-legacy-gate.mjs';
import { OWNED_COLUMNS } from '../config/master-ownership.mjs';

const phase = process.env.TEST_LEGACY_PHASE;
const ownerEnv = process.env.TEST_LEGACY_OWNER || 'all';
const owner = ownerEnv === 'unreadable' ? { readable: false, error: '試験: 持ち主を読めない' }
  : { readable: true, company: ownerEnv === 'all' ? [...OWNED_COLUMNS] : ownerEnv === 'none' ? [] : ownerEnv.split(',').map((s) => s.trim()).filter(Boolean) };
const withOwner = (p) => (p === 'legacy_open' ? { readable: true, phase: p } : { readable: true, phase: p, owner });
let n = 0;
if (phase) {
  __setLegacyPhaseReader(async () => {
    n++;
    if (phase === 'unreadable') return { readable: false, phase: null, error: '試験: 段階を読めない' };
    if (phase === 'legacy_open_then_frozen') return withOwner(n === 1 ? 'legacy_open' : 'frozen');
    return withOwner(phase);
  });
}
