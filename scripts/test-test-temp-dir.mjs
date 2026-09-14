import { test } from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { spawnSync } from 'node:child_process';
const helper = new URL('./test-temp-dir.mjs', import.meta.url).href;
test('temporary DATA_DIR lifecycle and external-directory protection', async t => {
  const parent = fs.realpathSync(os.tmpdir());
  const base = fs.mkdtempSync(path.join(parent, 'test-temp-owner-'));
  t.after(() => {
    assert.equal(path.dirname(fs.realpathSync(base)), parent);
    fs.rmSync(base, {recursive:true, force:true});
  });
  const scratch = path.join(base, 'scratch'), external = path.join(base, 'external');
  fs.mkdirSync(scratch); fs.mkdirSync(external);
  fs.writeFileSync(path.join(external,'keep.txt'), 'keep');
  const fixture = path.join(base, 'fixture.mjs');
  fs.writeFileSync(fixture, [
    "import fs from 'node:fs'; import path from 'node:path';",
    'import {temporaryTestDataDir} from '+JSON.stringify(helper)+';',
    "const mode=process.argv[2];",
    "const dir=await temporaryTestDataDir(import.meta.url,'lifecycle-', {reuseProvided: mode==='provided'});",
    "console.log(JSON.stringify({dir, args:process.argv.slice(2)}));",
    "if(mode==='provided') process.exit(0);",
    "const fd=fs.openSync(path.join(dir,'open.db'),'w');",
    "if(mode==='throw') throw new Error('intentional fixture failure');",
    "if(mode==='nested-link') fs.symlinkSync(process.argv[3],path.join(dir,'outside'),'junction');",
    "if(mode==='replace-root'){fs.closeSync(fd);fs.renameSync(dir,dir+'-moved');fs.symlinkSync(process.argv[3],dir,'junction');}",
    "process.exit(mode==='failure'?7:0);"
  ].join('\n'));
  const run = (mode, provided) => {
    const env = {...process.env,TMP:scratch,TEMP:scratch,TMPDIR:scratch};
    delete env.BFAITH_TEST_TEMP_ENTRY; delete env.DATA_DIR;
    if(provided) env.DATA_DIR = external;
    const result=spawnSync(process.execPath,[fixture,mode,external,'argument preserved'],{env,encoding:'utf8',timeout:20000,windowsHide:true});
    assert.equal(result.error,undefined);
    const record=JSON.parse(result.stdout.trim().split(/\r?\n/)[0]);
    assert.equal(record.args[2],'argument preserved');
    return {...result,record};
  };
  for(const [mode,code] of [['success',0],['failure',7],['throw',1],['nested-link',0]]) {
    await t.test(mode,()=>{
      const r=run(mode,false);
      assert.equal(r.status,code,r.stderr);
      assert.equal(fs.existsSync(r.record.dir),false,'owned directory must be removed');
      assert.equal(fs.readFileSync(path.join(external,'keep.txt'),'utf8'),'keep');
    });
  }
  await t.test('supplied directory is preserved when requested',()=>{
    const r=run('provided',true); assert.equal(r.status,0,r.stderr);
    assert.equal(r.record.dir,external);
    assert.equal(fs.readFileSync(path.join(external,'keep.txt'),'utf8'),'keep');
  });
  await t.test('default isolation does not use or delete supplied directory',()=>{
    const r=run('success',true); assert.equal(r.status,0,r.stderr);
    assert.notEqual(r.record.dir,external);
    assert.equal(fs.existsSync(r.record.dir),false);
    assert.equal(fs.readFileSync(path.join(external,'keep.txt'),'utf8'),'keep');
  });
  const rootFixture = path.join(base, 'root-fixture.mjs');
  fs.writeFileSync(rootFixture, [
    "import fs from 'node:fs'; import os from 'node:os'; import path from 'node:path';",
    'import {temporaryTestRoot} from '+JSON.stringify(helper)+';',
    "const root=await temporaryTestRoot(import.meta.url);",
    "const dirs=['first-','second-'].map(p=>fs.mkdtempSync(path.join(os.tmpdir(),p)));",
    "for(const d of dirs) fs.openSync(path.join(d,'open.db'),'w');",
    "fs.writeFileSync(path.join(os.tmpdir(),'loose.tmp'),'temporary');",
    "console.log(JSON.stringify({root,dirs,provided:process.env.DATA_DIR}));",
    "process.exit(Number(process.argv[2]));"
  ].join('\n'));
  for (const status of [0, 7]) await t.test('multiple temporary allocations, exit '+status, () => {
    const env = {...process.env, TMP:scratch, TEMP:scratch, TMPDIR:scratch, DATA_DIR:external};
    delete env.BFAITH_TEST_TEMP_ENTRY; delete env.BFAITH_TEST_TEMP_ROOT;
    const r=spawnSync(process.execPath,[rootFixture,String(status)],{env,encoding:'utf8',timeout:20000,windowsHide:true});
    assert.equal(r.error,undefined); assert.equal(r.status,status,r.stderr);
    const record=JSON.parse(r.stdout.trim().split(/\r?\n/)[0]);
    assert.equal(record.provided,external);
    for(const dir of record.dirs) assert.equal(path.dirname(dir),record.root);
    assert.equal(fs.existsSync(record.root),false);
    assert.equal(fs.readFileSync(path.join(external,'keep.txt'),'utf8'),'keep');
  });
  await t.test('replacement of owned root with junction is rejected',()=>{
    const r=run('replace-root',false); assert.equal(r.status,1);
    assert.match(r.stderr,/cleanup path changed/);
    assert.equal(fs.readFileSync(path.join(external,'keep.txt'),'utf8'),'keep');
    fs.unlinkSync(r.record.dir);
  });
});
