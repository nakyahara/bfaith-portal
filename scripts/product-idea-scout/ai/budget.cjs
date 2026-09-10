'use strict';
const {ROUTING}=require('./cli.cjs');
const {requireValue:check}=require('./common.cjs');
class RunBudget {
  constructor({run_id,deadline,state=null,profile='default',now=Date.now()}) {
    check(typeof run_id==='string' && run_id,'RUN_ID_REQUIRED');
    check(Number.isFinite(Date.parse(deadline)),'DEADLINE_REQUIRED');
    if(state)check(state.run_id===run_id && state.deadline===deadline,'BUDGET_STATE_MISMATCH');
    profile=state?.profile||profile;check(['default','kw-discovery-v2','kw-screened-v3'].includes(profile),'INVALID_BUDGET_PROFILE');
    this.state=state?structuredClone(state):{run_id,deadline,profile,started_at:now,calls:[],quota_blocked:false};
  }
  reserve(stage,{retry=false,now=Date.now()}={}) {
    check(ROUTING[stage],'INVALID_STAGE');
    check(!this.state.quota_blocked,'QUOTA_BLOCKED');
    check(now<Date.parse(this.state.deadline) && now-this.state.started_at<90*60000,'DEADLINE_EXCEEDED');
    check(this.state.calls.length<8,'CALL_LIMIT');
    if(retry)check(this.state.calls.some(c=>c.stage===stage) && !this.state.calls.some(c=>c.retry),'RETRY_LIMIT');
    else check(this.state.calls.filter(c=>c.stage===stage && !c.retry).length<(this.state.profile==='kw-screened-v3'&&['R01','R03'].includes(stage)?3:this.state.profile==='kw-discovery-v2'&&stage==='R01'?7:ROUTING[stage].calls),'STAGE_CALL_LIMIT');
    const call={id:this.state.calls.length+1,stage,retry,started_at:now,status:'reserved'};this.state.calls.push(call);return structuredClone(call);
  }
  finish(id,result) {
    const call=this.state.calls.find(c=>c.id===id);check(call && call.status==='reserved','INVALID_RESERVATION');
    call.status=result.status;call.usage=result.usage||null;
    if(result.status==='QUOTA_BLOCKED')this.state.quota_blocked=true;
  }
  snapshot(){return structuredClone(this.state);}
}
module.exports={RunBudget};
