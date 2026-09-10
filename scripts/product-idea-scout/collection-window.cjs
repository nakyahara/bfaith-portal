'use strict';
// Reserve 04:15-14:00 JST for the morning keyword run and existing daytime jobs.
function deadline(start,maxMs,enabled=process.env.SCOUT_KW_WINDOW==='1'){
  if(!enabled)return start+maxMs;
  const jst=new Date(start+9*3600000);const minutes=jst.getUTCHours()*60+jst.getUTCMinutes();
  let cutoff=Date.UTC(jst.getUTCFullYear(),jst.getUTCMonth(),jst.getUTCDate(),4,15)-9*3600000;
  if(minutes>=14*60)cutoff+=86400000;
  return Math.min(start+maxMs,Math.max(start,cutoff));
}
module.exports={deadline};
if(require.main===module)process.exit(deadline(Date.now(),19*3600000)>Date.now()+1000?0:3);
