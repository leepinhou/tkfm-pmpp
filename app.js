const MATERIALS=[
 {id:'horn',short:'角',name:'堅固之角',tier:'base'},{id:'ingot',short:'錠',name:'銀錠',tier:'base'},{id:'oil',short:'油',name:'儀式之油',tier:'base'},{id:'dust',short:'塵',name:'腐蝕之塵',tier:'base'},{id:'fang',short:'牙',name:'劇毒之牙',tier:'base'},{id:'beak',short:'鷲',name:'獅鷲之喙',tier:'base'},{id:'hide',short:'皮',name:'熊皮',tier:'base'},{id:'book',short:'書',name:'邪語之書',tier:'base'},{id:'feather',short:'羽',name:'精緻尾羽',tier:'base'},{id:'silk',short:'絲',name:'蟲巢之絲',tier:'base'},
 {id:'hornP',short:'+角',name:'狂牛之角',tier:'advanced'},{id:'ingotP',short:'+錠',name:'聖銀錠',tier:'advanced'},{id:'oilP',short:'+油',name:'聖油',tier:'advanced'},{id:'crystalP',short:'+晶',name:'暗龍結晶',tier:'advanced'},{id:'fangP',short:'+牙',name:'邪龍毒牙',tier:'advanced'},{id:'beakP',short:'+鷲',name:'聖鷲之喙',tier:'advanced'},{id:'hideP',short:'+皮',name:'巨鹿毛皮',tier:'advanced'},{id:'bookP',short:'+書',name:'墮天之書',tier:'advanced'},{id:'featherP',short:'+羽',name:'女妖尾羽',tier:'advanced'},{id:'silkP',short:'+絲',name:'蛛后之絲',tier:'advanced'}
];
const EXAMPLE={horn:18,ingot:11,oil:0,dust:18,fang:26,beak:36,hide:28,book:0,feather:28,silk:36,hornP:4,ingotP:0,oilP:0,crystalP:4,fangP:8,beakP:8,hideP:2,bookP:0,featherP:2,silkP:8};
const STORE='tkfm-potential-workshop-v2';
const $=id=>document.getElementById(id);
const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const material=id=>MATERIALS.find(m=>m.id===id);
function emptyCounts(){return Object.fromEntries(MATERIALS.map(m=>[m.id,0]));}
function restore(){try{const saved=JSON.parse(localStorage.getItem(STORE))||JSON.parse(localStorage.getItem('tkfm-potential-workshop-v1'));if(saved?.counts)return saved;}catch{}return null;}
let state=restore()||{counts:emptyCounts()};
state.counts={...emptyCounts(),...state.counts};
state.selectedIndex=Number.isInteger(state.selectedIndex)?state.selectedIndex:0;
state.expandedIndex=null;
state.timer=state.timer&&Number.isFinite(state.timer.deadline)?state.timer:null;
state.vibrate=typeof state.vibrate==='boolean'?state.vibrate:true;
state.operationOverride=Array.isArray(state.operationOverride)?state.operationOverride:null;
state.plan=null;
let timerHandle=null,wakeHandle=null,vibrationHandle=null;
const scheduler=new ProductionScheduler();
let schedulerRequest=0,schedulerResult=null,schedulerComputing=false,schedulerLocked=false,schedulerProgress=null;
function persist(){try{localStorage.setItem(STORE,JSON.stringify(state));}catch(e){console.warn('無法儲存本機排程',e);}}
function totalQty(){return MATERIALS.reduce((n,m)=>n+(Number(state.counts[m.id])||0),0);}
function formatDuration(seconds){seconds=Math.max(0,Math.round(seconds));const m=Math.floor(seconds/60),s=seconds%60;return m?`${m} 分${s?`${String(s).padStart(2,'0')} 秒`:''}`:`${s} 秒`;}
function formatTime(seconds){seconds=Math.max(0,seconds);const h=Math.floor(seconds/3600),m=Math.floor(seconds%3600/60),s=seconds%60;return h?`${String(h).padStart(2,'0')}:${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`:`${String(m).padStart(2,'0')}:${String(s).padStart(2,'0')}`;}

function addJob(lane,id,qty){if(qty<=0)return;const last=lane.jobs.at(-1);if(last&&last.id===id)last.qty+=qty;else lane.jobs.push({id,qty});lane.load+=qty;}
function materialOrder(){return MATERIALS.filter(m=>(Number(state.counts[m.id])||0)>0).sort((a,b)=>(a.tier==='base'?0:1)-(b.tier==='base'?0:1)||(Number(state.counts[a.id])-Number(state.counts[b.id]))||MATERIALS.indexOf(a)-MATERIALS.indexOf(b));}
function assignFinalGroup(lanes,items){
 const amounts=items.map(item=>Number(state.counts[item.id]));
 if(items.length===1){const amount=amounts[0],parts=Math.min(3,amount),base=Math.floor(amount/parts),extra=amount%parts;for(let i=0;i<parts;i++)addJob(lanes[i],items[0].id,base+(i<extra?1:0));return;}
 if(items.length===2){
  const [first,second]=items,[a,b]=amounts;
  if(a+b<3){addJob(lanes[0],first.id,a);addJob(lanes[1],second.id,b);return;}
  const splitIndex=a>=b?0:1,splitItem=items[splitIndex],splitAmount=amounts[splitIndex],otherItem=items[1-splitIndex],otherAmount=amounts[1-splitIndex];
  addJob(lanes[0],splitItem.id,Math.ceil(splitAmount/2));addJob(lanes[1],otherItem.id,otherAmount);addJob(lanes[2],splitItem.id,Math.floor(splitAmount/2));
 }
}
function makeGreedyPlan(){
 const items=materialOrder();if(!items.length)return null;
 const lanes=[0,1,2].map(()=>({jobs:[],load:0}));
 if(items.length<3){assignFinalGroup(lanes,items);return lanes.map(l=>l.jobs);}
 const first=items.slice(0,3),left=Object.fromEntries(items.map(item=>[item.id,Number(state.counts[item.id])])),anchor=left[first[0].id];
 first.forEach((item,lane)=>{const amount=lane===0?left[item.id]:Math.min(left[item.id],anchor+1);addJob(lanes[lane],item.id,amount);left[item.id]-=amount;});
 for(const item of first){if(left[item.id]<=0)continue;const lane=lanes.reduce((best,current,index)=>current.load<lanes[best].load?index:best,0);addJob(lanes[lane],item.id,left[item.id]);left[item.id]=0;}
 const remaining=items.slice(3),fullGroups=remaining.length-(remaining.length%3);
 for(let start=0;start<fullGroups;start+=3)remaining.slice(start,start+3).forEach((item,lane)=>addJob(lanes[lane],item.id,left[item.id]));
 const remainder=remaining.slice(fullGroups);if(remainder.length)assignFinalGroup(lanes,remainder);
 return lanes.map(l=>l.jobs);
}
function makePlan(){const seed=makeGreedyPlan();return seed?optimizePlan(seed):null;}
function simulateOperations(plan){
 if(!plan)return[];
 const cursor=[0,0,0],active=[null,null,null],steps=[];let now=0;
 for(let i=0;i<3;i++){const job=plan[i]?.[0];if(job)active[i]={job,dueAt:job.qty*30000-15000,finishAt:job.qty*30000};}
 const initial=active.map((a,i)=>a?{kind:'set',job:a.job}:{kind:'idle',line:i});
 steps.push({time:0,lines:initial,redLines:[]});
 if(active.some(Boolean)&&active.filter(Boolean).length<3)steps[0].redLines=active.map((a,i)=>a?null:i).filter(i=>i!==null);
 let guard=0;
 while(active.some(Boolean)&&guard++<2000){
  now=Math.min(...active.filter(Boolean).map(a=>a.dueAt));
  const due=[];for(let i=0;i<3;i++)if(active[i]&&active[i].dueAt<=now+1)due.push(i);
  due.forEach(i=>{cursor[i]++;active[i]=null;});
  const assigned=[];
  for(const i of due){const job=plan[i]?.[cursor[i]];if(job){assigned.push(i);active[i]={job,dueAt:now+job.qty*30000,finishAt:now+15000+job.qty*30000};}}
  const workRemains=active.some(Boolean)||plan.some((jobs,i)=>cursor[i]<jobs.length);
  if(assigned.length){
   const lines=active.map((a,i)=>{if(due.includes(i))return a?{kind:'set',job:a.job}:{kind:'idle',line:i};if(!a)return{kind:'idle',line:i};const remaining=Math.max(1,Math.ceil((a.finishAt-(now+15000))/30000));return{kind:'continue',job:a.job,remaining};});
   steps.push({time:now+15000,lines,redLines:workRemains?lines.map((l,i)=>l.kind==='idle'?i:null).filter(i=>i!==null):[]});
  }else if(workRemains){const previous=steps.at(-1);for(const i of due)if(!previous.redLines.includes(i))previous.redLines.push(i);}
 }
 return steps;
}
function candidateFromPlan(plan){const qty={},orders=[[],[],[]];for(let lane=0;lane<3;lane++)for(const job of plan[lane]||[]){qty[job.id]??=[0,0,0];qty[job.id][lane]+=job.qty;if(!orders[lane].includes(job.id))orders[lane].push(job.id);}return{qty,orders};}
function cloneCandidate(c){return{qty:Object.fromEntries(Object.entries(c.qty).map(([id,parts])=>[id,[...parts]])),orders:c.orders.map(line=>[...line])};}
function candidatePlan(c){return c.orders.map((ids,lane)=>ids.filter(id=>(c.qty[id]?.[lane]||0)>0).map(id=>({id,qty:c.qty[id][lane]})));}
function alignLaneOrder(candidate,lane){
 const ids=candidate.orders[lane].filter(id=>(candidate.qty[id]?.[lane]||0)>0),n=ids.length;if(n<2||n>16)return;
 const weights=ids.map(id=>candidate.qty[id][lane]),baseMask=ids.reduce((mask,id,i)=>material(id).tier==='base'?mask|(1<<i):mask,0),size=1<<n,sums=new Int32Array(size),cost=new Int16Array(size),previous=new Int16Array(size),chosen=new Int8Array(size);cost.fill(30000);previous.fill(-1);chosen.fill(-1);cost[0]=0;
 for(let mask=1;mask<size;mask++){const bit=mask&-mask,index=31-Math.clz32(bit);sums[mask]=sums[mask^bit]+weights[index];}
 const external=new Set();for(let other=0;other<3;other++)if(other!==lane){let total=0;for(const id of candidate.orders[other]){total+=candidate.qty[id]?.[other]||0;if(total)external.add(total);}}
 const target=sums[size-1];
 for(let mask=0;mask<size;mask++){if(cost[mask]===30000)continue;const remainingBase=baseMask&~mask;for(let i=0;i<n;i++)if(!(mask&(1<<i))&&(!remainingBase||(baseMask&(1<<i)))){const next=mask|(1<<i),boundary=sums[next],penalty=next===size-1||external.has(boundary)?0:1,value=cost[mask]+penalty;if(value<cost[next]){cost[next]=value;previous[next]=mask;chosen[next]=i;}}}
 let mask=size-1,order=[];while(mask){const i=chosen[mask];if(i<0)return;order.push(ids[i]);mask=previous[mask];}candidate.orders[lane]=order.reverse();
}
function alignCandidate(candidate){for(let pass=0;pass<3;pass++)for(let lane=0;lane<3;lane++)alignLaneOrder(candidate,lane);return candidate;}
function scorePlan(plan){
 const steps=simulateOperations(plan),idle=steps.reduce((sum,s)=>sum+s.redLines.length,0),makespan=Math.max(0,...plan.map(line=>line.reduce((sum,j)=>sum+j.qty,0)));
 const loads=plan.map(line=>line.reduce((sum,j)=>sum+j.qty,0)),imbalance=Math.max(...loads)-Math.min(...loads);
 const splits=Object.values(candidateFromPlan(plan).qty).reduce((sum,parts)=>sum+Math.max(0,parts.filter(q=>q>0).length-1),0);
 return idle*1e8+steps.length*1e6+imbalance*1e3+splits*100+makespan;
}
function insertMaterial(c,lane,id){
 const ids=c.orders[lane],tier=material(id).tier;
 if(tier==='advanced'){ids.push(id);return;}
 const firstAdvanced=ids.findIndex(other=>material(other).tier==='advanced');ids.splice(firstAdvanced<0?ids.length:firstAdvanced,0,id);
}
function mutateCandidate(source,random){
 const c=cloneCandidate(source),ids=MATERIALS.filter(m=>(Number(state.counts[m.id])||0)>0).map(m=>m.id);
 if(random()<.66){
  const id=ids[Math.floor(random()*ids.length)],froms=[0,1,2].filter(l=>(c.qty[id]?.[l]||0)>0);if(!froms.length)return c;
  const from=froms[Math.floor(random()*froms.length)],destOptions=[0,1,2].filter(l=>l!==from),to=destOptions[Math.floor(random()*destOptions.length)],amount=1+Math.floor(random()*c.qty[id][from]);
  c.qty[id][from]-=amount;c.qty[id][to]=(c.qty[id][to]||0)+amount;
  if(c.qty[id][from]===0)c.orders[from]=c.orders[from].filter(other=>other!==id);
  if(!c.orders[to].includes(id))insertMaterial(c,to,id);
 }else{
  const lane=Math.floor(random()*3),swaps=[];
  for(let i=0;i<c.orders[lane].length;i++)for(let j=i+1;j<c.orders[lane].length;j++)if(material(c.orders[lane][i]).tier===material(c.orders[lane][j]).tier)swaps.push([i,j]);
  if(swaps.length){const [i,j]=swaps[Math.floor(random()*swaps.length)];[c.orders[lane][i],c.orders[lane][j]]=[c.orders[lane][j],c.orders[lane][i]];}
 }
 return c;
}
function randomCandidate(random){
 const c={qty:{},orders:[[],[],[]]};
 for(const m of MATERIALS){const total=Number(state.counts[m.id])||0;if(!total)continue;c.qty[m.id]=[0,0,0];
  const maxParts=Math.min(3,total),r=random(),parts=maxParts===1?1:maxParts===2?2:r<.55?1:r<.85?2:3,lanes=[0,1,2].sort(()=>random()-.5).slice(0,parts);
  let left=total;for(let i=0;i<parts;i++){const remaining=parts-i,amount=i===parts-1?left:1+Math.floor(random()*(left-remaining+1));c.qty[m.id][lanes[i]]=amount;left-=amount;}
  for(const lane of lanes)insertMaterial(c,lane,m.id);
 }
 for(const line of c.orders){const base=line.filter(id=>material(id).tier==='base').sort(()=>random()-.5),advanced=line.filter(id=>material(id).tier==='advanced').sort(()=>random()-.5);line.splice(0,line.length,...base,...advanced);}
 return c;
}
function optimizePlan(seedPlan){
 const work=MATERIALS.filter(m=>(Number(state.counts[m.id])||0)>0);if(work.length<3)return seedPlan;
 const signature=work.map(m=>`${m.id}:${state.counts[m.id]}`).join('|');let seed=Array.from(signature).reduce((n,ch)=>((n*31+ch.charCodeAt(0))>>>0),2166136261)||1;
 const random=()=>{seed=(seed*1664525+1013904223)>>>0;return seed/4294967296;};
 let best=alignCandidate(candidateFromPlan(seedPlan)),bestScore=scorePlan(candidatePlan(best));const runs=12,iterations=260;
 for(let run=0;run<runs;run++){
  let current=run===0?candidateFromPlan(seedPlan):alignCandidate(randomCandidate(random));
  let currentScore=scorePlan(candidatePlan(current));
  for(let i=0;i<iterations;i++){
   const next=mutateCandidate(current,random),nextScore=scorePlan(candidatePlan(next)),temperature=18000*(1-i/iterations)+100;
   if(nextScore<bestScore){best=cloneCandidate(next);bestScore=nextScore;}
   if(nextScore<currentScore||random()<Math.exp((currentScore-nextScore)/temperature)){current=next;currentScore=nextScore;}
  }
  current=alignCandidate(current);const alignedScore=scorePlan(candidatePlan(current));if(alignedScore<bestScore){best=cloneCandidate(current);bestScore=alignedScore;}
 }
 return candidatePlan(best);
}
function getMaterialsFromUI(){return Object.fromEntries(MATERIALS.map(m=>[m.short,Number(state.counts[m.id])||0]));}
function recalculateSchedule(){
 const request=++schedulerRequest,materials=getMaterialsFromUI();schedulerResult=null;schedulerProgress=null;schedulerLocked=false;state.plan=null;schedulerComputing=Object.values(materials).some(qty=>qty>0);
 if(!schedulerComputing){scheduler.stop();render();return;}
 scheduler.calculate(materials,payload=>{
  if(request!==schedulerRequest||!payload)return;
  const type=payload.type,data=type?payload.data:payload;
  if(type==='PROGRESS'||(Number.isFinite(data?.percent)&&Number.isFinite(data?.nodes))){schedulerProgress=data;schedulerComputing=true;updateStatsBadge(schedulerResult?.stats,data);updateProgressUI();return;}
  const result=type?data:payload;
  if(!result?.productionList)return;
  if(type==='UPDATE'&&schedulerProgress)schedulerProgress={...schedulerProgress,nodes:result.debug?.exploredNodes??schedulerProgress.nodes,elapsedMs:result.debug?.executionTimeMs??schedulerProgress.elapsedMs,currentBestBatches:result.stats?.totalBatches??schedulerProgress.currentBestBatches,currentBestOps:result.stats?.totalOperations??schedulerProgress.currentBestOps,currentMixedBatches:result.stats?.mixedBatches??schedulerProgress.currentMixedBatches};
  schedulerResult=result;schedulerComputing=(type==='UPDATE'||!type)&&result.debug?.status==='SEARCHING';
  if(type==='COMPLETE'||result.debug?.isOptimal)schedulerProgress={percent:100,nodes:result.debug?.exploredNodes||0,elapsedMs:result.debug?.executionTimeMs||0,currentBestBatches:result.stats?.totalBatches||0,currentBestOps:result.stats?.totalOperations||0,currentMixedBatches:result.stats?.mixedBatches||0};
  state.selectedIndex=Math.min(state.selectedIndex,Math.max(0,schedulerRows(result).length-1));render();
 });
 render();
}
function schedulerRows(result){
 if(!result?.productionList)return [];
 return result.productionList.map((entry,index)=>{
  const label=Object.keys(entry)[0]||'',raw=entry[label]||[];
  const lines=Array.from({length:3},(_,i)=>{
   const item=raw[i];if(!item)return{kind:'idle',line:i};
   const [short,qty]=Object.entries(item)[0]||[];const m=MATERIALS.find(x=>x.short===short);
   return m?{kind:'set',job:{id:m.id,qty:Number(qty)||0}}:{kind:'idle',line:i};
  });
  const hasFollowing=index<result.productionList.length-1;
  return{displayLabel:label,lines,redLines:hasFollowing?lines.map((line,i)=>line.kind==='idle'?i:null).filter(i=>i!==null):[]};
 });
}
function buildOperations(plan){return state.operationOverride||schedulerRows(schedulerResult);}
function operationLabel(step){if(step.displayLabel)return step.displayLabel;const lines=step.lines.filter(line=>line.kind!=='idle');if(!lines.length)return '';const names=lines.map(line=>material(line.job.id).short).join(' + '),quantities=lines.map(line=>line.kind==='continue'?line.remaining:line.job.qty).join(' + ');return `${names}：${quantities}`;}
function operationProduction(step){const produced={};for(const line of step.lines){if(line.kind==='idle')continue;const qty=line.kind==='continue'?line.remaining:line.job.qty;produced[line.job.id]=(produced[line.job.id]||0)+qty;}return produced;}
function commonProgress(step){const quantities=step.lines.filter(line=>line.kind!=='idle').map(line=>line.kind==='continue'?line.remaining:line.job.qty);return quantities.length?Math.min(...quantities):0;}

function renderInputs(){
 const groups=[['base','初階素材','BASE MATERIALS'],['advanced','進階素材','ADVANCED MATERIALS']];
 $('demandGroups').innerHTML=groups.map(([tier,title,mark])=>`<section class="tier-group"><div class="tier-title">${title}<span class="tier-mark">${mark}</span></div><div class="material-grid">${MATERIALS.filter(m=>m.tier===tier).map(m=>`<button type="button" class="material-tile" data-material="${m.id}" title="${esc(m.name)}" aria-label="${esc(m.short)}，${Number(state.counts[m.id])||0} 件。點左半減少，右半增加；按住顯示全名。"><span class="material-short">${esc(m.short)}</span><span class="material-count">${Number(state.counts[m.id])||0}</span><span class="long-name">${esc(m.name)}</span></button>`).join('')}</div></section>`).join('');
document.querySelectorAll('.material-tile').forEach(tile=>{
  let down=null,longTimer=null,longShown=false,suppressClick=false;const id=tile.dataset.material;
  const adjust=delta=>{if(state.timer)return;state.operationOverride=null;state.counts[id]=Math.max(0,(Number(state.counts[id])||0)+delta);state.selectedIndex=0;state.expandedIndex=null;renderInputs();recalculateSchedule();};
  const begin=e=>{if(state.timer||e.button>0)return;down={x:e.clientX,y:e.clientY,start:Number(state.counts[id])||0,delta:0,moved:false};longShown=false;try{tile.setPointerCapture(e.pointerId);}catch{}longTimer=setTimeout(()=>{tile.classList.add('show-name');longShown=true;suppressClick=true;},500);};
  const move=e=>{if(!down)return;const dx=e.clientX-down.x,dy=e.clientY-down.y;if(Math.abs(dx)>8&&Math.abs(dx)>Math.abs(dy)){down.moved=true;clearTimeout(longTimer);down.delta=(dx<0?-1:1)*Math.max(1,Math.round(Math.abs(dx)/24));tile.querySelector('.material-count').textContent=Math.max(0,down.start+down.delta);}};
  const finish=()=>{clearTimeout(longTimer);if(!down)return;const swipe=down.moved,delta=down.delta;if(swipe){suppressClick=true;adjust(delta);}else if(longShown){setTimeout(()=>tile.classList.remove('show-name'),1500);}down=null;};
  const cancel=()=>{clearTimeout(longTimer);if(down)tile.querySelector('.material-count').textContent=down.start;down=null;};
  tile.addEventListener('pointerdown',begin);tile.addEventListener('pointermove',move);tile.addEventListener('pointerup',finish);tile.addEventListener('pointercancel',cancel);
  tile.addEventListener('click',e=>{if(suppressClick){suppressClick=false;return;}const rect=tile.getBoundingClientRect();adjust(e.clientX<rect.left+rect.width/2?-1:1);});
 });
}
function renderOperations(){
 const operations=buildOperations(state.plan),total=totalQty(),itemCount=MATERIALS.filter(m=>Number(state.counts[m.id])>0).length;
 $('demandSummary').textContent=total?`${itemCount} 種素材 · 共 ${total} 件需求`:'尚未設定需求';
 $('emptyPlan').hidden=operations.length>0;$('lanes').hidden=!operations.length;
 if(!operations.length){$('operationBadge').textContent=schedulerComputing?'計算中…':total?'已完成':'尚未排程';$('planSummary').textContent=schedulerComputing?'正在計算生產流程。':total?'目前需求已安排完成。可修改數量重新規劃。':'設定素材需求後，開始規劃生產流程。';$('lanes').innerHTML='';$('emptyPlan').innerHTML=schedulerComputing?'<span class="empty-icon">…</span><span>正在計算生產流程</span>':total?'<span class="empty-icon">✓</span><span>目前需求已安排完成</span>':'<span class="empty-icon">↗</span><span>你的生產計畫會出現在這裡</span>';return;}
 const stats=!state.operationOverride?schedulerResult?.stats:null;
 updateStatsBadge(stats);
 const rowsForTime=schedulerRows(schedulerResult),makespan=Math.max(0,...[0,1,2].map(lane=>rowsForTime.reduce((sum,step)=>sum+(step.lines[lane].kind==='idle'?0:step.lines[lane].job.qty),0)))*30;
 const selected=Math.min(state.selectedIndex,operations.length-1);state.selectedIndex=selected;
 const debug=schedulerResult?.debug;
 $('planSummary').textContent=`預估總耗時 ${formatDuration(makespan)}${schedulerLocked?' · 已鎖定目前排程':schedulerComputing?' · 正在搜尋':debug?.isOptimal?' · 已確認最優解':''}`;
 $('lanes').innerHTML=operations.map((step,index)=>{
  const label=operationLabel(step)||'沒有生產項目',common=commonProgress(step),isSelected=index===selected,isExpanded=index===state.expandedIndex;
  const rows=step.lines.map((line,i)=>{const red=step.redLines.includes(i);if(line.kind==='idle')return `<div class="operation-line ${red?'line-warning':''}"><span class="line-label">生產線 0${i+1}</span><span class="line-value">空置${red?'<small>需求完成前會空置</small>':''}</span></div>`;const qty=line.kind==='continue'?line.remaining:line.job.qty;return `<div class="operation-line ${red?'line-warning':''}"><span class="line-label">生產線 0${i+1}</span><span class="line-value"><b>${esc(material(line.job.id).short)} × ${qty}</b><small>${line.kind==='continue'?'接續生產':'本階段生產'}${red?' · 將空置':''}</small></span></div>`;}).join('');
  return `<article class="operation-card ${isSelected?'selected':''} ${isExpanded?'expanded':''} ${step.redLines.length?'has-warning':''}" data-operation="${index}" role="button" tabindex="${state.timer?'-1':'0'}" aria-disabled="${!!state.timer}" aria-expanded="${isExpanded}"><div class="operation-head"><strong>${esc(label)}</strong><span>${formatDuration(Math.max(0,common*30-15))}${step.redLines.length?'　⚠':''}</span></div><div class="operation-lines">${rows}</div></article>`;
 }).join('');
 const selectOperation=event=>{const card=event.currentTarget;if(state.timer)return;const index=Number(card.dataset.operation);if(index===state.selectedIndex){state.expandedIndex=state.expandedIndex===index?null:index;}else{state.selectedIndex=index;state.expandedIndex=null;}render();};
 $('lanes').querySelectorAll('[data-operation]').forEach(card=>{card.addEventListener('click',selectOperation);card.addEventListener('keydown',event=>{if(event.key==='Enter'||event.key===' '){event.preventDefault();selectOperation({currentTarget:card});}});});
}
function updateStatsBadge(stats,progress){
 const badge=$('operationBadge');if(state.operationOverride){badge.textContent=`${state.operationOverride.length} 項剩餘流程`;return;}
 if(progress){badge.textContent=`遊戲 ${progress.currentBestBatches||stats?.totalBatches||0} · 線設定 ${progress.currentBestOps??stats?.totalOperations??0} · 混搭 ${progress.currentMixedBatches??stats?.mixedBatches??0}`;return;}
 badge.textContent=stats?`遊戲 ${stats.totalBatches} · 線設定 ${stats.totalOperations} · 混搭 ${stats.mixedBatches}`:`${buildOperations(state.plan).length} 項流程`;
}
function render(){
 renderOperations();const canRun=!!buildOperations(state.plan).length;
 $('runBtn').disabled=(!canRun&&!schedulerComputing)||!!state.timer;$('runBtn').querySelector('span:first-child').textContent=schedulerComputing?'停止搜索':'生產此項';
 $('timerDock').hidden=!state.timer;
 updateProgressUI();
 if(state.timer)drawTimer();
 persist();
}
function updateProgressUI(){
 const panel=$('searchProgress'),bar=$('searchProgressBar'),text=$('searchProgressText');if(!panel||!bar||!text)return;
 const debug=schedulerResult?.debug;
 if(schedulerLocked){panel.hidden=true;return;}
 if(schedulerComputing){
  const progress=schedulerProgress,percent=Math.min(99,Math.max(0,Number(progress?.percent)||0));
  panel.hidden=false;bar.style.width=`${percent}%`;bar.parentElement.setAttribute('aria-valuenow',String(percent));
  text.textContent=progress?`分支 ${percent}% · ${Number(progress.nodes||0).toLocaleString()} 節點 · ${progress.currentBestBatches||0} 批次／${progress.currentBestOps||0} 線設定／${progress.currentMixedBatches||0} 混搭 · ${formatDuration((progress.elapsedMs||0)/1000)}`:(debug?.message||'搜尋進度準備中…');
  return;
 }
 if(debug?.isOptimal){const progress=schedulerProgress||{};panel.hidden=false;bar.style.width='100%';bar.parentElement.setAttribute('aria-valuenow','100');text.textContent=`已確認全域最優解 · ${Number(progress.nodes??debug.exploredNodes??0).toLocaleString()} 節點 · ${progress.elapsedMs??debug.executionTimeMs??0}ms`;return;}
 panel.hidden=true;
}
function startSelected(){
 if(state.timer)return;
 scheduler.stop();schedulerRequest++;
 schedulerComputing=false;schedulerLocked=true;schedulerProgress=null;
 stopVibration();
 const steps=buildOperations(state.plan);if(!steps.length)return;
 const index=Math.min(state.selectedIndex,steps.length-1),step=steps[index],progress=commonProgress(step),produced=operationProduction(step);
 if(!progress||!Object.keys(produced).length)return;
 const label=operationLabel(step);
 Object.entries(produced).forEach(([id,qty])=>state.counts[id]=Math.max(0,(Number(state.counts[id])||0)-qty));
 state.operationOverride=steps.filter((_,stepIndex)=>stepIndex!==index);state.plan=null;state.selectedIndex=Math.min(index,Math.max(0,buildOperations(state.plan).length-1));state.expandedIndex=null;
 state.timer={deadline:Date.now()+Math.max(0,progress*30000-15000),label,next:buildOperations(state.plan)[0]?operationLabel(buildOperations(state.plan)[0]):'本次需求已安排完成',notified:false};
 requestNotifications();renderInputs();render();scheduleTimer();
}
async function requestNotifications(){if(!('Notification'in window))return;if(Notification.permission==='default'){try{await Notification.requestPermission();}catch{}}}
function scheduleTimer(){clearTimeout(wakeHandle);if(!state.timer)return;const ms=state.timer.deadline-Date.now();if(ms<=0){finishTimer();return;}wakeHandle=setTimeout(finishTimer,ms);}
async function sendSystemNotification(timer){
 if(!('Notification'in window)||Notification.permission!=='granted')return;
 const body=timer.next==='本次需求已安排完成'?timer.next:`接下來：${timer.next}`;
 try{const registration=await navigator.serviceWorker?.ready;if(registration?.showNotification)await registration.showNotification('潛力工坊｜生產提醒',{body,icon:'./icon.svg',badge:'./icon.svg',tag:'tkfm-production-next',renotify:true,data:{url:'./'}});else new Notification('潛力工坊｜生產提醒',{body,icon:'./icon.svg',tag:'tkfm-production-next'});}catch(error){try{new Notification('潛力工坊｜生產提醒',{body,icon:'./icon.svg'});}catch{console.info('系統通知無法顯示',error);}}
}
function stopVibration(){if(vibrationHandle){clearInterval(vibrationHandle);vibrationHandle=null;}if(navigator.vibrate)navigator.vibrate(0);}
function finishTimer(){if(!state.timer)return;const timer=state.timer;clearTimeout(wakeHandle);wakeHandle=null;state.timer=null;persist();clearInterval(timerHandle);timerHandle=null;render();sendSystemNotification(timer);if(state.vibrate&&navigator.vibrate){navigator.vibrate([900,350,900,350,900]);vibrationHandle=setInterval(()=>navigator.vibrate([900,350,900,350,900]),4000);}}
function stopTimer(){clearTimeout(wakeHandle);clearInterval(timerHandle);stopVibration();wakeHandle=null;timerHandle=null;state.timer=null;persist();render();}
function drawTimer(){if(!state.timer)return;$('timerValue').textContent=formatTime(Math.ceil(Math.max(0,state.timer.deadline-Date.now())/1000));$('timerLabel').textContent=`等待提醒 · ${state.timer.label}`;clearInterval(timerHandle);timerHandle=setInterval(()=>{if(!state.timer){clearInterval(timerHandle);return;}const remain=state.timer.deadline-Date.now();if(remain<=0){finishTimer();return;}$('timerValue').textContent=formatTime(Math.ceil(remain/1000));},250);}
function checkTimer(){if(!state.timer)return;if(state.timer.deadline<=Date.now())finishTimer();else{render();scheduleTimer();}}

function adjustExampleOrClear(counts){if(state.timer)stopTimer();state.operationOverride=null;state.counts={...emptyCounts(),...counts};state.selectedIndex=0;state.expandedIndex=null;renderInputs();recalculateSchedule();}
 $('runBtn').addEventListener('click',()=>{if(schedulerComputing){scheduler.stop();schedulerRequest++;schedulerComputing=false;schedulerProgress=null;render();return;}startSelected();});
$('stopBtn').addEventListener('click',stopTimer);
$('exampleBtn').addEventListener('click',()=>adjustExampleOrClear(EXAMPLE));
$('clearBtn').addEventListener('click',()=>adjustExampleOrClear(emptyCounts()));
 $('vibrateToggle').checked=state.vibrate;$('vibrateToggle').addEventListener('change',()=>{state.vibrate=$('vibrateToggle').checked;if(!state.vibrate)stopVibration();persist();});
if('serviceWorker'in navigator)window.addEventListener('load',()=>navigator.serviceWorker.register('./sw.js').catch(error=>console.info('Service worker 無法啟用',error)));
document.addEventListener('visibilitychange',()=>{if(document.visibilityState==='visible')checkTimer();});
window.addEventListener('pageshow',checkTimer);
renderInputs();if(state.operationOverride)render();else recalculateSchedule();checkTimer();
