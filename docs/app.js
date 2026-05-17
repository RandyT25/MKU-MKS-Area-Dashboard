// MKU & MKS Dashboard — app.js

let company='ALL', stockFilter='all', activeDate=RAW.latest, charts={};

const fmtRp=n=>{if(n>=1e9)return'Rp '+(n/1e9).toFixed(1)+'B';if(n>=1e6)return'Rp '+(n/1e6).toFixed(1)+'M';if(n>=1e3)return'Rp '+(n/1e3).toFixed(0)+'K';return'Rp '+Math.round(n).toLocaleString();};
const fmtQ=n=>{const r=Math.round(n*100)/100;return r%1===0?r.toFixed(0):r.toFixed(1);};
const pct=(a,t)=>t>0?Math.round(a/t*100):0;
const fmtD=d=>{const[,m,dy]=d.split('-');return parseInt(dy)+' '+['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][parseInt(m)];};
const isLatest=d=>d===RAW.latest;
const isFull=d=>d==='ALL'?true:isLatest(d);
const COPTS={responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#8a93b0',font:{family:'Plus Jakarta Sans',size:11},boxWidth:10,padding:14}}},scales:{x:{ticks:{color:'#8a93b0',font:{family:'Plus Jakarta Sans',size:10}},grid:{color:'#f0f2f7'},border:{display:false}},y:{ticks:{color:'#8a93b0',font:{family:'Plus Jakarta Sans',size:10}},grid:{color:'#f0f2f7'},border:{display:false}}}};

function buildDT(){
  const lbl=activeDate==='ALL'?'📅 All Days':'📅 '+fmtD(activeDate);
  const mlbl=activeDate==='ALL'?'📅 All':'📅 '+fmtD(activeDate);
  const lbl_el=document.getElementById('date-dd-lbl');if(lbl_el)lbl_el.textContent=lbl;
  const mlbl_el=document.getElementById('m-date-dd-lbl');if(mlbl_el)mlbl_el.textContent=mlbl;
  const items=['ALL',...RAW.dates].map(d=>{
    const label=d==='ALL'?'All Days':fmtD(d)+(isLatest(d)?' ★':'');
    return`<button class="date-dd-item ${activeDate===d?'active':''}" onclick="setDate('${d}')"><span class="ddi-dot"></span>${label}</button>`;
  }).join('');
  ['date-dd-menu','m-date-dd-menu'].forEach(id=>{
    const el=document.getElementById(id);if(el)el.innerHTML=items;
  });
}
function setDate(d){
  activeDate=d;
  document.querySelectorAll('.date-dd-wrap').forEach(w=>w.classList.remove('open'));
  buildDT();renderAll();
}
function toggleDateDD(){
  const wrap=document.getElementById('date-dd-wrap');
  const mwrap=document.getElementById('m-date-dd-wrap');
  const isOpen=(wrap&&wrap.classList.contains('open'))||(mwrap&&mwrap.classList.contains('open'));
  document.querySelectorAll('.date-dd-wrap,.dl-wrap').forEach(w=>w.classList.remove('open'));
  if(!isOpen){if(wrap)wrap.classList.add('open');if(mwrap)mwrap.classList.add('open');}
}

// ── Data accessors ──────────────────────────────────────────────

function getSO(){
  let r=RAW.so;
  if(activeDate!=='ALL'&&activeDate!==RAW.latest)return[];
  if(company!=='ALL')r=r.filter(x=>x.division===(company==='MKU'?'MKU Bali':'MKS Bali'));
  return r;
}

function getSummary(d){
  return RAW.so_summary[d]||{rev:0,cnt:0,cust_cnt:0,mku_rev:0,mks_rev:0,rep_rev:{},prod_rev:{},cust:{}};
}

function getAggSummary(){
  const dates=activeDate==='ALL'?RAW.dates:[activeDate];
  const agg={rev:0,cnt:0,rep_rev:{},prod_rev:{},cust:{}};
  const custSet=new Set();
  const divMap={};RAW.so.forEach(r=>{divMap[r.sales]=r.division;});
  const divT=company==='MKU'?'MKU Bali':company==='MKS'?'MKS Bali':null;
  const okDiv=n=>!divT||!divMap[n]||divMap[n]===divT;
  if(divT&&dates.includes(RAW.latest)){
    RAW.so.filter(r=>r.division===divT).forEach(r=>{
      agg.rev+=r.revenue||0;agg.cnt+=1;
      agg.rep_rev[r.sales]=(agg.rep_rev[r.sales]||0)+r.revenue;
      agg.prod_rev[r.product]=(agg.prod_rev[r.product]||0)+r.revenue;
      if(!agg.cust[r.customer])agg.cust[r.customer]={rev:0,so:0,sales:r.sales,div:r.division};
      agg.cust[r.customer].rev+=r.revenue;agg.cust[r.customer].so+=1;custSet.add(r.customer);
    });
    dates.filter(d=>d!==RAW.latest).forEach(d=>{
      const s=getSummary(d);
      agg.rev+=company==='MKU'?(s.mku_rev||0):(s.mks_rev||0);
      Object.entries(s.rep_rev||{}).forEach(([k,v])=>{if(okDiv(k))agg.rep_rev[k]=(agg.rep_rev[k]||0)+v;});
      Object.entries(s.prod_rev||{}).forEach(([k,v])=>{agg.prod_rev[k]=(agg.prod_rev[k]||0)+v;});
      Object.entries(s.cust||{}).forEach(([k,v])=>{
        if(!okDiv(v.sales))return;
        if(!agg.cust[k])agg.cust[k]={rev:0,so:0,sales:v.sales,div:v.div};
        agg.cust[k].rev+=v.rev;agg.cust[k].so+=v.so;custSet.add(k);agg.cnt+=v.so;
      });
    });
  } else {
    dates.forEach(d=>{
      const s=getSummary(d);
      if(divT){
        agg.rev+=company==='MKU'?(s.mku_rev||0):(s.mks_rev||0);agg.cnt+=s.cnt||0;
        Object.entries(s.rep_rev||{}).forEach(([k,v])=>{if(okDiv(k))agg.rep_rev[k]=(agg.rep_rev[k]||0)+v;});
        Object.entries(s.prod_rev||{}).forEach(([k,v])=>{agg.prod_rev[k]=(agg.prod_rev[k]||0)+v;});
        Object.entries(s.cust||{}).forEach(([k,v])=>{
          if(!okDiv(v.sales))return;
          if(!agg.cust[k])agg.cust[k]={rev:0,so:0,sales:v.sales,div:v.div};
          agg.cust[k].rev+=v.rev;agg.cust[k].so+=v.so;custSet.add(k);
        });
      } else {
        agg.rev+=s.rev||0;agg.cnt+=s.cnt||0;
        Object.entries(s.rep_rev||{}).forEach(([k,v])=>{agg.rep_rev[k]=(agg.rep_rev[k]||0)+v;});
        Object.entries(s.prod_rev||{}).forEach(([k,v])=>{agg.prod_rev[k]=(agg.prod_rev[k]||0)+v;});
        Object.entries(s.cust||{}).forEach(([k,v])=>{
          if(!agg.cust[k])agg.cust[k]={rev:0,so:0,sales:v.sales,div:v.div};
          agg.cust[k].rev+=v.rev;agg.cust[k].so+=v.so;custSet.add(k);
        });
      }
    });
  }
  agg.cust_cnt=custSet.size;
  return agg;
}

function getDel(){
  const dates=activeDate==='ALL'?RAW.dates:[activeDate];
  let all=[];
  dates.forEach(d=>{
    const dd=RAW.delivery_by_date[d];if(!dd)return;
    if(isLatest(d)){
      if(company==='ALL'||company==='MKU')(dd.mku_full||[]).forEach(r=>all.push({...r,co:'MKU',date:d}));
      if(company==='ALL'||company==='MKS')(dd.mks_full||[]).forEach(r=>all.push({...r,co:'MKS',date:d}));
    } else {
      const issues=dd.issues||[];
      if(company!=='MKU') issues.filter(r=>r.co==='MKS'||!r.co).forEach(r=>all.push({...r,date:d}));
      if(company!=='MKS') issues.filter(r=>r.co==='MKU').forEach(r=>all.push({...r,date:d}));
    }
  });
  return all;
}

function getDelStats(){
  const dates=activeDate==='ALL'?RAW.dates:[activeDate];
  let tot=0,ful=0,by_area={};
  dates.forEach(d=>{
    const dd=RAW.delivery_by_date[d];if(!dd)return;
    if(isLatest(d)){
      let rows=[];
      if(company==='ALL'||company==='MKU')(dd.mku_full||[]).forEach(r=>rows.push({...r,co:'MKU'}));
      if(company==='ALL'||company==='MKS')(dd.mks_full||[]).forEach(r=>rows.push({...r,co:'MKS'}));
      tot+=rows.length;ful+=rows.filter(r=>r.ket==='FULFILLED').length;
      rows.forEach(r=>{const a=(r.area||'').trim()||'All Areas';if(!by_area[a])by_area[a]={t:0,ok:0};by_area[a].t+=1;if(r.ket==='FULFILLED')by_area[a].ok+=1;});
    } else {
      tot+=dd.tot||0;ful+=dd.ful||0;
      Object.entries(dd.by_area||{}).forEach(([a,v])=>{if(!by_area[a])by_area[a]={t:0,ok:0};by_area[a].t+=v.t;by_area[a].ok+=v.ok;});
    }
  });
  return{tot,ful,unf:tot-ful,by_area};
}

function getStk(){
  const date=activeDate==='ALL'?RAW.latest:activeDate;
  const sd=RAW.stock_by_date[date];if(!sd)return[];
  if(isLatest(date)){
    const mku=sd.MKU_full||sd.MKU||sd.mku||[];
    const mks=sd.MKS_full||sd.MKS||sd.mks||[];
    if(company==='MKU')return mku.map(s=>({...s,co:'MKU'}));
    if(company==='MKS')return mks.map(s=>({...s,co:'MKS'}));
    return[...mku.map(s=>({...s,co:'MKU'})),...mks.map(s=>({...s,co:'MKS'}))];
  }
  const mku=sd.MKU||[];const mks=sd.MKS||[];
  if(company==='MKU')return mku.map(s=>({...s,co:'MKU'}));
  if(company==='MKS')return mks.map(s=>({...s,co:'MKS'}));
  return[...mku.map(s=>({...s,co:'MKU'})),...mks.map(s=>({...s,co:'MKS'}))];
}

function getStkSummary(){
  const date=activeDate==='ALL'?RAW.latest:activeDate;
  const sd=RAW.stock_by_date[date];if(!sd)return null;
  return sd.summary||null;
}

function getTgt(){
  const date=activeDate==='ALL'?RAW.latest:activeDate;
  return RAW.targets_by_date[date]||RAW.targets_by_date[RAW.latest];
}

// ── UI ──────────────────────────────────────────────────────────

function setCompany(c){
  company=c;
  ['all','mku','mks'].forEach(x=>{
    const cls=x==='all'?'act-all':x==='mku'?'act-mku':'act-mks';
    const match=(c==='ALL'&&x==='all')||(c===x.toUpperCase());
    ['btn-'+x,'m-btn-'+x].forEach(id=>{const el=document.getElementById(id);if(el)el.className='co-btn'+(match?' '+cls:'');});
  });
  renderAll();
}

function switchTab(n){
  const tabs=['target','so','delivery','reps','stock','alerts','biz'];
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',tabs[i]===n));
  document.querySelectorAll('.tc').forEach(c=>c.classList.remove('active'));
  const el=document.getElementById('tc-'+n);if(el)el.classList.add('active');
}

function mobileTab(n){
  switchTab(n);
  document.querySelectorAll('.mnav').forEach(b=>b.classList.remove('active'));
  const btn=document.getElementById('mn-'+n);if(btn)btn.classList.add('active');
  window.scrollTo({top:0,behavior:'smooth'});
}

function renderAll(){buildDT();renderKPIs();renderTarget();renderSO();renderDel();renderReps();renderStock();renderAlerts();renderMoM();}

function renderKPIs(){
  const agg=getAggSummary();
  const delStats=getDelStats();
  const stk=getStk();
  const stkSum=getStkSummary();
  const outCount=stk.filter(s=>s.st==='out').length;
  const critCount=stk.filter(s=>s.st==='critical'||s.st==='low').length;
  const dateLabel=activeDate==='ALL'?(RAW.dates.length+' days'):fmtD(activeDate);
  document.getElementById('kpi-strip').innerHTML=`
    <div class="kpi-card c-mks"><div class="kpi-icon mks">💰</div><div class="kpi-label">Total Revenue</div><div class="kpi-value mks">${fmtRp(agg.rev)}</div><div class="kpi-sub">${agg.cnt} orders · ${dateLabel}</div></div>
    <div class="kpi-card c-gray"><div class="kpi-icon gray">👥</div><div class="kpi-label">Sales Reps</div><div class="kpi-value">${Object.keys(agg.rep_rev).length}</div><div class="kpi-sub">${agg.cust_cnt} customers</div></div>
    <div class="kpi-card c-grn"><div class="kpi-icon grn">🚚</div><div class="kpi-label">Fulfilment</div><div class="kpi-value grn">${delStats.tot>0?pct(delStats.ful,delStats.tot):'-'}%</div><div class="kpi-sub">${delStats.ful} of ${delStats.tot}</div></div>
    <div class="kpi-card ${delStats.unf>0?'c-org':'c-grn'}"><div class="kpi-icon ${delStats.unf>0?'org':'grn'}">📋</div><div class="kpi-label">Unfulfilled</div><div class="kpi-value ${delStats.unf>0?'org':''}">${delStats.unf}</div><div class="kpi-sub">Not fully delivered</div></div>
    <div class="kpi-card ${outCount+critCount>0?'c-mku':'c-grn'}"><div class="kpi-icon ${outCount+critCount>0?'mku':'grn'}">${outCount+critCount>0?'🔴':'✅'}</div><div class="kpi-label">Stock Alerts</div><div class="kpi-value ${outCount+critCount>0?'mku':''}">${outCount+critCount}</div><div class="kpi-sub">${outCount} out · ${critCount} low</div></div>
    <div class="kpi-card c-gray"><div class="kpi-icon gray">📦</div><div class="kpi-label">Active SKUs</div><div class="kpi-value">${stkSum?(stkSum.mku_total+stkSum.mks_total):stk.length}</div><div class="kpi-sub">Latest snapshot</div></div>`;
}

function renderTarget(){
  const {targets:T,area_targets:areas,nestle_areas:nestleA}=getTgt();
  const COL={FOOD:'#2563eb',BEVERAGE:'#059669',NESTLE:'#7c3aed'};
  const ICO={FOOD:'🍽️',BEVERAGE:'🥤',NESTLE:'☕'};
  const tot_t=Object.values(T).reduce((s,t)=>s+t.target,0);
  const tot_a=Object.values(T).reduce((s,t)=>s+t.achievement,0);
  const tp=pct(tot_a,tot_t);
  const cats=Object.keys(T);
  const lastDate=RAW.latest;
  const dayNum=parseInt(lastDate.split('-')[2]);
  // ── FIX: use actual days in current month, not hardcoded 30 ────
  const daysInMonth=new Date(parseInt(lastDate.split('-')[0]),parseInt(lastDate.split('-')[1]),0).getDate();
  const timePct=Math.round(dayNum/daysInMonth*100);
  const badgeCls=p=>p>=timePct?'b-grn':p>=(timePct*0.75)?'b-org':'b-red';
  const colP=p=>p>=timePct?'var(--grn)':p>=(timePct*0.75)?'var(--org)':'var(--mku)';

  document.getElementById('tgt-cats').innerHTML=`
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px">
      <div style="background:linear-gradient(135deg,#eff4ff,#dce8ff);border:1px solid #c7d8fc;border-radius:12px;padding:18px;border-top:3px solid var(--mks)">
        <div style="font-size:.61rem;font-weight:700;color:var(--mks);text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px">🎯 Grand Total</div>
        <div style="font-size:2rem;font-weight:800;line-height:1;margin-bottom:4px">${tp}%</div>
        <div style="font-size:.7rem;color:var(--txt2);margin-bottom:10px">${fmtRp(tot_a)} / ${fmtRp(tot_t)}</div>
        <div class="pb"><div class="pb-fill" style="width:${Math.min(tp,100)}%;background:var(--mks)"></div></div>
      </div>
      ${cats.map(c=>{const t=T[c],p=pct(t.achievement,t.target),col=COL[c];
        const bg=c==='FOOD'?'#eff4ff':c==='BEVERAGE'?'#ecfdf5':'#f5f3ff';
        const bd=c==='FOOD'?'#c7d8fc':c==='BEVERAGE'?'#a7f3d0':'#ddd6fe';
        return`<div style="background:${bg};border:1px solid ${bd};border-radius:12px;padding:18px;border-top:3px solid ${col}">
          <div style="font-size:.61rem;font-weight:700;color:${col};text-transform:uppercase;letter-spacing:.08em;margin-bottom:8px">${ICO[c]} ${c}</div>
          <div style="font-size:2rem;font-weight:800;line-height:1;margin-bottom:4px">${p}%</div>
          <div style="font-size:.7rem;color:var(--txt2);margin-bottom:10px">${fmtRp(t.achievement)} / ${fmtRp(t.target)}</div>
          <div class="pb"><div class="pb-fill" style="width:${Math.min(p,100)}%;background:${col}"></div></div>
        </div>`;}).join('')}
    </div>`;

  if(charts.global)charts.global.destroy();
  charts.global=new Chart(document.getElementById('ch-global'),{type:'bar',data:{labels:['Food & Bev'],datasets:[
    {label:'Food Target',data:[T.FOOD?.target||0],backgroundColor:'#c7d8fc',borderRadius:6,stack:'a'},
    {label:'Food Achieved',data:[T.FOOD?.achievement||0],backgroundColor:'#2563eb',borderRadius:6,stack:'b'},
    {label:'Bev Target',data:[T.BEVERAGE?.target||0],backgroundColor:'#a7f3d0',borderRadius:6,stack:'c'},
    {label:'Bev Achieved',data:[T.BEVERAGE?.achievement||0],backgroundColor:'#059669',borderRadius:6,stack:'d'},
  ]},options:{...COPTS,scales:{...COPTS.scales,y:{...COPTS.scales.y,ticks:{...COPTS.scales.y.ticks,callback:v=>v>=1e9?(v/1e9).toFixed(1)+'B':v>=1e6?(v/1e6).toFixed(0)+'M':v}}}}});

  if(charts.area)charts.area.destroy();
  charts.area=new Chart(document.getElementById('ch-area'),{type:'bar',data:{
    labels:(areas||[]).map(a=>a.area.length>14?a.area.slice(0,13)+'…':a.area),
    datasets:[
      {label:'Achieved',data:(areas||[]).map(a=>a.food_ach+a.bev_ach),backgroundColor:'#93b4f8',borderRadius:4,stack:'a'},
      {label:'Remaining',data:(areas||[]).map(a=>Math.max(0,(a.food_target+a.bev_target)-(a.food_ach+a.bev_ach))),backgroundColor:'#e4e8ef',stack:'a'}
    ]},options:{...COPTS,scales:{...COPTS.scales,x:{...COPTS.scales.x,stacked:true},y:{...COPTS.scales.y,stacked:true,ticks:{...COPTS.scales.y.ticks,callback:v=>v>=1e9?(v/1e9).toFixed(1)+'B':v>=1e6?(v/1e6).toFixed(0)+'M':v}}}}});

  // ── Area table with ↑↓ growth vs previous date ──────────────────
  const allDates=(RAW.dates||[]).slice().sort();
  const latestIdx=allDates.indexOf(RAW.latest);
  const prevDate=latestIdx>0?allDates[latestIdx-1]:null;
  const prevAreas=prevDate?(RAW.targets_by_date[prevDate]||{}).area_targets||[]:[];
  const prevAreaMap={};prevAreas.forEach(a=>{prevAreaMap[a.area]=a.pct||pct(a.food_ach+a.bev_ach,a.food_target+a.bev_target);});

  document.getElementById('tbl-area').innerHTML=`
    <thead><tr><th>Area</th><th>Sales</th><th class="num">Food</th><th class="num">Bev</th><th class="num">Target Total</th><th class="num">Achieved</th><th>% <span style="font-weight:400;color:var(--txt3);font-size:.58rem">(on track ≥${timePct}%)</span></th></tr></thead>
    <tbody>${(areas||[]).map(a=>{
      const p=a.pct,cls=badgeCls(p);
      const prevP=prevAreaMap[a.area];
      const delta=prevP!=null?p-prevP:null;
      const growthHtml=delta!=null&&delta!==0
        ?`<span style="font-size:.6rem;font-weight:700;color:${delta>0?'var(--grn)':'var(--mku)'};margin-left:4px">${delta>0?'↑':'↓'}${Math.abs(delta)}pp</span>`:'';
      return`<tr>
        <td style="font-weight:600">${a.area}</td><td style="color:var(--txt2);font-size:.68rem">${a.sales}</td>
        <td class="num">${fmtRp(a.food_ach)}</td><td class="num">${fmtRp(a.bev_ach)}</td>
        <td class="num" style="color:var(--txt3)">${fmtRp(a.food_target+a.bev_target)}</td>
        <td class="num" style="font-weight:700">${fmtRp(a.food_ach+a.bev_ach)}</td>
        <td><span class="badge ${cls}">${p}%</span>${growthHtml}</td></tr>`;}).join('')}
    </tbody>
    <tfoot><tr><td colspan="2"><strong>GRAND TOTAL</strong></td>
      <td class="num"><strong style="color:var(--mks)">${fmtRp((areas||[]).reduce((s,a)=>s+a.food_ach,0))}</strong></td>
      <td class="num"><strong style="color:var(--grn)">${fmtRp((areas||[]).reduce((s,a)=>s+a.bev_ach,0))}</strong></td>
      <td class="num">${fmtRp((areas||[]).reduce((s,a)=>s+a.food_target+a.bev_target,0))}</td>
      <td class="num"><strong>${fmtRp((areas||[]).reduce((s,a)=>s+a.food_ach+a.bev_ach,0))}</strong></td>
      <td><span class="badge ${badgeCls(tp)}">${tp}%</span></td>
    </tr></tfoot>`;

  document.getElementById('nestle-table').innerHTML=`
    <thead><tr><th>Channel</th><th>Sales</th><th class="num">Target</th><th class="num">Achieved</th><th>% <span style="font-weight:400;color:var(--txt3);font-size:.58rem">(on track ≥${timePct}%)</span></th></tr></thead>
    <tbody>${(nestleA||[]).map(n=>{const p=pct(n.achievement,n.target),cls=badgeCls(p);return`<tr>
      <td style="font-weight:600">${n.area}</td>
      <td style="color:var(--txt2);font-size:.68rem">${n.sales||'—'}</td>
      <td class="num" style="color:var(--txt3)">${fmtRp(n.target)}</td>
      <td class="num" style="font-weight:700">${fmtRp(n.achievement)}</td>
      <td><span class="badge ${cls}">${p}%</span></td></tr>`;}).join('')}</tbody>
    <tfoot><tr>
      <td colspan="2"><strong>GRAND TOTAL</strong></td>
      <td class="num">${fmtRp((nestleA||[]).reduce((s,n)=>s+n.target,0))}</td>
      <td class="num"><strong>${fmtRp((nestleA||[]).reduce((s,n)=>s+n.achievement,0))}</strong></td>
      <td><span class="badge ${badgeCls(pct((nestleA||[]).reduce((s,n)=>s+n.achievement,0),(nestleA||[]).reduce((s,n)=>s+n.target,0)))}">${pct((nestleA||[]).reduce((s,n)=>s+n.achievement,0),(nestleA||[]).reduce((s,n)=>s+n.target,0))}%</span></td>
    </tr></tfoot>`;

  // ── Balian table ──────────────────────────────────────────────
  const balian=getTgt().balian||[];
  const balianList=Array.isArray(balian)?balian:Object.entries(balian).map(([area,v])=>({area,sales:v.sales,ach:v.ach}));
  const balianTotal=balianList.reduce((s,r)=>s+r.ach,0);
  const balianEl=document.getElementById('tbl-balian');
  if(balianEl){
    if(balianList.length===0){
      balianEl.innerHTML='<tbody><tr><td colspan="3" style="text-align:center;color:var(--txt3);padding:20px">No Balian data for this date</td></tr></tbody>';
    } else {
      balianEl.innerHTML=`<thead><tr><th>Area</th><th>Sales</th><th class="num">Achievement</th></tr></thead><tbody>${balianList.map(r=>`<tr><td style="font-weight:600">${r.area}</td><td style="color:var(--txt2);font-size:.68rem">${r.sales}</td><td class="num" style="font-weight:700;color:${r.ach>0?'var(--org)':'var(--txt3)'}">${r.ach>0?fmtRp(r.ach):'—'}</td></tr>`).join('')}</tbody><tfoot><tr><td colspan="2"><strong>GRAND TOTAL</strong></td><td class="num"><strong style="color:var(--org)">${fmtRp(balianTotal)}</strong></td></tr></tfoot>`;
    }
  }
}

// ── SO tab — with segment donut + clickable customers ────────────
function renderSO(){
  const agg=getAggSummary();
  const isFullDay=isFull(activeDate);
  document.getElementById('so-co-lbl').textContent=company==='ALL'?'All':company;

  const divMapSO={};RAW.so.forEach(r=>{divMapSO[r.sales]=r.division;});
  const rS=Object.entries(agg.rep_rev)
    .filter(([n])=>{if(company==='ALL')return true;const div=divMapSO[n];if(div)return div===(company==='MKU'?'MKU Bali':'MKS Bali');return true;})
    .sort((a,b)=>b[1]-a[1]).slice(0,12);
  if(charts.rep)charts.rep.destroy();
  charts.rep=new Chart(document.getElementById('ch-rep'),{type:'bar',data:{labels:rS.map(([n])=>n),datasets:[{data:rS.map(([,v])=>v),backgroundColor:rS.map((_,i)=>i===0?'#2563eb':i<3?'#93b4f8':'#c7d8fc'),borderRadius:6}]},options:{indexAxis:'y',...COPTS,plugins:{legend:{display:false}},scales:{x:{...COPTS.scales.x,ticks:{...COPTS.scales.x.ticks,callback:v=>v>=1e6?(v/1e6).toFixed(0)+'M':v}},y:{...COPTS.scales.y,grid:{display:false}}}}});

  const pS=Object.entries(agg.prod_rev).sort((a,b)=>b[1]-a[1]).slice(0,10);
  if(charts.prod)charts.prod.destroy();
  charts.prod=new Chart(document.getElementById('ch-prod'),{type:'bar',data:{labels:pS.map(([n])=>n.length>28?n.slice(0,27)+'…':n),datasets:[{data:pS.map(([,v])=>v),backgroundColor:'#6ee7b7',borderRadius:6}]},options:{indexAxis:'y',...COPTS,plugins:{legend:{display:false}},scales:{x:{...COPTS.scales.x,ticks:{...COPTS.scales.x.ticks,callback:v=>v>=1e6?(v/1e6).toFixed(0)+'M':v}},y:{...COPTS.scales.y,grid:{display:false},ticks:{...COPTS.scales.y.ticks,font:{size:10}}}}}});

  // ── Segment donut (Food / Bev / Nestlé from targets) ──────────
  const tgt=getTgt();
  const segEl=document.getElementById('ch-seg');
  if(segEl&&tgt&&tgt.targets){
    const T=tgt.targets;
    const foodA=T.FOOD?.achievement||0,bevA=T.BEVERAGE?.achievement||0,nesA=T.NESTLE?.achievement||0;
    const segTot=foodA+bevA+nesA;
    if(charts.seg)charts.seg.destroy();
    charts.seg=new Chart(segEl,{type:'doughnut',data:{
      labels:['Food','Beverage','Nestlé'],
      datasets:[{data:[foodA,bevA,nesA],backgroundColor:['#2563eb','#059669','#7c3aed'],borderWidth:2,borderColor:'#fff'}]
    },options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{labels:{color:'#8a93b0',font:{family:'Plus Jakarta Sans',size:11},boxWidth:10,padding:14}},
      tooltip:{callbacks:{label:ctx=>`${ctx.label}: ${fmtRp(ctx.raw)} (${segTot>0?Math.round(ctx.raw/segTot*100):0}%)`}}}
    }});
  }

  // ── Top customers — clickable for history ────────────────────
  const cTop=Object.entries(agg.cust).sort((a,b)=>b[1].rev-a[1].rev).slice(0,20);
  document.getElementById('tbl-cust').innerHTML=`<thead><tr><th>#</th><th>Customer</th><th>Sales</th><th class="num">Orders</th><th class="num">Revenue</th></tr></thead>
    <tbody>${cTop.map(([n,v],i)=>`<tr>
      <td style="color:var(--txt3);font-weight:700">${i+1}</td>
      <td style="font-weight:600;cursor:pointer;color:var(--mks)" onclick="showCustHistory(${JSON.stringify(n)},${JSON.stringify(v.sales||'')})">🔍 ${n}</td>
      <td><span class="badge b-gray">${v.sales}</span></td>
      <td class="num">${v.so}</td>
      <td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(v.rev)}</td>
    </tr>`).join('')}</tbody>`;

  document.getElementById('so-count-lbl').textContent=agg.cnt+' orders';
  if(isFullDay){
    const so=getSO();
    document.getElementById('tbl-so').innerHTML=`<thead><tr><th>Date</th><th>No SO</th><th>Co</th><th>Customer</th><th>Sales</th><th>Product</th><th class="num">Qty</th><th class="num">Revenue</th></tr></thead><tbody>${so.map(r=>`<tr><td style="font-size:.63rem;color:var(--txt3);white-space:nowrap">${fmtD(r.date)}</td><td style="font-size:.62rem;color:var(--txt3)">${r.no_so}</td><td><span class="badge ${r.division==='MKU Bali'?'b-mku':'b-mks'}">${r.division==='MKU Bali'?'MKU':'MKS'}</span></td><td style="max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600">${r.customer}</td><td style="color:var(--txt2);font-size:.68rem">${r.sales}</td><td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:.68rem">${r.product}</td><td class="num">${fmtQ(r.so_pcs)} <span style="color:var(--txt3)">${r.unit}</span></td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(r.revenue)}</td></tr>`).join('')}</tbody>`;
  } else {
    document.getElementById('tbl-so').innerHTML=`<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--txt3)">📦 Detailed SO rows available for latest day only.<br><span style="font-size:.68rem">Select <strong>${fmtD(RAW.latest)} ★</strong> to see full order list.</span></td></tr>`;
  }
}

// ── Customer history modal ────────────────────────────────────────
function showCustHistory(custName, salesRep){
  const history=[];
  (RAW.dates||[]).forEach(d=>{
    const s=getSummary(d);
    const c=s.cust&&s.cust[custName];
    if(c)history.push({date:d,rev:c.rev,orders:c.so,sales:c.sales||salesRep});
  });
  // Also check latest day raw SO rows for product breakdown
  const latestRows=RAW.so.filter(r=>r.customer===custName);
  let prodSection='';
  if(latestRows.length){
    const prodMap={};
    latestRows.forEach(r=>{prodMap[r.product]=(prodMap[r.product]||0)+r.revenue;});
    const prodRows=Object.entries(prodMap).sort((a,b)=>b[1]-a[1]).slice(0,8)
      .map(([p,v])=>`<tr><td style="font-size:.68rem">${p.length>38?p.slice(0,37)+'…':p}</td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(v)}</td></tr>`).join('');
    prodSection=`<div style="margin-top:14px"><div style="font-size:.62rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:6px">Latest Day — Products</div><table class="tbl"><thead><tr><th>Product</th><th class="num">Revenue</th></tr></thead><tbody>${prodRows}</tbody></table></div>`;
  }
  const totalRev=history.reduce((s,h)=>s+h.rev,0);
  const totalOrders=history.reduce((s,h)=>s+h.orders,0);
  const histRows=history.sort((a,b)=>b.date.localeCompare(a.date)).slice(0,15)
    .map(h=>`<tr><td style="font-size:.68rem;white-space:nowrap">${fmtD(h.date)}</td><td class="num">${h.orders}</td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(h.rev)}</td></tr>`).join('');

  let modal=document.getElementById('cust-modal');
  if(modal)modal.remove();
  modal=document.createElement('div');
  modal.id='cust-modal';
  modal.style.cssText='position:fixed;inset:0;background:rgba(0,0,0,.5);z-index:9999;display:flex;align-items:center;justify-content:center;padding:20px';
  modal.onclick=e=>{if(e.target===modal)modal.remove();};
  modal.innerHTML=`<div style="background:var(--white);border-radius:16px;max-width:480px;width:100%;max-height:82vh;overflow-y:auto;padding:24px;box-shadow:0 20px 60px rgba(0,0,0,.25)">
    <div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:16px">
      <div><div style="font-size:.95rem;font-weight:800;color:var(--txt)">${custName}</div><div style="font-size:.68rem;color:var(--txt3);margin-top:2px">Sales Rep: ${salesRep||'—'}</div></div>
      <button onclick="document.getElementById('cust-modal').remove()" style="border:none;background:var(--bg);border-radius:8px;padding:6px 12px;cursor:pointer;font-size:.73rem;color:var(--txt2);font-family:inherit">✕ Close</button>
    </div>
    <div style="display:grid;grid-template-columns:1fr 1fr;gap:10px;margin-bottom:16px">
      <div style="background:var(--bg);border-radius:10px;padding:12px"><div style="font-size:.57rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:4px">Total Revenue (this month)</div><div style="font-size:1.05rem;font-weight:800;color:var(--mks)">${fmtRp(totalRev)}</div></div>
      <div style="background:var(--bg);border-radius:10px;padding:12px"><div style="font-size:.57rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:4px">Total Orders</div><div style="font-size:1.05rem;font-weight:800">${totalOrders}</div></div>
    </div>
    <div style="font-size:.62rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:6px">Order History</div>
    <table class="tbl"><thead><tr><th>Date</th><th class="num">Orders</th><th class="num">Revenue</th></tr></thead><tbody>${histRows||'<tr><td colspan="3" style="text-align:center;color:var(--txt3);padding:12px">No history in current month</td></tr>'}</tbody></table>
    ${prodSection}
  </div>`;
  document.body.appendChild(modal);
}

function renderDel(){
  const stats=getDelStats();
  const del=getDel();
  const isFullDay=isFull(activeDate);

  if(stats.tot===0&&!isFullDay){
    document.getElementById('del-kpis').innerHTML=`<div class="kpi-card c-gray" style="grid-column:span 5"><div class="kpi-icon gray">🕐</div><div class="kpi-label">Delivery</div><div class="kpi-value" style="font-size:1rem">Awaiting end-of-day files</div><div class="kpi-sub">Send MKU & MKS delivery reports</div></div>`;
    document.getElementById('tbl-bs').innerHTML='<tr><td colspan="7" style="text-align:center;color:var(--txt3);padding:20px">No delivery data</td></tr>';
    document.getElementById('tbl-del').innerHTML='<tr><td colspan="8" style="text-align:center;color:var(--txt3);padding:20px">No delivery data</td></tr>';
    return;
  }

  document.getElementById('del-kpis').innerHTML=`
    <div class="kpi-card c-grn"><div class="kpi-icon grn">✅</div><div class="kpi-label">Total Deliveries</div><div class="kpi-value">${stats.tot}</div><div class="kpi-sub">Dispatched</div></div>
    <div class="kpi-card c-grn"><div class="kpi-icon grn">📦</div><div class="kpi-label">Fulfilled</div><div class="kpi-value grn">${stats.ful}</div><div class="kpi-sub">${pct(stats.ful,stats.tot)}% rate</div></div>
    <div class="kpi-card ${stats.unf>0?'c-mku':'c-grn'}"><div class="kpi-icon ${stats.unf>0?'mku':'grn'}">🚫</div><div class="kpi-label">Unfulfilled</div><div class="kpi-value ${stats.unf>0?'mku':''}">${stats.unf}</div><div class="kpi-sub">Not delivered</div></div>
    <div class="kpi-card ${(stats.lost_rev||0)>0?'c-mku':'c-gray'}"><div class="kpi-icon ${(stats.lost_rev||0)>0?'mku':'gray'}">💸</div><div class="kpi-label">Revenue at Risk</div><div class="kpi-value ${(stats.lost_rev||0)>0?'mku':''}" style="font-size:1rem">${fmtRp(stats.lost_rev||0)}</div><div class="kpi-sub">${stats.unf} unfulfilled</div></div>
    <div class="kpi-card c-gray"><div class="kpi-icon gray">🏢</div><div class="kpi-label">Areas Served</div><div class="kpi-value">${Object.keys(stats.by_area).length}</div><div class="kpi-sub">Unique areas</div></div>`;

  const aS=Object.entries(stats.by_area).sort((a,b)=>b[1].t-a[1].t);
  if(charts.delArea)charts.delArea.destroy();
  charts.delArea=new Chart(document.getElementById('ch-del-area'),{type:'bar',data:{labels:aS.map(([n])=>n.length>16?n.slice(0,15)+'…':n),datasets:[{label:'Fulfilled',data:aS.map(([,v])=>v.ok),backgroundColor:'#6ee7b7',borderRadius:4,stack:'a'},{label:'Unfulfilled',data:aS.map(([,v])=>v.t-v.ok),backgroundColor:'#fca5a5',borderRadius:4,stack:'a'}]},options:{...COPTS,scales:{...COPTS.scales,x:{...COPTS.scales.x,stacked:true},y:{...COPTS.scales.y,stacked:true}}}});

  const bsI=del.filter(r=>r.ket==='UNFULFILLED').sort((a,b)=>(b.diff||0)-(a.diff||0));
  document.getElementById('del-bs-lbl').textContent=bsI.length+' issues';
  document.getElementById('tbl-bs').innerHTML=`<thead><tr><th>Customer</th><th>Sales</th><th>Product</th><th class="num">Qty</th><th>Status</th><th>Area</th><th>Co</th></tr></thead><tbody>${bsI.length?bsI.map(r=>`<tr><td style="font-weight:600;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${r.customer||'—'}</td><td style="font-size:.68rem;color:var(--txt2)">${r.sales||'—'}</td><td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:.68rem">${r.product||'—'}</td><td class="num">${fmtQ(r.qty_bs||0)} ${r.unit||''}</td><td><span class="badge b-red">UNFULFILLED</span></td><td style="font-size:.68rem">${r.area||'—'}</td><td><span class="badge ${r.co==='MKU'?'b-mku':'b-mks'}">${r.co||'—'}</span></td></tr>`).join(''):'<tr><td colspan="7" style="text-align:center;color:var(--txt3);padding:12px">✅ No issues</td></tr>'}</tbody>`;

  document.getElementById('del-count-lbl').textContent=stats.tot+' lines';
  if(isFullDay){
    document.getElementById('tbl-del').innerHTML=`<thead><tr><th>Co</th><th>Area</th><th>Customer</th><th>Sales</th><th>Product</th><th class="num">Qty</th><th>Unit</th><th>Status</th></tr></thead><tbody>${del.map(r=>`<tr><td><span class="badge ${r.co==='MKU'?'b-mku':'b-mks'}">${r.co}</span></td><td style="font-size:.68rem;color:var(--txt2);white-space:nowrap">${r.area||''}</td><td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600">${r.customer||''}</td><td style="font-size:.68rem;color:var(--txt2)">${r.sales||''}</td><td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:.68rem">${r.product||''}</td><td class="num">${fmtQ(r.qty_bs||0)}</td><td style="font-size:.68rem">${r.unit||''}</td><td><span class="badge ${r.ket==='FULFILLED'?'b-grn':'b-red'}">${r.ket==='FULFILLED'?'✓':'✗'}</span></td></tr>`).join('')}</tbody>`;
  } else {
    document.getElementById('tbl-del').innerHTML=`<tr><td colspan="8" style="text-align:center;padding:20px;color:var(--txt3)">🚚 Full delivery list available for latest day only.<br><span style="font-size:.68rem">Select <strong>${fmtD(RAW.latest)} ★</strong> to see all lines.</span></td></tr>`;
  }
}

// ── Reps — with biggest customer + dropped-off ───────────────────
function renderReps(){
  const agg=getAggSummary();
  document.getElementById('reps-lbl').textContent=(company==='ALL'?'All':company)+(activeDate==='ALL'?' · All days':' · '+fmtD(activeDate));
  const reps=Object.entries(agg.rep_rev).sort((a,b)=>b[1]-a[1]);
  const max=reps[0]?.[1]||1;
  const divMap={};RAW.so.forEach(r=>{divMap[r.sales]=r.division;});

  // Build dropped-off: customers seen in previous dates but not today
  const todayCusts=new Set(RAW.so.map(r=>r.customer));
  const prevCustsByRep={}; // rep -> {custName -> {lastSeen, rev}}
  (RAW.dates||[]).filter(d=>d!==RAW.latest).forEach(d=>{
    const s=getSummary(d);
    Object.entries(s.cust||{}).forEach(([c,v])=>{
      if(!prevCustsByRep[v.sales])prevCustsByRep[v.sales]={};
      if(!prevCustsByRep[v.sales][c]||prevCustsByRep[v.sales][c].lastSeen<d)
        prevCustsByRep[v.sales][c]={lastSeen:d,rev:v.rev};
    });
  });

  document.getElementById('tbl-reps').innerHTML=`
    <thead><tr><th>#</th><th>Rep</th><th>Div</th><th class="num">Revenue</th><th class="num">Orders</th><th class="num">Custs</th><th>Biggest Customer</th><th>⚠ Dropped Off</th><th style="width:80px">vs Top</th></tr></thead>
    <tbody>${reps.map(([n,rev],i)=>{
      const div=divMap[n]||'—';
      let orders=0;const custRevMap={};
      (activeDate==='ALL'?RAW.dates:[activeDate]).forEach(d=>{
        const s=getSummary(d);
        Object.entries(s.cust||{}).forEach(([c,v])=>{
          if(v.sales===n){orders+=v.so;custRevMap[c]=(custRevMap[c]||0)+v.rev;}
        });
      });
      const custList=Object.entries(custRevMap).sort((a,b)=>b[1]-a[1]);
      const biggest=custList[0];
      const droppedList=Object.entries(prevCustsByRep[n]||{})
        .filter(([c])=>!todayCusts.has(c))
        .sort((a,b)=>b[1].rev-a[1].rev);
      const dropped=droppedList[0];
      return`<tr>
        <td style="font-weight:700">${i===0?'🥇':i===1?'🥈':i===2?'🥉':i+1}</td>
        <td style="font-weight:700;color:${i===0?'var(--mks)':'var(--txt)'}">${n}</td>
        <td><span class="badge ${div==='MKU Bali'?'b-mku':'b-mks'}">${div==='MKU Bali'?'MKU':div==='MKS Bali'?'MKS':'—'}</span></td>
        <td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(rev)}</td>
        <td class="num">${orders||'—'}</td>
        <td class="num">${custList.length||'—'}</td>
        <td style="font-size:.65rem;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${biggest?`<span style="font-weight:600">${biggest[0]}</span><br><span style="color:var(--txt3)">${fmtRp(biggest[1])}</span>`:'—'}</td>
        <td style="font-size:.65rem;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${dropped?`<span style="color:var(--mku);font-weight:600">⚠ ${dropped[0]}</span><br><span style="color:var(--txt3);font-size:.58rem">last ${fmtD(dropped[1].lastSeen)}</span>`:'<span style="color:var(--grn);font-size:.63rem">✓ all active</span>'}</td>
        <td><div class="pb"><div class="pb-fill" style="width:${Math.round(rev/max*100)}%;background:${i===0?'var(--mks)':i<3?'#93b4f8':'#c7d8fc'}"></div></div></td>
      </tr>`;
    }).join('')}</tbody>`;
}

function renderStock(f){
  if(f)stockFilter=f;
  const stk=getStk();
  const stkSum=getStkSummary();
  const isFullDay=isFull(activeDate);
  const dl=activeDate==='ALL'?'Latest: '+fmtD(RAW.latest):fmtD(activeDate);

  let totalSKU=stk.length,outCnt=0,critCnt=0,lowCnt=0;
  if(stkSum&&!isFullDay){
    totalSKU=stkSum.mku_total+stkSum.mks_total;
    outCnt=stkSum.mku_out+stkSum.mks_out;
    critCnt=stkSum.mku_crit+stkSum.mks_crit;
    lowCnt=stkSum.mku_low+stkSum.mks_low;
  } else {
    outCnt=stk.filter(s=>s.st==='out').length;
    critCnt=stk.filter(s=>s.st==='critical').length;
    lowCnt=stk.filter(s=>s.st==='low').length;
    totalSKU=stk.length;
  }

  document.getElementById('stk-kpis').innerHTML=`
    <div class="kpi-card c-gray"><div class="kpi-icon gray">📦</div><div class="kpi-label">Active SKUs</div><div class="kpi-value">${totalSKU}</div><div class="kpi-sub">${dl}</div></div>
    <div class="kpi-card c-mku"><div class="kpi-icon mku">🔴</div><div class="kpi-label">Out of Stock</div><div class="kpi-value mku">${outCnt}</div><div class="kpi-sub">Zero inventory</div></div>
    <div class="kpi-card c-org"><div class="kpi-icon org">⚠️</div><div class="kpi-label">Critical &lt;3 days</div><div class="kpi-value org">${critCnt}</div><div class="kpi-sub">Urgent reorder</div></div>
    <div class="kpi-card c-org"><div class="kpi-icon org">🟡</div><div class="kpi-label">Low 3–7 days</div><div class="kpi-value org">${lowCnt}</div><div class="kpi-sub">Plan reorder</div></div>`;

  const okCnt=totalSKU-outCnt-critCnt-lowCnt;
  document.getElementById('stk-pills').innerHTML=[{f:'all',l:'All'},{f:'out',l:'🔴 Out ('+outCnt+')'},{f:'critical',l:'Critical ('+critCnt+')'},{f:'low',l:'Low ('+lowCnt+')'},{f:'ok',l:'OK ('+okCnt+')'}].map(({f:fl,l})=>`<button class="pill ${stockFilter===fl?'act':''}" onclick="renderStock('${fl}')">${l}</button>`).join('');

  if(!isFullDay&&stk.length===0&&stockFilter==='ok'){
    document.getElementById('sg').innerHTML=`<p style="color:var(--txt3);padding:20px;font-size:.75rem;grid-column:1/-1">✅ All items OK for this day — no alerts recorded.</p>`;
    return;
  }
  if(!isFullDay&&stockFilter==='ok'){
    document.getElementById('sg').innerHTML=`<p style="color:var(--txt3);padding:20px;font-size:.75rem;grid-column:1/-1">✅ OK items not stored for historical days. Select <strong>${fmtD(RAW.latest)} ★</strong> to browse all SKUs.</p>`;
    return;
  }

  let filtered=stk;
  if(stockFilter!=='all')filtered=stk.filter(s=>s.st===stockFilter);
  filtered.sort((a,b)=>({'out':0,'critical':1,'low':2,'ok':3}[a.st]-{'out':0,'critical':1,'low':2,'ok':3}[b.st]));
  document.getElementById('sg').innerHTML=filtered.map(s=>`<div class="si ${s.st}"><div class="si-code">${s.code||s.c||''}${company==='ALL'?' · <b>'+s.co+'</b>':''}</div><div class="si-name">${s.name||s.n||''}</div><div class="si-bottom"><div class="si-qty ${s.st}">${(s.saldo||s.s||0)<=0?'0':fmtQ(s.saldo||s.s||0)}<span style="font-size:.6rem;font-weight:400;margin-left:2px">${s.unit||s.u||''}</span></div><div class="si-days ${s.st}">${(s.saldo||s.s||0)<=0?'OUT':(s.buf||s.bf||0)>0?fmtQ(s.buf||s.bf||0)+'d':'—'}</div></div></div>`).join('')||'<p style="color:var(--txt3);padding:20px;font-size:.75rem">No items.</p>';
}

function renderAlerts(){
  const stk=getStk();
  const stkSum=getStkSummary();
  const stats=getDelStats();
  const isFullDay=isFull(activeDate);

  const outI=stk.filter(s=>s.st==='out').sort((a,b)=>b.a-a.a);
  const critI=stk.filter(s=>s.st==='critical').sort((a,b)=>a.bf-b.bf);
  const lowI=stk.filter(s=>s.st==='low').sort((a,b)=>a.bf-b.bf);

  let outCnt=outI.length,critCnt=critI.length+lowI.length;
  if(stkSum&&!isFullDay){outCnt=stkSum.mku_out+stkSum.mks_out;critCnt=stkSum.mku_crit+stkSum.mks_crit+stkSum.mku_low+stkSum.mks_low;}

  document.getElementById('alerts-summary').innerHTML=`
    <div style="background:var(--mku-l);border:1px solid #fca5a5;border-radius:10px;padding:14px 16px;display:flex;align-items:center;gap:12px"><div style="font-size:1.4rem">🔴</div><div><div style="font-size:.63rem;font-weight:700;color:var(--mku);text-transform:uppercase;margin-bottom:3px">Out of Stock</div><div style="font-size:1.5rem;font-weight:800;color:var(--mku)">${outCnt}</div><div style="font-size:.63rem;color:var(--mku);opacity:.7">active SKUs at zero</div></div></div>
    <div style="background:var(--org-l);border:1px solid #fcd34d;border-radius:10px;padding:14px 16px;display:flex;align-items:center;gap:12px"><div style="font-size:1.4rem">⚠️</div><div><div style="font-size:.63rem;font-weight:700;color:var(--org);text-transform:uppercase;margin-bottom:3px">Critical / Low</div><div style="font-size:1.5rem;font-weight:800;color:var(--org)">${critCnt}</div></div></div>
    <div style="background:${stats.unf>0?'var(--org-l)':'var(--grn-l)'};border:1px solid ${stats.unf>0?'#fcd34d':'#6ee7b7'};border-radius:10px;padding:14px 16px;display:flex;align-items:center;gap:12px"><div style="font-size:1.4rem">${stats.unf>0?'🚚':'✅'}</div><div><div style="font-size:.63rem;font-weight:700;color:${stats.unf>0?'var(--org)':'var(--grn)'};text-transform:uppercase;margin-bottom:3px">Unfulfilled</div><div style="font-size:1.5rem;font-weight:800;color:${stats.unf>0?'var(--org)':'var(--grn)'}">${stats.unf}</div></div></div>`;

  const unfI=[];
  (activeDate==='ALL'?RAW.dates:[activeDate]).forEach(d=>{
    const dd=RAW.delivery_by_date[d];if(!dd)return;
    if(isLatest(d)){[...(dd.mku_full||[]),...(dd.mks_full||[])].filter(r=>r.ket==='UNFULFILLED').forEach(r=>unfI.push({...r,date:d}));}
    else{(dd.issues||[]).forEach(r=>unfI.push({...r,date:d}));}
  });

  const secs=[
    {id:'a-out',ic:'🔴',tt:'Out of Stock — Active SKUs at Zero',cnt:outI.length,cc:outI.length?'red':'grn',items:outI.length?outI.map(s=>`<div class="al out"><span>🔴</span><div class="al-body"><strong>${s.name||s.n||''}</strong><br><span style="font-size:.68rem;color:var(--txt2)">${s.code||s.c||''} · Avg ${((s.avg3m||s.a)||0).toFixed(0)} ${s.unit||s.u||''}/mo</span></div><span class="al-co ${s.co.toLowerCase()}">${s.co}</span></div>`):['<p style="color:var(--txt3);font-size:.74rem;padding:4px 0">✅ No out-of-stock items</p>']},
    {id:'a-crit',ic:'🚨',tt:'Critical — Less than 3 Days Left',cnt:critI.length,cc:critI.length?'red':'grn',items:critI.length?critI.map(s=>`<div class="al out"><span>🚨</span><div class="al-body"><strong>${s.name||s.n||''}</strong><br><span style="font-size:.68rem;color:var(--mku);font-weight:600">${fmtQ(s.saldo||s.s||0)} ${s.unit||s.u||''} · ${(s.buf||s.bf||0)>0?(s.buf||s.bf||0).toFixed(1)+' days':'<1 day'}</span></div><span class="al-co ${s.co.toLowerCase()}">${s.co}</span></div>`):['<p style="color:var(--txt3);font-size:.74rem;padding:4px 0">✅ No critical items</p>']},
    {id:'a-low',ic:'⚠️',tt:'Low Stock — 3 to 7 Days Left',cnt:lowI.length,cc:lowI.length?'org':'grn',items:lowI.length?lowI.map(s=>`<div class="al warn"><span>⚠️</span><div class="al-body"><strong>${s.name||s.n||''}</strong><br><span style="font-size:.68rem;color:var(--org);font-weight:600">${fmtQ(s.saldo||s.s||0)} ${s.unit||s.u||''} · ${(s.buf||s.bf||0).toFixed(1)} days</span></div><span class="al-co ${s.co.toLowerCase()}">${s.co}</span></div>`):['<p style="color:var(--txt3);font-size:.74rem;padding:4px 0">✅ No low-stock items</p>']},
    {id:'a-unf',ic:'🚫',tt:'Unfulfilled Deliveries — Not Sent',cnt:unfI.length,cc:unfI.length?'red':'grn',items:unfI.length?unfI.map(r=>`<div class="al out"><span>🚫</span><div class="al-body"><strong>${r.customer||'—'}</strong><br><span style="font-size:.68rem;color:var(--txt2)">${r.product||'—'} · <span style="color:var(--mku);font-weight:700">NOT DELIVERED</span></span></div><span class="al-co ${(r.co||'mks').toLowerCase()}">${r.co||'—'}</span></div>`):['<p style="color:var(--txt3);font-size:.74rem;padding:4px 0">✅ All orders sent</p>']},
  ];
  document.getElementById('alerts-accordions').innerHTML=secs.map(s=>`<div class="accord" id="${s.id}"><div class="accord-hdr" onclick="tog('${s.id}')"><div class="accord-icon">${s.ic}</div><div class="accord-title">${s.tt}</div><span class="accord-count ${s.cc}">${s.cnt}</span><div class="accord-chev">▼</div></div><div class="accord-body"><div class="accord-inner">${s.items.join('')}</div></div></div>`).join('');
}

function tog(id){document.getElementById(id).classList.toggle('open');}
function toggleDL(){const isOpen=document.querySelector('.dl-wrap.open')!==null;document.querySelectorAll('.dl-wrap,.date-dd-wrap').forEach(w=>w.classList.remove('open'));if(!isOpen)document.querySelectorAll('.dl-wrap').forEach(w=>w.classList.add('open'));}
document.addEventListener('click',e=>{if(!e.target.closest('.dl-wrap')&&!e.target.closest('.date-dd-wrap')){document.querySelectorAll('.dl-wrap').forEach(w=>w.classList.remove('open'));document.querySelectorAll('.date-dd-wrap').forEach(w=>w.classList.remove('open'));}});

function dlExcel(){
  if(typeof XLSX==='undefined'){alert('Excel library not loaded. Please refresh the page.');return;}
  const mon=RAW.month||'April 2026';
  const wb=XLSX.utils.book_new();
  const addSheet=(name,headers,rows)=>{
    const ws=XLSX.utils.aoa_to_sheet([headers,...rows]);
    const cols=headers.map((h,ci)=>({wch:Math.min(Math.max(h.length,...rows.map(r=>String(r[ci]||'').length))+2,45)}));
    ws['!cols']=cols;ws['!freeze']={xSplit:0,ySplit:1};
    XLSX.utils.book_append_sheet(wb,ws,name);
  };
  const soRows=getSO();
  addSheet('Sales Orders',['Date','No SO','Division','Customer','Sales Rep','Product','SO Qty','Unit','FJ Qty','Revenue (Rp)','Type','Status'],soRows.map(r=>[r.date,r.no_so,r.division,r.customer,r.sales,r.product,r.so_pcs,r.unit,r.fj_pcs,Math.round(r.revenue),r.type,r.status]));
  const delRows=getDel();
  addSheet('Delivery',['Date','Division','Area','Customer','Sales Rep','Product','Qty','Unit','Status'],delRows.map(r=>[r.date||RAW.latest,r.co||'',r.area||'',r.customer||'',r.sales||'',r.product||'',r.qty_bs||0,r.unit||'',r.ket||'']));
  const stk=getStk();
  addSheet('Stock',['Division','Code','Product','Unit','Stock Qty','Avg/Month','Buffer Days','Status'],stk.map(s=>[s.co,s.code||s.c||'',s.name||s.n||'',s.unit||s.u||'',s.saldo||s.s||0,Math.round(s.avg3m||s.a||0),(s.buf||s.bf||0)>0?parseFloat((s.buf||s.bf||0).toFixed(1)):0,s.st.toUpperCase()]));
  XLSX.writeFile(wb,'MKU_MKS_Data_'+mon.replace(' ','_')+'.xlsx');
  document.querySelectorAll('.dl-wrap').forEach(w=>w.classList.remove('open'));
}

function dlPDF(){
  const mon=RAW.month||'April 2026';
  const agg=getAggSummary();
  const{targets:T,area_targets:areas,nestle_areas:nestleA}=getTgt();
  const tot_t=Object.values(T).reduce((s,t)=>s+t.target,0);
  const tot_a=Object.values(T).reduce((s,t)=>s+t.achievement,0);
  const tp=pct(tot_a,tot_t);
  const lastDate=RAW.latest;
  const dayNum=parseInt(lastDate.split('-')[2]);
  const daysInMonth=new Date(parseInt(lastDate.split('-')[0]),parseInt(lastDate.split('-')[1]),0).getDate();
  const timePct=Math.round(dayNum/daysInMonth*100);
  const top5=Object.entries(agg.rep_rev).sort((a,b)=>b[1]-a[1]).slice(0,5);
  const dateLabel=activeDate==='ALL'?'All Days':fmtD(activeDate);
  const colP=p=>p>=timePct?'#059669':p>=(timePct*0.75)?'#d97706':'#dc2626';
  const divMapPdf={};RAW.so.forEach(r=>{divMapPdf[r.sales]=r.division;});
  const htmlStr=`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>${mon} Report</title>
<link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>*{box-sizing:border-box;margin:0;padding:0;}body{font-family:'Plus Jakarta Sans',sans-serif;padding:28px 32px;font-size:11px;color:#1a2035;background:#fff;}.hdr{display:flex;justify-content:space-between;align-items:flex-end;margin-bottom:18px;padding-bottom:14px;border-bottom:3px solid #1a2035;}.ht{font-size:1.4rem;font-weight:800;}.mku{color:#dc2626;}.mks{color:#2563eb;}.badge-date{background:#eff4ff;color:#2563eb;font-size:.6rem;font-weight:700;padding:3px 8px;border-radius:4px;margin-top:6px;display:inline-block;}.section-title{font-size:.7rem;font-weight:800;text-transform:uppercase;letter-spacing:.06em;color:#8a93b0;margin:14px 0 7px;padding-bottom:5px;border-bottom:1px solid #e4e8ef;}.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:4px;}.kpi{border:1px solid #e4e8ef;border-radius:8px;padding:10px 12px;border-left:3px solid;}.kl{font-size:.55rem;font-weight:700;color:#8a93b0;text-transform:uppercase;letter-spacing:.05em;margin-bottom:3px;}.kv{font-size:1.05rem;font-weight:800;}.tgt-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:4px;}.tgt{border:1px solid #e4e8ef;border-radius:8px;padding:10px 12px;}.tn{font-size:.63rem;font-weight:700;margin-bottom:4px;}.tp{font-size:1.1rem;font-weight:800;margin-bottom:2px;}.pb{background:#e4e8ef;border-radius:99px;height:5px;overflow:hidden;margin-bottom:3px;}.pbf{height:5px;border-radius:99px;}.psub{font-size:.57rem;color:#8a93b0;}.grand{background:linear-gradient(135deg,#eff4ff,#dce8ff);border:1px solid #c7d8fc;border-radius:8px;padding:12px 16px;margin-bottom:10px;display:flex;align-items:center;justify-content:space-between;}.grand-pct{font-size:2rem;font-weight:800;color:#1a2035;}table{width:100%;border-collapse:collapse;font-size:.67rem;margin-bottom:10px;}th{background:#f4f6f9;padding:6px 8px;text-align:left;font-size:.55rem;font-weight:700;color:#8a93b0;text-transform:uppercase;border-bottom:1px solid #e4e8ef;}td{padding:5px 8px;border-bottom:1px solid #f4f6f9;vertical-align:middle;}td.r{text-align:right;}.pbar{background:#e4e8ef;border-radius:99px;height:4px;width:70px;display:inline-block;vertical-align:middle;overflow:hidden;}.pbar-f{height:4px;border-radius:99px;}.badge{display:inline-block;font-size:.55rem;font-weight:700;padding:2px 6px;border-radius:3px;}tfoot td{font-weight:700;background:#f8f9fd;border-top:2px solid #e4e8ef;}.ftr{margin-top:16px;padding-top:10px;border-top:1px solid #e4e8ef;display:flex;justify-content:space-between;font-size:.57rem;color:#8a93b0;}@media print{body{padding:14px 18px;}@page{margin:1cm;size:A4;}}</style></head><body>
<div class="hdr"><div><div class="ht"><span class="mku">MKU</span> &amp; <span class="mks">MKS</span> — ${mon} Report</div><div class="badge-date">📅 ${dateLabel} · Generated ${new Date().toLocaleDateString('id-ID')}</div></div><div style="font-size:.63rem;color:#8a93b0;text-align:right">Area Manager Dashboard<br><strong style="color:#1a2035">Confidential</strong></div></div>
<div class="section-title">📊 Key Performance Indicators</div>
<div class="kpis">
  <div class="kpi" style="border-left-color:#2563eb"><div class="kl">Total Revenue</div><div class="kv" style="color:#2563eb">${fmtRp(agg.rev)}</div><div style="font-size:.58rem;color:#8a93b0;margin-top:2px">${agg.cnt} orders · ${agg.cust_cnt} customers</div></div>
  <div class="kpi" style="border-left-color:#059669"><div class="kl">Monthly Target</div><div class="kv" style="color:${colP(tp)}">${tp}%</div><div style="font-size:.58rem;color:#8a93b0;margin-top:2px">${fmtRp(tot_a)} / ${fmtRp(tot_t)}</div></div>
  <div class="kpi" style="border-left-color:#7c3aed"><div class="kl">Nestlé Target</div><div class="kv" style="color:${colP(pct(T.NESTLE?.achievement||0,T.NESTLE?.target||1))}">${pct(T.NESTLE?.achievement||0,T.NESTLE?.target||1)}%</div><div style="font-size:.58rem;color:#8a93b0;margin-top:2px">${fmtRp(T.NESTLE?.achievement||0)} / ${fmtRp(T.NESTLE?.target||0)}</div></div>
  <div class="kpi" style="border-left-color:#d97706"><div class="kl">Time Elapsed</div><div class="kv" style="color:#d97706">${timePct}%</div><div style="font-size:.58rem;color:#8a93b0;margin-top:2px">Day ${dayNum} of ${daysInMonth} · on-track ≥${timePct}%</div></div>
</div>
<div class="section-title">🎯 Target vs Achievement</div>
<div class="grand"><div><div style="font-size:.6rem;font-weight:700;color:#2563eb;text-transform:uppercase;margin-bottom:4px">🎯 Grand Total (Food + Bev + Nestlé)</div><div style="font-size:.68rem;color:#4a5472">${fmtRp(tot_a)} achieved of ${fmtRp(tot_t)} target</div></div><div class="grand-pct">${tp}%</div></div>
<div class="tgt-grid">${Object.entries(T).map(([c,t])=>{const p=pct(t.achievement,t.target),col=colP(p);return`<div class="tgt"><div class="tn">${{FOOD:'🍽️ FOOD',BEVERAGE:'🥤 BEVERAGE',NESTLE:'☕ NESTLÉ'}[c]||c}</div><div class="tp" style="color:${col}">${p}%</div><div class="pb"><div class="pbf" style="width:${Math.min(p,100)}%;background:${col}"></div></div><div class="psub">${fmtRp(t.achievement)} / ${fmtRp(t.target)}</div></div>`;}).join('')}</div>
<div class="section-title">📍 Area Performance Detail</div>
<table><thead><tr><th>Area</th><th>Sales Rep</th><th class="r">Food Ach</th><th class="r">Bev Ach</th><th class="r">Target</th><th class="r">Achieved</th><th>Progress (≥${timePct}%)</th></tr></thead>
<tbody>${(areas||[]).map(a=>{const p=a.pct,col=colP(p);return`<tr><td style="font-weight:600;font-size:.65rem">${a.area}</td><td style="color:#8a93b0;font-size:.61rem">${a.sales}</td><td class="r">${fmtRp(a.food_ach)}</td><td class="r">${fmtRp(a.bev_ach)}</td><td class="r" style="color:#8a93b0">${fmtRp(a.food_target+a.bev_target)}</td><td class="r" style="font-weight:700">${fmtRp(a.food_ach+a.bev_ach)}</td><td><div style="display:flex;align-items:center;gap:5px"><div class="pbar"><div class="pbar-f" style="width:${Math.min(p,100)}%;background:${col}"></div></div><span style="font-weight:700;color:${col}">${p}%</span></div></td></tr>`;}).join('')}</tbody>
<tfoot><tr><td colspan="2">GRAND TOTAL</td><td class="r" style="color:#2563eb">${fmtRp((areas||[]).reduce((s,a)=>s+a.food_ach,0))}</td><td class="r" style="color:#059669">${fmtRp((areas||[]).reduce((s,a)=>s+a.bev_ach,0))}</td><td class="r">${fmtRp((areas||[]).reduce((s,a)=>s+a.food_target+a.bev_target,0))}</td><td class="r">${fmtRp((areas||[]).reduce((s,a)=>s+a.food_ach+a.bev_ach,0))}</td><td><span style="font-weight:700;color:${colP(tp)}">${tp}%</span></td></tr></tfoot></table>
<div class="section-title">☕ Nestlé Channel Detail</div>
<table><thead><tr><th>Channel</th><th>Sales Rep</th><th class="r">Target</th><th class="r">Achievement</th><th>Progress</th></tr></thead>
<tbody>${(nestleA||[]).map(n=>{const p=pct(n.achievement,n.target),col=colP(p);return`<tr><td style="font-weight:600">${n.area}</td><td style="color:#8a93b0;font-size:.61rem">${n.sales||'—'}</td><td class="r" style="color:#8a93b0">${fmtRp(n.target)}</td><td class="r" style="font-weight:700">${fmtRp(n.achievement)}</td><td><div style="display:flex;align-items:center;gap:5px"><div class="pbar"><div class="pbar-f" style="width:${Math.min(p,100)}%;background:${col}"></div></div><span style="font-weight:700;color:${col}">${p}%</span></div></td></tr>`;}).join('')}</tbody>
<tfoot><tr><td colspan="2">GRAND TOTAL</td><td class="r">${fmtRp((nestleA||[]).reduce((s,n)=>s+n.target,0))}</td><td class="r">${fmtRp((nestleA||[]).reduce((s,n)=>s+n.achievement,0))}</td><td><span style="font-weight:700;color:${colP(pct((nestleA||[]).reduce((s,n)=>s+n.achievement,0),(nestleA||[]).reduce((s,n)=>s+n.target,0)))}">${pct((nestleA||[]).reduce((s,n)=>s+n.achievement,0),(nestleA||[]).reduce((s,n)=>s+n.target,0))}%</span></td></tr></tfoot></table>
<div class="section-title">👥 Top Sales Reps</div>
<table><thead><tr><th>#</th><th>Rep</th><th>Division</th><th class="r">Revenue</th></tr></thead>
<tbody>${top5.map(([n,rv],i)=>{const div=divMapPdf[n]||'—';return`<tr><td style="font-weight:800;color:#8a93b0">${i===0?'🥇':i===1?'🥈':i===2?'🥉':i+1}</td><td style="font-weight:700">${n}</td><td><span class="badge" style="background:${div==='MKU Bali'?'#fef2f2':'#eff4ff'};color:${div==='MKU Bali'?'#dc2626':'#2563eb'}">${div==='MKU Bali'?'MKU':div==='MKS Bali'?'MKS':'—'}</span></td><td class="r" style="font-weight:700;color:#2563eb">${fmtRp(rv)}</td></tr>`;}).join('')}</tbody></table>
<div class="ftr"><span>MKU &amp; MKS Area Dashboard</span><span>${mon} · ${dateLabel}</span><span>Internal Use Only · Confidential</span></div>
</body></html>`;
  const w=window.open('','_blank');
  if(!w){alert('Please allow popups for this site to open the print report.');return;}
  w.document.write(htmlStr);w.document.close();
  setTimeout(()=>w.print(),800);
  document.querySelectorAll('.dl-wrap').forEach(w=>w.classList.remove('open'));
}

// ── customers.js loader — fixed double-load crash ─────────────────
let _custLoaded=false;
function loadCustomers(cb){
  if(_custLoaded&&window.CUSTOMERS){cb();return;}
  if(document._loadingCust){document._loadingCust.push(cb);return;}
  document._loadingCust=[cb];
  const s=document.createElement('script');
  s.src='customers.js?v=1';
  s.onload=function(){_custLoaded=true;(document._loadingCust||[]).forEach(f=>f());document._loadingCust=null;};
  s.onerror=function(){console.warn('customers.js not found');(document._loadingCust||[]).forEach(f=>f());document._loadingCust=null;};
  document.body.appendChild(s);
}

// ── Month-on-Month run rate ───────────────────────────────────────
// FIX: April & May showing same Rp 5B — now reads actual SO revenue
// per month from so_summary, not from targets achievement
function renderMoM(){
  const momEl=document.getElementById('mom-trend');
  if(!momEl||typeof RAW.months==='undefined')return;
  const monthKeys=Object.keys(RAW.months).sort();
  const curKey=typeof _mk!=='undefined'?_mk:monthKeys[monthKeys.length-1];
  const curIdx=monthKeys.indexOf(curKey);
  if(curIdx<1){momEl.innerHTML='';return;}
  const prevKey=monthKeys[curIdx-1];
  const curMo=RAW.months[curKey]||{},prevMo=RAW.months[prevKey]||{};
  const curDates=curMo.dates||[],prevDates=prevMo.dates||[];

  // Days elapsed = last date in each month
  const curDN=curDates.length?parseInt(curDates[curDates.length-1].split('-')[2]):1;
  const prevDN=prevDates.length?parseInt(prevDates[prevDates.length-1].split('-')[2]):1;
  // Total days in each month (for projection)
  const curDIM=new Date(parseInt(curKey.split('-')[0]),parseInt(curKey.split('-')[1]),0).getDate();
  const prevDIM=new Date(parseInt(prevKey.split('-')[0]),parseInt(prevKey.split('-')[1]),0).getDate();

  // ── FIX: sum SO revenue from so_summary, not targets achievement ─
  let curRev=0,prevRev=0;
  Object.values(curMo.so_summary||{}).forEach(s=>curRev+=s.rev||0);
  Object.values(prevMo.so_summary||{}).forEach(s=>prevRev+=s.rev||0);

  // Run rate = revenue per day elapsed (not per total days)
  const curRate=curDN>0?curRev/curDN:0;
  const prevRate=prevDN>0?prevRev/prevDN:0;
  const rateChg=prevRate>0?Math.round((curRate-prevRate)/prevRate*100):0;
  const col=rateChg>=0?'var(--grn)':'var(--mku)';

  momEl.innerHTML=`<div class="card" style="margin-bottom:14px">
    <div class="card-hdr"><div class="card-title"><div class="ci mks">📈</div>Month-on-Month Run Rate</div><span class="card-sub">${prevMo.label||prevKey} → ${curMo.label||curKey}</span></div>
    <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px">
      <div style="text-align:center;padding:12px;background:var(--bg);border-radius:10px">
        <div style="font-size:.6rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:6px">${prevMo.label||prevKey}</div>
        <div style="font-size:1rem;font-weight:800">${fmtRp(prevRev)}</div>
        <div style="font-size:.63rem;color:var(--txt3);margin-top:3px">${fmtRp(Math.round(prevRate))}/day · ${prevDN} of ${prevDIM} days</div>
      </div>
      <div style="text-align:center;padding:12px;background:var(--bg);border-radius:10px">
        <div style="font-size:.6rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:6px">${curMo.label||curKey} (${curDN} days)</div>
        <div style="font-size:1rem;font-weight:800">${fmtRp(curRev)}</div>
        <div style="font-size:.63rem;color:var(--txt3);margin-top:3px">${fmtRp(Math.round(curRate))}/day · ${curDN} of ${curDIM} days</div>
      </div>
      <div style="text-align:center;padding:12px;background:var(--bg);border-radius:10px">
        <div style="font-size:.6rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:6px">Run Rate Change</div>
        <div style="font-size:1.4rem;font-weight:800;color:${col}">${rateChg>=0?'▲':'▼'} ${Math.abs(rateChg)}%</div>
        <div style="font-size:.63rem;color:var(--txt3);margin-top:3px">${fmtRp(Math.round(curRate))}/day vs ${fmtRp(Math.round(prevRate))}/day</div>
      </div>
      <div style="text-align:center;padding:12px;background:var(--mks-l);border-radius:10px;border:1px solid #c7d8fc">
        <div style="font-size:.6rem;font-weight:700;color:var(--mks);text-transform:uppercase;margin-bottom:6px">Projected Month-End</div>
        <div style="font-size:1rem;font-weight:800;color:var(--mks)">${fmtRp(Math.round(curRate*curDIM))}</div>
        <div style="font-size:.63rem;color:var(--txt3);margin-top:3px">At current pace · ${curDIM} days</div>
      </div>
    </div>
  </div>`;
}

// ── Business tab ──────────────────────────────────────────────────
function renderBusiness(){
  loadCustomers(function(){
    const el1=document.getElementById('biz-area');
    if(el1&&window.CUSTOMERS){
      const areas=window.CUSTOMERS.areas||{};
      const months=window.CUSTOMERS.months||[];
      const rows=Object.values(areas).sort((a,b)=>b.total-a.total);
      const cols=months.slice(-3);
      el1.innerHTML=`<div class="card"><div class="card-hdr"><div class="card-title"><div class="ci grn">📍</div>Area Performance — Monthly Revenue</div></div><div class="tbl-wrap"><table class="tbl"><thead><tr><th>Area</th><th>Div</th>${cols.map(m=>`<th class="num">${m.slice(0,3)}</th>`).join('')}<th class="num">Total</th><th>Trend</th></tr></thead><tbody>${rows.map(a=>{const vals=cols.map(m=>a.monthly[m]||0);const last=vals[vals.length-1],prev=vals[vals.length-2]||0;const trend=prev>0?Math.round((last-prev)/prev*100):0;const arrow=trend>0?'<span style="color:var(--grn)">▲'+trend+'%</span>':trend<0?'<span style="color:var(--mku)">▼'+Math.abs(trend)+'%</span>':'—';return`<tr><td style="font-weight:600;font-size:.7rem">${a.name}</td><td style="font-size:.63rem;color:var(--txt3)">${(a.division||'').replace(' Bali','')}</td>${vals.map(v=>`<td class="num">${fmtRp(v)}</td>`).join('')}<td class="num" style="font-weight:700">${fmtRp(a.total)}</td><td>${arrow}</td></tr>`;}).join('')}</tbody></table></div></div>`;
    }
    const el2=document.getElementById('biz-seg');
    if(el2&&window.CUSTOMERS){
      const segs=window.CUSTOMERS.segments||{};
      const rows=Object.entries(segs).sort((a,b)=>b[1].total-a[1].total);
      const tot=rows.reduce((s,[,v])=>s+v.total,0);
      el2.innerHTML=`<div class="card"><div class="card-hdr"><div class="card-title"><div class="ci pur">🏷️</div>Customer Segment Breakdown</div></div><div class="tbl-wrap"><table class="tbl"><thead><tr><th>Segment</th><th class="num">Customers</th><th class="num">Total Revenue</th><th class="num">% of Total</th></tr></thead><tbody>${rows.map(([seg,v])=>`<tr><td style="font-weight:600">${seg}</td><td class="num">${v.cust_count}</td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(v.total)}</td><td class="num">${tot>0?Math.round(v.total/tot*100):0}%</td></tr>`).join('')}</tbody></table></div></div>`;
    }
    const el3=document.getElementById('biz-cust');
    if(el3&&window.CUSTOMERS)renderCustomerSearch('');
  });
}

function renderCustomerSearch(q){
  const el=document.getElementById('biz-cust');
  if(!el||!window.CUSTOMERS)return;
  const byRep=window.CUSTOMERS.by_rep||{};
  let all=[];
  Object.entries(byRep).forEach(([rep,rd])=>{Object.entries(rd.customers||{}).forEach(([code,c])=>{all.push({code,rep,...c});});});
  if(q)all=all.filter(c=>c.name.toLowerCase().includes(q.toLowerCase())||c.rep.toLowerCase().includes(q.toLowerCase()));
  all.sort((a,b)=>b.total-a.total);
  el.innerHTML=`<div class="card"><div class="card-hdr"><div class="card-title"><div class="ci org">👥</div>Customer Profiles</div></div><div style="margin-bottom:12px"><input type="text" value="${q||''}" placeholder="Search customer or rep..." oninput="renderCustomerSearch(this.value)" style="width:100%;padding:8px 12px;border:1px solid var(--border);border-radius:8px;font-size:.75rem;font-family:inherit"></div><div class="tbl-wrap"><table class="tbl"><thead><tr><th>Customer</th><th>Rep</th><th>Segment</th><th class="num">Total Spend</th><th>Last Order</th></tr></thead><tbody>${all.slice(0,50).map(c=>`<tr><td style="font-weight:600;font-size:.7rem">${c.name}</td><td style="font-size:.65rem;color:var(--txt2)">${c.rep}</td><td style="font-size:.63rem">${c.group||'—'}</td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(c.total)}</td><td style="font-size:.65rem;color:${c.last_month==='May'?'var(--grn)':'var(--org)'}">${c.last_month||'—'}</td></tr>`).join('')}</tbody></table></div></div>`;
}

function switchTabBiz(){switchTab('biz');renderBusiness();}
function mobileTabBiz(){mobileTab('biz');renderBusiness();}

renderAll();
renderMoM();
