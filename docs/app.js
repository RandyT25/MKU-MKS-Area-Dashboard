// MKU & MKS Dashboard — app.js (Option C: compressed history)

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
  const isOpen=wrap&&wrap.classList.contains('open');
  document.querySelectorAll('.date-dd-wrap,.dl-wrap').forEach(w=>w.classList.remove('open'));
  if(!isOpen){if(wrap)wrap.classList.add('open');if(mwrap)mwrap.classList.add('open');}
}

// ── Data accessors ──────────────────────────────────────────────

// Returns full SO rows — only available for latest day
function getSO(){
  let r=RAW.so; // latest day only
  if(activeDate!=='ALL'&&activeDate!==RAW.latest)return[]; // compressed day = no rows
  if(company!=='ALL')r=r.filter(x=>x.division===(company==='MKU'?'MKU Bali':'MKS Bali'));
  return r;
}

// Returns summary for a single date (always available)
function getSummary(d){
  return RAW.so_summary[d]||{rev:0,cnt:0,cust_cnt:0,mku_rev:0,mks_rev:0,rep_rev:{},prod_rev:{},cust:{}};
}

// Aggregate summaries across dates
function getAggSummary(){
  const dates=activeDate==='ALL'?RAW.dates:[activeDate];
  const agg={rev:0,cnt:0,rep_rev:{},prod_rev:{},cust:{}};
  const custSet=new Set();
  dates.forEach(d=>{
    const s=getSummary(d);
    agg.rev+=s.rev||0;
    agg.cnt+=s.cnt||0;
    Object.entries(s.rep_rev||{}).forEach(([k,v])=>{agg.rep_rev[k]=(agg.rep_rev[k]||0)+v;});
    Object.entries(s.prod_rev||{}).forEach(([k,v])=>{agg.prod_rev[k]=(agg.prod_rev[k]||0)+v;});
    Object.entries(s.cust||{}).forEach(([k,v])=>{
      if(!agg.cust[k])agg.cust[k]={rev:0,so:0,sales:v.sales,div:v.div};
      agg.cust[k].rev+=v.rev;agg.cust[k].so+=v.so;
    });
    Object.keys(s.cust||{}).forEach(k=>custSet.add(k));
  });
  agg.cust_cnt=custSet.size;
  return agg;
}

function getDel(){
  const dates=activeDate==='ALL'?RAW.dates:[activeDate];
  let all=[];
  dates.forEach(d=>{
    const dd=RAW.delivery_by_date[d];if(!dd)return;
    // Latest day has full records
    if(isLatest(d)){
      if(company==='ALL'||company==='MKU')(dd.mku_full||[]).forEach(r=>all.push({...r,co:'MKU',date:d}));
      if(company==='ALL'||company==='MKS')(dd.mks_full||[]).forEach(r=>all.push({...r,co:'MKS',date:d}));
    } else {
      // Compressed: rebuild minimal records from summary
      const issues=dd.issues||[];
      if(company!=='MKU') issues.filter(r=>r.co==='MKS'||!r.co).forEach(r=>all.push({...r,date:d}));
      if(company!=='MKS') issues.filter(r=>r.co==='MKU').forEach(r=>all.push({...r,date:d}));
    }
  });
  return all;
}

// Delivery stats (works for both full and compressed)
function getDelStats(){
  const dates=activeDate==='ALL'?RAW.dates:[activeDate];
  let tot=0,ful=0,by_area={};
  dates.forEach(d=>{
    const dd=RAW.delivery_by_date[d];if(!dd)return;
    if(isLatest(d)){
      const full=[...(dd.mku_full||[]),...(dd.mks_full||[])];
      if(company!=='ALL'){
        const filtered=full.filter(r=>r.co===(company==='MKU'?'MKU':'MKS')||
          (company==='MKU'&&(dd.mku_full||[]).includes(r))||
          (company==='MKS'&&(dd.mks_full||[]).includes(r)));
        // simplified: use summary
      }
      tot+=full.length;
      ful+=full.filter(r=>r.ket==='FULFILLED').length;
      Object.entries(dd.by_area||{}).forEach(([a,v])=>{
        if(!by_area[a])by_area[a]={t:0,ok:0};
        by_area[a].t+=v.t;by_area[a].ok+=v.ok;
      });
    } else {
      tot+=dd.tot||0;ful+=dd.ful||0;
      Object.entries(dd.by_area||{}).forEach(([a,v])=>{
        if(!by_area[a])by_area[a]={t:0,ok:0};
        by_area[a].t+=v.t;by_area[a].ok+=v.ok;
      });
    }
  });
  return{tot,ful,unf:tot-ful,by_area};
}

function getStk(){
  const date=activeDate==='ALL'?RAW.latest:activeDate;
  const sd=RAW.stock_by_date[date];if(!sd)return[];
  // Latest day has full lists
  if(isLatest(date)){
    const mku=sd.MKU_full||sd.MKU||sd.mku||[];
    const mks=sd.MKS_full||sd.MKS||sd.mks||[];
    if(company==='MKU')return mku.map(s=>({...s,co:'MKU'}));
    if(company==='MKS')return mks.map(s=>({...s,co:'MKS'}));
    return[...mku.map(s=>({...s,co:'MKU'})),...mks.map(s=>({...s,co:'MKS'}))];
  }
  // Compressed: only non-OK items
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
  const tabs=['target','so','delivery','reps','stock','alerts'];
  document.querySelectorAll('.tab').forEach((t,i)=>t.classList.toggle('active',tabs[i]===n));
  document.querySelectorAll('.tc').forEach(c=>c.classList.remove('active'));
  document.getElementById('tc-'+n).classList.add('active');
}
function mobileTab(n){
  switchTab(n);
  document.querySelectorAll('.mnav').forEach(b=>b.classList.remove('active'));
  const btn=document.getElementById('mn-'+n);if(btn)btn.classList.add('active');
  window.scrollTo({top:0,behavior:'smooth'});
}
function renderAll(){buildDT();renderKPIs();renderTarget();renderSO();renderDel();renderReps();renderStock();renderAlerts();}

function renderKPIs(){
  const agg=getAggSummary();
  const delStats=getDelStats();
  const stk=getStk();
  const stkSum=getStkSummary();
  const out=stk.filter(s=>s.st==='out').length+(stkSum&&!isFull(activeDate)?stkSum.mku_out+stkSum.mks_out-stk.filter(s=>s.st==='out').length:0);
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
  const timePct=Math.round(dayNum/30*100);
  const badgeCls=p=>p>=timePct?'b-grn':p>=(timePct*0.75)?'b-org':'b-red';

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
    labels:areas.map(a=>a.area.length>14?a.area.slice(0,13)+'…':a.area),
    datasets:[
      {label:'Achieved',data:areas.map(a=>a.food_ach+a.bev_ach),backgroundColor:'#93b4f8',borderRadius:4,stack:'a'},
      {label:'Remaining',data:areas.map(a=>Math.max(0,(a.food_target+a.bev_target)-(a.food_ach+a.bev_ach))),backgroundColor:'#e4e8ef',stack:'a'}
    ]},options:{...COPTS,scales:{...COPTS.scales,x:{...COPTS.scales.x,stacked:true},y:{...COPTS.scales.y,stacked:true,ticks:{...COPTS.scales.y.ticks,callback:v=>v>=1e9?(v/1e9).toFixed(1)+'B':v>=1e6?(v/1e6).toFixed(0)+'M':v}}}}});

  document.getElementById('tbl-area').innerHTML=`
    <thead><tr><th>Area</th><th>Sales</th><th class="num">Food</th><th class="num">Bev</th><th class="num">Target Total</th><th class="num">Achieved</th><th>% <span style="font-weight:400;color:var(--txt3);font-size:.58rem">(on track ≥${timePct}%)</span></th></tr></thead>
    <tbody>${areas.map(a=>{const p=a.pct,cls=badgeCls(p);return`<tr>
      <td style="font-weight:600">${a.area}</td><td style="color:var(--txt2);font-size:.68rem">${a.sales}</td>
      <td class="num">${fmtRp(a.food_ach)}</td><td class="num">${fmtRp(a.bev_ach)}</td>
      <td class="num" style="color:var(--txt3)">${fmtRp(a.food_target+a.bev_target)}</td>
      <td class="num" style="font-weight:700">${fmtRp(a.food_ach+a.bev_ach)}</td>
      <td><span class="badge ${cls}">${p}%</span></td></tr>`;}).join('')}</tbody>
    <tfoot><tr><td colspan="2"><strong>GRAND TOTAL</strong></td>
      <td class="num"><strong style="color:var(--mks)">${fmtRp(areas.reduce((s,a)=>s+a.food_ach,0))}</strong></td>
      <td class="num"><strong style="color:var(--grn)">${fmtRp(areas.reduce((s,a)=>s+a.bev_ach,0))}</strong></td>
      <td class="num">${fmtRp(areas.reduce((s,a)=>s+a.food_target+a.bev_target,0))}</td>
      <td class="num"><strong>${fmtRp(areas.reduce((s,a)=>s+a.food_ach+a.bev_ach,0))}</strong></td>
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
}

function renderSO(){
  const agg=getAggSummary();
  const isFullDay=isFull(activeDate);
  document.getElementById('so-co-lbl').textContent=company==='ALL'?'All':company;

  const rS=Object.entries(agg.rep_rev)
    .filter(([n])=>company==='ALL'||(company==='MKU'&&RAW.so.find(r=>r.sales===n&&r.division==='MKU Bali'))||(company==='MKS'&&RAW.so.find(r=>r.sales===n&&r.division==='MKS Bali'))||true)
    .sort((a,b)=>b[1]-a[1]).slice(0,12);
  if(charts.rep)charts.rep.destroy();
  charts.rep=new Chart(document.getElementById('ch-rep'),{type:'bar',data:{labels:rS.map(([n])=>n),datasets:[{data:rS.map(([,v])=>v),backgroundColor:rS.map((_,i)=>i===0?'#2563eb':i<3?'#93b4f8':'#c7d8fc'),borderRadius:6}]},options:{indexAxis:'y',...COPTS,plugins:{legend:{display:false}},scales:{x:{...COPTS.scales.x,ticks:{...COPTS.scales.x.ticks,callback:v=>v>=1e6?(v/1e6).toFixed(0)+'M':v}},y:{...COPTS.scales.y,grid:{display:false}}}}});

  const pS=Object.entries(agg.prod_rev).sort((a,b)=>b[1]-a[1]).slice(0,10);
  if(charts.prod)charts.prod.destroy();
  charts.prod=new Chart(document.getElementById('ch-prod'),{type:'bar',data:{labels:pS.map(([n])=>n.length>28?n.slice(0,27)+'…':n),datasets:[{data:pS.map(([,v])=>v),backgroundColor:'#6ee7b7',borderRadius:6}]},options:{indexAxis:'y',...COPTS,plugins:{legend:{display:false}},scales:{x:{...COPTS.scales.x,ticks:{...COPTS.scales.x.ticks,callback:v=>v>=1e6?(v/1e6).toFixed(0)+'M':v}},y:{...COPTS.scales.y,grid:{display:false},ticks:{...COPTS.scales.y.ticks,font:{size:10}}}}}});

  const cTop=Object.entries(agg.cust).sort((a,b)=>b[1].rev-a[1].rev).slice(0,20);
  document.getElementById('tbl-cust').innerHTML=`<thead><tr><th>#</th><th>Customer</th><th>Sales</th><th class="num">Orders</th><th class="num">Revenue</th></tr></thead><tbody>${cTop.map(([n,v],i)=>`<tr><td style="color:var(--txt3);font-weight:700">${i+1}</td><td style="font-weight:600">${n}</td><td><span class="badge b-gray">${v.sales}</span></td><td class="num">${v.so}</td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(v.rev)}</td></tr>`).join('')}</tbody>`;

  // Full SO table only for latest day
  document.getElementById('so-count-lbl').textContent=agg.cnt+' orders';
  if(isFullDay){
    const so=getSO();
    document.getElementById('tbl-so').innerHTML=`<thead><tr><th>Date</th><th>No SO</th><th>Co</th><th>Customer</th><th>Sales</th><th>Product</th><th class="num">Qty</th><th class="num">Revenue</th></tr></thead><tbody>${so.map(r=>`<tr><td style="font-size:.63rem;color:var(--txt3);white-space:nowrap">${fmtD(r.date)}</td><td style="font-size:.62rem;color:var(--txt3)">${r.no_so}</td><td><span class="badge ${r.division==='MKU Bali'?'b-mku':'b-mks'}">${r.division==='MKU Bali'?'MKU':'MKS'}</span></td><td style="max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-weight:600">${r.customer}</td><td style="color:var(--txt2);font-size:.68rem">${r.sales}</td><td style="max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:.68rem">${r.product}</td><td class="num">${fmtQ(r.so_pcs)} <span style="color:var(--txt3)">${r.unit}</span></td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(r.revenue)}</td></tr>`).join('')}</tbody>`;
  } else {
    document.getElementById('tbl-so').innerHTML=`<tr><td colspan="8" style="text-align:center;padding:24px;color:var(--txt3)">📦 Detailed SO rows available for latest day only.<br><span style="font-size:.68rem">Select <strong>${fmtD(RAW.latest)} ★</strong> to see full order list.</span></td></tr>`;
  }
}

function renderDel(){
  const stats=getDelStats();
  const del=getDel(); // full rows for latest, issues-only for old
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
    <div class="kpi-card c-gray"><div class="kpi-icon gray">📋</div><div class="kpi-label">Fulfilment Rate</div><div class="kpi-value">${pct(stats.ful,stats.tot)}%</div><div class="kpi-sub">Across all orders</div></div>
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

function renderReps(){
  const agg=getAggSummary();
  document.getElementById('reps-lbl').textContent=(company==='ALL'?'All':company)+(activeDate==='ALL'?' · All days':' · '+fmtD(activeDate));
  const reps=Object.entries(agg.rep_rev).sort((a,b)=>b[1]-a[1]);
  const max=reps[0]?.[1]||1;
  // Try to get div info from latest SO
  const divMap={};RAW.so.forEach(r=>{divMap[r.sales]=r.division;});
  document.getElementById('tbl-reps').innerHTML=`<thead><tr><th>#</th><th>Rep</th><th>Div</th><th class="num">Revenue</th><th class="num">Orders</th><th class="num">Customers</th><th style="width:100px">vs Top</th></tr></thead><tbody>${reps.map(([n,rev],i)=>{
    const div=divMap[n]||'—';
    // Get order/customer counts from summary
    let orders=0,custs=new Set();
    (activeDate==='ALL'?RAW.dates:[activeDate]).forEach(d=>{
      const s=getSummary(d);
      Object.entries(s.cust||{}).forEach(([c,v])=>{if(v.sales===n){orders+=v.so;custs.add(c);}});
    });
    return`<tr><td style="font-weight:700">${i===0?'🥇':i===1?'🥈':i===2?'🥉':i+1}</td><td style="font-weight:700;color:${i===0?'var(--mks)':'var(--txt)'}">${n}</td><td><span class="badge ${div==='MKU Bali'?'b-mku':'b-mks'}">${div==='MKU Bali'?'MKU':div==='MKS Bali'?'MKS':'—'}</span></td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(rev)}</td><td class="num">${orders||'—'}</td><td class="num">${custs.size||'—'}</td><td><div class="pb"><div class="pb-fill" style="width:${Math.round(rev/max*100)}%;background:${i===0?'var(--mks)':i<3?'#93b4f8':'#c7d8fc'}"></div></div></td></tr>`;
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
  document.getElementById('sg').innerHTML=filtered.map(s=>`<div class="si ${s.st}"><div class="si-code">${s.c}${company==='ALL'?' · <b>'+s.co+'</b>':''}</div><div class="si-name">${s.n}</div><div class="si-bottom"><div class="si-qty ${s.st}">${s.s<=0?'0':fmtQ(s.s)}<span style="font-size:.6rem;font-weight:400;margin-left:2px">${s.u}</span></div><div class="si-days ${s.st}">${s.s<=0?'OUT':s.bf>0?fmtQ(s.bf)+'d':'—'}</div></div></div>`).join('')||'<p style="color:var(--txt3);padding:20px;font-size:.75rem">No items.</p>';
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

  // Get unfulfilled delivery issues
  const unfI=[];
  (activeDate==='ALL'?RAW.dates:[activeDate]).forEach(d=>{
    const dd=RAW.delivery_by_date[d];if(!dd)return;
    if(isLatest(d)){[...(dd.mku_full||[]),...(dd.mks_full||[])].filter(r=>r.ket==='UNFULFILLED').forEach(r=>unfI.push({...r,date:d}));}
    else{(dd.issues||[]).forEach(r=>unfI.push({...r,date:d}));}
  });

  const secs=[
    {id:'a-out',ic:'🔴',tt:'Out of Stock — Active SKUs at Zero',cnt:outI.length,cc:outI.length?'red':'grn',items:outI.length?outI.map(s=>`<div class="al out"><span>🔴</span><div class="al-body"><strong>${s.n}</strong><br><span style="font-size:.68rem;color:var(--txt2)">${s.c} · Avg ${(s.a||0).toFixed(0)} ${s.u}/mo</span></div><span class="al-co ${s.co.toLowerCase()}">${s.co}</span></div>`):['<p style="color:var(--txt3);font-size:.74rem;padding:4px 0">✅ No out-of-stock items</p>']},
    {id:'a-crit',ic:'🚨',tt:'Critical — Less than 3 Days Left',cnt:critI.length,cc:critI.length?'red':'grn',items:critI.length?critI.map(s=>`<div class="al out"><span>🚨</span><div class="al-body"><strong>${s.n}</strong><br><span style="font-size:.68rem;color:var(--mku);font-weight:600">${fmtQ(s.s)} ${s.u} · ${s.bf>0?s.bf.toFixed(1)+' days':'<1 day'}</span></div><span class="al-co ${s.co.toLowerCase()}">${s.co}</span></div>`):['<p style="color:var(--txt3);font-size:.74rem;padding:4px 0">✅ No critical items</p>']},
    {id:'a-low',ic:'⚠️',tt:'Low Stock — 3 to 7 Days Left',cnt:lowI.length,cc:lowI.length?'org':'grn',items:lowI.length?lowI.map(s=>`<div class="al warn"><span>⚠️</span><div class="al-body"><strong>${s.n}</strong><br><span style="font-size:.68rem;color:var(--org);font-weight:600">${fmtQ(s.s)} ${s.u} · ${s.bf.toFixed(1)} days</span></div><span class="al-co ${s.co.toLowerCase()}">${s.co}</span></div>`):['<p style="color:var(--txt3);font-size:.74rem;padding:4px 0">✅ No low-stock items</p>']},
    {id:'a-unf',ic:'🚫',tt:'Unfulfilled Deliveries — Not Sent',cnt:unfI.length,cc:unfI.length?'red':'grn',items:unfI.length?unfI.map(r=>`<div class="al out"><span>🚫</span><div class="al-body"><strong>${r.customer||'—'}</strong><br><span style="font-size:.68rem;color:var(--txt2)">${r.product||'—'} · <span style="color:var(--mku);font-weight:700">NOT DELIVERED</span></span></div><span class="al-co ${(r.co||'mks').toLowerCase()}">${r.co||'—'}</span></div>`):['<p style="color:var(--txt3);font-size:.74rem;padding:4px 0">✅ All orders sent</p>']},
  ];
  document.getElementById('alerts-accordions').innerHTML=secs.map(s=>`<div class="accord" id="${s.id}"><div class="accord-hdr" onclick="tog('${s.id}')"><div class="accord-icon">${s.ic}</div><div class="accord-title">${s.tt}</div><span class="accord-count ${s.cc}">${s.cnt}</span><div class="accord-chev">▼</div></div><div class="accord-body"><div class="accord-inner">${s.items.join('')}</div></div></div>`).join('');
}

function tog(id){document.getElementById(id).classList.toggle('open');}
function toggleDL(){document.getElementById('dl-wrap').classList.toggle('open');}
document.addEventListener('click',e=>{if(!e.target.closest('.dl-wrap')&&!e.target.closest('.date-dd-wrap')){document.querySelectorAll('.dl-wrap').forEach(w=>w.classList.remove('open'));document.querySelectorAll('.date-dd-wrap').forEach(w=>w.classList.remove('open'));}});

function dlHTML(){const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([document.documentElement.outerHTML],{type:'text/html'}));a.download='MKU_MKS_Dashboard_Mar2026.html';a.click();document.querySelectorAll('.dl-wrap').forEach(w=>w.classList.remove('open'));}
function dlCSV(){
  const agg=getAggSummary();const stk=getStk();
  const csv=(h,r)=>[h.join(','),...r.map(row=>row.map(v=>String(v).includes(',')?`"${v}"`:v).join(','))].join('\n');
  const soRows=getSO().length>0?getSO():[];
  [{n:'SO_Mar2026.csv',c:csv(['Date','No SO','Division','Customer','Sales','Product','Qty','Unit','Revenue'],soRows.map(r=>[r.date,r.no_so,r.division,r.customer,r.sales,r.product,r.so_pcs,r.unit,Math.round(r.revenue)]))},
   {n:'Stock_Mar2026.csv',c:csv(['Code','Product','Unit','Stock','Avg/mo','Days','Status','Co'],stk.map(s=>[s.c,s.n,s.u,fmtQ(s.s),fmtQ(s.a),s.bf>0?fmtQ(s.bf):'0',s.st,s.co]))}
  ].forEach((f,i)=>setTimeout(()=>{const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([f.c],{type:'text/csv'}));a.download=f.n;a.click();},i*400));
  document.querySelectorAll('.dl-wrap').forEach(w=>w.classList.remove('open'));
}
function dlPDF(){
  const agg=getAggSummary();const{targets:T}=getTgt();
  const tot_t=Object.values(T).reduce((s,t)=>s+t.target,0),tot_a=Object.values(T).reduce((s,t)=>s+t.achievement,0);
  const top5=Object.entries(agg.rep_rev).sort((a,b)=>b[1]-a[1]).slice(0,5);
  const dateLabel=activeDate==='ALL'?'All Days':fmtD(activeDate);
  const htmlStr=`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Report Mar 2026</title><link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;600;700;800&display=swap" rel="stylesheet"><style>*{box-sizing:border-box;margin:0;padding:0;}body{font-family:'Plus Jakarta Sans',sans-serif;padding:28px;font-size:12px;color:#1a2035;}.hdr{display:flex;justify-content:space-between;margin-bottom:20px;padding-bottom:14px;border-bottom:2px solid #1a2035;}.ht{font-size:1.3rem;font-weight:800;}.mku{color:#dc2626;}.mks{color:#2563eb;}.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:8px;margin-bottom:18px;}.kpi{border:1px solid #e4e8ef;border-radius:7px;padding:10px;border-top:3px solid;}.kpi.bl{border-top-color:#2563eb;}.kpi.gr{border-top-color:#059669;}.kpi.pu{border-top-color:#7c3aed;}.kl{font-size:.55rem;font-weight:700;color:#8a93b0;text-transform:uppercase;margin-bottom:4px;}.kv{font-size:1.1rem;font-weight:800;}.kv.bl{color:#2563eb;}.tgts{display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin-bottom:18px;}.tgt{border:1px solid #e4e8ef;border-radius:7px;padding:10px;}.tn{font-size:.65rem;font-weight:700;margin-bottom:5px;}.tp{font-size:1rem;font-weight:800;margin-bottom:3px;}.pb{background:#e4e8ef;border-radius:99px;height:4px;}.pbf{height:4px;border-radius:99px;}.st{font-size:.75rem;font-weight:800;margin-bottom:8px;padding-bottom:6px;border-bottom:1px solid #e4e8ef;}table{width:100%;border-collapse:collapse;font-size:.7rem;}th{background:#f4f6f9;padding:6px 9px;text-align:left;font-size:.58rem;font-weight:700;color:#8a93b0;text-transform:uppercase;}td{padding:7px 9px;border-bottom:1px solid #f0f2f7;}td.r{text-align:right;}.ftr{margin-top:22px;padding-top:10px;border-top:1px solid #e4e8ef;display:flex;justify-content:space-between;font-size:.58rem;color:#8a93b0;}@media print{body{padding:14px;}}</style></head><body>
  <div class="hdr"><div><div class="ht"><span class="mku">MKU</span> &amp; <span class="mks">MKS</span> — March 2026 Report</div><div style="font-size:.7rem;color:#8a93b0;margin-top:4px">📅 ${dateLabel}</div></div><div style="font-size:.65rem;color:#8a93b0;text-align:right">Area Manager<br>Confidential</div></div>
  <div class="kpis"><div class="kpi bl"><div class="kl">Revenue</div><div class="kv bl">${fmtRp(agg.rev)}</div></div><div class="kpi gr"><div class="kl">Monthly Target</div><div class="kv" style="color:#059669">${pct(tot_a,tot_t)}%</div></div><div class="kpi pu"><div class="kl">Nestlé Target</div><div class="kv" style="color:#7c3aed">${pct(T.NESTLE?.achievement||0,T.NESTLE?.target||1)}%</div></div><div class="kpi bl"><div class="kl">Orders</div><div class="kv bl">${agg.cnt}</div></div></div>
  <div class="tgts">${Object.entries(T).map(([c,t])=>{const p=pct(t.achievement,t.target),col=p>=80?'#059669':p>=60?'#d97706':'#dc2626';return`<div class="tgt"><div class="tn">${{FOOD:'🍽️',BEVERAGE:'🥤',NESTLE:'☕'}[c]||''} ${c}</div><div class="tp" style="color:${col}">${p}%</div><div style="font-size:.6rem;color:#8a93b0;margin-bottom:5px">${fmtRp(t.achievement)} / ${fmtRp(t.target)}</div><div class="pb"><div class="pbf" style="width:${Math.min(p,100)}%;background:${col}"></div></div></div>`;}).join('')}</div>
  <div class="st">Top 5 Sales Reps</div><table><thead><tr><th>#</th><th>Rep</th><th class="r">Revenue</th></tr></thead><tbody>${top5.map(([n,rv],i)=>`<tr><td>${i+1}</td><td style="font-weight:700">${n}</td><td class="r" style="font-weight:700;color:#2563eb">${fmtRp(rv)}</td></tr>`).join('')}</tbody></table>
  <div class="ftr"><span>MKU &amp; MKS Dashboard</span><span>March 2026</span><span>Internal Use Only</span></div>
</body></html>`;
  const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([htmlStr],{type:'text/html'}));a.download='MKU_MKS_Report_Mar2026.html';a.click();
  document.querySelectorAll('.dl-wrap').forEach(w=>w.classList.remove('open'));
}

renderAll();
