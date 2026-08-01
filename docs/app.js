// MKU & MKS Dashboard — app.js (Option C: compressed history)

let company='ALL', stockFilter='all', activeDate='ALL', charts={};

const _HOLIDAYS=['2026-01-01','2026-01-16','2026-02-17','2026-03-19','2026-03-21','2026-03-22',
  '2026-04-03','2026-05-01','2026-05-14','2026-05-27','2026-05-31','2026-06-01'];
function _workDays(fromDate,toDate){
  let cnt=0,d=new Date(fromDate+'T00:00:00');
  const end=new Date(toDate+'T00:00:00');
  while(d<=end){
    const ds=d.getFullYear()+'-'+String(d.getMonth()+1).padStart(2,'0')+'-'+String(d.getDate()).padStart(2,'0');
    if(d.getDay()!==0&&!_HOLIDAYS.includes(ds))cnt++;
    d.setDate(d.getDate()+1);
  }
  return cnt;
}

const fmtRp=n=>{if(n>=1e9)return'Rp '+(n/1e9).toFixed(1)+'B';if(n>=1e6)return'Rp '+(n/1e6).toFixed(1)+'M';if(n>=1e3)return'Rp '+(n/1e3).toFixed(0)+'K';return'Rp '+Math.round(n).toLocaleString();};
const fmtQ=n=>{const r=Math.round(n*100)/100;return r%1===0?r.toFixed(0):r.toFixed(1);};
const pct=(a,t)=>t>0?Math.round(a/t*100):0;
const fmtD=d=>{const[,m,dy]=d.split('-');return parseInt(dy)+' '+['','Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][parseInt(m)];};
const isLatest=d=>d===RAW.latest;
const growthArr=(cur,prev)=>{if(!prev||prev===0)return'';const g=((cur-prev)/prev*100).toFixed(1);return parseFloat(g)>=0?'<span style="font-size:.6rem;font-weight:700;color:var(--grn)">&#8593; '+g+'%</span>':'<span style="font-size:.6rem;font-weight:700;color:var(--mku)">&#8595; '+Math.abs(g)+'%</span>';};
const isFull=d=>d==='ALL'?true:isLatest(d);
const COPTS={responsive:true,maintainAspectRatio:false,plugins:{legend:{labels:{color:'#8a93b0',font:{family:'Plus Jakarta Sans',size:11},boxWidth:10,padding:14}}},scales:{x:{ticks:{color:'#8a93b0',font:{family:'Plus Jakarta Sans',size:10}},grid:{color:'#f0f2f7'},border:{display:false}},y:{ticks:{color:'#8a93b0',font:{family:'Plus Jakarta Sans',size:10}},grid:{color:'#f0f2f7'},border:{display:false}}}};

function buildDT(){
  const lbl=activeDate==='ALL'?'📅 All Days':'📅 '+fmtD(activeDate);
  const mlbl=activeDate==='ALL'?'📅 All':'📅 '+fmtD(activeDate);
  const lbl_el=document.getElementById('date-dd-lbl');if(lbl_el)lbl_el.textContent=lbl;
  const mlbl_el=document.getElementById('m-date-dd-lbl');if(mlbl_el)mlbl_el.textContent=mlbl;
  const offSet=new Set(RAW.off_days||[]);
  const allDates=['ALL',...RAW.dates];
  const items=[];
  for(let i=0;i<allDates.length;i++){
    const d=allDates[i];
    const label=d==='ALL'?'All Days':fmtD(d)+(isLatest(d)?' ★':'');
    items.push(`<button class="date-dd-item ${activeDate===d?'active':''}" onclick="setDate('${d}')" style="font-size:.65rem;padding:6px 12px">${label}</button>`);
    // Insert off-day markers between this date and the next data date
    if(d!=='ALL'&&allDates[i+1]&&allDates[i+1]!=='ALL'){
      const cur=new Date(d+'T00:00:00'),nxt=new Date(allDates[i+1]+'T00:00:00');
      for(let t=new Date(cur);t<nxt;t.setDate(t.getDate()+1)){
        const ds=t.toISOString().slice(0,10);
        if(ds!==d&&offSet.has(ds))
          items.push(`<div style="font-size:.6rem;color:var(--txt3);padding:4px 12px;display:flex;align-items:center;gap:5px">📅 <span>${fmtD(ds)}</span><span style="background:#f1f5f9;border-radius:4px;padding:1px 5px;font-size:.55rem;font-weight:700;color:#64748b">OFF</span></div>`);
      }
    }
  }
  // Off days after the last data date
  (RAW.off_days||[]).filter(d=>d>RAW.dates[RAW.dates.length-1]).forEach(d=>{
    items.push(`<div style="font-size:.6rem;color:var(--txt3);padding:4px 12px;display:flex;align-items:center;gap:5px">📅 <span>${fmtD(d)}</span><span style="background:#f1f5f9;border-radius:4px;padding:1px 5px;font-size:.55rem;font-weight:700;color:#64748b">OFF</span></div>`);
  });
  const items_str=items.join('');
  ['date-dd-menu','m-date-dd-menu'].forEach(id=>{
    const el=document.getElementById(id);if(el)el.innerHTML=items_str;
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

// Aggregate summaries — company-aware
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
    const _dmk=d.slice(0,7);const _dmo=RAW.months[_dmk]||{};const _ddbd=_dmo.delivery_by_date||RAW.delivery_by_date||{};
    const dd=_ddbd[d];if(!dd)return;
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

// Delivery stats — company-aware
function getDelStats(){
  const dates=activeDate==='ALL'?RAW.dates:[activeDate];
  let tot=0,ful=0,by_area={};
  dates.forEach(d=>{
    const _smk=d.slice(0,7);const _smo=RAW.months[_smk]||{};const _sdbd=_smo.delivery_by_date||RAW.delivery_by_date||{};
    const dd=_sdbd[d];if(!dd)return;
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

function _buildPriceMap(){
  // Build product->unit_price from RAW.so using bs_so
  // bs_so is total line value; if so_pcs>0 use bs_so/so_pcs else use bs_so as-is
  const map={};
  (RAW.so||[]).forEach(r=>{
    if(!r.product) return;
    const qty=r.so_pcs||0;
    const val=r.bs_so||r.revenue||0;
    if(!map[r.product]&&val>0){
      map[r.product]=qty>0?val/qty:val;
    }
  });
  return map;
}

function getStk(){
  const date=activeDate==='ALL'?RAW.latest:activeDate;
  const mk=date.slice(0,7);
  const mo=RAW.months[mk]||{};
  const sbd=mo.stock_by_date||RAW.stock_by_date||{};
  const sd=sbd[date];if(!sd)return[];
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
  const mk=date.slice(0,7);
  const mo=RAW.months[mk]||{};
  const sbd=mo.stock_by_date||RAW.stock_by_date||{};
  const sd=sbd[date];if(!sd)return null;
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
  const outCount=stk.filter(s=>s.st==='out').length;
  const critCount=stk.filter(s=>s.st==='critical'||s.st==='low').length;
  const dateLabel=activeDate==='ALL'?(RAW.dates.length+' days'):fmtD(activeDate);
  const allDates=RAW.dates||[];
  const curIdx=activeDate==='ALL'?allDates.length-1:allDates.indexOf(activeDate);
  const prevD=curIdx>0?allDates[curIdx-1]:null;
  // Total revenue from pencapaian (Food+Bev+Nestle)
  const _curMk=activeDate==='ALL'?RAW.latest.slice(0,7):activeDate.slice(0,7);
  const _tbd=(RAW.months[_curMk]||{}).targets_by_date||{};
  const _tdate=activeDate==='ALL'?RAW.latest:activeDate;
  const _T=(_tbd[_tdate]||{}).targets||{};
  const pencRev=(_T.FOOD&&_T.FOOD.achievement||0)+(_T.BEVERAGE&&_T.BEVERAGE.achievement||0)+(_T.NESTLE&&_T.NESTLE.achievement||0);
  const totalRev=pencRev>0?pencRev:agg.rev;
  const prevT=prevD?((_tbd[prevD]||{}).targets||{}):null;
  const prevPencRev=prevT?((prevT.FOOD&&prevT.FOOD.achievement||0)+(prevT.BEVERAGE&&prevT.BEVERAGE.achievement||0)+(prevT.NESTLE&&prevT.NESTLE.achievement||0)):0;
  // Daily SO — always single day, never summed
  const _dailyDate=activeDate==='ALL'?RAW.latest:activeDate;
  const _dailyS=getSummary(_dailyDate);
  const dailyRev=_dailyS.rev||0;
  const dailyMku=_dailyS.mku_rev||0;
  const dailyMks=_dailyS.mks_rev||0;
  const dailyCnt=_dailyS.cnt||0;
  const _dailyPrev=prevD?getSummary(prevD):{};
  const dailyLbl=activeDate==='ALL'?'Latest day · '+fmtD(RAW.latest):fmtD(activeDate);
  // MKU/MKS rev from SO (company-aware)
  let mkuRev=0,mksRev=0;
  if(activeDate===RAW.latest||activeDate==='ALL'){
    const divMapK={};RAW.so.forEach(r=>{divMapK[r.sales]=r.division;});
    Object.entries(agg.rep_rev).forEach(([k,v])=>{if(divMapK[k]==='MKU Bali')mkuRev+=v;else if(divMapK[k]==='MKS Bali')mksRev+=v;});
  } else {
    const s=getSummary(activeDate);mkuRev=s.mku_rev||0;mksRev=s.mks_rev||0;
  }
  document.getElementById('kpi-strip').innerHTML=`
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:10px">
<div class="kpi-card c-mks"><div class="kpi-icon mks">📊</div><div class="kpi-label">Month Achievement</div><div class="kpi-value mks">${fmtRp(totalRev)}</div>${prevD&&prevPencRev>0?growthArr(totalRev,prevPencRev):''}<div class="kpi-sub">Cumulative · ${dateLabel}</div></div>
<div class="kpi-card c-grn"><div class="kpi-icon grn">💰</div><div class="kpi-label">Today's Sales</div><div class="kpi-value grn">${fmtRp(dailyRev)}</div>${prevD?growthArr(dailyRev,_dailyPrev.rev||0):''}<div class="kpi-sub">${dailyCnt} orders · ${dailyLbl}</div></div>
<div class="kpi-card c-mku"><div class="kpi-icon mku">🏢</div><div class="kpi-label">MKU Today</div><div class="kpi-value mku">${fmtRp(dailyMku)}</div>${prevD?growthArr(dailyMku,_dailyPrev.mku_rev||0):''}<div class="kpi-sub">MKU Bali</div></div>
<div class="kpi-card c-mks"><div class="kpi-icon mks">🏢</div><div class="kpi-label">MKS Today</div><div class="kpi-value mks">${fmtRp(dailyMks)}</div>${prevD?growthArr(dailyMks,_dailyPrev.mks_rev||0):''}<div class="kpi-sub">MKS Bali</div></div>
</div>
<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:10px">
<div class="kpi-card c-grn"><div class="kpi-icon grn">🚚</div><div class="kpi-label">Fulfilment</div><div class="kpi-value grn">${delStats.tot>0?pct(delStats.ful,delStats.tot):'-'}%</div><div class="kpi-sub">${delStats.ful} of ${delStats.tot}</div></div>
<div class="kpi-card ${outCount+critCount>0?'c-mku':'c-grn'}"><div class="kpi-icon ${outCount+critCount>0?'mku':'grn'}">${outCount+critCount>0?'🔴':'✅'}</div><div class="kpi-label">Stock Alerts</div><div class="kpi-value ${outCount+critCount>0?'mku':''}">${outCount+critCount}</div><div class="kpi-sub">${outCount} out · ${critCount} low</div></div>
<div class="kpi-card c-gray"><div class="kpi-icon gray">📦</div><div class="kpi-label">Active SKUs</div><div class="kpi-value">${stkSum?(stkSum.mku_total+stkSum.mks_total):stk.length}</div><div class="kpi-sub">Latest snapshot</div></div>
</div>`
}

function renderTarget(){
  const {targets:T,area_targets:areas,nestle_areas:nestleA}=getTgt();
  const COL={FOOD:'#2563eb',BEVERAGE:'#059669',NESTLE:'#7c3aed'};
  const ICO={FOOD:'🍽️',BEVERAGE:'🥤',NESTLE:'☕'};
  const tot_t=Object.values(T).reduce((s,t)=>s+t.target,0);
  const tot_a=Object.values(T).reduce((s,t)=>s+t.achievement,0);
  const tp=pct(tot_a,tot_t);
  const cats=Object.keys(T);
  const lastDate=activeDate==='ALL'?RAW.latest:activeDate;
  const[_ty,_tm,_td]=lastDate.split('-').map(Number);
  const dayNum=_td;
  const daysInMonth=new Date(_ty,_tm,0).getDate();

  const timePct=Math.round(dayNum/daysInMonth*100);
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

  // Build prev date area map for ↑↓ growth indicator
  // For a specific date: compare vs previous uploaded date
  // For All Days: compare latest date vs second-to-last date (day-over-day on most recent)
  const _allDates=(RAW.dates||[]).slice().sort();
  const _activeDateResolved=activeDate==='ALL'?RAW.latest:activeDate;
  const _curDIdx=_allDates.indexOf(_activeDateResolved);
  const _prevDate=_curDIdx>0?_allDates[_curDIdx-1]:null;
  const _activeMK=(_activeDateResolved||RAW.latest).slice(0,7);const _monthTbd=(RAW.months[_activeMK]||{}).targets_by_date||{};const _prevAreas=_prevDate?(_monthTbd[_prevDate]||{}).area_targets||[]:[];
  const _prevAreaMap={};_prevAreas.forEach(a=>{_prevAreaMap[a.area]=a.pct;});
  const _prevNestleArr=_prevDate?(RAW.months[_activeMK]?.targets_by_date?.[_prevDate]?.nestle_areas||null):null;

  document.getElementById('tbl-area').innerHTML=`
    <thead><tr><th>Area</th><th>Sales</th><th class="num">Food</th><th class="num">Bev</th><th class="num">Target Total</th><th class="num">Achieved</th><th>% <span style="font-weight:400;color:var(--txt3);font-size:.58rem">(on track ≥${timePct}%)</span></th></tr></thead>
    <tbody>${areas.map(a=>{const p=a.pct,cls=badgeCls(p);
      const prevP=_prevAreaMap[a.area];
      const delta=prevP!=null?p-prevP:null;
      const growthHtml=delta!=null&&delta!==0?`<span style="font-size:.6rem;font-weight:700;color:${delta>0?'var(--grn)':'var(--mku)'};margin-left:4px">${delta>0?'↑':'↓'}${Math.abs(delta)}%</span>`:'';
      return`<tr>
      <td style="font-weight:600">${a.area}</td><td style="color:var(--txt2);font-size:.68rem">${a.sales}</td>
      <td class="num">${fmtRp(a.food_ach)}</td><td class="num">${fmtRp(a.bev_ach)}</td>
      <td class="num" style="color:var(--txt3)">${fmtRp(a.food_target+a.bev_target)}</td>
      <td class="num" style="font-weight:700">${fmtRp(a.food_ach+a.bev_ach)}</td>
      <td><span class="badge ${cls}">${p}%</span>${growthHtml}</td></tr>`;}).join('')}</tbody>
    <tfoot><tr><td colspan="2"><strong>GRAND TOTAL</strong></td>
      <td class="num"><strong style="color:var(--mks)">${fmtRp(areas.reduce((s,a)=>s+a.food_ach,0))}</strong></td>
      <td class="num"><strong style="color:var(--grn)">${fmtRp(areas.reduce((s,a)=>s+a.bev_ach,0))}</strong></td>
      <td class="num">${fmtRp(areas.reduce((s,a)=>s+a.food_target+a.bev_target,0))}</td>
      <td class="num"><strong>${fmtRp(areas.reduce((s,a)=>s+a.food_ach+a.bev_ach,0))}</strong></td>
      <td>${(()=>{const tp2=pct(areas.reduce((s,a)=>s+a.food_ach+a.bev_ach,0),areas.reduce((s,a)=>s+a.food_target+a.bev_target,0));const prevTotP=_prevDate?pct((_prevAreas.reduce((s,a)=>s+a.food_ach+a.bev_ach,0)),(_prevAreas.reduce((s,a)=>s+a.food_target+a.bev_target,0))):null;const td=prevTotP!=null?tp2-prevTotP:null;const tarr=td!=null&&td!==0?`<span style="font-size:.6rem;font-weight:700;color:${td>0?'var(--grn)':' var(--mku)'};margin-left:4px">${td>0?'↑':'↓'}${Math.abs(td)}%</span>`:'';return`<span class="badge ${badgeCls(tp2)}">${tp2}%</span>${tarr}`;})()}</td></tr></tfoot>`;

  document.getElementById('nestle-table').innerHTML=`
    <thead><tr><th>Channel</th><th>Sales</th><th class="num">Target</th><th class="num">Achieved</th><th>% <span style="font-weight:400;color:var(--txt3);font-size:.58rem">(on track ≥${timePct}%)</span></th></tr></thead>
    <tbody>${(nestleA||[]).map((n,ni)=>{const p=pct(n.achievement,n.target),cls=badgeCls(p);const prevN=(_prevAreaMap&&_prevNestleArr)?_prevNestleArr[ni]:null;const prevNP=prevN?pct(prevN.achievement,prevN.target):null;const ndelta=prevNP!=null?p-prevNP:null;const narrow=ndelta!=null&&ndelta!==0?`<span style="font-size:.6rem;font-weight:700;color:${ndelta>0?'var(--grn)':'var(--mku)'};margin-left:4px">${ndelta>0?'↑':'↓'}${Math.abs(ndelta)}%</span>`:'';return`<tr>
      <td style="font-weight:600">${n.area}</td>
      <td style="color:var(--txt2);font-size:.68rem">${n.sales||'—'}</td>
      <td class="num" style="color:var(--txt3)">${fmtRp(n.target)}</td>
      <td class="num" style="font-weight:700">${fmtRp(n.achievement)}</td>
      <td><span class="badge ${cls}">${p}%</span>${narrow}</td></tr>`;}).join('')}</tbody>
    <tfoot><tr>
      <td colspan="2"><strong>GRAND TOTAL</strong></td>
      <td class="num">${fmtRp((nestleA||[]).reduce((s,n)=>s+n.target,0))}</td>
      <td class="num"><strong>${fmtRp((nestleA||[]).reduce((s,n)=>s+n.achievement,0))}</strong></td>
      <td><span class="badge ${badgeCls(pct((nestleA||[]).reduce((s,n)=>s+n.achievement,0),(nestleA||[]).reduce((s,n)=>s+n.target,0)))}">${pct((nestleA||[]).reduce((s,n)=>s+n.achievement,0),(nestleA||[]).reduce((s,n)=>s+n.target,0))}%</span></td>
    </tr></tfoot>`;

  // ── Balian table ──────────────────────────────────────────────
  const balian=getTgt().balian||[];
  // Support both old dict format and new list format
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

  const cTop=Object.entries(agg.cust).sort((a,b)=>b[1].rev-a[1].rev).slice(0,20);
  document.getElementById('tbl-cust').innerHTML=`<thead><tr><th>#</th><th>Customer</th><th>Sales</th><th class="num">Orders</th><th class="num">Revenue</th></tr></thead><tbody>${cTop.map(([n,v],i)=>`<tr><td style="color:var(--txt3);font-weight:700">${i+1}</td><td style="font-weight:600">${n}</td><td><span class="badge b-gray">${v.sales}</span></td><td class="num">${v.so}</td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(v.rev)}</td></tr>`).join('')}</tbody>`;

  // Segment donut — Food/Bev/Nestle from pencapaian
  const _soMk=(activeDate==='ALL'?RAW.latest:activeDate).slice(0,7);
  const _soTbd=(RAW.months[_soMk]||{}).targets_by_date||{};
  const _soDate=activeDate==='ALL'?RAW.latest:activeDate;
  const _soT=(_soTbd[_soDate]||{}).targets||{};
  const segFood=(_soT.FOOD&&_soT.FOOD.achievement)||0;
  const segBev=(_soT.BEVERAGE&&_soT.BEVERAGE.achievement)||0;
  const segNes=(_soT.NESTLE&&_soT.NESTLE.achievement)||0;
  const segEl=document.getElementById('ch-seg');
  if(segEl&&(segFood+segBev+segNes)>0){
    if(charts.seg)charts.seg.destroy();
    charts.seg=new Chart(segEl,{type:'doughnut',data:{
      labels:['Food','Beverage','Nestlé'],
      datasets:[{data:[segFood,segBev,segNes],
        backgroundColor:['#2563eb','#059669','#7c3aed'],
        borderWidth:0,hoverOffset:6}]
    },options:{responsive:true,maintainAspectRatio:false,
      plugins:{legend:{position:'bottom',labels:{color:'#8a93b0',font:{size:11},padding:12}},
        tooltip:{callbacks:{label:function(ctx){return' '+ctx.label+': '+fmtRp(ctx.parsed)+' ('+Math.round(ctx.parsed/(segFood+segBev+segNes)*100)+'%)';}}}
      }
    }});
  }

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

  // Insight banner
  const _worstArea=Object.entries(stats.by_area).sort((a,b)=>(b[1].t-b[1].ok)-(a[1].t-a[1].ok))[0];
  const _insightEl=document.getElementById('del-insight');
  if(_insightEl){
    if(stats.unf>0&&_worstArea){
      const _affected=new Set(del.filter(r=>r.ket==='UNFULFILLED').map(r=>r.customer)).size;
      const _pmap=_buildPriceMap();
      const _lostRev=del.filter(r=>r.ket==='UNFULFILLED').reduce((s,r)=>{
        const price=_pmap[r.product]||0;
        const qty=r.qty_bs||1;
        return s+(price>0?price*qty:0);
      },0);
      _insightEl.innerHTML=`<div style="background:var(--mku-l);border:1px solid var(--mku);border-radius:10px;padding:10px 16px;margin-bottom:12px;font-size:.75rem;display:flex;gap:12px;align-items:center"><span style="font-size:1.2rem">⚠️</span><span><strong>${stats.unf} unfulfilled lines</strong> · ${_affected} customers affected · <strong>${fmtRp(_lostRev)}</strong> at risk · Worst area: <strong>${_worstArea[0]}</strong> (${_worstArea[1].t-_worstArea[1].ok} issues)</span></div>`;
    } else {
      _insightEl.innerHTML=`<div style="background:var(--grn-l);border:1px solid var(--grn);border-radius:10px;padding:10px 16px;margin-bottom:12px;font-size:.75rem;display:flex;gap:12px;align-items:center"><span style="font-size:1.2rem">✅</span><span><strong>All ${stats.tot} deliveries fulfilled.</strong> No issues today.</span></div>`;
    }
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

function renderReps(){
  const agg=getAggSummary();
  document.getElementById('reps-lbl').textContent=(company==='ALL'?'All':company)+(activeDate==='ALL'?' · All days':' · '+fmtD(activeDate));
  const _HIDE_REPS=new Set(['Management Bali','Sales Retail','NP5','Unknown']);
  const reps=Object.entries(agg.rep_rev).filter(([n])=>!_HIDE_REPS.has(n)).sort((a,b)=>b[1]-a[1]);
  const max=reps[0]?.[1]||1;
  const divMap={};RAW.so.forEach(r=>{divMap[r.sales]=r.division;});

  // Day-over-day growth: get previous date's rep_rev
  const _rd=(RAW.dates||[]).slice().sort();
  const _ri=activeDate==='ALL'?_rd.length-1:_rd.indexOf(activeDate);
  const _prevD=_ri>0?_rd[_ri-1]:null;
  const _prevRR=_prevD?getSummary(_prevD).rep_rev:{};

  // Dropped-off customers: seen previously, not in today's SO
  const todayCusts=new Set(RAW.so.map(r=>r.customer));
  const prevCustsByRep={};
  (_rd.filter(d=>d!==RAW.latest)).forEach(d=>{
    const s=getSummary(d);
    Object.entries(s.cust||{}).forEach(([c,v])=>{
      if(!prevCustsByRep[v.sales])prevCustsByRep[v.sales]={};
      if(!prevCustsByRep[v.sales][c]||prevCustsByRep[v.sales][c].lastSeen<d)
        prevCustsByRep[v.sales][c]={lastSeen:d,rev:v.rev};
    });
  });

  // Insight banner
  const _rInsEl=document.getElementById('reps-insight');
  if(_rInsEl){
    const topRep=reps[0];
    const belowYest=reps.filter(([n,rev])=>_prevD&&(_prevRR[n]||0)>0&&rev<_prevRR[n]).length;
    const droppedCount=reps.filter(([n])=>Object.entries(prevCustsByRep[n]||{}).some(([c])=>!todayCusts.has(c))).length;
    _rInsEl.innerHTML=`<div style="background:var(--bg);border:1px solid var(--bdr);border-radius:10px;padding:10px 16px;margin-bottom:12px;font-size:.75rem;display:flex;gap:16px;align-items:center;flex-wrap:wrap">
      <span>🏆 <strong>Top rep:</strong> ${topRep?topRep[0]+' · '+fmtRp(topRep[1]):'—'}</span>
      ${belowYest>0?`<span style="color:var(--mku)">📉 <strong>${belowYest} rep${belowYest>1?'s':''}</strong> below yesterday</span>`:'<span style="color:var(--grn)">📈 All reps up vs yesterday</span>'}
      ${droppedCount>0?`<span style="color:var(--org)">⚠️ <strong>${droppedCount} rep${droppedCount>1?'s':''}</strong> with dropped customers</span>`:'<span style="color:var(--grn)">✅ No dropped customers</span>'}
    </div>`;
  }

  document.getElementById('tbl-reps').innerHTML=`<thead><tr>
    <th>#</th><th>Rep</th><th>Div</th>
    <th class="num">Revenue</th>
    <th class="num">Daily &plusmn;</th>
    <th class="num">Orders</th><th class="num">Customers</th>
    <th>Biggest Customer</th><th>⚠ Dropped Off</th>
    <th style="width:80px">vs Top</th>
  </tr></thead><tbody>${reps.map(([n,rev],i)=>{
    const div=divMap[n]||'—';
    let orders=0;const custRevMap={};
    (activeDate==='ALL'?RAW.dates:[activeDate]).forEach(d=>{
      const s=getSummary(d);
      Object.entries(s.cust||{}).forEach(([c,v])=>{if(v.sales===n){orders+=v.so;custRevMap[c]=(custRevMap[c]||0)+v.rev;}});
    });
    const custList=Object.entries(custRevMap).sort((a,b)=>b[1]-a[1]);
    const biggest=custList[0];
    const droppedList=Object.entries(prevCustsByRep[n]||{})
      .filter(([c])=>!todayCusts.has(c))
      .sort((a,b)=>b[1].rev-a[1].rev);
    const dropped=droppedList[0];
    // Growth vs previous date
    const prevRev=_prevRR[n]||0;
    let growthHtml='<span style="color:var(--txt3);font-size:.65rem">—</span>';
    if(_prevD&&prevRev>0){
      const diff=rev-prevRev,gPct=Math.round(diff/prevRev*100);
      const gPctCap=Math.min(Math.abs(gPct),999)*(gPct>=0?1:-1);
      const col=gPct>0?'var(--grn)':gPct<0?'var(--mku)':'var(--txt3)';
      const arrow=gPct>0?'▲':gPct<0?'▼':'';
      growthHtml=`<div style="font-weight:700;color:${col};font-size:.78rem;white-space:nowrap">${arrow} ${Math.abs(gPctCap)}%${Math.abs(gPct)>999?'+':''}</div><div style="font-size:.6rem;color:var(--txt3)">${diff>0?'+':''}${fmtRp(diff)}</div>`;
    } else if(_prevD&&prevRev===0&&rev>0){
      growthHtml=`<div style="font-weight:700;color:var(--grn);font-size:.72rem">🆕 New</div>`;
    }
    return`<tr>
      <td style="font-weight:700">${i===0?'🥇':i===1?'🥈':i===2?'🥉':i+1}</td>
      <td style="font-weight:700;color:${i===0?'var(--mks)':'var(--txt)'}">${n}</td>
      <td><span class="badge ${div==='MKU Bali'?'b-mku':'b-mks'}">${div==='MKU Bali'?'MKU':div==='MKS Bali'?'MKS':'—'}</span></td>
      <td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(rev)}</td>
      <td class="num">${growthHtml}</td>
      <td class="num">${orders||'—'}</td>
      <td class="num">${custList.length||'—'}</td>
      <td style="font-size:.65rem;max-width:120px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${biggest?`<span style="font-weight:600">${biggest[0]}</span><br><span style="color:var(--txt3)">${fmtRp(biggest[1])}</span>`:'—'}</td>
      <td style="font-size:.65rem;max-width:110px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${dropped?`<span style="color:var(--mku);font-weight:600">⚠ ${dropped[0]}</span><br><span style="color:var(--txt3);font-size:.58rem">last ${fmtD(dropped[1].lastSeen)}</span>`:'<span style="color:var(--grn);font-size:.63rem">✓ all active</span>'}</td>
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

  const _totalAlerts=outCnt+critCnt+stats.unf;
  document.getElementById('alerts-summary').innerHTML=`
    <div style="background:${outCnt>0?'var(--mku-l)':'var(--grn-l)'};border:1px solid ${outCnt>0?'var(--mku)':'var(--grn)'};border-radius:12px;padding:16px;display:flex;align-items:center;gap:14px">
      <div style="font-size:2rem">${outCnt>0?'🔴':'✅'}</div>
      <div style="flex:1">
        <div style="font-size:.6rem;font-weight:700;color:${outCnt>0?'var(--mku)':'var(--grn)'};text-transform:uppercase;letter-spacing:.05em">Out of Stock</div>
        <div style="font-size:1.8rem;font-weight:800;line-height:1.1;color:${outCnt>0?'var(--mku)':'var(--grn)'}">${outCnt}</div>
        <div style="font-size:.65rem;color:var(--txt3);margin-top:2px">${outCnt>0?'Action: reorder immediately':'All SKUs in stock'}</div>
      </div>
    </div>
    <div style="background:${critCnt>0?'var(--org-l)':'var(--grn-l)'};border:1px solid ${critCnt>0?'var(--org)':'var(--grn)'};border-radius:12px;padding:16px;display:flex;align-items:center;gap:14px">
      <div style="font-size:2rem">${critCnt>0?'⚠️':'✅'}</div>
      <div style="flex:1">
        <div style="font-size:.6rem;font-weight:700;color:${critCnt>0?'var(--org)':'var(--grn)'};text-transform:uppercase;letter-spacing:.05em">Critical / Low</div>
        <div style="font-size:1.8rem;font-weight:800;line-height:1.1;color:${critCnt>0?'var(--org)':'var(--grn)'}">${critCnt}</div>
        <div style="font-size:.65rem;color:var(--txt3);margin-top:2px">${critCnt>0?'Action: plan reorder this week':'Stock levels healthy'}</div>
      </div>
    </div>
    <div style="background:${stats.unf>0?'var(--org-l)':'var(--grn-l)'};border:1px solid ${stats.unf>0?'var(--org)':'var(--grn)'};border-radius:12px;padding:16px;display:flex;align-items:center;gap:14px">
      <div style="font-size:2rem">${stats.unf>0?'🚫':'✅'}</div>
      <div style="flex:1">
        <div style="font-size:.6rem;font-weight:700;color:${stats.unf>0?'var(--org)':'var(--grn)'};text-transform:uppercase;letter-spacing:.05em">Unfulfilled Orders</div>
        <div style="font-size:1.8rem;font-weight:800;line-height:1.1;color:${stats.unf>0?'var(--org)':'var(--grn)'}">${stats.unf}</div>
        <div style="font-size:.65rem;color:var(--txt3);margin-top:2px">${stats.unf>0?'Action: contact customers today':'All orders delivered'}</div>
      </div>
    </div>`;

  // Get unfulfilled delivery issues — read from correct nested path
  const unfI=[];
  (activeDate==='ALL'?RAW.dates:[activeDate]).forEach(d=>{
    const _mk=d.slice(0,7);const _mo=RAW.months[_mk]||{};
    const _dbd=_mo.delivery_by_date||RAW.delivery_by_date||{};
    const dd=_dbd[d];if(!dd)return;
    if(isLatest(d)){[...(dd.mku_full||[]),...(dd.mks_full||[])].filter(r=>r.ket==='UNFULFILLED').forEach(r=>unfI.push({...r,date:d}));}
    else{(dd.issues||[]).forEach(r=>unfI.push({...r,date:d}));}
  });

  const secs=[
    {id:'a-out',ic:'🔴',tt:'Out of Stock — Reorder Immediately',cnt:outI.length,cc:outI.length?'red':'grn',items:outI.length?outI.map(s=>`
      <div style="display:flex;align-items:center;padding:8px 12px;border-bottom:1px solid var(--bg);gap:10px">
        <div style="width:8px;height:8px;border-radius:50%;background:var(--mku);flex-shrink:0"></div>
        <div style="flex:1;min-width:0">
          <div style="font-weight:600;font-size:.72rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${s.name||''}</div>
          <div style="font-size:.62rem;color:var(--txt3)">${s.code||''} · avg ${((s.avg3m||0)).toFixed(0)} ${s.unit||''}/mo</div>
        </div>
        <span style="font-size:.6rem;font-weight:700;background:var(--mku-l);color:var(--mku);padding:2px 7px;border-radius:4px;flex-shrink:0">${s.co}</span>
      </div>`):['<div style="padding:12px;color:var(--grn);font-size:.74rem;text-align:center">✅ No out-of-stock items</div>']},
    {id:'a-crit',ic:'🚨',tt:'Critical — Less Than 3 Days Left',cnt:critI.length,cc:critI.length?'red':'grn',items:critI.length?critI.map(s=>`
      <div style="display:flex;align-items:center;padding:8px 12px;border-bottom:1px solid var(--bg);gap:10px">
        <div style="width:8px;height:8px;border-radius:50%;background:var(--mku);flex-shrink:0"></div>
        <div style="flex:1;min-width:0">
          <div style="font-weight:600;font-size:.72rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${s.name||''}</div>
          <div style="font-size:.62rem;color:var(--mku);font-weight:600">${fmtQ(s.saldo||0)} ${s.unit||''} left · ${(s.buf||0)>0?(s.buf||0).toFixed(1)+' days':'<1 day'}</div>
        </div>
        <span style="font-size:.6rem;font-weight:700;background:var(--mku-l);color:var(--mku);padding:2px 7px;border-radius:4px;flex-shrink:0">${s.co}</span>
      </div>`):['<div style="padding:12px;color:var(--grn);font-size:.74rem;text-align:center">✅ No critical items</div>']},
    {id:'a-low',ic:'⚠️',tt:'Low Stock — 3 to 7 Days Left',cnt:lowI.length,cc:lowI.length?'org':'grn',items:lowI.length?lowI.map(s=>`
      <div style="display:flex;align-items:center;padding:8px 12px;border-bottom:1px solid var(--bg);gap:10px">
        <div style="width:8px;height:8px;border-radius:50%;background:var(--org);flex-shrink:0"></div>
        <div style="flex:1;min-width:0">
          <div style="font-weight:600;font-size:.72rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${s.name||''}</div>
          <div style="font-size:.62rem;color:var(--org);font-weight:600">${fmtQ(s.saldo||0)} ${s.unit||''} left · ${(s.buf||0).toFixed(1)} days</div>
        </div>
        <span style="font-size:.6rem;font-weight:700;background:var(--org-l);color:var(--org);padding:2px 7px;border-radius:4px;flex-shrink:0">${s.co}</span>
      </div>`):['<div style="padding:12px;color:var(--grn);font-size:.74rem;text-align:center">✅ No low-stock items</div>']},
    {id:'a-unf',ic:'🚫',tt:'Unfulfilled Deliveries — Contact Customers',cnt:unfI.length,cc:unfI.length?'red':'grn',items:unfI.length?unfI.map(r=>`
      <div style="display:flex;align-items:center;padding:8px 12px;border-bottom:1px solid var(--bg);gap:10px">
        <div style="width:8px;height:8px;border-radius:50%;background:var(--mku);flex-shrink:0"></div>
        <div style="flex:1;min-width:0">
          <div style="font-weight:600;font-size:.72rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${r.customer||'—'}</div>
          <div style="font-size:.62rem;color:var(--txt3)">${r.product||'—'} · <span style="color:var(--mku);font-weight:700">not sent</span> · ${r.sales||'—'}</div>
        </div>
        <span style="font-size:.6rem;font-weight:700;background:${(r.co||'')=='MKU'?'var(--mku-l)':'var(--mks-l)'};color:${(r.co||'')=='MKU'?'var(--mku)':'var(--mks)'};padding:2px 7px;border-radius:4px;flex-shrink:0">${r.co||'—'}</span>
      </div>`):['<div style="padding:12px;color:var(--grn);font-size:.74rem;text-align:center">✅ All orders delivered</div>']},
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
    ws['!cols']=cols;
    ws['!freeze']={xSplit:0,ySplit:1};
    XLSX.utils.book_append_sheet(wb,ws,name);
  };
  // Sheet 1: Sales Orders
  const soRows=getSO();
  addSheet('Sales Orders',
    ['Date','No SO','Division','Customer','Sales Rep','Product','SO Qty','Unit','FJ Qty','Revenue (Rp)','Type','Status'],
    soRows.map(r=>[r.date,r.no_so,r.division,r.customer,r.sales,r.product,r.so_pcs,r.unit,r.fj_pcs,Math.round(r.revenue),r.type,r.status])
  );
  // Sheet 2: Delivery
  const delRows=getDel();
  addSheet('Delivery',
    ['Date','Division','Area','Customer','Sales Rep','Product','Qty','Unit','Status'],
    delRows.map(r=>[r.date||RAW.latest,r.co||'',r.area||'',r.customer||'',r.sales||'',r.product||'',r.qty_bs||0,r.unit||'',r.ket||''])
  );
  // Sheet 3: Stock
  const stk=getStk();
  addSheet('Stock',
    ['Division','Code','Product','Unit','Stock Qty','Avg/Month','Buffer Days','Status'],
    stk.map(s=>[s.co,s.code||s.c||'',s.name||s.n||'',s.unit||s.u||'',s.saldo||s.s||0,Math.round(s.avg3m||s.a||0),(s.buf||s.bf||0)>0?parseFloat((s.buf||s.bf||0).toFixed(1)):0,s.st.toUpperCase()])
  );
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
  const lastDate=activeDate==='ALL'?RAW.latest:activeDate;
  const[_ty2,_tm2,_td2]=lastDate.split('-').map(Number);
  const dayNum=_td2;
  const daysInMonth=new Date(_ty2,_tm2,0).getDate();
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
  w.document.write(htmlStr);
  w.document.close();
  setTimeout(()=>w.print(),800);
  document.querySelectorAll('.dl-wrap').forEach(w=>w.classList.remove('open'));
}

// ── customers.js loader ───────────────────────────────────────────────────────
let _CUST=null;
function loadCustomers(cb){
  if(_CUST){cb();return;}
  const s=document.createElement('script');
  s.src='customers.js';
  s.onload=function(){_CUST=window.CUSTOMERS||null;cb();};
  s.onerror=function(){console.warn('customers.js not loaded');cb();};
  document.body.appendChild(s);
}

// ── Month-on-Month ────────────────────────────────────────────────────────────
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
  const curDN=curDates.length?parseInt(curDates[curDates.length-1].split('-')[2]):1;
  const prevDN=prevDates.length?parseInt(prevDates[prevDates.length-1].split('-')[2]):1;
  const curDIM=new Date(parseInt(curKey.split('-')[0]),parseInt(curKey.split('-')[1]),0).getDate();
  // Use pencapaian totals (Food+Bev+Nestle) for MoM — matches management reporting
  const _curLastDate=curDates[curDates.length-1];
  const _prevLastDate=prevDates[prevDates.length-1];
  const _curT=Object.values((curMo.targets_by_date||{})[_curLastDate]?.targets||{});
  const _prevT=Object.values((prevMo.targets_by_date||{})[_prevLastDate]?.targets||{});
  const _curRevTgt=_curT.reduce((s,t)=>s+(t.achievement||0),0);
  const _prevRevTgt=_prevT.reduce((s,t)=>s+(t.achievement||0),0);
  // Fallback to SO summary if pencapaian not available
  const _curRevSO=Object.values(curMo.so_summary||{}).reduce((s,d)=>s+(d.rev||0),0);
  const _prevRevSO=Object.values(prevMo.so_summary||{}).reduce((s,d)=>s+(d.rev||0),0);
  const curRev=_curRevTgt>0?_curRevTgt:_curRevSO;
  const prevRev=_prevRevTgt>0?_prevRevTgt:_prevRevSO;
  const _curMonthStart=curKey+'-01';
  const _prevMonthStart=prevKey+'-01';
  const _curElapsed=_workDays(_curMonthStart,_curLastDate);
  const _prevElapsed=_workDays(_prevMonthStart,_prevLastDate);
  const curRate=_curElapsed>0?curRev/_curElapsed:0;
  const prevRate=_prevElapsed>0?prevRev/_prevElapsed:0;
  const rateChg=prevRate>0?Math.round((curRate-prevRate)/prevRate*100):0;
  const col=rateChg>=0?'var(--grn)':'var(--mku)';
  const curTgt=Object.values((curMo.targets_by_date||{})[curDates[curDates.length-1]]?.targets||{}).reduce((s,t)=>s+(t.target||0),0)||0;
  const _projY=parseInt(curKey.split('-')[0]),_projM=parseInt(curKey.split('-')[1]);
  const _monthStart=curKey+'-01';
  const _projEnd=new Date(_projY,_projM,0).toISOString().slice(0,10);
  const _projStart=curKey+'-'+String(curDN+1).padStart(2,'0');
  const _elapsedWork=_workDays(_monthStart,curDates[curDates.length-1]);
  const daysLeft=curDN<curDIM?_workDays(_projStart,_projEnd):0;
  const curRateWork=_elapsedWork>0?curRev/_elapsedWork:0;
  const projRev=curRev+(curRateWork*daysLeft);
  const reqPace=curTgt>0&&daysLeft>0?Math.round((curTgt-curRev)/daysLeft):0;
  const projPct=curTgt>0?Math.round(projRev/curTgt*100):0;
  const projCol=projPct>=100?'var(--grn)':projPct>=80?'var(--org)':'var(--mku)';
  momEl.innerHTML=`<div class="card" style="margin-bottom:14px"><div class="card-hdr"><div class="card-title"><div class="ci mks">📈</div>Month-on-Month Run Rate</div><span class="card-sub">${prevMo.label||prevKey} → ${curMo.label||curKey}</span></div><div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px"><div style="text-align:center;padding:12px;background:var(--bg);border-radius:10px"><div style="font-size:.6rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:6px">${prevMo.label||prevKey}</div><div style="font-size:1rem;font-weight:800">${fmtRp(prevRev)}</div><div style="font-size:.63rem;color:var(--txt3);margin-top:3px">${fmtRp(prevRate)}/day · ${prevDates.length} days</div></div><div style="text-align:center;padding:12px;background:var(--bg);border-radius:10px"><div style="font-size:.6rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:6px">${curMo.label||curKey} (${curDN} days)</div><div style="font-size:1rem;font-weight:800">${fmtRp(curRev)}</div><div style="font-size:.63rem;color:var(--txt3);margin-top:3px">${fmtRp(curRate)}/day</div></div><div style="text-align:center;padding:12px;background:var(--bg);border-radius:10px"><div style="font-size:.6rem;font-weight:700;color:var(--txt3);text-transform:uppercase;margin-bottom:6px">Run Rate Change</div><div style="font-size:1.4rem;font-weight:800;color:${col}">${rateChg>=0?'▲':'▼'} ${Math.abs(rateChg)}%</div><div style="font-size:.63rem;color:var(--txt3);margin-top:3px">${fmtRp(curRate)}/day vs ${fmtRp(prevRate)}/day</div></div><div style="text-align:center;padding:12px;background:var(--mks-l);border-radius:10px;border:1px solid #c7d8fc"><div style="font-size:.6rem;font-weight:700;color:var(--mks);text-transform:uppercase;margin-bottom:6px">Projected Month-End</div><div style="font-size:1.1rem;font-weight:800;color:${projCol}">${fmtRp(projRev)}</div><div style="font-size:.7rem;font-weight:800;color:${projCol};margin-top:2px">${projPct}% of target</div><div style="font-size:.6rem;color:var(--txt3);margin-top:3px">${reqPace>0?'Need '+fmtRp(reqPace)+'/day · ':''} ${daysLeft} days left</div></div></div></div>`;
}

// ── Business tab ──────────────────────────────────────────────────────────────
function renderBusiness(){
  loadCustomers(function(){
    const el1=document.getElementById('biz-area');
    if(el1&&_CUST){
      const areas=_CUST.areas||{};
      const months=_CUST.months||[];
      const rows=Object.values(areas).sort((a,b)=>b.total-a.total);
      const cols=months.slice(-3);

      // Insight: total customers, active this month, dropped off
      const allCusts=Object.values(CUSTOMERS.by_rep||{}).flatMap(r=>Object.values(r.customers||{}));
      const latestMon=months[months.length-1];
      const prevMon=months[months.length-2];
      const activeNow=allCusts.filter(c=>c.last_month===latestMon).length;
      const droppedOff=allCusts.filter(c=>c.last_month&&c.last_month!==latestMon&&c.monthly[prevMon]>0).length;
      const topCust=allCusts.sort((a,b)=>b.total-a.total)[0];

      el1.innerHTML=`
      <div style="background:var(--bg);border:1px solid var(--bdr);border-radius:12px;padding:12px 16px;margin-bottom:14px;display:flex;gap:24px;flex-wrap:wrap;font-size:.75rem">
        <span>👥 <strong>${allCusts.length}</strong> total customers</span>
        <span style="color:var(--grn)">✅ <strong>${activeNow}</strong> active in ${latestMon}</span>
        ${droppedOff>0?`<span style="color:var(--mku)">⚠️ <strong>${droppedOff}</strong> dropped off vs last month</span>`:''}
        ${topCust?`<span>🏆 Top: <strong>${topCust.name}</strong> · ${fmtRp(topCust.total)}</span>`:''}
      </div>
      <div class="card"><div class="card-hdr"><div class="card-title"><div class="ci grn">📍</div>Area Performance — Monthly Revenue</div></div><div class="tbl-wrap"><table class="tbl"><thead><tr><th>Area</th><th>Div</th>${cols.map(m=>`<th class="num">${m.slice(0,3)}</th>`).join('')}<th class="num">Total</th><th>Trend</th></tr></thead><tbody>${rows.map(a=>{const vals=cols.map(m=>a.monthly[m]||0);const last=vals[vals.length-1],prev=vals[vals.length-2]||0;const trend=prev>0?Math.round((last-prev)/prev*100):0;const col=trend>0?'var(--grn)':trend<0?'var(--mku)':'var(--txt3)';const arrow=trend>0?'▲'+trend+'%':trend<0?'▼'+Math.abs(trend)+'%':'—';return`<tr><td style="font-weight:600;font-size:.7rem">${a.name}</td><td style="font-size:.63rem;color:var(--txt3)">${(a.division||'').replace(' Bali','')}</td>${vals.map(v=>`<td class="num">${fmtRp(v)}</td>`).join('')}<td class="num" style="font-weight:700">${fmtRp(a.total)}</td><td style="font-weight:700;color:${col};font-size:.7rem">${arrow}</td></tr>`;}).join('')}</tbody></table></div></div>`;
    }
    const el2=document.getElementById('biz-seg');
    if(el2&&_CUST){
      const segs=_CUST.segments||{};
      const rows=Object.entries(segs).sort((a,b)=>b[1].total-a[1].total);
      const tot=rows.reduce((s,[,v])=>s+v.total,0);
      el2.innerHTML=`<div class="card"><div class="card-hdr"><div class="card-title"><div class="ci pur">🏷️</div>Customer Segment Breakdown</div></div><div class="tbl-wrap"><table class="tbl"><thead><tr><th>Segment</th><th class="num">Customers</th><th class="num">Total Revenue</th><th class="num">% of Total</th></tr></thead><tbody>${rows.map(([seg,v])=>`<tr><td style="font-weight:600">${seg}</td><td class="num">${v.cust_count}</td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(v.total)}</td><td class="num">${tot>0?Math.round(v.total/tot*100):0}%</td></tr>`).join('')}</tbody></table></div></div>`;
    }
    const el3=document.getElementById('biz-cust');
    if(el3&&_CUST) renderCustomerSearch('');
  });
}

function renderCustomerSearch(q){
  const el=document.getElementById('biz-cust');
  if(!el||!_CUST)return;
  const byRep=_CUST.by_rep||{};
  let all=[];
  Object.entries(byRep).forEach(([rep,rd])=>{Object.entries(rd.customers||{}).forEach(([code,c])=>{all.push({code,rep,...c});});});
  if(q)all=all.filter(c=>c.name.toLowerCase().includes(q.toLowerCase())||c.rep.toLowerCase().includes(q.toLowerCase()));
  all.sort((a,b)=>b.total-a.total);
  const _latMon=(_CUST.months||[]).slice(-1)[0]||'May';
  const _prevMon=(_CUST.months||[]).slice(-2,-1)[0]||'';
  el.innerHTML=`<div class="card"><div class="card-hdr"><div class="card-title"><div class="ci org">👥</div>Customer Profiles</div><span class="card-sub">${all.length} customers</span></div><div style="margin-bottom:12px;padding:12px 16px 0"><input type="text" value="${q||''}" placeholder="Search customer or rep..." oninput="renderCustomerSearch(this.value)" style="width:100%;padding:8px 12px;border:1px solid var(--bdr);border-radius:8px;font-size:.75rem;font-family:inherit"></div><div class="tbl-wrap"><table class="tbl"><thead><tr><th>Customer</th><th>Rep</th><th>Segment</th><th class="num">Total Spend</th><th>Last Order</th><th>Status</th></tr></thead><tbody>${all.slice(0,50).map(c=>{const isActive=c.last_month===_latMon;const isRecent=c.last_month===_prevMon;const statusCol=isActive?'var(--grn)':isRecent?'var(--org)':'var(--mku)';const statusLbl=isActive?'Active':isRecent?'Last month':'Inactive';return`<tr><td style="font-weight:600;font-size:.7rem">${c.name}</td><td style="font-size:.65rem;color:var(--txt2)">${c.rep}</td><td style="font-size:.63rem">${c.group||'—'}</td><td class="num" style="font-weight:700;color:var(--mks)">${fmtRp(c.total)}</td><td style="font-size:.65rem;color:var(--txt3)">${c.last_month||'—'}</td><td><span style="font-size:.6rem;font-weight:700;color:${statusCol};background:${isActive?'var(--grn-l)':isRecent?'var(--org-l)':'var(--mku-l)'};padding:2px 7px;border-radius:4px">${statusLbl}</span></td></tr>`;}).join('')}</tbody></table></div></div>`;
}

function switchTabBiz(){switchTab('biz');renderBusiness();}
function mobileTabBiz(){mobileTab('biz');renderBusiness();}

renderAll();
renderMoM();

