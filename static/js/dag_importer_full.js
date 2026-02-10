(function(){

  function setVal(inp, v){
    if(!inp) return;
    if(inp.type === 'checkbox'){
      inp.checked = !!v;
      return;
    }
    if(typeof v === 'object' && v !== null){
      inp.value = JSON.stringify(v);
      return;
    }
    inp.value = (v === null || v === undefined) ? '' : String(v);
  }

  function clearContainer(id){
    const el = document.getElementById(id);
    if(el) el.innerHTML = '';
  }

  function extractExternalDeps(spec){
    const tasks = Array.isArray(spec.tasks) ? [...spec.tasks] : [];
    const deps  = Array.isArray(spec.dependencies) ? [...spec.dependencies] : [];

    const byUp = new Map();
    deps.forEach(e=>{
      const u = e?.upstream || '';
      if(!byUp.has(u)) byUp.set(u, []);
      byUp.get(u).push(e?.downstream || '');
    });

    const extdeps = [];
    const keepTasks = [];
    const removeTaskIds = new Set();

    for(const t of tasks){
      if(t?.type === 'ExternalTaskSensor'){
        const tid = t.task_id || '';
        const outs = byUp.get(tid) || [];
        // Heurística: si tiene external_dag_id y tiene downstream, lo movemos a UI extdeps
        if(t.external_dag_id && outs.length){
          extdeps.push({
            external_dag_id: t.external_dag_id,
            external_task_id: t.external_task_id || '',
            mode: t.mode || 'reschedule',
            poke_interval: t.poke_interval || 60,
            timeout: t.timeout || 3600,
            downstream: outs[0] || ''
          });
          removeTaskIds.add(tid);
          continue;
        }
      }
      keepTasks.push(t);
    }

    const keepDeps = deps.filter(e=>{
      const u = e?.upstream || '';
      const d = e?.downstream || '';
      return !(removeTaskIds.has(u) || removeTaskIds.has(d));
    });

    return { tasks: keepTasks, dependencies: keepDeps, extdeps };
  }

  function applyScheduleFromSpec(spec){
    const kindSel = document.getElementById('schedule_kind');
    const tzSel   = document.getElementById('sched_tz');
    const cronPrev = document.getElementById('cron_preview');

    const schedule = (spec?.schedule ?? '').toString().trim();
    const tz = (spec?.timezone || 'UTC').toString().trim();

    if(tzSel) tzSel.value = tz;
    if(window.builderState) window.builderState.timezone = tz;

    // Defaults
    const setKind = (k)=>{ if(kindSel) kindSel.value = k; };

    // None
    if(schedule === 'None' || schedule === '@none'){
      setKind('@none');
      document.getElementById('btn_build_cron')?.click();
      return;
    }

    // @once
    if(schedule === '@once'){
      setKind('@once');
      const st = (spec?.start_time || '').toString();
      const [hh, mm] = st.includes(':') ? st.split(':') : ['00','00'];
      const oh = document.getElementById('once_hh');
      const om = document.getElementById('once_mm');
      if(oh) oh.value = String(hh).padStart(2,'0');
      if(om) om.value = String(mm).padStart(2,'0');
      document.getElementById('btn_build_cron')?.click();
      return;
    }

    // Calendarios
    if(schedule.startsWith('CAL|') || schedule.startsWith('CALC|')){
      setKind('@calendar');
      const parts = schedule.split('|');
      const cal = parts[1] || '';
      const calSel = document.getElementById('calendar_name');
      if(calSel) calSel.value = cal;

      if(schedule.startsWith('CAL|')){
        // CAL|<cal>|HH:MM
        const at = (parts[2] || '00:00');
        const [hh, mm] = at.split(':');
        const th = document.getElementById('t_hh');
        const tm = document.getElementById('t_mm');
        if(th) th.value = hh;
        if(tm) tm.value = mm;
        const cyc = document.getElementById('cyclic_on');
        if(cyc) cyc.checked = false;
      }else{
        // CALC|<cal>|unit|N|from|to|at_mm
        const unit = parts[2] || 'minutes';
        const N    = parts[3] || '15';
        const F    = parts[4] || '0';
        const T    = parts[5] || '23';
        const atmm = parts[6] || '0';
        const cyc = document.getElementById('cyclic_on');
        if(cyc) cyc.checked = true;
        const cu = document.getElementById('cyclic_unit');
        const cn = document.getElementById('cyclic_n');
        const cf = document.getElementById('cyc_from');
        const ct = document.getElementById('cyc_to');
        const cam = document.getElementById('cyclic_at_mm');
        if(cu) cu.value = unit;
        if(cn) cn.value = N;
        if(cf) cf.value = F;
        if(ct) ct.value = T;
        if(cam) cam.value = atmm;
      }
      document.getElementById('btn_build_cron')?.click();
      return;
    }

    // MULTI|cron1||cron2...
    if(schedule.startsWith('MULTI|')){
      setKind('@times');
      const body = schedule.slice('MULTI|'.length);
      const crons = body.split('||').map(s=>s.trim()).filter(Boolean);
      const pre = document.getElementById('times_crons');
      if(pre) pre.textContent = crons.join('\n');

      // Intento: reconstruir times_list
      const times = [];
      let dowField = null;
      for(const c of crons){
        const f = c.split(/\s+/);
        if(f.length !== 5) continue;
        const mm = f[0];
        const hh = f[1].split(','); // puede venir "9,12"
        dowField = dowField ?? f[4];
        hh.forEach(h=> times.push(`${String(h).padStart(2,'0')}:${String(mm).padStart(2,'0')}`));
      }
      const tl = document.getElementById('times_list');
      if(tl) tl.value = [...new Set(times)].join(' ');

      // DOW checkboxes (si aplica)
      if(dowField){
        const set = new Set();
        if(dowField.includes('-')){
          const [a,b] = dowField.split('-').map(x=>parseInt(x,10));
          for(let i=a;i<=b;i++) set.add(String(i));
        }else if(dowField === '*'){
          ['0','1','2','3','4','5','6'].forEach(x=>set.add(x));
        }else{
          dowField.split(',').forEach(x=>set.add(x.trim()));
        }
        document.querySelectorAll('input[name="times_dow"]').forEach(cb=>{
          cb.checked = set.has(cb.value);
        });
      }

      document.getElementById('btn_build_cron')?.click();
      return;
    }

    // Cron “simple” => intentamos mapearlo a hourly/daily/weekly/monthly/yearly (+ cíclico daily/weekly)
    const fields = schedule.split(/\s+/);
    if(fields.length === 5){
      const [minF, hourF, domF, monF, dowF] = fields;

      // Cíclico daily/weekly
      // */N F-T * * *
      let m;
      m = schedule.match(/^\*\/(\d+)\s+(\d+)-(\d+)\s+\*\s+\*\s+(\*|\d+(?:[-,]\d+)*)$/);
      if(m){
        const N = m[1], F = m[2], T = m[3], DOW = m[4];
        const cyc = document.getElementById('cyclic_on'); if(cyc) cyc.checked = true;
        const cu  = document.getElementById('cyclic_unit'); if(cu) cu.value = 'minutes';
        const cn  = document.getElementById('cyclic_n'); if(cn) cn.value = N;
        const cf  = document.getElementById('cyc_from'); if(cf) cf.value = F;
        const ct  = document.getElementById('cyc_to'); if(ct) ct.value = T;

        if(DOW === '*' ){
          setKind('@daily');
        }else{
          setKind('@weekly');
          const w = document.getElementById('w_dow'); if(w) w.value = DOW;
        }
        document.getElementById('btn_build_cron')?.click();
        return;
      }

      // mm */N * * *  (hours step)
      m = schedule.match(/^(\d+)\s+\*\/(\d+)\s+\*\s+\*\s+(\*|\d+(?:[-,]\d+)*)$/);
      if(m){
        const atmm = m[1], N = m[2], DOW = m[3];
        const cyc = document.getElementById('cyclic_on'); if(cyc) cyc.checked = true;
        const cu  = document.getElementById('cyclic_unit'); if(cu) cu.value = 'hours';
        const cn  = document.getElementById('cyclic_n'); if(cn) cn.value = N;
        const cam = document.getElementById('cyclic_at_mm'); if(cam) cam.value = atmm;
        const cf  = document.getElementById('cyc_from'); if(cf) cf.value = '0';
        const ct  = document.getElementById('cyc_to'); if(ct) ct.value = '23';

        if(DOW === '*') setKind('@daily');
        else { setKind('@weekly'); const w = document.getElementById('w_dow'); if(w) w.value = DOW; }

        document.getElementById('btn_build_cron')?.click();
        return;
      }

      // no cíclico
      const cyc = document.getElementById('cyclic_on'); if(cyc) cyc.checked = false;

      // hourly: mm * * * *
      if(hourF === '*' && domF === '*' && monF === '*' && dowF === '*'){
        setKind('@hourly');
        const hm = document.getElementById('h_mm'); if(hm) hm.value = minF;
        document.getElementById('btn_build_cron')?.click();
        return;
      }

      // daily: mm hh * * *
      if(domF === '*' && monF === '*' && dowF === '*'){
        setKind('@daily');
        const th = document.getElementById('t_hh'); if(th) th.value = hourF;
        const tm = document.getElementById('t_mm'); if(tm) tm.value = minF;
        document.getElementById('btn_build_cron')?.click();
        return;
      }

      // weekly: mm hh * * dow
      if(domF === '*' && monF === '*' && dowF !== '*'){
        setKind('@weekly');
        const th = document.getElementById('t_hh'); if(th) th.value = hourF;
        const tm = document.getElementById('t_mm'); if(tm) tm.value = minF;
        const wd = document.getElementById('w_dow'); if(wd) wd.value = dowF;
        document.getElementById('btn_build_cron')?.click();
        return;
      }

      // monthly: mm hh dom * *
      if(monF === '*' && dowF === '*' && domF !== '*'){
        setKind('@monthly');
        const th = document.getElementById('t_hh'); if(th) th.value = hourF;
        const tm = document.getElementById('t_mm'); if(tm) tm.value = minF;
        const md = document.getElementById('m_dom'); if(md) md.value = domF;
        document.getElementById('btn_build_cron')?.click();
        return;
      }

      // yearly: mm hh dom mon *
      if(dowF === '*' && domF !== '*' && monF !== '*'){
        setKind('@yearly');
        const th = document.getElementById('t_hh'); if(th) th.value = hourF;
        const tm = document.getElementById('t_mm'); if(tm) tm.value = minF;
        const ym = document.getElementById('y_mon'); if(ym) ym.value = monF;
        const yd = document.getElementById('y_dom'); if(yd) yd.value = domF;
        document.getElementById('btn_build_cron')?.click();
        return;
      }
    }

    // Si no logramos mapear, al menos dejamos visible el cron
    if(cronPrev){
      cronPrev.textContent = schedule || '—';
      cronPrev.title = 'Cron importado (no mapeado a UI)';
    }
    if(window.builderState) window.builderState.schedule = schedule;
  }

  window.resetBuilderUI = function(){
    // General
    setVal(document.getElementById('dag_id'), '');
    setVal(document.getElementById('description'), '');
    setVal(document.getElementById('owner'), 'owner');
    setVal(document.getElementById('app'), '');
    setVal(document.getElementById('subapp'), '');
    setVal(document.getElementById('retries'), 1);
    setVal(document.getElementById('retry_minutes'), 5);
    // start_date: lo deja vacío para que tu “default hoy” aplique si existe
    setVal(document.getElementById('start_date'), '');

    const catchup = document.getElementById('catchup'); if(catchup) catchup.checked = false;
    const confirm = document.getElementById('confirm_on'); if(confirm) confirm.checked = false;

    // schedule defaults
    const kindSel = document.getElementById('schedule_kind'); if(kindSel) kindSel.value = '@daily';
    const tzSel   = document.getElementById('sched_tz'); if(tzSel) tzSel.value = (window.builderState?.timezone || 'UTC');

    // assets
    const ao = document.getElementById('assets_on'); if(ao) ao.checked = false;
    const al = document.getElementById('asset_logic'); if(al) al.value = 'OR';
    setVal(document.getElementById('asset_prefix'), '');

    // containers
    clearContainer('tasks');
    clearContainer('deps');
    clearContainer('extdeps');
    clearContainer('asset_inlets');
    clearContainer('asset_outlets');

    // state panels
    if(window.builderState){
      window.builderState.params = {};
      window.builderState.argsets = {};
    }
    window.__globalParamsMgr?.render?.();
    window.__globalArgsetsMgr?.render?.();

    // refresh selects + graph
    if(typeof refreshTaskSelects === 'function') refreshTaskSelects();
    if(typeof updateGraph === 'function') updateGraph();

    // output
    const oc = document.getElementById('out_code'); if(oc) oc.textContent = '';
    const ov = document.getElementById('out_val');  if(ov) ov.textContent = '';
    const st = document.getElementById('statusChip'); if(st) st.textContent = 'sin validar';

    document.getElementById('btn_build_cron')?.click();
  };

  window.applyImportedSpec = function(spec){
    window.resetBuilderUI?.();

    // General
    setVal(document.getElementById('dag_id'), spec?.dag_id || '');
    setVal(document.getElementById('description'), spec?.description || '');
    setVal(document.getElementById('owner'), spec?.owner || 'owner');
    setVal(document.getElementById('app'), spec?.app || '');
    setVal(document.getElementById('subapp'), spec?.subapp || '');
    setVal(document.getElementById('retries'), spec?.retries ?? 1);
    setVal(document.getElementById('retry_minutes'), spec?.retry_minutes ?? 5);
    setVal(document.getElementById('start_date'), spec?.start_date || '');

    const catchup = document.getElementById('catchup'); if(catchup) catchup.checked = !!spec?.catchup;
    const confirm = document.getElementById('confirm_on'); if(confirm) confirm.checked = !!spec?.confirm;

    // Globals
    if(window.builderState){
      window.builderState.params = spec?.params || {};
      window.builderState.argsets = spec?.argsets || {};
      if(spec?.timezone) window.builderState.timezone = spec.timezone;
      if(spec?.schedule) window.builderState.schedule = spec.schedule;
    }
    window.__globalParamsMgr?.render?.();
    window.__globalArgsetsMgr?.render?.();

    // Schedule
    applyScheduleFromSpec(spec);

    // Assets UI
    const ao = document.getElementById('assets_on'); if(ao) ao.checked = !!spec?.assets_enabled;
    const al = document.getElementById('asset_logic'); if(al) al.value = (spec?.asset_logic || 'OR').toUpperCase();
    // prefix (si venía en URIs, no siempre existe en spec)
    if(spec?.asset_prefix) setVal(document.getElementById('asset_prefix'), spec.asset_prefix);

    if(typeof setAssetsUiEnabled === 'function') setAssetsUiEnabled(!!spec?.assets_enabled);

    // Pre-proceso extdeps
    const { tasks, dependencies, extdeps } = extractExternalDeps(spec);

    // Tasks
    const taskHost = document.getElementById('tasks');
    (tasks || []).forEach(t=>{
      if(!t?.type) return;
      const before = taskHost?.lastElementChild || null;
      if(typeof addTask === 'function') addTask(t.type);
      const card = taskHost?.lastElementChild;
      if(!card || card === before) return;

      // set all [data-k]
      card.querySelectorAll('[data-k]').forEach(inp=>{
        const k = inp.getAttribute('data-k');
        if(!k) return;
        if(k === 'type') return;
        if(Object.prototype.hasOwnProperty.call(t, k)){
          setVal(inp, t[k]);
        }
      });
    });

    if(typeof refreshTaskSelects === 'function') refreshTaskSelects();

    // Deps
    clearContainer('deps');
    (dependencies || []).forEach(e=>{
      if(typeof addDep === 'function') addDep();
      const host = document.getElementById('deps');
      const card = host?.lastElementChild;
      if(!card) return;
      const sels = card.querySelectorAll('select[data-k="_sel"]');
      if(sels?.length >= 2){
        sels[0].value = e.upstream || '';
        sels[1].value = e.downstream || '';
      }
    });

    // Ext deps
    clearContainer('extdeps');
    (extdeps || []).forEach(x=>{
      if(typeof addExternalDep === 'function') addExternalDep();
      const host = document.getElementById('extdeps');
      const card = host?.lastElementChild;
      if(!card) return;
      card.querySelectorAll('[data-k]').forEach(inp=>{
        const k = inp.getAttribute('data-k');
        if(Object.prototype.hasOwnProperty.call(x, k)){
          setVal(inp, x[k]);
        }
      });
      // downstream select:
      const ds = card.querySelector('select[data-k="downstream"]');
      if(ds) ds.value = x.downstream || '';
    });

    if(typeof refreshTaskSelects === 'function') refreshTaskSelects();

    // Assets rows (reconstruir desde tasks.inlets/outlets)
    clearContainer('asset_inlets');
    clearContainer('asset_outlets');

    const byTask = new Map();
    (tasks || []).forEach(t=>{ if(t?.task_id) byTask.set(t.task_id, t); });

    for(const t of (tasks || [])){
      (t.inlets || []).forEach(uri=>{
        if(typeof addAssetLinkRow === 'function') addAssetLinkRow('asset_inlets', 'inlet');
        const host = document.getElementById('asset_inlets');
        const card = host?.lastElementChild;
        if(!card) return;
        const sel = card.querySelector('select[data-k="task_id"]');
        const inp = card.querySelector('input[data-k="uri"]');
        if(sel) sel.value = t.task_id;
        if(inp) inp.value = uri;
      });
      (t.outlets || []).forEach(uri=>{
        if(typeof addAssetLinkRow === 'function') addAssetLinkRow('asset_outlets', 'outlet');
        const host = document.getElementById('asset_outlets');
        const card = host?.lastElementChild;
        if(!card) return;
        const sel = card.querySelector('select[data-k="task_id"]');
        const inp = card.querySelector('input[data-k="uri"]');
        if(sel) sel.value = t.task_id;
        if(inp) inp.value = uri;
      });
    }

    if(typeof refreshTaskSelects === 'function') refreshTaskSelects();
    if(typeof updateGraph === 'function') updateGraph();
  };

  async function handleImportFile(file){
    const name = (file?.name || '').toLowerCase();

    if(name.endsWith('.json')){
      const txt = await file.text();
      const spec = JSON.parse(txt);
      window.applyImportedSpec?.(spec);
      return;
    }

    if(name.endsWith('.py')){
      const fd = new FormData();
      fd.append('file', file);
      const r = await fetch('/api/import-dag', { method:'POST', body: fd });
      if(!r.ok) throw new Error(await r.text());
      const spec = await r.json();
      window.applyImportedSpec?.(spec);
      return;
    }

    throw new Error('Formato no soportado: usa .py o .json');
  }

  function wireButtons(){
    const fileInp = document.getElementById('import_file');

    const openPicker = (e)=>{
      e?.preventDefault?.();
      if(!fileInp) return;
      fileInp.value = '';
      fileInp.click();
    };

    document.getElementById('btn_import')?.addEventListener('click', openPicker);
    document.getElementById('btn_import_footer')?.addEventListener('click', openPicker);

    document.getElementById('btn_clear')?.addEventListener('click', (e)=>{ e.preventDefault(); window.resetBuilderUI?.(); });
    document.getElementById('btn_clear_footer')?.addEventListener('click', (e)=>{ e.preventDefault(); window.resetBuilderUI?.(); });

    fileInp?.addEventListener('change', async (e)=>{
      const f = e.target.files?.[0];
      if(!f) return;
      try{
        await handleImportFile(f);
      }catch(err){
        console.error(err);
        alert(String(err?.message || err));
      }finally{
        e.target.value = '';
      }
    });
  }

  if (document.readyState === 'loading') {
    window.addEventListener('DOMContentLoaded', wireButtons, { once:true });
  } else {
    wireButtons();
  }

})();
