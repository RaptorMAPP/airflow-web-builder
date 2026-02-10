// static/js/dag_import_ui.js
(function(){
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

    throw new Error('Formato no soportado: usa .json o .py');
  }

  document.getElementById('btn_import')?.addEventListener('click', (e)=>{
    e.preventDefault();
    const f = document.getElementById('import_file');
    if(f){ f.value=''; f.click(); }
  });

  document.getElementById('import_file')?.addEventListener('change', async (e)=>{
    const file = e.target.files?.[0];
    if(!file) return;
    try{
      await handleImportFile(file);
    }catch(err){
      console.error(err);
      alert(String(err?.message || err));
    }finally{
      e.target.value = '';
    }
  });

  document.getElementById('btn_clear')?.addEventListener('click', (e)=>{
    e.preventDefault();
    window.resetBuilderUI?.();
  });
})();
