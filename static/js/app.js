const globalEvents = [];

function pushGlobal(level, msg){
  globalEvents.unshift({level, msg, ts: new Date().toLocaleTimeString([], {hour12:false})});
  if(globalEvents.length > 200) globalEvents.pop();
  renderGlobalLog();
}
function renderGlobalLog(){
  const el = document.getElementById('globalLog');
  if(!el) return;
  el.innerHTML = globalEvents.map(e =>
    `<div class="row"><span class="t">[${e.ts}]</span> <span class="${e.level}">${e.msg}</span></div>`
  ).join('');
}

async function api(path, opts={}){
  const res = await fetch(path, {
    headers: opts.body instanceof FormData
      ? {}
      : {'Content-Type':'application/json'},
    credentials:'same-origin',
    ...opts
  });

  const text = await res.text();

  let data = {};
  try {
    data = text ? JSON.parse(text) : {};
  } catch (_) {
    data = {};
  }

  if(!res.ok){
    throw new Error(
      data.error ||
      `Server error ${res.status}: ${text || res.statusText}`
    );
  }

  return data;
}

/* ---------------- Auth view switching ---------------- */
function showAuthMsg(text, ok){
  const el = document.getElementById('authMsg');
  el.textContent = text;
  el.className = 'form-msg ' + (ok ? 'success' : 'error');
}
document.getElementById('tabSignin').onclick = () => {
  document.getElementById('tabSignin').classList.add('active');
  document.getElementById('tabSignup').classList.remove('active');
  document.getElementById('signinFields').classList.remove('hidden');
  document.getElementById('signupFields').classList.add('hidden');
  document.getElementById('authTitle').textContent = 'Sign in to your vault';
  document.getElementById('authMsg').className = 'form-msg';
};
document.getElementById('tabSignup').onclick = () => {
  document.getElementById('tabSignup').classList.add('active');
  document.getElementById('tabSignin').classList.remove('active');
  document.getElementById('signupFields').classList.remove('hidden');
  document.getElementById('signinFields').classList.add('hidden');
  document.getElementById('authTitle').textContent = 'Create your vault';
  document.getElementById('authMsg').className = 'form-msg';
};

document.getElementById('signinBtn').onclick = async () => {
  const login_id = document.getElementById('loginId').value.trim();
  const password = document.getElementById('loginPass').value;
  const remember = document.getElementById('rememberMe').checked;
  try{
    const data = await api('/api/login', {method:'POST', body:JSON.stringify({login_id,password,remember})});
    enterApp(data.username);
  }catch(e){ showAuthMsg(e.message, false); }
};
document.getElementById('loginPass').addEventListener('keydown', e => { if(e.key==='Enter') document.getElementById('signinBtn').click(); });
document.getElementById('signupBtn').onclick = async () => {
  const username = document.getElementById('suUsername').value.trim();
  const email = document.getElementById('suEmail').value.trim();

  if (!/^[A-Za-z0-9._%+-]+@gmail\.com$/i.test(email)) {
    alert("Please enter a valid Gmail address ending with @gmail.com");
    return;
  }

  const password = document.getElementById('suPassword').value;

  // ADD THESE 3 LINES
  const confirmPassword = document.getElementById('suConfirmPassword').value;

  if (password !== confirmPassword) {
    alert("Passwords do not match.");
    return;
  }

  try {
    await api('/api/signup', {
      method: 'POST',
      body: JSON.stringify({username, email, password})
    });

    showAuthMsg('Account created — you can sign in now.', true);
    document.getElementById('tabSignin').click();
    document.getElementById('loginId').value = username;

  } catch(e) {
    showAuthMsg(e.message, false);
  }
};

/* ---------------- Forgot password ---------------- */
let pendingResetId = null;
document.getElementById('forgotLink').onclick = () => {
  document.getElementById('authView').classList.add('hidden');
  document.getElementById('forgotStep1').classList.remove('hidden');
  document.getElementById('forgotId').value = document.getElementById('loginId').value;
};
function backToSignin(){
  document.getElementById('forgotStep1').classList.add('hidden');
  document.getElementById('forgotStep2').classList.add('hidden');
  document.getElementById('authView').classList.remove('hidden');
}
document.getElementById('backFromForgot1').onclick = backToSignin;
document.getElementById('backFromForgot2').onclick = backToSignin;

document.getElementById('sendCodeBtn').onclick = async () => {
  const login_id = document.getElementById('forgotId').value.trim();
  const msg = document.getElementById('forgotMsg1');
  try{
    const data = await api('/api/forgot/request', {method:'POST', body:JSON.stringify({login_id})});
    pendingResetId = login_id;
    document.getElementById('resetCodeDisplay').textContent = data.demo_code;
    document.getElementById('forgotStep1').classList.add('hidden');
    document.getElementById('forgotStep2').classList.remove('hidden');
  }catch(e){ msg.textContent = e.message; msg.className='form-msg error'; }
};

document.getElementById('resetPassBtn').onclick = async () => {
  const code = document.getElementById('enteredCode').value.trim();
  const new_password = document.getElementById('newPass').value;
  const msg = document.getElementById('forgotMsg2');
  try{
    await api('/api/forgot/reset', {method:'POST', body:JSON.stringify({login_id:pendingResetId, code, new_password})});
    msg.textContent = 'Password updated — you can sign in now.'; msg.className='form-msg success';
    setTimeout(() => {
      backToSignin();
      document.getElementById('tabSignin').click();
      document.getElementById('loginId').value = pendingResetId;
      document.getElementById('loginPass').value = '';
    }, 900);
  }catch(e){ msg.textContent = e.message; msg.className='form-msg error'; }
};

/* ---------------- App shell ---------------- */
function enterApp(username){
    document.getElementById('loginScreen').style.display='none';
    document.getElementById('app').classList.add('shown');

    document.getElementById('whoami').textContent = username;

    // Show first letter of username inside avatar
    document.getElementById('userAvatar').textContent =
        username.charAt(0).toUpperCase();

    pushGlobal('ok', `Signed in as ${username}.`);
    switchView('overview');
    refreshEverything();
  setInterval(() => { if(document.getElementById('app').classList.contains('shown')) refreshNodesAndFiles(); }, 5000);
}

document.getElementById('logoutBtn').onclick = async () => {
  await api('/api/logout', {method:'POST'});
  document.getElementById('app').classList.remove('shown');
  document.getElementById('loginScreen').style.display='flex';
  document.getElementById('loginPass').value='';
  backToSignin();
};

document.querySelectorAll('.nav-item').forEach(item => {
  item.onclick = () => switchView(item.dataset.view);
});
function switchView(view){
  document.querySelectorAll('.nav-item').forEach(n => n.classList.toggle('active', n.dataset.view===view));
  document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
  const target = document.getElementById('view-' + view);
  if(target) target.classList.add('active');
  if(view === 'overview') loadOverview();
  if(view === 'nodes') loadNodes();
  if(view === 'files') loadFiles();
  if(view === 'sharing') loadShares();
  if(view === 'benchmarks') loadBenchmark();
  if(view === 'activity') renderGlobalLog();
}

async function refreshEverything(){ await Promise.all([loadOverview(), loadFiles(), loadNodes()]); }
async function refreshNodesAndFiles(){ await Promise.all([loadNodes(), loadFiles(), loadOverview()]); }

/* ---------------- Overview ---------------- */
async function loadOverview(){
  try{
    const d = await api('/api/overview');
    document.getElementById('statNodes').textContent = `${d.nodes_online}/${d.nodes_total}`;
    document.getElementById('statFiles').textContent = d.files;
    document.getElementById('statChunks').textContent = d.chunks;
    document.getElementById('statReplicas').textContent = d.replicas;
    document.getElementById('statShares').textContent = d.active_shares;

    const rh = d.replication_health;
    const total = Math.max(1, rh.healthy + rh.degraded + rh.critical);
    const healthyDeg = (rh.healthy/total)*360;
    const degradedDeg = (rh.degraded/total)*360;
    const ring = document.getElementById('replRing');
    ring.innerHTML = `<div class="ring" style="background:conic-gradient(
      var(--ok) 0deg ${healthyDeg}deg,
      var(--warn) ${healthyDeg}deg ${healthyDeg+degradedDeg}deg,
      var(--bad) ${healthyDeg+degradedDeg}deg 360deg
    )"><div class="ring-hole"><span>${rh.healthy}/${total}</span><small>healthy</small></div></div>`;
    document.getElementById('replHealthyCount').textContent = rh.healthy;
    document.getElementById('replDegradedCount').textContent = rh.degraded;
    document.getElementById('replCriticalCount').textContent = rh.critical;
  }catch(e){}
}

/* ---------------- Files ---------------- */
async function loadFiles(){
  try{
    const d = await api('/api/files');
    const grid = document.getElementById('fileGrid');
    if(d.files.length === 0){
      grid.innerHTML = `<div class="empty-note">No files yet — upload one above.</div>`;
      return;
    }
    grid.innerHTML = '';
    d.files.forEach(f => {
      const div = document.createElement('div');
      div.className = 'file-card';
      div.innerHTML = `
        <div class="fname">${f.name}</div>
        <div class="fmeta">${(f.size/1024).toFixed(1)} KB · ${f.chunk_count} chunk(s)</div>
        <div class="fstatus"><span class="dot ${f.health}"></span> ${f.health==='healthy'?'all replicas healthy':f.health==='degraded'?'running on replica':'chunk unavailable'}</div>
        ${f.cloud_backup ? '<div class="cloud-badge">☁ cloud backup enabled</div>' : ''}
      `;
      div.onclick = () => openFile(f.id);
      grid.appendChild(div);
    });
  }catch(e){}
}

const dz = document.getElementById('dropzone');
const fileInput = document.getElementById('fileInput');
dz.onclick = (e) => { if(e.target.tagName !== 'INPUT') fileInput.click(); };
fileInput.onchange = e => {
  if(e.target.files.length) handleMultipleUploads(Array.from(e.target.files));
  e.target.value = '';
};
['dragover','dragenter'].forEach(evt => dz.addEventListener(evt, e => { e.preventDefault(); dz.classList.add('drag'); }));
['dragleave','drop'].forEach(evt => dz.addEventListener(evt, e => { e.preventDefault(); dz.classList.remove('drag'); }));
dz.addEventListener('drop', e => { if(e.dataTransfer.files.length) handleMultipleUploads(Array.from(e.dataTransfer.files)); });

async function handleUpload(file){
  const fd = new FormData();
  fd.append('file', file);
  fd.append('cloud_backup', document.getElementById('cloudBackupCheck').checked ? '1' : '0');
  pushGlobal('info', `Uploading "${file.name}"...`);
  try{
    const d = await api('/api/upload', {method:'POST', body:fd});
    pushGlobal('ok', `"${file.name}" uploaded, rack-aware replicated, and load-balanced.`);
    return d.file_id;
  }catch(e){
    pushGlobal('bad', `Upload failed for "${file.name}": ${e.message}`);
    throw e;
  }
}
async function handleMultipleUploads(files){
  pushGlobal('info', `Starting ${files.length} upload(s) in parallel...`);
  const results = await Promise.allSettled(files.map(handleUpload));
  const success = results.filter(r => r.status === 'fulfilled').map(r => r.value);
  const failed = results.length - success.length;
  await loadFiles(); await loadOverview(); await loadNodes();
  if(success.length === 1) openFile(success[0]);
  pushGlobal(failed ? 'warn' : 'ok',
    `${success.length}/${files.length} upload(s) completed${failed ? `; ${failed} failed` : ''}.`);
}

/* ---------------- File detail ---------------- */
let currentFileId = null;
document.getElementById('backLink').onclick = () => switchView('files');

async function openFile(id){
  currentFileId = id;
  switchView('detail');
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('shareResult').style.display = 'none';
  clearLocalLog();
  const d = await api('/api/files/' + id);
  document.getElementById('detailFileName').textContent = d.file.name;
  document.getElementById('detailFileMeta').textContent =
    `${(d.file.size/1024).toFixed(1)} KB · ${d.chunks.length} chunk(s) · ${d.file.cloud_backup ? 'cloud backup enabled' : 'no cloud backup'}`;
  renderChunkTable(d.chunks);
}

document.getElementById('deleteFileBtn').onclick = async () => {
  if(currentFileId === null) return;
  const name = document.getElementById('detailFileName').textContent || 'this file';
  const confirmed = window.confirm(`Delete "${name}" permanently?\n\nThis removes all distributed chunks/replicas, share links, and any cloud backup for this file.`);
  if(!confirmed) return;
  try{
    await api('/api/files/' + currentFileId, {method:'DELETE'});
    pushGlobal('ok', `"${name}" and all of its replicas were deleted.`);
    currentFileId = null;
    switchView('files');
    await loadFiles();
    await loadOverview();
    await loadShares();
    await loadNodes();
  }catch(e){
    pushGlobal('bad', `Delete failed for "${name}": ${e.message}`);
  }
};

function renderChunkTable(chunks){
  const tbody = document.querySelector('#metaTable tbody');
  tbody.innerHTML = '';
  chunks.forEach(c => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>chunk_${c.index}</td>
      <td>${(c.size/1024).toFixed(1)} KB</td>
      <td><span class="badge primary">${c.primary_name}</span></td>
      <td><span class="badge replica">${c.replica_name}</span></td>
      <td class="hash">${c.hash.slice(0,14)}…</td>
      <td class="${c.corrupted ? 'status-bad' : 'status-ok'}">${c.corrupted ? 'corrupted ✕' : 'intact ✓'}</td>
      <td>${c.corrupted ? '' : `<button class="corrupt-btn" data-id="${c.id}">simulate corruption</button>`}</td>
    `;
    tbody.appendChild(tr);
  });
  document.querySelectorAll('.corrupt-btn').forEach(btn => {
    btn.onclick = async () => {
      await api('/api/chunks/' + btn.dataset.id + '/corrupt', {method:'POST'});
      pushGlobal('warn', `Chunk ${btn.dataset.id} corrupted (simulated bit-flip) for integrity-check demo.`);
      logLocal('warn', `Chunk manually corrupted — try "Reconstruct & download" to see the integrity check catch it.`);
      const d = await api('/api/files/' + currentFileId);
      renderChunkTable(d.chunks);
    };
  });
}

document.getElementById('downloadBtn').onclick = async () => {
  logLocal('accent', 'Reconstructing file...');
  const res = await fetch('/api/files/' + currentFileId + '/download');
  if(!res.ok){
    const d = await res.json().catch(()=>({}));
    (d.events||[]).forEach(e => logLocal(e.level, e.msg));
    logLocal('bad', d.error || 'Reconstruction failed.');
    alert(d.error || 'Reconstruction failed — no available copies.');
    return;
  }
  const blob = await res.blob();
  const disposition = res.headers.get('Content-Disposition') || '';
  const match = disposition.match(/filename="?([^"]+)"?/);
  const filename = match ? match[1] : 'download';
  logLocal('ok', 'All chunks retrieved and verified. Triggering download.');
  pushGlobal('ok', `"${filename}" reconstructed and downloaded.`);
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = filename;
  document.body.appendChild(a); a.click(); a.remove(); URL.revokeObjectURL(url);
};

document.getElementById('shareBtn').onclick = async () => {
  const password = document.getElementById('sharePassword').value.trim();
  const expires_hours = document.getElementById('shareExpiry').value || null;
  const max_downloads = document.getElementById('shareLimit').value || null;
  const d = await api('/api/files/' + currentFileId + '/share', {
    method:'POST',
    body: JSON.stringify({password, expires_hours, max_downloads})
  });
  const full = window.location.origin + d.url;
  const box = document.getElementById('shareResult');
  box.style.display = 'block';
  box.innerHTML = `Share link${d.password_protected ? ' (password protected)' : ''}: <a href="${full}" target="_blank">${full}</a>`;
  pushGlobal('accent', `Share link created for the current file${d.password_protected ? ' with a password' : ''}.`);
};

function clearLocalLog(){ document.getElementById('log').innerHTML=''; }
function logLocal(level, msg){
  const el = document.getElementById('log');
  const row = document.createElement('div');
  row.className = 'row';
  row.innerHTML = `<span class="t">[${new Date().toLocaleTimeString([], {hour12:false})}]</span> <span class="${level}">${msg}</span>`;
  el.appendChild(row);
  el.scrollTop = el.scrollHeight;
}


/* ---------------- Performance benchmark ---------------- */
async function loadBenchmark(){
  try{
    const d = await api('/api/benchmark');
    renderBenchmark(d);
  }catch(e){
    const el=document.getElementById('benchmarkSummary'); if(el) el.textContent=e.message;
  }
}
function renderBenchmark(d){
  document.getElementById('benchmarkSummary').textContent =
    `Raw capacity ${(d.total_capacity_bytes/1073741824).toFixed(1)} GB · Used ${(d.used_bytes/1073741824).toFixed(3)} GB · ${d.raw_utilization_pct.toFixed(2)}% utilized · Cloud: ${d.cloud_mode} · Replication factor: ${d.replication_factor}`;
  const rows=d.nodes.map(n=>`<tr><td>${n.name}</td><td>${n.rack}</td><td>${n.utilization_pct}%</td></tr>`).join('');
  const samples=d.placement_samples.map(x=>`<tr><td>${(x.sample_bytes/1024)} KB</td><td>${x.placement_ms_10x} ms</td></tr>`).join('');
  document.getElementById('benchmarkTable').innerHTML = `
    <h3>Node utilization</h3><table><thead><tr><th>Node</th><th>Rack</th><th>Used</th></tr></thead><tbody>${rows}</tbody></table>
    <h3 style="margin-top:20px">Placement latency</h3><table><thead><tr><th>Sample</th><th>10 placement decisions</th></tr></thead><tbody>${samples}</tbody></table>`;
}
document.getElementById('runBenchmarkBtn').onclick=loadBenchmark;

/* ---------------- Nodes & health ---------------- */
async function loadNodes(){
  try{
    const d = await api('/api/nodes');
    const grid = document.getElementById('nodeGrid');
    grid.innerHTML = '';
    d.nodes.forEach(n => {
      const pct = Math.min(100, (n.used_bytes / n.cap_bytes) * 100);
      const div = document.createElement('div');
      div.className = 'node' + (n.alive ? '' : ' dead');
      div.innerHTML = `
        <div class="node-top">
          <div><div class="node-name">${n.name}</div><div class="node-addr">${n.addr}</div></div>
          <div class="status-dot"></div>
        </div>
        <div class="bar-track"><div class="bar-fill" style="width:${pct}%"></div></div>
        <div class="node-meta"><span>${(n.used_bytes/1024).toFixed(0)} KB used</span><span>${n.alive?'online':'offline'}</span></div>
        <div class="metric-row"><span>CPU</span><span>${n.metrics.cpu}%</span></div>
        <div class="metric-row"><span>RAM</span><span>${n.metrics.ram}%</span></div>
        <div class="metric-row"><span>Disk</span><span>${n.metrics.disk}%</span></div>
        <div class="metric-row"><span>Network</span><span>${n.metrics.net}%</span></div>
        <span class="risk-pill ${n.risk_label}">predicted risk: ${n.risk_label} (${n.risk_score})</span><br>
        <button class="kill-btn ${n.alive?'':'revive'}" data-id="${n.id}">${n.alive?'Simulate node failure':'Bring node back online'}</button>
      `;
      grid.appendChild(div);
    });
    document.querySelectorAll('.kill-btn').forEach(btn => {
      btn.onclick = async () => {
        const d2 = await api('/api/nodes/' + btn.dataset.id + '/toggle', {method:'POST'});
        (d2.events||[]).forEach(e => pushGlobal(e.level, e.msg));
        await loadNodes();
        await loadFiles();
      };
    });
  }catch(e){}
}

/* ---------------- Sharing manager ---------------- */
async function loadShareFileOptions(){
  try{
    const d = await api('/api/files');
    const sel = document.getElementById('multiShareFiles');
    if(!sel) return;
    sel.innerHTML = '';
    d.files.forEach(f => {
      const opt = document.createElement('option');
      opt.value = f.id;
      opt.textContent = `${f.name} — ${(f.size/1024).toFixed(1)} KB`;
      sel.appendChild(opt);
    });
  }catch(e){}
}

async function loadShares(){
  try{
    await loadShareFileOptions();
    const d = await api('/api/shares');
    const tbody = document.querySelector('#sharesTable tbody');
    const empty = document.getElementById('sharesEmpty');
    if(d.shares.length === 0){
      tbody.innerHTML = ''; empty.style.display = 'block';
    } else {
      empty.style.display = 'none';
      tbody.innerHTML = '';
      d.shares.forEach(s => {
        const full = window.location.origin + s.url;
        const expiry = s.expires_at ? new Date(s.expires_at*1000).toLocaleString() : 'Never';
        const limit = s.max_downloads ? `${s.download_count}/${s.max_downloads}` : `${s.download_count}/∞`;
        const tr = document.createElement('tr');
        tr.innerHTML = `<td>${s.file_name}</td><td><a href="${full}" target="_blank" class="mono-dim">${s.token}</a></td><td>${s.password_protected ? '🔒 password' : '— none —'}</td><td>${expiry}</td><td>${limit}</td><td class="${s.status==='active' ? 'status-ok' : 'status-bad'}">${s.status}</td><td>${s.status==='active' ? `<button class="corrupt-btn" data-token="${s.token}">revoke</button>` : ''}</td>`;
        tbody.appendChild(tr);
      });
    }
    document.querySelectorAll('#sharesTable .corrupt-btn').forEach(btn => {
      btn.onclick = async () => {
        await api('/api/shares/' + btn.dataset.token + '/revoke', {method:'POST'});
        pushGlobal('warn', `Share link ${btn.dataset.token} revoked.`);
        loadShares(); loadOverview();
      };
    });

    const cd = await api('/api/share-collections');
    const ctbody = document.querySelector('#collectionSharesTable tbody');
    const cempty = document.getElementById('collectionSharesEmpty');
    if(cd.collections.length === 0){
      ctbody.innerHTML = ''; cempty.style.display = 'block';
    } else {
      cempty.style.display = 'none'; ctbody.innerHTML='';
      cd.collections.forEach(c => {
        const full = window.location.origin + c.url;
        const expiry = c.expires_at ? new Date(c.expires_at*1000).toLocaleString() : 'Never';
        const limit = c.max_downloads ? `${c.download_count}/${c.max_downloads}` : `${c.download_count}/∞`;
        const tr=document.createElement('tr');
        tr.innerHTML=`<td>${c.file_count} file(s)</td><td><a href="${full}" target="_blank" class="mono-dim">${c.token}</a></td><td>${c.password_protected ? '🔒 password' : '— none —'}</td><td>${expiry}</td><td>${limit}</td><td class="${c.status==='active' ? 'status-ok' : 'status-bad'}">${c.status}</td><td>${c.status==='active' ? `<button class="corrupt-btn collection-revoke" data-token="${c.token}">revoke</button>` : ''}</td>`;
        ctbody.appendChild(tr);
      });
    }
    document.querySelectorAll('#collectionSharesTable .collection-revoke').forEach(btn => {
      btn.onclick = async () => {
        await api('/api/share-collections/' + btn.dataset.token + '/revoke', {method:'POST'});
        pushGlobal('warn', `Multi-file share link ${btn.dataset.token} revoked.`);
        loadShares(); loadOverview();
      };
    });
  }catch(e){ pushGlobal('bad', `Sharing manager failed to load: ${e.message}`); }
}

document.getElementById('multiShareBtn').onclick = async () => {
  const sel = document.getElementById('multiShareFiles');
  const file_ids = Array.from(sel.selectedOptions).map(o => Number(o.value));
  if(!file_ids.length){ alert('Select at least one stored file.'); return; }
  const password = document.getElementById('multiSharePassword').value.trim();
  const expires_hours = document.getElementById('multiShareExpiry').value || null;
  const max_downloads = document.getElementById('multiShareLimit').value || null;
  try{
    const d = await api('/api/share-collection', {method:'POST', body:JSON.stringify({file_ids,password,expires_hours,max_downloads})});
    const full = window.location.origin + d.url;
    const box=document.getElementById('multiShareResult'); box.style.display='block';
    box.innerHTML=`<div class="share-generated"><div class="share-generated-text">Multi-file share link${d.password_protected?' (password protected)':''}: <a href="${full}" target="_blank" rel="noopener">${full}</a></div><button type="button" class="ghost-btn copy-share-btn" data-share-url="${encodeURIComponent(full)}">📋 Copy Link</button></div>`;
    const copyBtn = box.querySelector('.copy-share-btn');
    copyBtn.onclick = async () => {
      const url = decodeURIComponent(copyBtn.dataset.shareUrl);
      try {
        await navigator.clipboard.writeText(url);
      } catch (_) {
        const ta = document.createElement('textarea');
        ta.value = url; ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta); ta.focus(); ta.select();
        document.execCommand('copy'); ta.remove();
      }
      const oldText = copyBtn.textContent;
      copyBtn.textContent = '✓ Copied!';
      setTimeout(() => { copyBtn.textContent = oldText; }, 1800);
      pushGlobal('accent', 'Share link copied to clipboard.');
    };
    pushGlobal('accent', `Created one share link for ${d.file_count} files.`);
    await loadShares(); loadOverview();
  }catch(e){ pushGlobal('bad', `Multi-file share failed: ${e.message}`); alert(e.message); }
};

/* ---------------- Boot: check if already logged in ---------------- */
(async function boot(){
  try{
    const d = await api('/api/me');
    if(d.loggedIn){ enterApp(d.username); }
  }catch(e){}
})();
/* ===== USER THREE-DOT MENU ===== */

document.addEventListener("DOMContentLoaded", function () {

    const button = document.getElementById("userMenuBtn");
    const dropdown = document.getElementById("userDropdown");

    if (!button || !dropdown) return;

    button.addEventListener("click", function (event) {
        event.stopPropagation();
        dropdown.classList.toggle("show");
    });

    document.addEventListener("click", function (event) {

        if (!dropdown.contains(event.target) &&
            !button.contains(event.target)) {

            dropdown.classList.remove("show");
        }

    });

});
const togglePassword = document.getElementById("togglePassword");
const loginPass = document.getElementById("loginPass");

if (togglePassword && loginPass) {
    togglePassword.addEventListener("click", () => {
        const isPassword = loginPass.type === "password";

        loginPass.type = isPassword ? "text" : "password";
        togglePassword.textContent = isPassword ? "🙈" : "👁";
    });
}

// ===== SOCIAL LOGIN BUTTONS =====
// Show/hide signup password
document.getElementById("toggleSignupPassword")?.addEventListener("click", function () {
    const input = document.getElementById("suPassword");

    if (input.type === "password") {
        input.type = "text";
        this.textContent = "🙈";
    } else {
        input.type = "password";
        this.textContent = "👁";
    }
});

// Show/hide confirm password
document.getElementById("toggleConfirmPassword")?.addEventListener("click", function () {
    const input = document.getElementById("suConfirmPassword");

    if (input.type === "password") {
        input.type = "text";
        this.textContent = "🙈";
    } else {
        input.type = "password";
        this.textContent = "👁";
    }
});