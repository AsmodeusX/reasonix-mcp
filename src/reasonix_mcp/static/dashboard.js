const state = { token: "", fleet: null, selected: null, detail: null, refreshTimer: null };
const $ = id => document.getElementById(id);

function tokenFromLocation() {
  const hash = new URLSearchParams(location.hash.replace(/^#/, ""));
  return hash.get("token") || sessionStorage.getItem("reasonix-dashboard-token") || "";
}
function headers(json=false) {
  const h = { Authorization: `Bearer ${state.token}` };
  if (json) h["Content-Type"] = "application/json";
  return h;
}
async function api(path, options={}) {
  const response = await fetch(path, { ...options, headers: { ...headers(Boolean(options.body)), ...(options.headers||{}) }, cache: "no-store" });
  let data = {};
  try { data = await response.json(); } catch (_) {}
  if (!response.ok) throw new Error(data.error || `${response.status} ${response.statusText}`);
  return data;
}
function toast(message, error=false) {
  const node = $("toast"); node.textContent = message; node.className = `toast${error ? " error" : ""}`;
  clearTimeout(node.timer); node.timer = setTimeout(() => node.classList.add("hidden"), 4500);
}
function escapeHtml(value="") { const d=document.createElement("div"); d.textContent=String(value); return d.innerHTML; }
function shortPath(path="") { const parts=path.split("/").filter(Boolean); return parts.length ? parts.slice(-2).join("/") : "Unknown project"; }
function when(ts) { if (!ts) return ""; const d=new Date(ts*1000); const delta=(Date.now()-d)/1000; if(delta<60)return "now"; if(delta<3600)return `${Math.floor(delta/60)}m`; if(delta<86400)return `${Math.floor(delta/3600)}h`; return d.toLocaleDateString(); }
function selectedSummary() {
  if (!state.fleet || !state.selected) return null;
  for (const owner of state.fleet.orchestrators) for (const session of owner.sessions) if(session.session_id===state.selected) return session;
  return null;
}

async function refreshFleet(keepSelection=true) {
  state.fleet = await api("/api/fleet");
  renderFleet();
  if (keepSelection && state.selected) {
    const found=selectedSummary();
    if(found) renderSummary(found);
  }
  $("connection-dot").classList.add("online"); $("connection-label").textContent="Live";
}
function matches(session) {
  const q=$("search").value.trim().toLowerCase(), f=$("status-filter").value;
  if(q && !`${session.task} ${session.cwd} ${session.session_id}`.toLowerCase().includes(q)) return false;
  if(f==="running" && session.status!=="running") return false;
  if(f==="current" && !session.live) return false;
  if(f==="previous" && !session.historical) return false;
  if(f==="permission" && !session.permission_request) return false;
  return true;
}
function renderFleet() {
  const root=$("fleet"); root.innerHTML="";
  for(const owner of state.fleet?.orchestrators||[]) {
    const sessions=owner.sessions.filter(matches); if(!sessions.length) continue;
    const section=document.createElement("section"); section.className="owner";
    section.innerHTML=`<div class="owner-header"><span>${escapeHtml(owner.label)}</span><span class="owner-count">${sessions.length}</span></div>`;
    for(const session of sessions) {
      const button=document.createElement("button"); button.className=`session-item ${session.session_id===state.selected?"active":""}`;
      const dot=session.permission_request?"permission":session.status;
      button.innerHTML=`<span class="status-dot ${escapeHtml(dot)}"></span><span><strong>${escapeHtml(session.task||session.session_id)}</strong><small>${escapeHtml(shortPath(session.cwd))} · ${escapeHtml(session.status)} ${when(session.updated_at)}</small></span>`;
      button.onclick=()=>selectSession(session.session_id); section.appendChild(button);
    }
    root.appendChild(section);
  }
}
async function selectSession(id) {
  state.selected=id; renderFleet();
  await loadDetail();
}
async function loadDetail() {
  if(!state.selected)return;
  try { state.detail=await api(`/api/sessions/${encodeURIComponent(state.selected)}`); renderDetail(); }
  catch(e){ toast(e.message,true); }
}
function renderSummary(session) {
  $("session-task").textContent=session.task||session.session_id;
  $("session-project").textContent=shortPath(session.cwd).toUpperCase();
  $("session-status").textContent=session.status; $("session-status").className=`badge ${session.status}`;
  $("session-id").textContent=session.session_id; $("session-updated").textContent=when(session.updated_at);
}
function renderDetail() {
  const summary=selectedSummary()||state.detail.session;
  $("empty").classList.add("hidden"); $("session-view").classList.remove("hidden"); $("inspector").classList.remove("hidden");
  renderSummary(summary);
  const session=state.detail.session||{}, config=session.config||summary.config||{};
  const plan=session.plan||summary.plan||[]; $("plan-count").textContent=plan.length?`${plan.filter(x=>x.status==="completed").length}/${plan.length}`:"";
  $("current-work").textContent=(session.current_work||summary.current_work||{}).summary||"";
  $("plan").innerHTML=plan.length?plan.map(item=>`<div class="plan-item ${escapeHtml(item.status)}"><span class="plan-state ${escapeHtml(item.status)}">${escapeHtml(item.status||"pending")}</span><span class="plan-text">${escapeHtml(item.content||item.title||"")}</span></div>`).join(""):`<div class="plan-empty">No structured plan reported.</div>`;
  $("messages").innerHTML=state.detail.messages.length?state.detail.messages.map(m=>`<article class="message ${m.role}"><div class="message-role">${escapeHtml(m.role)}</div><div class="message-text">${escapeHtml(m.text)}</div>${m.work_duration_ms?`<div class="message-meta">Worked ${Math.round(m.work_duration_ms/1000)}s</div>`:""}</article>`).join(""):`<div class="plan-empty">No persisted messages yet.</div>`;
  const tools=state.detail.transcript.tool_calls||[]; $("tool-count").textContent=tools.length; $("tool-calls").innerHTML=tools.map(t=>`<div class="tool-call"><strong>${escapeHtml(t.name)}</strong><pre>${escapeHtml(t.arguments)}</pre></div>`).join("")||`<div class="plan-empty">No recorded tool calls.</div>`;
  renderPermission(session.permission_request||summary.permission_request_detail||null);
  populateConfig(config);
  $("stop-button").disabled=!summary.live; $("resume-button").disabled=summary.live;
  $("resume-button").classList.toggle("hidden", summary.live);
  $("message-input").disabled=!summary.live; $("composer").querySelector("button[type=submit]").disabled=!summary.live;
  $("session-facts").innerHTML=facts({Status:summary.status,Project:summary.cwd||"—",Owner:state.detail.owner_id,Resumable:String(Boolean(summary.resumable)),"Stop reason":summary.stop_reason||"—"});
  $("transcript-facts").innerHTML=facts({Messages:state.detail.transcript.messages_total||0,"Tool calls":state.detail.transcript.tool_calls_total||0,Updated:when(state.detail.transcript.updated_at),Path:state.detail.transcript.transcript_path||"—"});
}
function facts(obj){return Object.entries(obj).map(([k,v])=>`<dt>${escapeHtml(k)}</dt><dd>${escapeHtml(v)}</dd>`).join("");}
function renderPermission(permission) {
  const card=$("permission-card"); if(!permission){card.classList.add("hidden");return;} card.classList.remove("hidden");
  if(permission===true){$("permission-title").textContent="Decision pending";$("permission-input").textContent="This request predates the dashboard-capable agent daemon. Use the orchestrator watch to answer it; full controls appear here after agentd safely reloads.";$("permission-options").innerHTML="";return;}
  const tc=permission.tool_call||{}; $("permission-title").textContent=tc.title||"Agent decision"; $("permission-input").textContent=JSON.stringify(tc.rawInput||{},null,2);
  const root=$("permission-options"); root.innerHTML=""; for(const option of permission.options||[]){const b=document.createElement("button");b.type="button";b.textContent=option.name||option.optionId;b.onclick=()=>act("permission",{option_id:option.optionId});root.appendChild(b);}
}
function populateConfig(config) {
  const models=state.fleet?.models?.models||[], model=$("model-select"); model.innerHTML=`<option value="">Keep current</option>`+models.map(m=>`<option value="${escapeHtml(m.ref)}">${escapeHtml(m.ref)}</option>`).join(""); model.value=config.model||"";
  const efforts=state.fleet?.models?.effort_options||[]; const effort=$("effort-select"); effort.innerHTML=`<option value="">Keep/default</option>`+efforts.map(e=>`<option value="${e}">${e}</option>`).join(""); effort.value=config.effort||"";
  $("approval-select").value=config.tool_approval||""; $("work-mode-select").value=config.work_mode||"";
}
async function act(action, body={}) {
  if(!state.selected)return;
  try { const result=await api(`/api/sessions/${encodeURIComponent(state.selected)}/${action}`,{method:"POST",body:JSON.stringify(body)}); toast(result.note||`${action} succeeded`); await refreshFleet(); await loadDetail(); return result; }
  catch(e){toast(e.message,true);throw e;}
}
async function eventStream() {
  while(state.token){
    try {
      const response=await fetch("/api/events",{headers:headers(),cache:"no-store"}); if(!response.ok)throw new Error("event stream rejected");
      const reader=response.body.getReader(), decoder=new TextDecoder(); let buffer="";
      while(true){const {value,done}=await reader.read();if(done)break;buffer+=decoder.decode(value,{stream:true});let cut;while((cut=buffer.indexOf("\n\n"))>=0){const block=buffer.slice(0,cut);buffer=buffer.slice(cut+2);const line=block.split("\n").find(x=>x.startsWith("data: "));if(line){const event=JSON.parse(line.slice(6));scheduleRefresh(event);}}}
    } catch(e){$("connection-dot").classList.remove("online");$("connection-label").textContent="Reconnecting…";await new Promise(r=>setTimeout(r,1500));}
  }
}
function scheduleRefresh(event){clearTimeout(state.refreshTimer);state.refreshTimer=setTimeout(async()=>{await refreshFleet();if(state.selected && (!event.session_id||event.session_id===state.selected||(event.event||{}).session_id===state.selected))await loadDetail();},180);}
async function authenticate(token){state.token=token;try{await api("/api/health");sessionStorage.setItem("reasonix-dashboard-token",token);history.replaceState(null,"",location.pathname);$("auth-screen").classList.add("hidden");$("app").classList.remove("hidden");await refreshFleet(false);eventStream();}catch(e){state.token="";$("auth-screen").classList.remove("hidden");$("app").classList.add("hidden");toast("Invalid or expired dashboard token",true);}}

$("auth-form").onsubmit=e=>{e.preventDefault();authenticate($("token-input").value.trim());};
$("search").oninput=renderFleet; $("status-filter").onchange=renderFleet; $("refresh").onclick=()=>refreshFleet().then(loadDetail);
$("reload-detail").onclick=loadDetail;
$("composer").onsubmit=e=>{e.preventDefault();const input=$("message-input"),message=input.value.trim();if(!message)return;act("send",{message,expect:$("send-expect").value}).then(()=>input.value="");};
$("message-input").onkeydown=e=>{if((e.ctrlKey||e.metaKey)&&e.key==="Enter"){$("composer").requestSubmit();}};
$("apply-config").onclick=()=>act("configure",{model:$("model-select").value,effort:$("effort-select").value,tool_approval:$("approval-select").value,work_mode:$("work-mode-select").value}).then(r=>{$("config-note").textContent=r.note||"Applied";});
$("stop-button").onclick=()=>{if(confirm("Stop this agent process? Its persisted session remains resumable."))act("stop");};
$("resume-button").onclick=()=>{const s=selectedSummary();const cwd=prompt("Resume working directory",s?.cwd||"");if(cwd!==null)act("resume",{cwd});};

const initial=tokenFromLocation(); if(initial)authenticate(initial); else $("auth-screen").classList.remove("hidden");
