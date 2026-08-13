const state = { token: "", fleet: null, routines: [], selected: null, selectedRoutine: null, detail: null, refreshTimer: null, collapsedOwners: new Set(), originalConfig: {}, timelineSession: null };
const $ = id => document.getElementById(id);

function tokenFromLocation() {
  const hash = new URLSearchParams(location.hash.replace(/^#/, ""));
  return hash.get("token") || sessionStorage.getItem("reasonix-dashboard-token") || "";
}
function sessionFromLocation() { return new URLSearchParams(location.hash.replace(/^#/, "")).get("session") || ""; }
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
function dateOf(ts) { return new Date(ts > 1e12 ? ts : ts*1000); }
function when(ts) { if (!ts) return ""; const d=dateOf(ts); const delta=(Date.now()-d)/1000; if(delta<60)return "now"; if(delta<3600)return `${Math.floor(delta/60)}m`; if(delta<86400)return `${Math.floor(delta/3600)}h`; return d.toLocaleDateString(); }
function dateTime(ts) { return ts ? dateOf(ts).toLocaleString() : "Time unavailable"; }
function formatText(value="") { return escapeHtml(value).replace(/^#{1,6}\s+(.+)$/gm,'<strong class="md-heading">$1</strong>').replace(/^\s*-\s+/gm,"• ").replace(/`([^`\n]+)`/g,"<code>$1</code>").replace(/\*\*([^*]+)\*\*/g,"<strong>$1</strong>"); }
function selectedSummary() {
  if (!state.fleet || !state.selected) return null;
  for (const owner of state.fleet.orchestrators) for (const session of owner.sessions) if(session.session_id===state.selected) return session;
  return null;
}

async function refreshFleet(keepSelection=true) {
  const [fleet,routineData] = await Promise.all([api("/api/fleet"),api("/api/routines")]);
  state.fleet=fleet;state.routines=routineData.routines||[];
  renderFleet();renderRoutines();
  if (keepSelection && state.selected) {
    const found=selectedSummary();
    if(found) renderSummary(found);
  }
  if(keepSelection&&state.selectedRoutine){if(selectedRoutine())renderRoutineDetail();else{$("routine-view").classList.add("hidden");$("empty").classList.remove("hidden");state.selectedRoutine=null;}}
  $("connection-dot").classList.add("online"); $("connection-label").textContent="Live";
}
function routineState(r){const active=[...(r.runs||[])].reverse().find(x=>x.status==="starting"||x.status==="running");return active?active.status:(r.enabled?((r.runs||[]).at(-1)?.status||"scheduled"):"paused");}
function scheduleText(r){if(r.schedule_kind==="manual")return "Manual";if(r.schedule_kind==="interval")return `Every ${r.interval_minutes}m`;return `${r.daily_at} ${r.timezone}`;}
function renderRoutines(){const root=$("routines");root.innerHTML="";for(const r of state.routines){const status=routineState(r),b=document.createElement("button");b.className=`routine-item ${r.routine_id===state.selectedRoutine?"active":""}`;b.innerHTML=`<span class="status-dot ${escapeHtml(status)}"></span><span><strong>${escapeHtml(r.name)}</strong><small>${escapeHtml(status)} · ${escapeHtml(scheduleText(r))}</small></span>`;b.onclick=()=>selectRoutine(r.routine_id);root.appendChild(b);}}
function matches(session) {
  const q=$("search").value.trim().toLowerCase(), f=$("status-filter").value;
  if(q && !`${session.task} ${session.cwd} ${session.session_id}`.toLowerCase().includes(q)) return false;
  if(f==="active" && session.status!=="running" && !(session.live&&session.permission_request)) return false;
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
    const collapsed=state.collapsedOwners.has(owner.owner_id);
    const ownerButton=document.createElement("button");ownerButton.type="button";ownerButton.className="owner-header";ownerButton.setAttribute("aria-expanded",String(!collapsed));
    const preview=sessions.slice(0,2).map(s=>`<span>${escapeHtml((s.task||s.session_id).slice(0,38))}</span>`).join("");
    ownerButton.innerHTML=`<span class="owner-caret">${collapsed?"›":"⌄"}</span><span class="owner-name">${escapeHtml(owner.label)}</span><span class="owner-stats"><b>${owner.counts.running}</b> running · ${sessions.length}</span><span class="owner-preview">${preview}</span>`;
    ownerButton.onclick=()=>{if(collapsed)state.collapsedOwners.delete(owner.owner_id);else state.collapsedOwners.add(owner.owner_id);renderFleet();};section.appendChild(ownerButton);
    const children=document.createElement("div");children.className=`owner-agents${collapsed?" hidden":""}`;
    for(const session of sessions) {
      const button=document.createElement("button"); button.className=`session-item ${session.session_id===state.selected?"active":""}`;
      const dot=session.permission_request?"permission":session.status;
      button.innerHTML=`<span class="status-dot ${escapeHtml(dot)}"></span><span><strong>${escapeHtml(session.task||session.session_id)}</strong><small>${escapeHtml(shortPath(session.cwd))} · ${escapeHtml(session.status)} · ${when(session.updated_at)}</small></span>`;
      button.onclick=()=>selectSession(session.session_id); children.appendChild(button);
    }
    section.appendChild(children);
    root.appendChild(section);
  }
}
async function selectSession(id) {
  state.selected=id;state.selectedRoutine=null;renderFleet();renderRoutines();$("routine-view").classList.add("hidden");
  await loadDetail();
}
function selectedRoutine(){return state.routines.find(r=>r.routine_id===state.selectedRoutine)||null;}
function selectRoutine(id){state.selectedRoutine=id;state.selected=null;state.detail=null;renderFleet();renderRoutines();$("empty").classList.add("hidden");$("session-view").classList.add("hidden");$("inspector").classList.add("hidden");$("routine-view").classList.remove("hidden");renderRoutineDetail();}
function renderRoutineDetail(){const r=selectedRoutine();if(!r)return;const active=[...(r.runs||[])].reverse().find(x=>["starting","running"].includes(x.status)),status=routineState(r);$("routine-title").textContent=r.name;$("routine-status").textContent=status;$("routine-status").className=`badge ${status}`;$("routine-id").textContent=r.routine_id;$("routine-next").textContent=r.next_run_at?`Next ${dateTime(r.next_run_at)}`:"No scheduled run";$("routine-prompt").textContent=r.prompt;$("routine-facts").innerHTML=facts({Schedule:scheduleText(r),Directory:r.cwd,Orchestrator:r.owner_id,Model:r.model,Effort:r.effort,Approval:r.tool_approval,"Work mode":r.work_mode,Delegation:r.delegation,"Overlap policy":r.overlap_policy,"Queued triggers":r.pending_triggers||0});$("routine-toggle").textContent=r.enabled?"Pause":"Enable";$("routine-stop").disabled=!active||!active.session_id;const runs=[...(r.runs||[])].reverse();$("routine-runs").innerHTML=runs.length?runs.map(run=>`<div class="routine-run"><strong>${escapeHtml(run.status)}</strong><span>${escapeHtml(run.message||run.error||"No result recorded yet")}<small>${escapeHtml(run.trigger||"")} · ${escapeHtml(dateTime(run.started_at||run.created_at))}${run.finished_at?` → ${escapeHtml(dateTime(run.finished_at))}`:""}</small></span>${run.session_id?`<a href="#" data-session="${escapeHtml(run.session_id)}">Open agent</a>`:""}</div>`).join(""):`<div class="plan-empty">No runs yet. Run it now or wait for its schedule.</div>`;for(const link of $("routine-runs").querySelectorAll("[data-session]"))link.onclick=e=>{e.preventDefault();selectSession(link.dataset.session);};}
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
  const runs=state.detail.transcript.runs||[],timeline=$("messages");$("run-count").textContent=runs.length?`${runs.length} / ${state.detail.transcript.runs_total}`:"";
  const changedSession=state.timelineSession!==state.selected,nearLatest=timeline.scrollHeight-timeline.scrollTop-timeline.clientHeight<80,previousScroll=timeline.scrollTop;
  timeline.innerHTML=runs.length?runs.map(run=>renderRun(run,summary.status==="running")).join(""):`<div class="plan-empty">No persisted runs yet.</div>`;state.timelineSession=state.selected;
  const restoreTimeline=()=>{timeline.scrollTop=(changedSession||nearLatest)?timeline.scrollHeight:previousScroll;};requestAnimationFrame(restoreTimeline);setTimeout(restoreTimeline,0);
  const tools=state.detail.transcript.tool_calls||[]; $("tool-count").textContent=tools.length; $("tool-calls").innerHTML=tools.map(t=>`<div class="tool-call"><strong>${escapeHtml(t.name)}</strong><pre>${escapeHtml(t.arguments)}</pre></div>`).join("")||`<div class="plan-empty">No recorded tool calls.</div>`;
  renderPermission(session.permission_request||summary.permission_request_detail||null);
  populateConfig(config,session.config_known,session.config_source,session.pending_config||{});
  $("stop-button").disabled=!summary.live; $("resume-button").disabled=summary.live;
  $("resume-button").classList.toggle("hidden", summary.live);
  $("message-input").disabled=!summary.live; $("composer").querySelector("button[type=submit]").disabled=!summary.live;
  $("session-facts").innerHTML=facts({Status:summary.status,Project:summary.cwd||"—",Owner:state.detail.owner_id,Resumable:String(Boolean(summary.resumable)),"Stop reason":summary.stop_reason||"—"});
  $("transcript-facts").innerHTML=facts({Runs:state.detail.transcript.runs_total||0,Messages:state.detail.transcript.messages_total||0,"Tool calls":state.detail.transcript.tool_calls_total||0,Updated:when(state.detail.transcript.updated_at),Path:state.detail.transcript.transcript_path||"—"});
}
function renderRun(run,isLive){const active=Boolean(run.active&&isLive);const entries=(run.entries||[]).map(renderEntry).join("");return `<section class="run-card ${active?"active-run":""}"><header><span>Run ${run.number}</span>${active?`<b class="live-run"><i></i>Active now</b>`:""}<time>${escapeHtml(dateTime(run.started_at))}</time></header><div class="run-timeline">${entries||`<div class="plan-empty">No recorded activity.</div>`}</div></section>`;}
function renderEntry(entry){if(entry.kind==="message")return `<article class="timeline-entry message ${entry.role}"><div class="entry-marker"></div><div><div class="message-role">${entry.role==="user"?"Prompt":"Agent message"}</div><div class="message-text">${formatText(entry.text)}</div>${entry.work_duration_ms?`<div class="message-meta">Worked ${Math.round(entry.work_duration_ms/1000)}s</div>`:""}</div></article>`;if(entry.kind==="orchestrator_message")return `<article class="timeline-entry orchestrator-message"><div class="entry-marker"></div><div><div class="message-role">Orchestrator · ${escapeHtml(entry.delivered||"steered")}</div><div class="message-text">${formatText(entry.text)}</div><div class="message-meta">${escapeHtml(dateTime(entry.created_at))}</div></div></article>`;if(entry.kind==="reasoning")return `<article class="timeline-entry reasoning"><div class="entry-marker"></div><div><div class="message-role">Reasoning</div><div class="message-text">${formatText(entry.text)}</div></div></article>`;if(entry.kind==="tool_call")return `<details class="timeline-entry activity"><summary><span class="entry-marker"></span><span>Tool · <strong>${escapeHtml(entry.name)}</strong></span></summary><pre>${escapeHtml(entry.arguments)}</pre></details>`;if(entry.kind==="tool_result")return `<details class="timeline-entry activity result"><summary><span class="entry-marker"></span><span>Result · <strong>${escapeHtml(entry.name)}</strong>${entry.truncated?" (tail)":""}</span></summary><pre>${escapeHtml(entry.text)}</pre></details>`;return "";}
function facts(obj){return Object.entries(obj).map(([k,v])=>`<dt>${escapeHtml(k)}</dt><dd>${escapeHtml(v)}</dd>`).join("");}
function renderPermission(permission) {
  const card=$("permission-card"); if(!permission){card.classList.add("hidden");return;} card.classList.remove("hidden");
  if(permission===true){$("permission-title").textContent="Decision pending";$("permission-input").textContent="This request predates the dashboard-capable agent daemon. Use the orchestrator watch to answer it; full controls appear here after agentd safely reloads.";$("permission-options").innerHTML="";return;}
  const tc=permission.tool_call||{}; $("permission-title").textContent=tc.title||"Agent decision"; $("permission-input").textContent=JSON.stringify(tc.rawInput||{},null,2);
  const root=$("permission-options"); root.innerHTML=""; for(const option of permission.options||[]){const b=document.createElement("button");b.type="button";b.textContent=option.name||option.optionId;b.onclick=()=>act("permission",{option_id:option.optionId});root.appendChild(b);}
}
function populateConfig(config,known,source,pending) {
  state.originalConfig={...config};const unknown="Not reported by the current agent daemon";
  const pendingText=Object.entries(pending||{}).map(([k,v])=>`${k}: ${v}`).join(" · ");
  $("current-config").innerHTML=[['Model',config.model],['Effort',config.effort],['Approval',config.tool_approval],['Mode',config.work_mode]].map(([k,v])=>`<div><span>${k}</span><strong class="${v?"":"unknown"}">${escapeHtml(v||"Unknown")}</strong></div>`).join("")+(!known?`<p>${source==="persisted_pending"?"Showing persisted settings queued for reload.":unknown+". Choose only fields you want to change."}</p>`:"")+(pendingText?`<p class="pending-config">Queued: ${escapeHtml(pendingText)}</p>`:"");
  const models=state.fleet?.models?.models||[], model=$("model-select");const refs=models.map(m=>m.ref);model.innerHTML=`<option value="">${config.model?"Select a different model":"Choose model…"}</option>`+(config.model&&!refs.includes(config.model)?`<option value="${escapeHtml(config.model)}">${escapeHtml(config.model)} (current)</option>`:"")+models.map(m=>`<option value="${escapeHtml(m.ref)}">${escapeHtml(m.ref)}${m.ref===config.model?" (current)":""}</option>`).join("");model.value=config.model||"";
  setOptions($("effort-select"),state.fleet?.models?.effort_options||[],config.effort,"Choose effort…");
  setOptions($("approval-select"),["ask","auto","yolo"],config.tool_approval,"Choose approval…");
  setOptions($("work-mode-select"),["economy","balanced","delivery"],config.work_mode,"Choose work mode…");
  updateConfigButton();
}
function setOptions(node,values,current,placeholder){node.innerHTML=`<option value="">${current?"Select a different value":placeholder}</option>`+values.map(v=>`<option value="${v}">${v}${v===current?" (current)":""}</option>`).join("");node.value=current||"";}
function configChanges(){const map={model:"model-select",effort:"effort-select",tool_approval:"approval-select",work_mode:"work-mode-select"},out={};for(const [key,id] of Object.entries(map)){const value=$(id).value;if(value&&value!==state.originalConfig[key])out[key]=value;}return out;}
function updateConfigButton(){$("apply-config").disabled=!Object.keys(configChanges()).length;}
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
function setRoutineScheduleFields(){const kind=$("routine-schedule").value;document.querySelector(".daily-field").classList.toggle("hidden",kind!=="daily");document.querySelector(".interval-field").classList.toggle("hidden",kind!=="interval");}
function openRoutineForm(r=null){const owners=state.fleet?.orchestrators||[],models=state.fleet?.models?.models||[],efforts=state.fleet?.models?.effort_options||[];$("routine-form-title").textContent=r?"Edit loop agent":"New loop agent";$("routine-edit-id").value=r?.routine_id||"";$("routine-name").value=r?.name||"";$("routine-owner").innerHTML=owners.map(o=>`<option value="${escapeHtml(o.owner_id)}">${escapeHtml(o.label)}</option>`).join("");$("routine-owner").value=r?.owner_id||owners[0]?.owner_id||"";$("routine-owner").disabled=Boolean(r);$("routine-cwd").value=r?.cwd||"";$("routine-instructions").value=r?.prompt||"";$("routine-schedule").value=r?.schedule_kind||"daily";$("routine-daily").value=r?.daily_at||"09:00";$("routine-interval").value=r?.interval_minutes||1440;$("routine-timezone").value=r?.timezone||Intl.DateTimeFormat().resolvedOptions().timeZone||"UTC";$("routine-overlap").value=r?.overlap_policy||"skip";$("routine-delegation").value=r?.delegation||"allowed";$("routine-model").innerHTML=models.map(m=>`<option value="${escapeHtml(m.ref)}">${escapeHtml(m.ref)}</option>`).join("");$("routine-model").value=r?.model||state.fleet?.models?.default||models[0]?.ref||"";$("routine-effort").innerHTML=efforts.map(v=>`<option value="${escapeHtml(v)}">${escapeHtml(v)}</option>`).join("");$("routine-effort").value=r?.effort||"max";$("routine-approval").value=r?.tool_approval||"auto";$("routine-mode").value=r?.work_mode||"balanced";$("routine-enabled").checked=r?.enabled??true;$("routine-immediate").checked=false;$("run-now-label").classList.toggle("hidden",Boolean(r));setRoutineScheduleFields();$("routine-dialog").showModal();}
function routineFormData(){return {name:$("routine-name").value.trim(),owner_id:$("routine-owner").value,cwd:$("routine-cwd").value.trim(),prompt:$("routine-instructions").value.trim(),schedule_kind:$("routine-schedule").value,daily_at:$("routine-daily").value,interval_minutes:Number($("routine-interval").value),timezone:$("routine-timezone").value.trim(),overlap_policy:$("routine-overlap").value,delegation:$("routine-delegation").value,model:$("routine-model").value,effort:$("routine-effort").value,tool_approval:$("routine-approval").value,work_mode:$("routine-mode").value,enabled:$("routine-enabled").checked,run_immediately:$("routine-immediate").checked};}
async function routineAction(action,body={}){const id=state.selectedRoutine;if(!id)return;try{const result=await api(`/api/routines/${encodeURIComponent(id)}/${action}`,{method:"POST",body:JSON.stringify(body)});toast(`${action} succeeded`);await refreshFleet();return result;}catch(e){toast(e.message,true);throw e;}}
async function authenticate(token){state.token=token;try{const requested=sessionFromLocation();await api("/api/health");sessionStorage.setItem("reasonix-dashboard-token",token);history.replaceState(null,"",location.pathname);$("auth-screen").classList.add("hidden");$("app").classList.remove("hidden");await refreshFleet(false);if(requested&&[...(state.fleet?.orchestrators||[])].some(o=>o.sessions.some(s=>s.session_id===requested)))await selectSession(requested);eventStream();}catch(e){state.token="";$("auth-screen").classList.remove("hidden");$("app").classList.add("hidden");toast("Invalid or expired dashboard token",true);}}

$("auth-form").onsubmit=e=>{e.preventDefault();authenticate($("token-input").value.trim());};
$("search").oninput=renderFleet; $("status-filter").onchange=renderFleet; $("refresh").onclick=()=>refreshFleet().then(loadDetail);
$("reload-detail").onclick=loadDetail;
$("composer").onsubmit=e=>{e.preventDefault();const input=$("message-input"),message=input.value.trim();if(!message)return;act("send",{message,expect:$("send-expect").value}).then(()=>input.value="");};
$("message-input").onkeydown=e=>{if((e.ctrlKey||e.metaKey)&&e.key==="Enter"){$("composer").requestSubmit();}};
for(const id of ["model-select","effort-select","approval-select","work-mode-select"])$(id).onchange=updateConfigButton;
$("apply-config").onclick=()=>act("configure",configChanges()).then(r=>{$("config-note").textContent=r.note||"Applied";});
$("stop-button").onclick=()=>{if(confirm("Stop this agent process? Its persisted session remains resumable."))act("stop");};
$("resume-button").onclick=()=>{const s=selectedSummary();const cwd=prompt("Resume working directory",s?.cwd||"");if(cwd!==null)act("resume",{cwd});};
$("new-routine").onclick=()=>openRoutineForm();$("routine-edit").onclick=()=>openRoutineForm(selectedRoutine());$("routine-schedule").onchange=setRoutineScheduleFields;for(const id of ["routine-cancel","routine-cancel-bottom"])$(id).onclick=()=>$("routine-dialog").close();
$("routine-form").onsubmit=async e=>{e.preventDefault();const id=$("routine-edit-id").value,data=routineFormData();try{let saved;if(id){delete data.owner_id;delete data.run_immediately;saved=await api(`/api/routines/${encodeURIComponent(id)}/configure`,{method:"POST",body:JSON.stringify(data)});}else saved=await api("/api/routines",{method:"POST",body:JSON.stringify(data)});$("routine-dialog").close();state.selectedRoutine=saved.routine_id;state.selected=null;await refreshFleet();renderRoutineDetail();toast(id?"Routine updated":"Routine created");}catch(err){toast(err.message,true);}};
$("routine-run").onclick=()=>routineAction("run");$("routine-stop").onclick=()=>{if(confirm("Stop the active routine parent agent?"))routineAction("stop");};$("routine-toggle").onclick=()=>{const r=selectedRoutine();routineAction("configure",{enabled:!r.enabled});};$("routine-delete").onclick=()=>{if(confirm("Delete this routine and its run history?"))routineAction("delete").then(()=>{state.selectedRoutine=null;$("routine-view").classList.add("hidden");$("empty").classList.remove("hidden");});};

const initial=tokenFromLocation(); if(initial)authenticate(initial); else $("auth-screen").classList.remove("hidden");
