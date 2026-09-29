// Ward Vitals demo control: polls the controller and renders the ward, alerts and batch layer.
"use strict";

const $ = (sel) => document.querySelector(sel);
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const fmtTime = (iso) => iso ? new Date(iso).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) : "";
const secs = (s) => s == null ? "–" : s < 60 ? `${s.toFixed(1)} s` : `${Math.floor(s / 60)} min ${Math.round(s % 60)} s`;

const SCENARIOS = [
  { id: "none", name: "Normal ward", story: "Everyone at their usual baseline", tech: "--scenario none" },
  { id: "spike", name: "Sudden deterioration", story: "Heart races and oxygen drops at once", tech: "HR 135–160 · SpO₂ 84–89" },
  { id: "hr_spike", name: "Heart-rate spike", story: "Only the heart rate shoots up", tech: "HR 135–160" },
  { id: "spo2_drop", name: "Gradual oxygen drop", story: "Oxygen slides down slowly: watch the trend arrow", tech: "SpO₂ −12 → FALLING" },
  { id: "outage", name: "Sensor feed outage", story: "All bedside monitors go silent", tech: "→ NoVitalsReceived" },
];
const PHASE_TEXT = { starting: "Starting in", active: "Running, ends in", finished: "Finished" };

let mode = "story";
let view = "control";          // "control" (the control centre) or "pipeline" (the live diagram)
let focusPatient = null;       // patient of the running scenario, outlined on the board
let drawerPatient = null;
const history = {};            // patient -> [{t, hr, spo2}] for the drawer sparklines
const seenAlerts = new Set();
let firstAlertLoad = true;
let feedStatus = null;

// ------------------------------------------------------------------ helpers --
async function get(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error((await r.json().catch(() => ({}))).detail || r.statusText);
  return r.json();
}
async function post(path, body) {
  const r = await fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body ?? {}) });
  const data = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(data.detail || r.statusText);
  return data;
}
function toast(text, err = false) {
  const t = $("#toast");
  t.textContent = text;
  t.className = "toast" + (err ? " err" : "");
  t.hidden = false;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => (t.hidden = true), 3500);
}
async function act(fn, ok) {
  try { await fn(); if (ok) toast(ok); refreshState(); }
  catch (e) { toast(e.message, true); }
}

// -------------------------------------------------------------------- setup --
function setup() {
  for (const sel of [$("#patient"), $("#plPatient")]) {
    for (let i = 1; i <= 15; i++) {
      const id = `P${String(i).padStart(3, "0")}`;
      sel.add(new Option(id, id, false, id === "P007"));
    }
    sel.addEventListener("change", (e) => { $("#patient").value = $("#plPatient").value = e.target.value; });
  }
  $("#scenarios").innerHTML = SCENARIOS.map((s) => `
    <button class="scenario" data-scenario="${s.id}">
      <b>${esc(s.name)}</b><span class="tech">${esc(s.tech)}</span>
    </button>`).join("");
  $("#scenarios").addEventListener("click", (e) => {
    const b = e.target.closest("[data-scenario]");
    if (!b || b.disabled) return;
    const scenario = b.dataset.scenario;
    act(() => post("/demo/api/scenario", {
      scenario, patient: $("#patient").value, duration_s: Number($("#duration").value),
    }), scenario === "none" ? "Back to a normal ward" : "Scenario starts in 3 s");
  });
  $("#malformed").addEventListener("change", (e) =>
    act(() => post("/demo/api/malformed", { rate: e.target.checked ? 0.05 : 0 }),
      e.target.checked ? "5% of readings are now broken" : "All readings valid again"));
  $("#pauseFeed").onclick = () => act(() => post("/demo/api/feed/pause"), "Feed paused");
  $("#resumeFeed").onclick = () => act(() => post("/demo/api/feed/resume"), "Feed resumed");
  $("#runDag").onclick = () => act(async () => {
    const r = await post("/demo/api/dag/run", { sim_day: Number($("#batchDay").value) });
    toast(`Airflow run queued for day ${r.sim_day}`);
    setTimeout(refreshBatch, 1500);
  });
  $("#corrupt").onclick = () => act(() => post("/demo/api/labs/corrupt", { sim_day: Number($("#batchDay").value) }),
    "Lab file corrupted: now run the daily consolidation");
  $("#restore").onclick = () => act(() => post("/demo/api/labs/restore", { sim_day: Number($("#batchDay").value) }),
    "Correct lab file restored: run the consolidation again");
  document.querySelectorAll(".mode button").forEach((b) => b.onclick = () => setMode(b.dataset.mode));
  document.querySelectorAll(".tabs button").forEach((b) => b.onclick = () => setView(b.dataset.view));
  // Quick actions on the Live pipeline tab: the same endpoints as the control centre.
  document.querySelector(".pl-actions").addEventListener("click", (e) => {
    const b = e.target.closest("[data-pl]");
    if (!b) return;
    const what = b.dataset.pl;
    if (what === "malformed") {
      const on = !(feedStatus && feedStatus.settings.malformed_rate > 0);
      act(() => post("/demo/api/malformed", { rate: on ? 0.05 : 0 }), on ? "5% of readings are now broken" : "All readings valid again");
    } else if (what === "dag") {
      act(async () => {
        const r = await post("/demo/api/dag/run", { sim_day: Number($("#batchDay").value) });
        toast(`Airflow run queued for day ${r.sim_day}`);
      });
    } else {
      act(() => post("/demo/api/scenario", { scenario: what, patient: $("#plPatient").value, duration_s: Number($("#duration").value) }),
        what === "none" ? "Back to a normal ward" : "Scenario starts in 3 s");
    }
  });
  $("#tiles").addEventListener("click", (e) => {
    const t = e.target.closest("[data-patient]");
    if (t) openDrawer(t.dataset.patient);
  });
  $("#drawerClose").onclick = closeDrawer;
  $("#drawer").addEventListener("click", (e) => { if (e.target.id === "drawer") closeDrawer(); });
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") closeDrawer();
    if (e.target.matches("input, select")) return;
    if (e.key === "t") setMode(mode === "story" ? "tech" : "story");   // presenter shortcuts
    if (e.key === "p") setView(view === "control" ? "pipeline" : "control");
  });
  // Bookmarkable start view: ?mode=tech opens the technical view, ?patient=P007 opens a patient.
  const q = new URLSearchParams(location.search);
  let saved = null;
  try { saved = localStorage.getItem("demoMode"); } catch { /* private window */ }
  setMode(q.get("mode") === "tech" || q.get("mode") === "story" ? q.get("mode") : saved || "story");
  if (/^P\d{3}$/.test(q.get("patient") || "")) openDrawer(q.get("patient"));
  let savedView = null;
  try { savedView = localStorage.getItem("demoView"); } catch { /* private window */ }
  // pipeline.js loads after this file, so the first view is shown once every script has run.
  document.addEventListener("DOMContentLoaded", () =>
    setView(["control", "pipeline"].includes(q.get("view")) ? q.get("view") : savedView || "control"));
}

function setView(v) {
  view = v;
  document.body.classList.toggle("view-pipeline", v === "pipeline");
  document.querySelectorAll(".tabs button").forEach((b) => b.classList.toggle("on", b.dataset.view === v));
  $(".layout").hidden = v === "pipeline";
  $("#pipeline").hidden = v !== "pipeline";
  if (v === "pipeline") Pipeline.show(); else Pipeline.hide();
  try { localStorage.setItem("demoView", v); } catch { /* private window */ }
}

function setMode(m) {
  mode = m;
  document.body.classList.remove("mode-story", "mode-tech");
  document.body.classList.add(`mode-${m}`);
  if (typeof Pipeline !== "undefined") Pipeline.renderCaptions();
  document.querySelectorAll(".mode button").forEach((b) => b.classList.toggle("on", b.dataset.mode === m));
  try { localStorage.setItem("demoMode", m); } catch { /* private window */ }
}

// ------------------------------------------------------------------- state --
async function refreshState() {
  let s;
  try { s = await get("/demo/api/state"); } catch { return; }
  feedStatus = s.feed;
  const c = s.clock;
  $("#simDay").textContent = c.sim_day;
  const into = Math.floor(c.seconds_into_day);
  $("#simTime").textContent = `${Math.floor(into / 60)}:${String(into % 60).padStart(2, "0")} of ${c.day_seconds / 60}:00`;
  $("#dayBar").style.width = `${(100 * c.seconds_into_day) / c.day_seconds}%`;
  const dayInput = $("#batchDay");
  if (dayInput.value === "" && document.activeElement !== dayInput) dayInput.value = c.last_complete_day;

  document.querySelectorAll("#services .chip").forEach((chip) => {
    const ok = s.services[chip.dataset.svc];
    chip.classList.toggle("ok", ok === true);
    chip.classList.toggle("bad", ok === false);
  });

  const f = s.feed;
  const active = f.scenario && f.scenario.phase !== "finished" ? f.scenario.scenario : "none";
  document.querySelectorAll(".scenario").forEach((b) => {
    b.disabled = !f.supported_scenarios.includes(b.dataset.scenario);
    b.title = b.disabled ? "Not in this simulator version: merge main (Member A's A8 scenarios)" : "";
    b.classList.toggle("active", b.dataset.scenario === active && !f.paused);
  });
  $("#malformed").checked = f.settings.malformed_rate > 0;
  $("#plMalformed").classList.toggle("on", f.settings.malformed_rate > 0);
  $("#plMalformed").textContent = f.settings.malformed_rate > 0 ? "Stop breaking" : "Break 5%";
  $("#malformedCount").textContent = f.malformed_sent;
  $("#pauseFeed").disabled = f.paused;
  $("#resumeFeed").disabled = !f.paused;
  focusPatient = f.scenario && f.scenario.phase !== "finished" ? f.scenario.patient : null;
  renderScenario(f);
  renderFeed(f);

  $("#links").innerHTML = [
    ["API docs", s.tools.api_docs], ["Kafka UI", s.tools.kafka_ui], ["Spark UI", s.tools.spark_ui],
    ["Airflow", s.tools.airflow], ["Grafana", s.tools.grafana], ["Prometheus alerts", s.tools.prometheus_alerts],
  ].map(([n, u]) => `<a href="${esc(u)}" target="_blank" rel="noopener">${esc(n)} &#8599;</a>`).join("");

  $("#timeline").innerHTML = s.actions.map((a) => `
    <li><time>${fmtTime(a.at)}</time> ${esc(a.title)}${a.detail ? `<small class="tech">${esc(a.detail)}</small>` : ""}</li>`).join("")
    || `<li class="muted">Nothing yet</li>`;
}

function renderScenario(f) {
  const box = $("#scenarioStatus");
  const run = f.scenario;
  if (f.paused) {
    box.hidden = false;
    box.innerHTML = `<span class="phase">Feed paused</span>`;
    return;
  }
  if (!run) { box.hidden = true; return; }
  const meta = SCENARIOS.find((s) => s.id === run.scenario);
  const left = run.phase === "starting" ? run.seconds_to_start : run.seconds_left;
  const who = run.patient ? ` &middot; ${esc(run.patient)}` : "";
  const m = (label, v) => `<li class="${v != null ? "done" : ""}"><span>${label}</span><strong>${v != null ? secs(v) : "waiting…"}</strong></li>`;
  const milestones = run.scenario === "outage"
    ? m("Board stale", run.stale_after_s)
    : m("First alert", run.first_alert_after_s) + m("CONCERNING", run.concerning_after_s);
  box.hidden = false;
  box.innerHTML = `
    <div><span class="phase">${esc(meta ? meta.name : run.scenario)}</span>${who}</div>
    <div class="muted">${PHASE_TEXT[run.phase]}${run.phase === "finished" ? "" : " " + secs(left)}</div>
    <ul>${milestones}</ul>`;
}

function renderFeed(f) {
  $("#partitions").innerHTML = [0, 1, 2].map((p) =>
    `<div><b>${f.per_partition[p] ?? 0}</b>partition ${p}</div>`).join("");
  $("#feed").innerHTML = f.recent.map((r) =>
    `<div class="${r.malformed ? "bad" : ""}"><span class="p">p${r.partition}</span> ${esc(r.key)} ${esc(r.payload)}</div>`).join("");
}

// -------------------------------------------------------------------- ward --
async function refreshWard() {
  let data;
  try { data = await get("/demo/api/ward"); }
  catch (e) {
    $("#tiles").innerHTML = `<div class="empty">API not reachable</div>`;
    return;
  }
  const patients = data.patients;
  const now = Date.now();
  const counts = { NORMAL: 0, WATCH: 0, CONCERNING: 0 };
  for (const p of patients) {
    if (!p.stale) counts[p.vital_risk.category] = (counts[p.vital_risk.category] || 0) + 1;
    const h = (history[p.patient_id] ||= []);       // one point per poll: ~5 min of history
    h.push({ t: now, hr: p.vitals.heart_rate.avg, spo2: p.vitals.spo2.min });
    if (h.length > 120) h.shift();
  }
  const alertsHour = (data.metrics.alerts_last_hour || []).reduce((a, r) => a + r.alerts, 0);
  $("#counts").innerHTML = `
    <div class="count normal"><strong>${counts.NORMAL}</strong><span>NORMAL</span></div>
    <div class="count watch"><strong>${counts.WATCH}</strong><span>WATCH</span></div>
    <div class="count concerning"><strong>${counts.CONCERNING}</strong><span>CONCERNING</span></div>
    <div class="count alerts"><strong>${alertsHour}</strong><span>ALERTS / H</span></div>`;

  $("#tiles").innerHTML = patients.length ? patients.map(tile).join("")
    : `<div class="empty">Waiting for the first windows&hellip;</div>`;
  if (drawerPatient) renderDrawer(drawerPatient, false);
}

function tile(p) {
  const cat = p.stale ? "STALE" : p.vital_risk.category;
  const v = p.vitals;
  const hr = v.heart_rate.avg, spo2 = v.spo2.min, sys = v.systolic_bp.max, temp = v.temperature.max;
  const reasons = new Set(p.vital_risk.reasons.map((r) => r.split("_")[0]));
  const trendHr = p.trends.heart_rate === "RISING" ? `<span class="trend up" title="rising">&uarr;</span>` : "";
  const trendSp = p.trends.spo2 === "FALLING" ? `<span class="trend down" title="falling">&darr;</span>` : "";
  const cls = `${cat.toLowerCase()}${p.patient_id === focusPatient ? " focus" : ""}${p.stale ? " stale" : ""}`;
  const pill = p.alerts_last_10m ? `<span class="alert-pill">${p.alerts_last_10m} alert${p.alerts_last_10m > 1 ? "s" : ""}</span>` : "";
  return `
  <button class="tile ${cls}" data-patient="${esc(p.patient_id)}">
    ${pill}
    <div class="tile-head"><b>${esc(p.patient_id)}</b></div>
    <span class="badge ${cat}">${p.stale ? "NO DATA" : cat} &middot; ${p.vital_risk.score}</span>
    <div class="vitals">
      <div class="${reasons.has("heart") ? "bad" : ""}">HR <b>${hr ?? "–"}</b>${trendHr}</div>
      <div class="${reasons.has("spo2") ? "bad" : ""}">SpO₂ <b>${spo2 ?? "–"}</b>${trendSp}</div>
      <div class="${reasons.has("systolic") ? "bad" : ""}">BP <b>${sys ?? "–"}</b></div>
      <div class="${reasons.has("temperature") ? "bad" : ""}">Temp <b>${temp ?? "–"}</b></div>
    </div>
  </button>`;
}

// ------------------------------------------------------------------ alerts --
async function refreshAlerts() {
  let data;
  try { data = await get("/demo/api/alerts?minutes=15"); } catch { return; }
  const list = data.alerts;
  $("#alerts").innerHTML = list.length ? list.map((a) => {
    const isNew = !firstAlertLoad && !seenAlerts.has(a.alert_id);
    seenAlerts.add(a.alert_id);
    const what = mode === "tech" ? `${esc(a.rule)} &middot; ${esc(a.alert_type)}` : esc(plainRule(a));
    return `<div class="alert ${esc(a.severity)}${isNew ? " new" : ""}">
      <div class="alert-top"><b>${esc(a.patient_id)} &middot; ${esc(a.severity)}</b><small>${fmtTime(a.event_time)}</small></div>
      <div>${what}</div>
      <small>${esc(a.metric)} ${a.metric_value ?? ""} (limit ${a.threshold ?? "–"})</small>
    </div>`;
  }).join("") : `<div class="empty">No alerts</div>`;
  firstAlertLoad = false;
}

function plainRule(a) {
  const m = { heart_rate: "Heart rate", spo2: "Oxygen level", systolic_bp: "Blood pressure", temperature: "Temperature" }[a.metric] || a.metric;
  const dir = /LOW/.test(a.rule) ? "too low" : "too high";
  return a.alert_type === "SUSTAINED" ? `${m} ${dir} for several readings in a row` : `${m} ${dir}`;
}

// ------------------------------------------------------------------- batch --
async function refreshBatch() {
  let b;
  try { b = await get("/demo/api/batch"); } catch { return; }
  $("#riskDay").textContent = b.sim_day != null ? `· day ${b.sim_day}` : "";
  const reports = b.reports.find((r) => r.sim_day === b.sim_day) || b.reports[0];
  $("#reportLinks").innerHTML = reports ? reports.files.map((f) =>
    `<a href="/reports/${encodeURIComponent(f)}" target="_blank" rel="noopener">${esc(f.endsWith(".html") ? "Report" : "CSV")} &#8599;</a>`).join("") : "";

  $("#dagRuns").innerHTML = b.dag_runs == null
    ? `<div class="muted">Airflow not reachable</div>`
    : b.dag_runs.slice(0, 1).map((r) => `
      <div class="dag-run">
        <div class="who"><b>${r.sim_day != null ? `Day ${esc(r.sim_day)}` : "Latest run"}</b><br><span class="run-state ${esc(r.state)}">${esc(r.state)}</span> <small>${fmtTime(r.start_date)}</small></div>
        <div class="tasks">${r.tasks.map((t) => `<span class="task ${esc(t.state || "none")}" title="${esc(t.task_id)}: ${esc(t.state || "not started")}">${esc(shortTask(t.task_id))}</span>`).join("")}</div>
      </div>`).join("") || `<div class="muted">No runs yet</div>`;

  const rows = b.daily_risk;
  $("#riskTable").innerHTML = rows.length ? `
    <thead><tr><th>Patient</th><th>Vitals only</th><th></th><th>With labs</th><th>Score</th><th class="tech">Vital reasons</th><th>Abnormal labs</th></tr></thead>
    <tbody>${rows.map((r) => `
      <tr class="${r.labs_changed_category ? "changed" : ""}">
        <td><b>${esc(r.patient_id)}</b></td>
        <td><span class="badge ${esc(r.vitals_only_category)}">${esc(r.vitals_only_category)}</span></td>
        <td class="arrow">&rarr;</td>
        <td><span class="badge ${esc(r.risk_category)}">${esc(r.risk_category)}</span></td>
        <td class="score">${r.vital_risk_score ?? 0} + ${r.lab_risk_score ?? "n/a"} = <b>${r.total_risk_score}</b></td>
        <td class="tech">${esc((r.vital_reasons || "").replaceAll(",", ", ")) || "–"}</td>
        <td>${r.lab_status === "LAB_UNAVAILABLE" ? `<i class="muted">no labs</i>` : esc((r.abnormal_labs || "").replaceAll(",", ", ")) || "–"}${r.lab_sim_day != null && r.lab_sim_day !== b.sim_day ? ` <small class="muted">(day ${r.lab_sim_day})</small>` : ""}</td>
      </tr>`).join("")}</tbody>`
    : `<tbody><tr><td class="muted">No daily risk yet</td></tr></tbody>`;

  $("#health").innerHTML = b.health.map((h) => {
    const d = h.details || {};
    const what = d.error || d.task_id || (d.sim_day != null ? `day ${d.sim_day}` : "");
    return `<span class="hevent ${esc(h.status)}" title="${esc(JSON.stringify(d))}">${fmtTime(h.checked_at)} ${esc(h.status)} ${esc(h.check_name)} ${esc(String(what).slice(0, 60))}</span>`;
  }).join("");
}

function shortTask(id) {
  return { resolve_sim_day: "resolve day", wait_for_lab_file: "wait for file", validate_lab_file: "validate",
    load_lab_results: "load labs", wait_for_lake_settle: "wait for lake", vital_daily_summary: "summary",
    risk_consolidation: "risk join", generate_report: "report" }[id] || id;
}

// ------------------------------------------------------------------ drawer --
function openDrawer(pid) {
  drawerPatient = pid;
  $("#drawer").hidden = false;
  $("#dTitle").textContent = pid;
  $("#dBody").innerHTML = `<p class="muted">Loading…</p>`;
  renderDrawer(pid, true);
}
function closeDrawer() { drawerPatient = null; $("#drawer").hidden = true; }

async function renderDrawer(pid) {
  let d;
  try { d = await get(`/demo/api/patient/${pid}`); }
  catch (e) { $("#dBody").innerHTML = `<p class="muted">${esc(e.message)}</p>`; return; }
  if (drawerPatient !== pid) return;
  const c = d.combined, sp = d.speed_view, labs = d.batch_view.labs;
  const labScore = c.lab_risk_score ?? "n/a";
  const h = history[pid] || [];
  $("#dBody").innerHTML = `
    <div class="equation">
      <div class="eq-box"><small>Vitals (live)</small><strong>${c.vital_risk_score ?? "–"}</strong></div>
      <span class="eq-op">+</span>
      <div class="eq-box"><small>Labs (day ${c.lab_sim_day ?? "–"})</small><strong>${labScore}</strong></div>
      <span class="eq-op">=</span>
      <div class="eq-box total ${esc(c.risk_category)}"><small>${esc(c.risk_category || "")}</small><strong>${c.total_risk_score ?? "–"}</strong></div>
    </div>
    ${c.lab_status === "LAB_UNAVAILABLE" ? `<p class="note">No recent labs: vitals only</p>` : ""}
    ${sp ? `<div class="reasons">${sp.vital_risk.reasons.map((r) => `<span>${esc(r.replaceAll("_", " "))}</span>`).join("")}</div>` : ""}
    <div class="spark-row">
      <div class="spark"><small>Heart rate <b>${sp?.vitals.heart_rate.avg ?? "–"}</b></small>${sparkline(h.map((x) => x.hr), 50, 120, true)}</div>
      <div class="spark"><small>SpO₂ <b>${sp?.vitals.spo2.min ?? "–"}</b></small>${sparkline(h.map((x) => x.spo2), 92, null, false)}</div>
    </div>
    <h3>Lab results${labs.sim_day != null ? ` · day ${labs.sim_day}` : ""}</h3>
    ${labs.results.length ? `<table><thead><tr><th>Test</th><th>Value</th><th>Range</th><th>Flag</th></tr></thead><tbody>
      ${labs.results.map((r) => `<tr><td>${esc(r.test_type)}</td><td>${r.value} ${esc(r.unit)}</td><td>${r.reference_low}–${r.reference_high}</td>
      <td class="flag-${esc(r.flag)}">${esc(r.flag || "ok")}${r.points ? ` (+${r.points})` : ""}</td></tr>`).join("")}</tbody></table>`
      : `<p class="muted">No lab results</p>`}
    <h3>Recent alerts</h3>
    ${d.recent_alerts.length ? d.recent_alerts.map((a) => `<div class="alert ${esc(a.severity)}"><div class="alert-top"><b>${esc(a.severity)}</b><small>${fmtTime(a.event_time)}</small></div><div>${mode === "tech" ? esc(a.rule) : esc(plainRule(a))}</div></div>`).join("")
      : `<p class="muted">No alerts</p>`}`;
}

function sparkline(values, limit, upper, highBad) {
  const pts = values.filter((v) => v != null);
  if (pts.length < 2) return `<svg viewBox="0 0 200 56"><text x="4" y="32" font-size="11" fill="#647083">collecting…</text></svg>`;
  const lo = Math.min(...pts, limit ?? Infinity, upper ?? Infinity) - 2;
  const hi = Math.max(...pts, limit ?? -Infinity, upper ?? -Infinity) + 2;
  const y = (v) => 52 - ((v - lo) / (hi - lo)) * 48;
  const x = (i) => (i / (pts.length - 1)) * 196 + 2;
  const path = pts.map((v, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join("");
  const lines = [limit, upper].filter((v) => v != null).map((v) =>
    `<line x1="0" x2="200" y1="${y(v)}" y2="${y(v)}" stroke="#c62f2f" stroke-dasharray="3 3" stroke-width="1" opacity=".6"/>`).join("");
  const last = pts[pts.length - 1];
  const bad = highBad ? last > (upper ?? Infinity) || last < limit : last < limit;
  return `<svg viewBox="0 0 200 56" preserveAspectRatio="none">${lines}
    <path d="${path}" fill="none" stroke="${bad ? "#c62f2f" : "#2456c8"}" stroke-width="2" vector-effect="non-scaling-stroke"/></svg>`;
}

// -------------------------------------------------------------------- loop --
setup();
refreshState(); refreshWard(); refreshAlerts(); refreshBatch();
setInterval(refreshState, 2000);
setInterval(refreshWard, 2500);
setInterval(refreshAlerts, 3000);
setInterval(refreshBatch, 8000);
