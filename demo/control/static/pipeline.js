// Live pipeline tab: the Lambda architecture as one diagram, animated by the live pipeline.
// Every moving dot stands for something that really happened since the last poll: a reading
// Kafka acknowledged, a Spark micro-batch, a dead-lettered record, an alert, a DAG task.
"use strict";

const Pipeline = (() => {
  const NS = "http://www.w3.org/2000/svg";
  const POLL_MS = 2000;
  const C = { p0: "#2f6bff", p1: "#0ea5b7", p2: "#e0409a", bad: "#e5322d", focus: "#f2a900",
              window: "#17a558", alert: "#f07d00", batch: "#7c4dff", read: "#64748b" };
  const FULL = { x: 8, y: 56, w: 1790, h: 836 };   // the whole diagram (matches the SVG's viewBox)
  const PART = [C.p0, C.p1, C.p2];
  const LANE_Y = [196, 263, 330];                 // tops of Kafka's three partition lanes
  const laneMid = (p) => LANE_Y[p] + 40;
  const TASKS = [["resolve_sim_day", "resolve day"], ["wait_for_lab_file", "wait for file"],
    ["validate_lab_file", "validate"], ["load_lab_results", "load labs"],
    ["wait_for_lake_settle", "wait for lake"], ["vital_daily_summary", "vitals summary"],
    ["risk_consolidation", "risk join"], ["generate_report", "report"]];

  // ------------------------------------------------------------------ layout --
  const NODES = [
    { id: "mon", x: 30, y: 130, w: 230, h: 250, icon: "pulse", title: "Monitors",
      story: "one per bed, every 2–5 s", tech: "vital_producer · seed 42" },
    { id: "kafka", x: 320, y: 110, w: 260, h: 300, icon: "queue", title: "Kafka",
      story: "stores every reading", tech: "topic patient-vitals" },
    { id: "spark", x: 650, y: 100, w: 350, h: 300, icon: "bolt", title: "Spark Streaming",
      story: "small batches, every 10 s", tech: "trigger 10 s · watermark 1 min" },
    { id: "dlq", x: 1055, y: 88, w: 210, h: 100, icon: "bin", title: "Dead letters",
      story: "broken readings", tech: "topic vitals-dlq" },
    { id: "alerts", x: 1055, y: 222, w: 210, h: 100, icon: "bell", title: "Alerts",
      story: "raised by the rules", tech: "topic patient-alerts" },
    { id: "cons", x: 1055, y: 350, w: 210, h: 102, icon: "save", title: "Alert store",
      story: "saved exactly once", tech: "alert_consumer.py" },
    { id: "lab", x: 30, y: 560, w: 230, h: 190, icon: "flask", title: "Hospital lab",
      story: "one lab file per day", tech: "lab_batch · 6 tests" },
    { id: "land", x: 320, y: 560, w: 260, h: 190, icon: "folder", title: "Landing zone",
      story: "files wait to be checked", tech: "data/landing/labs/" },
    { id: "af", x: 650, y: 520, w: 350, h: 310, icon: "clock", title: "Airflow",
      story: "runs the daily job", tech: "DAG daily_lab_consolidation" },
    { id: "lake", x: 1055, y: 500, w: 210, h: 120, icon: "waves", title: "Data lake",
      story: "every reading, kept", tech: "Parquet · by sim_day" },
    { id: "sb", x: 1055, y: 665, w: 210, h: 150, icon: "bolt", title: "Spark batch",
      story: "daily summary + risk", tech: "summary · risk join" },
    { id: "pg", x: 1320, y: 110, w: 240, h: 720, icon: "db", title: "PostgreSQL",
      story: "the serving database", tech: "ward DB · speed + batch" },
    { id: "obs", x: 1596, y: 110, w: 190, h: 210, icon: "gauge", title: "Monitoring",
      story: "watches every stage", tech: "Prometheus · Grafana" },
    { id: "api", x: 1596, y: 360, w: 190, h: 160, icon: "api", title: "Serving API",
      story: "live + daily, merged", tech: "FastAPI · combine()" },
    { id: "dash", x: 1596, y: 580, w: 190, h: 250, icon: "screen", title: "Ward board",
      story: "what the nurses see", tech: "GET /api/patients" },
  ];
  const BANDS = [
    { id: "speed", name: "SPEED LAYER", x: 14, y: 62, w: 1276, h: 400 },
    { id: "batch", name: "BATCH LAYER", x: 14, y: 478, w: 1276, h: 406 },
    { id: "serving", name: "SERVING LAYER", x: 1300, y: 62, w: 492, h: 822 },
  ];
  const LAYER = { mon: "speed", kafka: "speed", spark: "speed", dlq: "speed", alerts: "speed", cons: "speed",
    lab: "batch", land: "batch", af: "batch", lake: "batch", sb: "batch",
    pg: "serving", obs: "serving", api: "serving", dash: "serving" };
  const Q_ROWS = [
    { id: "q1", y: 180, story: "Q1 · live status", tech: "Q1 · 2-min windows", queries: ["q1_windows"] },
    { id: "q3", y: 250, story: "Q3 · alert rules", tech: "Q3 · threshold + sustained", queries: ["q3_threshold_alerts", "q3_sustained_alerts"] },
    { id: "q2", y: 320, story: "Q2 · archive", tech: "Q2 · Parquet archive", queries: ["q2_archive"] },
  ];
  const PG_ROWS = [
    { id: "status", y: 181, story: "Live status", tech: "patient_current_status" },
    { id: "valerts", y: 367, story: "Alerts", tech: "vital_alerts" },
    { id: "summary", y: 620, story: "Daily vitals summary", tech: "vital_daily_summary" },
    { id: "risk", y: 686, story: "Daily risk", tech: "daily_patient_risk" },
    { id: "labs", y: 752, story: "Lab results", tech: "lab_results" },
  ];
  const EDGES = {
    ...Object.fromEntries([0, 1, 2].map((p) => [`mon_k${p}`,
      `M260,255 C298,255 298,${laneMid(p)} 336,${laneMid(p)} L564,${laneMid(p)}`])),
    ...Object.fromEntries([0, 1, 2].map((p) => [`k${p}_sp`,
      `M564,${laneMid(p)} C612,${laneMid(p)} 610,280 650,280`])),
    q1_pg: "M984,210 L1334,210",
    q1_dlq: "M984,196 C1022,196 1020,138 1055,138",
    q3_al: "M984,280 L1055,280",
    al_cons: "M1160,322 L1160,350",
    cons_pg: "M1265,396 L1334,396",
    q2_lake: "M984,350 L1018,350 Q1030,350 1030,362 L1030,548 Q1030,560 1042,560 L1055,560",
    lake_sb: "M1160,620 L1160,665",
    lab_land: "M260,655 L320,655",
    land_af: "M580,655 L650,655",
    af_sb: "M1000,740 L1055,740",
    sb_sum: "M1265,700 L1288,700 L1288,649 L1334,649",
    sb_risk: "M1265,735 L1300,735 L1300,715 L1334,715",
    af_labs: "M900,830 L900,862 L1312,862 L1312,781 L1334,781",
    pg_api_s: "M1546,210 L1576,210 L1576,420 L1596,420",
    pg_api_b: "M1546,748 L1576,748 L1576,470 L1596,470",
    api_dash: "M1691,520 L1691,580",
  };
  const ICONS = {
    pulse: "M2 12h4l2-5 4 10 2-5h8",
    queue: "M4 6h16M4 12h16M4 18h16M7 4v4M12 10v4M17 16v4",
    bolt: "M13 2L4 14h7l-1 8 9-12h-7z",
    bin: "M4 7h16M9 7V4h6v3M6 7l1 13h10l1-13",
    bell: "M6 16v-5a6 6 0 0 1 12 0v5l2 2H4zM10 20a2 2 0 0 0 4 0",
    save: "M5 3h11l3 3v15H5zM8 3v6h8V3M8 21v-7h8v7",
    flask: "M9 3h6M10 3v6l-5 10a1.5 1.5 0 0 0 1.3 2h11.4a1.5 1.5 0 0 0 1.3-2L14 9V3M7.5 15h9",
    folder: "M3 6h6l2 2h10v11H3z",
    clock: "M12 3a9 9 0 1 0 0 18 9 9 0 0 0 0-18zM12 7v5l3 3",
    waves: "M2 7c3-3 5 3 8 0s5 3 8 0 4 0 4 0M2 13c3-3 5 3 8 0s5 3 8 0 4 0 4 0M2 19c3-3 5 3 8 0s5 3 8 0 4 0 4 0",
    db: "M4 6c0-2.7 16-2.7 16 0v12c0 2.7-16 2.7-16 0zM4 6c0 2.7 16 2.7 16 0M4 12c0 2.7 16 2.7 16 0",
    gauge: "M4 18a8 8 0 1 1 16 0M12 18l4-6",
    api: "M8 7l-5 5 5 5M16 7l5 5-5 5M14 4l-4 16",
    screen: "M3 4h18v12H3zM8 20h8M12 16v4",
  };

  // ------------------------------------------------------------------ helpers --
  const el = (tag, attrs = {}, parent = null, text = null) => {
    const e = document.createElementNS(NS, tag);
    for (const [k, v] of Object.entries(attrs)) if (v != null) e.setAttribute(k, v);
    if (text != null) e.textContent = text;
    if (parent) parent.appendChild(e);
    return e;
  };
  const byId = (id) => document.getElementById(id);
  const setText = (id, v) => { const e = byId(id); if (e && e.textContent !== String(v)) e.textContent = v; };
  const num = (v) => v == null ? "–" : Math.round(v).toLocaleString("en-US");
  const tech = () => document.body.classList.contains("mode-tech");
  const escHtml = (v) => String(v ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

  let svg, gEdges, gParticles, built = false;
  const paths = {};            // edge id -> {el, len, lastActive}
  const particles = [];
  let prev = null, timer = null, raf = null, visible = false;
  let lastMajorCaption = 0;
  const captions = [];
  const seenAlerts = new Set();
  let rate = { value: 0, samples: [] };

  // ------------------------------------------------------------------- build --
  function build() {
    svg = byId("plSvg");
    const defs = el("defs", {}, svg);
    const marker = el("marker", { id: "plArrow", viewBox: "0 0 10 10", refX: 8, refY: 5,
      markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse" }, defs);
    el("path", { d: "M0,0 L10,5 L0,10 z", class: "pl-arrow" }, marker);

    for (const b of BANDS) band(b);

    gEdges = el("g", {}, svg);
    for (const [id, d] of Object.entries(EDGES)) {
      const p = el("path", { d, id: `e-${id}`, class: "pl-edge",
        "marker-end": id.startsWith("mon_k") ? null : "url(#plArrow)" }, gEdges);
      paths[id] = { el: p, len: 0, lastActive: -Infinity };
    }
    for (const n of NODES) node(n);
    kafkaLanes(); sparkRows(); airflowTasks(); pgRows(); nodeMetrics();

    gParticles = el("g", {}, svg);
    for (const p of Object.values(paths)) p.len = p.el.getTotalLength();
    setupZoom();
    built = true;
  }

  function band(b) {
    el("rect", { id: `band-${b.id}`, x: b.x, y: b.y, width: b.w, height: b.h, rx: 22, class: `pl-band ${b.id}` }, svg);
    const g = el("g", { class: `pl-band-tag ${b.id}` }, svg);
    const pill = el("rect", { x: b.x + 16, y: b.y + 10, height: 34, rx: 9 }, g);
    const t = el("text", { x: b.x + 30, y: b.y + 34, class: "pl-band-label" }, g, b.name);
    pill.setAttribute("width", t.getBBox().width + 28);
  }

  function textPair(parent, x, y, story, techText, cls, anchor = "start") {
    el("text", { x, y, class: `${cls} story`, "text-anchor": anchor }, parent, story);
    el("text", { x, y, class: `${cls} tech`, "text-anchor": anchor }, parent, techText);
  }

  function node(n) {
    const g = el("g", { id: `n-${n.id}`, class: `pl-node layer-${LAYER[n.id]}` }, svg);
    el("rect", { x: n.x, y: n.y, width: n.w, height: n.h, rx: 16, class: "pl-box" }, g);
    const icon = el("g", { transform: `translate(${n.x + 16},${n.y + 14}) scale(1.15)` }, g);
    el("path", { d: ICONS[n.icon], class: "pl-icon" }, icon);
    el("text", { x: n.x + 50, y: n.y + 36, class: "pl-title" }, g, n.title);
    textPair(g, n.x + 18, n.y + 62, n.story, n.tech, "pl-sub");
    n.g = g;
  }

  const metric = (id, x, y, cls, text = "–", anchor = "start", parent = svg) =>
    el("text", { id, x, y, class: cls, "text-anchor": anchor }, parent, text);

  function kafkaLanes() {
    const g = byId("n-kafka");
    [0, 1, 2].forEach((p) => {
      el("rect", { x: 336, y: LANE_Y[p], width: 228, height: 56, rx: 10, class: "pl-lane",
        style: `stroke:${PART[p]}` }, g);
      el("text", { x: 350, y: LANE_Y[p] + 23, class: "pl-lane-name", fill: PART[p] }, g, `partition ${p}`);
      metric(`k-p${p}`, 550, LANE_Y[p] + 23, "pl-lane-count", "–", "end", g);
    });
  }

  function sparkRows() {
    const g = byId("n-spark");
    for (const r of Q_ROWS) {
      el("rect", { id: `row-${r.id}`, x: 666, y: r.y, width: 318, height: 60, rx: 10, class: "pl-row" }, g);
      textPair(g, 680, r.y + 25, r.story, r.tech, "pl-row-title");
      metric(`q-${r.id}`, 680, r.y + 49, "pl-row-metric", "waiting for metrics…", "start", g);
    }
  }

  function airflowTasks() {
    const g = byId("n-af");
    metric("af-run", 668, 616, "pl-m-med", "–", "start", g);
    TASKS.forEach(([id, label], i) => {
      const x = i < 4 ? 666 : 830, y = 636 + (i % 4) * 48;
      el("rect", { id: `task-${id}`, x, y, width: 154, height: 40, rx: 9, class: "pl-task none" }, g);
      el("text", { x: x + 12, y: y + 26, class: "pl-task-text" }, g, `${i + 1} ${label}`);
    });
  }

  function pgRows() {
    const g = byId("n-pg");
    for (const r of PG_ROWS) {
      el("rect", { id: `pgrow-${r.id}`, x: 1334, y: r.y, width: 212, height: 58, rx: 10, class: "pl-row" }, g);
      textPair(g, 1346, r.y + 22, r.story, r.tech, "pl-pg-name");
      metric(`pg-${r.id}`, 1346, r.y + 46, "pl-pg-val", "–", "start", g);
    }
  }

  function nodeMetrics() {
    const g = (id) => byId(`n-${id}`);
    metric("mon-rate", 48, 270, "pl-m-big", "–", "start", g("mon"));
    metric("mon-unit", 48, 298, "pl-m-unit", "readings / second", "start", g("mon"));
    metric("mon-sent", 48, 336, "pl-m-med", "–", "start", g("mon"));
    metric("mon-state", 48, 364, "pl-m-small", "–", "start", g("mon"));
    const pill = el("g", { id: "mon-pill", class: "pl-pill", visibility: "hidden" }, g("mon"));
    el("rect", { x: 30, y: 392, width: 230, height: 44, rx: 22 }, pill);
    metric("mon-pill-text", 145, 420, "pl-pill-text", "", "middle", pill);

    metric("dlq-count", 1073, 176, "pl-m-med", "–", "start", g("dlq"));
    metric("alerts-count", 1073, 310, "pl-m-med", "–", "start", g("alerts"));
    metric("cons-count", 1073, 440, "pl-m-med", "–", "start", g("cons"));

    metric("lab-next", 48, 680, "pl-m-big", "–", "start", g("lab"));
    metric("lab-unit", 48, 706, "pl-m-unit", "until the next lab file", "start", g("lab"));
    metric("lab-day", 48, 736, "pl-m-small", "–", "start", g("lab"));
    metric("land-file", 338, 672, "pl-m-mono", "–", "start", g("land"));
    metric("land-state", 338, 704, "pl-m-med", "–", "start", g("land"));
    metric("land-rows", 338, 734, "pl-m-small", "–", "start", g("land"));

    metric("lake-day", 1073, 590, "pl-m-med", "–", "start", g("lake"));
    metric("lake-size", 1073, 612, "pl-m-small", "–", "start", g("lake"));
    metric("sb-state", 1073, 758, "pl-m-med", "idle", "start", g("sb"));
    metric("sb-detail", 1073, 790, "pl-m-small", "–", "start", g("sb"));

    metric("obs-lag", 1614, 208, "pl-m-small", "–", "start", g("obs"));
    metric("obs-batch", 1614, 236, "pl-m-small", "–", "start", g("obs"));
    metric("obs-firing-l", 1614, 266, "pl-m-small", "Alerts firing:", "start", g("obs"));
    metric("obs-firing", 1614, 296, "pl-m-med", "–", "start", g("obs"));
    metric("api-state", 1614, 458, "pl-m-med", "–", "start", g("api"));
    textPair(g("api"), 1614, 490, "one score per patient", "speed ⊕ batch view", "pl-m-small");
    const counts = [["normal", "Normal", 684], ["watch", "Watch", 726], ["concerning", "Concerning", 768]];
    for (const [id, label, y] of counts) {
      el("circle", { cx: 1624, cy: y - 8, r: 9, class: `pl-dot ${id}` }, g("dash"));
      metric(`dash-${id}`, 1644, y, "pl-m-count", "–", "start", g("dash"));
      metric(`dash-${id}-l`, 1690, y, "pl-m-small", label, "start", g("dash"));
    }
    metric("dash-alerts", 1614, 810, "pl-m-small", "–", "start", g("dash"));
  }

  // -------------------------------------------------------------------- zoom --
  // Click a layer to zoom onto the layer, a box to zoom onto the box; the background or Esc
  // goes back. While zoomed, the page chrome is hidden and the diagram fills the screen.
  // The zoom moves the SVG's own viewBox and the particles live in the same SVG, so every
  // animation keeps running (and stays correct) while zoomed in.
  let view = { ...FULL }, tween = null, zoomed = null;     // zoomed: {kind: node|band, item, focusY}

  function toSvgPoint(evt) {
    const pt = svg.createSVGPoint();
    pt.x = evt.clientX;
    pt.y = evt.clientY;
    return pt.matrixTransform(svg.getScreenCTM().inverse());
  }
  const inside = (p, r) => p.x >= r.x && p.x <= r.x + r.w && p.y >= r.y && p.y <= r.y + r.h;
  const nodeAt = (p) => NODES.find((n) => inside(p, n));
  const bandAt = (p) => BANDS.find((b) => inside(p, b));

  function fit(cx, cy, w, h) {                               // grow w or h to the screen's shape
    const box = svg.getBoundingClientRect();
    const r = box.width && box.height ? box.width / box.height : FULL.w / FULL.h;
    if (w / h < r) w = h * r; else h = w / r;
    return { x: cx - w / 2, y: cy - h / 2, w, h };
  }

  // Keep the view inside the diagram, so a zoom never shows empty space past its edges.
  function clampToDiagram(v) {
    for (const [p, s] of [["x", "w"], ["y", "h"]]) {
      const lo = FULL[p], hi = FULL[p] + FULL[s];
      v[p] = v[s] <= FULL[s] ? Math.min(Math.max(v[p], lo), hi - v[s]) : lo + (FULL[s] - v[s]) / 2;
    }
    return v;
  }

  function zoomTarget(z) {
    const n = z.item;
    const pad = z.kind === "band" ? 30 : 150;   // a box is shown with its connections around it
    const maxH = FULL.h / 1.6;                  // enlarge at least 1.6x: something taller
    if (n.h + pad > maxH) {                     // (PostgreSQL, the serving layer) shows the half
      const top = (z.focusY ?? 0) < n.y + n.h / 2;                      // that was clicked
      const cy = top ? n.y - 20 + maxH / 2 : n.y + n.h + 20 - maxH / 2;
      return clampToDiagram(fit(n.x + n.w / 2, cy, n.w + pad, maxH));
    }
    return clampToDiagram(fit(n.x + n.w / 2, n.y + n.h / 2, n.w + pad, n.h + pad));
  }

  function setViewBox(v) {
    svg.setAttribute("viewBox", `${v.x.toFixed(1)} ${v.y.toFixed(1)} ${v.w.toFixed(1)} ${v.h.toFixed(1)}`);
  }

  function zoomTo(z) {
    zoomed = z;
    document.body.classList.toggle("pl-focus", !!z);         // chrome goes first: the canvas grows
    const target = z ? zoomTarget(z) : { ...FULL };
    tween = { from: { ...view }, to: target, start: performance.now(), dur: 650 };
    svg.classList.toggle("zoomed", z?.kind === "node");
    for (const b of BANDS) svg.classList.toggle(`zoom-${b.id}`, z?.kind === "band" && z.item === b);
    for (const m of NODES) m.g.classList.toggle("focus", z?.kind === "node" && m.id === z.item.id);
    if (!raf) raf = requestAnimationFrame(frame);
  }

  function stepZoom(now) {
    if (!tween) return;
    const t = Math.min(1, (now - tween.start) / tween.dur);
    const e = t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;    // ease in-out
    for (const k of ["x", "y", "w", "h"]) view[k] = tween.from[k] + (tween.to[k] - tween.from[k]) * e;
    setViewBox(view);
    if (t >= 1) tween = null;
  }

  function resetZoom() {
    zoomed = null;
    tween = null;
    view = { ...FULL };
    setViewBox(view);
    document.body.classList.remove("pl-focus");
    svg.classList.remove("zoomed", ...BANDS.map((b) => `zoom-${b.id}`));
    for (const m of NODES) m.g.classList.remove("focus");
  }

  function setupZoom() {
    svg.addEventListener("click", (evt) => {
      const p = toSvgPoint(evt);
      const n = nodeAt(p), b = bandAt(p);
      const same = (kind, item) => zoomed && zoomed.kind === kind && zoomed.item === item;
      if (n) zoomTo(same("node", n) ? null : { kind: "node", item: n, focusY: p.y });
      else if (b) zoomTo(same("band", b) ? null : { kind: "band", item: b, focusY: p.y });
      else zoomTo(null);
    });
    svg.addEventListener("mousemove", (evt) => {
      const p = toSvgPoint(evt);
      svg.style.cursor = nodeAt(p) || bandAt(p) ? "zoom-in" : zoomed ? "zoom-out" : "default";
    });
    document.addEventListener("keydown", (evt) => {
      if (evt.key === "Escape" && zoomed) zoomTo(null);
    });
    window.addEventListener("resize", () => {
      if (!zoomed) return;
      tween = null;
      view = zoomTarget(zoomed);
      setViewBox(view);
    });
  }

  // --------------------------------------------------------------- particles --
  function spawn(edge, color, { delay = 0, r = 7, label = null, square = false, speed = 420,
                                then = null } = {}) {
    const p = paths[edge];
    if (!p || particles.length > 260) return;
    const g = el("g", { class: "pl-particle", visibility: "hidden" }, gParticles);
    if (square) el("rect", { x: -9, y: -11, width: 18, height: 22, rx: 3, fill: color }, g);
    else el("circle", { r, fill: color }, g);
    if (label) el("text", { y: -r - 8, class: "pl-particle-label", "text-anchor": "middle" }, g, label);
    const now = performance.now();
    particles.push({ g, p, start: now + delay, dur: Math.max(550, (p.len / speed) * 1000), then });
    p.lastActive = Math.max(p.lastActive, now + delay);
  }

  // A chain: the next leg starts where the previous one ends (alert: topic → store → table).
  function chain(edges, color, opts = {}, onDone = null) {
    const [first, ...rest] = edges;
    spawn(first, color, { ...opts, then: rest.length ? () => chain(rest, color, { ...opts, delay: 0 }, onDone) : onDone });
  }

  let lastEdgeSweep = 0;
  function frame(now) {
    stepZoom(now);
    for (let i = particles.length - 1; i >= 0; i--) {
      const q = particles[i];
      const t = (now - q.start) / q.dur;
      if (t < 0) continue;
      if (t >= 1) {
        q.g.remove();
        particles.splice(i, 1);
        if (q.then) q.then();
        continue;
      }
      const pt = q.p.el.getPointAtLength(t * q.p.len);
      q.g.setAttribute("transform", `translate(${pt.x.toFixed(1)},${pt.y.toFixed(1)})`);
      q.g.setAttribute("visibility", "visible");
    }
    if (now - lastEdgeSweep > 200) {
      lastEdgeSweep = now;
      for (const p of Object.values(paths)) p.el.classList.toggle("active", now - p.lastActive < 3500 && p.lastActive <= now + 50);
    }
    raf = visible ? requestAnimationFrame(frame) : null;
  }

  function flash(id, cls = "flash") {
    const e = byId(id);
    if (!e) return;
    e.classList.remove(cls);
    void e.getBBox();                       // restart the CSS animation
    e.classList.add(cls);
    clearTimeout(e._flashTimer);
    e._flashTimer = setTimeout(() => e.classList.remove(cls), 1200);
  }

  // ---------------------------------------------------------------- captions --
  function caption(kind, story, techText, minor = false) {
    const now = Date.now();
    if (minor && now - lastMajorCaption < 7000) return;
    if (!minor) lastMajorCaption = now;
    captions.unshift({ kind, story, tech: techText || story, at: new Date() });
    captions.length = Math.min(captions.length, 4);
    renderCaptions();
  }
  function renderCaptions() {
    const box = byId("plTicker");
    if (!captions.length) {
      box.innerHTML = `<div class="cap first"><span class="k k-info"></span><span class="txt">Watching the pipeline… each dot is a real event.</span></div>`;
      return;
    }
    box.innerHTML = captions.map((c, i) => `
      <div class="cap ${i === 0 ? "first" : ""}"><span class="k k-${c.kind}"></span>
        <time>${c.at.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" })}</time>
        <span class="txt">${escHtml(tech() ? c.tech : c.story)}</span></div>`).join("");
  }

  // ------------------------------------------------------------------ update --
  let inFlight = false;
  async function poll() {
    if (inFlight) return;                 // a slow answer must not reorder the snapshots
    inFlight = true;
    let s;
    try {
      const r = await fetch("/demo/api/pipeline");
      if (!r.ok) throw new Error(r.statusText);
      s = await r.json();
    } catch (e) {
      caption("bad", "The control centre is not answering", `GET /demo/api/pipeline failed: ${e.message}`);
      return;
    } finally {
      inFlight = false;
    }
    if (!visible) return;
    if (!prev) (s.db?.latest_alerts || []).forEach((x) => seenAlerts.add(x.alert_id));
    render(s);
    if (prev) animate(prev, s);
    prev = s;
  }

  function render(s) {
    const f = s.feed;
    const t = Date.parse(s.now);
    rate.samples.push({ t, sent: f.sent });
    rate.samples = rate.samples.filter((x) => t - x.t < 12000);
    const a = rate.samples[0], b = rate.samples[rate.samples.length - 1];
    rate.value = b.t > a.t ? ((b.sent - a.sent) * 1000) / (b.t - a.t) : 0;
    setText("mon-rate", f.paused ? "0" : rate.value.toFixed(1));
    setText("mon-sent", `${num(f.sent)} sent`);
    const malformed = f.settings.malformed_rate > 0;
    setText("mon-state", f.paused ? "⏸ feed paused" : malformed ? `⚠ ${Math.round(f.settings.malformed_rate * 100)}% broken on purpose` : "● streaming");
    byId("mon-state").classList.toggle("warn", f.paused || malformed);
    const run = f.scenario && f.scenario.phase !== "finished" ? f.scenario : null;
    const pill = byId("mon-pill");
    pill.setAttribute("visibility", run ? "visible" : "hidden");
    if (run) {
      const name = { spike: "Deterioration", hr_spike: "HR spike", spo2_drop: "O₂ drop", outage: "Outage" }[run.scenario] || run.scenario;
      setText("mon-pill-text", `▶ ${name}${run.patient ? " · " + run.patient : ""}`);
    }

    const vit = s.kafka && s.kafka["patient-vitals"];
    [0, 1, 2].forEach((p) => setText(`k-p${p}`, vit ? num(vit.partitions[p]) : "–"));
    setText("dlq-count", s.kafka && s.kafka["vitals-dlq"] ? `${num(s.kafka["vitals-dlq"].total)} total` : "–");
    setText("alerts-count", s.kafka && s.kafka["patient-alerts"] ? `${num(s.kafka["patient-alerts"].total)} total` : "–");
    byId("n-kafka").classList.toggle("down", !s.kafka);

    const now = Date.parse(s.now) / 1000;
    for (const r of Q_ROWS) {
      const m = s.spark && s.spark[r.queries[0]];
      if (!m) { setText(`q-${r.id}`, s.spark ? "not running" : "metrics offline"); continue; }
      const rows = r.queries.reduce((acc, q) => acc + (s.spark[q]?.input_rows || 0), 0);
      const age = now - m.last_progress_timestamp_seconds;
      setText(`q-${r.id}`, `${num(rows)} rows · batch #${num(m.batch_id)} · ${(m.batch_duration_ms / 1000).toFixed(1)} s`);
      byId(`row-${r.id}`).classList.toggle("stale", age > 120);
    }
    byId("n-spark").classList.toggle("down", !s.spark);

    const db = s.db;
    if (db) {
      const cs = db.current_status, al = db.alerts;
      setText("pg-status", `${cs.patients} patients · ${cs.age_s == null ? "–" : Math.round(cs.age_s) + " s ago"}`);
      setText("pg-valerts", `${num(al.total)} · ${al.last_minute} in last min`);
      setText("pg-summary", db.summary ? `day ${db.summary.sim_day} · ${db.summary.rows} patients` : "no day yet");
      setText("pg-risk", db.risk ? `day ${db.risk.sim_day} · ${db.risk.concerning} concerning` : "no day yet");
      setText("pg-labs", `${num(db.labs.rows)} rows · day ${db.labs.latest_day ?? "–"}`);
      setText("cons-count", `${num(al.total)} stored`);
      setText("dash-normal", cs.normal); setText("dash-watch", cs.watch); setText("dash-concerning", cs.concerning);
      setText("dash-alerts", `${al.last_minute} alert${al.last_minute === 1 ? "" : "s"} / minute`);
    }
    byId("n-pg").classList.toggle("down", !db);

    const c = s.clock;
    const left = Math.max(0, c.day_seconds - c.seconds_into_day);
    setText("lab-next", `${Math.floor(left / 60)}:${String(Math.floor(left % 60)).padStart(2, "0")}`);
    setText("lab-day", `now simulating day ${c.sim_day}`);
    const land = s.landing;
    if (land && land.latest_day != null) {
      setText("land-file", `labs_day=${land.latest_day}.csv`);
      setText("land-state", land.corrupted ? "✗ corrupted (drill)" : land.marker ? "✓ complete" : "… being written");
      byId("land-state").classList.toggle("bad", land.corrupted);
      setText("land-rows", land.rows != null ? `${land.rows} rows · ${land.patients} patients` : "");
    } else setText("land-file", "no lab file yet");

    const lake = s.lake;
    setText("lake-day", lake ? `day ${lake.sim_day}: ${lake.files} files` : "not mounted");
    setText("lake-size", lake ? `${(lake.bytes / 1024).toFixed(0)} KB · ${lake.days} days kept` : "");

    const run2 = s.airflow;
    if (run2) {
      setText("af-run", `${run2.sim_day != null ? `Run for day ${run2.sim_day}` : "Scheduled run"} · ${run2.state}`);
      for (const tk of run2.tasks) byId(`task-${tk.task_id}`).setAttribute("class", `pl-task ${tk.state || "none"}`);
      const st = Object.fromEntries(run2.tasks.map((x) => [x.task_id, x.state]));
      const sbTask = st.risk_consolidation === "running" ? "risk join" : st.vital_daily_summary === "running" ? "vitals summary" : null;
      setText("sb-state", sbTask ? `▶ ${sbTask}` : "idle");
      setText("sb-detail", sbTask ? "spark-submit from Airflow" : `last run: ${run2.state}`);
      byId("n-sb").classList.toggle("busy", !!sbTask);
      byId("n-af").classList.toggle("busy", run2.state === "running" || run2.state === "queued");
    } else setText("af-run", "Airflow API not reachable");
    byId("n-af").classList.toggle("down", !run2);

    const lag = s.spark ? Math.max(0, ...Object.values(s.spark).map((m) => m.kafka_lag_records || 0)) : null;
    setText("obs-lag", lag == null ? "Kafka lag: –" : `Kafka lag: ${num(lag)} rows`);
    const q1 = s.spark && s.spark.q1_windows;
    setText("obs-batch", q1 ? `Q1 batch: ${(q1.batch_duration_ms / 1000).toFixed(1)} s` : "Q1 batch: –");
    const firing = s.prometheus;
    setText("obs-firing", firing == null ? "offline" : firing.length ? firing.join(", ") : "none ✓");
    byId("obs-firing").classList.toggle("bad", !!(firing && firing.length));
    byId("obs-firing").classList.toggle("muted", firing == null);

    setText("api-state", s.api ? "● online" : "● offline");
    byId("api-state").classList.toggle("bad", !s.api);

    renderTimer(f);
  }

  function renderTimer(f) {
    const box = byId("plTimer");
    const run = f.scenario;
    if (!run || (run.phase === "finished" && run.first_alert_after_s == null && run.stale_after_s == null)) {
      box.hidden = true; return;
    }
    const v = (s) => (s == null ? "…" : `${s.toFixed(1)} s`);
    box.hidden = false;
    box.innerHTML = run.scenario === "outage"
      ? `<b>Outage</b> ward board stale after <strong>${v(run.stale_after_s)}</strong>`
      : `<b>${escHtml(run.patient)}</b> first alert <strong>${v(run.first_alert_after_s)}</strong>
         · CONCERNING <strong>${v(run.concerning_after_s)}</strong>`;
  }

  // ----------------------------------------------------------------- animate --
  function animate(a, b) {
    const fa = a.feed, fb = b.feed;
    const focus = fb.scenario && fb.scenario.phase === "active" ? fb.scenario.patient : null;

    // 1. Readings: one dot per message Kafka acknowledged, in its partition's lane.
    const lastAt = Math.max(0, ...fa.recent.map((r) => r.at));
    const fresh = fb.recent.filter((r) => r.at > lastAt);
    [0, 1, 2].forEach((p) => {
      const n = Math.min(Math.max(0, (fb.per_partition[p] || 0) - (fa.per_partition[p] || 0)), 14);
      const known = fresh.filter((r) => r.partition === p);
      for (let i = 0; i < n; i++) {
        const r = known[i];
        const bad = r && r.malformed;
        const isFocus = r && focus && r.key === focus;
        spawn(`mon_k${p}`, bad ? C.bad : isFocus ? C.focus : PART[p], {
          delay: (i * POLL_MS) / Math.max(n, 1) + Math.random() * 120,
          r: isFocus ? 10 : 7, label: isFocus ? r.key : bad ? "broken" : null, speed: 260,
        });
      }
    });
    if (fb.paused && !fa.paused) caption("warn", "The bedside feed is paused: nothing new reaches Kafka", "Producer paused: no records produced to patient-vitals");
    if (!fb.paused && fa.paused) caption("info", "The bedside feed is running again", "Producer resumed");
    if (fb.settings.malformed_rate > 0 && !(fa.settings.malformed_rate > 0))
      caption("bad", "5% of readings are now broken on purpose. Watch them go to the dead letters", "malformed_rate = 0.05: Q1 routes invalid records to vitals-dlq");
    const sa = fa.scenario, sb = fb.scenario;
    if (sb && (!sa || sa.requested_at !== sb.requested_at) && sb.scenario !== "none")
      caption("focus", `Scenario started: ${scenarioText(sb)}`, `--scenario ${sb.scenario} --patient ${sb.patient ?? "all"} (${Math.round(sb.ends_at - sb.starts_at)} s)`);
    if (sb && sa && sa.requested_at === sb.requested_at) {
      if (sb.first_alert_after_s != null && sa.first_alert_after_s == null)
        caption("alert", `First alert for ${sb.patient} stored ${sb.first_alert_after_s.toFixed(1)} s after the change began`, `vital_alerts: first row for ${sb.patient} after ${sb.first_alert_after_s.toFixed(1)} s`);
      if (sb.concerning_after_s != null && sa.concerning_after_s == null)
        caption("alert", `${sb.patient} is now CONCERNING on the ward board (${sb.concerning_after_s.toFixed(0)} s)`, `patient_current_status.vital_risk_category = CONCERNING after ${sb.concerning_after_s.toFixed(1)} s`);
      if (sb.stale_after_s != null && sa.stale_after_s == null)
        caption("warn", `Ward board shows NO DATA after ${sb.stale_after_s.toFixed(0)} s of silence`, `all patients stale after ${sb.stale_after_s.toFixed(1)} s`);
    }

    // 2. Spark micro-batches: Kafka → Spark, then each query's output.
    const qa = a.spark || {}, qb = b.spark || {};
    const newBatch = (q) => qb[q] && qa[q] && qb[q].batch_id > qa[q].batch_id;
    if (newBatch("q1_windows")) {
      const rows = qb.q1_windows.input_rows || 0;
      const per = Math.min(Math.ceil(rows / 3), 4);
      let arrived = 0;
      [0, 1, 2].forEach((p) => {
        for (let i = 0; i < per; i++) spawn(`k${p}_sp`, PART[p], { delay: i * 110, r: 6, speed: 380,
          then: () => { if (++arrived === 1) { flash("row-q1"); q1Output(qb.q1_windows); } } });
      });
      caption("window", `Spark read ${rows} new readings and updated the patients' live status`,
        `Q1 micro-batch #${qb.q1_windows.batch_id}: ${rows} rows in ${(qb.q1_windows.batch_duration_ms / 1000).toFixed(1)} s → upsert patient_current_status`, true);
    }
    if (["q3_threshold_alerts", "q3_sustained_alerts"].some(newBatch)) setTimeout(() => flash("row-q3"), 700);
    if (newBatch("q2_archive")) {
      setTimeout(() => flash("row-q2"), 700);
      spawn("q2_lake", C.batch, { delay: 800, square: true, speed: 300, then: () => flash("n-lake") });
      caption("batch", "A new file of raw readings was saved to the data lake", `Q2 batch #${qb.q2_archive.batch_id}: ${qb.q2_archive.input_rows} rows → Parquet /data/lake/vitals/sim_day=${b.clock.sim_day}`, true);
    }

    // 3. Dead letters and alerts, from the Kafka offsets themselves.
    const d = (topic) => (b.kafka?.[topic]?.total ?? 0) - (a.kafka?.[topic]?.total ?? 0);
    const dlq = a.kafka && b.kafka ? d("vitals-dlq") : 0;
    for (let i = 0; i < Math.min(dlq, 6); i++)
      spawn("q1_dlq", C.bad, { delay: 600 + i * 250, label: i === 0 ? "✗" : null, then: () => flash("n-dlq", "flash-bad") });
    if (dlq > 0) caption("bad", `${dlq} broken reading${dlq > 1 ? "s were" : " was"} set aside, never shown to the nurses`, `Q1: ${dlq} invalid record(s) → vitals-dlq with a reason`);
    // Q3 → topic follows the topic's offsets; topic → store → table follows the stored rows,
    // which arrive a moment later (the alert consumer), and carry the patient's name.
    const al = a.kafka && b.kafka ? d("patient-alerts") : 0;
    for (let i = 0; i < Math.min(al, 5); i++)
      spawn("q3_al", C.alert, { delay: 500 + i * 250, r: 8, then: () => flash("n-alerts") });
    const stored = (b.db?.latest_alerts || []).filter((x) => !seenAlerts.has(x.alert_id));
    stored.forEach((x) => seenAlerts.add(x.alert_id));
    stored.slice(0, 5).forEach((x, i) =>
      chain(["al_cons", "cons_pg"], C.alert, { delay: 300 + i * 350, r: 9, label: x.patient_id },
        () => { flash("pgrow-valerts", "flash-alert"); flash("dash-concerning"); }));
    if (stored.length) {
      const x = stored[0], more = stored.length > 1 ? ` (+${stored.length - 1} more)` : "";
      caption("alert", `Alert: ${x.patient_id}: ${plainAlert(x)}${more}`,
        `Q3 → patient-alerts → vital_alerts: ${x.patient_id} ${x.rule} (${x.severity}), ${x.metric} = ${x.metric_value}${more}`);
    }

    // 4. Serving: the API reads both views and merges them for the ward board.
    spawn("pg_api_s", C.window, { r: 6, speed: 520 });
    spawn("pg_api_b", C.batch, { r: 6, speed: 700, then: () => spawn("api_dash", C.read, { r: 6, speed: 300 }) });

    // 5. Batch layer: lab file → Airflow → Spark batch → daily tables.
    const la = a.landing, lb = b.landing;
    if (lb && la && lb.latest_day !== la.latest_day && lb.latest_day != null) {
      chain(["lab_land"], C.batch, { square: true, speed: 160 }, () => flash("n-land"));
      caption("batch", `The lab results for day ${lb.latest_day} arrived as a file`, `lab_batch wrote labs_day=${lb.latest_day}.csv + _SUCCESS marker`);
    }
    batchAnimation(a.airflow, b.airflow);
  }

  function q1Output(m) {
    for (let i = 0; i < 5; i++) spawn("q1_pg", C.window, { delay: i * 140, r: 7, speed: 520,
      then: i === 4 ? () => { flash("pgrow-status"); flash("n-dash"); } : null });
  }

  function batchAnimation(ra, rb) {
    if (!rb) return;
    const same = ra && ra.run_id === rb.run_id;
    const before = same ? Object.fromEntries(ra.tasks.map((t) => [t.task_id, t.state])) : {};
    const now = Object.fromEntries(rb.tasks.map((t) => [t.task_id, t.state]));
    const became = (t, st) => now[t] === st && before[t] !== st;
    const forDay = rb.sim_day != null ? ` for day ${rb.sim_day}` : "";   // scheduled runs carry no day
    if (!same && ra) caption("batch", `The daily job started${forDay}`, `Airflow: new run ${rb.run_id} (${rb.run_type})`);
    if (became("wait_for_lab_file", "success")) spawn("land_af", C.batch, { square: true, speed: 200, then: () => flash("n-af") });
    if (now.load_lab_results === "running" || became("load_lab_results", "success"))
      spawn("af_labs", C.batch, { square: true, speed: 500, then: () => flash("pgrow-labs") });
    if (became("load_lab_results", "success")) caption("batch", "Checked lab results were loaded into the database", "load_lab_results: lab_results rows for the day replaced (idempotent)");
    if (now.vital_daily_summary === "running" || became("vital_daily_summary", "success")) {
      spawn("af_sb", C.batch, { speed: 260 });
      spawn("lake_sb", C.batch, { square: true, speed: 160, then: () => flash("n-sb") });
    }
    if (became("vital_daily_summary", "success")) {
      spawn("sb_sum", C.batch, { speed: 300, then: () => flash("pgrow-summary") });
      caption("batch", "Spark summarised the whole day of vitals from the lake", "vital_daily_summary: Spark batch over /data/lake/vitals → vital_daily_summary");
    }
    if (now.risk_consolidation === "running") spawn("af_sb", C.batch, { speed: 260 });
    if (became("risk_consolidation", "success")) {
      for (let i = 0; i < 4; i++) spawn("sb_risk", C.batch, { delay: i * 150, speed: 300, then: i === 3 ? () => flash("pgrow-risk") : null });
      caption("batch", `Daily risk${forDay} is ready: vitals and labs combined`, "risk_consolidation: summary ⟕ newest labs → daily_patient_risk");
    }
    if (became("generate_report", "success")) caption("batch", `The daily report${forDay} was written`, "generate_report → data/reports/risk_report_day=N.html/.csv");
    const failed = rb.tasks.find((t) => t.state === "failed" && before[t.task_id] !== "failed");
    if (failed) {
      flash(`task-${failed.task_id}`, "flash-bad"); flash("n-af", "flash-bad");
      caption("bad", failed.task_id === "validate_lab_file"
        ? "The lab file was rejected as a whole: nothing wrong was loaded, the team is alerted"
        : `The daily job failed at “${failed.task_id}”`,
        `${failed.task_id} FAILED → on_failure_callback → pipeline_health FAIL`);
    }
  }

  function scenarioText(s) {
    return { spike: `sudden deterioration of ${s.patient}`, hr_spike: `heart-rate spike for ${s.patient}`,
      spo2_drop: `gradual oxygen drop for ${s.patient}`, outage: "all monitors go silent" }[s.scenario] || s.scenario;
  }
  function plainAlert(x) {
    const m = { heart_rate: "heart rate", spo2: "oxygen level", systolic_bp: "blood pressure", temperature: "temperature" }[x.metric] || x.metric;
    return `${m} ${/LOW/.test(x.rule) ? "too low" : "too high"}${/SUSTAINED/.test(x.rule) ? " for several readings" : ""} (${x.metric_value})`;
  }

  // ------------------------------------------------------------------ public --
  function show() {
    if (!built) build();
    visible = true;
    prev = null;
    renderCaptions();
    poll();
    clearInterval(timer);
    timer = setInterval(poll, POLL_MS);
    if (!raf) raf = requestAnimationFrame(frame);
  }
  function hide() {
    visible = false;
    if (built) resetZoom();
    clearInterval(timer);
    timer = null;
    for (const q of particles) q.g.remove();
    particles.length = 0;
  }
  return { show, hide, renderCaptions };
})();
