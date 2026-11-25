/* gstscope web UI - no dependencies */
(function () {
  "use strict";

  const TOKEN = new URLSearchParams(location.search).get("token") || "";
  const API = "api/v1";
  const HIST = 300;
  const NODE_W = 196, NODE_H = 78, COL_GAP = 78, ROW_GAP = 22, PAD = 20;

  const S = {
    snap: null,
    tab: "pipelines",
    sel: null,          // selected pipeline id
    selEl: null,        // selected element id
    phist: {},          // pipeline id -> {series: [[t, v]]}
    plast: {},          // pipeline id -> last_seen seen
    prhist: {},         // probe id -> {fps: [], kbps: []}
  };

  // ---------------------------------------------------------------- utils
  const $ = (s, r) => (r || document).querySelector(s);
  const el = (tag, attrs, ...kids) => {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
      if (k === "class") e.className = v;
      else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
      else if (v !== undefined && v !== null) e.setAttribute(k, v);
    }
    for (const k of kids) if (k !== null && k !== undefined) e.append(k);
    return e;
  };
  const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g,
    (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const url = (p) => API + p + (TOKEN ? (p.includes("?") ? "&" : "?") + "token=" + encodeURIComponent(TOKEN) : "");
  const api = async (p, opt) => {
    const r = await fetch(url(p), Object.assign({ headers: { "Content-Type": "application/json" } }, opt || {}));
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || r.statusText);
    return j;
  };
  const fmtRate = (k) => k == null ? "–" : k >= 1000 ? (k / 1000).toFixed(k >= 100000 ? 0 : 1) + " Mb/s" : k.toFixed(0) + " kb/s";
  const fmtUs = (u) => u == null ? "–" : u >= 1000 ? (u / 1000).toFixed(u >= 100000 ? 0 : 1) + " ms" : u.toFixed(0) + " µs";
  const fmtFps = (f) => f == null ? "–" : f.toFixed(f >= 100 ? 0 : 1);
  const pct = (x) => x == null ? "–" : Math.round(x * 100) + "%";
  const short = (id) => id.split("/").pop();
  const parentPath = (id) => id.includes("/") ? id.slice(0, id.lastIndexOf("/")) : "";
  const clock = (t) => new Date(t * 1000).toLocaleTimeString([], { hour12: false });
  const push = (arr, v) => { arr.push(v); if (arr.length > HIST) arr.splice(0, arr.length - HIST); };

  // ---------------------------------------------------------------- chart
  function chart(canvas, series, opt) {
    opt = opt || {};
    const dpr = window.devicePixelRatio || 1;
    const w = canvas.clientWidth, h = canvas.clientHeight;
    if (!w || !h) return;
    canvas.width = w * dpr; canvas.height = h * dpr;
    const c = canvas.getContext("2d");
    c.scale(dpr, dpr);
    c.clearRect(0, 0, w, h);
    const all = series.flatMap((s) => s.data);
    if (!all.length) {
      c.fillStyle = "#5c6775"; c.font = "11px system-ui"; c.fillText("waiting for data…", 6, h / 2 + 4);
      return;
    }
    const t1 = Math.max(...all.map((p) => p[0]));
    const t0 = t1 - (opt.span || 120);
    let max = opt.max != null ? opt.max : Math.max(...all.filter((p) => p[0] >= t0).map((p) => p[1]), 0);
    if (max <= 0) max = 1;
    max *= opt.max != null ? 1 : 1.15;
    const X = (t) => ((t - t0) / (t1 - t0)) * (w - 2) + 1;
    const Y = (v) => h - 14 - (v / max) * (h - 22);
    c.strokeStyle = "#232b36"; c.lineWidth = 1;
    c.beginPath(); c.moveTo(0, Y(0) + .5); c.lineTo(w, Y(0) + .5); c.stroke();
    c.setLineDash([2, 3]); c.beginPath(); c.moveTo(0, Y(max / 1.15) + .5); c.lineTo(w, Y(max / 1.15) + .5); c.stroke(); c.setLineDash([]);
    for (const s of series) {
      const d = s.data.filter((p) => p[0] >= t0);
      if (!d.length) continue;
      c.strokeStyle = s.color; c.lineWidth = 1.5; c.beginPath();
      d.forEach((p, i) => (i ? c.lineTo(X(p[0]), Y(p[1])) : c.moveTo(X(p[0]), Y(p[1]))));
      c.stroke();
      if (s.fill) {
        c.lineTo(X(d[d.length - 1][0]), Y(0)); c.lineTo(X(d[0][0]), Y(0)); c.closePath();
        c.fillStyle = s.fill; c.fill();
      }
    }
    c.fillStyle = "#6f7b8a"; c.font = "10.5px ui-monospace, monospace";
    c.fillText((opt.fmt || String)(max / (opt.max != null ? 1 : 1.15)), 4, Y(max / (opt.max != null ? 1 : 1.15)) - 3);
    c.fillText("-" + (opt.span || 120) + "s", 4, h - 2);
  }

  // ---------------------------------------------------------------- tabs
  document.querySelectorAll(".tab").forEach((b) => b.addEventListener("click", () => {
    S.tab = b.dataset.tab;
    document.querySelectorAll(".tab").forEach((x) => x.classList.toggle("active", x === b));
    document.querySelectorAll(".tabpane").forEach((x) => x.classList.toggle("active", x.id === "tab-" + S.tab));
    history.replaceState(null, "", location.pathname + location.search + "#" + S.tab);
    render();
  }));
  if (location.hash === "#probes") $('.tab[data-tab="probes"]').click();

  // ---------------------------------------------------------------- pipelines
  function severity(p, eid) {
    const es = p.elstats[eid] || {};
    const pads = Object.values(p.pads).filter((x) => x.el === eid);
    if (pads.some((x) => x.flow_errors)) return "crit";
    if (es.util != null && es.util >= 1) return "crit";
    if (es.util != null && es.util >= 0.8) return "warn";
    if (es.queue && es.queue.fill >= 0.8) return "warn";
    if (es.qos_dropped) return "warn";
    return "";
  }

  function layout(p) {
    const cols = {};
    for (const e of p.topology.elements) (cols[e.depth] = cols[e.depth] || []).push(e);
    const pos = {};
    const depths = Object.keys(cols).map(Number).sort((a, b) => a - b);
    for (const d of depths) {
      const col = cols[d];
      // barycenter ordering using upstream rows
      col.forEach((e, i) => {
        const ups = p.topology.links.filter((l) => l.sink === e.id && pos[l.src]).map((l) => pos[l.src].row);
        e._bc = ups.length ? ups.reduce((a, b) => a + b, 0) / ups.length : i;
      });
      col.sort((a, b) => a._bc - b._bc);
      let row = 0;
      for (const e of col) {
        row = Math.max(row, Math.round(e._bc));
        pos[e.id] = { col: d, row: row };
        row++;
      }
    }
    let maxCol = 0, maxRow = 0;
    for (const v of Object.values(pos)) { maxCol = Math.max(maxCol, v.col); maxRow = Math.max(maxRow, v.row); }
    for (const v of Object.values(pos)) {
      v.x = PAD + v.col * (NODE_W + COL_GAP);
      v.y = PAD + v.row * (NODE_H + ROW_GAP);
    }
    return { pos, w: PAD * 2 + (maxCol + 1) * NODE_W + maxCol * COL_GAP, h: PAD * 2 + (maxRow + 1) * NODE_H + maxRow * ROW_GAP };
  }

  function drawGraph(p) {
    const svg = $("#graph");
    const L = layout(p);
    svg.setAttribute("width", L.w); svg.setAttribute("height", L.h);
    svg.setAttribute("viewBox", `0 0 ${L.w} ${L.h}`);
    let out = "";
    // edges
    const fanout = {};
    for (const l of p.topology.links) {
      const a = L.pos[l.src], b = L.pos[l.sink];
      if (!a || !b) continue;
      const k = l.src;
      fanout[k] = (fanout[k] || 0) + 1;
      const st = p.pads[l.src + ":" + l.src_pad] || {};
      const x1 = a.x + NODE_W, y1 = a.y + NODE_H / 2, x2 = b.x, y2 = b.y + NODE_H / 2;
      const dx = Math.max(30, (x2 - x1) / 2);
      const cls = st.flow_errors ? "edge err" : (st.fps || 0) === 0 ? "edge idle" : "edge";
      out += `<path class="${cls}" d="M${x1},${y1} C${x1 + dx},${y1} ${x2 - dx},${y2} ${x2},${y2}"><title>${esc(l.caps || "no caps")}</title></path>`;
      out += `<path d="M${x2 - 6},${y2 - 4} L${x2},${y2} L${x2 - 6},${y2 + 4}" class="${cls}"/>`;
      if (st.fps != null) {
        const lx = (x1 + x2) / 2, ly = (y1 + y2) / 2 - 6;
        out += `<text class="elabel" x="${lx}" y="${ly}" text-anchor="middle">${fmtFps(st.fps)} fps</text>`;
        out += `<text class="elabel" x="${lx}" y="${ly + 12}" text-anchor="middle">${fmtRate(st.kbps)}</text>`;
      }
    }
    // nodes
    for (const e of p.topology.elements) {
      const q = L.pos[e.id]; if (!q) continue;
      const es = p.elstats[e.id] || {};
      const sev = severity(p, e.id);
      const sel = S.selEl === e.id ? " sel" : "";
      let line3 = "", barV = null, barLbl = "";
      if (es.queue) { line3 = `queue ${es.queue.buffers}/${es.queue.max_buffers} · ${es.queue.time_ms.toFixed(0)} ms`; barV = es.queue.fill; barLbl = "fill"; }
      else if (es.proc_avg_us != null) { line3 = `${fmtUs(es.proc_avg_us)} avg · ${fmtUs(es.proc_max_us)} max`; barV = es.util; barLbl = "busy"; }
      else if (es.sink_avg_us != null) { line3 = `push ${fmtUs(es.sink_avg_us)} avg`; }
      else if (e.source) { line3 = "source"; }
      const kind = e.source ? "SRC" : e.sink ? "SINK" : /Decoder/.test(e.klass) ? "DEC" : /Encoder/.test(e.klass) ? "ENC" : /Demux/.test(e.klass) ? "DEMUX" : /Parser/.test(e.klass) ? "PARSE" : "";
      const pp = parentPath(e.id);
      out += `<g class="node ${sev}${sel}" data-id="${esc(e.id)}" transform="translate(${q.x},${q.y})">
        <title>${esc(e.id)}\n${esc(e.factory)}  ${esc(e.klass)}\nstate ${esc(e.state)}</title>
        <rect class="box" width="${NODE_W}" height="${NODE_H}"/>
        <text class="t1" x="10" y="19">${esc(short(e.id).slice(0, 24))}</text>
        ${kind ? `<text class="t2 kind" x="${NODE_W - 10}" y="18" text-anchor="end">${kind}</text>` : ""}
        <text class="t2" x="10" y="34">${esc((e.factory + (pp ? "  ⊂ " + pp : "")).slice(0, 30))}</text>
        <text class="t3" x="10" y="52">${esc(line3)}</text>
        ${barV != null ? `<rect class="bar-bg" x="10" y="61" width="${NODE_W - 84}" height="6" rx="3"/>
          <rect class="bar" x="10" y="61" width="${Math.max(2, Math.min(1, barV) * (NODE_W - 84))}" height="6" rx="3"/>
          <text class="t2" x="${NODE_W - 10}" y="67" text-anchor="end">${barLbl} ${pct(barV)}</text>` : ""}
      </g>`;
    }
    svg.innerHTML = out;
    svg.querySelectorAll(".node").forEach((n) => n.addEventListener("click", () => selectElement(n.dataset.id)));
  }

  function selectElement(id) {
    S.selEl = id;
    renderPipeline();
    const n = [...document.querySelectorAll("#graph .node")].find((x) => x.dataset.id === id);
    const wrap = $(".graphwrap");
    if (n && wrap) {
      const r = n.getBoundingClientRect(), w = wrap.getBoundingClientRect();
      if (r.left < w.left || r.right > w.right) wrap.scrollLeft += r.left - w.left - (w.width - r.width) / 2;
      if (r.top < w.top || r.bottom > w.bottom) wrap.scrollTop += r.top - w.top - (w.height - r.height) / 2;
    }
  }

  function renderElement(p) {
    const body = $("#el-body");
    const title = $("#el-panel .panel-title");
    const e = p.topology.elements.find((x) => x.id === S.selEl);
    if (!e) {
      title.innerHTML = 'Element <span class="muted">click a node</span>';
      body.className = "muted small";
      body.textContent = "Select an element in the graph to see its history.";
      return;
    }
    body.className = "";
    title.innerHTML = `${esc(e.id)} <span class="muted">${esc(e.factory)} · ${esc(e.klass)}</span>`;
    const es = p.elstats[e.id] || {};
    const outs = Object.values(p.pads).filter((x) => x.el === e.id);
    const H = S.phist[p.id] || {};
    const items = [
      ["state", e.state],
      ["out fps", outs.length ? fmtFps(outs.reduce((a, o) => a + (o.fps || 0), 0)) : "–"],
      ["out bitrate", outs.length ? fmtRate(outs.reduce((a, o) => a + (o.kbps || 0), 0)) : "–"],
      ["proc avg", fmtUs(es.proc_avg_us)], ["proc max", fmtUs(es.proc_max_us)],
      ["busy", pct(es.util)],
    ];
    if (es.sink_avg_us != null) items.push(["push avg", fmtUs(es.sink_avg_us)], ["push max", fmtUs(es.sink_max_us)]);
    if (es.queue) items.push(["queue", `${es.queue.buffers}/${es.queue.max_buffers}`], ["queue time", es.queue.time_ms.toFixed(0) + " ms"]);
    if (es.qos_dropped != null) items.push(["QoS dropped", es.qos_dropped]);
    if (es.pending) items.push(["in flight", es.pending]);
    const fe = outs.reduce((a, o) => a + (o.flow_errors || 0), 0);
    if (fe) items.push(["flow errors", fe + " (" + (outs.find((o) => o.last_flow) || {}).last_flow + ")"]);

    const fpsSeries = outs.map((o, i) => ({ data: H["fps:" + o.el + ":" + o.pad] || [], color: ["#41d6a6", "#5aa9ff", "#c792ea", "#f0b429"][i % 4] }));
    let html = `<div class="elgrid">${items.map(([k, v]) => `<div><label>${k}</label><b>${esc(v)}</b></div>`).join("")}</div>`;
    const charts = [];
    if (H["proc:" + e.id]) charts.push(["proc time (avg / max)", "c-proc"]);
    if (H["sink:" + e.id]) charts.push(["push time into sink (includes clock sync wait)", "c-sink"]);
    if (H["queue:" + e.id]) charts.push(["queue fill", "c-queue"]);
    if (outs.length) charts.push(["output fps", "c-fps"]);
    html += charts.map(([t, id]) => `<div class="chart"><div class="ct"><span>${t}</span></div><canvas id="${id}"></canvas></div>`).join("");
    const caps = p.topology.links.filter((l) => l.src === e.id).map((l) => `${esc(l.src_pad)} → ${esc(l.sink)}: ${esc(l.caps || "(no caps)")}`);
    if (caps.length) html += `<div class="caps">${caps.join("<br>")}</div>`;
    body.innerHTML = html;
    if ($("#c-proc")) chart($("#c-proc"), [{ data: H["procmax:" + e.id] || [], color: "#5c6775" }, { data: H["proc:" + e.id], color: "#41d6a6", fill: "rgba(65,214,166,.08)" }], { fmt: fmtUs });
    if ($("#c-sink")) chart($("#c-sink"), [{ data: H["sink:" + e.id], color: "#5aa9ff" }], { fmt: fmtUs });
    if ($("#c-queue")) chart($("#c-queue"), [{ data: H["queue:" + e.id], color: "#f0b429", fill: "rgba(240,180,41,.08)" }], { max: 1, fmt: pct });
    if ($("#c-fps")) chart($("#c-fps"), fpsSeries, { fmt: (v) => v.toFixed(1) });
  }

  function renderPipeline() {
    const snap = S.snap;
    const p = snap && snap.pipelines.find((x) => x.id === S.sel);
    $("#pempty").hidden = !!p;
    $("#pdetail").hidden = !p;
    if (!p) return;
    $("#p-name").textContent = p.name;
    $("#p-meta").textContent = `${p.proc} · pid ${p.pid} · ${p.gst} · tracer ${p.tracer}`;
    const totalErr = Object.values(p.pads).reduce((a, x) => a + (x.flow_errors || 0), 0);
    $("#p-pills").innerHTML = [
      ["state", p.state], ["", p.status], ["elements", p.elements],
      ["source fps", fmtFps(p.source_fps)], ["errors", p.errors + totalErr], ["warnings", p.warnings],
    ].map(([k, v]) => `<span class="pill">${k ? k + " " : ""}<b>${esc(v)}</b></span>`).join("");
    $("#p-hints").innerHTML = p.hints.map((h) =>
      `<div class="hint ${h.level}" ${h.element ? `data-el="${esc(h.element)}"` : ""}><span class="dot"></span><span>${esc(h.text)}</span></div>`).join("");
    $("#p-hints").querySelectorAll(".hint[data-el]").forEach((h) => h.addEventListener("click", () => selectElement(h.dataset.el)));
    drawGraph(p);
    renderElement(p);
    const evs = p.events.slice().reverse();
    $("#ev-body").innerHTML = evs.length ? evs.map((e) =>
      `<div class="ev"><span class="muted">${clock(e.time)}</span><span class="lv-${esc(e.level)}">${esc(e.level)}</span><span class="msg">${esc(e.src)}: ${esc(e.msg)}${e.debug ? `<br><span class="muted">${esc(e.debug).slice(0, 300)}</span>` : ""}</span></div>`).join("")
      : '<div class="muted small">No events.</div>';
  }

  function renderPipelineList() {
    const list = $("#plist");
    const ps = (S.snap ? S.snap.pipelines : []).slice().sort((a, b) => (a.status > b.status) - (a.status < b.status) || a.name.localeCompare(b.name));
    if (!S.sel && ps.length) selectPipeline(ps[0].id);
    if (S.sel && !ps.find((p) => p.id === S.sel)) { S.sel = ps.length ? ps[0].id : null; if (S.sel) selectPipeline(S.sel); }
    list.innerHTML = ps.map((p) => `<div class="pitem ${p.id === S.sel ? "sel" : ""} st-${p.status}" data-id="${esc(p.id)}">
      <div class="n"><span>${esc(p.name)}</span><span class="small ${p.hint_count ? "err" : "muted"}">${p.hint_count ? "⚠ " + p.hint_count : ""}</span></div>
      <div class="s">${esc(p.proc)} · ${p.pid} · ${esc(p.state)}${p.status === "stale" ? " · stale" : ""}</div>
      <div class="s">${fmtFps(p.source_fps)} fps · ${p.elements} elements</div></div>`).join("")
      || '<div class="pitem muted small">waiting for tracers…</div>';
    list.querySelectorAll(".pitem[data-id]").forEach((n) => n.addEventListener("click", () => selectPipeline(n.dataset.id)));
  }

  async function selectPipeline(id) {
    if (S.sel !== id) S.selEl = null;
    S.sel = id;
    try {
      const d = await api("/pipelines/" + encodeURIComponent(id));
      S.phist[id] = d.history || {};
      S.plast[id] = d.last_seen;
    } catch (e) { /* pipeline may be gone */ }
    render();
  }

  function appendPipelineHistory(snap) {
    for (const p of snap.pipelines) {
      if (!S.phist[p.id] || S.plast[p.id] === p.last_seen) continue;
      S.plast[p.id] = p.last_seen;
      const H = S.phist[p.id], t = p.last_seen;
      const add = (k, v) => { if (v == null) return; (H[k] = H[k] || []); push(H[k], [t, v]); };
      for (const [k, x] of Object.entries(p.pads)) { add("fps:" + k, x.fps); add("kbps:" + k, x.kbps); }
      for (const [id, e] of Object.entries(p.elstats)) {
        add("proc:" + id, e.proc_avg_us); add("procmax:" + id, e.proc_max_us); add("sink:" + id, e.sink_avg_us);
        if (e.queue) add("queue:" + id, e.queue.fill);
      }
    }
  }

  // ---------------------------------------------------------------- probes
  const cards = {};
  function renderProbes() {
    const probes = S.snap ? S.snap.probes : [];
    $("#prempty").hidden = probes.length > 0;
    const box = $("#probes");
    const seen = new Set();
    for (const p of probes) {
      seen.add(p.id);
      let c = cards[p.id];
      if (!c) {
        c = cards[p.id] = $("#tpl-card").content.firstElementChild.cloneNode(true);
        c.querySelector(".a-del").addEventListener("click", async () => {
          if (!confirm("Remove probe " + p.name + "?")) return;
          await api("/probes/" + p.id, { method: "DELETE" }).catch(() => {});
        });
        c.querySelector(".a-restart").addEventListener("click", () => api("/probes/" + p.id + "/restart", { method: "POST", body: "{}" }).catch(() => {}));
        box.append(c);
        if (!S.prhist[p.id]) {
          S.prhist[p.id] = { fps: [], kbps: [] };
          api("/probes/" + p.id).then((d) => {
            if (d.video && d.video.history) S.prhist[p.id] = { fps: d.video.history.fps.slice(), kbps: d.video.history.kbps.slice() };
          }).catch(() => {});
        }
      }
      const v = p.video;
      const H = S.prhist[p.id];
      if (v && S.snap) { push(H.fps, [S.snap.time, v.fps]); push(H.kbps, [S.snap.time, v.kbps]); }
      c.className = "card st-" + p.state;
      c.querySelector(".card-name").textContent = p.name;
      c.querySelector(".card-url").textContent = p.url + "  (" + p.transport + ")";
      const b = c.querySelector(".badge"); b.className = "badge " + p.state; b.textContent = p.state;
      c.querySelector(".card-err").textContent = p.error || "";
      const audio = p.streams.filter((s) => s.media === "audio").map((s) => s.codec).join(", ");
      c.querySelector(".card-codec").innerHTML = v
        ? `<b>${esc(v.codec)}</b> ${v.width ? v.width + "×" + v.height : ""} ${v.profile ? '<span class="muted">' + esc(v.profile) + "</span>" : ""}${audio ? ' <span class="muted">· audio ' + esc(audio) + "</span>" : ""}`
        : '<span class="muted">no video stream yet</span>';
      const set = (cls, txt, bad) => { const n = c.querySelector(cls); n.textContent = txt; n.classList.toggle("bad", !!bad); };
      set(".v-fps", v ? fmtFps(v.fps) + (v.nominal_fps ? " / " + v.nominal_fps : "") : "–", v && v.nominal_fps && v.fps < v.nominal_fps * 0.9);
      set(".v-kbps", v ? fmtRate(v.kbps) : "–");
      set(".v-gop", v && v.gop_frames != null ? `${v.gop_frames} · ${v.gop_sec}s` : "–", v && v.gop_sec > 4);
      set(".v-jit", v ? v.jitter_ms.toFixed(1) + " ms" : "–", v && v.jitter_ms > 20);
      set(".v-gap", v ? v.max_gap_ms.toFixed(0) + " ms" : "–", v && v.max_gap_ms > 500);
      set(".v-rec", String(p.reconnects), p.reconnects > 0);
      c.querySelector(".v-extra").textContent = (p.connect_ms != null ? "first frame " + p.connect_ms.toFixed(0) + " ms" : "") + (p.uptime ? " · up " + fmtDur(p.uptime) : "");
      const kmax = Math.max(1, ...H.kbps.map((x) => x[1]));
      const fmax = Math.max(1, ...H.fps.map((x) => x[1]));
      chart(c.querySelector(".spark"), [
        { data: H.kbps.map((x) => [x[0], x[1] / kmax * fmax]), color: "#3d4a5a", fill: "rgba(90,169,255,.07)" },
        { data: H.fps, color: "#41d6a6" },
      ], { fmt: (x) => x.toFixed(0) + " fps", span: 120 });
    }
    for (const id of Object.keys(cards)) if (!seen.has(id)) { cards[id].remove(); delete cards[id]; delete S.prhist[id]; }
  }
  const fmtDur = (s) => s < 120 ? Math.round(s) + "s" : s < 7200 ? Math.round(s / 60) + "m" : (s / 3600).toFixed(1) + "h";

  $("#addprobe").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    $("#pr-err").textContent = "";
    try {
      await api("/probes", { method: "POST", body: JSON.stringify({ url: $("#pr-url").value.trim(), name: $("#pr-name").value.trim(), transport: $("#pr-tr").value }) });
      $("#pr-url").value = ""; $("#pr-name").value = "";
    } catch (e) { $("#pr-err").textContent = e.message; }
  });

  // ---------------------------------------------------------------- main
  function render() {
    const snap = S.snap;
    if (snap) {
      const st = snap.status;
      $("#host").textContent = st.hostname + (st.board ? " · " + st.board : "") + " · v" + st.version;
      $("#cnt-pipelines").textContent = snap.pipelines.length;
      $("#cnt-probes").textContent = snap.probes.length;
    }
    if (S.tab === "pipelines") { renderPipelineList(); renderPipeline(); }
    else renderProbes();
  }

  function connect() {
    const es = new EventSource(url("/stream"));
    es.addEventListener("snapshot", (m) => {
      S.snap = JSON.parse(m.data);
      $("#live").classList.remove("off");
      appendPipelineHistory(S.snap);
      if (S.tab !== "probes") {
        // keep probe sparklines fed even when the tab is hidden
        for (const p of S.snap.probes) {
          const H = S.prhist[p.id]; if (H && p.video) { push(H.fps, [S.snap.time, p.video.fps]); push(H.kbps, [S.snap.time, p.video.kbps]); }
        }
      }
      render();
    });
    es.onerror = () => $("#live").classList.add("off");
  }
  window.addEventListener("resize", () => render());
  connect();
})();
