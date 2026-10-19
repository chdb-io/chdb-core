// Renders the nightly dashboard. Its data comes from the nightly API (config.json
// names it), a chDB Lambda function that queries the nightly store in S3, and from
// data/*.json, the copy report.py computes with chDB in CI, when the API cannot be
// reached. No dependencies. Every string from the data goes in through textContent,
// never innerHTML: test names and titles are untrusted.

const PLATFORMS = [
  { id: "linux-x86_64", label: "Linux x86_64", slot: 1 },
  { id: "linux-aarch64", label: "Linux arm64", slot: 2 },
  { id: "macos-arm64", label: "macOS arm64", slot: 3 },
  { id: "macos-x86_64", label: "macOS x86_64", slot: 4 },
];
const STATUS = {
  success: { cls: "good", icon: "✓", label: "Passed" },
  failure: { cls: "critical", icon: "✕", label: "Failed" },
  cancelled: { cls: "warning", icon: "!", label: "Cancelled" },
  timed_out: { cls: "critical", icon: "✕", label: "Timed out" },
};
const NOT_RUN = { cls: "none", icon: "–", label: "Not run" };
const API_TIMEOUT_MS = 10000;
const SVG = "http://www.w3.org/2000/svg";

const state = { days: 30, latest: null, history: [], source: "" };

function el(tag, attrs = {}, ...children) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attrs)) {
    if (value === undefined || value === null || value === false) continue;
    if (key === "class") node.className = value;
    else node.setAttribute(key, value);
  }
  for (const child of children.flat()) {
    if (child === null || child === undefined || child === false) continue;
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

function svg(tag, attrs = {}) {
  const node = document.createElementNS(SVG, tag);
  for (const [key, value] of Object.entries(attrs)) node.setAttribute(key, value);
  return node;
}

function fmt(value, digits = 0) {
  return Number(value).toLocaleString("en-US", { maximumFractionDigits: digits, minimumFractionDigits: digits });
}

function statusBadge(conclusion, url) {
  const s = STATUS[conclusion] || NOT_RUN;
  const inner = [el("span", { class: "dot", "aria-hidden": "true" }, s.icon), el("span", { class: "label" }, s.label)];
  return url ? el("a", { class: `status ${s.cls}`, href: url }, inner) : el("span", { class: `status ${s.cls}` }, inner);
}

function inRange(rows) {
  if (!rows.length) return rows;
  const newest = rows.map((r) => r.date).sort().at(-1);
  const cutoff = new Date(`${newest}T00:00:00Z`);
  cutoff.setUTCDate(cutoff.getUTCDate() - (state.days - 1));
  const from = cutoff.toISOString().slice(0, 10);
  return rows.filter((r) => r.date >= from);
}

// ------------------------------------------------------------------- summary tiles

function tile(label, value, ...sub) {
  return el("div", { class: "tile" }, el("div", { class: "label" }, label), el("div", { class: "value" }, value),
    el("div", { class: "sub" }, ...sub));
}

function renderTiles() {
  const root = document.getElementById("tiles");
  const latest = state.latest;
  if (!latest) {
    root.replaceChildren(el("p", { class: "empty" }, "No nightly has been published yet."));
    return;
  }
  const night = latest.night;
  const passed = latest.platforms.filter((p) => p.conclusion === "success");
  const failedNames = latest.platforms.filter((p) => p.conclusion !== "success")
    .map((p) => (PLATFORMS.find((x) => x.id === p.platform) || { label: p.platform }).label);
  const sync = latest.sync || {};
  const synced = sync.upstream_commit
    ? el("a", { href: sync.upstream_url }, el("code", {}, sync.upstream_commit.slice(0, 12)))
    : "Unknown";
  const syncSub = sync.last_run && sync.last_run.conclusion === "failure"
    ? [el("a", { href: sync.last_run.url }, "The last sync run failed.")]
    : ["The commit this nightly is synced to"];
  const failing = latest.failing_tests || [];
  root.replaceChildren(
    tile("Newest nightly", night.version || "–",
      `${night.date} · `, el("a", { href: night.commit_url }, night.head_sha.slice(0, 12))),
    tile("Platforms passing", `${passed.length} of ${latest.platforms.length}`,
      failedNames.length ? `Not passing: ${failedNames.join(", ")}` : "All platforms passed"),
    tile("ClickHouse master", synced, ...syncSub),
    tile("Failing tests", fmt(failing.length), failing.length ? "In the newest night" : "None in the newest night"),
  );
}

// --------------------------------------------------------------------- status table

function renderStatus() {
  const root = document.getElementById("status");
  const rows = inRange(state.history);
  if (!rows.length) {
    root.replaceChildren(el("p", { class: "empty" }, "No nights in this range."));
    return;
  }
  const dates = [...new Set(rows.map((r) => r.date))].sort().reverse();
  const head = el("tr", {}, el("th", {}, "Night"), el("th", {}, "Version"),
    PLATFORMS.map((p) => el("th", {}, p.label)), el("th", {}, "Failed tests"));
  const body = dates.map((date) => {
    const night = rows.filter((r) => r.date === date);
    const failed = night.reduce((sum, r) => sum + (r.tests_failed || 0), 0);
    return el("tr", {},
      el("td", {}, date),
      el("td", {}, el("code", {}, night[0].version || "–")),
      PLATFORMS.map((p) => {
        const r = night.find((x) => x.platform === p.id);
        return el("td", {}, statusBadge(r ? r.conclusion : "", r ? r.url : ""));
      }),
      el("td", {}, fmt(failed)));
  });
  root.replaceChildren(el("table", {}, el("thead", {}, head), el("tbody", {}, body)));
}

// ----------------------------------------------------------------------- line chart

function niceStep(max, count) {
  const raw = max / count;
  const magnitude = 10 ** Math.floor(Math.log10(raw || 1));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * magnitude).find((s) => s >= raw) || raw;
  return step;
}

function lineChart(root, { field, unit, digits }) {
  root.replaceChildren();
  const rows = inRange(state.history).filter((r) => r[field] > 0);
  if (!rows.length) {
    root.append(el("p", { class: "empty" }, "No data in this range."));
    return;
  }
  const dates = [...new Set(rows.map((r) => r.date))].sort();
  const series = PLATFORMS.map((p) => ({
    ...p,
    values: dates.map((d) => {
      const r = rows.find((x) => x.date === d && x.platform === p.id);
      return r ? r[field] : null;
    }),
  })).filter((s) => s.values.some((v) => v !== null));

  root.append(el("div", { class: "legend" }, series.map((s) =>
    el("span", { class: "key" }, el("span", { class: `line-key s${s.slot}` }), s.label))));

  const width = Math.max(root.clientWidth, 320);
  const height = 240;
  const m = { l: 44, r: 116, t: 12, b: 26 };
  const innerW = width - m.l - m.r;
  const innerH = height - m.t - m.b;
  const max = Math.max(...series.flatMap((s) => s.values.filter((v) => v !== null)));
  const step = niceStep(max, 4);
  const top = Math.ceil(max / step) * step;
  const x = (i) => m.l + (dates.length === 1 ? innerW / 2 : (i * innerW) / (dates.length - 1));
  const y = (v) => m.t + innerH - (v / top) * innerH;

  const chart = svg("svg", { viewBox: `0 0 ${width} ${height}`, role: "img",
    "aria-label": `${unit} per platform, ${dates[0]} to ${dates.at(-1)}` });
  for (let v = 0; v <= top + 1e-9; v += step) {
    chart.append(svg("line", { x1: m.l, x2: m.l + innerW, y1: y(v), y2: y(v),
      stroke: v === 0 ? "var(--baseline)" : "var(--grid)", "stroke-width": 1 }));
    const label = svg("text", { x: m.l - 6, y: y(v) + 4, "text-anchor": "end" });
    label.textContent = fmt(v, step < 1 ? 1 : 0);
    chart.append(label);
  }
  const ticks = dates.length <= 3 ? dates.map((_, i) => i) : [0, Math.floor((dates.length - 1) / 2), dates.length - 1];
  for (const i of ticks) {
    const label = svg("text", { x: x(i), y: height - 6, "text-anchor": "middle" });
    label.textContent = dates[i].slice(5);
    chart.append(label);
  }

  const ends = [];
  for (const s of series) {
    let d = "";
    s.values.forEach((v, i) => {
      if (v === null) return;
      const prev = i > 0 ? s.values[i - 1] : null;
      d += `${prev === null ? "M" : "L"}${x(i).toFixed(1)},${y(v).toFixed(1)}`;
    });
    chart.append(svg("path", { d, fill: "none", stroke: `var(--series-${s.slot})`, "stroke-width": 2,
      "stroke-linejoin": "round", "stroke-linecap": "round" }));
    const last = s.values.map((v, i) => [v, i]).filter(([v]) => v !== null).at(-1);
    chart.append(svg("circle", { cx: x(last[1]), cy: y(last[0]), r: 4, fill: `var(--series-${s.slot})`,
      stroke: "var(--surface-1)", "stroke-width": 2 }));
    ends.push({ s, v: last[0], i: last[1], y: y(last[0]) });
  }
  // End labels only when they do not collide; otherwise the legend and tooltip carry identity.
  const sorted = [...ends].sort((a, b) => a.y - b.y);
  const collide = sorted.some((e, k) => k > 0 && e.y - sorted[k - 1].y < 14);
  if (!collide) {
    for (const e of ends) {
      const label = svg("text", { x: x(e.i) + 8, y: e.y + 4, class: "end-label" });
      label.textContent = `${e.s.label} ${fmt(e.v, digits)}`;
      chart.append(label);
    }
  }

  const cross = svg("line", { y1: m.t, y2: m.t + innerH, stroke: "var(--baseline)", "stroke-width": 1, visibility: "hidden" });
  chart.append(cross);
  const hit = svg("rect", { x: m.l - 8, y: m.t, width: innerW + 16, height: innerH, fill: "transparent" });
  chart.append(hit);
  root.append(chart);

  const tip = el("div", { class: "tooltip", role: "status" });
  root.append(tip);
  root.tabIndex = 0;
  let current = -1;
  const show = (i) => {
    current = Math.max(0, Math.min(dates.length - 1, i));
    cross.setAttribute("x1", x(current));
    cross.setAttribute("x2", x(current));
    cross.setAttribute("visibility", "visible");
    tip.replaceChildren(el("div", { class: "date" }, dates[current]),
      ...series.map((s) => el("div", { class: "row" }, el("span", { class: `line-key s${s.slot}` }),
        el("strong", {}, s.values[current] === null ? "–" : `${fmt(s.values[current], digits)} ${unit}`),
        el("span", {}, s.label))));
    tip.style.display = "block";
    const px = (x(current) / width) * root.clientWidth;
    const left = px + 12 + tip.offsetWidth > root.clientWidth ? px - tip.offsetWidth - 12 : px + 12;
    tip.style.left = `${Math.max(0, left)}px`;
    tip.style.top = "36px";
  };
  const hide = () => {
    cross.setAttribute("visibility", "hidden");
    tip.style.display = "none";
  };
  hit.addEventListener("pointermove", (ev) => {
    const box = chart.getBoundingClientRect();
    const sx = ((ev.clientX - box.left) / box.width) * width;
    const i = dates.length === 1 ? 0 : Math.round(((sx - m.l) / innerW) * (dates.length - 1));
    show(i);
  });
  hit.addEventListener("pointerleave", hide);
  root.onkeydown = (ev) => {
    if (ev.key === "ArrowRight") show(current < 0 ? dates.length - 1 : current + 1);
    else if (ev.key === "ArrowLeft") show(current < 0 ? dates.length - 1 : current - 1);
    else if (ev.key === "Escape") hide();
    else return;
    ev.preventDefault();
  };
  root.onblur = hide;

  const table = el("table", {}, el("thead", {}, el("tr", {}, el("th", {}, "Night"), series.map((s) => el("th", {}, s.label)))),
    el("tbody", {}, dates.map((d, i) => el("tr", {}, el("td", {}, d),
      series.map((s) => el("td", {}, s.values[i] === null ? "–" : fmt(s.values[i], digits)))))));
  root.append(el("details", { class: "table-view" }, el("summary", {}, "Show as table"), table));
}

// --------------------------------------------------------------------- test tables

function testTable(rows, columns) {
  return el("table", {}, el("thead", {}, el("tr", {}, columns.map((c) => el("th", {}, c.title)))),
    el("tbody", {}, rows.map((r) => el("tr", {}, columns.map((c) => el("td", { class: c.cls }, c.value(r)))))));
}

function platformLabel(id) {
  return (PLATFORMS.find((p) => p.id === id) || { label: id }).label;
}

function renderTests() {
  const failing = (state.latest && state.latest.failing_tests) || [];
  document.getElementById("failing").replaceChildren(failing.length
    ? testTable(failing, [
      { title: "Test", cls: "test", value: (r) => r.test },
      { title: "Platform", value: (r) => platformLabel(r.platform) },
    ])
    : el("p", { class: "empty" }, "No failing tests."));
}

function link(href, text) {
  return href ? el("a", { href }, text) : text;
}

function renderInstall() {
  const latest = state.latest;
  const index = (latest && latest.install.index_url) || `${location.origin}/simple`;
  const root = document.getElementById("install");
  root.replaceChildren(
    el("p", { class: "caption" }, "Newest nightly (pre-release; not for production):"),
    el("pre", { class: "install" }, el("code", {},
      `pip install --pre --extra-index-url ${index} chdb-core\npip install --pre --extra-index-url ${index} chdb`)),
    el("p", { class: "caption" }, "chdb is chdb-io/chdb's main, pinned to the chdb-core of its night. ",
      "Pin one night with chdb-core==<version> or chdb==<version>. The newest 20 nights are kept on the ",
      el("a", { href: latest ? latest.install.release_url : "#" }, "nightly release"), "."),
  );
  if (!latest) return;
  const chdb = latest.chdb || {};
  if (chdb.status === "published") {
    root.append(el("p", { class: "caption" }, "chdb nightly of this night: ", el("code", {}, chdb.version), " from ",
      link(chdb.commit_url, el("code", {}, (chdb.commit || "").slice(0, 12))), ".",
      chdb.log ? [" ", el("a", { href: chdb.log }, "Build log"), "."] : ""));
  } else if (chdb.status) {
    const why = chdb.status === "failed" ? "its build failed" : "the night has no chdb-core wheel";
    root.append(el("p", { class: "caption" }, `No chdb nightly for this night: ${why}; the previous one stays the newest. `,
      chdb.log ? el("a", { href: chdb.log }, "Build log") : ""));
  }
  const logs = latest.platforms.filter((p) => p.log || p.error_log);
  if (logs.length) {
    root.append(el("p", { class: "caption" }, "Logs of the newest night: ",
      logs.map((p, k) => [k ? "; " : "", link(p.log, platformLabel(p.platform)),
        p.error_log ? [" (", el("a", { href: p.error_log }, "up to the first error"), ")"] : ""])));
  }
}

function renderFooter() {
  const latest = state.latest;
  const footer = document.getElementById("footer");
  if (!latest) return;
  const engine = latest.engine || {};
  footer.replaceChildren(`Data computed with chDB ${engine.version || ""}, ${state.source}. Published ${latest.generated_at}.`);
  document.getElementById("updated").textContent = `Updated ${latest.generated_at.replace("T", " ").replace("Z", " UTC")}.`;
}

function render() {
  renderTiles();
  renderStatus();
  lineChart(document.getElementById("duration-chart"), { field: "duration_min", unit: "min", digits: 0 });
  lineChart(document.getElementById("size-chart"), { field: "wheel_mb", unit: "MB", digits: 1 });
  renderTests();
  renderInstall();
  renderFooter();
}

// ------------------------------------------------------------------ theme & filters

const THEMES = ["system", "light", "dark"];
function applyTheme(theme) {
  if (theme === "system") delete document.documentElement.dataset.theme;
  else document.documentElement.dataset.theme = theme;
  document.getElementById("theme").textContent = `Theme: ${theme}`;
}

async function main() {
  let theme = localStorage.getItem("theme") || "system";
  applyTheme(theme);
  document.getElementById("theme").addEventListener("click", () => {
    theme = THEMES[(THEMES.indexOf(theme) + 1) % THEMES.length];
    localStorage.setItem("theme", theme);
    applyTheme(theme);
  });
  for (const button of document.querySelectorAll(".filters button")) {
    button.addEventListener("click", () => {
      state.days = Number(button.dataset.days);
      for (const b of document.querySelectorAll(".filters button")) b.setAttribute("aria-pressed", String(b === button));
      render();
    });
  }
  const get = (url, ms) => fetch(url, { cache: "no-store", signal: ms ? AbortSignal.timeout(ms) : undefined })
    .then((r) => (r.ok ? r.json() : null)).catch(() => null);
  const config = (await get("config.json")) || {};
  // An API instance that starts cold takes seconds to load chDB, and after a deploy far
  // longer; past API_TIMEOUT_MS the page shows the copy from publication instead.
  let [latest, history] = config.api
    ? await Promise.all(["latest", "history"].map((name) => get(`${config.api}/${name}`, API_TIMEOUT_MS)))
    : [];
  state.source = "live from the nightly API";
  if (!latest || !history) {
    [latest, history] = await Promise.all(["latest", "history"].map((name) => get(`data/${name}.json`)));
    state.source = "as of its publication";
  }
  state.latest = latest;
  state.history = (history && history.rows) || [];
  render();
  let timer;
  window.addEventListener("resize", () => {
    clearTimeout(timer);
    timer = setTimeout(render, 150);
  });
}

main();
