// Job scheduler dashboard. Plain JavaScript with no dependencies: it polls
// the API and redraws. Everything that came from the API is put on the page
// with textContent, never as HTML, because job types, errors, cron names and
// worker IDs are all chosen by whoever submitted them.
"use strict";

const STATS_EVERY = 1000; // ms
const EVENTS_EVERY = 1000;
const TABLES_EVERY = 2000; // dead-letter queue and crons
const MAX_EVENTS = 200; // rows kept in the events table
const RATE_WINDOW = 10; // seconds of samples behind each rate
const SPARK_POINTS = 60;
const TOKEN_KEY = "jobScheduler.apiToken";

const $ = (id) => document.getElementById(id);

// ---- API ----------------------------------------------------------------

function token() {
  try {
    return sessionStorage.getItem(TOKEN_KEY) || "";
  } catch {
    return "";
  }
}

class APIError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

async function api(method, path, body) {
  const headers = {};
  const t = token();
  if (t) headers.Authorization = "Bearer " + t;
  const init = { method, headers };
  if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    init.body = JSON.stringify(body);
  }
  const resp = await fetch(path, init);
  if (resp.status === 204) return null;
  let data = null;
  try {
    data = await resp.json();
  } catch {
    // not JSON; fall through with data = null
  }
  if (!resp.ok) {
    if (resp.status === 401) showTokenForm();
    throw new APIError(resp.status, (data && data.error) || resp.statusText);
  }
  return data;
}

// ---- DOM helpers ----------------------------------------------------------

// el builds an element. Children that are strings become text nodes, so
// they can never be read as HTML.
function el(tag, props, ...children) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) {
    if (k === "class") e.className = v;
    else if (k === "text") e.textContent = v;
    else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v);
  }
  for (const c of children) {
    if (c === null || c === undefined) continue;
    e.append(c instanceof Node ? c : String(c));
  }
  return e;
}

function td(text, cls) {
  return el("td", cls ? { class: cls, text } : { text });
}

function setText(id, text) {
  const e = $(id);
  if (e.textContent !== String(text)) e.textContent = text;
}

function shortID(id) {
  return id ? id.slice(0, 8) + "…" + id.slice(-4) : "–";
}

function jobLink(id) {
  if (!id) return "–";
  const a = el("a", {
    class: "job mono",
    href: "#",
    title: id,
    text: shortID(id),
  });
  a.addEventListener("click", (e) => {
    e.preventDefault();
    showJob(id);
  });
  return a;
}

const timeFmt = new Intl.DateTimeFormat(undefined, {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
});

function clock(iso) {
  const d = new Date(iso);
  return timeFmt.format(d) + "." + String(d.getMilliseconds()).padStart(3, "0");
}

function dateTime(iso) {
  return iso ? new Date(iso).toLocaleString() : "–";
}

// ago formats the time from then to now, both in ms.
function ago(ms) {
  const s = Math.max(0, ms) / 1000;
  if (s < 60) return s.toFixed(1) + " s ago";
  if (s < 3600) return Math.floor(s / 60) + " min ago";
  return Math.floor(s / 3600) + " h ago";
}

// until formats the time from now to then.
function until(iso, nowMs) {
  if (!iso) return "–";
  const s = (new Date(iso).getTime() - nowMs) / 1000;
  if (s <= 0) return "due";
  if (s < 90) return "in " + Math.ceil(s) + " s";
  if (s < 5400) return "in " + Math.round(s / 60) + " min";
  return dateTime(iso);
}

// serverOffset is Redis's clock minus this browser's, from the last
// snapshot, so "ago" and "in" read right even if the clocks disagree.
let serverOffset = 0;
const serverNow = () => Date.now() + serverOffset;

// ---- notices and the token form -----------------------------------------

let noticeTimer;
function notice(text, isError) {
  const n = $("notice");
  n.textContent = text;
  n.classList.toggle("error", !!isError);
  n.hidden = false;
  clearTimeout(noticeTimer);
  noticeTimer = setTimeout(() => (n.hidden = true), isError ? 8000 : 3000);
}

function showTokenForm() {
  $("token-form").hidden = false;
}

$("token-form").addEventListener("submit", (e) => {
  e.preventDefault();
  try {
    sessionStorage.setItem(TOKEN_KEY, $("token-input").value.trim());
  } catch {
    notice("Could not store the token in this tab.", true);
  }
  $("token-input").value = "";
  $("token-form").hidden = true;
  refreshAll();
});

// ---- connection status ----------------------------------------------------

let lastOK = 0;
let lastError = "";

function markOK() {
  lastOK = Date.now();
  lastError = "";
}

function markDown(err) {
  lastError = err instanceof APIError ? err.message : "API unreachable";
}

function drawConn() {
  const c = $("conn");
  if (lastError) {
    c.className = "conn down";
    c.textContent =
      lastError + (lastOK ? " · last update " + ago(Date.now() - lastOK) : "");
  } else if (lastOK) {
    c.className = "conn live";
    c.textContent = "live · updated " + ago(Date.now() - lastOK);
  }
}

// ---- stats: leader, queues, workers, counters, throughput ---------------

const samples = []; // {t, succeeded, submitted, failed}, one per stats poll

function counter(s, name) {
  return (s.counters && s.counters[name]) || 0;
}

function rate(name) {
  if (samples.length < 2) return null;
  const last = samples[samples.length - 1];
  let first = samples[0];
  for (const s of samples) {
    if (last.t - s.t <= RATE_WINDOW * 1000) {
      first = s;
      break;
    }
  }
  const dt = (last.t - first.t) / 1000;
  if (dt <= 0) return null;
  return Math.max(0, last[name] - first[name]) / dt;
}

function fmtRate(r) {
  if (r === null) return "–";
  return r >= 100 ? Math.round(r).toString() : r.toFixed(1);
}

function drawSpark() {
  const per = [];
  for (let i = 1; i < samples.length; i++) {
    const dt = (samples[i].t - samples[i - 1].t) / 1000;
    per.push(
      dt > 0
        ? Math.max(0, samples[i].succeeded - samples[i - 1].succeeded) / dt
        : 0,
    );
  }
  const pts = per.slice(-SPARK_POINTS);
  const max = Math.max(1, ...pts);
  const step = 120 / Math.max(1, SPARK_POINTS - 1);
  const offset = SPARK_POINTS - pts.length;
  $("spark-line").setAttribute(
    "points",
    pts
      .map(
        (v, i) =>
          ((offset + i) * step).toFixed(1) +
          "," +
          (23 - (v / max) * 22).toFixed(1),
      )
      .join(" "),
  );
}

const COUNTERS = [
  "submitted",
  "claimed",
  "succeeded",
  "retried",
  "dead",
  "reclaimed",
  "released",
  "fenced",
  "cancelled",
  "replayed",
  "purged",
  "cron_fired",
];

function drawStats(s) {
  const at = new Date(s.at).getTime();
  serverOffset = at - Date.now();

  setText(
    "leader",
    s.leader
      ? `leader: ${s.leader} · term ${s.epoch}`
      : `no leader · last term ${s.epoch}`,
  );
  for (const q of ["scheduled", "ready", "running", "dead"])
    setText("q-" + q, s.queues[q]);
  $("q-dead").classList.toggle("zero", s.queues.dead === 0);

  samples.push({
    t: at,
    succeeded: counter(s, "succeeded"),
    submitted: counter(s, "submitted"),
    failed: counter(s, "retried") + counter(s, "dead"),
  });
  while (samples.length > SPARK_POINTS + 1) samples.shift();
  setText("rate-succeeded", fmtRate(rate("succeeded")));
  setText("rate-submitted", fmtRate(rate("submitted")));
  setText("rate-failed", fmtRate(rate("failed")));
  drawSpark();

  const dl = $("counters");
  dl.replaceChildren(
    ...COUNTERS.map((name) =>
      el(
        "div",
        null,
        el("dt", { text: name }),
        el("dd", { text: counter(s, name) }),
      ),
    ),
  );

  const alive = s.workers.filter((w) => w.alive);
  const slots = alive.reduce((n, w) => n + w.concurrency, 0);
  const busy = alive.reduce((n, w) => n + w.busy, 0);
  setText("workers-sum", `${alive.length} alive · ${busy}/${slots} slots busy`);
  $("workers-empty").hidden = s.workers.length > 0;
  $("workers").replaceChildren(
    ...s.workers.map((w) => {
      const state = !w.alive ? "dead" : w.draining ? "draining" : "alive";
      const meter = el("meter", {
        min: "0",
        max: String(Math.max(1, w.concurrency)),
        value: String(w.busy),
      });
      return el(
        "tr",
        { class: state === "dead" ? "dead" : "" },
        td(w.id, "mono"),
        el(
          "td",
          null,
          el("span", {
            class: "badge " + state,
            text: state === "dead" ? "DEAD" : state,
          }),
        ),
        el("td", null, meter, `${w.busy}/${w.concurrency}`),
        td(ago(at - new Date(w.last_seen).getTime())),
        td(dateTime(w.started_at)),
      );
    }),
  );
}

async function pollStats() {
  try {
    drawStats(await api("GET", "/stats"));
    markOK();
  } catch (err) {
    markDown(err);
  }
  drawConn();
}

// ---- events -------------------------------------------------------------

let lastEventID = "";
let eventsLoaded = false;

function eventRow(e, isNew) {
  const change = (e.from || "new") + " → " + (e.to || "deleted");
  const row = el(
    "tr",
    { class: "ev-" + e.event + (isNew ? " ev-new" : "") },
    td(clock(e.at)),
    td(e.event),
    el("td", null, jobLink(e.job_id)),
    td(e.job_type),
    td(change),
    td(e.attempt),
    td(e.worker || ""),
    el("td", { class: "err", title: e.error || "", text: e.error || "" }),
  );
  return row;
}

async function pollEvents() {
  try {
    const path = eventsLoaded
      ? "/events?limit=1000&after=" + encodeURIComponent(lastEventID)
      : "/events?limit=50";
    const page = await api("GET", path);
    const body = $("events");
    for (const e of page.events) body.prepend(eventRow(e, eventsLoaded));
    while (body.rows.length > MAX_EVENTS) body.lastElementChild.remove();
    if (page.last_id) lastEventID = page.last_id;
    eventsLoaded = true;
  } catch (err) {
    markDown(err);
  }
}

// ---- dead-letter queue ----------------------------------------------------

// pointerOver reports whether the mouse is over the table holding tbody id.
// The tables with buttons don't redraw then, so a row can't move under the
// pointer just before a click lands on it. A redraw after the user's own
// click is forced.
function pointerOver(id) {
  return $(id).closest("table").matches(":hover");
}

async function replay(id, button) {
  button.disabled = true;
  try {
    await api("POST", "/dlq/" + encodeURIComponent(id) + "/replay");
    notice("Replayed job " + id);
  } catch (err) {
    notice("Replay failed: " + err.message, true);
  }
  pollDLQ(true);
}

async function pollDLQ(force) {
  try {
    const page = await api("GET", "/dlq?limit=50");
    setText(
      "dlq-sum",
      page.total > page.jobs.length
        ? `latest ${page.jobs.length} of ${page.total}`
        : `${page.total}`,
    );
    $("dlq-empty").hidden = page.total > 0;
    $("purge").disabled = page.total === 0;
    if (!force && pointerOver("dlq")) return;
    $("dlq").replaceChildren(
      ...page.jobs.map((j) => {
        const b = el("button", { type: "button", text: "Replay" });
        b.addEventListener("click", () => replay(j.id, b));
        return el(
          "tr",
          null,
          el("td", null, jobLink(j.id)),
          td(j.type),
          td(`${j.attempts}/${j.max_attempts}`),
          td(dateTime(j.finished_at)),
          el("td", {
            class: "err",
            title: j.last_error || "",
            text: j.last_error || "",
          }),
          el("td", null, b),
        );
      }),
    );
  } catch (err) {
    markDown(err);
  }
}

// Purging needs a second click within 4 s. This avoids a browser confirm()
// dialog, which blocks the page.
let purgeArmed = null;
$("purge").addEventListener("click", async () => {
  const b = $("purge");
  if (!purgeArmed) {
    b.classList.add("armed");
    b.textContent = "Click again to delete all dead jobs";
    purgeArmed = setTimeout(disarmPurge, 4000);
    return;
  }
  disarmPurge();
  b.disabled = true;
  try {
    const r = await api("DELETE", "/dlq");
    notice(`Deleted ${r.purged} dead jobs`);
  } catch (err) {
    notice("Purge failed: " + err.message, true);
  }
  pollDLQ(true);
});

function disarmPurge() {
  clearTimeout(purgeArmed);
  purgeArmed = null;
  const b = $("purge");
  b.classList.remove("armed");
  b.textContent = "Purge all";
}

// ---- crons ---------------------------------------------------------------

async function setCron(name, enabled, button) {
  button.disabled = true;
  try {
    await api("PATCH", "/crons/" + encodeURIComponent(name), { enabled });
    notice(`Cron ${name} ${enabled ? "enabled" : "disabled"}`);
  } catch (err) {
    notice("Could not change cron " + name + ": " + err.message, true);
  }
  pollCrons(true);
}

async function pollCrons(force) {
  try {
    const list = await api("GET", "/crons");
    const now = serverNow();
    $("crons-empty").hidden = list.crons.length > 0;
    if (!force && pointerOver("crons")) return;
    // By name, so a row stays where it is when its cron is turned off. (The
    // API lists disabled crons last.)
    list.crons.sort((a, b) => (a.name < b.name ? -1 : a.name > b.name ? 1 : 0));
    $("crons").replaceChildren(
      ...list.crons.map((c) => {
        const b = el("button", {
          type: "button",
          text: c.enabled ? "Disable" : "Enable",
        });
        b.addEventListener("click", () => setCron(c.name, !c.enabled, b));
        return el(
          "tr",
          null,
          td(c.name, "mono"),
          td(c.schedule, "mono"),
          td(c.job.type),
          td(c.enabled ? until(c.next_run, now) : "disabled"),
          el("td", null, jobLink(c.last_job_id)),
          el("td", null, b),
        );
      }),
    );
  } catch (err) {
    markDown(err);
  }
}

// ---- job details -----------------------------------------------------------

async function showJob(id) {
  const dialog = $("job-dialog");
  $("job-title").textContent = "Job " + id;
  $("job-json").textContent = "loading…";
  if (!dialog.open) dialog.showModal();
  try {
    $("job-json").textContent = JSON.stringify(
      await api("GET", "/jobs/" + encodeURIComponent(id)),
      null,
      2,
    );
  } catch (err) {
    $("job-json").textContent = err.message;
  }
}

// ---- polling ----------------------------------------------------------------

// every runs fn now and then every ms after each run finishes, so a slow
// API never gets overlapping requests from one page.
function every(ms, fn) {
  const tick = async () => {
    await fn();
    setTimeout(tick, ms);
  };
  tick();
}

function refreshAll() {
  pollStats();
  pollDLQ();
  pollCrons();
}

every(STATS_EVERY, pollStats);
every(EVENTS_EVERY, pollEvents);
every(TABLES_EVERY, async () => {
  await pollDLQ();
  await pollCrons();
});
setInterval(drawConn, 500);
