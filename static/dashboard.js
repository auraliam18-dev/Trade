"use strict";
const $ = (s) => document.querySelector(s),
  $$ = (s) => [...document.querySelectorAll(s)];
const state = {
  status: {},
  signals: [],
  universe: [],
  events: [],
  busy: false,
};
const esc = (v) =>
  String(v ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const num = (v, d = 2) =>
  Number(v || 0).toLocaleString("fa-IR", { maximumFractionDigits: d });
const price = (v) => {
  v = Number(v || 0);
  return v.toLocaleString("en-US", {
    maximumFractionDigits: v < 1 ? 8 : v < 100 ? 4 : 2,
  });
};
const money = (v) =>
  Number(v || 0).toLocaleString("en-US", { maximumFractionDigits: 2 }) +
  " USDT";
const compact = (v) =>
  new Intl.NumberFormat("fa-IR", {
    notation: "compact",
    maximumFractionDigits: 1,
  }).format(Number(v || 0));
function toast(t) {
  const el = $("#toast");
  el.textContent = t;
  el.classList.add("show");
  setTimeout(() => el.classList.remove("show"), 2500);
}
async function api(path, opt = {}) {
  const ctrl = new AbortController(),
    timer = setTimeout(() => ctrl.abort(), opt.timeout || 15000);
  try {
    const r = await fetch(path, {
      ...opt,
      signal: ctrl.signal,
      headers: { "Content-Type": "application/json", ...(opt.headers || {}) },
    });
    if (!r.ok) throw Error(`HTTP ${r.status}`);
    return await r.json();
  } finally {
    clearTimeout(timer);
  }
}
function metric(label, value, sub, cls = "") {
  return `<div class="metric"><label>${label}</label><strong class="${cls}">${value}</strong><small>${sub}</small></div>`;
}
function renderMetrics() {
  const s = state.status,
    p = s.paper || {},
    m = s.market_context || {},
    longs = state.signals.filter((x) => x.direction === "LONG").length;
  $("#metrics").innerHTML =
    metric(
      "سیگنال فعال",
      num(state.signals.length, 0),
      `${num(longs, 0)} لانگ · ${num(state.signals.length - longs, 0)} شورت`,
      "green",
    ) +
    metric(
      "حالت بازار",
      esc(m.risk_mode || "نامشخص"),
      `BTC: ${num(m.btc_change_24h_pct)}٪`,
      m.risk_mode === "RISK_OFF" ? "red" : "",
    ) +
    metric(
      "سرمایه Paper",
      money(p.equity || s.settings?.initial_equity),
      `P&L: ${money((p.realized_pnl || 0) + (p.unrealized_pnl || 0))}`,
      (p.realized_pnl || 0) + (p.unrealized_pnl || 0) >= 0 ? "green" : "red",
    ) +
    metric(
      "پوزیشن باز",
      num((p.active_positions || []).length, 0),
      `حداکثر ${num(s.settings?.max_open_positions, 0)} موقعیت`,
    ) +
    metric(
      "وضعیت موتور",
      esc(s.status || "OFFLINE"),
      s.last_error ? esc(s.last_error) : "داده بدون کش آینده‌نگر",
      s.status === "ONLINE" ? "green" : s.status === "ERROR" ? "red" : "amber",
    );
}
function renderSystemAlert() {
  const alert = $("#systemAlert");
  const status = state.status.status;
  if (status === "DATA_SOURCE_LIMITED") {
    alert.hidden = false;
    alert.className = "system-alert";
    alert.textContent =
      "دسترسی به منبع داده بازار محدود است؛ صفر بودن سیگنال‌ها در این وضعیت به معنی نبود ستاپ معاملاتی نیست. اتصال Binance Futures را بررسی کنید.";
  } else if (status === "ERROR") {
    alert.hidden = false;
    alert.className = "system-alert error";
    alert.textContent = `خطای موتور اسکن: ${state.status.last_error || "خطای نامشخص"}`;
  } else {
    alert.hidden = true;
    alert.textContent = "";
  }
}
function signalCard(x, i) {
  const side = (x.direction || "").toLowerCase();
  return `<article class="card ${side}"><div class="cardhead"><div><div class="symbol">${esc(x.binance_symbol)}</div><div class="asset">#${esc(x.rank)} · ${esc(x.asset_name || x.asset_symbol)}</div></div><span class="side">${esc(x.direction)}</span></div><div class="scoreline"><b>${num(x.score)} امتیاز</b><div class="scorebar"><i style="width:${Math.min(100, Number(x.score) || 0)}%"></i></div><span class="chip">${esc(x.grade)}</span></div><div class="levels"><div class="level"><label>ENTRY</label><b>${price(x.entry)}</b></div><div class="level"><label>STOP LOSS</label><b class="red">${price(x.stop_loss)}</b></div><div class="level"><label>TP 1</label><b class="green">${price(x.tp1)}</b></div><div class="level"><label>TP 2</label><b class="green">${price(x.tp2)}</b></div></div><div class="facts"><span class="chip">تایید ${num(x.tf_agreement, 0)}/3 TF</span><span class="chip">R:R ${num(x.rr)}</span><span class="chip">Funding ${num(Number(x.funding_rate) * 100, 4)}٪</span><span class="chip">24h ${num(x.change_24h_pct)}٪</span></div><div class="cardfoot"><span class="asset">${x.persistence === "entry_sl_tp_preserved" ? "🔒 سطوح ورود قفل شده" : "سیگنال تاییدشده"}</span><button class="link" data-detail="${i}">جزئیات و دلایل ←</button></div></article>`;
}
function renderSignals() {
  const q = $("#search").value.trim().toUpperCase(),
    dir = $("#direction").value,
    min = Number($("#quality").value);
  const rows = state.signals.filter(
    (x) =>
      (!q ||
        (x.binance_symbol || "").includes(q) ||
        (x.asset_name || "").toUpperCase().includes(q)) &&
      (!dir || x.direction === dir) &&
      Number(x.score) >= min,
  );
  $("#signalCount").textContent = `${num(rows.length, 0)} نتیجه`;
  $("#signalGrid").innerHTML = rows.length
    ? rows.map((x) => signalCard(x, state.signals.indexOf(x))).join("")
    : `<div class="empty"><b>فعلاً ستاپ تاییدشده‌ای نیست</b>موتور فقط سیگنال‌های دارای نقدشوندگی، روند و تایید چندتایم‌فریمی را نمایش می‌دهد.</div>`;
}
function renderUniverse() {
  const q = $("#radarSearch").value.trim().toUpperCase(),
    st = $("#radarStatus").value;
  const rows = state.universe.filter(
    (x) =>
      (!q || (x.binance_symbol || x.asset_symbol || "").includes(q)) &&
      (!st || x.status === st),
  );
  $("#universeCount").textContent =
    `${num(rows.length, 0)} از ${num(state.universe.length, 0)}`;
  $("#universeRows").innerHTML =
    rows
      .slice(0, 150)
      .map(
        (x) =>
          `<tr><td>${esc(x.rank || "—")}</td><td class="mono">${esc(x.binance_symbol || x.asset_symbol)}</td><td>${esc(x.status)}</td><td class="${x.direction === "LONG" ? "green" : x.direction === "SHORT" ? "red" : ""}">${esc(x.direction)}</td><td>${num(x.score)}</td><td>${num(x.tf_agreement, 0)}/3</td><td class="${Number(x.change_24h_pct) >= 0 ? "green" : "red"}">${num(x.change_24h_pct)}٪</td><td>${compact(x.quote_volume)}</td></tr>`,
      )
      .join("") || `<tr><td colspan="8">داده‌ای مطابق فیلتر نیست.</td></tr>`;
}
function renderPaper() {
  const s = state.status,
    p = s.paper || {},
    stats = p.stats || {};
  $("#paperStats").innerHTML =
    `<div><span>Equity</span><b>${money(p.equity || s.settings?.initial_equity)}</b></div><div><span>سود محقق‌شده</span><b class="${Number(p.realized_pnl) >= 0 ? "green" : "red"}">${money(p.realized_pnl)}</b></div><div><span>Win rate</span><b>${num(stats.win_rate_pct)}٪</b></div>`;
  $("#riskRules").innerHTML =
    `<div class="event"><p>ریسک هر معامله: <b>${num(s.settings?.risk_per_trade_pct)}٪</b></p></div><div class="event"><p>حد ضرر روزانه: <b>${num(s.settings?.max_daily_loss_pct)}٪</b></p></div><div class="event"><p>حداکثر پوزیشن همزمان: <b>${num(s.settings?.max_open_positions, 0)}</b></p></div>`;
  $("#startPaper").disabled = !!p.enabled;
  $("#stopPaper").disabled = !p.enabled;
  $("#positions").innerHTML =
    (p.active_positions || [])
      .map(
        (x) =>
          `<tr><td class="mono">${esc(x.symbol || x.binance_symbol)}</td><td>${esc(x.direction)}</td><td>${price(x.entry)}</td><td>${price(x.current_price)}</td><td class="${Number(x.unrealized_pnl) >= 0 ? "green" : "red"}">${money(x.unrealized_pnl)}</td><td>${money(x.risk_usdt)}</td><td>${esc(x.status || "ACTIVE")}</td></tr>`,
      )
      .join("") ||
    `<tr><td colspan="7">پوزیشن آزمایشی بازی وجود ندارد.</td></tr>`;
}
function renderEvents() {
  $("#eventList").innerHTML = state.events.length
    ? state.events
        .slice(0, 100)
        .map(
          (x) =>
            `<div class="event"><time>${esc(new Date(x.time).toLocaleString("fa-IR"))}</time><p class="${x.level === "ERROR" ? "red" : x.level === "WARN" ? "amber" : ""}">${esc(x.message)}</p></div>`,
        )
        .join("")
    : `<div class="empty">رویدادی ثبت نشده است.</div>`;
}
function openDetail(i) {
  const x = state.signals[i];
  if (!x) return;
  const plan = `${x.binance_symbol} ${x.direction}\nEntry: ${x.entry}\nSL: ${x.stop_loss}\nTP1: ${x.tp1}\nTP2: ${x.tp2}\nScore: ${x.score}`;
  $("#dialog").innerHTML =
    `<div class="dialoghead"><div><h2 style="margin:0" class="mono">${esc(x.binance_symbol)} · ${esc(x.direction)}</h2><span class="asset">امتیاز ${num(x.score)} · تایید ${num(x.tf_agreement, 0)}/3</span></div><button class="close">×</button></div><h3>دلایل تایید موتور</h3><ul class="reasons">${(x.reasons || []).map((r) => `<li>${esc(r)}</li>`).join("") || "<li>دلیلی ثبت نشده است.</li>"}</ul><div class="levels"><div class="level"><label>ADX 15m</label><b>${num(x.metrics?.["15m"]?.adx)}</b></div><div class="level"><label>RSI 15m</label><b>${num(x.metrics?.["15m"]?.rsi)}</b></div><div class="level"><label>Volume Z</label><b>${num(x.metrics?.["15m"]?.volume_z)}</b></div><div class="level"><label>Pre-pump</label><b>${num(x.pre_pump_score)}</b></div></div><button class="btn primary" id="copyPlan" style="margin-top:16px">کپی پلن معامله</button>`;
  $("#modal").classList.add("open");
  $(".close").onclick = () => $("#modal").classList.remove("open");
  $("#copyPlan").onclick = () =>
    navigator.clipboard.writeText(plan).then(() => toast("پلن معامله کپی شد"));
}
function setConnection(ok) {
  $("#dot").className =
    `dot ${ok ? (state.status.status === "ERROR" ? "error" : "online") : "error"}`;
  $("#statusText").textContent = ok
    ? state.status.status === "SCANNING"
      ? "در حال اسکن"
      : "متصل به موتور"
    : "قطع ارتباط";
}
async function refresh() {
  try {
    const [status, signals, universe, events] = await Promise.all([
      api("/api/status"),
      api("/api/signals"),
      api("/api/universe"),
      api("/api/events"),
    ]);
    Object.assign(state, {
      status,
      signals: Array.isArray(signals) ? signals : [],
      universe: Array.isArray(universe) ? universe : [],
      events: Array.isArray(events) ? events : [],
    });
    setConnection(true);
    renderSystemAlert();
    renderMetrics();
    renderSignals();
    renderUniverse();
    renderPaper();
    renderEvents();
    $("#updated").textContent =
      `آخرین اسکن: ${status.last_scan_at ? new Date(status.last_scan_at).toLocaleString("fa-IR") : "هنوز انجام نشده"}`;
  } catch (e) {
    setConnection(false);
    toast("ارتباط با سرور برقرار نشد");
  }
}
async function post(path, body = {}) {
  if (state.busy) return;
  state.busy = true;
  $("#scanBtn").disabled = true;
  try {
    await api(path, {
      method: "POST",
      body: JSON.stringify(body),
      timeout: 180000,
    });
    await refresh();
    toast("عملیات با موفقیت انجام شد");
  } catch (e) {
    toast("خطا در اجرای عملیات");
  } finally {
    state.busy = false;
    $("#scanBtn").disabled = false;
  }
}
$$(".tab").forEach(
  (b) =>
    (b.onclick = () => {
      $$(".tab,.panel").forEach((x) => x.classList.remove("active"));
      b.classList.add("active");
      $("#" + b.dataset.tab).classList.add("active");
    }),
);
["search", "direction", "quality"].forEach((id) =>
  $("#" + id).addEventListener("input", renderSignals),
);
["radarSearch", "radarStatus"].forEach((id) =>
  $("#" + id).addEventListener("input", renderUniverse),
);
$("#signalGrid").onclick = (e) => {
  const b = e.target.closest("[data-detail]");
  if (b) openDetail(Number(b.dataset.detail));
};
$("#modal").onclick = (e) => {
  if (e.target.id === "modal") e.currentTarget.classList.remove("open");
};
$("#scanBtn").onclick = () => post("/api/scan-now");
$("#startPaper").onclick = () => post("/api/start-paper", { hours: 8 });
$("#stopPaper").onclick = () => post("/api/stop-paper");
renderMetrics();
renderSignals();
refresh();
setInterval(refresh, 15000);
