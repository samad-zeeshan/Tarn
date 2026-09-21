/*
 * The opening replay: day 1 of the committed slice as a graph, with the analyst's alerts arriving at their true times.
 *
 * Everything is drawn as a pure function of one number, the second of the day, so playing,
 * pausing and dragging the scrubber backwards all show the same frame for the same second.
 */

const DAY = 86_400;
const DECAY = 1_500; // a login stays lit for 25 minutes of day time, long enough to see at speed
const TYPE = 360; // the agent's answer types out over six minutes of day time
const REDUCED = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

const $ = (s) => document.querySelector(s);
const int = (n) => Number(n).toLocaleString('en-US');
const pad2 = (n) => String(n).padStart(2, '0');
const clock = (t) => {
  const s = Math.max(0, Math.min(DAY - 1, Math.floor(t)));
  return `${pad2(Math.floor(s / 3600))}:${pad2(Math.floor((s % 3600) / 60))}:${pad2(s % 60)}`;
};
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);

/** Seeded PRNG, so the flow field is the same picture on every visit. */
function mulberry(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Smooth value noise on a lattice. Cheap, and plenty for a field nobody should notice first. */
function makeNoise(rand) {
  const N = 256;
  const perm = Array.from({ length: N }, (_, i) => i);
  for (let i = N - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [perm[i], perm[j]] = [perm[j], perm[i]];
  }
  const val = Array.from({ length: N }, () => rand());
  const h = (x, y) => val[perm[(perm[x & 255] + y) & 255]];
  const s = (t) => t * t * (3 - 2 * t);
  return (x, y) => {
    const xi = Math.floor(x);
    const yi = Math.floor(y);
    const u = s(x - xi);
    const v = s(y - yi);
    const a = h(xi, yi) + (h(xi + 1, yi) - h(xi, yi)) * u;
    const b = h(xi, yi + 1) + (h(xi + 1, yi + 1) - h(xi, yi + 1)) * u;
    return a + (b - a) * v;
  };
}

/**
 * Real time is not day time. The day runs fast when nothing happens and slows around each alert
 * and each attack login, so the verdict can be read as it types. This builds that mapping once.
 */
function buildClock(alerts, attacks) {
  const BASE = DAY / 22; // a quiet day crosses in about 22 seconds
  const windows = [
    ...alerts.map((a) => [a.t - 90, a.t + TYPE + 120, (TYPE + 210) / 2.6]),
    ...attacks.map((a) => [a.t - 30, a.t + 180, 210 / 0.7]),
  ];
  const cuts = [...new Set([0, DAY, ...windows.flatMap(([a, b]) => [Math.max(0, a), Math.min(DAY, b)])])]
    .sort((x, y) => x - y);
  const segs = [];
  let real = 0;
  for (let i = 0; i < cuts.length - 1; i++) {
    const t0 = cuts[i];
    const t1 = cuts[i + 1];
    const mid = (t0 + t1) / 2;
    const rate = windows.reduce((r, [a, b, w]) => (mid >= a && mid <= b ? Math.min(r, w) : r), BASE);
    const d = (t1 - t0) / rate;
    segs.push({ t0, t1, r0: real, r1: real + d });
    real += d;
  }
  const toDay = (r) => {
    const s = segs.find((g) => r <= g.r1) || segs[segs.length - 1];
    return s.t0 + ((r - s.r0) / (s.r1 - s.r0 || 1)) * (s.t1 - s.t0);
  };
  const toReal = (t) => {
    const s = segs.find((g) => t <= g.t1) || segs[segs.length - 1];
    return s.r0 + ((t - s.t0) / (s.t1 - s.t0 || 1)) * (s.r1 - s.r0);
  };
  return { total: real, toDay, toReal };
}

export async function setupNight(night, triage) {
  const stage = $('#stage');
  const canvas = $('#night-canvas');
  if (!stage || !canvas) return;
  const ctx = canvas.getContext('2d');
  // The static picture lives on its own canvas underneath, so each frame only clears and redraws
  // the few lines that are lit, instead of copying the whole graph again.
  const baseCanvas = document.createElement('canvas');
  baseCanvas.className = 'night-base';
  baseCanvas.setAttribute('aria-hidden', 'true');
  stage.insertBefore(baseCanvas, canvas);

  const n = night.names.length;
  const deg = new Uint16Array(n);
  night.edges.forEach(([a, b]) => { deg[a]++; deg[b]++; });
  const attackNodes = new Set(night.attacks.flatMap((a) => [a.src_node, a.dst_node]));
  const eventT = night.events.map((e) => e[0]);
  const clockMap = buildClock(night.alerts, night.attacks);

  let W = 0;
  let H = 0;
  let dpr = 1;
  let px = new Float32Array(n);
  let py = new Float32Array(n);
  let base = null;
  let grid = null;
  let hover = -1;
  let t = 0;

  // ---------------------------------------------------------------- layout and base layer
  function fit() {
    const r = stage.getBoundingClientRect();
    const cr = canvas.getBoundingClientRect();
    W = Math.round(cr.width || r.width);
    H = Math.round(cr.height || r.height);
    if (!W || !H) return;
    dpr = Math.min(window.devicePixelRatio || 1, 2);
    canvas.width = W * dpr;
    canvas.height = H * dpr;

    // On a wide screen the copy owns the left third, so the graph sits in the rest.
    const copy = $('.night-copy');
    const wide = W > 860 && getComputedStyle(copy).position === 'absolute';
    const left = wide ? Math.max(W * 0.3, copy.getBoundingClientRect().right - r.left + 24) : 12;
    const right = W - (wide ? 28 : 12);
    const top = 16;
    const bottom = H - (wide ? 20 : 12);
    const xs = night.x;
    const ys = night.y;
    const maxX = Math.max(...xs);
    const maxY = Math.max(...ys);
    const s = Math.min((right - left) / maxX, (bottom - top) / maxY);
    const ox = left + (right - left - maxX * s) / 2;
    const oy = top + (bottom - top - maxY * s) / 2;
    px = Float32Array.from(xs, (v) => ox + v * s);
    py = Float32Array.from(ys, (v) => oy + v * s);

    // A coarse grid for hover lookup, rebuilt only on resize.
    const cell = 24;
    grid = { cell, cols: Math.ceil(W / cell), map: new Map() };
    for (let i = 0; i < n; i++) {
      const k = Math.floor(py[i] / cell) * grid.cols + Math.floor(px[i] / cell);
      if (!grid.map.has(k)) grid.map.set(k, []);
      grid.map.get(k).push(i);
    }
    paintBase();
    draw();
  }

  function radius(i) {
    return Math.min(5.5, 0.9 + Math.sqrt(deg[i]) * 0.42);
  }

  function paintBase() {
    base = baseCanvas;
    base.width = W * dpr;
    base.height = H * dpr;
    const b = base.getContext('2d');
    b.scale(dpr, dpr);

    // The flow field. A tarn is a still mountain lake, and this is its surface: seeded currents
    // at three percent ink, drawn once, never animated, so it adds depth without adding motion.
    const rand = mulberry(20150101);
    const noise = makeNoise(rand);
    const lines = Math.round((W * H) / 2600);
    b.strokeStyle = 'rgba(232, 230, 225, 0.035)';
    b.lineWidth = 0.6;
    for (let k = 0; k < lines; k++) {
      let x = rand() * W;
      let y = rand() * H;
      b.beginPath();
      b.moveTo(x, y);
      for (let step = 0; step < 36; step++) {
        const a = noise(x * 0.0035, y * 0.0035) * Math.PI * 2.4 + 0.6;
        x += Math.cos(a) * 3;
        y += Math.sin(a) * 3;
        b.lineTo(x, y);
      }
      b.stroke();
    }

    b.strokeStyle = 'rgba(206, 214, 226, 0.075)';
    b.lineWidth = 0.6;
    b.beginPath();
    for (const [a, c] of night.edges) {
      b.moveTo(px[a], py[a]);
      b.lineTo(px[c], py[c]);
    }
    b.stroke();

    for (let i = 0; i < n; i++) {
      const r = radius(i);
      if (night.kind[i] === 'c') {
        b.fillStyle = deg[i] > 40 ? 'rgba(206, 214, 226, 0.62)' : 'rgba(128, 133, 141, 0.7)';
        b.fillRect(px[i] - r * 0.85, py[i] - r * 0.85, r * 1.7, r * 1.7);
      } else {
        b.fillStyle = 'rgba(169, 173, 179, 0.5)';
        b.beginPath();
        b.arc(px[i], py[i], r * 0.8, 0, Math.PI * 2);
        b.fill();
      }
    }
  }

  // ---------------------------------------------------------------- per-frame drawing
  function lowerBound(arr, v) {
    let lo = 0;
    let hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (arr[mid] < v) lo = mid + 1; else hi = mid;
    }
    return lo;
  }

  function ring(x, y, r, color, width, alpha) {
    ctx.globalAlpha = alpha;
    ctx.strokeStyle = color;
    ctx.lineWidth = width;
    ctx.beginPath();
    ctx.arc(x, y, r, 0, Math.PI * 2);
    ctx.stroke();
    ctx.globalAlpha = 1;
  }

  const ACCENT = '#f0b340';
  const SIGNAL = '#ff5a4f';

  function draw() {
    if (!base) return;
    ctx.setTransform(1, 0, 0, 1, 0, 0);
    ctx.clearRect(0, 0, canvas.width, canvas.height);
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

    // Logins from the slice light their edge for a while, then fade. This is the day's rhythm.
    if (!REDUCED || t >= DAY - 1) {
      const from = lowerBound(eventT, t - DECAY);
      const to = lowerBound(eventT, t + 0.001);
      ctx.lineWidth = 1;
      for (let k = from; k < to; k++) {
        const [te, e] = night.events[k];
        const [a, c] = night.edges[e];
        const f = 1 - (t - te) / DECAY;
        ctx.strokeStyle = `rgba(240, 179, 64, ${(0.12 + 0.5 * f).toFixed(3)})`;
        ctx.beginPath();
        ctx.moveTo(px[a], py[a]);
        ctx.lineTo(px[c], py[c]);
        ctx.stroke();
      }
    }

    for (const at of night.attacks) {
      if (t < at.t) continue;
      const [a, c] = night.edges[at.edge];
      ctx.strokeStyle = SIGNAL;
      ctx.lineWidth = 1.6;
      ctx.beginPath();
      ctx.moveTo(px[a], py[a]);
      ctx.lineTo(px[c], py[c]);
      ctx.stroke();
      ctx.fillStyle = SIGNAL;
      for (const i of [at.src_node, at.dst_node]) {
        ctx.beginPath();
        ctx.arc(px[i], py[i], radius(i) + 1, 0, Math.PI * 2);
        ctx.fill();
      }
      const age = t - at.t;
      if (age < 420) ring(px[at.dst_node], py[at.dst_node], 4 + age / 14, SIGNAL, 1.4, 1 - age / 420);
    }

    let latest = null;
    for (const al of night.alerts) {
      if (t < al.t) continue;
      latest = al;
      const age = t - al.t;
      const u = al.account_node;
      const d = al.dst_node;
      const grow = Math.min(1, age / 160);
      ctx.strokeStyle = ACCENT;
      ctx.lineWidth = 1.8;
      ctx.beginPath();
      ctx.moveTo(px[u], py[u]);
      ctx.lineTo(px[u] + (px[d] - px[u]) * grow, py[u] + (py[d] - py[u]) * grow);
      ctx.stroke();
      if (age < 600) ring(px[d], py[d], 5 + age / 18, ACCENT, 1.5, 1 - age / 600);
      if (age >= TYPE) {
        const said = al.call === 'true_positive';
        ring(px[d], py[d], radius(d) + 4, said ? SIGNAL : 'rgba(169, 173, 179, 0.7)', said ? 2 : 1, 1);
      }
    }

    // Name only the newest alert's target, so the picture never turns into a wall of labels.
    ctx.font = '500 11px "JetBrains Mono", monospace';
    if (latest) label(latest.dst_node, latest.destination, ACCENT);
    if (t >= night.attacks[0]?.t) label(night.attacks[0].src_node, `${night.attacks[0].source}, attack source`, SIGNAL);
    if (hover >= 0) {
      ring(px[hover], py[hover], radius(hover) + 5, '#e8e6e1', 1.2, 0.9);
    }
  }

  function label(i, text, color) {
    const x = px[i] + 10;
    const y = py[i] - 10;
    const w = ctx.measureText(text).width;
    ctx.fillStyle = 'rgba(11, 13, 16, 0.82)';
    ctx.fillRect(x - 4, y - 11, w + 8, 16);
    ctx.fillStyle = color;
    ctx.fillText(text, x, y + 1);
  }

  // ---------------------------------------------------------------- the feed
  const list = $('#alerts');
  const feed = list.closest('.feed');
  const empty = $('#feed-empty');
  const countEl = $('#feed-count');
  const countLabel = $('#feed-count-label');
  const toggle = $('#tri-agent');

  const rows = night.alerts.map((a) => {
    const li = document.createElement('li');
    const said = a.call === 'true_positive';
    li.className = `alert${a.attack ? ' truth-attack' : ''}${said ? '' : ' closed-by-agent'}`;
    li.hidden = true;
    const text = `Agent: ${said ? 'attack' : a.call.replace('_', ' ').replace('false positive', 'false alarm')}, confidence ${a.confidence.toFixed(2)}`;
    li.innerHTML = `<button type="button" class="alert-in" aria-expanded="false"><span class="alert-body">
      <span class="a-time">${clock(a.t)}</span>
      <span class="a-who" translate="no">${esc(a.account)} <span class="a-path">${esc(a.source)} to ${esc(a.destination)}</span></span>
      <span class="a-score" title="detector score">${a.score.toFixed(1)}</span>
      <span class="a-reason">${esc(a.reason)}</span>
      <span class="a-call${said ? ' said-attack' : ''}"><span class="typed"></span><span class="caret" hidden></span></span>
      <span class="a-why">${esc(a.why)}</span>
      <span class="a-truth">The answer key says this was the attack.</span>
    </span></button>`;
    const inner = li.firstElementChild;
    inner.addEventListener('click', () => {
      inner.setAttribute('aria-expanded', String(li.classList.toggle('open')));
    });
    list.append(li);
    return {
      a, li, text, said,
      typed: li.querySelector('.typed'), caret: li.querySelector('.caret'),
      shown: -1, visible: false, done: null,
    };
  });

  let agentOn = false;
  let lastVisible = 0;
  let shownCount = 0;
  const counter = { v: 0 };

  function setCount(v, animate) {
    if (v === shownCount) return;
    shownCount = v;
    if (animate && window.gsap && !REDUCED) {
      window.gsap.to(counter, {
        v, duration: 0.6, ease: 'power2.out', overwrite: true,
        onUpdate: () => { countEl.textContent = String(Math.round(counter.v)); },
      });
    } else {
      counter.v = v;
      countEl.textContent = String(v);
    }
  }

  function updateFeed(forward) {
    let visible = 0;
    let left = 0;
    for (const r of rows) {
      const on = t >= r.a.t;
      if (on !== r.visible) {
        r.visible = on;
        r.li.hidden = !on;
        if (on && forward && !REDUCED) {
          r.li.classList.add('arriving');
          setTimeout(() => r.li.classList.remove('arriving'), 600);
          list.scrollTo({ top: list.scrollHeight, behavior: 'smooth' });
        }
      }
      if (!on) continue;
      visible++;
      const k = Math.max(0, Math.min(1, (t - r.a.t) / TYPE));
      const chars = Math.round(k * r.text.length);
      if (chars !== r.shown) {
        r.shown = chars;
        r.typed.textContent = r.text.slice(0, chars);
      }
      // Judged on k, not on the character count: the last character can land a frame before k reaches 1.
      const done = k >= 1;
      if (done !== r.done) {
        r.done = done;
        r.caret.hidden = done;
        r.li.classList.toggle('said', done);
      }
      r.li.classList.toggle('closed', agentOn && !r.said);
      if (!agentOn || r.said) left++;
    }
    empty.hidden = visible > 0;
    // After a jump, keep the newest alert in view. Forward play scrolls smoothly on arrival instead.
    if (!forward && visible !== lastVisible) list.scrollTop = list.scrollHeight;
    lastVisible = visible;
    setCount(agentOn ? left : visible, false);
  }

  // ---------------------------------------------------------------- the workload strip
  const work = $('#tri-work');
  const w = triage.analyst.with;
  const workNums = {};
  if (w) {
    const wl = w.workload;
    work.innerHTML = `
      <div><span class="num-big" id="wk-alerts">${int(wl.alerts_read_without_agent)}</span>
        <span class="lbl">alerts a person sorts across the test days</span></div>
      <div><span class="num-big" id="wk-hours">${int(wl.analyst_hours_without_agent)}</span>
        <span class="lbl">analyst hours, at ${triage.triage_minutes} minutes an alert</span></div>
      <div><span class="num-big" id="wk-recall">100.0%</span>
        <span class="lbl">of attack alerts still called attacks or passed to a person</span></div>
      <div><p id="wk-note"></p></div>`;
    workNums.alerts = { el: $('#wk-alerts'), off: wl.alerts_read_without_agent, on: wl.alerts_read_with_agent, v: wl.alerts_read_without_agent, f: (v) => int(Math.round(v)) };
    workNums.hours = { el: $('#wk-hours'), off: wl.analyst_hours_without_agent, on: wl.analyst_hours_with_agent, v: wl.analyst_hours_without_agent, f: (v) => int(Math.round(v)) };
    workNums.recall = { el: $('#wk-recall'), off: 1, on: w.cascade.attack_recall, v: 1, f: (v) => `${(100 * v).toFixed(1)}%` };
  }
  const workNote = () => {
    const a = triage.analyst;
    const base = `Measured on the ${int(a.scored)} of ${int(a.size)} benchmark alerts the agent finished before the model became unavailable.`;
    $('#wk-note').textContent = agentOn
      ? `The agent closes anything it is ${w.cascade.accept_at} sure of or more, and it was that sure almost every time. It closed ${int(w.attacks_closed_as_benign)} real attacks as false alarms. ${base}`
      : `Without the agent a person reads every alert in the wider feed of the detector. ${base}`;
  };
  if (w) workNote();

  function setAgent(on) {
    agentOn = on;
    toggle.setAttribute('aria-checked', String(on));
    feed.classList.toggle('agent', on);
    countLabel.textContent = on ? 'left on the list' : 'alerts so far';
    const before = shownCount;
    updateFeed(false);
    const after = shownCount;
    shownCount = before;
    setCount(after, true);
    for (const key of Object.keys(workNums)) {
      const m = workNums[key];
      const to = on ? m.on : m.off;
      m.el.classList.toggle('good', on && key !== 'recall');
      m.el.classList.toggle('bad', on && key === 'recall');
      if (window.gsap && !REDUCED) {
        window.gsap.to(m, {
          v: to, duration: 1.1, ease: 'power3.out', overwrite: true,
          onUpdate: () => { m.el.textContent = m.f(m.v); },
        });
      } else {
        m.v = to;
        m.el.textContent = m.f(to);
      }
    }
    workNote();
  }
  toggle.addEventListener('click', () => setAgent(!agentOn));

  // ---------------------------------------------------------------- notes, ticks, clock
  $('#stage-note').textContent =
    `Drawn from the committed 1-in-${int(night.sample_rate)} sample of day ${night.day}: ` +
    `${int(n)} people and computers, ${int(night.logins_drawn)} logins between machines, and all ` +
    `${int(night.attacks.length)} attack logins that day. The alerts come from the full log. The log ` +
    'counts seconds from when recording began, so t+ is time since the day began, not a clock. ' +
    'The replay slows down when something happens.';
  const a = triage.analyst;
  $('#feed-note').textContent =
    `These are the ${int(night.alerts.length)} alerts from day ${night.day} in the agent's benchmark, read ` +
    `by ${a.model} with graph tools. Tap one to read its full reasoning.`;

  const ticks = $('#scrub-ticks');
  ticks.innerHTML =
    night.alerts.map((al) => `<i style="left:${(100 * al.t) / DAY}%"></i>`).join('') +
    night.attacks.map((at) => `<i class="attack" style="left:${(100 * at.t) / DAY}%"></i>`).join('') +
    [6, 12, 18].map((h) => `<b style="left:${(100 * h) / 24}%">${h}h</b>`).join('');

  const clockT = $('#clock-t');
  const clockDay = $('#clock-day');
  clockDay.textContent = `day ${night.day}`;
  const scrub = $('#scrub');
  const fill = $('#scrub-fill');
  const head = $('#scrub-head');
  let lastMinute = -1;
  // Measured on resize only. Reading it inside render() would force a layout every frame.
  let scrubW = scrub.clientWidth;
  new ResizeObserver(() => { scrubW = scrub.clientWidth; render(false); }).observe(scrub);

  function render(forward = true) {
    const p = t / DAY;
    fill.style.transform = `scaleX(${p})`;
    head.style.transform = `translateX(${p * scrubW}px)`;
    clockT.textContent = `t+ ${clock(t)}`;
    const minute = Math.floor(t / 60);
    if (minute !== lastMinute) {
      lastMinute = minute;
      scrub.setAttribute('aria-valuenow', String(Math.floor(t)));
      scrub.setAttribute('aria-valuetext', `${clock(t)} into day ${night.day}`);
    }
    updateFeed(forward);
    draw();
  }

  // ---------------------------------------------------------------- playback
  const play = $('#play');
  const state = { r: 0 };
  let tween = null;
  let playing = false;
  let autoPaused = false;

  function setPlayState(s) {
    play.dataset.state = s;
    play.setAttribute('aria-label', s === 'playing' ? 'Pause the replay' : s === 'ended' ? 'Replay the day' : 'Play the replay');
  }

  function seekReal(r, forward = false) {
    state.r = Math.max(0, Math.min(clockMap.total, r));
    t = clockMap.toDay(state.r);
    render(forward);
  }

  function start() {
    if (!window.gsap) return;
    if (state.r >= clockMap.total - 0.01) seekReal(0);
    tween?.kill();
    tween = window.gsap.to(state, {
      r: clockMap.total,
      duration: clockMap.total - state.r,
      ease: 'none',
      onUpdate: () => { t = clockMap.toDay(state.r); render(true); },
      onComplete: () => { playing = false; setPlayState('ended'); },
    });
    playing = true;
    setPlayState('playing');
  }

  function stop(state_ = 'paused') {
    tween?.kill();
    tween = null;
    playing = false;
    setPlayState(state_);
  }

  play.addEventListener('click', () => {
    autoPaused = false;
    if (playing) stop(); else start();
  });

  // The scrubber tracks the pointer 1:1 from pointer-down, and keeps tracking outside its box.
  let wasPlaying = false;
  const seekFromPointer = (e) => {
    const r = scrub.getBoundingClientRect();
    const p = Math.max(0, Math.min(1, (e.clientX - r.left) / r.width));
    seekReal(clockMap.toReal(p * DAY));
  };
  scrub.addEventListener('pointerdown', (e) => {
    try { scrub.setPointerCapture(e.pointerId); } catch { /* a synthetic pointer cannot be captured */ }
    scrub.classList.add('dragging');
    wasPlaying = playing;
    if (playing) stop();
    seekFromPointer(e);
  });
  scrub.addEventListener('pointermove', (e) => { if (scrub.classList.contains('dragging')) seekFromPointer(e); });
  const release = (e) => {
    if (!scrub.classList.contains('dragging')) return;
    if (scrub.hasPointerCapture(e.pointerId)) scrub.releasePointerCapture(e.pointerId);
    scrub.classList.remove('dragging');
    if (wasPlaying && t < DAY - 1) start();
    else setPlayState(t >= DAY - 1 ? 'ended' : 'paused');
  };
  scrub.addEventListener('pointerup', release);
  scrub.addEventListener('pointercancel', release);
  scrub.addEventListener('keydown', (e) => {
    const step = e.shiftKey ? 3600 : 900;
    let to = null;
    if (e.key === 'ArrowRight' || e.key === 'ArrowUp') to = t + step;
    else if (e.key === 'ArrowLeft' || e.key === 'ArrowDown') to = t - step;
    else if (e.key === 'Home') to = 0;
    else if (e.key === 'End') to = DAY - 1;
    else if (e.key === ' ' || e.key === 'Enter') { e.preventDefault(); play.click(); return; }
    if (to === null) return;
    e.preventDefault();
    if (playing) stop();
    seekReal(clockMap.toReal(Math.max(0, Math.min(DAY - 1, to))), to > t);
    if (t >= DAY - 1) setPlayState('ended');
  });

  // Hover names a point, so the graph can be read and not only watched.
  const tip = $('#stage-tip');
  let pending = null;
  canvas.addEventListener('pointermove', (e) => {
    if (e.pointerType !== 'mouse') return;
    pending = e;
    requestAnimationFrame(() => {
      if (!pending) return;
      const r = canvas.getBoundingClientRect();
      const x = pending.clientX - r.left;
      const y = pending.clientY - r.top;
      pending = null;
      let best = -1;
      let bd = 81;
      const cx = Math.floor(x / grid.cell);
      const cy = Math.floor(y / grid.cell);
      for (let gy = cy - 1; gy <= cy + 1; gy++) {
        for (let gx = cx - 1; gx <= cx + 1; gx++) {
          for (const i of grid.map.get(gy * grid.cols + gx) || []) {
            const d = (px[i] - x) ** 2 + (py[i] - y) ** 2;
            if (d < bd) { bd = d; best = i; }
          }
        }
      }
      if (best !== hover) {
        hover = best;
        if (best >= 0) {
          const kind = night.kind[best] === 'c' ? 'computer' : 'person';
          const role = attackNodes.has(best) ? ', touched by the attack' : '';
          tip.innerHTML = `${esc(night.names[best])} <span>${kind}${role}, ${int(deg[best])} link${deg[best] === 1 ? '' : 's'} that day</span>`;
          tip.hidden = false;
          const left = Math.min(px[best] + 12, W - tip.offsetWidth - 8);
          tip.style.transform = `translate(${left}px, ${Math.max(8, py[best] - 34)}px)`;
          tip.style.left = '0';
          tip.style.top = '0';
        } else tip.hidden = true;
        draw();
      }
    });
  });
  canvas.addEventListener('pointerleave', () => { hover = -1; tip.hidden = true; draw(); });

  // Nobody is watching a replay that is off screen or in a background tab.
  const io = new IntersectionObserver(([entry]) => {
    if (!entry.isIntersecting && playing) { autoPaused = true; stop(); }
    else if (entry.isIntersecting && autoPaused) { autoPaused = false; start(); }
  }, { threshold: 0.15 });
  io.observe(stage);
  document.addEventListener('visibilitychange', () => {
    if (document.hidden && playing) { autoPaused = true; stop(); }
    else if (!document.hidden && autoPaused) { autoPaused = false; start(); }
  });

  let resizeTimer = null;
  new ResizeObserver(() => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => { fit(); render(false); }, 120);
  }).observe(stage);

  fit();
  if (REDUCED || !window.gsap) {
    // Reduced motion lands on the finished day: every alert answered, nothing moving.
    seekReal(clockMap.total);
    setPlayState('ended');
  } else {
    seekReal(0);
    start();
  }
}
