/*
 * Hand rolled SVG charts. No chart library, no CDN.
 *
 * Every chart carries a direct value label and a table view. That is not decoration: the
 * palette validator warns that two light-mode slots fall under 3:1 contrast against white,
 * which obliges a non-colour channel for the value.
 */

const NS = 'http://www.w3.org/2000/svg';

const el = (name, attrs = {}, text) => {
  const node = document.createElementNS(NS, name);
  for (const [k, v] of Object.entries(attrs)) {
    if (v !== null && v !== undefined) node.setAttribute(k, String(v));
  }
  if (text !== undefined) node.textContent = String(text);
  return node;
};

const fmt = {
  int: (n) => Number(n).toLocaleString('en-US'),
  s: (n) => `${Number(n).toFixed(2)}s`,
  ms: (n) => `${Math.round(Number(n)).toLocaleString('en-US')} ms`,
  pct: (n) => `${Number(n).toFixed(1)}%`,
};

// One shared tooltip, positioned fixed. It follows the pointer and also keyboard focus, since
// a tooltip that only answers to a mouse is not accessible.
const tip = document.getElementById('tip');

function showTip(html, evt) {
  tip.innerHTML = html;
  tip.classList.add('show');
  tip.setAttribute('aria-hidden', 'false');
  const pad = 12;
  let x, y;
  if (evt && evt.clientX !== undefined && evt.clientX !== 0) {
    x = evt.clientX + pad;
    y = evt.clientY + pad;
  } else if (evt && evt.target) {
    // Keyboard focus has no pointer coords, so anchor to the mark itself.
    const r = evt.target.getBoundingClientRect();
    x = r.left + r.width / 2;
    y = r.top - pad;
  }
  const w = tip.offsetWidth;
  const h = tip.offsetHeight;
  if (x + w > window.innerWidth - pad) x = window.innerWidth - w - pad;
  if (y + h > window.innerHeight - pad) y = y - h - pad * 2;
  if (y < pad) y = pad;
  tip.style.left = `${Math.max(pad, x)}px`;
  tip.style.top = `${y}px`;
}

function hideTip() {
  tip.classList.remove('show');
  tip.setAttribute('aria-hidden', 'true');
}

/** Wire a mark for mouse and keyboard. */
function interactive(node, html, label) {
  node.classList.add('mark');
  node.setAttribute('tabindex', '0');
  node.setAttribute('role', 'img');
  node.setAttribute('aria-label', label);
  node.addEventListener('mouseenter', (e) => showTip(html, e));
  node.addEventListener('mousemove', (e) => showTip(html, e));
  node.addEventListener('mouseleave', hideTip);
  node.addEventListener('focus', (e) => showTip(html, e));
  node.addEventListener('blur', hideTip);
  return node;
}

function svgRoot(w, h, title) {
  const svg = el('svg', {
    viewBox: `0 0 ${w} ${h}`,
    role: 'img',
    'aria-label': title,
  });
  return svg;
}

let gradSeq = 0;

/** A left-to-right gradient of one colour, so a flat bar gets some depth. */
function gradientFor(svg, color) {
  let defs = svg.querySelector('defs');
  if (!defs) {
    defs = el('defs');
    svg.append(defs);
  }
  const id = `g${gradSeq++}`;
  const lg = el('linearGradient', { id, x1: '0', y1: '0', x2: '1', y2: '0' });
  lg.append(el('stop', { offset: '0%', 'stop-color': color, 'stop-opacity': 0.72 }));
  lg.append(el('stop', { offset: '100%', 'stop-color': color, 'stop-opacity': 1 }));
  defs.append(lg);
  return `url(#${id})`;
}

const REDUCED = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

/** Grow a bar out from the baseline the first time it is drawn. */
function growIn(rect, width) {
  if (REDUCED) {
    rect.setAttribute('width', width);
    return;
  }
  rect.setAttribute('width', 0);
  requestAnimationFrame(() => {
    rect.style.transition = 'width 720ms cubic-bezier(0.16, 1, 0.3, 1)';
    rect.setAttribute('width', width);
  });
}

/** Round the axis top up so it reads in human numbers. */
function niceMax(v) {
  if (v <= 0) return 1;
  const mag = 10 ** Math.floor(Math.log10(v));
  const norm = v / mag;
  const step = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10;
  return step * mag;
}

// Horizontal, not vertical, because the category labels are long phrases and rotating axis
// labels to fit vertical bars is unreadable.
export function barChart(mount, { rows, valueKey, labelKey, colorKey, format, note, max }) {
  mount.innerHTML = '';

  // Size the viewBox to the container's real pixel width so the SVG renders at 1:1. Fixing the
  // width and letting the browser stretch it scaled one chart to 1.57x and blew its 10px labels
  // up to 16px, which is why the typography looked inconsistent between panels.
  const W = Math.max(360, Math.round(mount.clientWidth || 560));
  const rowH = 44;
  const padL = Math.min(210, Math.round(W * 0.34));
  const padR = 78;
  const padT = 6;
  const H = padT + rows.length * rowH + 10;

  const svg = svgRoot(W, H, note || 'bar chart');
  const top = max ?? niceMax(Math.max(...rows.map((r) => r[valueKey])));
  const plot = W - padL - padR;
  const scale = (v) => (plot * v) / top;

  for (let i = 0; i <= 4; i++) {
    const x = padL + (plot * i) / 4;
    svg.append(
      el('line', {
        x1: x, y1: padT, x2: x, y2: H - 10,
        stroke: 'var(--grid)', 'stroke-width': 1,
      }),
    );
  }

  rows.forEach((r, i) => {
    const y = padT + i * rowH;
    const v = r[valueKey];
    const w = Math.max(3, scale(v));
    const color = r[colorKey] || 'var(--series-1)';

    // Labels wear ink, never the series colour. The mark beside them carries the identity.
    svg.append(
      el('text', {
        x: padL - 14, y: y + rowH / 2 + 4, 'text-anchor': 'end',
        fill: 'var(--fg-muted)', 'font-size': 12, 'font-family': 'var(--sans)',
      }, r[labelKey]),
    );

    const barH = rowH - 20;
    svg.append(
      el('rect', {
        x: padL, y: y + 10, width: plot, height: barH,
        rx: 5, fill: 'var(--track)',
      }),
    );

    const bar = el('rect', {
      x: padL, y: y + 10, height: barH,
      rx: 5, fill: gradientFor(svg, color),
    });
    interactive(
      bar,
      `<div><b>${r[labelKey]}</b></div><div><span class="tip-k">value</span> ${format(v)}</div>` +
        (r.tip ? `<div style="margin-top:4px;color:var(--fg-muted)">${r.tip}</div>` : ''),
      `${r[labelKey]}: ${format(v)}`,
    );
    svg.append(bar);
    growIn(bar, w);

    // The direct label is required by the contrast rule above, and it saves the eye a trip to
    // the axis anyway.
    svg.append(
      el('text', {
        x: padL + w + 10, y: y + rowH / 2 + 4,
        fill: 'var(--fg)', 'font-size': 12,
        'font-family': 'var(--mono)', 'font-weight': 500,
      }, format(v)),
    );
  });

  mount.append(svg);
}

// Change over an ordered domain, so a line.
export function lineChart(mount, { series, xKey, yKey, xLabel, yLabel, xTicks }) {
  mount.innerHTML = '';
  const W = Math.max(420, Math.round(mount.clientWidth || 1080));
  const H = Math.round(Math.min(380, Math.max(260, W * 0.30)));
  const padL = 64;
  const padR = 24;
  const padT = 18;
  const padB = 46;

  const all = series.flatMap((s) => s.points.map((p) => p[yKey]));
  const top = niceMax(Math.max(...all));
  const xs = series[0].points.map((p) => p[xKey]);
  const xMin = Math.min(...xs);
  const xMax = Math.max(...xs);

  const sx = (v) => padL + ((W - padL - padR) * (v - xMin)) / (xMax - xMin || 1);
  const sy = (v) => H - padB - ((H - padT - padB) * v) / top;

  const svg = svgRoot(W, H, `${yLabel} by ${xLabel}`);

  // y gridlines + ticks
  for (let i = 0; i <= 4; i++) {
    const v = (top * i) / 4;
    const y = sy(v);
    svg.append(el('line', { x1: padL, y1: y, x2: W - padR, y2: y, stroke: 'var(--grid)', 'stroke-width': 1 }));
    svg.append(
      el('text', {
        x: padL - 8, y: y + 4, 'text-anchor': 'end',
        fill: 'var(--fg-dim)', 'font-size': 11, 'font-family': 'var(--mono)',
      }, v >= 1000 ? `${Math.round(v / 1000)}k` : String(Math.round(v))),
    );
  }

  // x ticks
  (xTicks || xs).forEach((v) => {
    svg.append(
      el('text', {
        x: sx(v), y: H - padB + 18, 'text-anchor': 'middle',
        fill: 'var(--fg-dim)', 'font-size': 11, 'font-family': 'var(--mono)',
      }, String(v).padStart(2, '0')),
    );
  });
  svg.append(
    el('text', {
      x: (padL + W - padR) / 2, y: H - 4, 'text-anchor': 'middle',
      fill: 'var(--fg-dim)', 'font-size': 11, 'font-family': 'var(--sans)',
    }, xLabel),
  );

  // The shaded band is the measured trough, so it belongs on the chart rather than in a caption.
  if (mount.dataset.band) {
    const band = JSON.parse(mount.dataset.band);
    // Draw contiguous runs, or a band that wraps midnight renders as one wrong rectangle.
    const runs = [];
    band.sort((a, b) => a - b).forEach((h) => {
      const last = runs[runs.length - 1];
      if (last && h === last[last.length - 1] + 1) last.push(h);
      else runs.push([h]);
    });
    runs.forEach((run) => {
      const x0 = sx(run[0]) - (W - padL - padR) / 46;
      const x1 = sx(run[run.length - 1]) + (W - padL - padR) / 46;
      svg.append(
        el('rect', {
          x: Math.max(padL, x0), y: padT,
          width: Math.min(W - padR, x1) - Math.max(padL, x0), height: H - padT - padB,
          fill: 'var(--fg)', opacity: 0.05,
        }),
      );
    });
  }

  series.forEach((s) => {
    const d = s.points.map((p, i) => `${i === 0 ? 'M' : 'L'}${sx(p[xKey])},${sy(p[yKey])}`).join(' ');

    // A soft wash under the line. It gives the two series some weight without adding a second
    // encoding, since the line still carries the value.
    const defs = svg.querySelector('defs') || svg.insertBefore(el('defs'), svg.firstChild);
    const areaId = `a${gradSeq++}`;
    const ag = el('linearGradient', { id: areaId, x1: '0', y1: '0', x2: '0', y2: '1' });
    ag.append(el('stop', { offset: '0%', 'stop-color': s.color, 'stop-opacity': 0.22 }));
    ag.append(el('stop', { offset: '100%', 'stop-color': s.color, 'stop-opacity': 0 }));
    defs.append(ag);

    const area = `${d} L${sx(s.points[s.points.length - 1][xKey])},${H - padB} L${sx(s.points[0][xKey])},${H - padB} Z`;
    svg.append(el('path', { d: area, fill: `url(#${areaId})`, stroke: 'none' }));

    svg.append(el('path', { d, fill: 'none', stroke: s.color, 'stroke-width': 2.5, 'stroke-linejoin': 'round', 'stroke-linecap': 'round' }));

    s.points.forEach((p) => {
      // The 2px surface ring keeps overlapping series separable where they cross.
      const c = el('circle', {
        cx: sx(p[xKey]), cy: sy(p[yKey]), r: 4.5,
        fill: s.color, stroke: 'var(--surface)', 'stroke-width': 2,
      });
      interactive(
        c,
        `<div><b>${s.name}</b></div><div><span class="tip-k">hour</span> ${String(p[xKey]).padStart(2, '0')}:00</div>` +
          `<div><span class="tip-k">${yLabel}</span> ${fmt.int(p[yKey])}</div>`,
        `${s.name}, hour ${p[xKey]}: ${fmt.int(p[yKey])} ${yLabel}`,
      );
      svg.append(c);
    });
  });

  mount.append(svg);
}

// The table view. An accessibility fallback, and honestly the thing a data person reads first.
export function table(mountTable, { columns, rows, numeric = [], rowClass }) {
  mountTable.innerHTML = '';
  const thead = document.createElement('thead');
  const htr = document.createElement('tr');
  columns.forEach((c) => {
    const th = document.createElement('th');
    th.textContent = c.label ?? c;
    if (c.title) th.title = c.title;
    htr.append(th);
  });
  thead.append(htr);

  const tbody = document.createElement('tbody');
  rows.forEach((r) => {
    const tr = document.createElement('tr');
    if (rowClass) {
      const cls = rowClass(r);
      if (cls) tr.className = cls;
    }
    columns.forEach((c) => {
      const key = c.key ?? c;
      const td = document.createElement('td');
      const raw = r[key];
      td.textContent = c.format ? c.format(raw, r) : raw ?? ', ';
      if (numeric.includes(key)) td.className = 'n';
      tr.append(td);
    });
    tbody.append(tr);
  });

  mountTable.append(thead, tbody);
}

export { fmt };
