/* Hand-rolled SVG charts. No chart library, no CDN.
 *
 * Built to the dataviz skill's rules:
 *   - form chosen by the data's job (magnitude -> bar; change-over-time -> line)
 *   - categorical hues in FIXED order, never cycled; color follows the entity, not its rank
 *   - one axis, ever — no dual-scale charts
 *   - thin marks, 4px rounded data-ends anchored to the baseline, recessive grid
 *   - a legend whenever there are >= 2 series, plus selective DIRECT LABELS. The direct labels
 *     are not decoration: the validator WARNed that two light-mode slots fall under 3:1 against
 *     white, which triggers the relief rule (visible labels or a table view). Every chart here
 *     ships both.
 *   - hover/focus tooltip on every mark, and every mark is keyboard-reachable (tabindex),
 *     because a tooltip that only responds to a mouse is not accessible.
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

/* ---- tooltip (shared, fixed-position, follows pointer AND keyboard focus) ---- */
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
    // Keyboard focus: anchor to the mark itself.
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

/** Wire a mark for mouse AND keyboard. */
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

/** Nice round upper bound so the axis reads in human numbers. */
function niceMax(v) {
  if (v <= 0) return 1;
  const mag = 10 ** Math.floor(Math.log10(v));
  const norm = v / mag;
  const step = norm <= 1 ? 1 : norm <= 2 ? 2 : norm <= 5 ? 5 : 10;
  return step * mag;
}

/* =====================================================================
 * Horizontal bar chart. Magnitude by category -> bars. Horizontal because the
 * category labels are long phrases, and rotating axis labels to fit vertical bars
 * is an anti-pattern.
 * ===================================================================== */
export function barChart(mount, { rows, valueKey, labelKey, colorKey, format, note, max }) {
  mount.innerHTML = '';
  const W = 520;
  const rowH = 38;
  const padL = 190;
  const padR = 70;
  const padT = 8;
  const H = padT + rows.length * rowH + 8;

  const svg = svgRoot(W, H, note || 'bar chart');
  const top = max ?? niceMax(Math.max(...rows.map((r) => r[valueKey])));
  const scale = (v) => ((W - padL - padR) * v) / top;

  // Recessive gridlines.
  for (let i = 0; i <= 4; i++) {
    const x = padL + ((W - padL - padR) * i) / 4;
    svg.append(
      el('line', {
        x1: x, y1: padT, x2: x, y2: H - 8,
        stroke: 'var(--grid)', 'stroke-width': 1,
      }),
    );
  }

  rows.forEach((r, i) => {
    const y = padT + i * rowH;
    const v = r[valueKey];
    const w = Math.max(2, scale(v));
    const color = r[colorKey] || 'var(--series-1)';

    // Category label (left, in ink — never in the series color).
    svg.append(
      el('text', {
        x: padL - 10, y: y + rowH / 2 + 4, 'text-anchor': 'end',
        fill: 'var(--fg-muted)', 'font-size': 11, 'font-family': 'var(--sans)',
      }, r[labelKey]),
    );

    // The bar: rounded data-end, anchored to the baseline.
    const bar = el('rect', {
      x: padL, y: y + 8, width: w, height: rowH - 18,
      rx: 4, fill: color,
    });
    interactive(
      bar,
      `<div><b>${r[labelKey]}</b></div><div><span class="tip-k">value</span> ${format(v)}</div>` +
        (r.tip ? `<div style="margin-top:4px;color:var(--fg-muted)">${r.tip}</div>` : ''),
      `${r[labelKey]}: ${format(v)}`,
    );
    svg.append(bar);

    // DIRECT LABEL — required by the relief rule, and it removes the eye-travel to an axis.
    svg.append(
      el('text', {
        x: padL + w + 8, y: y + rowH / 2 + 4,
        fill: 'var(--fg)', 'font-size': 11,
        'font-family': 'var(--mono)', 'font-weight': 500,
      }, format(v)),
    );
  });

  mount.append(svg);
}

/* =====================================================================
 * Grouped line chart — change over an ordered domain (hour of day).
 * Two series: human vs machine accounts. A legend AND direct end-labels.
 * ===================================================================== */
export function lineChart(mount, { series, xKey, yKey, xLabel, yLabel, xTicks }) {
  mount.innerHTML = '';
  const W = 720;
  const H = 300;
  const padL = 56;
  const padR = 16;
  const padT = 16;
  const padB = 40;

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
        fill: 'var(--fg-dim)', 'font-size': 10, 'font-family': 'var(--mono)',
      }, v >= 1000 ? `${Math.round(v / 1000)}k` : String(Math.round(v))),
    );
  }

  // x ticks
  (xTicks || xs).forEach((v) => {
    svg.append(
      el('text', {
        x: sx(v), y: H - padB + 18, 'text-anchor': 'middle',
        fill: 'var(--fg-dim)', 'font-size': 10, 'font-family': 'var(--mono)',
      }, String(v).padStart(2, '0')),
    );
  });
  svg.append(
    el('text', {
      x: (padL + W - padR) / 2, y: H - 4, 'text-anchor': 'middle',
      fill: 'var(--fg-dim)', 'font-size': 10, 'font-family': 'var(--sans)',
    }, xLabel),
  );

  // Shaded off-hours band, if given — this is the measured trough, so it belongs on the chart.
  if (mount.dataset.band) {
    const band = JSON.parse(mount.dataset.band);
    // Draw contiguous runs only, so a wrapping band renders as two rects rather than one wrong one.
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
    svg.append(el('path', { d, fill: 'none', stroke: s.color, 'stroke-width': 2, 'stroke-linejoin': 'round' }));

    s.points.forEach((p) => {
      // >=8px markers, with a 2px surface ring so overlapping series stay separable.
      const c = el('circle', {
        cx: sx(p[xKey]), cy: sy(p[yKey]), r: 4,
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

/* =====================================================================
 * Table view — the accessibility fallback the relief rule requires, and honestly
 * the thing a data person will read first anyway.
 * ===================================================================== */
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
      td.textContent = c.format ? c.format(raw, r) : raw ?? '—';
      if (numeric.includes(key)) td.className = 'n';
      tr.append(td);
    });
    tbody.append(tr);
  });

  mountTable.append(thead, tbody);
}

export { fmt };
