/*
 * The demo page.
 *
 * No metric is ever typed into this page. Every number a visitor sees is read out of
 * data/bench.json, and site/audit.py fails the build if the HTML hard-codes one.
 */

import * as duckdb from './vendor/duckdb/duckdb-browser.mjs';
import { barChart, lineChart, table, fmt } from './charts.js';
import { PathExplorer } from './graph.js';

const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

// theme
const savedTheme = localStorage.getItem('tarn-theme');
if (savedTheme) document.documentElement.dataset.theme = savedTheme;
else if (window.matchMedia('(prefers-color-scheme: light)').matches) {
  document.documentElement.dataset.theme = 'light';
}

$('#theme-toggle').addEventListener('click', () => {
  const next = document.documentElement.dataset.theme === 'light' ? 'dark' : 'light';
  document.documentElement.dataset.theme = next;
  localStorage.setItem('tarn-theme', next);
  // Charts read CSS variables for colour, but the SVG text and grid colours are baked at draw
  // time, so re-render rather than leave a half-themed chart behind.
  if (window.__tarnRedraw) window.__tarnRedraw();
});

// cursor light
// One rAF-throttled listener for the whole page. Writing the pointer position into CSS custom
// properties lets the gradients do the work on the compositor instead of in JS.
(() => {
  if (window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;

  const glow = $('#glow');
  const root = document.documentElement;
  let x = 0;
  let y = 0;
  let queued = false;

  const paint = () => {
    queued = false;
    root.style.setProperty('--mx', `${x}px`);
    root.style.setProperty('--my', `${y}px`);
  };

  window.addEventListener('pointermove', (e) => {
    x = e.clientX;
    y = e.clientY;
    if (glow) glow.classList.add('on');

    // The card under the pointer gets the position in its OWN coordinates, so its highlight
    // tracks the cursor rather than the page.
    const card = e.target.closest?.('.card');
    if (card) {
      const r = card.getBoundingClientRect();
      card.style.setProperty('--cx', `${e.clientX - r.left}px`);
      card.style.setProperty('--cy', `${e.clientY - r.top}px`);
    }

    if (!queued) {
      queued = true;
      requestAnimationFrame(paint);
    }
  }, { passive: true });

  window.addEventListener('pointerleave', () => glow?.classList.remove('on'));
})();

// bench binding
function dig(obj, path) {
  return path.split('.').reduce((o, k) => (o == null ? o : o[k]), obj);
}

const FORMATTERS = {
  int: (v) => Number(v).toLocaleString('en-US'),
  round: (v) => String(Math.round(Number(v))),
  x: (v) => `${Number(v).toFixed(2)}×`,
  s: (v) => `${Number(v).toFixed(2)}s`,
  ms: (v) => `${Math.round(Number(v)).toLocaleString('en-US')} ms`,
  pct: (v) => `${Number(v).toFixed(1)}%`,
  raw: (v) => String(v),
};

/** Fill every [data-bench="path|format"] straight from the artifacts. */
function bindBench(bench) {
  $$('[data-bench]').forEach((node) => {
    const [path, f = 'raw'] = node.dataset.bench.split('|');
    const value = dig(bench, path);
    node.textContent =
      value === undefined || value === null ? '-' : (FORMATTERS[f] || FORMATTERS.raw)(value);
  });
}

// pipeline diagram
function pipelineDiagram(mount) {
  const stages = [
    ['The raw logs', 'a billion lines'],
    ['Spark', 'clean and summarise'],
    ['The warehouse', 'ready to query'],
    ['Streaming', 'count as they arrive'],
    ['The graph', 'who reaches what'],
    ['This page', 'live in your browser'],
  ];
  const W = 1100;
  const H = 84;
  const boxW = 158;
  const gap = (W - stages.length * boxW) / (stages.length - 1);

  const ns = 'http://www.w3.org/2000/svg';
  const svg = document.createElementNS(ns, 'svg');
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);
  svg.setAttribute('role', 'img');
  svg.setAttribute(
    'aria-label',
    'The pipeline: raw logs, then Spark, then a warehouse, then a streaming job, then a graph ' +
      'database, and finally this page.',
  );

  stages.forEach(([name, sub], i) => {
    const x = i * (boxW + gap);
    const g = document.createElementNS(ns, 'g');

    const rect = document.createElementNS(ns, 'rect');
    rect.setAttribute('x', x);
    rect.setAttribute('y', 16);
    rect.setAttribute('width', boxW);
    rect.setAttribute('height', 52);
    rect.setAttribute('rx', 8);
    rect.setAttribute('fill', 'var(--surface)');
    rect.setAttribute('stroke', i === stages.length - 1 ? 'var(--accent)' : 'var(--border)');
    g.append(rect);

    const t1 = document.createElementNS(ns, 'text');
    t1.setAttribute('x', x + boxW / 2);
    t1.setAttribute('y', 38);
    t1.setAttribute('text-anchor', 'middle');
    t1.setAttribute('font-size', 12);
    t1.setAttribute('font-family', 'var(--sans)');
    t1.setAttribute('font-weight', 500);
    t1.setAttribute('fill', i === stages.length - 1 ? 'var(--accent)' : 'var(--fg)');
    t1.textContent = name;
    g.append(t1);

    const t2 = document.createElementNS(ns, 'text');
    t2.setAttribute('x', x + boxW / 2);
    t2.setAttribute('y', 55);
    t2.setAttribute('text-anchor', 'middle');
    t2.setAttribute('font-size', 10);
    t2.setAttribute('font-family', 'var(--mono)');
    t2.setAttribute('fill', 'var(--fg-dim)');
    t2.textContent = sub;
    g.append(t2);

    if (i < stages.length - 1) {
      const line = document.createElementNS(ns, 'path');
      const x0 = x + boxW + 6;
      const x1 = x + boxW + gap - 6;
      line.setAttribute('d', `M${x0},42 L${x1},42 M${x1 - 5},38 L${x1},42 L${x1 - 5},46`);
      line.setAttribute('stroke', 'var(--axis)');
      line.setAttribute('stroke-width', 1.4);
      line.setAttribute('fill', 'none');
      g.append(line);
    }
    svg.append(g);
  });

  mount.innerHTML = '';
  mount.append(svg);
}

// DuckDB-WASM
let db = null;
let conn = null;

async function initDuckDB(bench, status) {
  status.textContent = 'Starting the database…';

  // The worker is loaded from a blob so its URL resolves under a GitHub Pages sub-path rather
  // than assuming the domain root.
  const workerUrl = new URL('./vendor/duckdb/duckdb-browser-eh.worker.js', import.meta.url);
  const wasmUrl = new URL('./vendor/duckdb/duckdb-eh.wasm', import.meta.url);

  const worker = new Worker(
    URL.createObjectURL(
      new Blob([`importScripts("${workerUrl}");`], { type: 'text/javascript' }),
    ),
  );
  const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
  db = new duckdb.AsyncDuckDB(logger, worker);
  await db.instantiate(wasmUrl.toString());

  conn = await db.connect();

  status.textContent = 'Downloading the results…';

  // Registered over HTTP, so DuckDB range-requests only the column chunks a query touches.
  const extracts = bench.extracts || {};
  const names = [];
  for (const [name, meta] of Object.entries(extracts)) {
    const url = new URL(`./${meta.file}`, import.meta.url).toString();
    await db.registerFileURL(`${name}.parquet`, url, duckdb.DuckDBDataProtocol.HTTP, false);
    await conn.query(
      `create or replace view ${name} as select * from read_parquet('${name}.parquet')`,
    );
    names.push(`${name} (${(meta.rows ?? 0).toLocaleString('en-US')} rows, ${meta.mb} MB)`);
  }

  $('#wb-schema').textContent =
    'Tables your browser now holds, and can query:\n\n' +
    names.map((n) => `  ${n}`).join('\n') +
    '\n\nThese are the real results the pipeline produced. Nothing was simplified for the demo.' +
    '\n\nThe rollup table is complete: every person, every day, machine accounts included. It' +
    '\nwould be smaller and quicker to leave the machine accounts out, but the scores in the' +
    '\nlast question are fractions of that whole group. Dropping them would quietly flatter the' +
    '\nresults, and this page would be lying in the one place it claims to be live.';

  return conn;
}

// workbench
const PRESETS = [
  {
    id: 'q1_fanout_week_over_week',
    label: 'Who suddenly touched far more machines?',
    finding:
      'An account that normally uses three machines and suddenly uses forty is worth a look. ' +
      'Each person is compared against <strong>their own last week</strong>, not against everyone ' +
      'else, because a backup service legitimately touches hundreds of machines every single day.',
    sql: `-- Q1: destination fan-out, week over week (lateral-movement precursor)
with weekly as (
  select r.src_user, t.week_index,
         max(r.distinct_dst_computers) as peak_fanout,
         sum(r.new_dst_computers)      as new_destinations,
         max(r.is_redteam_day::int)::boolean as had_redteam
  from rollup r
  join dim_time t on r.event_date = t.calendar_date
  join dim_identity i on r.src_user = i.identity_name
  where not i.is_machine_account
  group by 1, 2
)
select src_user as identity, week_index,
       lag(peak_fanout) over (partition by src_user order by week_index) as prev_week,
       peak_fanout,
       peak_fanout - lag(peak_fanout) over (partition by src_user order by week_index) as delta,
       new_destinations, had_redteam
from weekly
qualify prev_week is not null and peak_fanout > prev_week
order by delta desc
limit 25;`,
  },
  {
    id: 'q2_off_hours_vs_baseline',
    label: 'Who worked at hours they never work?',
    finding:
      'People active during the quiet hours who are basically never active then. The quiet hours ' +
      'were <strong>measured from the data</strong>, not assumed. This is the one that caught ' +
      'nothing at all.',
    sql: `-- Q2: off-hours share vs the identity's own baseline
select r.src_user as identity, r.event_date, r.auth_count, r.off_hours_events,
       round(r.off_hours_share, 4)                as off_hours_today,
       round(r.off_hours_share_baseline_mean, 4)  as their_baseline,
       round(r.off_hours_share - r.off_hours_share_baseline_mean, 4) as excess,
       r.is_redteam_day
from rollup r
join dim_identity i on r.src_user = i.identity_name
where not i.is_machine_account
  and r.auth_count >= 10
  and r.baseline_days_available >= 3
  and r.off_hours_share_baseline_mean < 0.05   -- they never work nights...
  and r.off_hours_share > 0.25                 -- ...and tonight they did
order by excess desc
limit 25;`,
  },
  {
    id: 'q3_new_access_path_rate',
    label: 'Who reached machines they never had before?',
    finding:
      'Every time somebody logs into a machine for the first time, a new route through the network ' +
      'exists that did not exist yesterday. This was the <strong>best of the four</strong> warning ' +
      'signs, and it still missed most of the attack.',
    sql: `-- Q3: first-time (identity -> computer) edges per day
select event_date,
       sum(new_dst_computers)     as new_edges,
       count(distinct src_user)   as active_identities,
       sum(is_redteam_day::int)   as redteam_identities_active
from rollup
group by event_date
order by event_date;`,
  },
  {
    id: 'q4_failure_spike_zscore',
    label: 'Whose password suddenly started failing?',
    finding:
      'A burst of failed logins can mean somebody is guessing passwords. Each person is measured ' +
      'against their own history, and <strong>today is left out of that history</strong>, or the ' +
      'spike would quietly raise the very average it is being compared against.',
    sql: `-- Q4: failure-ratio spike, z-score vs the identity's own history
select src_user as identity, event_date, auth_count, failure_count,
       round(failure_ratio, 4)          as failure_ratio_today,
       round(failure_ratio_zscore, 2)   as z_score,
       baseline_days_available, is_redteam_day
from rollup
where failure_ratio_zscore is not null
  and failure_ratio_zscore > 3
  and failure_count >= 5
order by failure_ratio_zscore desc
limit 25;`,
  },
  {
    id: 'q5_redteam_enrichment',
    label: 'So would any of it have worked?',
    finding:
      'The honest one. Each warning sign is scored against the answer key. How many attack days it ' +
      'caught, how many it <strong>missed</strong>, and how many innocent people it would have ' +
      'accused along the way. The numbers are not flattering, and that is the point.',
    sql: `-- Q5: the four analytics scored as detectors against LANL's ground truth.
-- recall = of the identity-days that really were compromised, how many did we flag?
-- alerts = how many identity-days a human would have to triage.
with scored as (
  select is_redteam_day,
    (fanout_zscore is not null and fanout_zscore > 3)                as d1_fanout,
    (baseline_days_available >= 3 and off_hours_share_baseline_mean < 0.05
       and off_hours_share > 0.25 and auth_count >= 10)              as d2_offhours,
    (new_dst_computers >= 5)                                         as d3_newpaths,
    (failure_ratio_zscore is not null and failure_ratio_zscore > 3
       and failure_count >= 5)                                       as d4_failures
  from rollup
),
flagged as (
  select *, (d1_fanout or d2_offhours or d3_newpaths or d4_failures) as d_any from scored
),
t as (select count(*) all_days, sum(is_redteam_day::int) rt_days from flagged),
d as (
  select 'Q1 fan-out spike' as detector, sum(d1_fanout::int) alerts,
         sum((d1_fanout and is_redteam_day)::int) caught from flagged
  union all select 'Q2 off-hours', sum(d2_offhours::int),
         sum((d2_offhours and is_redteam_day)::int) from flagged
  union all select 'Q3 new access paths', sum(d3_newpaths::int),
         sum((d3_newpaths and is_redteam_day)::int) from flagged
  union all select 'Q4 failure spike', sum(d4_failures::int),
         sum((d4_failures and is_redteam_day)::int) from flagged
  union all select 'ANY of Q1-Q4', sum(d_any::int),
         sum((d_any and is_redteam_day)::int) from flagged
)
select d.detector,
       t.rt_days                                        as redteam_days,
       d.caught                                         as caught,
       t.rt_days - d.caught                             as missed,
       round(100.0 * d.caught / nullif(t.rt_days,0), 1) as recall_pct,
       d.alerts                                         as alerts_to_triage,
       round(100.0 * d.caught / nullif(d.alerts,0), 3)  as precision_pct,
       round((d.caught*1.0/nullif(d.alerts,0)) / nullif(t.rt_days*1.0/t.all_days,0), 1)
                                                        as lift_over_random
from d cross join t
order by recall_pct desc nulls last;`,
  },
  {
    id: 'free',
    label: '✎ ask your own',
    finding:
      'Over to you. The tables are <code>rollup</code> (a row per person per day), ' +
      '<code>dim_identity</code> (people), <code>dim_computer</code> (machines), ' +
      '<code>dim_time</code> (days) and <code>redteam</code> (the answer key).',
    sql: `-- Anything you like. This is a real DuckDB running in your browser.
-- Views: rollup, dim_identity, dim_computer, dim_time, redteam

select i.identity_name, i.total_auth_events, i.lifetime_distinct_destinations,
       i.active_days, i.is_compromised
from dim_identity i
where i.is_compromised
order by i.lifetime_distinct_destinations desc
limit 20;`,
  },
];

function renderResult(mount, result) {
  const cols = result.schema.fields.map((f) => f.name);
  const rows = result.toArray().map((r) => {
    const o = r.toJSON();
    const out = {};
    for (const c of cols) {
      let v = o[c];
      if (typeof v === 'bigint') v = Number(v);
      if (v instanceof Date) v = v.toISOString().slice(0, 10);
      if (typeof v === 'number' && !Number.isInteger(v)) v = Number(v.toFixed(4));
      out[c] = v === null || v === undefined ? '-' : v;
    }
    return out;
  });

  const numeric = cols.filter((c) =>
    rows.some((r) => typeof r[c] === 'number'),
  );

  table(mount, {
    columns: cols,
    rows,
    numeric,
    // Ground truth gets a channel of its own, so the attacker's rows are findable without
    // reading every cell.
    rowClass: (r) =>
      r.is_redteam_day === true || r.had_redteam === true || r.is_compromised === true
        ? 'is-redteam'
        : '',
  });
  return rows.length;
}

async function runQuery(sql, statusEl, resultEl) {
  if (!conn) return;
  statusEl.className = 'wb-status';
  statusEl.textContent = 'Working…';
  const t0 = performance.now();
  try {
    const res = await conn.query(sql);
    const ms = performance.now() - t0;
    const n = renderResult(resultEl, res);
    resultEl.hidden = false;
    statusEl.className = 'wb-status ok';
    statusEl.textContent =
      `${n.toLocaleString('en-US')} row${n === 1 ? '' : 's'} in ${ms.toFixed(0)} ms, on your machine`;
  } catch (err) {
    statusEl.className = 'wb-status error';
    statusEl.textContent = String(err.message || err);
    resultEl.hidden = true;
  }
}

// charts
function drawCharts(bench) {
  const opt = bench.spark_opt;
  const lag = bench.streaming_lag;
  const diurnal = bench.diurnal;
  const graph = bench.graph_stats;

  // Spark optimization
  if (opt) {
    const order = ['baseline', 'broadcast_only', 'dedup_only', 'both'];
    const nice = {
      baseline: 'Baseline',
      broadcast_only: '+ broadcast join',
      dedup_only: '+ two-stage distinct',
      both: 'Both',
    };
    // Colour follows the entity, not the rank. Re-running the benchmark must not repaint the
    // bars just because the order changed.
    const color = {
      baseline: 'var(--series-1)',
      broadcast_only: 'var(--series-3)',
      dedup_only: 'var(--series-2)',
      both: 'var(--series-2)',
    };
    const rows = order
      .filter((k) => opt.variants?.[k])
      .map((k) => ({
        label: nice[k],
        seconds: opt.variants[k].median_seconds,
        color: color[k],
        tip: `runs: ${opt.variants[k].runs_seconds.map((s) => s.toFixed(1)).join(', ')}s<br>${
          opt.variants[k].label
        }`,
      }));

    barChart($('#chart-opt'), {
      rows,
      valueKey: 'seconds',
      labelKey: 'label',
      colorKey: 'color',
      format: fmt.s,
      note: 'Spark rollup runtime by optimization variant',
    });

    $('#chart-opt-cap').innerHTML =
      `Run on ${Number(opt.slice.rows).toLocaleString('en-US')} logins. Each version ran ` +
      `${opt.method.runs_per_variant} times and the middle result is shown. All four produced ` +
      `identical output.`;

    table($('#opt-table'), {
      columns: [
        { key: 'variant', label: 'version' },
        { key: 'median', label: 'time taken' },
        { key: 'speedup', label: 'speed-up' },
        { key: 'rows', label: 'rows out' },
        { key: 'checksum', label: 'fingerprint' },
      ],
      numeric: ['median', 'speedup', 'rows', 'checksum'],
      rows: order
        .filter((k) => opt.variants?.[k])
        .map((k) => ({
          variant: nice[k],
          median: `${opt.variants[k].median_seconds.toFixed(2)}s`,
          speedup: `${opt.variants[k].speedup_vs_baseline.toFixed(2)}×`,
          rows: Number(opt.variants[k].checksum.rows).toLocaleString('en-US'),
          checksum: String(opt.variants[k].checksum.hash).slice(0, 10),
        })),
    });

    $('#plan-baseline').textContent = opt.variants?.baseline?.physical_plan ?? '-';
    $('#plan-optimized').textContent = opt.variants?.both?.physical_plan ?? '-';

    const e = opt.environment;
    $('#prov-opt').innerHTML =
      `<b>Where these numbers come from.</b> One laptop: ${e.cpu}, ${e.cpu_count} cores, ` +
      `${e.memory_total_gb} GB of memory, running Spark ${e.spark} inside Docker.<br>` +
      `<b>What was measured.</b> ${Number(opt.slice.rows).toLocaleString('en-US')} logins across ` +
      `${opt.slice.dates} days. Every version saw exactly the same data.<br>` +
      `<b>How.</b> Each version ran ${opt.method.runs_per_variant} times and the middle result is ` +
      `reported. Every individual run is written down in the project. ` +
      `Measured ${opt.measured_at.slice(0, 10)}.`;
  }

  // streaming lag
  if (lag) {
    const rows = [
      { label: 'p50', ms: lag.lag_ms.p50, color: 'var(--series-2)' },
      { label: 'p90', ms: lag.lag_ms.p90, color: 'var(--series-1)' },
      { label: 'p95', ms: lag.lag_ms.p95, color: 'var(--series-3)' },
      { label: 'p99', ms: lag.lag_ms.p99, color: 'var(--series-4)' },
      { label: 'max', ms: lag.lag_ms.max, color: 'var(--series-4)' },
    ];
    barChart($('#chart-lag'), {
      rows,
      valueKey: 'ms',
      labelKey: 'label',
      colorKey: 'color',
      format: fmt.ms,
      note: 'End-to-end streaming lag percentiles',
    });
    $('#chart-lag-cap').textContent =
      `Measured across ${Number(lag.lag_ms.samples).toLocaleString('en-US')} minutes of activity. ` +
      `The clock starts when a login is sent and stops once its minute has been counted and saved, ` +
      `so the job's deliberate wait for late arrivals is included in every bar.`;

    table($('#stream-table'), {
      columns: [{ key: 'k', label: 'metric' }, { key: 'v', label: 'value' }],
      rows: [
        { k: 'logins processed', v: Number(lag.throughput.events_aggregated).toLocaleString('en-US') },
        { k: 'minutes of activity counted', v: Number(lag.throughput.windows_committed).toLocaleString('en-US') },
        { k: 'kept up at', v: `${Number(lag.throughput.events_per_second_sustained).toLocaleString('en-US')} logins a second` },
        { k: 'how long it ran', v: `${lag.throughput.run_seconds} seconds` },
        { k: 'counted in blocks of', v: lag.config.window },
        { k: 'waits this long for stragglers', v: lag.config.watermark },
        { k: 'checks for new data every', v: lag.config.trigger },
        { k: 'message queue', v: lag.config.broker },
      ],
    });

    const acc = lag.event_time_acceleration || {};
    $('#prov-stream').innerHTML =
      `<b>Something that would be easy to misread.</b> The 58 days of history were replayed at about ` +
      `${acc.acceleration_factor}x normal speed, so the job's wait for late arrivals costs far less ` +
      `real time here than it would live. The delay above cannot be understood without that, so it ` +
      `is stated.<br>` +
      `<b>Where it ran.</b> ${lag.environment.cpu}, on a laptop, with everything on the same machine. ` +
      `Measured ${lag.measured_at.slice(0, 10)}.`;
  }

  // diurnal
  if (diurnal) {
    const mount = $('#chart-diurnal');
    mount.dataset.band = JSON.stringify(diurnal.off_hours.band ?? []);
    lineChart(mount, {
      series: [
        {
          name: 'human accounts',
          color: 'var(--series-1)',
          points: diurnal.histogram.map((h) => ({ hour: h.hour, events: h.human_events })),
        },
        {
          name: 'machine accounts',
          color: 'var(--series-3)',
          points: diurnal.histogram.map((h) => ({ hour: h.hour, events: h.machine_events })),
        },
      ],
      xKey: 'hour',
      yKey: 'events',
      xLabel: 'hour of day (offset from start of collection)',
      yLabel: 'events',
      xTicks: [0, 3, 6, 9, 12, 15, 18, 21, 23],
    });

    const r = diurnal.peak_to_trough_ratio;
    $('#diurnal-sub').innerHTML =
      `<span class="legend" style="margin:0">
        <span class="legend-item"><span class="legend-swatch" style="background:var(--series-1)"></span>people, busiest hour is ${r.human_accounts}× the quietest</span>
        <span class="legend-item"><span class="legend-swatch" style="background:var(--series-3)"></span>machines, almost flat at ${r.machine_accounts}×</span>
        <span class="legend-item"><span class="legend-swatch" style="background:var(--fg);opacity:.18"></span>the quiet hours, worked out from this</span>
      </span>`;

    const band = diurnal.off_hours.band ?? [];
    $('#chart-diurnal-cap').innerHTML = band.length
      ? `The quiet hours come out as ${band[0]}:00 to ${(band[band.length - 1] + 1) % 24}:00, and that ` +
        `range was calculated from this data rather than assumed. Notice what happens if you do not ` +
        `separate people from machines: the combined line only varies by ${r.all_accounts}× from its ` +
        `busiest hour to its quietest, and the daily rhythm vanishes.`
      : 'No quiet hours could be found here. The line is too flat, so this signal cannot be used.';
  }

  // blast radius + Q5
  if (graph?.blast_radius) {
    const b = graph.blast_radius;

    // TWO charts, not one. 1-hop is ~100 hosts and 3-hop is ~14,000, so on a shared axis the
    // 1-hop bars collapse into invisible slivers, which hides the only measure that actually
    // discriminates. Different scales get different axes.
    barChart($('#chart-blast'), {
      rows: [
        {
          label: 'compromised', v: b.compromised_mean_hosts_1_hop, color: 'var(--series-4)',
          tip: b.at_1_hop?.verdict,
        },
        {
          label: 'ordinary', v: b.benign_control_mean_hosts_1_hop, color: 'var(--series-1)',
          tip: 'The control. Without it the number above means nothing.',
        },
      ],
      valueKey: 'v', labelKey: 'label', colorKey: 'color',
      format: (v) => Number(v).toLocaleString('en-US'),
      note: 'Hosts reachable at 1 hop',
    });

    barChart($('#chart-blast3'), {
      rows: [
        {
          label: 'compromised', v: b.compromised_mean_hosts_3_hops, color: 'var(--series-4)',
          tip: b.at_3_hops?.verdict,
        },
        {
          label: 'ordinary', v: b.benign_control_mean_hosts_3_hops, color: 'var(--series-1)',
          tip: b.at_3_hops?.verdict,
        },
      ],
      valueKey: 'v', labelKey: 'label', colorKey: 'color',
      format: (v) => Number(v).toLocaleString('en-US'),
      note: 'Hosts reachable at 3 hops',
      max: b.total_hosts_in_graph,
    });

    $('#chart-blast-cap').innerHTML =
      `<strong>Compare the two charts.</strong> The stolen accounts log into about ` +
      `${b.at_1_hop?.ratio}× as many machines as an ordinary person, and that is still only ` +
      `${b.at_1_hop?.pct_of_all_hosts_covered}% of the network. But give them three steps and they ` +
      `reach <em>nearly every machine there is</em>. So does everybody else. The second chart looks ` +
      `alarming and means nothing, because an ordinary employee scores the same. Even the first ` +
      `chart is not clean, because the attacker deliberately picked powerful accounts to steal. The ` +
      `finding that survives is the table below, not this one.`;
  }

  if (graph?.choke_points_top) {
    table($('#choke-table'), {
      columns: [
        { key: 'host', label: 'machine' },
        { key: 'identities', label: 'people who log into it',
          format: (v) => Number(v).toLocaleString('en-US') },
        { key: 'was_pivot', label: 'attack came from here', format: (v) => (v ? 'yes' : '-') },
        { key: 'was_target', label: 'attack reached it', format: (v) => (v ? 'yes' : '-') },
      ],
      numeric: ['identities'],
      rows: graph.choke_points_top,
      rowClass: (r) => (r.was_pivot || r.was_target ? 'is-redteam' : ''),
    });
  }

  const q5 = bench.queries?.q5_redteam_enrichment;
  if (q5) {
    const rows = q5.rows.filter((r) => r.detector && r.recall_pct !== '');
    barChart($('#chart-q5'), {
      rows: rows.map((r) => ({
        label: r.detector.replace(/\s*\(.*\)/, ''),
        recall: Number(r.recall_pct),
        color: r.detector.startsWith('ANY') ? 'var(--series-2)' : 'var(--series-1)',
        tip: `${r.redteam_days_caught} of ${r.redteam_days_total} compromised identity-days caught<br>` +
          `${Number(r.alerts_raised).toLocaleString('en-US')} alerts to triage · ` +
          `${r.lift_over_random || ','}× lift over random`,
      })),
      valueKey: 'recall',
      labelKey: 'label',
      colorKey: 'color',
      format: (v) => `${Number(v).toFixed(1)}%`,
      note: 'Detector recall against the labelled red-team events',
      max: 100,
    });
    $('#chart-q5-cap').textContent =
      'These bars only show how much of the attack each sign caught, which flatters them. The ' +
      'table below shows the other half: how many innocent people each one would have accused. A ' +
      'warning sign that catches a third of the attack and flags sixty thousand people is not a ' +
      'warning sign, it is a full-time job for whoever has to read it.';

    // The CSV hands everything back as strings, so "62.0" needs rendering as a count.
    const asInt = (v) => (v === '' || v == null ? '-' : Number(v).toLocaleString('en-US'));
    const asNum = (v, dp) => (v === '' || v == null ? '-' : Number(v).toFixed(dp));

    table($('#q5-table'), {
      columns: [
        { key: 'detector', label: 'warning sign' },
        { key: 'redteam_days_caught', label: 'caught', format: asInt },
        { key: 'redteam_days_MISSED', label: 'missed', format: asInt },
        { key: 'recall_pct', label: 'caught %', format: (v) => asNum(v, 1) },
        { key: 'alerts_raised', label: 'people it would flag', format: asInt },
        { key: 'precision_pct', label: 'of those, actually guilty %', format: (v) => asNum(v, 3) },
        { key: 'lift_over_random', label: 'better than guessing',
          format: (v) => (v === '' || v == null ? '-' : `${Number(v).toFixed(1)}×`) },
      ],
      numeric: [
        'redteam_days_caught', 'redteam_days_MISSED', 'recall_pct',
        'alerts_raised', 'precision_pct', 'lift_over_random',
      ],
      rows: q5.rows,
      // The detector that caught nothing is the most important row on the page.
      rowClass: (r) => (Number(r.recall_pct) === 0 ? 'is-redteam' : ''),
    });
  }
}

// boot
async function main() {
  const status = $('#wb-status');

  const bench = await (await fetch('data/bench.json')).json();
  window.__bench = bench;

  bindBench(bench);
  pipelineDiagram($('#pipeline'));
  drawCharts(bench);
  window.__tarnRedraw = () => {
    pipelineDiagram($('#pipeline'));
    drawCharts(bench);
  };

  /* footer provenance, from the artifacts */
  const e = bench.spark_opt?.environment;
  const when = bench.spark_opt?.measured_at?.slice(0, 10);
  $('#prov-recorded').innerHTML =
    `Everything else on this page. The speed test, the streaming delays and the graph were measured ` +
    `on <b>${e?.cpu ?? 'a laptop'}</b> on <b>${when ?? '-'}</b>, and what you see is played back from ` +
    `those recordings. The routes through the graph are genuine answers from a graph database, worked ` +
    `out ahead of time, because a browser cannot run that kind of query.`;

  $('#foot-note').innerHTML =
    `Tarn · built by <a href="https://github.com/samad-zeeshan">samad-zeeshan</a> · ` +
    `<a href="https://github.com/samad-zeeshan/tarn">the code</a> · ` +
    `data from A. D. Kent, <em>Comprehensive, Multi-Source Cyber-Security Events</em>, ` +
    `Los Alamos National Laboratory, 2015, free for anyone to use.`;

  /* graph explorer */
  try {
    const [graph, paths] = await Promise.all([
      fetch('data/graph.json').then((r) => r.json()),
      fetch('data/paths.json').then((r) => r.json()),
    ]);

    // paths.json is an envelope. The explorer wants the array inside it.
    const explorer = new PathExplorer($('#graph-canvas'), graph, paths.paths);
    const users = [...new Set(paths.paths.flatMap((p) => [p.from_user, p.to_user]))].sort();
    const from = $('#g-from');
    const to = $('#g-to');
    users.forEach((u) => {
      from.append(new Option(u, u));
      to.append(new Option(u, u));
    });

    const readout = $('#path-readout');
    const trace = () => {
      const p = explorer.setPath(from.value, to.value);
      if (!p) {
        readout.innerHTML =
          '<span style="color:var(--fg-dim)">These two are not connected within six steps of ' +
          'each other.</span>';
        return;
      }
      // hop_count counts every node change, and the chain alternates person, machine, person.
      // A reader thinks in people, so report the number of people-to-people steps.
      const steps = Math.max(1, Math.round(p.hop_count / 2));
      const hops = p.hops
        .map((h, i) => {
          const kind = p.kinds?.[i] === 'Computer' ? 'comp' : 'user';
          const what = kind === 'comp' ? 'machine' : 'person';
          return `<span class="hop ${kind}" title="${what} ${h}">${h}</span>`;
        })
        .join('<span class="hop-arrow">→</span>');
      readout.innerHTML =
        `<div style="color:var(--fg-muted)">${steps} step${steps === 1 ? '' : 's'} apart. ` +
        `${p.traverses_redteam
            ? '<span style="color:var(--critical)">This route uses a link the attacker actually used.</span>'
            : 'The attacker did not use this particular route.'}` +
        `</div>` +
        `<div class="path-hops">${hops}</div>`;
    };

    // Default to a pair that has a path. Alphabetical order lands on two identities the
    // exporter never queried, so the panel opens on "no path found", which reads like the
    // feature is broken.
    if (paths.paths.length) {
      from.value = paths.paths[0].from_user;
      to.value = paths.paths[0].to_user;
      trace();
    }
    from.addEventListener('change', trace);
    to.addEventListener('change', trace);
    $('#g-redteam').addEventListener('change', (ev) => explorer.setRedteam(ev.target.checked));
    $('#g-labels').addEventListener('change', (ev) => explorer.setLabels(ev.target.checked));
  } catch (err) {
    $('#graph-canvas').innerHTML =
      `<p style="padding:var(--s5);color:var(--fg-dim)">Graph export not found, run ` +
      `<code>make graph</code>.</p>`;
    console.warn('graph export missing', err);
  }

  /* workbench presets */
  const presetBar = $('#wb-presets');
  const editor = $('#wb-editor');
  const finding = $('#wb-finding');
  const result = $('#wb-result');
  let active = PRESETS[0];

  const select = (p) => {
    active = p;
    editor.value = p.sql;
    finding.innerHTML = p.finding;
    finding.hidden = false;
    $$('.preset').forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.id === p.id)));
  };

  PRESETS.forEach((p) => {
    const b = document.createElement('button');
    b.className = 'preset';
    b.dataset.id = p.id;
    b.textContent = p.label;
    b.setAttribute('aria-pressed', 'false');
    b.addEventListener('click', () => {
      select(p);
      if (conn) runQuery(editor.value, status, result);
    });
    presetBar.append(b);
  });
  select(PRESETS[0]);

  $('#wb-reset').addEventListener('click', () => select(active));
  $('#wb-run').addEventListener('click', () => runQuery(editor.value, status, result));
  editor.addEventListener('keydown', (ev) => {
    if ((ev.metaKey || ev.ctrlKey) && ev.key === 'Enter') {
      ev.preventDefault();
      runQuery(editor.value, status, result);
    }
  });

  // DuckDB last. The page is fully readable before the wasm lands.
  try {
    await initDuckDB(bench, status);
    $('#wb-run').disabled = false;
    status.className = 'wb-status ok';
    status.textContent = 'DuckDB ready, press Run (or ⌘/Ctrl + Enter)';
    await runQuery(editor.value, status, result);
  } catch (err) {
    status.className = 'wb-status error';
    status.textContent = `DuckDB failed to load: ${err.message ?? err}`;
    console.error(err);
  }
}

main();
