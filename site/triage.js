/*
 * The detector comparison, two days of alerts, one explained alert with its subgraph, and the agent's recorded verdicts.
 *
 * Everything here is read from data/triage.json, built from eval/results and the committed agent
 * runs. Nothing is computed live.
 */

const $ = (sel) => document.querySelector(sel);
const int = (n) => Number(n).toLocaleString('en-US');
// The recorded explanations say "about 131072s earlier", which is what the agent read. The page
// only rewrites it into hours or days for people.
const human = (text) => String(text).replace(/about (\d+)s earlier/, (_, n) => {
  const s = Number(n);
  if (s < 3600) return `about ${Math.round(s / 60)} minutes earlier`;
  if (s < 86400) return `about ${Math.round(s / 3600)} hours earlier`;
  return `about ${Math.round(s / 86400)} days earlier`;
});
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);
const REDUCED = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

function feedTable(day, budget, rows = 12) {
  const real = day.alerts.filter((a) => a.is_attack).length;
  const head = `<p class="tri-line">Day ${day.day}. The detector raised its ${int(day.alerts.length)} alerts.
    ${day.attack_events_that_day ? `${int(day.attack_events_that_day)} attack logins happened that day, and
    ${int(real)} of them made the top ${int(budget)}.` : 'No attack happened that day, so every alert is a false alarm.'}
    ${rows < day.alerts.length ? `The first ${int(rows)} are shown, attacks first.` : ''}</p>`;
  // Real attacks first, then the top of the list, so the one that matters is never cut off.
  const shown = [...day.alerts.filter((a) => a.is_attack), ...day.alerts.filter((a) => !a.is_attack)]
    .slice(0, rows);
  const body = shown.map((a) => `<tr class="${a.is_attack ? 'is-attack' : ''}">
      <td>${esc(a.account)}</td><td class="mono">${esc(a.source)} to ${esc(a.destination)}</td>
      <td class="num">${a.score.toFixed(1)}</td><td>${esc(human(a.top_reason))}</td>
      <td>${a.is_attack ? '<span class="tag-attack">attack</span>' : '<span class="tag-fa">false alarm</span>'}</td></tr>`).join('');
  return `${head}<div class="table-scroll"><table class="tri-table"><thead><tr><th>account</th>
    <th>login</th><th class="num">score</th><th>biggest reason</th><th>answer key</th></tr></thead>
    <tbody>${body}</tbody></table></div>`;
}

/** The alert's neighbourhood as a small two-column graph: where logins came from, where they went. */
function subgraphSvg(a) {
  const logins = [
    { src: a.source, dst: a.destination, kind: 'alert', who: a.account, s: 0 },
    ...a.subgraph.account_logins.map((e) => ({ src: e.source, dst: e.destination, kind: 'own', who: a.account, s: e.seconds_before })),
    ...a.subgraph.source_accounts.map((e) => ({ src: a.source, dst: e.destination, kind: 'other', who: e.account, s: e.seconds_before })),
  ];
  const srcs = [...new Set(logins.map((l) => l.src))];
  const dsts = [...new Set(logins.map((l) => l.dst))];
  const W = 560;
  const rowH = 22;
  const H = Math.max(srcs.length, dsts.length) * rowH + 40;
  const ySrc = (i) => 30 + ((H - 50) * (i + 0.5)) / srcs.length;
  const yDst = (i) => 20 + i * rowH + rowH / 2;
  const xs = 150;
  const xd = W - 150;
  // Draw the alert last so it sits on top of the lines it is being compared with.
  const order = [...logins].sort((p, q) => (p.kind === 'alert') - (q.kind === 'alert'));
  const lines = order.map((l) => {
    const y1 = ySrc(srcs.indexOf(l.src));
    const y2 = yDst(dsts.indexOf(l.dst));
    const mx = (xs + xd) / 2;
    const title = l.kind === 'alert' ? `${l.who}, the alert` : `${l.who}, ${int(l.s)} seconds earlier`;
    return `<path class="sg-${l.kind}" fill="none" d="M${xs},${y1} C${mx},${y1} ${mx},${y2} ${xd},${y2}"><title>${esc(title)}</title></path>`;
  }).join('');
  const sNodes = srcs.map((s, i) => `<rect x="${xs - 4}" y="${ySrc(i) - 4}" width="8" height="8" fill="${s === a.source ? 'var(--signal)' : 'var(--ink-2)'}"/>
    <text x="${xs - 12}" y="${ySrc(i) + 4}" text-anchor="end">${esc(s)}</text>`).join('');
  const dNodes = dsts.map((d, i) => `<rect x="${xd - 3}" y="${yDst(i) - 3}" width="6" height="6" fill="${d === a.destination ? 'var(--signal)' : 'var(--ink-3)'}"/>
    <text x="${xd + 10}" y="${yDst(i) + 4}">${esc(d)}</text>`).join('');
  return `<svg viewBox="0 0 ${W} ${H}" role="img" aria-label="Logins around the alert: ${srcs.length} source computers and ${dsts.length} destinations in the day before">
    <text x="${xs}" y="12" text-anchor="middle" style="fill:var(--ink-3)">from</text>
    <text x="${xd}" y="12" text-anchor="middle" style="fill:var(--ink-3)">to</text>
    ${lines}${sNodes}${dNodes}</svg>`;
}

function explained(a) {
  if (!a) return `<p class="tri-line">The detector put no attack login in this day's feed.</p>`;
  const max = Math.max(...a.contributions.map((c) => c.surprise));
  const bars = a.contributions.map((c) => `<div class="tri-bar">
      <span>${esc(human(c.meaning))}</span>
      <span class="tri-bar-track"><span style="width:${(100 * c.surprise) / max}%"></span></span>
      <span class="num">${c.surprise.toFixed(2)}</span></div>`).join('');
  const logins = a.subgraph.account_logins.map((e) =>
    `<li>${esc(e.source)} to ${esc(e.destination)}, ${int(e.seconds_before)} s earlier</li>`).join('')
    || '<li>none in the day before</li>';
  const others = a.subgraph.source_accounts.map((e) =>
    `<li>${esc(e.account)} to ${esc(e.destination)}, ${int(e.seconds_before)} s earlier</li>`).join('')
    || '<li>none in the day before</li>';
  return `<p class="tri-line"><strong>${esc(a.account)}</strong> logged into <strong>${esc(a.destination)}</strong>
    from <strong>${esc(a.source)}</strong> on day ${a.day}. It is a real attack login. Score ${a.score.toFixed(2)}, and the
    parts below add up to it exactly (${a.explained_score.toFixed(2)}).</p>
    <div class="tri-grid">
      <div class="tri-bars">${bars}</div>
      <div class="subgraph">${subgraphSvg(a)}
        <div class="sg-key"><span><i></i>the alert</span><span><i class="own"></i>this account, just before</span>
        <span><i class="other"></i>other accounts on ${esc(a.source)}</span></div></div>
    </div>
    <div class="tri-sub"><div><h4>This account, the day before</h4><ul>${logins}</ul></div>
    <div><h4>Other accounts on ${esc(a.source)}, the day before</h4><ul>${others}</ul></div></div>`;
}

function verdictCard(title, v) {
  if (!v) return `<div class="tri-verdict"><h4>${title}</h4><p>Not run.</p></div>`;
  const d = v.verdict;
  const call = d.verdict === 'true_positive' ? 'attack' : d.verdict === 'false_positive' ? 'false alarm' : d.verdict.replace('_', ' ');
  return `<div class="tri-verdict"><h4>${title}</h4>
    <p class="tri-call ${d.verdict}">${esc(call)}, confidence ${d.confidence.toFixed(2)}</p>
    ${d.path?.length ? `<p>Path: <span class="mono">${d.path.map(esc).join(' to ')}</span></p>` : ''}
    ${d.linked_accounts?.length ? `<p>Other accounts from the same host: <span class="mono">${d.linked_accounts.map(esc).join(', ')}</span></p>` : ''}
    <p>${esc(d.reason)}</p>
    <p class="tri-meta">${int(v.tool_calls)} tool calls (${v.tools_used.map(esc).join(', ') || 'none'}),
    ${int(v.hallucinated_calls)} refused, ${int(v.tokens)} tokens</p></div>`;
}

function verdicts(t) {
  const v = t.verdicts;
  if (!v.alert) return '<p class="tri-line">The agent did not finish an attack alert before the model became unavailable.</p>';
  const a = v.alert;
  return `<p class="tri-line">A real attack from the benchmark: <strong>${esc(a.account)}</strong> logged into
    <strong>${esc(a.destination)}</strong> from <strong>${esc(a.source)}</strong> on day ${a.day}. The answer key says the
    attacker worked from ${esc(v.truth.launch_host)}. The same alert went to ${esc(t.analyst.model)} twice. On the left it
    could ask graph questions. On the right it could only read the account's history.</p>
    <div class="tri-verdicts">${verdictCard('With graph tools', v.graph)}
    ${verdictCard('Without graph tools', v.nograph)}</div>`;
}

export function setupTriage(t) {
  const mount = $('#tri-view');
  if (!mount) return;

  const c = t.compare;
  const total = t.labels.redteam_identity_days_test_window;
  const entries = [
    ['v1, best rule set', c.v1_best], ['v2 graph detector, fixed in advance', c.headline],
    ['v2 without the v1 rules', c.graph_only], ['a graph neural network', c.gnn],
  ];
  const best = Math.max(...entries.map(([, b]) => b.identity_days_caught));
  $('#tri-compare').innerHTML = entries.map(([name, b]) => `<div class="ledger-row${b.identity_days_caught === best ? ' best' : ''}">
      <span class="name">${name}</span>
      <span class="lbar"><span style="width:${(100 * b.identity_days_caught) / total}%"></span></span>
      <span class="v">${int(b.identity_days_caught)} <small>of ${int(total)}</small></span></div>`).join('');

  // The bars grow once, when the reader reaches them, since the comparison is the point of the section.
  const barEls = [...document.querySelectorAll('#tri-compare .lbar span')];
  if (!REDUCED && window.gsap) {
    window.gsap.set(barEls, { scaleX: 0 });
    new IntersectionObserver((es, obs) => {
      if (!es[0].isIntersecting) return;
      obs.disconnect();
      window.gsap.to(barEls, { scaleX: 1, duration: 0.9, ease: 'power3.out', stagger: 0.08 });
    }, { threshold: 0.4 }).observe($('#tri-compare'));
  }

  const views = {
    normal: () => feedTable(t.normal_day, t.budget_per_day),
    attack: () => feedTable(t.attack_day, t.budget_per_day),
    explain: () => explained(t.explained),
    verdict: () => verdicts(t),
  };
  const buttons = document.querySelectorAll('#tri-buttons .tab');
  const show = (id) => {
    mount.innerHTML = views[id]();
    buttons.forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.view === id)));
    if (!REDUCED && window.gsap) {
      window.gsap.from(mount.children, { autoAlpha: 0, y: 6, duration: 0.32, ease: 'power2.out', stagger: 0.04 });
    }
  };
  buttons.forEach((b) => b.addEventListener('click', () => show(b.dataset.view)));
  show('normal');
}
