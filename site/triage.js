/*
 * The alert feed, one explained alert, and the analyst agent's recorded verdicts.
 *
 * Everything here is read from data/triage.json, which is built from eval/results and the
 * committed agent runs. Nothing is computed live.
 */

const $ = (sel) => document.querySelector(sel);
const int = (n) => Number(n).toLocaleString('en-US');
const pct = (x) => `${(100 * x).toFixed(1)}%`;
// The recorded explanations say "about 131072s earlier", which is what the agent read. The page
// only rewrites it into hours or days for people.
const human = (text) => String(text).replace(/about (\d+)s earlier/, (_, n) => {
  const s = Number(n);
  if (s < 3600) return `about ${Math.round(s / 60)} minutes earlier`;
  if (s < 86400) return `about ${Math.round(s / 3600)} hours earlier`;
  return `about ${Math.round(s / 86400)} days earlier`;
});
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' })[c]);

function feedTable(day, budget, rows = 12) {
  const real = day.alerts.filter((a) => a.is_attack).length;
  const head = `<p class="tri-line">Day ${day.day}. The detector raised its ${int(day.alerts.length)} alerts.
    ${day.attack_events_that_day ? `${int(day.attack_events_that_day)} attack logins happened that day, and
    ${int(real)} of them made the top ${int(budget)}.` : 'No attack happened that day, so every alert is a false alarm.'}</p>`;
  // Real attacks first, then the top of the list, so the one that matters is never cut off.
  const shown = [...day.alerts.filter((a) => a.is_attack), ...day.alerts.filter((a) => !a.is_attack)]
    .slice(0, rows);
  const body = shown.map((a) => `<tr class="${a.is_attack ? 'is-attack' : ''}">
      <td>${esc(a.account)}</td><td>${esc(a.source)} → ${esc(a.destination)}</td>
      <td class="num">${a.score.toFixed(1)}</td><td>${esc(human(a.top_reason))}</td>
      <td>${a.is_attack ? '<span class="tag attack">attack</span>' : '<span class="tag">false alarm</span>'}</td></tr>`).join('');
  return `${head}<div class="table-scroll"><table class="tri-table"><thead><tr><th>account</th>
    <th>login</th><th class="num">score</th><th>biggest reason</th><th>answer key</th></tr></thead>
    <tbody>${body}</tbody></table></div>`;
}

function explained(a) {
  if (!a) return `<p class="tri-line">The detector put no attack login in this day's feed.</p>`;
  const max = Math.max(...a.contributions.map((c) => c.surprise));
  const bars = a.contributions.map((c) => `<div class="tri-bar">
      <span class="tri-bar-label">${esc(human(c.meaning))}</span>
      <span class="tri-bar-track"><span style="width:${(100 * c.surprise) / max}%"></span></span>
      <span class="num">${c.surprise.toFixed(2)}</span></div>`).join('');
  const logins = a.subgraph.account_logins.map((e) =>
    `<li>${esc(e.source)} → ${esc(e.destination)}, ${int(e.seconds_before)} seconds earlier</li>`).join('')
    || '<li>none in the day before</li>';
  const others = a.subgraph.source_accounts.map((e) =>
    `<li>${esc(e.account)} → ${esc(e.destination)}, ${int(e.seconds_before)} seconds earlier</li>`).join('')
    || '<li>none in the day before</li>';
  return `<p class="tri-line"><strong>${esc(a.account)}</strong> logged into <strong>${esc(a.destination)}</strong>
    from <strong>${esc(a.source)}</strong> on day ${a.day}. Score ${a.score.toFixed(2)}. The parts below
    add up to it exactly (${a.explained_score.toFixed(2)}).</p>
    <div class="tri-bars">${bars}</div>
    <div class="tri-sub"><div><h4>This account, the day before</h4><ul>${logins}</ul></div>
    <div><h4>Other accounts on ${esc(a.source)}, the day before</h4><ul>${others}</ul></div></div>`;
}

function verdictCard(title, v) {
  if (!v) return `<div class="tri-verdict"><h4>${title}</h4><p>Not run.</p></div>`;
  const d = v.verdict;
  return `<div class="tri-verdict"><h4>${title}</h4>
    <p class="tri-call ${d.verdict}">${esc(d.verdict.replace('_', ' '))}, confidence ${d.confidence.toFixed(2)}</p>
    ${d.path?.length ? `<p>Path: ${d.path.map(esc).join(' → ')}</p>` : ''}
    ${d.linked_accounts?.length ? `<p>Other accounts from the same host: ${d.linked_accounts.map(esc).join(', ')}</p>` : ''}
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

function workload(t, withAgent) {
  const a = t.analyst.with;
  if (!a) return '<p class="tri-line">The agent benchmark has not been run.</p>';
  const w = a.workload;
  const hours = withAgent ? w.analyst_hours_with_agent : w.analyst_hours_without_agent;
  const alerts = withAgent ? w.alerts_read_with_agent : w.alerts_read_without_agent;
  return `<div class="tri-work"><div class="stat"><span class="stat-value">${int(Math.round(alerts))}</span>
      <div class="stat-label">alerts a person reads</div></div>
    <div class="stat"><span class="stat-value">${int(Math.round(hours))}</span>
      <div class="stat-label">analyst hours, at ${t.triage_minutes} minutes each</div></div>
    <div class="stat"><span class="stat-value">${withAgent ? pct(a.cascade.attack_recall) : pct(1)}</span>
      <div class="stat-label">of attack alerts still reach a person or are called attacks</div></div></div>
    <p class="tri-line">${withAgent
      ? `The agent closes what it is confident about (${t.analyst.with.cascade.accept_at} or higher) and passes the rest on. It closed ${int(a.attacks_closed_as_benign)} real attacks as false alarms in the benchmark.`
      : 'Without the agent, a person reads every alert in the wider feed.'}
    Measured on the ${int(t.analyst.scored)} of ${int(t.analyst.size)} benchmark alerts the agent finished before the model became unavailable.</p>`;
}

export async function setupTriage() {
  const mount = $('#tri-view');
  if (!mount) return;
  const t = await (await fetch('data/triage.json')).json();

  const c = t.compare;
  $('#tri-compare').innerHTML = [
    ['v1, best rule set', c.v1_best], ['v2 graph detector', c.headline],
    ['v2 without the v1 rules', c.graph_only], ['a graph neural network', c.gnn],
  ].map(([name, b]) => `<div class="stat"><span class="stat-value">${int(b.identity_days_caught)}</span>
      <div class="stat-label">${name}</div><div class="stat-note">of ${int(t.labels.redteam_identity_days_test_window)} attack account-days</div></div>`).join('');

  const views = {
    normal: () => feedTable(t.normal_day, t.budget_per_day),
    attack: () => feedTable(t.attack_day, t.budget_per_day),
    explain: () => explained(t.explained),
    verdict: () => verdicts(t),
  };
  const buttons = document.querySelectorAll('#tri-buttons .preset');
  const show = (id) => {
    mount.innerHTML = views[id]();
    buttons.forEach((b) => b.setAttribute('aria-pressed', String(b.dataset.view === id)));
  };
  buttons.forEach((b) => b.addEventListener('click', () => show(b.dataset.view)));
  show('normal');

  const toggle = $('#tri-agent');
  const work = () => { $('#tri-work').innerHTML = workload(t, toggle.checked); };
  toggle.addEventListener('change', work);
  work();
}
