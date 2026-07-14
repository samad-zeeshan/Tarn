/* Force-directed privilege-path explorer. Hand-rolled — no d3, no CDN.
 *
 * The physics is a plain Fruchterman-Reingold-ish loop: repulsion between every pair, spring
 * attraction along edges, mild gravity toward the centre, cooling over ~300 ticks. The graph is
 * bounded (the red-team subgraph plus benign context) precisely so an O(n^2) repulsion step is
 * affordable in a browser without a quadtree.
 *
 * The paths are REAL Neo4j shortestPath() results, precomputed by graph/export_paths.py, because
 * the browser cannot run Cypher. The page says so rather than implying it is querying live.
 */

const NS = 'http://www.w3.org/2000/svg';
const el = (n, a = {}) => {
  const node = document.createElementNS(NS, n);
  for (const [k, v] of Object.entries(a)) if (v != null) node.setAttribute(k, String(v));
  return node;
};

export class PathExplorer {
  constructor(mount, graph, paths) {
    this.mount = mount;
    this.graph = graph;
    this.paths = paths;
    this.showRedteam = true;
    this.showLabels = false;
    this.activePath = null;
    this.W = 760;
    this.H = 440;

    this.nodes = graph.nodes.map((n) => ({ ...n }));
    this.index = new Map(this.nodes.map((n, i) => [n.id, i]));
    this.links = graph.links
      .filter((l) => this.index.has(l.source) && this.index.has(l.target))
      .map((l) => ({ ...l, s: this.index.get(l.source), t: this.index.get(l.target) }));

    this.layout();
    this.render();
  }

  /** Deterministic seeding — a graph that reshuffles on every reload looks broken, not organic. */
  layout() {
    const { W, H } = this;
    const n = this.nodes.length;
    let seed = 42;
    const rnd = () => {
      seed = (seed * 1103515245 + 12345) & 0x7fffffff;
      return seed / 0x7fffffff;
    };

    // Seed on a circle: users on an inner ring, hosts on an outer one. Starting from a shape
    // that already encodes the bipartite structure converges far faster than random noise.
    this.nodes.forEach((node, i) => {
      const isUser = node.kind === 'user';
      const r = isUser ? Math.min(W, H) * 0.18 : Math.min(W, H) * 0.38;
      const a = (i / n) * Math.PI * 2 + (isUser ? 0 : 0.4);
      node.x = W / 2 + Math.cos(a) * r + (rnd() - 0.5) * 20;
      node.y = H / 2 + Math.sin(a) * r + (rnd() - 0.5) * 20;
      node.vx = 0;
      node.vy = 0;
      node.deg = 0;
    });
    this.links.forEach((l) => {
      this.nodes[l.s].deg++;
      this.nodes[l.t].deg++;
    });

    const K = Math.sqrt((W * H) / Math.max(n, 1)) * 0.62;
    let temp = W / 8;

    for (let step = 0; step < 320; step++) {
      // Repulsion (all pairs).
      for (let i = 0; i < n; i++) {
        const a = this.nodes[i];
        for (let j = i + 1; j < n; j++) {
          const b = this.nodes[j];
          let dx = a.x - b.x;
          let dy = a.y - b.y;
          let d2 = dx * dx + dy * dy;
          if (d2 < 0.01) {
            dx = (rnd() - 0.5) * 0.1;
            dy = (rnd() - 0.5) * 0.1;
            d2 = 0.01;
          }
          const d = Math.sqrt(d2);
          const f = (K * K) / d2;
          const fx = (dx / d) * f;
          const fy = (dy / d) * f;
          a.vx += fx; a.vy += fy;
          b.vx -= fx; b.vy -= fy;
        }
      }
      // Spring attraction along edges.
      for (const l of this.links) {
        const a = this.nodes[l.s];
        const b = this.nodes[l.t];
        const dx = a.x - b.x;
        const dy = a.y - b.y;
        const d = Math.max(0.1, Math.hypot(dx, dy));
        const f = (d * d) / K / 12;
        const fx = (dx / d) * f;
        const fy = (dy / d) * f;
        a.vx -= fx; a.vy -= fy;
        b.vx += fx; b.vy += fy;
      }
      // Gravity, then cool.
      for (const node of this.nodes) {
        node.vx += (W / 2 - node.x) * 0.008;
        node.vy += (H / 2 - node.y) * 0.008;
        const sp = Math.hypot(node.vx, node.vy);
        const lim = Math.min(sp, temp);
        if (sp > 0) {
          node.x += (node.vx / sp) * lim;
          node.y += (node.vy / sp) * lim;
        }
        node.x = Math.max(24, Math.min(W - 24, node.x));
        node.y = Math.max(20, Math.min(H - 20, node.y));
        node.vx = 0;
        node.vy = 0;
      }
      temp = Math.max(0.6, temp * 0.985);
    }
  }

  nodeClass(n) {
    if (n.kind === 'user') return 'node-user';
    if (n.pivot) return 'node-pivot';
    if (n.target) return 'node-target';
    return 'node-comp';
  }

  radius(n) {
    return n.kind === 'user' ? 5.5 : Math.min(9, 3.5 + Math.sqrt(n.deg || 1));
  }

  setPath(fromUser, toUser) {
    this.activePath =
      this.paths.find((p) => p.from_user === fromUser && p.to_user === toUser) ||
      this.paths.find((p) => p.from_user === toUser && p.to_user === fromUser) ||
      null;
    this.render();
    return this.activePath;
  }

  setRedteam(on) { this.showRedteam = on; this.render(); }
  setLabels(on) { this.showLabels = on; this.render(); }

  render() {
    const { W, H } = this;
    this.mount.innerHTML = '';
    const svg = el('svg', {
      viewBox: `0 0 ${W} ${H}`,
      role: 'img',
      'aria-label':
        'Force-directed graph of the red-team movement subgraph: identities, hosts, and the ' +
        'authentication edges between them.',
    });

    const onPath = new Set(this.activePath ? this.activePath.hops : []);
    const pathEdges = new Set();
    if (this.activePath) {
      const h = this.activePath.hops;
      for (let i = 0; i < h.length - 1; i++) {
        pathEdges.add(`${h[i]}|${h[i + 1]}`);
        pathEdges.add(`${h[i + 1]}|${h[i]}`);
      }
    }

    // Edges first, so nodes sit on top.
    for (const l of this.links) {
      const a = this.nodes[l.s];
      const b = this.nodes[l.t];
      const isPath = pathEdges.has(`${a.id}|${b.id}`);
      const isRt = l.redteam && this.showRedteam;
      if (l.redteam && !this.showRedteam && !isPath) {
        // Overlay off: draw the edge, but plainly.
      }
      const cls = isPath ? 'edge onpath' : isRt ? 'edge redteam' : 'edge';
      const line = el('line', {
        x1: a.x, y1: a.y, x2: b.x, y2: b.y,
        class: cls,
        opacity: this.activePath && !isPath ? 0.25 : isRt ? 0.85 : 0.4,
      });
      svg.append(line);
    }

    for (const n of this.nodes) {
      const isOn = onPath.has(n.id);
      const c = el('circle', {
        cx: n.x, cy: n.y, r: this.radius(n) * (isOn ? 1.5 : 1),
        class: `node ${this.nodeClass(n)}${isOn ? ' onpath' : ''}`,
        opacity: this.activePath && !isOn ? 0.35 : 1,
        tabindex: 0,
        role: 'img',
        'aria-label': `${n.kind === 'user' ? 'Identity' : 'Host'} ${n.id}${
          n.pivot ? ', red-team pivot' : n.target ? ', red-team target' : ''
        }, degree ${n.deg}`,
      });
      c.style.cursor = 'pointer';

      const label = `<div><b>${n.id}</b></div>
        <div><span class="tip-k">type</span> ${n.kind === 'user' ? 'identity' : 'host'}</div>
        <div><span class="tip-k">degree</span> ${n.deg}</div>
        ${n.pivot ? '<div style="color:var(--series-3)">red-team pivot host</div>' : ''}
        ${n.target ? '<div style="color:var(--series-4)">red-team target host</div>' : ''}`;

      const tip = document.getElementById('tip');
      const show = (e) => {
        tip.innerHTML = label;
        tip.classList.add('show');
        const r = c.getBoundingClientRect();
        const x = (e && e.clientX) || r.left + r.width / 2;
        const y = (e && e.clientY) || r.top;
        tip.style.left = `${Math.min(x + 12, window.innerWidth - tip.offsetWidth - 12)}px`;
        tip.style.top = `${Math.max(12, y - tip.offsetHeight - 8)}px`;
      };
      const hide = () => tip.classList.remove('show');
      c.addEventListener('mouseenter', show);
      c.addEventListener('mousemove', show);
      c.addEventListener('mouseleave', hide);
      c.addEventListener('focus', show);
      c.addEventListener('blur', hide);

      svg.append(c);

      if (this.showLabels || isOn || n.pivot) {
        svg.append(
          Object.assign(
            el('text', {
              x: n.x + this.radius(n) + 4,
              y: n.y + 3,
              class: 'node-label',
              'font-weight': isOn ? 600 : 400,
              fill: isOn ? 'var(--fg)' : 'var(--fg-muted)',
            }),
            { textContent: n.id },
          ),
        );
      }
    }

    this.mount.append(svg);
  }
}
