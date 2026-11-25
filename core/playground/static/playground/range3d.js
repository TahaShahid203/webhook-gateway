/* Webhook Range 3D: a neon datacenter city you can orbit. Every packet is a real webhook going through the real gateway.
   Engine (Three.js + bloom) -> stations -> narrated choreography -> polling the real system. Nothing is mocked. */
(() => {
'use strict';
const $ = (s) => document.querySelector(s);
const V3 = THREE.Vector3;
const rnd = (a, b) => a + Math.random() * (b - a);
const clamp = (x, a, b) => Math.max(a, Math.min(b, x));
const lerp = (a, b, t) => a + (b - a) * t;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const state = { broker: 'rabbitmq', tags: new Set(), cursors: {}, score: { fired: 0, blocked: 0, dups: 0, delivered: 0, retries: 0 },
  dupSeen: {}, breaker: {}, lastDepth: {}, crash: null, dead: 0, lastSfx: {}, mode: 'ok', paused: false, sound: false, events: 0, ride: true };
const HEX = { redisv: 0xd946ef, stripe: 0xb197fc, shopify: 0x5eead4, malicious: 0xff4d6d, ok: 0x4ade80, bad: 0xff4d6d, warn: 0xfbbf24, mail: 0x9ec5ff, rabbit: 0xff8a3d, redis: 0xef4444 };
const CSS = (h) => '#' + h.toString(16).padStart(6, '0');
const TN = { rabbitmq: 'RabbitMQ', redis: 'Redis (append-only file)', redis_volatile: 'Redis (NO persistence)' };
const qcol = (t) => (t === 'redis' ? HEX.redis : t === 'redis_volatile' ? HEX.redisv : HEX.rabbit);
const GUN = { stripe: 'gun-stripe', shopify: 'gun-shopify', malicious: 'gun-evil' };

// ======================= renderer, bloom, lights =======================
const canvas = $('#gl');
let renderer;
try { renderer = new THREE.WebGLRenderer({ canvas, antialias: true, powerPreference: 'high-performance' }); }
catch (e) { $('#nogl').style.display = 'flex'; throw e; }
renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 1.75));
renderer.toneMapping = THREE.ACESFilmicToneMapping; renderer.toneMappingExposure = 1.15;
const scene = new THREE.Scene();
scene.background = new THREE.Color(0x04060f); scene.fog = new THREE.FogExp2(0x060a1a, 0.0072);
const camera = new THREE.PerspectiveCamera(46, 1, 0.5, 700);
scene.add(new THREE.HemisphereLight(0x6f7dff, 0x0a0a1a, 0.85));
const sun = new THREE.DirectionalLight(0xbcd0ff, 0.55); sun.position.set(-30, 60, 40); scene.add(sun);
let composer, bloom;
function setupComposer() {
  composer = new THREE.EffectComposer(renderer);
  composer.addPass(new THREE.RenderPass(scene, camera));
  bloom = new THREE.UnrealBloomPass(new THREE.Vector2(innerWidth, innerHeight), 0.8, 0.55, 0.62);
  composer.addPass(bloom);
}
setupComposer();
function resize() { const w = innerWidth, h = innerHeight; renderer.setSize(w, h, false); composer.setSize(w, h); camera.aspect = w / h; camera.updateProjectionMatrix(); }
addEventListener('resize', resize); resize();

// ground, grid, stars
const ground = new THREE.Mesh(new THREE.PlaneGeometry(520, 520), new THREE.MeshStandardMaterial({ color: 0x050816, metalness: 0.7, roughness: 0.38 }));
ground.rotation.x = -Math.PI / 2; scene.add(ground);
const grid = new THREE.GridHelper(420, 105, 0x1d4f9a, 0x0e2a55); grid.position.y = 0.03; grid.material.transparent = true; grid.material.opacity = 0.55; scene.add(grid);
{ const n = 900, g = new THREE.BufferGeometry(), p = new Float32Array(n * 3);
  for (let i = 0; i < n; i++) { const a = rnd(0, 6.283), e = rnd(0.05, 1.3), r = 320; p[i * 3] = Math.cos(a) * Math.cos(e) * r; p[i * 3 + 1] = Math.sin(e) * r * 0.9 + 10; p[i * 3 + 2] = Math.sin(a) * Math.cos(e) * r; }
  g.setAttribute('position', new THREE.BufferAttribute(p, 3)); scene.add(new THREE.Points(g, new THREE.PointsMaterial({ color: 0x9fb7ff, size: 1.3, sizeAttenuation: false, transparent: true, opacity: 0.7, fog: false }))); }

// ======================= particles (one GPU buffer) =======================
const MAXP = 4000, P = { pos: new Float32Array(MAXP * 3), col: new Float32Array(MAXP * 3), size: new Float32Array(MAXP), alpha: new Float32Array(MAXP),
  vel: new Float32Array(MAXP * 3), life: new Float32Array(MAXP), max: new Float32Array(MAXP), g: new Float32Array(MAXP), s0: new Float32Array(MAXP), head: 0 };
const pgeo = new THREE.BufferGeometry();
pgeo.setAttribute('position', new THREE.BufferAttribute(P.pos, 3)); pgeo.setAttribute('aColor', new THREE.BufferAttribute(P.col, 3));
pgeo.setAttribute('aSize', new THREE.BufferAttribute(P.size, 1)); pgeo.setAttribute('aAlpha', new THREE.BufferAttribute(P.alpha, 1));
const pmat = new THREE.ShaderMaterial({ transparent: true, depthWrite: false, blending: THREE.AdditiveBlending,
  vertexShader: 'attribute vec3 aColor; attribute float aSize; attribute float aAlpha; varying vec3 vC; varying float vA; void main(){ vC=aColor; vA=aAlpha; vec4 mv=modelViewMatrix*vec4(position,1.0); gl_PointSize=aSize*(380.0/-mv.z); gl_Position=projectionMatrix*mv; }',
  fragmentShader: 'varying vec3 vC; varying float vA; void main(){ float d=length(gl_PointCoord-0.5); float a=smoothstep(0.5,0.0,d)*vA; if(a<0.01) discard; gl_FragColor=vec4(vC*(1.0+a),a); }' });
const points = new THREE.Points(pgeo, pmat); points.frustumCulled = false; scene.add(points);
const _c = new THREE.Color();
function emit(x, y, z, vx, vy, vz, color, size, life, g) {
  const i = P.head; P.head = (P.head + 1) % MAXP; _c.setHex(color);
  P.pos[i * 3] = x; P.pos[i * 3 + 1] = y; P.pos[i * 3 + 2] = z; P.vel[i * 3] = vx; P.vel[i * 3 + 1] = vy; P.vel[i * 3 + 2] = vz;
  P.col[i * 3] = _c.r; P.col[i * 3 + 1] = _c.g; P.col[i * 3 + 2] = _c.b; P.s0[i] = size; P.life[i] = life; P.max[i] = life; P.g[i] = g;
}
function burst(p, n, o = {}) {
  const sp = o.speed || 8, dir = o.dir, spread = o.spread == null ? 1 : o.spread;
  for (let i = 0; i < n; i++) {
    let vx = rnd(-1, 1), vy = rnd(-1, 1), vz = rnd(-1, 1); const l = Math.hypot(vx, vy, vz) || 1;
    vx /= l; vy /= l; vz /= l;
    if (dir) { vx = lerp(dir.x, vx, spread); vy = lerp(dir.y, vy, spread); vz = lerp(dir.z, vz, spread); }
    const s = rnd(0.3, 1) * sp;
    emit(p.x, p.y, p.z, vx * s, vy * s, vz * s, o.color || 0xffffff, (o.size || 1.2) * rnd(0.6, 1.3), (o.life || 800) * rnd(0.7, 1.2), o.g == null ? -6 : o.g);
  }
}
function updateParticles(dt) {
  const s = dt / 1000;
  for (let i = 0; i < MAXP; i++) {
    if (P.life[i] <= 0) { P.alpha[i] = 0; continue; }
    P.life[i] -= dt; const k = Math.max(0, P.life[i] / P.max[i]);
    P.vel[i * 3 + 1] += P.g[i] * s; P.pos[i * 3] += P.vel[i * 3] * s; P.pos[i * 3 + 1] += P.vel[i * 3 + 1] * s; P.pos[i * 3 + 2] += P.vel[i * 3 + 2] * s;
    P.vel[i * 3] *= 0.992; P.vel[i * 3 + 2] *= 0.992; P.alpha[i] = k; P.size[i] = P.s0[i] * (0.4 + 0.6 * k);
  }
  pgeo.attributes.position.needsUpdate = pgeo.attributes.aColor.needsUpdate = pgeo.attributes.aSize.needsUpdate = pgeo.attributes.aAlpha.needsUpdate = true;
}

// rings (shockwaves) and bolts
const rings = [];
function ring(p, color, o = {}) {
  const m = new THREE.Mesh(new THREE.RingGeometry(0.88, 1, 56), new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.95, side: THREE.DoubleSide, blending: THREE.AdditiveBlending, depthWrite: false }));
  m.position.copy(p); const orient = o.orient || 'cam'; if (orient === 'ground') m.rotation.x = -Math.PI / 2; else if (orient === 'x') m.rotation.y = Math.PI / 2;
  scene.add(m); rings.push({ m, t: 0, life: o.life || 700, size: o.size || 8, orient });
}
const bolts = [];
function bolt(p, color = 0xfbbf24) {
  const geo = new THREE.BufferGeometry(); geo.setAttribute('position', new THREE.BufferAttribute(new Float32Array(30 * 3), 3));
  const l = new THREE.Line(geo, new THREE.LineBasicMaterial({ color, transparent: true, opacity: 1 })); l.frustumCulled = false; scene.add(l); bolts.push({ l, p: p.clone(), t: 0, life: 500 });
}

// ======================= DOM overlays projected from 3D =======================
const labelsEl = $('#labels'), screenV = new V3();
function toScreen(v) { screenV.copy(v).project(camera); return { x: (screenV.x * 0.5 + 0.5) * innerWidth, y: (-screenV.y * 0.5 + 0.5) * innerHeight, behind: screenV.z > 1 }; }
const floaters = [];
function popText(p, text, color, life = 1500) {
  const el = document.createElement('div'); el.className = 'ftext'; el.textContent = text; el.style.color = CSS(color); labelsEl.appendChild(el);
  floaters.push({ el, p: p.clone(), t: 0, life });
}
const bubbles = {};
function say(id, text, kind = 'run', hold = 1800) {
  if (quiet()) return;
  let b = bubbles[id]; if (!b) { const el = document.createElement('div'); el.className = 'bubble'; labelsEl.appendChild(el); b = bubbles[id] = { el, t: null }; }
  b.el.textContent = text; b.el.dataset.kind = kind; b.el.classList.add('show'); b.id = id;
  clearTimeout(b.t); b.t = setTimeout(() => b.el.classList.remove('show'), T(hold) + 800);
}
function updateOverlays(dt) {
  for (let i = floaters.length - 1; i >= 0; i--) {
    const f = floaters[i]; f.t += dt; if (f.t > f.life) { f.el.remove(); floaters.splice(i, 1); continue; }
    f.p.y += dt * 0.0035; const s = toScreen(f.p); f.el.style.transform = `translate(${s.x}px,${s.y}px) translate(-50%,-100%)`; f.el.style.opacity = s.behind ? 0 : Math.min(1, (f.life - f.t) / 500);
  }
  for (const id in bubbles) { const b = bubbles[id], a = AN[id]; if (!a) continue; const s = toScreen(a.top || a.c); b.el.style.transform = `translate(${clamp(s.x, 150, innerWidth - 150)}px,${s.y - 6}px) translate(-50%,-100%)`; }
  document.querySelectorAll('.slabel').forEach((el) => { const a = AN[el.dataset.st]; if (!a) return; const s = toScreen(a.label || a.c); el.style.transform = `translate(${s.x}px,${s.y}px) translate(-50%,0)`; el.style.opacity = s.behind ? 0 : 1; });
}

// ======================= stations =======================
const AN = {}, ST = {};
function neon(mesh, color, op = 0.9) { const l = new THREE.LineSegments(new THREE.EdgesGeometry(mesh.geometry), new THREE.LineBasicMaterial({ color, transparent: true, opacity: op })); mesh.add(l); return l; }
const mat = (color, e = 1.2, o = {}) => new THREE.MeshStandardMaterial({ color: 0x0a0f22, emissive: color, emissiveIntensity: e * 0.42, metalness: 0.4, roughness: 0.35, ...o });
function glow(p, color, size = 10) { const m = new THREE.Mesh(new THREE.RingGeometry(size * 0.55, size, 48), new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.22, side: THREE.DoubleSide, blending: THREE.AdditiveBlending, depthWrite: false })); m.rotation.x = -Math.PI / 2; m.position.set(p.x, 0.06, p.z); scene.add(m); return m; }
function station(id, name, c, anchor, labelOffset = new V3(0, -2, 0)) {
  AN[id] = { c, label: c.clone().add(labelOffset), top: c.clone().add(new V3(0, anchor.h || 10, 0)), ...anchor };
  const el = document.createElement('div'); el.className = 'slabel'; el.dataset.st = id; el.innerHTML = `<b>${name}</b><span id="n-${id}"></span>`; labelsEl.appendChild(el);
  return AN[id];
}

// --- launch pads (guns)
['stripe', 'shopify', 'malicious'].forEach((k, i) => {
  const z = (i - 1) * 11, col = HEX[k], g = new THREE.Group(); g.position.set(-38, 0, z); scene.add(g);
  const base = new THREE.Mesh(new THREE.CylinderGeometry(3.4, 4, 0.8, 8), mat(col, 0.35)); base.position.y = 0.4; neon(base, col); g.add(base);
  const barrel = new THREE.Mesh(new THREE.BoxGeometry(6, 1.8, 1.8), mat(col, 0.9)); barrel.position.set(1.2, 2.6, 0); neon(barrel, 0xffffff, 0.5); g.add(barrel);
  const core = new THREE.Mesh(new THREE.IcosahedronGeometry(1.5, 1), mat(col, 2.2)); core.position.set(-2.2, 2.8, 0); g.add(core);
  const rg = new THREE.Mesh(new THREE.TorusGeometry(2.6, 0.12, 8, 40), new THREE.MeshBasicMaterial({ color: col })); rg.rotation.x = Math.PI / 2; rg.position.y = 0.9; g.add(rg);
  glow(g.position, col, 7);
  const pl = new THREE.PointLight(col, 1.4, 26); pl.position.set(-35, 5, z); scene.add(pl);
  ST['gun-' + (k === 'malicious' ? 'evil' : k)] = { g, barrel, core, rg, recoil: 0, light: pl, col };
  station('gun-' + (k === 'malicious' ? 'evil' : k), { stripe: '💳 STRIPE GUN', shopify: '🛍️ SHOPIFY GUN', malicious: '☠️ FAKE-WEBHOOK GUN' }[k], new V3(-38, 0, z), { mouth: new V3(-34.4, 2.8, z), h: 7, label: new V3(-38, 0, z + 5.6) });
  const lab = document.querySelector(`.slabel[data-st="gun-${k === 'malicious' ? 'evil' : k}"]`); lab.classList.add('clickable'); lab.title = 'click to fire';
  lab.onclick = () => fire(k);
  lab.style.pointerEvents = 'auto';
});
AN.cloud = { c: new V3(-44, 24, -20), top: new V3(-44, 26, -20) };

// --- edge gate
{
  const g = new THREE.Group(); g.position.set(-19, 0, 0); scene.add(g);
  const ringM = new THREE.Mesh(new THREE.TorusGeometry(7.4, 0.42, 16, 72), new THREE.MeshBasicMaterial({ color: 0x4ad0ff })); ringM.rotation.y = Math.PI / 2; ringM.position.y = 7.5; g.add(ringM);
  const ring2 = new THREE.Mesh(new THREE.TorusGeometry(8.6, 0.12, 8, 72), new THREE.MeshBasicMaterial({ color: 0x7a8cff, transparent: true, opacity: 0.7 })); ring2.rotation.y = Math.PI / 2; ring2.position.y = 7.5; g.add(ring2);
  const field = new THREE.Mesh(new THREE.CircleGeometry(7.2, 48), new THREE.MeshBasicMaterial({ color: 0x4ad0ff, transparent: true, opacity: 0.07, side: THREE.DoubleSide, blending: THREE.AdditiveBlending, depthWrite: false })); field.rotation.y = Math.PI / 2; field.position.y = 7.5; g.add(field);
  const bar = new THREE.Mesh(new THREE.PlaneGeometry(14.6, 0.35), new THREE.MeshBasicMaterial({ color: 0x5eead4, transparent: true, opacity: 0, side: THREE.DoubleSide, blending: THREE.AdditiveBlending, depthWrite: false })); bar.rotation.y = Math.PI / 2; bar.position.y = 7.5; g.add(bar);
  [-9.6, 9.6].forEach((z) => { const p = new THREE.Mesh(new THREE.BoxGeometry(1.6, 17, 1.6), mat(0x4a78d8, 0.5)); p.position.set(0, 8.5, z); neon(p, 0x7fb2ff); g.add(p); });
  const beam = new THREE.Mesh(new THREE.BoxGeometry(1.6, 1.2, 21), mat(0x4a78d8, 0.7)); beam.position.set(0, 17.2, 0); neon(beam, 0x7fb2ff); g.add(beam);
  glow(g.position, 0x4ad0ff, 12);
  const pl = new THREE.PointLight(0x4ad0ff, 1.8, 30); pl.position.set(-19, 8, 0); scene.add(pl);
  ST.edge = { g, ringM, ring2, field, bar, flash: 0, flashColor: 0x4ad0ff, scan: 0, light: pl };
  station('edge', '🏰 EDGE · FastAPI', new V3(-19, 7.5, 0), { h: 12, label: new V3(-19, 0, 10.5) });
}
const edgeIn = () => new V3(-23.8, 7.5 + rnd(-2.2, 2.2), rnd(-3.4, 3.4)), edgeOut = () => new V3(-14.5, 7.5, 0);

// --- queue tube
const TUBE = { x0: -9, x1: 7, y: 4, r: 2.2 };
{
  const g = new THREE.Group(); scene.add(g);
  const tube = new THREE.Mesh(new THREE.CylinderGeometry(TUBE.r, TUBE.r, TUBE.x1 - TUBE.x0, 40, 1, true), new THREE.MeshStandardMaterial({ color: 0x88aaff, transparent: true, opacity: 0.13, side: THREE.DoubleSide, metalness: 0.9, roughness: 0.1, emissive: 0x2244aa, emissiveIntensity: 0.6, depthWrite: false }));
  tube.rotation.z = Math.PI / 2; tube.position.set((TUBE.x0 + TUBE.x1) / 2, TUBE.y, 0); g.add(tube);
  const rings = []; [TUBE.x0, TUBE.x1].forEach((x) => { const r = new THREE.Mesh(new THREE.TorusGeometry(TUBE.r + 0.15, 0.2, 10, 40), new THREE.MeshBasicMaterial({ color: HEX.rabbit })); r.rotation.y = Math.PI / 2; r.position.set(x, TUBE.y, 0); g.add(r); rings.push(r); });
  for (let i = 1; i < 6; i++) { const r = new THREE.Mesh(new THREE.TorusGeometry(TUBE.r + 0.05, 0.05, 6, 32), new THREE.MeshBasicMaterial({ color: 0x4f6fbf, transparent: true, opacity: 0.6 })); r.rotation.y = Math.PI / 2; r.position.set(lerp(TUBE.x0, TUBE.x1, i / 6), TUBE.y, 0); g.add(r); }
  const stand = new THREE.Mesh(new THREE.BoxGeometry(TUBE.x1 - TUBE.x0 + 2, 0.5, 5), mat(0x2a3a66, 0.3)); stand.position.set((TUBE.x0 + TUBE.x1) / 2, 0.25, 0); neon(stand, 0x4a78d8, 0.6); g.add(stand);
  glow(new V3((TUBE.x0 + TUBE.x1) / 2, 0, 0), HEX.rabbit, 11);
  ST.queue = { g, rings, caps: [], target: 0, stuck: false };
  station('queue', '📬 DURABLE QUEUE', new V3((TUBE.x0 + TUBE.x1) / 2, TUBE.y, 0), { in: new V3(TUBE.x0 - 0.5, TUBE.y, 0), out: new V3(TUBE.x1 + 0.2, TUBE.y, 0), h: 8, label: new V3((TUBE.x0 + TUBE.x1) / 2, 0, 5.6) });
  AN.pipe = AN.queue;
}

// --- ingest worker: dedup checkpoint + turbine gears
{
  const g = new THREE.Group(); g.position.set(17, 0, 0); scene.add(g);
  const gear = (r, teeth, col, x, y, z) => { const gr = new THREE.Group(); gr.position.set(x, y, z);
    const body = new THREE.Mesh(new THREE.CylinderGeometry(r, r, 1.4, 28), mat(col, 0.5)); body.rotation.x = Math.PI / 2; neon(body, col, 0.7); gr.add(body);
    for (let i = 0; i < teeth; i++) { const a = (i / teeth) * Math.PI * 2, t = new THREE.Mesh(new THREE.BoxGeometry(r * 0.32, r * 0.42, 1.4), mat(col, 0.8)); t.position.set(Math.cos(a) * (r + r * 0.12), Math.sin(a) * (r + r * 0.12), 0); t.rotation.z = a; gr.add(t); }
    const hub = new THREE.Mesh(new THREE.CylinderGeometry(r * 0.28, r * 0.28, 1.8, 16), mat(0xfbbf24, 1.6)); hub.rotation.x = Math.PI / 2; gr.add(hub); g.add(gr); return gr; };
  const big = gear(4.6, 12, 0xd8884a, 0, 6.3, -3.2), small = gear(2.9, 8, 0xe9a35f, 6.5, 3.2, -3.2);
  const frame = new THREE.Group(); frame.position.set(0, 4.5, 0);
  [[0, 4.2, 0, 0.5, 0.5, 9], [0, -4.2, 0, 0.5, 0.5, 9], [0, 0, 4.4, 0.5, 8.9, 0.5], [0, 0, -4.4, 0.5, 8.9, 0.5]].forEach(([x, y, z, w, h, d]) => { const b = new THREE.Mesh(new THREE.BoxGeometry(w, h, d), mat(0xfbbf24, 1.5)); b.position.set(x, y, z); frame.add(b); });
  g.add(frame); glow(g.position, 0xfbbf24, 9);
  const pl = new THREE.PointLight(0xfbbf24, 1.2, 24); pl.position.set(17, 6, 3); scene.add(pl);
  ST.worker = { g, big, small, frame, speed: 0.4, busy: 0, paused: false, light: pl };
  station('worker', '⚙️ INGEST WORKER', new V3(17, 4.5, 0), { in: new V3(12.2, 4.5, 0), out: new V3(21.8, 4.5, 0), h: 11, label: new V3(17, 0, 6.2) });
}

// --- postgres vault: a stack of discs that grows with every stored event
const VAULT = { x: 33, layers: 0, discs: [], flash: 0 };
{
  const g = new THREE.Group(); g.position.set(VAULT.x, 0, 0); scene.add(g);
  const base = new THREE.Mesh(new THREE.CylinderGeometry(5.6, 6, 0.6, 32), mat(0x2a8f6a, 0.4)); base.position.y = 0.3; neon(base, 0x4ade80, 0.7); g.add(base);
  glow(g.position, 0x4ade80, 10);
  const pl = new THREE.PointLight(0x4ade80, 1.4, 26); pl.position.set(VAULT.x, 9, 4); scene.add(pl);
  ST.vault = { g, light: pl };
  station('vault', '🗄️ POSTGRES', new V3(VAULT.x, 3, 0), { in: new V3(VAULT.x - 4.8, 4.5, 0), out: new V3(VAULT.x, 1.5, 4.6), h: 16, label: new V3(VAULT.x, 0, 7.4) });
  AN.vault.topOf = () => new V3(VAULT.x, 1 + VAULT.layers * 0.55 + 1, 0);
  VAULT.g = g;
}
function setVaultLayers(n) {
  n = clamp(n, 3, 34);
  while (VAULT.layers < n) { const i = VAULT.layers, d = new THREE.Mesh(new THREE.CylinderGeometry(4.5, 4.5, 0.42, 36), mat(i % 2 ? 0x2fa37a : 0x35c48e, 0.9)); d.position.y = 0.9 + i * 0.55; d.scale.set(0.01, 1, 0.01); neon(d, 0x7dffb0, 0.5); VAULT.g.add(d); VAULT.discs.push({ m: d, t: 0 }); VAULT.layers++; }
}
setVaultLayers(3);

// --- delivery hub (dispatcher)
{
  const g = new THREE.Group(); g.position.set(33, 0, 21); scene.add(g);
  const pad = new THREE.Mesh(new THREE.CylinderGeometry(5, 5.4, 0.6, 8), mat(0x4a78d8, 0.4)); pad.position.y = 0.3; neon(pad, 0x7fb2ff, 0.7); g.add(pad);
  const core = new THREE.Mesh(new THREE.OctahedronGeometry(2.4, 0), mat(0x7aa7ff, 1.6)); core.position.y = 6; neon(core, 0xffffff, 0.6); g.add(core);
  const orbit = new THREE.Mesh(new THREE.TorusGeometry(4, 0.14, 8, 48), new THREE.MeshBasicMaterial({ color: 0x7aa7ff })); orbit.rotation.x = Math.PI / 2.4; orbit.position.y = 6; g.add(orbit);
  glow(g.position, 0x7aa7ff, 9);
  const pl = new THREE.PointLight(0x7aa7ff, 1.3, 26); pl.position.set(33, 8, 24); scene.add(pl);
  ST.dispatcher = { g, core, orbit, busy: 0, light: pl };
  station('dispatcher', '📮 DELIVERY WORKERS', new V3(33, 6, 21), { in: new V3(33, 6, 17), out: new V3(36.5, 6, 21), h: 11, label: new V3(33, 0, 27) });
}

// --- consumers: skyscrapers whose windows tell you their health
function windowTexture(lit) {
  const c = document.createElement('canvas'); c.width = 128; c.height = 256; const x = c.getContext('2d'); x.fillStyle = '#05070f'; x.fillRect(0, 0, 128, 256);
  for (let r = 0; r < 16; r++) for (let q = 0; q < 6; q++) { const on = lit ? Math.random() > 0.18 : false; x.fillStyle = on ? ['#ffe9a8', '#bfe3ff', '#ffd1f0'][(r + q) % 3] : '#10162a'; x.fillRect(8 + q * 19, 8 + r * 15, 12, 9); }
  const t = new THREE.CanvasTexture(c); t.wrapS = t.wrapT = THREE.RepeatWrapping; return t;
}
const WIN = { lit: windowTexture(true), dark: windowTexture(false) };
function building(id, name, z, col) {
  const g = new THREE.Group(); g.position.set(52, 0, z); scene.add(g);
  const body = new THREE.Mesh(new THREE.BoxGeometry(9, 16, 9), new THREE.MeshStandardMaterial({ color: 0x0b1022, emissive: 0xffffff, emissiveMap: WIN.lit, emissiveIntensity: 0.8, metalness: 0.5, roughness: 0.4 })); body.position.y = 8; neon(body, col, 0.9); g.add(body);
  const top = new THREE.Mesh(new THREE.BoxGeometry(10, 0.8, 10), mat(col, 1.4)); top.position.y = 16.4; g.add(top);
  const spire = new THREE.Mesh(new THREE.CylinderGeometry(0.1, 0.35, 5, 8), mat(col, 2)); spire.position.y = 19; g.add(spire);
  const dome = new THREE.Mesh(new THREE.SphereGeometry(12.5, 28, 18), new THREE.MeshBasicMaterial({ color: 0xfbbf24, wireframe: true, transparent: true, opacity: 0.0 })); dome.position.y = 6; g.add(dome);
  glow(g.position, col, 9);
  const pl = new THREE.PointLight(col, 1.2, 28); pl.position.set(48, 10, z); scene.add(pl);
  ST[id] = { g, body, dome, top, flash: 0, locked: false, mode: 'ok', light: pl, col };
  station(id, name, new V3(52, 8, z), { in: new V3(47.2, 8, z), h: 24, label: new V3(52, 0, z + 7.2) });
}
building('c-demo', '🏦 STRIPE CONSUMER', -9, 0xb197fc);
building('c-shop', '🏪 SHOP CONSUMER', 7, 0x5eead4);

// --- graveyard
const GRAVE = { stones: [] };
{
  const g = new THREE.Group(); g.position.set(52, 0, 25); scene.add(g);
  const plot = new THREE.Mesh(new THREE.CylinderGeometry(8, 8.4, 0.4, 10), mat(0x5a2a3a, 0.35)); plot.position.y = 0.2; neon(plot, 0xff4d6d, 0.5); g.add(plot);
  glow(g.position, 0xff4d6d, 9); GRAVE.g = g;
  const pl = new THREE.PointLight(0xff4d6d, 0.9, 20); pl.position.set(50, 6, 25); scene.add(pl);
  station('grave', '🪦 DEAD LETTERS', new V3(52, 2, 25), { in: new V3(46.5, 3, 25), h: 8, label: new V3(52, 0, 31) });
}
function setStones(n) {
  n = clamp(n, 0, 18);
  while (GRAVE.stones.length < n) { const i = GRAVE.stones.length, s = new THREE.Mesh(new THREE.BoxGeometry(1.5, 2.6, 0.5), mat(0xff4d6d, 0.7)); neon(s, 0xffd0d8, 0.8);
    s.position.set(-5 + (i % 6) * 2, 1.7, -3 + Math.floor(i / 6) * 3); s.rotation.y = rnd(-0.3, 0.3); s.scale.set(1, 0.01, 1); GRAVE.g.add(s); GRAVE.stones.push({ m: s, t: 0 }); }
  while (GRAVE.stones.length > n) { const s = GRAVE.stones.pop(); GRAVE.g.remove(s.m); }
}

// --- light rails between stations (with pulses of light travelling along)
const PIPES = [];
function pipe(key, a, b, color, lift = 0) {
  const mid = a.clone().add(b).multiplyScalar(0.5); mid.y += lift;
  const curve = new THREE.QuadraticBezierCurve3(a, mid, b);
  const tube = new THREE.Mesh(new THREE.TubeGeometry(curve, 40, 0.13, 6, false), new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.32 }));
  scene.add(tube);
  const pulses = []; for (let i = 0; i < 4; i++) { const p = new THREE.Mesh(new THREE.SphereGeometry(0.3, 8, 8), new THREE.MeshBasicMaterial({ color, transparent: true, opacity: 0.9 })); scene.add(p); pulses.push({ m: p, o: i / 4 }); }
  PIPES.push({ key, curve, tube, pulses, color, heat: 0 });
}
['gun-stripe', 'gun-shopify', 'gun-evil'].forEach((id, i) => pipe('gun' + i, AN[id].mouth.clone(), new V3(-23.8, 7.5 + (i - 1) * 1.2, (i - 1) * 2), [0x6b57c9, 0x2aa196, 0xb83a52][i], 2));
pipe('edge-q', edgeOut(), AN.queue.in.clone(), 0x4a78d8, 1);
pipe('q-w', AN.queue.out.clone(), AN.worker.in.clone(), 0xd8884a, 1);
pipe('w-v', AN.worker.out.clone(), AN.vault.in.clone(), 0x4ad88f, 1);
pipe('v-d', AN.vault.out.clone(), AN.dispatcher.in.clone(), 0x4ad88f, 1.5);
pipe('d-c1', AN.dispatcher.out.clone(), AN['c-demo'].in.clone(), 0x7aa7ff, 6);
pipe('d-c2', AN.dispatcher.out.clone(), AN['c-shop'].in.clone(), 0x7aa7ff, 4);
pipe('d-g', AN.dispatcher.out.clone(), AN.grave.in.clone(), 0x8a4a4a, 1);
const heat = (k, v = 1) => { const p = PIPES.find((x) => x.key === k); if (p) p.heat = Math.min(1.5, p.heat + v); };

// ======================= packets =======================
const packets = [];
let heroPkt = null;
function makePacket(kind, color, ghost) {
  let geo;
  if (kind === 'evil') geo = new THREE.OctahedronGeometry(1.35, 0);
  else if (kind === 'ack') geo = new THREE.SphereGeometry(0.95, 16, 12);
  else if (kind === 'bag') geo = new THREE.BoxGeometry(1.9, 1.9, 1.0);
  else if (kind === 'letter') geo = new THREE.BoxGeometry(2.2, 1.4, 0.25);
  else geo = new THREE.BoxGeometry(2.4, 1.5, 0.4);
  const m = new THREE.Mesh(geo, new THREE.MeshStandardMaterial({ color: 0x10152b, emissive: color, emissiveIntensity: ghost ? 0.55 : 1.15, transparent: ghost, opacity: ghost ? 0.38 : 1, metalness: 0.3, roughness: 0.3 }));
  neon(m, ghost ? color : 0xffffff, ghost ? 0.5 : 0.85);
  if (kind === 'card' || kind === 'letter') { const stripe = new THREE.Mesh(new THREE.PlaneGeometry(2.0, 0.28), new THREE.MeshBasicMaterial({ color: 0xffffff, transparent: true, opacity: ghost ? 0.3 : 0.9 })); stripe.position.z = (kind === 'card' ? 0.21 : 0.14); m.add(stripe); }
  return m;
}
const KIND = { stripe: 'card', shopify: 'bag', malicious: 'evil' };
function fly(from, to, o = {}) {
  return new Promise((res) => {
    const m = makePacket(o.kind || 'card', o.color || HEX.mail, o.ghost); m.position.copy(from); scene.add(m);
    const pk = { m, a: from.clone(), b: to.clone(), arc: o.arc == null ? 3 : o.arc, t: 0, dur: Math.max(60, T(o.dur || 700)), res, color: o.color || HEX.mail, hover: !!o.hover, hero: !!o.hero, spin: rnd(0.8, 1.6) };
    packets.push(pk); if (pk.hero) heroPkt = pk;
  });
}
const hover = (p, o) => fly(p, p, { ...o, hover: true, arc: 0 });
function updatePackets(dt) {
  for (let i = packets.length - 1; i >= 0; i--) {
    const k = packets[i]; k.t += dt / k.dur; const t = Math.min(1, k.t), u = t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2;
    if (k.hover) { k.m.position.copy(k.a); k.m.position.y += Math.sin(k.t * 12) * 0.25; } else {
      const cx = (k.a.x + k.b.x) / 2, cy = (k.a.y + k.b.y) / 2 + k.arc, cz = (k.a.z + k.b.z) / 2, v = 1 - u;
      k.m.position.set(v * v * k.a.x + 2 * v * u * cx + u * u * k.b.x, v * v * k.a.y + 2 * v * u * cy + u * u * k.b.y, v * v * k.a.z + 2 * v * u * cz + u * u * k.b.z); }
    k.m.rotation.x += dt * 0.0012 * k.spin; k.m.rotation.y += dt * 0.0017 * k.spin;
    const p = k.m.position; for (let j = 0; j < 2; j++) emit(p.x + rnd(-.2, .2), p.y + rnd(-.2, .2), p.z + rnd(-.2, .2), rnd(-1, 1), rnd(-1, 1), rnd(-1, 1), k.color, 1.5, 520, 0);
    if (t >= 1) { scene.remove(k.m); k.m.traverse((c) => { if (c.geometry) c.geometry.dispose(); }); packets.splice(i, 1); if (heroPkt === k) heroPkt = null; k.res(k.b.clone()); }
  }
}

// ======================= camera director =======================
const cam = { theta: -0.28, phi: 1.1, radius: 98, target: new V3(11, 3, 9), drag: false, lx: 0, ly: 0, idle: 0, mix: 0, shake: 0, ridePos: new V3(), rideLook: new V3(), auto: true, hold: 0, shotUntil: 0, shotMix: 0, shotPos: new V3(), shotLook: new V3() };
canvas.addEventListener('pointerdown', (e) => { cam.drag = true; cam.lx = e.clientX; cam.ly = e.clientY; cam.idle = 0; canvas.setPointerCapture(e.pointerId); });
canvas.addEventListener('pointerup', () => { cam.drag = false; });
canvas.addEventListener('pointermove', (e) => { if (!cam.drag) return; cam.theta -= (e.clientX - cam.lx) * 0.005; cam.phi = clamp(cam.phi - (e.clientY - cam.ly) * 0.004, 0.35, 1.5); cam.lx = e.clientX; cam.ly = e.clientY; cam.idle = 0; });
canvas.addEventListener('wheel', (e) => { e.preventDefault(); cam.radius = clamp(cam.radius * (1 + e.deltaY * 0.001), 28, 160); cam.idle = 0; }, { passive: false });
const _p = new V3(), _l = new V3();
function updateCamera(dt, now) {
  cam.idle += dt; const orbitTheta = cam.theta + (cam.auto && cam.idle > 2500 ? Math.sin(now / 7000) * 0.22 : 0);
  _p.set(cam.target.x + cam.radius * Math.sin(cam.phi) * Math.sin(orbitTheta), cam.target.y + cam.radius * Math.cos(cam.phi), cam.target.z + cam.radius * Math.sin(cam.phi) * Math.cos(orbitTheta));
  _l.copy(cam.target);
  const riding = state.ride && heroPkt && !cam.drag; cam.hold = riding ? 2600 : Math.max(0, cam.hold - dt);
  cam.mix = lerp(cam.mix, (riding || cam.hold > 0) ? 1 : 0, 1 - Math.exp(-dt / 420));
  if (riding) { cam.ridePos.lerp(_tmp.copy(heroPkt.m.position).add(RIDE_OFF), 1 - Math.exp(-dt / 520)); cam.rideLook.lerp(_tmp.copy(heroPkt.m.position).add(RIDE_LOOK), 1 - Math.exp(-dt / 260)); }
  else if (cam.mix < 0.02) { cam.ridePos.copy(_p); cam.rideLook.copy(_l); }
  camera.position.lerpVectors(_p, cam.ridePos, cam.mix); _lk.lerpVectors(_l, cam.rideLook, cam.mix);
  if (cam.shake > 0.01) { camera.position.x += rnd(-1, 1) * cam.shake; camera.position.y += rnd(-1, 1) * cam.shake; cam.shake *= Math.exp(-dt / 160); }
  { const on = now < cam.shotUntil; cam.shotMix = lerp(cam.shotMix, on ? 1 : 0, 1 - Math.exp(-dt / 380)); if (cam.shotMix > 0.01) { camera.position.lerp(cam.shotPos, cam.shotMix); _lk.lerp(cam.shotLook, cam.shotMix); } }
  camera.lookAt(_lk);
}
const _tmp = new V3(), _lk = new V3(), RIDE_OFF = new V3(-13, 8.5, 23), RIDE_LOOK = new V3(6, 1, 0);
// the director: cut the camera to a key moment (a dead letter, a tripped breaker) so nobody has to hunt for it
function director(pos, look, ms) { cam.shotPos.copy(pos); cam.shotLook.copy(look); cam.shotUntil = performance.now() + ms; }
const shake = (s = 0.9) => { cam.shake = Math.max(cam.shake, s); };
function resetCamera() { cam.theta = -0.28; cam.phi = 1.1; cam.radius = 98; cam.idle = 3000; }

// ======================= animation of the stations =======================
let tPrev = performance.now();
function frame(now) {
  const dt = Math.min(60, now - tPrev); tPrev = now; const s = dt / 1000;
  // guns: recoil
  ['gun-stripe', 'gun-shopify', 'gun-evil'].forEach((id) => { const q = ST[id]; q.recoil = Math.max(0, q.recoil - dt / 260); q.barrel.position.x = 1.2 - q.recoil * 1.6; q.core.rotation.y += s * 2; q.core.rotation.x += s * 1.3; q.light.intensity = 1.2 + q.recoil * 4; q.rg.rotation.z += s * 0.8; });
  // edge: ring pulses and the scanning bar
  const E = ST.edge; E.flash = Math.max(0, E.flash - dt / 700); const fc = new THREE.Color(E.flashColor), base = new THREE.Color(0x4ad0ff);
  E.ringM.material.color.copy(base).lerp(fc, Math.min(1, E.flash * 1.4)); E.ring2.rotation.x += s * 0.5; E.field.material.opacity = 0.07 + E.flash * 0.4; E.light.intensity = 1.6 + E.flash * 5;
  E.scan = Math.max(0, E.scan - dt); E.bar.material.opacity = E.scan > 0 ? 0.9 : 0; if (E.scan > 0) E.bar.position.y = 7.5 + Math.sin((1 - E.scan / E.scanMax) * Math.PI * 2) * 6.5;
  // worker: gears (fast while busy, still while paused)
  const W = ST.worker; W.busy = Math.max(0, W.busy - dt); const spd = state.paused ? 0 : (W.busy > 0 ? 4.2 : 0.55); W.speed = lerp(W.speed, spd, 1 - Math.exp(-dt / 220));
  W.big.rotation.z -= W.speed * s; W.small.rotation.z += W.speed * s * 1.6; W.light.intensity = state.paused ? 0.15 : 1.0 + (W.busy > 0 ? 1.4 : 0);
  // vault discs pop in
  VAULT.flash = Math.max(0, VAULT.flash - dt / 600); ST.vault.light.intensity = 1.2 + VAULT.flash * 4;
  VAULT.discs.forEach((d) => { if (d.t < 1) { d.t = Math.min(1, d.t + dt / 500); const e = 1 - Math.pow(1 - d.t, 3); d.m.scale.set(e, 1, e); } });
  GRAVE.stones.forEach((st2) => { if (st2.t < 1) { st2.t = Math.min(1, st2.t + dt / 600); st2.m.scale.set(1, 1 - Math.pow(1 - st2.t, 3) * 0 - (1 - st2.t) * 0.99, 1); } });
  // dispatcher
  const D = ST.dispatcher; D.busy = Math.max(0, D.busy - dt); D.core.rotation.y += s * (D.busy > 0 ? 5 : 1); D.orbit.rotation.z += s * (D.busy > 0 ? 4 : 0.9); D.core.position.y = 6 + Math.sin(now / 700) * 0.4; D.light.intensity = 1.1 + (D.busy > 0 ? 1.5 : 0);
  // consumers: windows show health, domes show the breaker
  ['c-demo', 'c-shop'].forEach((id) => {
    const c = ST[id]; c.flash = Math.max(0, c.flash - dt / 650); const m = c.body.material;
    let target = state.mode === 'down' ? WIN.dark : WIN.lit; if (m.emissiveMap !== target) { m.emissiveMap = target; m.needsUpdate = true; }
    m.emissiveIntensity = (state.mode === 'down' ? 0.15 : state.mode === 'flaky' ? 0.35 + 0.5 * (Math.sin(now / 90) > 0 ? 1 : 0.15) : state.mode === 'slow' ? 0.5 + 0.25 * Math.sin(now / 500) : 0.8) + c.flash * 1.2;
    c.light.intensity = 1 + c.flash * 4; c.dome.material.opacity = lerp(c.dome.material.opacity, c.locked ? 0.34 : 0, 1 - Math.exp(-dt / 250)); c.dome.rotation.y += s * 0.4;
    c.top.material.emissiveIntensity = 1.4 + c.flash * 3;
  });
  // queue rings colour = transport; capsules inside the tube
  const Q = ST.queue, down = state.crash && state.crash.phase === 'down', qc = down ? 0x4a0d0d : qcol(state.broker); Q.rings.forEach((r) => r.material.color.setHex(qc));
  const want = Q.target; while (Q.caps.length < want) { const c = new THREE.Mesh(new THREE.BoxGeometry(0.36, 1.3, 1.3), new THREE.MeshStandardMaterial({ color: 0x1a0e05, emissive: qc, emissiveIntensity: 2 })); c.userData.s = 0; c.position.set(TUBE.x0 + 0.5, TUBE.y, rnd(-0.4, 0.4)); scene.add(c); Q.caps.push(c); }
  while (Q.caps.length > want) scene.remove(Q.caps.pop());
  Q.caps.forEach((c, i) => { const tx = TUBE.x1 - 0.7 - i * 0.46; c.position.x = lerp(c.position.x, tx, 1 - Math.exp(-dt / 160)); c.position.y = TUBE.y + (state.paused && Q.target > 3 ? Math.sin(now / 120 + i) * 0.08 : 0); c.material.emissive.setHex(qc); c.material.emissiveIntensity = state.paused ? 1.2 + 0.8 * Math.sin(now / 200 + i * 0.4) : 2; });
  // rails: pulses of light
  PIPES.forEach((p) => { p.heat = Math.max(0, p.heat - dt / 2200); p.tube.material.opacity = 0.2 + p.heat * 0.5;
    p.pulses.forEach((q) => { q.o = (q.o + s * (0.07 + p.heat * 0.5)) % 1; q.m.position.copy(p.curve.getPoint(q.o)); q.m.material.opacity = 0.35 + p.heat * 0.6; q.m.scale.setScalar(0.8 + p.heat * 1.2); }); });
  // rings and bolts
  for (let i = rings.length - 1; i >= 0; i--) { const r = rings[i]; r.t += dt; const k = r.t / r.life; if (k >= 1) { scene.remove(r.m); rings.splice(i, 1); continue; }
    r.m.scale.setScalar(0.5 + k * r.size); r.m.material.opacity = (1 - k) * 0.95; if (r.orient === 'cam') r.m.lookAt(camera.position); }
  for (let i = bolts.length - 1; i >= 0; i--) { const b = bolts[i]; b.t += dt; if (b.t > b.life) { scene.remove(b.l); bolts.splice(i, 1); continue; }
    const a = b.l.geometry.attributes.position.array; let x = b.p.x + rnd(-1, 1), y = b.p.y + 22, z = b.p.z + rnd(-1, 1); for (let j = 0; j < 30; j++) { a[j * 3] = x; a[j * 3 + 1] = y; a[j * 3 + 2] = z; x += rnd(-2.2, 2.2); y -= 22 / 29; z += rnd(-2.2, 2.2); }
    b.l.geometry.attributes.position.needsUpdate = true; b.l.material.opacity = 1 - b.t / b.life; }
  if (Math.random() < dt / 55) { const ids = ['gun-stripe', 'gun-shopify', 'gun-evil', 'edge', 'queue', 'worker', 'vault', 'dispatcher', 'c-demo', 'c-shop', 'grave'], id = ids[Math.floor(Math.random() * ids.length)], a = AN[id].c, col = ST[id] && ST[id].col ? ST[id].col : [0x4ad0ff, 0xff8a3d, 0xfbbf24, 0x4ade80, 0x7aa7ff][Math.floor(Math.random() * 5)];
    emit(a.x + rnd(-4, 4), 0.4, a.z + rnd(-4, 4), rnd(-0.4, 0.4), rnd(1.2, 3.2), rnd(-0.4, 0.4), col, rnd(0.8, 1.6), rnd(2200, 4200), 0); }
  updatePackets(dt); updateParticles(dt); updateCamera(dt, now); updateOverlays(dt);
  // sleeping worker snores, a downed consumer smokes
  if (state.paused && Math.random() < dt / 700) popText(new V3(17, 12, 0), 'z', 0x8aa0d0, 1600);
  if (state.mode === 'down') ['c-demo', 'c-shop'].forEach((id) => { if (Math.random() < dt / 120) burst(new V3(52 + rnd(-3, 3), 17, AN[id].c.z + rnd(-3, 3)), 1, { color: 0x556070, speed: 1.2, dir: new V3(0, 1, 0), spread: 0.4, life: 1800, size: 3, g: 0.4 }); });
  composer.render();
  requestAnimationFrame(frame);
}

// ======================= sound (off until you turn it on) =======================
let actx = null;
function beep(f, dur = 0.08, type = 'square', vol = 0.04, slide = 0) {
  if (!state.sound) return; actx = actx || new (window.AudioContext || window.webkitAudioContext)();
  const t = actx.currentTime, o = actx.createOscillator(), g = actx.createGain(); o.type = type; o.frequency.setValueAtTime(f, t);
  if (slide) o.frequency.exponentialRampToValueAtTime(Math.max(30, f + slide), t + dur);
  g.gain.setValueAtTime(vol, t); g.gain.exponentialRampToValueAtTime(0.0001, t + dur); o.connect(g).connect(actx.destination); o.start(t); o.stop(t + dur);
}
const SFX = { pew: () => beep(900, 0.09, 'square', 0.035, -650), clang: () => { beep(190, 0.2, 'sawtooth', 0.05, -110); beep(95, 0.22, 'square', 0.04); }, thud: () => beep(120, 0.1, 'sine', 0.07, -60),
  ding: () => { beep(880, 0.08, 'triangle', 0.04); setTimeout(() => beep(1320, 0.12, 'triangle', 0.04), 70); }, buzz: () => beep(130, 0.16, 'sawtooth', 0.04), zap: () => beep(1600, 0.22, 'sawtooth', 0.035, -1400) };
function sfx(n) { const t = performance.now(); if (t - (state.lastSfx[n] || 0) < 45) return; state.lastSfx[n] = t; SFX[n](); }

// ======================= log, score, trace =======================
function log(text, cls, link) {
  const box = $('#log'), d = document.createElement('div'), t = new Date().toLocaleTimeString([], { hour12: false });
  d.className = cls || ''; d.innerHTML = `<span class="dim">${t}</span> ` + (link ? `<a href="${link}" target="_blank">${text}</a>` : text);
  box.appendChild(d); while (box.children.length > 100) box.removeChild(box.firstChild); box.scrollTop = box.scrollHeight;
}
function addScore(k, n = 1) { state.score[k] += n; const el = $('#c-' + k); el.querySelector('.n').textContent = state.score[k].toLocaleString(); el.classList.remove('pop'); void el.offsetWidth; el.classList.add('pop'); }
const cards = {};
function traceCard(title, color) {
  const box = $('#trace'); if (box.firstElementChild && !box.firstElementChild.classList.contains('tcard')) box.innerHTML = '';
  const el = document.createElement('div'); el.className = 'tcard'; el.innerHTML = `<div class="th" style="color:${CSS(color)}">${title}</div><ol></ol>`;
  box.prepend(el); while (box.children.length > 5) box.lastElementChild.remove();
  const steps = {};
  return { head: (t) => { el.querySelector('.th').textContent = t; },
    step(key, text, st = 'run', link) { let li = steps[key]; if (!li) { li = steps[key] = document.createElement('li'); el.querySelector('ol').appendChild(li); }
      li.className = st; li.innerHTML = ({ ok: '✓ ', bad: '✖ ', warn: '⚠ ', run: '◌ ' })[st] + (link ? `<a href="${link}" target="_blank">${text}</a>` : text); } };
}

// ======================= choreography: slow, narrated, driven by real results =======================
let PACE = 1, active = 0;
const T = (ms) => ms * PACE * (active > 10 ? 0.2 : 1);
const quiet = () => active > 10;
const OUT = (s) => ({ 200: 'accepted', 401: 'rejected', 404: 'unknown_source', 429: 'rate_limited', 503: 'unavailable', 0: 'unavailable' }[s] || 'rejected');
const adminLink = (id) => '/admin/events/event/' + id + '/change/';
const num = (id, t) => { const e = $('#n-' + id); if (e) e.textContent = t; };

function edgeFlash(color, scanMs = 0) { ST.edge.flash = 1; ST.edge.flashColor = color; if (scanMs) { ST.edge.scan = ST.edge.scanMax = T(scanMs); } }

async function gate(shot, pos, rec) {
  const e = AN.edge.c, outcome = OUT(shot.status), card = rec.card, accepted = outcome === 'accepted' || outcome === 'quarantined';
  say('edge', '🔍 verifying HMAC-SHA256 on the raw bytes…', 'run'); card?.step('sig', 'Edge: verifying the signature on the raw request bytes', 'run');
  edgeFlash(0x5eead4, 950); await hover(pos, { kind: rec.kind, color: rec.color, ghost: rec.ghost, dur: 950, hero: rec.hero });
  if (!accepted) {
    edgeFlash(HEX.bad); shake(1.3); const hit = pos.clone();
    ring(hit, HEX.bad, { size: 16, life: 700, orient: 'x' }); ring(hit, 0xffffff, { size: 9, life: 450, orient: 'x' });
    burst(hit, 70, { color: HEX.bad, speed: 16, dir: new V3(-1, 0.3, 0), spread: 0.6, life: 1100, size: 1.6, g: -9 }); burst(hit, 24, { color: 0xffffff, speed: 10, life: 700, size: 1 });
    const why = { rejected: ['✖ signature does not match', '401 · nothing stored, nothing queued'], unknown_source: ['✖ no such source', '404'], rate_limited: ['⏱ rate limit exceeded', '429 · retry in 1 s (Retry-After)'], unavailable: ['⚠ the queue could not confirm', '503 · the provider will retry, nothing is lost'] }[outcome] || ['✖ refused', String(shot.status)];
    say('edge', why[0], 'bad'); popText(hit.clone().add(new V3(-2, 3, 0)), why[1], HEX.bad, 2000); sfx(outcome === 'unavailable' ? 'buzz' : 'clang');
    card?.step('sig', outcome === 'rejected' ? `Edge: signature does NOT match${rec.label ? ' (' + rec.label + ')' : ''}: rejected` : why[0].slice(2), 'bad');
    card?.step('end', `${why[1]} (answered in ${shot.ms} ms real time)`, 'bad'); await sleep(T(800)); return false;
  }
  edgeFlash(HEX.ok); ring(AN.edge.c, HEX.ok, { size: 20, life: 800, orient: 'x' }); say('edge', '✓ signature valid', 'ok'); card?.step('sig', 'Edge verified the signature on the raw bytes (constant-time compare)', 'ok'); await sleep(T(500));
  if (shot.source === 'stripe-a') { say('edge', '🕒 timestamp inside the 5-minute window ✓', 'ok'); card?.step('ts', 'Signed timestamp is inside the 5-minute replay window', 'ok'); await sleep(T(550)); }
  else card?.step('ts', 'Shopify-style signature has no timestamp: duplicates are caught by event id and body hash instead', 'ok');
  if (outcome === 'quarantined') { say('edge', '🔒 stale timestamp: held, never delivered', 'warn'); card?.step('ts', 'Timestamp is stale but the signature is valid: held (quarantined), never delivered', 'warn'); }
  say('edge', '📨 publishing to the durable queue… waiting for the broker to confirm', 'run'); sfx('thud'); heat('edge-q');
  await fly(edgeOut().add(new V3(-2, 0, 0)), AN.queue.in.clone(), { kind: rec.kind, color: rec.color, ghost: rec.ghost, dur: 1000, arc: 2, hero: rec.hero });
  ring(AN.queue.in, HEX.rabbit, { size: 8, life: 600, orient: 'x' }); say('queue', '✓ broker confirmed: the message is durable (survives a crash)', 'ok');
  card?.step('queue', `Queued durably: broker confirmed, THEN the edge answered 200 (${shot.ms} ms real time)`, 'ok'); $('#real-ms').textContent = shot.ms + ' ms';
  const gunId = GUN[rec.gun] || 'gun-stripe';
  fly(AN.edge.c.clone().add(new V3(-3, 0, 0)), AN[gunId].mouth.clone().add(new V3(0, 2, 0)), { kind: 'ack', color: HEX.ok, dur: 1100, arc: 7 }).then((p) => { popText(p, '200 OK', HEX.ok, 1500); ring(p, HEX.ok, { size: 6, life: 500 }); });
  await sleep(T(450)); return true;
}

const chains = {};
function enqueue(key, fn) { chains[key] = (chains[key] || Promise.resolve()).then(fn).catch((e) => console.error(e)); }

async function stageStored(ev) {
  window.__range.stats.stored++;
  const card = cards[ev.provider_id || ev.id], W = ST.worker, H = !!(card && card.isHero);
  active++; try {
    say('queue', '↳ the ingest worker takes the next message', 'run', 900); heat('q-w');
    await fly(AN.queue.out.clone(), AN.worker.in.clone(), { kind: 'card', color: 0xffb066, dur: 900, arc: 1.5, hero: H });
    W.busy = T(2600); say('worker', '🔎 dedup: have we seen this event id before? → new', 'run'); burst(AN.worker.c, 24, { color: 0xfbbf24, speed: 10, life: 800 });
    card?.step('dedup', `Ingest dedup: ${ev.provider_id ? 'event id ' + ev.provider_id.slice(0, 18) : 'body hash'} not seen before`, 'ok');
    const mid = AN.worker.c.clone(); await hover(mid, { kind: 'card', color: 0xffb066, dur: 800, hero: H });
    say('worker', '💾 BEGIN → save event + delivery rows → COMMIT', 'run'); await hover(mid, { kind: 'card', color: 0x7dffb0, dur: 850, hero: H });
    heat('w-v'); await fly(AN.worker.out.clone(), AN.vault.topOf().add(new V3(0, 3, 0)), { kind: 'card', color: 0x7dffb0, dur: 900, arc: 3, hero: H });
    const v = AN.vault.topOf(); VAULT.flash = 1; ring(v, ev.status === 'quarantined' ? HEX.warn : HEX.ok, { size: 12, life: 700, orient: 'ground' }); burst(v, 26, { color: HEX.ok, speed: 9, dir: new V3(0, 1, 0), spread: 0.7, life: 900 }); sfx('thud');
    state.events++; setVaultLayers(3 + Math.floor(state.events / 2));
    say('vault', ev.status === 'quarantined' ? '🔒 held · never delivered' : `✓ stored · event ${ev.short}`, ev.status === 'quarantined' ? 'warn' : 'ok');
    card?.step('stored', ev.status === 'quarantined' ? `Stored but quarantined (event ${ev.short})` : `Stored in Postgres (event ${ev.short}); event + delivery row in ONE transaction, only then acked to the queue`, ev.status === 'quarantined' ? 'warn' : 'ok', adminLink(ev.id));
    await sleep(T(500));
  } finally { active--; }
}

async function stageDup(e, fresh) {
  const card = cards[(e.provider_id || e.id) + '#dup'], W = ST.worker;
  active++; try {
    for (let i = 0; i < Math.min(fresh, 3); i++) {
      await fly(AN.queue.out.clone(), AN.worker.in.clone(), { kind: 'card', color: HEX.warn, ghost: true, dur: 900, arc: 1.5 }); W.busy = T(2000);
      say('worker', '🔎 dedup: this event id was already stored → duplicate', 'warn'); card?.step('dedup', 'Ingest dedup: this event id was ALREADY stored: duplicate', 'warn');
      await hover(AN.worker.c.clone(), { kind: 'card', color: HEX.warn, ghost: true, dur: 850 });
      await fly(AN.worker.out.clone(), AN.vault.topOf().add(new V3(0, 3, 0)), { kind: 'card', color: HEX.warn, ghost: true, dur: 800, arc: 4 });
      const v = AN.vault.topOf(); burst(v, 40, { color: HEX.warn, speed: 7, life: 1100, size: 1.8, g: 2 }); ring(v, HEX.warn, { size: 11, life: 700, orient: 'ground' }); popText(v.clone().add(new V3(0, 4, 0)), 'DUPLICATE ✋', HEX.warn, 1800); sfx('buzz');
      say('vault', '✋ duplicate dropped: the consumer will get it ONCE', 'warn');
      card?.step('end', 'Dropped. Nothing was delivered twice because of this repeat (a counter is bumped instead)', 'warn', adminLink(e.id)); addScore('dups'); await sleep(T(600));
    }
  } finally { active--; }
}

async function stageAttempt(a) {
  window.__range.stats.attempts++;
  const D = ST.dispatcher, id = ST[a.consumer] ? a.consumer : 'c-demo', B = ST[id], A2 = AN[id], ok = a.status && a.status < 300, card = cards[a.provider_id], H = !!(card && card.isHero);
  active++; try {
    if (a.number === 1 && !a.round) {
      // Postgres sends nothing: the delivery worker keeps asking it "is anything due?" and claims what it finds (the outbox)
      say('dispatcher', '🔎 asks Postgres: "any deliveries due?"', 'run'); heat('v-d');
      await fly(AN.dispatcher.c.clone().add(new V3(0, 3, -2)), AN.vault.out.clone().add(new V3(0, 2, 0)), { kind: 'ack', color: 0x7aa7ff, dur: 800, arc: 2 });
      ring(AN.vault.out, 0x7aa7ff, { size: 8, life: 600, orient: 'ground' }); say('vault', '📋 outbox row: 1 delivery due (saved with the event, same transaction)', 'ok'); card?.step('claim', 'Delivery worker asked Postgres "anything due?" and claimed this delivery (the outbox: written in the same transaction as the event)', 'ok');
      await sleep(T(700));
      await fly(AN.vault.out.clone(), AN.dispatcher.in.clone(), { kind: 'letter', color: 0x7dffb0, dur: 900, arc: 2, hero: H }); say('dispatcher', '✓ claimed: marked in-flight so no other worker sends it', 'ok'); await sleep(T(500));
    }
    say('dispatcher', `claim delivery → sign → POST (attempt ${a.number})`, 'run'); D.busy = T(1600); heat(id === 'c-shop' ? 'd-c2' : 'd-c1'); await sleep(T(500));
    card?.step('d' + a.number + 'a', `Delivery attempt ${a.number} → ${a.consumer}: signed POST with the event id and attempt number`, 'run');
    await fly(AN.dispatcher.out.clone(), A2.in.clone(), { kind: 'letter', color: ok ? HEX.ok : HEX.mail, dur: 1200, arc: 5, hero: H });
    const c = A2.c.clone();
    if (ok) {
      B.flash = 1; ring(c, HEX.ok, { size: 18, life: 800, orient: 'ground' }); ring(A2.in, HEX.ok, { size: 9, life: 600, orient: 'x' });
      for (let k = 0; k < 3; k++) burst(c.clone().add(new V3(0, 17, 0)), 40, { color: [HEX.ok, HEX.warn, 0x7aa7ff, 0xff6bcb][(k + Math.floor(Math.random() * 4)) % 4], speed: 14, dir: new V3(0, 1, 0), spread: 0.55, life: 1700, size: 1.5, g: -9 });
      popText(c.clone().add(new V3(0, 20, 0)), '200 ✓ delivered', HEX.ok, 1800); sfx('ding'); say(id, '✓ 200: processed', 'ok'); addScore('delivered');
      card?.step('d' + a.number, `Attempt ${a.number} → ${a.consumer}: 200 ✓ DELIVERED${a.round ? ' (replay)' : ''}`, 'ok', adminLink(a.event)); await sleep(T(600));
    } else {
      B.flash = 1; ring(A2.in, HEX.bad, { size: 12, life: 700, orient: 'x' }); burst(A2.in, 30, { color: 0x77808f, speed: 5, dir: new V3(0, 1, 0), spread: 0.6, life: 1800, size: 3, g: 0.5 }); burst(A2.in, 18, { color: HEX.bad, speed: 11, life: 700 });
      popText(c.clone().add(new V3(0, 20, 0)), a.status ? a.status + ' ✖' : 'no answer', HEX.bad, 1800); sfx('buzz'); say(id, a.status ? `✖ ${a.status}: failed` : '✖ no answer', 'bad'); addScore('retries'); shake(0.5);
      card?.step('d' + a.number, `Attempt ${a.number} → ${a.consumer}: ${a.status || 'no answer'} ✖. Retry later: growing wait + jitter, never dropped`, 'bad', adminLink(a.event));
      await fly(A2.in.clone(), AN.dispatcher.out.clone(), { kind: 'letter', color: HEX.bad, ghost: true, dur: 900, arc: 5, hero: H });
      say('dispatcher', '⏳ backoff: wait, then try again', 'warn'); popText(AN.dispatcher.c.clone().add(new V3(0, 6, 0)), 'retry ↻', HEX.warn, 1600); await sleep(T(500));
    }
  } finally { active--; }
}

// ---- webhooks fired from this page: a full narrated journey each ----
async function journeyUI(rec) {
  active++; try {
    await sleep(T(rec.delay)); const g = AN[GUN[rec.gun]]; sfx('pew'); const gs = ST[GUN[rec.gun]]; gs.recoil = 1; burst(g.mouth, 28, { color: HEX[rec.gun], speed: 12, dir: new V3(1, 0.2, 0), spread: 0.5, life: 600 }); ring(g.mouth, HEX[rec.gun], { size: 6, life: 450, orient: 'x' });
    const pos = await fly(g.mouth.clone(), edgeIn(), { kind: KIND[rec.gun], color: HEX[rec.gun], ghost: rec.ghost, dur: 1300, arc: rnd(3, 7), hero: rec.hero });
    const res = await rec.resP, shot = res.shots[rec.index]; if (!shot) return;
    if (shot.ref) { cards[shot.ref + (shot.duplicate ? '#dup' : '')] = rec.card; rec.card.head(`${rec.title} · ${shot.ref.slice(0, 20) || shot.label}`); } else rec.card.head(`${rec.title} (${shot.label})`);
    rec.label = shot.label; rec.source = shot.source; rec.kind = KIND[rec.gun]; rec.color = HEX[rec.gun];
    await gate(shot, pos, rec);
  } finally { active--; rec.done(); }
}
async function journeyExternal(it) {
  active++; try {
    const pos = await fly(AN.cloud.c.clone(), edgeIn(), { kind: 'card', color: HEX.mail, dur: 1100, arc: -4 });
    await gate({ status: it.status, ms: '–', source: it.source, ref: '' }, pos, { kind: 'card', color: HEX.mail, gun: it.source === 'shop-a' ? 'shopify' : 'stripe' });
  } finally { active--; }
}
function onEdgeItem(it) {
  const cls = it.outcome === 'accepted' ? 'ok' : it.outcome === 'quarantined' ? 'warn' : 'bad';
  log(`edge ← ${it.source}: ${it.status} ${it.outcome}` + (it.broker ? ` [${it.broker}]` : ''), cls); num('edge', `${it.status} ${it.outcome}`);
  addScore('fired'); if (it.outcome === 'rejected') addScore('blocked');
  if (it.tag && state.tags.has(it.tag)) return;
  journeyExternal(it);
}

// ======================= firing =======================
let tagN = 0;
async function fire(gun) {
  const count = Math.max(1, Math.min(30, +$('#opt-count').value || 1)), dup = $('#opt-dup').checked && gun !== 'malicious';
  const total = count * (dup ? 2 : 1), tag = `ui${Date.now()}-${tagN++}`; state.tags.add(tag);
  { const tp = $('#p-trace'); if (tp.classList.contains('closed')) { tp.classList.remove('closed'); tp.querySelector('.collapse').textContent = '–'; } }
  const resP = fetch('/play/api/fire', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ gun, count, duplicate: dup, broker: state.broker, tag }) })
    .then((r) => r.json()).catch((e) => { log('could not reach the gateway: ' + e, 'bad'); return { shots: [] }; });
  const visual = Math.min(total, PACE >= 1 ? 6 : 14), title = { stripe: '💳 Stripe webhook', shopify: '🛍️ Shopify webhook', malicious: '☠️ fake webhook' }[gun], spacing = PACE >= 1 ? 800 : 320;
  for (let i = 0; i < visual; i++) {
    const card = traceCard(title, HEX[gun]); card.isHero = i === 0; card.step('fire', gun === 'malicious' ? 'Fired: forged, tampered or unsigned on purpose' : gun === 'stripe' ? 'Fired & signed: t=timestamp, v1=HMAC-SHA256(secret, "timestamp.body")' : 'Fired & signed: base64 HMAC-SHA256(secret, body), plus a webhook id header', 'ok');
    let done; const finished = new Promise((r) => { done = r; });
    journeyUI({ gun, index: i, ghost: dup && i % 2 === 1, resP, card, title, delay: i * spacing, done, hero: i === 0 });
    resP.then((res) => { const s = res.shots[i]; if (s && s.ref) chains[s.ref + (s.duplicate ? '#dup' : '')] = finished; });
  }
  setTimeout(poll, 120);
}

// ======================= polling the real system =======================
const HOUSE = { 'c-demo': 1, 'c-shop': 1 };
async function poll() {
  const c = state.cursors, qs = new URLSearchParams();
  if (c.edge != null) qs.set('edge', c.edge); if (c.attempt != null) qs.set('attempt', c.attempt);
  if (c.event_ts) qs.set('event_ts', c.event_ts); if (c.dup_ts) qs.set('dup_ts', c.dup_ts);
  let d; try { d = await (await fetch('/play/api/poll?' + qs)).json(); } catch (e) { return; }
  state.cursors = d.cursors; d.edge.items.slice(-40).forEach(onEdgeItem);
  d.events.forEach((ev) => { log(`stored ${ev.short} ${ev.source} ${ev.type || ''} ${ev.provider_id ? '(' + ev.provider_id.slice(0, 14) + ')' : ''}` + (ev.status === 'quarantined' ? ' QUARANTINED' : ''), ev.status === 'quarantined' ? 'warn' : 'ok', adminLink(ev.id)); enqueue(ev.provider_id || ev.id, () => stageStored(ev)); });
  d.duplicates.forEach((e) => { const seen = state.dupSeen[e.id] || 0, fresh = e.count - seen; state.dupSeen[e.id] = e.count; if (fresh <= 0) return;
    log(`duplicate absorbed (${(e.provider_id || 'body hash').slice(0, 16)}) ×${e.count}: delivered once only`, 'warn', adminLink(e.id)); enqueue((e.provider_id || e.id) + '#dup', () => stageDup(e, fresh)); });
  d.attempts.filter((a) => a.done).slice(0, 40).forEach((a) => { log(`deliver ${a.provider_id ? a.provider_id.slice(0, 14) : a.event.slice(0, 8)} → ${a.consumer} attempt ${a.number}` + (a.round ? ` (replay #${a.round})` : '') + `: ${a.status || a.error || 'no answer'}`, a.status && a.status < 300 ? 'ok' : 'bad', adminLink(a.event)); enqueue(a.provider_id || a.event, () => stageAttempt(a)); });

  const s = d.snapshot, dead = Object.values(s.dead_by_consumer).reduce((x, y) => x + y, 0), depth = d.queue[state.broker];
  ST.queue.target = depth == null ? 0 : Math.min(depth, 34);
  if (depth != null) state.lastDepth[state.broker] = depth;
  num('queue', depth == null ? '💥 BROKER DOWN' : `${depth} waiting · ${TN[state.broker]}`);
  crashWatch(depth);
  num('vault', `${s.events_total} events stored`); num('grave', `${dead} dead`); num('dispatcher', s.in_flight ? `${s.in_flight} in flight` : 'retry · backoff');
  if (state.events < s.events_total && !active) { state.events = s.events_total; setVaultLayers(3 + Math.floor(state.events / 2)); }
  if (dead > state.dead) { director(new V3(40, 13, 46), new V3(52, 2, 25), 5200); for (let i = 0; i < Math.min(4, dead - state.dead); i++) fly(AN.dispatcher.out.clone(), AN.grave.in.clone(), { kind: 'letter', color: HEX.bad, dur: 1600, arc: 6, delay: i * 200 }).then((p) => { ring(p, HEX.bad, { size: 10, life: 800, orient: 'x' }); bolt(AN.grave.c, HEX.bad); });
    burst(AN.grave.c, 40, { color: 0x888890, speed: 4, dir: new V3(0, 1, 0), spread: 0.6, life: 1800, size: 3, g: 0.3 }); popText(AN.grave.c.clone().add(new V3(0, 6, 0)), 'dead letter 🪦', HEX.bad, 1800); say('grave', 'every retry used up: kept forever, never dropped, replayable', 'bad', 3200); sfx('buzz'); }
  if (dead < state.dead) { say('grave', '↻ replayed: same event ids, flagged as replays', 'ok'); for (let i = 0; i < Math.min(6, state.dead - dead); i++) fly(AN.grave.c.clone().add(new V3(0, 3, 0)), AN.dispatcher.c.clone().add(new V3(0, 3, 0)), { kind: 'ack', color: 0xc9b6ff, ghost: true, dur: 1500, arc: 8 }); }
  state.dead = dead; setStones(dead);
  Object.keys(HOUSE).forEach((name) => {
    const st = s.breaker[name] || 'closed', prev = state.breaker[name] || 'closed', B = ST[name], c = AN[name].c;
    B.locked = st !== 'closed';
    if (prev === 'closed' && st !== 'closed') { director(new V3(28, 17, 30), c.clone().add(new V3(0, 6, 0)), 4600); bolt(c.clone().add(new V3(-3, 0, 0))); bolt(c.clone().add(new V3(3, 0, 2)), 0xffffff); shake(1.4); popText(c.clone().add(new V3(0, 22, 0)), '🔒 BREAKER OPEN', HEX.warn, 2200); say(name, '🔒 5 failures in a row: paused. Events wait, none dropped', 'warn', 2800); sfx('zap'); log(`circuit breaker OPEN on ${name}: deliveries paused, events wait`, 'warn'); }
    if (prev !== 'closed' && st === 'closed') { ring(c, HEX.ok, { size: 22, life: 900, orient: 'ground' }); burst(c.clone().add(new V3(0, 8, 0)), 40, { color: HEX.ok, speed: 10 }); popText(c.clone().add(new V3(0, 22, 0)), '🔓 breaker closed', HEX.ok, 1800); say(name, '🔓 a probe succeeded: deliveries resume', 'ok'); sfx('ding'); log(`circuit breaker closed on ${name}: deliveries resume`, 'ok'); }
    state.breaker[name] = st;
  });
  state.paused = d.controls.ingest_paused; $('#sw-ingest').textContent = 'ingest worker: ' + (state.paused ? 'OFF' : 'ON'); $('#sw-ingest').classList.toggle('on', state.paused);
  num('worker', state.paused ? '💤 paused: events wait' : 'dedup · store');
  state.mode = d.controls.consumer_mode; document.querySelectorAll('#seg-consumer button').forEach((b) => b.classList.toggle('on', b.dataset.v === state.mode));
  Object.keys(HOUSE).forEach((id) => { if (!ST[id].locked) num(id, ({ ok: 'healthy', flaky: 'flaky', slow: 'slow ⏳', down: 'down 💥' })[state.mode]); else num(id, 'paused 🔒'); });
  const stat = (l, v) => `<span>${l} <b>${v}</b></span>`;
  $('#stats').innerHTML = stat('events/min', s.events_last_minute) + stat('deliveries due', s.queue_depth) + stat('in flight', s.in_flight) + stat('p95 receive→delivered', s.delivery_p95_latency_seconds_15m == null ? '–' : s.delivery_p95_latency_seconds_15m + 's') + stat('alerts open', s.open_alerts) + stat('RabbitMQ', d.queue.rabbitmq ?? '–') + stat('Redis', d.queue.redis ?? '–') + stat('Redis·volatile', d.queue.redis_volatile ?? '–');
  $('#btn-crash').style.display = d.controls.chaos ? '' : 'none';
}

// ======================= crash the queue: a real kill -9 on the selected broker =======================
$('#btn-crash').onclick = async () => {
  if (state.crash) return;
  const t = state.broker, before = state.lastDepth[t] || 0, c = AN.queue.c.clone();
  state.crash = { t, before, phase: 'killing', since: performance.now(), sawDown: false };
  say('queue', `💥 kill -9 on ${TN[t]}…`, 'bad', 3200); shake(2.4); sfx('zap'); director(new V3(-26, 15, 36), c, 7000);
  ring(c, HEX.bad, { size: 28, life: 900, orient: 'x' }); burst(c, 110, { color: HEX.bad, speed: 20, life: 1300, size: 2 }); bolt(c.clone().add(new V3(-4, -9, 0)), HEX.bad); bolt(c.clone().add(new V3(5, -9, 2)), 0xffffff);
  log(`💥 kill -9 sent to ${TN[t]} with ${before} acknowledged webhook(s) waiting in it. The edge had answered 200 for every one of them.`, 'bad');
  const r = await api('chaos', { target: t });
  if (!r.ok) { state.crash = null; log('crash failed: ' + r.error, 'bad'); }
};
function crashWatch(depth) {
  const cr = state.crash; if (!cr) return;
  if (depth == null) { cr.sawDown = true; cr.phase = 'down'; return; }
  if (cr.sawDown) { verdict(cr, depth); state.crash = null; } else if (performance.now() - cr.since > 25000) state.crash = null;
}
function verdict(cr, after) {
  const lost = Math.max(0, cr.before - after), el = $('#verdict'), c = AN.queue.c.clone(); let html, cls;
  if (cr.before < 3) { cls = 'none'; html = `<b>Nothing was waiting</b><span>The queue was empty, so there was nothing to lose. Switch the <i>ingest worker OFF</i>, fire a burst of 12, then crash it.</span>`; }
  else if (lost === 0) { cls = 'ok'; html = `<b>✅ SURVIVED ${after} / ${cr.before}</b><span>${TN[cr.t]}: killed with kill -9, restarted, and every acknowledged webhook was still there.</span>`;
    ring(c, HEX.ok, { size: 30, life: 1000, orient: 'x' }); for (let k = 0; k < 3; k++) burst(c.clone().add(new V3(0, 6, 0)), 50, { color: [HEX.ok, HEX.warn, 0x7aa7ff][k], speed: 14, dir: new V3(0, 1, 0), spread: 0.6, life: 1700, g: -8 }); sfx('ding'); }
  else { cls = 'bad'; html = `<b>💀 LOST ${lost} of ${cr.before}</b><span>${TN[cr.t]}: these webhooks were answered <u>200</u> to the provider, and are gone. This is why the queue's durability matters.</span>`;
    bolt(c, HEX.bad); shake(1.6); burst(c, 70, { color: 0x77808f, speed: 6, dir: new V3(0, 1, 0), spread: 0.7, life: 2200, size: 3, g: 0.4 }); sfx('buzz'); }
  el.className = cls; el.innerHTML = html; el.style.display = 'flex'; clearTimeout(verdict.t); verdict.t = setTimeout(() => { el.style.display = 'none'; }, 9000);
  log(cls === 'ok' ? `✅ ${TN[cr.t]} survived kill -9: ${after}/${cr.before} acknowledged webhooks intact` : cls === 'bad' ? `💀 ${TN[cr.t]} lost ${lost} of ${cr.before} acknowledged webhooks in the crash` : 'crash finished (nothing was queued)', cls === 'bad' ? 'bad' : 'ok');
}

// ======================= controls =======================
const api = (path, body) => fetch('/play/api/' + path, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body || {}) }).then((r) => r.json());
document.querySelectorAll('[data-fire]').forEach((b) => { b.onclick = () => fire(b.dataset.fire); });
document.querySelectorAll('#seg-broker button').forEach((b) => b.onclick = () => { state.broker = b.dataset.v; document.querySelectorAll('#seg-broker button').forEach((x) => { x.classList.toggle('on', x === b); x.classList.remove('rabbit', 'redis', 'redisv'); }); b.classList.add({ rabbitmq: 'rabbit', redis: 'redis', redis_volatile: 'redisv' }[b.dataset.v]); log('transport switched to ' + TN[state.broker] + ': new webhooks go through it', 'dim'); });
document.querySelectorAll('#seg-consumer button').forEach((b) => b.onclick = async () => { await api('control', { consumer_mode: b.dataset.v }); log('consumer is now ' + b.textContent, b.dataset.v === 'ok' ? 'ok' : 'warn'); setTimeout(poll, 100); });
document.querySelectorAll('#seg-pace button').forEach((b) => b.onclick = () => { PACE = +b.dataset.v; document.querySelectorAll('#seg-pace button').forEach((x) => x.classList.toggle('on', x === b)); $('#pace-note').style.display = PACE >= 1 ? '' : 'none'; });
$('#sw-ingest').onclick = async () => { const paused = !$('#sw-ingest').classList.contains('on'); await api('control', { ingest_paused: paused }); log(paused ? 'ingest worker OFF: accepted events now wait in the queue' : 'ingest worker ON: draining the queue', paused ? 'warn' : 'ok'); setTimeout(poll, 100); };
$('#btn-replay').onclick = async () => { const r = await api('replay_dead'); log(`replayed ${r.replayed} dead deliveries (same event ids, flagged as replays)`, 'ok'); setTimeout(poll, 200); };
$('#sw-sound').onclick = () => { state.sound = !state.sound; $('#sw-sound').textContent = state.sound ? '🔊 sound: on' : '🔇 sound: off'; $('#sw-sound').classList.toggle('on', state.sound); if (state.sound) sfx('ding'); };
$('#sw-ride').onclick = () => { state.ride = !state.ride; $('#sw-ride').classList.toggle('on', state.ride); $('#sw-ride').textContent = state.ride ? '🎥 ride along: on' : '🎥 ride along: off'; };
$('#btn-cam').onclick = resetCamera;
document.querySelectorAll('.collapse').forEach((b) => b.onclick = () => { const p = b.closest('.hudp'); p.classList.toggle('closed'); b.textContent = p.classList.contains('closed') ? '▢' : '–'; });
addEventListener('keydown', (e) => { if (e.target.tagName === 'INPUT') return; if (e.key === '1') fire('stripe'); if (e.key === '2') fire('shopify'); if (e.key === '3') fire('malicious'); if (e.key === 'r') resetCamera(); });

window.__range = { state, ST, AN, cam, fire, poll, packets, stats: { stored: 0, attempts: 0, dups: 0, flights: 0 } };
$('#loader').style.display = 'none';
poll(); setInterval(poll, 300); requestAnimationFrame(frame);
})();
