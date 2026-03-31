/* ============================================================
   UC Data Quality Explorer — Single-Page Application
   ============================================================ */

let state = {
  catalogs: [],
  selectedCatalog: null,
  catalogsLoading: false,
  scanned: false,
  scanning: false,
  stats: null,
  schemas: [],
  tables: [],
  groups: [],
  selectedTable: null,
  compareResult: null,
  threshold: 0.5,
};

// ===== Router =====
function getPage() {
  const hash = location.hash || '#/';
  if (hash.startsWith('#/catalog')) return 'catalog';
  if (hash.startsWith('#/duplicates')) return 'duplicates';
  if (hash.startsWith('#/compare')) return 'compare';
  return 'dashboard';
}

function navigate() {
  const page = getPage();
  document.querySelectorAll('.nav-link').forEach(l => {
    l.classList.toggle('active', l.dataset.page === page);
  });
  render(page);
}

window.addEventListener('hashchange', navigate);
window.addEventListener('load', async () => {
  await loadCatalogs();
  navigate();
});

async function loadCatalogs() {
  state.catalogsLoading = true;
  try {
    state.catalogs = await API.listCatalogs();
    if (state.catalogs.length && !state.selectedCatalog) {
      state.selectedCatalog = state.catalogs[0].name;
    }
  } catch (e) {
    console.warn('Failed to load catalogs:', e.message);
  }
  state.catalogsLoading = false;
}

function onCatalogChange(name) {
  if (name === state.selectedCatalog) return;
  state.selectedCatalog = name;
  state.scanned = false;
  state.stats = null;
  state.schemas = [];
  state.tables = [];
  state.groups = [];
  state.selectedTable = null;
  state.compareResult = null;
  navigate();
}

// ===== Render =====
const $ = id => document.getElementById(id);
const main = () => $('main-content');

function render(page) {
  switch (page) {
    case 'dashboard': renderDashboard(); break;
    case 'catalog': renderCatalog(); break;
    case 'duplicates': renderDuplicates(); break;
    case 'compare': renderCompare(); break;
  }
}

// ===== Catalog Selector =====
function catalogSelector() {
  if (state.catalogsLoading) {
    return '<div style="margin-bottom:20px"><span style="color:var(--text-muted);font-size:13px">Loading catalogs...</span></div>';
  }
  if (!state.catalogs.length) {
    return '<div style="margin-bottom:20px"><span style="color:var(--yellow);font-size:13px">No catalogs found. Check service principal permissions.</span></div>';
  }
  return `
    <div class="catalog-selector" style="margin-bottom:20px;display:flex;align-items:center;gap:12px">
      <label style="font-size:13px;font-weight:600;color:var(--text-muted)">Catalog</label>
      <select id="catalog-select" style="min-width:220px">
        ${state.catalogs.map(c =>
          `<option value="${c.name}" ${c.name === state.selectedCatalog ? 'selected' : ''}>${c.name}</option>`
        ).join('')}
      </select>
      <span style="font-size:12px;color:var(--text-dim)">${state.catalogs.length} catalog${state.catalogs.length !== 1 ? 's' : ''} available</span>
    </div>
  `;
}

function bindCatalogSelector() {
  const sel = $('catalog-select');
  if (sel) sel.onchange = (e) => onCatalogChange(e.target.value);
}

// ===== Utilities =====
function html(strings, ...vals) {
  return strings.reduce((out, str, i) => out + str + (vals[i] ?? ''), '');
}

function similarityColor(score) {
  if (score >= 0.8) return 'var(--red)';
  if (score >= 0.6) return 'var(--yellow)';
  return 'var(--green)';
}

function formatNumber(n) {
  if (n == null) return '\u2014';
  return n.toLocaleString();
}

function timeAgo(ts) {
  if (!ts) return '\u2014';
  const d = new Date(ts);
  const now = Date.now();
  const diff = now - d.getTime();
  if (diff < 3600000) return `${Math.floor(diff / 60000)}m ago`;
  if (diff < 86400000) return `${Math.floor(diff / 3600000)}h ago`;
  return `${Math.floor(diff / 86400000)}d ago`;
}

function loading(msg = 'Loading...') {
  return `<div class="loading"><div class="spinner"></div>${msg}</div>`;
}

function permBadges(permissions) {
  if (!permissions || !permissions.length) return '<span class="tag tag-yellow">No grants found</span>';
  return permissions.map(p => {
    const isWrite = p.privileges.some(pr =>
      pr === 'ALL_PRIVILEGES' || pr === 'MODIFY' || pr === 'CREATE'
    );
    const isRead = p.privileges.some(pr => pr === 'SELECT');
    let tags = '';
    if (isWrite) tags += `<span class="tag tag-blue">WRITE</span> `;
    else if (isRead) tags += `<span class="tag tag-green">READ</span> `;
    else tags += p.privileges.map(pr => `<span class="tag tag-accent">${pr}</span>`).join(' ');
    return `<div class="perm-row">
      <span class="perm-principal">${p.principal}</span>
      <span class="perm-badges">${tags}</span>
    </div>`;
  }).join('');
}

// ===== Dashboard =====
async function renderDashboard() {
  const cat = state.selectedCatalog;
  main().innerHTML = `
    <h2 class="page-title">Dashboard</h2>
    <p class="page-desc">Scan Unity Catalog metadata, detect duplicate datasets, and identify gold-standard tables.</p>
    ${catalogSelector()}
    <div style="margin-bottom:20px">
      <button class="btn btn-primary" id="scan-btn" ${state.scanning || !cat ? 'disabled' : ''}>
        ${state.scanning ? '<div class="spinner" style="width:14px;height:14px;margin-right:6px"></div> Scanning...' : 'Scan Catalog'}
      </button>
    </div>
    <div class="stats-grid" id="stats-grid">
      ${state.stats ? renderStats(state.stats) : '<div class="stat-card"><div class="stat-label">Status</div><div class="stat-value" style="font-size:16px;color:var(--text-muted)">Select a catalog and click "Scan Catalog" to begin</div></div>'}
    </div>
    <div id="top-duplicates"></div>
  `;

  bindCatalogSelector();
  $('scan-btn').onclick = doScan;
  if (state.groups.length) renderTopDuplicates();
}

function renderStats(s) {
  return `
    <div class="stat-card"><div class="stat-label">Catalog</div><div class="stat-value" style="font-size:16px">${s.catalog || state.selectedCatalog}</div></div>
    <div class="stat-card"><div class="stat-label">Schemas</div><div class="stat-value">${s.schema_count}</div></div>
    <div class="stat-card"><div class="stat-label">Tables</div><div class="stat-value">${s.table_count}</div></div>
    <div class="stat-card"><div class="stat-label">Columns</div><div class="stat-value">${s.column_count}</div></div>
    <div class="stat-card"><div class="stat-label">Duplicate Groups</div><div class="stat-value accent">${state.groups.length}</div></div>
  `;
}

async function doScan() {
  const cat = state.selectedCatalog;
  if (!cat) { alert('Select a catalog first'); return; }
  state.scanning = true;
  renderDashboard();
  try {
    state.stats = await API.scanCatalog(cat);
    state.schemas = await API.getSchemas(cat);
    state.tables = await API.getTables(null, cat);
    state.groups = await API.detectDuplicates(state.threshold, cat);
    state.scanned = true;
  } catch (e) {
    alert('Scan failed: ' + e.message);
  }
  state.scanning = false;
  renderDashboard();
}

function renderTopDuplicates() {
  const el = $('top-duplicates');
  if (!state.groups.length) {
    el.innerHTML = '<div class="card"><div class="empty-state"><h3>No duplicates detected</h3><p>All tables appear unique.</p></div></div>';
    return;
  }
  el.innerHTML = `
    <div class="section-title">Top Duplicate Groups</div>
    ${state.groups.slice(0, 5).map(g => renderDupGroupCard(g)).join('')}
    ${state.groups.length > 5 ? `<p style="color:var(--text-muted);font-size:13px">+ ${state.groups.length - 5} more groups. <a href="#/duplicates" style="color:var(--accent)">View all</a></p>` : ''}
  `;
}

// ===== Catalog Explorer =====
async function renderCatalog() {
  if (!state.scanned) {
    main().innerHTML = `
      <h2 class="page-title">Catalog Explorer</h2>
      <p class="page-desc">Browse schemas and tables in Unity Catalog.</p>
      <div class="empty-state"><h3>No data scanned yet</h3><p>Go to the Dashboard and click "Scan Catalog" first.</p></div>
    `;
    return;
  }

  main().innerHTML = `
    <h2 class="page-title">Catalog Explorer</h2>
    <p class="page-desc">Browsing <strong>${state.selectedCatalog}</strong>. Click a table to see its metadata and permissions.</p>
    <div class="tree-container">
      <div class="tree-panel" id="tree-panel">${renderTree()}</div>
      <div class="detail-panel" id="detail-panel">
        <div class="empty-state"><h3>Select a table</h3><p>Click on a table in the tree to view details.</p></div>
      </div>
    </div>
  `;

  $('tree-panel').onclick = async (e) => {
    const tableEl = e.target.closest('.tree-table');
    if (tableEl) {
      const { schema, table } = tableEl.dataset;
      document.querySelectorAll('.tree-table').forEach(t => t.classList.remove('active'));
      tableEl.classList.add('active');
      $('detail-panel').innerHTML = loading('Loading table details...');
      try {
        const info = await API.getTable(schema, table, state.selectedCatalog);
        state.selectedTable = info;
        renderTableDetail(info);
      } catch (e) {
        $('detail-panel').innerHTML = `<div class="empty-state"><h3>Error</h3><p>${e.message}</p></div>`;
      }
    }
    const schemaEl = e.target.closest('.tree-schema-name');
    if (schemaEl) {
      const tables = schemaEl.nextElementSibling;
      if (tables) tables.style.display = tables.style.display === 'none' ? 'block' : 'none';
    }
  };
}

function renderTree() {
  return state.schemas.map(s => {
    const tables = state.tables.filter(t => t.schema === s.name);
    return `
      <div class="tree-schema">
        <div class="tree-schema-name">
          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 19a2 2 0 01-2 2H4a2 2 0 01-2-2V5a2 2 0 012-2h5l2 3h9a2 2 0 012 2z"/></svg>
          ${s.name}
          <span class="count">${s.table_count}</span>
        </div>
        <div class="tree-tables">
          ${tables.map(t => `<div class="tree-table" data-schema="${t.schema}" data-table="${t.name}">${t.name}</div>`).join('')}
        </div>
      </div>
    `;
  }).join('');
}

function renderTableDetail(info) {
  const dp = $('detail-panel');
  dp.innerHTML = `
    <h3 style="font-size:18px;font-weight:700;margin-bottom:4px">${info.name}</h3>
    <p style="font-size:12px;color:var(--text-muted);margin-bottom:16px">${info.full_name}</p>

    ${info.comment ? `<div class="card" style="margin-bottom:16px;background:var(--bg)"><p style="font-size:13px;color:var(--text-muted)">${info.comment}</p></div>` : ''}

    <div class="stats-grid" style="grid-template-columns:repeat(3,1fr);margin-bottom:20px">
      <div class="stat-card"><div class="stat-label">Rows</div><div class="stat-value" style="font-size:20px">${formatNumber(info.row_count)}</div></div>
      <div class="stat-card"><div class="stat-label">Columns</div><div class="stat-value" style="font-size:20px">${info.columns.length}</div></div>
      <div class="stat-card"><div class="stat-label">Owner</div><div class="stat-value" style="font-size:14px">${info.owner || '\u2014'}</div></div>
    </div>

    <div class="section">
      <div class="section-title">Access Permissions</div>
      <div class="perm-list">${permBadges(info.permissions)}</div>
    </div>

    <div class="section">
      <div class="section-title">Columns</div>
      <table class="data-table">
        <thead><tr><th>#</th><th>Name</th><th>Type</th><th>Nullable</th></tr></thead>
        <tbody>
          ${info.columns.map((c, i) => `
            <tr>
              <td style="color:var(--text-dim)">${i + 1}</td>
              <td style="font-weight:600">${c.name}</td>
              <td><span class="tag tag-accent">${c.type_name}</span></td>
              <td>${c.nullable ? '\u2713' : '\u2717'}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
  `;
}

// ===== Duplicates =====
async function renderDuplicates() {
  if (!state.scanned) {
    main().innerHTML = `
      <h2 class="page-title">Duplicate Detection</h2>
      <p class="page-desc">Find duplicate and similar datasets across schemas.</p>
      <div class="empty-state"><h3>No data scanned yet</h3><p>Go to the Dashboard and click "Scan Catalog" first.</p></div>
    `;
    return;
  }

  main().innerHTML = `
    <h2 class="page-title">Duplicate Detection</h2>
    <p class="page-desc">Tables in <strong>${state.selectedCatalog}</strong> grouped by similarity. The gold badge marks the recommended standard dataset.</p>
    <div class="threshold-control">
      <label>Similarity Threshold</label>
      <input type="range" id="threshold-slider" min="0.1" max="1.0" step="0.05" value="${state.threshold}" />
      <span class="threshold-value" id="threshold-val">${(state.threshold * 100).toFixed(0)}%</span>
      <button class="btn btn-outline btn-sm" id="redetect-btn">Re-detect</button>
    </div>
    <div id="dup-groups">${state.groups.length ? state.groups.map(g => renderDupGroupCard(g)).join('') : '<div class="empty-state"><h3>No duplicates found</h3><p>Try lowering the threshold.</p></div>'}</div>
  `;

  $('threshold-slider').oninput = (e) => {
    state.threshold = parseFloat(e.target.value);
    $('threshold-val').textContent = (state.threshold * 100).toFixed(0) + '%';
  };

  $('redetect-btn').onclick = async () => {
    $('dup-groups').innerHTML = loading('Detecting duplicates...');
    state.groups = await API.detectDuplicates(state.threshold, state.selectedCatalog);
    $('dup-groups').innerHTML = state.groups.length
      ? state.groups.map(g => renderDupGroupCard(g)).join('')
      : '<div class="empty-state"><h3>No duplicates found</h3><p>Try lowering the threshold.</p></div>';
  };
}

function renderDupGroupCard(g) {
  const maxScore = g.pairs.length ? Math.max(...g.pairs.map(p => p.composite_score)) : 0;
  return `
    <div class="dup-group">
      <div class="dup-group-header">
        <span class="dup-group-title">Group ${g.group_id} \u2014 ${g.tables.length} tables</span>
        <span class="similarity-score" style="color:${similarityColor(maxScore)}">${(maxScore * 100).toFixed(0)}% max similarity</span>
      </div>
      <div class="similarity-bar"><div class="similarity-bar-fill" style="width:${maxScore * 100}%;background:${similarityColor(maxScore)}"></div></div>
      <div class="dup-tables-list" style="margin-top:10px">
        ${g.tables.map(t => {
          const isGold = t === g.gold_standard;
          const parts = t.split('.');
          const score = g.gold_scores[t];
          return `<span class="dup-table-tag ${isGold ? 'gold' : ''}" title="Gold score: ${score ?? '\u2014'}">${isGold ? '\u2605 ' : ''}${parts[1]}.${parts[2]}</span>`;
        }).join('')}
      </div>
      ${g.gold_standard ? `<div style="margin-top:8px"><span class="gold-badge">\u2605 Gold Standard: ${g.gold_standard.split('.').slice(1).join('.')}</span></div>` : ''}
      <div style="margin-top:12px">
        <table class="data-table">
          <thead><tr><th>Table A</th><th>Table B</th><th>Columns</th><th>Types</th><th>Name</th><th>Score</th><th></th></tr></thead>
          <tbody>
            ${g.pairs.slice(0, 6).map(p => {
              const a = p.table_a.split('.').slice(1).join('.');
              const b = p.table_b.split('.').slice(1).join('.');
              return `<tr>
                <td style="font-weight:500">${a}</td>
                <td style="font-weight:500">${b}</td>
                <td>${(p.column_similarity * 100).toFixed(0)}%</td>
                <td>${(p.type_similarity * 100).toFixed(0)}%</td>
                <td>${(p.name_similarity * 100).toFixed(0)}%</td>
                <td><span class="similarity-score" style="color:${similarityColor(p.composite_score)}">${(p.composite_score * 100).toFixed(0)}%</span></td>
                <td><button class="btn btn-outline btn-sm compare-btn" data-a="${p.table_a}" data-b="${p.table_b}">Compare</button></td>
              </tr>`;
            }).join('')}
          </tbody>
        </table>
      </div>
    </div>
  `;
}

document.addEventListener('click', (e) => {
  const btn = e.target.closest('.compare-btn');
  if (btn) {
    const a = btn.dataset.a.split('.');
    const b = btn.dataset.b.split('.');
    location.hash = `#/compare?s1=${a[1]}&t1=${a[2]}&s2=${b[1]}&t2=${b[2]}`;
  }
});

// ===== Compare =====
async function renderCompare() {
  const params = new URLSearchParams(location.hash.split('?')[1] || '');
  const s1 = params.get('s1'), t1 = params.get('t1');
  const s2 = params.get('s2'), t2 = params.get('t2');

  if (!state.scanned) {
    main().innerHTML = `
      <h2 class="page-title">Compare Tables</h2>
      <p class="page-desc">Side-by-side comparison of two tables.</p>
      <div class="empty-state"><h3>No data scanned yet</h3><p>Go to the Dashboard and click "Scan Catalog" first.</p></div>
    `;
    return;
  }

  main().innerHTML = `
    <h2 class="page-title">Compare Tables</h2>
    <p class="page-desc">Side-by-side schema diff, permissions, and sample data comparison in <strong>${state.selectedCatalog}</strong>.</p>
    <div class="compare-selector" id="compare-form">
      <div class="field-group">
        <span class="field-label">Table A</span>
        <select id="sel-a">
          <option value="">Select table...</option>
          ${state.tables.map(t => `<option value="${t.schema}|${t.name}" ${t.schema === s1 && t.name === t1 ? 'selected' : ''}>${t.schema}.${t.name}</option>`).join('')}
        </select>
      </div>
      <div class="field-group">
        <span class="field-label">Table B</span>
        <select id="sel-b">
          <option value="">Select table...</option>
          ${state.tables.map(t => `<option value="${t.schema}|${t.name}" ${t.schema === s2 && t.name === t2 ? 'selected' : ''}>${t.schema}.${t.name}</option>`).join('')}
        </select>
      </div>
      <button class="btn btn-primary" id="compare-go">Compare</button>
    </div>
    <div id="compare-result"></div>
  `;

  $('compare-go').onclick = doCompare;

  if (s1 && t1 && s2 && t2) {
    doCompare();
  }
}

async function doCompare() {
  const a = $('sel-a').value.split('|');
  const b = $('sel-b').value.split('|');
  if (a.length < 2 || b.length < 2) { alert('Select two tables'); return; }
  const [s1, t1] = a;
  const [s2, t2] = b;
  const cat = state.selectedCatalog;

  const el = $('compare-result');
  el.innerHTML = loading('Comparing tables...');

  try {
    const [result, sampleA, sampleB] = await Promise.all([
      API.compareTables(s1, t1, s2, t2, cat),
      API.getSample(s1, t1, cat).catch(() => null),
      API.getSample(s2, t2, cat).catch(() => null),
    ]);
    state.compareResult = result;
    renderCompareResult(result, sampleA, sampleB);
  } catch (e) {
    el.innerHTML = `<div class="empty-state"><h3>Comparison failed</h3><p>${e.message}</p></div>`;
  }
}

function renderCompareResult(r, sampleA, sampleB) {
  const el = $('compare-result');
  el.innerHTML = `
    <div class="compare-grid" style="margin-bottom:20px">
      <div class="card">
        <h4 style="font-weight:700;margin-bottom:8px">${r.table_a.schema}.${r.table_a.name}</h4>
        <div style="font-size:13px;color:var(--text-muted)">
          <div>Rows: <strong>${formatNumber(r.table_a.row_count)}</strong></div>
          <div>Columns: <strong>${r.table_a.column_count}</strong></div>
          <div>Owner: ${r.table_a.owner || '\u2014'}</div>
          ${r.table_a.comment ? `<div style="margin-top:6px;font-style:italic">${r.table_a.comment}</div>` : ''}
        </div>
      </div>
      <div class="card">
        <h4 style="font-weight:700;margin-bottom:8px">${r.table_b.schema}.${r.table_b.name}</h4>
        <div style="font-size:13px;color:var(--text-muted)">
          <div>Rows: <strong>${formatNumber(r.table_b.row_count)}</strong></div>
          <div>Columns: <strong>${r.table_b.column_count}</strong></div>
          <div>Owner: ${r.table_b.owner || '\u2014'}</div>
          ${r.table_b.comment ? `<div style="margin-top:6px;font-style:italic">${r.table_b.comment}</div>` : ''}
        </div>
      </div>
    </div>

    <div class="stats-grid" style="grid-template-columns:repeat(3,1fr);margin-bottom:20px">
      <div class="stat-card"><div class="stat-label">Shared Columns</div><div class="stat-value" style="font-size:24px;color:var(--green)">${r.shared_columns}</div></div>
      <div class="stat-card"><div class="stat-label">Only in A</div><div class="stat-value" style="font-size:24px;color:var(--red)">${r.only_a_columns}</div></div>
      <div class="stat-card"><div class="stat-label">Only in B</div><div class="stat-value" style="font-size:24px;color:var(--blue)">${r.only_b_columns}</div></div>
    </div>

    ${r.permissions_diff && r.permissions_diff.length ? `
    <div class="section">
      <div class="section-title">Access Permissions Comparison</div>
      <table class="data-table">
        <thead><tr><th>Principal</th><th>${r.table_a.schema}.${r.table_a.name}</th><th>${r.table_b.schema}.${r.table_b.name}</th><th>Match</th></tr></thead>
        <tbody>
          ${r.permissions_diff.map(p => `
            <tr>
              <td style="font-weight:600">${p.principal}</td>
              <td>${(p.privileges_a || []).map(pr => `<span class="tag ${pr === 'SELECT' ? 'tag-green' : 'tag-blue'}">${pr}</span> `).join('') || '\u2014'}</td>
              <td>${(p.privileges_b || []).map(pr => `<span class="tag ${pr === 'SELECT' ? 'tag-green' : 'tag-blue'}">${pr}</span> `).join('') || '\u2014'}</td>
              <td>${p.match ? '<span class="tag tag-green">Match</span>' : '<span class="tag tag-yellow">Differs</span>'}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>
    ` : ''}

    <div class="section">
      <div class="section-title">Column Schema Diff</div>
      <table class="data-table">
        <thead><tr><th>Column</th><th>Status</th><th>Type (A)</th><th>Type (B)</th></tr></thead>
        <tbody>
          ${r.column_diff.map(c => `
            <tr class="diff-row-${c.status}">
              <td style="font-weight:600">${c.column}</td>
              <td>
                ${c.status === 'shared' ? (c.type_match ? '<span class="tag tag-green">Shared</span>' : '<span class="tag tag-yellow">Type Mismatch</span>') : ''}
                ${c.status === 'only_a' ? `<span class="tag tag-red">Only in A</span>` : ''}
                ${c.status === 'only_b' ? `<span class="tag tag-blue">Only in B</span>` : ''}
              </td>
              <td>${c.type_a || '\u2014'}</td>
              <td>${c.type_b || '\u2014'}</td>
            </tr>
          `).join('')}
        </tbody>
      </table>
    </div>

    <div class="section">
      <div class="section-title">Sample Data</div>
      <div class="compare-grid">
        <div>
          <h4 style="font-size:13px;font-weight:600;margin-bottom:8px">${r.table_a.schema}.${r.table_a.name}</h4>
          ${sampleA ? renderSampleTable(sampleA) : '<p style="color:var(--text-muted);font-size:13px">Could not load sample data</p>'}
        </div>
        <div>
          <h4 style="font-size:13px;font-weight:600;margin-bottom:8px">${r.table_b.schema}.${r.table_b.name}</h4>
          ${sampleB ? renderSampleTable(sampleB) : '<p style="color:var(--text-muted);font-size:13px">Could not load sample data</p>'}
        </div>
      </div>
    </div>
  `;
}

function renderSampleTable(data) {
  if (!data || !data.columns) return '<p style="color:var(--text-muted)">No data</p>';
  return `
    <div class="sample-container">
      <table class="data-table">
        <thead><tr>${data.columns.map(c => `<th>${c.name}</th>`).join('')}</tr></thead>
        <tbody>
          ${(data.rows || []).slice(0, 8).map(row =>
            `<tr>${row.map(v => `<td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${v ?? '<null>'}</td>`).join('')}</tr>`
          ).join('')}
        </tbody>
      </table>
    </div>
  `;
}
