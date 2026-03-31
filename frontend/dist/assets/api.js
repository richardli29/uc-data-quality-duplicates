const API = {
  async get(path) {
    const resp = await fetch(`/api${path}`);
    if (!resp.ok) throw new Error(`API error: ${resp.status}`);
    return resp.json();
  },

  _qs(params) {
    const p = Object.entries(params).filter(([, v]) => v != null && v !== '');
    return p.length ? '?' + p.map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&') : '';
  },

  listCatalogs() { return this.get('/catalog/list'); },

  scanCatalog(catalog) {
    return this.get('/catalog/scan' + this._qs({ catalog }));
  },

  getSchemas(catalog) {
    return this.get('/catalog/schemas' + this._qs({ catalog }));
  },

  getTables(schema, catalog) {
    return this.get('/catalog/tables' + this._qs({ schema, catalog }));
  },

  getTable(schema, table, catalog) {
    return this.get(`/catalog/table/${schema}/${table}` + this._qs({ catalog }));
  },

  detectDuplicates(threshold = 0.5, catalog) {
    return this.get('/duplicates/detect' + this._qs({ threshold, catalog }));
  },

  getGroups() { return this.get('/duplicates/groups'); },

  compareTables(s1, t1, s2, t2, catalog) {
    return this.get(`/compare/${s1}/${t1}/${s2}/${t2}` + this._qs({ catalog }));
  },

  getSample(schema, table, catalog) {
    return this.get(`/compare/sample/${schema}/${table}` + this._qs({ catalog }));
  },
};
