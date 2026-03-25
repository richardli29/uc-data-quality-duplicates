const API = {
  async get(path) {
    const resp = await fetch(`/api${path}`);
    if (!resp.ok) throw new Error(`API error: ${resp.status}`);
    return resp.json();
  },

  scanCatalog() { return this.get('/catalog/scan'); },
  getSchemas() { return this.get('/catalog/schemas'); },
  getTables(schema) { return this.get(`/catalog/tables${schema ? `?schema=${schema}` : ''}`); },
  getTable(schema, table) { return this.get(`/catalog/table/${schema}/${table}`); },
  detectDuplicates(threshold = 0.5) { return this.get(`/duplicates/detect?threshold=${threshold}`); },
  getGroups() { return this.get('/duplicates/groups'); },
  compareTables(s1, t1, s2, t2) { return this.get(`/compare/${s1}/${t1}/${s2}/${t2}`); },
  getSample(schema, table) { return this.get(`/compare/sample/${schema}/${table}`); },
};
