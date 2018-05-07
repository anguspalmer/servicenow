module.exports = class CDoc {
  constructor(client) {
    this.client = client;
  }

  async get(tableName) {
    return await this.client.do({
      url: `/v2/table/sys_documentation`,
      params: {
        sysparm_query: `name=${tableName}`,
        sysparm_fields: `element,label,plural,help,hint,language,url`
      }
    });
  }

  //TODO
  // -> sync doc, tableName, column, docSpec
  // currently, doc is pulled in table.get, the correct labels
  // are swapped in

  log(...args) {
    this.client.log("[doc]", ...args);
  }
};
