const sync = require("sync");

const { one, sn2js } = require("./util");

//split out table editing functionality
module.exports = class ServiceNowClientTable {
  constructor(client) {
    this.client = client;
    this.infoSchemas = null;
  }

  /**
   * Returns the table info for the given table.
   * Like a more detailed schema.
   * @param {string} tableName
   */
  async get(tableName) {
    let params = {
      sysparm_query: `name=${tableName}`
    };
    //get table and its columns at once
    let [tables, columnList] = await sync.map(
      2,
      [`/v2/table/sys_db_object`, `/v2/table/sys_dictionary`],
      async url => {
        let resp = await this.client.api({
          method: "GET",
          url,
          params
        });
        if (resp.status !== 200) {
          throw `GET table info failed: ${resp.statusText}`;
        }
        let results = resp.data.result;
        if (!Array.isArray(results)) {
          throw `GET table expected results array`;
        }
        return results;
      }
    );
    //recurse to get schema's schema
    if (!this.infoSchemas) {
      console.log("GET INFO SCHEMA");
      let [table, column] = await sync.wait(true, [
        this.client.getSchema("sys_db_object"),
        this.client.getSchema("sys_dictionary")
      ]);
      this.infoSchemas = { table, column };
    }
    //validate schema
    let table = sn2js(this.infoSchemas.table, one(tables));
    let columns = {};
    columnList.forEach(c => {
      //skip null columns (seem to be "collection")
      if (c.sys_update_name === `sys_dictionary_${c.name}_null`) {
        return;
      }
      //all should have a column id...
      let id = c.element;
      if (!id) {
        throw `Expected "element" property to be set`;
      }
      columns[id] = sn2js(this.infoSchemas.column, c);
    });
    table.columns = columns;
    //ready
    return table;
  }

  /**
   * Delete a servicenow table.
   * @param {string} tableName The target table
   */
  async delete(tableName) {
    //
  }

  /**
   * Add a column to a table.
   * @param {string} tableName The target table
   * @param {object} columnSpec The new column to create.
   */
  async addColumn(tableName, columnSpec) {
    //
  }

  /**
   * Delete a column from a table.
   * @param {string} tableName The target table
   * @param {object} columnName The existing column to delete.
   */
  async deleteColumn(tableName, columnName) {
    //
  }

  /**
   * Sync a JS table spec with a servicenow table.
   * @param {string} spec The table specification
   */
  async sync(spec) {
    if (!spec || typeof spec !== "object") {
      throw `Invalid spec`;
    }
    let { servicenow, columns } = spec;
    //expand string
    if (typeof servicenow === "string") {
      servicenow = { id: servicenow };
    }
    //validate JS columns
    for (let k in columns) {
    }
    //fetch and validate servicenow columns
    //perform diff
  }
};
