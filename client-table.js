const sync = require("sync");

const { one, sn2js, isGUID } = require("./util");

//split out table editing functionality
module.exports = class ServiceNowClientTable {
  constructor(client) {
    this.client = client;
    this.infoSchemas = null;
  }

  /**
   * Returns the table info for the given table.
   * Like a more detailed schema.
   * @param {string} tableNameOrSysID
   */
  async get(tableNameOrSysID) {
    let rawTable = await this.getRaw(tableNameOrSysID);
    let table = {
      name: rawTable.name,
      label: rawTable.label,
      is_extendable: rawTable.is_extendable,
      parent: undefined,
      columns: {}
    };
    const add = (rawTable, raw) => {
      let id = raw.element;
      if (!id) {
        throw `Column has no name`;
      }
      //already defined?
      if (id in table.columns) {
        let col = table.columns[id];
        if (!id.startsWith("sys_")) {
          col.overridden = true;
        }
        if (rawTable.name !== table.name) {
          col.table = rawTable.name;
        }
        return;
      }
      let col = {};
      col.name = id;
      col.label = raw.column_label;
      if (rawTable.name !== table.name) {
        col.table = rawTable.name;
      }
      for (let k in raw) {
        let v = raw[k];
        //exclude some
        if (
          k.startsWith("sys_") ||
          (k === "active" && v === true) ||
          k === "calculation" ||
          k === "attributes" ||
          k === "element" /*already included as id*/ ||
          k === "name" /*table name*/ ||
          k === "column_label" ||
          (k === "use_reference_qualifier" && v === "simple") ||
          (typeof v === "string" && v.startsWith("javascript:"))
        ) {
          continue;
        }
        //transform
        if (k === "internal_type") {
          k = "type";
          v = v.value;
        }
        if (k === "choice") {
          if (v === 1) {
            v = true;
          } else {
            continue;
          }
        }
        //copy over all 'true' flags, numbers and strings
        //all numbers
        if (v === true || typeof v === "number" || typeof v === "string") {
          col[k] = v;
        }
      }
      table.columns[id] = col;
    };
    //include parent name
    if (rawTable.super_class && !table.parent) {
      table.parent = rawTable.super_class.name;
    }
    //add columns from entire hierarchy
    let curr = rawTable;
    while (curr) {
      for (let k in curr.columns) {
        add(curr, curr.columns[k]);
      }
      curr = curr.super_class;
    }
    //"pretty" table
    return table;
  }

  /**
   * Returns the table raw info for the given table.
   * Like an extremely detailed schema.
   * @param {string} tableNameOrSysID
   */
  async getRaw(tableNameOrSysID) {
    //fetch table
    let table;
    if (isGUID(tableNameOrSysID)) {
      table = await this.client.do({
        url: `/v2/table/sys_db_object/${tableNameOrSysID}`
      });
    } else {
      table = one(
        await this.client.do({
          url: `/v2/table/sys_db_object`,
          params: { sysparm_query: `name=${tableNameOrSysID}` }
        })
      );
    }
    if (!table) {
      throw `Table "${tableNameOrSysID}" does not exist`;
    }
    //fetch columns
    let columnList = await this.client.do({
      url: `/v2/table/sys_dictionary`,
      params: { sysparm_query: `name=${table.name}` }
    });
    //get schema's schema
    if (!this.infoSchemas) {
      let [table, column] = await sync.wait(true, [
        this.client.getSchema("sys_db_object"),
        this.client.getSchema("sys_dictionary")
      ]);
      this.infoSchemas = { table, column };
    }
    //validate table schema
    table = sn2js(this.infoSchemas.table, table);
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
      //validate column schema
      columns[id] = sn2js(this.infoSchemas.column, c);
    });
    table.columns = columns;
    //recurse into superclass
    if (table.super_class) {
      table.super_class = await this.getRaw(table.super_class.value);
    }
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
   * @param {object} spec The table specification.
   * @param {object} opts Options to customise the sync.
   */
  async sync(spec, opts) {
    if (!spec || typeof spec !== "object") {
      throw `Invalid spec`;
    }
    if (!opts || typeof opts !== "object") {
      opts = {};
    }
    console.log(spec);
    let { servicenow, columns } = spec;
    //expand string
    if (typeof servicenow === "string") {
      servicenow = { id: servicenow };
    }
    console.log(servicenow, columns);
    //validate JS columns
    for (let k in columns) {
      let c = columns[k];
      console.log(k, c);
    }
    //fetch and validate servicenow columns
    //perform diff
  }
};
