const sync = require("sync");

const { cache } = require("cache");
const { one, isGUID, titlize } = require("./util");
const { snColumn } = require("./util-table");

//split out table editing functionality
module.exports = class ServiceNowClientTable {
  constructor(client) {
    this.client = client;
    // this.get = cache.wrap(this, "get", "1s");
  }

  /**
   * Returns the table info for the given table.
   * Like a more detailed schema.
   * NOTE from @jpillora: sys_tags does not appear in column list.
   * @param {string} tableNameOrSysID
   */
  async get(tableNameOrSysID) {
    if (!tableNameOrSysID) {
      throw `Missing table name / table sys_id`;
    }
    let rawTable = await this.getRaw(tableNameOrSysID);
    let table = {
      name: rawTable.name,
      label: rawTable.label,
      is_extendable: rawTable.is_extendable,
      parent: undefined,
      parents: [],
      columns: {}
    };
    //add columns from entire hierarchy
    let curr = rawTable;
    while (curr) {
      for (let k in curr.columns) {
        let raw = curr.columns[k];
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
          if (curr.name !== table.name) {
            col.table = curr.name;
          }
          continue;
        }
        let col = {};
        col.name = id;
        col.label = raw.column_label;
        if (curr.name !== table.name) {
          col.table = curr.name;
        }
        for (let k in raw) {
          let v = raw[k];
          //exclude some
          if (
            //exlude all sys except id/author/updated
            (k !== "sys_created_by" &&
              k !== "sys_created_on" &&
              k !== "sys_id" &&
              k.startsWith("sys_")) ||
            (k === "active" && v === true) ||
            k === "calculation" ||
            k === "attributes" ||
            k === "element" /*included as id*/ ||
            k === "column_label" /*included as label*/ ||
            k === "name" /*table name*/ ||
            (k === "use_reference_qualifier" && v === "simple") ||
            (k === "choice" && v === 0) ||
            (typeof v === "string" && v.startsWith("javascript:"))
          ) {
            continue;
          }
          //transform
          if (k === "internal_type") {
            k = "type";
            v = v.value;
          } else if (k === "reference") {
            k = "referenceTable";
            v = v.value;
          }
          //copy over all 'true' flags, numbers and strings
          //all numbers
          if (v === true || typeof v === "number" || typeof v === "string") {
            col[k] = v;
          }
        }
        table.columns[id] = col;
      }
      curr = curr.super_class;
      //note all parent tables
      if (curr) {
        if (!table.parent) {
          table.parent = curr.name;
        }
        table.parents.push(curr.name);
      }
    }
    //"pretty" table information
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
    //validate table schema
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
      columns[id] = c;
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
    let col = snColumn(columnSpec);
    this.log(`table "${tableName}": add column "${col.element}"`);
    return await this.client.create("sys_dictionary", col);
  }

  /**
   * Updates a column in a table.
   * @param {string} tableName The target table
   * @param {object} columnName The existing column to update (must include sys_id).
   */
  async updateColumn(tableName, columnSpec) {
    let col = snColumn(columnSpec);
    this.log(`table "${tableName}": update column "${col.element}"`);
    return await this.client.update("sys_dictionary", col);
  }

  /**
   * Removes a column from a table.
   * @param {string} tableName The target table
   * @param {object} columnName The existing column to delete.
   */
  async removeColumn(tableName, columnName) {
    throw `Not implemented`;
  }

  /**
   * Sync a JS table spec with a servicenow table.
   * @param {object} table The table specification.
   * @param {object} opts Options to customise the sync.
   */
  async sync(table, opts) {
    if (!table || typeof table !== "object") {
      throw `Invalid spec`;
    }
    if (!opts) {
      opts = {};
    } else if (typeof opts !== "object") {
      throw `Invalid options`;
    }
    //fetch and validate servicenow columns
    let existingTable = await this.get(table.id);
    let existingColumns = existingTable.columns;
    let newColumns = table.columns;
    //perform diff new vs existing
    let pending = [];
    pending.add = 0;
    pending.update = 0;
    pending.remove = 0;
    pending.match = 0;
    for (let k in newColumns) {
      let col = newColumns[k];
      let { id, name } = col;
      if (id !== name && id in existingColumns) {
        throw `Column "${id}" already exists on "${existingTable.name}" but ` +
          `attempting to create as "${name}"`;
      }
      //already exists? need updating?
      if (name in existingColumns) {
        let ecol = existingColumns[name];
        let update = false;
        let ncol = {
          name: name,
          sys_id: ecol.sys_id
        };
        //update these columns
        for (let k of ["label", "max_length"]) {
          if (k in col && col[k] !== ecol[k]) {
            update = true;
            ncol[k] = col[k];
          }
        }
        //ensure these columns match
        for (let k of ["type", "referenceTable"]) {
          if (k in col && col[k] !== ecol[k]) {
            throw `Column "${k}" differs, however it cannot be updated`;
          }
        }
        if (update) {
          if (ecol.table) {
            this.log(
              `WARNING: update column (${name}) from ` +
                `parent table (${ecol.table}) not allowed`,
              ncol
            );
          } else {
            pending.push({
              description: `Update column "${col.name}"`,
              execute: async () =>
                await this.updateColumn(existingTable.name, ncol)
            });
            pending.update++;
          }
        } else {
          pending.match++;
        }
      } else {
        //doesnt exist, add!
        pending.push({
          description: `Add new column "${col.name}"`,
          execute: async () => await this.addColumn(existingTable.name, col)
        });
        pending.add++;
      }
    }
    //return pending actions
    if (opts.dryrun) {
      return pending;
    }
    return this.commit(pending);
  }

  /**
   * Commit a set of pending changes to a servicenow table.
   * @param {object} pending The pending operations.
   */
  async commit(pending) {
    this.log(`committing #${pending.length} changes`);
    for (let change of pending) {
      this.log(change.description);
      await change.execute();
    }
    return true;
  }

  log(...args) {
    this.client.log(...args);
  }

  debug(...args) {
    this.client.debug(...args);
  }
};
