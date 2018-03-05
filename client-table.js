const sync = require("sync");

const { cache } = require("cache");
const { one, sn2js, isGUID, titlize } = require("./util");

//split out table editing functionality
module.exports = class ServiceNowClientTable {
  constructor(client) {
    this.client = client;
    this.infoSchemas = null;
    this.get = cache.wrap(this, "get", "1s");
  }

  /**
   * Returns the table info for the given table.
   * Like a more detailed schema.
   * NOTE from @jpillora: sys_tags does not appear in column list.
   * @param {string} tableNameOrSysID
   */
  async get(tableNameOrSysID) {
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
            (k !== "sys_created_by" &&
              k !== "sys_updated_by" &&
              k !== "sys_id" &&
              k.startsWith("sys_")) ||
            (k === "active" && v === true) ||
            k === "calculation" ||
            k === "attributes" ||
            k === "element" /*already included as id*/ ||
            k === "name" /*table name*/ ||
            k === "column_label" ||
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
    //throws on failure,
    //sets defaults
    let col = snColumn(columnSpec);
    this.log(`table "${tableName}": add column "${col.name}"`);
    return await this.client.create("sys_dictionary", {
      active: true,
      name: tableName,
      display: false,
      column_label: col.label,
      element: col.name,
      max_length: col.max_length,
      internal_type: col.type
    });
  }

  /**
   * Updates a column in a table.
   * @param {string} tableName The target table
   * @param {object} columnName The existing column to update (must include sys_id).
   */
  async updateColumn(tableName, columnUpdates) {
    let updates = {};
    for (let k in columnUpdates) {
      let v = columnUpdates[k];
      if (k === "name") {
        k = "element";
      } else if (k === "type") {
        k = "internal_type";
      } else if (k === "label") {
        k = "column_label";
      }
      updates[k] = v;
    }
    this.log(`table "${tableName}": update column "${columnUpdates.name}"`);
    return await this.client.update("sys_dictionary", updates);
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
    let { servicenow, columns } = spec;
    //expand string
    if (typeof servicenow === "string") {
      servicenow = { id: servicenow };
    }
    //extract JS columns
    let incoming = {};
    for (let id in columns) {
      let c = columns[id];
      //expand type
      if (typeof c !== "object") {
        c = { type: c };
      }
      //extract column info
      let { type, servicenow = {} } = c;
      //convert function (sequelize type class) to string
      if (typeof type === "function") {
        let t = type;
        if (t.key) {
          type = t.key;
        } else {
          type = t.name;
        }
      }
      if (typeof type !== "string") {
        throw `Expected string type on column "${id}"`;
      }
      type = type.toLowerCase();
      if (typeof type !== "string") {
        throw `Column (${id}) type must be a string`;
      }
      //expand column label
      if (typeof servicenow === "string") {
        servicenow = { name: servicenow };
      }
      let { name = `u_${id}`, label = titlize(name) } = servicenow;
      //convert to service-now form
      incoming[name] = snColumn({
        id,
        name,
        type,
        label
      });
    }
    //fetch and validate servicenow columns
    let table = await this.get(servicenow.id);
    let existing = table.columns;
    //perform diff incoming vs existing
    let pending = { table: table.name, add: [], updates: [], remove: [] };
    for (let k in incoming) {
      let col = incoming[k];
      let { id, name } = col;
      if (id !== name && id in existing) {
        throw `Column "${id}" already exists but attempting to create as "${name}"`;
      }
      if (name in existing) {
        //compare type and max_length
        let ecol = existing[name];
        let update = false;
        let updates = {
          name: name,
          sys_id: ecol.sys_id
        };
        for (let k of ["label", "max_length"]) {
          if (k in col && col[k] !== ecol[k]) {
            update = true;
            updates[k] = col[k];
          }
        }
        if (update) {
          if (ecol.table) {
            this.log(
              `WARNING: update column (${name}) from ` +
                `parent table (${ecol.table}) not allowed`,
              updates
            );
          } else {
            pending.updates.push(updates);
          }
        }
      } else {
        pending.add.push(col);
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
    for (let col of pending.add) {
      await this.addColumn(pending.table, col);
    }
    for (let col of pending.updates) {
      await this.updateColumn(pending.table, col);
    }
    for (let col of pending.remove) {
      await this.removeColumn(pending.table, col);
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

//converts a JS column into a SN column
const snColumn = col => {
  let { name, label, type, max_length } = col;
  if (!name) {
    throw `Missing column name`;
  }
  if (!label) {
    label = titlize(name);
  }
  let default_length = 40;
  switch (type) {
    case "text":
      default_length = 65000;
      break;
    case "string":
      default_length = 255;
      break;
    case "integer":
    case "float":
    case "decimal":
      break;
    case "date":
      type = "glide_date_time";
      break;
    default:
      throw `Unknown column type "${type}"`;
  }
  if (!max_length) {
    max_length = default_length;
  }
  return {
    name,
    label,
    type,
    max_length
  };
};
