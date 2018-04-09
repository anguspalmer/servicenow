const sync = require("sync");
const isEqual = require("lodash.isequal");
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
    let rootTable = await this.getRaw(tableNameOrSysID);
    if (rootTable === null) {
      return null; //not found
    }
    let table = {
      name: rootTable.name,
      label: rootTable.label,
      is_extendable: rootTable.is_extendable,
      sys_id: rootTable.sys_id,
      parent: undefined,
      parents: [],
      columns: {}
    };
    //add columns from entire hierarchy
    let rawTable = rootTable;
    while (rawTable) {
      //add parent tables
      if (rawTable !== rootTable) {
        if (!table.parent) {
          table.parent = rawTable.name;
        }
        table.parents.push(rawTable.name);
      }
      //add columns from this table
      for (let k in rawTable.columns) {
        let raw = rawTable.columns[k];
        let id = raw.element;
        if (!id) {
          throw `Column has no name`;
        }
        //already defined?
        if (id in table.columns) {
          let col = table.columns[id];
          //any column found twice is "overridden"
          if (!id.startsWith("sys_")) {
            col.overridden = true;
          }
          //use first choice map found
          if (!col.choice_map && raw.choice_map) {
            col.choice_map = raw.choice_map;
          }
          //use LAST parent (source parent)
          col.table = rawTable.name;
          continue;
        }
        let col = {};
        col.name = id;
        col.label = raw.column_label;
        col.table = rawTable.name;
        col.choice_map = raw.choice_map;
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
          //transforms
          if (k === "internal_type") {
            k = "type";
            v = v.toLowerCase();
          } else if (k === "reference") {
            k = "reference_table";
          }
          if (k === "choice") {
            if (v === 1) {
              v = "nullable";
            } else if (v === 2) {
              v = "suggestion";
            } else if (v === 3) {
              v = "required";
            }
          }
          //copy over all 'true' flags, numbers and strings
          //all numbers
          if (
            v === true ||
            v instanceof Date ||
            typeof v === "number" ||
            typeof v === "string"
          ) {
            col[k] = v;
          }
        }
        table.columns[id] = col;
      }
      //has another parent?
      rawTable = rawTable.super_class;
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
        url: `/v2/table/sys_db_object/${tableNameOrSysID}`,
        params: {
          sysparm_exclude_reference_link: true
        }
      });
    } else {
      table = one(
        await this.client.do({
          url: `/v2/table/sys_db_object`,
          params: {
            sysparm_exclude_reference_link: true,
            sysparm_query: `name=${tableNameOrSysID}`
          }
        })
      );
    }
    if (!table) {
      return null;
    }
    //wait on a few promises and map the results
    let [columnList, choiceList] = await sync.wait([
      this.client.do({
        url: `/v2/table/sys_dictionary`,
        params: {
          sysparm_exclude_reference_link: true,
          sysparm_query: `name=${table.name}`
        }
      }),
      this.client.do({
        url: `/v2/table/sys_choice`,
        params: {
          sysparm_exclude_reference_link: true,
          sysparm_query: `name=${table.name}`
        }
      })
    ]);
    //extract and group choice fields
    let tableChoices = {};
    for (let ch of choiceList) {
      let id = ch.element;
      let col = tableChoices[id];
      if (!col) {
        col = {};
        tableChoices[id] = col;
      }
      col[ch.value] = ch.label;
    }
    //validate table schema
    let columns = {};
    for (let c of columnList) {
      //skip null columns (seem to be "collection")
      if (c.sys_update_name === `sys_dictionary_${c.name}_null`) {
        continue;
      }
      //all should have a column id...
      let id = c.element;
      if (!id) {
        throw `Expected "element" property to be set`;
      }
      //add actual choice list where possible
      if (id in tableChoices) {
        c.choice_map = tableChoices[id];
      }
      //validate column schema
      columns[id] = c;
    }
    table.columns = columns;
    //recurse into superclass
    if (table.super_class) {
      table.super_class = await this.getRaw(table.super_class);
    }
    //ready
    return table;
  }

  async getTableSysID(name) {
    let table = one(
      await this.client.do({
        url: `/v2/table/sys_db_object`,
        params: { sysparm_query: `name=${name}`, sysparm_fields: "sys_id" }
      })
    );
    if (!table) {
      throw `Table "${name}" not found`;
    }
    return table.sys_id;
  }

  async getTableColumnSysID(tableName, colName) {
    let table = one(
      await this.client.do({
        url: `/v2/table/sys_dictionary`,
        params: {
          sysparm_query: `name=${tableName}^element=${colName}`,
          sysparm_fields: "sys_id"
        }
      })
    );
    if (!table) {
      throw `Table "${tableName}" column "${colName}" not found`;
    }
    return table.sys_id;
  }

  /**
   * Create a servicenow table.
   * @param {string} tableName The target table
   */
  async create(tableSpec) {
    if (!tableSpec) {
      throw `Missing table spec`;
    }
    let { name, label, parent } = tableSpec;
    let parentSysID;
    if (!name) {
      throw `Missing table name`;
    } else if (!/^u_/.test(name)) {
      throw `Table name (${name}) must begin with "u_"`;
    } else if (!name) {
      throw `Missing table name`;
    }
    if (parent) {
      if (isGUID(parent)) {
        parentSysID = parent;
      } else {
        parentSysID = await this.getTableSysID(parent);
      }
      let spec = await this.get(parentSysID);
      if (!spec.is_extendable) {
        throw `Parent table (${spec.name}) is not extendable`;
      }
    }
    let table = {
      name,
      super_class: parentSysID,
      label,
      access: "public",
      sys_scope: "global",
      ws_access: "true"
    };
    this.log(`add table "${name}"`, table);
    return await this.client.create("sys_db_object", table);
  }
  /**
   * Delete a servicenow table. Very dangerous :O
   * @param {string} tableName The target table
   */
  async delete(tableSysID) {
    if (!isGUID(tableSysID)) {
      throw `Invalid sys_id`;
    }
    throw `Not implemented yet`;
  }

  /**
   * Create a table column.
   * @param {string} tableName The target table
   * @param {object} columnSpec The new column to create.
   */
  async createColumn(tableName, columnSpec) {
    let col = snColumn(columnSpec);
    if (!/^u_/.test(col.element)) {
      throw `Column name (${col.element}) must begin with "u_"`;
    }
    //required fields for creation
    col.active = true;
    col.display = false;
    col.name = tableName;
    //ready!
    this.log(`table "${tableName}": add column "${col.element}"`, col);
    await this.client.create("sys_dictionary", col);
    //update success, has choice list? sync that too
    if (columnSpec.choice && columnSpec.choice_map) {
      await this.syncChoices(tableName, col.element, columnSpec.choice_map);
    }
    return true;
  }

  /**
   * Updates a column in a table.
   * @param {string} tableName The target table
   * @param {object} columnName The existing column to update (must include sys_id).
   */
  async updateColumn(tableName, columnSpec) {
    let col = snColumn(columnSpec);
    let userColumn = /^u_/.test(col.element);
    let syncChoices = Boolean(columnSpec.choice_map);
    if (!syncChoices && !userColumn) {
      throw `Column name (${col.element}) must begin with "u_"`;
    }
    //update user column
    if (userColumn) {
      this.log(`table "${tableName}": update column "${col.element}"`, col);
      await this.client.update("sys_dictionary", col);
    }
    //update choice list
    if (syncChoices) {
      await this.syncChoices(tableName, col.element, columnSpec.choice_map);
    }
    return true;
  }

  async syncChoices(tableName, colName, choiceMap) {
    if (!choiceMap) {
      throw `Column (${colName}) missing choice map`;
    }
    this.log(`table "${tableName}": update column "${colName}": sync choices`);
    let choiceList = await this.client.do({
      url: `/v2/table/sys_choice`,
      params: {
        sysparm_exclude_reference_link: true,
        sysparm_query: `name=${tableName}^element=${colName}`
      }
    });
    for (let value in choiceMap) {
      let label = choiceMap[value];
      //find existing choice
      let existingChoice = null;
      for (let i = 0; i < choiceList.length; i++) {
        let c = choiceList[i];
        if (c.value === value) {
          existingChoice = c;
          choiceList.splice(i, 1);
          break;
        }
      }
      //new choice
      let newChoice = {
        name: tableName,
        element: colName,
        value,
        label,
        sys_domain: "global",
        inactive: false
      };
      //create new choice
      if (!existingChoice) {
        this.log("create choice list:", newChoice);
        await this.client.create("sys_choice", newChoice);
        continue;
      }
      if (isEqual(existingChoice, newChoice)) {
        continue; //no changes!
      }
      //update choice
      let updateChoice = {
        sys_id: existingChoice.sys_id,
        ...newChoice
      };
      this.log("update choice list:", updateChoice);
      await this.client.update("sys_choice", updateChoice);
    }
    //delete remaining choices
    for (let c of choiceList) {
      this.log("delete choice list:", c);
      await this.client.delete("sys_choice", c);
    }
    //syncd!
    return true;
  }

  /**
   * Removes a column from a table.
   * @param {string} tableName The target table
   * @param {object} columnName The existing column to delete.
   */
  async deleteColumn(tableName, columnName) {
    if (!/^u_/.test(columnName)) {
      throw `Column name (${columnName}) must begin with "u_"`;
    }
    let sysId = await this.getTableColumnSysID(tableName, columnName);
    this.log(`table "${tableName}": delete column "${columnName}"`);
    return await this.client.delete("sys_dictionary", sysId);
  }

  /**
   * Sync a JS table spec with a servicenow table.
   * @param {object} table The table specification.
   * @param {object} opts Options to customise the sync.
   */
  async sync(table, opts) {
    if (!table || typeof table !== "object") {
      throw `Invalid table spec`;
    }
    let tableName = table.name;
    if (!tableName) {
      throw `Missing name from table spec`;
    }
    if (!opts) {
      opts = {};
    } else if (typeof opts !== "object") {
      throw `Invalid options`;
    }
    let newColumns = table.columns;
    //perform diff
    let pending = {};
    //fetch and validate servicenow columns
    let existingTable = await this.get(table.name);
    if (!existingTable) {
      //add table and sync all columns
      pending.table = {
        name: table.name,
        type: "create",
        description: `Create table "${table.name}"`,
        commit: async () => await this.create(table)
      };
    } else {
      //ensure parent rule is enforced
      if (table.parent && table.parent !== existingTable.parent) {
        throw `Parent table mismatch. Table "${table.name}" should have ` +
          `parent "${table.parent}" but found "${existingTable.parent}"`;
      }
      //add/update/remove columns
      let existingColumns = existingTable.columns;
      pending.columns = this.syncColumns(
        tableName,
        existingColumns,
        newColumns
      );
    }
    //commit pending actions straight away
    if (opts.commit) {
      return this.commitAll(pending);
    }
    return pending;
  }

  //sync columns returns a set of pending actions
  //but does not commit them.
  syncColumns(tableName, existingColumns, newColumns) {
    let pending = {};
    for (let dmId in newColumns) {
      let col = newColumns[dmId];
      let { id, name } = col;
      if (id !== name && id in existingColumns) {
        pending[dmId] = {
          name: id,
          type: "error",
          description:
            `Column "${id}" already exists on "${tableName}" but ` +
            `attempting to create as "${name}"`
        };
        continue;
      }
      let hasColumn = name in existingColumns;
      //doesnt exist, create!
      if (!hasColumn) {
        if (!name.startsWith("u_")) {
          pending[dmId] = {
            name,
            type: "error",
            description:
              `Create column "${name}" ` +
              `not allowed, does not start with "u_"`
          };
          continue;
        }
        pending[dmId] = {
          name,
          type: "create",
          description: `Create new column "${name}"`,
          commit: async () => await this.createColumn(tableName, col)
        };
        continue;
      }
      //already exists? need updating?
      let ecol = existingColumns[name];
      let ncol = {
        name: name,
        sys_id: ecol.sys_id
      };
      let detail = [];
      //update these columns
      let changedField = false;
      for (let k of ["label", "max_length", "choice"]) {
        let v = ecol[k];
        if (k === "choice" && "choice" in ecol) {
          if (v === "nullable") {
            v = 1;
          } else if (v === "suggestion") {
            v = 2;
          } else if (v === "required") {
            v = 3;
          } else {
            throw `sync-column: unknown choice string "${v}"`;
          }
        }
        if (k in col && col[k] !== v) {
          changedField = true;
          ncol[k] = col[k];
          detail.push(`"${k}" from "${v}" to "${col[k]}"`);
        }
      }
      //sync choice map?
      let changedChoices = false;
      if (col.choice_map) {
        if (!isEqual(col.choice_map, ecol.choice_map)) {
          detail.push(`sync choice list`);
          changedChoices = true;
          ncol.choice_map = col.choice_map;
        }
      }
      let changed = changedField || changedChoices;
      if (!changed) {
        continue;
      }
      //ensure these columns match
      let match = true;
      for (let k of ["type", "reference_table"]) {
        if (k in col && col[k] !== ecol[k]) {
          pending[dmId] = {
            name,
            type: "error",
            description:
              `Column "${name}" ` +
              `${k} differs (${ecol[k]} => ${col[k]}), ` +
              `however it cannot be updated`
          };
          match = false;
          break;
        }
      }
      if (!match) {
        continue;
      }
      //ensure changes are allowed
      let prevent = null;
      if (changedField) {
        if (tableName !== ecol.table) {
          prevent = `column exists on parent table (${ecol.table})`;
        } else if (!name.startsWith("u_")) {
          prevent = `column is out-of-the-box`;
        }
      }
      if (prevent) {
        pending[dmId] = {
          name,
          type: "error",
          description:
            `Update column "${name}" (${detail.join(",")}) from ` +
            `not allowed, ${prevent}`
        };
        continue;
      }
      //changes are valid!!! update!
      pending[dmId] = {
        name,
        type: "update",
        description: `Update column "${name}" (${detail.join(",")})`,
        commit: async () => await this.updateColumn(tableName, ncol)
      };
    }
    //
    let index = {};
    for (let dmId in newColumns) {
      let col = newColumns[dmId];
      index[col.name] = col;
    }
    for (let name in existingColumns) {
      if (!name.startsWith("u_")) {
        continue; //can only delete user columns
      }
      if (name in index) {
        continue; //found in both, dont delete
      }
      let col = existingColumns[name];
      if (col.table !== tableName) {
        continue; //found on another table, dont delete
      }
      if (col.sys_created_by !== this.client.username) {
        continue; //can only delete columns made by us
      }
      pending[name] = {
        name: name,
        type: "delete",
        description: `Delete column "${name}"`,
        commit: async () => await this.deleteColumn(tableName, name)
      };
    }
    return pending;
  }

  /**
   * Commit a set of pending changes to a servicenow table.
   * @param {object} pending The pending operations.
   */
  async commitAll(pending) {
    let columnChanges = Object.values(pending.columns);
    let errors = columnChanges.filter(p => p.type === "error");
    if (errors.length > 0) {
      throw `Cannot commit changes, encountered ${errors.length} errors`;
    }
    this.log(`committing all changes`);
    if (pending.table) {
      this.log(pending.table.description);
      await pending.table.commit();
    }
    for (let change of columnChanges) {
      this.log(change.description);
      await change.commit();
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
