const { one } = require("./util");
const { snColumn } = require("./util-table");
const isEqual = require("lodash.isequal");

module.exports = class CColumn {
  constructor(client) {
    this.client = client;
  }

  async getSysID(tableName, colName) {
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
   * Create a table column.
   * @param {string} tableName The target table
   * @param {object} columnSpec The new column to create.
   */
  async create(tableName, columnSpec) {
    let col = snColumn(columnSpec);
    if (!/^u_/.test(col.element)) {
      throw `Column name (${col.element}) must begin with "u_"`;
    }
    //required fields for creation
    col.active = true;
    col.display = false;
    col.name = tableName;
    //ready!
    this.log(`table ${tableName}: add column "${col.element}"`, col);
    await this.client.create("sys_dictionary", col);
    //create success, has choice list? sync that too
    if (columnSpec.choice && columnSpec.choice_map) {
      await this.client.choice.sync(
        tableName,
        col.element,
        columnSpec.choice_map
      );
    }
    //create data policy
    if (columnSpec.data_policy) {
      let readOnly = columnSpec.data_policy === "readonly";
      await this.client.policy.syncData(tableName, { [col.element]: readOnly });
    }
    return true;
  }

  /**
   * Updates a column in a table.
   * @param {string} tableName The target table
   * @param {object} columnName The existing column to update (must include sys_id).
   */
  async update(tableName, columnSpec) {
    let col = snColumn(columnSpec);
    let userColumn = /^u_/.test(col.element);
    let syncColumn = Object.keys(col).length > 2;
    if (syncColumn && !userColumn) {
      throw `Column name (${col.element}) must begin with "u_"`;
    }
    //update user column
    if (syncColumn) {
      this.log(`table ${tableName}: update column "${col.element}"`, col);
      await this.client.update("sys_dictionary", col);
    }
    //update choice list
    if (columnSpec.choice_map) {
      await this.client.choice.sync(
        tableName,
        col.element,
        columnSpec.choice_map
      );
    }
    //update data policy
    if (columnSpec.data_policy) {
      let readOnly = columnSpec.data_policy === "readonly";
      await this.client.policy.syncData(tableName, { [col.element]: readOnly });
    }
    return true;
  }

  /**
   * Removes a column from a table.
   * @param {string} tableName The target table
   * @param {object} columnName The existing column to delete.
   */
  async delete(tableName, columnName) {
    if (!/^u_/.test(columnName)) {
      throw `Column name (${columnName}) must begin with "u_"`;
    }
    let sysId = await this.getSysID(tableName, columnName);
    this.log(`table ${tableName}: delete column "${columnName}"`);
    return await this.client.delete("sys_dictionary", sysId);
  }

  //sync columns returns a set of pending actions
  //but does not commit them.
  sync(tableName, existingColumns, newColumns) {
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
          commit: async () => await this.create(tableName, col)
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
          detail.push(`choice list`);
          changedChoices = true;
          ncol.choice_map = col.choice_map;
        }
      }
      //sync data policy
      let changedDataPolicy = false;
      if (col.data_policy !== ecol.data_policy) {
        detail.push(
          `data policy "${ecol.data_policy}" => "${col.data_policy}"`
        );
        changedDataPolicy = true;
        ncol.data_policy = col.data_policy;
      }
      //anything changed?
      let changed = changedField || changedChoices || changedDataPolicy;
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
        commit: async () => await this.update(tableName, ncol)
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
        commit: async () => await this.delete(tableName, name)
      };
    }
    return pending;
  }

  log(...args) {
    this.client.log("[column]", ...args);
  }
};
