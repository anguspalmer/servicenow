const sync = require("sync");
const { one, isGUID } = require("./util");
const { expandTable } = require("./util-table");

//split out table editing functionality
module.exports = class CTable {
  constructor(client) {
    this.client = client;
  }

  async getSysID(name) {
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
        //find in-use label
        let rawDoc = raw.doc || {};
        let label = rawDoc.label || raw.column_label;
        //trim doc
        delete rawDoc.label;
        if (
          rawDoc.plural &&
          (rawDoc.plural === label || rawDoc.plural === `${label}s`)
        ) {
          delete rawDoc.plural;
        }
        if (rawDoc.language === "en") {
          delete rawDoc.language;
        }
        if (Object.keys(rawDoc).length === 0) {
          rawDoc = null;
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
          if (rawDoc) {
            col.doc = rawDoc;
          }
          //use last label
          col.label = label;
          //use LAST parent (source parent)
          col.table = rawTable.name;
          continue;
        }
        let col = {};
        col.name = id;
        col.label = label;
        col.table = rawTable.name;
        col.data_policy = raw.data_policy;
        col.choice_map = raw.choice_map;
        if (rawDoc) {
          col.doc = rawDoc;
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
    let [columnList, choiceList, ruleList, docList] = await sync.wait([
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
          sysparm_query: `name=${table.name}`,
          sysparm_fields: "element,value,label"
        }
      }),
      this.client.do({
        url: `/v2/table/sys_data_policy_rule`,
        params: {
          sysparm_exclude_reference_link: true,
          sysparm_query:
            `table=${table.name}^` + `sys_created_by=${this.client.username}`,
          sysparm_fields: "field,disabled"
        }
      }),
      this.client.do({
        url: `/v2/table/sys_documentation`,
        params: {
          sysparm_query: `name=${table.name}`,
          sysparm_fields: `element,label,plural,help,hint,language,url`
        }
      })
    ]);
    //extract and group choice fields
    const tableChoices = {};
    for (const choice of choiceList) {
      const id = choice.element;
      let col = tableChoices[id];
      if (!col) {
        col = {};
        tableChoices[id] = col;
      }
      col[choice.value] = choice.label;
    }
    //extract and group rules
    const columnDataPolicies = {};
    for (const rule of ruleList) {
      if (rule.disabled === "true") {
        columnDataPolicies[rule.field] = "readonly";
      } else if (rule.disabled === "false") {
        columnDataPolicies[rule.field] = "writable";
      }
    }
    //extract column docs
    const columnDocs = {};
    for (const doc of docList) {
      //cleanup
      const id = doc.element;
      delete doc.element;
      //store
      if (id) {
        columnDocs[id] = doc;
      } else if (!table.doc) {
        table.doc = doc;
      } else {
        this.log(`unexpected doc on table ${table.name}:`, doc);
      }
    }
    //validate table schema
    let columns = {};
    for (let column of columnList) {
      //skip null columns (seem to be "collection")
      if (column.sys_update_name === `sys_dictionary_${column.name}_null`) {
        continue;
      }
      //all should have a column id...
      let id = column.element;
      if (!id) {
        throw `Expected "element" property to be set`;
      }
      //add actual choice list where possible
      if (id in tableChoices) {
        column.choice_map = tableChoices[id];
      }
      //add data policy settings where possible
      if (id in columnDataPolicies) {
        column.data_policy = columnDataPolicies[id];
      }
      //add doc where applicable
      if (id in columnDocs) {
        column.doc = columnDocs[id];
      }
      //validate column schema
      columns[id] = column;
    }
    table.columns = columns;
    //recurse into superclass
    if (table.super_class) {
      table.super_class = await this.getRaw(table.super_class);
    }
    //ready
    return table;
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
        parentSysID = await this.getSysID(parent);
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
    //validate provided options and expand into defaults
    expandTable(table);
    //check sync option
    if (!opts) {
      opts = {};
    } else if (typeof opts !== "object") {
      throw `Invalid options`;
    }
    let newColumns = table.columns;
    //perform diff
    let pending = {};
    //fetch and validate servicenow columns
    this.log("sync pre-fetch existing table:", table.name);
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
      pending.columns = this.client.column.sync(
        tableName,
        existingColumns,
        newColumns
      );
    }
    //commit pending actions straight away
    if (opts.commit && (pending.table || pending.columns)) {
      this.log("sync table commit all changes:", table.name);
      const newTable = pending.table && !pending.columns;
      //commit all
      await this.commitAll(pending);
      //commit all again?
      if (newTable) {
        //wait a bit...
        sync.sleep(2000);
        //recurse once to get column changes
        let p = await this.sync(table);
        const newCols = !p.table && p.columns;
        if (!newCols) {
          throw `expected new cols after table change`;
        }
        await this.commitAll(p);
      }
      //commited!
      return true;
    }
    return pending;
  }

  /**
   * Commit a set of pending changes to a servicenow table.
   * @param {object} pending The pending operations.
   */
  async commitAll(pending) {
    this.log(`committing all changes`);
    if (pending.table) {
      this.log(pending.table.description);
      await pending.table.commit();
    }
    if (pending.columns) {
      let columnChanges = Object.values(pending.columns);
      let errors = columnChanges.filter(p => p.type === "error");
      if (errors.length > 0) {
        throw `Cannot commit changes, encountered ${errors.length} errors: ` +
          errors.map(p => p.description).join(",");
      }
      for (let change of columnChanges) {
        this.log(change.description);
        await change.commit();
      }
    }
    return true;
  }

  log(...args) {
    this.client.log("[table]", ...args);
  }
};
