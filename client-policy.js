const sync = require("sync");
const { one, subsetOf } = require("./util");

module.exports = class CPolicy {
  constructor(client) {
    this.client = client;
  }

  async toggle(tableName, isActive) {
    return await this.toggleData(tableName, isActive);
  }

  async toggleData(tableName, isActive) {
    let tablePolicy = await this.getMine("data", tableName);
    if (isActive === undefined) {
      isActive = !tablePolicy.active; //default to toggle
    }
    if (typeof isActive !== "boolean") {
      throw `expected isactive boolean`;
    }
    this.log(`${isActive ? "" : "un"}lock ${tableName} data policy...`);
    if (tablePolicy.active !== isActive) {
      let tableClass = tablePolicy.sys_class_name;
      await this.client.update(tableClass, {
        sys_id: tablePolicy.sys_id,
        active: isActive
      });
    }
    return true;
  }

  async getMine(type, tableName) {
    //ui/data policy differences:
    const isData = type === "data";
    const isUi = type === "ui";
    let policyTable = null;
    let policyTableRef = null;
    let policyDefaults = null;
    if (isData) {
      policyTable = "sys_data_policy2";
      policyTableRef = "model_table";
      policyDefaults = {
        apply_import_set: true,
        apply_soap: false,
        enforce_ui: true
      };
    } else if (isUi) {
      throw `Not supported. linking policy to rule is blocked by a system ACL.`;
      policyTable = "sys_ui_policy";
      policyTableRef = "table";
      policyDefaults = {
        global: true,
        on_load: true,
        run_scripts: false,
        ui_type: 0
      };
    } else {
      throw `Invalid type ${type}`;
    }
    //parent table sys id is required when creating new rules
    const get = async () => {
      let policies = await this.client.do({
        url: `/v2/table/${policyTable}`,
        params: {
          sysparm_exclude_reference_link: true,
          sysparm_query:
            `${policyTableRef}=${tableName}^` +
            `sys_created_by=${this.client.username}`
        }
      });
      return one(policies);
    };
    let tablePolicy = await get();
    //table policy missing! create it
    let targetPolicy = {
      ...policyDefaults,
      [policyTableRef]: tableName,
      conditions: `sys_created_by=${this.client.username}^EQ`,
      short_description: `Auto-generated ${type} policy`,
      inherit: false
    };
    //missing? create it
    if (!tablePolicy) {
      this.log(`table ${tableName}: create ${type} policy`);
      await this.client.create(policyTable, targetPolicy);
      tablePolicy = await get();
    }
    //fields dont match? update it
    if (!subsetOf(targetPolicy, tablePolicy)) {
      this.log(`table ${tableName}: update ${type} policy`);
      await this.client.update(policyTable, {
        sys_id: tablePolicy.sys_id,
        ...targetPolicy
      });
    }
    //sanity check
    if (!tablePolicy || !tablePolicy.sys_id) {
      throw `This should not happen`;
    }
    return tablePolicy;
  }

  async syncData(tableName, columns, opts) {
    return await this.sync("data", tableName, columns, opts);
  }

  async syncUi(tableName, columns, opts) {
    return await this.sync("ui", tableName, columns, opts);
  }

  async sync(type, tableName, columns, opts = {}) {
    //ui/data policy differences:
    const isData = type === "data";
    const isUi = type === "ui";
    let ruleTable = null;
    let rulePolicyRef = null;
    let ruleDefaults = null;
    if (isData) {
      ruleTable = "sys_data_policy_rule";
      rulePolicyRef = "sys_data_policy";
      ruleDefaults = {
        mandatory: "ignore"
      };
    } else if (isUi) {
      throw `Not supported. linking policy to rule is blocked by a system ACL.`;
      ruleTable = "sys_ui_policy_action";
      rulePolicyRef = "ui_policy";
      ruleDefaults = {
        mandatory: "ignore",
        visible: "ignore"
      };
    } else {
      throw `Invalid policy type ${type}`;
    }
    if (!tableName) {
      throw `Missing table name`;
    }
    //TODO customise mandatory/visible
    let { readOnly = true, doDeletes = false } = opts;
    if (typeof columns === "string") {
      columns = { [columns]: readOnly };
    } else if (Array.isArray(columns)) {
      const names = columns;
      columns = {};
      names.forEach(n => (columns[n] = readOnly));
    } else if (!columns || typeof columns !== "object") {
      throw `Columns should be a plain object (column => readonly bool)`;
    }
    for (let columnName in columns) {
      if (typeof columns[columnName] !== "boolean") {
        throw `Column (${columnName}) policy must be boolean (read-only flag)`;
      }
    }
    let columnNames = Object.keys(columns);
    if (columnNames.length === 0 && !doDeletes) {
      return true; //done!
    }
    this.log(
      `table ${tableName}: sync ${type} policy (#${columnNames.length} columns)`
    );

    let tablePolicy = await this.getMine(type, tableName);
    //delta sync policies
    //fetch policies for this table, created by this user!
    const newRules = [];
    for (const columnName in columns) {
      const readOnly = columns[columnName];
      newRules.push({
        ...ruleDefaults,
        [rulePolicyRef]: tablePolicy.sys_id,
        table: tableName,
        field: columnName,
        disabled: String(readOnly)
      });
    }
    //if were syncing one rule, query directly for it
    const singleQuery =
      newRules.length === 1 && !doDeletes ? `^field=${newRules[0].field}` : ``;
    //get list of all existing rules for this table
    const existingRules = await this.client.do({
      url: `/v2/table/${ruleTable}`,
      params: {
        sysparm_exclude_reference_link: true,
        sysparm_query:
          `table=${tableName}^` +
          `${rulePolicyRef}=${tablePolicy.sys_id}^` +
          `sys_created_by=${this.client.username}` +
          singleQuery
      }
    });
    //perform diff
    await sync.diff({
      prev: existingRules,
      next: newRules,
      index: "field",
      create: async newRule => {
        this.log(
          `table ${tableName}: "${newRule.field}": create ${type} policy rule`
        );
        await this.client.create(ruleTable, newRule);
      },
      equal: (existingRule, newRule) => subsetOf(newRule, existingRule),
      update: async (newRule, existingRule) => {
        this.log(
          `table ${tableName}: "${newRule.field}": update ${type} policy rule`
        );
        await this.client.update(ruleTable, {
          sys_id: existingRule.sys_id,
          ...newRule
        });
      },
      delete: async existingRule => {
        if (doDeletes) {
          this.log(
            `table ${tableName}: "${existingRule.field}": delete ${type} policy`
          );
          await this.client.delete(ruleTable, existingRule);
        }
      }
    });
    return true;
  }

  log(...args) {
    this.client.log("[policy]", ...args);
  }
};
