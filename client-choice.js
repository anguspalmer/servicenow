const sync = require("sync");
const { subsetOf } = require("./util");

module.exports = class CChoice {
  constructor(client) {
    this.client = client;
  }

  async sync(tableName, colName, choiceMap) {
    if (!choiceMap) {
      throw `Column (${colName}) missing choice map`;
    }
    let newChoices = [];
    for (let value in choiceMap) {
      newChoices.push({
        name: tableName,
        element: colName,
        value,
        label: choiceMap[value],
        inactive: false
      });
    }
    const loglist = newChoices.map(c => `${c.value}=${c.label}`).join(" ");
    this.log(
      `table ${tableName}: update column "${colName}": sync choices: ${loglist}`
    );
    let existingChoices = await this.client.do({
      url: `/v2/table/sys_choice`,
      params: {
        sysparm_exclude_reference_link: true,
        sysparm_query: `name=${tableName}^element=${colName}`
      }
    });
    //perform diff
    await sync.diff({
      prev: existingChoices,
      next: newChoices,
      index: "value",
      create: async newChoice => {
        this.log("create choice list:", newChoice);
        await this.client.create("sys_choice", newChoice);
      },
      equal: (existingChoice, newChoice) => subsetOf(newChoice, existingChoice),
      update: async (newChoice, existingChoice) => {
        newChoice.sys_id = existingChoice.sys_id;
        this.log("update choice list:", newChoice);
        await this.client.update("sys_choice", newChoice);
      },
      delete: async existingChoice => {
        this.log("delete choice list:", existingChoice);
        await this.client.delete("sys_choice", existingChoice);
      }
    });
    //syncd!
    return true;
  }

  log(...args) {
    this.client.log("[choice]", ...args);
  }
};
