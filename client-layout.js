const sync = require("sync");
const { one } = require("./util");

module.exports = class CLayout {
  constructor(client) {
    this.client = client;
  }

  async resetList() {
    throw `UNABLE TO IMPLEMENT`;
  }

  async resetForm(tableName) {
    throw `UNABLE TO IMPLEMENT`;
    // tables can be edited, however changes are not reflected in the UI.
    // change only seems to occur when you hit Save on the slush bucket...
    const table = await this.client.table.get(tableName);
    const columns = Object.values(table.columns)
      .filter(col => !col.name.startsWith("sys_"))
      .sort(
        (a, b) =>
          +new Date(a.sys_created_on) < +new Date(b.sys_created_on) ? -1 : 1
      );

    const user = await this.client.getUser();
    if (!user) {
      throw `Not authenticated`;
    }
    this.log(`reset: ${tableName}: create sys_ui_section`);
    //section getter
    const getParams = {
      url: "/v2/table/sys_ui_section",
      params: {
        sysparm_query: `name=${tableName}`
      }
    };
    const get = async () => one(await this.client.do(getParams));
    //upsert
    let section = await get();
    if (!section) {
      this.log(`create section:`, tableName);
      await this.client.create("sys_ui_section", {
        title: true,
        header: false,
        view: "Default view",
        name: tableName,
        sys_domain: user.sys_domain.value
      });
      section = await get();
    }
    if (!section.sys_id) {
      throw "missing sys id";
    }
    //diff columns
    const existing = await this.client.getRecords("sys_ui_element", {
      fields: ["sys_id", "element", "position"],
      query: `sys_ui_section=${section.sys_id}`
    });
    const incoming = [];
    let position = 0;
    for (const column of columns) {
      incoming.push({
        sys_ui_section: section.sys_id,
        position,
        element: column.name
      });
      position++;
    }
    await sync.diff({
      prev: existing,
      next: incoming,
      index: el => `${el.position}|${el.element}`,
      create: async incoming => {
        this.log(`create element:`, incoming);
        await this.client.create("sys_ui_element", incoming);
      },
      delete: async existing => {
        this.log(`delete element:`, existing);
        await this.client.delete("sys_ui_element", existing);
      }
    });
  }

  log(...args) {
    this.client.log("[layout]", ...args);
  }
};
