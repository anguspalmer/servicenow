module.exports = class CLayout {
  constructor(client) {
    this.client = client;
  }

  async resetList() {
    throw `UNABLE TO IMPLEMENT`;
  }

  async resetForm(/* tableName */) {
    throw `UNABLE TO IMPLEMENT`;
    // TODO
    //   pull list of columns sorted by creation date,
    //   exluding those columns with table creation date (matching "sys_id")
    // NOTE:
    //   GET sys_ui_section b70e2de64f66830088ba1d801310c796
    //   -> name: "u_imp_dm_backup_job"
    //  GET sys_ui_element sys_ui_section=b70e2de64f66830088ba1d801310c796 element,position limit:30
    //   -> element: "u_client"
    //      position: 0
    //   -> element: "u_company"
    //      position: 1
    //
    // tables can be edited, however changes are not reflected in the UI.
    // change only seems to occur when you hit Save on the slush bucket...
  }

  log(...args) {
    this.client.log("[layout]", ...args);
  }
};
