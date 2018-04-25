const sync = require("sync");
const API_CONCURRENCY = 40;

//split out table delta functionality
module.exports = class CDelta {
  constructor(client) {
    this.client = client;
  }

  /**
   * delta.merge, syncs objects in an array with a ServiceNow table
   * @param {string} tableName The target import table
   * @param {array} incomingRows
   * @param {any} status The log-status instance for this task, see app/etl/log-status.js
   */
  async merge(run = {}) {
    const { tableName, rows, status = console } = run;
    if (!tableName) {
      throw "No table specified";
    } else if (!Array.isArray(rows)) {
      throw `Incoming rows must be an array`;
    }
    //
    let { primaryKey, deletedFlag, allowDeletes } = run;
    if (!primaryKey) {
      throw `Primary key required`;
    }
    if (!deletedFlag) {
      deletedFlag = "u_in_datamart";
    }
    if (allowDeletes !== true) {
      allowDeletes = false;
    }
    //load all existing rows
    const existingRows = await this.client.getRecords(tableName, { status });
    if (!Array.isArray(existingRows)) {
      throw `Existing rows must be an array`;
    }
    const incomingRows = rows;
    const rawRows = {
      incoming: incomingRows,
      existing: existingRows
    };
    //process and index all existing and incoming rows
    const processedRows = {
      incoming: [],
      existing: []
    };
    const index = {
      incoming: {},
      existing: {}
    };
    //validate and index all rows
    for (const type in rawRows) {
      let missing = 0;
      const duplicates = new Set();
      for (let row of rawRows[type]) {
        let cid = row[primaryKey];
        if (!cid) {
          missing++;
          continue;
        }
        if (cid in index[type]) {
          duplicates.add(cid);
          continue;
        }
        let snRow = await this.client.schema.convertSN(tableName, row);
        index[type][cid] = snRow;
        processedRows[type].push(snRow);
      }
      if (missing > 0) {
        status.warn(`Found #${missing} ${type} rows with no "${primaryKey}"`);
      }
      if (duplicates.size > 0) {
        status.warn(`Found #${duplicates.size} ${type} duplicate rows`);
      }
    }
    status.log(
      `delta merging #${
        processedRows.incoming.length
      } entries into ${tableName}, ` +
        `found #${processedRows.existing.length} existing entries`
    );
    let rowsMatched = 0;
    //split into three groups
    let pending = {
      create: [],
      update: [],
      delete: []
    };
    //load table schema
    let schema = await this.client.schema.get(tableName);
    //pending creates/updates
    for (let incomingRow of processedRows.incoming) {
      //incoming row exists => currently in datamart
      if (deletedFlag in schema) {
        incomingRow[deletedFlag] = "1"; //true
      }
      let cid = incomingRow[primaryKey];
      let existingRow = index.existing[cid];
      //create
      if (!existingRow) {
        //first discovered now!
        if (
          "first_discovered" in schema &&
          !("first_discovered" in incomingRow)
        ) {
          incomingRow.first_discovered = new Date();
        }
        pending.create.push(incomingRow);
        continue;
      }
      if (!existingRow.sys_id) {
        throw `Existing row missing "sys_id"`;
      }
      //compare against existing row
      let payload = {};
      let changed = false;
      for (let k in incomingRow) {
        //check this column is actually in the schema
        if (!(k in schema)) {
          status.warn(`Found undefined column "${k}"`);
          continue;
        }
        let incomingVal = incomingRow[k];
        let existingVal = existingRow[k];
        //values are all strings.
        //values are all either defined or not.
        //undefined values are the empty string.
        if (incomingVal === undefined) {
          incomingVal = "";
        }
        if (existingVal === undefined) {
          existingVal = "";
        }
        //compare using their
        if (JSON.stringify(incomingVal) !== JSON.stringify(existingVal)) {
          changed = true;
          payload[k] = incomingVal;
          this.debug(
            `${tableName}: ${cid}: ${k}: '${existingVal}' => '${incomingVal}'`
          );
        }
      }
      if (changed) {
        //ensure correct sys_id
        payload.sys_id = existingRow.sys_id;
        payload.sys_class_name = existingRow.sys_class_name;
        pending.update.push(payload);
      } else {
        rowsMatched++;
      }
    }
    //pending deletes
    for (let existingRow of processedRows.existing) {
      let cid = existingRow[primaryKey];
      if (cid in index.incoming) {
        continue;
      }
      let payload = {
        sys_id: existingRow.sys_id
      };
      //missing from incoming, delete it!
      if (deletedFlag in schema) {
        if (existingRow[deletedFlag] === "0") {
          continue; //already deleted
        }
        payload[deletedFlag] = "0";
      }
      pending.delete.push(payload);
    }
    //note incoming api actions
    let msg = [];
    for (let action in pending) {
      let count = pending[action].length;
      if (count > 0) {
        msg.push(`${action} #${count} rows`);
      }
    }
    if (msg.length === 0) {
      status.log(`No changes`);
      return {
        rowsMatched,
        rowsCreated: 0,
        rowsUpdated: 0,
        rowsDeleted: 0
      };
    }
    status.log(msg.join(", "));
    status.add(
      pending.create.length + pending.update.length + pending.delete.length
    );
    //de-activate data policy
    await this.client.policy.toggle(tableName, false);
    //try-catch to ensure we always re-active data policy
    try {
      //create all
      this.log(`creating #${pending.create.length}`);
      await sync.each(API_CONCURRENCY, pending.create, async row => {
        //perform creation
        await this.client.create(tableName, row);
        //mark 1 action done
        status.done();
      });
      //update all
      this.log(`updating #${pending.update.length}`);
      await sync.each(API_CONCURRENCY, pending.update, async row => {
        //perform update
        await this.client.update(tableName, row);
        //mark 1 action done
        status.done();
      });
      //delete all
      this.log(`deleting #${pending.delete.length}`);
      await sync.each(API_CONCURRENCY, pending.delete, async row => {
        //perform deletion
        if (allowDeletes) {
          //permanently delete existing
          await this.client.delete(tableName, row);
        } else {
          //"delete" existing (sets deleted flag)
          await this.client.update(tableName, row);
        }
        //mark 1 action done
        status.done();
      });
    } catch (err) {
      throw err;
    } finally {
      //re-activate data policy
      await this.client.policy.toggle(tableName, true);
    }
    //provide merge results
    return {
      rowsMatched,
      rowsCreated: pending.create.length,
      rowsUpdated: pending.update.length,
      rowsDeleted: pending.delete.length
    };
  }

  log(...args) {
    this.client.log("[delta]", ...args);
  }

  debug(...args) {
    this.client.debug("[delta]", ...args);
  }
};
