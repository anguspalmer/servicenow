const sync = require("sync");
const crypto = require("crypto");
const { bindMethods } = require("misc");
const API_CONCURRENCY = 40;

//split out table delta functionality
module.exports = class CDelta {
  constructor(client) {
    bindMethods(this);
    this.client = client;
  }

  /**
   * delta.merge, syncs objects in an array with a ServiceNow table
   * @param {string} tableName The target import table
   * @param {array} incomingRows
   * @param {any} status The log-status instance for this task, see app/etl/log-status.js
   */
  async merge(run = {}) {
    const { tableName, rows } = run;
    if (!tableName) {
      throw "No table specified";
    } else if (!Array.isArray(rows)) {
      throw `Incoming rows must be an array`;
    }
    let { primaryKey, deletedFlag, allowDeletes } = run;
    if (typeof primaryKey === "string") {
      //pick single key
      const k = primaryKey;
      primaryKey = row => row[k];
    }
    if (primaryKey === undefined) {
      //by default, hash all user keys and values
      primaryKey = row => {
        const h = crypto.createHash("md5");
        const keys = Object.keys(row)
          .filter(k => k.startsWith("u_"))
          .sort();
        for (const key of keys) {
          h.update(`${key}=${row[key]}`);
        }
        return h.digest("hex");
      };
    }
    if (typeof primaryKey !== "function") {
      throw `Primary key must be function or string`;
    }
    if (!deletedFlag) {
      deletedFlag = "u_in_datamart";
    }
    if (allowDeletes !== true) {
      allowDeletes = false;
    }
    let { status } = run;
    if (!status) {
      status = {
        log: this.log,
        warn: this.log.bind(null, "WARN"),
        debug: this.debug,
        add: () => {},
        done: () => {}
      };
    }
    //load all existing rows
    const existingRows = await this.client.getRecords(tableName, {
      // query: `sys_created_by: ${this.client.username}`,
      status,
      cache: true
    });
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
    const duplicates = {
      incoming: [],
      existing: []
    };
    //validate and index all rows
    for (const type in rawRows) {
      const missing = new Set();
      const duplicatePks = new Set();
      for (let row of rawRows[type]) {
        let snRow = await this.client.schema.convertSN(tableName, row);
        let cid = primaryKey(snRow);
        if (cid) {
          //only index rows with a pk
          if (cid in index[type]) {
            duplicatePks.add(cid);
            duplicates[type].push(row);
            continue;
          }
          index[type][cid] = snRow;
        } else {
          //log missing pks
          missing.add(snRow);
        }
        processedRows[type].push(snRow);
      }
      if (missing.size > 0) {
        status.warn(`found #${missing.size} ${type} rows with no primary key`);
      }
      if (duplicatePks.size > 0) {
        let action = "ignoring";
        if (allowDeletes && type === "existing") {
          action = "deleting";
        }
        status.warn(`${action} #${duplicatePks.size} ${type} duplicate rows`);
      }
    }
    status.log(
      `delta merging #${processedRows.incoming.length} ` +
        `entries into ${tableName}, ` +
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
    //determine if flag can be used
    const hasDeletedFlag = deletedFlag in schema;
    //pending creates/updates
    for (let incomingRow of processedRows.incoming) {
      //incoming row exists => currently in datamart
      if (hasDeletedFlag) {
        incomingRow[deletedFlag] = "1"; //mark exists
      }
      let cid = primaryKey(incomingRow);
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
          status.debug(
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
    //pending deletes (if deletes are possible)
    if (allowDeletes || hasDeletedFlag) {
      //find set of sys_ids to delete
      const sysIds = [];
      //check if not in the index
      for (let existingRow of processedRows.existing) {
        let cid = primaryKey(existingRow);
        if (!cid || cid in index.incoming) {
          continue;
        }
        if (
          !allowDeletes &&
          hasDeletedFlag &&
          existingRow[deletedFlag] === "0"
        ) {
          continue; //already "deleted"
        }
        sysIds.push(existingRow.sys_id);
      }
      //existing duplicates should be wiped
      for (let existingRow of duplicates.existing) {
        sysIds.push(existingRow.sys_id);
      }
      //prepare sn object
      for (let sysId of sysIds) {
        let payload = { sys_id: sysId };
        //missing from incoming, delete it!
        if (hasDeletedFlag) {
          payload[deletedFlag] = "0"; //mark deleted
        }
        pending.delete.push(payload);
      }
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
    //try-catch to ensure we always re-activate data policy
    try {
      //create all
      status.log(`creating #${pending.create.length}`);
      await sync.each(API_CONCURRENCY, pending.create, async row => {
        //perform creation
        await this.client.create(tableName, row);
        //mark 1 action done
        status.done();
      });
      //update all
      status.log(`updating #${pending.update.length}`);
      await sync.each(API_CONCURRENCY, pending.update, async row => {
        //perform update
        await this.client.update(tableName, row);
        //mark 1 action done
        status.done();
      });
      //delete all
      status.log(`deleting #${pending.delete.length}`);
      await sync.each(API_CONCURRENCY, pending.delete, async row => {
        //perform deletion
        if (allowDeletes) {
          //permanently delete existing
          await this.client.delete(tableName, row);
        } else if (hasDeletedFlag) {
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
