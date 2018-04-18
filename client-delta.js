const sync = require("sync");
const { convertSN } = require("./util");
const API_CONCURRENCY = 40;

//split out table delta functionality
module.exports = class ServiceNowClientTable {
  constructor(client) {
    this.client = client;
  }

  /**
   * delta.merge, syncs objects in an array with a ServiceNow table
   * @param {string} tableName The target import table
   * @param {array} incomingRows
   * @param {any} status The log-status instance for this task, see app/etl/log-status.js
   */
  async merge(tableName, incomingRows, status = console, opts = {}) {
    if (!tableName) {
      throw "No table specified";
    } else if (!Array.isArray(incomingRows)) {
      throw `Incoming rows must be an array`;
    }
    //
    let { primaryKey, allowDeletes } = opts;
    if (!primaryKey) {
      throw `Primary key required`;
    }
    const deletedFlag = allowDeletes ? null : "u_in_datamart";
    //load table schema
    let schema = await this.client.getSchema(tableName);
    //load all existing rows
    let existingRows = await this.client.get(tableName);
    if (!Array.isArray(existingRows)) {
      throw `Existing rows must be an array`;
    }
    let allRows = {
      incoming: incomingRows,
      existing: existingRows
    };
    let rows = {
      incoming: [],
      existing: []
    };
    let index = {
      incoming: {},
      existing: {}
    };
    //validate and index all rows
    for (let type in allRows) {
      let missing = 0;
      let duplicates = {};
      for (let row of allRows[type]) {
        let cid = row[primaryKey];
        if (!cid) {
          missing++;
          continue;
        }
        if (cid in index[type]) {
          duplicates[cid] = true;
          continue;
        }
        let snRow = convertSN(schema, row);
        index[type][cid] = snRow;
        rows[type].push(snRow);
      }
      if (missing > 0) {
        status.warn(`Found #${missing} ${type} rows with no "${primaryKey}"`);
      }
      let numDuplicates = Object.keys(duplicates).length;
      if (numDuplicates > 0) {
        status.warn(`Found #${numDuplicates} ${type} duplicate rows`);
      }
    }
    status.log(
      `delta merging #${rows.incoming.length} entries into ${tableName}, ` +
        `found #${rows.existing.length} existing entries`
    );
    let rowsMatched = 0;
    //split into three groups
    let pending = {
      create: [],
      update: [],
      delete: []
    };
    //pending creates/updates
    for (let incomingRow of rows.incoming) {
      //incoming row exists => currently in datamart
      if (deletedFlag) {
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
        let s = schema[k];
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
          this.log(
            `DEBUG: ${tableName}: ${cid}: ${k}: '${existingVal}' => '${incomingVal}'`
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
    for (let existingRow of rows.existing) {
      let cid = existingRow[primaryKey];
      if (cid in index.incoming) {
        continue;
      }
      let payload = {
        sys_id: existingRow.sys_id
      };
      //missing from incoming, delete it!
      if (deletedFlag) {
        //already "deleted"?
        if (existingRow[deletedFlag] === "0") {
          continue;
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
    if (status) {
      status.add(
        pending.create.length + pending.update.length + pending.delete.length
      );
    }
    //de-activate data policy
    await this.client.table.toggleDataPolicy(tableName, false);
    //try-catch to ensure we always re-active data policy
    try {
      //create all
      await sync.each(API_CONCURRENCY, pending.create, async row => {
        //perform creation
        await this.client.create(tableName, row);
        //mark 1 action done
        if (status) {
          status.done();
        }
      });
      //update all
      await sync.each(API_CONCURRENCY, pending.update, async row => {
        //perform update
        await this.client.update(tableName, row);
        //mark 1 action done
        if (status) {
          status.done();
        }
      });
      //delete all
      await sync.each(API_CONCURRENCY, pending.delete, async row => {
        //perform deletion
        if (deletedFlag && deletedFlag in row) {
          //"delete" existing
          await this.client.update(tableName, row);
        } else {
          //permanently delete existing
          await this.client.delete(tableName, row);
        }
        //mark 1 action done
        if (status) {
          status.done();
        }
      });
    } catch (err) {
      throw err;
    } finally {
      //re-activate data policy
      await this.client.table.toggleDataPolicy(tableName, true);
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
};

/**
 * deltaImportAll objects in an array to the ServiceNow via the Import API
 * @param {string} tableName The target import table
 * @param {array} rows Synchonously send with backoff when throttled by ServiceNow.
 * @param {any} status The log-status instance for this task, see app/etl/log-status.js
 */
// async deltaImportAll(tableName, rows, status) {
//   if (!tableName) {
//     throw "No table specified";
//   } else if (!/^u_imp_dm_/.test(tableName)) {
//     throw `Invalid table specified (${tableName})`;
//   } else if (!Array.isArray(rows)) {
//     throw `Rows must be an array`;
//   } else if (rows.length === 0) {
//     throw `Rows are empty`;
//   }
//   //get existing 0-50%, delta changes 50-100%
//   status.setStages(2);
//   // TODO check firstRow for use of non-"u_" columns?
//   // let firstRow = rows[0];
//   //fetch this tables schema
//   let schema = await this.getSchema(tableName);
//   //prepare hash helper making use of schema
//   let hashRow = createRowHash(schema);
//   //rows to add and remove
//   let pending = { add: [], remove: [] };
//   //index all incoming rows
//   let incomingIndex = {};
//   for (let row of rows) {
//     let h = hashRow(row);
//     if (h in incomingIndex) {
//       status.warn(`Duplicate row:`, row);
//     }
//     incomingIndex[h] = row;
//   }
//   //load all existing imports
//   let existingImports = await this.get(tableName, null, null, status);
//   let matchedIndex = {};
//   for (let row of existingImports) {
//     let h = hashRow(row);
//     let match = h in incomingIndex;
//     if (match) {
//       //existing row matched, don't delete, don't import
//       matchedIndex[h] = true;
//     } else {
//       //existing row not matched needs to be deleted
//       pending.remove.push(row);
//     }
//   }
//   //incoming rows unmatched in the index need to be added
//   for (let h in incomingIndex) {
//     if (!matchedIndex[h]) {
//       let row = incomingIndex[h];
//       pending.add.push(row);
//     }
//   }
//   //categorised, perform api calls!
//   status.doneStage();
//   status.add(pending.add.length + pending.remove.length);
//   //stats (no update, create == import)
//   let rowsCreated = 0;
//   let rowsUpdated = 0;
//   let rowsDeleted = 0;
//   let rowsErrored = 0;
//   let messages = [];
//   //delete rows
//   if (pending.remove.length > 0) {
//     status.message(`Deleting #${pending.remove.length} unmatched rows...`);
//     await sync.each(API_CONCURRENCY, pending.remove, async row => {
//       await this.delete(tableName, row);
//       rowsDeleted++;
//       status.done(1);
//     });
//   }
//   //add rows
//   if (pending.add.length > 0) {
//     status.message(`Importing #${pending.add.length} changed rows...`);
//     //collect errors
//     let errorMessages = {};
//     //unmatched rows are ready for import!
//     await sync.each(API_CONCURRENCY, pending.add, async row => {
//       let data, error;
//       try {
//         data = await this.import(tableName, row);
//         //import done
//         rowsCreated++;
//         //check transform result, can still fail...
//         let { result } = data;
//         let changed = false;
//         for (let r of result) {
//           let ignore =
//             r.status_message &&
//             r.status_message.startsWith("Row transform ignored");
//           if (r.status === "error" && !ignore) {
//             error = r.error_message;
//           }
//           if (r.status === "updated" || r.status === "inserted") {
//             changed = true;
//           }
//         }
//         //data was actually updated
//         if (changed) {
//           rowsUpdated++;
//         }
//       } catch (err) {
//         error = err.toString();
//       }
//       //done!
//       if (error) {
//         if (error.length > 4096) {
//           //prevent 20MB from being accidently inserting into the DB :(
//           error = error.slice(0, 4096);
//         }
//         rowsErrored++;
//         let n = errorMessages[error] || 1;
//         errorMessages[error] = n + 1;
//       }
//       //mark 1 action done
//       status.done(1);
//     });
//     for (let msg in errorMessages) {
//       let n = errorMessages[msg];
//       status.warn(`${msg} (occured ${n} times)`); //add as warnings
//     }
//   }
//   if (rowsCreated > 0) {
//     messages.push(`${rowsCreated} imported`);
//   }
//   if (rowsUpdated > 0) {
//     messages.push(`${rowsUpdated} changes`);
//   }
//   if (rowsDeleted > 0) {
//     messages.push(`${rowsDeleted} cleared`);
//   }
//   if (rowsErrored > 0) {
//     messages.push(`${rowsErrored} errored`);
//   }
//   if (messages.length === 0) {
//     messages.push(`No changes`);
//   }
//   //all done
//   status.log(`[snc] delta-import: ${tableName}: ${messages.join(", ")}`);
//   status.doneStage();
//   return {
//     rowsCreated,
//     rowsUpdated,
//     rowsDeleted,
//     rowsErrored
//   };
// }
