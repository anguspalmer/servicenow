const axios = require("axios");
const { cache } = require("cache");
const sync = require("sync");
const crypto = require("crypto");
const md5 = m =>
  crypto
    .createHash("md5")
    .update(m)
    .digest("hex");
const Backoff = require("backo");
const xml2js = require("xml2js");
const parseXML = sync.promisify(xml2js.parseString);

const fakeApi = require("./fake-api");
const API_CONCURRENCY = 40;

/**
 * @class
 * Simple client to grab all rows from a table in ServiceNow.
 * Returns results as JSON
 * Allows renaming of fields.
 * @example
 * let snc = new ServiceNowClient({
 *     user: "foo",
 *     pass: "bar",
 *     instance: "ac3dev"
 *   });
 *   let results = await snc.get("u_commvault_products")
 */
module.exports = class ServiceNowClient {
  constructor(config) {
    //validate config
    let { username, password, instance } = config;
    let fake = instance === "ac3dev" && (!username || !password);
    if (fake) {
      this.log(
        `No auth and environment is ${instance}, ServiceNow API will be faked.`
      );
    } else if (!username || !password || !instance) {
      throw "[snc] configuration incomplete";
    }
    this.instance = instance;
    this.defaultParams = {
      sysparm_exclude_reference_link: true
    };
    this.fake = fake;
    this.api = fake
      ? fakeApi
      : axios.create({
          baseURL: `https://${this.instance}.service-now.com/api/now`,
          auth: {
            username,
            password
          },
          validateStatus: () => true
        });
  }

  /**
   * Returns the number of rows in the given table.
   * @param {string} tableName
   */
  async getCount(tableName) {
    let resp = await this.api({
      method: "GET",
      url: `/v1/stats/${tableName}`,
      params: {
        sysparm_count: true
      }
    });
    if (resp.status !== 200) {
      throw `GET count failed: ${resp.statusText})`;
    }
    let count = prop(resp, "data", "result", "stats", "count");
    if (!/^\d+$/.test(count)) {
      throw `Invalid count response`;
    }
    return parseInt(count);
  }

  /**
   * Returns the schema of the given table.
   * @param {string} tableName
   */
  async getSchema(tableName) {
    let resp = await this.api({
      method: "GET",
      baseURL: `https://${this.instance}.service-now.com/`,
      url: `${tableName}.do?SCHEMA`
    });
    if (resp.status !== 200) {
      throw `GET schema failed: ${resp.statusText}`;
    }
    let contentType = resp.headers["content-type"];
    if (contentType !== "text/xml") {
      throw `GET schema failed: non-xml content type (${contentType})`;
    }
    let schema = await parseXML(resp.data);
    let elements = prop(schema, tableName, "element");
    if (!Array.isArray(elements)) {
      throw `GET schema failed: expected array of columns`;
    }
    let columns = {};
    elements.forEach(elem => {
      let attrs = elem.$;
      //require name and type
      let col = {};
      if (!attrs.name) {
        throw `Missing column name`;
      }
      col.name = attrs.name;
      delete attrs.name;
      if (!attrs.internal_type) {
        throw `Missing column type`;
      }
      col.type = attrs.internal_type;
      delete attrs.internal_type;
      //optional max length, with validation
      if (attrs.max_length) {
        let l = parseInt(attrs.max_length, 10);
        if (isNaN(l)) {
          throw `Invalid max length`;
        }
        col.maxLength = l;
        delete attrs.max_length;
      }
      //copy over rest
      for (let k in attrs) {
        let v = attrs[k];
        if (v === "true") {
          v = true;
        } else if (v === "false") {
          v = false;
        } else if (/^\d+$/.test(v)) {
          v = parseInt(v, 10);
        }
        col[k] = v;
      }
      columns[col.name] = col;
    });
    return columns;
  }

  /**
   * Returns the table info for the given table.
   * Like a more detailed schema.
   * @param {string} tableName
   */
  async getTableInfo(tableName) {}

  /**
   * Returns an array of json objects using the ServiceNow Table API
   * Use [columns] array to limit fields returned and rename fields.
   * Since ServiceNow adds a 'u_' to custom columns and this makes
   * code messy, use an object in the columns array to rename the column as in {old_name: 'new_name'}
   * @example get("u_dm_backup_retention_policy",['u_name',{u_code:'code'}]);
   * @param {string} tableName
   * @param {array} columns Optional array of columns to show. If null all columns are returned
   * @param {array} query Optional query string to filter records exactly as it appears in a ServiceNow list view
   */
  async get(tableName, columns, query, status) {
    return await this.getRecords(tableName, {
      columns,
      query,
      status
    });
  }

  /**
   * Returns an array of json objects using the ServiceNow Table API
   * Use [columns] array to limit fields returned and rename fields.
   * Since ServiceNow adds a 'u_' to custom columns and this makes
   * code messy, use an object in the columns array to rename the column as in {old_name: 'new_name'}
   * @example get("u_dm_backup_retention_policy",['u_name',{u_code:'code'}]);
   * @param {string} tableName
   * @param {array} columns Optional array of columns to show. If null all columns are returned
   * @param {array} query Optional query string to filter records exactly as it appears in a ServiceNow list view
   */
  async getRecords(tableName, opts = {}) {
    let { columns, query, status } = opts;
    let params = {
      ...this.defaultParams
    };
    let fields = [];
    let renameFields = {};
    // limit results to specified columns
    if (columns) {
      for (let c of columns) {
        if (typeof c == "object") {
          // handle the column renaming
          let name = Object.keys(c)[0];
          let newName = c[name];
          fields.push(name);
          renameFields[name] = newName;
        } else {
          fields.push(c);
        }
      }
      params.sysparm_fields = fields.join(",");
    }
    if (query) {
      params.sysparm_query = query;
    }
    // Update Cache if we got data, fetch from cache if we didn't
    let cacheKey = `${tableName}-${md5(JSON.stringify([columns, query]))}`;
    // Response data
    // Attempt to load cached data.
    let data = await cache.get(cacheKey, this.fake ? null : "1s");
    if (data) {
      // Successfully loaded from cache
      this.log(`Using local cache of ${tableName}`);
      return data;
    }
    // Count number of records
    const count = await this.getCount(tableName);
    if (count > 100000) {
      //never collect more than 100k to prevent memory-crashes
      if (status) status.warn("found over 100k rows");
      return [];
    }
    if (status) status.add(count);
    // Fetch from service now
    const limit = 1000;
    const totalPages = Math.ceil(count / limit);
    let pages = [];
    for (let i = 0; i < totalPages; i++) {
      pages.push(i);
    }
    let datas = await sync.map(4, pages, async page => {
      this.log(
        `GET #${count} records from "${tableName}" (page ${page +
          1}/${totalPages})`
      );
      let resp;
      try {
        resp = await this.api({
          method: "GET",
          url: `/v2/table/${tableName}`,
          params: {
            ...params,
            sysparm_limit: limit,
            sysparm_offset: page * limit
          }
        });
      } catch (err) {
        //request failed to send
        throw `GET ${tableName} failed: ${err}`;
      }
      //throw the provided error
      let err = prop(resp, "data", "error", "message");
      if (err) {
        throw `GET ${tableName} failed: ${err}`;
      }
      //throw generic error
      if (resp.status !== 200) {
        // this.log(resp.data);
        throw `GET ${tableName} failed: status ${resp.status} (${
          resp.statusText
        })`;
      }
      let d = prop(resp, "data", "result");
      if (!Array.isArray(d)) {
        throw `GET ${tableName} failed: invalid result`;
      }
      if (status) status.done(d.length);
      return d;
    });
    // join all parts
    data = [].concat(...datas);
    // Rename the fields
    if (Object.keys(renameFields).length) {
      data = data.map(row => {
        for (let f in row) {
          if (renameFields[f]) {
            row[renameFields[f]] = row[f];
            delete row[f];
          }
        }
        return row;
      });
    }
    // Cache for future
    if (data && data.length > 0) {
      await cache.put(cacheKey, data);
    }
    return data;
  }

  /**
   * import the provided row to the ServiceNow via the import API
   * @param {string} tableName The target table
   * @param {object} row The target object.
   */
  async import(tableName, row) {
    return await this.call({
      action: "import",
      tableName,
      row
    });
  }
  /**
   * create the provided row in ServiceNow via the table API
   * @param {string} tableName The target table
   * @param {object} row The target object.
   */
  async create(tableName, row) {
    return await this.call({
      action: "create",
      tableName,
      row
    });
  }

  /**
   * update the provided row in ServiceNow via the table API
   * @param {string} tableName The target table
   * @param {object} row The target object.
   */
  async update(tableName, row) {
    return await this.call({
      action: "update",
      tableName,
      row
    });
  }

  /**
   * delete the provided row from ServiceNow via the table API
   * @param {string} tableName The target table
   * @param {object} row The target object.
   */
  async delete(tableName, row) {
    return await this.call({
      action: "delete",
      tableName,
      row
    });
  }

  /**
   * call the table or import API
   * @param {object} config The call configuration object.
   */
  async call(config) {
    //pull variables
    let { tableName, row, action } = config;
    let doImport = action === "import";
    let doCreate = action === "create";
    let doUpdate = action === "update";
    let doDelete = action === "delete";
    //validate variables
    if (!tableName) {
      throw "No table specified";
    } else if (!row || typeof row !== "object") {
      throw `Row must be an object`;
    }
    if (!/^u_(imp_)?dm_/.test(tableName)) {
      throw `Invalid table specified (${tableName})`;
    }
    let importTable = Boolean(RegExp.$1);
    if ((doCreate || doUpdate) && importTable) {
      throw `Expected "u_dm_..." table`; //get/delete okay
    } else if (doImport && !importTable) {
      throw `Expected "u_imp_dm_..." table`;
    }
    let hasSysId = Boolean(row.sys_id);
    if ((doUpdate || doDelete) && !hasSysId) {
      throw `row is missing sys_id`;
    }
    let method, url;
    if (doImport) {
      method = "POST";
      url = `v1/import/${tableName}`;
    } else if (doUpdate) {
      method = "PUT";
      url = `/v2/table/${tableName}/${row.sys_id}`;
    } else if (doDelete) {
      method = "DELETE";
      url = `/v2/table/${tableName}/${row.sys_id}`;
    } else if (doCreate) {
      method = "POST";
      url = `/v2/table/${tableName}`;
    } else {
      throw `Invalid action (${action})`;
    }
    let data = method === "DELETE" ? undefined : row;
    //try 3 times!
    let b = new Backoff({ min: 1000, max: 30000, jitter: 0.5, factor: 3 });
    for (let attempt = 0; attempt < 3; attempt++) {
      //attempt...
      let response;
      try {
        response = await this.api({
          method,
          url,
          data
        });
      } catch (err) {
        //tcp disconnected, retry
        if (err.code === "ECONNRESET" || err.code === "EAI_AGAIN") {
          await sync.sleep(b.duration());
          continue;
        }
        //another error...
        throw `API call failed: ${err}`;
        break;
      }
      //rate-limited, retry....
      if (response.status === 429) {
        await sync.sleep(b.duration());
        continue;
      }
      //got a response
      let successStatus = doUpdate ? 200 : doDelete ? 204 : 201;
      if (response.status !== successStatus) {
        throw `Status ${response.status}`;
      }
      return response.data;
    }
    throw `Too many retries`;
  }

  /**
   * deltaImportAll objects in an array to the ServiceNow via the Import API
   * @param {string} tableName The target import table
   * @param {array} rows Synchonously send with backoff when throttled by ServiceNow.
   * @param {any} status The log-status instance for this task, see app/etl/log-status.js
   */
  async deltaImportAll(tableName, rows, status) {
    if (!tableName) {
      throw "No table specified";
    } else if (!/^u_imp_dm_/.test(tableName)) {
      throw `Invalid table specified (${tableName})`;
    } else if (!Array.isArray(rows)) {
      throw `Rows must be an array`;
    } else if (rows.length === 0) {
      throw `Rows are empty`;
    }
    //get existing 0-50%, delta changes 50-100%
    status.setStages(2);
    // TODO check firstRow for use of non-"u_" columns?
    // let firstRow = rows[0];
    //fetch this tables schema
    let schema = await this.getSchema(tableName);
    //prepare hash helper making use of schema
    let hashRow = createRowHash(schema);
    //rows to add and remove
    let pending = { add: [], remove: [] };
    //index all incoming rows
    let incomingIndex = {};
    for (let row of rows) {
      let h = hashRow(row);
      if (h in incomingIndex) {
        status.warn(`Duplicate row:`, row);
      }
      incomingIndex[h] = row;
    }
    //load all existing imports
    let existingImports = await this.get(tableName, null, null, status);
    let matchedIndex = {};
    for (let row of existingImports) {
      let h = hashRow(row);
      let match = h in incomingIndex;
      if (match) {
        //existing row matched, don't delete, don't import
        matchedIndex[h] = true;
      } else {
        //existing row not matched needs to be deleted
        pending.remove.push(row);
      }
    }
    //incoming rows unmatched in the index need to be added
    for (let h in incomingIndex) {
      if (!matchedIndex[h]) {
        let row = incomingIndex[h];
        pending.add.push(row);
      }
    }
    //categorised, perform api calls!
    status.doneStage();
    status.add(pending.add.length + pending.remove.length);
    //stats (no update, create == import)
    let rowsCreated = 0;
    let rowsUpdated = 0;
    let rowsDeleted = 0;
    let rowsErrored = 0;
    let messages = [];
    //delete rows
    if (pending.remove.length > 0) {
      status.message(`Deleting #${pending.remove.length} unmatched rows...`);
      await sync.each(API_CONCURRENCY, pending.remove, async row => {
        await this.delete(tableName, row);
        rowsDeleted++;
        status.done(1);
      });
    }
    //add rows
    if (pending.add.length > 0) {
      status.message(`Importing #${pending.add.length} changed rows...`);
      //collect errors
      let errorMessages = {};
      //unmatched rows are ready for import!
      await sync.each(API_CONCURRENCY, pending.add, async row => {
        let data, error;
        try {
          data = await this.import(tableName, row);
          //import done
          rowsCreated++;
          //check transform result, can still fail...
          let { result } = data;
          let changed = false;
          for (let r of result) {
            let ignore =
              r.status_message &&
              r.status_message.startsWith("Row transform ignored");
            if (r.status === "error" && !ignore) {
              error = r.error_message;
            }
            if (r.status === "updated" || r.status === "inserted") {
              changed = true;
            }
          }
          //data was actually updated
          if (changed) {
            rowsUpdated++;
          }
        } catch (err) {
          error = err.toString();
        }
        //done!
        if (error) {
          if (error.length > 4096) {
            //prevent 20MB from being accidently inserting into the DB :(
            error = error.slice(0, 4096);
          }
          rowsErrored++;
          let n = errorMessages[error] || 1;
          errorMessages[error] = n + 1;
        }
        //mark 1 action done
        status.done(1);
      });
      for (let msg in errorMessages) {
        let n = errorMessages[msg];
        status.warn(`${msg} (occured ${n} times)`); //add as warnings
      }
    }
    if (rowsCreated > 0) {
      messages.push(`${rowsCreated} imported`);
    }
    if (rowsUpdated > 0) {
      messages.push(`${rowsUpdated} changes`);
    }
    if (rowsDeleted > 0) {
      messages.push(`${rowsDeleted} cleared`);
    }
    if (rowsErrored > 0) {
      messages.push(`${rowsErrored} errored`);
    }
    if (messages.length === 0) {
      messages.push(`No changes`);
    }
    //all done
    status.log(`[snc] delta-import: ${tableName}: ${messages.join(", ")}`);
    status.doneStage();
    return {
      rowsCreated,
      rowsUpdated,
      rowsDeleted,
      rowsErrored
    };
  }

  /**
   * deltaSyncAll objects in an array with a ServiceNow table
   * @param {string} tableName The target import table
   * @param {array} rows Send with backoff when throttled by ServiceNow.
   * @param {any} status The log-status instance for this task, see app/etl/log-status.js
   */
  async deltaSyncAll(tableName, rows, status) {
    if (!tableName) {
      throw "No table specified";
    } else if (!/^u_dm_/.test(tableName)) {
      throw `Invalid table specified (${tableName})`;
    } else if (!Array.isArray(rows)) {
      throw `Rows must be an array`;
    }
    //capture stats
    let rowsCreated = 0;
    let rowsUpdated = 0;
    let rowsDeleted = 0;
    //load all existing rows
    let existingRows = await this.get(tableName);
    //index by "id"
    let index = {};
    for (let row of existingRows) {
      let id = row.u_id;
      if (!id) {
        //no id, dont index
        continue;
      } else if (id in index) {
        //already indexed, dont re-index
        continue;
      }
      index[id] = row;
    }
    status.log(
      `syncing #${rows.length} entries with ${tableName}, ` +
        `found #${existingRows.length} existing entries ` +
        `(indexed #${Object.keys(index).length})`
    );
    //split into two groups
    let existing = {};
    rows.forEach(row => {
      let id = row.u_id;
      if (id && id in index) {
        existing[id] = index[id];
        delete index[id];
      }
    });
    let missing = index;
    let missingRows = Object.values(missing);
    //mark changes+deletes actions to be done
    if (status) {
      status.add(rows.length + missingRows.length);
    }
    //merge!
    await sync.each(API_CONCURRENCY, rows, async row => {
      let id = row.u_id;
      let exists = id && id in existing;
      if (exists) {
        //update existing entry
        let prev = existing[id];
        //use existing system id
        row.sys_id = prev.sys_id;
        //compare against prev
        let changed = false;
        for (let k in row) {
          if (String(row[k]) !== prev[k]) {
            changed = true;
            break;
          }
        }
        if (changed) {
          //update existing
          await this.update(tableName, row);
          rowsUpdated++;
        }
      } else {
        //create new entry
        await this.create(tableName, row);
        rowsCreated++;
      }
      //mark 1 action done
      if (status) {
        status.done();
      }
    });
    //those remaining in the index need to be deleted
    if (missingRows.length > 0) {
      await sync.each(API_CONCURRENCY, missingRows, async row => {
        let deleted = row.u_deleted;
        if (deleted === "1") {
          //already "deleted"
          return;
        }
        if (deleted === undefined) {
          //table allows real deletes
          await this.delete(tableName, row);
        } else {
          //table just sets deleted flag
          await this.update(tableName, {
            sys_id: row.sys_id,
            u_deleted: 1
          });
        }
        rowsDeleted++;
        //mark 1 action done
        if (status) {
          status.done();
        }
      });
    }
    //all done
    status.log(`synced #${rows.length} entries to ${tableName}`);
    //provide merge results
    return {
      rowsCreated,
      rowsUpdated,
      rowsDeleted
    };
  }

  log(...args) {
    console.log("[snc]", ...args);
  }
};

const prop = (obj, ...path) => {
  let o = obj;
  for (let i = 0; i < path.length; i++) {
    let k = path[i];
    if (o && typeof o === "object" && k in o) {
      o = o[k];
    } else {
      return undefined;
    }
  }
  return o;
};

const createRowHash = schema => {
  return row => {
    // let debug =
    //   row.u_correlation_id ===
    //   "110-2000|6000C29c-2306-5620-4593-0c916aa61136|502990c1-807a-63ae-6bb0-0eab061ecb3e";
    // if (debug) console.log("DEBUG");
    let h = crypto.createHash("md5");
    for (let col in schema) {
      //only compare user fields
      //TODO use col in firstRow instead?
      if (!/^u_/.test(col)) {
        continue;
      }
      let spec = schema[col];
      let value = row[col];
      if (value === "" || value === null || value === undefined) {
        //s-now api returns "" for all null / empty / blank fields
        //so we must hash of these to blank
        value = ``;
      } else if (spec.type === "boolean") {
        //needs converting
        if (typeof value === "string") {
          value = value === "true";
        } else if (typeof value === "number") {
          value = value === 1;
        }
        //s-now api returns booleans as 1 or 0
        value = `"${value ? 1 : 0}"`;
      } else {
        //number needs rounding before converting
        if (typeof value === "number") {
          if (spec.type === "decimal") {
            value = Math.round(value * 100) / 100; //2 places
          } else if (spec.type === "integer") {
            value = Math.round(value);
          }
        }
        //swap out all fancy characters
        value = String(value).replace(/[^A-Za-z0-9\-\_]/g, "_");
        //trim length
        if (
          spec.type === "string" &&
          spec.maxLength &&
          value.length > spec.maxLength
        ) {
          console.log(
            `<WARN> Truncated column ${col} with length  ${value.length}`
          );
          value = value.slice(0, spec.maxLength);
        }
        //quote
        value = `"${value}"`;
      }
      let segment = `${col}=${value}`;
      // if (debug) console.log(segment);
      h.update(segment);
    }
    return h.digest("hex");
  };
};
