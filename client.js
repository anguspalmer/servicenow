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

const ServiceNowClientTable = require("./client-table");
const fakeApi = require("./fake-api");
const { convertJS, prop, one, createRowHash, isGUID } = require("./util");
const API_CONCURRENCY = 40;
const EXPIRES_AT = Symbol();

/**
 * @class
 * Simple client to grab all rows from a table in ServiceNow.
 * Returns results as JSON
 * Allows renaming of fields.
 * @example
 * let snc = new ServiceNowClient({
 *   user: "foo",
 *   pass: "bar",
 *   instance: "ac3dev"
 * });
 * let results = await snc.get("u_commvault_products")
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
    this.enableCache = config.cache === true;
    this.enableDebug = config.debug === true;
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
    this.username = username;
    this.table = new ServiceNowClientTable(this);
    this.schemaCache = {};
  }

  /**
   * Wraps axios to provide service now improvements.
   * Features:
   * - URL validation
   * - Backoff retries when server overloaded
   * - Error message extraction
   * - Content type validation
   * - Auto XML parsing
   * - Response data type validation
   * - TODO Response table schema validation
   * @param {object} request axios request object
   */
  async do(request) {
    let { method, url } = request;
    if (!method) {
      method = "GET";
    } else {
      method = method.toUpperCase();
    }
    let isRead = method === "GET" || method === "HEAD";
    let isWrite = !isRead;
    if (!url) {
      throw `Missing URL`;
    }
    //validate URL (must use versioned api)
    let hasData = Boolean(request.data);
    let isXML = /\.do\b/.test(url);
    let isValidJSONURL = /\/v\d\/(import|table|stats|attachment)\/(\w+)(\/(\w+))?$/.test(
      url
    );
    let apiType = RegExp.$1;
    let tableAPI = apiType === "table";
    let importAPI = apiType === "import";
    let tableName = RegExp.$2;
    let sysID = RegExp.$4;
    //validate inputs
    if (sysID && !isGUID(sysID)) {
      throw `Invalid URL sys_id`;
    }
    if (!sysID && tableAPI && (method === "PUT" || method === "DELETE")) {
      throw `Expected sys ID in URL`;
    }
    let isArray = !sysID;
    let isJSON = !isXML;
    if (isJSON && !isValidJSONURL) {
      throw `Invalid URL`;
    }
    if (importAPI && !/^u_imp_dm_/.test(tableName)) {
      throw `Invalid import table specified (${tableName})`;
    }
    //do request!
    let resp;
    //with retries
    let backoff,
      attempt = 0;
    while (true) {
      //retry attempts must be delayed
      if (attempt > 0) {
        //service-now overloaded currently...
        if (attempt === 3) {
          throw `Too many retries`;
        }
        //initialise backoff timer
        if (!backoff) {
          backoff = new Backoff({
            min: 1000,
            max: 30000,
            jitter: 0.5,
            factor: 3
          });
        }
        let d = backoff.duration();
        await sync.sleep(d);
      }
      //perform HTTP request, determine if we can rety
      let retry = false;
      try {
        this.debug(`do: ${method} ${url}...`);
        resp = await this.api(request);
      } catch (err) {
        //tcp disconnected, retry
        if (err.code === "ECONNRESET" || err.code === "EAI_AGAIN") {
          retry = true;
        }
      }
      if (resp.status === 429) {
        retry = true;
      }
      if (!retry) {
        break;
      }
      attempt++;
    }
    //request has no data
    if (resp.status === 204 || resp.status === 201) {
      return true;
    }
    //got response
    let { data } = resp;
    if (!data) {
      throw `Expected response data`;
    }
    //validate type
    let contentType = resp.headers["content-type"];
    if (isXML && !contentType.startsWith("text/xml")) {
      throw `Expected XML (got ${contentType})`;
    } else if (isJSON && !contentType.startsWith("application/json")) {
      throw `Expected JSON (got ${contentType})`;
    }
    //auto-parse xml
    if (isXML) {
      data = await parseXML(data);
    }
    //check for errors (TODO xml errors...)
    if (data && data.error && data.error.message) {
      let msg = data.error.message;
      if (data.error.detail) {
        msg += ` (${data.error.detail})`;
      }
      throw `${method} "${tableName}" failed: ${msg}`;
    }
    //generic error (should not happen...)
    if (resp.status !== 200) {
      throw `${method} "${tableName}" failed: ${resp.statusText}`;
    }
    //XML data
    if (isXML) {
      return data;
    }
    //JSON data always has "result"
    let { result } = data;
    if (tableAPI && isRead && isArray && !Array.isArray(result)) {
      throw `Expected array result`;
    }
    //table api results? use schema to convert to JS types
    if (tableAPI && tableName) {
      let schema = await this.getSchema(tableName);
      result = convertJS(schema, result);
    }
    //done!
    return result;
  }

  /**
   * Returns the number of rows in the given table.
   * @param {string} tableName
   */
  async getUser(username = this.username) {
    let result = await this.do({
      method: "GET",
      url: `/v1/table/sys_user`,
      params: {
        sysparm_query: `user_name=${username}`
      }
    });
    let user = one(result);
    return user;
  }

  /**
   * Returns the number of rows in the given table.
   * @param {string} tableName
   */
  async getCount(tableName, query) {
    let result = await this.do({
      method: "GET",
      url: `/v1/stats/${tableName}`,
      params: {
        sysparm_count: true,
        sysparm_query: query
      }
    });
    let count = prop(result, "stats", "count");
    if (!/^\d+$/.test(count)) {
      throw `Invalid count response`;
    }
    return parseInt(count, 10);
  }

  /**
   * Returns the schema of the given table.
   * @param {string} tableName
   */
  async getSchema(tableName, invalidate = false) {
    //force remove cache
    if (invalidate) {
      delete this.schemaCache[tableName];
    }
    //attempt to load from cache
    if (tableName in this.schemaCache) {
      let schema = this.schemaCache[tableName];
      //inflight? wait and re-load from cache
      if (schema instanceof Promise) {
        await schema;
        schema = this.schemaCache[tableName];
      }
      if (+new Date() < schema[EXPIRES_AT]) {
        return schema;
      }
      delete this.schemaCache[tableName];
    }
    //mark get-schema as inflight, others following must wait!
    let done;
    this.schemaCache[tableName] = new Promise(d => (done = d));
    //load from servicenow
    let resp = await this.do({
      method: "GET",
      baseURL: `https://${this.instance}.service-now.com/`,
      url: `${tableName}.do?SCHEMA`
    });
    let elements = prop(resp, tableName, "element");
    if (!Array.isArray(elements)) {
      throw `GET schema failed: expected array of columns`;
    }
    let schema = {};
    elements.sort((a, b) => {
      return a.$.name < b.$.name ? -1 : 1;
    });
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
      schema[col.name] = col;
    });
    //add to cache with 5 minute expiry
    schema[EXPIRES_AT] = +new Date() + 5 * 60 * 1000;
    this.schemaCache[tableName] = schema;
    done();
    //return!
    return schema;
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
    // Response data
    let data;
    // Attempt to load cached data?
    let cacheKey;
    if (this.enableCache) {
      cacheKey = `${tableName}-${md5(JSON.stringify([columns, query]))}`;
      data = await cache.get(cacheKey, this.fake ? null : "1s");
      if (data) {
        // Successfully loaded from cache
        this.log(`Using local cache of ${tableName}`);
        return data;
      }
    }
    // Count number of records
    const count = await this.getCount(tableName, query);
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
      let results = await this.do({
        method: "GET",
        url: `/v2/table/${tableName}`,
        params: {
          ...params,
          sysparm_limit: limit,
          sysparm_offset: page * limit
        }
      });
      if (status) status.done(results.length);
      return results;
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
    if (this.enableCache && data && data.length > 0) {
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
    return await this.do({
      method: "POST",
      url: `/v1/import/${tableName}`,
      data: row
    });
  }
  /**
   * create the provided row in ServiceNow via the table API
   * @param {string} tableName The target table
   * @param {object} row The target object.
   */
  async create(tableName, row) {
    return await this.do({
      method: "POST",
      url: `/v2/table/${tableName}`,
      data: row
    });
  }

  /**
   * update the provided row in ServiceNow via the table API
   * @param {string} tableName The target table
   * @param {object} row The target object.
   */
  async update(tableName, row) {
    if (!row.sys_id) {
      throw `row requires "sys_id"`;
    }
    return await this.do({
      method: "PUT",
      url: `/v2/table/${tableName}/${row.sys_id}`, //sys_id will be extracted from row
      data: row
    });
  }

  /**
   * delete the provided row from ServiceNow via the table API
   * @param {string} tableName The target table
   * @param {object} row The target object.
   */
  async delete(tableName, row) {
    if (!row.sys_id) {
      throw `row requires "sys_id"`;
    }
    return await this.do({
      method: "DELETE",
      url: `/v2/table/${tableName}/${row.sys_id}`
    });
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
  async deltaMerge(tableName, rows, status) {
    if (!tableName) {
      throw "No table specified";
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

  debug(...args) {
    if (this.enableDebug) {
      this.log(...args);
    }
  }
};
