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
const ServiceNowClientDelta = require("./client-delta");
const ServiceNowClientRelationships = require("./client-relationships");
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
    this.readOnly = config.readOnly === true;
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
    this.delta = new ServiceNowClientDelta(this);
    this.relate = new ServiceNowClientRelationships(this);
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
   * - Request data type conversion (JSON -> SN Strings)
   * - Response data type validation
   * - Response table schema validation
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
    if (isWrite && this.readOnly) {
      throw `Request (${method} ${url}) blocked, read-only mode is enabled`;
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
    let resp, respErr;
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
        this.debug(
          `do: ${method} ${url}`,
          request.params || "",
          hasData ? request.data : ""
        );
        respErr = null;
        resp = await this.api(request);
      } catch (err) {
        respErr = err.toString();
        this.debug(`FAILED: do: ${method} ${url}`, respErr);
        //tcp disconnected, retry
        if (err.code === "ECONNRESET" || err.code === "EAI_AGAIN") {
          retry = true;
        }
      }
      if (resp && resp.status === 429) {
        retry = true;
      }
      if (!retry) {
        break;
      }
      attempt++;
    }
    if (respErr) {
      throw respErr;
    }
    //request has no data
    if (resp.status === 204 || resp.status === 201) {
      return true;
    }
    //has data
    let { data } = resp;
    //failed status?
    if (resp.status !== 200) {
      this.log("error response:", resp.statusText, data);
      if (resp.status === 403) {
        throw `Unauthorised (user ${this.username}, ${method} ${url})`;
      }
      throw `Unexpected error (${resp.statusText})`;
    }
    if (!data) {
      // if (isRead) {
      throw `Expected response data (${method} ${url})`;
      // } else {
      //   return true;
      // }
    }
    //validate type
    let contentType = resp.headers["content-type"] || "";
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
      //TODO: support fields=company.name
      //requires schema.company.schema
      //STAGE2: do we need company.name?
      //do we just need sys_id?
      //sys_id in DM?
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
    // Fetch from service now
    const limit = 500;
    const totalPages = Math.ceil(count / limit);
    if (status) status.add(totalPages);
    let pages = [];
    for (let i = 0; i < totalPages; i++) {
      pages.push(i);
    }
    let totalRecords = 0;
    let datas = await sync.map(4, pages, async page => {
      let results = await this.do({
        method: "GET",
        url: `/v2/table/${tableName}`,
        params: {
          ...params,
          sysparm_limit: limit,
          sysparm_offset: page * limit
        }
      });
      if (status) status.done(1);
      totalRecords += results.length;
      if (totalPages > 1) {
        this.log(
          `got #${totalRecords} records from "${tableName}"` +
            ` (page ${page + 1}/${totalPages})`
        );
      }
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
    if (!row) {
      throw `No row data provided`;
    } else if (row.sys_id) {
      throw `new rows cannot contain "sys_id`;
    }
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
    if (typeof row === "string") {
      let sys_id = row;
      row = { sys_id };
    }
    if (!row.sys_id) {
      throw `row requires "sys_id"`;
    }
    return await this.do({
      method: "DELETE",
      url: `/v2/table/${tableName}/${row.sys_id}`
    });
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
