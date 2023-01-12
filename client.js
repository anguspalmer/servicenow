const axios = require("axios");
const { cache } = require("cache");
const recordCache = cache.sub("sn-cache");
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
const { prop, one, isGUID, snowDate } = require("./util");
//client-class splits
const CChoice = require("./client-choice");
const CColumn = require("./client-column");
const CDelta = require("./client-delta");
const CLayout = require("./client-layout");
const CPolicy = require("./client-policy");
const CRelate = require("./client-relate");
const CSchema = require("./client-schema");
const CTable = require("./client-table");

/**
 * @class
 * Simple client to grab all rows from a table in ServiceNow.
 * Returns results as JSON
 * Allows renaming of fields.
 * @example
 * let snc = new ServiceNowClient({
 *   user: "foo",
 *   pass: "bar",
 *   instance: "dev"
 * });
 * let results = await snc.get("cmdb_ci_server")
 */
module.exports = class ServiceNowClient {
  constructor(config) {
    //validate config
    let { username, password, instance } = config;
    let fake = instance === "dev" && (!username || !password);
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
          validateStatus: () => true,
          timeout: 60 * 1000
        });
    this.username = username;
    //rate limiting queues
    let { readConcurrency = 40, writeConcurrency = 80 } = config;
    this.readBucket = new sync.TokenBucket(readConcurrency);
    this.writeBucket = new sync.TokenBucket(writeConcurrency);
    //submodules
    this.choice = new CChoice(this);
    this.column = new CColumn(this);
    this.delta = new CDelta(this);
    this.layout = new CLayout(this);
    this.policy = new CPolicy(this);
    this.relate = new CRelate(this);
    this.schema = new CSchema(this);
    this.table = new CTable(this);
    //ready
    this.log(`using cache dir: ${recordCache.base}`);
  }

  //number of active reads occuring right now.
  //this should never surpass <readConcurrency>.
  get numReads() {
    return this.readBucket.numTokens;
  }

  //number of active writes occuring right now.
  //this should never surpass <writeConcurrency>.
  get numWrites() {
    return this.writeBucket.numTokens;
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
    let isValidJSONURL = /\/v\d\/(import|table|stats|attachment)(\/(\w+))?(\/(\w+))?$/.test(
      url
    );
    let apiType = RegExp.$1;
    let tableAPI = apiType === "table";
    let importAPI = apiType === "import";
    let attachmentAPI = apiType === "attachment";
    let tableName = RegExp.$3;
    let sysID = RegExp.$5;
    const isFile = attachmentAPI && sysID === "file"
    if (isFile) sysID = RegExp.$3;

    //validate inputs
    if (sysID && !isGUID(sysID)) {
      throw `Invalid URL sys_id`;
    }
    if (!sysID && tableAPI && (method === "PUT" || method === "DELETE")) {
      throw `Expected sys ID in URL`;
    }
    let isArray = !sysID;
    let isJSON = !isXML && !isFile;
    if ((isJSON || isFile) && !isValidJSONURL) {
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
      //perform HTTP request, determine if we can retry
      let retry = false;
      const bucket = isRead ? this.readBucket : this.writeBucket;
      await bucket.run(async () => {
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
          if (err.code === "ECONNRESET" || err.code === "EAI_AGAIN" || err.code ==='ETIMEDOUT') {
            retry = true;
          }
        }
      });
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
      throw `Expected response data (${method} ${url})`;
    }
    //validate type
    let contentType = resp.headers["content-type"] || "";
    if (isXML && !contentType.startsWith("text/xml")) {
      throw `Expected XML (got ${contentType})`;
    } else if (isJSON && !contentType.startsWith("application/json")) {
      throw `Expected JSON (got ${contentType})`;
    }
    if (isFile) return data;

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
      result = await this.schema.convertJS(tableName, result);
    }
    //done!
    return result;
  }

  async authenticate() {
    //check servicenow credentials
    const snUser = await this.getUser();
    this.log(
      `servicenow authenticated as "${snUser.name}" on "${this.instance}"`
    );
    return snUser;
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
      throw `Invalid count response ("${count})`;
    }
    return parseInt(count, 10);
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
    let { columns, query, fields = [], status } = opts;
    //pick logger
    const logger = status || this;
    //individual request caching
    let cacheRecords = opts.cache === true;
    //prepare params for all requests
    let params = {
      ...this.defaultParams
    };
    if (!query) {
      query = "";
    }
    if (!Array.isArray(fields)) {
      throw `expected fields to be an array`;
    }
    // limit results to specified columns
    let renameFields = {};
    if (columns) {
      for (let c of columns) {
        if (typeof c === "string") {
          fields.push(c);
        } else if (c && typeof c == "object") {
          for (let name in c) {
            let newName = c[name];
            renameFields[name] = newName;
            fields.push(name);
            break;
          }
        } else {
          throw `Invalid column`;
        }
      }
    }
    if (fields.length > 0) {
      params.sysparm_fields = fields.join(",");
    }
    // Response data
    let data;
    // Attempt to load cached data?
    let cacheKey;
    if (cacheRecords && !/sys_updated_on/.test(query)) {
      cacheKey = `${this.instance}-${tableName}`;
      //custom query? encode as a hash
      if (fields.length > 0 || query) {
        cacheKey += `-${md5(JSON.stringify([fields, query]))}`;
      }
      //convert modified-time into a snow date
      const mtime = snowDate(await recordCache.mtime(cacheKey));
      const isCached = Boolean(mtime);
      const cacheExpiry = "3d";
      //pull data from cache if its less than 3 days old
      const data = isCached
        ? await recordCache.get(cacheKey, cacheExpiry)
        : null;
      //have cached rows!
      if (Array.isArray(data)) {
        const q = [];
        if (query) {
          q.push(query);
        }
        const updatedAfter = await this.getCount(
          tableName,
          q.concat(`sys_updated_on>=${mtime}`).join("^")
        );
        //no new rows added!
        if (updatedAfter === 0) {
          const updatedBefore = await this.getCount(
            tableName,
            q.concat(`sys_updated_on<=${mtime}`).join("^")
          );
          //none have changed since cache time, safe to use
          //CAVEAT: not safe to use if schema has changed!
          if (updatedBefore === data.length) {
            logger.log(`Read cache: ${cacheKey}`);
            //JSON cannot store date objects, so we must convert all
            //date columns from strings into Dates.
            //TODO: FIX: @jpillora: 'data' should also contain hash of schema!
            const s = await this.schema.get(tableName);
            const dates = [];
            for (let k in s) {
              if (s[k].type === "glide_date_time") {
                dates.push(k);
              }
            }
            for (let row of data) {
              for (let k of dates) {
                const v = row[k];
                if (v) {
                  const d = new Date(v);
                  if (!isNaN(+d)) {
                    row[k] = d;
                  }
                }
              }
            }
            //ready
            return data;
          }
        }
      }
    }
    // Count number of records
    const count = await this.getCount(tableName, query);
    if (count > 100000) {
      //never collect more than 100k to prevent memory-crashes
      throw `table "${tableName}" has over 100,00 rows"`;
    }
    // Fetch from service now
    if (query) {
      params.sysparm_query = query;
    }
    const maxRecords = opts.maxRecords
      ? Math.min(count, opts.maxRecords)
      : count;
    const pageSize = Math.min(maxRecords, opts.pageSize ? opts.pageSize : 500);
    const totalPages = Math.ceil(maxRecords / pageSize);
    if (status && status.add) {
      status.add(totalPages);
    }
    let pages = [];
    for (let i = 0; i < totalPages; i++) {
      pages.push(i);
    }
    let totalRecords = 0;
    const datas = await sync.map(4, pages, async page => {
      const results = await this.do({
        method: "GET",
        url: `/v2/table/${tableName}`,
        params: {
          ...params,
          sysparm_limit: pageSize,
          sysparm_offset: page * pageSize
        }
      });
      if (status && status.done) {
        status.done(1);
      }
      totalRecords += results.length;
      if (totalPages > 1) {
        logger.log(
          `got #${totalRecords} records from "${tableName}"` +
            ` (page ${page + 1}/${totalPages})`
        );
      }
      // Rename the fields
      for (const row of results) {
        for (let f in renameFields) {
          if (f in row && renameFields[f] !== f) {
            row[renameFields[f]] = row[f];
            delete row[f];
          }
        }
      }
      return results;
    });
    // join all parts
    data = [].concat(...datas);
    // Cache for future
    if (cacheRecords && cacheKey && data && data.length > 0) {
      await recordCache.put(cacheKey, data);
      logger.log(`Wrote cache: ${cacheKey}`);
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
      console.error("[snc] <DEBUG>", ...args);
    }
  }
};
