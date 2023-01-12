const sync = require("sync");
const { prop, snowDate, round, isGUID } = require("./util");
const EXPIRES_AT = Symbol();
const EXPIRE_AFTER = 5 * 60 * 1000;

module.exports = class CSchema {
  constructor(client) {
    this.client = client;
    this.cache = {};
  }

  /**
   * Returns the schema of the given table.
   * @param {string} tableName
   */
  async get(tableName, invalidate = false) {
    //force remove cache
    if (invalidate) {
      delete this.cache[tableName];
    }
    //attempt to load from cache
    if (tableName in this.cache) {
      let schema = this.cache[tableName];
      //inflight? wait and re-load from cache
      if (schema instanceof Promise) {
        await schema;
        schema = this.cache[tableName];
      }
      if (+new Date() < schema[EXPIRES_AT]) {
        return schema;
      }
      delete this.cache[tableName];
    }
    //mark get-schema as inflight, others following must wait!
    let done;
    this.cache[tableName] = new Promise(d => (done = d));
    //load from servicenow
    let resp = await this.client.do({
      method: "GET",
      baseURL: `https://${this.client.instance}.service-now.com/`,
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
    schema[EXPIRES_AT] = +new Date() + EXPIRE_AFTER;
    this.cache[tableName] = schema;
    done();
    //return!
    return schema;
  }

  async convertJS(tableName, row) {
    const schema = await this.get(tableName);
    if (!schema) {
      throw `Missing schema for ${tableName}`;
    }
    //run against all elements
    if (Array.isArray(row)) {
      const rows = row;
      return await sync.map(
        1,
        rows,
        async r => await this.convertJS(tableName, r)
      );
    }
    //must be an object
    if (!row || typeof row !== "object") {
      throw `Invalid row`;
    }
    let obj = {};
    for (let key of Object.keys(row).sort()) {
      let k = key;
      let kschema = schema;
      let o = obj;
      //dot keys required nested schema lookups
      if (key.includes(".")) {
        const parents = key.split(".");
        //the outer name is the target key,
        k = parents.pop();
        //the remaining keys are parent schemas:
        while (parents.length > 0) {
          const k = parents.shift();
          const s = kschema[k];
          //s must be a reference field
          if (!s || s.type !== "reference") {
            throw `Invalid reference "${key}" (parent "${k}")`;
          }
          if (!o[k]) {
            o[k] = {};
          }
          kschema = await this.get(s.reference_table);
          o = o[k];
        }
      }
      //pull schema field
      let s = kschema[k];
      if (!s) {
        this.warn(`schema missing key "${k}"`);
        continue;
      }
      let v = row[key];
      //must have value
      if (v == undefined || v === "") {
        continue;
      }
      //change string => schema type
      let t = s.type;
      if (t === "boolean") {
        if (v === "true") {
          v = true;
        } else if (v === "false") {
          v = false;
        } else {
          throw `Invalid boolean "${v}"`;
        }
      } else if (t === "integer") {
        let i = parseInt(v, 10);
        if (!isNaN(i)) {
          v = i;
        } else if (isNaN(i) && !s.choice_list) {
          // if using sysparm_display_value = true then
          // choice lists will always be Strings. So ignore
          // the field type if it can't be parsed to int
          throw `Invalid integer "${v}"`;
        }
      } else if (t === "float" || t === "decimal") {
        //will either have up to 7 or 2 decimal places
        let i = parseFloat(v, 10);
        if (isNaN(i)) {
          throw `Invalid float/decimal "${v}"`;
        }
        v = i;
      } else if (t === "glide_date_time") {
        let d;
        let time, yyyy, mm, dd;
        //display=false implies UTC dates
        if (/^(\d\d\d\d)-(\d\d)-(\d\d) (\d\d:\d\d:\d\d)$/.test(v)) {
          yyyy = RegExp.$1;
          mm = RegExp.$2;
          dd = RegExp.$3;
          time = RegExp.$4
        //display=true dates in local format
        }else if (/^(\d\d)-(\d\d)-(\d\d\d\d) (\d\d:\d\d:\d\d)$/.test(v)) {
          dd = RegExp.$1;
          mm = RegExp.$2;
          yyyy = RegExp.$3;
          time = RegExp.$4
        }else{
          throw `Unexpected date format "${v}"`;
        }
        d = new Date(`${yyyy}-${mm}-${dd}T${time}Z`);
        if (isNaN(d)) {
          throw `Invalid date "${v}"`;
        }
        v = d;
      } else if (t === "string") {
        //noop
      } else if (t === "reference") {
        //"leaf" reference, just keep link-object or sys_id string
      } else {
        // NOTE: objects (link+value) are left untouched
        // console.log("CONVERT", v, "TO", t);
      }
      o[k] = v;
    }
    return obj;
  }

  async convertSN(tableName, obj) {
    const schema = await this.get(tableName);
    if (!schema) {
      throw `Missing schema for ${tableName}`;
    }
    //run against all elements
    if (Array.isArray(obj)) {
      const objs = obj;
      return await sync.map(
        1,
        objs,
        async o => await this.convertSN(tableName, o)
      );
    }
    //must be an object
    if (!obj || typeof obj !== "object") {
      throw `Invalid object`;
    }
    //values must be strings.
    //values must be either defined or not.
    //if undefined, empty string, otherwise defined
    let row = {};
    for (let k in schema) {
      let s = schema[k];
      //skip missing fields
      if (!(k in obj)) {
        continue;
      }
      let v = obj[k];
      //change schema type => string
      let t = s.type;
      //check boolean first, ensures always defined (0 or 1)
      if (t === "boolean") {
        if (typeof v === "string") {
          v = v === "true";
        } else if (typeof v === "number") {
          v = v === 1;
        } else if (v === null) {
          v = false;
        }
        if (typeof v !== "boolean") {
          throw `"${k}" expected boolean "${v}"`;
        }
        //servicenow api returns booleans as 1 or 0
        v = `${v ? 1 : 0}`;
      } else if (v === null || v === undefined) {
        //undefined values are the empty string.
        v = "";
      } else if (t === "decimal" || t === "float") {
        if (typeof v === "string") {
          v = parseFloat(v);
        }
        if (typeof v !== "number" || isNaN(v)) {
          throw `"${k}" expected number "${v}"`;
        }
        //round specific number of places.
        //see https://docs.servicenow.com/bundle/london-platform-administration/page/administer/reference-pages/reference/r_FieldTypes.html
        //"Decimal - Number with up to two digits after the decimal points (for example, 12.34)."
        //"Floating Point Number - Number with up to seven digits after the decimal point"
        const places = t === "float" ? 7 : 2;
        v = `${round(v, places)}`;
      } else if (t === "integer" || t === "long") {
        if (typeof v === "string") {
          v = parseInt(v, 10);
        }
        if (typeof v !== "number" || isNaN(v)) {
          throw `"${k}" expected number "${v}"`;
        }
        v = `${Math.round(v)}`;
      } else if (t == "string") {
        //convert boolean and number to string
        if (typeof v === "boolean" || typeof v === "number") {
          v = String(v);
        }
        //trim length
        if (s.max_length && v.length > s.max_length) {
          console.log(`<WARN> Truncated column ${k} with length  ${v.length}`);
          v = v.slice(0, s.max_length);
        }
      } else if (t === "glide_date_time") {
        if (
          typeof v === "string" &&
          (dateTimeZone.test(v) || dateTimeUTC.test(v))
        ) {
          v = new Date(v);
        }
        if (!(v instanceof Date)) {
          throw `"${k}" expected date (got ${typeof v} ${v})`;
        }
        v = snowDate(v);
      } else if (t === "reference") {
        if (!v) {
          v = "";
        } else if (!isGUID(v)) {
          throw `"${k}" expected guid string (got ${v})`;
        }
      }
      //sanity check
      if (typeof v !== "string") {
        throw `"${k}" expected string (found type '${typeof v}' with value '${v}')`;
      }
      //ready!
      row[k] = v;
    }
    return row;
  }

  log(...args) {
    this.client.log("[table]", ...args);
  }

  warn(...args) {
    this.log("WARNING", ...args);
  }
};

const dateTimeZone = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} [\+\-]\d{2}:\d{2}$/;
const dateTimeUTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;
