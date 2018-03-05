exports.prop = (obj, ...path) => {
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

exports.one = arr => {
  if (!Array.isArray(arr)) {
    let obj = arr;
    if (!obj || typeof obj !== "object") {
      throw `Expected object`;
    }
    return obj;
  }
  if (arr.length === 0) {
    return null; //no result
  }
  if (arr.length > 1) {
    throw `Multiple results`;
  }
  return arr[0];
};

exports.strip = input => {
  let output = {};
  let keys = Object.keys(input).sort();
  for (let k of keys) {
    let v = input[k];
    if (v !== "" && v !== "false") {
      output[k] = v;
    }
  }
  return output;
};

exports.createRowHash = schema => {
  return obj => {
    // let debug =
    //   row.u_correlation_id ===
    //   "110-2000|6000C29c-2306-5620-4593-0c916aa61136|502990c1-807a-63ae-6bb0-0eab061ecb3e";
    // if (debug) console.log("DEBUG");
    let h = crypto.createHash("md5");
    let row = exports.js2sn(obj);
    for (let col in schema) {
      //only compare user fields
      //TODO use col in firstRow instead?
      if (!/^u_/.test(col)) {
        continue;
      }
      let spec = schema[col];
      let value = row[col];
      //swap out all fancy characters
      value = String(value).replace(/[^A-Za-z0-9\-\_]/g, "_");

      //quote
      value = `"${value}"`;

      let segment = `${col}=${value}`;
      // if (debug) console.log(segment);
      h.update(segment);
    }
    return h.digest("hex");
  };
};

exports.sn2js = (schema, row) => {
  if (!schema) {
    throw `Missing schema`;
  }
  //run against all elements
  if (Array.isArray(row)) {
    return row.map(r => {
      this.sn2js(schema, r);
    });
  }
  //must be an object
  if (!row || typeof row !== "object") {
    throw `Invalid row`;
  }
  let obj = {};
  for (let k in schema) {
    let s = schema[k];
    let v = row[k];
    //must have value
    if (v == undefined || v === "") {
      continue;
    }
    //change string =>  schema type
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
      if (isNaN(i)) {
        throw `Invalid integer "${v}"`;
      }
      v = i;
    } else if (t === "glide_date_time") {
      if (!/^(\d\d\d\d-\d\d-\d\d) (\d\d:\d\d:\d\d)$/.test(v)) {
        throw `Unexpected date format "${v}"`;
      }
      //display=false implies UTC dates?
      let d = new Date(`${RegExp.$1}T${RegExp.$2}Z`);
      if (isNaN(d)) {
        throw `Invalid date "${v}"`;
      }
      v = d;
    } else if (t === "string") {
      //noop
    } else {
      // console.log("CONVERT", v, "TO", t);
    }
    obj[k] = v;
  }
  return obj;
};

exports.js2sn = (schema, obj) => {
  if (!schema) {
    throw `Missing schema`;
  }
  //run against all elements
  if (Array.isArray(obj)) {
    return obj.map(o => {
      this.sn2js(schema, o);
    });
  }
  //must be an object
  if (!obj || typeof obj !== "object") {
    throw `Invalid object`;
  }
  let row = {};
  for (let k in schema) {
    let s = schema[k];
    let v = obj[k];
    //must have schema and value
    if (v === undefined || v === "") {
      continue;
    }
    //change schema type => string
    let t = s.type;
    if (v === "" || v === null || v === undefined) {
      //servicenow api returns "" for all null / empty / blank fields
      v = "";
    } else if (t === "boolean") {
      if (typeof v === "boolean") {
        //noop;
      } else if (typeof v === "string") {
        v = v === "true";
      } else if (typeof v === "number") {
        v = v === 1;
      } else {
        throw `Invalid boolean v: ${v}`;
      }
      //servicenow api returns booleans as 1 or 0
      v = `"${v ? 1 : 0}"`;
    } else if (t === "decimal") {
      if (typeof v === "number") {
        throw `Invalid number v: ${v}`;
      }
      v = Math.round(v * 100) / 100; //2 places
    } else if (t === "integer") {
      if (typeof v === "number") {
        throw `Invalid number v: ${v}`;
      }
      v = Math.round(v);
    } else if (t == "string") {
      //trim length
      if (s.maxLength && v.length > s.maxLength) {
        console.log(`<WARN> Truncated column ${k} with length  ${v.length}`);
        v = v.slice(0, s.maxLength);
      }
    } else if (typeof v !== "string") {
      throw `ServiceNow only supports strings (found: ${v})`;
    }
    row[k] = v;
  }
  return row;
};

exports.isGUID = str => /^[a-f0-9]{32}$/.test(str);
