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
      throw new Error(`Expected object`);
    }
    return obj;
  }
  if (arr.length === 0) {
    return null; //no result
  }
  if (arr.length > 1) {
    throw new Error(`Multiple results`);
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

exports.isGUID = str => /^[a-f0-9]{32}$/.test(str);

const titlizeMap = {
  ip: "IP",
  mac: "MAC",
  api: "API",
  id: "ID",
  guid: "GUID",
  uuid: "UUID",
  vm: "VM",
  iops: "IOPS",
  cpg: "CPG",
  cpu: "CPU",
  ram: "RAM",
  gb: "GB",
  mb: "MB",
  caas: "CaaS",
  os: "OS",
  dns: "DNS",
  ci: "CI",
  ha: "HA",
  ssd: "SSD",
  url: "URL",
  rest: "REST",
  soap: "SOAP",
  lun: "LUN",
  qos: "QoS"
};

exports.titlize = slug =>
  slug
    .toLowerCase()
    .replace(/^u_/, "")
    .split("_")
    .map((p, i) => {
      //has preset mapping?
      if (p in titlizeMap) {
        return titlizeMap[p];
      }
      //automatically titlize first word
      if (i === 0) {
        return p.charAt(0).toUpperCase() + p.slice(1);
      }
      //leave the rest as lowercase
      return p;
    })
    .join(" ");

exports.subsetOf = (small, big) => {
  if (big && typeof big === "object" && small && typeof small === "object") {
    let match = true;
    for (const key in small) {
      if (!(key in big)) {
        match = false;
        // console.log("SNOW-EQ: MISSING:", key);
        continue;
      }
      const b = String(big[key]);
      const s = String(small[key]);
      if (b !== s) {
        match = false;
        // console.log("SNOW-EQ: MISMATCH:", key, p, n);
        continue;
      }
    }
    return match;
  }
  return big === small;
};

exports.snowDate = date => {
  //is valid date object
  if (date && date instanceof Date && !isNaN(+date)) {
    return date
      .toISOString()
      .replace("T", " ")
      .replace(/(\.\d+)?Z/, "");
  }
  return null;
};

exports.round = (n, places = 2) => {
  const factor = Math.pow(10, places);
  return Math.round(n * factor) / factor;
};
