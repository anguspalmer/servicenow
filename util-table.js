const { titlize } = require("./util");

//expand short-hand js table,
//should be no-op on a valid table
exports.expandTable = table => {
  if (!table.name) {
    throw `Table must have a "name"`;
  }
  if (!table.label) {
    let name = table.name.replace(/(u_)?(cmdb_)?(ci_)?/, "");
    table.label = titlize(name);
  }
  const cmdbTable = /cmdb/.test(table.name);
  if (cmdbTable && !table.primaryKey) {
    table.primaryKey = "correlation_id";
  }
  if (table.allowDeletes !== true) {
    table.allowDeletes = false;
  }
  for (let key in table.columns) {
    let col = table.columns[key];
    //infer name property from key
    if (!col.name) {
      if (cmdbTable && key === "name") {
        //out-of-the-box, use id as is
        col.name = key;
      } else {
        //add "u_" to id
        col.name = (/^u_/.test(key) ? "" : "u_") + key;
      }
    }
    exports.expandColumn(col);
  }
};

//expand short-hand js column,
//should be no-op on a valid column
exports.expandColumn = col => {
  if (!col.name) {
    throw `expand-column: "name" is missing`;
  }
  if (typeof col.type !== "string") {
    throw `expand-column: "${col.name}" is missing the "type" property`;
  }
  col.type = col.type.toLowerCase();
  if (!col.label) {
    col.label = titlize(col.name);
  }
  let default_length;
  //check type AND set default length
  switch (col.type) {
    case "text":
      default_length = 4000;
      col.type = "string";
      break;
    case "html":
      default_length = 4000;
      break;
    case "url":
    case "string":
      default_length = 255;
      break;
    case "choice":
      default_length = 40;
      break;
    case "bigint":
    case "long":
      col.type = "long";
    case "integer":
    case "float":
    case "decimal":
    case "boolean":
      default_length = 40;
      break;
    case "date":
    case "glide_date_time":
      default_length = 40;
      col.type = "glide_date_time";
      break;
    case "reference_list":
      default_length = 4000;
      col.type = "glide_list";
      break;
    case "reference":
    case "guid":
      default_length = 32;
      break;
    default:
      throw `expand-column: unknown type "${col.type}"`;
  }
  if (!col.max_length) {
    col.max_length = default_length;
  }
  const isReference = col.type === "reference" || col.type === "glide_list";
  if (isReference && !col.reference_table) {
    throw `expand-column: "${col.name}" is missing` +
      ` the "reference_table" property`;
  }
  if (!col.choice_map && col.choice) {
    throw `expand-column: "${col.name}" missing "choice_map"`;
  }
  if (col.choice_map && !col.choice) {
    col.choice = "nullable";
  }
  if (typeof col.choice === "string") {
    let v = col.choice;
    if (v === "nullable") {
      v = 1;
    } else if (v === "suggestion") {
      v = 2;
    } else if (v === "required") {
      v = 3;
    } else {
      throw `expand-column: unknown choice string "${v}"`;
    }
    col.choice = v;
  }
  if ("syncback" in col) {
    if (typeof col.syncback !== "boolean") {
      throw `expand-column: "syncback" must be a boolean`;
    }
  }
  if ("data_policy" in col) {
    if (col.data_policy !== "readonly" && col.data_policy !== "writable") {
      throw `expand-column: "data_policy" must be a "readonly" or "writable"`;
    }
  }
};

//convert js column to sn column
const snColumnMap = {
  sys_id: "sys_id",
  name: "element",
  type: "internal_type",
  label: "column_label",
  max_length: "max_length",
  choice: "choice",
  choice_map: false, //syncd separately
  reference_table: "reference",
  reference_field: false, //used in deltamerge
  data_policy: false //syncd separately
};

exports.snColumn = js => {
  if (!js) {
    throw `Missing column`;
  } else if (!js.name) {
    throw `Missing column name`;
  }
  let sn = {};
  for (let k in js) {
    let v = js[k];
    let newk = snColumnMap[k];
    if (newk === false) {
      continue;
    } else if (!newk) {
      console.log(
        `WARNING: servicenow: util-table: no column mapped to "${k}"`
      );
      continue;
    }
    sn[newk] = v;
  }
  return sn;
};
