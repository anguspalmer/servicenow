const { titlize } = require("./util");

//expand short-hand js table
exports.expandTable = table => {
  if (!table.name) {
    throw `Table must have a "name"`;
  }
  for (let id in table.columns) {
    let col = table.columns[id];
    if (!col.name) {
      //infer name
      if (/cmdb/.test(table.name) && id === "name") {
        //out-of-the-box, use id as is
        col.name = id;
      } else {
        //add "u_" to id
        col.name = `u_${id}`;
      }
    }
    exports.expandColumn(col);
    table.columns[id] = col;
  }
};

//expand short-hand js column
exports.expandColumn = col => {
  if (!col.name) {
    throw `Missing column "name"`;
  }
  if (typeof col.type !== "string") {
    throw `Column "${col.name}" is missing the "type" property`;
  }
  col.type = col.type.toLowerCase();
  if (!col.label) {
    col.label = titlize(col.name);
  }
  let default_length;
  //check type AND set default length
  switch (col.type) {
    case "text":
      default_length = 65000;
      break;
    case "string":
      default_length = 255;
      break;
    case "integer":
    case "float":
    case "decimal":
    case "boolean":
      default_length = 40;
      break;
    case "date":
      default_length = 40;
      col.type = "glide_date_time";
      break;
    case "reference":
    case "guid":
      default_length = 32;
      break;
    default:
      throw `Unknown column type "${col.type}"`;
  }
  if (!col.max_length) {
    col.max_length = default_length;
  }
  if (col.type === "reference" && !col.reference_table) {
    throw `Column "${col.name}" is missing the "reference_table" property`;
  }
};

//convert js column to sn column
exports.snColumn = js => {
  if (!js) {
    throw `Missing column`;
  } else if (!js.name) {
    throw `Missing column name`;
  }
  let sn = {};
  for (let k in js) {
    let v = js[k];
    if (k === "name") {
      k = "element";
    } else if (k === "type") {
      k = "internal_type";
    } else if (k === "label") {
      k = "column_label";
    }
    sn[k] = v;
  }
  return sn;
};
