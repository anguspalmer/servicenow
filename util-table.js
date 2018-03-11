const { titlize } = require("./util");

//expand short-hand js table
exports.expandTable = table => {
  if (!table.id) {
    throw `Table must have an "id"`;
  }
  for (let name in table.columns) {
    let col = table.columns[name];
    if (col.name) {
      //"name" provided
      name = col.name;
    } else {
      //"name" inferred (add "u_")
      let uname = `u_${name}`;
      name = uname;
      col.name = uname;
    }
    table.columns[name] = exports.expandColumn(col);
  }
  return table;
};

//expand short-hand js column
exports.expandColumn = col => {
  if (!col.name) {
    throw `Missing column "name"`;
  }
  if (typeof col.type !== "string") {
    throw `Expected column "${col.name}" type to be a string`;
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
    default:
      throw `Unknown column type "${col.type}"`;
  }
  if (!col.max_length) {
    col.max_length = default_length;
  }
  return col;
};
