const sync = require("sync");
const { one } = require("./util");
const CONCURRENCY = 20;

module.exports = class CRelate {
  constructor(client) {
    this.client = client;
  }

  //sync all rows found in <table> using the provided <relationships>.
  async table(tableName, relationships, status = null) {
    //build list of columns we need
    const columns = Object.keys(relationships);
    if (columns.length === 0) {
      return true;
    }
    //also include sys_id
    columns.push("sys_id");
    //load all existing rows
    const rows = await this.client.getRecords(tableName, {
      columns,
      status,
      cache: true
    });
    //sync relationships!
    return await this.rows(rows, relationships, status);
  }

  //sync <rows> using the provided <relationships>.
  //
  // notes:
  //   rows is an array of CI objects.
  //   each row is expected to have one or more
  //   reference columns (columns which contain the
  //   sys_id of their parent CI). given the referenced
  //   CI is the parent, it is the "one" in the "one-to-
  //   many" relationship, and is therefore the upstream
  //   CI, since the provided CIs (rows) are downstream
  //   (i.e. downtime flows "downstream").
  //
  //   cmdb_rel_ci =
  //     "type"   "<parent to child>::<child to parent>"
  //           OR "<parent_descriptor>::<child_descriptor>"
  //           EG "Managed by"   ::  "Manages"
  //     "parent" <ci> EG "virtual machine"
  //     "child"  <ci> EG "esx host" / "esx vcenter"
  //
  //   parent ci shows relationships:
  //      <parent ci> "downstream rels..."
  //         "<parent to child>" --> "<child ci>"
  //         (testvm, managed by, esxhost01)
  //
  //   child ci shows relationships:
  //      <child ci> "upstream rels..."
  //         "<child to parent>" --> "<parent ci>"
  //         (esxhost01, manages, testvm)
  //
  //   upstream = depends on me
  //   downstream = dependant on
  //
  async rows(rows, relationships, status = null) {
    if (!Array.isArray(rows)) {
      throw `Rows must be an array`;
    }
    if (!status) {
      //NOTE: @jpillora: allow this case?
      throw `Merge without "status" not supported`;
    }
    //convert spec.columns into relationship types
    const types = {};
    const typeIds = new Set();
    for (const columnName in relationships) {
      const relationship = relationships[columnName];
      if (!relationship) {
        throw `column "${columnName}" missing "relationship"`;
      } else if (!/^(.+)::(.+)$/.test(relationship)) {
        throw `column "${columnName}" invalid "relationship" format (${relationship})`;
      }
      const parent_descriptor = RegExp.$1;
      const child_descriptor = RegExp.$2;
      const query =
        `parent_descriptor=${parent_descriptor}^` +
        `child_descriptor=${child_descriptor}`;
      const request = {
        url: "/v2/table/cmdb_rel_type",
        params: {
          sysparm_query: query
        }
      };
      const get = async () => one(await this.client.do(request));
      let record = await get();
      if (!record) {
        //TODO: jpillora: 26/9/2018:
        //create relationship fails if EITHER parent or child already exists
        //on another relationship. also, randomly API-created relationships
        //appear as blank in the "CI relationship" widget. for now create manually.
        throw `Missing relationship type "${relationship}". Please create manually.`;
        // status.log(`creating relationship: "${relationship}"`);
        // const fields = { parent_descriptor, child_descriptor };
        // await this.client.create("cmdb_rel_type", fields);
        // record = await get();
      }
      if (typeIds.has(record.sys_id)) {
        throw `Multiple relationships for type "${record.name}"`;
      }
      typeIds.add(record.sys_id);
      types[columnName] = record;
    }
    const typeNames = Object.keys(types);
    if (typeNames.length === 0) {
      //no rows to relate
      return true;
    }
    //compute all relationships for all types
    for (const columnName in types) {
      const type = types[columnName];
      const sourceIds = new Set();
      //compute all new relationships
      const newRels = [];
      for (const row of rows) {
        if (!row.sys_id) {
          throw `Row missing "sys_id"`;
        }
        sourceIds.add(row.sys_id);
        const otherId = row[columnName];
        if (!otherId) {
          //disconnected
          continue;
        }
        const parent = row.sys_id;
        const child = otherId;
        const newRel = {
          type: type.sys_id,
          child,
          parent
        };
        newRels.push(newRel);
      }
      //DEBUG
      // if (/vm_instance/.test(columnName)) while (newRels.length > 30) newRels.pop();

      //compute all existing relationships
      //(get all, exclude those not matching the incoming set)
      const existingRels = (await this.client.getRecords("cmdb_rel_ci", {
        status,
        fields: ["sys_id", "parent", "child"],
        query: `type=${type.sys_id}`,
        cache: true
      })).filter(rel => {
        return sourceIds.has(rel.parent);
      });
      //print status
      if (status) {
        status.log(
          `syncing CI relationships ` +
            `(incoming #${newRels.length}, existing #${existingRels.length})`
        );
      }
      //perform diff!
      let results = await sync.diff({
        status,
        concurrency: CONCURRENCY,
        prev: existingRels,
        next: newRels,
        index: rel => `${rel.parent}|${rel.child}`,
        create: async newRel => {
          // status.log(`relate: ${newRel.parent} -> ${newRel.child}`);
          await this.client.create("cmdb_rel_ci", newRel);
        },
        delete: async existingRel => {
          // status.log(`unrelate: ${existingRel.parent} -> ${existingRel.child}`);
          await this.client.delete("cmdb_rel_ci", existingRel);
        }
      });
      //print status
      if (status) {
        let msgs = [];
        if (results.create.length) {
          msgs.push(`created #${results.create.length}`);
        }
        if (results.delete.length) {
          msgs.push(`deleted #${results.delete.length}`);
        }
        let msg = msgs.length > 0 ? msgs.join(", ") : "no changes";
        status.log(`synced CI relationships (${msg})`);
      }
      //done!
    }
    return true;
  }

  log(...args) {
    this.client.log("[relate]", ...args);
  }
};
