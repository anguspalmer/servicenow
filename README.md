One at a time:

    [DM] -> IMPORT -> [SN XMAP 1,2,3]

Bulk insert:

    [DM] -> DELTA -> [MIRROR SN] -> [SN XMAP 1,2,3]

    Requires mirror table

### Delta Import

* `GET /api/now/v2/table/u_imp_dm_vm_instance` uses the table API to returns all previous imports
* compare incoming dataset to previous imports
* import only those with changed data
* after an import, delete the older import result to ensure its PK is unique

### Auto Table sync

* Automate table and column creation using glide record
  https://community.servicenow.com/thread/178725

### Count records

`https://ac3dev.service-now.com/api/now/v1/stats/u_imp_dm_backup_job?sysparm_limit=10&sysparm_count=true`
