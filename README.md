# servicenow

A ServiceNow REST API client

* Provides low-level API, `do()` which `axios` to provide ServiceNow specific helpers to all HTTP requests
* Provides high-level APIs to CRUD records and modify tables

### Table sync refernces

* Automate table and column creation using glide record
  https://community.servicenow.com/thread/178725

### Count records

`https://ac3dev.service-now.com/api/now/v1/stats/u_imp_dm_backup_job?sysparm_limit=10&sysparm_count=true`
