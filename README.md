# servicenow

A ServiceNow REST API client

* Provides low-level API, `do()` which `axios` to provide ServiceNow specific helpers to all HTTP requests
* Provides high-level APIs to CRUD records and modify tables

### Table sync refernces

* Automate table and column creation using glide record
  https://community.servicenow.com/thread/178725

### Count records

`https://ac3dev.service-now.com/api/now/v1/stats/u_imp_dm_backup_job?sysparm_limit=10&sysparm_count=true`

### RnD Notes

#### Data Policy

had a look, it should be doable with a `Data Policy`:

1.  create a new data policy for each table here `sys_data_policy2` with the policy: `created_by = DataMart`
2.  create new read-only rules for each field here `sys_data_policy_rule`

works when done via platform UI, can GET using table API too:

from data policy:

```
"model_table": "u_cmdb_ci_trendmicro_dsm_host",
"conditions": "sys_created_by=DataMart^EQ",
```

from rule:

```
"mandatory": "ignore",
"disabled": "true", //it appears this represents read-only
"field": "u_update_checked",
"table": "u_cmdb_ci_trendmicro_dsm_host",
```

shouldnt be hard to sync table/columns with policies/rules, will put it aside for now though, since its not critical atm (edited)
