
select
  tt.table_schema,
  tt.table_name,
  -- pg stg 删表语句
  concat('drop table if exists stg_7d.' ,tt.table_name,';') as gp_drop_sql,
  -- pg 建表语句
  concat('create table stg_7d.',tt.table_name,'\n(\n',' ',tt.gp_column_desc,
         ',last_modify_time timestamp','\n) with (\n APPENDONLY=true,\n ORIENTATION=column,\n COMPRESSTYPE=zlib,\n COMPRESSLEVEL=5,\n BLOCKSIZE=65536,\n OIDS=false)\n distributed by (',dist_column,')\n;',
		 '\n',
		 'COMMENT ON TABLE stg_7d.',tt.table_name,' is \'',t.table_comment,'\';' ,
		 '\n',
		 replace(replace(tt.gp_comment_desc,',comment','comment'),'sevend_replace_flag.','stg_7d.'),
		 '\n;\n'
		 'create table ods_7d.',tt.table_name,'\n(\n',' ',tt.gp_column_desc,
         ',last_modify_time timestamp','\n) with (\n APPENDONLY=true,\n ORIENTATION=column,\n COMPRESSTYPE=zlib,\n COMPRESSLEVEL=5,\n BLOCKSIZE=65536,\n OIDS=false)\n distributed by (',dist_column,')\n;',
		 '\n',
		 'COMMENT ON TABLE ods_7d.',tt.table_name,' is \'',t.table_comment,'\';' ,
		 '\n',
		 replace(replace(tt.gp_comment_desc,',comment','comment'),'sevend_replace_flag.','ods_7d.'),
         '\n',
		 'grant select on ods_7d.',tt.table_name,' to dw_select;'
		)  as gp_create_sql,
  -- odps 删除表语句
  concat('drop table if exists ods_7d_' ,tt.table_name,';') as odps_drop_sql,
  -- odps 建表语句
  concat('create table stg_7d_',tt.table_name,'\n(\n',' ',tt.odps_column_desc,
         ',last_modify_time datetime comment \'系统最后更新时间戳\'','\n) \n comment \'',t.table_comment,'\';',
		 '\n',
		 'create table ods_7d_',tt.table_name,'\n(\n',' ',tt.odps_column_desc,
         ',last_modify_time datetime comment \'系统最后更新时间戳\'','\n) \n comment \'',t.table_comment,'\';'
		 ) as odps_create_sql,
 -- odps stg 合并
 concat(
  'insert overwrite table ods_7d_',
  tt.table_name,
  '\nselect\n  --所有select操作，如果stg表有数据，说明发生了变动，以stg表为准\n  ',
  ods_overwrite_script,
  ',getdate() as last_modify_time from ods_7d_',
  tt.table_name,
  ' a \nfull outer join stg_7d_',tt.table_name,' b\n  on a.id  = b.id ;'
  ) as ods_overwrite_scrpit,
  -- 初始化同步
concat(
'
{
  "order":{
    "hops":[
      {
        "from":"Reader",
        "to":"Writer"
      }
    ]
  },
  "setting":{
    "errorLimit":{
      "record":"0"
    },
    "speed":{
      "concurrent":2,
      "dmu":2,
      "throttle":false
    }
  },
  "steps":[
    {
      "category":"reader",
      "name":"Reader",
      "parameter":{
        "column":[',column_sync,',"now()"\n],
        "connection":[
          {
            "datasource":"sevend_db",
            "table":["',tt.table_name,'"
            ]
          }
        ],
        "encoding":"UTF-8",
        "splitPk":"id",
        "where":""
      },
      "stepType":"mysql"
    },
    {
      "category":"writer",
      "name":"Writer",
      "parameter":{
        "column":[',column_sync,',"last_modify_time"\n],
        "datasource":"odps_first",
        "partition":"",
        "table":"ods_7d_',tt.table_name,'",
        "truncate":true
      },
      "stepType":"odps"
    }
  ],
  "type":"job",
  "version":"2.0"
}') as init_data_sync_script,
-- odps stg 同步
concat(
'{
  "order":{
    "hops":[
      {
        "from":"Reader",
        "to":"Writer"
      }
    ]
  },
  "setting":{
    "errorLimit":{
      "record":"0"
    },
    "speed":{
      "concurrent":2,
      "dmu":2,
      "throttle":false
    }
  },
  "steps":[
    {
      "category":"reader",
      "name":"Reader",
      "parameter":{
        "column":[',column_sync,',"now()"\n],
        "connection":[
          {
            "datasource":"sevend_db",
            "table":["',tt.table_name,'"
            ]
          }
        ],
        "encoding":"UTF-8",
        "splitPk":"id",
        "where":"update_time >= str_to_date(\'${bdp.system.bizdate}\',\'%Y%m%d\')"
      },
      "stepType":"mysql"
    },
    {
      "category":"writer",
      "name":"Writer",
      "parameter":{
        "column":[',column_sync,',"last_modify_time"\n],
        "datasource":"odps_first",
        "partition":"",
        "table":"stg_7d_',tt.table_name,'",
        "truncate":true
      },
      "stepType":"odps"
    }
  ],
  "type":"job",
  "version":"2.0"
}
') as stg_data_sync_script,

concat(
'{
  "order":{
    "hops":[
      {
        "from":"Reader",
        "to":"Writer"
      }
    ]
  },
  "setting":{
    "errorLimit":{
      "record":"0"
    },
    "speed":{
      "concurrent":1,
      "dmu":1,
      "throttle":false
    }
  },
  "steps":[
    {
      "category":"reader",
      "name":"Reader",
      "parameter":{
        "column":[', column_sync, ',"last_modify_time"\n],
        "datasource":"odps_first",
        "partition":[],
        "table":"stg_7d_', tt.table_name, '"
      },
      "stepType":"odps"
    },
    {
      "category":"writer",
      "name":"Writer",
      "parameter":{
        "column":[', column_sync, ',"last_modify_time"\n],
        "datasource":"HybirdDB_for_PostgreSQL",
        "postSql":["set optimizer=on;",
                   "delete from ods_7d.',tt.table_name, ' t1 using stg.', tt.table_name,' t2 where t1.id=t2.id;
                   "insert into ods_7d.', tt.table_name, ' select * from stg_7d.',tt.table_name, ';"\n
        ],
        "preSql":["truncate table stg_7d.', tt.table_name, ';"\n],
        "table":"stg_7d.', tt.table_name, '"
      },
      "stepType":"postgresql"
    }
  ],
  "type":"job",
  "version":"2.0"
}
'
) as pg_data_sync_script,

concat(
'{
    "type": "job",
    "steps": [
        {
            "stepType": "mysql",
            "parameter": {
                "column": [', column_sync, ',"now()"\n],
                "connection": [
                    {
                        "datasource": "sevend_db",
                        "table": ["', tt.table_name, '"]
                    }
                ],
                "where": "",
                "splitPk": "id",
                "encoding": "UTF-8"
            },
            "name": "Reader",
            "category": "reader"
        },
        {
            "stepType": "postgresql",
            "parameter": {
                "postSql": [],
                "datasource": "HybirdDB_for_PostgreSQL",
                "column": [', column_sync, ',"last_modify_time"\n],
                "table": "ods_7d.', tt.table_name, '",
                "preSql": ["truncate table ods_7d.', tt.table_name, ';"\n]
            },
            "name": "Writer",
            "category": "writer"
        }
    ],
    "version": "2.0",
    "order": {
        "hops": [
            {
                "from": "Reader",
                "to": "Writer"
            }
        ]
    },
    "setting": {
        "errorLimit": {
            "record": ""
        },
        "speed": {
            "concurrent": 2,
            "throttle": false,
            "dmu": 1
        }
    }
}'
) as mysql2pg_init
from
 (
  select
    table_schema,
    table_name,
    min(case when column_name = 'customer_id' then 'customer_id'
             when column_name = 'id'	then 'id'
             else null end) dist_column ,
    group_concat(column_name,' ' ,
    case when data_type = 'tinyint' then 'int'
	     when data_type = 'smallint' then 'int'
	     when data_type = 'int' then 'int'
	     when data_type = 'bigint' then 'bigint'
	     when data_type = 'double' then 'numeric(11,7)'
	     when data_type = 'datetime' then 'timestamp'
         else column_type end,'\n ') as gp_column_desc,
    group_concat('comment on column ',table_schema,'_replace_flag','.',table_name,'.',column_name,' is \'',replace(column_comment,'\'',''),'\' ;\n') as gp_comment_desc,
    group_concat(column_name,' ' ,
    case when data_type like '%int' then 'bigint'
	     when data_type in ('timestamp','date') then 'datetime'
		 when data_type = 'double' then 'decimal'
		 when data_type in ('char','text','varchar','time') then 'string'
         else data_type end,' comment \'',replace(column_comment,'\'',''),'\'\n ') as odps_column_desc,
    group_concat('"',column_name,'"','\n') as column_sync,
    group_concat('case when b.id is not null then b.',column_name ,' else a.',column_name,' end as ',column_name,'\n  ') as ods_overwrite_script
  from information_schema.columns c
  where
  data_type not in ('mediumblob','mediumtext','text')
  and table_schema = 'sevend'
  and table_name  = 't_szs_loan_info'
  and column_name not in
  ('phone',
'idNumber',
'cardNo',
'bind_phone',
'card_id',
'bank_card',
'customer_mobile',
'mobile',
'family_contract_mobile',
'friend_contract_mobile',
'id_card_no',
'id_photo_positive_url',
'id_photo_negative_url',
'name_card_mobile',
'id_no',
'bank_card_no',
'id_no_pic_url',
'bank_card_pic_url',
'card_no',
'loan_bank_card',
'bank_detail',
'id_photo_positive_key',
'id_end_time',
'id_card_name',
'id_photo_negative_key'
)
  group by table_schema,table_name
 ) tt
  inner join
    information_schema.tables t
  on tt.table_name = t.table_name
  and tt.table_schema = t.table_schema;

