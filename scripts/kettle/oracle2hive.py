# -*- coding: utf-8 -*-

import json
import dbutils


get_ora_meta_sql = '''
SELECT t1.OWNER
       ,t1.TABLE_NAME
       ,t1.COLUMN_NAME
       ,t1.DATA_TYPE
       ,t1.DATA_LENGTH
       ,t1.DATA_PRECISION
       ,t1.DATA_SCALE
       ,t2.COMMENTS
  FROM DBA_TAB_COLUMNS t1
  LEFT JOIN DBA_COL_COMMENTS t2
    ON t1.OWNER = t2.OWNER
   AND t1.TABLE_NAME = t2.TABLE_NAME
   AND t1.COLUMN_NAME = t2.COLUMN_NAME
 WHERE t1.OWNER = '%s'
   AND t1.TABLE_NAME = '%s'
 ORDER BY COLUMN_ID
'''

get_mysql_meta_sql = '''
SELECT TABLE_SCHEMA
       ,TABLE_NAME
       ,COLUMN_NAME
       ,ORDINAL_POSITION
       ,DATA_TYPE
       ,CHARACTER_MAXIMUM_LENGTH
       ,CHARACTER_OCTET_LENGTH
       ,NUMERIC_PRECISION
       ,NUMERIC_SCALE
       ,COLUMN_TYPE
  FROM INFORMATION_SCHEMA.COLUMNS
 WHERE TABLE_SCHEMA = '%s' 
   AND TABLE_NAME  = '%s'
   ;
'''


def get_ora_meta(conn, sql, src_schema, src_tb, hive_schema='', hive_tb=''):
    fields = []
    field_attrs = []
    cur = conn.cursor()
    sql = sql % (src_schema.upper(), src_tb.upper())
    print(sql)
    print('--' * 30)
    cur.execute(sql)
    res = cur.fetchall()

    for field in res:
        if field[3] == 'CLOB' or field == 'DATE':
            field_attr = field[2] + ' STRING ' + 'COMMENT \'' + str(field[7]) + '\''
            field_attrs.append(field_attr)
        elif field[3] == 'VARCHAR2' or field[3] == 'VARCHAR' or field[3] == 'CHAR':
            field_attr = field[2] + ' VARCHAR(' + str(field[4]) + ') COMMENT \'' + str(field[7]) + '\''
            field_attrs.append(field_attr)
        elif field[3] == 'NUMBER':
            field_attr = ''
            if field[6] == 0:
                field_attr = field[2] + ' BIGINT ' + 'COMMENT \'' + str(field[7]) + '\''
            elif field[5] is not None and field[6] is not None:
                field_attr = field[2] + ' DECIMAL(' + str(field[5]) + ',' + str(field[6]) + ') COMMENT \'' + str(field[7]) + '\''
            else:
                field_attr = field[2] + ' DECIMAL(23,4)' + ' COMMENT \'' + str(field[7]) + '\''
            field_attrs.append(field_attr)
        else:
            field_attr = field[2] + ' STRING ' + ' COMMENT \'' + str(field[7]) + '\''
            field_attrs.append(field_attr)
        # print(field)
        fields.append(field[2])
        # break
    cur.close()
    fields = ','.join(fields)
    field_attrs = ',\n'.join(field_attrs)
    # print(field_attrs)
    create_str = '''
CREATE TABLE %s.%s (\n%s\n)
PARTITIONED BY (TX_DATE STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS PARQUE
LOCATION '/DWZQ/%s/%s';
    '''
    hive_tb = '%s_%s' % (src_schema, src_tb)
    hive_tb_temp = '%s_TEMP' % hive_tb
    create_stmt = create_str % (hive_schema.upper(), hive_tb.upper(), field_attrs, hive_schema.upper(), hive_tb.upper())
    create_stmt_temp = create_str % (hive_schema.upper(), hive_tb_temp.upper(), field_attrs, hive_schema.upper(), hive_tb_temp.upper())
    print(create_stmt)
    print(create_stmt_temp)
    return create_stmt


def get_mysql_meta(conn, sql, src_schema, src_tb, hive_schema='', hive_tb=''):
    fields = []
    field_attrs = []
    cur = conn.cursor()
    sql = sql % (src_schema.upper(), src_tb.upper())
    print(sql)
    print('--' * 30)
    cur.execute(sql)
    res = cur.fetchall()

    for field in res:
        if field[3] == 'CLOB' or field == 'DATE':
            field_attr = field[2] + ' STRING ' + 'COMMENT \'' + str(field[7]) + '\''
            field_attrs.append(field_attr)
        elif field[3] == 'VARCHAR2' or field[3] == 'VARCHAR' or field[3] == 'CHAR':
            field_attr = field[2] + ' VARCHAR(' + str(field[4]) + ') COMMENT \'' + str(field[7]) + '\''
            field_attrs.append(field_attr)
        elif field[3] == 'NUMBER':
            field_attr = ''
            if field[6] == 0:
                field_attr = field[2] + ' BIGINT ' + 'COMMENT \'' + str(field[7]) + '\''
            elif field[5] is not None and field[6] is not None:
                field_attr = field[2] + ' DECIMAL(' + str(field[5]) + ',' + str(field[6]) + ') COMMENT \'' + str(field[7]) + '\''
            else:
                field_attr = field[2] + ' DECIMAL(23,4)' + ' COMMENT \'' + str(field[7]) + '\''
            field_attrs.append(field_attr)
        else:
            field_attr = field[2] + ' STRING ' + ' COMMENT \'' + str(field[7]) + '\''
            field_attrs.append(field_attr)
        # print(field)
        fields.append(field[2])
        # break
    cur.close()
    fields = ','.join(fields)
    field_attrs = ',\n'.join(field_attrs)
    # print(field_attrs)
    create_str = '''
CREATE TABLE %s.%s (\n%s\n)
PARTITIONED BY (TX_DATE STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\u0001'
STORED AS PARQUE
LOCATION '/DWZQ/%s/%s';
    '''
    hive_tb = '%s_%s' % (src_schema, src_tb)
    hive_tb_temp = '%s_TEMP' % hive_tb
    create_stmt = create_str % (hive_schema.upper(), hive_tb.upper(), field_attrs, hive_schema.upper(), hive_tb.upper())
    create_stmt_temp = create_str % (hive_schema.upper(), hive_tb_temp.upper(), field_attrs, hive_schema.upper(), hive_tb_temp.upper())
    print(create_stmt)
    print(create_stmt_temp)
    return create_stmt


def run(tb_info_details):
    for tb_info in tb_info_details:
        conn = dbutils.get_conn(tb_info['data_src'].lower())
        src_owner = tb_info['src_tb'].split('.')[0]
        src_tb = tb_info['src_tb'].split('.')[1]
        hive_schema = tb_info['data_src']
        # hive_tb =
        get_ora_meta(conn, get_ora_meta_sql, src_owner, src_tb, hive_schema)


if __name__ == '__main__':
    with open('cfg.json', 'r') as f:
        tb_info_details = json.load(f)
    run(tb_info_details)
