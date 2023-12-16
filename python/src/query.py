#cloud_function(platforms=[Platform.AWS], memory=3008, config=config)
def yourFunction(request, context):
    import json
    import logging
    from Inspector import Inspector
    import time
    from datetime import datetime
    import mysql.connector
    from decimal import Decimal

    
    # Import the module and collect data 
    inspector = Inspector()
    inspector.inspectAll()
    
    filename = '100001 Sales Records.csv'
    table_name = filename.split('.csv')[0]
    
    filters = request.get("filters", {})
    group_by = request["aggregations"]["groupBy"]
    operations = request["aggregations"]["operations"]

    connection = mysql.connector.connect(host='tlq.cluster-xxxx.us-west-2.rds.amazonaws.com',
                                         database='TLQ',
                                         port='3306',
                                         user='admin',
                                         passwd='xxxx')
                                         
    cursor = connection.cursor()
    

    sql = "SELECT "
    sql += ", ".join(group_by + operations)
    sql += " FROM `" + table_name + "`"
    if filters:
        where_clauses = [f"`{key}` = '{value}'" for key, value in filters.items()]
        sql += " WHERE " + " AND ".join(where_clauses)
    sql += " GROUP BY " + ", ".join(group_by)

    # print(sql)
    cursor.execute(sql)
    result = cursor.fetchall()
    

    columns = [col[0] for col in cursor.description]
    response = [dict(zip(columns, row)) for row in result]

    # print(response)
    def handle_decimal(obj):
        if isinstance(obj, Decimal):
            return float(obj)

    inspector.addAttribute('sqlsuccess', response)
    inspector.inspectAllDeltas()
    return inspector.finish()
