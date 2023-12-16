#cloud_function(platforms=[Platform.AWS], memory=3008, config=config)
def yourFunction(request, context):
    import json
    import logging
    from Inspector import Inspector
    import time
    import boto3
    from datetime import datetime
    import mysql.connector
    
    # Import the module and collect data 
    inspector = Inspector()
    inspector.inspectAll()

    s3 = boto3.resource('s3')
    
    filename = '100001 Sales Records.csv'
    obj = s3.Object('test.bucket.462562f23.kl', filename)
    body = obj.get()['Body'].read().decode('utf-8')
    body = body.strip().split('\r\n')
    headers = body[0].split(',')
    data_rows = body[1:]
    table_name = filename.split('.csv')[0]
    sql_insert_query = f'INSERT INTO `{table_name}` (`Region`, `Country`, `Item Type`, `Sales Channel`, `Order Priority`, `Order Date`, `Order ID`, `Ship Date`, `Units Sold`, `Unit Price`, `Unit Cost`, `Total Revenue`, `Total Cost`, `Total Profit`, `Order Processing Time`, `Gross Margin`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'


    connection = mysql.connector.connect(host='tlq.cluster-xxxx.us-west-2.rds.amazonaws.com',
                                         database='TLQ',
                                         port='3306',
                                         user='admin',
                                         passwd='xxxx')
    sql = "create table " + "`" + filename.split('.csv')[0] +  "`" + " (`Region` text, `Country` text, `Item Type` text, `Sales Channel` text, `Order Priority` text, `Order Date` date, `Order ID` integer PRIMARY KEY, `Ship Date` date, `Units Sold` integer, `Unit Price` float, `Unit Cost` float, `Total Revenue` float, `Total Cost` float, `Total Profit` float, `Order Processing Time` integer, `Gross Margin` float);"
    
    
    cursor = connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS `%s`;"%(filename.split('.csv')[0]))
    cursor.execute(sql)
    # print(sql)

    
    prepared_data = []
    for row in data_rows:
        columns = row.strip().split(',')
        columns[5] = datetime.strptime(columns[5], '%m/%d/%Y').date()
        columns[7] = datetime.strptime(columns[7], '%m/%d/%Y').date()
        prepared_data.append(columns)
    cursor.executemany(sql_insert_query, prepared_data)
    connection.commit()

    inspector.addAttribute('sqlsuccess', cursor.rowcount)
    inspector.inspectAllDeltas()
    return inspector.finish()
